
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "salting_demo_data"

# Demo settings
DEMO_ROWS = 10000
HOT_KEY_THRESHOLD = 500
TARGET_ROWS_PER_BUCKET = 500
MAX_BUCKETS = 16


# --------------------------------------------------
# 1. CREATE SAMPLE TRANSACTION DATA
# --------------------------------------------------
def create_demo_transactions(spark):

    ordinal = F.col("id") % 100

    customer_id = (
        F.when(ordinal < 40, F.lit("C101"))
        .when(ordinal < 60, F.lit("C205"))
        .when(ordinal < 68, F.lit("C330"))
        .otherwise(
            F.concat(
                F.lit("C"),
                (F.lit(401) + F.col("id") % 20).cast("string")
            )
        )
    )

    return (
        spark.range(DEMO_ROWS)
        .select(
            F.concat(
                F.lit("T"),
                F.lpad(F.col("id").cast("string"), 7, "0")
            ).alias("transaction_id"),

            customer_id.alias("customer_id"),

            (F.col("id") % 1000 + 1)
            .cast("decimal(18,2)")
            .alias("amount")
        )
    )


# --------------------------------------------------
# 2. CREATE CUSTOMER REFERENCE DATA
# --------------------------------------------------
def create_customer_reference(spark):

    rows = [
        ("C101", "Alice"),
        ("C205", "Bob"),
        ("C330", "Charlie")
    ]

    rows += [
        (f"C{i}", f"Customer {i}")
        for i in range(401, 421)
    ]

    return spark.createDataFrame(
        rows,
        ["customer_id", "customer_name"]
    )


# --------------------------------------------------
# 3. IDENTIFY MULTIPLE HOT KEYS
# --------------------------------------------------
def profile_hot_keys(historical_transactions):

    return (
        historical_transactions
        .groupBy("customer_id")
        .agg(F.count("*").alias("key_count"))

        .filter(
            F.col("key_count") > HOT_KEY_THRESHOLD
        )

        .withColumn(
            "bucket_count",
            F.least(
                F.lit(MAX_BUCKETS),
                F.greatest(
                    F.lit(2),
                    F.ceil(
                        F.col("key_count") /
                        F.lit(TARGET_ROWS_PER_BUCKET)
                    ).cast("int")
                )
            )
        )

        .select(
            "customer_id",
            "key_count",
            "bucket_count"
        )
    )


# --------------------------------------------------
# 4. SALT TRANSACTIONS AND CUSTOMER REFERENCE
# --------------------------------------------------
def salted_join(transactions, customers, hot_config):

    config = hot_config.select(
        "customer_id",
        "bucket_count"
    )

    # SALT TRANSACTIONS
    tx = (
        transactions
        .join(
            F.broadcast(config),
            "customer_id",
            "left"
        )

        .withColumn(
            "bucket_count",
            F.coalesce("bucket_count", F.lit(1))
        )

        .withColumn(
            "salt",
            F.pmod(
                F.xxhash64("transaction_id"),
                F.col("bucket_count")
            ).cast("int")
        )

        .drop("bucket_count")
    )

    # REPLICATE HOT CUSTOMER ROWS
    dimension = (
        customers
        .join(
            F.broadcast(config),
            "customer_id",
            "left"
        )

        .withColumn(
            "bucket_count",
            F.coalesce("bucket_count", F.lit(1))
        )

        .withColumn(
            "salt",
            F.explode(
                F.sequence(
                    F.lit(0),
                    F.col("bucket_count") - F.lit(1)
                )
            )
        )

        .drop("bucket_count")
    )

    # SALTED JOIN
    result = (
        tx.join(
            dimension,
            ["customer_id", "salt"],
            "left"
        )
        .drop("salt")
    )

    return result, tx, dimension


# --------------------------------------------------
# 5. VALIDATE THE OUTPUT
# --------------------------------------------------
def validate(transactions, joined):

    input_count = transactions.count()
    output_count = joined.count()

    input_amount = (
        transactions.agg(F.sum("amount")).first()[0]
    )

    output_amount = (
        joined.agg(F.sum("amount")).first()[0]
    )

    missing_customers = (
        joined.filter(
            F.col("customer_name").isNull()
        ).count()
    )

    duplicate_tx = (
        joined.groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    print("Input rows:", input_count)
    print("Output rows:", output_count)
    print("Input amount:", input_amount)
    print("Output amount:", output_amount)

    assert input_count == output_count
    assert input_amount == output_amount
    assert missing_customers == 0
    assert duplicate_tx == 0

    print("VALIDATION PASSED")


# --------------------------------------------------
# 6. MAIN SPARK APPLICATION
# --------------------------------------------------
def main():

    spark = (
        SparkSession.builder
        .master("local[4]")
        .appName("DynamicMultiHotKeySalting")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        customers = create_customer_reference(spark)
        customers.show(1000, truncate=False)
        demo_transactions = create_demo_transactions(spark)
        demo_transactions.show(1000, truncate=False)

        # Generate historical hot-key configuration
        config_path = DATA_DIR / "hot_key_config"

        profile_hot_keys(
            demo_transactions
        ).write.mode("overwrite").parquet(
            str(config_path)
        )

        config = (
            spark.read
            .parquet(str(config_path))
            .cache()
        )

        print("\nHOT KEY CONFIGURATION")
        config.orderBy(
            F.desc("key_count")
        ).show(truncate=False)

        # Reference must contain one row per customer
        duplicates = (
            customers.groupBy("customer_id")
            .count()
            .filter("count > 1")
            .limit(1)
            .count()
        )

        assert duplicates == 0

        mode = os.environ.get(
            "RUN_MODE", "batch"
        ).strip().lower()

        # -------- BATCH MODE --------
        if mode == "batch":

            result, salted_tx, replicated_customers = (
                salted_join(
                    demo_transactions,
                    customers,
                    config
                )
            )

            print("\nBUCKET DISTRIBUTION FOR C101")

            (
                salted_tx
                .filter(F.col("customer_id") == "C101")
                .groupBy("salt")
                .count()
                .orderBy("salt")
                .show()
            )

            print("\nCUSTOMER C101 REPLICATION")

            (
                replicated_customers
                .filter(F.col("customer_id") == "C101")
                .orderBy("salt")
                .show(25)
            )

            print("\nFINAL JOINED RESULTS")

            result.orderBy(
                "transaction_id"
            ).show(15, truncate=False)

            validate(demo_transactions, result)

            output_path = DATA_DIR / "batch_output"

            (
                result.write
                .mode("overwrite")
                .parquet(str(output_path))
            )

            print("Saved output:", output_path)

        # -------- STREAMING MODE --------
        elif mode == "stream":

            input_path = DATA_DIR / "stream_input"

            # Generate initial local event files once
            if not any(input_path.glob("part-*.parquet")):
                (
                    demo_transactions.write
                    .mode("overwrite")
                    .parquet(str(input_path))
                )

            streaming_df = (
                spark.readStream
                .schema(demo_transactions.schema)
                .parquet(str(input_path))
            )

            output_root = DATA_DIR / "stream_output"

            def process_micro_batch(batch_df, batch_id):

                result, _, _ = salted_join(
                    batch_df,
                    customers,
                    config
                )

                print("Processing batch:", batch_id)

                validate(batch_df, result)

                batch_path = (
                        output_root / f"batch_{batch_id}"
                )

                (
                    result.write
                    .mode("overwrite")
                    .parquet(str(batch_path))
                )

                print("Saved batch:", batch_path)

            query = (
                streaming_df.writeStream
                .foreachBatch(process_micro_batch)
                .option(
                    "checkpointLocation",
                    str(DATA_DIR / "stream_checkpoint")
                )
                .trigger(availableNow=True)
                .start()
            )

            query.awaitTermination()

            print("Streaming simulation completed")

        else:
            raise ValueError(
                "RUN_MODE must be batch or stream"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
