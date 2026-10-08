from pyspark.sql.functions import (
    col,
    lit,
    when,
    max as spark_max,
    row_number,
    current_date,
    date_sub
)

from pyspark.sql.window import Window

def apply(self, source_df, target_df):

    # --------------------------------------------------
    # 1. Current target records
    # --------------------------------------------------

    current_target_df = (
        target_df
        .filter(col("current") == "Y")
    )

    # --------------------------------------------------
    # 2. Compare source with current target
    # --------------------------------------------------

    joined_df = (
        source_df.alias("s")
        .join(
            current_target_df.alias("t"),
            col("s.product_id") == col("t.product_id"),
            "left"
        )
    )

    # --------------------------------------------------
    # 3. Classify records
    # --------------------------------------------------

    classified_df = (
        joined_df
        .withColumn(
            "record_status",
            when(
                col("t.product_id").isNull(),
                lit("NEW")
            )
            .when(
                (~col("s.product_name").eqNullSafe(col("t.product_name"))) |
                (~col("s.category").eqNullSafe(col("t.category"))) |
                (~col("s.price").eqNullSafe(col("t.price"))),
                lit("CHANGED")
            )
            .otherwise(
                lit("UNCHANGED")
            )
        )
    )

    # --------------------------------------------------
    # 4. Get changed product IDs
    # --------------------------------------------------

    changed_ids_df = (
        classified_df
        .filter(col("record_status") == "CHANGED")
        .select(
            col("s.product_id").alias("product_id")
        )
    )

    # --------------------------------------------------
    # 5. Expire old changed records
    # --------------------------------------------------

    expired_target_df = (
        target_df.alias("t")
        .join(
            changed_ids_df.alias("c"),
            col("t.product_id") == col("c.product_id"),
            "left"
        )

        # create condition BEFORE changing columns
        .withColumn(
            "should_expire",
            col("c.product_id").isNotNull() &
            (col("t.current") == "Y")
        )

        .withColumn(
            "end_date",
            when(
                col("should_expire"),
                date_sub(current_date(), 1)
            )
            .otherwise(
                (col("t.end_date"))
            )
        )

        .withColumn(
            "current",
            when(
                col("should_expire"),
                lit("N")
            )
            .otherwise(
                col("t.current")
            )
        )

        .select(
            col("t.product_sk").alias("product_sk"),
            col("t.product_id").alias("product_id"),
            col("t.product_name").alias("product_name"),
            col("t.category").alias("category"),
            col("t.price").alias("price"),
            col("t.start_date").alias("start_date"),
            col("end_date"),
            col("current")
        )
    )

    # --------------------------------------------------
    # 6. Create rows to INSERT
    # NEW + CHANGED
    # --------------------------------------------------

    insert_df = (
        classified_df
        .filter(
            col("record_status").isin("NEW", "CHANGED")
        )
        .select(
            col("s.product_id").alias("product_id"),
            col("s.product_name").alias("product_name"),
            col("s.category").alias("category"),
            col("s.price").alias("price")
        )
        .withColumn(
            "start_date",
            current_date()
        )
        .withColumn(
            "end_date",
            lit("9999-12-31").cast("date")
        )
        .withColumn(
            "current",
            lit("Y")
        )
    )

    # --------------------------------------------------
    # 7. Generate surrogate keys
    # --------------------------------------------------

    max_sk = (
        target_df
        .agg(
            spark_max("product_sk")
        )
        .collect()[0][0]
    )

    window_spec = Window.orderBy("product_id")

    insert_df = (
        insert_df
        .withColumn(
            "product_sk",
            row_number().over(window_spec) + lit(max_sk)
        )
        .select(
            "product_sk",
            "product_id",
            "product_name",
            "category",
            "price",
            "start_date",
            "end_date",
            "current"
        )
    )

    # --------------------------------------------------
    # 8. Combine history + new versions
    # --------------------------------------------------

    final_df = (
        expired_target_df
        .unionByName(insert_df)
        .orderBy("product_sk")
    )

    return final_df