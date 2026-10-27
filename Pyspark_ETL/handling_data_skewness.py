from pyspark.sql import functions as F

def handling_data_skewness(spark):
    sales_data = [
        (101, "P001", 100),
        (101, "P002", 200),
        (101, "P003", 150),
        (102, "P004", 300),
        (103, "P005", 400),
        (101, "P006", 500),
        (104, "P007", 250)]


    sales_df = spark.createDataFrame(sales_data, ["store_id", "product_id", "amount"])

    print(" Sales Dataframe ")
    sales_df.show()

    store_data = [
        (101, "Walmart"),
        (102, "Target"),
        (103, "Costco"),
        (104, "BestBuy")]

    store_df = spark.createDataFrame(store_data, ["store_id", "store_name"])

    print(" Store Dataframe ")
    store_df.show()

    num_salt_keys = 3
    print("Number of salt keys: ", num_salt_keys)

    sales_df_salted = (sales_df
                       .withColumn("salt", F.floor(F.rand() * num_salt_keys))
                       #1. - Generates a random number between 0.0 and 1.0 `rand()`
                       #2. `floor()` - Rounds down to the nearest integer
                       #3. - In this code, it's set to 3, so it will generate values 0, 1, or 2 `num_salt_keys`
                       .withColumn("salted_store_id", F.concat_ws("_", F.col("store_id"), F.col("salt"))))

    print(" salted sales dataframe ")
    sales_df_salted.show()

    expanded_store_df = (store_df
                         .crossJoin(spark.range(0, num_salt_keys).toDF("salt"))
                         .withColumn("salted_store_id", F.concat_ws("_",F.col("store_id"), F.col("salt"))))

    print(" expanded dataframe ")
    expanded_store_df.show()

    joined_df = sales_df_salted.join(expanded_store_df, "salted_store_id", "inner").drop("salted_store_id", "salt")

    joined_df.show()

def handling_data_skewness_2(spark):

    transactions_df = spark.createDataFrame(
        [
            ("T1", "C101", 500),
            ("T2", "C101", 750),
            ("T3", "C101", 300),
            ("T4", "C101", 950),
            ("T5", "C102", 200),
            ("T6", "C103", 400),
        ],
        ["transaction_id", "customer_id", "amount"]
    )

    print(" Transactions Dataframe ")

    transactions_df.show()


    # Customer reference data
    customers_df = spark.createDataFrame(
        [
            ("C101", "Alice"),
            ("C102", "Bob"),
            ("C103", "John"),
        ],
        ["customer_id", "customer_name"]
    )

    print(" Customers Dataframe ")

    customers_df.show()

    HOT_KEY = "C101"
    NUM_BUCKETS = 10

    # STEP 1: Salt hot-key transactions

    # xxhash64() - produces a deterministic hash (ex. 2969245061977936502) from the transaction ID.
    # pmod()     - positive modulo. With a positive divisor of 10, gives us a bucket number between 0 and 9, even for negative hashes.

    transactions_salted = (
        transactions_df
        .withColumn(
            "salt",
            F.when(
                F.col("customer_id") == HOT_KEY,                                        # if customer_id = c101
                F.pmod( F.xxhash64("transaction_id"), F.lit(NUM_BUCKETS)).cast("int")   # pmod(2969245061977936502 , 10) gets a positive modulo 2
            ).otherwise(F.lit(0))                                                       # else 0
        )
    )


    # STEP 2: Replicate hot customer reference
    #       sequence() creates the bucket numbers, and
    #       explode() converts the array into separate rows.
    customers_salted = (
        customers_df
        .withColumn(
            "salt_array",
            F.when(
                F.col("customer_id") == HOT_KEY,
                F.sequence(F.lit(0), F.lit(NUM_BUCKETS - 1))
            ).otherwise(F.array(F.lit(0)))
        )
    )

    print("Before Explode")
    customers_salted.show(10, False)

    customers_salted_2 = (
        customers_salted
        .withColumn("salt", F.explode("salt_array"))
        .drop("salt_array")
                          )

    print("After Explode")
    customers_salted_2.show(10, False)

    # STEP 3: Join using customer_id + salt
    result_df = (
        transactions_salted
        .join(
            customers_salted_2,
            on=["customer_id", "salt"],
            how="left"
        )
        .drop("salt")
    )

    # Inspect results
    print("Salted transactions")
    transactions_salted.show()

    print("Replicated customer reference")
    customers_salted_2.orderBy("customer_id", "salt").show()

    print("Final joined output")
    result_df.orderBy("transaction_id").show()

    # STEP 4: Validate record counts
    source_count = transactions_df.count()
    target_count = result_df.count()

    print("Source count:", source_count)
    print("Target count:", target_count)

    assert source_count == target_count