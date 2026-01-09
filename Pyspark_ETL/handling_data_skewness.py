from pyspark.sql.functions import col,concat_ws,lit,rand,floor

def HandlingDataSkewness(spark):
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
                       .withColumn("salt", floor(rand() * num_salt_keys))
                       #1. - Generates a random number between 0.0 and 1.0 `rand()`
                       #2. `floor()` - Rounds down to the nearest integer
                       #3. - In this code, it's set to 3, so it will generate values 0, 1, or 2 `num_salt_keys`
                       .withColumn("salted_store_id", concat_ws("_", col("store_id"), col("salt"))))

    print(" salted sales dataframe ")
    sales_df_salted.show()

    expanded_store_df = (store_df
                         .crossJoin(spark.range(0, num_salt_keys).toDF("salt"))
                         .withColumn("salted_store_id", concat_ws("_",col("store_id"), col("salt"))))

    print(" expanded dataframe ")
    expanded_store_df.show()

    joined_df = sales_df_salted.join(expanded_store_df, "salted_store_id", "inner").drop("salted_store_id", "salt")

    joined_df.show()
