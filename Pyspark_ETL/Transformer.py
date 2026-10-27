from pyspark.sql import functions as F
from pyspark.sql.window import Window

def transform_employee_data(df):
    # Example transformation: Calculate bonus (10% of salary)
    df_transformed = df.withColumn("bonus", df["salary"] * 0.1)

    # Filter employees with salary greater than 70,000
    df_transformed = df_transformed.filter(df["salary"] > 70000)

    return df_transformed

def transform_customer_data(df):
    # Example transformation: Fill missing purchase_amount with average purchase amount
    avg_purchase = df.agg(F.avg("purchase_amount")).collect()[0][0]
    df_transformed = df.fillna({"purchase_amount": avg_purchase})

    # Example transformation: Add a new column 'VIP' based on purchase amount
    df_transformed = df_transformed.withColumn("VIP",
                                               F.when(df_transformed["purchase_amount"] > 1500, "Yes").otherwise("No"))

    return df_transformed

def transform_student_data(df):
    # Example transformation: Classify students based on score
    df_transformed = df.withColumn("class",
                                   F.when(df["score"] >= 90, "A").when(df["score"] >= 75, "B").otherwise("C"))

    return df_transformed

def transform_transaction_data(df):
    # Example transformation: Convert TransactionDate to Date type
    df_transformed = df.withColumn("TransactionDate",
                                   F.to_date(df["TransactionDate"], "yyyy-MM-dd"))

    # Calculate the total amount of transactions per UserID
    df_transformed = df_transformed.groupBy("UserID").agg(F.sum("TransactionAmount").alias("TotalSpent"))

    return df_transformed

def transform_measurement_data(df):
    # Example transformation: Filter measurements taken after July 11, 2022
    df_transformed = df.filter(F.col("measurement_time") > "2022-07-11 00:00:00")

    # Example transformation: Add row number based on measurement value descending
    window_spec = Window.orderBy(F.col("measurement_value").desc())
    df_transformed = df_transformed.withColumn("row_num", F.row_number().over(window_spec))

    return df_transformed

def transform_ECT_data(df):
    # a. Replace all NULL values in the Quantity column with 0.
    # b. Replace all NULL values in the Price column with the average price of the existing data.
    # c. Drop rows where the Product column is NULL.
    # d. Fill the missing Sales_Date with a default value of '2025-01-01'.
    # e. Drop rows where all columns are NULL.

    # a. Replace all NULL values in the Quantity column with 0.
    df = df.fillna({'Quantity': 0})

    # b. Replace all NULL values in the Price column with the average price of the existing data.
    mean_value = df.select(F.avg(F.col("Price"))).first()[0]
    df = df.na.fill({"Price": mean_value})

    # c. Drop rows where the Product column is NULL.
    df = df.filter(df.Product.isNotNull())

    # d. Fill the missing Sales_Date with a default value of '2025-01-01'.
    df = df.fillna({'Sales_Date': '2025-01-01'})

    # e. Drop rows where all columns are NULL.
    df = df.dropna(how='all')

    return df


# Keep only COMPLETED transactions with amount > 0.
# Remove duplicate transaction_id values, retaining the row with the latest updated_at.
# Select transaction_id, customer_id, amount, and updated_at.
# Write the result in Parquet format to gs://curated-bucket/transactions/ using overwrite mode.

def transform_transactions_data(df):

    df = df.withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
           .withColumn("updated_at", F.col("updated_at").cast("timestamp"))

    filtered_df = df.filter( (F.col("amount") > 0) &
                             (F.col("status") == "COMPLETED")
                            )

    window_spec = Window.partitionBy("transaction_id").orderBy(F.col("updated_at").desc())

    curated_df = filtered_df.withColumn("row_num", F.row_number().over(window_spec)) \
                                .filter(F.col("row_num") == 1) \
                                .select("transaction_id","customer_id", "amount", "updated_at")

    curated_df.show(10, False)

    return curated_df

# You are working on a retail analytics pipeline.
# Your business team wants to find the top two customers in each region based on total completed transaction amount.

def get_top_two_customers_by_region(df):


    filtered_df = df.filter(F.col("status") == "COMPLETED")

    window_spec = Window.partitionBy("region").orderBy(
        F.col("total_amount").desc(),
              F.col("customer_id").asc()   # secondary condition, if two customers has same amount, assigns rank as per id
    )

    grouped_df = filtered_df.groupBy(F.col("customer_id"), F.col("region")) \
                            .agg(F.sum(F.col("amount")).alias("total_amount")) \
                            .withColumn("row_num", F.row_number().over(window_spec)) \
                            .filter(F.col("row_num") <= 2) \

    return grouped_df

def find_missing_transactions(source_df, target_df):

    missing_df = source_df.join(
        target_df.select("transaction_id"),
        on="transaction_id",
        how="left_anti"
    )

    return missing_df


def apply_transformations(dataframes):

    transformed_data = {}

    # Apply transformations to each DataFrame
    transformed_data["employee"] = transform_employee_data(dataframes["employee"])
    transformed_data["customer"] = transform_customer_data(dataframes["customer"])
    transformed_data["student"] = transform_student_data(dataframes["student"])
    transformed_data["transaction"] = transform_transaction_data(dataframes["transaction"])
    transformed_data["measurement"] = transform_measurement_data(dataframes["measurement"])
    transformed_data["ECT"] = transform_ECT_data(dataframes["ECT"])

    return transformed_data
