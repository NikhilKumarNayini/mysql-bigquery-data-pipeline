from pyspark.sql import SparkSession
from Extractor import extract_data

from Transformer import apply_transformations
from Loader import load_data

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MySQL JDBC Example") \
        .config("spark.jars", "/Users/nikkinani/Downloads/mysql-connector-j-8.2.0/mysql-connector-j-8.2.0.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Extract sample data
    # extracted_data = extract_data(spark)
    #
    # # Transform data
    # transformed_data = apply_transformations(extracted_data)
    #
    # # Step 4: Show transformed data
    # for name, df in transformed_data.items():
    #     print(f"\n====== Transformed {name} DataFrame ======")
    #     df.show(truncate=False)

    # from handling_data_skewness import HandlingDataSkewness
    # HandlingDataSkewness(spark)

    from interviewPrep import total_sales_per_product
    total_sales_per_product(spark)

    # Stop Spark when the program ends
    spark.stop()
    print("✅ Spark session stopped successfully.")

if __name__ == "__main__":
    main()

