import scd
import Extractor
import initial_dimension_load

from pyspark.sql import SparkSession



def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MySQL JDBC Example") \
        .config("spark.jars", "/Users/nikkinani/Downloads/mysql-connector-j-8.2.0/mysql-connector-j-8.2.0.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    #extracted source data
    source_path = "/Users/nikkinani/Downloads/source_products.csv"

    source_df = Extractor.read_csv(source_path, spark)

    print("SOURCE DATA", source_df.show(truncate=False))

    # 2. Create initial dimension
    initial_dim_df = initial_dimension_load.create_initial_dimension_load(
        source_df
    )

    print("INITIAL DIMENSION")
    initial_dim_df.show(truncate=False)

    # 3. Load dimension into MySQL
    initial_dimension_load.write_initial_dimension_to_mysql(
        initial_dim_df
    )

    spark.stop()



    final_df = scd.apply(source_df, target_df)
    #
    #
    # print("FINAL SCD TYPE 2 DATA")
    # print("Target Data count: ", final_df.count())
    # final_df.show(truncate=False)
    #
    # final_df.write \
    #     .mode("overwrite") \
    #     .csv("/Users/nikkinani/Downloads/target_products.csv")
    #
    # print("Final SCD TYPE 2 DATA Validation")
    # final_df \
    #     .filter(col("current") == "Y") \
    #     .groupBy("product_id") \
    #     .agg(count("*").alias("current_count")) \
    #     .filter(col("current_count") > 1) \
    #     .show()
    #
    #

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

    # from interviewPrep import total_sales_per_product
    # total_sales_per_product(spark)


    # Stop Spark when the program ends
    spark.stop()
    print("✅ Spark session stopped successfully.")

if __name__ == "__main__":
    main()

