
import pyspark

import Extractor, Transformer

from pyspark.sql import SparkSession
from Pyspark_ETL import config


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MySQL JDBC Example") \
        .config(
        "spark.jars.packages",
        f"{config.mysql_package},{config.bigquery_package}") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Read MySQL source
        source_df = Extractor.get_mysql_data(spark)

        print("MYSQL SOURCE DATA")
        source_df.show(truncate=False)

        # Read BigQuery target
        target_df = Extractor.get_target_data(spark)

        print("BIGQUERY TARGET DATA")
        target_df.show(truncate=False)

        # Perform reconciliation
        missing_df = Transformer.find_missing_transactions(
            source_df,
            target_df
        )

        print("MISSING TRANSACTIONS")
        missing_df.show(truncate=False)

    finally:
        spark.stop()

    spark.stop()
    print("✅ Spark session stopped successfully.")

if __name__ == "__main__":
    main()

