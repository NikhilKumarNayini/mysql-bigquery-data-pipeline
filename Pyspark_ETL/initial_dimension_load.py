from datetime import date

from pyspark.sql.functions import (
    lit,
    current_date,
    row_number
)

from pyspark.sql.window import Window


def create_initial_dimension_load(source_df):

    window_spec = Window.orderBy("product_id")

    initial_dim_df = source_df \
        .withColumn("product_sk", row_number().over(window_spec)) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit(date(9999, 12, 31))) \
        .withColumn("current", lit("Y")) \
        .select( "product_sk",
                 "product_id",
                 "product_name",
                 "category",
                 "price",
                 "start_date",
                 "end_date",
                 "current")

    return (
        initial_dim_df.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/SCD_Type2")
        .option("dbtable", "dim_products")
        .option("user", "root")
        .option("password", "root")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("overwrite")
        .save()
    )