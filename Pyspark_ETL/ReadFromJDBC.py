
def read_from_jdbc(spark):
    """Reads and writes data to MySQL using the provided Spark session."""
    url = "jdbc:mysql://localhost:3306/employees"

    properties = {
        "user": "root",
        "password": "NewPassword",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        # Read data from MySQL
        df = spark.read.jdbc(url=url, table="employees", properties=properties)
        df.show()

        # Write data to MySQL
        df.write.jdbc(url=url, table="table_name", mode="overwrite", properties=properties)

        print("✅ Data successfully read from and written to MySQL.")

    except Exception as e:
        print(f"❌ Error occurred: {e}")

