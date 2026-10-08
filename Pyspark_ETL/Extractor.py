from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def read_csv(filepath, spark: SparkSession):

    return spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .csv(filepath)

def read_mysql_table(spark: SparkSession, table_name: str):
    return (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/SCD_Type2")
        .option("dbtable", table_name)
        .option("user", "root")
        .option("password", "root")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )

def extract_data(spark: SparkSession):

    # Sample data for various datasets
    data_employee = [
        (101, "Rajesh", "IT", 75000, "Bangalore"),
        (102, "Priya", "HR", 60000, "Mumbai"),
        (103, "Anil", "IT", 80000, "Hyderabad"),
        (104, "Sneha", "HR", 62000, "Pune"),
        (105, "Manish", "Finance", 90000, "Chennai"),
        (106, "Suresh", "IT", 78000, "Bangalore")
    ]

    data_customer = [
        (201, "Aman", "Delhi", 1500, "Electronics"),
        (202, "Kiran", "Mumbai", None, "Fashion"),
        (203, "Ravi", "Bangalore", 2000, "Electronics"),
        (204, "Simran", "Hyderabad", None, "Fashion"),
        (205, "Vinay", "Pune", 1800, "Electronics"),
        (206, "Pooja", "Chennai", 1300, "Grocery"),
        (206, "Pooja", "Chennai", 2000, "Fashion")
    ]

    data_student = [
        (301, "Rohit", "Maharashtra", 85),
        (302, "Sneha", "Karnataka", 92),
        (303, "Amit", "Maharashtra", 90),
        (304, "Kunal", "Karnataka", 88),
        (305, "Nidhi", "Maharashtra", 78),
        (306, "Pavan", "Karnataka", 80)
    ]

    data_transaction = [
        ("TXN1", "U1", "PayPal", 100.00, "2024-01-10"),
        ("TXN2", "U2", "CreditCard", 250.00, "2024-02-15"),
        ("TXN3", "U3", "PayPal", 150.00, "2024-03-20"),
        ("TXN4", "U4", "ApplePay", 200.00, "2023-12-25"),
        ("TXN5", "U5", "PayPal", 50.00, "2024-05-05"),
        ("TXN6", "U5", "PayPal", 70.00, "2023-05-05")
    ]

    data_measurement = [
        (131233, 1109.51, '2022-07-10 09:00:00'),
        (135211, 1662.74, '2022-07-10 11:00:00'),
        (143562, 1124.50, '2022-07-11 13:15:00'),
        (346462, 1234.14, '2022-07-11 15:00:00'),
        (124245, 1252.62, '2022-07-11 16:45:00'),
        (523542, 1246.24, '2022-07-10 14:30:00'),
        (143251, 1246.56, '2022-07-11 18:00:00'),
        (141565, 1452.40, '2022-07-12 08:00:00'),
        (253622, 1244.30, '2022-07-12 14:00:00'),
        (353625, 1451.00, '2022-07-12 15:00:00')
    ]

    ElectronicsData = [
        (1, "Laptop", 10, 50000, "North", "2025-01-01"),
        (2, "Mobile", None, 15000, "South", None),
        (3, "Tablet", 20, None, "West", "2025-01-03"),
        (4, "Desktop", 15, 30000, None, "2025-01-04"),
        (5, None, None, None, "East", "2025-01-05") ]

    # Define column names
    employee_columns = ["emp_id", "name", "department", "salary", "city"]
    customer_columns = ["cust_id", "cust_name", "city", "purchase_amount", "product_category"]
    student_columns = ["student_id", "student_name", "state", "score"]
    transaction_columns = ["TransactionID", "UserID", "PaymentMethod", "TransactionAmount", "TransactionDate"]
    measurement_columns = ['measurement_id', 'measurement_value', 'measurement_time']
    ECT_columns = ["Sales_ID", "Product", "Quantity", "Price", "Region", "Sales_Date"]

    # Convert data to DataFrame
    df_employee = spark.createDataFrame(data_employee, employee_columns)
    df_customer = spark.createDataFrame(data_customer, customer_columns)
    df_student = spark.createDataFrame(data_student, student_columns)
    df_transaction = spark.createDataFrame(data_transaction, transaction_columns)
    df_measurement = spark.createDataFrame(data_measurement, measurement_columns)
    df_ECT = spark.createDataFrame(ElectronicsData, ECT_columns)

    # Return all DataFrames in a dictionary for easy access
    return {
        "employee": df_employee,
        "customer": df_customer,
        "student": df_student,
        "transaction": df_transaction,
        "measurement": df_measurement,
        "ECT": df_ECT
    }