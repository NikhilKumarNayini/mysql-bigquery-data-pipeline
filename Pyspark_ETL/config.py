import pyspark
PROJECT_ID = "nyc-311-scd-pipeline"
DATASET = "spark_interview_practice"

MYSQL_HOST     = "127.0.0.1"
MYSQL_PORT     = "3306"
MYSQL_DATABASE = "spark_practice"
DRIVER         = "com.mysql.cj.jdbc.Driver"
DB_TABLE       = "source_transactions"
BQ_TABLE       = "target_transactions"


spark_version = ".".join(pyspark.__version__.split(".")[:2])

bigquery_package = (
    f"com.google.cloud.spark:"
    f"spark-{spark_version}-bigquery:0.46.0"
)

mysql_package = "com.mysql:mysql-connector-j:9.7.0"