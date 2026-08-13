from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Bronze_Customers").getOrCreate()
    df_customers = spark.read.json("/opt/airflow/dags/1_landing/customers/*")
    df_customers.write.mode("overwrite").parquet("/opt/airflow/data/2_bronze/customers")
    spark.stop()

if __name__ == "__main__":
    main()