from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Bronze_Orders").getOrCreate()
    df_orders = spark.read.json("/opt/airflow/dags/1_landing/orders/*")
    df_orders.write.mode("overwrite").parquet("/opt/airflow/data/2_bronze/orders")
    spark.stop()

if __name__ == "__main__":
    main()