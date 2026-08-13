from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Bronze_Order_Items").getOrCreate()
    df_order_items = spark.read.json("/opt/airflow/dags/1_landing/order_items/*")
    df_order_items.write.mode("overwrite").parquet("/opt/airflow/data/2_bronze/order_items")
    spark.stop()

if __name__ == "__main__":
    main()