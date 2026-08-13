from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Inspect_Gold").getOrCreate()
    df = spark.read.parquet("/opt/airflow/data/4_gold/sales_summary")

    print("--- Schema ---")
    df.printSchema()
    print("--- Rows ---")
    df.show(50, False)

    spark.stop()


if __name__ == "__main__":
    main()
