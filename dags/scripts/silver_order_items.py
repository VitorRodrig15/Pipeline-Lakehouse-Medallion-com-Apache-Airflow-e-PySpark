from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Silver_Order_Items").getOrCreate()

    # 1. Leitura da Bronze com caminho absoluto
    df_bronze = spark.read.parquet("/opt/airflow/data/2_bronze/order_items")

    # 2. Remoção dos prefixos das colunas
    df_silver = df_bronze
    for col_name in df_bronze.columns:
        if col_name.startswith("order_item_"):
            new_col_name = col_name.replace("order_item_", "", 1)
            df_silver = df_silver.withColumnRenamed(col_name, new_col_name)
        elif col_name.startswith("order_"):
            new_col_name = col_name.replace("order_", "", 1)
            df_silver = df_silver.withColumnRenamed(col_name, new_col_name)

    # 3. Escrita na Silver com caminho absoluto
    df_silver.write.mode("overwrite").parquet("/opt/airflow/data/3_silver/order_items")

    spark.stop()


if __name__ == "__main__":
    main()