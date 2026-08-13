from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Silver_Customers").getOrCreate()

    # 1. Leitura da Bronze com caminho absoluto
    df_bronze = spark.read.parquet("/opt/airflow/data/2_bronze/customers")

    # 2. Remoção do prefixo 'customer_' das colunas
    df_silver = df_bronze
    for col_name in df_bronze.columns:
        if col_name.startswith("customer_"):
            new_col_name = col_name.replace("customer_", "", 1)
            df_silver = df_silver.withColumnRenamed(col_name, new_col_name)

    # 3. Escrita na Silver com caminho absoluto
    df_silver.write.mode("overwrite").parquet("/opt/airflow/data/3_silver/customers")

    spark.stop()


if __name__ == "__main__":
    main()