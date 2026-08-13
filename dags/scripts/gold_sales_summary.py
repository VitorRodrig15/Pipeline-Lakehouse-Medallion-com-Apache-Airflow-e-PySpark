from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum, round

def main():
    spark = SparkSession.builder.appName("Gold_Sales_Summary").getOrCreate()

    # 1. Leitura da camada Silver com caminhos absolutos
    df_customers = spark.read.parquet("/opt/airflow/data/3_silver/customers")
    df_orders = spark.read.parquet("/opt/airflow/data/3_silver/orders")
    df_items = spark.read.parquet("/opt/airflow/data/3_silver/order_items")

    # 2. Cruzamento dos dados (Joins)
    df_joined = df_orders \
        .join(df_customers, df_orders["customer_id"] == df_customers["id"], "inner") \
        .join(df_items, df_orders["id"] == df_items["order_id"], "inner")

    # 3. Agregação solicitada no exercício (city, state, quantidade_pedidos e valor_total_pedidos)
    # Usar a coluna `subtotal` de `df_items` (cada item já tem subtotal=product_price*quantity)
    df_gold = df_joined.groupBy("city", "state") \
        .agg(
            countDistinct(df_orders["id"]).alias("quantidade_pedidos"),
            round(sum(df_items["subtotal"]), 2).alias("valor_total_pedidos")
        )

    # 4. Escrita na camada Gold com caminho absoluto
    df_gold.write.mode("overwrite").parquet("/opt/airflow/data/4_gold/sales_summary")

    spark.stop()


if __name__ == "__main__":
    main()