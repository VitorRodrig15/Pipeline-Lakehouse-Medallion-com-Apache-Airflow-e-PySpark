from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

def processar_dados():
    spark = SparkSession.builder \
        .appName("TesteLocalPySpark") \
        .master("local[*]") \
        .getOrCreate()

    # Criando um DataFrame de teste
    dados = [("Vendas", 1000), ("TI", 2500), ("Marketing", 1500)]
    df = spark.createDataFrame(dados, ["Area", "Orcamento"])
    
    print("--- Resultado do DataFrame ---")
    df.show()
    
    spark.stop()

with DAG(
    dag_id="pyspark_local_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    tarefa_pyspark = PythonOperator(
        task_id="executar_pyspark",
        python_callable=processar_dados,
    )