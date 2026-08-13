from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'engenharia_dados',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='pipeline_lakehouse_medallion',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # --- CAMADA BRONZE (3 Tarefas) ---
    bronze_customers = BashOperator(
        task_id='bronze_customers',
        bash_command='python3 /opt/airflow/dags/scripts/bronze_customers.py'
    )

    bronze_orders = BashOperator(
        task_id='bronze_orders',
        bash_command='python3 /opt/airflow/dags/scripts/bronze_orders.py'
    )

    bronze_order_items = BashOperator(
        task_id='bronze_order_items',
        bash_command='python3 /opt/airflow/dags/scripts/bronze_order_items.py'
    )

    # --- CAMADA SILVER (3 Tarefas) ---
    silver_customers = BashOperator(
        task_id='silver_customers',
        bash_command='python3 /opt/airflow/dags/scripts/silver_customers.py'
    )

    silver_orders = BashOperator(
        task_id='silver_orders',
        bash_command='python3 /opt/airflow/dags/scripts/silver_orders.py'
    )

    silver_order_items = BashOperator(
        task_id='silver_order_items',
        bash_command='python3 /opt/airflow/dags/scripts/silver_order_items.py'
    )

    # --- CAMADA GOLD (1 Tarefa) ---
    gold_sales_summary = BashOperator(
        task_id='gold_sales_summary',
        bash_command='python3 /opt/airflow/dags/scripts/gold_sales_summary.py'
    )

    # --- FLUXO E PARALELISMO ---
    # Cada Silver depende apenas do seu respectivo Bronze
    bronze_customers >> silver_customers
    bronze_orders >> silver_orders
    bronze_order_items >> silver_order_items

    # A Gold só roda quando todas as Silvers terminarem
    [silver_customers, silver_orders, silver_order_items] >> gold_sales_summary