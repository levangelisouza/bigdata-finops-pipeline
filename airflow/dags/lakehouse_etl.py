from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG('lakehouse_etl',
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         description='Orquestra ETL Bronze → Silver → Gold com Spark e Airflow') as dag:

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='docker exec spark-local spark-submit /opt/spark_app/silver/transform_bronze_to_silver.py'
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='docker exec spark-local spark-submit /opt/spark_app/gold/aggregate_silver_to_gold.py'
    )

    bronze_to_silver >> silver_to_gold

