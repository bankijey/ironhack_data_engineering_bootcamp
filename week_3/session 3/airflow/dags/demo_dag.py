
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime

def hello():
    print("Airflow is working!")

with DAG(
    dag_id="demo_pipeline",
    start_date=datetime(2024, 1, 1),
    # schedule_interval="@daily",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=hello
    )

    task1
    