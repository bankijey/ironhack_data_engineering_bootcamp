from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import os
import pandas as pd

PROCESSED_FILE = "/opt/airflow/data/processed/cleaned_sales.csv"
ALERT_EMAIL_TO = os.environ["ALERT_EMAIL_TO"]

default_args = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def check_sales(**context):

    df = pd.read_csv(PROCESSED_FILE)

    total_sales = df["Total_Sales"].sum()

    print(f"Total Sales = {total_sales}")

    if total_sales < 100000:
        context["ti"].xcom_push(
            key="alert_message",
            value=f"ALERT: Sales dropped to {total_sales}"
        )

with DAG(
    dag_id="retail_alert_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    sales_check_task = PythonOperator(
        task_id="check_sales",
        python_callable=check_sales
    )

    send_email = EmailOperator(
        task_id="send_email_alert",
        to=ALERT_EMAIL_TO,
        subject="Retail Sales Alert",
        html_content="""
        <h3>Sales Alert Triggered</h3>
        <p>Please check today's sales report.</p>
        """,
        retries=3,
    )

    sales_check_task >> send_email