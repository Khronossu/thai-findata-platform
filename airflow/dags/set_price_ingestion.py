from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
from producers.set_producer import run

with DAG(
    dag_id='set_price_ingestion',
    schedule_interval='0 18 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    timezone= "Asia/Bangkok",
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:
    task = PythonOperator(
        task_id='fetch_and_publish',
        python_callable=run,
    )