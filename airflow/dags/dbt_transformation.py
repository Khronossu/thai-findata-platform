from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='dbt_transform',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:
    t_run_silver = BashOperator(
        task_id='run_silver',
        bash_command='dbt run --select staging.*'
    )
    t_run_gold = BashOperator(
        task_id='run_gold',
        bash_command='dbt run --select gold.*',
        sla=timedelta(minutes=15)
    )

    t_run_silver >> t_run_gold