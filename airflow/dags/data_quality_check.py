from airflow import DAG
from datetime import timedelta, datetime
from data_quality.expectations import validate_transactions
from slack_sdk import WebClient
from pyspark.sql import SparkSession
from airflow.operators.python import PythonOperator

import os

def run_quality_checks():
    #spark_session
    spark = SparkSession.builder \
        .appName("data_quality_check") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"]) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"]) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    df = spark.table("local.bronze.transactions")
    result = validate_transactions(df)

    if result['critical_failures']:
        client = WebClient(token=os.environ['SLACK_BOT_TOKEN'])
        client.chat_postMessage(
            channel='#data-alerts',
            text=f"CRITICAL: Data quality failures: {result['critical_failures']}"
        )
        raise Exception(f'Critical data quality failures: {result["critical_failures"]}')
    if result['soft_failures']:
        print(result['soft_failures'])

    spark.stop()

with DAG(
    dag_id='data_quality_check',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:
    task = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_quality_checks,
    )