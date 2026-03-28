from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from slack_sdk import WebClient
import os

def check_health():
    spark = SparkSession.builder.appName("StreamingMonitor") \
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

    result = spark.sql("""
                    SELECT MAX(ingestion_timestamp) as last_ingestion
                    FROM local.bronze.transactions
                    """).collect()[0]
    last_ingestion = result['last_ingestion']
    minute_since = (datetime.now(timezone.utc) - last_ingestion.replace(tzinfo=timezone.utc)).total_seconds() / 60

    if minute_since > 10:
        client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
        client.chat_postMessage(
            channel='#data-alerts',
            text=f"ALERT: Bronze ingestion inactive for {minute_since:.0f} minutes"
        )
    spark.stop()

with DAG(
    dag_id='streaming_monitor',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:
    task = PythonOperator(
    task_id='streaming_monitor',
    python_callable=check_health
    )