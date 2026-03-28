import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from pyspark.sql import SparkSession

def run_compaction():
    spark = SparkSession.builder.appName("IcebergCompaction") \
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
    spark.sql("""
              CALL local.system.rewrite_data_files(
              table => 'local.bronze.transactions',
              strategy => 'binpack',
              options => map('target-file-size-bytes', '134217728'))
              """)
    spark.stop()

    
with DAG(
    dag_id='iceberg_compaction',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:
    task = PythonOperator(
        task_id='run_compaction',
        python_callable=run_compaction,
    )