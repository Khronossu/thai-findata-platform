from slack_sdk import WebClient
import os
from datetime import datetime, timezone
import statistics
from pyspark.sql import SparkSession

def compute_baseline(historical_values: list[float]) -> tuple[float, float]:
    return statistics.mean(historical_values), statistics.stdev(historical_values)

def is_anomalous(current_value: float, mean: float, std: float) -> tuple[bool, float]:
    deviation_in_sigma = (current_value - mean) / std
    return abs(deviation_in_sigma) > 2, deviation_in_sigma

def send_slack_alert(table: str, metric: str, current: float, mean: float, std: float, deviation: float, last_7: list[float], action: str) -> None:
    client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    alert = {
        "table": table,
        "metric": metric,
        "current_value": current,
        "expected_range": f'{mean - 2*std:.0f}-{mean + 2*std:.0f}',
        "deviation_sigma": round(deviation, 1),
        "last_7_days": last_7,
        "suggested_action": action,
        "dashboard_url": "http://localhost:3000/d/thai-platform" 
    }
    client.chat_postMessage(
        channel='#data-alerts',
        text=str(alert)
    )

def run_checks():
    spark = SparkSession.builder.appName("anomaly_detector") \
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
    
    today_dow = datetime.now(timezone.utc).weekday()

    history = spark.sql(f"""
                        SELECT DATE(event_timestamp) as dt, COUNT(*) as row_count
                        FROM local.gold.fct_transactions
                        WHERE DAYOFWEEK(event_timestamp) = {today_dow + 1}
                        GROUP BY DATE(event_timestamp)
                        ORDER BY dt DESC
                        LIMIT 14
                        """).collect()
    historical_value = [row['row_count'] for row in history]
    current = historical_value[0]
    baseline_values = historical_value[1:]

    mean, std = compute_baseline(baseline_values)
    anomalous, deviation = is_anomalous(current, mean, std)
    
    if anomalous:
        send_slack_alert(
            table="gold.fct_transactions",
            metric="row_count",
            current=current,
            mean=mean,
            std=std,
            deviation=deviation,
            last_7=historical_value[:7],
            action='Check AIRFLOW DAG dbt_transaction - last run status'
        )
    spark.stop()

if __name__ == "__main__": run_checks()