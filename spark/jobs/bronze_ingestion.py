"""
Stage 3 — Bronze Ingestion
Reads PromptPay transactions from Kafka, validates, applies PII masking,
and writes to Iceberg bronze tables.
Invalid records go to quarantine — never silently dropped.
"""
import os
import uuid
import hashlib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
)

# ── SPARK SESSION ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ThaiPlatformBronzeIngestion") \
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

# ── CONSTANTS ────────────────────────────────────────────────────
PIPELINE_RUN_ID = str(uuid.uuid4())
SALT = os.environ["PII_SALT"]
SOURCE_TOPIC = "promptpay_transactions"

# ── KAFKA MESSAGE SCHEMA ─────────────────────────────────────────
PROMPTPAY_SCHEMA = StructType([
    StructField("transaction_id",     StringType(),  True),
    StructField("sender_national_id", StringType(),  True),
    StructField("sender_account",     StringType(),  True),
    StructField("receiver_account",   StringType(),  True),
    StructField("amount_thb",         DoubleType(),  True),
    StructField("merchant_category",  StringType(),  True),
    StructField("channel",            StringType(),  True),
    StructField("location_lat",       DoubleType(),  True),
    StructField("location_lng",       DoubleType(),  True),
    StructField("event_timestamp",    StringType(),  True),
    StructField("is_anomaly",         BooleanType(), True),
    StructField("anomaly_type",       StringType(),  True),
])

# ── PII MASKING UDFs ─────────────────────────────────────────────
# Applied before Bronze write — unmasked PII never reaches the data lake.

@F.udf(StringType())
def mask_national_id(national_id: str) -> str:
    if national_id is None:
        return None
    return hashlib.sha256(f"{SALT}{national_id}".encode()).hexdigest()

@F.udf(StringType())
def mask_account(account: str) -> str:
    if account is None:
        return None
    return f"****{account[-4:]}"

# ── TABLE SETUP ──────────────────────────────────────────────────
def create_tables_if_not_exist() -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.transactions (
            transaction_id           STRING    NOT NULL,
            sender_national_id_hash  STRING    NOT NULL,
            sender_account_masked    STRING    NOT NULL,
            receiver_account_masked  STRING    NOT NULL,
            amount_thb               DOUBLE    NOT NULL,
            merchant_category        STRING    NOT NULL,
            channel                  STRING    NOT NULL,
            location_lat             DOUBLE,
            location_lng             DOUBLE,
            event_timestamp          TIMESTAMP NOT NULL,
            is_anomaly               BOOLEAN   NOT NULL,
            anomaly_type             STRING,
            ingestion_timestamp      TIMESTAMP NOT NULL,
            pipeline_run_id          STRING    NOT NULL,
            is_pii_masked            BOOLEAN   NOT NULL,
            event_date               DATE      NOT NULL
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.target-file-size-bytes'    = '134217728'
        )
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.quarantine (
            original_record   STRING    NOT NULL,
            rejection_reason  STRING    NOT NULL,
            rejected_at       TIMESTAMP NOT NULL,
            pipeline_run_id   STRING    NOT NULL,
            source_topic      STRING    NOT NULL
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

# ── BATCH PROCESSOR ──────────────────────────────────────────────
def process_batch(batch_df, epoch_id) -> None:
    if batch_df.isEmpty():
        return

    # Parse raw Kafka bytes → JSON struct
    parsed = batch_df.select(
        F.col("value").cast(StringType()).alias("raw_json"),
        F.from_json(
            F.col("value").cast(StringType()), PROMPTPAY_SCHEMA
        ).alias("d"),
    )

    # Validate — first failing rule wins, written to quarantine
    validated = parsed.withColumn(
        "rejection_reason",
        F.when(F.col("d.transaction_id").isNull(),     "missing transaction_id")
         .when(F.col("d.sender_national_id").isNull(), "missing sender_national_id")
         .when(F.col("d.sender_account").isNull(),     "missing sender_account")
         .when(F.col("d.receiver_account").isNull(),   "missing receiver_account")
         .when(F.col("d.amount_thb").isNull(),         "missing amount_thb")
         .when(F.col("d.amount_thb") <= 0,             "amount_thb must be positive")
         .when(F.col("d.merchant_category").isNull(),  "missing merchant_category")
         .when(F.col("d.channel").isNull(),            "missing channel")
         .when(F.col("d.event_timestamp").isNull(),    "missing event_timestamp")
         .when(F.col("d.is_anomaly").isNull(),         "missing is_anomaly")
         .otherwise(None)
    )

    valid_df   = validated.filter(F.col("rejection_reason").isNull()).cache()
    invalid_df = validated.filter(F.col("rejection_reason").isNotNull())

    # Quarantine — never silently drop
    if not invalid_df.isEmpty():
        invalid_df.select(
            F.col("raw_json").alias("original_record"),
            F.col("rejection_reason"),
            F.current_timestamp().alias("rejected_at"),
            F.lit(PIPELINE_RUN_ID).alias("pipeline_run_id"),
            F.lit(SOURCE_TOPIC).alias("source_topic"),
        ).writeTo("local.bronze.quarantine").append()

    # Mask PII and write Bronze
    if not valid_df.isEmpty():
        event_ts = F.to_timestamp(F.col("d.event_timestamp"))

        valid_df.select(
            F.col("d.transaction_id"),
            mask_national_id(F.col("d.sender_national_id")).alias("sender_national_id_hash"),
            mask_account(F.col("d.sender_account")).alias("sender_account_masked"),
            mask_account(F.col("d.receiver_account")).alias("receiver_account_masked"),
            F.col("d.amount_thb"),
            F.col("d.merchant_category"),
            F.col("d.channel"),
            F.col("d.location_lat"),
            F.col("d.location_lng"),
            event_ts.alias("event_timestamp"),
            F.col("d.is_anomaly"),
            F.col("d.anomaly_type"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.lit(PIPELINE_RUN_ID).alias("pipeline_run_id"),
            F.lit(True).alias("is_pii_masked"),
            F.to_date(event_ts).alias("event_date"),
        ).writeTo("local.bronze.transactions").append()

    valid_df.unpersist()

# ── STREAMING READ ───────────────────────────────────────────────
def build_stream():
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("subscribe", SOURCE_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Pre-parse event_timestamp so watermark can be applied on the stream
    df_parsed = df_raw.select(
        F.col("value"),
        F.to_timestamp(
            F.get_json_object(F.col("value").cast(StringType()), "$.event_timestamp")
        ).alias("event_timestamp"),
    )

    # 10-min watermark — deliberate tradeoff for fraud detection latency
    df_watermarked = df_parsed.withWatermark("event_timestamp", "10 minutes")

    return df_watermarked.writeStream \
        .outputMode("append") \
        .option("checkpointLocation",
                "s3a://warehouse/checkpoints/bronze/transactions/") \
        .trigger(processingTime="30 seconds") \
        .foreachBatch(process_batch) \
        .start()

# ── ENTRY POINT ──────────────────────────────────────────────────
if __name__ == "__main__":
    create_tables_if_not_exist()
    query = build_stream()
    print(f"Bronze ingestion started | pipeline_run_id={PIPELINE_RUN_ID}")
    query.awaitTermination()
