import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master('local').appName('test').getOrCreate()

def test_deduplication(spark):
    data = [
        ("txn_001", 500.0, '2026-03-31 10:00:00'),
        ("txn_001", 500.0, "2026-03-31 10:01:00"),
        ("txn_002", 200.0, "2026-03-31 10:00:00"),
    ]
    df = spark.createDataFrame(data, ['transaction_id', 'amount_thb', 'ingestion_timestamp'])

    #apply deduplication - same logic as stg_transaction.sql
    window = Window.partitionBy('transaction_id').orderBy(F.desc('ingestion_timestamp'))
    deduped = df.withColumn('rn', F.row_number().over(window)).filter(F.col('rn') == 1).drop('rn')

    assert deduped.count() == 2

def test_no_negative_amounts(spark):
    data = [
        ('txn_001', 500.0),
        ('txn_002', -100.0), #should be filtered out
        ('txn_003', 0.0) #invalid
    ]
    df = spark.createDataFrame(data, ['transaction_id', 'amount_thb'])
    filtered = df.filter(F.col('amount_thb') > 0)
    assert filtered.count() == 1

def test_invalid_records_reach_quarantine(spark):
    data = [
        (None, 500.0), # null transaction_id - invalid
        ("txn_001", 200.0), # valid
    ]
    df = spark.createDataFrame(data, ["transaction_id", "amount_thb"])
    invalid = df.filter(F.col("transaction_id").isNull())
    valid = df.filter(F.col("transaction_id").isNotNull())
    assert invalid.count() == 1
    assert valid.count() == 1