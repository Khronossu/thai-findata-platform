from datetime import datetime, timezone
import json

def write_to_quarantine(spark, records: list[dict], reason: str, pipeline_run_id: str, source_topic: str) -> None:
    if not records:
        return

    quarantine_df = spark.createDataFrame(
        [(json.dumps(record), reason, datetime.now(timezone.utc), pipeline_run_id, source_topic) for record in records],
        schema=["original_record", "rejection_reason", "rejected_at", "pipeline_run_id", "source_topic"]
    )

    quarantine_df.writeTo("local.bronze.quarantine").append()