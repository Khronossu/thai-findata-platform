  WITH deduplicated AS (
      SELECT
          *,
          ROW_NUMBER() OVER (
              PARTITION BY transaction_id
              ORDER BY ingestion_timestamp DESC
          ) AS rn
      FROM local.bronze.transactions
  )

  SELECT
      transaction_id,
      sender_national_id_hash,
      sender_account_masked,
      receiver_account_masked,
      amount_thb,
      merchant_category,
      channel,
      event_timestamp,
      is_anomaly,
      anomaly_type,
      ingestion_timestamp,
      pipeline_run_id,
      event_date
  FROM deduplicated
  WHERE rn = 1
    AND is_pii_masked = true