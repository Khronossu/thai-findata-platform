SELECT                                                                                                                                                       
    t.transaction_id,
    t.sender_national_id_hash,
    t.sender_account_masked,
    t.receiver_account_masked,
    t.amount_thb,
    t.merchant_category,
    t.channel,
    t.event_timestamp,
    t.is_anomaly,
    t.anomaly_type,
    t.ingestion_timestamp,
    t.pipeline_run_id,
    t.event_date,                                                                                                                      
    d.risk_tier                                                                                                                                              
  FROM {{ ref('stg_transactions') }} t                                                                                                                         
  JOIN {{ ref('dim_customer_risk_profile') }} d
      ON t.sender_national_id_hash = d.customer_hash                                                                                                           
      AND t.event_timestamp BETWEEN d.valid_from AND d.valid_to