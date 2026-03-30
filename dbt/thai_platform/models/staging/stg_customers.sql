SELECT
    sender_national_id_hash,
    AVG(amount_thb) as avg_amount_thb,
    COUNT(*) as transaction_count,
    MIN(event_timestamp) as first_seen,
    MAX(event_timestamp) as last_seen
FROM {{ ref('stg_transactions') }}
GROUP BY sender_national_id_hash