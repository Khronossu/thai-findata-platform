SELECT
    sender_national_id_hash AS customer_hash,
    CASE
        WHEN avg_amount_thb >= 5000 AND transaction_count >= 100 THEN 'HIGH'
        WHEN avg_amount_thb >= 2000 OR transaction_count >= 50 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier,
    first_seen AS valid_from,
    CAST('9999-12-31' AS DATE) AS valid_to,
    TRUE AS is_current
FROM {{ ref('stg_customers') }}