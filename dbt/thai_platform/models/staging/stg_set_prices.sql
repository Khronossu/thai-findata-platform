  WITH deduplicated AS (
      SELECT
          *,
          ROW_NUMBER() OVER (
              PARTITION BY ticker, trade_date
              ORDER BY ingested_at DESC
          ) AS rn
      FROM local.bronze.set_prices
  )

    SELECT
        ticker,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        ingested_at
    FROM deduplicated
    WHERE rn = 1
    AND is_correction = false