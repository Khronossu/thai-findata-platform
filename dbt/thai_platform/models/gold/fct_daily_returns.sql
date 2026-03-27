WITH prices AS (
    SELECT
        p.ticker,
        p.trade_date,
        p.close,
        d.sector,
        d.market_cap_tier,
        LAG(p.close, 7)  OVER (PARTITION BY p.ticker ORDER BY p.trade_date) AS close_7d_ago,
        LAG(p.close, 30) OVER (PARTITION BY p.ticker ORDER BY p.trade_date) AS close_30d_ago
    FROM {{ ref('stg_set_prices') }} p
    JOIN {{ ref('dim_set_ticker') }} d ON p.ticker = d.ticker
)

SELECT
    ticker,
    trade_date,
    close,
    sector,
    market_cap_tier,
    (close - close_7d_ago)/close_7d_ago as return_7d,
    (close - close_30d_ago)/close_30d_ago as return_30d
FROM prices