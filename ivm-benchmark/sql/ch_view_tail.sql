-- Shared tail: best_signals CTE + final SELECT.
-- Assumes all_signals and best_txn (with a distance column) are defined in the head.
-- __CONFIDENCE__ replaced by Python: 'high' for CH-full, 'medium' for CH-light.
,
best_signals AS (
    SELECT
        cc_num,
        argMax(signal_type, priority) AS signal_type,
        max(priority)                 AS max_priority
    FROM all_signals
    GROUP BY cc_num
)
SELECT
    b.cc_num,
    b.latest_ts                                               AS ts,
    b.max_amt                                                 AS amt,
    b.category,
    b.shipping_lat,
    b.shipping_long,
    round(b.distance, 3)                                      AS distance,
    0.0                                                       AS avg_7day,
    s.signal_type,
    '__CONFIDENCE__'                                          AS confidence,
    s.max_priority * 1000 + least(b.max_amt, toFloat64(9999)) AS review_priority
FROM best_txn AS b
JOIN best_signals AS s USING (cc_num)
WHERE b.latest_ts >= (SELECT MAX(ts) FROM transactions) - INTERVAL 30 MINUTE
ORDER BY review_priority DESC
