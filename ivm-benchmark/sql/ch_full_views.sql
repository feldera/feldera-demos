-- CH-full views: fraud_signals_full (full O(N) scan) + alert count view.
-- Thresholds/priorities are substituted by Python at setup time (__GB30__ etc.).
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  ClickHouse CTE        →  Feldera equivalent (feldera_views.sql)             │
-- ├──────────────────────────────────────┬──────────────────────────────────────┤
-- │  txn_enriched                        │  txn_enriched                         │
-- │  txn_avg7  (+ latest_bucket)         │  txn_avg7                             │
-- │  flagged_gb30                        │  flagged_gift_card_burst_30d          │
-- │  flagged_gb45                        │  flagged_gift_card_burst_45d          │
-- │  flagged_sv7                         │  flagged_spend_velocity_7d            │
-- │  txn_dist_far_bucketed               │  txn_dist_far_bucketed                │
-- │  flagged_disp                        │  flagged_repeated_displacement        │
-- │  fraud_alerts                        │  fraud_alerts                         │
-- │  best_per_card                       │  best_per_card                        │
-- │  fraud_card_latest_txn               │  fraud_card_latest_ts/txn             │
-- │  final SELECT                        │  fraud_alert_details  (mat. view)     │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE OR REPLACE VIEW fraud_signals_full AS
WITH
txn_enriched AS (
    SELECT
        t.cc_num, t.ts, t.amt, t.category,
        t.shipping_lat, t.shipping_long,
        abs(t.shipping_lat - c.lat) + abs(t.shipping_long - c.long) AS distance
    FROM transactions AS t
    LEFT JOIN customers AS c USING (cc_num)
),
flagged_gb30 AS (
    SELECT DISTINCT cc_num, 'gift_card_burst_30d' AS signal_type, __PRIO_GB30__ AS priority
    FROM txn_enriched WHERE category = 'gift card'
    GROUP BY cc_num, intDiv(toUnixTimestamp(ts), 2592000)
    HAVING count() >= __GB30__
),
flagged_gb45 AS (
    SELECT DISTINCT cc_num, 'gift_card_burst_45d' AS signal_type, __PRIO_GB45__ AS priority
    FROM txn_enriched WHERE category = 'gift card'
    GROUP BY cc_num, intDiv(toUnixTimestamp(ts), 3888000)
    HAVING count() >= __GB45__
),
flagged_sv7 AS (
    SELECT DISTINCT cc_num, 'spend_velocity_7d' AS signal_type, __PRIO_SV7__ AS priority
    FROM txn_enriched
    GROUP BY cc_num, intDiv(toUnixTimestamp(ts), 604800)
    HAVING count() >= __SV7__
),
txn_dist_far_bucketed AS (
    SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400)     AS day_bucket FROM txn_enriched WHERE distance > 20.0
    UNION ALL
    SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400) - 1 AS day_bucket FROM txn_enriched WHERE distance > 20.0
    UNION ALL
    SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400) - 2 AS day_bucket FROM txn_enriched WHERE distance > 20.0
),
flagged_disp AS (
    SELECT DISTINCT cc_num, 'repeated_displacement' AS signal_type, __PRIO_DISP__ AS priority
    FROM txn_dist_far_bucketed
    GROUP BY cc_num, day_bucket
    HAVING count() >= __DISP__
),
fraud_alerts AS (
    SELECT cc_num, signal_type, priority FROM flagged_gb30
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_gb45
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_sv7
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_disp
),
txn_avg7 AS (
    SELECT cc_num,
           intDiv(toUnixTimestamp(ts), 604800) AS bucket,
           avg(amt) AS avg_7day
    FROM transactions
    GROUP BY cc_num, bucket
),
fraud_card_latest_txn AS (
    SELECT
        t.cc_num,
        max(t.ts)                                                                    AS latest_ts,
        argMax(t.amt,           t.ts)                                               AS max_amt,
        argMax(t.category,      t.ts)                                               AS category,
        argMax(t.shipping_lat,  t.ts)                                               AS shipping_lat,
        argMax(t.shipping_long, t.ts)                                               AS shipping_long,
        argMax(abs(t.shipping_lat - c.lat) + abs(t.shipping_long - c.long), t.ts)  AS distance,
        argMax(intDiv(toUnixTimestamp(t.ts), 604800), t.ts)                         AS latest_bucket
    FROM transactions AS t
    LEFT JOIN customers AS c USING (cc_num)
    GROUP BY t.cc_num
),
best_per_card AS (
    SELECT
        cc_num,
        argMax(signal_type, priority) AS signal_type,
        max(priority)                 AS max_priority
    FROM fraud_alerts
    GROUP BY cc_num
)
SELECT
    b.cc_num                                                  AS cc_num,
    b.latest_ts                                               AS ts,
    b.max_amt                                                 AS amt,
    b.category,
    b.shipping_lat,
    b.shipping_long,
    round(b.distance, 3)                                      AS distance,
    coalesce(a.avg_7day, 0.0)                                 AS avg_7day,
    s.signal_type,
    'high'                                                    AS confidence,
    s.max_priority * 1000 + least(b.max_amt, toFloat64(9999)) AS review_priority
FROM fraud_card_latest_txn AS b
JOIN best_per_card AS s USING (cc_num)
LEFT JOIN txn_avg7 AS a ON b.cc_num = a.cc_num AND b.latest_bucket = a.bucket
ORDER BY review_priority DESC;

