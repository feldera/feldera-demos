-- CH-light views: fraud_signals_light (MV-backed O(delta) query) + alert count view.
-- Thresholds/priorities are substituted by Python at setup time (__GB30__ etc.).
-- Key difference from CH-full: signal counts come from pre-built SummingMergeTree MVs,
-- not recomputed from raw transactions each query.
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  ClickHouse CTE        →  Feldera equivalent (feldera_views.sql)             │
-- ├──────────────────────────────────────┬──────────────────────────────────────┤
-- │  (reads gb30_counts MV)              │  flagged_gift_card_burst_30d          │
-- │  flagged_gb30                        │    → fraud_alerts (gb30 arm)          │
-- │  (reads gb45_counts MV)              │  flagged_gift_card_burst_45d          │
-- │  flagged_gb45                        │    → fraud_alerts (gb45 arm)          │
-- │  (reads sv7_counts MV)               │  flagged_spend_velocity_7d            │
-- │  flagged_sv7                         │    → fraud_alerts (sv7 arm)           │
-- │  (reads disp_counts MV)              │  flagged_repeated_displacement        │
-- │  flagged_disp                        │    → fraud_alerts (disp arm)          │
-- │  fraud_alerts                        │  fraud_alerts                         │
-- │  flagged_cards                       │  (implicit — fraud_alerts join)       │
-- │  txn_avg7  (filtered to flagged)     │  TRANSACTION_WITH_AGGREGATES          │
-- │  fraud_card_latest_txn               │  fraud_card_latest_ts/txn             │
-- │  best_per_card                       │  best_per_card                        │
-- │  final SELECT                        │  fraud_alert_details  (mat. view)     │
-- └─────────────────────────────────────────────────────────────────────────────┘

-- ── Materialized Views: fire on every INSERT to transactions ──────────────────
-- Each MV writes pre-aggregated counts into the corresponding backing table
-- (defined in ch_light_tables.sql).

-- Note: UNION ALL in a single MV only fires the first SELECT in ClickHouse.
-- disp uses three separate MVs so each day-bucket offset is captured independently.

CREATE MATERIALIZED VIEW IF NOT EXISTS gb30_mv
TO gb30_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 2592000) AS bucket, count() AS cnt
FROM transactions WHERE category = 'gift card'
GROUP BY cc_num, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS gb45_mv
TO gb45_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 3888000) AS bucket, count() AS cnt
FROM transactions WHERE category = 'gift card'
GROUP BY cc_num, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS sv7_mv
TO sv7_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 604800) AS bucket, count() AS cnt
FROM transactions
GROUP BY cc_num, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS disp_mv_d0
TO disp_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400)     AS bucket, count() AS cnt
FROM transactions WHERE shipping_lat != 0
GROUP BY cc_num, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS disp_mv_d1
TO disp_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400) - 1 AS bucket, count() AS cnt
FROM transactions WHERE shipping_lat != 0
GROUP BY cc_num, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS disp_mv_d2
TO disp_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400) - 2 AS bucket, count() AS cnt
FROM transactions WHERE shipping_lat != 0
GROUP BY cc_num, bucket;

-- ── Query views ───────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW fraud_signals_light AS
WITH
flagged_gb30 AS (
    SELECT DISTINCT cc_num, 'gift_card_burst_30d' AS signal_type, __PRIO_GB30__ AS priority
    FROM gb30_counts
    GROUP BY cc_num, bucket
    HAVING sum(cnt) >= __GB30__
),
flagged_gb45 AS (
    SELECT DISTINCT cc_num, 'gift_card_burst_45d' AS signal_type, __PRIO_GB45__ AS priority
    FROM gb45_counts
    GROUP BY cc_num, bucket
    HAVING sum(cnt) >= __GB45__
),
flagged_sv7 AS (
    SELECT DISTINCT cc_num, 'spend_velocity_7d' AS signal_type, __PRIO_SV7__ AS priority
    FROM sv7_counts
    GROUP BY cc_num, bucket
    HAVING sum(cnt) >= __SV7__
),
flagged_disp AS (
    SELECT DISTINCT cc_num, 'repeated_displacement' AS signal_type, __PRIO_DISP__ AS priority
    FROM disp_counts
    GROUP BY cc_num, bucket
    HAVING sum(cnt) >= __DISP__
),
fraud_alerts AS (
    SELECT cc_num, signal_type, priority FROM flagged_gb30
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_gb45
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_sv7
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_disp
),
flagged_cards AS (
    SELECT DISTINCT cc_num FROM fraud_alerts
),
txn_avg7 AS (
    SELECT cc_num,
           intDiv(toUnixTimestamp(ts), 604800) AS bucket,
           avg(amt) AS avg_7day
    FROM transactions
    WHERE cc_num IN (SELECT cc_num FROM flagged_cards)
    GROUP BY cc_num, bucket
),
fraud_card_latest_txn AS (
    SELECT
        cc_num,
        max(ts)                                          AS latest_ts,
        argMax(amt,           ts)                        AS max_amt,
        argMax(category,      ts)                        AS category,
        argMax(shipping_lat,  ts)                        AS shipping_lat,
        argMax(shipping_long, ts)                        AS shipping_long,
        0.0                                              AS distance,
        argMax(intDiv(toUnixTimestamp(ts), 604800), ts)  AS latest_bucket
    FROM transactions
    WHERE cc_num IN (SELECT cc_num FROM flagged_cards)
    GROUP BY cc_num
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
    'medium'                                                  AS confidence,
    s.max_priority * 1000 + least(b.max_amt, toFloat64(9999)) AS review_priority
FROM fraud_card_latest_txn AS b
JOIN best_per_card AS s USING (cc_num)
LEFT JOIN txn_avg7 AS a ON b.cc_num = a.cc_num AND b.latest_bucket = a.bucket
ORDER BY review_priority DESC;

