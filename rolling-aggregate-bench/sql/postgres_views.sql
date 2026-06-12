-- PostgreSQL fraud detection view: fraud_signals_full (full O(N) scan).
-- Threshold/priority functions injected from constants.py at setup time (GB30() etc.).
-- PostgreSQL supports INTERVAL-based RANGE bounds natively — no integer-second workaround needed.
-- Mirrors the ClickHouse architecture: full pipeline as CTEs inside a single view.
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  PostgreSQL CTE               →  ClickHouse equivalent (clickhouse_views)   │
-- ├──────────────────────────────────────────┬──────────────────────────────────┤
-- │  TRANSACTION_WITH_DISTANCE               │  TRANSACTION_WITH_DISTANCE        │
-- │  TRANSACTION_WITH_AGGREGATES             │  TRANSACTION_WITH_AGGREGATES      │
-- │  flagged_gb30 + flagged_gb45 +           │  flagged_gb30 + flagged_gb45 +    │
-- │    flagged_sv7 + flagged_disp            │    flagged_sv7 + flagged_disp     │
-- │  fraud_alerts                            │  fraud_alerts                     │
-- │  card_suspicion_score                    │  card_suspicion_score             │
-- │  fraud_alert_summary                     │  fraud_alert_summary              │
-- │  fraud_card_latest_txn                   │  fraud_card_latest_txn            │
-- │  final SELECT                            │  final SELECT                     │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE OR REPLACE VIEW fraud_signals_full AS
WITH
TRANSACTION_WITH_DISTANCE AS (
    SELECT
        t.cc_num, t.ts, t.amt, t.category,
        t.shipping_lat, t.shipping_long,
        ABS(t.shipping_lat - c.lat) + ABS(t.shipping_long - c.long) AS distance
    FROM transactions AS t
    LEFT JOIN customers AS c USING (cc_num)
    WHERE t.ts IS NOT NULL
),
TRANSACTION_WITH_AGGREGATES AS (
    SELECT
        *,
        AVG(amt)                                                          OVER window_7day  AS avg_7day,
        SUM(CASE WHEN category = 'gift card' THEN 1   ELSE 0 END)        OVER window_30day AS gift_count_30day,
        SUM(CASE WHEN category = 'gift card' THEN amt ELSE 0 END)        OVER window_30day AS gift_sum_30day,
        SUM(CASE WHEN category = 'gift card' THEN 1   ELSE 0 END)        OVER window_45day AS gift_count_45day,
        SUM(CASE WHEN category = 'gift card' THEN amt ELSE 0 END)        OVER window_45day AS gift_sum_45day,
        COUNT(*)                                                          OVER window_7day  AS txn_count_7day,
        SUM(amt)                                                          OVER window_7day  AS txn_sum_7day,
        SUM(CASE WHEN distance > DIST() THEN 1   ELSE 0 END)             OVER window_3day  AS disp_count_3day,
        SUM(CASE WHEN distance > DIST() THEN amt ELSE 0 END)             OVER window_3day  AS disp_sum_3day
    FROM TRANSACTION_WITH_DISTANCE
    WINDOW
        window_3day  AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL '3 days'  PRECEDING AND CURRENT ROW),
        window_7day  AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL '7 days'  PRECEDING AND CURRENT ROW),
        window_30day AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW),
        window_45day AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL '45 days' PRECEDING AND CURRENT ROW)
),
flagged_gb30 AS (
    SELECT cc_num, ts AS window_start, gift_sum_30day AS total_amt,
           'gift_card_burst_30d' AS signal_type, PRIO_GB30() AS priority
    FROM TRANSACTION_WITH_AGGREGATES
    WHERE gift_count_30day >= GB30()
),
flagged_gb45 AS (
    SELECT cc_num, ts AS window_start, gift_sum_45day AS total_amt,
           'gift_card_burst_45d' AS signal_type, PRIO_GB45() AS priority
    FROM TRANSACTION_WITH_AGGREGATES
    WHERE gift_count_45day >= GB45()
),
flagged_sv7 AS (
    SELECT cc_num, ts AS window_start, txn_sum_7day AS total_spend,
           'spend_velocity_7d' AS signal_type, PRIO_SV7() AS priority
    FROM TRANSACTION_WITH_AGGREGATES
    WHERE txn_count_7day >= SV7()
),
flagged_disp AS (
    SELECT cc_num, ts AS window_start, disp_sum_3day AS total_amt,
           'repeated_displacement' AS signal_type, PRIO_DISP() AS priority
    FROM TRANSACTION_WITH_AGGREGATES
    WHERE disp_count_3day >= DISP()
),
fraud_alerts AS (
    SELECT cc_num, window_start AS ts, total_amt   AS amt, signal_type, priority FROM flagged_gb30
    UNION ALL SELECT cc_num, window_start AS ts, total_amt   AS amt, signal_type, priority FROM flagged_gb45
    UNION ALL SELECT cc_num, window_start AS ts, total_spend AS amt, signal_type, priority FROM flagged_sv7
    UNION ALL SELECT cc_num, window_start AS ts, total_amt   AS amt, signal_type, priority FROM flagged_disp
),
-- suspicion score per card: sum of priorities across all fired signals
card_suspicion_score AS (
    SELECT cc_num,
           SUM(priority) AS total_priority
    FROM fraud_alerts
    GROUP BY cc_num
),
-- highest-value alert amount and most recent alert timestamp per card
fraud_alert_summary AS (
    SELECT cc_num, MAX(amt) AS alert_amt, MAX(ts) AS alert_ts
    FROM fraud_alerts
    GROUP BY cc_num
),
-- most recent transaction per card: DISTINCT ON gives the actual row at MAX(ts),
-- equivalent to ClickHouse argMax(col, ts).
fraud_card_latest_txn AS (
    SELECT DISTINCT ON (cc_num)
        cc_num,
        ts            AS latest_ts,
        amt           AS max_amt,
        category,
        shipping_lat,
        shipping_long,
        distance,
        avg_7day
    FROM TRANSACTION_WITH_AGGREGATES
    ORDER BY cc_num, ts DESC
)
SELECT
    b.cc_num,
    s.total_priority,
    a.alert_amt,
    a.alert_ts,
    b.latest_ts                                                           AS ts,
    b.max_amt                                                             AS amt,
    b.category,
    b.shipping_lat,
    b.shipping_long,
    ROUND(CAST(b.distance AS NUMERIC), 3)                                 AS distance,
    COALESCE(b.avg_7day, 0.0)                                             AS avg_7day,
    s.total_priority * REVIEW_SCALE() + LEAST(b.max_amt, REVIEW_CAP())   AS review_priority
FROM fraud_card_latest_txn AS b
JOIN card_suspicion_score  AS s USING (cc_num)
JOIN fraud_alert_summary   AS a USING (cc_num)
ORDER BY review_priority DESC;
