-- CH-full views: fraud_signals_full (full O(N) scan).
-- Thresholds/priorities use ClickHouse lambda UDFs defined in ch_full_tables.sql (GB30() etc.).
-- Window RANGE bounds are literal seconds (ClickHouse rejects non-literal RANGE offsets).
-- Mirrors the Feldera architecture: all rolling aggregates computed once in
-- TRANSACTION_WITH_AGGREGATES using named WINDOW clauses — signal CTEs are WHERE filters.
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  ClickHouse CTE               →  Feldera equivalent (feldera_views.sql)     │
-- ├──────────────────────────────────────────┬──────────────────────────────────┤
-- │  TRANSACTION_WITH_DISTANCE               │  TRANSACTION_WITH_DISTANCE        │
-- │  TRANSACTION_WITH_AGGREGATES             │  TRANSACTION_WITH_AGGREGATES      │
-- │  flagged_gb30                            │  flagged_gift_card_burst_30d      │
-- │  flagged_gb45                            │  flagged_gift_card_burst_45d      │
-- │  flagged_sv7                             │  flagged_spend_velocity_7d        │
-- │  flagged_disp                            │  flagged_repeated_displacement    │
-- │  fraud_alerts                            │  fraud_alerts                     │
-- │  card_suspicion_score                    │  card_suspicion_score             │
-- │  fraud_card_latest_txn                   │  fraud_card_latest_ts/txn         │
-- │  final SELECT                            │  fraud_alert_details (mat. view)  │
-- └─────────────────────────────────────────────────────────────────────────────┘
CREATE OR REPLACE VIEW fraud_signals_full AS
WITH
TRANSACTION_WITH_DISTANCE AS (
    SELECT
        t.cc_num, t.ts, t.amt, t.category,
        t.shipping_lat, t.shipping_long,
        abs(t.shipping_lat - c.lat) + abs(t.shipping_long - c.long) AS distance
    FROM transactions AS t
    LEFT JOIN customers AS c USING (cc_num)
    WHERE t.ts IS NOT NULL
),
TRANSACTION_WITH_AGGREGATES AS (
    SELECT
        *,
        avg(amt)                                         OVER window_7day  AS avg_7day,
        sum(if(category = 'gift card', 1,   0))          OVER window_30day AS gift_count_30day,
        sum(if(category = 'gift card', amt, 0))          OVER window_30day AS gift_sum_30day,
        sum(if(category = 'gift card', 1,   0))          OVER window_45day AS gift_count_45day,
        sum(if(category = 'gift card', amt, 0))          OVER window_45day AS gift_sum_45day,
        count()                                          OVER window_7day  AS txn_count_7day,
        sum(amt)                                         OVER window_7day  AS txn_sum_7day,
        sum(if(distance > DIST(), 1,   0))                 OVER window_3day  AS disp_count_3day,
        sum(if(distance > DIST(), amt, 0))                 OVER window_3day  AS disp_sum_3day
    FROM TRANSACTION_WITH_DISTANCE
    WINDOW
        window_3day  AS (PARTITION BY cc_num ORDER BY toUnixTimestamp(ts) RANGE BETWEEN {window_3d_secs}  PRECEDING AND CURRENT ROW),  -- 3 days
        window_7day  AS (PARTITION BY cc_num ORDER BY toUnixTimestamp(ts) RANGE BETWEEN {window_7d_secs}  PRECEDING AND CURRENT ROW),  -- 7 days
        window_30day AS (PARTITION BY cc_num ORDER BY toUnixTimestamp(ts) RANGE BETWEEN {window_30d_secs} PRECEDING AND CURRENT ROW), -- 30 days
        window_45day AS (PARTITION BY cc_num ORDER BY toUnixTimestamp(ts) RANGE BETWEEN {window_45d_secs} PRECEDING AND CURRENT ROW)  -- 45 days
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
           sum(priority) AS total_priority
    FROM fraud_alerts
    GROUP BY cc_num
),
-- highest-value alert amount and most recent alert timestamp per card
fraud_alert_summary AS (
    SELECT cc_num, max(amt) AS alert_amt, max(ts) AS alert_ts
    FROM fraud_alerts
    GROUP BY cc_num
),
fraud_card_latest_txn AS (
    SELECT
        cc_num,
        max(ts)               AS latest_ts,
        argMax(amt,           ts) AS max_amt,
        argMax(category,      ts) AS category,
        argMax(shipping_lat,  ts) AS shipping_lat,
        argMax(shipping_long, ts) AS shipping_long,
        argMax(distance,      ts) AS distance,
        argMax(avg_7day,      ts) AS avg_7day
    FROM TRANSACTION_WITH_AGGREGATES
    GROUP BY cc_num
)
SELECT
    b.cc_num                                                          AS cc_num,
    s.total_priority,
    a.alert_amt,
    a.alert_ts,
    b.latest_ts                                                       AS ts,
    b.max_amt                                                         AS amt,
    b.category,
    b.shipping_lat,
    b.shipping_long,
    round(b.distance, 3)                                              AS distance,
    coalesce(b.avg_7day, 0.0)                                         AS avg_7day,
    s.total_priority * REVIEW_SCALE() + least(b.max_amt, REVIEW_CAP()) AS review_priority
FROM fraud_card_latest_txn AS b
JOIN card_suspicion_score  AS s USING (cc_num)
JOIN fraud_alert_summary   AS a USING (cc_num)
ORDER BY review_priority DESC;
