-- Feldera fraud detection pipeline — view definitions.
-- All thresholds and priorities come from scalar functions injected at deploy time
-- (see feldera_functions_sql() in constants.py / engine_feldera.py).
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  Feldera view                    →  ClickHouse equivalent (ch_full_views)   │
-- ├──────────────────────────────────────────┬──────────────────────────────────┤
-- │  TRANSACTION_WITH_DISTANCE (view)        │  CTE: TRANSACTION_WITH_DISTANCE  │
-- │  TRANSACTION_WITH_AGGREGATES (view)      │  CTE: TRANSACTION_WITH_AGGREGATES│
-- │  fraud_alerts              (view)        │  CTEs: flagged_* + fraud_alerts   │
-- │  card_suspicion_score      (view)        │  CTE: card_suspicion_score        │
-- │  fraud_alert_summary       (view)        │  CTE: fraud_alert_summary         │
-- │  fraud_card_latest_txn     (view)        │  CTE: fraud_card_latest_txn       │
-- │  fraud_alert_details       (mat. view)   │  final SELECT                    │
-- └─────────────────────────────────────────────────────────────────────────────┘

-- ── Base enrichment ───────────────────────────────────────────────────────────

-- TRANSACTION_WITH_DISTANCE: enrich every transaction with Manhattan distance
-- between the shipping address and the cardholder's home address.
-- NULL timestamps are filtered here so all downstream views are clean.
CREATE VIEW TRANSACTION_WITH_DISTANCE AS
SELECT
    t.*,
    ABS(t.shipping_lat - c.lat) + ABS(t.shipping_long - c.long) AS distance
FROM TRANSACTION AS t
LEFT JOIN CUSTOMER AS c ON t.cc_num = c.cc_num
WHERE t.ts IS NOT NULL;

-- TRANSACTION_WITH_AGGREGATES: add all rolling window aggregates needed for
-- fraud signal detection in one place, using named WINDOW clauses to avoid
-- repeating the partition/order/range spec per column.
--
-- Windows:
--   window_3day  — 3-day trailing  (displacement signal)
--   window_7day  — 7-day trailing  (spend velocity + avg spend)
--   window_30day — 30-day trailing (gift card burst)
--   window_45day — 45-day trailing (gift card burst, wider)
--
-- Columns produced:
--   avg_7day         — trailing 7-day average spend (analyst enrichment)
--   gift_count_30day — gift card transactions in past 30 days (threshold: GB30())
--   gift_sum_30day   — total gift card spend in past 30 days
--   gift_count_45day — same over 45 days                      (threshold: GB45())
--   gift_sum_45day   — same over 45 days
--   txn_count_7day   — total transactions in past 7 days      (threshold: SV7())
--   txn_sum_7day     — total spend in past 7 days
--   disp_count_3day  — far-from-home txns (>DIST()°) in past 3 days (threshold: DISP())
--   disp_sum_3day    — far-from-home spend in past 3 days
CREATE VIEW TRANSACTION_WITH_AGGREGATES AS
SELECT
    *,
    AVG(amt)                                                         OVER window_7day  AS avg_7day,
    SUM(CASE WHEN category = 'gift card' THEN 1   ELSE 0 END)       OVER window_30day AS gift_count_30day,
    SUM(CASE WHEN category = 'gift card' THEN amt ELSE 0 END)       OVER window_30day AS gift_sum_30day,
    SUM(CASE WHEN category = 'gift card' THEN 1   ELSE 0 END)       OVER window_45day AS gift_count_45day,
    SUM(CASE WHEN category = 'gift card' THEN amt ELSE 0 END)       OVER window_45day AS gift_sum_45day,
    COUNT(*)                                                         OVER window_7day  AS txn_count_7day,
    SUM(amt)                                                         OVER window_7day  AS txn_sum_7day,
    SUM(CASE WHEN distance > DIST() THEN 1   ELSE 0 END)            OVER window_3day  AS disp_count_3day,
    SUM(CASE WHEN distance > DIST() THEN amt ELSE 0 END)            OVER window_3day  AS disp_sum_3day
FROM TRANSACTION_WITH_DISTANCE
WINDOW
    window_3day  AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 3  DAYS PRECEDING AND CURRENT ROW),
    window_7day  AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 7  DAYS PRECEDING AND CURRENT ROW),
    window_30day AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW),
    window_45day AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 45 DAYS PRECEDING AND CURRENT ROW);

-- ── Signal detection + alert stream ──────────────────────────────────────────
-- fraud_alerts: one row per (card, transaction) where that transaction sits at
-- the tip of a qualifying window.  All four signals inlined as one UNION ALL.
--
--   signal              priority      window   threshold
--   gift_card_burst_30d  PRIO_GB30()  30 days  GB30() gift card txns
--   gift_card_burst_45d  PRIO_GB45()  45 days  GB45() gift card txns
--   spend_velocity_7d    PRIO_SV7()    7 days  SV7()  total txns
--   repeated_displacement PRIO_DISP()  3 days  DISP() far-from-home txns
CREATE VIEW fraud_alerts AS
SELECT cc_num, ts, gift_sum_30day AS amt, 'gift_card_burst_30d'   AS signal_type, PRIO_GB30() AS priority
FROM TRANSACTION_WITH_AGGREGATES WHERE gift_count_30day >= GB30()
UNION ALL
SELECT cc_num, ts, gift_sum_45day AS amt, 'gift_card_burst_45d'   AS signal_type, PRIO_GB45() AS priority
FROM TRANSACTION_WITH_AGGREGATES WHERE gift_count_45day >= GB45()
UNION ALL
SELECT cc_num, ts, txn_sum_7day   AS amt, 'spend_velocity_7d'     AS signal_type, PRIO_SV7()  AS priority
FROM TRANSACTION_WITH_AGGREGATES WHERE txn_count_7day  >= SV7()
UNION ALL
SELECT cc_num, ts, disp_sum_3day  AS amt, 'repeated_displacement' AS signal_type, PRIO_DISP() AS priority
FROM TRANSACTION_WITH_AGGREGATES WHERE disp_count_3day >= DISP();

-- ── Per-card aggregates ───────────────────────────────────────────────────────

-- card_suspicion_score: sum of all fired signal priorities per flagged card.
-- Multi-signal cards rank higher (e.g. score 8 if two signals fire with prio 3+5).
CREATE VIEW card_suspicion_score AS
SELECT cc_num, SUM(priority) AS total_priority
FROM fraud_alerts
GROUP BY cc_num;

-- fraud_alert_summary: highest alert amount and most recent alert timestamp per card.
CREATE VIEW fraud_alert_summary AS
SELECT cc_num, MAX(amt) AS alert_amt, MAX(ts) AS alert_ts
FROM fraud_alerts
GROUP BY cc_num;

-- ── Latest transaction enrichment ─────────────────────────────────────────────

-- fraud_card_latest_txn: enriched snapshot of the most recent transaction per card,
-- across ALL cards (unflagged included). The JOIN in fraud_alert_details filters to
-- flagged cards only — same pattern as ClickHouse's fraud_card_latest_txn CTE.
-- MAX(ts) + GROUP BY collapses timestamp ties to one row per card (argMax equivalent).
CREATE VIEW fraud_card_latest_txn AS
SELECT
    cc_num,
    MAX(ts)              AS ts,
    MIN(amt)             AS amt,
    MIN(category)        AS category,
    MIN(shipping_lat)    AS shipping_lat,
    MIN(shipping_long)   AS shipping_long,
    MIN(distance)        AS distance,
    MIN(avg_7day)        AS avg_7day
FROM TRANSACTION_WITH_AGGREGATES
GROUP BY cc_num;

-- ── Summary ───────────────────────────────────────────────────────────────────
-- fraud_alert_details: one row per flagged card.
-- Columns match ClickHouse fraud_signals_full final SELECT.
-- review_priority = total_priority * REVIEW_SCALE() + LEAST(amt, REVIEW_CAP())
--   Multiplying by REVIEW_SCALE() ensures priority tiers never bleed into each other.
-- Materialized so the demo query — SELECT COUNT(*) FROM fraud_alert_details — is O(1).
CREATE MATERIALIZED VIEW fraud_alert_details AS
SELECT
    b.cc_num,
    s.total_priority,
    a.alert_amt,
    a.alert_ts,
    b.ts,
    b.amt,
    b.category,
    b.shipping_lat,
    b.shipping_long,
    ROUND(b.distance, 3)                                                                         AS distance,
    COALESCE(b.avg_7day, CAST(0.0 AS DOUBLE))                                                    AS avg_7day,
    CAST(s.total_priority AS DOUBLE) * CAST(REVIEW_SCALE() AS DOUBLE) + LEAST(CAST(b.amt AS DOUBLE), REVIEW_CAP()) AS review_priority
FROM fraud_card_latest_txn  AS b
JOIN card_suspicion_score   AS s ON b.cc_num = s.cc_num
JOIN fraud_alert_summary    AS a ON b.cc_num = a.cc_num;
