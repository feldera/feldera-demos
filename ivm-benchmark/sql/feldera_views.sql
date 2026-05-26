-- Feldera fraud detection pipeline — view definitions.
-- Threshold/priority CREATE FUNCTION preamble is generated from constants.py at setup time
-- (see engine_feldera.py: feldera_functions_sql()) — no hardcoded values here.
-- Built on the architecture of agentic-fraud-detection/programs/fraud_detection_demo.sql:
-- all rolling aggregates are computed once in TRANSACTION_WITH_AGGREGATES using named
-- WINDOW clauses; signal detection views are simple WHERE filters on that view.
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  Feldera view                    →  ClickHouse equivalent (ch_full_views)   │
-- ├──────────────────────────────────────────┬──────────────────────────────────┤
-- │  TRANSACTION_WITH_DISTANCE (view)        │  CTE: txn_enriched               │
-- │  TRANSACTION_WITH_AGGREGATES (view)      │  CTEs: txn_with_avg7 + signals   │
-- │  flagged_gift_card_burst_30d (view)      │  CTE: flagged_gb30               │
-- │  flagged_gift_card_burst_45d (view)      │  CTE: flagged_gb45               │
-- │  flagged_spend_velocity_7d   (view)      │  CTE: flagged_sv7                │
-- │  flagged_repeated_displacement (view)    │  CTE: flagged_disp               │
-- │  fraud_alerts                (view)      │  CTE: fraud_alerts               │
-- │  best_per_card               (view)      │  CTE: best_per_card              │
-- │  fraud_card_latest_ts        (view)      │  (folded into latest_txn)        │
-- │  fraud_card_latest_txn       (view)      │  CTE: fraud_card_latest_txn      │
-- │  fraud_alert_details   (mat. view)       │  final SELECT                    │
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
--   avg_7day         — trailing 7-day average spend (enrichment for analysts)
--   gift_count_30day — gift card transactions in past 30 days (signal threshold)
--   gift_sum_30day   — total gift card spend in past 30 days   (alert amount)
--   gift_count_45day — same over 45 days
--   gift_sum_45day   — same over 45 days
--   txn_count_7day   — total transactions in past 7 days       (signal threshold)
--   txn_sum_7day     — total spend in past 7 days              (alert amount)
--   disp_count_3day  — far-from-home transactions in past 3 days (signal threshold)
--   disp_sum_3day    — far-from-home spend in past 3 days        (alert amount)
CREATE VIEW TRANSACTION_WITH_AGGREGATES AS
SELECT
    *,
    AVG(amt)                                                    OVER window_7day  AS avg_7day,
    SUM(CASE WHEN category = 'gift card' THEN 1   ELSE 0 END)  OVER window_30day AS gift_count_30day,
    SUM(CASE WHEN category = 'gift card' THEN amt ELSE 0 END)  OVER window_30day AS gift_sum_30day,
    SUM(CASE WHEN category = 'gift card' THEN 1   ELSE 0 END)  OVER window_45day AS gift_count_45day,
    SUM(CASE WHEN category = 'gift card' THEN amt ELSE 0 END)  OVER window_45day AS gift_sum_45day,
    COUNT(*)                                                    OVER window_7day  AS txn_count_7day,
    SUM(amt)                                                    OVER window_7day  AS txn_sum_7day,
    SUM(CASE WHEN distance > 20.0 THEN 1   ELSE 0 END)         OVER window_3day  AS disp_count_3day,
    SUM(CASE WHEN distance > 20.0 THEN amt ELSE 0 END)         OVER window_3day  AS disp_sum_3day
FROM TRANSACTION_WITH_DISTANCE
WINDOW
    window_3day  AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 3  DAYS PRECEDING AND CURRENT ROW),
    window_7day  AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 7  DAYS PRECEDING AND CURRENT ROW),
    window_30day AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW),
    window_45day AS (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 45 DAYS PRECEDING AND CURRENT ROW);

-- ── Signal detection: WHERE filters on TRANSACTION_WITH_AGGREGATES ────────────
-- Each view emits one row per transaction that sits at the tip of a qualifying
-- window — i.e., each moment a card crosses the threshold.

-- flagged_gift_card_burst_30d: card has >= GB30() gift card transactions in
-- the trailing 30-day window.  Detects bursts of gift card purchases within a
-- month — a common money-laundering pattern.
CREATE VIEW flagged_gift_card_burst_30d AS
SELECT cc_num, ts AS window_start, gift_count_30day AS gift_card_count, gift_sum_30day AS total_amt
FROM TRANSACTION_WITH_AGGREGATES
WHERE gift_count_30day >= GB30();

-- flagged_gift_card_burst_45d: same signal over a wider 45-day window.
-- Catches slower accumulation that slips under the 30-day threshold.
CREATE VIEW flagged_gift_card_burst_45d AS
SELECT cc_num, ts AS window_start, gift_count_45day AS gift_card_count, gift_sum_45day AS total_amt
FROM TRANSACTION_WITH_AGGREGATES
WHERE gift_count_45day >= GB45();

-- flagged_spend_velocity_7d: card has >= SV7() transactions in the trailing
-- 7-day window.  Detects unusually high transaction frequency — a sign of account
-- takeover or card-testing attacks.
CREATE VIEW flagged_spend_velocity_7d AS
SELECT cc_num, ts AS window_start, txn_count_7day AS txn_count, txn_sum_7day AS total_spend
FROM TRANSACTION_WITH_AGGREGATES
WHERE txn_count_7day >= SV7();

-- flagged_repeated_displacement: card has >= DISP() far-from-home transactions
-- (distance > 20°) in the trailing 3-day window.  Detects the card being used
-- far from the cardholder's home on multiple consecutive days — indicative of
-- physical theft or cloning.
CREATE VIEW flagged_repeated_displacement AS
SELECT cc_num, ts AS window_start, disp_count_3day AS distant_count, disp_sum_3day AS total_amt
FROM TRANSACTION_WITH_AGGREGATES
WHERE disp_count_3day >= DISP();

-- ── Alert aggregation ─────────────────────────────────────────────────────────

-- fraud_alerts: unified stream of all four signals with numeric priority:
--   repeated_displacement=5 (highest), gift_card_burst_45d=4, gift_card_burst_30d=3,
--   spend_velocity_7d=1 (lowest).
CREATE VIEW fraud_alerts AS
SELECT cc_num, window_start AS ts, total_amt   AS amt, 'gift_card_burst_30d'   AS signal_type, PRIO_GB30() AS priority FROM flagged_gift_card_burst_30d
UNION ALL
SELECT cc_num, window_start AS ts, total_amt   AS amt, 'gift_card_burst_45d'   AS signal_type, PRIO_GB45() AS priority FROM flagged_gift_card_burst_45d
UNION ALL
SELECT cc_num, window_start AS ts, total_spend AS amt, 'spend_velocity_7d'     AS signal_type, PRIO_SV7()  AS priority FROM flagged_spend_velocity_7d
UNION ALL
SELECT cc_num, window_start AS ts, total_amt   AS amt, 'repeated_displacement' AS signal_type, PRIO_DISP() AS priority FROM flagged_repeated_displacement;

-- best_per_card: highest-priority signal per card — ensures one output row
-- per card regardless of how many signals fired simultaneously.
CREATE VIEW best_per_card AS
SELECT cc_num, MAX(priority) AS max_priority, MIN(signal_type) AS signal_type
FROM fraud_alerts
GROUP BY cc_num;

-- ── Enriched alert details ────────────────────────────────────────────────────

-- fraud_card_latest_ts: most-recent transaction timestamp per fraud-flagged card.
CREATE VIEW fraud_card_latest_ts AS
SELECT t.cc_num, MAX(t.ts) AS latest_ts
FROM TRANSACTION t
INNER JOIN fraud_alerts a ON t.cc_num = a.cc_num
GROUP BY t.cc_num;

-- fraud_card_latest_txn: full enriched row (distance + avg_7day) for the most
-- recent transaction of each fraud-flagged card — the analyst context snapshot.
CREATE VIEW fraud_card_latest_txn AS
SELECT t.cc_num, t.ts, t.amt, t.category, t.shipping_lat, t.shipping_long, t.distance, t.avg_7day
FROM TRANSACTION_WITH_AGGREGATES t
INNER JOIN fraud_card_latest_ts lt ON t.cc_num = lt.cc_num AND t.ts = lt.latest_ts;

-- fraud_alert_details: final materialized output — one row per flagged card.
-- Materialized so the demo query is O(flagged cards), not O(all transactions).
CREATE MATERIALIZED VIEW fraud_alert_details AS
SELECT
    b.cc_num,
    b.signal_type,
    MAX(a.amt)           AS alert_amt,
    MAX(a.ts)            AS alert_ts,
    MIN(t.ts)            AS ts,
    MIN(t.amt)           AS amt,
    MIN(t.category)      AS category,
    MIN(t.shipping_lat)  AS shipping_lat,
    MIN(t.shipping_long) AS shipping_long,
    MIN(t.distance)      AS distance,
    MIN(t.avg_7day)      AS avg_7day
FROM best_per_card b
INNER JOIN fraud_alerts a ON b.cc_num = a.cc_num AND a.priority = b.max_priority
INNER JOIN fraud_card_latest_txn t ON b.cc_num = t.cc_num
GROUP BY b.cc_num, b.signal_type;
