-- Feldera fraud detection pipeline — view definitions.
-- Thresholds/priorities are substituted by Python at setup time (__GB30__ etc.).
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  Feldera view              →  ClickHouse equivalent (ch_full_views.sql)     │
-- ├──────────────────────────────────────┬──────────────────────────────────────┤
-- │  txn_enriched (view)                 │  CTE: txn_enriched                    │
-- │  txn_avg7 (view)                     │  CTE: txn_avg7  (+ latest_bucket col) │
-- │  flagged_gift_card_burst_30d (view)  │  CTE: flagged_gb30                    │
-- │  flagged_gift_card_burst_45d (view)  │  CTE: flagged_gb45                    │
-- │  flagged_spend_velocity_7d   (view)  │  CTE: flagged_sv7                     │
-- │  txn_dist_far_bucketed       (view)  │  CTE: txn_dist_far_bucketed           │
-- │  flagged_repeated_displacement(view) │  CTE: flagged_disp                    │
-- │  fraud_alerts                (view)  │  CTE: fraud_alerts                    │
-- │  best_per_card               (view)  │  CTE: best_per_card                   │
-- │  fraud_card_latest_ts        (view)  │  (folded into fraud_card_latest_txn)  │
-- │  fraud_card_latest_txn       (view)  │  CTE: fraud_card_latest_txn           │
-- │  fraud_alert_details   (mat. view)   │  final SELECT in fraud_signals_full   │
-- ├──────────────────────────────────────┴──────────────────────────────────────┤
-- │  Epoch bucket:  TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts)  │
-- │                   / N                                                        │
-- │  ≡ CH:          intDiv(toUnixTimestamp(ts), N)                               │
-- └─────────────────────────────────────────────────────────────────────────────┘

CREATE VIEW txn_enriched AS
SELECT
    t.*,
    ABS(t.shipping_lat - c.lat) + ABS(t.shipping_long - c.long) AS distance
FROM TRANSACTION AS t
LEFT JOIN CUSTOMER AS c ON t.cc_num = c.cc_num;

CREATE VIEW txn_avg7 AS
SELECT t.*,
       agg.avg_7day
FROM txn_enriched t
LEFT JOIN (
    SELECT cc_num,
           TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 604800 AS bucket,
           AVG(amt) AS avg_7day
    FROM TRANSACTION
    GROUP BY cc_num, TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 604800
) agg ON t.cc_num = agg.cc_num
     AND TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', t.ts) / 604800 = agg.bucket;

CREATE VIEW txn_notnull AS
SELECT * FROM TRANSACTION WHERE ts IS NOT NULL;

CREATE VIEW txn_gift_notnull AS
SELECT * FROM TRANSACTION WHERE ts IS NOT NULL AND category = 'gift card';

CREATE VIEW txn_dist_far_notnull AS
SELECT * FROM txn_enriched WHERE ts IS NOT NULL AND distance > 20.0;

-- ── Signal detection: epoch-aligned GROUP BY ─────────────────────────────────
-- Matches CH-full/CH-light epoch semantics exactly: one bucket per (cc_num, epoch).
-- bucket = floor(unix_epoch / bucket_seconds) — same as intDiv() in ClickHouse.

-- gb30: 30-day epoch bucket  (30 * 86400 = 2592000 s)
CREATE VIEW flagged_gift_card_burst_30d AS
SELECT cc_num,
       TIMESTAMPADD(SECOND,
           TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 2592000 * 2592000,
           TIMESTAMP '1970-01-01 00:00:00') AS window_start,
       COUNT(*) AS gift_card_count, SUM(amt) AS total_amt
FROM txn_gift_notnull
GROUP BY cc_num, TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 2592000
HAVING COUNT(*) >= __GB30__;

-- gb45: 45-day epoch bucket  (45 * 86400 = 3888000 s)
CREATE VIEW flagged_gift_card_burst_45d AS
SELECT cc_num,
       TIMESTAMPADD(SECOND,
           TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 3888000 * 3888000,
           TIMESTAMP '1970-01-01 00:00:00') AS window_start,
       COUNT(*) AS gift_card_count, SUM(amt) AS total_amt
FROM txn_gift_notnull
GROUP BY cc_num, TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 3888000
HAVING COUNT(*) >= __GB45__;

-- sv7: 7-day epoch bucket  (7 * 86400 = 604800 s)
CREATE VIEW flagged_spend_velocity_7d AS
WITH sv7_bucketed AS (
    SELECT cc_num, amt,
           TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 604800 AS bucket
    FROM txn_notnull
)
SELECT cc_num,
       TIMESTAMPADD(SECOND, bucket * 604800, TIMESTAMP '1970-01-01 00:00:00') AS window_start,
       COUNT(*) AS txn_count, SUM(amt) AS total_spend
FROM sv7_bucketed
GROUP BY cc_num, bucket
HAVING COUNT(*) >= __SV7__;

-- disp: 3-day sliding window via 3-way UNION ALL offset trick.
-- A far transaction at day D contributes to buckets D, D-1, D-2;
-- bucket B therefore counts all far transactions in [B, B+3d).
CREATE VIEW txn_dist_far_bucketed AS
SELECT cc_num, amt,
       TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 86400     AS day_bucket
FROM txn_dist_far_notnull
UNION ALL
SELECT cc_num, amt,
       TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 86400 - 1 AS day_bucket
FROM txn_dist_far_notnull
UNION ALL
SELECT cc_num, amt,
       TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 86400 - 2 AS day_bucket
FROM txn_dist_far_notnull;

CREATE VIEW flagged_repeated_displacement AS
SELECT cc_num,
       TIMESTAMPADD(SECOND,
           day_bucket * 86400,
           TIMESTAMP '1970-01-01 00:00:00') AS window_start,
       COUNT(*) AS distant_count,
       SUM(amt)  AS total_amt
FROM txn_dist_far_bucketed
GROUP BY cc_num, day_bucket
HAVING COUNT(*) >= __DISP__;

-- ── fraud_alerts: unified alert table ────────────────────────────────────────

CREATE VIEW fraud_alerts AS
SELECT cc_num, window_start AS ts, total_amt   AS amt, 'gift_card_burst_30d'   AS signal_type, __PRIO_GB30__ AS priority FROM flagged_gift_card_burst_30d
UNION ALL
SELECT cc_num, window_start AS ts, total_amt   AS amt, 'gift_card_burst_45d'   AS signal_type, __PRIO_GB45__ AS priority FROM flagged_gift_card_burst_45d
UNION ALL
SELECT cc_num, window_start AS ts, total_spend AS amt, 'spend_velocity_7d'     AS signal_type, __PRIO_SV7__  AS priority FROM flagged_spend_velocity_7d
UNION ALL
SELECT cc_num, window_start AS ts, total_amt   AS amt, 'repeated_displacement' AS signal_type, __PRIO_DISP__ AS priority FROM flagged_repeated_displacement;

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

-- fraud_card_latest_txn: full row for that most-recent transaction (with distance + avg_7day).
CREATE VIEW fraud_card_latest_txn AS
SELECT t.cc_num, t.ts, t.amt, t.category, t.shipping_lat, t.shipping_long, t.distance, t.avg_7day
FROM txn_avg7 t
INNER JOIN fraud_card_latest_ts lt ON t.cc_num = lt.cc_num AND t.ts = lt.latest_ts;

-- fraud_alert_details: one row per cc_num (best signal wins) with transaction enrichment.
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
