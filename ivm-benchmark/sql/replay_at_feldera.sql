-- Fraud detection pipeline for try.feldera.com / HTTP-ingress replay.
-- Tables have no connectors (data is pushed via HTTP ingress).
-- LATENESS is omitted so historical data is never dropped.
--
-- Signal detection uses epoch-floor GROUP BY (not TUMBLE/HOP) to exactly match
-- PostgreSQL-strict semantics: partial windows are included eagerly, not deferred
-- until window_end passes the watermark.

CREATE TABLE CUSTOMER (
    cc_num BIGINT NOT NULL PRIMARY KEY,
    name   VARCHAR,
    lat    DOUBLE,
    long   DOUBLE
) WITH ('materialized' = 'true');

CREATE TABLE TRANSACTION (
    category     VARCHAR,
    ts           TIMESTAMP,
    amt          DECIMAL(38, 2),
    cc_num       BIGINT NOT NULL,
    shipping_lat DOUBLE,
    shipping_long DOUBLE,
    FOREIGN KEY (cc_num) REFERENCES CUSTOMER(cc_num)
) WITH ('materialized' = 'true');

-- ── Views ────────────────────────────────────────────────────────────────────

CREATE VIEW TRANSACTION_WITH_DISTANCE AS
SELECT
    t.*,
    ABS(t.shipping_lat - c.lat) + ABS(t.shipping_long - c.long) AS distance
FROM TRANSACTION AS t
LEFT JOIN CUSTOMER AS c ON t.cc_num = c.cc_num;

CREATE VIEW TRANSACTION_WITH_AGGREGATES AS
SELECT
    *,
    AVG(amt) OVER (
        PARTITION BY cc_num ORDER BY ts
        RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
    ) AS avg_7day
FROM TRANSACTION_WITH_DISTANCE;

CREATE VIEW txn_notnull AS
SELECT * FROM TRANSACTION WHERE ts IS NOT NULL;

CREATE VIEW txn_gift_notnull AS
SELECT * FROM TRANSACTION WHERE ts IS NOT NULL AND category = 'gift card';

CREATE VIEW txn_dist_far_notnull AS
SELECT * FROM TRANSACTION_WITH_DISTANCE WHERE ts IS NOT NULL AND distance > 20.0;

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
SELECT cc_num,
       TIMESTAMPADD(SECOND,
           TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 604800 * 604800,
           TIMESTAMP '1970-01-01 00:00:00') AS window_start,
       COUNT(*) AS txn_count, SUM(amt) AS total_spend
FROM txn_notnull
GROUP BY cc_num, TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', ts) / 604800
HAVING COUNT(*) >= __SV7__;

-- disp: 3-day sliding window via 3-way UNION ALL offset trick — mirrors PG-strict's
-- CROSS JOIN (VALUES (0),(1),(2)) approach.  A far transaction at day D contributes
-- to buckets D, D-1, D-2; bucket B therefore counts all far transactions in [B, B+3d).
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
SELECT cc_num, MAX(priority) AS max_priority
FROM fraud_alerts
GROUP BY cc_num;

-- ── Enriched alert details ────────────────────────────────────────────────────
-- Use TRANSACTION_WITH_DISTANCE (not TRANSACTION_WITH_AGGREGATES) to avoid the
-- 7-day sliding window aggregate in the IVM graph — that window causes O(N) IVM cost
-- as window state grows with history. avg_7day is omitted from the benchmark path.

-- fraud_card_latest_ts: most-recent transaction timestamp per fraud-flagged card.
CREATE VIEW fraud_card_latest_ts AS
SELECT t.cc_num, MAX(t.ts) AS latest_ts
FROM TRANSACTION t
INNER JOIN fraud_alerts a ON t.cc_num = a.cc_num
GROUP BY t.cc_num;

-- fraud_card_latest_txn: full row for that most-recent transaction (with distance).
CREATE VIEW fraud_card_latest_txn AS
SELECT t.cc_num, t.ts, t.amt, t.category, t.shipping_lat, t.shipping_long, t.distance
FROM TRANSACTION_WITH_DISTANCE t
INNER JOIN fraud_card_latest_ts lt ON t.cc_num = lt.cc_num AND t.ts = lt.latest_ts;

-- max_transaction_ts: incrementally maintained so query-time filter is O(1).
CREATE MATERIALIZED VIEW max_transaction_ts AS
SELECT MAX(ts) AS max_ts FROM TRANSACTION;

-- fraud_alert_details: one row per cc_num (best signal wins) with transaction enrichment.
-- No recency filter here — IVM is O(delta). The 2-hour filter is applied at query time
-- (feldera_query.sql) against this small MV, keeping query cost O(flagged_cards).
CREATE MATERIALIZED VIEW fraud_alert_details AS
SELECT
    a.cc_num,
    MIN(a.signal_type)   AS signal_type,
    MAX(a.amt)           AS alert_amt,
    MAX(a.ts)            AS alert_ts,
    MIN(t.ts)            AS ts,
    MIN(t.amt)           AS amt,
    MIN(t.category)      AS category,
    MIN(t.shipping_lat)  AS shipping_lat,
    MIN(t.shipping_long) AS shipping_long,
    MIN(t.distance)      AS distance
FROM fraud_alerts a
INNER JOIN best_per_card b ON a.cc_num = b.cc_num AND a.priority = b.max_priority
INNER JOIN fraud_card_latest_txn t ON a.cc_num = t.cc_num
GROUP BY a.cc_num;
