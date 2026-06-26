-- clickhouse_ivm — REAL incremental materialized views. Per the ClickHouse docs, a MATERIALIZED VIEW
-- is a trigger that runs its SELECT on each newly-inserted block of the source table; with an
-- AggregatingMergeTree target and -State/-Merge combinators, per-key partial aggregates accumulate
-- and merge in the background — maintained incrementally, in proportion to new data (NOT recomputed).
--
-- Each windowed signal is maintained as a FIXED per-(card, bucket) count (countState) — there is no
-- rolling window, so the detector approximates the trailing window at QUERY time by summing two
-- adjacent buckets. Fan-out is a distinct count maintained with uniqState (exact at these small
-- cardinalities). Impossible travel (ordered LAG) is OMITTED — an incremental MV keeps no ordered
-- cross-row state. The MVs trigger on inserts to tok_transactions; tok_customer is a static
-- dimension (loaded before the stream), the only join shape an incremental MV supports.
-- Base tables come from schema.clickhouse_ivm.sql. A card is flagged if it trips ANY signal.

-- signal 1: gift-card count per (card, 30-day bucket)
CREATE MATERIALIZED VIEW tok_gift_mv ENGINE = AggregatingMergeTree ORDER BY (cc_num, bk) AS
SELECT cc_num, intDiv(toUInt32(toUnixTimestamp(ts)), 2592000) AS bk, countState() AS cs
FROM tok_transactions WHERE category = 'gift card' GROUP BY cc_num, bk;

-- signal 2: total count per (card, 7-day bucket)
CREATE MATERIALIZED VIEW tok_vel_mv ENGINE = AggregatingMergeTree ORDER BY (cc_num, bk) AS
SELECT cc_num, intDiv(toUInt32(toUnixTimestamp(ts)), 604800) AS bk, countState() AS cs
FROM tok_transactions GROUP BY cc_num, bk;

-- signal 3: far-from-home count per (card, 3-day bucket) — equi-join to the static customer dimension
CREATE MATERIALIZED VIEW tok_far_mv ENGINE = AggregatingMergeTree ORDER BY (cc_num, bk) AS
SELECT t.cc_num AS cc_num, intDiv(toUInt32(toUnixTimestamp(t.ts)), 259200) AS bk, countState() AS cs
FROM tok_transactions t INNER JOIN tok_customer cust ON t.cc_num = cust.cc_num
WHERE abs(t.shipping_lat - cust.lat) + abs(t.shipping_long - cust.long) > 0.5
GROUP BY t.cc_num, bk;

-- signal 5: distinct shipping locations per (card, day) — uniqState (exact at small counts)
CREATE MATERIALIZED VIEW tok_fan_mv ENGINE = AggregatingMergeTree ORDER BY (cc_num, d) AS
SELECT cc_num, toDate(ts) AS d, uniqState(cityHash64(floor(shipping_lat * 10), floor(shipping_long * 10))) AS us
FROM tok_transactions GROUP BY cc_num, d;

-- flagged_card: read the maintained partial states (-Merge), approximate each rolling window by
-- summing two ADJACENT fixed buckets at the true threshold, UNION the signals.
CREATE VIEW flagged_card AS
WITH
g30 AS (SELECT cc_num, bk, countMerge(cs) AS c FROM tok_gift_mv GROUP BY cc_num, bk),
vel AS (SELECT cc_num, bk, countMerge(cs) AS c FROM tok_vel_mv  GROUP BY cc_num, bk),
far AS (SELECT cc_num, bk, countMerge(cs) AS c FROM tok_far_mv  GROUP BY cc_num, bk),
fan AS (SELECT cc_num, d,  uniqMerge(us)  AS u FROM tok_fan_mv  GROUP BY cc_num, d)
SELECT DISTINCT cc_num FROM (
    SELECT a.cc_num FROM g30 a LEFT JOIN g30 b ON a.cc_num = b.cc_num AND b.bk = a.bk - 1 WHERE a.c + b.c >= 23
    UNION DISTINCT
    SELECT a.cc_num FROM vel a LEFT JOIN vel b ON a.cc_num = b.cc_num AND b.bk = a.bk - 1 WHERE a.c + b.c >= 35
    UNION DISTINCT
    SELECT a.cc_num FROM far a LEFT JOIN far b ON a.cc_num = b.cc_num AND b.bk = a.bk - 1 WHERE a.c + b.c >= 25
    UNION DISTINCT
    SELECT cc_num FROM fan WHERE u >= 10
);
