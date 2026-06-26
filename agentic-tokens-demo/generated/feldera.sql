-- feldera — EXACT signal fidelity. Native IVM maintains the TRUE rolling windows (gift-30d,
-- velocity-7d, displacement-3d) AND an EXACT distinct-location count per day (geographic fan-out)
-- AND an ordered consecutive-pair LAG (impossible travel). Implements all five signals exactly.
-- Runner reads `flagged_card` (the set) and `card_txn` (per-card txn counts, for scoring).
-- DETECTOR ONLY — the CUSTOMER + TRANSACTION tables come from generated/schema.feldera.sql; the
-- runner runs the composed file against them. Defines only the views over those tables.
SET FELDERA_IGNORE_WARNING_UNUSED_COLUMN = 1;

-- signal 1: gift-card burst over a trailing 30-day rolling window
CREATE VIEW gift_roll AS
SELECT cc_num,
       COUNT(*) OVER (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW) AS c30
FROM TRANSACTION WHERE category = 'gift card';

-- signal 2: spending velocity over a trailing 7-day rolling window (any category)
CREATE VIEW vel_roll AS
SELECT cc_num,
       COUNT(*) OVER (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW) AS c7
FROM TRANSACTION;

-- signal 3: repeated displacement over a trailing 3-day rolling window (far from home — needs the join)
CREATE VIEW far_txn AS
SELECT t.cc_num, t.ts
FROM TRANSACTION t JOIN CUSTOMER c ON t.cc_num = c.cc_num
WHERE ABS(t.shipping_lat - c.lat) + ABS(t.shipping_long - c.long) > 0.5;
CREATE VIEW disp_roll AS
SELECT cc_num,
       COUNT(*) OVER (PARTITION BY cc_num ORDER BY ts RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND CURRENT ROW) AS c3
FROM far_txn;

-- signal 5: geographic fan-out — distinct shipping locations (0.1-degree cells) per card per day.
-- Exact distinct via SELECT DISTINCT then COUNT(*).
CREATE VIEW geo_cells AS
SELECT DISTINCT cc_num, CAST(ts AS DATE) AS d, FLOOR(shipping_lat * 10) AS clat, FLOOR(shipping_long * 10) AS clong
FROM TRANSACTION;
CREATE VIEW fanout AS
SELECT cc_num, d, COUNT(*) AS nloc FROM geo_cells GROUP BY cc_num, d;

-- signal 4: impossible travel — compare each purchase to the card's PREVIOUS one (ordered LAG).
-- Fires when consecutive purchases are > 1.0 apart in space yet < 1 hour apart in time.
CREATE VIEW seq AS
SELECT cc_num, ts, shipping_lat, shipping_long,
       LAG(shipping_lat)  OVER w AS plat,
       LAG(shipping_long) OVER w AS plong,
       LAG(ts)            OVER w AS pts
FROM TRANSACTION
WINDOW w AS (PARTITION BY cc_num ORDER BY ts);
CREATE VIEW travel_hit AS
SELECT cc_num FROM seq
WHERE pts IS NOT NULL
  AND ABS(shipping_lat - plat) + ABS(shipping_long - plong) > 1.0
  AND ts < pts + INTERVAL '1' HOUR;

-- a card is suspicious if it trips ANY signal
CREATE MATERIALIZED VIEW flagged_card AS
SELECT DISTINCT cc_num FROM (
    SELECT cc_num FROM gift_roll  WHERE c30 >= 23
    UNION ALL SELECT cc_num FROM vel_roll    WHERE c7 >= 35
    UNION ALL SELECT cc_num FROM disp_roll   WHERE c3 >= 25
    UNION ALL SELECT cc_num FROM fanout      WHERE nloc >= 10
    UNION ALL SELECT cc_num FROM travel_hit
);

CREATE MATERIALIZED VIEW card_txn AS
SELECT cc_num, COUNT(*) AS n FROM TRANSACTION GROUP BY cc_num;
