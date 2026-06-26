-- postgres_ivm — fixed-bucket rollup under a LOW-LATENCY BUDGET. This is a real-time detector: each
-- batch must be scored fast. PostgreSQL has no incremental view maintenance (native MVs are
-- REFRESH-only = full recompute), so the EXACT signals — true rolling window, COUNT(DISTINCT), ordered
-- LAG — would each be an O(N) recompute over the whole growing history and blow the budget. To stay
-- within it the detector is driven to the cheapest shapes: a SINGLE fixed calendar bucket per windowed
-- signal (threshold LOWERED, since one bucket splits boundary-straddling activity → a coarse
-- approximation), and it DROPS the costliest signals — fan-out (a distinct count) and impossible-travel
-- (an ordered LAG). Accuracy traded for latency. (All of it is expressible in standard
-- SQL — just not within the budget without incremental maintenance.) Flag if ANY implemented signal trips.
CREATE VIEW flagged_card AS
WITH flagged AS (
  -- gift-card burst: single 30-day bucket, threshold lowered 23 → 12 (a boundary split is ~13)
  SELECT cc_num FROM transactions WHERE category = 'gift card'
    GROUP BY cc_num, floor(extract(epoch FROM ts) / 2592000) HAVING count(*) >= 12
  UNION
  -- spending velocity: single 7-day bucket, threshold lowered 35 → 18
  SELECT cc_num FROM transactions
    GROUP BY cc_num, floor(extract(epoch FROM ts) / 604800) HAVING count(*) >= 18
  UNION
  -- repeated displacement: single 3-day bucket + equi-join to the static customer dim, 25 → 13
  SELECT t.cc_num FROM transactions t JOIN customer cust ON t.cc_num = cust.cc_num
    WHERE abs(t.shipping_lat - cust.lat) + abs(t.shipping_long - cust.long) > 0.5
    GROUP BY t.cc_num, floor(extract(epoch FROM t.ts) / 259200) HAVING count(*) >= 13
  -- fan-out (distinct count) and impossible-travel (ordered LAG) OMITTED — too costly under the budget.
)
SELECT DISTINCT cc_num FROM flagged;
