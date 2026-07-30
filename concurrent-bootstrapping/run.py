#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "feldera",
#     "pyarrow>=17",
# ]
# ///
"""
Concurrent bootstrapping demo driver (Feldera).

Two identical pipelines are backfilled with a large clickstream dataset and
checkpointed. Then new, expensive analytical views are added and the pipelines
are restarted so Feldera *bootstraps* those views from the checkpointed state:

  - bootstrap-conventional : stop-the-world bootstrap (every view freezes)
  - bootstrap-concurrent   : concurrent bootstrap (existing views stay live)

A live datagen stream keeps feeding both pipelines during the bootstrap window,
so the dashboard shows the difference: conventional freezes all panels; concurrent
keeps the pre-existing panels updating while the new ones stay silent until the
bootstrap finishes.

Commands:
  setup      create both pipelines, backfill + checkpoint the base dataset
  bootstrap  add the new views and restart both pipelines (fires the demo)
  reset      roll both pipelines back to the pre-bootstrap checkpoint
  serve      run the side-by-side web dashboard
  status     print deployment / bootstrap state of both pipelines
  teardown   delete both pipelines and their storage

Run with the Feldera Python SDK available (see README).  All Feldera interaction
goes through the SDK; nothing here calls the REST API directly.
"""
import argparse
import sys
import time
from datetime import date, timedelta

from feldera import FelderaClient, PipelineBuilder, Pipeline
from feldera.runtime_config import RuntimeConfig
from feldera.enums import BootstrapPolicy

HOST = "http://localhost:8080"
CONVENTIONAL = "bootstrap-conventional"
CONCURRENT = "bootstrap-concurrent"
PIPELINES = [CONVENTIONAL, CONCURRENT]

# Dataset knobs (override on the CLI).
DEFAULT_SIZE = 20_000_000     # backfilled clickstream events per pipeline (~25-30s bootstrap)
BULK_CONNECTORS = 8           # parallel datagen connectors (throughput ~= N x single)
LIVE_RATE = 2_000             # live clickstream events/sec during the demo
LIVE_ORDER_RATE = 500         # live orders/sec -- drives the existing views that stay live
WORKERS = 8

EVENT_TYPES = '["page_view","product_view","add_to_cart","begin_checkout","purchase"]'
DEVICES = '["mobile","desktop","tablet"]'
COUNTRIES = '["US","UK","DE","FR","IN","BR","CA","AU"]'
# A recent 14-day window: keeps the "by day" dashboard histograms (order revenue,
# daily reach) focused on a handful of days that all visibly grow as the stream flows.
TS_START = date(2025, 11, 17)
TS_DAYS = 14
TS_RANGE = (f'["{TS_START.isoformat()}T00:00:00Z",'
            f'"{(TS_START + timedelta(TS_DAYS)).isoformat()}T00:00:00Z"]')


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------
# Keyspace is deliberately bounded so the new views have a predictable size
# (uniform user x product over a huge keyspace would explode their cardinality and
# make bootstrap time scale with output size rather than input size).
N_USERS = 10_000
N_PRODUCTS = 500


def _clickstream_fields(id_lo, id_hi, event_ts=None):
    event_ts = event_ts or f'{{"strategy":"uniform","range":{TS_RANGE}}}'
    return f'''"event_id":{{"strategy":"increment","range":[{id_lo},{id_hi}]}},
        "user_id":{{"strategy":"uniform","range":[1,{N_USERS + 1}]}},
        "product_id":{{"strategy":"uniform","range":[1,{N_PRODUCTS + 1}]}},
        "event_type":{{"values":{EVENT_TYPES}}},
        "device_type":{{"values":{DEVICES}}},
        "geo_country":{{"values":{COUNTRIES}}},
        "event_timestamp":{event_ts},
        "ingested_at":{{"strategy":"uniform","range":{TS_RANGE}}}'''


def _bulk_connector(k, per):
    """One of BULK_CONNECTORS parallel generators; disjoint event_id ranges keep the PK unique."""
    base = k * 10_000_000_000
    return (f'{{"name":"bulk-{k}","paused":false,"transport":{{"name":"datagen","config":{{'
            f'"plan":[{{"limit":{per},"fields":{{{_clickstream_fields(base, base + 10_000_000_000)}}}}}]}}}}}}')


# Live clicks are skewed across the backfilled days so "clicks by day" reads like real
# daily traffic (weekday-high, weekend-low, gently rising) instead of a flat wall. The
# backfill stays uniform, so this shape is exactly what the post-cutover delta reveals.
_DAY_WEIGHTS = [10, 11, 11, 12, 12, 6, 5, 13, 14, 14, 15, 15, 8, 7]


def _live_ts_values():
    vals = []
    for i, w in enumerate(_DAY_WEIGHTS[:TS_DAYS]):
        ts = f'"{(TS_START + timedelta(i)).isoformat()}T12:00:00Z"'
        vals.extend([ts] * w)
    return "[" + ",".join(vals) + "]"


def _live_clickstream_connector():
    """Unbounded low-rate live stream; high event_id range avoids colliding with bulk.
    event_timestamp is skewed toward recent days (see _DAY_WEIGHTS) so the live-delta
    'clicks by day' histogram has a realistic daily shape instead of a flat wall."""
    event_ts = f'{{"values":{_live_ts_values()}}}'
    return (f'{{"name":"live","transport":{{"name":"datagen","config":{{'
            f'"plan":[{{"rate":{LIVE_RATE},"fields":{{{_clickstream_fields(900_000_000_000, 999_000_000_000, event_ts)}}}}}]}}}}}}')


def _live_orders_connector():
    # Repeat values to weight the mix (most orders delivered/confirmed, few cancelled/
    # returned) so the "orders by status" histogram looks like a real order book.
    statuses = ('["delivered","delivered","delivered","delivered","confirmed","confirmed",'
                '"confirmed","shipped","shipped","pending","pending","cancelled","returned"]')
    fields = (f'"order_id":{{"strategy":"increment","range":[1,999999999]}},'
              f'"user_id":{{"strategy":"uniform","range":[1,{N_USERS + 1}]}},'
              f'"order_status":{{"values":{statuses}}},'
              f'"order_total":{{"strategy":"uniform","range":[10,500]}},'
              f'"created_at":{{"strategy":"uniform","range":{TS_RANGE}}}')
    return (f'{{"name":"live-orders","transport":{{"name":"datagen","config":{{'
            f'"plan":[{{"rate":{LIVE_ORDER_RATE},"fields":{{{fields}}}}}]}}}}}}')


def build_sql(size, heavy):
    """v1 (heavy=False): tables + existing live views.  v2 (heavy=True): + new expensive views."""
    per = size // BULK_CONNECTORS
    clickstream_conns = "[" + ",".join(
        [_bulk_connector(k, per) for k in range(BULK_CONNECTORS)] + [_live_clickstream_connector()]
    ) + "]"

    # The table section below is byte-identical between v1 and v2 so the only
    # difference is the appended views -- the precondition concurrent bootstrap needs.
    sql = f"""-- ============================================================================
-- Concurrent bootstrapping demo
-- All tables are materialized so new views can bootstrap from checkpointed state.
-- No LATENESS anywhere: bootstrapping is incompatible with LATENESS.
-- ============================================================================

CREATE TABLE clickstream (
    event_id BIGINT NOT NULL PRIMARY KEY,
    user_id BIGINT,
    session_id VARCHAR,
    event_type VARCHAR,
    product_id BIGINT,
    device_type VARCHAR,
    geo_country VARCHAR,
    event_timestamp TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL
) WITH ( 'materialized' = 'true', 'connectors' = '{clickstream_conns}' );

CREATE TABLE orders (
    order_id BIGINT NOT NULL PRIMARY KEY,
    user_id BIGINT,
    order_status VARCHAR,
    order_total DECIMAL(12,2),
    created_at TIMESTAMP
) WITH ( 'materialized' = 'true', 'connectors' = '[{_live_orders_connector()}]' );

-- ---- EXISTING views over `orders`. They are INDEPENDENT of the new views (which
-- ---- aggregate `clickstream`), so a concurrent bootstrap keeps them fully live on the
-- ---- order stream and ad-hoc queries return correct, updating values throughout.
-- ---- (A view over `clickstream` would instead be pulled into the bootstrap, since that
-- ---- table is replayed to backfill the new views -- the "unmodified views may need
-- ---- bootstrapping" caveat -- and would read empty until the cutover.)
CREATE MATERIALIZED VIEW gold_live_orders AS
SELECT order_status, COUNT(*) AS orders, SUM(order_total) AS revenue
FROM orders
GROUP BY order_status;

CREATE MATERIALIZED VIEW gold_order_revenue AS
SELECT CAST(created_at AS DATE) AS day, COUNT(*) AS orders, SUM(order_total) AS revenue
FROM orders
GROUP BY CAST(created_at AS DATE);
"""

    if heavy:
        sql += """
-- ---- NEW views: expensive first build; silent until bootstrapping completes ----
-- Every view below scans the entire backfilled history once to build its initial
-- contents. That first build is what "bootstrapping" does, and what takes time.

-- Per (user, product) engagement matrix, with an exact distinct-event-type count.
CREATE MATERIALIZED VIEW gold_user_product_engagement AS
SELECT user_id, product_id,
       COUNT(*) AS events,
       COUNT(DISTINCT event_type) AS distinct_event_types,
       SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
FROM clickstream
WHERE user_id IS NOT NULL AND product_id IS NOT NULL
GROUP BY user_id, product_id;

-- Per product per day reach with an exact distinct-user count.
CREATE MATERIALIZED VIEW gold_product_daily_reach AS
SELECT product_id, CAST(event_timestamp AS DATE) AS day,
       COUNT(*) AS events,
       COUNT(DISTINCT user_id) AS unique_users
FROM clickstream
GROUP BY product_id, CAST(event_timestamp AS DATE);

-- Per-user lifetime profile with exact distinct counts over the whole history.
CREATE MATERIALIZED VIEW gold_user_stats AS
SELECT user_id,
       COUNT(*) AS events,
       COUNT(DISTINCT product_id) AS products_touched,
       COUNT(DISTINCT CAST(event_timestamp AS DATE)) AS active_days,
       SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
FROM clickstream
WHERE user_id IS NOT NULL
GROUP BY user_id;
"""
    return sql


# View classification for the dashboard.
EXISTING_VIEWS = ["gold_live_orders", "gold_order_revenue"]
NEW_VIEWS = ["gold_product_daily_reach", "gold_user_product_engagement", "gold_user_stats"]


# ---------------------------------------------------------------------------
# Lifecycle helpers
# ---------------------------------------------------------------------------
def client():
    return FelderaClient(HOST)


def _runtime(p):
    """Deployment runtime status as a bare string; the SDK raises on a transient None."""
    try:
        s = p.deployment_runtime_status()
        return str(s).split(".")[-1] if s is not None else "unknown"
    except Exception:
        return "unknown"


def _wait_program(p, timeout=300):
    t = time.time()
    while time.time() - t < timeout:
        if "Success" in str(p.program_status()):
            return
        if "Error" in str(p.program_status()) or "Failed" in str(p.program_status()):
            raise RuntimeError(f"compilation failed: {p.program_status()}")
        time.sleep(3)
    raise TimeoutError("program compile timeout")


def _bulk_done(stats):
    """True once every named bulk connector has reached end_of_input."""
    bulk = [i for i in stats.inputs if "bulk-" in (i.endpoint_name or "")]
    return bool(bulk) and all(i.metrics.end_of_input for i in bulk)


def _generate_and_checkpoint(p, size):
    """Start the pipeline, wait for the bulk generators to finish, checkpoint, stop."""
    p.start(wait=True)
    t = time.time()
    last = 0
    while True:
        s = p.stats()
        tot = s.global_metrics.total_input_records or 0
        if _bulk_done(s):
            break
        if time.time() - t > 3600:
            raise TimeoutError("generation timeout")
        if tot - last >= 50_000_000:
            print(f"    {p.name}: {tot:,} rows ({time.time()-t:.0f}s, "
                  f"{(s.global_metrics.storage_bytes or 0)/1e9:.1f}GB)")
            last = tot
        time.sleep(1)
    print(f"    {p.name}: generated {p.stats().global_metrics.total_input_records:,} in {time.time()-t:.0f}s")
    # never checkpoint mid-transaction (that deadlocks the checkpoint)
    while str(p.stats().global_metrics.transaction_status).split(".")[-1] != "NoTransaction":
        time.sleep(0.5)
    p.stop(force=False, wait=True)   # force=False => checkpoint before stopping


def cmd_setup(args):
    c = client()
    for name in PIPELINES:
        print(f"[setup] {name}: creating v1 + backfilling {args.size:,} events...")
        p = PipelineBuilder(
            c, name, sql=build_sql(args.size, heavy=False),
            runtime_config=RuntimeConfig(workers=WORKERS, storage=True),
        ).create_or_replace()
        _generate_and_checkpoint(p, args.size)
        print(f"[setup] {name}: checkpointed and stopped.")
    print("[setup] done. Run `run.py serve` and then `run.py bootstrap`.")


def cmd_bootstrap(args):
    c = client()
    pipes = {n: Pipeline.get(n, c) for n in PIPELINES}
    # Stage the v2 program on both while stopped (keeps the checkpoint).
    for n, p in pipes.items():
        p.modify(sql=build_sql(args.size, heavy=True))
    for n, p in pipes.items():
        _wait_program(p)
    print("[bootstrap] both pipelines staged with new views; firing bootstrap...")
    # Fire both nearly simultaneously; wait=False so the demo window is observable.
    pipes[CONVENTIONAL].start(bootstrap_policy=BootstrapPolicy.ALLOW, concurrent_bootstrap=False, wait=False)
    pipes[CONCURRENT].start(bootstrap_policy=BootstrapPolicy.ALLOW, concurrent_bootstrap=True, wait=False)
    print("[bootstrap] fired. Watch the dashboard: conventional freezes, concurrent stays live.")


def cmd_reset(args):
    c = client()
    for n in PIPELINES:
        p = Pipeline.get(n, c)
        p.stop(force=True, wait=True)   # force stop => no new checkpoint; rolls back to the pre-bootstrap one
        print(f"[reset] {n}: rolled back to pre-bootstrap checkpoint.")


def cmd_status(args):
    c = client()
    for n in PIPELINES:
        p = Pipeline.get(n, c)
        try:
            g = p.stats().global_metrics
            phase = g.concurrent_bootstrap_phase
            prog = g.concurrent_bootstrap_progress
        except Exception:
            phase = prog = None
        print(f"  {n:26s} deploy={str(p.status()).split('.')[-1]:10s} runtime={_runtime(p):22s} "
              f"cbphase={phase} progress={prog}")


def cmd_teardown(args):
    c = client()
    for n in PIPELINES:
        try:
            p = Pipeline.get(n, c)
            p.stop(force=True, wait=True)
            p.delete(clear_storage=True)
            print(f"[teardown] deleted {n}")
        except Exception as e:
            print(f"[teardown] {n}: {e}")


def cmd_serve(args):
    from dashboard_server import serve
    serve(HOST, PIPELINES, EXISTING_VIEWS, NEW_VIEWS, port=args.port, size=args.size)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in [("setup", cmd_setup), ("bootstrap", cmd_bootstrap), ("reset", cmd_reset),
                     ("status", cmd_status), ("teardown", cmd_teardown), ("serve", cmd_serve)]:
        s = sub.add_parser(name)
        s.set_defaults(fn=fn)
        if name in ("setup", "bootstrap", "serve"):
            s.add_argument("--size", type=int, default=DEFAULT_SIZE,
                           help="backfilled clickstream events per pipeline "
                                "(serve uses it to pace the live-fill bars)")
        if name == "serve":
            s.add_argument("--port", type=int, default=8090)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
