#!/usr/bin/env python3
"""gen_demo_data.py — planted-fraud streaming dataset, built on rolling-aggregate-bench/src/gen_data.py.

REALISTIC setup: the detector logic is IDENTICAL across all engines (gift-card count over a
trailing 30-day window >= 23). What differs is SIGNAL FIDELITY — Feldera maintains the true
rolling window exactly; ClickHouse/Postgres can only maintain fixed calendar buckets, so they
approximate the signal. To avoid MISSING boundary-straddling fraud, the bucket proxy must
over-cover (sum adjacent buckets ~= 60 days), and that over-coverage OVER-COUNTS legitimate
cards whose activity is spread out — producing false positives at the SAME threshold. No
per-engine threshold tuning; the over-approximation is a property of the noisy signal.

Three planted populations live in the streaming batches (a 60-day window):
  sharp-burst fraud (cc 999...) — dense gift burst; true 30d rolling >> 23; everyone flags it.
  slow-burn fraud  (cc 998...) — ~26 gift/day straddling a 30-day bucket boundary; true rolling
                                 >= 23 (Feldera flags). A single fixed bucket SPLITS it (~13/13)
                                 and would MISS it — which is why the proxy over-covers.
  borderline-legit (cc 200...) — ~28 gift spread evenly over ~52 days: true 30-day rolling stays
                                 BELOW 23 (exact CLEARS them) but the over-covering ~60-day
                                 proxy sums all ~28 >= 23 (approx FLAGS them) → FALSE POSITIVES.

Also writes labels.csv (cc_num,label) for fraud + borderline cards so the runner can score each
engine's flags against the exact ground truth (precision / recall, false-positive cost).
"""

import argparse
import csv
import random
import sys
from datetime import datetime, timedelta, date
from pathlib import Path

_BENCH = Path(__file__).resolve().parent.parent.parent / "rolling-aggregate-bench" / "src"
sys.path.insert(0, str(_BENCH))
from gen_data import write_customers, TXN_FIELDS   # noqa: E402  (bench writer + schema)

NORMAL_BASE     = 100_000_000_000_000
BORDERLINE_BASE = 200_000_000_000_000
NOISE_BASE      = 300_000_000_000_000
TRAVEL_BASE     = 996_000_000_000_000
FANOUT_BASE     = 997_000_000_000_000
SLOWBURN_BASE   = 998_000_000_000_000
SHARP_BASE      = 999_000_000_000_000
CATEGORIES       = ["gift card", "grocery", "travel", "games"]
FRAUD_CATEGORIES = ["gift card", "grocery", "travel"]
TS_START         = datetime(2024, 8, 26)   # stream start; a 30-day epoch boundary (2024-09-14)
                                            # falls mid-window so slow-burn can straddle it

HOME_LAT, HOME_LONG             = (25.0, 50.0), (-100.02, -99.98)
FRAUD_HOME_LAT, FRAUD_HOME_LONG = (35.0, 45.0), (-80.0, -70.0)
SHIP_LAT, SHIP_LONG             = (25.0, 50.0), (-126.0, -70.0)

_FIRST = ["Eula", "Brandy", "Otis", "Lena", "Marcus", "Priya", "Dale", "Nina", "Cleo", "Hank"]
_LAST  = ["Steuber", "Huels", "Quigley", "Lemke", "Nader", "Hyatt", "Larkin", "Dooley", "Hahn", "Torp"]
AMT_MIX = [(0.46, 0, 50), (0.07, 50, 100), (0.04, 100, 150), (0.12, 150, 500),
           (0.07, 500, 1000), (0.11, 1000, 3000), (0.07, 3000, 6000), (0.06, 6000, 10000)]


def _name(r): return f"{r.choice(_FIRST)} {r.choice(_LAST)}"
def _fmt(dt): return dt.strftime("%Y-%m-%d %H:%M:%S")
def _eday(dt): return (dt.date() - date(1970, 1, 1)).days


def _amt(r):
    u, c = r.random(), 0.0
    for p, lo, hi in AMT_MIX:
        c += p
        if u <= c:
            return round(r.uniform(lo, hi), 2)
    return round(r.uniform(0, 50), 2)


def generate_customers(n_normal, n_sharp, n_slow, n_borderline, n_fanout, n_travel, n_noise, rng):
    rows, homes = [], {}
    for i in range(n_normal):
        cc = NORMAL_BASE + i
        lat, lon = round(rng.uniform(*HOME_LAT), 6), round(rng.uniform(*HOME_LONG), 6)
        homes[cc] = (lat, lon)
        rows.append({"cc_num": cc, "name": _name(rng), "lat": lat, "long": lon})
    # busy-but-legit gift buyers (cc 300…) — genuinely cross the gift threshold in a true 30-day
    # window, so EVERY engine (Feldera included) flags them. They are the RULE's own false positives
    # (the threshold can't tell a heavy legit buyer from a launderer) → a realistic shared baseline.
    noise_ids = [NOISE_BASE + i for i in range(n_noise)]
    for cc in noise_ids:
        lat, lon = round(rng.uniform(*HOME_LAT), 6), round(rng.uniform(*HOME_LONG), 6)
        homes[cc] = (lat, lon)
        rows.append({"cc_num": cc, "name": _name(rng), "lat": lat, "long": lon})
    # borderline-legit "steady gift-card resellers": legitimate, ship near home — their gift
    # volume's TRUE 30-day rolling count stays under threshold, but the over-covering bucket
    # proxy sums it over the threshold. These are the false-positive source (the gap-maker).
    borderline_ids = [BORDERLINE_BASE + i for i in range(n_borderline)]
    for cc in borderline_ids:
        lat, lon = round(rng.uniform(*HOME_LAT), 6), round(rng.uniform(*HOME_LONG), 6)
        homes[cc] = (lat, lon)
        rows.append({"cc_num": cc, "name": _name(rng), "lat": lat, "long": lon})
    sharp_ids  = [SHARP_BASE + (k + 1) for k in range(n_sharp)]
    slow_ids   = [SLOWBURN_BASE + (k + 1) for k in range(n_slow)]
    # geographic fan-out fraud (cc 997…): one card shipped to many DISTINCT locations in a day.
    fanout_ids = [FANOUT_BASE + (k + 1) for k in range(n_fanout)]
    # impossible-travel fraud (cc 996…): consecutive purchases far apart in space, minutes apart.
    travel_ids = [TRAVEL_BASE + (k + 1) for k in range(n_travel)]
    for cc in sharp_ids + slow_ids + fanout_ids + travel_ids:
        lat, lon = round(rng.uniform(*FRAUD_HOME_LAT), 6), round(rng.uniform(*FRAUD_HOME_LONG), 6)
        homes[cc] = (lat, lon)
        rows.append({"cc_num": cc, "name": _name(rng), "lat": lat, "long": lon})
    return rows, homes, sharp_ids, slow_ids, borderline_ids, fanout_ids, travel_ids, noise_ids


def _normal_txn(rng, dt, n_normal, homes):
    # normal customers ship NEAR their own home (consistent addresses) — so consecutive purchases
    # are never far apart (no spurious impossible-travel), shipping is never far from home (no
    # spurious displacement), and a day's locations cluster (no spurious fan-out).
    # Jitter is ±0.24/axis so two normal points are at most 0.96 (L1) apart — strictly below the
    # 1.0 impossible-travel threshold — and any single point is at most 0.48 from home — below the
    # 0.5 "far" threshold. So normals CANNOT trip any signal at any data scale → Feldera stays 0 FP.
    cc = NORMAL_BASE + rng.randrange(n_normal)
    hlat, hlon = homes[cc]
    return {"category": rng.choice(CATEGORIES), "ts": _fmt(dt), "amt": _amt(rng), "cc_num": cc,
            "shipping_lat": round(hlat + rng.uniform(-0.24, 0.24), 6),
            "shipping_long": round(hlon + rng.uniform(-0.24, 0.24), 6)}


def write_stream_batches(batches_dir, n_normal, sharp_ids, slow_ids, burst, sb_gift,
                         borderline_ids, bl_gift, bl_span, fanout_ids, fo_locs,
                         travel_ids, noise_ids, homes, stream_days, n_batches, batch_rows, rng):
    """Streaming window: normal traffic + planted fraud + borderline-legit, merged by time."""
    batches_dir.mkdir(parents=True, exist_ok=True)
    stream_start = TS_START
    stream_end   = stream_start + timedelta(days=stream_days)

    fraud = []
    # sharp-burst fraud — dense gift burst staggered across the window
    slice_secs = stream_days * 86400 / max(1, len(sharp_ids))
    for k, cc in enumerate(sharp_ids):
        start = stream_start + timedelta(seconds=k * slice_secs + slice_secs * 0.1)
        for m in range(burst):
            ts = start + timedelta(minutes=m)
            fraud.append((ts, {"category": FRAUD_CATEGORIES[m % 3], "ts": _fmt(ts),
                "amt": round(rng.uniform(100, 5000), 2), "cc_num": cc,
                "shipping_lat": round(rng.uniform(*SHIP_LAT), 6),
                "shipping_long": round(rng.uniform(*SHIP_LONG), 6)}))
    # slow-burn fraud — ~26 gift/day straddling the 30-day epoch boundary
    boundary = next((stream_start + timedelta(days=o) for o in range(stream_days)
                     if _eday(stream_start + timedelta(days=o)) % 30 == 0),
                    stream_start + timedelta(days=stream_days // 2))
    for cc in slow_ids:
        for i in range(sb_gift):
            day = boundary + timedelta(days=i - sb_gift // 2)
            if stream_start <= day < stream_end:
                ts = day.replace(hour=12)
                fraud.append((ts, {"category": "gift card", "ts": _fmt(ts),
                    "amt": round(rng.uniform(100, 1500), 2), "cc_num": cc,
                    "shipping_lat": round(rng.uniform(*SHIP_LAT), 6),
                    "shipping_long": round(rng.uniform(*SHIP_LONG), 6)}))
    n_fraud = len(fraud)

    # borderline-legit: bl_gift gift txns spread EVENLY over bl_span days (bl_span > 30) so the
    # true trailing-30-day rolling count peaks at ~bl_gift*30/bl_span (< threshold), while a wider
    # fixed ~60-day bucket sums all bl_gift (>= threshold). Near-home, so they trip no other signal —
    # legitimate cards that sit between the two ways of measuring the same window.
    # STAGGER each card's start across the window (leaving room for its bl_span span) so the cards
    # don't all fill their fixed 30-day buckets in lockstep — that spreads the IVM engines' flip
    # events (and thus the per-step suspicious-txn spikes) across the steps instead of one big burst.
    bl_room_days = max(0.0, stream_days - bl_span - 1)
    for cc in borderline_ids:
        hlat, hlon = homes[cc]
        g = rng.randint(bl_gift - 8, bl_gift + 6)   # cards differ in how many gifts they make
        bl_start = stream_start + timedelta(days=1 + rng.uniform(0, bl_room_days))
        for i in range(g):
            ts = bl_start + timedelta(seconds=(i + 0.5) * bl_span * 86400 / g)
            fraud.append((ts, {"category": "gift card", "ts": _fmt(ts),
                "amt": round(rng.uniform(50, 500), 2), "cc_num": cc,
                "shipping_lat": round(hlat + rng.uniform(-0.2, 0.2), 6),
                "shipping_long": round(hlon + rng.uniform(-0.2, 0.2), 6)}))
    n_border = len(fraud) - n_fraud

    # geographic fan-out: fo_locs txns on ONE day, each to a DISTINCT location (distinct
    # 0.1-degree cells). Trips ONLY the distinct-location signal — not gift (non-gift category),
    # not velocity (<35), not displacement (<25 in 3 days). Feldera + ClickHouse (uniq) catch it;
    # the Postgres bucketed rollup (no distinct count) cannot, so it misses this fraud.
    fo_day = stream_start + timedelta(days=10)
    for cc in fanout_ids:
        for i in range(fo_locs):
            ts = fo_day + timedelta(hours=min(23, i), minutes=(i * 7) % 60)
            lat = 26.0 + i * (22.0 / max(1, fo_locs))    # spread ~26..48
            lon = -125.0 + i * (33.0 / max(1, fo_locs))  # spread ~-125..-92 (far from home)
            fraud.append((ts, {"category": "games", "ts": _fmt(ts),
                "amt": round(rng.uniform(50, 800), 2), "cc_num": cc,
                "shipping_lat": round(lat, 6), "shipping_long": round(lon, 6)}))
    n_fanout = len(fraud) - n_fraud - n_border

    # impossible travel: 2 consecutive purchases ~20 apart in space, 30 min apart in time — the
    # implied speed is impossible. Trips ONLY the consecutive-pair (LAG) signal. cc 996…
    tr_day = stream_start + timedelta(days=20)
    for j, cc in enumerate(travel_ids):
        t0 = tr_day + timedelta(minutes=(j * 13) % 1000)
        for lat, lon, dt in ((30.0, -100.0, t0), (40.0, -90.0, t0 + timedelta(minutes=30))):
            fraud.append((dt, {"category": "travel", "ts": _fmt(dt),
                "amt": round(rng.uniform(50, 800), 2), "cc_num": cc,
                "shipping_lat": lat, "shipping_long": lon}))
    n_travel = len(fraud) - n_fraud - n_border - n_fanout

    # busy-but-legit: 18–40 gift txns within a ~24-day span (< 30 days) so the TRUE rolling-30d count
    # equals the total → those above the threshold trip the gift rule in EVERY engine (shared, the
    # RULE's own false positives, not the engine's). Near-home, so no other signal fires.
    nz_start = stream_start + timedelta(days=5)
    for cc in noise_ids:
        hlat, hlon = homes[cc]
        g = rng.randint(18, 40)
        for i in range(g):
            ts = nz_start + timedelta(seconds=(i + 0.5) * 24 * 86400 / g)
            fraud.append((ts, {"category": "gift card", "ts": _fmt(ts),
                "amt": round(rng.uniform(50, 500), 2), "cc_num": cc,
                "shipping_lat": round(hlat + rng.uniform(-0.2, 0.2), 6),
                "shipping_long": round(hlon + rng.uniform(-0.2, 0.2), 6)}))
    n_noise = len(fraud) - n_fraud - n_border - n_fanout - n_travel
    fraud.sort(key=lambda r: r[0])

    total = n_batches * batch_rows
    n_normal_txns = max(0, total - len(fraud))
    step = (stream_days * 86400) / max(1, n_normal_txns)

    def _rows():
        fj = 0
        for i in range(n_normal_txns):
            dt = stream_start + timedelta(seconds=i * step)
            while fj < len(fraud) and fraud[fj][0] <= dt:
                yield fraud[fj][1]; fj += 1
            yield _normal_txn(rng, dt, n_normal, homes)
        while fj < len(fraud):
            yield fraud[fj][1]; fj += 1

    it = _rows()
    written = 0
    for b in range(n_batches):
        with open(batches_dir / f"batch_{b+1:04d}.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=TXN_FIELDS); w.writeheader()
            for _ in range(batch_rows):
                try:
                    w.writerow(next(it)); written += 1
                except StopIteration:
                    break
    print(f"  {batches_dir}/  ({n_batches} batches x {batch_rows:,} = {written:,} rows; "
          f"{n_fraud:,} fraud + {n_fanout:,} fan-out + {n_travel:,} travel + {n_border:,} borderline + "
          f"{n_noise:,} busy-legit txns)")
    return n_fraud, n_border, n_fanout, n_travel, n_noise, boundary


def write_labels(path, sharp_ids, slow_ids, fanout_ids, travel_ids, borderline_ids):
    """cc_num -> label for the planted special cards (everything else is 'normal')."""
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["cc_num", "label"])
        for cc in sharp_ids:      w.writerow([cc, "fraud"])
        for cc in slow_ids:       w.writerow([cc, "fraud"])
        for cc in fanout_ids:     w.writerow([cc, "fraud"])
        for cc in travel_ids:     w.writerow([cc, "fraud"])
        for cc in borderline_ids: w.writerow([cc, "borderline"])
    print(f"  {path}  ({len(sharp_ids)+len(slow_ids)+len(fanout_ids)+len(travel_ids)} fraud + {len(borderline_ids)} borderline labels)")


# logical schema → per-engine DDL, generated from the ACTUAL columns the generator writes so the
# table/column contract is always correct and can be checked visually against the data.
_TXN_TYPES = {
    "feldera":        {"category": "VARCHAR", "ts": "TIMESTAMP", "amt": "DECIMAL(38, 2)", "cc_num": "BIGINT NOT NULL",
                       "shipping_lat": "DOUBLE", "shipping_long": "DOUBLE"},
    "postgres_ivm":   {"category": "text", "ts": "timestamp", "amt": "double precision", "cc_num": "bigint",
                       "shipping_lat": "double precision", "shipping_long": "double precision"},
    "clickhouse_ivm": {"category": "String", "ts": "DateTime", "amt": "Float64", "cc_num": "Int64",
                       "shipping_lat": "Float64", "shipping_long": "Float64"},
}
_CUST_DDL = {
    "feldera":        "cc_num BIGINT NOT NULL PRIMARY KEY, name VARCHAR, lat DOUBLE, long DOUBLE",
    "postgres_ivm":   "cc_num bigint PRIMARY KEY, name text, lat double precision, long double precision",
    "clickhouse_ivm": "cc_num Int64, name String, lat Float64, long Float64",
}
_TBL = {"feldera": ("TRANSACTION", "CUSTOMER"), "postgres_ivm": ("transactions", "customer"),
        "clickhouse_ivm": ("tok_transactions", "tok_customer")}
_TXN_SUFFIX  = {"feldera": " WITH ('materialized' = 'true')", "postgres_ivm": "",
                "clickhouse_ivm": " ENGINE = MergeTree ORDER BY (cc_num, ts)"}
_CUST_SUFFIX = {"feldera": " WITH ('materialized' = 'true')", "postgres_ivm": "",
                "clickhouse_ivm": " ENGINE = MergeTree ORDER BY cc_num"}
# table-level extras (e.g. Feldera foreign key) appended inside the column list
_TXN_EXTRA   = {"feldera": ",\n  FOREIGN KEY (cc_num) REFERENCES CUSTOMER(cc_num)",
                "postgres_ivm": "", "clickhouse_ivm": ""}


def write_schema():
    """Emit the COMPLETE per-engine program, ONE FILE PER ENGINE, into the **`generated/`** folder
    (`generated/schema.<engine>.sql`) — the SAME folder as the detector views (`generated/<engine>.sql`)
    they are composed from, so all the SQL lives in one place (separate from the CSV data). Each file
    is the table DDL (generated from the columns actually written) FOLLOWED BY the detector's
    `flagged_card` view; the runner reads ONLY this file per engine and hardcodes no DDL. Customer
    table is written first, transactions second, views last."""
    gen_dir = Path(__file__).resolve().parent.parent / "generated"   # demo root (this file lives in src/)
    gen_dir.mkdir(parents=True, exist_ok=True)
    hdr = ("-- GENERATED by gen_demo_data.py — schema of customers.csv + batches/*.csv.\n"
           f"-- transaction columns (in CSV order): {', '.join(TXN_FIELDS)}\n"
           "-- customer columns: cc_num, name, lat, long   (lat/long = cardholder HOME)\n"
           "-- 'far from home' = |shipping_lat - lat| + |shipping_long - long| > 0.5\n\n")
    missing = []
    for eng in ("feldera", "postgres_ivm", "clickhouse_ivm"):
        txn_tbl, cust_tbl = _TBL[eng]
        cols = ",\n  ".join(f"{c} {_TXN_TYPES[eng][c]}" for c in TXN_FIELDS)
        det_path = gen_dir / f"{eng}.sql"
        detector = det_path.read_text().strip() if det_path.exists() else None
        if detector is None:
            missing.append(eng)
        with open(gen_dir / f"schema.{eng}.sql", "w") as f:
            f.write(hdr)
            if detector:
                f.write(f"-- {eng} — COMPLETE program: tables (below) + detector view(s) (further down).\n")
                f.write(f"-- The runner creates everything from THIS file and queries `flagged_card` each step.\n")
            else:
                f.write(f"-- {eng} — TABLES ONLY. Write generated/{eng}.sql (the detector, exposing a\n")
                f.write(f"-- `flagged_card` view), then rerun `--schema-only` to compose it in below.\n")
            f.write(f"CREATE TABLE {cust_tbl} ({_CUST_DDL[eng]}){_CUST_SUFFIX[eng]};\n")
            f.write(f"CREATE TABLE {txn_tbl} (\n  {cols}{_TXN_EXTRA[eng]}\n){_TXN_SUFFIX[eng]};\n")
            if detector:
                f.write("\n-- ============================== DETECTOR ==============================\n")
                f.write(detector + "\n")
    kind = "tables only — no detectors yet" if len(missing) == 3 else "tables + detector views"
    print(f"  {gen_dir}/schema.{{feldera,postgres_ivm,clickhouse_ivm}}.sql  ({kind})")
    if missing:
        print(f"  (no generated/<engine>.sql yet for: {', '.join(missing)} — write them, then rerun --schema-only)")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--customers", type=int, default=200000)
    p.add_argument("--sharp", type=int, default=15)
    p.add_argument("--burst", type=int, default=800)
    p.add_argument("--slow", type=int, default=10)
    p.add_argument("--sb-gift", type=int, default=26)
    p.add_argument("--borderline", type=int, default=10000,
                   help="legitimate spread-out gift-card buyers — the false-positive source: "
                        "the over-covering bucket proxy flags them, the exact rolling window clears them")
    p.add_argument("--bl-gift", type=int, default=28, help="gift txns per borderline card")
    p.add_argument("--bl-span", type=int, default=52,
                   help="days over which a borderline card's gifts are spread (>30 so the true "
                        "30-day rolling count stays under the threshold)")
    p.add_argument("--fanout", type=int, default=50,
                   help="geographic fan-out fraud cards — caught only by the distinct-location signal: "
                        "Feldera + ClickHouse (uniq) catch them, the Postgres bucketed rollup cannot")
    p.add_argument("--fo-locs", type=int, default=12, help="distinct shipping locations per fan-out card in a day")
    p.add_argument("--travel", type=int, default=30,
                   help="impossible-travel fraud cards — caught only by the consecutive-pair (LAG) "
                        "signal: Feldera catches them, both IVM engines cannot")
    p.add_argument("--noise", type=int, default=0,
                   help="OPTIONAL busy-but-legit gift buyers that genuinely cross the gift threshold — "
                        "the RULE's own false positives, flagged by EVERY engine incl. Feldera. Off by "
                        "default so Feldera flags exactly the fraud; set >0 to show the rule's own imprecision")
    p.add_argument("--batches", type=int, default=100)
    p.add_argument("--batch-rows", type=int, default=50000)
    p.add_argument("--stream-days", type=int, default=60)
    p.add_argument("--out-dir", default="data/demo")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--schema-only", action="store_true",
                   help="(re)write only the per-engine schema.<engine>.sql files (tables + detector "
                        "views) from the current generated/<engine>.sql; do NOT regenerate the dataset")
    a = p.parse_args()

    out = Path(a.out_dir); out.mkdir(parents=True, exist_ok=True)
    if a.schema_only:
        write_schema()
        print("done (schema only).")
        return

    rng = random.Random(a.seed)
    print(f"Generating realistic planted-fraud dataset -> {out}/  (reusing bench gen_data)")
    rows, homes, sharp_ids, slow_ids, borderline_ids, fanout_ids, travel_ids, noise_ids = generate_customers(
        a.customers, a.sharp, a.slow, a.borderline, a.fanout, a.travel, a.noise, rng)
    write_customers(out / "customers.csv", rows)               # REUSED from bench gen_data
    n_fraud, n_border, n_fanout, n_travel, n_noise, boundary = write_stream_batches(out / "batches", a.customers,
        sharp_ids, slow_ids, a.burst, a.sb_gift, borderline_ids, a.bl_gift, a.bl_span, fanout_ids, a.fo_locs,
        travel_ids, noise_ids, homes, a.stream_days, a.batches, a.batch_rows, rng)
    write_labels(out / "labels.csv", sharp_ids, slow_ids, fanout_ids, travel_ids, borderline_ids)
    write_schema()
    print(f"  ground truth: {a.sharp + a.slow + a.fanout + a.travel} fraud cards SHOULD be flagged "
          f"({a.sharp} sharp, {a.slow} slow-burn, {a.fanout} fan-out, {a.travel} impossible-travel); "
          f"{len(borderline_ids):,} borderline + {len(noise_ids):,} busy-legit should NOT (busy-legit trip the "
          f"gift RULE in every engine; borderline sit between a true rolling window and a wider fixed bucket).")
    print("done.")


if __name__ == "__main__":
    main()
