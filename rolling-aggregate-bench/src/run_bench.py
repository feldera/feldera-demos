#!/usr/bin/env python3
"""
run_bench.py — end-to-end benchmark: generate data, scan thresholds, run engines.

Steps:
  1. Generate customers.csv + transactions.csv + batches/ for the given scale.
  2. Scan all transactions and compute p<percentile> fraud signal thresholds.
  3. Apply those thresholds and run the benchmark (Feldera / ClickHouse / PostgreSQL).

Usage:
    python3 src/run_bench.py --customers 500000 --preload 1000000 --batch-size 1000 --batches 10 --sequential --mode feldera ch pg
    python3 src/run_bench.py --customers 5000000 --preload 200000000 --batch-size 1000 --batches 10 --sequential --mode feldera ch
"""

import argparse
import datetime
import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).parent           # src/
_ROOT = _HERE.parent                    # rolling-aggregate-bench/

sys.path.insert(0, str(_HERE))          # constants, demo_runner, engines


def _banner(msg: str) -> None:
    w = max(len(msg) + 4, 64)
    print(f"\n{'━' * w}\n  {msg}\n{'━' * w}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Data generation
    parser.add_argument("--customers",  type=int, default=10_000,
                        help="Number of customers (default: %(default)s)")
    parser.add_argument("--preload",    type=int, default=10_000,
                        help="Preload transaction rows (default: %(default)s)")
    parser.add_argument("--batch-size", type=int, default=1_000,
                        help="Rows per streaming batch (default: %(default)s)")
    parser.add_argument("--batches",    type=int, default=10,
                        help="Number of streaming batches (default: %(default)s)")
    parser.add_argument("--data-dir",   default=None,
                        help="Output / data directory (default: auto-named from params)")
    parser.add_argument("--seed",       type=int, default=42)
    # Threshold scan
    parser.add_argument("--percentile", type=float, default=99.0,
                        help="Percentile for threshold suggestion (default: 99 → top 1%% of cards)")
    parser.add_argument("--sample-cards", type=int, default=100_000,
                        help="Cards to sample for threshold scan (default: 100K; use 0 for all)")
    # Demo runner options
    parser.add_argument("--no-feldera",    action="store_true")
    parser.add_argument("--no-clickhouse", action="store_true")
    parser.add_argument("--no-postgres",     action="store_true")
    parser.add_argument("--postgres-steps",  type=int, default=None,
                        help="Limit PostgreSQL to this many steps (default: same as --batches)")
    parser.add_argument("--mode",          nargs="+", default=None,
                        choices=["feldera", "ch", "pg"],
                        metavar="ENGINE",
                        help="one or more of: feldera ch pg  (default: feldera ch)")
    parser.add_argument("--postgres-user", default=None,
                        help="PostgreSQL username (default: from constants.py)")
    parser.add_argument("--max-rss-mb",    type=int, default=None,
                        help="Feldera pipeline memory cap in MB (e.g. 64000 for ~64 GB)")
    parser.add_argument("--sequential",    action="store_true",
                        help="Run engines one at a time (clean isolated timing)")
    parser.add_argument("--mock",          action="store_true",
                        help="Simulate queries — no DB needed (skips steps 1 and 2)")
    args = parser.parse_args()

    # ── Resolve data directory ─────────────────────────────────────────────────
    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        data_dir = _ROOT / "data" / (
            f"{args.customers}c_{args.preload}pre"
            f"_{args.batch_size}bs_{args.batches}bat"
        )

    # ── STEP 1: Generate data ──────────────────────────────────────────────────
    _already_exists = (data_dir / "customers.csv").exists() and (data_dir / "batches").is_dir()
    _banner(f"STEP 1/3  Generate data → {data_dir.relative_to(_ROOT)}/")
    if not args.mock and _already_exists:
        print(f"  (skipped — {data_dir.relative_to(_ROOT)}/ already exists)")
    elif not args.mock:
        ret = subprocess.run([
            sys.executable, str(_HERE / "gen_data.py"),
            "--customers",  str(args.customers),
            "--preload",    str(args.preload),
            "--batch-size", str(args.batch_size),
            "--batches",    str(args.batches),
            "--out-dir",    str(data_dir),
            "--seed",       str(args.seed),
        ], cwd=str(_ROOT))
        if ret.returncode != 0:
            sys.exit(f"Data generation failed (exit {ret.returncode})")
    else:
        print("  (skipped — mock mode)")

    # ── STEP 2: Scan thresholds ────────────────────────────────────────────────
    _banner(f"STEP 2/3  Scan thresholds at p{args.percentile:.0f}")
    import json
    import random
    import constants as _c
    from scan_thresholds import load_customers, peak_window_count, pct, report

    gb30 = _c.GIFT_BURST_30D_THRESHOLD
    gb45 = _c.GIFT_BURST_45D_THRESHOLD
    sv7  = _c.SPEND_VELOCITY_7D_THRESHOLD
    disp = _c.DISPLACEMENT_THRESHOLD

    if not args.mock:
        # ── Fast path: use cached thresholds if available ──────────────────
        _cache = data_dir / "thresholds.json"
        if _cache.exists():
            _t = json.loads(_cache.read_text())
            gb30, gb45, sv7, disp = _t["gb30"], _t["gb45"], _t["sv7"], _t["disp"]
            print(f"  (cached) gb30={gb30}  gb45={gb45}  sv7={sv7}  disp={disp}")
        else:
            # ── Sample-based scan: stream transactions, keep only sampled cards ──
            # p99 is stable with 100K-card sample (~2% of 5M); avoids loading all
            # 200M rows into memory and speeds up scanning by ~50×.
            _n_customers = sum(1 for _ in open(data_dir / "customers.csv")) - 1
            SAMPLE_SIZE = _n_customers if args.sample_cards == 0 else min(args.sample_cards, _n_customers)
            customers = load_customers(data_dir)
            all_cc = list(customers.keys())
            rng = random.Random(args.seed)
            sampled = set(rng.sample(all_cc, min(SAMPLE_SIZE, len(all_cc))))
            print(f"  Sampling {len(sampled):,} of {len(all_cc):,} cards …")

            # Stream transactions.csv + batches, keeping only sampled cards.
            import csv as _csv
            from collections import defaultdict
            from datetime import datetime as _dt, timedelta
            TS_FMT = "%Y-%m-%d %H:%M:%S"
            by_card: dict = defaultdict(list)

            def _stream(path):
                with open(path, newline="") as f:
                    for row in _csv.DictReader(f):
                        if not row["ts"]:
                            continue
                        cc = int(row["cc_num"])
                        if cc not in sampled:
                            continue
                        by_card[cc].append({
                            "ts":           _dt.strptime(row["ts"], TS_FMT),
                            "amt":          float(row["amt"])           if row["amt"]           else 0.0,
                            "category":     row["category"],
                            "shipping_lat": float(row["shipping_lat"])  if row["shipping_lat"]  else 0.0,
                            "shipping_long":float(row["shipping_long"]) if row["shipping_long"] else 0.0,
                        })

            preload = data_dir / "transactions.csv"
            if preload.exists():
                print(f"  Streaming {preload.name} …")
                _stream(preload)
            batches_dir = data_dir / "batches"
            if batches_dir.is_dir():
                for bp in sorted(batches_dir.glob("batch_*.csv")):
                    _stream(bp)

            for cc in by_card:
                by_card[cc].sort(key=lambda t: t["ts"])

            dist_thr = _c.DIST_MILES_THRESHOLD
            n_sampled = len(by_card)
            print(f"  {n_sampled:,} active sampled cards\n")
            print("Computing per-card peak window counts …")

            peaks_gb30, peaks_gb45, peaks_sv7, peaks_disp = [], [], [], []
            for cc, card_txns in by_card.items():
                home    = customers[cc]
                is_gift = lambda t: t["category"] == "gift card"
                is_far  = lambda t, h=home: (
                    abs(t["shipping_lat"]  - h["lat"])
                  + abs(t["shipping_long"] - h["long"]) > dist_thr
                )
                peaks_gb30.append(peak_window_count(card_txns, 30, is_gift))
                peaks_gb45.append(peak_window_count(card_txns, 45, is_gift))
                peaks_sv7.append( peak_window_count(card_txns,  7, None))
                peaks_disp.append(peak_window_count(card_txns,  3, is_far))

            p = args.percentile
            print(f"\nSignal threshold analysis  (target percentile: {p})\n{'─' * 64}")
            gb30 = report("gift_card_burst_30d   (gift cards in 30-day window)",  peaks_gb30, _c.GIFT_BURST_30D_THRESHOLD,    p)
            gb45 = report("gift_card_burst_45d   (gift cards in 45-day window)",  peaks_gb45, _c.GIFT_BURST_45D_THRESHOLD,    p)
            sv7  = report("spend_velocity_7d     (total txns in 7-day window)",   peaks_sv7,  _c.SPEND_VELOCITY_7D_THRESHOLD, p)
            disp = report("repeated_displacement (far-from-home in 3-day window)",peaks_disp, _c.DISPLACEMENT_THRESHOLD,      p)
            print(f"{'─' * 64}")

            # Guard: if >10% of sampled cards are flagged the threshold is degenerate.
            MAX_FLAG_PCT = 0.10
            def _clamp(threshold, peaks, label):
                n = len(peaks)
                frac = sum(1 for v in peaks if v >= threshold) / n
                if frac <= MAX_FLAG_PCT:
                    return threshold
                for t in range(threshold + 1, max(peaks) + 2):
                    frac = sum(1 for v in peaks if v >= t) / n
                    if frac <= MAX_FLAG_PCT:
                        print(f"  [{label}] bumped threshold {threshold}→{t} ({frac*100:.1f}% flagged)")
                        return t
                print(f"  [{label}] WARNING: can't satisfy ≤{MAX_FLAG_PCT*100:.0f}% even at max — using {max(peaks)+1}")
                return max(peaks) + 1
            gb30 = _clamp(gb30, peaks_gb30, "gb30")
            gb45 = _clamp(gb45, peaks_gb45, "gb45")
            sv7  = _clamp(sv7,  peaks_sv7,  "sv7")
            disp = _clamp(disp, peaks_disp, "disp")

            # Save cache so reruns skip the scan entirely.
            _cache.write_text(json.dumps(
                {"gb30": gb30, "gb45": gb45, "sv7": sv7, "disp": disp,
                 "percentile": args.percentile, "sample_cards": len(sampled)}
            ))
            print(f"  (thresholds cached → {_cache.name})")

        # Patch constants so the engines see the data-driven thresholds.
        _c.GIFT_BURST_30D_THRESHOLD    = gb30
        _c.GIFT_BURST_45D_THRESHOLD    = gb45
        _c.SPEND_VELOCITY_7D_THRESHOLD = sv7
        _c.DISPLACEMENT_THRESHOLD      = disp
        print(f"\nApplied p{args.percentile:.0f} thresholds: gb30={gb30}  gb45={gb45}  sv7={sv7}  disp={disp}")
    else:
        print("  (skipped — mock mode)")

    # ── STEP 3: Run benchmark ──────────────────────────────────────────────────
    _banner("STEP 3/3  Run benchmark  (Feldera vs ClickHouse vs PostgreSQL)")

    ts       = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = _ROOT / "results" / f"{data_dir.name}_{ts}.txt"

    # Call demo_runner.main() directly so the patched constants module is visible
    # to the engine setup calls (engine.setup() re-reads _c.THRESHOLD at runtime).
    demo_argv = [
        "demo_runner",
        "--data-dir", str(data_dir),
        "--output",   str(out_file),
        "--steps",    str(args.batches),
    ]
    if args.no_feldera:
        demo_argv.append("--no-feldera")
    if args.no_clickhouse:
        demo_argv.append("--no-clickhouse")
    if args.no_postgres:
        demo_argv.append("--no-postgres")
    if args.postgres_steps is not None:
        demo_argv += ["--postgres-steps", str(args.postgres_steps)]
    if args.mode:
        demo_argv += ["--mode"] + args.mode
    if args.postgres_user:
        demo_argv += ["--postgres-user", args.postgres_user]
    if args.max_rss_mb is not None:
        demo_argv += ["--max-rss-mb", str(args.max_rss_mb)]
    if args.sequential:
        demo_argv.append("--sequential")
    if args.mock:
        demo_argv.append("--mock")

    saved_argv, sys.argv = sys.argv, demo_argv
    try:
        import demo_runner
        demo_runner.main()
    finally:
        sys.argv = saved_argv

    print(f"\nResults → {out_file.relative_to(_ROOT)}")


if __name__ == "__main__":
    main()
