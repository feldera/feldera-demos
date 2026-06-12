#!/usr/bin/env python3
"""Generate fraud-detection CSV data parameterized by scale.

Produces customers.csv and transactions.csv in --out-dir, matching the
schema used by demo_runner.py / engine_feldera.py.

  customers.csv   — cc_num, name, lat, long
  transactions.csv — category, ts, amt, cc_num, shipping_lat, shipping_long

Transactions are laid out so that the first --preload rows form the preload
history and the remaining --batches * --batch-size rows form the streaming
batches, all in one file for easy slicing.

Usage:
  python run_csv_pipeline.py                                      # defaults
  python run_csv_pipeline.py --customers 100000 --preload 4000000 --batch-size 2000 --batches 100
  python run_csv_pipeline.py --customers 50000 --preload 1000000 --batch-size 1000 --batches 50 --out-dir data/custom
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

CATEGORIES  = ["gift card", "foo bar"]
TS_START    = datetime(2024, 1, 1)
TS_STEP     = timedelta(seconds=1)   # 1 second per transaction row


def generate_customers(n: int) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append({
            "cc_num": i,
            "name":   f"Customer_{i}",
            "lat":    round(random.uniform(-90, 90), 6),
            "long":   round(random.uniform(-90, 90), 6),
        })
    return rows


def write_customers(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["cc_num", "name", "lat", "long"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {path}  ({len(rows):,} rows)")


TXN_FIELDS = ["category", "ts", "amt", "cc_num", "shipping_lat", "shipping_long"]


def _random_txn(ts: datetime, n_customers: int) -> dict:
    return {
        "category":     random.choice(CATEGORIES),
        "ts":           ts.strftime("%Y-%m-%d %H:%M:%S"),
        "amt":          round(random.uniform(0, 1000), 2),
        "cc_num":       random.randint(0, n_customers - 1),
        "shipping_lat": round(random.uniform(-90, 90), 6),
        "shipping_long":round(random.uniform(-90, 90), 6),
    }


def write_transactions(path: Path, n_customers: int, n_rows: int, ts_start: datetime) -> datetime:
    """Write n_rows transactions starting at ts_start. Returns the next unused ts."""
    ts = ts_start
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TXN_FIELDS)
        writer.writeheader()
        for _ in range(n_rows):
            writer.writerow(_random_txn(ts, n_customers))
            ts += TS_STEP
    print(f"  {path}  ({n_rows:,} rows)")
    return ts


def write_batches(batches_dir: Path, n_customers: int, batch_size: int, n_batches: int, ts_start: datetime) -> None:
    batches_dir.mkdir(exist_ok=True)
    ts = ts_start
    for i in range(n_batches):
        path = batches_dir / f"batch_{i+1:04d}.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=TXN_FIELDS)
            writer.writeheader()
            for _ in range(batch_size):
                writer.writerow(_random_txn(ts, n_customers))
                ts += TS_STEP
        if (i + 1) % 10 == 0 or (i + 1) == n_batches:
            print(f"  {batches_dir}/  ({i+1}/{n_batches} batches written)", end="\r")
    print(f"  {batches_dir}/  ({n_batches} batches × {batch_size} rows)          ")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--customers",  type=int, default=100_000, help="Number of customers (default: %(default)s)")
    parser.add_argument("--preload",    type=int, default=4_000_000, help="Preload transaction rows (default: %(default)s)")
    parser.add_argument("--batch-size", type=int, default=1_000,   help="Rows per streaming batch (default: %(default)s)")
    parser.add_argument("--batches",    type=int, default=100,     help="Number of streaming batches (default: %(default)s)")
    parser.add_argument("--out-dir",    default=None,              help="Output directory (default: data/<customers>c_<total>t)")
    parser.add_argument("--seed",       type=int, default=42,      help="Random seed (default: %(default)s)")
    args = parser.parse_args()

    random.seed(args.seed)

    n_stream = args.batches * args.batch_size
    n_total  = args.preload + n_stream

    out_dir = Path(args.out_dir) if args.out_dir else Path(f"data/{args.customers}c_{args.preload}pre_{args.batch_size}bs_{args.batches}bat")
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating data → {out_dir}/")
    print(f"  customers : {args.customers:,}")
    print(f"  preload   : {args.preload:,} transactions")
    print(f"  stream    : {args.batches} batches × {args.batch_size} = {n_stream:,} transactions")
    print(f"  total txns: {n_total:,}")
    print()

    customers = generate_customers(args.customers)
    write_customers(out_dir / "customers.csv", customers)
    next_ts = write_transactions(out_dir / "transactions.csv", args.customers, args.preload, TS_START)
    write_batches(out_dir / "batches", args.customers, args.batch_size, args.batches, next_ts)

    print()
    print(f"Done. Output in {out_dir}/")
    print(f"  customers.csv       — {args.customers:,} rows")
    print(f"  transactions.csv    — {args.preload:,} rows (preload)")
    print(f"  batches/            — {args.batches} files × {args.batch_size} rows")


if __name__ == "__main__":
    main()
