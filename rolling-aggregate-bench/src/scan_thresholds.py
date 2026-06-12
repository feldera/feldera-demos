#!/usr/bin/env python3
"""
scan_thresholds.py — scan transaction CSV data and suggest fraud signal thresholds.

For each signal, computes the per-card peak window count across all transactions,
then reports the distribution and suggests a threshold at a target percentile
(default: 99th — flags the top 1% of cards).

Usage:
  python3 scan_thresholds.py --data-dir <DATA_DIR>
  python3 scan_thresholds.py --data-dir <DATA_DIR> --percentile 95
"""

import argparse
import csv
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
import constants as c

TS_FMT = "%Y-%m-%d %H:%M:%S"


def load_customers(data_dir: Path) -> dict:
    customers = {}
    with open(data_dir / "customers.csv", newline="") as f:
        for row in csv.DictReader(f):
            cc = int(row["cc_num"])
            customers[cc] = {
                "lat":  float(row["lat"])  if row["lat"]  else 0.0,
                "long": float(row["long"]) if row["long"] else 0.0,
            }
    return customers


def load_transactions(data_dir: Path) -> list:
    txns = []
    preload = data_dir / "transactions.csv"
    if preload.exists():
        _read_csv(preload, txns)
    batches_dir = data_dir / "batches"
    if batches_dir.is_dir():
        for path in sorted(batches_dir.glob("batch_*.csv")):
            _read_csv(path, txns)
    return txns


def _read_csv(path: Path, out: list) -> None:
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if not row["ts"]:
                continue
            out.append({
                "cc_num":       int(row["cc_num"]),
                "ts":           datetime.strptime(row["ts"], TS_FMT),
                "amt":          float(row["amt"])            if row["amt"]            else 0.0,
                "category":     row["category"],
                "shipping_lat": float(row["shipping_lat"])   if row["shipping_lat"]   else 0.0,
                "shipping_long":float(row["shipping_long"])  if row["shipping_long"]  else 0.0,
            })


def group_by_card(txns: list) -> dict:
    by_card = defaultdict(list)
    for t in txns:
        by_card[t["cc_num"]].append(t)
    for cc in by_card:
        by_card[cc].sort(key=lambda t: t["ts"])
    return by_card


def peak_window_count(txns: list, window_days: int, predicate=None) -> int:
    """Max count of predicate-matching transactions in any trailing window_days window."""
    cutoff_delta = timedelta(days=window_days)
    all_q  = deque()   # all txns in current window (for expiry tracking)
    pred_q = deque()   # predicate-matching txns in current window
    peak   = 0
    for txn in txns:
        cutoff = txn["ts"] - cutoff_delta
        while all_q and all_q[0]["ts"] < cutoff:
            old = all_q.popleft()
            if pred_q and pred_q[0] is old:
                pred_q.popleft()
        all_q.append(txn)
        if predicate is None or predicate(txn):
            pred_q.append(txn)
        n = len(pred_q) if predicate else len(all_q)
        if n > peak:
            peak = n
    return peak


def pct(values: list, p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = (len(s) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (idx - lo) * (s[hi] - s[lo])


def report(label: str, peaks: list, current: int, target_pct: float) -> int:
    n         = len(peaks)
    suggested = max(1, round(pct(peaks, target_pct)))
    n_current  = sum(1 for v in peaks if v >= current)
    n_suggested = sum(1 for v in peaks if v >= suggested)
    print(f"  {label}")
    print(f"    p50={pct(peaks,50):.1f}  p90={pct(peaks,90):.1f}  "
          f"p95={pct(peaks,95):.1f}  p99={pct(peaks,99):.1f}  max={max(peaks)}")
    print(f"    current  threshold={current:>4}  →  {n_current:>5}/{n} cards flagged  ({100*n_current/n:.1f}%)")
    print(f"    p{target_pct:.0f}     threshold={suggested:>4}  →  {n_suggested:>5}/{n} cards flagged  ({100*n_suggested/n:.1f}%)")
    print()
    return suggested


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--data-dir",   required=True)
    parser.add_argument("--percentile", type=float, default=99.0,
                        help="Threshold percentile (default: 99 → flags top 1%% of cards)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    print(f"Loading {data_dir} …")
    customers = load_customers(data_dir)
    txns      = load_transactions(data_dir)
    print(f"  {len(customers):,} customers  {len(txns):,} transactions")

    by_card = group_by_card(txns)
    print(f"  {len(by_card):,} active cards\n")

    dist_thr = c.DIST_MILES_THRESHOLD
    print("Computing per-card peak window counts …")

    gift30, gift45, txn7, disp3 = [], [], [], []
    for cc, card_txns in by_card.items():
        home = customers.get(cc, {"lat": 0.0, "long": 0.0})
        is_gift = lambda t: t["category"] == "gift card"
        is_far  = lambda t, h=home: abs(t["shipping_lat"] - h["lat"]) + abs(t["shipping_long"] - h["long"]) > dist_thr
        gift30.append(peak_window_count(card_txns, 30, is_gift))
        gift45.append(peak_window_count(card_txns, 45, is_gift))
        txn7.append(  peak_window_count(card_txns,  7, None))
        disp3.append( peak_window_count(card_txns,  3, is_far))

    print(f"\nSignal threshold analysis  (target percentile: {args.percentile})\n")
    print("─" * 64)
    s1 = report("gift_card_burst_30d   (gift cards in 30-day window)",  gift30, c.GIFT_BURST_30D_THRESHOLD,    args.percentile)
    s2 = report("gift_card_burst_45d   (gift cards in 45-day window)",  gift45, c.GIFT_BURST_45D_THRESHOLD,    args.percentile)
    s3 = report("spend_velocity_7d     (total txns in 7-day window)",   txn7,   c.SPEND_VELOCITY_7D_THRESHOLD, args.percentile)
    s4 = report("repeated_displacement (far-from-home in 3-day window)",disp3,  c.DISPLACEMENT_THRESHOLD,      args.percentile)
    print("─" * 64)
    print(f"Suggested constants.py values at p{args.percentile:.0f}:")
    print(f"  GIFT_BURST_30D_THRESHOLD    = {s1}")
    print(f"  GIFT_BURST_45D_THRESHOLD    = {s2}")
    print(f"  SPEND_VELOCITY_7D_THRESHOLD = {s3}")
    print(f"  DISPLACEMENT_THRESHOLD      = {s4}")


if __name__ == "__main__":
    main()
