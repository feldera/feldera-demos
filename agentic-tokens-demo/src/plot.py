#!/usr/bin/env python3
"""plot.py — two time-series from results/metrics.csv (written by runner.py):

  1. SUSPICIOUS TRANSACTIONS PER STEP — the NEW transactions flagged in each batch (the LLM-analysis
     workload that step). Computed as the step-to-step delta of the cumulative suspicious-txn count.
  2. CUMULATIVE LLM COST (Claude Opus 4.8) vs stream progress = cumulative workload × $/txn.

Both are plotted against the streaming step (one step = one 20,000-row batch ingested over the
60-day window), one line per engine — the per-step workload and the bill it runs up.

Usage:  python3 plot.py        # writes results/timeseries.png
"""
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from cost_estimate import per_txn_usd, DEFAULT_TOKENS_IN, DEFAULT_TOKENS_OUT

HERE    = Path(__file__).resolve().parent.parent   # demo root (this file lives in src/)
METRICS = HERE / "results" / "metrics.csv"
OUT     = HERE / "results" / "timeseries.png"

# Claude Opus 4.8 list price (input $5 / output $25 per 1M tokens) → $/analysed-transaction.
OPUS_IN, OPUS_OUT = 5.00, 25.00
PTX = per_txn_usd(DEFAULT_TOKENS_IN, DEFAULT_TOKENS_OUT, OPUS_IN, OPUS_OUT)

STYLE = {  # engine -> (legend label, colour)
    "feldera":        ("Feldera",            "#2ca02c"),
    "clickhouse_ivm": ("ClickHouse-IVM",  "#ff7f0e"),
    "postgres_ivm":   ("Postgres-SIM-IVM",     "#d62728"),
}
ORDER = ["feldera", "clickhouse_ivm", "postgres_ivm"]


def load():
    step = defaultdict(list)
    susp = defaultdict(list)
    for r in csv.DictReader(open(METRICS)):
        e = r["engine"]
        step[e].append(int(r["step"]))
        susp[e].append(int(r["susp_txns"]))
    return step, susp


def main():
    step, susp = load()
    if not susp:
        raise SystemExit(f"no rows in {METRICS} — run runner.py first")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 9.5), sharex=True)
    fig.suptitle("The number of suspicious transactions and the cost of analysing them over the stream",
                 fontsize=14, fontweight="bold")

    max_per = max_cost = 0.0
    for e in ORDER:
        if e not in susp:
            continue
        lbl, c = STYLE[e]
        xs  = step[e]
        cum = susp[e]                                                       # cumulative susp txns (metrics.csv)
        per = [cum[0]] + [cum[i] - cum[i - 1] for i in range(1, len(cum))]  # NEW susp txns each step
        cost = [v * PTX for v in cum]                                       # cumulative LLM cost
        max_per = max(max_per, max(per)); max_cost = max(max_cost, max(cost))
        ax1.plot(xs, per, color=c, lw=1.9, label=f"{lbl}  ({cum[-1]:,} total)")
        ax2.plot(xs, cost, color=c, lw=2.4, label=lbl)
        ax2.annotate(f"  ${cost[-1]:,.0f}", (xs[-1], cost[-1]), color=c, va="center",
                     fontsize=10, fontweight="bold")

    ax1.set_title("Suspicious transactions per step  (new transactions flagged each day)",
                  fontsize=12, loc="left")
    ax1.set_ylabel("suspicious transactions / step")
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1000:.0f}k" if v >= 1000 else f"{v:.0f}"))

    ax2.set_title(f"Cumulative LLM cost — Claude Opus 4.8  (${PTX:.2f} / transaction)",
                  fontsize=12, loc="left")
    ax2.set_ylabel("cumulative cost (USD)")
    ax2.set_xlabel("stream day  (1 step = 1 day; 30 days of history preloaded before day 0)")
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v:,.0f}"))

    for ax in (ax1, ax2):
        ax.grid(True, alpha=0.3)
        ax.margins(x=0.10)
        ax.legend(loc="upper left", framealpha=0.9)
    ax1.set_ylim(0, max_per * 1.15)    # 15% headroom above the tallest spike
    ax2.set_ylim(0, max_cost * 1.15)   # 15% headroom above the highest cost

    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(OUT, dpi=130)
    print(f"wrote {OUT}")
    print(f"  Opus 4.8 $/txn = ${PTX:.4f}")
    for e in ORDER:
        if e in susp:
            print(f"  {e:<16} final: {susp[e][-1]:>8,} susp txns  →  ${susp[e][-1]*PTX:>10,.2f}")


if __name__ == "__main__":
    main()
