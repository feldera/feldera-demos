#!/usr/bin/env python3
"""cost_estimate.py — the TOTAL LLM analysis cost of each engine's flags, across frontier models.

Every transaction an engine flags as suspicious is sent to an LLM agent for analysis (~12,000 in +
2,000 out tokens) — you can't tell a false positive from real fraud until you've analysed it, so ALL
flagged transactions cost money. This reads results/score.csv and prices the total analysis workload
(suspicious txns × $/txn) per engine across frontier models. Each engine's bill scales with how many
transactions it flags. Numbers are deterministic — no API key, no live calls.
"""

import argparse
import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent   # demo root (this file lives in src/)
DEFAULT_TOKENS_IN  = 12000  # agentic triage: fraud policy + txn + retrieved card history + customer profile
DEFAULT_TOKENS_OUT = 2000   # structured verdict + rationale + recommended action

# Anthropic: Claude API reference (2026-06-04). Others: official pages, web-verified 2026-06-24.
PRICING = [
    ("Anthropic",  "Claude Opus 4.8",     5.00,  25.00),
    ("Anthropic",  "Claude Sonnet 4.6",   3.00,  15.00),
    ("Anthropic",  "Claude Haiku 4.5",    1.00,   5.00),
    ("OpenAI",     "GPT-5.5",             5.00,  30.00),
    ("Google",     "Gemini 3.1 Pro",      2.00,  12.00),
    ("DeepSeek",   "DeepSeek-V4-Flash",   0.14,   0.28),
]
ORDER = ["feldera", "clickhouse_ivm", "postgres_ivm"]
LABEL = {"feldera": "Feldera", "clickhouse_ivm": "ClickHouse-IVM", "postgres_ivm": "Postgres-SIM-IVM"}
FALLBACK = {  # (suspicious_txns, fp_txns) if score.csv absent
    "feldera": (12920, 0), "clickhouse_ivm": (202498, 189638), "postgres_ivm": (271516, 259256),
}


def load(path: Path):
    """engine -> (suspicious_txns analysed, of which false-positive txns)."""
    if not path.exists():
        return dict(FALLBACK), False
    d = {r["engine"]: (int(r["suspicious_txns"]), int(r["fp_txns"])) for r in csv.DictReader(open(path))}
    return d, True


def per_txn_usd(tin, tout, p_in, p_out):
    return tin / 1_000_000 * p_in + tout / 1_000_000 * p_out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tokens-in", type=int, default=DEFAULT_TOKENS_IN)
    ap.add_argument("--tokens-out", type=int, default=DEFAULT_TOKENS_OUT)
    ap.add_argument("--score", default=str(HERE / "results" / "score.csv"))
    a = ap.parse_args()

    sc, from_csv = load(Path(a.score))
    engines = [e for e in ORDER if e in sc]

    print("=" * 80)
    print("  TOTAL LLM ANALYSIS COST   (every flagged transaction is analysed × $/txn)")
    print("=" * 80)
    print(f"  per-transaction analysis: {a.tokens_in:,} in + {a.tokens_out:,} out tokens")
    print(f"  workload: {'results/score.csv' if from_csv else 'fallback'}\n")
    for e in engines:
        susp, fp = sc[e]
        print(f"      {LABEL[e]:<18} {susp:>9,} suspicious txns to analyse ({fp:>9,} of them false positives)")
    print()

    hdr = "  ".join(f"{LABEL[e]:>16}" for e in engines)
    print(f"  {'provider':<10} {'model':<20} {'$/txn':>8}  | {hdr}")
    print("  " + "-" * (42 + 18 * len(engines)))
    for provider, model, p_in, p_out in PRICING:
        ptx = per_txn_usd(a.tokens_in, a.tokens_out, p_in, p_out)
        cells = "  ".join(f"${sc[e][0] * ptx:>14,.0f}" for e in engines)
        print(f"  {provider:<10} {model:<20} ${ptx:>7.4f}  | {cells}")
    print("  " + "-" * (42 + 18 * len(engines)))
    print()
    print("  Every suspicious transaction must be analysed (you can't tell a false positive from real")
    print("  fraud until you do), so each engine's bill scales with how many transactions it flags.")


if __name__ == "__main__":
    main()
