#!/usr/bin/env python3 -u
"""
Fraud investigator — polls fraud_alerts, classifies each new card with a
rule-based engine, and writes verdicts to investigator.log.

No external API keys required.

Usage:
    python3 fraud_investigator.py [pipeline_name] [--duration N] [--max-cards N]

Environment:
    FELDERA_HOST  Feldera API base URL (default: http://localhost:8080)
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env", override=False)
except ImportError:
    pass

parser = argparse.ArgumentParser(description="Fraud investigator")
parser.add_argument("pipeline", nargs="?", default="fraud_detection_demo")
parser.add_argument("--duration", type=int, default=30, help="Stop after N seconds (default: 30)")
parser.add_argument("--max-cards", type=int, default=100, help="Stop after N cards (default: 100)")
args = parser.parse_args()

PIPELINE = args.pipeline
DURATION = args.duration
MAX_CARDS = args.max_cards
POLL_INTERVAL = 10
LOG_FILE = Path(__file__).parent / "investigator.log"
FELDERA_HOST    = os.environ.get("FELDERA_HOST", "http://localhost:8080")
FELDERA_API_KEY = os.environ.get("FELDERA_API_KEY", "") or os.environ.get("FDA_API_KEY", "")


def fda_query(sql: str) -> list[dict]:
    cmd = ["fda", "--host", FELDERA_HOST]
    if FELDERA_API_KEY:
        cmd += ["--auth", FELDERA_API_KEY]
    cmd += ["query", PIPELINE, sql, "--format", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        err = (result.stderr or result.stdout).strip()
        log(f"ERROR: fda query failed — {err}")
        return []
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def gather_context(cc_num: int) -> dict:
    signals = fda_query(
        f"SELECT signal_type, COUNT(*) AS cnt, SUM(amt) AS total_amt "
        f"FROM fraud_alerts WHERE cc_num = {cc_num} GROUP BY signal_type ORDER BY signal_type"
    )
    return {"cc_num": cc_num, "signals": signals}


def classify(context: dict) -> dict:
    """
    Rule-based classifier — signal-count and amount based, no name matching.
    Works regardless of what signal views were generated.
    """
    signals = context["signals"]
    n_signals = len(signals)
    total_amt = sum(float(s.get("total_amt") or 0) for s in signals)
    total_hits = sum(int(s.get("cnt") or 0) for s in signals)

    if n_signals == 0:
        return {"verdict": "BENIGN", "confidence": "LOW",
                "reason": "no signals fired"}

    # Multiple signals always HIGH
    if n_signals >= 3:
        return {"verdict": "SUSPICIOUS", "confidence": "HIGH",
                "reason": f"{n_signals} distinct signals fired — {total_hits} flagged transactions totalling ${total_amt:.2f}"}

    if n_signals == 2:
        return {"verdict": "SUSPICIOUS", "confidence": "HIGH",
                "reason": f"2 signals corroborate each other — {total_hits} flagged transactions totalling ${total_amt:.2f}"}

    # Single signal — use amount to gauge severity
    avg_amt = total_amt / total_hits if total_hits > 0 else 0

    if avg_amt >= 500:
        return {"verdict": "SUSPICIOUS", "confidence": "MEDIUM",
                "reason": f"1 signal, high average flagged amount ${avg_amt:.2f} — warrants review"}

    if avg_amt >= 100:
        return {"verdict": "SUSPICIOUS", "confidence": "LOW",
                "reason": f"1 signal, moderate average flagged amount ${avg_amt:.2f} — low priority"}

    return {"verdict": "BENIGN", "confidence": "LOW",
            "reason": f"1 signal, low average flagged amount ${avg_amt:.2f} — likely benign"}


def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str):
    line = f"[{now()}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def main():
    seen: set[int] = set()
    investigated = 0
    start_time = time.time()

    LOG_FILE.write_text("")  # clear log before each run
    log(f"=== Fraud investigator started — pipeline={PIPELINE} duration={DURATION}s max_cards={MAX_CARDS} ===")

    while True:
        elapsed = time.time() - start_time
        if elapsed >= DURATION:
            log(f"Stopped after {int(elapsed)}s.")
            break
        if investigated >= MAX_CARDS:
            log(f"Stopped after {investigated} cards.")
            break

        rows = fda_query("SELECT DISTINCT cc_num FROM fraud_alerts")
        new_cards = [r["cc_num"] for r in rows if r["cc_num"] not in seen]

        for cc_num in new_cards:
            if time.time() - start_time >= DURATION or investigated >= MAX_CARDS:
                break
            seen.add(cc_num)
            context = gather_context(cc_num)
            result = classify(context)

            verdict = result["verdict"]
            confidence = result.get("confidence", "?")
            reason = result.get("reason", "")
            signals_str = ", ".join(s["signal_type"] for s in context["signals"])
            log(f"{verdict} [{confidence}] cc={cc_num} | signals: {signals_str} | {reason}")
            investigated += 1

        if time.time() - start_time < DURATION and investigated < MAX_CARDS:
            time.sleep(POLL_INTERVAL)

    log(f"=== Done. Investigated {investigated} cards. Log: {LOG_FILE} ===")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\nStopped by user.")
