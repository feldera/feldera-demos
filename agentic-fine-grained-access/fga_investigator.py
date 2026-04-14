#!/usr/bin/env python3 -u
"""
Access investigator — polls access_alerts, classifies each new user with a
rule-based engine, and optionally blocks suspicious users in real time by
setting is_banned=true in the users table via the Feldera ingress API.

No external API keys required.

Usage:
    python3 fga_investigator.py [pipeline_name] [--duration N] [--max-users N] [--block]

Environment:
    FELDERA_HOST  Feldera API base URL (default: http://localhost:8080)
    FELDERA_API_KEY   Feldera API key (optional)
"""

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env", override=False)
except ImportError:
    pass

parser = argparse.ArgumentParser(description="Access investigator")
parser.add_argument("pipeline", nargs="?", default="fga")
parser.add_argument("--duration", type=int, default=30, help="Stop after N seconds (default: 30)")
parser.add_argument("--max-users", type=int, default=200, help="Stop after N users (default: 200)")
parser.add_argument("--block", action="store_true", help="Block SUSPICIOUS users by setting is_banned=true")
args = parser.parse_args()

PIPELINE = args.pipeline
DURATION = args.duration
MAX_USERS = args.max_users
BLOCK = args.block
POLL_INTERVAL = 10
LOG_FILE = Path(__file__).parent / "investigator.log"
FELDERA_HOST = os.environ.get("FELDERA_HOST", "http://localhost:8080")
FDA_API_KEY = os.environ.get("FELDERA_API_KEY", "") or os.environ.get("FDA_API_KEY", "")


def fda_query(sql: str) -> list[dict]:
    cmd = ["fda", "--host", FELDERA_HOST]
    if FDA_API_KEY:
        cmd += ["--auth", FDA_API_KEY]
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


def gather_context(user_id: int) -> dict:
    signals = fda_query(
        f"SELECT signal_type, COUNT(*) AS cnt, MAX(metric_value) AS peak_metric "
        f"FROM access_alerts WHERE user_id = {user_id} GROUP BY signal_type ORDER BY signal_type"
    )
    return {"user_id": user_id, "signals": signals}


def classify(context: dict) -> dict:
    """
    Rule-based classifier — uses peak metric_value (folder_count / user_count) for severity.
    Falls back to hit count if metric_value is not available.
    """
    signals = context["signals"]
    n_signals = len(signals)
    if n_signals == 0:
        return {"verdict": "BENIGN", "confidence": "LOW",
                "reason": "no signals fired"}

    total_hits = sum(int(s.get("cnt") or 0) for s in signals)
    peak_metric = max((int(s.get("peak_metric") or 0) for s in signals), default=0)

    # Multiple corroborating signals → always suspicious
    if n_signals >= 2:
        return {"verdict": "SUSPICIOUS", "confidence": "HIGH",
                "reason": f"{n_signals} signals corroborate — peak metric={peak_metric}, {total_hits} flagged events"}

    # Single signal — use peak metric_value to gauge severity
    if peak_metric >= 50:
        return {"verdict": "SUSPICIOUS", "confidence": "HIGH",
                "reason": f"peak metric_value={peak_metric} — extreme anomaly"}

    if peak_metric >= 20:
        return {"verdict": "SUSPICIOUS", "confidence": "MEDIUM",
                "reason": f"peak metric_value={peak_metric} — warrants review"}

    if peak_metric > 0:
        return {"verdict": "BENIGN", "confidence": "LOW",
                "reason": f"peak metric_value={peak_metric} — within normal range"}

    # No metric_value column — fall back to hit count
    if total_hits >= 100:
        return {"verdict": "SUSPICIOUS", "confidence": "HIGH",
                "reason": f"{total_hits} flagged events — high volume"}
    if total_hits >= 20:
        return {"verdict": "SUSPICIOUS", "confidence": "MEDIUM",
                "reason": f"{total_hits} flagged events — warrants review"}
    return {"verdict": "BENIGN", "confidence": "LOW",
            "reason": f"only {total_hits} flagged event(s) — likely benign"}


def block_user(user_id: int) -> bool:
    """
    Set is_banned=true for user_id via the Feldera ingress API.
    Pushes a delete + insert pair so the recursive permission views
    (user_can_read, user_can_write) immediately drop this user's access.
    """
    # Fetch current user record to preserve name
    rows = fda_query(f"SELECT id, name, is_banned FROM users WHERE id = {user_id}")
    if not rows:
        log(f"  ⚠ user={user_id} not found in users table — skipping block")
        return False
    user = rows[0]
    if user.get("is_banned"):
        log(f"  ℹ user={user_id} is already banned")
        return True

    url = (f"{FELDERA_HOST}/v0/pipelines/{PIPELINE}/ingress/users"
           f"?format=json&update_format=insert_delete")
    payload = (
        json.dumps({"delete": {"id": user_id, "name": user.get("name"), "is_banned": False}}) + "\n" +
        json.dumps({"insert": {"id": user_id, "name": user.get("name"), "is_banned": True}})
    ).encode()
    headers = {"Content-Type": "application/json"}
    if FDA_API_KEY:
        headers["Authorization"] = f"Bearer {FDA_API_KEY}"

    req = urllib.request.Request(url, data=payload, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
        return True
    except urllib.error.HTTPError as e:
        log(f"  ERROR: block_user HTTP {e.code} for user={user_id} — {e.read().decode()[:200]}")
        return False
    except Exception as e:
        log(f"  ERROR: block_user failed for user={user_id} — {e}")
        return False


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
    blocked = 0
    start_time = time.time()

    LOG_FILE.write_text("")  # clear log before each run
    log(f"=== Access investigator started — pipeline={PIPELINE} "
        f"duration={DURATION}s max_users={MAX_USERS} block={BLOCK} ===")

    while True:
        elapsed = time.time() - start_time
        if elapsed >= DURATION:
            log(f"Stopped after {int(elapsed)}s.")
            break
        if investigated >= MAX_USERS:
            log(f"Stopped after {investigated} users.")
            break

        rows = fda_query("SELECT DISTINCT user_id FROM access_alerts WHERE user_id IS NOT NULL")
        new_users = [r["user_id"] for r in rows if r["user_id"] not in seen]

        for user_id in new_users:
            if time.time() - start_time >= DURATION or investigated >= MAX_USERS:
                break
            seen.add(user_id)
            context = gather_context(user_id)
            result = classify(context)

            verdict = result["verdict"]
            confidence = result.get("confidence", "?")
            reason = result.get("reason", "")
            signals_str = ", ".join(s["signal_type"] for s in context["signals"])
            log(f"{verdict} [{confidence}] user={user_id} | signals: {signals_str} | {reason}")

            if BLOCK and verdict == "SUSPICIOUS":
                ok = block_user(user_id)
                if ok:
                    log(f"  → BLOCKED user={user_id} (is_banned=true, permissions revoked immediately)")
                    blocked += 1

            investigated += 1

        if time.time() - start_time < DURATION and investigated < MAX_USERS:
            time.sleep(POLL_INTERVAL)

    log(f"=== Done. Investigated {investigated} users, blocked {blocked}. Log: {LOG_FILE} ===")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("\nStopped by user.")
