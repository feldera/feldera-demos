#!/usr/bin/env python3
"""runner.py — realistic scoring benchmark.

The detection logic is IDENTICAL across all engines: a card is suspicious if its gift-card count
over a trailing 30-day window reaches 23. The engines differ ONLY in how faithfully they compute
that signal:
  feldera        — exact rolling window (native IVM).
  postgres_ivm   — over-covering 2-adjacent-bucket proxy (can't maintain a rolling window).
  clickhouse_ivm — same proxy.

Per engine: stream the batches (recording per-step flagged-card + suspicious-transaction counts),
then take the FINAL flagged card set and score it against labels.csv (planted ground truth):
  true positives  = flagged fraud cards
  false positives = flagged legitimate cards (the proxy's over-coverage) -> wasted LLM analysis
  false negatives = fraud cards missed
Saves results/metrics.csv (per-step flagged + suspicious-txn counts) and results/score.csv
(confusion matrix + false-positive workload that drives the LLM cost).

Usage:
  python3 src/runner.py --data data/demo --preload 30 --steps 60
"""

import argparse
import csv
import getpass
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE  = Path(__file__).resolve().parent.parent   # demo root (this file lives in src/)
GEN   = HERE / "generated"                        # all SQL: detectors + composed schema.<engine>.sql
BENCH = HERE.parent / "rolling-aggregate-bench" / "src"
sys.path.insert(0, str(BENCH))

TXN_COLS = ["category", "ts", "amt", "cc_num", "shipping_lat", "shipping_long"]


def _batches(data: Path, steps: int):
    files = sorted((data / "batches").glob("batch_*.csv"))
    return files[:steps] if steps else files


def _ch_rows(path):
    out = []
    for r in csv.DictReader(open(path)):
        out.append([r["category"], datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
                    float(r["amt"]), int(r["cc_num"]), float(r["shipping_lat"]), float(r["shipping_long"])])
    return out


def _feldera_rows(path):
    return [{"category": r["category"], "ts": r["ts"], "amt": float(r["amt"]), "cc_num": int(r["cc_num"]),
             "shipping_lat": float(r["shipping_lat"]), "shipping_long": float(r["shipping_long"])}
            for r in csv.DictReader(open(path))]


def load_labels(data: Path):
    """cc_num -> ground-truth class. fraud cards SHOULD be flagged; borderline should NOT."""
    fraud, borderline = set(), set()
    with open(data / "labels.csv") as f:
        for r in csv.DictReader(f):
            (fraud if r["label"] == "fraud" else borderline).add(int(r["cc_num"]))
    return fraud, borderline


def schema_sql(engine: str) -> str:
    """The engine's COMPLETE program (tables + flagged_card view), read from
    generated/schema.<engine>.sql (gen_demo_data.py composes it there, next to the detector views).
    Comments stripped. NO DDL is hardcoded in this runner."""
    text = (GEN / f"schema.{engine}.sql").read_text()
    return "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--")).strip()


def _exec_schema(engine, run_stmt):
    """Run each ';'-separated statement (tables + the flagged_card view) from the engine's COMPLETE
    schema file via run_stmt."""
    for stmt in (s.strip() for s in schema_sql(engine).split(";")):
        if stmt:
            run_stmt(stmt)


# ── PostgreSQL (db tokens_demo) ──────────────────────────────────────────────────
def run_pg_stream(data, batches, record, preload=0):
    from engine_postgres import _connect
    pg_user = os.environ.get("PGUSER") or getpass.getuser()   # local-socket peer auth → current OS user
    conn = _connect("/var/run/postgresql", 5432, "tokens_demo", pg_user, "")
    cur = conn.cursor()
    cur.execute("DROP VIEW IF EXISTS flagged_card")
    cur.execute("DROP TABLE IF EXISTS transactions"); cur.execute("DROP TABLE IF EXISTS customer")
    _exec_schema("postgres_ivm", cur.execute)   # tables + flagged_card view, from generated/schema.postgres_ivm.sql
    conn.commit()
    with open(data / "customers.csv") as f:
        cur.copy_expert("COPY customer (cc_num,name,lat,long) FROM STDIN CSV HEADER", f)
    cur.execute("CREATE INDEX ON transactions (cc_num, ts)"); conn.commit()
    pre, stream = batches[:preload], batches[preload:]
    for bf in pre:                                   # PRELOAD history (bulk, unmeasured)
        with open(bf) as f:
            cur.copy_expert(f"COPY transactions ({','.join(TXN_COLS)}) FROM STDIN CSV HEADER", f)
    cur.execute("ANALYZE transactions"); conn.commit()
    print(f"  [pg] preloaded {len(pre)} batches; streaming {len(stream)} …")
    flagged = set()
    for step, bf in enumerate(stream):
        with open(bf) as f:
            cur.copy_expert(f"COPY transactions ({','.join(TXN_COLS)}) FROM STDIN CSV HEADER", f)
        conn.commit()
        cur.execute("SELECT cc_num FROM flagged_card"); rows = cur.fetchall()
        flagged = {int(r[0]) for r in rows}
        susp = 0  # suspicious txns SO FAR = transactions of currently-flagged cards (LLM workload)
        if flagged:
            cur.execute("SELECT count(*) FROM transactions WHERE cc_num = ANY(%s)", (list(flagged),))
            susp = int(cur.fetchone()[0])
        record(step, "postgres_ivm", len(flagged), susp)
    cur.execute("SELECT cc_num, count(*) FROM transactions GROUP BY cc_num")
    card_txns = {int(r[0]): int(r[1]) for r in cur.fetchall()}
    conn.close()
    return flagged, card_txns


# ── ClickHouse (db fraud_detection_light, tok_ tables) ──────────────────────────
def run_ch_stream(data, batches, record, preload=0):
    from engine_clickhouse import _connect
    client = _connect("localhost", 8123, "fraud_detection_light", "demo", "")
    # drop any prior demo objects (base tables + incremental MVs + the flagged_card view);
    # views/MVs before tables. Detector-agnostic: anything named tok_* or flagged_card.
    objs = client.query("SELECT name, engine FROM system.tables WHERE database = 'fraud_detection_light' "
                        "AND (name LIKE 'tok_%' OR name = 'flagged_card')").result_rows
    for name, engine in sorted(objs, key=lambda r: 0 if r[1].endswith("View") else 1):
        client.command(f"DROP {'VIEW' if engine.endswith('View') else 'TABLE'} IF EXISTS {name}")
    _exec_schema("clickhouse_ivm", client.command)   # base tables + incremental MVs + flagged_card view
    client.insert("tok_customer", [[int(r["cc_num"]), r["name"], float(r["lat"]), float(r["long"])]
                                   for r in csv.DictReader(open(data / "customers.csv"))],
                  column_names=["cc_num", "name", "lat", "long"])
    pre, stream = batches[:preload], batches[preload:]
    for bf in pre:                                   # PRELOAD history (bulk, unmeasured)
        client.insert("tok_transactions", _ch_rows(bf), column_names=TXN_COLS)
    print(f"  [ch] preloaded {len(pre)} batches; streaming {len(stream)} …")
    flagged = set()
    for step, bf in enumerate(stream):
        client.insert("tok_transactions", _ch_rows(bf), column_names=TXN_COLS)
        res = client.query("SELECT cc_num FROM flagged_card")
        flagged = {int(r[0]) for r in res.result_rows}
        susp = 0  # suspicious txns SO FAR = transactions of currently-flagged cards (LLM workload)
        if flagged:
            ids = ",".join(str(c) for c in flagged)
            susp = int(client.query(f"SELECT count() FROM tok_transactions WHERE cc_num IN ({ids})").result_rows[0][0])
        record(step, "clickhouse_ivm", len(flagged), susp)
    res = client.query("SELECT cc_num, count() FROM tok_transactions GROUP BY cc_num")
    card_txns = {int(r[0]): int(r[1]) for r in res.result_rows}
    return flagged, card_txns


# ── Feldera (pipeline) ──────────────────────────────────────────────────────────
def run_feldera_stream(data, batches, record, preload=0):
    import engine_feldera as ef
    ef._PIPELINE = "tokens-demo-runner"
    from engine_feldera import _FelderaEngine, DEFAULT_API_URL, DEFAULT_API_KEY
    eng = _FelderaEngine(DEFAULT_API_URL, DEFAULT_API_KEY)
    # the complete program (tables + detector views) lives in generated/schema.feldera.sql
    eng.setup(schema_sql("feldera"))
    customers = [{"cc_num": int(r["cc_num"]), "name": r["name"], "lat": float(r["lat"]), "long": float(r["long"])}
                 for r in csv.DictReader(open(data / "customers.csv"))]
    eng.start_transaction(); eng.push("CUSTOMER", customers)
    eng.wait_for_ingestion(len(customers)); eng.commit_transaction()
    pre, stream = batches[:preload], batches[preload:]
    for bf in pre:                                   # PRELOAD history (bulk, unmeasured)
        rows = _feldera_rows(bf)
        eng.start_transaction(); eng.push("TRANSACTION", rows)
        eng.wait_for_ingestion(len(rows)); eng.commit_transaction()
    print(f"  [feldera] preloaded {len(pre)} batches; streaming {len(stream)} …")
    flagged = set()
    for step, bf in enumerate(stream):
        rows = _feldera_rows(bf)
        eng.start_transaction(); eng.push("TRANSACTION", rows)
        eng.wait_for_ingestion(len(rows)); eng.commit_transaction()
        res = eng.query("SELECT cc_num FROM flagged_card")
        flagged = {int(r["cc_num"]) for r in res}
        susp = 0  # suspicious txns SO FAR = transactions of currently-flagged cards (LLM workload)
        if flagged:
            ids = ",".join(str(c) for c in flagged)
            ct = eng.query(f"SELECT n FROM card_txn WHERE cc_num IN ({ids})")
            susp = sum(int(r["n"]) for r in ct)
        record(step, "feldera", len(flagged), susp)
    ct = eng.query("SELECT cc_num, n FROM card_txn")
    card_txns = {int(r["cc_num"]): int(r["n"]) for r in ct}
    try:
        eng._pipeline.stop(force=True); eng._pipeline.delete()
    except Exception:
        pass
    return flagged, card_txns


def score(engine, flagged, card_txns, fraud, borderline):
    tp = len(flagged & fraud)
    fp = len(flagged - fraud)
    fn = len(fraud - flagged)
    susp_txns = sum(card_txns.get(cc, 0) for cc in flagged)
    fp_txns   = sum(card_txns.get(cc, 0) for cc in flagged if cc not in fraud)
    prec = tp / (tp + fp) if (tp + fp) else 1.0
    rec  = tp / (tp + fn) if (tp + fn) else 1.0
    return {"engine": engine, "flagged": len(flagged), "tp": tp, "fp": fp, "fn": fn,
            "precision": prec, "recall": rec, "suspicious_txns": susp_txns, "fp_txns": fp_txns}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(HERE / "data" / "demo"))
    ap.add_argument("--preload", type=int, default=0,
                    help="batches (days) to bulk-load as history BEFORE the measured stream (unmeasured)")
    ap.add_argument("--steps", type=int, default=100, help="measured streaming steps after the preload")
    ap.add_argument("--out", default=str(HERE / "results" / "metrics.csv"))
    ap.add_argument("--score-out", default=str(HERE / "results" / "score.csv"))
    args = ap.parse_args()
    data = Path(args.data)

    # each engine's COMPLETE program (tables + flagged_card view) is its generated/schema.<engine>.sql
    def has(e): return (GEN / f"schema.{e}.sql").exists()
    batches = _batches(data, args.preload + args.steps)   # first --preload are history, rest are measured
    if not batches:
        sys.exit(f"no batches in {data}/batches — run gen_demo_data.py first")
    if len(batches) < args.preload + args.steps:
        sys.exit(f"need {args.preload}+{args.steps} batches but only {len(batches)} exist — regenerate with more --batches")
    fraud, borderline = load_labels(data)

    metrics = []
    def record(step, engine, n, susp): metrics.append((step, engine, n, susp))
    results = {}

    print(f"data: {data}   preload: {args.preload}   measured steps: {args.steps}   "
          f"ground truth: {len(fraud)} fraud, {len(borderline)} borderline-legit\n")
    if has("postgres_ivm"):
        print("PostgreSQL:"); results["postgres_ivm"] = run_pg_stream(data, batches, record, args.preload)
    if has("clickhouse_ivm"):
        print("ClickHouse:"); results["clickhouse_ivm"] = run_ch_stream(data, batches, record, args.preload)
    if has("feldera"):
        print("Feldera:");    results["feldera"] = run_feldera_stream(data, batches, record, args.preload)

    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["step", "engine", "flagged_cards", "susp_txns"]); w.writerows(metrics)
    print(f"\nsaved {len(metrics)} per-step rows → {out}")

    rows = [score(e, fl_set, ct, fraud, borderline) for e, (fl_set, ct) in results.items()]
    order = {"feldera": 0, "postgres_ivm": 1, "clickhouse_ivm": 2}
    rows.sort(key=lambda r: order.get(r["engine"], 9))
    with open(args.score_out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)

    print(f"\n{'='*92}\n  SCORE vs exact ground truth ({len(fraud)} fraud should flag, {len(borderline)} borderline should NOT)\n{'='*92}")
    print(f"  {'engine':<16} {'flagged':>8} {'TP':>5} {'FP':>7} {'FN':>4} {'precision':>10} {'recall':>8} {'susp_txns':>11} {'wasted(FP)':>11}")
    for r in rows:
        tag = "  ← exact" if r["engine"] == "feldera" else ""
        print(f"  {r['engine']:<16} {r['flagged']:>8,} {r['tp']:>5} {r['fp']:>7,} {r['fn']:>4} "
              f"{r['precision']*100:>9.1f}% {r['recall']*100:>7.1f}% {r['suspicious_txns']:>11,} {r['fp_txns']:>11,}{tag}")
    print(f"\n  Same detector + same threshold (23) everywhere; the only difference is signal fidelity.")
    print(f"  FP = legitimate cards the over-covering bucket proxy flags that exact IVM clears.")
    print(f"  wasted(FP) txns = the LLM-analysis workload spent on false positives → feed to cost_estimate.py.")


if __name__ == "__main__":
    main()
