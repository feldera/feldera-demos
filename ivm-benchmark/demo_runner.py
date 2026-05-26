#!/usr/bin/env python3
"""
demo_runner.py — Feldera vs ClickHouse fraud detection benchmark.

Engines:
  CH-full  (sim 0) — all 4 signals, full O(N) columnar scan per step
  Feldera  (sim 1) — all 4 signals, O(delta) IVM — fast AND complete

Demo modes (--mode):
  latency / full  — CH-full vs Feldera: speed story

Data scales (--data-dir):
  data/0.1x   ~600K transactions  — quick smoke test
  data/1x     ~6M  transactions  — standard demo (default)
  data/10x    ~60M transactions  — maximum latency gap

Usage:
    python3 demo_runner.py --mock                # no DB needed
    python3 demo_runner.py --data-dir data/0.1x  # quick real run
"""

import argparse
import csv as csv_mod
import queue
import random
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import constants
from constants import (
    GIFT_BURST_30D_THRESHOLD, GIFT_BURST_45D_THRESHOLD,  # noqa: F401 (re-exported)
    SPEND_VELOCITY_7D_THRESHOLD, DISPLACEMENT_THRESHOLD,  # noqa: F401
    ALL_SIGNALS,
    N_STEPS, STEP_INTERVAL, PRELOAD_ROWS, DATA_DIR,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD,
    SIM_NAMES, DEMO_MODES,
    MOCK_QUERY_BASE, MOCK_QUERY_GROWTH,
)


# ── Metric ─────────────────────────────────────────────────────────────────────

@dataclass
class MetricPoint:
    sim_id:      int
    step:        int
    wall_time:   float
    label:       str
    n_filtered:  int
    insert_time: float   # start_tx → all pushes ACK'd (Feldera); 0 for CH
    refresh_time: float  # push ACK → commit done, i.e. IVM time (Feldera); 0 for CH
    query_time:  float   # SELECT time (Feldera) or full-scan/MV-lookup time (CH)


# ── Mock helpers ───────────────────────────────────────────────────────────────

def _mock_query(sim_id):
    def _q(step_idx):
        base = MOCK_QUERY_BASE[sim_id] + MOCK_QUERY_GROWTH[sim_id] * step_idx
        q_t  = base + random.uniform(-0.05, 0.15)
        time.sleep(max(0.0, q_t))

        rng = random.Random(step_idx)
        n       = rng.randint(10, 20)
        signals = ALL_SIGNALS
        conf    = "high"

        txns = [{
            "cc_num":       random.randint(10**14, 10**15),
            "ts":           datetime.now(),
            "amt":          random.uniform(20, 4000),
            "category":     random.choice(["gift card", "grocery", "shopping_net"]),
            "signal_type":  random.choice(signals),
            "confidence":   conf,
            "shipping_lat": random.uniform(30, 48),
            "shipping_long":random.uniform(-120, -70),
            "distance":     round(random.uniform(0, 2.5), 3),
            "avg_7day":     round(random.uniform(50, 2000), 2),
        } for _ in range(n)]
        if sim_id == 1:  # Feldera mock: simulated time is IVM refresh
            return txns, 0.0, q_t, 0.0, f"batch {step_idx+1}/{N_STEPS}"
        else:            # CH mock: simulated time is scan/query
            return txns, 0.0, 0.0, q_t, f"batch {step_idx+1}/{N_STEPS}"
    return _q


# ── Simulation worker thread ───────────────────────────────────────────────────

class SimWorker(threading.Thread):
    def __init__(self, sim_id, query_fn, metrics_q, start_event, demo_t0_ref):
        super().__init__(daemon=True, name=SIM_NAMES[sim_id])
        self.sim_id      = sim_id
        self.query_fn    = query_fn
        self.metrics_q   = metrics_q
        self.start_event = start_event
        self.demo_t0_ref = demo_t0_ref

    def run(self):
        self.start_event.wait()

        for i in range(N_STEPS):
            try:
                txns, insert_time, refresh_time, query_time, label = self.query_fn(i)
            except Exception as e:
                print(f"[{self.name}] step {i} error: {e}")
                txns, insert_time, refresh_time, query_time, label = [], 0.0, 0.0, 0.0, f"batch {i+1}/{N_STEPS}"

            snap_t = time.perf_counter() - self.demo_t0_ref[0]

            self.metrics_q.put(MetricPoint(
                sim_id       = self.sim_id,
                step         = i,
                wall_time    = snap_t,
                label        = label,
                n_filtered   = len(txns),
                insert_time  = insert_time,
                refresh_time = refresh_time,
                query_time   = query_time,
            ))



# ── CSV split ──────────────────────────────────────────────────────────────────

def split_csv(data_dir, n_steps, preload_rows=PRELOAD_ROWS, batch_rows=None):
    import json
    data_dir  = Path(data_dir)
    br_tag    = f"_br{batch_rows}" if batch_rows else ""
    cache_dir = data_dir / ".cache" / f"pr{preload_rows}_s{n_steps}{br_tag}"
    meta_file = cache_dir / "meta.json"

    if meta_file.exists():
        meta = json.loads(meta_file.read_text())
        preload = meta["preload"]
        batches = meta["batches"]
        preload["path"] = cache_dir / "preload.csv"
        for i, b in enumerate(batches):
            b["path"] = cache_dir / f"batch_{i:02d}.csv"
        print(f"[split] Using cached split: {cache_dir}")
        return preload, batches

    cache_dir.mkdir(parents=True, exist_ok=True)

    # Count total rows (needed to compute batch_size when batch_rows not given).
    total = sum(1 for _ in open(data_dir / "transactions.csv", newline="")) - 1  # subtract header
    n_stream = max(0, total - preload_rows)

    if batch_rows:
        batch_size     = max(1, batch_rows)
        n_actual_steps = max(1, (n_stream + batch_size - 1) // batch_size)
        if n_steps > 0:
            n_actual_steps = min(n_actual_steps, n_steps)
    else:
        batch_size     = max(1, n_stream // n_steps) if n_steps > 0 else n_stream
        n_actual_steps = n_steps
    print(f"[split] preload={preload_rows:,} rows  streaming={n_stream:,} rows  "
          f"({batch_size:,} rows/batch × {n_actual_steps} steps)")

    header = ["category", "ts", "amt", "cc_num", "shipping_lat", "shipping_long"]

    preload = {"path": cache_dir / "preload.csv", "n_rows": 0, "ts_min": None, "ts_max": None}
    batches = [{"path": cache_dir / f"batch_{i:02d}.csv",
                "n_rows": 0, "ts_min": None, "ts_max": None}
               for i in range(n_actual_steps)]

    all_files  = [preload] + batches
    open_files = [open(str(f["path"]), "w", newline="") for f in all_files]
    writers    = [csv_mod.writer(f) for f in open_files]
    for w in writers:
        w.writerow(header)

    row_idx    = 0
    stream_idx = 0
    with open(data_dir / "transactions.csv", newline="") as f:
        for row in csv_mod.DictReader(f):
            ts = row["ts"]
            r  = [row["category"], ts, row["amt"],
                  row["cc_num"], row["shipping_lat"], row["shipping_long"]]
            if row_idx < preload_rows:
                writers[0].writerow(r)
                _upd(preload, ts)
            else:
                b_idx = stream_idx // batch_size
                if b_idx < n_actual_steps:
                    writers[b_idx + 1].writerow(r)
                    _upd(batches[b_idx], ts)
                stream_idx += 1
            row_idx += 1

    for f in open_files:
        f.close()

    meta = {
        "preload": {k: v for k, v in preload.items() if k != "path"},
        "batches": [{k: v for k, v in b.items() if k != "path"} for b in batches],
    }
    meta_file.write_text(json.dumps(meta, indent=2))

    print(f"  preload : {preload['n_rows']:,} rows")
    for i, b in enumerate(batches):
        print(f"  batch {i:2d}: {b['n_rows']:,} rows")

    return preload, batches


def _upd(b, ts):
    b["n_rows"] += 1
    if b["ts_min"] is None or ts < b["ts_min"]:
        b["ts_min"] = ts
    if b["ts_max"] is None or ts > b["ts_max"]:
        b["ts_max"] = ts


# ── Shared CSV parser ──────────────────────────────────────────────────────────

def _parse_std_rows(path) -> list[dict]:
    """Parse a split-csv batch file into the standardized dict format for push_step()."""
    rows = []
    with open(path, newline="") as f:
        for row in csv_mod.DictReader(f):
            rows.append({
                "category":      row["category"],
                "ts":            row["ts"],
                "amt":           float(row["amt"])           if row["amt"]           else None,
                "cc_num":        int(row["cc_num"]),
                "shipping_lat":  float(row["shipping_lat"])  if row["shipping_lat"]  else None,
                "shipping_long": float(row["shipping_long"]) if row["shipping_long"] else None,
            })
    return rows


# ── Engine coordinator (real mode) ────────────────────────────────────────────

def _build_coordinator(args, active_sims, skip_clickhouse, skip_feldera, api_url, api_key, start_event):
    """Set up engines, load data, return (query_fns, push_threads) for the streaming loop."""
    from collections import defaultdict
    from engine_ch      import ClickHouseFullEngine
    from engine_feldera import FelderaFraudEngine

    engines = []
    if not skip_feldera:
        engines.append(FelderaFraudEngine(api_url, api_key))
    if not skip_clickhouse:
        engines.append(ClickHouseFullEngine(
            args.clickhouse_host, args.clickhouse_port, args.clickhouse_database,
            args.clickhouse_user, args.clickhouse_password))

    # Split CSV and load each storage group in parallel (primary before secondary).
    preload, batches = split_csv(args.data_dir, args.steps, PRELOAD_ROWS, args.batch_rows)
    batches = batches[:N_STEPS]   # honour --max-steps (N_STEPS = min(--max-steps, --steps))
    preload_path = preload["path"] if preload["n_rows"] > 0 else None

    storage_groups = defaultdict(list)
    for e in engines:
        storage_groups[e.storage_id].append(e)

    preload_times: dict = {}
    preload_times_lock  = threading.Lock()

    def _setup_group(group):
        t0 = time.perf_counter()
        primary = group[0]
        for e in group:
            e.setup(preload_path, Path(args.data_dir))
        elapsed = time.perf_counter() - t0
        with preload_times_lock:
            preload_times[primary.name] = {
                "total": elapsed,
                "push":  primary.preload_push_time(),
                "ivm":   primary.preload_ivm_time(),
            }

    print("Loading data …")
    setup_threads = [
        threading.Thread(target=_setup_group, args=(g,), daemon=True)
        for g in storage_groups.values()
    ]
    for t in setup_threads: t.start()
    for t in setup_threads: t.join()
    print("Load complete.\n")

    # Build per-step windows: each step queries only new rows in the current batch.
    # batch_starts[i] = previous win_end (dataset start for step 0).
    _dataset_start = datetime.strptime(batches[0]["ts_min"], "%Y-%m-%d %H:%M:%S") if batches else datetime(1970, 1, 1)
    _max_ts, win_ends = None, []
    for b in batches:
        if _max_ts is None or b["ts_max"] > _max_ts:
            _max_ts = b["ts_max"]
        win_ends.append(datetime.strptime(_max_ts, "%Y-%m-%d %H:%M:%S"))
    batch_starts = [_dataset_start] + win_ends[:-1]

    print("Window schedule:")
    for i, (b, ws, we) in enumerate(zip(batches, batch_starts, win_ends)):
        print(f"  step {i:2d}: {ws.date()} → {we.date()}  ({b['n_rows']:,} new rows)")

    # Fairness barrier: each push worker waits for ALL engines to finish querying
    # step N-1 before pushing step N, so every engine sees identical data timelines.
    step_query_done = {sid: [threading.Event() for _ in range(N_STEPS)] for sid in active_sims}
    all_done_events = [threading.Event() for _ in range(N_STEPS)]

    def _sync_monitor():
        for step in range(N_STEPS):
            for sid in active_sims:
                step_query_done[sid][step].wait()
            all_done_events[step].set()
            print(f"[sync] step {step} complete across all engines")

    threading.Thread(target=_sync_monitor, daemon=True, name="sync-monitor").start()

    # One push thread per storage group; timing is recorded on the primary engine.
    storage_primaries = {s: grp[0] for s, grp in storage_groups.items()}
    push_done = {s: [threading.Event() for _ in range(N_STEPS)] for s in storage_groups}

    def _push_worker(primary, storage_key):
        start_event.wait()
        for step_idx in range(N_STEPS):
            if step_idx > 0:
                all_done_events[step_idx - 1].wait()
            rows = _parse_std_rows(batches[step_idx]["path"])
            print(f"[{primary.name}-push] step {step_idx}: pushing {len(rows):,} rows …")
            primary.push_step(rows)
            push_done[storage_key][step_idx].set()
            print(f"[{primary.name}-push] step {step_idx}: "
                  f"insert={primary.insert_time()*1000:.1f}ms  "
                  f"refresh={primary.refresh_time()*1000:.1f}ms")

    push_threads = [
        threading.Thread(
            target=_push_worker, args=(storage_primaries[s], s),
            daemon=True, name=f"{s}-pusher")
        for s in storage_groups
    ]

    # One query closure per engine; insert/refresh timing comes from the group primary.
    def _query_fn(engine, storage_key):
        primary      = storage_primaries[storage_key]
        sid          = engine.sim_id
        seen_cc_nums = set()   # fraud cards reported in previous steps

        def _fn(step_idx):
            ws = batch_starts[step_idx]
            we = win_ends[step_idx]
            push_done[storage_key][step_idx].wait()
            insert_t  = primary.insert_time()
            refresh_t = primary.refresh_time()
            txns, query_t = engine.query(ws, we)
            # Emit only cards flagged for the first time this step.
            new_txns = [t for t in txns if t["cc_num"] not in seen_cc_nums]
            seen_cc_nums.update(t["cc_num"] for t in txns)
            txns = new_txns
            step_query_done[sid][step_idx].set()
            print(f"[{engine.name}] step {step_idx}: "
                  f"insert {insert_t*1000:.1f}ms  IVM {refresh_t*1000:.1f}ms  "
                  f"query {query_t*1000:.1f}ms  {len(txns)} new alerts")
            return txns, insert_t, refresh_t, query_t, f"batch {step_idx+1}/{N_STEPS}  ({we.date()})"

        return _fn

    query_fns = {e.sim_id: _query_fn(e, e.storage_id) for e in engines}
    split_meta = {
        "preload_rows":   preload["n_rows"],
        "n_batches":      len(batches),
        "rows_per_batch": batches[0]["n_rows"] if batches else 0,
    }
    return query_fns, push_threads, preload_times, split_meta


# ── Sequential benchmark ───────────────────────────────────────────────────────

def _run_sequential_benchmark(args, active_sims, skip_clickhouse, skip_feldera,
                               api_url, api_key) -> tuple:
    """Run each engine group end-to-end before starting the next.

    Each group sets up fresh (preload), runs all N_STEPS steps, then the
    next group starts — no cross-engine CPU/IO contention at any step.
    Returns (perf_data, preload_times, split_meta).
    """
    from collections import defaultdict
    from engine_ch      import ClickHouseFullEngine
    from engine_feldera import FelderaFraudEngine

    engines = []
    if not skip_feldera:
        engines.append(FelderaFraudEngine(api_url, api_key))
    if not skip_clickhouse:
        engines.append(ClickHouseFullEngine(
            args.clickhouse_host, args.clickhouse_port, args.clickhouse_database,
            args.clickhouse_user, args.clickhouse_password))

    preload, batches = split_csv(args.data_dir, args.steps, PRELOAD_ROWS, args.batch_rows)
    batches      = batches[:N_STEPS]
    preload_path = preload["path"] if preload["n_rows"] > 0 else None

    storage_groups = defaultdict(list)
    for e in engines:
        storage_groups[e.storage_id].append(e)

    # Drop empty batches (can occur when preload_rows leaves only a short tail).
    batches = [b for b in batches if b["n_rows"] > 0]

    first_ts = next((b["ts_min"] for b in batches if b["ts_min"]), None)
    _dataset_start = datetime.strptime(first_ts, "%Y-%m-%d %H:%M:%S") if first_ts else datetime(1970, 1, 1)
    _max_ts, win_ends = None, []
    for b in batches:
        if _max_ts is None or b["ts_max"] > _max_ts:
            _max_ts = b["ts_max"]
        win_ends.append(datetime.strptime(_max_ts, "%Y-%m-%d %H:%M:%S"))
    batch_starts = [_dataset_start] + win_ends[:-1]

    perf_data    = {sid: dict(labels=[], wall_times=[], insert_times=[],
                              refresh_times=[], query_times=[], n_filtered=[])
                    for sid in active_sims}
    preload_times = {}

    for storage_key, group in storage_groups.items():
        primary = group[0]

        # ── Setup (preload) ────────────────────────────────────────────────
        print(f"\n{'='*60}")
        print(f"[{primary.name}] Setting up …")
        t0 = time.perf_counter()
        for e in group:
            e.setup(preload_path, Path(args.data_dir))
        elapsed = time.perf_counter() - t0
        preload_times[primary.name] = {
            "total": elapsed,
            "push":  primary.preload_push_time(),
            "ivm":   primary.preload_ivm_time(),
        }
        print(f"[{primary.name}] Load complete ({elapsed:.1f}s). Running {N_STEPS} steps …\n")

        # ── All steps for this engine group ───────────────────────────────
        seen_cc_nums: dict[int, set] = {e.sim_id: set() for e in group}
        for step_idx in range(len(batches)):
            rows = _parse_std_rows(batches[step_idx]["path"])
            ws   = batch_starts[step_idx]
            we   = win_ends[step_idx]

            primary.push_step(rows)
            insert_t  = primary.insert_time()
            refresh_t = primary.refresh_time()

            for engine in group:
                txns, query_t = engine.query(ws, we)
                seen      = seen_cc_nums[engine.sim_id]
                new_txns  = [t for t in txns if t["cc_num"] not in seen]
                seen.update(t["cc_num"] for t in txns)
                print(f"[{engine.name}] step {step_idx + 1:>3}: "
                      f"insert {insert_t*1000:.1f}ms  IVM {refresh_t*1000:.1f}ms  "
                      f"query {query_t*1000:.1f}ms  "
                      f"alerts={len(txns)}  new={len(new_txns)}")

                d = perf_data[engine.sim_id]
                d["labels"].append(f"batch {step_idx+1}/{N_STEPS}  ({we.date()})")
                d["wall_times"].append(step_idx + 1)
                d["insert_times"].append(insert_t)
                d["refresh_times"].append(refresh_t)
                d["query_times"].append(query_t)
                d["n_filtered"].append(len(new_txns))

    split_meta = {
        "preload_rows":   preload["n_rows"],
        "n_batches":      len(batches),
        "rows_per_batch": batches[0]["n_rows"] if batches else 0,
    }
    return perf_data, preload_times, split_meta


# ── Summary table ──────────────────────────────────────────────────────────────

def _fmt_t(seconds: float) -> str:
    return f"{seconds*1000:.0f}ms" if seconds < 1.0 else f"{seconds:.2f}s"


def _run_headless(active_sims: list, workers: list, metrics_q: "queue.Queue") -> dict:
    """Drain metrics_q until all SimWorker threads finish; return perf_data dict."""
    data = {sid: dict(
                labels=[], wall_times=[],
                insert_times=[], refresh_times=[], query_times=[],
                n_filtered=[],
            )
            for sid in active_sims}
    pending: dict = {}

    def _flush():
        while not metrics_q.empty():
            pt = metrics_q.get_nowait()
            if pt.sim_id not in data:
                continue
            pending.setdefault(pt.step, {})[pt.sim_id] = pt
        for step in sorted(pending.keys()):
            for sim_id, pt in pending[step].items():
                d = data[sim_id]
                d["labels"].append(pt.label)
                d["wall_times"].append(step + 1)
                d["insert_times"].append(pt.insert_time)
                d["refresh_times"].append(pt.refresh_time)
                d["query_times"].append(pt.query_time)
                d["n_filtered"].append(pt.n_filtered)
                n_done = sum(len(d["wall_times"]) for d in data.values())
                print(f"\r  step {step+1:3d}/{N_STEPS}  "
                      f"({n_done} pts collected)", end="", flush=True)
        pending.clear()

    print("Running headless — waiting for all steps to complete …")
    while any(w.is_alive() for w in workers):
        _flush()
        time.sleep(0.1)
    _flush()   # drain any remaining items after all threads exit
    print()    # newline after the \r progress line
    return data


def _print_summary(data: dict, active_sims: list,
                   preload_times: "dict | None" = None,
                   split_meta: "dict | None" = None,
                   output_file: "str | None" = None) -> None:
    """Print a per-step latency table; optionally tee to output_file."""
    import sys, io
    n_steps = max((len(data[sid]["wall_times"]) for sid in active_sims), default=0)
    if n_steps == 0:
        return

    buf = io.StringIO()
    _out = [sys.stdout, buf]

    def _p(*args, **kwargs):
        kwargs.pop("flush", None)
        for f in _out:
            print(*args, **kwargs, file=f)

    # Layout: step(4) + engine + ins(7) + ref(7) + qry(7) + total(7) + n(4) + spacing
    ENG_W = max(len(SIM_NAMES[sid]) for sid in active_sims)
    T     = 7
    N     = 4
    total_w = 2 + 4 + 2 + ENG_W + 2 + T + 2 + T + 2 + T + 2 + T + 2 + N
    sep  = "━" * total_w
    thin = "─" * total_w

    def _row(step_label, name, ins, ref, qry, total="", n=""):
        return (f"  {step_label:>4}  {name:<{ENG_W}}"
                f"  {ins:>{T}}  {ref:>{T}}  {qry:>{T}}  {total:>{T}}  {n:>{N}}")

    _p(f"\n{sep}")
    if split_meta:
        _p(f"# rows: preload={split_meta['preload_rows']:,}"
           f"  batches={split_meta['n_batches']} × ~{split_meta['rows_per_batch']:,}/step")
    if preload_times:
        parts = []
        for name, t in preload_times.items():
            if isinstance(t, dict):
                s = f"{name}: {_fmt_t(t['total'])}"
                if t["push"] or t["ivm"]:
                    s += f" (push={_fmt_t(t['push'])}, ivm={_fmt_t(t['ivm'])})" if t["ivm"] \
                         else f" (ins={_fmt_t(t['push'])})"
            else:
                s = f"{name}: {_fmt_t(t)}"
            parts.append(s)
        _p(f"  PRELOAD  {'   '.join(parts)}")
    _p("  STEP LATENCY SUMMARY")
    _p(sep)
    _p(_row("step", "engine", "ins", "ref+qry", "qry", "total", "n"))
    _p(thin)

    for step in range(n_steps):
        for i, sid in enumerate(active_sims):
            d           = data[sid]
            step_label  = str(step + 1) if i == 0 else ""
            name        = SIM_NAMES[sid]
            if step >= len(d["wall_times"]):
                _p(_row(step_label, name, "—", "—", "—", "—"))
            else:
                is_feldera = any(d["refresh_times"])
                ins_t = d["insert_times"][step]
                ref_t = d["refresh_times"][step]
                qry_t = d["query_times"][step]
                if is_feldera:
                    ref_str = _fmt_t(ref_t)
                    qry_str = _fmt_t(qry_t)
                else:
                    ref_str = _fmt_t(ref_t + qry_t)
                    qry_str = "—"
                _p(_row(
                    step_label, name,
                    _fmt_t(ins_t), ref_str, qry_str,
                    _fmt_t(ins_t + ref_t + qry_t),
                    d["n_filtered"][step],
                ))
        _p(thin)

    # Average / total rows.
    _p(_row("avg", "", "ins", "ref+qry", "qry", "total", ""))
    _p(thin)
    for sid in active_sims:
        d = data[sid]
        n = len(d["wall_times"])
        if n:
            is_feldera = any(d["refresh_times"])
            avg_ins = sum(d["insert_times"]) / n
            avg_ref = sum(d["refresh_times"]) / n
            avg_qry = sum(d["query_times"]) / n
            if is_feldera:
                ref_str = _fmt_t(avg_ref)
                qry_str = _fmt_t(avg_qry)
            else:
                ref_str = _fmt_t(avg_ref + avg_qry)
                qry_str = "—"
            _p(_row(
                "", SIM_NAMES[sid],
                _fmt_t(avg_ins), ref_str, qry_str,
                _fmt_t(avg_ins + avg_ref + avg_qry), "",
            ))
    _p(sep)
    _p("")

    if output_file:
        with open(output_file, "w") as fh:
            fh.write(buf.getvalue())
        print(f"[summary] saved to {output_file}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    global N_STEPS, STEP_INTERVAL, PRELOAD_ROWS
    parser = argparse.ArgumentParser()
    parser.add_argument("--output",       default=None, metavar="FILE",
                        help="Save summary table to this file (e.g. experiments/results.txt)")
    parser.add_argument("--mock",         action="store_true",
                        help="Simulate queries (no DB needed)")
    parser.add_argument("--mode",         default="full",
                        choices=list(DEMO_MODES),
                        help="latency | accuracy | full  (default: full)")
    parser.add_argument("--no-feldera",   action="store_true")
    parser.add_argument("--no-clickhouse", action="store_true",
                        help="Disable ClickHouse engine")
    parser.add_argument("--data-dir",     default=DATA_DIR,
                        help="data/0.1x | data/1x | data/10x  (default: data/1x)")
    parser.add_argument("--clickhouse-host",     default=CLICKHOUSE_HOST)
    parser.add_argument("--clickhouse-port",     type=int, default=CLICKHOUSE_PORT)
    parser.add_argument("--clickhouse-database", default=CLICKHOUSE_DATABASE)
    parser.add_argument("--clickhouse-user",     default=CLICKHOUSE_USERNAME)
    parser.add_argument("--clickhouse-password", default=CLICKHOUSE_PASSWORD)
    parser.add_argument("--api-url",      default=None)
    parser.add_argument("--api-key",      default=None)
    parser.add_argument("--steps",        type=int,   default=N_STEPS)
    parser.add_argument("--max-steps",    type=int,   default=None,
                        help="Halt streaming after this many steps. Use --steps "
                             "for cache split layout, --max-steps to stop early. "
                             "Default: run all --steps batches.")
    parser.add_argument("--interval",     type=float, default=STEP_INTERVAL,
                        help="Seconds between batches (default 10)")
    parser.add_argument("--preload-rows",  type=int,   default=PRELOAD_ROWS,
                        help="Number of rows to load as history before streaming starts "
                             "(default: 0 — stream everything).")
    parser.add_argument("--batch-rows",    type=int,   default=None,
                        help="Fix each streaming batch to exactly this many rows. "
                             "The actual number of steps is derived from the data. "
                             "Overrides the even-split default.")
    parser.add_argument("--sequential",   action="store_true",
                        help="Run engines one at a time per step (no parallelism) "
                             "for clean isolated timing measurements")
    args = parser.parse_args()

    from engine_feldera import DEFAULT_API_URL, DEFAULT_API_KEY

    # ── Connectivity pre-check ─────────────────────────────────────────────
    if not args.mock:
        _need_clickhouse      = 0 in list(DEMO_MODES[args.mode]) and not args.no_clickhouse
        _need_feldera = 1 in list(DEMO_MODES[args.mode]) and not args.no_feldera
        errors = []
        if _need_clickhouse:
            try:
                host = args.clickhouse_host
                port = args.clickhouse_port
                s = socket.create_connection((host, port), timeout=3)
                s.close()
            except OSError:
                errors.append(
                    f"ClickHouse unreachable at {args.clickhouse_host}:{args.clickhouse_port}\n"
                    f"  Start it with:  docker start clickhouse-server\n"
                    f"  Or:  docker run -d --name clickhouse-server "
                    f"-p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server"
                )
        if _need_feldera:
            import urllib.parse as _up
            _parsed = _up.urlparse(args.api_url or DEFAULT_API_URL)
            _fhost  = _parsed.hostname or "localhost"
            _fport  = _parsed.port or 8080
            try:
                s = socket.create_connection((_fhost, _fport), timeout=3)
                s.close()
            except OSError:
                errors.append(
                    f"Feldera unreachable at {_fhost}:{_fport}\n"
                    f"  Start it with:  docker start feldera\n"
                    f"  Or:  docker run -d --name feldera -p 8080:8080 "
                    f"images.feldera.com/feldera/pipeline-manager:latest"
                )
        if errors:
            print("\nERROR: required services are not reachable:\n")
            for e in errors:
                print(f"  {e}\n")
            raise SystemExit(1)

    # Cache layout uses the requested --steps; --max-steps caps execution.
    PLAN_STEPS    = args.steps
    N_STEPS       = min(args.max_steps, PLAN_STEPS) if args.max_steps is not None else PLAN_STEPS
    STEP_INTERVAL = args.interval
    PRELOAD_ROWS = args.preload_rows
    if N_STEPS < PLAN_STEPS:
        print(f"[max-steps] running {N_STEPS} of {PLAN_STEPS} planned batches "
              f"(cache layout from --steps {PLAN_STEPS})")

    active_sims = list(DEMO_MODES[args.mode])
    if args.no_clickhouse:
        active_sims = [s for s in active_sims if s != 0]
    if args.no_feldera:
        active_sims = [s for s in active_sims if s != 1]

    api_url = args.api_url or DEFAULT_API_URL
    api_key = args.api_key or DEFAULT_API_KEY

    skip_clickhouse      = 0 not in active_sims
    skip_feldera = 1 not in active_sims

    start_event  = threading.Event()
    query_fns    = {}
    push_threads = []

    preload_times: dict = {}
    split_meta:    dict = {}
    if args.mock:
        for sid in active_sims:
            query_fns[sid] = _mock_query(sid)
    elif args.sequential:
        perf_data, preload_times, split_meta = _run_sequential_benchmark(
            args, active_sims, skip_clickhouse, skip_feldera, api_url, api_key)
        _print_summary(perf_data, active_sims, preload_times,
                       split_meta=split_meta, output_file=args.output)
        return
    else:
        query_fns, push_threads, preload_times, split_meta = _build_coordinator(
            args, active_sims, skip_clickhouse, skip_feldera, api_url, api_key, start_event)

    # ── Start workers ──────────────────────────────────────────────────────
    metrics_q   = queue.Queue()
    demo_t0_ref = [None]

    workers = [SimWorker(sid, query_fns[sid],
                         metrics_q, start_event, demo_t0_ref)
               for sid in active_sims if sid in query_fns]
    for w in workers:
        w.start()
    for t in push_threads:
        t.start()

    sim_names_str = "  |  ".join(SIM_NAMES[sid] for sid in active_sims)
    print(f"\nDemo [{args.mode}]: {sim_names_str}")
    print(f"Steps: {N_STEPS} × {STEP_INTERVAL}s  "
          f"({'mock' if args.mock else 'real'} mode)")

    demo_t0_ref[0] = time.perf_counter()
    start_event.set()

    perf_data = _run_headless(active_sims, workers, metrics_q)

    _print_summary(perf_data, active_sims, preload_times or None,
                   split_meta=split_meta or None,
                   output_file=args.output)


if __name__ == "__main__":
    main()
