#!/usr/bin/env python3
"""
engine_ch.py — ClickHouse FraudEngine implementation.

ClickHouseFullEngine  (sim 0, "CH-full")
  Primary for the "clickhouse" storage group.  Owns schema setup, customer
  load, and all INSERTs.  Runs a full O(N) columnar scan on every query —
  latency grows with history size.
"""

import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import constants as _c
from constants import clickhouse_functions_sql
from engine_base import FraudEngine

_SQL_DIR      = Path(__file__).parent / "sql"
_CHUNK_SIZE   = 250_000   # rows per INSERT — keeps memory bounded during large preloads
_PUSH_WORKERS = 10           # parallel INSERT threads during preload

_FULL_TABLES_SQL = (_SQL_DIR / "ch_full_tables.sql").read_text()
_FULL_VIEWS_SQL  = (_SQL_DIR / "ch_full_views.sql").read_text()
_FULL_QUERY      = (_SQL_DIR / "ch_full_query.sql").read_text().strip()


# ── Low-level helpers ──────────────────────────────────────────────────────────

def _exec_sql(client, sql: str) -> None:
    """Execute a SQL string split on ';', skipping comment-only blocks."""
    for stmt in sql.split(";"):
        lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
        stmt  = "\n".join(lines).strip()
        if stmt:
            client.command(stmt)


def _connect(host: str, port: int, database: str, user: str, password: str):
    try:
        import clickhouse_connect
    except ImportError:
        raise ImportError("Run: pip install clickhouse-connect")

    tmp = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database="default")
    tmp.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database)


def _insert(conn: dict, table: str, columns: list[str], rows: list[list]) -> None:
    """Parallel chunked INSERT. Each worker gets its own client so concurrent
    INSERTs don't share a clickhouse-connect session (which would conflict)."""
    import clickhouse_connect
    chunks = [rows[i: i + _CHUNK_SIZE] for i in range(0, len(rows), _CHUNK_SIZE)]
    def _worker(chunk):
        c = clickhouse_connect.get_client(**conn)
        c.insert(table, chunk, column_names=columns)
    with ThreadPoolExecutor(max_workers=_PUSH_WORKERS) as pool:
        futs = [pool.submit(_worker, c) for c in chunks]
        for f in as_completed(futs):
            f.result()


def _stream_insert(conn: dict, table: str, columns: list[str], csv_path: Path,
                   row_parser) -> int:
    """Stream-insert from CSV in parallel chunks without loading all rows into memory.

    Reads _CHUNK_SIZE rows at a time and submits each chunk to a thread pool,
    keeping at most _PUSH_WORKERS inserts in flight simultaneously.
    """
    import clickhouse_connect
    total = 0

    def _flush(c):
        client = clickhouse_connect.get_client(**conn)
        client.insert(table, c, column_names=columns)

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        chunk: list = []
        with ThreadPoolExecutor(max_workers=_PUSH_WORKERS) as pool:
            pending = []
            for raw in reader:
                chunk.append(row_parser(raw))
                if len(chunk) >= _CHUNK_SIZE:
                    pending.append(pool.submit(_flush, chunk))
                    total += len(chunk)
                    chunk = []
                    # Drain one future if too many in flight to bound memory
                    if len(pending) >= _PUSH_WORKERS:
                        pending.pop(0).result()
            if chunk:
                pending.append(pool.submit(_flush, chunk))
                total += len(chunk)
            for f in pending:
                f.result()
    return total


def _to_clickhouse_rows(rows: list[dict]) -> list[list]:
    """Convert standardized push_step() dicts to list[list] for CH INSERT."""
    _fi = datetime.fromisoformat
    _utc = timezone.utc
    return [
        [
            r["cc_num"],
            _fi(r["ts"]).replace(tzinfo=_utc),
            r["amt"]           if r["amt"]           is not None else 0.0,
            r["category"],
            r["shipping_lat"]  if r["shipping_lat"]  is not None else 0.0,
            r["shipping_long"] if r["shipping_long"] is not None else 0.0,
        ]
        for r in rows
    ]


def _parse_result(result) -> list[dict]:
    """Two shapes are accepted:
    (a) full rows: cc_num, signal_type, amt, ... — one dict per row
    (b) count-only: single row with column 'n_alerts' — we synthesize N stub
        dicts with deterministic cc_nums (-1..-N) so the demo's seen_cc_nums
        dedup reports the per-step *delta* (= newly flagged cards) instead of
        the cumulative count.
    """
    rows_data = list(result.named_results())
    # Count-only shape
    if len(rows_data) == 1 and "n_alerts" in rows_data[0] and "cc_num" not in rows_data[0]:
        n = int(rows_data[0]["n_alerts"])
        return [{
            "cc_num":          -(i + 1),               # deterministic: same ids → dedup catches them
            "ts":              0,
            "amt":             0.0,
            "category":        "",
            "shipping_lat":    0.0,
            "shipping_long":   0.0,
            "distance":        0.0,
            "avg_7day":        0.0,
            "signal_type":     "count",
            "confidence":      "high",
            "review_priority": 0.0,
        } for i in range(n)]
    # Full-row shape (legacy / --llm friendly)
    rows = []
    for row in rows_data:
        rows.append({
            "cc_num":          row["cc_num"],
            "ts":              row.get("ts", 0),
            "amt":             float(row.get("amt") or 0),
            "category":        row.get("category", ""),
            "shipping_lat":    float(row.get("shipping_lat") or 0),
            "shipping_long":   float(row.get("shipping_long") or 0),
            "distance":        float(row.get("distance") or 0),
            "avg_7day":        0.0,
            "signal_type":     row["signal_type"],
            "confidence":      row.get("confidence", "high"),
            "review_priority": float(row.get("review_priority") or 0),
        })
    return rows


# ── Shared base ────────────────────────────────────────────────────────────────

class _ClickHouseBase(FraudEngine):
    storage_id = "clickhouse"

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self._host          = host
        self._port          = port
        self._database      = database
        self._user          = user
        self._password      = password
        self._client        = None
        self._insert_t      = 0.0
        self._preload_ins_t = 0.0

    def _get_client(self):
        if self._client is None:
            self._client = _connect(
                self._host, self._port, self._database, self._user, self._password)
        return self._client

    def _conn_kwargs(self) -> dict:
        return dict(host=self._host, port=self._port, database=self._database,
                    username=self._user, password=self._password)

    def insert_time(self) -> float:
        return self._insert_t

    def preload_push_time(self) -> float:
        return self._preload_ins_t


# ── Engines ────────────────────────────────────────────────────────────────────

class ClickHouseFullEngine(_ClickHouseBase):
    sim_id = 0
    name   = "CH-full"

    def setup(self, preload_path: "Path | None", data_dir: Path) -> None:
        client = self._get_client()
        _exec_sql(client, _FULL_TABLES_SQL)
        _exec_sql(client, clickhouse_functions_sql(
            gb30=_c.GIFT_BURST_30D_THRESHOLD,
            gb45=_c.GIFT_BURST_45D_THRESHOLD,
            sv7=_c.SPEND_VELOCITY_7D_THRESHOLD,
            disp=_c.DISPLACEMENT_THRESHOLD,
            prio=_c.SIGNAL_PRIORITY,
        ))
        client.command("TRUNCATE TABLE IF EXISTS transactions")
        client.command("TRUNCATE TABLE IF EXISTS customers")
        print("[CH] Tables ready.")

        rows = []
        with open(Path(data_dir) / "customers.csv", newline="") as f:
            for row in csv.DictReader(f):
                rows.append([
                    int(row["cc_num"]),
                    row["name"],
                    float(row["lat"])  if row["lat"]  else 0.0,
                    float(row["long"]) if row["long"] else 0.0,
                ])
        _insert(self._conn_kwargs(), "customers", ["cc_num", "name", "lat", "long"], rows)
        print(f"[CH] {len(rows):,} customers inserted.")

        if preload_path is not None:
            def _parse_txn(row):
                return [
                    int(row["cc_num"]),
                    datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
                    float(row["amt"])           if row["amt"]           else 0.0,
                    row["category"],
                    float(row["shipping_lat"])  if row["shipping_lat"]  else 0.0,
                    float(row["shipping_long"]) if row["shipping_long"] else 0.0,
                ]
            t0 = time.perf_counter()
            n = _stream_insert(self._conn_kwargs(), "transactions",
                               ["cc_num", "ts", "amt", "category", "shipping_lat", "shipping_long"],
                               preload_path, _parse_txn)
            self._preload_ins_t = time.perf_counter() - t0
            print(f"[CH] {n:,} preload rows in {self._preload_ins_t:.1f}s")

        _exec_sql(client, _FULL_VIEWS_SQL)
        print("[CH] fraud_signals_full + count views created.")

    def push_step(self, rows: list[dict]) -> None:
        t0 = time.perf_counter()
        self._get_client().insert("transactions", _to_clickhouse_rows(rows),
            column_names=["cc_num", "ts", "amt", "category", "shipping_lat", "shipping_long"])
        self._insert_t = time.perf_counter() - t0

    def query(self, win_start: datetime, win_end: datetime) -> tuple[list[dict], float]:
        t0  = time.perf_counter()
        result = self._get_client().query(_FULL_QUERY)
        return _parse_result(result), time.perf_counter() - t0


