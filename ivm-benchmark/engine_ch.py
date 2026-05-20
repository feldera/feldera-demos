#!/usr/bin/env python3
"""
engine_ch.py — ClickHouse FraudEngine implementations.

ClickHouseFullEngine  (sim 0, "CH-full")
  Primary for the "clickhouse" storage group.  Owns schema setup, customer
  load, and all INSERTs.  Runs a full O(N) columnar scan on every query —
  latency grows with history size.

ClickHouseMVEngine  (sim 1, "CH-light")
  Secondary.  SummingMergeTree MVs provide O(delta) query cost for three
  signals; repeated_displacement uses worst-case approximation (no customer JOIN, no distance check).
  setup() and push_step() are no-ops — ClickHouseFullEngine does both.
"""

import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import constants as _c
from engine_base import FraudEngine

_SQL_DIR      = Path(__file__).parent / "sql"
_CHUNK_SIZE   = 1_000_000   # rows per INSERT — keeps memory bounded during large preloads
_PUSH_WORKERS = 16           # parallel INSERT threads during preload

_VIEW_TAIL      = (_SQL_DIR / "ch_view_tail.sql").read_text()
_FULL_VIEW_DDL  = (_SQL_DIR / "ch_full_head.sql").read_text()  + _VIEW_TAIL.replace("__CONFIDENCE__", "high")
_LIGHT_VIEW_DDL = (_SQL_DIR / "ch_light_head.sql").read_text() + _VIEW_TAIL.replace("__CONFIDENCE__", "medium")
_FULL_COUNT_DDL  = "CREATE OR REPLACE VIEW fraud_alert_count_full  AS SELECT count(DISTINCT cc_num) AS n_alerts FROM fraud_signals_full"
_LIGHT_COUNT_DDL = "CREATE OR REPLACE VIEW fraud_alert_count_light AS SELECT count(DISTINCT cc_num) AS n_alerts FROM fraud_signals_light"
_FULL_QUERY     = (_SQL_DIR / "ch_full_query.sql").read_text()
_LIGHT_QUERY    = (_SQL_DIR / "ch_light_query.sql").read_text()


def _substitute(sql: str) -> str:
    """Replace threshold and priority placeholders from constants."""
    return (sql
        .replace("__GB30__",      str(_c.GIFT_BURST_30D_THRESHOLD))
        .replace("__GB45__",      str(_c.GIFT_BURST_45D_THRESHOLD))
        .replace("__SV7__",       str(_c.SPEND_VELOCITY_7D_THRESHOLD))
        .replace("__DISP__",      str(_c.DISPLACEMENT_THRESHOLD))
        .replace("__PRIO_GB30__", str(_c.SIGNAL_PRIORITY["gift_card_burst_30d"]))
        .replace("__PRIO_GB45__", str(_c.SIGNAL_PRIORITY["gift_card_burst_45d"]))
        .replace("__PRIO_SV7__",  str(_c.SIGNAL_PRIORITY["spend_velocity_7d"]))
        .replace("__PRIO_DISP__", str(_c.SIGNAL_PRIORITY["repeated_displacement"]))
    )


# ── Low-level helpers ──────────────────────────────────────────────────────────

def _connect(host: str, port: int, database: str, user: str, password: str):
    try:
        import clickhouse_connect
    except ImportError:
        raise ImportError("Run: pip install clickhouse-connect")

    tmp = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database="default")
    tmp.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database)

    for stmt in (_SQL_DIR / "setup_clickhouse.sql").read_text().split(";"):
        lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
        stmt  = "\n".join(lines).strip()
        if stmt:
            client.command(stmt)
    return client


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


def _to_ch_rows(rows: list[dict]) -> list[list]:
    """Convert standardized push_step() dicts to list[list] for CH INSERT."""
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        out.append([
            r["cc_num"],
            ts,
            r["amt"]           if r["amt"]           is not None else 0.0,
            r["category"],
            r["shipping_lat"]  if r["shipping_lat"]  is not None else 0.0,
            r["shipping_long"] if r["shipping_long"] is not None else 0.0,
        ])
    return out


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
        print("[CH] Truncating tables …")
        client.command("TRUNCATE TABLE IF EXISTS transactions")
        client.command("TRUNCATE TABLE IF EXISTS customers")

        # Create MV schema (idempotent) and clear MV backing tables.
        for stmt in (_SQL_DIR / "setup_clickhouse_mv.sql").read_text().split(";"):
            lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
            stmt  = "\n".join(lines).strip()
            if stmt:
                client.command(stmt)
        for tbl in ("gb30_counts", "gb45_counts", "sv7_counts", "disp_counts"):
            client.command(f"TRUNCATE TABLE IF EXISTS {tbl}")
        print("[CH] MV tables ready.")

        # Load customers.
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

        # Load preload transactions.
        if preload_path is not None:
            rows = []
            with open(preload_path, newline="") as f:
                for row in csv.DictReader(f):
                    rows.append([
                        int(row["cc_num"]),
                        datetime.strptime(row["ts"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),
                        float(row["amt"])           if row["amt"]           else 0.0,
                        row["category"],
                        float(row["shipping_lat"])  if row["shipping_lat"]  else 0.0,
                        float(row["shipping_long"]) if row["shipping_long"] else 0.0,
                    ])
            t0 = time.perf_counter()
            _insert(self._conn_kwargs(), "transactions",
                    ["cc_num", "ts", "amt", "category", "shipping_lat", "shipping_long"], rows)
            self._preload_ins_t = time.perf_counter() - t0
            print(f"[CH] {len(rows):,} preload rows in {self._preload_ins_t:.1f}s")

        # Create fraud_signals_full view with current thresholds and priorities.
        ddl = _substitute(_FULL_VIEW_DDL)
        client.command(ddl)
        client.command(_FULL_COUNT_DDL)
        print("[CH] fraud_signals_full + count views created.")

    def push_step(self, rows: list[dict]) -> None:
        t0 = time.perf_counter()
        _insert(self._conn_kwargs(), "transactions",
                ["cc_num", "ts", "amt", "category", "shipping_lat", "shipping_long"],
                _to_ch_rows(rows))
        self._insert_t = time.perf_counter() - t0

    def query(self, _win_start: datetime, _win_end: datetime) -> tuple[list[dict], float]:
        t0 = time.perf_counter()
        result = self._get_client().query(_FULL_QUERY)
        return _parse_result(result), time.perf_counter() - t0


class ClickHouseMVEngine(_ClickHouseBase):
    sim_id = 1
    name   = "CH-light"

    def setup(self, _preload_path: "Path | None", _data_dir: Path) -> None:
        client = self._get_client()   # ClickHouseFullEngine already set up schema
        ddl = _substitute(_LIGHT_VIEW_DDL)
        client.command(ddl)
        client.command(_LIGHT_COUNT_DDL)
        print("[CH] fraud_signals_light + count views created.")

    def push_step(self, _rows: list[dict]) -> None:
        pass   # MVs update automatically on ClickHouseFullEngine's INSERT

    def query(self, _win_start: datetime, _win_end: datetime) -> tuple[list[dict], float]:
        t0 = time.perf_counter()
        result = self._get_client().query(_LIGHT_QUERY)
        return _parse_result(result), time.perf_counter() - t0
