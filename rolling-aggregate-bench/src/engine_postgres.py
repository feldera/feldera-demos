#!/usr/bin/env python3
"""
engine_postgres.py — PostgreSQL FraudEngine implementation.

PostgresFullEngine  (sim 2, "PostgreSQL")
  Full O(N) scan per query — same complexity class as ClickHouse.
  Uses COPY FROM STDIN for fast bulk loading and INTERVAL-based RANGE windows.

Timing model:
  push_step()  — times COPY FROM STDIN only           → reported as ins
  query()      — times SELECT COUNT(*) FROM fraud_signals_full → reported as ref+qry
  refresh_time — always 0 (no IVM; full scan at query time)
"""

import csv
import io
import time
from datetime import datetime
from pathlib import Path

import psycopg2

import constants as _c
from constants import postgres_functions_sql
from engine_base import FraudEngine

_SQL_DIR    = Path(__file__).parent.parent / "sql"
_TABLES_SQL = (_SQL_DIR / "postgres_tables.sql").read_text()
_VIEWS_SQL  = (_SQL_DIR / "postgres_views.sql").read_text()
_QUERY      = (_SQL_DIR / "postgres_query.sql").read_text().strip()

_CHUNK_SIZE = 250_000   # rows per COPY batch


# ── Low-level helpers ──────────────────────────────────────────────────────────

def _connect(host, port, database, user, password):
    """Connect, creating the database if it doesn't exist."""
    kw = dict(host=host, port=port, user=user, password=password)
    admin = psycopg2.connect(dbname="postgres", **kw)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{database}"')
    admin.close()
    conn = psycopg2.connect(dbname=database, **kw)
    with conn.cursor() as cur:
        cur.execute("SET work_mem = '8GB'")
    conn.commit()
    return conn


def _exec_sql(conn, sql: str) -> None:
    """Execute a multi-statement SQL block split on ';', skipping blanks."""
    with conn.cursor() as cur:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    conn.commit()


def _copy_buf(conn, table: str, columns: list[str], buf: io.StringIO) -> None:
    cols = ", ".join(columns)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN CSV", buf)
    conn.commit()


def _copy_rows(conn, table: str, columns: list[str], rows: list[list]) -> None:
    buf = io.StringIO()
    csv.writer(buf).writerows(rows)
    _copy_buf(conn, table, columns, buf)


def _stream_copy(conn, table: str, columns: list[str], csv_path: Path,
                 row_parser) -> int:
    """Stream-copy a CSV file in chunks without loading it all into memory."""
    total = 0
    buf = io.StringIO()
    writer = csv.writer(buf)

    with open(csv_path, newline="") as f:
        for raw in csv.DictReader(f):
            writer.writerow(row_parser(raw))
            total += 1
            if total % _CHUNK_SIZE == 0:
                _copy_buf(conn, table, columns, buf)
                buf = io.StringIO()
                writer = csv.writer(buf)

    if buf.tell():
        _copy_buf(conn, table, columns, buf)
    return total


def _parse_result(rows) -> list[dict]:
    if not rows:
        return []
    n = int(rows[0][0])
    return [{"cc_num": -(i + 1), "ts": 0, "amt": 0.0, "category": "",
             "shipping_lat": 0.0, "shipping_long": 0.0, "distance": 0.0,
             "avg_7day": 0.0, "signal_type": "count", "confidence": "high",
             "review_priority": 0.0} for i in range(n)]


# ── Engine ─────────────────────────────────────────────────────────────────────

class PostgresFullEngine(FraudEngine):
    sim_id     = 2
    name       = "PostgreSQL"
    storage_id = "postgres"

    def __init__(self, host: str = _c.POSTGRES_HOST, port: int = _c.POSTGRES_PORT,
                 database: str = _c.POSTGRES_DATABASE, user: str = _c.POSTGRES_USERNAME,
                 password: str = _c.POSTGRES_PASSWORD):
        self._host          = host
        self._port          = port
        self._database      = database
        self._user          = user
        self._password      = password
        self._conn          = None
        self._insert_t      = 0.0
        self._preload_ins_t = 0.0

    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = _connect(self._host, self._port, self._database,
                                  self._user, self._password)
        return self._conn

    def insert_time(self)      -> float: return self._insert_t
    def refresh_time(self)     -> float: return 0.0
    def preload_push_time(self) -> float: return self._preload_ins_t
    def preload_ivm_time(self)  -> float: return 0.0

    def setup(self, preload_path: "Path | None", data_dir: Path) -> None:
        conn = self._get_conn()

        _exec_sql(conn, _TABLES_SQL)
        _exec_sql(conn, postgres_functions_sql(
            gb30=_c.GIFT_BURST_30D_THRESHOLD,
            gb45=_c.GIFT_BURST_45D_THRESHOLD,
            sv7=_c.SPEND_VELOCITY_7D_THRESHOLD,
            disp=_c.DISPLACEMENT_THRESHOLD,
            dist_miles=_c.DIST_MILES_THRESHOLD,
            prio=_c.SIGNAL_PRIORITY,
        ))

        print("[PG] Tables ready.")

        cust_rows = []
        with open(Path(data_dir) / "customers.csv", newline="") as f:
            for row in csv.DictReader(f):
                cust_rows.append([
                    int(row["cc_num"]), row["name"],
                    float(row["lat"])  if row["lat"]  else 0.0,
                    float(row["long"]) if row["long"] else 0.0,
                ])
        _copy_rows(conn, "customers", ["cc_num", "name", "lat", "long"], cust_rows)
        print(f"[PG] {len(cust_rows):,} customers inserted.")

        if preload_path is not None:
            def _parse_txn(row):
                return [
                    int(row["cc_num"]), row["ts"],
                    float(row["amt"])           if row["amt"]           else 0.0,
                    row["category"],
                    float(row["shipping_lat"])  if row["shipping_lat"]  else 0.0,
                    float(row["shipping_long"]) if row["shipping_long"] else 0.0,
                ]
            t0 = time.perf_counter()
            n = _stream_copy(conn, "transactions",
                             ["cc_num", "ts", "amt", "category", "shipping_lat", "shipping_long"],
                             preload_path, _parse_txn)
            self._preload_ins_t = time.perf_counter() - t0
            print(f"[PG] {n:,} preload rows in {self._preload_ins_t:.1f}s")

        _exec_sql(conn, _VIEWS_SQL)
        print("[PG] fraud_signals_full view created.")

    def push_step(self, rows: list[dict]) -> None:
        buf = io.StringIO()
        writer = csv.writer(buf)
        for r in rows:
            writer.writerow([
                r["cc_num"], r["ts"],
                r["amt"]           if r["amt"]           is not None else 0.0,
                r["category"],
                r["shipping_lat"]  if r["shipping_lat"]  is not None else 0.0,
                r["shipping_long"] if r["shipping_long"] is not None else 0.0,
            ])
        t0 = time.perf_counter()
        _copy_buf(self._get_conn(), "transactions",
                  ["cc_num", "ts", "amt", "category", "shipping_lat", "shipping_long"], buf)
        self._insert_t = time.perf_counter() - t0

    def query(self, win_start: datetime, win_end: datetime) -> tuple[list[dict], float]:
        conn = self._get_conn()
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(_QUERY)
            rows = cur.fetchall()
        elapsed = time.perf_counter() - t0
        return _parse_result(rows), elapsed
