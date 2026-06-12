"""
constants.py — shared constants for the rolling-aggregate benchmark.

Thresholds and priorities are the single source of truth: all three engines
generate their dialect-specific CREATE FUNCTION SQL from these values via
feldera_functions_sql() / clickhouse_functions_sql() / postgres_functions_sql() at setup time.
"""

# ── Fraud signal thresholds ────────────────────────────────────────────────────

GIFT_BURST_30D_THRESHOLD    = 2
GIFT_BURST_45D_THRESHOLD    = 2
SPEND_VELOCITY_7D_THRESHOLD = 2
DISPLACEMENT_THRESHOLD      = 2
DIST_MILES_THRESHOLD        = 20.0   # Manhattan-distance threshold for "far from home"

# ── Fraud signal lists ─────────────────────────────────────────────────────────

ALL_SIGNALS = [
    "gift_card_burst_30d",
    "gift_card_burst_45d",
    "spend_velocity_7d",
    "repeated_displacement",
]

# ── Demo streaming config ──────────────────────────────────────────────────────

N_STEPS       = 50
STEP_INTERVAL = 10.0      # seconds between batches
PRELOAD_ROWS  = 0         # rows of history loaded before the benchmark loop
DATA_DIR      = "data/0.01x" # default when running demo_runner.py directly; run_bench.py auto-names the dir

# ── ClickHouse connection defaults ─────────────────────────────────────────────

CLICKHOUSE_HOST           = "localhost"
CLICKHOUSE_PORT           = 8123
CLICKHOUSE_DATABASE       = "fraud_detection"
CLICKHOUSE_USERNAME       = "demo"
CLICKHOUSE_PASSWORD       = ""

# ── PostgreSQL connection defaults ────────────────────────────────────────────

POSTGRES_HOST     = "/var/run/postgresql"   # Unix socket; use "localhost" for TCP
POSTGRES_PORT     = 5432
POSTGRES_DATABASE = "fraud_detection"
POSTGRES_USERNAME = "nina"
POSTGRES_PASSWORD = ""

# ── Feldera connection defaults ────────────────────────────────────────────────

FELDERA_PIPELINE_NAME = "fraud-detection-replay"

# ── Signal priority (higher = shown first; must match SQL priority literals) ───

SIGNAL_PRIORITY = {
    "gift_card_burst_45d":   4,
    "gift_card_burst_30d":   3,
    "repeated_displacement": 5,
    "spend_velocity_7d":     1,
}

# ── Sim IDs: 0=CH-full, 1=Feldera, 2=PostgreSQL ───────────────────────────────

SIM_NAMES  = ["ClickHouse", "Feldera", "PostgreSQL"]

ENGINE_INDICES = {
    "ch":      0,
    "feldera": 1,
    "pg":      2,
}

DEFAULT_MODE = ["feldera", "ch"]

# ── ClickHouse window RANGE bounds (seconds) ──────────────────────────────────
# ClickHouse RANGE requires literal integers — UDFs cannot be used here.
# Used as template variables in clickhouse_views.sql via engine_clickhouse.py.

WINDOW_3D_SECS  = 3  * 24 * 3600   # 259200
WINDOW_7D_SECS  = 7  * 24 * 3600   # 604800
WINDOW_30D_SECS = 30 * 24 * 3600   # 2592000
WINDOW_45D_SECS = 45 * 24 * 3600   # 3888000

# ── Review priority formula ────────────────────────────────────────────────────

REVIEW_PRIORITY_SCALE = 1000   # total_priority * SCALE so priority dominates sort
REVIEW_AMT_CAP        = 9999   # cap on max_amt contribution

# ── Mock latency profiles ──────────────────────────────────────────────────────
# CH-full grows clearly (O(N) scan), Feldera flat.

MOCK_QUERY_BASE   = [0.30, 0.05, 4.00]   # base seconds: CH-full, Feldera, PostgreSQL
MOCK_QUERY_GROWTH = [0.06, 0.00, 0.10]   # seconds added per step


# ── SQL function generators ────────────────────────────────────────────────────

def feldera_functions_sql(gb30: int, gb45: int, sv7: int, disp: int,
                          dist_miles: float, prio: dict,
                          review_scale: int = REVIEW_PRIORITY_SCALE,
                          review_cap: int = REVIEW_AMT_CAP) -> str:
    """Generate Feldera CREATE FUNCTION statements for thresholds and priorities."""
    return (
        f"CREATE FUNCTION GB30()         RETURNS INTEGER NOT NULL AS {gb30};\n"
        f"CREATE FUNCTION GB45()         RETURNS INTEGER NOT NULL AS {gb45};\n"
        f"CREATE FUNCTION SV7()          RETURNS INTEGER NOT NULL AS {sv7};\n"
        f"CREATE FUNCTION DISP()         RETURNS INTEGER NOT NULL AS {disp};\n"
        f"CREATE FUNCTION DIST()         RETURNS DOUBLE  NOT NULL AS CAST({dist_miles} AS DOUBLE);\n"
        f"\n"
        f"CREATE FUNCTION PRIO_GB30()    RETURNS INTEGER NOT NULL AS {prio['gift_card_burst_30d']};\n"
        f"CREATE FUNCTION PRIO_GB45()    RETURNS INTEGER NOT NULL AS {prio['gift_card_burst_45d']};\n"
        f"CREATE FUNCTION PRIO_SV7()     RETURNS INTEGER NOT NULL AS {prio['spend_velocity_7d']};\n"
        f"CREATE FUNCTION PRIO_DISP()    RETURNS INTEGER NOT NULL AS {prio['repeated_displacement']};\n"
        f"\n"
        f"CREATE FUNCTION REVIEW_SCALE() RETURNS INTEGER NOT NULL AS {review_scale};\n"
        f"CREATE FUNCTION REVIEW_CAP()   RETURNS DOUBLE  NOT NULL AS CAST({review_cap} AS DOUBLE);\n"
    )


def postgres_functions_sql(gb30: int, gb45: int, sv7: int, disp: int,
                           dist_miles: float, prio: dict,
                           review_scale: int = REVIEW_PRIORITY_SCALE,
                           review_cap: int = REVIEW_AMT_CAP) -> str:
    """Generate PostgreSQL CREATE FUNCTION statements for thresholds and priorities."""
    def _fn(name, ret, val):
        return (f"CREATE OR REPLACE FUNCTION {name}() RETURNS {ret} "
                f"LANGUAGE sql IMMUTABLE AS $$ SELECT {val} $$;\n")
    return (
        _fn("GB30",         "INTEGER",          gb30)
        + _fn("GB45",       "INTEGER",          gb45)
        + _fn("SV7",        "INTEGER",          sv7)
        + _fn("DISP",       "INTEGER",          disp)
        + _fn("DIST",       "DOUBLE PRECISION", dist_miles)
        + _fn("PRIO_GB30",  "INTEGER",          prio["gift_card_burst_30d"])
        + _fn("PRIO_GB45",  "INTEGER",          prio["gift_card_burst_45d"])
        + _fn("PRIO_SV7",   "INTEGER",          prio["spend_velocity_7d"])
        + _fn("PRIO_DISP",  "INTEGER",          prio["repeated_displacement"])
        + _fn("REVIEW_SCALE", "INTEGER",        review_scale)
        + _fn("REVIEW_CAP",   "DOUBLE PRECISION", review_cap)
    )


def clickhouse_functions_sql(gb30: int, gb45: int, sv7: int, disp: int,
                             dist_miles: float, review_scale: int, review_cap: int,
                             prio: dict) -> str:
    """Generate ClickHouse CREATE FUNCTION statements for thresholds and priorities."""
    return (
        f"CREATE OR REPLACE FUNCTION GB30         AS () -> toUInt32({gb30});\n"
        f"CREATE OR REPLACE FUNCTION GB45         AS () -> toUInt32({gb45});\n"
        f"CREATE OR REPLACE FUNCTION SV7          AS () -> toUInt32({sv7});\n"
        f"CREATE OR REPLACE FUNCTION DISP         AS () -> toUInt32({disp});\n"
        f"CREATE OR REPLACE FUNCTION DIST         AS () -> toFloat64({dist_miles});\n"
        f"CREATE OR REPLACE FUNCTION REVIEW_SCALE AS () -> toUInt32({review_scale});\n"
        f"CREATE OR REPLACE FUNCTION REVIEW_CAP   AS () -> toFloat64({review_cap});\n"
        f"CREATE OR REPLACE FUNCTION PRIO_GB30    AS () -> toUInt32({prio['gift_card_burst_30d']});\n"
        f"CREATE OR REPLACE FUNCTION PRIO_GB45    AS () -> toUInt32({prio['gift_card_burst_45d']});\n"
        f"CREATE OR REPLACE FUNCTION PRIO_SV7     AS () -> toUInt32({prio['spend_velocity_7d']});\n"
        f"CREATE OR REPLACE FUNCTION PRIO_DISP    AS () -> toUInt32({prio['repeated_displacement']});\n"
    )
