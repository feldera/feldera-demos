"""
constants.py — shared constants for the fraud detection benchmark demo.

Thresholds and priorities are the single source of truth: both engines
generate their dialect-specific CREATE FUNCTION SQL from these values via
feldera_functions_sql() / clickhouse_functions_sql() at setup time.
"""

# ── Fraud signal thresholds ────────────────────────────────────────────────────

GIFT_BURST_30D_THRESHOLD    = 20
GIFT_BURST_45D_THRESHOLD    = 20
SPEND_VELOCITY_7D_THRESHOLD = 20
DISPLACEMENT_THRESHOLD      = 10

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
DATA_DIR      = "data/0.1x" # default scale; data/1x = standard demo, data/10x = max gap

# ── ClickHouse connection defaults ─────────────────────────────────────────────

CLICKHOUSE_HOST           = "localhost"
CLICKHOUSE_PORT           = 8123
CLICKHOUSE_DATABASE       = "fraud_detection"
CLICKHOUSE_USERNAME       = "demo"
CLICKHOUSE_PASSWORD       = ""

# ── Feldera connection defaults ────────────────────────────────────────────────

FELDERA_PIPELINE_NAME = "fraud-detection-replay"

# ── Signal priority (higher = shown first; must match SQL priority literals) ───

SIGNAL_PRIORITY = {
    "gift_card_burst_45d":   4,
    "gift_card_burst_30d":   3,
    "repeated_displacement": 5,
    "spend_velocity_7d":     1,
}

# ── Sim IDs: 0=CH-full, 1=Feldera ─────────────────────────────────────────────

SIM_NAMES  = ["ClickHouse", "Feldera"]

DEMO_MODES = {
    "latency": [0, 1],   # speed story: CH-full (O(N)) vs Feldera (O(delta))
    "full":    [0, 1],   # alias for latency
}

# ── Mock latency profiles ──────────────────────────────────────────────────────
# CH-full grows clearly (O(N) scan), Feldera flat.

MOCK_QUERY_BASE   = [0.30, 0.05]   # base seconds: CH-full, Feldera
MOCK_QUERY_GROWTH = [0.06, 0.00]   # seconds added per step (CH-full: 0.3→3.3s over 50 steps)


# ── SQL function generators ────────────────────────────────────────────────────

def feldera_functions_sql(gb30: int, gb45: int, sv7: int, disp: int,
                          prio: dict) -> str:
    """Generate Feldera CREATE FUNCTION statements for thresholds and priorities."""
    return (
        f"CREATE FUNCTION GB30()      RETURNS INTEGER NOT NULL AS {gb30};\n"
        f"CREATE FUNCTION GB45()      RETURNS INTEGER NOT NULL AS {gb45};\n"
        f"CREATE FUNCTION SV7()       RETURNS INTEGER NOT NULL AS {sv7};\n"
        f"CREATE FUNCTION DISP()      RETURNS INTEGER NOT NULL AS {disp};\n"
        f"\n"
        f"CREATE FUNCTION PRIO_GB30() RETURNS INTEGER NOT NULL AS {prio['gift_card_burst_30d']};\n"
        f"CREATE FUNCTION PRIO_GB45() RETURNS INTEGER NOT NULL AS {prio['gift_card_burst_45d']};\n"
        f"CREATE FUNCTION PRIO_SV7()  RETURNS INTEGER NOT NULL AS {prio['spend_velocity_7d']};\n"
        f"CREATE FUNCTION PRIO_DISP() RETURNS INTEGER NOT NULL AS {prio['repeated_displacement']};\n"
    )


def clickhouse_functions_sql(gb30: int, gb45: int, sv7: int, disp: int,
                     prio: dict) -> str:
    """Generate ClickHouse CREATE FUNCTION statements for thresholds and priorities."""
    return (
        f"CREATE FUNCTION IF NOT EXISTS GB30      AS () -> toUInt32({gb30});\n"
        f"CREATE FUNCTION IF NOT EXISTS GB45      AS () -> toUInt32({gb45});\n"
        f"CREATE FUNCTION IF NOT EXISTS SV7       AS () -> toUInt32({sv7});\n"
        f"CREATE FUNCTION IF NOT EXISTS DISP      AS () -> toUInt32({disp});\n"
        f"CREATE FUNCTION IF NOT EXISTS PRIO_GB30 AS () -> toUInt32({prio['gift_card_burst_30d']});\n"
        f"CREATE FUNCTION IF NOT EXISTS PRIO_GB45 AS () -> toUInt32({prio['gift_card_burst_45d']});\n"
        f"CREATE FUNCTION IF NOT EXISTS PRIO_SV7  AS () -> toUInt32({prio['spend_velocity_7d']});\n"
        f"CREATE FUNCTION IF NOT EXISTS PRIO_DISP AS () -> toUInt32({prio['repeated_displacement']});\n"
    )
