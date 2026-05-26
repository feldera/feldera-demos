"""
constants.py — shared constants for the fraud detection benchmark demo.

Fraud thresholds must match feldera_views.sql exactly so results are
comparable across engines.
"""

# ── Fraud signal thresholds (per dataset scale) ────────────────────────────────
#
# Thresholds scale with data volume: 10x data → 10x transactions per card
# per epoch bucket, so thresholds must scale proportionally to maintain
# comparable detection rates across dataset sizes.
#
# CH-light uses the same thresholds as CH-full/Feldera. Because CH-light's
# approximations over-count (epoch-aligned windows, no distance check), it
# naturally flags more cards at the same threshold — making the accuracy
# comparison meaningful without artificially handicapping it.
#
# Structure: {data_dir_suffix: (gb30, gb45, sv7, disp)}
THRESHOLD_PROFILES = {
    "0.1x": ( 20,  20,  20,  10),
    "1x":   ( 20,  20,  20,  10),
    "5x":   ( 20,  20,  20,  10),
    "10x":  ( 20,  20,  20,  10),
    "20x":  ( 20,  20,  20,  10),
}

# Active thresholds — overridden by main() from THRESHOLD_PROFILES based on --data-dir.
# Defaults match the "1x" profile so standalone imports are consistent.
(GIFT_BURST_30D_THRESHOLD,
 GIFT_BURST_45D_THRESHOLD,
 SPEND_VELOCITY_7D_THRESHOLD,
 DISPLACEMENT_THRESHOLD) = THRESHOLD_PROFILES["1x"]

# ── Fraud signal lists ─────────────────────────────────────────────────────────

ALL_SIGNALS = [
    "gift_card_burst_30d",
    "gift_card_burst_45d",
    "spend_velocity_7d",
    "repeated_displacement",
]

CH_LIGHT_SIGNALS = [
    "gift_card_burst_30d",
    "gift_card_burst_45d",
    "spend_velocity_7d",
    # repeated_displacement absent — requires customer JOIN, unsupported in CH MVs
]

# ── Demo streaming config ──────────────────────────────────────────────────────

N_STEPS       = 50
STEP_INTERVAL = 10.0      # seconds between batches
PRELOAD_ROWS  = 0         # rows of history loaded before the benchmark loop
DATA_DIR      = "data/0.1x" # default scale; data/1x = standard demo, data/10x = max gap

# ── ClickHouse connection defaults ─────────────────────────────────────────────

CH_HOST           = "localhost"
CH_PORT           = 8123
CH_DATABASE       = "fraud_detection"
CH_DATABASE_LIGHT = "fraud_detection_light"
CH_USERNAME       = "demo"
CH_PASSWORD       = ""

# ── Feldera connection defaults ────────────────────────────────────────────────

FELDERA_PIPELINE_NAME = "fraud-detection-replay"

# ── Signal priority (higher = shown first; must match SQL priority literals) ───

SIGNAL_PRIORITY = {
    "gift_card_burst_45d":   4,
    "gift_card_burst_30d":   3,
    "repeated_displacement": 5,
    "spend_velocity_7d":     1,
}

# ── Sim IDs: 0=CH-full, 1=CH-light, 2=Feldera ─────────────────────────────────

SIM_NAMES  = ["CH-full", "CH-light", "Feldera"]

DEMO_MODES = {
    "latency":  [0, 2],      # speed story: CH-full (O(N)) vs Feldera (O(delta))
    "accuracy": [1, 2],      # completeness: CH-light (approx displacement) vs Feldera (exact displacement)
    "full":     [0, 1, 2],   # all three side-by-side
}

# ── Mock latency profiles ──────────────────────────────────────────────────────
# CH-full grows clearly (O(N) scan), CH-light slight growth, Feldera flat.

MOCK_QUERY_BASE   = [0.30, 0.05, 0.05]   # base seconds: CH-full, CH-light, Feldera
MOCK_QUERY_GROWTH = [0.06, 0.01, 0.00]   # seconds added per step (CH-full: 0.3→3.3s over 50 steps)
