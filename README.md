# Feldera Demos

Demos and examples for the [Feldera](https://feldera.com) streaming SQL platform.

## Agentic Demos Setup

These demos run inside **Claude Code** — install it first from https://claude.ai/code.

Open this repo in Claude Code:

```bash
claude .
```

Claude handles all steps automatically: Feldera setup, pipeline loading, SQL generation, and live monitoring.

### Prerequisites

**Feldera instance** — set one in `.env` at the repo root:

| Option | How |
|--------|-----|
| Docker (no account needed) | Leave `.env` as-is — Claude pulls and starts the container |
| Remote instance (e.g. try.feldera.com) | Add `FELDERA_HOST=https://...` and `FELDERA_API_KEY=apikey:...` to `.env` |

**`fda` CLI (v0.282.0):**

```bash
cargo install fda --version 0.282.0
```

---

## Demos

| Demo | Folder | Claude Code command | Description |
|------|--------|---------------------|-------------|
| Agentic Fraud Detection | [agentic-fraud-detection/](agentic-fraud-detection/README.md) | `/run_fraud_demo` | Claude reads a real card-skimming attack report, maps signals to a live transaction pipeline, generates SQL detection views, and runs a live fraud investigator that classifies alerts in real time. |
| Agentic Fine-Grained Access | [agentic-fine-grained-access/](agentic-fine-grained-access/README.md) | `/run_fga_demo` | Claude generates detection views for rapid enumeration and hot-folder attacks in a shared file system governed by recursive group permissions. A live investigator classifies flagged users and blocks SUSPICIOUS ones by pushing `is_banned=true` — Feldera revokes their access within milliseconds. |
| Fraud Detection (Delta Lake) | [fraud-detection-delta-lake/](fraud-detection-delta-lake/) | `/run_fraud_delta_lake` | Batch fraud detection reading from a Delta Lake table. |
| Debezium + Postgres | [debezium-postgres/](debezium-postgres/) | `/run_debezium_postgres` | CDC pipeline ingesting Postgres changes via Debezium. |
| Debezium + MySQL | [debezium-mysql/](debezium-mysql/) | `/run_debezium_mysql` | CDC pipeline ingesting MySQL changes via Debezium. |
| Debezium + JDBC | [debezium-jdbc/](debezium-jdbc/) | `/run_debezium_jdbc` | CDC pipeline ingesting via Debezium JDBC connector. |
| Hopsworks Integration | [hopsworks/](hopsworks/) | `/run_hopsworks` | Feature pipeline integration with Hopsworks feature store. |
| TikTok Recommender System | [tik-tok-recommender-system/](tik-tok-recommender-system/) | `/run_tiktok` | TikTok-style recommendation system using Feldera + Hopsworks. |

## Structure

```
feldera-demos/
├── CLAUDE.md                        # Claude Code entry point
├── .env
├── utils/                           # Shared Python utilities
│   ├── pipeline_manager.py          # REST API wrapper for pipeline lifecycle
│   └── utils.py                     # URL fetcher with browser User-Agent
├── .claude/guides/                  # Claude Code guides (internal)
│   ├── setup/
│   │   ├── feldera-setup-docker.md  # Feldera setup: Docker or remote instance
│   │   ├── feldera-load-pipeline.md # Create or reset a pipeline to base SQL
│   │   └── feldera-redeploy.md      # Validate + stop + set program + start + poll
│   └── shared-analyze/
│       ├── feldera-analyze.md       # Core detection view engine
│       └── feldera-sql-generator.md # SQL generation rules and pitfalls
├── agentic-fraud-detection/         # Fraud detection demo
│   ├── programs/                    # Base pipeline SQL
│   ├── patterns/                    # Embedded attack pattern
│   └── fraud_investigator.py        # Live rule-based investigator
├── agentic-fine-grained-access/     # Fine-grained access control demo
│   ├── programs/                    # Base pipeline SQL
│   ├── patterns/                    # Access anomaly pattern descriptions
│   └── fga_investigator.py          # Live investigator with real-time blocking
├── fraud-detection-delta-lake/          # Batch fraud detection from Delta Lake
├── debezium-postgres/                   # CDC pipeline: Postgres via Debezium
├── debezium-mysql/                      # CDC pipeline: MySQL via Debezium
├── debezium-jdbc/                       # CDC pipeline: JDBC via Debezium
├── hopsworks/                           # Hopsworks feature store integration
└── tik-tok-recommender-system/          # TikTok-style recommender with Hopsworks
```
