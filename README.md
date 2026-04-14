# Feldera Demos

Demos and examples for the [Feldera](https://feldera.com) streaming SQL platform.

## Agentic Demos Setup

These demos run inside **Claude Code** — install it first from https://claude.ai/code.

Open this repo in Claude Code:

```bash
claude .
```

Then run a demo using a slash command or natural language:

| Demo | Slash command | Natural language |
|------|--------------|-----------------|
| Fraud Detection | `/run_fraud_demo` | `run fraud detection` |
| Fine-Grained Access | `/run_fga_demo` | `run fine-grained authorization demo` |

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

| Demo | Folder | Command / How to run | Description |
|------|--------|----------------------|-------------|
| Agentic Fraud Detection | [agentic-fraud-detection/](agentic-fraud-detection/README.md) | `/run_fraud_demo` (Claude Code command) | Claude reads a real card-skimming attack report, maps signals to a live transaction pipeline, generates SQL detection views, and runs a live fraud investigator that classifies alerts in real time. |
| Agentic Fine-Grained Access | [agentic-fine-grained-access/](agentic-fine-grained-access/README.md) | `/run_fga_demo` (Claude Code command) | Claude generates detection views for rapid enumeration and hot-folder attacks in a shared file system governed by recursive group permissions. A live investigator classifies flagged users and blocks SUSPICIOUS ones by pushing `is_banned=true` — Feldera revokes their access within milliseconds. |
| Fraud Detection (Delta Lake) | [fraud-detection-delta-lake/](fraud-detection-delta-lake/) | `python run.py` | Batch fraud detection reading from a Delta Lake table. |
| Simple Count | [simple-count/](simple-count/) | `python run.py` | Minimal pipeline example: count events from a Kafka topic. |
| Debezium + Postgres | [debezium-postgres/](debezium-postgres/) | `python run.py` | CDC pipeline ingesting Postgres changes via Debezium. |
| Debezium + MySQL | [debezium-mysql/](debezium-mysql/) | `python run.py` | CDC pipeline ingesting MySQL changes via Debezium. |
| Debezium + JDBC | [debezium-jdbc/](debezium-jdbc/) | `python run.py` | CDC pipeline ingesting via Debezium JDBC connector. |
| Hopsworks Integration | [hopsworks/](hopsworks/) | Jupyter notebooks | Feature pipeline integration with Hopsworks feature store. |
| Hopsworks TikTok Rec Sys | [hopsworks-tik-tok-rec-sys/](hopsworks-tik-tok-rec-sys/) | `python 1_pipeline.py` | TikTok-style recommendation system using Feldera + Hopsworks. |
| Supply Chain Tutorial | [supply-chain-tutorial/](supply-chain-tutorial/) | `python run.py` | Supply chain analytics tutorial pipeline. |

## Structure

```
feldera-demos/
├── CLAUDE.md                        # Claude Code entry point
├── .env
├── utils/                           # Shared Python utilities
│   ├── pipeline_manager.py          # REST API wrapper for pipeline lifecycle
│   └── utils.py                     # URL fetcher with browser User-Agent
├── agentic-guides/                  # Claude Code guides
│   ├── setup/
│   │   ├── feldera-setup-docker.md  # Feldera setup: Docker or remote instance
│   │   ├── feldera-load-pipeline.md # Create or reset a pipeline to base SQL
│   │   └── feldera-redeploy.md      # Validate + stop + set program + start + poll
│   ├── shared-analyze/
│   │   ├── feldera-analyze.md       # Core detection view engine
│   │   └── feldera-sql-generator.md # SQL generation rules and pitfalls
│   ├── agentic-fraud-detection/
│   │   └── feldera-analyze-fraud.md # Fraud demo entry point
│   └── agentic-fine-grained-access/
│       └── feldera-analyze-fga.md   # FGA demo entry point
├── agentic-fraud-detection/         # Fraud detection demo
│   ├── programs/                    # Base pipeline SQL
│   ├── patterns/                    # Embedded attack pattern
│   └── fraud_investigator.py        # Live rule-based investigator
├── agentic-fine-grained-access/     # Fine-grained access control demo
│   ├── programs/                    # Base pipeline SQL
│   ├── patterns/                    # Access anomaly pattern descriptions
│   └── fga_investigator.py          # Live investigator with real-time blocking
├── fraud-detection-delta-lake/
├── simple-count/
├── debezium-postgres/
├── debezium-mysql/
├── debezium-jdbc/
├── hopsworks/
├── hopsworks-tik-tok-rec-sys/
└── supply-chain-tutorial/
```
