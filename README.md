<!-- markdownlint-disable MD033 MD041 -->
<p align="center">
  <h3 align="center">Feldera Demos</h3>
</p>

<p align="center">
    <img src="https://readme-typing-svg.demolab.com?font=DM+Sans+Mono&pause=1000&color=C533B9&center=true&vCenter=true&width=435&lines=%24+claude+%2Frun_fraud_demo;%24+claude+%2Frun_fga_demo;%24+claude+%2Frun_fraud_delta_lake;%24+claude+%2Frun_debezium_mysql;%24+claude+run_hopsworks;%24+claude+%2Frun_tiktok;%24+claude+run_debezium_postgres;%24+claude+run_debezium_jdbc" alt="Run the demos" />
</p>

<p align="center">
  <a href="https://felderacommunity.slack.com/join/shared_invite/zt-222bq930h-dgsu5IEzAihHg8nQt~dHzA" alt="Slack" title="Feldera Pro Tips Discussion & Support">
    <img src="https://img.shields.io/badge/Slack-1k_Online-79A564?logo=slack&logoColor=white&style=for-the-badge"/></a>
  <a href="https://discord.gg/s6t5n9UzHE" alt="Discord" title="Feldera Pro Tips Discussion & Support">
    <img src="https://img.shields.io/discord/1223851723110748251?color=79A564&logo=discord&logoColor=white&style=for-the-badge"/></a>
</p>
<!-- markdownlint-enable MD033 -->

## ⚡ Quick setup

Demos and examples for the [Feldera](https://feldera.com) incremental compute engine.

All demos can be run inside **Claude Code** — install it first from https://claude.ai/code.

Open this repo in Claude Code:

```bash
claude .
```

Claude handles all steps automatically: Feldera setup, pipeline loading, SQL generation, and live monitoring.

> [!NOTE]
> If you want to run demos without the assistance of `claude`. Look into the
> `README.md` of the individual demo folders.

## 🚀 Available Demos

| Demo | Folder | Claude Code Command | Interfaces with | Description |
|------|--------|---------------------|--------------|-------------|
| Agentic Fraud Detection | [agentic-fraud-detection/](agentic-fraud-detection/README.md) | `/run_fraud_demo` | AI Agent (Claude) | Claude reads a real card-skimming attack report, maps signals to a live transaction pipeline, generates SQL detection views, and runs a live fraud investigator that classifies alerts in real time. |
| Agentic Fine-Grained Access | [agentic-fine-grained-access/](agentic-fine-grained-access/README.md) | `/run_fga_demo` | AI Agent (Claude) | Claude generates detection views for rapid enumeration and hot-folder attacks in a shared file system governed by recursive group permissions. A live investigator classifies flagged users and blocks SUSPICIOUS ones by pushing `is_banned=true` — Feldera revokes their access within milliseconds. |
| Fraud Detection (Delta Lake) | [fraud-detection-delta-lake/](fraud-detection-delta-lake/) | `/run_fraud_delta_lake` | Delta Lake, S3, XGBoost | Batch fraud detection reading from Delta Lake tables on S3, with XGBoost model training. |
| Debezium + Postgres | [debezium-postgres/](debezium-postgres/) | `/run_debezium_postgres` | Postgres, Debezium, Redpanda/Kafka | CDC pipeline ingesting Postgres changes via Debezium and Redpanda/Kafka. |
| Debezium + MySQL | [debezium-mysql/](debezium-mysql/) | `/run_debezium_mysql` | MySQL, Debezium, Redpanda/Kafka | CDC pipeline ingesting MySQL changes via Debezium and Redpanda/Kafka. |
| Debezium + JDBC | [debezium-jdbc/](debezium-jdbc/) | `/run_debezium_jdbc` | Postgres, Debezium, Redpanda/Kafka | CDC pipeline sinking Feldera views to Postgres via Redpanda/Kafka and JDBC sink connectors. |
| Hopsworks Integration | [hopsworks/](hopsworks/) | `/run_hopsworks` | Hopsworks, Kafka, XGBoost | Feature pipeline integration with Hopsworks feature store and Kafka, with XGBoost model training. |
| TikTok Recommender System | [tik-tok-recommender-system/](tik-tok-recommender-system/) | `/run_tiktok` | Redpanda/Kafka | TikTok-style recommendation system using Feldera and Redpanda/Kafka. |

## ⚙️ Pre-requisites

For a smooth experience, the following dependencies should be setup.

#### feldera

**Feldera instance**: You can use feldera using the [free feldera online sandbox](https://try.feldera.com) or a local setup using docker. claude will

| Option | How |
|--------|-----|
| Remote instance (e.g. try.feldera.com) | Add `FELDERA_HOST=https://...` and `FELDERA_API_KEY=apikey:...` to `.env` |
| Docker (no account needed) | Leave `.env` as-is — Claude pulls and starts the container, docker or podman is required to be installed |

> Note that demos which rely on docker compose scripts to setup 3rd party services (postgres, mysql, kafka/redpanda) will only work with the local docker form factor.

#### fda CLI

Some demos use the fda CLI to interact with feldera.
You can find [instructions to install it in our docs](https://docs.feldera.com/interface/cli#quick-install).

```
curl -fsSL https://feldera.com/install-fda | bash
```

#### uv

Some demos use pythons scripts and the feldera python SDK to interact with feldera. For this a recent installation of [uv](https://docs.astral.sh/uv/getting-started/installation/) is necessary.
