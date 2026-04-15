# Debezium JDBC Sink Demo

Streams the output of a Feldera materialized view to PostgreSQL via two Kafka JDBC sink connectors:

- **Debezium JDBC Sink** (`io.debezium.connector.jdbc.JdbcSinkConnector`) — JSON format
- **Confluent JDBC Sink** (`io.confluent.connect.jdbc.JdbcSinkConnector`) — Avro format

The demo creates a Feldera pipeline with a test table, inserts 10,000 records (100 batches of 100), and validates that the deduplicated result (199 unique rows) arrives in both PostgreSQL tables.

## Prerequisites

- Docker (with Compose v2)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Data flow

```
Feldera pipeline
  └─► Kafka topic (json_jdbc_test)  ──► Debezium JDBC Sink  ──► Postgres table
  └─► Kafka topic (avro_jdbc_test)  ──► Confluent JDBC Sink  ──► Postgres table
```

## Steps

### 0. Shut down any previous instance

Before starting, make sure no previous instance of this demo is still running. Leftover containers (especially Postgres) can hold open connections that cause the demo to fail with errors like `database "jdbc_test_db" is being accessed by other users`.

```bash
docker compose -f debezium-jdbc/docker-compose.yml down -v
```

This stops all containers and removes their volumes. Safe to run even if nothing is currently up.

### 1. Start the services

From the repository root, run:

```bash
docker compose -f debezium-jdbc/docker-compose.yml up -d --build --wait
```

This starts four containers:

| Service | Port | Purpose |
|---------|------|---------|
| feldera | 8080 | Feldera pipeline manager + runtime |
| redpanda | 19092 (Kafka), 18081 (Schema Registry) | Kafka-compatible message broker |
| kafka-connect | 8083 | Hosts the JDBC sink connectors |
| postgres | 6432 | Target database |

Wait until all services report healthy. You can check with:

```bash
docker compose -f debezium-jdbc/docker-compose.yml ps
```

All four services should show `healthy` (or `running` for postgres). This typically takes 30-60 seconds.
The feldera pipeline-manager image is around 2 GiB so it might take longer on slow networks.

### 2. Run the demo

```bash
uv run debezium-jdbc/run.py --api-url http://localhost:8080 --start
```

What you will see:

1. **Connector cleanup** — deletes any previous connector instances
2. **Database creation** — drops and recreates `jdbc_test_db` in Postgres
3. **Topic + connector setup** — creates Kafka topics (`json_jdbc_test`, `avro_jdbc_test`) and registers both JDBC sink connectors with Kafka Connect
4. **Pipeline creation** — submits a Feldera pipeline (`demo-debezium-jdbc-pipeline`) with a `test_table` and a `test_view` that outputs to both Kafka topics
5. **Data generation** — inserts 10,000 records (100 batches of 100, with overlapping IDs producing 199 unique rows)
6. **Validation** — polls both Postgres tables until each contains exactly 199 rows, confirming end-to-end delivery through both JSON and Avro paths

Example output:

```
Deleting old connector jdbc-test-connector-json
Deleting old connector jdbc-test-connector-avro
(Re-)creating test database jdbc_test_db
Database created
(Re-)creating topic
Create connector
Checking connector status
...
Creating the pipeline...
Starting the pipeline...
Pipeline started
Generating records...
Batch 0
Batch 1
...
Waiting for Postgres table json_jdbc_test to be created
Done!
Waiting for 199 rows in table json_jdbc_test
Found 199 rows
Done!
Waiting for Postgres table avro_jdbc_test to be created
Done!
Waiting for 199 rows in table avro_jdbc_test
Found 199 rows
Done!
```

### 3. Inspect results

After the demo completes, the script automatically queries both Feldera and Postgres to display sample rows. You should see output like:
Make sure to this output appears and is visible to the user.

```
--- Feldera ad-hoc query (5 rows from test_view) ---
{'id': 0, 'f1': True, 'f2': 'foo', ...}
{'id': 1, 'f1': True, 'f2': 'foo', ...}
...

--- Postgres: json_jdbc_test (5 rows) ---
(0, True, 'foo', ...)
(1, True, 'foo', ...)
...

--- Postgres: avro_jdbc_test (5 rows) ---
(0, True, 'foo', ...)
(1, True, 'foo', ...)
...
```

This confirms data flowed end-to-end: Feldera materialized view → Kafka → JDBC Sink → Postgres.

## 4. Summary

Next steps is for the user to inspect everything in this demo:

### Visit the Feldera Web UI at [http://localhost:8080](http://localhost:8080)

See the pipeline, inspect the tables and views, and browse the data interactively.

### Connect to Postgres to explore the data

```bash
docker compose -f debezium-jdbc/docker-compose.yml exec psql -h localhost -p 6432 -U postgres -d jdbc_test_db
```

Password: `postgres`. Once connected, try to run these commands:

```sql
SELECT * FROM json_jdbc_test LIMIT 5;
SELECT * FROM avro_jdbc_test LIMIT 5;
SELECT count(*) FROM json_jdbc_test;
```

### Important: Clean-up when inspection is completed

```bash
docker compose -f debezium-jdbc/docker-compose.yml down -v
```

This stops all containers and removes their volumes.
