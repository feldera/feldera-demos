# Debezium Postgres Source Demo

Replicates a Postgres source table into Feldera via the Debezium Postgres source connector and Kafka (RedPanda), using **both JSON and Avro formats in parallel** so you can compare them side by side.

- **Debezium Postgres Source** (`io.debezium.connector.postgresql.PostgresConnector`) — streams row-level changes from Postgres WAL via logical replication
- Two connector instances publish to two Kafka topic namespaces (`json.*` and `avro.*`), each consuming the same source table through its own replication slot
- Feldera ingests both topic streams into two mirrored tables (`json_test_table`, `avro_test_table`)

## Prerequisites

- Docker (with Compose v2)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Data flow

```
Postgres (test_schema.test_table)
  ├─► Debezium Postgres Source (JSON) ──► Kafka topic (json.test_schema.test_table) ──► Feldera: json_test_table
  └─► Debezium Postgres Source (Avro) ──► Kafka topic (avro.test_schema.test_table) ──► Feldera: avro_test_table
                                          (Avro schema stored in RedPanda Schema Registry)
```

## Steps

### 0. Shut down any previous instance

Before starting, make sure no previous instance of this demo is still running. Leftover containers (especially Postgres) can hold open connections, replication slots, or stale state that cause the demo to fail.

```bash
docker compose -f debezium-postgres/docker-compose.yml down -v
```

This stops all containers and removes their volumes. Safe to run even if nothing is currently up.

### 1. Start the services

From the repository root, run:

```bash
docker compose -f debezium-postgres/docker-compose.yml up -d --build --wait
```

This starts four containers:

| Service | Port | Purpose |
|---------|------|---------|
| feldera | 8080 | Feldera pipeline manager + runtime |
| redpanda | 19092 (Kafka), 18081 (Schema Registry) | Kafka-compatible message broker with Avro schema registry |
| kafka-connect | 8083 | Hosts both Debezium Postgres source connectors |
| postgres | 6432 | Source database (logical replication enabled) |

Wait until all services report healthy. You can check with:

```bash
docker compose -f debezium-postgres/docker-compose.yml ps
```

All four services should show `healthy`. This typically takes 30-60 seconds.
The feldera pipeline-manager image is around 2 GiB so it might take longer on slow networks.

### 2. Run the demo

```bash
uv run debezium-postgres/run.py --api-url http://localhost:8080 --start
```

What you will see:

1. **Schema + data population** — creates `test_schema.test_table` in Postgres (12 columns, mixed types) and inserts `NUM_RECORDS` (default 10,000) rows
2. **JSON connector** — deletes any previous instance, registers a new Debezium Postgres source connector with `topic.prefix=json`, waits for RUNNING state, waits for the `json.test_schema.test_table` topic to appear
3. **Avro connector** — same but with `topic.prefix=avro`, a second replication slot (`debezium_slot_1`), and the Confluent Avro converter pointed at the RedPanda schema registry
4. **Pipeline creation** — submits the Feldera pipeline (`demo-debezium-postgres-pipeline`) with two mirrored materialized tables (`json_test_table`, `avro_test_table`) that ingest each Kafka topic
5. **Pipeline start** — compiles the SQL program and starts the pipeline
6. **Validation** — queries Postgres source + both Feldera tables to verify end-to-end replication

Example output:

```
(Re-)creating test schema 'test_schema'
Populating 'test_schema.test_table' with 10000 records
1000 records
2000 records
...
Deleting old connector test-connector-json
Creating connector test-connector-json
Checking test-connector-json connector status
Waiting for test-connector-json to create Kafka topics
Topics ready: ['json.test_schema.test_table']
Deleting old connector test-connector-avro
Creating connector test-connector-avro
...
Topics ready: ['avro.test_schema.test_table']
Creating the pipeline...
Starting the pipeline...
Pipeline started
```

### 3. Inspect results

After the demo completes, the script automatically queries Postgres and both Feldera tables to display sample rows. You should see output like:
Make sure to this output appears and is visible to the user.

```
--- Postgres source: test_schema.test_table ---
Row count: 10000

-- First 5 rows --
(0, 0, 'foo0', 0.01, True, datetime.datetime(2024, 8, 30, 10, 30), UUID('123e4567-e89b-12d3-a456-426614174000'))
(1, 100, 'foo1', 1.01, True, datetime.datetime(2024, 8, 30, 10, 30), UUID('123e4567-e89b-12d3-a456-426614174000'))
...

--- Feldera tables (replicated from Postgres via CDC) ---

-- json_test_table --
Row count: 10000
First 5 rows:
{'id': 0, 'bi': 0, 's': 'foo0', 'd': 0.01, 'b': True}
{'id': 1, 'bi': 100, 's': 'foo1', 'd': 1.01, 'b': True}
...

-- avro_test_table --
Row count: 10000
First 5 rows:
{'id': 0, 'bi': 0, 's': 'foo0', 'd': 0.01, 'b': True}
{'id': 1, 'bi': 100, 's': 'foo1', 'd': 1.01, 'b': True}
...
```

Both Feldera tables should reach `count=10000`, confirming that data flowed end-to-end through both the JSON and Avro paths in parallel.

## 4. Summary

Next steps is for you to inspect everything in this demo:

### Visit the Feldera Web UI at [http://localhost:8080](http://localhost:8080) 

See the `demo-debezium-postgres-pipeline`, inspect both `json_test_table` and `avro_test_table`, and run ad-hoc SQL queries. In the Ad-Hoc Query tab try:

```sql
-- Verify both paths agree
SELECT count(*) FROM json_test_table;
SELECT count(*) FROM avro_test_table;

-- Sample rows from each format
SELECT * FROM json_test_table ORDER BY id LIMIT 10;
SELECT * FROM avro_test_table ORDER BY id LIMIT 10;

-- Cross-check the two formats produce identical data.
-- Joins the JSON and Avro tables on `id` and returns any rows where the
-- string column `s` differs between the two paths. An empty result set
-- means both connectors replicated every row identically — any rows here
-- would indicate one path fell behind or dropped/corrupted an event.
SELECT j.id, j.s AS json_s, a.s AS avro_s
FROM json_test_table j
JOIN avro_test_table a ON a.id = j.id
WHERE j.s <> a.s
LIMIT 5;
```

### Modify data in Postgres and watch CDC propagate through both paths. 

Connect with:

```bash
docker compose -f debezium-postgres/docker-compose.yml exec postgres \
    psql -U postgres -d postgres
```

Try some changes:

```sql
SELECT count(*) FROM test_schema.test_table;
INSERT INTO test_schema.test_table (id, bi, s, d, f, i, b, ts, dt, json1, json2, uuid_)
  VALUES (999999, 0, 'hello-cdc', 0.0, 0.0, 0, true,
          '2024-08-30 10:30:00', '2024-08-30',
          '{"foo":"bar"}', '{"foo":"bar"}',
          '123e4567-e89b-12d3-a456-426614174000');
UPDATE test_schema.test_table SET s = 'updated' WHERE id = 0;
DELETE FROM test_schema.test_table WHERE id = 1;
```

You can re-query the Feldera Web UI (Ad-Hoc Query tab) or watch the Performance tab — the changes appear on both `json_test_table` and `avro_test_table` instantly.

### Important: Clean-up when inspection is completed

```bash
docker compose -f debezium-postgres/docker-compose.yml down -v
```

This stops all containers and removes their volumes (including the Postgres replication slots).
