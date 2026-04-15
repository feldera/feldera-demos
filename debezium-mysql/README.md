# Debezium MySQL Source Demo

Replicates a MySQL `inventory` database into Feldera via the Debezium MySQL source connector and Kafka (RedPanda).

- **Debezium MySQL Source** (`io.debezium.connector.mysql.MySqlConnector`) emits row-level change events as JSON to Kafka topics
- Feldera consumes those topics as CDC input (`update_format: debezium`, `json_flavor: debezium_mysql`) and maintains mirrored tables

## Prerequisites

- Docker (with Compose v2)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Data flow

```
MySQL (inventory db)
  └─► Debezium MySQL Source ──► Kafka topics (inventory.inventory.*) ──► Feldera tables
                                                                        (customers, addresses,
                                                                         orders, products,
                                                                         products_on_hand)
```

## Steps

### 0. Shut down any previous instance

Before starting, make sure no previous instance of this demo is still running. Leftover containers (especially MySQL) can hold open connections or stale state that cause the demo to fail.

```bash
docker compose -f debezium-mysql/docker-compose.yml down -v
```

This stops all containers and removes their volumes. Safe to run even if nothing is currently up.

### 1. Start the services

From the repository root, run:

```bash
docker compose -f debezium-mysql/docker-compose.yml up -d --build --wait
```

This starts four containers:

| Service | Port | Purpose |
|---------|------|---------|
| feldera | 8080 | Feldera pipeline manager + runtime |
| redpanda | 19092 (Kafka), 18081 (Schema Registry) | Kafka-compatible message broker |
| kafka-connect | 8083 | Hosts the Debezium MySQL source connector |
| mysql | 3306 | Pre-populated `inventory` database (Debezium tutorial sample) |

Wait until all services report healthy. You can check with:

```bash
docker compose -f debezium-mysql/docker-compose.yml ps
```

All four services should show `healthy`. This typically takes 30-60 seconds.
The feldera pipeline-manager image is around 2 GiB so it might take longer on slow networks.

### 2. Run the demo

```bash
uv run debezium-mysql/run.py --api-url http://localhost:8080 --start
```

What you will see:

1. **Connector cleanup** — deletes any previous `inventory-connector` instance
2. **Connector creation** — registers the Debezium MySQL source connector with Kafka Connect and waits for it to reach RUNNING state
3. **Topic check** — waits until the connector has created all five expected Kafka topics (`inventory.inventory.customers`, `...addresses`, `...orders`, `...products`, `...products_on_hand`) from its initial snapshot
4. **Pipeline creation** — submits the Feldera pipeline (`debezium-mysql`) with tables that ingest each Kafka topic using `update_format: debezium`
5. **Pipeline start** — compiles the SQL program and starts the pipeline
6. **Validation** — queries both MySQL (source) and Feldera (replica) to show sample rows have been replicated via CDC

Example output:

```
Deleting old connector inventory-connector
Creating connector
Checking connector status
Waiting for the connector to create Kafka topics
All expected topics created.
Creating the pipeline...
Starting the pipeline...
Pipeline started
```

### 3. Inspect results

After the demo completes, the script automatically queries both MySQL and Feldera to display sample rows. You should see output like:
Make sure to this output appears and is visible to the user.

```
--- MySQL source: inventory database ---

-- customers (5 rows) --
(1001, 'Sally', 'Thomas', 'sally.thomas@acme.com')
(1002, 'George', 'Bailey', 'gbailey@foobar.com')
(1003, 'Edward', 'Walker', 'ed@walker.com')
(1004, 'Anne', 'Kretchmar', 'annek@noanswer.org')

-- products (5 rows) --
(101, 'scooter', 'Small 2-wheel scooter', 3.14)
(102, 'car battery', '12V car battery', 8.1)
...

--- Feldera tables (replicated from MySQL via CDC) ---

-- customers (5 rows) --
{'id': 1001, 'first_name': 'Sally', 'last_name': 'Thomas', 'email': 'sally.thomas@acme.com'}
{'id': 1002, 'first_name': 'George', 'last_name': 'Bailey', 'email': 'gbailey@foobar.com'}
...

-- products (5 rows) --
{'id': 101, 'name': 'scooter', 'description': 'Small 2-wheel scooter', 'weight': 3.14}
...
```

This confirms data flowed end-to-end: MySQL → Debezium → Kafka → Feldera.

## 4. Summary
Tell the user the following (do not edit text in this section): 


Next steps is for you to inspect everything in this demo:

### Visit the Feldera Web UI at [http://localhost:8080](http://localhost:8080)

See the `debezium-mysql` pipeline, inspect the tables, and run ad-hoc SQL queries against the replicated data. In the Ad-Hoc Query tab try:

```sql
-- All customers replicated from MySQL
SELECT * FROM customers ORDER BY id;

-- Join products with on-hand stock
SELECT p.id, p.name, p.description, poh.quantity
FROM products p
JOIN products_on_hand poh ON poh.product_id = p.id
ORDER BY p.id;

-- Orders with customer details
SELECT o.order_number, o.order_date, o.quantity, c.first_name, c.last_name, p.name AS product
FROM orders o
JOIN customers c ON c.id = o.purchaser
JOIN products p ON p.id = o.product_id
ORDER BY o.order_number;
```


### Modify data in MySQL and watch CDC propagate the changes.

Connect with:

```bash
docker compose -f debezium-mysql/docker-compose.yml exec mysql mysql -uroot -pdebezium inventory
```

Try to make some changes:

```sql
SELECT * FROM customers;
INSERT INTO customers VALUES (1005, 'Jane', 'Doe', 'jane@example.com');
UPDATE customers SET email = 'new@example.com' WHERE id = 1001;
DELETE FROM customers WHERE id = 1005;
```

You can re-query the Feldera Web UI (Ad-Hoc Query tab) or watch the Performance Tab, the changes should have been ingested already.

### Important: Clean-up when inspection is completed

```bash
docker compose -f debezium-mysql/docker-compose.yml down -v
```

This stops all containers and removes their volumes.
