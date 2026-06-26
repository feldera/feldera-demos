# Data Schema — the tables the detector runs against

> This is the human-readable narrative. The **concrete, always-correct DDL is generated per
> dataset, one file per engine** at `generated/schema.<engine>.sql` (e.g. `generated/schema.feldera.sql`,
> `schema.postgres_ivm.sql`, `schema.clickhouse_ivm.sql`) by `src/gen_demo_data.py`, straight from the
> columns it actually writes — `cat` it to check the contract visually. The agent writes each
> detector against its engine's generated file.

Two tables, keyed by the card number `cc_num`. The **logical** schema is identical across engines;
only the **physical table names** differ (the runner creates them). The detector reads only these
two tables and outputs the set of suspicious `cc_num`.

## `transactions` — the event stream (one row per purchase, streamed in batches)

| column | type | meaning |
|--------|------|---------|
| `cc_num` | BIGINT | the card |
| `ts` | TIMESTAMP | when the purchase happened |
| `category` | VARCHAR | merchant type: `gift card`, `grocery`, `travel`, `games` |
| `amt` | DECIMAL / DOUBLE | amount |
| `shipping_lat` | DOUBLE | latitude the purchase shipped to |
| `shipping_long` | DOUBLE | longitude the purchase shipped to |

## `customer` — the static dimension (one row per card holder, loaded once up front)

| column | type | meaning |
|--------|------|---------|
| `cc_num` | BIGINT (primary key) | the card |
| `name` | VARCHAR | cardholder name |
| `lat` | DOUBLE | cardholder **home** latitude |
| `long` | DOUBLE | cardholder **home** longitude |

**Distance / "far from home":** `|shipping_lat − lat| + |shipping_long − long| > 0.5`
(join each transaction to its `customer` row to compute it).

## Physical table names per engine

| engine | transactions table | customer table |
|--------|---------------------|----------------|
| `feldera` | `TRANSACTION` | `CUSTOMER` |
| `postgres_ivm` | `transactions` | `customer` |
| `clickhouse_ivm` | `tok_transactions` | `tok_customer` |

Column names are identical across all three. Write your detector against the physical table names
for the engine you are targeting.
