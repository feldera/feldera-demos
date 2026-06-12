-- PostgreSQL fraud detection schema — base tables.
-- Threshold/priority functions are generated from constants.py at setup time
-- (see engine_postgres.py: postgres_functions_sql()) — no hardcoded values here.
-- Executed by PostgresFullEngine.setup() (idempotent — DROP CASCADE + CREATE).

DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    cc_num  BIGINT NOT NULL PRIMARY KEY,
    name    TEXT,
    lat     DOUBLE PRECISION,
    long    DOUBLE PRECISION
);

CREATE TABLE transactions (
    cc_num        BIGINT NOT NULL REFERENCES customers(cc_num),
    ts            TIMESTAMP NOT NULL,
    amt           NUMERIC(38,2),
    category      TEXT,
    shipping_lat  DOUBLE PRECISION,
    shipping_long DOUBLE PRECISION
);

CREATE INDEX transactions_cc_num_ts ON transactions (cc_num, ts);
