-- Feldera fraud detection pipeline — table definitions.
-- Data is pushed via HTTP ingress; no connectors.
--
-- ┌─────────────────────────────────────────────────────────────────────────────┐
-- │  Feldera table     →  ClickHouse equivalent (ch_full_views.sql)             │
-- ├──────────────────────────────────────┬──────────────────────────────────────┤
-- │  TABLE CUSTOMER                      │  customers                            │
-- │  TABLE TRANSACTION                   │  transactions                         │
-- └─────────────────────────────────────────────────────────────────────────────┘

CREATE TABLE CUSTOMER (
    cc_num BIGINT NOT NULL PRIMARY KEY,
    name   VARCHAR,
    lat    DOUBLE,
    long   DOUBLE
) WITH ('materialized' = 'true');

CREATE TABLE TRANSACTION (
    category     VARCHAR,
    ts           TIMESTAMP,
    amt          DECIMAL(38, 2),
    cc_num       BIGINT NOT NULL,
    shipping_lat DOUBLE,
    shipping_long DOUBLE,
    FOREIGN KEY (cc_num) REFERENCES CUSTOMER(cc_num)
) WITH ('materialized' = 'true');
