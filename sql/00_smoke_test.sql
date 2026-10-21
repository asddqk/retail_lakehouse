-- Этап 1: проверка, что MinIO + Iceberg + Trino работают вместе.

CREATE SCHEMA IF NOT EXISTS iceberg.test
WITH (location = 's3://warehouse/test');

CREATE TABLE IF NOT EXISTS iceberg.test.orders (
    order_id     varchar,
    customer_id  varchar,
    order_ts     timestamp(6),
    amount       decimal(10, 2)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(order_ts)']
);

INSERT INTO iceberg.test.orders VALUES
    ('o1', 'c1', TIMESTAMP '2026-10-20 10:15:00', 1500.00),
    ('o2', 'c2', TIMESTAMP '2026-10-20 12:40:00',  320.50),
    ('o3', 'c1', TIMESTAMP '2026-10-21 09:05:00',  780.00);

SELECT * FROM iceberg.test.orders ORDER BY order_ts;

INSERT INTO iceberg.test.orders VALUES
    ('o4', 'c3', TIMESTAMP '2026-10-21 18:30:00', 2100.00);

SELECT snapshot_id, committed_at, operation
FROM iceberg.test."orders$snapshots"
ORDER BY committed_at;

SELECT file_path, record_count, partition
FROM iceberg.test."orders$files";

SELECT * FROM iceberg.test."orders$partitions";

EXPLAIN ANALYZE
SELECT sum(amount)
FROM iceberg.test.orders
WHERE order_ts >= TIMESTAMP '2026-10-21 00:00:00';
