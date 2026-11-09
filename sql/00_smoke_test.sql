-- Этап 1: проверка, что MinIO + Iceberg + Trino работают вместе.
-- Выполняй запросы по одному сверху вниз (в DBeaver: курсор на запросе + Cmd+Enter).

-- 1. Создать схему (в Iceberg она называется namespace) и указать, где в S3 лежат её таблицы
CREATE SCHEMA IF NOT EXISTS iceberg.test
WITH (location = 's3://warehouse/test');

-- 2. Создать таблицу Iceberg в формате Parquet с партиционированием по дню
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

-- 3. Первая вставка (создаст снапшот №1)
INSERT INTO iceberg.test.orders VALUES
    ('o1', 'c1', TIMESTAMP '2026-10-20 10:15:00', 1500.00),
    ('o2', 'c2', TIMESTAMP '2026-10-20 12:40:00',  320.50),
    ('o3', 'c1', TIMESTAMP '2026-10-21 09:05:00',  780.00);

-- 4. Проверить данные
SELECT * FROM iceberg.test.orders ORDER BY order_ts;

-- 5. Вторая вставка (создаст снапшот №2)
INSERT INTO iceberg.test.orders VALUES
    ('o4', 'c3', TIMESTAMP '2026-10-21 18:30:00', 2100.00);

-- 6. Служебные таблицы Iceberg: история снапшотов
SELECT snapshot_id, committed_at, operation
FROM iceberg.test."orders$snapshots"
ORDER BY committed_at;

-- 7. Какие Parquet-файлы лежат в таблице и в каких партициях
SELECT file_path, record_count, partition
FROM iceberg.test."orders$files";

-- 8. Статистика по партициям (по дням)
SELECT * FROM iceberg.test."orders$partitions";

-- 9. Time travel: подставь snapshot_id ПЕРВОГО снапшота из запроса 6.
--    Должно вернуться 3 строки — состояние до второй вставки.
-- SELECT * FROM iceberg.test.orders FOR VERSION AS OF <snapshot_id>;

-- 10. Планы выполнения: видно, что Trino читает только нужную партицию
EXPLAIN ANALYZE
SELECT sum(amount)
FROM iceberg.test.orders
WHERE order_ts >= TIMESTAMP '2026-10-21 00:00:00';
