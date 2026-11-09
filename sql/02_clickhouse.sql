-- Запросы к ClickHouse (DBeaver: новое соединение ClickHouse, localhost:8123, admin / password)
-- или в браузере: http://localhost:8123/play

SELECT count() FROM retail.fct_sales;

-- выручка по категориям
SELECT product_category, uniqExact(order_id) AS orders, sum(price) AS revenue
FROM retail.fct_sales
WHERE NOT is_canceled
GROUP BY product_category
ORDER BY revenue DESC;

-- как данные лежат на диске: партиции (месяцы) и куски (parts)
SELECT partition, count() AS parts, sum(rows) AS rows,
       formatReadableSize(sum(data_compressed_bytes)) AS compressed,
       formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed
FROM system.parts
WHERE database = 'retail' AND table = 'fct_sales' AND active
GROUP BY partition
ORDER BY partition;

-- сжатие по колонкам
SELECT name, type,
       formatReadableSize(data_compressed_bytes) AS compressed,
       round(data_uncompressed_bytes / nullIf(data_compressed_bytes, 0), 1) AS ratio
FROM system.columns
WHERE database = 'retail' AND table = 'fct_sales'
ORDER BY data_compressed_bytes DESC;

-- сколько гранул прочитано благодаря ORDER BY (первичному ключу)
EXPLAIN indexes = 1
SELECT sum(price) FROM retail.fct_sales
WHERE order_date = (SELECT max(order_date) FROM retail.fct_sales);

-- ClickHouse умеет читать Parquet прямо из S3 (MinIO) без загрузки
SELECT count()
FROM s3('http://minio:9000/landing/orders/*/*.parquet', 'admin', 'password', 'Parquet');
