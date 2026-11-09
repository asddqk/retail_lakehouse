-- Запросы для изучения стенда в DBeaver (подключение Trino: localhost:8080, пользователь любой).
-- Выполняй по одному: курсор на запросе + Cmd+Enter.

-- ===================================================================== слои
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

-- ===================================================================== витрины
SELECT * FROM iceberg.gold.mart_daily_sales ORDER BY order_date DESC LIMIT 14;

SELECT * FROM iceberg.gold.mart_category_weekly ORDER BY week_start DESC, revenue DESC;

SELECT * FROM iceberg.gold.mart_top_products_30d ORDER BY revenue_rank LIMIT 10;

-- ===================================================================== качество данных
-- сколько строк пришло в каждой загрузке, сколько дублей и отбраковки
SELECT * FROM iceberg.quality.dq_load_summary ORDER BY load_date;

-- что именно ушло в карантин
SELECT source_table, reason, count(*) AS rows_cnt
FROM iceberg.quality.dq_rejected_rows
GROUP BY 1, 2;

-- дедупликация: один заказ в Bronze встречается несколько раз (новый, обновления статуса, дубли)
SELECT order_id, count(*) AS versions, array_agg(order_status ORDER BY load_ts) AS statuses
FROM iceberg.bronze.orders
GROUP BY order_id
HAVING count(*) > 2
LIMIT 10;

-- ===================================================================== SCD2
-- история цен одного товара
SELECT * FROM iceberg.silver.silver_product_prices_scd2
WHERE product_id = (
    SELECT product_id FROM iceberg.silver.silver_product_prices_scd2
    GROUP BY product_id ORDER BY count(*) DESC LIMIT 1
)
ORDER BY valid_from;

-- скидка относительно прайса на момент покупки (point-in-time join в fct_sales)
SELECT product_category,
       round(avg(discount_amount), 2) AS avg_discount,
       round(100 * sum(discount_amount) / sum(list_price_at_purchase), 2) AS discount_pct
FROM iceberg.gold.fct_sales
GROUP BY 1
ORDER BY discount_pct DESC;

-- ===================================================================== устройство Iceberg
-- снапшоты: каждая загрузка и каждый запуск dbt — новая версия таблицы
SELECT committed_at, snapshot_id, operation,
       summary['added-records'] AS added, summary['total-records'] AS total
FROM iceberg.silver."silver_orders$snapshots"
ORDER BY committed_at;

-- файлы и партиции
SELECT partition, count(*) AS files, sum(record_count) AS records,
       round(sum(file_size_in_bytes) / 1024.0, 1) AS size_kb
FROM iceberg.silver."silver_orders$files"
GROUP BY partition
ORDER BY 1 DESC
LIMIT 10;

-- time travel: состояние таблицы на момент первого снапшота
-- (подставь snapshot_id из запроса выше)
-- SELECT count(*) FROM iceberg.silver.silver_orders FOR VERSION AS OF <snapshot_id>;

-- или на момент времени
-- SELECT count(*) FROM iceberg.silver.silver_orders FOR TIMESTAMP AS OF TIMESTAMP '2026-11-08 12:00:00 UTC';

-- план запроса: видно, сколько данных прочитано после отсечения партиций
EXPLAIN ANALYZE
SELECT count(*)
FROM iceberg.silver.silver_orders
WHERE order_purchase_ts >= (SELECT max(order_purchase_ts) FROM iceberg.silver.silver_orders) - INTERVAL '1' DAY;

-- ===================================================================== compaction вручную
-- ALTER TABLE iceberg.bronze.orders EXECUTE optimize;
-- SELECT count(*) AS files FROM iceberg.bronze."orders$files";
