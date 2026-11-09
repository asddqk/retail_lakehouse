-- Витрины Gold в ClickHouse для быстрых аналитических запросов и BI.
-- Таблицы создаются при первом старте контейнера (docker-entrypoint-initdb.d).

CREATE DATABASE IF NOT EXISTS retail;

-- Факт продаж: партиции по месяцам, сортировка под типовые фильтры (дата, категория, продавец).
-- В ClickHouse нет обычных индексов: данные физически упорядочены по ORDER BY,
-- и по разреженному первичному индексу отбрасываются ненужные гранулы.
CREATE TABLE IF NOT EXISTS retail.fct_sales
(
    order_id               String,
    order_item_id          Int32,
    order_date             Date,
    order_purchase_ts      DateTime64(6),
    order_status           LowCardinality(String),
    is_canceled            Bool,
    customer_id            String,
    seller_id              String,
    product_id             String,
    product_category       LowCardinality(String),
    price                  Decimal(12, 2),
    freight_value          Decimal(12, 2),
    item_total             Decimal(13, 2),
    list_price_at_purchase Nullable(Decimal(12, 2)),
    discount_amount        Nullable(Decimal(13, 2))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, product_category, seller_id);

CREATE TABLE IF NOT EXISTS retail.mart_daily_sales
(
    order_date          Date,
    orders_cnt          UInt64,
    items_cnt           UInt64,
    customers_cnt       UInt64,
    revenue             Decimal(38, 2),
    freight             Decimal(38, 2),
    discounts           Decimal(38, 2),
    avg_check           Nullable(Float64),
    canceled_orders_cnt UInt64
)
ENGINE = MergeTree
ORDER BY order_date;

CREATE TABLE IF NOT EXISTS retail.mart_category_weekly
(
    week_start        Date,
    product_category  LowCardinality(String),
    orders_cnt        UInt64,
    revenue           Decimal(38, 2),
    prev_week_revenue Nullable(Decimal(38, 2)),
    wow_change_pct    Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (week_start, product_category);

CREATE TABLE IF NOT EXISTS retail.mart_seller_daily
(
    order_date   Date,
    seller_id    String,
    seller_state Nullable(String),
    orders_cnt   UInt64,
    items_cnt    UInt64,
    revenue      Decimal(38, 2)
)
ENGINE = MergeTree
ORDER BY (order_date, seller_id);
