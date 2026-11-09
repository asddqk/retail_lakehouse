"""Экспорт витрин Gold из Iceberg (через Trino) в ClickHouse.

Набор колонок берётся из таблицы ClickHouse, поэтому порядок и состав колонок
задаются один раз — в clickhouse/init.sql. Загрузка идемпотентна:
таблица очищается и заполняется заново (витрины небольшие).
"""
from __future__ import annotations

import logging
from decimal import Decimal

import clickhouse_connect
import trino

from retail.config import Settings, get_settings

log = logging.getLogger(__name__)

GOLD_TABLES = ["fct_sales", "mart_daily_sales", "mart_category_weekly", "mart_seller_daily"]
BATCH_SIZE = 50_000


def trino_connection(settings: Settings | None = None) -> trino.dbapi.Connection:
    s = settings or get_settings()
    return trino.dbapi.connect(host=s.trino_host, port=s.trino_port, user=s.trino_user, catalog=s.trino_catalog)


def clickhouse_client(settings: Settings | None = None):
    s = settings or get_settings()
    return clickhouse_connect.get_client(
        host=s.ch_host, port=s.ch_port, username=s.ch_user, password=s.ch_password, database=s.ch_database
    )


def _normalize(value):
    # Float-колонки ClickHouse (avg_check, wow_change_pct) приходят из Trino как Decimal
    return float(value) if isinstance(value, Decimal) else value


def export_table(table: str, ch=None, conn=None, settings: Settings | None = None) -> int:
    s = settings or get_settings()
    ch = ch or clickhouse_client(s)
    conn = conn or trino_connection(s)

    described = ch.query(f"DESCRIBE TABLE {s.ch_database}.{table}").result_rows
    columns = [row[0] for row in described]
    float_cols = {row[0] for row in described if "Float" in row[1]}

    cursor = conn.cursor()
    cursor.execute(f"SELECT {', '.join(columns)} FROM {s.trino_catalog}.gold.{table}")

    ch.command(f"TRUNCATE TABLE {s.ch_database}.{table}")
    total = 0
    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        if float_cols:
            idx = [i for i, c in enumerate(columns) if c in float_cols]
            rows = [[_normalize(v) if i in idx else v for i, v in enumerate(r)] for r in rows]
        ch.insert(table, rows, column_names=columns, database=s.ch_database)
        total += len(rows)
    log.info("clickhouse %s.%s: %d rows", s.ch_database, table, total)
    return total


def export_gold(settings: Settings | None = None) -> dict[str, int]:
    s = settings or get_settings()
    ch = clickhouse_client(s)
    conn = trino_connection(s)
    return {table: export_table(table, ch, conn, s) for table in GOLD_TABLES}
