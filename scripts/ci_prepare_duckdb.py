"""Подготовка данных для CI: генерация → landing (S3) → bronze (Iceberg) → копия bronze в DuckDB.

В GitHub Actions нет MinIO и Trino, поэтому:
* S3 заменяет moto (локальный S3-совместимый сервер),
* каталог Iceberg — SQLite (pyiceberg SqlCatalog),
* модели dbt выполняются на DuckDB (target `ci`).

Так проверяется весь Python-код и SQL всех моделей и тестов dbt.

Запуск:  python scripts/ci_prepare_duckdb.py --start 2026-10-01 --days 10
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import duckdb

from retail import bronze
from retail.cli import run_day
from retail.storage import ensure_bucket, get_catalog


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 1))
    parser.add_argument("--days", type=int, default=10)
    parser.add_argument("--volume", type=int, default=300)
    parser.add_argument("--duckdb", default=os.environ.get("RETAIL_DUCKDB_PATH", "ci/retail.duckdb"))
    args = parser.parse_args()

    ensure_bucket("landing")
    ensure_bucket("warehouse")
    bronze.ensure_namespaces()
    for n in range(args.days):
        run_day(args.start + timedelta(days=n), volume=args.volume)

    Path(args.duckdb).parent.mkdir(parents=True, exist_ok=True)
    if Path(args.duckdb).exists():
        Path(args.duckdb).unlink()
    con = duckdb.connect(args.duckdb)
    con.execute("create schema if not exists bronze")
    catalog = get_catalog()
    for (_, name) in catalog.list_tables("bronze"):
        arrow = catalog.load_table(f"bronze.{name}").scan().to_arrow()
        con.register("tmp_arrow", arrow)
        con.execute(f"create or replace table bronze.{name} as select * from tmp_arrow")
        con.unregister("tmp_arrow")
        logging.info("duckdb bronze.%s: %d rows", name, arrow.num_rows)
    con.close()


if __name__ == "__main__":
    main()
