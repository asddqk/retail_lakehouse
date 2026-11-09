"""Landing: CSV из учётной системы → Parquet в S3 (MinIO).

Файлы раскладываются по датам загрузки:
    s3://landing/<table>/load_date=YYYY-MM-DD/<table>.parquet

Все колонки сохраняются как строки: на этом шаге данные не интерпретируются,
приведение типов делается в слое Silver. Повторный запуск за ту же дату
перезаписывает тот же файл, поэтому шаг идемпотентен.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from retail.config import Settings, get_settings
from retail.storage import ensure_bucket, s3_filesystem

log = logging.getLogger(__name__)

# Реальные файлы датасета Olist можно подложить вместо сгенерированных
OLIST_FILE_NAMES = {
    "olist_customers_dataset": "customers",
    "olist_sellers_dataset": "sellers",
    "olist_products_dataset": "products",
    "olist_orders_dataset": "orders",
    "olist_order_items_dataset": "order_items",
    "olist_order_payments_dataset": "order_payments",
}


def table_name_for(path: Path) -> str:
    return OLIST_FILE_NAMES.get(path.stem, path.stem)


def landing_key(table: str, load_date: date, settings: Settings | None = None) -> str:
    s = settings or get_settings()
    return f"{s.landing_bucket}/{table}/load_date={load_date.isoformat()}/{table}.parquet"


def land_file(csv_path: Path, load_date: date, settings: Settings | None = None) -> dict:
    """Один CSV → один Parquet в S3. Возвращает статистику для логов."""
    s = settings or get_settings()
    table = table_name_for(csv_path)
    frame = pl.read_csv(csv_path, infer_schema=False)  # всё читаем как текст
    frame = frame.with_columns(pl.all().replace("", None))  # пустые строки → NULL
    key = landing_key(table, load_date, s)
    filesystem = s3_filesystem(s)
    with filesystem.open_output_stream(key) as out:
        pq.write_table(frame.to_arrow(), out, compression="zstd")
    log.info("landed %s: %d rows -> s3://%s", table, frame.height, key)
    return {"table": table, "rows": frame.height, "key": key}


def land_directory(source_dir: str | Path, load_date: date, settings: Settings | None = None) -> list[dict]:
    """Все CSV из каталога → landing."""
    s = settings or get_settings()
    ensure_bucket(s.landing_bucket, s)
    files = sorted(Path(source_dir).glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"в {source_dir} нет CSV-файлов")
    return [land_file(path, load_date, s) for path in files]


def landed_tables(load_date: date, settings: Settings | None = None) -> list[str]:
    """Какие таблицы лежат в landing за дату."""
    s = settings or get_settings()
    filesystem = s3_filesystem(s)
    from pyarrow import fs as pafs

    selector = pafs.FileSelector(s.landing_bucket, recursive=True, allow_not_found=True)
    suffix = f"/load_date={load_date.isoformat()}/"
    tables = set()
    for info in filesystem.get_file_info(selector):
        if suffix in info.path and info.path.endswith(".parquet"):
            tables.add(info.path.split("/")[1])
    return sorted(tables)
