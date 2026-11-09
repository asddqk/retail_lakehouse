"""Bronze: Parquet из landing → таблицы Iceberg (namespace `bronze`).

* данные сохраняются как есть (строки) + технические поля load_date, load_ts, source_file;
* таблицы партиционированы по load_date;
* загрузка за дату идемпотентна: партиция load_date перезаписывается целиком
  (overwrite с фильтром), повторный запуск не создаёт дублей;
* новые колонки в источнике добавляются в таблицу автоматически (schema evolution).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo
from pyiceberg.table import Table

from retail.config import Settings, get_settings
from retail.landing import landed_tables, landing_key
from retail.storage import get_catalog, s3_filesystem

log = logging.getLogger(__name__)

NAMESPACES = ("bronze", "silver", "gold", "snapshots", "quality")
TECH_COLUMNS = {
    "load_date": pa.date32(),
    "load_ts": pa.timestamp("us", tz="UTC"),
    "source_file": pa.string(),
}


def ensure_namespaces(catalog: Catalog | None = None, settings: Settings | None = None) -> None:
    """Создаёт namespace'ы (схемы) с явным расположением в бакете warehouse."""
    s = settings or get_settings()
    catalog = catalog or get_catalog()
    for ns in NAMESPACES:
        try:
            catalog.create_namespace(ns, {"location": f"{s.catalog_warehouse.rstrip('/')}/{ns}"})
            log.info("created namespace %s", ns)
        except NamespaceAlreadyExistsError:
            pass


def _with_tech_columns(data: pa.Table, load_date: date, source_file: str) -> pa.Table:
    n = data.num_rows
    load_ts = datetime.now(timezone.utc).replace(microsecond=0)
    data = data.append_column("load_date", pa.array([load_date] * n, pa.date32()))
    data = data.append_column("load_ts", pa.array([load_ts] * n, pa.timestamp("us", tz="UTC")))
    return data.append_column("source_file", pa.array([source_file] * n, pa.string()))


def _get_or_create_table(catalog: Catalog, identifier: str, schema: pa.Schema) -> Table:
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        table = catalog.create_table(identifier, schema=schema, properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "zstd",
        })
        with table.update_spec() as spec:
            spec.add_identity("load_date")
        log.info("created table %s", identifier)
        return catalog.load_table(identifier)
    # schema evolution: новые колонки из источника добавляются в таблицу
    with table.update_schema() as update:
        update.union_by_name(schema)
    return catalog.load_table(identifier)


def _align(data: pa.Table, table: Table) -> pa.Table:
    """Приводит набор колонок к схеме таблицы (недостающие колонки → NULL)."""
    target = table.schema().as_arrow()
    columns = []
    for fld in target:
        if fld.name in data.column_names:
            columns.append(data.column(fld.name).cast(fld.type))
        else:
            columns.append(pa.nulls(data.num_rows, fld.type))
    return pa.Table.from_arrays(columns, schema=target)


def load_table(table_name: str, load_date: date, catalog: Catalog | None = None,
               settings: Settings | None = None, data: pa.Table | None = None,
               source_file: str | None = None) -> int:
    """Загружает одну таблицу за дату. Возвращает число строк."""
    s = settings or get_settings()
    catalog = catalog or get_catalog()
    if data is None:
        key = landing_key(table_name, load_date, s)
        with s3_filesystem(s).open_input_file(key) as fh:
            data = pq.read_table(fh)
        source_file = f"s3://{key}"
    data = _with_tech_columns(data, load_date, source_file or "unknown")

    table = _get_or_create_table(catalog, f"bronze.{table_name}", data.schema)
    aligned = _align(data, table)
    # идемпотентность: перезаписываем только партицию этой даты
    table.overwrite(aligned, overwrite_filter=EqualTo("load_date", load_date.isoformat()))
    log.info("bronze.%s: %d rows for load_date=%s", table_name, aligned.num_rows, load_date)
    return aligned.num_rows


def load_day(load_date: date, catalog: Catalog | None = None, settings: Settings | None = None) -> dict[str, int]:
    """Загружает в Bronze все таблицы, которые есть в landing за дату."""
    s = settings or get_settings()
    catalog = catalog or get_catalog()
    ensure_namespaces(catalog, s)
    tables = landed_tables(load_date, s)
    if not tables:
        raise FileNotFoundError(f"в landing нет файлов за {load_date}")
    return {name: load_table(name, load_date, catalog, s) for name in tables}
