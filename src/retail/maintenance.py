"""Обслуживание таблиц Iceberg через Trino.

* optimize            — compaction: склеивает мелкие файлы (каждая загрузка создаёт новые);
* expire_snapshots    — удаляет старые снапшоты (time travel дальше порога станет недоступен);
* remove_orphan_files — удаляет файлы, на которые не ссылается ни один снапшот.

Порог хранения 7 дней — минимальный по умолчанию в Trino (iceberg.expire-snapshots.min-retention).
"""
from __future__ import annotations

import logging

from retail.clickhouse_export import trino_connection
from retail.config import get_settings

log = logging.getLogger(__name__)


def run_maintenance(schemas: list[str] | None = None, retention: str = "7d") -> None:
    s = get_settings()
    conn = trino_connection(s)
    cur = conn.cursor()
    for schema in schemas or ["bronze", "silver", "gold"]:
        cur.execute(
            f"SELECT table_name FROM {s.trino_catalog}.information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'"
        )
        for (table,) in cur.fetchall():
            fqn = f"{s.trino_catalog}.{schema}.{table}"
            for statement in (
                f"ALTER TABLE {fqn} EXECUTE optimize(file_size_threshold => '64MB')",
                f"ALTER TABLE {fqn} EXECUTE expire_snapshots(retention_threshold => '{retention}')",
                f"ALTER TABLE {fqn} EXECUTE remove_orphan_files(retention_threshold => '{retention}')",
            ):
                cur.execute(statement)
                cur.fetchall()
            log.info("maintenance done: %s", fqn)
