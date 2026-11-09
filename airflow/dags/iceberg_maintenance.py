"""Еженедельное обслуживание таблиц Iceberg.

Каждая ежедневная загрузка добавляет новые файлы и снапшоты. Без обслуживания
таблица обрастает тысячами мелких файлов, и чтение замедляется.

* optimize            — compaction мелких файлов;
* expire_snapshots    — удаление снапшотов старше 7 дней;
* remove_orphan_files — удаление файлов, на которые никто не ссылается.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, task

with DAG(
    dag_id="iceberg_maintenance",
    schedule="0 4 * * 0",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["retail", "iceberg"],
    doc_md=__doc__,
) as dag:

    @task
    def maintain(schema: str) -> None:
        from retail.maintenance import run_maintenance

        run_maintenance(schemas=[schema])

    # по одной задаче на слой: видно, где ушло больше времени
    maintain.override(task_id="bronze")("bronze") >> \
        maintain.override(task_id="silver")("silver") >> \
        maintain.override(task_id="gold")("gold")
