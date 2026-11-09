"""Ежедневный пайплайн Retail Lakehouse.

    init → generate → land → load_bronze → dbt (Cosmos: модели + тесты) → export_clickhouse

* generate     — выгрузка учётной системы за день (CSV, схема Olist);
* land         — CSV → Parquet в S3 (MinIO), бакет landing;
* load_bronze  — landing → Iceberg bronze (идемпотентно, перезапись партиции дня);
* dbt          — Cosmos превращает каждую модель и её тесты dbt в отдельные задачи Airflow;
* export_clickhouse — витрины Gold → ClickHouse.

Запуск за конкретную дату: Trigger DAG → параметр run_date (YYYY-MM-DD).
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, TestBehavior

DBT_PROJECT_DIR = Path(os.environ.get("RETAIL_DBT_PROJECT_DIR", "/opt/airflow/dbt/retail_lakehouse"))
DBT_EXECUTABLE = os.environ.get("RETAIL_DBT_EXECUTABLE", "/opt/airflow/dbt_venv/bin/dbt")
DBT_TARGET = os.environ.get("RETAIL_DBT_TARGET", "trino")


def _run_date() -> date:
    """Дата загрузки: параметр run_date или дата запуска DAG."""
    ctx = get_current_context()
    override = ctx["params"].get("run_date")
    if override:
        return date.fromisoformat(override)
    run = ctx["dag_run"]
    return (run.logical_date or run.run_after).date()


with DAG(
    dag_id="retail_daily",
    description="Генерация → S3 → Iceberg bronze → dbt silver/gold → ClickHouse",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    params={"run_date": Param(None, type=["null", "string"], description="Дата загрузки YYYY-MM-DD")},
    tags=["retail", "lakehouse"],
    doc_md=__doc__,
) as dag:

    @task
    def init() -> None:
        from retail.cli import cmd_init

        cmd_init(None)

    @task
    def generate() -> str:
        from retail.config import get_settings
        from retail.generator import generate as gen

        day = _run_date()
        gen(day, Path(get_settings().data_dir) / "generated")
        return day.isoformat()

    @task
    def land(run_date: str) -> list[dict]:
        from retail.config import get_settings
        from retail.landing import land_directory

        day = date.fromisoformat(run_date)
        return land_directory(Path(get_settings().data_dir) / "generated" / run_date, day)

    @task
    def load_bronze(run_date: str) -> dict[str, int]:
        from retail.bronze import load_day

        return load_day(date.fromisoformat(run_date))

    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="retail_lakehouse",
            target_name=DBT_TARGET,
            profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
        ),
        # dbt установлен в отдельном venv (рекомендация Cosmos), запускается отдельным процессом
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            test_behavior=TestBehavior.AFTER_EACH,  # тесты модели запускаются сразу после неё
            # тест, зависящий от нескольких моделей (например, сверка платежей и позиций),
            # выносится в отдельную задачу и запускается после всех этих моделей
            should_detach_multiple_parents_tests=True,
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
    )

    @task
    def export_clickhouse() -> dict[str, int]:
        from retail.clickhouse_export import export_gold

        return export_gold()

    run_date = generate()
    landed = land(run_date)
    bronze = load_bronze(run_date)

    init() >> run_date
    landed >> bronze >> dbt >> export_clickhouse()
