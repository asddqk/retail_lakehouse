"""Командная строка проекта.

Примеры (внутри контейнера airflow):
    python -m retail.cli init
    python -m retail.cli day --date 2026-11-01
    python -m retail.cli backfill --start 2026-10-01 --end 2026-11-07
    python -m retail.cli land --date 2018-01-01 --source /opt/airflow/data/olist   # реальный Olist
    python -m retail.cli export-clickhouse
    python -m retail.cli maintenance
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

from retail import bronze, generator, landing
from retail.config import get_settings
from retail.storage import ensure_bucket

log = logging.getLogger("retail")


def _date(value: str) -> date:
    return date.fromisoformat(value)


def cmd_init(_: argparse.Namespace) -> None:
    s = get_settings()
    ensure_bucket(s.landing_bucket)
    ensure_bucket(s.warehouse_bucket)
    bronze.ensure_namespaces()
    log.info("buckets and namespaces are ready")


def run_day(day: date, volume: int = 800) -> dict[str, int]:
    """Генерация → landing → bronze за один день."""
    s = get_settings()
    raw_dir = Path(s.data_dir) / "generated"
    generator.generate(day, raw_dir, base_volume=volume)
    landing.land_directory(raw_dir / day.isoformat(), day)
    return bronze.load_day(day)


def cmd_day(args: argparse.Namespace) -> None:
    log.info("%s: %s", args.date, run_day(args.date, args.volume))


def cmd_backfill(args: argparse.Namespace) -> None:
    day = args.start
    while day <= args.end:
        log.info("%s: %s", day, run_day(day, args.volume))
        day += timedelta(days=1)


def cmd_generate(args: argparse.Namespace) -> None:
    out = Path(get_settings().data_dir) / "generated"
    paths = generator.generate(args.date, out, base_volume=args.volume)
    log.info("generated %s", {k: str(v) for k, v in paths.items()})


def cmd_land(args: argparse.Namespace) -> None:
    source = args.source or Path(get_settings().data_dir) / "generated" / args.date.isoformat()
    for item in landing.land_directory(source, args.date):
        log.info("%s", item)


def cmd_bronze(args: argparse.Namespace) -> None:
    log.info("%s", bronze.load_day(args.date))


def cmd_export_clickhouse(_: argparse.Namespace) -> None:
    from retail.clickhouse_export import export_gold

    log.info("%s", export_gold())


def cmd_maintenance(args: argparse.Namespace) -> None:
    from retail.maintenance import run_maintenance

    run_maintenance(schemas=args.schemas)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="retail")
    sub = parser.add_subparsers(required=True)

    sub.add_parser("init", help="создать бакеты и схемы").set_defaults(func=cmd_init)

    p = sub.add_parser("day", help="генерация + landing + bronze за день")
    p.add_argument("--date", type=_date, required=True)
    p.add_argument("--volume", type=int, default=800)
    p.set_defaults(func=cmd_day)

    p = sub.add_parser("backfill", help="загрузка истории за диапазон дат")
    p.add_argument("--start", type=_date, required=True)
    p.add_argument("--end", type=_date, required=True)
    p.add_argument("--volume", type=int, default=800)
    p.set_defaults(func=cmd_backfill)

    p = sub.add_parser("generate", help="только сгенерировать CSV")
    p.add_argument("--date", type=_date, required=True)
    p.add_argument("--volume", type=int, default=800)
    p.set_defaults(func=cmd_generate)

    p = sub.add_parser("land", help="CSV → Parquet в S3")
    p.add_argument("--date", type=_date, required=True)
    p.add_argument("--source", type=Path, help="каталог с CSV (например, реальный Olist)")
    p.set_defaults(func=cmd_land)

    p = sub.add_parser("bronze", help="landing → Iceberg bronze")
    p.add_argument("--date", type=_date, required=True)
    p.set_defaults(func=cmd_bronze)

    sub.add_parser("export-clickhouse", help="витрины Gold → ClickHouse").set_defaults(func=cmd_export_clickhouse)

    p = sub.add_parser("maintenance", help="compaction и очистка снапшотов Iceberg")
    p.add_argument("--schemas", nargs="+", default=["bronze", "silver", "gold"])
    p.set_defaults(func=cmd_maintenance)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
