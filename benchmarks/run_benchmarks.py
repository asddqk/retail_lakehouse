"""Бенчмарки: время, пиковая память и CPU.

1. formats  — CSV vs Parquet (snappy / zstd): размер, запись, полное чтение,
               чтение двух колонок (column pruning), чтение с фильтром.
2. pruning  — Trino + Iceberg: запрос с фильтром по партиционированной колонке
               и тот же запрос с функцией над колонкой (партиции не отсекаются).
3. engines  — один и тот же аналитический запрос в Trino (Iceberg) и в ClickHouse (MergeTree).

Запуск внутри контейнера airflow:
    python /opt/airflow/benchmarks/run_benchmarks.py              # всё
    python /opt/airflow/benchmarks/run_benchmarks.py --only formats
Результат печатается и сохраняется в benchmarks/results.md.
"""
from __future__ import annotations

import argparse
import os
import statistics
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import psutil

HERE = Path(__file__).resolve().parent


# --------------------------------------------------------------------------- измерения

@dataclass
class Measurement:
    seconds: float = 0.0
    cpu_seconds: float = 0.0
    peak_rss_mb: float = 0.0


@contextmanager
def measure():
    """Замеряет wall time, CPU time процесса и пиковую память (RSS, опрос каждые 5 мс)."""
    proc = psutil.Process(os.getpid())
    result = Measurement()
    base_rss = proc.memory_info().rss
    peak = [base_rss]
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            peak[0] = max(peak[0], proc.memory_info().rss)
            time.sleep(0.005)

    thread = threading.Thread(target=sample, daemon=True)
    thread.start()
    cpu0 = sum(proc.cpu_times()[:2])
    t0 = time.perf_counter()
    try:
        yield result
    finally:
        result.seconds = time.perf_counter() - t0
        result.cpu_seconds = sum(proc.cpu_times()[:2]) - cpu0
        stop.set()
        thread.join()
        result.peak_rss_mb = (peak[0] - base_rss) / 1024 / 1024


def median_of(fn, runs: int = 5) -> Measurement:
    results = []
    for _ in range(runs):
        with measure() as m:
            fn()
        results.append(m)
    return Measurement(
        seconds=statistics.median(r.seconds for r in results),
        cpu_seconds=statistics.median(r.cpu_seconds for r in results),
        peak_rss_mb=max(r.peak_rss_mb for r in results),
    )


def table(headers: list[str], rows: list[list]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(" --- " for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


# --------------------------------------------------------------------------- 1. форматы

def bench_formats(data_dir: Path, rows_target: int) -> str:
    from retail.generator import generate
    from datetime import date, timedelta

    # собираем таблицу заказов нужного размера из сгенерированных выгрузок
    frames, day = [], date(2026, 9, 1)
    total = 0
    while total < rows_target:
        path = data_dir / day.isoformat() / "orders.csv"
        if not path.exists():
            generate(day, data_dir, base_volume=800)
        frame = pl.read_csv(path, infer_schema=False)
        frames.append(frame)
        total += frame.height
        day += timedelta(days=1)
    orders = pl.concat(frames).with_columns(
        pl.col("order_purchase_timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S")
    )

    tmp = Path(tempfile.mkdtemp(prefix="bench_"))
    variants = {
        "CSV": (lambda p: orders.write_csv(p), lambda p, **kw: pl.scan_csv(p, try_parse_dates=True)),
        "Parquet (snappy)": (lambda p: orders.write_parquet(p, compression="snappy"), lambda p, **kw: pl.scan_parquet(p)),
        "Parquet (zstd)": (lambda p: orders.write_parquet(p, compression="zstd"), lambda p, **kw: pl.scan_parquet(p)),
    }
    cutoff = orders["order_purchase_timestamp"].max().replace(hour=0, minute=0, second=0)
    rows = []
    for name, (writer, scanner) in variants.items():
        path = tmp / ("orders.csv" if name == "CSV" else f"orders_{name.split('(')[1][:-1]}.parquet")
        with measure() as w:
            writer(path)
        size_mb = path.stat().st_size / 1024 / 1024
        full = median_of(lambda: scanner(path).collect())
        cols = median_of(lambda: scanner(path).select("order_id", "order_status").collect())
        filt = median_of(lambda: scanner(path).filter(pl.col("order_purchase_timestamp") >= cutoff).collect())
        rows.append([
            name, f"{size_mb:.1f}", f"{w.seconds:.2f}",
            f"{full.seconds:.3f}", f"{full.peak_rss_mb:.0f}",
            f"{cols.seconds:.3f}", f"{cols.peak_rss_mb:.0f}",
            f"{filt.seconds:.3f}",
        ])
    header = (f"### 1. CSV vs Parquet\n\nТаблица заказов: {orders.height:,} строк, "
              f"{orders.width} колонок. Время — медиана 5 запусков.\n\n")
    return header + table(
        ["Формат", "Размер, МБ", "Запись, с", "Чтение всего, с", "Память, МБ",
         "Чтение 2 колонок, с", "Память, МБ", "Фильтр по дате, с"], rows)


# --------------------------------------------------------------------------- 2. партиции в Trino

def _trino_run(cur, sql: str) -> tuple[float, dict]:
    t0 = time.perf_counter()
    cur.execute(sql)
    cur.fetchall()
    return time.perf_counter() - t0, cur.stats or {}


def bench_pruning() -> str:
    from retail.clickhouse_export import trino_connection

    cur = trino_connection().cursor()
    cur.execute("SELECT max(order_date) FROM iceberg.silver.silver_orders")
    (day,) = cur.fetchone()
    queries = {
        "Фильтр по колонке партиции (диапазон времени)":
            f"SELECT count(*), count(DISTINCT customer_id) FROM iceberg.silver.silver_orders "
            f"WHERE order_purchase_ts >= TIMESTAMP '{day} 00:00:00' AND order_purchase_ts < TIMESTAMP '{day} 00:00:00' + INTERVAL '1' DAY",
        "Функция над колонкой (партиции не отсекаются)":
            f"SELECT count(*), count(DISTINCT customer_id) FROM iceberg.silver.silver_orders "
            f"WHERE date_format(order_purchase_ts, '%Y-%m-%d') = '{day}'",
    }
    rows = []
    for name, sql in queries.items():
        _trino_run(cur, sql)  # прогрев
        runs = [_trino_run(cur, sql) for _ in range(5)]
        stats = runs[-1][1]
        rows.append([
            name,
            f"{statistics.median(r[0] for r in runs):.3f}",
            f"{stats.get('processedRows', '—'):,}" if isinstance(stats.get("processedRows"), int) else "—",
            f"{stats.get('physicalInputBytes', 0) / 1024:.0f}" if stats.get("physicalInputBytes") else "—",
        ])
    return ("### 2. Отсечение партиций в Trino + Iceberg\n\n"
            "Таблица silver.silver_orders партиционирована по day(order_purchase_ts).\n\n"
            + table(["Запрос", "Время, с", "Прочитано строк", "Прочитано с диска, КБ"], rows))


# --------------------------------------------------------------------------- 3. Trino vs ClickHouse

ENGINE_SQL = """
SELECT product_category, count(DISTINCT order_id) AS orders, sum(price) AS revenue
FROM {table}
WHERE NOT is_canceled
GROUP BY product_category
ORDER BY revenue DESC
"""


def bench_engines() -> str:
    from retail.clickhouse_export import clickhouse_client, trino_connection

    cur = trino_connection().cursor()
    ch = clickhouse_client()
    trino_sql = ENGINE_SQL.format(table="iceberg.gold.fct_sales")
    ch_sql = ENGINE_SQL.format(table="retail.fct_sales")

    def run_trino():
        cur.execute(trino_sql)
        cur.fetchall()

    run_trino()
    ch.query(ch_sql)  # прогрев
    t = median_of(run_trino)
    c = median_of(lambda: ch.query(ch_sql))
    cur.execute("SELECT count(*) FROM iceberg.gold.fct_sales")
    (n,) = cur.fetchone()
    return (f"### 3. Trino (Iceberg в S3) vs ClickHouse (MergeTree)\n\n"
            f"Выручка и число заказов по категориям, fct_sales: {n:,} строк. Медиана 5 запусков.\n\n"
            + table(["Движок", "Время, с"], [["Trino + Iceberg", f"{t.seconds:.3f}"],
                                             ["ClickHouse", f"{c.seconds:.3f}"]]))


# --------------------------------------------------------------------------- main

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["formats", "pruning", "engines"])
    parser.add_argument("--rows", type=int, default=300_000, help="строк для теста форматов")
    parser.add_argument("--data-dir", default=os.environ.get("RETAIL_DATA_DIR", "/opt/airflow/data"))
    parser.add_argument("--out", default=str(HERE / "results.md"))
    args = parser.parse_args()

    sections = {
        "formats": lambda: bench_formats(Path(args.data_dir) / "bench", args.rows),
        "pruning": bench_pruning,
        "engines": bench_engines,
    }
    parts = ["# Результаты бенчмарков", "",
             f"Запуск: {time.strftime('%Y-%m-%d %H:%M')}, CPU: {psutil.cpu_count()} ядер, "
             f"RAM: {psutil.virtual_memory().total / 1024 ** 3:.1f} ГБ (внутри контейнера Docker)", ""]
    for name, fn in sections.items():
        if args.only and args.only != name:
            continue
        try:
            parts += [fn(), ""]
        except Exception as exc:  # например, стенд не поднят
            parts += [f"### {name}: пропущено ({type(exc).__name__}: {exc})", ""]
        print(parts[-2], flush=True)
    Path(args.out).write_text("\n".join(parts), encoding="utf-8")
    print(f"\nСохранено в {args.out}")


if __name__ == "__main__":
    main()
