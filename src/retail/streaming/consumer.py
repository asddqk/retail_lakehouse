"""Консьюмер: Kafka → Iceberg (bronze.orders_stream) микро-батчами.

* сообщения копятся в батч (по размеру или по времени) и пишутся в Iceberg одним коммитом —
  так не плодятся тысячи мелких файлов;
* offset в Kafka фиксируется только ПОСЛЕ успешной записи в Iceberg.
  Если процесс упадёт между записью и коммитом offset, батч прочитается повторно —
  это гарантия at-least-once; дубли убирает дедупликация в Silver (silver_orders).

Запуск: python -m retail.streaming.consumer --batch-size 500 --flush-seconds 30
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date, datetime, timezone

import pyarrow as pa
from kafka import KafkaConsumer

from retail.bronze import _get_or_create_table, ensure_namespaces
from retail.config import get_settings
from retail.generator import COLUMNS
from retail.storage import get_catalog

log = logging.getLogger(__name__)

TABLE = "bronze.orders_stream"


def to_arrow(messages: list) -> pa.Table:
    columns = COLUMNS["orders"]
    data = {c: pa.array([m.value.get(c) for m in messages], pa.string()) for c in columns}
    n = len(messages)
    data["load_date"] = pa.array([date.today()] * n, pa.date32())
    data["load_ts"] = pa.array([datetime.now(timezone.utc).replace(microsecond=0)] * n, pa.timestamp("us", tz="UTC"))
    data["source_file"] = pa.array([f"kafka://{m.topic}/{m.partition}/{m.offset}" for m in messages], pa.string())
    return pa.table(data)


def flush(messages: list, consumer: KafkaConsumer) -> None:
    batch = to_arrow(messages)
    catalog = get_catalog()
    for attempt in range(5):
        try:
            table = _get_or_create_table(catalog, TABLE, batch.schema)
            table.append(batch)
            break
        except Exception as exc:  # конфликт коммитов в каталоге — повторяем
            log.warning("append failed (attempt %d): %s", attempt + 1, exc)
            time.sleep(2 ** attempt)
    else:
        raise RuntimeError("не удалось записать батч в Iceberg, offset не фиксируем")
    consumer.commit()
    log.info("committed %d messages to %s", len(messages), TABLE)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--flush-seconds", type=int, default=30)
    args = parser.parse_args()

    s = get_settings()
    ensure_namespaces()
    consumer = KafkaConsumer(
        s.kafka_topic,
        bootstrap_servers=s.kafka_bootstrap,
        group_id="bronze-loader",
        enable_auto_commit=False,          # коммитим offset сами, после записи в Iceberg
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    buffer: list = []
    last_flush = time.monotonic()
    while True:
        for records in consumer.poll(timeout_ms=1000, max_records=args.batch_size).values():
            buffer.extend(records)
        due = time.monotonic() - last_flush >= args.flush_seconds
        if buffer and (len(buffer) >= args.batch_size or due):
            flush(buffer, consumer)
            buffer = []
            last_flush = time.monotonic()
        elif due:
            last_flush = time.monotonic()


if __name__ == "__main__":
    main()
