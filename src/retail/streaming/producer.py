"""Продюсер: имитирует поток новых заказов с сайта и пишет их в Kafka (JSON).

~2% сообщений отправляются повторно — так ведёт себя продюсер с ретраями
(at-least-once). Дубли убираются дедупликацией в слое Silver.

Запуск: python -m retail.streaming.producer --rate 5
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta

from kafka import KafkaProducer

from retail.config import get_settings
from retail.generator import N_CUSTOMERS, customer_row

log = logging.getLogger(__name__)


def make_order(rnd: random.Random) -> dict:
    now = datetime.now().replace(microsecond=0)
    return {
        "order_id": uuid.uuid4().hex,
        "customer_id": customer_row(rnd.randrange(N_CUSTOMERS))["customer_id"],
        "order_status": "created",
        "order_purchase_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": (now + timedelta(days=rnd.randint(10, 25))).strftime("%Y-%m-%d 00:00:00"),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=5.0, help="заказов в секунду")
    parser.add_argument("--limit", type=int, default=0, help="остановиться после N сообщений (0 — бесконечно)")
    args = parser.parse_args()

    s = get_settings()
    producer = KafkaProducer(
        bootstrap_servers=s.kafka_bootstrap,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
        retries=5,
    )
    rnd = random.Random()
    sent = 0
    while not args.limit or sent < args.limit:
        order = make_order(rnd)
        # ключ = order_id: все события одного заказа попадают в одну партицию (порядок сохраняется)
        producer.send(s.kafka_topic, key=order["order_id"], value=order)
        if rnd.random() < 0.02:
            producer.send(s.kafka_topic, key=order["order_id"], value=order)  # дубль
        sent += 1
        if sent % 100 == 0:
            log.info("sent %d orders to %s", sent, s.kafka_topic)
        time.sleep(1 / args.rate)
    producer.flush()


if __name__ == "__main__":
    main()
