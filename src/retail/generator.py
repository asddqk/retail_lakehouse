"""Генератор ежедневных выгрузок интернет-магазина в схеме датасета Olist.

Каждый день генератор пишет CSV-файлы так, как их отдавала бы учётная система:

* orders          — новые заказы дня + заказы прошлых дней, у которых сменился статус (CDC);
* order_items     — позиции заказов, впервые попавших в выгрузку;
* order_payments  — платежи по этим заказам;
* customers       — покупатели из этих заказов;
* sellers, products, product_prices — полные справочники на дату.

Чтобы было на чём проверять качество данных, в выгрузку намеренно добавляются:

* точные дубли строк (~2% заказов, ~1% позиций);
* заказы без customer_id (~0.5%) — должны уйти в карантин;
* опоздавшие записи (~3% заказов приходят через 1–3 дня после даты покупки);
* расхождение суммы платежей и суммы позиций (~0.3% заказов).

Генерация детерминирована: одна и та же дата всегда даёт одни и те же файлы,
поэтому перезапуск загрузки за день идемпотентен.
"""
from __future__ import annotations

import csv
import hashlib
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

EPOCH = date(2026, 1, 1)  # дата, с которой считаются изменения цен
N_CUSTOMERS = 5000
N_SELLERS = 150
N_PRODUCTS = 600
STATUS_WINDOW_DAYS = 12  # сколько дней назад ищем заказы со сменой статуса

STATES = [
    ("SP", "sao paulo", "01"), ("RJ", "rio de janeiro", "20"), ("MG", "belo horizonte", "30"),
    ("RS", "porto alegre", "90"), ("PR", "curitiba", "80"), ("SC", "florianopolis", "88"),
    ("BA", "salvador", "40"), ("DF", "brasilia", "70"),
]
CATEGORIES = [
    ("cama_mesa_banho", 60.0), ("beleza_saude", 80.0), ("esporte_lazer", 90.0),
    ("informatica_acessorios", 150.0), ("moveis_decoracao", 120.0), ("utilidades_domesticas", 50.0),
    ("relogios_presentes", 200.0), ("telefonia", 110.0), ("brinquedos", 70.0), ("automotivo", 95.0),
]
PAYMENT_TYPES = [("credit_card", 74), ("boleto", 19), ("voucher", 5), ("debit_card", 2)]

COLUMNS = {
    "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "sellers": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    "products": ["product_id", "product_category_name", "product_weight_g", "product_length_cm",
                 "product_height_cm", "product_width_cm"],
    "product_prices": ["product_id", "list_price", "price_updated_at"],
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
               "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
    "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price",
                    "freight_value"],
    "order_payments": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
}
TABLES = list(COLUMNS)


# --------------------------------------------------------------------------- helpers

def _h(*parts: object) -> int:
    """Стабильный хеш (не зависит от PYTHONHASHSEED)."""
    return int(hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()[:12], 16)


def _id(*parts: object) -> str:
    """32-символьный идентификатор в стиле Olist."""
    return hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()


def _ts(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def _weighted(rnd: random.Random, options: list[tuple[str, int]]) -> str:
    return rnd.choices([o for o, _ in options], weights=[w for _, w in options])[0]


# --------------------------------------------------------------------------- справочники

def customer_row(idx: int) -> dict:
    state, city, zip_prefix = STATES[_h("cs", idx) % len(STATES)]
    return {
        "customer_id": _id("customer", idx),
        "customer_unique_id": _id("customer_unique", idx),
        "customer_zip_code_prefix": f"{zip_prefix}{_h('cz', idx) % 1000:03d}",
        "customer_city": city,
        "customer_state": state,
    }


def seller_row(idx: int) -> dict:
    state, city, zip_prefix = STATES[_h("ss", idx) % len(STATES)]
    return {
        "seller_id": _id("seller", idx),
        "seller_zip_code_prefix": f"{zip_prefix}{_h('sz', idx) % 1000:03d}",
        "seller_city": city,
        "seller_state": state,
    }


def product_seller_idx(idx: int) -> int:
    return _h("ps", idx) % N_SELLERS


def product_row(idx: int) -> dict:
    category, _ = CATEGORIES[_h("pc", idx) % len(CATEGORIES)]
    return {
        "product_id": _id("product", idx),
        "product_category_name": category,
        "product_weight_g": str(100 + _h("pw", idx) % 9900),
        "product_length_cm": str(10 + _h("pl", idx) % 90),
        "product_height_cm": str(2 + _h("ph", idx) % 60),
        "product_width_cm": str(8 + _h("pwd", idx) % 60),
    }


def base_price(idx: int) -> float:
    _, cat_price = CATEGORIES[_h("pc", idx) % len(CATEGORIES)]
    return round(cat_price * (0.5 + (_h("pb", idx) % 150) / 100), 2)


_PRICE_CACHE: dict[date, dict[int, tuple[float, date]]] = {}


def price_table(as_of: date) -> dict[int, tuple[float, date]]:
    """Цена каждого товара на дату и дата последнего изменения.

    Цена меняется примерно раз в 40 дней на −10%…+10%: это история для SCD2.
    Результаты кешируются, следующая дата досчитывается от ближайшей предыдущей.
    """
    if as_of in _PRICE_CACHE:
        return _PRICE_CACHE[as_of]
    earlier = [d for d in _PRICE_CACHE if d < as_of]
    if earlier:
        start = max(earlier)
        prices = dict(_PRICE_CACHE[start])
        day = start + timedelta(days=1)
    else:
        prices = {i: (base_price(i), EPOCH) for i in range(N_PRODUCTS)}
        day = EPOCH + timedelta(days=1)
    while day <= as_of:
        for i in range(N_PRODUCTS):
            if _h("price_change", i, day) % 40 == 0:
                factor = 0.9 + (_h("price_factor", i, day) % 21) / 100
                prices[i] = (round(prices[i][0] * factor, 2), day)
        day += timedelta(days=1)
    if len(_PRICE_CACHE) > 64:
        _PRICE_CACHE.pop(min(_PRICE_CACHE))
    _PRICE_CACHE[as_of] = prices
    return prices


# --------------------------------------------------------------------------- заказы

@dataclass
class Order:
    order_id: str
    customer_idx: int
    purchase_ts: datetime
    approved_delay_min: int | None
    ship_age: int
    deliver_age: int
    cancel_age: int | None
    late_days: int
    estimated_days: int
    missing_customer: bool
    items: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)

    @property
    def purchase_date(self) -> date:
        return self.purchase_ts.date()

    @property
    def first_seen(self) -> date:
        return self.purchase_date + timedelta(days=self.late_days)

    def status_at(self, day: date) -> str:
        age = (day - self.purchase_date).days
        if self.cancel_age is not None and age >= self.cancel_age:
            return "canceled"
        if age >= self.deliver_age:
            return "delivered"
        if age >= self.ship_age:
            return "shipped"
        if age >= 1:
            return "invoiced"
        return "approved" if self.approved_delay_min is not None else "created"

    def row_at(self, day: date) -> dict:
        status = self.status_at(day)
        p = self.purchase_ts
        approved = p + timedelta(minutes=self.approved_delay_min) if self.approved_delay_min is not None else None
        shipped = status in ("shipped", "delivered")
        carrier = (datetime.combine(self.purchase_date + timedelta(days=self.ship_age), datetime.min.time())
                   + timedelta(hours=14)) if shipped else None
        delivered = (datetime.combine(self.purchase_date + timedelta(days=self.deliver_age), datetime.min.time())
                     + timedelta(hours=18)) if status == "delivered" else None
        estimated = datetime.combine(self.purchase_date + timedelta(days=self.estimated_days), datetime.min.time())
        return {
            "order_id": self.order_id,
            "customer_id": "" if self.missing_customer else customer_row(self.customer_idx)["customer_id"],
            "order_status": status,
            "order_purchase_timestamp": _ts(p),
            "order_approved_at": _ts(approved),
            "order_delivered_carrier_date": _ts(carrier),
            "order_delivered_customer_date": _ts(delivered),
            "order_estimated_delivery_date": _ts(estimated),
        }


def orders_created_on(day: date, prices: dict[int, tuple[float, date]] | None = None,
                      base_volume: int = 800) -> list[Order]:
    """Все заказы, оформленные в этот день (детерминированно)."""
    rnd = random.Random(f"orders-{day.isoformat()}")
    prices = prices or price_table(day)
    weekend = day.weekday() >= 5
    volume = int(base_volume * (1.2 if weekend else 1.0) * rnd.uniform(0.9, 1.1))
    orders = []
    for n in range(volume):
        purchase_ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rnd.randrange(86400))
        ship_age = rnd.randint(1, 3)
        order = Order(
            order_id=_id("order", day, n),
            customer_idx=rnd.randrange(N_CUSTOMERS),
            purchase_ts=purchase_ts,
            approved_delay_min=None if rnd.random() < 0.01 else rnd.randint(5, 180),
            ship_age=ship_age,
            deliver_age=ship_age + rnd.randint(2, 8),
            cancel_age=rnd.randint(0, 1) if rnd.random() < 0.03 else None,
            late_days=rnd.randint(1, 3) if rnd.random() < 0.03 else 0,
            estimated_days=rnd.randint(10, 25),
            missing_customer=rnd.random() < 0.005,
        )
        # позиции заказа
        total = 0.0
        for item_no in range(1, rnd.choice([1, 1, 1, 1, 2, 2, 3]) + 1):
            product_idx = rnd.randrange(N_PRODUCTS)
            list_price = prices[product_idx][0]
            price = round(list_price * (1 - rnd.choice([0, 0, 0, 0.05, 0.1, 0.15])), 2)
            freight = round(rnd.uniform(5, 40), 2)
            total += price + freight
            order.items.append({
                "order_id": order.order_id,
                "order_item_id": str(item_no),
                "product_id": _id("product", product_idx),
                "seller_id": _id("seller", product_seller_idx(product_idx)),
                "shipping_limit_date": _ts(purchase_ts + timedelta(days=ship_age)),
                "price": f"{price:.2f}",
                "freight_value": f"{freight:.2f}",
            })
        # платежи
        total = round(total, 2)
        if rnd.random() < 0.003:  # намеренное расхождение для теста качества
            total = round(total + rnd.uniform(0.01, 5), 2)
        ptype = _weighted(rnd, PAYMENT_TYPES)
        if ptype == "voucher" and total > 20:
            voucher = round(min(total * rnd.uniform(0.1, 0.5), 50), 2)
            parts = [("voucher", voucher, 1), ("credit_card", round(total - voucher, 2), rnd.randint(1, 6))]
        else:
            installments = rnd.randint(1, 10) if ptype == "credit_card" else 1
            parts = [(ptype, total, installments)]
        for seq, (pt, value, inst) in enumerate(parts, start=1):
            order.payments.append({
                "order_id": order.order_id,
                "payment_sequential": str(seq),
                "payment_type": pt,
                "payment_installments": str(inst),
                "payment_value": f"{value:.2f}",
            })
        orders.append(order)
    return orders


# --------------------------------------------------------------------------- выгрузка дня

def build_extract(day: date, base_volume: int = 800) -> dict[str, list[dict]]:
    """Строки всех таблиц для выгрузки за день `day`."""
    for back in range(STATUS_WINDOW_DAYS, -1, -1):  # прогрев кеша цен по возрастанию дат
        price_table(day - timedelta(days=back))
    prices = price_table(day)
    rnd = random.Random(f"extract-{day.isoformat()}")

    orders_rows: list[dict] = []
    items_rows: list[dict] = []
    payments_rows: list[dict] = []
    customer_idxs: set[int] = set()

    for back in range(0, STATUS_WINDOW_DAYS + 1):
        created = day - timedelta(days=back)
        for order in orders_created_on(created, price_table(created) if back else prices, base_volume):
            if order.first_seen == day:  # первое появление заказа в выгрузке
                orders_rows.append(order.row_at(day))
                items_rows.extend(order.items)
                payments_rows.extend(order.payments)
                if not order.missing_customer:
                    customer_idxs.add(order.customer_idx)
            elif order.first_seen < day and order.status_at(day) != order.status_at(day - timedelta(days=1)):
                orders_rows.append(order.row_at(day))  # обновление статуса (CDC)
                if not order.missing_customer:
                    customer_idxs.add(order.customer_idx)  # справочник покупателей для всех заказов выгрузки

    # намеренные дубли
    orders_rows += [dict(r) for r in rnd.sample(orders_rows, k=int(len(orders_rows) * 0.02))]
    items_rows += [dict(r) for r in rnd.sample(items_rows, k=int(len(items_rows) * 0.01))]

    return {
        "customers": [customer_row(i) for i in sorted(customer_idxs)],
        "sellers": [seller_row(i) for i in range(N_SELLERS)],
        "products": [product_row(i) for i in range(N_PRODUCTS)],
        "product_prices": [
            {"product_id": _id("product", i), "list_price": f"{p:.2f}", "price_updated_at": f"{d.isoformat()} 00:00:00"}
            for i, (p, d) in prices.items()
        ],
        "orders": orders_rows,
        "order_items": items_rows,
        "order_payments": payments_rows,
    }


def generate(day: date, out_dir: str | Path, base_volume: int = 800) -> dict[str, Path]:
    """Пишет CSV-файлы за день в out_dir/<YYYY-MM-DD>/<table>.csv и возвращает пути."""
    target = Path(out_dir) / day.isoformat()
    target.mkdir(parents=True, exist_ok=True)
    paths = {}
    for table, rows in build_extract(day, base_volume).items():
        path = target / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS[table])
            writer.writeheader()
            writer.writerows(rows)
        paths[table] = path
    return paths
