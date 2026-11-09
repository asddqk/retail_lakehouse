from datetime import date

from retail import generator

DAY = date(2026, 10, 15)


def test_generation_is_deterministic():
    first = generator.build_extract(DAY, base_volume=200)
    second = generator.build_extract(DAY, base_volume=200)
    assert first == second


def test_extract_contains_quality_issues():
    extract = generator.build_extract(DAY, base_volume=400)
    orders = extract["orders"]
    ids = [r["order_id"] for r in orders]
    assert len(ids) > len(set(ids)), "должны быть дубли и обновления статусов"
    assert any(r["customer_id"] == "" for r in orders), "должны быть заказы без customer_id"
    assert {"approved", "shipped", "delivered"} <= {r["order_status"] for r in orders}


def test_every_customer_of_extract_is_present():
    extract = generator.build_extract(DAY, base_volume=200)
    customers = {r["customer_id"] for r in extract["customers"]}
    referenced = {r["customer_id"] for r in extract["orders"] if r["customer_id"]}
    assert referenced <= customers


def test_prices_change_over_time():
    early = generator.price_table(date(2026, 2, 1))
    late = generator.price_table(date(2026, 10, 1))
    changed = sum(1 for i in early if early[i][0] != late[i][0])
    assert changed > 100
