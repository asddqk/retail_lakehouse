from datetime import date

import pyarrow as pa

from retail import bronze
from retail.cli import run_day
from retail.storage import get_catalog

DAY = date(2026, 10, 15)


def test_day_lands_in_bronze(lakehouse):
    counts = run_day(DAY, volume=100)
    assert set(counts) == {"customers", "sellers", "products", "product_prices",
                           "orders", "order_items", "order_payments"}
    table = get_catalog().load_table("bronze.orders")
    assert table.scan().to_arrow().num_rows == counts["orders"]
    assert [f.name for f in table.spec().fields] == ["load_date"]


def test_reload_is_idempotent(lakehouse):
    first = run_day(DAY, volume=100)
    second = run_day(DAY, volume=100)
    assert first == second
    rows = get_catalog().load_table("bronze.orders").scan().to_arrow().num_rows
    assert rows == first["orders"], "повторная загрузка не должна дублировать строки"


def test_new_source_column_is_added(lakehouse):
    bronze.ensure_namespaces()
    base = pa.table({"seller_id": ["a"], "seller_city": ["x"]})
    bronze.load_table("sellers_evo", DAY, data=base, source_file="test")
    evolved = pa.table({"seller_id": ["b"], "seller_city": ["y"], "seller_rating": ["5"]})
    bronze.load_table("sellers_evo", date(2026, 10, 16), data=evolved, source_file="test")
    table = get_catalog().load_table("bronze.sellers_evo")
    assert "seller_rating" in table.schema().column_names
    assert table.scan().to_arrow().num_rows == 2
