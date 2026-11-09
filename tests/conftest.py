"""Общие фикстуры: локальный S3 (moto) и каталог Iceberg на SQLite."""
from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def lakehouse(tmp_path, monkeypatch):
    """Чистый S3 + каталог Iceberg на каждый тест."""
    from moto.server import ThreadedMotoServer

    port = _free_port()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    monkeypatch.setenv("RETAIL_S3_ENDPOINT", f"http://127.0.0.1:{port}")
    monkeypatch.setenv("RETAIL_CATALOG_TYPE", "sql")
    monkeypatch.setenv("RETAIL_CATALOG_URI", f"sqlite:///{tmp_path}/catalog.db")
    monkeypatch.setenv("RETAIL_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "admin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "password")

    from retail import storage

    storage.get_catalog.cache_clear()
    storage.ensure_bucket("landing")
    storage.ensure_bucket("warehouse")
    yield tmp_path
    storage.get_catalog.cache_clear()
    server.stop()
