"""Настройки берутся из переменных окружения (их задаёт docker-compose).

Значения по умолчанию подходят для запуска внутри docker-compose.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    # S3 / MinIO
    s3_endpoint: str = field(default_factory=lambda: _env("RETAIL_S3_ENDPOINT", "http://minio:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("RETAIL_S3_ACCESS_KEY", "admin"))
    s3_secret_key: str = field(default_factory=lambda: _env("RETAIL_S3_SECRET_KEY", "password"))
    s3_region: str = field(default_factory=lambda: _env("RETAIL_S3_REGION", "us-east-1"))
    landing_bucket: str = field(default_factory=lambda: _env("RETAIL_LANDING_BUCKET", "landing"))
    warehouse_bucket: str = field(default_factory=lambda: _env("RETAIL_WAREHOUSE_BUCKET", "warehouse"))

    # Каталог Iceberg: rest (docker-compose) или sql (локальные тесты и CI)
    catalog_type: str = field(default_factory=lambda: _env("RETAIL_CATALOG_TYPE", "rest"))
    catalog_uri: str = field(default_factory=lambda: _env("RETAIL_CATALOG_URI", "http://iceberg-rest:8181"))
    catalog_warehouse: str = field(default_factory=lambda: _env("RETAIL_CATALOG_WAREHOUSE", "s3://warehouse/"))

    # Trino
    trino_host: str = field(default_factory=lambda: _env("RETAIL_TRINO_HOST", "trino"))
    trino_port: int = field(default_factory=lambda: int(_env("RETAIL_TRINO_PORT", "8080")))
    trino_user: str = field(default_factory=lambda: _env("RETAIL_TRINO_USER", "airflow"))
    trino_catalog: str = field(default_factory=lambda: _env("RETAIL_TRINO_CATALOG", "iceberg"))

    # ClickHouse
    ch_host: str = field(default_factory=lambda: _env("RETAIL_CH_HOST", "clickhouse"))
    ch_port: int = field(default_factory=lambda: int(_env("RETAIL_CH_PORT", "8123")))
    ch_user: str = field(default_factory=lambda: _env("RETAIL_CH_USER", "admin"))
    ch_password: str = field(default_factory=lambda: _env("RETAIL_CH_PASSWORD", "password"))
    ch_database: str = field(default_factory=lambda: _env("RETAIL_CH_DATABASE", "retail"))

    # Kafka
    kafka_bootstrap: str = field(default_factory=lambda: _env("RETAIL_KAFKA_BOOTSTRAP", "kafka:9092"))
    kafka_topic: str = field(default_factory=lambda: _env("RETAIL_KAFKA_TOPIC", "retail.orders"))

    # Локальные каталоги
    data_dir: str = field(default_factory=lambda: _env("RETAIL_DATA_DIR", "/opt/airflow/data"))

    @property
    def s3_endpoint_host(self) -> str:
        """Адрес без схемы: minio:9000."""
        return self.s3_endpoint.split("://", 1)[-1]

    @property
    def s3_scheme(self) -> str:
        return self.s3_endpoint.split("://", 1)[0] if "://" in self.s3_endpoint else "http"


def get_settings() -> Settings:
    return Settings()
