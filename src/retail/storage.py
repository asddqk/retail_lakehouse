"""Подключения к S3 (MinIO) и каталогу Iceberg."""
from __future__ import annotations

from functools import lru_cache

from pyarrow import fs as pafs
from pyiceberg.catalog import Catalog, load_catalog

from retail.config import Settings, get_settings


def s3_filesystem(settings: Settings | None = None) -> pafs.S3FileSystem:
    s = settings or get_settings()
    return pafs.S3FileSystem(
        access_key=s.s3_access_key,
        secret_key=s.s3_secret_key,
        endpoint_override=s.s3_endpoint_host,
        scheme=s.s3_scheme,
        region=s.s3_region,
        allow_bucket_creation=True,
    )


def catalog_properties(settings: Settings | None = None) -> dict[str, str]:
    s = settings or get_settings()
    props = {
        "type": s.catalog_type,
        "uri": s.catalog_uri,
        "warehouse": s.catalog_warehouse,
        "s3.endpoint": s.s3_endpoint,
        "s3.access-key-id": s.s3_access_key,
        "s3.secret-access-key": s.s3_secret_key,
        "s3.region": s.s3_region,
    }
    return props


@lru_cache(maxsize=1)
def get_catalog() -> Catalog:
    return load_catalog("retail", **catalog_properties())


def ensure_bucket(bucket: str, settings: Settings | None = None) -> None:
    filesystem = s3_filesystem(settings)
    info = filesystem.get_file_info(bucket)
    if info.type == pafs.FileType.NotFound:
        filesystem.create_dir(bucket)
