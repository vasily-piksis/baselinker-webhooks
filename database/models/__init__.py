"""SQLAlchemy ORM models for the Exchange service database.

This package contains all database models for migrating from file-based
storage to PostgreSQL. Models are designed to be used by both the Exchange
API (FastAPI) and Airflow DAGs.

Tables:
    - events: Webhook events with processing status
    - idempotency_records: Idempotency digests and cached results
    - order_inbox: BaseLinker order webhook events
    - discogs_csv_records: Discogs CSV upload metadata and row data
    - basecom_export_records: Base.com export file metadata and data
    - catalog_state: Product catalog state for BaseLinker Shop Integration
    - master_catalog: Master catalog entries with Discogs release mappings
"""

from database.models.base import Base
from database.models.basecom_export import BasecomExportRecord
from database.models.catalog_state import CatalogState
from database.models.discogs_csv import DiscogsCsvRecord
from database.models.event import Event
from database.models.idempotency import IdempotencyRecord
from database.models.master_catalog import MasterCatalog

__all__ = [
    "Base",
    "Event",
    "IdempotencyRecord",
    "DiscogsCsvRecord",
    "BasecomExportRecord",
    "CatalogState",
    "MasterCatalog",
]
