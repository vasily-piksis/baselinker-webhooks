"""Processors package for data transformation.

This package contains processors for transforming data between systems:
- inventory_processor.py: Inventory event processing (from processor.py)
- discogs_csv_processor.py: Discogs CSV generation (from discogs_csv.py)
- basecom_processor.py: Base.com file generation (from basecom.py)
"""

from exchange.processors.inventory_processor import (
    process_inventory_event,
    reprocess_event,
    reprocess_event_file,
)
from exchange.processors.discogs_csv_processor import (
    exchange_rows_from_baselinker_event,
    generate_discogs_csv_text,
    generate_discogs_csv_text_for_record,
    inventory_rows_from_baselinker_event,
    write_discogs_csv_file,
)
from exchange.processors.basecom_processor import (
    BASECOM_FIELD_ORDER,
    build_basecom_rows,
    generate_basecom_export_bytes_for_record,
    write_basecom_file,
)

__all__ = [
    # Inventory processor
    "process_inventory_event",
    "reprocess_event",
    "reprocess_event_file",
    # Discogs CSV processor
    "exchange_rows_from_baselinker_event",
    "generate_discogs_csv_text",
    "generate_discogs_csv_text_for_record",
    "inventory_rows_from_baselinker_event",
    "write_discogs_csv_file",
    # Basecom processor
    "BASECOM_FIELD_ORDER",
    "build_basecom_rows",
    "generate_basecom_export_bytes_for_record",
    "write_basecom_file",
]
