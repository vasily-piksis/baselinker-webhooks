# exchange/processor.py
"""Compatibility shim for exchange/processor.py.

This module re-exports from the new location at exchange/processors/inventory_processor.py.
For new code, import directly from exchange.processors:
    from exchange.processors import process_inventory_event, reprocess_event
"""

from exchange.processors.inventory_processor import (
    process_inventory_event,
    reprocess_event,
    reprocess_event_file,
)

__all__ = [
    "process_inventory_event",
    "reprocess_event",
    "reprocess_event_file",
]
