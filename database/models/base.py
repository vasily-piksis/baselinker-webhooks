"""SQLAlchemy DeclarativeBase for all ORM models.

This module provides the base class that all database models inherit from.
It uses SQLAlchemy's declarative_base pattern for ORM mapping, compatible
with both SQLAlchemy 1.4 and 2.0.
"""

from typing import Any

try:
    from sqlalchemy.orm import declarative_base, Mapped, mapped_column
except Exception:  # pragma: no cover - SQLAlchemy < 2.0 fallback
    from sqlalchemy.orm import declarative_base
    from sqlalchemy import Column as mapped_column

    Mapped = Any  # type: ignore[misc]

# Create the declarative base class
# All database models should inherit from this class
Base = declarative_base()

__all__ = ["Base", "Mapped", "mapped_column"]
