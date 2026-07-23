"""Database configuration and connection settings.

This module provides configuration for the application database connection.
The database URL can be configured via the APP_DATABASE_URL environment variable.
Connection pool settings can be configured via environment variables.
"""

from __future__ import annotations

import os

# Default database URL for local development
DEFAULT_DATABASE_URL = "postgresql+psycopg2://exchange:exchange@localhost:5434/exchange"

# Get database URL from environment or use default
DATABASE_URL = os.getenv("APP_DATABASE_URL", DEFAULT_DATABASE_URL)

# Connection pool settings
POOL_SIZE = int(os.getenv("APP_DATABASE_POOL_SIZE", "5"))
MAX_OVERFLOW = int(os.getenv("APP_DATABASE_MAX_OVERFLOW", "10"))
POOL_TIMEOUT = int(os.getenv("APP_DATABASE_POOL_TIMEOUT", "30"))
POOL_RECYCLE = int(os.getenv("APP_DATABASE_POOL_RECYCLE", "1800"))

# Optional monitoring and logging
POOL_ECHO = os.getenv("APP_DATABASE_POOL_ECHO", "false").lower() == "true"
POOL_MONITORING = os.getenv("APP_DATABASE_POOL_MONITORING", "false").lower() == "true"

# Retry configuration
RETRY_MAX_ATTEMPTS = int(os.getenv("APP_DATABASE_RETRY_MAX_ATTEMPTS", "3"))
RETRY_MIN_DELAY = int(os.getenv("APP_DATABASE_RETRY_MIN_DELAY", "1"))
RETRY_MAX_DELAY = int(os.getenv("APP_DATABASE_RETRY_MAX_DELAY", "10"))

__all__ = [
    "DATABASE_URL",
    "POOL_SIZE",
    "MAX_OVERFLOW",
    "POOL_TIMEOUT",
    "POOL_RECYCLE",
    "POOL_ECHO",
    "POOL_MONITORING",
    "RETRY_MAX_ATTEMPTS",
    "RETRY_MIN_DELAY",
    "RETRY_MAX_DELAY",
]
