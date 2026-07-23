"""Alembic migration environment configuration.

This module configures the Alembic migration environment for the Exchange
service database. It imports all SQLAlchemy models and sets up the database
connection for migrations.
"""

from logging.config import fileConfig
from typing import Any, Literal

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# Import database configuration
import sys
import os

# Add project root to path so we can import database models
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# Import Base and all models for autogenerate
# These imports are required for Alembic to detect models during autogenerate
from database.models.base import Base  # noqa: E402
from database.models import (  # noqa: E402, F401  # pylint: disable=unused-import
    Event,
    IdempotencyRecord,
    OrderInbox,
    DiscogsCsvRecord,
    BasecomExportRecord,
    CatalogState,
    MasterCatalog,
)

# Import database config
from database.config import DATABASE_URL  # noqa: E402

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Set database URL from environment or config
config.set_main_option("sqlalchemy.url", DATABASE_URL)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def include_object(
    object: Any,
    name: str | None,
    type_: Literal[
        "schema",
        "table",
        "column",
        "index",
        "unique_constraint",
        "foreign_key_constraint",
    ],
    reflected: bool,
    compare_to: Any | None,
) -> bool:
    """Filter objects to include/exclude from autogenerate.

    This function excludes Airflow metadata tables from migrations,
    as we're using a separate application database.

    Args:
        object: The object being examined
        name: Name of the object
        type_: Type of object (table, column, etc.)
        reflected: Whether the object was reflected from the database
        compare_to: The object being compared against

    Returns:
        bool: True to include the object, False to exclude
    """
    if type_ == "table":
        if name is None:
            return True
        # Exclude Airflow metadata tables (if any exist in the database)
        if (
            name.startswith("ab_")
            or name.startswith("dag_")
            or name.startswith("log_")
            or name.startswith("task_")
        ):
            return False
        # Exclude other Airflow-related tables
        if name in ["alembic_version", "connection", "variable", "xcom"]:
            return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
