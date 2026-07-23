"""Database session management for shared use by FastAPI and Airflow.

This module provides SQLAlchemy engine and session management that can be used
by both the Exchange API (FastAPI) and Airflow DAGs. It includes connection
pooling, proper session lifecycle management, and error handling.

Usage in FastAPI:
    from database.session import get_db
    from fastapi import Depends
    from sqlalchemy.orm import Session

    @app.get("/items/")
    async def read_items(db: Session = Depends(get_db)):
        items = db.query(Item).all()
        return items

Usage in Airflow:
    from database.session import get_session
    from database.models.event import Event

    def my_task(**context):
        with get_session() as session:
            event = session.query(Event).first()
            session.commit()
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import InterfaceError, OperationalError, ProgrammingError
from sqlalchemy.orm import Session, sessionmaker
import tenacity  # type: ignore[import-untyped]

from database.config import (
    DATABASE_URL,
    MAX_OVERFLOW,
    POOL_ECHO,
    POOL_MONITORING,
    POOL_RECYCLE,
    POOL_SIZE,
    POOL_TIMEOUT,
    RETRY_MAX_ATTEMPTS,
    RETRY_MAX_DELAY,
    RETRY_MIN_DELAY,
)
from exchange.errors import ConnectionError as DatabaseConnectionError, DatabaseError

retry = tenacity.retry
retry_if_exception_type = getattr(tenacity, "retry_if_exception_type")  # type: ignore[attr-defined]
stop_after_attempt = tenacity.stop_after_attempt
wait_exponential = tenacity.wait_exponential

logger = logging.getLogger(__name__)


def _log_pool_event(event_type: str, **kwargs: Any) -> None:
    """Log pool events if monitoring is enabled.

    Args:
        event_type: Pool event name.
        **kwargs: Additional event metadata for logging.
    """
    if POOL_MONITORING:
        logger.debug("Pool event: %s - %s", event_type, kwargs)


# Set up pool event listeners for monitoring
def _setup_pool_monitoring(engine_instance: Any) -> None:
    """Set up connection pool event listeners for monitoring.

    Args:
        engine_instance: SQLAlchemy engine instance to attach listeners to.
    """
    if not POOL_MONITORING:
        return

    @event.listens_for(engine_instance, "connect")
    def on_connect(dbapi_conn: Any, connection_record: Any) -> None:
        """Log when a new connection is created.

        Args:
            dbapi_conn: DB-API connection object.
            connection_record: SQLAlchemy connection record.
        """
        _log_pool_event("connection_created", connection_id=id(dbapi_conn))

    @event.listens_for(engine_instance, "checkout")
    def on_checkout(dbapi_conn: Any, connection_record: Any, connection_proxy: Any) -> None:
        """Log when a connection is checked out from the pool.

        Args:
            dbapi_conn: DB-API connection object.
            connection_record: SQLAlchemy connection record.
            connection_proxy: SQLAlchemy connection proxy.
        """
        _log_pool_event("connection_checked_out", connection_id=id(dbapi_conn))

    @event.listens_for(engine_instance, "checkin")
    def on_checkin(dbapi_conn: Any, connection_record: Any) -> None:
        """Log when a connection is returned to the pool.

        Args:
            dbapi_conn: DB-API connection object.
            connection_record: SQLAlchemy connection record.
        """
        _log_pool_event("connection_checked_in", connection_id=id(dbapi_conn))

    @event.listens_for(engine_instance, "invalidate")
    def on_invalidate(dbapi_conn: Any, connection_record: Any, exception: Any) -> None:
        """Log when a connection is invalidated.

        Args:
            dbapi_conn: DB-API connection object.
            connection_record: SQLAlchemy connection record.
            exception: Exception that triggered invalidation.
        """
        _log_pool_event(
            "connection_invalidated",
            connection_id=id(dbapi_conn),
            exception=str(exception) if exception else None,
        )


# Create SQLAlchemy engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_timeout=POOL_TIMEOUT,
    pool_recycle=POOL_RECYCLE,
    pool_pre_ping=True,  # Test connections before using
    echo=POOL_ECHO,  # SQL query logging (via APP_DATABASE_POOL_ECHO env var)
    future=True,  # Use SQLAlchemy 2.0 style (compatible with 1.4)
)

# Set up pool monitoring if enabled
_setup_pool_monitoring(engine)

# Create session factory bound to engine
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,  # Use transactions
    autoflush=True,  # Auto-flush before queries
    expire_on_commit=False,  # Keep objects accessible after commit
    class_=Session,
)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context manager for database sessions (for Airflow tasks and general use).

    Provides a database session with proper lifecycle management:
    - Session is created on entry
    - Transaction is committed on successful exit
    - Transaction is rolled back on exception
    - Session is closed on exit

    Yields:
        Session: SQLAlchemy session object

    Raises:
        DatabaseConnectionError: If database connection fails
        DatabaseError: If a database error occurs

    Example:
        from database.session import get_session
        from database.models.event import Event

        with get_session() as session:
            event = session.query(Event).first()
            session.commit()  # Explicit commit for changes
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except (OperationalError, InterfaceError) as e:
        session.rollback()
        logger.error("Database connection error: %s", e, exc_info=True)
        raise DatabaseConnectionError(
            "Database connection error",
            error_code="db_connection_error",
            context={"error": str(e)},
        ) from e
    except ProgrammingError as e:
        session.rollback()
        logger.error("Database programming error: %s", e, exc_info=True)
        raise DatabaseError(
            "Database programming error",
            error_code="db_programming_error",
            context={"error": str(e)},
        ) from e
    except Exception as e:
        session.rollback()
        logger.error("Database error: %s", e, exc_info=True)
        raise DatabaseError(
            "Database error",
            error_code="db_error",
            context={"error": str(e)},
        ) from e
    finally:
        session.close()


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency for database session injection.

    This function is designed to be used with FastAPI's Depends() for
    dependency injection. The session is automatically closed after the
    request completes.

    Yields:
        Session: SQLAlchemy session object

    Raises:
        DatabaseConnectionError: If database connection fails
        DatabaseError: If a database error occurs

    Example:
        from database.session import get_db
        from fastapi import Depends
        from sqlalchemy.orm import Session

        @app.get("/items/")
        async def read_items(db: Session = Depends(get_db)):
            items = db.query(Item).all()
            return items
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except (OperationalError, InterfaceError) as e:
        session.rollback()
        logger.error("Database connection error: %s", e, exc_info=True)
        raise DatabaseConnectionError(
            "Database connection error",
            error_code="db_connection_error",
            context={"error": str(e)},
        ) from e
    except ProgrammingError as e:
        session.rollback()
        logger.error("Database programming error: %s", e, exc_info=True)
        raise DatabaseError(
            "Database programming error",
            error_code="db_programming_error",
            context={"error": str(e)},
        ) from e
    except Exception as e:
        session.rollback()
        logger.error("Database error: %s", e, exc_info=True)
        raise DatabaseError(
            "Database error",
            error_code="db_error",
            context={"error": str(e)},
        ) from e
    finally:
        session.close()


def get_engine() -> Engine:
    """Get the SQLAlchemy engine instance.

    Returns:
        Engine: SQLAlchemy engine instance

    Note:
        This is primarily for advanced use cases. Most code should use
        get_session() or get_db() instead.
    """
    return engine


def get_pool_stats() -> Dict[str, Any]:
    """Get connection pool statistics.

    Returns:
        dict: Dictionary containing pool statistics:
            - size: Current pool size
            - checked_out: Number of connections currently checked out
            - overflow: Number of overflow connections in use
            - max_connections: Maximum total connections (pool_size + max_overflow)
            - utilization: Percentage of max connections in use
            - available: Number of available connections

    Example:
        from database.session import get_pool_stats

        stats = get_pool_stats()
        print(f"Pool utilization: {stats['utilization']:.1f}%")
    """
    pool = engine.pool
    size = pool.size()
    checked_out = pool.checkedout()
    overflow = pool.overflow()
    max_connections = POOL_SIZE + MAX_OVERFLOW
    # overflow can be negative (connections below pool size), so use max(0, overflow)
    total_in_use = checked_out + max(0, overflow)
    available = max(0, max_connections - total_in_use)
    utilization = (total_in_use / max_connections * 100) if max_connections > 0 else 0

    stats = {
        "size": size,
        "checked_out": checked_out,
        "overflow": overflow,
        "max_connections": max_connections,
        "available": available,
        "utilization": round(utilization, 2),
    }

    if POOL_MONITORING:
        logger.debug("Pool statistics: %s", stats)

    return stats


def with_retry(func: Callable) -> Callable:
    """Decorator to add retry logic to database operations.

    Retries on transient database connection errors (OperationalError, InterfaceError)
    with exponential backoff.

    Args:
        func: Function to wrap with retry logic

    Returns:
        Wrapped function with retry logic

    Example:
        from database.session import with_retry

        @with_retry
        def my_database_operation(session):
            return session.query(Event).all()
    """
    retry_decorator = retry(
        stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
        wait=wait_exponential(
            multiplier=1,
            min=RETRY_MIN_DELAY,
            max=RETRY_MAX_DELAY,
        ),
        retry=retry_if_exception_type((OperationalError, InterfaceError)),
        reraise=True,
    )

    return retry_decorator(func)


__all__ = [
    "get_db",
    "get_session",
    "get_engine",
    "get_pool_stats",
    "with_retry",
    "SessionLocal",
    "engine",
]
