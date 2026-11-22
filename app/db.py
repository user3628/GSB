from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine as SAEngine
from sqlalchemy.orm import Session, sessionmaker

from .config import database_path
from .models import Base


class Database:
    def __init__(self, url: str) -> None:
        self.url = url
        path = database_path(url)
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: SAEngine = create_engine(
            url,
            future=True,
            connect_args={"check_same_thread": False, "timeout": 30},
            pool_pre_ping=True,
        )
        event.listen(self.engine, "connect", self._configure_connection)
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)

    @staticmethod
    def _configure_connection(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=FULL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA wal_autocheckpoint=100")
        cursor.close()

    def create_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    @contextmanager
    def read_session(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            # SQLite does not start a read snapshot for a bare SELECT in every
            # driver mode. Pin one explicitly so a query returning many rows and
            # its state_version observe the same committed WAL snapshot.
            session.connection().exec_driver_sql("BEGIN")
            yield session
        finally:
            session.rollback()
            session.close()

    @contextmanager
    def write_session(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def dispose(self) -> None:
        self.engine.dispose()
