from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.sql import text

from settings import Settings


class Base(DeclarativeBase):
    pass


_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def init_db(settings: Settings) -> None:
    global _engine, _SessionLocal

    _engine = create_engine(
        settings.sqlalchemy_database_url,
        echo=settings.sqlalchemy_echo,
        future=True,
    )
    _SessionLocal = sessionmaker(
        bind=_engine,
        autoflush=False,
        autocommit=False,
        future=True,
    )


def get_engine() -> Engine:
    if _engine is None:
        raise RuntimeError("Database is not initialized. Call init_db(settings) first.")
    return _engine


def create_tables() -> None:
    import accounting.models  # noqa: F401
    import news.models  # noqa: F401
    import users.models  # noqa: F401

    with get_engine().begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    Base.metadata.create_all(bind=get_engine())


@contextmanager
def get_session() -> Iterator[Session]:
    if _SessionLocal is None:
        raise RuntimeError("Database is not initialized. Call init_db(settings) first.")

    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
