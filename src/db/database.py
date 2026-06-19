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
    with get_engine().begin() as connection:
        connection.execute(
            text("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS topic varchar(256)")
        )
        connection.execute(
            text(
                """
                ALTER TABLE news_articles
                ADD COLUMN IF NOT EXISTS visibility varchar(32) NOT NULL DEFAULT 'public'
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE news_articles
                SET status = CASE status
                    WHEN 'raw' THEN 'pending'
                    WHEN 'queued' THEN 'pending'
                    WHEN 'parsed' THEN 'pending'
                    WHEN 'vectorized' THEN 'processed'
                    WHEN 'failed' THEN 'error'
                    ELSE status
                END
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE news_articles
                SET metadata = jsonb_set(
                    COALESCE(metadata, '{}'::jsonb),
                    '{pipeline_error}',
                    jsonb_build_object('message', metadata->>'pipeline_error'),
                    true
                )
                WHERE jsonb_typeof(metadata->'pipeline_error') = 'string'
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE article_pipeline_state
                SET novelty_label = COALESCE(NULLIF(novelty_label, ''), 'significant'),
                    p_significant = COALESCE(
                        p_significant,
                        CASE
                            WHEN novelty_label IN ('minor', 'duplicate') THEN 0.0
                            ELSE 1.0
                        END
                    ),
                    novelty_comment = COALESCE(
                        novelty_comment,
                        'cluster seed; significant by definition'
                    )
                WHERE novelty_label IS NULL
                   OR novelty_label = ''
                   OR p_significant IS NULL
                """
            )
        )
        connection.execute(
            text(
                """
                ALTER TABLE news_articles
                    ALTER COLUMN status SET DEFAULT 'not_started',
                    ALTER COLUMN visibility SET DEFAULT 'draft',
                    ALTER COLUMN content SET NOT NULL,
                    ALTER COLUMN published_at SET NOT NULL
                """
            )
        )
        connection.execute(
            text(
                """
                ALTER TABLE article_pipeline_state
                    ALTER COLUMN novelty_label SET NOT NULL,
                    ALTER COLUMN p_significant SET NOT NULL
                """
            )
        )
        connection.execute(
            text(
                """
                ALTER TABLE news_articles
                    DROP CONSTRAINT IF EXISTS ck_news_articles_status,
                    DROP CONSTRAINT IF EXISTS ck_news_articles_visibility,
                    DROP CONSTRAINT IF EXISTS uq_news_article_canonical_url,
                    DROP CONSTRAINT IF EXISTS uq_news_article_content_hash;

                ALTER TABLE news_articles
                    ADD CONSTRAINT ck_news_articles_status
                    CHECK (
                        status IN (
                            'not_started', 'pending', 'processing', 'processed', 'error'
                        )
                    ),
                    ADD CONSTRAINT ck_news_articles_visibility
                    CHECK (visibility IN ('draft', 'public'));

                CREATE INDEX IF NOT EXISTS ix_news_articles_canonical_url
                    ON news_articles (canonical_url);
                CREATE INDEX IF NOT EXISTS ix_news_articles_content_hash
                    ON news_articles (content_hash);
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_article_pipeline_embeddings_hnsw_cosine
                ON article_pipeline_embeddings
                USING hnsw (embedding vector_cosine_ops)
                """
            )
        )


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
