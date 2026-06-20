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
    _import_models()

    with get_engine().begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    Base.metadata.create_all(bind=get_engine())
    with get_engine().begin() as connection:
        connection.execute(
            text(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS organization_id uuid
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO organizations (id, name, created_at)
                SELECT id, login, NOW()
                FROM users
                WHERE organization_id IS NULL
                ON CONFLICT (id) DO NOTHING
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE users
                SET organization_id = id
                WHERE organization_id IS NULL
                """
            )
        )
        connection.execute(
            text(
                """
                ALTER TABLE users
                    ALTER COLUMN organization_id SET NOT NULL,
                    DROP CONSTRAINT IF EXISTS fk_users_organization_id;

                ALTER TABLE users
                    ADD CONSTRAINT fk_users_organization_id
                    FOREIGN KEY (organization_id)
                    REFERENCES organizations (id)
                    ON DELETE RESTRICT;

                CREATE INDEX IF NOT EXISTS ix_users_organization_id
                    ON users (organization_id)
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'accounts'
                          AND column_name = 'user_id'
                    ) THEN
                        ALTER TABLE accounts RENAME COLUMN user_id TO organization_id;
                        ALTER TABLE accounts
                            DROP CONSTRAINT IF EXISTS accounts_user_id_fkey,
                            ADD CONSTRAINT accounts_organization_id_fkey
                            FOREIGN KEY (organization_id)
                            REFERENCES organizations (id)
                            ON DELETE CASCADE;
                    END IF;
                END
                $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'transactions'
                          AND column_name = 'user_id'
                    ) THEN
                        ALTER TABLE transactions RENAME COLUMN user_id TO actor_user_id;
                        ALTER TABLE transactions
                            ADD COLUMN organization_id uuid;

                        UPDATE transactions AS transaction
                        SET organization_id = users.organization_id
                        FROM users
                        WHERE transaction.actor_user_id = users.id;

                        ALTER TABLE transactions
                            ALTER COLUMN organization_id SET NOT NULL,
                            DROP CONSTRAINT IF EXISTS transactions_user_id_fkey,
                            ADD CONSTRAINT transactions_actor_user_id_fkey
                            FOREIGN KEY (actor_user_id)
                            REFERENCES users (id)
                            ON DELETE SET NULL,
                            ADD CONSTRAINT transactions_organization_id_fkey
                            FOREIGN KEY (organization_id)
                            REFERENCES organizations (id)
                            ON DELETE CASCADE;

                        ALTER INDEX IF EXISTS ix_transactions_user_id
                            RENAME TO ix_transactions_actor_user_id;
                        CREATE INDEX IF NOT EXISTS ix_transactions_organization_id
                            ON transactions (organization_id);
                    END IF;
                END
                $$;
                """
            )
        )
        connection.execute(
            text("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS topic varchar(256)")
        )
        connection.execute(
            text(
                """
                ALTER TABLE transactions
                ADD COLUMN IF NOT EXISTS batch_id uuid;

                CREATE INDEX IF NOT EXISTS ix_transactions_batch_id
                    ON transactions (batch_id)
                """
            )
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
                ALTER TABLE article_pipeline_state
                    ADD COLUMN IF NOT EXISTS manual_novelty_label varchar(32),
                    ADD COLUMN IF NOT EXISTS manual_novelty_actor_id uuid,
                    ADD COLUMN IF NOT EXISTS manual_novelty_updated_at timestamptz;

                ALTER TABLE article_pipeline_state
                    DROP CONSTRAINT IF EXISTS ck_article_pipeline_manual_novelty_label,
                    DROP CONSTRAINT IF EXISTS fk_article_pipeline_manual_novelty_actor;

                ALTER TABLE article_pipeline_state
                    ADD CONSTRAINT ck_article_pipeline_manual_novelty_label
                    CHECK (
                        manual_novelty_label IS NULL
                        OR manual_novelty_label IN ('significant', 'minor', 'duplicate')
                    ),
                    ADD CONSTRAINT fk_article_pipeline_manual_novelty_actor
                    FOREIGN KEY (manual_novelty_actor_id)
                    REFERENCES users (id)
                    ON DELETE SET NULL;

                CREATE INDEX IF NOT EXISTS ix_article_pipeline_manual_novelty_actor
                    ON article_pipeline_state (manual_novelty_actor_id);
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
                    CHECK (visibility IN ('draft', 'public', 'archived'));

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


def drop_tables() -> None:
    _import_models()
    Base.metadata.drop_all(bind=get_engine())


def _import_models() -> None:
    import accounting.models  # noqa: F401
    import news.models  # noqa: F401
    import users.models  # noqa: F401


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
