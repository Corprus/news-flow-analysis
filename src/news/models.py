from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import UserDefinedType

from db.database import Base


class Vector(UserDefinedType):
    cache_ok = True

    def __init__(self, dimensions: int) -> None:
        self.dimensions = dimensions

    def get_col_spec(self, **kw: Any) -> str:
        return f"vector({self.dimensions})"


class ArticleStatus(StrEnum):
    RAW = "raw"
    QUEUED = "queued"
    PROCESSING = "processing"
    PARSED = "parsed"
    VECTORIZED = "vectorized"
    FAILED = "failed"


class ArticleOrigin(StrEnum):
    USER_SUBMITTED = "user_submitted"
    AUTO_FETCHED = "auto_fetched"


class SearchQueryStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class EventStatus(StrEnum):
    ACTIVE = "active"
    STALE = "stale"
    ARCHIVED = "archived"


class EventArticleRole(StrEnum):
    SEED = "seed"
    RELATED = "related"
    DUPLICATE = "duplicate"
    UPDATE = "update"


class NewsSource(Base):
    __tablename__ = "news_sources"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False, unique=True, index=True)
    site_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    rss_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    language: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    country: Mapped[str | None] = mapped_column(String(8), nullable=True, index=True)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

    articles: Mapped[list[NewsArticle]] = relationship(
        back_populates="source",
        cascade="all, delete-orphan",
    )


class NewsArticle(Base):
    __tablename__ = "news_articles"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    source_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("news_sources.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    submitted_by_user_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("users.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    external_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    canonical_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    title: Mapped[str] = mapped_column(String(1024), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    language: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    author: Mapped[str | None] = mapped_column(String(512), nullable=True)
    image_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    novelty_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=ArticleStatus.RAW.value,
        index=True,
    )
    origin: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=ArticleOrigin.AUTO_FETCHED.value,
        index=True,
    )
    content_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    extra_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        default=dict,
    )

    source: Mapped[NewsSource | None] = relationship(back_populates="articles")
    submissions: Mapped[list[NewsArticleSubmission]] = relationship(
        back_populates="article",
        cascade="all, delete-orphan",
    )
    embeddings: Mapped[list[ArticleEmbedding]] = relationship(
        back_populates="article",
        cascade="all, delete-orphan",
    )
    event_links: Mapped[list[EventArticle]] = relationship(
        back_populates="article",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uq_news_article_source_external_id"),
        UniqueConstraint("canonical_url", name="uq_news_article_canonical_url"),
        UniqueConstraint("content_hash", name="uq_news_article_content_hash"),
        Index("ix_news_articles_published_at", "published_at"),
        Index("ix_news_articles_novelty_score", "novelty_score"),
    )


class ArticleEmbedding(Base):
    __tablename__ = "article_embeddings"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    article_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("news_articles.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    model_name: Mapped[str] = mapped_column(String(256), nullable=False)
    model_revision: Mapped[str] = mapped_column(String(128), nullable=False)
    embedding: Mapped[list[float]] = mapped_column(Vector(768), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

    article: Mapped[NewsArticle] = relationship(back_populates="embeddings")

    __table_args__ = (
        UniqueConstraint(
            "article_id",
            "model_name",
            "model_revision",
            name="uq_article_embedding_model_revision",
        ),
    )


class NewsArticleSubmission(Base):
    __tablename__ = "news_article_submissions"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    article_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("news_articles.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    user_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

    article: Mapped[NewsArticle] = relationship(back_populates="submissions")

    __table_args__ = (
        UniqueConstraint("article_id", "user_id", name="uq_news_article_submission_user"),
        Index("ix_news_article_submissions_created_at", "created_at"),
    )


class NewsEvent(Base):
    __tablename__ = "news_events"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    title: Mapped[str] = mapped_column(String(1024), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=EventStatus.ACTIVE.value,
        index=True,
    )
    language: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    extra_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    article_links: Mapped[list[EventArticle]] = relationship(
        back_populates="event",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_news_events_last_seen_at", "last_seen_at"),
        Index("ix_news_events_started_at", "started_at"),
    )


class NewsSearchQuery(Base):
    __tablename__ = "news_search_queries"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    user_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("users.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    filters: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=SearchQueryStatus.QUEUED.value,
        index=True,
    )
    top_k: Mapped[int] = mapped_column(Integer, nullable=False, default=20)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        Index("ix_news_search_queries_created_at", "created_at"),
    )


class EventArticle(Base):
    __tablename__ = "event_articles"

    event_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("news_events.id", ondelete="CASCADE"),
        primary_key=True,
    )
    article_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("news_articles.id", ondelete="CASCADE"),
        primary_key=True,
    )
    similarity: Mapped[float | None] = mapped_column(nullable=True)
    role: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=EventArticleRole.RELATED.value,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

    event: Mapped[NewsEvent] = relationship(back_populates="article_links")
    article: Mapped[NewsArticle] = relationship(back_populates="event_links")

    __table_args__ = (
        Index("ix_event_articles_article_id", "article_id"),
        Index("ix_event_articles_role", "role"),
    )
