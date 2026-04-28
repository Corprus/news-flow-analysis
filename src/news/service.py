from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from news.models import (
    ArticleOrigin,
    ArticleStatus,
    NewsArticle,
    NewsSearchQuery,
    SearchQueryStatus,
)


@dataclass(frozen=True)
class NewsSearchFilters:
    language: str | None = None
    source_id: UUID | None = None
    published_from: datetime | None = None
    published_to: datetime | None = None
    submitted_by_user_id: UUID | None = None
    min_novelty_score: float | None = None

    def to_payload(self) -> dict[str, str]:
        payload: dict[str, str] = {}
        if self.language is not None:
            payload["language"] = self.language
        if self.source_id is not None:
            payload["source_id"] = str(self.source_id)
        if self.published_from is not None:
            payload["published_from"] = self.published_from.isoformat()
        if self.published_to is not None:
            payload["published_to"] = self.published_to.isoformat()
        if self.submitted_by_user_id is not None:
            payload["submitted_by_user_id"] = str(self.submitted_by_user_id)
        if self.min_novelty_score is not None:
            payload["min_novelty_score"] = str(self.min_novelty_score)
        return payload


class NewsService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_user_article(
        self,
        *,
        user_id: UUID,
        title: str,
        content: str,
        url: str | None = None,
        canonical_url: str | None = None,
        summary: str | None = None,
        language: str | None = None,
        published_at: datetime | None = None,
    ) -> NewsArticle:
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        existing_article = self._find_existing_article(canonical_url, content_hash)
        if existing_article is not None:
            return existing_article

        article = NewsArticle(
            submitted_by_user_id=str(user_id),
            title=title,
            content=content,
            summary=summary,
            url=url,
            canonical_url=canonical_url,
            published_at=published_at,
            language=language,
            status=ArticleStatus.QUEUED.value,
            origin=ArticleOrigin.USER_SUBMITTED.value,
            content_hash=content_hash,
        )
        self._session.add(article)
        self._session.flush()
        return article

    def create_search_query(
        self,
        *,
        user_id: UUID,
        query_text: str,
        filters: NewsSearchFilters,
        top_k: int,
    ) -> NewsSearchQuery:
        search_query = NewsSearchQuery(
            user_id=str(user_id),
            query_text=query_text,
            filters=filters.to_payload(),
            status=SearchQueryStatus.QUEUED.value,
            top_k=top_k,
        )
        self._session.add(search_query)
        self._session.flush()
        return search_query

    def list_user_articles(
        self,
        user_id: UUID,
        limit: int = 50,
        offset: int = 0,
    ) -> list[NewsArticle]:
        statement = (
            select(NewsArticle)
            .where(NewsArticle.submitted_by_user_id == str(user_id))
            .order_by(NewsArticle.fetched_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(self._session.execute(statement).scalars().all())

    def list_search_queries(
        self,
        user_id: UUID,
        limit: int = 50,
        offset: int = 0,
    ) -> list[NewsSearchQuery]:
        statement = (
            select(NewsSearchQuery)
            .where(NewsSearchQuery.user_id == str(user_id))
            .order_by(NewsSearchQuery.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(self._session.execute(statement).scalars().all())

    def commit(self) -> None:
        self._session.commit()

    def _find_existing_article(
        self,
        canonical_url: str | None,
        content_hash: str,
    ) -> NewsArticle | None:
        if canonical_url:
            article_by_url = self._session.execute(
                select(NewsArticle).where(NewsArticle.canonical_url == canonical_url)
            ).scalars().first()
            if article_by_url is not None:
                return article_by_url

        return self._session.execute(
            select(NewsArticle).where(NewsArticle.content_hash == content_hash)
        ).scalars().first()
