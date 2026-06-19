from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from news.models import (
    ArticleOrigin,
    ArticleStatus,
    ArticleVisibility,
    NewsArticle,
    NewsArticleSubmission,
    NewsSearchQuery,
    SearchQueryStatus,
)


@dataclass(frozen=True)
class NewsSearchFilters:
    language: str | None = None
    source_id: UUID | None = None
    published_from: datetime | None = None
    published_to: datetime | None = None
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
        published_at: datetime,
        url: str | None = None,
        canonical_url: str | None = None,
        summary: str | None = None,
        language: str | None = None,
        topic: str | None = None,
    ) -> NewsArticle:
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        existing_article = self._find_existing_article(
            canonical_url,
            content_hash,
            user_id,
        )
        if existing_article is not None:
            self._add_submission(existing_article.id, user_id)
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
            topic=topic,
            visibility=ArticleVisibility.DRAFT.value,
            status=ArticleStatus.NOT_STARTED.value,
            origin=ArticleOrigin.USER_SUBMITTED.value,
            content_hash=content_hash,
        )
        self._session.add(article)
        self._session.flush()
        self._add_submission(article.id, user_id)
        return article

    def publish_user_article(self, article_id: UUID, user_id: UUID) -> NewsArticle | None:
        article = (
            self._session.execute(
                select(NewsArticle)
                .join(
                    NewsArticleSubmission,
                    NewsArticleSubmission.article_id == NewsArticle.id,
                )
                .where(
                    NewsArticle.id == str(article_id),
                    NewsArticleSubmission.user_id == str(user_id),
                )
            )
            .scalars()
            .first()
        )
        if article is None:
            return None
        if article.visibility != ArticleVisibility.DRAFT.value:
            raise ValueError("Only draft articles can be published")
        if article.status != ArticleStatus.NOT_STARTED.value:
            raise ValueError("Draft article has already entered processing")
        article.visibility = ArticleVisibility.PUBLIC.value
        article.status = ArticleStatus.PENDING.value
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
            .options(selectinload(NewsArticle.pipeline_state))
            .join(NewsArticleSubmission, NewsArticleSubmission.article_id == NewsArticle.id)
            .where(NewsArticleSubmission.user_id == str(user_id))
            .order_by(NewsArticleSubmission.created_at.desc())
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
        user_id: UUID,
    ) -> NewsArticle | None:
        allowed = or_(
            NewsArticle.visibility == ArticleVisibility.PUBLIC.value,
            NewsArticle.submitted_by_user_id == str(user_id),
        )
        if canonical_url:
            article_by_url = (
                self._session.execute(
                    select(NewsArticle).where(
                        NewsArticle.canonical_url == canonical_url,
                        allowed,
                    )
                )
                .scalars()
                .first()
            )
            if article_by_url is not None:
                return article_by_url

        return (
            self._session.execute(
                select(NewsArticle).where(
                    NewsArticle.content_hash == content_hash,
                    allowed,
                )
            )
            .scalars()
            .first()
        )

    def _add_submission(self, article_id: str, user_id: UUID) -> None:
        existing_submission = (
            self._session.execute(
                select(NewsArticleSubmission).where(
                    NewsArticleSubmission.article_id == article_id,
                    NewsArticleSubmission.user_id == str(user_id),
                )
            )
            .scalars()
            .first()
        )
        if existing_submission is not None:
            return

        self._session.add(
            NewsArticleSubmission(
                article_id=article_id,
                user_id=str(user_id),
            )
        )
        self._session.flush()
