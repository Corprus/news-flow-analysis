from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from news.importers import ImportedNews
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
    min_relevance: float = 0.5

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
        payload["min_relevance"] = str(self.min_relevance)
        return payload


@dataclass(frozen=True)
class NewsImportResult:
    total_rows: int
    created_count: int
    duplicate_count: int
    article_ids: list[str]


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
            if existing_article.visibility == ArticleVisibility.PUBLIC.value:
                raise ValueError(
                    "Такая новость уже существует и была опубликована"
                )
            existing_article.title = title
            existing_article.content = content
            existing_article.summary = summary
            existing_article.url = url
            existing_article.canonical_url = canonical_url
            existing_article.published_at = published_at
            existing_article.language = language
            existing_article.topic = topic
            existing_article.content_hash = content_hash
            self._add_submission(existing_article.id, user_id)
            self._session.flush()
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

    def import_user_articles(
        self,
        *,
        user_id: UUID,
        format_id: str,
        articles: Iterable[ImportedNews],
    ) -> NewsImportResult:
        article_ids: list[str] = []
        created_count = 0
        total_rows = 0
        for imported in articles:
            total_rows += 1
            content_hash = hashlib.sha256(imported.content.encode()).hexdigest()
            existing_article = self._find_existing_article(
                imported.url,
                content_hash,
                user_id,
            )
            if existing_article is not None:
                self._add_submission(existing_article.id, user_id)
                article_ids.append(existing_article.id)
                continue

            metadata = {
                "import": {
                    "format": format_id,
                    **(imported.metadata or {}),
                }
            }
            article = NewsArticle(
                submitted_by_user_id=str(user_id),
                external_id=imported.external_id,
                title=imported.title,
                content=imported.content,
                url=imported.url,
                canonical_url=imported.url,
                published_at=imported.published_at,
                language=imported.language,
                topic=imported.topic,
                visibility=ArticleVisibility.DRAFT.value,
                status=ArticleStatus.NOT_STARTED.value,
                origin=ArticleOrigin.USER_SUBMITTED.value,
                content_hash=content_hash,
                extra_metadata=metadata,
            )
            self._session.add(article)
            self._session.flush()
            self._add_submission(article.id, user_id)
            article_ids.append(article.id)
            created_count += 1

        return NewsImportResult(
            total_rows=total_rows,
            created_count=created_count,
            duplicate_count=total_rows - created_count,
            article_ids=article_ids,
        )

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

    def publish_user_articles(
        self,
        article_ids: Iterable[UUID],
        user_id: UUID,
        *,
        allow_already_public: bool = False,
    ) -> list[NewsArticle]:
        unique_ids = list(dict.fromkeys(str(article_id) for article_id in article_ids))
        if not unique_ids:
            return []
        articles = list(
            self._session.execute(
                select(NewsArticle)
                .join(
                    NewsArticleSubmission,
                    NewsArticleSubmission.article_id == NewsArticle.id,
                )
                .where(
                    NewsArticle.id.in_(unique_ids),
                    NewsArticleSubmission.user_id == str(user_id),
                )
                .with_for_update()
            )
            .scalars()
            .all()
        )
        article_by_id = {article.id: article for article in articles}
        missing = [article_id for article_id in unique_ids if article_id not in article_by_id]
        if missing:
            raise LookupError(f"Draft articles not found: {missing[:10]}")

        invalid: list[str] = []
        for article_id in unique_ids:
            article = article_by_id[article_id]
            if article.visibility == ArticleVisibility.PUBLIC.value:
                if not allow_already_public:
                    invalid.append(f"{article.id}: already public")
                continue
            if article.visibility != ArticleVisibility.DRAFT.value:
                invalid.append(f"{article.id}: not a draft")
                continue
            if article.status != ArticleStatus.NOT_STARTED.value:
                invalid.append(f"{article.id}: already entered processing")
        if invalid:
            raise ValueError(
                "All articles must be publishable drafts: " + "; ".join(invalid[:10])
            )

        published: list[NewsArticle] = []
        for article_id in unique_ids:
            article = article_by_id[article_id]
            if article.visibility == ArticleVisibility.PUBLIC.value:
                continue
            article.visibility = ArticleVisibility.PUBLIC.value
            article.status = ArticleStatus.PENDING.value
            published.append(article)
        self._session.flush()
        return published

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

    def rollback(self) -> None:
        self._session.rollback()

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
