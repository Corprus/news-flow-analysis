from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile, status
from fastapi.exceptions import HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

from accounting.exceptions import InsufficientBalanceError, UserAccountNotFoundError
from accounting.models import TransactionReason
from accounting.service import AccountingService
from db.news_pipeline_jobs import NewsPipelineJobRepository
from messaging.rabbitmq import RabbitPublisher
from news.importers import (
    MAX_IMPORT_FILE_BYTES,
    NewsImportError,
    news_importers,
)
from news.models import (
    ArticleOrigin,
    ArticleStatus,
    ArticleVisibility,
    NewsArticle,
    NewsSearchQuery,
    SearchQueryStatus,
)
from news.service import NewsSearchFilters, NewsService
from settings import Settings, get_settings
from users.deps import CurrentUser, SessionDep, authenticate, ensure_publisher

router = APIRouter(prefix="/v1/news", tags=["news"])
search_router = APIRouter(prefix="/v1/news-search", tags=["news-search"])

CurrentUserDep = Annotated[CurrentUser, Depends(authenticate)]


class AddNewsRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    title: str = Field(min_length=1, max_length=1024)
    content: str = Field(min_length=1)
    url: str | None = Field(default=None, max_length=2048)
    canonical_url: str | None = Field(default=None, max_length=2048)
    summary: str | None = None
    language: str | None = Field(default=None, max_length=16)
    topic: str | None = Field(default=None, max_length=256)
    published_at: datetime
    publish_immediately: bool = False

    @field_validator("published_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("published_at must include timezone information")
        return value


class NewsArticleResponse(BaseModel):
    article_id: UUID
    visibility: ArticleVisibility
    status: ArticleStatus
    job_id: UUID | None = None


class NewsImportFormatResponse(BaseModel):
    id: str
    label: str
    file_extensions: list[str]
    media_types: list[str]


class NewsImportResponse(BaseModel):
    format: str
    total_rows: int
    created_count: int
    duplicate_count: int
    article_ids: list[UUID]
    published_count: int
    job_id: UUID | None = None


class PublishNewsBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    article_ids: list[UUID] = Field(min_length=1, max_length=10_000)


class PublishNewsBatchResponse(BaseModel):
    article_ids: list[UUID]
    published_count: int
    status: Literal[ArticleStatus.PENDING]
    job_id: UUID


class NewsArticlePublishResponse(BaseModel):
    article_id: UUID
    visibility: Literal[ArticleVisibility.PUBLIC]
    status: Literal[ArticleStatus.PENDING]
    job_id: UUID


class NewsArticleHistoryItem(BaseModel):
    article_id: UUID
    title: str
    visibility: ArticleVisibility
    status: ArticleStatus
    origin: ArticleOrigin
    language: str | None
    novelty_score: float | None
    cluster_id: str | None
    novelty_label: Literal["significant", "minor", "duplicate"] | None
    assignment_needs_review: bool | None
    novelty_needs_review: bool | None
    late_arrival: bool | None
    processed_at: datetime | None
    pipeline_error: dict | None
    published_at: datetime
    fetched_at: datetime
    url: str | None


class NewsSearchRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    query_text: str = Field(min_length=1)
    top_k: int = Field(default=20, ge=1, le=100)
    language: str | None = Field(default=None, max_length=16)
    source_id: UUID | None = None
    published_from: datetime | None = None
    published_to: datetime | None = None
    min_novelty_score: float | None = Field(default=None, ge=0, le=1)
    min_relevance: float = Field(default=0.5, ge=0, le=1)

    @field_validator("published_from", "published_to")
    @classmethod
    def require_filter_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("published date filters must include timezone information")
        return value

    @field_validator("published_to")
    @classmethod
    def require_valid_date_range(
        cls,
        value: datetime | None,
        info,
    ) -> datetime | None:
        published_from = info.data.get("published_from")
        if value is not None and published_from is not None and value < published_from:
            raise ValueError("published_to must not be earlier than published_from")
        return value


class NewsSearchResponse(BaseModel):
    query_id: UUID
    status: SearchQueryStatus
    job_id: UUID


class NewsSearchHistoryItem(BaseModel):
    query_id: UUID
    query_text: str
    status: SearchQueryStatus
    filters: dict[str, str]
    top_k: int
    result: dict | None
    error: str | None
    created_at: datetime
    updated_at: datetime


def get_news_service(session: SessionDep) -> NewsService:
    return NewsService(session)


def get_accounting_service(session: SessionDep) -> AccountingService:
    return AccountingService(session)


def get_publisher(request: Request) -> RabbitPublisher:
    return request.app.state.publisher


def get_job_repository(request: Request) -> NewsPipelineJobRepository:
    return request.app.state.repository


async def enqueue_vectorization_job(
    *,
    repository: NewsPipelineJobRepository,
    publisher: RabbitPublisher,
    payload: dict,
) -> UUID:
    job_id = uuid4()
    await repository.mark_queued(str(job_id), payload)
    await publisher.publish(
        {
            "job_id": str(job_id),
            "type": (
                "news_search"
                if payload.get("target_type") == "news_search_query"
                else "news_pipeline"
            ),
            "payload": payload,
        }
    )
    return job_id


@router.post("", response_model=NewsArticleResponse, status_code=status.HTTP_201_CREATED)
async def add_news(
    request: AddNewsRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    settings: Annotated[Settings, Depends(get_settings)],
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsPipelineJobRepository, Depends(get_job_repository)],
) -> NewsArticleResponse:
    ensure_publisher(current_user)
    try:
        article = news.add_user_article(
            user_id=current_user.id,
            title=request.title,
            content=request.content,
            url=request.url,
            canonical_url=request.canonical_url,
            summary=request.summary,
            language=request.language,
            topic=request.topic,
            published_at=request.published_at,
        )
        published: list[NewsArticle] = []
        if request.publish_immediately:
            published = news.publish_user_articles(
                [UUID(article.id)],
                current_user.id,
                allow_already_public=True,
            )
            _withdraw_for_articles_or_raise(
                accounting=accounting,
                user_id=current_user.id,
                amount_per_article=settings.news_add_cost,
                articles=published,
                batch_id=uuid4() if len(published) > 1 else None,
            )
        news.commit()
    except ValueError as exc:
        news.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception:
        news.rollback()
        raise
    job_id = await _enqueue_articles_if_any(
        repository=repository,
        publisher=publisher,
        articles=published,
    )
    return NewsArticleResponse(
        article_id=UUID(article.id),
        visibility=article.visibility,
        status=article.status,
        job_id=job_id,
    )


@router.get("/import-formats", response_model=list[NewsImportFormatResponse])
def get_news_import_formats(
    current_user: CurrentUserDep,
) -> list[NewsImportFormatResponse]:
    ensure_publisher(current_user)
    return [
        NewsImportFormatResponse(
            id=item.id,
            label=item.label,
            file_extensions=list(item.file_extensions),
            media_types=list(item.media_types),
        )
        for item in news_importers.list_formats()
    ]


@router.post(
    "/import",
    response_model=NewsImportResponse,
    status_code=status.HTTP_201_CREATED,
)
async def import_news(
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    file: Annotated[UploadFile, File()],
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    settings: Annotated[Settings, Depends(get_settings)],
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsPipelineJobRepository, Depends(get_job_repository)],
    import_format: Annotated[str, Form(alias="format")],
    publish_immediately: Annotated[bool, Form()] = False,
) -> NewsImportResponse:
    ensure_publisher(current_user)
    content = await file.read(MAX_IMPORT_FILE_BYTES + 1)
    if len(content) > MAX_IMPORT_FILE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Uploaded news file exceeds the 50 MiB limit",
        )
    try:
        articles = news_importers.parse(import_format, content)
    except NewsImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    try:
        result = news.import_user_articles(
            user_id=current_user.id,
            format_id=import_format,
            articles=articles,
        )
        published: list[NewsArticle] = []
        if publish_immediately:
            published = news.publish_user_articles(
                [UUID(article_id) for article_id in result.article_ids],
                current_user.id,
                allow_already_public=True,
            )
            _withdraw_for_articles_or_raise(
                accounting=accounting,
                user_id=current_user.id,
                amount_per_article=settings.news_add_cost,
                articles=published,
            )
        news.commit()
    except ValueError as exc:
        news.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception:
        news.rollback()
        raise
    job_id = await _enqueue_articles_if_any(
        repository=repository,
        publisher=publisher,
        articles=published,
    )
    return NewsImportResponse(
        format=import_format,
        total_rows=result.total_rows,
        created_count=result.created_count,
        duplicate_count=result.duplicate_count,
        article_ids=[UUID(article_id) for article_id in result.article_ids],
        published_count=len(published),
        job_id=job_id,
    )


@router.post(
    "/publish",
    response_model=PublishNewsBatchResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def publish_news_batch(
    request: PublishNewsBatchRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    settings: Annotated[Settings, Depends(get_settings)],
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsPipelineJobRepository, Depends(get_job_repository)],
) -> PublishNewsBatchResponse:
    ensure_publisher(current_user)
    try:
        articles = news.publish_user_articles(
            request.article_ids,
            current_user.id,
            allow_already_public=False,
        )
        _withdraw_for_articles_or_raise(
            accounting=accounting,
            user_id=current_user.id,
            amount_per_article=settings.news_add_cost,
            articles=articles,
            batch_id=uuid4() if len(articles) > 1 else None,
        )
        news.commit()
    except LookupError as exc:
        news.rollback()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        news.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception:
        news.rollback()
        raise
    job_id = await _enqueue_articles_if_any(
        repository=repository,
        publisher=publisher,
        articles=articles,
    )
    assert job_id is not None
    return PublishNewsBatchResponse(
        article_ids=[UUID(article.id) for article in articles],
        published_count=len(articles),
        status=ArticleStatus.PENDING,
        job_id=job_id,
    )


@router.post(
    "/{article_id}/publish",
    response_model=NewsArticlePublishResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def publish_news(
    article_id: UUID,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    settings: Annotated[Settings, Depends(get_settings)],
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsPipelineJobRepository, Depends(get_job_repository)],
) -> NewsArticlePublishResponse:
    ensure_publisher(current_user)
    try:
        article = news.publish_user_article(article_id, current_user.id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    if article is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Draft not found")
    _withdraw_or_raise(
        accounting=accounting,
        user_id=current_user.id,
        amount=settings.news_add_cost,
        reason=TransactionReason.NEWS_ADD,
        reference_id=article_id,
    )
    news.commit()
    job_id = await enqueue_vectorization_job(
        repository=repository,
        publisher=publisher,
        payload=_article_vectorization_payload(article),
    )
    return NewsArticlePublishResponse(
        article_id=article_id,
        visibility=ArticleVisibility.PUBLIC,
        status=ArticleStatus.PENDING,
        job_id=job_id,
    )


@router.get("/me/history", response_model=list[NewsArticleHistoryItem])
def get_my_news_history(
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[NewsArticleHistoryItem]:
    articles = news.list_user_articles(current_user.id, limit, offset)
    return [_article_history_item(article) for article in articles]


@search_router.post("", response_model=NewsSearchResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_news_search(
    request: NewsSearchRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    settings: Annotated[Settings, Depends(get_settings)],
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsPipelineJobRepository, Depends(get_job_repository)],
) -> NewsSearchResponse:
    filters = NewsSearchFilters(
        language=request.language,
        source_id=request.source_id,
        published_from=request.published_from,
        published_to=request.published_to,
        min_novelty_score=request.min_novelty_score,
        min_relevance=request.min_relevance,
    )
    search_query = news.create_search_query(
        user_id=current_user.id,
        query_text=request.query_text,
        filters=filters,
        top_k=request.top_k,
    )
    _withdraw_or_raise(
        accounting=accounting,
        user_id=current_user.id,
        amount=settings.news_search_cost,
        reason=TransactionReason.NEWS_SEARCH,
        reference_id=UUID(search_query.id),
    )
    news.commit()
    job_id = await enqueue_vectorization_job(
        repository=repository,
        publisher=publisher,
        payload=_search_vectorization_payload(search_query),
    )
    return NewsSearchResponse(
        query_id=UUID(search_query.id),
        status=search_query.status,
        job_id=job_id,
    )


@search_router.get("/history", response_model=list[NewsSearchHistoryItem])
def get_my_search_history(
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[NewsSearchHistoryItem]:
    return [
        _search_history_item(search_query)
        for search_query in news.list_search_queries(current_user.id, limit, offset)
    ]


def _article_vectorization_payload(article: NewsArticle) -> dict:
    return {
        "news_ids": [article.id],
        "mode": "incremental",
    }


def _articles_vectorization_payload(articles: Iterable[NewsArticle]) -> dict:
    return {
        "news_ids": [article.id for article in articles],
        "mode": "incremental",
    }


async def _enqueue_articles_if_any(
    *,
    repository: NewsPipelineJobRepository,
    publisher: RabbitPublisher,
    articles: list[NewsArticle],
) -> UUID | None:
    if not articles:
        return None
    return await enqueue_vectorization_job(
        repository=repository,
        publisher=publisher,
        payload=_articles_vectorization_payload(articles),
    )


def _search_vectorization_payload(search_query: NewsSearchQuery) -> dict:
    return {
        "target_type": "news_search_query",
        "query_id": search_query.id,
        "text": search_query.query_text,
        "filters": search_query.filters,
        "top_k": search_query.top_k,
    }


def _article_history_item(article: NewsArticle) -> NewsArticleHistoryItem:
    pipeline_state = article.pipeline_state
    return NewsArticleHistoryItem(
        article_id=UUID(article.id),
        title=article.title,
        visibility=article.visibility,
        status=article.status,
        origin=article.origin,
        language=article.language,
        novelty_score=article.novelty_score,
        cluster_id=pipeline_state.cluster_id if pipeline_state else None,
        novelty_label=pipeline_state.novelty_label if pipeline_state else None,
        assignment_needs_review=(
            pipeline_state.assignment_needs_review if pipeline_state else None
        ),
        novelty_needs_review=(
            pipeline_state.novelty_needs_review if pipeline_state else None
        ),
        late_arrival=pipeline_state.late_arrival if pipeline_state else None,
        processed_at=pipeline_state.processed_at if pipeline_state else None,
        pipeline_error=article.extra_metadata.get("pipeline_error"),
        published_at=article.published_at,
        fetched_at=article.fetched_at,
        url=article.url,
    )


def _search_history_item(search_query: NewsSearchQuery) -> NewsSearchHistoryItem:
    return NewsSearchHistoryItem(
        query_id=UUID(search_query.id),
        query_text=search_query.query_text,
        status=search_query.status,
        filters=search_query.filters,
        top_k=search_query.top_k,
        result=search_query.result,
        error=search_query.error,
        created_at=search_query.created_at,
        updated_at=search_query.updated_at,
    )


def _withdraw_or_raise(
    *,
    accounting: AccountingService,
    user_id: UUID,
    amount,
    reason: TransactionReason,
    reference_id: UUID,
    batch_id: UUID | None = None,
) -> None:
    if amount == 0:
        return

    try:
        accounting.withdraw_credit(user_id, amount, reason, reference_id, batch_id)
    except InsufficientBalanceError as exc:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="Insufficient credits",
        ) from exc
    except UserAccountNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User account not found",
        ) from exc


def _withdraw_for_articles_or_raise(
    *,
    accounting: AccountingService,
    user_id: UUID,
    amount_per_article,
    articles: Iterable[NewsArticle],
    batch_id: UUID | None = None,
) -> None:
    for article in articles:
        _withdraw_or_raise(
            accounting=accounting,
            user_id=user_id,
            amount=amount_per_article,
            reason=TransactionReason.NEWS_ADD,
            reference_id=UUID(article.id),
            batch_id=batch_id,
        )
