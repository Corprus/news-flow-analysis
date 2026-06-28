from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.exceptions import HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

from accounting.exceptions import InsufficientBalanceError, UserAccountNotFoundError
from accounting.models import TransactionReason
from accounting.service import AccountingService
from db.database import get_session
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
from news.search_results import group_search_items
from news.service import NewsSearchFilters, NewsService
from settings import Settings, get_settings
from users.deps import CurrentUser, SessionDep, authenticate, ensure_publisher
from users.models import UserRole

router = APIRouter(prefix="/news", tags=["news"])
search_router = APIRouter(prefix="/news-search", tags=["news-search"])
MAX_BATCH_ARTICLES = 50_000

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


class NewsImportJobResponse(BaseModel):
    import_job_id: UUID
    status: Literal["queued"]


class NewsImportJobStatus(BaseModel):
    import_job_id: UUID
    status: Literal["queued", "processing", "done", "failed"]
    request: dict[str, object]
    result: dict[str, object] | None
    created_at: datetime
    updated_at: datetime


class PublishNewsBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    article_ids: list[UUID] = Field(min_length=1, max_length=MAX_BATCH_ARTICLES)


class PublishNewsBatchResponse(BaseModel):
    article_ids: list[UUID]
    published_count: int
    status: Literal[ArticleStatus.PENDING]
    job_id: UUID


class DeleteNewsBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    article_ids: list[UUID] = Field(min_length=1, max_length=MAX_BATCH_ARTICLES)


class DeleteNewsBatchResponse(BaseModel):
    deleted_count: int


class ChangeNewsVisibilityResponse(BaseModel):
    updated_count: int


class NoveltyLabelUpdate(BaseModel):
    article_id: UUID
    label: Literal["significant", "minor", "duplicate"] | None


class UpdateNoveltyLabelsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    updates: list[NoveltyLabelUpdate] = Field(
        min_length=1,
        max_length=MAX_BATCH_ARTICLES,
    )


class UpdateNoveltyLabelsResponse(BaseModel):
    updated_count: int


class ReprocessNewsResponse(BaseModel):
    article_ids: list[UUID]
    queued_count: int
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
    model_novelty_label: Literal["significant", "minor", "duplicate"] | None
    manual_novelty_label: Literal["significant", "minor", "duplicate"] | None
    manual_novelty_updated_at: datetime | None
    assignment_needs_review: bool | None
    novelty_needs_review: bool | None
    late_arrival: bool | None
    processed_at: datetime | None
    pipeline_error: dict | None
    possible_duplicate: bool
    published_at: datetime
    fetched_at: datetime
    url: str | None


class NewsFeedResponse(BaseModel):
    clusters: list[dict]
    items: list[dict]
    total: int
    total_clusters: int
    limit: int
    offset: int


class AdjacentNewsDatesResponse(BaseModel):
    previous_date: datetime | None
    next_date: datetime | None


class LatestNewsDateResponse(BaseModel):
    latest_date: datetime | None


class NewsSearchRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    query_text: str = Field(min_length=1)
    top_k: int = Field(default=20, ge=1, le=100)
    organization_id: UUID | None = None
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
            organization_id=current_user.organization_id,
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
            detail="Файл с новостями превышает допустимый размер 200 МБ",
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
            organization_id=current_user.organization_id,
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
    "/import-jobs",
    response_model=NewsImportJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_news_import_job(
    current_user: CurrentUserDep,
    background_tasks: BackgroundTasks,
    file: Annotated[UploadFile, File()],
    settings: Annotated[Settings, Depends(get_settings)],
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsPipelineJobRepository, Depends(get_job_repository)],
    import_format: Annotated[str, Form(alias="format")],
    publish_immediately: Annotated[bool, Form()] = False,
) -> NewsImportJobResponse:
    ensure_publisher(current_user)
    content = await file.read(MAX_IMPORT_FILE_BYTES + 1)
    if len(content) > MAX_IMPORT_FILE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Uploaded news file exceeds the 200 MB limit",
        )

    import_job_id = uuid4()
    payload = {
        "type": "news_import",
        "format": import_format,
        "file_name": file.filename,
        "publish_immediately": publish_immediately,
        "user_id": str(current_user.id),
        "organization_id": str(current_user.organization_id),
    }
    await repository.mark_queued(str(import_job_id), payload)
    background_tasks.add_task(
        _run_news_import_job,
        str(import_job_id),
        payload,
        content,
        current_user,
        settings,
        publisher,
        repository,
    )
    return NewsImportJobResponse(import_job_id=import_job_id, status="queued")


@router.get("/import-jobs/{import_job_id}", response_model=NewsImportJobStatus)
async def get_news_import_job(
    import_job_id: UUID,
    current_user: CurrentUserDep,
    repository: Annotated[NewsPipelineJobRepository, Depends(get_job_repository)],
) -> NewsImportJobStatus:
    ensure_publisher(current_user)
    job = await repository.get(str(import_job_id))
    if job is None or job["request"].get("type") != "news_import":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Import job not found",
        )
    if (
        current_user.role != UserRole.ADMIN
        and job["request"].get("user_id") != str(current_user.id)
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot access another user's import job",
        )

    return NewsImportJobStatus(
        import_job_id=UUID(job["job_id"]),
        status=job["status"],
        request=job["request"],
        result=job["result"],
        created_at=job["created_at"],
        updated_at=job["updated_at"],
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


@router.delete(
    "",
    response_model=DeleteNewsBatchResponse,
)
def delete_news_drafts(
    request: DeleteNewsBatchRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
) -> DeleteNewsBatchResponse:
    ensure_publisher(current_user)
    try:
        deleted_count = news.delete_user_drafts(
            request.article_ids,
            current_user.id,
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
    return DeleteNewsBatchResponse(deleted_count=deleted_count)


@router.post(
    "/archive",
    response_model=ChangeNewsVisibilityResponse,
)
def archive_news(
    request: DeleteNewsBatchRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
) -> ChangeNewsVisibilityResponse:
    ensure_publisher(current_user)
    return _change_news_visibility(
        news.archive_user_articles,
        request.article_ids,
        current_user.id,
        news,
    )


@router.post(
    "/restore",
    response_model=ChangeNewsVisibilityResponse,
)
def restore_news(
    request: DeleteNewsBatchRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
) -> ChangeNewsVisibilityResponse:
    ensure_publisher(current_user)
    return _change_news_visibility(
        news.restore_user_articles,
        request.article_ids,
        current_user.id,
        news,
    )


@router.post(
    "/moderation-labels",
    response_model=UpdateNoveltyLabelsResponse,
)
def update_novelty_labels(
    request: UpdateNoveltyLabelsRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
) -> UpdateNoveltyLabelsResponse:
    ensure_publisher(current_user)
    labels = {item.article_id: item.label for item in request.updates}
    try:
        updated_count = news.set_user_article_novelty_labels(labels, current_user.id)
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
    return UpdateNoveltyLabelsResponse(updated_count=updated_count)


@router.post(
    "/reprocess",
    response_model=ReprocessNewsResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def reprocess_news(
    request: DeleteNewsBatchRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    settings: Annotated[Settings, Depends(get_settings)],
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsPipelineJobRepository, Depends(get_job_repository)],
) -> ReprocessNewsResponse:
    ensure_publisher(current_user)
    try:
        articles = news.prepare_user_articles_for_reprocessing(
            request.article_ids,
            current_user.id,
        )
        _withdraw_for_articles_or_raise(
            accounting=accounting,
            user_id=current_user.id,
            amount_per_article=settings.news_add_cost,
            articles=articles,
            reason=TransactionReason.NEWS_REPROCESS,
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
    return ReprocessNewsResponse(
        article_ids=[UUID(article.id) for article in articles],
        queued_count=len(articles),
        status=ArticleStatus.PENDING,
        job_id=job_id,
    )


def _change_news_visibility(
    operation,
    article_ids: list[UUID],
    user_id: UUID,
    news: NewsService,
) -> ChangeNewsVisibilityResponse:
    try:
        updated_count = operation(article_ids, user_id)
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
    return ChangeNewsVisibilityResponse(updated_count=updated_count)


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
    limit: Annotated[int, Query(ge=1, le=10_000)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[NewsArticleHistoryItem]:
    articles = news.list_user_articles(current_user.id, limit, offset)
    return [_article_history_item(article) for article in articles]


@router.get("/feed", response_model=NewsFeedResponse)
def get_news_feed(
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    published_from: datetime,
    published_to: datetime,
    organization_id: UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> NewsFeedResponse:
    visible_organization_id = _visible_organization_id(current_user, organization_id)
    for value in (published_from, published_to):
        if value.tzinfo is None or value.utcoffset() is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="published date filters must include timezone information",
            )
    if published_to <= published_from:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="published_to must be later than published_from",
        )

    articles, total = news.list_public_articles_by_period(
        published_from=published_from,
        published_to=published_to,
        organization_id=visible_organization_id,
    )
    items = []
    for rank, article in enumerate(articles, start=1):
        pipeline_state = article.pipeline_state
        novelty_label = (
            pipeline_state.manual_novelty_label or pipeline_state.novelty_label
            if pipeline_state
            else None
        )
        items.append(
            {
                "article_id": article.id,
                "title": article.title,
                "status": article.status,
                "language": article.language,
                "novelty_score": article.novelty_score,
                "published_at": article.published_at.isoformat(),
                "rank": rank,
                "cluster_id": (
                    pipeline_state.cluster_id if pipeline_state else article.id
                ),
                "novelty_label": novelty_label,
                "p_significant": (
                    pipeline_state.p_significant if pipeline_state else None
                ),
                "url": article.url,
                "summary": article.summary,
                "content": article.content,
            }
        )
    cluster_summaries = news.list_cluster_summaries(
        (item["cluster_id"] for item in items),
        organization_id=visible_organization_id,
    )
    all_clusters = group_search_items(
        items,
        top_k=len(items),
        cluster_summaries=cluster_summaries,
    )
    clusters = all_clusters[offset : offset + limit]
    selected_cluster_ids = {cluster["cluster_id"] for cluster in clusters}
    selected_items = [
        item for item in items if item["cluster_id"] in selected_cluster_ids
    ]
    return NewsFeedResponse(
        clusters=clusters,
        items=selected_items,
        total=total,
        total_clusters=len(all_clusters),
        limit=limit,
        offset=offset,
    )


@router.get("/feed/adjacent-dates", response_model=AdjacentNewsDatesResponse)
def get_adjacent_news_dates(
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    published_from: datetime,
    published_to: datetime,
    organization_id: UUID | None = None,
) -> AdjacentNewsDatesResponse:
    visible_organization_id = _visible_organization_id(current_user, organization_id)
    for value in (published_from, published_to):
        if value.tzinfo is None or value.utcoffset() is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="published date filters must include timezone information",
            )
    if published_to <= published_from:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="published_to must be later than published_from",
        )
    previous_date, next_date = news.get_adjacent_public_article_dates(
        published_from=published_from,
        published_to=published_to,
        organization_id=visible_organization_id,
    )
    return AdjacentNewsDatesResponse(
        previous_date=previous_date,
        next_date=next_date,
    )


@router.get("/feed/latest-date", response_model=LatestNewsDateResponse)
def get_latest_news_date(
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    organization_id: UUID | None = None,
) -> LatestNewsDateResponse:
    visible_organization_id = _visible_organization_id(current_user, organization_id)
    return LatestNewsDateResponse(
        latest_date=news.get_latest_public_article_date(visible_organization_id),
    )


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
    visible_organization_id = _visible_organization_id(
        current_user,
        request.organization_id,
    )
    filters = NewsSearchFilters(
        organization_id=visible_organization_id,
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
        "organization_id": article.organization_id,
        "mode": "incremental",
    }


def _articles_vectorization_payload(articles: Iterable[NewsArticle]) -> dict:
    article_list = list(articles)
    organization_ids = {
        str(article.organization_id)
        for article in article_list
        if article.organization_id is not None
    }
    if len(organization_ids) != 1:
        raise ValueError("All articles in a pipeline job must belong to one organization")
    return {
        "news_ids": [article.id for article in article_list],
        "organization_id": organization_ids.pop(),
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


async def _run_news_import_job(
    import_job_id: str,
    payload: dict,
    content: bytes,
    current_user: CurrentUser,
    settings: Settings,
    publisher: RabbitPublisher,
    repository: NewsPipelineJobRepository,
) -> None:
    await repository.mark_processing(import_job_id, payload)
    try:
        result = await asyncio.to_thread(
            _import_news_content_in_new_session,
            content,
            current_user,
            settings,
            payload["format"],
            payload["publish_immediately"],
        )
        if result.published_count:
            result.job_id = await enqueue_vectorization_job(
                repository=repository,
                publisher=publisher,
                payload={
                    "news_ids": [str(article_id) for article_id in result.article_ids],
                    "organization_id": str(current_user.organization_id),
                    "mode": "incremental",
                },
            )
        await repository.mark_done(import_job_id, result.model_dump(mode="json"))
    except Exception as exc:
        await repository.mark_failed(import_job_id, str(exc))


def _import_news_content_in_new_session(
    content: bytes,
    current_user: CurrentUser,
    settings: Settings,
    import_format: str,
    publish_immediately: bool,
) -> NewsImportResponse:
    with get_session() as session:
        news = NewsService(session)
        accounting = AccountingService(session)
        try:
            articles = news_importers.parse(import_format, content)
            result = news.import_user_articles(
                user_id=current_user.id,
                organization_id=current_user.organization_id,
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
        except Exception:
            news.rollback()
            raise

    return NewsImportResponse(
        format=import_format,
        total_rows=result.total_rows,
        created_count=result.created_count,
        duplicate_count=result.duplicate_count,
        article_ids=[UUID(article_id) for article_id in result.article_ids],
        published_count=len(published),
        job_id=None,
    )


def _search_vectorization_payload(search_query: NewsSearchQuery) -> dict:
    return {
        "target_type": "news_search_query",
        "query_id": search_query.id,
        "organization_id": search_query.filters.get("organization_id"),
        "text": search_query.query_text,
        "filters": search_query.filters,
        "top_k": search_query.top_k,
    }


def _visible_organization_id(
    current_user: CurrentUser,
    requested_organization_id: UUID | None,
) -> UUID | None:
    if current_user.role == UserRole.ADMIN:
        return requested_organization_id
    if (
        requested_organization_id is not None
        and requested_organization_id != current_user.organization_id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot access another organization's news",
        )
    return current_user.organization_id


def _article_history_item(article: NewsArticle) -> NewsArticleHistoryItem:
    pipeline_state = article.pipeline_state
    import_metadata = article.extra_metadata.get("import") or {}
    effective_novelty_label = (
        pipeline_state.manual_novelty_label or pipeline_state.novelty_label
        if pipeline_state
        else None
    )
    return NewsArticleHistoryItem(
        article_id=UUID(article.id),
        title=article.title,
        visibility=article.visibility,
        status=article.status,
        origin=article.origin,
        language=article.language,
        novelty_score=article.novelty_score,
        cluster_id=pipeline_state.cluster_id if pipeline_state else None,
        novelty_label=effective_novelty_label,
        model_novelty_label=pipeline_state.novelty_label if pipeline_state else None,
        manual_novelty_label=(
            pipeline_state.manual_novelty_label if pipeline_state else None
        ),
        manual_novelty_updated_at=(
            pipeline_state.manual_novelty_updated_at if pipeline_state else None
        ),
        assignment_needs_review=(
            pipeline_state.assignment_needs_review if pipeline_state else None
        ),
        novelty_needs_review=(
            pipeline_state.novelty_needs_review if pipeline_state else None
        ),
        late_arrival=pipeline_state.late_arrival if pipeline_state else None,
        processed_at=pipeline_state.processed_at if pipeline_state else None,
        pipeline_error=article.extra_metadata.get("pipeline_error"),
        possible_duplicate=bool(import_metadata.get("possible_duplicate_of")),
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
    reason: TransactionReason = TransactionReason.NEWS_ADD,
    batch_id: UUID | None = None,
) -> None:
    for article in articles:
        _withdraw_or_raise(
            accounting=accounting,
            user_id=user_id,
            amount=amount_per_article,
            reason=reason,
            reference_id=UUID(article.id),
            batch_id=batch_id,
        )
