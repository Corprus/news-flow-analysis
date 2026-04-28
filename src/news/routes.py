from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.exceptions import HTTPException
from pydantic import BaseModel, Field

from accounting.exceptions import InsufficientBalanceError, UserAccountNotFoundError
from accounting.models import TransactionReason
from accounting.service import AccountingService
from db.news_vectorization_jobs import NewsVectorizationJobRepository
from messaging.rabbitmq import RabbitPublisher
from news.models import NewsArticle, NewsSearchQuery
from news.service import NewsSearchFilters, NewsService
from settings import Settings, get_settings
from users.deps import CurrentUser, SessionDep, authenticate

router = APIRouter(prefix="/v1/news", tags=["news"])
search_router = APIRouter(prefix="/v1/news-search", tags=["news-search"])

CurrentUserDep = Annotated[CurrentUser, Depends(authenticate)]


class AddNewsRequest(BaseModel):
    title: str = Field(min_length=1, max_length=1024)
    content: str = Field(min_length=1)
    url: str | None = Field(default=None, max_length=2048)
    canonical_url: str | None = Field(default=None, max_length=2048)
    summary: str | None = None
    language: str | None = Field(default=None, max_length=16)
    published_at: datetime | None = None


class NewsArticleResponse(BaseModel):
    article_id: UUID
    status: str
    vectorization_job_id: UUID


class NewsArticleHistoryItem(BaseModel):
    article_id: UUID
    title: str
    status: str
    origin: str
    language: str | None
    novelty_score: float | None
    published_at: datetime | None
    fetched_at: datetime
    url: str | None


class NewsSearchRequest(BaseModel):
    query_text: str = Field(min_length=1)
    top_k: int = Field(default=20, ge=1, le=100)
    language: str | None = Field(default=None, max_length=16)
    source_id: UUID | None = None
    published_from: datetime | None = None
    published_to: datetime | None = None
    submitted_by_user_id: UUID | None = None
    min_novelty_score: float | None = Field(default=None, ge=0, le=1)


class NewsSearchResponse(BaseModel):
    query_id: UUID
    status: str
    vectorization_job_id: UUID


class NewsSearchHistoryItem(BaseModel):
    query_id: UUID
    query_text: str
    status: str
    filters: dict
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


def get_job_repository(request: Request) -> NewsVectorizationJobRepository:
    return request.app.state.repository


async def enqueue_vectorization_job(
    *,
    repository: NewsVectorizationJobRepository,
    publisher: RabbitPublisher,
    payload: dict,
) -> UUID:
    job_id = uuid4()
    await repository.mark_queued(str(job_id), payload)
    await publisher.publish(
        {
            "job_id": str(job_id),
            "type": "news_vectorization",
            "payload": payload,
        }
    )
    return job_id


@router.post("", response_model=NewsArticleResponse, status_code=status.HTTP_202_ACCEPTED)
async def add_news(
    request: AddNewsRequest,
    current_user: CurrentUserDep,
    news: Annotated[NewsService, Depends(get_news_service)],
    accounting: Annotated[AccountingService, Depends(get_accounting_service)],
    settings: Annotated[Settings, Depends(get_settings)],
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsVectorizationJobRepository, Depends(get_job_repository)],
) -> NewsArticleResponse:
    article = news.add_user_article(
        user_id=current_user.id,
        title=request.title,
        content=request.content,
        url=request.url,
        canonical_url=request.canonical_url,
        summary=request.summary,
        language=request.language,
        published_at=request.published_at,
    )
    _withdraw_or_raise(
        accounting=accounting,
        user_id=current_user.id,
        amount=settings.news_add_cost,
        reason=TransactionReason.NEWS_ADD,
        reference_id=UUID(article.id),
    )
    news.commit()
    job_id = await enqueue_vectorization_job(
        repository=repository,
        publisher=publisher,
        payload=_article_vectorization_payload(article),
    )
    return NewsArticleResponse(
        article_id=UUID(article.id),
        status=article.status,
        vectorization_job_id=job_id,
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
    repository: Annotated[NewsVectorizationJobRepository, Depends(get_job_repository)],
) -> NewsSearchResponse:
    filters = NewsSearchFilters(
        language=request.language,
        source_id=request.source_id,
        published_from=request.published_from,
        published_to=request.published_to,
        submitted_by_user_id=request.submitted_by_user_id,
        min_novelty_score=request.min_novelty_score,
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
        vectorization_job_id=job_id,
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
        "target_type": "news_article",
        "article_id": article.id,
        "text": article.content or article.summary or article.title,
        "title": article.title,
        "language": article.language,
    }


def _search_vectorization_payload(search_query: NewsSearchQuery) -> dict:
    return {
        "target_type": "news_search_query",
        "query_id": search_query.id,
        "text": search_query.query_text,
        "filters": search_query.filters,
        "top_k": search_query.top_k,
    }


def _article_history_item(article: NewsArticle) -> NewsArticleHistoryItem:
    return NewsArticleHistoryItem(
        article_id=UUID(article.id),
        title=article.title,
        status=article.status,
        origin=article.origin,
        language=article.language,
        novelty_score=article.novelty_score,
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
) -> None:
    try:
        accounting.withdraw_credit(user_id, amount, reason, reference_id)
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
