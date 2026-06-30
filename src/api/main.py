import asyncio
import inspect
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import Depends, FastAPI, Request, status
from fastapi.exceptions import HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_client import make_asgi_app
from pydantic import BaseModel, ConfigDict, Field

from accounting.routes import router as accounting_router
from api.demo import DemoSeedResult, seed_demo, validate_demo_settings
from api.metrics import register_api_database_metrics
from db.database import create_tables, drop_tables, get_session, init_db
from db.news_pipeline_jobs import NewsPipelineJobRepository
from messaging.rabbitmq import RabbitPublisher
from news.pipeline_jobs import enqueue_pipeline_job
from news.routes import router as news_router
from news.routes import search_router as news_search_router
from settings import Settings, get_settings
from users.routes import admin_router, auth_router, organization_router
from users.routes import router as users_router

JobStatus = Literal["queued", "processing", "done", "failed"]
MAX_PIPELINE_NEWS_IDS = 50_000
STARTUP_RETRY_ATTEMPTS = 12
STARTUP_RETRY_DELAY_SECONDS = 5

logger = logging.getLogger(__name__)


class ApiGZipMiddleware(GZipMiddleware):
    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") == "http" and str(scope.get("path", "")).startswith(
            "/metrics"
        ):
            await self.app(scope, receive, send)
            return
        await super().__call__(scope, receive, send)


class NewsVectorizationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    news_ids: list[UUID] = Field(min_length=1, max_length=MAX_PIPELINE_NEWS_IDS)
    organization_id: UUID
    mode: Literal["full", "incremental"] = "incremental"


class NewsVectorizationJobResponse(BaseModel):
    job_id: UUID
    status: Literal["queued"]


class NewsVectorizationJobStatus(BaseModel):
    job_id: UUID
    status: JobStatus
    request: dict[str, object]
    result: dict[str, object] | None
    created_at: datetime
    updated_at: datetime


def get_publisher(request: Request) -> RabbitPublisher:
    return request.app.state.publisher


def get_repository(request: Request) -> NewsPipelineJobRepository:
    return request.app.state.repository


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    register_api_database_metrics(settings.database_url)
    validate_demo_settings(settings)
    init_db(settings)
    if settings.demo_mode:
        await _run_startup_step_with_retries("drop demo tables", drop_tables)
    await _run_startup_step_with_retries("create database tables", create_tables)
    repository = NewsPipelineJobRepository(settings.database_url)
    publisher = RabbitPublisher(settings.rabbitmq_url, settings.news_vectorization_queue)
    await _run_startup_step_with_retries(
        "initialize pipeline job repository",
        repository.initialize,
    )
    await _run_startup_step_with_retries("connect RabbitMQ publisher", publisher.connect)
    app.state.repository = repository
    app.state.publisher = publisher
    if settings.demo_mode:
        with get_session() as session:
            demo = seed_demo(session, settings)
        await enqueue_demo_pipeline_jobs(repository, publisher, demo)
    yield
    await publisher.close()


async def _run_startup_step_with_retries(
    name: str,
    operation: Callable[[], Any],
) -> Any:
    for attempt in range(1, STARTUP_RETRY_ATTEMPTS + 1):
        try:
            result = operation()
            if inspect.isawaitable(result):
                return await result
            return result
        except Exception:
            if attempt >= STARTUP_RETRY_ATTEMPTS:
                raise
            logger.warning(
                "Startup step %s failed on attempt %s/%s; retrying in %s seconds",
                name,
                attempt,
                STARTUP_RETRY_ATTEMPTS,
                STARTUP_RETRY_DELAY_SECONDS,
                exc_info=True,
            )
            await asyncio.sleep(STARTUP_RETRY_DELAY_SECONDS)


app = FastAPI(title="Semantic News Novelty API", version="0.1.0", lifespan=lifespan)
app.add_middleware(ApiGZipMiddleware, minimum_size=1024)
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(organization_router)
app.include_router(admin_router)
app.include_router(accounting_router)
app.include_router(news_router)
app.include_router(news_search_router)
app.mount("/metrics", make_asgi_app())


@app.get("/health")
async def health(settings: Annotated[Settings, Depends(get_settings)]) -> dict[str, str]:
    return {"status": "ok", "service": "api", "env": settings.app_env}


@app.post(
    "/news-pipeline",
    response_model=NewsVectorizationJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_news_vectorization_job(
    request: NewsVectorizationRequest,
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsPipelineJobRepository, Depends(get_repository)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> NewsVectorizationJobResponse:
    payload = request.model_dump(mode="json")
    job_id = await enqueue_pipeline_job(
        repository=repository,
        publisher=publisher,
        payload=payload,
        chunk_size=settings.pipeline_chunk_size,
    )
    return NewsVectorizationJobResponse(job_id=job_id, status="queued")


async def enqueue_demo_pipeline_jobs(
    repository: NewsPipelineJobRepository,
    publisher: RabbitPublisher,
    demo: DemoSeedResult,
) -> None:
    for batch in demo.pipeline_batches:
        if not batch.article_ids_to_process:
            continue
        payload = {
            "news_ids": batch.article_ids_to_process,
            "organization_id": str(batch.organization_id),
            "mode": "incremental",
        }
        await enqueue_pipeline_job(
            repository=repository,
            publisher=publisher,
            payload=payload,
        )


@app.get("/news-pipeline/{job_id}", response_model=NewsVectorizationJobStatus)
async def get_news_vectorization_job(
    job_id: UUID,
    repository: Annotated[NewsPipelineJobRepository, Depends(get_repository)],
) -> NewsVectorizationJobStatus:
    job = await repository.get(str(job_id))
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    return NewsVectorizationJobStatus(
        job_id=UUID(job["job_id"]),
        status=job["status"],
        request=job["request"],
        result=job["result"],
        created_at=job["created_at"],
        updated_at=job["updated_at"],
    )


app.add_api_route(
    "/news-vectorization",
    create_news_vectorization_job,
    methods=["POST"],
    response_model=NewsVectorizationJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    deprecated=True,
    include_in_schema=False,
)
app.add_api_route(
    "/news-vectorization/{job_id}",
    get_news_vectorization_job,
    methods=["GET"],
    response_model=NewsVectorizationJobStatus,
    deprecated=True,
    include_in_schema=False,
)
