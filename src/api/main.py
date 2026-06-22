from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Request, status
from fastapi.exceptions import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from accounting.routes import router as accounting_router
from api.demo import seed_demo, validate_demo_settings
from db.database import create_tables, drop_tables, get_session, init_db
from db.news_pipeline_jobs import NewsPipelineJobRepository
from messaging.rabbitmq import RabbitPublisher
from news.routes import router as news_router
from news.routes import search_router as news_search_router
from settings import Settings, get_settings
from users.routes import admin_router, auth_router, organization_router
from users.routes import router as users_router

JobStatus = Literal["queued", "processing", "done", "failed"]


class NewsVectorizationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    news_ids: list[UUID] = Field(min_length=1, max_length=10_000)
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
    validate_demo_settings(settings)
    init_db(settings)
    if settings.demo_mode:
        drop_tables()
    create_tables()
    repository = NewsPipelineJobRepository(settings.database_url)
    publisher = RabbitPublisher(settings.rabbitmq_url, settings.news_vectorization_queue)
    await repository.initialize()
    await publisher.connect()
    app.state.repository = repository
    app.state.publisher = publisher
    if settings.demo_mode:
        with get_session() as session:
            demo = seed_demo(session, settings)
        if demo.article_ids_to_process:
            job_id = str(uuid4())
            payload = {
                "news_ids": demo.article_ids_to_process,
                "mode": "incremental",
            }
            await repository.mark_queued(job_id, payload)
            await publisher.publish(
                {
                    "job_id": job_id,
                    "type": "news_pipeline",
                    "payload": payload,
                }
            )
    yield
    await publisher.close()


app = FastAPI(title="Semantic News Novelty API", version="0.1.0", lifespan=lifespan)
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(organization_router)
app.include_router(admin_router)
app.include_router(accounting_router)
app.include_router(news_router)
app.include_router(news_search_router)


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
) -> NewsVectorizationJobResponse:
    job_id = str(uuid4())
    payload = request.model_dump(mode="json")
    await repository.mark_queued(job_id, payload)
    await publisher.publish(
        {
            "job_id": job_id,
            "type": "news_pipeline",
            "payload": payload,
        }
    )
    return NewsVectorizationJobResponse(job_id=UUID(job_id), status="queued")


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
