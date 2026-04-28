from contextlib import asynccontextmanager
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Request, status
from fastapi.exceptions import HTTPException
from pydantic import BaseModel, Field

from db.database import create_tables, init_db
from db.news_vectorization_jobs import NewsVectorizationJobRepository
from messaging.rabbitmq import RabbitPublisher
from settings import Settings, get_settings
from users.routes import auth_router
from users.routes import router as users_router


class NewsVectorizationRequest(BaseModel):
    text: str = Field(min_length=1)
    title: str | None = None
    source_url: str | None = None


class NewsVectorizationJobResponse(BaseModel):
    job_id: str
    status: str


class NewsVectorizationJobStatus(BaseModel):
    job_id: str
    status: str
    request: dict
    result: dict | None


def get_publisher(request: Request) -> RabbitPublisher:
    return request.app.state.publisher


def get_repository(request: Request) -> NewsVectorizationJobRepository:
    return request.app.state.repository


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    init_db(settings)
    create_tables()
    repository = NewsVectorizationJobRepository(settings.database_url)
    publisher = RabbitPublisher(settings.rabbitmq_url, settings.news_vectorization_queue)
    await repository.initialize()
    await publisher.connect()
    app.state.repository = repository
    app.state.publisher = publisher
    yield
    await publisher.close()


app = FastAPI(title="News Flow API", version="0.1.0", lifespan=lifespan)
app.include_router(auth_router)
app.include_router(users_router)


@app.get("/health")
async def health(settings: Annotated[Settings, Depends(get_settings)]) -> dict[str, str]:
    return {"status": "ok", "service": "api", "env": settings.app_env}


@app.post(
    "/v1/news-vectorization",
    response_model=NewsVectorizationJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_news_vectorization_job(
    request: NewsVectorizationRequest,
    publisher: Annotated[RabbitPublisher, Depends(get_publisher)],
    repository: Annotated[NewsVectorizationJobRepository, Depends(get_repository)],
) -> NewsVectorizationJobResponse:
    job_id = str(uuid4())
    payload = request.model_dump()
    await repository.mark_queued(job_id, payload)
    await publisher.publish(
        {
            "job_id": job_id,
            "type": "news_vectorization",
            "payload": payload,
        }
    )
    return NewsVectorizationJobResponse(job_id=job_id, status="queued")


@app.get("/v1/news-vectorization/{job_id}", response_model=NewsVectorizationJobStatus)
async def get_news_vectorization_job(
    job_id: str,
    repository: Annotated[NewsVectorizationJobRepository, Depends(get_repository)],
) -> NewsVectorizationJobStatus:
    job = await repository.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    return NewsVectorizationJobStatus(
        job_id=job["job_id"],
        status=job["status"],
        request=job["request"],
        result=job["result"],
    )
