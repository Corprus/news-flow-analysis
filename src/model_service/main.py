from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request

from db.news_vectorization_jobs import NewsVectorizationJobRepository
from messaging.rabbitmq import RabbitConsumer
from services.news_vectorizer import NewsVectorizer
from settings import get_settings


async def handle_news_vectorization_job(app: FastAPI, message: dict[str, Any]) -> None:
    if message.get("type") != "news_vectorization":
        return

    job_id = message["job_id"]
    payload = message["payload"]
    repository: NewsVectorizationJobRepository = app.state.repository
    vectorizer: NewsVectorizer = app.state.model

    await repository.mark_processing(job_id, payload)
    result = await vectorizer.vectorize_text(text=payload["text"])
    await repository.mark_done(job_id, result)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    repository = NewsVectorizationJobRepository(settings.database_url)
    model = NewsVectorizer(settings.model_source)

    await repository.initialize()
    await model.load()

    app.state.repository = repository
    app.state.model = model

    async def handler(message: dict[str, Any]) -> None:
        await handle_news_vectorization_job(app, message)

    consumer = RabbitConsumer(settings.rabbitmq_url, settings.news_vectorization_queue, handler)
    await consumer.start()
    app.state.consumer = consumer

    yield
    await consumer.close()


app = FastAPI(title="News Flow Model Service", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    model: NewsVectorizer = request.app.state.model
    return {
        "status": "ok",
        "service": "model-service",
        "model_loaded": str(model.is_loaded).lower(),
        "model_source": model.model_source,
        "resolved_model_source": model.resolved_model_source,
    }
