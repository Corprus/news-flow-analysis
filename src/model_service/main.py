from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request

from db.news_vectorization_jobs import NewsVectorizationJobRepository
from messaging.rabbitmq import RabbitConsumer
from news.vectorization_repository import NewsVectorizationRepository
from services.news_vectorizer import NewsVectorizer
from settings import get_settings


async def handle_news_vectorization_job(app: FastAPI, message: dict[str, Any]) -> None:
    if message.get("type") != "news_vectorization":
        return

    job_id = message["job_id"]
    payload = message["payload"]
    repository: NewsVectorizationJobRepository = app.state.repository
    news_repository: NewsVectorizationRepository = app.state.news_repository
    vectorizer: NewsVectorizer = app.state.model

    target_type = payload.get("target_type")

    try:
        await repository.mark_processing(job_id, payload)
        if target_type == "news_article":
            await news_repository.mark_article_processing(payload["article_id"])
        elif target_type == "news_search_query":
            await news_repository.mark_search_processing(payload["query_id"])

        result = await vectorizer.vectorize_text(text=payload["text"])

        if target_type == "news_article":
            await _handle_article_vectorization(news_repository, payload, result)
        elif target_type == "news_search_query":
            search_result = await _handle_search_query(news_repository, payload, result)
            result = {**result, "search_result": search_result}

        await repository.mark_done(job_id, _job_result(result))
    except Exception as exc:
        error = str(exc)
        await repository.mark_failed(job_id, error)
        if target_type == "news_article" and payload.get("article_id"):
            await news_repository.mark_article_failed(payload["article_id"], error)
        if target_type == "news_search_query" and payload.get("query_id"):
            await news_repository.mark_search_failed(payload["query_id"], error)


async def _handle_article_vectorization(
    repository: NewsVectorizationRepository,
    payload: dict[str, Any],
    vectorization_result: dict[str, Any],
) -> None:
    article_id = payload["article_id"]
    await repository.save_article_embedding(
        article_id=article_id,
        embedding=vectorization_result["embedding"],
        model_name=vectorization_result["model_source"],
        model_revision=vectorization_result["resolved_model_source"],
    )


async def _handle_search_query(
    repository: NewsVectorizationRepository,
    payload: dict[str, Any],
    vectorization_result: dict[str, Any],
) -> dict[str, Any]:
    query_id = payload["query_id"]
    return await repository.complete_search_query(
        query_id=query_id,
        query_embedding=vectorization_result["embedding"],
        filters=payload.get("filters") or {},
        top_k=int(payload.get("top_k") or 20),
    )


def _job_result(vectorization_result: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in vectorization_result.items()
        if key != "embedding"
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    repository = NewsVectorizationJobRepository(settings.database_url)
    news_repository = NewsVectorizationRepository(settings.database_url)
    model = NewsVectorizer(settings.model_source)

    await repository.initialize()
    await model.load()

    app.state.repository = repository
    app.state.news_repository = news_repository
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
