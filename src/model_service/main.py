from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from time import monotonic
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from prometheus_client import REGISTRY, make_asgi_app

from db.news_pipeline_jobs import NewsPipelineJobRepository
from final_pipeline import FinalPipelineConfig, IncrementalNewsNoveltyPipeline, load_pipeline
from messaging.rabbitmq import RabbitConsumer, RabbitPublisher
from model_service.gpu_metrics import NvidiaGpuCollector
from model_service.metrics import (
    PIPELINE_ARTICLES_PROCESSED,
    PIPELINE_CHUNK_ARTICLES,
    PIPELINE_CHUNKS,
    PIPELINE_JOB_DURATION,
    PIPELINE_JOBS,
    PIPELINE_JOBS_IN_PROGRESS,
    PIPELINE_LAST_JOB_DURATION,
    PIPELINE_LAST_JOB_THROUGHPUT,
    PROCESSED_ARTICLES,
)
from news.pipeline_jobs import (
    PIPELINE_MODE_AGGREGATE,
    PIPELINE_MODE_FULL,
    PIPELINE_MODE_INCREMENTAL,
    PIPELINE_MODE_VECTORIZE,
)
from news.pipeline_repository import NewsPipelineRepository
from settings import get_settings


async def handle_message(app: FastAPI, message: dict[str, Any]) -> None:
    message_type = message.get("type")
    if message_type == "news_pipeline":
        await _handle_pipeline_job(app, message)
    elif message_type == "news_search":
        await _handle_search_job(app, message)


async def _handle_pipeline_job(app: FastAPI, message: dict[str, Any]) -> None:
    job_id = str(message["job_id"])
    payload = message["payload"]
    mode = str(payload.get("mode", "incremental"))
    started_at = monotonic()
    jobs: NewsPipelineJobRepository = app.state.jobs
    repository: NewsPipelineRepository = app.state.pipeline_repository
    news_ids: list[str] = []
    status = "failed"
    PIPELINE_JOBS_IN_PROGRESS.labels(mode=mode).inc()
    try:
        news_ids = list(dict.fromkeys(str(value) for value in payload["news_ids"]))
        if not news_ids:
            raise ValueError("news_ids must contain at least one article ID")
        if mode not in {
            PIPELINE_MODE_FULL,
            PIPELINE_MODE_INCREMENTAL,
            PIPELINE_MODE_VECTORIZE,
            PIPELINE_MODE_AGGREGATE,
        }:
            raise ValueError(f"Unsupported pipeline mode: {mode}")
        organization_id = payload.get("organization_id")
        if organization_id is not None:
            organization_id = str(organization_id)
        await jobs.mark_processing(job_id, payload)
        if mode == PIPELINE_MODE_VECTORIZE:
            result = await _run_vectorization_stage(
                app=app,
                repository=repository,
                news_ids=news_ids,
                organization_id=organization_id,
            )
            await jobs.mark_done(job_id, result)
            await _maybe_publish_aggregate_job(app, payload)
            PIPELINE_CHUNKS.labels(status="done").inc()
            PIPELINE_CHUNK_ARTICLES.labels(status="done").inc(result["embedded_count"])
        elif mode == PIPELINE_MODE_FULL:
            requested = await repository.load_articles(news_ids, organization_id)
            await repository.mark_articles_processing(news_ids)
            result = await asyncio.to_thread(app.state.full_pipeline.run, requested)
            await repository.save_result(result)
            await jobs.mark_done(job_id, _pipeline_result_payload(result))
            PIPELINE_ARTICLES_PROCESSED.labels(mode=mode).inc(len(result.requested_ids))
        else:
            new_embeddings = None
            if mode == PIPELINE_MODE_AGGREGATE:
                new_embeddings = await repository.load_embeddings(
                    article_ids=news_ids,
                    model_name=app.state.config.embedding_model_name,
                    model_revision=app.state.config.embedding_model_revision,
                )
            result = await _run_incremental_stage(
                app=app,
                repository=repository,
                news_ids=news_ids,
                organization_id=organization_id,
                new_embeddings=new_embeddings,
            )
            result_payload = _pipeline_result_payload(result)
            await jobs.mark_done(job_id, result_payload)
            parent_job_id = payload.get("parent_job_id")
            if mode == PIPELINE_MODE_AGGREGATE and parent_job_id:
                await jobs.mark_done(str(parent_job_id), result_payload)
            PIPELINE_ARTICLES_PROCESSED.labels(mode=mode).inc(len(result.requested_ids))
        PIPELINE_JOBS.labels(mode=mode, status="done").inc()
        PROCESSED_ARTICLES.set(await repository.count_processed_articles())
        status = "done"
    except Exception as exc:
        error = str(exc)
        await jobs.mark_failed(job_id, error)
        parent_job_id = payload.get("parent_job_id")
        if parent_job_id:
            await jobs.mark_failed(str(parent_job_id), error)
        if news_ids:
            await repository.mark_articles_error(news_ids, error)
        if mode == PIPELINE_MODE_VECTORIZE:
            PIPELINE_CHUNKS.labels(status="failed").inc()
            PIPELINE_CHUNK_ARTICLES.labels(status="failed").inc(len(news_ids))
        PIPELINE_JOBS.labels(mode=mode, status="failed").inc()
    finally:
        duration = monotonic() - started_at
        PIPELINE_JOB_DURATION.labels(mode=mode).observe(duration)
        PIPELINE_LAST_JOB_DURATION.labels(mode=mode, status=status).set(duration)
        if status == "done":
            PIPELINE_LAST_JOB_THROUGHPUT.labels(mode=mode).set(
                len(news_ids) / duration
            )
        PIPELINE_JOBS_IN_PROGRESS.labels(mode=mode).dec()


async def _run_vectorization_stage(
    *,
    app: FastAPI,
    repository: NewsPipelineRepository,
    news_ids: list[str],
    organization_id: str | None,
) -> dict[str, Any]:
    requested = await repository.load_articles(news_ids, organization_id)
    await repository.mark_articles_processing(news_ids)
    embedding_ids, embeddings = await asyncio.to_thread(
        app.state.incremental_pipeline.encode_new_embeddings,
        requested,
    )
    await repository.save_embeddings(
        article_ids=embedding_ids,
        embeddings=embeddings,
        model_name=app.state.config.embedding_model_name,
        model_revision=app.state.config.embedding_model_revision,
    )
    return {
        "mode": PIPELINE_MODE_VECTORIZE,
        "requested_ids": embedding_ids,
        "embedded_count": len(embedding_ids),
        "versions": {
            "embedding_model": app.state.config.embedding_model_name,
            "embedding_model_revision": app.state.config.embedding_model_revision,
        },
    }


async def _run_incremental_stage(
    *,
    app: FastAPI,
    repository: NewsPipelineRepository,
    news_ids: list[str],
    organization_id: str | None,
    new_embeddings,
):
    requested = await repository.load_articles(news_ids, organization_id)
    resolved_organization_id = str(requested["organization_id"].iloc[0])
    await repository.mark_articles_processing(news_ids)
    history, history_embeddings = await repository.load_history(
        organization_id=resolved_organization_id,
        exclude_news_ids=news_ids,
        embedding_model=app.state.config.embedding_model_name,
        embedding_model_revision=app.state.config.embedding_model_revision,
    )
    result = await asyncio.to_thread(
        app.state.incremental_pipeline.process,
        historical_news_df=history,
        historical_embeddings=history_embeddings,
        new_news_df=requested,
        new_embeddings=new_embeddings,
    )
    await repository.save_result(result)
    return result


def _pipeline_result_payload(result) -> dict[str, Any]:
    return {
        "mode": result.mode,
        "requested_ids": result.requested_ids,
        "updated_ids": result.updated_ids,
        "context_count": len(result.context_ids),
        "diagnostics": result.diagnostics,
        "versions": vars(result.versions),
    }


async def _maybe_publish_aggregate_job(app: FastAPI, child_payload: dict[str, Any]) -> None:
    parent_job_id = child_payload.get("parent_job_id")
    aggregate_job_id = child_payload.get("aggregate_job_id")
    if not parent_job_id or not aggregate_job_id:
        return
    jobs: NewsPipelineJobRepository = app.state.jobs
    children = await jobs.list_children(str(parent_job_id))
    completed = sum(1 for child in children if child["status"] == "done")
    failed = [child for child in children if child["status"] == "failed"]
    parent = await jobs.get(str(parent_job_id))
    parent_result = {
        **((parent or {}).get("result") or {}),
        "stage": "vectorizing",
        "completed_chunks": completed,
        "failed_chunks": len(failed),
        "chunk_count": len(children),
    }
    await jobs.update_result(str(parent_job_id), parent_result)
    if failed:
        await jobs.mark_failed(str(parent_job_id), "One or more vectorization chunks failed")
        return
    if completed != len(children):
        return
    aggregate = await jobs.get(str(aggregate_job_id))
    if aggregate is None or aggregate["status"] != "queued":
        return
    await app.state.publisher.publish(
        {
            "job_id": str(aggregate_job_id),
            "type": "news_pipeline",
            "payload": aggregate["request"],
        }
    )


async def _handle_search_job(app: FastAPI, message: dict[str, Any]) -> None:
    job_id = str(message["job_id"])
    payload = message["payload"]
    query_id = str(payload["query_id"])
    jobs: NewsPipelineJobRepository = app.state.jobs
    repository: NewsPipelineRepository = app.state.pipeline_repository
    try:
        await jobs.mark_processing(job_id, payload)
        await repository.mark_search_processing(query_id)
        embedding = await asyncio.to_thread(
            app.state.full_pipeline.encoder.encode_texts,
            [str(payload["text"])],
        )
        result = await repository.complete_search_query(
            query_id=query_id,
            organization_id=payload.get("organization_id"),
            query_embedding=embedding[0].tolist(),
            filters=payload.get("filters") or {},
            top_k=int(payload.get("top_k") or 20),
            model_name=app.state.config.embedding_model_name,
            model_revision=app.state.config.embedding_model_revision,
        )
        await jobs.mark_done(job_id, {"search_result": result})
    except Exception as exc:
        error = str(exc)
        await jobs.mark_failed(job_id, error)
        await repository.mark_search_failed(query_id, error)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    jobs = NewsPipelineJobRepository(settings.database_url)
    repository = NewsPipelineRepository(settings.database_url)
    publisher = RabbitPublisher(settings.rabbitmq_url, settings.news_vectorization_queue)
    config = FinalPipelineConfig.from_json(Path(settings.pipeline_config_path))
    full_pipeline = await asyncio.to_thread(
        load_pipeline,
        model_path=settings.pipeline_model_path,
        config=config,
        device=settings.pipeline_device,
        project_root=Path.cwd(),
    )
    incremental_pipeline = IncrementalNewsNoveltyPipeline(
        encoder=full_pipeline.encoder,
        novelty_model=full_pipeline.novelty_model,
        final_config=config,
    )

    await jobs.initialize()
    await publisher.connect()
    app.state.jobs = jobs
    app.state.pipeline_repository = repository
    app.state.publisher = publisher
    app.state.config = config
    app.state.full_pipeline = full_pipeline
    app.state.incremental_pipeline = incremental_pipeline
    PROCESSED_ARTICLES.set(await repository.count_processed_articles())
    latest_job = await jobs.get_latest_completed()
    if latest_job is not None:
        mode = str(latest_job["request"].get("mode", "incremental"))
        status = str(latest_job["status"])
        duration = (
            latest_job["updated_at"] - latest_job["created_at"]
        ).total_seconds()
        PIPELINE_LAST_JOB_DURATION.labels(mode=mode, status=status).set(duration)
        if status == "done" and duration > 0:
            result = latest_job.get("result") or {}
            article_ids = result.get("requested_ids") or latest_job["request"].get(
                "news_ids",
                [],
            )
            PIPELINE_LAST_JOB_THROUGHPUT.labels(mode=mode).set(
                len(article_ids) / duration
            )

    async def handler(message: dict[str, Any]) -> None:
        await handle_message(app, message)

    consumer = RabbitConsumer(settings.rabbitmq_url, settings.news_vectorization_queue, handler)
    await consumer.start()
    app.state.consumer = consumer
    yield
    await consumer.close()
    await publisher.close()


app = FastAPI(
    title="Semantic News Novelty Model Service",
    version="1.0.0",
    lifespan=lifespan,
)
REGISTRY.register(NvidiaGpuCollector())
app.mount("/metrics", make_asgi_app())


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    config: FinalPipelineConfig = request.app.state.config
    consumer: RabbitConsumer | None = getattr(request.app.state, "consumer", None)
    if consumer is None or not consumer.is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="RabbitMQ consumer is not connected",
        )
    return {
        "status": "ok",
        "service": "model-service",
        "pipeline_version": config.pipeline_version,
        "embedding_model": config.embedding_model_name,
    }
