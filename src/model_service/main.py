from __future__ import annotations

import asyncio
from concurrent.futures import Future
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from time import monotonic
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from prometheus_client import REGISTRY, make_asgi_app

from db.news_pipeline_jobs import NewsPipelineJobRepository
from final_pipeline import FinalPipelineConfig, IncrementalNewsNoveltyPipeline, load_pipeline
from messaging.rabbitmq import RabbitConsumer, RabbitPublisher
from model.significance_model import CatBoostSignificanceModel
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
    PIPELINE_STAGE_ARTICLES,
    PIPELINE_STAGE_DURATION,
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
    current_stage: str | None = None
    current_stage_started_at: float | None = None
    PIPELINE_JOBS_IN_PROGRESS.labels(mode=mode).inc()

    async def mark_stage(
        stage: str,
        *,
        progress_percent: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        nonlocal current_stage, current_stage_started_at
        now = monotonic()
        if current_stage is not None and current_stage != stage:
            if current_stage_started_at is not None:
                previous_stage_elapsed = now - current_stage_started_at
                PIPELINE_STAGE_DURATION.labels(mode=mode, stage=current_stage).observe(
                    previous_stage_elapsed
                )
                await _update_pipeline_job_stage_duration(
                    jobs=jobs,
                    job_id=job_id,
                    payload=payload,
                    stage=current_stage,
                    stage_elapsed_seconds=previous_stage_elapsed,
                    pipeline_elapsed_seconds=now - started_at,
                )
            PIPELINE_STAGE_ARTICLES.labels(mode=mode, stage=current_stage).set(0)
            current_stage_started_at = now
        elif current_stage is None:
            current_stage_started_at = now
        current_stage = stage
        PIPELINE_STAGE_ARTICLES.labels(mode=mode, stage=stage).set(len(news_ids))
        stage_elapsed_seconds = (
            now - current_stage_started_at
            if current_stage_started_at is not None
            else 0.0
        )
        await _update_pipeline_job_stage(
            jobs=jobs,
            job_id=job_id,
            payload=payload,
            stage=stage,
            article_count=len(news_ids),
            progress_percent=progress_percent,
            stage_elapsed_seconds=stage_elapsed_seconds,
            pipeline_elapsed_seconds=now - started_at,
            extra=extra,
        )

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
            await mark_stage("vectorization", progress_percent=None)
            result = await _run_vectorization_stage(
                app=app,
                repository=repository,
                news_ids=news_ids,
                organization_id=organization_id,
            )
            await jobs.mark_done(job_id, result)
            await _maybe_publish_next_aggregate_job(app, payload)
            PIPELINE_CHUNKS.labels(status="done").inc()
            PIPELINE_CHUNK_ARTICLES.labels(status="done").inc(result["embedded_count"])
        elif mode == PIPELINE_MODE_FULL:
            await mark_stage("full_pipeline", progress_percent=None)
            requested = await repository.load_articles(news_ids, organization_id)
            await repository.mark_articles_processing(news_ids)
            result = await asyncio.to_thread(app.state.full_pipeline.run, requested)
            await repository.save_result(result)
            await jobs.mark_done(job_id, _pipeline_result_payload(result))
            PIPELINE_ARTICLES_PROCESSED.labels(mode=mode).inc(len(result.requested_ids))
        else:
            new_embeddings = None
            process_progress_callback = None
            progress_update_futures: list[Future] = []
            if mode == PIPELINE_MODE_AGGREGATE:
                await mark_stage("loading_embeddings", progress_percent=91)
                new_embeddings = await repository.load_embeddings(
                    article_ids=news_ids,
                    model_name=app.state.config.embedding_model_name,
                    model_revision=app.state.config.embedding_model_revision,
                )
                await mark_stage("loading_history", progress_percent=92)
                loop = asyncio.get_running_loop()

                def process_progress_callback(
                    stage: str,
                    details: dict[str, Any],
                ) -> None:
                    progress_percent = _aggregate_stage_progress_percent(stage, details)
                    progress_update_futures.append(
                        asyncio.run_coroutine_threadsafe(
                            mark_stage(
                                stage,
                                progress_percent=progress_percent,
                                extra={"stage_details": details},
                            ),
                            loop,
                        )
                    )

            else:
                await mark_stage("incremental_pipeline", progress_percent=None)
            result = await _run_incremental_stage(
                app=app,
                repository=repository,
                news_ids=news_ids,
                organization_id=organization_id,
                new_embeddings=new_embeddings,
                progress_callback=process_progress_callback,
            )
            for future in progress_update_futures:
                await asyncio.wrap_future(future)
            result_payload = _pipeline_result_payload(result)
            await jobs.mark_done(job_id, result_payload)
            parent_job_id = payload.get("parent_job_id")
            if mode == PIPELINE_MODE_AGGREGATE and parent_job_id:
                if payload.get("batch_count"):
                    await _maybe_publish_next_aggregate_job(app, payload)
                else:
                    await jobs.mark_done(str(parent_job_id), result_payload)
            elif parent_job_id:
                await jobs.mark_done(str(parent_job_id), result_payload)
            PIPELINE_ARTICLES_PROCESSED.labels(mode=mode).inc(len(result.requested_ids))
        PIPELINE_JOBS.labels(mode=mode, status="done").inc()
        PROCESSED_ARTICLES.set(await repository.count_processed_articles())
        status = "done"
    except asyncio.CancelledError:
        error = "Pipeline job was cancelled"
        await jobs.mark_failed(job_id, error)
        parent_job_id = payload.get("parent_job_id")
        if parent_job_id:
            await jobs.mark_failed(str(parent_job_id), error)
        if news_ids:
            await repository.mark_articles_error(news_ids, error)
        PIPELINE_JOBS.labels(mode=mode, status="failed").inc()
        raise
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
        if current_stage is not None:
            if current_stage_started_at is not None:
                final_stage_elapsed = monotonic() - current_stage_started_at
                PIPELINE_STAGE_DURATION.labels(mode=mode, stage=current_stage).observe(
                    final_stage_elapsed
                )
                await _update_pipeline_job_stage_duration(
                    jobs=jobs,
                    job_id=job_id,
                    payload=payload,
                    stage=current_stage,
                    stage_elapsed_seconds=final_stage_elapsed,
                    pipeline_elapsed_seconds=duration,
                )
            PIPELINE_STAGE_ARTICLES.labels(mode=mode, stage=current_stage).set(0)
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


async def _update_pipeline_job_stage(
    *,
    jobs: NewsPipelineJobRepository,
    job_id: str,
    payload: dict[str, Any],
    stage: str,
    article_count: int,
    progress_percent: int | None,
    stage_elapsed_seconds: float,
    pipeline_elapsed_seconds: float,
    extra: dict[str, Any] | None = None,
) -> None:
    job = await jobs.get(job_id)
    previous_result = (job or {}).get("result") or {}
    stage_timings = _stage_timings_payload(
        previous_result=previous_result,
        stage=stage,
        article_count=article_count,
        progress_percent=progress_percent,
        stage_elapsed_seconds=stage_elapsed_seconds,
        pipeline_elapsed_seconds=pipeline_elapsed_seconds,
        extra=extra,
    )
    result = {
        **previous_result,
        "stage": stage,
        "stage_article_count": article_count,
        "stage_timings": stage_timings,
        **(extra or {}),
    }
    if progress_percent is not None:
        result["progress_percent"] = progress_percent
    await jobs.update_result(job_id, result)

    parent_job_id = payload.get("parent_job_id")
    if not parent_job_id:
        return
    parent = await jobs.get(str(parent_job_id))
    if parent is None:
        return
    parent_previous_result = parent.get("result") or {}
    parent_stage_timings = _stage_timings_payload(
        previous_result=parent_previous_result,
        stage=stage,
        article_count=article_count,
        progress_percent=progress_percent,
        stage_elapsed_seconds=stage_elapsed_seconds,
        pipeline_elapsed_seconds=pipeline_elapsed_seconds,
        extra=extra,
    )
    parent_result = {
        **parent_previous_result,
        "stage": stage,
        "stage_article_count": article_count,
        "active_child_job_id": job_id,
        "stage_timings": parent_stage_timings,
    }
    if progress_percent is not None:
        parent_result["progress_percent"] = progress_percent
    await jobs.update_result(str(parent_job_id), parent_result)


async def _update_pipeline_job_stage_duration(
    *,
    jobs: NewsPipelineJobRepository,
    job_id: str,
    payload: dict[str, Any],
    stage: str,
    stage_elapsed_seconds: float,
    pipeline_elapsed_seconds: float,
) -> None:
    job = await jobs.get(job_id)
    if job is None:
        return
    result = _result_with_closed_stage(
        previous_result=job.get("result") or {},
        stage=stage,
        stage_elapsed_seconds=stage_elapsed_seconds,
        pipeline_elapsed_seconds=pipeline_elapsed_seconds,
    )
    await jobs.update_result(job_id, result)

    parent_job_id = payload.get("parent_job_id")
    if not parent_job_id:
        return
    parent = await jobs.get(str(parent_job_id))
    if parent is None:
        return
    parent_result = _result_with_closed_stage(
        previous_result=parent.get("result") or {},
        stage=stage,
        stage_elapsed_seconds=stage_elapsed_seconds,
        pipeline_elapsed_seconds=pipeline_elapsed_seconds,
    )
    await jobs.update_result(str(parent_job_id), parent_result)


def _result_with_closed_stage(
    *,
    previous_result: dict[str, Any],
    stage: str,
    stage_elapsed_seconds: float,
    pipeline_elapsed_seconds: float,
) -> dict[str, Any]:
    result = dict(previous_result)
    stage_timings = result.get("stage_timings")
    if not isinstance(stage_timings, dict):
        stage_timings = {}
    else:
        stage_timings = dict(stage_timings)
    stage_entry = stage_timings.get(stage)
    if not isinstance(stage_entry, dict):
        stage_entry = {}
    else:
        stage_entry = dict(stage_entry)
    stage_entry["stage_elapsed_seconds"] = round(stage_elapsed_seconds, 3)
    stage_entry["pipeline_elapsed_seconds"] = round(pipeline_elapsed_seconds, 3)
    stage_timings[stage] = stage_entry
    result["stage_timings"] = stage_timings
    return result


def _stage_timings_payload(
    *,
    previous_result: dict[str, Any],
    stage: str,
    article_count: int,
    progress_percent: int | None,
    stage_elapsed_seconds: float,
    pipeline_elapsed_seconds: float,
    extra: dict[str, Any] | None,
) -> dict[str, Any]:
    stage_timings = previous_result.get("stage_timings")
    if not isinstance(stage_timings, dict):
        stage_timings = {}
    else:
        stage_timings = dict(stage_timings)
    stage_entry = stage_timings.get(stage)
    if not isinstance(stage_entry, dict):
        stage_entry = {}
    else:
        stage_entry = dict(stage_entry)
    stage_entry.update(
        {
            "article_count": int(article_count),
            "stage_elapsed_seconds": round(stage_elapsed_seconds, 3),
            "pipeline_elapsed_seconds": round(pipeline_elapsed_seconds, 3),
        }
    )
    if progress_percent is not None:
        stage_entry["progress_percent"] = int(progress_percent)
    stage_details = (extra or {}).get("stage_details")
    if isinstance(stage_details, dict):
        for key in (
            "completed_rows",
            "total_rows",
            "elapsed_seconds",
            "historical_rows",
            "history_rows",
            "new_rows",
            "assigned_to_existing",
            "created_clusters",
            "candidate_scoring_seconds",
            "merge_seconds",
            "selection_seconds",
            "affected_lookup_seconds",
            "history_append_seconds",
            "embedding_normalization_seconds",
            "candidate_rows_total",
            "candidate_rows_avg",
            "candidate_rows_max",
            "rows_without_candidates",
            "merged_cluster_count",
            "merged_component_count",
        ):
            if key in stage_details:
                stage_entry[key] = stage_details[key]
    stage_timings[stage] = stage_entry
    return stage_timings


async def _run_incremental_stage(
    *,
    app: FastAPI,
    repository: NewsPipelineRepository,
    news_ids: list[str],
    organization_id: str | None,
    new_embeddings,
    progress_callback=None,
):
    requested = await repository.load_articles(news_ids, organization_id)
    resolved_organization_id = str(requested["organization_id"].iloc[0])
    history_published_from, history_published_to = _history_date_bounds(
        requested,
        window_days=app.state.settings.pipeline_history_window_days,
    )
    await repository.mark_articles_processing(news_ids)
    history, history_embeddings = await repository.load_history(
        organization_id=resolved_organization_id,
        exclude_news_ids=news_ids,
        embedding_model=app.state.config.embedding_model_name,
        embedding_model_revision=app.state.config.embedding_model_revision,
        published_from=history_published_from,
        published_to=history_published_to,
        expand_clusters=app.state.settings.pipeline_history_expand_clusters,
        cluster_expansion_max_rows=(
            app.state.settings.pipeline_history_cluster_expansion_max_rows
        ),
    )
    result = await asyncio.to_thread(
        app.state.incremental_pipeline.process,
        historical_news_df=history,
        historical_embeddings=history_embeddings,
        new_news_df=requested,
        new_embeddings=new_embeddings,
        progress_callback=progress_callback,
    )
    await repository.save_result(result)
    return result


def _history_date_bounds(
    requested,
    *,
    window_days: int,
) -> tuple[Any | None, Any | None]:
    if requested.empty or "published_at" not in requested.columns:
        return None, None
    min_date = requested["published_at"].min()
    max_date = requested["published_at"].max()
    if min_date is None or max_date is None:
        return None, None
    window = timedelta(days=window_days)
    return _to_python_datetime(min_date - window), _to_python_datetime(max_date + window)


def _to_python_datetime(value):
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    return value


def _aggregate_stage_progress_percent(stage: str, details: dict[str, Any]) -> int:
    if stage == "prepare_history":
        return 93
    if stage == "prepare_new_batch":
        return 94
    if stage == "cluster_assignment":
        total_rows = int(details.get("total_rows") or 0)
        completed_rows = int(details.get("completed_rows") or 0)
        if total_rows <= 0:
            return 95
        return min(97, 94 + int((completed_rows / total_rows) * 3))
    if stage == "novelty_prediction":
        return 98
    if stage == "novelty_prediction_done":
        return 99
    if stage == "result_assembly":
        return 99
    return 95


def _pipeline_result_payload(result) -> dict[str, Any]:
    return {
        "mode": result.mode,
        "requested_ids": result.requested_ids,
        "updated_ids": result.updated_ids,
        "context_count": len(result.context_ids),
        "diagnostics": result.diagnostics,
        "versions": vars(result.versions),
    }


async def _maybe_publish_next_aggregate_job(
    app: FastAPI,
    child_payload: dict[str, Any],
) -> None:
    parent_job_id = child_payload.get("parent_job_id")
    if not parent_job_id:
        return
    jobs: NewsPipelineJobRepository = app.state.jobs
    vector_children = await jobs.list_children(
        str(parent_job_id),
        mode=PIPELINE_MODE_VECTORIZE,
    )
    vector_completed = sum(1 for child in vector_children if child["status"] == "done")
    vector_failed = [child for child in vector_children if child["status"] == "failed"]
    parent = await jobs.get(str(parent_job_id))
    if parent is None:
        return
    parent_result = {
        **(parent.get("result") or {}),
        "stage": "vectorizing",
        "progress_percent": (
            int((vector_completed / len(vector_children)) * 60)
            if vector_children
            else 0
        ),
        "completed_chunks": vector_completed,
        "failed_chunks": len(vector_failed),
        "chunk_count": len(vector_children),
    }
    if vector_failed:
        await jobs.update_result(str(parent_job_id), parent_result)
        await jobs.mark_failed(str(parent_job_id), "One or more vectorization chunks failed")
        return
    if vector_children and vector_completed != len(vector_children):
        await jobs.update_result(str(parent_job_id), parent_result)
        return

    aggregate_children = await jobs.list_children(
        str(parent_job_id),
        mode=PIPELINE_MODE_AGGREGATE,
    )
    aggregate_completed = sum(
        1 for child in aggregate_children if child["status"] == "done"
    )
    aggregate_failed = [
        child for child in aggregate_children if child["status"] == "failed"
    ]
    parent_result = {
        **parent_result,
        "stage": "aggregating",
        "progress_percent": (
            60 + int((aggregate_completed / len(aggregate_children)) * 40)
            if aggregate_children
            else 60
        ),
        "completed_aggregate_batches": aggregate_completed,
        "failed_aggregate_batches": len(aggregate_failed),
        "aggregate_batch_count": len(aggregate_children),
    }
    await jobs.update_result(str(parent_job_id), parent_result)
    if aggregate_failed:
        await jobs.mark_failed(str(parent_job_id), "One or more aggregate batches failed")
        return
    if aggregate_children and aggregate_completed == len(aggregate_children):
        await jobs.mark_done(
            str(parent_job_id),
            _parent_result_payload(parent["request"], aggregate_children),
        )
        return

    next_batch = next(
        (
            child
            for child in aggregate_children
            if child["status"] == "queued"
            and int(child["request"].get("batch_index") or 0) == aggregate_completed + 1
        ),
        None,
    )
    if next_batch is None:
        return
    if not await jobs.mark_dispatched_if_queued(str(next_batch["job_id"])):
        return
    await app.state.publisher.publish(
        {
            "job_id": str(next_batch["job_id"]),
            "type": "news_pipeline",
            "payload": next_batch["request"],
        },
        queue_name=app.state.aggregation_queue,
    )


def _parent_result_payload(
    parent_payload: dict[str, Any],
    aggregate_children: list[dict[str, Any]],
) -> dict[str, Any]:
    updated_ids: list[str] = []
    versions: dict[str, Any] = {}
    diagnostics: dict[str, Any] = {"aggregate_batches": len(aggregate_children)}
    for child in aggregate_children:
        result = child.get("result") or {}
        updated_ids.extend(str(value) for value in result.get("updated_ids") or [])
        if not versions and isinstance(result.get("versions"), dict):
            versions = result["versions"]
    return {
        "mode": parent_payload.get("mode", PIPELINE_MODE_INCREMENTAL),
        "requested_ids": list(parent_payload.get("news_ids") or []),
        "updated_ids": list(dict.fromkeys(updated_ids)),
        "context_count": 0,
        "diagnostics": diagnostics,
        "versions": versions,
    }


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
    worker_role = settings.model_service_role.lower().strip()
    if worker_role not in {"all", "vectorizer", "aggregator"}:
        raise RuntimeError(
            "MODEL_SERVICE_ROLE must be one of: all, vectorizer, aggregator"
        )
    consumer_queue = (
        settings.news_aggregation_queue
        if worker_role == "aggregator"
        else settings.news_vectorization_queue
    )
    aggregation_queue = (
        settings.news_vectorization_queue
        if worker_role == "all"
        else settings.news_aggregation_queue
    )
    config = FinalPipelineConfig.from_json(Path(settings.pipeline_config_path))
    full_pipeline = None
    if worker_role in {"all", "vectorizer"}:
        full_pipeline = await asyncio.to_thread(
            load_pipeline,
            model_path=settings.pipeline_model_path,
            config=config,
            device=settings.pipeline_device,
            project_root=Path.cwd(),
        )
        encoder = full_pipeline.encoder
        novelty_model = full_pipeline.novelty_model
    else:
        encoder = None
        novelty_model = await asyncio.to_thread(
            CatBoostSignificanceModel.load,
            Path(settings.pipeline_model_path),
        )
    incremental_pipeline = IncrementalNewsNoveltyPipeline(
        encoder=encoder,
        novelty_model=novelty_model,
        final_config=config,
    )

    await jobs.initialize()
    await publisher.connect()
    await publisher.declare_queue(settings.news_aggregation_queue)
    app.state.jobs = jobs
    app.state.settings = settings
    app.state.pipeline_repository = repository
    app.state.publisher = publisher
    app.state.worker_role = worker_role
    app.state.aggregation_queue = aggregation_queue
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

    consumer = RabbitConsumer(settings.rabbitmq_url, consumer_queue, handler)
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
        "service": _service_name(str(getattr(request.app.state, "worker_role", "all"))),
        "role": str(getattr(request.app.state, "worker_role", "all")),
        "pipeline_version": config.pipeline_version,
        "embedding_model": config.embedding_model_name,
    }


def _service_name(worker_role: str) -> str:
    if worker_role == "vectorizer":
        return "model-service-vectorizer"
    if worker_role == "aggregator":
        return "model-service-processor"
    return "model-service"
