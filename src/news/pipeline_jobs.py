from __future__ import annotations

from uuid import UUID, uuid4

from db.news_pipeline_jobs import NewsPipelineJobRepository
from messaging.rabbitmq import RabbitPublisher

PIPELINE_MODE_FULL = "full"
PIPELINE_MODE_INCREMENTAL = "incremental"
PIPELINE_MODE_INCREMENTAL_CHUNKED = "incremental_chunked"
PIPELINE_MODE_VECTORIZE = "vectorize"
PIPELINE_MODE_AGGREGATE = "aggregate"
DEFAULT_PIPELINE_CHUNK_SIZE = 5_000
DEFAULT_PIPELINE_AGGREGATE_BATCH_SIZE = 1_000


def chunk_news_ids(news_ids: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    return [
        news_ids[position : position + chunk_size]
        for position in range(0, len(news_ids), chunk_size)
    ]


async def enqueue_pipeline_job(
    *,
    repository: NewsPipelineJobRepository,
    publisher: RabbitPublisher,
    payload: dict,
    chunk_size: int = DEFAULT_PIPELINE_CHUNK_SIZE,
    aggregate_batch_size: int = DEFAULT_PIPELINE_AGGREGATE_BATCH_SIZE,
) -> UUID:
    message_type = (
        "news_search" if payload.get("target_type") == "news_search_query" else "news_pipeline"
    )
    if message_type != "news_pipeline" or not _should_split_pipeline_payload(
        payload,
        chunk_size,
    ):
        job_id = uuid4()
        await repository.mark_queued(str(job_id), payload)
        await _publish_job(
            publisher,
            job_id=str(job_id),
            message_type=message_type,
            payload=payload,
        )
        return job_id

    return await _enqueue_chunked_incremental_job(
        repository=repository,
        publisher=publisher,
        payload=payload,
        chunk_size=chunk_size,
        aggregate_batch_size=aggregate_batch_size,
    )


def _should_split_pipeline_payload(payload: dict, chunk_size: int) -> bool:
    return (
        payload.get("mode", PIPELINE_MODE_INCREMENTAL) == PIPELINE_MODE_INCREMENTAL
        and len(payload.get("news_ids") or []) > chunk_size
    )


async def _enqueue_chunked_incremental_job(
    *,
    repository: NewsPipelineJobRepository,
    publisher: RabbitPublisher,
    payload: dict,
    chunk_size: int,
    aggregate_batch_size: int,
) -> UUID:
    parent_job_id = str(uuid4())
    news_ids = list(dict.fromkeys(str(value) for value in payload["news_ids"]))
    news_ids = await repository.order_article_ids(
        news_ids,
        organization_id=payload.get("organization_id"),
    )
    chunks = chunk_news_ids(news_ids, chunk_size)
    aggregate_batches = chunk_news_ids(news_ids, aggregate_batch_size)
    child_job_ids = [str(uuid4()) for _ in chunks]
    aggregate_job_ids = [str(uuid4()) for _ in aggregate_batches]
    parent_payload = {
        **payload,
        "news_ids": news_ids,
        "mode": PIPELINE_MODE_INCREMENTAL_CHUNKED,
        "chunk_size": chunk_size,
        "aggregate_batch_size": aggregate_batch_size,
        "chunk_count": len(chunks),
        "aggregate_batch_count": len(aggregate_batches),
        "child_job_ids": child_job_ids,
        "aggregate_job_ids": aggregate_job_ids,
    }

    await repository.mark_processing(parent_job_id, parent_payload)
    for index, (aggregate_job_id, batch) in enumerate(
        zip(aggregate_job_ids, aggregate_batches, strict=True),
        start=1,
    ):
        aggregate_payload = {
            **payload,
            "news_ids": batch,
            "mode": PIPELINE_MODE_AGGREGATE,
            "parent_job_id": parent_job_id,
            "batch_index": index,
            "batch_count": len(aggregate_batches),
            "chunk_count": len(chunks),
        }
        await repository.mark_queued(aggregate_job_id, aggregate_payload)
    for index, (child_job_id, chunk) in enumerate(zip(child_job_ids, chunks, strict=True), start=1):
        child_payload = {
            **payload,
            "news_ids": chunk,
            "mode": PIPELINE_MODE_VECTORIZE,
            "parent_job_id": parent_job_id,
            "chunk_index": index,
            "chunk_count": len(chunks),
        }
        await repository.mark_queued(child_job_id, child_payload)
        await _publish_job(
            publisher,
            job_id=child_job_id,
            message_type="news_pipeline",
            payload=child_payload,
        )
    return UUID(parent_job_id)


async def _publish_job(
    publisher: RabbitPublisher,
    *,
    job_id: str,
    message_type: str,
    payload: dict,
) -> None:
    await publisher.publish(
        {
            "job_id": job_id,
            "type": message_type,
            "payload": payload,
        }
    )
