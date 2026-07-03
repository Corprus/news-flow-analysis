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


async def start_streaming_incremental_job(
    *,
    repository: NewsPipelineJobRepository,
    payload: dict,
    chunk_size: int,
    aggregate_batch_size: int,
) -> UUID:
    parent_job_id = str(uuid4())
    parent_payload = {
        **payload,
        "news_ids": [],
        "mode": PIPELINE_MODE_INCREMENTAL_CHUNKED,
        "chunk_size": chunk_size,
        "aggregate_batch_size": aggregate_batch_size,
        "chunk_count": 0,
        "aggregate_batch_count": 0,
        "child_job_ids": [],
        "aggregate_job_ids": [],
        "streaming_import": True,
    }
    await repository.mark_processing(parent_job_id, parent_payload)
    return UUID(parent_job_id)


async def enqueue_streaming_vectorize_chunk(
    *,
    repository: NewsPipelineJobRepository,
    publisher: RabbitPublisher,
    parent_job_id: UUID,
    parent_payload: dict,
    news_ids: list[str],
) -> None:
    if not news_ids:
        return
    parent = await repository.get(str(parent_job_id))
    request = dict((parent or {}).get("request") or {})
    existing_news_ids = [str(value) for value in request.get("news_ids") or []]
    child_job_ids = [str(value) for value in request.get("child_job_ids") or []]
    chunk_index = len(child_job_ids) + 1
    child_job_id = str(uuid4())
    child_job_ids.append(child_job_id)
    updated_news_ids = existing_news_ids + [str(value) for value in news_ids]
    updated_parent_payload = {
        **parent_payload,
        **request,
        "news_ids": updated_news_ids,
        "mode": PIPELINE_MODE_INCREMENTAL_CHUNKED,
        "chunk_count": len(child_job_ids),
        "child_job_ids": child_job_ids,
        "streaming_import": True,
    }
    await repository.mark_processing(str(parent_job_id), updated_parent_payload)

    child_payload = {
        **parent_payload,
        "news_ids": [str(value) for value in news_ids],
        "mode": PIPELINE_MODE_VECTORIZE,
        "parent_job_id": str(parent_job_id),
        "chunk_index": chunk_index,
    }
    await repository.mark_queued(child_job_id, child_payload)
    await _publish_job(
        publisher,
        job_id=child_job_id,
        message_type="news_pipeline",
        payload=child_payload,
    )


async def finalize_streaming_incremental_job(
    *,
    repository: NewsPipelineJobRepository,
    publisher: RabbitPublisher,
    parent_job_id: UUID,
    parent_payload: dict,
    aggregate_batch_size: int,
    aggregation_queue_name: str,
) -> UUID:
    parent = await repository.get(str(parent_job_id))
    request = dict((parent or {}).get("request") or {})
    news_ids = list(dict.fromkeys(str(value) for value in request.get("news_ids") or []))
    news_ids = await repository.order_article_ids(
        news_ids,
        organization_id=parent_payload.get("organization_id"),
    )
    aggregate_batches = chunk_news_ids(news_ids, aggregate_batch_size)
    aggregate_job_ids = [str(uuid4()) for _ in aggregate_batches]
    updated_parent_payload = {
        **parent_payload,
        **request,
        "news_ids": news_ids,
        "mode": PIPELINE_MODE_INCREMENTAL_CHUNKED,
        "aggregate_batch_size": aggregate_batch_size,
        "aggregate_batch_count": len(aggregate_batches),
        "aggregate_job_ids": aggregate_job_ids,
        "streaming_import": True,
    }
    await repository.mark_processing(str(parent_job_id), updated_parent_payload)

    for index, (aggregate_job_id, batch) in enumerate(
        zip(aggregate_job_ids, aggregate_batches, strict=True),
        start=1,
    ):
        aggregate_payload = {
            **parent_payload,
            "news_ids": batch,
            "mode": PIPELINE_MODE_AGGREGATE,
            "parent_job_id": str(parent_job_id),
            "batch_index": index,
            "batch_count": len(aggregate_batches),
            "chunk_count": int(updated_parent_payload.get("chunk_count") or 0),
        }
        await repository.mark_queued(aggregate_job_id, aggregate_payload)

    await _publish_first_aggregate_if_ready(
        repository=repository,
        publisher=publisher,
        parent_job_id=str(parent_job_id),
        aggregation_queue_name=aggregation_queue_name,
    )
    return parent_job_id


async def _publish_first_aggregate_if_ready(
    *,
    repository: NewsPipelineJobRepository,
    publisher: RabbitPublisher,
    parent_job_id: str,
    aggregation_queue_name: str,
) -> None:
    vector_children = await repository.list_children(
        parent_job_id,
        mode=PIPELINE_MODE_VECTORIZE,
    )
    if not vector_children or any(child["status"] != "done" for child in vector_children):
        return
    aggregate_children = await repository.list_children(
        parent_job_id,
        mode=PIPELINE_MODE_AGGREGATE,
    )
    first_batch = next(
        (
            child
            for child in aggregate_children
            if child["status"] == "queued"
            and int(child["request"].get("batch_index") or 0) == 1
        ),
        None,
    )
    if first_batch is None:
        return
    if not await repository.mark_dispatched_if_queued(str(first_batch["job_id"])):
        return
    await publisher.publish(
        {
            "job_id": str(first_batch["job_id"]),
            "type": "news_pipeline",
            "payload": first_batch["request"],
        },
        queue_name=aggregation_queue_name,
    )


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
