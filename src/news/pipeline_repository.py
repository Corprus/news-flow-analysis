from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
from psycopg import AsyncConnection

from final_pipeline.result import PipelineResult
from news.models import ArticleStatus, ArticleVisibility, SearchQueryStatus


def _vector_literal(embedding: list[float] | np.ndarray) -> str:
    return "[" + ",".join(str(float(value)) for value in embedding) + "]"


def _clean_scalar(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _group_search_items(
    items: list[dict[str, Any]],
    *,
    top_k: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        cluster_id = str(item["cluster_id"])
        grouped.setdefault(cluster_id, []).append(item)

    clusters: list[dict[str, Any]] = []
    for cluster_items in list(grouped.values())[:top_k]:
        representative = cluster_items[0]
        chronological_items = sorted(
            cluster_items,
            key=lambda item: (item.get("published_at") or "", item["rank"]),
        )
        published_dates = [
            item["published_at"]
            for item in chronological_items
            if item.get("published_at")
        ]
        clusters.append(
            {
                "cluster_id": representative["cluster_id"],
                "score": representative["score"],
                "representative_article_id": representative["article_id"],
                "representative_title": representative["title"],
                "article_count": len(cluster_items),
                "significant_count": sum(
                    item.get("novelty_label") == "significant"
                    for item in cluster_items
                ),
                "published_from": published_dates[0] if published_dates else None,
                "published_to": published_dates[-1] if published_dates else None,
                "items": chronological_items,
            }
        )
    return clusters


class NewsPipelineRepository:
    """PostgreSQL persistence boundary for full and incremental pipeline runs."""

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    async def load_articles(self, news_ids: list[str]) -> pd.DataFrame:
        if not news_ids:
            return pd.DataFrame()
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT
                        id::text AS news_id,
                        published_at,
                        COALESCE(topic, '<missing>') AS topic,
                        title,
                        content AS text,
                        COALESCE(url, '') AS url
                    FROM news_articles
                    WHERE id = ANY(%s::uuid[])
                      AND visibility = %s
                    """,
                    (news_ids, ArticleVisibility.PUBLIC.value),
                )
                rows = await cursor.fetchall()
                columns = [column.name for column in cursor.description]
        frame = pd.DataFrame(rows, columns=columns)
        found = set(frame.get("news_id", pd.Series(dtype=str)).astype(str))
        missing = [news_id for news_id in news_ids if news_id not in found]
        if missing:
            raise ValueError(f"News articles not found or not public: {missing[:10]}")
        invalid: list[str] = []
        for row in frame.to_dict("records"):
            missing_fields = [
                field
                for field in ("title", "text", "published_at")
                if row.get(field) is None
                or (isinstance(row.get(field), str) and not row[field].strip())
            ]
            if missing_fields:
                invalid.append(f"{row['news_id']}: {', '.join(missing_fields)}")
        if invalid:
            raise ValueError(
                "News articles are not ready for pipeline; missing required fields: "
                + "; ".join(invalid[:10])
            )
        return frame

    async def load_history(
        self,
        *,
        exclude_news_ids: list[str],
        embedding_model: str,
        embedding_model_revision: str,
    ) -> tuple[pd.DataFrame, np.ndarray]:
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT
                        a.id::text AS news_id,
                        a.published_at,
                        COALESCE(a.topic, '<missing>') AS topic,
                        a.title,
                        a.content AS text,
                        COALESCE(a.url, '') AS url,
                        s.cluster_id,
                        s.baseline_component_id,
                        s.assignment_method,
                        s.assignment_parent_news_id::text,
                        s.assignment_similarity,
                        s.attached_to_component_id,
                        e.embedding::text
                    FROM article_pipeline_state s
                    JOIN news_articles a ON a.id = s.article_id
                    JOIN article_pipeline_embeddings e ON e.article_id = a.id
                    WHERE NOT (a.id = ANY(%s::uuid[]))
                      AND a.status = %s
                      AND a.visibility = %s
                      AND e.model_name = %s
                      AND e.model_revision = %s
                    ORDER BY a.published_at, a.id
                    """,
                    (
                        exclude_news_ids,
                        ArticleStatus.PROCESSED.value,
                        ArticleVisibility.PUBLIC.value,
                        embedding_model,
                        embedding_model_revision,
                    ),
                )
                rows = await cursor.fetchall()
                columns = [column.name for column in cursor.description]
        if not rows:
            return (
                pd.DataFrame(
                    columns=[
                        "news_id",
                        "published_at",
                        "topic",
                        "title",
                        "text",
                        "url",
                        "cluster_id",
                        "baseline_component_id",
                        "assignment_method",
                        "assignment_parent_news_id",
                        "assignment_similarity",
                        "attached_to_component_id",
                    ]
                ),
                np.empty((0, 1024), dtype=np.float32),
            )
        frame = pd.DataFrame(rows, columns=columns)
        embeddings = np.asarray(
            [json.loads(value) for value in frame.pop("embedding")],
            dtype=np.float32,
        )
        return frame, embeddings

    async def mark_articles_processing(self, news_ids: list[str]) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_articles
                SET status = %s
                WHERE id = ANY(%s::uuid[])
                  AND visibility = %s
                """,
                (
                    ArticleStatus.PROCESSING.value,
                    news_ids,
                    ArticleVisibility.PUBLIC.value,
                ),
            )

    async def save_result(self, result: PipelineResult) -> None:
        prediction_by_id = (
            result.predictions.assign(news_id=result.predictions["news_id"].astype(str))
            .set_index("news_id")
            .to_dict("index")
        )
        assignment_by_id = (
            result.assignments.assign(news_id=result.assignments["news_id"].astype(str))
            .drop_duplicates("news_id", keep="last")
            .set_index("news_id")
            .to_dict("index")
        )
        embedding_by_id = {
            str(news_id): result.embeddings[position]
            for position, news_id in enumerate(result.embedding_ids)
        }
        persisted_ids = list(
            dict.fromkeys(
                [
                    *result.updated_ids,
                    *assignment_by_id.keys(),
                    *prediction_by_id.keys(),
                ]
            )
        )
        self._validate_result(
            result=result,
            persisted_ids=persisted_ids,
            prediction_by_id=prediction_by_id,
            assignment_by_id=assignment_by_id,
            embedding_by_id=embedding_by_id,
        )

        async with await AsyncConnection.connect(self._database_url) as connection:
            for news_id, embedding in embedding_by_id.items():
                await connection.execute(
                    """
                    INSERT INTO article_pipeline_embeddings (
                        id, article_id, model_name, model_revision, embedding, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s::vector, now())
                    ON CONFLICT (article_id, model_name, model_revision) DO UPDATE
                    SET embedding = EXCLUDED.embedding, created_at = now()
                    """,
                    (
                        str(uuid4()),
                        news_id,
                        result.versions.embedding_model,
                        result.versions.embedding_model_revision,
                        _vector_literal(embedding),
                    ),
                )

            existing = await self._load_existing_state(connection, persisted_ids)
            for news_id in persisted_ids:
                assignment = {**existing.get(news_id, {}), **assignment_by_id.get(news_id, {})}
                prediction = {**existing.get(news_id, {}), **prediction_by_id.get(news_id, {})}
                cluster_id = _clean_scalar(
                    assignment.get("cluster_id") or prediction.get("cluster_id")
                )
                baseline_component_id = _clean_scalar(
                    assignment.get("baseline_component_id")
                    or existing.get(news_id, {}).get("baseline_component_id")
                    or cluster_id
                )
                if cluster_id is None:
                    raise ValueError(f"Missing cluster assignment for news_id={news_id}")
                novelty_label = _clean_scalar(prediction.get("novelty_label"))
                p_significant = _clean_scalar(prediction.get("p_significant"))
                if novelty_label not in {"significant", "minor", "duplicate"}:
                    raise ValueError(
                        f"Missing or invalid novelty_label for news_id={news_id}"
                    )
                if p_significant is None or not 0.0 <= float(p_significant) <= 1.0:
                    raise ValueError(
                        f"Missing or invalid p_significant for news_id={news_id}"
                    )
                await connection.execute(
                    """
                    INSERT INTO article_pipeline_state (
                        article_id, cluster_id, baseline_component_id,
                        assignment_method, update_method,
                        assignment_parent_news_id, assignment_similarity,
                        attached_to_component_id, assignment_needs_review, late_arrival,
                        novelty_label, p_significant, novelty_comment, novelty_needs_review,
                        pipeline_version, embedding_model, embedding_model_revision,
                        novelty_model_version, config_version, processed_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                    )
                    ON CONFLICT (article_id) DO UPDATE SET
                        cluster_id = EXCLUDED.cluster_id,
                        baseline_component_id = EXCLUDED.baseline_component_id,
                        assignment_method = EXCLUDED.assignment_method,
                        update_method = EXCLUDED.update_method,
                        assignment_parent_news_id = EXCLUDED.assignment_parent_news_id,
                        assignment_similarity = EXCLUDED.assignment_similarity,
                        attached_to_component_id = EXCLUDED.attached_to_component_id,
                        assignment_needs_review = EXCLUDED.assignment_needs_review,
                        late_arrival = EXCLUDED.late_arrival,
                        novelty_label = EXCLUDED.novelty_label,
                        p_significant = EXCLUDED.p_significant,
                        novelty_comment = EXCLUDED.novelty_comment,
                        novelty_needs_review = EXCLUDED.novelty_needs_review,
                        pipeline_version = EXCLUDED.pipeline_version,
                        embedding_model = EXCLUDED.embedding_model,
                        embedding_model_revision = EXCLUDED.embedding_model_revision,
                        novelty_model_version = EXCLUDED.novelty_model_version,
                        config_version = EXCLUDED.config_version,
                        processed_at = now()
                    """,
                    (
                        news_id,
                        cluster_id,
                        baseline_component_id,
                        _clean_scalar(assignment.get("assignment_method")) or "baseline",
                        _clean_scalar(assignment.get("update_method")) or result.mode,
                        _clean_scalar(assignment.get("assignment_parent_news_id")),
                        _clean_scalar(assignment.get("assignment_similarity")),
                        _clean_scalar(assignment.get("attached_to_component_id")),
                        bool(_clean_scalar(assignment.get("assignment_needs_review")) or False),
                        bool(_clean_scalar(assignment.get("late_arrival")) or False),
                        novelty_label,
                        p_significant,
                        _clean_scalar(prediction.get("comment")),
                        bool(_clean_scalar(prediction.get("needs_review")) or False),
                        result.versions.pipeline_version,
                        result.versions.embedding_model,
                        result.versions.embedding_model_revision,
                        result.versions.novelty_model_version,
                        result.versions.config_version,
                    ),
                )

            await connection.execute(
                """
                UPDATE news_articles a
                SET status = %s,
                    novelty_score = s.p_significant,
                    metadata = COALESCE(a.metadata, '{}'::jsonb) - 'pipeline_error'
                FROM article_pipeline_state s
                WHERE a.id = s.article_id
                  AND a.id = ANY(%s::uuid[])
                """,
                (ArticleStatus.PROCESSED.value, persisted_ids),
            )

    async def mark_articles_error(self, news_ids: list[str], error: str) -> None:
        error_payload = json.dumps(
            {
                "message": error,
                "failed_at": datetime.now(UTC).isoformat(),
            }
        )
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_articles
                SET status = %s,
                    metadata = jsonb_set(
                        COALESCE(metadata, '{}'::jsonb),
                        '{pipeline_error}',
                        %s::jsonb,
                        true
                    )
                WHERE id = ANY(%s::uuid[])
                  AND visibility = %s
                """,
                (
                    ArticleStatus.ERROR.value,
                    error_payload,
                    news_ids,
                    ArticleVisibility.PUBLIC.value,
                ),
            )

    async def complete_search_query(
        self,
        *,
        query_id: str,
        query_embedding: list[float],
        filters: dict[str, Any],
        top_k: int,
        model_name: str,
        model_revision: str,
    ) -> dict[str, Any]:
        conditions = [
            "a.status = %s",
            "a.visibility = %s",
            "e.model_name = %s",
            "e.model_revision = %s",
        ]
        params: list[Any] = [
            ArticleStatus.PROCESSED.value,
            ArticleVisibility.PUBLIC.value,
            model_name,
            model_revision,
        ]
        filter_columns = {
            "language": "a.language = %s",
            "source_id": "a.source_id = %s",
            "published_from": "a.published_at >= %s",
            "published_to": "a.published_at <= %s",
        }
        for key, condition in filter_columns.items():
            if filters.get(key):
                conditions.append(condition)
                params.append(filters[key])
        if filters.get("min_novelty_score") is not None:
            conditions.append("a.novelty_score >= %s")
            params.append(float(filters["min_novelty_score"]))
        vector = _vector_literal(query_embedding)
        candidate_limit = min(max(top_k * 10, 100), 1000)
        query = f"""
            SELECT
                a.id::text, a.title, a.status, a.language, a.novelty_score,
                a.published_at, 1 - (e.embedding <=> %s::vector) AS score,
                COALESCE(s.cluster_id, a.id::text) AS cluster_id,
                COALESCE(s.manual_novelty_label, s.novelty_label) AS novelty_label,
                s.p_significant, a.url, a.summary, a.content
            FROM article_pipeline_embeddings e
            JOIN news_articles a ON a.id = e.article_id
            LEFT JOIN article_pipeline_state s ON s.article_id = a.id
            WHERE {" AND ".join(conditions)}
            ORDER BY e.embedding <=> %s::vector
            LIMIT %s
        """
        params = [vector, *params, vector, candidate_limit]
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()
            items = [
                {
                    "article_id": row[0],
                    "title": row[1],
                    "status": row[2],
                    "language": row[3],
                    "novelty_score": row[4],
                    "published_at": row[5].isoformat() if row[5] else None,
                    "score": float(row[6]),
                    "rank": rank,
                    "cluster_id": row[7],
                    "novelty_label": row[8],
                    "p_significant": row[9],
                    "url": row[10],
                    "summary": row[11],
                    "content": row[12],
                }
                for rank, row in enumerate(rows, start=1)
                if float(row[6]) >= float(filters.get("min_relevance", 0.5))
            ]
            clusters = _group_search_items(items, top_k=top_k)
            selected_cluster_ids = {
                cluster["cluster_id"] for cluster in clusters
            }
            selected_items = [
                item for item in items if item["cluster_id"] in selected_cluster_ids
            ]
            result = {"clusters": clusters, "items": selected_items}
            await connection.execute(
                """
                UPDATE news_search_queries
                SET status = %s, result = %s::jsonb, error = NULL, updated_at = now()
                WHERE id = %s
                """,
                (SearchQueryStatus.DONE.value, json.dumps(result), query_id),
            )
        return result

    async def mark_search_processing(self, query_id: str) -> None:
        await self._set_search_status(query_id, SearchQueryStatus.PROCESSING.value)

    async def mark_search_failed(self, query_id: str, error: str) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_search_queries
                SET status = %s, error = %s, updated_at = now()
                WHERE id = %s
                """,
                (SearchQueryStatus.FAILED.value, error, query_id),
            )

    async def _set_search_status(self, query_id: str, status: str) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                "UPDATE news_search_queries SET status = %s, updated_at = now() WHERE id = %s",
                (status, query_id),
            )

    @staticmethod
    def _validate_result(
        *,
        result: PipelineResult,
        persisted_ids: list[str],
        prediction_by_id: dict[str, dict[str, Any]],
        assignment_by_id: dict[str, dict[str, Any]],
        embedding_by_id: dict[str, np.ndarray],
    ) -> None:
        if not persisted_ids:
            raise ValueError("Pipeline result does not contain any article IDs")
        missing_embeddings = [
            news_id for news_id in result.requested_ids if news_id not in embedding_by_id
        ]
        if missing_embeddings:
            raise ValueError(f"Missing embeddings for requested news: {missing_embeddings[:10]}")
        missing_predictions = [
            news_id for news_id in result.requested_ids if news_id not in prediction_by_id
        ]
        if missing_predictions:
            raise ValueError(
                f"Missing novelty predictions for requested news: {missing_predictions[:10]}"
            )
        missing_assignments = [
            news_id
            for news_id in result.requested_ids
            if news_id not in assignment_by_id
            and not prediction_by_id.get(news_id, {}).get("cluster_id")
        ]
        if missing_assignments:
            raise ValueError(
                f"Missing cluster assignments for requested news: {missing_assignments[:10]}"
            )

    @staticmethod
    async def _load_existing_state(
        connection: AsyncConnection,
        news_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not news_ids:
            return {}
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT
                    article_id::text, cluster_id, baseline_component_id,
                    assignment_method, update_method, assignment_parent_news_id::text,
                    assignment_similarity, attached_to_component_id,
                    assignment_needs_review, late_arrival,
                    novelty_label, p_significant, novelty_comment, novelty_needs_review
                FROM article_pipeline_state
                WHERE article_id = ANY(%s::uuid[])
                """,
                (news_ids,),
            )
            rows = await cursor.fetchall()
            columns = [column.name for column in cursor.description]
        return {
            row[0]: dict(zip(columns[1:], row[1:], strict=True))
            for row in rows
        }
