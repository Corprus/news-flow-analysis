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
from news.search_results import group_search_items


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


class NewsPipelineRepository:
    """PostgreSQL persistence boundary for full and incremental pipeline runs."""

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    async def count_processed_articles(self) -> int:
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM news_articles WHERE status = %s",
                    (ArticleStatus.PROCESSED.value,),
                )
                row = await cursor.fetchone()
        return int(row[0]) if row is not None else 0

    async def load_articles(
        self,
        news_ids: list[str],
        organization_id: str | None,
    ) -> pd.DataFrame:
        if not news_ids:
            return pd.DataFrame()
        organization_id = await self.resolve_article_organization_id(
            news_ids,
            organization_id,
        )
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT
                        id::text AS news_id,
                        organization_id::text AS organization_id,
                        published_at,
                        COALESCE(topic, '<missing>') AS topic,
                        title,
                        content AS text,
                        COALESCE(url, '') AS url
                    FROM news_articles
                    WHERE id = ANY(%s::uuid[])
                      AND visibility = %s
                      AND organization_id = %s
                    """,
                    (news_ids, ArticleVisibility.PUBLIC.value, organization_id),
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
        organization_id: str,
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
                        a.organization_id::text AS organization_id,
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
                      AND a.organization_id = %s
                      AND e.model_name = %s
                      AND e.model_revision = %s
                    ORDER BY a.published_at, a.id
                    """,
                    (
                        exclude_news_ids,
                        ArticleStatus.PROCESSED.value,
                        ArticleVisibility.PUBLIC.value,
                        organization_id,
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
                        "organization_id",
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
            affected_cluster_ids = self._collect_affected_cluster_ids(
                existing=existing,
                assignment_by_id=assignment_by_id,
                prediction_by_id=prediction_by_id,
            )
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
            affected_cluster_ids.update(
                await self._load_current_cluster_ids(connection, persisted_ids)
            )
            await self._refresh_cluster_summaries(
                connection,
                cluster_ids=affected_cluster_ids,
                embedding_model=result.versions.embedding_model,
                embedding_model_revision=result.versions.embedding_model_revision,
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
        organization_id: str | None,
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
        if organization_id is not None:
            conditions.append("a.organization_id = %s")
            params.append(organization_id)
        vector = _vector_literal(query_embedding)
        candidate_limit = min(max(top_k * 10, 100), 1000)
        query = f"""
            SELECT
                a.id::text, a.title, a.status, a.language, a.novelty_score,
                a.organization_id::text,
                a.published_at, 1 - (e.embedding <=> %s::vector) AS score,
                COALESCE(s.cluster_id, a.id::text) AS cluster_id,
                COALESCE(s.manual_novelty_label, s.novelty_label) AS novelty_label,
                s.p_significant, a.url, a.summary, a.content,
                cs.representative_article_id::text,
                representative.title,
                cs.article_count,
                cs.started_at,
                cs.last_seen_at
            FROM article_pipeline_embeddings e
            JOIN news_articles a ON a.id = e.article_id
            LEFT JOIN article_pipeline_state s ON s.article_id = a.id
            LEFT JOIN news_cluster_summaries cs
                ON cs.cluster_id = COALESCE(s.cluster_id, a.id::text)
               AND cs.organization_id = a.organization_id
            LEFT JOIN news_articles representative
                ON representative.id = cs.representative_article_id
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
                    "organization_id": row[5],
                    "published_at": row[6].isoformat() if row[6] else None,
                    "score": float(row[7]),
                    "rank": rank,
                    "cluster_id": row[8],
                    "novelty_label": row[9],
                    "p_significant": row[10],
                    "url": row[11],
                    "summary": row[12],
                    "content": row[13],
                    "cluster_representative_article_id": row[14],
                    "cluster_representative_title": row[15],
                    "cluster_article_count": row[16],
                    "cluster_published_from": row[17].isoformat() if row[17] else None,
                    "cluster_published_to": row[18].isoformat() if row[18] else None,
                }
                for rank, row in enumerate(rows, start=1)
                if float(row[7]) >= float(filters.get("min_relevance", 0.5))
            ]
            clusters = group_search_items(items, top_k=top_k)
            selected_cluster_ids = [cluster["cluster_id"] for cluster in clusters]
            selected_items = await self._load_cluster_items(
                connection,
                cluster_ids=selected_cluster_ids,
                organization_id=organization_id,
                matched_items=items,
            )
            if selected_items:
                cluster_scores = {
                    cluster["cluster_id"]: cluster.get("score")
                    for cluster in clusters
                    if cluster.get("score") is not None
                }
                clusters = group_search_items(
                    selected_items,
                    top_k=len(selected_cluster_ids),
                )
                for cluster in clusters:
                    score = cluster_scores.get(cluster["cluster_id"])
                    if score is not None:
                        cluster["score"] = score
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

    @staticmethod
    async def _load_cluster_items(
        connection: AsyncConnection,
        *,
        cluster_ids: list[str],
        organization_id: str | None,
        matched_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not cluster_ids:
            return []
        conditions = [
            "a.status = %s",
            "a.visibility = %s",
            "COALESCE(s.cluster_id, a.id::text) = ANY(%s::text[])",
        ]
        params: list[Any] = [
            ArticleStatus.PROCESSED.value,
            ArticleVisibility.PUBLIC.value,
            cluster_ids,
        ]
        if organization_id is not None:
            conditions.append("a.organization_id = %s")
            params.append(organization_id)
        query = f"""
            SELECT
                a.id::text, a.title, a.status, a.language, a.novelty_score,
                a.organization_id::text,
                a.published_at,
                COALESCE(s.cluster_id, a.id::text) AS cluster_id,
                COALESCE(s.manual_novelty_label, s.novelty_label) AS novelty_label,
                s.p_significant, a.url, a.summary, a.content,
                cs.representative_article_id::text,
                representative.title,
                cs.article_count,
                cs.started_at,
                cs.last_seen_at
            FROM news_articles a
            LEFT JOIN article_pipeline_state s ON s.article_id = a.id
            LEFT JOIN news_cluster_summaries cs
                ON cs.cluster_id = COALESCE(s.cluster_id, a.id::text)
               AND cs.organization_id = a.organization_id
            LEFT JOIN news_articles representative
                ON representative.id = cs.representative_article_id
            WHERE {" AND ".join(conditions)}
            ORDER BY
                array_position(%s::text[], COALESCE(s.cluster_id, a.id::text)),
                a.published_at,
                a.id
        """
        params.append(cluster_ids)
        matched_by_id = {item["article_id"]: item for item in matched_items}
        async with connection.cursor() as cursor:
            await cursor.execute(query, params)
            rows = await cursor.fetchall()
        items: list[dict[str, Any]] = []
        for rank, row in enumerate(rows, start=1):
            item = {
                "article_id": row[0],
                "title": row[1],
                "status": row[2],
                "language": row[3],
                "novelty_score": row[4],
                "organization_id": row[5],
                "published_at": row[6].isoformat() if row[6] else None,
                "rank": rank,
                "cluster_id": row[7],
                "novelty_label": row[8],
                "p_significant": row[9],
                "url": row[10],
                "summary": row[11],
                "content": row[12],
                "cluster_representative_article_id": row[13],
                "cluster_representative_title": row[14],
                "cluster_article_count": row[15],
                "cluster_published_from": row[16].isoformat() if row[16] else None,
                "cluster_published_to": row[17].isoformat() if row[17] else None,
            }
            matched = matched_by_id.get(item["article_id"])
            if matched and matched.get("score") is not None:
                item["score"] = matched["score"]
            items.append(item)
        return items

    async def resolve_article_organization_id(
        self,
        news_ids: list[str],
        organization_id: str | None,
    ) -> str:
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT DISTINCT organization_id::text
                    FROM news_articles
                    WHERE id = ANY(%s::uuid[])
                    """,
                    (news_ids,),
                )
                rows = await cursor.fetchall()
        found_organization_ids = {str(row[0]) for row in rows}
        if not found_organization_ids:
            raise ValueError(f"News articles not found: {news_ids[:10]}")
        if len(found_organization_ids) != 1:
            raise ValueError("Pipeline job cannot mix organizations")
        found_organization_id = found_organization_ids.pop()
        if organization_id is not None and organization_id != found_organization_id:
            raise ValueError("Pipeline job organization_id does not match news articles")
        return found_organization_id

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

    @staticmethod
    def _collect_affected_cluster_ids(
        *,
        existing: dict[str, dict[str, Any]],
        assignment_by_id: dict[str, dict[str, Any]],
        prediction_by_id: dict[str, dict[str, Any]],
    ) -> set[str]:
        cluster_ids: set[str] = set()
        for row in existing.values():
            cluster_id = _clean_scalar(row.get("cluster_id"))
            if cluster_id:
                cluster_ids.add(str(cluster_id))
        for row in [*assignment_by_id.values(), *prediction_by_id.values()]:
            for key in ("cluster_id", "previous_cluster_id"):
                cluster_id = _clean_scalar(row.get(key))
                if cluster_id:
                    cluster_ids.add(str(cluster_id))
            for key in ("merged_cluster_ids",):
                value = row.get(key)
                if isinstance(value, list):
                    cluster_ids.update(str(item) for item in value if item)
        return cluster_ids

    @staticmethod
    async def _load_current_cluster_ids(
        connection: AsyncConnection,
        news_ids: list[str],
    ) -> set[str]:
        if not news_ids:
            return set()
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT DISTINCT cluster_id
                FROM article_pipeline_state
                WHERE article_id = ANY(%s::uuid[])
                """,
                (news_ids,),
            )
            rows = await cursor.fetchall()
        return {str(row[0]) for row in rows if row[0]}

    @staticmethod
    async def _refresh_cluster_summaries(
        connection: AsyncConnection,
        *,
        cluster_ids: set[str],
        embedding_model: str,
        embedding_model_revision: str,
    ) -> None:
        if not cluster_ids:
            return
        ordered_cluster_ids = sorted(cluster_ids)
        await connection.execute(
            "DELETE FROM news_cluster_summaries WHERE cluster_id = ANY(%s::text[])",
            (ordered_cluster_ids,),
        )
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT
                    a.organization_id::text,
                    s.cluster_id,
                    a.id::text,
                    a.published_at,
                    COALESCE(s.manual_novelty_label, s.novelty_label) AS novelty_label,
                    e.embedding::text
                FROM article_pipeline_state s
                JOIN news_articles a ON a.id = s.article_id
                JOIN article_pipeline_embeddings e ON e.article_id = a.id
                WHERE s.cluster_id = ANY(%s::text[])
                  AND a.status = %s
                  AND a.visibility = %s
                  AND e.model_name = %s
                  AND e.model_revision = %s
                ORDER BY s.cluster_id, a.published_at, a.id
                """,
                (
                    ordered_cluster_ids,
                    ArticleStatus.PROCESSED.value,
                    ArticleVisibility.PUBLIC.value,
                    embedding_model,
                    embedding_model_revision,
                ),
            )
            rows = await cursor.fetchall()

        rows_by_cluster: dict[tuple[str, str], list[tuple[Any, ...]]] = {}
        for row in rows:
            rows_by_cluster.setdefault((str(row[0]), str(row[1])), []).append(row)

        for (organization_id, cluster_id), cluster_rows in rows_by_cluster.items():
            embeddings = np.asarray(
                [json.loads(row[5]) for row in cluster_rows],
                dtype=np.float32,
            )
            norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
            normalized_embeddings = np.divide(
                embeddings,
                norms,
                out=np.zeros_like(embeddings),
                where=norms > 0,
            )
            centroid = normalized_embeddings.mean(axis=0)
            centroid_norm = float(np.linalg.norm(centroid))
            if centroid_norm > 0:
                centroid = centroid / centroid_norm
            similarities = normalized_embeddings @ centroid
            novelty_priority = {
                "significant": 2,
                "minor": 1,
                "duplicate": 0,
            }
            representative_position = min(
                range(len(cluster_rows)),
                key=lambda position: (
                    -float(similarities[position]),
                    -novelty_priority.get(str(cluster_rows[position][4]), 0),
                    cluster_rows[position][3],
                    str(cluster_rows[position][2]),
                ),
            )
            representative = cluster_rows[representative_position]
            dates = [row[3] for row in cluster_rows if row[3] is not None]
            started_at = min(dates) if dates else None
            last_seen_at = max(dates) if dates else None
            await connection.execute(
                """
                INSERT INTO news_cluster_summaries (
                    organization_id, cluster_id, representative_article_id, article_count,
                    started_at, last_seen_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (organization_id, cluster_id) DO UPDATE SET
                    representative_article_id = EXCLUDED.representative_article_id,
                    article_count = EXCLUDED.article_count,
                    started_at = EXCLUDED.started_at,
                    last_seen_at = EXCLUDED.last_seen_at,
                    updated_at = now()
                """,
                (
                    organization_id,
                    cluster_id,
                    representative[2],
                    len(cluster_rows),
                    started_at,
                    last_seen_at,
                ),
            )
