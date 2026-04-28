from __future__ import annotations

import json
import math
from typing import Any
from uuid import uuid4

from psycopg import AsyncConnection

from news.models import ArticleStatus, SearchQueryStatus


def _vector_literal(embedding: list[float]) -> str:
    return "[" + ",".join(str(float(value)) for value in embedding) + "]"


def _cosine_score(left: list[float], right: list[float]) -> float:
    numerator = sum(a * b for a, b in zip(left, right, strict=False))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


class NewsVectorizationRepository:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    async def mark_article_processing(self, article_id: str) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_articles
                SET status = %s
                WHERE id = %s
                """,
                (ArticleStatus.PROCESSING.value, article_id),
            )

    async def save_article_embedding(
        self,
        *,
        article_id: str,
        embedding: list[float],
        model_name: str,
        model_revision: str,
    ) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                INSERT INTO article_embeddings (
                    id,
                    article_id,
                    model_name,
                    model_revision,
                    embedding,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s::vector, now())
                ON CONFLICT (article_id, model_name, model_revision) DO UPDATE
                SET embedding = EXCLUDED.embedding,
                    created_at = now()
                """,
                (
                    str(uuid4()),
                    article_id,
                    model_name,
                    model_revision,
                    _vector_literal(embedding),
                ),
            )
            await connection.execute(
                """
                UPDATE news_articles
                SET status = %s
                WHERE id = %s
                """,
                (ArticleStatus.VECTORIZED.value, article_id),
            )

    async def mark_article_failed(self, article_id: str, error: str) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_articles
                SET status = %s,
                    metadata = jsonb_set(
                        COALESCE(metadata, '{}'::jsonb),
                        '{vectorization_error}',
                        to_jsonb(%s::text),
                        true
                    )
                WHERE id = %s
                """,
                (ArticleStatus.FAILED.value, error, article_id),
            )

    async def mark_search_processing(self, query_id: str) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_search_queries
                SET status = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (SearchQueryStatus.PROCESSING.value, query_id),
            )

    async def complete_search_query(
        self,
        *,
        query_id: str,
        query_embedding: list[float],
        filters: dict[str, Any],
        top_k: int,
    ) -> dict[str, Any]:
        rows = await self._candidate_rows(filters)
        scored = []
        for row in rows:
            article_id, title, status, language, novelty_score, published_at, embedding = row
            score = _cosine_score(query_embedding, embedding)
            scored.append(
                {
                    "article_id": article_id,
                    "title": title,
                    "status": status,
                    "language": language,
                    "novelty_score": novelty_score,
                    "published_at": published_at.isoformat() if published_at else None,
                    "score": score,
                }
            )

        scored.sort(key=lambda item: item["score"], reverse=True)
        items = [
            {**item, "rank": rank}
            for rank, item in enumerate(scored[:top_k], start=1)
        ]
        result = {"items": items}

        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_search_queries
                SET status = %s,
                    result = %s::jsonb,
                    error = NULL,
                    updated_at = now()
                WHERE id = %s
                """,
                (SearchQueryStatus.DONE.value, json.dumps(result), query_id),
            )
        return result

    async def mark_search_failed(self, query_id: str, error: str) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_search_queries
                SET status = %s,
                    error = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (SearchQueryStatus.FAILED.value, error, query_id),
            )

    async def _candidate_rows(self, filters: dict[str, Any]) -> list[tuple]:
        conditions = ["a.status = %s"]
        params: list[Any] = [ArticleStatus.VECTORIZED.value]

        if filters.get("language"):
            conditions.append("a.language = %s")
            params.append(filters["language"])
        if filters.get("source_id"):
            conditions.append("a.source_id = %s")
            params.append(filters["source_id"])
        if filters.get("submitted_by_user_id"):
            conditions.append("a.submitted_by_user_id = %s")
            params.append(filters["submitted_by_user_id"])
        if filters.get("published_from"):
            conditions.append("a.published_at >= %s")
            params.append(filters["published_from"])
        if filters.get("published_to"):
            conditions.append("a.published_at <= %s")
            params.append(filters["published_to"])
        if filters.get("min_novelty_score"):
            conditions.append("a.novelty_score >= %s")
            params.append(float(filters["min_novelty_score"]))

        query = f"""
            SELECT
                a.id::text,
                a.title,
                a.status,
                a.language,
                a.novelty_score,
                a.published_at,
                e.embedding::text
            FROM article_embeddings e
            JOIN news_articles a ON a.id = e.article_id
            WHERE {" AND ".join(conditions)}
        """
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()

        return [
            (*row[:6], json.loads(row[6]))
            for row in rows
        ]
