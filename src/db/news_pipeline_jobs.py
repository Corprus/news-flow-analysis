import json
from typing import Any

from psycopg import AsyncConnection


class NewsPipelineJobRepository:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    async def initialize(self) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await connection.execute(
                """
                CREATE TABLE IF NOT EXISTS news_pipeline_jobs (
                    id uuid PRIMARY KEY,
                    status text NOT NULL,
                    request jsonb NOT NULL,
                    result jsonb,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )

    async def mark_queued(self, job_id: str, request: dict[str, Any]) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                INSERT INTO news_pipeline_jobs (id, status, request, updated_at)
                VALUES (%s, 'queued', %s::jsonb, now())
                ON CONFLICT (id) DO UPDATE
                SET status = 'queued',
                    request = EXCLUDED.request,
                    updated_at = now()
                """,
                (job_id, json.dumps(request)),
            )

    async def mark_processing(self, job_id: str, request: dict[str, Any]) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                INSERT INTO news_pipeline_jobs (id, status, request, updated_at)
                VALUES (%s, 'processing', %s::jsonb, now())
                ON CONFLICT (id) DO UPDATE
                SET status = 'processing',
                    request = EXCLUDED.request,
                    updated_at = now()
                """,
                (job_id, json.dumps(request)),
            )

    async def update_result(self, job_id: str, result: dict[str, Any]) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_pipeline_jobs
                SET result = %s::jsonb,
                    updated_at = now()
                WHERE id = %s
                """,
                (json.dumps(result), job_id),
            )

    async def mark_done(self, job_id: str, result: dict[str, Any]) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                WITH incoming AS (
                    SELECT %s::jsonb AS payload
                )
                UPDATE news_pipeline_jobs
                SET status = 'done',
                    result = CASE
                        WHEN (
                            COALESCE(news_pipeline_jobs.result, '{}'::jsonb)
                            || incoming.payload
                        ) ? 'stage_details'
                        THEN jsonb_set(
                            jsonb_set(
                                COALESCE(news_pipeline_jobs.result, '{}'::jsonb)
                                    || incoming.payload,
                                '{stage_details,completed_rows}',
                                COALESCE(
                                    (
                                        COALESCE(news_pipeline_jobs.result, '{}'::jsonb)
                                        || incoming.payload
                                    )#>'{stage_details,total_rows}',
                                    (
                                        COALESCE(news_pipeline_jobs.result, '{}'::jsonb)
                                        || incoming.payload
                                    )#>'{stage_details,completed_rows}',
                                    '0'::jsonb
                                ),
                                true
                            ),
                            '{progress_percent}',
                            '100'::jsonb,
                            true
                        )
                        ELSE COALESCE(news_pipeline_jobs.result, '{}'::jsonb)
                            || incoming.payload
                    END,
                    updated_at = now()
                FROM incoming
                WHERE id = %s
                """,
                (json.dumps(result), job_id),
            )

    async def mark_failed(self, job_id: str, error: str) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_pipeline_jobs
                SET status = 'failed',
                    result = %s::jsonb,
                    updated_at = now()
                WHERE id = %s
                """,
                (json.dumps({"error": error}), job_id),
            )

    async def mark_dispatched_if_queued(self, job_id: str) -> bool:
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    UPDATE news_pipeline_jobs
                    SET result = jsonb_set(
                            COALESCE(result, '{}'::jsonb),
                            '{dispatched_at}',
                            to_jsonb(now()::text),
                            true
                        ),
                        updated_at = now()
                    WHERE id = %s
                      AND status = 'queued'
                      AND NOT (COALESCE(result, '{}'::jsonb) ? 'dispatched_at')
                    RETURNING id
                    """,
                    (job_id,),
                )
                row = await cursor.fetchone()
        return row is not None

    async def list_children(
        self,
        parent_job_id: str,
        *,
        mode: str | None = None,
    ) -> list[dict[str, Any]]:
        mode_filter = "AND request->>'mode' = %s" if mode is not None else ""
        params: tuple[Any, ...] = (
            (parent_job_id, mode) if mode is not None else (parent_job_id,)
        )
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""
                    SELECT id::text, status, request, result, created_at, updated_at
                    FROM news_pipeline_jobs
                    WHERE request->>'parent_job_id' = %s
                      {mode_filter}
                    ORDER BY
                        COALESCE((request->>'chunk_index')::int, (request->>'batch_index')::int, 0),
                        created_at
                    """,
                    params,
                )
                rows = await cursor.fetchall()

        return [
            {
                "job_id": row[0],
                "status": row[1],
                "request": row[2],
                "result": row[3],
                "created_at": row[4],
                "updated_at": row[5],
            }
            for row in rows
        ]

    async def order_article_ids(
        self,
        news_ids: list[str],
        *,
        organization_id: str | None,
    ) -> list[str]:
        if not news_ids:
            return []
        conditions = ["id = ANY(%s::uuid[])"]
        params: list[Any] = [news_ids]
        if organization_id is not None:
            conditions.append("organization_id = %s")
            params.append(organization_id)
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"""
                    SELECT id::text
                    FROM news_articles
                    WHERE {" AND ".join(conditions)}
                    ORDER BY published_at, id
                    """,
                    params,
                )
                rows = await cursor.fetchall()
        ordered_ids = [str(row[0]) for row in rows]
        found = set(ordered_ids)
        missing = [news_id for news_id in news_ids if news_id not in found]
        if missing:
            raise ValueError(f"News articles not found for pipeline job: {missing[:10]}")
        return ordered_ids

    async def get(self, job_id: str) -> dict[str, Any] | None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT id::text, status, request, result, created_at, updated_at
                    FROM news_pipeline_jobs
                    WHERE id = %s
                    """,
                    (job_id,),
                )
                row = await cursor.fetchone()

        if row is None:
            return None

        return {
            "job_id": row[0],
            "status": row[1],
            "request": row[2],
            "result": row[3],
            "created_at": row[4],
            "updated_at": row[5],
        }

    async def get_latest_by_request_type_and_user(
        self,
        request_type: str,
        user_id: str,
    ) -> dict[str, Any] | None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT id::text, status, request, result, created_at, updated_at
                    FROM news_pipeline_jobs
                    WHERE request->>'type' = %s
                      AND request->>'user_id' = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (request_type, user_id),
                )
                row = await cursor.fetchone()

        if row is None:
            return None

        return {
            "job_id": row[0],
            "status": row[1],
            "request": row[2],
            "result": row[3],
            "created_at": row[4],
            "updated_at": row[5],
        }

    async def get_latest_completed(self) -> dict[str, Any] | None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT status, request, result, created_at, updated_at
                    FROM news_pipeline_jobs
                    WHERE status IN ('done', 'failed')
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                )
                row = await cursor.fetchone()
        if row is None:
            return None
        return {
            "status": row[0],
            "request": row[1],
            "result": row[2],
            "created_at": row[3],
            "updated_at": row[4],
        }
