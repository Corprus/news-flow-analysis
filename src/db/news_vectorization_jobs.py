import json
from typing import Any

from psycopg import AsyncConnection


class NewsVectorizationJobRepository:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    async def initialize(self) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute("CREATE EXTENSION IF NOT EXISTS vector")
            await connection.execute(
                """
                CREATE TABLE IF NOT EXISTS news_vectorization_jobs (
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
                INSERT INTO news_vectorization_jobs (id, status, request, updated_at)
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
                INSERT INTO news_vectorization_jobs (id, status, request, updated_at)
                VALUES (%s, 'processing', %s::jsonb, now())
                ON CONFLICT (id) DO UPDATE
                SET status = 'processing',
                    request = EXCLUDED.request,
                    updated_at = now()
                """,
                (job_id, json.dumps(request)),
            )

    async def mark_done(self, job_id: str, result: dict[str, Any]) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_vectorization_jobs
                SET status = 'done',
                    result = %s::jsonb,
                    updated_at = now()
                WHERE id = %s
                """,
                (json.dumps(result), job_id),
            )

    async def mark_failed(self, job_id: str, error: str) -> None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            await connection.execute(
                """
                UPDATE news_vectorization_jobs
                SET status = 'failed',
                    result = %s::jsonb,
                    updated_at = now()
                WHERE id = %s
                """,
                (json.dumps({"error": error}), job_id),
            )

    async def get(self, job_id: str) -> dict[str, Any] | None:
        async with await AsyncConnection.connect(self._database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT id::text, status, request, result, created_at, updated_at
                    FROM news_vectorization_jobs
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
