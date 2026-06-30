import json
from collections.abc import Iterator

from prometheus_client import REGISTRY, Counter, Gauge, Histogram
from prometheus_client.core import GaugeMetricFamily
from psycopg import connect

IMPORT_JOB_DURATION = Histogram(
    "news_flow_import_job_duration_seconds",
    "News import job duration in seconds.",
    ("status",),
    buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600),
)
IMPORT_JOBS = Counter(
    "news_flow_import_jobs_total",
    "Number of completed news import jobs.",
    ("status",),
)
IMPORT_JOBS_IN_PROGRESS = Gauge(
    "news_flow_import_jobs_in_progress",
    "Number of news import jobs currently running.",
)
IMPORT_ROWS = Counter(
    "news_flow_import_rows_total",
    "Number of rows seen by news import jobs.",
    ("kind",),
)


_database_collector_registered = False
IMPORT_JOB_STATUSES = ("queued", "processing", "done", "failed")


class ApiDatabaseCollector:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    def collect(self) -> Iterator[GaugeMetricFamily]:
        import_jobs = GaugeMetricFamily(
            "news_flow_import_jobs_db",
            "Number of news import jobs persisted in PostgreSQL.",
            labels=["status"],
        )
        import_rows = GaugeMetricFamily(
            "news_flow_import_rows_db",
            "Number of news import rows persisted in PostgreSQL job results.",
            labels=["kind"],
        )
        import_in_progress = GaugeMetricFamily(
            "news_flow_import_jobs_in_progress_db",
            "Number of queued or processing news import jobs in PostgreSQL.",
        )
        import_progress = GaugeMetricFamily(
            "news_flow_import_latest_progress_percent",
            "Progress percent of the latest news import job.",
            labels=["status"],
        )
        pipeline_queue_articles = GaugeMetricFamily(
            "news_flow_pipeline_queue_articles",
            "Number of article IDs stored in pipeline jobs by status and mode.",
            labels=["status", "mode"],
        )
        pipeline_queue_jobs = GaugeMetricFamily(
            "news_flow_pipeline_queue_jobs",
            "Number of pipeline jobs by status and mode.",
            labels=["status", "mode"],
        )

        with connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status, count(*)
                    FROM news_pipeline_jobs
                    WHERE request->>'type' = 'news_import'
                    GROUP BY status
                    """
                )
                in_progress = 0
                job_counts = dict.fromkeys(IMPORT_JOB_STATUSES, 0)
                for status, count in cursor.fetchall():
                    status = str(status)
                    value = int(count)
                    job_counts[status] = value
                    if status in {"queued", "processing"}:
                        in_progress += value
                for status, value in job_counts.items():
                    import_jobs.add_metric([status], value)
                import_in_progress.add_metric([], in_progress)

                cursor.execute(
                    """
                    SELECT result
                    FROM news_pipeline_jobs
                    WHERE request->>'type' = 'news_import'
                      AND result IS NOT NULL
                    """
                )
                row_totals = {"total": 0, "created": 0, "duplicate": 0, "published": 0}
                for (result,) in cursor.fetchall():
                    result = _coerce_json(result)
                    row_totals["total"] += int(result.get("total_rows") or 0)
                    row_totals["created"] += int(result.get("created_count") or 0)
                    row_totals["duplicate"] += int(result.get("duplicate_count") or 0)
                    row_totals["published"] += int(result.get("published_count") or 0)
                for kind, value in row_totals.items():
                    import_rows.add_metric([kind], value)

                cursor.execute(
                    """
                    SELECT status, result
                    FROM news_pipeline_jobs
                    WHERE request->>'type' = 'news_import'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                )
                latest = cursor.fetchone()
                if latest is not None:
                    status, result = latest
                    result = _coerce_json(result)
                    import_progress.add_metric(
                        [str(status)],
                        float(result.get("progress_percent") or 0),
                    )

                cursor.execute(
                    """
                    SELECT
                        status,
                        COALESCE(request->>'mode', request->>'type', 'unknown') AS mode,
                        count(*) AS jobs,
                        COALESCE(sum(jsonb_array_length(request->'news_ids')), 0) AS articles
                    FROM news_pipeline_jobs
                    WHERE request ? 'news_ids'
                      AND COALESCE(request->>'mode', '') <> 'incremental_chunked'
                    GROUP BY status, mode
                    """
                )
                for status, mode, jobs, articles in cursor.fetchall():
                    labels = [str(status), str(mode)]
                    pipeline_queue_jobs.add_metric(labels, int(jobs))
                    pipeline_queue_articles.add_metric(labels, int(articles))

        yield import_jobs
        yield import_rows
        yield import_in_progress
        yield import_progress
        yield pipeline_queue_jobs
        yield pipeline_queue_articles


def register_api_database_metrics(database_url: str) -> None:
    global _database_collector_registered
    if _database_collector_registered:
        return
    REGISTRY.register(ApiDatabaseCollector(database_url))
    _database_collector_registered = True


def _coerce_json(value) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)
