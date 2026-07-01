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
PIPELINE_JOB_STATUSES = ("queued", "processing", "done", "failed")
PIPELINE_JOB_MODES = (
    "incremental",
    "vectorize",
    "aggregate",
    "full",
    "news_search",
)
STAGE_DETAIL_EXCLUDED_KEYS = {
    "article_count",
    "completed_rows",
    "total_rows",
    "elapsed_seconds",
    "progress_percent",
    "stage_elapsed_seconds",
    "pipeline_elapsed_seconds",
    "history_rows",
    "historical_rows",
    "new_rows",
}


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
        pipeline_stage_rows = GaugeMetricFamily(
            "news_flow_pipeline_stage_rows",
            "Number of rows reported by active pipeline stage details.",
            labels=["mode", "stage", "kind"],
        )
        pipeline_stage_progress = GaugeMetricFamily(
            "news_flow_pipeline_stage_progress_percent",
            "Progress percent reported by active pipeline stage details.",
            labels=["mode", "stage"],
        )
        pipeline_stage_elapsed = GaugeMetricFamily(
            "news_flow_pipeline_stage_elapsed_seconds",
            "Elapsed seconds reported by active pipeline stage details.",
            labels=["mode", "stage"],
        )
        pipeline_job_duration_db = GaugeMetricFamily(
            "news_flow_pipeline_job_duration_db_seconds",
            "Persisted pipeline job duration statistics from PostgreSQL.",
            labels=["mode", "status", "stat"],
        )
        pipeline_latest_batch_duration = GaugeMetricFamily(
            "news_flow_pipeline_latest_batch_duration_seconds",
            "Duration of child jobs for the latest chunked pipeline parent.",
            labels=["mode", "job_index", "status"],
        )
        pipeline_latest_batch_throughput = GaugeMetricFamily(
            "news_flow_pipeline_latest_batch_throughput",
            "Throughput of child jobs for the latest chunked pipeline parent.",
            labels=["mode", "job_index", "status"],
        )
        pipeline_latest_batch_history_rows = GaugeMetricFamily(
            "news_flow_pipeline_latest_batch_history_rows",
            "History rows seen by aggregate child jobs for the latest chunked parent.",
            labels=["mode", "job_index", "status"],
        )
        pipeline_latest_stage_duration = GaugeMetricFamily(
            "news_flow_pipeline_latest_stage_duration_seconds",
            "Stage duration for child jobs of the latest chunked pipeline parent.",
            labels=["mode", "stage", "job_index", "status"],
        )
        pipeline_latest_stage_detail = GaugeMetricFamily(
            "news_flow_pipeline_latest_stage_detail",
            (
                "Numeric details reported by stages for child jobs of the latest "
                "chunked pipeline parent."
            ),
            labels=["mode", "stage", "detail", "job_index", "status"],
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
                      AND status <> 'failed'
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
                pipeline_counts = {
                    (status, mode): (int(jobs), int(articles))
                    for status, mode, jobs, articles in cursor.fetchall()
                }
                for status in PIPELINE_JOB_STATUSES:
                    for mode in PIPELINE_JOB_MODES:
                        jobs, articles = pipeline_counts.get((status, mode), (0, 0))
                        labels = [status, mode]
                        pipeline_queue_jobs.add_metric(labels, jobs)
                        pipeline_queue_articles.add_metric(labels, articles)

                for (status, mode), (jobs, articles) in pipeline_counts.items():
                    if status in PIPELINE_JOB_STATUSES and mode in PIPELINE_JOB_MODES:
                        continue
                    labels = [str(status), str(mode)]
                    pipeline_queue_jobs.add_metric(labels, jobs)
                    pipeline_queue_articles.add_metric(labels, articles)

                cursor.execute(
                    """
                    WITH active_stages AS (
                        SELECT
                            COALESCE(request->>'mode', request->>'type', 'unknown') AS mode,
                            COALESCE(result->>'stage', 'unknown') AS stage,
                            result->'stage_details' AS details
                        FROM news_pipeline_jobs
                        WHERE (
                                status = 'processing'
                                OR (
                                    status = 'done'
                                    AND updated_at >= now() - interval '2 minutes'
                                )
                            )
                          AND result ? 'stage_details'
                          AND jsonb_typeof(result->'stage_details') = 'object'
                    )
                    SELECT
                        mode,
                        stage,
                        SUM(
                            CASE
                                WHEN (details->>'completed_rows') ~ '^[0-9]+(\\.[0-9]+)?$'
                                THEN (details->>'completed_rows')::double precision
                                ELSE 0
                            END
                        ) AS completed_rows,
                        SUM(
                            CASE
                                WHEN (details->>'total_rows') ~ '^[0-9]+(\\.[0-9]+)?$'
                                THEN (details->>'total_rows')::double precision
                                ELSE 0
                            END
                        ) AS total_rows,
                        MAX(
                            CASE
                                WHEN (details->>'elapsed_seconds') ~ '^[0-9]+(\\.[0-9]+)?$'
                                THEN (details->>'elapsed_seconds')::double precision
                                ELSE 0
                            END
                        ) AS elapsed_seconds
                    FROM active_stages
                    GROUP BY mode, stage
                    """
                )
                for mode, stage, completed, total, elapsed in cursor.fetchall():
                    completed = float(completed or 0)
                    total = float(total or 0)
                    elapsed = float(elapsed or 0)
                    labels = [str(mode), str(stage)]
                    pipeline_stage_rows.add_metric([*labels, "completed"], completed)
                    pipeline_stage_rows.add_metric([*labels, "total"], total)
                    pipeline_stage_elapsed.add_metric(labels, elapsed)
                    if total > 0:
                        pipeline_stage_progress.add_metric(labels, completed / total * 100)

                cursor.execute(
                    """
                    SELECT
                        COALESCE(request->>'mode', request->>'type', 'unknown') AS mode,
                        status,
                        EXTRACT(EPOCH FROM updated_at - created_at)::double precision
                            AS duration_seconds
                    FROM news_pipeline_jobs
                    WHERE status IN ('done', 'failed')
                      AND updated_at IS NOT NULL
                      AND created_at IS NOT NULL
                    """
                )
                duration_values: dict[tuple[str, str], list[float]] = {}
                for mode, status, duration in cursor.fetchall():
                    if duration is None:
                        continue
                    duration_values.setdefault((str(mode), str(status)), []).append(
                        float(duration)
                    )
                for (mode, status), values in duration_values.items():
                    for stat, value in _duration_stats(values).items():
                        pipeline_job_duration_db.add_metric(
                            [mode, status, stat],
                            value,
                        )

                cursor.execute(
                    """
                    SELECT id::text
                    FROM news_pipeline_jobs
                    WHERE request->>'mode' = 'incremental_chunked'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                )
                latest_parent = cursor.fetchone()
                if latest_parent is not None:
                    parent_id = str(latest_parent[0])
                    cursor.execute(
                        """
                        SELECT
                            COALESCE(request->>'mode', 'unknown') AS mode,
                            COALESCE(
                                request->>'batch_index',
                                request->>'chunk_index',
                                '0'
                            ) AS job_index,
                            status,
                            request,
                            result,
                            EXTRACT(EPOCH FROM updated_at - created_at)::double precision
                                AS duration_seconds
                        FROM news_pipeline_jobs
                        WHERE request->>'parent_job_id' = %s
                          AND request->>'mode' IN ('vectorize', 'aggregate')
                        ORDER BY
                            COALESCE(
                                (request->>'batch_index')::int,
                                (request->>'chunk_index')::int,
                                0
                            )
                        """,
                        (parent_id,),
                    )
                    for mode, job_index, status, request, result, duration in cursor.fetchall():
                        mode = str(mode)
                        job_index = str(job_index)
                        status = str(status)
                        request = _coerce_json(request)
                        result = _coerce_json(result)
                        duration = float(duration or 0)
                        labels = [mode, job_index, status]
                        pipeline_latest_batch_duration.add_metric(labels, duration)
                        article_count = len(request.get("news_ids") or [])
                        if duration > 0 and article_count > 0:
                            pipeline_latest_batch_throughput.add_metric(
                                labels,
                                article_count / duration,
                            )
                        stage_timings = result.get("stage_timings")
                        if not isinstance(stage_timings, dict):
                            stage_timings = {}
                        history_rows = _history_rows_from_stage_timings(stage_timings)
                        if history_rows is not None:
                            pipeline_latest_batch_history_rows.add_metric(
                                labels,
                                float(history_rows),
                            )
                        for stage, stage_result in stage_timings.items():
                            if not isinstance(stage_result, dict):
                                continue
                            stage_duration = _float_or_none(
                                stage_result.get("stage_elapsed_seconds")
                            )
                            if stage_duration is None:
                                continue
                            pipeline_latest_stage_duration.add_metric(
                                [mode, str(stage), job_index, status],
                                stage_duration,
                            )
                            for detail, value in stage_result.items():
                                if detail in STAGE_DETAIL_EXCLUDED_KEYS:
                                    continue
                                numeric_value = _float_or_none(value)
                                if numeric_value is None:
                                    continue
                                pipeline_latest_stage_detail.add_metric(
                                    [mode, str(stage), str(detail), job_index, status],
                                    numeric_value,
                                )

        yield import_jobs
        yield import_rows
        yield import_in_progress
        yield import_progress
        yield pipeline_queue_jobs
        yield pipeline_queue_articles
        yield pipeline_stage_rows
        yield pipeline_stage_progress
        yield pipeline_stage_elapsed
        yield pipeline_job_duration_db
        yield pipeline_latest_batch_duration
        yield pipeline_latest_batch_throughput
        yield pipeline_latest_batch_history_rows
        yield pipeline_latest_stage_duration
        yield pipeline_latest_stage_detail


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


def _duration_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)
    return {
        "avg": sum(ordered) / len(ordered),
        "max": ordered[-1],
        "p50": _quantile(ordered, 0.5),
        "p95": _quantile(ordered, 0.95),
    }


def _quantile(ordered_values: list[float], quantile: float) -> float:
    if len(ordered_values) == 1:
        return ordered_values[0]
    position = (len(ordered_values) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered_values) - 1)
    fraction = position - lower
    return ordered_values[lower] * (1 - fraction) + ordered_values[upper] * fraction


def _float_or_none(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _history_rows_from_stage_timings(stage_timings: dict) -> float | None:
    for stage in ("cluster_assignment", "prepare_history"):
        stage_result = stage_timings.get(stage)
        if not isinstance(stage_result, dict):
            continue
        for key in ("history_rows", "historical_rows"):
            value = _float_or_none(stage_result.get(key))
            if value is not None:
                return value
    return None
