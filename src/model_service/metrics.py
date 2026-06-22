from prometheus_client import Counter, Gauge, Histogram

PIPELINE_ARTICLES_PROCESSED = Counter(
    "news_flow_pipeline_articles_processed_total",
    "Number of news articles successfully processed by the pipeline.",
    ("mode",),
)
PIPELINE_JOBS = Counter(
    "news_flow_pipeline_jobs_total",
    "Number of completed pipeline jobs.",
    ("mode", "status"),
)
PIPELINE_JOB_DURATION = Histogram(
    "news_flow_pipeline_job_duration_seconds",
    "Pipeline job duration in seconds.",
    ("mode",),
    buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600),
)
PIPELINE_JOBS_IN_PROGRESS = Gauge(
    "news_flow_pipeline_jobs_in_progress",
    "Number of pipeline jobs currently being processed.",
    ("mode",),
)
PIPELINE_LAST_JOB_DURATION = Gauge(
    "news_flow_pipeline_last_job_duration_seconds",
    "Duration of the last completed pipeline job in seconds.",
    ("mode", "status"),
)
PIPELINE_LAST_JOB_THROUGHPUT = Gauge(
    "news_flow_pipeline_last_job_throughput",
    "Average throughput of the last successful pipeline job in news per second.",
    ("mode",),
)
PROCESSED_ARTICLES = Gauge(
    "news_flow_processed_articles",
    "Current number of news articles with processed status in PostgreSQL.",
)
