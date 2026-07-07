from api import metrics


class _FakeCursor:
    def __init__(self) -> None:
        self._query = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, query: str, params=None) -> None:
        self._query = query

    def fetchall(self) -> list:
        if "FROM news_pipeline_jobs" in self._query and "SELECT result" in self._query:
            return [
                (
                    {
                        "total_rows": 10,
                        "created_count": 8,
                        "duplicate_count": 2,
                        "published_count": 0,
                    },
                )
            ]
        if (
            "FROM news_pipeline_jobs" in self._query
            and "news_search_query" in self._query
            and "count(*) AS jobs" in self._query
        ):
            return [("queued", "news_search", 2, 0)]
        if (
            "FROM news_pipeline_jobs" in self._query
            and "news_search_query" in self._query
            and "oldest_age_seconds" in self._query
        ):
            return [("queued", 12.5), ("processing", 3.0)]
        return []

    def fetchone(self):
        if "FROM news_articles" in self._query:
            return (5,)
        return None


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor()


def test_import_rows_db_counts_published_imported_articles(monkeypatch) -> None:
    """Метрика опубликованных импортов берётся из текущего состояния статей."""
    monkeypatch.setattr(metrics, "connect", lambda database_url: _FakeConnection())

    families = list(metrics.ApiDatabaseCollector("postgresql://test").collect())
    import_rows = next(
        family for family in families if family.name == "news_flow_import_rows_db"
    )
    values = {sample.labels["kind"]: sample.value for sample in import_rows.samples}

    assert values == {
        "total": 10,
        "created": 8,
        "duplicate": 2,
        "published": 5,
    }


def test_pipeline_queue_db_counts_search_jobs_without_articles(monkeypatch) -> None:
    """Метрика pipeline backlog показывает поисковые jobs отдельно от статей."""
    monkeypatch.setattr(metrics, "connect", lambda database_url: _FakeConnection())

    families = list(metrics.ApiDatabaseCollector("postgresql://test").collect())
    pipeline_jobs = next(
        family for family in families if family.name == "news_flow_pipeline_queue_jobs"
    )
    pipeline_articles = next(
        family
        for family in families
        if family.name == "news_flow_pipeline_queue_articles"
    )

    job_values = {
        (sample.labels["status"], sample.labels["mode"]): sample.value
        for sample in pipeline_jobs.samples
    }
    article_values = {
        (sample.labels["status"], sample.labels["mode"]): sample.value
        for sample in pipeline_articles.samples
    }

    assert job_values[("queued", "news_search")] == 2
    assert article_values[("queued", "news_search")] == 0


def test_search_job_age_metric_reports_oldest_active_jobs(monkeypatch) -> None:
    """Метрика search age показывает ожидание и активную обработку поиска."""
    monkeypatch.setattr(metrics, "connect", lambda database_url: _FakeConnection())

    families = list(metrics.ApiDatabaseCollector("postgresql://test").collect())
    search_age = next(
        family for family in families if family.name == "news_flow_search_job_age_seconds"
    )
    values = {sample.labels["status"]: sample.value for sample in search_age.samples}

    assert values == {
        "queued": 12.5,
        "processing": 3.0,
    }
