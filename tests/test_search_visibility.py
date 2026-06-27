import asyncio
from datetime import UTC, datetime

from news.models import ArticleStatus, ArticleVisibility
from news.pipeline_repository import NewsPipelineRepository


class _FakeCursor:
    def __init__(self, executed) -> None:
        self._executed = executed

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    async def execute(self, query, params) -> None:
        self._executed.append((query, params))

    async def fetchall(self):
        return [
            (
                "00000000-0000-0000-0000-000000000001",
                "Public article from another publisher",
                ArticleStatus.PROCESSED.value,
                "ru",
                0.8,
                "10000000-0000-0000-0000-000000000001",
                datetime(2026, 1, 2, tzinfo=UTC),
                0.95,
                "cluster-1",
                "minor",
                0.2,
                "https://example.com/minor",
                "Short summary",
                "Full article text",
                "00000000-0000-0000-0000-000000000002",
                "Medoid cluster title",
                7,
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 5, tzinfo=UTC),
            ),
            (
                "00000000-0000-0000-0000-000000000002",
                "Significant earlier update",
                ArticleStatus.PROCESSED.value,
                "ru",
                0.9,
                "10000000-0000-0000-0000-000000000001",
                datetime(2026, 1, 1, tzinfo=UTC),
                0.9,
                "cluster-1",
                "significant",
                0.9,
                "https://example.com/significant",
                None,
                "Significant full article text",
                "00000000-0000-0000-0000-000000000002",
                "Medoid cluster title",
                7,
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 5, tzinfo=UTC),
            ),
            (
                "00000000-0000-0000-0000-000000000003",
                "Another story",
                ArticleStatus.PROCESSED.value,
                "ru",
                0.7,
                "10000000-0000-0000-0000-000000000001",
                datetime(2026, 1, 3, tzinfo=UTC),
                0.8,
                "cluster-2",
                "significant",
                0.8,
                None,
                None,
                "Another story text",
                None,
                None,
                None,
                None,
                None,
            )
        ]


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_queries = []
        self.update_queries = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    def cursor(self):
        return _FakeCursor(self.cursor_queries)

    async def execute(self, query, params) -> None:
        self.update_queries.append((query, params))


def test_search_is_scoped_to_requested_organization(monkeypatch) -> None:
    connection = _FakeConnection()

    async def connect(_database_url):
        return connection

    monkeypatch.setattr(
        "news.pipeline_repository.AsyncConnection.connect",
        connect,
    )
    repository = NewsPipelineRepository("postgresql://test")

    result = asyncio.run(
        repository.complete_search_query(
            query_id="00000000-0000-0000-0000-000000000002",
            organization_id="10000000-0000-0000-0000-000000000001",
            query_embedding=[1.0, 0.0],
            filters={},
            top_k=20,
            model_name="test-model",
            model_revision="test-revision",
        )
    )

    search_sql, search_params = connection.cursor_queries[0]
    normalized_sql = " ".join(search_sql.split()).lower()

    assert "a.visibility = %s" in normalized_sql
    assert "a.organization_id = %s" in normalized_sql
    assert ArticleVisibility.PUBLIC.value in search_params
    assert ArticleStatus.PROCESSED.value in search_params
    assert "submitted_by_user_id" not in normalized_sql
    assert "user_id" not in normalized_sql
    assert "10000000-0000-0000-0000-000000000001" in search_params
    assert result["items"][0]["title"] == "Public article from another publisher"
    assert result["items"][0]["organization_id"] == (
        "10000000-0000-0000-0000-000000000001"
    )
    assert result["items"][0]["summary"] == "Short summary"
    assert result["items"][0]["content"] == "Full article text"
    assert "left join article_pipeline_state" in normalized_sql
    assert "left join news_cluster_summaries" in normalized_sql
    assert "coalesce(s.manual_novelty_label, s.novelty_label)" in normalized_sql
    assert len(result["clusters"]) == 2
    assert result["clusters"][0]["cluster_id"] == "cluster-1"
    assert result["clusters"][0]["representative_article_id"] == (
        "00000000-0000-0000-0000-000000000002"
    )
    assert result["clusters"][0]["representative_title"] == "Medoid cluster title"
    assert result["clusters"][0]["article_count"] == 7
    assert result["clusters"][0]["published_from"] == "2026-01-01T00:00:00+00:00"
    assert result["clusters"][0]["published_to"] == "2026-01-05T00:00:00+00:00"
    assert result["clusters"][0]["significant_count"] == 1
    assert [
        item["title"] for item in result["clusters"][0]["items"]
    ] == [
        "Significant earlier update",
        "Public article from another publisher",
    ]


def test_search_filters_candidates_below_minimum_relevance(monkeypatch) -> None:
    connection = _FakeConnection()

    async def connect(_database_url):
        return connection

    monkeypatch.setattr(
        "news.pipeline_repository.AsyncConnection.connect",
        connect,
    )
    repository = NewsPipelineRepository("postgresql://test")

    result = asyncio.run(
        repository.complete_search_query(
            query_id="00000000-0000-0000-0000-000000000002",
            organization_id="10000000-0000-0000-0000-000000000001",
            query_embedding=[1.0, 0.0],
            filters={"min_relevance": 0.91},
            top_k=20,
            model_name="test-model",
            model_revision="test-revision",
        )
    )

    assert [item["score"] for item in result["items"]] == [0.95]


def test_admin_search_can_run_without_organization_scope(monkeypatch) -> None:
    connection = _FakeConnection()

    async def connect(_database_url):
        return connection

    monkeypatch.setattr(
        "news.pipeline_repository.AsyncConnection.connect",
        connect,
    )
    repository = NewsPipelineRepository("postgresql://test")

    asyncio.run(
        repository.complete_search_query(
            query_id="00000000-0000-0000-0000-000000000002",
            organization_id=None,
            query_embedding=[1.0, 0.0],
            filters={},
            top_k=20,
            model_name="test-model",
            model_revision="test-revision",
        )
    )

    search_sql, _ = connection.cursor_queries[0]
    normalized_sql = " ".join(search_sql.split()).lower()

    assert "a.organization_id = %s" not in normalized_sql
