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
                datetime(2026, 1, 2, tzinfo=UTC),
                0.95,
                "cluster-1",
                "minor",
                0.2,
                "https://example.com/minor",
            ),
            (
                "00000000-0000-0000-0000-000000000002",
                "Significant earlier update",
                ArticleStatus.PROCESSED.value,
                "ru",
                0.9,
                datetime(2026, 1, 1, tzinfo=UTC),
                0.9,
                "cluster-1",
                "significant",
                0.9,
                "https://example.com/significant",
            ),
            (
                "00000000-0000-0000-0000-000000000003",
                "Another story",
                ArticleStatus.PROCESSED.value,
                "ru",
                0.7,
                datetime(2026, 1, 3, tzinfo=UTC),
                0.8,
                "cluster-2",
                "significant",
                0.8,
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


def test_search_returns_all_public_articles_without_publisher_scope(monkeypatch) -> None:
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
    assert ArticleVisibility.PUBLIC.value in search_params
    assert ArticleStatus.PROCESSED.value in search_params
    assert "submitted_by_user_id" not in normalized_sql
    assert "user_id" not in normalized_sql
    assert "organization_id" not in normalized_sql
    assert result["items"][0]["title"] == "Public article from another publisher"
    assert "left join article_pipeline_state" in normalized_sql
    assert len(result["clusters"]) == 2
    assert result["clusters"][0]["cluster_id"] == "cluster-1"
    assert result["clusters"][0]["representative_article_id"] == (
        "00000000-0000-0000-0000-000000000001"
    )
    assert result["clusters"][0]["significant_count"] == 1
    assert [
        item["title"] for item in result["clusters"][0]["items"]
    ] == [
        "Significant earlier update",
        "Public article from another publisher",
    ]
