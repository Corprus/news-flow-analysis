from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from news.models import ArticleStatus, ArticleVisibility
from news.routes import get_news_feed
from news.search_results import group_search_items
from news.service import NewsService


class _ScalarCollection:
    def __init__(self, values) -> None:
        self._values = values

    def all(self):
        return self._values


class _QueryResult:
    def __init__(self, *, scalar=None, values=None) -> None:
        self._scalar = scalar
        self._values = values or []

    def scalar_one(self):
        return self._scalar

    def scalar_one_or_none(self):
        return self._scalar

    def scalars(self):
        return _ScalarCollection(self._values)


class _RecordingSession:
    def __init__(self, articles) -> None:
        self.articles = articles
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        if len(self.statements) == 1:
            return _QueryResult(scalar=len(self.articles))
        return _QueryResult(values=self.articles)


class _AdjacentDateSession:
    def __init__(self, previous_date, next_date) -> None:
        self.results = iter([previous_date, next_date])
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        return _QueryResult(scalar=next(self.results))


def test_news_feed_query_filters_public_articles_by_half_open_period() -> None:
    article = SimpleNamespace(id=str(uuid4()))
    session = _RecordingSession([article])
    service = NewsService(session)  # type: ignore[arg-type]
    published_from = datetime(2026, 6, 22, tzinfo=UTC)
    published_to = datetime(2026, 6, 23, tzinfo=UTC)

    articles, total = service.list_public_articles_by_period(
        published_from=published_from,
        published_to=published_to,
    )

    count_sql = str(session.statements[0])
    select_sql = str(session.statements[1])
    normalized_sql = " ".join(select_sql.split()).lower()

    assert articles == [article]
    assert total == 1
    assert "news_articles.visibility" in count_sql
    assert "news_articles.status" in count_sql
    assert "news_articles.published_at >=" in normalized_sql
    assert "news_articles.published_at <" in normalized_sql
    assert "order by news_articles.published_at desc" in normalized_sql
    assert session.statements[1]._limit_clause is None
    assert session.statements[1]._offset_clause is None
    assert ArticleVisibility.PUBLIC.value in session.statements[1].compile().params.values()
    assert ArticleStatus.PROCESSED.value in session.statements[1].compile().params.values()


def test_adjacent_news_dates_skip_empty_periods() -> None:
    previous_date = datetime(2026, 6, 20, 15, tzinfo=UTC)
    next_date = datetime(2026, 6, 25, 9, tzinfo=UTC)
    session = _AdjacentDateSession(previous_date, next_date)
    service = NewsService(session)  # type: ignore[arg-type]

    result = service.get_adjacent_public_article_dates(
        published_from=datetime(2026, 6, 22, tzinfo=UTC),
        published_to=datetime(2026, 6, 23, tzinfo=UTC),
    )

    previous_sql = " ".join(str(session.statements[0]).split()).lower()
    next_sql = " ".join(str(session.statements[1]).split()).lower()
    assert result == (previous_date, next_date)
    assert "max(news_articles.published_at)" in previous_sql
    assert "news_articles.published_at <" in previous_sql
    assert "min(news_articles.published_at)" in next_sql
    assert "news_articles.published_at >=" in next_sql


def test_latest_news_date_uses_processed_public_articles() -> None:
    latest_date = datetime(2026, 6, 25, 9, tzinfo=UTC)
    session = _AdjacentDateSession(latest_date, None)
    service = NewsService(session)  # type: ignore[arg-type]

    result = service.get_latest_public_article_date()

    sql = " ".join(str(session.statements[0]).split()).lower()
    params = session.statements[0].compile().params.values()
    assert result == latest_date
    assert "max(news_articles.published_at)" in sql
    assert ArticleVisibility.PUBLIC.value in params
    assert ArticleStatus.PROCESSED.value in params


class _NewsServiceSpy:
    def __init__(self, article) -> None:
        self.article = article
        self.calls = []

    def list_public_articles_by_period(self, **kwargs):
        self.calls.append(kwargs)
        return [self.article], 1


def test_news_feed_response_matches_clustered_search_result_without_relevance() -> None:
    article_id = uuid4()
    published_at = datetime(2026, 6, 22, 12, tzinfo=UTC)
    pipeline_state = SimpleNamespace(
        cluster_id="cluster-1",
        novelty_label="minor",
        manual_novelty_label="significant",
        p_significant=0.91,
    )
    article = SimpleNamespace(
        id=str(article_id),
        title="Новость",
        status=ArticleStatus.PROCESSED.value,
        summary="Краткое описание",
        content="Полный текст",
        published_at=published_at,
        language="ru",
        novelty_score=0.91,
        url="https://example.com/news",
        pipeline_state=pipeline_state,
    )
    service = _NewsServiceSpy(article)

    response = get_news_feed(
        current_user=object(),  # type: ignore[arg-type]
        news=service,  # type: ignore[arg-type]
        published_from=datetime(2026, 6, 22, tzinfo=UTC),
        published_to=datetime(2026, 6, 23, tzinfo=UTC),
        limit=50,
        offset=0,
    )

    assert response.total == 1
    assert response.total_clusters == 1
    assert response.items[0]["article_id"] == str(article_id)
    assert response.items[0]["content"] == "Полный текст"
    assert response.items[0]["novelty_label"] == "significant"
    assert "score" not in response.items[0]
    assert response.clusters[0]["cluster_id"] == "cluster-1"
    assert response.clusters[0]["representative_article_id"] == str(article_id)
    assert response.clusters[0]["significant_count"] == 1
    assert "score" not in response.clusters[0]
    assert set(service.calls[0]) == {"published_from", "published_to"}


def test_news_feed_rejects_empty_or_reversed_period() -> None:
    service = _NewsServiceSpy(None)
    same_time = datetime(2026, 6, 22, tzinfo=UTC)

    with pytest.raises(HTTPException) as error:
        get_news_feed(
            current_user=object(),  # type: ignore[arg-type]
            news=service,  # type: ignore[arg-type]
            published_from=same_time,
            published_to=same_time,
        )

    assert error.value.status_code == 422
    assert service.calls == []


def test_group_search_items_keeps_search_score_only_when_present() -> None:
    base_item = {
        "article_id": "article-1",
        "title": "Новость",
        "published_at": "2026-06-22T12:00:00+00:00",
        "rank": 1,
        "cluster_id": "cluster-1",
        "novelty_label": "significant",
    }

    feed_cluster = group_search_items([base_item], top_k=10)[0]
    search_cluster = group_search_items(
        [{**base_item, "score": 0.95}],
        top_k=10,
    )[0]

    assert "score" not in feed_cluster
    assert search_cluster["score"] == 0.95
