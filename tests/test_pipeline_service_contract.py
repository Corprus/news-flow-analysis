import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from api.main import NewsVectorizationJobStatus, NewsVectorizationRequest
from final_pipeline.result import PipelineResult, PipelineVersions
from model.significance_model import CatBoostSignificanceModel
from model_service.main import _run_vectorization_stage
from news.models import (
    ArticlePipelineEmbedding,
    ArticlePipelineState,
    ArticleStatus,
    ArticleVisibility,
    NewsArticle,
    NewsClusterSummary,
)
from news.pipeline_jobs import chunk_news_ids, enqueue_pipeline_job
from news.pipeline_repository import NewsPipelineRepository
from news.routes import (
    AddNewsRequest,
    DeleteNewsBatchRequest,
    NewsArticlePublishResponse,
    NewsArticleResponse,
    NewsSearchRequest,
    NewsSearchResponse,
    NoveltyLabelUpdate,
    PublishNewsBatchRequest,
    UpdateNoveltyLabelsRequest,
    _article_vectorization_payload,
    _articles_vectorization_payload,
)


class _ClusterSummaryCursor:
    def __init__(self, rows) -> None:
        self.rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    async def execute(self, query, params) -> None:
        return None

    async def fetchall(self):
        return self.rows


class _ClusterSummaryConnection:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.executed = []

    def cursor(self):
        return _ClusterSummaryCursor(self.rows)

    async def execute(self, query, params) -> None:
        self.executed.append((query, params))


class _HistoryColumn:
    def __init__(self, name: str) -> None:
        self.name = name


class _HistoryCursor:
    columns = [
        _HistoryColumn(name)
        for name in (
            "news_id",
            "organization_id",
            "published_at",
            "topic",
            "title",
            "text",
            "url",
            "cluster_id",
            "baseline_component_id",
            "assignment_method",
            "assignment_parent_news_id",
            "assignment_similarity",
            "attached_to_component_id",
            "embedding",
        )
    ]

    def __init__(self, row_batches) -> None:
        self.row_batches = list(row_batches)
        self.description = self.columns
        self.executed = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    async def execute(self, query, params) -> None:
        self.executed.append((query, params))

    async def fetchall(self):
        return self.row_batches.pop(0)


class _HistoryConnection:
    def __init__(self, row_batches) -> None:
        self.cursor_instance = _HistoryCursor(row_batches)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    def cursor(self):
        return self.cursor_instance


class _Article:
    id = "00000000-0000-0000-0000-000000000001"
    organization_id = "10000000-0000-0000-0000-000000000001"


class _PipelineJobRepositorySpy:
    def __init__(self, ordered_ids: list[str]) -> None:
        self.ordered_ids = ordered_ids
        self.processing: list[tuple[str, dict]] = []
        self.queued: list[tuple[str, dict]] = []

    async def mark_processing(self, job_id: str, payload: dict) -> None:
        self.processing.append((job_id, payload))

    async def mark_queued(self, job_id: str, payload: dict) -> None:
        self.queued.append((job_id, payload))

    async def order_article_ids(
        self,
        news_ids: list[str],
        *,
        organization_id: str | None,
    ) -> list[str]:
        assert set(news_ids) == set(self.ordered_ids)
        assert organization_id == "10000000-0000-0000-0000-000000000001"
        return self.ordered_ids


class _PipelinePublisherSpy:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def publish(self, message: dict) -> None:
        self.messages.append(message)


class _VectorizationRepositorySpy:
    def __init__(self, existing_embedding_ids: set[str]) -> None:
        self.existing_embedding_ids = existing_embedding_ids
        self.processing_ids: list[str] = []
        self.saved_article_ids: list[str] = []
        self.saved_embeddings: np.ndarray | None = None

    async def load_articles(self, news_ids: list[str], organization_id: str | None):
        return pd.DataFrame(
            [
                {
                    "news_id": news_id,
                    "organization_id": organization_id,
                    "published_at": datetime(2026, 1, index + 1, tzinfo=UTC),
                    "topic": "topic",
                    "title": f"Title {news_id}",
                    "text": f"Text {news_id}",
                    "url": "",
                }
                for index, news_id in enumerate(news_ids)
            ]
        )

    async def mark_articles_processing(self, news_ids: list[str]) -> None:
        self.processing_ids = list(news_ids)

    async def load_existing_embedding_ids(
        self,
        *,
        article_ids: list[str],
        model_name: str,
        model_revision: str,
    ) -> set[str]:
        assert model_name == "test-model"
        assert model_revision == "test-revision"
        return set(article_ids) & self.existing_embedding_ids

    async def save_embeddings(
        self,
        *,
        article_ids: list[str],
        embeddings: np.ndarray,
        model_name: str,
        model_revision: str,
    ) -> None:
        assert model_name == "test-model"
        assert model_revision == "test-revision"
        self.saved_article_ids = list(article_ids)
        self.saved_embeddings = np.asarray(embeddings)


class _VectorizationPipelineSpy:
    def __init__(self) -> None:
        self.encoded_ids: list[str] = []

    def encode_new_embeddings(self, news_df: pd.DataFrame):
        self.encoded_ids = news_df["news_id"].astype(str).tolist()
        embeddings = np.asarray(
            [
                [float(index), 1.0]
                for index, _news_id in enumerate(self.encoded_ids, start=1)
            ],
            dtype=np.float32,
        )
        return self.encoded_ids, embeddings


class _VectorizationPipelineMustNotRun:
    def encode_new_embeddings(self, _news_df: pd.DataFrame):
        raise AssertionError("encoder must not run for already embedded articles")


def _vectorization_app(pipeline) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(
            config=SimpleNamespace(
                embedding_model_name="test-model",
                embedding_model_revision="test-revision",
            ),
            incremental_pipeline=pipeline,
        )
    )


def _history_row(news_id: str, cluster_id: str, published_at: datetime):
    return (
        news_id,
        "10000000-0000-0000-0000-000000000001",
        published_at,
        "topic",
        f"Title {news_id}",
        f"Text {news_id}",
        "",
        cluster_id,
        cluster_id,
        "baseline",
        None,
        None,
        None,
        "[1.0,0.0]",
    )


def test_pipeline_job_contract_contains_ids_and_mode() -> None:
    """Контракт pipeline job передаёт ids статей, организацию и режим обработки."""
    request = NewsVectorizationRequest(
        news_ids=[
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ],
        organization_id="10000000-0000-0000-0000-000000000001",
        mode="full",
    )

    assert request.model_dump(mode="json") == {
        "news_ids": [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ],
        "organization_id": "10000000-0000-0000-0000-000000000001",
        "mode": "full",
    }


def test_pipeline_job_accepts_50k_article_ids() -> None:
    """Pipeline job принимает максимально допустимые 50 000 article ids."""
    article_ids = [
        f"00000000-0000-0000-0000-{index:012d}"
        for index in range(1, 50_001)
    ]

    request = NewsVectorizationRequest(
        news_ids=article_ids,
        organization_id="10000000-0000-0000-0000-000000000001",
        mode="full",
    )

    assert len(request.news_ids) == 50_000


def test_large_incremental_pipeline_job_is_split_into_chunks() -> None:
    """Большой incremental job режется на чанки заданного размера."""
    chunks = chunk_news_ids(["news-1", "news-2", "news-3", "news-4", "news-5"], 2)

    assert chunks == [["news-1", "news-2"], ["news-3", "news-4"], ["news-5"]]


def test_large_incremental_pipeline_job_plans_ordered_aggregate_batches() -> None:
    """Incremental job сохраняет порядок статей и планирует aggregate-батчи."""
    repository = _PipelineJobRepositorySpy(
        ["news-2", "news-4", "news-1", "news-3", "news-5"]
    )
    publisher = _PipelinePublisherSpy()

    job_id = asyncio.run(
        enqueue_pipeline_job(
            repository=repository,  # type: ignore[arg-type]
            publisher=publisher,  # type: ignore[arg-type]
            payload={
                "news_ids": ["news-1", "news-2", "news-3", "news-4", "news-5"],
                "organization_id": "10000000-0000-0000-0000-000000000001",
                "mode": "incremental",
            },
            chunk_size=2,
            aggregate_batch_size=3,
        )
    )

    parent_payload = repository.processing[0][1]
    queued_payloads = [payload for _, payload in repository.queued]
    aggregate_payloads = [
        payload for payload in queued_payloads if payload["mode"] == "aggregate"
    ]
    vectorize_payloads = [
        payload for payload in queued_payloads if payload["mode"] == "vectorize"
    ]

    assert str(job_id) == repository.processing[0][0]
    assert parent_payload["news_ids"] == ["news-2", "news-4", "news-1", "news-3", "news-5"]
    assert parent_payload["aggregate_batch_count"] == 2
    assert [payload["news_ids"] for payload in aggregate_payloads] == [
        ["news-2", "news-4", "news-1"],
        ["news-3", "news-5"],
    ]
    assert [payload["batch_index"] for payload in aggregate_payloads] == [1, 2]
    assert [payload["news_ids"] for payload in vectorize_payloads] == [
        ["news-2", "news-4"],
        ["news-1", "news-3"],
        ["news-5"],
    ]
    assert [message["payload"]["mode"] for message in publisher.messages] == [
        "vectorize",
        "vectorize",
        "vectorize",
    ]


def test_vectorization_stage_skips_existing_embeddings() -> None:
    """Стадия векторизации не пересчитывает уже сохранённые embeddings."""
    repository = _VectorizationRepositorySpy(existing_embedding_ids={"news-2"})
    pipeline = _VectorizationPipelineSpy()

    result = asyncio.run(
        _run_vectorization_stage(
            app=_vectorization_app(pipeline),  # type: ignore[arg-type]
            repository=repository,  # type: ignore[arg-type]
            news_ids=["news-1", "news-2", "news-3"],
            organization_id="10000000-0000-0000-0000-000000000001",
        )
    )

    assert repository.processing_ids == ["news-1", "news-2", "news-3"]
    assert pipeline.encoded_ids == ["news-1", "news-3"]
    assert repository.saved_article_ids == ["news-1", "news-3"]
    assert result["requested_ids"] == ["news-1", "news-2", "news-3"]
    assert result["embedded_ids"] == ["news-1", "news-3"]
    assert result["skipped_ids"] == ["news-2"]
    assert result["embedded_count"] == 2
    assert result["skipped_count"] == 1


def test_vectorization_stage_does_not_encode_when_all_embeddings_exist() -> None:
    """Если все embeddings уже есть, модель векторизации не вызывается."""
    repository = _VectorizationRepositorySpy(existing_embedding_ids={"news-1", "news-2"})

    result = asyncio.run(
        _run_vectorization_stage(
            app=_vectorization_app(_VectorizationPipelineMustNotRun()),  # type: ignore[arg-type]
            repository=repository,  # type: ignore[arg-type]
            news_ids=["news-1", "news-2"],
            organization_id="10000000-0000-0000-0000-000000000001",
        )
    )

    assert repository.processing_ids == ["news-1", "news-2"]
    assert repository.saved_article_ids == []
    assert result["embedded_ids"] == []
    assert result["skipped_ids"] == ["news-1", "news-2"]
    assert result["embedded_count"] == 0
    assert result["skipped_count"] == 2


def test_history_load_expands_window_to_full_clusters(monkeypatch) -> None:
    """Загрузка истории расширяет временное окно до полных известных кластеров."""
    window_rows = [
        _history_row(
            "00000000-0000-0000-0000-000000000001",
            "cluster-1",
            datetime(2026, 1, 2, tzinfo=UTC),
        )
    ]
    expanded_rows = [
        _history_row(
            "00000000-0000-0000-0000-000000000000",
            "cluster-1",
            datetime(2025, 12, 1, tzinfo=UTC),
        ),
        *window_rows,
    ]
    connection = _HistoryConnection([window_rows, expanded_rows])

    async def connect(_database_url):
        return connection

    monkeypatch.setattr("news.pipeline_repository.AsyncConnection.connect", connect)

    history, embeddings = asyncio.run(
        NewsPipelineRepository("postgresql://test").load_history(
            organization_id="10000000-0000-0000-0000-000000000001",
            exclude_news_ids=["00000000-0000-0000-0000-000000000099"],
            embedding_model="test-model",
            embedding_model_revision="test-revision",
            published_from=datetime(2026, 1, 1, tzinfo=UTC),
            published_to=datetime(2026, 1, 31, tzinfo=UTC),
            expand_clusters=True,
            cluster_expansion_max_rows=10,
        )
    )

    assert history["news_id"].tolist() == [
        "00000000-0000-0000-0000-000000000000",
        "00000000-0000-0000-0000-000000000001",
    ]
    assert embeddings.shape == (2, 2)
    assert len(connection.cursor_instance.executed) == 2
    assert "s.cluster_id = ANY" in connection.cursor_instance.executed[1][0]


def test_history_load_keeps_window_when_cluster_expansion_exceeds_limit(monkeypatch) -> None:
    """История не расширяется, если полный кластер превышает заданный лимит строк."""
    window_rows = [
        _history_row(
            "00000000-0000-0000-0000-000000000001",
            "cluster-1",
            datetime(2026, 1, 2, tzinfo=UTC),
        )
    ]
    expanded_rows = [
        _history_row(
            f"00000000-0000-0000-0000-{index:012d}",
            "cluster-1",
            datetime(2025, 12, index + 1, tzinfo=UTC),
        )
        for index in range(3)
    ]
    connection = _HistoryConnection([window_rows, expanded_rows])

    async def connect(_database_url):
        return connection

    monkeypatch.setattr("news.pipeline_repository.AsyncConnection.connect", connect)

    history, embeddings = asyncio.run(
        NewsPipelineRepository("postgresql://test").load_history(
            organization_id="10000000-0000-0000-0000-000000000001",
            exclude_news_ids=["00000000-0000-0000-0000-000000000099"],
            embedding_model="test-model",
            embedding_model_revision="test-revision",
            published_from=datetime(2026, 1, 1, tzinfo=UTC),
            published_to=datetime(2026, 1, 31, tzinfo=UTC),
            expand_clusters=True,
            cluster_expansion_max_rows=2,
        )
    )

    assert history["news_id"].tolist() == [
        "00000000-0000-0000-0000-000000000001"
    ]
    assert embeddings.shape == (1, 2)
    assert len(connection.cursor_instance.executed) == 2


def test_pipeline_job_rejects_more_than_50k_article_ids() -> None:
    """Pipeline job отклоняет payload больше 50 000 статей."""
    article_ids = [
        f"00000000-0000-0000-0000-{index:012d}"
        for index in range(1, 50_002)
    ]

    with pytest.raises(ValidationError):
        NewsVectorizationRequest(
            news_ids=article_ids,
            organization_id="10000000-0000-0000-0000-000000000001",
        )


def test_news_batch_contract_accepts_50k_article_ids() -> None:
    """Batch API для публикаций принимает предельные 50 000 article ids."""
    article_ids = [
        f"00000000-0000-0000-0000-{index:012d}"
        for index in range(1, 50_001)
    ]

    assert len(PublishNewsBatchRequest(article_ids=article_ids).article_ids) == 50_000
    assert len(DeleteNewsBatchRequest(article_ids=article_ids).article_ids) == 50_000
    assert (
        len(
            UpdateNoveltyLabelsRequest(
                updates=[
                    NoveltyLabelUpdate(article_id=article_id, label="minor")
                    for article_id in article_ids
                ]
            ).updates
        )
        == 50_000
    )


def test_new_article_uses_incremental_pipeline_by_default() -> None:
    """Новая статья по умолчанию отправляется в incremental pipeline."""
    assert _article_vectorization_payload(_Article()) == {
        "news_ids": [_Article.id],
        "organization_id": _Article.organization_id,
        "mode": "incremental",
    }


def test_multiple_articles_use_one_incremental_pipeline_payload() -> None:
    """Несколько статей одной организации упаковываются в один incremental payload."""
    organization_id = "10000000-0000-0000-0000-000000000001"
    first = type("Article", (), {"id": "news-1", "organization_id": organization_id})()
    second = type("Article", (), {"id": "news-2", "organization_id": organization_id})()

    assert _articles_vectorization_payload([first, second]) == {
        "news_ids": ["news-1", "news-2"],
        "organization_id": organization_id,
        "mode": "incremental",
    }


def test_multiple_articles_reject_mixed_organizations_in_pipeline_payload() -> None:
    """Pipeline payload запрещает смешивать статьи разных организаций."""
    first = type(
        "Article",
        (),
        {
            "id": "news-1",
            "organization_id": "10000000-0000-0000-0000-000000000001",
        },
    )()
    second = type(
        "Article",
        (),
        {
            "id": "news-2",
            "organization_id": "10000000-0000-0000-0000-000000000002",
        },
    )()

    with pytest.raises(ValueError, match="one organization"):
        _articles_vectorization_payload([first, second])


def test_add_and_batch_publish_contracts_expose_immediate_publication() -> None:
    """Контракты добавления и batch-публикации явно поддерживают немедленную публикацию."""
    add_request = AddNewsRequest(
        title="Title",
        content="Content",
        published_at="2026-01-01T12:00:00+00:00",
        publish_immediately=True,
    )
    batch_request = PublishNewsBatchRequest(
        article_ids=[
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
    )

    assert add_request.publish_immediately
    assert len(batch_request.article_ids) == 2


def test_pipeline_storage_uses_bge_m3_vector_dimensions() -> None:
    """Хранилище pipeline использует размерность BGE-M3 и обязательные поля состояния."""
    embedding_column = ArticlePipelineEmbedding.__table__.c.embedding

    assert embedding_column.type.dimensions == 1024
    assert ArticlePipelineState.__tablename__ == "article_pipeline_state"
    assert ArticlePipelineState.__table__.c.manual_novelty_label.nullable
    assert ArticlePipelineState.__table__.c.manual_novelty_actor_id.nullable
    assert ArticlePipelineState.__table__.c.manual_novelty_updated_at.nullable
    assert NewsClusterSummary.__tablename__ == "news_cluster_summaries"
    assert NewsClusterSummary.__table__.c.organization_id.nullable is False
    assert NewsClusterSummary.__table__.c.representative_article_id.nullable is False
    assert NewsClusterSummary.__table__.c.article_count.nullable is False


def test_cluster_summary_representative_is_article_nearest_to_centroid() -> None:
    """Представителем кластера становится статья, ближайшая к центроиду embeddings."""
    connection = _ClusterSummaryConnection(
        [
            (
                "10000000-0000-0000-0000-000000000001",
                "cluster-1",
                "00000000-0000-0000-0000-000000000001",
                datetime(2026, 1, 1, tzinfo=UTC),
                "significant",
                "[1,0]",
            ),
            (
                "10000000-0000-0000-0000-000000000001",
                "cluster-1",
                "00000000-0000-0000-0000-000000000002",
                datetime(2026, 1, 2, tzinfo=UTC),
                "minor",
                "[0.8,0.6]",
            ),
            (
                "10000000-0000-0000-0000-000000000001",
                "cluster-1",
                "00000000-0000-0000-0000-000000000003",
                datetime(2026, 1, 3, tzinfo=UTC),
                "significant",
                "[0,1]",
            ),
        ]
    )

    asyncio.run(
        NewsPipelineRepository._refresh_cluster_summaries(
            connection,  # type: ignore[arg-type]
            cluster_ids={"cluster-1"},
            embedding_model="test-model",
            embedding_model_revision="test-revision",
        )
    )

    insert_query, insert_params = connection.executed[-1]
    assert "news_cluster_summaries" in insert_query
    assert insert_params[2] == "00000000-0000-0000-0000-000000000002"
    assert insert_params[3] == 3
    assert insert_params[4] == datetime(2026, 1, 1, tzinfo=UTC)
    assert insert_params[5] == datetime(2026, 1, 3, tzinfo=UTC)


def test_article_status_contract_and_required_storage_fields() -> None:
    """Контракт статусов статьи и обязательных storage-полей не меняется случайно."""
    assert {status.value for status in ArticleStatus} == {
        "not_started",
        "pending",
        "processing",
        "processed",
        "error",
    }
    assert NewsArticle.__table__.c.status.default.arg == "not_started"
    assert NewsArticle.__table__.c.visibility.default.arg == "draft"
    assert not NewsArticle.__table__.c.content.nullable
    assert not NewsArticle.__table__.c.published_at.nullable
    assert not ArticlePipelineState.__table__.c.novelty_label.nullable
    assert not ArticlePipelineState.__table__.c.p_significant.nullable
    assert {visibility.value for visibility in ArticleVisibility} == {
        "draft",
        "public",
        "archived",
    }


def test_add_news_requires_content_and_timezone_aware_published_at() -> None:
    """Добавление новости требует текст и timezone-aware дату публикации."""
    with pytest.raises(ValidationError):
        AddNewsRequest(title="Title", content="Content")
    with pytest.raises(ValidationError):
        AddNewsRequest(
            title="Title",
            content="Content",
            published_at="2026-01-01T12:00:00",
        )


def test_pipeline_job_status_contract_contains_timestamps() -> None:
    """Публичный статус pipeline job содержит временные метки жизненного цикла."""
    status = NewsVectorizationJobStatus(
        job_id="00000000-0000-0000-0000-000000000001",
        status="done",
        request={"news_ids": ["00000000-0000-0000-0000-000000000001"]},
        result={"updated_ids": ["00000000-0000-0000-0000-000000000001"]},
        created_at="2026-01-01T12:00:00+00:00",
        updated_at="2026-01-01T12:01:00+00:00",
    )

    assert status.status == "done"


def test_public_job_response_uses_generic_job_id() -> None:
    """Публичные ответы используют generic `job_id`, а не внутреннее имя очереди."""
    article_response = NewsArticleResponse(
        article_id="00000000-0000-0000-0000-000000000001",
        visibility="draft",
        status="not_started",
    )
    publish_response = NewsArticlePublishResponse(
        article_id="00000000-0000-0000-0000-000000000001",
        visibility="public",
        status="pending",
        job_id="00000000-0000-0000-0000-000000000002",
    )
    search_response = NewsSearchResponse(
        query_id="00000000-0000-0000-0000-000000000003",
        status="queued",
        job_id="00000000-0000-0000-0000-000000000004",
    )

    assert "vectorization_job_id" not in article_response.model_dump()
    assert publish_response.visibility == "public"
    assert "vectorization_job_id" not in search_response.model_dump()


def test_search_date_filters_require_timezone_and_valid_range() -> None:
    """Фильтры поиска требуют timezone-aware даты и корректный диапазон."""
    with pytest.raises(ValidationError):
        NewsSearchRequest(
            query_text="economy",
            published_from="2026-01-01T12:00:00",
        )
    with pytest.raises(ValidationError):
        NewsSearchRequest(
            query_text="economy",
            published_from="2026-01-02T12:00:00+00:00",
            published_to="2026-01-01T12:00:00+00:00",
        )
    with pytest.raises(ValidationError):
        NewsSearchRequest(
            query_text="economy",
            submitted_by_user_id="00000000-0000-0000-0000-000000000001",
        )


class _ModelMustNotRunForSeed:
    """Падает, если seed-кейс ошибочно дошёл до ML-классификатора."""

    def predict_proba(self, _features):
        raise AssertionError("cluster seed must not be passed to the classifier")


def test_first_article_in_cluster_is_significant_seed() -> None:
    """Первая статья нового кластера считается significant без вызова модели."""
    model = CatBoostSignificanceModel(model=_ModelMustNotRunForSeed())
    news = pd.DataFrame(
        [
            {
                "news_id": "seed-1",
                "cluster_id": "cluster-1",
                "topic": "economy",
                "published_at": "2026-01-01T12:00:00+00:00",
                "title": "First article",
                "text": "First article content",
            }
        ]
    )

    result = model.predict_clustered_with_fallback(
        news_df=news,
        embeddings=np.asarray([[1.0, 0.0]], dtype=np.float32),
    )

    assert result.loc[0, "novelty_label"] == "significant"
    assert result.loc[0, "p_significant"] == 1.0
    assert not bool(result.loc[0, "needs_review"])
    assert result.loc[0, "comment"] == "cluster seed; significant by definition"


def test_processed_result_requires_embedding_assignment_and_novelty() -> None:
    """PipelineResult требует согласованные embeddings, assignments и novelty labels."""
    versions = PipelineVersions(
        pipeline_version="test",
        embedding_model="BAAI/bge-m3",
        embedding_model_revision="test",
        novelty_model_version="test",
        config_version="test",
    )
    result = PipelineResult(
        mode="incremental",
        requested_ids=["news-1"],
        updated_ids=["news-1"],
        context_ids=[],
        predictions=pd.DataFrame(
            [
                {
                    "news_id": "news-1",
                    "cluster_id": "cluster-1",
                    "novelty_label": "significant",
                    "p_significant": 1.0,
                }
            ]
        ),
        assignments=pd.DataFrame(
            [{"news_id": "news-1", "cluster_id": "cluster-1"}]
        ),
        embedding_ids=["news-1"],
        embeddings=np.asarray([[1.0, 0.0]], dtype=np.float32),
        diagnostics={},
        versions=versions,
    )

    NewsPipelineRepository._validate_result(
        result=result,
        persisted_ids=["news-1"],
        prediction_by_id={
            "news-1": {
                "cluster_id": "cluster-1",
                "novelty_label": "significant",
                "p_significant": 1.0,
            }
        },
        assignment_by_id={"news-1": {"cluster_id": "cluster-1"}},
        embedding_by_id={"news-1": np.asarray([1.0, 0.0], dtype=np.float32)},
    )

    with pytest.raises(ValueError, match="Missing embeddings"):
        NewsPipelineRepository._validate_result(
            result=result,
            persisted_ids=["news-1"],
            prediction_by_id={"news-1": {"cluster_id": "cluster-1"}},
            assignment_by_id={"news-1": {"cluster_id": "cluster-1"}},
            embedding_by_id={},
        )
