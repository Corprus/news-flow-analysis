import asyncio
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from api.main import NewsVectorizationJobStatus, NewsVectorizationRequest
from final_pipeline.result import PipelineResult, PipelineVersions
from model.significance_model import CatBoostSignificanceModel
from news.models import (
    ArticlePipelineEmbedding,
    ArticlePipelineState,
    ArticleStatus,
    ArticleVisibility,
    NewsArticle,
    NewsClusterSummary,
)
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


class _Article:
    id = "00000000-0000-0000-0000-000000000001"
    organization_id = "10000000-0000-0000-0000-000000000001"


def test_pipeline_job_contract_contains_ids_and_mode() -> None:
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


def test_pipeline_job_rejects_more_than_50k_article_ids() -> None:
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
    assert _article_vectorization_payload(_Article()) == {
        "news_ids": [_Article.id],
        "organization_id": _Article.organization_id,
        "mode": "incremental",
    }


def test_multiple_articles_use_one_incremental_pipeline_payload() -> None:
    organization_id = "10000000-0000-0000-0000-000000000001"
    first = type("Article", (), {"id": "news-1", "organization_id": organization_id})()
    second = type("Article", (), {"id": "news-2", "organization_id": organization_id})()

    assert _articles_vectorization_payload([first, second]) == {
        "news_ids": ["news-1", "news-2"],
        "organization_id": organization_id,
        "mode": "incremental",
    }


def test_multiple_articles_reject_mixed_organizations_in_pipeline_payload() -> None:
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
    with pytest.raises(ValidationError):
        AddNewsRequest(title="Title", content="Content")
    with pytest.raises(ValidationError):
        AddNewsRequest(
            title="Title",
            content="Content",
            published_at="2026-01-01T12:00:00",
        )


def test_pipeline_job_status_contract_contains_timestamps() -> None:
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
    def predict_proba(self, _features):
        raise AssertionError("cluster seed must not be passed to the classifier")


def test_first_article_in_cluster_is_significant_seed() -> None:
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
