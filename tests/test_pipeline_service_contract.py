from api.main import NewsVectorizationRequest
from news.models import ArticlePipelineEmbedding, ArticlePipelineState
from news.routes import _article_vectorization_payload


class _Article:
    id = "00000000-0000-0000-0000-000000000001"


def test_pipeline_job_contract_contains_ids_and_mode() -> None:
    request = NewsVectorizationRequest(
        news_ids=[
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ],
        mode="full",
    )

    assert request.model_dump() == {
        "news_ids": [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ],
        "mode": "full",
    }


def test_new_article_uses_incremental_pipeline_by_default() -> None:
    assert _article_vectorization_payload(_Article()) == {
        "news_ids": [_Article.id],
        "mode": "incremental",
    }


def test_pipeline_storage_uses_bge_m3_vector_dimensions() -> None:
    embedding_column = ArticlePipelineEmbedding.__table__.c.embedding

    assert embedding_column.type.dimensions == 1024
    assert ArticlePipelineState.__tablename__ == "article_pipeline_state"
