from __future__ import annotations

import sys
import types

import numpy as np
import pandas as pd

try:
    import catboost  # noqa: F401
except ModuleNotFoundError:
    catboost_stub = types.ModuleType("catboost")
    catboost_stub.CatBoostClassifier = type("CatBoostClassifier", (), {})
    sys.modules["catboost"] = catboost_stub

from final_pipeline import FinalNewsNoveltyPipeline


class _UnusedEncoder:
    def encode_dataframe(self, *args, **kwargs):
        raise AssertionError("Encoder must not be called when embeddings are provided")


class _RecordingEncoder:
    def __init__(self, embeddings: np.ndarray) -> None:
        self.embeddings = np.asarray(embeddings, dtype=np.float32)
        self.news_ids: list[str] = []

    def encode_dataframe(self, df, **kwargs):
        self.news_ids = df["news_id"].astype(str).tolist()
        return self.embeddings.copy()


class _RecordingNoveltyModel:
    def __init__(self) -> None:
        self.news_df: pd.DataFrame | None = None
        self.embeddings: np.ndarray | None = None

    def predict_clustered_with_fallback(self, *, news_df, embeddings, **kwargs):
        self.news_df = news_df.copy()
        self.embeddings = np.asarray(embeddings, dtype=np.float32).copy()
        count = len(news_df)
        return pd.DataFrame(
            {
                "news_id": news_df["news_id"].astype(str),
                "published_at": news_df["published_at"],
                "topic": news_df["topic"],
                "title": news_df["title"],
                "text": news_df["text"],
                "cluster_id": news_df["cluster_id"].astype(str),
                "novelty_label": ["minor"] * count,
                "comment": ["test"] * count,
                "needs_review": [False] * count,
                "p_significant": [0.1] * count,
            }
        )


def _row(
    news_id: str,
    published_at: str,
    *,
    title: str,
    text: str = "",
    topic: str = "economy",
) -> dict:
    return {
        "news_id": news_id,
        "published_at": published_at,
        "topic": topic,
        "title": title,
        "text": text,
    }


def _pipeline(
    *,
    encoder=None,
) -> tuple[FinalNewsNoveltyPipeline, _RecordingNoveltyModel]:
    novelty_model = _RecordingNoveltyModel()
    pipeline = FinalNewsNoveltyPipeline(
        encoder=encoder or _UnusedEncoder(),
        novelty_model=novelty_model,
    )
    return pipeline, novelty_model


def test_full_pipeline_accepts_embeddings_and_returns_unified_result() -> None:
    news = pd.DataFrame(
        [
            _row("new-2", "2026-01-02", title="Продолжение"),
            _row("new-1", "2026-01-01", title="Начало"),
        ]
    )
    pipeline, novelty_model = _pipeline()

    result = pipeline.run(
        news,
        embeddings=np.array(
            [
                [0.9, 0.1],
                [1.0, 0.0],
            ],
            dtype=np.float32,
        ),
    )

    assert result.mode == "full"
    assert result.requested_ids == ["new-1", "new-2"]
    assert result.updated_ids == result.requested_ids
    assert result.context_ids == []
    assert result.embedding_ids == result.requested_ids
    np.testing.assert_allclose(result.embeddings[0], [1.0, 0.0])
    np.testing.assert_allclose(result.embeddings[1], [0.9, 0.1])
    assert result.assignments["assignment_method"].tolist() == ["baseline", "baseline"]
    assert result.assignments["baseline_component_id"].nunique() == 1
    assert result.predictions["news_id"].tolist() == result.requested_ids
    assert result.versions.pipeline_version == "final-v3-provenance-v1"
    assert novelty_model.news_df is not None
    assert novelty_model.news_df["news_id"].tolist() == result.requested_ids


def test_full_pipeline_baseline_clusters_similar_news() -> None:
    news = pd.DataFrame(
        [
            _row("news-1", "2026-01-01", title="Компания опубликовала отчет"),
            _row("news-2", "2026-01-02", title="Компания уточнила отчет"),
        ]
    )
    pipeline, _ = _pipeline()

    result = pipeline.run(
        news,
        embeddings=np.array(
            [
                [1.0, 0.0],
                [0.9, 0.1],
            ],
            dtype=np.float32,
        ),
    )

    assert result.assignments["cluster_id"].nunique() == 1
    assert result.diagnostics["base_clustering"]["n_clusters"] == 1


def test_full_pipeline_exp10_attaches_singleton_with_evidence() -> None:
    news = pd.DataFrame(
        [
            _row(
                "target-1",
                "2026-01-01",
                title="Банк снизил ставку до 12 процентов",
            ),
            _row(
                "target-2",
                "2026-01-02",
                title="Решение банка по ставке 12 процентов",
            ),
            _row(
                "source-1",
                "2026-01-03",
                title="Банк сохранил ставку 12 процентов",
            ),
        ]
    )
    pipeline, _ = _pipeline()

    result = pipeline.run(
        news,
        embeddings=np.array(
            [
                [1.0, 0.0],
                [1.0, 0.0],
                [0.78, 0.625779],
            ],
            dtype=np.float32,
        ),
    )

    assert result.diagnostics["base_clustering"]["n_clusters"] == 2
    assert result.diagnostics["attach_clustering"]["attached_source_clusters"] == 1
    assert result.assignments["cluster_id"].nunique() == 1
    attached = result.assignments[result.assignments["assignment_method"].eq("attach")]
    assert len(attached) == 1
    assert attached.iloc[0]["assignment_parent_news_id"] in {"target-1", "target-2"}
    assert attached.iloc[0]["assignment_similarity"] > 0.75
    assert attached.iloc[0]["baseline_component_id"] != attached.iloc[0]["cluster_id"]


def test_full_pipeline_keeps_different_topics_in_separate_clusters() -> None:
    news = pd.DataFrame(
        [
            _row("economy-1", "2026-01-01", title="Экономика", topic="economy"),
            _row("politics-1", "2026-01-01", title="Политика", topic="politics"),
        ]
    )
    pipeline, _ = _pipeline()

    result = pipeline.run(
        news,
        embeddings=np.array(
            [
                [1.0, 0.0],
                [1.0, 0.0],
            ],
            dtype=np.float32,
        ),
    )

    assert result.assignments["cluster_id"].nunique() == 2
    assert result.diagnostics["final_clusters"] == 2


def test_full_pipeline_calculates_embeddings_when_they_are_not_provided() -> None:
    news = pd.DataFrame(
        [
            _row("news-2", "2026-01-02", title="Продолжение"),
            _row("news-1", "2026-01-01", title="Начало"),
        ]
    )
    encoder = _RecordingEncoder(
        np.array(
            [
                [1.0, 0.0],
                [0.9, 0.1],
            ],
            dtype=np.float32,
        )
    )
    pipeline, novelty_model = _pipeline(encoder=encoder)

    result = pipeline.run(news)

    assert encoder.news_ids == ["news-1", "news-2"]
    assert result.embedding_ids == ["news-1", "news-2"]
    assert novelty_model.embeddings is not None
    np.testing.assert_allclose(novelty_model.embeddings, encoder.embeddings)


def test_full_pipeline_rejects_misaligned_embeddings() -> None:
    news = pd.DataFrame([_row("news-1", "2026-01-01", title="Новость")])
    pipeline, _ = _pipeline()

    try:
        pipeline.run(
            news,
            embeddings=np.array(
                [
                    [1.0, 0.0],
                    [0.0, 1.0],
                ],
                dtype=np.float32,
            ),
        )
    except ValueError as exc:
        assert "aligned with raw_news_df" in str(exc)
    else:
        raise AssertionError("Expected ValueError for misaligned embeddings")
