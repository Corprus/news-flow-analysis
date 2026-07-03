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

from final_pipeline import (
    FinalPipelineConfig,
    IncrementalNewsNoveltyPipeline,
    IncrementalPipelineConfig,
)
from model.attach_clustering import AttachClusteringConfig, BaselineClusteringConfig


class _UnusedEncoder:
    def encode_dataframe(self, *args, **kwargs):
        raise AssertionError("Encoder must not be called when new_embeddings are provided")


class _RecordingNoveltyModel:
    def __init__(self) -> None:
        self.news_df: pd.DataFrame | None = None
        self.embeddings: np.ndarray | None = None

    def predict_clustered_with_fallback(self, *, news_df, embeddings, **kwargs):
        self.news_df = news_df.copy()
        self.embeddings = np.asarray(embeddings).copy()
        return pd.DataFrame(
            {
                "news_id": news_df["news_id"].astype(str),
                "published_at": news_df["published_at"],
                "topic": news_df["topic"],
                "title": news_df["title"],
                "text": news_df["text"],
                "cluster_id": news_df["cluster_id"].astype(str),
                "novelty_label": "minor",
                "comment": "test",
                "needs_review": False,
                "p_significant": 0.1,
            }
        )


def _news(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _row(
    news_id: str,
    published_at: str,
    *,
    title: str,
    text: str = "",
    topic: str = "economy",
    cluster_id: str | None = None,
) -> dict:
    row = {
        "news_id": news_id,
        "published_at": published_at,
        "topic": topic,
        "title": title,
        "text": text,
    }
    if cluster_id is not None:
        row["cluster_id"] = cluster_id
    return row


def _pipeline() -> tuple[IncrementalNewsNoveltyPipeline, _RecordingNoveltyModel]:
    novelty_model = _RecordingNoveltyModel()
    pipeline = IncrementalNewsNoveltyPipeline(
        encoder=_UnusedEncoder(),
        novelty_model=novelty_model,
    )
    return pipeline, novelty_model


def test_incremental_config_can_be_built_from_final_clustering_config() -> None:
    final_config = FinalPipelineConfig(
        base_clustering=BaselineClusteringConfig(
            story_threshold=0.8,
            story_window_days=10,
        ),
        attach_clustering=AttachClusteringConfig(
            min_similarity=0.74,
            max_days=9,
            min_margin=0.04,
            title_jaccard_threshold=0.12,
            min_shared_numbers=2,
        ),
    )

    incremental_config = IncrementalPipelineConfig.from_final_config(final_config)

    assert incremental_config.baseline_similarity == 0.8
    assert incremental_config.baseline_window_days == 10
    assert incremental_config.attach_similarity == 0.74
    assert incremental_config.attach_window_days == 9
    assert incremental_config.min_margin == 0.04
    assert incremental_config.title_jaccard_threshold == 0.12
    assert incremental_config.min_shared_numbers == 2


def test_assigns_new_article_to_existing_cluster_by_baseline_similarity() -> None:
    historical = _news(
        [
            _row(
                "old-1",
                "2026-01-01",
                title="Компания опубликовала отчет",
                cluster_id="event-1",
            )
        ]
    )
    new = _news(
        [_row("new-1", "2026-01-02", title="Компания уточнила опубликованный отчет")]
    )
    pipeline, novelty_model = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
        new_news_df=new,
        new_embeddings=np.array([[0.9, 0.1]], dtype=np.float32),
    )

    assignment = result.assignments.iloc[0]
    assert assignment["cluster_id"] == "event-1"
    assert assignment["assignment_method"] == "baseline"
    assert result.predictions["news_id"].tolist() == ["new-1"]
    assert result.requested_ids == ["new-1"]
    assert result.updated_ids == ["new-1"]
    assert result.embedding_ids == ["new-1"]
    assert result.embeddings.shape == (1, 2)
    assert novelty_model.news_df is not None
    assert novelty_model.news_df["news_id"].tolist() == ["old-1", "new-1"]


def test_uses_exp10_evidence_for_lower_similarity_attach() -> None:
    historical = _news(
        [
            _row(
                "old-1",
                "2026-01-01",
                title="Банк снизил ставку до 12 процентов",
                cluster_id="event-1",
            )
        ]
    )
    new = _news(
        [
            _row(
                "new-1",
                "2026-01-03",
                title="Банк сохранил ставку 12 процентов",
            )
        ]
    )
    pipeline, _ = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
        new_news_df=new,
        new_embeddings=np.array([[0.78, 0.625779]], dtype=np.float32),
    )

    assignment = result.assignments.iloc[0]
    assert assignment["cluster_id"] == "event-1"
    assert assignment["assignment_method"] == "attach"


def test_ambiguous_existing_clusters_are_not_merged() -> None:
    historical = _news(
        [
            _row(
                "old-1",
                "2026-01-01",
                title="Банк обсудил ставку 12 процентов",
                cluster_id="event-1",
            ),
            _row(
                "old-2",
                "2026-01-01",
                title="Комиссия оценила ставку 12 процентов",
                cluster_id="event-2",
            ),
        ]
    )
    new = _news(
        [_row("new-1", "2026-01-02", title="Решение по ставке 12 процентов")]
    )
    pipeline, _ = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.array(
            [
                [0.8, 0.6],
                [0.79, 0.613106],
            ],
            dtype=np.float32,
        ),
        new_news_df=new,
        new_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
    )

    assignment = result.assignments.iloc[0]
    assert assignment["cluster_id"] not in {"event-1", "event-2"}
    assert assignment["assignment_method"] == "baseline"
    assert assignment["update_method"] == "new_cluster_ambiguous"
    assert bool(assignment["assignment_needs_review"])
    assert bool(result.predictions.iloc[0]["needs_review"])
    assert historical["cluster_id"].tolist() == ["event-1", "event-2"]


def test_baseline_bridge_merges_existing_clusters() -> None:
    historical = _news(
        [
            _row(
                "old-1",
                "2026-01-01",
                title="Первая часть сюжета",
                cluster_id="event-1",
            ),
            _row(
                "old-2",
                "2026-01-02",
                title="Вторая часть сюжета",
                cluster_id="event-2",
            ),
        ]
    )
    new = _news([_row("new-1", "2026-01-03", title="Общее продолжение")])
    pipeline, _ = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.array(
            [
                [1.0, 0.0],
                [0.9, 0.43589],
            ],
            dtype=np.float32,
        ),
        new_news_df=new,
        new_embeddings=np.array([[0.99, 0.14]], dtype=np.float32),
    )

    new_assignment = result.assignments[result.assignments["news_id"].eq("new-1")].iloc[0]
    historical_updates = result.assignments[
        result.assignments["update_method"].eq("cluster_merge")
    ]
    assert new_assignment["assignment_method"] == "baseline"
    assert new_assignment["update_method"] == "baseline_merge"
    assert new_assignment["cluster_id"] in {"event-1", "event-2"}
    assert len(historical_updates) == 1
    assert historical_updates.iloc[0]["previous_cluster_id"] in {"event-1", "event-2"}
    assert historical_updates.iloc[0]["cluster_id"] == new_assignment["cluster_id"]
    assert result.updated_ids == ["new-1", "old-1", "old-2"]
    assert result.diagnostics["merged_clusters"] == 1


def test_baseline_merge_updates_earlier_assignment_in_same_batch() -> None:
    historical = pd.DataFrame(
        columns=["news_id", "published_at", "topic", "title", "text", "cluster_id"]
    )
    new = _news(
        [
            _row("new-1", "2026-01-01", title="Первая линия"),
            _row("new-2", "2026-01-02", title="Вторая линия"),
            _row("new-3", "2026-01-03", title="Связующая публикация"),
        ]
    )
    pipeline, _ = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.empty((0, 3), dtype=np.float32),
        new_news_df=new,
        new_embeddings=np.array(
            [
                [0.84, 0.542586, 0.0],
                [0.84, -0.542586, 0.0],
                [1.0, 0.0, 0.0],
            ],
            dtype=np.float32,
        ),
    )

    assert result.assignments["cluster_id"].nunique() == 1
    assert result.predictions["cluster_id"].nunique() == 1
    assert result.assignments.iloc[-1]["update_method"] == "baseline_merge"


def test_cluster_merge_preserves_attach_provenance() -> None:
    historical = _news(
        [
            _row(
                "old-1",
                "2026-01-01",
                title="Основная линия",
                cluster_id="event-1",
            ),
            _row(
                "old-2",
                "2026-01-02",
                title="Ранее присоединённая линия",
                cluster_id="event-2",
            ),
        ]
    )
    historical["baseline_component_id"] = ["base-1", "base-2"]
    historical["assignment_method"] = ["baseline", "attach"]
    historical["assignment_parent_news_id"] = [pd.NA, "parent-2"]
    historical["assignment_similarity"] = [np.nan, 0.78]
    historical["attached_to_component_id"] = [pd.NA, "base-parent"]
    new = _news([_row("new-1", "2026-01-03", title="Связующая публикация")])
    pipeline, _ = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.array(
            [
                [1.0, 0.0],
                [0.9, 0.43589],
            ],
            dtype=np.float32,
        ),
        new_news_df=new,
        new_embeddings=np.array([[0.99, 0.14]], dtype=np.float32),
    )

    attached_update = result.assignments[result.assignments["news_id"].eq("old-2")].iloc[0]
    assert attached_update["assignment_method"] == "attach"
    assert attached_update["update_method"] == "cluster_merge"
    assert attached_update["assignment_parent_news_id"] == "parent-2"
    assert attached_update["attached_to_component_id"] == "base-parent"


def test_new_batch_is_processed_chronologically_and_embeddings_stay_aligned() -> None:
    historical = pd.DataFrame(
        columns=[
            "news_id",
            "published_at",
            "topic",
            "title",
            "text",
            "cluster_id",
        ]
    )
    new = _news(
        [
            _row("new-2", "2026-01-02", title="Продолжение сюжета"),
            _row("new-1", "2026-01-01", title="Начало сюжета"),
        ]
    )
    pipeline, novelty_model = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.empty((0, 2), dtype=np.float32),
        new_news_df=new,
        new_embeddings=np.array(
            [
                [0.9, 0.1],
                [1.0, 0.0],
            ],
            dtype=np.float32,
        ),
    )

    assert result.requested_ids == ["new-1", "new-2"]
    assert result.assignments["cluster_id"].nunique() == 1
    assert result.assignments["assignment_method"].tolist() == ["baseline", "baseline"]
    assert result.assignments["update_method"].tolist() == ["new_cluster", "baseline"]
    assert result.embedding_ids == ["new-1", "new-2"]
    np.testing.assert_allclose(result.embeddings[0], [1.0, 0.0])
    np.testing.assert_allclose(result.embeddings[1], [0.9, 0.1])
    assert novelty_model.embeddings is not None
    np.testing.assert_allclose(novelty_model.embeddings[0], [1.0, 0.0])
    np.testing.assert_allclose(novelty_model.embeddings[1], [0.9, 0.1])


def test_late_arrival_uses_future_article_and_recalculates_later_cluster_items() -> None:
    historical = _news(
        [
            _row(
                "past-1",
                "2026-01-01",
                title="Начало сюжета",
                cluster_id="event-1",
            ),
            _row(
                "future-1",
                "2026-01-10",
                title="Продолжение сюжета",
                cluster_id="event-1",
            )
        ]
    )
    new = _news([_row("late-1", "2026-01-05", title="Опоздавшая публикация")])
    pipeline, _ = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.array(
            [
                [0.0, 1.0],
                [1.0, 0.0],
            ],
            dtype=np.float32,
        ),
        new_news_df=new,
        new_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
    )

    assignment = result.assignments.iloc[0]
    assert assignment["cluster_id"] == "event-1"
    assert assignment["assignment_method"] == "baseline"
    assert bool(assignment["late_arrival"])
    assert assignment["affected_historical_count"] == 1
    assert result.predictions["news_id"].tolist() == ["late-1", "future-1"]
    assert result.updated_ids == ["late-1", "future-1"]
    assert result.diagnostics["recalculated_news_ids"] == ["future-1"]


def test_article_is_not_late_relative_to_unrelated_newer_cluster() -> None:
    historical = _news(
        [
            _row(
                "target-1",
                "2026-01-01",
                title="Целевой сюжет",
                cluster_id="event-target",
            ),
            _row(
                "unrelated-1",
                "2026-01-10",
                title="Другой сюжет",
                topic="politics",
                cluster_id="event-unrelated",
            ),
        ]
    )
    new = _news([_row("new-1", "2026-01-05", title="Продолжение целевого сюжета")])
    pipeline, _ = _pipeline()

    result = pipeline.process(
        historical_news_df=historical,
        historical_embeddings=np.array(
            [
                [1.0, 0.0],
                [0.0, 1.0],
            ],
            dtype=np.float32,
        ),
        new_news_df=new,
        new_embeddings=np.array([[1.0, 0.0]], dtype=np.float32),
    )

    assignment = result.assignments.iloc[0]
    assert assignment["cluster_id"] == "event-target"
    assert not bool(assignment["late_arrival"])
    assert result.updated_ids == ["new-1"]
