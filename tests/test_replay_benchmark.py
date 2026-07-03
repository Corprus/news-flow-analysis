from __future__ import annotations

import numpy as np
import pandas as pd

from final_pipeline.replay_benchmark import (
    ReplayBenchmarkConfig,
    _cluster_structure_metrics,
    _service_like_history,
    _upsert_by_id,
)


def test_cluster_structure_metrics_detect_fragmentation_without_false_merge() -> None:
    comparison = pd.DataFrame(
        {
            "full": ["a", "a", "a", "b", "b"],
            "incremental": ["x", "x", "y", "z", "z"],
        }
    )

    metrics = _cluster_structure_metrics(
        comparison,
        full_column="full",
        incremental_column="incremental",
    )

    assert metrics["fragmented_full_clusters"] == 1
    assert metrics["fragmented_full_cluster_rate"] == 0.5
    assert metrics["false_merged_incremental_clusters"] == 0
    assert metrics["pairwise_precision"] == 1.0
    assert metrics["pairwise_recall"] < 1.0


def test_upsert_replaces_recalculated_predictions() -> None:
    state = pd.DataFrame(
        {
            "news_id": ["1", "2"],
            "p_significant": [0.1, 0.2],
        }
    )
    updates = pd.DataFrame(
        {
            "news_id": ["2", "3"],
            "p_significant": [0.8, 0.3],
        }
    )

    result = _upsert_by_id(state, updates, id_column="news_id").sort_values("news_id")

    np.testing.assert_allclose(result["p_significant"], [0.1, 0.8, 0.3])
    assert ReplayBenchmarkConfig().checkpoint_days == 7


def test_service_like_history_expands_window_to_full_clusters() -> None:
    history = pd.DataFrame(
        {
            "news_id": ["old-a", "near-a", "near-b", "old-c"],
            "published_at": pd.to_datetime(
                ["2026-01-01", "2026-01-20", "2026-01-21", "2025-01-01"]
            ),
            "cluster_id": ["a", "a", "b", "c"],
        }
    )
    new_news = pd.DataFrame(
        {
            "news_id": ["new"],
            "published_at": pd.to_datetime(["2026-01-22"]),
        }
    )

    result, diagnostics = _service_like_history(
        history,
        new_news,
        cfg=ReplayBenchmarkConfig(
            history_window_days=7,
            expand_history_clusters=True,
            history_cluster_expansion_max_rows=10,
        ),
    )

    assert result["news_id"].tolist() == ["old-a", "near-a", "near-b"]
    assert diagnostics["history_rows_full"] == 4
    assert diagnostics["history_rows_window"] == 2
    assert diagnostics["history_rows_used"] == 3
    assert diagnostics["history_expanded_clusters"] == 2
