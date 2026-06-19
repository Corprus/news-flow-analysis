from __future__ import annotations

import numpy as np
import pandas as pd

from final_pipeline.replay_benchmark import (
    ReplayBenchmarkConfig,
    _cluster_structure_metrics,
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
