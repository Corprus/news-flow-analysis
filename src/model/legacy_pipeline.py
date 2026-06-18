from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .legacy_clustering import LegacyBaselineGraphClusterer, LegacyBaselineGraphClustererConfig
from .data import prepare_legacy_baseline_input
from .embeddings import get_or_create_id_aligned_embeddings


@dataclass
class LegacyNewsPipelineConfig:
    """Config for the reproduced legacy semantic-news pipeline."""

    embeddings_cache_path: Path
    story_threshold: float = 0.82
    story_window_days: int = 14
    use_topic_blocking: bool = True
    normalize_embeddings_for_clustering: bool = True


@dataclass
class LegacyNewsPipelineResult:
    """Artifacts produced by the legacy pipeline."""

    predictions: pd.DataFrame
    clustered_news: pd.DataFrame
    embeddings: np.ndarray
    clusterer: LegacyBaselineGraphClusterer


class LegacyNewsNoveltyPipeline:
    """Full reproduced old pipeline.

    Pipeline:
        raw candidate pool
        -> legacy input preparation
        -> BGE-M3 embeddings with id-aware cache
        -> legacy graph clustering
        -> CatBoost significance model with first-item fallback
    """

    def __init__(
        self,
        encoder,
        novelty_model,
        config: LegacyNewsPipelineConfig,
    ) -> None:
        self.encoder = encoder
        self.novelty_model = novelty_model
        self.config = config

    def predict(
        self,
        raw_df: pd.DataFrame,
    ) -> LegacyNewsPipelineResult:
        news = prepare_legacy_baseline_input(raw_df)

        embeddings = get_or_create_id_aligned_embeddings(
            encoder=self.encoder,
            df=news,
            cache_path=self.config.embeddings_cache_path,
            id_column="news_id",
            text_column="model_text",
        )

        clusterer = LegacyBaselineGraphClusterer(
            LegacyBaselineGraphClustererConfig(
                story_threshold=self.config.story_threshold,
                story_window_days=self.config.story_window_days,
                use_topic_blocking=self.config.use_topic_blocking,
                normalize_embeddings=self.config.normalize_embeddings_for_clustering,
            )
        )

        cluster_ids = clusterer.fit_predict(
            news_df=news,
            embeddings=embeddings,
        )

        clustered_news = news.copy()
        clustered_news["cluster_id"] = cluster_ids.values

        predictions = self.novelty_model.predict_clustered_with_fallback(
            news_df=clustered_news,
            embeddings=embeddings,
        )

        return LegacyNewsPipelineResult(
            predictions=predictions,
            clustered_news=clustered_news,
            embeddings=embeddings,
            clusterer=clusterer,
        )