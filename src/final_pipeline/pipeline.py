from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from model.attach_clustering import (
    build_baseline_cluster_ids,
    build_best_candidate_attach_clusters,
    build_candidate_pairs,
    make_clustered_news,
)
from model.data import prepare_legacy_baseline_input
from model.embeddings import SentenceTransformerEncoder, get_or_create_id_aligned_embeddings
from model.significance_model import CatBoostSignificanceModel

from .config import (
    FINAL_MODEL_RELATIVE_PATH,
    FINAL_PIPELINE_CONFIG_RELATIVE_PATH,
    FinalPipelineConfig,
)


@dataclass
class FinalPipelineResult:
    predictions: pd.DataFrame
    clustered_news: pd.DataFrame
    embeddings: np.ndarray
    diagnostics: dict


class FinalNewsNoveltyPipeline:
    """Финальный pipeline проекта Semantic News Novelty."""

    def __init__(
        self,
        *,
        encoder: SentenceTransformerEncoder,
        novelty_model: CatBoostSignificanceModel,
        config: FinalPipelineConfig | None = None,
    ) -> None:
        self.encoder = encoder
        self.novelty_model = novelty_model
        self.config = config or FinalPipelineConfig()

    def run(
        self,
        raw_news_df: pd.DataFrame,
        *,
        embeddings_cache_path: str | Path,
        force_recompute_embeddings: bool = False,
    ) -> FinalPipelineResult:
        cfg = self.config

        news = prepare_legacy_baseline_input(raw_news_df)
        embeddings = get_or_create_id_aligned_embeddings(
            encoder=self.encoder,
            df=news,
            cache_path=embeddings_cache_path,
            id_column=cfg.id_column,
            text_column=cfg.text_column,
            force_recompute=force_recompute_embeddings,
        )

        base_ids, base_diag = build_baseline_cluster_ids(
            news,
            embeddings,
            config=cfg.base_clustering,
        )

        candidate_pairs = build_candidate_pairs(
            news,
            embeddings,
            min_similarity=cfg.attach_clustering.min_similarity,
            max_days=max(cfg.attach_clustering.max_days, cfg.base_clustering.story_window_days),
        )

        final_ids, attach_diag, selected_attachments = build_best_candidate_attach_clusters(
            news,
            candidate_pairs,
            base_ids,
            config=cfg.attach_clustering,
        )

        clustered_news = make_clustered_news(news, final_ids)
        predictions = self.novelty_model.predict_clustered_with_fallback(
            news_df=clustered_news,
            embeddings=embeddings,
        )

        diagnostics = {
            "base_clustering": base_diag,
            "attach_clustering": attach_diag,
            "attach_clustering_config": {
                "min_similarity": float(cfg.attach_clustering.min_similarity),
                "max_days": int(cfg.attach_clustering.max_days),
                "min_margin": float(cfg.attach_clustering.min_margin),
                "source_max_cluster_size": int(cfg.attach_clustering.source_max_cluster_size),
                "require_evidence": bool(cfg.attach_clustering.require_evidence),
                "title_jaccard_threshold": float(cfg.attach_clustering.title_jaccard_threshold),
                "min_shared_numbers": int(cfg.attach_clustering.min_shared_numbers),
                "cluster_prefix": str(cfg.attach_clustering.cluster_prefix),
            },
            "candidate_pairs": int(len(candidate_pairs)),
            "selected_attachments": int(len(selected_attachments)),
            "final_clusters": int(clustered_news["cluster_id"].nunique()),
        }
        return FinalPipelineResult(
            predictions=predictions,
            clustered_news=clustered_news,
            embeddings=embeddings,
            diagnostics=diagnostics,
        )


def load_pipeline(
    *,
    model_path: str | Path | None = None,
    config: FinalPipelineConfig | None = None,
    device: str | None = None,
    project_root: str | Path | None = None,
) -> FinalNewsNoveltyPipeline:
    """Создаёт pipeline из model artifact и конфигурации."""

    root = Path(project_root).resolve() if project_root is not None else Path.cwd().resolve()
    if config is None:
        config_path = root / FINAL_PIPELINE_CONFIG_RELATIVE_PATH
        cfg = (
            FinalPipelineConfig.from_json(config_path)
            if config_path.exists()
            else FinalPipelineConfig()
        )
    else:
        cfg = config
    if model_path is None:
        model_path = root / FINAL_MODEL_RELATIVE_PATH
    else:
        model_path = Path(model_path)
        if not model_path.is_absolute():
            model_path = root / model_path
    encoder = SentenceTransformerEncoder(
        model_name=cfg.embedding_model_name,
        device=device,
        batch_size=cfg.embedding_batch_size,
        normalize_embeddings=cfg.normalize_embeddings,
        show_progress_bar=cfg.show_progress_bar,
    )
    novelty_model = CatBoostSignificanceModel.load(Path(model_path))
    return FinalNewsNoveltyPipeline(encoder=encoder, novelty_model=novelty_model, config=cfg)
