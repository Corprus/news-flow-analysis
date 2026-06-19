from __future__ import annotations

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
from .result import PipelineResult, PipelineVersions

FinalPipelineResult = PipelineResult


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
        embeddings: np.ndarray | None = None,
        embeddings_cache_path: str | Path | None = None,
        force_recompute_embeddings: bool = False,
    ) -> FinalPipelineResult:
        cfg = self.config

        news, embeddings = self._prepare_news_and_embeddings(
            raw_news_df=raw_news_df,
            embeddings=embeddings,
            embeddings_cache_path=embeddings_cache_path,
            force_recompute_embeddings=force_recompute_embeddings,
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
            "mode": "full",
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
        requested_ids = news[cfg.id_column].astype(str).tolist()
        assignments = clustered_news[[cfg.id_column, "cluster_id"]].copy()
        assignments["baseline_component_id"] = (
            pd.Series(base_ids, dtype="string").astype(str).to_numpy()
        )
        assignments["assignment_method"] = "baseline"
        assignments["update_method"] = "full"
        assignments["assignment_parent_news_id"] = pd.NA
        assignments["assignment_similarity"] = np.nan
        assignments["attached_to_component_id"] = pd.NA
        if not selected_attachments.empty:
            attach_by_component = selected_attachments.set_index("source_cluster")
            source_components = set(attach_by_component.index.astype(str))
            attached_mask = assignments["baseline_component_id"].isin(source_components)
            assignments.loc[attached_mask, "assignment_method"] = "attach"
            assignments.loc[attached_mask, "assignment_parent_news_id"] = (
                assignments.loc[attached_mask, "baseline_component_id"]
                .map(attach_by_component["target_news_id"])
                .to_numpy()
            )
            assignments.loc[attached_mask, "assignment_similarity"] = (
                assignments.loc[attached_mask, "baseline_component_id"]
                .map(attach_by_component["best_similarity"])
                .to_numpy()
            )
            assignments.loc[attached_mask, "attached_to_component_id"] = (
                assignments.loc[attached_mask, "baseline_component_id"]
                .map(attach_by_component["target_cluster"])
                .to_numpy()
            )
        return PipelineResult(
            mode="full",
            requested_ids=requested_ids,
            updated_ids=requested_ids.copy(),
            context_ids=[],
            predictions=predictions,
            assignments=assignments,
            embedding_ids=requested_ids.copy(),
            embeddings=embeddings,
            diagnostics=diagnostics,
            versions=self._versions(),
        )

    def _prepare_news_and_embeddings(
        self,
        *,
        raw_news_df: pd.DataFrame,
        embeddings: np.ndarray | None,
        embeddings_cache_path: str | Path | None,
        force_recompute_embeddings: bool,
    ) -> tuple[pd.DataFrame, np.ndarray]:
        cfg = self.config
        position_column = "_final_pipeline_embedding_position"
        positioned = raw_news_df.copy()
        positioned[position_column] = np.arange(len(positioned))
        news = prepare_legacy_baseline_input(positioned)
        positions = news.pop(position_column).to_numpy(dtype=int)

        if embeddings is not None:
            matrix = np.asarray(embeddings, dtype=np.float32)
            if matrix.ndim != 2 or len(matrix) != len(raw_news_df):
                raise ValueError(
                    "embeddings must be a two-dimensional matrix aligned with raw_news_df"
                )
            return news, matrix[positions]

        if embeddings_cache_path is not None:
            matrix = get_or_create_id_aligned_embeddings(
                encoder=self.encoder,
                df=news,
                cache_path=embeddings_cache_path,
                id_column=cfg.id_column,
                text_column=cfg.text_column,
                force_recompute=force_recompute_embeddings,
            )
        else:
            matrix = self.encoder.encode_dataframe(
                news,
                text_column=cfg.text_column,
                id_column=cfg.id_column,
                cache_path=None,
                force_recompute=True,
            )
        return news, np.asarray(matrix, dtype=np.float32)

    def _versions(self) -> PipelineVersions:
        cfg = self.config
        return PipelineVersions(
            pipeline_version=cfg.pipeline_version,
            embedding_model=cfg.embedding_model_name,
            embedding_model_revision=cfg.embedding_model_revision,
            novelty_model_version=cfg.novelty_model_version,
            config_version=cfg.config_version,
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
