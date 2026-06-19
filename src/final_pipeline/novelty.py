from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from .config import LEGACY_FEATURE_COLUMNS, FinalPipelineConfig
from .embeddings import l2_normalize
from .features import LegacyFeatureBuilder


class FinalNoveltyModel:
    """Final-step модель novelty detection.

    Объект model может быть CatBoost, sklearn Pipeline/MLP/LogReg или любая модель
    с методом predict_proba. Это важно для exp_10c, где лучшим вариантом оказался
    MLP-классификатор поверх legacy features.
    """

    def __init__(self, model: Any, config: FinalPipelineConfig) -> None:
        if not hasattr(model, "predict_proba"):
            raise TypeError("model должен иметь метод predict_proba")
        self.model = model
        self.config = config
        self.feature_columns = list(config.feature_columns or LEGACY_FEATURE_COLUMNS)

    @classmethod
    def load(
        cls,
        model_path: str | Path,
        *,
        model_config_path: str | Path | None = None,
        pipeline_config: FinalPipelineConfig | None = None,
    ) -> FinalNoveltyModel:
        model_path = Path(model_path)
        config = pipeline_config or FinalPipelineConfig()

        if model_config_path is not None and Path(model_config_path).exists():
            raw = json.loads(Path(model_config_path).read_text(encoding="utf-8"))
            overrides = {}
            if "threshold" in raw:
                overrides["novelty_threshold"] = float(raw["threshold"])
            if "duplicate_threshold" in raw:
                overrides["duplicate_threshold"] = float(raw["duplicate_threshold"])
            if "review_margin" in raw:
                overrides["review_margin"] = float(raw["review_margin"])
            if "feature_columns" in raw:
                overrides["feature_columns"] = tuple(raw["feature_columns"])
            if overrides:
                config = replace(config, **overrides)

        if not model_path.exists():
            raise FileNotFoundError(f"Novelty model not found: {model_path}")

        if model_path.suffix.lower() == ".cbm":
            from catboost import CatBoostClassifier

            model = CatBoostClassifier()
            model.load_model(str(model_path))
        else:
            loaded = joblib.load(model_path)
            if isinstance(loaded, dict) and "model" in loaded:
                model = loaded["model"]
                if "feature_columns" in loaded:
                    config = replace(config, feature_columns=tuple(loaded["feature_columns"]))
                if "threshold" in loaded:
                    config = replace(config, novelty_threshold=float(loaded["threshold"]))
            elif hasattr(loaded, "model") and hasattr(loaded.model, "predict_proba"):
                model = loaded.model
                if hasattr(loaded, "feature_columns"):
                    config = replace(config, feature_columns=tuple(loaded.feature_columns))
            else:
                model = loaded

        return cls(model=model, config=config)

    def save(self, model_path: str | Path) -> None:
        model_path = Path(model_path)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "model": self.model,
                "feature_columns": self.feature_columns,
                "threshold": self.config.novelty_threshold,
                "duplicate_threshold": self.config.duplicate_threshold,
                "review_margin": self.config.review_margin,
            },
            model_path,
        )

    def _predict_one_from_context(
        self,
        context_df: pd.DataFrame,
        context_embeddings: np.ndarray,
        *,
        reason: str,
    ) -> tuple[str, bool, str, float]:
        cfg = self.config
        features = LegacyFeatureBuilder(
            cluster_column=cfg.cluster_column,
            id_column=cfg.id_column,
            topic_column=cfg.topic_column,
            date_column=cfg.date_column,
            title_column=cfg.title_column,
            text_column=cfg.text_raw_column,
        ).build(context_df, context_embeddings)

        current_id = str(context_df.iloc[-1][cfg.id_column])
        current_features = features[features[cfg.id_column].astype(str).eq(current_id)]
        if len(current_features) != 1:
            raise ValueError(
                f"Не удалось построить ровно одну строку признаков для news_id={current_id}"
            )

        missing = [
            column for column in self.feature_columns if column not in current_features.columns
        ]
        if missing:
            raise ValueError(f"Нет feature columns для novelty model: {missing}")

        proba = float(self.model.predict_proba(current_features[self.feature_columns])[:, 1][0])
        max_prev_similarity = float(current_features["max_prev_similarity"].iloc[0])
        if proba >= cfg.novelty_threshold:
            label = "significant"
        else:
            label = "duplicate" if max_prev_similarity >= cfg.duplicate_threshold else "minor"
        needs_review = abs(proba - cfg.novelty_threshold) <= cfg.review_margin
        comment = (
            f"{reason}; p_significant={proba:.4f}; max_prev_similarity={max_prev_similarity:.4f}"
        )
        return label, bool(needs_review), comment, proba

    def predict_clustered_with_fallback(
        self, news_df: pd.DataFrame, embeddings: np.ndarray
    ) -> pd.DataFrame:
        """Предсказывает novelty_label для уже кластеризованных новостей."""

        cfg = self.config
        if len(news_df) != len(embeddings):
            raise ValueError("news_df and embeddings must have the same length")
        required = [cfg.id_column, cfg.cluster_column, cfg.topic_column, cfg.date_column]
        missing = [column for column in required if column not in news_df.columns]
        if missing:
            raise ValueError(f"news_df missing columns: {missing}")

        df = news_df.copy().reset_index(drop=True)
        df[cfg.id_column] = df[cfg.id_column].astype(str)
        df[cfg.date_column] = pd.to_datetime(df[cfg.date_column], errors="coerce")
        df["_row_pos"] = np.arange(len(df))
        emb = l2_normalize(np.asarray(embeddings, dtype=np.float32))

        outputs: list[dict] = []
        sorted_df = df.sort_values(
            [cfg.cluster_column, cfg.date_column, "_row_pos"], kind="mergesort"
        )

        for _, group in sorted_df.groupby(cfg.cluster_column, sort=False, dropna=False):
            history_indices: list[int] = []
            for _, row in group.iterrows():
                idx = int(row["_row_pos"])
                label = "significant"
                needs_review = False
                comment = "cluster seed; significant by definition"
                proba = 1.0

                if history_indices:
                    context_indices = history_indices + [idx]
                    label, needs_review, comment, proba = self._predict_one_from_context(
                        df.iloc[context_indices].copy(),
                        emb[context_indices],
                        reason="cluster_context",
                    )

                outputs.append(
                    {
                        cfg.id_column: row[cfg.id_column],
                        "published_at": row.get("published_at"),
                        "topic": row.get("topic", ""),
                        "title": row.get("title", ""),
                        "text": row.get("text", ""),
                        cfg.cluster_column: row[cfg.cluster_column],
                        "novelty_label": label,
                        "comment": comment,
                        "needs_review": needs_review,
                        "p_significant": proba,
                    }
                )
                history_indices.append(idx)

        result = pd.DataFrame(outputs)
        return result.sort_values("published_at", kind="mergesort").reset_index(drop=True)
