from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

from .config import SignificanceModelConfig
from .data import ensure_prediction_schema
from .embeddings import l2_normalize
from .features import DEFAULT_FEATURE_COLUMNS, build_legacy_significance_features


class CatBoostSignificanceModel:
    """Binary significant/not-significant model with deterministic post-processing."""

    def __init__(self, config: SignificanceModelConfig | None = None, model=None) -> None:
        self.config = config or SignificanceModelConfig()
        self.model = model
        self.feature_columns = list(self.config.feature_columns or DEFAULT_FEATURE_COLUMNS)

    def _make_training_frame(
        self,
        features_df: pd.DataFrame,
        labels_df: pd.DataFrame,
        id_column: str = "news_id",
        label_column: str = "novelty_label",
    ) -> tuple[pd.DataFrame, np.ndarray]:
        labels = labels_df[[id_column, label_column]].copy()
        labels[id_column] = labels[id_column].astype(str)
        labels[label_column] = labels[label_column].fillna("").astype(str).str.strip()

        frame = features_df.copy()
        frame[id_column] = frame[id_column].astype(str)
        frame = frame.merge(labels, on=id_column, how="inner")

        positives = set(self.config.positive_labels)
        negatives = set(self.config.negative_labels)
        frame = frame[frame[label_column].isin(positives | negatives)].copy()
        if frame.empty:
            raise ValueError("No trainable labels after filtering. Check novelty_label values.")
        y = frame[label_column].isin(positives).astype(int).to_numpy()
        return frame, y

    def fit(
        self,
        features_df: pd.DataFrame,
        labels_df: pd.DataFrame,
        id_column: str = "news_id",
        label_column: str = "novelty_label",
        catboost_params: dict | None = None,
    ) -> CatBoostSignificanceModel:
        try:
            from catboost import CatBoostClassifier
        except ImportError as exc:
            raise ImportError("catboost is required to train CatBoostSignificanceModel.") from exc

        frame, y = self._make_training_frame(features_df, labels_df, id_column, label_column)
        missing = [col for col in self.feature_columns if col not in frame.columns]
        if missing:
            raise ValueError(f"Missing feature columns: {missing}")

        params = {
            "iterations": 500,
            "depth": 5,
            "learning_rate": 0.05,
            "l2_leaf_reg": 5.0,
            "loss_function": "Logloss",
            "eval_metric": "F1",
            "random_seed": self.config.random_state,
            "verbose": 100,
        }
        if catboost_params:
            params.update(catboost_params)
        self.model = CatBoostClassifier(**params)
        self.model.fit(frame[self.feature_columns], y)
        return self

    def predict_proba(self, features_df: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            raise ValueError("Model is not fitted or loaded.")
        missing = [col for col in self.feature_columns if col not in features_df.columns]
        if missing:
            raise ValueError(f"Missing feature columns: {missing}")
        proba = self.model.predict_proba(features_df[self.feature_columns])[:, 1]
        return np.asarray(proba, dtype=np.float32)

    def predict_labels(self, features_df: pd.DataFrame) -> pd.DataFrame:
        proba = self.predict_proba(features_df)
        out = features_df.copy()
        out["p_significant"] = proba
        out["novelty_label"] = np.where(proba >= self.config.threshold, "significant", "minor")
        similarity_column = (
            "max_prev_similarity"
            if "max_prev_similarity" in out.columns
            else "max_prev_sim"
            if "max_prev_sim" in out.columns
            else None
        )
        if similarity_column is not None:
            duplicate_mask = (proba < self.config.threshold) & (
                out[similarity_column] >= self.config.duplicate_threshold
            )
            out.loc[duplicate_mask, "novelty_label"] = "duplicate"
        out["needs_review"] = np.abs(proba - self.config.threshold) <= self.config.review_margin
        return out

    def predict_eval_schema(
        self,
        news_df: pd.DataFrame,
        features_df: pd.DataFrame,
        id_column: str = "news_id",
    ) -> pd.DataFrame:
        pred = self.predict_labels(features_df)
        columns_from_news = ["news_id", "published_at", "topic", "title", "text"]
        base = news_df[columns_from_news].copy()
        base[id_column] = base[id_column].astype(str)
        pred[id_column] = pred[id_column].astype(str)
        result = base.merge(
            pred[[id_column, "cluster_id", "novelty_label", "needs_review"]],
            on=id_column,
            how="left",
        )
        result["comment"] = ""
        return ensure_prediction_schema(result)

    def save(self, model_path: str | Path, config_path: str | Path | None = None) -> None:
        if self.model is None:
            raise ValueError("Model is not fitted.")
        model_path = Path(model_path)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save_model(str(model_path))
        if config_path:
            cfg = {
                "threshold": self.config.threshold,
                "duplicate_threshold": self.config.duplicate_threshold,
                "review_margin": self.config.review_margin,
                "positive_labels": list(self.config.positive_labels),
                "negative_labels": list(self.config.negative_labels),
                "ignored_labels": list(self.config.ignored_labels),
                "random_state": self.config.random_state,
                "feature_columns": self.feature_columns,
            }
            Path(config_path).write_text(
                json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    @classmethod
    def load(
        cls,
        model_path: Path,
        config_path: Path | None = None,
        config: SignificanceModelConfig | None = None,
    ) -> CatBoostSignificanceModel:
        if config_path and Path(config_path).exists():
            raw = json.loads(Path(config_path).read_text(encoding="utf-8"))

            allowed_fields = {field.name for field in fields(SignificanceModelConfig)}
            filtered_raw = {key: value for key, value in raw.items() if key in allowed_fields}

            ignored_keys = sorted(set(raw) - allowed_fields)
            if ignored_keys:
                print(f"Ignored unknown config keys: {ignored_keys}")

            config = SignificanceModelConfig(**filtered_raw)

        config = config or SignificanceModelConfig()

        model_path = Path(model_path)

        if model_path.suffix.lower() == ".joblib":
            loaded = joblib.load(model_path)

            if isinstance(loaded, CatBoostClassifier):
                model = loaded
            elif isinstance(loaded, CatBoostSignificanceModel):
                return loaded
            elif isinstance(loaded, dict) and "model" in loaded:
                model = loaded["model"]
            else:
                raise TypeError(f"Unsupported joblib model object type: {type(loaded)}")
        else:
            model = CatBoostClassifier()
            model.load_model(str(model_path))

        wrapper = cls(config=config)
        wrapper.model = model
        return wrapper

    def predict_clustered_with_fallback(
        self,
        news_df: pd.DataFrame,
        embeddings: np.ndarray,
        id_column: str = "news_id",
        cluster_column: str = "cluster_id",
        topic_column: str = "topic",
        date_column: str = "published_at",
        title_column: str = "title",
        text_column: str = "text",
    ) -> pd.DataFrame:
        """Predict novelty labels for already-clustered news.

        This method reproduces the old inference logic:

        1. Inside each cluster, news are processed in chronological order.
        2. Non-first items are compared only with previous items from the same cluster.
        3. The first item of every cluster is a significant cluster seed.
        4. The CatBoost model is applied only when previous in-cluster context exists.

        Parameters
        ----------
        news_df:
            Candidate-pool dataframe with ready `cluster_id`.
            Row order must correspond to `embeddings`.
        embeddings:
            Embedding matrix where embeddings[i] corresponds to news_df.iloc[i].

        Returns
        -------
        pd.DataFrame
            Prediction dataframe in eval-like schema.
        """
        if self.model is None:
            raise ValueError("CatBoost model is not loaded/fitted")

        if len(news_df) != len(embeddings):
            raise ValueError(
                f"news_df and embeddings must have the same length: "
                f"{len(news_df)} != {len(embeddings)}"
            )

        required_columns = {id_column, cluster_column, topic_column, date_column}
        missing_columns = required_columns - set(news_df.columns)
        if missing_columns:
            raise ValueError(f"news_df is missing required columns: {sorted(missing_columns)}")

        df = news_df.copy()
        df[id_column] = df[id_column].astype(str)
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df["_row_pos"] = np.arange(len(df))

        emb = l2_normalize(np.asarray(embeddings, dtype=np.float32))

        feature_columns = list(
            getattr(self, "feature_columns", None)
            or getattr(self.config, "feature_columns", None)
            or []
        )

        if not feature_columns:
            raise ValueError("No feature columns configured for CatBoostSignificanceModel")

        threshold = float(getattr(self.config, "threshold", 0.42))
        duplicate_threshold = float(getattr(self.config, "duplicate_threshold", 0.90))
        review_margin = float(getattr(self.config, "review_margin", 0.10))

        def _predict_from_context(
            context_df: pd.DataFrame,
            context_embeddings: np.ndarray,
            reason: str,
        ) -> tuple[str, bool, str, float]:
            """Build one legacy feature row for the last row of context and classify it."""
            features_df = build_legacy_significance_features(
                news_df=context_df,
                embeddings=context_embeddings,
                cluster_column=cluster_column,
                id_column=id_column,
                topic_column=topic_column,
                date_column=date_column,
                title_column=title_column,
                text_column=text_column,
            )

            current_id = str(context_df.iloc[-1][id_column])
            current_features = features_df[features_df[id_column].astype(str) == current_id]

            if len(current_features) != 1:
                raise ValueError(
                    f"Could not build exactly one feature row for current_id={current_id}; "
                    f"got {len(current_features)}"
                )

            missing = [col for col in feature_columns if col not in current_features.columns]
            if missing:
                raise ValueError(f"Missing feature columns for prediction: {missing}")

            X = current_features[feature_columns]

            proba = float(self.model.predict_proba(X)[:, 1][0])
            max_prev_similarity = float(current_features["max_prev_similarity"].iloc[0])

            if proba >= threshold:
                label = "significant"
            else:
                label = "duplicate" if max_prev_similarity >= duplicate_threshold else "minor"

            needs_review = abs(proba - threshold) <= review_margin

            comment = (
                f"{reason}; "
                f"p_significant={proba:.4f}; "
                f"max_prev_similarity={max_prev_similarity:.4f}"
            )

            return label, needs_review, comment, proba

        prediction_rows: list[dict] = []

        sorted_df = df.sort_values(
            [cluster_column, date_column, "_row_pos"],
            kind="mergesort",
        )

        for _cluster_id, group in sorted_df.groupby(cluster_column, sort=False, dropna=False):
            group_indices = group.index.to_list()

            previous_indices_in_cluster: list[int] = []

            for position, current_idx in enumerate(group_indices):
                current = df.loc[current_idx]

                label: str
                needs_review: bool
                comment: str
                proba: float | None

                if position == 0:
                    label = "significant"
                    needs_review = False
                    comment = "cluster seed; significant by definition"
                    proba = 1.0
                else:
                    # Обычная ветка: текущая новость сравнивается только с предыдущими
                    # новостями своего же кластера.
                    context_indices = previous_indices_in_cluster + [current_idx]
                    context_df = df.loc[context_indices].copy()
                    context_embeddings = emb[context_indices]

                    label, needs_review, comment, proba = _predict_from_context(
                        context_df=context_df,
                        context_embeddings=context_embeddings,
                        reason=f"in-cluster previous context; position={position}",
                    )

                prediction_rows.append(
                    {
                        id_column: current[id_column],
                        "published_at": current.get(date_column),
                        "topic": current.get(topic_column, ""),
                        "title": current.get(title_column, ""),
                        "text": current.get(text_column, ""),
                        cluster_column: current[cluster_column],
                        "novelty_label": label,
                        "comment": comment,
                        "needs_review": bool(needs_review),
                        "p_significant": np.nan if proba is None else float(proba),
                    }
                )

                previous_indices_in_cluster.append(current_idx)

        result = pd.DataFrame(prediction_rows)

        # Возвращаем порядок исходного news_df, чтобы downstream merge/debug были предсказуемыми.
        order = df[[id_column, "_row_pos"]].copy()
        result[id_column] = result[id_column].astype(str)

        result = result.merge(order, on=id_column, how="left")
        result = (
            result.sort_values("_row_pos", kind="mergesort")
            .drop(columns=["_row_pos"])
            .reset_index(drop=True)
        )

        return result
