from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .config import SignificanceModelConfig
from .data import normalize_news_id
from .features import LEGACY_SIGNIFICANCE_FEATURE_COLUMNS
from .significance_model import CatBoostSignificanceModel


@dataclass(frozen=True)
class ThresholdSearchResult:
    threshold: float
    f1: float
    precision: float
    recall: float


def _sklearn_features(frame: pd.DataFrame, feature_columns: list[str] | tuple[str, ...]) -> pd.DataFrame:
    """Return finite numeric features while preserving sklearn feature names."""

    features = frame.loc[:, list(feature_columns)].copy().astype(np.float32)
    return features.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _target_vector(frame: pd.DataFrame) -> np.ndarray:
    return frame["is_significant"].to_numpy(dtype=np.int8, copy=False)


def make_significance_training_frame(
    features_df: pd.DataFrame,
    labels_df: pd.DataFrame,
    *,
    eval_df: pd.DataFrame | None = None,
    feature_columns: list[str] | tuple[str, ...] = LEGACY_SIGNIFICANCE_FEATURE_COLUMNS,
    id_column: str = "news_id",
    label_column: str = "novelty_label",
) -> pd.DataFrame:
    """Собирает train frame для binary significant/not-significant модели.

    Golden/eval строки можно исключить через `eval_df`, чтобы не было прямой утечки.
    """

    features = features_df.copy()
    labels = labels_df[[id_column, label_column]].copy()
    features[id_column] = normalize_news_id(features[id_column])
    labels[id_column] = normalize_news_id(labels[id_column])
    labels[label_column] = labels[label_column].fillna("").astype(str).str.strip().str.lower()

    if eval_df is not None:
        eval_ids = set(normalize_news_id(eval_df[id_column]))
        labels = labels[~labels[id_column].isin(eval_ids)].copy()

    frame = features.merge(labels, on=id_column, how="inner")
    allowed = {"significant", "minor", "duplicate"}
    frame = frame[frame[label_column].isin(allowed)].copy()
    frame["is_significant"] = frame[label_column].eq("significant").astype(int)

    missing = [column for column in feature_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Нет feature columns: {missing}")

    return frame.reset_index(drop=True)


def train_validation_split(
    frame: pd.DataFrame,
    *,
    validation_size: float = 0.25,
    random_state: int = 42,
    group_column: str = "cluster_id",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Делит train frame на train/validation по cluster_id, если это возможно."""

    if group_column in frame.columns and frame[group_column].nunique() >= 3:
        from sklearn.model_selection import GroupShuffleSplit

        splitter = GroupShuffleSplit(n_splits=1, test_size=validation_size, random_state=random_state)
        train_idx, val_idx = next(splitter.split(frame, groups=frame[group_column].astype(str)))
        return frame.iloc[train_idx].copy(), frame.iloc[val_idx].copy()

    from sklearn.model_selection import train_test_split

    train, val = train_test_split(
        frame,
        test_size=validation_size,
        random_state=random_state,
        stratify=frame["is_significant"] if frame["is_significant"].nunique() == 2 else None,
    )
    return train.copy(), val.copy()


def find_best_threshold(
    y_true: np.ndarray | pd.Series,
    proba: np.ndarray | pd.Series,
    *,
    thresholds: np.ndarray | None = None,
) -> ThresholdSearchResult:
    """Подбирает threshold по validation F1 для класса significant."""

    y = np.asarray(y_true).astype(int)
    p = np.asarray(proba, dtype=float)
    thresholds = thresholds if thresholds is not None else np.linspace(0.05, 0.95, 181)

    best = ThresholdSearchResult(threshold=0.5, f1=-1.0, precision=0.0, recall=0.0)
    for threshold in thresholds:
        pred = p >= threshold
        tp = int(((y == 1) & pred).sum())
        fp = int(((y == 0) & pred).sum())
        fn = int(((y == 1) & (~pred)).sum())
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        if f1 > best.f1:
            best = ThresholdSearchResult(
                threshold=float(threshold),
                f1=float(f1),
                precision=float(precision),
                recall=float(recall),
            )
    return best


def wrap_predict_proba_model(
    model: Any,
    *,
    threshold: float,
    feature_columns: list[str] | tuple[str, ...] = LEGACY_SIGNIFICANCE_FEATURE_COLUMNS,
    duplicate_threshold: float = 0.90,
    review_margin: float = 0.10,
) -> CatBoostSignificanceModel:
    """Заворачивает sklearn/CatBoost модель в общий wrapper с fallback."""

    wrapper = CatBoostSignificanceModel(
        config=SignificanceModelConfig(
            threshold=float(threshold),
            duplicate_threshold=float(duplicate_threshold),
            review_margin=float(review_margin),
            feature_columns=tuple(feature_columns),
        ),
        model=model,
    )
    wrapper.feature_columns = list(feature_columns)
    return wrapper


def fit_catboost_binary(
    train_frame: pd.DataFrame,
    val_frame: pd.DataFrame,
    *,
    feature_columns: list[str] | tuple[str, ...] = LEGACY_SIGNIFICANCE_FEATURE_COLUMNS,
    random_state: int = 42,
) -> tuple[CatBoostSignificanceModel, ThresholdSearchResult]:
    """Обучает CatBoost и возвращает wrapper + threshold validation."""

    from catboost import CatBoostClassifier

    params = {
        "iterations": 500,
        "depth": 5,
        "learning_rate": 0.05,
        "l2_leaf_reg": 5.0,
        "loss_function": "Logloss",
        "eval_metric": "F1",
        "random_seed": random_state,
        "verbose": 100,
    }
    candidate = CatBoostClassifier(**params)
    candidate.fit(
        train_frame[list(feature_columns)],
        train_frame["is_significant"],
        eval_set=(val_frame[list(feature_columns)], val_frame["is_significant"]),
        use_best_model=True,
    )
    threshold = find_best_threshold(val_frame["is_significant"], candidate.predict_proba(val_frame[list(feature_columns)])[:, 1])

    final = CatBoostClassifier(**params)
    final.fit(pd.concat([train_frame, val_frame])[list(feature_columns)], pd.concat([train_frame, val_frame])["is_significant"])
    return wrap_predict_proba_model(final, threshold=threshold.threshold, feature_columns=feature_columns), threshold


def fit_mlp_binary(
    train_frame: pd.DataFrame,
    val_frame: pd.DataFrame,
    *,
    feature_columns: list[str] | tuple[str, ...] = LEGACY_SIGNIFICANCE_FEATURE_COLUMNS,
    random_state: int = 42,
) -> tuple[CatBoostSignificanceModel, ThresholdSearchResult]:
    """Обучает sklearn MLPClassifier и возвращает wrapper + threshold validation."""

    from sklearn.neural_network import MLPClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    def make_model() -> Pipeline:
        return Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "mlp",
                    MLPClassifier(
                        hidden_layer_sizes=(64, 32),
                        activation="relu",
                        alpha=1e-3,
                        learning_rate_init=1e-3,
                        max_iter=500,
                        early_stopping=True,
                        validation_fraction=0.15,
                        random_state=random_state,
                    ),
                ),
            ]
        )

    candidate = make_model()
    candidate.fit(_sklearn_features(train_frame, feature_columns), _target_vector(train_frame))
    threshold = find_best_threshold(
        _target_vector(val_frame),
        candidate.predict_proba(_sklearn_features(val_frame, feature_columns))[:, 1],
    )

    final = make_model()
    full = pd.concat([train_frame, val_frame])
    final.fit(_sklearn_features(full, feature_columns), _target_vector(full))
    return wrap_predict_proba_model(final, threshold=threshold.threshold, feature_columns=feature_columns), threshold


def fit_logreg_binary(
    train_frame: pd.DataFrame,
    val_frame: pd.DataFrame,
    *,
    feature_columns: list[str] | tuple[str, ...] = LEGACY_SIGNIFICANCE_FEATURE_COLUMNS,
    random_state: int = 42,
) -> tuple[CatBoostSignificanceModel, ThresholdSearchResult]:
    """Обучает LogisticRegression и возвращает wrapper + threshold validation."""

    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    def make_model() -> Pipeline:
        return Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "logreg",
                    LogisticRegression(
                        max_iter=1000,
                        class_weight="balanced",
                        random_state=random_state,
                        solver="liblinear",
                    ),
                ),
            ]
        )

    candidate = make_model()
    candidate.fit(_sklearn_features(train_frame, feature_columns), _target_vector(train_frame))
    threshold = find_best_threshold(
        _target_vector(val_frame),
        candidate.predict_proba(_sklearn_features(val_frame, feature_columns))[:, 1],
    )

    final = make_model()
    full = pd.concat([train_frame, val_frame])
    final.fit(_sklearn_features(full, feature_columns), _target_vector(full))
    return wrap_predict_proba_model(final, threshold=threshold.threshold, feature_columns=feature_columns), threshold
