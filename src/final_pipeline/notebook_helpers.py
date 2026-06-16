from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score
from sklearn.model_selection import GroupShuffleSplit

from .evaluation import compact_metrics_table, evaluate_predictions, normalize_news_id


@dataclass
class ExperimentTracker:
    """Небольшой реестр результатов экспериментов в ноутбуке."""

    predictions_dir: Path
    save_prediction_csv: Callable[[pd.DataFrame, Path], None] | None = None
    evaluate_fn: Callable[[pd.DataFrame, pd.DataFrame], dict] = evaluate_predictions

    def __post_init__(self) -> None:
        self.predictions_dir = Path(self.predictions_dir)
        self.predictions_dir.mkdir(parents=True, exist_ok=True)
        self.results_table = pd.DataFrame()

    def register(
        self,
        *,
        experiment: str,
        golden: pd.DataFrame,
        prediction: pd.DataFrame,
        embedding_variant: str,
        clustering: str,
        novelty_variant: str,
        comment: str,
    ) -> dict:
        path = self.predictions_dir / f"{experiment}.csv"
        if self.save_prediction_csv is not None:
            self.save_prediction_csv(prediction, path)
        else:
            prediction.to_csv(path, index=False)

        metrics = self.evaluate_fn(golden, prediction)
        row = {
            "experiment": experiment,
            **metrics,
            "prediction_path": str(path),
            "embedding_variant": embedding_variant,
            "clustering": clustering,
            "novelty_variant": novelty_variant,
            "comment": comment,
        }
        if not self.results_table.empty and "experiment" in self.results_table.columns:
            self.results_table = self.results_table[self.results_table["experiment"] != experiment].copy()
        self.results_table = pd.concat([self.results_table, pd.DataFrame([row])], ignore_index=True)
        return metrics

    def compact(self) -> pd.DataFrame:
        return compact_metrics_table(self.results_table)


def find_best_threshold(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    thresholds: np.ndarray | None = None,
) -> tuple[float, float]:
    """Подбирает порог по F1 на validation split."""

    if thresholds is None:
        thresholds = np.linspace(0.20, 0.80, 61)
    best_threshold = 0.42
    best_f1 = -1.0
    for threshold in thresholds:
        y_pred = (y_proba >= threshold).astype(int)
        score = f1_score(y_true, y_pred, zero_division=0)
        if score > best_f1:
            best_threshold = float(threshold)
            best_f1 = float(score)
    return best_threshold, best_f1


def make_train_frame(
    *,
    features_df: pd.DataFrame,
    silver_labels: pd.DataFrame,
    golden: pd.DataFrame,
    feature_columns: list[str] | tuple[str, ...],
    label_column: str = "novelty_label",
    id_column: str = "news_id",
) -> pd.DataFrame:
    """Собирает train frame из silver и исключает все news_id из golden."""

    labels = silver_labels.copy()
    labels[id_column] = normalize_news_id(labels[id_column])
    if label_column not in labels.columns and "source_novelty_label" in labels.columns:
        label_column = "source_novelty_label"
    labels[label_column] = labels[label_column].fillna("").astype(str).str.strip()

    features = features_df.copy()
    features[id_column] = normalize_news_id(features[id_column])
    frame = features.merge(labels[[id_column, label_column]], on=id_column, how="inner")
    frame = frame[frame[label_column].isin({"significant", "minor", "duplicate"})].copy()

    golden_ids = set(normalize_news_id(golden[id_column]))
    frame = frame[~frame[id_column].isin(golden_ids)].copy()
    frame["is_significant"] = frame[label_column].eq("significant").astype(int)

    missing = [column for column in feature_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Нет feature columns: {missing}")
    return frame.reset_index(drop=True)


def split_train_validation(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str] | tuple[str, ...],
    group_column: str = "cluster_id",
    target_column: str = "is_significant",
    test_size: float = 0.2,
    random_state: int = 42,
):
    """Group split по cluster_id, чтобы не смешивать один сюжет между train/validation."""

    splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=random_state)
    groups = frame[group_column].astype(str) if group_column in frame.columns else frame.index.astype(str)
    train_idx, val_idx = next(splitter.split(frame, groups=groups))
    train = frame.iloc[train_idx].copy()
    val = frame.iloc[val_idx].copy()
    return (
        train[list(feature_columns)],
        train[target_column].astype(int).to_numpy(),
        val[list(feature_columns)],
        val[target_column].astype(int).to_numpy(),
        train,
        val,
    )
