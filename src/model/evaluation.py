from __future__ import annotations

from collections.abc import Iterable
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

from .data import load_annotation


def _safe_div(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


def _pairs_from_clusters(
    df: pd.DataFrame, id_column: str, cluster_column: str
) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for _, group in df[[id_column, cluster_column]].dropna().groupby(cluster_column):
        ids = sorted(group[id_column].astype(str).unique())
        if len(ids) > 1:
            pairs.update(tuple(pair) for pair in combinations(ids, 2))
    return pairs


def evaluate_clusters(
    reference_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    id_column: str = "news_id",
    cluster_column: str = "cluster_id",
) -> dict:
    ref = reference_df[[id_column, cluster_column]].copy()
    pred = prediction_df[[id_column, cluster_column]].copy()
    ref[id_column] = ref[id_column].astype(str)
    pred[id_column] = pred[id_column].astype(str)
    common_ids = sorted(set(ref[id_column]) & set(pred[id_column]))
    ref = ref[ref[id_column].isin(common_ids)]
    pred = pred[pred[id_column].isin(common_ids)]

    ref_pairs = _pairs_from_clusters(ref, id_column, cluster_column)
    pred_pairs = _pairs_from_clusters(pred, id_column, cluster_column)
    tp = len(ref_pairs & pred_pairs)
    fp = len(pred_pairs - ref_pairs)
    fn = len(ref_pairs - pred_pairs)
    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2 * precision * recall, precision + recall)
    return {
        "cluster_eval_items": len(common_ids),
        "total_ref_pairs": len(ref_pairs),
        "total_pred_pairs": len(pred_pairs),
        "tp_same_pairs": tp,
        "fp_false_merge_pairs": fp,
        "fn_missed_pairs": fn,
        "pairwise_precision": precision,
        "pairwise_recall": recall,
        "pairwise_f1": f1,
        "false_merge_rate": _safe_div(fp, tp + fp),
    }


def evaluate_significance_binary(
    reference_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    id_column: str = "news_id",
    label_column: str = "novelty_label",
    positive_label: str = "significant",
    skip_reference_labels: Iterable[str] = ("", "wrong_cluster", "unclear"),
) -> dict:
    ref = reference_df[[id_column, label_column]].copy()
    pred = prediction_df[[id_column, label_column]].copy()
    ref[id_column] = ref[id_column].astype(str)
    pred[id_column] = pred[id_column].astype(str)
    ref[label_column] = ref[label_column].fillna("").astype(str).str.strip()
    pred[label_column] = pred[label_column].fillna("").astype(str).str.strip()

    merged = ref.merge(pred, on=id_column, how="inner", suffixes=("_ref", "_pred"))
    skip = set(skip_reference_labels)
    merged = merged[~merged[f"{label_column}_ref"].isin(skip)].copy()
    if merged.empty:
        return {
            "novelty_eval_rows": 0,
            "significance_accuracy": 0.0,
            "significant_precision": 0.0,
            "significant_recall": 0.0,
            "significant_f1": 0.0,
            "significant_tp": 0,
            "significant_fp": 0,
            "significant_fn": 0,
            "significant_tn": 0,
        }

    y_true = (merged[f"{label_column}_ref"] == positive_label).to_numpy()
    y_pred = (merged[f"{label_column}_pred"] == positive_label).to_numpy()
    tp = int(np.logical_and(y_true, y_pred).sum())
    fp = int(np.logical_and(~y_true, y_pred).sum())
    fn = int(np.logical_and(y_true, ~y_pred).sum())
    tn = int(np.logical_and(~y_true, ~y_pred).sum())
    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2 * precision * recall, precision + recall)
    accuracy = _safe_div(tp + tn, len(merged))
    return {
        "novelty_eval_rows": int(len(merged)),
        "significance_accuracy": accuracy,
        "significant_precision": precision,
        "significant_recall": recall,
        "significant_f1": f1,
        "significant_tp": tp,
        "significant_fp": fp,
        "significant_fn": fn,
        "significant_tn": tn,
    }


def evaluate_predictions(
    reference: pd.DataFrame | str | Path,
    prediction: pd.DataFrame | str | Path,
    id_column: str = "news_id",
) -> dict:
    if isinstance(reference, (str, Path)):
        reference_df = load_annotation(reference, validate=False)
    else:
        reference_df = reference.copy()
    if isinstance(prediction, (str, Path)):
        prediction_df = load_annotation(prediction, validate=False)
    else:
        prediction_df = prediction.copy()

    reference_df[id_column] = reference_df[id_column].astype(str)
    prediction_df[id_column] = prediction_df[id_column].astype(str)
    ref_ids = set(reference_df[id_column])
    pred_ids = set(prediction_df[id_column])
    overlap = len(ref_ids & pred_ids)

    result = {
        "reference_rows": len(reference_df),
        "candidate_rows": len(prediction_df),
        "overlap_rows": overlap,
        "coverage": _safe_div(overlap, len(ref_ids)),
    }
    result.update(evaluate_clusters(reference_df, prediction_df, id_column=id_column))
    result.update(evaluate_significance_binary(reference_df, prediction_df, id_column=id_column))
    return result


def add_experiment_result(
    table: pd.DataFrame | None,
    experiment_name: str,
    metrics: dict,
    comment: str = "",
    **metadata,
) -> pd.DataFrame:
    row = {"experiment": experiment_name, **metadata, **metrics, "comment": comment}
    if table is None:
        return pd.DataFrame([row])
    return pd.concat([table, pd.DataFrame([row])], ignore_index=True)


def compact_metrics_table(
    results_table: pd.DataFrame,
    drop_duplicate_experiments: bool = True,
    keep: str = "last",
    exclude_experiments: list[str] | None = None,
) -> pd.DataFrame:
    """Return compact experiment metrics table.

    By default, if the same experiment was added several times during notebook
    reruns, only the latest row is shown.
    """

    if results_table is None or results_table.empty:
        return pd.DataFrame()

    table = results_table.copy()

    if exclude_experiments:
        table = table[~table["experiment"].isin(exclude_experiments)].copy()

    if drop_duplicate_experiments and "experiment" in table.columns:
        duplicated_mask = table.duplicated(subset=["experiment"], keep=False)

        if duplicated_mask.any():
            duplicated_names = table.loc[duplicated_mask, "experiment"].drop_duplicates().tolist()
            print(f"Removed duplicate experiment rows, keep={keep}: {duplicated_names}")

        table = table.drop_duplicates(subset=["experiment"], keep=keep).reset_index(drop=True)

    compact_columns = [
        "experiment",
        "pairwise_f1",
        "false_merge_rate",
        "significance_accuracy",
        "significant_precision",
        "significant_recall",
        "significant_f1",
        "comment",
    ]

    existing_columns = [column for column in compact_columns if column in table.columns]

    return table[existing_columns]
