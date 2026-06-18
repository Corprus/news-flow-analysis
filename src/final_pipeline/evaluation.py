from __future__ import annotations

import pandas as pd
from sklearn.metrics import confusion_matrix


def normalize_news_id(series: pd.Series) -> pd.Series:
    """Приводит news_id к строковому виду, пригодному для join."""

    return series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)


def pair_count(count: int) -> int:
    return int(count * (count - 1) // 2)


def compute_pairwise_cluster_metrics(
    reference_cluster_ids: pd.Series,
    candidate_cluster_ids: pd.Series,
) -> dict:
    """Быстрые pairwise-метрики кластеризации без O(n^2) перебора пар."""

    frame = pd.DataFrame(
        {
            "ref": reference_cluster_ids.astype(str).to_numpy(),
            "cand": candidate_cluster_ids.astype(str).to_numpy(),
        }
    )

    n_items = len(frame)
    total_pairs = pair_count(n_items)

    ref_sizes = frame.groupby("ref").size()
    cand_sizes = frame.groupby("cand").size()
    joint_sizes = frame.groupby(["ref", "cand"]).size()

    total_ref_pairs = int(ref_sizes.map(pair_count).sum())
    total_pred_pairs = int(cand_sizes.map(pair_count).sum())
    tp_same_pairs = int(joint_sizes.map(pair_count).sum())

    fp_false_merge_pairs = total_pred_pairs - tp_same_pairs
    fn_missed_same_pairs = total_ref_pairs - tp_same_pairs

    precision = tp_same_pairs / total_pred_pairs if total_pred_pairs else 0.0
    recall = tp_same_pairs / total_ref_pairs if total_ref_pairs else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    false_merge_rate = fp_false_merge_pairs / total_pred_pairs if total_pred_pairs else 0.0

    return {
        "n_items": n_items,
        "total_pairs": total_pairs,
        "total_ref_pairs": total_ref_pairs,
        "total_pred_pairs": total_pred_pairs,
        "tp_same_pairs": tp_same_pairs,
        "fp_false_merge_pairs": fp_false_merge_pairs,
        "fn_missed_same_pairs": fn_missed_same_pairs,
        "pairwise_precision": precision,
        "pairwise_recall": recall,
        "pairwise_f1": f1,
        "false_merge_rate": false_merge_rate,
    }


def evaluate_cluster_ids_on_annotation(
    *,
    annotation_df: pd.DataFrame,
    candidate_news_df: pd.DataFrame,
    candidate_cluster_ids: pd.Series,
    id_column: str = "news_id",
    cluster_column: str = "cluster_id",
) -> dict:
    """Оценивает cluster_id на annotation dataframe с колонками news_id/cluster_id."""

    reference = annotation_df[[id_column, cluster_column]].copy()
    reference[id_column] = normalize_news_id(reference[id_column])
    reference[cluster_column] = reference[cluster_column].astype(str)
    reference = reference.dropna(subset=[id_column, cluster_column])
    reference = reference.drop_duplicates(subset=[id_column], keep="first")

    candidate = candidate_news_df[[id_column]].copy()
    candidate[id_column] = normalize_news_id(candidate[id_column])
    candidate[cluster_column] = candidate_cluster_ids.astype(str).to_numpy()

    merged = reference.merge(candidate, on=id_column, how="inner", suffixes=("_ref", "_pred"))

    metrics = compute_pairwise_cluster_metrics(
        reference_cluster_ids=merged[f"{cluster_column}_ref"],
        candidate_cluster_ids=merged[f"{cluster_column}_pred"],
    )
    metrics["coverage"] = len(merged) / len(reference) if len(reference) else 0.0
    return metrics


def normalize_novelty_label(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().lower()


def to_binary_significance(value: object) -> str:
    return "significant" if normalize_novelty_label(value) == "significant" else "not_significant"


def compute_binary_significance_metrics(y_true: pd.Series, y_pred: pd.Series) -> dict:
    labels = ["not_significant", "significant"]
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    tn, fp, fn, tp = cm.ravel()
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    accuracy = (tp + tn) / cm.sum() if cm.sum() else 0.0
    return {
        "novelty_eval_rows": int(cm.sum()),
        "significance_accuracy": accuracy,
        "significant_precision": precision,
        "significant_recall": recall,
        "significant_f1": f1,
        "significant_tp": int(tp),
        "significant_fp": int(fp),
        "significant_fn": int(fn),
        "significant_tn": int(tn),
    }


def evaluate_predictions(
    reference_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    *,
    id_column: str = "news_id",
    cluster_column: str = "cluster_id",
    label_column: str = "novelty_label",
) -> dict:
    """Единая end-to-end оценка prediction dataframe в eval-схеме."""

    ref = reference_df.copy()
    pred = prediction_df.copy()
    ref[id_column] = normalize_news_id(ref[id_column])
    pred[id_column] = normalize_news_id(pred[id_column])

    joined = ref.merge(pred, on=id_column, how="inner", suffixes=("_ref", "_pred"))

    cluster_metrics = compute_pairwise_cluster_metrics(
        joined[f"{cluster_column}_ref"].astype(str),
        joined[f"{cluster_column}_pred"].astype(str),
    )

    ref_label_col = f"{label_column}_ref"
    pred_label_col = f"{label_column}_pred"

    novelty = joined.copy()
    novelty["ref_label_norm"] = novelty[ref_label_col].map(normalize_novelty_label)
    novelty = novelty[novelty["ref_label_norm"].ne("")].copy()
    novelty["y_true"] = novelty[ref_label_col].map(to_binary_significance)
    novelty["y_pred"] = novelty[pred_label_col].map(to_binary_significance)

    novelty_metrics = compute_binary_significance_metrics(novelty["y_true"], novelty["y_pred"])

    return {
        "reference_rows": int(len(ref)),
        "candidate_rows": int(len(pred)),
        "overlap_rows": int(len(joined)),
        "coverage": len(joined) / len(ref) if len(ref) else 0.0,
        "cluster_eval_items": int(len(joined)),
        **cluster_metrics,
        **novelty_metrics,
    }


def compact_metrics_table(results_table: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "experiment",
        "pairwise_f1",
        "false_merge_rate",
        "significance_accuracy",
        "significant_precision",
        "significant_recall",
        "significant_f1",
        "comment",
    ]
    existing = [column for column in columns if column in results_table.columns]
    return results_table[existing].copy()
