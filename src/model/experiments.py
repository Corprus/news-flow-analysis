from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .data import ensure_prediction_schema, save_prediction_csv
from .evaluation import add_experiment_result, evaluate_predictions
from .features import build_previous_only_features


def make_prediction_from_clusterer_and_model(
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    clusterer,
    novelty_model,
    id_column: str = "news_id",
) -> pd.DataFrame:
    clustered = news_df.copy()
    clustered["cluster_id"] = clusterer.fit_predict(clustered, embeddings)
    features = build_previous_only_features(clustered, embeddings)

    if hasattr(novelty_model, "predict_eval_schema"):
        pred = novelty_model.predict_eval_schema(clustered, features, id_column=id_column)
    else:
        labelled = novelty_model.predict_labels(features)
        base = clustered[["news_id", "published_at", "topic", "title", "text", "cluster_id"]].copy()
        labelled[id_column] = labelled[id_column].astype(str)
        base[id_column] = base[id_column].astype(str)
        pred = base.drop(columns=["cluster_id"]).merge(
            labelled[[id_column, "cluster_id", "novelty_label", "needs_review"]],
            on=id_column,
            how="left",
        )
        pred["comment"] = ""
        pred = ensure_prediction_schema(pred)
    return pred


def attach_clusters_from_prediction(
    news_df: pd.DataFrame,
    prediction_df: pd.DataFrame,
    id_column: str = "news_id",
    cluster_column: str = "cluster_id",
) -> pd.DataFrame:
    """Attach saved cluster_id from an old prediction file to news_df order."""
    base = news_df.copy()
    base[id_column] = base[id_column].astype(str)

    clusters = prediction_df[[id_column, cluster_column]].copy()
    clusters[id_column] = clusters[id_column].astype(str)
    clusters = clusters.drop_duplicates(subset=[id_column], keep="first")

    out = base.drop(columns=[cluster_column], errors="ignore").merge(
        clusters,
        on=id_column,
        how="left",
    )

    missing = out[cluster_column].isna().sum()
    if missing:
        raise ValueError(
            f"Missing saved cluster_id for {missing} rows. "
            "Check that old baseline predictions and eval_news use the same candidate pool."
        )

    return out


def run_and_evaluate_experiment(
    experiment_name: str,
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    clusterer,
    novelty_model,
    golden_df: pd.DataFrame,
    predictions_dir: str | Path,
    results_table: pd.DataFrame | None = None,
    comment: str = "",
    **metadata,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    pred = make_prediction_from_clusterer_and_model(news_df, embeddings, clusterer, novelty_model)
    path = Path(predictions_dir) / f"{experiment_name}.csv"
    save_prediction_csv(pred, path)
    metrics = evaluate_predictions(golden_df, pred)
    results_table = add_experiment_result(
        results_table,
        experiment_name=experiment_name,
        metrics=metrics,
        prediction_path=str(path),
        comment=comment,
        **metadata,
    )
    return pred, results_table, metrics
