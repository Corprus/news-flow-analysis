from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

import numpy as np
import pandas as pd
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

from .evaluation import compute_pairwise_cluster_metrics


@dataclass(frozen=True)
class ReplayBenchmarkConfig:
    bootstrap_days: int = 14
    checkpoint_days: int = 7
    max_checkpoints: int | None = None
    id_column: str = "news_id"
    date_column: str = "published_at"
    cluster_column: str = "cluster_id"
    label_column: str = "novelty_label"
    probability_column: str = "p_significant"


@dataclass
class ReplayBenchmarkResult:
    metrics: pd.DataFrame
    assignments: pd.DataFrame
    novelty_comparison: pd.DataFrame
    checkpoint_diagnostics: pd.DataFrame


def _normalize_ids(values: pd.Series) -> pd.Series:
    return values.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)


def _upsert_by_id(
    state: pd.DataFrame,
    updates: pd.DataFrame,
    *,
    id_column: str,
) -> pd.DataFrame:
    if updates.empty:
        return state.copy()
    current = state.copy()
    incoming = updates.copy()
    current[id_column] = _normalize_ids(current[id_column])
    incoming[id_column] = _normalize_ids(incoming[id_column])
    current = current[~current[id_column].isin(set(incoming[id_column]))]
    return pd.concat([current, incoming], ignore_index=True, sort=False)


def _cluster_structure_metrics(
    comparison: pd.DataFrame,
    *,
    full_column: str,
    incremental_column: str,
) -> dict:
    pairwise = compute_pairwise_cluster_metrics(
        comparison[full_column],
        comparison[incremental_column],
    )
    contingency = (
        comparison.groupby([full_column, incremental_column], observed=True)
        .size()
        .rename("rows")
        .reset_index()
    )
    full_spans = contingency.groupby(full_column, observed=True)[incremental_column].nunique()
    incremental_spans = contingency.groupby(incremental_column, observed=True)[
        full_column
    ].nunique()
    full_sizes = comparison.groupby(full_column, observed=True).size()
    incremental_sizes = comparison.groupby(incremental_column, observed=True).size()
    multi_full = full_sizes[full_sizes.gt(1)].index
    multi_incremental = incremental_sizes[incremental_sizes.gt(1)].index
    fragmented = full_spans.reindex(multi_full, fill_value=1).gt(1)
    false_merged = incremental_spans.reindex(multi_incremental, fill_value=1).gt(1)

    return {
        **pairwise,
        "adjusted_rand_index": adjusted_rand_score(
            comparison[full_column],
            comparison[incremental_column],
        ),
        "normalized_mutual_info": normalized_mutual_info_score(
            comparison[full_column],
            comparison[incremental_column],
        ),
        "full_clusters": int(comparison[full_column].nunique()),
        "incremental_clusters": int(comparison[incremental_column].nunique()),
        "cluster_count_delta": int(
            comparison[incremental_column].nunique() - comparison[full_column].nunique()
        ),
        "multi_item_full_clusters": int(len(multi_full)),
        "fragmented_full_clusters": int(fragmented.sum()),
        "fragmented_full_cluster_rate": (
            float(fragmented.mean()) if len(fragmented) else 0.0
        ),
        "fragmentation_excess_clusters": int((full_spans - 1).clip(lower=0).sum()),
        "multi_item_incremental_clusters": int(len(multi_incremental)),
        "false_merged_incremental_clusters": int(false_merged.sum()),
        "false_merged_incremental_cluster_rate": (
            float(false_merged.mean()) if len(false_merged) else 0.0
        ),
        "false_merge_excess_clusters": int((incremental_spans - 1).clip(lower=0).sum()),
    }


def _novelty_metrics(
    comparison: pd.DataFrame,
    *,
    full_label_column: str,
    incremental_label_column: str,
    full_probability_column: str,
    incremental_probability_column: str,
) -> dict:
    full_labels = comparison[full_label_column].fillna("").astype(str)
    incremental_labels = comparison[incremental_label_column].fillna("").astype(str)
    full_probability = pd.to_numeric(comparison[full_probability_column], errors="coerce")
    incremental_probability = pd.to_numeric(
        comparison[incremental_probability_column],
        errors="coerce",
    )
    valid_probability = full_probability.notna() & incremental_probability.notna()
    probability_error = (
        full_probability[valid_probability] - incremental_probability[valid_probability]
    ).abs()
    return {
        "novelty_rows": int(len(comparison)),
        "novelty_label_agreement": float(full_labels.eq(incremental_labels).mean()),
        "significance_binary_agreement": float(
            full_labels.eq("significant").eq(incremental_labels.eq("significant")).mean()
        ),
        "p_significant_mae": (
            float(probability_error.mean()) if len(probability_error) else np.nan
        ),
        "p_significant_p95_abs_error": (
            float(probability_error.quantile(0.95)) if len(probability_error) else np.nan
        ),
    }


def run_replay_benchmark(
    *,
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    full_pipeline,
    incremental_pipeline,
    config: ReplayBenchmarkConfig | None = None,
) -> ReplayBenchmarkResult:
    cfg = config or ReplayBenchmarkConfig()
    frame = news_df.copy()
    frame[cfg.id_column] = _normalize_ids(frame[cfg.id_column])
    frame[cfg.date_column] = pd.to_datetime(frame[cfg.date_column], errors="coerce")
    if frame[cfg.date_column].isna().any():
        raise ValueError(f"Invalid values in {cfg.date_column}")
    if frame[cfg.id_column].duplicated().any():
        raise ValueError(f"Duplicate values in {cfg.id_column}")

    matrix = np.asarray(embeddings, dtype=np.float32)
    if matrix.ndim != 2 or len(matrix) != len(frame):
        raise ValueError("embeddings must be a two-dimensional matrix aligned with news_df")

    position_column = "_replay_embedding_position"
    frame[position_column] = np.arange(len(frame))
    frame = frame.sort_values(
        [cfg.date_column, cfg.id_column],
        kind="mergesort",
    ).reset_index(drop=True)
    matrix = matrix[frame.pop(position_column).to_numpy(dtype=int)]

    first_date = frame[cfg.date_column].min().normalize()
    last_date = frame[cfg.date_column].max()
    bootstrap_end = first_date + pd.Timedelta(days=cfg.bootstrap_days)
    bootstrap_count = int(frame[cfg.date_column].lt(bootstrap_end).sum())
    if bootstrap_count == 0 or bootstrap_count == len(frame):
        raise ValueError("bootstrap_days must leave non-empty bootstrap and replay partitions")

    bootstrap_news = frame.iloc[:bootstrap_count].copy()
    bootstrap_embeddings = matrix[:bootstrap_count]
    bootstrap_result = full_pipeline.run(
        bootstrap_news,
        embeddings=bootstrap_embeddings,
    )
    assignment_state = bootstrap_result.assignments.copy()
    prediction_state = bootstrap_result.predictions.copy()

    checkpoint_ends: list[pd.Timestamp] = []
    checkpoint_end = bootstrap_end + pd.Timedelta(days=cfg.checkpoint_days)
    while checkpoint_end <= last_date + pd.Timedelta(days=cfg.checkpoint_days):
        checkpoint_ends.append(min(checkpoint_end, last_date + pd.Timedelta(microseconds=1)))
        if checkpoint_ends[-1] > last_date:
            break
        checkpoint_end += pd.Timedelta(days=cfg.checkpoint_days)
        if cfg.max_checkpoints is not None and len(checkpoint_ends) >= cfg.max_checkpoints:
            break

    metric_rows: list[dict] = []
    assignment_rows: list[pd.DataFrame] = []
    novelty_rows: list[pd.DataFrame] = []
    diagnostic_rows: list[dict] = []
    previous_count = bootstrap_count

    for checkpoint_number, current_end in enumerate(checkpoint_ends, start=1):
        cumulative_count = int(frame[cfg.date_column].lt(current_end).sum())
        if cumulative_count <= previous_count:
            continue
        cumulative_news = frame.iloc[:cumulative_count].copy()
        cumulative_embeddings = matrix[:cumulative_count]
        new_news = frame.iloc[previous_count:cumulative_count].copy()
        new_embeddings = matrix[previous_count:cumulative_count]

        assignment_columns = [
            column
            for column in [
                cfg.id_column,
                cfg.cluster_column,
                "baseline_component_id",
                "assignment_method",
                "assignment_parent_news_id",
                "assignment_similarity",
                "attached_to_component_id",
            ]
            if column in assignment_state.columns
        ]
        history = cumulative_news.iloc[:previous_count].merge(
            assignment_state[assignment_columns],
            on=cfg.id_column,
            how="left",
            validate="one_to_one",
        )
        incremental_started = perf_counter()
        incremental_result = incremental_pipeline.process(
            historical_news_df=history,
            historical_embeddings=matrix[:previous_count],
            new_news_df=new_news,
            new_embeddings=new_embeddings,
        )
        incremental_seconds = perf_counter() - incremental_started
        assignment_state = _upsert_by_id(
            assignment_state,
            incremental_result.assignments,
            id_column=cfg.id_column,
        )
        prediction_state = _upsert_by_id(
            prediction_state,
            incremental_result.predictions,
            id_column=cfg.id_column,
        )

        full_started = perf_counter()
        full_result = full_pipeline.run(
            cumulative_news,
            embeddings=cumulative_embeddings,
        )
        full_seconds = perf_counter() - full_started

        comparison_columns = [
            column
            for column in [
                cfg.id_column,
                cfg.cluster_column,
                "baseline_component_id",
                "assignment_method",
            ]
            if column in full_result.assignments.columns
            and column in assignment_state.columns
        ]
        assignment_comparison = full_result.assignments[comparison_columns].merge(
            assignment_state[comparison_columns],
            on=cfg.id_column,
            how="inner",
            suffixes=("_full", "_incremental"),
            validate="one_to_one",
        )
        full_cluster_column = f"{cfg.cluster_column}_full"
        incremental_cluster_column = f"{cfg.cluster_column}_incremental"
        structure_metrics = _cluster_structure_metrics(
            assignment_comparison,
            full_column=full_cluster_column,
            incremental_column=incremental_cluster_column,
        )

        prediction_columns = [
            cfg.id_column,
            cfg.label_column,
            cfg.probability_column,
        ]
        novelty_comparison = full_result.predictions[prediction_columns].merge(
            prediction_state[prediction_columns],
            on=cfg.id_column,
            how="inner",
            suffixes=("_full", "_incremental"),
            validate="one_to_one",
        )
        novelty_metrics = _novelty_metrics(
            novelty_comparison,
            full_label_column=f"{cfg.label_column}_full",
            incremental_label_column=f"{cfg.label_column}_incremental",
            full_probability_column=f"{cfg.probability_column}_full",
            incremental_probability_column=f"{cfg.probability_column}_incremental",
        )

        checkpoint_date = cumulative_news[cfg.date_column].max()
        age_days = (checkpoint_date.normalize() - bootstrap_end).days + 1
        metric_rows.append(
            {
                "checkpoint": checkpoint_number,
                "checkpoint_date": checkpoint_date,
                "days_since_bootstrap": int(age_days),
                "rows": int(cumulative_count),
                "new_rows": int(cumulative_count - previous_count),
                "incremental_seconds": float(incremental_seconds),
                "full_seconds": float(full_seconds),
                **structure_metrics,
                **novelty_metrics,
                "created_clusters_in_batch": int(
                    incremental_result.diagnostics["created_clusters"]
                ),
                "ambiguous_assignments_in_batch": int(
                    incremental_result.diagnostics["ambiguous_assignments"]
                ),
                "late_arrivals_in_batch": int(incremental_result.diagnostics["late_arrivals"]),
                "merged_clusters_in_batch": int(
                    incremental_result.diagnostics.get("merged_clusters", 0)
                ),
                "reassigned_historical_rows_in_batch": int(
                    incremental_result.diagnostics.get("reassigned_historical_rows", 0)
                ),
            }
        )

        assignment_comparison.insert(0, "checkpoint", checkpoint_number)
        assignment_comparison.insert(1, "checkpoint_date", checkpoint_date)
        assignment_rows.append(assignment_comparison)
        novelty_comparison.insert(0, "checkpoint", checkpoint_number)
        novelty_comparison.insert(1, "checkpoint_date", checkpoint_date)
        novelty_rows.append(novelty_comparison)
        diagnostic_rows.append(
            {
                "checkpoint": checkpoint_number,
                "checkpoint_date": checkpoint_date,
                **incremental_result.diagnostics,
            }
        )
        previous_count = cumulative_count

    return ReplayBenchmarkResult(
        metrics=pd.DataFrame(metric_rows),
        assignments=pd.concat(assignment_rows, ignore_index=True),
        novelty_comparison=pd.concat(novelty_rows, ignore_index=True),
        checkpoint_diagnostics=pd.DataFrame(diagnostic_rows),
    )
