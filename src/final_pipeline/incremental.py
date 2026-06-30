from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd

from model.attach_clustering import extract_numbers, jaccard, tokenize_for_jaccard
from model.data import normalize_news_id, prepare_legacy_baseline_input
from model.embeddings import SentenceTransformerEncoder, l2_normalize
from model.significance_model import CatBoostSignificanceModel

from .config import (
    FINAL_MODEL_RELATIVE_PATH,
    FINAL_PIPELINE_CONFIG_RELATIVE_PATH,
    FinalPipelineConfig,
)
from .result import PipelineResult, PipelineVersions


@dataclass(frozen=True)
class IncrementalPipelineConfig:
    """Rules for assigning new articles and preserving baseline graph connectivity."""

    baseline_similarity: float = 0.82
    baseline_window_days: int = 14
    attach_similarity: float = 0.75
    attach_window_days: int = 7
    min_margin: float = 0.03
    title_jaccard_threshold: float = 0.15
    min_shared_numbers: int = 1
    merge_baseline_connected_clusters: bool = True
    new_cluster_prefix: str = "incremental"
    baseline_component_column: str = "baseline_component_id"
    id_column: str = "news_id"
    cluster_column: str = "cluster_id"
    topic_column: str = "topic"
    date_column: str = "published_at"
    title_column: str = "title"
    text_column: str = "text"
    text_embedding_column: str = "model_text"


IncrementalPipelineResult = PipelineResult
ProgressCallback = Callable[[str, dict[str, Any]], None]


class IncrementalNewsNoveltyPipeline:
    """Assign new articles to stable clusters and predict novelty incrementally.

    A new article is assigned to at most one resulting cluster. When it has baseline
    edges to several existing clusters, those clusters are merged because the full
    baseline graph would place them in the same connected component. Ambiguous
    evidence-aware attach candidates are not merged.
    """

    def __init__(
        self,
        *,
        encoder,
        novelty_model: CatBoostSignificanceModel,
        config: IncrementalPipelineConfig | None = None,
        final_config: FinalPipelineConfig | None = None,
    ) -> None:
        self.encoder = encoder
        self.novelty_model = novelty_model
        self.config = config or IncrementalPipelineConfig()
        self.final_config = final_config or FinalPipelineConfig()

    def encode_new_embeddings(self, news_df: pd.DataFrame) -> tuple[list[str], np.ndarray]:
        cfg = self.config
        news = self._prepare_without_embeddings(news_df)
        if news.empty:
            raise ValueError("news_df must contain at least one article")
        embeddings = self.encoder.encode_dataframe(
            news,
            text_column=cfg.text_embedding_column,
            id_column=cfg.id_column,
            cache_path=None,
            force_recompute=True,
        )
        return (
            news[cfg.id_column].astype(str).tolist(),
            np.asarray(embeddings, dtype=np.float32),
        )

    def process(
        self,
        *,
        historical_news_df: pd.DataFrame,
        historical_embeddings: np.ndarray,
        new_news_df: pd.DataFrame,
        new_embeddings: np.ndarray | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> IncrementalPipelineResult:
        process_started_at = perf_counter()

        def report_progress(stage: str, **details: Any) -> None:
            if progress_callback is None:
                return
            progress_callback(
                stage,
                {
                    "elapsed_seconds": round(perf_counter() - process_started_at, 3),
                    **details,
                },
            )

        cfg = self.config
        historical_news, historical_embeddings = self._prepare_with_embeddings(
            historical_news_df,
            historical_embeddings,
            frame_name="historical_news_df",
        )
        self._validate_history(historical_news)
        report_progress(
            "prepare_history",
            historical_rows=int(len(historical_news)),
        )

        if new_embeddings is None:
            new_news = self._prepare_without_embeddings(new_news_df)
            new_embeddings = self.encoder.encode_dataframe(
                new_news,
                text_column=cfg.text_embedding_column,
                id_column=cfg.id_column,
                cache_path=None,
                force_recompute=True,
            )
            new_embeddings = np.asarray(new_embeddings, dtype=np.float32)
        else:
            new_news, new_embeddings = self._prepare_with_embeddings(
                new_news_df,
                new_embeddings,
                frame_name="new_news_df",
            )
        report_progress(
            "prepare_new_batch",
            new_rows=int(len(new_news)),
        )

        if new_news.empty:
            raise ValueError("new_news_df must contain at least one article")
        self._validate_embedding_dimensions(historical_embeddings, new_embeddings)
        self._validate_ids(historical_news, new_news)

        history = historical_news.copy().reset_index(drop=True)
        history_embeddings = l2_normalize(np.asarray(historical_embeddings, dtype=np.float32))
        normalized_new_embeddings = l2_normalize(np.asarray(new_embeddings, dtype=np.float32))

        existing_cluster_ids = set(history[cfg.cluster_column].astype(str))
        existing_component_ids = set(history[cfg.baseline_component_column].astype(str))
        historical_ids = set(historical_news[cfg.id_column].astype(str))
        affected_historical_ids: set[str] = set()
        reassigned_historical_rows: list[dict] = []
        reassigned_historical_ids: set[str] = set()
        assignment_rows: list[dict] = []
        merged_cluster_count = 0
        merged_component_count = 0
        progress_interval = max(1, min(1_000, max(len(new_news) // 20, 1)))
        report_progress(
            "cluster_assignment",
            completed_rows=0,
            total_rows=int(len(new_news)),
            history_rows=int(len(history)),
        )

        for new_position, current in new_news.iterrows():
            current_embedding = normalized_new_embeddings[new_position]
            candidates = self._score_candidate_clusters(
                history=history,
                history_embeddings=history_embeddings,
                current=current,
                current_embedding=current_embedding,
                allow_future=True,
            )
            merge = self._merge_baseline_connected_clusters(
                history=history,
                candidates=candidates,
            )
            if merge is not None:
                (
                    canonical_cluster_id,
                    canonical_component_id,
                    merged_cluster_ids,
                    merged_component_ids,
                    changed_rows,
                ) = merge
                merged_cluster_count += len(merged_cluster_ids) - 1
                merged_component_count += len(merged_component_ids) - 1
                for row in changed_rows:
                    news_id = str(row[cfg.id_column])
                    if news_id in historical_ids and news_id not in reassigned_historical_ids:
                        reassigned_historical_rows.append(row)
                        reassigned_historical_ids.add(news_id)
                affected_historical_ids.update(
                    history.loc[
                        history[cfg.cluster_column].astype(str).eq(canonical_cluster_id)
                        & history[cfg.id_column].astype(str).isin(historical_ids),
                        cfg.id_column,
                    ].astype(str)
                )
                candidates = candidates[
                    ~candidates[cfg.cluster_column].astype(str).isin(merged_cluster_ids)
                    | candidates[cfg.cluster_column].astype(str).eq(canonical_cluster_id)
                ].copy()
                candidates.loc[
                    candidates[cfg.cluster_column].astype(str).eq(canonical_cluster_id),
                    cfg.cluster_column,
                ] = canonical_cluster_id
                candidates = candidates.drop_duplicates(
                    subset=[cfg.cluster_column],
                    keep="first",
                )
                candidates.loc[
                    candidates[cfg.cluster_column].astype(str).eq(canonical_cluster_id),
                    "baseline_component_ids",
                ] = pd.Series(
                    [[canonical_component_id]],
                    index=candidates.index[
                        candidates[cfg.cluster_column].astype(str).eq(
                            canonical_cluster_id
                        )
                    ],
                )
            assignment = self._select_assignment(
                current=current,
                candidates=candidates,
                existing_cluster_ids=existing_cluster_ids,
                existing_component_ids=existing_component_ids,
            )
            if merge is not None:
                assignment["assignment_method"] = "baseline"
                assignment["update_method"] = "baseline_merge"
                assignment["merged_cluster_ids"] = merged_cluster_ids
                assignment["merged_cluster_count"] = len(merged_cluster_ids) - 1
                assignment["merged_baseline_component_ids"] = merged_component_ids
            else:
                assignment.setdefault("update_method", assignment["assignment_method"])
                assignment["merged_cluster_ids"] = []
                assignment["merged_cluster_count"] = 0
                assignment["merged_baseline_component_ids"] = []
            cluster_id = str(assignment[cfg.cluster_column])
            baseline_component_id = str(assignment[cfg.baseline_component_column])
            assigned_to_existing = assignment["update_method"] in {
                "baseline",
                "baseline_merge",
                "attach",
            }
            late_arrival = assigned_to_existing and self._is_late_arrival_for_cluster(
                history=history,
                current=current,
                cluster_id=cluster_id,
            )
            affected_ids = self._find_affected_historical_ids(
                history=history,
                historical_ids=historical_ids,
                current=current,
                cluster_id=cluster_id,
            )
            affected_historical_ids.update(affected_ids)
            assignment["late_arrival"] = bool(late_arrival)
            assignment["affected_historical_count"] = int(len(affected_ids))
            existing_cluster_ids.add(cluster_id)
            existing_component_ids.add(baseline_component_id)
            assignment_rows.append(assignment)

            history_row = current.to_dict()
            history_row[cfg.cluster_column] = cluster_id
            history_row[cfg.baseline_component_column] = baseline_component_id
            history = pd.concat([history, pd.DataFrame([history_row])], ignore_index=True)
            history_embeddings = np.vstack([history_embeddings, current_embedding])
            completed_rows = len(assignment_rows)
            if completed_rows % progress_interval == 0 or completed_rows == len(new_news):
                report_progress(
                    "cluster_assignment",
                    completed_rows=int(completed_rows),
                    total_rows=int(len(new_news)),
                    history_rows=int(len(history)),
                    assigned_to_existing=int(
                        sum(
                            row["update_method"]
                            in {"baseline", "baseline_merge", "attach"}
                            for row in assignment_rows
                        )
                    ),
                    created_clusters=int(
                        sum(
                            row["update_method"]
                            in {"new_cluster", "new_cluster_ambiguous"}
                            for row in assignment_rows
                        )
                    ),
                )

        assignments = pd.DataFrame(assignment_rows)
        final_cluster_by_id = history.set_index(cfg.id_column)[cfg.cluster_column].astype(str)
        assignments[cfg.cluster_column] = (
            assignments[cfg.id_column].astype(str).map(final_cluster_by_id)
        )
        final_component_by_id = history.set_index(cfg.id_column)[
            cfg.baseline_component_column
        ].astype(str)
        assignments[cfg.baseline_component_column] = (
            assignments[cfg.id_column].astype(str).map(final_component_by_id)
        )
        new_clustered = new_news.copy()
        new_clustered[cfg.cluster_column] = (
            new_clustered[cfg.id_column].astype(str).map(final_cluster_by_id)
        )

        historical_count = len(historical_news)
        historical_clustered = historical_news.copy()
        historical_clustered[cfg.cluster_column] = (
            historical_clustered[cfg.id_column].astype(str).map(final_cluster_by_id)
        )
        combined_news = pd.concat([historical_clustered, new_clustered], ignore_index=True)
        combined_embeddings = np.vstack([historical_embeddings, new_embeddings]).astype(np.float32)

        report_progress(
            "novelty_prediction",
            combined_rows=int(len(combined_news)),
            new_rows=int(len(new_clustered)),
            historical_rows=int(len(historical_clustered)),
        )
        all_predictions = self.novelty_model.predict_clustered_with_fallback(
            news_df=combined_news,
            embeddings=combined_embeddings,
            id_column=cfg.id_column,
            cluster_column=cfg.cluster_column,
            topic_column=cfg.topic_column,
            date_column=cfg.date_column,
            title_column=cfg.title_column,
            text_column=cfg.text_column,
        )
        report_progress(
            "novelty_prediction_done",
            prediction_rows=int(len(all_predictions)),
        )
        new_ids = set(new_clustered[cfg.id_column].astype(str))
        predictions = all_predictions[
            all_predictions[cfg.id_column].astype(str).isin(new_ids)
        ].copy()
        predictions = new_clustered[[cfg.id_column]].merge(
            predictions,
            on=cfg.id_column,
            how="left",
        )
        predictions = predictions.merge(
            assignments[
                [
                    cfg.id_column,
                    "assignment_method",
                    "update_method",
                    "assignment_similarity",
                    "second_best_similarity",
                    "assignment_margin",
                    "assignment_needs_review",
                    "late_arrival",
                    "affected_historical_count",
                    "merged_cluster_ids",
                    "merged_cluster_count",
                ]
            ],
            on=cfg.id_column,
            how="left",
        )
        if "needs_review" in predictions.columns:
            predictions["needs_review"] = (
                predictions["needs_review"].fillna(False)
                | predictions["assignment_needs_review"].fillna(False)
            )

        recalculated_predictions = all_predictions[
            all_predictions[cfg.id_column].astype(str).isin(affected_historical_ids)
        ].copy()
        if affected_historical_ids:
            affected_order = historical_news[
                historical_news[cfg.id_column].astype(str).isin(affected_historical_ids)
            ][[cfg.id_column]].copy()
            recalculated_predictions = affected_order.merge(
                recalculated_predictions,
                on=cfg.id_column,
                how="left",
            )
        persistence_predictions = pd.concat(
            [predictions, recalculated_predictions],
            ignore_index=True,
            sort=False,
        )
        requested_ids = new_clustered[cfg.id_column].astype(str).tolist()
        recalculated_ids = recalculated_predictions[cfg.id_column].astype(str).tolist()
        updated_ids = list(dict.fromkeys([*requested_ids, *recalculated_ids]))
        context_ids = historical_news[cfg.id_column].astype(str).tolist()
        persistence_assignments = pd.concat(
            [assignments, pd.DataFrame(reassigned_historical_rows)],
            ignore_index=True,
            sort=False,
        )
        report_progress(
            "result_assembly",
            requested_rows=int(len(requested_ids)),
            updated_rows=int(len(updated_ids)),
            recalculated_historical_rows=int(len(affected_historical_ids)),
        )

        diagnostics = {
            "historical_rows": int(historical_count),
            "new_rows": int(len(new_news)),
            "assigned_to_existing": int(
                assignments["update_method"].isin(
                    ["baseline", "baseline_merge", "attach"]
                ).sum()
            ),
            "created_clusters": int(
                assignments["update_method"].isin(
                    ["new_cluster", "new_cluster_ambiguous"]
                ).sum()
            ),
            "ambiguous_assignments": int(assignments["assignment_needs_review"].sum()),
            "late_arrivals": int(assignments["late_arrival"].sum()),
            "recalculated_historical_rows": int(len(affected_historical_ids)),
            "recalculated_news_ids": sorted(affected_historical_ids),
            "merged_clusters": int(merged_cluster_count),
            "merged_baseline_components": int(merged_component_count),
            "reassigned_historical_rows": int(len(reassigned_historical_ids)),
        }
        return PipelineResult(
            mode="incremental",
            requested_ids=requested_ids,
            updated_ids=updated_ids,
            context_ids=context_ids,
            predictions=persistence_predictions,
            assignments=persistence_assignments,
            embedding_ids=requested_ids.copy(),
            embeddings=np.asarray(new_embeddings, dtype=np.float32),
            diagnostics=diagnostics,
            versions=self._versions(),
        )

    def _prepare_without_embeddings(self, news_df: pd.DataFrame) -> pd.DataFrame:
        self._validate_required_columns(news_df)
        return prepare_legacy_baseline_input(news_df)

    def _prepare_with_embeddings(
        self,
        news_df: pd.DataFrame,
        embeddings: np.ndarray,
        *,
        frame_name: str,
    ) -> tuple[pd.DataFrame, np.ndarray]:
        self._validate_required_columns(news_df)
        embeddings = np.asarray(embeddings, dtype=np.float32)
        if len(news_df) != len(embeddings):
            raise ValueError(
                f"{frame_name} and embeddings must have the same length: "
                f"{len(news_df)} != {len(embeddings)}"
            )

        position_column = "_incremental_embedding_position"
        if position_column in news_df.columns:
            raise ValueError(f"Reserved column is present in {frame_name}: {position_column}")
        positioned = news_df.copy()
        positioned[position_column] = np.arange(len(positioned))
        prepared = prepare_legacy_baseline_input(positioned)
        positions = prepared.pop(position_column).to_numpy(dtype=int)
        return prepared, embeddings[positions]

    def _validate_required_columns(self, news_df: pd.DataFrame) -> None:
        cfg = self.config
        required = {
            cfg.id_column,
            cfg.date_column,
            cfg.topic_column,
            cfg.title_column,
            cfg.text_column,
        }
        missing = required - set(news_df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

    def _validate_history(self, historical_news: pd.DataFrame) -> None:
        cfg = self.config
        cluster_column = cfg.cluster_column
        if cluster_column not in historical_news.columns:
            raise ValueError(
                f"historical_news_df must contain stable {cluster_column!r} assignments"
            )
        if historical_news[cluster_column].isna().any():
            raise ValueError(f"historical_news_df contains empty {cluster_column!r} values")
        historical_news[cluster_column] = historical_news[cluster_column].astype(str)
        if cfg.baseline_component_column not in historical_news.columns:
            historical_news[cfg.baseline_component_column] = historical_news[cluster_column]
        if historical_news[cfg.baseline_component_column].isna().any():
            raise ValueError(
                "historical_news_df contains empty "
                f"{cfg.baseline_component_column!r} values"
            )
        historical_news[cfg.baseline_component_column] = historical_news[
            cfg.baseline_component_column
        ].astype(str)
        if "assignment_method" not in historical_news.columns:
            historical_news["assignment_method"] = "baseline"
        if "assignment_parent_news_id" not in historical_news.columns:
            historical_news["assignment_parent_news_id"] = pd.NA
        if "assignment_similarity" not in historical_news.columns:
            historical_news["assignment_similarity"] = np.nan
        if "attached_to_component_id" not in historical_news.columns:
            historical_news["attached_to_component_id"] = pd.NA

    def _validate_ids(
        self,
        historical_news: pd.DataFrame,
        new_news: pd.DataFrame,
    ) -> None:
        id_column = self.config.id_column
        historical_ids = normalize_news_id(historical_news[id_column])
        new_ids = normalize_news_id(new_news[id_column])
        if historical_ids.duplicated().any():
            raise ValueError("historical_news_df contains duplicate news_id values")
        if new_ids.duplicated().any():
            raise ValueError("new_news_df contains duplicate news_id values")
        overlap = set(historical_ids) & set(new_ids)
        if overlap:
            examples = sorted(overlap)[:10]
            raise ValueError(f"New articles are already present in history: {examples}")

    @staticmethod
    def _validate_embedding_dimensions(
        historical_embeddings: np.ndarray,
        new_embeddings: np.ndarray,
    ) -> None:
        if historical_embeddings.ndim != 2:
            raise ValueError("historical_embeddings must be a two-dimensional matrix")
        if new_embeddings.ndim != 2:
            raise ValueError("new_embeddings must be a two-dimensional matrix")
        if historical_embeddings.shape[1] != new_embeddings.shape[1]:
            raise ValueError(
                "Historical and new embedding dimensions differ: "
                f"{historical_embeddings.shape[1]} != {new_embeddings.shape[1]}"
            )

    def _score_candidate_clusters(
        self,
        *,
        history: pd.DataFrame,
        history_embeddings: np.ndarray,
        current: pd.Series,
        current_embedding: np.ndarray,
        allow_future: bool,
    ) -> pd.DataFrame:
        cfg = self.config
        if history.empty:
            return pd.DataFrame()

        current_date = current[cfg.date_column]
        dates = pd.to_datetime(history[cfg.date_column], errors="coerce")
        signed_delta_days = (current_date - dates).dt.total_seconds() / (24 * 60 * 60)
        distance_days = signed_delta_days.abs() if allow_future else signed_delta_days
        temporal_mask = distance_days.le(cfg.baseline_window_days)
        if not allow_future:
            temporal_mask &= signed_delta_days.ge(0)
        candidate_mask = history[cfg.topic_column].astype(str).eq(
            str(current[cfg.topic_column])
        ) & temporal_mask
        candidate_indices = history.index[candidate_mask].to_numpy(dtype=int)
        if len(candidate_indices) == 0:
            return pd.DataFrame()

        similarities = history_embeddings[candidate_indices] @ current_embedding
        current_title_tokens = tokenize_for_jaccard(current[cfg.title_column])
        current_numbers = extract_numbers(
            current[cfg.title_column],
            current[cfg.text_column],
        )
        rows: list[dict] = []
        for index, similarity in zip(candidate_indices, similarities, strict=True):
            previous = history.loc[index]
            days = float(distance_days.loc[index])
            title_similarity = jaccard(
                tokenize_for_jaccard(previous[cfg.title_column]),
                current_title_tokens,
            )
            shared_numbers = len(
                extract_numbers(
                    previous[cfg.title_column],
                    previous[cfg.text_column],
                )
                & current_numbers
            )
            strong = float(similarity) >= cfg.baseline_similarity
            attach = (
                float(similarity) >= cfg.attach_similarity
                and days <= cfg.attach_window_days
                and (
                    title_similarity >= cfg.title_jaccard_threshold
                    or shared_numbers >= cfg.min_shared_numbers
                )
            )
            if not strong and not attach:
                continue
            rows.append(
                {
                    cfg.cluster_column: str(previous[cfg.cluster_column]),
                    cfg.baseline_component_column: str(
                        previous[cfg.baseline_component_column]
                    ),
                    "parent_news_id": str(previous[cfg.id_column]),
                    "assignment_method": "baseline" if strong else "attach",
                    "similarity": float(similarity),
                    "days_diff": days,
                    "title_jaccard": float(title_similarity),
                    "shared_numbers_count": int(shared_numbers),
                }
            )
        if not rows:
            return pd.DataFrame()

        pair_candidates = pd.DataFrame(rows)
        cluster_sizes = history[cfg.cluster_column].astype(str).value_counts()
        aggregated_rows: list[dict] = []
        for cluster_id, part in pair_candidates.groupby(cfg.cluster_column, sort=False):
            best = part.sort_values(
                ["similarity", "title_jaccard", "shared_numbers_count", "days_diff"],
                ascending=[False, False, False, True],
                kind="mergesort",
            ).iloc[0]
            aggregated_rows.append(
                {
                    cfg.cluster_column: str(cluster_id),
                    "assignment_method": (
                        "baseline"
                        if part["assignment_method"].eq("baseline").any()
                        else "attach"
                    ),
                    "similarity": float(part["similarity"].max()),
                    "cluster_size": int(cluster_sizes.get(str(cluster_id), 0)),
                    "min_days_diff": float(part["days_diff"].min()),
                    "max_title_jaccard": float(part["title_jaccard"].max()),
                    "max_shared_numbers_count": int(part["shared_numbers_count"].max()),
                    "best_pair_method": str(best["assignment_method"]),
                    "best_parent_news_id": str(best["parent_news_id"]),
                    "best_parent_component_id": str(
                        best[cfg.baseline_component_column]
                    ),
                    "baseline_component_ids": sorted(
                        part.loc[
                            part["assignment_method"].eq("baseline"),
                            cfg.baseline_component_column,
                        ]
                        .astype(str)
                        .unique()
                        .tolist()
                    ),
                }
            )
        return pd.DataFrame(aggregated_rows).sort_values(
            ["similarity", "cluster_size", "min_days_diff", cfg.cluster_column],
            ascending=[False, False, True, True],
            kind="mergesort",
        )

    def _merge_baseline_connected_clusters(
        self,
        *,
        history: pd.DataFrame,
        candidates: pd.DataFrame,
    ) -> tuple[str, str, list[str], list[str], list[dict]] | None:
        cfg = self.config
        if not cfg.merge_baseline_connected_clusters or candidates.empty:
            return None
        baseline_candidates = candidates[
            candidates["assignment_method"].eq("baseline")
        ].copy()
        if baseline_candidates.empty:
            return None
        merged_cluster_ids = (
            baseline_candidates[cfg.cluster_column].astype(str).drop_duplicates().tolist()
        )
        canonical_cluster_id = merged_cluster_ids[0]
        merged_component_ids = list(
            dict.fromkeys(
                component_id
                for component_ids in baseline_candidates["baseline_component_ids"]
                for component_id in component_ids
            )
        )
        if len(merged_component_ids) < 2:
            return None
        canonical_component_id = merged_component_ids[0]
        absorbed_cluster_ids = set(merged_cluster_ids[1:])
        absorbed_component_ids = set(merged_component_ids[1:])
        mask = history[cfg.cluster_column].astype(str).isin(absorbed_cluster_ids)
        component_mask = history[cfg.baseline_component_column].astype(str).isin(
            absorbed_component_ids
        )
        changed = history.loc[
            mask | component_mask,
            [
                cfg.id_column,
                cfg.cluster_column,
                cfg.baseline_component_column,
                "assignment_method",
                "assignment_parent_news_id",
                "assignment_similarity",
                "attached_to_component_id",
            ],
        ].copy()
        history.loc[mask, cfg.cluster_column] = canonical_cluster_id
        history.loc[component_mask, cfg.baseline_component_column] = canonical_component_id

        rows: list[dict] = []
        for _, row in changed.iterrows():
            rows.append(
                {
                    cfg.id_column: str(row[cfg.id_column]),
                    cfg.cluster_column: canonical_cluster_id,
                    cfg.baseline_component_column: (
                        canonical_component_id
                        if str(row[cfg.baseline_component_column])
                        in absorbed_component_ids
                        else str(row[cfg.baseline_component_column])
                    ),
                    "previous_cluster_id": str(row[cfg.cluster_column]),
                    "previous_baseline_component_id": str(
                        row[cfg.baseline_component_column]
                    ),
                    "assignment_method": str(row["assignment_method"]),
                    "assignment_parent_news_id": row["assignment_parent_news_id"],
                    "assignment_similarity": row["assignment_similarity"],
                    "attached_to_component_id": row["attached_to_component_id"],
                    "update_method": "cluster_merge",
                    "assignment_needs_review": False,
                    "merged_cluster_ids": merged_cluster_ids,
                    "merged_cluster_count": len(merged_cluster_ids) - 1,
                    "merged_baseline_component_ids": merged_component_ids,
                }
            )
        return (
            canonical_cluster_id,
            canonical_component_id,
            merged_cluster_ids,
            merged_component_ids,
            rows,
        )

    def _is_late_arrival_for_cluster(
        self,
        *,
        history: pd.DataFrame,
        current: pd.Series,
        cluster_id: str,
    ) -> bool:
        cfg = self.config
        cluster_history = history[
            history[cfg.cluster_column].astype(str).eq(str(cluster_id))
        ]
        if cluster_history.empty:
            return False
        latest_known_date = pd.to_datetime(
            cluster_history[cfg.date_column],
            errors="coerce",
        ).max()
        current_date = pd.to_datetime(current[cfg.date_column], errors="coerce")
        return bool(pd.notna(latest_known_date) and current_date < latest_known_date)

    def _find_affected_historical_ids(
        self,
        *,
        history: pd.DataFrame,
        historical_ids: set[str],
        current: pd.Series,
        cluster_id: str,
    ) -> list[str]:
        cfg = self.config
        current_date = pd.to_datetime(current[cfg.date_column], errors="coerce")
        if history.empty or pd.isna(current_date):
            return []
        history_ids = history[cfg.id_column].astype(str)
        history_dates = pd.to_datetime(history[cfg.date_column], errors="coerce")
        mask = (
            history_ids.isin(historical_ids)
            & history[cfg.cluster_column].astype(str).eq(cluster_id)
            & history_dates.gt(current_date)
        )
        return history.loc[mask, cfg.id_column].astype(str).tolist()

    def _select_assignment(
        self,
        *,
        current: pd.Series,
        candidates: pd.DataFrame,
        existing_cluster_ids: set[str],
        existing_component_ids: set[str],
    ) -> dict:
        cfg = self.config
        news_id = str(current[cfg.id_column])
        if candidates.empty:
            cluster_id = self._make_new_cluster_id(news_id, existing_cluster_ids)
            return {
                cfg.id_column: news_id,
                cfg.cluster_column: cluster_id,
                cfg.baseline_component_column: self._make_new_component_id(
                    news_id,
                    existing_component_ids,
                ),
                "assignment_method": "baseline",
                "update_method": "new_cluster",
                "assignment_parent_news_id": pd.NA,
                "attached_to_component_id": pd.NA,
                "assignment_similarity": np.nan,
                "second_best_similarity": np.nan,
                "assignment_margin": np.nan,
                "assignment_needs_review": False,
            }

        candidates = candidates.reset_index(drop=True)
        baseline_candidates = candidates[
            candidates["assignment_method"].eq("baseline")
        ].reset_index(drop=True)
        if not baseline_candidates.empty:
            best = baseline_candidates.iloc[0]
            return {
                cfg.id_column: news_id,
                cfg.cluster_column: str(best[cfg.cluster_column]),
                cfg.baseline_component_column: str(
                    best["baseline_component_ids"][0]
                ),
                "assignment_method": "baseline",
                "update_method": "baseline",
                "assignment_parent_news_id": str(best["best_parent_news_id"]),
                "attached_to_component_id": pd.NA,
                "assignment_similarity": float(best["similarity"]),
                "second_best_similarity": np.nan,
                "assignment_margin": np.nan,
                "assignment_needs_review": False,
            }

        best = candidates.iloc[0]
        second_similarity = (
            float(candidates.iloc[1]["similarity"]) if len(candidates) > 1 else -np.inf
        )
        margin = (
            float(best["similarity"] - second_similarity)
            if np.isfinite(second_similarity)
            else np.inf
        )
        if len(candidates) > 1 and margin < cfg.min_margin:
            cluster_id = self._make_new_cluster_id(news_id, existing_cluster_ids)
            return {
                cfg.id_column: news_id,
                cfg.cluster_column: cluster_id,
                cfg.baseline_component_column: self._make_new_component_id(
                    news_id,
                    existing_component_ids,
                ),
                "assignment_method": "baseline",
                "update_method": "new_cluster_ambiguous",
                "assignment_parent_news_id": pd.NA,
                "attached_to_component_id": pd.NA,
                "assignment_similarity": float(best["similarity"]),
                "second_best_similarity": second_similarity,
                "assignment_margin": margin,
                "assignment_needs_review": True,
            }

        return {
            cfg.id_column: news_id,
            cfg.cluster_column: str(best[cfg.cluster_column]),
            cfg.baseline_component_column: self._make_new_component_id(
                news_id,
                existing_component_ids,
            ),
            "assignment_method": str(best["assignment_method"]),
            "update_method": str(best["assignment_method"]),
            "assignment_parent_news_id": str(best["best_parent_news_id"]),
            "attached_to_component_id": str(best["best_parent_component_id"]),
            "assignment_similarity": float(best["similarity"]),
            "second_best_similarity": (
                np.nan if not np.isfinite(second_similarity) else second_similarity
            ),
            "assignment_margin": np.nan if not np.isfinite(margin) else margin,
            "assignment_needs_review": False,
        }

    def _make_new_cluster_id(self, news_id: str, existing_cluster_ids: set[str]) -> str:
        prefix = self.config.new_cluster_prefix
        digest = hashlib.sha1(news_id.encode("utf-8")).hexdigest()[:12]
        base = f"{prefix}_{digest}"
        cluster_id = base
        suffix = 1
        while cluster_id in existing_cluster_ids:
            cluster_id = f"{base}_{suffix}"
            suffix += 1
        return cluster_id

    @staticmethod
    def _make_new_component_id(news_id: str, existing_component_ids: set[str]) -> str:
        digest = hashlib.sha1(news_id.encode("utf-8")).hexdigest()[:12]
        base = f"incremental_base_{digest}"
        component_id = base
        suffix = 1
        while component_id in existing_component_ids:
            component_id = f"{base}_{suffix}"
            suffix += 1
        return component_id

    def _versions(self) -> PipelineVersions:
        cfg = self.final_config
        return PipelineVersions(
            pipeline_version=f"{cfg.pipeline_version}-incremental-v3",
            embedding_model=cfg.embedding_model_name,
            embedding_model_revision=cfg.embedding_model_revision,
            novelty_model_version=cfg.novelty_model_version,
            config_version=cfg.config_version,
        )


def load_incremental_pipeline(
    *,
    model_path: str | Path | None = None,
    final_config: FinalPipelineConfig | None = None,
    incremental_config: IncrementalPipelineConfig | None = None,
    device: str | None = None,
    project_root: str | Path | None = None,
) -> IncrementalNewsNoveltyPipeline:
    """Load the selected v3 encoder/model for incremental inference."""

    root = Path(project_root).resolve() if project_root is not None else Path.cwd().resolve()
    if final_config is None:
        config_path = root / FINAL_PIPELINE_CONFIG_RELATIVE_PATH
        final_config = (
            FinalPipelineConfig.from_json(config_path)
            if config_path.exists()
            else FinalPipelineConfig()
        )
    if model_path is None:
        resolved_model_path = root / FINAL_MODEL_RELATIVE_PATH
    else:
        resolved_model_path = Path(model_path)
        if not resolved_model_path.is_absolute():
            resolved_model_path = root / resolved_model_path

    encoder = SentenceTransformerEncoder(
        model_name=final_config.embedding_model_name,
        device=device,
        batch_size=final_config.embedding_batch_size,
        normalize_embeddings=final_config.normalize_embeddings,
        show_progress_bar=final_config.show_progress_bar,
    )
    novelty_model = CatBoostSignificanceModel.load(resolved_model_path)
    return IncrementalNewsNoveltyPipeline(
        encoder=encoder,
        novelty_model=novelty_model,
        config=incremental_config,
        final_config=final_config,
    )
