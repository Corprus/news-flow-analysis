from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from .embeddings import l2_normalize


# ---------------------------------------------------------------------------
# Feature sets
# ---------------------------------------------------------------------------

LEGACY_SIGNIFICANCE_FEATURE_COLUMNS = (
    "position_in_cluster",
    "cluster_size_so_far",
    "days_since_previous",
    "days_since_cluster_start",
    "max_prev_similarity",
    "mean_prev_similarity",
    "min_prev_similarity",
    "top2_mean_similarity",
    "top3_mean_similarity",
    "last_prev_similarity",
    "previous_centroid_similarity",
    "previous_centroid_distance",
    "title_jaccard_max",
    "text_jaccard_max",
    "shared_numbers_count",
    "new_numbers_count",
    "title_length",
    "text_length",
)

# Keep DEFAULT_FEATURE_COLUMNS compatible with the saved CatBoost model from the
# previous notebook. That model was trained on 18 unnamed columns, matching the
# legacy feature order above.
DEFAULT_FEATURE_COLUMNS = LEGACY_SIGNIFICANCE_FEATURE_COLUMNS

FEATURE_COLUMNS = list(LEGACY_SIGNIFICANCE_FEATURE_COLUMNS)

# Experimental feature set from the improvement scaffold. Keep it available for
# later ablation experiments, but do not use it as the default for the saved
# legacy CatBoost model.
PREVIOUS_ONLY_EXPERIMENTAL_FEATURE_COLUMNS = (
    "prev_count",
    "position_in_cluster",
    "is_first_in_cluster",
    "max_prev_sim",
    "mean_prev_sim",
    "top3_prev_sim_mean",
    "top5_prev_sim_mean",
    "prev_centroid_sim",
    "prev_centroid_distance",
    "days_since_prev_news",
    "days_since_cluster_start",
    "cluster_density_last_3_days",
    "cluster_density_last_7_days",
    "topic_prev_count_30d",
    "topic_max_prev_sim_30d",
    "topic_top3_prev_sim_30d",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+", flags=re.IGNORECASE)
_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and np.isnan(value):
        return ""
    return str(value)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _text_len(row: pd.Series, text_column: str, length_column: str) -> int:
    """Prefer precomputed length column, fall back to len(text)."""
    if length_column in row.index and not pd.isna(row[length_column]):
        try:
            return int(row[length_column])
        except Exception:
            pass
    return len(_safe_text(row.get(text_column, "")))


def _token_set(text: object) -> set[str]:
    return {m.group(0).lower() for m in _TOKEN_RE.finditer(_safe_text(text))}


def _number_set(texts: Iterable[object]) -> set[str]:
    result: set[str] = set()
    for text in texts:
        normalized = _safe_text(text).replace(",", ".")
        result.update(m.group(0) for m in _NUMBER_RE.finditer(normalized))
    return result


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return float(len(left & right) / len(union))


def _max_jaccard(current_tokens: set[str], previous_tokens: list[set[str]]) -> float:
    if not previous_tokens:
        return 0.0
    return float(max(_jaccard(current_tokens, tokens) for tokens in previous_tokens))


def _mean_top_k(values: np.ndarray, k: int, default: float = 0.0) -> float:
    if values.size == 0:
        return default
    top = np.sort(values)[-k:]
    return float(top.mean())


def _days_between(current_date: pd.Timestamp, previous_date: pd.Timestamp | None, default: float = -1.0) -> float:
    if previous_date is None or pd.isna(current_date) or pd.isna(previous_date):
        return default
    return float((current_date - previous_date).total_seconds() / (24 * 60 * 60))


def _ensure_cluster_column(
    news_df: pd.DataFrame,
    cluster_ids: np.ndarray | list | pd.Series | None,
    cluster_column: str,
) -> pd.DataFrame:
    df = news_df.copy()
    if cluster_ids is not None:
        if len(cluster_ids) != len(df):
            raise ValueError("cluster_ids and news_df must have the same length")
        df[cluster_column] = np.asarray(cluster_ids)
    if cluster_column not in df.columns:
        raise ValueError(
            f"news_df must contain {cluster_column!r}, or cluster_ids must be provided"
        )
    return df


# ---------------------------------------------------------------------------
# Legacy 18-feature builder
# ---------------------------------------------------------------------------

def build_legacy_significance_features(
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    cluster_ids: np.ndarray | list | pd.Series | None = None,
    cluster_column: str = "cluster_id",
    id_column: str = "news_id",
    topic_column: str = "topic",
    date_column: str = "published_at",
    title_column: str = "title",
    text_column: str = "text",
    title_length_column: str = "title_length",
    text_length_column: str = "text_length",
) -> pd.DataFrame:
    """Build the 18 legacy features used by the saved CatBoost model.

    The function is previous-only: for each news item, all features are computed
    only from earlier items within the same predicted cluster.

    The returned rows are in the original `news_df` order.
    """
    if len(news_df) != len(embeddings):
        raise ValueError("news_df and embeddings must have the same length")

    df = _ensure_cluster_column(news_df, cluster_ids, cluster_column=cluster_column)
    df = df.copy()
    df["_row_pos"] = np.arange(len(df))
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    emb = l2_normalize(np.asarray(embeddings, dtype=np.float32))

    rows: list[dict] = []
    sorted_df = df.sort_values([cluster_column, date_column, "_row_pos"], kind="mergesort")

    for _, group in sorted_df.groupby(cluster_column, sort=False, dropna=False):
        history_indices: list[int] = []
        history_dates: list[pd.Timestamp] = []
        history_title_tokens: list[set[str]] = []
        history_text_tokens: list[set[str]] = []
        history_numbers_union: set[str] = set()

        cluster_start: pd.Timestamp | None = None

        for position, (_, current) in enumerate(group.iterrows()):
            idx = int(current["_row_pos"])
            current_date = current[date_column]
            current_emb = emb[idx]

            current_title = _safe_text(current.get(title_column, ""))
            current_text = _safe_text(current.get(text_column, ""))
            current_title_tokens = _token_set(current_title)
            current_text_tokens = _token_set(current_text)
            current_numbers = _number_set((current_title, current_text))

            prev_count = len(history_indices)

            if prev_count:
                prev_emb = emb[history_indices]
                sims = prev_emb @ current_emb

                centroid = l2_normalize(prev_emb.mean(axis=0, keepdims=True))[0]
                centroid_sim = float(centroid @ current_emb)

                days_since_previous = _days_between(current_date, history_dates[-1])
                days_since_cluster_start = _days_between(current_date, cluster_start)

                shared_numbers = current_numbers & history_numbers_union
                new_numbers = current_numbers - history_numbers_union

                item_features = {
                    "position_in_cluster": int(position),
                    "cluster_size_so_far": int(prev_count),
                    "days_since_previous": days_since_previous,
                    "days_since_cluster_start": days_since_cluster_start,
                    "max_prev_similarity": float(sims.max()),
                    "mean_prev_similarity": float(sims.mean()),
                    "min_prev_similarity": float(sims.min()),
                    "top2_mean_similarity": _mean_top_k(sims, 2),
                    "top3_mean_similarity": _mean_top_k(sims, 3),
                    "last_prev_similarity": float(sims[-1]),
                    "previous_centroid_similarity": centroid_sim,
                    "previous_centroid_distance": float(1.0 - centroid_sim),
                    "title_jaccard_max": _max_jaccard(current_title_tokens, history_title_tokens),
                    "text_jaccard_max": _max_jaccard(current_text_tokens, history_text_tokens),
                    "shared_numbers_count": int(len(shared_numbers)),
                    "new_numbers_count": int(len(new_numbers)),
                    "title_length": _text_len(current, title_column, title_length_column),
                    "text_length": _text_len(current, text_column, text_length_column),
                }
            else:
                cluster_start = current_date
                item_features = {
                    "position_in_cluster": int(position),
                    "cluster_size_so_far": 0,
                    "days_since_previous": -1.0,
                    "days_since_cluster_start": 0.0,
                    "max_prev_similarity": 0.0,
                    "mean_prev_similarity": 0.0,
                    "min_prev_similarity": 0.0,
                    "top2_mean_similarity": 0.0,
                    "top3_mean_similarity": 0.0,
                    "last_prev_similarity": 0.0,
                    "previous_centroid_similarity": 0.0,
                    "previous_centroid_distance": 1.0,
                    "title_jaccard_max": 0.0,
                    "text_jaccard_max": 0.0,
                    "shared_numbers_count": 0,
                    "new_numbers_count": int(len(current_numbers)),
                    "title_length": _text_len(current, title_column, title_length_column),
                    "text_length": _text_len(current, text_column, text_length_column),
                }

            rows.append(
                {
                    "_row_pos": idx,
                    id_column: current[id_column],
                    cluster_column: current[cluster_column],
                    topic_column: current.get(topic_column, ""),
                    date_column: current_date,
                    **item_features,
                }
            )

            history_indices.append(idx)
            history_dates.append(current_date)
            history_title_tokens.append(current_title_tokens)
            history_text_tokens.append(current_text_tokens)
            history_numbers_union.update(current_numbers)

    result = pd.DataFrame(rows).sort_values("_row_pos", kind="mergesort").drop(columns=["_row_pos"])

    for col in LEGACY_SIGNIFICANCE_FEATURE_COLUMNS:
        if col not in result.columns:
            result[col] = 0.0

    result[list(LEGACY_SIGNIFICANCE_FEATURE_COLUMNS)] = (
        result[list(LEGACY_SIGNIFICANCE_FEATURE_COLUMNS)]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    return result


@dataclass
class LegacySignificanceFeatureBuilder:
    """Feature builder compatible with the saved 18-feature CatBoost model."""

    cluster_column: str = "cluster_id"
    id_column: str = "news_id"
    topic_column: str = "topic"
    date_column: str = "published_at"
    title_column: str = "title"
    text_column: str = "text"
    title_length_column: str = "title_length"
    text_length_column: str = "text_length"

    feature_columns = LEGACY_SIGNIFICANCE_FEATURE_COLUMNS

    def build(
        self,
        news_df: pd.DataFrame,
        embeddings: np.ndarray,
        cluster_ids: np.ndarray | list | pd.Series | None = None,
    ) -> pd.DataFrame:
        return build_legacy_significance_features(
            news_df=news_df,
            embeddings=embeddings,
            cluster_ids=cluster_ids,
            cluster_column=self.cluster_column,
            id_column=self.id_column,
            topic_column=self.topic_column,
            date_column=self.date_column,
            title_column=self.title_column,
            text_column=self.text_column,
            title_length_column=self.title_length_column,
            text_length_column=self.text_length_column,
        )


# ---------------------------------------------------------------------------
# Experimental 16-feature builder kept for later experiments
# ---------------------------------------------------------------------------

def build_experimental_previous_only_features(
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    cluster_ids: np.ndarray | list | pd.Series | None = None,
    cluster_column: str = "cluster_id",
    id_column: str = "news_id",
    topic_column: str = "topic",
    date_column: str = "published_at",
    topic_fallback_window_days: int = 30,
) -> pd.DataFrame:
    """Build the newer 16-feature experimental set.

    This is not the default because the saved CatBoost model from the previous
    notebook expects the 18 legacy features.
    """
    if len(news_df) != len(embeddings):
        raise ValueError("news_df and embeddings must have the same length")

    df = _ensure_cluster_column(news_df, cluster_ids, cluster_column=cluster_column)
    df = df.copy()
    df["_row_pos"] = np.arange(len(df))
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    emb = l2_normalize(np.asarray(embeddings, dtype=np.float32))

    rows: list[dict] = []

    sorted_by_time = df.sort_values(date_column, kind="mergesort")
    topic_history: dict[object, list[int]] = {}

    for _, current in sorted_by_time.iterrows():
        idx = int(current["_row_pos"])
        current_date = current[date_column]
        topic = current.get(topic_column, "")
        hist = topic_history.setdefault(topic, [])

        if hist:
            hist_dates = df.iloc[hist][date_column]
            if pd.isna(current_date):
                in_window = np.ones(len(hist), dtype=bool)
            else:
                deltas = (current_date - hist_dates).dt.total_seconds().to_numpy() / (24 * 60 * 60)
                in_window = (deltas >= 0) & (deltas <= topic_fallback_window_days)

            topic_candidates = np.asarray(hist, dtype=int)[in_window]
            topic_sims = emb[topic_candidates] @ emb[idx] if len(topic_candidates) else np.asarray([], dtype=np.float32)
        else:
            topic_candidates = np.asarray([], dtype=int)
            topic_sims = np.asarray([], dtype=np.float32)

        rows.append(
            {
                "_row_pos": idx,
                id_column: current[id_column],
                cluster_column: current[cluster_column],
                topic_column: current.get(topic_column, ""),
                date_column: current_date,
                "topic_prev_count_30d": int(len(topic_candidates)),
                "topic_max_prev_sim_30d": float(topic_sims.max()) if topic_sims.size else 0.0,
                "topic_top3_prev_sim_30d": _mean_top_k(topic_sims, 3),
            }
        )

        hist.append(idx)

    topic_features = pd.DataFrame(rows).set_index(id_column)

    cluster_rows: list[dict] = []
    for _, group in df.sort_values(date_column, kind="mergesort").groupby(cluster_column, sort=False, dropna=False):
        history_indices: list[int] = []
        history_dates: list[pd.Timestamp] = []
        cluster_start = None

        for position, (_, current) in enumerate(group.iterrows()):
            idx = int(current["_row_pos"])
            current_date = current[date_column]
            current_emb = emb[idx]
            prev_count = len(history_indices)

            if prev_count:
                prev_emb = emb[history_indices]
                sims = prev_emb @ current_emb
                centroid = l2_normalize(prev_emb.mean(axis=0, keepdims=True))[0]
                prev_centroid_sim = float(centroid @ current_emb)

                if pd.isna(current_date):
                    days_since_prev = -1.0
                    days_since_start = -1.0
                    density3 = 0
                    density7 = 0
                else:
                    last_date = history_dates[-1]
                    days_since_prev = _days_between(current_date, last_date)
                    days_since_start = _days_between(current_date, cluster_start)
                    deltas = np.array([
                        (current_date - d).total_seconds() / (24 * 60 * 60)
                        for d in history_dates
                        if not pd.isna(d)
                    ])
                    density3 = int(((deltas >= 0) & (deltas <= 3)).sum()) if deltas.size else 0
                    density7 = int(((deltas >= 0) & (deltas <= 7)).sum()) if deltas.size else 0

                item_features = {
                    "prev_count": prev_count,
                    "position_in_cluster": position,
                    "is_first_in_cluster": 0,
                    "max_prev_sim": float(sims.max()),
                    "mean_prev_sim": float(sims.mean()),
                    "top3_prev_sim_mean": _mean_top_k(sims, 3),
                    "top5_prev_sim_mean": _mean_top_k(sims, 5),
                    "prev_centroid_sim": prev_centroid_sim,
                    "prev_centroid_distance": float(1.0 - prev_centroid_sim),
                    "days_since_prev_news": float(days_since_prev),
                    "days_since_cluster_start": float(days_since_start),
                    "cluster_density_last_3_days": density3,
                    "cluster_density_last_7_days": density7,
                }
            else:
                cluster_start = current_date
                item_features = {
                    "prev_count": 0,
                    "position_in_cluster": position,
                    "is_first_in_cluster": 1,
                    "max_prev_sim": 0.0,
                    "mean_prev_sim": 0.0,
                    "top3_prev_sim_mean": 0.0,
                    "top5_prev_sim_mean": 0.0,
                    "prev_centroid_sim": 0.0,
                    "prev_centroid_distance": 1.0,
                    "days_since_prev_news": -1.0,
                    "days_since_cluster_start": 0.0,
                    "cluster_density_last_3_days": 0,
                    "cluster_density_last_7_days": 0,
                }

            cluster_rows.append({id_column: current[id_column], **item_features})
            history_indices.append(idx)
            history_dates.append(current_date)

    cluster_features = pd.DataFrame(cluster_rows).set_index(id_column)
    result = cluster_features.join(topic_features, how="left").reset_index()

    for col in PREVIOUS_ONLY_EXPERIMENTAL_FEATURE_COLUMNS:
        if col not in result.columns:
            result[col] = 0.0

    result[list(PREVIOUS_ONLY_EXPERIMENTAL_FEATURE_COLUMNS)] = (
        result[list(PREVIOUS_ONLY_EXPERIMENTAL_FEATURE_COLUMNS)]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    return result


@dataclass
class ExperimentalPreviousOnlyFeatureBuilder:
    """Feature builder for the newer 16-feature experimental set."""

    cluster_column: str = "cluster_id"
    id_column: str = "news_id"
    topic_column: str = "topic"
    date_column: str = "published_at"
    topic_fallback_window_days: int = 30

    feature_columns = PREVIOUS_ONLY_EXPERIMENTAL_FEATURE_COLUMNS

    def build(
        self,
        news_df: pd.DataFrame,
        embeddings: np.ndarray,
        cluster_ids: np.ndarray | list | pd.Series | None = None,
    ) -> pd.DataFrame:
        return build_experimental_previous_only_features(
            news_df=news_df,
            embeddings=embeddings,
            cluster_ids=cluster_ids,
            cluster_column=self.cluster_column,
            id_column=self.id_column,
            topic_column=self.topic_column,
            date_column=self.date_column,
            topic_fallback_window_days=self.topic_fallback_window_days,
        )


# Backward-compatible public function used by the current notebook/model code.
# It now returns the 18 legacy features by default, so the saved CatBoost model
# can be used without changing the notebook.
def build_previous_only_features(
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    cluster_ids: np.ndarray | list | pd.Series | None = None,
    cluster_column: str = "cluster_id",
    id_column: str = "news_id",
    topic_column: str = "topic",
    date_column: str = "published_at",
    **kwargs,
) -> pd.DataFrame:
    return build_legacy_significance_features(
        news_df=news_df,
        embeddings=embeddings,
        cluster_ids=cluster_ids,
        cluster_column=cluster_column,
        id_column=id_column,
        topic_column=topic_column,
        date_column=date_column,
        title_column=kwargs.get("title_column", "title"),
        text_column=kwargs.get("text_column", "text"),
        title_length_column=kwargs.get("title_length_column", "title_length"),
        text_length_column=kwargs.get("text_length_column", "text_length"),
    )
