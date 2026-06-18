# Вспомогательные признаки для final-step модели novelty detection.
# Код оставлен совместимым с legacy feature contract из ноутбука.

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .config import LEGACY_FEATURE_COLUMNS
from .embeddings import l2_normalize

_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+", flags=re.IGNORECASE)
_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")


def _safe_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def _text_len(row: pd.Series, text_column: str, length_column: str) -> int:
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
    return float(len(left & right) / len(union)) if union else 0.0


def _max_jaccard(current_tokens: set[str], previous_tokens: list[set[str]]) -> float:
    if not previous_tokens:
        return 0.0
    return float(max(_jaccard(current_tokens, tokens) for tokens in previous_tokens))


def _mean_top_k(values: np.ndarray, k: int, default: float = 0.0) -> float:
    if values.size == 0:
        return default
    return float(np.sort(values)[-k:].mean())


def _days_between(
    current_date: pd.Timestamp, previous_date: pd.Timestamp | None, default: float = -1.0
) -> float:
    if previous_date is None or pd.isna(current_date) or pd.isna(previous_date):
        return default
    return float((current_date - previous_date).total_seconds() / (24 * 60 * 60))


@dataclass
class LegacyFeatureBuilder:
    cluster_column: str = "cluster_id"
    id_column: str = "news_id"
    topic_column: str = "topic"
    date_column: str = "published_at"
    title_column: str = "title"
    text_column: str = "text"
    title_length_column: str = "title_length"
    text_length_column: str = "text_length"

    feature_columns: tuple[str, ...] = LEGACY_FEATURE_COLUMNS

    def build(
        self,
        news_df: pd.DataFrame,
        embeddings: np.ndarray,
        cluster_ids: np.ndarray | list | pd.Series | None = None,
    ) -> pd.DataFrame:
        if len(news_df) != len(embeddings):
            raise ValueError("news_df and embeddings must have the same length")

        df = news_df.copy().reset_index(drop=True)
        if cluster_ids is not None:
            if len(cluster_ids) != len(df):
                raise ValueError("cluster_ids and news_df must have the same length")
            df[self.cluster_column] = np.asarray(cluster_ids)
        if self.cluster_column not in df.columns:
            raise ValueError(f"news_df must contain {self.cluster_column!r}")

        df["_row_pos"] = np.arange(len(df))
        df[self.date_column] = pd.to_datetime(df[self.date_column], errors="coerce")
        emb = l2_normalize(np.asarray(embeddings, dtype=np.float32))

        rows: list[dict] = []
        sorted_df = df.sort_values(
            [self.cluster_column, self.date_column, "_row_pos"], kind="mergesort"
        )

        for _, group in sorted_df.groupby(self.cluster_column, sort=False, dropna=False):
            history_indices: list[int] = []
            history_dates: list[pd.Timestamp] = []
            history_title_tokens: list[set[str]] = []
            history_text_tokens: list[set[str]] = []
            history_numbers_union: set[str] = set()
            cluster_start: pd.Timestamp | None = None

            for position, (_, current) in enumerate(group.iterrows()):
                idx = int(current["_row_pos"])
                current_date = current[self.date_column]
                current_emb = emb[idx]

                current_title = _safe_text(current.get(self.title_column, ""))
                current_text = _safe_text(current.get(self.text_column, ""))
                current_title_tokens = _token_set(current_title)
                current_text_tokens = _token_set(current_text)
                current_numbers = _number_set((current_title, current_text))

                prev_count = len(history_indices)

                if prev_count:
                    prev_emb = emb[history_indices]
                    sims = prev_emb @ current_emb
                    centroid = l2_normalize(prev_emb.mean(axis=0, keepdims=True))[0]
                    centroid_sim = float(centroid @ current_emb)

                    shared_numbers = current_numbers & history_numbers_union
                    new_numbers = current_numbers - history_numbers_union

                    item_features = {
                        "position_in_cluster": int(position),
                        "cluster_size_so_far": int(prev_count),
                        "days_since_previous": _days_between(current_date, history_dates[-1]),
                        "days_since_cluster_start": _days_between(current_date, cluster_start),
                        "max_prev_similarity": float(sims.max()),
                        "mean_prev_similarity": float(sims.mean()),
                        "min_prev_similarity": float(sims.min()),
                        "top2_mean_similarity": _mean_top_k(sims, 2),
                        "top3_mean_similarity": _mean_top_k(sims, 3),
                        "last_prev_similarity": float(sims[-1]),
                        "previous_centroid_similarity": centroid_sim,
                        "previous_centroid_distance": float(1.0 - centroid_sim),
                        "title_jaccard_max": _max_jaccard(
                            current_title_tokens, history_title_tokens
                        ),
                        "text_jaccard_max": _max_jaccard(current_text_tokens, history_text_tokens),
                        "shared_numbers_count": int(len(shared_numbers)),
                        "new_numbers_count": int(len(new_numbers)),
                        "title_length": _text_len(
                            current, self.title_column, self.title_length_column
                        ),
                        "text_length": _text_len(
                            current, self.text_column, self.text_length_column
                        ),
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
                        "title_length": _text_len(
                            current, self.title_column, self.title_length_column
                        ),
                        "text_length": _text_len(
                            current, self.text_column, self.text_length_column
                        ),
                    }

                rows.append(
                    {
                        "_row_pos": idx,
                        self.id_column: current[self.id_column],
                        self.cluster_column: current[self.cluster_column],
                        self.topic_column: current.get(self.topic_column, ""),
                        self.date_column: current_date,
                        **item_features,
                    }
                )

                history_indices.append(idx)
                history_dates.append(current_date)
                history_title_tokens.append(current_title_tokens)
                history_text_tokens.append(current_text_tokens)
                history_numbers_union.update(current_numbers)

        result = (
            pd.DataFrame(rows)
            .sort_values("_row_pos", kind="mergesort")
            .drop(columns=["_row_pos"])
            .reset_index(drop=True)
        )

        for column in self.feature_columns:
            if column not in result.columns:
                result[column] = 0.0

        result[list(self.feature_columns)] = (
            result[list(self.feature_columns)].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        )

        return result
