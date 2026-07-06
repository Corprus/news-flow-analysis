from __future__ import annotations

import re
from collections.abc import Iterable

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from .embeddings import l2_normalize

# ---------------------------------------------------------------------------
# Наборы признаков
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

# DEFAULT_FEATURE_COLUMNS должен оставаться совместимым с сохранённой CatBoost
# моделью из предыдущего ноутбука. Она обучалась на 18 безымянных колонках,
# соответствующих legacy-порядку признаков выше.
DEFAULT_FEATURE_COLUMNS = LEGACY_SIGNIFICANCE_FEATURE_COLUMNS

FEATURE_COLUMNS = list(LEGACY_SIGNIFICANCE_FEATURE_COLUMNS)


# ---------------------------------------------------------------------------
# Вспомогательные функции
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
    """Использовать готовую длину текста, а при её отсутствии считать len(text)."""
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


def _days_between(
    current_date: pd.Timestamp, previous_date: pd.Timestamp | None, default: float = -1.0
) -> float:
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
# Legacy-сборщик 18 признаков
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
    show_progress: bool = False,
) -> pd.DataFrame:
    """Собрать 18 legacy-признаков для сохранённой CatBoost-модели.

    Функция previous-only: для каждой новости все признаки считаются только по
    более ранним элементам того же предсказанного кластера.

    Строки возвращаются в исходном порядке `news_df`.
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
    cluster_groups = list(sorted_df.groupby(cluster_column, sort=False, dropna=False))

    for _, group in tqdm(
        cluster_groups,
        total=len(cluster_groups),
        desc="Build previous-only features",
        disable=not show_progress,
    ):
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


# Публичная функция для обратной совместимости с текущими ноутбуками и модельным
# кодом. По умолчанию она возвращает 18 legacy-признаков, чтобы сохранённую
# CatBoost-модель можно было использовать без изменений в ноутбуке.
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
