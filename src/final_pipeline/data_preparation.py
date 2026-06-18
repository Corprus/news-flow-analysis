from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable

import pandas as pd

_WORD_RE = re.compile(r"\w+", flags=re.UNICODE)


def _safe_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def _stable_id_from_values(values: Iterable[object]) -> str:
    joined = "|".join(_safe_text(value) for value in values)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]


def prepare_news_dataframe(
    news_df: pd.DataFrame,
    *,
    id_column: str = "news_id",
    url_column: str = "url",
    title_column: str = "title",
    text_column: str = "text",
    topic_column: str = "topic",
    tags_column: str = "tags",
    date_column: str = "published_at",
    model_text_column: str = "model_text",
) -> pd.DataFrame:
    """Приводит новости к clean-like схеме, ожидаемой финальным pipeline."""

    df = news_df.copy()

    for column in [url_column, title_column, text_column, topic_column, tags_column]:
        if column not in df.columns:
            df[column] = ""

    if id_column not in df.columns:
        df[id_column] = [
            _stable_id_from_values(
                (row.get(url_column), row.get(title_column), row.get(date_column))
            )
            for _, row in df.iterrows()
        ]

    if date_column not in df.columns:
        raise ValueError(f"Входной dataframe должен содержать колонку {date_column!r}")

    df[id_column] = df[id_column].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df = df[df[date_column].notna()].copy()

    df[title_column] = df[title_column].fillna("").astype(str)
    df[text_column] = df[text_column].fillna("").astype(str)
    df[topic_column] = df[topic_column].fillna("<missing>").astype(str)
    df[tags_column] = df[tags_column].fillna("").astype(str)
    df[url_column] = df[url_column].fillna("").astype(str)

    if model_text_column not in df.columns:
        df[model_text_column] = (df[title_column] + "\n\n" + df[text_column]).str.strip()
    else:
        df[model_text_column] = df[model_text_column].fillna("").astype(str)
        empty_mask = df[model_text_column].str.strip().eq("")
        df.loc[empty_mask, model_text_column] = (
            df.loc[empty_mask, title_column] + "\n\n" + df.loc[empty_mask, text_column]
        ).str.strip()

    df["title_length"] = df[title_column].str.len().astype(int)
    df["text_length"] = df[text_column].str.len().astype(int)
    df["title_num_words"] = df[title_column].apply(lambda x: len(_WORD_RE.findall(x))).astype(int)
    df["text_num_words"] = df[text_column].apply(lambda x: len(_WORD_RE.findall(x))).astype(int)
    df["model_length"] = df[model_text_column].str.len().astype(int)
    df["model_num_words"] = (
        df[model_text_column].apply(lambda x: len(_WORD_RE.findall(x))).astype(int)
    )

    return df.sort_values([date_column, id_column], kind="mergesort").reset_index(drop=True)


def prepare_candidate_news(
    candidate_pool_df: pd.DataFrame,
    clean_news_df: pd.DataFrame,
    *,
    id_column: str = "news_id",
) -> pd.DataFrame:
    """Восстанавливает clean-like candidate pool по news_id.

    Candidate pool хранит список новостей для end-to-end прогона. Если в clean_news
    есть полные строки с такими news_id, берём их оттуда; иначе нормализуем сам pool.
    """

    pool = candidate_pool_df.copy()
    clean = clean_news_df.copy()
    pool[id_column] = pool[id_column].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    clean[id_column] = clean[id_column].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    clean_cols = [
        "news_id",
        "url",
        "title",
        "text",
        "topic",
        "tags",
        "published_at",
        "text_length",
        "text_num_words",
        "title_length",
        "title_num_words",
        "model_text",
        "model_length",
        "model_num_words",
    ]
    clean_cols = [column for column in clean_cols if column in clean.columns]

    merged = pool[[id_column]].merge(clean[clean_cols], on=id_column, how="left")
    missing_mask = (
        merged["title"].isna() if "title" in merged.columns else pd.Series(True, index=merged.index)
    )

    if missing_mask.any():
        fallback = prepare_news_dataframe(pool)
        fallback = fallback.set_index(id_column)
        merged = merged.set_index(id_column)
        for column in fallback.columns:
            if column not in merged.columns:
                merged[column] = pd.NA
            merged.loc[missing_mask.to_numpy(), column] = fallback.loc[
                merged.index[missing_mask.to_numpy()], column
            ].to_numpy()
        merged = merged.reset_index()

    return prepare_news_dataframe(merged)
