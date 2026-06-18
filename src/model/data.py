from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

CLEAN_REQUIRED_COLUMNS = (
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
)

ANNOTATION_REQUIRED_COLUMNS = (
    "news_id",
    "published_at",
    "topic",
    "title",
    "text",
    "cluster_id",
    "novelty_label",
    "comment",
    "needs_review",
)


def read_csv(path: str | Path, **kwargs) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_csv(path, **kwargs)


def normalize_news_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize ids, dates and text columns without changing the semantic content."""
    result = df.copy()
    if "news_id" in result.columns:
        result["news_id"] = result["news_id"].astype(str)
    if "published_at" in result.columns:
        result["published_at"] = pd.to_datetime(result["published_at"], errors="coerce")
    for col in ("title", "text", "model_text", "topic", "tags"):
        if col in result.columns:
            result[col] = result[col].fillna("").astype(str)
    if "model_text" not in result.columns and {"title", "text"}.issubset(result.columns):
        result["model_text"] = (result["title"].fillna("") + "\n" + result["text"].fillna("")).str.strip()
    return result


def validate_columns(df: pd.DataFrame, required_columns: Sequence[str], name: str = "dataframe") -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def load_clean_news(path: str | Path, validate: bool = True) -> pd.DataFrame:
    df = normalize_news_dataframe(read_csv(path))
    if validate:
        validate_columns(df, CLEAN_REQUIRED_COLUMNS, "clean news dataset")
    return df


def load_annotation(path: str | Path, validate: bool = True) -> pd.DataFrame:
    df = normalize_news_dataframe(read_csv(path))
    if "novelty_label" in df.columns:
        df["novelty_label"] = df["novelty_label"].fillna("").astype(str).str.strip()
    if "needs_review" in df.columns:
        df["needs_review"] = df["needs_review"].fillna(False)
    if validate:
        validate_columns(df, ANNOTATION_REQUIRED_COLUMNS, "annotation dataset")
    return df


def annotation_to_clean_like(
    annotation_df: pd.DataFrame,
    source_clean_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Convert a golden/silver annotation-like table to the clean-like model input.

    If a source clean dataset is provided, url/tags/precomputed length columns are taken
    from it. Otherwise the missing columns are synthesized.
    """
    ann = normalize_news_dataframe(annotation_df)
    if source_clean_df is not None:
        clean = normalize_news_dataframe(source_clean_df)
        keep = [col for col in CLEAN_REQUIRED_COLUMNS if col in clean.columns]
        merged = ann[["news_id"]].merge(clean[keep], on="news_id", how="left")
        for col in ("published_at", "topic", "title", "text"):
            if col in ann.columns:
                merged[col] = merged[col].combine_first(ann[col])
        df = merged
    else:
        df = ann.copy()

    for col in CLEAN_REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df["model_text"] = df["model_text"].where(
        df["model_text"].astype(str).str.len() > 0,
        (df["title"].fillna("") + "\n" + df["text"].fillna("")).str.strip(),
    )
    df["text_length"] = df["text"].astype(str).str.len()
    df["title_length"] = df["title"].astype(str).str.len()
    df["model_length"] = df["model_text"].astype(str).str.len()
    df["text_num_words"] = df["text"].astype(str).str.split().str.len()
    df["title_num_words"] = df["title"].astype(str).str.split().str.len()
    df["model_num_words"] = df["model_text"].astype(str).str.split().str.len()

    return normalize_news_dataframe(df[list(CLEAN_REQUIRED_COLUMNS)])


def remove_train_eval_leakage(
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    id_column: str = "news_id",
) -> pd.DataFrame:
    """Drop from train all rows that are present in the evaluation subset."""
    eval_ids = set(eval_df[id_column].astype(str))
    return train_df[~train_df[id_column].astype(str).isin(eval_ids)].copy()


def save_prediction_csv(df: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    if "published_at" in out.columns:
        out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    out.to_csv(path, index=False)
    return path


def ensure_prediction_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Return prediction dataframe in the simple eval schema."""
    result = df.copy()
    for col in ANNOTATION_REQUIRED_COLUMNS:
        if col not in result.columns:
            if col == "needs_review":
                result[col] = False
            else:
                result[col] = ""
    return result[list(ANNOTATION_REQUIRED_COLUMNS)]

def normalize_news_id(series: pd.Series) -> pd.Series:
    """Normalize news_id values to stable string representation."""
    return (
        series
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def prepare_legacy_baseline_input(
    raw_df: pd.DataFrame,
    id_column: str = "news_id",
    date_column: str = "published_at",
    title_column: str = "title",
    text_column: str = "text",
    model_text_column: str = "model_text",
) -> pd.DataFrame:
    """Prepare candidate-pool input for the legacy baseline pipeline.

    This function fixes the data contract before embeddings and clustering:
    - normalizes news_id;
    - parses published_at;
    - creates model_text if it is missing;
    - sorts rows deterministically by published_at and news_id.

    The row order after this function is the row order expected by embeddings.
    """

    required_columns = {id_column, date_column, title_column, text_column}
    missing_columns = required_columns - set(raw_df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    df = raw_df.copy()

    df[id_column] = normalize_news_id(df[id_column])
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    if df[date_column].isna().any():
        missing_dates = int(df[date_column].isna().sum())
        raise ValueError(f"Found {missing_dates} rows with invalid {date_column}")

    df[title_column] = df[title_column].fillna("").astype(str)
    df[text_column] = df[text_column].fillna("").astype(str)

    if model_text_column not in df.columns:
        df[model_text_column] = (
            df[title_column].fillna("").astype(str)
            + "\n\n"
            + df[text_column].fillna("").astype(str)
        )
    else:
        df[model_text_column] = df[model_text_column].fillna("").astype(str)

    # Numeric sort key keeps news_id ordering stable for Lenta numeric ids.
    df["_news_id_sort_key"] = pd.to_numeric(df[id_column], errors="coerce")

    df = (
        df
        .sort_values([date_column, "_news_id_sort_key", id_column], kind="mergesort")
        .drop(columns=["_news_id_sort_key"])
        .reset_index(drop=True)
    )

    return df