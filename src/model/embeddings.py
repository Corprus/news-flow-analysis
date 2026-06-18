from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd

from .data import normalize_news_id


def l2_normalize(matrix: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    matrix = np.asarray(matrix, dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / np.maximum(norms, eps)


class SentenceTransformerEncoder:
    """Thin wrapper around sentence-transformers with deterministic caching."""

    def __init__(
        self,
        model_name: str = "BAAI/bge-m3",
        device: str | None = None,
        batch_size: int = 16,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = True,
    ) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise ImportError(
                "sentence-transformers is required for encoding. Install it in the course environment."
            ) from exc

        self.model_name = model_name
        self.batch_size = batch_size
        self.normalize_embeddings = normalize_embeddings
        self.show_progress_bar = show_progress_bar
        self.model = SentenceTransformer(model_name, device=device)

    def encode_texts(self, texts: Iterable[str]) -> np.ndarray:
        embeddings = self.model.encode(
            list(texts),
            batch_size=self.batch_size,
            normalize_embeddings=self.normalize_embeddings,
            show_progress_bar=self.show_progress_bar,
        )
        return np.asarray(embeddings, dtype=np.float32)

    def encode_dataframe(
        self,
        df: pd.DataFrame,
        text_column: str = "model_text",
        id_column: str = "news_id",
        cache_path: str | Path | None = None,
        force_recompute: bool = False,
    ) -> np.ndarray:
        ids = df[id_column].astype(str).to_numpy()
        cache = Path(cache_path) if cache_path else None
        if cache and cache.exists() and not force_recompute:
            loaded = np.load(cache, allow_pickle=True)
            cached_ids = loaded["ids"].astype(str)
            if np.array_equal(cached_ids, ids):
                return loaded["embeddings"].astype(np.float32)

        embeddings = self.encode_texts(df[text_column].fillna("").astype(str).tolist())
        if cache:
            cache.parent.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(cache, ids=ids, embeddings=embeddings)
        return embeddings


def load_cached_embeddings(path: str | Path, expected_ids: Iterable[str] | None = None) -> np.ndarray:
    loaded = np.load(Path(path), allow_pickle=True)
    embeddings = loaded["embeddings"].astype(np.float32)
    if expected_ids is not None:
        expected = np.asarray(list(map(str, expected_ids)))
        actual = loaded["ids"].astype(str)
        if not np.array_equal(actual, expected):
            raise ValueError("Cached embedding ids do not match dataframe ids.")
    return embeddings


def save_cached_embeddings(path: str | Path, ids: Iterable[str], embeddings: np.ndarray) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, ids=np.asarray(list(map(str, ids))), embeddings=np.asarray(embeddings, dtype=np.float32))
    return path


def load_id_aligned_embeddings(
    cache_path: str | Path,
    target_ids: pd.Series | np.ndarray | list,
) -> np.ndarray:
    """Load embeddings from an id-aware cache and align them to target_ids order."""

    cache_path = Path(cache_path)

    with np.load(cache_path, allow_pickle=True) as data:
        keys = set(data.files)

        if "ids" not in keys or "embeddings" not in keys:
            raise ValueError(
                f"Embedding cache {cache_path} is not id-aware. "
                "Expected keys: 'ids' and 'embeddings'. "
                "Old matrix-only caches are unsafe for reusable pipelines."
            )

        cache_ids = data["ids"].astype(str)
        embeddings = data["embeddings"].astype(np.float32)

    target_ids = normalize_news_id(pd.Series(target_ids)).to_numpy()

    id_to_pos = {
        news_id: pos
        for pos, news_id in enumerate(cache_ids)
    }

    missing_ids = [
        news_id
        for news_id in target_ids
        if news_id not in id_to_pos
    ]

    if missing_ids:
        raise ValueError(
            f"Missing embeddings for {len(missing_ids)} ids. "
            f"Examples: {missing_ids[:10]}"
        )

    positions = np.array(
        [id_to_pos[news_id] for news_id in target_ids],
        dtype=int,
    )

    return embeddings[positions]


def save_id_aligned_embeddings(
    cache_path: str | Path,
    ids: pd.Series | np.ndarray | list,
    embeddings: np.ndarray,
) -> None:
    """Save embeddings together with news_id values."""

    cache_path = Path(cache_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    ids = normalize_news_id(pd.Series(ids)).to_numpy()
    embeddings = np.asarray(embeddings, dtype=np.float32)

    if len(ids) != len(embeddings):
        raise ValueError(
            f"ids and embeddings length mismatch: {len(ids)} != {len(embeddings)}"
        )

    np.savez_compressed(
        cache_path,
        ids=ids,
        embeddings=embeddings,
    )


def get_or_create_id_aligned_embeddings(
    encoder,
    df: pd.DataFrame,
    cache_path: str | Path,
    id_column: str = "news_id",
    text_column: str = "model_text",
    force_recompute: bool = False,
) -> np.ndarray:
    """Return embeddings aligned to df order, using an id-aware cache.

    This helper deliberately refuses old matrix-only caches because they are
    order-dependent and caused the baseline reproduction bug.
    """

    cache_path = Path(cache_path)
    ids = normalize_news_id(df[id_column])

    if cache_path.exists() and not force_recompute:
        return load_id_aligned_embeddings(cache_path, ids)

    embeddings = encoder.encode_dataframe(
        df,
        text_column=text_column,
        id_column=id_column,
        cache_path=None,
        force_recompute=True,
    )

    save_id_aligned_embeddings(
        cache_path=cache_path,
        ids=ids,
        embeddings=embeddings,
    )

    return embeddings