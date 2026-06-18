from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def l2_normalize(matrix: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    matrix = np.asarray(matrix, dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / np.maximum(norms, eps)


class BGEEmbedder:
    """Обёртка над sentence-transformers для финального pipeline."""

    def __init__(
        self,
        model_name: str = "BAAI/bge-m3",
        *,
        device: str | None = None,
        batch_size: int = 16,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = True,
    ) -> None:
        from sentence_transformers import SentenceTransformer

        self.model_name = model_name
        self.batch_size = batch_size
        self.normalize_embeddings = normalize_embeddings
        self.show_progress_bar = show_progress_bar
        self.model = SentenceTransformer(model_name, device=device)

    def encode_texts(self, texts: list[str]) -> np.ndarray:
        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=self.normalize_embeddings,
            show_progress_bar=self.show_progress_bar,
            convert_to_numpy=True,
        )
        embeddings = np.asarray(embeddings, dtype=np.float32)
        return l2_normalize(embeddings) if self.normalize_embeddings else embeddings

    def encode_dataframe(
        self,
        news_df: pd.DataFrame,
        *,
        text_column: str = "model_text",
        id_column: str = "news_id",
        cache_path: str | Path | None = None,
        force_recompute: bool = False,
    ) -> np.ndarray:
        cache = Path(cache_path) if cache_path is not None else None
        if cache is not None and cache.exists() and not force_recompute:
            loaded = np.load(cache, allow_pickle=True)
            embeddings = loaded["embeddings"].astype(np.float32)
            if "news_ids" in loaded.files:
                cached_ids = loaded["news_ids"].astype(str)
                current_ids = news_df[id_column].astype(str).to_numpy()
                if len(cached_ids) != len(current_ids) or not np.all(cached_ids == current_ids):
                    raise ValueError(
                        f"Embeddings cache id mismatch: {cache}. "
                        "Пересчитай cache или используй другой путь."
                    )
            return l2_normalize(embeddings) if self.normalize_embeddings else embeddings

        texts = news_df[text_column].fillna("").astype(str).tolist()
        embeddings = self.encode_texts(texts)
        if cache is not None:
            cache.parent.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(
                cache,
                embeddings=embeddings.astype(np.float32),
                news_ids=news_df[id_column].astype(str).to_numpy(),
            )
        return embeddings
