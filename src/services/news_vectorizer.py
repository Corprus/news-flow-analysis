import asyncio
from pathlib import Path
from typing import Any

from model_registry import resolve_model_source


class NewsVectorizer:
    def __init__(self, model_source: str) -> None:
        self._model_source = model_source
        self._resolved_model_source = resolve_model_source(model_source)
        self._model: Any | None = None
        self._loaded = False

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @property
    def model_source(self) -> str:
        return self._model_source

    @property
    def resolved_model_source(self) -> str:
        return self._resolved_model_source

    async def load(self) -> None:
        if self._looks_like_local_path(self._resolved_model_source):
            model_path = Path(self._resolved_model_source).expanduser()
            if not model_path.exists():
                raise FileNotFoundError(
                    "Fine-tuned sentence-transformers model was not found at "
                    f"{model_path}. Run scripts/train_embeddings.py first "
                    "or set LOCAL_MODEL_SOURCE/REMOTE_MODEL_SOURCE."
                )
            resolved_model_source = str(model_path)
        else:
            resolved_model_source = self._resolved_model_source

        self._model = await asyncio.to_thread(
            self._load_sentence_transformer,
            resolved_model_source,
        )
        self._loaded = True

    def _looks_like_local_path(self, value: str) -> bool:
        path = Path(value)
        return path.is_absolute() or value.startswith((".", "~")) or "\\" in value

    def _load_sentence_transformer(self, model_source: str) -> Any:
        from sentence_transformers import SentenceTransformer

        if "@" in model_source and not self._looks_like_local_path(model_source):
            repo_id, revision = model_source.rsplit("@", 1)
            return SentenceTransformer(repo_id, revision=revision)
        return SentenceTransformer(model_source)

    async def vectorize_text(self, text: str) -> dict[str, Any]:
        if not self._loaded or self._model is None:
            raise RuntimeError("News vectorizer model is not loaded")

        embedding = await asyncio.to_thread(
            self._model.encode,
            text,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

        return {
            "model_source": self._model_source,
            "resolved_model_source": self._resolved_model_source,
            "embedding_dimensions": int(embedding.shape[0]),
            "embedding": embedding.astype(float).tolist(),
        }
