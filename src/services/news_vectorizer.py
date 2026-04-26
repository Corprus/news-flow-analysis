import asyncio
from pathlib import Path
from typing import Any


class NewsVectorizer:
    def __init__(self, model_name_or_path: str) -> None:
        self._model_name_or_path = model_name_or_path
        self._model: Any | None = None
        self._loaded = False

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @property
    def model_name_or_path(self) -> str:
        return self._model_name_or_path

    async def load(self) -> None:
        if self._looks_like_local_path(self._model_name_or_path):
            model_path = Path(self._model_name_or_path).expanduser()
            if not model_path.exists():
                raise FileNotFoundError(
                    "Fine-tuned sentence-transformers model was not found at "
                    f"{model_path}. Run scripts/train_embeddings.py first "
                    "or set MODEL_NAME_OR_PATH."
                )
            model_name_or_path = str(model_path)
        else:
            model_name_or_path = self._model_name_or_path

        self._model = await asyncio.to_thread(self._load_sentence_transformer, model_name_or_path)
        self._loaded = True

    def _looks_like_local_path(self, value: str) -> bool:
        path = Path(value)
        return path.is_absolute() or value.startswith((".", "~")) or "\\" in value

    def _load_sentence_transformer(self, model_name_or_path: str) -> Any:
        from sentence_transformers import SentenceTransformer

        return SentenceTransformer(model_name_or_path)

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
            "model_name_or_path": self._model_name_or_path,
            "embedding_dimensions": int(embedding.shape[0]),
        }
