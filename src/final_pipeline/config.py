from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
import json
from typing import Any

from model.attach_clustering import AttachClusteringConfig, BaselineClusteringConfig


FINAL_MODEL_RELATIVE_PATH = Path(
    "data/artifacts/models/final_exp10/exp_10a_current_model_on_exp10_clustering.joblib"
)
FINAL_PIPELINE_CONFIG_RELATIVE_PATH = Path(
    "data/artifacts/models/final_exp10/final_pipeline_config.json"
)


@dataclass(frozen=True)
class FinalPipelineConfig:
    """Конфигурация финального inference pipeline.

    По умолчанию используется связка:
    BAAI/bge-m3 → baseline exp_00b → exp_10 attach → novelty model artifact.
    """

    embedding_model_name: str = "BAAI/bge-m3"
    embedding_batch_size: int = 16
    normalize_embeddings: bool = True
    show_progress_bar: bool = True
    text_column: str = "model_text"
    id_column: str = "news_id"

    base_clustering: BaselineClusteringConfig = field(default_factory=BaselineClusteringConfig)
    attach_clustering: AttachClusteringConfig = field(default_factory=AttachClusteringConfig)

    @classmethod
    def from_json(cls, path: str | Path) -> "FinalPipelineConfig":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        kwargs: dict[str, Any] = dict(raw)
        if "base_clustering" in kwargs and isinstance(kwargs["base_clustering"], dict):
            kwargs["base_clustering"] = BaselineClusteringConfig(**kwargs["base_clustering"])
        if "attach_clustering" in kwargs and isinstance(kwargs["attach_clustering"], dict):
            kwargs["attach_clustering"] = AttachClusteringConfig(**kwargs["attach_clustering"])
        allowed = set(cls.__dataclass_fields__)
        kwargs = {key: value for key, value in kwargs.items() if key in allowed}
        return cls(**kwargs)

    def to_json(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2), encoding="utf-8")
