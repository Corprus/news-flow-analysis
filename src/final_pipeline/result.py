from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PipelineVersions:
    pipeline_version: str
    embedding_model: str
    embedding_model_revision: str
    novelty_model_version: str
    config_version: str


@dataclass
class PipelineResult:
    """Unified persistence-oriented result for full and incremental modes."""

    mode: str
    requested_ids: list[str]
    updated_ids: list[str]
    context_ids: list[str]
    predictions: pd.DataFrame
    assignments: pd.DataFrame
    embedding_ids: list[str]
    embeddings: np.ndarray
    diagnostics: dict
    versions: PipelineVersions
