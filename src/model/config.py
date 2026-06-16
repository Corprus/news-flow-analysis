from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence


@dataclass
class ExperimentPaths:
    """Centralized paths used by the experiment notebook.

    Adjust `project_root` in the notebook; dependent paths are filled from it
    in `__post_init__` unless they are explicitly provided.
    """

    project_root: Path = Path("..").resolve()
    data_dir: Path | None = None
    prepared_dir: Path | None = None
    artifacts_dir: Path | None = None
    predictions_dir: Path | None = None

    clean_news_path: Path | None = None
    golden_path: Path | None = None
    silver_path: Path | None = None

    current_catboost_model_path: Path | None = None
    current_catboost_config_path: Path | None = None
    significance_model_dir: Path | None = None

    first_item_fallback_enabled: bool = True
    fallback_window_days: int = 30
    fallback_similarity_threshold: float = 0.78
    fallback_max_previous_candidates: int = 10


    def __post_init__(self) -> None:
        self.project_root = Path(self.project_root).resolve()
        self.data_dir = Path(self.data_dir) if self.data_dir is not None else self.project_root / "data"
        self.prepared_dir = Path(self.prepared_dir) if self.prepared_dir is not None else self.data_dir / "prepared"
        self.artifacts_dir = Path(self.artifacts_dir) if self.artifacts_dir is not None else self.data_dir / "artifacts"
        self.models_dir = self.artifacts_dir / "models"
        self.significance_model_dir = (
            Path(self.significance_model_dir)
            if self.significance_model_dir is not None
            else self.models_dir / "significance_model"
        )
        self.predictions_dir = Path(self.predictions_dir) if self.predictions_dir is not None else self.data_dir / "predictions"

        self.clean_news_path = Path(self.clean_news_path) if self.clean_news_path is not None else self.prepared_dir / "lenta_clean_news.csv"
        self.golden_path = Path(self.golden_path) if self.golden_path is not None else self.prepared_dir / "lenta_golden_set_human.csv"
        self.silver_path = Path(self.silver_path) if self.silver_path is not None else self.prepared_dir / "lenta_silver_set_llm.csv"

        self.current_catboost_model_path = (
            Path(self.current_catboost_model_path)
            if self.current_catboost_model_path is not None
            else self.significance_model_dir / "catboost_is_significant.joblib"
        )
        self.current_catboost_config_path = (
            Path(self.current_catboost_config_path)
            if self.current_catboost_config_path is not None
            else self.significance_model_dir / "semantic_significance_model_config.json"
        )


@dataclass(frozen=True)
class SemanticNewsBaselineConfig:
    """Parameters of the previously fixed baseline/model pipeline."""

    embedding_model_name: str = "BAAI/bge-m3"
    story_threshold: float = 0.82
    story_window_days: int = 14
    minor_threshold: float = 0.88
    duplicate_threshold: float = 0.90
    review_margin: float = 0.03
    text_column: str = "model_text"
    topic_column: str = "topic"
    date_column: str = "published_at"
    id_column: str = "news_id"


@dataclass(frozen=True)
class SignificanceModelConfig:
    """CatBoost/MLP final-step model parameters.

    The model is binary: significant vs not significant. The final text label is restored
    by post-processing: non-significant high-similarity items can be labelled duplicate,
    the rest are labelled minor.
    """

    threshold: float = 0.42
    duplicate_threshold: float = 0.90
    review_margin: float = 0.10
    positive_labels: Sequence[str] = field(default_factory=lambda: ("significant",))
    negative_labels: Sequence[str] = field(default_factory=lambda: ("minor", "duplicate"))
    ignored_labels: Sequence[str] = field(default_factory=lambda: ("", "wrong_cluster", "unclear"))
    random_state: int = 42

    # Keep the order stable: saved models and experiment tables depend on it.
    feature_columns: Sequence[str] = field(
        default_factory=lambda: (
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
    )
