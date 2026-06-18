from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .config import SignificanceModelConfig
from .features import DEFAULT_FEATURE_COLUMNS


@dataclass
class MLPConfig:
    hidden_dim: int = 128
    dropout: float = 0.10
    epochs: int = 40
    batch_size: int = 128
    learning_rate: float = 1e-3
    weight_decay: float = 1e-4
    random_state: int = 42


class TorchTabularSignificanceMLP:
    """Small neural alternative to CatBoost for the final novelty step.

    It deliberately uses the same previous-only features to keep the comparison fair.
    """

    def __init__(
        self,
        model_config: SignificanceModelConfig | None = None,
        mlp_config: MLPConfig | None = None,
    ) -> None:
        self.model_config = model_config or SignificanceModelConfig()
        self.mlp_config = mlp_config or MLPConfig()
        self.feature_columns = list(self.model_config.feature_columns or DEFAULT_FEATURE_COLUMNS)
        self.model = None
        self.mean_ = None
        self.std_ = None

    def _prepare_training_data(self, features_df: pd.DataFrame, labels_df: pd.DataFrame):
        frame = features_df.merge(
            labels_df[["news_id", "novelty_label"]], on="news_id", how="inner"
        )
        allowed = set(self.model_config.positive_labels) | set(self.model_config.negative_labels)
        frame["novelty_label"] = frame["novelty_label"].fillna("").astype(str).str.strip()
        frame = frame[frame["novelty_label"].isin(allowed)].copy()
        y = (
            frame["novelty_label"]
            .isin(self.model_config.positive_labels)
            .astype(np.float32)
            .to_numpy()
        )
        x = frame[self.feature_columns].astype(np.float32).to_numpy()
        return x, y

    def fit(self, features_df: pd.DataFrame, labels_df: pd.DataFrame):
        try:
            import torch
            import torch.nn as nn
            from torch.utils.data import DataLoader, TensorDataset
        except ImportError as exc:
            raise ImportError("torch is required for TorchTabularSignificanceMLP.") from exc

        torch.manual_seed(self.mlp_config.random_state)
        x, y = self._prepare_training_data(features_df, labels_df)
        self.mean_ = x.mean(axis=0, keepdims=True)
        self.std_ = x.std(axis=0, keepdims=True) + 1e-6
        x = (x - self.mean_) / self.std_

        dataset = TensorDataset(
            torch.tensor(x, dtype=torch.float32), torch.tensor(y[:, None], dtype=torch.float32)
        )
        loader = DataLoader(dataset, batch_size=self.mlp_config.batch_size, shuffle=True)

        self.model = nn.Sequential(
            nn.Linear(x.shape[1], self.mlp_config.hidden_dim),
            nn.ReLU(),
            nn.Dropout(self.mlp_config.dropout),
            nn.Linear(self.mlp_config.hidden_dim, self.mlp_config.hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(self.mlp_config.dropout),
            nn.Linear(self.mlp_config.hidden_dim // 2, 1),
        )
        optimizer = torch.optim.AdamW(
            self.model.parameters(),
            lr=self.mlp_config.learning_rate,
            weight_decay=self.mlp_config.weight_decay,
        )
        criterion = nn.BCEWithLogitsLoss()
        history = []
        for epoch in range(self.mlp_config.epochs):
            losses = []
            self.model.train()
            for xb, yb in loader:
                optimizer.zero_grad()
                logits = self.model(xb)
                loss = criterion(logits, yb)
                loss.backward()
                optimizer.step()
                losses.append(float(loss.detach().cpu()))
            history.append({"epoch": epoch + 1, "loss": float(np.mean(losses)) if losses else 0.0})
        return pd.DataFrame(history)

    def predict_proba(self, features_df: pd.DataFrame) -> np.ndarray:
        if self.model is None or self.mean_ is None or self.std_ is None:
            raise ValueError("Model is not fitted.")
        try:
            import torch
        except ImportError as exc:
            raise ImportError("torch is required for TorchTabularSignificanceMLP.") from exc
        x = features_df[self.feature_columns].astype(np.float32).to_numpy()
        x = (x - self.mean_) / self.std_
        self.model.eval()
        with torch.no_grad():
            logits = self.model(torch.tensor(x, dtype=torch.float32)).squeeze(1)
            return torch.sigmoid(logits).cpu().numpy()

    def predict_labels(self, features_df: pd.DataFrame) -> pd.DataFrame:
        proba = self.predict_proba(features_df)
        out = features_df.copy()
        out["p_significant"] = proba
        out["novelty_label"] = np.where(
            proba >= self.model_config.threshold, "significant", "minor"
        )
        if "max_prev_sim" in out.columns:
            duplicate_mask = (proba < self.model_config.threshold) & (
                out["max_prev_sim"] >= self.model_config.duplicate_threshold
            )
            out.loc[duplicate_mask, "novelty_label"] = "duplicate"
        out["needs_review"] = (
            np.abs(proba - self.model_config.threshold) <= self.model_config.review_margin
        )
        return out
