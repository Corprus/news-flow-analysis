from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from .embeddings import l2_normalize


@dataclass
class PairDatasetConfig:
    max_positive_pairs_per_cluster: int = 20
    max_negative_pairs_per_topic: int = 50
    hard_negative_min_similarity: float = 0.70
    random_state: int = 42


def build_contrastive_pairs(
    labels_df: pd.DataFrame,
    embeddings: np.ndarray,
    id_order: Iterable[str],
    id_column: str = "news_id",
    topic_column: str = "topic",
    cluster_column: str = "cluster_id",
    config: PairDatasetConfig | None = None,
) -> pd.DataFrame:
    """Build positive and hard-negative pairs for projection/adaptor training.

    Positive pairs:
        same annotated cluster.

    Hard negative pairs:
        same topic, different cluster, high semantic similarity.

    This implementation avoids slow per-pair DataFrame lookups and computes
    topic-level similarities with vectorized numpy operations.
    """
    cfg = config or PairDatasetConfig()
    rng = np.random.default_rng(cfg.random_state)

    id_order = pd.Series(list(id_order)).astype(str).tolist()
    id_to_pos = {news_id: i for i, news_id in enumerate(id_order)}

    if len(id_order) != len(embeddings):
        raise ValueError(
            f"id_order and embeddings must have the same length: "
            f"{len(id_order)} != {len(embeddings)}"
        )

    df = labels_df.copy()
    df[id_column] = df[id_column].astype(str)

    # Оставляем только те новости, для которых есть embedding.
    df = df[df[id_column].isin(id_to_pos)].copy()

    # Защита от дублей news_id в разметке.
    df = df.drop_duplicates(subset=[id_column], keep="first").copy()

    emb = l2_normalize(np.asarray(embeddings, dtype=np.float32))

    pairs: list[dict] = []

    # Быстрые lookup-таблицы вместо df.loc внутри циклов.
    cluster_by_id = df.set_index(id_column)[cluster_column].astype(str).to_dict()
    topic_by_id = df.set_index(id_column)[topic_column].astype(str).to_dict()

    # ------------------------------------------------------------------
    # Positive pairs: внутри одного cluster_id.
    # ------------------------------------------------------------------
    cluster_groups = list(df.groupby(cluster_column, dropna=False, sort=False))

    for _cluster_id, group in tqdm(
        cluster_groups,
        desc="Building positive pairs by cluster",
        total=len(cluster_groups),
    ):
        ids = group[id_column].astype(str).unique().tolist()

        if len(ids) < 2:
            continue

        all_pairs = [(a, b) for i, a in enumerate(ids) for b in ids[i + 1 :]]

        if len(all_pairs) > cfg.max_positive_pairs_per_cluster:
            selected_idx = rng.choice(
                len(all_pairs),
                size=cfg.max_positive_pairs_per_cluster,
                replace=False,
            )
            all_pairs = [all_pairs[i] for i in selected_idx]

        for a, b in all_pairs:
            sim = float(emb[id_to_pos[a]] @ emb[id_to_pos[b]])

            pairs.append(
                {
                    "news_id_a": a,
                    "news_id_b": b,
                    "target": 1,
                    "base_similarity": sim,
                    "topic": topic_by_id.get(a),
                    "cluster_id_a": cluster_by_id.get(a),
                    "cluster_id_b": cluster_by_id.get(b),
                }
            )

    # ------------------------------------------------------------------
    # Hard negative pairs: внутри одного topic, но из разных cluster_id.
    # ------------------------------------------------------------------
    topic_groups = list(df.groupby(topic_column, dropna=False, sort=False))

    for topic, group in tqdm(
        topic_groups,
        desc="Building hard negative pairs by topic",
        total=len(topic_groups),
    ):
        ids = group[id_column].astype(str).unique().tolist()

        if len(ids) < 2:
            continue

        positions = np.array([id_to_pos[news_id] for news_id in ids], dtype=int)
        clusters = np.array([cluster_by_id[news_id] for news_id in ids], dtype=str)

        topic_emb = emb[positions]

        # embeddings нормализованы, поэтому dot product = cosine similarity.
        sim_matrix = topic_emb @ topic_emb.T

        # Только верхний треугольник, чтобы не дублировать пары и не брать self-pairs.
        upper_mask = np.triu(
            np.ones(sim_matrix.shape, dtype=bool),
            k=1,
        )

        different_cluster_mask = clusters[:, None] != clusters[None, :]

        hard_negative_mask = (
            upper_mask & different_cluster_mask & (sim_matrix >= cfg.hard_negative_min_similarity)
        )

        row_idx, col_idx = np.where(hard_negative_mask)

        if len(row_idx) == 0:
            continue

        sims = sim_matrix[row_idx, col_idx]

        # Берём top-N самых похожих hard negatives внутри topic.
        order = np.argsort(-sims)
        order = order[: cfg.max_negative_pairs_per_topic]

        for pair_idx in order:
            i = int(row_idx[pair_idx])
            j = int(col_idx[pair_idx])
            sim = float(sims[pair_idx])

            a = ids[i]
            b = ids[j]

            pairs.append(
                {
                    "news_id_a": a,
                    "news_id_b": b,
                    "target": -1,
                    "base_similarity": sim,
                    "topic": str(topic),
                    "cluster_id_a": cluster_by_id.get(a),
                    "cluster_id_b": cluster_by_id.get(b),
                }
            )

    result = pd.DataFrame(pairs)

    if result.empty:
        return pd.DataFrame(
            columns=[
                "news_id_a",
                "news_id_b",
                "target",
                "base_similarity",
                "topic",
                "cluster_id_a",
                "cluster_id_b",
            ]
        )

    # Стабильный порядок колонок.
    return result[
        [
            "news_id_a",
            "news_id_b",
            "target",
            "base_similarity",
            "topic",
            "cluster_id_a",
            "cluster_id_b",
        ]
    ]


def train_projection_adapter(
    embeddings: np.ndarray,
    id_order: Iterable[str],
    pairs_df: pd.DataFrame,
    output_dim: int | None = None,
    hidden_dim: int = 512,
    epochs: int = 10,
    batch_size: int = 256,
    learning_rate: float = 1e-3,
    margin: float = 0.2,
    random_state: int = 42,
):
    """Train a small projection over frozen embeddings with CosineEmbeddingLoss."""
    try:
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset
    except ImportError as exc:
        raise ImportError("torch is required for projection adapter training.") from exc

    torch.manual_seed(random_state)
    emb = l2_normalize(embeddings)
    id_to_pos = {str(news_id): i for i, news_id in enumerate(id_order)}
    output_dim = output_dim or emb.shape[1]

    idx_a = pairs_df["news_id_a"].astype(str).map(id_to_pos).to_numpy()
    idx_b = pairs_df["news_id_b"].astype(str).map(id_to_pos).to_numpy()
    target = pairs_df["target"].astype(np.float32).to_numpy()
    keep = ~pd.isna(idx_a) & ~pd.isna(idx_b)
    idx_a = idx_a[keep].astype(int)
    idx_b = idx_b[keep].astype(int)
    target = target[keep]

    x1 = torch.tensor(emb[idx_a], dtype=torch.float32)
    x2 = torch.tensor(emb[idx_b], dtype=torch.float32)
    y = torch.tensor(target, dtype=torch.float32)
    loader = DataLoader(TensorDataset(x1, x2, y), batch_size=batch_size, shuffle=True)

    class ProjectionAdapter(nn.Module):
        def __init__(self, input_dim: int, hidden_dim: int, output_dim: int):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, output_dim),
            )

        def forward(self, x):
            z = self.net(x)
            return torch.nn.functional.normalize(z, dim=1)

    model = ProjectionAdapter(emb.shape[1], hidden_dim, output_dim)
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    criterion = nn.CosineEmbeddingLoss(margin=margin)
    history = []
    for epoch in range(epochs):
        model.train()
        losses = []
        for batch_a, batch_b, batch_y in loader:
            optimizer.zero_grad()
            za = model(batch_a)
            zb = model(batch_b)
            loss = criterion(za, zb, batch_y)
            loss.backward()
            optimizer.step()
            losses.append(float(loss.detach().cpu()))
        history.append({"epoch": epoch + 1, "loss": float(np.mean(losses)) if losses else 0.0})
    return model, pd.DataFrame(history)


def apply_projection_adapter(model, embeddings: np.ndarray, batch_size: int = 1024) -> np.ndarray:
    try:
        import torch
    except ImportError as exc:
        raise ImportError("torch is required for projection adapter inference.") from exc
    model.eval()
    emb = l2_normalize(embeddings)
    outputs = []
    with torch.no_grad():
        for start in range(0, len(emb), batch_size):
            batch = torch.tensor(emb[start : start + batch_size], dtype=torch.float32)
            outputs.append(model(batch).cpu().numpy())
    return l2_normalize(np.vstack(outputs))
