from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .embeddings import l2_normalize


class _DSU:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


def _safe_slug(value: object) -> str:
    text = str(value) if value is not None else "unknown"
    allowed = []
    for ch in text.lower():
        if ch.isalnum():
            allowed.append(ch)
        elif ch in {" ", "-", "_", "/"}:
            allowed.append("_")
    slug = "".join(allowed).strip("_")
    return slug or "unknown"


def _date_distance_days(dates: pd.Series) -> np.ndarray:
    values = pd.to_datetime(dates, errors="coerce").to_numpy(dtype="datetime64[ns]")
    # nanoseconds -> days
    diff_ns = np.abs(values[:, None] - values[None, :]).astype("timedelta64[ns]").astype(np.float64)
    return diff_ns / (24 * 60 * 60 * 1e9)


def _component_ids(local_indices: list[int], dsu: _DSU, prefix: str) -> dict[int, str]:
    root_to_cluster: dict[int, str] = {}
    result: dict[int, str] = {}
    for local_pos, original_idx in enumerate(local_indices):
        root = dsu.find(local_pos)
        if root not in root_to_cluster:
            root_to_cluster[root] = f"{prefix}_{len(root_to_cluster):04d}"
        result[original_idx] = root_to_cluster[root]
    return result


@dataclass
class ThresholdGraphClusterer:
    """Previous baseline clusterer: connected components in a similarity graph."""

    similarity_threshold: float = 0.82
    window_days: int = 14
    topic_column: str = "topic"
    date_column: str = "published_at"

    def fit_predict(self, news_df: pd.DataFrame, embeddings: np.ndarray) -> pd.Series:
        if len(news_df) != len(embeddings):
            raise ValueError("news_df and embeddings must have the same length")
        original_index = news_df.index
        work = news_df.reset_index(drop=True)
        emb = l2_normalize(embeddings)
        cluster_by_idx: dict[int, str] = {}

        for topic, group in work.groupby(self.topic_column, sort=False, dropna=False):
            local_indices = group.index.to_list()
            local_emb = emb[local_indices]
            sim = local_emb @ local_emb.T
            days = _date_distance_days(group[self.date_column])
            n = len(local_indices)
            dsu = _DSU(n)
            for i in range(n):
                for j in range(i + 1, n):
                    if days[i, j] <= self.window_days and sim[i, j] >= self.similarity_threshold:
                        dsu.union(i, j)
            cluster_by_idx.update(_component_ids(local_indices, dsu, _safe_slug(topic)))

        return pd.Series(
            [cluster_by_idx[i] for i in range(len(work))], index=original_index, name="cluster_id"
        )


@dataclass
class MutualKnnTemporalClusterer:
    """Experimental clusterer that reduces chaining via mutual top-k links."""

    similarity_threshold: float = 0.80
    window_days: int = 14
    k: int = 5
    topic_column: str = "topic"
    date_column: str = "published_at"

    def fit_predict(self, news_df: pd.DataFrame, embeddings: np.ndarray) -> pd.Series:
        if len(news_df) != len(embeddings):
            raise ValueError("news_df and embeddings must have the same length")
        original_index = news_df.index
        work = news_df.reset_index(drop=True)
        emb = l2_normalize(embeddings)
        cluster_by_idx: dict[int, str] = {}

        for topic, group in work.groupby(self.topic_column, sort=False, dropna=False):
            local_indices = group.index.to_list()
            local_emb = emb[local_indices]
            sim = local_emb @ local_emb.T
            days = _date_distance_days(group[self.date_column])
            n = len(local_indices)
            dsu = _DSU(n)
            directed: set[tuple[int, int]] = set()
            for i in range(n):
                candidates = [
                    j
                    for j in range(n)
                    if j != i
                    and days[i, j] <= self.window_days
                    and sim[i, j] >= self.similarity_threshold
                ]
                candidates = sorted(candidates, key=lambda j: sim[i, j], reverse=True)[: self.k]
                directed.update((i, j) for j in candidates)
            for i, j in list(directed):
                if (j, i) in directed:
                    dsu.union(i, j)
            prefix = f"{_safe_slug(topic)}_mknn"
            cluster_by_idx.update(_component_ids(local_indices, dsu, prefix))

        return pd.Series(
            [cluster_by_idx[i] for i in range(len(work))], index=original_index, name="cluster_id"
        )


@dataclass
class TemporalDecayGraphClusterer:
    """Similarity graph with an explicit penalty for temporal distance."""

    similarity_threshold: float = 0.82
    window_days: int = 30
    decay_per_day: float = 0.002
    topic_column: str = "topic"
    date_column: str = "published_at"

    def fit_predict(self, news_df: pd.DataFrame, embeddings: np.ndarray) -> pd.Series:
        if len(news_df) != len(embeddings):
            raise ValueError("news_df and embeddings must have the same length")
        original_index = news_df.index
        work = news_df.reset_index(drop=True)
        emb = l2_normalize(embeddings)
        cluster_by_idx: dict[int, str] = {}

        for topic, group in work.groupby(self.topic_column, sort=False, dropna=False):
            local_indices = group.index.to_list()
            local_emb = emb[local_indices]
            sim = local_emb @ local_emb.T
            days = _date_distance_days(group[self.date_column])
            effective_sim = sim - self.decay_per_day * days
            n = len(local_indices)
            dsu = _DSU(n)
            for i in range(n):
                for j in range(i + 1, n):
                    if (
                        days[i, j] <= self.window_days
                        and effective_sim[i, j] >= self.similarity_threshold
                    ):
                        dsu.union(i, j)
            prefix = f"{_safe_slug(topic)}_decay"
            cluster_by_idx.update(_component_ids(local_indices, dsu, prefix))

        return pd.Series(
            [cluster_by_idx[i] for i in range(len(work))], index=original_index, name="cluster_id"
        )


@dataclass
class OnlineLifecycleClusterer:
    """Sequential clusterer: old inactive clusters stop accepting new items.

    This directly models the project requirement: semantically similar news months apart
    can describe different real-world events.
    """

    similarity_threshold: float = 0.82
    lifecycle_days: int = 14
    centroid_weight: float = 0.6
    max_member_weight: float = 0.4
    topic_column: str = "topic"
    date_column: str = "published_at"

    def fit_predict(self, news_df: pd.DataFrame, embeddings: np.ndarray) -> pd.Series:
        if len(news_df) != len(embeddings):
            raise ValueError("news_df and embeddings must have the same length")
        original_index = news_df.index
        work = news_df.reset_index(drop=True)
        emb = l2_normalize(embeddings)
        dates = pd.to_datetime(work[self.date_column], errors="coerce")
        result = pd.Series(index=work.index, dtype=object, name="cluster_id")

        for topic, topic_df in (
            work.assign(_date=dates)
            .sort_values("_date")
            .groupby(self.topic_column, sort=False, dropna=False)
        ):
            clusters: list[dict] = []
            topic_slug = _safe_slug(topic)
            next_id = 0
            for idx, row in topic_df.iterrows():
                current_date = row["_date"]
                current_emb = emb[idx]
                best_cluster = None
                best_score = -1.0

                for cluster in clusters:
                    if pd.isna(current_date) or pd.isna(cluster["last_seen"]):
                        active = True
                    else:
                        active = (current_date - cluster["last_seen"]).days <= self.lifecycle_days
                    if not active:
                        continue

                    centroid = cluster["centroid"]
                    centroid_sim = float(current_emb @ centroid)
                    member_sims = cluster["members_emb"] @ current_emb
                    max_member_sim = float(member_sims.max()) if len(member_sims) else centroid_sim
                    score = (
                        self.centroid_weight * centroid_sim
                        + self.max_member_weight * max_member_sim
                    )
                    if score > best_score:
                        best_score = score
                        best_cluster = cluster

                if best_cluster is None or best_score < self.similarity_threshold:
                    cluster_id = f"{topic_slug}_life_{next_id:04d}"
                    next_id += 1
                    clusters.append(
                        {
                            "cluster_id": cluster_id,
                            "members_idx": [idx],
                            "members_emb": current_emb[None, :],
                            "centroid": current_emb,
                            "last_seen": current_date,
                        }
                    )
                    result.loc[idx] = cluster_id
                else:
                    best_cluster["members_idx"].append(idx)
                    best_cluster["members_emb"] = np.vstack(
                        [best_cluster["members_emb"], current_emb]
                    )
                    best_cluster["centroid"] = l2_normalize(
                        best_cluster["members_emb"].mean(axis=0, keepdims=True)
                    )[0]
                    best_cluster["last_seen"] = current_date
                    result.loc[idx] = best_cluster["cluster_id"]

        return pd.Series(result.to_numpy(), index=original_index, name="cluster_id")
