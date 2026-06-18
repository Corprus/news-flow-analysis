from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class LegacyBaselineGraphClustererConfig:
    """Config for baseline-compatible graph clustering.

    This clusterer reproduces the clustering logic from the old baseline notebook:

    - news are blocked by topic;
    - inside each topic, rows are sorted by published_at;
    - two news are connected if:
        cosine_similarity >= story_threshold
        and time distance <= story_window_days;
    - connected components become story clusters.
    """

    story_threshold: float = 0.82
    story_window_days: int = 14
    use_topic_blocking: bool = True
    normalize_embeddings: bool = True
    cluster_prefix: str = "baseline_cluster"


class UnionFind:
    """Small deterministic Union-Find implementation for connected components."""

    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        root_a = self.find(a)
        root_b = self.find(b)

        if root_a == root_b:
            return

        if self.rank[root_a] < self.rank[root_b]:
            self.parent[root_a] = root_b
        elif self.rank[root_a] > self.rank[root_b]:
            self.parent[root_b] = root_a
        else:
            self.parent[root_b] = root_a
            self.rank[root_a] += 1


class LegacyBaselineGraphClusterer:
    """Baseline-compatible semantic graph clusterer.

    Important:
    - `news_df` row order must correspond to `embeddings` row order.
    - `embeddings[i]` must represent `news_df.iloc[i]`.
    - For exact baseline compatibility, use the same candidate pool and BAAI/bge-m3
      embeddings as in the old baseline notebook.
    """

    def __init__(
        self,
        config: LegacyBaselineGraphClustererConfig | None = None,
    ) -> None:
        self.config = config or LegacyBaselineGraphClustererConfig()
        self.last_graph_edges_: int | None = None
        self.last_cluster_count_: int | None = None

    def fit_predict(
        self,
        news_df: pd.DataFrame,
        embeddings: np.ndarray,
        topic_column: str = "topic",
        date_column: str = "published_at",
    ) -> pd.Series:
        if len(news_df) != len(embeddings):
            raise ValueError(
                f"news_df and embeddings must have the same length: "
                f"{len(news_df)} != {len(embeddings)}"
            )

        required_columns = {topic_column, date_column}
        missing_columns = required_columns - set(news_df.columns)
        if missing_columns:
            raise ValueError(f"news_df is missing columns: {sorted(missing_columns)}")

        df = news_df.copy().reset_index(drop=True)
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

        if df[date_column].isna().any():
            missing_dates = int(df[date_column].isna().sum())
            raise ValueError(f"Found {missing_dates} rows with invalid published_at")

        emb = np.asarray(embeddings, dtype=np.float32)

        if self.config.normalize_embeddings:
            norms = np.linalg.norm(emb, axis=1, keepdims=True)
            norms = np.where(norms == 0, 1.0, norms)
            emb = emb / norms

        n = len(df)
        union_find = UnionFind(n)

        time_values = df[date_column].to_numpy()

        if self.config.use_topic_blocking:
            groups = df.groupby(topic_column, sort=False).indices.values()
        else:
            groups = [np.arange(n)]

        max_delta = np.timedelta64(self.config.story_window_days, "D")
        total_edges = 0

        for raw_indices in groups:
            indices = np.array(list(raw_indices), dtype=int)

            if len(indices) <= 1:
                continue

            # Важная деталь старого baseline:
            # внутри topic сортируем по времени и смотрим только вперёд,
            # пока не вышли за story_window_days.
            indices = indices[np.argsort(time_values[indices])]

            for local_i, global_i in enumerate(indices):
                j = local_i + 1

                while j < len(indices):
                    global_j = indices[j]

                    if time_values[global_j] - time_values[global_i] > max_delta:
                        break

                    similarity = float(np.dot(emb[global_i], emb[global_j]))

                    if similarity >= self.config.story_threshold:
                        union_find.union(global_i, global_j)
                        total_edges += 1

                    j += 1

        roots = [union_find.find(i) for i in range(n)]

        root_to_cluster: dict[int, str] = {}
        cluster_ids: list[str] = []

        # Важная деталь старого baseline:
        # cluster_id назначаются в порядке прохода по исходным строкам.
        for root in roots:
            if root not in root_to_cluster:
                root_to_cluster[root] = (
                    f"{self.config.cluster_prefix}_{len(root_to_cluster):05d}"
                )

            cluster_ids.append(root_to_cluster[root])

        self.last_graph_edges_ = total_edges
        self.last_cluster_count_ = len(set(cluster_ids))

        print("Рёбер в графе похожести:", self.last_graph_edges_)
        print("Количество кластеров:", self.last_cluster_count_)

        return pd.Series(cluster_ids, index=news_df.index, name="cluster_id")
    
def compare_saved_and_reproduced_clusters(
    saved_pred: pd.DataFrame,
    reproduced_pred: pd.DataFrame,
    id_column: str = "news_id",
    cluster_column: str = "cluster_id",
) -> dict:
    saved = saved_pred[[id_column, cluster_column]].copy()
    reproduced = reproduced_pred[[id_column, cluster_column]].copy()

    saved[id_column] = saved[id_column].astype(str)
    reproduced[id_column] = reproduced[id_column].astype(str)

    merged = saved.merge(
        reproduced,
        on=id_column,
        how="inner",
        suffixes=("_saved", "_reproduced"),
    )

    saved_clusters = merged[f"{cluster_column}_saved"].astype(str).to_numpy()
    reproduced_clusters = merged[f"{cluster_column}_reproduced"].astype(str).to_numpy()

    tp = fp = fn = tn = 0

    n = len(merged)

    for i in range(n):
        for j in range(i + 1, n):
            saved_same = saved_clusters[i] == saved_clusters[j]
            reproduced_same = reproduced_clusters[i] == reproduced_clusters[j]

            if saved_same and reproduced_same:
                tp += 1
            elif not saved_same and reproduced_same:
                fp += 1
            elif saved_same and not reproduced_same:
                fn += 1
            else:
                tn += 1

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return {
        "rows": n,
        "saved_clusters": int(pd.Series(saved_clusters).nunique()),
        "reproduced_clusters": int(pd.Series(reproduced_clusters).nunique()),
        "tp_same_pairs": tp,
        "fp_false_merge_pairs": fp,
        "fn_missed_pairs": fn,
        "tn_diff_pairs": tn,
        "pairwise_precision": precision,
        "pairwise_recall": recall,
        "pairwise_f1": f1,
    }