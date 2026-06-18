from __future__ import annotations

from dataclasses import asdict, dataclass
from itertools import product
import re

import numpy as np
import pandas as pd

from .config import AttachClusteringConfig, BaseClusteringConfig, SilverPositiveSelectionConfig
from .embeddings import l2_normalize
from .evaluation import evaluate_cluster_ids_on_annotation, normalize_news_id, pair_count

_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+", flags=re.IGNORECASE)
_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")


def _safe_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def tokenize_for_jaccard(text: object) -> set[str]:
    return {m.group(0).lower() for m in _TOKEN_RE.finditer(_safe_text(text))}


def extract_numbers(*texts: object) -> set[str]:
    result: set[str] = set()
    for text in texts:
        normalized = _safe_text(text).replace(",", ".")
        result.update(m.group(0) for m in _NUMBER_RE.finditer(normalized))
    return result


def jaccard(left: set[str], right: set[str]) -> float:
    union = left | right
    if not union:
        return 0.0
    return float(len(left & right) / len(union))


class UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, left: int, right: int) -> bool:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return False
        if self.rank[root_left] < self.rank[root_right]:
            self.parent[root_left] = root_right
        elif self.rank[root_left] > self.rank[root_right]:
            self.parent[root_right] = root_left
        else:
            self.parent[root_right] = root_left
            self.rank[root_left] += 1
        return True


def _relabel_roots(roots: list[int], prefix: str) -> pd.Series:
    root_to_cluster: dict[int, str] = {}
    out: list[str] = []
    for root in roots:
        if root not in root_to_cluster:
            root_to_cluster[root] = f"{prefix}_{len(root_to_cluster):06d}"
        out.append(root_to_cluster[root])
    return pd.Series(out, dtype="string")


def build_threshold_graph_cluster_ids(
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    config: BaseClusteringConfig | None = None,
) -> tuple[pd.Series, dict]:
    """Строгая topic-aware temporal graph кластеризация baseline exp_00b."""

    cfg = config or BaseClusteringConfig()
    if len(news_df) != len(embeddings):
        raise ValueError("news_df and embeddings must have the same length")

    df = news_df.copy().reset_index(drop=True)
    df[cfg.date_column] = pd.to_datetime(df[cfg.date_column], errors="coerce")
    df[cfg.topic_column] = df[cfg.topic_column].fillna("<missing>").astype(str)
    emb = l2_normalize(embeddings)

    n = len(df)
    uf = UnionFind(n)
    edge_count = 0
    similarity_checks = 0

    groups = df.groupby(cfg.topic_column, sort=False, dropna=False) if cfg.use_topic_blocking else [("<all>", df)]

    for _, part in groups:
        if len(part) <= 1:
            continue
        local = part.sort_values([cfg.date_column, cfg.id_column], kind="mergesort")
        idx = local.index.to_numpy(dtype=int)
        dates = local[cfg.date_column].to_numpy()
        local_embeddings = emb[idx]
        sim_matrix = local_embeddings @ local_embeddings.T

        for i in range(len(idx)):
            date_i = dates[i]
            for j in range(i + 1, len(idx)):
                date_j = dates[j]
                if pd.isna(date_i) or pd.isna(date_j):
                    days_diff = 0.0
                else:
                    days_diff = (date_j - date_i) / np.timedelta64(1, "D")
                if days_diff > cfg.story_window_days:
                    break
                similarity_checks += 1
                if sim_matrix[i, j] >= cfg.story_threshold:
                    if uf.union(int(idx[i]), int(idx[j])):
                        edge_count += 1

    cluster_ids = _relabel_roots([uf.find(i) for i in range(n)], cfg.cluster_prefix)
    diagnostics = {
        "n_rows": int(n),
        "n_clusters": int(cluster_ids.nunique()),
        "edge_count": int(edge_count),
        "similarity_checks": int(similarity_checks),
        "story_threshold": float(cfg.story_threshold),
        "story_window_days": int(cfg.story_window_days),
    }
    return cluster_ids, diagnostics


def build_candidate_pairs(
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    *,
    min_similarity: float = 0.75,
    max_days: int = 14,
    id_column: str = "news_id",
    topic_column: str = "topic",
    date_column: str = "published_at",
    title_column: str = "title",
    text_column: str = "text",
) -> pd.DataFrame:
    """Строит пары-кандидаты для second-pass attach.

    В пары попадают новости одной темы внутри временного окна и с similarity
    не ниже минимального порога sweep. Для каждой пары сразу считаются evidence-признаки.
    """

    if len(news_df) != len(embeddings):
        raise ValueError("news_df and embeddings must have the same length")

    df = news_df.copy().reset_index(drop=True)
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df[topic_column] = df[topic_column].fillna("<missing>").astype(str)
    emb = l2_normalize(embeddings)

    title_tokens = [tokenize_for_jaccard(value) for value in df[title_column].fillna("")]
    text_tokens = [tokenize_for_jaccard(value) for value in df[text_column].fillna("")]
    number_sets = [
        extract_numbers(row.get(title_column, ""), row.get(text_column, ""))
        for _, row in df.iterrows()
    ]

    rows: list[dict] = []
    for _, part in df.groupby(topic_column, sort=False, dropna=False):
        local = part.sort_values([date_column, id_column], kind="mergesort")
        idx = local.index.to_numpy(dtype=int)
        dates = local[date_column].to_numpy()
        local_embeddings = emb[idx]
        sim_matrix = local_embeddings @ local_embeddings.T

        for i in range(len(idx)):
            date_i = dates[i]
            left = int(idx[i])
            for j in range(i + 1, len(idx)):
                date_j = dates[j]
                right = int(idx[j])
                if pd.isna(date_i) or pd.isna(date_j):
                    days_diff = 0.0
                else:
                    days_diff = float((date_j - date_i) / np.timedelta64(1, "D"))
                if days_diff > max_days:
                    break
                similarity = float(sim_matrix[i, j])
                if similarity < min_similarity:
                    continue
                rows.append({
                    "left_index": left,
                    "right_index": right,
                    "left_news_id": df.loc[left, id_column],
                    "right_news_id": df.loc[right, id_column],
                    "topic": df.loc[left, topic_column],
                    "similarity": similarity,
                    "days_diff": days_diff,
                    "title_jaccard": jaccard(title_tokens[left], title_tokens[right]),
                    "text_jaccard": jaccard(text_tokens[left], text_tokens[right]),
                    "shared_numbers_count": int(len(number_sets[left] & number_sets[right])),
                })

    return pd.DataFrame(rows)


def _cluster_size_map(cluster_ids: pd.Series) -> dict[str, int]:
    return cluster_ids.astype(str).value_counts().astype(int).to_dict()


def build_best_candidate_attach_clusters(
    news_df: pd.DataFrame,
    candidate_pairs: pd.DataFrame,
    base_cluster_ids: pd.Series,
    config: AttachClusteringConfig,
) -> tuple[pd.Series, dict, pd.DataFrame]:
    """Консервативно прикрепляет маленькие baseline-кластеры к лучшему большему кластеру.

    В отличие от union-find second pass, здесь не добавляются все связи подряд.
    Каждый маленький source-кластер получает максимум один target-кластер.
    """

    n = len(news_df)
    cluster_ids = base_cluster_ids.astype(str).reset_index(drop=True).copy()
    cluster_sizes = _cluster_size_map(cluster_ids)

    if candidate_pairs.empty:
        return cluster_ids, {"candidate_attach_edges": 0, "attached_source_clusters": 0, "attached_rows": 0}, pd.DataFrame()

    rows: list[dict] = []
    for row in candidate_pairs.itertuples(index=False):
        left = int(row.left_index)
        right = int(row.right_index)
        left_cluster = str(cluster_ids.iloc[left])
        right_cluster = str(cluster_ids.iloc[right])
        if left_cluster == right_cluster:
            continue

        if row.similarity < config.min_similarity or row.days_diff > config.max_days:
            continue

        has_evidence = (
            row.title_jaccard >= config.title_jaccard_threshold
            or row.shared_numbers_count >= config.min_shared_numbers
        )
        if config.require_evidence and not has_evidence:
            continue

        for source_cluster, target_cluster, source_idx, target_idx in [
            (left_cluster, right_cluster, left, right),
            (right_cluster, left_cluster, right, left),
        ]:
            source_size = cluster_sizes[source_cluster]
            target_size = cluster_sizes[target_cluster]
            if source_size > config.source_max_cluster_size:
                continue
            if target_size <= source_size:
                continue
            rows.append({
                "source_cluster": source_cluster,
                "target_cluster": target_cluster,
                "source_size": int(source_size),
                "target_size": int(target_size),
                "source_index": int(source_idx),
                "target_index": int(target_idx),
                "source_news_id": news_df.iloc[source_idx].get("news_id"),
                "target_news_id": news_df.iloc[target_idx].get("news_id"),
                "source_title": news_df.iloc[source_idx].get("title", ""),
                "target_title": news_df.iloc[target_idx].get("title", ""),
                "best_similarity": float(row.similarity),
                "min_days_diff": float(row.days_diff),
                "max_title_jaccard": float(row.title_jaccard),
                "max_shared_numbers_count": int(row.shared_numbers_count),
            })

    attach_edges = pd.DataFrame(rows)
    if attach_edges.empty:
        diagnostics = {"candidate_attach_edges": 0, "attached_source_clusters": 0, "attached_rows": 0}
        return cluster_ids, diagnostics, attach_edges

    best_rows: list[pd.Series] = []
    for source_cluster, part in attach_edges.groupby("source_cluster", sort=False):
        # Сначала агрегируем лучший edge по target-кластеру.
        per_target = (
            part.sort_values(["best_similarity", "max_title_jaccard", "max_shared_numbers_count"], ascending=False)
            .groupby("target_cluster", sort=False)
            .head(1)
            .sort_values("best_similarity", ascending=False)
            .reset_index(drop=True)
        )
        best = per_target.iloc[0].copy()
        second_best_similarity = float(per_target.iloc[1]["best_similarity"]) if len(per_target) > 1 else -np.inf
        best["second_best_similarity"] = second_best_similarity
        best["margin"] = float(best["best_similarity"] - second_best_similarity) if np.isfinite(second_best_similarity) else np.inf
        if best["margin"] >= config.min_margin:
            best_rows.append(best)

    selected = pd.DataFrame(best_rows)
    if selected.empty:
        diagnostics = {"candidate_attach_edges": int(len(attach_edges)), "attached_source_clusters": 0, "attached_rows": 0}
        return cluster_ids, diagnostics, selected

    attached_rows = 0
    result_ids = cluster_ids.copy()
    for row in selected.itertuples(index=False):
        source_cluster = str(row.source_cluster)
        target_cluster = str(row.target_cluster)
        mask = result_ids.eq(source_cluster)
        attached_rows += int(mask.sum())
        result_ids.loc[mask] = target_cluster

    # Финально переименуем кластеры в стабильные компактные имена.
    mapping: dict[str, str] = {}
    relabelled: list[str] = []
    for cluster_id in result_ids.astype(str):
        if cluster_id not in mapping:
            mapping[cluster_id] = f"{config.cluster_prefix}_{len(mapping):06d}"
        relabelled.append(mapping[cluster_id])

    final_ids = pd.Series(relabelled, dtype="string")
    diagnostics = {
        "candidate_attach_edges": int(len(attach_edges)),
        "attached_source_clusters": int(len(selected)),
        "attached_rows": int(attached_rows),
        "n_clusters": int(final_ids.nunique()),
    }
    return final_ids, diagnostics, selected.reset_index(drop=True)


def max_cluster_size(cluster_ids: pd.Series) -> int:
    if len(cluster_ids) == 0:
        return 0
    return int(cluster_ids.astype(str).value_counts().max())


def silver_positive_pair_count(silver_reference: pd.DataFrame, cluster_column: str = "cluster_id") -> int:
    return int(silver_reference[cluster_column].astype(str).value_counts().map(pair_count).sum())


def run_silver_positive_attach_sweep(
    *,
    news_df: pd.DataFrame,
    candidate_pairs: pd.DataFrame,
    base_cluster_ids: pd.Series,
    silver_reference: pd.DataFrame,
    selection_config: SilverPositiveSelectionConfig | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.Series], dict[str, pd.DataFrame]]:
    """Подбирает attach-параметры только по silver-positive recall.

    Пары из разных silver-кластеров здесь не считаются надёжными negative.
    """

    cfg = selection_config or SilverPositiveSelectionConfig()
    base_metrics = evaluate_cluster_ids_on_annotation(
        annotation_df=silver_reference,
        candidate_news_df=news_df,
        candidate_cluster_ids=base_cluster_ids,
    )
    base_pred_pairs = max(int(base_metrics["total_pred_pairs"]), 1)

    rows: list[dict] = []
    cluster_ids_by_variant: dict[str, pd.Series] = {"baseline_strict_0.82": base_cluster_ids.astype(str).reset_index(drop=True)}
    attachments_by_variant: dict[str, pd.DataFrame] = {}

    rows.append({
        "variant": "baseline_strict_0.82",
        "min_similarity": np.nan,
        "max_days": np.nan,
        "min_margin": np.nan,
        "source_max_cluster_size": np.nan,
        "title_jaccard_threshold": np.nan,
        "min_shared_numbers": np.nan,
        "require_evidence": np.nan,
        "candidate_attach_edges": 0,
        "attached_source_clusters": 0,
        "attached_rows": 0,
        "all_data_max_cluster_size": max_cluster_size(base_cluster_ids),
        "silver_positive_pairs": base_metrics["total_ref_pairs"],
        "silver_recovered_positive_pairs": base_metrics["tp_same_pairs"],
        "silver_missed_positive_pairs": base_metrics["fn_missed_same_pairs"],
        "silver_positive_recall": base_metrics["pairwise_recall"],
        "silver_total_pred_pairs": base_metrics["total_pred_pairs"],
        "silver_pred_pair_growth": 1.0,
    })

    for values in product(
        cfg.min_similarities,
        cfg.max_days_values,
        cfg.min_margins,
        cfg.source_max_cluster_sizes,
        cfg.title_jaccard_thresholds,
        cfg.min_shared_numbers_values,
    ):
        min_similarity, max_days, min_margin, source_size, title_jaccard, min_numbers = values
        variant = (
            f"exp10_src{source_size}_sim{min_similarity:.2f}_days{max_days}"
            f"_m{min_margin:.2f}_tj{title_jaccard:.2f}_num{min_numbers}"
        )
        attach_cfg = AttachClusteringConfig(
            min_similarity=min_similarity,
            max_days=max_days,
            min_margin=min_margin,
            source_max_cluster_size=source_size,
            require_evidence=cfg.require_evidence,
            title_jaccard_threshold=title_jaccard,
            min_shared_numbers=min_numbers,
            cluster_prefix=variant,
        )
        cluster_ids, diagnostics, selected = build_best_candidate_attach_clusters(
            news_df=news_df,
            candidate_pairs=candidate_pairs,
            base_cluster_ids=base_cluster_ids,
            config=attach_cfg,
        )
        silver_metrics = evaluate_cluster_ids_on_annotation(
            annotation_df=silver_reference,
            candidate_news_df=news_df,
            candidate_cluster_ids=cluster_ids,
        )
        pred_growth = silver_metrics["total_pred_pairs"] / base_pred_pairs
        rows.append({
            "variant": variant,
            "min_similarity": min_similarity,
            "max_days": max_days,
            "min_margin": min_margin,
            "source_max_cluster_size": source_size,
            "title_jaccard_threshold": title_jaccard,
            "min_shared_numbers": min_numbers,
            "require_evidence": cfg.require_evidence,
            **diagnostics,
            "all_data_max_cluster_size": max_cluster_size(cluster_ids),
            "silver_positive_pairs": silver_metrics["total_ref_pairs"],
            "silver_recovered_positive_pairs": silver_metrics["tp_same_pairs"],
            "silver_missed_positive_pairs": silver_metrics["fn_missed_same_pairs"],
            "silver_positive_recall": silver_metrics["pairwise_recall"],
            "silver_total_pred_pairs": silver_metrics["total_pred_pairs"],
            "silver_pred_pair_growth": pred_growth,
        })
        cluster_ids_by_variant[variant] = cluster_ids
        attachments_by_variant[variant] = selected

    return pd.DataFrame(rows), cluster_ids_by_variant, attachments_by_variant


def select_silver_positive_variant(
    sweep_df: pd.DataFrame,
    selection_config: SilverPositiveSelectionConfig | None = None,
) -> pd.Series:
    """Выбирает вариант с максимальным silver-positive recall под safety-ограничениями."""

    cfg = selection_config or SilverPositiveSelectionConfig()
    eligible = sweep_df[
        (sweep_df["silver_pred_pair_growth"] <= cfg.max_pred_pair_growth_over_baseline)
        & (sweep_df["all_data_max_cluster_size"] <= cfg.max_all_data_cluster_size)
    ].copy()
    if eligible.empty:
        eligible = sweep_df[sweep_df["variant"].eq("baseline_strict_0.82")].copy()
    eligible = eligible.sort_values(
        ["silver_positive_recall", "silver_pred_pair_growth", "attached_source_clusters"],
        ascending=[False, True, True],
    ).reset_index(drop=True)
    return eligible.iloc[0]
