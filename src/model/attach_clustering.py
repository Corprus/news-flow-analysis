from __future__ import annotations

import re
import time
from dataclasses import asdict, dataclass
from itertools import product

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from .data import normalize_news_id
from .embeddings import l2_normalize
from .legacy_clustering import LegacyBaselineGraphClusterer, LegacyBaselineGraphClustererConfig

_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+", flags=re.IGNORECASE)
_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")


@dataclass(frozen=True)
class AttachClusteringConfig:
    """Параметры второго прохода кластеризации.

    Значения по умолчанию соответствуют `exp_10`: они выбраны без использования
    human golden, по silver-positive сигналу и с ограничением роста числа
    предсказанных same-story пар.
    """

    min_similarity: float = 0.75
    max_days: int = 7
    min_margin: float = 0.03
    source_max_cluster_size: int = 2
    require_evidence: bool = True
    title_jaccard_threshold: float = 0.15
    min_shared_numbers: int = 1
    cluster_prefix: str = "exp10"


@dataclass(frozen=True)
class SilverPositiveSelectionConfig:
    """Сетка и safety-ограничения для выбора attach-параметров по silver.

    Silver используется только как weak-positive источник: пары внутри одного
    silver-кластера считаются вероятными same-story парами. Пары из разных
    silver-кластеров не считаются надёжными негативами.
    """

    min_similarities: tuple[float, ...] = (0.75, 0.76, 0.77, 0.78, 0.79, 0.80)
    max_days_values: tuple[int, ...] = (7, 10, 14)
    min_margins: tuple[float, ...] = (0.03, 0.05, 0.07, 0.10)
    source_max_cluster_sizes: tuple[int, ...] = (1, 2)
    title_jaccard_thresholds: tuple[float, ...] = (0.05, 0.10, 0.15)
    min_shared_numbers_values: tuple[int, ...] = (1, 2)
    require_evidence: bool = True
    max_pred_pair_growth_over_baseline: float = 1.55
    max_all_data_cluster_size: int = 80


@dataclass(frozen=True)
class BaselineClusteringConfig:
    """Параметры строгого baseline `exp_00b`."""

    story_threshold: float = 0.82
    story_window_days: int = 14
    use_topic_blocking: bool = True
    normalize_embeddings: bool = True
    cluster_prefix: str = "exp00b"


def safe_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def tokenize_for_jaccard(text: object) -> set[str]:
    return {m.group(0).lower() for m in _TOKEN_RE.finditer(safe_text(text))}


def extract_numbers(*texts: object) -> set[str]:
    result: set[str] = set()
    for text in texts:
        normalized = safe_text(text).replace(",", ".")
        result.update(match.group(0) for match in _NUMBER_RE.finditer(normalized))
    return result


def jaccard(left: set[str], right: set[str]) -> float:
    union = left | right
    if not union:
        return 0.0
    return float(len(left & right) / len(union))


def pair_count(count: int) -> int:
    return int(count * (count - 1) // 2)


def max_cluster_size(cluster_ids: pd.Series | np.ndarray | list) -> int:
    series = pd.Series(cluster_ids).astype(str)
    if series.empty:
        return 0
    return int(series.value_counts().max())


def build_baseline_cluster_ids(
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    config: BaselineClusteringConfig | None = None,
) -> tuple[pd.Series, dict]:
    """Строит строгую baseline-кластеризацию, совместимую с `exp_00b`."""

    cfg = config or BaselineClusteringConfig()
    clusterer = LegacyBaselineGraphClusterer(
        LegacyBaselineGraphClustererConfig(
            story_threshold=cfg.story_threshold,
            story_window_days=cfg.story_window_days,
            use_topic_blocking=cfg.use_topic_blocking,
            normalize_embeddings=cfg.normalize_embeddings,
            cluster_prefix=cfg.cluster_prefix,
        )
    )
    cluster_ids = clusterer.fit_predict(news_df, embeddings).astype(str)
    diagnostics = {
        "n_rows": int(len(news_df)),
        "n_clusters": int(cluster_ids.nunique()),
        "edge_count": int(clusterer.last_graph_edges_ or 0),
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
    show_progress: bool = False,
) -> pd.DataFrame:
    """Строит пары-кандидаты для второго прохода attach.

    В пары попадают новости одной темы, внутри временного окна и с cosine
    similarity не ниже `min_similarity`. Для каждой пары сразу считаются
    evidence-признаки: похожесть заголовков, текстов и пересечения чисел.
    """

    if len(news_df) != len(embeddings):
        raise ValueError("news_df and embeddings must have the same length")

    df = news_df.copy().reset_index(drop=True)
    df[id_column] = normalize_news_id(df[id_column])
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df[topic_column] = df[topic_column].fillna("<missing>").astype(str)
    emb = l2_normalize(np.asarray(embeddings, dtype=np.float32))

    title_tokens = [tokenize_for_jaccard(value) for value in df[title_column].fillna("")]
    text_tokens = [tokenize_for_jaccard(value) for value in df[text_column].fillna("")]
    number_sets = [
        extract_numbers(row.get(title_column, ""), row.get(text_column, ""))
        for _, row in tqdm(
            df.iterrows(),
            total=len(df),
            desc="Extract pair evidence",
            disable=not show_progress,
        )
    ]

    rows: list[dict] = []
    topic_groups = list(df.groupby(topic_column, sort=False, dropna=False))

    for topic, part in tqdm(
        topic_groups,
        total=len(topic_groups),
        desc="Build candidate pairs",
        disable=not show_progress,
    ):
        if len(part) <= 1:
            continue

        local = part.sort_values([date_column, id_column], kind="mergesort")
        idx = local.index.to_numpy(dtype=int)
        dates = local[date_column].to_numpy()
        local_embeddings = emb[idx]
        sim_matrix = local_embeddings @ local_embeddings.T

        for i in range(len(idx)):
            date_i = dates[i]
            for j in range(i + 1, len(idx)):
                date_j = dates[j]
                if pd.isna(date_i) or pd.isna(date_j):
                    days_diff = 0.0
                else:
                    days_diff = float((date_j - date_i) / np.timedelta64(1, "D"))
                if days_diff > max_days:
                    break

                similarity = float(sim_matrix[i, j])
                if similarity < min_similarity:
                    continue

                left_idx = int(idx[i])
                right_idx = int(idx[j])
                left_numbers = number_sets[left_idx]
                right_numbers = number_sets[right_idx]

                rows.append(
                    {
                        "left_idx": left_idx,
                        "right_idx": right_idx,
                        "left_news_id": df.at[left_idx, id_column],
                        "right_news_id": df.at[right_idx, id_column],
                        "topic": topic,
                        "similarity": similarity,
                        "days_diff": days_diff,
                        "title_jaccard": jaccard(title_tokens[left_idx], title_tokens[right_idx]),
                        "text_jaccard": jaccard(text_tokens[left_idx], text_tokens[right_idx]),
                        "shared_numbers_count": int(len(left_numbers & right_numbers)),
                    }
                )

    return pd.DataFrame(rows)


def build_best_candidate_attach_clusters(
    news_df: pd.DataFrame,
    candidate_pairs: pd.DataFrame,
    base_cluster_ids: pd.Series | np.ndarray | list,
    config: AttachClusteringConfig | None = None,
    show_progress: bool = False,
) -> tuple[pd.Series, dict, pd.DataFrame]:
    """Второй проход кластеризации через conservative best-candidate attach.

    Алгоритм не добавляет все дополнительные связи в Union-Find. Вместо этого
    маленький baseline-кластер прикрепляется к одному лучшему более крупному
    кластеру, если выполнены ограничения по similarity, времени, margin и
    evidence. Это снижает риск chaining-effect и ложных склеек.
    """

    cfg = config or AttachClusteringConfig()
    if len(news_df) != len(base_cluster_ids):
        raise ValueError("news_df and base_cluster_ids must have the same length")

    df = news_df.copy().reset_index(drop=True)
    base_ids = pd.Series(base_cluster_ids, index=df.index, dtype="string").astype(str)
    cluster_sizes = base_ids.value_counts().to_dict()

    if candidate_pairs.empty:
        diagnostics = {
            "candidate_attach_edges": 0,
            "attached_source_clusters": 0,
            "attached_rows": 0,
        }
        return base_ids.rename("cluster_id"), diagnostics, pd.DataFrame()

    pairs = candidate_pairs.copy()
    pairs = pairs[
        (pairs["similarity"] >= cfg.min_similarity) & (pairs["days_diff"] <= cfg.max_days)
    ].copy()

    if pairs.empty:
        diagnostics = {
            "candidate_attach_edges": 0,
            "attached_source_clusters": 0,
            "attached_rows": 0,
        }
        return base_ids.rename("cluster_id"), diagnostics, pd.DataFrame()

    rows: list[dict] = []

    for _, pair in tqdm(
        pairs.iterrows(),
        total=len(pairs),
        desc="Score attach candidates",
        leave=False,
        disable=not show_progress,
    ):
        left_idx = int(pair["left_idx"])
        right_idx = int(pair["right_idx"])
        left_cluster = str(base_ids.iloc[left_idx])
        right_cluster = str(base_ids.iloc[right_idx])

        if left_cluster == right_cluster:
            continue

        for source_cluster, target_cluster, source_idx, target_idx in (
            (left_cluster, right_cluster, left_idx, right_idx),
            (right_cluster, left_cluster, right_idx, left_idx),
        ):
            source_size = int(cluster_sizes.get(source_cluster, 0))
            target_size = int(cluster_sizes.get(target_cluster, 0))

            if source_size > cfg.source_max_cluster_size:
                continue
            if target_size <= source_size:
                continue

            has_title_evidence = float(pair["title_jaccard"]) >= cfg.title_jaccard_threshold
            has_number_evidence = int(pair["shared_numbers_count"]) >= cfg.min_shared_numbers

            if cfg.require_evidence and not (has_title_evidence or has_number_evidence):
                continue

            rows.append(
                {
                    "source_cluster": source_cluster,
                    "target_cluster": target_cluster,
                    "source_idx": source_idx,
                    "target_idx": target_idx,
                    "source_news_id": df.iloc[source_idx].get("news_id"),
                    "target_news_id": df.iloc[target_idx].get("news_id"),
                    "source_title": df.iloc[source_idx].get("title", ""),
                    "target_title": df.iloc[target_idx].get("title", ""),
                    "source_size": source_size,
                    "target_size": target_size,
                    "best_similarity": float(pair["similarity"]),
                    "min_days_diff": float(pair["days_diff"]),
                    "max_title_jaccard": float(pair["title_jaccard"]),
                    "max_shared_numbers_count": int(pair["shared_numbers_count"]),
                }
            )

    raw_candidates = pd.DataFrame(rows)
    if raw_candidates.empty:
        diagnostics = {
            "candidate_attach_edges": 0,
            "candidate_attach_targets": 0,
            "attached_source_clusters": 0,
            "attached_rows": 0,
        }
        return base_ids.rename("cluster_id"), diagnostics, raw_candidates

    # Важно: margin должен считаться между целевыми кластерами, а не между
    # отдельными парами новостей. Иначе один source-кластер может иметь две
    # почти одинаковые пары в один и тот же target-кластер и будет ошибочно
    # отброшен как "неуверенный". Именно это ломало воспроизводимость старого
    # exp_10 после рефакторинга v3.
    target_rows: list[dict] = []
    for (_source_cluster, _target_cluster), part in raw_candidates.groupby(
        ["source_cluster", "target_cluster"],
        sort=False,
    ):
        best_idx = part["best_similarity"].astype(float).idxmax()
        best = part.loc[best_idx].to_dict()
        best["best_similarity"] = float(part["best_similarity"].max())
        best["min_days_diff"] = float(part["min_days_diff"].min())
        best["max_title_jaccard"] = float(part["max_title_jaccard"].max())
        best["max_shared_numbers_count"] = int(part["max_shared_numbers_count"].max())
        target_rows.append(best)

    candidates = pd.DataFrame(target_rows)

    candidates = candidates.sort_values(
        ["source_cluster", "best_similarity", "target_size", "min_days_diff"],
        ascending=[True, False, False, True],
        kind="mergesort",
    )

    selected_rows: list[dict] = []
    for _source_cluster, part in candidates.groupby("source_cluster", sort=False):
        part = part.reset_index(drop=True)
        best = part.iloc[0].to_dict()
        second_best_similarity = (
            float(part.iloc[1]["best_similarity"]) if len(part) > 1 else -np.inf
        )
        margin = float(best["best_similarity"] - second_best_similarity)

        if len(part) > 1 and margin < cfg.min_margin:
            continue

        best["second_best_similarity"] = second_best_similarity
        best["margin"] = margin
        selected_rows.append(best)

    selected = pd.DataFrame(selected_rows)
    final_ids = base_ids.copy()

    # Применяем attach в порядке убывания similarity. Один source-кластер
    # прикрепляется только один раз, а target остаётся baseline-кластером.
    if not selected.empty:
        selected = selected.sort_values("best_similarity", ascending=False, kind="mergesort")
        source_to_target = dict(
            zip(
                selected["source_cluster"],
                selected["target_cluster"],
                strict=True,
            )
        )
        final_ids = final_ids.map(
            lambda cluster_id: source_to_target.get(str(cluster_id), str(cluster_id))
        )

    diagnostics = {
        "candidate_attach_edges": int(len(raw_candidates)),
        "candidate_attach_targets": int(len(candidates)),
        "attached_source_clusters": int(len(selected)),
        "attached_rows": int(
            sum(
                cluster_sizes.get(str(cluster), 0) for cluster in selected.get("source_cluster", [])
            )
        ),
    }
    return final_ids.astype(str).rename("cluster_id"), diagnostics, selected


def make_clustered_news(
    news_df: pd.DataFrame, cluster_ids: pd.Series | np.ndarray | list
) -> pd.DataFrame:
    """Возвращает копию news_df с заданным cluster_id."""

    if len(news_df) != len(cluster_ids):
        raise ValueError("news_df and cluster_ids must have the same length")
    out = news_df.copy().reset_index(drop=True)
    out["cluster_id"] = pd.Series(cluster_ids, dtype="string").astype(str).to_numpy()
    return out


def evaluate_cluster_ids_on_reference(
    reference_df: pd.DataFrame,
    candidate_news_df: pd.DataFrame,
    candidate_cluster_ids: pd.Series | np.ndarray | list,
    *,
    id_column: str = "news_id",
    cluster_column: str = "cluster_id",
) -> dict:
    """Быстрая pairwise-оценка cluster_id без O(n^2) цикла."""

    reference = reference_df[[id_column, cluster_column]].copy()
    reference[id_column] = normalize_news_id(reference[id_column])
    reference[cluster_column] = reference[cluster_column].astype(str)
    reference = reference.dropna(subset=[id_column, cluster_column])
    reference = reference.drop_duplicates(subset=[id_column], keep="first")

    candidate = candidate_news_df[[id_column]].copy()
    candidate[id_column] = normalize_news_id(candidate[id_column])
    candidate["cluster_id_pred"] = (
        pd.Series(candidate_cluster_ids, dtype="string").astype(str).to_numpy()
    )

    merged = reference.merge(candidate, on=id_column, how="inner")
    ref = merged[cluster_column].astype(str)
    pred = merged["cluster_id_pred"].astype(str)

    frame = pd.DataFrame({"ref": ref.to_numpy(), "pred": pred.to_numpy()})
    ref_sizes = frame.groupby("ref").size()
    pred_sizes = frame.groupby("pred").size()
    joint_sizes = frame.groupby(["ref", "pred"]).size()

    total_ref_pairs = int(ref_sizes.map(pair_count).sum())
    total_pred_pairs = int(pred_sizes.map(pair_count).sum())
    tp_same_pairs = int(joint_sizes.map(pair_count).sum())
    fp_false_merge_pairs = total_pred_pairs - tp_same_pairs
    fn_missed_same_pairs = total_ref_pairs - tp_same_pairs

    precision = tp_same_pairs / total_pred_pairs if total_pred_pairs else 0.0
    recall = tp_same_pairs / total_ref_pairs if total_ref_pairs else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    false_merge_rate = fp_false_merge_pairs / total_pred_pairs if total_pred_pairs else 0.0

    return {
        "n_items": int(len(merged)),
        "total_ref_pairs": total_ref_pairs,
        "total_pred_pairs": total_pred_pairs,
        "tp_same_pairs": tp_same_pairs,
        "fp_false_merge_pairs": fp_false_merge_pairs,
        "fn_missed_same_pairs": fn_missed_same_pairs,
        "pairwise_precision": float(precision),
        "pairwise_recall": float(recall),
        "pairwise_f1": float(f1),
        "false_merge_rate": float(false_merge_rate),
        "coverage": float(len(merged) / len(reference)) if len(reference) else 0.0,
    }


def prepare_silver_positive_reference(
    silver_df: pd.DataFrame,
    golden_df: pd.DataFrame,
    candidate_news_df: pd.DataFrame,
    *,
    id_column: str = "news_id",
    cluster_column: str = "cluster_id",
) -> pd.DataFrame:
    """Готовит silver для weak-positive выбора: удаляет golden и чужие ids."""

    silver = silver_df.copy()
    silver[id_column] = normalize_news_id(silver[id_column])
    silver[cluster_column] = silver[cluster_column].astype(str)

    golden_ids = set(normalize_news_id(golden_df[id_column]))
    candidate_ids = set(normalize_news_id(candidate_news_df[id_column]))

    silver = silver[~silver[id_column].isin(golden_ids)].copy()
    silver = silver[silver[id_column].isin(candidate_ids)].copy()
    silver = silver.drop_duplicates(subset=[id_column], keep="first")
    return silver


def run_silver_positive_attach_sweep(
    news_df: pd.DataFrame,
    embeddings: np.ndarray,
    silver_reference: pd.DataFrame,
    base_cluster_ids: pd.Series | np.ndarray | list,
    selection_config: SilverPositiveSelectionConfig | None = None,
    *,
    candidate_pairs: pd.DataFrame | None = None,
    show_progress: bool = False,
) -> tuple[pd.DataFrame, dict[str, pd.Series], dict[str, pd.DataFrame]]:
    """Подбирает attach-параметры только по silver-positive recall.

    Возвращает таблицу sweep, cluster_id для каждого варианта и выбранные attach.
    Golden внутри этой функции не используется.
    """

    cfg = selection_config or SilverPositiveSelectionConfig()
    if show_progress:
        candidate_pair_count = "auto" if candidate_pairs is None else len(candidate_pairs)
        print(
            "Starting run_silver_positive_attach_sweep: "
            f"news_rows={len(news_df)}, silver_rows={len(silver_reference)}, "
            f"candidate_pairs={candidate_pair_count}",
            flush=True,
        )

    base_ids = pd.Series(base_cluster_ids, dtype="string").astype(str)

    if candidate_pairs is None:
        if show_progress:
            print("Building candidate_pairs inside sweep", flush=True)
        candidate_pairs = build_candidate_pairs(
            news_df,
            embeddings,
            min_similarity=min(cfg.min_similarities),
            max_days=max(cfg.max_days_values),
            show_progress=show_progress,
        )

    if show_progress:
        print("Evaluating baseline silver metrics", flush=True)
    baseline_metrics = evaluate_cluster_ids_on_reference(silver_reference, news_df, base_ids)
    baseline_pred_pairs = max(int(baseline_metrics["total_pred_pairs"]), 1)
    if show_progress:
        print(
            "Baseline silver metrics ready: "
            f"positive_pairs={baseline_metrics['total_ref_pairs']}, "
            f"pred_pairs={baseline_metrics['total_pred_pairs']}",
            flush=True,
        )

    rows: list[dict] = []
    cluster_ids_by_variant: dict[str, pd.Series] = {"baseline_strict_0.82": base_ids}
    attachments_by_variant: dict[str, pd.DataFrame] = {"baseline_strict_0.82": pd.DataFrame()}

    rows.append(
        {
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
            "all_data_max_cluster_size": max_cluster_size(base_ids),
            "silver_positive_pairs": baseline_metrics["total_ref_pairs"],
            "silver_recovered_positive_pairs": baseline_metrics["tp_same_pairs"],
            "silver_missed_positive_pairs": baseline_metrics["fn_missed_same_pairs"],
            "silver_positive_recall": baseline_metrics["pairwise_recall"],
            "silver_total_pred_pairs": baseline_metrics["total_pred_pairs"],
            "silver_pred_pair_growth": 1.0,
        }
    )

    sweep_variants = list(
        product(
            cfg.min_similarities,
            cfg.max_days_values,
            cfg.min_margins,
            cfg.source_max_cluster_sizes,
            cfg.title_jaccard_thresholds,
            cfg.min_shared_numbers_values,
        )
    )

    if show_progress:
        print(f"Running exp_10 attach sweep: {len(sweep_variants)} variants")

    started_at = time.perf_counter()
    for variant_index, (
        min_similarity,
        max_days,
        min_margin,
        source_size,
        title_jaccard_threshold,
        min_shared_numbers,
    ) in enumerate(
        tqdm(
            sweep_variants,
            total=len(sweep_variants),
            desc="exp_10 attach sweep",
            disable=not show_progress,
        ),
        start=1,
    ):
        variant_name = (
            f"exp10_src{source_size}_sim{min_similarity:.2f}"
            f"_days{max_days}_m{min_margin:.2f}"
            f"_tj{title_jaccard_threshold:.2f}_num{min_shared_numbers}"
        )
        if show_progress and (variant_index == 1 or variant_index % 10 == 0):
            elapsed = time.perf_counter() - started_at
            print(
                f"[exp_10 sweep] {variant_index}/{len(sweep_variants)} "
                f"variants; elapsed={elapsed:.1f}s; current={variant_name}",
                flush=True,
            )
        attach_config = AttachClusteringConfig(
            min_similarity=min_similarity,
            max_days=max_days,
            min_margin=min_margin,
            source_max_cluster_size=source_size,
            require_evidence=cfg.require_evidence,
            title_jaccard_threshold=title_jaccard_threshold,
            min_shared_numbers=min_shared_numbers,
            cluster_prefix=variant_name,
        )
        cluster_ids, diagnostics, selected = build_best_candidate_attach_clusters(
            news_df,
            candidate_pairs,
            base_ids,
            attach_config,
            show_progress=show_progress,
        )
        silver_metrics = evaluate_cluster_ids_on_reference(silver_reference, news_df, cluster_ids)
        pred_pair_growth = silver_metrics["total_pred_pairs"] / baseline_pred_pairs

        rows.append(
            {
                "variant": variant_name,
                **asdict(attach_config),
                **diagnostics,
                "all_data_max_cluster_size": max_cluster_size(cluster_ids),
                "silver_positive_pairs": silver_metrics["total_ref_pairs"],
                "silver_recovered_positive_pairs": silver_metrics["tp_same_pairs"],
                "silver_missed_positive_pairs": silver_metrics["fn_missed_same_pairs"],
                "silver_positive_recall": silver_metrics["pairwise_recall"],
                "silver_total_pred_pairs": silver_metrics["total_pred_pairs"],
                "silver_pred_pair_growth": float(pred_pair_growth),
            }
        )
        cluster_ids_by_variant[variant_name] = cluster_ids
        attachments_by_variant[variant_name] = selected

    sweep = pd.DataFrame(rows)
    return sweep, cluster_ids_by_variant, attachments_by_variant


def select_silver_positive_variant(
    sweep_df: pd.DataFrame,
    selection_config: SilverPositiveSelectionConfig | None = None,
) -> pd.Series:
    """Выбирает лучший вариант sweep без использования golden."""

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
        kind="mergesort",
    ).reset_index(drop=True)
    return eligible.iloc[0]


def get_attach_config_from_sweep_row(row: pd.Series) -> AttachClusteringConfig:
    """Создаёт AttachClusteringConfig из строки sweep."""

    return AttachClusteringConfig(
        min_similarity=float(row["min_similarity"]),
        max_days=int(row["max_days"]),
        min_margin=float(row["min_margin"]),
        source_max_cluster_size=int(row["source_max_cluster_size"]),
        require_evidence=bool(row["require_evidence"]),
        title_jaccard_threshold=float(row["title_jaccard_threshold"]),
        min_shared_numbers=int(row["min_shared_numbers"]),
        cluster_prefix=str(row["variant"]),
    )
