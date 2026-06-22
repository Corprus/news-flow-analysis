from __future__ import annotations

from typing import Any


def group_search_items(
    items: list[dict[str, Any]],
    *,
    top_k: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        cluster_id = str(item["cluster_id"])
        grouped.setdefault(cluster_id, []).append(item)

    clusters: list[dict[str, Any]] = []
    for cluster_items in list(grouped.values())[:top_k]:
        representative = cluster_items[0]
        chronological_items = sorted(
            cluster_items,
            key=lambda item: (item.get("published_at") or "", item["rank"]),
        )
        published_dates = [
            item["published_at"]
            for item in chronological_items
            if item.get("published_at")
        ]
        cluster = {
            "cluster_id": representative["cluster_id"],
            "representative_article_id": representative["article_id"],
            "representative_title": representative["title"],
            "article_count": len(cluster_items),
            "significant_count": sum(
                item.get("novelty_label") == "significant"
                for item in cluster_items
            ),
            "published_from": published_dates[0] if published_dates else None,
            "published_to": published_dates[-1] if published_dates else None,
            "items": chronological_items,
        }
        if representative.get("score") is not None:
            cluster["score"] = representative["score"]
        clusters.append(cluster)
    return clusters
