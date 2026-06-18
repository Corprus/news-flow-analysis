from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd


def add_project_paths(project_root: Path) -> None:
    for path in [project_root, project_root / "src"]:
        text = str(path.resolve())
        if text not in sys.path:
            sys.path.insert(0, text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark final v3 news novelty pipeline.")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--input", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--model-path", type=Path, default=None)
    parser.add_argument("--pipeline-config", type=Path, default=None)
    parser.add_argument("--embedding-cache", type=Path, default=None)
    parser.add_argument("--n-rows", type=int, default=10_000)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--device", type=str, default=None)
    parser.add_argument("--force-recompute-embeddings", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = args.project_root.resolve()
    add_project_paths(project_root)

    from final_pipeline import FinalPipelineConfig, load_pipeline
    from final_pipeline.config import FINAL_PIPELINE_CONFIG_RELATIVE_PATH

    input_path = args.input or project_root / "data" / "prepared" / "lenta_clean_news.csv"
    output_dir = args.output_dir or project_root / "data" / "artifacts" / "final_pipeline_benchmark"
    output_dir.mkdir(parents=True, exist_ok=True)

    config_path = args.pipeline_config or project_root / FINAL_PIPELINE_CONFIG_RELATIVE_PATH
    if not config_path.is_absolute():
        config_path = project_root / config_path
    config = FinalPipelineConfig.from_json(config_path) if config_path.exists() else FinalPipelineConfig()
    if args.no_progress:
        config = FinalPipelineConfig(
            embedding_model_name=config.embedding_model_name,
            embedding_batch_size=config.embedding_batch_size,
            normalize_embeddings=config.normalize_embeddings,
            show_progress_bar=False,
            text_column=config.text_column,
            id_column=config.id_column,
            base_clustering=config.base_clustering,
            attach_clustering=config.attach_clustering,
        )

    raw_news = pd.read_csv(input_path)
    if args.n_rows and len(raw_news) > args.n_rows:
        raw_news = raw_news.sample(n=args.n_rows, random_state=args.random_state).sort_index()

    predictions_path = output_dir / f"final_predictions_{len(raw_news)}.csv"
    timing_path = output_dir / f"final_benchmark_{len(raw_news)}_timing.json"
    embedding_cache = args.embedding_cache or output_dir / f"embeddings_{len(raw_news)}.npz"

    started = time.perf_counter()
    pipeline = load_pipeline(
        model_path=args.model_path,
        config=config,
        device=args.device,
        project_root=project_root,
    )
    load_seconds = time.perf_counter() - started

    predict_started = time.perf_counter()
    result = pipeline.run(
        raw_news,
        embeddings_cache_path=embedding_cache,
        force_recompute_embeddings=args.force_recompute_embeddings,
    )
    predict_seconds = time.perf_counter() - predict_started

    result.predictions.to_csv(predictions_path, index=False)
    total_seconds = time.perf_counter() - started
    payload = {
        "rows": int(len(raw_news)),
        "total_seconds": total_seconds,
        "load_pipeline_seconds": load_seconds,
        "predict_seconds": predict_seconds,
        "rows_per_second": float(len(raw_news) / total_seconds) if total_seconds else None,
        "input_path": str(input_path),
        "predictions_path": str(predictions_path),
        "embedding_cache": str(embedding_cache),
        "model_path": str(args.model_path) if args.model_path else None,
        "pipeline_config": str(config_path) if config_path.exists() else None,
        "diagnostics": result.diagnostics,
    }
    timing_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
