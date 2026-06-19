from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def add_project_paths(project_root: Path) -> None:
    for path in [project_root, project_root / "src"]:
        text = str(path.resolve())
        if text not in sys.path:
            sys.path.insert(0, text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay benchmark: incremental pipeline against full reclustering."
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--input", type=Path, default=None)
    parser.add_argument("--embedding-cache", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--model-path", type=Path, default=None)
    parser.add_argument("--pipeline-config", type=Path, default=None)
    parser.add_argument("--bootstrap-days", type=int, default=14)
    parser.add_argument("--checkpoint-days", type=int, default=7)
    parser.add_argument("--max-checkpoints", type=int, default=None)
    parser.add_argument("--device", type=str, default=None)
    return parser.parse_args()


def _resolve(project_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else project_root / path


def main() -> None:
    args = parse_args()
    project_root = args.project_root.resolve()
    add_project_paths(project_root)

    from final_pipeline import IncrementalNewsNoveltyPipeline, load_pipeline
    from final_pipeline.config import FINAL_PIPELINE_CONFIG_RELATIVE_PATH, FinalPipelineConfig
    from final_pipeline.replay_benchmark import ReplayBenchmarkConfig, run_replay_benchmark
    from model.embeddings import load_id_aligned_embeddings

    input_path = _resolve(
        project_root,
        args.input or Path("data/prepared/lenta_golden_candidate_pool.csv"),
    )
    embedding_cache = _resolve(
        project_root,
        args.embedding_cache
        or Path("data/artifacts/embeddings/bge_m3_candidate_pool_id_aligned.npz"),
    )
    output_dir = _resolve(
        project_root,
        args.output_dir or Path("data/artifacts/incremental_pipeline_benchmark"),
    )
    config_path = _resolve(
        project_root,
        args.pipeline_config or FINAL_PIPELINE_CONFIG_RELATIVE_PATH,
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    news = pd.read_csv(input_path)
    news["news_id"] = news["news_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    embeddings = load_id_aligned_embeddings(embedding_cache, news["news_id"])
    final_config = FinalPipelineConfig.from_json(config_path)
    full_pipeline = load_pipeline(
        model_path=args.model_path,
        config=final_config,
        device=args.device,
        project_root=project_root,
    )
    incremental_pipeline = IncrementalNewsNoveltyPipeline(
        encoder=full_pipeline.encoder,
        novelty_model=full_pipeline.novelty_model,
        final_config=final_config,
    )
    result = run_replay_benchmark(
        news_df=news,
        embeddings=embeddings,
        full_pipeline=full_pipeline,
        incremental_pipeline=incremental_pipeline,
        config=ReplayBenchmarkConfig(
            bootstrap_days=args.bootstrap_days,
            checkpoint_days=args.checkpoint_days,
            max_checkpoints=args.max_checkpoints,
        ),
    )

    metrics_path = output_dir / "incremental_vs_full_metrics.csv"
    assignments_path = output_dir / "incremental_vs_full_assignments.csv"
    novelty_path = output_dir / "incremental_vs_full_novelty.csv"
    diagnostics_path = output_dir / "incremental_checkpoint_diagnostics.csv"
    summary_path = output_dir / "summary.json"
    result.metrics.to_csv(metrics_path, index=False)
    result.assignments.to_csv(assignments_path, index=False)
    result.novelty_comparison.to_csv(novelty_path, index=False)
    result.checkpoint_diagnostics.to_csv(diagnostics_path, index=False)

    final_row = result.metrics.iloc[-1].to_dict()
    summary = {
        "input": str(input_path),
        "embedding_cache": str(embedding_cache),
        "rows": int(len(news)),
        "bootstrap_days": args.bootstrap_days,
        "checkpoint_days": args.checkpoint_days,
        "checkpoints": int(len(result.metrics)),
        "final_checkpoint": {
            key: value.isoformat() if isinstance(value, pd.Timestamp) else value
            for key, value in final_row.items()
        },
        "metrics_path": str(metrics_path),
    }
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=float),
        encoding="utf-8",
    )
    print(result.metrics.to_string(index=False))
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=float))


if __name__ == "__main__":
    main()
