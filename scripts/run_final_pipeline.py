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


def main() -> None:
    parser = argparse.ArgumentParser(description="Запуск финального Semantic News Novelty pipeline")
    parser.add_argument("--input", required=True, help="CSV с clean-like/raw news")
    parser.add_argument("--output", required=True, help="CSV с предсказаниями")
    parser.add_argument("--model", default=None, help="joblib artifact финальной novelty-модели")
    parser.add_argument("--embeddings-cache", required=True, help="id-aware npz cache для embeddings")
    parser.add_argument("--config", default=None, help="JSON config pipeline")
    parser.add_argument("--project-root", default=".", help="Корень проекта")
    parser.add_argument("--device", default=None, help="cuda/cpu для sentence-transformers")
    parser.add_argument("--force-recompute-embeddings", action="store_true")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    add_project_paths(project_root)

    from final_pipeline import FinalPipelineConfig, load_pipeline
    from final_pipeline.config import FINAL_PIPELINE_CONFIG_RELATIVE_PATH

    config_path = Path(args.config) if args.config else project_root / FINAL_PIPELINE_CONFIG_RELATIVE_PATH
    if not config_path.is_absolute():
        config_path = project_root / config_path
    config = FinalPipelineConfig.from_json(config_path) if config_path.exists() else FinalPipelineConfig()
    news = pd.read_csv(args.input)
    pipeline = load_pipeline(model_path=args.model, config=config, device=args.device, project_root=project_root)
    result = pipeline.run(
        news,
        embeddings_cache_path=args.embeddings_cache,
        force_recompute_embeddings=args.force_recompute_embeddings,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.predictions.to_csv(output_path, index=False)

    print(json.dumps(result.diagnostics, ensure_ascii=False, indent=2))
    print(f"Saved predictions: {output_path}")


if __name__ == "__main__":
    main()
