"""Download a Hugging Face sentence-transformers model into a local directory."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

DEFAULT_REPO_ID = "Korprus/news-flow-ru-vectorization-mpnet"
DEFAULT_OUTPUT_DIR = Path("models/news-flow-ru-vectorization-mpnet/final")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a sentence-transformers model from Hugging Face Hub."
    )
    parser.add_argument("--repo-id", default=None)
    parser.add_argument("--revision", default=None)
    parser.add_argument(
        "--metadata-path",
        type=Path,
        default=None,
        help=(
            "Read repo_id and commit hash from a publish_model.py metadata JSON. "
            "Explicit --repo-id or --revision values take precedence."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--local-files-only", action="store_true")
    parser.add_argument(
        "--disable-xet",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Disable Xet download backend. Useful on Windows if the HF cache "
            "has permission issues."
        ),
    )
    return parser.parse_args()


def load_model_metadata(metadata_path: Path) -> dict[str, str]:
    with metadata_path.open(encoding="utf-8") as file:
        metadata = json.load(file)
    if not isinstance(metadata, dict):
        raise TypeError(f"Expected JSON object in {metadata_path}")
    return metadata


def model_dir_is_ready(output_dir: Path) -> bool:
    required_files = [
        "config.json",
        "modules.json",
        "model.safetensors",
        "tokenizer.json",
        "1_Pooling/config.json",
    ]
    return all((output_dir / file_name).exists() for file_name in required_files)


def download_model(
    repo_id: str,
    output_dir: Path,
    revision: str | None = None,
    force: bool = False,
    local_files_only: bool = False,
) -> Path:
    from huggingface_hub import snapshot_download

    output_dir = output_dir.resolve()
    if model_dir_is_ready(output_dir) and not force:
        print(f"Model already exists: {output_dir}")
        return output_dir

    output_dir.mkdir(parents=True, exist_ok=True)
    token = os.environ.get("HF_TOKEN")
    path = snapshot_download(
        repo_id=repo_id,
        repo_type="model",
        revision=revision,
        local_dir=output_dir,
        token=token or None,
        local_files_only=local_files_only,
    )
    print(f"Downloaded model to: {path}")
    return Path(path)


def main() -> None:
    args = parse_args()
    if args.disable_xet:
        os.environ.setdefault("HF_HUB_DISABLE_XET", "1")

    repo_id = args.repo_id or DEFAULT_REPO_ID
    revision = args.revision
    if args.metadata_path:
        metadata = load_model_metadata(args.metadata_path)
        repo_id = args.repo_id or metadata["repo_id"]
        revision = args.revision or metadata.get("model_revision") or metadata["commit_hash"]

    download_model(
        repo_id=repo_id,
        output_dir=args.output_dir,
        revision=revision,
        force=args.force,
        local_files_only=args.local_files_only,
    )


if __name__ == "__main__":
    main()
