"""Publish a local sentence-transformers model directory to Hugging Face Hub."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_MODEL_DIR = Path("models/news-flow-ru-vectorization-mpnet/final")
DEFAULT_REPO_ID = "Korprus/news-flow-ru-vectorization-mpnet"
DEFAULT_METADATA_PATH = Path("configs/model_registry/latest_model.json")


@dataclass(frozen=True)
class PublishedModel:
    repo_id: str
    repo_type: str
    model_revision: str
    commit_hash: str
    commit_url: str
    revision: str | None
    tag: str | None
    version: str | None
    local_model_path: str
    docker_model_dir: str
    model_dir: str
    published_at: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload the final sentence-transformers model to Hugging Face Hub."
    )
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--repo-id", default=DEFAULT_REPO_ID)
    parser.add_argument("--repo-type", default="model", choices=["model", "dataset", "space"])
    parser.add_argument("--revision", default=None)
    parser.add_argument("--tag", default=None)
    parser.add_argument(
        "--auto-tag",
        choices=["none", "patch", "minor", "major"],
        default="patch",
        help="Automatically create the next semantic version tag from metadata.",
    )
    parser.add_argument("--tag-message", default=None)
    parser.add_argument("--private", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--metadata-path", type=Path, default=DEFAULT_METADATA_PATH)
    parser.add_argument(
        "--docker-model-dir",
        default="/app/models/news-flow-ru-vectorization-mpnet/final",
    )
    parser.add_argument("--skip-metadata", action="store_true")
    parser.add_argument(
        "--commit-message",
        default="Upload fine-tuned sentence-transformers model",
    )
    parser.add_argument(
        "--disable-xet",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Disable Xet upload backend. Useful on Windows if the HF cache "
            "has permission issues."
        ),
    )
    return parser.parse_args()


def load_existing_metadata(metadata_path: Path) -> dict[str, object]:
    if not metadata_path.exists():
        return {}
    with metadata_path.open(encoding="utf-8") as file:
        metadata = json.load(file)
    if not isinstance(metadata, dict):
        raise TypeError(f"Expected JSON object in {metadata_path}")
    return metadata


def parse_version_tag(tag: str | None) -> tuple[int, int, int] | None:
    if not tag:
        return None
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", tag)
    if not match:
        return None
    return tuple(int(part) for part in match.groups())


def increment_version(version: tuple[int, int, int] | None, bump: str) -> str:
    if version is None:
        return "v1.0.0" if bump == "major" else "v0.1.0"

    major, minor, patch = version or (0, 0, 0)
    if bump == "major":
        major += 1
        minor = 0
        patch = 0
    elif bump == "minor":
        minor += 1
        patch = 0
    elif bump == "patch":
        patch += 1
    else:
        raise ValueError(f"Unsupported version bump: {bump}")
    return f"v{major}.{minor}.{patch}"


def resolve_tag(tag: str | None, auto_tag: str, metadata_path: Path) -> str | None:
    if tag:
        return tag
    if auto_tag == "none":
        return None

    metadata = load_existing_metadata(metadata_path)
    current_tag = metadata.get("tag")
    if not isinstance(current_tag, str):
        current_tag = None
    return increment_version(parse_version_tag(current_tag), auto_tag)


def validate_model_dir(model_dir: Path) -> Path:
    model_dir = model_dir.resolve()
    required_files = [
        "config.json",
        "modules.json",
        "model.safetensors",
        "tokenizer.json",
        "1_Pooling/config.json",
    ]
    missing = [file_name for file_name in required_files if not (model_dir / file_name).exists()]
    if missing:
        raise FileNotFoundError(
            f"Model directory {model_dir} is missing required files: {', '.join(missing)}"
        )
    return model_dir


def publish_model(
    model_dir: Path,
    repo_id: str,
    repo_type: str = "model",
    revision: str | None = None,
    tag: str | None = None,
    tag_message: str | None = None,
    private: bool | None = None,
    commit_message: str = "Upload fine-tuned sentence-transformers model",
    docker_model_dir: str = "/app/models/news-flow-ru-vectorization-mpnet/final",
) -> PublishedModel:
    from huggingface_hub import HfApi, create_repo

    model_dir = validate_model_dir(model_dir)
    create_repo(repo_id=repo_id, repo_type=repo_type, private=private, exist_ok=True)

    api = HfApi()
    commit_info = api.upload_folder(
        folder_path=str(model_dir),
        repo_id=repo_id,
        repo_type=repo_type,
        revision=revision,
        path_in_repo=".",
        commit_message=commit_message,
    )
    commit_hash = str(commit_info.oid)
    if tag:
        api.create_tag(
            repo_id=repo_id,
            repo_type=repo_type,
            tag=tag,
            tag_message=tag_message,
            revision=commit_hash,
            exist_ok=True,
        )

    return PublishedModel(
        repo_id=repo_id,
        repo_type=repo_type,
        model_revision=commit_hash,
        commit_hash=commit_hash,
        commit_url=str(commit_info.commit_url),
        revision=revision,
        tag=tag,
        version=tag.removeprefix("v") if tag else None,
        local_model_path=str(model_dir),
        docker_model_dir=docker_model_dir,
        model_dir=str(model_dir),
        published_at=datetime.now(UTC).isoformat(),
    )


def write_metadata(metadata_path: Path, published_model: PublishedModel) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with metadata_path.open("w", encoding="utf-8") as file:
        json.dump(asdict(published_model), file, ensure_ascii=False, indent=2)
        file.write("\n")
    print(f"Saved model metadata to: {metadata_path}")


def main() -> None:
    args = parse_args()
    if args.disable_xet:
        os.environ.setdefault("HF_HUB_DISABLE_XET", "1")

    tag = resolve_tag(args.tag, args.auto_tag, args.metadata_path)
    published_model = publish_model(
        model_dir=args.model_dir,
        repo_id=args.repo_id,
        repo_type=args.repo_type,
        revision=args.revision,
        tag=tag,
        tag_message=args.tag_message,
        private=args.private,
        commit_message=args.commit_message,
        docker_model_dir=args.docker_model_dir,
    )
    if not args.skip_metadata:
        write_metadata(args.metadata_path, published_model)

    print(json.dumps(asdict(published_model), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
