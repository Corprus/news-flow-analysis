"""Дообучение sentence-transformers модели на русскоязычных новостных парафразах."""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any


DEFAULT_DATASET_NAME = "merionum/ru_paraphraser"
DEFAULT_BASE_MODEL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"


def find_project_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for path in [current, *current.parents]:
        if (path / ".git").exists() and (path / "README.md").exists():
            return path
    raise RuntimeError("Project root not found")


def parse_args() -> argparse.Namespace:
    project_root = find_project_root()

    parser = argparse.ArgumentParser(
        description="Fine-tune sentence-transformers model on ru_paraphraser."
    )
    parser.add_argument("--dataset-name", default=DEFAULT_DATASET_NAME)
    parser.add_argument(
        "--dataset-cache-path",
        type=Path,
        default=project_root / "data" / "raw" / "ru_paraphraser_hf",
    )
    parser.add_argument(
        "--prepared-pairs-path",
        type=Path,
        default=project_root / "data" / "prepared" / "ru_paraphraser_positive_pairs.parquet",
    )
    parser.add_argument("--base-model", default=DEFAULT_BASE_MODEL)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root / "models" / "ru-news-mpnet-paraphrase-mnrl",
    )
    parser.add_argument("--epochs", type=float, default=1)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--eval-batch-size", type=int, default=16)
    parser.add_argument("--learning-rate", type=float, default=2e-5)
    parser.add_argument("--warmup-ratio", type=float, default=0.1)
    parser.add_argument("--eval-size", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--fp16", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--bf16", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--save-steps", type=int, default=500)
    parser.add_argument("--save-total-limit", type=int, default=2)
    parser.add_argument("--logging-steps", type=int, default=50)
    parser.add_argument("--negative-sample-size", type=int, default=2000)
    parser.add_argument("--skip-evaluation", action="store_true")
    return parser.parse_args()


def load_or_download_dataset(dataset_name: str, cache_path: Path) -> DatasetDict:
    from datasets import DatasetDict, load_dataset, load_from_disk

    # Храним локальную копию датасета Hugging Face, чтобы повторные запуски
    # обучения не зависели от сети после первой загрузки.
    if cache_path.exists():
        dataset = load_from_disk(str(cache_path))
        print(f"Loaded dataset from disk: {cache_path}")
    else:
        dataset = load_dataset(dataset_name)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        dataset.save_to_disk(str(cache_path))
        print(f"Downloaded dataset and saved to: {cache_path}")

    if not isinstance(dataset, DatasetDict):
        raise TypeError(f"Expected DatasetDict, got {type(dataset)!r}")
    return dataset


def detect_columns(df: Any) -> tuple[str, str, str]:
    cols = set(df.columns)
    # У датасетов с парафразами могут отличаться имена колонок в зависимости
    # от источника/версии. Определяем смысловые роли колонок, а не жёсткие имена.
    possible_text1 = ["text_1", "text1", "sentence1", "sentence_1", "s1"]
    possible_text2 = ["text_2", "text2", "sentence2", "sentence_2", "s2"]
    possible_label = ["label", "class", "target"]

    text1_col = next((col for col in possible_text1 if col in cols), None)
    text2_col = next((col for col in possible_text2 if col in cols), None)
    label_col = next((col for col in possible_label if col in cols), None)

    if text1_col is None or text2_col is None or label_col is None:
        raise ValueError(f"Could not detect columns. Available columns: {df.columns.tolist()}")

    return text1_col, text2_col, label_col


def prepare_positive_pairs(
    train_df: Any,
    text1_col: str,
    text2_col: str,
    label_col: str,
    prepared_pairs_path: Path,
) -> Any:
    # MultipleNegativesRankingLoss ожидает positive pairs. Остальные примеры
    # в batch используются как implicit negatives, поэтому явные negative rows
    # в trainer не передаются.
    positive_df = train_df[train_df[label_col] == 1].copy()
    positive_df = positive_df[[text1_col, text2_col, label_col]].dropna()
    positive_df[text1_col] = positive_df[text1_col].astype(str).str.strip()
    positive_df[text2_col] = positive_df[text2_col].astype(str).str.strip()
    positive_df = positive_df[
        (positive_df[text1_col].str.len() > 0) & (positive_df[text2_col].str.len() > 0)
    ].reset_index(drop=True)

    prepared_pairs_path.parent.mkdir(parents=True, exist_ok=True)
    positive_df.to_parquet(prepared_pairs_path, index=False)
    print(f"Saved positive pairs to: {prepared_pairs_path}")
    print(f"Positive pairs: {len(positive_df)}")

    return positive_df


def to_training_dataset(df: Any, text1_col: str, text2_col: str) -> Any:
    from datasets import Dataset

    # SentenceTransformerTrainer использует первые две текстовые колонки как
    # парные входы для ranking loss.
    return Dataset.from_dict(
        {
            "anchor": df[text1_col].astype(str).tolist(),
            "positive": df[text2_col].astype(str).tolist(),
        }
    )


def fine_tune_model(
    train_dataset: Any,
    args: argparse.Namespace,
) -> Path:
    from sentence_transformers import SentenceTransformer
    from sentence_transformers.losses import MultipleNegativesRankingLoss
    from sentence_transformers.trainer import SentenceTransformerTrainer
    from sentence_transformers.training_args import SentenceTransformerTrainingArguments

    model = SentenceTransformer(args.base_model)
    loss = MultipleNegativesRankingLoss(model)

    # Trainer сохраняет промежуточные checkpoints в output_dir. Компактная
    # модель для последующего использования сохраняется отдельно в output_dir/final.
    training_args = SentenceTransformerTrainingArguments(
        output_dir=str(args.output_dir),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.eval_batch_size,
        learning_rate=args.learning_rate,
        warmup_ratio=args.warmup_ratio,
        fp16=args.fp16,
        bf16=args.bf16,
        logging_steps=args.logging_steps,
        save_steps=args.save_steps,
        save_total_limit=args.save_total_limit,
        eval_strategy="no",
        report_to="none",
    )

    trainer = SentenceTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        loss=loss,
    )
    trainer.train()

    final_model_path = args.output_dir / "final"
    model.save(str(final_model_path))
    print(f"Saved model to: {final_model_path}")
    return final_model_path


def build_retrieval_eval_data(
    train_df: Any,
    eval_pairs_df: Any,
    text1_col: str,
    text2_col: str,
    label_col: str,
    negative_sample_size: int,
    seed: int,
) -> tuple[list[str], list[str], list[int]]:
    # Собираем retrieval-задачу как в notebook evaluation: у каждого query есть
    # один известный positive candidate и distractors из negative pairs.
    queries = eval_pairs_df[text1_col].astype(str).tolist()
    positives = eval_pairs_df[text2_col].astype(str).tolist()
    negatives_pool = train_df[train_df[label_col] == -1][text2_col].astype(str).dropna().tolist()

    random.seed(seed)
    extra_negatives = random.sample(negatives_pool, min(negative_sample_size, len(negatives_pool)))

    seen: set[str] = set()
    candidates: list[str] = []
    for text in [*positives, *extra_negatives]:
        if text not in seen:
            candidates.append(text)
            seen.add(text)

    candidate_index = {text: index for index, text in enumerate(candidates)}
    target_indices = [candidate_index[text] for text in positives]

    print(f"Retrieval eval queries: {len(queries)}")
    print(f"Retrieval eval candidates: {len(candidates)}")
    return queries, candidates, target_indices


def evaluate_retrieval(
    model: Any,
    queries: list[str],
    candidates: list[str],
    target_indices: list[int],
    top_k: int = 10,
    batch_size: int = 64,
) -> dict[str, float]:
    import faiss
    import numpy as np

    # Embeddings нормализованы, поэтому inner product в FAISS эквивалентен
    # cosine similarity.
    query_emb = model.encode(
        queries,
        batch_size=batch_size,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=True,
    ).astype("float32")
    candidate_emb = model.encode(
        candidates,
        batch_size=batch_size,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=True,
    ).astype("float32")

    index = faiss.IndexFlatIP(candidate_emb.shape[1])
    index.add(candidate_emb)
    _, indices = index.search(query_emb, top_k)

    recall_at_1 = 0
    recall_at_5 = 0
    recall_at_10 = 0
    mrr = 0.0

    for row_index, retrieved in enumerate(indices):
        target = target_indices[row_index]
        if target == retrieved[0]:
            recall_at_1 += 1
        if target in retrieved[:5]:
            recall_at_5 += 1
        if target in retrieved[:10]:
            recall_at_10 += 1

        rank_positions = np.where(retrieved == target)[0]
        if len(rank_positions) > 0:
            mrr += 1.0 / (int(rank_positions[0]) + 1)

    n = len(queries)
    return {
        "Recall@1": recall_at_1 / n,
        "Recall@5": recall_at_5 / n,
        "Recall@10": recall_at_10 / n,
        "MRR@10": mrr / n,
    }


def write_metrics(metrics_path: Path, metrics: dict[str, Any]) -> None:
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", encoding="utf-8") as file:
        json.dump(metrics, file, ensure_ascii=False, indent=2)
    print(f"Saved metrics to: {metrics_path}")


def main() -> None:
    from sentence_transformers import SentenceTransformer
    from sklearn.model_selection import train_test_split

    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    dataset = load_or_download_dataset(args.dataset_name, args.dataset_cache_path)
    train_df = dataset["train"].to_pandas()
    text1_col, text2_col, label_col = detect_columns(train_df)
    train_df[label_col] = train_df[label_col].astype(int)

    # Скрипт повторяет notebooks/01_train_embeddings.ipynb, но без
    # исследовательских ячеек: здесь оставлен только воспроизводимый pipeline.
    positive_df = prepare_positive_pairs(
        train_df=train_df,
        text1_col=text1_col,
        text2_col=text2_col,
        label_col=label_col,
        prepared_pairs_path=args.prepared_pairs_path,
    )
    train_pairs_df, eval_pairs_df = train_test_split(
        positive_df,
        test_size=args.eval_size,
        random_state=args.seed,
        shuffle=True,
    )
    train_dataset = to_training_dataset(train_pairs_df, text1_col, text2_col)

    final_model_path = fine_tune_model(train_dataset, args)

    if args.skip_evaluation:
        return

    queries, candidates, target_indices = build_retrieval_eval_data(
        train_df=train_df,
        eval_pairs_df=eval_pairs_df,
        text1_col=text1_col,
        text2_col=text2_col,
        label_col=label_col,
        negative_sample_size=args.negative_sample_size,
        seed=args.seed,
    )
    base_model = SentenceTransformer(args.base_model)
    fine_tuned_model = SentenceTransformer(str(final_model_path))
    metrics = {
        "base": evaluate_retrieval(base_model, queries, candidates, target_indices),
        "fine_tuned": evaluate_retrieval(
            fine_tuned_model,
            queries,
            candidates,
            target_indices,
        ),
    }
    print(metrics)
    write_metrics(args.output_dir / "metrics.json", metrics)


if __name__ == "__main__":
    main()
