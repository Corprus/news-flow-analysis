# Скрипты

## Дообучение embedding-модели

Скрипт:

```text
scripts/train_embeddings.py
```

Скрипт вынесен из экспериментального ноутбука:

```text
experiments/01_train_embeddings.ipynb
```

Ноутбук можно использовать для пошагового анализа и проверки идей. Скрипт предназначен для воспроизводимого запуска обучения.

Базовый запуск:

```bash
python scripts/train_embeddings.py
```

Запуск без retrieval-оценки после обучения:

```bash
python scripts/train_embeddings.py --skip-evaluation
```

Основные параметры:

- `--dataset-name` — датасет Hugging Face, по умолчанию `merionum/ru_paraphraser`.
- `--dataset-cache-path` — локальный путь для кэша датасета.
- `--prepared-pairs-path` — путь для сохранения подготовленных positive pairs.
- `--base-model` — базовая `sentence-transformers` модель.
- `--output-dir` — директория для checkpoints и финальной модели.
- `--epochs` — количество эпох.
- `--batch-size` — batch size для обучения.
- `--no-fp16` — отключить fp16, если обучение запускается на CPU или GPU без поддержки fp16.

По умолчанию финальная модель сохраняется в:

```text
models/news-flow-ru-vectorization-mpnet/final
```

Директория `models/` не хранится в Git.
