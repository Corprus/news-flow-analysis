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

## Hugging Face Hub

Ручная публикация финальной модели:

```bash
python scripts/publish_model.py \
  --repo-id Korprus/news-flow-ru-vectorization-mpnet \
  --model-dir models/news-flow-ru-vectorization-mpnet/final
```

После upload скрипт сохраняет metadata в:

```text
configs/model_registry/latest_model.json
```

В metadata фиксируется `commit_hash`. Для разработки можно использовать `main`, но для
release/Docker-сборки лучше использовать `commit_hash` или tag.

По умолчанию скрипт сам создаёт следующий patch tag на основе текущего registry-файла
(`v0.1.0` -> `v0.1.1`). Можно явно выбрать стратегию:

```bash
python scripts/publish_model.py --auto-tag minor
python scripts/publish_model.py --auto-tag major
python scripts/publish_model.py --tag v1.0.0
python scripts/publish_model.py --auto-tag none
```

Опциональный upload сразу после обучения:

```bash
python scripts/train_embeddings.py \
  --push-to-hub \
  --hub-model-id Korprus/news-flow-ru-vectorization-mpnet
```

Загрузка модели из Hugging Face Hub в локальную директорию:

```bash
python scripts/download_model.py \
  --repo-id Korprus/news-flow-ru-vectorization-mpnet \
  --revision 789ab95331d9abc4f5f23d1e3d5d24bb8af28086 \
  --output-dir models/news-flow-ru-vectorization-mpnet/final
```

Загрузка версии из model registry metadata:

```bash
python scripts/download_model.py \
  --metadata-path configs/model_registry/latest_model.json \
  --output-dir models/news-flow-ru-vectorization-mpnet/final
```

Для private repo нужен `HF_TOKEN` или предварительный `hf auth login`.

## Windows workflow wrappers

Локальный тестовый режим: обучить модель, собрать сервис и запустить compose stack с
локальной моделью, примонтированной в контейнер через параметры единственного
`docker-compose.yml`:

```bat
scripts\windows\train_local_model.cmd
```

Аргументы после имени `.cmd` передаются в `scripts/train_embeddings.py`, например:

```bat
scripts\windows\train_local_model.cmd --skip-evaluation
```

Hugging Face режим: опубликовать текущую локальную модель, обновить
`configs/model_registry/latest_model.json`, собрать `model-service` image с моделью,
скачанной из Hugging Face Hub, и запустить stack:

```bat
scripts\windows\publish_hf_model.cmd
```

Аргументы после имени `.cmd` передаются в `scripts/publish_model.py`, например:

```bat
scripts\windows\publish_hf_model.cmd --auto-tag minor
```

Если модель private, перед запуском задайте `HF_TOKEN` в текущей консоли или положите токен
в локальный файл `token.local`. Этот файл исключён из Git.
