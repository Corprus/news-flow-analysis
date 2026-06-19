# Структура репозитория

Основное описание проекта, результаты и команды запуска находятся в
[`README.md`](../README.md).

## Код приложения

```text
src/
  api/             внешний FastAPI API
  model_service/   RabbitMQ consumer, векторизация и semantic search
  news/            новости, поисковые запросы и работа с embeddings
  users/           пользователи и bearer-аутентификация
  accounting/      баланс и журнал операций
  db/              PostgreSQL, pgvector и модели хранения
  messaging/       RabbitMQ publisher/consumer
  services/        загрузка и вызов embedding-модели
  model/           экспериментальная clustering/novelty логика
  final_pipeline/  финальный offline/inference pipeline
```

Сервисный контур использует финальный BGE-M3 novelty pipeline. Детали runtime-контракта
описаны в основном README.

## Эксперименты

Ноутбуки находятся в `notebooks/`:

- `01_train_embeddings.ipynb` — ранний эксперимент по дообучению embeddings;
- `02_lenta_event_grouping.ipynb` — первичная событийная группировка;
- `03_data_analysis_and_dataset_preparation.ipynb` — EDA и подготовка данных;
- `04_evaluate_annotations_and_predictions.ipynb` — оценка разметки и предсказаний;
- `05_semantic_baseline_model.ipynb` — baseline clustering и novelty classifier;
- `06_model_improvement_experiments_v3.ipynb` — выбор финального pipeline;
- `07_bronze_annotation_export.ipynb` — подготовка bronze-разметки;
- `08_finetune_bge_m3_embeddings.ipynb` — ablation с дообучением BGE-M3.
- `09_search_and_dedup_benchmark.ipynb` — search benchmark и дедупликация Top-10.

## Инфраструктура

```text
docker-compose.yml       PostgreSQL, RabbitMQ, API, model-service, UI и nginx
docker/                  Dockerfiles и конфигурация сервисов
ui/                      Streamlit-интерфейс
.github/workflows/       Ruff, pytest, Compose validation и ручная сборка images
```

## Скрипты

`scripts/` содержит запуск и benchmark финального BGE-M3 pipeline, а также проверку
runtime model artifacts.

Локальная справка: [`scripts/README.md`](../scripts/README.md).

## Данные и generated artifacts

```text
data/raw/          исходные датасеты
data/prepared/     подготовленные выборки
data/artifacts/    модели, embeddings, разметка и benchmark outputs
data/predictions/  результаты inference и экспериментов
models/            локальные сервисные модели
```

Крупные датасеты, caches, checkpoints и predictions обычно исключены из Git. Исключение —
небольшие runtime-конфиги и явно зафиксированные финальные артефакты, необходимые для
воспроизводимого запуска.

Подробности: [`data/README.md`](../data/README.md) и
[`model_artifacts.md`](model_artifacts.md).

## Документация

Проектные документы собраны в `docs/`:

- [`final_pipeline.md`](final_pipeline.md);
- [`model_improvement.md`](model_improvement.md);
- [`service_stack.md`](service_stack.md);
- [`ci_cd.md`](ci_cd.md);
- [`prototype.md`](prototype.md);
- [`project_context.md`](project_context.md).

README внутри `data/`, `scripts/` и `tests/fixtures/` намеренно остаются рядом с
соответствующими директориями.
