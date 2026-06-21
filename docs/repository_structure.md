# Структура репозитория

Основное описание проекта, результаты и команды запуска находятся в
[`README.md`](../README.md).

## Код приложения

```text
src/
  api/             внешний FastAPI API
  model_service/   RabbitMQ консюмер, векторизация и семантический поиск
  news/            новости, поисковые запросы и работа с мбеддингами
  users/           пользователи и bearer-аутентификация
  accounting/      баланс и журнал операций
  db/              PostgreSQL, pgvector и модели хранения
  messaging/       издатель и консюмер сообщений RabbitMQ
  services/        загрузка и вызов модели эмбеддингов
  model/           экспериментальная логика кластеризации и оценки новизны
  final_pipeline/  финальный пайплайн
```

Сервисный контур использует финальный пайплайн оценки новизны на BGE-M3. Детали рабочего контракта описаны в основном README.

## Эксперименты

Ноутбуки находятся в `notebooks/`:

- `01_train_embeddings.ipynb` — ранний эксперимент по дообучению эмбеддингов;
- `02_lenta_event_grouping.ipynb` — первичная событийная группировка;
- `03_data_analysis_and_dataset_preparation.ipynb` — EDA и подготовка данных;
- `04_evaluate_annotations_and_predictions.ipynb` — оценка разметки и предсказаний;
- `05_semantic_baseline_model.ipynb` — базовая кластеризация и классификатор новизны;
- `06_model_improvement_experiments_v3.ipynb` — выбор финального конвейера;
- `07_bronze_annotation_export.ipynb` — подготовка bronze-разметки;
- `08_finetune_bge_m3_embeddings.ipynb` — ablation с дообучением BGE-M3.
- `09_search_and_dedup_benchmark.ipynb` — тест поиска и дедупликация Top-10.

## Инфраструктура

```text
docker-compose.yml       PostgreSQL, RabbitMQ, API, model-service, UI и nginx
docker/                  Dockerfiles и конфигурация сервисов
ui/                      Streamlit-интерфейс
.github/workflows/       Ruff, pytest, Compose validation и ручная сборка images
```

## Скрипты

`scripts/` содержит запуск и тест производительности финального конвейера BGE-M3,
а также проверку рабочих артефактов модели.

Локальная справка: [`scripts/README.md`](../scripts/README.md).

## Данные и generated artifacts

```text
data/raw/          исходные датасеты
data/prepared/     подготовленные выборки
data/artifacts/    модели, эмбеддинги, разметка и результаты тестов производительности
data/predictions/  результаты inference и экспериментов
models/            локальные сервисные модели
```

Крупные датасеты, кеши, контрольные точки и предсказания обычно исключены из Git. Исключение —
небольшие рабочие конфигурации и явно зафиксированные финальные артефакты, необходимые для
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
