# Semantic News Novelty

## Актуальная runtime-архитектура

Финальный BGE-M3 clustering/novelty pipeline встроен в отдельный
`model-service`.

- RabbitMQ job: `{"news_ids": ["uuid", ...], "mode": "incremental|full"}`;
- API: `POST /api/v1/news-pipeline`;
- embeddings: `BAAI/bge-m3`, `vector(1024)` в PostgreSQL/pgvector;
- pipeline state: cluster assignment, novelty, provenance и версии моделей
  хранятся в `article_pipeline_state`;
- пользовательские новости создаются как приватные drafts и попадают в общий
  корпус только после явной публикации;
- incremental mode читает историю и embeddings из PostgreSQL;
- semantic search выполняется по всем `public/processed` новостям оператором
  cosine distance pgvector.

Подробности запуска и контракта: [`docs/service_stack.md`](docs/service_stack.md).

Модуль постобработки уже собранного новостного потока для отраслевого desk research.

Проект группирует публикации в инфоповоды, находит повторы и близкие пересказы, выделяет
значимые обновления и поддерживает семантический поиск. Основной пользователь — аналитик
малого или среднего исследовательского агентства, который готовит B2B-обзоры рынков и
тематические подборки.

Проект не является full-cycle системой мониторинга источников и не заменяет эксперта.
Его задача — превратить входящий поток клиента в рабочую структуру, с которой аналитик
может быстрее перейти от ручной вычитки к анализу.

## Текущее состояние

В репозитории есть два связанных, но технически отдельных контура.

### 1. Финальный pipeline кластеризации и новизны

Пакеты `src/model/` и `src/final_pipeline/` реализуют полный offline/inference pipeline:

1. подготовка и нормализация новостей;
2. получение embeddings через `BAAI/bge-m3`;
3. строгая графовая кластеризация по семантической и временной близости;
4. консервативный второй проход `best-candidate attach`;
5. классификация новости как `significant`, `minor` или `duplicate`;
6. маркировка неуверенных случаев через `needs_review`.

Финальная конфигурация выбрана в
[`notebooks/06_model_improvement_experiments_v3.ipynb`](notebooks/06_model_improvement_experiments_v3.ipynb):

- embeddings: `BAAI/bge-m3`;
- базовая кластеризация: `exp_00b`, threshold `0.82`, окно `14` дней;
- второй проход: `exp10_src2_sim0.75_days7_m0.03_tj0.15_num1`;
- novelty classifier: `exp_10a_current_model_on_exp10_clustering`;
- novelty threshold: `0.42`;
- duplicate threshold: `0.90`;
- review margin: `0.10`.

Первый элемент кластера не передаётся novelty classifier: он является seed кластера
и получает `significant` по детерминированному правилу.

Runtime-артефакты:

```text
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

### 2. Сервис семантического поиска

Docker Compose поднимает:

- `nginx` — единая точка входа;
- `api` — FastAPI для пользователей, новостей, поиска и учёта операций;
- `model-service` — загрузка embedding-модели и обработка RabbitMQ jobs;
- `rabbitmq` — очередь задач векторизации;
- `postgres` — PostgreSQL 16 с `pgvector`;
- `ui` — Streamlit-интерфейс.

Итоговая embedding-модель проекта — стандартная `BAAI/bge-m3`, без дополнительного
дообучения. Финальный BGE-M3 pipeline пока запускается отдельным скриптом и ещё не
встроен в асинхронный model-service. Существующая сервисная реализация векторизации
должна быть приведена к тому же runtime-контракту.

## Результаты

Параметры `exp_10` подбирались по silver weak-positive сигналу без использования
golden-разметки. После фиксации конфигурации итоговое качество оценивалось на golden как
на отложенной экспертной validation/holdout-выборке: 121 публикация для кластеризации и
87 строк с доступной novelty-разметкой.

| Метрика | Целевой уровень | Baseline `exp_00b` | Финальный `exp_10a` | Статус |
|---|---:|---:|---:|---|
| Pairwise precision кластеризации | диагностическая | 1.0000 | **0.9811** | — |
| Pairwise recall кластеризации | диагностическая | 0.6257 | **0.8342** | — |
| Pairwise F1 кластеризации | ≥ 0.75–0.80 | 0.7697 | **0.9017** | достигнуто |
| False merge rate | ≤ 10–15% | 0.00% | **1.89%** | достигнуто |
| Precision значимых обновлений | диагностическая | 0.8471 | **0.8471** | — |
| Recall значимых обновлений | ≥ 0.80 | 0.9863 | **0.9863** | достигнуто |
| F1 значимых обновлений | ≥ 0.75 | 0.9114 | **0.9114** | достигнуто |

Финальный вариант заметно уменьшает фрагментацию сюжетов. Цена прироста recall —
3 ошибочно объединённые пары из 159 предсказанных same-story пар.

Novelty-метрики включают детерминированное правило `cluster seed → significant`.
Поэтому они одинаковы для baseline и `exp_10` при одной модели: улучшение `exp_10`
относится к кластеризации, а не к novelty classifier.

Результаты нельзя считать production-гарантией: golden-набор небольшой, а продуктовая
проверка на 300–500 публикациях клиента ещё не проведена.

### Search и deduplication smoke-test

[`notebooks/09_search_and_dedup_benchmark.ipynb`](notebooks/09_search_and_dedup_benchmark.ipynb)
проверяет поисковые критерии презентации на 119 синтетических запросах из заголовков
golden-публикаций. Релевантность и повторы оцениваются по экспертным кластерам, а
дедупликация использует предсказанные кластеры `exp_10a`.

| Вариант | MRR@10 | Цель MRR | Доля повторов Top-10 | Снижение к keyword | Цель снижения | Статус |
|---|---:|---:|---:|---:|---:|---|
| TF-IDF keyword baseline | 0.9067 | baseline | 46.64% | — | baseline | — |
| BGE-M3 без дедупликации | 0.9926 | > 0.9067 | 51.51% | −10.45% | ≥ 30% | не достигнуто по dedup |
| BGE-M3 + `exp_10a` cluster collapse | **0.9944** | > 0.9067 | **11.60%** | **75.14%** | ≥ 30% | достигнуто |

Таким образом, технические критерии `MRR@10 > keyword baseline` и снижение повторов
минимум на 30% выполнены. При этом semantic ranking сам по себе не снижает повторы:
эффект обеспечивает отдельный cluster-collapse этап.

Это smoke-test на малом golden-корпусе, где запросом служит заголовок документа. Для
продуктового вывода нужны реальные запросы аналитиков и независимая поисковая разметка.

## Статус минимальных требований MVP

Все минимальные технические требования, зафиксированные для текущего MVP, достигнуты.

| Требование | Минимальный порог | Получено | Статус |
|---|---:|---:|---|
| Качество группировки инфоповодов | Pairwise F1 ≥ 0.75–0.80 | **0.9017** | достигнуто |
| Ошибочная склейка разных сюжетов | False merge ≤ 10–15% | **1.89%** | достигнуто |
| F1 значимых обновлений | F1 ≥ 0.75 | **0.9114** | достигнуто |
| Полнота значимых обновлений | Recall ≥ 0.80 | **0.9863** | достигнуто |
| Семантический поиск | MRR@10 выше keyword baseline | **0.9944 против 0.9067** | достигнуто |
| Снижение повторов в Top-10 | не менее 30% к baseline | **75.14%** | достигнуто |

Метрики clustering и novelty рассчитаны на golden-наборе из 121 публикации; novelty
разметка доступна для 87 строк. Search/deduplication smoke-test рассчитан на 119
синтетических запросах из заголовков этого же golden-набора.

Это означает, что технический критерий завершения MVP выполнен. Следующий этап — не
дополнительная оптимизация под текущий golden-набор, а внешняя проверка: 300–500
публикаций из потока клиента, реальные поисковые запросы и независимая экспертная
разметка.

## Быстрый запуск сервисного стека

Требования:

- Docker с Compose;
- доступ к Hugging Face при первом запуске remote-модели;
- свободные порты `80` и `15672` либо их переопределение в `.env`.

Создайте локальную конфигурацию:

```powershell
Copy-Item .env.example .env
```

Обязательно замените секреты в `.env`:

```text
POSTGRES_PASSWORD
RABBITMQ_PASSWORD
PASSWORD_HASH_SECRET
ACCESS_TOKEN_SECRET
```

Запустите стек:

```powershell
docker compose up --build
```

После старта:

- UI: <http://localhost/>
- API/OpenAPI: <http://localhost/api/docs>
- RabbitMQ management: <http://localhost:15672/>
- API health: <http://localhost/api/health>

Простейший асинхронный запуск pipeline для уже сохранённой статьи:

```powershell
$job = Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost/api/v1/news-pipeline `
  -ContentType application/json `
  -Body '{"news_ids":["ARTICLE_UUID"],"mode":"incremental"}'

Invoke-RestMethod "http://localhost/api/v1/news-pipeline/$($job.job_id)"
```

Полные пользовательские сценарии включают регистрацию, bearer-аутентификацию,
пополнение баланса администратором, добавление новостей и семантический поиск.

## Запуск финального novelty pipeline

Установите зависимости:

```powershell
python -m pip install -r requirements_model_improvement.txt
```

Запуск из корня проекта:

```powershell
python scripts/run_final_pipeline.py `
  --project-root . `
  --input data/prepared/lenta_clean_news.csv `
  --output data/predictions/final_pipeline_predictions.csv `
  --embeddings-cache data/artifacts/embeddings/final_pipeline_bge_m3.npz `
  --device cuda
```

Для CPU замените `--device cuda` на `--device cpu`. Параметры `--model` и `--config`
необязательны: по умолчанию используются финальные артефакты из
`data/artifacts/models/final_exp10/`.

Ожидаемая входная схема приводится к полям:

```text
news_id, published_at, topic, title, text
```

Основные выходные поля:

```text
cluster_id
novelty_label
p_significant
needs_review
comment
max_prev_similarity
```

`novelty_label` принимает значения `significant`, `minor` или `duplicate`. Первый
элемент каждого нового кластера всегда получает `significant` и `p_significant=1.0`
как seed кластера; классификатор для него не вызывается.

## Производительность

Сохранённый benchmark на 10 000 публикаций:

- окружение: Windows, Python 3.11, CUDA;
- общее время: `238.9` с;
- скорость: `41.9` публикации/с;
- embeddings: `191.8` с;
- novelty classification: `24.9` с;
- кластеризация: `1.1` с.

Основное узкое место — расчёт BGE-M3 embeddings. Повторные запуски следует выполнять с
id-aware `.npz` cache.

Запуск benchmark:

```powershell
python scripts/benchmark_final_pipeline.py `
  --project-root . `
  --n-rows 10000 `
  --device cuda
```

Результаты сохраняются в `data/artifacts/final_pipeline_benchmark/`.

## Данные и модели

Полные датасеты и крупные generated artifacts не должны храниться в Git.

Основные локальные пути:

```text
data/raw/lenta.csv
data/prepared/lenta_clean_news.csv
data/artifacts/
data/predictions/
models/
```

Исследовательский pipeline использует Lenta.ru, golden ручную разметку и silver-разметку,
подготовленную с помощью LLM. `BAAI/bge-m3` загружается через `sentence-transformers`
или используется из локального Hugging Face cache.

## Структура репозитория

```text
src/
  api/                 внешний FastAPI API
  model_service/       worker векторизации и поиска
  news/                новости, embeddings и semantic search
  users/               пользователи и авторизация
  accounting/          баланс и операции
  model/               экспериментальная ML-логика
  final_pipeline/      финальный clustering + novelty runtime
  db/                  PostgreSQL и pgvector
  messaging/           RabbitMQ
notebooks/             EDA, baseline, evaluation и improvement experiments
scripts/               inference, benchmark и вспомогательные model scripts
configs/               runtime-конфигурация
docs/                  продуктовая и техническая документация
tests/                 автоматические тесты
```

Ключевые ноутбуки:

- [`03_data_analysis_and_dataset_preparation.ipynb`](notebooks/03_data_analysis_and_dataset_preparation.ipynb) —
  EDA и подготовка данных;
- [`04_evaluate_annotations_and_predictions.ipynb`](notebooks/04_evaluate_annotations_and_predictions.ipynb) —
  единая оценка разметки и предсказаний;
- [`05_semantic_baseline_model.ipynb`](notebooks/05_semantic_baseline_model.ipynb) —
  baseline кластеризации и novelty model;
- [`06_model_improvement_experiments_v3.ipynb`](notebooks/06_model_improvement_experiments_v3.ipynb) —
  выбор финальной конфигурации;
- [`07_bronze_annotation_export.ipynb`](notebooks/07_bronze_annotation_export.ipynb) —
  подготовка bronze-разметки;
- [`08_finetune_bge_m3_embeddings.ipynb`](notebooks/08_finetune_bge_m3_embeddings.ipynb) —
  отрицательный эксперимент с дообучением BGE-M3, не вошедший в финальную конфигурацию;
- [`09_search_and_dedup_benchmark.ipynb`](notebooks/09_search_and_dedup_benchmark.ipynb) —
  TF-IDF/BGE-M3 search benchmark и оценка повторов в Top-10.

## Проверки

```powershell
python -m pytest
ruff check .
docker compose config --quiet
```

GitHub Actions выполняет Ruff, pytest и проверку Docker Compose. Сборка Docker images
запускается вручную через `workflow_dispatch`.

## Ограничения и ближайшие задачи

- встроить финальный BGE-M3 clustering/novelty pipeline в model-service;
- провести пилот на 300–500 публикациях из реального потока клиента;
- повторить search/dedup benchmark на реальных запросах аналитиков;
- проверить качество на новых темах, источниках и временных периодах;
- добавить объяснение, какой новый факт сделал публикацию значимой;

`MRR@10` относится только к качеству ранжирования семантического поиска. Для оценки
кластеризации и смысловой новизны используются Pairwise F1, false merge rate, precision,
recall и F1 значимых обновлений.

## Дополнительная документация

- [Финальный pipeline](docs/final_pipeline.md)
- [Эксперименты по улучшению модели](docs/model_improvement.md)
- [Сервисный стек](docs/service_stack.md)
- [CI/CD](docs/ci_cd.md)
- [Прототип и продуктовые гипотезы](docs/prototype.md)
- [Данные](data/README.md)
- [Скрипты](scripts/README.md)
- [Конкурентный анализ](docs/Конкуретный%20анализ%20SNN.xlsx)
- [Презентация benchmarking](docs/Benchmarking_SNN.pptx)

## Лицензия

[MIT](LICENSE)
