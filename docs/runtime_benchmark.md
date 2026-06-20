# Runtime service benchmark

Измерение выполнено 20 июня 2026 года на чистом demo-запуске:

```powershell
docker compose down -v
docker compose up -d --no-build
```

После запуска не выполнялись ручные рестарты или повторная постановка задач.
У всех шести контейнеров `RestartCount=0`.

## Окружение

- Windows 11, Docker Desktop, WSL2;
- NVIDIA GeForce RTX 4070, 12 GiB VRAM;
- PyTorch `2.7.1+cu128`, CUDA runtime `12.8`;
- embedding model: `BAAI/bge-m3`;
- embedding batch size: `16`;
- локальный Hugging Face cache смонтирован через
  `MODEL_SERVICE_HF_CACHE=E:/MLCache/huggingface`;
- demo corpus: 250 публикаций Lenta.ru;
- PostgreSQL 16 + pgvector;
- RabbitMQ 3.13.

## Время старта сервисов

Готовность определялась по timestamp первого сообщения готовности в логах
контейнера. Общее время отсчитывалось от запуска первого инфраструктурного
контейнера.

| Сервис | Время от старта контейнера | Общее время от начала запуска | Критерий готовности |
|---|---:|---:|---|
| PostgreSQL | 2.23 с | 2.23 с | `database system is ready to accept connections` |
| RabbitMQ | 11.10 с | 11.11 с | `Server startup complete` |
| API | 2.93 с | 15.25 с | FastAPI `Application startup complete` |
| UI | 1.01 с | 19.07 с | Streamlit server ready |
| nginx | 0.24 с | 19.05 с | nginx configuration ready |
| model-service | 26.72 с | 44.79 с | FastAPI `Application startup complete` после загрузки BGE-M3 |

Практически это означает:

- UI и HTTP API доступны примерно через 19 секунд;
- model-service готов принимать pipeline/search jobs примерно через 45 секунд;
- первая demo pipeline job завершает обработку 250 публикаций примерно через
  56.5 секунды от начала запуска стека.

## Обработка demo-очереди

### Холодный demo job

Первая задача создаётся API до полной готовности model-service и ожидает consumer
в RabbitMQ.

| Метрика | Значение |
|---|---:|
| Режим | `incremental`, пустая история |
| Публикаций | 250 |
| Время job по PostgreSQL | 41.266 с |
| Средняя скорость с учётом ожидания холодного model-service | 6.06 публикации/с |
| Время от начала запуска стека до готовых результатов | 56.50 с |

Время job включает ожидание запуска model-service и загрузки BGE-M3. Это
пользовательская latency первого запуска, а не только вычислительное время
pipeline.

### Прогретый полный пересчёт

После загрузки модели была отправлена отдельная `full` job на те же 250
публикаций через `POST /api/v1/news-pipeline`.

| Метрика | Значение |
|---|---:|
| Серверное время по `news_pipeline_jobs` | 8.314 с |
| End-to-end время от HTTP POST до статуса `done` | 8.766 с |
| Throughput end-to-end | 28.52 публикации/с |
| Среднее время на публикацию | 35.1 мс |
| Итоговых кластеров | 173 |

После benchmark все 250 статей остались в статусе `processed`, semantic search
успешно вернул пять кластеров, API и model-service остались healthy.

## Semantic search

Две прогретые search jobs завершились за:

- 0.169 с;
- 0.120 с.

Это серверное время асинхронной задачи по timestamps PostgreSQL. Оно не включает
время UI, клиентский polling и сетевую задержку пользователя.

## Сравнение на одинаковых 10 000 новостей

20 июня 2026 года выполнен отдельный горячий прогон на тех же 10 000 `news_id`,
которые использовались в сохранённом standalone benchmark
`final_predictions_10000.csv`.

Перед замером:

- BGE-M3 была загружена и прогрета на RTX 4070;
- таблицы новостей и pipeline-результатов были очищены;
- пользователи и организации demo остались в базе;
- CSV был восстановлен из `lenta_clean_news.csv` в порядке ID исходного benchmark;
- сервисы во время эксперимента не перезапускались.

| Метрика | Standalone full | Service full | Service incremental |
|---|---:|---:|---:|
| Новостей | 10 000 | 10 000 | 10 000 |
| Модель | загружается в замере | прогрета | прогрета |
| Время pipeline | 238.93 с | 226.33 с | 697.71 с |
| Throughput | 41.85 новости/с | 44.18 новости/с | 14.33 новости/с |
| HTTP import и persistence | — | импорт не выполнялся | 49.47 с |
| Import + pipeline | — | — | 747.47 с |
| Итоговых embeddings/state | CSV/NPZ artifact | 10 000 / 10 000 | 10 000 / 10 000 |

Standalone benchmark включает 13.97 с загрузки моделей. Без этого этапа его
сопоставимое время составляет 224.96 с, что практически совпадает с 226.33 с
горячего service full. Разница 1.37 с находится в пределах естественной
вариативности CUDA-прогона и включает чтение данных из PostgreSQL, RabbitMQ,
обновление статусов и сохранение 10 000 embeddings и pipeline states.

Таким образом, замедление incremental до 697.71 с объясняется прежде всего самим
последовательным алгоритмом назначения новых статей к растущей истории. Оно не
является стоимостью Docker Compose или PostgreSQL persistence.

Контекст не загружается из базы отдельно для каждой новости. В начале job сервис
одним запросом получает обработанную историю и сохранённые embeddings. После этого
incremental pipeline последовательно обрабатывает новые публикации в памяти:

1. фильтрует растущую историю по теме и временному окну;
2. вычисляет similarity с подходящими кандидатами;
3. выбирает или создаёт кластер;
4. добавляет текущую публикацию и embedding в историю следующей итерации.

На batch из 10 000 записей поздние элементы сравниваются с контекстом, содержащим
почти весь batch. Дополнительную стоимость создаёт расширение контекста через
`pd.concat` и `np.vstack` на каждой итерации. Это нормальная семантика
incremental-обработки, но неэффективный способ первичной загрузки большого корпуса.
Для таких загрузок следует использовать `full`; incremental предназначен для
небольших поступающих порций на фоне уже обработанной истории.

Persistence сейчас выполняет по одному upsert для каждого embedding и pipeline
state. Пакетная запись остаётся разумной технической оптимизацией, но результаты
full benchmark не показывают её как значимый end-to-end bottleneck. Чтобы измерить
её точно, нужен отдельный stage timer вокруг `save_result`, а затем A/B-прогон
текущей и batch-реализации.

После обработки:

- все 10 000 статей имели статус `processed`;
- в базе находилось 10 000 embeddings и 10 000 pipeline states;
- job `05ae693a-a8a0-4f13-99e8-02c67e5c4dcc` завершилась со статусом `done`;
- full job `d7a03446-bee5-4039-9092-af447128b5d2` завершилась за 226.33 с;
- semantic search по полному корпусу вернул пять кластеров;
- Ruff завершился без ошибок, pytest: `71 passed`.

## Воспроизведение

Чистый demo startup:

```powershell
docker compose down -v
docker compose up -d --no-build
```

Проверка пользователей, организаций и semantic search:

```powershell
python scripts/demo_smoke_test.py
```

Offline benchmark на 10 000 строк:

```powershell
python scripts/benchmark_final_pipeline.py `
  --project-root . `
  --n-rows 10000 `
  --device cuda
```

Service benchmark на том же наборе ID:

```powershell
python scripts/benchmark_service_pipeline.py --reset-news --timeout 1800
```

Горячий `full` по уже загруженному корпусу:

```powershell
python scripts/benchmark_existing_service_pipeline.py --mode full --limit 10000
```

Команда удаляет только новости, поисковые запросы и pipeline jobs. Пользователи,
организации и настройки demo сохраняются. Для импорта полнотекстового корпуса
nginx и API допускают файл до 128 MiB; число строк по-прежнему ограничено 10 000.
