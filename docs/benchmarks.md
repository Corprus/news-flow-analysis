# Бенчмарки

Документ объединяет измерения производительности runtime и расхождения
инкрементального режима. Качество модели находится в
[evaluation.md](evaluation.md).

## Окружение GPU

Измерения 20 июня 2026 года:

- Windows 11, Docker Desktop, WSL2;
- NVIDIA GeForce RTX 4070, 12 GiB;
- PyTorch `2.7.1+cu128`, CUDA `12.8`;
- `BAAI/bge-m3`, batch size 16;
- PostgreSQL 16 + pgvector;
- RabbitMQ 3.13.

## Запуск сервисов

| Событие | Время от начала |
|---|---:|
| PostgreSQL готов | 2.23 с |
| RabbitMQ готов | 11.11 с |
| API готов | 15.25 с |
| UI/nginx доступны | ≈ 19.1 с |
| model-service с BGE-M3 готов | 44.79 с |
| холодное demo на 250 статьях завершено | 56.50 с |

Прогретый полный пересчёт 250 публикаций занял 8.77 с end-to-end, или
28.52 публикации/с. Прогретый семантический поиск занимал 0.12–0.17 с
серверного времени.

## Одинаковые 10 000 публикаций

| Вариант | Режим | Время | Throughput |
|---|---|---:|---:|
| Автономный скрипт, включая загрузку модели | `full` | 238.93 с | 41.85/с |
| Docker Compose, модель прогрета | `full` | 226.33 с | 44.18/с |
| Docker Compose, пустая история | `incremental` | 697.71 с | 14.33/с |

Автономная загрузка моделей заняла 13.97 с. Без неё сравнимое время —
224.96 с, практически совпадающее с горячим сервисным `full`.

Замедление `incremental` определяется последовательной обработкой растущей
истории и расширением контекста через `pd.concat`/`np.vstack`, а не самой
сервисной обвязкой.

HTTP import 10 000 публикаций занял 49.47 с. Полное пользовательское время
import + incremental pipeline — 747.47 с.

## Сервисный прогон 50 000 публикаций

Измерение 28 июня 2026 года на том же GPU-стеке: Docker Compose, прогретый
`model-service`, `BAAI/bge-m3`, batch size 16, PostgreSQL 16 + pgvector,
RabbitMQ 3.13. Входной файл подготовлен через
`scripts/build_lenta_import_sample.py --limit 50000`.

| Этап | Результат |
|---|---:|
| HTTP import 50 000 публикаций | 114.42 с |
| `full` pipeline job, 50 000 публикаций | 1237 с |
| Throughput pipeline | 40.42/с |
| Embedding batches | 3125 |
| Embedding phase по логам | 14:27 |
| Рёбер в графе похожести | 4429 |
| Кластеров | 46 412 |

Job `52ff1cef-14b1-45fb-b8ea-e22b410a2655` завершился в статусе `done`:
50 000 requested IDs и 50 000 updated IDs. В БД после прогона стало 51 000
`processed` публикаций и 51 000 строк в `article_pipeline_embeddings`/
`article_pipeline_state`: 1 000 baseline + 50 000 benchmark. Дублирующиеся
50 000 `draft/not_started` строк от предыдущей попытки import, завершившейся
nginx 504 до увеличения proxy timeout, в job не включались.

Пиковые и средние ресурсы за окно job `05:18:49Z`-`05:39:26Z`:

| Ресурс | Peak | Avg |
|---|---:|---:|
| GPU utilization | 100% | 78% |
| VRAM, RTX 4070 | 8.33 GiB | 8.23 GiB |
| model-service CPU | 3.92 cores | 1.03 cores |
| model-service RAM | 4.85 GiB | 2.21 GiB |
| postgres CPU | 0.95 cores | 0.16 cores |
| postgres RAM | 1.03 GiB | 0.55 GiB |
| api CPU | 0.27 cores | 0.05 cores |
| api RAM | 0.38 GiB | 0.37 GiB |
| rabbitmq CPU | 7.47 cores | 1.23 cores |
| rabbitmq RAM | 0.27 GiB | 0.21 GiB |

RabbitMQ очередь во время job держала одно `unacked` сообщение и после
завершения стала пустой (`messages=0`, `unacked=0`). Новых RabbitMQ timeout
в окне 50k job не было; зафиксированный `missed heartbeats from client,
timeout: 60s` относится к более ранней попытке до старта этого job.

Для 50k через HTTP API потребовались изменения лимитов: import, batch publish,
batch delete, novelty labels и `/news-pipeline` принимают до 50 000 элементов.
Также для `/api/` в nginx увеличены `proxy_send_timeout` и
`proxy_read_timeout` до 600 с: без этого одиночный import 50k успевал
закоммититься на backend, но nginx возвращал 504 примерно через 60 с.

Артефакты прогона:

- `data/import/lenta_import_sample_50000.csv`;
- `data/artifacts/service_runtime_benchmark/import_50000_response.json`;
- `data/artifacts/service_runtime_benchmark/service_full_50000.json`;
- `data/artifacts/service_runtime_benchmark/service_full_50000_metrics.json`.

## Сервисный chunked incremental 50 000 публикаций

Измерение 30 июня 2026 года на том же GPU-стеке после разделения runtime на
`model-service-vectorizer-gpu` и `model-service-processor`. Конфигурация:
`PIPELINE_CHUNK_SIZE=5000`, `PIPELINE_AGGREGATE_BATCH_SIZE=1000`,
`PIPELINE_HISTORY_WINDOW_DAYS=30`, `PIPELINE_HISTORY_EXPAND_CLUSTERS=true`,
`PIPELINE_HISTORY_CLUSTER_EXPANSION_MAX_ROWS=20000`.

| Этап | Результат |
|---|---:|
| `incremental_chunked` parent job, 50 000 публикаций | 29:01 |
| `vectorize` child jobs | 10 |
| Окно выполнения `vectorize` | 16:45 |
| `aggregate` child jobs | 50 |
| Окно выполнения `aggregate` | 29:01 |
| Время от первого pipeline job до последнего обновления | 38:48 |
| Итоговый статус публикаций в БД | 50 050 `processed` |

В текущей базе до прогона уже было 50 обработанных публикаций, поэтому итоговый
счётчик `processed` равен 50 050. Для основного bulk job в `news_pipeline_jobs`
зафиксировано 50 000 requested news: 10 `vectorize` jobs по 5 000 публикаций и
50 `aggregate` jobs по 1 000 публикаций. Все jobs завершились в статусе `done`;
старого зависания одной монолитной aggregate job не наблюдалось.

Суммарные Prometheus-метрики processor за время жизни контейнера включали также
предыдущий прогон, поэтому для количества jobs надёжнее использовать
`news_pipeline_jobs`. По текущим stage duration самым дорогим участком остаётся
CPU-часть aggregate:

| Стадия processor | Суммарное время |
|---|---:|
| `cluster_assignment` | 366 с |
| `novelty_prediction` | 335 с |
| `result_assembly` | 203 с |
| `loading_history` | 151 с |

Этот результат является актуальным runtime baseline для большого импорта с
публикацией сразу. Старый `full`-прогон выше остаётся отдельным сравнением:
он считает весь корпус совместно и не эквивалентен production-сценарию
`incremental_chunked`.

## Сервисный streaming incremental 200 000 публикаций

Измерение 1 июля 2026 года на том же Docker/GPU-стеке. Конфигурация:
`PIPELINE_CHUNK_SIZE=5000`, `PIPELINE_AGGREGATE_BATCH_SIZE=5000`,
асинхронный import job с публикацией сразу и streaming-векторизацией по мере
импорта. Входной файл:
`data/import/lenta_import_sample_200000.zip`.

| Этап | Результат |
|---|---:|
| HTTP import job, 200 000 публикаций | 9:56 |
| `incremental_chunked` parent job | 2:30:27 |
| End-to-end от старта import до конца pipeline | 2:30:36 |
| End-to-end throughput | 22.1 публикации/с |
| `vectorize` child jobs | 40 x 5 000 |
| Окно выполнения `vectorize` | 1:23:07 |
| `aggregate` child jobs | 40 x 5 000 |
| Aggregate tail после завершения vectorize | ≈ 1:07:06 |
| Окно `aggregate` jobs в БД | 2:20:41 |
| Итоговый статус публикаций в БД | 200 050 `processed` |


Ресурсы после завершения прогона:

| Контейнер | RAM |
|---|---:|
| api | 1.239 GiB |
| model-service-vectorizer-gpu | 1.7 GiB |
| model-service-processor | 688 MiB |
| postgres | 4.455 GiB |
| ui | 478 MiB |
| rabbitmq | 212 MiB |
| prometheus | 82 MiB |
| grafana | 109 MiB |

Практический вывод: 200k не упирается в лимит API или RabbitMQ. Узкое место
остаётся вычислительным: один GPU-векторизатор и последовательная CPU-часть
`aggregate`. Увеличение `PIPELINE_AGGREGATE_BATCH_SIZE` до 5 000 сократило
количество aggregate jobs с потенциальных 200 до 40, но не отменило
хронологическую последовательность novelty-расчёта.

## CPU

Измерения 22 июня 2026 года на AMD Ryzen 9 5900X и поднаборе 1 000 публикаций:

| Конфигурация | Разбиение | Время | Throughput |
|---|---:|---:|---:|
| 1 CPU-контейнер | 1 × 1 000 | 465.51 с | 2.15/с |
| 4 CPU-контейнера | 4 × 250 | 407.19 с | 2.46/с |

Четыре контейнера дали только 1.14× ускорения из-за oversubscription: 48
PyTorch threads конкурировали за 12 физических ядер.

Тест четырёх workers измеряет суммарную скорость независимых jobs. Он не
сохраняет глобальную кластеризацию между разделами и не ускоряет один `full`
job.

Пробный CPU job на 10 000 публикаций превысил 30-минутный acknowledgement
timeout RabbitMQ и не считается валидным измерением.

## Расхождение incremental и full

Replay выполнен на 3 176 публикациях за март–апрель 2004 года. Первые 14 дней
использовались для начального full, затем поступали недельные батчи.

После добавления строгого объединения кластеров:

| Дней | Строк | Pairwise F1 | Recall | Fragmentation |
|---:|---:|---:|---:|---:|
| 7 | 1 131 | 0.917 | 0.942 | 7.8% |
| 14 | 1 496 | 0.908 | 0.924 | 9.2% |
| 21 | 1 852 | 0.887 | 0.915 | 9.7% |
| 28 | 2 207 | 0.877 | 0.906 | 9.6% |
| 35 | 2 541 | 0.868 | 0.891 | 8.9% |
| 42 | 2 890 | 0.853 | 0.888 | 9.5% |
| 47 | 3 176 | 0.836 | 0.859 | 10.8% |

Согласованность новизны на последней точке — 98.1%. Основная причина полного
пересчёта — накопление ошибок структуры кластеров.

Стартовая эксплуатационная политика: полный reclustering раз в две недели с
мониторингом. Перед production-фиксацией интервал нужно проверить на нескольких
окнах и реалистичных late arrivals.

## Команды

```console
python scripts/benchmark_final_pipeline.py --project-root . --n-rows 10000 --device cuda
python scripts/benchmark_service_pipeline.py --reset-news --timeout 1800
python scripts/benchmark_existing_service_pipeline.py --mode full --limit 10000
python scripts/benchmark_parallel_service_pipeline.py --workers 4 --limit 1000 --timeout 1800
python scripts/benchmark_incremental_pipeline.py --help
```

Результаты сохраняются в `data/artifacts/` и не должны коммититься как обычные
runtime-выводы.

## Ограничения сравнения

- GPU и CPU измерены на разных размерах корпуса;
- четыре CPU jobs не эквивалентны одному глобальному full;
- поисковые времена не включают UI, polling и сеть пользователя;
- результаты зависят от прогрева модели и состояния кеша.
