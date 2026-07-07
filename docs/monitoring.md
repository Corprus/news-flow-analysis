# Мониторинг

Docker Compose включает минимальный стек мониторинга для демонстрации и локальной
диагностики:

- `grafana` — четыре готовых дашборда: полный и компактный обзор на английском
  и русском языке;
- `prometheus` — сбор и хранение временных рядов;
- `metrics-exporter` — CPU и RAM контейнеров через Docker API;
- встроенный `rabbitmq_prometheus` — состояние очередей RabbitMQ;
- `/metrics` у `api` — импорт новостей, импортные задания и DB-backed счётчики;
- `/metrics` у `model-service-vectorizer-*` и `model-service-processor` — pipeline,
  vectorize/aggregate stages, throughput и ошибки;
- NVML в GPU-воркере — загрузка GPU, VRAM и температура.

## Запуск

Задайте пароль администратора Grafana в `.env`:

```text
GRAFANA_ADMIN_PASSWORD=change_me
```

Запустите или пересоздайте стек:

```console
docker compose up --build -d
```

Интерфейсы:

- Grafana: <http://localhost:3000/>
- Prometheus: <http://localhost:9090/>

Порты можно изменить через `GRAFANA_PORT` и `PROMETHEUS_PORT`.

## Дашборды

Grafana автоматически подхватывает JSON-файлы из `docker/grafana/dashboards`.
В поставке есть четыре дашборда.

| Дашборд | Файл | Назначение |
|---|---|---|
| `Semantic News Novelty - Monitoring` | `news-flow-overview.json` | полный обзор на английском |
| `Semantic News Novelty - Мониторинг` | `news-flow-overview-ru.json` | полный обзор на русском |
| `Semantic News Novelty - Compact Overview` | `news-flow-compact.json` | короткий экран для демонстрации на английском |
| `Semantic News Novelty - Краткий обзор` | `news-flow-compact-ru.json` | короткий экран для демонстрации на русском |

Полный обзор сделан для пользователя, а не как полный набор внутренних
debug-графиков. Панели сгруппированы в четыре секции.

| Секция | Что показывает | Как читать |
|---|---|---|
| `1. System status` | обработанные статьи, backlog pipeline, очередь RabbitMQ и ошибки | быстрый ответ: система свободна, занята или сломалась |
| `2. Current import and pipeline work` | прогресс активного этапа, строки импорта, статьи по стадиям и глубина очереди | что прямо сейчас делает система |
| `3. Throughput and duration` | скорость последней job, p95 длительности, batch duration, vectorization chunks и aggregate stages | насколько быстро работает обработка |
| `4. Runtime resources` | CPU, RAM, GPU utilization, GPU memory и температура | хватает ли ресурсов Docker/GPU |

Основной нормальный сценарий после завершения обработки:

- `Pipeline backlog` возвращается к нулю;
- `RabbitMQ queue` возвращается к нулю;
- `search wait` / `ожидание поиска` остаётся близким к нулю или быстро падает
  после завершения текущего vectorization chunk;
- `Failed pipeline jobs` остаётся `0`;
- `Processed articles` соответствует числу успешно обработанных публикаций;
- GPU-панели пустые в CPU-режиме и появляются только при работающем GPU-воркере.

## Почему меньше графиков

Старый dashboard смешивал пользовательские индикаторы, debug-панели batch-level
и ресурсные метрики в одном полотне. Из-за этого было сложно понять порядок
чтения: графики повторяли похожие данные, использовали разные масштабы и
показывали слишком много series с `job_index`.

Текущая версия оставляет на основном экране только то, что помогает объяснить
демо:

- текущее состояние системы;
- ход импорта и pipeline;
- скорость и длительность обработки;
- нагрузку на runtime-ресурсы.

Подробности вроде `Aggregate cluster assignment details`, per-job-index history
rows и полные persisted duration stats лучше держать в отдельном debug dashboard,
если они понадобятся для разработки.

Компактные дашборды занимают один короткий экран и показывают только основные
характеристики: обработанные статьи, backlog, RabbitMQ, прогресс активного
этапа, ошибки, импорт, скорость, длительность и текущие ресурсы. Ресурсные
панели вынесены в отдельные ряды: CPU/RAM отдельно, GPU utilization, VRAM и
температура отдельно. Так компактный экран остаётся читаемым на широком
мониторе и не прячет важную память GPU. Ресурсные панели сделаны time series,
чтобы на них были видны шкалы времени, вертикальные значения и пики за период.

В компактном варианте `Pipeline in range` / `Pipeline за период` показывает
завершённые статьи и jobs за выбранный диапазон Grafana. Текущая очередь
вынесена в отдельный `Backlog`, поэтому нули в backlog после обработки являются
нормальным состоянием. В `Backlog` также показаны search jobs и возраст самой
старой ожидающей search job; это помогает увидеть, обгоняет ли поиск
bulk-векторизацию. У панели `Pipeline in range` две оси: слева количество
статей, справа количество jobs, потому что их масштабы обычно различаются на
порядки.

Во всех dashboard’ах `Import rows` / `Строки импорта` серия `published` /
`опубликовано` считает импортированные статьи, которые сейчас находятся в
публикации. Сюда входят и строки, опубликованные сразу при импорте, и черновики,
опубликованные позже из UI.

## Проверка источников

В Prometheus откройте `Status → Target health`. Ожидаемые targets:

- `metrics-exporter`;
- `rabbitmq`;
- `api`;
- `model-service-vectorizer-gpu`, `model-service-vectorizer-cpu` и/или
  `model-service-processor`, в зависимости от режима;
- `prometheus`.

Prometheus использует DNS service discovery и автоматически обнаруживает
запущенные реплики воркеров.

## Ограничения MVP

- метрики приложения сбрасываются при перезапуске `api` или model-service
  containers, но исторические значения остаются в Prometheus;
- часть import/pipeline счётчиков восстанавливается из PostgreSQL;
- `metrics-exporter` получает read-only доступ к Docker socket;
- GPU-панели появляются только при запущенном NVIDIA GPU-воркере с доступным
  NVML; в CPU-режиме отсутствие этих series ожидаемо;
- Grafana и Prometheus опубликованы на host без TLS, поэтому для внешнего
  окружения их нужно закрыть firewall, reverse proxy или внутренней сетью.
