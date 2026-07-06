# Мониторинг

Docker Compose включает минимальный стек мониторинга для демонстрации и локальной
диагностики:

- `grafana` — обзорный дашборд `Semantic News Novelty - Monitoring`;
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

## Дашборд

Дашборд сделан как обзор для пользователя, а не как полный набор внутренних
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
