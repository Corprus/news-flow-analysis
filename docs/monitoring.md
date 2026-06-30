# Мониторинг

Docker Compose включает минимальный стек мониторинга:

- `grafana` — готовый дашборд `Semantic News Novelty — мониторинг`;
- `prometheus` — сбор и хранение временных рядов;
- `metrics-exporter` — CPU и RAM контейнеров через Docker API;
- встроенный `rabbitmq_prometheus` — состояние очередей;
- endpoint `/metrics` у `api` — импорт новостей и фоновые import jobs;
- endpoint `/metrics` у `model-service` — скорость и результаты обработки;
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

## Дашборд MVP

Дашборд создаётся автоматически и показывает:

- текущее количество новостей со статусом `processed`;
- количество готовых и обрабатываемых сообщений RabbitMQ;
- среднюю скорость последнего успешного задания в новостях в секунду;
- количество неуспешных pipeline jobs после старта воркера;
- длительность последнего завершённого pipeline job;
- количество import jobs, импортированных строк и длительность импорта;
- количество vectorization chunks и статей, прошедших embeddings-стадию;
- текущий этап активного pipeline job через `news_flow_pipeline_stage_articles`;
- длительность child jobs последнего большого pipeline, throughput, рост истории
  по aggregate-пачкам и длительность отдельных стадий через DB-backed метрики
  `news_flow_pipeline_latest_*`;
- количество статей, находящихся в queued/processing pipeline jobs;
- CPU и RAM по сервисам Docker Compose;
- загрузку GPU, использование видеопамяти и температуру.

Панели import jobs и import rows используют DB-backed метрики из
`news_pipeline_jobs`. Они восстанавливаются после перезапуска API и показывают
уже завершённые импорты, а не только in-memory counters текущего процесса.

CPU и GPU utilization отображаются на одном временном графике, RAM и VRAM —
на втором. Это позволяет сопоставлять переход между GPU- и CPU-фазами
пайплайна без переключения между панелями.

У utilization-графика CPU использует левую шкалу, а GPU — правую фиксированную
шкалу 0–100%. Суммарная загрузка CPU может превышать 100%, поскольку измеряется
в долях используемых процессорных ядер.

Скорость рассчитывается по счётчику успешно обработанных входных статей.
Повторная обработка увеличивает этот счётчик, но не увеличивает текущее
количество строк со статусом `processed`.

## Проверка источников

В Prometheus откройте `Status → Target health`. Ожидаемые targets:

- `metrics-exporter`;
- `rabbitmq`;
- `api`;
- `model-service-gpu` и/или `model-service-cpu`, в зависимости от режима;
- `prometheus`.

Prometheus использует DNS service discovery и автоматически обнаруживает
запущенные реплики воркеров.

## Ограничения MVP

- метрики приложения сбрасываются при перезапуске `api` или `model-service`,
  исторические значения сохраняются в Prometheus;
- общее количество обработанных новостей восстанавливается из PostgreSQL;
- `metrics-exporter` получает read-only доступ к Docker socket;
- GPU-панели появляются только при запущенном NVIDIA GPU-воркере с доступным
  NVML; в CPU-режиме отсутствие этих series ожидаемо;
- Grafana и Prometheus опубликованы на host без TLS, поэтому для внешнего
  окружения их нужно закрыть firewall, reverse proxy или внутренней сетью.
