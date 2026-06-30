# HTTP API

Полная интерактивная схема доступна после запуска по адресу
<http://localhost/api/docs>. Ниже перечислены основные сценарии, а не полный
OpenAPI-контракт.

Все пути через nginx имеют префикс `/api`.

## Аутентификация и роли

```text
POST /api/auth/login
GET  /api/users/me
```

API использует bearer token. Основные роли:

- `user` — поиск и просмотр собственных операций;
- `publisher` — импорт, публикация и управление статьями;
- `admin` — пользователи, организации, кредиты и аудит.

## Создание и импорт публикаций

```text
POST /api/news
GET  /api/news/import-formats
POST /api/news/import
```

`POST /api/news` создаёт черновик. Поле `publish_immediately=true` позволяет
сразу опубликовать его и поставить задачу обработки.

Импорт выполняется multipart-запросом с полями:

- `format`;
- `file`;
- `publish_immediately`.

Формат `lenta` принимает CSV с обязательными `title`, `text` и `date` либо
`published_at`. Вместо одиночного CSV можно передать ZIP-архив: из него будут
прочитаны все `.csv`, остальные файлы игнорируются.

Максимум одного импорта определяется серверной конфигурацией; актуальный код
допускает файл до 200 MiB и до 50 000 строк. Большой импорт выполняется
асинхронно через import job, статус можно смотреть через:

```text
POST /api/news/import-jobs
GET  /api/news/import-jobs/latest
GET  /api/news/import-jobs/{import_job_id}
```

## Жизненный цикл статей

```text
POST   /api/news/{article_id}/publish
POST   /api/news/publish
DELETE /api/news
POST   /api/news/archive
POST   /api/news/restore
POST   /api/news/reprocess
POST   /api/news/moderation-labels
GET    /api/news/me/history
```

Черновики не участвуют в поиске и кластеризации. Публикация списывает стоимость
операции, переводит статью в `public/pending` и создаёт задачу.

Пакетная публикация атомарна на уровне базы: если хотя бы одна выбранная статья
не может быть опубликована или средств недостаточно, не публикуется ни одна.
Для больших импортов предпочтительно включать `publish_immediately=true` при
создании import job: это избегает ручного выбора десятков тысяч черновиков в UI.

История `/api/news/me/history` поддерживает пагинацию `limit/offset` и фильтр
`visibility=draft|public|archived`. UI загружает отдельные страницы по видимости,
а выбор в таблицах относится только к текущей странице.

## Задачи пайплайна

```text
POST /api/news-pipeline
GET  /api/news-pipeline/{job_id}
```

Запрос:

```json
{
  "news_ids": ["ARTICLE_UUID"],
  "mode": "incremental"
}
```

`mode` принимает `incremental` или `full`. Статус задачи:
`queued`, `processing`, `done` или `failed`.

Большой `incremental` job автоматически разбивается на child jobs режима
`vectorize`. Они считают и сохраняют embeddings пачками, после чего aggregate
job запускает кластеризацию и модель один раз на всей исходной пачке. Клиенту
возвращается ID parent job; его статус становится `done` после завершения
aggregate job.

## Семантический поиск

```text
POST /api/news-search
```

Поиск асинхронный и выполняется по всем статьям в состоянии
`public/processed`. Доступны фильтры языка, источника, периода, минимальной
релевантности и новизны.

Черновики и необработанные статьи исключаются. Дедупликация выдачи использует
предсказанные кластеры.

## Учёт операций

```text
GET  /api/accounting/me/balance
GET  /api/accounting/me/transactions
POST /api/accounting/credits
GET  /api/accounting/admin/transactions
```

Стоимость публикации задаётся `NEWS_ADD_COST`. Администратор управляет
кредитами, пользователь видит баланс и историю операций.

## Контракт времени

`published_at` должен содержать смещение относительно UTC, например:

```text
2026-06-19T12:00:00+03:00
```

Это необходимо для корректного временного порядка, окон кластеризации и
обработки late arrivals.

## Связанные документы

- [Архитектура](architecture.md)
- [Быстрый запуск](getting_started.md)
- [ML-пайплайн](ml_pipeline.md)
