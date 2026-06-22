# HTTP API

Полная интерактивная схема доступна после запуска по адресу
<http://localhost/api/docs>. Ниже перечислены основные сценарии, а не полный
OpenAPI-контракт.

Все пути через nginx имеют префикс `/api`.

## Аутентификация и роли

```text
POST /api/v1/auth/login
GET  /api/v1/users/me
```

API использует bearer token. Основные роли:

- `user` — поиск и просмотр собственных операций;
- `publisher` — импорт, публикация и управление статьями;
- `admin` — пользователи, организации, кредиты и аудит.

## Создание и импорт публикаций

```text
POST /api/v1/news
GET  /api/v1/news/import-formats
POST /api/v1/news/import
```

`POST /api/v1/news` создаёт черновик. Поле `publish_immediately=true` позволяет
сразу опубликовать его и поставить задачу обработки.

Импорт выполняется multipart-запросом с полями:

- `format`;
- `file`;
- `publish_immediately`.

Формат `lenta` принимает CSV с обязательными `title`, `text` и `date` либо
`published_at`. Максимум одного импорта определяется серверной конфигурацией;
актуальный код допускает файл до 200 MiB и до 10 000 строк.

## Жизненный цикл статей

```text
POST   /api/v1/news/{article_id}/publish
POST   /api/v1/news/publish
DELETE /api/v1/news
POST   /api/v1/news/archive
POST   /api/v1/news/restore
POST   /api/v1/news/reprocess
POST   /api/v1/news/moderation-labels
GET    /api/v1/news/me/history
```

Черновики не участвуют в поиске и кластеризации. Публикация списывает стоимость
операции, переводит статью в `public/pending` и создаёт задачу.

Пакетная публикация атомарна на уровне базы: если хотя бы одна выбранная статья
не может быть опубликована или средств недостаточно, не публикуется ни одна.

## Задачи пайплайна

```text
POST /api/v1/news-pipeline
GET  /api/v1/news-pipeline/{job_id}
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

## Семантический поиск

```text
POST /api/v1/news-search
```

Поиск асинхронный и выполняется по всем статьям в состоянии
`public/processed`. Доступны фильтры языка, источника, периода, минимальной
релевантности и новизны.

Черновики и необработанные статьи исключаются. Дедупликация выдачи использует
предсказанные кластеры.

## Учёт операций

```text
GET  /api/v1/accounting/me/balance
GET  /api/v1/accounting/me/transactions
POST /api/v1/accounting/credits
GET  /api/v1/accounting/admin/transactions
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
