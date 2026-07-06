# Скрипты

Скрипты запускают offline pipeline, smoke tests и измерения производительности.

## Инференс

```powershell
python scripts/run_final_pipeline.py --help
```

Запускает финальный clustering/novelty pipeline и сохраняет predictions.

## Проверка стека

```powershell
python scripts/demo_smoke_test.py
```

Проверяет демонстрационных пользователей, обработку публикаций и поиск.

## Бенчмарки

```powershell
python scripts/benchmark_final_pipeline.py --help
python scripts/benchmark_service_pipeline.py --help
python scripts/benchmark_existing_service_pipeline.py --help
python scripts/benchmark_incremental_pipeline.py --help
python scripts/benchmark_parallel_service_pipeline.py --help
```

`benchmark_parallel_service_pipeline.py` создаёт независимый `full` job для
каждой части корпуса. Это измеряет суммарный throughput workers, но не сохраняет
глобальную кластеризацию между частями.

Методика и актуальные результаты: [бенчмарки](../docs/benchmarks.md).

## Подготовка demo

```powershell
python scripts/build_demo_fixture.py
python scripts/build_lenta_import_sample.py --limit 1000 --target data/import/lenta_import_sample_1000.zip
python scripts/build_lenta_import_sample.py --date-from 2018-01-01 --date-to 2018-12-31 --limit 5000 --target data/import/lenta_2018_5000.zip
```

`build_demo_fixture.py` выбирает последние 1000 валидных публикаций из полного
архива Lenta.ru, сохраняя исходные даты. Более широкий временной поток нужен,
чтобы в demo были не только начальные сообщения сюжетов, но также обновления
и вероятные дубликаты.

`build_lenta_import_sample.py` готовит ZIP для ручного импорта через UI или API.
Если `data/raw/lenta-ru-news.csv.bz2` отсутствует, скрипт скачивает открытый
датасет Lenta.ru в локальный кеш. Выборку можно ограничить через `--limit`,
`--date-from`, `--date-to` и сохранить как ZIP или обычный CSV через `--csv`.

## Проверка артефактов

```powershell
python scripts/inspect_model_artifacts.py --help
```

Проверяет модель и конфигурацию из
`data/artifacts/models/final_exp10/`.
