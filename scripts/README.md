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
python scripts/build_lenta_import_sample.py
```

`build_demo_fixture.py` выбирает последние 1000 валидных публикаций из полного
архива Lenta.ru, сохраняя исходные даты. Более широкий временной поток нужен,
чтобы в demo были не только начальные сообщения сюжетов, но также обновления
и вероятные дубликаты.

## Проверка артефактов

```powershell
python scripts/inspect_model_artifacts.py --help
```

Проверяет модель и конфигурацию из
`data/artifacts/models/final_exp10/`.
