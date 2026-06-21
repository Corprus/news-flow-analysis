# Скрипты

Скрипты обслуживают финальный BGE-M3 clustering/novelty pipeline.

## Запуск pipeline

```bash
python scripts/run_final_pipeline.py --help
```

Скрипт запускает финальный pipeline над входным набором новостей и сохраняет результаты.

## Benchmark

Полный pipeline:

```bash
python scripts/benchmark_final_pipeline.py --help
```

Incremental pipeline:

```bash
python scripts/benchmark_incremental_pipeline.py --help
```

Aggregate throughput of several service workers on an existing corpus:

```powershell
python scripts/benchmark_parallel_service_pipeline.py --workers 4 --limit 1000
```

For more than one worker the script submits one non-overlapping `full` job per
worker. This measures aggregate throughput; clustering is independent inside
each partition.

## Проверка артефактов

```bash
python scripts/inspect_model_artifacts.py --help
```

Скрипт проверяет runtime-модель и конфигурацию из
`data/artifacts/models/final_exp10/`.
