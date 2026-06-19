# Артефакты моделей

В runtime используется один набор model artifacts финального clustering/novelty
pipeline.

## Финальный clustering/novelty pipeline

Offline/inference pipeline использует:

- embedding-модель `BAAI/bge-m3`, загружаемую через `sentence-transformers`;
- сохранённый wrapper novelty classifier;
- JSON-конфигурацию кластеризации.

Runtime-файлы:

```text
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

Выбранная конфигурация — `exp_10a_current_model_on_exp10_clustering`. Подробности и
актуальные метрики находятся в [`final_pipeline.md`](final_pipeline.md) и
[`model_improvement.md`](model_improvement.md).

Дообученная на русских парафразах BGE-M3 проверялась как ablation, но не вошла в
финальный runtime, поскольку уступила базовой `BAAI/bge-m3` в downstream-оценке.

## Что не хранить в Git

Как правило, не коммитируются:

- полные checkpoints transformer-моделей;
- Hugging Face caches;
- embedding caches `.npz`;
- FAISS-индексы;
- большие подготовленные датасеты;
- predictions и временные training outputs.

Допустимо хранить небольшие runtime-артефакты и metadata, если они нужны для
воспроизводимого запуска и явно включены в проект.

Для production-поставки предпочтительны Hugging Face Hub или объектное хранилище с
зафиксированной ревизией и checksum.
