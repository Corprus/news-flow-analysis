# Улучшение модели Semantic News Novelty

Документ описывает актуальную структуру экспериментов по улучшению модели и то,
как результаты этих экспериментов связаны с финальным пайплайном инференса.

## Ключевые ноутбуки

```text
notebooks/06_model_improvement_experiments_v3.ipynb
notebooks/07_bronze_annotation_export.ipynb
notebooks/08_finetune_bge_m3_embeddings.ipynb
```

Главный ноутбук для выбора финального решения - `06_model_improvement_experiments_v3.ipynb`.
Он фиксирует цепочку экспериментов:

1. воспроизведение бейзлайна `exp_00b`;
2. выбор кластеризации без переобучения модели новизны;
3. подбор присоединения к лучшему кандидату `exp_10` по silver-positive сигналу;
4. сравнение final-step моделей на выбранной кластеризации;
5. экспорт финальной модели и конфигурации;
6. необязательное исследование с дообученной BGE-M3.

## Актуальный результат

Финальным вариантом выбран:

```text
exp_10a_current_model_on_exp10_clustering
```

Это текущая сохраненная CatBoost-модель, примененная к новой выбранной
`exp_10` кластеризации. По актуальной таблице экспериментов она дала лучший
`significant_f1` среди `exp_10*`.

После фиксации рантайм-правила «первая публикация кластера → `significant`» итоговая оценка на
87 строках golden-разметки новизны:

- precision: `0.8471`;
- recall: `0.9863`;
- F1: `0.9114`.

Эти метрики одинаковы для `exp_00b` и `exp_10` при одной модели новизны, потому что
изменение кластеризации не поменяло метки на размеченном подмножестве. Измеренный
прирост `exp_10` относится к кластеризации: попарный F1 вырос с `0.7697` до `0.9017`.

Финальные рантайм-артефакты:

```text
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

`final_pipeline_config.json` фиксирует параметры выбранной кластеризации:

```text
exp10_src2_sim0.75_days7_m0.03_tj0.15_num1
```

## Структура кода

Экспериментальная и рантайм-логика вынесена из ноутбуков в `src/model/` и
`src/final_pipeline/`.

```text
src/model/
  config.py                 # пути и конфигурации экспериментов/моделей
  data.py                   # загрузка, нормализация схем, сохранение CSV с предсказаниями
  embeddings.py             # энкодер SentenceTransformer и кеш эмбеддингов с учётом ID
  legacy_clustering.py      # строгая бейзлайн-кластеризация на графе
  attach_clustering.py      # кластеризация exp_10 с присоединением и перебор silver-positive
  features.py               # признаки только по предыдущим публикациям для модели новизны
  classifier_training.py    # CatBoost / MLP / LogisticRegression training helpers
  significance_model.py     # обёртка модели новизны и правило для первой публикации кластера
  evaluation.py             # метрики кластеризации и метки новизны
  experiment_tracking.py    # сохранение CSV с предсказаниями и таблицы экспериментов

src/final_pipeline/
  config.py                 # конфигурация финального пайплайна инференса
  pipeline.py               # оркестрация, близкая к промышленной
```

`src/final_pipeline/` использует реализацию из `src/model/`, чтобы ноутбук и
рантайм не расходились по кластеризации, построению признаков и правилу
«первая публикация кластера → `significant`».

## Контракт признаков

Актуальный final-step feature set - 18 legacy previous-only признаков из
`model.features.LEGACY_SIGNIFICANCE_FEATURE_COLUMNS`.

Важно: старый experimental-набор с именами вроде `prev_count` и `max_prev_sim`
не является рантайм-контрактом финальной модели. Для финального пайплайна
используются признаки вроде:

```text
position_in_cluster
cluster_size_so_far
max_prev_similarity
previous_centroid_similarity
title_jaccard_max
shared_numbers_count
```

## Дообученная BGE-M3

`08_finetune_bge_m3_embeddings.ipynb` обучал BGE-M3 на русских парафразах и
сохранял модель локально:

```text
E:\MLCache\news-flow-analysis\models\bge_m3_ru_paraphrase_mnrl
```

По текущей финальной конфигурации рантайм использует базовый `BAAI/bge-m3`.
Дообученная модель остаётся результатом исследования, а не частью финального пайплайна обработки.

## Что коммитить

Нужно хранить:

```text
src/model/
src/final_pipeline/
scripts/run_final_pipeline.py
scripts/benchmark_final_pipeline.py
scripts/inspect_model_artifacts.py
requirements_model_improvement.txt
docs/final_pipeline.md
docs/model_improvement.md
data/artifacts/models/final_exp10/final_novelty_model.joblib
data/artifacts/models/final_exp10/final_pipeline_config.json
```

Не нужно хранить:

```text
data/predictions/
data/artifacts/embeddings/
notebooks/catboost_info/
*.zip
src/final_pipeline_old/
```

Эти файлы являются сгенерированными результатами, локальными кешами или старым кодом.

## Проверка работоспособности

Быстрый запуск финального пайплайна:

```powershell
python scripts/run_final_pipeline.py `
  --project-root . `
  --input data/prepared/lenta_clean_news.csv `
  --output data/predictions/final_pipeline_v3_predictions.csv `
  --embeddings-cache data/artifacts/embeddings/final_pipeline_v3_bge_m3.npz `
  --device cuda
```

`--model` и `--config` можно не указывать: по умолчанию подхватываются
финальные артефакты `exp_10a` из `data/artifacts/models/final_exp10/`.

Быстрый бенчмарк:

```powershell
python scripts/benchmark_final_pipeline.py `
  --project-root . `
  --n-rows 10000 `
  --device cuda
```

## Следующий этап

После фиксации финального пайплайна дальнейшая работа логично делится на два
потока:

1. косметическая чистка ноутбуков и текстовых выводов;
2. разработка сервиса вокруг `src/final_pipeline`.

Для сервиса важно использовать `load_pipeline(project_root=...)` из
`src/final_pipeline/pipeline.py`: он автоматически загружает выбранную `10a`
модель и сохранённую конфигурацию кластеризации.

## Связанные документы

- [Финальный пайплайн](final_pipeline.md)
- [Основной README](../README.md)
