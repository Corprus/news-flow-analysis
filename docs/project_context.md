# Контекст проекта

Проект: News Event Detection.

Цель: система анализа новостного потока, которая группирует новости в события и определяет роль новости: новое событие, обновление или дубль.

Данные:
- Lenta.ru dataset: `data/raw/lenta.csv`, колонки: url, title, text, topic, tags, date.
- ru_paraphraser: использовался для дообучения embedding-модели на парафразах.

Модель:
- base: `sentence-transformers/paraphrase-multilingual-mpnet-base-v2`
- fine-tuning: metric learning на парафразах.
- фактически использован `MultipleNegativesRankingLoss`; Triplet Loss рассматривается как возможное расширение.
- сохранена в `models/news-flow-ru-vectorization-mpnet/final`.
- веса модели не хранятся в Git; политика хранения описана в `docs/model_artifacts.md`.

Оценка:
- base: Recall@1 = 0.9527, Recall@5 = 0.9941, MRR@10 = 0.9699
- fine-tuned: Recall@1 = 0.9645, Recall@5 = 1.0, MRR@10 = 0.9798

Поиск:
- используется FAISS CPU.
- embeddings нормализуются, similarity считается через inner product.

Логика событий:
- одна семантика недостаточна: похожие новости из разных лет не должны быть одним событием.
- событие определяется через semantic similarity + temporal proximity.
- новая новость присоединяется к событию, если есть близкий сосед по embedding и он близок по времени.
- иначе создаётся новое событие.

MVP:
1. Подготовить Lenta.
2. Посчитать embeddings.
3. Построить FAISS index.
4. Реализовать nearest-neighbor grouping.
5. Добавить time window.
6. Присвоить роль новости: new_event / update / duplicate.
