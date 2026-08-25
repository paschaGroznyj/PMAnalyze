# PMAnalyze

Сервис аналитики статей по Process Mining: сбор статей, генерация ревью и гибридный поиск по базе ревью.

## Что делает сервис
1. Сбор метаданных статей (через `catchpm-browser`) и запись в `process_mining.papers_metadata`.
2. Оценка релевантности и категоризация статей LLM.
3. Генерация ревью (Markdown) и запись в `process_mining.reviews`.
4. Векторизация ревью и хранение эмбеддингов в `process_mining.review_embeddings`.
5. Быстрый и гибридный поиск по ревью + опциональное LLM-саммари найденного.

## Ключевые API
### Pipeline
- `POST /api/pipeline/run` — полный цикл (фон)
- `POST /api/pipeline/process?limit=50` — обработать уже собранные статьи

### Контент
- `GET /api/articles?only_relevant=true` — список статей
- `GET /api/articles/{id}/review` — ревью по статье
- `GET /api/runs` — история запусков
- `GET /api/stats` — общая статистика пайплайна

### Поиск
- `GET /api/search/quick?q=&limit=` — live typeahead (ILIKE + pg_trgm)
- `POST /api/search/hybrid` — BM25 + vector search (RRF fusion)
- `GET /api/search/stats` — статистика векторного индекса

## Текущее состояние поиска
- Модель эмбеддингов: `Qwen/Qwen3-VL-Embedding-8B`
- Размерность вектора: `4096`
- В таблице `review_embeddings`: `119` векторизованных ревью
- Поддержка `llm_summary=true` в hybrid-поиске (модель sammary по умолчанию: `anthropic/claude-haiku-4.5`)

## Схема БД (process_mining)
- `papers_metadata` — метаданные статей и служебные флаги
- `reviews` — markdown-ревью (1:1 к статье)
- `review_embeddings` — текст ревью + embedding vector(4096) + tsv
- `parser_runs` — журнал запусков пайплайна

## Архитектура
PMAnalyze не содержит тяжёлый парсинг внутри себя: парсинг и PDF-извлечение выполняются через `catchpm-browser` по HTTP:
- `_collect()` -> `POST /api/parser/run`
- `_fetch_pdf_text()` -> `POST /api/pdf/fulltext`

## Инфраструктура Postgres + pgvector
Для закрепления расширения в образе добавлены:
- `infra/postgres/Dockerfile.pgvector`
- `infra/postgres/init-pgvector.sql`

Рекомендуемый образ БД:
- `catchpm-postgres:pgvector-0.8.0`

Важно: `init-pgvector.sql` исполняется только при первой инициализации кластера (пустой `PGDATA`). Для существующего тома расширение создаётся один раз вручную:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

## Локальный запуск
```bash
docker build -t pmanalyze:local .
docker run --rm -p 18021:8000 --env-file .env pmanalyze:local
```

## Зависимости
См. `requirements.txt`.
