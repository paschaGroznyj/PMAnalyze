# PMAnalyze

Сервис аналитики статей по Process Mining: еженедельный сбор -> оценка релевантности
+ категория (1 LLM-запрос) -> чтение первых 4 стр. PDF -> генерация ревью (markdown).

## Пайплайн
1. Крон (раз в неделю, пн 12:00 МСК) дергает `POST catchpm-browser:9333/api/parser/run`
   -> JSON метаданных статей (arxiv API/HTML, crossref, CORE API, fluxicon, google scholar).
2. Upsert в `process_mining.papers_metadata` (уникальность source+external_id).
3. Для каждой new-статьи: LLM оценивает релевантность (4 критерия, порог 0.6)
   и сразу выдаёт категорию — одним JSON.
4. Для релевантных: `POST /api/pdf/fulltext` (первые 4 стр.) -> LLM формирует ревью в md.
5. Ревью пишется в `process_mining.reviews.review_md` (только markdown, рендер на фронте).

## Схема (process_mining)
- `papers_metadata` — метаданные + флаги (все обнулены на старте)
- `reviews` — ревью в markdown (1:1 к статье)
- `parser_runs` — журнал запусков
- `ui_article_quarantine` — ручные отметки «карантина» из панели Articles.
  Нужна для операционной работы: пометить статьи на перепроверку/удаление.
  Используется UI-чекбоксом (колонка `Q`) и API:
  - `GET /api/articles` -> поле `quarantine_active`
  - `POST /api/articles/{id}/quarantine` -> включить/снять карантин.

## API
- `POST /api/pipeline/run` — полный цикл (фон)
- `POST /api/pipeline/process?limit=50` — обработать уже собранные (без сбора)
- `GET  /api/articles?only_relevant=true` — список
- `GET  /api/articles/{id}/review` — ревью статьи
- `GET  /api/runs` / `GET /api/stats`

## Архитектура
Микросервисная. PMAnalyze — только http-клиент: всё "железо" парсинга
(httpx / BeautifulSoup / playwright / pypdf) живёт в сервисе-парсере
`catchpm-browser` (порт 9333). PMAnalyze дёргает его по докер-сети:
`_collect()` -> `POST /api/parser/run`, `_fetch_pdf_text()` -> `POST /api/pdf/fulltext`,
точечное обогащение метаданных -> `POST /api/core/enrich`.
Никакого playwright/pypdf и золота внутри PMAnalyze нет.

## Донор
Логика перенесена из `catchpm-chat-v6:/app/article_pipeline.py`.
Парсер-эндпоинты `/api/parser/run` и `/api/pdf/fulltext` вынесены в отдельный
подключаемый патч `browser_parser_patch/pm_articles_router.py` — устанавливается
в сервис `catchpm-browser` (см. `browser_parser_patch/README.md`), gold-логика не затронута.

## Инициализация БД
Применить `sql/001_schema.sql` (дропает бесполезные колонки, оставляет category,
обнуляет флаги, создаёт reviews/parser_runs).

## UI
Шаблон фронта будет добавлен позже (templates/).
