# browser_parser_patch

Патч-модуль для **существующего** сервиса-парсера `catchpm-browser`
(контейнер `catchpm-browser`, образ `catchpm-browser-parser:v3-coreapi`, порт `9333`).

Это НЕ часть PMAnalyze. Всё "железо" парсинга (httpx / BeautifulSoup /
playwright / pypdf) живёт внутри сервиса-парсера. PMAnalyze — только http-клиент,
дёргает эндпоинты по докер-сети.

## Что даёт

Добавляет к боевому `parser_service.py` (health + fetch/pages + gold/scan) три эндпоинта:

| Метод | Путь                 | Назначение                                              |
|-------|----------------------|---------------------------------------------------------|
| POST  | `/api/parser/run`    | Сбор статей PM: arxiv API/HTML, fluxicon, google scholar |
| POST  | `/api/pdf/fulltext`  | Первые N (=4) страниц PDF через pypdf                    |
| POST  | `/api/core/enrich`   | Точечное обогащение CORE по DOI/Title                    |

Gold-логика не затрагивается.

## Установка

1. Скопировать `pm_articles_router.py` в рабочую директорию контейнера
   (туда же, где `parser_service.py`, т.е. `/app`):

   ```bash
   docker cp browser_parser_patch/pm_articles_router.py catchpm-browser:/app/pm_articles_router.py
   ```

2. Подключить роутер в `parser_service.py` (одна строка после создания `app`):

   ```python
   from pm_articles_router import router as pm_router
   app.include_router(pm_router)
   ```

3. Проверить зависимости в образе: `httpx`, `beautifulsoup4`, `playwright`, `pypdf`.
   (playwright уже присутствует — используется google scholar. pypdf нужен для fulltext.)

4. Перезапустить сервис:

   ```bash
   docker restart catchpm-browser
   curl -s localhost:9333/health
   ```

## Контракт

### POST /api/parser/run
Заголовок: `X-Api-Key: <PARSER_SERVICE_API_KEY>` (если переменная задана в контейнере).

Запрос:
```json
{ "query": "<опц., по умолчанию PM_QUERY_DEFAULT>", "max_per_source": 20, "ui_lang": "en" }
```
Ответ:
```json
{
  "ok": true,
  "query": "...",
  "sources": { "arxiv_api": 20, "arxiv_html": 18, "crossref": 12, "core_api": 10, "fluxicon": 5, "google_scholar": 10 },
  "errors": {},
  "total": 53,
  "articles": [
    { "source": "arxiv_api", "external_id": "2408.12345", "title": "...",
      "date_sub": "2026-08-20", "pdf_url": "https://arxiv.org/pdf/2408.12345.pdf",
      "authors": {"list": ["..."]}, "tags": ["arxiv","api"],
      "abstract": "...", "category": "process_mining" }
  ],
  "started_at": 0, "finished_at": 0
}
```

### POST /api/pdf/fulltext
Запрос:
```json
{ "urls": ["https://arxiv.org/pdf/2408.12345.pdf"], "max_pages": 4, "max_chars": 20000, "max_urls": 25 }
```
Ответ:
```json
{
  "ok": true, "requested": 1, "read_ok": 1,
  "results": [
    { "url": "...", "norm_url": "...", "ok": true, "error": null,
      "pages_read": 4, "chars": 8123, "text": "..." }
  ],
  "generated_at": "2026-08-24T09:00:00+00:00"
}
```


### POST /api/core/enrich
Запрос:
```json
{ "doi": "10.1002/0471741442.ch10", "title": "Process Mining", "limit": 5 }
```
Ответ:
```json
{
  "ok": true,
  "query": {"doi": "...", "title": "..."},
  "total": 3,
  "best_match": {"doi": "...", "title": "...", "downloadUrl": "..."},
  "results": [ ... ],
  "errors": []
}
```
