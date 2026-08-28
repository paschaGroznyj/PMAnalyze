# Local Knowledge Graph — PMAnalyze (offline 6.4.10)

Офлайн-воспроизведение панели **Knowledge Graph** из дашборда в Jupyter-ноутбуке:
поля ввода, кнопка поиска, ползунок глубины связей, флажок LLM-саммари,
панель визуала графа (vis-network) и карточки узлов.

## Данные
Архив выгружается кнопкой «выгрузить» на панели графа. Внутри:
- `pm_knowledge_local.sqlite` (вариант A), либо
- `knowledge.csv`, `knowledge_relations.csv`, ... (вариант B).

## Запуск
```
pip install -r requirements.txt
jupyter notebook local_pm_pipeline_6_4_10.ipynb
```

## Переменные окружения
| Переменная | Назначение | Дефолт |
|---|---|---|
| `PM_SOURCE_MODE` | `sqlite` (A) или `csv` (B) | `sqlite` |
| `PM_DATA_DIR` | папка с распакованным архивом | текущая |
| `PM_SQLITE` | путь к sqlite (вариант A) | `<DATA_DIR>/pm_knowledge_local.sqlite` |
| `PM_CSV_DIR` | папка с CSV (вариант B) | `<DATA_DIR>` |
| `PM_LOCAL_EMBED_MODEL` | модель векторизации (dim детектится авто) | `paraphrase-multilingual-MiniLM-L12-v2` |
| `PM_LLM_BASE_URL` | OpenAI-совместимый endpoint (саммари) | пусто (off) |
| `PM_LLM_API_KEY` | ключ LLM | пусто (off) |
| `PM_LLM_MODEL` | модель саммари | `anthropic/claude-haiku-4.5` |

## Что повторяет из веб-панели
- Гибридный поиск BM25 (FTS5) + вектор (cosine), слияние RRF (rrf_k=60).
- Раскрытие подграфа по глубине связей (BFS), как ползунок в UI.
- Палитра/формы узлов: knowledge (dot, мятный), wiki (star, сиреневый), хабы крупнее.
- Карточка узла: заголовок, meta (id/status/importance), тело.
- Опциональное LLM-саммари по топ-контексту (max_ctx=100).

Адаптация под длину вектора: `VECTOR_DIM` определяется прогоном модели,
никаких хардкодов размерности.
