"""PMAnalyze API: управление еженедельным пайплайном + дашборд + дайджест."""
import os
import json
import time
import asyncpg
import httpx
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel

from app.pipeline import PMAnalyzePipeline, Settings
from app.prompts import CATEGORIES
from app.email_sender import EmailSender
from app.digest import collect_digest, render_digest_html, PRESET_TITLE, render_reviews_markdown, upload_markdown_to_obs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

DB_DSN = os.getenv("PMANALYZE_DSN", "postgresql://catchpm:catchpm2026@catchpm-postgres:5432/catchpm_chat")

# SMTP (Yandex) для дайджеста
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.yandex.ru")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_LOGIN = os.getenv("SMTP_LOGIN", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")

pipeline: PMAnalyzePipeline | None = None
pool: asyncpg.Pool | None = None
mailer: EmailSender | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pipeline, pool, mailer
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)
    pipeline = PMAnalyzePipeline(pool, Settings())
    mailer = EmailSender(SMTP_HOST, SMTP_PORT, SMTP_LOGIN, SMTP_PASSWORD)
    await pipeline.start()
    yield
    if pipeline:
        await pipeline.stop()
    if pool:
        await pool.close()


app = FastAPI(title="PMAnalyze", lifespan=lifespan)


def _normalize_authors_payload(raw):
    """Нормализует authors к list[str] для API (array | object | json-string)."""
    def clean_list(vals):
        out = []
        seen = set()
        for x in vals or []:
            t = str(x).strip()
            if not t:
                continue
            k = t.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(t)
        return out

    if raw is None:
        return []

    if isinstance(raw, list):
        return clean_list(raw)

    if isinstance(raw, dict):
        for key in ('list', 'LIST', 'authors', 'Authors'):
            v = raw.get(key)
            if isinstance(v, list):
                return clean_list(v)
        keys = [k for k in raw.keys() if isinstance(k, str) and k.strip()]
        return clean_list(keys)

    if isinstance(raw, str):
        t = raw.strip()
        if not t:
            return []
        if (t.startswith('{') and t.endswith('}')) or (t.startswith('[') and t.endswith(']')):
            try:
                return _normalize_authors_payload(json.loads(t))
            except Exception:
                pass
        parts = [x.strip() for x in t.replace(';', ',').split(',')]
        return clean_list(parts)

    return clean_list([raw])


# ---------------- Dashboard (Jinja-free: статичный HTML) ----------------
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open(os.path.join(TEMPLATES_DIR, "dashboard.html"), encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/health")
async def health():
    return {"ok": True, "service": "PMAnalyze"}


# ---------------- Stats для дашборда ----------------
@app.get("/api/stats")
async def stats():
    async with pool.acquire() as con:
        base = await con.fetchrow("""
            SELECT count(*) total,
                   count(*) FILTER (WHERE p.is_relevant) relevant,
                   count(*) FILTER (WHERE EXISTS (
                       SELECT 1 FROM process_mining.reviews r WHERE r.article_id = p.id
                   )) reviewed,
                   count(*) FILTER (WHERE p.relevance_score IS NOT NULL) assessed,
                   count(*) FILTER (WHERE p.is_relevant IS NOT NULL AND p.relevance_score IS NOT NULL AND NOT p.is_relevant) irrelevant,
                   count(*) FILTER (
                       WHERE p.llm_status = 'review_error'
                         AND p.relevance_reasoning IN ('content_unavailable_or_invalid_source_url','invalid_or_unreachable_source_url')
                   ) invalid_sources,
                   count(*) FILTER (WHERE p.created_at >= now() - interval '7 days' AND p.is_relevant = TRUE) week_new,
                   COALESCE(round((avg(p.relevance_score) FILTER (WHERE p.is_relevant))::numeric, 2), 0) avg_relevance
            FROM process_mining.papers_metadata p
        """)
        cat_rows = await con.fetch("""
            SELECT split_part(category, ':', 1) AS grp, count(*) AS c
            FROM process_mining.papers_metadata
            WHERE is_relevant = TRUE AND category IS NOT NULL AND category <> ''
            GROUP BY 1
            ORDER BY 2 DESC
        """)
        _unused_cat_rows = await con.fetch("""
            SELECT COALESCE(category,'Без категории') category, count(*) c
            FROM process_mining.papers_metadata
            WHERE is_relevant = TRUE
            GROUP BY 1
        """)
        # динамика по неделям (последние 8)
        dyn_rows = await con.fetch("""
            SELECT to_char(date_trunc('week', p.created_at), 'DD.MM') wk,
                   count(*) FILTER (WHERE p.is_relevant = TRUE) collected,
                   count(*) FILTER (
                       WHERE p.is_relevant = TRUE
                         AND EXISTS (SELECT 1 FROM process_mining.reviews r WHERE r.article_id = p.id)
                   ) processed
            FROM process_mining.papers_metadata p
            WHERE p.created_at >= now() - interval '8 weeks'
            GROUP BY date_trunc('week', p.created_at)
            ORDER BY date_trunc('week', p.created_at)
        """)
    # Группируем по префиксу до ':' — реальные категории вида
    # "Общие принципы: Моделирование в PM" схлопываются в "Общие принципы".
    # Плоские англоязычные (AI & Process Mining и т.п.) остаются как есть.
    by_category = {}
    for r in cat_rows:
        grp = (r["grp"] or "").strip() or "Без категории"
        by_category[grp] = by_category.get(grp, 0) + r["c"]
    return {
        "ok": True,
        "total": base["total"], "relevant": base["relevant"], "reviewed": base["reviewed"],
        "assessed": base["assessed"], "irrelevant": base["irrelevant"],
        "invalid_sources": base["invalid_sources"],
        "week_new": base["week_new"], "avg_relevance": float(base["avg_relevance"]),
        "by_category": by_category,
        "dynamics": {
            "labels":    [r["wk"] for r in dyn_rows],
            "collected": [r["collected"] for r in dyn_rows],
            "processed": [r["processed"] for r in dyn_rows],
        },
    }


# ---------------- Ручные кнопки ----------------
@app.post("/api/run/parser")
async def run_parser(background: BackgroundTasks):
    """Полный цикл: сбор -> релевантность -> ревью (фоново)."""
    # Блокируем спам запуска в пределах инстанса
    if not await pipeline._try_claim_parser_run():
        return JSONResponse({"ok": False, "status": "busy", "reason": "parser_already_running"}, status_code=409)

    # Блокируем параллельные запуски между инстансами/пользователями
    if not await pipeline._try_lock():
        await pipeline._release_parser_run()
        return JSONResponse({"ok": False, "status": "busy", "reason": "parser_lock_busy"}, status_code=409)

    async def _run_parser_prelocked():
        try:
            await pipeline.run_once()
        finally:
            await pipeline._release_parser_run()

    background.add_task(_run_parser_prelocked)
    return {"ok": True, "status": "started"}


@app.post("/api/run/reviews")
async def run_reviews(background: BackgroundTasks, limit: int = 50, mode: str = "only"):
    """Обработать ревью без нового сбора.

    mode=only: только генерация ревью для уже релевантных без пересчета категорий.
    mode=full: старая логика process_pending (new/review_error с reassess).
    """
    # Не даем прожать кнопку повторно, пока обработка уже идет.
    if not await pipeline._try_claim_reviews_run():
        return JSONResponse({"ok": False, "status": "busy", "reason": "reviews_already_running"}, status_code=409)

    if not await pipeline._try_lock():
        await pipeline._release_reviews_run()
        return JSONResponse({"ok": False, "status": "busy", "reason": "reviews_lock_busy"}, status_code=409)

    mode = (mode or "only").lower()
    if mode == "full":
        background.add_task(pipeline.process_pending_prelocked, limit)
    else:
        background.add_task(pipeline.process_reviews_only_prelocked, limit)
    return {"ok": True, "status": "started", "limit": limit, "mode": mode}


# ---------------- Дайджест на почту ----------------
class DigestReq(BaseModel):
    preset: str = "week"          # week | month | quarter
    email: str


@app.post("/api/digest/send")
async def digest_send(req: DigestReq):
    if req.preset not in PRESET_TITLE:
        return JSONResponse({"ok": False, "error": "bad_preset"}, status_code=400)
    if not req.email or "@" not in req.email:
        return JSONResponse({"ok": False, "error": "bad_email"}, status_code=400)
    if not SMTP_LOGIN or not SMTP_PASSWORD:
        return JSONResponse({"ok": False, "error": "smtp_not_configured"}, status_code=500)

    data = await collect_digest(pool, req.preset)

    # Полная склейка ревью -> OBS (markdown)
    md_full = render_reviews_markdown(data)
    obs_res = upload_markdown_to_obs(
        md_full,
        req.preset,
        data.get("date_from_local"),
        data.get("date_to_local"),
    )
    if obs_res.get("ok"):
        data["full_list_url"] = obs_res.get("public_url")
    else:
        data["full_list_url"] = ""

    html = render_digest_html(data)
    subject = f"PMAnalyze — Дайджест Process Mining ({PRESET_TITLE[req.preset]})"
    res = mailer.send_digest([req.email], subject, html)
    if res.get("status") == "ok":
        return {
            "ok": True,
            "sent_to": res["sent_to"],
            "articles": len(data["articles"]),
            "obs": obs_res,
            "period": {
                "from": str(data.get("date_from_local")),
                "to": str(data.get("date_to_local")),
            },
        }
    return JSONResponse({"ok": False, "error": res.get("message", "send_failed"), "obs": obs_res}, status_code=500)


# ---------------- Существующие данные-эндпоинты ----------------
@app.post("/api/pipeline/run")
async def trigger_run(background: BackgroundTasks):
    background.add_task(pipeline.run_once)
    return {"ok": True, "status": "started"}


@app.post("/api/pipeline/process")
async def trigger_process(limit: int = 50):
    res = await pipeline.process_pending(limit=limit)
    return {"ok": True, **res}


@app.get("/api/articles")
async def list_articles(page: int = 1, per_page: int = 20, only_relevant: bool = False):
    page = max(1, page)
    per_page = min(50, max(1, per_page))
    offset = (page - 1) * per_page
    where = "WHERE is_relevant = TRUE" if only_relevant else ""
    async with pool.acquire() as con:
        total = await con.fetchval(
            f"SELECT count(*) FROM process_mining.papers_metadata {where}")
        rows = await con.fetch(f"""
            SELECT p.id, p.source, p.title, p.date_sub, p.url_article,
                   p.category, p.relevance_score, p.is_relevant,
                   (r.article_id IS NOT NULL) AS has_review,
                   p.processed_at,
                   CASE
                     WHEN p.source ILIKE 'yandex_disk:%' THEN COALESCE(p.pdf_url, p.url_article)
                     ELSE p.url_article
                   END AS display_url
            FROM process_mining.papers_metadata p
            LEFT JOIN process_mining.reviews r ON r.article_id = p.id
            {where}
            ORDER BY p.date_sub DESC NULLS LAST, p.id DESC
            LIMIT $1 OFFSET $2
        """, per_page, offset)
    return {
        "ok": True,
        "page": page, "per_page": per_page,
        "total": total, "pages": (total + per_page - 1) // per_page,
        "articles": [dict(r) for r in rows],
    }


@app.get("/api/articles/{article_id}/review")
async def get_review(article_id: int):
    async with pool.acquire() as con:
        art = await con.fetchrow("""
            SELECT id, title, source, category, relevance_score, is_relevant,
                   relevance_reasoning, abstract, url_article, pdf_url, date_sub,
                   authors, tags, processed_at
            FROM process_mining.papers_metadata WHERE id=$1
        """, article_id)
        rev = await con.fetchrow(
            "SELECT review_md, model_name, created_at FROM process_mining.reviews WHERE article_id=$1",
            article_id)
    if not art:
        return JSONResponse({"ok": False, "error": "article_not_found"}, status_code=404)
    a = dict(art)
    # authors может быть array/object/json-string — отдаем стабильно list[str]
    a["authors"] = _normalize_authors_payload(a.get("authors"))
    return {"ok": True, "article": a, "review": dict(rev) if rev else None}



# ---------------- Weather widget ----------------
# offset (часы от UTC) -> (город, lat, lon). +3 MSK -> Москва, +4 Самара, +5 Екб.
_W_CITY_BY_OFFSET = {
    3: ("Москва", 55.7522, 37.6156),
    4: ("Самара", 53.1959, 50.1000),
    5: ("Екатеринбург", 56.8389, 60.6057),
}
_W_FALLBACK = ("Москва", 55.7522, 37.6156)
_W_OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
_W_TTL = 1800  # 30 минут
_w_cache: dict = {}  # {(lat,lon): {"data": {...}, "ts": float}}


async def _w_fetch(lat: float, lon: float):
    """Запрос к Open-Meteo: temperature_2m + cloud_cover. None при сбое."""
    try:
        async with httpx.AsyncClient(timeout=6) as client:
            r = await client.get(_W_OPEN_METEO, params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,cloud_cover",
            })
            r.raise_for_status()
            cur = r.json().get("current", {})
            return {
                "temp": cur.get("temperature_2m"),
                "cloud_cover": cur.get("cloud_cover"),
            }
    except Exception as e:
        print(f"[pmanalyze] weather fetch error: {e}")
        return None


@app.get("/api/widget/weather")
async def widget_weather(tz_offset: int = 3):
    city, lat, lon = _W_CITY_BY_OFFSET.get(tz_offset, _W_FALLBACK)
    ck = (round(lat, 4), round(lon, 4))
    now = time.time()
    entry = _w_cache.get(ck)

    # свежий кеш (< 30 мин)
    if entry and (now - entry["ts"]) <= _W_TTL and entry["data"]:
        return {"ok": True, "city": city, "source": "cache", **entry["data"]}

    fresh = await _w_fetch(lat, lon)
    if fresh:
        _w_cache[ck] = {"data": fresh, "ts": now}
        return {"ok": True, "city": city, "source": "api", **fresh}

    # API упал, есть устаревший кеш — отдаём его
    if entry and entry["data"]:
        return {"ok": True, "city": city, "source": "stale", **entry["data"]}

    # погоды нет вообще — фронт нарисует прочерк, но время оставит
    return {"ok": False, "city": city, "temp": None, "cloud_cover": None}


@app.get("/api/runs")
async def list_runs(limit: int = 20):
    async with pool.acquire() as con:
        rows = await con.fetch(
            "SELECT * FROM process_mining.parser_runs ORDER BY id DESC LIMIT $1",
            min(100, max(1, limit)))
    return {"ok": True, "runs": [dict(r) for r in rows]}
