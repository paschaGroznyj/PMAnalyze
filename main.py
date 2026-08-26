"""PMAnalyze API: управление еженедельным пайплайном + дашборд + дайджест."""
import os
import asyncio
import json
import time
import logging
import asyncpg
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel

from app.pipeline import PMAnalyzePipeline, Settings
from app.prompts import CATEGORIES
from app.email_sender import EmailSender
from app.digest import collect_digest, render_digest_html, PRESET_TITLE, render_reviews_markdown, upload_markdown_to_obs
from app.search import search_quick, search_hybrid, llm_summary

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

DB_DSN = os.getenv("PMANALYZE_DSN", "postgresql://catchpm:catchpm2026@catchpm-postgres:5432/catchpm_chat")

# SMTP (Yandex) для дайджеста
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.yandex.ru")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_LOGIN = os.getenv("SMTP_LOGIN", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")

# Digest subscribers / schedule
DIGEST_SUBSCRIBERS = os.getenv("DIGEST_SUBSCRIBERS", "")
DIGEST_ENABLED = os.getenv("DIGEST_ENABLED", "1").lower() in ("1", "true", "yes", "on")
DIGEST_WEEKLY_DOW_MSK = int(os.getenv("DIGEST_WEEKLY_DOW_MSK", "1"))      # 1=вторник
DIGEST_WEEKLY_HOUR_MSK = int(os.getenv("DIGEST_WEEKLY_HOUR_MSK", "10"))   # 10:00 MSK
DIGEST_WEEKLY_MINUTE_MSK = int(os.getenv("DIGEST_WEEKLY_MINUTE_MSK", "0"))

pipeline: PMAnalyzePipeline | None = None
pool: asyncpg.Pool | None = None
mailer: EmailSender | None = None
digest_task: asyncio.Task | None = None
digest_stop: asyncio.Event = asyncio.Event()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pipeline, pool, mailer, digest_task
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)
    pipeline = PMAnalyzePipeline(pool, Settings())
    mailer = EmailSender(SMTP_HOST, SMTP_PORT, SMTP_LOGIN, SMTP_PASSWORD)

    async with pool.acquire() as con:
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.digest_recipients (
                id BIGSERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    await pipeline.start()

    digest_stop.clear()
    if DIGEST_ENABLED:
        digest_task = asyncio.create_task(_digest_scheduler_loop())

    yield

    digest_stop.set()
    if digest_task:
        digest_task.cancel()
        try:
            await digest_task
        except Exception:
            pass
        digest_task = None

    if pipeline:
        await pipeline.stop()
    if pool:
        await pool.close()


app = FastAPI(title="PMAnalyze", lifespan=lifespan)


class _DropNoisyProgressEndpoint(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "/api/run/reviews/progress" not in msg


logging.getLogger("uvicorn.access").addFilter(_DropNoisyProgressEndpoint())
api_logger = logging.getLogger("uvicorn.error")


def _parse_emails_csv(raw: str) -> list[str]:
    out, seen = [], set()
    for t in (raw or "").replace(";", ",").split(","):
        e = t.strip()
        if not e or "@" not in e:
            continue
        k = e.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(e)
    return out


async def _db_digest_recipients() -> list[str]:
    async with pool.acquire() as con:
        rows = await con.fetch("SELECT email FROM process_mining.digest_recipients WHERE enabled=TRUE ORDER BY id")
    return [r["email"] for r in rows if r.get("email")]


async def _effective_digest_recipients() -> list[str]:
    env_list = _parse_emails_csv(DIGEST_SUBSCRIBERS)
    db_list = await _db_digest_recipients()
    out, seen = [], set()
    for e in env_list + db_list:
        k = e.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(e)
    return out


def _subject_for_preset(preset: str) -> str:
    return f"PMAnalyze — Дайджест Process Mining ({PRESET_TITLE.get(preset, preset)})"


async def _build_digest_payload(preset: str) -> tuple[dict, str]:
    data = await collect_digest(pool, preset)
    md_full = render_reviews_markdown(data)
    obs_res = upload_markdown_to_obs(
        md_full,
        preset,
        data.get("date_from_local"),
        data.get("date_to_local"),
    )
    data["full_list_url"] = obs_res.get("public_url") if obs_res.get("ok") else ""
    html = render_digest_html(data)
    return data, html


async def _send_digest_to_subscribers(preset: str = "sub_week") -> dict:
    recipients = await _effective_digest_recipients()
    if not recipients:
        return {"ok": False, "error": "no_recipients"}
    if not SMTP_LOGIN or not SMTP_PASSWORD:
        return {"ok": False, "error": "smtp_not_configured"}

    data, html = await _build_digest_payload(preset)
    subject = _subject_for_preset(preset)

    sent, failed = [], []
    for email in recipients:
        res = mailer.send_digest([email], subject, html)
        if res.get("status") == "ok":
            sent.append(email)
        else:
            failed.append({"email": email, "error": res.get("message", "send_failed")})
        await asyncio.sleep(0.4)

    return {
        "ok": len(sent) > 0,
        "sent": sent,
        "failed": failed,
        "period": {
            "from": str(data.get("date_from_local")),
            "to": str(data.get("date_to_local")),
        },
        "articles": len(data.get("articles") or []),
    }


def _seconds_to_next_digest_run_msk() -> int:
    msk = timezone(timedelta(hours=3))
    now = datetime.now(msk)
    dow = max(0, min(6, int(DIGEST_WEEKLY_DOW_MSK)))
    target = now.replace(hour=int(DIGEST_WEEKLY_HOUR_MSK), minute=int(DIGEST_WEEKLY_MINUTE_MSK), second=0, microsecond=0)
    days_ahead = (dow - now.weekday()) % 7
    target = target + timedelta(days=days_ahead)
    if target <= now:
        target = target + timedelta(days=7)
    return max(1, int((target - now).total_seconds()))


async def _digest_scheduler_loop():
    while not digest_stop.is_set():
        wait_s = _seconds_to_next_digest_run_msk()
        try:
            await asyncio.wait_for(digest_stop.wait(), timeout=wait_s)
            break
        except asyncio.TimeoutError:
            pass

        try:
            res = await _send_digest_to_subscribers("sub_week")
            print(f"[pmanalyze] digest scheduler result: {res}")
        except Exception as e:
            print(f"[pmanalyze] digest scheduler error: {e}")


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
                   count(*) FILTER (
                       WHERE p.is_relevant = TRUE
                         AND EXISTS (SELECT 1 FROM process_mining.reviews r WHERE r.article_id = p.id)
                   ) reviewed,
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
async def run_parser(background: BackgroundTasks, request: Request):
    """Полный цикл: сбор -> релевантность -> ревью (фоново)."""
    client_ip = (request.client.host if request and request.client else "unknown")
    ua = request.headers.get("user-agent", "")[:180]
    api_logger.info(f"parser_button_click ip={client_ip} ua={ua}")
    # Блокируем спам запуска в пределах инстанса
    if not await pipeline._try_claim_parser_run():
        api_logger.info("parser_button_reject reason=parser_already_running")
        return JSONResponse({"ok": False, "status": "busy", "reason": "parser_already_running"}, status_code=409)

    # Блокируем параллельные запуски между инстансами/пользователями
    if not await pipeline._try_lock():
        await pipeline._release_parser_run()
        api_logger.info("parser_button_reject reason=parser_lock_busy")
        return JSONResponse({"ok": False, "status": "busy", "reason": "parser_lock_busy"}, status_code=409)

    async def _run_parser_prelocked():
        try:
            await pipeline.run_once()
        finally:
            await pipeline._release_parser_run()

    background.add_task(_run_parser_prelocked)
    api_logger.info("parser_button_accepted status=started")
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


@app.get("/api/run/reviews/progress")
async def run_reviews_progress():
    """Прогресс текущего запуска ревью для UI (кнопка 121/125)."""
    p = await pipeline.get_reviews_progress()
    total = int(p.get("total") or 0)
    done = int(p.get("done") or 0)
    errors = int(p.get("errors") or 0)
    remaining = max(0, total - done)
    return {
        "ok": True,
        "running": bool(p.get("running")),
        "mode": p.get("mode") or "only",
        "done": done,
        "total": total,
        "errors": errors,
        "remaining": remaining,
        "started_at": p.get("started_at"),
        "updated_at": p.get("updated_at"),
    }


# ---------------- Дайджест на почту ----------------
class DigestReq(BaseModel):
    preset: str = "week"          # week | month | quarter | sub_week
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


@app.get("/api/digest/subscribers")
async def digest_subscribers():
    eff = await _effective_digest_recipients()
    return {
        "ok": True,
        "env_count": len(_parse_emails_csv(DIGEST_SUBSCRIBERS)),
        "db_count": len(await _db_digest_recipients()),
        "total": len(eff),
        "emails": eff,
    }


@app.post("/api/digest/send/subscribers")
async def digest_send_subscribers(preset: str = "sub_week"):
    if preset not in PRESET_TITLE:
        return JSONResponse({"ok": False, "error": "bad_preset"}, status_code=400)
    res = await _send_digest_to_subscribers(preset)
    code = 200 if res.get("ok") else 500
    return JSONResponse(res, status_code=code)




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
# offset (часы от UTC) -> (город, lat, lon). Поддерживаем крупные города по миру.
_W_CITY_BY_OFFSET = {
    -12.0: ("Бейкер-Айленд", 0.1936, -176.4769),
    -11.0: ("Паго-Паго", -14.2756, -170.7020),
    -10.0: ("Гонолулу", 21.3069, -157.8583),
    -9.0: ("Анкоридж", 61.2181, -149.9003),
    -8.0: ("Лос-Анджелес", 34.0522, -118.2437),
    -7.0: ("Денвер", 39.7392, -104.9903),
    -6.0: ("Чикаго", 41.8781, -87.6298),
    -5.0: ("Нью-Йорк", 40.7128, -74.0060),
    -4.0: ("Сантьяго", -33.4489, -70.6693),
    -3.0: ("Сан-Паулу", -23.5558, -46.6396),
    -2.0: ("Южная Георгия", -54.4296, -36.5879),
    -1.0: ("Азорские о-ва", 37.7412, -25.6756),
     0.0: ("Лондон", 51.5074, -0.1278),
     1.0: ("Берлин", 52.5200, 13.4050),
     2.0: ("Калининград", 54.7104, 20.4522),
     3.0: ("Москва", 55.7522, 37.6156),
     3.5: ("Тегеран", 35.6892, 51.3890),
     4.0: ("Самара", 53.1959, 50.1000),
     4.5: ("Кабул", 34.5553, 69.2075),
     5.0: ("Екатеринбург", 56.8389, 60.6057),
     5.5: ("Нью-Дели", 28.6139, 77.2090),
     5.75: ("Катманду", 27.7172, 85.3240),
     6.0: ("Омск", 54.9885, 73.3242),
     6.5: ("Янгон", 16.8409, 96.1735),
     7.0: ("Красноярск", 56.0153, 92.8932),
     8.0: ("Пекин", 39.9042, 116.4074),
     8.75: ("Юкла", 62.8864, 132.7968),
     9.0: ("Токио", 35.6762, 139.6503),
     9.5: ("Дарвин", -12.4634, 130.8456),
    10.0: ("Сидней", -33.8688, 151.2093),
    11.0: ("Владивосток", 43.1155, 131.8855),
    12.0: ("Окленд", -36.8485, 174.7633),
    13.0: ("Нукуалофа", -21.1394, -175.2048),
    14.0: ("Киритимати", 1.8721, -157.4278),
}
_W_FALLBACK = ("Москва", 55.7522, 37.6156)
_W_OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
_W_TTL = 1800  # 30 минут
_w_cache: dict = {}  # {(lat,lon): {"data": {...}, "ts": float}}


def _w_pick_city_by_offset(tz_offset: float):
    """Выбираем ближайший поддерживаемый часовой пояс (включая 0.5/0.75)."""
    keys = list(_W_CITY_BY_OFFSET.keys())
    nearest = min(keys, key=lambda k: abs(float(k) - float(tz_offset)))
    return _W_CITY_BY_OFFSET.get(nearest, _W_FALLBACK)


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
async def widget_weather(tz_offset: float = 3):
    city, lat, lon = _w_pick_city_by_offset(tz_offset)
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


# ---------------- Поиск по ревью (quick / hybrid / +LLM-саммари) ----------------
@app.get("/api/search/stats")
async def api_search_stats():
    """Сколько векторов в базе + покрытие ревью эмбеддингами."""
    try:
        async with pool.acquire() as con:
            row = await con.fetchrow(
                """
                SELECT
                    (SELECT count(*) FROM process_mining.review_embeddings) AS total_rows,
                    (SELECT count(*) FROM process_mining.review_embeddings
                      WHERE embedding IS NOT NULL) AS with_embedding,
                    (SELECT count(DISTINCT article_id) FROM process_mining.reviews
                      WHERE review_md IS NOT NULL AND length(trim(review_md)) > 0) AS reviewable_articles,
                    (SELECT max(updated_at) FROM process_mining.review_embeddings) AS updated_at,
                    (SELECT model FROM process_mining.review_embeddings
                      ORDER BY updated_at DESC NULLS LAST LIMIT 1) AS model
                """
            )
        d = dict(row)
        upd = d.get("updated_at")
        return {
            "ok": True,
            "vectors": d["with_embedding"],
            "total_rows": d["total_rows"],
            "reviewable_articles": d["reviewable_articles"],
            "dim": 4096,
            "model": d.get("model"),
            "updated_at": upd.isoformat() if upd else None,
        }
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)


class HybridSearchIn(BaseModel):
    q: str
    limit: int = 10
    llm_summary: bool = False


@app.get("/api/search/quick")
async def api_search_quick(q: str = "", limit: int = 8):
    """Live-поиск по совпадению слов (pg_trgm/ILIKE), для typeahead."""
    try:
        items = await search_quick(pool, q, min(20, max(1, limit)))
        return {"ok": True, "q": q, "count": len(items), "items": items}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)


@app.post("/api/search/hybrid")
async def api_search_hybrid(body: HybridSearchIn):
    """Гибридный поиск BM25 + вектор (RRF). Опционально LLM-саммари по топу."""
    q = (body.q or "").strip()
    if not q:
        return JSONResponse({"ok": False, "error": "empty query"}, status_code=400)
    try:
        items = await search_hybrid(pool, q, min(20, max(1, body.limit)))
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"search: {type(e).__name__}: {e}"}, status_code=500)

    summary = None
    if body.llm_summary and items:
        try:
            summary = await llm_summary(q, items[:6])
        except Exception as e:
            summary = None
            return {"ok": True, "q": q, "count": len(items), "items": items,
                    "summary": None, "summary_error": f"{type(e).__name__}: {e}"}
    return {"ok": True, "q": q, "count": len(items), "items": items, "summary": summary}
