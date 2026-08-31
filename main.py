"""PMAnalyze API: управление еженедельным пайплайном + дашборд + дайджест."""
import os
import ast
import asyncio
import json
import time
import logging
import asyncpg
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone, date as _date

import uuid
import io
import csv
import tempfile
import zipfile
import sqlite3
import hashlib
import secrets
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from fastapi import FastAPI, BackgroundTasks, Request, Response, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.pipeline import PMAnalyzePipeline, Settings
from app.prompts import CATEGORIES
from app.email_sender import EmailSender
from app.digest import collect_digest, render_digest_html, PRESET_TITLE, render_reviews_markdown, upload_markdown_to_obs
from app.search import search_quick, search_hybrid, llm_summary, search_kg_hybrid, llm_summary_kg, search_kg_quick, LLM_MODEL, EMBED_DIM
from app.kg_runtime import KGRunManager

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
UI_ACCESS_RETENTION_DAYS = int(os.getenv("UI_ACCESS_RETENTION_DAYS", "90"))

# --- Onboarding / визитор-кука (фундамент под Authentik) ---
# Сейчас: идентификация по ip + куке браузера (visitor_id).
# Потом: подхватим user из заголовков Authentik-аутпоста, без переписывания.
ONBOARDING_COOKIE = os.getenv("ONBOARDING_COOKIE", "pma_visitor")
ONBOARDING_COOKIE_MAX_AGE = int(os.getenv("ONBOARDING_COOKIE_MAX_AGE", str(60 * 60 * 24 * 365)))  # 1 год
# Заголовки, которые прокидывает Authentik forward-auth outpost (появятся позже)
AUTHENTIK_HEADER_USERNAME = os.getenv("AUTHENTIK_HEADER_USERNAME", "x-authentik-username")
AUTHENTIK_HEADER_UID = os.getenv("AUTHENTIK_HEADER_UID", "x-authentik-uid")
AUTHENTIK_HEADER_EMAIL = os.getenv("AUTHENTIK_HEADER_EMAIL", "x-authentik-email")
AUTHENTIK_HEADER_GROUPS = os.getenv("AUTHENTIK_HEADER_GROUPS", "x-authentik-groups")

pipeline: PMAnalyzePipeline | None = None
pool: asyncpg.Pool | None = None
mailer: EmailSender | None = None
digest_task: asyncio.Task | None = None
digest_stop: asyncio.Event = asyncio.Event()
kg_manager: KGRunManager | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pipeline, pool, mailer, digest_task, kg_manager
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)
    pipeline = PMAnalyzePipeline(pool, Settings())
    kg_manager = KGRunManager(pool, pipeline)
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
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.ui_access_log (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                ip TEXT NOT NULL,
                user_agent TEXT,
                referer TEXT,
                path TEXT NOT NULL,
                query TEXT
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_ui_access_log_created_at
            ON process_mining.ui_access_log(created_at DESC)
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_ui_access_log_ip
            ON process_mining.ui_access_log(ip)
        """)

        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.llm_query_logs (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                route TEXT NOT NULL,
                search_type TEXT NOT NULL,
                user_id TEXT,
                visitor_id TEXT,
                client_ip TEXT,
                query_text TEXT NOT NULL,
                llm_requested BOOLEAN NOT NULL DEFAULT FALSE,
                llm_called BOOLEAN NOT NULL DEFAULT FALSE,
                model_name TEXT,
                tokens_usage JSONB NOT NULL DEFAULT '{"input_tokens":0,"output_tokens":0,"total_tokens":0}'::jsonb,
                found_count INT,
                used_count INT,
                summary_error TEXT,
                meta JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_query_logs_created_at
            ON process_mining.llm_query_logs(created_at DESC)
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_query_logs_search_type
            ON process_mining.llm_query_logs(search_type)
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_query_logs_user_id
            ON process_mining.llm_query_logs(user_id)
        """)

        # Онбординг-события. Сейчас ключ = visitor_id(кука) + ip.
        # user_id / username / email / groups заполнятся позже из Authentik.
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.ui_onboarding (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                visitor_id TEXT NOT NULL,
                ip TEXT,
                user_id TEXT,
                username TEXT,
                email TEXT,
                groups TEXT,
                event TEXT NOT NULL,
                step INT,
                meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                user_agent TEXT
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_ui_onboarding_visitor
            ON process_mining.ui_onboarding(visitor_id)
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_ui_onboarding_event
            ON process_mining.ui_onboarding(event)
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_ui_onboarding_created_at
            ON process_mining.ui_onboarding(created_at DESC)
        """)

        # Карантин статей в UI (ручные отметки на панели Articles).
        # active=TRUE -> статья помечена к перепроверке/удалению.
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.ui_article_quarantine (
                article_id BIGINT PRIMARY KEY,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                note TEXT,
                marked_by TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT fk_ui_article_quarantine_article
                    FOREIGN KEY(article_id)
                    REFERENCES process_mining.papers_metadata(id)
                    ON DELETE CASCADE
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_ui_article_quarantine_active
            ON process_mining.ui_article_quarantine(active)
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_ui_article_quarantine_updated_at
            ON process_mining.ui_article_quarantine(updated_at DESC)
        """)
        await con.execute("""
            ALTER TABLE process_mining.papers_metadata
            ADD COLUMN IF NOT EXISTS kg_processed BOOLEAN NOT NULL DEFAULT FALSE
        """)
        await con.execute("""
            ALTER TABLE process_mining.papers_metadata
            ADD COLUMN IF NOT EXISTS kg_processed_at TIMESTAMPTZ
        """)

        await con.execute("""
            ALTER TABLE process_mining.wiki_pages
            ADD COLUMN IF NOT EXISTS source_url TEXT
        """)
        await con.execute("""
            UPDATE process_mining.wiki_pages
            SET source_url = (
                SELECT elem
                FROM jsonb_array_elements_text(COALESCE(links, '[]'::jsonb)) AS elem
                WHERE elem ~ '^https?://'
                LIMIT 1
            )
            WHERE COALESCE(source_url, '') = ''
        """)

        await con.execute(f"""
            CREATE TABLE IF NOT EXISTS process_mining.knowledge_embeddings (
                knowledge_id INTEGER PRIMARY KEY
                    REFERENCES process_mining.knowledge(id) ON DELETE CASCADE,
                article_id BIGINT
                    REFERENCES process_mining.papers_metadata(id) ON DELETE SET NULL,
                content TEXT NOT NULL,
                embedding VECTOR({EMBED_DIM}),
                tsv TSVECTOR,
                model TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_knowledge_embeddings_article
            ON process_mining.knowledge_embeddings(article_id)
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_knowledge_embeddings_tsv
            ON process_mining.knowledge_embeddings USING GIN(tsv)
        """)

        await con.execute(f"""
            CREATE TABLE IF NOT EXISTS process_mining.wiki_page_embeddings (
                wiki_page_id INTEGER PRIMARY KEY
                    REFERENCES process_mining.wiki_pages(id) ON DELETE CASCADE,
                content TEXT NOT NULL,
                embedding VECTOR({EMBED_DIM}),
                tsv TSVECTOR,
                model TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_wiki_page_embeddings_tsv
            ON process_mining.wiki_page_embeddings USING GIN(tsv)
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.user_private_kb_keys (
                user_id TEXT PRIMARY KEY,
                secret_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                rotated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.user_private_kb_imports (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                source_url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'created',
                archive_bytes BIGINT,
                files_total INT NOT NULL DEFAULT 0,
                files_parsed INT NOT NULL DEFAULT 0,
                chunks_created INT NOT NULL DEFAULT 0,
                error_text TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_private_kb_imports_user_created
            ON process_mining.user_private_kb_imports(user_id, created_at DESC)
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.user_private_kb_files (
                id BIGSERIAL PRIMARY KEY,
                import_id BIGINT NOT NULL REFERENCES process_mining.user_private_kb_imports(id) ON DELETE CASCADE,
                user_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_ext TEXT,
                file_size BIGINT,
                text_chars INT,
                status TEXT NOT NULL DEFAULT 'parsed',
                error_text TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_private_kb_files_import
            ON process_mining.user_private_kb_files(import_id)
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.user_private_kb_nodes (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                import_id BIGINT NOT NULL REFERENCES process_mining.user_private_kb_imports(id) ON DELETE CASCADE,
                file_id BIGINT REFERENCES process_mining.user_private_kb_files(id) ON DELETE SET NULL,
                node_text TEXT NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_private_kb_nodes_user
            ON process_mining.user_private_kb_nodes(user_id, created_at DESC)
        """)
        # --- private KB graph tables (nodes+edges) + progress columns ---
        await con.execute("ALTER TABLE process_mining.user_private_kb_imports ADD COLUMN IF NOT EXISTS nodes_created INT NOT NULL DEFAULT 0")
        await con.execute("ALTER TABLE process_mining.user_private_kb_imports ADD COLUMN IF NOT EXISTS edges_created INT NOT NULL DEFAULT 0")
        await con.execute("ALTER TABLE process_mining.user_private_kb_imports ADD COLUMN IF NOT EXISTS current_file TEXT")
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.user_private_kb_graph_nodes (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                import_id BIGINT NOT NULL REFERENCES process_mining.user_private_kb_imports(id) ON DELETE CASCADE,
                file_id BIGINT REFERENCES process_mining.user_private_kb_files(id) ON DELETE SET NULL,
                label TEXT,
                text_knowledge TEXT NOT NULL,
                importance REAL NOT NULL DEFAULT 0.5,
                tags JSONB NOT NULL DEFAULT '[]'::jsonb,
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_private_kb_graph_nodes_user
            ON process_mining.user_private_kb_graph_nodes(user_id, import_id)
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS process_mining.user_private_kb_graph_edges (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                import_id BIGINT NOT NULL REFERENCES process_mining.user_private_kb_imports(id) ON DELETE CASCADE,
                source_node_id BIGINT NOT NULL REFERENCES process_mining.user_private_kb_graph_nodes(id) ON DELETE CASCADE,
                target_node_id BIGINT NOT NULL REFERENCES process_mining.user_private_kb_graph_nodes(id) ON DELETE CASCADE,
                relation_type TEXT NOT NULL DEFAULT 'relates_to',
                relevance_score REAL NOT NULL DEFAULT 0.5,
                importance REAL NOT NULL DEFAULT 0.5,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await con.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_private_kb_graph_edges_import
            ON process_mining.user_private_kb_graph_edges(import_id)
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
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


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


def _authors_from_dictlike_string(s):
    """Достаёт имена авторов из строки-словаря без кавычек ключей.

    Примеры входа:
      "{Paul Kobialka: null, Andrea Pferscher: null}"
      "{Xin Su: /search/?searchtype=author&query=Su%2C+X, Jia Wei: /search/...}"

    Логика: имя автора — это подстрока ПЕРЕД ':'. Значение может содержать
    запятые (напр. url-энкод 'Su%2C+X'), поэтому наивный split(',') сломается.
    Мы ищем начало нового автора как ', <ключ>:' — то есть запятую, за которой
    идёт текст до следующего ':'.
    """
    import re as _re
    body = s.strip().strip('{}').strip()
    if not body:
        return []
    # Разбиваем по запятой, ЗА которой следует "ключ:" (ключ без запятых/двоеточий).
    # Разделитель — запятая, после которой идёт непустой текст и ':'.
    parts = _re.split(r',(?=\s*[^,:]+?\s*:)', body)
    names = []
    for p in parts:
        left = p.split(':', 1)[0].strip()
        if left:
            names.append(left)
    return names


def _normalize_authors_payload(raw):
    """Нормализует authors к list[str] для API.

    Поддерживает: list | dict (авторы в ключах) | json-string | python-literal string.
    """
    def clean_list(vals):
        out = []
        seen = set()
        for x in vals or []:
            t = str(x).strip().strip('"\'')
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
        # Нормальный кейс: массив готовых имён.
        joined = ", ".join(str(x) for x in raw)
        stripped = joined.strip()
        # Битый кейс из БД: массив — это по факту разорванный по запятым
        # dict-literal вида
        #   ["{Xin Su: /search/...Su%2C+X", " Jia Wei: /search/...", " Chun Ouyang: ...}"]
        # или
        #   ["{Paul Kobialka: null", " Andrea Pferscher: null", ... "}"]
        # Признак: склеенная строка начинается с "{" и заканчивается "}",
        # а в сегментах есть "ключ: значение".
        if stripped.startswith('{') and stripped.endswith('}'):
            names = _authors_from_dictlike_string(stripped)
            if names:
                return clean_list(names)
        return clean_list(raw)

    if isinstance(raw, dict):
        # Частый кейс: авторы лежат прямо в ключах dict.
        keys = [k for k in raw.keys() if isinstance(k, str) and k.strip()]
        if keys:
            return clean_list(keys)
        for key in ('list', 'LIST', 'authors', 'Authors'):
            v = raw.get(key)
            if isinstance(v, list):
                return clean_list(v)
        return []

    if isinstance(raw, str):
        t = raw.strip()
        if not t:
            return []

        # JSON-строка объекта/массива.
        if (t.startswith('{') and t.endswith('}')) or (t.startswith('[') and t.endswith(']')):
            try:
                return _normalize_authors_payload(json.loads(t))
            except Exception:
                pass
            # fallback для python-literal вида {'A': 1, 'B': 1}
            try:
                return _normalize_authors_payload(ast.literal_eval(t))
            except Exception:
                pass
            # fallback для dict-like без кавычек ключей:
            #   "{Xin Su: /search/...Su%2C+X, Jia Wei: ...}"
            names = _authors_from_dictlike_string(t)
            if names:
                return clean_list(names)

        parts = [x.strip() for x in t.replace(';', ',').split(',')]
        return clean_list(parts)

    return clean_list([raw])


def _extract_client_ip(request: Request) -> str:
    xff = (request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        return xff.split(",")[0].strip()[:64]
    xrip = (request.headers.get("x-real-ip") or "").strip()
    if xrip:
        return xrip[:64]
    if request.client and request.client.host:
        return str(request.client.host)[:64]
    return "unknown"


async def _log_ui_access(request: Request):
    try:
        ip = _extract_client_ip(request)
        ua = (request.headers.get("user-agent") or "")[:512]
        ref = (request.headers.get("referer") or "")[:1024]
        path = (request.url.path or "")[:256]
        query = (request.url.query or "")[:1024]
        retention = max(1, int(UI_ACCESS_RETENTION_DAYS or 90))

        async with pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO process_mining.ui_access_log(ip, user_agent, referer, path, query)
                VALUES ($1, $2, $3, $4, $5)
                """,
                ip, ua, ref, path, query,
            )
            await con.execute(
                """
                DELETE FROM process_mining.ui_access_log
                WHERE created_at < NOW() - make_interval(days => $1)
                """,
                retention,
            )
    except Exception as e:
        api_logger.warning(f"ui_access_log_error: {e}")


async def _log_llm_query(
    request: Request,
    *,
    route: str,
    search_type: str,
    query_text: str,
    llm_requested: bool,
    llm_called: bool,
    model_name: str | None,
    tokens_usage: dict | None,
    found_count: int | None = None,
    used_count: int | None = None,
    summary_error: str | None = None,
    meta: dict | None = None,
):
    """Логирование пользовательских запросов к поиску/LLM в process_mining.llm_query_logs."""
    try:
        ident = current_identity(request)
        visitor_id = _get_visitor_id(request)
        ip = _extract_client_ip(request)
        uid = ident.get("user_id") or None

        q = (query_text or "").strip()[:8000]
        se = (summary_error or "").strip()[:2000] or None

        tu = tokens_usage or {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        try:
            tu_json = json.dumps({
                "input_tokens": int(tu.get("input_tokens", 0) or 0),
                "output_tokens": int(tu.get("output_tokens", 0) or 0),
                "total_tokens": int(tu.get("total_tokens", 0) or 0),
            }, ensure_ascii=False)
        except Exception:
            tu_json = '{"input_tokens":0,"output_tokens":0,"total_tokens":0}'

        meta_json = json.dumps(meta or {}, ensure_ascii=False)

        async with pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO process_mining.llm_query_logs(
                    route, search_type, user_id, visitor_id, client_ip,
                    query_text, llm_requested, llm_called, model_name,
                    tokens_usage, found_count, used_count, summary_error, meta
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14::jsonb)
                """,
                (route or "")[:128],
                (search_type or "other")[:32],
                str(uid)[:128] if uid is not None else None,
                (visitor_id or "")[:128] or None,
                (ip or "")[:64] or None,
                q,
                bool(llm_requested),
                bool(llm_called),
                (model_name or "")[:128] or None,
                tu_json,
                int(found_count) if found_count is not None else None,
                int(used_count) if used_count is not None else None,
                se,
                meta_json,
            )
    except Exception as e:
        api_logger.warning(f"llm_query_log_error: {e}")


# ---------------- Identity / Onboarding (фундамент под Authentik) ----------------
def current_identity(request: Request) -> dict:
    """Текущий пользователь.

    Сейчас Authentik ещё нет — заголовков не будет, вернётся анонимный dict.
    Когда включат forward-auth outpost, аутпост начнёт прокидывать
    X-authentik-* заголовки, и этот же код автоматически заполнит поля.
    Логику вызова менять не потребуется.
    """
    h = request.headers
    username = (h.get(AUTHENTIK_HEADER_USERNAME) or "").strip() or None
    uid = (h.get(AUTHENTIK_HEADER_UID) or "").strip() or None
    email = (h.get(AUTHENTIK_HEADER_EMAIL) or "").strip() or None
    groups = (h.get(AUTHENTIK_HEADER_GROUPS) or "").strip() or None
    return {
        "authenticated": bool(username or uid),
        "user_id": uid,
        "username": username,
        "email": email,
        "groups": groups,
    }


def _get_visitor_id(request: Request) -> str | None:
    v = (request.cookies.get(ONBOARDING_COOKIE) or "").strip()
    return v or None


def _ensure_visitor_cookie(request: Request, response: Response) -> str:
    """Читает visitor_id из куки, если нет — генерит и ставит куку."""
    vid = _get_visitor_id(request)
    if not vid:
        vid = uuid.uuid4().hex
        response.set_cookie(
            key=ONBOARDING_COOKIE,
            value=vid,
            max_age=ONBOARDING_COOKIE_MAX_AGE,
            httponly=False,   # фронт может прочитать при желании; не секрет
            samesite="lax",
            path="/",
        )
    return vid


# ---------------- Dashboard (Jinja-free: статичный HTML) ----------------
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    await _log_ui_access(request)
    resp = HTMLResponse("")
    _ensure_visitor_cookie(request, resp)
    with open(os.path.join(TEMPLATES_DIR, "dashboard.html"), encoding="utf-8") as f:
        html = f.read()
    # Склейка партиала онбординг-тура перед </body>.
    onb_path = os.path.join(TEMPLATES_DIR, "_onboarding.html")
    if os.path.exists(onb_path):
        with open(onb_path, encoding="utf-8") as f:
            onb_html = f.read()
        html = html.replace("</body>", onb_html + "\n</body>", 1)
    resp.body = html.encode("utf-8")
    resp.headers["content-length"] = str(len(resp.body))
    return resp


# ---------------- Onboarding API ----------------
class OnboardingEventReq(BaseModel):
    event: str                     # 'shown' | 'completed' | 'skipped' | 'step_view'
    step: int | None = None
    meta: dict | None = None


_ONB_TERMINAL_EVENTS = ("completed", "skipped")
_ONB_ALLOWED_EVENTS = ("shown", "completed", "skipped", "step_view")


@app.get("/api/onboarding/status")
async def onboarding_status(request: Request):
    """Показывать ли онбординг. Показ один раз: после completed/skipped — нет."""
    resp_cookie = Response()
    vid = _ensure_visitor_cookie(request, resp_cookie)
    ident = current_identity(request)

    async with pool.acquire() as con:
        row = await con.fetchrow(
            """
            SELECT 1
            FROM process_mining.ui_onboarding
            WHERE visitor_id = $1 AND event = ANY($2::text[])
            LIMIT 1
            """,
            vid, list(_ONB_TERMINAL_EVENTS),
        )
    seen = row is not None
    payload = {
        "ok": True,
        "visitor_id": vid,
        "should_show": not seen,
        "authenticated": ident["authenticated"],
        "username": ident["username"],
    }
    out = JSONResponse(payload)
    # переносим Set-Cookie, если кука только что создана
    sc = resp_cookie.headers.get("set-cookie")
    if sc:
        out.headers["set-cookie"] = sc
    return out


@app.post("/api/onboarding/event")
async def onboarding_event(req: OnboardingEventReq, request: Request):
    if req.event not in _ONB_ALLOWED_EVENTS:
        return JSONResponse({"ok": False, "error": "bad_event"}, status_code=400)

    resp_cookie = Response()
    vid = _ensure_visitor_cookie(request, resp_cookie)
    ident = current_identity(request)
    ip = _extract_client_ip(request)
    ua = (request.headers.get("user-agent") or "")[:512]
    meta_json = json.dumps(req.meta or {}, ensure_ascii=False)

    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO process_mining.ui_onboarding
                (visitor_id, ip, user_id, username, email, groups, event, step, meta, user_agent)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
            """,
            vid, ip,
            ident["user_id"], ident["username"], ident["email"], ident["groups"],
            req.event, req.step, meta_json, ua,
        )

    out = JSONResponse({"ok": True, "visitor_id": vid, "event": req.event})
    sc = resp_cookie.headers.get("set-cookie")
    if sc:
        out.headers["set-cookie"] = sc
    return out


@app.get("/api/me")
async def me(request: Request):
    """Кто я сейчас. До Authentik — аноним + visitor_id. Полезно фронту/дебагу."""
    ident = current_identity(request)
    return {"ok": True, "visitor_id": _get_visitor_id(request), **ident}


@app.get("/health")
async def health():
    return {"ok": True, "service": "PMAnalyze"}


# ---------------- Private KB import (Google Drive ZIP <= 10MB) ----------------
PRIVATE_KB_ARCHIVE_MAX_BYTES = int(os.getenv("PRIVATE_KB_ARCHIVE_MAX_BYTES", str(10 * 1024 * 1024)))
PRIVATE_KB_MAX_TEXT_CHARS = int(os.getenv("PRIVATE_KB_MAX_TEXT_CHARS", "50000"))
PRIVATE_KB_SECRET_SALT = os.getenv("PRIVATE_KB_SECRET_SALT", "pm-private-kb-salt")
_PRIVATE_KB_ALLOWED_EXT = {".pdf", ".docx", ".md", ".txt"}


class PrivateKBDriveImportReq(BaseModel):
    drive_url: str = Field(min_length=10, max_length=3000)
    regenerate_secret: bool = False


class PrivateKBVerifyReq(BaseModel):
    secret_code: str = Field(min_length=6, max_length=128)


def _require_user_id(request: Request) -> str:
    ident = current_identity(request)
    uid = (ident.get("user_id") or "").strip()
    if uid:
        return uid
    # DEV-fallback: Authentik not wired yet -> derive stable id from visitor cookie / IP
    vid = _get_visitor_id(request)
    if vid:
        return "dev-" + vid
    ip = _extract_client_ip(request) or "local"
    return "dev-" + str(ip)


def _extract_gdrive_file_id(url: str) -> str | None:
    u = (url or "").strip()
    if not u:
        return None
    # https://drive.google.com/file/d/<ID>/view
    m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", u)
    if m:
        return m.group(1)
    # https://drive.google.com/open?id=<ID> or uc?id=<ID>
    try:
        q = parse_qs(urlparse(u).query)
        if q.get("id"):
            return q["id"][0]
    except Exception:
        pass
    return None


def _hash_secret(secret_code: str, salt: str) -> str:
    return hashlib.sha256((salt + "::" + secret_code).encode("utf-8")).hexdigest()


def _generate_secret_code() -> str:
    # ~20 символов base62/url-safe без спецсимволов
    return secrets.token_urlsafe(16).replace("-", "A").replace("_", "B")[:20]


def _chunk_text(text: str, chunk_size: int = 1200, overlap: int = 200) -> list[str]:
    t = (text or "").strip()
    if not t:
        return []
    out = []
    step = max(200, chunk_size - overlap)
    i = 0
    while i < len(t):
        part = t[i:i + chunk_size].strip()
        if len(part) >= 80:
            out.append(part)
        i += step
    return out


def _read_txt_md(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            return raw.decode(enc, errors="ignore")
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def _read_docx(path: Path) -> str:
    try:
        from docx import Document  # type: ignore
    except Exception as e:
        raise RuntimeError(f"python-docx недоступен: {e}")
    try:
        if path.stat().st_size < 4000:
            import zipfile as _zf
            if not _zf.is_zipfile(str(path)):
                raise RuntimeError("файл не является валидным .docx (пустой или битый архив)")
        d = Document(str(path))
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"не удалось открыть .docx (возможно старый .doc или повреждён): {type(e).__name__}")
    return "\n".join((p.text or "").strip() for p in d.paragraphs if (p.text or "").strip())


def _read_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as e:
        raise RuntimeError(f"pypdf недоступен: {e}")
    r = PdfReader(str(path))
    pages = []
    for pg in r.pages:
        try:
            pages.append(pg.extract_text() or "")
        except Exception:
            continue
    return "\n".join(pages)


def _pkb_fix_zip_name(info) -> str:
    """Чинит имена из zip: если флаг UTF-8 не выставлен, zipfile декодит как cp437.
    Возвращаем корректную строку (пробуем cp866/utf-8 для кириллицы)."""
    name = info.filename
    try:
        # бит 0x800 = имя уже в UTF-8, чинить не нужно
        if getattr(info, "flag_bits", 0) & 0x800:
            return name
        raw = name.encode("cp437", errors="replace")
        for enc in ("cp866", "utf-8", "cp1251"):
            try:
                dec = raw.decode(enc)
                # эвристика: если получили осмысленную кириллицу/латиницу без replacement-символов
                if "\ufffd" not in dec:
                    return dec
            except Exception:
                continue
        return raw.decode("utf-8", errors="ignore") or name
    except Exception:
        return name


def _safe_extract_zip(zip_path: Path, out_dir: Path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            fixed_name = _pkb_fix_zip_name(info)
            info.filename = fixed_name  # чтобы дальнейшая логика видела нормальное имя
            # анти zip-slip
            target = (out_dir / fixed_name).resolve()
            if not str(target).startswith(str(out_dir.resolve())):
                continue
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, open(target, "wb") as dst:
                dst.write(src.read())


async def _download_gdrive_zip(url: str) -> tuple[bytes, str]:
    fid = _extract_gdrive_file_id(url)
    if not fid:
        raise HTTPException(status_code=400, detail="Не удалось извлечь file_id из ссылки Google Drive")
    direct_url = f"https://drive.google.com/uc?export=download&id={fid}"
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as cli:
        r = await cli.get(direct_url)
        if r.status_code >= 400:
            raise HTTPException(status_code=400, detail=f"Google Drive вернул статус {r.status_code}")
        content = r.content
        if not content:
            raise HTTPException(status_code=400, detail="Пустой ответ при скачивании архива")
        if len(content) > PRIVATE_KB_ARCHIVE_MAX_BYTES:
            raise HTTPException(status_code=413, detail=f"Архив больше лимита {PRIVATE_KB_ARCHIVE_MAX_BYTES} байт")
        return content, direct_url


# ---------------- Private KB: async import with progress ----------------
_PRIVATE_KB_JOBS: dict[int, dict] = {}


from app.kg_prompts import build_knowledge_extraction_prompt as _pkb_build_extract
from app.kg_prompts import build_relation_inference_prompt as _pkb_build_rel
from app.kg_prompts import RELATION_TYPES as _PKB_REL_TYPES
from app.kg_prompts import build_cross_link_prompt as _pkb_build_cross
import hashlib as _pkb_hashlib



_PKB_CROSS_MIN_VEC_SIM = float(os.getenv("PKB_CROSS_MIN_VEC_SIM", "0.75"))
_PKB_CROSS_TOP_K = int(os.getenv("PKB_CROSS_TOP_K", "8"))


def _pkb_fname_hash(name: str) -> str:
    """Хеш нормализованного имени документа для дедупа дублей."""
    base = (name or "").strip().lower()
    # берём только имя файла без пути
    if "/" in base:
        base = base.rsplit("/", 1)[-1]
    if "\\" in base:
        base = base.rsplit("\\", 1)[-1]
    return _pkb_hashlib.md5(base.encode("utf-8", "ignore")).hexdigest()


async def _pkb_cross_link(import_id: int, user_id: str, allowed_rel: set, job: dict) -> int:
    """Глобальная фаза: межфайловые связи по всем узлам импорта.

    Возвращает число созданных cross-рёбер. Никогда не бросает наружу —
    ошибки складываются в job['errors'].
    """
    created = 0
    try:
        async with pool.acquire() as con:
            nrows = await con.fetch(
                "SELECT n.id, n.file_id, n.text_knowledge, "
                "COALESCE(f.file_name, '') AS file_name "
                "FROM process_mining.user_private_kb_graph_nodes n "
                "LEFT JOIN process_mining.user_private_kb_files f ON f.id = n.file_id "
                "WHERE n.user_id=$1 AND n.import_id=$2 ORDER BY n.id",
                user_id, import_id,
            )
        nodes = [dict(r) for r in nrows]
        if len(nodes) < 2:
            return 0

        # дедуп документов по хешу имени: effective_doc = хеш имени файла.
        # узлы с одинаковым doc-хешем считаются ОДНИМ документом.
        for nd in nodes:
            nd["doc_hash"] = _pkb_fname_hash(nd.get("file_name") or ("fid:" + str(nd.get("file_id"))))

        texts = [((nd.get("text_knowledge") or "").strip())[:30000] for nd in nodes]

        # батч-эмбеддинги всех узлов одним запросом (как в search-эндпоинте)
        import httpx as _hx
        from app.search import EMBED_BASE as _EB, EMBED_KEY as _EK, EMBED_MODEL as _EM
        async with _hx.AsyncClient(timeout=120) as cl:
            resp = await cl.post(
                _EB + "/embeddings",
                headers={"Authorization": "Bearer " + _EK, "Content-Type": "application/json"},
                json={"model": _EM, "input": texts},
            )
            resp.raise_for_status()
            emb_data = resp.json()["data"]
        vecs = {int(nodes[i]["id"]): emb_data[i]["embedding"] for i in range(len(nodes))}

        by_id = {int(nd["id"]): nd for nd in nodes}

        # уже существующие рёбра импорта -> для дедупа
        async with pool.acquire() as con:
            erows = await con.fetch(
                "SELECT source_node_id, target_node_id, relation_type "
                "FROM process_mining.user_private_kb_graph_edges "
                "WHERE user_id=$1 AND import_id=$2",
                user_id, import_id,
            )
        seen = set()
        for e in erows:
            seen.add((int(e["source_node_id"]), int(e["target_node_id"]), e["relation_type"]))

        for nd in nodes:
            new_id = int(nd["id"])
            new_text = (nd.get("text_knowledge") or "").strip()
            if len(new_text) < 20:
                continue
            qv = vecs.get(new_id) or []
            if not qv:
                continue

            # кандидаты из ДРУГИХ документов (иной doc_hash) с косинусом >= порога
            scored = []
            for other in nodes:
                oid = int(other["id"])
                if oid == new_id:
                    continue
                if other["doc_hash"] == nd["doc_hash"]:
                    continue  # тот же документ (или дубль по имени) -> пропускаем
                sim = _pkb_cos(qv, vecs.get(oid) or [])
                if sim >= _PKB_CROSS_MIN_VEC_SIM:
                    scored.append((oid, sim))
            if not scored:
                continue
            scored.sort(key=lambda x: x[1], reverse=True)
            cand_ids = [oid for oid, _ in scored[:_PKB_CROSS_TOP_K]]
            cand_nodes = []
            for cid in cand_ids:
                ct = (by_id[cid].get("text_knowledge") or "").strip()
                if len(ct) >= 20:
                    cand_nodes.append({"id": cid, "text_knowledge": ct})
            if not cand_nodes:
                continue
            cand_set = {int(c["id"]) for c in cand_nodes}

            cross_prompt = _pkb_build_cross(
                json.dumps({"id": new_id, "text_knowledge": new_text}, ensure_ascii=False),
                json.dumps(cand_nodes, ensure_ascii=False),
            )
            try:
                cross_raw = await pipeline._llm(pipeline.settings.review_model, cross_prompt, max_tokens=1800)
                cross_data = pipeline._extract_json(cross_raw) or {}
            except Exception as e:
                job.setdefault("errors", []).append({"file": None, "error": "cross_llm: " + str(e)[:400]})
                continue
            rel_cross = cross_data.get("relations") if isinstance(cross_data, dict) else []
            if not isinstance(rel_cross, list):
                continue

            for rr in rel_cross[:60]:
                if not isinstance(rr, dict):
                    continue
                try:
                    sid = int(rr.get("source_id"))
                    tid = int(rr.get("target_id"))
                except Exception:
                    continue
                if sid == tid:
                    continue
                # строго A(new) <-> B(candidate)
                if not ((sid == new_id and tid in cand_set) or (tid == new_id and sid in cand_set)):
                    continue
                rtype = str(rr.get("relation_type") or "relates_to").strip()
                if rtype not in allowed_rel:
                    rtype = "relates_to"
                try:
                    rscore = max(0.0, min(1.0, float(rr.get("relevance_score", 0.5) or 0.5)))
                except Exception:
                    rscore = 0.5
                try:
                    rimp = max(0.0, min(1.0, float(rr.get("importance", 0.5) or 0.5)))
                except Exception:
                    rimp = 0.5
                key = (sid, tid, rtype)
                if key in seen:
                    continue
                seen.add(key)
                async with pool.acquire() as con:
                    await con.execute(
                        "INSERT INTO process_mining.user_private_kb_graph_edges("
                        "user_id, import_id, source_node_id, target_node_id, "
                        "relation_type, relevance_score, importance) "
                        "VALUES($1,$2,$3,$4,$5,$6,$7)",
                        user_id, import_id, sid, tid, rtype, rscore, rimp,
                    )
                created += 1
                job["edges_created"] = job.get("edges_created", 0) + 1
    except Exception as e:
        job.setdefault("errors", []).append({"file": None, "error": "cross_link: " + str(e)[:600]})
    return created


async def _pkb_process_import(import_id: int, user_id: str, drive_url: str):
    job = _PRIVATE_KB_JOBS.setdefault(import_id, {})
    job.update({"status": "downloading", "files_total": 0, "files_done": 0,
                "current_file": None, "nodes_created": 0, "edges_created": 0, "errors": []})
    allowed_rel = set(_PKB_REL_TYPES)
    try:
        archive_bytes, resolved_url = await _download_gdrive_zip(drive_url)
        async with pool.acquire() as con:
            await con.execute(
                "UPDATE process_mining.user_private_kb_imports SET status=$2, source_url=$3, archive_bytes=$4, updated_at=NOW() WHERE id=$1",
                import_id, "unzipping", resolved_url, len(archive_bytes),
            )

        with tempfile.TemporaryDirectory(prefix="pkb_") as td:
            zpath = Path(td) / "bundle.zip"
            xdir = Path(td) / "unzipped"
            xdir.mkdir(parents=True, exist_ok=True)
            zpath.write_bytes(archive_bytes)
            _safe_extract_zip(zpath, xdir)

            files = [fp for fp in xdir.rglob("*") if fp.is_file() and fp.suffix.lower() in _PRIVATE_KB_ALLOWED_EXT]
            if not files:
                raise RuntimeError("В архиве не найдено поддерживаемых файлов: pdf/docx/md/txt")

            job["files_total"] = len(files)
            job["status"] = "processing"
            async with pool.acquire() as con:
                await con.execute(
                    "UPDATE process_mining.user_private_kb_imports SET status=$2, files_total=$3, updated_at=NOW() WHERE id=$1",
                    import_id, "processing", len(files),
                )

            files_parsed = 0
            total_nodes = 0
            total_edges = 0

            for fp in files:
                fname = str(fp.relative_to(xdir))[:500]
                job["current_file"] = fname
                async with pool.acquire() as con:
                    await con.execute(
                        "UPDATE process_mining.user_private_kb_imports SET current_file=$2, updated_at=NOW() WHERE id=$1",
                        import_id, fname,
                    )
                ext = fp.suffix.lower()
                size_b = fp.stat().st_size
                fstatus = "parsed"
                ferr = None
                txt = ""
                try:
                    if ext in (".txt", ".md"):
                        txt = _read_txt_md(fp)
                    elif ext == ".docx":
                        txt = _read_docx(fp)
                    elif ext == ".pdf":
                        txt = _read_pdf(fp)
                except Exception as e:
                    fstatus = "error"
                    ferr = str(e)[:1000]

                txt = (txt or "").strip()
                if len(txt) > PRIVATE_KB_MAX_TEXT_CHARS:
                    txt = txt[:PRIVATE_KB_MAX_TEXT_CHARS]
                text_chars = len(txt)

                async with pool.acquire() as con:
                    file_id = await con.fetchval(
                        "INSERT INTO process_mining.user_private_kb_files("
                        "import_id, user_id, file_name, file_ext, file_size, text_chars, status, error_text) "
                        "VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
                        import_id, user_id, fname, ext, size_b, text_chars, fstatus, ferr,
                    )

                if fstatus != "parsed" or text_chars < 80:
                    if ferr:
                        job["errors"].append({"file": fname, "error": ferr})
                    job["files_done"] += 1
                    continue

                files_parsed += 1

                try:
                    source_meta = json.dumps({"file_name": fp.name, "scope": "private_kb", "user_id": user_id}, ensure_ascii=False)
                    ex_prompt = _pkb_build_extract(source_text=txt, source_meta=source_meta)
                    ex_raw = await pipeline._llm(pipeline.settings.review_model, ex_prompt, max_tokens=2200)
                    ex_data = pipeline._extract_json(ex_raw) or {}
                    atoms = ex_data.get("atoms") if isinstance(ex_data, dict) else []
                    if not isinstance(atoms, list):
                        atoms = []
                except Exception as e:
                    job["errors"].append({"file": fname, "error": "extraction: " + str(e)})
                    atoms = []

                inserted_nodes = []
                for atom in atoms[:30]:
                    if not isinstance(atom, dict):
                        continue
                    text_k = str(atom.get("text_knowledge") or "").strip()
                    if len(text_k) < 20:
                        continue
                    try:
                        imp = max(0.0, min(1.0, float(atom.get("importance", 0.5) or 0.5)))
                    except Exception:
                        imp = 0.5
                    tags = atom.get("tags") if isinstance(atom.get("tags"), list) else []
                    label = text_k[:80]
                    md = {"file_name": fp.name, "import_id": int(import_id), "kind": "private_kb_node"}
                    async with pool.acquire() as con:
                        nid = await con.fetchval(
                            "INSERT INTO process_mining.user_private_kb_graph_nodes("
                            "user_id, import_id, file_id, label, text_knowledge, importance, tags, metadata) "
                            "VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb) RETURNING id",
                            user_id, import_id, file_id, label, text_k, imp,
                            json.dumps(tags, ensure_ascii=False), json.dumps(md, ensure_ascii=False),
                        )
                    inserted_nodes.append({"id": int(nid), "text": text_k})
                    total_nodes += 1
                    job["nodes_created"] = total_nodes

                if len(inserted_nodes) >= 2:
                    try:
                        rel_prompt = _pkb_build_rel(json.dumps(inserted_nodes, ensure_ascii=False))
                        rel_raw = await pipeline._llm(pipeline.settings.review_model, rel_prompt, max_tokens=1800)
                        rel_data = pipeline._extract_json(rel_raw) or {}
                        rels = rel_data.get("relations") if isinstance(rel_data, dict) else []
                        if not isinstance(rels, list):
                            rels = []
                    except Exception as e:
                        job["errors"].append({"file": fname, "error": "relations: " + str(e)})
                        rels = []
                    id_set = {n["id"] for n in inserted_nodes}
                    for rr in rels[:120]:
                        if not isinstance(rr, dict):
                            continue
                        try:
                            sid = int(rr.get("source_id"))
                            tid = int(rr.get("target_id"))
                        except Exception:
                            continue
                        if sid == tid or sid not in id_set or tid not in id_set:
                            continue
                        rtype = str(rr.get("relation_type") or "relates_to").strip()
                        if rtype not in allowed_rel:
                            rtype = "relates_to"
                        try:
                            rscore = max(0.0, min(1.0, float(rr.get("relevance_score", 0.5) or 0.5)))
                        except Exception:
                            rscore = 0.5
                        try:
                            rimp = max(0.0, min(1.0, float(rr.get("importance", 0.5) or 0.5)))
                        except Exception:
                            rimp = 0.5
                        async with pool.acquire() as con:
                            await con.execute(
                                "INSERT INTO process_mining.user_private_kb_graph_edges("
                                "user_id, import_id, source_node_id, target_node_id, "
                                "relation_type, relevance_score, importance) "
                                "VALUES($1,$2,$3,$4,$5,$6,$7)",
                                user_id, import_id, sid, tid, rtype, rscore, rimp,
                            )
                        total_edges += 1
                        job["edges_created"] = total_edges

                job["files_done"] += 1
                async with pool.acquire() as con:
                    await con.execute(
                        "UPDATE process_mining.user_private_kb_imports "
                        "SET files_parsed=$2, nodes_created=$3, edges_created=$4, updated_at=NOW() WHERE id=$1",
                        import_id, files_parsed, total_nodes, total_edges,
                    )

            # --- ГЛОБАЛЬНАЯ ФАЗА: межфайловые связи (cross-file linking) ---
            job["status"] = "linking"
            async with pool.acquire() as con:
                await con.execute(
                    "UPDATE process_mining.user_private_kb_imports SET status='linking', current_file=NULL, updated_at=NOW() WHERE id=$1",
                    import_id,
                )
            cross_edges = await _pkb_cross_link(import_id, user_id, allowed_rel, job)
            total_edges += cross_edges

            async with pool.acquire() as con:
                await con.execute(
                    "UPDATE process_mining.user_private_kb_imports "
                    "SET status='done', current_file=NULL, files_parsed=$2, "
                    "nodes_created=$3, edges_created=$4, updated_at=NOW() WHERE id=$1",
                    import_id, files_parsed, total_nodes, total_edges,
                )
            job["status"] = "done"
            job["current_file"] = None
    except Exception as e:
        emsg = (type(e).__name__ + ": " + str(e))[:2000]
        job["status"] = "error"
        job["errors"].append({"file": None, "error": emsg})
        try:
            async with pool.acquire() as con:
                await con.execute(
                    "UPDATE process_mining.user_private_kb_imports SET status='error', error_text=$2, updated_at=NOW() WHERE id=$1",
                    import_id, emsg,
                )
        except Exception:
            pass


@app.post("/api/private-kb/import/start")
async def private_kb_import_start(req: PrivateKBDriveImportReq, request: Request):
    user_id = _require_user_id(request)
    async with pool.acquire() as con:
        has_key = await con.fetchval(
            "SELECT 1 FROM process_mining.user_private_kb_keys WHERE user_id=$1 AND is_active=TRUE", user_id
        )
        if not has_key:
            raise HTTPException(status_code=403, detail="Секретный ключ не инициализирован. Сначала сгенерируйте ключ.")
        import_id = await con.fetchval(
            "INSERT INTO process_mining.user_private_kb_imports(user_id, source_url, status, updated_at) "
            "VALUES($1,$2,'created',NOW()) RETURNING id",
            user_id, req.drive_url.strip(),
        )
    asyncio.create_task(_pkb_process_import(int(import_id), user_id, req.drive_url.strip()))
    return {"ok": True, "import_id": int(import_id), "status": "created"}


@app.get("/api/private-kb/import/status")
async def private_kb_import_status(request: Request, import_id: int):
    user_id = _require_user_id(request)
    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT id, status, files_total, files_parsed, nodes_created, edges_created, "
            "current_file, error_text, updated_at "
            "FROM process_mining.user_private_kb_imports WHERE id=$1 AND user_id=$2",
            import_id, user_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="import not found")
    job = _PRIVATE_KB_JOBS.get(int(import_id), {})
    return {
        "ok": True,
        "import_id": int(row["id"]),
        "status": row["status"],
        "files_total": int(row["files_total"] or 0),
        "files_done": int(job.get("files_done", row["files_parsed"] or 0)),
        "files_parsed": int(row["files_parsed"] or 0),
        "nodes_created": int(row["nodes_created"] or 0),
        "edges_created": int(row["edges_created"] or 0),
        "current_file": job.get("current_file", row["current_file"]),
        "errors": job.get("errors", []),
        "error_text": row["error_text"],
    }


@app.post("/api/private-kb/secret/generate")
async def private_kb_secret_generate(request: Request):
    user_id = _require_user_id(request)
    issued_secret = _generate_secret_code()
    salt = secrets.token_hex(8)
    sh = _hash_secret(issued_secret, PRIVATE_KB_SECRET_SALT + ":" + salt)
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO process_mining.user_private_kb_keys(user_id, secret_hash, salt, is_active, updated_at, rotated_at) "
            "VALUES($1,$2,$3,TRUE,NOW(),NOW()) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "secret_hash=EXCLUDED.secret_hash, salt=EXCLUDED.salt, "
            "is_active=TRUE, updated_at=NOW(), rotated_at=NOW()",
            user_id, sh, salt,
        )
    return {"ok": True, "secret_code": issued_secret,
            "note": "Показывается один раз. Сохраните ключ — восстановить его нельзя."}


@app.post("/api/private-kb/verify")
async def private_kb_verify(req: PrivateKBVerifyReq, request: Request):
    user_id = _require_user_id(request)
    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT secret_hash, salt, is_active FROM process_mining.user_private_kb_keys WHERE user_id=$1",
            user_id,
        )
    if not row or not row.get("is_active"):
        return {"ok": False, "verified": False, "reason": "secret_not_initialized"}
    expected = row["secret_hash"]
    salt = row["salt"]
    got = _hash_secret(req.secret_code.strip(), PRIVATE_KB_SECRET_SALT + ":" + salt)
    return {"ok": True, "verified": secrets.compare_digest(expected, got)}


@app.post("/api/private-kb/nodes")
async def private_kb_nodes(req: PrivateKBVerifyReq, request: Request, limit: int = 50, offset: int = 0):
    user_id = _require_user_id(request)
    limit = max(1, min(200, int(limit or 50)))
    offset = max(0, int(offset or 0))

    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT secret_hash, salt, is_active FROM process_mining.user_private_kb_keys WHERE user_id=$1",
            user_id,
        )
        if not row or not row.get("is_active"):
            return {"ok": False, "error": "secret_not_initialized"}
        got = _hash_secret(req.secret_code.strip(), PRIVATE_KB_SECRET_SALT + ":" + row["salt"])
        if not secrets.compare_digest(row["secret_hash"], got):
            return {"ok": False, "error": "forbidden"}

        total = await con.fetchval(
            "SELECT COUNT(*)::int FROM process_mining.user_private_kb_nodes WHERE user_id=$1",
            user_id,
        )
        rows = await con.fetch(
            """
            SELECT id, import_id, file_id, node_text, metadata, created_at
            FROM process_mining.user_private_kb_nodes
            WHERE user_id=$1
            ORDER BY id DESC
            LIMIT $2 OFFSET $3
            """,
            user_id, limit, offset,
        )

    items = []
    for r in rows:
        items.append({
            "id": int(r["id"]),
            "import_id": int(r["import_id"]),
            "file_id": int(r["file_id"]) if r.get("file_id") is not None else None,
            "text": str(r["node_text"] or ""),
            "metadata": r["metadata"] if isinstance(r["metadata"], dict) else {},
            "created_at": str(r["created_at"]),
        })
    return {"ok": True, "user_id": user_id, "total": int(total or 0), "limit": limit, "offset": offset, "items": items}


@app.get("/api/private-kb/stats")
async def private_kb_stats(request: Request):
    user_id = _require_user_id(request)
    async with pool.acquire() as con:
        imports = await con.fetchrow(
            """
            SELECT COUNT(*)::int AS total_imports,
                   COALESCE(SUM(files_total),0)::int AS files_total,
                   COALESCE(SUM(files_parsed),0)::int AS files_parsed,
                   COALESCE(SUM(chunks_created),0)::int AS chunks_created
            FROM process_mining.user_private_kb_imports
            WHERE user_id=$1 AND status='done'
            """,
            user_id,
        )
        key_exists = await con.fetchval(
            "SELECT 1 FROM process_mining.user_private_kb_keys WHERE user_id=$1 AND is_active=TRUE",
            user_id,
        )
    return {"ok": True, "user_id": user_id, "has_secret": bool(key_exists), **dict(imports or {})}


# ==== PKB GRAPH ENDPOINTS ====
import math as _pkb_math


def _pkb_cos(a: list, b: list) -> float:
    if not a or not b:
        return 0.0
    s = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        s += x * y
        na += x * x
        nb += y * y
    if na <= 0 or nb <= 0:
        return 0.0
    return s / (_pkb_math.sqrt(na) * _pkb_math.sqrt(nb))


async def _pkb_load_graph(user_id: str) -> dict:
    """Все узлы/рёбра всех импортов юзера -> payload для vis-network."""
    async with pool.acquire() as con:
        nrows = await con.fetch(
            """
            SELECT id, label, text_knowledge, importance, tags, metadata, import_id
            FROM process_mining.user_private_kb_graph_nodes
            WHERE user_id=$1
            ORDER BY id
            """,
            user_id,
        )
        erows = await con.fetch(
            """
            SELECT source_node_id, target_node_id, relation_type, relevance_score, importance
            FROM process_mining.user_private_kb_graph_edges
            WHERE user_id=$1
            """,
            user_id,
        )
    nodes = []
    for r in nrows:
        text = (r["text_knowledge"] or "").strip()
        label = (r["label"] or text[:80] or ("Node " + str(r["id"])))
        try:
            tags = json.loads(r["tags"]) if isinstance(r["tags"], str) else (r["tags"] or [])
        except Exception:
            tags = []
        nodes.append({
            "id": "p" + str(r["id"]),
            "raw_id": int(r["id"]),
            "label": label if len(label) <= 42 else label[:42] + "…",
            "full_label": label,
            "text": text,
            "importance": float(r["importance"] or 0.5),
            "tags": tags,
            "import_id": int(r["import_id"]) if r["import_id"] is not None else None,
        })
    edges = []
    for r in erows:
        edges.append({
            "from": "p" + str(r["source_node_id"]),
            "to": "p" + str(r["target_node_id"]),
            "relation": r["relation_type"] or "relates_to",
            "relevance": float(r["relevance_score"] or 0.5),
            "importance": float(r["importance"] or 0.5),
        })
    return {"ok": True, "nodes": nodes, "edges": edges,
            "counts": {"nodes": len(nodes), "edges": len(edges)}}


@app.get("/api/private-kb/graph/full")
async def private_kb_graph_full(request: Request):
    user_id = _require_user_id(request)
    return await _pkb_load_graph(user_id)


@app.get("/api/private-kb/graph/card")
async def private_kb_graph_card(request: Request, node_id: str):
    user_id = _require_user_id(request)
    raw = node_id[1:] if node_id.startswith("p") else node_id
    try:
        rid = int(raw)
    except Exception:
        return JSONResponse({"ok": False, "error": "bad node_id"}, status_code=400)
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """
            SELECT n.id, n.label, n.text_knowledge, n.importance, n.tags, n.metadata,
                   n.import_id, n.created_at
            FROM process_mining.user_private_kb_graph_nodes n
            WHERE n.user_id=$1 AND n.id=$2
            """,
            user_id, rid,
        )
        if not row:
            return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
        neigh = await con.fetch(
            """
            SELECT CASE WHEN e.source_node_id=$2 THEN e.target_node_id ELSE e.source_node_id END AS nid,
                   e.relation_type, e.relevance_score
            FROM process_mining.user_private_kb_graph_edges e
            WHERE e.user_id=$1 AND (e.source_node_id=$2 OR e.target_node_id=$2)
            """,
            user_id, rid,
        )
        neigh_ids = [r["nid"] for r in neigh]
        labels = {}
        if neigh_ids:
            lr = await con.fetch(
                "SELECT id, label, text_knowledge FROM process_mining.user_private_kb_graph_nodes "
                "WHERE user_id=$1 AND id = ANY($2::bigint[])",
                user_id, neigh_ids,
            )
            for r in lr:
                labels[r["id"]] = (r["label"] or (r["text_knowledge"] or "")[:60])
    try:
        tags = json.loads(row["tags"]) if isinstance(row["tags"], str) else (row["tags"] or [])
    except Exception:
        tags = []
    try:
        md = json.loads(row["metadata"]) if isinstance(row["metadata"], str) else (row["metadata"] or {})
    except Exception:
        md = {}
    neighbors = []
    for r in neigh:
        neighbors.append({
            "node_id": "p" + str(r["nid"]),
            "label": labels.get(r["nid"], "Node " + str(r["nid"])),
            "relation": r["relation_type"],
            "relevance": float(r["relevance_score"] or 0.5),
        })
    return {
        "ok": True,
        "node_id": "p" + str(row["id"]),
        "label": row["label"],
        "text": row["text_knowledge"],
        "importance": float(row["importance"] or 0.5),
        "tags": tags,
        "metadata": md,
        "import_id": int(row["import_id"]) if row["import_id"] is not None else None,
        "neighbors": neighbors,
    }


class PkbGraphSearchIn(BaseModel):
    query: str = ""
    depth: int = 1
    want_summary: bool = False
    limit: int = 25


@app.post("/api/private-kb/graph/search")
async def private_kb_graph_search(body: PkbGraphSearchIn, request: Request):
    """Семантический поиск по узлам персонального графа + BFS по глубине + LLM-саммари.

    Эмбеддинги узлов PKB не хранятся -> считаем on-the-fly (узлов немного).
    """
    from app.search import embed_text as _pkb_embed, llm_summary_kg as _pkb_llm_sum

    user_id = _require_user_id(request)
    q = (body.query or "").strip()
    if not q:
        return JSONResponse({"ok": False, "error": "empty query"}, status_code=400)
    depth = max(0, min(4, int(body.depth or 1)))
    limit = max(1, min(80, int(body.limit or 25)))

    async with pool.acquire() as con:
        nrows = await con.fetch(
            "SELECT id, label, text_knowledge, importance FROM process_mining.user_private_kb_graph_nodes "
            "WHERE user_id=$1 ORDER BY id",
            user_id,
        )
        erows = await con.fetch(
            "SELECT source_node_id AS s, target_node_id AS t, relation_type FROM process_mining.user_private_kb_graph_edges "
            "WHERE user_id=$1",
            user_id,
        )
    if not nrows:
        return {"ok": True, "query": q, "found": 0, "seeds": [], "nodes": [], "edges": [],
                "summary": None, "summary_error": None}

    # embeddings: запрос + узлы
    try:
        qvec = await _pkb_embed(q)
    except Exception as e:
        return JSONResponse({"ok": False, "error": "embed_query: " + str(e)}, status_code=500)

    texts = [((r["text_knowledge"] or r["label"] or "").strip()) for r in nrows]
    node_vecs = {}
    try:
        # батч-эмбеддинги узлов одним запросом
        import httpx as _pkb_httpx
        from app.search import EMBED_BASE as _EB, EMBED_KEY as _EK, EMBED_MODEL as _EM
        async with _pkb_httpx.AsyncClient(timeout=90) as cl:
            r = await cl.post(
                _EB + "/embeddings",
                headers={"Authorization": "Bearer " + _EK, "Content-Type": "application/json"},
                json={"model": _EM, "input": texts},
            )
            r.raise_for_status()
            data = r.json()["data"]
        for i, r0 in enumerate(nrows):
            node_vecs[int(r0["id"])] = data[i]["embedding"]
    except Exception as e:
        return JSONResponse({"ok": False, "error": "embed_nodes: " + str(e)}, status_code=500)

    scored = []
    for r0 in nrows:
        nid = int(r0["id"])
        sim = _pkb_cos(qvec, node_vecs.get(nid, []))
        scored.append((nid, sim))
    scored.sort(key=lambda x: x[1], reverse=True)

    # сиды: топ по косинусу с порогом
    seeds = [nid for nid, sim in scored if sim >= 0.30][:limit]
    if not seeds:
        seeds = [nid for nid, _ in scored[:min(8, len(scored))]]

    # adjacency для BFS
    adj = {}
    for e in erows:
        adj.setdefault(int(e["s"]), set()).add(int(e["t"]))
        adj.setdefault(int(e["t"]), set()).add(int(e["s"]))

    selected = set(seeds)
    frontier = set(seeds)
    for _ in range(depth):
        nxt = set()
        for n in frontier:
            for m in adj.get(n, ()):  # соседи
                if m not in selected:
                    nxt.add(m)
        selected |= nxt
        frontier = nxt
        if not frontier:
            break

    sim_map = {nid: sim for nid, sim in scored}
    by_id = {int(r["id"]): r for r in nrows}
    out_nodes = []
    for nid in selected:
        r = by_id.get(nid)
        if not r:
            continue
        text = (r["text_knowledge"] or "").strip()
        label = r["label"] or text[:80]
        out_nodes.append({
            "id": "p" + str(nid),
            "node_id": "p" + str(nid),
            "raw_id": nid,
            "label": label if len(label) <= 42 else label[:42] + "…",
            "full_label": label,
            "text": text,
            "importance": float(r["importance"] or 0.5),
            "score": round(float(sim_map.get(nid, 0.0)), 6),
            "is_seed": nid in set(seeds),
        })
    out_nodes.sort(key=lambda x: x["score"], reverse=True)

    sel = selected
    out_edges = []
    for e in erows:
        sfrom = int(e["s"]); sto = int(e["t"])
        if sfrom in sel and sto in sel:
            out_edges.append({
                "from": "p" + str(sfrom), "to": "p" + str(sto),
                "relation": e["relation_type"] or "relates_to",
            })

    summary = None
    summary_error = None
    if body.want_summary and out_nodes:
        try:
            snippets = [{"kind": "knowledge", "title": n["full_label"], "content": n["text"]}
                        for n in out_nodes[:12]]
            summary, _tok = await _pkb_llm_sum(q, snippets)
        except Exception as e:
            summary_error = type(e).__name__ + ": " + str(e)

    return {
        "ok": True,
        "query": q,
        "depth": depth,
        "found": len(seeds),
        "seeds": ["p" + str(x) for x in seeds],
        "nodes": out_nodes,
        "edges": out_edges,
        "counts": {"nodes": len(out_nodes), "edges": len(out_edges)},
        "summary": summary,
        "summary_error": summary_error,
    }


# ---------------- Local pipeline export ----------------
_LOCAL_EXPORT_TABLES = [
    "knowledge",
    "metadata_knowledge",
    "knowledge_review",
    "knowledge_tags",
    "knowledge_relations",
    "entity_relations",
    "wiki_pages",
]


def _to_cell(v):
    if v is None:
        return ""
    if isinstance(v, (datetime, _date)):
        return v.isoformat()
    if isinstance(v, (dict, list, tuple)):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    return str(v)


async def _collect_table_columns(con: asyncpg.Connection, table: str) -> list[str]:
    rows = await con.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'process_mining' AND table_name = $1
        ORDER BY ordinal_position
        """,
        table,
    )
    return [r["column_name"] for r in rows]


async def _export_local_tables(tmp_dir: str, mode: str) -> dict:
    csv_dir = os.path.join(tmp_dir, "csv")
    os.makedirs(csv_dir, exist_ok=True)
    sqlite_path = os.path.join(tmp_dir, "pm_knowledge_local.sqlite")
    sq = sqlite3.connect(sqlite_path)
    meta = {"tables": {}, "sqlite_path": sqlite_path}

    try:
        async with pool.acquire() as con:
            for t in _LOCAL_EXPORT_TABLES:
                cols = await _collect_table_columns(con, t)
                if not cols:
                    continue
                q = f'SELECT * FROM process_mining."{t}"'
                rows = await con.fetch(q)

                csv_path = os.path.join(csv_dir, f"{t}.csv")
                with open(csv_path, "w", encoding="utf-8", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(cols)
                    for r in rows:
                        w.writerow([_to_cell(r[c]) for c in cols])

                if mode == "sqlite_csv":
                    col_sql = ", ".join([f'"{c}" TEXT' for c in cols])
                    sq.execute(f'DROP TABLE IF EXISTS "{t}"')
                    sq.execute(f'CREATE TABLE "{t}" ({col_sql})')
                    if rows:
                        ph = ",".join(["?"] * len(cols))
                        col_names = ", ".join([f'"{c}"' for c in cols])
                        ins = f'INSERT INTO "{t}" ({col_names}) VALUES ({ph})'
                        sq.executemany(ins, [[_to_cell(r[c]) for c in cols] for r in rows])

                meta["tables"][t] = len(rows)

        sq.commit()
        return meta
    finally:
        sq.close()


@app.get("/api/local-pipeline/export")
async def local_pipeline_export(mode: str = "sqlite_csv"):
    mode = (mode or "sqlite_csv").strip().lower()
    if mode not in ("sqlite_csv", "csv_only"):
        return JSONResponse({"ok": False, "error": "bad_mode"}, status_code=400)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_dir = os.path.join(BASE_DIR, "local_pipeline_6_4_10")
    app_dir = os.path.join(base_dir, "local_graph_app")

    with tempfile.TemporaryDirectory(prefix="pma_local_export_") as td:
        meta = await _export_local_tables(td, mode)
        zip_path = os.path.join(td, f"local_graph_app_{mode}_{ts}.zip")

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            # 1) only FastAPI app files (no root notebook/pipeline files)
            if os.path.isdir(app_dir):
                for root, dirs, files in os.walk(app_dir):
                    dirs[:] = [d for d in dirs if d != "__pycache__"]
                    for name in files:
                        if name.endswith(".pyc"):
                            continue
                        full = os.path.join(root, name)
                        rel = os.path.relpath(full, app_dir)
                        # generated fresh below
                        if rel.replace('\\', '/').startswith('data/'):
                            continue
                        if name == "pm_knowledge_local.sqlite":
                            continue
                        zf.write(full, arcname=os.path.join("local_graph_app", rel))

            # 2) generated data goes inside local_graph_app/data
            if mode == "sqlite_csv":
                sp = meta.get("sqlite_path")
                if sp and os.path.exists(sp):
                    zf.write(sp, arcname="local_graph_app/data/pm_knowledge_local.sqlite")

            csv_dir = os.path.join(td, "csv")
            if os.path.isdir(csv_dir):
                for name in sorted(os.listdir(csv_dir)):
                    pp = os.path.join(csv_dir, name)
                    if os.path.isfile(pp):
                        zf.write(pp, arcname=f"local_graph_app/data/{name}")

            zf.writestr(
                "local_graph_app/manifest.json",
                json.dumps(
                    {
                        "ok": True,
                        "mode": mode,
                        "generated_at_utc": ts,
                        "tables": meta.get("tables", {}),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )

        with open(zip_path, "rb") as f:
            data = f.read()

    headers = {
        "Content-Disposition": f'attachment; filename="local_graph_app_{mode}_{ts}.zip"'
    }
    return StreamingResponse(io.BytesIO(data), media_type="application/zip", headers=headers)


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
        src_rows = await con.fetch("""
            SELECT lower(coalesce(source,'')) AS source, count(*)::int AS c
            FROM process_mining.papers_metadata
            GROUP BY 1
            ORDER BY 2 DESC
        """)
        # динамика по дням (последние 14 дней, включая пустые даты)
        dyn_rows = await con.fetch("""
            WITH days AS (
                SELECT generate_series(
                    date_trunc('day', now()) - interval '13 days',
                    date_trunc('day', now()),
                    interval '1 day'
                ) AS d
            )
            SELECT to_char(days.d, 'DD.MM') AS wk,
                   COALESCE(count(*) FILTER (WHERE p.is_relevant = TRUE), 0) AS collected,
                   COALESCE(count(*) FILTER (
                       WHERE p.is_relevant = TRUE
                         AND EXISTS (SELECT 1 FROM process_mining.reviews r WHERE r.article_id = p.id)
                   ), 0) AS processed
            FROM days
            LEFT JOIN process_mining.papers_metadata p
              ON date_trunc('day', p.created_at) = days.d
            GROUP BY days.d
            ORDER BY days.d
        """)
    # Группируем по префиксу до ':' — реальные категории вида
    # "Общие принципы: Моделирование в PM" схлопываются в "Общие принципы".
    # Плоские англоязычные (AI & Process Mining и т.п.) остаются как есть.
    by_category = {}
    for r in cat_rows:
        grp = (r["grp"] or "").strip() or "Без категории"
        by_category[grp] = by_category.get(grp, 0) + r["c"]
    by_source = { (r["source"] or "unknown"): int(r["c"]) for r in src_rows }
    return {
        "ok": True,
        "total": base["total"], "relevant": base["relevant"], "reviewed": base["reviewed"],
        "assessed": base["assessed"], "irrelevant": base["irrelevant"],
        "invalid_sources": base["invalid_sources"],
        "week_new": base["week_new"], "avg_relevance": float(base["avg_relevance"]),
        "by_category": by_category,
        "by_source": by_source,
        "dynamics": {
            "labels":    [r["wk"] for r in dyn_rows],
            "collected": [r["collected"] for r in dyn_rows],
            "processed": [r["processed"] for r in dyn_rows],
        },
    }


# ---------------- Ручные кнопки ----------------
class RunParserReq(BaseModel):
    include_sources: list[str] = []


class KGStartReq(BaseModel):
    limit: int = 0
    batch_size: int = 1


@app.post("/api/run/parser")
async def run_parser(body: RunParserReq | None = None, background: BackgroundTasks = None, request: Request = None):
    """Полный цикл: сбор -> релевантность -> ревью (фоново)."""
    client_ip = (request.client.host if request and request.client else "unknown")
    ua = request.headers.get("user-agent", "")[:180] if request else ""

    allowed = {"arxiv_api","arxiv_html","crossref","core_api","fluxicon","google_scholar"}
    include_sources = []
    for s in ((body.include_sources if body else []) or []):
        k = str(s or "").strip().lower()
        if k in allowed and k not in include_sources:
            include_sources.append(k)
    if not include_sources:
        include_sources = ["arxiv_api","arxiv_html","crossref","core_api","fluxicon","google_scholar"]

    api_logger.info(f"parser_button_click ip={client_ip} ua={ua} include_sources={','.join(include_sources)}")
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
            await pipeline.run_once(parser_sources=include_sources)
        finally:
            await pipeline._release_parser_run()

    background.add_task(_run_parser_prelocked)
    api_logger.info("parser_button_accepted status=started")
    return {"ok": True, "status": "started", "include_sources": include_sources}


@app.get("/api/run/parser/progress")
async def run_parser_progress():
    p = await pipeline.get_parser_progress()
    return {
        "ok": True,
        "running": bool(p.get("running")),
        "total_sources": int(p.get("total_sources") or 0),
        "done_sources": int(p.get("done_sources") or 0),
        "current_source": p.get("current_source") or "",
        "found_raw_total": int(p.get("found_raw_total") or 0),
        "found_final_total": int(p.get("found_final_total") or 0),
        "per_source_raw": p.get("per_source_raw") or {},
        "per_source_final": p.get("per_source_final") or {},
        "dropped_old_by_source": p.get("dropped_old_by_source") or {},
        "errors": p.get("errors") or {},
        "started_at": p.get("started_at"),
        "updated_at": p.get("updated_at"),
    }


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


@app.post("/api/kg/process")
async def run_kg_process(limit: int = 30):
    res = await pipeline.process_knowledge_graph(limit=limit)
    if not res.get("ok") and res.get("skipped") == "locked":
        return JSONResponse({"ok": False, "status": "busy", "reason": "kg_lock_busy"}, status_code=409)
    return res


@app.get("/api/kg/run/status")
async def kg_run_status():
    if not kg_manager:
        return JSONResponse({"ok": False, "error": "kg_manager_not_ready"}, status_code=503)
    return await kg_manager.status()


@app.post("/api/kg/run/start")
async def kg_run_start(req: KGStartReq):
    if not kg_manager:
        return JSONResponse({"ok": False, "error": "kg_manager_not_ready"}, status_code=503)
    res = await kg_manager.start(limit=req.limit, batch_size=req.batch_size)
    if not res.get("ok") and res.get("status") == "busy":
        return JSONResponse(res, status_code=409)
    return res


@app.post("/api/kg/run/stop")
async def kg_run_stop():
    if not kg_manager:
        return JSONResponse({"ok": False, "error": "kg_manager_not_ready"}, status_code=503)
    return await kg_manager.stop()


@app.get("/api/graph")
async def graph_data(limit_nodes: int = 450, limit_wiki: int = 120, lite: bool = True):
    if not kg_manager:
        return JSONResponse({"ok": False, "error": "kg_manager_not_ready"}, status_code=503)
    return await kg_manager.graph_payload(limit_nodes=limit_nodes, limit_wiki=limit_wiki, lite=lite)


@app.get("/api/graph/card")
async def graph_card(node_id: str):
    if not kg_manager:
        return JSONResponse({"ok": False, "error": "kg_manager_not_ready"}, status_code=503)
    d = await kg_manager.card_payload(node_id=node_id)
    if not d.get("ok"):
        return JSONResponse(d, status_code=404 if d.get("error")=="not_found" else 400)
    return d


# Разрешённые поля сортировки: alias -> SQL-выражение
_ARTICLES_SORT_MAP = {
    "date": "p.date_sub",
    "title": "p.title",
    "category": "p.category",
    "source": "p.source",
    "score": "p.relevance_score",
    "relevant": "p.is_relevant",
    "review": "(r.article_id IS NOT NULL)",
    "quarantine": "COALESCE(q.active, FALSE)",
    "processed": "p.processed_at",
    "kg": "p.kg_processed",
    "id": "p.id",
}


@app.get("/api/articles")
async def list_articles(
    page: int = 1,
    per_page: int = 20,
    only_relevant: bool = False,
    # --- кастомные фильтры по всем полям ---
    q: str | None = None,                 # общий поиск: title + abstract + authors
    title: str | None = None,             # подстрока в названии
    author: str | None = None,            # подстрока в авторах
    tag: str | None = None,               # подстрока в тегах
    category: str | None = None,          # точное совпадение категории
    source: str | None = None,            # точное совпадение источника
    relevant: str | None = None,          # yes|no|any
    has_review: str | None = None,        # yes|no|any
    quarantine: str | None = None,        # yes|no|any
    kg_processed: str | None = None,      # yes|no|any
    score_min: float | None = None,
    score_max: float | None = None,
    date_from: str | None = None,         # YYYY-MM-DD
    date_to: str | None = None,           # YYYY-MM-DD
    sort: str = "date",
    order: str = "desc",
):
    page = max(1, page)
    per_page = min(50, max(1, per_page))
    offset = (page - 1) * per_page

    conds: list[str] = []
    args: list = []

    def add(expr_tmpl: str, value):
        args.append(value)
        conds.append(expr_tmpl.format(n=len(args)))

    if only_relevant:
        conds.append("p.is_relevant = TRUE")

    if q:
        add(
            "(p.title ILIKE '%'||${n}||'%' OR p.abstract ILIKE '%'||${n}||'%' "
            "OR p.authors::text ILIKE '%'||${n}||'%')",
            q.strip(),
        )
    if title:
        add("p.title ILIKE '%'||${n}||'%'", title.strip())
    if author:
        add("p.authors::text ILIKE '%'||${n}||'%'", author.strip())
    if tag:
        add("array_to_string(COALESCE(p.tags, '{{}}'), ',') ILIKE '%'||${n}||'%'", tag.strip())
    if category:
        add("p.category = ${n}", category.strip())
    if source:
        add("p.source = ${n}", source.strip())

    def tri(val, expr_true, expr_false):
        v = (val or "any").strip().lower()
        if v in ("yes", "true", "1"):
            conds.append(expr_true)
        elif v in ("no", "false", "0"):
            conds.append(expr_false)

    tri(relevant, "p.is_relevant = TRUE", "COALESCE(p.is_relevant, FALSE) = FALSE")
    tri(has_review, "r.article_id IS NOT NULL", "r.article_id IS NULL")
    tri(quarantine, "COALESCE(q.active, FALSE) = TRUE", "COALESCE(q.active, FALSE) = FALSE")
    tri(kg_processed, "COALESCE(p.kg_processed, FALSE) = TRUE", "COALESCE(p.kg_processed, FALSE) = FALSE")

    if score_min is not None:
        add("p.relevance_score >= ${n}", float(score_min))
    if score_max is not None:
        add("p.relevance_score <= ${n}", float(score_max))
    if date_from:
        try:
            add("p.date_sub >= ${n}", _date.fromisoformat(date_from.strip()))
        except ValueError:
            pass
    if date_to:
        try:
            add("p.date_sub <= ${n}", _date.fromisoformat(date_to.strip()))
        except ValueError:
            pass

    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    sort_expr = _ARTICLES_SORT_MAP.get((sort or "date").lower(), "p.date_sub")
    order_dir = "ASC" if (order or "desc").lower() == "asc" else "DESC"
    order_by = f"{sort_expr} {order_dir} NULLS LAST, p.id DESC"

    base_join = """
        FROM process_mining.papers_metadata p
        LEFT JOIN process_mining.reviews r ON r.article_id = p.id
        LEFT JOIN process_mining.ui_article_quarantine q ON q.article_id = p.id
    """

    async with pool.acquire() as con:
        total = await con.fetchval(
            f"SELECT count(*) {base_join} {where}", *args)
        rows = await con.fetch(f"""
            SELECT p.id, p.source, p.title, p.date_sub, p.url_article,
                   p.category, p.relevance_score, p.is_relevant,
                   (r.article_id IS NOT NULL) AS has_review,
                   p.processed_at,
                   COALESCE(p.kg_processed, FALSE) AS kg_processed,
                   p.kg_processed_at,
                   CASE
                     WHEN p.source ILIKE 'yandex_disk:%' THEN COALESCE(p.pdf_url, p.url_article)
                     ELSE p.url_article
                   END AS display_url,
                   COALESCE(q.active, FALSE) AS quarantine_active
            {base_join}
            {where}
            ORDER BY {order_by}
            LIMIT ${len(args)+1} OFFSET ${len(args)+2}
        """, *args, per_page, offset)

    return {
        "ok": True,
        "page": page, "per_page": per_page,
        "total": total, "pages": (total + per_page - 1) // per_page,
        "articles": [dict(r) for r in rows],
    }


@app.get("/api/articles/filters")
async def articles_filter_options():
    """Справочник значений для селектов фильтра (категории, источники)."""
    async with pool.acquire() as con:
        cats = await con.fetch(
            "SELECT DISTINCT category FROM process_mining.papers_metadata "
            "WHERE category IS NOT NULL AND category <> '' ORDER BY 1")
        srcs = await con.fetch(
            "SELECT DISTINCT source FROM process_mining.papers_metadata "
            "WHERE source IS NOT NULL AND source <> '' ORDER BY 1")
    return {
        "ok": True,
        "categories": [r["category"] for r in cats],
        "sources": [r["source"] for r in srcs],
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



class QuarantineToggleReq(BaseModel):
    active: bool
    note: str | None = None


@app.post("/api/articles/{article_id}/quarantine")
async def set_article_quarantine(article_id: int, req: QuarantineToggleReq, request: Request):
    article_id = int(article_id)
    active = bool(req.active)
    note = (req.note or "").strip() or None
    ident = current_identity(request)
    marker = ident.get("username") or ident.get("user_id") or _get_visitor_id(request)

    async with pool.acquire() as con:
        exists = await con.fetchval(
            "SELECT 1 FROM process_mining.papers_metadata WHERE id=$1",
            article_id,
        )
        if not exists:
            return JSONResponse({"ok": False, "error": "article_not_found"}, status_code=404)

        if active:
            await con.execute(
                """
                INSERT INTO process_mining.ui_article_quarantine(article_id, active, note, marked_by, created_at, updated_at)
                VALUES($1, TRUE, $2, $3, NOW(), NOW())
                ON CONFLICT (article_id)
                DO UPDATE SET
                    active = TRUE,
                    note = EXCLUDED.note,
                    marked_by = EXCLUDED.marked_by,
                    updated_at = NOW()
                """,
                article_id,
                note,
                marker,
            )
        else:
            await con.execute(
                """
                INSERT INTO process_mining.ui_article_quarantine(article_id, active, note, marked_by, created_at, updated_at)
                VALUES($1, FALSE, $2, $3, NOW(), NOW())
                ON CONFLICT (article_id)
                DO UPDATE SET
                    active = FALSE,
                    note = EXCLUDED.note,
                    marked_by = EXCLUDED.marked_by,
                    updated_at = NOW()
                """,
                article_id,
                note,
                marker,
            )

        row = await con.fetchrow(
            """
            SELECT article_id, active, note, marked_by, created_at, updated_at
            FROM process_mining.ui_article_quarantine
            WHERE article_id=$1
            """,
            article_id,
        )

    return {"ok": True, "quarantine": dict(row)}



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
            "dim": EMBED_DIM,
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
async def api_search_quick(request: Request, q: str = "", limit: int = 8):
    """Live-поиск по совпадению слов (pg_trgm/ILIKE), для typeahead."""
    try:
        items = await search_quick(pool, q, min(20, max(1, limit)))
        await _log_llm_query(
            request,
            route="/api/search/quick",
            search_type="review",
            query_text=q,
            llm_requested=False,
            llm_called=False,
            model_name=None,
            tokens_usage={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
            found_count=len(items),
            used_count=0,
            summary_error=None,
            meta={"mode": "quick", "limit": min(20, max(1, limit))},
        )
        return {"ok": True, "q": q, "count": len(items), "items": items}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)


@app.get("/api/kg/search/quick")
async def api_kg_search_quick(request: Request, q: str = "", limit: int = 20):
    """Live GET-поиск по узлам KG (для фронтовой подсветки в реальном времени)."""
    q = (q or "").strip()
    if len(q) < 2:
        await _log_llm_query(
            request,
            route="/api/kg/search/quick",
            search_type="graph",
            query_text=q,
            llm_requested=False,
            llm_called=False,
            model_name=None,
            tokens_usage={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
            found_count=0,
            used_count=0,
            summary_error=None,
            meta={"mode": "kg_quick", "reason": "query_too_short"},
        )
        return {"ok": True, "q": q, "count": 0, "items": []}
    try:
        items = await search_kg_quick(pool, q, min(150, max(1, limit)))
        await _log_llm_query(
            request,
            route="/api/kg/search/quick",
            search_type="graph",
            query_text=q,
            llm_requested=False,
            llm_called=False,
            model_name=None,
            tokens_usage={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
            found_count=len(items),
            used_count=0,
            summary_error=None,
            meta={"mode": "kg_quick", "limit": min(150, max(1, limit))},
        )
        return {"ok": True, "q": q, "count": len(items), "items": items}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=500)


@app.post("/api/search/hybrid")
async def api_search_hybrid(body: HybridSearchIn, request: Request):
    """Гибридный поиск BM25 + вектор (RRF). Опционально LLM-саммари по топу."""
    q = (body.q or "").strip()
    if not q:
        return JSONResponse({"ok": False, "error": "empty query"}, status_code=400)
    try:
        items = await search_hybrid(pool, q, min(20, max(1, body.limit)))
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"search: {type(e).__name__}: {e}"}, status_code=500)

    summary = None
    summary_error = None
    tokens_usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    llm_called = False
    if body.llm_summary and items:
        try:
            summary, tokens_usage = await llm_summary(q, items[:6])
            llm_called = True
        except Exception as e:
            summary = None
            summary_error = f"{type(e).__name__}: {e}"

    await _log_llm_query(
        request,
        route="/api/search/hybrid",
        search_type="review",
        query_text=q,
        llm_requested=bool(body.llm_summary),
        llm_called=llm_called,
        model_name=LLM_MODEL if llm_called else None,
        tokens_usage=tokens_usage,
        found_count=len(items),
        used_count=min(len(items), 6) if llm_called else 0,
        summary_error=summary_error,
        meta={"mode": "hybrid", "limit": min(20, max(1, body.limit))},
    )

    return {
        "ok": True,
        "q": q,
        "count": len(items),
        "items": items,
        "summary": summary,
        "summary_error": summary_error,
        "tokens_usage": tokens_usage,
    }


class KgSearchIn(BaseModel):
    q: str
    limit: int = 120
    max_ctx: int = 100
    llm_summary: bool = False


@app.post("/api/kg/search")
async def api_kg_search(body: KgSearchIn, request: Request):
    """Гибридный поиск (BM25 + вектор, RRF) по узлам графа знаний (knowledge + wiki).

    Всегда выполняет гибридный поиск по эмбеддингам графа, независимо от того,
    есть ли локальные совпадения подсветки на фронте.
    Найдено может быть больше, чем реально уходит в модель: контекст LLM
    ограничен max_ctx (по умолчанию 100) первыми (лучшими по RRF) узлами.
    """
    q = (body.q or "").strip()
    if not q:
        return JSONResponse({"ok": False, "error": "empty query"}, status_code=400)

    max_ctx = min(100, max(1, body.max_ctx))
    limit = min(150, max(1, body.limit))

    try:
        items = await search_kg_hybrid(pool, q, limit)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"kg_search: {type(e).__name__}: {e}"}, status_code=500)

    found = len(items)
    ctx_items = items[:max_ctx]
    used = len(ctx_items)

    summary = None
    summary_error = None
    tokens_usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    llm_called = False
    if body.llm_summary and ctx_items:
        try:
            summary, tokens_usage = await llm_summary_kg(q, ctx_items)
            llm_called = True
        except Exception as e:
            summary_error = f"{type(e).__name__}: {e}"

    await _log_llm_query(
        request,
        route="/api/kg/search",
        search_type="graph",
        query_text=q,
        llm_requested=bool(body.llm_summary),
        llm_called=llm_called,
        model_name=LLM_MODEL if llm_called else None,
        tokens_usage=tokens_usage,
        found_count=found,
        used_count=used if llm_called else 0,
        summary_error=summary_error,
        meta={"mode": "kg_hybrid", "limit": limit, "max_ctx": max_ctx},
    )

    return {
        "ok": True,
        "q": q,
        "found": found,
        "used": used,
        "max_ctx": max_ctx,
        "items": items,
        "summary": summary,
        "summary_error": summary_error,
        "tokens_usage": tokens_usage,
    }
