"""PMAnalyze pipeline: еженедельный сбор статей PM -> релевантность+категория -> PDF -> ревью (md).

Схема БД: process_mining (papers_metadata, reviews, parser_runs).
LLM: cloud.ru (anthropic/claude-sonnet-4.6) через /v1/messages.
Парсер: catchpm-browser (POST /api/parser/run, POST /api/pdf/fulltext).
"""
import asyncio
import io
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta, date
from urllib.parse import quote, urlparse

import httpx


def _to_date(value):
    """Парсит строку/дату в datetime.date для asyncpg ($4 date_sub).
    Возвращает None, если распарсить нельзя (в БД пойдёт NULL)."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d.%m.%Y", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None

from app.prompts import build_relevance_prompt, build_review_prompt, CATEGORIES


@dataclass
class Settings:
    enabled: bool = True
    # еженедельно: раз в 7 суток, запуск в понедельник 12:00 МСК
    weekly_dow_msk: int = 0            # 0=понедельник
    weekly_hour_msk: int = 12
    run_stale_seconds: int = 7200
    lock_key: int = 20260824
    max_per_source: int = 20
    relevance_threshold: float = 0.6

    cloud_key: str = field(default_factory=lambda: os.getenv("CLOUD_LLM_ACCESS_KEY", "").strip())
    cloud_messages_url: str = "https://foundation-models.api.cloud.ru/v1/messages"
    cloud_chat_url: str = "https://foundation-models.api.cloud.ru/v1/chat/completions"
    relevance_model: str = field(default_factory=lambda: os.getenv("ARTICLE_RELEVANCE_MODEL", "anthropic/claude-sonnet-4.6"))
    review_model: str = field(default_factory=lambda: os.getenv("ARTICLE_REVIEW_MODEL", "anthropic/claude-sonnet-4.6"))

    parser_run_url: str = field(default_factory=lambda: os.getenv("PARSER_SERVICE_URL", "http://catchpm-browser:9333/api/parser/run"))
    parser_pdf_url: str = field(default_factory=lambda: os.getenv("PARSER_PDF_URL", "http://catchpm-browser:9333/api/pdf/fulltext"))
    parser_timeout: int = int(os.getenv("PARSER_SERVICE_TIMEOUT_SECONDS", "180"))
    parser_api_key: str = field(default_factory=lambda: os.getenv("PARSER_SERVICE_API_KEY", "").strip())


class PMAnalyzePipeline:
    def __init__(self, pool, settings: Settings):
        self.pool = pool
        self.settings = settings
        self._task = None
        self._stop = asyncio.Event()

        self._parser_state_lock = asyncio.Lock()
        self._parser_running = False

        self._reviews_state_lock = asyncio.Lock()
        self._reviews_running = False

    @staticmethod
    def _clean_author_name(v: str) -> str:
        s = (v or "").strip()
        if not s:
            return ""
        s = re.sub(r"^[\[\{\(\"'\s]+|[\]\}\)\"'\s]+$", "", s)
        s = re.sub(r"\s+", " ", s)
        s = s.replace("\u00a0", " ")
        s = re.sub(r"\bet\s*al\b\.?", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*:\s*$", "", s)
        s = s.strip(" ,;:-")
        if not s:
            return ""
        bad = {"unknown", "n/a", "na", "author", "authors"}
        if s.lower() in bad:
            return ""
        if len(s) < 3:
            return ""

        # Отбрасываем мусорные одиночные токены (например "BEEREPOOT" после очистки "ET AL")
        parts = [p for p in s.split(" ") if p]
        if len(parts) < 2:
            return ""

        # Если вся строка ВЕРХНИМ РЕГИСТРОМ и без нормального формата ФИО — считаем шумом
        if s.upper() == s and not re.search(r"[a-z]", s):
            return ""

        return s

    def _normalize_authors(self, raw) -> list[str]:
        vals = []
        if raw is None:
            return vals
        if isinstance(raw, list):
            vals = raw
        elif isinstance(raw, str):
            t = raw.strip()
            # возможно JSON-строка
            if t.startswith("[") and t.endswith("]"):
                try:
                    parsed = json.loads(t)
                    if isinstance(parsed, list):
                        vals = parsed
                    else:
                        vals = [t]
                except Exception:
                    vals = re.split(r"[,;]|\band\b", t)
            else:
                vals = re.split(r"[,;]|\band\b", t)
        else:
            vals = [str(raw)]

        out = []
        seen = set()
        for v in vals:
            c = self._clean_author_name(str(v))
            if not c:
                continue
            key = c.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(c)
        return out

    # ---------------- lifecycle ----------------
    async def start(self):
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._stop.set()
        if self._task:
            await self._task
            self._task = None

    async def _seconds_to_next_weekly_run_msk(self) -> int:
        msk = timezone(timedelta(hours=3))
        now = datetime.now(msk)
        target = now.replace(hour=self.settings.weekly_hour_msk, minute=0, second=0, microsecond=0)
        days_ahead = (self.settings.weekly_dow_msk - now.weekday()) % 7
        target = target + timedelta(days=days_ahead)
        if target <= now:
            target = target + timedelta(days=7)
        return max(1, int((target - now).total_seconds()))

    async def _loop(self):
        while not self._stop.is_set():
            wait_s = await self._seconds_to_next_weekly_run_msk()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait_s)
                break  # stop requested
            except asyncio.TimeoutError:
                pass
            if self.settings.enabled:
                try:
                    await self.run_once()
                except Exception as e:
                    print(f"[pmanalyze] run_once fatal: {e}")

    # ---------------- distributed lock ----------------
    async def _try_lock(self) -> bool:
        async with self.pool.acquire() as con:
            return bool(await con.fetchval("SELECT pg_try_advisory_lock($1)", self.settings.lock_key))

    async def _unlock(self):
        async with self.pool.acquire() as con:
            await con.execute("SELECT pg_advisory_unlock($1)", self.settings.lock_key)

    # ---------------- main run ----------------
    async def run_once(self) -> dict:
        if not await self._try_lock():
            print("[pmanalyze] another run holds the lock, skip")
            return {"ok": False, "skipped": "locked"}
        run_id = None
        stats = {"fetched": 0, "inserted": 0, "relevant": 0, "reviewed": 0, "errors": 0}
        try:
            async with self.pool.acquire() as con:
                run_id = await con.fetchval(
                    "INSERT INTO process_mining.parser_runs (status) VALUES ('running') RETURNING id"
                )

            # 1) сбор метаданных
            fetched = await self._collect()
            stats["fetched"] = len(fetched)

            # 2) upsert в papers_metadata
            new_ids = await self._upsert_articles(fetched)
            stats["inserted"] = len(new_ids)

            # 3) обработка всех new/review_error
            proc = await self.process_pending(limit=200)
            stats["relevant"] = proc["relevant"]
            stats["reviewed"] = proc["reviewed"]
            stats["errors"] = proc["errors"]

            async with self.pool.acquire() as con:
                await con.execute(
                    """UPDATE process_mining.parser_runs
                       SET finished_at=now(), status='done',
                           fetched=$2, inserted=$3, relevant=$4, reviewed=$5, errors=$6
                       WHERE id=$1""",
                    run_id, stats["fetched"], stats["inserted"],
                    stats["relevant"], stats["reviewed"], stats["errors"],
                )
            return {"ok": True, "run_id": run_id, **stats}
        except Exception as e:
            if run_id is not None:
                async with self.pool.acquire() as con:
                    await con.execute(
                        "UPDATE process_mining.parser_runs SET finished_at=now(), status='error', detail=$2 WHERE id=$1",
                        run_id, json.dumps({"error": str(e)[:500]}),
                    )
            print(f"[pmanalyze] run error: {e}")
            return {"ok": False, "error": str(e)[:300]}
        finally:
            await self._unlock()

    async def run_once_guarded(self) -> dict:
        # Защита от повторного старта парсинга в рамках одного инстанса
        if not await self._try_claim_parser_run():
            print("[pmanalyze] run_once already running in this instance, skip")
            return {"ok": False, "skipped": "already_running"}
        try:
            return await self.run_once()
        finally:
            await self._release_parser_run()

    # ---------------- collect from parser service ----------------
    async def _collect(self) -> list[dict]:
        headers = {"Content-Type": "application/json"}
        if self.settings.parser_api_key:
            headers["X-Api-Key"] = self.settings.parser_api_key
        payload = {"max_per_source": int(self.settings.max_per_source), "ui_lang": "en"}
        try:
            async with httpx.AsyncClient(timeout=self.settings.parser_timeout) as client:
                r = await client.post(self.settings.parser_run_url, headers=headers, json=payload)
                r.raise_for_status()
                data = r.json()
            return data.get("articles", []) if data.get("ok") else []
        except Exception as e:
            print(f"[pmanalyze] collect error: {e}")
            return []

    async def _upsert_articles(self, articles: list[dict]) -> list[int]:
        new_ids = []
        async with self.pool.acquire() as con:
            for a in articles:
                ext = (a.get("external_id") or "").strip()
                src = (a.get("source") or "").strip()
                if not ext or not src:
                    continue
                authors = a.get("authors")
                tags = a.get("tags")
                row = await con.fetchrow(
                    """
                    INSERT INTO process_mining.papers_metadata
                        (external_id, source, title, date_sub, url_article, pdf_url,
                         authors, tags, abstract, category, is_relevant, review_flag,
                         llm_status, created_at, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,FALSE,FALSE,'new',now(),now())
                    ON CONFLICT (source, external_id) DO NOTHING
                    RETURNING id
                    """,
                    ext, src, a.get("title", ""), _to_date(a.get("date_sub")),
                    a.get("url_article") or a.get("pdf_url"), a.get("pdf_url"),
                    json.dumps(authors) if authors is not None else None,
                    tags if tags is not None else None,
                    a.get("abstract", ""),
                )
                if row:
                    new_ids.append(int(row["id"]))
        return new_ids

    # ---------------- LLM ----------------
    async def _llm(self, model: str, prompt: str, max_tokens: int = 1500) -> str:
        key = self.settings.cloud_key
        if not key:
            return ""
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        use_messages = str(model).startswith("anthropic/")
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                if use_messages:
                    payload = {
                        "model": model,
                        "max_tokens": max_tokens,
                        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
                    }
                    r = await client.post(self.settings.cloud_messages_url, headers=headers, json=payload)
                    if r.status_code != 200:
                        print(f"[pmanalyze] llm messages {r.status_code}: {r.text[:200]}")
                        return ""
                    data = r.json()
                    parts = data.get("content", [])
                    return "".join(p.get("text", "") for p in parts if isinstance(p, dict)).strip()
                else:
                    payload = {
                        "model": model,
                        "max_tokens": max_tokens,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                    r = await client.post(self.settings.cloud_chat_url, headers=headers, json=payload)
                    if r.status_code != 200:
                        return ""
                    return r.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"[pmanalyze] llm error: {e}")
            return ""

    @staticmethod
    def _extract_json(text: str) -> dict | None:
        if not text:
            return None
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            return None
        try:
            return json.loads(m.group(0))
        except Exception:
            return None

    async def assess_relevance(self, art: dict) -> dict:
        prompt = build_relevance_prompt(art.get("title", ""), art.get("abstract", ""))
        raw = await self._llm(self.settings.relevance_model, prompt, max_tokens=800)
        data = self._extract_json(raw)
        if not data:
            return {"overall_score": 0.0, "is_relevant": False,
                    "category": "Без категории", "reasoning": "llm_parse_failed"}
        score = float(data.get("overall_score", 0.0) or 0.0)
        is_rel = bool(data.get("is_relevant", False)) and score >= self.settings.relevance_threshold
        cat = data.get("category") or "Без категории"
        if cat not in CATEGORIES:
            cat = "Без категории"
        if not is_rel:
            cat = "Без категории"
        return {"overall_score": round(score, 1), "is_relevant": is_rel,
                "category": cat, "reasoning": (data.get("reasoning") or "")[:2000]}

    async def _fetch_page_text(self, url: str) -> str:
        if not url:
            return ""
        headers = {"Content-Type": "application/json"}
        if self.settings.parser_api_key:
            headers["X-Api-Key"] = self.settings.parser_api_key
        try:
            async with httpx.AsyncClient(timeout=self.settings.parser_timeout) as client:
                r = await client.post(
                    "http://catchpm-browser:9333/api/fetch/pages",
                    headers=headers,
                    json={"urls": [url], "max_chars": 12000, "wait_ms": 1800, "timeout_ms": 45000, "max_urls": 1},
                )
                r.raise_for_status()
                data = r.json()
            res = (data.get("results") or [{}])[0]
            if not res.get("ok"):
                return ""
            txt = (res.get("text") or "").strip()
            if txt:
                return txt
            ex = (res.get("excerpt") or "").strip()
            if ex:
                return ex
            return ""
        except Exception as e:
            print(f"[pmanalyze] page fetch error: {e}")
            return ""

    async def _download_pdf_text(self, pdf_url: str) -> str:
        """Скачивание бинарного PDF и извлечение текста (fallback для yandex/web links)."""
        if not pdf_url:
            return ""
        try:
            # Для Yandex Disk директ-линк через cloud API
            dl_url = pdf_url
            if "disk.yandex." in (urlparse(pdf_url).netloc or ""):
                parsed = urlparse(pdf_url)
                parts = [p for p in (parsed.path or "").split("/") if p]
                # Ожидаем формат: /d/<public_id>/<optional/path/to/file.pdf>
                if len(parts) >= 2 and parts[0] == "d":
                    public_id = parts[1]
                    public_key = f"{parsed.scheme}://{parsed.netloc}/d/{public_id}"
                    api = f"https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key={quote(public_key, safe='')}"
                    if len(parts) > 2:
                        ypath = "/" + "/".join(parts[2:])
                        api += f"&path={quote(ypath, safe='')}"
                else:
                    api = f"https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key={quote(pdf_url, safe='')}"

                async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
                    rr = await client.get(api)
                    if rr.status_code == 200:
                        href = (rr.json() or {}).get("href")
                        if href:
                            dl_url = href
            async with httpx.AsyncClient(timeout=90.0, follow_redirects=True) as client:
                r = await client.get(dl_url, headers={"User-Agent": "Mozilla/5.0"})
                r.raise_for_status()
                ctype = (r.headers.get("Content-Type") or "").lower()
                blob = r.content
            if not blob:
                return ""
            # Если вернулся HTML, а не PDF — не пытаемся парсить как pdf
            if "pdf" not in ctype and not blob.startswith(b"%PDF"):
                return ""

            # Локальный импорт, чтобы не ломать старт сервиса если пакет отсутствует
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(blob))
            parts = []
            for p in reader.pages[:25]:
                t = (p.extract_text() or "").strip()
                if t:
                    parts.append(t)
            txt = "\n\n".join(parts).strip()
            return txt[:24000] if txt else ""
        except Exception as e:
            print(f"[pmanalyze] pdf download/extract error: {e}")
            return ""

    def _looks_like_yandex_listing(self, text: str) -> bool:
        if not text:
            return False
        t = text.lower()
        markers = ["яндекс диск", "yandex disk", "содержимое", "размер", "кб", "мб"]
        files_hits = len(re.findall(r"\.pdf", t))
        return (any(m in t for m in markers) and files_hits >= 3) or files_hits >= 8

    def _looks_like_refusal(self, text: str) -> bool:
        if not text:
            return False
        t = text.lower()
        bad = [
            "я вижу, что",
            "текст статьи",
            "не был передан",
            "вставьте текст",
            "список файлов",
            "содержимое яндекс диска",
            "чтобы выполнить задачу, мне нужен",
        ]
        return any(x in t for x in bad)

    async def generate_review(self, art: dict) -> tuple[str, str | None]:
        # Для Yandex: сначала бинарный PDF -> затем page fetch fallback.
        # Для прочих источников: page fetch -> pdf fallback.
        url_article = (art.get("url_article") or "").strip()
        pdf_url = (art.get("pdf_url") or "").strip()
        source = (art.get("source") or "").strip().lower()

        # Ссылка в промпте должна указывать на реальный PDF-документ.
        source_url = pdf_url or url_article

        page_text = ""

        if source.startswith("yandex_disk:"):
            page_text = await self._download_pdf_text(pdf_url)
            if not page_text:
                page_text = await self._fetch_page_text(pdf_url or source_url)
                if self._looks_like_yandex_listing(page_text):
                    page_text = ""
            if not page_text:
                page_text = await self._fetch_page_text(source_url)
                if self._looks_like_yandex_listing(page_text):
                    page_text = ""
        else:
            page_text = await self._fetch_page_text(source_url)
            if (not page_text or self._looks_like_yandex_listing(page_text)) and pdf_url and pdf_url != source_url:
                page_text = await self._fetch_page_text(pdf_url)
            if not page_text:
                page_text = await self._download_pdf_text(pdf_url)

        if not page_text:
            print(f"[pmanalyze] empty content for article_id={art.get('id')} source={art.get('source')} ext={art.get('external_id')}")
            return "", "content_unavailable_or_invalid_source_url"

        filename = f"{art.get('source','src')}_{art.get('external_id','id')}"
        authors_hint = self._normalize_authors(art.get("authors"))
        prompt = build_review_prompt(filename, source_url, page_text, authors_hint=authors_hint)
        review = await self._llm(self.settings.review_model, prompt, max_tokens=2500)
        if self._looks_like_refusal(review):
            print(f"[pmanalyze] refusal-like review for article_id={art.get('id')} source={art.get('source')}")
            return "", "llm_refusal_or_bad_input"
        return review, None

    async def _try_claim_parser_run(self) -> bool:
        async with self._parser_state_lock:
            if self._parser_running:
                return False
            self._parser_running = True
            return True

    async def _release_parser_run(self):
        async with self._parser_state_lock:
            self._parser_running = False

    async def _try_claim_reviews_run(self) -> bool:
        async with self._reviews_state_lock:
            if self._reviews_running:
                return False
            self._reviews_running = True
            return True

    async def _release_reviews_run(self):
        async with self._reviews_state_lock:
            self._reviews_running = False

    async def process_pending_prelocked(self, limit: int = 200) -> dict:
        """Выполнить обработку, предполагая что локи уже захвачены вызывающим кодом."""
        try:
            res = await self.process_pending(limit=limit)
            return {"ok": True, **res}
        finally:
            await self._unlock()
            await self._release_reviews_run()

    async def process_reviews_only_prelocked(self, limit: int = 200) -> dict:
        """Сгенерировать ревью только для уже релевантных статей без пересчета релевантности/категорий."""
        try:
            res = await self.process_reviews_only(limit=limit)
            return {"ok": True, **res}
        finally:
            await self._unlock()
            await self._release_reviews_run()

    async def process_pending_guarded(self, limit: int = 200) -> dict:
        # Защита от повторного старта в рамках одного контейнера
        if not await self._try_claim_reviews_run():
            print("[pmanalyze] process_pending already running in this instance, skip")
            return {"ok": False, "skipped": "already_running"}

        # Защита от параллельного старта между разными воркерами/пользователями
        if not await self._try_lock():
            await self._release_reviews_run()
            print("[pmanalyze] process_pending lock busy, skip")
            return {"ok": False, "skipped": "locked"}

        return await self.process_pending_prelocked(limit=limit)

    # ---------------- processing loop ----------------
    async def process_pending(self, limit: int = 200) -> dict:
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                """SELECT id FROM process_mining.papers_metadata
                   WHERE llm_status IN ('new','review_error')
                   ORDER BY created_at ASC, id ASC LIMIT $1""",
                max(1, int(limit)),
            )
        relevant = reviewed = irrelevant = errors = 0
        for r in rows:
            aid = int(r["id"])
            art = await self._get_article(aid)
            if not art:
                continue
            try:
                await self._set_status(aid, "llm_processing")
                rel = await self.assess_relevance(art)
                await self._save_relevance(aid, rel)
                if not rel["is_relevant"]:
                    irrelevant += 1
                    await self._set_status(aid, "irrelevant")
                    continue
                relevant += 1
                review_md, review_err = await self.generate_review(art)
                if not review_md:
                    errors += 1
                    await self._set_status(aid, "review_error", review_err or "review_generation_failed")
                    continue
                await self._save_review(aid, review_md)
                reviewed += 1
                await self._set_status(aid, "reviewed")
            except Exception as e:
                errors += 1
                await self._set_status(aid, "review_error", str(e)[:300])
        return {"relevant": relevant, "reviewed": reviewed,
                "irrelevant": irrelevant, "errors": errors}

    async def process_reviews_only(self, limit: int = 200) -> dict:
        """Собрать ревью для already-relevant без вызова assess_relevance()."""
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                """SELECT p.id
                   FROM process_mining.papers_metadata p
                   LEFT JOIN process_mining.reviews r ON r.article_id = p.id
                   WHERE p.is_relevant = TRUE
                     AND r.article_id IS NULL
                     AND p.llm_status IN ('done','review_error','llm_processing','new')
                   ORDER BY p.created_at ASC, p.id ASC
                   LIMIT $1""",
                max(1, int(limit)),
            )
        reviewed = errors = skipped = 0
        for r in rows:
            aid = int(r["id"])
            art = await self._get_article(aid)
            if not art:
                skipped += 1
                continue
            try:
                await self._set_status(aid, "llm_processing")
                review_md, review_err = await self.generate_review(art)
                if not review_md:
                    errors += 1
                    await self._set_status(aid, "review_error", review_err or "review_generation_failed")
                    continue
                await self._save_review(aid, review_md)
                reviewed += 1
                await self._set_status(aid, "reviewed")
            except Exception as e:
                errors += 1
                await self._set_status(aid, "review_error", str(e)[:300])
        return {"reviewed": reviewed, "errors": errors, "skipped": skipped, "total": len(rows)}

    # ---------------- db helpers ----------------
    async def _get_article(self, aid: int) -> dict | None:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("SELECT * FROM process_mining.papers_metadata WHERE id=$1", aid)
        return dict(row) if row else None

    async def _set_status(self, aid: int, status: str, err: str | None = None):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE process_mining.papers_metadata SET llm_status=$2, updated_at=now(), relevance_reasoning=COALESCE($3, relevance_reasoning) WHERE id=$1",
                aid, status, err,
            )

    async def _save_relevance(self, aid: int, rel: dict):
        async with self.pool.acquire() as con:
            await con.execute(
                """UPDATE process_mining.papers_metadata
                   SET relevance_score=$2, is_relevant=$3, relevance_reasoning=$4,
                       category=$5, processed_at=now(), updated_at=now()
                   WHERE id=$1""",
                aid, rel["overall_score"], rel["is_relevant"],
                rel["reasoning"], rel["category"],
            )

    async def _save_review(self, aid: int, review_md: str):
        async with self.pool.acquire() as con:
            await con.execute(
                """INSERT INTO process_mining.reviews (article_id, review_md, model_name)
                   VALUES ($1,$2,$3)
                   ON CONFLICT (article_id) DO UPDATE
                   SET review_md=EXCLUDED.review_md, model_name=EXCLUDED.model_name, created_at=now()""",
                aid, review_md, self.settings.review_model,
            )
            await con.execute(
                "UPDATE process_mining.papers_metadata SET review_flag=TRUE, updated_at=now() WHERE id=$1",
                aid,
            )
