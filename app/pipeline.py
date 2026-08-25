"""PMAnalyze pipeline: еженедельный сбор статей PM -> релевантность+категория -> PDF -> ревью (md).

Схема БД: process_mining (papers_metadata, reviews, parser_runs).
LLM: cloud.ru (anthropic/claude-sonnet-4.6) через /v1/messages.
Парсер: catchpm-browser (POST /api/parser/run, POST /api/pdf/fulltext).
"""
import asyncio
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

import httpx

from prompts import build_relevance_prompt, build_review_prompt, CATEGORIES


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
                    ext, src, a.get("title", ""), a.get("date_sub"),
                    a.get("url_article") or a.get("pdf_url"), a.get("pdf_url"),
                    json.dumps(authors) if authors is not None else None,
                    json.dumps(tags) if tags is not None else None,
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

    async def _fetch_pdf_text(self, pdf_url: str) -> str:
        if not pdf_url:
            return ""
        headers = {"Content-Type": "application/json"}
        if self.settings.parser_api_key:
            headers["X-Api-Key"] = self.settings.parser_api_key
        try:
            async with httpx.AsyncClient(timeout=self.settings.parser_timeout) as client:
                r = await client.post(self.settings.parser_pdf_url, headers=headers,
                                      json={"urls": [pdf_url], "max_pages": 4})
                r.raise_for_status()
                data = r.json()
            res = (data.get("results") or [{}])[0]
            return res.get("text", "") if res.get("ok") else ""
        except Exception as e:
            print(f"[pmanalyze] pdf fetch error: {e}")
            return ""

    async def generate_review(self, art: dict) -> str:
        pdf_url = art.get("pdf_url", "")
        pdf_text = await self._fetch_pdf_text(pdf_url)
        if not pdf_text:
            return ""
        filename = f"{art.get('source','src')}_{art.get('external_id','id')}"
        prompt = build_review_prompt(filename, pdf_url, pdf_text)
        return await self._llm(self.settings.review_model, prompt, max_tokens=2500)

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
                review_md = await self.generate_review(art)
                if not review_md:
                    errors += 1
                    await self._set_status(aid, "review_error")
                    continue
                await self._save_review(aid, review_md)
                reviewed += 1
                await self._set_status(aid, "reviewed")
            except Exception as e:
                errors += 1
                await self._set_status(aid, "review_error", str(e)[:300])
        return {"relevant": relevant, "reviewed": reviewed,
                "irrelevant": irrelevant, "errors": errors}

    # ---------------- db helpers ----------------
    async def _get_article(self, aid: int) -> dict | None:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("SELECT * FROM process_mining.papers_metadata WHERE id=$1", aid)
        return dict(row) if row else None

    async def _set_status(self, aid: int, status: str, err: str | None = None):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE process_mining.papers_metadata SET llm_status=$2, updated_at=now() WHERE id=$1",
                aid, status,
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
