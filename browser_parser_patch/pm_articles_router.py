"""
PM articles parser — подключаемый патч-модуль для сервиса catchpm-browser.

Устанавливает два эндпоинта поверх существующего FastAPI-приложения парсера:
    POST /api/parser/run     — сбор свежих статей Process Mining
                               (arxiv API/HTML, fluxicon, google scholar)
    POST /api/pdf/fulltext   — первые N (=4) страниц PDF через pypdf

Всё "железо" парсинга (httpx, BeautifulSoup, playwright, pypdf) живёт ЗДЕСЬ,
внутри сервиса-парсера. PMAnalyze дёргает эти эндпоинты по докер-сети.

ПОДКЛЮЧЕНИЕ (в parser_service.py контейнера catchpm-browser):
    from pm_articles_router import router as pm_router
    app.include_router(pm_router)

Зависимости в образе: httpx, beautifulsoup4, playwright, pypdf.
Аутентификация: заголовок X-Api-Key должен совпадать с env PARSER_SERVICE_API_KEY
(если переменная задана; иначе проверка пропускается).
"""
import asyncio
import hashlib
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, Header
from pydantic import BaseModel

router = APIRouter()
EXPECTED_KEY = os.getenv("PARSER_SERVICE_API_KEY", "").strip()

import re as _pm_re
from dataclasses import asdict as _pm_asdict, dataclass as _pm_dataclass
from urllib.parse import urlencode as _pm_urlencode
from xml.etree import ElementTree as _pm_ET

PM_QUERY_DEFAULT: str = (
    '"process mining" OR '
    '"process discovery" OR '
    '"event logs" OR '
    '"process model" OR '
    '"audit trail" OR '
    '"conformance checking" OR '
    '"process enhancement" OR '
    '"root cause analysis" OR '
    '"process monitoring" OR '
    '"compliance checking" OR '
    '"deviation analysis" OR '
    '"continuous auditing" OR '
    '"business process analysis" OR '
    '"process intelligence" OR '
    '"task mining" OR '
    '"operational analytics" OR '
    '"financial auditing" OR '
    '"internal audit" OR '
    '"process compliance" OR '
    '"audit log analysis"'
)


@_pm_dataclass
class PMArticle:
    source: str
    external_id: str
    title: str
    date_sub: str
    pdf_url: str
    authors: dict | None
    tags: str | list | None
    abstract: str
    category: str | None


class PMParserService:
    def __init__(self):
        self.timeout_http = float(os.getenv("PARSER_HTTP_TIMEOUT_SECONDS", "40"))
        self.timeout_scholar = float(os.getenv("PARSER_SCHOLAR_TIMEOUT_SECONDS", "60"))

    async def collect_all_sources(self, query: str, max_per_source: int, ui_lang: str) -> dict:
        tasks = [
            self.parse_arxiv_api(query, max_per_source),
            self.parse_arxiv_html(query, max_per_source),
            self.parse_fluxicon(max_per_source),
            self.parse_google_scholar_browser(query, max_per_source, ui_lang),
        ]
        chunks = await asyncio.gather(*tasks, return_exceptions=True)

        out: list[PMArticle] = []
        source_stats: dict[str, int] = {}
        errors: dict[str, str] = {}
        sources = ["arxiv_api", "arxiv_html", "fluxicon", "google_scholar"]

        for src, chunk in zip(sources, chunks):
            if isinstance(chunk, Exception):
                errors[src] = str(chunk)[:300]
                source_stats[src] = 0
                continue
            source_stats[src] = len(chunk)
            out.extend(chunk)

        uniq = {}
        for a in out:
            if not a.external_id:
                a.external_id = hashlib.md5((a.source + a.title).encode()).hexdigest()
            uniq[(a.source, a.external_id)] = a

        articles = [_pm_asdict(a) for a in uniq.values()]
        return {
            "ok": True,
            "query": query,
            "sources": source_stats,
            "errors": errors,
            "total": len(articles),
            "articles": articles,
        }

    async def parse_arxiv_api(self, query: str, max_per_source: int) -> list[PMArticle]:
        q = f"all:({query})"
        params = {
            "search_query": q,
            "start": "0",
            "max_results": str(max_per_source),
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        url = "https://export.arxiv.org/api/query?" + _pm_urlencode(params)
        async with httpx.AsyncClient(timeout=self.timeout_http, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": "catchpm-parser-service/1.0"})
            r.raise_for_status()
            text = r.text

        ns = {"atom": "http://www.w3.org/2005/Atom"}
        root = _pm_ET.fromstring(text)
        out: list[PMArticle] = []
        for e in root.findall("atom:entry", ns):
            id_url = (e.findtext("atom:id", "", ns) or "").strip()
            external_id = id_url.rsplit("/", 1)[-1] if id_url else hashlib.md5(id_url.encode()).hexdigest()
            title = _pm_re.sub(r"\s+", " ", (e.findtext("atom:title", "", ns) or "")).strip()
            abstract = _pm_re.sub(r"\s+", " ", (e.findtext("atom:summary", "", ns) or "")).strip()
            published = (e.findtext("atom:published", "", ns) or "")[:10]

            pdf_url = ""
            for l in e.findall("atom:link", ns):
                if l.attrib.get("title") == "pdf":
                    pdf_url = l.attrib.get("href", "")
                    break
            if not pdf_url and external_id:
                pdf_url = f"https://arxiv.org/pdf/{external_id}.pdf"

            authors = []
            for a in e.findall("atom:author", ns):
                nm = (a.findtext("atom:name", "", ns) or "").strip()
                if nm:
                    authors.append(nm)

            out.append(
                PMArticle(
                    source="arxiv_api",
                    external_id=external_id,
                    title=title,
                    date_sub=published or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    pdf_url=pdf_url,
                    authors={"list": authors} if authors else None,
                    tags=["arxiv", "api"],
                    abstract=abstract,
                    category="process_mining",
                )
            )
        return out

    async def parse_arxiv_html(self, query: str, max_per_source: int) -> list[PMArticle]:
        params = {"query": query, "searchtype": "all", "source": "header", "start": "0"}
        url = "https://arxiv.org/search/?" + _pm_urlencode(params)
        async with httpx.AsyncClient(timeout=self.timeout_http, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            html = r.text

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        out: list[PMArticle] = []
        cards = soup.select("li.arxiv-result")[:max_per_source]
        for c in cards:
            title_el = c.select_one("p.title")
            title = _pm_re.sub(r"\s+", " ", title_el.get_text(" ", strip=True) if title_el else "").strip()
            abs_el = c.select_one("span.abstract-full")
            abstract = _pm_re.sub(r"\s+", " ", abs_el.get_text(" ", strip=True) if abs_el else "").strip()

            id_el = c.select_one("p.list-title a")
            href = id_el.get("href", "") if id_el else ""
            external_id = href.rsplit("/", 1)[-1] if href else hashlib.md5(title.encode()).hexdigest()
            pdf_url = f"https://arxiv.org/pdf/{external_id}.pdf" if external_id else ""

            date_txt = ""
            sub_el = c.select_one("p.is-size-7")
            if sub_el:
                m = _pm_re.search(r"Submitted\s+(\d+\s+\w+\s+\d{4})", sub_el.get_text(" ", strip=True))
                if m:
                    try:
                        date_txt = datetime.strptime(m.group(1), "%d %B %Y").strftime("%Y-%m-%d")
                    except Exception:
                        date_txt = ""

            authors = [a.get_text(" ", strip=True) for a in c.select("p.authors a")]
            out.append(
                PMArticle(
                    source="arxiv_html",
                    external_id=external_id,
                    title=title,
                    date_sub=date_txt or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    pdf_url=pdf_url,
                    authors={"list": authors} if authors else None,
                    tags=["arxiv", "html"],
                    abstract=abstract,
                    category="process_mining",
                )
            )
        return out

    async def parse_fluxicon(self, max_per_source: int) -> list[PMArticle]:
        base_url = "https://www.fluxicon.com"
        url = f"{base_url}/blog/"
        async with httpx.AsyncClient(timeout=self.timeout_http, follow_redirects=True) as client:
            r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            html = r.text

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        out: list[PMArticle] = []
        items = soup.select("article, .post")[:max_per_source]
        for it in items:
            a = it.select_one("h2 a, h3 a, .entry-title a")
            if not a:
                continue
            href = a.get("href", "").strip()
            if href.startswith("/"):
                href = base_url + href
            title = a.get_text(" ", strip=True)
            if not title:
                continue
            full_text = it.get_text(" ", strip=True)
            if "process" not in full_text.lower() and "mining" not in full_text.lower():
                continue

            ext = href.rstrip("/").rsplit("/", 1)[-1] or hashlib.md5(href.encode()).hexdigest()
            out.append(
                PMArticle(
                    source="fluxicon",
                    external_id=ext,
                    title=title,
                    date_sub=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    pdf_url=href,
                    authors=None,
                    tags=["blog", "fluxicon"],
                    abstract=full_text[:1200],
                    category="process_mining",
                )
            )
        return out

    def build_scholar_url(self, query: str, start: int, ui_lang: str = "ru", fulltext: bool = False) -> str:
        params = {
            "hl": ui_lang,
            "as_sdt": "1,5" if fulltext else "0,5",
            "q": query,
            "scisbd": "1",
            "start": str(start),
        }
        return "https://scholar.google.com/scholar?" + _pm_urlencode(params)

    async def parse_google_scholar_browser(self, query: str, max_per_source: int, ui_lang: str) -> list[PMArticle]:
        from playwright.async_api import async_playwright

        url = self.build_scholar_url(query, start=0, ui_lang=ui_lang, fulltext=False)
        out: list[PMArticle] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            ctx = await browser.new_context()
            page = await ctx.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=int(self.timeout_scholar * 1000))
                cards = await page.query_selector_all("div.gs_r.gs_or.gs_scl")
                for card in cards[:max_per_source]:
                    title_el = await card.query_selector("h3.gs_rt")
                    if not title_el:
                        continue
                    title = (await title_el.inner_text()).strip()
                    link_el = await card.query_selector("h3.gs_rt a")
                    href = await link_el.get_attribute("href") if link_el else ""
                    abs_el = await card.query_selector("div.gs_rs")
                    abstract = (await abs_el.inner_text()).strip() if abs_el else ""
                    meta_el = await card.query_selector("div.gs_a")
                    meta = (await meta_el.inner_text()).strip() if meta_el else ""
                    ext = hashlib.md5((href or title).encode()).hexdigest()
                    out.append(
                        PMArticle(
                            source="google_scholar",
                            external_id=ext,
                            title=title,
                            date_sub=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            pdf_url=href or "",
                            authors={"meta": meta} if meta else None,
                            tags=["scholar", "browser"],
                            abstract=abstract,
                            category="process_mining",
                        )
                    )
            finally:
                await page.close()
                await ctx.close()
                await browser.close()
        return out


pm_svc = PMParserService()


class ParserRunPayload(BaseModel):
    query: str = PM_QUERY_DEFAULT
    max_per_source: int = 20
    ui_lang: str = "en"


@router.post("/api/parser/run")
async def run_parser(payload: ParserRunPayload, x_api_key: Optional[str] = Header(default=None)):
    if EXPECTED_KEY and x_api_key != EXPECTED_KEY:
        return {"ok": False, "error": "unauthorized"}
    q = (payload.query or PM_QUERY_DEFAULT).strip()
    max_per_source = max(1, min(int(payload.max_per_source), 30))
    ui_lang = (payload.ui_lang or "en").strip()[:5]

    started = int(datetime.now(timezone.utc).timestamp())
    result = await pm_svc.collect_all_sources(q, max_per_source, ui_lang)
    result["started_at"] = started
    result["finished_at"] = int(datetime.now(timezone.utc).timestamp())
    return result


# ---- PDF fulltext: первые N страниц статьи (по умолчанию 4) ----
def _pm_normalize_pdf_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    lu = u.lower()
    if lu.endswith(".pdf"):
        return u
    if "arxiv.org/abs/" in lu:
        return u.replace("/abs/", "/pdf/").rstrip("/") + ".pdf"
    return u  # прочие источники: пробуем как есть


async def _pm_fetch_pdf_excerpt(pdf_url: str, max_pages: int, max_chars: int) -> dict:
    norm = _pm_normalize_pdf_url(pdf_url)
    row = {"url": pdf_url, "norm_url": norm, "ok": False, "error": None,
           "pages_read": 0, "chars": 0, "text": ""}
    if not norm:
        row["error"] = "empty_or_unsupported_url"
        return row
    try:
        from pypdf import PdfReader
    except Exception as e:
        row["error"] = f"pypdf_missing: {e}"
        return row
    try:
        import io
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            r = await client.get(norm, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            data = r.content
        ctype = (r.headers.get("content-type") or "").lower()
        if "pdf" not in ctype and bytes(data[:5]) != b"%PDF-":
            row["error"] = f"not_pdf(content-type={ctype[:60]})"
            return row
        reader = PdfReader(io.BytesIO(data))
        parts = []
        n = 0
        for page in reader.pages[:max_pages]:
            t = page.extract_text() or ""
            if t:
                parts.append(t)
            n += 1
        text = "\n".join(parts)[:max_chars]
        row.update({"ok": True, "pages_read": n, "chars": len(text), "text": text})
    except Exception as e:
        row["error"] = str(e)[:300]
    return row


class PdfFulltextPayload(BaseModel):
    urls: list[str] = []
    max_pages: int = 4
    max_chars: int = 20000
    max_urls: int = 25


@router.post("/api/pdf/fulltext")
async def pdf_fulltext(payload: PdfFulltextPayload, x_api_key: Optional[str] = Header(default=None)):
    if EXPECTED_KEY and x_api_key != EXPECTED_KEY:
        return {"ok": False, "error": "unauthorized"}
    urls, seen = [], set()
    for u in payload.urls:
        u = (u or "").strip()
        if u and u.startswith("http") and u not in seen:
            seen.add(u)
            urls.append(u)
        if len(urls) >= max(1, min(int(payload.max_urls), 40)):
            break
    max_pages = max(1, min(int(payload.max_pages), 12))
    max_chars = max(500, min(int(payload.max_chars), 60000))
    results = []
    for u in urls:
        results.append(await _pm_fetch_pdf_excerpt(u, max_pages, max_chars))
    ok_n = sum(1 for r in results if r["ok"])
    return {"ok": True, "requested": len(urls), "read_ok": ok_n,
            "results": results,
            "generated_at": datetime.now(timezone.utc).isoformat()}
