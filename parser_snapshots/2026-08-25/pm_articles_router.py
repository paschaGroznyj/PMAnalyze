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

    async def _parse_scholar_combined(self, query: str, max_per_source: int, ui_lang: str) -> list["PMArticle"]:
        """Google Scholar: сначала ScrapingDog API, при пустом результате —
        fallback на браузерный скрейп."""
        try:
            res = await self.parse_scholar_scrapingdog(query, max_per_source)
        except Exception:
            res = []
        if res:
            return res
        return await self.parse_google_scholar_browser(query, max_per_source, ui_lang)

    async def collect_all_sources(self, query: str, max_per_source: int, ui_lang: str) -> dict:
        tasks = [
            self.parse_arxiv_api(query, max_per_source),
            self.parse_arxiv_html(query, max_per_source),
            self.parse_crossref(query, max_per_source),
            self.parse_core_api(query, max_per_source),
            self.parse_fluxicon(max_per_source),
            self._parse_scholar_combined(query, max_per_source, ui_lang),
        ]
        chunks = await asyncio.gather(*tasks, return_exceptions=True)

        out: list[PMArticle] = []
        source_stats: dict[str, int] = {}
        errors: dict[str, str] = {}
        sources = ["arxiv_api", "arxiv_html", "crossref", "core_api", "fluxicon", "google_scholar"]

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
    async def parse_crossref(self, query: str, max_per_source: int) -> list[PMArticle]:
        # Crossref лучше работает на коротких тематических запросах,
        # чем на длинных OR-выражениях.
        topical_queries = [
            "process mining",
            "event log process mining",
            "conformance checking",
            "process discovery",
            "task mining",
            "object-centric event log",
            "business process intelligence",
            "process mining automation",
        ]
        if query and len(query.strip()) <= 80:
            topical_queries.insert(0, query.strip())

        allowlist = (
            "process mining", "event log", "event logs", "conformance checking",
            "process discovery", "task mining", "object-centric", "ocel",
            "process model", "business process", "workflow mining", "process intelligence",
            "process analytics", "rpa", "robotic process automation",
        )
        blocklist = (
            "project management", "product management", "pm2.5", "particulate matter",
            "preventive maintenance", "predictive maintenance", "portfolio management",
        )

        def _is_pm_candidate(title: str, abstract: str, subjects: list[str]) -> bool:
            txt = f"{title} {abstract} {' '.join(subjects)}".lower()
            if any(b in txt for b in blocklist):
                return False
            score = 0
            score += sum(1 for k in allowlist if k in txt)
            # Минимум 1 сигнал в title/abstract/subject
            return score >= 1

        rows_per_query = max(15, min(int(max_per_source) * 4, 120))
        url = "https://api.crossref.org/works"
        mailto = os.getenv("CROSSREF_MAILTO", "pisoldatkin@sberbank.ru").strip()
        headers = {
            "User-Agent": f"catchpm-parser-service/1.1 (mailto:{mailto})",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=self.timeout_http, follow_redirects=True) as client:
            tasks = []
            for tq in topical_queries:
                params = {
                    "query.bibliographic": tq,
                    "rows": str(rows_per_query),
                    "sort": "published",
                    "order": "desc",
                    "select": "DOI,title,abstract,author,published-print,published-online,issued,URL,link,type,subject",
                    "filter": "from-pub-date:2018-01-01",
                }
                tasks.append(client.get(url, params=params, headers=headers))
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        out: list[PMArticle] = []
        for resp in responses:
            if isinstance(resp, Exception):
                continue
            try:
                resp.raise_for_status()
                data = resp.json() or {}
            except Exception:
                continue

            items = ((data.get("message") or {}).get("items") or [])
            for it in items:
                doi = (it.get("DOI") or "").strip()
                title_raw = (it.get("title") or [])
                title = ""
                if isinstance(title_raw, list) and title_raw:
                    title = _pm_re.sub(r"\s+", " ", str(title_raw[0])).strip()
                elif isinstance(title_raw, str):
                    title = _pm_re.sub(r"\s+", " ", title_raw).strip()
                if not title:
                    continue

                # Дата публикации. Приоритет — фактически опубликованные даты
                # (online/issued) над плановым published-print, который у Crossref
                # часто указывает на будущие выпуски журналов.
                _today = datetime.now(timezone.utc).date()
                _parsed_dates = []
                for k in ("published-online", "issued", "published-print"):
                    dp = (it.get(k) or {}).get("date-parts") or []
                    if dp and isinstance(dp[0], list) and dp[0]:
                        y = int(dp[0][0])
                        m = int(dp[0][1]) if len(dp[0]) > 1 else 1
                        d = int(dp[0][2]) if len(dp[0]) > 2 else 1
                        try:
                            _parsed_dates.append(datetime(y, m, d, tzinfo=timezone.utc).date())
                        except Exception:
                            pass

                # Берём самую свежую НЕ будущую дату; если все даты в будущем —
                # статья считается ещё не опубликованной и отбрасывается.
                _valid_dates = [dt for dt in _parsed_dates if dt <= _today]
                if _parsed_dates and not _valid_dates:
                    continue
                date_sub = max(_valid_dates).strftime("%Y-%m-%d") if _valid_dates else ""

                authors_list = []
                for a in (it.get("author") or []):
                    g = (a.get("given") or "").strip()
                    f = (a.get("family") or "").strip()
                    full = (g + " " + f).strip()
                    if full:
                        authors_list.append(full)

                pdf_url = (it.get("URL") or "").strip()
                for l in (it.get("link") or []):
                    ct = (l.get("content-type") or "").lower()
                    if "pdf" in ct and l.get("URL"):
                        pdf_url = l.get("URL")
                        break

                abstract = _pm_re.sub(r"\s+", " ", (it.get("abstract") or "")).strip()
                if abstract:
                    abstract = _pm_re.sub(r"</?[^>]+>", " ", abstract)
                    abstract = _pm_re.sub(r"\s+", " ", abstract).strip()

                subj = it.get("subject") or []
                subjects = [str(x) for x in subj if x][:5] if isinstance(subj, list) else []
                if not _is_pm_candidate(title, abstract, subjects):
                    continue

                tags = ["crossref", str(it.get("type") or "work")]
                if subjects:
                    tags.extend(subjects[:3])

                out.append(
                    PMArticle(
                        source="crossref",
                        external_id=doi or hashlib.md5((title + pdf_url).encode()).hexdigest(),
                        title=title,
                        date_sub=date_sub or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        pdf_url=pdf_url,
                        authors={"list": authors_list} if authors_list else None,
                        tags=tags,
                        abstract=abstract,
                        category="process_mining",
                    )
                )

        # дедуп и ограничение
        uniq = {}
        for a in out:
            uniq[(a.source, a.external_id)] = a
        ranked = list(uniq.values())
        ranked.sort(key=lambda x: x.date_sub, reverse=True)
        return ranked[:max_per_source]

    async def parse_core_api(self, query: str, max_per_source: int) -> list[PMArticle]:
        """CORE API v3 /search/works.

        Особенности источника:
        - требует Bearer CORE_API_KEY
        - чувствителен к широким запросам (возможны 500/timeout), поэтому используем
          узкие title-based запросы и мягкие ретраи с backoff
        """
        api_key = os.getenv("CORE_API_KEY", "").strip()
        if not api_key:
            return []

        base_url = "https://api.core.ac.uk/v3/search/works"
        this_year = datetime.now(timezone.utc).year
        page_size = max(5, min(int(max_per_source), 10))

        allowlist = (
            "process mining", "event log", "event logs", "conformance checking",
            "process discovery", "task mining", "object-centric", "ocel",
            "process model", "business process", "workflow mining", "process intelligence",
            "process analytics", "audit", "compliance checking", "deviation analysis",
        )
        blocklist = (
            "project management", "product management", "pm2.5", "particulate matter",
            "preventive maintenance", "predictive maintenance", "portfolio management",
        )

        def _norm(txt: str) -> str:
            return _pm_re.sub(r"\s+", " ", (txt or "")).strip()

        def _is_pm(title: str, abstract: str) -> bool:
            txt = f"{title} {abstract}".lower()
            if any(b in txt for b in blocklist):
                return False
            return any(k in txt for k in allowlist)

        def _extract_year(item: dict) -> str:
            yp = item.get("yearPublished")
            if isinstance(yp, int):
                y = yp
            else:
                pd = str(item.get("publishedDate") or "")
                m = _pm_re.search(r"\b(19|20)\d{2}\b", pd)
                y = int(m.group(0)) if m else this_year
            y = min(max(y, 1900), this_year)
            return f"{y}-01-01"

        # CORE иногда отдаёт timeout на широких формулировках; начинаем с узкого запроса.
        queries = [
            '(title:"process mining") AND (title:"event log" OR title:"conformance checking" OR title:"task mining" OR title:"OCEL" OR title:"process discovery")',
            '(title:"process mining") AND (title:"event log" OR title:"conformance checking" OR title:"task mining")',
            'title:"process mining"',
        ]
        q_user = (query or "").strip()
        if q_user and len(q_user) <= 64 and '"' not in q_user:
            queries.insert(0, f'title:"{q_user}"')

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": "catchpm-parser-service/1.2",
        }

        uniq: dict[str, PMArticle] = {}

        async with httpx.AsyncClient(timeout=self.timeout_http, follow_redirects=True) as client:
            for q in queries:
                offset = 0
                # максимум 3 страницы на один вариант запроса
                for _ in range(3):
                    params = {"q": q, "limit": page_size, "offset": offset}
                    data = None
                    for attempt in range(3):
                        try:
                            resp = await client.get(base_url, headers=headers, params=params)
                            if resp.status_code in (429, 500, 502, 503, 504):
                                await asyncio.sleep(1.2 * (attempt + 1))
                                continue
                            resp.raise_for_status()
                            data = resp.json() or {}
                            break
                        except Exception:
                            if attempt >= 2:
                                data = None
                            else:
                                await asyncio.sleep(1.2 * (attempt + 1))

                    if not data:
                        break

                    batch = data.get("results") or []
                    if not batch:
                        break

                    for item in batch:
                        title = _norm(item.get("title") or "")
                        if not title:
                            continue
                        abstract = _norm(item.get("abstract") or "")
                        if not _is_pm(title, abstract):
                            continue

                        download_url = (item.get("downloadUrl") or "").strip()
                        item_id = str(item.get("id") or "").strip()
                        doi = (item.get("doi") or "").strip()
                        ext = doi or item_id or hashlib.md5((title + download_url).encode()).hexdigest()

                        authors_list = []
                        for a in (item.get("authors") or []):
                            if isinstance(a, dict):
                                nm = (a.get("name") or "").strip()
                                if nm:
                                    authors_list.append(nm)
                            elif isinstance(a, str) and a.strip():
                                authors_list.append(a.strip())

                        tags = ["core", "works"]
                        if item.get("isOpenAccess") is True:
                            tags.append("open_access")

                        art = PMArticle(
                            source="core_api",
                            external_id=ext,
                            title=title,
                            date_sub=_extract_year(item),
                            pdf_url=download_url,
                            authors={"list": authors_list} if authors_list else None,
                            tags=tags,
                            abstract=abstract,
                            category="process_mining",
                        )
                        uniq[f"{art.source}:{art.external_id}"] = art

                        if len(uniq) >= max_per_source:
                            break

                    if len(uniq) >= max_per_source:
                        break
                    offset += page_size
                if len(uniq) >= max_per_source:
                    break

        ranked = list(uniq.values())
        ranked.sort(key=lambda x: x.date_sub, reverse=True)
        return ranked[:max_per_source]

    async def core_enrich_by_doi_or_title(self, doi: str = "", title: str = "", limit: int = 5) -> dict:
        """Точечное обогащение из CORE по DOI и/или названию.

        Возвращает компактные метаданные + лучший матч.
        """
        api_key = os.getenv("CORE_API_KEY", "").strip()
        if not api_key:
            return {"ok": False, "error": "core_api_key_missing", "results": [], "total": 0}

        doi = (doi or "").strip()
        title = _pm_re.sub(r"\s+", " ", (title or "")).strip()
        if not doi and not title:
            return {"ok": False, "error": "empty_query", "results": [], "total": 0}

        base_url = "https://api.core.ac.uk/v3/search/works"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": "catchpm-parser-service/1.2",
        }

        def _safe(s: str) -> str:
            return (s or "").replace('"', ' ').strip()

        queries = []
        if doi:
            d = _safe(doi)
            queries.extend([
                f'doi:"{d}"',
                f'"{d}"',
            ])
        if title:
            t = _safe(title)
            if t:
                queries.extend([
                    f'title:"{t}"',
                    f'"{t}"',
                ])
                # мягкое укорочение для длинных title
                toks = [x for x in t.split() if len(x) > 2][:8]
                if toks:
                    queries.append(" ".join(toks))

        # remove duplicates preserving order
        dedup_q = []
        seen_q = set()
        for q in queries:
            if q and q not in seen_q:
                seen_q.add(q)
                dedup_q.append(q)
        queries = dedup_q[:6]

        def _compact(item: dict) -> dict:
            au = []
            for a in (item.get("authors") or []):
                if isinstance(a, dict):
                    nm = (a.get("name") or "").strip()
                    if nm:
                        au.append(nm)
                elif isinstance(a, str) and a.strip():
                    au.append(a.strip())
            t = _pm_re.sub(r"\s+", " ", str(item.get("title") or "")).strip()
            ab = _pm_re.sub(r"\s+", " ", str(item.get("abstract") or "")).strip()
            return {
                "id": item.get("id"),
                "doi": item.get("doi"),
                "title": t,
                "yearPublished": item.get("yearPublished"),
                "publishedDate": item.get("publishedDate"),
                "downloadUrl": item.get("downloadUrl"),
                "isOpenAccess": item.get("isOpenAccess"),
                "citationCount": item.get("citationCount"),
                "authors": au,
                "abstract": ab[:1400],
            }

        def _score(it: dict) -> int:
            s = 0
            doi_it = (it.get("doi") or "").lower().strip()
            title_it = (it.get("title") or "").lower().strip()
            if doi and doi_it == doi.lower():
                s += 100
            if title:
                t = title.lower()
                if t and t == title_it:
                    s += 60
                if t and t in title_it:
                    s += 30
            if it.get("downloadUrl"):
                s += 5
            if it.get("isOpenAccess") is True:
                s += 5
            return s

        uniq = {}
        errors = []
        req_limit = max(1, min(int(limit or 5), 10))

        async with httpx.AsyncClient(timeout=self.timeout_http, follow_redirects=True) as client:
            for q in queries:
                params = {"q": q, "limit": req_limit, "offset": 0}
                data = None
                for attempt in range(3):
                    try:
                        resp = await client.get(base_url, headers=headers, params=params)
                        if resp.status_code in (429, 500, 502, 503, 504):
                            await asyncio.sleep(1.0 * (attempt + 1))
                            continue
                        resp.raise_for_status()
                        data = resp.json() or {}
                        break
                    except Exception as e:
                        if attempt >= 2:
                            errors.append(f"q={q[:80]} err={str(e)[:120]}")
                        else:
                            await asyncio.sleep(1.0 * (attempt + 1))
                if not data:
                    continue

                for item in (data.get("results") or []):
                    c = _compact(item)
                    key = (str(c.get("doi") or "").lower().strip() or str(c.get("id") or "") or hashlib.md5((c.get("title") or "").encode()).hexdigest())
                    if key not in uniq:
                        uniq[key] = c

        ranked = list(uniq.values())
        ranked.sort(key=_score, reverse=True)
        ranked = ranked[:req_limit]

        return {
            "ok": True,
            "query": {"doi": doi, "title": title},
            "total": len(ranked),
            "best_match": ranked[0] if ranked else None,
            "results": ranked,
            "errors": errors[:10],
        }

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

    async def parse_scholar_scrapingdog(self, query: str, max_per_source: int) -> list[PMArticle]:
        """Google Scholar через ScrapingDog API (надёжнее браузерного скрейпа).

        Берём только статьи с PDF. Год из displayed_link. Даты из будущего
        отбрасываются на общем рубеже upsert; здесь дополнительно клампим год.
        """
        api_key = os.getenv("SCRAPINGDOG_API_KEY", "").strip()
        if not api_key:
            return []

        base_url = "https://api.scrapingdog.com/google_scholar"
        this_year = datetime.now(timezone.utc).year
        results = max(10, min(int(max_per_source) * 2, 20))

        allowlist = (
            "process mining", "event log", "event logs", "conformance checking",
            "process discovery", "task mining", "object-centric", "ocel",
            "process model", "business process", "workflow mining", "process intelligence",
            "process analytics", "audit", "compliance checking", "deviation analysis",
        )
        blocklist = (
            "project management", "product management", "pm2.5", "particulate matter",
            "preventive maintenance", "portfolio management",
        )

        def _is_pm(title: str, abstract: str) -> bool:
            txt = f"{title} {abstract}".lower()
            if any(b in txt for b in blocklist):
                return False
            return any(k in txt for k in allowlist)

        def _year(displayed_link: str) -> str:
            m = _pm_re.search(r"\b(19|20)\d{2}\b", displayed_link or "")
            return m.group(0) if m else ""

        params = {
            "api_key": api_key,
            "query": query,
            "results": results,
            "page": 0,
            "language": "en",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout_scholar, follow_redirects=True) as client:
                resp = await client.get(base_url, params=params)
                resp.raise_for_status()
                data = resp.json() or {}
        except Exception:
            return []

        out: list[PMArticle] = []
        for item in (data.get("scholar_results") or []):
            pdf_url = ""
            for res in (item.get("resources") or []):
                if (res.get("type") or "").upper() == "PDF" and res.get("link"):
                    pdf_url = res.get("link")
                    break
            if not pdf_url:
                continue

            title = (item.get("title") or "").strip()
            if not title:
                continue
            snippet = (item.get("snippet") or "").strip()
            if not _is_pm(title, snippet):
                continue

            title_link = (item.get("title_link") or "").strip()
            scholar_id = (item.get("id") or "").strip()
            authors = [a.get("name", "") for a in (item.get("authors") or []) if a.get("name")]

            yr = _year(item.get("displayed_link") or "")
            date_sub = ""
            if yr:
                try:
                    y_int = min(int(yr), this_year)
                    date_sub = f"{y_int}-01-01"
                except Exception:
                    date_sub = ""

            cited_by = (
                ((item.get("inline_links") or {}).get("cited_by") or {}).get("total") or ""
            )
            tags = ["scholar", "scrapingdog"]
            if cited_by:
                tags.append(f"cited_by:{cited_by}")

            out.append(
                PMArticle(
                    source="google_scholar",
                    external_id=scholar_id or hashlib.md5((title + pdf_url).encode()).hexdigest(),
                    title=title,
                    date_sub=date_sub or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    pdf_url=pdf_url,
                    authors={"list": authors} if authors else None,
                    tags=tags,
                    abstract=snippet,
                    category="process_mining",
                )
            )

        uniq = {}
        for a in out:
            uniq[a.external_id] = a
        return list(uniq.values())[:max_per_source]

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


class CoreEnrichPayload(BaseModel):
    doi: str = ""
    title: str = ""
    limit: int = 5


@router.post("/api/core/enrich")
async def core_enrich(payload: CoreEnrichPayload, x_api_key: Optional[str] = Header(default=None)):
    if EXPECTED_KEY and x_api_key != EXPECTED_KEY:
        return {"ok": False, "error": "unauthorized"}
    doi = (payload.doi or "").strip()
    title = (payload.title or "").strip()
    limit = max(1, min(int(payload.limit), 10))
    return await pm_svc.core_enrich_by_doi_or_title(doi=doi, title=title, limit=limit)


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


_PM_BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def _pm_extract_pdf_text(data: bytes, max_pages: int, max_chars: int) -> dict:
    """Достаёт текст из PDF-байтов через pypdf. Возвращает {pages_read, chars, text}."""
    import io
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    parts, n = [], 0
    for page in reader.pages[:max_pages]:
        t = page.extract_text() or ""
        if t:
            parts.append(t)
        n += 1
    text = "\n".join(parts)[:max_chars]
    return {"pages_read": n, "chars": len(text), "text": text}


async def _pm_fetch_pdf_via_playwright(url: str, max_pages: int, max_chars: int) -> dict:
    """
    Fallback для источников, отдающих 403 / HTML-лендинг вместо PDF по httpx
    (openalex/crossref/zenodo и пр.). Открывает страницу реальным браузером,
    перехватывает первый PDF-ответ через page.on('response') и парсит его.
    Прямой PDF-URL тоже пробуем открыть в браузере — часть сайтов отдаёт байты
    только в контексте сессии с настоящим UA.
    """
    from playwright.async_api import async_playwright
    grabbed = {"data": None}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        ctx = await browser.new_context(
            user_agent=_PM_BROWSER_UA,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = await ctx.new_page()

        async def _on_resp(resp):
            try:
                if grabbed["data"] is not None:
                    return
                ct = (resp.headers.get("content-type") or "").lower()
                if "pdf" in ct:
                    body = await resp.body()
                    if body[:5] == b"%PDF-":
                        grabbed["data"] = body
            except Exception:
                pass

        page.on("response", _on_resp)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(3500)

            # Если PDF не перехвачен пассивно — ищем прямую ссылку на файл
            # на лендинге (zenodo/repo-страницы кладут .pdf в <a href>).
            if grabbed["data"] is None:
                try:
                    hrefs = await page.eval_on_selector_all(
                        "a",
                        "els => els.map(e => e.href).filter(h => h && "
                        "(h.toLowerCase().includes('.pdf') || "
                        "h.toLowerCase().includes('/files/')))",
                    )
                except Exception:
                    hrefs = []
                seen_h = set()
                for h in hrefs:
                    if not h or h in seen_h:
                        continue
                    seen_h.add(h)
                    try:
                        resp = await ctx.request.get(h, timeout=45000)
                        ct = (resp.headers.get("content-type") or "").lower()
                        body = await resp.body()
                        if ("pdf" in ct or body[:5] == b"%PDF-") and body[:5] == b"%PDF-":
                            grabbed["data"] = body
                            break
                    except Exception:
                        continue
        except Exception:
            pass
        finally:
            await browser.close()

    if grabbed["data"] is None:
        return {"ok": False, "error": "playwright_no_pdf_captured"}
    try:
        res = _pm_extract_pdf_text(grabbed["data"], max_pages, max_chars)
        return {"ok": True, "via": "playwright", **res}
    except Exception as e:
        return {"ok": False, "error": f"playwright_pdf_parse_error: {str(e)[:200]}"}


async def _pm_fetch_pdf_excerpt(pdf_url: str, max_pages: int, max_chars: int) -> dict:
    norm = _pm_normalize_pdf_url(pdf_url)
    row = {"url": pdf_url, "norm_url": norm, "ok": False, "error": None,
           "pages_read": 0, "chars": 0, "text": "", "via": None}
    if not norm:
        row["error"] = "empty_or_unsupported_url"
        return row
    try:
        from pypdf import PdfReader  # noqa: F401 (проверка наличия)
    except Exception as e:
        row["error"] = f"pypdf_missing: {e}"
        return row

    # 1) быстрый путь: httpx
    httpx_error = None
    try:
        import io
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            r = await client.get(norm, headers={
                "User-Agent": _PM_BROWSER_UA,
                "Accept": "application/pdf,text/html,*/*",
            })
        if r.status_code == 200:
            data = r.content
            ctype = (r.headers.get("content-type") or "").lower()
            if "pdf" in ctype or bytes(data[:5]) == b"%PDF-":
                res = _pm_extract_pdf_text(data, max_pages, max_chars)
                row.update({"ok": True, "via": "httpx", **res})
                return row
            httpx_error = f"not_pdf(content-type={ctype[:60]})"
        else:
            httpx_error = f"http_{r.status_code}"
    except Exception as e:
        httpx_error = str(e)[:200]

    # 2) fallback: playwright-рендер (обходит часть 403 / JS-лендингов)
    try:
        pw = await _pm_fetch_pdf_via_playwright(norm, max_pages, max_chars)
        if pw.get("ok"):
            row.update({"ok": True, "via": "playwright",
                        "pages_read": pw["pages_read"], "chars": pw["chars"],
                        "text": pw["text"]})
            return row
        row["error"] = f"httpx:{httpx_error}; playwright:{pw.get('error')}"
    except Exception as e:
        row["error"] = f"httpx:{httpx_error}; playwright_exc:{str(e)[:200]}"
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
