import asyncio
import hashlib
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Header
from pydantic import BaseModel

app = FastAPI(title="catchpm-browser-parser-service")
EXPECTED_KEY = os.getenv("PARSER_SERVICE_API_KEY", "").strip()

from pm_articles_router import router as pm_router
app.include_router(pm_router)


@app.get("/health")
async def health():
    return {"ok": True}


# ============ GOLD BROWSER SCAN v2 (appended to parser_service.py) ============
import re as _gold_re
from datetime import datetime as _gdt, timezone as _gtz, timedelta as _gtd

GOLD_SOURCES = {
    "kitco_news":            "https://www.kitco.com/news/",
    "kitco_main":            "https://www.kitco.com/",
    "world_gold_council":    "https://www.gold.org/goldhub",
    "tradingeconomics_gold": "https://tradingeconomics.com/commodity/gold",
    "metalsdaily":           "https://www.metalsdaily.com/",
}

GOLD_KEY_TERMS = (
    "gold", "xau", "bullion", "precious", "fed", "fomc", "inflation",
    "cpi", "rates", "yield", "yields", "dollar", "usd", "treasury",
    "rally", "record", "ounce", "safe haven", "jackson hole", "pce",
)

# JS-сборщик: заголовок + href + дата из атрибутов ИЛИ из текста карточки (relative/absolute)
GOLD_EVAL_JS = r"""
() => {
  const out = [];
  const seen = new Set();
  const sels = [
    'article a', 'h1 a', 'h2 a', 'h3 a',
    'a[href*="/news/"]', 'a[href*="/article"]', 'a[href*="/commodity"]',
    '[class*="headline"] a', '[class*="title"] a', '[class*="card"] a',
    '.te-news a', '.calendar a'
  ];
  const nodes = document.querySelectorAll(sels.join(','));

  // Регэкспы для дат внутри текста карточки
  const reRelative = /\b(\d{1,2})\s*(min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks)\s*(ago)?\b/i;
  const reWords    = /\b(just now|today|yesterday|a day ago|an hour ago|a minute ago)\b/i;
  const reAbsolute = /\b((Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(,?\s+\d{4})?)\b/i;
  const reISO      = /\b(\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?)?)\b/;

  const findDateAttr = (el) => {
    let n = el;
    for (let up = 0; up < 5 && n; up++) {
      if (n.querySelector) {
        const t = n.querySelector('time[datetime]');
        if (t) return t.getAttribute('datetime');
      }
      if (n.getAttribute) {
        const d = n.getAttribute('datetime') || n.getAttribute('data-date') ||
                  n.getAttribute('data-published') || n.getAttribute('data-timestamp') ||
                  n.getAttribute('data-time');
        if (d) return d;
      }
      n = n.parentElement;
    }
    return null;
  };

  const findDateText = (el) => {
    let n = el;
    for (let up = 0; up < 5 && n; up++) {
      const txt = (n.innerText || n.textContent || '').slice(0, 400);
      let m = txt.match(reISO) || txt.match(reRelative) || txt.match(reWords) || txt.match(reAbsolute);
      if (m) return m[0];
      n = n.parentElement;
    }
    return null;
  };

  for (const a of nodes) {
    const title = (a.innerText || a.textContent || '').replace(/\s+/g, ' ').trim();
    let href = a.href || '';
    if (!title || title.length < 16 || title.length > 300) continue;
    const key = title.slice(0, 120).toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    const dAttr = findDateAttr(a);
    const dText = dAttr ? null : findDateText(a);
    out.push({ title, url: href, date_raw: dAttr, date_text: dText });
    if (out.length >= 120) break;
  }
  return out;
};
"""

_GOLD_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _gold_parse_relative(raw):
    """Разбор 'today'/'yesterday'/'2h ago'/'5 hours ago'/'3 days ago' -> datetime."""
    if not raw:
        return None
    low = raw.strip().lower()
    now = _gdt.now(_gtz.utc)
    if "just now" in low or "a minute ago" in low or "an hour ago" in low:
        return now
    if "today" in low:
        return now
    if "yesterday" in low or "a day ago" in low:
        return now - _gtd(days=1)
    m = _gold_re.search(
        r"(\d{1,2})\s*(min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks)",
        low,
    )
    if m:
        num = int(m.group(1))
        unit = m.group(2)
        if unit.startswith("min"):
            return now - _gtd(minutes=num)
        if unit in ("h", "hr", "hrs", "hour", "hours"):
            return now - _gtd(hours=num)
        if unit in ("d", "day", "days"):
            return now - _gtd(days=num)
        if unit in ("w", "week", "weeks"):
            return now - _gtd(weeks=num)
    return None


def _gold_parse_absolute_text(raw):
    """Разбор 'Aug 23', 'Aug 23, 2026', 'August 23 2026' -> datetime (год = текущий, если нет)."""
    if not raw:
        return None
    txt = raw.strip()
    now = _gdt.now(_gtz.utc)
    m = _gold_re.search(
        r"([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:,?\s+(\d{4}))?",
        txt,
    )
    if not m:
        return None
    mon = _GOLD_MONTHS.get(m.group(1)[:3].lower())
    if not mon:
        return None
    day = int(m.group(2))
    year = int(m.group(3)) if m.group(3) else now.year
    try:
        dt = _gdt(year, mon, day, tzinfo=_gtz.utc)
        # если дата "в будущем" из-за отсутствия года — откатываем на год назад
        if dt > now + _gtd(days=1):
            dt = dt.replace(year=year - 1)
        return dt
    except Exception:
        return None


def _gold_parse_date(raw, raw_text=None):
    """Основной разбор: сперва машинные форматы из date_raw, затем текстовые из date_text."""
    def _machine(r):
        if not r:
            return None
        r = r.strip()
        fmts = ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d",
                "%d %b %Y", "%d %B %Y", "%b %d, %Y", "%B %d, %Y")
        if r.isdigit() and len(r) >= 10:
            try:
                return _gdt.fromtimestamp(int(r[:10]), tz=_gtz.utc)
            except Exception:
                pass
        for f in fmts:
            try:
                dt = _gdt.strptime(r, f)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_gtz.utc)
                return dt
            except Exception:
                continue
        try:
            return _gdt.fromisoformat(r.replace("Z", "+00:00"))
        except Exception:
            return None

    dt = _machine(raw)
    if dt:
        return dt, "confirmed"
    # текстовые из date_text
    if raw_text:
        dt = _machine(raw_text)
        if dt:
            return dt, "confirmed"
        dt = _gold_parse_relative(raw_text)
        if dt:
            return dt, "relative"
        dt = _gold_parse_absolute_text(raw_text)
        if dt:
            return dt, "text"
    return None, None


def _gold_score(title):
    low = title.lower()
    return sum(1 for k in GOLD_KEY_TERMS if k in low)


async def _gold_scan_source(browser, source_key, url, within_hours, max_items):
    now = _gdt.now(_gtz.utc)
    cutoff = now - _gtd(hours=within_hours)
    ctx = await browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"),
        locale="en-US",
        viewport={"width": 1440, "height": 2200},
    )
    page = await ctx.new_page()
    result = {"source": source_key, "url": url, "ok": False, "status": None,
              "error": None, "items": [], "undated": []}
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        result["status"] = resp.status if resp else None
        try:
            await page.wait_for_timeout(2500)
        except Exception:
            pass
        raw_items = await page.evaluate(GOLD_EVAL_JS)
        result["ok"] = True
        fresh, undated, seen = [], [], set()
        for it in raw_items:
            title = (it.get("title") or "").strip()
            href = it.get("url") or url
            if _gold_score(title) == 0:
                continue
            k = title[:120].lower()
            if k in seen:
                continue
            seen.add(k)
            dt, conf = _gold_parse_date(it.get("date_raw"), it.get("date_text"))
            row = {"title": title, "url": href,
                   "date": dt.isoformat() if dt else None,
                   "date_confidence": conf or "unconfirmed"}
            if dt is None:
                # дата не определена — считаем "вероятно свежим" (листинги показывают свежее)
                undated.append(row)
            elif dt >= cutoff:
                fresh.append(row)
            # если dt старше cutoff — отбрасываем (реально старая новость)
        fresh.sort(key=lambda r: _gold_score(r["title"]), reverse=True)
        undated.sort(key=lambda r: _gold_score(r["title"]), reverse=True)
        result["items"] = fresh[:max_items]
        result["undated"] = undated[:max_items]
    except Exception as e:
        result["error"] = str(e)[:300]
    finally:
        await page.close()
        await ctx.close()
    return result


# ===================== FETCH_PAGES_PATCH =====================
# Открывает список ссылок в браузере по очереди, чистит HTML, отдаёт текст.
from bs4 import BeautifulSoup as _FP_BS

_FP_DROP_TAGS = ["script", "style", "noscript", "svg", "header", "footer",
                 "nav", "form", "iframe", "aside", "button", "figure"]


def _fp_clean_html(html: str, max_chars: int) -> str:
    soup = _FP_BS(html, "html.parser")
    for t in soup(_FP_DROP_TAGS):
        t.decompose()
    node = soup.find("article") or soup.find("main") or soup.body or soup
    txt = node.get_text(" ", strip=True) if node else ""
    txt = _gold_re.sub(r"\s+", " ", txt).strip()
    return txt[:max_chars]


async def _fp_fetch_one(browser, url, wait_ms, timeout_ms, max_chars):
    ctx = await browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"),
        locale="en-US",
        viewport={"width": 1440, "height": 2000},
    )
    page = await ctx.new_page()
    row = {"url": url, "ok": False, "status": None, "error": None,
           "title": None, "excerpt": "", "chars": 0}
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        row["status"] = resp.status if resp else None
        try:
            await page.wait_for_timeout(wait_ms)
        except Exception:
            pass
        try:
            row["title"] = (await page.title() or "").strip()[:300]
        except Exception:
            pass
        html = await page.content()
        txt = _fp_clean_html(html, max_chars)
        row["excerpt"] = txt
        row["chars"] = len(txt)
        row["ok"] = True
    except Exception as e:
        row["error"] = str(e)[:300]
    finally:
        try:
            await page.close()
        except Exception:
            pass
        try:
            await ctx.close()
        except Exception:
            pass
    return row


class FetchPagesPayload(BaseModel):
    urls: list[str] = []
    max_chars: int = 1500
    wait_ms: int = 1500
    timeout_ms: int = 30000
    max_urls: int = 25


@app.post("/api/fetch/pages")
async def fetch_pages(payload: FetchPagesPayload, x_api_key: Optional[str] = Header(default=None)):
    if EXPECTED_KEY and x_api_key != EXPECTED_KEY:
        return {"ok": False, "error": "unauthorized"}
    from playwright.async_api import async_playwright
    urls, seen = [], set()
    for u in payload.urls:
        u = (u or "").strip()
        if u and u.startswith("http") and u not in seen:
            seen.add(u)
            urls.append(u)
        if len(urls) >= max(1, min(int(payload.max_urls), 40)):
            break
    max_chars = max(200, min(int(payload.max_chars), 6000))
    wait_ms = max(0, min(int(payload.wait_ms), 6000))
    timeout_ms = max(5000, min(int(payload.timeout_ms), 60000))
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
        try:
            for u in urls:
                results.append(await _fp_fetch_one(browser, u, wait_ms, timeout_ms, max_chars))
        finally:
            await browser.close()
    ok_n = sum(1 for r in results if r["ok"])
    return {"ok": True, "requested": len(urls), "fetched_ok": ok_n,
            "results": results,
            "generated_at": _gdt.now(_gtz.utc).isoformat()}
# =================== END FETCH_PAGES_PATCH ===================


class GoldScanPayload(BaseModel):
    within_hours: int = 24
    max_items: int = 10


@app.post("/api/gold/scan")
async def gold_scan(payload: GoldScanPayload, x_api_key: Optional[str] = Header(default=None)):
    if EXPECTED_KEY and x_api_key != EXPECTED_KEY:
        return {"ok": False, "error": "unauthorized"}
    from playwright.async_api import async_playwright
    within = max(1, min(int(payload.within_hours), 168))
    mx = max(1, min(int(payload.max_items), 25))
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"])
        try:
            results = []
            for key, url in GOLD_SOURCES.items():
                results.append(await _gold_scan_source(browser, key, url, within, mx))
        finally:
            await browser.close()
    ok = sum(1 for r in results if r["ok"])
    fresh_total = sum(len(r["items"]) for r in results)
    undated_total = sum(len(r["undated"]) for r in results)
    return {"ok": True, "within_hours": within, "sources_ok": ok,
            "sources_total": len(results),
            "items_total": fresh_total,
            "undated_total": undated_total,
            "generated_at": _gdt.now(_gtz.utc).isoformat(), "sources": results}
