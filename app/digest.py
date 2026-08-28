"""Формирование дайджеста PMAnalyze: календарные периоды + email HTML + выгрузка полного markdown в OBS."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta, time as dtime
from html import escape
from zoneinfo import ZoneInfo
import os
import re
import json
import ast
import uuid
from dotenv import load_dotenv
from pathlib import Path

PRESET_TITLE = {"week": "за неделю", "month": "за месяц", "quarter": "за квартал", "sub_week": "за 7 дней до прошлого вторника"}

# подхватываем локальный .env (для OBS), если есть
load_dotenv(Path("/app/.env"))

# Ретро-палитра (совпадает с дашбордом)
INK = "#1E1A16"
PAPER = "#FAF3E7"
ORANGE = "#FF5A1F"
CAT_COLORS = {
    "Общие принципы": "#FFD9C7",
    "Методики исследования": "#CDE8D5",
    "PM-лог": "#FBE7A1",
    "Проверка качества": "#E3D5F5",
    "Без категории": "#CFE6F7",
}


def period_bounds(preset: str, tz_name: str = "Europe/Moscow"):
    """Границы периода для дайджеста.

    week     -> текущая календарная неделя (с понедельника 00:00 до now)
    month    -> текущий календарный месяц (с 1-го числа 00:00 до now)
    quarter  -> текущий календарный квартал (с 1-го числа квартала 00:00 до now)
    sub_week -> 7 полных дней ДО прошлого вторника (включая оба края):
                [прошлый вторник-7д ; прошлый вторник-1д], Europe/Moscow.
    """
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)

    p = (preset or "week").lower()

    if p == "sub_week":
        # Вторник в Python weekday() == 1.
        # "Прошлый вторник" = строго до текущей даты:
        # если сегодня вторник -> берём вторник прошлой недели.
        today = now_local.date()
        days_since_tue = (today.weekday() - 1) % 7
        if days_since_tue == 0:
            days_since_tue = 7
        prev_tuesday = today - timedelta(days=days_since_tue)

        end_day = prev_tuesday - timedelta(days=1)     # понедельник перед прошлым вторником
        start_day = end_day - timedelta(days=6)        # ровно 7 дней

        start_local = datetime.combine(start_day, dtime.min, tz)
        end_local = datetime.combine(end_day, dtime.max, tz)

    elif p == "month":
        start_local = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_local = now_local

    elif p == "quarter":
        q_start_month = ((now_local.month - 1) // 3) * 3 + 1
        start_local = now_local.replace(month=q_start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        end_local = now_local

    else:
        # week
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        start_local = start_local.fromordinal(start_local.toordinal() - now_local.weekday()).replace(
            tzinfo=tz, hour=0, minute=0, second=0, microsecond=0
        )
        end_local = now_local

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    return start_utc, end_utc, start_local, end_local


def _normalize_authors(authors):
    if authors is None:
        return []

    def _uniq(vals):
        out, seen = [], set()
        for a in vals:
            t = str(a).strip().strip('"\'')
            if not t:
                continue
            k = t.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(t)
        return out

    if isinstance(authors, list):
        vals = authors
    elif isinstance(authors, dict):
        # Кейс из UI: авторы могут храниться прямо в ключах dict.
        keys = [k for k in authors.keys() if str(k).strip()]
        if keys:
            vals = keys
        elif isinstance(authors.get("list"), list):
            vals = authors.get("list")
        elif isinstance(authors.get("LIST"), list):
            vals = authors.get("LIST")
        else:
            vals = []
    else:
        t = str(authors).strip()
        if not t:
            vals = []
        elif (t.startswith("{") and t.endswith("}")) or (t.startswith("[") and t.endswith("]")):
            parsed = None
            try:
                parsed = json.loads(t)
            except Exception:
                try:
                    parsed = ast.literal_eval(t)
                except Exception:
                    parsed = None
            vals = _normalize_authors(parsed) if parsed is not None else [x.strip() for x in re.split(r"[,;]", t) if x.strip()]
        else:
            vals = [x.strip() for x in re.split(r"[,;]", t) if x.strip()]

    return _uniq(vals)


async def collect_digest(pool, preset: str) -> dict:
    """Собрать данные дайджеста:
    - в выборку попадают статьи по фронтовой дате (p.date_sub), как в колонке "Дата"
    - в письмо/OBS выводим только 1 (последнее) review на статью
    """
    date_from_utc, date_to_utc, date_from_local, date_to_local = period_bounds(preset)

    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT p.id, p.title, p.category, p.relevance_score, p.source,
                   p.url_article, p.pdf_url, p.authors, p.created_at, p.date_sub,
                   r.review_md, r.reviewed_at
            FROM process_mining.papers_metadata p
            JOIN LATERAL (
                SELECT rv.review_md, rv.created_at AS reviewed_at
                FROM process_mining.reviews rv
                WHERE rv.article_id = p.id
                ORDER BY rv.created_at DESC
                LIMIT 1
            ) r ON TRUE
            WHERE p.date_sub >= $1::date
              AND p.date_sub <= $2::date
              AND p.is_relevant = TRUE
            ORDER BY p.date_sub DESC NULLS LAST, p.relevance_score DESC NULLS LAST, p.id DESC
            """,
            date_from_local.date(), date_to_local.date(),
        )

        totals = await con.fetchrow(
            """
            SELECT count(*) FILTER (WHERE p.date_sub >= $1::date AND p.date_sub <= $2::date) AS new_articles,
                   count(*) FILTER (WHERE p.date_sub >= $1::date AND p.date_sub <= $2::date AND p.is_relevant = TRUE) AS relevant_articles,
                   count(*) FILTER (
                       WHERE p.date_sub >= $1::date AND p.date_sub <= $2::date
                         AND p.is_relevant = TRUE
                         AND EXISTS (SELECT 1 FROM process_mining.reviews r WHERE r.article_id = p.id)
                   ) AS reviewed_relevant
            FROM process_mining.papers_metadata p
            """,
            date_from_local.date(), date_to_local.date(),
        )

    arts = []
    for r in rows:
        d = dict(r)
        d["authors"] = _normalize_authors(d.get("authors"))
        arts.append(d)

    return {
        "preset": preset,
        "date_from": date_from_local.date(),
        "date_to": date_to_local.date(),
        "date_from_local": date_from_local,
        "date_to_local": date_to_local,
        "articles": arts,
        "new_articles": int(totals["new_articles"]) if totals else 0,
        "relevant_articles": int(totals["relevant_articles"]) if totals else 0,
        "reviewed_relevant": int(totals["reviewed_relevant"]) if totals else 0,
    }


def _review_excerpt_2_3(md: str) -> str:
    """Сильно сокращаем ревью: оставляем примерно 1/3 исходного текста."""
    if not md:
        return ""
    txt = md.replace("\r", "").strip()
    if not txt:
        return ""

    keep = int(len(txt) * 0.33)
    keep = max(280, min(keep, 2100))
    if len(txt) <= keep:
        return txt

    cut = txt[:keep]
    # стараемся резать по границе абзаца, потом по предложению
    last_para = cut.rfind("\n\n")
    if last_para > 300:
        cut = cut[:last_para]
    else:
        last_dot = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
        if last_dot > 250:
            cut = cut[: last_dot + 1]
    return cut.rstrip() + "\n…"


def render_reviews_markdown(data: dict) -> str:
    """Полный markdown со склейкой ревью за период (карточки подряд)."""
    preset = data.get("preset", "week")
    title_ru = PRESET_TITLE.get(preset, "за период")
    df = data["date_from_local"].strftime("%d.%m.%Y %H:%M")
    dt = data["date_to_local"].strftime("%d.%m.%Y %H:%M")

    lines = []
    lines.append(f"# PMAnalyze Digest ({title_ru})")
    lines.append(f"Период: {df} — {dt} (Europe/Moscow)")
    lines.append(f"Получено статей: {data.get('new_articles', 0)}")
    lines.append(f"Релевантных: {data.get('relevant_articles', 0)}")
    lines.append(f"С ревью: {len(data.get('articles', []))}")
    lines.append("")

    for i, a in enumerate(data.get("articles", []), start=1):
        title = (a.get("title") or "Без названия").strip()
        cat = a.get("category") or "Без категории"
        score = a.get("relevance_score")
        score_txt = f"{score:.2f}" if isinstance(score, (int, float)) else "—"
        date_sub = a.get("date_sub")
        reviewed = a.get("reviewed_at")
        if date_sub is None:
            created = a.get("created_at")
            date_txt = created.astimezone(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y") if created else "—"
        else:
            # date_sub может быть date/datetime/str
            if hasattr(date_sub, "strftime"):
                date_txt = date_sub.strftime("%d.%m.%Y")
            else:
                ds = str(date_sub)[:10]
                y,m,d = (ds.split("-")+["",""])[:3]
                date_txt = f"{d}.{m}.{y}" if y and m and d else str(date_sub)
        reviewed_txt = reviewed.astimezone(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y %H:%M") if reviewed else "—"
        authors = ", ".join(a.get("authors") or []) or "—"
        link = a.get("url_article") or a.get("pdf_url") or ""

        lines.append(f"## {i}. {title}")
        lines.append(f"- Категория: {cat}")
        lines.append(f"- Relevance score: {score_txt}")
        lines.append(f"- Дата: {date_txt}")
        lines.append(f"- Ревью подготовлено: {reviewed_txt}")
        lines.append(f"- Авторы: {authors}")
        lines.append(f"- Ссылка: {link if link else '—'}")
        lines.append("")
        lines.append("### Ревью")
        lines.append((a.get("review_md") or "").strip())
        lines.append("\n---\n")

    return "\n".join(lines)


def upload_markdown_to_obs(md_text: str, preset: str, date_from_local: datetime, date_to_local: datetime) -> dict:
    """Загрузка markdown в OBS. Возвращает dict {ok, obs_uri, public_url, key, error}."""
    try:
        import boto3
        from botocore.client import Config
    except Exception as e:
        return {"ok": False, "error": f"boto3_import_error: {e}"}

    bucket = os.getenv("OBS_OPEN_BUCKET") or os.getenv("OBS_BUCKET")
    endpoint = os.getenv("OBS_ENDPOINT", "").strip()
    region = os.getenv("OBS_REGION", "ru-moscow")
    access_key = os.getenv("OBS_ACCESS_KEY", "").strip()
    secret_key = os.getenv("OBS_SECRET_KEY", "").strip()

    if not all([bucket, endpoint, access_key, secret_key]):
        return {"ok": False, "error": "obs_env_not_configured"}

    p = (preset or "period").lower()
    pfrom = date_from_local.strftime("%Y%m%d")
    pto = date_to_local.strftime("%Y%m%d")
    uniq = uuid.uuid4().hex[:8]
    filename = f"digest_{p}_{pfrom}_{pto}_{uniq}.md"
    key = f"PM/digests/{filename}"

    session = boto3.session.Session()
    s3 = session.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(s3={"addressing_style": "virtual"}),
    )

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=md_text.encode("utf-8"),
        ContentType="text/markdown; charset=utf-8",
    )

    ep = endpoint.replace("https://", "").replace("http://", "").rstrip("/")
    public_url = f"https://{bucket}.{ep}/{key}"
    return {
        "ok": True,
        "bucket": bucket,
        "key": key,
        "obs_uri": f"obs://{bucket}/{key}",
        "public_url": public_url,
    }


def render_digest_html(data: dict) -> str:
    preset = data["preset"]
    df = data["date_from_local"].strftime("%d.%m.%Y")
    dt = data["date_to_local"].strftime("%d.%m.%Y")
    arts = data["articles"]
    title_ru = PRESET_TITLE.get(preset, "за период")

    n_rev = len(arts)
    avg_rel = round(sum((a.get("relevance_score") or 0) for a in arts) / n_rev, 2) if n_rev else 0.0

    cards = []
    for a in arts:
        cat = a.get("category") or "Без категории"
        cat_bg = CAT_COLORS.get(cat, "#CFE6F7")
        score = a.get("relevance_score")
        score_txt = f"{score:.2f}" if score is not None else "—"
        link = a.get("url_article") or a.get("pdf_url") or "#"

        title = escape(a.get("title", "") or "Без названия")
        excerpt = escape(_review_excerpt_2_3(a.get("review_md", "")))
        date_sub = a.get("date_sub")
        reviewed = a.get("reviewed_at")
        if date_sub is None:
            created = a.get("created_at")
            dtxt = created.astimezone(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y") if created else "—"
        else:
            if hasattr(date_sub, "strftime"):
                dtxt = date_sub.strftime("%d.%m.%Y")
            else:
                ds = str(date_sub)[:10]
                y,m,d = (ds.split("-")+["",""])[:3]
                dtxt = f"{d}.{m}.{y}" if y and m and d else str(date_sub)
        rtxt = reviewed.astimezone(ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y") if reviewed else "—"
        authors = ", ".join(a.get("authors") or []) or "—"

        cards.append(f"""
        <tr><td style="padding:0 0 18px 0;">
          <table width="100%" cellpadding="0" cellspacing="0" style="border:2px solid {INK};background:{PAPER};">
            <tr><td style="padding:16px 18px;">
              <span style="display:inline-block;background:{cat_bg};border:2px solid {INK};padding:2px 8px;
                     font-family:'Space Mono',monospace;font-size:11px;letter-spacing:0.15em;text-transform:uppercase;">{escape(cat)}</span>
              <span style="display:inline-block;background:{ORANGE};color:#fff;border:2px solid {INK};padding:2px 8px;margin-left:6px;
                     font-family:'Space Mono',monospace;font-size:11px;letter-spacing:0.1em;">REL {score_txt}</span>
              <div style="font-family:Georgia,serif;font-size:19px;font-weight:700;color:{INK};margin:12px 0 6px;line-height:1.25;">{title}</div>
              <div style="font-family:'Space Mono',monospace;font-size:11px;color:#6b6055;letter-spacing:0.04em;">
                Дата: {dtxt} · Дата ревью: {rtxt}
              </div>
              <div style="font-family:Arial,sans-serif;font-size:13px;color:#6b6055;line-height:1.45;margin-top:5px;overflow-wrap:anywhere;word-break:break-word;">
                Авторы: {escape(authors)}
              </div>
              <div style="font-family:Arial,sans-serif;font-size:14px;color:#4a4038;line-height:1.5;margin-top:10px;overflow-wrap:anywhere;word-break:break-word;white-space:pre-wrap;">{excerpt}</div>
              <div style="margin-top:10px;font-family:'Space Mono',monospace;font-size:11px;letter-spacing:0.1em;text-transform:uppercase;color:{ORANGE};">
                <a href="{escape(link)}" style="color:{ORANGE};text-decoration:none;overflow-wrap:anywhere;word-break:break-word;">→ читать источник</a>
              </div>
            </td></tr>
          </table>
        </td></tr>""")

    cards_html = "".join(cards) if cards else f"""
      <tr><td style="padding:24px;text-align:center;font-family:'Space Mono',monospace;
             font-size:13px;letter-spacing:0.1em;color:{INK};border:2px dashed {INK};">
        ЗА ЭТОТ ПЕРИОД РЕВЬЮ НЕ ПОДГОТОВЛЕНО
      </td></tr>"""

    full_list_url = data.get("full_list_url") or ""
    full_list_block = ""
    if full_list_url:
        full_list_block = f"""
        <tr><td style="padding:10px 0 18px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="border:2px solid {INK};background:#CFE6F7;"><tr><td style="padding:12px 14px;">
            <div style="font-family:'Space Mono',monospace;font-size:11px;letter-spacing:0.1em;color:{INK};text-transform:uppercase;">Полный список ревью (Markdown)</div>
            <div style="margin-top:6px;font-family:Arial,sans-serif;font-size:14px;overflow-wrap:anywhere;word-break:break-word;"><a href="{escape(full_list_url)}" style="color:{ORANGE};overflow-wrap:anywhere;word-break:break-word;">{escape(full_list_url)}</a></div>
          </td></tr></table>
        </td></tr>
        """

    return f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>PMAnalyze Digest</title></head>
<body style="margin:0;padding:0;background:#e8ddc8;width:100% !important;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#e8ddc8;padding:12px 0;table-layout:fixed;">
<tr><td align="center">
  <table width="100%" cellpadding="0" cellspacing="0" style="max-width:640px;width:100%;margin:0 auto;">

    <tr><td style="background:{INK};padding:26px 24px;">
      <div style="font-family:'Space Mono',monospace;font-size:11px;letter-spacing:0.35em;text-transform:uppercase;color:{ORANGE};">Process Mining Practice</div>
      <div style="font-family:Georgia,serif;font-size:34px;font-weight:800;color:{PAPER};margin-top:6px;line-height:1;">PMAnalyze — Дайджест</div>
      <div style="font-family:'Space Mono',monospace;font-size:12px;letter-spacing:0.15em;color:#c9beac;margin-top:10px;">{title_ru.upper()} · {df} — {dt}</div>
    </td></tr>

    <tr><td style="padding:18px 0;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td width="33%" style="padding-right:8px;"><table width="100%" style="border:2px solid {INK};background:#FFD9C7;"><tr><td style="padding:14px;text-align:center;">
            <div style="font-family:Georgia,serif;font-size:30px;font-weight:800;color:{INK};">{n_rev}</div>
            <div style="font-family:'Space Mono',monospace;font-size:10px;letter-spacing:0.2em;text-transform:uppercase;color:{INK};margin-top:4px;">ревью в письме</div>
        </td></tr></table></td>
        <td width="33%" style="padding:0 4px;"><table width="100%" style="border:2px solid {INK};background:#CDE8D5;"><tr><td style="padding:14px;text-align:center;">
            <div style="font-family:Georgia,serif;font-size:30px;font-weight:800;color:{INK};">{data.get('relevant_articles', 0)}</div>
            <div style="font-family:'Space Mono',monospace;font-size:10px;letter-spacing:0.2em;text-transform:uppercase;color:{INK};margin-top:4px;">релевантных получено</div>
        </td></tr></table></td>
        <td width="33%" style="padding-left:8px;"><table width="100%" style="border:2px solid {INK};background:#FBE7A1;"><tr><td style="padding:14px;text-align:center;">
            <div style="font-family:Georgia,serif;font-size:30px;font-weight:800;color:{INK};">{avg_rel}</div>
            <div style="font-family:'Space Mono',monospace;font-size:10px;letter-spacing:0.2em;text-transform:uppercase;color:{INK};margin-top:4px;">avg relevance</div>
        </td></tr></table></td>
      </tr></table>
    </td></tr>

    {full_list_block}

    <tr><td style="padding:6px 0 14px;">
      <div style="font-family:'Space Mono',monospace;font-size:12px;letter-spacing:0.25em;text-transform:uppercase;color:{INK};border-bottom:2px solid {INK};padding-bottom:8px;">Подготовленные ревью</div>
    </td></tr>

    {cards_html}

    <tr><td style="background:{ORANGE};padding:16px 24px;margin-top:12px;">
      <div style="font-family:'Space Mono',monospace;font-size:11px;letter-spacing:0.2em;text-transform:uppercase;color:#fff;">Автоматический дайджест PMAnalyze · сгенерировано {datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M UTC')}</div>
    </td></tr>

  </table>
</td></tr></table>
</body></html>"""
