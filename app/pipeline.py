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

from app.search import embed_text, EMBED_MODEL, TS_CONFIG


def _to_date(value):
    """Парсит строку/дату в datetime.date для asyncpg ($4 date_sub).
    Возвращает None, если распарсить нельзя (в БД пойдёт NULL).
    Даты из будущего отбрасываются (возвращается None) — защита от
    published-print будущих выпусков журналов (Crossref и пр.)."""
    def _guard_future(d):
        if d is None:
            return None
        # допускаем небольшой люфт на разницу часовых поясов
        if d > (datetime.now(timezone.utc).date() + timedelta(days=1)):
            return None
        return d

    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return _guard_future(value)
    if isinstance(value, datetime):
        return _guard_future(value.date())
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d.%m.%Y", "%d %b %Y", "%d %B %Y"):
        try:
            return _guard_future(datetime.strptime(s, fmt).date())
        except ValueError:
            continue
    try:
        return _guard_future(datetime.fromisoformat(s.replace("Z", "+00:00")).date())
    except ValueError:
        return None

from app.prompts import build_relevance_prompt, build_review_prompt, build_annotation_translation_prompt, CATEGORIES
from app.kg_prompts import (
    build_knowledge_extraction_prompt,
    build_relation_inference_prompt,
    build_cross_link_prompt,
    build_wiki_synthesis_prompt,
    build_knowledge_lint_prompt,
    RELATION_TYPES,
)


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

    # KG merge/collapse settings
    kg_collapse_every_articles: int = int(os.getenv("KG_COLLAPSE_EVERY_ARTICLES", "10"))
    kg_exact_min_norm_len: int = int(os.getenv("KG_EXACT_MIN_NORM_LEN", "120"))
    kg_semantic_threshold: float = float(os.getenv("KG_SEMANTIC_MERGE_THRESHOLD", "0.93"))
    kg_semantic_pair_limit: int = int(os.getenv("KG_SEMANTIC_PAIR_LIMIT", "120"))

    # KG relation density control
    kg_relation_min_score: float = float(os.getenv("KG_RELATION_MIN_SCORE", "0.75"))
    kg_relates_to_min_score: float = float(os.getenv("KG_RELATES_TO_MIN_SCORE", "0.82"))
    kg_cross_min_vec_sim: float = float(os.getenv("KG_CROSS_MIN_VEC_SIM", "0.68"))
    kg_generic_target_min_score: float = float(os.getenv("KG_GENERIC_TARGET_MIN_SCORE", "0.88"))
    kg_generic_in_degree_cap: int = int(os.getenv("KG_GENERIC_IN_DEGREE_CAP", "28"))

    # Safety checker for relates_to deletion in pruning stage
    kg_relates_to_delete_max_per_run: int = int(os.getenv("KG_RELATES_TO_DELETE_MAX_PER_RUN", "400"))
    kg_relates_to_delete_max_share: float = float(os.getenv("KG_RELATES_TO_DELETE_MAX_SHARE", "0.12"))

    cloud_key: str = field(default_factory=lambda: os.getenv("CLOUD_LLM_ACCESS_KEY", "").strip())
    cloud_messages_url: str = "https://foundation-models.api.cloud.ru/v1/messages"
    cloud_chat_url: str = "https://foundation-models.api.cloud.ru/v1/chat/completions"
    relevance_model: str = field(default_factory=lambda: os.getenv("ARTICLE_RELEVANCE_MODEL", "anthropic/claude-sonnet-4.6"))
    review_model: str = field(default_factory=lambda: os.getenv("ARTICLE_REVIEW_MODEL", "anthropic/claude-sonnet-4.6"))

    parser_run_url: str = field(default_factory=lambda: os.getenv("PARSER_SERVICE_URL", "http://catchpm-browser:9333/api/parser/run"))
    parser_pdf_url: str = field(default_factory=lambda: os.getenv("PARSER_PDF_URL", "http://catchpm-browser:9333/api/pdf/fulltext"))
    parser_timeout: int = int(os.getenv("PARSER_SERVICE_TIMEOUT_SECONDS", "180"))
    parser_sources_concurrency: int = int(os.getenv("PARSER_SOURCES_CONCURRENCY", "3"))
    parser_api_key: str = field(default_factory=lambda: os.getenv("PARSER_SERVICE_API_KEY", "").strip())


class PMAnalyzePipeline:
    def __init__(self, pool, settings: Settings):
        self.pool = pool
        self.settings = settings
        self._task = None
        self._stop = asyncio.Event()
        self._kg_processed_total = 0

        self._parser_state_lock = asyncio.Lock()
        self._parser_running = False
        self._parser_progress_lock = asyncio.Lock()
        self._parser_progress = {
            "running": False,
            "total_sources": 0,
            "done_sources": 0,
            "current_source": "",
            "active_sources": [],
            "found_raw_total": 0,
            "found_final_total": 0,
            "per_source_raw": {},
            "per_source_final": {},
            "dropped_old_by_source": {},
            "errors": {},
            "started_at": None,
            "updated_at": None,
        }

        self._reviews_state_lock = asyncio.Lock()
        self._reviews_running = False
        self._reviews_progress_lock = asyncio.Lock()
        self._reviews_progress = {
            "running": False,
            "mode": "only",
            "total": 0,
            "done": 0,
            "errors": 0,
            "started_at": None,
            "updated_at": None,
        }

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

    @staticmethod
    def _normalize_title_key(v: str) -> str:
        s = (v or "").strip().lower()
        if not s:
            return ""
        s = s.replace(" ", " ")
        s = re.sub(r"\s+", " ", s)
        s = s.strip(" .,:;!?\"'`“”‘’()[]{}")
        return s

    @staticmethod
    def _canonical_source(source: str) -> str:
        s = (source or "").strip().lower()
        if s in {"arxiv", "arxiv_api", "arxiv_html", "https://arxiv.org"}:
            return "arxiv"
        return s

    @staticmethod
    def _canonical_external_id(source: str, external_id: str) -> str:
        ext = (external_id or "").strip().lower()
        if PMAnalyzePipeline._canonical_source(source) == "arxiv":
            ext = re.sub(r"^arxiv_", "", ext)
            ext = re.sub(r"v[0-9]+$", "", ext)
        return ext

    @staticmethod
    def _is_valid_http_url(v: str) -> bool:
        s = (v or "").strip()
        if not s or s in {"-1", "null", "none"}:
            return False
        try:
            p = urlparse(s)
            if p.scheme not in {"http", "https"}:
                return False
            if not p.netloc:
                return False
            return True
        except Exception:
            return False

    @classmethod
    def _pick_preferred_source_url(cls, url_article: str, pdf_url: str) -> str:
        """Берём валидный URL из двух полей papers_metadata: url_article/pdf_url.
        Предпочтение PDF при его валидности, иначе страница статьи.
        """
        ua = (url_article or "").strip()
        pu = (pdf_url or "").strip()
        if cls._is_valid_http_url(pu):
            return pu
        if cls._is_valid_http_url(ua):
            return ua
        return ""

    @classmethod
    def _first_valid_http_url(cls, *urls: str) -> str:
        for u in urls:
            v = (u or "").strip()
            if cls._is_valid_http_url(v):
                return v
        return ""

    @staticmethod
    def _normalize_knowledge_text_for_exact_merge(text: str) -> str:
        s = (text or "").strip().lower()
        if not s:
            return ""
        s = s.replace("\u00a0", " ")
        s = re.sub(r"https?://\S+", " ", s)
        # убираем markdown/служебные символы, но оставляем буквы/цифры
        s = re.sub(r"[`*_#>\[\](){}\"'“”‘’]", " ", s)
        s = re.sub(r"[^\w\sа-яё-]", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"\s+", " ", s).strip(" .,:;!?")
        return s

    @staticmethod
    def _is_generic_definition_text(text: str) -> bool:
        s = (text or "").strip().lower()
        if len(s) < 40:
            return False
        patterns = [
            r"\bопределяется как\b",
            r"\bэто подход\b",
            r"\bэто метод\b",
            r"\bэто дисциплина\b",
            r"\bзанимается\b",
            r"\bis defined as\b",
            r"\bis a\b",
            r"\bis an\b",
            r"\bdiscipline\b",
            r"\bsubfield\b",
        ]
        return any(re.search(p, s) for p in patterns)

    async def _allow_relation_by_density(self, target_id: int, relation_type: str, relevance_score: float) -> bool:
        # Базовые пороги
        if relevance_score < float(self.settings.kg_relation_min_score or 0.75):
            return False
        if relation_type == "relates_to" and relevance_score < float(self.settings.kg_relates_to_min_score or 0.82):
            return False

        # Доп. защита для generic-definition целей
        async with self.pool.acquire() as con:
            ttext = await con.fetchval(
                "SELECT text_knowledge FROM process_mining.knowledge WHERE id=$1 AND COALESCE(status,'active') <> 'merged'",
                int(target_id),
            )
            if not ttext:
                return True
            if not self._is_generic_definition_text(str(ttext)):
                return True

            if relevance_score < float(self.settings.kg_generic_target_min_score or 0.88):
                return False

            indeg = await con.fetchval(
                "SELECT count(*) FROM process_mining.entity_relations WHERE target_id=$1",
                int(target_id),
            )
            cap = max(5, int(self.settings.kg_generic_in_degree_cap or 28))
            if int(indeg or 0) >= cap:
                return False
        return True

    async def _prune_generic_definition_inbound(self) -> int:
        cap = max(5, int(self.settings.kg_generic_in_degree_cap or 28))
        async with self.pool.acquire() as con:
            deleted = await con.fetchval(
                """
                WITH generic AS (
                    SELECT id
                    FROM process_mining.knowledge
                    WHERE COALESCE(status,'active') <> 'merged'
                      AND lower(COALESCE(text_knowledge,'')) ~
                          '(определяется как|это подход|это метод|это дисциплина|занимается|is defined as|is a |is an |discipline|subfield)'
                ),
                ranked AS (
                    SELECT
                        e.id,
                        ROW_NUMBER() OVER (
                            PARTITION BY e.target_id
                            ORDER BY e.relevance_score DESC NULLS LAST, e.importance DESC NULLS LAST, e.id ASC
                        ) AS rn
                    FROM process_mining.entity_relations e
                    JOIN generic g ON g.id = e.target_id
                ),
                del AS (
                    DELETE FROM process_mining.entity_relations e
                    USING ranked r
                    WHERE e.id = r.id
                      AND r.rn > $1
                    RETURNING 1
                )
                SELECT COUNT(*) FROM del
                """,
                cap,
            )
        return int(deleted or 0)

    async def _check_relates_to_deletion_guard(self, deleted_relates_to: int) -> dict:
        """Guard against excessive relates_to pruning in one run.

        Returns:
          {
            "ok": bool,
            "deleted": int,
            "max_abs": int,
            "max_share": float,
            "baseline_relates_to": int,
            "actual_share": float,
            "reason": str,
          }
        """
        deleted = max(0, int(deleted_relates_to or 0))
        max_abs = max(1, int(self.settings.kg_relates_to_delete_max_per_run or 400))
        max_share = float(self.settings.kg_relates_to_delete_max_share or 0.12)

        async with self.pool.acquire() as con:
            baseline = await con.fetchval(
                """
                SELECT count(*)
                FROM process_mining.entity_relations
                WHERE lower(COALESCE(relation_type,'')) IN ('relates_to','related_to','relates','associated_with')
                """
            )

        baseline = int(baseline or 0)
        total_before = baseline + deleted
        actual_share = (deleted / total_before) if total_before > 0 else 0.0

        over_abs = deleted > max_abs
        over_share = actual_share > max_share
        ok = not (over_abs or over_share)

        reason_parts = []
        if over_abs:
            reason_parts.append(f"abs_limit_exceeded: {deleted} > {max_abs}")
        if over_share:
            reason_parts.append(f"share_limit_exceeded: {actual_share:.4f} > {max_share:.4f}")

        return {
            "ok": ok,
            "deleted": deleted,
            "max_abs": max_abs,
            "max_share": max_share,
            "baseline_relates_to": baseline,
            "actual_share": actual_share,
            "reason": "; ".join(reason_parts) if reason_parts else "ok",
        }

    async def _collapse_knowledge_nodes(self) -> dict:
        """Схлопывание дубликатов knowledge:
        1) exact-схлопывание по нормализованному тексту
        2) semantic-схлопывание по cosine similarity embeddings

        Канонический узел = минимальный id в группе.
        Перекидываем рёбра и source_ids wiki на канон, дубль помечаем status='merged',
        metadata_knowledge += merged_into/merged_reason.
        """
        exact_groups = 0
        exact_nodes_merged = 0
        sem_groups = 0
        sem_nodes_merged = 0

        # ---- 1) exact groups ----
        async with self.pool.acquire() as con:
            rows = await con.fetch(
                """
                SELECT id, text_knowledge
                FROM process_mining.knowledge
                WHERE COALESCE(status,'active') <> 'merged'
                ORDER BY id ASC
                """
            )

        by_norm: dict[str, list[int]] = {}
        min_len = max(40, int(self.settings.kg_exact_min_norm_len or 120))
        for r in rows:
            kid = int(r["id"])
            norm = self._normalize_knowledge_text_for_exact_merge(str(r["text_knowledge"] or ""))
            if len(norm) < min_len:
                continue
            by_norm.setdefault(norm, []).append(kid)

        for _, ids in by_norm.items():
            if len(ids) < 2:
                continue
            ids = sorted(set(int(x) for x in ids))
            canonical_id = ids[0]
            dup_ids = ids[1:]
            changed = await self._merge_knowledge_duplicates(canonical_id, dup_ids, reason="exact_text")
            if changed:
                exact_groups += 1
                exact_nodes_merged += len(dup_ids)

        # ---- 2) semantic pairs ----
        th = float(self.settings.kg_semantic_threshold or 0.93)
        pair_limit = max(20, min(400, int(self.settings.kg_semantic_pair_limit or 120)))

        async with self.pool.acquire() as con:
            pairs = await con.fetch(
                """
                WITH cands AS (
                  SELECT
                    k1.id AS id1,
                    k2.id AS id2,
                    1 - (ke1.embedding <=> ke2.embedding) AS sim
                  FROM process_mining.knowledge_embeddings ke1
                  JOIN process_mining.knowledge_embeddings ke2 ON ke1.knowledge_id < ke2.knowledge_id
                  JOIN process_mining.knowledge k1 ON k1.id = ke1.knowledge_id
                  JOIN process_mining.knowledge k2 ON k2.id = ke2.knowledge_id
                  WHERE COALESCE(k1.status,'active') <> 'merged'
                    AND COALESCE(k2.status,'active') <> 'merged'
                )
                SELECT id1, id2, sim
                FROM cands
                WHERE sim >= $1
                ORDER BY sim DESC
                LIMIT $2
                """,
                th,
                pair_limit,
            )

        # union-find по парам
        parent: dict[int, int] = {}

        def find(x: int) -> int:
            parent.setdefault(x, x)
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(a: int, b: int):
            ra, rb = find(a), find(b)
            if ra == rb:
                return
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

        for p in pairs:
            try:
                a = int(p["id1"])
                b = int(p["id2"])
            except Exception:
                continue
            union(a, b)

        groups: dict[int, list[int]] = {}
        for x in list(parent.keys()):
            groups.setdefault(find(x), []).append(x)

        for _, ids in groups.items():
            uniq = sorted(set(int(i) for i in ids))
            if len(uniq) < 2:
                continue
            canonical_id = uniq[0]
            dup_ids = uniq[1:]
            changed = await self._merge_knowledge_duplicates(canonical_id, dup_ids, reason=f"semantic_sim>={th:.2f}")
            if changed:
                sem_groups += 1
                sem_nodes_merged += len(dup_ids)

        # ---- 3) prune inbound to generic-definition targets ----
        generic_pruned = await self._prune_generic_definition_inbound()

        # ---- 4) checker: контролируем объём удаления generic-связей relates_to ----
        relates_to_guard = await self._check_relates_to_deletion_guard(generic_pruned)
        if not relates_to_guard.get("ok", True):
            print(
                "[pmanalyze] WARNING relates_to prune guard triggered: "
                f"deleted={relates_to_guard.get('deleted')} "
                f"baseline={relates_to_guard.get('baseline_relates_to')} "
                f"share={relates_to_guard.get('actual_share')} "
                f"reason={relates_to_guard.get('reason')}"
            )

        return {
            "ok": True,
            "exact_groups": exact_groups,
            "exact_nodes_merged": exact_nodes_merged,
            "semantic_groups": sem_groups,
            "semantic_nodes_merged": sem_nodes_merged,
            "semantic_threshold": th,
            "pair_limit": pair_limit,
            "generic_pruned_edges": generic_pruned,
            "relates_to_guard": relates_to_guard,
        }

    async def _merge_knowledge_duplicates(self, canonical_id: int, dup_ids: list[int], reason: str) -> bool:
        canonical_id = int(canonical_id)
        dup_ids = sorted(set(int(x) for x in dup_ids if int(x) != canonical_id))
        if not dup_ids:
            return False

        async with self.pool.acquire() as con:
            # убеждаемся, что канон жив
            alive = await con.fetchval(
                "SELECT 1 FROM process_mining.knowledge WHERE id=$1 AND COALESCE(status,'active') <> 'merged'",
                canonical_id,
            )
            if not alive:
                return False

            # 1) source_id -> canonical
            await con.execute(
                """
                UPDATE process_mining.entity_relations
                SET source_id = $1
                WHERE source_id = ANY($2::int[])
                """,
                canonical_id,
                dup_ids,
            )

            # 2) target_id -> canonical
            await con.execute(
                """
                UPDATE process_mining.entity_relations
                SET target_id = $1
                WHERE target_id = ANY($2::int[])
                """,
                canonical_id,
                dup_ids,
            )

            # 3) удаляем самопетли после редиректа
            await con.execute(
                """
                DELETE FROM process_mining.entity_relations
                WHERE source_id = target_id
                """
            )

            # 4) дедуп рёбер по (source,target,type), оставляем max(score/importance)
            await con.execute(
                """
                WITH ranked AS (
                    SELECT id,
                           ROW_NUMBER() OVER (
                             PARTITION BY source_id, target_id, relation_type
                             ORDER BY relevance_score DESC NULLS LAST, importance DESC NULLS LAST, id ASC
                           ) AS rn
                    FROM process_mining.entity_relations
                )
                DELETE FROM process_mining.entity_relations e
                USING ranked r
                WHERE e.id = r.id AND r.rn > 1
                """
            )

            # 5) wiki source_ids: заменяем dup -> canonical, уникализируем
            wiki_rows = await con.fetch(
                """
                SELECT id, source_ids
                FROM process_mining.wiki_pages
                WHERE source_ids IS NOT NULL
                """
            )
            dup_set = set(dup_ids)
            for w in wiki_rows:
                wid = int(w["id"])
                src = w["source_ids"]
                if not isinstance(src, list):
                    continue
                changed_local = False
                new_ids = []
                for v in src:
                    try:
                        iv = int(v)
                    except Exception:
                        continue
                    if iv in dup_set:
                        iv = canonical_id
                        changed_local = True
                    new_ids.append(iv)
                if not changed_local:
                    continue
                # unique preserve order
                seen = set()
                uniq = []
                for iv in new_ids:
                    if iv in seen:
                        continue
                    seen.add(iv)
                    uniq.append(iv)
                await con.execute(
                    "UPDATE process_mining.wiki_pages SET source_ids=$2::jsonb, updated_at=now() WHERE id=$1",
                    wid,
                    json.dumps(uniq),
                )

            # 6) помечаем дубль как merged
            for did in dup_ids:
                await con.execute(
                    """
                    UPDATE process_mining.knowledge
                    SET status='merged',
                        metadata_knowledge = COALESCE(metadata_knowledge,'{}'::jsonb)
                            || jsonb_build_object('merged_into',$1,'merged_reason',$2,'merged_at',now()::text),
                        updated_at = now()
                    WHERE id=$3
                    """,
                    canonical_id,
                    reason,
                    did,
                )

            # 7) если у дубликатов были embeddings — чистим их (на каноне уже есть свой)
            await con.execute(
                "DELETE FROM process_mining.knowledge_embeddings WHERE knowledge_id = ANY($1::int[])",
                dup_ids,
            )

        return True

    async def _maybe_run_periodic_collapse(self) -> dict | None:
        every = max(0, int(self.settings.kg_collapse_every_articles or 0))
        if every <= 0:
            return None
        if self._kg_processed_total <= 0:
            return None
        if self._kg_processed_total % every != 0:
            return None
        try:
            res = await self._collapse_knowledge_nodes()
            print(f"[pmanalyze] collapse run after processed={self._kg_processed_total}: {res}")
            return res
        except Exception as e:
            print(f"[pmanalyze] collapse error after processed={self._kg_processed_total}: {type(e).__name__}: {e}")
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

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
    async def run_once(self, parser_sources: list[str] | None = None) -> dict:
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
            fetched = await self._collect(parser_sources=parser_sources)
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

    async def run_once_guarded(self, parser_sources: list[str] | None = None) -> dict:
        # Защита от повторного старта парсинга в рамках одного инстанса
        if not await self._try_claim_parser_run():
            print("[pmanalyze] run_once already running in this instance, skip")
            return {"ok": False, "skipped": "already_running"}
        try:
            return await self.run_once(parser_sources=parser_sources)
        finally:
            await self._release_parser_run()

    # ---------------- collect from parser service ----------------
    async def _collect(self, parser_sources: list[str] | None = None) -> list[dict]:
        headers = {"Content-Type": "application/json"}
        if self.settings.parser_api_key:
            headers["X-Api-Key"] = self.settings.parser_api_key

        default_sources = ["arxiv_api","arxiv_html","crossref","core_api","fluxicon","google_scholar"]
        source_list = [str(s).strip().lower() for s in (parser_sources or default_sources) if str(s).strip()]
        if not source_list:
            source_list = default_sources

        await self._parser_progress_start(source_list)
        try:
            max_c = int(self.settings.parser_sources_concurrency or 1)
            max_c = max(1, min(max_c, len(source_list)))
            sem = asyncio.Semaphore(max_c)

            async with httpx.AsyncClient(timeout=self.settings.parser_timeout) as client:
                async def _run_source(src: str) -> list[dict]:
                    async with sem:
                        await self._parser_progress_source_start(src)
                        payload = {
                            "max_per_source": int(self.settings.max_per_source),
                            "ui_lang": "en",
                            "include_sources": [src],
                        }
                        try:
                            r = await client.post(self.settings.parser_run_url, headers=headers, json=payload)
                            r.raise_for_status()
                            data = r.json()
                            if not data.get("ok"):
                                err = data.get("error") or data.get("errors") or "unknown"
                                await self._parser_progress_source_done(src, 0, 0, 0, str(err))
                                print(f"[pmanalyze] collect parser_not_ok source={src} url={self.settings.parser_run_url} error={err}")
                                return []

                            arts = data.get("articles", []) or []
                            src_raw = (data.get("sources_raw") or data.get("sources") or {})
                            src_fin = (data.get("sources") or {})
                            src_drop = (data.get("dropped_old_by_source") or {})
                            raw_cnt = int(src_raw.get(src, len(arts)) or 0)
                            fin_cnt = int(src_fin.get(src, len(arts)) or 0)
                            drop_cnt = int(src_drop.get(src, 0) or 0)
                            errs = data.get("errors") or {}
                            src_err = errs.get(src, "") if isinstance(errs, dict) else ""

                            await self._parser_progress_source_done(src, raw_cnt, fin_cnt, drop_cnt, str(src_err or ""))
                            print(f"[pmanalyze] collect source={src} raw={raw_cnt} final={fin_cnt} dropped_old={drop_cnt}")
                            return arts

                        except httpx.HTTPStatusError as e:
                            status = e.response.status_code if e.response is not None else "n/a"
                            req_url = str(e.request.url) if e.request is not None else self.settings.parser_run_url
                            body = ""
                            try:
                                if e.response is not None:
                                    body = (e.response.text or "")[:700]
                            except Exception:
                                body = "<response_text_unavailable>"
                            await self._parser_progress_source_done(src, 0, 0, 0, f"http_{status}")
                            print(
                                f"[pmanalyze] collect http_error type={type(e).__name__} status={status} "
                                f"url={req_url} source={src} payload={payload} body={body}"
                            )
                            return []
                        except Exception as e:
                            await self._parser_progress_source_done(src, 0, 0, 0, repr(e))
                            print(
                                f"[pmanalyze] collect error type={type(e).__name__} repr={repr(e)} "
                                f"url={self.settings.parser_run_url} source={src} payload={payload}"
                            )
                            return []

                tasks = [asyncio.create_task(_run_source(src)) for src in source_list]
                results = await asyncio.gather(*tasks)
                out: list[dict] = []
                for arts in results:
                    if arts:
                        out.extend(arts)
                return out
        finally:
            await self._parser_progress_finish()

    async def _upsert_articles(self, articles: list[dict]) -> list[int]:
        """Upsert новых статей + фильтрация дублей по title и canonical (source, external_id)."""
        new_ids = []
        async with self.pool.acquire() as con:
            existing_titles_rows = await con.fetch(
                """
                SELECT lower(trim(regexp_replace(coalesce(title,''), '\s+', ' ', 'g'))) AS norm_title
                FROM process_mining.papers_metadata
                WHERE coalesce(title,'') <> ''
                """
            )
            existing_title_keys = {r['norm_title'] for r in existing_titles_rows if r.get('norm_title')}
            batch_title_keys = set()
            batch_canon_keys = set()

            for a in articles:
                ext = (a.get("external_id") or "").strip()
                src = (a.get("source") or "").strip()
                if not ext or not src:
                    continue

                canon_src = self._canonical_source(src)
                canon_ext = self._canonical_external_id(src, ext)
                canon_key = (canon_src, canon_ext)
                if not canon_src or not canon_ext:
                    continue
                if canon_key in batch_canon_keys:
                    continue

                exists_canon = await con.fetchval(
                    """
                    SELECT 1
                    FROM process_mining.papers_metadata p
                    WHERE (
                        CASE
                            WHEN lower(COALESCE(p.source,'')) = ANY (ARRAY['arxiv','arxiv_api','arxiv_html','https://arxiv.org']) THEN 'arxiv'
                            ELSE lower(TRIM(BOTH FROM COALESCE(p.source,'')))
                        END
                    ) = $1
                    AND (
                        CASE
                            WHEN lower(COALESCE(p.source,'')) = ANY (ARRAY['arxiv','arxiv_api','arxiv_html','https://arxiv.org'])
                                THEN regexp_replace(regexp_replace(lower(TRIM(BOTH FROM COALESCE(p.external_id,''))), '^arxiv_', ''), 'v[0-9]+$', '')
                            ELSE lower(TRIM(BOTH FROM COALESCE(p.external_id,'')))
                        END
                    ) = $2
                    LIMIT 1
                    """,
                    canon_src, canon_ext,
                )
                if exists_canon:
                    continue

                title = (a.get("title") or "").strip()
                title_key = self._normalize_title_key(title)
                if title_key and (title_key in existing_title_keys or title_key in batch_title_keys):
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
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    ext, src, title, _to_date(a.get("date_sub")),
                    a.get("url_article") or a.get("pdf_url"), a.get("pdf_url"),
                    json.dumps(authors) if authors is not None else None,
                    tags if tags is not None else None,
                    a.get("abstract", ""),
                )
                batch_canon_keys.add(canon_key)
                if row:
                    new_ids.append(int(row["id"]))
                    if title_key:
                        existing_title_keys.add(title_key)
                        batch_title_keys.add(title_key)
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

    async def _fetch_pdf_text_via_parser(self, urls: list[str], max_pages: int = 6, max_chars: int = 24000) -> tuple[str, str, str]:
        """Пробует вытащить текст PDF через catchpm-browser /api/pdf/fulltext.
        Возвращает: (text, used_url, method)
        """
        clean = []
        for u in (urls or []):
            u = (u or "").strip()
            if u and u.startswith("http") and u not in clean:
                clean.append(u)
        if not clean:
            return "", "", ""

        headers = {"Content-Type": "application/json"}
        if self.settings.parser_api_key:
            headers["X-Api-Key"] = self.settings.parser_api_key

        payload = {
            "urls": clean[:8],
            "max_pages": max(1, min(int(max_pages), 12)),
            "max_chars": max(1000, min(int(max_chars), 60000)),
            "max_urls": min(len(clean), 8),
        }
        try:
            async with httpx.AsyncClient(timeout=75.0) as client:
                r = await client.post(self.settings.parser_pdf_url, headers=headers, json=payload)
                r.raise_for_status()
                data = r.json() or {}
            for row in (data.get("results") or []):
                txt = (row.get("text") or "").strip()
                if row.get("ok") and txt:
                    used_url = (row.get("used_url") or row.get("norm_url") or row.get("url") or "").strip()
                    method = (row.get("method") or "").strip()
                    return txt[:24000], used_url, method
        except Exception as e:
            print(f"[pmanalyze] parser pdf/fulltext error: {e}")
        return "", "", ""

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

    def _extract_ieee_arnumber(self, *urls: str) -> str:
        """Достаёт arnumber из ieee-URL (query или /document/<id>)."""
        for u in urls:
            if not u:
                continue
            try:
                q = urlparse(u).query or ""
                m = re.search(r"(?:^|&)arnumber=(\d+)(?:&|$)", q)
                if m:
                    return m.group(1)
                m = re.search(r"/document/(\d+)", u)
                if m:
                    return m.group(1)
                m = re.search(r"/(\d+)\.pdf", u)
                if m:
                    return m.group(1)
            except Exception:
                pass
        return ""

    def _ieee_candidate_urls(self, url_article: str, pdf_url: str) -> list[str]:
        """Кандидаты для IEEE: сначала страница статьи, затем stamp-URL и исходный pdf."""
        out: list[str] = []
        for u in [url_article, pdf_url]:
            if u and u not in out:
                out.append(u)
        ar = self._extract_ieee_arnumber(url_article, pdf_url)
        if ar:
            for u in [
                f"https://ieeexplore.ieee.org/document/{ar}",
                f"https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber={ar}",
                f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={ar}",
            ]:
                if u not in out:
                    out.append(u)
        return out

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
        # Ловим только явные отказы/запросы прислать данные, без слишком общих фраз.
        bad = [
            "текст статьи не был передан",
            "не был передан текст статьи",
            "вставьте текст статьи",
            "пришлите текст статьи",
            "предоставьте текст статьи",
            "чтобы выполнить задачу, мне нужен",
            "не могу выполнить задачу без",
            "недостаточно данных для выполнения",
            "список файлов",
            "содержимое яндекс диска",
        ]
        if any(x in t for x in bad):
            return True
        # Мягкая эвристика: одновременно есть маркер отказа + просьба прислать контент.
        refusal_markers = ["не могу", "невозможно", "не получится", "недостаточно данных"]
        request_markers = ["пришлите", "предоставьте", "вставьте", "нужен текст"]
        return any(rm in t for rm in refusal_markers) and any(qm in t for qm in request_markers)

    def _looks_like_unavailable_review_template(self, text: str) -> bool:
        """Ловит шаблон-заглушку про недоступный текст/403, который нельзя сохранять как ревью."""
        if not text:
            return False
        t = text.lower()
        markers = [
            "текст статьи недоступен",
            "403 forbidden",
            "ошибка 403",
            "извлечение информации невозможно",
            "название оригинальной статьи на английском*: отсутствует",
        ]
        hits = sum(1 for m in markers if m in t)
        return hits >= 2

    async def generate_review(self, art: dict) -> tuple[str, str | None]:
        # Для Yandex: сначала бинарный PDF -> затем page fetch fallback.
        # Для прочих источников: page fetch -> pdf fallback.
        url_article = (art.get("url_article") or "").strip()
        pdf_url = (art.get("pdf_url") or "").strip()
        source = (art.get("source") or "").strip().lower()

        # Ссылка в промпте должна указывать на реальный PDF-документ.
        source_url = self._pick_preferred_source_url(url_article, pdf_url)
        if not source_url:
            source_url = self._first_valid_http_url(pdf_url, url_article)
        original_source_url = source_url

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
            # Важно: для IEEE прямой PDF часто даёт 418. Сначала пробуем страницу статьи.
            candidate_urls = []
            if "ieeexplore.ieee.org" in source:
                candidate_urls = self._ieee_candidate_urls(url_article, pdf_url)
            else:
                # Общий порядок: приоритет PDF (и валидных http), затем страница статьи.
                for u in [pdf_url, source_url, url_article]:
                    if self._is_valid_http_url(u) and u not in candidate_urls:
                        candidate_urls.append(u)

            for u in candidate_urls:
                page_text = await self._fetch_page_text(u)
                if page_text and not self._looks_like_yandex_listing(page_text):
                    source_url = u
                    break
                page_text = ""

            if not page_text:
                # Последний fallback — PDF fulltext через parser-сервис (лучше переживает anti-bot).
                if "ieeexplore.ieee.org" in source:
                    ar = self._extract_ieee_arnumber(url_article, pdf_url)
                    pdf_candidates = []
                    if ar:
                        pdf_candidates.extend([
                            f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={ar}",
                            f"https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber={ar}",
                        ])
                    if pdf_url:
                        pdf_candidates.append(pdf_url)
                    page_text, parser_used_url, parser_method = await self._fetch_pdf_text_via_parser(pdf_candidates)
                    if page_text and parser_used_url:
                        source_url = parser_used_url
                    if page_text and source_url != original_source_url:
                        print(f"[pmanalyze] source fallback success article_id={art.get('id')} from={original_source_url} to={source_url} via={parser_method or 'parser_pdf_fulltext'}")
                    if not page_text and ar:
                        page_text = await self._download_pdf_text(f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={ar}")
                        if page_text:
                            source_url = f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={ar}"
                else:
                    # Для остальных источников тоже пробуем parser PDF endpoint перед прямой загрузкой
                    page_text, parser_used_url, parser_method = await self._fetch_pdf_text_via_parser([pdf_url, source_url])
                    if page_text and parser_used_url:
                        source_url = parser_used_url
                    if page_text and source_url != original_source_url:
                        print(f"[pmanalyze] source fallback success article_id={art.get('id')} from={original_source_url} to={source_url} via={parser_method or 'parser_pdf_fulltext'}")

                if not page_text:
                    page_text = await self._download_pdf_text(pdf_url)

        if not page_text:
            print(f"[pmanalyze] empty content for article_id={art.get('id')} source={art.get('source')} ext={art.get('external_id')} -> fallback_to_abstract")
            # Fallback: если полный текст недоступен, генерим НЕ шаблонное ревью,
            # а перевод аннотации (по запросу PM).
            abstract_text = (art.get("abstract") or "").strip()
            if not abstract_text:
                return "", "content_unavailable_or_invalid_source_url"
            filename = f"{art.get('source','src')}_{art.get('external_id','id')}"
            authors_hint = self._normalize_authors(art.get("authors"))
            ann_prompt = build_annotation_translation_prompt(
                title=(art.get("title") or "").strip(),
                source_url=(source_url or pdf_url or url_article or "").strip(),
                abstract_text=abstract_text,
                authors_hint=authors_hint,
            )
            ann_review = await self._llm(self.settings.review_model, ann_prompt, max_tokens=1200)
            if not (ann_review or "").strip():
                return "", "annotation_translation_failed"
            return ann_review, None

        # Если пробились через альтернативный URL — исходный невалидный URL помечаем как -1
        if source_url and original_source_url and source_url != original_source_url:
            await self._mark_invalid_source_url(int(art.get("id")), original_source_url)

        filename = f"{art.get('source','src')}_{art.get('external_id','id')}"
        authors_hint = self._normalize_authors(art.get("authors"))
        prompt = build_review_prompt(filename, source_url, page_text, authors_hint=authors_hint)

        # 1-я попытка
        review = await self._llm(self.settings.review_model, prompt, max_tokens=2500)
        if self._looks_like_refusal(review):
            print(f"[pmanalyze] refusal-like review for article_id={art.get('id')} source={art.get('source')} -> retry")
            # 2-я попытка: более короткий и строгий промпт-антиотказ
            retry_prompt = (
                prompt
                + "\n\nВАЖНО: входной текст статьи уже передан выше. "
                  "Не проси прислать текст, не описывай ограничения. "
                  "Сразу выдай итог строго по шаблону."
            )
            review = await self._llm(self.settings.review_model, retry_prompt, max_tokens=2500)
            if self._looks_like_refusal(review):
                print(f"[pmanalyze] refusal-like review persisted for article_id={art.get('id')} source={art.get('source')}")
                return "", "llm_refusal_or_bad_input"

        # Защита от сохранения шаблона-заглушки про 403/недоступный текст.
        if self._looks_like_unavailable_review_template(review):
            print(f"[pmanalyze] unavailable-template detected for article_id={art.get('id')} -> fallback_to_abstract_translation")
            abstract_text = (art.get("abstract") or "").strip()
            if not abstract_text:
                return "", "content_unavailable_or_invalid_source_url"
            ann_prompt = build_annotation_translation_prompt(
                title=(art.get("title") or "").strip(),
                source_url=(source_url or pdf_url or url_article or "").strip(),
                abstract_text=abstract_text,
                authors_hint=authors_hint,
            )
            review = await self._llm(self.settings.review_model, ann_prompt, max_tokens=1200)
            if not (review or "").strip() or self._looks_like_unavailable_review_template(review):
                return "", "annotation_translation_failed"

        if not (review or "").strip():
            return "", "review_generation_failed"

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

    @staticmethod
    def _format_active_sources(active: list[str]) -> str:
        src = [str(s).strip().lower() for s in (active or []) if str(s).strip()]
        if not src:
            return ""
        if len(src) <= 2:
            return ", ".join(src)
        return ", ".join(src[:2]) + f" (+{len(src)-2})"

    async def _parser_progress_start(self, sources: list[str]):
        now = datetime.now(timezone.utc).isoformat()
        src = [str(s).strip().lower() for s in (sources or []) if str(s).strip()]
        async with self._parser_progress_lock:
            self._parser_progress = {
                "running": True,
                "total_sources": len(src),
                "done_sources": 0,
                "current_source": "",
                "active_sources": [],
                "found_raw_total": 0,
                "found_final_total": 0,
                "per_source_raw": {s: 0 for s in src},
                "per_source_final": {s: 0 for s in src},
                "dropped_old_by_source": {s: 0 for s in src},
                "errors": {},
                "started_at": now,
                "updated_at": now,
            }

    async def _parser_progress_source_start(self, source: str):
        now = datetime.now(timezone.utc).isoformat()
        s = (source or "").strip().lower()
        async with self._parser_progress_lock:
            active = list(self._parser_progress.get("active_sources") or [])
            if s and s not in active:
                active.append(s)
            self._parser_progress["active_sources"] = active
            self._parser_progress["current_source"] = self._format_active_sources(active)
            self._parser_progress["updated_at"] = now

    async def _parser_progress_source_done(self, source: str, raw_cnt: int, final_cnt: int, dropped_old: int = 0, err: str = ""):
        now = datetime.now(timezone.utc).isoformat()
        s = (source or "").strip().lower()
        async with self._parser_progress_lock:
            psr = dict(self._parser_progress.get("per_source_raw") or {})
            psf = dict(self._parser_progress.get("per_source_final") or {})
            pso = dict(self._parser_progress.get("dropped_old_by_source") or {})
            errs = dict(self._parser_progress.get("errors") or {})
            active = [x for x in list(self._parser_progress.get("active_sources") or []) if x != s]
            psr[s] = int(raw_cnt or 0)
            psf[s] = int(final_cnt or 0)
            pso[s] = int(dropped_old or 0)
            if err:
                errs[s] = str(err)[:300]
            self._parser_progress["per_source_raw"] = psr
            self._parser_progress["per_source_final"] = psf
            self._parser_progress["dropped_old_by_source"] = pso
            self._parser_progress["errors"] = errs
            self._parser_progress["active_sources"] = active
            self._parser_progress["done_sources"] = int(self._parser_progress.get("done_sources", 0)) + 1
            self._parser_progress["found_raw_total"] = int(self._parser_progress.get("found_raw_total", 0)) + int(raw_cnt or 0)
            self._parser_progress["found_final_total"] = int(self._parser_progress.get("found_final_total", 0)) + int(final_cnt or 0)
            self._parser_progress["current_source"] = self._format_active_sources(active)
            self._parser_progress["updated_at"] = now

    async def _parser_progress_finish(self):
        now = datetime.now(timezone.utc).isoformat()
        async with self._parser_progress_lock:
            # После завершения цикла очищаем прогресс полностью,
            # чтобы после F5 UI показывал "парсер: idle".
            self._parser_progress = {
                "running": False,
                "total_sources": 0,
                "done_sources": 0,
                "current_source": "",
                "active_sources": [],
                "found_raw_total": 0,
                "found_final_total": 0,
                "per_source_raw": {},
                "per_source_final": {},
                "dropped_old_by_source": {},
                "errors": {},
                "started_at": None,
                "updated_at": now,
            }

    async def get_parser_progress(self) -> dict:
        async with self._parser_progress_lock:
            return dict(self._parser_progress)

    async def _try_claim_reviews_run(self) -> bool:
        async with self._reviews_state_lock:
            if self._reviews_running:
                return False
            self._reviews_running = True
            return True

    async def _reviews_progress_start(self, mode: str, total: int):
        now = datetime.now(timezone.utc)
        async with self._reviews_progress_lock:
            self._reviews_progress = {
                "running": True,
                "mode": (mode or "only"),
                "total": int(total),
                "done": 0,
                "errors": 0,
                "started_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }

    async def _reviews_progress_tick(self, done_inc: int = 1, err_inc: int = 0):
        now = datetime.now(timezone.utc).isoformat()
        async with self._reviews_progress_lock:
            self._reviews_progress["done"] = int(self._reviews_progress.get("done", 0)) + int(done_inc)
            self._reviews_progress["errors"] = int(self._reviews_progress.get("errors", 0)) + int(err_inc)
            self._reviews_progress["updated_at"] = now

    async def _reviews_progress_finish(self):
        now = datetime.now(timezone.utc).isoformat()
        async with self._reviews_progress_lock:
            self._reviews_progress["running"] = False
            self._reviews_progress["updated_at"] = now

    async def get_reviews_progress(self) -> dict:
        async with self._reviews_progress_lock:
            return dict(self._reviews_progress)

    async def _release_reviews_run(self):
        async with self._reviews_state_lock:
            self._reviews_running = False

    async def process_pending_prelocked(self, limit: int = 200) -> dict:
        """Выполнить обработку, предполагая что локи уже захвачены вызывающим кодом."""
        try:
            res = await self.process_pending(limit=limit)
            return {"ok": True, **res}
        finally:
            await self._reviews_progress_finish()
            await self._unlock()
            await self._release_reviews_run()

    async def process_reviews_only_prelocked(self, limit: int = 200) -> dict:
        """Сгенерировать ревью только для уже релевантных статей без пересчета релевантности/категорий."""
        try:
            res = await self.process_reviews_only(limit=limit)
            return {"ok": True, **res}
        finally:
            await self._reviews_progress_finish()
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
        await self._reviews_progress_start("full", len(rows))
        relevant = reviewed = irrelevant = errors = 0
        for r in rows:
            aid = int(r["id"])
            art = await self._get_article(aid)
            if not art:
                await self._reviews_progress_tick(done_inc=1, err_inc=1)
                continue
            try:
                await self._set_status(aid, "llm_processing")
                rel = await self.assess_relevance(art)
                await self._save_relevance(aid, rel)
                if not rel["is_relevant"]:
                    irrelevant += 1
                    await self._set_status(aid, "irrelevant")
                    await self._reviews_progress_tick(done_inc=1)
                    continue
                relevant += 1
                review_md, review_err = await self.generate_review(art)
                if not review_md:
                    errors += 1
                    await self._set_status(aid, "review_error", review_err or "review_generation_failed")
                    await self._reviews_progress_tick(done_inc=1, err_inc=1)
                    continue
                if self._looks_like_refusal(review_md) or self._looks_like_unavailable_review_template(review_md):
                    errors += 1
                    await self._set_status(aid, "review_error", "review_contains_unavailable_or_refusal_template")
                    await self._reviews_progress_tick(done_inc=1, err_inc=1)
                    continue
                await self._save_review(aid, review_md)
                reviewed += 1
                await self._set_status(aid, "reviewed")
                await self._reviews_progress_tick(done_inc=1)
            except Exception as e:
                errors += 1
                await self._set_status(aid, "review_error", str(e)[:300])
                await self._reviews_progress_tick(done_inc=1, err_inc=1)
        return {"relevant": relevant, "reviewed": reviewed,
                "irrelevant": irrelevant, "errors": errors, "total": len(rows)}

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
        await self._reviews_progress_start("only", len(rows))
        reviewed = errors = skipped = 0
        for r in rows:
            aid = int(r["id"])
            art = await self._get_article(aid)
            if not art:
                skipped += 1
                await self._reviews_progress_tick(done_inc=1, err_inc=1)
                continue
            try:
                await self._set_status(aid, "llm_processing")
                review_md, review_err = await self.generate_review(art)
                if not review_md:
                    errors += 1
                    await self._set_status(aid, "review_error", review_err or "review_generation_failed")
                    await self._reviews_progress_tick(done_inc=1, err_inc=1)
                    continue
                if self._looks_like_refusal(review_md) or self._looks_like_unavailable_review_template(review_md):
                    errors += 1
                    await self._set_status(aid, "review_error", "review_contains_unavailable_or_refusal_template")
                    await self._reviews_progress_tick(done_inc=1, err_inc=1)
                    continue
                await self._save_review(aid, review_md)
                reviewed += 1
                await self._set_status(aid, "reviewed")
                await self._reviews_progress_tick(done_inc=1)
            except Exception as e:
                errors += 1
                await self._set_status(aid, "review_error", str(e)[:300])
                await self._reviews_progress_tick(done_inc=1, err_inc=1)
        return {"reviewed": reviewed, "errors": errors, "skipped": skipped, "total": len(rows)}

    async def _fetch_article_text_for_kg(self, art: dict) -> tuple[str, str, str | None]:
        """Возвращает (text, source_url, error_code). Для KG берём ~первые 4 страницы."""
        url_article = (art.get("url_article") or "").strip()
        pdf_url = (art.get("pdf_url") or "").strip()
        source = (art.get("source") or "").strip().lower()

        source_url = self._pick_preferred_source_url(url_article, pdf_url)
        if not source_url:
            source_url = self._first_valid_http_url(pdf_url, url_article)
        original_source_url = source_url

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
            candidate_urls = []
            if "ieeexplore.ieee.org" in source:
                candidate_urls = self._ieee_candidate_urls(url_article, pdf_url)
            else:
                for u in [pdf_url, source_url, url_article]:
                    if self._is_valid_http_url(u) and u not in candidate_urls:
                        candidate_urls.append(u)

            for u in candidate_urls:
                page_text = await self._fetch_page_text(u)
                if page_text and not self._looks_like_yandex_listing(page_text):
                    source_url = u
                    break
                page_text = ""

            if not page_text:
                page_text, parser_used_url, _parser_method = await self._fetch_pdf_text_via_parser(
                    [pdf_url, source_url], max_pages=4, max_chars=18000
                )
                if page_text and parser_used_url:
                    source_url = parser_used_url

                if not page_text and "ieeexplore.ieee.org" in source:
                    ar = self._extract_ieee_arnumber(url_article, pdf_url)
                    if ar:
                        page_text = await self._download_pdf_text(
                            f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={ar}"
                        )
                        if page_text:
                            source_url = f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={ar}"
                if not page_text:
                    page_text = await self._download_pdf_text(pdf_url)

        text = (page_text or "").strip()
        if text:
            text = text[:18000]
            if source_url and original_source_url and source_url != original_source_url:
                await self._mark_invalid_source_url(int(art.get("id")), original_source_url)
            return text, source_url, None

        abstract_text = (art.get("abstract") or "").strip()
        if abstract_text:
            fallback_url = self._first_valid_http_url(source_url, pdf_url, url_article)
            return abstract_text[:8000], fallback_url, None

        return "", source_url, "content_unavailable_or_invalid_source_url"

    async def _upsert_knowledge_embedding(self, knowledge_id: int, article_id: int, text_knowledge: str):
        text = (text_knowledge or "").strip()
        if not text:
            return
        try:
            emb = await embed_text(text[:30000])
            vec_lit = "[" + ",".join(f"{x:.7f}" for x in emb) + "]"
            async with self.pool.acquire() as con:
                await con.execute(
                    """
                    INSERT INTO process_mining.knowledge_embeddings
                        (knowledge_id, article_id, content, embedding, tsv, model, created_at, updated_at)
                    VALUES ($1, $2, $3, $4::vector, to_tsvector($6, $3), $5, now(), now())
                    ON CONFLICT (knowledge_id) DO UPDATE SET
                        article_id = EXCLUDED.article_id,
                        content = EXCLUDED.content,
                        embedding = EXCLUDED.embedding,
                        tsv = EXCLUDED.tsv,
                        model = EXCLUDED.model,
                        updated_at = now()
                    """,
                    knowledge_id, article_id, text, vec_lit, EMBED_MODEL, TS_CONFIG,
                )
        except Exception as e:
            print(f"[pmanalyze] knowledge embedding upsert error knowledge_id={knowledge_id}: {type(e).__name__}: {e}")

    async def _upsert_wiki_page_embedding(self, wiki_page_id: int, content_md: str):
        text = (content_md or "").strip()
        if not text:
            return
        try:
            emb = await embed_text(text[:30000])
            vec_lit = "[" + ",".join(f"{x:.7f}" for x in emb) + "]"
            async with self.pool.acquire() as con:
                await con.execute(
                    """
                    INSERT INTO process_mining.wiki_page_embeddings
                        (wiki_page_id, content, embedding, tsv, model, created_at, updated_at)
                    VALUES ($1, $2, $3::vector, to_tsvector($5, $2), $4, now(), now())
                    ON CONFLICT (wiki_page_id) DO UPDATE SET
                        content = EXCLUDED.content,
                        embedding = EXCLUDED.embedding,
                        tsv = EXCLUDED.tsv,
                        model = EXCLUDED.model,
                        updated_at = now()
                    """,
                    wiki_page_id, text, vec_lit, EMBED_MODEL, TS_CONFIG,
                )
        except Exception as e:
            print(f"[pmanalyze] wiki embedding upsert error wiki_page_id={wiki_page_id}: {type(e).__name__}: {e}")

    async def _hybrid_knowledge_candidates(
        self,
        query_text: str,
        article_id: int,
        exclude_ids: set[int] | None = None,
        top_k: int = 6,
        min_vec_sim: float = 0.62,
    ) -> list[dict]:
        """Retrieval кандидатов для cross-article linking.

        ГЕЙТ по vec_sim (семантика), НЕ по гибриду: лексика (ts_rank_cd) на
        англоязычных PM-терминах при russian-конфиге даёт околоноль и, будучи
        в формуле, топила валидные пары под порог. Теперь lex_rank считается
        только для tie-break реранка внутри уже прошедших порог кандидатов.
        """
        text = (query_text or "").strip()
        if len(text) < 20:
            return []

        try:
            emb = await embed_text(text[:30000])
        except Exception as e:
            print(f"[pmanalyze] candidate embed error article_id={article_id}: {type(e).__name__}: {e}")
            return []

        vec_lit = "[" + ",".join(f"{x:.7f}" for x in emb) + "]"
        ex_ids = sorted(int(x) for x in (exclude_ids or set()) if int(x) > 0)
        if not ex_ids:
            ex_ids = [0]

        q = text[:1200]
        try:
            async with self.pool.acquire() as con:
                rows = await con.fetch(
                    """
                    SELECT
                        ke.knowledge_id AS id,
                        ke.content AS text_knowledge,
                        GREATEST(0.0, 1 - (ke.embedding <=> $1::vector)) AS vec_sim,
                        ts_rank_cd(ke.tsv, plainto_tsquery($2, $3)) AS lex_rank
                    FROM process_mining.knowledge_embeddings ke
                    JOIN process_mining.knowledge k ON k.id = ke.knowledge_id
                    WHERE ke.knowledge_id <> ALL($4::bigint[])
                      AND COALESCE(k.metadata_knowledge ->> 'article_id', '') <> $5
                      AND (1 - (ke.embedding <=> $1::vector)) >= $6
                    ORDER BY
                        (1 - (ke.embedding <=> $1::vector)) DESC,
                        ts_rank_cd(ke.tsv, plainto_tsquery($2, $3)) DESC
                    LIMIT $7
                    """,
                    vec_lit,
                    TS_CONFIG,
                    q,
                    ex_ids,
                    str(article_id),
                    float(min_vec_sim),
                    max(1, int(top_k)),
                )
        except Exception as e:
            print(f"[pmanalyze] hybrid candidate query error article_id={article_id}: {type(e).__name__}: {e}")
            return []

        out = []
        for r in rows:
            try:
                vsim = float(r["vec_sim"] or 0.0)
            except Exception:
                vsim = 0.0
            if vsim < float(min_vec_sim):
                continue
            out.append({
                "id": int(r["id"]),
                "text_knowledge": str(r["text_knowledge"] or "").strip(),
                "vec_sim": vsim,
                "lex_rank": float(r["lex_rank"] or 0.0),
            })
        return out

    async def process_knowledge_graph(self, limit: int = 30, stop_event: asyncio.Event | None = None) -> dict:
        """KG executor: релевантные статьи -> атомы -> связи -> wiki -> embeddings."""
        if not await self._try_lock():
            print("[pmanalyze] process_knowledge_graph lock busy, skip")
            return {"ok": False, "skipped": "locked"}

        processed = created_nodes = created_edges = created_pages = errors = 0
        cross_candidates = cross_pairs_evaluated = cross_edges = 0
        stop_requested = False

        def _stop_now() -> bool:
            try:
                return bool(stop_event and stop_event.is_set())
            except Exception:
                return False
        try:
            async with self.pool.acquire() as con:
                rows = await con.fetch(
                    """
                    SELECT p.id
                    FROM process_mining.papers_metadata p
                    WHERE p.is_relevant = TRUE
                      AND COALESCE(p.kg_processed, FALSE) = FALSE
                    ORDER BY p.created_at ASC, p.id ASC
                    LIMIT $1
                    """,
                    max(1, int(limit)),
                )

            allowed_rel = set(RELATION_TYPES)

            for r in rows:
                if _stop_now():
                    stop_requested = True
                    break

                aid = int(r["id"])
                art = await self._get_article(aid)
                if not art:
                    errors += 1
                    continue
                try:
                    source_text, source_url, err = await self._fetch_article_text_for_kg(art)
                    if not source_text:
                        errors += 1
                        if err:
                            await self._set_status(aid, "review_error", err)
                        continue

                    if _stop_now():
                        stop_requested = True
                        break

                    source_meta = json.dumps({
                        "article_id": aid,
                        "title": art.get("title") or "",
                        "source": art.get("source") or "",
                        "source_url": source_url or art.get("url_article") or art.get("pdf_url") or "",
                        "authors": self._normalize_authors(art.get("authors")),
                        "date_sub": str(art.get("date_sub") or ""),
                    }, ensure_ascii=False)

                    ex_prompt = build_knowledge_extraction_prompt(source_text=source_text, source_meta=source_meta)
                    ex_raw = await self._llm(self.settings.review_model, ex_prompt, max_tokens=2200)
                    ex_data = self._extract_json(ex_raw) or {}
                    atoms = ex_data.get("atoms") if isinstance(ex_data, dict) else []
                    if not isinstance(atoms, list):
                        atoms = []

                    if _stop_now():
                        stop_requested = True
                        break

                    inserted_nodes = []
                    for atom in atoms[:30]:
                        if _stop_now():
                            stop_requested = True
                            break
                        if not isinstance(atom, dict):
                            continue
                        text_k = str(atom.get("text_knowledge") or "").strip()
                        if len(text_k) < 20:
                            continue
                        try:
                            imp = float(atom.get("importance", 0.5) or 0.5)
                        except Exception:
                            imp = 0.5
                        imp = max(0.0, min(1.0, imp))
                        tags = atom.get("tags") if isinstance(atom.get("tags"), list) else []

                        lint_prompt = build_knowledge_lint_prompt("knowledge", json.dumps(atom, ensure_ascii=False))
                        lint_raw = await self._llm(self.settings.review_model, lint_prompt, max_tokens=700)
                        lint = self._extract_json(lint_raw) or {}
                        verdict = str(lint.get("verdict") or "pass").lower()
                        reason = str(lint.get("reason") or "").strip()[:2000]

                        if verdict == "fail":
                            async with self.pool.acquire() as con:
                                await con.execute(
                                    "INSERT INTO process_mining.knowledge_review (knowledge_id, reason, status) VALUES (NULL, $1, 'pending')",
                                    f"KG lint fail article_id={aid}: {reason or text_k[:240]}",
                                )
                            continue

                        status = "active" if verdict == "pass" else "review"
                        meta_source = self._first_valid_http_url(
                            source_url,
                            (art.get("pdf_url") or ""),
                            (art.get("url_article") or ""),
                        )
                        meta = {
                            "source": meta_source,
                            "article_id": aid,
                            "article_title": art.get("title") or "",
                            "tags": tags,
                            "pipeline": "kg",
                        }
                        async with self.pool.acquire() as con:
                            existing_id = await con.fetchval(
                                """
                                SELECT id
                                FROM process_mining.knowledge
                                WHERE text_knowledge = $1
                                  AND metadata_knowledge ->> 'article_id' = $2
                                ORDER BY id DESC
                                LIMIT 1
                                """,
                                text_k,
                                str(aid),
                            )
                            if existing_id is not None:
                                kid = int(existing_id)
                            else:
                                kid = await con.fetchval(
                                    """
                                    INSERT INTO process_mining.knowledge
                                        (text_knowledge, metadata_knowledge, importance, status, provenance, created_at, updated_at)
                                    VALUES ($1, $2::jsonb, $3, $4, 'kg-pipeline', now(), now())
                                    RETURNING id
                                    """,
                                    text_k, json.dumps(meta, ensure_ascii=False), imp, status,
                                )
                                created_nodes += 1
                        kid = int(kid)
                        inserted_nodes.append({"id": kid, "text_knowledge": text_k})
                        await self._upsert_knowledge_embedding(kid, aid, text_k)

                    if stop_requested:
                        break

                    if inserted_nodes:
                        rel_prompt = build_relation_inference_prompt(json.dumps(inserted_nodes, ensure_ascii=False))
                        rel_raw = await self._llm(self.settings.review_model, rel_prompt, max_tokens=1800)
                        rel_data = self._extract_json(rel_raw) or {}
                        rels = rel_data.get("relations") if isinstance(rel_data, dict) else []
                        if isinstance(rels, list):
                            id_set = {int(n["id"]) for n in inserted_nodes}
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
                                    continue
                                try:
                                    rscore = float(rr.get("relevance_score", 0.5) or 0.5)
                                except Exception:
                                    rscore = 0.5
                                try:
                                    rimp = float(rr.get("importance", 0.5) or 0.5)
                                except Exception:
                                    rimp = 0.5
                                rscore = max(0.0, min(1.0, rscore))
                                rimp = max(0.0, min(1.0, rimp))

                                # density guard: режем слабые/generic-связи
                                if not await self._allow_relation_by_density(tid, rtype, rscore):
                                    continue

                                async with self.pool.acquire() as con:
                                    existing_rel = await con.fetchval(
                                        """
                                        SELECT id
                                        FROM process_mining.entity_relations
                                        WHERE source_id = $1
                                          AND target_id = $2
                                          AND relation_type = $3
                                        ORDER BY id DESC
                                        LIMIT 1
                                        """,
                                        sid, tid, rtype,
                                    )
                                    if existing_rel is None:
                                        await con.execute(
                                            """
                                            INSERT INTO process_mining.entity_relations
                                                (source_id, target_id, relation_type, relevance_score, importance, provenance, created_at)
                                            VALUES ($1,$2,$3,$4,$5,'kg-pipeline',now())
                                            """,
                                            sid, tid, rtype, rscore, rimp,
                                        )
                                        created_edges += 1

                        # Cross-article linking: pairwise-инференс.
                        # Для КАЖДОГО нового узла берём его top-K семантических
                        # кандидатов из графа (гейт по vec_sim) и отдельным
                        # промптом просим LLM оценить связь A(new) <-> B(candidate).
                        inserted_id_set = {int(n["id"]) for n in inserted_nodes}
                        for nn in inserted_nodes[:25]:
                            if _stop_now():
                                stop_requested = True
                                break
                            new_id = int(nn.get("id"))
                            new_text = str(nn.get("text_knowledge") or "").strip()
                            if len(new_text) < 20:
                                continue

                            cands = await self._hybrid_knowledge_candidates(
                                query_text=new_text,
                                article_id=aid,
                                exclude_ids=inserted_id_set,
                                top_k=8,
                                min_vec_sim=float(self.settings.kg_cross_min_vec_sim or 0.68),
                            )
                            cand_nodes = []
                            for c in cands:
                                cid = int(c.get("id"))
                                if cid in inserted_id_set:
                                    continue
                                ctext = str(c.get("text_knowledge") or "").strip()
                                if len(ctext) < 20:
                                    continue
                                cand_nodes.append({"id": cid, "text_knowledge": ctext})
                            if not cand_nodes:
                                continue

                            cross_candidates += len(cand_nodes)
                            cand_id_set = {int(c["id"]) for c in cand_nodes}

                            cross_prompt = build_cross_link_prompt(
                                json.dumps({"id": new_id, "text_knowledge": new_text}, ensure_ascii=False),
                                json.dumps(cand_nodes, ensure_ascii=False),
                            )
                            cross_raw = await self._llm(self.settings.review_model, cross_prompt, max_tokens=1800)
                            cross_data = self._extract_json(cross_raw) or {}
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
                                # Строго A(new) <-> B(candidate) для ЭТОГО нового узла
                                pair_new_to_old = (sid == new_id and tid in cand_id_set)
                                pair_old_to_new = (tid == new_id and sid in cand_id_set)
                                if not (pair_new_to_old or pair_old_to_new):
                                    continue
                                cross_pairs_evaluated += 1
                                rtype = str(rr.get("relation_type") or "relates_to").strip()
                                if rtype not in allowed_rel:
                                    continue
                                try:
                                    rscore = float(rr.get("relevance_score", 0.5) or 0.5)
                                except Exception:
                                    rscore = 0.5
                                try:
                                    rimp = float(rr.get("importance", 0.5) or 0.5)
                                except Exception:
                                    rimp = 0.5
                                rscore = max(0.0, min(1.0, rscore))
                                rimp = max(0.0, min(1.0, rimp))

                                if not await self._allow_relation_by_density(tid, rtype, rscore):
                                    continue

                                async with self.pool.acquire() as con:
                                    existing_rel = await con.fetchval(
                                        """
                                        SELECT id
                                        FROM process_mining.entity_relations
                                        WHERE source_id = $1
                                          AND target_id = $2
                                          AND relation_type = $3
                                        ORDER BY id DESC
                                        LIMIT 1
                                        """,
                                        sid, tid, rtype,
                                    )
                                    if existing_rel is None:
                                        await con.execute(
                                            """
                                            INSERT INTO process_mining.entity_relations
                                                (source_id, target_id, relation_type, relevance_score, importance, provenance, created_at)
                                            VALUES ($1,$2,$3,$4,$5,'kg-pipeline-cross',now())
                                            """,
                                            sid, tid, rtype, rscore, rimp,
                                        )
                                        created_edges += 1
                                        cross_edges += 1

                        if _stop_now():
                            stop_requested = True
                            break

                        topic = (art.get("title") or "").strip() or f"Article #{aid}"
                        wiki_prompt = build_wiki_synthesis_prompt(topic=topic, nodes_json=json.dumps(inserted_nodes, ensure_ascii=False))
                        wiki_raw = await self._llm(self.settings.review_model, wiki_prompt, max_tokens=2600)
                        wiki_data = self._extract_json(wiki_raw) or {}
                        if isinstance(wiki_data, dict):
                            title = str(wiki_data.get("title") or topic).strip()[:500]
                            content_md = str(wiki_data.get("content_md") or "").strip()
                            src_ids = wiki_data.get("source_ids") if isinstance(wiki_data.get("source_ids"), list) else [n["id"] for n in inserted_nodes]
                            src_ids = [int(x) for x in src_ids if str(x).isdigit()]
                            index_entry = str(wiki_data.get("index_entry") or title)[:300]
                            try:
                                wimp = float(wiki_data.get("importance", 0.6) or 0.6)
                            except Exception:
                                wimp = 0.6
                            wimp = max(0.0, min(1.0, wimp))

                            if content_md:
                                lint_w_prompt = build_knowledge_lint_prompt("wiki_page", json.dumps({
                                    "title": title,
                                    "content_md": content_md,
                                    "source_ids": src_ids,
                                    "index_entry": index_entry,
                                }, ensure_ascii=False))
                                lint_w_raw = await self._llm(self.settings.review_model, lint_w_prompt, max_tokens=700)
                                lint_w = self._extract_json(lint_w_raw) or {}
                                w_verdict = str(lint_w.get("verdict") or "pass").lower()
                                w_reason = str(lint_w.get("reason") or "").strip()[:2000]

                                if w_verdict == "fail":
                                    async with self.pool.acquire() as con:
                                        await con.execute(
                                            "INSERT INTO process_mining.knowledge_review (knowledge_id, reason, status) VALUES (NULL, $1, 'pending')",
                                            f"KG wiki lint fail article_id={aid}: {w_reason or title}",
                                        )
                                else:
                                    w_status = "active" if w_verdict == "pass" else "draft"
                                    source_url_text = self._first_valid_http_url(source_url)
                                    src_ids_json = json.dumps(src_ids)
                                    async with self.pool.acquire() as con:
                                        existing_wid = await con.fetchval(
                                            """
                                            SELECT id
                                            FROM process_mining.wiki_pages
                                            WHERE title = $1
                                              AND index_entry = $2
                                              AND (
                                                    ($3 <> '' AND COALESCE(source_url,'') = $3)
                                                 OR ($3 = '' AND source_ids = $4::jsonb)
                                              )
                                            ORDER BY id DESC
                                            LIMIT 1
                                            """,
                                            title,
                                            index_entry,
                                            source_url_text,
                                            src_ids_json,
                                        )

                                        if existing_wid is None:
                                            wid = await con.fetchval(
                                                """
                                                INSERT INTO process_mining.wiki_pages
                                                    (title, content_md, source_ids, source_url, index_entry, status, importance, created_at, updated_at)
                                                VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, now(), now())
                                                RETURNING id
                                                """,
                                                title,
                                                content_md,
                                                src_ids_json,
                                                source_url_text,
                                                index_entry,
                                                w_status,
                                                wimp,
                                            )
                                            created_pages += 1
                                        else:
                                            wid = int(existing_wid)
                                            await con.execute(
                                                """
                                                UPDATE process_mining.wiki_pages
                                                SET content_md = $2,
                                                    source_ids = $3::jsonb,
                                                    source_url = $4,
                                                    status = $5,
                                                    importance = $6,
                                                    updated_at = now()
                                                WHERE id = $1
                                                """,
                                                wid,
                                                content_md,
                                                src_ids_json,
                                                source_url_text,
                                                w_status,
                                                wimp,
                                            )
                                    await self._upsert_wiki_page_embedding(int(wid), content_md)

                    if _stop_now():
                        stop_requested = True
                        break

                    async with self.pool.acquire() as con:
                        await con.execute(
                            """
                            UPDATE process_mining.papers_metadata
                            SET kg_processed = TRUE,
                                kg_processed_at = now(),
                                updated_at = now()
                            WHERE id = $1
                            """,
                            aid,
                        )
                    processed += 1
                    self._kg_processed_total += 1
                    await self._maybe_run_periodic_collapse()
                except Exception as e:
                    errors += 1
                    print(f"[pmanalyze] process_knowledge_graph article_id={aid} error: {type(e).__name__}: {e}")

            return {
                "ok": True,
                "total": len(rows),
                "processed": processed,
                "created_nodes": created_nodes,
                "created_edges": created_edges,
                "created_pages": created_pages,
                "cross_candidates": cross_candidates,
                "cross_pairs_evaluated": cross_pairs_evaluated,
                "cross_edges": cross_edges,
                "errors": errors,
                "stop_requested": stop_requested,
            }
        finally:
            await self._unlock()

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

    async def _mark_invalid_source_url(self, aid: int, bad_url: str):
        """Если удалось прочитать статью через альтернативный URL, помечаем исходный битый URL как -1."""
        if not bad_url:
            return
        async with self.pool.acquire() as con:
            await con.execute(
                """UPDATE process_mining.papers_metadata
                   SET url_article = CASE WHEN url_article=$2 THEN '-1' ELSE url_article END,
                       pdf_url     = CASE WHEN pdf_url=$2 THEN '-1' ELSE pdf_url END,
                       updated_at=now()
                   WHERE id=$1""",
                aid, bad_url,
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
        text = (review_md or "").strip()
        if not text:
            raise ValueError("empty_review_md")
        if self._looks_like_refusal(text) or self._looks_like_unavailable_review_template(text):
            raise ValueError("unsafe_review_template_blocked")

        async with self.pool.acquire() as con:
            review_id = await con.fetchval(
                """INSERT INTO process_mining.reviews (article_id, review_md, model_name)
                   VALUES ($1,$2,$3)
                   ON CONFLICT (article_id) DO UPDATE
                   SET review_md=EXCLUDED.review_md, model_name=EXCLUDED.model_name, created_at=now()
                   RETURNING id""",
                aid, text, self.settings.review_model,
            )
            await con.execute(
                "UPDATE process_mining.papers_metadata SET review_flag=TRUE, updated_at=now() WHERE id=$1",
                aid,
            )

        # После успешного ревью сразу векторизуем и сохраняем в review_embeddings.
        await self._upsert_review_embedding(aid, int(review_id), text)

    async def _upsert_review_embedding(self, aid: int, review_id: int, review_md: str):
        text = (review_md or "").strip()
        if not text:
            return
        try:
            emb = await embed_text(text[:30000])
            vec_lit = "[" + ",".join(f"{x:.7f}" for x in emb) + "]"
            async with self.pool.acquire() as con:
                await con.execute(
                    """
                    INSERT INTO process_mining.review_embeddings
                        (article_id, review_id, content, embedding, tsv, model, created_at, updated_at)
                    VALUES ($1, $2, $3, $4::vector, to_tsvector($6, $3), $5, now(), now())
                    ON CONFLICT (article_id) DO UPDATE SET
                        review_id = EXCLUDED.review_id,
                        content   = EXCLUDED.content,
                        embedding = EXCLUDED.embedding,
                        tsv       = EXCLUDED.tsv,
                        model     = EXCLUDED.model,
                        updated_at = now()
                    """,
                    aid, review_id, text, vec_lit, EMBED_MODEL, TS_CONFIG,
                )
        except Exception as e:
            # Не валим пайплайн из-за сбоя эмбеддинга, ревью уже сохранено.
            print(f"[pmanalyze] embedding upsert error article_id={aid}: {type(e).__name__}: {e}")
