"""Гибридный поиск по ревью PMAnalyze: pg_trgm (quick) + BM25/tsvector + pgvector (hybrid) + LLM-саммари.

Векторизуем и ищем по content = review_md.
Эмбеддинги: Qwen/Qwen3-VL-Embedding-8B (4096) через cloud.ru foundation-models.
LLM-саммари: anthropic/claude-haiku-4.5 через тот же шлюз.
"""
import os
import json
import httpx

CLOUD_KEY = os.getenv("CLOUD_LLM_ACCESS_KEY", "")
CLOUD_BASE = os.getenv("CLOUD_LLM_BASE", "https://foundation-models.api.cloud.ru/v1")
EMBED_MODEL = os.getenv("PM_EMBED_MODEL", "Qwen/Qwen3-VL-Embedding-8B")
EMBED_DIM = 4096
LLM_MODEL = os.getenv("PM_SUMMARY_MODEL", "anthropic/claude-haiku-4.5")

# Русскоязычный ts-конфиг: 'russian' покрывает и латиницу приемлемо для смешанного текста.
TS_CONFIG = "russian"


# ---------------- Внешние вызовы (cloud.ru) ----------------
async def embed_text(text: str) -> list[float]:
    """Один эмбеддинг для запроса/ревью. Кидает исключение при сбое."""
    text = (text or "").strip()
    if not text:
        raise ValueError("empty text for embedding")
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{CLOUD_BASE}/embeddings",
            headers={"Authorization": f"Bearer {CLOUD_KEY}", "Content-Type": "application/json"},
            json={"model": EMBED_MODEL, "input": [text]},
        )
        r.raise_for_status()
        return r.json()["data"][0]["embedding"]


async def llm_summary(query: str, snippets: list[dict]) -> str:
    """Саммари по найденным ревью через claude-haiku-4.5."""
    ctx_parts = []
    for i, s in enumerate(snippets, 1):
        ctx_parts.append(f"[{i}] {s.get('title') or 'Без названия'}\n{(s.get('content') or '')[:2500]}")
    context = "\n\n---\n\n".join(ctx_parts)
    prompt = (
        f"Ты аналитик Process Mining. По запросу пользователю нужно краткое саммари "
        f"на основе найденных ревью статей. Отвечай по-русски, структурированно, без воды.\n\n"
        f"Запрос: {query}\n\n"
        f"Найденные ревью:\n{context}\n\n"
        f"Дай связное саммари (3-6 предложений): что есть по теме запроса, какие ключевые идеи, "
        f"на какие статьи стоит смотреть (ссылайся номерами [n])."
    )
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"{CLOUD_BASE}/chat/completions",
            headers={"Authorization": f"Bearer {CLOUD_KEY}", "Content-Type": "application/json"},
            json={
                "model": LLM_MODEL,
                "max_tokens": 2500,
                "temperature": 0.5,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]


def _vec_literal(vec: list[float]) -> str:
    """pgvector-литерал '[a,b,c]' для параметра запроса."""
    return "[" + ",".join(f"{x:.7f}" for x in vec) + "]"


# ---------------- 1. QUICK: live-поиск по словам (pg_trgm/ILIKE) ----------------
async def search_quick(pool, q: str, limit: int = 10) -> list[dict]:
    q = (q or "").strip()
    if len(q) < 2:
        return []
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT e.article_id, e.review_id, p.title, p.url_article,
                   left(e.content, 240) AS snippet,
                   similarity(e.content, $1) AS sim
            FROM process_mining.review_embeddings e
            JOIN process_mining.papers_metadata p ON p.id = e.article_id
            WHERE e.content ILIKE '%' || $1 || '%'
               OR e.content % $1
            ORDER BY (e.content ILIKE '%' || $1 || '%') DESC, sim DESC NULLS LAST
            LIMIT $2
            """,
            q, limit,
        )
    return [dict(r) for r in rows]


# ---------------- 2. HYBRID: BM25 (tsvector) + вектор, слияние RRF ----------------
async def search_hybrid(pool, q: str, limit: int = 10, rrf_k: int = 60) -> list[dict]:
    q = (q or "").strip()
    if not q:
        return []

    qvec = await embed_text(q)
    vec_lit = _vec_literal(qvec)

    async with pool.acquire() as con:
        # BM25-подобный рейтинг через ts_rank_cd + websearch_to_tsquery
        bm25 = await con.fetch(
            """
            SELECT e.article_id,
                   ts_rank_cd(e.tsv, websearch_to_tsquery($2, $1)) AS score
            FROM process_mining.review_embeddings e
            WHERE e.tsv @@ websearch_to_tsquery($2, $1)
            ORDER BY score DESC
            LIMIT 50
            """,
            q, TS_CONFIG,
        )
        # Векторный: косинусная дистанция (<=>), меньше = ближе
        vec = await con.fetch(
            f"""
            SELECT e.article_id,
                   1 - (e.embedding <=> $1::vector) AS score
            FROM process_mining.review_embeddings e
            WHERE e.embedding IS NOT NULL
            ORDER BY e.embedding <=> $1::vector
            LIMIT 50
            """,
            vec_lit,
        )

        # RRF-слияние
        rank_scores: dict[int, float] = {}
        for rank, row in enumerate(bm25, 1):
            aid = row["article_id"]
            rank_scores[aid] = rank_scores.get(aid, 0.0) + 1.0 / (rrf_k + rank)
        for rank, row in enumerate(vec, 1):
            aid = row["article_id"]
            rank_scores[aid] = rank_scores.get(aid, 0.0) + 1.0 / (rrf_k + rank)

        if not rank_scores:
            return []

        top = sorted(rank_scores.items(), key=lambda x: x[1], reverse=True)[:limit]
        ids = [aid for aid, _ in top]

        meta = await con.fetch(
            """
            SELECT e.article_id, e.review_id, p.title, p.url_article,
                   p.source, p.date_sub,
                   left(e.content, 400) AS snippet, e.content
            FROM process_mining.review_embeddings e
            JOIN process_mining.papers_metadata p ON p.id = e.article_id
            WHERE e.article_id = ANY($1::bigint[])
            """,
            ids,
        )
        by_id = {r["article_id"]: dict(r) for r in meta}

    results = []
    for aid, sc in top:
        item = by_id.get(aid)
        if not item:
            continue
        item["score"] = round(sc, 6)
        item["rrf_score"] = round(sc, 6)
        ds = item.get("date_sub")
        if ds is not None and not isinstance(ds, str):
            item["date_sub"] = ds.isoformat()
        results.append(item)
    return results
