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
def _extract_tokens_usage(payload: dict) -> dict:
    """Нормализует usage разных форматов ответа в единый JSON.

    Возвращает ключи: input_tokens, output_tokens, total_tokens.
    """
    usage = payload.get("usage") or {}

    # OpenAI-style
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    total_tokens = usage.get("total_tokens")

    # Anthropic/messages-style
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")

    in_tok = input_tokens if input_tokens is not None else prompt_tokens
    out_tok = output_tokens if output_tokens is not None else completion_tokens

    def _to_int(v):
        try:
            return int(v)
        except Exception:
            return 0

    in_tok = _to_int(in_tok)
    out_tok = _to_int(out_tok)
    total = _to_int(total_tokens) if total_tokens is not None else (in_tok + out_tok)

    return {
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "total_tokens": total,
    }


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


async def llm_summary(query: str, snippets: list[dict]) -> tuple[str, dict]:
    """Саммари по найденным ревью через claude-haiku-4.5.

    Возвращает (summary_text, tokens_usage_json).
    """
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
        data = r.json()
        text = data["choices"][0]["message"]["content"]
        tokens = _extract_tokens_usage(data)
        return text, tokens


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


# ---------------- 3. KG-HYBRID: поиск по узлам графа (knowledge + wiki) ----------------
async def search_kg_hybrid(
    pool,
    q: str,
    limit: int = 15,
    rrf_k: int = 60,
    vec_min_sim_knowledge: float = 0.50,
    vec_min_sim_wiki: float = 0.50,
) -> list[dict]:
    """Гибридный поиск (BM25 tsvector + pgvector) по узлам графа знаний.

    Ищет одновременно по process_mining.knowledge_embeddings (kind=knowledge)
    и process_mining.wiki_page_embeddings (kind=wiki), сливает RRF.
    Возвращает узлы с node_id вида 'k123' / 'w45' — совместимо с фронтом графа.
    """
    q = (q or "").strip()
    if not q:
        return []

    qvec = await embed_text(q)
    vec_lit = _vec_literal(qvec)

    async with pool.acquire() as con:
        # --- knowledge: BM25 + вектор ---
        k_bm25 = await con.fetch(
            """
            SELECT ke.knowledge_id AS id,
                   ts_rank_cd(ke.tsv, websearch_to_tsquery($2, $1)) AS score
            FROM process_mining.knowledge_embeddings ke
            WHERE ke.tsv @@ websearch_to_tsquery($2, $1)
            ORDER BY score DESC
            LIMIT 40
            """,
            q, TS_CONFIG,
        )
        k_vec = await con.fetch(
            """
            SELECT ke.knowledge_id AS id,
                   1 - (ke.embedding <=> $1::vector) AS score
            FROM process_mining.knowledge_embeddings ke
            WHERE ke.embedding IS NOT NULL
              AND (1 - (ke.embedding <=> $1::vector)) >= $2
            ORDER BY ke.embedding <=> $1::vector
            LIMIT 40
            """,
            vec_lit,
            vec_min_sim_knowledge,
        )
        # --- wiki: BM25 + вектор ---
        w_bm25 = await con.fetch(
            """
            SELECT we.wiki_page_id AS id,
                   ts_rank_cd(we.tsv, websearch_to_tsquery($2, $1)) AS score
            FROM process_mining.wiki_page_embeddings we
            WHERE we.tsv @@ websearch_to_tsquery($2, $1)
            ORDER BY score DESC
            LIMIT 40
            """,
            q, TS_CONFIG,
        )
        w_vec = await con.fetch(
            """
            SELECT we.wiki_page_id AS id,
                   1 - (we.embedding <=> $1::vector) AS score
            FROM process_mining.wiki_page_embeddings we
            WHERE we.embedding IS NOT NULL
              AND (1 - (we.embedding <=> $1::vector)) >= $2
            ORDER BY we.embedding <=> $1::vector
            LIMIT 40
            """,
            vec_lit,
            vec_min_sim_wiki,
        )

        # RRF-слияние с раздельными пространствами ключей (kind, id)
        rank_scores: dict[tuple, float] = {}

        def _accum(rows, kind):
            for rank, row in enumerate(rows, 1):
                key = (kind, row["id"])
                rank_scores[key] = rank_scores.get(key, 0.0) + 1.0 / (rrf_k + rank)

        _accum(k_bm25, "knowledge")
        _accum(k_vec, "knowledge")
        _accum(w_bm25, "wiki")
        _accum(w_vec, "wiki")

        has_bm25 = bool(k_bm25 or w_bm25)
        has_vec = bool(k_vec or w_vec)
        # Если нет ни текстовых совпадений, ни векторных кандидатов выше порога,
        # возвращаем пустую выдачу вместо шумного fixed-top.
        if not has_bm25 and not has_vec:
            return []

        if not rank_scores:
            return []

        top = sorted(rank_scores.items(), key=lambda x: x[1], reverse=True)[:limit]
        k_ids = [kid for (knd, kid), _ in top if knd == "knowledge"]
        w_ids = [wid for (knd, wid), _ in top if knd == "wiki"]

        k_meta = {}
        if k_ids:
            rows = await con.fetch(
                """
                SELECT k.id, k.text_knowledge, k.importance, k.status,
                       ke.article_id, p.title AS article_title
                FROM process_mining.knowledge k
                LEFT JOIN process_mining.knowledge_embeddings ke ON ke.knowledge_id = k.id
                LEFT JOIN process_mining.papers_metadata p ON p.id = ke.article_id
                WHERE k.id = ANY($1::int[])
                """,
                k_ids,
            )
            k_meta = {r["id"]: dict(r) for r in rows}

        w_meta = {}
        if w_ids:
            rows = await con.fetch(
                """
                SELECT w.id, w.title, w.content_md
                FROM process_mining.wiki_pages w
                WHERE w.id = ANY($1::int[])
                """,
                w_ids,
            )
            w_meta = {r["id"]: dict(r) for r in rows}

    results = []
    for (kind, _id), sc in top:
        if kind == "knowledge":
            m = k_meta.get(_id)
            if not m:
                continue
            text = (m.get("text_knowledge") or "").strip()
            results.append({
                "node_id": f"k{_id}",
                "kind": "knowledge",
                "id": _id,
                "title": (text[:120] or "Knowledge"),
                "content": text,
                "importance": float(m.get("importance") or 0),
                "status": m.get("status"),
                "article_id": m.get("article_id"),
                "article_title": m.get("article_title"),
                "score": round(sc, 6),
            })
        else:
            m = w_meta.get(_id)
            if not m:
                continue
            results.append({
                "node_id": f"w{_id}",
                "kind": "wiki",
                "id": _id,
                "title": m.get("title") or "Wiki page",
                "content": (m.get("content_md") or ""),
                "score": round(sc, 6),
            })
    return results


# ---------------- 4. KG-QUICK: live GET-поиск по тексту на бэке (без эмбеддингов) ----------------
async def search_kg_quick(pool, q: str, limit: int = 20) -> list[dict]:
    q = (q or "").strip()
    if len(q) < 2:
        return []

    lim = min(150, max(1, int(limit or 20)))
    async with pool.acquire() as con:
        k_rows = await con.fetch(
            """
            SELECT k.id, left(k.text_knowledge, 160) AS label
            FROM process_mining.knowledge k
            WHERE COALESCE(k.status,'active') <> 'merged'
              AND k.text_knowledge ILIKE '%' || $1 || '%'
            ORDER BY k.id DESC
            LIMIT $2
            """,
            q,
            lim,
        )

        w_rows = await con.fetch(
            """
            SELECT w.id, left(w.title, 160) AS label
            FROM process_mining.wiki_pages w
            WHERE COALESCE(w.status,'active') <> 'merged'
              AND (
                    w.title ILIKE '%' || $1 || '%'
                 OR COALESCE(w.content_md,'') ILIKE '%' || $1 || '%'
              )
            ORDER BY w.id DESC
            LIMIT $2
            """,
            q,
            lim,
        )

    out = []
    for r in k_rows:
        out.append({"node_id": f"k{int(r['id'])}", "kind": "knowledge", "id": int(r["id"]), "label": r.get("label") or "Knowledge"})
    for r in w_rows:
        out.append({"node_id": f"w{int(r['id'])}", "kind": "wiki", "id": int(r["id"]), "label": r.get("label") or "Wiki"})

    # Стабильный deterministic срез
    out.sort(key=lambda x: (x["kind"], -x["id"]))
    return out[:lim]


async def llm_summary_kg(query: str, snippets: list[dict]) -> tuple[str, dict]:
    """Саммари по найденным узлам графа знаний.

    Возвращает (summary_text, tokens_usage_json).
    """
    ctx_parts = []
    for i, s in enumerate(snippets, 1):
        kind = "WIKI" if s.get("kind") == "wiki" else "KNOWLEDGE"
        ctx_parts.append(f"[{i}] ({kind}) {s.get('title') or 'Без названия'}\n{(s.get('content') or '')[:2200]}")
    context = "\n\n---\n\n".join(ctx_parts)
    prompt = (
        f"Ты аналитик Process Mining. По запросу пользователя нужно краткое саммари "
        f"на основе найденных узлов графа знаний (knowledge-карточки и wiki-страницы). "
        f"Отвечай по-русски, структурированно, без воды.\n\n"
        f"Запрос: {query}\n\n"
        f"Найденные узлы графа:\n{context}\n\n"
        f"Дай связное саммари (3-6 предложений): что есть по теме запроса, ключевые идеи, "
        f"на какие узлы стоит смотреть (ссылайся номерами [n])."
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
        data = r.json()
        text = data["choices"][0]["message"]["content"]
        tokens = _extract_tokens_usage(data)
        return text, tokens
