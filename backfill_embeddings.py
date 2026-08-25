"""Бэкфилл эмбеддингов ревью в process_mining.review_embeddings.

Берём последнее review на статью (по created_at), векторизуем review_md через
Qwen/Qwen3-VL-Embedding-8B (cloud.ru), заполняем content/embedding/tsv/model.
Идемпотентно: пропускает записи, у которых review_id и content не изменились.
"""
import os
import asyncio
import asyncpg
import httpx

DSN = os.getenv("PMANALYZE_DSN", "postgresql://catchpm:catchpm2026@catchpm-postgres:5432/catchpm_chat")
CLOUD_KEY = os.getenv("CLOUD_LLM_ACCESS_KEY", "")
CLOUD_BASE = os.getenv("CLOUD_LLM_BASE", "https://foundation-models.api.cloud.ru/v1")
EMBED_MODEL = os.getenv("PM_EMBED_MODEL", "Qwen/Qwen3-VL-Embedding-8B")
TS_CONFIG = "russian"


def vec_literal(vec):
    return "[" + ",".join(f"{x:.7f}" for x in vec) + "]"


async def embed_one(client, text):
    r = await client.post(
        f"{CLOUD_BASE}/embeddings",
        headers={"Authorization": f"Bearer {CLOUD_KEY}", "Content-Type": "application/json"},
        json={"model": EMBED_MODEL, "input": [text[:30000]]},
    )
    r.raise_for_status()
    return r.json()["data"][0]["embedding"]


async def main():
    con = await asyncpg.connect(DSN)
    # последнее ревью на статью
    rows = await con.fetch(
        """
        SELECT DISTINCT ON (r.article_id)
               r.article_id, r.id AS review_id, r.review_md, r.model_name, r.created_at
        FROM process_mining.reviews r
        WHERE r.review_md IS NOT NULL AND length(trim(r.review_md)) > 0
        ORDER BY r.article_id, r.created_at DESC
        """
    )
    print(f"reviews to process: {len(rows)}")

    # что уже есть
    existing = {
        r["article_id"]: r["review_id"]
        for r in await con.fetch("SELECT article_id, review_id FROM process_mining.review_embeddings")
    }

    done, skipped, failed = 0, 0, 0
    async with httpx.AsyncClient(timeout=90) as client:
        for row in rows:
            aid = row["article_id"]
            content = row["review_md"].strip()
            if existing.get(aid) == row["review_id"]:
                skipped += 1
                continue
            try:
                emb = await embed_one(client, content)
            except Exception as e:
                failed += 1
                print(f"  FAIL article_id={aid}: {type(e).__name__}: {str(e)[:200]}")
                continue
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
                aid, row["review_id"], content, vec_literal(emb), EMBED_MODEL, TS_CONFIG,
            )
            done += 1
            if done % 10 == 0:
                print(f"  ...{done} embedded")

    total = await con.fetchval("SELECT count(*) FROM process_mining.review_embeddings")
    with_vec = await con.fetchval("SELECT count(*) FROM process_mining.review_embeddings WHERE embedding IS NOT NULL")
    print(f"DONE: inserted/updated={done}, skipped={skipped}, failed={failed}")
    print(f"TABLE: total={total}, with_embedding={with_vec}")
    await con.close()


if __name__ == "__main__":
    asyncio.run(main())
