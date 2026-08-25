"""PMAnalyze API: управление еженедельным пайплайном ревью статей Process Mining."""
import os
import asyncpg
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse

from app.pipeline import PMAnalyzePipeline, Settings

DB_DSN = os.getenv("PMANALYZE_DSN", "postgresql://catchpm:catchpm2026@catchpm-postgres:5432/catchpm_chat")

pipeline: PMAnalyzePipeline | None = None
pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pipeline, pool
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)
    pipeline = PMAnalyzePipeline(pool, Settings())
    await pipeline.start()
    yield
    if pipeline:
        await pipeline.stop()
    if pool:
        await pool.close()


app = FastAPI(title="PMAnalyze", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True, "service": "PMAnalyze"}


@app.post("/api/pipeline/run")
async def trigger_run(background: BackgroundTasks):
    """Ручной запуск полного цикла (сбор -> релевантность -> ревью)."""
    background.add_task(pipeline.run_once)
    return {"ok": True, "status": "started"}


@app.post("/api/pipeline/process")
async def trigger_process(limit: int = 50):
    """Обработать уже собранные статьи со статусом new/review_error (без нового сбора)."""
    res = await pipeline.process_pending(limit=limit)
    return {"ok": True, **res}


@app.get("/api/articles")
async def list_articles(only_relevant: bool = False, limit: int = 50, offset: int = 0):
    where = "WHERE is_relevant = TRUE" if only_relevant else ""
    async with pool.acquire() as con:
        rows = await con.fetch(f"""
            SELECT id, external_id, source, title, date_sub, url_article, pdf_url,
                   category, relevance_score, is_relevant, review_flag, llm_status, processed_at
            FROM process_mining.papers_metadata
            {where}
            ORDER BY date_sub DESC NULLS LAST, id DESC
            LIMIT $1 OFFSET $2
        """, min(200, max(1, limit)), max(0, offset))
    return {"ok": True, "count": len(rows), "articles": [dict(r) for r in rows]}


@app.get("/api/articles/{article_id}/review")
async def get_review(article_id: int):
    async with pool.acquire() as con:
        art = await con.fetchrow(
            "SELECT id, title, category, relevance_score FROM process_mining.papers_metadata WHERE id=$1",
            article_id)
        rev = await con.fetchrow(
            "SELECT review_md, model_name, created_at FROM process_mining.reviews WHERE article_id=$1",
            article_id)
    if not art:
        return JSONResponse({"ok": False, "error": "article_not_found"}, status_code=404)
    return {"ok": True, "article": dict(art),
            "review": dict(rev) if rev else None}


@app.get("/api/runs")
async def list_runs(limit: int = 20):
    async with pool.acquire() as con:
        rows = await con.fetch(
            "SELECT * FROM process_mining.parser_runs ORDER BY id DESC LIMIT $1",
            min(100, max(1, limit)))
    return {"ok": True, "runs": [dict(r) for r in rows]}


@app.get("/api/stats")
async def stats():
    async with pool.acquire() as con:
        row = await con.fetchrow("""
            SELECT count(*) total,
                   count(*) FILTER (WHERE is_relevant) relevant,
                   count(*) FILTER (WHERE review_flag) reviewed,
                   count(*) FILTER (WHERE llm_status='new') pending
            FROM process_mining.papers_metadata
        """)
    return {"ok": True, **dict(row)}
