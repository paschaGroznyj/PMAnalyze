import asyncio
import json
from datetime import datetime, timezone


class KGRunManager:
    def __init__(self, pool, pipeline):
        self.pool = pool
        self.pipeline = pipeline
        self._lock = asyncio.Lock()
        self._task = None
        self._stop_event = None
        self._state = {
            "running": False,
            "stop_requested": False,
            "total": 0,
            "done": 0,
            "remaining": 0,
            "started_at": None,
            "finished_at": None,
            "created_nodes": 0,
            "created_edges": 0,
            "created_pages": 0,
            "errors": 0,
            "last_error": None,
        }
        # TTL-кеш графа: {key: (ts, payload)}; инвалидируется после прогона
        self._graph_cache = {}
        self._graph_cache_ttl = 60.0

    @staticmethod
    def _now_iso():
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _coerce_id_list(v):
        """source_ids может прийти как list, как JSON-строка '[1,2,3]' (asyncpg без codec)
        или как None. Приводим к списку int."""
        if v is None:
            return []
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except Exception:
                return []
        if not isinstance(v, (list, tuple)):
            return []
        out = []
        for x in v:
            try:
                out.append(int(x))
            except Exception:
                continue
        return out

    async def _count_pending(self) -> int:
        async with self.pool.acquire() as con:
            v = await con.fetchval(
                """
                SELECT count(*)
                FROM process_mining.papers_metadata
                WHERE is_relevant = TRUE
                  AND COALESCE(kg_processed, FALSE) = FALSE
                """
            )
        return int(v or 0)

    async def status(self) -> dict:
        async with self._lock:
            s = dict(self._state)
        return {"ok": True, **s}

    async def start(self, limit: int = 0, batch_size: int = 1) -> dict:
        async with self._lock:
            if self._state["running"]:
                return {"ok": False, "status": "busy", "reason": "kg_already_running", **self._state}

            # cleanup old task ref
            if self._task and self._task.done():
                self._task = None

            pending = await self._count_pending()
            if pending <= 0:
                self._state = {
                    **self._state,
                    "running": False,
                    "stop_requested": False,
                    "total": 0,
                    "done": 0,
                    "remaining": 0,
                    "started_at": None,
                    "finished_at": self._now_iso(),
                    "last_error": None,
                }
                return {"ok": True, "status": "idle", **self._state}

            total = pending if int(limit or 0) <= 0 else min(pending, int(limit))
            self._stop_event = asyncio.Event()
            self._state = {
                "running": True,
                "stop_requested": False,
                "total": int(total),
                "done": 0,
                "remaining": int(total),
                "started_at": self._now_iso(),
                "finished_at": None,
                "created_nodes": 0,
                "created_edges": 0,
                "created_pages": 0,
                "errors": 0,
                "last_error": None,
            }
            self._task = asyncio.create_task(self._runner(total=total, batch_size=max(1, int(batch_size or 1))))
            return {"ok": True, "status": "started", **self._state}

    async def stop(self) -> dict:
        async with self._lock:
            if not self._state["running"]:
                return {"ok": True, "status": "idle", **self._state}
            self._state["stop_requested"] = True
            if self._stop_event:
                self._stop_event.set()
            return {"ok": True, "status": "stopping", **self._state}

    async def _runner(self, total: int, batch_size: int):
        stalled = 0
        try:
            while True:
                if self._stop_event and self._stop_event.is_set():
                    break

                async with self._lock:
                    done = int(self._state.get("done") or 0)
                if done >= total:
                    break

                pending_before = await self._count_pending()
                if pending_before <= 0:
                    break

                step = min(batch_size, total - done, pending_before)
                if step <= 0:
                    break

                res = await self.pipeline.process_knowledge_graph(limit=step, stop_event=self._stop_event)
                if not res.get("ok") and res.get("skipped") == "locked":
                    await asyncio.sleep(1.0)
                    continue

                processed = int(res.get("processed") or 0)
                p_after = await self._count_pending()
                progressed = max(0, pending_before - p_after)
                inc = progressed if progressed > 0 else processed

                async with self._lock:
                    self._state["created_nodes"] += int(res.get("created_nodes") or 0)
                    self._state["created_edges"] += int(res.get("created_edges") or 0)
                    self._state["created_pages"] += int(res.get("created_pages") or 0)
                    self._state["errors"] += int(res.get("errors") or 0)
                    self._state["done"] = min(total, int(self._state["done"] or 0) + max(0, inc))
                    self._state["remaining"] = max(0, total - int(self._state["done"] or 0))

                if inc <= 0:
                    stalled += 1
                    if stalled >= 3:
                        async with self._lock:
                            self._state["last_error"] = "kg_progress_stalled"
                        break
                else:
                    stalled = 0
        except Exception as e:
            async with self._lock:
                self._state["last_error"] = f"{type(e).__name__}: {e}"
        finally:
            async with self._lock:
                self._state["running"] = False
                self._state["finished_at"] = self._now_iso()
                self._task = None
                self._stop_event = None
            self._graph_cache = {}  # граф изменился — сброс кеша

    async def graph_payload(self, limit_nodes: int = 450, limit_wiki: int = 120) -> dict:
        import time
        ln = max(50, min(2000, int(limit_nodes or 450)))
        lw = max(20, min(500, int(limit_wiki or 120)))
        key = (ln, lw)
        now = time.monotonic()
        hit = self._graph_cache.get(key)
        if hit and (now - hit[0]) < self._graph_cache_ttl:
            return {**hit[1], "cached": True}
        payload = await self._graph_payload_uncached(ln, lw)
        if payload.get("ok"):
            self._graph_cache[key] = (now, payload)
        return payload

    async def _graph_payload_uncached(self, limit_nodes: int = 450, limit_wiki: int = 120) -> dict:
        limit_nodes = max(50, min(2000, int(limit_nodes or 450)))
        limit_wiki = max(20, min(500, int(limit_wiki or 120)))
        async with self.pool.acquire() as con:
            nodes_rows = await con.fetch(
                """
                SELECT id, text_knowledge, metadata_knowledge, importance, status, created_at
                FROM process_mining.knowledge
                ORDER BY id DESC
                LIMIT $1
                """,
                limit_nodes,
            )

            ids = [int(r["id"]) for r in nodes_rows]
            edges_rows = []
            if ids:
                edges_rows = await con.fetch(
                    """
                    SELECT id, source_id, target_id, relation_type, relevance_score, importance
                    FROM process_mining.entity_relations
                    WHERE source_id = ANY($1::int[]) AND target_id = ANY($1::int[])
                    ORDER BY id DESC
                    LIMIT $2
                    """,
                    ids,
                    limit_nodes * 6,
                )

            wiki_rows = await con.fetch(
                """
                SELECT id, title, content_md, source_ids, status, importance, updated_at
                FROM process_mining.wiki_pages
                ORDER BY id DESC
                LIMIT $1
                """,
                limit_wiki,
            )

        out_nodes = []
        id_set = set(ids)
        for r in nodes_rows:
            md = r["metadata_knowledge"] if isinstance(r["metadata_knowledge"], dict) else {}
            text = str(r["text_knowledge"] or "")
            out_nodes.append({
                "id": f"k{int(r['id'])}",
                "kind": "knowledge",
                "label": text[:72] + ("…" if len(text) > 72 else ""),
                "title": text,
                "importance": float(r["importance"] or 0),
                "status": r["status"],
                "article_id": md.get("article_id") if isinstance(md, dict) else None,
                "source": md.get("source") if isinstance(md, dict) else None,
                "created_at": str(r["created_at"] or ""),
            })

        out_edges = []
        for e in edges_rows:
            sid = int(e["source_id"])
            tid = int(e["target_id"])
            if sid not in id_set or tid not in id_set:
                continue
            out_edges.append({
                "id": f"e{int(e['id'])}",
                "from": f"k{sid}",
                "to": f"k{tid}",
                "label": e["relation_type"],
                "kind": "relation",
                "score": float(e["relevance_score"] or 0),
                "importance": float(e["importance"] or 0),
            })

        for w in wiki_rows:
            wid = int(w["id"])
            title = str(w["title"] or f"Wiki #{wid}")
            out_nodes.append({
                "id": f"w{wid}",
                "kind": "wiki",
                "label": title[:72] + ("…" if len(title) > 72 else ""),
                "title": title,
                "importance": float(w["importance"] or 0),
                "status": w["status"],
                "updated_at": str(w["updated_at"] or ""),
            })
            src_ids = self._coerce_id_list(w["source_ids"])
            for sid in src_ids:
                try:
                    sid = int(sid)
                except Exception:
                    continue
                if sid in id_set:
                    out_edges.append({
                        "id": f"wk{wid}_{sid}",
                        "from": f"k{sid}",
                        "to": f"w{wid}",
                        "label": "in_wiki",
                        "kind": "wiki_link",
                        "score": 1.0,
                        "importance": 0.5,
                    })

        return {
            "ok": True,
            "nodes": out_nodes,
            "edges": out_edges,
            "counts": {
                "knowledge": len(nodes_rows),
                "relations": len([e for e in out_edges if e.get("kind") == "relation"]),
                "wiki_pages": len(wiki_rows),
            },
        }

    async def card_payload(self, node_id: str) -> dict:
        node_id = (node_id or "").strip().lower()
        if not node_id or node_id[0] not in ("k", "w") or not node_id[1:].isdigit():
            return {"ok": False, "error": "bad_node_id"}
        kind = "knowledge" if node_id.startswith("k") else "wiki"
        rid = int(node_id[1:])

        async with self.pool.acquire() as con:
            if kind == "knowledge":
                row = await con.fetchrow(
                    """
                    SELECT id, text_knowledge, metadata_knowledge, importance, status, provenance, created_at, updated_at
                    FROM process_mining.knowledge
                    WHERE id=$1
                    """,
                    rid,
                )
                if not row:
                    return {"ok": False, "error": "not_found"}
                md = row["metadata_knowledge"] if isinstance(row["metadata_knowledge"], dict) else {}
                return {
                    "ok": True,
                    "kind": "knowledge",
                    "id": rid,
                    "title": f"Knowledge #{rid}",
                    "text": row["text_knowledge"] or "",
                    "metadata": md,
                    "importance": float(row["importance"] or 0),
                    "status": row["status"],
                    "provenance": row["provenance"],
                    "article_id": md.get("article_id") if isinstance(md, dict) else None,
                    "created_at": str(row["created_at"] or ""),
                    "updated_at": str(row["updated_at"] or ""),
                }

            row = await con.fetchrow(
                """
                SELECT id, title, content_md, source_ids, source_url, links, index_entry, status, importance, created_at, updated_at
                FROM process_mining.wiki_pages
                WHERE id=$1
                """,
                rid,
            )
            if not row:
                return {"ok": False, "error": "not_found"}
            source_url = str(row["source_url"] or "").strip()

            # Совместимость со старыми данными: если source_url пустой, пробуем legacy links[].
            links = row["links"] if isinstance(row["links"], list) else []
            if not source_url:
                for u in links:
                    su = str(u or "").strip()
                    if su.startswith("http://") or su.startswith("https://"):
                        source_url = su
                        break

            # Fallback: если в wiki нет валидного URL, пробуем взять source
            # из metadata_knowledge связанных knowledge-узлов (source_ids).
            src_ids = self._coerce_id_list(row["source_ids"])
            if not source_url and src_ids:
                krow = await con.fetchrow(
                    """
                    SELECT metadata_knowledge->>'source' AS src
                    FROM process_mining.knowledge
                    WHERE id = ANY($1::int[])
                      AND (metadata_knowledge->>'source') ~ '^https?://'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    src_ids,
                )
                if krow and krow.get("src"):
                    source_url = str(krow["src"]).strip()

            return {
                "ok": True,
                "kind": "wiki",
                "id": rid,
                "title": row["title"] or f"Wiki #{rid}",
                "text": row["content_md"] or "",
                "source_ids": row["source_ids"] if isinstance(row["source_ids"], list) else [],
                "source_url": source_url,
                "index_entry": row["index_entry"] or "",
                "importance": float(row["importance"] or 0),
                "status": row["status"],
                "created_at": str(row["created_at"] or ""),
                "updated_at": str(row["updated_at"] or ""),
            }
