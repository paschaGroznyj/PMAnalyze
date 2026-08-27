(function(){
  const byId = (id)=>document.getElementById(id);
  const toast = (msg)=>{ if(typeof window.toast === "function") window.toast(msg); };

  const btnStart = byId("btn-kg-start");
  const btnRefresh = byId("btn-kg-refresh");
  const runInfo = byId("kg-run-info");
  const graphInfo = byId("kg-graph-info");
  const graphEl = byId("kg-canvas");

  const btnSearchToggle = byId("btn-kg-search-toggle");
  const searchBox = byId("kg-search-box");
  const searchQ = byId("kg-search-q");
  const searchGo = byId("kg-search-go");
  const searchLlm = byId("kg-search-llm");
  const depthEl = byId("kg-depth");
  const depthVal = byId("kg-depth-val");
  const chunksCount = byId("kg-chunks-count");
  const searchHint = byId("kg-search-hint");
  const searchSummary = byId("kg-search-summary");

  if(!btnStart || !graphEl) return;

  let network = null;
  let nodesDS = null;
  let edgesDS = null;
  let knownNodeIds = new Set();
  let knownEdgeIds = new Set();
  let pollTimer = null;
  let graphTimer = null;
  let lastRunning = null;

  let nodeRawMap = new Map(); // id -> raw node payload
  let edgeRawMap = new Map(); // id -> raw edge payload
  let adjacency = new Map();  // id -> Set(neighbor ids)
  let baseLabels = new Map(); // id -> original vis label
  let baseKind = new Map();   // id -> knowledge/wiki

  let lastMatches = [];
  let lastSelection = new Set();

  const MAX_CTX = 15; // лимит чанков, реально уходящих в модель
  const HYBRID_STATE_KEY = "kg_hybrid_state_v1";
  let lastHybridState = null;

  function saveHybridState(state){
    lastHybridState = state;
    try{ sessionStorage.setItem(HYBRID_STATE_KEY, JSON.stringify(state)); }catch(_){ }
  }

  function clearHybridState(){
    lastHybridState = null;
    try{ sessionStorage.removeItem(HYBRID_STATE_KEY); }catch(_){ }
  }

  function restoreHybridStateFromSession(){
    try{
      const raw = sessionStorage.getItem(HYBRID_STATE_KEY);
      if(!raw) return;
      const st = JSON.parse(raw);
      if(!st || typeof st.q !== "string") return;
      lastHybridState = st;
      if(searchQ && !String(searchQ.value||"").trim()) searchQ.value = st.q;
      if(searchLlm && typeof st.want_summary === "boolean") searchLlm.checked = !!st.want_summary;
      if(depthEl && Number.isFinite(Number(st.depth))){
        depthEl.value = String(Math.max(0, Math.min(5, Number(st.depth))));
      }
      if(depthVal && depthEl) depthVal.textContent = String(depthEl.value || "2");
      if(searchBox && btnSearchToggle){
        searchBox.style.display = "block";
        btnSearchToggle.textContent = "Поиск по графу ▴";
      }
    }catch(_){ }
  }

  function reapplyHybridState(){
    if(!lastHybridState) return false;
    const qNow = String(searchQ?.value || "").trim();
    if(!qNow || qNow !== String(lastHybridState.q || "")) return false;

    const depth = Math.max(0, Math.min(5, Number(depthEl?.value || lastHybridState.depth || 2)));
    const found = Number(lastHybridState.found || 0);
    const used = Number(lastHybridState.used || 0);
    const maxCtx = Number(lastHybridState.max_ctx || MAX_CTX);
    const serverNodeIds = Array.isArray(lastHybridState.node_ids) ? lastHybridState.node_ids : [];
    const presentIds = serverNodeIds.filter(id => nodeRawMap.has(id));

    if(presentIds.length){
      const dmap = bfsDepth(presentIds, depth);
      applySelection(presentIds, dmap, "depth");
    }else{
      applySelection([], null, "live");
    }

    if(chunksCount) chunksCount.textContent = `найдено ${found} · в модель ${used}/${maxCtx} · depth ${depth}`;
    if(searchHint){
      searchHint.textContent = found
        ? `Гибридный поиск: найдено ${found}, в модель ${used}/${maxCtx}. На графе подсвечено ${presentIds.length}.`
        : `Гибридный поиск: совпадений в базе нет.`;
    }

    if(searchSummary){
      const wantSummary = !!lastHybridState.want_summary;
      if(!wantSummary){
        searchSummary.style.display = "none";
        searchSummary.innerHTML = "";
      }else if(lastHybridState.summary){
        const meta = `найдено ${found} · в модель ${used}/${maxCtx}`;
        searchSummary.style.display = "block";
        searchSummary.innerHTML = `<div class="mono" style="font-size:10px;color:#6b6055;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px">LLM-саммари · ${meta}</div>${mdToHtml(lastHybridState.summary)}`;
      }else if(lastHybridState.summary_error){
        searchSummary.style.display = "block";
        searchSummary.innerHTML = `<span class="mono" style="font-size:11px;color:#9a8f82">Не удалось: ${esc(lastHybridState.summary_error)}</span>`;
      }else if(found === 0){
        searchSummary.style.display = "block";
        searchSummary.innerHTML = `<span class="mono" style="font-size:11px;color:#9a8f82">Совпадений в базе нет — саммари не по чему строить.</span>`;
      }
    }

    return true;
  }

  function esc(s){
    return String(s||"").replace(/[&<>\"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));
  }

  function patchBtn(state){
    const running = !!state.running;
    const total = Number(state.total||0);
    const done = Number(state.done||0);
    const rem = Number(state.remaining||0);

    if(running){
      btnStart.classList.add("running");
      btnStart.innerHTML = `🕸 граф ${done}/${total} <span class="kg-stop-ico" aria-hidden="true"><span class="sq"></span></span>`;
      btnStart.disabled = false;
      btnStart.title = "Остановить генерацию графа";
      if(runInfo) runInfo.textContent = `в работе: ${done}/${total} · осталось: ${rem}`;
    }else{
      btnStart.classList.remove("running");
      btnStart.textContent = "🕸 создание графа";
      btnStart.disabled = false;
      btnStart.title = "Запустить генерацию графа";
      if(runInfo){
        if(state.finished_at){
          const extra = state.last_error ? ` · ошибка: ${state.last_error}` : "";
          runInfo.textContent = `завершено: ${done}/${total}${extra}`;
        }else{
          runInfo.textContent = "готов к запуску";
        }
      }
    }
  }

  async function fetchJson(url, opts){
    const r = await fetch(url, opts || {});
    const d = await r.json();
    d.__status = r.status;
    return d;
  }

  async function refreshStatus(){
    try{
      const d = await fetchJson("/api/kg/run/status");
      if(d.ok){
        patchBtn(d);
        if(lastRunning === true && !d.running){
          refreshGraph();
        }
        lastRunning = !!d.running;
      }
    }catch(_){ }
  }

  function mdToHtml(md){
    if(!md) return "";
    return esc(md)
      .replace(/\n\n+/g, "</p><p>")
      .replace(/\n/g, "<br>")
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      .replace(/`(.+?)`/g, "<code>$1</code>");
  }

  function ensureCardModal(){
    let modal = byId("kg-card-modal");
    if(modal) return modal;
    const html = `
      <div class="modal-bg" id="kg-card-modal">
        <div class="modal art-modal-box">
          <div class="art-close" id="kg-card-close">✕</div>
          <div class="mono" id="kg-card-kind">—</div>
          <h2 id="kg-card-title">—</h2>
          <div class="am-meta mono" id="kg-card-meta">—</div>
          <div class="am-body" id="kg-card-body">—</div>
          <div class="foot" id="kg-card-foot"></div>
        </div>
      </div>`;
    document.body.insertAdjacentHTML("beforeend", html);
    modal = byId("kg-card-modal");
    byId("kg-card-close").onclick = ()=> modal.classList.remove("on");
    modal.onclick = (e)=>{ if(e.target.id === "kg-card-modal") modal.classList.remove("on"); };
    return modal;
  }

  async function openCard(nodeId){
    try{
      const d = await fetchJson(`/api/graph/card?node_id=${encodeURIComponent(nodeId)}`);
      if(!d.ok){ toast("Не удалось открыть карточку"); return; }
      const modal = ensureCardModal();
      byId("kg-card-kind").textContent = d.kind === "wiki" ? "WIKI PAGE" : "KNOWLEDGE";
      byId("kg-card-title").textContent = d.title || "Без названия";
      byId("kg-card-meta").textContent = `ID: ${d.id} · status: ${d.status||"-"} · importance: ${Number(d.importance||0).toFixed(2)}`;
      byId("kg-card-body").innerHTML = `<p>${mdToHtml(d.text||"")}</p>`;
      const foot = byId("kg-card-foot");
      foot.innerHTML = "";

      if(d.article_id && typeof window.openArticle === "function"){
        const btn = document.createElement("button");
        btn.className = "btn";
        btn.textContent = `открыть статью #${d.article_id}`;
        btn.onclick = ()=>{
          modal.classList.remove("on");
          window.openArticle(Number(d.article_id));
        };
        foot.appendChild(btn);
      }
      modal.classList.add("on");
    }catch(_){ toast("Ошибка загрузки карточки"); }
  }

  function toVisNode(n){
    const isWiki = n.kind === "wiki";
    return {
      id: n.id,
      label: n.label,
      title: esc(n.title||n.label||""),
      shape: isWiki ? "star" : "dot",
      size: isWiki ? 20 : (10 + Math.round(Number(n.importance||0)*14)),
      color: isWiki
        ? {background: "#E3D5F5", border: "#1E1A16"}
        : {background: "#CDE8D5", border: "#1E1A16"},
      font: {face: "Space Mono", size: 11, color: "#1E1A16"},
    };
  }

  function toVisEdge(e){
    return {
      id: e.id,
      from: e.from,
      to: e.to,
      label: e.label || "",
      arrows: "to",
      color: {color: e.kind === "wiki_link" ? "#7b6f8f" : "#FF5A1F"},
      font: {align: "middle", size: 9, color: "#3a342d"},
      smooth: {type: "dynamic"},
      width: 1.2,
    };
  }

  function updateGraphInfo(payload){
    if(graphInfo && payload.counts){
      graphInfo.textContent = `узлы knowledge: ${payload.counts.knowledge} · wiki: ${payload.counts.wiki_pages} · связей: ${payload.counts.relations}`;
    }
  }

  function rebuildAdjacency(){
    adjacency = new Map();
    for(const id of knownNodeIds) adjacency.set(id, new Set());
    const edges = edgesDS ? edgesDS.get() : [];
    for(const e of edges){
      if(!adjacency.has(e.from)) adjacency.set(e.from, new Set());
      if(!adjacency.has(e.to)) adjacency.set(e.to, new Set());
      adjacency.get(e.from).add(e.to);
      adjacency.get(e.to).add(e.from);
    }
  }

  function resetVisual(){
    if(!nodesDS || !edgesDS) return;
    const nUpd = [];
    for(const n of nodesDS.get()){
      const kind = baseKind.get(n.id) || "knowledge";
      const isWiki = kind === "wiki";
      nUpd.push({
        id: n.id,
        label: baseLabels.get(n.id) || n.label,
        hidden: false,
        opacity: 1,
        color: isWiki ? {background:"#E3D5F5", border:"#1E1A16"} : {background:"#CDE8D5", border:"#1E1A16"},
        font: {color:"#1E1A16", size: 11, face:"Space Mono"},
      });
    }
    const eUpd = edgesDS.get().map(e=>({
      id: e.id,
      hidden: false,
      color: {color: (edgeRawMap.get(e.id)?.kind === "wiki_link" ? "#7b6f8f" : "#FF5A1F")},
      width: 1.2,
    }));
    nodesDS.update(nUpd);
    edgesDS.update(eUpd);
  }

  function findMatches(q){
    const text = String(q||"").trim().toLowerCase();
    if(text.length < 2) return [];
    const toks = text.split(/\s+/).filter(Boolean);
    const out = [];
    for(const [id, raw] of nodeRawMap.entries()){
      const hay = `${raw.label||""} ${raw.title||""}`.toLowerCase();
      const ok = toks.every(t => hay.includes(t));
      if(ok) out.push(id);
    }
    return out;
  }

  function bfsDepth(matchIds, depth){
    const dmap = new Map();
    const q = [];
    for(const id of matchIds){
      dmap.set(id, 0);
      q.push(id);
    }
    while(q.length){
      const cur = q.shift();
      const curD = dmap.get(cur) || 0;
      if(curD >= depth) continue;
      const nei = adjacency.get(cur) || new Set();
      for(const nb of nei){
        if(!dmap.has(nb)){
          dmap.set(nb, curD + 1);
          q.push(nb);
        }
      }
    }
    return dmap;
  }

  function applySelection(matchIds, depthMap, mode){
    if(!nodesDS || !edgesDS) return;
    const selected = new Set(depthMap ? Array.from(depthMap.keys()) : matchIds);
    lastSelection = selected;
    const palette = ["#FF5A1F", "#FF7D51", "#FFA581", "#FFC9B1", "#FFE3D8", "#FFF1EA"];

    const nUpd = [];
    for(const n of nodesDS.get()){
      const base = baseLabels.get(n.id) || n.label;
      if(selected.size === 0){
        const kind = baseKind.get(n.id) || "knowledge";
        const isWiki = kind === "wiki";
        nUpd.push({
          id: n.id,
          label: base,
          hidden: false,
          opacity: 1,
          color: isWiki ? {background:"#E3D5F5", border:"#1E1A16"} : {background:"#CDE8D5", border:"#1E1A16"},
          font: {color:"#1E1A16", size: 11, face:"Space Mono"},
        });
        continue;
      }
      if(selected.has(n.id)){
        const lvl = depthMap ? Number(depthMap.get(n.id) || 0) : 0;
        const bg = palette[Math.min(lvl, palette.length-1)] || palette[palette.length-1];
        const suffix = mode === "depth" && lvl > 0 ? ` (d${lvl})` : "";
        nUpd.push({
          id: n.id,
          label: base + suffix,
          hidden: false,
          opacity: 1,
          color: {background:bg, border:"#1E1A16"},
          font: {color: lvl===0 ? "#fff" : "#1E1A16", size: 11, face:"Space Mono"},
        });
      }else{
        nUpd.push({
          id: n.id,
          label: base,
          hidden: false,
          opacity: 0.18,
          color: {background:"#F1EEE9", border:"#C9BEAC"},
          font: {color:"#B0A69A", size: 10, face:"Space Mono"},
        });
      }
    }

    const eUpd = [];
    for(const e of edgesDS.get()){
      const on = selected.has(e.from) && selected.has(e.to);
      if(selected.size === 0){
        eUpd.push({
          id: e.id,
          hidden: false,
          color: {color: (edgeRawMap.get(e.id)?.kind === "wiki_link" ? "#7b6f8f" : "#FF5A1F")},
          width: 1.2,
        });
      }else if(on){
        const lvlFrom = depthMap ? Number(depthMap.get(e.from) || 0) : 0;
        const lvlTo = depthMap ? Number(depthMap.get(e.to) || 0) : 0;
        const lv = Math.max(lvlFrom, lvlTo);
        const col = lv === 0 ? "#FF5A1F" : "#FF8C66";
        eUpd.push({id: e.id, hidden: false, color: {color: col}, width: 2});
      }else{
        eUpd.push({id: e.id, hidden: false, color: {color: "#E6DDD0"}, width: 0.6});
      }
    }

    nodesDS.update(nUpd);
    edgesDS.update(eUpd);
  }

  function buildContextFromSelection(depthMap){
    const rows = [];
    const ordered = Array.from(depthMap.entries()).sort((a,b)=>a[1]-b[1]);
    for(const [id, d] of ordered){
      const raw = nodeRawMap.get(id);
      if(!raw) continue;
      const title = String(raw.title || raw.label || "").replace(/\s+/g, " ").trim();
      if(!title) continue;
      rows.push(`[d${d}] ${title}`);
    }
    return rows;
  }

  // Гибридный поиск по узлам графа. Идёт на сервер ВСЕГДА (по лупе/Enter),
  // независимо от того, есть ли локальные совпадения подсветки.
  async function runHybridKg(q){
    if(!q || q.length < 2){
      clearHybridState();
      toast("Введите запрос (мин. 2 символа)");
      return;
    }
    const depth = Math.max(0, Math.min(5, Number(depthEl?.value || 2)));
    const wantSummary = !!(searchLlm && searchLlm.checked);

    if(searchSummary){
      searchSummary.style.display = "block";
      searchSummary.innerHTML = `<span class="mono" style="font-size:11px;color:#6b6055">Гибридный поиск по графу…</span>`;
    }

    let d;
    try{
      d = await fetchJson("/api/kg/search", {
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body: JSON.stringify({q, limit: 30, max_ctx: MAX_CTX, llm_summary: wantSummary})
      });
    }catch(_){
      if(searchSummary) searchSummary.innerHTML = `<span class="mono" style="font-size:11px;color:#9a8f82">Ошибка сети при поиске.</span>`;
      return;
    }

    if(!d || !d.ok){
      if(searchSummary) searchSummary.innerHTML = `<span class="mono" style="font-size:11px;color:#9a8f82">Ошибка поиска: ${esc((d&&d.error)||"unknown")}</span>`;
      return;
    }

    const found = Number(d.found || 0);
    const used = Number(d.used || 0);
    const maxCtx = Number(d.max_ctx || MAX_CTX);

    // Обновляем бейдж и подсказку по РЕАЛЬНОМУ серверному результату.
    if(chunksCount) chunksCount.textContent = `найдено ${found} · в модель ${used}/${maxCtx} · depth ${depth}`;

    // Подсветим узлы, которые сервер вернул как результат гибридного поиска.
    const serverNodeIds = (d.items || []).map(it => it.node_id).filter(Boolean);
    saveHybridState({
      q,
      depth,
      want_summary: wantSummary,
      found,
      used,
      max_ctx: maxCtx,
      node_ids: serverNodeIds,
      summary: d.summary || null,
      summary_error: d.summary_error || null,
      ts: Date.now(),
    });
    const presentIds = serverNodeIds.filter(id => nodeRawMap.has(id));
    if(presentIds.length){
      const dmap = bfsDepth(presentIds, depth);
      applySelection(presentIds, dmap, "depth");
    }

    if(searchHint){
      searchHint.textContent = found
        ? `Гибридный поиск: найдено ${found}, в модель ${used}/${maxCtx}. На графе подсвечено ${presentIds.length}.`
        : `Гибридный поиск: совпадений в базе нет.`;
    }

    // Рендер summary
    if(!searchSummary) return;
    if(!wantSummary){
      searchSummary.style.display = "none";
      searchSummary.innerHTML = "";
      return;
    }
    if(d.summary){
      const meta = `найдено ${found} · в модель ${used}/${maxCtx}`;
      searchSummary.innerHTML = `<div class="mono" style="font-size:10px;color:#6b6055;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px">LLM-саммари · ${meta}</div>${mdToHtml(d.summary)}`;
    }else if(d.summary_error){
      searchSummary.innerHTML = `<span class="mono" style="font-size:11px;color:#9a8f82">Не удалось: ${esc(d.summary_error)}</span>`;
    }else if(found === 0){
      searchSummary.innerHTML = `<span class="mono" style="font-size:11px;color:#9a8f82">Совпадений в базе нет — саммари не по чему строить.</span>`;
    }else{
      searchSummary.innerHTML = `<span class="mono" style="font-size:11px;color:#9a8f82">Саммари не вернулось.</span>`;
    }
  }

  function updateChunksBadge(n, depth){
    if(!chunksCount) return;
    const num = Number(n||0);
    const d = Number(depth||depthEl?.value||0);
    const used = Math.min(num, MAX_CTX);
    chunksCount.textContent = `найдено ${num} · в модель ${used}/${MAX_CTX} · depth ${d}`;
  }

  function applyDepthRealtime(runSummary){
    const q = (searchQ?.value || "").trim();
    if(!q || q.length < 2){
      lastMatches = [];
      lastSelection = new Set();
      window.__kg_graph_context = "";
      updateChunksBadge(0, depthEl?.value || 0);
      if(searchHint) searchHint.textContent = "Подсветка работает при вводе. Глубина применяется в реальном времени при движении ползунка.";
      resetVisual();
      if(searchSummary && !runSummary){
        searchSummary.style.display = "none";
        searchSummary.innerHTML = "";
      }
      return;
    }

    const depth = Math.max(0, Math.min(5, Number(depthEl?.value || 2)));
    const matches = lastMatches.length ? lastMatches : findMatches(q);
    lastMatches = matches;

    if(!matches.length){
      updateChunksBadge(0, depth);
      window.__kg_graph_context = "";
      applySelection([], null, "live");
      if(searchHint) searchHint.textContent = "Совпадений нет.";
      if(searchSummary && !runSummary){
        searchSummary.style.display = "none";
        searchSummary.innerHTML = "";
      }
      return;
    }

    const dmap = bfsDepth(matches, depth);
    applySelection(matches, dmap, "depth");

    const rows = buildContextFromSelection(dmap);
    const ctxRows = rows.slice(0, MAX_CTX);
    window.__kg_graph_context = ctxRows.join("\n");
    updateChunksBadge(rows.length, depth);

    if(searchHint){
      searchHint.textContent = `Совпадений: ${matches.length}. Глубина ${depth}. `
        + `Найдено ${rows.length} чанков, в модель уходит ${ctxRows.length}/${MAX_CTX}.`;
    }

    // Гибридный поиск / summary всегда идёт на сервер по лупе/Enter,
    // даже если локальных совпадений подсветки нет — см. runHybridKg().
  }

  function applyRealtimeFilter(){
    const q = (searchQ?.value || "").trim();
    if(!q || q.length < 2){
      lastMatches = [];
      updateChunksBadge(0, depthEl?.value || 0);
      if(searchHint) searchHint.textContent = "Подсветка работает при вводе. Глубина применяется в реальном времени при движении ползунка.";
      resetVisual();
      return;
    }
    const matches = findMatches(q);
    lastMatches = matches;
    applyDepthRealtime(false);
  }

  let jellyTimer = null;
  function enableJelly(ms){
    if(!network) return;
    network.setOptions({
      physics: {
        enabled: true,
        stabilization: false,
        barnesHut: {gravitationalConstant: -17000, springLength: 130, springConstant: 0.045, damping: 0.18},
      }
    });
    if(jellyTimer) clearTimeout(jellyTimer);
    jellyTimer = setTimeout(()=>{
      if(network) network.setOptions({physics: {enabled: false}});
      jellyTimer = null;
    }, ms || 900);
  }

  function syncGraph(payload){
    const inNodes = payload.nodes || [];
    const inEdges = payload.edges || [];

    for(const n of inNodes){
      nodeRawMap.set(n.id, n);
      baseKind.set(n.id, n.kind || "knowledge");
    }
    for(const e of inEdges){
      edgeRawMap.set(e.id, e);
    }

    if(!network){
      const visNodes = inNodes.map(toVisNode);
      const visEdges = inEdges.map(toVisEdge);
      nodesDS = new vis.DataSet(visNodes);
      edgesDS = new vis.DataSet(visEdges);
      knownNodeIds = new Set(visNodes.map(n=>n.id));
      knownEdgeIds = new Set(visEdges.map(e=>e.id));
      baseLabels = new Map(visNodes.map(n=>[n.id, n.label]));
      rebuildAdjacency();

      const data = {nodes: nodesDS, edges: edgesDS};
      const options = {
        autoResize: true,
        interaction: {
          hover: true,
          dragView: true,
          zoomView: true,
          navigationButtons: true,
        },
        physics: {
          enabled: true,
          stabilization: true,
          barnesHut: {gravitationalConstant: -20000, springLength: 120, springConstant: 0.04, damping: 0.2},
        },
        nodes: {borderWidth: 2},
        edges: {width: 1.2},
      };

      network = new vis.Network(graphEl, data, options);
      network.on("doubleClick", (params)=>{
        if(params.nodes && params.nodes[0]) openCard(params.nodes[0]);
      });
      network.on("click", (params)=>{
        if(params.nodes && params.nodes[0]) openCard(params.nodes[0]);
      });
      network.on("dragStart", (params)=>{
        if(params && params.nodes && params.nodes.length) enableJelly(1200);
      });
      network.on("dragEnd", (params)=>{
        if(params && params.nodes && params.nodes.length) enableJelly(900);
      });
      network.once("stabilizationIterationsDone", ()=>{
        network.setOptions({physics: {enabled: false}});
      });
      updateGraphInfo(payload);
      return true;
    }

    const addNodes = [];
    for(const n of inNodes){
      if(!knownNodeIds.has(n.id)) addNodes.push(toVisNode(n));
    }

    const addEdges = [];
    for(const e of inEdges){
      if(!knownEdgeIds.has(e.id)) addEdges.push(toVisEdge(e));
    }

    if(addNodes.length === 0 && addEdges.length === 0){
      updateGraphInfo(payload);
      return false;
    }

    if(addNodes.length){
      nodesDS.add(addNodes);
      for(const n of addNodes){
        knownNodeIds.add(n.id);
        baseLabels.set(n.id, n.label);
      }
    }
    if(addEdges.length){
      edgesDS.add(addEdges);
      for(const e of addEdges) knownEdgeIds.add(e.id);
    }

    rebuildAdjacency();
    enableJelly(1000);
    updateGraphInfo(payload);
    return true;
  }

  async function refreshGraph(){
    try{
      const d = await fetchJson("/api/graph?limit_nodes=450&limit_wiki=120");
      if(!d.ok) return;
      syncGraph(d);
      if(!(reapplyHybridState())){
        if(searchQ && String(searchQ.value||"").trim().length>=2){
          applyRealtimeFilter();
        }
      }
    }catch(_){ }
  }

  async function hardRefreshGraph(){
    try{
      if(btnRefresh) btnRefresh.disabled = true;
      const d = await fetchJson("/api/graph?limit_nodes=450&limit_wiki=120");
      if(!d.ok){ toast("Не удалось обновить граф"); return; }
      if(!network){
        syncGraph(d);
      }else{
        const visNodes = (d.nodes||[]).map(toVisNode);
        const visEdges = (d.edges||[]).map(toVisEdge);
        nodesDS = new vis.DataSet(visNodes);
        edgesDS = new vis.DataSet(visEdges);
        knownNodeIds = new Set(visNodes.map(n=>n.id));
        knownEdgeIds = new Set(visEdges.map(e=>e.id));
        baseLabels = new Map(visNodes.map(n=>[n.id, n.label]));
        nodeRawMap = new Map((d.nodes||[]).map(n=>[n.id, n]));
        edgeRawMap = new Map((d.edges||[]).map(e=>[e.id, e]));
        baseKind = new Map((d.nodes||[]).map(n=>[n.id, n.kind || "knowledge"]));
        rebuildAdjacency();
        network.setData({nodes: nodesDS, edges: edgesDS});
        enableJelly(1100);
        updateGraphInfo(d);
      }
      if(!(reapplyHybridState())){
        if(searchQ && String(searchQ.value||"").trim().length>=2){
          applyRealtimeFilter();
        }else{
          resetVisual();
        }
      }
      toast("Граф обновлён");
    }catch(_){
      toast("Ошибка обновления графа");
    }finally{
      if(btnRefresh) btnRefresh.disabled = false;
    }
  }

  if(btnRefresh){
    btnRefresh.onclick = hardRefreshGraph;
  }

  if(btnSearchToggle && searchBox){
    btnSearchToggle.onclick = ()=>{
      const open = searchBox.style.display !== "none";
      searchBox.style.display = open ? "none" : "block";
      btnSearchToggle.textContent = open ? "Поиск по графу ▾" : "Поиск по графу ▴";
    };
  }

  if(depthEl && depthVal){
    const syncDepth = ()=>{
      depthVal.textContent = String(depthEl.value || "2");
      applyDepthRealtime(false);
    };
    depthEl.addEventListener("input", syncDepth);
    syncDepth();
  }

  if(searchQ){
    searchQ.addEventListener("input", applyRealtimeFilter);
    searchQ.addEventListener("keydown", (e)=>{
      if(e.key === "Enter"){
        e.preventDefault();
        runHybridKg((searchQ.value || "").trim());
      }
    });
  }

  if(searchGo){
    searchGo.onclick = ()=> runHybridKg((searchQ?.value || "").trim());
  }

  btnStart.onclick = async ()=>{
    try{
      const st = await fetchJson("/api/kg/run/status");
      if(st.ok && st.running){
        await fetchJson("/api/kg/run/stop", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
        });
        toast("Остановка запрошена");
        await refreshStatus();
        return;
      }

      const d = await fetchJson("/api/kg/run/start", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({limit: 0, batch_size: 1}),
      });
      if(!d.ok && d.status === "busy"){
        toast("Генерация графа уже запущена");
      }else if(d.ok){
        toast("Генерация графа запущена");
      }
      await refreshStatus();
      await refreshGraph();
    }catch(_){ toast("Ошибка управления генерацией графа"); }
  };

  refreshStatus();
  refreshGraph();
  pollTimer = setInterval(refreshStatus, 3000);
  graphTimer = setInterval(refreshGraph, 20000);
})();
