(function(){
  const byId = (id)=>document.getElementById(id);
  const toast = (msg)=>{ if(typeof window.toast === "function") window.toast(msg); };

  const btnStart = byId("btn-kg-start");
  const btnRefresh = byId("btn-kg-refresh");
  const btnExport = byId("btn-kg-export-global");
  const runInfo = byId("kg-run-info");
  const graphInfo = byId("kg-graph-info");
  const graphEl = byId("kg-canvas");

  const btnSearchToggle = byId("btn-kg-search-toggle");
  const searchBox = byId("kg-search-box");
  const btnLoadFull = byId("btn-kg-load-full");
  const searchQ = byId("kg-search-q");
  const searchGo = byId("kg-search-go");
  const searchLlm = byId("kg-search-llm");
  const depthEl = byId("kg-depth");
  const depthVal = byId("kg-depth-val");
  const chunksCount = byId("kg-chunks-count");
  const searchHint = byId("kg-search-hint");
  const searchSummary = byId("kg-search-summary");
  const summaryWrap = byId("kg-summary-wrap");
  const btnSummaryCheck = byId("btn-kg-summary-check");

  if(!btnStart || !graphEl) return;

  let network = null;
  let currentLayoutMode = "force"; // force (дефолт) | hierarchical | radial
  let nodesDS = null;
  let edgesDS = null;
  let hideRelates = false; // скрывать связи relates_to в реальном времени

  // Типы связей, которые считаем "слабыми/типовыми" и прячем по чекбоксу.
  const RELATES_LABELS = new Set(["relates_to", "related_to", "relates", "associated_with"]);
  function isRelatesEdge(edgeId){
    const raw = edgeRawMap.get(edgeId);
    if(!raw) return false;
    const lbl = String(raw.label || raw.relation_type || "").trim().toLowerCase();
    return RELATES_LABELS.has(lbl);
  }

  // Пресеты раскладки. force = текущая боевая физика (эталон), не меняем.
  function getLayoutOptions(mode){
    if(mode === "hierarchical"){
      return {
        layout: {
          hierarchical: {
            enabled: true,
            direction: "LR",
            sortMethod: "hubsize",
            nodeSpacing: 170,
            levelSeparation: 240,
            treeSpacing: 220,
          },
        },
        physics: {enabled: false, stabilization: false},
      };
    }
    if(mode === "radial"){
      return {
        layout: {
          hierarchical: false,
          improvedLayout: true,
          randomSeed: 7,
        },
        physics: {
          enabled: true,
          solver: "forceAtlas2Based",
          stabilization: {enabled: true, iterations: 320, updateInterval: 25},
          forceAtlas2Based: {
            gravitationalConstant: -14,
            centralGravity: 0.16,
            springLength: 165,
            springConstant: 0.09,
            damping: 0.68,
            avoidOverlap: 0.8,
          },
          minVelocity: 0.38,
          timestep: 0.4,
        },
      };
    }
    // force (дефолт, эталонная боевая физика)
    return {
      layout: {hierarchical: false},
      physics: {
        enabled: true,
        stabilization: {enabled: true, iterations: 320, updateInterval: 25},
        barnesHut: {
          gravitationalConstant: -18000,
          centralGravity: 0.2,
          springLength: 260,
          springConstant: 0.035,
          damping: 0.5,
          avoidOverlap: 0.78
        },
        minVelocity: 0.45,
      },
    };
  }
  let knownNodeIds = new Set();
  let knownEdgeIds = new Set();
  let pollTimer = null;
  let graphTimer = null;
  let lastRunning = null;
  let currentPollMs = 0;

  let nodeRawMap = new Map(); // id -> raw node payload
  let edgeRawMap = new Map(); // id -> raw edge payload
  let adjacency = new Map();  // id -> Set(neighbor ids)
  let baseLabels = new Map(); // id -> original vis label
  let baseKind = new Map();   // id -> knowledge/wiki

  let lastMatches = [];
  let lastMatchesQuery = "";
  let quickSearchTimer = null;
  let quickSearchSeq = 0;
  let lastSelection = new Set();

  const MAX_CTX = 100; // лимит чанков, реально уходящих в модель
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
    // По просьбе: после F5 не восстанавливаем прошлый поиск,
    // стартуем в чистом состоянии.
    clearHybridState();
    if(searchQ) searchQ.value = "";
    if(searchLlm) searchLlm.checked = false;
    if(depthEl) depthEl.value = "1";
    if(depthVal) depthVal.textContent = "1";
    setSummaryButtonEnabled(false);
    updateSummaryToggleMeta(0,0,MAX_CTX);
    setSummaryHtml("");
  }

  function applyDepthFromHybridState(){
    if(!lastHybridState) return false;
    const qNow = String(searchQ?.value || "").trim();
    if(!qNow || qNow !== String(lastHybridState.q || "")) return false;

    const depth = Math.max(0, Math.min(5, Number(depthEl?.value || lastHybridState.depth || 1)));
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

    // Пересчёт только визуальной глубины: найдено/used остаются от последнего гибридного запроса.
    if(chunksCount) chunksCount.textContent = `найдено ${found} · в модель ${used}/${maxCtx} · depth ${depth}`;
    updateSummaryToggleMeta(found, used, maxCtx);
    setSummaryButtonEnabled(!!lastHybridState.want_summary);
    if(searchHint){
      searchHint.textContent = found
        ? `Гибридный поиск: найдено ${found}, в модель ${used}/${maxCtx}. На графе подсвечено ${presentIds.length}.`
        : `Гибридный поиск: совпадений в базе нет.`;
    }

    // Обновим только depth в состоянии, чтобы не терять поиск при обновлениях графа.
    saveHybridState({...lastHybridState, depth, ts: Date.now()});
    return true;
  }

  function reapplyHybridState(){
    if(!lastHybridState) return false;
    const qNow = String(searchQ?.value || "").trim();
    if(!qNow || qNow !== String(lastHybridState.q || "")) return false;

    const depth = Math.max(0, Math.min(5, Number(depthEl?.value || lastHybridState.depth || 1)));
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
    updateSummaryToggleMeta(found, used, maxCtx);
    setSummaryButtonEnabled(!!lastHybridState.want_summary);
    if(searchHint){
      searchHint.textContent = found
        ? `Гибридный поиск: найдено ${found}, в модель ${used}/${maxCtx}. На графе подсвечено ${presentIds.length}.`
        : `Гибридный поиск: совпадений в базе нет.`;
    }

    const wantSummary = !!lastHybridState.want_summary;
    setSummaryButtonEnabled(wantSummary);
    if(!wantSummary){
      setSummaryHtml("");
    }else if(lastHybridState.summary){
      const meta = `найдено ${found} · в модель ${used}/${maxCtx}`;
      setSummaryHtml(renderSummaryHtml(lastHybridState.summary, (lastHybridState.items || []), meta));
      wireSummaryRefLinks();
      if(btnSummaryCheck) btnSummaryCheck.classList.add("open");
      applySummaryPanelVisibility();
    }else if(lastHybridState.summary_error){
      setSummaryHtml(`<span class="mono" style="font-size:11px;color:#9a8f82">Не удалось: ${esc(lastHybridState.summary_error)}</span>`);
    }else if(found === 0){
      setSummaryHtml(`<span class="mono" style="font-size:11px;color:#9a8f82">Совпадений в базе нет — саммари не по чему строить.</span>`);
    }

    return true;
  }

  // Совместимость со старыми вызовами — метка убрана из UI, ничего не делаем.
  function updateSummaryToggleMeta(){ }

  // Совместимость: включение/выключение больше не управляет отдельной кнопкой
  // на панели, а прячет/показывает всю область саммари.
  function setSummaryButtonEnabled(enabled){
    if(!enabled) hideSummaryArea();
  }

  function hideSummaryArea(){
    if(searchSummary) searchSummary.innerHTML = "";
    if(summaryWrap) summaryWrap.style.display = "none";
  }

  function applySummaryPanelVisibility(){
    if(!summaryWrap || !btnSummaryCheck) return;
    const open = btnSummaryCheck.classList.contains("open");
    summaryWrap.classList.toggle("collapsed", !open);
    btnSummaryCheck.title = open ? "Свернуть саммари" : "Развернуть саммари";
  }

  // Показывает область саммари (кнопка сворачивания появляется только тут).
  function setSummaryHtml(html){
    if(!searchSummary) return;
    if(!html){ hideSummaryArea(); return; }
    searchSummary.innerHTML = html;
    if(summaryWrap) summaryWrap.style.display = "block";
    // При новом контенте раскрываем область по умолчанию.
    if(btnSummaryCheck) btnSummaryCheck.classList.add("open");
    applySummaryPanelVisibility();
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

  async function downloadExportZip(mode){
    const r = await fetch(`/api/local-pipeline/export?mode=${encodeURIComponent(mode || "sqlite_csv")}`);
    if(!r.ok){
      const t = await r.text().catch(()=>"");
      throw new Error(t || `http_${r.status}`);
    }
    const blob = await r.blob();
    let fileName = `local_pipeline_${mode || "sqlite_csv"}.zip`;
    const cd = r.headers.get("content-disposition") || "";
    const m = cd.match(/filename="?([^";]+)"?/i);
    if(m && m[1]) fileName = m[1];
    const u = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = u;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(()=>URL.revokeObjectURL(u), 1200);
  }

  async function refreshStatus(){
    try{
      const d = await fetchJson("/api/kg/run/status");
      if(d.ok){
        patchBtn(d);
        if(lastRunning === true && !d.running){
          // Прогон только что завершился — граф изменился.
          // Автоотрисовку не запускаем: граф строится только по кнопке
          // «Показать граф». Подсказываем пользователю, что данные обновились.
          toast("Граф обновлён на бэке — нажмите «Показать граф»");
        }
        lastRunning = !!d.running;
        // Адаптивный поллинг: часто во время прогона, редко в покое.
        applyStatusPollInterval(!!d.running);
      }
    }catch(_){ }
  }

  // Управление частотой опроса статуса. Во время прогона — 3 сек,
  // в покое — 20 сек, чтобы не засыпать бэк лишними GET /api/kg/run/status.
  function applyStatusPollInterval(running){
    const want = running ? 3000 : 20000;
    if(want === currentPollMs && pollTimer) return;
    currentPollMs = want;
    if(pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(refreshStatus, currentPollMs);
  }

  function mdToHtml(md){
    if(!md) return "";
    return esc(md)
      .replace(/\n\n+/g, "</p><p>")
      .replace(/\n/g, "<br>")
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      .replace(/`(.+?)`/g, "<code>$1</code>");
  }

  function wikiMdToHtml(md){
    const raw = String(md || "").replace(/\r\n?/g, "\n");
    if(!raw.trim()) return "";

    const codeBlocks = [];
    let text = raw.replace(/```([\s\S]*?)```/g, (_m, code)=>{
      const i = codeBlocks.push(`<pre><code>${esc(String(code || "").trim())}</code></pre>`) - 1;
      return `@@CODEBLOCK_${i}@@`;
    });

    const lines = text.split("\n");
    let out = "";
    let inList = false;

    const flushList = ()=>{
      if(inList){ out += "</ul>"; inList = false; }
    };

    const inline = (t)=>{
      const e = esc(t || "");
      return e
        .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
        .replace(/\*(.+?)\*/g, "<em>$1</em>")
        .replace(/`(.+?)`/g, "<code>$1</code>")
        .replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
    };

    for(const line of lines){
      const t = line.trim();
      if(!t){ flushList(); continue; }

      if(/^@@CODEBLOCK_\d+@@$/.test(t)){
        flushList();
        out += t;
        continue;
      }

      const hm = t.match(/^(#{1,6})\s+(.+)$/);
      if(hm){
        flushList();
        const lvl = Math.min(6, hm[1].length);
        out += `<h${lvl}>${inline(hm[2])}</h${lvl}>`;
        continue;
      }

      const lm = t.match(/^[-*+]\s+(.+)$/);
      if(lm){
        if(!inList){ out += "<ul>"; inList = true; }
        out += `<li>${inline(lm[1])}</li>`;
        continue;
      }

      flushList();
      out += `<p>${inline(t)}</p>`;
    }
    flushList();

    out = out.replace(/@@CODEBLOCK_(\d+)@@/g, (_m, i)=> codeBlocks[Number(i)] || "");
    return out;
  }

  function domainFromUrl(url){
    try{
      const h = new URL(String(url || "")).hostname || "";
      return h.replace(/^www\./i, "");
    }catch(_){
      return "";
    }
  }

  function renderSummaryHtml(summary, items, meta){
    const md = String(summary || "");
    let html = mdToHtml(md);
    const refs = [];
    const refGroupRe = /\[((?:\d+\s*(?:,\s*\d+\s*)*))\]/g;
    let m;
    while((m = refGroupRe.exec(md)) !== null){
      const nums = String(m[1] || "")
        .split(",")
        .map(s => Number(String(s || "").trim()))
        .filter(n => Number.isFinite(n));
      for(const idx of nums){
        if(idx >= 1 && idx <= (items?.length || 0) && !refs.includes(idx)) refs.push(idx);
      }
    }

    const title = `<div class="mono" style="font-size:12px;color:#6b6055;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px">LLM-саммари · ${esc(meta || "")}</div>`;

    const mkRef = (idx)=>{
      if(!Number.isFinite(idx) || idx < 1 || idx > (items?.length || 0)) return `[${esc(String(idx))}]`;
      const it = items[idx - 1] || {};
      const nodeId = String(it.node_id || "");
      const kind = it.kind === "wiki" ? "wiki" : "knowledge";
      if(!nodeId) return `[${idx}]`;
      return `<a href="#" class="kg-ref-link" data-ref-index="${idx}" data-node-id="${esc(nodeId)}" data-kind="${kind}">[${idx}]</a>`;
    };

    // Внедряем ссылки прямо в текст саммари:
    // [n] и составные [n, m] -> кликабельные [n], [m]
    html = html.replace(/\[((?:\d+\s*(?:,\s*\d+\s*)*))\]/g, (_all, body)=>{
      const nums = String(body || "")
        .split(",")
        .map(s => Number(String(s || "").trim()))
        .filter(n => Number.isFinite(n));
      if(!nums.length) return _all;
      return nums.map((n)=>mkRef(n)).join(", ");
    });

    // Нижний блок "Источники" убираем: ссылки остаются только внутри текста LLM.
    return `${title}${html}`;
  }

  function wireSummaryRefLinks(){
    if(!searchSummary) return;
    searchSummary.querySelectorAll(".kg-ref-link").forEach((a)=>{
      a.addEventListener("click", (e)=>{
        e.preventDefault();
        const nodeId = a.getAttribute("data-node-id") || "";
        if(!nodeId) return;

        // Ultra-fast режим для больших графов: без долгой анимации камеры,
        // чтобы убрать лаг при переходе по ссылке-источнику из summary.
        const bigGraph = nodeCount() >= 100;

        // Скролл к графу без smooth (минимум конкурирующих анимаций).
        try{ graphEl?.scrollIntoView({block:"center"}); }catch(_){ }

        try{
          if(jellyTimer){ clearTimeout(jellyTimer); jellyTimer = null; }
          network?.setOptions({physics: {enabled: false}});

          if(bigGraph){
            const pos = network?.getPositions?.([nodeId])?.[nodeId];
            if(pos){
              // Для 100+ узлов: мгновенное центрирование, без zoom-анимации.
              network?.moveTo({
                position: {x: pos.x, y: pos.y},
                scale: 1.06,
                animation: false,
              });
            }else{
              network?.focus(nodeId, {
                scale: 1.06,
                animation: false,
              });
            }
          }else{
            network?.focus(nodeId, {
              scale: 1.12,
              animation: {duration: 260, easingFunction: "easeInOutQuad"}
            });
          }

          nodesDS?.update([{id: nodeId, borderWidth: 4}]);
          setTimeout(()=>{
            try{ nodesDS?.update([{id: nodeId, borderWidth: 2}]); }catch(_){ }
          }, bigGraph ? 900 : 1200);
        }catch(_){ }

        // Карточку открываем практически сразу: камера уже на месте
        // (для bigGraph — мгновенно, для small — короткая анимация).
        setTimeout(()=> openCard(nodeId), bigGraph ? 120 : 320);
      });
    });
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
      byId("kg-card-body").innerHTML = (d.kind === "wiki") ? wikiMdToHtml(d.text||"") : `<p>${mdToHtml(d.text||"")}</p>`;
      const foot = byId("kg-card-foot");
      foot.innerHTML = "";

      const sourceUrl = (
        (typeof d.source_url === "string" && /^https?:\/\//i.test(d.source_url.trim()) && d.source_url.trim())
        || ""
      );

      if(d.kind === "wiki" && sourceUrl){
        const a = document.createElement("a");
        a.className = "btn";
        a.href = sourceUrl;
        a.target = "_blank";
        a.rel = "noopener noreferrer";
        a.textContent = "открыть источник";
        foot.appendChild(a);

        const host = domainFromUrl(sourceUrl);
        if(host){
          const hint = document.createElement("span");
          hint.className = "mono";
          hint.style.fontSize = "11px";
          hint.style.opacity = "0.75";
          hint.style.marginLeft = "8px";
          hint.textContent = `(${host})`;
          foot.appendChild(hint);
        }
      }

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

  // SVG-домик Tabler (filled home) как data-URI для формы хаба shape:"image".
  // Цвет задаётся в самом SVG (fill). Иконка на белой «плашке» с обводкой.
  function hubHouseImage(){
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="44" height="44" viewBox="0 0 24 24">`
      + `<rect x="0.5" y="0.5" width="23" height="23" rx="4" fill="#FFD9C7" stroke="#1E1A16" stroke-width="1.4"/>`
      + `<g transform="translate(2.4 2.4) scale(0.8)" fill="#1E1A16">`
      + `<path d="M12.707 2.293l9 9c.63 .63 .184 1.707 -.707 1.707h-1v6a3 3 0 0 1 -3 3h-1v-7a3 3 0 0 0 -2.824 -2.995l-.176 -.005h-2a3 3 0 0 0 -3 3v7h-1a3 3 0 0 1 -3 -3v-6h-1c-.89 0 -1.337 -1.077 -.707 -1.707l9 -9a1 1 0 0 1 1.414 0m.293 11.707a1 1 0 0 1 1 1v7h-4v-7a1 1 0 0 1 .883 -.993l.117 -.007z"/>`
      + `</g></svg>`;
    return "data:image/svg+xml;charset=utf-8," + encodeURIComponent(svg);
  }
  const HUB_HOUSE_IMG = hubHouseImage();

  function toVisNode(n){
    const isWiki = n.kind === "wiki";
    return {
      id: n.id,
      // Текст с обычных узлов убран для скорости отрисовки; название доступно в tooltip.
      label: undefined,
      title: esc(n.title||n.label||""),
      shape: isWiki ? "star" : "dot",
      size: isWiki ? 20 : (10 + Math.round(Number(n.importance||0)*14)),
      color: isWiki
        ? {background: "#E3D5F5", border: "#1E1A16"}
        : {background: "#CDE8D5", border: "#1E1A16"},
      font: {face: "Space Mono", size: 11, color: "#1E1A16"},
    };
  }

  // Порог, с которого узел считается хабом и рисуется «домиком».
  const HUB_DEGREE = 20;

  // Проставляет узлам-хабам (степень >= HUB_DEGREE) форму домика (vis shape "triangle"
  // + увеличенный размер + акцентный цвет). Вызывать ПОСЛЕ rebuildAdjacency,
  // чтобы степени уже были посчитаны. Узлам ниже порога возвращает базовый вид.
  function applyHubShapes(){
    if(!nodesDS) return;
    const upd = [];
    for(const n of nodesDS.get()){
      const deg = (adjacency.get(n.id) || new Set()).size;
      const kind = baseKind.get(n.id) || "knowledge";
      const isWiki = kind === "wiki";
      if(deg >= HUB_DEGREE){
        upd.push({
          id: n.id,
          shape: "image",
          image: HUB_HOUSE_IMG,
          size: 22 + Math.min(18, deg - HUB_DEGREE),
          label: undefined,
          borderWidth: 0,
          title: esc((n.title || n.label || "") + "  ·  хаб (связей: " + deg + ")"),
        });
      }else{
        // вернуть базовый вид (на случай, если раньше был хабом); текст скрыт
        upd.push({
          id: n.id,
          shape: isWiki ? "star" : "dot",
          label: undefined,
          borderWidth: 2,
          color: isWiki
            ? {background: "#E3D5F5", border: "#1E1A16"}
            : {background: "#CDE8D5", border: "#1E1A16"},
        });
      }
    }
    if(upd.length) nodesDS.update(upd);
  }

  // Управление лоадером-заставкой поверх канваса.
  function showGraphLoader(on){
    const el = byId("kg-loader");
    if(!el) return;
    if(on) el.classList.add("on");
    else el.classList.remove("on");
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
      // При активном фильтре relates_to не учитываем такие рёбра в связности,
      // чтобы глубина связей (bfsDepth) не проходила через скрытые связи.
      if(hideRelates && isRelatesEdge(e.id)) continue;
      if(!adjacency.has(e.from)) adjacency.set(e.from, new Set());
      if(!adjacency.has(e.to)) adjacency.set(e.to, new Set());
      adjacency.get(e.from).add(e.to);
      adjacency.get(e.to).add(e.from);
    }
  }

  // Прячет/показывает рёбра relates_to в реальном времени и пересобирает
  // связность, затем переприменяет текущую подсветку/глубину.
  function applyRelatesVisibility(){
    if(!edgesDS) return;
    const eUpd = [];
    for(const e of edgesDS.get()){
      if(isRelatesEdge(e.id)){
        eUpd.push({id: e.id, hidden: hideRelates});
      }
    }
    if(eUpd.length) edgesDS.update(eUpd);
    rebuildAdjacency();
    // Переприменяем текущее состояние: если активен поиск/глубина — пересчитать,
    // иначе просто сброс визуала (скрытые рёбра останутся hidden).
    const q = (searchQ?.value || "").trim();
    if(q && q.length >= 2){
      applyDepthRealtime(false);
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
        label: undefined,
        hidden: false,
        opacity: 1,
        color: isWiki ? {background:"#E3D5F5", border:"#1E1A16"} : {background:"#CDE8D5", border:"#1E1A16"},
        font: {color:"#1E1A16", size: 11, face:"Space Mono"},
      });
    }
    const eUpd = edgesDS.get().map(e=>({
      id: e.id,
      hidden: (hideRelates && isRelatesEdge(e.id)) ? true : false,
      color: {color: (edgeRawMap.get(e.id)?.kind === "wiki_link" ? "#7b6f8f" : "#FF5A1F")},
      width: 1.2,
    }));
    nodesDS.update(nUpd);
    edgesDS.update(eUpd);
  }

  async function quickSearchServer(q){
    const text = String(q||"").trim();
    if(text.length < 2) return [];
    try{
      const d = await fetchJson(`/api/kg/search/quick?q=${encodeURIComponent(text)}&limit=150`);
      if(!d || !d.ok || !Array.isArray(d.items)) return [];
      return d.items.map(it => it && it.node_id).filter(Boolean);
    }catch(_){
      return [];
    }
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
          label: undefined,
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
        nUpd.push({
          id: n.id,
          label: undefined,
          hidden: false,
          opacity: 1,
          color: {background:bg, border:"#1E1A16"},
          font: {color: lvl===0 ? "#fff" : "#1E1A16", size: 11, face:"Space Mono"},
        });
      }else{
        nUpd.push({
          id: n.id,
          label: undefined,
          hidden: false,
          opacity: 0.18,
          color: {background:"#F1EEE9", border:"#C9BEAC"},
          font: {color:"#B0A69A", size: 10, face:"Space Mono"},
        });
      }
    }

    const eUpd = [];
    for(const e of edgesDS.get()){
      const relHidden = hideRelates && isRelatesEdge(e.id);
      const on = selected.has(e.from) && selected.has(e.to);
      if(relHidden){
        eUpd.push({id: e.id, hidden: true});
      }else if(selected.size === 0){
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
    const depth = Math.max(0, Math.min(5, Number(depthEl?.value || 1)));
    const wantSummary = !!(searchLlm && searchLlm.checked);

    if(wantSummary){
      setSummaryButtonEnabled(true);
      if(btnSummaryCheck) btnSummaryCheck.classList.add("open");
      setSummaryHtml(`<span class="mono" style="font-size:11px;color:#6b6055">Гибридный поиск по графу…</span>`);
    }else{
      setSummaryButtonEnabled(false);
      setSummaryHtml("");
    }

    let d;
    try{
      d = await fetchJson("/api/kg/search", {
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body: JSON.stringify({q, limit: 120, max_ctx: MAX_CTX, llm_summary: wantSummary})
      });
    }catch(_){
      if(wantSummary) setSummaryHtml(`<span class="mono" style="font-size:11px;color:#9a8f82">Ошибка сети при поиске.</span>`);
      return;
    }

    if(!d || !d.ok){
      if(wantSummary) setSummaryHtml(`<span class="mono" style="font-size:11px;color:#9a8f82">Ошибка поиска: ${esc((d&&d.error)||"unknown")}</span>`);
      return;
    }

    const found = Number(d.found || 0);
    const used = Number(d.used || 0);
    const maxCtx = Number(d.max_ctx || MAX_CTX);

    // Обновляем бейдж и подсказку по РЕАЛЬНОМУ серверному результату.
    if(chunksCount) chunksCount.textContent = `найдено ${found} · в модель ${used}/${maxCtx} · depth ${depth}`;
    updateSummaryToggleMeta(found, used, maxCtx);
    setSummaryButtonEnabled(wantSummary);

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
      items: d.items || [],
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
    if(!wantSummary){
      setSummaryHtml("");
      return;
    }
    if(d.summary){
      const meta = `найдено ${found} · в модель ${used}/${maxCtx}`;
      setSummaryHtml(renderSummaryHtml(d.summary, (d.items || []), meta));
      wireSummaryRefLinks();
    }else if(d.summary_error){
      setSummaryHtml(`<span class="mono" style="font-size:11px;color:#9a8f82">Не удалось: ${esc(d.summary_error)}</span>`);
    }else if(found === 0){
      setSummaryHtml(`<span class="mono" style="font-size:11px;color:#9a8f82">Совпадений в базе нет — саммари не по чему строить.</span>`);
    }else{
      setSummaryHtml(`<span class="mono" style="font-size:11px;color:#9a8f82">Саммари не вернулось.</span>`);
    }
  }

  function updateChunksBadge(n, depth){
    if(!chunksCount) return;
    const num = Number(n||0);
    const d = Number(depth||depthEl?.value||0);
    const used = Math.min(num, MAX_CTX);
    chunksCount.textContent = `найдено ${num} · в модель ${used}/${MAX_CTX} · depth ${d}`;
    updateSummaryToggleMeta(num, used, MAX_CTX);
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
        setSummaryHtml("");
      }
      setSummaryButtonEnabled(false);
      return;
    }

    // Если есть результаты гибридного поиска по этому же запросу —
    // не сбрасываем их при движении ползунка, а только пересчитываем глубину.
    if(lastHybridState && String(lastHybridState.q || "") === q){
      if(applyDepthFromHybridState()) return;
    }

    const depth = Math.max(0, Math.min(5, Number(depthEl?.value || 1)));
    let matches = [];
    if(lastMatchesQuery === q && Array.isArray(lastMatches) && lastMatches.length){
      matches = lastMatches;
    }else{
      matches = findMatches(q);
      lastMatches = matches;
      lastMatchesQuery = q;
    }

    if(!matches.length){
      updateChunksBadge(0, depth);
      window.__kg_graph_context = "";
      applySelection([], null, "live");
      if(searchHint) searchHint.textContent = "Совпадений нет.";
      if(searchSummary && !runSummary){
        setSummaryHtml("");
      }
      setSummaryButtonEnabled(false);
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
      if(quickSearchTimer){ clearTimeout(quickSearchTimer); quickSearchTimer = null; }
      lastMatches = [];
      lastMatchesQuery = "";
      updateChunksBadge(0, depthEl?.value || 0);
      if(searchHint) searchHint.textContent = "Подсветка работает при вводе. Глубина применяется в реальном времени при движении ползунка.";
      resetVisual();
      return;
    }

    if(quickSearchTimer){ clearTimeout(quickSearchTimer); quickSearchTimer = null; }
    const reqId = ++quickSearchSeq;
    quickSearchTimer = setTimeout(async ()=>{
      const currentQ = (searchQ?.value || "").trim();
      if(!currentQ || currentQ.length < 2) return;
      const serverMatches = await quickSearchServer(currentQ);
      if(reqId !== quickSearchSeq) return; // устаревший ответ
      lastMatches = serverMatches;
      lastMatchesQuery = currentQ;
      applyDepthRealtime(false);
    }, 300);
  }

  // Применить выбранный layout к уже загруженным данным графа.
  // Важно: вызывается ТОЛЬКО при ручной перерисовке (без realtime на change).
  function applySelectedLayoutRender(iterations){
    if(!network) return;

    if(jellyTimer){ clearTimeout(jellyTimer); jellyTimer = null; }
    if(dragLocalActive){
      try{ endLocalDrag(); }catch(_){ }
    }

    try{
      network.setOptions({
        layout: {hierarchical: false},
        physics: {enabled: false, stabilization: false}
      });
    }catch(_){ }

    const mode = currentLayoutMode || "force";
    const opts = getLayoutOptions(mode);
    try{ network.setOptions(opts); }catch(_){ }

    if(mode === "hierarchical"){
      setTimeout(()=>{
        try{ network.fit({animation:{duration:360, easingFunction:"easeInOutQuad"}}); }catch(_){ }
        try{ network.setOptions({physics:{enabled:false}}); }catch(_){ }
        applyHubShapes();
        showGraphLoader(false);
      }, 40);
      return;
    }

    const iters = Number(iterations || (mode === "radial" ? 320 : 300));
    network.once("stabilizationIterationsDone", ()=>{
      try{ network.setOptions({physics:{enabled:false}}); }catch(_){ }
      applyHubShapes();
      showGraphLoader(false);
    });
    try{
      network.stabilize(iters);
    }catch(_){
      setTimeout(()=>{
        try{ network.setOptions({physics:{enabled:false}}); }catch(_){ }
        applyHubShapes();
        showGraphLoader(false);
      }, 120);
    }
  }

  // Выбор режима раскладки БЕЗ ререндера в реальном времени.
  // Применение — только по ручному обновлению графа.
  function applyLayoutMode(mode){
    currentLayoutMode = mode || "force";
    if(searchHint){
      searchHint.textContent = `Режим раскладки: ${currentLayoutMode}. Нажмите «Обновить граф», чтобы применить.`;
    }
    toast("Режим раскладки сохранён. Нажмите «Обновить граф».");
  }

  let jellyTimer = null;
  function enableJelly(ms){
    if(!network) return;
    network.setOptions({
      physics: {
        enabled: true,
        stabilization: false,
        barnesHut: {
          gravitationalConstant: -4500,
          centralGravity: 0.2,
          springLength: 230,
          springConstant: 0.03,
          damping: 0.68
        },
      }
    });
    if(jellyTimer) clearTimeout(jellyTimer);
    jellyTimer = setTimeout(()=>{
      if(network) network.setOptions({physics: {enabled: false}});
      jellyTimer = null;
    }, ms || 900);
  }

  // Порог, после которого «глобальное желе» на перетаскивании отключается,
  // и включается локальный режим: двигается только узел + соседи depth<=2.
  const JELLY_NODE_LIMIT = 100;
  const DRAG_LOCAL_DEPTH = 2;
  const DRAG_MAX_LOCAL_NODES = 36; // жёстный лимит подвижных узлов для FPS
  let dragLocalActive = false;
  let dragFrozenIds = null; // Set замороженных узлов, которым вернём fixed=false

  function nodeCount(){
    try{ return knownNodeIds ? knownNodeIds.size : 0; }catch(_){ return 0; }
  }

  function trimLocalSet(localMap, maxNodes){
    const arr = Array.from(localMap.entries()); // [id, depth]
    if(arr.length <= maxNodes) return new Set(arr.map(([id])=>id));
    // приоритет: меньшая глубина, затем меньшая степень (меньше "взрывает" физику)
    arr.sort((a,b)=>{
      const d = (a[1]||0) - (b[1]||0);
      if(d) return d;
      const da = (adjacency.get(a[0]) || new Set()).size;
      const db = (adjacency.get(b[0]) || new Set()).size;
      return da - db;
    });
    return new Set(arr.slice(0, maxNodes).map(([id])=>id));
  }

  // Локальное перетаскивание: физика только для таскаемого узла и его
  // соседей до глубины DRAG_LOCAL_DEPTH; остальные узлы фиксируются на месте,
  // чтобы весь граф не «плыл желе».
  function startLocalDrag(dragIds){
    if(!network || !nodesDS) return;

    const localRaw = bfsDepth(dragIds, DRAG_LOCAL_DEPTH); // Map id->depth
    const localSet = trimLocalSet(localRaw, DRAG_MAX_LOCAL_NODES);

    const freezeUpd = [];
    const localUpd = [];
    dragFrozenIds = new Set();

    for(const id of knownNodeIds){
      if(localSet.has(id)){
        localUpd.push({id, fixed: {x: false, y: false}});
      }else{
        // Без x/y и physics:false: меньше payload в DataSet.update,
        // меньше лаг на больших графах.
        freezeUpd.push({id, fixed: {x: true, y: true}});
        dragFrozenIds.add(id);
      }
    }

    if(freezeUpd.length) nodesDS.update(freezeUpd);
    if(localUpd.length) nodesDS.update(localUpd);

    network.setOptions({
      interaction: {hover: false, hideEdgesOnDrag: true},
      physics: {
        enabled: true,
        stabilization: false,
        barnesHut: {
          gravitationalConstant: -3200,
          springLength: 150,
          springConstant: 0.03,
          damping: 0.72,
        },
        minVelocity: 0.4,
      }
    });

    dragLocalActive = true;
  }

  function endLocalDrag(){
    if(!network || !nodesDS){ dragLocalActive = false; dragFrozenIds = null; return; }

    if(dragFrozenIds && dragFrozenIds.size){
      const upd = [];
      for(const id of dragFrozenIds){
        upd.push({id, fixed: {x: false, y: false}});
      }
      if(upd.length) nodesDS.update(upd);
    }

    dragFrozenIds = null;
    dragLocalActive = false;

    if(jellyTimer) clearTimeout(jellyTimer);
    jellyTimer = setTimeout(()=>{
      if(network){
        network.setOptions({
          interaction: {hover: true, hideEdgesOnDrag: true},
          physics: {enabled: false}
        });
      }
      jellyTimer = null;
    }, 260);
  }

  // Единая точка обработки начала drag: для маленьких графов — прежнее
  // «желе», для больших (100+) — локальный режим.
  function handleDragStart(params){
    if(!params || !params.nodes || !params.nodes.length) return;
    if(nodeCount() >= JELLY_NODE_LIMIT){
      startLocalDrag(params.nodes);
    }else{
      enableJelly(1200);
    }
  }

  function handleDragEnd(params){
    if(!params || !params.nodes || !params.nodes.length) return;
    if(dragLocalActive){
      endLocalDrag();
    }else{
      enableJelly(900);
    }
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
      const options = Object.assign({
        autoResize: true,
        interaction: {
          hover: true,
          dragView: true,
          zoomView: true,
          navigationButtons: true,
        },
        nodes: {borderWidth: 2},
        edges: {width: 1.2},
      }, getLayoutOptions(currentLayoutMode));

      network = new vis.Network(graphEl, data, options);
      network.on("doubleClick", (params)=>{
        if(params.nodes && params.nodes[0]) openCard(params.nodes[0]);
      });
      network.on("click", (params)=>{
        if(params.nodes && params.nodes[0]) openCard(params.nodes[0]);
      });
      network.on("dragStart", handleDragStart);
      network.on("dragEnd", handleDragEnd);
      applySelectedLayoutRender(currentLayoutMode === "radial" ? 320 : 300);
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
    applyHubShapes();
    enableJelly(1000);
    updateGraphInfo(payload);
    return true;
  }

  async function refreshGraph(){
    const firstRender = !network;
    try{
      if(firstRender) showGraphLoader(true);
      const d = await fetchJson("/api/graph?limit_nodes=450&limit_wiki=120&lite=1");
      if(!d.ok){ if(firstRender) showGraphLoader(false); return; }
      syncGraph(d);
      if(!(reapplyHybridState())){
        if(searchQ && String(searchQ.value||"").trim().length>=2){
          applyRealtimeFilter();
        }
      }
    }catch(_){ }
  }

  async function loadFullGraph(){
    try{
      if(btnLoadFull) btnLoadFull.disabled = true;
      if(btnLoadFull) btnLoadFull.classList.add("on");
      showGraphLoader(true);
      const d = await fetchJson("/api/graph?limit_nodes=2000&limit_wiki=500&lite=1");
      if(!d.ok){ toast("Не удалось загрузить полный граф"); return; }
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
        applySelectedLayoutRender(currentLayoutMode === "radial" ? 350 : 320);
        updateGraphInfo(d);
      }
      if(!(reapplyHybridState())){
        if(searchQ && String(searchQ.value||"").trim().length>=2){
          applyRealtimeFilter();
        }else{
          resetVisual();
        }
      }
      if(btnLoadFull) btnLoadFull.classList.add("hidden");
      toast("Полный граф загружен");
    }catch(_){
      toast("Ошибка загрузки полного графа");
    }finally{
      if(btnLoadFull) btnLoadFull.disabled = false;
      if(btnLoadFull) btnLoadFull.classList.remove("on");
    }
  }

  async function hardRefreshGraph(){
    try{
      if(btnRefresh) btnRefresh.disabled = true;
      showGraphLoader(true);
      const d = await fetchJson("/api/graph?limit_nodes=450&limit_wiki=120&lite=1");
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
        // Ручной refresh рендерит строго в выбранном режиме раскладки.
        applySelectedLayoutRender(currentLayoutMode === "radial" ? 320 : 300);
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

  const hideRelatesEl = byId("kg-hide-relates");
  if(hideRelatesEl){
    hideRelates = !!hideRelatesEl.checked;
    hideRelatesEl.addEventListener("change", ()=>{
      hideRelates = !!hideRelatesEl.checked;
      applyRelatesVisibility();
    });
  }

  const layoutModeEl = byId("kg-layout-mode");
  if(layoutModeEl){
    layoutModeEl.value = currentLayoutMode;
    layoutModeEl.addEventListener("change", ()=>{
      applyLayoutMode(layoutModeEl.value || "force");
    });
  }
  if(btnLoadFull){
    btnLoadFull.onclick = loadFullGraph;
  }

  if(btnExport){
    btnExport.onclick = async ()=>{
      try{
        btnExport.disabled = true;
        await downloadExportZip("sqlite_csv");
        toast("Выгрузка готова");
      }catch(_){
        try{
          await downloadExportZip("csv_only");
          toast("Выгрузка готова (csv-only)");
        }catch(_e){
          toast("Ошибка выгрузки");
        }
      }finally{
        btnExport.disabled = false;
      }
    };
  }

  if(btnSearchToggle && searchBox){
    btnSearchToggle.onclick = ()=>{
      const open = searchBox.style.display !== "none";
      searchBox.style.display = open ? "none" : "block";
      btnSearchToggle.textContent = open ? "Поиск по графу ▾" : "Поиск по графу ▴";
    };
  }

  if(btnSummaryCheck){
    btnSummaryCheck.onclick = ()=>{
      btnSummaryCheck.classList.toggle("open");
      applySummaryPanelVisibility();
    };
  }
  hideSummaryArea();

  restoreHybridStateFromSession();

  if(depthEl && depthVal){
    const syncDepth = ()=>{
      depthVal.textContent = String(depthEl.value || "1");
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
      // Граф не перерисовываем автоматически — только по кнопке «Показать граф».
    }catch(_){ toast("Ошибка управления генерацией графа"); }
  };

  refreshStatus();
  // Авто-построение графа при загрузке страницы (F5) убрано намеренно:
  // граф строится ТОЛЬКО по кнопке «Показать граф» (loadFullGraph),
  // чтобы не грузить тяжёлый рендер на каждом заходе.
  // Периодический refreshGraph тоже убран; один раз граф подтягивается
  // автоматически по завершении KG-прогона (см. refreshStatus).
  applyStatusPollInterval(false);
})();
