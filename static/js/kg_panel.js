(function(){
  const byId = (id)=>document.getElementById(id);
  const toast = (msg)=>{ if(typeof window.toast === "function") window.toast(msg); };

  const btnStart = byId("btn-kg-start");
  const btnStop = byId("btn-kg-stop");
  const btnRefresh = byId("btn-kg-refresh");
  const runInfo = byId("kg-run-info");
  const graphInfo = byId("kg-graph-info");
  const graphEl = byId("kg-canvas");

  if(!btnStart || !btnStop || !graphEl) return;

  let network = null;
  let nodesDS = null;
  let edgesDS = null;
  let knownNodeIds = new Set();
  let knownEdgeIds = new Set();
  let pollTimer = null;
  let graphTimer = null;
  let lastRunning = null;

  function esc(s){
    return String(s||"").replace(/[&<>\"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));
  }

  function patchBtn(state){
    const running = !!state.running;
    const total = Number(state.total||0);
    const done = Number(state.done||0);
    const rem = Number(state.remaining||0);

    if(running){
      btnStart.textContent = `🕸 граф ${done}/${total}`;
      btnStart.disabled = true;
      btnStop.disabled = false;
      if(runInfo) runInfo.textContent = `в работе: ${done}/${total} · осталось: ${rem}`;
    }else{
      btnStart.textContent = "🕸 создание графа";
      btnStart.disabled = false;
      btnStop.disabled = true;
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
      font: {face: "Space Mono", size: 11},
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
    };
  }

  function updateGraphInfo(payload){
    if(graphInfo && payload.counts){
      graphInfo.textContent = `узлы knowledge: ${payload.counts.knowledge} · wiki: ${payload.counts.wiki_pages} · связей: ${payload.counts.relations}`;
    }
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

    if(!network){
      const visNodes = inNodes.map(toVisNode);
      const visEdges = inEdges.map(toVisEdge);
      nodesDS = new vis.DataSet(visNodes);
      edgesDS = new vis.DataSet(visEdges);
      knownNodeIds = new Set(visNodes.map(n=>n.id));
      knownEdgeIds = new Set(visEdges.map(e=>e.id));

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
      for(const n of addNodes) knownNodeIds.add(n.id);
    }
    if(addEdges.length){
      edgesDS.add(addEdges);
      for(const e of addEdges) knownEdgeIds.add(e.id);
    }

    enableJelly(1000);

    updateGraphInfo(payload);
    return true;
  }


  async function refreshGraph(){
    try{
      const d = await fetchJson("/api/graph?limit_nodes=450&limit_wiki=120");
      if(!d.ok) return;
      syncGraph(d);
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
        network.setData({nodes: nodesDS, edges: edgesDS});
        enableJelly(1100);
        updateGraphInfo(d);
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

  btnStart.onclick = async ()=>{
    try{
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
    }catch(_){ toast("Ошибка запуска генерации графа"); }
  };

  btnStop.onclick = async ()=>{
    try{
      await fetchJson("/api/kg/run/stop", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
      });
      toast("Остановка запрошена");
      await refreshStatus();
    }catch(_){ toast("Ошибка остановки генерации графа"); }
  };

  refreshStatus();
  refreshGraph();
  pollTimer = setInterval(refreshStatus, 3000);
  graphTimer = setInterval(refreshGraph, 20000);
})();
