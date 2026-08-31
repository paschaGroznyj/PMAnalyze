/* ===== Personal Knowledge Base panel logic ===== */
(function () {
  "use strict";

  var $ = function (id) { return document.getElementById(id); };
  var pollTimer = null;
  var keyConfirmed = false;   // ключ подтверждён в текущей сессии
  var pendingUrl = null;      // ссылка, которую ждём запустить после ключа
  var pkbLastFocusNode = null;
  var pkbLastFocusTs = 0;

  // ===== PKB debug logging =====
  var PKB_DBG = false;
  try {
    if (window && window.localStorage && window.localStorage.getItem("pkb_debug") === "0") PKB_DBG = false;
    if (window && window.location && /(?:\?|&)pkb_debug=0(?:&|$)/.test(window.location.search)) PKB_DBG = false;
    if (window && window.location && /(?:\?|&)pkb_debug=1(?:&|$)/.test(window.location.search)) PKB_DBG = true;
  } catch (e) {}
  function pkbDbg() {
    if (!PKB_DBG || !window.console) return;
    var args = Array.prototype.slice.call(arguments);
    args.unshift("[PKB][DBG]");
    try { console.log.apply(console, args); } catch (e) {}
  }
  function pkbDbgWarn() {
    if (!PKB_DBG || !window.console) return;
    var args = Array.prototype.slice.call(arguments);
    args.unshift("[PKB][WARN]");
    try { console.warn.apply(console, args); } catch (e) {}
  }
  function pkbDbgErr() {
    if (!PKB_DBG || !window.console) return;
    var args = Array.prototype.slice.call(arguments);
    args.unshift("[PKB][ERR]");
    try { console.error.apply(console, args); } catch (e) {}
  }

  function setKeyStatus(msg, kind) {
    var el = $("pkb-key-status");
    if (!el) return;
    el.textContent = msg || "";
    el.className = "pkb-key-status mono" + (kind ? " " + kind : "");
  }

  function setModalStatus(msg, kind) {
    var el = $("pkb-modal-status");
    if (!el) return;
    el.textContent = msg || "";
    el.className = "pkb-key-status mono" + (kind ? " " + kind : "");
  }

  async function api(path, opts) {
    opts = opts || {};
    opts.headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
    var r = await fetch(path, opts);
    var data = null;
    try { data = await r.json(); } catch (e) { data = null; }
    if (!r.ok) {
      var msg = (data && (data.detail || data.error)) || ("HTTP " + r.status);
      throw new Error(msg);
    }
    return data;
  }

  // ---------- модалка: управление экранами ----------
  function showScreen(name) {
    ["choice", "secret", "verify"].forEach(function (s) {
      var el = $("pkb-screen-" + s);
      if (el) el.hidden = (s !== name);
    });
    var titleMap = {
      choice: "Секретный ключ",
      secret: "Ваш новый ключ",
      verify: "Вход по ключу"
    };
    var t = $("pkb-modal-title");
    if (t) t.textContent = titleMap[name] || "Секретный ключ";
  }

  function openModal() {
    setModalStatus("", null);
    showScreen("choice");
    var modal = $("pkb-modal");
    if (modal) modal.hidden = false;
  }

  function closeModal() {
    var modal = $("pkb-modal");
    if (modal) modal.hidden = true;
  }

  // ---------- генерация нового ключа ----------
  async function genKey() {
    showScreen("secret");
    var val = $("pkb-secret-value");
    var ok = $("pkb-modal-ok");
    var chk = $("pkb-confirm-saved");
    if (val) val.textContent = "генерация…";
    if (chk) chk.checked = false;
    if (ok) ok.disabled = true;
    try {
      var data = await api("/api/private-kb/secret/generate", { method: "POST", body: "{}" });
      var secret = data && data.secret_code ? data.secret_code : "";
      if (val) val.textContent = secret || "(пустой ответ сервера)";
    } catch (e) {
      if (val) val.textContent = "ошибка";
      // не откатываемся на экран выбора вслепую — показываем причину прямо тут
      if (ok) ok.disabled = true;
      var box = document.querySelector("#pkb-screen-secret .pkb-modal-desc");
      if (box) { box.textContent = "Не удалось сгенерировать ключ: " + e.message; box.classList.add("err"); }
    }
  }

  // ---------- проверка существующего ключа ----------
  async function verifyKey() {
    var code = (($("pkb-key-input") || {}).value || "").trim();
    if (code.length < 6) { setModalStatus("Введите ключ", "err"); return; }
    setModalStatus("проверка…", null);
    try {
      var data = await api("/api/private-kb/verify", {
        method: "POST", body: JSON.stringify({ secret_code: code })
      });
      if (data && data.verified) {
        onKeyConfirmed("Ключ верный. Импорт запущен.");
      } else {
        setModalStatus("Ключ неверный или не инициализирован", "err");
      }
    } catch (e) {
      setModalStatus("Ошибка: " + e.message, "err");
    }
  }

  // ---------- ключ подтверждён -> продолжаем импорт ----------
  function onKeyConfirmed(statusMsg) {
    keyConfirmed = true;
    closeModal();
    setKeyStatus(statusMsg || "Ключ подтверждён.", "ok");
    var url = pendingUrl;
    pendingUrl = null;
    if (url && url.length >= 10) {
      // сценарий: подтвердили ключ ради запуска импорта
      startImport(url);
    } else {
      // сценарий: подтвердили ключ ради просмотра существующего графа
      pkbShowGraphWrap();
      pkbLoadFull();
    }
  }

  function bindModal() {
    var close = $("pkb-modal-close");
    var overlay = $("pkb-modal");
    if (close) close.addEventListener("click", closeModal);
    if (overlay) overlay.addEventListener("click", function (e) {
      if (e.target === overlay) closeModal();
    });

    var genBtn = $("pkb-choice-gen");
    var haveBtn = $("pkb-choice-have");
    if (genBtn) genBtn.addEventListener("click", genKey);
    if (haveBtn) haveBtn.addEventListener("click", function () {
      setModalStatus("", null);
      showScreen("verify");
      var inp = $("pkb-key-input");
      if (inp) { inp.value = ""; inp.focus(); }
    });

    var backS = $("pkb-secret-back");
    var backV = $("pkb-verify-back");
    if (backS) backS.addEventListener("click", function () { setModalStatus("", null); showScreen("choice"); });
    if (backV) backV.addEventListener("click", function () { setModalStatus("", null); showScreen("choice"); });

    var chk = $("pkb-confirm-saved");
    var ok = $("pkb-modal-ok");
    if (chk && ok) chk.addEventListener("change", function () { ok.disabled = !chk.checked; });
    if (ok) ok.addEventListener("click", function () {
      onKeyConfirmed("Ключ сохранён. Импорт запущен.");
    });

    var copy = $("pkb-copy-secret");
    if (copy) copy.addEventListener("click", function () {
      var v = ($("pkb-secret-value") || {}).textContent || "";
      if (navigator.clipboard) navigator.clipboard.writeText(v);
      copy.textContent = "Скопировано";
      setTimeout(function () { copy.textContent = "Копировать"; }, 1500);
    });

    var verifyBtn = $("pkb-verify-key");
    if (verifyBtn) verifyBtn.addEventListener("click", verifyKey);
    var keyInput = $("pkb-key-input");
    if (keyInput) keyInput.addEventListener("keydown", function (e) {
      if (e.key === "Enter") verifyKey();
    });
  }

  // ---------- импорт + прогресс ----------
  function renderProgress(st) {
    var box = $("pkb-progress");
    if (box) box.hidden = false;
    var total = st.files_total || 0;
    var done = st.files_done || 0;
    var pct = total > 0 ? Math.round((done / total) * 100) : (st.status === "done" ? 100 : 5);
    var fill = $("pkb-bar-fill");
    if (fill) fill.style.width = pct + "%";
    var statusMap = {
      created: "инициализация", downloading: "скачивание архива",
      unzipping: "распаковка", processing: "обработка документов",
      done: "готово", error: "ошибка"
    };
    var sEl = $("pkb-progress-status");
    if (sEl) sEl.textContent = statusMap[st.status] || st.status || "";
    var cEl = $("pkb-progress-count");
    if (cEl) cEl.textContent = done + " / " + total;
    var fEl = $("pkb-current-file");
    if (fEl) fEl.textContent = st.current_file ? ("файл: " + st.current_file) : "";
    var stEl = $("pkb-progress-stats");
    if (stEl) stEl.textContent = "узлов: " + (st.nodes_created || 0) + " · связей: " + (st.edges_created || 0);
    var errBox = $("pkb-errors");
    if (errBox) {
      errBox.innerHTML = "";
      var errs = st.errors || [];
      if (st.error_text) errs = errs.concat([{ file: null, error: st.error_text }]);
      errs.slice(0, 12).forEach(function (e) {
        var d = document.createElement("div");
        d.className = "pkb-err-item";
        d.textContent = (e.file ? e.file + ": " : "") + e.error;
        errBox.appendChild(d);
      });
    }
  }

  async function pollStatus(importId) {
    try {
      var st = await api("/api/private-kb/import/status?import_id=" + importId, { method: "GET" });
      renderProgress(st);
      if (st.status === "done" || st.status === "error") {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        var btn = $("pkb-start");
        if (btn) { btn.disabled = false; btn.textContent = "Обработать"; }
        if (st.status === "done") {
          pkbShowGraphWrap();
          pkbLoadFull();
        }
      }
    } catch (e) {
      renderProgress({ status: "error", error_text: e.message });
      if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
      var b = $("pkb-start");
      if (b) { b.disabled = false; b.textContent = "Обработать"; }
    }
  }

  async function startImport(url) {
    var btn = $("pkb-start");
    if (btn) { btn.disabled = true; btn.textContent = "Запуск…"; }
    try {
      var data = await api("/api/private-kb/import/start", {
        method: "POST", body: JSON.stringify({ drive_url: url })
      });
      var importId = data.import_id;
      renderProgress({ status: "created", files_total: 0, files_done: 0 });
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = setInterval(function () { pollStatus(importId); }, 1500);
      pollStatus(importId);
    } catch (e) {
      // если сервер требует ключ — откроем модалку
      var m = (e.message || "").toLowerCase();
      if (m.indexOf("ключ") !== -1 || m.indexOf("key") !== -1 || m.indexOf("401") !== -1 || m.indexOf("403") !== -1) {
        keyConfirmed = false;
        pendingUrl = url;
        if (btn) { btn.disabled = false; btn.textContent = "Обработать"; }
        openModal();
        return;
      }
      renderProgress({ status: "error", error_text: e.message });
      if (btn) { btn.disabled = false; btn.textContent = "Обработать"; }
    }
  }

  function bindStart() {
    var btn = $("pkb-start");
    if (!btn) return;
    btn.addEventListener("click", function () {
      var url = (($("pkb-drive-url") || {}).value || "").trim();
      if (url.length < 10) { setKeyStatus("Вставьте ссылку на архив", "err"); return; }
      setKeyStatus("", null);
      // Модалка ключа показывается ТОЛЬКО при попытке отправить ссылку
      if (!keyConfirmed) {
        pendingUrl = url;
        openModal();
        api("/api/private-kb/stats", { method: "GET" }).then(function (st) {
          if (st && st.has_secret) {
            setModalStatus("", null);
            showScreen("verify");
            var inp = $("pkb-key-input");
            if (inp) { inp.value = ""; inp.focus(); }
          } else {
            showScreen("choice");
          }
        }).catch(function () {
          showScreen("choice");
        });
        return;
      }
      startImport(url);
    });
  }


  // ===== PKB GRAPH (переписано с нуля, единый путь клика через vis) =====
  var pkbNet = null;
  var pkbNodesDS = null;
  var pkbEdgesDS = null;
  var pkbAdj = {};          // id -> [ids]
  var pkbNodeMeta = {};     // id -> {label, title, importance}
  var pkbBuilt = false;
  var pkbSearchTimer = null;
  var pkbLastItems = [];
  var pkbSelectedNodeId = null;
  var pkbPendingConfirm = null;
  var PKB_PAL = ["#FF5A1F", "#FF7D51", "#FFA581", "#FFC9B1", "#FFE3D8", "#FFF1EA"];

  function pkbEscHtml(s) {
    return String(s || "").replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }
  function pkbMdToHtml(md) {
    if (!md) return "";
    return pkbEscHtml(md)
      .replace(/\n\n+/g, "</p><p>")
      .replace(/\n/g, "<br>")
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      .replace(/`(.+?)`/g, "<code>$1</code>");
  }
  function pkbColorFor(imp) {
    if (imp >= 0.75) return "#FF5A1F";
    if (imp >= 0.55) return "#FBE7A1";
    if (imp >= 0.40) return "#CDE8D5";
    return "#CFE6F7";
  }

  async function pkbApiGet(path) {
    var r = await fetch(path, { method: "GET" });
    var d = null; try { d = await r.json(); } catch (e) {}
    if (!r.ok) throw new Error((d && (d.error || d.detail)) || ("HTTP " + r.status));
    return d;
  }
  async function pkbApiPost(path, body) {
    var r = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    var d = null; try { d = await r.json(); } catch (e) {}
    if (!r.ok) throw new Error((d && (d.error || d.detail)) || ("HTTP " + r.status));
    return d;
  }

  function pkbShowGraphWrap() {
    var w = $("pkb-graph-wrap");
    if (w) w.hidden = false;
  }
  function pkbCloseCard() {
    var card = $("pkb-kg-card");
    if (card) { card.hidden = true; card.innerHTML = ""; }
  }

  function pkbEnsureCard() {
    var card = $("pkb-kg-card");
    if (card) return card;
    var host = $("pkb-kg-canvas");
    if (!host) {
      pkbDbgErr("pkbEnsureCard:noHost");
      return null;
    }
    card = document.createElement("div");
    card.id = "pkb-kg-card";
    card.className = "kg-card pkb-kg-card";
    card.hidden = true;
    host.appendChild(card);
    pkbDbgWarn("pkbEnsureCard:created_missing_card", {hostChildren: host.children ? host.children.length : null});
    return card;
  }

  function pkbNormNodeId(nid) {
    if (nid === null || nid === undefined) return null;
    var s = String(nid).trim();
    return s ? s : null;
  }

  function pkbBuildNetwork(nodes, edges) {
    var host = $("pkb-kg-canvas");
    pkbDbg("pkbBuildNetwork:start", {hasHost: !!host, hasVis: !!window.vis, nodes: (nodes||[]).length, edges: (edges||[]).length});
    if (!host || !window.vis) {
      pkbDbgErr("pkbBuildNetwork:abort", {reason: !host ? "no_host" : "no_vis"});
      return;
    }
    try {
      var rect = host.getBoundingClientRect();
      pkbDbg("pkbBuildNetwork:hostRect", {w: Math.round(rect.width), h: Math.round(rect.height), hidden: host.hidden});
    } catch (e) {}

    var visNodes = nodes.map(function (n) {
      return {
        id: n.id, title: n.full_label || n.label,
        color: { background: pkbColorFor(n.importance), border: "#1E1A16" },
        borderWidth: 2,
        font: { face: "Space Mono", size: 11, color: "#1E1A16" },
        shape: "dot",
        size: 10 + Math.round((Number(n.importance) || 0.5) * 14)
      };
    });
    var visEdges = edges.map(function (e, i) {
      return { id: "e" + i, from: e.from, to: e.to, label: e.relation || "",
        font: { align: "middle", size: 9, color: "#3a342d" },
        color: { color: (e.relation && e.relation !== "relates_to") ? "#FF5A1F" : "#7b6f8f" },
        arrows: "to", smooth: { type: "continuous" } };
    });

    pkbNodesDS = new vis.DataSet(visNodes);
    pkbEdgesDS = new vis.DataSet(visEdges);

    pkbAdj = {};
    edges.forEach(function (e) {
      (pkbAdj[e.from] = pkbAdj[e.from] || []).push(e.to);
      (pkbAdj[e.to] = pkbAdj[e.to] || []).push(e.from);
    });
    pkbNodeMeta = {};
    nodes.forEach(function (n) {
      pkbNodeMeta[n.id] = { label: n.label || "", title: n.full_label || n.title || "", importance: n.importance };
    });

    var options = {
      physics: { stabilization: { iterations: 180 }, barnesHut: { gravitationalConstant: -8000, springLength: 130 } },
      interaction: { hover: true, tooltipDelay: 120, dragNodes: true, dragView: true, zoomView: true },
      nodes: { scaling: { min: 12, max: 34 } }
    };

    if (pkbNet) {
      pkbDbg("pkbBuildNetwork:destroyPrevious");
      try { pkbNet.destroy(); } catch (e) { pkbDbgErr("pkbBuildNetwork:destroyPrevious:error", e && e.message ? e.message : e); }
      pkbNet = null;
    }
    pkbNet = new vis.Network(host, { nodes: pkbNodesDS, edges: pkbEdgesDS }, options);
    pkbDbg("pkbBuildNetwork:networkCreated");

    // ЕДИНСТВЕННЫЙ путь: событие vis "click". Никаких нативных canvas-listener'ов
    // с preventDefault — именно они ломали drag/zoom и уводили клик в слайдер.
    pkbNet.on("click", function (params) {
      var nid = null;
      var at = null;
      if (params && params.nodes && params.nodes.length) {
        nid = params.nodes[0];
      } else if (params && params.pointer && params.pointer.DOM) {
        // fallback: params.nodes бывает пуст, если клик распознан как микро-drag —
        // достаём узел прямо из координат указателя
        try { at = pkbNet.getNodeAt(params.pointer.DOM); nid = at; } catch (e) {}
      }
      pkbDbg("vis:click", {nodes: params && params.nodes ? params.nodes : [], edges: params && params.edges ? params.edges : [], pointer: params && params.pointer ? params.pointer.DOM : null, getNodeAt: at, rawNode: nid});
      nid = pkbNormNodeId(nid);
      if (nid) {
        pkbDbg("vis:click->focus", {nid: nid});
        pkbOpenNodeCardSafe(nid);
      } else {
        pkbDbgWarn("vis:click->emptyNode closeCard");
        pkbCloseCard();
      }
    });
    // selectNode — ещё один надёжный источник, если click проглотил узел
    pkbNet.on("selectNode", function (params) {
      var nid = (params && params.nodes && params.nodes.length) ? params.nodes[0] : null;
      pkbDbg("vis:selectNode", {nodes: params && params.nodes ? params.nodes : [], rawNode: nid});
      nid = pkbNormNodeId(nid);
      if (nid) pkbOpenNodeCardSafe(nid);
    });
    pkbNet.on("doubleClick", function (params) {
      var nid = (params && params.nodes && params.nodes.length) ? params.nodes[0] : null;
      pkbDbg("vis:doubleClick", {nodes: params && params.nodes ? params.nodes : [], rawNode: nid});
      nid = pkbNormNodeId(nid);
      if (nid) pkbOpenNodeCardSafe(nid);
    });
    pkbNet.on("hold", function (params) {
      var nid = (params && params.nodes && params.nodes.length) ? params.nodes[0] : null;
      pkbDbg("vis:hold", {nodes: params && params.nodes ? params.nodes : [], rawNode: nid});
      nid = pkbNormNodeId(nid);
      if (nid) pkbOpenNodeCardSafe(nid);
    });

    // дополнительная телеметрия vis событий
    pkbNet.on("select", function (params) { pkbDbg("vis:select", params || {}); });
    pkbNet.on("deselectNode", function (params) { pkbDbg("vis:deselectNode", params || {}); });
    pkbNet.on("dragStart", function (params) { pkbDbg("vis:dragStart", params || {}); });
    pkbNet.on("dragEnd", function (params) { pkbDbg("vis:dragEnd", params || {}); });
    pkbNet.on("zoom", function (params) { pkbDbg("vis:zoom", {scale: params && params.scale}); });
    pkbNet.on("release", function (params) { pkbDbg("vis:release", params || {}); });
    pkbNet.once("stabilized", function (iter) { pkbDbg("vis:stabilized", {iterations: iter}); });

    // сырые DOM-события на холсте/контейнере
    ["pointerdown","pointerup","click","mousedown","mouseup","touchstart","touchend"].forEach(function (evt) {
      host.addEventListener(evt, function (e) {
        var t = e && e.target;
        pkbDbg("dom:" + evt, {target: t ? (t.tagName + (t.className ? "." + String(t.className).replace(/\s+/g,".") : "")) : null, x: e.clientX, y: e.clientY});
      }, true);
    });

    pkbBuilt = true;
    var cardAtReady = $("pkb-kg-card");
    pkbDbg("pkbBuildNetwork:ready", {pkbBuilt: pkbBuilt, hasCard: !!cardAtReady, hostChildren: host && host.children ? host.children.length : null});
  }

  function pkbOpenNodeCardSafe(nodeId) {
    var now = Date.now();
    if (pkbLastFocusNode === nodeId && (now - pkbLastFocusTs) < 260) {
      pkbDbg("pkbOpenNodeCardSafe:dedup", {nodeId: nodeId, dt: now - pkbLastFocusTs});
      return;
    }
    pkbLastFocusNode = nodeId;
    pkbLastFocusTs = now;
    pkbFocusNode(nodeId, true);
  }

  function pkbFocusNode(nodeId, openCard) {
    nodeId = pkbNormNodeId(nodeId);
    pkbSelectedNodeId = nodeId || null;
    pkbDbg("pkbFocusNode:call", {nodeId: nodeId, openCard: !!openCard, hasNet: !!pkbNet});
    if (!nodeId || !pkbNet) {
      pkbDbgWarn("pkbFocusNode:skip", {nodeId: nodeId, hasNet: !!pkbNet});
      return;
    }
    try {
      pkbNet.selectNodes([nodeId]);
      // Без анимированного focus: он вызывал рывки и блокирующие микро-анимации после клика
      pkbNet.moveTo({ position: pkbNet.getPosition(nodeId), scale: pkbNet.getScale(), animation: false });
    } catch (e) {}
    if (pkbNodesDS) {
      try {
        pkbNodesDS.update([{ id: nodeId, borderWidth: 5, color: { background: "#FF5A1F", border: "#1E1A16" } }]);
        setTimeout(function () {
          try {
            var m = pkbNodeMeta[nodeId] || {};
            pkbNodesDS.update([{ id: nodeId, borderWidth: 2, color: { background: pkbColorFor(m.importance), border: "#1E1A16" } }]);
          } catch (e) {}
        }, 1200);
      } catch (e) {}
    }
    if (openCard) {
      pkbDbg("pkbFocusNode:openCard", {nodeId: nodeId});
      pkbOpenCard(nodeId);
    }
  }

  function pkbEnableCardScrollIsolation(card) {
    if (!card || card.__pkbWheelBound) return;
    var stop = function (e) { e.stopPropagation(); };
    ["wheel", "mousewheel", "DOMMouseScroll", "touchstart", "touchmove"].forEach(function (evt) {
      card.addEventListener(evt, stop, { passive: true });
    });
    card.__pkbWheelBound = true;
  }

  async function pkbOpenCard(nodeId) {
    nodeId = pkbNormNodeId(nodeId);
    var card = pkbEnsureCard();
    pkbDbg("pkbOpenCard:start", {nodeId: nodeId, hasCard: !!card});
    if (!card || !nodeId) {
      pkbDbgWarn("pkbOpenCard:skip", {nodeId: nodeId, hasCard: !!card});
      return;
    }
    card.hidden = false;
    card.innerHTML = '<div class="mono" style="padding:6px 0">загрузка карточки…</div>';
    try {
      var cardUrl = "/api/private-kb/graph/card?node_id=" + encodeURIComponent(nodeId);
      pkbDbg("pkbOpenCard:fetch", cardUrl);
      var d = await pkbApiGet(cardUrl);
      pkbDbg("pkbOpenCard:ok", {node_id: d && d.node_id, hasText: !!(d && (d.text || d.text_knowledge)), neighbors: d && d.neighbors ? d.neighbors.length : 0});
      if (d && d.ok === false) throw new Error(d.error || "not_found");
      var tags = (d.tags || []).map(function (t) { return "#" + t; }).join(" ");
      var neigh = (d.neighbors || []).map(function (nb) {
        return '<button type="button" data-nid="' + pkbEscHtml(nb.node_id) + '">' +
          pkbEscHtml(nb.label || nb.node_id) + (nb.relation ? ' · ' + pkbEscHtml(nb.relation) : '') + '</button>';
      }).join("");
      card.innerHTML =
        '<button class="pkb-kg-card-close" type="button" aria-label="Закрыть">×</button>' +
        '<button class="pkb-kg-card-delete" type="button" aria-label="Удалить узел" title="Удалить эту карточку и её связи">' +
          '<svg viewBox="0 0 24 24" aria-hidden="true"><polyline points="3 6 5 6 21 6"></polyline><path d="M8 6V4h8v2"></path><path d="M19 6l-1 14H6L5 6"></path></svg>' +
        '</button>' +
        '<h4>' + pkbEscHtml(d.label || d.node_id) + '</h4>' +
        (tags ? '<div class="mono" style="font-size:11px;margin-bottom:6px;color:#6b5a45">' + pkbEscHtml(tags) + '</div>' : '') +
        '<div class="pkb-kg-card-text">' + pkbMdToHtml(d.text || d.text_knowledge || "") + '</div>' +
        (neigh ? '<div class="pkb-kg-neigh">' + neigh + '</div>' : '');
      pkbEnableCardScrollIsolation(card);
      var x = card.querySelector(".pkb-kg-card-close");
      if (x) x.addEventListener("click", pkbCloseCard);
      var del = card.querySelector(".pkb-kg-card-delete");
      if (del) del.addEventListener("click", pkbDeleteSelectedNode);
      Array.prototype.forEach.call(card.querySelectorAll(".pkb-kg-neigh button"), function (b) {
        b.addEventListener("click", function () { pkbOpenNodeCardSafe(b.getAttribute("data-nid")); });
      });
    } catch (e) {
      pkbDbgErr("pkbOpenCard:error", {nodeId: nodeId, message: e && e.message ? e.message : String(e)});
      card.innerHTML =
        '<button class="pkb-kg-card-close" type="button" aria-label="Закрыть">×</button>' +
        '<button class="pkb-kg-card-delete" type="button" aria-label="Удалить узел" title="Удалить эту карточку и её связи">' +
          '<svg viewBox="0 0 24 24" aria-hidden="true"><polyline points="3 6 5 6 21 6"></polyline><path d="M8 6V4h8v2"></path><path d="M19 6l-1 14H6L5 6"></path></svg>' +
        '</button>' +
        '<div class="mono">ошибка карточки: ' + pkbEscHtml(e.message) + '</div>';
      pkbEnableCardScrollIsolation(card);
      var x2 = card.querySelector(".pkb-kg-card-close");
      if (x2) x2.addEventListener("click", pkbCloseCard);
      var del2 = card.querySelector(".pkb-kg-card-delete");
      if (del2) del2.addEventListener("click", pkbDeleteSelectedNode);
    }
  }

  function pkbSetConfirmStatus(msg, kind) {
    var el = $("pkb-confirm-status");
    if (!el) return;
    el.textContent = msg || "";
    el.className = "pkb-key-status mono" + (kind ? " " + kind : "");
  }

  function pkbOpenConfirm(title, text, onOk) {
    var modal = $("pkb-confirm");
    var t = $("pkb-confirm-title");
    var d = $("pkb-confirm-text");
    var ok = $("pkb-confirm-ok");
    if (!modal || !ok) return;
    if (t) t.textContent = title || "Подтверждение";
    if (d) d.textContent = text || "Вы уверены?";
    pkbSetConfirmStatus("", null);
    ok.disabled = false;
    ok.textContent = "Подтвердить";
    pkbPendingConfirm = onOk || null;
    modal.hidden = false;
  }

  function pkbCloseConfirm() {
    var modal = $("pkb-confirm");
    if (modal) modal.hidden = true;
    pkbPendingConfirm = null;
  }

  async function pkbDeleteSelectedNode() {
    var nid = pkbSelectedNodeId || null;
    if (!nid) {
      var c = $("pkb-kg-counts");
      if (c) c.textContent = "Сначала откройте карточку узла";
      return;
    }
    pkbOpenConfirm(
      "Удалить узел",
      "Удалить открытую карточку и все её связи? Это действие нельзя отменить.",
      async function () {
        var okBtn = $("pkb-confirm-ok");
        if (okBtn) { okBtn.disabled = true; okBtn.textContent = "Удаляем…"; }
        try {
          var d = await pkbApiPost("/api/private-kb/graph/node/delete", { node_id: nid });
          pkbSetConfirmStatus("Удалено: узел " + (d.deleted_node_id || nid) + ", связей: " + (d.deleted_edges || 0), "ok");
          pkbCloseCard();
          pkbSelectedNodeId = null;
          await pkbLoadFull();
          setTimeout(pkbCloseConfirm, 180);
        } catch (e) {
          pkbSetConfirmStatus("Ошибка удаления: " + e.message, "err");
          if (okBtn) { okBtn.disabled = false; okBtn.textContent = "Подтвердить"; }
        }
      }
    );
  }

  async function pkbDeleteAllNodes() {
    pkbOpenConfirm(
      "Удалить весь граф",
      "Удалить все узлы и все связи в вашей персональной базе? Это действие нельзя отменить.",
      async function () {
        var okBtn = $("pkb-confirm-ok");
        if (okBtn) { okBtn.disabled = true; okBtn.textContent = "Удаляем…"; }
        try {
          var d = await pkbApiPost("/api/private-kb/graph/clear", {});
          pkbSetConfirmStatus("Удалено узлов: " + (d.deleted_nodes || 0) + ", связей: " + (d.deleted_edges || 0), "ok");
          pkbCloseCard();
          pkbSelectedNodeId = null;
          await pkbLoadFull();
          setTimeout(pkbCloseConfirm, 180);
        } catch (e) {
          pkbSetConfirmStatus("Ошибка очистки: " + e.message, "err");
          if (okBtn) { okBtn.disabled = false; okBtn.textContent = "Подтвердить"; }
        }
      }
    );
  }

  async function pkbExportCsv() {
    try {
      var r = await fetch('/api/private-kb/graph/export.csv', { method: 'GET' });
      if (!r.ok) {
        var txt = await r.text().catch(function(){ return ''; });
        throw new Error(txt || ('HTTP ' + r.status));
      }
      var blob = await r.blob();
      var fileName = 'private_kb_graph.csv';
      var cd = r.headers.get('content-disposition') || '';
      var m = cd.match(/filename="?([^";]+)"?/i);
      if (m && m[1]) fileName = m[1];
      var u = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = u;
      a.download = fileName;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(function(){ URL.revokeObjectURL(u); }, 1200);
    } catch (e) {
      var c = $("pkb-kg-counts");
      if (c) c.textContent = "ошибка выгрузки: " + e.message;
    }
  }

  // ---- локальный realtime-фильтр ----
  function pkbFindMatches(q) {
    var text = String(q || "").trim().toLowerCase();
    if (text.length < 2) return [];
    var toks = text.split(/\s+/).filter(Boolean);
    var out = [];
    for (var id in pkbNodeMeta) {
      if (!pkbNodeMeta.hasOwnProperty(id)) continue;
      var m = pkbNodeMeta[id];
      var hay = ((m.label || "") + " " + (m.title || "")).toLowerCase();
      if (toks.every(function (t) { return hay.indexOf(t) !== -1; })) out.push(id);
    }
    return out;
  }
  function pkbBfsDepth(matchIds, depth) {
    var dmap = {}, queue = [];
    matchIds.forEach(function (id) { dmap[id] = 0; queue.push(id); });
    while (queue.length) {
      var cur = queue.shift(), curD = dmap[cur] || 0;
      if (curD >= depth) continue;
      (pkbAdj[cur] || []).forEach(function (nb) {
        if (!(nb in dmap)) { dmap[nb] = curD + 1; queue.push(nb); }
      });
    }
    return dmap;
  }
  function pkbApplySelection(matchIds, depthMap) {
    if (!pkbNodesDS) return;
    var selected = depthMap ? Object.keys(depthMap) : (matchIds || []);
    var selSet = {};
    selected.forEach(function (id) { selSet[id] = true; });
    var upd = [];
    pkbNodesDS.get().forEach(function (n) {
      if (!selected.length) {
        var m0 = pkbNodeMeta[n.id] || {};
        upd.push({ id: n.id, opacity: 1, color: { background: pkbColorFor(m0.importance), border: "#1E1A16" },
          font: { face: "Space Mono", size: 11, color: "#1E1A16" } });
        return;
      }
      if (selSet[n.id]) {
        var lvl = depthMap ? Number(depthMap[n.id] || 0) : 0;
        upd.push({ id: n.id, opacity: 1, color: { background: PKB_PAL[Math.min(lvl, PKB_PAL.length - 1)], border: "#1E1A16" },
          font: { face: "Space Mono", size: 11, color: lvl === 0 ? "#fff" : "#1E1A16" } });
      } else {
        upd.push({ id: n.id, opacity: 0.18, color: { background: "#EDE7DF", border: "#C9BEB0" },
          font: { face: "Space Mono", size: 11, color: "#B4A895" } });
      }
    });
    try { pkbNodesDS.update(upd); } catch (e) {}
  }
  function pkbRealtimeFilter() {
    var q = (($("pkb-kg-query") || {}).value || "").trim();
    var depth = parseInt((($("pkb-kg-depth") || {}).value) || "1", 10);
    var hint = $("pkb-kg-hint");
    var chunks = $("pkb-kg-chunks-count");
    if (!q) {
      pkbApplySelection([], null);
      if (chunks) chunks.textContent = "узлов в выдаче: " + (pkbNodesDS ? pkbNodesDS.length : 0);
      if (hint) hint.textContent = "Подсветка — по мере ввода. Глубина связей — ползунком. Лупа — семантика + LLM.";
      return;
    }
    var matches = pkbFindMatches(q);
    if (!matches.length) {
      pkbApplySelection([], null);
      if (chunks) chunks.textContent = "узлов в выдаче: 0";
      if (hint) hint.textContent = "Совпадений нет. Нажмите лупу для семантического поиска по смыслу.";
      return;
    }
    var dmap = pkbBfsDepth(matches, depth);
    pkbApplySelection(matches, dmap);
    if (chunks) chunks.textContent = "узлов в выдаче: " + Object.keys(dmap).length;
    if (hint) hint.textContent = "Найдено: " + matches.length + " · с глубиной " + depth + ": " + Object.keys(dmap).length + ".";
  }

  async function pkbLoadFull() {
    pkbDbg("pkbLoadFull:start");
    var loader = $("pkb-kg-loader");
    var btn = $("pkb-kg-load-full");
    if (btn) btn.style.display = "none";
    if (loader) loader.hidden = false;
    try {
      var d = await pkbApiGet("/api/private-kb/graph/full");
      pkbBuildNetwork(d.nodes || [], d.edges || []);
      pkbCloseCard();
      var c = $("pkb-kg-counts");
      if (c) c.textContent = "узлов: " + (d.counts ? d.counts.nodes : (d.nodes || []).length) +
        " · связей: " + (d.counts ? d.counts.edges : (d.edges || []).length);
      var ch = $("pkb-kg-chunks-count");
      if (ch) ch.textContent = "узлов в выдаче: " + (d.counts ? d.counts.nodes : (d.nodes || []).length);
    } catch (e) {
      pkbDbgErr("pkbLoadFull:error", e && e.message ? e.message : e);
      if (btn) btn.style.display = "";
      var cc = $("pkb-kg-counts");
      if (cc) cc.textContent = "ошибка: " + e.message;
    } finally {
      if (loader) loader.hidden = true;
    }
  }

  // ---- семантический поиск + LLM ----
  function pkbRenderSummary(md, items, metaLabel) {
    items = items || [];
    var html = "<p>" + pkbMdToHtml(md) + "</p>";
    html = html.replace(/\[((?:\d+\s*(?:,\s*\d+\s*)*))\]/g, function (all, body) {
      var nums = String(body || "").split(",").map(function (s) { return parseInt(s.trim(), 10); })
        .filter(function (n) { return !isNaN(n); });
      if (!nums.length) return all;
      return nums.map(function (idx) {
        if (idx < 1 || idx > items.length) return "[" + idx + "]";
        var it = items[idx - 1] || {};
        var nid = String(it.node_id || it.id || "");
        if (!nid) return "[" + idx + "]";
        return '<a href="#" class="pkb-ref-link" data-nid="' + pkbEscHtml(nid) + '">[' + idx + "]</a>";
      }).join(", ");
    });
    var title = metaLabel
      ? '<div class="mono" style="font-size:11px;color:#6b6055;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px">LLM-саммари · ' + pkbEscHtml(metaLabel) + "</div>"
      : "";
    return title + html;
  }
  function pkbWireRefLinks() {
    var box = $("pkb-kg-summary");
    if (!box) return;
    Array.prototype.forEach.call(box.querySelectorAll(".pkb-ref-link"), function (a) {
      a.addEventListener("click", function (e) {
        e.preventDefault();
        var nid = a.getAttribute("data-nid");
        if (nid) pkbOpenNodeCardSafe(nid);
      });
    });
  }
  async function pkbSearch() {
    pkbDbg("pkbSearch:start");
    var q = (($("pkb-kg-query") || {}).value || "").trim();
    if (!q) { if (pkbBuilt) pkbApplySelection([], null); return; }
    var depth = parseInt((($("pkb-kg-depth") || {}).value) || "1", 10);
    var wantSum = !!(($("pkb-kg-want-summary") || {}).checked);
    var loader = $("pkb-kg-loader");
    var sumBox = $("pkb-kg-summary");
    if (loader) loader.hidden = false;
    if (sumBox) { sumBox.hidden = true; sumBox.innerHTML = ""; }
    try {
      var d = await pkbApiPost("/api/private-kb/graph/search", { query: q, depth: depth, want_summary: wantSum });
      pkbLastItems = d.nodes || [];
      pkbBuildNetwork(d.nodes || [], d.edges || []);
      pkbCloseCard();
      var seeds = d.seeds || [];
      if (pkbNodesDS && seeds.length) {
        try {
          pkbNodesDS.update(seeds.map(function (id) {
            return { id: id, borderWidth: 4, color: { background: "#FF5A1F", border: "#1E1A16" }, size: 18 };
          }));
        } catch (e) {}
      }
      var c = $("pkb-kg-counts");
      if (c) c.textContent = "сидов: " + (d.found || 0) + " · узлов: " +
        (d.counts ? d.counts.nodes : 0) + " · связей: " + (d.counts ? d.counts.edges : 0);
      var ch2 = $("pkb-kg-chunks-count");
      if (ch2) ch2.textContent = "узлов в выдаче: " + (d.counts ? d.counts.nodes : 0);
      if (wantSum && sumBox) {
        if (d.summary) {
          sumBox.hidden = false;
          sumBox.innerHTML = pkbRenderSummary(d.summary, pkbLastItems, q);
          pkbWireRefLinks();
        } else if (d.summary_error) {
          sumBox.hidden = false;
          sumBox.innerHTML = '<span class="mono" style="font-size:11px;color:#9a8f82">саммари недоступно: ' + pkbEscHtml(d.summary_error) + "</span>";
        }
      }
    } catch (e) {
      var cc = $("pkb-kg-counts");
      if (cc) cc.textContent = "ошибка поиска: " + e.message;
    } finally {
      if (loader) loader.hidden = true;
    }
  }

  function pkbBindGraph() {
    var btnFull = $("pkb-kg-load-full");
    if (btnFull) btnFull.addEventListener("click", pkbLoadFull);
    var refresh = $("pkb-kg-refresh");
    if (refresh) refresh.addEventListener("click", function () {
      var qEl = $("pkb-kg-query");
      pkbLoadFull().then(function () { if (qEl && qEl.value.trim()) pkbRealtimeFilter(); });
    });
    var delAll = $("pkb-kg-delete-all");
    if (delAll) delAll.addEventListener("click", pkbDeleteAllNodes);
    var exportBtn = $("btn-kg-export");
    if (exportBtn) exportBtn.addEventListener("click", pkbExportCsv);
    var cCancel = $("pkb-confirm-cancel");
    if (cCancel) cCancel.addEventListener("click", pkbCloseConfirm);
    var cOverlay = $("pkb-confirm");
    if (cOverlay) cOverlay.addEventListener("click", function (e) { if (e.target === cOverlay) pkbCloseConfirm(); });
    var cOk = $("pkb-confirm-ok");
    if (cOk) cOk.addEventListener("click", function () { if (typeof pkbPendingConfirm === "function") pkbPendingConfirm(); });
    var go = $("pkb-kg-search-go");
    if (go) go.addEventListener("click", pkbSearch);
    var qi = $("pkb-kg-query");
    if (qi) {
      qi.addEventListener("keydown", function (e) { if (e.key === "Enter") { e.preventDefault(); pkbSearch(); } });
      qi.addEventListener("input", function () {
        if (pkbSearchTimer) clearTimeout(pkbSearchTimer);
        pkbSearchTimer = setTimeout(pkbRealtimeFilter, 140);
      });
    }
    var depth = $("pkb-kg-depth");
    var badge = $("pkb-kg-depth-badge");
    if (depth) {
      if (badge) badge.textContent = depth.value;
      depth.addEventListener("input", function () {
        if (badge) badge.textContent = depth.value;
        pkbRealtimeFilter();
      });
    }
  }

  async function openGraphFlow() {
    if (keyConfirmed) { pkbShowGraphWrap(); if (!pkbBuilt) pkbLoadFull(); return; }
    pendingUrl = null;
    try {
      var st = await api("/api/private-kb/stats", { method: "GET" });
      openModal();
      if (st && st.has_secret) {
        setModalStatus("", null);
        showScreen("verify");
        var inp = $("pkb-key-input");
        if (inp) { inp.value = ""; inp.focus(); }
      } else {
        showScreen("choice");
      }
    } catch (e) {
      openModal();
      showScreen("choice");
    }
  }

  function bindOpenGraph() {
    var btn = $("pkb-open-graph");
    if (btn) btn.addEventListener("click", openGraphFlow);
  }

  // ---- init ----
  function pkbInit() {
    pkbDbg("pkbInit", {debug: PKB_DBG});
    bindModal();
    bindStart();
    bindOpenGraph();
    pkbBindGraph();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", pkbInit);
  } else {
    pkbInit();
  }
})();
