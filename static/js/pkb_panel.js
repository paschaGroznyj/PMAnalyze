/* ===== Personal Knowledge Base panel logic ===== */
(function () {
  "use strict";

  var $ = function (id) { return document.getElementById(id); };
  var pollTimer = null;
  var keyConfirmed = false;   // ключ подтверждён в текущей сессии
  var pendingUrl = null;      // ссылка, которую ждём запустить после ключа

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
        return;
      }
      startImport(url);
    });
  }


  // ===== PKB GRAPH =====
  var pkbNet = null;
  var pkbNodesDS = null;
  var pkbEdgesDS = null;
  var pkbAdj = {};          // id -> [ids]
  var pkbAllNodes = [];     // сырые узлы из full
  var pkbSeeds = [];        // текущие seed-id (search)
  var pkbBaseColor = "#CFE6F7";

  function pkbShowGraphWrap() {
    var w = $("pkb-graph-wrap");
    if (w) w.hidden = false;
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

  function pkbColorFor(imp) {
    // палитра по важности
    if (imp >= 0.75) return "#FF5A1F";
    if (imp >= 0.55) return "#FBE7A1";
    if (imp >= 0.4) return "#CDE8D5";
    return "#CFE6F7";
  }

  function pkbBuildNetwork(nodes, edges) {
    var canvas = $("pkb-kg-canvas");
    if (!canvas || !window.vis) return;
    var visNodes = nodes.map(function (n) {
      return {
        id: n.id, label: undefined, title: n.full_label || n.label,
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
    // adjacency
    pkbAdj = {};
    edges.forEach(function (e) {
      (pkbAdj[e.from] = pkbAdj[e.from] || []).push(e.to);
      (pkbAdj[e.to] = pkbAdj[e.to] || []).push(e.from);
    });
    var data = { nodes: pkbNodesDS, edges: pkbEdgesDS };
    var options = {
      physics: { stabilization: { iterations: 180 }, barnesHut: { gravitationalConstant: -8000, springLength: 130 } },
      interaction: { hover: true, tooltipDelay: 120 },
      nodes: { scaling: { min: 12, max: 34 } }
    };
    if (pkbNet) { pkbNet.destroy(); pkbNet = null; }
    pkbNet = new vis.Network(canvas, data, options);
    pkbNet.on("click", function (params) {
      if (params.nodes && params.nodes.length) {
        pkbOpenCard(params.nodes[0]);
      }
    });
  }

  async function pkbLoadFull() {
    var loader = $("pkb-kg-loader");
    var btn = $("pkb-kg-load-full");
    if (btn) btn.style.display = "none";
    if (loader) loader.hidden = false;
    try {
      var d = await pkbApiGet("/api/private-kb/graph/full");
      pkbAllNodes = d.nodes || [];
      pkbBuildNetwork(d.nodes || [], d.edges || []);
      var c = $("pkb-kg-counts");
      if (c) c.textContent = "узлов: " + (d.counts ? d.counts.nodes : (d.nodes || []).length) +
        " · связей: " + (d.counts ? d.counts.edges : (d.edges || []).length);
    } catch (e) {
      if (btn) { btn.style.display = ""; }
      var cc = $("pkb-kg-counts");
      if (cc) cc.textContent = "ошибка: " + e.message;
    } finally {
      if (loader) loader.hidden = true;
    }
  }

  async function pkbOpenCard(nodeId) {
    var card = $("pkb-kg-card");
    if (!card) return;
    card.hidden = false;
    card.innerHTML = '<div class="mono">загрузка…</div>';
    try {
      var d = await pkbApiGet("/api/private-kb/graph/card?node_id=" + encodeURIComponent(nodeId));
      var tags = (d.tags || []).map(function (t) { return "#" + t; }).join(" ");
      var neigh = (d.neighbors || []).map(function (nb) {
        return '<button data-nid="' + nb.node_id + '">' + (nb.label || nb.node_id) +
          (nb.relation ? ' · ' + nb.relation : '') + '</button>';
      }).join("");
      card.innerHTML =
        '<h4>' + (d.label || d.node_id) + '</h4>' +
        (tags ? '<div class="mono" style="font-size:11px;margin-bottom:6px;color:#6b5a45">' + tags + '</div>' : '') +
        '<div class="pkb-kg-card-text">' + ((d.text || "").replace(/</g, "&lt;")) + '</div>' +
        (neigh ? '<div class="pkb-kg-neigh">' + neigh + '</div>' : '');
      Array.prototype.forEach.call(card.querySelectorAll(".pkb-kg-neigh button"), function (b) {
        b.addEventListener("click", function () {
          var nid = b.getAttribute("data-nid");
          if (pkbNet) { try { pkbNet.selectNodes([nid]); pkbNet.focus(nid, { scale: 1.1, animation: true }); } catch (e) {} }
          pkbOpenCard(nid);
        });
      });
    } catch (e) {
      card.innerHTML = '<div class="mono">ошибка карточки: ' + e.message + '</div>';
    }
  }

  async function pkbSearch() {
    var q = (($("pkb-kg-query") || {}).value || "").trim();
    if (!q) { pkbLoadFull(); return; }
    var depth = parseInt((($("pkb-kg-depth") || {}).value) || "1", 10);
    var wantSum = !!(($("pkb-kg-want-summary") || {}).checked);
    var loader = $("pkb-kg-loader");
    var btn = $("pkb-kg-load-full");
    var sumBox = $("pkb-kg-summary");
    if (btn) btn.style.display = "none";
    if (loader) loader.hidden = false;
    if (sumBox) { sumBox.hidden = true; sumBox.textContent = ""; }
    try {
      var d = await pkbApiPost("/api/private-kb/graph/search", { query: q, depth: depth, want_summary: wantSum });
      pkbSeeds = d.seeds || [];
      pkbBuildNetwork(d.nodes || [], d.edges || []);
      // подсветка seed-узлов
      if (pkbNodesDS && pkbSeeds.length) {
        var upd = pkbSeeds.map(function (id) { return { id: id, borderWidth: 4, color: { background: "#FF5A1F", border: "#1E1A16" } , size: 18 }; });
        try { pkbNodesDS.update(upd); } catch (e) {}
      }
      var c = $("pkb-kg-counts");
      if (c) c.textContent = "найдено сидов: " + (d.found || 0) + " · узлов: " +
        (d.counts ? d.counts.nodes : 0) + " · связей: " + (d.counts ? d.counts.edges : 0);
      if (wantSum && sumBox) {
        if (d.summary) { sumBox.hidden = false; sumBox.textContent = d.summary; }
        else if (d.summary_error) { sumBox.hidden = false; sumBox.textContent = "саммари недоступно: " + d.summary_error; }
      }
    } catch (e) {
      var cc = $("pkb-kg-counts");
      if (cc) cc.textContent = "ошибка поиска: " + e.message;
    } finally {
      if (loader) loader.hidden = true;
    }
  }

  var pkbSearchTimer = null;
  function pkbBindGraph() {
    var btnFull = $("pkb-kg-load-full");
    if (btnFull) btnFull.addEventListener("click", pkbLoadFull);
    var refresh = $("pkb-kg-refresh");
    if (refresh) refresh.addEventListener("click", function () {
      var q = (($("pkb-kg-query") || {}).value || "").trim();
      if (q) pkbSearch(); else pkbLoadFull();
    });
    var go = $("pkb-kg-search-go");
    if (go) go.addEventListener("click", pkbSearch);
    var qi = $("pkb-kg-query");
    if (qi) {
      qi.addEventListener("keydown", function (e) { if (e.key === "Enter") pkbSearch(); });
      qi.addEventListener("input", function () {
        if (pkbSearchTimer) clearTimeout(pkbSearchTimer);
        pkbSearchTimer = setTimeout(function () {
          var v = qi.value.trim();
          if (v.length >= 3) pkbSearch();
        }, 400);
      });
    }
    var depth = $("pkb-kg-depth");
    var badge = $("pkb-kg-depth-badge");
    if (depth) depth.addEventListener("input", function () {
      if (badge) badge.textContent = depth.value;
      var q = (($("pkb-kg-query") || {}).value || "").trim();
      if (q) {
        if (pkbSearchTimer) clearTimeout(pkbSearchTimer);
        pkbSearchTimer = setTimeout(pkbSearch, 250);
      }
    });
  }

  // ---------- кнопка "Открыть мой граф": заранее спросить ключ ----------
  async function openGraphFlow() {
    // если ключ уже подтверждён в этой сессии — сразу открываем граф
    if (keyConfirmed) {
      pkbShowGraphWrap();
      pkbLoadFull();
      return;
    }
    // иначе проверим, есть ли вообще ключ у пользователя,
    // и откроем модалку на нужном экране
    pendingUrl = null; // важно: не запускать импорт после ввода ключа
    try {
      var st = await api("/api/private-kb/stats", { method: "GET" });
      openModal();
      if (st && st.has_secret) {
        // ключ есть -> сразу экран ввода
        setModalStatus("", null);
        showScreen("verify");
        var inp = $("pkb-key-input");
        if (inp) { inp.value = ""; inp.focus(); }
      } else {
        // ключа нет -> экран выбора (сгенерировать / ввести)
        showScreen("choice");
      }
    } catch (e) {
      // не смогли узнать статус — открываем обычную модалку выбора
      openModal();
      showScreen("choice");
    }
  }

  function bindOpenGraph() {
    var btn = $("pkb-open-graph");
    if (btn) btn.addEventListener("click", openGraphFlow);
  }

  function init() {
    bindModal();
    bindStart();
    bindOpenGraph();
    pkbBindGraph();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
