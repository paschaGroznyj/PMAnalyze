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
    var url = pendingUrl || (($("pkb-drive-url") || {}).value || "").trim();
    pendingUrl = null;
    if (url && url.length >= 10) {
      startImport(url);
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

  function init() {
    bindModal();
    bindStart();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
