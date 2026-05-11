// ============================================================
// ADEQ TOOLBAR — Popup v4
// ============================================================

import { checkDuplicate, pushToMonday, updateMonday, getMondayBoardIndex, fetchImportCandidates, fetchMondayForRefresh, parseTrafficText } from "../modules/monday.js";
import { getTraffic, formatTraffic, passesTrafficFilter, setTrafficAuthToken } from "../modules/traffic.js";
import { scrapeEmailsFromPage, findDecisionMakerViaApollo, quickValidateEmail, revealApolloEmail } from "../modules/scraper.js";
import { runAudit }                                                                            from "../modules/audit.js";
import { generatePitch }                                                                     from "../modules/gemini.js";
// (geminiSearch.searchEmailsWithGemini removido — no se usa en popup, solo en scraper.js)
import { verifyEmail, verifyEmailDeep, isGarbageEmail }                                         from "../modules/emailVerifier.js";
import { runCascade }                                                                          from "../modules/cascade.js";
import { detectBanners }                                                                       from "../modules/bannerDetector.js";
import { saveHistory, loadHistory, clearHistory, saveSendDate,
         loadKeywordsFromDB, importKeywordsToDB, clearKeywordsDB, countKeywordsDB,
         searchKeywordsInDB, supabaseSignIn, supabaseRefresh, supabaseResetPassword, fetchApiKeys, setSupabaseAuth,
         uploadCsvDomains, getCsvQueueStats, getCsvQueueHistory, clearCsvQueue, getCsvQueueEnabled, setCsvQueueEnabled,
         getPitchDrafts, savePitchDraft, deletePitchDraft,
         getAutopilotEnabled, getAutopilotState, setAutopilotEnabled, saveAutopilotFeedback,
         getAutopilotTarget, setAutopilotTarget,
         fetchReviewQueue, validateReviewItem, rejectReviewItem, updateReviewItem, clearPendingProspects,
         getDailyValidationCount, getApiUsageToday, getCustomPrompt, setCustomPrompt,
         insertPitchFeedback, matchPitchFeedback, getApiUsageForProvider,
         setDomainGeo, getDomainGeo }                                                          from "../modules/supabase.js";
import { voyageEmbed, buildPitchContext }                                                    from "../modules/voyageEmbed.js";
import { sendEmail, getGmailProfile, getGmailSignature, getGmailToken, clearAllCachedTokens, appendClosingIfMissing } from "../modules/gmail.js";
import { getKeywords, searchGoogleForDomain }                                                  from "../modules/keywords.js";
import { scoreProspect }                                                                        from "../modules/scoring.js";
import { CONFIG }                                                                               from "../config.js";
import { callProxy, setProxyAuth, onRapidApiCapReached, onRapidApiHit, getRapidApiMonthlyStatus } from "../modules/apiProxy.js";
import { isAdminEmail, getRole, TEAM_EMAILS }                                                   from "../modules/roles.js";
import { DIEGO_VOICE_PROMPT, GLOBAL_PROMPT_KEY }                                                from "../modules/diegoVoicePrompt.js";
import { fetchAllUserLimits, fetchUserLimit, upsertUserLimit, deleteUserLimit,
         getUserDailyUsage, incrementUserDailyCounter, checkUserCanDo }                          from "../modules/userLimits.js";
import { startUsageSession, endUsageSession, fetchUsageStats }                                   from "../modules/usageTracking.js";
import { lockProspect, getActiveProspectLock, unlockProspect, createHandoff,
         fetchPendingHandoffsForUser, updateHandoffStatus,
         setVacationStatus, getUserStatus, expireOldHandoffs }                                   from "../modules/coordination.js";
import { logAuditEvent }                                                                         from "../modules/auditLog.js";

// ============================================================
// DEMO MODE
// Cuando el admin lo prende, ningún hit a APIs pagas se ejecuta.
// Devuelve datos mockeados realistas para presentar la herramienta sin gastar.
// ============================================================
let _demoModeEnabled = false;
export function setDemoMode(on) {
  _demoModeEnabled = !!on;
  chrome.storage.local.set({ _demoMode: _demoModeEnabled }).catch(() => {});
  document.body.setAttribute("data-demo-mode", _demoModeEnabled ? "on" : "off");
  const indicator = document.getElementById("demo-mode-indicator");
  if (indicator) indicator.style.display = _demoModeEnabled ? "inline-block" : "none";
}
export function isDemoMode() { return _demoModeEnabled; }

async function loadDemoModeFromStorage() {
  try {
    const { _demoMode } = await chrome.storage.local.get("_demoMode");
    setDemoMode(!!_demoMode);
  } catch {}
}

// ============================================================
// QUICK ACCESS — últimos 20 dominios analizados
// ============================================================
async function getRecentDomains(n = 20) {
  try {
    const { _recentDomains = [] } = await chrome.storage.local.get("_recentDomains");
    return _recentDomains.slice(0, n);
  } catch { return []; }
}
async function pushRecentDomain(domain) {
  if (!domain) return;
  try {
    const { _recentDomains = [] } = await chrome.storage.local.get("_recentDomains");
    const filtered = _recentDomains.filter(d => d !== domain);
    filtered.unshift(domain);
    await chrome.storage.local.set({ _recentDomains: filtered.slice(0, 20) });
  } catch {}
}

// ============================================================
// COORDINACIÓN ENTRE MBs (lock + handoff + vacation)
// ============================================================
// Liberar locks al cerrar la toolbar (fix audit Bug 1)
// Sin esto, los locks duraban 30 min completos aunque el MB cierre la pestaña.
// Ahora se libera apenas el side panel se descarga.
function _wireProspectLockCleanup() {
  if (window._lockCleanupWired) return;
  window._lockCleanupWired = true;
  const release = () => {
    if (state.domain && state.accessToken && state.loginEmail) {
      try {
        // sendBeacon es la única manera confiable de mandar request en unload
        const url  = `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_prospect_locks?domain=eq.${encodeURIComponent(state.domain)}&locked_by=eq.${encodeURIComponent(state.loginEmail)}`;
        // Fallback con keepalive fetch (sendBeacon no soporta DELETE)
        fetch(url, {
          method: "DELETE",
          headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` },
          keepalive: true,
        }).catch(() => {});
      } catch {}
    }
  };
  window.addEventListener("beforeunload", release);
  window.addEventListener("pagehide", release);
}

async function checkProspectLock() {
  _wireProspectLockCleanup();
  if (!state.domain || !state.accessToken) return;
  const lock = await getActiveProspectLock(state.accessToken, state.domain);
  const el = document.getElementById("duplicate-result");
  if (!lock) {
    // Auto-claim: si está libre y vos lo abriste, te lo asignamos
    lockProspect(state.accessToken, state.domain, state.loginEmail).catch(() => {});
    return;
  }
  if (lock.locked_by.toLowerCase() === (state.loginEmail || "").toLowerCase()) return;
  // Otro MB lo tiene tomado → mostrar warning
  if (el) {
    const minutesLeft = Math.max(0, Math.round((new Date(lock.expires_at) - Date.now()) / 60_000));
    const warn = document.createElement("div");
    warn.className = "lock-warning";
    warn.innerHTML = `🔒 ${esc(lock.locked_by)} está trabajando este prospecto (${minutesLeft} min restantes). Coordiná antes de pushear.`;
    el.parentElement?.insertBefore(warn, el.nextSibling);
  }
}

function setupVacationToggle() {
  const settings = document.getElementById("settings-modal");
  if (!settings) return;
  // Si ya existe la sección, no duplicarla
  if (document.getElementById("vacation-toggle-section")) return;
  const sec = document.createElement("div");
  sec.id = "vacation-toggle-section";
  sec.innerHTML = `
    <h3 style="margin-top:16px;font-size:13px">🏖️ Estado</h3>
    <label style="display:flex;align-items:center;gap:8px;font-size:12px;margin:6px 0">
      <input type="checkbox" id="vacation-checkbox" />
      En vacaciones / no asignar nuevos leads
    </label>
    <input type="date" id="vacation-until" class="form-input" style="font-size:11px" placeholder="Hasta cuándo" />
    <div id="vacation-status" style="font-size:10px;color:#94a3b8;margin-top:4px"></div>
  `;
  settings.querySelector(".modal-content")?.appendChild(sec) || settings.appendChild(sec);

  const cb     = document.getElementById("vacation-checkbox");
  const until  = document.getElementById("vacation-until");
  const status = document.getElementById("vacation-status");

  // Cargar estado actual
  getUserStatus(state.accessToken, state.loginEmail).then(s => {
    if (s?.vacation_until) {
      cb.checked = true;
      until.value = s.vacation_until;
      status.textContent = `Estás marcado como en vacaciones hasta ${s.vacation_until}.`;
    }
  }).catch(() => {});

  const save = async () => {
    const u = cb.checked ? (until.value || null) : null;
    await setVacationStatus(state.accessToken, state.loginEmail, u);
    status.textContent = u ? `✅ Marcado como en vacaciones hasta ${u}.` : "✅ Activo.";
  };
  cb.addEventListener("change", save);
  until.addEventListener("change", save);
}

async function checkPendingHandoffs() {
  if (!state.loginEmail || !state.accessToken) return;
  const pending = await fetchPendingHandoffsForUser(state.accessToken, state.loginEmail);
  if (!pending.length) return;
  // Mostrar como banner amarillo
  const header = document.querySelector(".header");
  if (!header || document.getElementById("handoff-banner")) return;
  const banner = document.createElement("div");
  banner.id = "handoff-banner";
  banner.className = "handoff-banner";
  banner.innerHTML = `
    <strong>📨 ${pending.length} hand-off${pending.length > 1 ? "s" : ""} pendiente${pending.length > 1 ? "s" : ""}:</strong>
    <ul style="margin:6px 0 0;padding-left:20px;font-size:11px">
      ${pending.slice(0, 5).map(h => `
        <li>
          ${esc(h.from_email)} → vos · <strong>${esc(h.domain)}</strong>${h.note ? ` · "${esc(h.note)}"` : ""}
          <button data-handoff-id="${h.id}" data-action="accepted" class="handoff-btn-accept">✅</button>
          <button data-handoff-id="${h.id}" data-action="rejected" class="handoff-btn-reject">❌</button>
        </li>
      `).join("")}
    </ul>
  `;
  header.parentElement?.insertBefore(banner, header.nextSibling);
  banner.addEventListener("click", async (e) => {
    const id = e.target.dataset?.handoffId;
    const action = e.target.dataset?.action;
    if (!id || !action) return;
    await updateHandoffStatus(state.accessToken, id, action);
    e.target.closest("li").remove();
    if (!banner.querySelector("li")) banner.remove();
  });
}

// ============================================================
// AUTO-REFRESH ON URL CHANGE
// El side panel queda abierto al navegar de tab/URL. Detectamos cambios
// y disparamos re-análisis después de 3s estables (evita refresh constante
// si el user navega rápido).
// ============================================================
let _autoRefreshTimer = null;
let _autoRefreshLastDomain = null;
let _autoRefreshWired = false;

function setupAutoRefreshOnUrlChange() {
  if (_autoRefreshWired) return;
  _autoRefreshWired = true;
  _autoRefreshLastDomain = state.domain;

  const scheduleRecheck = (newUrl, tabId) => {
    if (!newUrl || newUrl.startsWith("chrome://") || newUrl.startsWith("about:")) return;
    const newDomain = extractDomain(newUrl);
    if (!newDomain || newDomain === _autoRefreshLastDomain) return;
    clearTimeout(_autoRefreshTimer);
    _autoRefreshTimer = setTimeout(async () => {
      // Re-confirmar que la tab activa sigue siendo esta URL (si el user navegó otra vez, esperamos)
      try {
        const [active] = await chrome.tabs.query({ active: true, currentWindow: true });
        if (!active || active.id !== tabId) return; // user cambió de tab
        if (extractDomain(active.url) !== newDomain) return; // URL cambió de nuevo
        _autoRefreshLastDomain = newDomain;
        // Re-trigger analysis sobre la nueva URL
        state.tabId  = active.id;
        state.url    = active.url;
        state.domain = newDomain;
        const siteEl = document.getElementById("site-url");
        if (siteEl) siteEl.textContent = newDomain;
        const seedEl = document.getElementById("cascade-seed");
        if (seedEl) seedEl.value = newDomain;
        // Limpiar resultados previos antes de re-analizar
        resetAnalysisUI();
        // Re-correr el flujo de análisis
        runAnalysisPipeline();
      } catch (e) { console.warn("[auto-refresh] failed:", e.message); }
    }, 3000);
  };

  chrome.tabs.onUpdated.addListener((tabId, changeInfo) => {
    if (changeInfo.url) scheduleRecheck(changeInfo.url, tabId);
  });
  chrome.tabs.onActivated.addListener(async ({ tabId }) => {
    try {
      const tab = await chrome.tabs.get(tabId);
      if (tab?.url) scheduleRecheck(tab.url, tabId);
    } catch {}
  });
}

function resetAnalysisUI() {
  // Limpiar los resultados visibles para evitar confundir al user con datos del dominio anterior
  ["traffic-result", "traffic-breakdown", "traffic-countries", "traffic-category", "traffic-filter",
   "duplicate-result", "email-result", "pitch-text", "form-pv-display"].forEach(id => {
    const el = document.getElementById(id);
    if (el) {
      if (el.tagName === "INPUT" || el.tagName === "TEXTAREA") el.value = "";
      else el.textContent = "";
    }
  });
}

async function forceRefreshAnalysis() {
  // Borrar cache local del dominio actual antes de re-correr la pipeline
  if (state.domain) {
    try {
      const all = await chrome.storage.local.get(null);
      const keys = Object.keys(all).filter(k => k.includes(state.domain));
      if (keys.length) await chrome.storage.local.remove(keys);
    } catch {}
  }
  resetAnalysisUI();
  runAnalysisPipeline();
}

async function checkPersonalQuotaWarning() {
  if (!state.loginEmail || !state.accessToken) return;
  try {
    const limit = await fetchUserLimit(state.accessToken, state.loginEmail);
    if (!limit?.monthly_api_cap) return; // sin cap personal, no aplica
    // El usage personal lo aproximamos con la suma del mes en toolbar_api_usage.
    const period = new Date().toISOString().slice(0, 7);
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage?user_email=eq.${encodeURIComponent(state.loginEmail)}&day=gte.${period}-01&select=by_provider`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    const used = rows.reduce((acc, r) => acc + parseInt(r.by_provider?.rapidapi || 0, 10), 0);
    const pct = (used / limit.monthly_api_cap) * 100;
    if (pct >= 80) showPersonalQuotaBanner({ used, limit: limit.monthly_api_cap, pct });
  } catch {}
}

function showPersonalQuotaBanner({ used, limit, pct }) {
  const banner = document.getElementById("rapidapi-cap-banner");
  if (!banner || banner.style.display !== "none") return; // ya hay banner global
  banner.style.display = "flex";
  banner.classList.add(pct >= 100 ? "cap-reached" : "cap-warning");
  document.getElementById("cap-banner-icon").textContent = pct >= 100 ? "⛔" : "⚠️";
  document.getElementById("cap-banner-title").textContent = `Tu cap personal está al ${Math.round(pct)}%`;
  document.getElementById("cap-banner-detail").textContent = ` — ${used.toLocaleString()} / ${limit.toLocaleString()} hits este mes.`;
}

function runAnalysisPipeline() {
  runDuplicateCheck().catch(() => {});
  runTrafficCheck().catch(() => {});
  runEmailScraper().catch(() => {});
  if (typeof runAuditCheck === "function")     runAuditCheck().catch(() => {});
  if (typeof runBannerDetection === "function") runBannerDetection().catch(() => {});
  if (typeof runPageContext === "function")    runPageContext().catch(() => {});
}

// ============================================================
// ADMIN VIEW TOGGLE
// El admin tiene la misma UI que el media buyer, pero puede activar el modo
// admin con triple-click en el logo (no llama la atención y no clutterea
// la UI normal).
// ============================================================
function showAdminDeniedToast() {
  // Toast efímero que se auto-destruye. No alert() porque interrumpe demasiado.
  let t = document.getElementById("admin-denied-toast");
  if (t) { t.remove(); }
  t = document.createElement("div");
  t.id = "admin-denied-toast";
  t.style.cssText = `
    position: fixed; top: 12px; left: 50%; transform: translateX(-50%);
    background: #fee2e2; color: #991b1b; border: 1px solid #ef4444;
    padding: 10px 16px; border-radius: 8px; font-size: 12px; font-weight: 600;
    z-index: 99999; box-shadow: 0 4px 12px rgba(0,0,0,0.15);
  `;
  t.textContent = "🔒 Solo el admin (mgargiulo@adeqmedia.com) puede abrir este panel.";
  document.body.appendChild(t);
  setTimeout(() => { t.style.opacity = "0"; t.style.transition = "opacity 0.4s"; }, 2200);
  setTimeout(() => { t.remove(); }, 2700);
}

function wireAdminViewToggle() {
  const logo = document.querySelector(".logo");
  if (!logo || logo._adminWired) return;
  logo._adminWired = true;
  logo.style.cursor = "pointer";
  logo.title = "Triple-click para abrir Admin Panel";

  let clicks = 0; let timer = null;
  logo.addEventListener("click", () => {
    clicks++;
    clearTimeout(timer);
    timer = setTimeout(() => { clicks = 0; }, 600);
    if (clicks >= 3) {
      clicks = 0;
      toggleAdminView();
    }
  });
}

function toggleAdminView() {
  // Solo el admin (mgargiulo@adeqmedia.com) puede abrir el panel.
  // Los demás MBs reciben feedback visible si triple-clickean por accidente.
  if (!isAdminEmail(state.loginEmail)) {
    showAdminDeniedToast();
    return;
  }
  state.adminViewActive = !state.adminViewActive;
  document.body.setAttribute("data-admin-view", state.adminViewActive ? "on" : "off");
  const panel = document.getElementById("admin-panel");
  if (panel) panel.style.display = state.adminViewActive ? "block" : "none";
  const logo = document.querySelector(".logo .logo-text");
  if (logo) logo.style.color = state.adminViewActive ? "#fbbf24" : "";
  if (state.adminViewActive && !state._adminInited) {
    initAdminPanel();
    state._adminInited = true;
  }
}

// ============================================================
// ADMIN PANEL — init + lógica de tabs
// ============================================================
function initAdminPanel() {
  // Tab switcher
  document.querySelectorAll(".admin-tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const tab = btn.dataset.adminTab;
      document.querySelectorAll(".admin-tab-btn").forEach(b => b.classList.toggle("active", b === btn));
      document.querySelectorAll(".admin-tab-content").forEach(c => {
        c.classList.toggle("active", c.id === `admin-tab-${tab}`);
      });
      if (tab === "activity")  loadAdminActivity();
      if (tab === "agent")     loadAdminAgent();
      if (tab === "limits")    loadAdminLimits();
      if (tab === "blocklist") loadAdminBlocklist();
    });
  });
  // Wire activity filters
  document.getElementById("admin-filter-period")?.addEventListener("change", loadAdminActivity);
  document.getElementById("admin-filter-user")?.addEventListener("change", loadAdminActivity);
  document.getElementById("admin-refresh-stats")?.addEventListener("click", loadAdminActivity);
  // Wire limits
  document.getElementById("admin-add-user-limit")?.addEventListener("click", () => addAdminLimitRow());
  // Wire blocklist
  document.getElementById("admin-blocklist-save")?.addEventListener("click", saveAdminBlocklist);
  document.getElementById("admin-blocklist-csv")?.addEventListener("change", handleBlocklistCsvUpload);
  // Wire reset cache button
  document.getElementById("admin-reset-cache-btn")?.addEventListener("click", resetTrafficCacheAboveThreshold);
  // Wire agent
  document.getElementById("agent-toggle")?.addEventListener("change", toggleAgent);
  document.getElementById("agent-cfg-save")?.addEventListener("click", saveAgentThresholds);
  document.getElementById("agent-pause-1h")?.addEventListener("click", pauseAgent1h);
  document.getElementById("agent-focus-save")?.addEventListener("click", saveAgentFocus);

  loadAdminActivity();
}

async function resetTrafficCacheAboveThreshold() {
  const threshold = parseInt(document.getElementById("admin-reset-cache-threshold").value, 10) || 400000;
  const status = document.getElementById("admin-reset-cache-status");
  if (!confirm(`¿Borrar TODOS los dominios cacheados con visits ≥ ${threshold.toLocaleString()}?\n\nEsto fuerza al equipo a re-analizarlos (gastará API).`)) return;
  status.textContent = "⏳ Borrando...";
  try {
    // Borrar via PostgREST con filtros sobre el JSONB.
    // PostgREST no soporta operaciones JSON >= directamente — usamos RPC alternativa:
    // delete WHERE data->>'rawVisits' >= threshold.
    // En PostgREST esto se hace así (filter en columna data sub-key):
    const headers = {
      "apikey": CONFIG.SUPABASE_ANON_KEY,
      "Authorization": `Bearer ${state.accessToken}`,
      "Prefer": "return=representation",
    };
    // Trick: traer todos los rows, filtrar client-side, deletear los matches.
    // Funciona OK porque la cache no debería tener millones de rows.
    const listRes = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_traffic_cache?select=domain,data`,
      { headers }
    );
    if (!listRes.ok) { status.textContent = "❌ No se pudo leer la cache."; return; }
    const rows = await listRes.json();
    const toDelete = rows.filter(r => {
      const d = r.data || {};
      const visits = Math.max(
        parseInt(d.rawVisits || 0, 10),
        parseInt(d.visits || 0, 10),
        parseInt(d.pageViews || 0, 10),
        parseInt(d.monthly || 0, 10),
      );
      return visits >= threshold;
    });
    if (!toDelete.length) { status.textContent = `✅ No hay dominios cacheados con visits ≥ ${threshold.toLocaleString()}.`; return; }

    // Delete en bulk usando in.()
    const domains = toDelete.map(r => r.domain);
    const chunks = [];
    for (let i = 0; i < domains.length; i += 50) chunks.push(domains.slice(i, i + 50));
    let deleted = 0;
    for (const chunk of chunks) {
      const inList = chunk.map(d => `"${encodeURIComponent(d)}"`).join(",");
      const delRes = await fetch(
        `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_traffic_cache?domain=in.(${inList})`,
        { method: "DELETE", headers }
      );
      if (delRes.ok) deleted += chunk.length;
    }
    status.textContent = `✅ ${deleted} dominios borrados de la cache compartida. El equipo los re-analizará en el próximo análisis.`;
    logAuditEvent(state.accessToken, {
      user_email: state.loginEmail, action: "reset_traffic_cache",
      details: { threshold, deleted, sample: domains.slice(0, 10) },
    });
  } catch (e) {
    status.textContent = "❌ Error: " + e.message;
  }
}

// ── Limits tab ─────────────────────────────────────────────
async function loadAdminLimits() {
  const list = document.getElementById("admin-limits-list");
  if (!list) return;
  list.innerHTML = '<div class="admin-help">Cargando...</div>';
  const limits = await fetchAllUserLimits(state.accessToken);
  list.innerHTML = "";
  // Header con labels de cada columna
  const header = document.createElement("div");
  header.className = "admin-limit-row";
  header.style.background = "transparent";
  header.style.border = "none";
  header.style.padding = "0 4px";
  header.innerHTML = `
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px">Email</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center">API/mes</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center">AP min/sesión</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center">AP prosp/día</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center">AP on</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center">Save</span>
    <span></span>
  `;
  list.appendChild(header);
  // Mergear: existentes en DB + cualquier TEAM_EMAILS que falte.
  // Así el admin siempre ve filas para todo el equipo, aunque no estén en DB.
  const seen = new Set();
  limits.forEach(l => { if (l.user_email) seen.add(l.user_email.toLowerCase()); list.appendChild(buildLimitRow(l)); });
  TEAM_EMAILS.forEach(email => {
    if (!seen.has(email.toLowerCase())) {
      list.appendChild(buildLimitRow({
        user_email: email,
        autopilot_enabled: true,
        monthly_api_cap: null,
        daily_emails_cap: 100,
        daily_monday_cap: 100,
      }, false)); // false = no es nuevo (email readonly), pero sin row en DB todavía
    }
  });
}

function addAdminLimitRow() {
  const list = document.getElementById("admin-limits-list");
  if (!list) return;
  list.appendChild(buildLimitRow({
    user_email: "",
    autopilot_enabled: true,
    monthly_api_cap: null,
    daily_emails_cap: 100,
    daily_monday_cap: 100,
  }, true));
}

function buildLimitRow(l, isNew = false) {
  const row = document.createElement("div");
  row.className = "admin-limit-row";
  row.innerHTML = `
    <input type="email" class="form-input lim-email" value="${esc(l.user_email)}" placeholder="user@adeqmedia.com" ${isNew ? "" : "readonly"} />
    <input type="number" class="form-input lim-monthly" value="${l.monthly_api_cap || ""}" placeholder="API/mes" min="0" title="Cap mensual de RapidAPI hits para este usuario (vacío = sin cap individual)" />
    <input type="number" class="form-input lim-ap-mins" value="${l.autopilot_daily_minutes ?? 60}" placeholder="min" min="5" max="240" title="Duración máxima de UNA sesión de autopilot, en minutos" />
    <input type="number" class="form-input lim-ap-prospects" value="${l.autopilot_daily_prospects ?? 75}" placeholder="prosp" min="0" max="500" title="Cantidad máxima de prospectos procesados por día por este usuario en autopilot" />
    <span class="lim-autopilot ${l.autopilot_enabled ? "toggle-yes" : "toggle-no"}" title="Click para alternar Autopilot ON/OFF">${l.autopilot_enabled ? "AP ✓" : "AP ✗"}</span>
    <button class="save-btn" title="Guardar cambios">💾</button>
    <button class="del-btn" title="Eliminar">×</button>
  `;
  // Click en Save → fuerza guardado + feedback visual
  row.querySelector(".save-btn").addEventListener("click", async () => {
    await saveLimitRow(row);
    const btn = row.querySelector(".save-btn");
    btn.textContent = "✓"; btn.classList.add("saved");
    setTimeout(() => { btn.textContent = "💾"; btn.classList.remove("saved"); }, 1500);
  });
  // Toggle autopilot
  row.querySelector(".lim-autopilot").addEventListener("click", (e) => {
    const isOn = e.target.classList.contains("toggle-yes");
    e.target.classList.toggle("toggle-yes", !isOn);
    e.target.classList.toggle("toggle-no", isOn);
    e.target.textContent = !isOn ? "AP ✓" : "AP ✗";
    if (!isNew) saveLimitRow(row);
  });
  // Delete
  row.querySelector(".del-btn").addEventListener("click", async () => {
    const email = row.querySelector(".lim-email").value;
    if (email && confirm(`Eliminar límites de ${email}?`)) {
      await deleteUserLimit(state.accessToken, email);
    }
    row.remove();
  });
  // Auto-save on blur de cualquier input numérico o de email
  row.querySelectorAll("input[type=number], input[type=email]").forEach(inp => {
    inp.addEventListener("change", () => saveLimitRow(row));
  });
  return row;
}

function addAdminLimitRowExisting(row) { /* placeholder por si se necesita más adelante */ }

async function saveLimitRow(row) {
  const email = row.querySelector(".lim-email").value.trim().toLowerCase();
  if (!email) return;
  const apMins = parseInt(row.querySelector(".lim-ap-mins").value, 10);
  const apProsp = parseInt(row.querySelector(".lim-ap-prospects").value, 10);
  const limit = {
    user_email:                email,
    autopilot_enabled:         row.querySelector(".lim-autopilot").classList.contains("toggle-yes"),
    monthly_api_cap:           parseInt(row.querySelector(".lim-monthly").value, 10) || null,
    autopilot_daily_minutes:   isNaN(apMins) || apMins < 5 ? 60 : Math.min(apMins, 240),
    autopilot_daily_prospects: isNaN(apProsp) || apProsp < 0 ? 75 : Math.min(apProsp, 500),
    // Caps de email/monday quedaron descontinuados por simplicidad; mando 999999 para que no bloqueen.
    daily_emails_cap:          999999,
    daily_monday_cap:          999999,
  };
  const ok = await upsertUserLimit(state.accessToken, limit);
  row.style.borderColor = ok ? "#34d399" : "#f87171";
  setTimeout(() => { row.style.borderColor = ""; }, 800);
  // Audit
  if (ok) logAuditEvent(state.accessToken, {
    user_email: state.loginEmail, action: "set_user_limit",
    target: email, details: limit,
  });
}

// ── Blocklist tab ──────────────────────────────────────────
// ════════════════════════════════════════════════════════════════
// 🤖 ADMIN AGENT TAB
// ════════════════════════════════════════════════════════════════

async function _readAgentConfig() {
  // Lee toolbar_config keys que empiezan con agent_
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=in.(agent_enabled_users,agent_threshold_traffic,agent_threshold_score,agent_max_per_day,agent_active_hours_start,agent_active_hours_end,agent_paused_until,agent_focus_config)&select=key,value`,
      { headers }
    );
    const rows = await res.json();
    const cfg = {};
    rows.forEach(r => { cfg[r.key] = r.value; });
    return cfg;
  } catch { return {}; }
}

async function _writeAgentConfig(updates) {
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" };
  // upsert per key
  const promises = Object.entries(updates).map(([key, value]) =>
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config`, {
      method: "POST", headers, body: JSON.stringify({ key, value: String(value) }),
    })
  );
  await Promise.all(promises);
}

async function loadAdminAgent() {
  const cfg = await _readAgentConfig();
  let users = [];
  try { users = JSON.parse(cfg.agent_enabled_users || "[]"); } catch {}
  const myEmail = (state.loginEmail || "").toLowerCase();
  const enabled = users.map(u => u.toLowerCase()).includes(myEmail);

  // Toggle state
  const toggle = document.getElementById("agent-toggle");
  if (toggle) toggle.checked = enabled;

  // Status text — incluye chequeo auto-on/off por hora España
  const statusEl = document.getElementById("agent-toggle-status");
  if (statusEl) {
    const pausedUntil = cfg.agent_paused_until ? new Date(cfg.agent_paused_until) : null;
    const isPaused = pausedUntil && pausedUntil > new Date();
    const startH = parseInt(cfg.agent_active_hours_start || "9", 10);
    const endH   = parseInt(cfg.agent_active_hours_end   || "20", 10);
    // Calcular hora actual España
    let spainH = 0;
    try {
      const fmt = new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", hour: "numeric", hour12: false });
      spainH = parseInt(fmt.format(new Date()), 10);
    } catch {}
    const inActiveWindow = startH < endH ? (spainH >= startH && spainH < endH) : (spainH >= startH || spainH < endH);
    if (isPaused) {
      const minLeft = Math.round((pausedUntil - Date.now()) / 60000);
      statusEl.innerHTML = `⏸ <strong style="color:#f87171">Pausado por ${minLeft}min</strong> (kill switch o pause manual)`;
    } else if (enabled && !inActiveWindow) {
      statusEl.innerHTML = `🌙 Master ON, pero <strong style="color:#fbbf24">FUERA de horario activo</strong> (${startH}h-${endH}h España). Auto-resume cuando entre el horario. Hora España actual: <strong>${spainH}h</strong>.`;
    } else if (enabled && inActiveWindow) {
      statusEl.innerHTML = `🟢 <strong style="color:#34d399">Activo</strong>. Procesa cada ~5min. Mandando como <strong>${esc(myEmail)}</strong>. Horario activo: ${startH}h-${endH}h España.`;
    } else {
      statusEl.innerHTML = `⚪ Inactivo. Activá el toggle (master switch) para que arranque automático en horario activo.`;
    }
  }

  // Inputs de threshold
  const setVal = (id, v, dflt) => { const el = document.getElementById(id); if (el) el.value = v || dflt; };
  setVal("agent-cfg-traffic",      cfg.agent_threshold_traffic,  500000);
  setVal("agent-cfg-score",        cfg.agent_threshold_score,     40);
  setVal("agent-cfg-max",          cfg.agent_max_per_day,         20);
  setVal("agent-cfg-active-start", cfg.agent_active_hours_start,   9);
  setVal("agent-cfg-active-end",   cfg.agent_active_hours_end,    20);
  document.getElementById("agent-stat-cap").textContent = cfg.agent_max_per_day || "20";

  // Focus config (JSON)
  let focus = { geos_priority: [], geos_excluded: [], categories_priority: [], weekly_target: 0, daily_override: 0 };
  try { focus = { ...focus, ...JSON.parse(cfg.agent_focus_config || "{}") }; } catch {}
  const setStr = (id, v) => { const el = document.getElementById(id); if (el) el.value = (Array.isArray(v) ? v : []).join(","); };
  setStr("agent-focus-geos-priority",  focus.geos_priority);
  setStr("agent-focus-geos-excluded",  focus.geos_excluded);
  setStr("agent-focus-categories",     focus.categories_priority);
  setVal("agent-focus-daily",  focus.daily_override, 0);
  setVal("agent-focus-weekly", focus.weekly_target,  0);

  // Stats hoy
  await _refreshAgentStats(myEmail);
  await _refreshAgentFeed();
}

async function _refreshAgentStats(userEmail) {
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
  const cutoff24 = new Date(Date.now() - 24 * 3600_000).toISOString();
  const startToday = new Date(); startToday.setHours(0,0,0,0);
  try {
    const [sentRes, skipRes, failRes] = await Promise.all([
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(userEmail)}&action=eq.sent&created_at=gte.${startToday.toISOString()}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(userEmail)}&action=eq.skipped&created_at=gte.${cutoff24}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(userEmail)}&action=eq.failed&created_at=gte.${cutoff24}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
    ]);
    const parseCount = (res) => { const m = (res.headers.get("content-range") || "").match(/\/(\d+)$/); return m ? parseInt(m[1]) : 0; };
    document.getElementById("agent-stat-sent").textContent    = parseCount(sentRes);
    document.getElementById("agent-stat-skipped").textContent = parseCount(skipRes);
    document.getElementById("agent-stat-failed").textContent  = parseCount(failRes);
  } catch {}
}

async function _refreshAgentFeed() {
  const wrap = document.getElementById("agent-actions-feed");
  if (!wrap) return;
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?select=*&order=created_at.desc&limit=50`,
      { headers }
    );
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) { wrap.innerHTML = '<div style="color:#94a3b8">Sin actividad aún. Activá el toggle para arrancar.</div>'; return; }
    const icons = { sent: "✅", skipped: "⏭", failed: "❌", kill_switch: "🚨" };
    wrap.innerHTML = rows.map(r => {
      const time = new Date(r.created_at).toLocaleString("es-AR", { hour: "2-digit", minute: "2-digit", day: "2-digit", month: "2-digit" });
      const icon = icons[r.action] || "·";
      const reasonStr = r.reason ? `<span style="color:#94a3b8"> · ${esc(r.reason)}</span>` : "";
      const subjectStr = r.pitch_subject ? `<br/><span style="color:#cbd5e1;margin-left:18px;font-style:italic">"${esc(r.pitch_subject.substring(0, 60))}"</span>` : "";
      const colorMap = { sent: "#34d399", skipped: "#fbbf24", failed: "#f87171", kill_switch: "#ef4444" };
      const color = colorMap[r.action] || "#cbd5e1";
      return `<div style="padding:3px 0;border-bottom:1px solid #334155">
        <span style="color:${color}">${icon}</span>
        <span style="color:#94a3b8;font-size:9px">[${esc(time)}]</span>
        <strong style="color:#e2e8f0">${esc(r.domain)}</strong>${reasonStr}
        ${subjectStr}
      </div>`;
    }).join("");
  } catch (e) {
    wrap.innerHTML = `<div style="color:#f87171">Error: ${esc(e.message || String(e))}</div>`;
  }
}

async function toggleAgent(e) {
  const enabled = e.target.checked;
  const myEmail = (state.loginEmail || "").toLowerCase();
  const cfg = await _readAgentConfig();
  let users = [];
  try { users = JSON.parse(cfg.agent_enabled_users || "[]"); } catch {}
  users = users.map(u => u.toLowerCase()).filter(u => u !== myEmail);
  if (enabled) users.push(myEmail);
  await _writeAgentConfig({ agent_enabled_users: JSON.stringify(users) });
  await loadAdminAgent();
  showToast(enabled ? "🟢 Agent activado" : "⚪ Agent desactivado", "info");
}

async function saveAgentThresholds() {
  const updates = {
    agent_threshold_traffic:  parseInt(document.getElementById("agent-cfg-traffic").value, 10) || 500000,
    agent_threshold_score:    parseInt(document.getElementById("agent-cfg-score").value, 10) || 40,
    agent_max_per_day:        parseInt(document.getElementById("agent-cfg-max").value, 10) || 20,
    agent_active_hours_start: parseInt(document.getElementById("agent-cfg-active-start").value, 10) || 9,
    agent_active_hours_end:   parseInt(document.getElementById("agent-cfg-active-end").value, 10) || 20,
  };
  await _writeAgentConfig(updates);
  showToast("✅ Thresholds guardados", "info");
  await loadAdminAgent();
}

async function saveAgentFocus() {
  const parseList = (id) => (document.getElementById(id).value || "")
    .split(",").map(s => s.trim()).filter(Boolean);
  const focus = {
    geos_priority:       parseList("agent-focus-geos-priority").map(s => s.toUpperCase()),
    geos_excluded:       parseList("agent-focus-geos-excluded").map(s => s.toUpperCase()),
    categories_priority: parseList("agent-focus-categories").map(s => s.toLowerCase()),
    daily_override:      parseInt(document.getElementById("agent-focus-daily").value, 10) || 0,
    weekly_target:       parseInt(document.getElementById("agent-focus-weekly").value, 10) || 0,
  };
  await _writeAgentConfig({ agent_focus_config: JSON.stringify(focus) });
  showToast("✅ Focus de la semana guardado", "info");
  await loadAdminAgent();
}

async function pauseAgent1h() {
  if (!confirm("Pausar el agent durante 1 hora?")) return;
  const pauseUntil = new Date(Date.now() + 3600_000).toISOString();
  await _writeAgentConfig({ agent_paused_until: pauseUntil });
  showToast("⏸ Agent pausado 1h", "warn");
  await loadAdminAgent();
}

async function loadAdminBlocklist() {
  const ta = document.getElementById("admin-blocklist-text");
  const status = document.getElementById("admin-blocklist-status");
  if (!ta) return;
  status.textContent = "Cargando...";
  try {
    const { TOP_500_BLOCKED } = await import("../modules/blockedDomainsTop500.js");
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_url_blocklist?select=domain&order=domain`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
    );
    const adminList = res.ok ? (await res.json()).map(r => r.domain) : [];
    ta.value = adminList.join("\n");
    status.innerHTML = `
      <strong>${adminList.length}</strong> dominios admin custom (editables) +
      <strong>${TOP_500_BLOCKED.length}</strong> dominios baked-in (top 500 no-publishers, gratuitos).
      <br/><span style="opacity:.7">Total efectivo: ${(adminList.length + TOP_500_BLOCKED.length).toLocaleString()} dominios bloqueados pre-API.</span>
    `;
  } catch (e) { status.textContent = "Error: " + e.message; }
}

async function saveAdminBlocklist() {
  const ta = document.getElementById("admin-blocklist-text");
  const status = document.getElementById("admin-blocklist-status");
  const domains = (ta.value || "")
    .split(/[\n,]/).map(d => d.trim().toLowerCase().replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/.*$/, ""))
    .filter(d => d && d.includes("."));
  if (!domains.length) { status.textContent = "❌ Lista vacía."; return; }
  status.textContent = "⏳ Guardando...";
  try {
    // Borrar todo y re-insertar (operación admin, no es alta frecuencia)
    await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_url_blocklist?domain=neq.zzz_never_match`, {
      method: "DELETE",
      headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` },
    });
    await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_url_blocklist`, {
      method: "POST",
      headers: {
        "apikey": CONFIG.SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${state.accessToken}`,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify(domains.map(d => ({ domain: d, added_by: state.loginEmail }))),
    });
    status.textContent = `✅ ${domains.length} dominios guardados.`;
    logAuditEvent(state.accessToken, {
      user_email: state.loginEmail, action: "edit_blocklist",
      details: { count: domains.length },
    });
  } catch (e) { status.textContent = "❌ Error: " + e.message; }
}

function handleBlocklistCsvUpload(e) {
  const file = e.target.files?.[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = (ev) => {
    document.getElementById("admin-blocklist-text").value = ev.target.result;
  };
  reader.readAsText(file);
}

// ── Activity tab — fetch + render stats + chart + per-user + live feed ─
function _periodToRange(period) {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const fmt = (d) => d.toISOString().slice(0, 10);
  switch (period) {
    case "today":     return { from: fmt(today), to: fmt(today) };
    case "yesterday": { const y = new Date(today); y.setDate(y.getDate() - 1); return { from: fmt(y), to: fmt(y) }; }
    case "last7":     { const f = new Date(today); f.setDate(f.getDate() - 6); return { from: fmt(f), to: fmt(today) }; }
    case "last30":    { const f = new Date(today); f.setDate(f.getDate() - 29); return { from: fmt(f), to: fmt(today) }; }
    case "this_month": { const f = new Date(now.getFullYear(), now.getMonth(), 1); return { from: fmt(f), to: fmt(today) }; }
    case "last_month": {
      const f = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const t = new Date(now.getFullYear(), now.getMonth(), 0);
      return { from: fmt(f), to: fmt(t) };
    }
    default: return { from: fmt(today), to: fmt(today) };
  }
}

async function loadAdminActivity() {
  const summary = document.getElementById("admin-stats-summary");
  if (summary) summary.querySelectorAll(".stat-num").forEach(el => { el.textContent = "..."; });
  const period = document.getElementById("admin-filter-period")?.value || "last7";
  const userFilter = document.getElementById("admin-filter-user")?.value || "";
  const { from, to } = _periodToRange(period);

  // Cargar lista de usuarios para el filtro (1 vez)
  await populateUserFilter();

  // Fetch en paralelo: historial + sendtrack + api_usage
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
  // toolbar_historial usa media_buyer (NO user_email) — antes el filtro era erróneo y devolvía 0.
  const histUserClause   = userFilter ? `&media_buyer=eq.${encodeURIComponent(userFilter)}` : "";
  const queueUserClause  = userFilter ? `&created_by=eq.${encodeURIComponent(userFilter)}` : "";
  const usageUserClause  = userFilter ? `&user_email=eq.${encodeURIComponent(userFilter)}` : "";

  const [histRes, trackRes, usageRes, sessionsRes, queueRes] = await Promise.all([
    // toolbar_historial — sites analizados manualmente desde toolbar.
    // Filtro por created_at (timestamp confiable) en vez de date (string DD/MM/YYYY).
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_historial?created_at=gte.${from}&created_at=lte.${to}T23:59:59${histUserClause}&select=*&order=created_at.desc&limit=2000`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_sendtrack?send_date=gte.${from}&send_date=lte.${to}&select=domain,send_date`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage?day=gte.${from}&day=lte.${to}${usageUserClause}&select=*`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
    fetchUsageStats(state.accessToken, { from, to, userEmail: userFilter }),
    // toolbar_review_queue — prospects agregados por autopilot + CSV + Monday refresh.
    // Esta es la fuente real del trabajo del worker (historial NO refleja autopilot).
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_review_queue?created_at=gte.${from}&created_at=lte.${to}T23:59:59${queueUserClause}&select=domain,traffic,geo,category,score,source,status,created_by,created_at,validated_by,validated_at&order=created_at.desc&limit=3000`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
  ]);

  // ── Métricas ────────────────────────────────────────────────
  // sites = analyses manuales (historial) + prospects agregados por worker (review_queue, dedup por domain).
  const histDomains    = new Set(histRes.map(h => (h.domain || "").toLowerCase()).filter(Boolean));
  const queueDomains   = new Set(queueRes.map(q => (q.domain || "").toLowerCase()).filter(Boolean));
  const allDomains     = new Set([...histDomains, ...queueDomains]);
  const sites          = allDomains.size;
  const emails         = usageRes.reduce((acc, r) => acc + parseInt(r.by_provider?._emails_sent || 0, 10), 0);
  const monday         = usageRes.reduce((acc, r) => acc + parseInt(r.by_provider?._monday_pushes || 0, 10), 0);
  // toolbar_historial usa page_views/raw_visits (no 'traffic'). Combinar histórico + review_queue para tráfico.
  const trafficOf      = (h) => parseInt(h.page_views || h.raw_visits || h.traffic || 0, 10);
  const allRows        = [...histRes, ...queueRes];
  const above500k      = allRows.filter(h => trafficOf(h) >= 500000).length;
  const below500k      = allRows.filter(h => { const t = trafficOf(h); return t > 0 && t < 500000; }).length;
  // Tiempo REAL de autopilot (sumar duration_sec de sesiones kind=autopilot)
  const apSec = sessionsRes
    .filter(s => s.kind === "autopilot" && s.duration_sec)
    .reduce((acc, s) => acc + s.duration_sec, 0);
  const apTimeStr = apSec >= 3600
    ? `${Math.floor(apSec / 3600)}h ${Math.round((apSec % 3600) / 60)}m`
    : `${Math.round(apSec / 60)}m`;

  document.getElementById("stat-sites").textContent     = sites.toLocaleString();
  document.getElementById("stat-emails").textContent    = emails.toLocaleString();
  document.getElementById("stat-monday").textContent    = monday.toLocaleString();
  document.getElementById("stat-500k-up").textContent   = above500k.toLocaleString();

  // Combinar fuentes para los renders. Normalizar review_queue rows al shape de historial
  // para que las funciones de render no necesiten saber del origen.
  const queueAsHist = queueRes.map(q => ({
    domain:       q.domain,
    media_buyer:  q.created_by || "",
    page_views:   q.traffic || 0,
    raw_visits:   q.traffic || 0,
    is_new:       true,
    date:         q.created_at,
    created_at:   q.created_at,
    geo:          q.geo,
    category:     q.category,
    source:       q.source || "autopilot",
    status:       q.status,
    score:        q.score,
  }));
  const combined = [...histRes, ...queueAsHist];

  // Chart por día
  renderAdminChart(combined, from, to);

  // Comparador de Media Buyers — at-a-glance side-by-side
  renderAdminComparator(combined, usageRes, sessionsRes);

  // Resumen narrativo por MB (cards con tips para 1:1)
  renderAdminMBSummaries(combined, usageRes, sessionsRes);
}

// ── Helper compartido: agrega métricas por user (usado por Comparator + Summaries) ──
function _aggregateByUser(historial, usage, sessions) {
  const byUser = new Map();
  TEAM_EMAILS.forEach(e => byUser.set(e.toLowerCase(), {
    sites: 0, autopilotSites: 0, manualSites: 0,
    geos: {}, categories: {},
    above500k: 0, below500k: 0,
    emails: 0, monday: 0, claude: 0,
    apSec: 0, popupSec: 0,
  }));
  const ensure = (u) => {
    if (!byUser.has(u)) byUser.set(u, {
      sites: 0, autopilotSites: 0, manualSites: 0,
      geos: {}, categories: {},
      above500k: 0, below500k: 0,
      emails: 0, monday: 0, claude: 0,
      apSec: 0, popupSec: 0,
    });
    return byUser.get(u);
  };
  historial.forEach(h => {
    const u = (h.media_buyer || h.user_email || h.created_by || "unknown").toLowerCase();
    const o = ensure(u);
    o.sites++;
    if (h.source === "autopilot") o.autopilotSites++; else o.manualSites++;
    const g = (h.geo || "").trim();
    if (g) o.geos[g] = (o.geos[g] || 0) + 1;
    const c = (h.category || "").trim();
    if (c) o.categories[c] = (o.categories[c] || 0) + 1;
    const traffic = parseInt(h.page_views || h.raw_visits || h.traffic || 0, 10);
    if (traffic >= 500000) o.above500k++;
    else if (traffic > 0)  o.below500k++;
  });
  usage.forEach(r => {
    const o = ensure((r.user_email || "unknown").toLowerCase());
    o.emails += parseInt(r.by_provider?._emails_sent  || 0, 10);
    o.monday += parseInt(r.by_provider?._monday_pushes || 0, 10);
    o.claude += parseInt(r.by_provider?.anthropic     || 0, 10);
  });
  sessions.forEach(s => {
    const o = ensure((s.user_email || "unknown").toLowerCase());
    if (s.kind === "autopilot") o.apSec   += s.duration_sec || 0;
    if (s.kind === "popup")     o.popupSec += s.duration_sec || 0;
  });
  return byUser;
}

// ── Comparador horizontal: tabla con métricas en filas, MBs en columnas ──
// Pensado para "ver de un vistazo quién está produciendo y quién no".
function renderAdminComparator(historial, usage, sessions) {
  const wrap = document.getElementById("admin-mb-comparator");
  if (!wrap) return;
  const byUser = _aggregateByUser(historial, usage, sessions);
  // Solo TEAM_EMAILS (3 MBs). Outsiders van solo al Resumen narrativo.
  const mbs = TEAM_EMAILS.map(e => e.toLowerCase()).map(u => ({ user: u, ...byUser.get(u) }));
  const shortName = (e) => ({
    "mgargiulo@adeqmedia.com": "Maxi",
    "dhorovitz@adeqmedia.com": "Diego",
    "sales@adeqmedia.com":     "Agus",
  })[e] || e.split("@")[0];

  const fmtTime = (sec) => sec >= 3600 ? `${Math.floor(sec/3600)}h${Math.round((sec%3600)/60)}m` : `${Math.round(sec/60)}m`;
  const topKey  = (obj) => Object.entries(obj).sort((a,b) => b[1]-a[1])[0]?.[0] || "—";
  const pctConv = (mb) => mb.sites > 0 ? Math.round((mb.monday / mb.sites) * 100) : 0;

  // Definición de filas: { label, group, get(mb) → value, fmt(value) → display, isBest higher/lower, colorFn(value) → class }
  const groups = [
    {
      title: "VOLUMEN",
      rows: [
        { label: "Sitios analizados",  get: m => m.sites,  fmt: v => v.toLocaleString() },
        { label: "Manual / Autopilot", get: m => m.manualSites + m.autopilotSites, fmt: (_, m) => `${m.manualSites} / ${m.autopilotSites}` },
        { label: "Tiempo Autopilot",   get: m => m.apSec,  fmt: v => fmtTime(v) },
      ],
    },
    {
      title: "CALIDAD",
      rows: [
        { label: "% +500K visits", get: m => m.sites ? Math.round((m.above500k / m.sites) * 100) : 0, fmt: v => `${v}%` },
        { label: "Top GEO",        get: m => Object.values(m.geos).reduce((a,b)=>a+b,0), fmt: (_, m) => topKey(m.geos), noBar: true },
        { label: "Top Categoría",  get: m => Object.values(m.categories).reduce((a,b)=>a+b,0), fmt: (_, m) => topKey(m.categories), noBar: true },
      ],
    },
    {
      title: "OUTREACH",
      rows: [
        { label: "Emails enviados",   get: m => m.emails, fmt: v => v.toLocaleString() },
        { label: "Pushes Monday",     get: m => m.monday, fmt: v => v.toLocaleString() },
        { label: "Conv. push/site",   get: m => pctConv(m), fmt: v => `${v}%`, color: v => v >= 5 ? "good" : v >= 2 ? "warn" : "bad" },
      ],
    },
    {
      title: "EFICIENCIA",
      rows: [
        { label: "Pitches Claude", get: m => m.claude, fmt: v => v.toLocaleString() },
        { label: "Mails / Push",   get: m => m.monday ? Math.round((m.emails / m.monday) * 10) / 10 : 0, fmt: v => v ? v.toFixed(1) : "—" },
      ],
    },
  ];

  const html = [];
  html.push(`<div class="mbc-row mbc-header"><div class="mbc-cell mbc-row-label">Métrica</div>${mbs.map(m => `<div class="mbc-cell"><strong>${shortName(m.user)}</strong></div>`).join("")}</div>`);
  groups.forEach(g => {
    html.push(`<div class="mbc-group-sep">${g.title}</div>`);
    g.rows.forEach(row => {
      const values = mbs.map(m => row.get(m));
      const max = Math.max(1, ...values);
      const bestIdx = values.indexOf(Math.max(...values));
      html.push(`<div class="mbc-row"><div class="mbc-cell mbc-row-label">${esc(row.label)}</div>`);
      mbs.forEach((m, i) => {
        const v = values[i];
        const display = row.fmt(v, m);
        const isBest = v === values[bestIdx] && v > 0 && !row.noBar;
        const colorCls = row.color ? ` mbc-${row.color(v)}` : "";
        const barW = row.noBar ? 0 : Math.round((v / max) * 100);
        html.push(`<div class="mbc-cell${isBest ? " mbc-best" : ""}${colorCls}">
          ${barW > 0 ? `<div class="mbc-bar" style="--w:${barW}%"></div>` : ""}
          <span class="mbc-val">${esc(String(display))}</span>
        </div>`);
      });
      html.push(`</div>`);
    });
  });
  wrap.innerHTML = html.join("");
}

// ── Resumen narrativo por MB ──────────────────────────────
// Genera cards con frases en lenguaje natural sobre la actividad de cada MB.
// Pensado para que el admin lea durante un 1:1 ("mirá tu resumen").
function renderAdminMBSummaries(historial, usage, sessions) {
  const wrap = document.getElementById("admin-mb-summaries");
  if (!wrap) return;
  // Agrupar todo por usuario
  const byUser = new Map();
  TEAM_EMAILS.forEach(e => byUser.set(e.toLowerCase(), {
    sites: 0, autopilotSites: 0, geos: {}, categories: {},
    above500k: 0, below500k: 0, emails: 0, monday: 0, claude: 0,
    apSec: 0, popupSec: 0,
  }));
  historial.forEach(h => {
    // Compatibilidad: toolbar_historial usa media_buyer; review_queue usa created_by; api_usage usa user_email.
    const u = (h.media_buyer || h.user_email || h.created_by || "unknown").toLowerCase();
    if (!byUser.has(u)) byUser.set(u, { sites: 0, autopilotSites: 0, geos: {}, categories: {}, above500k: 0, below500k: 0, emails: 0, monday: 0, claude: 0, apSec: 0, popupSec: 0 });
    const o = byUser.get(u);
    o.sites++;
    if (h.source === "autopilot") o.autopilotSites++;
    const g = (h.geo || "").trim();
    if (g) o.geos[g] = (o.geos[g] || 0) + 1;
    const c = (h.category || "").trim();
    if (c) o.categories[c] = (o.categories[c] || 0) + 1;
    // Tráfico puede venir como page_views (historial) | raw_visits (historial) | traffic (review_queue)
    const traffic = parseInt(h.page_views || h.raw_visits || h.traffic || 0, 10);
    if (traffic >= 500000) o.above500k++;
    else if (traffic > 0)  o.below500k++;
  });
  usage.forEach(r => {
    const u = (r.user_email || "unknown").toLowerCase();
    if (!byUser.has(u)) byUser.set(u, { sites: 0, autopilotSites: 0, geos: {}, categories: {}, above500k: 0, below500k: 0, emails: 0, monday: 0, claude: 0, apSec: 0, popupSec: 0 });
    const o = byUser.get(u);
    o.emails += parseInt(r.by_provider?._emails_sent || 0, 10);
    o.monday += parseInt(r.by_provider?._monday_pushes || 0, 10);
    o.claude += parseInt(r.by_provider?.anthropic || 0, 10);
  });
  sessions.forEach(s => {
    const u = (s.user_email || "unknown").toLowerCase();
    if (!byUser.has(u)) byUser.set(u, { sites: 0, autopilotSites: 0, geos: {}, categories: {}, above500k: 0, below500k: 0, emails: 0, monday: 0, claude: 0, apSec: 0, popupSec: 0 });
    const o = byUser.get(u);
    if (s.kind === "autopilot") o.apSec += s.duration_sec || 0;
    if (s.kind === "popup")     o.popupSec += s.duration_sec || 0;
  });

  wrap.innerHTML = "";
  [...byUser.entries()].sort((a, b) => b[1].sites - a[1].sites).forEach(([email, s]) => {
    const card = document.createElement("div");
    card.className = "mb-summary-card";

    // Grade automático en base a actividad + conversion
    let grade = "none", gradeLabel = "Sin actividad";
    if (s.sites > 0) {
      const conv = s.sites > 0 ? (s.monday / s.sites) * 100 : 0;
      if (s.sites >= 30 && conv >= 5)      { grade = "high"; gradeLabel = "🔥 Activo y conversor"; }
      else if (s.sites >= 30 && conv < 5)  { grade = "mid";  gradeLabel = "🟡 Activo, pocas conversiones"; }
      else if (s.sites > 0)                { grade = "low";  gradeLabel = "🔻 Baja actividad"; }
    }

    // Top GEO + categoría
    const topGeoArr = Object.entries(s.geos).sort((a, b) => b[1] - a[1]).slice(0, 3);
    const topCatArr = Object.entries(s.categories).sort((a, b) => b[1] - a[1]).slice(0, 2);
    const topGeoStr = topGeoArr.length
      ? topGeoArr.map(([g, n]) => `${g} (${Math.round((n / s.sites) * 100)}%)`).join(", ")
      : "—";
    const topCatStr = topCatArr.length ? topCatArr.map(([c]) => c).join(", ") : "—";

    // Tiempos legibles
    const apHrs = Math.floor(s.apSec / 3600), apMin = Math.round((s.apSec % 3600) / 60);
    const apTimeStr = s.apSec >= 3600 ? `${apHrs}h ${apMin}m` : `${Math.round(s.apSec / 60)}m`;
    const popupHrs = Math.floor(s.popupSec / 3600), popupMin = Math.round((s.popupSec % 3600) / 60);
    const popupTimeStr = s.popupSec >= 3600 ? `${popupHrs}h ${popupMin}m` : `${Math.round(s.popupSec / 60)}m`;

    // Conversiones
    const convPct = s.sites > 0 ? ((s.monday / s.sites) * 100).toFixed(1) : "0";
    const emailPct = s.monday > 0 ? Math.round((s.emails / s.monday) * 100) : 0;

    // Frases narrativas — solo las que tengan datos relevantes
    const lines = [];
    if (s.sites === 0) {
      lines.push("Sin actividad registrada en este período.");
    } else {
      lines.push(`Analizó <strong>${s.sites}</strong> sitios (${s.autopilotSites} via autopilot, ${s.sites - s.autopilotSites} manual).`);
      if (topGeoArr.length) lines.push(`Foco geográfico: <strong>${topGeoStr}</strong>.`);
      if (topCatArr.length) lines.push(`Categorías más analizadas: <strong>${topCatStr}</strong>.`);
      lines.push(`Calidad de leads: <strong>${s.above500k}</strong> sitios +500K vs <strong>${s.below500k}</strong> chicos.`);
      if (s.emails > 0 || s.monday > 0) {
        lines.push(`Outreach: <strong>${s.emails}</strong> emails enviados, <strong>${s.monday}</strong> pushes a Monday (conv: <strong>${convPct}%</strong>).`);
      } else {
        lines.push(`⚠️ Cero emails enviados y cero pushes a Monday — analizó pero no avanzó leads.`);
      }
      if (s.apSec > 0)    lines.push(`Tiempo Autopilot ON: <strong>${apTimeStr}</strong>.`);
      if (s.popupSec > 0) lines.push(`Tiempo con toolbar abierta: <strong>${popupTimeStr}</strong>.`);
      if (s.claude > 0)   lines.push(`Pitches generados con IA: <strong>${s.claude}</strong>.`);
    }

    card.innerHTML = `
      <div class="head">
        <span class="who">${esc(email)}</span>
        <span class="grade ${grade}">${gradeLabel}</span>
      </div>
      <ul>${lines.map(l => `<li>${l}</li>`).join("")}</ul>
    `;
    wrap.appendChild(card);
  });
}

let _userFilterPopulated = false;
async function populateUserFilter() {
  if (_userFilterPopulated) return;
  const sel = document.getElementById("admin-filter-user");
  if (!sel) return;
  // Pre-poblar con el equipo conocido (incluso si todavía no tienen actividad).
  // Después mergeamos con los emails que aparecen en toolbar_api_usage por si
  // hay usuarios fuera del team que también usaron la herramienta.
  const seen = new Set(TEAM_EMAILS.map(e => e.toLowerCase()));
  TEAM_EMAILS.forEach(e => {
    const opt = document.createElement("option");
    opt.value = e; opt.textContent = e;
    sel.appendChild(opt);
  });
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage?select=user_email&order=user_email`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    [...new Set(rows.map(r => (r.user_email || "").toLowerCase()).filter(Boolean))]
      .filter(e => !seen.has(e))
      .forEach(e => {
        const opt = document.createElement("option");
        opt.value = e; opt.textContent = e;
        sel.appendChild(opt);
      });
    _userFilterPopulated = true;
  } catch {}
}

function renderAdminChart(historial, from, to) {
  const canvas = document.getElementById("admin-chart");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  // Buckets por día
  const days = [];
  const start = new Date(from); const end = new Date(to);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    days.push(d.toISOString().slice(0, 10));
  }
  // historial puede tener `created_at` (preferido), `date`, o `fecha` (legacy). Match por prefijo YYYY-MM-DD.
  const counts = days.map(day => historial.filter(h => {
    const ts = (h.created_at || h.date || h.fecha || "").toString();
    return ts.startsWith(day);
  }).length);
  const max = Math.max(1, ...counts);

  // Dibujar
  const W = canvas.width = canvas.offsetWidth * 2; // retina
  const H = canvas.height = 360;
  ctx.scale(2, 2);
  const w = W / 2; const h = H / 2;
  ctx.clearRect(0, 0, w, h);
  // Bars
  const barW = (w - 40) / days.length;
  ctx.font = "10px -apple-system, sans-serif";
  ctx.textAlign = "center";
  days.forEach((day, i) => {
    const x = 20 + i * barW;
    const barHeight = (counts[i] / max) * (h - 40);
    ctx.fillStyle = "#38bdf8";
    ctx.fillRect(x + 1, h - 20 - barHeight, barW - 2, barHeight);
    // Label valor
    if (counts[i] > 0) {
      ctx.fillStyle = "#e2e8f0";
      ctx.fillText(counts[i], x + barW / 2, h - 22 - barHeight);
    }
    // Label día (solo cada N para que entren)
    const showLabel = days.length <= 14 || i % Math.ceil(days.length / 14) === 0;
    if (showLabel) {
      ctx.fillStyle = "#94a3b8";
      ctx.fillText(day.slice(5), x + barW / 2, h - 6);
    }
  });
}

// ── Global toast helper para errores que antes iban silenciosos a console.
//    Reemplaza el patrón console.warn() puro por un aviso visible al user.
// Cierra cualquier modal visible al presionar Escape — UX estándar.
// Y atajos de teclado para acciones frecuentes:
//   Cmd/Ctrl+M  → Push to Monday
//   Cmd/Ctrl+E  → Send via Gmail
//   Cmd/Ctrl+G  → Generate pitch with Claude
// Se setea una sola vez al cargar.
if (typeof window !== "undefined" && !window._modalEscWired) {
  window._modalEscWired = true;
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      document.querySelectorAll(".modal").forEach(m => {
        if (m.style.display && m.style.display !== "none") m.style.display = "none";
      });
      return;
    }
    // Shortcuts requieren Cmd (Mac) o Ctrl (Win/Linux). Skip si user está
    // tipeando en input/textarea (excepto Cmd+Enter para submit).
    if (!(e.metaKey || e.ctrlKey)) return;
    const tag = (e.target?.tagName || "").toLowerCase();
    const inField = tag === "input" || tag === "textarea" || e.target?.isContentEditable;
    const key = e.key.toLowerCase();
    let btn = null;
    if (key === "m" && !inField) btn = document.getElementById("btn-push-monday");
    else if (key === "e" && !inField) btn = document.getElementById("btn-send-gmail");
    else if (key === "g" && !inField) btn = document.getElementById("btn-pitch-generate") || document.getElementById("btn-autopush-prepare");
    if (btn && !btn.disabled) {
      e.preventDefault();
      btn.click();
      btn.style.outline = "2px solid #fbbf24";
      setTimeout(() => { btn.style.outline = ""; }, 400);
    }
  });
}

// Anti-flicker helper para botones: garantiza que el "loading state" dure al
// menos 350ms aunque la operación termine antes. Evita el parpadeo
// "⏳... ↻" que confunde al user porque no llega a leer.
async function withMinDuration(promise, minMs = 350) {
  const start = Date.now();
  const result = await promise;
  const elapsed = Date.now() - start;
  if (elapsed < minMs) await new Promise(r => setTimeout(r, minMs - elapsed));
  return result;
}

function showToast(message, kind = "info", durationMs) {
  if (!message) return;
  // Default: 6s para info/ok, 8s para warn/error (más texto, hay que poder leer)
  if (durationMs == null) durationMs = (kind === "error" || kind === "warn") ? 8000 : 6000;
  // Anti-overlap: si hay > 3 toasts visibles, retirar el más viejo antes de agregar
  const existing = document.querySelectorAll(".global-toast");
  if (existing.length >= 3) existing[0].remove();
  const t = document.createElement("div");
  t.className = `global-toast ${kind}`;
  t.textContent = message;
  document.body.appendChild(t);
  setTimeout(() => { t.style.opacity = "0"; t.style.transition = "opacity 0.4s"; }, durationMs);
  setTimeout(() => { t.remove(); }, durationMs + 500);
}

// Wrapper para console.warn que ADEMÁS muestra toast en errores user-facing.
// Para warnings de debug interno, seguimos usando console.warn directo.
window.notifyUserError = (msg) => {
  console.warn("[user-error]", msg);
  try { showToast(msg, "error"); } catch {}
};
window.notifyUserOk    = (msg) => { try { showToast(msg, "ok"); } catch {} };
window.notifyUserWarn  = (msg) => { try { showToast(msg, "warn"); } catch {} };

// ---- RapidAPI footer counter (siempre visible) ----
// Counter SHARED entre todos los MBs (lee desde Supabase toolbar_config).
// Período de facturación: 6 al 6 (alineado al billing real de RapidAPI plan PRO).
// Reset automático cuando entra un nuevo período.
function renderRapidApiFooterCounter({ used, limit, period } = {}) {
  const el = document.getElementById("rapidapi-monthly-counter");
  if (!el || used == null || limit == null) return;
  const pct = limit > 0 ? (used / limit) * 100 : 0;
  el.classList.remove("usage-warning", "usage-danger", "usage-reached");
  if (pct >= 100)     el.classList.add("usage-reached");
  else if (pct >= 80) el.classList.add("usage-danger");
  else if (pct >= 50) el.classList.add("usage-warning");
  el.textContent = `SW: ${used.toLocaleString()} / ${limit.toLocaleString()}`;
  el.title = `SimilarWeb cycle (${period || "—"} → next 6th): ${used.toLocaleString()} hits of ${limit.toLocaleString()} (${pct.toFixed(1)}%). Shared across all team MBs. Compare with your RapidAPI dashboard.`;
}

// ---- RapidAPI monthly usage banner (3 niveles: 50% / 80% / 100%) ----
function renderRapidApiUsageBanner({ used, limit, period, scope } = {}) {
  const banner = document.getElementById("rapidapi-cap-banner");
  const icon   = document.getElementById("cap-banner-icon");
  const title  = document.getElementById("cap-banner-title");
  const detail = document.getElementById("cap-banner-detail");
  if (!banner || used == null || limit == null) return;

  const pct = limit > 0 ? (used / limit) * 100 : 0;
  banner.classList.remove("cap-warning", "cap-danger", "cap-reached");

  // < 50% → no mostrar nada
  if (pct < 50) { banner.style.display = "none"; return; }

  banner.style.display = "flex";
  const usedStr  = used.toLocaleString();
  const limitStr = limit.toLocaleString();
  const periodStr = period || "este mes";

  // Etiqueta para distinguir cap personal vs global del equipo
  const scopeLabel = scope === "user" ? "TU CAP PERSONAL" : "Cap del equipo";

  if (pct >= 100) {
    banner.classList.add("cap-reached");
    icon.textContent  = "⛔";
    title.textContent = `${scopeLabel}: límite mensual alcanzado`;
    detail.textContent = ` — ${usedStr} / ${limitStr} en ${periodStr}. Las consultas de tráfico están pausadas hasta el próximo mes.`;
  } else if (pct >= 80) {
    banner.classList.add("cap-danger");
    icon.textContent  = "🔶";
    title.textContent = `${scopeLabel} al ${Math.round(pct)}% — atención`;
    detail.textContent = ` — ${usedStr} / ${limitStr} en ${periodStr}. Quedan pocas consultas; el autopilot puede cortarse pronto.`;
  } else {
    banner.classList.add("cap-warning");
    icon.textContent  = "⚠️";
    title.textContent = `${scopeLabel} al ${Math.round(pct)}%`;
    detail.textContent = ` — ${usedStr} / ${limitStr} en ${periodStr}. Avisamos para que estés al tanto.`;
  }
}

// Llamado por apiProxy cuando se llega al cap (100%)
function showRapidApiCapBanner(state) { renderRapidApiUsageBanner(state); }

// ---- Estado global ----
const state = {
  domain: "", url: "", tabId: null,
  traffic: 0, visits: 0, pagesPerVisit: null, trafficData: null,
  emails: [], emailSources: new Map(), emailSentInSession: false, techStack: [], partners: [], banners: null,
  adsTxt: null, revenueGap: null,
  pitch: "", duplicate: null,
  mediaBuyer: "Agus",
  score: null,
  mondayItemId: null,
  mondaySnapshot: null,
  category: "",
  siteLanguage: "",
  pageTitle: "", pageDescription: "",
  decisionMakerName: "",
  generatedPitches: [],
  loginEmail: "",
  gmailEmail: "",
  accessToken: "",
  role: "media_buyer",   // "admin" | "media_buyer" — set en init()
  adminViewActive: false, // toggle UI admin (logo triple-click)
};

// Auto-push conditions
const autoPushReady = { traffic: false, notDup: false, email: false };

// ── HTML sanitizer — prevents XSS in innerHTML ────────────────
function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#x27;");
}

// ── Input validation helpers ──────────────────────────────────
function isValidEmail(email) {
  return typeof email === "string" &&
    /^[^\s@]{1,64}@[^\s@]{1,253}\.[^\s@]{2,}$/.test(email.trim());
}

function sanitizeDomain(domain) {
  return (domain || "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/.*$/, "")
    .replace(/[^a-z0-9.\-]/g, "")  // strip chars that can't be in a domain
    .slice(0, 253);
}

// ── Client-side rate limiter ──────────────────────────────────
// Prevents accidental API flooding. Soft limit: 60 calls / minute.
const _rateLimiter = (() => {
  const MAX = 60;
  const WINDOW_MS = 60_000;
  let timestamps = [];
  return {
    check() {
      const now = Date.now();
      timestamps = timestamps.filter(t => now - t < WINDOW_MS);
      if (timestamps.length >= MAX) return false;
      timestamps.push(now);
      return true;
    },
    remaining() {
      const now = Date.now();
      timestamps = timestamps.filter(t => now - t < WINDOW_MS);
      return Math.max(0, MAX - timestamps.length);
    },
  };
})();

// ── Caché de sesión (mismo dominio, distinta URL dentro del sitio) ──
// Usa chrome.storage.session: persiste mientras el browser esté abierto.
// Evita gastar créditos de tráfico o llamadas a Monday al navegar entre
// páginas del mismo dominio (p.ej. home → nota → contacto).
async function getSessionCache(domain) {
  try {
    const key    = `sess_${domain.toLowerCase()}`;
    const stored = await chrome.storage.session.get(key);
    return stored[key] || null;
  } catch { return null; }
}
async function setSessionCache(domain, data) {
  try {
    const key = `sess_${domain.toLowerCase()}`;
    await chrome.storage.session.set({ [key]: { ...data, _ts: Date.now() } });
  } catch {}
}

let cascadeResults    = [];
let cascadeRawResults = []; // todos los sites recibidos antes de filtrar — para Apply filters
let cascadeSelected = new Set();
let cascadeBlockedExecSet = new Set(); // domains owned by other media buyers (last 45d) — keep filtered across re-applies

// ============================================================
// INIT
// ============================================================
document.addEventListener("DOMContentLoaded", async () => {

  // ── Autenticación ─────────────────────────────────────────
  let { auth } = await chrome.storage.local.get("auth");
  let loginOk  = auth?.loggedIn && auth?.user && auth?.accessToken;

  if (loginOk && auth.expiresAt <= Date.now()) {
    // Token expirado — intentar refresh automático
    if (auth.refreshToken) {
      const refreshed = await supabaseRefresh(auth.refreshToken);
      if (refreshed.error) {
        loginOk = false;
      } else {
        auth.accessToken  = refreshed.access_token;
        auth.refreshToken = refreshed.refresh_token;
        auth.expiresAt    = Date.now() + (refreshed.expires_in * 1000);
        await chrome.storage.local.set({ auth });
      }
    } else {
      loginOk = false;
    }
  }

  if (!loginOk) {
    initLoginScreen();
    return;
  }

  // Seed the shared Supabase auth token so every supabase.js request uses the user JWT (not anon)
  setSupabaseAuth(auth.accessToken);
  setTrafficAuthToken(auth.accessToken);
  // Seed the Edge Function proxy auth — Gemini/Apollo/RapidAPI calls go through Supabase
  setProxyAuth(auth.accessToken, auth.user);

  // Uso mensual de RapidAPI: footer counter (siempre visible) + banner (≥50%).
  // Re-chequea cada 60s para reflejar hits de otros MBs en paralelo.
  const handleUsageUpdate = (s) => {
    renderRapidApiFooterCounter(s);
    renderRapidApiUsageBanner(s);
  };
  onRapidApiCapReached(handleUsageUpdate);
  // Refresh inmediato del footer counter al instante después de cada hit
  // (en vez de esperar al polling de 60s). Permite al user ver subir el
  // contador en tiempo real al analizar un dominio nuevo.
  onRapidApiHit(handleUsageUpdate);
  const refreshUsage = () => {
    getRapidApiMonthlyStatus().then(handleUsageUpdate).catch(() => {});
  };
  refreshUsage();
  setInterval(refreshUsage, 60_000);

  // Auto-refresh 2 min before expiry so long-lived panels stay authenticated
  const scheduleRefresh = () => {
    const msUntil = (auth.expiresAt || 0) - Date.now() - 2 * 60 * 1000;
    if (msUntil <= 0) return;
    setTimeout(async () => {
      try {
        const r = await supabaseRefresh(auth.refreshToken);
        if (!r.error && r.access_token) {
          auth.accessToken  = r.access_token;
          auth.refreshToken = r.refresh_token;
          auth.expiresAt    = Date.now() + (r.expires_in * 1000);
          state.accessToken = r.access_token;
          await chrome.storage.local.set({ auth });
          setSupabaseAuth(r.access_token);
          setProxyAuth(r.access_token, auth.user);
          setTrafficAuthToken(r.access_token);
          scheduleRefresh();
        }
      } catch (e) { console.warn("[AuthRefresh]", e.message); }
    }, msUntil);
  };
  scheduleRefresh();

  // ── Cargar API keys desde Supabase (requiere JWT válido) ──
  const apiKeys = await fetchApiKeys(auth.accessToken);
  if (!apiKeys) {
    showError("Could not load API configuration. Check your connection or sign in again.");
    return;
  }
  // Monday API key por usuario (cae en la default si no hay específica)
  const loginEmail   = (auth?.user || "").toLowerCase().trim();
  const userMondayKey = apiKeys[`monday_api_key_${loginEmail}`];
  CONFIG.MONDAY_API_KEY = userMondayKey || apiKeys.monday_api_key || "";
  CONFIG.RAPIDAPI_KEY   = apiKeys.rapidapi_key    || "";
  CONFIG.GEMINI_API_KEY = apiKeys.gemini_api_key  || "";
  CONFIG.APOLLO_API_KEY = apiKeys.apollo_api_key  || "";

  // Usuario autenticado — ocultar login y mostrar app
  document.getElementById("login-screen").style.display = "none";
  applyUserFromAuth(auth);

  // ── Inicio normal ─────────────────────────────────────────
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

  if (!tab?.url || tab.url.startsWith("chrome://") || tab.url.startsWith("about:")) {
    showError("Navigate to a website to use the toolbar.");
    return;
  }

  state.tabId  = tab.id;
  state.url    = tab.url;
  state.domain = extractDomain(tab.url);

  const _siteUrlEl = document.getElementById("site-url");
  _siteUrlEl.textContent = state.domain;
  _siteUrlEl.title = state.domain; // tooltip por si está truncado por ellipsis
  document.getElementById("cascade-seed").value   = state.domain;

  // Auto-refresh on URL change (side panel queda abierto al navegar):
  // detectamos cambios de tab/URL y disparamos re-análisis después de 3s estables.
  setupAutoRefreshOnUrlChange();

  // Atajo de teclado: Cmd/Ctrl+Shift+R fuerza re-análisis del dominio actual
  // (ignora cache local, va directo a la API).
  document.addEventListener("keydown", (e) => {
    const isRefresh = (e.metaKey || e.ctrlKey) && e.shiftKey && (e.key === "R" || e.key === "r");
    if (isRefresh) {
      e.preventDefault();
      forceRefreshAnalysis();
    }
  });

  // Check personal quota: si el usuario está al 80% de su cap mensual personal,
  // mostrar banner discreto encima del header.
  checkPersonalQuotaWarning();

  // Tracking real de tiempo: abrir sesión "popup" en Supabase. Se cierra al
  // unload o cuando el side panel se oculta (visibility change).
  startUsageSession(state.accessToken, state.loginEmail, "popup").catch(() => {});

  // Coordinación entre MBs: vacation toggle en settings + check de handoffs pendientes
  setupVacationToggle();
  checkPendingHandoffs();
  window.addEventListener("beforeunload", () => endUsageSession(state.accessToken));
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") endUsageSession(state.accessToken);
    else startUsageSession(state.accessToken, state.loginEmail, "popup").catch(() => {});
  });

  // mediaBuyer is derived from auth login — do NOT override from legacy storage key
  prefillMondayForm();
  initTabs();
  initHistoryModal();
  bindButtons();
  initPitchDrafts(); // used by Prospects cards + Analysis — cheap, keep eager
  initPitchInlineControls(); // bandera + trash al lado del pitch
  // Poblar dropdowns de países desde MONDAY_COUNTRIES (lista exacta del board)
  populateCountryDropdown(document.getElementById("form-geo"));
  populateCountryDropdown(document.getElementById("import-geo"));
  populateCountryDropdown(document.getElementById("refresh-geo"));
  bindCustomPromptHandlers();
  // Prompt GLOBAL obligatorio (Diego's voice). Todos los MBs lo usan, no
  // pueden editarlo. Solo el admin puede cambiarlo desde Settings.
  // Fetch del valor en Supabase (key=__global__); si no existe, fallback al
  // constante bakeado en diegoVoicePrompt.js.
  getCustomPrompt(auth.accessToken, GLOBAL_PROMPT_KEY)
    .then(p => { state.customPrompt = (p && p.trim()) ? p : DIEGO_VOICE_PROMPT; })
    .catch(() => { state.customPrompt = DIEGO_VOICE_PROMPT; });
  // initKeywords, initAutopilot, initProspectsTab, initCsvQueue, loadHistoryTab → lazy on tab click

  // Show the toolbar login email as the Gmail "from" account
  const fromEl = document.getElementById("gmail-from");
  if (fromEl && state.loginEmail) fromEl.textContent = `From: ${state.loginEmail}`;

  // Contador de API calls + límites del plan en footer
  updateApiFooter();

  // Análisis core en paralelo — no bloquea el render del UI
  Promise.all([
    runDuplicateCheck() .catch(e => console.error("[DupCheck]",   e)),
    runTrafficCheck()   .catch(e => console.error("[Traffic]",    e)),
    runAuditCheck()     .catch(e => console.error("[Audit]",      e)),
    runEmailScraper()   .catch(e => console.error("[Email]",      e)),
    runBannerDetection().catch(e => console.error("[Banners]",    e)),
    runPageContext()    .catch(e => console.error("[PageCtx]",    e)),
  ]).then(async () => {
    runAutoFill();
    // Autocarga del borrador del idioma de la GEO (priority asc).
    // Se hace AFTER runAutoFill para que form-geo ya esté seteado.
    autofillDraftOnLoad().catch(e => console.warn("[DraftAutofill]", e));
    // Guardar en caché de sesión los datos costosos (evita créditos en subpáginas)
    setSessionCache(state.domain, {
      duplicate:   state.duplicate,
      trafficData: state.trafficData,
      traffic:     state.traffic,
      visits:      state.visits,
      pagesPerVisit: state.pagesPerVisit,
      category:    state.category,
      emails:      state.emails,
    });
    // Contabilizar +400K separando nuevos de duplicados
    if (state.traffic >= CONFIG.MIN_TRAFFIC) {
      const isNewSite = !state.duplicate?.found;
      const nowMonth  = new Date().toISOString().substring(0, 7);
      const statsKey  = userKey("historyStats");
      const defaults  = { total: 0, month: nowMonth, monthNew: 0, monthDups: 0, monthMonday: 0,
                          monthNewQual: 0, monthDupQual: 0,
                          monthMondayNewQual: 0, monthMondayDupQual: 0, monthMondayBelow: 0 };
      const { [statsKey]: hs = defaults } = await chrome.storage.local.get(statsKey);
      if (hs.month !== nowMonth) {
        Object.assign(hs, { month: nowMonth, monthNew: 0, monthDups: 0, monthMonday: 0,
          monthNewQual: 0, monthDupQual: 0, monthMondayNewQual: 0, monthMondayDupQual: 0, monthMondayBelow: 0 });
      }
      if (isNewSite) hs.monthNewQual = (hs.monthNewQual || 0) + 1;
      else           hs.monthDupQual = (hs.monthDupQual || 0) + 1;
      await chrome.storage.local.set({ [statsKey]: hs });
      loadHistoryTab().catch(() => {});
    }
  }).catch(() => {});
});

// ============================================================
// MONDAY FORM
// ============================================================
// AUTO-FILL FORMULARIO MONDAY
// ============================================================
// Códigos de país de SimilarWeb → texto del select #form-geo
// Los nombres están en CASTELLANO con primera letra mayúscula porque ese es
// el formato que tiene la columna GEO del board de Monday. Si Monday cambia
// (ej. agregan Honduras, Costa Rica), agregar acá también.
// ── Lista exacta de países del board Monday ───────────────────
// Esta es la "lista final" que se mostró en el dropdown del board. Tiene
// duplicados/variantes (Bosnia + Bosnia y Herzegovina, Holanda + Netherlands
// + Paises Bajos, Reino Unido + Gran Bretaña) que se preservan a propósito —
// si Monday los tiene como opciones distintas, el toolbar también.
const MONDAY_COUNTRIES = [
  "Afghanistan","Albania","Alemania","Algeria","Arabia Saudita","Argelia","Argentina",
  "Australia","Austria","Bangladesh","Bielorrusia","Bolivia","Bosnia",
  "Bosnia y Herzegovina","Brasil","Bulgaria","Bélgica","Camerun","Canadá","Catar",
  "Chile","China","Chipre","Colombia","Congo","Corea","Corea del Norte","Corea del Sur",
  "Costa Rica","Costa de Marfil","Croacia","Dinamarca","Dubai","Ecuador","Egipto",
  "El Salvador","Emiratos Arabes","Emiratos Arabes Unidos","Eslovaquia","Eslovenia",
  "España","Estados Unidos","Filipinas","Finlandia","Francia","Georgia","Ghana",
  "Gran Bretaña","Grecia","Guatemala","Haiti","Holanda","Honduras","Hungria",
  "India","Indonesia","Iran","Iraq","Irlanda","Israel","Italia","Jamaica","Japon",
  "Jordan","Jordania","Kenia","Kuwait","Letonia","Lituania","Macedonia","Malasia",
  "Marruecos","Mauricio","Mexico","Moldavia","Myanmar","Nambia","Netherlands",
  "Nicaragua","Nigeria","Noruega","Nueva Zelanda","Oman","Paises Bajos","Pakistan",
  "Palestina","Panama","Paraguay","Peru","Polonia","Portugal","Puerto Rico","Qatar",
  "Reino Unido","Republica Checa","Republica Dominicana","Republica de Corea",
  "Rumania","Rusia","Senegal","Serbia","Singapur","Siria","Somalia","Sri Lanka",
  "Sudafrica","Suecia","Suiza","Tailandia","Taiwan","Tanzania","Trinidad y Tobago",
  "Tunez","Turquia","Ucrania","Uganda","Uruguay","Venezuela","Vietnam","Yermen","Zimbabwe"
];

// ISO alpha-2 → label canónico que matchea EXACTAMENTE una entrada de
// MONDAY_COUNTRIES (mismo string, misma capitalización, mismas tildes/sin tildes).
// Cuando el toolbar autocompleta #form-geo, busca por este label, así que
// debe coincidir char-by-char con una <option> del select.
const GEO_LABEL = {
  US:"Estados Unidos", GB:"Reino Unido", AR:"Argentina", MX:"Mexico", CO:"Colombia",
  CL:"Chile", ES:"España", BR:"Brasil", FR:"Francia", DE:"Alemania", IT:"Italia",
  PT:"Portugal", NL:"Paises Bajos", BE:"Bélgica", CH:"Suiza", AT:"Austria",
  PL:"Polonia", HU:"Hungria", CZ:"Republica Checa", RO:"Rumania", GR:"Grecia",
  TR:"Turquia", IL:"Israel", AE:"Emiratos Arabes Unidos", SA:"Arabia Saudita",
  MA:"Marruecos", EG:"Egipto", IN:"India", JP:"Japon", KR:"Corea del Sur",
  KP:"Corea del Norte", AU:"Australia", CA:"Canadá", SE:"Suecia", NO:"Noruega",
  DK:"Dinamarca", FI:"Finlandia", VN:"Vietnam", TH:"Tailandia", ID:"Indonesia",
  PH:"Filipinas", CN:"China", SG:"Singapur", ZA:"Sudafrica", NG:"Nigeria", PE:"Peru",
  EC:"Ecuador", VE:"Venezuela", BO:"Bolivia", PY:"Paraguay", UY:"Uruguay",
  RU:"Rusia", UA:"Ucrania", IE:"Irlanda", DO:"Republica Dominicana",
  CR:"Costa Rica", PA:"Panama", GT:"Guatemala", HN:"Honduras", SV:"El Salvador",
  NI:"Nicaragua", PR:"Puerto Rico", NZ:"Nueva Zelanda",
  // Países nuevos del listado Monday
  AF:"Afghanistan", AL:"Albania", DZ:"Algeria", BD:"Bangladesh", BY:"Bielorrusia",
  BA:"Bosnia y Herzegovina", BG:"Bulgaria", CM:"Camerun", QA:"Qatar", CY:"Chipre",
  CG:"Congo", CI:"Costa de Marfil", HR:"Croacia", SK:"Eslovaquia", SI:"Eslovenia",
  GE:"Georgia", GH:"Ghana", HT:"Haiti", IR:"Iran", IQ:"Iraq", JM:"Jamaica",
  JO:"Jordania", KE:"Kenia", KW:"Kuwait", LV:"Letonia", LT:"Lituania",
  MK:"Macedonia", MY:"Malasia", MU:"Mauricio", MD:"Moldavia", MM:"Myanmar",
  NA:"Nambia", OM:"Oman", PK:"Pakistan", PS:"Palestina", RS:"Serbia",
  SY:"Siria", SO:"Somalia", LK:"Sri Lanka", TW:"Taiwan", TZ:"Tanzania",
  TT:"Trinidad y Tobago", TN:"Tunez", UG:"Uganda", YE:"Yermen", ZW:"Zimbabwe",
  SN:"Senegal",
};

// Helper: poblar dinámicamente un <select> con MONDAY_COUNTRIES + opción "All"/"-".
// keepFirst: si true, no toca la primera <option> existente (el "All"/"— elegir —")
function populateCountryDropdown(selectEl, { keepFirst = true } = {}) {
  if (!selectEl) return;
  const firstOpt = keepFirst && selectEl.options[0] ? selectEl.options[0].outerHTML : "";
  selectEl.innerHTML = firstOpt + MONDAY_COUNTRIES.map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join("");
}

// 2-letter language code → #form-idioma value (matches Monday column indexes)
// Supported: English(0), Spanish(1), Italian(2), Portuguese(3), Arabic(6)
const LANG_TO_IDIOMA = { en:"0", es:"1", it:"2", pt:"3", ar:"6" };
// Domain TLD fallback for language detection
const TLD_TO_LANG = { es:"es",mx:"es",ar:"es",co:"es",cl:"es",pe:"es",uy:"es",py:"es",bo:"es",
  br:"pt",pt:"pt",it:"it" };

function detectLangFromDomain(domain) {
  const tld = (domain || "").split(".").pop()?.toLowerCase();
  return TLD_TO_LANG[tld] || "";
}

function runAutoFill() {
  const dup    = state.duplicate;
  const isNew  = !dup?.found;

  // Condición para llenar campos SENSIBLES (email, idioma): nuevo o dup > 30 días
  let shouldFill = isNew;
  if (!isNew && dup?.fecha) {
    const daysSince = (Date.now() - new Date(dup.fecha).getTime()) / 86_400_000;
    shouldFill = daysSince > 30;
  }

  // ── GEO siempre se autocompleta si tenemos dato y el campo está vacío ────
  // (no depende de shouldFill — si cambia el top country después de un mes,
  //  debería reflejarse en Monday sin forzar que el ítem sea "nuevo")
  let topCountry = state.trafficData?.topCountries?.[0]?.code;
  if (!topCountry) {
    const firstChip = document.querySelector(".country-flag-chip[data-code]");
    if (firstChip) topCountry = firstChip.dataset.code;
  }
  if (topCountry && GEO_LABEL[topCountry]) {
    const geoSel = document.getElementById("form-geo");
    if (geoSel && !geoSel.value) { // solo si el select está vacío — no pisa selección manual del user
      const opt = [...geoSel.options].find(o => o.text === GEO_LABEL[topCountry]);
      if (opt) geoSel.value = opt.value || opt.text;
    }
  }

  if (!shouldFill) return;

  // ── IDIOMA: del DOM o TLD ─────────────────────────────────────
  const lang = state.siteLanguage || detectLangFromDomain(state.domain);
  if (lang) {
    const idiomaVal = LANG_TO_IDIOMA[lang];
    if (idiomaVal !== undefined) {
      const idSel = document.getElementById("form-idioma");
      if (idSel) idSel.value = idiomaVal;
    }
  }

  // ── EMAIL: mejor opción disponible ──────────────────────────
  const bestEmail = state.emails?.[0] || dup?.email || "";
  if (bestEmail) {
    const emailEl = document.getElementById("form-email");
    if (emailEl && !emailEl.value) emailEl.value = bestEmail;
  }
}

// ============================================================
// "2026-04-25" → "25/04/2026"
function toDisplayDate(iso) {
  if (!iso) return "";
  const [y, m, d] = iso.split("-");
  if (!y || !m || !d) return iso;
  return `${d}/${m}/${y}`;
}
// "25/04/2026" → "2026-04-25"  (lo que Monday espera)
function toIsoDate(display) {
  if (!display) return "";
  const [d, m, y] = display.split("/");
  if (!d || !m || !y) return display;
  return `${y}-${m.padStart(2,"0")}-${d.padStart(2,"0")}`;
}

function prefillMondayForm() {
  document.getElementById("form-fecha").value     = toDisplayDate(new Date().toISOString().split("T")[0]);
  document.getElementById("form-ejecutivo").value = state.mediaBuyer;
  // Default estado: "Propuesta Vigente (T)" (id 4) para envíos/updates desde la toolbar
  document.getElementById("form-estado").value    = "4";
}

function fillMondayFormFromDuplicate(dup) {
  if (dup.email) document.getElementById("form-email").value = dup.email;
  if (dup.fecha) document.getElementById("form-fecha").value = toDisplayDate(dup.fecha);
  // Pre-fill Traffic desde Monday — solo si todavía no tenemos dato de SimilarWeb.
  // Cubre el caso "duplicado + SimilarWeb sin datos": evita el bloqueo del push.
  if (dup.trafico && !state.traffic) {
    const parsed = parseTrafficText(dup.trafico);
    if (parsed > 0) {
      state.traffic = parsed;
      const pvInput = document.getElementById("form-pv-display");
      if (pvInput) pvInput.value = formatTraffic(parsed);
    }
  }
  if (dup.geo) {
    const sel = document.getElementById("form-geo");
    const opt = [...sel.options].find(o => o.value === dup.geo || o.text === dup.geo);
    if (opt) sel.value = opt.value;
  }
  if (dup.idioma) {
    const sel = document.getElementById("form-idioma");
    const raw = dup.idioma.trim().toLowerCase();
    // Map Monday label (Spanish or English) → select value (index)
    const MONDAY_IDIOMA = {
      "ingles": "0", "inglés": "0", "english": "0",
      "espanol": "1", "español": "1", "spanish": "1",
      "italiano": "2", "italian": "2",
      "portugues": "3", "portugués": "3", "portuguese": "3",
      "arabe": "6", "árabe": "6", "arabic": "6",
    };
    const mapped = MONDAY_IDIOMA[raw];
    if (mapped !== undefined) {
      sel.value = mapped;
    } else {
      // Fallback: matchear contra el texto de cada opción
      const opt = [...sel.options].find(o => o.text.trim().toLowerCase() === raw);
      if (opt) sel.value = opt.value;
    }
  }
  // NOTA: No sobrescribimos form-ejecutivo con dup.ejecutivo (dueño original).
  // Cada actualización debe quedar registrada bajo el usuario logueado.
}

// ============================================================
// TABS
// ============================================================
function initHistoryModal() {
  const openBtn  = document.getElementById("btn-history-open");
  const closeBtn = document.getElementById("btn-history-close");
  const modal    = document.getElementById("history-modal");
  if (!openBtn || !modal) return;

  openBtn.addEventListener("click", async () => {
    modal.style.display = "flex";
    const listEl = document.getElementById("history-list");
    if (listEl) listEl.innerHTML = '<div class="cascade-empty">⏳ Loading history...</div>';
    try { await loadHistoryTab(); }
    catch (e) {
      console.error("[History]", e);
      if (listEl) listEl.innerHTML = '<div class="cascade-empty">Error loading history.</div>';
    }
  });
  closeBtn?.addEventListener("click", () => { modal.style.display = "none"; });
  modal.addEventListener("click", (e) => {
    if (e.target === modal) modal.style.display = "none"; // click on backdrop closes
  });
}

function initTabs() {
  const loadedTabs = new Set(["core"]); // core loads eagerly with the page

  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
      document.querySelectorAll(".tab-content").forEach(c => c.classList.remove("active"));
      btn.classList.add("active");
      const tabId  = btn.dataset.tab;
      const tabEl  = document.getElementById(`tab-${tabId}`);
      tabEl.classList.add("active");

      // Lazy load: only initialize the tab the first time it's opened
      if (!loadedTabs.has(tabId)) {
        loadedTabs.add(tabId);
        try {
          if (tabId === "cascade") {
            await initKeywords();
          } else if (tabId === "prospects") {
            await initProspectsTab();
            await initAutopilot();
            await loadProspectsTab();
          } else if (tabId === "import") {
            await initCsvQueue();
          }
        } catch (e) {
          console.error(`[Tab ${tabId}]`, e);
        }
      }
    });
  });

  // Banner global Railway muerto — corre siempre que el popup esté abierto.
  // Solo aparece cuando autopilot/csv_queue ON + heartbeat > 5min stale.
  pollRailwayDeadBanner();
  setInterval(() => {
    if (document.visibilityState !== "hidden") pollRailwayDeadBanner();
  }, 30_000);
}

// ============================================================
// ANÁLISIS CORE
// ============================================================
async function runDuplicateCheck() {
  const el = document.getElementById("duplicate-result");
  try {
    // Usar caché de sesión solo si ya confirmamos que ES duplicado.
    // Si el caché dice "no encontrado", siempre re-consulta Monday porque
    // el ítem puede haberse creado en este mismo browser session.
    let result;
    const sess = await getSessionCache(state.domain);
    if (sess?.duplicate?.found === true) {
      result = sess.duplicate;
    } else {
      result = await checkDuplicate(state.domain);
    }
    state.duplicate = result;

    if (result.found) {
      el.textContent = `⚠️ DUPLICADO · ${result.status} · ${result.ejecutivo || "—"}`;
      el.className   = "status-badge duplicate";
      state.mondayItemId = result.itemId;
      fillMondayFormFromDuplicate(result);
      document.getElementById("btn-push-monday").textContent = "🔄 Update in Monday";
    } else {
      el.textContent = "✅ Nuevo prospecto";
      el.className   = "status-badge new";
      autoPushReady.notDup = true;
      checkAutoPush();
    }
    // Lock check: si OTRO MB ya está trabajando este dominio, mostrar warning
    checkProspectLock();
    // Detectar idioma siempre (nuevo y duplicado) para el pitch
    autoDetectPageLanguage();
  } catch {
    el.textContent = "⚠️ Monday API error";
    el.className   = "status-badge";
  }
}

async function autoDetectPageLanguage() {
  // ── Regla por TLD del dominio ─────────────────────────────
  const tld = state.domain.split(".").pop()?.toLowerCase();

  const TLD_LANG = {
    // Español — LATAM + España
    ar:"es", mx:"es", co:"es", cl:"es", pe:"es", ec:"es", ve:"es",
    uy:"es", py:"es", bo:"es", cr:"es", pa:"es", do:"es", gt:"es",
    hn:"es", sv:"es", cu:"es", ni:"es", es:"es",
    // Portugués
    br:"pt", pt:"pt",
    // Italiano
    it:"it",
    // Árabe
    sa:"ar", ae:"ar", eg:"ar", ma:"ar", dz:"ar", tn:"ar",
    ly:"ar", iq:"ar", sy:"ar", jo:"ar", lb:"ar", kw:"ar",
    qa:"ar", bh:"ar", om:"ar", ye:"ar",
    // Francés
    fr:"fr",
    // Alemán
    de:"de",
  };

  let lang = TLD_LANG[tld] || "";

  // ── Fallback 1: país top del tráfico ─────────────────────
  if (!lang && state.trafficData?.topCountries?.length) {
    const topCode = state.trafficData.topCountries[0].code?.toUpperCase();
    const COUNTRY_LANG = {
      AR:"es",MX:"es",CO:"es",CL:"es",PE:"es",EC:"es",VE:"es",
      UY:"es",PY:"es",BO:"es",CR:"es",PA:"es",DO:"es",GT:"es",
      HN:"es",SV:"es",CU:"es",NI:"es",ES:"es",
      BR:"pt",PT:"pt",
      IT:"it",
      SA:"ar",AE:"ar",EG:"ar",MA:"ar",DZ:"ar",TN:"ar",
      LY:"ar",IQ:"ar",SY:"ar",JO:"ar",LB:"ar",KW:"ar",
      QA:"ar",BH:"ar",OM:"ar",YE:"ar",
      FR:"fr",DE:"de",
    };
    lang = COUNTRY_LANG[topCode] || "";
  }

  // ── Fallback 2: atributo lang del HTML ────────────────────
  if (!lang) {
    try {
      const [{ result: pageLang }] = await chrome.scripting.executeScript({
        target: { tabId: state.tabId },
        func:   () => (document.documentElement.lang || "").toLowerCase(),
      });
      if (pageLang) lang = pageLang.split("-")[0];
    } catch { /* sin permisos */ }
  }

  if (!lang) lang = "en"; // default inglés

  state.siteLanguage = lang;

  // Sincronizar selector de idioma del pitch
  const pitchLangSel = document.getElementById("pitch-language");
  if (pitchLangSel) pitchLangSel.value = lang;

  // Auto-select in Monday language selector (supported langs only)
  const MAP = { en:"0", es:"1", it:"2", pt:"3", ar:"6" };
  const index = MAP[lang];
  if (index !== undefined) {
    const sel = document.getElementById("form-idioma");
    if (sel && sel.value === "") sel.value = index;
  }
}

// Después de getTraffic, si SimilarWeb no devolvió país, completar con señales de página
function enrichTrafficWithPageSignals(trafficData) {
  if (!trafficData) return trafficData;
  if (trafficData.topCountries?.length > 0) return trafficData; // ya tenemos
  const inferred = inferCountryFromPageSignals();
  if (inferred) {
    const COUNTRY_NAMES = {
      AR:"Argentina",MX:"Mexico",CO:"Colombia",CL:"Chile",BR:"Brazil",PE:"Peru",
      UY:"Uruguay",PY:"Paraguay",BO:"Bolivia",EC:"Ecuador",VE:"Venezuela",ES:"Spain",
      US:"United States",GB:"United Kingdom",CA:"Canada",FR:"France",DE:"Germany",
      IT:"Italy",PT:"Portugal",NL:"Netherlands",BE:"Belgium",CH:"Switzerland",
      AT:"Austria",SE:"Sweden",NO:"Norway",DK:"Denmark",FI:"Finland",PL:"Poland",
      GR:"Greece",HU:"Hungary",CZ:"Czech Republic",RO:"Romania",IE:"Ireland",
      IL:"Israel",AE:"UAE",SA:"Saudi Arabia",EG:"Egypt",MA:"Morocco",TR:"Turkey",
      IN:"India",JP:"Japan",KR:"South Korea",CN:"China",SG:"Singapore",MY:"Malaysia",
      ID:"Indonesia",PH:"Philippines",TH:"Thailand",VN:"Vietnam",AU:"Australia",
      NZ:"New Zealand",ZA:"South Africa",NG:"Nigeria",RU:"Russia",UA:"Ukraine",
      HK:"Hong Kong",TW:"Taiwan",PK:"Pakistan",BD:"Bangladesh",
    };
    trafficData.topCountries = [{
      code: inferred.code,
      name: COUNTRY_NAMES[inferred.code] || inferred.code,
      share: 0,
      source: inferred.source, // "lang-region" | "og-locale" | "phone" | "currency"
    }];
    console.log(`[GEO] ${state.domain} inferido por ${inferred.source}: ${inferred.code}`);
    // Persistir al cache compartido — esto es info que NO viene de SimilarWeb,
    // muy valiosa porque sale del análisis de la página real.
    const SOURCE_CONF = { "lang-region": 5, "og-locale": 6, "phone": 5, "currency": 4, "footer-name": 7 };
    if (state.accessToken && state.domain) {
      setDomainGeo(state.accessToken, state.domain, inferred.code,
                   inferred.source, SOURCE_CONF[inferred.source] || 5).catch(() => {});
    }
  }
  return trafficData;
}

async function runTrafficCheck() {
  const metricEl    = document.getElementById("traffic-result");
  const unitEl      = document.getElementById("traffic-unit");
  const breakdownEl = document.getElementById("traffic-breakdown");
  const categoryEl  = document.getElementById("traffic-category");
  const filterEl    = document.getElementById("traffic-filter");

  try {
    // Caché de sesión primero — mismo dominio, distinta subpágina
    const sess = await getSessionCache(state.domain);
    let data;
    if (sess?.trafficData) {
      data = { ...sess.trafficData, fromCache: true, cachedDaysAgo: sess.trafficData.cachedDaysAgo ?? 0 };
    } else {
      data = await getTraffic(state.domain);
    }
    if (!data) {
      metricEl.textContent = "No data"; metricEl.className = "metric";
      filterEl.textContent = "Configure your RapidAPI key"; filterEl.className = "filter-tag fail";
      return;
    }

    state.traffic       = data.pageViews ?? data.rawVisits ?? 0;
    state.visits        = data.rawVisits || 0;
    state.pagesPerVisit = data.pagesPerVisit || null;
    state.trafficData   = enrichTrafficWithPageSignals(data);
    state.category      = data.category || "";

    // Auto-set categoría en el selector de pitch
    if (data.category) {
      const mapped = mapCategory(data.category);
      const catSel = document.getElementById("pitch-category");
      if (catSel && mapped) catSel.value = mapped;
    }

    const cacheStr = data.fromCache ? ` <span class="cache-badge">⚡ Cache · ${data.cachedDaysAgo}d ago</span>` : "";

    // Bandera del top country (la que vamos a mostrar pegada al métrica principal)
    // Buscar primero en topCountries, fallback a la inferida desde page signals
    const topC = (data.topCountries || []).find(c => c.code) || null;
    const mainFlagHtml = topC
      ? `<span class="main-country-flag" title="País principal de tráfico: ${esc(topC.name || topC.code)}" data-code="${esc(topC.code)}" style="font-size:18px;margin-left:6px;cursor:help">${countryFlag(topC.code)}</span>`
      : "";

    if (data.noPageViewData) {
      metricEl.innerHTML = `${formatTraffic(state.visits)}${mainFlagHtml}`;
      if (unitEl) unitEl.textContent = "visits/mo";
      breakdownEl.innerHTML = `<span class="no-pageview-note">No page-view data</span>${cacheStr}`;
    } else if (data.estimatedPages) {
      // Estimación por categoría — claramente etiquetado como ~
      metricEl.innerHTML = `~${formatTraffic(state.traffic)}${mainFlagHtml}`;
      if (unitEl) unitEl.textContent = "pages/mo (est.)";
      breakdownEl.innerHTML = `${formatTraffic(state.visits)} visits × ~${data.pagesPerVisit} p/v <span class="pv-estimated">(estimado conservador)</span>${cacheStr}`;
    } else {
      metricEl.innerHTML = `${formatTraffic(state.traffic)}${mainFlagHtml}`;
      if (unitEl) unitEl.textContent = "pages/mo";
      const srcLabel = data.ppvSource === "engagement" ? ` <span class="pv-source">via /engagement</span>` : "";
      breakdownEl.innerHTML = `${formatTraffic(state.visits)} visits × ${data.pagesPerVisit} p/v${srcLabel}${cacheStr}`;
    }
    metricEl.className = "metric";

    // Actualizar Páginas Vistas en formulario Monday
    // Solo sobrescribir el input de Traffic si SimilarWeb devolvió un dato real.
    // Si SimilarWeb falló (state.traffic === 0), preservamos lo prefilled desde Monday
    // o lo que el usuario haya tipeado manualmente.
    const pvDisplay = document.getElementById("form-pv-display");
    // No pisar el input si el user lo está tipeando (focused) o si ya tiene
    // un valor manual — preserva la edición y evita pérdida de focus mid-typing.
    if (pvDisplay && state.traffic
        && document.activeElement !== pvDisplay
        && !pvDisplay.value) {
      pvDisplay.value = formatTraffic(state.traffic);
    }

    // Top 3 países con banderas — chips secundarios SOLO si el % es significativo (>1%)
    // y con tooltip claro. El % es "share del tráfico mundial del sitio".
    const countriesEl = document.getElementById("traffic-countries");
    if (countriesEl) {
      const chips = (data.topCountries || [])
        .map(c => {
          const raw = Number(c.share) || 0;
          const pct = raw > 1 ? Math.round(raw) : Math.round(raw * 100);
          return { ...c, pct };
        })
        .filter(c => c.code && c.pct >= 1) // descarta el "0%" confuso
        .slice(0, 3);
      if (chips.length) {
        countriesEl.innerHTML = chips.map(c => {
          const tip = `${esc(c.name || c.code)} — ${c.pct}% del tráfico del sitio viene de acá`;
          return `<span class="country-flag-chip" data-code="${esc(c.code)}" title="${tip}">${countryFlag(c.code)} ${c.pct}%</span>`;
        }).join("");
      } else {
        countriesEl.innerHTML = "";
      }
    }

    if (data.category) {
      const catLabel = simplifyCategory(data.category);
      categoryEl.textContent = `${catLabel}${data.globalRank ? ` · Global #${data.globalRank.toLocaleString()}` : ""}`;
    }

    const trafficForFilter = state.traffic || state.visits;
    filterEl.textContent = passesTrafficFilter(trafficForFilter) ? "✅ Supera umbral 400K" : "❌ Bajo umbral 400K — no enriquecer";
    filterEl.className   = `filter-tag ${passesTrafficFilter(trafficForFilter) ? "pass" : "fail"}`;

    if (passesTrafficFilter(trafficForFilter)) {
      autoPushReady.traffic = true;
      checkAutoPush();
    }

    // Actualizar footer después de una llamada real
    updateApiFooter();

    updateScore();

  } catch (e) {
    console.error("Traffic error:", e);
    metricEl.textContent = "Error"; metricEl.className = "metric";
  }
}

async function runAuditCheck() {
  const partnersEl = document.getElementById("partners-result");

  try {
    const audit      = await runAudit(state.url, state.traffic);
    state.techStack  = audit.techStack;
    state.adsTxt     = audit.adsTxt;
    state.revenueGap = audit.revenueGap;
    state.partners   = audit.allPartners;

    partnersEl.className = "partners-result";
    partnersEl.innerHTML = audit.allPartners.map(p =>
      `<span class="partner-chip ${p.found ? "partner-found" : "partner-miss"}">${p.found ? "✓ " : ""}${esc(p.name)}</span>`
    ).join("");

    updateScore();

  } catch {
    partnersEl.textContent = "Error"; partnersEl.className = "partners-result";
  }
}

async function runBannerDetection() {
  const el  = document.getElementById("banner-result");
  const btn = document.getElementById("btn-detect-banners");
  el.textContent = "⏳ Detectando...";
  try {
    const result  = await detectBanners(state.tabId);
    state.banners = result;

    if (result.error) {
      el.textContent = `Error: ${result.error}`;
      el.className   = "banner-result";
    } else if (!result.total) {
      el.textContent = "No ad formats detected";
      el.className   = "banner-result";
    } else {
      el.className = "banner-result";
      el.innerHTML = result.summary.map(s => {
        let cls = "banner-chip";
        if (s.type === "Video Instream" || s.type === "Video Slider") cls += " video";
        if (s.type === "In Text Banners" || s.type === "In Image")    cls += " display";
        if (s.type === "Interstitial")                                 cls += " native";
        return `<span class="${cls}" title="${esc(s.detail)}">${esc(s.type)}</span>`;
      }).join("");
      if (result.notes) {
        const notesDiv = document.createElement("div");
        notesDiv.className = "banner-notes";
        notesDiv.textContent = result.notes;
        el.appendChild(notesDiv);
      }
    }
  } catch {
    el.textContent = "Detection error"; el.className = "banner-result";
  }
  if (btn) { btn.disabled = false; btn.textContent = "↻ Actualizar"; }
}

async function runPageContext() {
  try {
    const [{ result }] = await chrome.scripting.executeScript({
      target: { tabId: state.tabId },
      func: () => {
        const langFull = (document.documentElement.lang || navigator.language || "").toLowerCase();
        const text = document.body?.innerText?.slice(0, 50_000) || "";
        // Phone country codes (e.g. "+54 11", "+52 55")
        const phoneMatches = text.match(/\+\s?(\d{1,3})[\s\-(]/g) || [];
        const phoneCodes = [...new Set(phoneMatches.map(p => (p.match(/\d+/) || [""])[0]))].slice(0, 5);
        // Currency hints
        const currencyRx = /\b(ARS|MXN|COP|CLP|PEN|UYU|BRL|EUR|GBP|USD|JPY|CNY|INR|RUB|TRY|AED|SAR|EGP)\b|US\$|R\$|CHF|€|£|¥/gi;
        const currencies = [...new Set((text.match(currencyRx) || []).map(s => s.toUpperCase()))].slice(0, 5);
        const ogLocale = document.querySelector('meta[property="og:locale"]')?.content || "";

        // Footer scan: address-aware country detection
        // Footer suele tener "Buenos Aires, Argentina" / "México, CDMX" / "Av X 123, Lima, Perú"
        const footerEls = document.querySelectorAll("footer, .footer, #footer, [class*='footer'], address");
        let footerText = "";
        for (const el of footerEls) {
          footerText += (el.innerText || el.textContent || "") + "\n";
          if (footerText.length > 5000) break;
        }
        // Si no hay footer, usar últimos 3000 chars del body (suelen tener address)
        if (footerText.length < 100) footerText = text.slice(-3000);

        return {
          title:    document.title || "",
          lang:     langFull.substring(0, 2),
          langFull,
          ogLocale: ogLocale.toLowerCase(),
          phoneCodes,
          currencies,
          footerText: footerText.slice(0, 5000),
          description: document.querySelector('meta[name="description"]')?.content
                    || document.querySelector('meta[property="og:description"]')?.content
                    || "",
        };
      },
    });
    state.pageTitle       = (result?.title       || "").substring(0, 80);
    state.pageDescription = (result?.description || "").substring(0, 180);
    state.siteLanguage    = result?.lang || "";
    state.siteLangFull    = result?.langFull || "";
    state.siteOgLocale    = result?.ogLocale || "";
    state.sitePhoneCodes  = result?.phoneCodes || [];
    state.siteCurrencies  = result?.currencies || [];
    state.siteFooterText  = result?.footerText || "";
  } catch { /* sin permisos en esa página */ }
}

// Heurística multi-señal para inferir país cuando SimilarWeb no devuelve nada.
// Devuelve { code, name, source } o null. Ranking de señales:
//   1. lang con región (es-AR, en-GB, pt-BR)  — más fuerte
//   2. og:locale (es_AR)                       — idem
//   3. phone country code (+54, +52)          — fuerte
//   4. currency symbol (ARS, MXN, COP)         — fuerte
//   5. TLD país-específico (.com.ar, .es)     — medio (ya cubierto en traffic.js)
function inferCountryFromPageSignals() {
  const PHONE_TO_CC = {
    "54":"AR","52":"MX","57":"CO","56":"CL","55":"BR","51":"PE","598":"UY","595":"PY",
    "591":"BO","593":"EC","58":"VE","34":"ES","1":"US","44":"GB","33":"FR","49":"DE",
    "39":"IT","351":"PT","31":"NL","32":"BE","41":"CH","43":"AT","48":"PL","30":"GR",
    "36":"HU","420":"CZ","40":"RO","353":"IE","972":"IL","971":"AE","966":"SA","20":"EG",
    "212":"MA","90":"TR","91":"IN","81":"JP","82":"KR","86":"CN","65":"SG","60":"MY",
    "62":"ID","63":"PH","66":"TH","84":"VN","61":"AU","64":"NZ","27":"ZA","234":"NG",
    "7":"RU","380":"UA",
  };
  const CURRENCY_TO_CC = {
    ARS:"AR", MXN:"MX", COP:"CO", CLP:"CL", PEN:"PE", UYU:"UY", BRL:"BR",
    "R$":"BR", USD:"US", "US$":"US", GBP:"GB", "£":"GB", JPY:"JP", "¥":"JP",
    CNY:"CN", INR:"IN", RUB:"RU", TRY:"TR", AED:"AE", SAR:"SA", EGP:"EG",
    // EUR is ambiguous, skip
  };
  const LANG_REGION_TO_CC = {
    "es-ar":"AR","es-mx":"MX","es-co":"CO","es-cl":"CL","es-pe":"PE","es-uy":"UY",
    "es-py":"PY","es-bo":"BO","es-ec":"EC","es-ve":"VE","es-es":"ES","es-419":"AR",
    "pt-br":"BR","pt-pt":"PT","en-us":"US","en-gb":"GB","en-au":"AU","en-ca":"CA",
    "en-nz":"NZ","en-za":"ZA","fr-fr":"FR","fr-ca":"CA","de-de":"DE","de-at":"AT",
    "de-ch":"CH","it-it":"IT","nl-nl":"NL","nl-be":"BE","pl-pl":"PL","ru-ru":"RU",
    "tr-tr":"TR","ja-jp":"JP","ko-kr":"KR","zh-cn":"CN","zh-hk":"HK","zh-tw":"TW",
    "ar-sa":"SA","ar-eg":"EG","ar-ae":"AE",
  };
  // 1. lang con región
  const lf = (state.siteLangFull || "").replace("_","-").toLowerCase();
  if (LANG_REGION_TO_CC[lf]) return { code: LANG_REGION_TO_CC[lf], source: "lang-region" };
  // 2. og:locale (mismo formato típicamente)
  const og = (state.siteOgLocale || "").replace("_","-").toLowerCase();
  if (LANG_REGION_TO_CC[og]) return { code: LANG_REGION_TO_CC[og], source: "og-locale" };
  // 3. Phone country code
  for (const code of (state.sitePhoneCodes || [])) {
    if (PHONE_TO_CC[code]) return { code: PHONE_TO_CC[code], source: "phone" };
  }
  // 4. Currency
  for (const cur of (state.siteCurrencies || [])) {
    if (CURRENCY_TO_CC[cur]) return { code: CURRENCY_TO_CC[cur], source: "currency" };
  }
  // 5. Footer / address scan — busca menciones de país (Argentina, México, España, etc.)
  //    Match más fuerte si está al final del texto (típico de address en footer).
  const FOOTER_NAME_TO_CC = {
    "argentina":"AR","méxico":"MX","mexico":"MX","colombia":"CO","chile":"CL","perú":"PE","peru":"PE",
    "uruguay":"UY","paraguay":"PY","bolivia":"BO","ecuador":"EC","venezuela":"VE","brasil":"BR","brazil":"BR",
    "españa":"ES","spain":"ES","estados unidos":"US","united states":"US","reino unido":"GB","united kingdom":"GB",
    "francia":"FR","france":"FR","alemania":"DE","germany":"DE","italia":"IT","italy":"IT","portugal":"PT",
    "países bajos":"NL","holanda":"NL","netherlands":"NL","bélgica":"BE","belgium":"BE","suiza":"CH","switzerland":"CH",
    "austria":"AT","polonia":"PL","poland":"PL","hungría":"HU","hungary":"HU","república checa":"CZ","czech":"CZ",
    "rumanía":"RO","romania":"RO","grecia":"GR","greece":"GR","irlanda":"IE","ireland":"IE","luxemburgo":"LU","luxembourg":"LU",
    "suecia":"SE","sweden":"SE","noruega":"NO","norway":"NO","dinamarca":"DK","denmark":"DK","finlandia":"FI","finland":"FI",
    "turquía":"TR","turkey":"TR","israel":"IL","emiratos árabes":"AE","uae":"AE","arabia saudita":"SA","saudi arabia":"SA",
    "marruecos":"MA","morocco":"MA","egipto":"EG","egypt":"EG","india":"IN","japón":"JP","japan":"JP",
    "corea del sur":"KR","south korea":"KR","china":"CN","singapur":"SG","singapore":"SG","filipinas":"PH","philippines":"PH",
    "indonesia":"ID","vietnam":"VN","tailandia":"TH","thailand":"TH","sudáfrica":"ZA","south africa":"ZA","nigeria":"NG",
    "australia":"AU","nueva zelanda":"NZ","new zealand":"NZ","canadá":"CA","canada":"CA",
    "costa rica":"CR","panamá":"PA","panama":"PA","guatemala":"GT","honduras":"HN","el salvador":"SV","nicaragua":"NI",
    "cuba":"CU","puerto rico":"PR","república dominicana":"DO",
  };
  const footerLower = (state.siteFooterText || "").toLowerCase();
  if (footerLower) {
    // Busca el match más al final del texto (más confiable — típicamente la address está al final)
    let bestMatch = null;
    let bestPos = -1;
    for (const [name, code] of Object.entries(FOOTER_NAME_TO_CC)) {
      // Word boundary: precedido por coma, espacio, salto de línea o inicio
      const rx = new RegExp(`(?:^|[,\\s.\\n])${name.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")}(?:[,.\\s\\n]|$)`, "i");
      const idx = footerLower.search(rx);
      if (idx > bestPos) { bestPos = idx; bestMatch = code; }
    }
    if (bestMatch) return { code: bestMatch, source: "footer-address" };
  }
  return null;
}

async function runEmailScraper() {
  const el = document.getElementById("email-result");
  try {
    // Si ya tenemos emails en caché de sesión (misma visita al dominio), usarlos
    // Igualmente re-scrapeamos la página actual para no perdernos emails de subpáginas
    const sess = await getSessionCache(state.domain);
    // Page scrape (free) + Apollo (paid pero auto, decisión user 2026-05-08:
    // mostrar al menos 2 emails de Apollo por default sin necesidad de click).
    // Apollo se cachea en session, así que re-visitar subpáginas no re-paga.
    state.emails = []; state.emailSources = new Map();
    addEmailsWithSource((sess?.emails || []).filter(quickValidateEmail), "Cache");

    const [pageEmails, apolloResult] = await Promise.all([
      scrapeEmailsFromPage(state.tabId),
      // Si la cache de sesión ya tiene Apollo emails, no re-disparar
      (sess?.apolloPreloaded)
        ? Promise.resolve(null)
        : findDecisionMakerViaApollo(state.domain).catch(() => null),
    ]);
    addEmailsWithSource(pageEmails.filter(quickValidateEmail), "Page");

    // Apollo: agregar los unlocked al pool + render preview
    if (apolloResult) {
      const unlockedEmails = (apolloResult.people || []).filter(p => p.unlocked && p.email).map(p => p.email);
      if (unlockedEmails.length) {
        addEmailsWithSource(unlockedEmails, "Apollo");
      }
      if (apolloResult.name)     state.decisionMakerName = apolloResult.name.split(" ")[0];
      if (apolloResult.linkedin) showLinkedIn(apolloResult.linkedin);
      // Pre-render Apollo people block (default 2 visible + ver más)
      const apolloEl = document.getElementById("apollo-result");
      if (apolloEl) renderApolloPeople(apolloEl, apolloResult);
      // Persistir Apollo en session cache para subpáginas
      try {
        const cur = await getSessionCache(state.domain) || {};
        await setSessionCache(state.domain, { ...cur, apolloPreloaded: true });
      } catch {}
    }

    const allEmails = state.emails;
    if (allEmails.length > 0 || state.duplicate?.email) {
      renderEmailList(allEmails);
      autoPushReady.email = true;
      checkAutoPush();
    } else {
      renderEmailList([]);
    }
  } catch {
    el.textContent = "Scraping error"; el.className = "email-value";
  }

  updateScore();

  const isNew = !state.duplicate?.found;
  await saveHistory({
    domain:     state.domain,
    mediaBuyer: state.mediaBuyer,
    pageViews:  state.traffic,
    rawVisits:  state.visits,
    isNew,
    ejecutivo:  state.mediaBuyer,
    email:      state.emails[0] || "",
    partners:   state.techStack,
    geo:        detectGeo(),
    date:       new Date().toISOString().split("T")[0],
  });

  // Actualizar contador persistente POR USUARIO (nunca se borra con Limpiar)
  {
    const nowMonth = new Date().toISOString().substring(0, 7);
    const statsKey = userKey("historyStats");
    const defaults = { total: 0, month: nowMonth, monthNew: 0, monthDups: 0 };
    const { [statsKey]: hs = defaults } = await chrome.storage.local.get(statsKey);

    // Resetear contadores si cambió el mes
    if (hs.month !== nowMonth) {
      hs.month     = nowMonth;
      hs.monthNew  = 0;
      hs.monthDups = 0;
    }

    hs.total++;
    if (isNew) { hs.monthNew++;  }
    else        { hs.monthDups++; }

    await chrome.storage.local.set({ [statsKey]: hs });
  }

  // Solo refrescar si el modal de historial está abierto
  if (document.getElementById("history-modal")?.style.display === "flex") {
    await loadHistoryTab();
  }
}

// ============================================================
// SCORE
// ============================================================
function updateScore() {
  if (!state.trafficData) return;
  const result = scoreProspect({
    pageViews:  state.traffic,
    rawVisits:  state.visits,
    partners:   state.partners?.length > 0 ? state.partners : null,
    emailFound: state.emails?.length > 0 ? true : (state.emails?.length === 0 ? false : null),
  });
  state.score = result;
  renderScore(result);
}

function renderScore(s) {
  const el = document.getElementById("score-badge");
  if (!el) return;
  el.style.display = "flex";
  el.innerHTML = `
    <div class="score-grade" style="background:${s.color}">${s.grade}</div>
    <span class="score-label">${s.label}</span>
  `;
}

// ============================================================
// AUTO-PUSH
// ============================================================
function checkAutoPush() {
  if (autoPushReady.traffic && autoPushReady.notDup && autoPushReady.email) {
    const banner = document.getElementById("autopush-banner");
    if (banner) banner.style.display = "block";
  }
}

// ============================================================
// EMAIL
// ============================================================
function addEmailsWithSource(emails, source) {
  for (const e of emails) {
    if (!e) continue;
    // Filtro garbage al ingreso — no contamina state.emails con whois/abuse/etc.
    if (isGarbageEmail(e)) continue;
    if (!state.emailSources.has(e)) state.emailSources.set(e, source);
    if (!state.emails.includes(e)) state.emails.push(e);
  }
}

// Cache de resultados de verifyEmail para no re-verificar el mismo email
// dentro de la misma sesión del side panel.
const _emailVerifyCache = new Map(); // email -> {valid, tags, reason, score}

function _verifyClass(result) {
  if (!result) return "verify-pending";
  const tags = result.tags || [];
  // Rojo: garbage absoluta o SMTP rechazó
  if (!result.valid || tags.includes("typo") || tags.includes("descartable") ||
      tags.includes("descartable-remoto") || tags.includes("undeliverable") ||
      tags.includes("spam") || tags.includes("sin-dns") || tags.includes("sin-mx") ||
      tags.includes("proxy-whois")) {
    return "verify-bad";
  }
  // Amarillo: válido pero dudoso (rol, catch-all, TLD raro)
  if (tags.includes("rol") || tags.includes("tld-sospechoso") ||
      tags.includes("catch-all") || tags.includes("catch-all-provider")) {
    return "verify-warn";
  }
  return "verify-good";
}

function _verifyTooltip(result) {
  if (!result) return "Verificando…";
  const status = result.valid ? "✔ Válido" : "✖ Inválido";
  const reason = result.reason || "";
  const tags   = (result.tags || []).join(", ");
  const src    = result.deepSource === "eva" ? "[SMTP confirmado]"
               : result.deepSource === "local-only" ? "[solo DNS local]"
               : "";
  const cache  = result.fromCache ? "[cache]" : "";
  return [status, reason, tags ? `[${tags}]` : "", src, cache].filter(Boolean).join(" — ");
}

async function autoVerifyEmailChips(listEl) {
  const chips = [...listEl.querySelectorAll(".email-chip[data-email]")];
  // Limit a 12 emails en paralelo para no saturar (DNS-over-HTTPS soporta esto sin drama)
  const batch = chips.slice(0, 12);
  await Promise.all(batch.map(async (chip) => {
    const email = chip.dataset.email;
    if (!email) return;
    let result = _emailVerifyCache.get(email);
    if (!result) {
      // verifyEmailDeep = local + remote (eva.pingutil con cache 30 días)
      try { result = await verifyEmailDeep(email); }
      catch { result = { valid: false, reason: "Error verifying", tags: ["error"] }; }
      _emailVerifyCache.set(email, result);
    }
    const cls = _verifyClass(result);
    chip.classList.remove("verify-pending", "verify-good", "verify-warn", "verify-bad");
    chip.classList.add(cls);
    chip.title = _verifyTooltip(result);
    // Si era el chip seleccionado, sincronizar el badge global con el color
    const badge = document.getElementById("email-verify-badge");
    if (chip.classList.contains("selected") && badge) {
      _renderVerifyBadge(badge, result);
    }
  }));
}

function _renderVerifyBadge(badge, result) {
  if (!result) { badge.style.display = "none"; return; }
  const cls = _verifyClass(result);
  badge.style.display = "inline-block";
  badge.title = _verifyTooltip(result);
  if (cls === "verify-good") { badge.textContent = "✔"; badge.className = "verify-badge ok"; }
  else if (cls === "verify-warn") { badge.textContent = "⚠"; badge.className = "verify-badge unknown"; }
  else { badge.textContent = "✖"; badge.className = "verify-badge fail"; }
}

function renderEmailList(emails) {
  const resultEl = document.getElementById("email-result");
  const listEl   = document.getElementById("email-list");
  const badge    = document.getElementById("email-verify-badge");
  const formEl   = document.getElementById("form-email");

  const isDup       = state.duplicate?.found;
  const mondayEmail = isDup ? (state.duplicate.email || "").trim() : "";

  // 1. Dedupe + excluir el email de Monday + DESCARTAR garbage (whois/proxy/abuse).
  //    Estos emails nunca deberían aparecer en la UI — son inservibles.
  const cleaned = [...new Set(emails.map(e => e.trim()).filter(Boolean))]
    .filter(e => e !== mondayEmail)
    .filter(e => !isGarbageEmail(e));

  // 2. ORDEN: Apollo primero (siempre), después scraping/Gemini/otros.
  //    Dentro de cada bucket, conservar el orden original.
  const isApollo = (e) => (state.emailSources.get(e) || "").toLowerCase() === "apollo";
  const suggested = [
    ...cleaned.filter(isApollo),
    ...cleaned.filter(e => !isApollo(e)),
  ];

  if (!mondayEmail && suggested.length === 0) {
    resultEl.style.display = "block";
    resultEl.textContent   = "Sin emails válidos — probá Apollo";
    resultEl.className     = "email-value";
    listEl.style.display   = "none";
    return;
  }

  resultEl.style.display = "none";
  listEl.style.display   = "block";
  listEl.className       = "email-list";
  badge.style.display    = "none";

  let html = "";

  const chipFor = (email, extraClass = "") => {
    const src = state.emailSources.get(email) || "";
    const srcBadge = src ? `<span class="email-src-badge">${esc(src)}</span>` : "";
    const cached = _emailVerifyCache.get(email);
    const verCls = cached ? _verifyClass(cached) : "verify-pending";
    return `<div class="email-chip ${extraClass} ${verCls}" data-email="${esc(email)}" title="Verificando…">${esc(email)}${srcBadge}</div>`;
  };

  if (mondayEmail) {
    html += `<div class="email-group-label">📋 Actual (Monday)</div>${chipFor(mondayEmail, "monday")}`;
  }

  if (suggested.length > 0) {
    const VISIBLE = 7;
    const hidden  = suggested.length > VISIBLE ? suggested.slice(VISIBLE) : [];
    const visible = suggested.slice(0, VISIBLE);
    html += `<div class="email-group-label">${mondayEmail ? "💡 Sugeridas" : "📧 Encontradas"} (${suggested.length})</div>`;
    visible.forEach(email => { html += chipFor(email); });
    if (hidden.length > 0) {
      html += `<div class="email-chips-hidden" style="display:none">`;
      hidden.forEach(email => { html += chipFor(email); });
      html += `</div>`;
      html += `<button class="email-show-more" type="button" style="font-size:10px;background:transparent;border:none;color:var(--adeq-blue);cursor:pointer;padding:4px 0;text-decoration:underline">+ ver ${hidden.length} más…</button>`;
    }
  }

  listEl.innerHTML = html;

  // Toggle "ver más"
  const showMoreBtn = listEl.querySelector(".email-show-more");
  if (showMoreBtn) {
    showMoreBtn.addEventListener("click", () => {
      const hiddenBlock = listEl.querySelector(".email-chips-hidden");
      if (hiddenBlock) {
        hiddenBlock.style.display = "block";
        showMoreBtn.style.display = "none";
      }
    });
  }

  // Seleccionar el primero por defecto
  const firstChip = listEl.querySelector(".email-chip");
  if (firstChip) {
    firstChip.classList.add("selected");
    formEl.value = firstChip.dataset.email;
  }

  // Click para seleccionar
  listEl.querySelectorAll(".email-chip").forEach(chip => {
    chip.addEventListener("click", () => {
      listEl.querySelectorAll(".email-chip").forEach(c => c.classList.remove("selected"));
      chip.classList.add("selected");
      formEl.value = chip.dataset.email;
      // Sincronizar badge con el resultado cacheado del chip seleccionado
      const cached = _emailVerifyCache.get(chip.dataset.email);
      _renderVerifyBadge(badge, cached);
    });
  });

  // Auto-verify en background — sin bloquear el render
  autoVerifyEmailChips(listEl).catch(e => console.warn("[autoVerify]", e));
}

function setEmail(email) {
  if (email && !state.emails.includes(email)) state.emails.unshift(email);
  renderEmailList(state.emails);
}

async function verifyCurrentEmail() {
  const email = document.getElementById("form-email").value.trim();
  const badge = document.getElementById("email-verify-badge");
  if (!email || !email.includes("@")) return;

  badge.textContent   = "...";
  badge.className     = "verify-badge unknown";
  badge.style.display = "inline-block";

  const result = await verifyEmail(email);
  const tags   = result.tags || [];

  if (!result.valid || tags.includes("typo") || tags.includes("descartable") ||
      tags.includes("sin-dns") || tags.includes("sin-mx")) {
    badge.textContent = "✖ Invalid";
    badge.className   = "verify-badge fail";
    badge.title       = result.reason + (result.suggestion ? ` → ${result.suggestion}` : "");
  } else if (tags.includes("rol")) {
    badge.textContent = "⚠ Role Address";
    badge.className   = "verify-badge unknown";
    badge.title       = result.reason;
  } else if (result.valid) {
    badge.textContent = "✔ Valid";
    badge.className   = "verify-badge ok";
    badge.title       = result.reason;
  } else {
    badge.textContent = "✖ Invalid";
    badge.className   = "verify-badge fail";
    badge.title       = result.reason;
  }
}

function showLinkedIn(url) {
  if (!url) return;
  const row  = document.getElementById("linkedin-row");
  const link = document.getElementById("linkedin-link");
  link.href = url; row.style.display = "block";
}

// Render Apollo people list (unlocked + locked + no-email) with reveal buttons
function renderApolloPeople(resultEl, result) {
  const people = result.people || [];
  if (!people.length) {
    resultEl.textContent = result.note || "No Apollo data";
    return;
  }

  const summary = result.note || `${people.length} people`;
  // Por default muestra hasta 2 personas; el resto detrás de un "Ver más" toggle.
  // Decisión user 2026-05-08: Apollo auto-trigger + preview de 2 emails por
  // default sin click adicional.
  const PREVIEW_COUNT = 2;
  const renderRow = (p, i) => {
    const emailCell = p.unlocked
      ? `<a href="mailto:${esc(p.email)}" class="apollo-row-email">${esc(p.email)}</a>`
      : p.email
        ? `<span class="apollo-row-locked" title="${esc(p.status||"")}">🔒 locked</span>
           <button class="apollo-reveal-btn" data-idx="${i}" title="Unlock (uses 1 Apollo credit)">🔓 Reveal</button>`
        : `<span class="apollo-row-locked">— no email</span>
           <button class="apollo-reveal-btn" data-idx="${i}" title="Try to reveal via match (uses 1 credit)">🔓 Try</button>`;
    const linkedin = p.linkedin
      ? `<a href="${esc(p.linkedin)}" target="_blank" rel="noopener" class="apollo-row-link">in</a>`
      : "";
    return `
      <div class="apollo-row" data-idx="${i}">
        <div class="apollo-row-main">
          <div class="apollo-row-name">${esc(p.name || "(no name)")} ${linkedin}</div>
          <div class="apollo-row-title">${esc(p.title || "")}</div>
        </div>
        <div class="apollo-row-email-cell">${emailCell}</div>
      </div>`;
  };
  const visibleRows = people.slice(0, PREVIEW_COUNT).map(renderRow).join("");
  const hiddenRows  = people.slice(PREVIEW_COUNT).map(renderRow).join("");
  const showMoreBtn = people.length > PREVIEW_COUNT
    ? `<button class="apollo-show-more" type="button" style="font-size:10px;background:transparent;border:none;color:#0369a1;cursor:pointer;padding:4px 0;text-decoration:underline;width:100%;text-align:left">+ ver ${people.length - PREVIEW_COUNT} más…</button>`
    : "";

  resultEl.innerHTML = `
    <details class="apollo-details" open>
      <summary class="apollo-summary">👥 ${esc(summary)} <span class="apollo-toggle">click to toggle</span></summary>
      <div class="apollo-list">
        ${visibleRows}
        ${hiddenRows ? `<div class="apollo-hidden" style="display:none">${hiddenRows}</div>` : ""}
        ${showMoreBtn}
      </div>
    </details>
  `;
  // Wire show-more
  resultEl.querySelector(".apollo-show-more")?.addEventListener("click", (e) => {
    const block = resultEl.querySelector(".apollo-hidden");
    if (block) { block.style.display = "block"; e.target.style.display = "none"; }
  });

  // Reveal click handlers
  resultEl.querySelectorAll(".apollo-reveal-btn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      const idx = parseInt(btn.dataset.idx);
      const person = people[idx];
      btn.disabled = true; btn.textContent = "⏳";
      const r = await revealApolloEmail({
        id:         person.id,
        first_name: person.first_name,
        last_name:  person.last_name,
        domain:     state.domain,
      });
      if (r.ok && r.person?.email) {
        people[idx] = r.person;
        addEmailsWithSource([r.person.email], "Apollo");
        setEmail(r.person.email);
        autoPushReady.email = true; checkAutoPush(); updateScore();
        renderApolloPeople(resultEl, result); // re-render with new state
      } else {
        btn.disabled = false;
        btn.textContent = "❌";
        btn.title = r.error || "Reveal failed";
      }
    });
  });
}

// ============================================================
// BOTONES
// ============================================================
// ── RAG helpers (Voyage embeddings + Supabase pgvector retrieval) ──
// Returns { likeBodies: string[], dislikeBodies: string[] }. Empty arrays on
// any failure — caller silently falls back to heuristic favorites.
async function ragRetrievePitchExamples({ domain, category, geo, language, traffic }) {
  try {
    const ctxStr = buildPitchContext({ domain, category, geo, language, traffic });
    const embedding = await voyageEmbed(ctxStr, "query");
    if (!embedding) return { likeBodies: [], dislikeBodies: [] };
    const [likes, dislikes] = await Promise.all([
      matchPitchFeedback(state.accessToken, state.loginEmail, embedding, "liked",    3),
      matchPitchFeedback(state.accessToken, state.loginEmail, embedding, "disliked", 2),
    ]);
    return {
      likeBodies:    likes.map(r => r.pitch_body).filter(Boolean),
      dislikeBodies: dislikes.map(r => r.pitch_body).filter(Boolean),
    };
  } catch (e) {
    console.warn("[RAG] retrieval failed:", e.message);
    return { likeBodies: [], dislikeBodies: [] };
  }
}

// Save feedback (liked/disliked) into the RAG store with embedded context.
// Best-effort: non-blocking, never throws to caller.
async function ragSavePitchFeedback(action, pitchBody, pitchSubject, ctxFields) {
  try {
    // reason (si lo hay) entra al context para que el embedding capture el porqué
    // del dislike y el RAG futuro pueda evitar errores similares.
    const { reason, ...ctxClean } = ctxFields || {};
    let ctxStr = buildPitchContext(ctxClean);
    if (reason) ctxStr += `\nUSER_FEEDBACK_REASON: ${reason}`;
    const embedding = await voyageEmbed(ctxStr, "document");
    if (!embedding) return;
    await insertPitchFeedback(state.accessToken, state.loginEmail, {
      ...ctxClean,
      pitch_body:    pitchBody,
      pitch_subject: pitchSubject || "",
      context:       ctxStr,
      embedding,
      action,
    });
  } catch (e) {
    console.warn("[RAG] save failed:", e.message);
  }
}

// Auto-save del pitch text en chrome.storage.local (debounce 3s).
// Persiste por dominio. Si el user cierra la toolbar antes de mandar,
// al volver al mismo dominio se restaura el draft.
let _pitchDraftTimer = null;
function _autoSavePitchDraft() {
  const ta = document.getElementById("pitch-text");
  if (!ta || !state.domain) return;
  clearTimeout(_pitchDraftTimer);
  _pitchDraftTimer = setTimeout(() => {
    const v = ta.value;
    if (!v || v.trim().length < 20) return; // no guardar drafts triviales
    chrome.storage.local.set({ [`_draft_pitch_${state.domain}`]: { body: v, ts: Date.now() } }).catch(() => {});
  }, 3000);
}
async function _restorePitchDraft() {
  const ta = document.getElementById("pitch-text");
  if (!ta || !state.domain || ta.value) return; // no pisar pitch ya generado
  try {
    const key = `_draft_pitch_${state.domain}`;
    const { [key]: draft } = await chrome.storage.local.get(key);
    if (!draft?.body) return;
    // TTL 7d — drafts viejos se descartan
    if (Date.now() - (draft.ts || 0) > 7 * 86_400_000) {
      chrome.storage.local.remove(key).catch(() => {});
      return;
    }
    ta.value = draft.body;
    showToast(`📝 Draft restaurado para ${state.domain} (${Math.round((Date.now() - draft.ts) / 60000)}m atrás)`, "info");
  } catch {}
}

async function bindButtons() {

  // Auto-save draft del pitch (debounce 3s) + restore al cargar
  document.getElementById("pitch-text")?.addEventListener("input", _autoSavePitchDraft);
  _restorePitchDraft();

  // Verificar email
  // btn-verify-email REMOVIDO — la verificación es automática on-render (autoVerifyEmailChips)

  // Apollo
  document.getElementById("btn-apollo").addEventListener("click", async () => {
    const btn      = document.getElementById("btn-apollo");
    const resultEl = document.getElementById("apollo-result");
    btn.disabled   = true; btn.textContent = "⏳...";
    const result   = await findDecisionMakerViaApollo(state.domain);

    // 1. Unlocked emails → add to email list immediately (preview)
    const unlockedEmails = (result.people || []).filter(p => p.unlocked && p.email).map(p => p.email);
    if (unlockedEmails.length) {
      addEmailsWithSource(unlockedEmails, "Apollo");
      setEmail(unlockedEmails[0]);
      autoPushReady.email = true;
      checkAutoPush();
      updateScore();
    }
    if (result.name) state.decisionMakerName = result.name.split(" ")[0];
    if (result.linkedin) showLinkedIn(result.linkedin);

    // 2. Render full people list on click-to-expand
    renderApolloPeople(resultEl, result);

    btn.disabled = false; btn.textContent = "👤 Apollo";
  });

  // Botón Gemini email — REMOVIDO. La búsqueda Gemini consumía $$ y nadie lo
  // tildaba. El scraper + Apollo cubren los emails reales.

  // Fecha Hoy
  document.getElementById("btn-fecha-hoy").addEventListener("click", () => {
    document.getElementById("form-fecha").value = toDisplayDate(new Date().toISOString().split("T")[0]);
  });

  // Banner detector — refresh manual
  document.getElementById("btn-detect-banners").addEventListener("click", async () => {
    const btn = document.getElementById("btn-detect-banners");
    btn.disabled = true; btn.textContent = "⏳...";
    await runBannerDetection();
    btn.disabled = false; btn.textContent = "↻ Actualizar";
  });

  // Generar Pitch
  // ── Pitch style: pills CICLAN al click — solo el active es visible ─
  // Cada grupo tiene N pills (.active visible, resto hidden). Click en el
  // visible avanza al siguiente del array. Compatible con getPitchConfig.
  const cycleGroup = (group) => {
    const pills  = [...group.querySelectorAll(".pitch-pill")];
    const curIdx = pills.findIndex(p => p.classList.contains("active"));
    const nextIdx = (curIdx + 1) % pills.length;
    pills.forEach((p, i) => {
      p.classList.toggle("active", i === nextIdx);
      p.hidden = i !== nextIdx;
    });
  };
  document.querySelectorAll(".pitch-cycle-pills").forEach(group => {
    group.addEventListener("click", () => cycleGroup(group));
  });
  // Compat: viejo .pitch-pills (no-cycle) sigue funcionando
  document.querySelectorAll(".pitch-pills:not(.pitch-cycle-pills)").forEach(group => {
    group.querySelectorAll(".pitch-pill").forEach(pill => {
      pill.addEventListener("click", () => {
        group.querySelectorAll(".pitch-pill").forEach(p => p.classList.remove("active"));
        pill.classList.add("active");
      });
    });
  });

  function getPitchConfig() {
    // Soporta tanto .pitch-cycle-pills como .pitch-pills (legacy)
    const val = (group) => document.querySelector(`[data-group="${group}"] .pitch-pill.active`)?.dataset.val || "";
    return {
      tone:    val("tone")    || "informal",
      length:  val("length")  || "short",
      focus:   val("focus")   || "analysis",
      opening: val("opening") || "direct",
    };
  }

  function pitchConfigKey(cfg) {
    return `pitch_favs_${cfg.tone}_${cfg.length}_${cfg.focus}_${cfg.opening}`;
  }

  async function loadFavPitches(cfg) {
    const key = userKey(pitchConfigKey(cfg));
    const stored = await chrome.storage.local.get(key);
    return stored[key] || [];
  }

  async function saveFavPitch(cfg, pitch) {
    const key    = userKey(pitchConfigKey(cfg));
    const stored = await chrome.storage.local.get(key);
    const favs   = stored[key] || [];
    if (favs.includes(pitch)) return;
    favs.unshift(pitch);
    await chrome.storage.local.set({ [key]: favs.slice(0, 5) });
  }

  async function loadDislikePitches() {
    const key = userKey("pitch_dislikes");
    const stored = await chrome.storage.local.get(key);
    return stored[key] || [];
  }

  async function saveDislikePitch(pitch) {
    const key    = userKey("pitch_dislikes");
    const stored = await chrome.storage.local.get(key);
    const list   = stored[key] || [];
    if (list.includes(pitch)) return;
    list.unshift(pitch);
    await chrome.storage.local.set({ [key]: list.slice(0, 5) });
  }

  // RAG helpers are defined at module scope (see ragRetrievePitchExamples / ragSavePitchFeedback below).

  function showSubjectChips(subjects) {
    const chipsEl   = document.getElementById("pitch-subjects");
    const subjectEl = document.getElementById("form-subject");
    if (!chipsEl) return;
    if (!subjects?.length) { chipsEl.style.display = "none"; return; }
    chipsEl.style.display = "block";
    chipsEl.innerHTML =
      `<div class="subject-chips-label">Asuntos sugeridos:</div>` +
      subjects.map(s => `<button class="subject-chip" data-subject="${esc(s)}">${esc(s)}</button>`).join("");
    chipsEl.querySelectorAll(".subject-chip").forEach(btn => {
      btn.addEventListener("click", () => {
        if (subjectEl) subjectEl.value = btn.dataset.subject;
        chipsEl.querySelectorAll(".subject-chip").forEach(b => b.classList.remove("selected"));
        btn.classList.add("selected");
      });
    });
  }

  document.getElementById("btn-generate-pitch").addEventListener("click", async () => {
    const btn        = document.getElementById("btn-generate-pitch");
    const likeBtn    = document.getElementById("btn-pitch-like");
    const dislikeBtn = document.getElementById("btn-pitch-dislike");
    const likeStatus = document.getElementById("pitch-like-status");
    const ta         = document.getElementById("pitch-text");
    btn.disabled = true; btn.textContent = "⏳ Generating..."; ta.value = "";
    likeBtn.style.display = "none"; dislikeBtn.style.display = "none";
    likeStatus.textContent = "";
    const chipsEl = document.getElementById("pitch-subjects");
    if (chipsEl) chipsEl.style.display = "none";
    try {
      const category    = document.getElementById("pitch-category")?.value || state.category;
      const siteLanguage = document.getElementById("pitch-language")?.value || state.siteLanguage || "en";
      const cfg         = getPitchConfig();
      const [favLocal, dislLocal, rag] = await Promise.all([
        loadFavPitches(cfg),
        loadDislikePitches(),
        ragRetrievePitchExamples({ domain: state.domain, category, geo: detectGeo?.() || "", language: siteLanguage, traffic: state.traffic }),
      ]);
      // Merge: RAG first (semantically relevant), then heuristic, dedupe, cap
      const favExamples = [...new Set([...rag.likeBodies,    ...favLocal])].slice(0, 5);
      const dislikes    = [...new Set([...rag.dislikeBodies, ...dislLocal])].slice(0, 5);
      const bannerInfo  = state.banners?.summary?.map(s =>
        s.detail ? `${s.type} (${s.detail})` : s.type
      ).join(", ") || "";
      const result = await generatePitch({
        domain: state.domain, traffic: state.traffic,
        techStack: state.techStack, adsTxt: state.adsTxt,
        revenueGap: state.revenueGap, banners: bannerInfo, category,
        siteLanguage,
        pageTitle: state.pageTitle, pageDescription: state.pageDescription,
        decisionMakerName: state.decisionMakerName,
        previousPitches: state.generatedPitches.slice(-3),
        dislikes,
        customPrompt: state.customPrompt || "",
        ...cfg, favExamples,
      });
      state.pitch = result.body;
      ta.value    = result.body;
      // Guardar en historial de pitches de esta sesión (para anti-repetición)
      state.generatedPitches.push(result.body);
      if (state.generatedPitches.length > 5) state.generatedPitches.shift();
      // Asuntos sugeridos — siempre overwrite con el primero del idioma correcto
      showSubjectChips(result.subjects);
      const subjectEl = document.getElementById("form-subject");
      if (subjectEl && result.subjects?.[0]) {
        subjectEl.value = result.subjects[0];
      }
      btn.textContent = "✨ Regenerar";
      likeBtn.style.display = "inline-block";
      dislikeBtn.style.display = "inline-block";
    } catch (err) {
      ta.value = `Error: ${err.message}`; btn.textContent = "✨ Generar Pitch";
    }
    btn.disabled = false;
  });

  document.getElementById("btn-pitch-like").addEventListener("click", async () => {
    const pitch   = document.getElementById("pitch-text").value.trim();
    const subject = document.getElementById("form-subject")?.value?.trim() || "";
    if (!pitch) return;
    const cfg = getPitchConfig();
    await saveFavPitch(cfg, pitch);
    ragSavePitchFeedback("liked", pitch, subject, {
      domain: state.domain, category: state.category,
      geo: detectGeo?.() || "", language: state.siteLanguage || "", traffic: state.traffic || 0,
    });
    const likeStatus = document.getElementById("pitch-like-status");
    likeStatus.textContent = "✓ Saved as style example (RAG)";
    setTimeout(() => { likeStatus.textContent = ""; }, 2500);
  });

  document.getElementById("btn-pitch-dislike").addEventListener("click", async () => {
    const pitch   = document.getElementById("pitch-text").value.trim();
    const subject = document.getElementById("form-subject")?.value?.trim() || "";
    if (!pitch) return;
    // Pedir razón opcional — entra al embedding para que el RAG aprenda QUÉ evitar.
    // Vacío = sin comentario, igual marca dislike.
    const reason = prompt(
      "👎 ¿Qué falló? (opcional — ayuda a la IA a evitar ese error en el futuro)\n\n" +
      "Ejemplos: 'inventó un mes', 'dijo que no tiene ads.txt cuando sí tiene', 'tono muy formal', 'muy largo'",
      ""
    );
    await saveDislikePitch(pitch);
    ragSavePitchFeedback("disliked", pitch, subject, {
      domain: state.domain, category: state.category,
      geo: detectGeo?.() || "", language: state.siteLanguage || "", traffic: state.traffic || 0,
      reason: (reason || "").trim().substring(0, 500),
    });
    const likeStatus = document.getElementById("pitch-like-status");
    likeStatus.textContent = reason ? "✗ Saved with reason (RAG)" : "✗ Marked to avoid (RAG)";
    setTimeout(() => { likeStatus.textContent = ""; }, 2500);
  });

  // Auto-push "Preparar todo"
  document.getElementById("btn-autopush-prepare").addEventListener("click", async () => {
    const btn     = document.getElementById("btn-autopush-prepare");
    const stepsEl = document.getElementById("autopush-steps");
    btn.disabled  = true; btn.textContent = "⏳ Preparando...";

    try {
      const category    = document.getElementById("pitch-category")?.value || state.category;
      const cfg         = getPitchConfig();
      const [favLocal, dislLocal, rag] = await Promise.all([
        loadFavPitches(cfg),
        loadDislikePitches(),
        ragRetrievePitchExamples({ domain: state.domain, category, geo: detectGeo?.() || "", language: state.siteLanguage, traffic: state.traffic }),
      ]);
      const favExamples = [...new Set([...rag.likeBodies,    ...favLocal])].slice(0, 5);
      const dislikes    = [...new Set([...rag.dislikeBodies, ...dislLocal])].slice(0, 5);
      const result      = await generatePitch({
        domain: state.domain, traffic: state.traffic,
        techStack: state.techStack, adsTxt: state.adsTxt,
        revenueGap: state.revenueGap, category,
        siteLanguage: state.siteLanguage,
        pageTitle: state.pageTitle, pageDescription: state.pageDescription,
        decisionMakerName: state.decisionMakerName,
        previousPitches: state.generatedPitches.slice(-3),
        dislikes,
        customPrompt: state.customPrompt || "",
        ...cfg, favExamples,
      });
      state.pitch = result.body;
      document.getElementById("pitch-text").value = result.body;
      state.generatedPitches.push(result.body);
      if (state.generatedPitches.length > 5) state.generatedPitches.shift();
      showSubjectChips(result.subjects);

      const subjectEl = document.getElementById("form-subject");
      if (subjectEl && result.subjects?.[0]) subjectEl.value = result.subjects[0];
      // NO fallback en inglés — si Gemini no devolvió subjects, dejar vacío para que el user lo complete

      stepsEl.innerHTML = `
        <div class="autopush-step">✅ Pitch generated</div>
        <div class="autopush-step">✅ Subject pre-filled</div>
        <div class="autopush-step">→ Review the form and send with the buttons below</div>
      `;
      btn.textContent = "✅ Ready — review and send";
    } catch (err) {
      console.error("[Prepare all]", err);
      stepsEl.innerHTML = `<div class="autopush-step" style="color:var(--danger)">❌ ${esc(err.message || "Unknown error")}</div>`;
      btn.disabled = false; btn.textContent = "⚡ Prepare all";
    }
  });

  // Monday push
  // ── Monday snapshot helpers ───────────────────────────────
  function getMondayFormValues() {
    const fechaDisplay = document.getElementById("form-fecha").value.trim();
    return {
      email:     document.getElementById("form-email").value.trim(),
      geo:       document.getElementById("form-geo").value,
      idioma:    document.getElementById("form-idioma").value,
      estado:    document.getElementById("form-estado").value,
      ejecutivo: document.getElementById("form-ejecutivo").value,
      fecha:     toIsoDate(fechaDisplay),
      pitch:     document.getElementById("pitch-text").value.trim(),
    };
  }

  function checkMondayChanged() {
    const btn = document.getElementById("btn-push-monday");
    if (!state.mondaySnapshot) return;
    const cur  = getMondayFormValues();
    const snap = state.mondaySnapshot;
    const changed = Object.keys(snap).some(k => cur[k] !== snap[k]);
    if (changed) {
      btn.disabled    = false;
      btn.textContent = state.duplicate?.found ? "🔄 Update in Monday" : "🚀 Send to Monday";
      btn.classList.remove("btn-sent");
    } else {
      btn.disabled    = true;
      btn.textContent = "✅ Already in Monday";
      btn.classList.add("btn-sent");
    }
  }

  // Detectar cambios en los campos del formulario
  ["form-email","form-geo","form-idioma","form-estado","form-fecha","pitch-text"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener(el.tagName === "TEXTAREA" ? "input" : "change", checkMondayChanged);
  });
  document.getElementById("form-email")?.addEventListener("input", checkMondayChanged);

  // Traffic editable: parsear input del usuario → state.traffic
  document.getElementById("form-pv-display")?.addEventListener("input", (e) => {
    state.traffic = parseTrafficText(e.target.value);
    checkMondayChanged();
  });

  document.getElementById("btn-push-monday").addEventListener("click", async () => {
    const btn    = document.getElementById("btn-push-monday");
    const res    = document.getElementById("push-result");
    const { email, geo, idioma, estado, fecha, pitch, ejecutivo } = getMondayFormValues();

    // Cap diario de Monday pushes por usuario (admin lo configura)
    const can = await checkUserCanDo(state.accessToken, state.loginEmail, "push_monday");
    if (!can.allowed) {
      res.textContent = `⛔ ${can.reason}`; res.className = "push-result error"; return;
    }
    // Rate limit check
    if (!_rateLimiter.check()) {
      res.textContent = "⚠️ Too many requests — please wait a moment"; res.className = "push-result error"; return;
    }
    // Validate email if provided
    if (email && !isValidEmail(email)) {
      res.textContent = "❌ Invalid email format"; res.className = "push-result error"; return;
    }
    // Guard #1: GEO obligatorio — Monday no debe recibir items sin país
    if (!geo) {
      res.textContent = "❌ GEO obligatorio. Completá el campo GEO antes de enviar a Monday.";
      res.className = "push-result error";
      document.getElementById("form-geo")?.scrollIntoView({ behavior: "smooth", block: "center" });
      return;
    }
    // Guard #2: Páginas Vistas obligatorio — bloqueamos si traffic === 0
    const traffic = state.traffic || state.visits || 0;
    if (!traffic || traffic === 0) {
      res.textContent = "❌ Páginas Vistas obligatorio. SimilarWeb no devolvió datos — completá manualmente o re-analizá.";
      res.className = "push-result error";
      return;
    }
    // Guard #3: no dejar pushear NI updatear Monday si todavía no se mandó email
    // en esta sesión. Aplica también a duplicados (botón "🔄 Update in Monday")
    // para evitar que se actualice un item sin haber re-mandado el pitch.
    if (!state.emailSentInSession) {
      const action = state.duplicate?.found ? "update" : "push";
      const msg = `❌ Mandá el email primero (botón Send via Gmail) antes de hacer ${action} en Monday.`;
      res.textContent = msg; res.className = "push-result error";
      document.getElementById("btn-send-gmail")?.scrollIntoView({ behavior: "smooth", block: "center" });
      return;
    }

    btn.disabled = true; btn.textContent = "⏳ Sending..."; res.textContent = "";

    // Snapshot antes de que el push mute state.duplicate — necesario para el contador
    const wasNewPush = !state.duplicate?.found;

    try {
      if (state.duplicate?.found && state.mondayItemId) {
        await updateMonday({
          itemId: state.mondayItemId,
          traffic: formatTraffic(state.traffic),
          email, geo, idioma, pitch, estado, fecha, ejecutivo,
          loginEmail: state.loginEmail,
        });
        res.textContent = "✅ Updated in Monday"; res.className = "push-result ok";
        incrementUserDailyCounter(state.accessToken, state.loginEmail, "monday").catch(() => {});
      } else {
        const item = await pushToMonday({
          domain: state.domain,
          traffic: formatTraffic(state.traffic),
          email, geo, idioma, pitch, estado, fecha, ejecutivo,
          loginEmail: state.loginEmail,
        });
        state.mondayItemId = item?.id;
        res.textContent = `✅ Created: ${item?.name || state.domain}`; res.className = "push-result ok";
        incrementUserDailyCounter(state.accessToken, state.loginEmail, "monday").catch(() => {});

        // Actualizar caché de sesión: ahora sí es duplicado para futuras subpáginas
        state.duplicate = { found: true, itemId: item?.id, status: "", ejecutivo: state.mediaBuyer, email, geo };
        setSessionCache(state.domain, {
          duplicate:     state.duplicate,
          trafficData:   state.trafficData,
          traffic:       state.traffic,
          visits:        state.visits,
          pagesPerVisit: state.pagesPerVisit,
          category:      state.category,
          emails:        state.emails,
        });
      }
      // Guardar snapshot — bloquea el botón hasta que algo cambie
      state.mondaySnapshot = getMondayFormValues();
      btn.textContent      = "✅ Ya en Monday";
      btn.classList.add("btn-sent");

      // Incrementar contador de Monday pushes con contexto (nuevo/dup, +400k o no)
      const nowMonth   = new Date().toISOString().substring(0, 7);
      const statsKey   = userKey("historyStats");
      const defaults   = { total: 0, month: nowMonth, monthNew: 0, monthDups: 0, monthMonday: 0,
                           monthNewQual: 0, monthDupQual: 0,
                           monthMondayNewQual: 0, monthMondayDupQual: 0, monthMondayBelow: 0 };
      const { [statsKey]: hs = defaults } = await chrome.storage.local.get(statsKey);
      if (hs.month !== nowMonth) {
        Object.assign(hs, { month: nowMonth, monthNew: 0, monthDups: 0, monthMonday: 0,
          monthNewQual: 0, monthDupQual: 0, monthMondayNewQual: 0, monthMondayDupQual: 0, monthMondayBelow: 0 });
      }
      const isQual = state.traffic >= CONFIG.MIN_TRAFFIC;
      hs.monthMonday = (hs.monthMonday || 0) + 1;
      if (!isQual)        hs.monthMondayBelow    = (hs.monthMondayBelow    || 0) + 1;
      else if (wasNewPush) hs.monthMondayNewQual  = (hs.monthMondayNewQual  || 0) + 1;
      else                hs.monthMondayDupQual  = (hs.monthMondayDupQual  || 0) + 1;
      await chrome.storage.local.set({ [statsKey]: hs });
      loadHistoryTab();
    } catch (err) {
      res.textContent = `❌ ${err.message}`; res.className = "push-result error";
      btn.disabled    = false;
      btn.textContent = state.duplicate?.found ? "🔄 Update in Monday" : "🚀 Send to Monday";
    }
  });

  // Gmail
  document.getElementById("btn-send-gmail").addEventListener("click", async () => {
    const btn     = document.getElementById("btn-send-gmail");
    const res     = document.getElementById("gmail-result");
    const email   = document.getElementById("form-email").value.trim() ||
                    document.getElementById("email-result").textContent.trim();
    const pitch   = document.getElementById("pitch-text").value || state.pitch;
    const subjectRaw = document.getElementById("form-subject").value.trim();

    if (!isValidEmail(email)) {
      res.textContent = "❌ Enter a valid email first"; res.className = "push-result error"; return;
    }
    // Cap diario de emails enviados por usuario (admin lo configura)
    const can = await checkUserCanDo(state.accessToken, state.loginEmail, "send_email");
    if (!can.allowed) {
      res.textContent = `⛔ ${can.reason}`; res.className = "push-result error"; return;
    }
    // Subject obligatorio — no dejamos enviar si está vacío
    if (!subjectRaw) {
      res.textContent = "❌ El asunto (Subject) es obligatorio. Completalo antes de enviar el email.";
      res.className   = "push-result error";
      document.getElementById("form-subject")?.focus();
      return;
    }
    const subject = subjectRaw;
    if (!pitch) {
      res.textContent = "❌ Generate the pitch first"; res.className = "push-result error"; return;
    }
    if (!_rateLimiter.check()) {
      res.textContent = "⚠️ Too many requests — please wait a moment"; res.className = "push-result error"; return;
    }

    btn.disabled = true; btn.textContent = "⏳ Authenticating...";

    // ── Fetch Gmail signature (will trigger OAuth window if no cached token) ──
    btn.textContent = "⏳ Preparing...";
    const gmailSig = await getGmailSignature();
    const lang     = state.siteLanguage || state.monday?.idioma || "es";

    // Stripping de cualquier cierre viejo en CUALQUIER idioma (Gemini puede meter "Best regards,"
    // aunque el pitch sea en español). Luego agregamos el cierre localizado correcto.
    let bodyToSend = pitch.replace(
      /\n+\s*(best\s*regards|kind\s*regards|regards|sincerely|cheers|thanks\b|thank\s*you|saludos(?:\s*cordiales)?|un\s*saludo|cordialmente|atentamente|cumprimentos|abraços|abracos|cordiali\s*saluti|cordialement|mit\s*freundlichen\s*grüßen)[.,!]*\s*\n[\s\S]{0,200}$/i,
      ""
    ).trimEnd();
    bodyToSend = appendClosingIfMissing(bodyToSend, lang);
    bodyToSend = gmailSig ? bodyToSend + "\n\n" + gmailSig : bodyToSend;

    btn.textContent = "⏳ Sending...";
    const result = await sendEmail({ to: email, subject, body: bodyToSend, expectedFrom: state.loginEmail });

    if (result.ok) {
      state.emailSentInSession = true; // unlock el push a Monday
      incrementUserDailyCounter(state.accessToken, state.loginEmail, "emails").catch(() => {});
      const today = new Date().toISOString().split("T")[0];
      // Persistir el envío para tracking/historial (sin generar follow-ups,
      // los hace el CRM externo).
      await saveSendDate(state.domain, { sendDate: today, pitch, email }).catch(() => {});

      // Update "From" label with the actual Gmail account used
      const fromEl = document.getElementById("gmail-from");
      if (fromEl) fromEl.textContent = `From: ${state.loginEmail}`;
      const assocEl = document.getElementById("settings-gmail-assoc");
      if (assocEl) assocEl.textContent = state.loginEmail;

      res.textContent = `✅ Email sent`;
      res.className   = "push-result ok";
      btn.textContent = "✅ Sent";
    } else {
      res.textContent = `❌ ${result.error}`; res.className = "push-result error";
      btn.disabled    = false; btn.textContent = "📧 Send via Gmail";
    }
  });

  // Cascada
  document.getElementById("btn-cascade-start").addEventListener("click", startCascade);
  document.getElementById("cascade-seed").addEventListener("keydown", e => {
    if (e.key === "Enter") startCascade();
  });
  document.getElementById("btn-push-all").addEventListener("click", openCascadeSelected);
  document.getElementById("btn-cascade-apply-filters")?.addEventListener("click", applyCascadeFilters);
  document.getElementById("btn-check-all").addEventListener("click", () => {
    const resultsEl = document.getElementById("cascade-results");
    resultsEl.querySelectorAll("input[type=checkbox]").forEach(cb => {
      cb.checked = true;
      cascadeSelected.add(cb.closest(".cascade-item")?.dataset.domain);
    });
    updateCascadeSummary();
  });

  // Historial — limpiar solo la lista del usuario actual, los stats persisten
  document.getElementById("btn-clear-history").addEventListener("click", async () => {
    await chrome.storage.local.set({ [userKey("history")]: [] });
    loadHistoryTab();
  });

  // Refresh — re-analiza la pestaña activa
  document.getElementById("btn-refresh").addEventListener("click", () => window.location.reload());

  // ── Cascade · Import Monday collapsable ──────────────────────
  // El card "Import URLs from Monday" se mudó a Cascade. Lo dejo colapsado
  // por default para no robarle espacio a Keywords/Filtros que son lo más usado.
  (() => {
    const toggle = document.getElementById("cascade-monday-toggle");
    const panel  = document.getElementById("cascade-monday-panel");
    const arrow  = document.getElementById("cascade-monday-arrow");
    if (!toggle || !panel) return;
    panel.style.display = "none";
    arrow && (arrow.textContent = "▶");
    toggle.addEventListener("click", () => {
      const open = panel.style.display !== "none";
      panel.style.display = open ? "none" : "block";
      if (arrow) arrow.textContent = open ? "▶" : "▼";
    });
  })();

  // ── Import Tab ────────────────────────────────────────────────
  document.getElementById("btn-import-go").addEventListener("click", async () => {
    const btn      = document.getElementById("btn-import-go");
    const resultEl = document.getElementById("import-result");
    const listEl   = document.getElementById("import-list");

    const geo          = document.getElementById("import-geo").value;
    const idioma       = document.getElementById("import-idioma").value;
    const trafficVal   = document.getElementById("import-traffic").value;
    const [tMinRaw, tMaxRaw] = trafficVal.split(":").map(v => v === "" ? 0 : Number(v));
    const minTraffic   = tMinRaw || 0;
    const maxTraffic   = tMaxRaw || 0;

    btn.disabled = true; btn.textContent = "⏳ Querying Monday...";
    resultEl.textContent = ""; resultEl.className = "push-result";
    listEl.innerHTML = "";

    try {
      const candidates = await fetchImportCandidates({ geo, idioma, minTraffic, maxTraffic });

      if (candidates.length === 0) {
        resultEl.textContent = "No URLs found with those filters in Monday.";
        resultEl.className = "push-result error";
        btn.disabled = false; btn.textContent = "🚀 Importar 15 URLs";
        return;
      }

      // Mezclar aleatoriamente y tomar 15 (para que no siempre salgan los mismos)
      const shuffled = [...candidates].sort(() => Math.random() - 0.5);
      const selected = shuffled.slice(0, 15);

      listEl.innerHTML = selected.map((item, i) => `
        <div class="import-item">
          <span class="import-num">${i + 1}</span>
          <span class="import-domain">${esc(item.domain)}</span>
          <span class="import-meta">${item.traffic ? esc(formatTraffic(item.traffic)) + " vis" : ""}</span>
        </div>`).join("");

      resultEl.textContent = `${selected.length} URLs — opening tabs...`;
      resultEl.className = "push-result ok";

      // Abrir tabs con delay para que Chrome no las bloquee
      selected.forEach((item, i) => {
        setTimeout(() => chrome.tabs.create({ url: item.url, active: false }), i * 400);
      });

      resultEl.textContent = `✅ ${selected.length} tabs opened (${candidates.length - selected.length} more with these filters)`;

    } catch (err) {
      resultEl.textContent = `❌ Error: ${err.message}`;
      resultEl.className = "push-result error";
    }

    btn.disabled = false; btn.textContent = "🚀 Importar 15 URLs";
  });

  // ── Import → Auto-prospector Queue ─────────────────────────────
  // En vez de abrir tabs, encola los 15 dominios para que Railway los procese en background
  document.getElementById("btn-import-queue").addEventListener("click", async () => {
    const btn      = document.getElementById("btn-import-queue");
    const resultEl = document.getElementById("import-result");
    const listEl   = document.getElementById("import-list");

    const geo          = document.getElementById("import-geo").value;
    const idioma       = document.getElementById("import-idioma").value;
    const trafficVal   = document.getElementById("import-traffic").value;
    const [tMinRaw, tMaxRaw] = trafficVal.split(":").map(v => v === "" ? 0 : Number(v));
    const minTraffic   = tMinRaw || 0;
    const maxTraffic   = tMaxRaw || 0;

    btn.disabled = true; btn.textContent = "⏳ Querying Monday...";
    resultEl.textContent = ""; resultEl.className = "push-result";
    listEl.innerHTML = "";

    try {
      const candidates = await fetchImportCandidates({ geo, idioma, minTraffic, maxTraffic });

      if (candidates.length === 0) {
        resultEl.textContent = "No URLs found with those filters in Monday.";
        resultEl.className = "push-result error";
        btn.disabled = false; btn.textContent = "📤 Send to Queue";
        return;
      }

      const shuffled = [...candidates].sort(() => Math.random() - 0.5);
      const selected = shuffled.slice(0, 15);

      listEl.innerHTML = selected.map((item, i) => `
        <div class="import-item">
          <span class="import-num">${i + 1}</span>
          <span class="import-domain">${esc(item.domain)}</span>
          <span class="import-meta">${item.traffic ? esc(formatTraffic(item.traffic)) + " vis" : ""}</span>
        </div>`).join("");

      resultEl.textContent = `Uploading ${selected.length} domains to queue...`;

      const upload = await uploadCsvDomains(selected.map(s => s.domain), state.loginEmail, state.accessToken);

      resultEl.textContent = `✅ ${upload.inserted} added to queue (${selected.length - upload.inserted} already queued). Railway will process if toggle is ON.`;
      resultEl.className = "push-result ok";

    } catch (err) {
      resultEl.textContent = `❌ Error: ${err.message}`;
      resultEl.className = "push-result error";
    }

    btn.disabled = false; btn.textContent = "📤 Send to Queue";
  });

  // initCsvQueue() → lazy-loaded when user clicks the Import tab (initTabs)
  // initPitchDrafts() → loaded eagerly in DOMContentLoaded

  // Settings
  document.getElementById("btn-settings").addEventListener("click", openSettings);
  document.getElementById("btn-close-settings").addEventListener("click", closeSettings);

  // Gmail sign-in (trigger interactive OAuth) — rechaza si Chrome Gmail no coincide
  document.getElementById("btn-gmail-connect")?.addEventListener("click", async () => {
    const btn   = document.getElementById("btn-gmail-connect");
    const warn  = document.getElementById("gmail-mismatch-warn");
    const expected = (state.loginEmail || "").toLowerCase().trim();

    btn.disabled = true; btn.textContent = "⏳ Authorizing...";
    if (warn) { warn.style.display = "none"; warn.textContent = ""; warn.className = "gmail-mismatch"; }

    try {
      // Limpiar token cacheado para forzar re-auth con scopes actualizados
      await clearAllCachedTokens();
      const token = await getGmailToken(true); // interactive — fuerza el prompt

      if (!token) {
        showGmailFeedback("error", "Could not get authorization. Please accept permissions on the Google prompt.");
        return;
      }

      const profile = await getGmailProfile(false);
      if (!profile?.email) {
        showGmailFeedback("error", "Could not read Google account email. Try again or check permissions.");
        try { await fetch(`https://oauth2.googleapis.com/revoke?token=${encodeURIComponent(token)}`, { method: "POST" }); } catch {}
        await new Promise(r => chrome.identity.removeCachedAuthToken({ token }, r));
        return;
      }

      if (profile.email !== expected) {
        // MISMATCH
        try { await fetch(`https://oauth2.googleapis.com/revoke?token=${encodeURIComponent(token)}`, { method: "POST" }); } catch {}
        await new Promise(r => chrome.identity.removeCachedAuthToken({ token }, r));
        showGmailFeedback("warn", `You picked ${profile.email} but must sign in with ${expected}. Try again and select the correct account.`);
      } else {
        showGmailFeedback("ok", `✓ Gmail connected: ${profile.email}`);
      }
    } finally {
      btn.disabled = false; btn.textContent = "Sign in to Gmail";
      await refreshGmailStatus();
    }
  });

  // Gmail sign-out (revoca el token y limpia caché)
  document.getElementById("btn-gmail-signout")?.addEventListener("click", async () => {
    const btn = document.getElementById("btn-gmail-signout");
    btn.disabled = true; btn.textContent = "⏳ Closing...";
    try {
      const token = await getGmailToken(false);
      if (token) {
        await fetch(`https://oauth2.googleapis.com/revoke?token=${encodeURIComponent(token)}`, { method: "POST" });
        await new Promise(resolve => chrome.identity.removeCachedAuthToken({ token }, resolve));
      }
      await clearAllCachedTokens();
      showGmailFeedback("ok", "✓ Gmail signed out. Click Sign in to Gmail to reconnect.");
    } catch (e) {
      showGmailFeedback("error", `Sign-out error: ${e.message}`);
    }
    btn.disabled = false; btn.textContent = "Sign out Gmail";
    await refreshGmailStatus();
  });

  document.getElementById("modal-overlay").addEventListener("click", closeSettings);
  document.getElementById("btn-run-diag").addEventListener("click", runDiagnostic);
  document.getElementById("btn-logout").addEventListener("click", logout);

  // Toggle panel de import de keywords (Cascade → Google Keywords)
  document.getElementById("btn-kw-import-toggle")?.addEventListener("click", () => {
    const panel = document.getElementById("kw-import-panel");
    if (!panel) return;
    panel.style.display = panel.style.display === "none" ? "block" : "none";
    // Al mostrar, refrescar el count
    if (panel.style.display !== "none") {
      const countEl = document.getElementById("kw-db-count-inline");
      if (countEl) countEl.textContent = `${dbKeywords.length} phrases imported`;
    }
  });

  // Keywords DB — importar CSV (ahora desde Cascade → Google Keywords)
  document.getElementById("kw-csv-input")?.addEventListener("change", async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const resultEl = document.getElementById("kw-import-result");
    resultEl.textContent = "Processing...";
    const reader = new FileReader();
    reader.onload = async (ev) => {
      const text = ev.target.result;
      const phrases = text
        .split(/[\n\r]+/)
        .map(s => s.split(/[,;]/).map(p => p.trim().replace(/^["']|["']$/g, "")).filter(p => p.length > 1 && p.length < 200))
        .flat()
        .filter((v, i, a) => a.indexOf(v) === i); // dedup

      const rows = phrases.map(phrase => ({ phrase, lang: detectPhraseLang(phrase) }));
      const { count } = await importKeywordsToDB(rows);
      dbKeywords = [...dbKeywords, ...rows.map(r => ({ kw: r.phrase, lang: r.lang, db: true }))];
      resultEl.textContent = `✅ ${count} phrases imported (total: ${dbKeywords.length})`;
      const countEl = document.getElementById("kw-db-count-inline");
      if (countEl) countEl.textContent = `${dbKeywords.length} phrases imported`;
      e.target.value = "";
      filterKeywords();
    };
    reader.readAsText(file);
  });

  document.getElementById("btn-kw-delete-all-inline")?.addEventListener("click", async () => {
    if (!confirm("Delete ALL phrases from the keyword database?")) return;
    const resultEl = document.getElementById("kw-import-result");
    resultEl.textContent = "Clearing...";
    await clearKeywordsDB(state.accessToken);
    dbKeywords = [];
    const countEl = document.getElementById("kw-db-count-inline");
    if (countEl) countEl.textContent = "0 phrases";
    resultEl.textContent = "✅ Keyword database cleared";
    filterKeywords();
  });
}

// ============================================================
// SETTINGS
// ============================================================
function detectPhraseLang(phrase) {
  if (/[\u0600-\u06FF]/.test(phrase)) return "ar";
  if (/[àáâãäåèéêëìíîïòóôõöùúûüý]/i.test(phrase)) {
    if (/[ñ¿¡]/i.test(phrase)) return "es";
    if (/[ção]/i.test(phrase)) return "pt";
    if (/[àâèêîôùûœæ]/i.test(phrase)) return "fr";
    if (/[àèìòù]/i.test(phrase)) return "it";
    return "es";
  }
  return "en";
}

async function openSettings() {
  document.getElementById("settings-modal").style.display = "flex";

  // Prompt GLOBAL — solo el admin lo ve y lo puede editar.
  // Para los MBs, ocultamos toda la sección.
  const promptSection = document.getElementById("settings-custom-prompt")?.closest(".settings-gmail-row");
  const isAdmin = state.role === "admin";
  if (promptSection) promptSection.style.display = isAdmin ? "" : "none";
  if (isAdmin) {
    const promptEl = document.getElementById("settings-custom-prompt");
    if (promptEl) promptEl.value = state.customPrompt || "";
    const statusEl = document.getElementById("custom-prompt-status");
    if (statusEl) statusEl.textContent = state.customPrompt
      ? `${state.customPrompt.length} chars saved (GLOBAL — afecta a todo el equipo)`
      : "empty";
  }

  await refreshGmailStatus();
}

function bindCustomPromptHandlers() {
  const saveBtn  = document.getElementById("btn-save-custom-prompt");
  const clearBtn = document.getElementById("btn-clear-custom-prompt");
  const taEl     = document.getElementById("settings-custom-prompt");
  const statusEl = document.getElementById("custom-prompt-status");
  if (!saveBtn || !taEl) return;

  saveBtn.addEventListener("click", async () => {
    if (state.role !== "admin") {
      statusEl.textContent = "❌ Solo admin puede modificar el prompt global.";
      statusEl.style.color = "var(--danger)";
      return;
    }
    const value = taEl.value.trim();
    saveBtn.disabled = true; saveBtn.textContent = "⏳ Saving...";
    // Guarda como prompt GLOBAL (key = __global__) que todos los MBs van a leer.
    const r = await setCustomPrompt(state.accessToken, GLOBAL_PROMPT_KEY, value);
    saveBtn.disabled = false; saveBtn.textContent = "💾 Save prompt";
    if (!r.ok) { statusEl.textContent = `❌ Save failed (${r.status || r.error})`; statusEl.style.color = "var(--danger)"; return; }
    state.customPrompt = value;
    statusEl.textContent = `✅ Saved · ${value.length} chars (GLOBAL)`;
    statusEl.style.color = "var(--success-text)";
    setTimeout(() => { statusEl.style.color = "var(--text-muted)"; }, 3000);
    // Audit log
    logAuditEvent(state.accessToken, {
      user_email: state.loginEmail, action: "edit_global_prompt",
      details: { length: value.length },
    });
  });

  clearBtn?.addEventListener("click", async () => {
    if (state.role !== "admin") return;
    if (!confirm("¿Resetear al prompt baked-in (Diego's voice default)?")) return;
    const r = await setCustomPrompt(state.accessToken, GLOBAL_PROMPT_KEY, "");
    if (r.ok) {
      state.customPrompt = DIEGO_VOICE_PROMPT;
      taEl.value = DIEGO_VOICE_PROMPT;
      statusEl.textContent = "Reset al default";
    }
  });
}

// Feedback visible en el panel de Gmail (ok / warn / error)
function showGmailFeedback(kind, text) {
  const warn = document.getElementById("gmail-mismatch-warn");
  if (!warn) return;
  warn.className     = `gmail-mismatch gmail-feedback-${kind}`;
  warn.textContent   = text;
  warn.style.display = "block";
  // Auto-ocultar mensajes de OK después de 6s para no ser molesto
  if (kind === "ok") {
    clearTimeout(warn._hideTimer);
    warn._hideTimer = setTimeout(() => { warn.style.display = "none"; }, 6000);
  }
}

// Lee el estado real de la cuenta Gmail de Chrome y lo muestra
async function refreshGmailStatus() {
  const emailEl    = document.getElementById("settings-gmail-assoc");
  const badgeEl    = document.getElementById("gmail-status-badge");
  const warnEl     = document.getElementById("gmail-mismatch-warn");
  const connectBtn = document.getElementById("btn-gmail-connect");
  const signoutBtn = document.getElementById("btn-gmail-signout");
  if (!emailEl || !badgeEl) return;

  const profile  = await getGmailProfile(false); // no interactive
  const expected = (state.loginEmail || "").toLowerCase();

  warnEl.style.display = "none";
  warnEl.textContent   = "";

  if (!profile?.email) {
    emailEl.textContent   = "Not connected";
    badgeEl.textContent   = "OFF";
    badgeEl.className     = "gmail-status-badge off";
    connectBtn.style.display = "inline-block";
    signoutBtn.style.display = "none";
    return;
  }

  emailEl.textContent      = profile.email;
  connectBtn.style.display = "none";
  signoutBtn.style.display = "inline-block";

  if (expected && profile.email !== expected) {
    badgeEl.textContent = "MISMATCH";
    badgeEl.className   = "gmail-status-badge mismatch";
    warnEl.textContent  = `⚠ Chrome is signed in as ${profile.email}, but you logged into the toolbar as ${expected}. Sign out and reconnect with the correct account.`;
    warnEl.style.display = "block";
  } else {
    badgeEl.textContent = "ON";
    badgeEl.className   = "gmail-status-badge on";
  }
}

function closeSettings() {
  document.getElementById("settings-modal").style.display = "none";
}

// ============================================================
// KEYWORDS
// ============================================================
let dbKeywords       = []; // frases para rotación en pantalla (sin búsqueda)
let kwRotationTimer  = null;
const KW_ROTATION_MS = 30 * 1000;
let kwNextRotation   = Date.now() + KW_ROTATION_MS;
let kwSearchTimer    = null; // debounce para búsqueda

async function initKeywords() {
  // Restaurar filtros de Cascade guardados de sesiones previas (UX audit fix)
  try {
    const { _cascadeFilters } = await chrome.storage.local.get("_cascadeFilters");
    if (_cascadeFilters) {
      const t = document.getElementById("cascade-min-traffic");
      const r = document.getElementById("cascade-max-rank");
      const l = document.getElementById("cascade-language");
      if (t && _cascadeFilters.traffic) t.value = _cascadeFilters.traffic;
      if (r && _cascadeFilters.rank)    r.value = _cascadeFilters.rank;
      if (l && _cascadeFilters.lang)    l.value = _cascadeFilters.lang;
    }
  } catch {}
  // Cargar muestra inicial para rotación en pantalla
  const rows = await loadKeywordsFromDB();
  dbKeywords = rows
    .filter(r => typeof r.phrase === "string" && r.phrase.length > 0)
    .map(r => ({ kw: r.phrase, lang: r.lang || "en", db: true }));

  filterKeywords();
  document.getElementById("kw-language").addEventListener("change", filterKeywords);
  document.getElementById("kw-search").addEventListener("input", () => {
    // Debounce: espera 350ms después de que el usuario deje de tipear
    clearTimeout(kwSearchTimer);
    const search = document.getElementById("kw-search").value.trim();
    if (!search) {
      filterKeywords(); // sin búsqueda → rotación normal
      return;
    }
    document.getElementById("keywords-list").innerHTML = '<span class="kw-empty">Searching...</span>';
    kwSearchTimer = setTimeout(runKeywordSearch, 350);
  });
  startKwRotation();
}

function startKwRotation() {
  // Cleanup de timers previos — evita leak si initKeywords corre múltiples veces
  if (window._kwTimers) {
    clearInterval(window._kwTimers.rotTimer);
    clearInterval(window._kwTimers.tickTimer);
  }
  kwNextRotation = Date.now() + KW_ROTATION_MS;
  updateRotationTimer();
  const rotTimer  = setInterval(() => {
    if (document.visibilityState === "hidden") return;
    kwNextRotation = Date.now() + KW_ROTATION_MS;
    filterKeywords();
  }, KW_ROTATION_MS);
  const tickTimer = setInterval(() => {
    if (document.visibilityState === "hidden") return;
    updateRotationTimer();
  }, 10 * 1000);
  window._kwTimers = { rotTimer, tickTimer };
}

function updateRotationTimer() {
  const el = document.getElementById("kw-rotation-timer");
  if (!el) return;
  const secsLeft = Math.max(0, Math.round((kwNextRotation - Date.now()) / 1000));
  const mins = Math.floor(secsLeft / 60);
  const secs = secsLeft % 60;
  el.textContent = `↻ rota en ${secsLeft}s`;
}

// Búsqueda directa en Supabase (se llama tras debounce)
async function runKeywordSearch() {
  const lang   = document.getElementById("kw-language").value;
  const search = document.getElementById("kw-search").value.trim();
  if (!search) { filterKeywords(); return; }

  const { rows, error } = await searchKeywordsInDB(search, lang);

  if (error) {
    // Error de API — fallback a búsqueda local + mostrar aviso
    const term    = search.toLowerCase();
    const local   = dbKeywords.filter(k =>
      typeof k.kw === "string" && k.kw.toLowerCase().includes(term) &&
      (!lang || !k.lang || k.lang === lang)
    );
    const el = document.getElementById("keywords-list");
    if (local.length > 0) {
      renderKeywords(local, term);
      el.insertAdjacentHTML("afterbegin", `<div class="kw-api-warn">⚠ Supabase: ${esc(error)} — showing local results</div>`);
    } else {
      el.innerHTML = `<span class="kw-empty">Error al buscar: ${esc(error)}</span>`;
    }
    return;
  }

  const results = rows
    .filter(r => typeof r.phrase === "string" && r.phrase.length > 0)
    .map(r => ({ kw: r.phrase, lang: r.lang || "en", db: true }));

  if (results.length === 0) {
    document.getElementById("keywords-list").innerHTML =
      '<span class="kw-empty">No results — the keyword isn\'t in the database</span>';
    return;
  }

  renderKeywords(results, search.toLowerCase());
}

function filterKeywords() {
  const lang = document.getElementById("kw-language").value;

  // Pool: DB primero, built-in como fallback — siempre filtrar nulos
  let pool = (dbKeywords.length > 0
    ? dbKeywords
    : getKeywords(lang).map(k => ({ ...k, db: false }))
  ).filter(k => typeof k.kw === "string" && k.kw.length > 0);

  if (lang) pool = pool.filter(k => !k.lang || k.lang === lang);

  // Sin búsqueda: mezcla aleatoria para rotación
  const shuffled = pool.slice().sort(() => Math.random() - 0.5);
  renderKeywords(shuffled, "");
}

function renderKeywords(kws, search = "") {
  const el    = document.getElementById("keywords-list");
  const limit = search ? Infinity : 100;
  if (kws.length === 0) {
    el.innerHTML = '<span class="kw-empty">No results</span>'; return;
  }
  const visible = kws.slice(0, limit);
  el.innerHTML = visible.map(k =>
    `<button class="kw-chip${k.db ? " kw-imported" : ""}" data-kw="${esc(k.kw)}">${esc(k.kw)}</button>`
  ).join("");
  if (kws.length > limit) {
    el.innerHTML += `<span class="kw-empty" style="margin-top:4px">… y ${kws.length - limit} más. Refiná la búsqueda.</span>`;
  }
  el.querySelectorAll(".kw-chip").forEach(btn => {
    btn.addEventListener("click", () => searchGoogleForDomain(btn.dataset.kw));
  });
}

// ── Claves de storage por usuario ─────────────────────────────
function userKey(base) {
  const name = (state.mediaBuyer || "shared").toLowerCase();
  return `${base}_${name}`;
}

// ============================================================
// HISTORIAL
// ============================================================
async function loadHistoryTab() {
  const nowMonth = new Date().toISOString().substring(0, 7);
  const statsKey = userKey("historyStats");
  const defaults = {
    total: 0, month: nowMonth, monthNew: 0, monthDups: 0, monthMonday: 0,
    monthNewQual: 0, monthDupQual: 0,
    monthMondayNewQual: 0, monthMondayDupQual: 0, monthMondayBelow: 0,
  };
  const { [statsKey]: hs = defaults } = await chrome.storage.local.get(statsKey);

  const same = hs.month === nowMonth;
  const n = (k) => same ? (hs[k] || 0) : 0;

  const monthNew          = n("monthNew");
  const monthDups         = n("monthDups");
  const monthNewQual      = n("monthNewQual");
  const monthDupQual      = n("monthDupQual");
  const monthMondayNQ     = n("monthMondayNewQual");
  const monthMondayDQ     = n("monthMondayDupQual");
  const monthMondayBelow  = n("monthMondayBelow");

  const monthNewBelow  = Math.max(0, monthNew  - monthNewQual);
  const monthDupBelow  = Math.max(0, monthDups - monthDupQual);
  const monthBelow     = monthNewBelow + monthDupBelow;
  const monthTotal     = monthNew + monthDups;

  const pct = (a, b) => b ? `<span class="sc-pct">${Math.round(a/b*100)}%</span>` : `<span class="sc-pct sc-pct--empty">—</span>`;
  const mesLabel = new Date().toLocaleString("es-AR", { month: "long", year: "numeric" });

  const listEl  = document.getElementById("history-list");
  const statsEl = document.getElementById("history-stats");

  statsEl.innerHTML = `
    <div class="stat-summary-row">
      <span class="stat-summary-total">All-time total: <strong>${hs.total}</strong></span>
      <span class="stat-summary-month">${monthTotal} analyzed · ${mesLabel}</span>
    </div>

    <div class="stat-category-block">
      <div class="stat-cat-header stat-cat-new">📗 New <span class="stat-cat-count">${monthNew}</span></div>
      <div class="stat-grid">
        <div class="stat-card stat-card--qual">
          <span class="sc-num">${monthNewQual}</span>
          ${pct(monthNewQual, monthNew)}
          <span class="sc-lbl">+400K</span>
        </div>
        <div class="stat-card stat-card--monday">
          <span class="sc-num">${monthMondayNQ}</span>
          ${pct(monthMondayNQ, monthNewQual)}
          <span class="sc-lbl">→ Monday</span>
        </div>
        <div class="stat-card stat-card--neutral">
          <span class="sc-num">${monthNewBelow}</span>
          ${pct(monthNewBelow, monthNew)}
          <span class="sc-lbl">&lt;400K</span>
        </div>
      </div>
    </div>

    <div class="stat-category-block">
      <div class="stat-cat-header stat-cat-dup">🔵 Duplicates <span class="stat-cat-count">${monthDups}</span></div>
      <div class="stat-grid">
        <div class="stat-card stat-card--qual">
          <span class="sc-num">${monthDupQual}</span>
          ${pct(monthDupQual, monthDups)}
          <span class="sc-lbl">+400K</span>
        </div>
        <div class="stat-card stat-card--monday">
          <span class="sc-num">${monthMondayDQ}</span>
          ${pct(monthMondayDQ, monthDupQual)}
          <span class="sc-lbl">→ Monday</span>
        </div>
        <div class="stat-card stat-card--neutral">
          <span class="sc-num">${monthDupBelow}</span>
          ${pct(monthDupBelow, monthDups)}
          <span class="sc-lbl">&lt;400K</span>
        </div>
      </div>
    </div>

    <div class="stat-category-block">
      <div class="stat-cat-header stat-cat-below">🔻 Below 400K <span class="stat-cat-count">${monthBelow}</span></div>
      <div class="stat-grid">
        <div class="stat-card ${monthMondayBelow > 0 ? "stat-card--warn" : "stat-card--neutral"}">
          <span class="sc-num">${monthMondayBelow}</span>
          ${pct(monthMondayBelow, monthBelow)}
          <span class="sc-lbl">→ Monday${monthMondayBelow > 0 ? " ⚠️" : ""}</span>
        </div>
      </div>
    </div>
  `;

  // Lista: solo de chrome.storage (no Supabase), deduplicada por dominio, POR USUARIO
  const histKey = userKey("history");
  const { [histKey]: raw = [] } = await chrome.storage.local.get(histKey);
  const seen = new Set();
  const history = raw.filter(h => {
    if (!h.isNew || seen.has(h.domain)) return false;
    seen.add(h.domain);
    return true;
  });

  if (history.length === 0) {
    listEl.innerHTML = '<div class="cascade-empty">No sites in the list.</div>';
    return;
  }

  const PAGE_SIZE = 20;
  let historyPage = 0;

  function renderHistoryPage() {
    const start   = 0;
    const end     = (historyPage + 1) * PAGE_SIZE;
    const visible = history.slice(start, end);

    listEl.innerHTML = visible.map(h => `
      <div class="history-item" data-url="https://${esc(h.domain)}">
        <img class="history-favicon" loading="lazy" src="https://www.google.com/s2/favicons?domain=${esc(h.domain)}&sz=16" onerror="this.style.display='none'" />
        <span class="history-domain">${esc(h.domain)}</span>
        <span class="history-traffic">${esc(h.traffic || formatTraffic(h.pageViews) || "--")}</span>
        <span class="history-buyer">${esc(h.mediaBuyer || "")}</span>
        ${h.source === "auto" ? '<span class="history-auto-badge">AUTO</span>' : ""}
        <span class="history-date">${esc(h.date || "")}</span>
      </div>
    `).join("");

    if (end < history.length) {
      const moreBtn = document.createElement("button");
      moreBtn.className   = "btn-history-more";
      moreBtn.textContent = `Show more (${history.length - end} remaining)`;
      moreBtn.addEventListener("click", () => { historyPage++; renderHistoryPage(); });
      listEl.appendChild(moreBtn);
    }

    listEl.querySelectorAll(".history-item").forEach(item => {
      item.addEventListener("click", () => chrome.tabs.create({ url: item.dataset.url }));
    });
  }

  renderHistoryPage();
}

// ============================================================
// CASCADA
// ============================================================
const COUNTRY_NAMES = {
  // Latinoamérica
  AR:"Argentina",MX:"México",CO:"Colombia",CL:"Chile",BR:"Brasil",PE:"Perú",
  EC:"Ecuador",VE:"Venezuela",UY:"Uruguay",PY:"Paraguay",BO:"Bolivia",
  CR:"Costa Rica",PA:"Panamá",DO:"Rep. Dominicana",GT:"Guatemala",HN:"Honduras",
  SV:"El Salvador",CU:"Cuba",PR:"Puerto Rico",
  // Norteamérica
  US:"Estados Unidos",CA:"Canadá",
  // Europa Occidental
  GB:"Reino Unido",ES:"España",PT:"Portugal",FR:"Francia",DE:"Alemania",
  IT:"Italia",NL:"Países Bajos",BE:"Bélgica",CH:"Suiza",AT:"Austria",
  SE:"Suecia",NO:"Noruega",DK:"Dinamarca",FI:"Finlandia",IE:"Irlanda",
  // Europa del Este
  PL:"Polonia",RO:"Rumania",HU:"Hungría",CZ:"Rep. Checa",SK:"Eslovaquia",
  BG:"Bulgaria",HR:"Croacia",RS:"Serbia",GR:"Grecia",UA:"Ucrania",RU:"Rusia",
  // Medio Oriente y África
  TR:"Turquía",IL:"Israel",AE:"Emiratos Árabes",SA:"Arabia Saudita",
  EG:"Egipto",MA:"Marruecos",NG:"Nigeria",ZA:"Sudáfrica",
  KE:"Kenia",GH:"Ghana",ET:"Etiopía",TZ:"Tanzania",
  // Asia y Oceanía
  IN:"India",JP:"Japón",KR:"Corea del Sur",CN:"China",TW:"Taiwán",HK:"Hong Kong",
  SG:"Singapur",MY:"Malasia",ID:"Indonesia",PH:"Filipinas",TH:"Tailandia",
  VN:"Vietnam",PK:"Pakistán",BD:"Bangladesh",AU:"Australia",NZ:"Nueva Zelanda",
};

async function startCascade() {
  const seed = document.getElementById("cascade-seed").value.trim()
    .replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "");
  if (!seed) return;

  const btn       = document.getElementById("btn-cascade-start");
  const statusEl  = document.getElementById("cascade-status");
  const resultsEl = document.getElementById("cascade-results");
  const actionsEl = document.getElementById("cascade-actions");

  const parseRange = (val) => {
    const [a, b] = val.split(":");
    return [a === "" || a == null ? 0 : Number(a), b === "" || b == null ? Infinity : Number(b)];
  };
  const [tMin, tMax] = parseRange(document.getElementById("cascade-min-traffic").value);
  const [rMin, rMax] = parseRange(document.getElementById("cascade-max-rank").value);
  const langFilter = document.getElementById("cascade-language").value;

  btn.disabled = true; btn.textContent = "⏳";
  cascadeResults = []; cascadeRawResults = []; cascadeSelected = new Set(); cascadeBlockedExecSet = new Set();
  resultsEl.innerHTML = ""; actionsEl.style.display = "none";

  // Dominios genéricos/irrelevantes que SimilarWeb suele devolver por tráfico
  const CASCADE_BLOCKLIST = new Set([
    "google.com","youtube.com","facebook.com","instagram.com","twitter.com","x.com",
    "tiktok.com","linkedin.com","reddit.com","wikipedia.org","amazon.com","ebay.com",
    "chatgpt.com","openai.com","chat.openai.com","bing.com","yahoo.com","msn.com",
    "microsoft.com","apple.com","netflix.com","twitch.tv","pinterest.com","snapchat.com",
    "whatsapp.com","telegram.org","discord.com","zoom.us","dropbox.com","shopify.com",
    "wordpress.com","blogger.com","tumblr.com","medium.com","substack.com","quora.com",
    "stackoverflow.com","github.com","gitlab.com","canva.com","figma.com",
  ]);

  const CASCADE_LIMIT = 50;

  // Cargar índice de Monday para filtrar dominios de otros ejecutivos (últimos 45 días)
  statusEl.textContent = "Querying Monday...";
  const boardIndex = await getMondayBoardIndex();
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - 45);
  const cutoffStr = cutoffDate.toISOString().split("T")[0];

  let filteredCount = 0;

  const isBlockedByExec = (domain) => {
    const clean = domain.replace(/^www\./, "").toLowerCase();
    const info  = boardIndex.get(clean);
    if (!info) return false;
    return info.ejecutivo &&
           info.ejecutivo !== state.mediaBuyer &&
           info.fecha     >= cutoffStr;
  };

  const passesFilters = (site) => {
    if (CASCADE_BLOCKLIST.has(site.domain.replace(/^www\./, ""))) return false;
    if (tMin > 0 && site.visits < tMin) return false;
    if (tMax !== Infinity && site.visits > tMax) return false;
    if (rMin > 0 && site.globalRank && site.globalRank < rMin) return false;
    if (rMax !== Infinity && site.globalRank && site.globalRank > rMax) return false;
    if (langFilter && site.countryCode !== langFilter) return false;
    if (isBlockedByExec(site.domain)) { filteredCount++; cascadeBlockedExecSet.add(site.domain.replace(/^www\./, "").toLowerCase()); return false; }
    return true;
  };

  const addResult = (site) => {
    if (cascadeResults.length >= CASCADE_LIMIT) return false;
    cascadeResults.push(site);
    appendCascadeItem(site, resultsEl);
    // Mostrar botones ni bien aparece el primer resultado
    if (cascadeResults.length === 1) {
      actionsEl.style.display = "block";
      updateCascadeSummary();
    }
    return true;
  };

  const onProgress = ({ status, domain: d, site, level }) => {
    if (status === "searching") {
      statusEl.textContent = `Level ${level}: searching similar sites of ${d}...`;
    } else if (status === "found") {
      cascadeRawResults.push(site);
      if (passesFilters(site)) addResult(site);
    }
  };

  try {
    await runCascade(seed, onProgress);

    if (cascadeResults.length === 0) {
      resultsEl.innerHTML = '<div class="cascade-empty">No prospects found with those filters.</div>';
    } else {
      const limitMsg = cascadeResults.length >= CASCADE_LIMIT ? ` (limit ${CASCADE_LIMIT})` : "";
      statusEl.textContent = `✅ ${cascadeResults.length} prospects${limitMsg}${filteredCount ? ` · ${filteredCount} filtered out` : ""}`;
      actionsEl.style.display = "block";
      updateCascadeSummary();
    }
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
  }

  btn.disabled = false; btn.textContent = "Search";
}

function appendCascadeItem(site, container) {
  const item = document.createElement("div");
  item.className      = "cascade-item";
  item.dataset.domain = site.domain;

  const rankColor  = !site.globalRank ? "" : site.globalRank < 50000 ? "rank-ok" : site.globalRank < 200000 ? "rank-warn" : "rank-bad";
  const rankText   = site.globalRank ? `#${site.globalRank.toLocaleString()}` : "—";
  const countryStr = site.countryCode ? (COUNTRY_NAMES[site.countryCode] || site.countryCode) : "—";

  // Si visits=0 (Cascade post-refactor: no enriquece para ahorrar hits) → mostrar "?".
  // El usuario verá los datos reales recién al abrir Analysis del dominio elegido.
  const visitsLabel = site.visits > 0 ? formatTraffic(site.visits) : "?";
  const grade = site.visits > 0
    ? (() => { const s = scoreProspect({ pageViews: site.visits, rawVisits: site.visits }); return `<span class="score-grade-sm" style="background:${s.color}" title="${s.label}">${s.grade}</span>`; })()
    : `<span class="score-grade-sm" style="background:#94a3b8" title="Sin enriquecer — abrí el dominio para ver datos">?</span>`;

  item.innerHTML = `
    <input type="checkbox" />
    <img class="cascade-favicon" loading="lazy" src="https://www.google.com/s2/favicons?domain=${esc(site.domain)}&sz=16" onerror="this.style.display='none'" />
    <span class="cascade-domain" title="${esc(site.domain)}">${esc(site.domain)}</span>
    <span class="cascade-visits">${esc(visitsLabel)}</span>
    <span class="cascade-rank ${rankColor}">${rankText}</span>
    <span class="cascade-country">${esc(countryStr)}</span>
    ${grade}
  `;

  const cb = item.querySelector("input");
  // unchecked by default — user picks what to open
  cb.addEventListener("change", () => {
    if (cb.checked) cascadeSelected.add(site.domain);
    else            cascadeSelected.delete(site.domain);
    updateCascadeSummary();
  });
  item.addEventListener("click", e => {
    if (e.target.tagName !== "INPUT") { cb.checked = !cb.checked; cb.dispatchEvent(new Event("change")); }
  });

  container.appendChild(item);
}

function updateCascadeSummary() {
  const el = document.getElementById("cascade-summary");
  if (el) el.textContent = `${cascadeSelected.size} selected of ${cascadeResults.length}`;
}

// Re-aplica los filtros actuales a los raw results (sin re-consultar la API)
function applyCascadeFilters() {
  const resultsEl = document.getElementById("cascade-results");
  const statusEl  = document.getElementById("cascade-status");
  const actionsEl = document.getElementById("cascade-actions");
  if (!cascadeRawResults.length) {
    if (statusEl) statusEl.textContent = "No results to filter — run a search first.";
    return;
  }

  const parseRange = (val) => {
    const [a, b] = val.split(":");
    return [a === "" || a == null ? 0 : Number(a), b === "" || b == null ? Infinity : Number(b)];
  };
  const trafficVal = document.getElementById("cascade-min-traffic").value;
  const rankVal    = document.getElementById("cascade-max-rank").value;
  const langFilter = document.getElementById("cascade-language").value;
  const [tMin, tMax] = parseRange(trafficVal);
  const [rMin, rMax] = parseRange(rankVal);
  // Persistir filtros para próximas sesiones (UX audit fix)
  chrome.storage.local.set({ _cascadeFilters: { traffic: trafficVal, rank: rankVal, lang: langFilter } }).catch(() => {});

  const BLOCKLIST = new Set([
    "google.com","youtube.com","facebook.com","instagram.com","twitter.com","x.com",
    "tiktok.com","linkedin.com","reddit.com","wikipedia.org","amazon.com","ebay.com",
    "chatgpt.com","openai.com","chat.openai.com","bing.com","yahoo.com","msn.com",
    "microsoft.com","apple.com","netflix.com","twitch.tv","pinterest.com","snapchat.com",
    "whatsapp.com","telegram.org","discord.com","zoom.us","dropbox.com","shopify.com",
    "wordpress.com","blogger.com","tumblr.com","medium.com","substack.com","quora.com",
    "stackoverflow.com","github.com","gitlab.com","canva.com","figma.com",
  ]);
  const LIMIT = 50;

  const filtered = cascadeRawResults.filter(site => {
    const clean = site.domain.replace(/^www\./, "").toLowerCase();
    if (BLOCKLIST.has(clean)) return false;
    if (cascadeBlockedExecSet.has(clean)) return false;
    // Filtros traffic/rank/lang SOLO si el site tiene esos datos. Cascade
    // ahora no enriquece (decisión user 2026-05-08), entonces visits=0 y
    // countryCode="" para todos. Los filtros se aplican post-Analysis del
    // dominio elegido.
    if (site.visits > 0) {
      if (tMin > 0 && site.visits < tMin) return false;
      if (tMax !== Infinity && site.visits > tMax) return false;
    }
    if (rMin > 0 && site.globalRank && site.globalRank < rMin) return false;
    if (rMax !== Infinity && site.globalRank && site.globalRank > rMax) return false;
    if (langFilter && site.countryCode && site.countryCode !== langFilter) return false;
    return true;
  }).slice(0, LIMIT);

  // Re-renderizar desde cero
  cascadeResults  = [];
  cascadeSelected = new Set();
  resultsEl.innerHTML = "";
  for (const site of filtered) {
    cascadeResults.push(site);
    appendCascadeItem(site, resultsEl);
  }

  if (cascadeResults.length === 0) {
    resultsEl.innerHTML = '<div class="cascade-empty">No results match the current filters.</div>';
    actionsEl.style.display = "none";
  } else {
    actionsEl.style.display = "block";
    updateCascadeSummary();
  }

  if (statusEl) statusEl.textContent = `✅ ${cascadeResults.length} results matching filters (of ${cascadeRawResults.length} total)`;
}

function openCascadeSelected() {
  const result = document.getElementById("cascade-push-result");
  const toOpen = cascadeResults.filter(s => cascadeSelected.has(s.domain));

  if (toOpen.length === 0) {
    result.textContent = "Select at least one site"; result.className = "push-result error"; return;
  }

  toOpen.forEach(site => chrome.tabs.create({ url: `https://${site.domain}`, active: false }));
  result.textContent = `✅ ${toOpen.length} sites opened in new tabs`;
  result.className   = "push-result ok";
}

// ============================================================
// HELPERS
// ============================================================
function extractDomain(url) {
  try {
    const hostname = new URL(url).hostname.replace(/^www\./, "");
    return sanitizeDomain(hostname);
  } catch { return sanitizeDomain(url); }
}

function countryFlag(code) {
  if (!code || code.length !== 2) return "🌐";
  return [...code.toUpperCase()].map(c => String.fromCodePoint(c.charCodeAt(0) + 127397)).join("");
}

function detectGeo() {
  const tld = state.domain.split(".").pop()?.toLowerCase();
  const map = { es:"ES",mx:"MX",ar:"AR",co:"CO",cl:"CL",br:"BR",pt:"PT",fr:"FR",de:"DE",it:"IT",hu:"HU",vn:"VN",uk:"UK",au:"AU",ca:"CA" };
  return map[tld] || "US";
}

function simplifyCategory(raw) {
  if (!raw) return "";
  // Tomar solo el segmento raíz (antes del primer "/")
  const root = raw.split("/")[0].replace(/_/g, " ").trim();
  const map = {
    "Computers Electronics and Technology": "Tech & Electrónica",
    "News and Media":                       "Noticias & Media",
    "Arts and Entertainment":               "Entretenimiento",
    "Sports":                               "Deportes",
    "Finance":                              "Finanzas",
    "Health":                               "Salud",
    "Travel and Tourism":                   "Viajes",
    "Business and Consumer Services":       "Negocios",
    "Shopping":                             "Shopping",
    "Games":                                "Juegos",
    "Food and Drink":                       "Comida & Bebidas",
    "Hobbies and Leisure":                  "Hobbies",
    "Science and Education":                "Educación",
    "Law and Government":                   "Gobierno",
    "Vehicles":                             "Vehículos",
    "Adult":                                "Adultos",
    "Reference Materials":                  "Referencia",
    "Community and Society":                "Comunidad",
    "Home and Garden":                      "Hogar",
    "Pets and Animals":                     "Mascotas",
    "Beauty and Fashion":                   "Moda & Belleza",
    "Family and Parenting":                 "Familia",
    "Real Estate":                          "Inmobiliaria",
    "Jobs and Career":                      "Empleo",
  };
  return map[root] || root;
}

function mapCategory(category) {
  if (!category) return "";
  const lower = category.toLowerCase();
  if (lower.includes("sport"))                                                    return "sports";
  if (lower.includes("news") || lower.includes("media"))                         return "news";
  if (lower.includes("finance") || lower.includes("banking"))                    return "finance";
  if (lower.includes("tech") || lower.includes("computer"))                      return "technology";
  if (lower.includes("entertainment") || lower.includes("music") || lower.includes("movie")) return "entertainment";
  if (lower.includes("health") || lower.includes("medical") || lower.includes("wellness"))   return "health";
  if (lower.includes("travel") || lower.includes("hotel") || lower.includes("tourism"))      return "travel";
  if (lower.includes("gambl") || lower.includes("casino") || lower.includes("betting") || lower.includes("poker")) return "gambling";
  if (lower.includes("automotive") || lower.includes("vehicle") || lower.includes("car"))    return "automotive";
  if (lower.includes("food") || lower.includes("drink") || lower.includes("recipe") || lower.includes("cooking") || lower.includes("restaurant")) return "food";
  if (lower.includes("real_estate") || lower.includes("property") || lower.includes("housing") || lower.includes("inmob")) return "realestate";
  if (lower.includes("business") || lower.includes("marketing") || lower.includes("b2b") || lower.includes("digital_marketing")) return "business";
  return "";
}

function showError(msg) {
  const div = document.createElement("div");
  div.style.cssText = "padding:20px;color:#fc8181;font-family:sans-serif;font-size:13px;";
  div.textContent = msg;
  document.body.innerHTML = "";
  document.body.appendChild(div);
}

// ============================================================
// DIAGNÓSTICO DE APIs
// ============================================================
async function runDiagnostic() {
  const btn    = document.getElementById("btn-run-diag");
  const resEl  = document.getElementById("diag-result");
  const domain = document.getElementById("diag-domain").value.trim()
    .replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "")
    || "bbc.com";

  btn.disabled = true; btn.textContent = "⏳";
  resEl.innerHTML = `
    <div class="diag-row"><span class="diag-label">SimilarWeb</span><span class="diag-loading">Testing...</span></div>
    <div class="diag-row"><span class="diag-label">Apollo</span><span class="diag-loading">Testing...</span></div>
  `;

  // ── Test SimilarWeb (via proxy) ────────────────────────────
  const swPromise = (async () => {
    try {
      const r = await callProxy("rapidapi", `/similar-sites?domain=${encodeURIComponent(domain)}`, { method: "GET" });
      const quota = r.quota?.providerRemaining != null ? ` · ${r.quota.providerRemaining} quota left` : "";
      if (!r.ok) return { ok: false, msg: `HTTP ${r.status}${quota}` };
      const d = r.data || {};
      if (d.error || !d.Visits) return { ok: false, msg: `No data for ${domain}${quota}` };
      return { ok: true, msg: `${Math.round(d.Visits / 1000)}K visits/mo${quota}` };
    } catch (e) {
      return { ok: false, msg: e.message };
    }
  })();

  // ── Test Apollo (via proxy) ─────────────────────────────────
  const apolloPromise = (async () => {
    try {
      const r = await callProxy("apollo", "/v1/mixed_people/api_search", {
        method: "POST",
        body: {
          q_organization_domains_list: [domain],
          person_titles: ["CEO","founder","owner","publisher","editor"],
          per_page: 3, page: 1,
        },
      });
      if (!r.ok) {
        const err = r.data || {};
        return { ok: false, msg: `HTTP ${r.status}: ${err?.message || r.text?.substring(0, 80) || ""}` };
      }
      const d      = r.data || {};
      const people = Array.isArray(d?.people) ? d.people : [];
      if (people.length === 0) return { ok: false, msg: "No results for this domain" };
      const found = people
        .filter(p => p.email)
        .map(p => `${p.first_name || ""} ${p.last_name || ""} — ${p.email} (${p.email_status || "?"})`.trim())
        .slice(0, 2).join(" · ");
      return { ok: true, msg: found || `${people.length} persona(s) encontradas` };
    } catch (e) {
      return { ok: false, msg: e.message };
    }
  })();

  const [sw, apollo] = await Promise.all([swPromise, apolloPromise]);

  resEl.innerHTML = `
    <div class="diag-row">
      <span class="diag-label">SimilarWeb</span>
      <span class="${sw.ok ? "diag-ok" : "diag-error"}">${sw.ok ? "✅" : "❌"} ${esc(sw.msg)}</span>
    </div>
    <div class="diag-row">
      <span class="diag-label">Apollo</span>
      <span class="${apollo.ok ? "diag-ok" : "diag-error"}">${apollo.ok ? "✅" : "❌"} ${esc(apollo.msg)}</span>
    </div>
    <div class="diag-detail" style="margin-top:4px">Testeado con: ${esc(domain)}</div>
  `;

  btn.disabled = false; btn.textContent = "▶ Testear";
}

const CONFIG_DIAG = {
  get RAPIDAPI_KEY()  { return CONFIG.RAPIDAPI_KEY; },
  get RAPIDAPI_HOST() { return CONFIG.RAPIDAPI_TRAFFIC_HOST; },
  get APOLLO_KEY()    { return CONFIG.APOLLO_API_KEY; },
};

// ============================================================
// LOGIN / AUTH
// ============================================================
function initLoginScreen() {
  const screen    = document.getElementById("login-screen");
  const btn       = document.getElementById("btn-login");
  const forgotBtn = document.getElementById("btn-forgot-password");
  const errorEl   = document.getElementById("login-error");
  const infoEl    = document.getElementById("login-info");

  screen.style.display = "flex";

  // Enter key en cualquier campo
  ["login-email", "login-password"].forEach(id => {
    document.getElementById(id).addEventListener("keydown", e => {
      if (e.key === "Enter") attemptLogin();
    });
  });

  btn.addEventListener("click", attemptLogin);

  // Prominent Disclosure (Chrome Web Store): el botón se habilita solo
  // cuando el usuario marca el consent. Persistimos en chrome.storage para
  // no pedirlo cada vez después del primer login.
  const consentBox = document.getElementById("consent-checkbox");
  if (consentBox) {
    chrome.storage.local.get(["_privacyConsentAt"]).then(({ _privacyConsentAt }) => {
      if (_privacyConsentAt) { consentBox.checked = true; btn.disabled = false; }
    }).catch(() => {});
    consentBox.addEventListener("change", () => {
      btn.disabled = !consentBox.checked;
      if (consentBox.checked) {
        chrome.storage.local.set({ _privacyConsentAt: new Date().toISOString() }).catch(() => {});
      }
    });
  }

  forgotBtn?.addEventListener("click", async () => {
    const email = document.getElementById("login-email").value.trim().toLowerCase();
    errorEl.textContent = ""; infoEl.textContent = "";

    const AUTHORIZED_RESET = new Set([
      "mgargiulo@adeqmedia.com",
      "sales@adeqmedia.com",
      "dhorovitz@adeqmedia.com",
    ]);

    if (!email) { errorEl.textContent = "Enter your email first"; return; }
    if (!AUTHORIZED_RESET.has(email)) { errorEl.textContent = "Email not authorized"; return; }

    forgotBtn.disabled = true; forgotBtn.textContent = "Sending...";
    const result = await supabaseResetPassword(email);
    forgotBtn.disabled = false; forgotBtn.textContent = "Forgot password?";

    if (result.error) {
      errorEl.textContent = result.error;
    } else {
      infoEl.textContent = "✓ Password reset email sent.";
    }
  });

  async function attemptLogin() {
    const email    = document.getElementById("login-email").value.trim().toLowerCase();
    const password = document.getElementById("login-password").value;

    errorEl.textContent = "";

    const AUTHORIZED = {
      "mgargiulo@adeqmedia.com": "Max",
      "sales@adeqmedia.com":     "Agus",
      "dhorovitz@adeqmedia.com": "Diego",
    };

    if (!AUTHORIZED[email]) {
      errorEl.textContent = "Unauthorized email";
      return;
    }
    if (!password) {
      errorEl.textContent = "Enter your password";
      return;
    }

    btn.disabled = true; btn.textContent = "⏳ Signing in...";

    const result = await supabaseSignIn(email, password);
    if (result.error) {
      errorEl.textContent = result.error;
      btn.disabled = false; btn.textContent = "Sign In";
      return;
    }

    const auth = {
      loggedIn:     true,
      user:         email,
      name:         AUTHORIZED[email],
      ts:           Date.now(),
      accessToken:  result.access_token,
      refreshToken: result.refresh_token,
      expiresAt:    Date.now() + (result.expires_in * 1000),
    };
    await chrome.storage.local.set({ auth });
    window.location.reload();
  }
}

function applyUserFromAuth(auth) {
  const AUTHORIZED = {
    "mgargiulo@adeqmedia.com": "Max",
    "sales@adeqmedia.com":     "Agus",
    "dhorovitz@adeqmedia.com": "Diego",
  };

  const email = (auth?.user || "").toLowerCase().trim();
  const name  = AUTHORIZED[email] || auth?.name || "";

  if (!name) {
    console.error("[auth] No se pudo determinar el usuario logueado.");
    // Forzar re-login: borra auth y recarga
    chrome.storage.local.remove("auth").then(() => window.location.reload());
    return;
  }

  state.mediaBuyer   = name;
  state.loginEmail   = email;
  state.accessToken  = auth?.accessToken || "";
  state.role         = getRole(email);
  // Marcar el body con el role para que el CSS pueda mostrar/ocultar UI admin
  document.body.setAttribute("data-role", state.role);
  // Wire del triple-click para TODOS los users — los no-admin reciben un alert
  // visible cuando lo intentan (en vez de fallar silencioso)
  wireAdminViewToggle();
  // Cleanup periódico que solo el admin dispara: marca como 'expired' los
  // handoffs que llevan +7 días sin aceptarse. No bloquea, fire-and-forget.
  if (state.role === "admin") {
    expireOldHandoffs(state.accessToken).catch(() => {});
  }

  // Pre-seleccionar ejecutivo en el form de Monday
  const execSel = document.getElementById("form-ejecutivo");
  if (execSel) execSel.value = name;

  // Mostrar sesión activa en settings
  const userEl = document.getElementById("settings-user");
  if (userEl) userEl.textContent = `${name} · ${email}`;
}

// ── CSV Bulk Queue ────────────────────────────────────────────
async function initCsvQueue() {
  const fileInput      = document.getElementById("csv-file-input");
  const uploadBtn      = document.getElementById("btn-csv-upload");
  const uploadRes      = document.getElementById("csv-upload-result");
  const statsEl        = document.getElementById("csv-queue-stats");
  const refreshBtn     = document.getElementById("btn-csv-refresh");
  const enabledCbx     = document.getElementById("csv-queue-enabled");
  const clearProc      = document.getElementById("btn-csv-clear-processed");
  const clearAll       = document.getElementById("btn-csv-clear-all");
  const historyEl      = document.getElementById("csv-history-list");
  const historyRefresh = document.getElementById("btn-csv-history-refresh");
  if (!uploadBtn) return;

  const refreshStats = async () => {
    statsEl.textContent = "Loading...";
    const stats = await getCsvQueueStats(state.accessToken);
    statsEl.innerHTML = `
      Total: <strong>${stats.total}</strong><br>
      ⏳ Pending: <strong>${stats.pending}</strong> · 🔄 Processing: <strong>${stats.processing}</strong><br>
      ✅ Done: <strong>${stats.done}</strong> · ❌ Error: <strong>${stats.error}</strong> · ⏭ Skipped: <strong>${stats.skipped}</strong>
    `;
  };

  let currentHistorySource = "csv"; // "csv" | "monday"

  const refreshHistory = async () => {
    if (!historyEl) return;
    historyEl.textContent = "Loading...";
    const rows = await getCsvQueueHistory(state.accessToken, 30, currentHistorySource);
    if (rows.length === 0) {
      historyEl.innerHTML = `<div style="color:var(--text-muted);font-style:italic">No domains processed yet in "${currentHistorySource === "csv" ? "External CSV" : "Monday"}"</div>`;
      return;
    }
    const statusIcon = { done: "✅", error: "❌", skipped: "⏭" };
    historyEl.innerHTML = rows.map(r => {
      const icon = statusIcon[r.status] || "•";
      const when = r.processed_at ? new Date(r.processed_at).toLocaleString("es-AR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }) : "";
      const err  = r.status === "error" && r.error_message ? ` <span style="color:#e53e3e">— ${esc(r.error_message.substring(0, 80))}</span>` : "";
      const skip = r.status === "skipped" && r.error_message ? ` <span style="color:var(--text-muted)">— ${esc(r.error_message)}</span>` : "";
      return `<div style="padding:3px 0;border-bottom:1px solid var(--border)"><span>${icon}</span> <strong>${esc(r.domain)}</strong> <span style="color:var(--text-muted)">${when}</span>${err}${skip}</div>`;
    }).join("");
  };

  // Tab switcher del historial
  const tabCsv    = document.getElementById("tab-history-csv");
  const tabMonday = document.getElementById("tab-history-monday");
  const setTab = (which) => {
    currentHistorySource = which;
    [tabCsv, tabMonday].forEach(t => {
      if (!t) return;
      t.classList.remove("history-tab-active");
      t.style.borderBottomColor = "transparent";
    });
    const active = which === "csv" ? tabCsv : tabMonday;
    if (active) { active.classList.add("history-tab-active"); active.style.borderBottomColor = "var(--primary)"; }
    refreshHistory();
  };
  tabCsv?.addEventListener("click", () => setTab("csv"));
  tabMonday?.addEventListener("click", () => setTab("monday"));

  const refreshAll = async () => { await Promise.all([refreshStats(), refreshHistory()]); };

  // Auto-refresh cada 10s cuando AUTO ON está activo
  let heartbeatTimer = null;
  const startHeartbeat = () => {
    if (heartbeatTimer) return;
    heartbeatTimer = setInterval(() => {
      if (document.visibilityState === "hidden") return;
      refreshStats(); refreshHistory();
    }, 10_000);
  };
  const stopHeartbeat = () => { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } };

  // Estado inicial del toggle + mutex check
  const refreshCsvMutex = async () => {
    const st = await import("../modules/supabase.js").then(m => m.getCsvQueueState(state.accessToken)).catch(() => null);
    if (!st) return;
    enabledCbx.checked = st.enabled;
    const isMine = st.sessionUser && st.sessionUser.toLowerCase() === (state.loginEmail || "").toLowerCase();
    const otherActive = st.enabled && st.sessionUser && !isMine;
    // UI lock cuando otro user lo tiene activo
    enabledCbx.disabled = otherActive;
    let badge = document.getElementById("csv-queue-lock-badge");
    if (otherActive) {
      if (!badge) {
        badge = document.createElement("div");
        badge.id = "csv-queue-lock-badge";
        badge.style.cssText = "font-size:10px;color:#991b1b;background:rgba(239,68,68,0.12);border:1px solid #ef4444;border-radius:4px;padding:3px 6px;margin-top:6px";
        enabledCbx.closest("div").parentElement?.appendChild(badge);
      }
      const ownerShort = st.sessionUser.split("@")[0];
      badge.textContent = `🔒 Locked by ${ownerShort}. Solo ese user puede apagar este toggle.`;
    } else {
      badge?.remove();
    }
  };
  await refreshCsvMutex();
  if (enabledCbx.checked) startHeartbeat();
  enabledCbx.addEventListener("change", async () => {
    // Pre-check mutex antes de tocar
    const st = await import("../modules/supabase.js").then(m => m.getCsvQueueState(state.accessToken)).catch(() => null);
    if (st && st.enabled && st.sessionUser
        && st.sessionUser.toLowerCase() !== (state.loginEmail || "").toLowerCase()) {
      alert(`🔒 ${st.sessionUser} prendió Auto Import. Solo ese usuario puede apagarlo.`);
      enabledCbx.checked = true; // forzar a quedar visualmente ON
      return;
    }
    await setCsvQueueEnabled(enabledCbx.checked, state.accessToken, state.loginEmail);
    if (enabledCbx.checked) startHeartbeat();
    else stopHeartbeat();
    await refreshCsvMutex();
  });
  // Polling cada 30s para refrescar el lock badge si otro MB cambia el toggle
  setInterval(refreshCsvMutex, 30_000);

  refreshBtn.addEventListener("click", refreshStats);
  historyRefresh?.addEventListener("click", refreshHistory);
  await refreshAll();

  // Upload CSV
  uploadBtn.addEventListener("click", async () => {
    const file = fileInput.files?.[0];
    if (!file) { uploadRes.textContent = "Pick a CSV file first"; uploadRes.className = "push-result error"; return; }

    uploadBtn.disabled = true; uploadBtn.textContent = "⏳...";
    uploadRes.textContent = "Reading file..."; uploadRes.className = "push-result";

    try {
      const text    = (await file.text()).replace(/^\uFEFF/, "");
      const domains = text.split(/[\r\n]+/)
        .map(l => l.trim())
        .filter(Boolean)
        // Tomar la primera columna — respeta comillas básicas ("acme, inc.com" → acme, inc.com)
        .map(l => {
          const m = l.match(/^"([^"]*)"|^([^,]*)/);
          return ((m && (m[1] ?? m[2])) || "").trim();
        })
        // Limpiar dominio
        .map(d => d.replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/.*$/, "").toLowerCase())
        .filter(d => /^[a-z0-9.-]+\.[a-z]{2,}$/i.test(d));

      if (domains.length === 0) { uploadRes.textContent = "No valid domains found"; uploadRes.className = "push-result error"; return; }

      // Dedupe
      const unique = [...new Set(domains)];

      // ERROR si pasa el límite por tirada (75). Decisión user 2026-05-08:
      // no permitir uploads de más de 75 por vez. Si tienen un CSV grande,
      // que lo dividan en archivos de hasta 75 dominios cada uno.
      if (unique.length > 75) {
        uploadRes.innerHTML = `❌ <strong>Too many domains:</strong> ${unique.length} found. Per-batch limit is <strong>75 domains</strong>. Split your CSV into smaller files (max 75 each) and upload them separately. Daily cap stays at 300/user.`;
        uploadRes.className = "push-result error";
        return;
      }

      uploadRes.textContent = `Uploading ${unique.length} domains...`;

      const result = await uploadCsvDomains(unique, state.loginEmail, state.accessToken);
      uploadRes.textContent = `✅ ${result.inserted} added (${result.attempted - result.inserted} duplicates ignored).`;
      uploadRes.className = "push-result ok";
      fileInput.value = "";
      await refreshAll();
    } catch (err) {
      uploadRes.textContent = `❌ ${err.message}`;
      uploadRes.className = "push-result error";
    } finally {
      uploadBtn.disabled = false; uploadBtn.textContent = "Upload";
    }
  });

  // Refresh desde Monday (sin CSV) — busca Ciclo Finalizado + filtros
  document.getElementById("btn-refresh-from-monday")?.addEventListener("click", async () => {
    const btn      = document.getElementById("btn-refresh-from-monday");
    const resultEl = document.getElementById("refresh-from-monday-result");
    const geo      = document.getElementById("refresh-geo").value;
    const idioma   = document.getElementById("refresh-idioma").value;
    // Cap fijo 75 por tirada (decisión user 2026-05-08). El input refresh-limit
    // ahora es hidden con value="75" — ya no hay UI para cambiarlo.
    const limit    = 75;

    btn.disabled = true; btn.textContent = "⏳ Querying Monday...";
    resultEl.textContent = ""; resultEl.className = "push-result";

    try {
      const domains = await fetchMondayForRefresh({ geo, idioma, limit });
      if (domains.length === 0) {
        resultEl.textContent = "No Ciclo Finalizado domains match those filters.";
        resultEl.className = "push-result error";
        return;
      }
      resultEl.textContent = `Found ${domains.length}, uploading to queue...`;
      const up = await uploadCsvDomains(domains, state.loginEmail, state.accessToken, "monday");
      resultEl.textContent = `✅ ${up.inserted} added (${domains.length - up.inserted} already queued).`;
      // Si tu user ya alcanzó el límite diario, avisar
      if (up.inserted === 0 && domains.length > 0) {
        resultEl.textContent += " Si llegaste al cap diario de 300, esperá hasta mañana o pedile al admin que lo suba.";
      }
      resultEl.className = "push-result ok";
      await refreshAll();
    } catch (err) {
      resultEl.textContent = `❌ ${err.message}`;
      resultEl.className = "push-result error";
    } finally {
      btn.disabled = false; btn.textContent = "🔄 Fetch &amp; queue from Monday";
    }
  });

  clearProc.addEventListener("click", async () => {
    if (!confirm("¿Limpiar entries ya procesados (done/error/skipped)?\n\nNO afecta los prospectos generados ni la cola pending.")) return;
    await clearCsvQueue(state.accessToken, true);
    await refreshAll();
  });

  // Show toggle: revela el botón Clear ALL solo si el user lo pide explícitamente
  document.getElementById("btn-csv-show-clear-all")?.addEventListener("click", (e) => {
    const allBtn = clearAll;
    if (allBtn.style.display === "none") { allBtn.style.display = ""; e.target.style.display = "none"; }
  });

  clearAll.addEventListener("click", async () => {
    // Doble confirmación: primero un confirm general, después escribir "BORRAR" para confirmar
    if (!confirm("⚠️ Esto borra TODO de la cola de imports incluyendo items PENDING (en cola sin procesar).\n\nLos prospectos ya generados en Prospects NO se borran, solo la cola de input.\n\n¿Continuar?")) return;
    const confirmText = prompt("Para confirmar, escribí BORRAR (en mayúsculas):");
    if (confirmText !== "BORRAR") { alert("Cancelado."); return; }
    await clearCsvQueue(state.accessToken, false);
    await refreshStats();
    // Re-ocultar el botón ALL después de usarlo
    clearAll.style.display = "none";
    document.getElementById("btn-csv-show-clear-all").style.display = "";
  });

  // ── sellers.json import ─────────────────────────────────────
  await initSellersJsonImport(refreshAll);
}

// Caps de sellers.json import:
//   - QUEUE: 75 dominios por tirada (igual que Monday/CSV)
//   - OPEN TABS: 30 pestañas por click (memory + popup blocker)
//   - QUEUE PENDING TOTAL: 300 (compartido entre todos los imports)
const SELLERS_QUEUE_CAP_PER_RUN = 75;
const SELLERS_OPEN_TABS_CAP     = 30;
const MAX_QUEUE_PENDING         = 300;

async function initSellersJsonImport(refreshAll) {
  const { DEFAULT_SELLERS_COMPANIES, fetchSellersJson, findKnownDomains } = await import("../modules/sellersJson.js");
  const sel        = document.getElementById("sellers-company-select");
  const urlEl      = document.getElementById("sellers-company-url");
  const capInput   = document.getElementById("sellers-cap-input");
  const fetchBtn   = document.getElementById("btn-sellers-fetch");
  const resEl      = document.getElementById("sellers-result");
  const editBtn    = document.getElementById("btn-sellers-edit");
  const modal      = document.getElementById("sellers-edit-modal");
  const modalClose = document.getElementById("btn-sellers-edit-close");
  const modalSave  = document.getElementById("btn-sellers-edit-save");
  const modalReset = document.getElementById("btn-sellers-edit-reset");
  const modalArea  = document.getElementById("sellers-edit-textarea");
  const modalStat  = document.getElementById("sellers-edit-status");
  if (!sel || !fetchBtn) return;

  // Persistir lista en chrome.storage.local. Cada user puede tener su lista
  // (no es un setting compartido del equipo).
  const STORAGE_KEY = "sellers_companies_v1";
  const loadList = async () => {
    const { [STORAGE_KEY]: stored } = await chrome.storage.local.get(STORAGE_KEY);
    return Array.isArray(stored) && stored.length ? stored : DEFAULT_SELLERS_COMPANIES;
  };
  const saveList = async (list) => chrome.storage.local.set({ [STORAGE_KEY]: list });

  const renderSelect = async () => {
    const list = await loadList();
    sel.innerHTML = list.map((c, i) => `<option value="${i}">${c.name}</option>`).join("");
    const cur = list[0];
    if (cur && urlEl) urlEl.textContent = cur.url;
    sel.dataset._list = JSON.stringify(list);
  };
  await renderSelect();

  sel.addEventListener("change", () => {
    const list = JSON.parse(sel.dataset._list || "[]");
    const cur  = list[parseInt(sel.value, 10)];
    if (cur && urlEl) urlEl.textContent = cur.url;
  });

  // Edit modal
  editBtn?.addEventListener("click", async () => {
    const list = await loadList();
    modalArea.value = list.map(c => `${c.name} | ${c.url}`).join("\n");
    modalStat.textContent = "";
    modal.style.display = "flex";
  });
  modalClose?.addEventListener("click", () => { modal.style.display = "none"; });
  modal.addEventListener("click", (e) => { if (e.target === modal) modal.style.display = "none"; });
  modalReset?.addEventListener("click", () => {
    if (!confirm("Restaurar lista por defecto? Se pierde lo editado.")) return;
    modalArea.value = DEFAULT_SELLERS_COMPANIES.map(c => `${c.name} | ${c.url}`).join("\n");
  });
  modalSave?.addEventListener("click", async () => {
    const lines = (modalArea.value || "").split("\n").map(l => l.trim()).filter(Boolean);
    const parsed = [];
    const errors = [];
    lines.forEach((line, i) => {
      const m = line.match(/^(.+?)\s*\|\s*(https?:\/\/\S+)$/);
      if (!m) { errors.push(`Línea ${i + 1}: formato inválido`); return; }
      const url = m[2];
      if (!url.includes("sellers.json")) errors.push(`Línea ${i + 1}: la URL debería terminar en /sellers.json`);
      parsed.push({ name: m[1].trim(), url });
    });
    if (parsed.length === 0) { modalStat.style.color = "#dc2626"; modalStat.textContent = "❌ Sin entries válidos."; return; }
    await saveList(parsed);
    await renderSelect();
    modalStat.style.color = errors.length ? "#d97706" : "#16a34a";
    modalStat.textContent = errors.length
      ? `⚠️ Guardado ${parsed.length} con avisos: ${errors.slice(0, 2).join("; ")}`
      : `✅ Guardado ${parsed.length} empresa(s).`;
    setTimeout(() => { modal.style.display = "none"; }, 1500);
  });

  // Fetch + queue
  fetchBtn.addEventListener("click", async () => {
    const list = JSON.parse(sel.dataset._list || "[]");
    const company = list[parseInt(sel.value, 10)];
    if (!company) return;
    // Cap auto-clamped: si user pone más de 75, lo bajamos a 75
    const userCap = parseInt(capInput.value, 10) || SELLERS_QUEUE_CAP_PER_RUN;
    const cap = Math.max(1, Math.min(SELLERS_QUEUE_CAP_PER_RUN, userCap));
    if (userCap > SELLERS_QUEUE_CAP_PER_RUN) capInput.value = String(cap);

    // Pre-check: queue pending capacity
    const stats = await import("../modules/supabase.js").then(m => m.getCsvQueueStats(state.accessToken));
    const pendingNow = stats?.pending || 0;
    const space     = MAX_QUEUE_PENDING - pendingNow;
    if (space <= 0) {
      resEl.innerHTML = `<span style="color:#dc2626">❌ Queue llena: ${pendingNow}/${MAX_QUEUE_PENDING} pending. Esperá que el worker procese antes de cargar más.</span>`;
      return;
    }
    const allowed = Math.min(cap, space);

    fetchBtn.disabled = true;
    resEl.innerHTML = `⏳ Fetching ${company.url}...`;
    try {
      const domains = await fetchSellersJson(company.url);
      if (domains.length === 0) {
        resEl.innerHTML = `<span style="color:#d97706">⚠️ No se encontraron PUBLISHER en sellers.json.</span>`;
        return;
      }
      // ── Dedup: filtrar dominios YA conocidos por el sistema ────
      // Skip los que están en csv_queue / review_queue / historial / sendtrack /
      // blocklist. Evita re-procesar leads que ya pasamos.
      resEl.innerHTML = `🔍 Found ${domains.length}. Chequeando duplicados contra sistema...`;
      const known = await findKnownDomains(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_ANON_KEY, state.accessToken, domains);
      const fresh = domains.filter(d => !known.has(d));
      const knownCount = domains.length - fresh.length;
      if (fresh.length === 0) {
        resEl.innerHTML = `<span style="color:#d97706">⚠️ ${domains.length} dominios — TODOS ya conocidos por el sistema (csv_queue/review_queue/historial/blocklist). Nada nuevo para encolar.</span>`;
        return;
      }
      const slice   = fresh.slice(0, allowed);
      const skipped = fresh.length - slice.length;
      const { uploadCsvDomains } = await import("../modules/supabase.js");
      const result = await uploadCsvDomains(slice, state.loginEmail, state.accessToken, "sellers_json");
      const ins = result?.inserted || 0;
      const dup = slice.length - ins;
      const lines = [
        `✅ ${company.name}: <strong>${ins} nuevos encolados</strong>`,
        `📊 Found total: ${domains.length} | Ya conocidos: ${knownCount} | Frescos: ${fresh.length}`,
      ];
      if (dup > 0)     lines.push(`⚠️ ${dup} duplicados a último momento (race con otro MB)`);
      if (skipped > 0) lines.push(`📥 ${skipped} no encolados (cap ${allowed} alcanzado)`);
      resEl.innerHTML = `<span style="color:#16a34a;line-height:1.5;display:block">${lines.join("<br/>")}</span>`;
      await refreshAll?.();
    } catch (e) {
      resEl.innerHTML = `<span style="color:#dc2626">❌ Error: ${esc(e.message || String(e))}</span>`;
    } finally {
      fetchBtn.disabled = false;
    }
  });

  // ── Open tabs: extrae dominios y los abre en pestañas (manual prospecting) ──
  // No encola en csv_queue. Igual aplica dedup contra sistema para no abrir
  // sitios ya prospectados o ya en la cola.
  document.getElementById("btn-sellers-open")?.addEventListener("click", async () => {
    const list = JSON.parse(sel.dataset._list || "[]");
    const company = list[parseInt(sel.value, 10)];
    if (!company) return;
    // Cap auto-clamped a 30 para Open tabs (más que eso es kamikaze para Chrome)
    const userCap = parseInt(capInput.value, 10) || SELLERS_OPEN_TABS_CAP;
    const cap = Math.max(1, Math.min(SELLERS_OPEN_TABS_CAP, userCap));
    if (userCap > SELLERS_OPEN_TABS_CAP) capInput.value = String(cap);
    const openBtn = document.getElementById("btn-sellers-open");
    openBtn.disabled = true;
    resEl.innerHTML = `⏳ Fetching ${company.url}...`;
    try {
      const domains = await fetchSellersJson(company.url);
      if (domains.length === 0) {
        resEl.innerHTML = `<span style="color:#d97706">⚠️ Sin PUBLISHER en sellers.json.</span>`;
        return;
      }
      resEl.innerHTML = `🔍 Found ${domains.length}. Filtrando duplicados...`;
      const known = await findKnownDomains(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_ANON_KEY, state.accessToken, domains);
      const fresh = domains.filter(d => !known.has(d));
      if (fresh.length === 0) {
        resEl.innerHTML = `<span style="color:#d97706">⚠️ Todos ya conocidos por el sistema.</span>`;
        return;
      }
      const N = Math.min(cap, fresh.length);
      if (!confirm(`Abrir ${N} pestañas en el navegador?\n\nFound: ${domains.length} | Frescos: ${fresh.length} | Cap: ${SELLERS_OPEN_TABS_CAP}/click`)) {
        return;
      }
      const slice = fresh.slice(0, N);
      resEl.innerHTML = `🪟 Abriendo ${N} pestañas (delay 400ms para que Chrome no las bloquee)...`;
      slice.forEach((domain, i) => {
        setTimeout(() => chrome.tabs.create({ url: `https://${domain}`, active: false }), i * 400);
      });
      const left = fresh.length - N;
      resEl.innerHTML = `<span style="color:#16a34a">✅ ${N} pestañas abiertas${left > 0 ? `. ${left} frescos sin abrir (cap ${SELLERS_OPEN_TABS_CAP}/click).` : "."}</span>`;
    } catch (e) {
      resEl.innerHTML = `<span style="color:#dc2626">❌ Error: ${esc(e.message || String(e))}</span>`;
    } finally {
      openBtn.disabled = false;
    }
  });

  // Discover REMOVIDO — feature poco útil por user request 2026-05-11.
  // probeSellersJson + fetchAdsTxtSystems siguen exportadas en sellersJson.js
  // por si en futuro se reactivan.
}

// ── Autopilot toggle + target ─────────────────────────────────
const AUTOPILOT_DURATION_MS = 60 * 60 * 1000; // 1 hora max de sesión autopilot
let _autopilotTimer = null;

// ── Pitch Drafts: cache + helpers ─────────────────────────────
// Cache de drafts del user (toda la lista, ordenada por priority asc).
// Se rellena en la primera apertura del modal o en el primer auto-fill.
const _draftsState = {
  all: [],            // todos los drafts del user + defaults
  byLang: new Map(),  // language -> drafts[] (ordenados priority asc)
  flagIdxByLang: new Map(), // language -> índice actual del rotator
  loaded: false,
  currentLang: "",
};

// Solo soportamos 5 idiomas en drafts: ES / EN / IT / PT / AR
const LANG_FLAG = { es:"🇪🇸", en:"🇬🇧", it:"🇮🇹", pt:"🇵🇹", ar:"🇸🇦" };
// GEO (alpha-2) → idioma del template. Países sin idioma soportado caen a EN.
const GEO_TO_LANG = {
  // Spanish-speaking
  AR:"es", MX:"es", CO:"es", CL:"es", PE:"es", UY:"es", PY:"es", BO:"es",
  EC:"es", VE:"es", DO:"es", CR:"es", PA:"es", GT:"es", HN:"es", SV:"es",
  NI:"es", CU:"es", PR:"es", ES:"es",
  // English-speaking
  US:"en", GB:"en", CA:"en", AU:"en", NZ:"en", IE:"en", IN:"en", ZA:"en", SG:"en",
  // Portuguese
  BR:"pt", PT:"pt",
  // Italian
  IT:"it", CH:"it",
  // Arabic
  AE:"ar", SA:"ar", EG:"ar", MA:"ar",
  // Otros países sin idioma propio en nuestro stack → EN como default razonable
  FR:"en", BE:"en", LU:"en", DE:"en", AT:"en", NL:"en", PL:"en", TR:"en",
};

function _rebuildDraftsByLang() {
  _draftsState.byLang.clear();
  for (const d of _draftsState.all) {
    const lang = d.language || "es";
    if (!_draftsState.byLang.has(lang)) _draftsState.byLang.set(lang, []);
    _draftsState.byLang.get(lang).push(d);
  }
  // priority asc, defaults primero ante empate
  for (const arr of _draftsState.byLang.values()) {
    arr.sort((a, b) => {
      const pa = a.priority ?? 3, pb = b.priority ?? 3;
      if (pa !== pb) return pa - pb;
      return (a.user_email === "_default_" ? -1 : 1) - (b.user_email === "_default_" ? -1 : 1);
    });
  }
}

async function loadDraftsCache(force = false) {
  if (_draftsState.loaded && !force) return;
  if (!state.accessToken) return;
  const drafts = await getPitchDrafts(state.accessToken, state.loginEmail);
  _draftsState.all = Array.isArray(drafts) ? drafts : [];
  _rebuildDraftsByLang();
  _draftsState.loaded = true;
}

function applyDraftToPitch(d, { silent = false } = {}) {
  if (!d) return;
  const domain = state.domain || "example.com";
  const subject = (d.subject || "").replace(/\{\{domain\}\}/g, domain);
  const body    = (d.body    || "").replace(/\{\{domain\}\}/g, domain);
  const pitchEl   = document.getElementById("pitch-text");
  const subjectEl = document.getElementById("form-subject");
  if (pitchEl)   pitchEl.value   = body;
  if (subjectEl && subject) subjectEl.value = subject;
  if (!silent) pitchEl?.dispatchEvent(new Event("input"));
}

// ── Detección robusta del idioma del pitch ───────────────────
// Cascada de señales (de más confiable a menos):
//   1. <html lang="..."> de la página + meta og:locale (lo que captura runPageContext)
//   2. Análisis del texto real (page title + description + footer) con heurísticas
//      por idioma (palabras frecuentes, caracteres únicos)
//   3. GEO_TO_LANG (mapeo país→idioma)
//   4. Fallback: "en"
// El user reporta que español a veces detecta inglés. Causa: SimilarWeb a veces
// devuelve GEO US para sitios .com.ar, y siteLanguage queda vacío si no hay
// <html lang>. La detección de texto resuelve ese caso.

function _detectLangFromText(text) {
  if (!text || text.length < 30) return null;
  const t = text.toLowerCase();
  // Caracteres únicos por idioma (señales fuertes)
  const hasArabic     = /[؀-ۿ]/.test(text);
  if (hasArabic) return "ar";

  // Conteo de palabras-marcador frecuentes (stopwords muy típicas)
  const markers = {
    es: /\b(que|los|las|para|por|con|una|del|este|esta|pero|cuando|donde|como|porque|sobre|tambien|nuestra|nuestro|hola|gracias|hace)\b/g,
    pt: /\b(que|nao|para|com|uma|por|esse|essa|mas|quando|onde|como|porque|sobre|nossa|nosso|ola|obrigad|dele|dela|voce)\b/g,
    it: /\b(che|non|per|con|una|del|della|sono|sono|questo|questa|quando|dove|come|perche|sopra|grazie|nostra|nostro|ciao)\b/g,
    en: /\b(the|and|that|for|with|this|from|have|been|will|would|could|should|about|which|their|there|where|when|because|hello|thanks)\b/g,
  };
  const scores = {};
  let total = 0;
  for (const [lang, re] of Object.entries(markers)) {
    const m = t.match(re);
    scores[lang] = m ? m.length : 0;
    total += scores[lang];
  }
  // Bonus por caracteres únicos
  if (/[ñáéíóúü¿¡]/.test(text)) scores.es += 5;
  if (/[ãõçàáâ]/.test(text))    scores.pt += 5;
  if (/[àèéìòù]/.test(text))    scores.it += 5;

  if (total < 3) return null; // muy poca señal
  // Ganador con margen de >=2 sobre el segundo (evita empates dudosos)
  const sorted = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  if (sorted[0][1] >= sorted[1][1] + 2) return sorted[0][0];
  return null;
}

const SUPPORTED_LANGS = new Set(["es", "en", "it", "pt", "ar"]);

function _resolvePitchLang() {
  // 1. siteLanguage del <html lang> — el más confiable cuando existe
  const htmlLang = (state.siteLanguage || "").toLowerCase().split("-")[0];
  if (SUPPORTED_LANGS.has(htmlLang)) return htmlLang;

  // 2. og:locale (es_AR, pt_BR, etc.)
  const og = (state.siteOgLocale || "").toLowerCase().split(/[-_]/)[0];
  if (SUPPORTED_LANGS.has(og)) return og;

  // 3. Análisis del texto real de la página (página + meta + footer)
  const sample = [state.pageTitle, state.pageDescription, state.siteFooterText]
    .filter(Boolean).join(" ").substring(0, 4000);
  const detected = _detectLangFromText(sample);
  if (detected && SUPPORTED_LANGS.has(detected)) return detected;

  // 4. Mapeo GEO → idioma (último recurso)
  const geoSel = document.getElementById("form-geo");
  const geoText = (geoSel?.value || "").toLowerCase();
  let geoCode = "";
  for (const [code, label] of Object.entries(GEO_LABEL)) {
    if (label.toLowerCase() === geoText) { geoCode = code; break; }
  }
  const langFromGeo = GEO_TO_LANG[geoCode];
  if (SUPPORTED_LANGS.has(langFromGeo)) return langFromGeo;

  // 5. Default
  return "en";
}

// Render del chip strip — todos los drafts visibles, idioma detectado primero.
// Reusable: pasale el contenedor, callback de selección, draftId activo.
function renderDraftChips(containerEl, { activeId = null, preferredLang = null, onPick }) {
  if (!containerEl) return;
  const all = _draftsState.all || [];
  if (all.length === 0) {
    containerEl.innerHTML = `<div class="draft-chips-empty">Sin borradores. Creá uno desde 📝 (header).</div>`;
    return;
  }
  // Ordenar: idioma preferido primero (por priority), después el resto agrupado por idioma
  const lang = preferredLang || "en";
  const matchLang = all.filter(d => d.language === lang)
                       .sort((a, b) => (a.priority ?? 3) - (b.priority ?? 3));
  const otherLangs = all.filter(d => d.language !== lang)
                         .sort((a, b) => {
                           if (a.language !== b.language) return a.language.localeCompare(b.language);
                           return (a.priority ?? 3) - (b.priority ?? 3);
                         });
  const ordered = [...matchLang, ...otherLangs];

  containerEl.innerHTML = ordered.map(d => {
    const flag = LANG_FLAG[d.language] || "🌐";
    const cleanName = (d.name || "Template").replace(/^[A-Z]{2}\s*·\s*/, "");
    const isActive = String(d.id) === String(activeId);
    return `<button type="button" class="draft-chip${isActive ? " active" : ""}" data-id="${esc(String(d.id))}" title="${esc(d.subject || cleanName)}">
      <span class="draft-chip-flag">${flag}</span>${esc(cleanName)}
    </button>`;
  }).join("");

  containerEl.querySelectorAll(".draft-chip").forEach(chip => {
    chip.addEventListener("click", () => {
      const id = chip.dataset.id;
      const d  = all.find(x => String(x.id) === id);
      if (!d) return;
      // Marcar activo
      containerEl.querySelectorAll(".draft-chip").forEach(c => c.classList.remove("active"));
      chip.classList.add("active");
      onPick?.(d);
    });
  });
}

// Analysis tab: bandera + nombre del template + chips de idiomas a la derecha.
// Click bandera → rota dentro del idioma actual.
// Click chip de idioma → cambia el idioma + carga prio-1 de ese idioma.
function updatePitchFlagButton() {
  const flagBtn   = document.getElementById("btn-pitch-flag");
  const flagEmoji = document.getElementById("pitch-flag-emoji");
  const nameEl    = document.getElementById("pitch-flag-name");
  if (!flagBtn || !flagEmoji || !nameEl) return;
  const lang   = _draftsState.currentLang || _resolvePitchLang();
  const drafts = _draftsState.byLang.get(lang) || [];
  flagEmoji.textContent = LANG_FLAG[lang] || "🌐";
  if (drafts.length === 0) {
    nameEl.textContent = LANG_FLAG[lang] ? `Sin templates en ${lang.toUpperCase()}` : "Idioma desconocido";
    flagBtn.title = `No hay drafts. Abrí 📝 (header) para crear uno.`;
  } else {
    const idx     = _draftsState.flagIdxByLang.get(lang) ?? 0;
    const current = drafts[idx];
    const cleanName = (current?.name || `Template ${idx + 1}`).replace(/^[A-Z]{2}\s*·\s*/, "");
    nameEl.textContent = `${cleanName} (${idx + 1}/${drafts.length})`;
    flagBtn.title = `Click para rotar templates del mismo idioma`;
  }
  // Chips de idioma — solo los que tienen al menos 1 draft
  const flagsContainer = document.getElementById("pitch-lang-flags");
  if (flagsContainer) {
    const order = ["es", "en", "it", "pt", "ar"];
    flagsContainer.innerHTML = order.map(l => {
      const flag    = LANG_FLAG[l];
      const has     = (_draftsState.byLang.get(l) || []).length > 0;
      const cls     = `pitch-lang-flag${l === lang ? " active" : ""}${has ? "" : " disabled"}`;
      return `<button type="button" class="${cls}" data-lang="${l}" title="${has ? "Cambiar a " + l.toUpperCase() : "Sin templates en " + l.toUpperCase()}">${flag}</button>`;
    }).join("");
    flagsContainer.querySelectorAll(".pitch-lang-flag").forEach(btn => {
      btn.addEventListener("click", () => {
        const l = btn.dataset.lang;
        const list = _draftsState.byLang.get(l) || [];
        if (list.length === 0) return;
        _draftsState.currentLang = l;
        _draftsState.flagIdxByLang.set(l, 0);
        // Aplicar prio-1 del nuevo idioma
        const d = list[0];
        const domain = state.domain || "example.com";
        const ptEl   = document.getElementById("pitch-text");
        const subjEl = document.getElementById("form-subject");
        if (ptEl)   ptEl.value   = (d.body    || "").replace(/\{\{domain\}\}/g, domain);
        if (subjEl) subjEl.value = (d.subject || "").replace(/\{\{domain\}\}/g, domain);
        ptEl?.dispatchEvent(new Event("input"));
        updatePitchFlagButton();
      });
    });
  }
}

// Auto-carga el draft de prioridad 1 (o el primero ordenado) en el pitch al cargar la web.
// Se invoca después de runAutoFill para tener la GEO ya seteada.
async function autofillDraftOnLoad() {
  await loadDraftsCache();
  const lang   = _resolvePitchLang();
  _draftsState.currentLang = lang;
  const drafts = _draftsState.byLang.get(lang) || [];
  if (drafts.length === 0) {
    updatePitchFlagButton();
    return;
  }
  _draftsState.flagIdxByLang.set(lang, 0);
  applyDraftToPitch(drafts[0], { silent: true });
  updatePitchFlagButton();
}

function rotatePitchTemplate() {
  const lang   = _draftsState.currentLang || _resolvePitchLang();
  const drafts = _draftsState.byLang.get(lang) || [];
  if (drafts.length === 0) return;
  const cur  = _draftsState.flagIdxByLang.get(lang) ?? 0;
  const next = (cur + 1) % drafts.length;
  _draftsState.flagIdxByLang.set(lang, next);
  applyDraftToPitch(drafts[next], { silent: true });
  updatePitchFlagButton();
}

function initPitchInlineControls() {
  const flagBtn  = document.getElementById("btn-pitch-flag");
  const clearBtn = document.getElementById("btn-pitch-clear");
  const pitchEl  = document.getElementById("pitch-text");
  flagBtn?.addEventListener("click", rotatePitchTemplate);
  clearBtn?.addEventListener("click", () => {
    if (!pitchEl) return;
    pitchEl.value = "";
    pitchEl.dispatchEvent(new Event("input"));
    pitchEl.focus();
  });
  // Si cambia la GEO manualmente, recalcular idioma + actualizar bandera
  document.getElementById("form-geo")?.addEventListener("change", () => {
    _draftsState.currentLang = _resolvePitchLang();
    _draftsState.flagIdxByLang.set(_draftsState.currentLang, 0);
    updatePitchFlagButton();
  });
}

// ── Pitch Drafts modal ────────────────────────────────────────
function initPitchDrafts() {
  const openBtn    = document.getElementById("btn-pitch-draft");
  const modal      = document.getElementById("drafts-modal");
  const overlay    = document.getElementById("drafts-modal-overlay");
  const closeBtn   = document.getElementById("btn-drafts-close");
  const listEl     = document.getElementById("drafts-list");
  const nameEl     = document.getElementById("draft-name");
  const langEl     = document.getElementById("draft-language");
  const prioEl     = document.getElementById("draft-priority");
  const subjectEl  = document.getElementById("draft-subject");
  const bodyEl     = document.getElementById("draft-body");
  const saveBtn    = document.getElementById("btn-draft-save");
  const newBtn     = document.getElementById("btn-draft-new");
  const delBtn     = document.getElementById("btn-draft-delete");
  const modeLbl    = document.getElementById("drafts-form-mode");
  const resultEl   = document.getElementById("draft-save-result");
  if (!openBtn || !modal) return;

  let editingId = null;
  let lastDrafts = [];

  const clearForm = () => {
    editingId = null;
    nameEl.value = ""; subjectEl.value = ""; bodyEl.value = "";
    langEl.value = state.siteLanguage || "es";
    if (prioEl) prioEl.value = "3";
    modeLbl.textContent = "Nuevo borrador";
    delBtn.style.display = "none";
    resultEl.textContent = "";
  };

  const renderList = (drafts) => {
    lastDrafts = drafts;
    if (drafts.length === 0) {
      listEl.innerHTML = '<div style="color:var(--text-muted);font-style:italic;padding:8px">Sin borradores. Creá el primero abajo.</div>';
      return;
    }
    // Agrupar por idioma para que se vea más prolijo
    const groups = new Map();
    for (const d of drafts) {
      const k = d.language || "??";
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(d);
    }
    const html = [];
    for (const [lang, items] of groups) {
      const flag = LANG_FLAG[lang] || "🌐";
      html.push(`<div style="font-size:10px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:.4px;margin:8px 0 4px">${flag} ${lang.toUpperCase()} · ${items.length} ${items.length === 1 ? "borrador" : "borradores"}</div>`);
      for (const d of items) {
        const isDefault = d.user_email === "_default_";
        const tagClass  = isDefault ? "color:#0369a1" : "color:var(--text-muted)";
        const prio      = d.priority ?? 3;
        const stars     = "⭐".repeat(prio);
        // Toda la card es clickeable para editar (incluso defaults — al editar
        // un default se crea una copia privada del user, los demás siguen viendo el original)
        html.push(`
          <div class="draft-item" data-id="${d.id}" style="padding:10px;border:1px solid var(--border);border-radius:6px;margin-bottom:6px;background:var(--bg-soft, #fafafa);cursor:pointer" title="Click para editar">
            <div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
              <strong style="font-size:13px">${esc(d.name)}</strong>
              <span style="font-size:10px; ${tagClass}" title="Prioridad ${prio}">${stars}${isDefault ? " · DEFAULT" : ""}</span>
            </div>
            ${d.subject ? `<div style="font-size:11px;color:var(--text-muted);margin-top:4px"><strong>Asunto:</strong> ${esc(d.subject)}</div>` : ""}
            <div style="font-size:10px;color:var(--text-muted);margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc((d.body || "").substring(0, 100))}…</div>
            <div style="display:flex;gap:4px;margin-top:8px">
              <button class="btn btn-primary btn-sm draft-use-btn" data-id="${d.id}" style="font-size:10px;padding:3px 10px">✅ Usar este</button>
              <button class="btn btn-secondary btn-sm draft-edit-btn" data-id="${d.id}" style="font-size:10px;padding:3px 10px">✏️ Editar</button>
            </div>
          </div>`);
      }
    }
    listEl.innerHTML = html.join("");

    listEl.querySelectorAll(".draft-use-btn").forEach(b => b.addEventListener("click", (e) => {
      e.stopPropagation();
      const id = b.dataset.id;
      const d  = lastDrafts.find(x => String(x.id) === id);
      if (!d) return;
      applyDraftToPitch(d);
      close();
    }));

    listEl.querySelectorAll(".draft-edit-btn").forEach(b => b.addEventListener("click", (e) => {
      e.stopPropagation();
      const id = b.dataset.id;
      const d  = lastDrafts.find(x => String(x.id) === id);
      if (!d) return;
      editingId = d.id;
      nameEl.value = d.name; langEl.value = d.language;
      if (prioEl) prioEl.value = String(d.priority ?? 3);
      subjectEl.value = d.subject || ""; bodyEl.value = d.body;
      modeLbl.textContent = `Editando: ${d.name}`;
      delBtn.style.display = "inline-block";
    }));
  };

  const load = async () => {
    const drafts = await getPitchDrafts(state.accessToken, state.loginEmail);
    _draftsState.all = drafts; _rebuildDraftsByLang(); _draftsState.loaded = true;
    renderList(drafts);
    updatePitchFlagButton();
  };

  const open = () => {
    clearForm();
    modal.style.display = "flex";
    load();
  };
  const close = () => { modal.style.display = "none"; };

  openBtn.addEventListener("click", open);
  closeBtn?.addEventListener("click", close);
  overlay?.addEventListener("click", close);

  newBtn.addEventListener("click", clearForm);

  saveBtn.addEventListener("click", async () => {
    const name    = nameEl.value.trim();
    const body    = bodyEl.value.trim();
    const language = langEl.value;
    const subject  = subjectEl.value.trim();
    const priority = parseInt(prioEl?.value || "3", 10) || 3;
    if (!name)    { resultEl.textContent = "❌ Falta el nombre"; resultEl.style.color = "#e53e3e"; return; }
    if (!body)    { resultEl.textContent = "❌ Falta el cuerpo del email"; resultEl.style.color = "#e53e3e"; return; }

    saveBtn.disabled = true; saveBtn.textContent = "⏳...";
    const result = await savePitchDraft(state.accessToken, {
      id: editingId, user_email: state.loginEmail, name, language, subject, body, priority,
    });
    saveBtn.disabled = false; saveBtn.textContent = "💾 Guardar";

    if (result.ok) {
      resultEl.textContent = editingId ? "✅ Actualizado" : "✅ Guardado";
      resultEl.style.color = "#16a34a";
      editingId = result.data?.id || null;
      modeLbl.textContent = editingId ? `Editando: ${name}` : "Nuevo borrador";
      delBtn.style.display = editingId ? "inline-block" : "none";
      await load();
    } else {
      resultEl.textContent = `❌ ${result.error || "Error"}`;
      resultEl.style.color = "#e53e3e";
    }
  });

  delBtn.addEventListener("click", async () => {
    if (!editingId) return;
    if (!confirm("¿Borrar este borrador?")) return;
    await deletePitchDraft(state.accessToken, editingId);
    clearForm();
    await load();
  });
}

// Banner global rojo cuando un flag está ON pero Railway no late hace > 5 min.
// NO auto-apaga el toggle (evita loops). Solo avisa.
function renderRailwayDeadBanner({ heartbeatAt, autopilotEnabled, csvEnabled }) {
  const flagOn = autopilotEnabled || csvEnabled;
  const ageMs  = heartbeatAt ? (Date.now() - heartbeatAt.getTime()) : Infinity;
  const dead   = flagOn && ageMs > 5 * 60_000;
  let el = document.getElementById("railway-dead-banner");
  if (!dead) { el?.remove(); return; }
  if (!el) {
    el = document.createElement("div");
    el.id = "railway-dead-banner";
    el.style.cssText = "position:fixed;top:0;left:0;right:0;z-index:9999;background:#7f1d1d;color:#fff;font-size:12px;padding:8px 12px;text-align:center;border-bottom:2px solid #ef4444;cursor:pointer";
    el.title = "Click para ocultar (volverá a aparecer en 30s si sigue caído)";
    el.addEventListener("click", () => el.remove());
    document.body.prepend(el);
  }
  const ageLabel = ageMs === Infinity ? "nunca" : (ageMs > 3600_000 ? `${Math.round(ageMs/3600_000)}h` : `${Math.round(ageMs/60_000)}m`);
  const which = [autopilotEnabled && "Autopilot", csvEnabled && "Auto Import"].filter(Boolean).join(" + ");
  el.textContent = `⚠️ ${which} ON pero Railway no responde hace ${ageLabel}. Apagá y volvé a prender el toggle, o revisá Railway dashboard.`;
}

async function pollRailwayDeadBanner() {
  try {
    const [{ enabled: autopilotEnabled, heartbeatAt }, csvSt] = await Promise.all([
      getAutopilotState(state.accessToken),
      import("../modules/supabase.js").then(m => m.getCsvQueueState(state.accessToken)).catch(() => ({ enabled: false })),
    ]);
    renderRailwayDeadBanner({ heartbeatAt, autopilotEnabled, csvEnabled: !!csvSt?.enabled });
  } catch {}
}

function renderRailwayHeartbeat(heartbeatAt) {
  const el = document.getElementById("railway-heartbeat");
  if (!el) return;
  if (!heartbeatAt) {
    el.textContent = "Railway: never seen";
    el.style.color = "#f87171";
    return;
  }
  const ageSec = Math.round((Date.now() - heartbeatAt.getTime()) / 1000);
  let label, color;
  if (ageSec < 30)       { label = "🟢 Railway alive"; color = "#86efac"; }
  else if (ageSec < 180) { label = `🟡 Railway last seen ${ageSec}s ago`; color = "#fde047"; }
  else if (ageSec < 3600){ label = `🔴 Railway stale (${Math.round(ageSec/60)}m)`; color = "#f87171"; }
  else                   { label = `⚫ Railway down (${Math.round(ageSec/3600)}h)`; color = "#f87171"; }
  el.textContent = label;
  el.style.color = color;
}

async function initAutopilot() {
  const btn         = document.getElementById("btn-autopilot");
  const targetBtn   = document.getElementById("btn-autopilot-target");
  const panel       = document.getElementById("autopilot-target-panel");
  const geoSel      = document.getElementById("autopilot-target-geo");
  const catSel      = document.getElementById("autopilot-target-category");
  const trafficSel  = document.getElementById("autopilot-target-traffic");
  const saveBtn     = document.getElementById("btn-autopilot-target-save");
  const closeBtn    = document.getElementById("btn-autopilot-target-close");
  const currentLbl  = document.getElementById("autopilot-target-current");
  if (!btn) return;

  const [{ enabled, heartbeatAt, sessionUser, sessionStart }, target] = await Promise.all([
    getAutopilotState(state.accessToken),
    getAutopilotTarget(state.accessToken),
  ]);

  // If autopilot is currently active under a different user, warn — don't silently
  // overwrite their session. User can still enable, but knows they'll take over.
  if (enabled && sessionUser && sessionUser.toLowerCase() !== (state.loginEmail || "").toLowerCase()) {
    const heartbeatAge = heartbeatAt ? Math.round((Date.now() - heartbeatAt.getTime()) / 1000) : 999;
    if (heartbeatAge < 120) {
      console.warn(`[Autopilot] active under ${sessionUser} (heartbeat ${heartbeatAge}s ago). Enabling now will hijack their quota.`);
      const heartEl = document.getElementById("railway-heartbeat");
      if (heartEl) heartEl.title = `⚠️ ${sessionUser} is currently running autopilot — enabling will switch ownership`;
    }
  }

  renderRailwayHeartbeat(heartbeatAt);
  // Refresca heartbeat cada 15s mientras el tab Prospects esté visible
  setInterval(async () => {
    if (document.visibilityState === "hidden") return;
    const tab = document.getElementById("tab-prospects");
    if (!tab?.classList.contains("active")) return;
    const st = await getAutopilotState(state.accessToken);
    renderRailwayHeartbeat(st.heartbeatAt);
  }, 15_000);

  // RULE: keep autopilot ON across side-panel reopens IF this user owns the
  // active session and it hasn't exceeded its 60-min window. Force OFF only
  // when the session expired or belongs to a different user.
  const sessionAgeMs = (sessionStart instanceof Date)
    ? Date.now() - sessionStart.getTime()
    : Infinity;
  const meSession = enabled && sessionUser
    && sessionUser.toLowerCase() === (state.loginEmail || "").toLowerCase()
    && sessionAgeMs < AUTOPILOT_DURATION_MS;

  // Mostrar el toggle ON si CUALQUIER user tiene autopilot activo (visibilidad
  // compartida del estado del equipo). Si no soy el dueño, el botón queda
  // bloqueado con un cartel "Locked by <user>".
  const otherActive = enabled && !meSession && sessionUser
                   && sessionAgeMs < AUTOPILOT_DURATION_MS;

  if (meSession) {
    setAutopilotUI(btn, true);
    setAutopilotLockBadge(null); // limpio el lock badge si lo tenía
    const remaining = Math.max(0, AUTOPILOT_DURATION_MS - sessionAgeMs);
    if (remaining > 0) startAutopilotCountdown(btn, remaining);
  } else if (otherActive) {
    // Otro user lo tiene corriendo. Mostramos toggle ON pero bloqueado.
    setAutopilotUI(btn, true);
    btn.disabled = true;
    btn.style.opacity = "0.6";
    btn.style.cursor = "not-allowed";
    setAutopilotLockBadge(sessionUser, sessionAgeMs);
  } else {
    setAutopilotUI(btn, false);
    btn.disabled = false;
    btn.style.opacity = "";
    btn.style.cursor = "";
    setAutopilotLockBadge(null);
    if (enabled) {
      // session expired and nobody owns it — flip OFF in DB
      await setAutopilotEnabled(false, state.accessToken);
      console.log("[Autopilot] Forced OFF on panel open — session expired");
    }
  }

  updateTargetLabel(target, targetBtn, currentLbl);
  // Restore previously-selected geos (multi-select: comma-separated in config)
  if (geoSel) {
    const saved = new Set((target.geo || "").split(",").map(s => s.trim()).filter(Boolean));
    Array.from(geoSel.options).forEach(o => { o.selected = saved.has(o.value); });
  }
  if (catSel)     catSel.value     = target.category   || "";
  if (trafficSel) trafficSel.value = target.minTraffic || "400000";

  btn.addEventListener("click", async () => {
    if (btn.disabled) return; // locked por otro user
    const isOn = btn.classList.contains("active");
    if (isOn) {
      // Solo el dueño puede apagar — re-validar antes de cerrar
      const cur = await getAutopilotState(state.accessToken);
      if (cur.enabled && cur.sessionUser
          && cur.sessionUser.toLowerCase() !== (state.loginEmail || "").toLowerCase()) {
        alert(`🔒 Autopilot corriendo bajo ${cur.sessionUser}. Solo ese usuario puede apagarlo.`);
        return;
      }
      // Apagar (soy el dueño)
      clearAutopilotTimer();
      setAutopilotUI(btn, false);
      await setAutopilotEnabled(false, state.accessToken);
      endUsageSession(state.accessToken).catch(() => {});
      startUsageSession(state.accessToken, state.loginEmail, "popup").catch(() => {});
    } else {
      // Cap por usuario: el admin pudo desactivarle el autopilot a este MB
      const can = await checkUserCanDo(state.accessToken, state.loginEmail, "autopilot_on");
      if (!can.allowed) { alert(`⛔ ${can.reason}`); return; }

      // Mutex: otro user lo tiene activo
      const cur = await getAutopilotState(state.accessToken);
      if (cur.enabled && cur.sessionUser
          && cur.sessionUser.toLowerCase() !== (state.loginEmail || "").toLowerCase()
          && cur.heartbeatAt && (Date.now() - cur.heartbeatAt.getTime()) < 120_000) {
        const elapsed = cur.sessionStart ? Math.round((Date.now() - cur.sessionStart.getTime()) / 60000) : 0;
        const remaining = Math.max(0, 60 - elapsed);
        alert(`🔒 ${cur.sessionUser} ya tiene Autopilot corriendo (hace ${elapsed} min · termina en ~${remaining} min).\n\nNo se puede prender 2 autopilots a la vez. Esperá a que termine o pedile que lo apague.`);
        return;
      }
      // Turn on
      setAutopilotUI(btn, true);
      setAutopilotLockBadge(null);
      await setAutopilotEnabled(true, state.accessToken, state.loginEmail);
      startAutopilotCountdown(btn, AUTOPILOT_DURATION_MS);
      startUsageSession(state.accessToken, state.loginEmail, "autopilot").catch(() => {});
    }
  });

  // Refresco de live stats del autopilot cada 10s
  const refreshAutopilotLiveStats = async () => {
    try {
      const res = await fetch(
        `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=eq.auto_session_stats&select=value`,
        { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
      );
      if (!res.ok) return;
      const rows = await res.json();
      const raw  = rows?.[0]?.value;
      if (!raw) { document.getElementById("autopilot-live-stats").style.display = "none"; return; }
      let stats; try { stats = JSON.parse(raw); } catch { return; }
      const ageSec = stats.lastUpdate ? Math.round((Date.now() - stats.lastUpdate) / 1000) : Infinity;
      // Mostrar solo si hay update en los últimos 5 minutos (sesión "activa")
      const wrap = document.getElementById("autopilot-live-stats");
      if (!wrap) return;
      if (ageSec > 300) { wrap.style.display = "none"; return; }
      wrap.style.display = "block";
      document.getElementById("ap-live-user").textContent      = stats.sessionUser || "?";
      document.getElementById("ap-live-processed").textContent = (stats.processed || 0).toLocaleString();
      document.getElementById("ap-live-added").textContent     = (stats.added || 0).toLocaleString();
      document.getElementById("ap-live-filtered").textContent  = (stats.filtered || 0).toLocaleString();
      document.getElementById("ap-live-age").textContent       = `hace ${ageSec}s`;
      document.getElementById("ap-live-last").textContent      = stats.lastDomain ? `Último: ${stats.lastDomain}` : "";
    } catch {}
  };
  refreshAutopilotLiveStats();
  setInterval(refreshAutopilotLiveStats, 10_000);

  // Refresco periódico del estado mutex (cada 30s) para que cuando el dueño
  // apague desde su browser, los demás vean el lock liberado sin reload manual.
  setInterval(async () => {
    try {
      const cur = await getAutopilotState(state.accessToken);
      const isMe = cur.sessionUser?.toLowerCase() === (state.loginEmail || "").toLowerCase();
      const ageMs = cur.sessionStart ? Date.now() - cur.sessionStart.getTime() : Infinity;
      if (cur.enabled && !isMe && cur.sessionUser && ageMs < AUTOPILOT_DURATION_MS) {
        setAutopilotUI(btn, true);
        btn.disabled = true; btn.style.opacity = "0.6"; btn.style.cursor = "not-allowed";
        setAutopilotLockBadge(cur.sessionUser, ageMs);
      } else if (!cur.enabled || !cur.sessionUser) {
        if (btn.disabled) {
          btn.disabled = false; btn.style.opacity = ""; btn.style.cursor = "";
          setAutopilotUI(btn, false);
          setAutopilotLockBadge(null);
        }
      }
    } catch {}
  }, 30_000);

  targetBtn?.addEventListener("click", () => {
    if (!panel) return;
    panel.style.display = panel.style.display === "none" ? "block" : "none";
  });

  saveBtn?.addEventListener("click", async () => {
    // Multi-select geos → comma-separated string
    const geo = geoSel
      ? Array.from(geoSel.selectedOptions).map(o => o.value).filter(Boolean).join(",")
      : "";
    const cat        = catSel?.value     || "";
    const minTraffic = trafficSel?.value || "400000";
    await setAutopilotTarget(geo, cat, minTraffic, state.accessToken);
    updateTargetLabel({ geo, category: cat, minTraffic }, targetBtn, currentLbl);
    if (panel) panel.style.display = "none";
  });

  closeBtn?.addEventListener("click", () => {
    if (panel) panel.style.display = "none";
  });
}

function startAutopilotCountdown(btn, durationMs) {
  clearAutopilotTimer();
  const endsAt = Date.now() + durationMs;

  const tick = () => {
    const left = endsAt - Date.now();
    if (left <= 0) {
      clearAutopilotTimer();
      setAutopilotUI(btn, false);
      setAutopilotEnabled(false, state.accessToken);
      return;
    }
    const mins = Math.ceil(left / 60000);
    const labelEl = btn.querySelector(".autopilot-label");
    if (labelEl) labelEl.textContent = `AUTO ON ${mins}m`;
  };

  tick();
  _autopilotTimer = setInterval(tick, 30000); // actualiza cada 30s
}

function clearAutopilotTimer() {
  if (_autopilotTimer) { clearInterval(_autopilotTimer); _autopilotTimer = null; }
}

function updateTargetLabel(target, targetBtn, currentLbl) {
  const trafficNum = parseInt(target.minTraffic || "400000");
  const trafficLbl = trafficNum >= 10000001 ? "+10M+" : trafficNum >= 1000000 ? `+${trafficNum/1000000}M` : `+${trafficNum/1000}K`;
  // Multi-GEO: shorten when many
  const geos = (target.geo || "").split(",").map(s => s.trim()).filter(Boolean);
  const geoLbl = geos.length === 0 ? "" : geos.length <= 2 ? geos.join("+") : `${geos.length} geos`;
  const parts = [geoLbl, target.category, trafficLbl !== "+400K" ? trafficLbl : ""].filter(Boolean);
  const label = parts.length ? parts.join(" · ") : "All";
  if (targetBtn) targetBtn.title = `Target: ${label}`;
  if (currentLbl) currentLbl.textContent = `Active target: ${label}`;
  if (targetBtn) targetBtn.style.color = (geos.length || target.category) ? "var(--primary)" : "";
}

function setAutopilotLockBadge(ownerEmail, sessionAgeMs) {
  // Crea/actualiza un cartel "🔒 Locked by X" al lado del btn-autopilot.
  // Pasar ownerEmail=null para removerlo.
  let badge = document.getElementById("autopilot-lock-badge");
  if (!ownerEmail) { badge?.remove(); return; }
  const remaining = Math.max(0, Math.round((AUTOPILOT_DURATION_MS - sessionAgeMs) / 60000));
  const ownerShort = ownerEmail.split("@")[0];
  if (!badge) {
    badge = document.createElement("span");
    badge.id = "autopilot-lock-badge";
    badge.style.cssText = `
      display: inline-flex; align-items: center; gap: 4px;
      background: rgba(239,68,68,0.15); color: #fca5a5;
      border: 1px solid rgba(239,68,68,0.3); border-radius: 6px;
      padding: 3px 8px; font-size: 10px; font-weight: 700;
      margin-left: 6px;
    `;
    document.getElementById("btn-autopilot")?.parentElement?.appendChild(badge);
  }
  badge.innerHTML = `🔒 Locked by <strong>${esc(ownerShort)}</strong> · ~${remaining}m`;
  badge.title = `Autopilot corriendo bajo ${ownerEmail}. Solo ese usuario puede apagarlo.`;
}

function setAutopilotUI(btn, enabled) {
  btn.classList.toggle("active", enabled);
  const labelEl = btn.querySelector(".autopilot-label");
  if (labelEl) labelEl.textContent = enabled ? "AUTO ON" : "AUTO OFF";
  btn.title = enabled
    ? "Autopilot ON — runs 1h then shuts off. Click to turn off now."
    : "Autopilot OFF — click to activate (1h)";
}

async function logout() {
  await chrome.storage.local.remove("auth");
  window.location.reload();
}

// ============================================================
// PROSPECTS TAB — Review queue del auto-prospector
// ============================================================

const LANG_TO_IDX    = { en: "0", es: "1", it: "2", pt: "3", ar: "6" };
const LANG_NAMES_PRO = { en: "Ingles", es: "Español", it: "Italiano", pt: "Portugues", ar: "Arabe", fr: "Frances", de: "Aleman" };

function defaultOwnerForLang(lang) {
  if (lang === "es" || lang === "pt") return "Agus";
  if (lang === "en")                  return "Max";
  return "Diego";
}

function defaultStatusForOwner(_owner) {
  return "4"; // Propuesta Vigente (T) — default para envíos/updates desde la toolbar
}

let _cachedProspectDrafts = []; // cache de borradores para los dropdowns de las cards

// Empty state inteligente: lee toolbar_config.auto_session_stats + flags +
// toolbar_csv_queue.pending para decidir qué mostrar al user.
async function renderProspectsEmptyState(listEl) {
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };

  const [statsRes, flagsRes, csvCountRes] = await Promise.all([
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=in.(auto_session_stats,auto_prospecting_enabled,csv_queue_enabled,auto_session_user)&select=key,value`, { headers }),
    Promise.resolve(null), // placeholder
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=in.(pending,processing)&select=domain,status,source&limit=5&order=updated_at.desc.nullslast`, { headers }),
  ]);

  const cfgMap = {};
  if (statsRes.ok) {
    const rows = await statsRes.json();
    rows.forEach(r => { cfgMap[r.key] = r.value; });
  }
  const csvItems = csvCountRes.ok ? await csvCountRes.json() : [];

  const autopilotOn = cfgMap.auto_prospecting_enabled === "true";
  const csvQueueOn  = cfgMap.csv_queue_enabled === "true";
  const apUser      = cfgMap.auto_session_user || "";
  let apStats = null;
  try { apStats = JSON.parse(cfgMap.auto_session_stats || "null"); } catch {}
  const apActive = apStats && apStats.lastUpdate && (Date.now() - apStats.lastUpdate < 5 * 60 * 1000);

  const csvProcessing = csvItems.find(i => i.status === "processing");
  const csvPendingCount = csvItems.filter(i => i.status === "pending").length;

  // Case 1: Autopilot active and processing
  let html = "";
  if (autopilotOn && apActive) {
    html += `
      <div class="cascade-empty" style="background:#dbeafe;border:1px solid #3b82f6;color:#1e3a8a;padding:12px;border-radius:8px;margin-bottom:8px;text-align:left">
        <div style="font-weight:700;margin-bottom:4px">🤖 Autopilot processing · ${esc(apUser || "?")}</div>
        <div style="font-size:11px;line-height:1.5">
          Last analyzed: <strong>${esc(apStats?.lastDomain || "—")}</strong><br/>
          📊 ${apStats?.processed || 0} processed · ✅ ${apStats?.added || 0} added · ⚠️ ${apStats?.filtered || 0} filtered (sub-threshold or GEO mismatch)
        </div>
      </div>
    `;
  }

  // Case 2: CSV/Monday URL queue active with items
  if (csvQueueOn && (csvProcessing || csvPendingCount > 0)) {
    const sourceLabel = csvProcessing?.source === "monday" ? "Monday URL" : "External CSV";
    html += `
      <div class="cascade-empty" style="background:#fef3c7;border:1px solid #f59e0b;color:#78350f;padding:12px;border-radius:8px;margin-bottom:8px;text-align:left">
        <div style="font-weight:700;margin-bottom:4px">📥 Auto Import processing · ${esc(sourceLabel)}</div>
        <div style="font-size:11px;line-height:1.5">
          ${csvProcessing ? `Currently processing: <strong>${esc(csvProcessing.domain)}</strong><br/>` : ""}
          ⏳ ${csvPendingCount} pending in queue
        </div>
      </div>
    `;
  }

  // Case 3: nothing active
  if (!html) {
    if (!autopilotOn && !csvQueueOn) {
      html = '<div class="cascade-empty">No activity. Turn on Autopilot or upload a CSV / Monday URL to get started.</div>';
    } else if (autopilotOn && !apActive) {
      html = `<div class="cascade-empty">🤖 Autopilot ON (${esc(apUser || "?")}), but the worker hasn't reported activity in the last 5 minutes. Check Railway logs if this persists.</div>`;
    } else if (csvQueueOn && csvPendingCount === 0) {
      html = '<div class="cascade-empty" style="background:#d1fae5;border:1px solid #10b981;color:#064e3b;padding:12px;border-radius:8px;text-align:left"><div style="font-weight:700;margin-bottom:4px">✅ Work finished</div><div style="font-size:11px">All imports processed. Pending prospects went through filtering. Upload a new CSV or Monday URL refresh to add more.</div></div>';
    } else {
      html = '<div class="cascade-empty">No pending prospects.</div>';
    }
  }

  listEl.innerHTML = html;
}

async function loadProspectsTab() {
  const listEl  = document.getElementById("prospects-list");
  const statsEl = document.getElementById("prospects-stats");
  if (!listEl) return;

  listEl.innerHTML = '<div class="cascade-empty">⏳ Loading...</div>';

  const dateFilter   = document.getElementById("prospects-date-filter")?.value   || "";
  const sourceFilter = document.getElementById("prospects-source-filter")?.value || "";
  const userFilter   = document.getElementById("prospects-user-filter")?.value   || "";
  let rows = [];
  let dailyCount = 0;
  try {
    [rows, dailyCount, _cachedProspectDrafts] = await Promise.all([
      fetchReviewQueue(state.accessToken, { dateFilter, sourceFilter, userFilter }),
      getDailyValidationCount(state.accessToken, state.loginEmail),
      getPitchDrafts(state.accessToken, state.loginEmail),
    ]);
    // Sincronizar la cache global de drafts (la usa la bandera+autocarga de cada card)
    _draftsState.all = _cachedProspectDrafts;
    _rebuildDraftsByLang();
    _draftsState.loaded = true;
  } catch (err) {
    listEl.innerHTML = `<div class="cascade-empty" style="color:#e53e3e">❌ Error loading prospects: ${esc(err.message || String(err))}</div>`;
    return;
  }

  updateProspectsDailyBar(dailyCount);
  if (statsEl) statsEl.textContent = rows.length ? `${rows.length} pending candidates` : "No pending candidates";

  if (!rows.length) {
    // Empty state inteligente: si hay actividad activa del worker, mostrar qué
    // está procesando; si todo está apagado, mostrar el mensaje genérico.
    listEl.innerHTML = '<div class="cascade-empty">⏳ Verificando actividad del worker...</div>';
    renderProspectsEmptyState(listEl).catch(() => {
      listEl.innerHTML = '<div class="cascade-empty">No pending prospects.</div>';
    });
    return;
  }

  listEl.innerHTML = rows.map(r => renderProspectCard(r)).join("");

  listEl.querySelectorAll(".pcard").forEach(card => {
    const id   = parseInt(card.dataset.id);
    const data = rows.find(r => r.id === id);
    if (data) initProspectCard(card, data);
  });
}

function updateProspectsDailyBar(count) {
  const countEl = document.getElementById("prospects-daily-count");
  const fillEl  = document.getElementById("prospects-daily-fill");
  if (countEl) countEl.textContent = count;
  if (fillEl)  fillEl.style.width  = Math.min(100, (count / 50) * 100) + "%";
  if (fillEl)  fillEl.style.background = count >= 50 ? "#e53e3e" : count >= 40 ? "#d97706" : "#3b82f6";
}

function renderProspectCard(r) {
  const trafficFmt  = r.traffic ? formatTraffic(r.traffic) : "Sin data";
  // Resolver idioma sincronicamente al render — evita que aparezca "🌐 —"
  // mientras carga el cache de drafts. Default a EN si no hay matcheo.
  const _initLang = (() => {
    // 1) language ya viene del worker
    const dl = (r.language || "").toLowerCase().split("-")[0];
    if (LANG_FLAG[dl]) return dl;
    // 2) geo como código ISO (AR/MX/BR) — caso más común desde el worker
    const geoUp = (r.geo || "").trim().toUpperCase();
    if (GEO_TO_LANG[geoUp] && LANG_FLAG[GEO_TO_LANG[geoUp]]) return GEO_TO_LANG[geoUp];
    // 3) geo como label ("Argentina", "México")
    const geoText = (r.geo || "").trim().toLowerCase();
    for (const [code, label] of Object.entries(GEO_LABEL)) {
      if (label.toLowerCase() === geoText) {
        const lang = GEO_TO_LANG[code];
        if (LANG_FLAG[lang]) return lang;
      }
    }
    // 4) TLD del dominio (.com.ar / .mx / .br / .it / .pt) — fallback obvio
    const tldLang = detectLangFromDomain(r.domain || "");
    if (LANG_FLAG[tldLang]) return tldLang;
    // 5) default
    return "en";
  })();
  // Pre-render de los 5 chips de banderas — visibles desde el primer paint
  const _langFlagsHTML = ["es", "en", "it", "pt", "ar"].map(l => {
    const isActive = l === _initLang;
    return `<button type="button" class="pitch-lang-flag${isActive ? " active" : ""}" data-lang="${l}" title="${l.toUpperCase()}">${LANG_FLAG[l]}</button>`;
  }).join("");
  // Filtrar garbage (whois, abuse, postmaster, etc.) y dedupe.
  // Backend ya guarda Apollo primero (apolloEmails antes que scraperEmails),
  // así que el orden del array preserva la prioridad Apollo.
  const emails      = (Array.isArray(r.emails) ? r.emails : [])
    .map(e => (e || "").trim())
    .filter(Boolean)
    .filter(e => !isGarbageEmail(e))
    .filter((e, i, arr) => arr.indexOf(e) === i);
  const hasEmail    = emails.length > 0;
  // Owner = SIEMPRE el usuario logueado (mediaBuyer). Antes usaba
  // defaultOwnerForLang(r.language) que asignaba según idioma del prospect,
  // ignorando quién está realmente trabajando el lead.
  const owner       = state.mediaBuyer || defaultOwnerForLang(r.language);
  const status      = defaultStatusForOwner(owner);
  // langIdx para el select de Monday — usa _initLang (con fallback TLD/geo)
  // en lugar de r.language crudo, que muchas veces viene vacío del worker.
  const langIdx     = LANG_TO_IDX[_initLang] || LANG_TO_IDX[r.language] || "0";
  const langName    = LANG_NAMES_PRO[_initLang] || LANG_NAMES_PRO[r.language] || r.language || "—";
  const adNetworks  = Array.isArray(r.ad_networks) ? r.ad_networks : [];
  const subjects    = Array.isArray(r.pitch_subjects) ? r.pitch_subjects : [];

  // Score badge color
  const score = r.score || 0;
  const scoreBg = score >= 60 ? "#16a34a" : score >= 35 ? "#d97706" : "#6b7280";
  const scoreBadge = score > 0
    ? `<span style="font-size:10px;font-weight:700;color:#fff;background:${scoreBg};border-radius:4px;padding:1px 5px;flex-shrink:0">${score}</span>`
    : "";

  // Lead temperature indicator — señal at-a-glance del potencial.
  // 🔥 HOT  : traffic >= 1M OR (score >= 60 AND email)
  // ☀️ WARM : traffic >= 400K AND email AND (adNetworks OR score >= 40)
  // ❄️ COLD : todo lo demás (sin email + low traffic)
  const _trafficN = parseInt(r.traffic || 0, 10);
  const _hasEmail = hasEmail;
  const _hasAdNet = adNetworks.length > 0;
  let tempBadge = "";
  if (_trafficN >= 1_000_000 || (score >= 60 && _hasEmail)) {
    tempBadge = `<span title="🔥 HOT lead — alta probabilidad de cierre" style="font-size:11px;flex-shrink:0">🔥</span>`;
  } else if (_trafficN >= 400_000 && _hasEmail && (_hasAdNet || score >= 40)) {
    tempBadge = `<span title="☀️ WARM lead — vale el outreach" style="font-size:11px;flex-shrink:0">☀️</span>`;
  } else if (!_hasEmail && _trafficN < 400_000) {
    tempBadge = `<span title="❄️ COLD lead — sin email y bajo tráfico, evaluá si vale el esfuerzo" style="font-size:11px;flex-shrink:0;opacity:0.7">❄️</span>`;
  }

  const emailOptions = emails.map((e, i) => `
    <label style="display:flex;align-items:center;gap:5px;font-size:11px;cursor:pointer;margin-bottom:3px">
      <input type="radio" name="email_${r.id}" value="${esc(e)}" ${i === 0 ? "checked" : ""} class="pcard-email-radio" />
      ${esc(e)}
    </label>`).join("");

  const ownerOptions = ["Agus", "Diego", "Max"].map(o =>
    `<option value="${o}" ${o === owner ? "selected" : ""}>${o}</option>`).join("");

  const statusOptions = [
    ["1","En Negociacion"],["9","Masivo - Agus"],["6","Masivo - Diego"],["10","Masivo - Max"],
    ["8","Mail No Enviado"],["3","Propuesta Vigente"],["4","Propuesta Vigente (T)"],["5","Ciclo Finalizado"]
  ].map(([v,l]) => `<option value="${v}" ${v === status ? "selected" : ""}>${l}</option>`).join("");

  const langOptions = [
    ["0","English"],["1","Spanish"],["2","Italian"],["3","Portuguese"],["6","Arabic"]
  ].map(([v,l]) => `<option value="${v}" ${v === langIdx ? "selected" : ""}>${l}</option>`).join("");

  // Subject chips (3 variants from Gemini)
  const subjectChips = subjects.length > 0
    ? `<div class="pcard-subject-chips" style="display:flex;flex-direction:column;gap:3px;margin-bottom:6px">
        ${subjects.map((s, i) => `
          <button class="pcard-subject-chip" data-subject="${esc(s)}"
            style="text-align:left;font-size:10px;padding:3px 7px;border-radius:4px;border:1px solid var(--border);cursor:pointer;background:${i===0?"var(--primary)":"transparent"};color:${i===0?"#fff":"var(--text)"};line-height:1.3">
            ${esc(s)}
          </button>`).join("")}
       </div>`
    : "";

  // Ad networks row
  const adNetRow = adNetworks.length > 0
    ? `<span style="font-size:10px;color:#7c3aed">📡 ${esc(adNetworks.slice(0,3).join(", "))}</span>`
    : "";

  // Page title as subtitle
  const titleRow = r.page_title
    ? `<div style="font-size:10px;color:var(--text-muted);font-style:italic;margin-top:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.page_title)}">${esc(r.page_title)}</div>`
    : "";

  return `
  <div class="pcard" data-id="${r.id}" data-domain="${esc(r.domain)}" data-source="${esc(r.source || 'autopilot')}" data-monday-id="${r.monday_item_id || ''}" style="background:var(--card-bg);border:1px solid var(--border);border-radius:8px;margin:0 8px 8px;overflow:hidden">

    <!-- Summary row -->
    <div style="display:flex;align-items:center;gap:6px;padding:8px 10px">
      <input type="checkbox" class="pcard-bulk-cbx" data-id="${r.id}" title="Seleccionar para bulk action" style="margin:0;cursor:pointer;flex-shrink:0" />
      <div style="flex:1;min-width:0">
        <div style="display:flex;align-items:center;gap:5px">
          ${(() => {
            // Source badge minimal: solo el icono en cuadradito chico, libera espacio
            // para el nombre del MB y la URL del dominio. El label completo queda en title.
            const src = r.source || "autopilot";
            const badges = {
              autopilot:      ["🤖", "Auto",            "#6366f1"],
              csv:            ["📥", "CSV import",      "#0ea5e9"],
              monday_refresh: ["🔄", "Monday refresh",  "#f59e0b"],
              sellers_json:   ["📋", "sellers.json",    "#8b5cf6"],
            };
            const [icon, label, color] = badges[src] || badges.autopilot;
            return `<span title="Origen: ${label}" style="font-size:11px;background:${color};border-radius:3px;padding:1px 4px;line-height:1;flex-shrink:0;display:inline-flex;align-items:center;justify-content:center;width:18px;height:16px">${icon}</span>`;
          })()}
          ${(() => {
            // Badge del usuario que generó el item (autopilot o import). Mapea
            // email a nombre corto: Maxi / Diego / Agus. Si no matchea, usa
            // la parte antes del @.
            const email = (r.created_by || "").toLowerCase();
            if (!email) return "";
            const userMap = {
              "mgargiulo@adeqmedia.com": ["Maxi",  "#10b981"],
              "dhorovitz@adeqmedia.com": ["Diego", "#a855f7"],
              "sales@adeqmedia.com":     ["Agus",  "#ec4899"],
            };
            const [name, color] = userMap[email] || [email.split("@")[0], "#64748b"];
            return `<span title="Origen del item: ${esc(email)}" style="font-size:9px;font-weight:700;color:#fff;background:${color};border-radius:4px;padding:1px 5px;flex-shrink:0">👤 ${esc(name)}</span>`;
          })()}
          <a class="pcard-domain-link" href="#" data-url="https://www.${esc(r.domain)}"
             style="font-weight:700;font-size:12px;color:var(--primary);text-decoration:none;word-break:break-all;line-height:1.3"
             title="${esc(r.domain)}">
            ${esc(r.domain)} ↗
          </a>
          ${scoreBadge}
          ${tempBadge}
        </div>
        ${titleRow}
        <div style="font-size:10px;color:var(--text-muted);margin-top:3px;display:flex;flex-wrap:wrap;gap:4px">
          <span>📊 ${trafficFmt}</span>
          ${r.geo      ? `<span>🌎 ${esc(r.geo)}</span>`      : ""}
          ${r.language ? `<span>🗣 ${esc(langName)}</span>`    : ""}
          ${r.category ? `<span>📁 ${esc(r.category)}</span>` : ""}
          ${r.contact_name ? `<span>👤 ${esc(r.contact_name)}</span>` : ""}
          ${hasEmail ? `<span style="color:#3b82f6">✉️ ${emails.length}</span>` : '<span style="color:#e53e3e">✉️ —</span>'}
          ${adNetRow}
        </div>
      </div>
      <div style="display:flex;gap:3px;flex-shrink:0">
        <button class="btn btn-secondary btn-sm pcard-enrich-btn" title="Buscar tráfico + emails frescos (Apollo + scraper)" style="padding:3px 7px;font-size:10px" ${(r.traffic && hasEmail) ? "style='display:none'" : ""}>🔍 Data</button>
        <button class="btn btn-secondary btn-sm pcard-expand-btn" title="Expandir para revisar datos, email y pitch antes de enviar" style="padding:3px 7px">▼ Revisar</button>
        <button class="btn btn-sm pcard-reject-btn" title="❌ Descartar — no sirve, no volver a procesar" style="padding:3px 7px;color:#e53e3e;background:transparent;border:1px solid var(--border)">❌</button>
      </div>
    </div>

    <!-- Expandable detail panel -->
    <div class="pcard-detail" style="display:none;border-top:1px solid var(--border);padding:10px">

      <!-- Email selection — autoverify + Apollo first + ver más toggle (mismo sistema que Analysis) -->
      <div style="font-size:11px;font-weight:700;color:var(--text-muted);margin-bottom:5px;text-transform:uppercase;letter-spacing:.5px">Select email to send</div>
      <div class="pcard-email-list email-list">
        ${hasEmail ? "" : '<div style="font-size:11px;color:#e53e3e;margin-bottom:4px">No emails — escribilo manualmente abajo</div>'}
      </div>
      <input type="text" class="form-input pcard-email-manual" placeholder="Enter email manually..." style="margin-top:4px;font-size:11px;padding:4px 7px" />

      <!-- Pitch — replica EXACTA del bloque del tab Analysis para que el user
           se acostumbre a una sola visual. Reusa las mismas CSS classes
           (.pitch-controls, .pitch-pills, .pitch-textarea, .pitch-actions). -->
      <div class="sub-section" style="padding:0;margin-top:10px">
        <div class="sub-title-row">
          <span class="sub-title">🤖 Pitch with Claude</span>
          <select class="category-select pcard-pitch-language" title="Email language" style="font-size:11px;padding:2px 4px">
            <option value="en" ${_initLang === "en" ? "selected" : ""}>English</option>
            <option value="es" ${_initLang === "es" ? "selected" : ""}>Spanish</option>
            <option value="it" ${_initLang === "it" ? "selected" : ""}>Italian</option>
            <option value="pt" ${_initLang === "pt" ? "selected" : ""}>Portuguese</option>
            <option value="ar" ${_initLang === "ar" ? "selected" : ""}>Arabic</option>
          </select>
          <select class="category-select pcard-pitch-category" title="Site category" style="font-size:11px;padding:2px 4px">
            <option value="">Auto category</option>
            <option value="sports">Sports</option>
            <option value="news">News &amp; Media</option>
            <option value="finance">Finance</option>
            <option value="technology">Technology</option>
            <option value="entertainment">Entertainment</option>
            <option value="health">Health</option>
            <option value="travel">Travel</option>
            <option value="gambling">Gambling</option>
            <option value="automotive">Automotive</option>
            <option value="food">Food &amp; Drink</option>
            <option value="realestate">Real Estate</option>
            <option value="business">Business / B2B</option>
          </select>
        </div>

        <!-- Style compacto: pills ciclan al click (1 fila) -->
        <div class="pitch-style-row">
          <div class="pitch-cycle-pills pcard-pitch-pills" data-group="tone">
            <button class="pitch-pill active" data-val="informal" type="button">💬 Informal</button>
            <button class="pitch-pill" data-val="formal" type="button" hidden>💬 Formal</button>
          </div>
          <div class="pitch-cycle-pills pcard-pitch-pills" data-group="length">
            <button class="pitch-pill active" data-val="short" type="button">📏 Short</button>
            <button class="pitch-pill" data-val="long" type="button" hidden>📏 Long</button>
          </div>
          <div class="pitch-cycle-pills pcard-pitch-pills" data-group="focus">
            <button class="pitch-pill active" data-val="analysis" type="button">📊 Analysis</button>
            <button class="pitch-pill" data-val="nodataanalysis" type="button" hidden>📊 No analysis</button>
          </div>
          <div class="pitch-cycle-pills pcard-pitch-pills" data-group="opening">
            <button class="pitch-pill active" data-val="direct" type="button">🚀 Direct</button>
            <button class="pitch-pill" data-val="problem" type="button" hidden>🚀 Problem</button>
            <button class="pitch-pill" data-val="praise" type="button" hidden>🚀 Praise</button>
          </div>
        </div>

        <!-- Asunto separado visualmente (igual que Analysis) -->
        <div class="pitch-subject-row">
          <label class="pitch-subject-label">📨 Asunto</label>
          <input type="text" class="form-input pitch-subject-input pcard-subject" value="${esc(subjects[0] || r.pitch_subject || "")}" placeholder="Asunto del email..." />
        </div>
        <div class="pcard-chips-area">${r.pitch ? subjectChips : ""}</div>

        <!-- Toolbar idéntica a Analysis: bandera+rotator | chips de idiomas + Limpiar -->
        <div class="pitch-toolbar">
          <button type="button" class="pcard-flag-btn pitch-tool-btn pitch-tool-flag" title="Click para rotar templates del idioma">
            <span class="pcard-flag-emoji">${LANG_FLAG[_initLang] || "🇬🇧"}</span>
            <span class="pcard-flag-name pitch-tool-name">cargando…</span>
          </button>
          <div class="pitch-toolbar-right">
            <div class="pcard-lang-flags pitch-lang-flags" title="Cambiar idioma del template">${_langFlagsHTML}</div>
            <button type="button" class="pcard-clear-btn pitch-tool-btn pitch-tool-clear" title="Limpiar pitch">🗑️ Limpiar</button>
          </div>
        </div>
        <textarea class="pitch-textarea pcard-pitch" rows="5" placeholder="Borrador del idioma se autocarga acá...">${esc(r.pitch || "")}</textarea>

        <div class="pitch-actions" style="margin-top:6px">
          <button class="btn btn-primary btn-sm pcard-generate-btn" type="button">${r.pitch ? "✨ Regenerate Pitch" : "✨ Generate Pitch"}</button>
        </div>
        ${adNetworks.length > 0 ? `<div style="font-size:10px;color:#7c3aed;margin-top:4px">📡 Ad networks: ${esc(adNetworks.join(", "))}</div>` : ""}
        <div class="pcard-generate-result" style="font-size:10px;color:#e53e3e;margin-top:3px"></div>
      </div>

      <!-- Monday fields -->
      <div style="font-size:11px;font-weight:700;color:var(--text-muted);margin:10px 0 6px;text-transform:uppercase;letter-spacing:.5px">Monday data</div>
      <div class="form-grid">
        <label class="form-label">Owner</label>
        <select class="form-select pcard-owner">${ownerOptions}</select>
        <label class="form-label">Status</label>
        <select class="form-select pcard-status">${statusOptions}</select>
        <label class="form-label">Language</label>
        <select class="form-select pcard-lang">${langOptions}</select>
        <label class="form-label">GEO</label>
        <input type="text" class="form-input pcard-geo" value="${esc(r.geo || "")}" placeholder="e.g. Mexico" style="font-size:11px;padding:4px 7px" />
        <label class="form-label">Email</label>
        <input type="text" class="form-input pcard-email-monday" value="${esc(emails[0] || "")}" placeholder="Email a Monday" style="font-size:11px;padding:4px 7px" title="Auto-completado con el email seleccionado arriba — editable" />
        <label class="form-label">Date</label>
        <input type="text" class="form-input pcard-date" value="${toDisplayDate(new Date().toISOString().split("T")[0])}" placeholder="DD/MM/YYYY" maxlength="10" style="font-size:11px;padding:4px 7px" title="Auto-completada con hoy — editable" />
        <label class="form-label">Traffic</label>
        <input type="text" class="form-input pcard-traffic" value="${esc(r.traffic ? formatTraffic(r.traffic) : "")}" placeholder="ej: 500K, 1.2M, 3500000" style="font-size:11px;padding:4px 7px" title="Editable — si SimilarWeb no devolvió, completá manualmente. Acepta '500K', '1.2M' o número crudo." />
      </div>

      <!-- Action buttons in panel -->
      <div style="display:flex;gap:6px;margin-top:10px">
        <button class="btn btn-success btn-sm pcard-validate-expanded" style="flex:1">✅ Push + Send Email</button>
      </div>
      <div class="pcard-result" style="min-height:14px;font-size:11px;margin-top:5px;color:#16a34a"></div>
    </div>

  </div>`;
}

function initProspectCard(card, data) {
  const id = data.id;

  // Re-computar emails filtrados (mismo filtro que renderProspectCard).
  // OJO: antes usábamos `emails` del closure de renderProspectCard → ReferenceError
  // que mataba TODOS los handlers de la card (expand, validate, reject).
  const emails = (Array.isArray(data.emails) ? data.emails : [])
    .map(e => (e || "").trim())
    .filter(Boolean)
    .filter(e => !isGarbageEmail(e))
    .filter((e, i, arr) => arr.indexOf(e) === i);

  // Domain link → open tab
  card.querySelector(".pcard-domain-link")?.addEventListener("click", e => {
    e.preventDefault();
    chrome.tabs.create({ url: e.currentTarget.dataset.url, active: false });
  });

  // Enriquecer: tráfico fresco + Apollo si faltan datos
  card.querySelector(".pcard-enrich-btn")?.addEventListener("click", async (e) => {
    const btn = e.currentTarget;
    // Pre-check: si la card YA tiene tráfico Y emails, no gastar hits
    const hasTrafficAlready = !!(data.traffic && data.traffic > 0);
    const hasEmailsAlready  = Array.isArray(data.emails) && data.emails.filter(em => em && !isGarbageEmail(em)).length > 0;
    if (hasTrafficAlready && hasEmailsAlready) {
      btn.disabled = true;
      btn.style.background = "#f1f5f9";
      btn.style.color = "#64748b";
      btn.textContent = "✅ Ya tiene datos";
      btn.title = `Tráfico: ${data.traffic.toLocaleString()} · ${data.emails.length} email(s). Ya tiene todo lo importante — no se gasta hit.`;
      setTimeout(() => {
        btn.disabled = false;
        btn.style.background = "";
        btn.style.color = "";
        btn.textContent = "🔍 Data";
      }, 2500);
      return;
    }
    btn.disabled = true; btn.textContent = "⏳ ...";
    try {
      // Optimización: si ya tiene tráfico, NO llamar getTraffic. Si ya tiene
      // emails, NO llamar Apollo. Solo disparamos lo que falta.
      const [traffic, apollo] = await Promise.all([
        hasTrafficAlready ? Promise.resolve(null) : getTraffic(data.domain).catch(() => null),
        hasEmailsAlready  ? Promise.resolve(null) : findDecisionMakerViaApollo(data.domain).catch(() => null),
      ]);
      // Update local state + UI
      let updated = false;
      if (traffic && (traffic.pageViews || traffic.rawVisits)) {
        const newTraffic = traffic.pageViews || traffic.rawVisits;
        data.traffic = newTraffic;
        const trafficSpan = card.querySelector(".pcard-summary-row span");
        // Re-render entera la card con la data actualizada (más simple que parchear DOM)
        updated = true;
      }
      if (apollo?.email && !data.emails?.includes(apollo.email)) {
        data.emails = [apollo.email, ...(data.emails || [])];
        if (apollo.first_name) data.contact_name = `${apollo.first_name} ${apollo.last_name || ""}`.trim();
        updated = true;
      }
      if (updated) {
        // Re-render esta card en su lugar
        const newHtml = renderProspectCard(data);
        const tmp = document.createElement("div");
        tmp.innerHTML = newHtml;
        const newCard = tmp.firstElementChild;
        card.replaceWith(newCard);
        initProspectCard(newCard, data);
      } else {
        btn.textContent = "Sin más data";
        setTimeout(() => { btn.disabled = false; btn.textContent = "🔍 Data"; }, 2000);
      }
    } catch (err) {
      console.warn("[pcard enrich]", err);
      btn.disabled = false; btn.textContent = "🔍 Data";
    }
  });

  // Expand toggle — al abrir, lock del prospect 30 min para que otros MBs no lo
  // toquen al mismo tiempo. Se libera al cerrar la card o al cerrar la toolbar.
  card.querySelector(".pcard-expand-btn")?.addEventListener("click", async () => {
    const panel = card.querySelector(".pcard-detail");
    const btn   = card.querySelector(".pcard-expand-btn");
    const open  = panel.style.display === "none";
    panel.style.display = open ? "block" : "none";
    btn.textContent     = open ? "▲" : "▼";
    if (open && data.domain) {
      // Verificar si otro MB ya lo lockeó
      const lock = await getActiveProspectLock(state.accessToken, data.domain);
      if (lock && lock.locked_by.toLowerCase() !== (state.loginEmail || "").toLowerCase()) {
        const minLeft = Math.max(0, Math.round((new Date(lock.expires_at) - Date.now()) / 60000));
        showToast(`🔒 ${lock.locked_by} está revisando este prospecto (${minLeft} min). Coordiná antes de pushear.`, "warn", 6000);
      } else {
        lockProspect(state.accessToken, data.domain, state.loginEmail).catch(() => {});
      }
      // Auto-fetch tráfico si la card no lo tiene — usa cache 90d → 0 hits
      // si ya fue analizado por cualquier MB. Solo gasta hit si es dominio fresh.
      if (!data.traffic && !card.dataset._trafficFetched) {
        card.dataset._trafficFetched = "1";
        const trafficInput = card.querySelector(".pcard-traffic");
        if (trafficInput && !trafficInput.value) {
          trafficInput.placeholder = "⏳ Buscando tráfico…";
          getTraffic(data.domain).then(t => {
            const v = t?.pageViews || t?.rawVisits || 0;
            if (v > 0) {
              data.traffic = v;
              trafficInput.value = formatTraffic(v);
              trafficInput.placeholder = "ej: 500K, 1.2M, 3500000";
            } else {
              trafficInput.placeholder = "Sin data — completá manual";
            }
          }).catch(() => {
            trafficInput.placeholder = "Error — completá manual";
          });
        }
      }
    } else if (!open && data.domain) {
      // Al cerrar, liberar el lock SI somos el dueño
      unlockProspect(state.accessToken, data.domain, state.loginEmail).catch(() => {});
    }
  });

  // Draft dropdown REMOVIDO — ahora hay chips de banderas + bandera-rotator (ver _updateCardUI)

  // ── Lista de emails con autoverify + Apollo first + ver más toggle ────
  // Mismo sistema que Analysis. Apollo va primero. Click sincroniza el
  // campo "Email" de Monday data abajo.
  const renderProspectEmailList = () => {
    const listEl = card.querySelector(".pcard-email-list");
    if (!listEl || emails.length === 0) return;

    // Backend ya guardó Apollo primero en r.emails (apolloEmails antes que scraperEmails),
    // pero por las dudas reordenamos por source si lo tenemos
    const sorted = emails;

    const VISIBLE = 5;
    const visible = sorted.slice(0, VISIBLE);
    const hidden  = sorted.slice(VISIBLE);

    const chipFor = (e) => {
      const cached = _emailVerifyCache.get(e);
      const cls    = cached ? _verifyClass(cached) : "verify-pending";
      return `<div class="email-chip ${cls}" data-email="${esc(e)}" title="Verificando…">${esc(e)}</div>`;
    };

    let html = visible.map(chipFor).join("");
    if (hidden.length > 0) {
      html += `<div class="email-chips-hidden" style="display:none">${hidden.map(chipFor).join("")}</div>`;
      html += `<button class="email-show-more" type="button" style="font-size:10px;background:transparent;border:none;color:#0369a1;cursor:pointer;padding:4px 0;text-decoration:underline">+ ver ${hidden.length} más…</button>`;
    }
    listEl.innerHTML = html;

    // Toggle ver más
    listEl.querySelector(".email-show-more")?.addEventListener("click", (e) => {
      const block = listEl.querySelector(".email-chips-hidden");
      if (block) { block.style.display = "block"; e.target.style.display = "none"; }
    });

    // Click chip = seleccionar + sincronizar email a Monday data
    const mondayEmailEl = card.querySelector(".pcard-email-monday");
    listEl.querySelectorAll(".email-chip").forEach(chip => {
      chip.addEventListener("click", () => {
        listEl.querySelectorAll(".email-chip").forEach(c => c.classList.remove("selected"));
        chip.classList.add("selected");
        if (mondayEmailEl) mondayEmailEl.value = chip.dataset.email;
      });
    });
    // Pre-seleccionar el primero (Apollo)
    const first = listEl.querySelector(".email-chip");
    if (first) first.classList.add("selected");

    // Auto-verify en background — pinta colores
    autoVerifyEmailChips(listEl).catch(e => console.warn("[pcard autoVerify]", e));
  };
  renderProspectEmailList();

  // ── Pitch style pills CICLAN al click (compacto, 1 fila) ──────
  try {
    card.querySelectorAll(".pcard-pitch-pills").forEach(group => {
      group.addEventListener("click", () => {
        const pills  = [...group.querySelectorAll(".pitch-pill")];
        const cur    = pills.findIndex(p => p.classList.contains("active"));
        const next   = (cur + 1) % pills.length;
        pills.forEach((p, i) => {
          p.classList.toggle("active", i === next);
          p.hidden = i !== next;
        });
      });
    });

    const langSel = card.querySelector(".pcard-pitch-language");
    const langVal = (data.language || "").split("-")[0];
    if (langSel && langVal) {
      const opt = [...langSel.options].find(o => o.value === langVal);
      if (opt) langSel.value = langVal;
    }
    const catSel = card.querySelector(".pcard-pitch-category");
    if (catSel && data.category) {
      try {
        const mapped = mapCategory(data.category);
        const opt = mapped ? [...catSel.options].find(o => o.value === mapped) : null;
        if (opt) catSel.value = mapped;
      } catch {}
    }
  } catch (e) {
    console.warn("[ProspectCard] pills init failed:", e);
  }

  // ── Bandera + trash + autocarga del draft del idioma de la card ────
  // Cascada: data.language > análisis del pitch existente > GEO > default
  const _resolveCardLang = () => {
    // 1. Idioma explícito del prospect
    const dl = (data.language || "").toLowerCase().split("-")[0];
    if (SUPPORTED_LANGS.has(dl)) return dl;
    // 2. Análisis del pitch + page_title si ya hay texto
    const sample = [data.pitch, data.page_title].filter(Boolean).join(" ");
    const detected = _detectLangFromText(sample);
    if (detected && SUPPORTED_LANGS.has(detected)) return detected;
    // 3. GEO label → alpha2 → lang
    const geoText = (data.geo || "").trim().toLowerCase();
    let geoCode = "";
    for (const [code, label] of Object.entries(GEO_LABEL)) {
      if (label.toLowerCase() === geoText) { geoCode = code; break; }
    }
    if (SUPPORTED_LANGS.has(GEO_TO_LANG[geoCode])) return GEO_TO_LANG[geoCode];
    // 4. Default
    return "en";
  };
  const cardLang = _resolveCardLang();
  // Estado local del rotator de esta card
  const cardFlag = { lang: cardLang, idx: 0 };

  const _applyCardDraft = (d) => {
    if (!d) return;
    const replaceVars = (s) => (s || "").replace(/\{\{domain\}\}/g, data.domain || "");
    const pitchEl   = card.querySelector(".pcard-pitch");
    const subjectEl = card.querySelector(".pcard-subject");
    if (pitchEl)   pitchEl.value   = replaceVars(d.body);
    if (subjectEl) subjectEl.value = replaceVars(d.subject || "");
  };

  const _cardDraftsForLang = () => (_draftsState.byLang.get(cardFlag.lang) || []);

  const _updateCardUI = () => {
    // Bandera + nombre del template
    const flagEmoji = card.querySelector(".pcard-flag-emoji");
    const nameEl    = card.querySelector(".pcard-flag-name");
    const flagBtn   = card.querySelector(".pcard-flag-btn");
    const drafts    = _cardDraftsForLang();
    if (flagEmoji && nameEl && flagBtn) {
      const flag = LANG_FLAG[cardFlag.lang];
      flagEmoji.textContent = flag || "🌐";
      if (drafts.length === 0) {
        nameEl.textContent = flag ? `Sin templates en ${cardFlag.lang.toUpperCase()}` : "Idioma desconocido";
        flagBtn.title = `No hay drafts en este idioma`;
      } else {
        const cur = drafts[cardFlag.idx];
        const cleanName = (cur?.name || `Template ${cardFlag.idx + 1}`).replace(/^[A-Z]{2}\s*·\s*/, "");
        nameEl.textContent = `${cleanName} (${cardFlag.idx + 1}/${drafts.length})`;
        flagBtn.title = `Click para rotar`;
      }
    }
    // Chips de idiomas
    const flagsEl = card.querySelector(".pcard-lang-flags");
    if (flagsEl) {
      const order = ["es", "en", "it", "pt", "ar"];
      flagsEl.innerHTML = order.map(l => {
        const flag    = LANG_FLAG[l];
        const has     = (_draftsState.byLang.get(l) || []).length > 0;
        const cls     = `pitch-lang-flag${l === cardFlag.lang ? " active" : ""}${has ? "" : " disabled"}`;
        return `<button type="button" class="${cls}" data-lang="${l}" title="${has ? "Cambiar a " + l.toUpperCase() : "Sin templates"}">${flag}</button>`;
      }).join("");
      flagsEl.querySelectorAll(".pitch-lang-flag").forEach(btn => {
        btn.addEventListener("click", () => {
          const l = btn.dataset.lang;
          const list = _draftsState.byLang.get(l) || [];
          if (list.length === 0) return;
          cardFlag.lang = l;
          cardFlag.idx  = 0;
          _applyCardDraft(list[0]);
          _updateCardUI();
        });
      });
    }
  };

  (async () => {
    if (!_draftsState.loaded) {
      try { await loadDraftsCache(); } catch {}
    }
    // Si el prospect no trae pitch, autocargar el draft del idioma detectado.
    // BUGFIX: antes usaba `r` (undefined en este scope) → throw silencioso →
    // el draft jamás se aplicaba. Ahora usa `data` que sí está definido.
    if (!data.pitch || !data.pitch.trim()) {
      const drafts = _cardDraftsForLang();
      if (drafts.length > 0) {
        cardFlag.idx = 0;
        _applyCardDraft(drafts[0]);
      } else {
        // Fallback: prio-1 de cualquier idioma
        const sorted = (_draftsState.all || []).slice().sort((a,b) => (a.priority??3)-(b.priority??3));
        const fb = sorted[0];
        if (fb) {
          cardFlag.lang = fb.language;
          cardFlag.idx  = 0;
          _applyCardDraft(fb);
        }
      }
    }
    _updateCardUI();
  })();

  // Bandera = rotar template del mismo idioma
  card.querySelector(".pcard-flag-btn")?.addEventListener("click", () => {
    const drafts = _cardDraftsForLang();
    if (drafts.length === 0) return;
    cardFlag.idx = (cardFlag.idx + 1) % drafts.length;
    _applyCardDraft(drafts[cardFlag.idx]);
    _updateCardUI();
  });

  // Trash = limpiar pitch
  card.querySelector(".pcard-clear-btn")?.addEventListener("click", () => {
    const pitchEl = card.querySelector(".pcard-pitch");
    if (pitchEl) { pitchEl.value = ""; pitchEl.focus(); }
  });

  // Subject chips — click selects subject into input
  card.querySelectorAll(".pcard-subject-chip").forEach(chip => {
    chip.addEventListener("click", () => {
      card.querySelectorAll(".pcard-subject-chip").forEach(c => {
        c.style.background = "transparent";
        c.style.color      = "var(--text)";
      });
      chip.style.background = "var(--primary)";
      chip.style.color      = "#fff";
      const subjectInput = card.querySelector(".pcard-subject");
      if (subjectInput) subjectInput.value = chip.dataset.subject;
    });
  });

  // Generate pitch button
  card.querySelector(".pcard-generate-btn")?.addEventListener("click", async () => {
    const btn       = card.querySelector(".pcard-generate-btn");
    const ta        = card.querySelector(".pcard-pitch");
    const subjInput = card.querySelector(".pcard-subject");
    const chipsArea = card.querySelector(".pcard-chips-area");
    const errEl     = card.querySelector(".pcard-generate-result");

    btn.disabled    = true;
    btn.textContent = "⏳ Generating...";
    errEl.textContent = "";

    try {
      // Config local de la card (los pills/selectores propios). Si no existen
      // (caso tabs viejas), cae al getPitchConfig global del Analysis.
      const _localPillVal = (group) => card.querySelector(`.pcard-pitch-pills[data-group="${group}"] .pitch-pill.active`)?.dataset.val || "";
      const cfg = (() => {
        const localCfg = {
          tone:    _localPillVal("tone")    || "informal",
          length:  _localPillVal("length")  || "short",
          focus:   _localPillVal("focus")   || "analysis",
          opening: _localPillVal("opening") || "direct",
        };
        return localCfg;
      })();
      const cardLanguage = card.querySelector(".pcard-pitch-language")?.value || data.language || "en";
      const cardCategory = card.querySelector(".pcard-pitch-category")?.value || data.category || "";

      // Solo RAG (Voyage) — loadFavPitches/getPitchConfig viven en otro
      // closure y no son accesibles desde acá, pero el RAG es suficiente.
      const rag = await ragRetrievePitchExamples({
        domain:   data.domain,
        category: cardCategory,
        geo:      data.geo || "",
        language: cardLanguage,
        traffic:  data.traffic  || 0,
      });
      const favExamples = [...new Set(rag.likeBodies)].slice(0, 5);
      const dislikes    = [...new Set(rag.dislikeBodies)].slice(0, 5);
      const result      = await generatePitch({
        domain:            data.domain,
        traffic:           data.traffic,
        techStack:         Array.isArray(data.ad_networks) ? data.ad_networks : [],
        adsTxt:            null,
        revenueGap:        null,
        banners:           "",
        category:          cardCategory,
        siteLanguage:      cardLanguage,
        pageTitle:         data.page_title   || "",
        pageDescription:   "",
        decisionMakerName: data.contact_name || "",
        previousPitches:   [],
        dislikes,
        favExamples,
        customPrompt:      state.customPrompt || "",
        ...cfg,
      });

      // Update textarea + subject
      ta.value        = result.body || "";
      if (result.subjects?.[0]) subjInput.value = result.subjects[0];

      // Rebuild subject chips
      if (chipsArea && result.subjects?.length > 0) {
        chipsArea.innerHTML = `<div class="pcard-subject-chips" style="display:flex;flex-direction:column;gap:3px;margin-bottom:6px">
          ${result.subjects.map((s, i) => `
            <button class="pcard-subject-chip" data-subject="${esc(s)}"
              style="text-align:left;font-size:10px;padding:3px 7px;border-radius:4px;border:1px solid var(--border);cursor:pointer;background:${i===0?"var(--primary)":"transparent"};color:${i===0?"#fff":"var(--text)"};line-height:1.3">
              ${esc(s)}
            </button>`).join("")}
        </div>`;
        chipsArea.querySelectorAll(".pcard-subject-chip").forEach(chip => {
          chip.addEventListener("click", () => {
            chipsArea.querySelectorAll(".pcard-subject-chip").forEach(c => {
              c.style.background = "transparent"; c.style.color = "var(--text)";
            });
            chip.style.background = "var(--primary)"; chip.style.color = "#fff";
            subjInput.value = chip.dataset.subject;
          });
        });
      }

      // Save back to Supabase
      await updateReviewItem(state.accessToken, data.id, {
        pitch:          result.body          || "",
        pitch_subject:  result.subjects?.[0] || "",
        pitch_subjects: result.subjects      || [],
      });

      btn.textContent = "✨ Regenerate";
    } catch (err) {
      errEl.textContent = `Error: ${err.message}`;
      btn.textContent   = "✨ Generate pitch";
    }
    btn.disabled = false;
  });

  // 👍 Like — entrena al autopilot para buscar más tipos similares
  card.querySelector(".pcard-like-btn")?.addEventListener("click", async (e) => {
    e.stopPropagation();
    const btn = e.currentTarget;
    btn.disabled = true; btn.textContent = "⏳";
    await saveAutopilotFeedback(state.accessToken, {
      user_email: state.loginEmail, domain: data.domain, action: "liked",
      category: data.category, geo: data.geo, ad_networks: data.ad_networks,
    });
    btn.textContent = "👍"; btn.style.background = "#d1fae5"; btn.style.borderColor = "#34d399";
    btn.title = "✓ Like saved — autopilot will prioritize similar patterns";
  });

  // 👎 Dislike — el autopilot aprende a evitar categoría/geo/dominio
  card.querySelector(".pcard-dislike-btn")?.addEventListener("click", async (e) => {
    e.stopPropagation();
    const btn = e.currentTarget;
    btn.disabled = true; btn.textContent = "⏳";
    await saveAutopilotFeedback(state.accessToken, {
      user_email: state.loginEmail, domain: data.domain, action: "disliked",
      category: data.category, geo: data.geo, ad_networks: data.ad_networks,
    });
    btn.textContent = "👎"; btn.style.background = "#fee2e2"; btn.style.borderColor = "#fca5a5";
    btn.title = "✓ Dislike saved — autopilot will avoid this type";
  });

  // Reject
  card.querySelector(".pcard-reject-btn")?.addEventListener("click", async () => {
    if (!confirm(`Reject and permanently block "${data.domain}"?`)) return;
    card.style.opacity = "0.4";
    // Al rechazar, también guardar dislike para el learning
    await Promise.all([
      rejectReviewItem(state.accessToken, id, data.domain),
      saveAutopilotFeedback(state.accessToken, {
        user_email: state.loginEmail, domain: data.domain, action: "disliked",
        category: data.category, geo: data.geo, ad_networks: data.ad_networks,
      }),
    ]);
    card.remove();
    refreshProspectsStats();
  });

  // Validate (compact) → uses data defaults
  card.querySelector(".pcard-validate-btn")?.addEventListener("click", async () => {
    await validateProspect(card, data, true);
  });

  // Validate (expanded)
  card.querySelector(".pcard-validate-expanded")?.addEventListener("click", async () => {
    await validateProspect(card, data, true);
  });
}

function getSelectedEmail(card) {
  // Prioridad: el INPUT MANUAL siempre gana — es el override explícito del user.
  // Después: el campo Monday (que se sincroniza con el chip seleccionado),
  // después chip seleccionado, después radio.
  const manual  = card.querySelector(".pcard-email-manual")?.value?.trim();
  if (manual && manual.includes("@")) return manual;
  const monday  = card.querySelector(".pcard-email-monday")?.value?.trim();
  if (monday && monday.includes("@")) return monday;
  const selected = card.querySelector(".pcard-email-list .email-chip.selected");
  if (selected?.dataset.email) return selected.dataset.email;
  const radio   = card.querySelector(".pcard-email-radio:checked");
  return radio?.value || "";
}

async function validateProspect(card, data, doSendEmail) {
  const resultEl = card.querySelector(".pcard-result");
  const setResult = (msg, ok = true) => {
    if (resultEl) { resultEl.textContent = msg; resultEl.style.color = ok ? "#16a34a" : "#e53e3e"; }
  };

  // Daily limit check
  const dailyCount = await getDailyValidationCount(state.accessToken, state.loginEmail);
  if (dailyCount >= 50) {
    setResult("Daily limit reached (50/day).", false);
    return;
  }

  const email     = getSelectedEmail(card);
  const pitch     = card.querySelector(".pcard-pitch")?.value?.trim()     || data.pitch || "";
  const subject   = card.querySelector(".pcard-subject")?.value?.trim()   || data.pitch_subject || "";
  const ejecutivo = card.querySelector(".pcard-owner")?.value             || defaultOwnerForLang(data.language);
  const estado    = card.querySelector(".pcard-status")?.value            || defaultStatusForOwner(ejecutivo);
  const idioma    = card.querySelector(".pcard-lang")?.value              || LANG_TO_IDX[data.language] || "0";
  const geo       = card.querySelector(".pcard-geo")?.value?.trim()       || data.geo || "";
  const dateStr   = card.querySelector(".pcard-date")?.value?.trim()      || "";

  // Tráfico EDITABLE: leer del input pcard-traffic. Acepta "500K" / "1.2M" / número crudo.
  const trafficInputRaw = card.querySelector(".pcard-traffic")?.value?.trim() || "";
  const parseTrafficInput = (s) => {
    if (!s) return 0;
    const cleaned = s.toLowerCase().replace(/[\s,.]/g, "").replace(",", "");
    const m = cleaned.match(/^([\d]+(?:\.[\d]+)?)([km])?$/);
    if (!m) return 0;
    const n = parseFloat(m[1]);
    if (m[2] === "k") return Math.round(n * 1000);
    if (m[2] === "m") return Math.round(n * 1000000);
    return Math.round(n);
  };
  const trafficNum = parseTrafficInput(trafficInputRaw) || data.traffic || 0;
  const traffic    = trafficNum ? formatTraffic(trafficNum) : "";

  // ── VALIDACIONES OBLIGATORIAS ────────────────────────────────
  // Misma lógica que Analysis (btn-push-monday). Si falta cualquier
  // dato esencial, NO se pushea a Monday.

  // 1. GEO
  if (!geo) {
    setResult("❌ GEO obligatorio. Completá el campo GEO antes de enviar.", false);
    card.querySelector(".pcard-geo")?.focus();
    return;
  }
  // 2. Tráfico (Páginas Vistas) > 0 — leído del input editable
  if (!trafficNum || trafficNum === 0) {
    setResult("❌ Páginas Vistas obligatorio. Completá el campo Traffic (acepta 500K, 1.2M o número crudo).", false);
    card.querySelector(".pcard-traffic")?.focus();
    return;
  }
  // 3. Email válido (siempre obligatorio para push, aún si no se manda mail)
  if (!email) {
    setResult("❌ Email obligatorio. Elegí uno arriba o escribilo manualmente.", false);
    card.querySelector(".pcard-email-monday")?.focus();
    return;
  }
  if (!isValidEmail(email)) {
    setResult(`❌ Email inválido: ${email}`, false);
    card.querySelector(".pcard-email-monday")?.focus();
    return;
  }
  // 4. Subject + Pitch (obligatorios para enviar mail)
  if (doSendEmail) {
    if (!subject) {
      setResult("❌ Asunto obligatorio. Completalo antes de enviar.", false);
      card.querySelector(".pcard-subject")?.focus();
      return;
    }
    if (!pitch) {
      setResult("❌ Cuerpo del email obligatorio.", false);
      card.querySelector(".pcard-pitch")?.focus();
      return;
    }
  }
  // 5. Owner / Status / Language deben estar seteados
  if (!ejecutivo)        { setResult("❌ Owner obligatorio.", false);    return; }
  if (!estado)           { setResult("❌ Status obligatorio.", false);   return; }
  if (idioma === "" || idioma == null) { setResult("❌ Language obligatorio.", false); return; }
  // 6. Date — debe ser DD/MM/YYYY parseable
  if (!dateStr) {
    setResult("❌ Date obligatorio.", false);
    card.querySelector(".pcard-date")?.focus();
    return;
  }
  const dateParts = dateStr.split("/");
  if (dateParts.length !== 3 || dateParts[2].length !== 4 || isNaN(parseInt(dateParts[0])) || isNaN(parseInt(dateParts[1]))) {
    setResult("❌ Date inválido. Formato DD/MM/YYYY.", false);
    card.querySelector(".pcard-date")?.focus();
    return;
  }

  // Disable buttons during processing
  card.querySelectorAll("button").forEach(b => { b.disabled = true; });
  setResult("⏳ Processing...", true);

  try {
    // 1. Push to Monday — UPDATE si vino de Monday Refresh (tiene monday_item_id),
    //    CREATE si es Autopilot/CSV externo (item nuevo).
    //    En AMBOS casos pisamos TODAS las columnas con la data fresca del review_queue
    //    + lo que el user editó en la card (geo, email, date, etc.).
    const mondayItemId = data.monday_item_id || null;
    // Date desde el campo editable de la card (DD/MM/YYYY → YYYY-MM-DD para Monday)
    const dateInput = card.querySelector(".pcard-date")?.value?.trim() || "";
    let fechaISO = new Date().toISOString().split("T")[0];
    if (dateInput) {
      const parts = dateInput.split("/");
      if (parts.length === 3 && parts[2].length === 4) {
        fechaISO = `${parts[2]}-${parts[1].padStart(2,"0")}-${parts[0].padStart(2,"0")}`;
      }
    }
    const mondayPayload = {
      domain:    data.domain,
      traffic,
      email,                        // Siempre se envía a Monday (no solo si doSendEmail)
      geo,
      pitch,
      estado,
      ejecutivo,
      idioma,
      fecha:     fechaISO,
      loginEmail: state.loginEmail,
    };
    if (mondayItemId) {
      await updateMonday({ ...mondayPayload, itemId: mondayItemId });
    } else {
      await pushToMonday(mondayPayload);
    }

    // 2. Send email (if requested and email available)
    if (doSendEmail && email) {
      const signature = await getGmailSignature();
      const lang      = data?.language || "es";
      // Sacar cierres viejos en cualquier idioma, luego agregar el correcto
      const stripped = (pitch || "").replace(
        /\n+\s*(best\s*regards|kind\s*regards|regards|sincerely|cheers|thanks\b|thank\s*you|saludos(?:\s*cordiales)?|un\s*saludo|cordialmente|atentamente|cumprimentos|abraços|abracos|cordiali\s*saluti|cordialement|mit\s*freundlichen\s*grüßen)[.,!]*\s*\n[\s\S]{0,200}$/i,
        ""
      ).trimEnd();
      const bodyWithClosing = appendClosingIfMissing(stripped, lang);
      const fullBody  = bodyWithClosing + (signature ? "\n\n" + signature : "");
      const result    = await sendEmail({ to: email, subject, body: fullBody, expectedFrom: state.loginEmail });
      if (!result.ok) throw new Error(result.error || "Gmail error");
    }

    // 3. Save to historial
    await saveHistory({
      domain:     data.domain,
      mediaBuyer: ejecutivo,
      pageViews:  data.traffic || 0,
      rawVisits:  data.traffic || 0,
      isNew:      true,
      ejecutivo,
      email:      email || "",
      geo,
      date:       new Date().toISOString().split("T")[0],
      source:     "auto-validated",
    });

    // 4. Mark validated in review queue
    const mark = await validateReviewItem(state.accessToken, data.id, state.loginEmail);
    if (!mark.ok) throw new Error(`Could not mark validated: ${mark.error}`);

    // 5. Update UI
    card.style.opacity = "0.3";
    setResult(doSendEmail && email ? "✅ Monday + Email sent!" : "✅ Pushed to Monday");
    setTimeout(() => { card.remove(); refreshProspectsStats(); }, 1200);

  } catch (err) {
    console.error("[Prospects validate]", err.message);
    setResult("❌ " + err.message, false);
    card.querySelectorAll("button").forEach(b => { b.disabled = false; });
  }
}

async function refreshProspectsStats() {
  const listEl  = document.getElementById("prospects-list");
  const statsEl = document.getElementById("prospects-stats");
  const remaining = listEl?.querySelectorAll(".pcard").length || 0;
  if (statsEl) statsEl.textContent = remaining ? `${remaining} pending candidates` : "No pending candidates";
  if (!remaining && listEl) listEl.innerHTML = '<div class="cascade-empty">Queue empty — great work! 🎉</div>';
  const count = await getDailyValidationCount(state.accessToken, state.loginEmail);
  updateProspectsDailyBar(count);
}

async function initProspectsTab() {
  // Restore filtros guardados de sesiones previas
  try {
    const { _prospectsDateFilter, _prospectsSourceFilter, _prospectsUserFilter } = await chrome.storage.local.get(["_prospectsDateFilter", "_prospectsSourceFilter", "_prospectsUserFilter"]);
    const dateEl   = document.getElementById("prospects-date-filter");
    const sourceEl = document.getElementById("prospects-source-filter");
    const userEl   = document.getElementById("prospects-user-filter");
    if (dateEl   && _prospectsDateFilter)   dateEl.value   = _prospectsDateFilter;
    if (sourceEl && _prospectsSourceFilter) sourceEl.value = _prospectsSourceFilter;
    if (userEl   && _prospectsUserFilter)   userEl.value   = _prospectsUserFilter;
  } catch {}
  document.getElementById("btn-prospects-refresh")?.addEventListener("click", async () => {
    await loadProspectsTab();
  });
  document.getElementById("prospects-date-filter")?.addEventListener("change", async (e) => {
    chrome.storage.local.set({ _prospectsDateFilter: e.target.value }).catch(() => {});
    await loadProspectsTab();
  });
  document.getElementById("prospects-source-filter")?.addEventListener("change", async (e) => {
    chrome.storage.local.set({ _prospectsSourceFilter: e.target.value }).catch(() => {});
    await loadProspectsTab();
  });
  document.getElementById("prospects-user-filter")?.addEventListener("change", async (e) => {
    chrome.storage.local.set({ _prospectsUserFilter: e.target.value }).catch(() => {});
    await loadProspectsTab();
  });
  // ── Filter presets: combos rápidos de un click ──────────
  document.querySelectorAll(".prospects-preset").forEach(btn => {
    btn.addEventListener("click", async () => {
      const preset = btn.dataset.preset;
      const dateEl   = document.getElementById("prospects-date-filter");
      const sourceEl = document.getElementById("prospects-source-filter");
      const userEl   = document.getElementById("prospects-user-filter");
      const myEmail  = (state.loginEmail || "").toLowerCase();
      switch (preset) {
        case "mine7":
          if (dateEl)   dateEl.value   = "last7";
          if (sourceEl) sourceEl.value = "";
          if (userEl)   userEl.value   = TEAM_EMAILS.find(e => e.toLowerCase() === myEmail) || "";
          break;
        case "all-today":
          if (dateEl)   dateEl.value   = "today";
          if (sourceEl) sourceEl.value = "";
          if (userEl)   userEl.value   = "";
          break;
        case "all-time":
          if (dateEl)   dateEl.value   = "";
          if (sourceEl) sourceEl.value = "";
          if (userEl)   userEl.value   = "";
          break;
        case "autopilot-only":
          if (dateEl)   dateEl.value   = "last7";
          if (sourceEl) sourceEl.value = "autopilot";
          if (userEl)   userEl.value   = "";
          break;
      }
      // Persistir + recargar
      chrome.storage.local.set({
        _prospectsDateFilter:   dateEl?.value || "",
        _prospectsSourceFilter: sourceEl?.value || "",
        _prospectsUserFilter:   userEl?.value || "",
      }).catch(() => {});
      await loadProspectsTab();
    });
  });

  // ── Bulk actions: checkbox selection + reject masivo ──────────
  // Validate masivo NO se ofrece: cada lead requiere chequear email/pitch/tráfico
  // antes de enviar. Reject sí porque es seguro y el caso más común (descartar
  // lotes de baja calidad de un golpe).
  const bulkBar     = document.getElementById("prospects-bulk-bar");
  const bulkCount   = document.getElementById("prospects-bulk-count");
  const updateBulkBar = () => {
    const checked = document.querySelectorAll(".pcard-bulk-cbx:checked");
    if (!bulkBar || !bulkCount) return;
    if (checked.length > 0) {
      bulkBar.style.display = "flex";
      bulkCount.textContent = `${checked.length} selected`;
    } else {
      bulkBar.style.display = "none";
    }
  };
  // Listener delegado en prospects-list — sobrevive a re-renders.
  document.getElementById("prospects-list")?.addEventListener("change", (e) => {
    if (e.target?.classList?.contains("pcard-bulk-cbx")) updateBulkBar();
  });
  document.getElementById("btn-bulk-clear")?.addEventListener("click", () => {
    document.querySelectorAll(".pcard-bulk-cbx:checked").forEach(c => { c.checked = false; });
    updateBulkBar();
  });
  document.getElementById("btn-bulk-open")?.addEventListener("click", () => {
    const checked = [...document.querySelectorAll(".pcard-bulk-cbx:checked")];
    if (checked.length === 0) return;
    const HARD_CAP = 30;
    if (checked.length > HARD_CAP) {
      if (!confirm(`Vas a abrir ${checked.length} pestañas. Cap defensivo es ${HARD_CAP}. ¿Continuar igual?`)) return;
    }
    const domains = checked
      .map(c => c.closest(".pcard")?.dataset?.domain)
      .filter(Boolean);
    domains.forEach((domain, i) => {
      setTimeout(() => chrome.tabs.create({ url: `https://${domain}`, active: false }), i * 400);
    });
    showToast(`🪟 Abriendo ${domains.length} pestañas...`, "info");
  });
  document.getElementById("btn-bulk-reject")?.addEventListener("click", async () => {
    const checked = [...document.querySelectorAll(".pcard-bulk-cbx:checked")];
    if (checked.length === 0) return;
    if (!confirm(`Rechazar ${checked.length} prospects? Esto los marca como permanentemente bloqueados.`)) return;
    const ids = checked.map(c => parseInt(c.dataset.id, 10)).filter(Boolean);
    showToast(`⏳ Rechazando ${ids.length} prospects...`, "info");
    let ok = 0, fail = 0;
    for (const id of ids) {
      const card = document.querySelector(`.pcard[data-id="${id}"]`);
      const domain = card?.dataset?.domain || "";
      try {
        await rejectReviewItem(state.accessToken, id, domain);
        card?.remove();
        ok++;
      } catch { fail++; }
    }
    showToast(`✅ Rechazados ${ok}${fail > 0 ? ` (${fail} fallaron)` : ""}.`, fail > 0 ? "warn" : "info");
    updateBulkBar();
    refreshProspectsStats();
  });

  // Auto-refresh: si autopilot o csv_queue ON, recargar lista cada 30s
  // para que prospects de Railway aparezcan progresivamente sin click manual.
  setInterval(async () => {
    if (document.visibilityState === "hidden") return;
    const tab = document.getElementById("tab-prospects");
    if (!tab?.classList.contains("active")) return;
    try {
      const [{ enabled: ap }, csvSt] = await Promise.all([
        getAutopilotState(state.accessToken),
        import("../modules/supabase.js").then(m => m.getCsvQueueState(state.accessToken)).catch(() => ({ enabled: false })),
      ]);
      if (ap || csvSt?.enabled) await loadProspectsTab();
    } catch {}
  }, 30_000);
}

async function updateApiFooter() {
  const el = document.getElementById("req-counter");
  if (!el) return;
  const usage = await getApiUsageToday(state.accessToken, state.loginEmail);
  const bp = usage.byProvider || {};
  const c  = bp.anthropic || 0;
  const g  = bp.gemini    || 0;
  const a  = bp.apollo    || 0;
  const r  = bp.rapidapi  || 0;
  const v  = bp.voyage    || 0;
  el.textContent = `API today: ${usage.total} (C:${c}/G:${g}/A:${a}/R:${r}/V:${v})`;
  el.title = `Anthropic (Claude): ${c}\nGemini: ${g}\nApollo: ${a}\nRapidAPI: ${r}\nVoyage (RAG): ${v}`;
  el.style.color = usage.total > 400 ? "#e53e3e" : usage.total > 250 ? "#d97706" : "#a0aec0";
}
