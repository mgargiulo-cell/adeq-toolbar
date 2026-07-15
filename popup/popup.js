// ============================================================
// ADEQ TOOLBAR — Popup v4
// ============================================================

import { checkDuplicate, pushToMonday, updateMonday, getMondayBoardIndex, fetchImportCandidates, fetchMondayForRefresh, parseTrafficText } from "../modules/monday.js";
import { getTraffic, formatTraffic, passesTrafficFilter, setTrafficAuthToken } from "../modules/traffic.js";
import { scrapeEmailsFromPage, scrapeContactPages, scrapeWebsiteInformer, scrapeEmailsFromSocialLinks, findDecisionMakerViaApollo, quickValidateEmail, revealApolloEmail } from "../modules/scraper.js";
import { runAudit }                                                                            from "../modules/audit.js";
import { generatePitch }                                                                     from "../modules/gemini.js";
// (geminiSearch.searchEmailsWithGemini removido — no se usa en popup, solo en scraper.js)
import { verifyEmail, verifyEmailDeep, isGarbageEmail }                                         from "../modules/emailVerifier.js";
import { runCascade }                                                                          from "../modules/cascade.js";
import { detectBanners }                                                                       from "../modules/bannerDetector.js";
import { saveHistory, loadHistory, clearHistory, saveSendDate,
         loadKeywordsFromDB, importKeywordsToDB, clearKeywordsDB, countKeywordsDB,
         searchKeywordsInDB, supabaseSignIn, supabaseRefresh, supabaseResetPassword, fetchApiKeys, setSupabaseAuth,
         uploadCsvDomains, getCsvQueueStats, getCsvQueueHistory, clearCsvQueue, getCsvQueueEnabled, setCsvQueueEnabled, logImportAttempt,
         getPitchDrafts, savePitchDraft, deletePitchDraft,
         getAutopilotEnabled, getAutopilotState, setAutopilotEnabled, saveAutopilotFeedback,
         fetchRejectedSignatures, trafficBucketLabel,
         getAutopilotTarget, setAutopilotTarget,
         fetchReviewQueue, validateReviewItem, rejectReviewItem, updateReviewItem, clearPendingProspects,
         getDailyValidationCount, getApiUsageToday, getCustomPrompt, setCustomPrompt,
         insertPitchFeedback, matchPitchFeedback, getApiUsageForProvider,
         setDomainGeo, getDomainGeo }                                                          from "../modules/supabase.js";
import { voyageEmbed, buildPitchContext }                                                    from "../modules/voyageEmbed.js";
import { sendEmail, getGmailProfile, getGmailSignature, getGmailToken, clearAllCachedTokens, appendClosingIfMissing } from "../modules/gmail.js";
import { markReviewQueueAsContacted, queueReengagement, createManualSendTracking, isEmailBounced } from "../modules/supabase.js";
import { fetchNotifications, markNotificationRead, markAllNotificationsRead, createNotification } from "../modules/supabase.js";
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

// Maxi 2026-07-01 (B2): loadDemoModeFromStorage / getRecentDomains / pushRecentDomain
// removidas — código muerto (0 call-sites, verificado en auditoría).

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
    warn.innerHTML = `🔒 ${esc(lock.locked_by)} is working on this prospect (${minutesLeft} min left). Coordinate before pushing.`;
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
    <input type="date" id="vacation-until" class="form-input" style="font-size:11px" placeholder="Until when" />
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
      status.textContent = `You are marked on vacation until ${s.vacation_until}.`;
    }
  }).catch(() => {});

  const save = async () => {
    const u = cb.checked ? (until.value || null) : null;
    await setVacationStatus(state.accessToken, state.loginEmail, u);
    status.textContent = u ? `✅ Marked as on vacation until ${u}.` : "✅ Active.";
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

    // RESET INMEDIATO — no esperar 3s. Evita mostrar data mezclada de URL vieja.
    _autoRefreshLastDomain = newDomain;
    state.tabId  = tabId;
    state.url    = newUrl;
    state.domain = newDomain;
    const siteEl = document.getElementById("site-url");
    if (siteEl) siteEl.textContent = newDomain;
    const seedEl = document.getElementById("cascade-seed");
    if (seedEl) seedEl.value = newDomain;
    // Limpieza COMPLETA del estado + UI ANTES de re-analizar
    resetAnalysisUI();

    // Debounce solo 800ms — espera a que la URL termine de estabilizarse
    // (Chrome dispara onUpdated varias veces durante el load), luego re-analiza.
    clearTimeout(_autoRefreshTimer);
    _autoRefreshTimer = setTimeout(async () => {
      try {
        const [active] = await chrome.tabs.query({ active: true, currentWindow: true });
        if (!active || active.id !== tabId) return;
        if (extractDomain(active.url) !== newDomain) return;
        // Re-correr análisis
        runAnalysisPipeline();
      } catch (e) { console.warn("[auto-refresh] failed:", e.message); }
    }, 800);
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
  // Limpieza COMPLETA al cambiar de tab/dominio — evita enviar mail con datos de URL vieja.
  // 1) Resetear estado interno (state.X)
  state.duplicate     = null;
  state.mondayItemId  = null;
  state.traffic       = 0;
  state.visits        = 0;
  state.pagesPerVisit = null;
  state.trafficData   = null;
  state.category      = "";
  state.siteLanguage  = "";
  state.siteOgLocale  = "";
  state.pageTitle     = "";
  state.pageDescription = "";
  state.siteFooterText = "";
  state.nonPublisherType = "";
  try { renderNonPublisherNotice(); } catch {}   // limpia el aviso del dominio anterior
  state.emails        = [];
  state.pageSocialLinks = []; // Maxi 2026-06-17 v3: limpiar socials del dominio anterior
  state.apolloPeople  = [];
  state.contactName   = "";
  state.pitchSubject  = "";
  state.pitchSubjects = [];
  state.adsTxt        = null;
  state.banners       = null;
  state.techStack     = [];
  state.revenueGap    = null;
  // Pitch / IA — quedaban con basura de la URL anterior
  state.pitch              = "";
  state.generatedPitches   = [];
  state.decisionMakerName  = "";

  // 2) Limpiar TODOS los elementos de UI (display + inputs + selects)
  const textIds = [
    "traffic-result", "traffic-breakdown", "traffic-countries", "traffic-category", "traffic-filter",
    "duplicate-result", "email-result", "score-badge", "audit-result", "tech-stack-result",
    "banner-result", "revenue-gap-result", "page-context-result", "similar-result",
  ];
  textIds.forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.textContent = ""; el.className = el.className.split(" ")[0]; }
  });

  // 3) Limpiar inputs/textareas/selects de Monday + Pitch
  const inputIds = [
    "form-pv-display", "form-subject", "pitch-text",
    "form-fecha", "form-telefono", "form-email-search",
    "form-email-futuro",
  ];
  // También limpiar el status text del email futuro
  const futStatus = document.getElementById("email-futuro-status");
  if (futStatus) { futStatus.textContent = ""; futStatus.style.color = ""; }
  inputIds.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });

  // 4) Resetear selects Monday a default
  const selectDefaults = {
    "form-geo":       "",
    "form-idioma":    "",
    "form-ejecutivo": state.mediaBuyer || "",
    "form-status":    "",
    "pitch-category": "",
    "pitch-tone":     "informal",
    "pitch-length":   "short",
    "pitch-focus":    "analysis",
    "pitch-opening":  "direct",
  };
  Object.entries(selectDefaults).forEach(([id, v]) => {
    const el = document.getElementById(id);
    if (el) el.value = v;
  });

  // 5) Resetear flags auto-push
  autoPushReady.traffic = false;
  autoPushReady.notDup  = false;
  autoPushReady.email   = false;

  // 6) Limpiar email options/radios del DOM (rendered cada análisis)
  const emailListEl = document.getElementById("email-result");
  if (emailListEl) emailListEl.innerHTML = "";

  // 7) Reset botón push-monday a "Send" (no "Update")
  const pushBtn = document.getElementById("btn-push-monday");
  if (pushBtn) pushBtn.textContent = "🚀 Send to Monday";

  // 8) Esconder pulgares/status del pitch (solo se muestran después de generar)
  ["btn-pitch-like", "btn-pitch-dislike"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = "none";
  });
  const likeStatus = document.getElementById("pitch-like-status");
  if (likeStatus) likeStatus.textContent = "";

  // 9) Esconder subject chips (sugerencias de la URL anterior)
  const subjChips = document.getElementById("pitch-subjects");
  if (subjChips) { subjChips.style.display = "none"; subjChips.innerHTML = ""; }

  // 10) Reset botón "Generar Pitch" si quedó en "Regenerar"
  const genBtn = document.getElementById("btn-generate-pitch");
  if (genBtn) { genBtn.textContent = "✨ Generate Pitch"; genBtn.disabled = false; }

  // 11) Restaurar draft del pitch del NUEVO dominio (si existe).
  //     Si no hay draft previo, el textarea queda vacío (ya se limpió arriba).
  //     Free: chrome.storage.local, no toca APIs pagas.
  if (typeof _restorePitchDraft === "function") {
    setTimeout(() => _restorePitchDraft(), 50);
  }
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
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage?user_email=eq.${encodeURIComponent(state.loginEmail)}&day=gte.${period}-01&select=by_provider&limit=100`,
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
  document.getElementById("cap-banner-title").textContent = `Your personal cap is at ${Math.round(pct)}%`;
  document.getElementById("cap-banner-detail").textContent = ` — ${used.toLocaleString()} / ${limit.toLocaleString()} hits this month.`;
}

// Lock para evitar 2-3 pipelines en paralelo si el user clickea rapido o
// dispara onUpdated multiple veces durante un page load.
let _pipelineRunning = false;
let _pipelinePendingDomain = null;
function runAnalysisPipeline() {
  if (_pipelineRunning) {
    // Otro pipeline en curso — guardamos el domain por si cambió, para
    // re-correr cuando termine. Evita N pipelines paralelos.
    _pipelinePendingDomain = state.domain;
    return;
  }
  _pipelineRunning = true;
  const startedDomain = state.domain;

  const tasks = [
    runDuplicateCheck().catch(() => {}),
    runTrafficCheck().catch(() => {}),
    runEmailScraper().catch(() => {}),
  ];
  if (typeof runAuditCheck === "function")     tasks.push(runAuditCheck().catch(() => {}));
  if (typeof runBannerDetection === "function") tasks.push(runBannerDetection().catch(() => {}));
  if (typeof runPageContext === "function")    tasks.push(runPageContext().catch(() => {}));

  Promise.all(tasks).finally(() => {
    _pipelineRunning = false;
    // Si durante el pipeline cambió el domain, re-correr el nuevo
    if (_pipelinePendingDomain && _pipelinePendingDomain !== startedDomain) {
      _pipelinePendingDomain = null;
      runAnalysisPipeline();
    } else {
      _pipelinePendingDomain = null;
    }
  });
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
  logo.title = "Triple-click for admin";

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
  // Wire activity filters — refresh stats + agent feed (que usa el mismo filtro user)
  document.getElementById("admin-filter-period")?.addEventListener("change", () => { loadAdminActivity(); _refreshAgentFeed(); });
  document.getElementById("admin-filter-user")?.addEventListener("change",   () => { loadAdminActivity(); _refreshAgentFeed(); });
  document.getElementById("admin-refresh-stats")?.addEventListener("click",  () => { loadAdminActivity(); _refreshAgentFeed(); });
  // Wire limits
  document.getElementById("admin-add-user-limit")?.addEventListener("click", () => addAdminLimitRow());
  document.getElementById("admin-global-caps-save")?.addEventListener("click", saveAdminGlobalCaps);
  // Wire blocklist
  document.getElementById("admin-blocklist-save")?.addEventListener("click", saveAdminBlocklist);
  document.getElementById("admin-blocklist-csv")?.addEventListener("change", handleBlocklistCsvUpload);
  // Wire reset cache button
  document.getElementById("admin-reset-cache-btn")?.addEventListener("click", resetTrafficCacheAboveThreshold);
  // Wire agent
  document.getElementById("agent-toggle")?.addEventListener("change", toggleAgent);
  document.getElementById("agent-cfg-save")?.addEventListener("click", saveAgentThresholds);
  document.getElementById("btn-feeder-runs-refresh")?.addEventListener("click", () => _refreshFeederRuns());
  document.getElementById("agent-pause-1h")?.addEventListener("click", pauseAgent1h);
  document.getElementById("agent-unpause")?.addEventListener("click", async () => {
    if (!confirm("Resume agent now? This clears any pause (manual or kill switch).")) return;
    await _writeAgentConfig({ agent_paused_until: "" });
    if (typeof showToast === "function") showToast("▶ Agent resumed", "ok", 3000);
    await loadAdminAgent();
  });
  document.getElementById("agent-refresh-toggle")?.addEventListener("click", toggleRefreshEmptyLeads);
  document.getElementById("agent-feed-export-csv")?.addEventListener("click", _exportAgentFeedCsv);
  document.getElementById("admin-export-comparator-csv")?.addEventListener("click", exportComparatorCsv);
  document.getElementById("agent-focus-save")?.addEventListener("click", saveAgentFocus);

  loadAdminActivity();

  // Auto-refresh cada 30s mientras el admin tiene un tab activo (evita stale data).
  // Solo si el admin está visible en el DOM (panel admin abierto).
  setInterval(() => {
    const panelOpen = document.getElementById("admin-panel")?.style.display !== "none";  // Maxi 2026-07-01: era "admin-panel-overlay" (no existe) → el intervalo corría SIEMPRE aunque el panel estuviera cerrado, gastando queries a Supabase cada 30s toda la sesión.
    const docVisible = document.visibilityState !== "hidden";
    if (!panelOpen || !docVisible) return;
    const activeTab = document.querySelector(".admin-tab-btn.active")?.dataset.adminTab;
    if (activeTab === "activity") { loadAdminActivity(); _refreshAgentFeed(); }
    else if (activeTab === "agent") { _refreshAgentFeed(); }
  }, 30_000);
}

async function resetTrafficCacheAboveThreshold() {
  const threshold = parseInt(document.getElementById("admin-reset-cache-threshold").value, 10) || 400000;
  const status = document.getElementById("admin-reset-cache-status");
  if (!confirm(`Delete ALL cached domains with visits ≥ ${threshold.toLocaleString()}?\n\nThis forces the team to re-analyze them (will spend API).`)) return;
  status.textContent = "⏳ Deleting...";
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
    if (!listRes.ok) { status.textContent = "❌ Could not read cache."; return; }
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
    status.textContent = `✅ ${deleted} domains cleared from shared cache. The team will re-analyze them next time.`;
    logAuditEvent(state.accessToken, {
      user_email: state.loginEmail, action: "reset_traffic_cache",
      details: { threshold, deleted, sample: domains.slice(0, 10) },
    });
  } catch (e) {
    status.textContent = "❌ Error: " + e.message;
  }
}

// ── Limits tab ─────────────────────────────────────────────
async function loadAdminGlobalCaps() {
  const csvEl = document.getElementById("admin-csv-daily-cap");
  const apEl  = document.getElementById("admin-autopilot-daily-cap");
  const stEl  = document.getElementById("admin-global-caps-status");
  if (!csvEl || !apEl) return;
  try {
    const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
    const [cfgRes, pendingRes, waitingRes, nextDayRes, bandLiveRes] = await Promise.all([
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=in.(csv_queue_daily_cap,autopilot_daily_cap_global,csv_daily_count,csv_daily_count_date,autopilot_daily_count,autopilot_daily_count_date,review_queue_band_status)&select=key,value`, { headers }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.pending&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.waiting_pool&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.next_day&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      // Band count LIVE — query directo a review_queue en vez de leer el cache de config
      // (que puede estar stale si worker no corrió hace rato)
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.350000&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
    ]);
    if (cfgRes.ok) {
      const rows = await cfgRes.json();
      const map = {};
      rows.forEach(r => { map[r.key] = r.value; });
      csvEl.value = map.csv_queue_daily_cap || "1000";
      apEl.value  = map.autopilot_daily_cap_global || "1000";
      const fmt = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit" });
      const today = fmt.format(new Date());
      const csvCount = map.csv_daily_count_date === today ? parseInt(map.csv_daily_count || "0", 10) : 0;
      const apCount  = map.autopilot_daily_count_date === today ? parseInt(map.autopilot_daily_count || "0", 10) : 0;
      const csvCap = parseInt(csvEl.value, 10);
      const apCap  = parseInt(apEl.value, 10);
      const csvPct = csvCap > 0 ? Math.round(csvCount / csvCap * 100) : 0;
      const apPct  = apCap  > 0 ? Math.round(apCount  / apCap  * 100) : 0;
      const parseCount = (r) => {
        const range = r.headers.get("content-range") || r.headers.get("Content-Range") || "";
        const m = range.match(/\/(\d+)$/);
        return m ? parseInt(m[1]) : 0;
      };
      const pendingCount = parseCount(pendingRes);
      const waitingCount = parseCount(waitingRes);
      const nextDayCount = parseCount(nextDayRes);
      // Band count LIVE — query directo a review_queue (en vez de cache config stale)
      const bandValid = parseCount(bandLiveRes);
      // Saturación 500: a partir de ahí los crons del feeder pausan intake.
      // Sin "mínimo" — los crons disparan en horarios fijos (9/12/15/18/20).
      let bandColor = "#10b981";  // verde por default
      if (bandValid >= 500) bandColor = "#f59e0b";  // ámbar: saturado
      stEl.innerHTML = `
        <div>📦 CSV ${csvCount}/${csvCap} · 🤖 Autopilot ${apCount}/${apCap}</div>
        <div>⚙️ pending ${pendingCount}/200 · ⏳ waiting ${waitingCount}/300 · 🌅 next_day ${nextDayCount}</div>
        <div style="color:${bandColor}">📊 Review queue: ${bandValid} (saturates at 500)</div>
      `;
    }
  } catch (e) { stEl.textContent = `Error: ${e.message}`; }
}

async function saveAdminGlobalCaps() {
  const csvEl = document.getElementById("admin-csv-daily-cap");
  const apEl  = document.getElementById("admin-autopilot-daily-cap");
  const stEl  = document.getElementById("admin-global-caps-status");
  const btn   = document.getElementById("admin-global-caps-save");
  if (!csvEl || !apEl) return;
  const csv = parseInt(csvEl.value, 10) || 1000;
  const ap  = parseInt(apEl.value, 10)  || 300;
  if (csv < 50 || csv > 10000) { stEl.textContent = "❌ CSV cap must be between 50 and 10000"; return; }
  if (ap  < 50 || ap  > 5000)  { stEl.textContent = "❌ Autopilot cap must be between 50 and 5000"; return; }
  if (btn) { btn.disabled = true; btn.textContent = "Saving..."; }
  try {
    const headers = {
      "apikey": CONFIG.SUPABASE_ANON_KEY,
      "Authorization": `Bearer ${state.accessToken}`,
      "Content-Type": "application/json",
      "Prefer": "resolution=merge-duplicates,return=minimal",
    };
    await Promise.all([
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config`, {
        method: "POST", headers,
        body: JSON.stringify([{ key: "csv_queue_daily_cap", value: String(csv) }]),
      }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config`, {
        method: "POST", headers,
        body: JSON.stringify([{ key: "autopilot_daily_cap_global", value: String(ap) }]),
      }),
    ]);
    stEl.textContent = `✅ Saved: CSV ${csv}/day · Autopilot ${ap}/day`;
    setTimeout(() => loadAdminGlobalCaps(), 500);
  } catch (e) {
    stEl.textContent = `❌ Error: ${e.message}`;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = "💾 Save global caps"; }
  }
}

async function loadAdminLimits() {
  // Caps globales primero (sección nueva)
  loadAdminGlobalCaps().catch(() => {});

  const list = document.getElementById("admin-limits-list");
  if (!list) return;
  list.innerHTML = '<div class="admin-help">Loading…</div>';
  const limits = await fetchAllUserLimits(state.accessToken);
  list.innerHTML = "";
  // Header con labels de cada columna
  const header = document.createElement("div");
  header.className = "admin-limit-row";
  header.style.background = "transparent";
  header.style.border = "none";
  header.style.padding = "0 4px";
  header.innerHTML = `
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px" title="Media buyer email">User</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center" title="Monthly RapidAPI hits cap (empty = no per-user cap, falls back to global 40K)">RapidAPI/mo</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center" title="Max duration of ONE autopilot session, in minutes">Session max</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center" title="Max prospects this user can add to pool per day (autopilot + CSV)">Prospects/day</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center" title="Habilita/deshabilita el autopilot para este user (independiente del agente IA)">Autopilot</span>
    <span style="font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.4px;text-align:center">Guardar</span>
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

// Mensaje unificado para CSV / Monday / Sellers.json uploads.
// Toma { inserted, attempted, intoPending, intoWaiting, intoNextDay } y devuelve mensaje claro.
function formatUploadResult(result, attempted) {
  const ins = result?.inserted || 0;
  const attempt = attempted || result?.attempted || 0;
  const dup = Math.max(0, attempt - ins);
  const nextDay = result?.intoNextDay || 0;
  const waiting = result?.intoWaiting || 0;
  if (ins === 0) {
    if (dup > 0) return { msg: `ℹ️ Los ${dup} ya estaban en el sistema (no es error, son repetidos)`, color: "#0ea5e9" };
    return { msg: `⏸ 0 nuevos para agregar`, color: "#94a3b8" };
  }
  let parts = [`✅ ${ins} agregados — aparecen primero en Prospects`];
  if (waiting > 0)  parts.push(`${waiting} en espera`);
  if (nextDay > 0)  parts.push(`${nextDay} para mañana`);
  if (dup > 0)      parts.push(`${dup} repetidos`);
  return { msg: parts.join(" · "), color: "#16a34a" };
}

function addAdminLimitRow() {
  const list = document.getElementById("admin-limits-list");
  if (!list) return;
  list.appendChild(buildLimitRow({
    user_email: "",
    autopilot_enabled: true,
    monthly_api_cap: 10000,
    autopilot_daily_minutes: 30,
    autopilot_daily_prospects: 500,
    daily_emails_cap: 100,
    daily_monday_cap: 100,
  }, true));
}

function buildLimitRow(l, isNew = false) {
  const row = document.createElement("div");
  row.className = "admin-limit-row";
  row.innerHTML = `
    <input type="email" class="form-input lim-email" value="${esc(l.user_email)}" placeholder="user@adeqmedia.com" ${isNew ? "" : "readonly"} />
    <input type="number" class="form-input lim-monthly" value="${l.monthly_api_cap || 10000}" placeholder="API/mo" min="0" title="Monthly RapidAPI hits cap" />
    <input type="number" class="form-input lim-ap-mins" value="${l.autopilot_daily_minutes ?? 30}" placeholder="min" min="5" max="60" title="Max duration of ONE autopilot session, in minutes" />
    <input type="number" class="form-input lim-ap-prospects" value="${l.autopilot_daily_prospects ?? 500}" placeholder="prosp" min="0" max="1000" title="Max prospects processed per day by this user in autopilot" />
    <span class="lim-autopilot ${l.autopilot_enabled ? "toggle-yes" : "toggle-no"}" title="Click para alternar Autopilot ON/OFF">${l.autopilot_enabled ? "AP ✓" : "AP ✗"}</span>
    <button class="save-btn" title="Save changes">💾</button>
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
    if (email && confirm(`Delete limits for ${email}?`)) {
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

/* addAdminLimitRowExisting removida (B2) — código muerto */

async function saveLimitRow(row) {
  const email = row.querySelector(".lim-email").value.trim().toLowerCase();
  if (!email) return;
  const apMins = parseInt(row.querySelector(".lim-ap-mins").value, 10);
  const apProsp = parseInt(row.querySelector(".lim-ap-prospects").value, 10);
  const limit = {
    user_email:                email,
    autopilot_enabled:         row.querySelector(".lim-autopilot").classList.contains("toggle-yes"),
    monthly_api_cap:           parseInt(row.querySelector(".lim-monthly").value, 10) || null,
    autopilot_daily_minutes:   isNaN(apMins) || apMins < 5 ? 30 : Math.min(apMins, 60),
    autopilot_daily_prospects: isNaN(apProsp) || apProsp < 0 ? 500 : Math.min(apProsp, 1000),
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
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=in.(agent_enabled_users,agent_threshold_traffic,agent_max_per_day,agent_active_hours_start,agent_active_hours_end,agent_paused_until,agent_paused_reason,agent_focus_config)&select=key,value`,
      // Maxi 2026-06-19 (fix): timeout. Sin esto, si el fetch se colgaba,
      // loadAdminAgent quedaba congelado en este await → los chips de GEO/Cat
      // nunca se dibujaban y el panel Auto-feeder quedaba en "Loading..." eterno.
      { headers, signal: AbortSignal.timeout(8000) }
    );
    const rows = await res.json();
    const cfg = {};
    rows.forEach(r => { cfg[r.key] = r.value; });
    return cfg;
  } catch { return {}; }
}

async function _writeAgentConfig(updates) {
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" };
  // upsert per key — chequea res.ok para detectar fallas RLS (401/403)
  const results = await Promise.all(Object.entries(updates).map(async ([key, value]) => {
    try {
      const res = await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config`, {
        method: "POST", headers, body: JSON.stringify({ key, value: String(value) }),
      });
      if (!res.ok) {
        const errText = await res.text().catch(() => "");
        return { key, ok: false, status: res.status, err: errText.substring(0, 200) };
      }
      return { key, ok: true };
    } catch (e) {
      return { key, ok: false, err: e.message };
    }
  }));
  const failed = results.filter(r => !r.ok);
  if (failed.length > 0) {
    const msg = failed.map(f => `${f.key}: ${f.status || ""} ${f.err || ""}`).join("\n");
    showToast(`❌ Error guardando agent config (RLS?): ${msg}`, "error", 12000);
    throw new Error(msg);
  }
}

// Categorías populares en publishers — para chips selector del Focus
const AGENT_CATEGORIES = [
  "news", "sports", "entertainment", "finance", "technology",
  "health", "lifestyle", "travel", "gambling", "automotive",
  "food", "real estate", "education", "gaming", "music",
  "fashion", "politics", "weather", "science", "shopping",
];

// ── Maxi 2026-06-19: DOS configs GEO/categoría que comparten los mismos chips ──
//   🤖 Agente (envío, key agent_focus_config) · 🏭 Worker (descubrimiento,
//   key worker_discovery_config). El toggle cambia cuál set editan los chips.
window._focusMode  = window._focusMode || "agent";
window._focusStore = window._focusStore || {
  agent:  { geos_priority: [], geos_excluded: [], categories_priority: [] },
  worker: { geos_priority: [], geos_excluded: [], categories_priority: [] },
};
function _focusInputEls() {
  return {
    pri: document.getElementById("agent-focus-geos-priority"),
    exc: document.getElementById("agent-focus-geos-excluded"),
    cat: document.getElementById("agent-focus-categories"),
  };
}
function _captureFocusInputs() {
  const { pri, exc, cat } = _focusInputEls();
  const list = (el) => (el?.value || "").split(",").map(s => s.trim()).filter(Boolean);
  window._focusStore[window._focusMode] = {
    geos_priority:       list(pri).map(s => s.toUpperCase()),
    geos_excluded:       list(exc).map(s => s.toUpperCase()),
    categories_priority: list(cat).map(s => s.toLowerCase()),
  };
}
function _applyFocusMode(mode) {
  window._focusMode = (mode === "worker") ? "worker" : "agent";
  const { pri, exc, cat } = _focusInputEls();
  const s = window._focusStore[window._focusMode] || {};
  if (pri) pri.value = (s.geos_priority || []).join(",");
  if (exc) exc.value = (s.geos_excluded || []).join(",");
  if (cat) cat.value = (s.categories_priority || []).join(",");
  _renderGeoChips();
  _renderCategoryChips();
  const aBtn = document.getElementById("focus-mode-agent");
  const wBtn = document.getElementById("focus-mode-worker");
  const hint = document.getElementById("focus-mode-hint");
  const ON = "#2563eb", OFF = "#334155";
  if (aBtn) { aBtn.style.background = window._focusMode === "agent"  ? ON : OFF; aBtn.style.color = "#fff"; }
  if (wBtn) { wBtn.style.background = window._focusMode === "worker" ? ON : OFF; wBtn.style.color = "#fff"; }
  if (hint) hint.textContent = window._focusMode === "agent"
    ? "Editando: a qué GEOs/categorías el AGENTE ENVÍA mails (de Prospects)."
    : "Editando: qué GEOs/categorías el WORKER trae a Prospects (descubrimiento).";
}
function _setFocusMode(mode) { _captureFocusInputs(); _applyFocusMode(mode); }

// Maxi 2026-06-18: chips de GEO/Cat reescritos con delegación de eventos +
// listeners persistentes. Antes fallaba en algunos paths (no se actualizaba
// el visual al click). Ahora el listener queda 1 vez en el wrap padre y
// sobrevive re-renders. Estado: 0=ignored, 1=priority (verde), 2=excluded (rojo).
function _renderGeoChips() {
  const wrap = document.getElementById("agent-focus-geos-chips");
  if (!wrap) return;
  const priInput = document.getElementById("agent-focus-geos-priority");
  const excInput = document.getElementById("agent-focus-geos-excluded");
  if (!priInput || !excInput) return;
  const priSet = new Set((priInput.value || "").split(",").map(s => s.trim().toUpperCase()).filter(Boolean));
  const excSet = new Set((excInput.value || "").split(",").map(s => s.trim().toUpperCase()).filter(Boolean));

  const entries = Object.entries(GEO_LABEL).sort((a, b) => a[1].localeCompare(b[1]));
  wrap.innerHTML = entries.map(([code, name]) => {
    const isPri = priSet.has(code);
    const isExc = excSet.has(code);
    const bg = isPri ? "#16a34a" : isExc ? "#dc2626" : "#334155";
    const ico = isPri ? "✓" : isExc ? "✕" : "";
    return `<button type="button" class="agent-geo-chip" data-code="${code}" style="background:${bg};color:#fff;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer;font-weight:600;margin:1px">${ico} ${name}</button>`;
  }).join("");

  // Delegación: 1 solo listener en el wrap, sobrevive a re-renders.
  if (!wrap.dataset._delegated) {
    wrap.dataset._delegated = "1";
    wrap.addEventListener("click", (e) => {
      const chip = e.target.closest(".agent-geo-chip");
      if (!chip) return;
      const code = chip.dataset.code;
      const pri = document.getElementById("agent-focus-geos-priority");
      const exc = document.getElementById("agent-focus-geos-excluded");
      if (!pri || !exc) return;
      const p = new Set((pri.value || "").split(",").map(s => s.trim().toUpperCase()).filter(Boolean));
      const x = new Set((exc.value || "").split(",").map(s => s.trim().toUpperCase()).filter(Boolean));
      // Cycle: ignored → priority → excluded → ignored
      if (p.has(code))      { p.delete(code); x.add(code); }
      else if (x.has(code)) { x.delete(code); }
      else                  { p.add(code); }
      pri.value = [...p].join(",");
      exc.value = [...x].join(",");
      _renderGeoChips();
    });
  }
}

function _renderCategoryChips() {
  const wrap = document.getElementById("agent-focus-categories-chips");
  if (!wrap) return;
  const input = document.getElementById("agent-focus-categories");
  if (!input) return;
  const selected = new Set((input.value || "").split(",").map(s => s.trim().toLowerCase()).filter(Boolean));

  wrap.innerHTML = AGENT_CATEGORIES.map(cat => {
    const isOn = selected.has(cat);
    const bg = isOn ? "#16a34a" : "#334155";
    const ico = isOn ? "✓ " : "";
    return `<button type="button" class="agent-cat-chip" data-cat="${cat}" style="background:${bg};color:#fff;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer;font-weight:600;margin:1px">${ico}${cat}</button>`;
  }).join("");

  if (!wrap.dataset._delegated) {
    wrap.dataset._delegated = "1";
    wrap.addEventListener("click", (e) => {
      const chip = e.target.closest(".agent-cat-chip");
      if (!chip) return;
      const inp = document.getElementById("agent-focus-categories");
      if (!inp) return;
      const cat = chip.dataset.cat;
      const cur = new Set((inp.value || "").split(",").map(s => s.trim().toLowerCase()).filter(Boolean));
      if (cur.has(cat)) cur.delete(cat); else cur.add(cat);
      inp.value = [...cur].join(",");
      _renderCategoryChips();
    });
  }
}

// Populate hour selects (0-23)
function _populateHourSelects() {
  ["agent-cfg-active-start", "agent-cfg-active-end"].forEach(id => {
    const sel = document.getElementById(id);
    if (!sel || sel.options.length > 0) return;
    for (let h = 0; h < 24; h++) {
      const opt = document.createElement("option");
      opt.value = String(h);
      opt.textContent = `${String(h).padStart(2, "0")}:00`;
      sel.appendChild(opt);
    }
  });
}

async function loadAdminAgent() {
  _populateHourSelects();
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
    const endH   = parseInt(cfg.agent_active_hours_end   || "23", 10);
    // Calcular hora actual España
    let spainH = 0;
    try {
      const fmt = new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", hour: "numeric", hour12: false });
      spainH = parseInt(fmt.format(new Date()), 10);
    } catch {}
    const inActiveWindow = startH < endH ? (spainH >= startH && spainH < endH) : (spainH >= startH || spainH < endH);
    if (isPaused) {
      const minLeft = Math.round((pausedUntil - Date.now()) / 60000);
      const reason = cfg.agent_paused_reason || "";
      const reasonStr = reason ? ` <span style="color:#94a3b8;font-size:10px">(${esc(reason.substring(0, 60))})</span>` : "";
      statusEl.innerHTML = `<strong style="color:#f87171">⏸ Paused ${minLeft}min</strong>${reasonStr}`;
    } else if (enabled && !inActiveWindow) {
      statusEl.innerHTML = `<strong style="color:#fbbf24">🌙 Off-hours</strong>`;
    } else if (enabled && inActiveWindow) {
      statusEl.innerHTML = `<strong style="color:#34d399">🟢 Active</strong>`;
    } else {
      statusEl.innerHTML = `<span style="color:#94a3b8">⚪ Inactive</span>`;
    }
  }

  // Inputs de threshold
  const setVal = (id, v, dflt) => { const el = document.getElementById(id); if (el) el.value = v || dflt; };
  setVal("agent-cfg-traffic",      cfg.agent_threshold_traffic,  500000);
  // agent_threshold_score: hidden field, ya no se usa para filtrar (decisión 2026-05-18).
  setVal("agent-cfg-max",          cfg.agent_max_per_day,         10);
  setVal("agent-cfg-active-start", cfg.agent_active_hours_start,   9);
  setVal("agent-cfg-active-end",   cfg.agent_active_hours_end,    20);
  document.getElementById("agent-stat-cap").textContent = cfg.agent_max_per_day || "10";

  // Focus config (JSON) — DOS sets: 🤖 agente (envío) y 🏭 worker (descubrimiento)
  let focus = { geos_priority: [], geos_excluded: [], categories_priority: [], weekly_target: 0, daily_override: 0 };
  try { focus = { ...focus, ...JSON.parse(cfg.agent_focus_config || "{}") }; } catch {}
  let wdisc = { geos_priority: [], geos_excluded: [], categories_priority: [] };
  try { wdisc = { ...wdisc, ...JSON.parse(cfg.worker_discovery_config || "{}") }; } catch {}
  window._focusStore = {
    agent:  { geos_priority: focus.geos_priority || [], geos_excluded: focus.geos_excluded || [], categories_priority: focus.categories_priority || [] },
    worker: { geos_priority: wdisc.geos_priority || [], geos_excluded: wdisc.geos_excluded || [], categories_priority: wdisc.categories_priority || [] },
  };
  setVal("agent-focus-daily",  focus.daily_override, 0);
  setVal("agent-focus-weekly", focus.weekly_target,  0);

  // Cablear el toggle Agente/Worker (1 vez) y aplicar el modo activo (carga el set + render).
  ["focus-mode-agent", "focus-mode-worker"].forEach(id => {
    const b = document.getElementById(id);
    if (b && !b.dataset._wired) { b.dataset._wired = "1"; b.addEventListener("click", () => _setFocusMode(b.dataset.mode)); }
  });
  _applyFocusMode(window._focusMode || "agent");

  // Stats hoy
  // Maxi 2026-06-19 (fix): en paralelo + allSettled. Antes era await secuencial:
  // si uno se colgaba, los siguientes (ej. feeder runs) nunca corrían.
  await Promise.allSettled([
    _refreshAgentStats(myEmail),
    _refreshAgentFeed(),
    _refreshFeederRuns(),
  ]);
}

async function _refreshAgentStats(userEmail) {
  // Bug fix 2026-05-13: counter contaba solo action='sent'. Ahora cuenta
  // todas las variantes de send (sent + re_sent + bounce_retry_sent +
  // monday_ok legacy) y normaliza email a lowercase para case-mismatch.
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
  const cutoff24 = new Date(Date.now() - 24 * 3600_000).toISOString();
  const startToday = new Date(); startToday.setHours(0,0,0,0);
  const emailLower = (userEmail || "").toLowerCase();
  const userClause = `user_email=eq.${encodeURIComponent(emailLower)}`;
  // NO incluir 'monday_ok' — es row separado por cada send (audit trail
  // de Monday push). Si lo sumamos, contamos cada email 2× (sent + monday_ok).
  const sentActions = "action=in.(sent,re_sent,bounce_retry_sent)";
  try {
    const [sentRes, skipRes, failRes] = await Promise.all([
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?${userClause}&${sentActions}&created_at=gte.${startToday.toISOString()}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?${userClause}&action=eq.skipped&created_at=gte.${cutoff24}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?${userClause}&action=in.(failed,monday_failed)&created_at=gte.${cutoff24}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
    ]);
    const parseCount = (res) => { const m = (res.headers.get("content-range") || "").match(/\/(\d+)$/); return m ? parseInt(m[1]) : 0; };
    document.getElementById("agent-stat-sent").textContent    = parseCount(sentRes);
    document.getElementById("agent-stat-skipped").textContent = parseCount(skipRes);
    document.getElementById("agent-stat-failed").textContent  = parseCount(failRes);
  } catch {}
}

// Panel de Auto-feeder runs (Admin → Agent tab) — muestra los últimos 20
// disparos del cron 9/12/15/18/20 Madrid con métricas y conversion rate real.
async function _refreshFeederRuns() {
  const sumEl  = document.getElementById("feeder-runs-summary");
  const listEl = document.getElementById("feeder-runs-list");
  if (!listEl || !sumEl) return;
  listEl.textContent = "Loading...";
  sumEl.textContent  = "Loading...";
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_feeder_runs?select=*&order=cron_at.desc&limit=20`,
      // Maxi 2026-06-19 (fix): timeout para que no quede "Loading..." eterno si
      // el fetch se cuelga (sin esto el panel nunca salía del estado inicial).
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` }, signal: AbortSignal.timeout(8000) }
    );
    if (!res.ok) {
      listEl.innerHTML = `<div style="color:#f87171">HTTP ${res.status} — ¿RLS bloquea SELECT en toolbar_feeder_runs? (debe permitir solo a admin)</div>`;
      sumEl.textContent = "—";
      return;
    }
    const runs = await res.json();
    if (!Array.isArray(runs) || runs.length === 0) {
      listEl.innerHTML = `<div style="color:#94a3b8;font-style:italic">No runs yet. Next crons: 9/12/15/18/20 Madrid Mon-Fri.</div>`;
      sumEl.innerHTML = `<span style="color:#94a3b8">No recent activity.</span>`;
      return;
    }
    // Stats agregadas para el último día con actividad
    const todayISO = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
    const todayRuns = runs.filter(r => (r.cron_at || "").slice(0, 10) === todayISO);
    const todayOk = todayRuns.filter(r => r.status === "ok");
    const dayGross = todayOk.reduce((s, r) => s + (parseInt(r.gross_total, 10) || 0), 0);
    const dayEff = todayOk.reduce((s, r) => s + (parseInt(r.effective_added, 10) || 0), 0);
    const dayConv = dayGross > 0 ? (dayEff / dayGross * 100).toFixed(1) : "—";
    const target = 150;
    const pct = Math.min(100, Math.round((dayEff / target) * 100));
    sumEl.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;gap:8px">
        <div>
          <strong style="color:#10b981">Today:</strong> ${dayEff}/${target} effective ·
          <span style="color:#94a3b8">${dayGross} imported · conv ${dayConv}%</span>
        </div>
        <div style="font-size:10px;color:#94a3b8">${todayRuns.length} crons run</div>
      </div>
      <div style="margin-top:6px;height:6px;background:#0f172a;border-radius:3px;overflow:hidden">
        <div style="width:${pct}%;height:100%;background:linear-gradient(90deg,#3b82f6,#10b981)"></div>
      </div>
    `;
    // Render runs (compact)
    const statusBadge = (s) => {
      const map = {
        ok:                     ["✅", "#10b981"],
        skipped_rapidapi:       ["⏸",  "#f59e0b"],
        skipped_saturated:      ["⏸",  "#6366f1"],
        skipped_daily_target:   ["✓",  "#10b981"],
        incomplete:             ["⚠️", "#f87171"],
      };
      const [icon, color] = map[s] || ["•", "#94a3b8"];
      return `<span style="color:${color}">${icon} ${s}</span>`;
    };
    listEl.innerHTML = runs.map(r => {
      const slot = (r.slot_label || "").slice(11) || "—";
      const date = (r.cron_at || "").slice(5, 10);
      const sources = `S:${r.gross_sellers || 0} M:${r.gross_monday || 0} J:${r.gross_majestic || 0}`;
      const gross = r.gross_total ?? "—";
      const eff = r.effective_added != null ? r.effective_added : "<span style='color:#94a3b8'>pending</span>";
      const conv = r.conversion_pct != null ? `${parseFloat(r.conversion_pct).toFixed(1)}%` : "—";
      const note = r.notes ? ` <span style="color:#94a3b8">${esc(r.notes.substring(0, 50))}</span>` : "";
      return `<div style="padding:2px 0;border-bottom:1px solid #334155;line-height:1.6">
        <span style="color:#94a3b8">${date} ${slot}</span> ·
        ${statusBadge(r.status || "unknown")} ·
        <span style="color:#cbd5e1">${sources}</span> ·
        gross <strong>${gross}</strong> →
        eff <strong style="color:#10b981">${eff}</strong> · ${conv}${note}
      </div>`;
    }).join("");
  } catch (e) {
    listEl.innerHTML = `<div style="color:#f87171">Error: ${esc(e.message || String(e))}</div>`;
    sumEl.textContent = "Error";
  }
}

async function _refreshAgentFeed() {
  const wrap = document.getElementById("agent-actions-feed");
  if (!wrap) return;
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
  try {
    // Filtro por user opcional: si el admin filtró por user en Activity tab,
    // respetamos ese filtro acá también. Default = todos los agentes.
    const userFilter = (document.getElementById("admin-filter-user")?.value || "").trim();
    const userClause = userFilter ? `&user_email=eq.${encodeURIComponent(userFilter.toLowerCase())}` : "";
    // Política user 2026-05-18: en el feed solo se muestran ACCIONES REALES
    // (sent / re_sent / bounce_retry_sent). No nos interesa ver skips ni
    // reserved ni heartbeats — eso es ruido. Si necesitás debug, está en
    // toolbar_agent_actions completo en Supabase.
    const realActions = "action=in.(sent,re_sent,bounce_retry_sent)";
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?${userClause ? userClause + "&" : ""}${realActions}&select=*&order=created_at.desc&limit=50`,
      { headers }
    );
    if (!res.ok) {
      const errBody = await res.text().catch(() => "");
      wrap.innerHTML = `<div style="color:#f87171">HTTP ${res.status} ${esc(errBody.slice(0,120))}<br/>Likely: RLS blocks SELECT on toolbar_agent_actions.</div>`;
      return;
    }
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) {
      wrap.innerHTML = `<div style="color:#94a3b8">No activity yet. Toggle Agent ON to start sending.</div>`;
      return;
    }
    // Guardar para el botón CSV
    window._lastAgentFeedRows = rows;
    const icons = { sent: "✅", re_sent: "🔁", bounce_retry_sent: "🎯", skipped: "⏭", failed: "❌", monday_failed: "⚠️", monday_ok: "🟢", kill_switch: "🚨", cycle_no_candidates: "🔍", cycle_heartbeat: "💓", reserved: "⏳" };
    const colorMap = { sent: "#34d399", re_sent: "#22d3ee", bounce_retry_sent: "#a78bfa", skipped: "#fbbf24", failed: "#f87171", monday_failed: "#fb923c", monday_ok: "#34d399", kill_switch: "#ef4444", cycle_no_candidates: "#94a3b8", cycle_heartbeat: "#64748b", reserved: "#64748b" };
    const actionLabels = { sent: "Sent", re_sent: "Re-sent (follow-up)", bounce_retry_sent: "Bounce retry sent", skipped: "Skipped", failed: "Failed", monday_failed: "Monday push failed", monday_ok: "Monday pushed", kill_switch: "Kill switch fired", cycle_no_candidates: "No candidates this cycle", cycle_heartbeat: "Heartbeat", reserved: "Reserved slot" };
    wrap.innerHTML = rows.map(r => {
      const time = new Date(r.created_at).toLocaleString("es-AR", { hour: "2-digit", minute: "2-digit", day: "2-digit", month: "2-digit" });
      const icon = icons[r.action] || "·";
      const color = colorMap[r.action] || "#cbd5e1";
      const email = r.details?.email || "";
      const userShort = (r.user_email || "").split("@")[0];
      const reasonStr = r.reason ? `<span style="color:#94a3b8"> · ${esc(r.reason)}</span>` : "";
      const emailStr = email ? `<br/><span style="color:#60a5fa;margin-left:18px;font-size:10px">→ ${esc(email)}</span>` : "";
      const subjectStr = r.pitch_subject ? `<br/><span style="color:#cbd5e1;margin-left:18px;font-style:italic;font-size:10px">"${esc(r.pitch_subject.substring(0, 70))}"</span>` : "";
      const label = actionLabels[r.action] || r.action;
      return `<div style="padding:5px 0;border-bottom:1px solid #334155">
        <span style="color:${color}">${icon}</span>
        <span style="color:#94a3b8;font-size:9px">[${esc(time)}]</span>
        <span style="color:#a78bfa;font-size:9px">${esc(userShort)}</span>
        <span style="color:${color};font-size:9px;font-weight:600;margin:0 4px">${esc(label)}</span>
        <strong style="color:#e2e8f0">${esc(r.domain)}</strong>${reasonStr}
        ${emailStr}${subjectStr}
      </div>`;
    }).join("");
  } catch (e) {
    wrap.innerHTML = `<div style="color:#f87171">Error: ${esc(e.message || String(e))}</div>`;
  }
}

function _exportAgentFeedCsv() {
  const rows = window._lastAgentFeedRows || [];
  if (rows.length === 0) { alert("No actions loaded yet."); return; }
  const csvCell = (v) => {
    if (v == null) return "";
    const s = String(v).replace(/"/g, '""');
    return `"${s}"`;
  };
  const headers = ["created_at", "user_email", "domain", "action", "reason", "email_to", "pitch_subject", "traffic", "geo", "language"];
  const lines = [headers.join(",")];
  rows.forEach(r => {
    lines.push([
      csvCell(r.created_at),
      csvCell(r.user_email),
      csvCell(r.domain),
      csvCell(r.action),
      csvCell(r.reason),
      csvCell(r.details?.email),
      csvCell(r.pitch_subject),
      csvCell(r.details?.traffic),
      csvCell(r.details?.geo),
      csvCell(r.details?.language),
    ].join(","));
  });
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href = url;
  a.download = `agent_actions_${new Date().toISOString().split("T")[0]}.csv`;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// Helper: si está fuera de 9-23 L-V Madrid, pregunta por cuántas horas activar override (1-8).
// Setea toolbar_config.manual_override_until. Devuelve true si puede proceder, false si canceló.
async function _checkManualOverrideIfOutside() {
  const mNow = new Date(new Date().toLocaleString("en-US", { timeZone: "Europe/Madrid" }));
  const h = mNow.getHours();
  const d = mNow.getDay();
  const outside = (d === 0 || d === 6 || h < 9 || h >= 23);
  if (!outside) return true;
  const ans = prompt(`⚠️ Outside operating hours (9-23 Mon-Fri Madrid). Madrid time: ${h}h.\n\nFor how many hours enable override? (1-8)\nAfter that the worker goes back to sleep.`, "2");
  if (ans === null) return false;
  const hrs = parseInt(ans, 10);
  if (!hrs || hrs < 1 || hrs > 8) { alert("⛔ Invalid value. Must be between 1 and 8 hours."); return false; }
  const until = new Date(Date.now() + hrs * 60 * 60 * 1000).toISOString();
  await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config`, {
    method: "POST",
    headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" },
    body: JSON.stringify({ key: "manual_override_until", value: until }),
  }).catch(() => {});
  return true;
}

async function toggleAgent(e) {
  const enabled = e.target.checked;
  // Solo aplicar gate al PRENDER (apagar siempre puede)
  if (enabled && !(await _checkManualOverrideIfOutside())) { e.target.checked = false; return; }
  const myEmail = (state.loginEmail || "").toLowerCase();
  const cfg = await _readAgentConfig();
  let users = [];
  try { users = JSON.parse(cfg.agent_enabled_users || "[]"); } catch {}
  users = users.map(u => u.toLowerCase()).filter(u => u !== myEmail);
  if (enabled) users.push(myEmail);
  // Bug fix 2026-05-13: setear agent_manual_off para que el self-activator
  // del worker respete el toggle OFF. Sin este flag, el self-activator
  // re-poblaba enabled_users cada tick L-V 9-23 Madrid → agent seguía
  // mandando aunque el admin tocara OFF.
  await _writeAgentConfig({
    agent_enabled_users: JSON.stringify(users),
    agent_manual_off:    users.length === 0 ? "true" : "false",
  });
  await loadAdminAgent();
  showToast(enabled ? "🟢 Agent activated" : "⚪ Agent deactivated (will not auto-reactivate)", "info");
}

async function saveAgentThresholds() {
  // Maxi 2026-06-17: UNIFICADO. El botón "💾 Guardar TODO" ahora persiste
  // thresholds + max/day + GEOs + categorías + weekly de UNA. Antes era
  // 2 botones separados (Save thresholds vs Save focus), confuso para el MB.
  // Además: el max_per_day APLICA A TODOS los MBs (es config global del worker).
  const updates = {
    agent_threshold_traffic:  parseInt(document.getElementById("agent-cfg-traffic").value, 10) || 400000,
    agent_max_per_day:        parseInt(document.getElementById("agent-cfg-max").value, 10) || 10,
    agent_active_hours_start: parseInt(document.getElementById("agent-cfg-active-start").value, 10) || 9,
    agent_active_hours_end:   parseInt(document.getElementById("agent-cfg-active-end").value, 10) || 23,
  };
  // Maxi 2026-06-17: si el admin baja el cap global (ej. 30→15), también
  // limpiamos `agent_max_per_day_by_user` para que NO siga overrideando.
  // Antes Diego/Agus quedaban en 15 hardcoded; ahora con UI unificada el cap
  // global gana sobre el override per-usuario.
  updates.agent_max_per_day_by_user = "{}";

  // También persistimos focus (GEOs + categorías + weekly) en el mismo save.
  const parseList = (id) => (document.getElementById(id).value || "")
    .split(",").map(s => s.trim()).filter(Boolean);
  const focus = {
    geos_priority:       parseList("agent-focus-geos-priority").map(s => s.toUpperCase()),
    geos_excluded:       parseList("agent-focus-geos-excluded").map(s => s.toUpperCase()),
    categories_priority: parseList("agent-focus-categories").map(s => s.toLowerCase()),
    daily_override:      parseInt(document.getElementById("agent-focus-daily").value, 10) || 0,
    weekly_target:       parseInt(document.getElementById("agent-focus-weekly").value, 10) || 0,
  };
  updates.agent_focus_config = JSON.stringify(focus);

  await _writeAgentConfig(updates);
  showToast("✅ Configuración guardada (aplica a los 3 MBs)", "info");
  await loadAdminAgent();
}

async function saveAgentFocus() {
  // Captura el modo que se está editando al store, después escribe AMBAS configs.
  _captureFocusInputs();
  const a = window._focusStore.agent  || {};
  const w = window._focusStore.worker || {};
  const focus = {
    geos_priority:       a.geos_priority || [],
    geos_excluded:       a.geos_excluded || [],
    categories_priority: a.categories_priority || [],
    daily_override:      parseInt(document.getElementById("agent-focus-daily").value, 10) || 0,
    weekly_target:       parseInt(document.getElementById("agent-focus-weekly").value, 10) || 0,
  };
  const worker = {
    geos_priority:       w.geos_priority || [],
    geos_excluded:       w.geos_excluded || [],
    categories_priority: w.categories_priority || [],
  };
  await _writeAgentConfig({
    agent_focus_config:     JSON.stringify(focus),    // 🤖 a qué envía el agente
    worker_discovery_config: JSON.stringify(worker),  // 🏭 qué trae el worker a Prospects
  });
  showToast("✅ Config guardada (agente + worker)", "info");
  await loadAdminAgent();
}

// Exporta el comparador horizontal del admin como CSV.
// Lee el DOM ya renderizado (contiene los datos del periodo + filtros aplicados).
function exportComparatorCsv() {
  const wrap = document.getElementById("admin-mb-comparator");
  if (!wrap) { showToast("❌ Comparator not loaded", "error"); return; }
  const rows = [];
  // Iterar las filas del grid: cada label + 4 celdas (3 MBs + Agent)
  const cells = wrap.querySelectorAll(".mbc-cell, .mbc-group-sep, .mbc-row-label");
  let currentRow = [];
  let isHeader = true;
  let groupTitle = "";

  // Approach simple: iterar children y agrupar por filas
  const allChildren = [...wrap.children];
  let i = 0;
  while (i < allChildren.length) {
    const node = allChildren[i];
    if (node.classList.contains("mbc-row")) {
      const cellsInRow = [...node.querySelectorAll(".mbc-cell")];
      const rowVals = cellsInRow.map(c => (c.querySelector(".mbc-val")?.textContent || c.textContent || "").trim());
      // Si es header, prepend "Métrica"
      if (node.classList.contains("mbc-header")) {
        rows.push(rowVals.map(v => `"${v.replace(/"/g, '""')}"`).join(","));
      } else {
        // Las row labels normales: primera celda es la métrica
        rows.push((groupTitle ? `[${groupTitle}] ` : "") + rowVals.map(v => `"${v.replace(/"/g, '""')}"`).join(","));
      }
    } else if (node.classList.contains("mbc-group-sep")) {
      groupTitle = (node.textContent || "").trim();
    }
    i++;
  }

  if (rows.length === 0) { showToast("❌ No data to export", "error"); return; }

  // Filtros aplicados al título del archivo
  const period = document.getElementById("admin-filter-period")?.value || "";
  const userF = document.getElementById("admin-filter-user")?.value || "all";
  const today = new Date().toISOString().split("T")[0];
  const filename = `adeq-comparador-${period}-${userF.replace(/[^a-z0-9]/gi, "_")}-${today}.csv`;

  const csv = "﻿" + rows.join("\n"); // BOM UTF-8 para Excel
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  setTimeout(() => { URL.revokeObjectURL(url); a.remove(); }, 100);
  showToast(`✅ CSV descargado: ${filename}`, "info");
}

async function toggleRefreshEmptyLeads() {
  // Lee estado actual + lo invierte
  const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=eq.agent_refresh_empty_leads&select=value`,
      { headers }
    );
    const rows = await res.json();
    const current = rows?.[0]?.value === "true";
    const newVal = !current;
    await _writeAgentConfig({ agent_refresh_empty_leads: String(newVal) });
    showToast(newVal ? "🔄 Refresh enabled — worker processes 1 lead/cycle" : "⏸ Refresh disabled", "info");
    const statusEl = document.getElementById("agent-refresh-status");
    if (statusEl) statusEl.textContent = `Refresh leads sin traffic: ${newVal ? "🟢 ON" : "⚪ OFF"}`;
    const btnEl = document.getElementById("agent-refresh-toggle");
    if (btnEl) btnEl.textContent = newVal ? "⏸ Pause refresh" : "🔄 Activate refresh";
  } catch (e) {
    showToast("❌ Error: " + e.message, "error");
  }
}

async function pauseAgent1h() {
  if (!confirm("Pause agent for 1 hour?")) return;
  const pauseUntil = new Date(Date.now() + 3600_000).toISOString();
  await _writeAgentConfig({ agent_paused_until: pauseUntil });
  showToast("⏸ Agent paused 1h", "warn");
  await loadAdminAgent();
}

async function loadAdminBlocklist() {
  const ta = document.getElementById("admin-blocklist-text");
  const status = document.getElementById("admin-blocklist-status");
  if (!ta) return;
  status.textContent = "Loading…";
  try {
    const { TOP_500_BLOCKED } = await import("../modules/blockedDomainsTop500.js");
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_url_blocklist?select=domain,category,reason,added_by&order=category,domain`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
    );
    const adminList = res.ok ? await res.json() : [];
    const byCategory = { manual: [], corporate: [], inoperativo: [] };
    adminList.forEach(r => {
      const cat = r.category || "manual";
      if (byCategory[cat]) byCategory[cat].push(r);
    });
    // Textarea muestra solo "manual" (los editables)
    ta.value = byCategory.manual.map(r => r.domain).join("\n");
    status.innerHTML = `
      <div style="margin-bottom:6px"><strong>${adminList.length}</strong> dominios admin custom (editables abajo) + <strong>${TOP_500_BLOCKED.length}</strong> baked-in.</div>
      <div style="display:flex;gap:12px;font-size:11px">
        <span>✋ manual: <strong>${byCategory.manual.length}</strong></span>
        <span>🏢 corporate: <strong>${byCategory.corporate.length}</strong></span>
        <span title="Auto-bloqueados tras 3 freeze cycles sin traffic data">🚫 inoperativo: <strong>${byCategory.inoperativo.length}</strong></span>
      </div>
      <div style="opacity:.7;font-size:10px;margin-top:4px">Total efectivo: ${(adminList.length + TOP_500_BLOCKED.length).toLocaleString()} dominios bloqueados pre-API.</div>
    `;
  } catch (e) { status.textContent = "Error: " + e.message; }
}

async function saveAdminBlocklist() {
  const ta = document.getElementById("admin-blocklist-text");
  const status = document.getElementById("admin-blocklist-status");
  const domains = (ta.value || "")
    .split(/[\n,]/).map(d => d.trim().toLowerCase().replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/.*$/, ""))
    .filter(d => d && d.includes("."));
  if (!domains.length) { status.textContent = "❌ Empty list."; return; }
  status.textContent = "⏳ Saving...";
  try {
    // Borrar SOLO category='manual' (no tocar inoperativo/corporate auto-poblados)
    await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_url_blocklist?category=eq.manual`, {
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
      body: JSON.stringify(domains.map(d => ({ domain: d, category: "manual", added_by: state.loginEmail }))),
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

  // toolbar_bounce_retries — 1 row por cada bounce detectado (con mb_email).
  // La usamos para calcular % bounce por MB en el comparador.
  const bounceUserClause = userFilter ? `&mb_email=eq.${encodeURIComponent(userFilter.toLowerCase())}` : "";
  const [histRes, trackRes, usageRes, sessionsRes, queueRes, agentActions, bounceRetries] = await Promise.all([
    // toolbar_historial — sites analizados manualmente desde toolbar.
    // Filtro por created_at (timestamp confiable) en vez de date (string DD/MM/YYYY).
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_historial?created_at=gte.${from}&created_at=lte.${to}T23:59:59${histUserClause}&select=*&order=created_at.desc&limit=2000`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_sendtrack?send_date=gte.${from}&send_date=lte.${to}&select=domain,send_date`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage?day=gte.${from}&day=lte.${to}${usageUserClause}&select=*`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
    fetchUsageStats(state.accessToken, { from, to, userEmail: userFilter }),
    // toolbar_review_queue — DOS queries para capturar TODA la actividad:
    // 1) Rows CREADOS en el rango (descubrimientos/imports del periodo)
    // 2) Rows VALIDADOS en el rango (procesados desde Prospects en el periodo)
    // Ej: Diego valida hoy un lead creado la semana pasada → si no lo traemos
    // por validated_at, no contamos esa acción de Diego.
    // Combinamos los 2 arrays + dedupe por id en el aggregator más abajo.
    Promise.all([
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_review_queue?created_at=gte.${from}&created_at=lte.${to}T23:59:59${queueUserClause}&select=domain,traffic,geo,category,score,source,status,created_by,created_at,validated_by,validated_at,id&order=created_at.desc&limit=3000`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_review_queue?validated_at=gte.${from}&validated_at=lte.${to}T23:59:59&select=domain,traffic,geo,category,score,source,status,created_by,created_at,validated_by,validated_at,id&order=validated_at.desc&limit=3000`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
    ]).then(([byCreate, byValidate]) => {
      // Dedupe por id (un row puede aparecer en ambos arrays)
      const seen = new Set();
      const merged = [];
      [...byCreate, ...byValidate].forEach(r => {
        if (!seen.has(r.id)) { seen.add(r.id); merged.push(r); }
      });
      return merged;
    }),
    // toolbar_agent_actions — el agente cuenta como "MB" más en el comparador.
    // Filtramos action=sent (los exitosos) en el período.
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=in.(sent,monday_ok)&created_at=gte.${from}&created_at=lte.${to}T23:59:59&select=domain,user_email,action,pitch_subject,details,created_at&order=created_at.desc&limit=5000`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
    fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_bounce_retries?created_at=gte.${from}&created_at=lte.${to}T23:59:59${bounceUserClause}&select=mb_email,domain,bounce_type,created_at&order=created_at.desc&limit=3000`, { headers }).then(r => r.ok ? r.json() : []).catch(() => []),
  ]);


  // ── Métricas ────────────────────────────────────────────────
  // sites = analyses manuales (historial) + prospects agregados por worker (review_queue, dedup por domain).
  const histDomains    = new Set(histRes.map(h => (h.domain || "").toLowerCase()).filter(Boolean));
  const queueDomains   = new Set(queueRes.map(q => (q.domain || "").toLowerCase()).filter(Boolean));
  const allDomains     = new Set([...histDomains, ...queueDomains]);
  const sites          = allDomains.size;
  // user 2026-05-29: SEPARACIÓN MB MANUAL vs AGENTE.
  // Manual = lo que el MB hizo a mano desde el popup (api_usage o agent_actions
  // con details.ui_origin='toolbar_manual').
  // Agente = lo que el bot automatizado disparó (resto de agent_actions).
  const popupOpens     = usageRes.reduce((acc, r) => acc + parseInt(r.by_provider?._popup_opens   || 0, 10), 0);
  const sitesManual    = usageRes.reduce((acc, r) => acc + parseInt(r.by_provider?._sites_analyzed || 0, 10), 0);
  const emailsManualFromUsage = usageRes.reduce((acc, r) => acc + parseInt(r.by_provider?._emails_sent   || 0, 10), 0);
  const mondayManualFromUsage = usageRes.reduce((acc, r) => acc + parseInt(r.by_provider?._monday_pushes || 0, 10), 0);
  // Agente: solo lo que NO es ui_origin=toolbar_manual.
  const isManualAction = (a) => (a.details?.ui_origin || "") === "toolbar_manual";
  // Maxi 2026-06-19 (fix): las tarjetas de arriba ("AGENTE AUTOMATIZADO acting
  // for this MB") deben respetar el filtro USUARIO. Antes contaban agentActions
  // GLOBAL (todos los MBs) → mostraba lo mismo para Maxi/Diego/Agus. El fetch de
  // agent_actions NO filtra por user_email a propósito (el comparador de abajo
  // necesita TODOS los MBs), así que filtramos acá una copia para las stat cards.
  const agentActionsMB = userFilter
    ? agentActions.filter(a => (a.user_email || "").toLowerCase() === userFilter.toLowerCase())
    : agentActions;
  const emailsAgent = agentActionsMB.filter(a => !isManualAction(a) && (a.action === "sent" || a.action === "re_sent" || a.action === "bounce_retry_sent")).length;
  const mondayAgent = agentActionsMB.filter(a => !isManualAction(a) && a.action === "monday_ok").length;
  // Bounce rate del agente (rebotes / emails enviados por el agente).
  const bouncesAgent = Array.isArray(bounceRetries) ? bounceRetries.length : 0;
  const bouncePct = emailsAgent > 0 ? Math.round((bouncesAgent / emailsAgent) * 100) : 0;
  // Totales (compat): los podemos seguir usando en otras vistas.
  const emails = emailsManualFromUsage + emailsAgent;
  const monday = mondayManualFromUsage + mondayAgent;
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

  // 👤 MB MANUAL
  document.getElementById("stat-opens").textContent          = popupOpens.toLocaleString();
  document.getElementById("stat-sites").textContent          = (sitesManual || sites).toLocaleString();
  document.getElementById("stat-emails-manual").textContent  = emailsManualFromUsage.toLocaleString();
  document.getElementById("stat-monday-manual").textContent  = mondayManualFromUsage.toLocaleString();
  // 🤖 AGENTE
  document.getElementById("stat-emails-agent").textContent   = emailsAgent.toLocaleString();
  document.getElementById("stat-monday-agent").textContent   = mondayAgent.toLocaleString();
  document.getElementById("stat-500k-up").textContent        = above500k.toLocaleString();
  document.getElementById("stat-bounces").textContent        = emailsAgent > 0 ? `${bouncePct}%` : "—";

  // Maxi 2026-06-17: Quickview compacto por MB — manual + agente + eficacia.
  renderAdminMBQuickview(usageRes, agentActions, bounceRetries, queueRes);
  renderAdminConversionBySource().catch(() => {});
  renderAdminSourcePerformance().catch(() => {});

  // Combinar fuentes para los renders. Normalizar review_queue rows al shape de historial.
  // CADA review_queue row genera 1 row para el created_by (descubrió/importó) Y
  // 1 row adicional para el validated_by (procesó). Así la "actividad" de cada MB
  // refleja TODO lo que hizo, no solo lo que descubrió.
  const queueAsHist = [];
  queueRes.forEach(q => {
    const baseRow = {
      domain:       q.domain,
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
    };
    // Row 1: para el creador (quien lo metió al pool)
    if (q.created_by) {
      queueAsHist.push({ ...baseRow, media_buyer: q.created_by });
    }
    // Row 2: para el validador (quien lo procesó/mando mail). Filtra agentes y "agent:..."
    // Solo cuenta MBs humanos en el comparador. El agente y procesos automáticos tienen su propia columna.
    const _vb = q.validated_by || "";
    const _isAutoProcess = _vb.startsWith("agent:") || _vb.startsWith("admin_blocklist") || _vb.startsWith("worker_") || _vb === "admin_blocklist_cleanup";
    if (_vb && !_isAutoProcess && _vb !== q.created_by) {
      queueAsHist.push({
        ...baseRow,
        media_buyer: q.validated_by,
        date: q.validated_at || q.created_at,
        created_at: q.validated_at || q.created_at,
        source: "validated", // marca que esta row representa la VALIDACIÓN, no la creación
      });
    }
  });
  const combined = [...histRes, ...queueAsHist];

  // Chart por día
  renderAdminChart(combined, from, to);

  // Comparador de Media Buyers — at-a-glance side-by-side (agente incluído como columna extra)
  renderAdminComparator(combined, usageRes, sessionsRes, agentActions, bounceRetries);
  renderSourcePerformance().catch(() => {});

  // Resumen narrativo por MB (cards con tips para 1:1)
  renderAdminMBSummaries(combined, usageRes, sessionsRes, agentActions);
}

// Maxi 2026-06-17: tablero compacto por MB en la parte superior del admin
// activity tab. Mostramos 1 fila por MB con: Manual (envío hecho a mano desde
// la toolbar), Agente (envío automático del worker), Eficacia (validados/
// enviados). Simple para que Maxi vea quien trabaja cuánto sin escarbar tablas.
// Maxi 2026-06-18: conversion rate por source (apollo/informer/scrape/social/generic)
// Mide respuestas REALES (excluye OOO) de toolbar_response_tracking en últimos 30d.
async function renderAdminConversionBySource() {
  const wrap = document.getElementById("admin-conversion-by-source");
  if (!wrap) return;
  try {
    const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
    const since = new Date(Date.now() - 30 * 86400_000).toISOString();
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_response_tracking?sent_at=gte.${since}&select=source,response_type&limit=10000`,
      { headers }
    );
    if (!res.ok) {
      wrap.innerHTML = '<div style="opacity:.6;font-style:italic">Sin data — la tabla aún no tiene envíos registrados.</div>';
      return;
    }
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) {
      wrap.innerHTML = '<div style="opacity:.6;font-style:italic">Sin envíos en los últimos 30 días. Esperá a que el agente registre actividad.</div>';
      return;
    }
    // Agregar por source
    const SOURCE_META = {
      apollo:   { label: "🎯 Apollo",       color: "#7c3aed" },
      informer: { label: "🔍 Informer",     color: "#0ea5e9" },
      scrape:   { label: "🌐 Sitio (HTML)", color: "#10b981" },
      generic:  { label: "📨 Genérico",     color: "#94a3b8" },
      Facebook: { label: "📘 Facebook",     color: "#1877f2" },
      YouTube:  { label: "▶️ YouTube",       color: "#ff0000" },
      Twitter:  { label: "🐦 Twitter",      color: "#1da1f2" },
      unknown:  { label: "❓ Unknown",      color: "#64748b" },
    };
    const counts = new Map();
    for (const r of rows) {
      const src = r.source || "unknown";
      const c = counts.get(src) || { sent: 0, real: 0, ooo: 0 };
      c.sent++;
      if (r.response_type === "real") c.real++;
      if (r.response_type === "ooo")  c.ooo++;
      counts.set(src, c);
    }
    // Ordenar por conversion rate desc
    const sorted = [...counts.entries()]
      .map(([src, c]) => ({ src, ...c, rate: c.sent > 0 ? c.real / c.sent : 0 }))
      .sort((a, b) => b.rate - a.rate);
    const html = sorted.map(r => {
      const meta = SOURCE_META[r.src] || SOURCE_META.unknown;
      const ratePct = (r.rate * 100).toFixed(1);
      const barWidth = Math.min(100, r.rate * 500); // 20% real = 100% bar
      const rateColor = r.rate >= 0.05 ? "#16a34a" : r.rate >= 0.02 ? "#d97706" : "#dc2626";
      return `
        <div style="display:grid;grid-template-columns:120px 1fr 80px 80px;gap:6px;align-items:center;padding:4px 6px;background:#0f172a;border-radius:4px;border-left:3px solid ${meta.color}">
          <div style="color:${meta.color};font-weight:700">${meta.label}</div>
          <div style="background:#1e293b;height:8px;border-radius:4px;overflow:hidden;position:relative">
            <div style="height:100%;width:${barWidth}%;background:${rateColor};border-radius:4px"></div>
          </div>
          <div style="text-align:right;color:${rateColor};font-weight:700">${ratePct}% real</div>
          <div style="text-align:right;color:#94a3b8;font-size:10px">${r.real}/${r.sent} envíos${r.ooo > 0 ? ` · ${r.ooo} OOO` : ""}</div>
        </div>
      `;
    }).join("");
    wrap.innerHTML = html + `
      <div style="font-size:10px;color:#64748b;text-align:center;padding-top:4px">
        Tasa de respuesta REAL (excluye Out-of-Office). El agente usa estos números para rankear qué fuente priorizar.
      </div>
    `;
  } catch (e) {
    wrap.innerHTML = `<div style="opacity:.6;color:#dc2626">Error: ${esc(e.message || String(e))}</div>`;
  }
}

// Maxi 2026-06-19: rendimiento por MOTOR de descubrimiento. Cruza lo que cada fuente
// PUSO en Prospects (toolbar_review_queue.source) con lo ENVIADO (toolbar_sendtrack,
// por dominio). Mayor % = el motor que mejor convierte. AutoGoogle arranca en 0.
async function renderAdminSourcePerformance() {
  const wrap = document.getElementById("admin-source-performance");
  if (!wrap) return;
  try {
    const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
    const [rqRes, stRes] = await Promise.all([
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_review_queue?select=domain,source&limit=50000`, { headers }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_sendtrack?select=domain&limit=50000`, { headers }),
    ]);
    if (!rqRes.ok) { wrap.innerHTML = '<div style="opacity:.6;font-style:italic">Sin data todavía.</div>'; return; }
    const rq = await rqRes.json();
    const st = stRes.ok ? await stRes.json() : [];
    const norm = d => String(d || "").toLowerCase().replace(/^www\./, "");
    const sentSet = new Set((Array.isArray(st) ? st : []).map(r => norm(r.domain)));
    const CAT = (s) => {
      s = (s || "").toLowerCase();
      if (s.includes("autopilot"))  return "🤖 Autopilot";
      if (s.includes("autogoogle")) return "🔎 AutoGoogle";
      if (s.includes("monday"))     return "🔄 Monday";
      if (s.includes("sellers"))    return "📋 sellers.json";
      if (s.includes("manual"))     return "✋ Manual";
      if (s.includes("csv"))        return "📥 CSV";
      return "❓ Otro";
    };
    const byCat = new Map();
    for (const r of (Array.isArray(rq) ? rq : [])) {
      const dom = norm(r.domain); if (!dom) continue;
      const cat = CAT(r.source);
      const c = byCat.get(cat) || { dom: new Set(), sent: new Set() };
      c.dom.add(dom);
      if (sentSet.has(dom)) c.sent.add(dom);
      byCat.set(cat, c);
    }
    const rows = [...byCat.entries()]
      .map(([cat, c]) => ({ cat, total: c.dom.size, sent: c.sent.size, pct: c.dom.size ? (c.sent.size / c.dom.size * 100) : 0 }))
      .sort((a, b) => b.pct - a.pct);
    if (rows.length === 0) { wrap.innerHTML = '<div style="opacity:.6;font-style:italic">Sin prospects todavía — esperá a que los motores carguen.</div>'; return; }
    wrap.innerHTML = rows.map(r => {
      const color = r.pct >= 40 ? "#16a34a" : r.pct >= 20 ? "#d97706" : "#dc2626";
      return `<div style="display:grid;grid-template-columns:120px 1fr 56px 78px;gap:6px;align-items:center;padding:4px 6px;background:#0f172a;border-radius:4px">
        <div style="font-weight:700">${r.cat}</div>
        <div style="background:#1e293b;height:8px;border-radius:4px;overflow:hidden"><div style="height:100%;width:${Math.min(100, r.pct)}%;background:${color};border-radius:4px"></div></div>
        <div style="text-align:right;color:${color};font-weight:700">${r.pct.toFixed(1)}%</div>
        <div style="text-align:right;color:#94a3b8;font-size:10px">${r.sent}/${r.total}</div>
      </div>`;
    }).join("") + `<div style="font-size:10px;color:#64748b;text-align:center;padding-top:4px">% de lo que cada motor puso en Prospects que terminó enviado (cruce con sendtrack por dominio).</div>`;
  } catch (e) {
    wrap.innerHTML = `<div style="opacity:.6;color:#dc2626">Error: ${esc(e.message || String(e))}</div>`;
  }
}

function renderAdminMBQuickview(usageRes, agentActions, bounceRetries, queueRes) {
  const wrap = document.getElementById("admin-mb-quickview");
  if (!wrap) return;
  const MB_INFO = [
    { email: "mgargiulo@adeqmedia.com", name: "Maxi",  color: "#10b981" },
    { email: "sales@adeqmedia.com",     name: "Agus",  color: "#ec4899" },
    { email: "dhorovitz@adeqmedia.com", name: "Diego", color: "#a855f7" },
  ];

  const isManualAction = (a) => (a.details?.ui_origin || "") === "toolbar_manual";

  const rowsHtml = MB_INFO.map(mb => {
    const emailLower = mb.email.toLowerCase();

    // Manual emails: del api_usage._emails_sent + agent_actions con ui_origin=toolbar_manual
    const manualFromUsage = (usageRes || []).filter(u => (u.user_email || "").toLowerCase() === emailLower)
      .reduce((acc, r) => acc + parseInt(r.by_provider?._emails_sent || 0, 10), 0);
    const manualFromActions = (agentActions || []).filter(a => (a.user_email || "").toLowerCase() === emailLower && isManualAction(a) && (a.action === "sent" || a.action === "monday_ok")).length;
    const manualEmails = manualFromUsage + manualFromActions;

    // Agente: agent_actions sin ui_origin=toolbar_manual
    const agentEmails = (agentActions || []).filter(a =>
      (a.user_email || "").toLowerCase() === emailLower &&
      !isManualAction(a) &&
      (a.action === "sent" || a.action === "re_sent" || a.action === "bounce_retry_sent")
    ).length;

    // Bounces detectados para este MB
    const bounces = (bounceRetries || []).filter(b => (b.mb_email || "").toLowerCase() === emailLower).length;
    const bouncePct = agentEmails > 0 ? Math.round((bounces / agentEmails) * 100) : 0;

    // Validados (procesados desde Prospects) por este MB
    const validated = (queueRes || []).filter(q => (q.validated_by || "").toLowerCase() === emailLower).length;
    const totalSends = manualEmails + agentEmails;
    const efficacy = totalSends > 0 ? Math.round((validated / totalSends) * 100) : 0;

    // Color del badge eficacia
    const effColor = efficacy >= 70 ? "#16a34a" : efficacy >= 40 ? "#d97706" : "#dc2626";
    const bounceColor = bouncePct >= 10 ? "#dc2626" : bouncePct >= 5 ? "#d97706" : "#16a34a";

    return `
      <div style="display:grid;grid-template-columns:80px 1fr 1fr 90px 90px;gap:8px;align-items:center;padding:6px 8px;background:#0f172a;border-radius:4px;border-left:3px solid ${mb.color}">
        <div style="color:${mb.color};font-weight:700">${mb.name}</div>
        <div title="Manual desde la toolbar (analysis tab)">
          👤 <strong>${manualEmails}</strong> <span style="opacity:.6">manual</span>
        </div>
        <div title="Agente automatizado (sent + re_sent + bounce_retry_sent)">
          🤖 <strong>${agentEmails}</strong> <span style="opacity:.6">agente</span>
        </div>
        <div title="% rebote = bounces / agentEmails" style="color:${bounceColor};text-align:center;font-weight:600">
          🚫 ${bounces} (${bouncePct}%)
        </div>
        <div title="Eficacia = validated / total enviados" style="color:${effColor};text-align:center;font-weight:700">
          ✅ ${efficacy}%
        </div>
      </div>
    `;
  }).join("");

  wrap.innerHTML = rowsHtml + `
    <div style="font-size:10px;color:#64748b;text-align:center;padding-top:4px">
      👤=manual desde toolbar · 🤖=agente automático · 🚫=rebotes detectados · ✅=eficacia (validated/sent)
    </div>
  `;
}

// ── Helper compartido: agrega métricas por user (usado por Comparator + Summaries) ──
// Normaliza display names → emails (h.media_buyer="Diego" → "dhorovitz@adeqmedia.com")
const NAME_TO_EMAIL = {
  "diego":     "dhorovitz@adeqmedia.com",
  "dhorovitz": "dhorovitz@adeqmedia.com",
  "dhorovits": "dhorovitz@adeqmedia.com",
  "agus":      "sales@adeqmedia.com",
  "agustina":  "sales@adeqmedia.com",
  "sales":     "sales@adeqmedia.com",
  "max":       "mgargiulo@adeqmedia.com",
  "maxi":      "mgargiulo@adeqmedia.com",
  "maximiliano": "mgargiulo@adeqmedia.com",
  "mgargiulo": "mgargiulo@adeqmedia.com",
  "maximiliano gargiulo": "mgargiulo@adeqmedia.com",
};
function _normalizeUserKey(raw) {
  if (!raw) return "unknown";
  const lower = String(raw).toLowerCase().trim();
  if (lower.includes("@")) return lower; // ya es email
  if (NAME_TO_EMAIL[lower]) return NAME_TO_EMAIL[lower];
  // Match WORD-BOUNDARY (no substring): "max" no debe matchear "maximus", ni
  // "diego" matchear "dieguito". Solo si la key aparece como palabra entera.
  for (const k of Object.keys(NAME_TO_EMAIL)) {
    const re = new RegExp(`(^|\\W)${k}(\\W|$)`, "i");
    if (re.test(lower)) return NAME_TO_EMAIL[k];
  }
  return lower;
}

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
    const u = _normalizeUserKey(h.user_email || h.created_by || h.media_buyer);
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
    // Cada row de historial es UN push a Monday del MB (siempre se crea al pushear).
    // Y si tiene email guardado, también fue un envío.
    o.monday++;
    if (h.email && /\@/.test(h.email)) o.emails++;
  });
  usage.forEach(r => {
    const o = ensure(_normalizeUserKey(r.user_email));
    // Tomamos MAX del usage-counter vs el ya contado de historial (no sumar — duplica).
    const usageEmails = parseInt(r.by_provider?._emails_sent  || 0, 10);
    const usageMonday = parseInt(r.by_provider?._monday_pushes || 0, 10);
    if (usageEmails > o.emails) o.emails = usageEmails;
    if (usageMonday > o.monday) o.monday = usageMonday;
    o.claude += parseInt(r.by_provider?.anthropic || 0, 10);
  });
  sessions.forEach(s => {
    const o = ensure(_normalizeUserKey(s.user_email));
    if (s.kind === "autopilot") o.apSec   += s.duration_sec || 0;
    if (s.kind === "popup")     o.popupSec += s.duration_sec || 0;
  });
  return byUser;
}

// ── Comparador horizontal: tabla con métricas en filas, MBs en columnas ──
// Pensado para "ver de un vistazo quién está produciendo y quién no".
function renderAdminComparator(historial, usage, sessions, agentActions = [], bounceRetries = []) {
  const wrap = document.getElementById("admin-mb-comparator");
  if (!wrap) return;
  const byUser = _aggregateByUser(historial, usage, sessions);
  // Bounces por MB — cada row en toolbar_bounce_retries = 1 bounce atribuido
  // al MB que mandó el email original. % bounce = bounces / emails enviados.
  const bouncesByUser = {};
  (bounceRetries || []).forEach(r => {
    const u = (r.mb_email || "").toLowerCase();
    if (!u) return;
    bouncesByUser[u] = (bouncesByUser[u] || 0) + 1;
  });
  // Agregar AGENTE como un "MB" más — agrega métricas de agent_actions sent.
  const agentBucket = {
    sites: agentActions.length,
    autopilotSites: agentActions.length, manualSites: 0,
    geos: {}, categories: {},
    above500k: 0, below500k: 0,
    emails: agentActions.length,
    monday: agentActions.filter(a => a.details?.monday_item_id || true).length, // todos los sent llegan a Monday
    claude: agentActions.filter(a => a.details?.source === "claude").length,
    apSec: 0, popupSec: 0,
    isAgent: true,
  };
  agentActions.forEach(a => {
    const g = (a.details?.geo || "").trim();
    if (g) agentBucket.geos[g] = (agentBucket.geos[g] || 0) + 1;
    const t = parseInt(a.details?.traffic || 0, 10);
    if (t >= 500000) agentBucket.above500k++;
    else if (t > 0)  agentBucket.below500k++;
  });

  // Columnas: 3 MBs + 1 columna Agent (si hay actividad o no, siempre se muestra)
  const mbs = TEAM_EMAILS.map(e => e.toLowerCase()).map(u => ({ user: u, ...byUser.get(u), bounces: bouncesByUser[u] || 0 }));
  mbs.push({ user: "agent", ...agentBucket, bounces: 0 });

  const shortName = (e) => ({
    "mgargiulo@adeqmedia.com": "Maxi",
    "dhorovitz@adeqmedia.com": "Diego",
    "sales@adeqmedia.com":     "Agus",
    "agent":                   "🤖 Agent",
  })[e] || e.split("@")[0];

  const fmtTime = (sec) => sec >= 3600 ? `${Math.floor(sec/3600)}h${Math.round((sec%3600)/60)}m` : `${Math.round(sec/60)}m`;
  const topKey  = (obj) => Object.entries(obj).sort((a,b) => b[1]-a[1])[0]?.[0] || "—";
  const pctConv = (mb) => mb.sites > 0 ? Math.round((mb.monday / mb.sites) * 100) : 0;

  // Definición de filas: { label, group, get(mb) → value, fmt(value) → display, isBest higher/lower, colorFn(value) → class }
  const groups = [
    {
      title: "USO MANUAL (👤 lo que hace el MB)",
      rows: [
        { label: "Aperturas toolbar",      get: m => m.opens || 0, fmt: v => v.toLocaleString() },
        { label: "URLs analizadas",        get: m => m.sitesManual || m.sites, fmt: v => v.toLocaleString() },
        { label: "Analysis / Prospects",   get: m => (m.manualSites || 0) + (m.autopilotSites || 0), fmt: (_, m) => `${m.manualSites || 0} / ${m.autopilotSites || 0}`, _help: "Analysis = manual desde Analysis tab. Prospects = pickeados del review_queue." },
        { label: "Autopilot time",         get: m => m.apSec, fmt: v => fmtTime(v) },
      ],
    },
    {
      title: "QUALITY",
      rows: [
        { label: "% +500K visits", get: m => m.sites ? Math.round((m.above500k / m.sites) * 100) : 0, fmt: v => `${v}%` },
        { label: "Top GEO",        get: m => Object.values(m.geos).reduce((a,b)=>a+b,0), fmt: (_, m) => topKey(m.geos), noBar: true },
        { label: "Top Category",   get: m => Object.values(m.categories).reduce((a,b)=>a+b,0), fmt: (_, m) => topKey(m.categories), noBar: true },
      ],
    },
    {
      title: "OUTREACH (👤 MANUAL del MB)",
      rows: [
        { label: "Emails — manual",    get: m => m.emailsManual || 0, fmt: v => v.toLocaleString() },
        { label: "Monday — manual",    get: m => m.mondayManual || 0, fmt: v => v.toLocaleString() },
      ],
    },
    {
      title: "OUTREACH (🤖 AGENTE automatizado)",
      rows: [
        { label: "Emails — agente",    get: m => m.emailsAgent || 0, fmt: v => v.toLocaleString() },
        { label: "Monday — agente",    get: m => m.mondayAgent || 0, fmt: v => v.toLocaleString() },
        { label: "Conv. push/site",    get: m => pctConv(m), fmt: v => `${v}%`, color: v => v >= 5 ? "good" : v >= 2 ? "warn" : "bad" },
        { label: "% bounce",           get: m => m.emails > 0 ? Math.round((m.bounces / m.emails) * 100) : 0, fmt: (v, m) => m.emails > 0 ? `${v}% (${m.bounces})` : "—", color: v => v <= 3 ? "good" : v <= 8 ? "warn" : "bad", noBar: true },
      ],
    },
    {
      title: "EFFICIENCY",
      rows: [
        { label: "Claude pitches", get: m => m.claude, fmt: v => v.toLocaleString() },
        { label: "Mails / Push",   get: m => m.monday ? Math.round((m.emails / m.monday) * 10) / 10 : 0, fmt: v => v ? v.toFixed(1) : "—" },
      ],
    },
  ];

  const html = [];
  html.push(`<div class="mbc-row mbc-header"><div class="mbc-cell mbc-row-label">Metric</div>${mbs.map(m => `<div class="mbc-cell"><strong>${shortName(m.user)}</strong></div>`).join("")}</div>`);
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

// ── Source Performance — engagement rolling 30d por MB y source ──
// Lee toolbar_source_performance (agregada por el worker 1×/día) y muestra
// por MB qué source (apollo/informer/scrape/generic/manual) está produciendo
// mejor open_rate y bounce_rate. Usado por el dynamic ranker en el worker
// para auto-ajustar el pick de email source.
async function renderSourcePerformance() {
  let wrap = document.getElementById("admin-source-performance");
  if (!wrap) {
    // Auto-inyectar el contenedor justo después del comparator si no existe.
    const cmpEl = document.getElementById("admin-mb-comparator");
    if (!cmpEl) return;
    wrap = document.createElement("div");
    wrap.id = "admin-source-performance";
    wrap.style.cssText = "margin-top:16px;padding:10px;background:#0f172a;border-radius:6px";
    cmpEl.parentNode.insertBefore(wrap, cmpEl.nextSibling);
  }
  wrap.innerHTML = `<div style="color:#94a3b8;font-size:11px">Loading source performance…</div>`;
  try {
    const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_source_performance?window_days=eq.30&select=mb_email,source,sent,opens,bounces,open_rate,bounce_rate,score,computed_at&order=mb_email.asc,score.desc`,
      { headers }
    );
    if (!res.ok) {
      wrap.innerHTML = `<div style="color:#f87171;font-size:11px">Source perf HTTP ${res.status} (¿la tabla aún no se creó? Corré sql/2026-05-19_source_performance.sql)</div>`;
      return;
    }
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) {
      wrap.innerHTML = `<div style="color:#94a3b8;font-size:11px">Aún no hay datos de source performance. El job corre 1×/día — esperá hasta mañana o forzá con <code>maybeRunSourcePerformanceAggregate</code>.</div>`;
      return;
    }
    // Group by mb_email
    const byMb = {};
    rows.forEach(r => {
      const m = r.mb_email || "_global";
      if (!byMb[m]) byMb[m] = [];
      byMb[m].push(r);
    });
    const SOURCES = ["apollo", "informer", "scrape", "generic", "manual"];
    const computedAt = rows[0]?.computed_at ? new Date(rows[0].computed_at).toLocaleString() : "—";
    const cellFor = (r) => {
      if (!r || !r.sent) return `<td style="color:#475569;text-align:center">—</td>`;
      const op = (parseFloat(r.open_rate) * 100).toFixed(0);
      const bo = (parseFloat(r.bounce_rate) * 100).toFixed(0);
      const opColor = op >= 25 ? "#10b981" : op >= 10 ? "#f59e0b" : "#f87171";
      const boColor = bo <= 3 ? "#10b981" : bo <= 8 ? "#f59e0b" : "#f87171";
      const lowSample = r.sent < 50 ? " <span title='sample chico, ranker usa default' style='color:#f59e0b'>·</span>" : "";
      return `<td style="text-align:center;padding:3px 6px;font-size:10px">
        <div><span style="color:${opColor};font-weight:600">${op}%</span> open${lowSample}</div>
        <div><span style="color:${boColor}">${bo}%</span> bounce</div>
        <div style="color:#64748b;font-size:9px">n=${r.sent}</div>
      </td>`;
    };
    let html = `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
      <strong style="color:#e2e8f0;font-size:12px">🧠 Source Performance (rolling 30d)</strong>
      <span style="color:#64748b;font-size:10px">computed: ${computedAt}</span>
    </div>
    <div style="color:#94a3b8;font-size:10px;margin-bottom:8px">
      El agente usa estos rates para elegir qué source priorizar por MB.
      <span style="color:#f59e0b">·</span> = sample &lt; 50 (ranker default).
      ε-greedy 10% explora siempre.
    </div>
    <table style="width:100%;border-collapse:collapse;font-size:11px">
      <thead>
        <tr style="color:#94a3b8">
          <th style="text-align:left;padding:4px 6px">MB</th>
          ${SOURCES.map(s => `<th style="text-align:center;padding:4px 6px;text-transform:capitalize">${s}</th>`).join("")}
        </tr>
      </thead>
      <tbody>`;
    const mbOrder = Object.keys(byMb).sort((a, b) => a === "_global" ? -1 : b === "_global" ? 1 : a.localeCompare(b));
    mbOrder.forEach(mb => {
      const shortName = mb === "_global" ? "🌐 Global" : mb.split("@")[0];
      html += `<tr style="border-top:1px solid #1e293b">
        <td style="padding:4px 6px;color:#e2e8f0;font-weight:600">${esc(shortName)}</td>
        ${SOURCES.map(s => cellFor(byMb[mb].find(r => r.source === s))).join("")}
      </tr>`;
    });
    html += `</tbody></table>`;
    wrap.innerHTML = html;
  } catch (e) {
    wrap.innerHTML = `<div style="color:#f87171;font-size:11px">Source perf error: ${esc(e.message)}</div>`;
  }
}

// ── Resumen narrativo por MB ──────────────────────────────
// Genera cards con frases en lenguaje natural sobre la actividad de cada MB.
// Pensado para que el admin lea durante un 1:1 ("mirá tu resumen").
function renderAdminMBSummaries(historial, usage, sessions, agentActions = []) {
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
    const u = _normalizeUserKey(h.media_buyer || h.user_email || h.created_by);
    if (!byUser.has(u)) byUser.set(u, { sites: 0, autopilotSites: 0, geos: {}, categories: {}, above500k: 0, below500k: 0, emails: 0, monday: 0, claude: 0, apSec: 0, popupSec: 0 });
    const o = byUser.get(u);
    o.sites++;
    if (h.source === "autopilot") o.autopilotSites++;
    const g = (h.geo || "").trim();
    if (g) o.geos[g] = (o.geos[g] || 0) + 1;
    const c = (h.category || "").trim();
    if (c) o.categories[c] = (o.categories[c] || 0) + 1;
    const traffic = parseInt(h.page_views || h.raw_visits || h.traffic || 0, 10);
    if (traffic >= 500000) o.above500k++;
    else if (traffic > 0)  o.below500k++;
    // Cada row historial = 1 push Monday + 1 send (si hay email)
    o.monday++;
    if (h.email && /\@/.test(h.email)) o.emails++;
  });
  // user 2026-05-29: nueva separación MB MANUAL vs AGENTE.
  // - emailsManual / mondayManual: lo que el MB hizo a mano desde el popup
  //   (agent_actions con details.ui_origin='toolbar_manual' + api_usage popup).
  // - emailsAgent / mondayAgent: lo que el agente automatizado hizo en su nombre.
  // Los totales (emails/monday) se computan sumando ambos para retro-compat.
  usage.forEach(r => {
    const u = _normalizeUserKey(r.user_email);
    if (!byUser.has(u)) byUser.set(u, { sites: 0, autopilotSites: 0, geos: {}, categories: {}, above500k: 0, below500k: 0, emails: 0, monday: 0, emailsManual: 0, emailsAgent: 0, mondayManual: 0, mondayAgent: 0, opens: 0, sitesManual: 0, claude: 0, apSec: 0, popupSec: 0 });
    const o = byUser.get(u);
    // SUMAR (api_usage tiene 1 row por día, hay que sumar todos los días)
    o.opens        += parseInt(r.by_provider?._popup_opens    || 0, 10);
    o.sitesManual  += parseInt(r.by_provider?._sites_analyzed || 0, 10);
    o.emailsManual += parseInt(r.by_provider?._emails_sent    || 0, 10);
    o.mondayManual += parseInt(r.by_provider?._monday_pushes  || 0, 10);
    o.claude       += parseInt(r.by_provider?.anthropic || 0, 10);
  });
  sessions.forEach(s => {
    const u = _normalizeUserKey(s.user_email);
    if (!byUser.has(u)) byUser.set(u, { sites: 0, autopilotSites: 0, geos: {}, categories: {}, above500k: 0, below500k: 0, emails: 0, monday: 0, emailsManual: 0, emailsAgent: 0, mondayManual: 0, mondayAgent: 0, claude: 0, apSec: 0, popupSec: 0 });
    const o = byUser.get(u);
    if (s.kind === "autopilot") o.apSec += s.duration_sec || 0;
    if (s.kind === "popup")     o.popupSec += s.duration_sec || 0;
  });
  // Agent actions: separar MANUAL (ui_origin='toolbar_manual') de AGENTE automatizado.
  agentActions.forEach(a => {
    const u = _normalizeUserKey(a.user_email);
    if (!byUser.has(u)) byUser.set(u, { sites: 0, autopilotSites: 0, geos: {}, categories: {}, above500k: 0, below500k: 0, emails: 0, monday: 0, emailsManual: 0, emailsAgent: 0, mondayManual: 0, mondayAgent: 0, claude: 0, apSec: 0, popupSec: 0 });
    const o = byUser.get(u);
    const isManual = (a.details?.ui_origin || "") === "toolbar_manual";
    if (a.action === "sent") {
      // Los manuales ya fueron contados desde api_usage._emails_sent — evitar double-count.
      if (!isManual) o.emailsAgent++;
    }
    if (a.action === "monday_ok") {
      // Los manuales ya fueron contados desde api_usage._monday_pushes.
      if (!isManual) o.mondayAgent++;
    }
    if (a.details?.source === "claude") o.claude++;
  });
  // Totales (compat con el resto del panel)
  for (const o of byUser.values()) {
    o.emails = o.emailsManual + o.emailsAgent;
    o.monday = o.mondayManual + o.mondayAgent;
  }

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
      lines.push("No activity logged in this period.");
    } else {
      lines.push(`Analyzed <strong>${s.sites}</strong> sites (${s.autopilotSites} via autopilot, ${s.sites - s.autopilotSites} manual).`);
      if (topGeoArr.length) lines.push(`Geographic focus: <strong>${topGeoStr}</strong>.`);
      if (topCatArr.length) lines.push(`Top analyzed categories: <strong>${topCatStr}</strong>.`);
      lines.push(`Calidad de leads: <strong>${s.above500k}</strong> sitios +500K vs <strong>${s.below500k}</strong> chicos.`);
      if (s.emails > 0 || s.monday > 0) {
        lines.push(`Outreach: <strong>${s.emails}</strong> emails enviados, <strong>${s.monday}</strong> pushes a Monday (conv: <strong>${convPct}%</strong>).`);
      } else {
        lines.push(`⚠️ Zero emails sent and zero Monday pushes — analyzed but didn't advance leads.`);
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
    else if (key === "g" && !inField) btn = document.getElementById("btn-generate-pitch") || document.getElementById("btn-autopush-prepare");
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
/* withMinDuration removida (B2) — código muerto */

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
  else if (pct >= 75) el.classList.add("usage-danger");
  else if (pct >= 50) el.classList.add("usage-warning");
  el.textContent = `SW: ${used.toLocaleString()} / ${limit.toLocaleString()}`;
  el.title = `SimilarWeb this calendar month (${period || "—"}): ${used.toLocaleString()} of ${limit.toLocaleString()} (${pct.toFixed(1)}%). Shared across team. Hard stop at limit.`;
}

// Apollo monthly counter — sumado de todos los MBs vía toolbar_config.apollo_calls_month
// Version check — usa chrome.runtime.requestUpdateCheck (Chrome consulta al
// Web Store oficial). Si hay update aprobado y aplicable → 🔴 update. Fallback
// a GitHub solo si la API nativa falla (modo dev sin store). Click → re-check.
async function checkExtensionVersion() {
  const el = document.getElementById("version-badge");
  if (!el) return;
  const localVer = chrome.runtime.getManifest().version;
  el.textContent = `v${localVer} ⏳`;
  el.style.background = "rgba(148,163,184,0.15)";
  el.style.color = "#94a3b8";

  // 1° Chrome Web Store (fuente de verdad)
  const cwsResult = await _checkChromeWebStoreUpdate();
  if (cwsResult.ok) {
    if (cwsResult.hasUpdate) {
      const remoteVer = cwsResult.version || "?";
      el.textContent = `v${localVer} 🔴 update`;
      el.style.background = "rgba(239,68,68,0.15)";
      el.style.color = "#f87171";
      el.title = `Update disponible en Chrome Web Store: v${remoteVer}. Chrome la instala sola en las próximas horas, o forzá en chrome://extensions → Update.`;
      if (!window._versionUpdateToastShown) {
        window._versionUpdateToastShown = true;
        if (typeof showToast === "function") showToast(`🔴 Update v${remoteVer} disponible en Chrome Web Store (vos tenés v${localVer})`, "warn", 8000);
      }
    } else {
      el.textContent = `v${localVer} ✓`;
      el.style.background = "rgba(52,211,153,0.15)";
      el.style.color = "#34d399";
      el.title = `Última versión instalada (v${localVer}) — Chrome Web Store al día`;
    }
    return;
  }

  // 2° Fallback a GitHub (modo dev / API throttled / no instalada desde store)
  try {
    const res = await fetch("https://raw.githubusercontent.com/mgargiulo-cell/adeq-toolbar/main/manifest.json", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const remote = await res.json();
    const remoteVer = remote.version || "0.0.0";
    const cmp = _semverCompare(localVer, remoteVer);
    if (cmp >= 0) {
      el.textContent = `v${localVer} ✓`;
      el.style.background = "rgba(52,211,153,0.15)";
      el.style.color = "#34d399";
      el.title = `Latest version installed (v${localVer}) — comparado vs GitHub (fallback)`;
    } else {
      el.textContent = `v${localVer} ⚠ github`;
      el.style.background = "rgba(251,191,36,0.15)";
      el.style.color = "#fbbf24";
      el.title = `v${remoteVer} en GitHub (todavía no en Chrome Web Store). Esperá review o cargá ZIP local.`;
    }
  } catch (e) {
    el.textContent = `v${localVer} ?`;
    el.style.background = "rgba(251,191,36,0.15)";
    el.style.color = "#fbbf24";
    el.title = `No se pudo verificar update: ${e.message}`;
  }
}

// Wrapper: chrome.runtime.requestUpdateCheck → {ok, hasUpdate, version}.
// status posibles: "update_available" (hay) | "no_update" (al día) | "throttled" (rate-limit).
function _checkChromeWebStoreUpdate() {
  return new Promise((resolve) => {
    try {
      if (!chrome?.runtime?.requestUpdateCheck) return resolve({ ok: false });
      chrome.runtime.requestUpdateCheck((status, details) => {
        if (chrome.runtime.lastError) return resolve({ ok: false });
        if (status === "update_available") return resolve({ ok: true, hasUpdate: true, version: details?.version });
        if (status === "no_update")        return resolve({ ok: true, hasUpdate: false });
        return resolve({ ok: false }); // throttled → fallback
      });
    } catch { resolve({ ok: false }); }
  });
}
function _semverCompare(a, b) {
  const pa = a.split(".").map(Number);
  const pb = b.split(".").map(Number);
  for (let i = 0; i < 3; i++) {
    const x = pa[i] || 0, y = pb[i] || 0;
    if (x > y) return 1;
    if (x < y) return -1;
  }
  return 0;
}

async function refreshApolloFooterCounter() {
  const el = document.getElementById("apollo-monthly-counter");
  if (!el || !state.accessToken) return;
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=in.(apollo_calls_month,apollo_calls_month_period,apollo_monthly_limit)&select=key,value`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    const map = {};
    rows.forEach(r => { map[r.key] = r.value; });
    const period = new Date().toISOString().slice(0, 7);
    const sameMonth = (map.apollo_calls_month_period || "").slice(0, 7) === period;
    const used = sameMonth ? parseInt(map.apollo_calls_month || "0", 10) : 0;
    const limit = parseInt(map.apollo_monthly_limit || "2400", 10);
    const pct = limit > 0 ? (used / limit) * 100 : 0;
    el.classList.remove("usage-warning", "usage-danger", "usage-reached");
    if (pct >= 100)     el.classList.add("usage-reached");
    else if (pct >= 75) el.classList.add("usage-danger");
    else if (pct >= 50) el.classList.add("usage-warning");
    el.textContent = `Apollo: ${used.toLocaleString()} / ${limit.toLocaleString()}`;
    el.title = `Apollo unlocks this month (${period}): ${used} of ${limit} (${pct.toFixed(1)}%). Stops at limit — fallback a scraping.`;
    // Trigger banner si ≥75%
    if (pct >= 75) _showApolloWarningBanner({ used, limit, period, pct });
  } catch {}
}

async function refreshSerperFooterCounter() {
  const el = document.getElementById("serper-monthly-counter");
  if (!el || !state.accessToken) return;
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=in.(autogoogle_serper_used,autogoogle_serper_period)&select=key,value`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    const map = {};
    rows.forEach(r => { map[r.key] = r.value; });
    const period = new Date().toISOString().slice(0, 7);
    const sameMonth = (map.autogoogle_serper_period || "").slice(0, 7) === period;
    const used = sameMonth ? parseInt(map.autogoogle_serper_used || "0", 10) : 0;
    const limit = 2500;
    const pct = limit > 0 ? (used / limit) * 100 : 0;
    el.classList.remove("usage-warning", "usage-danger", "usage-reached");
    if (pct >= 100)     el.classList.add("usage-reached");
    else if (pct >= 75) el.classList.add("usage-danger");
    else if (pct >= 50) el.classList.add("usage-warning");
    el.textContent = `AutoGoogle: ${used.toLocaleString()} / ${limit.toLocaleString()}`;
    el.title = `Búsquedas Serper (AutoGoogle) este mes (${period}): ${used} de ${limit} (${pct.toFixed(1)}%). Plan free; el pacing reparte por días restantes.`;
  } catch {}
}

// Estado in-memory para combinar SW + Apollo en un solo banner.
// Cada provider reporta su {pct, used, limit, period}; el banner muestra el más crítico.
const _capState = { sw: null, apollo: null };
function _refreshCombinedCapBanner() {
  const banner = document.getElementById("rapidapi-cap-banner");
  const title  = document.getElementById("cap-banner-title");
  const detail = document.getElementById("cap-banner-detail");
  const icon   = document.getElementById("cap-banner-icon");
  if (!banner || !title) return;

  // Pick el provider con mayor pct (entre los que pasan threshold 75%)
  const candidates = [];
  if (_capState.sw     && _capState.sw.pct     >= 75) candidates.push({ name: "SimilarWeb", ..._capState.sw });
  if (_capState.apollo && _capState.apollo.pct >= 75) candidates.push({ name: "Apollo",     ..._capState.apollo });
  if (candidates.length === 0) { banner.style.display = "none"; return; }

  candidates.sort((a, b) => b.pct - a.pct);
  const top = candidates[0];
  const others = candidates.slice(1);

  banner.classList.remove("cap-warning", "cap-danger", "cap-reached");
  if (top.pct >= 100)     { banner.classList.add("cap-reached"); icon.textContent = "⛔"; }
  else if (top.pct >= 90) { banner.classList.add("cap-danger");  icon.textContent = "⚠️"; }
  else                    { banner.classList.add("cap-warning"); icon.textContent = "⚠️"; }

  const reached = top.pct >= 100;
  title.textContent = reached
    ? `${top.name} cap reached — paused`
    : `${top.name} at ${top.pct.toFixed(0)}% of monthly cap`;
  let detailText = ` — ${top.used.toLocaleString()} / ${top.limit.toLocaleString()} in ${top.period}.`;
  if (others.length) {
    detailText += ` (also ${others.map(o => `${o.name} ${o.pct.toFixed(0)}%`).join(", ")})`;
  }
  detail.textContent = detailText;
  banner.style.display = "flex";
}

function _showApolloWarningBanner({ used, limit, period, pct }) {
  _capState.apollo = { used, limit, period, pct };
  _refreshCombinedCapBanner();
}

// ---- RapidAPI monthly usage banner (3 niveles: 50% / 80% / 100%) ----
function renderRapidApiUsageBanner({ used, limit, period, scope } = {}) {
  const banner = document.getElementById("rapidapi-cap-banner");
  if (!banner || used == null || limit == null) return;
  const pct = limit > 0 ? (used / limit) * 100 : 0;

  // Update shared state — el banner combinado decide qué mostrar
  _capState.sw = { used, limit, period: period || new Date().toISOString().slice(0, 7), pct, scope };
  // Si pct < 50% para SW, lo limpiamos para que Apollo solo se muestre
  if (pct < 50) _capState.sw = null;
  _refreshCombinedCapBanner();
  // Caída temprana — el resto era código viejo que pisaba el banner combinado
  return;

  // (legacy unreachable — mantenemos por si necesitamos rollback)
  // eslint-disable-next-line no-unreachable
  const icon   = document.getElementById("cap-banner-icon");
  const title  = document.getElementById("cap-banner-title");
  const detail = document.getElementById("cap-banner-detail");
  banner.classList.remove("cap-warning", "cap-danger", "cap-reached");
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
    title.textContent = `${scopeLabel}: monthly limit reached`;
    detail.textContent = ` — ${usedStr} / ${limitStr} in ${periodStr}. Traffic lookups paused until next month.`;
  } else if (pct >= 80) {
    banner.classList.add("cap-danger");
    icon.textContent  = "🔶";
    title.textContent = `${scopeLabel} at ${Math.round(pct)}% — heads up`;
    detail.textContent = ` — ${usedStr} / ${limitStr} en ${periodStr}. Quedan pocas consultas; el autopilot puede cortarse pronto.`;
  } else {
    banner.classList.add("cap-warning");
    icon.textContent  = "⚠️";
    title.textContent = `${scopeLabel} al ${Math.round(pct)}%`;
    detail.textContent = ` — ${usedStr} / ${limitStr} in ${periodStr}. Heads up.`;
  }
}

// Llamado por apiProxy cuando se llega al cap (100%)
/* showRapidApiCapBanner removida (B2) — código muerto */

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
let cascadePage = 1;                      // Maxi 2026-06-22: paginación de "Find similar websites"
const CASCADE_PAGE_SIZE = 30;             // máx 30 por página + nav 1-2-3
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
  refreshApolloFooterCounter();
  refreshSerperFooterCounter();
  checkExtensionVersion();
  // Maxi 2026-07-03 perf: no pollear footer counters (Supabase) cuando el side-panel
  // está oculto. Al reabrirlo, el próximo tick (≤60s) refresca. Ahorra queries en
  // background × 3 MBs con la toolbar cerrada pero la sesión viva.
  setInterval(() => { if (document.visibilityState !== "hidden") refreshUsage(); }, 60_000);
  setInterval(() => { if (document.visibilityState !== "hidden") refreshApolloFooterCounter(); }, 60_000);
  // Re-check version cada 30 min en background
  setInterval(checkExtensionVersion, 30 * 60_000);
  document.getElementById("version-badge")?.addEventListener("click", checkExtensionVersion);

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
  // user 2026-05-29: instrumentación nueva del panel admin — cuenta aperturas
  // de la toolbar (1 por load). Es la métrica "cuánto la usa".
  incrementUserDailyCounter(state.accessToken, state.loginEmail, "opens").catch(() => {});

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

// ── Re-fetch ligero del HTML de un dominio (para Prospects card "🔍 Data") ──
// Trae title, language (html lang + heurística), ad_networks. NO ejecuta JS.
// Usado para que Prospects pueda re-analizar leads igual que Analysis sin tab abierta.
async function _fetchPageMetaForProspect(domain) {
  if (!domain) return null;
  try {
    const url = /^https?:\/\//.test(domain) ? domain : `https://${domain}`;
    const res = await fetch(url, {
      method: "GET",
      headers: { "User-Agent": "Mozilla/5.0 (compatible; ADEQbot/1.0)" },
      signal: AbortSignal.timeout(8000),
      redirect: "follow",
      mode: "cors",
    }).catch(() => null);
    if (!res || !res.ok) return null;
    const html = await res.text();
    const title = (html.match(/<title[^>]*>([^<]{1,200})<\/title>/i) || [])[1]?.trim() || "";
    const htmlLang = (html.match(/<html[^>]+lang=["']([a-z]{2})/i) || [])[1] || "";
    const ogLocale = (html.match(/<meta[^>]+property=["']og:locale["'][^>]+content=["']([a-z]{2})/i) || [])[1] || "";
    // Adnets (mismo set que el worker)
    const ADN = [
      ["Sparteo", /sparteo\.com/i], ["Seedtag", /seedtag\.com/i], ["Taboola", /taboola/i],
      ["Missena", /missena\.com/i], ["Viads", /viads\.com|viads\.io/i], ["MGID", /mgid\.com/i],
      ["Clever Advertising", /clever-advertising|cleveradvertising/i], ["Vidoomy", /vidoomy\.com/i],
      ["Vidverto", /vidverto\.com/i], ["Ezoic", /ezoic\.com|ezojs\.com|ez\.ai/i],
      ["Clickio", /clickio\.com|clickio\.net/i], ["Optad360", /optad360\.com/i],
      ["Snigel", /snigel\.com/i],
    ];
    const adNetworks = ADN.filter(([_, re]) => re.test(html)).map(([n]) => n);
    // Lang resolution: html → og → text heuristic → tld
    let language = (htmlLang || ogLocale || "").toLowerCase().split(/[-_]/)[0];
    if (!["es","en","it","pt","ar"].includes(language)) {
      const sample = (title + " " + html.replace(/<[^>]+>/g, " ").substring(0, 3000)).toLowerCase();
      if (/[؀-ۿ]/.test(html)) language = "ar";
      // Maxi 2026-07-08: solo chars distintivos (ñ/¿/¡/ã/õ) + stopwords; los acentos
      // compartidos (á/é/í/ó/ú) los usan idiomas no soportados → no deciden solos.
      else if (/[ñ¿¡]|noticias|últimas|fútbol|política|economía/.test(sample)) language = "es";
      else if (/[ãõ]|notícias|esportes|cidade/.test(sample)) language = "pt";
      else if (/notizie|sport|città/.test(sample)) language = "it";
      else language = detectLangFromDomain(domain) || "en";
    }
    return { title: title.substring(0, 180), language, adNetworks };
  } catch { return null; }
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
  // Maxi 2026-07-09: mejor email por tiering comercial (no el [0] de inserción) → autofill Monday
  // con publicidad@/comercial@ o el decision-maker de Apollo, según la elección del user (Q4).
  const bestEmail = _bestEmailByTier(state.emails) || dup?.email || "";
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

  // Click rate-limiter — bloquea clicks repetidos en <300ms (user
  // tendría "doble-click rebote" perception si la pestaña ya cambió).
  let _lastTabClick = 0;
  // Cache de querySelectorAll para no re-scan DOM en cada click
  const _tabBtnsCache = document.querySelectorAll(".tab-btn");
  const _tabContentsCache = document.querySelectorAll(".tab-content");

  _tabBtnsCache.forEach(btn => {
    btn.addEventListener("click", async () => {
      // Rate-limit: ignorar segundo click <300ms
      const now = Date.now();
      if (now - _lastTabClick < 300) return;
      _lastTabClick = now;

      // ── PASO 1: DOM update INMEDIATO (síncrono, ~1ms) ───────────
      // Esto hace que la pestaña visualmente cambie YA, sin esperar
      // que async init termine. Mejora la percepción de respuesta.
      _tabBtnsCache.forEach(b => b.classList.remove("active"));
      _tabContentsCache.forEach(c => c.classList.remove("active"));
      btn.classList.add("active");
      const tabId  = btn.dataset.tab;
      const tabEl  = document.getElementById(`tab-${tabId}`);
      if (tabEl) tabEl.classList.add("active");

      // ── PASO 2: yield al browser para repaint ANTES del trabajo ──
      // BUG FIX 2026-05-13: rAF no dispara cuando side panel queda hidden
      // → await colgaba para siempre → clicks acumulados → freeze total.
      // Ahora usamos setTimeout(20) puro: yields al browser sin depender
      // de visibility state.
      await new Promise(resolve => setTimeout(resolve, 20));

      // ── PASO 3: Lazy load async (puede tardar pero la UI ya respondió)
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
          // Si falló, permitir reintentar
          loadedTabs.delete(tabId);
        }
      }
    });
  });

  // Banner Railway muerto — REMOVIDO. Generaba falsos positivos cuando el worker
  // estaba OK pero la columna heartbeat no se updateaba a tiempo. User pidió quitarlo.
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
      el.textContent = `⚠️ DUPLICATE · ${result.status} · ${result.ejecutivo || "—"}`;
      el.className   = "status-badge duplicate";
      state.mondayItemId = result.itemId;
      fillMondayFormFromDuplicate(result);
      document.getElementById("btn-push-monday").textContent = "🔄 Update in Monday";

      // Si es duplicado re-prospectable (Ciclo Finalizado / Mail No Enviado),
      // los datos en Monday pueden tener meses → forzar refresh de tráfico.
      // Esto sobreescribe el cache 90d y trae fresh de RapidAPI.
      // Solo forzamos refresh si: (a) status reprospectable Y (b) el cache está viejo
      // (>30d). Si el cache es reciente, lo respetamos para no quemar RapidAPI.
      const reprospectable = /ciclo\s*finalizado|mail\s*no\s*enviado/i.test(result.status || "");
      if (reprospectable) {
        // Esperar a runTrafficCheck para ver si los datos vinieron de cache reciente.
        // Si cache > 30d, re-fetch con forceRefresh.
        setTimeout(() => {
          if (state.trafficData?.fromCache && (state.trafficData?.cachedDaysAgo || 0) > 30) {
            console.log(`[Duplicate] reprospectable + cache >30d → forzando refresh`);
            runTrafficCheck({ forceRefresh: true }).catch(() => {});
          }
        }, 1500);
      }
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

async function runTrafficCheck(opts = {}) {
  const { forceRefresh = false } = opts;
  const metricEl    = document.getElementById("traffic-result");
  const unitEl      = document.getElementById("traffic-unit");
  const breakdownEl = document.getElementById("traffic-breakdown");
  const categoryEl  = document.getElementById("traffic-category");
  const filterEl    = document.getElementById("traffic-filter");

  try {
    // Caché de sesión primero — mismo dominio, distinta subpágina (salvo forceRefresh)
    const sess = forceRefresh ? null : await getSessionCache(state.domain);
    let data;
    if (sess?.trafficData) {
      data = { ...sess.trafficData, fromCache: true, cachedDaysAgo: sess.trafficData.cachedDaysAgo ?? 0 };
    } else {
      data = await getTraffic(state.domain, { forceRefresh });
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
      ? `<span class="main-country-flag" title="Top traffic country: ${esc(topC.name || topC.code)}" data-code="${esc(topC.code)}" style="font-size:18px;margin-left:6px;cursor:help">${countryFlag(topC.code)}</span>`
      : "";

    // Maxi 2026-06-17 v2: si visits=0 mostrar botón explícito de "Re-verificar".
    // Antes solo mostraba "0" sin acción → MB perdía leads de millones de
    // visitas que la cache había guardado como 0 por un error transitorio.
    if (!state.visits || state.visits === 0) {
      metricEl.innerHTML = `<span style="color:#f59e0b">⚠️ Sin tráfico detectado</span>${mainFlagHtml}`;
      if (unitEl) unitEl.textContent = "";
      // Maxi 2026-06-18: 3 acciones cuando no encuentra tráfico: re-verificar,
      // abrir SimilarWeb directo (para chequear visual), y mensaje claro.
      // Maxi 2026-06-22: hypestat en vez de SimilarWeb (SW limita sesiones rápido).
      const swUrl  = `https://hypestat.com/info/${esc(state.domain || "")}`;
      const sw2Url = `https://www.similarweb.com/website/${esc(state.domain || "")}/`;
      breakdownEl.innerHTML = `
        <div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin-bottom:4px">
          <button id="btn-traffic-recheck" type="button" style="font-size:11px;padding:4px 10px;background:#0ea5e9;color:#fff;border:none;border-radius:4px;cursor:pointer">🔄 Re-verificar</button>
          <a id="btn-similarweb-open" href="${swUrl}" target="_blank" rel="noopener" style="font-size:11px;padding:4px 10px;background:#10b981;color:#fff;border-radius:4px;text-decoration:none;display:inline-flex;align-items:center;gap:3px" title="Hypestat — sin límite de sesión">📊 Ver tráfico (Hypestat)</a>
          <a href="${sw2Url}" target="_blank" rel="noopener" style="font-size:11px;padding:4px 10px;background:#334155;color:#cbd5e1;border-radius:4px;text-decoration:none">SW</a>
        </div>${cacheStr}
      `;
      setTimeout(() => {
        document.getElementById("btn-traffic-recheck")?.addEventListener("click", () => {
          runTrafficCheck({ forceRefresh: true }).catch(() => {});
        });
      }, 0);
    } else if (data.noPageViewData) {
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
          const tip = `${esc(c.name || c.code)} — ${c.pct}% of site traffic comes from here`;
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
    filterEl.textContent = passesTrafficFilter(trafficForFilter) ? "✅ Supera umbral 350K" : "❌ Bajo umbral 350K — no enriquecer";
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
    // user 2026-05-29: trackeo URL analizada (con dedup por dominio en la sesión
    // para no contar refreshes del mismo sitio como múltiples análisis).
    try {
      const _domNorm = (state.domain || "").toLowerCase();
      window._analyzedDomainsThisSession = window._analyzedDomainsThisSession || new Set();
      if (_domNorm && !window._analyzedDomainsThisSession.has(_domNorm)) {
        window._analyzedDomainsThisSession.add(_domNorm);
        incrementUserDailyCounter(state.accessToken, state.loginEmail, "sites").catch(() => {});
      }
    } catch {}
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

        // Maxi 2026-07-09: DETECTOR ESTRUCTURAL de sitio NO-PUBLISHER (por CÓMO está construido).
        // Solo para AVISAR al MB — no bloquea nada. Paridad con el worker (schema.org @type +
        // señales de intención: carrito, home-banking, admisiones, reservas, donaciones).
        // Maxi 2026-07-09: ALTA PRECISIÓN (0 falsos positivos validado). Tienda SOLO por plataforma
        // real o botón add-to-cart + checkout juntos; el resto por schema.org @type. Las keywords
        // (banco/viajes/edu/inmobiliaria/servicio/ONG) SOLO cuentan si el sitio NO corre programmatic
        // (un publisher sí lo corre; un banco/hotel/tienda en su sitio propio no) → evita marcar mal
        // finance-news / travel-blogs / education-content.
        let nonPublisherType = "";
        try {
          const _h = document.documentElement.innerHTML || "";
          const _hits = (arr) => arr.reduce((n, re) => n + (re.test(_h) ? 1 : 0), 0);
          // hasDisplayAds = programmatic O red partner de ADEQ (Taboola/MGID/Ezoic/Seedtag/Teads...).
          // Si el sitio muestra ads → es publisher → NO se marca por schema/keywords (regla de oro).
          const hasDisplayAds = /(pagead2\.googlesyndication|adsbygoogle|googletagservices|securepubads|googletag\.cmd|div-gpt-ad|data-ad-slot|doubleclick\.net|amazon-adsystem|criteo|pubmatic|rubiconproject|magnite|openx\.net|prebid|adnxs\.com|appnexus|33across|sovrn|indexexchange|casalemedia|smartadserver|adform\.net|yieldmo|sharethrough|gumgum|adsrvr\.org|fundingchoicesmessages|taboola|outbrain|mgid\.com|ezoic|ezojs|seedtag|teads|vidoomy|sparteo|missena|snigel|clickio|optad360)/i.test(_h);
          // Solo plataformas DEDICADAS (Woo/Magento salieron del 1-hit: medios en WordPress usan Woo).
          const storePlatform = /Shopify\.theme|Shopify\.shop|Shopify\.routes|window\.Shopify|id=["']shopify-section|vteximg|vtexassets|vtexcommercestable|portal\.vtex|\/\/[a-z0-9-]+\.vtex\.|nuvemshop|tiendanube|lojaintegrada|prestashop|bigcommerce|demandware|dwstatic|\/on\/demandware/i;
          const storeCartBtn  = /add[\s-]?to[\s-]?cart|a[ñn]adir al carrito|agregar al carr(o|ito)|adicionar ao carrinho|sepete ekle|a[ñn]adir a la (cesta|bolsa)|in den warenkorb/i;
          const storeCheckout = /\/(checkout|cart\/add|onepage|finalizar-compra|finalizar_compra|sepet|carrinho)\b|class=["'][^"']*(add-to-cart|addtocart|btn-cart|buy-button)/i;
          const storeOg = /og:type["'][^>]*content=["']product["']/i;
          const isStore = storePlatform.test(_h) || (storeCartBtn.test(_h) && storeCheckout.test(_h)) || (storeOg.test(_h) && storeCartBtn.test(_h));
          const bankKw   = [/online banking|internet banking|home ?banking|banca (digital|en l[íi]nea)|neobank|acesse sua conta|abr[ai] (sua|tu) conta/i, /abrir (tu |una )?cuenta( corriente| de ahorro| bancaria)|open (a |your )?bank account|conta corrente/i];
          const travelKw = [/car (hire|rental)|rent a car|alquiler de (coches?|autos?)|pauschalreise|urlaubsangebote|hotel buchen|best rate guarantee|book your (stay|room)/i, /reserva (tu |una )?(habitaci[óo]n|estancia)|habitaciones disponibles|mejor tarifa garantizada/i];
          const eduKw    = [/proceso de admisi[óo]n|solicita tu (admisi[óo]n|plaza)|admisiones abiertas/i, /oferta acad[ée]mica|vida universitaria|nuestras? (titulaciones|carreras universitarias)|campus universitario/i];
          const svcKw    = [/solicita(r)? (tu |un )?presupuesto|pide presupuesto|request a (demo|quote)|solicita una demo|book a demo/i, /nuestros servicios profesionales|market research|investigaci[óo]n de mercado|consultor[íi]a (empresarial|estrat[ée]gica)/i];
          const npoKw    = [/donate now|make a donation|registered charity|become a volunteer|hacer una donaci[óo]n/i, /recaudaci[óo]n de fondos|fundraising campaign|apoya (nuestra|la) causa/i];
          const realtyKw = [/pisos? en (venta|alquiler)|propiedades? en (venta|alquiler)|casas? en venta|im[óo]veis (para|à) (venda|alugar)/i, /publica(r)? tu (anuncio|propiedad) gratis|m² (construidos|[úu]tiles)|\d+ dormitorios/i];
          if (isStore) nonPublisherType = "online store / e-commerce";
          // schema Y keywords SOLO cuentan si NO hay ads display (un publisher con ads —aunque reseñe
          // un hotel con schema Hotel— NO se marca). Igual que el worker.
          else if (!hasDisplayAds) {
            if (/"@type"\s*:\s*"(BankOrCreditUnion|FinancialService|InsuranceAgency)"/i.test(_h)) nonPublisherType = "bank / financial services";
            else if (/"@type"\s*:\s*"(CollegeOrUniversity|EducationalOrganization|School|University)"/i.test(_h)) nonPublisherType = "university / education";
            else if (/"@type"\s*:\s*"(Hotel|LodgingBusiness|Resort|TravelAgency|AutoRental|RentACar|Campground|BedAndBreakfast)"/i.test(_h)) nonPublisherType = "travel / hotel / car rental";
            else if (/"@type"\s*:\s*"(NGO|NonprofitOrganization|Charity)"/i.test(_h)) nonPublisherType = "nonprofit / charity";
            else if (/"@type"\s*:\s*"(RealEstateListing|RealEstateAgent|Residence|Apartment|SingleFamilyResidence)"/i.test(_h)) nonPublisherType = "real estate / listings";
            else if (_hits(bankKw) >= 2) nonPublisherType = "bank / financial services";
            else if (_hits(travelKw) >= 2) nonPublisherType = "travel / hotel / car rental";
            else if (_hits(realtyKw) >= 2) nonPublisherType = "real estate / listings";
            else if (_hits(eduKw) >= 2) nonPublisherType = "university / education";
            else if (_hits(svcKw) >= 2) nonPublisherType = "service / agency";
            else if (_hits(npoKw) >= 2) nonPublisherType = "nonprofit / charity";
          }
        } catch {}
        return {
          title:    document.title || "",
          lang:     langFull.substring(0, 2),
          langFull,
          ogLocale: ogLocale.toLowerCase(),
          phoneCodes,
          currencies,
          nonPublisherType,
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
    state.nonPublisherType = result?.nonPublisherType || "";
    renderNonPublisherNotice();
  } catch { /* sin permisos en esa página */ }
}

// Maxi 2026-07-09: aviso NO bloqueante — si el sitio abierto NO es un publisher de contenido
// (tienda/banco/universidad/viajes/ONG), se lo avisa al MB en inglés. El user pidió: "aunque el
// MB nunca va a abrir bancos, sería bueno que le diga sitio web no recomendado de prospectar".
function renderNonPublisherNotice() {
  const el = document.getElementById("nonpub-notice");
  if (!el) return;
  const t = state.nonPublisherType;
  if (t) {
    el.style.display = "block";
    el.innerHTML = `⚠️ Website not recommended for prospecting <span style="opacity:.75">(${esc(t)})</span> — not a content publisher.`;
  } else {
    el.style.display = "none";
    el.innerHTML = "";
  }
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

// Maxi 2026-06-19: Apollo AUTO-PACING en Analysis MANUAL (decisión user). Mismo
// criterio que el agente automatizado: si NO hay decision-maker NO genérico por
// fuentes gratis → fuerza Apollo (prob 1.0); si ya hay → probabilidad = % de cupo
// MENSUAL restante (más reveals al inicio del ciclo 6-a-6, menos al final). Se
// auto-calibra solo y SIEMPRE respeta el tope: si no queda cupo, no revela.
// Cada reveal consume 1 crédito Apollo (/v1/people/match).
function _isGenericEmailLocal(email) {
  const local = String(email || "").split("@")[0].toLowerCase().replace(/[._-]/g, "");
  return /^(info|contact|hello|hi|admin|support|sales|team|press|media|marketing|office|general|ventas|contacto|hola|ayuda|soporte|prensa|comercial|webmaster|enquiries|hr|jobs|careers|noreply|donotreply)$/.test(local);
}
// Maxi 2026-07-09: rol de VENTA DE PAUTA/PUBLICIDAD = "mejor opción" para ADEQ (elección user Q4).
// Regex acotado (no matchea admin/advisor). Paridad con el worker (AD_SALES_LOCAL).
const _AD_SALES_LOCAL_RE = /^(?:publicidad|publicidade|publicit[ea]|pubblicit|werbung|vermarkt|advertis|advert\b|\badv\b|ads\b|ad[-_.]?sales|adverten|anunci|anzeigen|reklam|iklan|regiepub|regie\b|comercial|commercial|ventas|vendas|vente|verkauf|verkoop|sales\b|salesteam|marketing|mktg?\b|monetiz|media[-_.]?sales|raccolta|auglys|annons|inventory|programmatic|patrocin|sponsor)/i;
// Tier de SELECCIÓN del email (paridad worker _pickTier): apollo/informer nominal (4) >
// publicidad@/comercial@/ventas@ scrapeado (3) > persona scrapeada (2) > genérico info@/contacto@ (0).
function _emailPickTierClient(email) {
  const src = (state.emailSources.get(email) || "").toLowerCase();
  if (src === "apollo" || src === "informer") return 4;      // decision-maker verificado
  const local = String(email || "").toLowerCase().split("@")[0];
  if (_AD_SALES_LOCAL_RE.test(local)) return 3;              // rol comercial/publicidad
  if (!_isGenericEmailLocal(email)) return 2;                // persona / rol no-genérico
  return 0;                                                   // genérico
}
function _bestEmailByTier(emails) {
  const list = (emails || []).filter(Boolean);
  if (!list.length) return "";
  return [...list].sort((a, b) => _emailPickTierClient(b) - _emailPickTierClient(a))[0];
}
async function _apolloAutoPaceReveal(apolloResult, domainGuard) {
  try {
    if (!apolloResult || !Array.isArray(apolloResult.people)) return;
    // Candidato a revelar: primer contacto bloqueado con id (Apollo los ordena por relevancia)
    const locked = apolloResult.people.find(p => !p.unlocked && p.id);
    if (!locked) return;
    // ¿Ya tenemos un decision-maker NO genérico por fuentes gratis?
    const haveDM = (state.emails || []).some(e => !_isGenericEmailLocal(e));
    // Cupo mensual Apollo (mismo origen que el footer)
    const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
    const r = await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=in.(apollo_calls_month,apollo_monthly_limit)&select=key,value`, { headers, signal: AbortSignal.timeout(6000) });
    const rows = r.ok ? await r.json() : [];
    const map = {}; (rows || []).forEach(x => { map[x.key] = x.value; });
    const limit = parseInt(map.apollo_monthly_limit || "2400", 10) || 2400;
    const used  = parseInt(map.apollo_calls_month   || "0", 10) || 0;
    const remaining = Math.max(0, limit - used);
    if (remaining <= 0) return; // tope mensual alcanzado → no gastar
    const remainingPct = Math.max(0, Math.min(1, remaining / limit));
    const chance = haveDM ? remainingPct : 1;          // sin DM → siempre; con DM → pacing
    if (Math.random() >= chance) return;               // el pacing decidió que no esta vez
    const rev = await revealApolloEmail({ id: locked.id, first_name: locked.first_name, last_name: locked.last_name, domain: state.domain });
    if (state.domain !== domainGuard) return;          // cambió de dominio durante el reveal
    if (rev?.ok && rev.person?.email) {
      const idx = apolloResult.people.indexOf(locked);
      if (idx >= 0) apolloResult.people[idx] = rev.person;
      addEmailsWithSource([rev.person.email], "Apollo", domainGuard);
      const apolloEl = document.getElementById("apollo-result");
      if (apolloEl) renderApolloPeople(apolloEl, apolloResult);
      renderEmailList(state.emails);
      refreshApolloFooterCounter();
  refreshSerperFooterCounter();
      console.log(`[Apollo auto-pace] reveal ${rev.person.email} (haveDM=${haveDM}, chance=${(chance*100).toFixed(0)}%, cupo=${remaining}/${limit})`);
    }
  } catch {}
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
    const _dom = state.domain; // domain guard para descartar promesas tardías
    state.emails = []; state.emailSources = new Map();
    addEmailsWithSource((sess?.emails || []).filter(quickValidateEmail), "Cache", _dom);

    // Fuentes externas (contacto/directorio + website informer) — solo la primera
    // vez por dominio en la sesión; después vienen cacheadas como "Cache".
    const needExternal = !sess?.contactScraped;
    const [pageResult, contactEmails, informerEmails, apolloResult] = await Promise.all([
      scrapeEmailsFromPage(state.tabId),
      needExternal ? scrapeContactPages(`https://${state.domain}`).catch(() => []) : Promise.resolve([]),
      needExternal ? scrapeWebsiteInformer(state.domain).catch(() => []) : Promise.resolve([]),
      // Si la cache de sesión ya tiene Apollo emails, no re-disparar
      (sess?.apolloPreloaded)
        ? Promise.resolve(null)
        : findDecisionMakerViaApollo(state.domain).catch(() => null),
    ]);
    // Maxi 2026-06-17 v2: pageResult ahora es { emails, socialLinks }.
    // Backward compat: si vino como array, lo tratamos como emails directos.
    const pageEmails = Array.isArray(pageResult) ? pageResult : (pageResult?.emails || []);
    const pageSocialLinks = Array.isArray(pageResult?.socialLinks) ? pageResult.socialLinks : [];
    // Maxi 2026-06-17 v4: NO mostrar social links como chips separados —
    // INSTEAD, entrar a las redes y EXTRAER emails que estén publicados
    // ahí (FB business email, YouTube "contact for business", etc).
    // Cada email encontrado se suma con source="social_facebook"/"_youtube"/etc.
    if (pageSocialLinks.length > 0) {
      state.pageSocialLinks = pageSocialLinks; // se mantiene por compat con renderEmailList
      scrapeEmailsFromSocialLinks(pageSocialLinks).then(socialEmailMap => {
        if (!socialEmailMap || socialEmailMap.size === 0) return;
        if (state.domain !== _dom) return; // domain cambió durante el fetch
        const validEmails = [];
        socialEmailMap.forEach((src, em) => {
          if (quickValidateEmail(em)) {
            validEmails.push(em);
            // Sobreescribir el source con el social específico
            state.emailSources.set(em, src);
          }
        });
        if (validEmails.length === 0) return;
        addEmailsWithSource(validEmails, "Social", _dom);
        // Re-render para reflejar los nuevos emails con su source social_*
        renderEmailList(state.emails);
      }).catch(() => {});
    }
    addEmailsWithSource(pageEmails.filter(quickValidateEmail), "Page", _dom);
    addEmailsWithSource(contactEmails.filter(quickValidateEmail), "Scrape", _dom);
    addEmailsWithSource(informerEmails.filter(quickValidateEmail), "Informer", _dom);
    // Marcar que ya bajamos las fuentes externas para este dominio (evita refetch)
    if (needExternal && state.domain === _dom) {
      try {
        const cur = await getSessionCache(state.domain) || {};
        await setSessionCache(state.domain, { ...cur, contactScraped: true });
      } catch {}
    }

    // Apollo: agregar los unlocked al pool + render preview
    if (apolloResult) {
      const unlockedEmails = (apolloResult.people || []).filter(p => p.unlocked && p.email).map(p => p.email);
      if (unlockedEmails.length) {
        addEmailsWithSource(unlockedEmails, "Apollo", _dom);
      }
      if (apolloResult.name)     state.decisionMakerName = apolloResult.name.split(" ")[0];
      if (apolloResult.linkedin) showLinkedIn(apolloResult.linkedin);
      // Pre-render Apollo people block (default 2 visible + ver más)
      const apolloEl = document.getElementById("apollo-result");
      if (apolloEl) renderApolloPeople(apolloEl, apolloResult);
      // Maxi 2026-06-19: auto-pacing Apollo (1 reveal automático según cupo, como el agente)
      await _apolloAutoPaceReveal(apolloResult, _dom);
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
function addEmailsWithSource(emails, source, domainGuard = null) {
  // Race guard: si pasaste domainGuard y el state.domain cambió (user cambió
  // de tab), descartamos estos emails — vienen de una promesa del dominio
  // anterior que resolvió tarde. Sin esto se mezclan emails de dominios distintos.
  if (domainGuard && state.domain !== domainGuard) return;

  // Validación adicional: solo agregamos emails cuyo dominio matchee el actual
  // O sea webmail conocido (gmail/hotmail/etc — válido para decision makers).
  // Sin esto, scrapes de webs sociales (LinkedIn, Twitter) inyectan emails
  // de OTROS sites (partner emails, signature emails, etc.).
  const currentSite = (state.domain || "").toLowerCase().replace(/^www\./, "");
  const WEBMAIL = /^(gmail|hotmail|outlook|live|yahoo|aol|icloud|protonmail|gmx|me)\.com$/;
  const _belongsToCurrent = (email) => {
    if (!currentSite) return true;
    const dom = (email.split("@")[1] || "").toLowerCase();
    if (!dom) return false;
    if (dom === currentSite) return true;
    if (dom.endsWith("." + currentSite) || currentSite.endsWith("." + dom)) return true;
    if (WEBMAIL.test(dom)) return true; // emails personales gmail/hotmail OK
    return false;
  };

  for (const e of emails) {
    if (!e) continue;
    if (isGarbageEmail(e)) continue;
    if (!_belongsToCurrent(e)) continue; // anti-leak entre dominios
    if (!state.emailSources.has(e)) state.emailSources.set(e, source);
    if (!state.emails.includes(e)) state.emails.push(e);
  }
}

// Cache de resultados de verifyEmail para no re-verificar el mismo email
// dentro de la misma sesión del side panel. LRU eviction a 2000 entries
// para evitar crecimiento sin bound en sesiones largas (10K+ verifications).
const _emailVerifyCache = new Map(); // email -> {valid, tags, reason, score}
const _VERIFY_CACHE_MAX = 2000;
function _evictVerifyCacheIfFull() {
  if (_emailVerifyCache.size < _VERIFY_CACHE_MAX) return;
  // Borrar el 20% más viejo (Map mantiene orden de inserción → primeros = más viejos)
  const toRemove = Math.floor(_VERIFY_CACHE_MAX * 0.2);
  const keys = _emailVerifyCache.keys();
  for (let i = 0; i < toRemove; i++) _emailVerifyCache.delete(keys.next().value);
}

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

// Memoization para _emailGrade — antes recalculaba en cada render.
// Key: email|verifyHash|source. Verifica hash es un proxy del result.
const _gradeCache = new Map();
const _GRADE_CACHE_MAX = 500;

// Grade A-E por email — diferencia calidad relativa entre emails válidos.
function _emailGrade(email, result, source) {
  // Memo lookup
  const verifyTag = result ? `${result.valid ? "v" : "i"}:${(result.tags || []).join(",")}:${result.deepSource || ""}` : "none";
  const cacheKey = `${email}|${verifyTag}|${source || ""}`;
  const memo = _gradeCache.get(cacheKey);
  if (memo) return memo;
  const r = _emailGradeCompute(email, result, source);
  if (_gradeCache.size >= _GRADE_CACHE_MAX) _gradeCache.clear();
  _gradeCache.set(cacheKey, r);
  return r;
}

// Logica A-E auditada y reescrita 2026-06-17. Mide CALIDAD/DELIVERABILIDAD del
// email, no calidad del lead (eso es el score del card). Para que Maxi pueda
// auditar el grado, el `label` incluye los factores que pesaron.
//
// Factores:
//   ▸ Verify SMTP/disify → +25/+30 (factor más fuerte cuando existe)
//   ▸ Source: apollo verified +20, informer +15, scrape +10, generic 0
//     (apollo "guessed" NO suma — solo verified)
//   ▸ Formato local-part: nombre.apellido +15, normal 5-18 chars +5
//   ▸ Penalty: rol genérico -25, catch-all -20, tld sospechoso -15
//   ▸ Tags de error fatales → E directo (typo, sin-mx, descartable, spam)
//   ▸ SIN verify: baseline 40, max teorico = B (necesitás verify para A)
function _emailGradeCompute(email, result, source) {
  if (!email || !email.includes("@")) return { grade: "E", label: "Inválido — formato malo" };
  const reasons = [];
  let score = 40; // baseline: prospect sin verify queda en "C bueno potencial"
  const tags = result?.tags || [];

  // ── Verify result — gate fatal + bonus si existe ──────────────
  let verified = false;
  if (result) {
    if (!result.valid) return { grade: "E", label: "Inválido — SMTP rechazó" };
    if (tags.includes("descartable") || tags.includes("descartable-remoto") ||
        tags.includes("undeliverable") || tags.includes("typo") ||
        tags.includes("sin-dns") || tags.includes("sin-mx") ||
        tags.includes("spam") || tags.includes("proxy-whois")) {
      return { grade: "E", label: "Descartable — " + (tags.find(t => ["typo","sin-mx","spam","proxy-whois","sin-dns"].includes(t)) || "tag rojo") };
    }
    if (result.deepSource === "eva") { score += 30; verified = true; reasons.push("SMTP+30"); }
    else if (result.deepSource === "disify") { score += 25; verified = true; reasons.push("DNS-remoto+25"); }
    else if (result.deepSource === "local-only") { score += 10; reasons.push("DNS-local+10"); }
    if (tags.includes("catch-all"))          { score -= 20; reasons.push("catch-all-20"); }
    if (tags.includes("catch-all-provider")) { score -= 12; reasons.push("catchAllProv-12"); }
    if (tags.includes("tld-sospechoso"))     { score -= 15; reasons.push("TLD-sosp-15"); }
  }

  // ── Source ────────────────────────────────────────────────────
  const src = (source || "").toLowerCase();
  if (src === "apollo")        { score += 20; reasons.push("apollo+20"); }
  else if (src === "informer") { score += 15; reasons.push("informer+15"); }
  else if (src === "scrape" || src === "scraping") { score += 10; reasons.push("sitio+10"); }
  else if (src === "generic")  { score -= 5;  reasons.push("genérico-5"); }
  else if (src === "gemini")   { score += 3;  reasons.push("gemini+3"); }

  // ── Formato local-part ────────────────────────────────────────
  const local = email.split("@")[0].toLowerCase();
  if (/^[a-z]+\.[a-z]+$/.test(local) && local.length >= 5 && local.length <= 24) {
    score += 15; reasons.push("nombre.apellido+15");
  } else if (/^[a-z]+$/.test(local) && local.length >= 5 && local.length <= 18) {
    score += 5; reasons.push("local-normal+5");
  }
  // Roles genéricos: penalidad fuerte (no son personas)
  if (/^(info|contact|contacto|contato|hello|hi|hola|admin|sales|ventas|comercial|support|soporte|atendimento|help|webmaster|noreply|no-reply|press|prensa|media|advertising|publicidad|publicidade|news|marketing)$/i.test(local)) {
    score -= 25; reasons.push("rol-25");
  }
  // Aleatorio / hash sospechoso: penalty leve
  if (/^[a-z0-9]{20,}$/.test(local)) { score -= 10; reasons.push("hash-10"); }

  // ── Mapeo a grade. SIN verify, max es B. ──────────────────────
  let grade, label;
  if (score >= 80 && verified)     { grade = "A"; label = "Excelente — verificado + señales fuertes"; }
  else if (score >= 80)            { grade = "B"; label = "Muy bueno — sin verificar (cap por falta de SMTP)"; }
  else if (score >= 65)            { grade = "B"; label = "Bueno"; }
  else if (score >= 50)            { grade = "C"; label = "Regular"; }
  else if (score >= 30)            { grade = "D"; label = "Bajo"; }
  else                             { grade = "E"; label = "Muy bajo"; }
  // Append breakdown a label para auditing (visible en tooltip)
  if (reasons.length) label += ` · score:${score} (${reasons.join(", ")})`;
  return { grade, label };
}

function _verifyTooltip(result) {
  if (!result) return "Verificando…";
  const status = result.valid ? "✔ Valid" : "✖ Invalid";
  const reason = result.reason || "";
  const tags   = (result.tags || []).join(", ");
  const src    = result.deepSource === "disify" || result.deepSource === "eva" ? "[remoto confirmado]"
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
      _evictVerifyCacheIfFull();
      _emailVerifyCache.set(email, result);
    }
    const cls = _verifyClass(result);
    chip.classList.remove("verify-pending", "verify-good", "verify-warn", "verify-bad");
    chip.classList.add(cls);
    chip.title = _verifyTooltip(result);
    // Refrescar grade A-E ahora que tenemos verify result
    const src = state.emailSources.get(email) || "";
    const g = _emailGrade(email, result, src);
    const oldBadge = chip.querySelector(".email-grade");
    if (oldBadge) {
      oldBadge.textContent = g.grade;
      oldBadge.className = `email-grade email-grade-${g.grade}`;
      oldBadge.title = g.label;
    }
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

// Maxi 2026-06-17 v3: chips de Social Media para AGREGAR junto a la lista de
// emails — mismo bloque, con label "Social Media" delante. Se renderizan
// inline con .email-chip mock para mantener el estilo. Devuelve HTML string.
const _SOCIAL_META = {
  "facebook.com":  { icon: "📘", label: "Facebook",  color: "#1877f2" },
  "linkedin.com":  { icon: "💼", label: "LinkedIn",  color: "#0a66c2" },
  "instagram.com": { icon: "📸", label: "Instagram", color: "#e4405f" },
  "twitter.com":   { icon: "🐦", label: "Twitter",   color: "#1da1f2" },
  "x.com":         { icon: "𝕏",  label: "X",         color: "#000000" },
  "youtube.com":   { icon: "▶️", label: "YouTube",   color: "#ff0000" },
};

/* _buildSocialMediaChipsHTML / _wireSocialMediaChipClicks removidas (B2) — código muerto */

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

  // 2. ORDEN (Maxi 2026-07-09): tiering comercial (paridad worker). apollo/informer nominal >
  //    publicidad@/comercial@/ventas@ > persona scrapeada > genérico. Sort estable (Chrome) →
  //    dentro de cada tier conserva el orden de inserción. Antes solo era "Apollo primero".
  const suggested = [...cleaned].sort((a, b) => _emailPickTierClient(b) - _emailPickTierClient(a));

  // Maxi 2026-06-17 v3: si NO hay emails ni socials, mensaje fallback simple.
  const _socials = Array.isArray(state.pageSocialLinks) ? state.pageSocialLinks : [];
  if (!mondayEmail && suggested.length === 0 && _socials.length === 0) {
    resultEl.style.display = "block";
    resultEl.textContent = "No valid emails — try Apollo";
    resultEl.className   = "email-value";
    listEl.style.display = "none";
    return;
  }
  // Si solo hay socials (no emails), mostramos la lista igual abajo —
  // los chips sociales se renderizan junto a los emails con label "(social media)"
  // y son clickeables para abrir en pestaña. NO se pueden seleccionar para envío.

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
    const g = _emailGrade(email, cached, src);
    const gradeBadge = `<span class="email-grade email-grade-${g.grade}" title="${esc(g.label)}">${g.grade}</span>`;
    // Maxi 2026-06-18: botón "+" rota por los 3 slots de adicionales.
    // - Si email ya está en un slot, muestra el número (1/2/3)
    // - Click cuando está vacío → asigna al primer slot LIBRE
    // - Click cuando está asignado → libera el slot
    // Antes solo apuntaba al slot 1 → no se podían usar los slots 2/3.
    const slotIdxByEmail = (em) => {
      const v1 = (document.getElementById("form-email-futuro")?.value || "").toLowerCase().trim();
      const v2 = (document.getElementById("form-email-futuro-2")?.value || "").toLowerCase().trim();
      const v3 = (document.getElementById("form-email-futuro-3")?.value || "").toLowerCase().trim();
      const e = em.toLowerCase();
      if (e === v1) return 1;
      if (e === v2) return 2;
      if (e === v3) return 3;
      return 0;
    };
    const curSlot = slotIdxByEmail(email);
    const btnLabel = curSlot ? String(curSlot) : "+";
    const btnTitle = curSlot
      ? `Asignado como Adicional ${curSlot} — click para quitar`
      : "Click para agregar como Contacto Adicional (envío paralelo día 0)";
    const futureBtn = `<button type="button" class="email-future-btn ${curSlot ? "assigned" : ""}" data-email-future="${esc(email)}" title="${esc(btnTitle)}">${btnLabel}</button>`;
    return `<div class="email-chip ${extraClass} ${verCls} ${curSlot ? "slot-future" : ""}" data-email="${esc(email)}" title="Click = enviar ahora. + (derecha) = agregar como adicional">${gradeBadge}${esc(email)}${srcBadge}${futureBtn}</div>`;
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
      html += `<button class="email-show-more" type="button" style="font-size:10px;background:transparent;border:none;color:var(--adeq-blue);cursor:pointer;padding:4px 0;text-decoration:underline">+ show ${hidden.length} more…</button>`;
    }
  }

  // Maxi 2026-06-17 v4: emails extraídos de redes sociales (Facebook/YouTube/
  // Twitter) ya vienen mezclados en `suggested` con state.emailSources de
  // "Facebook"/"YouTube"/"Twitter" — el chip src-badge los identifica visualmente.

  // Maxi 2026-06-18: contact forms detectados → chip clickeable separado.
  // Vienen en state.emailSources con key __contact_form_N__ (worker).
  const cfChips = [];
  state.emailSources.forEach((v, k) => {
    if (k.startsWith("__contact_form_")) {
      const url = typeof v === "string" ? "" : (v?.url || "");
      if (url) cfChips.push(url);
    }
  });
  if (cfChips.length > 0) {
    html += `<div class="email-group-label" style="margin-top:8px">📝 Contact Form</div>`;
    cfChips.forEach((url, i) => {
      html += `<a href="#" class="contact-form-chip" data-cf-url="${esc(url)}" title="Abrir formulario de contacto" style="display:inline-flex;align-items:center;gap:4px;padding:3px 9px;background:#10b981;color:#fff;border-radius:4px;font-size:11px;text-decoration:none;margin:2px;font-weight:600">📝 Form ${cfChips.length > 1 ? i + 1 : ""}</a>`;
    });
  }

  listEl.innerHTML = html;

  // Wire contact form chips
  listEl.querySelectorAll(".contact-form-chip").forEach(el => {
    el.addEventListener("click", (ev) => {
      ev.preventDefault();
      const url = el.dataset.cfUrl;
      if (url) chrome.tabs.create({ url, active: false });
    });
  });

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

  // Auto-selección: si el sitio es duplicado de Monday, NUNCA pre-seleccionamos
  // el email viejo de Monday — siempre preferimos uno fresco de Apollo/scrape
  // (que ya viene rankeado: ad ops > publicidad > marketing > online > dev).
  // Si no hay ninguno fresco, recién ahí caemos al de Monday como fallback.
  // Maxi 2026-06-18: excluir slot-future del auto-select del principal
  const preferredChip = listEl.querySelector(".email-chip:not(.monday):not(.slot-future)")
                     || listEl.querySelector(".email-chip:not(.slot-future)")
                     || listEl.querySelector(".email-chip");
  if (preferredChip) {
    preferredChip.classList.add("selected");
    formEl.value = preferredChip.dataset.email;
  }

  // Click para seleccionar (slot 1 = email principal, envío ahora)
  listEl.querySelectorAll(".email-chip").forEach(chip => {
    chip.addEventListener("click", (ev) => {
      // Si tocó el botón "→ 2", no procesar como click de slot 1
      if (ev.target.classList.contains("email-future-btn")) return;
      listEl.querySelectorAll(".email-chip").forEach(c => c.classList.remove("selected"));
      chip.classList.add("selected");
      formEl.value = chip.dataset.email;
      const cached = _emailVerifyCache.get(chip.dataset.email);
      _renderVerifyBadge(badge, cached);
      // Si el mismo email estaba en slot 2, limpiar slot 2 (no puede estar en ambos)
      const fut = document.getElementById("form-email-futuro");
      if (fut && fut.value === chip.dataset.email) {
        fut.value = "";
        listEl.querySelectorAll(".email-chip.slot-future").forEach(c => c.classList.remove("slot-future"));
      }
    });
  });

  // Botón "→ 2" — asigna ese email al slot Email Futuro
  // Maxi 2026-06-18: handler del botón "+" que rota por los 3 slots de adicionales.
  listEl.querySelectorAll(".email-future-btn").forEach(btn => {
    btn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const email = btn.dataset.emailFuture;
      if (!email) return;
      const slots = [
        document.getElementById("form-email-futuro"),
        document.getElementById("form-email-futuro-2"),
        document.getElementById("form-email-futuro-3"),
      ].filter(Boolean);
      if (slots.length === 0) return;
      const lowerE = email.toLowerCase().trim();
      // ¿Ya está asignado a algún slot? → quitarlo
      const existingIdx = slots.findIndex(s => (s.value || "").toLowerCase().trim() === lowerE);
      if (existingIdx !== -1) {
        slots[existingIdx].value = "";
      } else {
        // No estaba → asignar al primer slot LIBRE
        const freeSlot = slots.find(s => !(s.value || "").trim());
        if (!freeSlot) {
          // Sin slots libres → sobreescribir el último para que igual pueda agregarlo
          slots[slots.length - 1].value = email;
        } else {
          freeSlot.value = email;
        }
        // Si el email estaba en slot principal, liberarlo
        if (formEl.value === email) {
          formEl.value = "";
          const next = listEl.querySelector(".email-chip:not(.slot-future):not(.monday)") || listEl.querySelector(".email-chip:not(.slot-future)");
          if (next) {
            listEl.querySelectorAll(".email-chip").forEach(c => c.classList.remove("selected"));
            next.classList.add("selected");
            formEl.value = next.dataset.email;
          }
        }
      }
      // Re-render para actualizar labels (+/1/2/3) de TODOS los chips
      renderEmailList(state.emails);
    });
  });

  // Auto-verify en background — sin bloquear el render
  autoVerifyEmailChips(listEl).catch(e => console.warn("[autoVerify]", e));
}

function setEmail(email) {
  if (email && !state.emails.includes(email)) state.emails.unshift(email);
  renderEmailList(state.emails);
}

/* verifyCurrentEmail removida (B2) — código muerto (0 call-sites) */

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
    ? `<button class="apollo-show-more" type="button" style="font-size:10px;background:transparent;border:none;color:#0369a1;cursor:pointer;padding:4px 0;text-decoration:underline;width:100%;text-align:left">+ show ${people.length - PREVIEW_COUNT} more…</button>`
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
      if (!person) { btn.textContent = "❌"; btn.title = "Stale index — re-render the card"; return; }
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
// M2: reglas destiladas del feedback 👍/👎 del MB (las sintetiza el worker 1×/día
// y las guarda en toolbar_config `pitch_rules_<email>`). Acá las leemos para
// inyectarlas también en la generación MANUAL del popup. Cache 10min.
let _pitchRulesBlockCache = { at: 0, email: "", block: "" };
async function _getPitchRulesBlockPopup() {
  const email = (state.loginEmail || "").toLowerCase();
  if (!email) return "";
  if (_pitchRulesBlockCache.email === email && (Date.now() - _pitchRulesBlockCache.at) < 600_000) {
    return _pitchRulesBlockCache.block;
  }
  let block = "";
  try {
    const r = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=eq.${encodeURIComponent(`pitch_rules_${email}`)}&select=value`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` }, signal: AbortSignal.timeout(6000) }
    );
    if (r.ok) {
      const rows = await r.json();
      const parsed = rows?.[0]?.value ? JSON.parse(rows[0].value) : null;
      if (parsed && (parsed.do?.length || parsed.avoid?.length)) {
        const doStr    = (parsed.do || []).map(x => `- ${x}`).join("\n");
        const avoidStr = (parsed.avoid || []).map(x => `- ${x}`).join("\n");
        block = `LEARNED RULES FROM THIS MB'S FEEDBACK (always follow):${doStr ? `\nDO:\n${doStr}` : ""}${avoidStr ? `\nAVOID:\n${avoidStr}` : ""}`;
      }
    }
  } catch {}
  _pitchRulesBlockCache = { at: Date.now(), email, block };
  return block;
}

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
    showToast(`📝 Draft restored for ${state.domain} (${Math.round((Date.now() - draft.ts) / 60000)}m ago)`, "info");
  } catch {}
}

async function bindButtons() {

  // Auto-save draft del pitch (debounce 3s) + restore al cargar
  document.getElementById("pitch-text")?.addEventListener("input", _autoSavePitchDraft);
  _restorePitchDraft();

  // Verificar email
  // btn-verify-email REMOVIDO — la verificación es automática on-render (autoVerifyEmailChips)

  // Apollo
  // Maxi 2026-07-08: re-escanear el DOM de la página ACTUAL. Resuelve el caso de emails
  // JS-renderizados (ej. livestly.com/contact tiene los emails por JS → no están en el
  // fetch, pero SÍ en el DOM). El MB navega a la página de contacto y clickea acá.
  document.getElementById("btn-rescan-page")?.addEventListener("click", async () => {
    const btn      = document.getElementById("btn-rescan-page");
    const resultEl = document.getElementById("apollo-result");
    const _lbl = btn.textContent;
    btn.disabled = true; btn.textContent = "⏳ Escaneando...";
    try {
      const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
      const tabId = tab?.id || state.tabId;
      const pageResult = await scrapeEmailsFromPage(tabId);
      const found = (Array.isArray(pageResult) ? pageResult : (pageResult?.emails || [])).filter(quickValidateEmail);
      const before = state.emails.length;
      addEmailsWithSource(found, "Page", state.domain);
      const added = state.emails.length - before;
      renderEmailList(state.emails);
      if (resultEl) {
        resultEl.style.display = "block";
        resultEl.textContent = added > 0
          ? `✅ ${added} email(s) nuevos de esta página`
          : (found.length > 0 ? "Ya estaban todos en la lista" : "Sin emails en el DOM de esta página");
      }
    } catch (e) {
      if (resultEl) { resultEl.style.display = "block"; resultEl.textContent = `❌ ${e.message}`; }
    } finally {
      btn.disabled = false; btn.textContent = _lbl;
    }
  });

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
        feedbackRules: await _getPitchRulesBlockPopup(),
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
      ta.value = `Error: ${err.message}`; btn.textContent = "✨ Generate Pitch";
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
      "👎 What failed? (optional — helps the AI avoid this in future)\n\n" +
      "Examples: 'invented a month', 'said no ads.txt when site has it', 'too formal', 'too long'",
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
        feedbackRules: await _getPitchRulesBlockPopup(),
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
      res.textContent = "❌ GEO required. Fill the GEO field before pushing to Monday.";
      res.className = "push-result error";
      document.getElementById("form-geo")?.scrollIntoView({ behavior: "smooth", block: "center" });
      return;
    }
    // Guard #2: Páginas Vistas obligatorio — bloqueamos si traffic === 0
    const traffic = state.traffic || state.visits || 0;
    if (!traffic || traffic === 0) {
      res.textContent = "❌ Page Views required. SimilarWeb returned no data — fill manually or re-analyze.";
      res.className = "push-result error";
      return;
    }
    // Guard #3: no dejar pushear NI updatear Monday si todavía no se mandó email
    // en esta sesión. Aplica también a duplicados (botón "🔄 Update in Monday")
    // para evitar que se actualice un item sin haber re-mandado el pitch.
    if (!state.emailSentInSession) {
      const action = state.duplicate?.found ? "update" : "push";
      const msg = `❌ Send the email first (Send via Gmail button) before ${action} in Monday.`;
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
    // Guard anti-rebote: si el email ya está marcado como bounced en la DB
    // global, NO se permite enviarle. Evita re-contactar direcciones muertas.
    try {
      const b = await isEmailBounced(state.accessToken, email);
      if (b.bounced) {
        res.textContent = `🚫 Cannot send: ${email} is in the bounced emails database (${b.reason || "bounced"}). Use a different address.`;
        res.className   = "push-result error";
        return;
      }
    } catch {}
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

    // ── Tracking pixel para detectar open (necesario para Email Futuro) ──
    // Insertamos primero en toolbar_agent_actions para tener un id estable,
    // después embedeamos el pixel apuntando a /functions/v1/track-open?aid=ID.
    // Si la inserción falla, mandamos igual el email sin pixel (no bloqueante).
    let trackingActionId = null;
    try {
      const tr = await createManualSendTracking(state.accessToken, {
        user_email:    state.loginEmail,
        domain:        state.domain,
        email_to:      email,
        pitch_subject: subject,
        language:      lang,
        // Attribution para toolbar_source_performance: si el email vino de los
        // candidates detectados (apollo/informer/scrape/generic), pasamos ese
        // source. Si el MB lo tipeó a mano, queda "manual" por default.
        email_source:  (state.emailSources?.get(email) || "").toLowerCase() || "manual",
      });
      if (tr.ok && tr.id) {
        trackingActionId = tr.id;
        const pixelUrl = `${CONFIG.SUPABASE_URL}/functions/v1/track-open?aid=${tr.id}`;
        // Inyectamos al final del body, en línea aparte para que Gmail lo procese
        // como HTML inline (1x1 transparent). Si el cliente envía text/plain, queda
        // como string al final que tampoco molesta.
        bodyToSend = `${bodyToSend}\n\n<img src="${pixelUrl}" width="1" height="1" alt="" style="display:none" />`;
      } else {
        console.warn("[tracking] manual send insert failed:", tr.status, tr.error);
      }
    } catch (e) {
      console.warn("[tracking] exception:", e.message);
    }

    btn.textContent = "⏳ Sending...";
    const result = await sendEmail({ to: email, subject, body: bodyToSend, expectedFrom: state.loginEmail });

    if (result.ok) {
      state.emailSentInSession = true; // unlock el push a Monday
      incrementUserDailyCounter(state.accessToken, state.loginEmail, "emails").catch(() => {});
      const today = new Date().toISOString().split("T")[0];
      // Persistir el envío para tracking/historial (sin generar follow-ups,
      // los hace el CRM externo).
      await saveSendDate(state.domain, { sendDate: today, pitch, email }).catch(() => {});
      // Marcar review_queue items de este dominio como contactados — desaparecen
      // de Prospects inmediato (otros MBs no los van a re-contactar).
      markReviewQueueAsContacted(state.accessToken, state.domain, state.loginEmail).catch(() => {});

      // ── Emails Adicionales (hasta 3) — Maxi 2026-06-18 ──────────────
      // CAMBIO DE LÓGICA: ya NO se encolan para +11/+22/+33d. Ahora salen
      // EN PARALELO el día 0, junto con el original. La idea: ganar al que
      // responda primero. Si el ORIGINAL después rebota/OOO, el bounce
      // handler actualiza Monday con el email del que SÍ funcionó.
      //
      // Gaps cubiertos:
      //   - skip si future === original (dedup)
      //   - bounce check antes de cada send (lista global de bounces)
      //   - registro en response_tracking para tracking de conversión
      const futStatusEl = document.getElementById("email-futuro-status");
      const futureSlots = ["form-email-futuro", "form-email-futuro-2", "form-email-futuro-3"];
      const sentMsgs  = [];
      const failMsgs  = [];
      for (const slotId of futureSlots) {
        const futureEmail = document.getElementById(slotId)?.value?.trim()?.toLowerCase();
        if (!futureEmail || !futureEmail.includes("@")) continue;
        if (futureEmail === email.toLowerCase()) { failMsgs.push(`⏭️ ${futureEmail} igual al principal`); continue; }
        const bFut = await isEmailBounced(state.accessToken, futureEmail).catch(() => ({ bounced: false }));
        if (bFut.bounced) { failMsgs.push(`🚫 ${futureEmail} bounced`); continue; }
        // Mandar EL MISMO subject + body al adicional
        const altRes = await sendEmail({ to: futureEmail, subject, body: bodyToSend, expectedFrom: state.loginEmail });
        if (altRes.ok) {
          sentMsgs.push(`✅ ${futureEmail}`);
          incrementUserDailyCounter(state.accessToken, state.loginEmail, "emails").catch(() => {});
          // Registrar también en response_tracking (source = "manual_extra")
          fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_response_tracking`, {
            method: "POST",
            headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({
              mb_email:      state.loginEmail.toLowerCase(),
              domain:        state.domain,
              email_sent_to: futureEmail,
              source:        "manual_extra",
              geo:           document.getElementById("form-geo")?.value || "",
              category:      state.category || "",
              sent_at:       new Date().toISOString(),
            }),
          }).catch(() => {});
        } else {
          failMsgs.push(`❌ ${futureEmail}: ${altRes.error || "send fail"}`);
        }
      }
      if (futStatusEl && (sentMsgs.length || failMsgs.length)) {
        const parts = [];
        if (sentMsgs.length) parts.push(`Adicionales enviados día 0: ${sentMsgs.join(" · ")}`);
        if (failMsgs.length) parts.push(`Problemas: ${failMsgs.join(" · ")}`);
        futStatusEl.textContent = parts.join(" | ");
        futStatusEl.style.color = failMsgs.length && !sentMsgs.length ? "#dc2626" : sentMsgs.length ? "#16a34a" : "#d97706";
      }

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
  document.getElementById("btn-push-all-results")?.addEventListener("click", () => {
    // Abre TODAS las URLs visibles en cascade results (no solo las seleccionadas)
    const allDomains = (cascadeResults || []).map(s => s.domain).filter(Boolean);
    if (!allDomains.length) { alert("No hay sitios en los resultados."); return; }
    if (!confirm(`Abrir ${allDomains.length} tabs?`)) return;
    allDomains.forEach((d, i) => {
      setTimeout(() => chrome.tabs.create({ url: `https://${d}`, active: false }), i * 50);
    });
  });
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

      // Maxi 2026-07-01: carga HUMANA (no feeder) → source "manual", NO "csv".
      // Diego se quejó: "sigue diciendo csv cuando no cargó csv". El default "csv" mentía.
      const upload = await uploadCsvDomains(selected.map(s => s.domain), state.loginEmail, state.accessToken, "manual");

      { const r = formatUploadResult(upload, selected.length); resultEl.textContent = r.msg; resultEl.style.color = r.color; }
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

  // Notification bell — toggle panel, render lista, marcar leída/dismiss.
  initNotificationBell();

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

// ── Notification bell ─────────────────────────────────────────
// Toggle panel, lista, marcar leída/all-read. Auto-refresh badge cada 60s.
let _notifPollTimer = null;

function initNotificationBell() {
  const btn  = document.getElementById("btn-notif-bell");
  const panel = document.getElementById("notif-panel");
  if (!btn || !panel) return;

  btn.addEventListener("click", async (e) => {
    e.stopPropagation();
    const visible = panel.style.display === "flex";
    panel.style.display = visible ? "none" : "flex";
    if (!visible) await renderNotificationPanel();
  });

  // Click fuera cierra
  document.addEventListener("click", (e) => {
    if (!panel.contains(e.target) && e.target.id !== "btn-notif-bell") {
      panel.style.display = "none";
    }
  });

  document.getElementById("btn-notif-mark-all")?.addEventListener("click", async () => {
    if (!state.loginEmail) return;
    await markAllNotificationsRead(state.accessToken, state.loginEmail);
    await renderNotificationPanel();
    await refreshNotificationBadge();
  });

  // Initial + polling
  refreshNotificationBadge();
  if (_notifPollTimer) clearInterval(_notifPollTimer);
  // Maxi 2026-07-03 perf: no pollear el badge de notificaciones cuando el panel está oculto.
  _notifPollTimer = setInterval(() => { if (document.visibilityState !== "hidden") refreshNotificationBadge(); }, 60_000);
}

async function refreshNotificationBadge() {
  const badge = document.getElementById("notif-bell-badge");
  if (!badge || !state.loginEmail || !state.accessToken) return;
  // Maxi 2026-07-03 perf: el badge solo necesita el COUNT de no-leídas, no las 50
  // filas completas. Usamos count=exact (Range 0-0) con filtro read_at server-side
  // → 0 filas de egress en vez de hasta 50 rows con body/message cada 60s × 3 MBs.
  const mb = (state.loginEmail || "").toLowerCase();
  let unread = 0;
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_notifications?mb_email=eq.${encodeURIComponent(mb)}&dismissed_at=is.null&read_at=is.null&select=id`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    if (!res.ok) return;
    const m = (res.headers.get("content-range") || "").match(/\/(\d+)$/);
    unread = m ? parseInt(m[1], 10) : 0;
  } catch { return; }
  if (unread > 0) {
    badge.textContent = unread > 99 ? "99+" : String(unread);
    badge.style.display = "block";
  } else {
    badge.style.display = "none";
  }
}

async function renderNotificationPanel() {
  const list = document.getElementById("notif-list");
  if (!list || !state.loginEmail) return;
  list.innerHTML = `<div style="padding:20px;text-align:center;color:#64748b;font-size:11px">Loading…</div>`;
  const notifs = await fetchNotifications(state.accessToken, state.loginEmail, { limit: 30 });
  if (notifs.length === 0) {
    list.innerHTML = `<div style="padding:24px;text-align:center;color:#64748b;font-size:11px">No notifications yet.</div>`;
    return;
  }
  const sevColor = { info: "#60a5fa", success: "#10b981", warning: "#f59e0b", error: "#ef4444" };
  const sevIcon  = { info: "ℹ️", success: "✅", warning: "⚠️", error: "🚨" };
  const fmtAgo = (iso) => {
    const ms = Date.now() - new Date(iso).getTime();
    const m = Math.floor(ms / 60000);
    if (m < 1) return "just now";
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    return `${Math.floor(h / 24)}d ago`;
  };
  list.innerHTML = notifs.map(n => {
    const isUnread = !n.read_at;
    const color = sevColor[n.severity] || "#60a5fa";
    const icon  = sevIcon[n.severity] || "ℹ️";
    return `<div data-notif-id="${n.id}" style="padding:10px 12px;border-bottom:1px solid #1e293b;${isUnread ? "background:#1e293b" : ""};cursor:pointer" class="notif-item">
      <div style="display:flex;align-items:flex-start;gap:8px">
        <span style="font-size:14px;flex-shrink:0">${icon}</span>
        <div style="flex:1;min-width:0">
          <div style="color:${color};font-weight:600;font-size:12px;line-height:1.3">${esc(n.title)}</div>
          ${n.body ? `<div style="color:#cbd5e1;font-size:11px;margin-top:2px;line-height:1.4">${esc(n.body)}</div>` : ""}
          <div style="color:#64748b;font-size:10px;margin-top:4px">${fmtAgo(n.created_at)}${n.mb_email === "_admin" ? " · admin alert" : ""}</div>
        </div>
        ${isUnread ? `<div style="width:6px;height:6px;border-radius:50%;background:${color};flex-shrink:0;margin-top:4px" title="unread"></div>` : ""}
      </div>
    </div>`;
  }).join("");
  // Bind clicks
  list.querySelectorAll(".notif-item").forEach(el => {
    el.addEventListener("click", async () => {
      const id = el.dataset.notifId;
      await markNotificationRead(state.accessToken, id);
      await renderNotificationPanel();
      await refreshNotificationBadge();
    });
  });
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
    const statusEl = document.getElementById("custom-prompt-status");
    if (promptEl) {
      // Re-fetch del DB cada vez que se abre el modal para mostrar la versión vigente
      // (no la baked). Si el user actualizó vía SQL externo, lo refleja al instante.
      try {
        const fresh = await getCustomPrompt(state.accessToken, GLOBAL_PROMPT_KEY);
        if (fresh && fresh.trim()) {
          state.customPrompt = fresh;
          promptEl.value = fresh;
        } else {
          promptEl.value = state.customPrompt || "";
        }
      } catch {
        promptEl.value = state.customPrompt || "";
      }
    }
    if (statusEl) statusEl.textContent = state.customPrompt
      ? `${state.customPrompt.length} chars saved (GLOBAL — affects the whole team)`
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
    if (!confirm("Reset to baked-in prompt (ADEQ Style default)?")) return;
    const r = await setCustomPrompt(state.accessToken, GLOBAL_PROMPT_KEY, "");
    if (r.ok) {
      state.customPrompt = DIEGO_VOICE_PROMPT;
      taEl.value = DIEGO_VOICE_PROMPT;
      statusEl.textContent = "Reset to default";
    }
  });

  // ── Header 🧠 button: dedicated prompt modal — same logic, simpler UI ──
  // Re-fetcha desde Supabase cada vez que se abre, así refleja edits via SQL.
  const promptBtn   = document.getElementById("btn-prompt-open");
  const promptModal = document.getElementById("prompt-modal");
  const promptOver  = document.getElementById("prompt-modal-overlay");
  const promptClose = document.getElementById("btn-prompt-close");
  const promptSave  = document.getElementById("btn-prompt-save");
  const promptReset = document.getElementById("btn-prompt-reset");
  const promptEdit  = document.getElementById("prompt-editor");
  const promptStat  = document.getElementById("prompt-status");
  if (promptBtn && promptModal) {
    promptBtn.addEventListener("click", async () => {
      promptModal.style.display = "flex";
      promptStat.textContent = "Loading from Supabase...";
      promptEdit.value = "";
      try {
        const fresh = await getCustomPrompt(state.accessToken, GLOBAL_PROMPT_KEY);
        if (fresh && fresh.trim()) {
          promptEdit.value = fresh;
          state.customPrompt = fresh;
          promptStat.textContent = `${fresh.length} chars · loaded from Supabase (GLOBAL)`;
        } else {
          promptEdit.value = state.customPrompt || DIEGO_VOICE_PROMPT;
          promptStat.textContent = `Empty in DB · using baked default (${(state.customPrompt || DIEGO_VOICE_PROMPT).length} chars)`;
        }
        // Read-only para non-admin
        promptEdit.readOnly = state.role !== "admin";
        promptSave.style.display = state.role === "admin" ? "" : "none";
        promptReset.style.display = state.role === "admin" ? "" : "none";
      } catch (e) {
        promptStat.textContent = `Error loading: ${e.message}`;
      }
    });
    [promptClose, promptOver].forEach(el => el?.addEventListener("click", () => { promptModal.style.display = "none"; }));
    promptSave?.addEventListener("click", async () => {
      if (state.role !== "admin") return;
      const value = promptEdit.value.trim();
      promptSave.disabled = true; promptSave.textContent = "⏳ Saving...";
      const r = await setCustomPrompt(state.accessToken, GLOBAL_PROMPT_KEY, value);
      promptSave.disabled = false; promptSave.textContent = "💾 Save to Supabase";
      if (!r.ok) { promptStat.textContent = `❌ Save failed (${r.status || r.error})`; return; }
      state.customPrompt = value;
      promptStat.textContent = `✅ Saved · ${value.length} chars`;
      logAuditEvent(state.accessToken, {
        user_email: state.loginEmail, action: "edit_global_prompt",
        details: { length: value.length, source: "header_button" },
      });
    });
    promptReset?.addEventListener("click", async () => {
      if (state.role !== "admin") return;
      if (!confirm("Reset to baked-in default?")) return;
      promptEdit.value = DIEGO_VOICE_PROMPT;
      promptStat.textContent = `Reset shown — click Save to persist.`;
    });
  }
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

// Búsqueda sobre TODO el contenido disponible (user 2026-06-19): built-in de los 12
// idiomas (getKeywords) + importados de DB + complemento de Supabase. Antes sólo
// consultaba la DB, así que el search no encontraba idiomas que están en el built-in
// (aparecían en la rotación pero no al filtrar).
async function runKeywordSearch() {
  const lang   = document.getElementById("kw-language").value;
  const search = document.getElementById("kw-search").value.trim();
  if (!search) { filterKeywords(); return; }
  const term = search.toLowerCase();

  // 1) Pool completo local: built-in (todos los idiomas) + lo importado de DB. Dedup por frase.
  const seen = new Set();
  const results = [];
  const pushAll = (arr, db) => {
    for (const k of (Array.isArray(arr) ? arr : [])) {
      const kw = k && k.kw;
      if (typeof kw !== "string" || !kw) continue;
      const key = kw.toLowerCase();
      if (seen.has(key)) continue;
      if (!key.includes(term)) continue;
      if (lang && k.lang && k.lang !== lang) continue;
      seen.add(key);
      results.push({ kw, lang: k.lang || "", db });
    }
  };
  pushAll(getKeywords(""), false);   // built-in 12 idiomas (~3.490 frases)
  pushAll(dbKeywords, true);         // keywords importados a la DB

  // 2) Complemento opcional: frases que viven sólo en Supabase (no rompe si falla).
  try {
    const { rows, error } = await searchKeywordsInDB(search, lang);
    if (!error && Array.isArray(rows)) {
      for (const r of rows) {
        const phrase = r && r.phrase;
        if (typeof phrase !== "string" || !phrase) continue;
        const key = phrase.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        results.push({ kw: phrase, lang: r.lang || "en", db: true });
      }
    }
  } catch {}

  if (results.length === 0) {
    document.getElementById("keywords-list").innerHTML =
      '<span class="kw-empty">No results — no keyword matches in any language</span>';
    return;
  }
  renderKeywords(results, term);
}

function filterKeywords() {
  const lang = document.getElementById("kw-language").value;

  // Pool: SIEMPRE built-in (12 idiomas) + lo importado de DB, deduplicado por frase.
  // Antes usaba SOLO dbKeywords cuando había imports → al filtrar por un idioma que no
  // estaba en la DB (polish, german, etc.) no aparecía nada aunque el built-in lo tuviera.
  const seen = new Set();
  let pool = [];
  const addPool = (arr, db) => {
    for (const k of (Array.isArray(arr) ? arr : [])) {
      if (typeof k.kw !== "string" || !k.kw) continue;
      const key = k.kw.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      pool.push({ kw: k.kw, lang: k.lang || "", db });
    }
  };
  addPool(getKeywords("").map(k => ({ ...k, db: false })), false);
  addPool(dbKeywords, true);

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
    el.innerHTML += `<span class="kw-empty" style="margin-top:4px">… and ${kws.length - limit} more. Refine search.</span>`;
  }
  el.querySelectorAll(".kw-chip").forEach(btn => {
    btn.addEventListener("click", () => searchGoogleForDomain(btn.dataset.kw, document.getElementById("kw-country")?.value || ""));
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
          <span class="sc-lbl">+350K</span>
        </div>
        <div class="stat-card stat-card--monday">
          <span class="sc-num">${monthMondayNQ}</span>
          ${pct(monthMondayNQ, monthNewQual)}
          <span class="sc-lbl">→ Monday</span>
        </div>
        <div class="stat-card stat-card--neutral">
          <span class="sc-num">${monthNewBelow}</span>
          ${pct(monthNewBelow, monthNew)}
          <span class="sc-lbl">&lt;350K</span>
        </div>
      </div>
    </div>

    <div class="stat-category-block">
      <div class="stat-cat-header stat-cat-dup">🔵 Duplicates <span class="stat-cat-count">${monthDups}</span></div>
      <div class="stat-grid">
        <div class="stat-card stat-card--qual">
          <span class="sc-num">${monthDupQual}</span>
          ${pct(monthDupQual, monthDups)}
          <span class="sc-lbl">+350K</span>
        </div>
        <div class="stat-card stat-card--monday">
          <span class="sc-num">${monthMondayDQ}</span>
          ${pct(monthMondayDQ, monthDupQual)}
          <span class="sc-lbl">→ Monday</span>
        </div>
        <div class="stat-card stat-card--neutral">
          <span class="sc-num">${monthDupBelow}</span>
          ${pct(monthDupBelow, monthDups)}
          <span class="sc-lbl">&lt;350K</span>
        </div>
      </div>
    </div>

    <div class="stat-category-block">
      <div class="stat-cat-header stat-cat-below">🔻 Below 350K <span class="stat-cat-count">${monthBelow}</span></div>
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
  // Filters opcionales — la sección de filtros está oculta (display:none) desde
  // el cleanup 2026-05-08, así que estos elementos pueden no existir.
  const [tMin, tMax] = parseRange(document.getElementById("cascade-min-traffic")?.value || "");
  const [rMin, rMax] = parseRange(document.getElementById("cascade-max-rank")?.value    || "");
  const langFilter = document.getElementById("cascade-language")?.value || "";

  btn.disabled = true; btn.textContent = "⏳";
  cascadeResults = []; cascadeRawResults = []; cascadeSelected = new Set(); cascadeBlockedExecSet = new Set();
  cascadePage = 1;
  resultsEl.innerHTML = ""; actionsEl.style.display = "none";
  { const nav = document.getElementById("cascade-pagination"); if (nav) nav.style.display = "none"; }

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

  const CASCADE_LIMIT = 150; // Maxi 2026-06-19: 50→150 (mostrar más similares; el scrape es gratis)

  // Cargar índice de Monday para filtrar dominios de otros ejecutivos (últimos 45 días).
  // Si falla (timeout/401/red), seguimos igual con índice vacío — no bloqueamos al MB.
  statusEl.textContent = "Step 1/2: checking Monday for other MBs' active domains...";
  const boardIndex = await Promise.race([
    getMondayBoardIndex(),
    new Promise((_, reject) => setTimeout(() => reject(new Error("Monday lookup timeout 20s")), 20000)),
  ]).catch((e) => {
    console.warn("[cascade] Monday board index failed:", e.message);
    statusEl.textContent = `⚠ Monday no respondió (${e.message}) — sigo sin filtro de ejecutivos`;
    return new Map();
  });
  let filteredCount = 0;

  // Bloqueado SI: está en Monday Y tiene estado distinto a "Ciclo Finalizado".
  // Razón: si está en ciclo activo (LIVE / Propuesta / En Negociación / Rebotado /
  // Descartado / etc.) no se debe re-prospectar. Solo los "Ciclo Finalizado" son
  // recyclables y aparecen en cascade aunque ya estén cargados en Monday.
  const isBlockedByExec = (domain) => {
    const clean = domain.replace(/^www\./, "").toLowerCase();
    const info  = boardIndex.get(clean);
    if (!info) return false;                         // no está en Monday → libre
    const estado = (info.estado || "").trim();
    if (!estado) return false;                       // sin estado → no bloquear
    return estado.toLowerCase() !== "ciclo finalizado"; // bloquea todo lo demás
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
      actionsEl.style.display = "none";
    } else {
      const limitMsg = cascadeResults.length >= CASCADE_LIMIT ? ` (limit ${CASCADE_LIMIT})` : "";
      statusEl.textContent = `✅ ${cascadeResults.length} prospects${limitMsg}${filteredCount ? ` · ${filteredCount} filtered out` : ""}`;
      // Maxi 2026-06-22: re-render PAGINADO (30/página) — reemplaza el volcado streaming.
      renderCascadePage(1);
      // Defensive: forzar visibilidad + setAttribute para evitar que algún
      // CSS override mate el display. User reportó botones invisibles 2026-05-13.
      actionsEl.style.cssText = "display: block !important; margin-top: 10px";
      actionsEl.removeAttribute("hidden");
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

  // Cascade post-refactor 2026-05-08: NO enriquece (ahorra hits RapidAPI).
  // Antes había columnas visits/rank/country pero salían siempre "?", "—", "—".
  // Ahora simplificado: checkbox + favicon + domain + botón "Open" individual.
  item.innerHTML = `
    <input type="checkbox" />
    <img class="cascade-favicon" loading="lazy" src="https://www.google.com/s2/favicons?domain=${esc(site.domain)}&sz=16" onerror="this.style.display='none'" />
    <span class="cascade-domain" title="${esc(site.domain)}" style="flex:1">${esc(site.domain)}</span>
    <button class="cascade-open-one btn btn-sm btn-secondary" title="Abrir esta URL" style="padding:2px 8px;font-size:11px">↗ Open</button>
  `;
  item.querySelector(".cascade-open-one")?.addEventListener("click", (e) => {
    e.stopPropagation();
    chrome.tabs.create({ url: `https://${site.domain}`, active: false });
  });

  const cb = item.querySelector("input");
  // Maxi 2026-06-22: reflejar la selección (persiste al cambiar de página)
  cb.checked = cascadeSelected.has(site.domain);
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

// Maxi 2026-06-22: render PAGINADO de los similares — máx 30 por página + nav 1-2-3.
function renderCascadePage(page) {
  const resultsEl = document.getElementById("cascade-results");
  if (!resultsEl) return;
  const total = cascadeResults.length;
  const pages = Math.max(1, Math.ceil(total / CASCADE_PAGE_SIZE));
  cascadePage = Math.min(Math.max(1, page), pages);
  const start = (cascadePage - 1) * CASCADE_PAGE_SIZE;
  const slice = cascadeResults.slice(start, start + CASCADE_PAGE_SIZE);

  resultsEl.innerHTML = "";
  for (const site of slice) appendCascadeItem(site, resultsEl);

  // Barra de paginación (se crea/actualiza debajo de la lista)
  let nav = document.getElementById("cascade-pagination");
  if (!nav) {
    nav = document.createElement("div");
    nav.id = "cascade-pagination";
    nav.style.cssText = "display:flex;flex-wrap:wrap;gap:4px;justify-content:center;align-items:center;margin:10px 0 4px";
    resultsEl.parentNode.insertBefore(nav, resultsEl.nextSibling);
  }
  if (pages <= 1) { nav.innerHTML = ""; nav.style.display = "none"; return; }
  nav.style.display = "flex";
  const btn = (label, target, disabled, active) =>
    `<button class="cascade-page-btn" data-page="${target}" ${disabled ? "disabled" : ""}
       style="min-width:26px;padding:3px 7px;font-size:11px;border-radius:4px;cursor:${disabled ? "default" : "pointer"};
       border:1px solid ${active ? "#0ea5e9" : "#334155"};background:${active ? "#0ea5e9" : "#1e293b"};
       color:${active ? "#fff" : "#cbd5e1"};font-weight:${active ? 700 : 400};opacity:${disabled ? .4 : 1}">${label}</button>`;
  // Ventana de páginas alrededor de la actual (compacta)
  const win = [];
  const from = Math.max(1, cascadePage - 2), to = Math.min(pages, cascadePage + 2);
  for (let p = from; p <= to; p++) win.push(p);
  let html = btn("‹", cascadePage - 1, cascadePage === 1, false);
  if (from > 1) html += btn("1", 1, false, cascadePage === 1) + (from > 2 ? `<span style="color:#64748b">…</span>` : "");
  for (const p of win) html += btn(String(p), p, false, p === cascadePage);
  if (to < pages) html += (to < pages - 1 ? `<span style="color:#64748b">…</span>` : "") + btn(String(pages), pages, false, cascadePage === pages);
  html += btn("›", cascadePage + 1, cascadePage === pages, false);
  html += `<span style="font-size:10px;color:#64748b;margin-left:6px">${total} sitios</span>`;
  nav.innerHTML = html;
  nav.querySelectorAll(".cascade-page-btn").forEach(b => {
    b.addEventListener("click", () => { if (!b.disabled) renderCascadePage(parseInt(b.dataset.page, 10)); });
  });
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
  // Maxi 2026-07-01: cascade-min-traffic / cascade-max-rank fueron removidos del HTML
  // (cleanup 2026-05-08) → sin ?. esto CRASHEABA "Apply filters" con TypeError. Guardado.
  const trafficVal = document.getElementById("cascade-min-traffic")?.value || "";
  const rankVal    = document.getElementById("cascade-max-rank")?.value || "";
  const langFilter = document.getElementById("cascade-language")?.value || "";
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

  // Re-renderizar desde cero — PAGINADO (Maxi 2026-06-22: 30/página + nav)
  cascadeResults  = filtered.slice();
  cascadeSelected = new Set();
  resultsEl.innerHTML = "";
  if (cascadeResults.length === 0) {
    resultsEl.innerHTML = '<div class="cascade-empty">No results match the current filters.</div>';
    actionsEl.style.display = "none";
    const nav = document.getElementById("cascade-pagination"); if (nav) nav.style.display = "none";
  } else {
    renderCascadePage(1);
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

  // FIX 2026-05-18: limpiar los campos en cada apertura del login screen y
  // bloquear autofill. Caso reportado: mgargiulo abrió la extensión, Chrome
  // tenía autofill con email/password de Agus, mgargiulo no lo vio y entró
  // a la cuenta equivocada. Forzar input manual cada vez evita el mix-up.
  const emailIn = document.getElementById("login-email");
  const passIn  = document.getElementById("login-password");
  if (emailIn) {
    emailIn.value = "";
    emailIn.setAttribute("autocomplete", "off");
    emailIn.setAttribute("autocapitalize", "off");
    emailIn.setAttribute("spellcheck", "false");
    // Algunos managers (1Password, LastPass) ignoran autocomplete=off → reforzar:
    emailIn.setAttribute("name", "adeq-login-email-" + Date.now());
  }
  if (passIn) {
    passIn.value = "";
    passIn.setAttribute("autocomplete", "new-password");
    passIn.setAttribute("name", "adeq-login-pwd-" + Date.now());
  }
  setTimeout(() => emailIn?.focus(), 100);

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

    // Verificación de identidad: lo que Supabase autenticó tiene que ser
    // EL MISMO email que se tipeó. Si no matcha (sesión cacheada cruzada,
    // mix-up de credenciales, etc.) abortamos para no entrar como otro user.
    // Bug reportado 2026-05-18: mgargiulo tipeaba su email pero entraba
    // como sales@adeqmedia.com (Agus).
    const realEmail = (result.authenticated_email || "").toLowerCase();
    if (realEmail && realEmail !== email.toLowerCase()) {
      errorEl.textContent = `⚠️ Sesión cruzada: Supabase autenticó "${realEmail}", no "${email}". Cerrá y reabrí la extensión y reintenta.`;
      btn.disabled = false; btn.textContent = "Sign In";
      // Limpiar cualquier auth viejo que pudiera estar contaminando
      await chrome.storage.local.remove("auth").catch(() => {});
      return;
    }

    const auth = {
      loggedIn:     true,
      user:         realEmail || email, // fuente de verdad: el email autenticado
      name:         AUTHORIZED[realEmail || email],
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
  // Hide History button for admin (pedido user 2026-05-18 — botón innecesario
  // para mgargiulo). Otros MBs lo siguen viendo.
  if (state.role === "admin") {
    const hb = document.getElementById("btn-history-open");
    if (hb) hb.style.display = "none";
  }
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

  // Maxi 2026-07-03 perf: cache TTL para el breakdown "por qué se descartaron hoy".
  // El heartbeat (10s con AUTO ON) traía hasta 2000 filas error_message CADA tick.
  // El breakdown es un diagnóstico de movimiento lento → lo cacheamos 30s. Los contadores
  // (pending/processing/mis uploads) siguen refrescando en cada tick.
  let _skipBreakdownCache = { ts: 0, html: "" };
  const SKIP_BREAKDOWN_TTL = 30_000;

  const refreshStats = async () => {
    statsEl.textContent = "Loading...";
    const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
    const myEmail = (state.loginEmail || "").toLowerCase();
    const todayStartIso = new Date(new Date().setHours(0,0,0,0)).toISOString();
    const userClause = `uploaded_by=eq.${encodeURIComponent(myEmail)}`;
    const [stats, reviewPending, myPendingRes, myDoneRes, mySkippedRes, myNextDayRes] = await Promise.all([
      getCsvQueueStats(state.accessToken),
      import("../modules/supabase.js").then(m => m.getReviewQueuePendingCount(state.accessToken)).catch(() => 0),
      // Mis uploads hoy — per-status counts
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?${userClause}&status=in.(pending,processing,waiting_pool)&uploaded_at=gte.${todayStartIso}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?${userClause}&status=eq.done&processed_at=gte.${todayStartIso}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?${userClause}&status=in.(skipped,error)&processed_at=gte.${todayStartIso}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?${userClause}&status=eq.next_day&uploaded_at=gte.${todayStartIso}&select=id`, { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }),
    ]);
    const csvPending    = stats.pending || 0;
    const csvProcessing = stats.processing || 0;
    const waitlistCount = stats.waiting_pool || 0;
    const parseCount = (r) => { const m = (r.headers.get("content-range") || "").match(/\/(\d+)$/); return m ? parseInt(m[1]) : 0; };
    const myPending = parseCount(myPendingRes);
    const myDone = parseCount(myDoneRes);
    const mySkipped = parseCount(mySkippedRes);
    const myNextDay = parseCount(myNextDayRes);
    const myTotal = myPending + myDone + mySkipped + myNextDay;

    // Maxi 2026-06-19: DIAGNÓSTICO "¿por qué 0 en Prospects?". Agrupa el error_message
    // de los descartados HOY (todos: imports + worker) por motivo, para ver el embudo real.
    let skipBreakdownHtml = "";
    if (Date.now() - _skipBreakdownCache.ts < SKIP_BREAKDOWN_TTL) {
      skipBreakdownHtml = _skipBreakdownCache.html;
    } else try {
      const r = await fetch(
        `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=in.(skipped,error,frozen)&processed_at=gte.${todayStartIso}&select=error_message&limit=2000`,
        { headers }
      );
      const rows = r.ok ? await r.json() : [];
      if (Array.isArray(rows) && rows.length) {
        // Normaliza el error_message a una "razón" corta (el texto antes de ":" o "—").
        const bucket = {};
        const label = (msg) => {
          const m = (msg || "").toLowerCase();
          if (!m) return "sin motivo";
          if (m.includes("pageviews") && m.includes("below")) return "🚦 tráfico < 350K";
          if (m.includes("no_traffic") || m.includes("sin traffic")) return "📉 sin dato de tráfico";
          if (m.includes("frozen") || m.includes("freeze")) return "🧊 congelado (3 fallos tráfico)";
          if (m.includes("deprio-geo")) return "🌎 GEO USA/UK/CA/AU/NZ/IE";
          if (m.includes("geo_saturated")) return "⚖️ GEO saturado en pool";
          if (m.includes("not_publisher") || m.includes("publisher_signal") || m.includes("haiku_")) return "🗑 no es publisher (filtro basura)";
          if (m.includes("category-blocked")) return "🏦 categoría bloqueada";
          if (m.includes("en monday") || m.includes("estado=")) return "📋 ya en Monday (ciclo activo)";
          if (m.includes("blocked:")) return "🚫 en blocklist";
          if (m.includes("unreachable") || m.includes("site_unreachable")) return "💀 sitio caído / sin emails";
          return "❓ otro: " + m.slice(0, 30);
        };
        rows.forEach(row => { const k = label(row.error_message); bucket[k] = (bucket[k] || 0) + 1; });
        const sorted = Object.entries(bucket).sort((a, b) => b[1] - a[1]);
        skipBreakdownHtml = `
          <div style="margin-top:8px;padding-top:6px;border-top:1px dashed var(--border);font-size:11px">
            <strong style="color:#dc2626">🔎 Por qué se descartaron hoy (${rows.length}):</strong>
            ${sorted.map(([k, n]) => `<div style="margin-left:8px;color:var(--text-muted)">• ${k} — <strong>${n}</strong></div>`).join("")}
          </div>`;
      }
      _skipBreakdownCache = { ts: Date.now(), html: skipBreakdownHtml };
    } catch {}

    // Maxi 2026-06-18: quitada la línea "Prospects (To Review)" — duplicada con el tab counter
    statsEl.innerHTML = `
      <div style="margin-bottom:4px"><strong>⚙️ Processing queue:</strong> ${csvPending}/${CSV_PENDING_CAP} pending${csvProcessing ? ` + ${csvProcessing} processing` : ""}</div>
      <div style="margin-bottom:4px"><strong>⏳ Waitlist:</strong> ${waitlistCount}/${WAITLIST_CAP} waiting (auto-promotes when freed)</div>
      ${myTotal > 0 ? `
      <div style="margin-top:8px;padding-top:6px;border-top:1px dashed var(--border);font-size:11px">
        <strong style="color:#3b82f6">👤 My uploads today:</strong>
        ${myPending > 0 ? ` <span style="color:#f59e0b">${myPending} in queue</span> ·` : ""}
        ${myDone > 0 ? ` <span style="color:#16a34a">${myDone} done</span> ·` : ""}
        ${myNextDay > 0 ? ` <span style="color:#8b5cf6">${myNextDay} tomorrow</span> ·` : ""}
        ${mySkipped > 0 ? ` <span style="color:#94a3b8">${mySkipped} skipped</span> ·` : ""}
        <span style="opacity:.7">(total ${myTotal})</span>
      </div>` : ""}
      ${skipBreakdownHtml}
    `;
  };

  // ── ACTIVITY VIEW (2026-05-18) ────────────────────────────────
  // Reemplazo del raw item dump por resumen agrupado por MB + Agent.
  // Fuente: toolbar_import_attempts (intentos humanos) + toolbar_feeder_runs (Agent).
  const HISTORY_MBS = [
    { email: "mgargiulo@adeqmedia.com", name: "Maxi" },
    { email: "sales@adeqmedia.com",     name: "Agus" },
    { email: "dhorovitz@adeqmedia.com", name: "Diego" },
  ];
  const HISTORY_SOURCE_LABEL = {
    csv:          { icon: "📥", label: "CSV upload" },
    manual:       { icon: "✋", label: "Import manual" },
    sellers_json: { icon: "📋", label: "Sellers.json" },
    monday:       { icon: "🔄", label: "Monday refresh" },
  };
  let _historyRange = "today"; // "today" | "7days"

  function _madridStartOfToday() {
    const fmt = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit",
    });
    const dateStr = fmt.format(new Date());
    return new Date(`${dateStr}T00:00:00Z`).toISOString();  // ~ today 00:00 Madrid (approx)
  }

  async function _fetchActivityData(sinceISO) {
    const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
    const [attemptsRes, feederRes] = await Promise.all([
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_import_attempts?at=gte.${sinceISO}&select=*&order=at.desc&limit=500`, { headers }),
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_feeder_runs?cron_at=gte.${sinceISO}&select=*&order=cron_at.desc&limit=300`, { headers }), // Maxi 2026-07-03 perf: cap de seguridad (ya viene date-bounded, pero evita un scan sin techo)
    ]);
    const attempts = attemptsRes.ok ? await attemptsRes.json() : [];
    const feeders  = feederRes.ok  ? await feederRes.json()  : [];
    return { attempts, feeders };
  }

  // Maxi 2026-06-18: "to Prospects" se calcula AHORA mismo desde review_queue
  // (no del campo effective_added que se mide 30 min después del cron y queda
  // NULL en los runs recientes → bug de "0 to Prospects" cuando los runs
  // estaban activos pero sin medir).
  async function _renderAgentBlock(feeders) {
    if (!feeders || feeders.length === 0) {
      return `
        <div style="padding:8px;border:1px solid #6366f1;border-radius:6px;margin-bottom:8px;background:rgba(99,102,241,0.06)">
          <div style="font-weight:600;color:#6366f1">🤖 Agent</div>
        </div>`;
    }
    const okRuns = feeders.filter(r => r.status === "ok");
    const grossTotal     = okRuns.reduce((s, r) => s + (parseInt(r.gross_total, 10)     || 0), 0);
    const grossSellers   = okRuns.reduce((s, r) => s + (parseInt(r.gross_sellers, 10)   || 0), 0);
    const grossMonday    = okRuns.reduce((s, r) => s + (parseInt(r.gross_monday, 10)    || 0), 0);
    const grossMajestic  = okRuns.reduce((s, r) => s + (parseInt(r.gross_majestic, 10)  || 0), 0);

    // Calcular effectiveTotal en vivo desde review_queue (creados hoy con source del worker)
    // En vez de leer effective_added que puede ser NULL para runs recientes.
    let effectiveTotal = 0;
    try {
      const todayStart = new Date(); todayStart.setHours(0,0,0,0);
      const since = todayStart.toISOString();
      const headers = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` };
      const rqRes = await fetch(
        `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_review_queue?created_at=gte.${since}&source=in.(csv,autopilot,sellers_json,monday_refresh,autogoogle)&select=id`,
        { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }
      );
      const rangeHdr = rqRes.headers.get("content-range") || "";
      effectiveTotal = parseInt(rangeHdr.match(/\/(\d+)$/)?.[1] || "0", 10);
    } catch {}

    const conv = grossTotal > 0 ? (effectiveTotal / grossTotal * 100).toFixed(1) : "—";
    const skipped = feeders.length - okRuns.length;
    const last = feeders[0];
    const lastTime = last?.slot_label?.slice(11) || "—";
    const lastStatus = last?.status === "ok" ? "✅ ok" : (last?.status || "—");
    return `
      <div style="padding:8px;border:1px solid #6366f1;border-radius:6px;margin-bottom:8px;background:rgba(99,102,241,0.06)">
        <div style="font-weight:600;color:#6366f1">🤖 Agent</div>
        <div style="font-size:11px;margin-top:4px;line-height:1.6">
          ${okRuns.length} cron${okRuns.length === 1 ? "" : "s"} ok${skipped > 0 ? ` (${skipped} skipped)` : ""} ·
          <strong>${grossTotal}</strong> imported →
          <strong style="color:#16a34a">${effectiveTotal}</strong> to Prospects ·
          conv ${conv}%
        </div>
        <div style="font-size:10px;color:var(--text-muted);margin-top:2px">
          └─ sellers ${grossSellers} · Monday ${grossMonday} · Majestic ${grossMajestic} · last ${lastTime} ${lastStatus}
        </div>
      </div>`;
  }

  function _renderMbBlock(name, email, attempts) {
    const mine = attempts.filter(a => (a.user_email || "").toLowerCase() === email);
    if (mine.length === 0) {
      return `<div style="font-size:11px;padding:4px 0;color:var(--text-muted)">👤 <strong>${name}</strong> no activity today</div>`;
    }
    const grouped = {};
    mine.forEach(a => {
      const src = a.source || "csv";
      if (!grouped[src]) grouped[src] = { attempted: 0, deduped: 0, inserted: 0, batches: 0, details: new Set() };
      grouped[src].attempted += parseInt(a.attempted_count, 10) || 0;
      grouped[src].deduped   += parseInt(a.deduped_count,   10) || 0;
      grouped[src].inserted  += parseInt(a.inserted_count,  10) || 0;
      grouped[src].batches++;
      if (a.source_detail) grouped[src].details.add(a.source_detail);
    });
    const totalAtt = mine.reduce((s, a) => s + (parseInt(a.attempted_count, 10) || 0), 0);
    const totalIns = mine.reduce((s, a) => s + (parseInt(a.inserted_count,  10) || 0), 0);
    const lines = Object.entries(grouped).map(([src, g]) => {
      const { icon, label } = HISTORY_SOURCE_LABEL[src] || { icon: "•", label: src };
      const detailStr = g.details.size > 0 && g.details.size <= 3 ? ` (${[...g.details].join(", ")})` : "";
      const insertedNote = g.inserted === 0 && g.attempted > 0
        ? `<span style="color:#d97706">${g.attempted} attempts · all duplicates</span>`
        : `${g.attempted} attempts → <strong>${g.inserted}</strong> queued`;
      const batchNote = g.batches > 1 ? ` · ${g.batches}×` : "";
      return `<div style="margin-left:14px;font-size:11px;line-height:1.6">${icon} ${label}${detailStr}: ${insertedNote}${batchNote}</div>`;
    }).join("");
    return `
      <div style="margin:8px 0;padding-bottom:6px;border-bottom:1px dashed var(--border)">
        <div style="font-weight:600;font-size:12px">👤 ${name}</div>
        ${lines}
        <div style="margin-left:14px;font-size:10px;color:var(--text-muted);margin-top:2px">Total: ${totalAtt} attempts · ${totalIns} queued</div>
      </div>`;
  }

  async function _renderActivityToday(attempts, feeders) {
    let html = await _renderAgentBlock(feeders);
    HISTORY_MBS.forEach(mb => { html += _renderMbBlock(mb.name, mb.email, attempts); });
    return html;
  }

  async function _renderActivity7Days(attempts, feeders) {
    // Agrupar por fecha (YYYY-MM-DD Madrid)
    const dateOf = (iso) => {
      try {
        return new Date(iso).toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
      } catch { return ""; }
    };
    const byDate = {};
    attempts.forEach(a => {
      const d = dateOf(a.at);
      if (!d) return;
      if (!byDate[d]) byDate[d] = { attempts: [], feeders: [] };
      byDate[d].attempts.push(a);
    });
    feeders.forEach(f => {
      const d = dateOf(f.cron_at);
      if (!d) return;
      if (!byDate[d]) byDate[d] = { attempts: [], feeders: [] };
      byDate[d].feeders.push(f);
    });
    const dates = Object.keys(byDate).sort().reverse();
    if (dates.length === 0) return `<div style="color:var(--text-muted);font-style:italic;padding:8px">No activity in the last 7 days.</div>`;
    const blocks = await Promise.all(dates.map(async d => `
      <details style="margin-bottom:6px;border:1px solid var(--border);border-radius:6px;padding:6px" ${d === dates[0] ? "open" : ""}>
        <summary style="cursor:pointer;font-weight:600;font-size:12px">${d}</summary>
        <div style="margin-top:6px">
          ${await _renderActivityToday(byDate[d].attempts, byDate[d].feeders)}
        </div>
      </details>
    `));
    return blocks.join("");
  }

  const refreshHistory = async () => {
    if (!historyEl) return;
    historyEl.textContent = "Loading...";
    const titleEl = document.getElementById("history-title");
    const toggleBtn = document.getElementById("btn-csv-history-toggle-range");
    if (titleEl) titleEl.textContent = _historyRange === "today" ? "Import Activity Today" : "Import Activity — Last 7 days";
    if (toggleBtn) toggleBtn.textContent = _historyRange === "today" ? "View last 7 days" : "View today only";
    const sinceISO = _historyRange === "today"
      ? _madridStartOfToday()
      : new Date(Date.now() - 7 * 86400_000).toISOString();
    try {
      const { attempts, feeders } = await _fetchActivityData(sinceISO);
      historyEl.innerHTML = _historyRange === "today"
        ? await _renderActivityToday(attempts, feeders)
        : await _renderActivity7Days(attempts, feeders);
    } catch (e) {
      historyEl.innerHTML = `<div style="color:#e53e3e">Error: ${esc(e.message || String(e))}</div>`;
    }
  };

  document.getElementById("btn-csv-history-toggle-range")?.addEventListener("click", () => {
    _historyRange = _historyRange === "today" ? "7days" : "today";
    refreshHistory();
  });

  const refreshAll = async () => { await Promise.all([refreshStats(), refreshHistory()]); };

  // Auto-refresh cada 10s cuando AUTO ON está activo
  let heartbeatTimer = null;
  const startHeartbeat = () => {
    if (heartbeatTimer) return;
    // Maxi 2026-07-03 perf: 10s → 20s. Con AUTO ON dispara refreshStats+refreshHistory
    // (varios count-scans + activity fetch) en cada tick × 3 MBs. 20s corta esa carga a
    // la mitad sobre la base Micro; la percepción de "live" del import sigue fluida.
    heartbeatTimer = setInterval(() => {
      if (document.visibilityState === "hidden") return;
      refreshStats(); refreshHistory();
    }, 20_000);
  };
  const stopHeartbeat = () => { if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; } };

  // Estado inicial del toggle + mutex check
  const refreshCsvMutex = async () => {
    // Maxi 2026-06-19: el toggle visible de AUTO IMPORT se eliminó (2026-06-18;
    // el worker prende/apaga el flag solo). Antes esta función pintaba un cartel
    // "🔒 Locked by ... apagar este toggle" al lado de un input ahora OCULTO →
    // aparecía suelto y confundía. Quitamos el cartel (era código muerto de UI).
    // Mantenemos el sync del hidden input por compat con el resto de popup.js.
    const st = await import("../modules/supabase.js").then(m => m.getCsvQueueState(state.accessToken)).catch(() => null);
    if (!st) return;
    enabledCbx.checked = st.enabled;
    // Limpiar cartel viejo si quedó renderizado de una versión anterior.
    document.getElementById("csv-queue-lock-badge")?.remove();
  };
  await refreshCsvMutex();
  if (enabledCbx.checked) startHeartbeat();
  enabledCbx.addEventListener("change", async () => {
    // Pre-check: si autopilot está corriendo, queue queda bloqueado hasta que termine.
    // El worker no chequea flags durante una sesión de autopilot (loop sync ~20min).
    if (enabledCbx.checked) {
      try {
        const ap = await getAutopilotState(state.accessToken);
        // No usar heartbeat staleness — el heartbeat no se update durante la
        // sesión interna de autopilot (loop sync ~20min). Confiamos en `enabled`.
        const apActive = !!ap?.enabled;
        if (apActive) {
          const owner = ap.sessionUser ? ap.sessionUser.split("@")[0] : "someone";
          const elapsed = ap.sessionStart ? Math.round((Date.now() - ap.sessionStart.getTime()) / 60000) : 0;
          const remaining = Math.max(0, 20 - elapsed);
          alert(`⏳ Autopilot is currently running (${owner}, ~${remaining} min left).\n\nQueue cannot start until autopilot finishes its session. Wait for it to end or turn autopilot OFF first.`);
          enabledCbx.checked = false;
          return;
        }
      } catch {}
    }
    // Pre-check mutex antes de tocar
    const st = await import("../modules/supabase.js").then(m => m.getCsvQueueState(state.accessToken)).catch(() => null);
    if (st && st.enabled && st.sessionUser
        && st.sessionUser.toLowerCase() !== (state.loginEmail || "").toLowerCase()) {
      alert(`🔒 ${st.sessionUser} turned on Auto Import. Only that user can turn it off.`);
      enabledCbx.checked = true; // forzar a quedar visualmente ON
      return;
    }
    // Gate horario operativo (solo al prender)
    if (enabledCbx.checked && !(await _checkManualOverrideIfOutside())) {
      enabledCbx.checked = false;
      return;
    }
    // setCsvQueueEnabled ahora devuelve bool. Si falla la save (red, RLS, etc.),
    // revertimos el checkbox local + mostramos error — antes quedaba ON visual
    // pero la DB no se updateaba, y al siguiente poll se destildaba "solo".
    const desired = enabledCbx.checked;
    const ok = await setCsvQueueEnabled(desired, state.accessToken, state.loginEmail);
    if (!ok) {
      enabledCbx.checked = !desired;
      showToast(`❌ No se pudo ${desired ? "activar" : "desactivar"} Auto Import. Reintentá; si persiste, revisá sesión/permisos o pedile a un admin que verifique RLS.`, "error");
      return;
    }
    if (enabledCbx.checked) startHeartbeat();
    else stopHeartbeat();
    await refreshCsvMutex();
  });
  // Polling cada 30s para refrescar el lock badge si otro MB cambia el toggle
  // Maxi 2026-07-03 perf: skip cuando el panel está oculto (no hay UI que actualizar).
  setInterval(() => { if (document.visibilityState !== "hidden") refreshCsvMutex(); }, 30_000);

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

      // Cap por tirada actualizado a 500 (user 2026-05-13: 200 csv_queue + 300 waitlist).
      // Pre-check de capacidad real (queue + waitlist) se hace abajo en getCsvQueueStats.
      if (unique.length > 500) {
        uploadRes.innerHTML = `❌ <strong>Too many domains:</strong> ${unique.length} found. Per-batch limit is <strong>500 domains</strong> (200 queue + 300 waitlist). Split your CSV into smaller files.`;
        uploadRes.className = "push-result error";
        return;
      }
      // Pre-check capacity: csv pending (max 200) + waitlist (max 300) = 500 total
      const _stats = await getCsvQueueStats(state.accessToken);
      const _pending = _stats?.pending || 0;
      const _waiting = _stats?.waiting_pool || 0;
      const _capacityTotal = CSV_PENDING_CAP + WAITLIST_CAP; // 1000
      if (_pending + _waiting + unique.length > _capacityTotal) {
        uploadRes.innerHTML = `❌ <strong>System saturated:</strong> ${_pending}/${CSV_PENDING_CAP} processing + ${_waiting}/${WAITLIST_CAP} waiting. Cannot add ${unique.length} more. Wait for worker.`;
        uploadRes.className = "push-result error";
        return;
      }

      // ── Dedup contra el sistema completo (igual que sellers.json) ────
      // Sin esto, dominios ya en review_queue / historial / sendtrack / blocklist
      // se re-procesaban y gastaban API. El auto-feeder los puede haber metido
      // horas antes — si el MB sube el mismo CSV, se descartan acá.
      uploadRes.textContent = `🔍 Checking duplicates against system...`;
      const { findKnownDomains } = await import("../modules/sellersJson.js");
      const _known = await findKnownDomains(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_ANON_KEY, state.accessToken, unique);
      const _fresh = unique.filter(d => !_known.has(d));
      const _skippedKnown = unique.length - _fresh.length;
      if (_fresh.length === 0) {
        // Log attempt even with 0 inserted — so MB shows up as "tried but all duplicates"
        logImportAttempt(state.accessToken, {
          userEmail: state.loginEmail, source: "csv",
          sourceDetail: file?.name || "", attempted: unique.length, deduped: unique.length, inserted: 0,
        });
        uploadRes.innerHTML = `<span style="color:#0ea5e9">ℹ️ Los ${unique.length} dominios ya están en el sistema (no es un error — el sistema descarta repetidos para no duplicarte trabajo). No hay leads nuevos para agregar de este archivo.</span>`;
        uploadRes.className = "push-result";
        return;
      }

      uploadRes.textContent = `Uploading ${_fresh.length} domains${_skippedKnown > 0 ? ` (${_skippedKnown} already in system, skipped)` : ""}...`;

      const result = await uploadCsvDomains(_fresh, state.loginEmail, state.accessToken, "manual");
      logImportAttempt(state.accessToken, {
        userEmail: state.loginEmail, source: "manual",
        sourceDetail: file?.name || "",
        attempted: unique.length, deduped: _skippedKnown, inserted: result?.inserted || 0,
      });
      { const r = formatUploadResult(result, unique.length); uploadRes.textContent = r.msg; uploadRes.className = "push-result ok"; uploadRes.style.color = r.color; }
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
    // Cap fijo 100 por tirada (user 2026-05-18, antes 75). El input refresh-limit
    // está hidden con value="100" — ya no hay UI para cambiarlo. El shuffle
    // Fisher-Yates del pool 1000 (modules/monday.js) garantiza variedad.
    const limit    = 100;

    btn.disabled = true; btn.textContent = "⏳ Querying Monday...";
    resultEl.textContent = ""; resultEl.className = "push-result";

    try {
      const _filterDetail = [geo || "all", idioma || "all"].join("/");
      const domains = await fetchMondayForRefresh({ geo, idioma, limit });
      if (domains.length === 0) {
        logImportAttempt(state.accessToken, {
          userEmail: state.loginEmail, source: "monday",
          sourceDetail: _filterDetail, attempted: 0, deduped: 0, inserted: 0,
        });
        resultEl.textContent = "No Ciclo Finalizado domains match those filters.";
        resultEl.className = "push-result error";
        return;
      }
      // Dedup against system — Maxi 2026-06-18: para Monday refresh solo
      // chequeamos csv_queue activo + blocklist (NO review_queue cerrado,
      // sendtrack, historial). Esos son del ciclo viejo que YA terminó
      // (por eso está en "Ciclo Finalizado" en Monday). Re-prospectar = OK.
      resultEl.textContent = `🔍 Found ${domains.length}, checking duplicates...`;
      const { findKnownDomains } = await import("../modules/sellersJson.js");
      const _known = await findKnownDomains(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_ANON_KEY, state.accessToken, domains, { mode: "monday_refresh" });
      const _fresh = domains.filter(d => !_known.has(d));
      const _skippedKnown = domains.length - _fresh.length;
      if (_fresh.length === 0) {
        logImportAttempt(state.accessToken, {
          userEmail: state.loginEmail, source: "monday",
          sourceDetail: _filterDetail, attempted: domains.length, deduped: domains.length, inserted: 0,
        });
        resultEl.textContent = `ℹ️ Los ${domains.length} dominios ya estaban en el sistema (repetidos). 0 nuevos para agregar — no es un error, es que ya los conocíamos.`;
        resultEl.className = "push-result";
        return;
      }
      resultEl.textContent = `Uploading ${_fresh.length} fresh${_skippedKnown > 0 ? ` (${_skippedKnown} already in system)` : ""}...`;
      const up = await uploadCsvDomains(_fresh, state.loginEmail, state.accessToken, "monday_refresh");
      logImportAttempt(state.accessToken, {
        userEmail: state.loginEmail, source: "monday",
        sourceDetail: _filterDetail, attempted: domains.length, deduped: _skippedKnown, inserted: up?.inserted || 0,
      });
      { const r = formatUploadResult(up, _fresh.length); resultEl.textContent = r.msg; resultEl.style.color = r.color; }
      resultEl.className = "push-result ok";
      await refreshAll();
    } catch (err) {
      resultEl.textContent = `❌ ${err.message}`;
      resultEl.className = "push-result error";
    } finally {
      btn.disabled = false; btn.textContent = "🔄 Fetch & queue from Monday";
    }
  });

  clearProc.addEventListener("click", async () => {
    if (!confirm("Clear already-processed entries (done/error/skipped)?\n\nDoes NOT affect generated prospects or pending queue.")) return;
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
    if (!confirm("⚠️ This deletes EVERYTHING in the import queue including PENDING items.\n\nGenerated prospects in Prospects tab are NOT affected.\n\nContinue?")) return;
    const confirmText = prompt("Type DELETE to confirm:");
    if (confirmText !== "DELETE") { alert("Canceled."); return; }
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
// Caps de la cola (acordados con user 2026-05-11):
// - csv_queue.pending: 200 (cola de procesamiento del worker — throttle fijo del worker)
// - csv_queue.waiting_pool: 800 (buffer en hold; el worker promueve → pending al liberarse)
// - review_queue (Prospects): SIN CAP (más leads = más variedad para los MBs)
// Maxi 2026-06-19: capacidad total 500 → 1000 (buffer 300→800). El pending sigue en 200
// porque lo throttlea el worker; agrandar el buffer deja entrar más URLs de los json nuevos.
const SELLERS_QUEUE_CAP_PER_RUN = 100;
const SELLERS_OPEN_TABS_CAP     = 30;
const CSV_PENDING_CAP           = 200;
const WAITLIST_CAP              = 800;

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

  // Filtro por continente (Maxi 2026-06-19): clasifica cada fuente por TLD del dominio;
  // las multinacionales (.com/.io/.tv…) caen en "Global". "All" muestra todas.
  const _sellerRegion = (url) => {
    const h = String(url || "").replace(/^https?:\/\//, "").replace(/^www\.|^static\.cdn\./, "").split("/")[0].toLowerCase();
    const ends = (...tlds) => tlds.some(t => h.endsWith(t));
    // Maxi 2026-06-22: Norteamérica / Centroamérica / Sudamérica SEPARADAS (no "LATAM").
    if (ends(".us", ".ca", ".mx", ".com.mx")) return "NA";
    if (ends(".gt", ".cr", ".pa", ".sv", ".hn", ".ni", ".do", ".pr", ".cu", ".bz", ".ht", ".jm", ".tt", ".com.gt", ".com.pa", ".com.do", ".com.sv", ".com.hn", ".com.ni", ".com.pr")) return "CAM";
    if (ends(".br", ".ar", ".cl", ".co", ".pe", ".uy", ".ec", ".ve", ".py", ".bo", ".com.br", ".com.ar", ".com.co", ".com.uy", ".com.bo", ".com.pe", ".com.ve", ".com.ec")) return "SA";
    if (ends(".jp", ".kr", ".cn", ".vn", ".tw", ".hk", ".sg", ".in", ".id", ".my", ".th", ".ph", ".co.jp")) return "APAC";
    if (ends(".au", ".nz", ".com.au")) return "OC";
    if (ends(".tr", ".com.tr", ".ae", ".sa", ".eg", ".il", ".qa", ".kw", ".jo", ".lb", ".com.sa", ".com.eg")) return "MENA";
    if (ends(".za", ".co.za", ".ng", ".com.ng", ".ke", ".co.ke", ".ma", ".dz", ".tn", ".gh", ".com.gh", ".ug", ".co.ug", ".tz", ".co.tz", ".sn", ".ci", ".cm", ".ao", ".mz", ".zm", ".zw", ".co.zw", ".rw", ".et")) return "AFR";
    if (ends(".de", ".fr", ".it", ".es", ".nl", ".be", ".ch", ".at", ".pl", ".cz", ".ro", ".gr", ".se", ".fi", ".dk", ".no", ".pt", ".ie", ".uk", ".co.uk", ".si", ".bg", ".hr", ".sk", ".eu")) return "EU";
    return "GLOBAL";
  };
  const _SELLER_REGIONS = ["ALL", "GLOBAL", "NA", "CAM", "SA", "EU", "APAC", "MENA", "AFR", "OC"];
  const _SELLER_REGION_LABEL = { ALL: "🌍 All", GLOBAL: "🌐 Global", NA: "🌎 Norteamérica", CAM: "🌎 Centroamérica", SA: "🌎 Sudamérica", EU: "🇪🇺 Europe", APAC: "🌏 Asia-Pacific", MENA: "🕌 MENA", AFR: "🌍 Africa", OC: "🦘 Oceania" };
  let _sellersRegionSel = "ALL";
  const renderSelect = async () => {
    const list = await loadList();
    const counts = {};
    list.forEach(c => { const r = _sellerRegion(c.url); counts[r] = (counts[r] || 0) + 1; });
    const filtered = _sellersRegionSel === "ALL" ? list : list.filter(c => _sellerRegion(c.url) === _sellersRegionSel);
    sel.innerHTML = filtered.map(c => `<option value="${list.indexOf(c)}">${c.name}</option>`).join("");
    const cur = filtered[0];
    if (urlEl) urlEl.textContent = cur ? cur.url : "—";
    sel.dataset._list = JSON.stringify(list);
    const chipsEl = document.getElementById("sellers-region-filter");
    if (chipsEl) {
      // Maxi 2026-06-22: mostrar SIEMPRE todos los chips (con (0) si no hay fuentes).
      chipsEl.innerHTML = _SELLER_REGIONS.map(r => {
        const on = _sellersRegionSel === r;
        return `<button type="button" data-region="${r}" style="font-size:10px;padding:2px 7px;border-radius:10px;cursor:pointer;border:1px solid ${on ? "#0ea5e9" : "#334155"};background:${on ? "#0ea5e9" : "#1e293b"};color:${on ? "#fff" : "#cbd5e1"};font-weight:600">${_SELLER_REGION_LABEL[r]}${r !== "ALL" ? ` (${counts[r] || 0})` : ""}</button>`;
      }).join("");
    }
  };
  await renderSelect();
  document.getElementById("sellers-region-filter")?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-region]");
    if (!btn) return;
    _sellersRegionSel = btn.dataset.region;
    renderSelect();
  });

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
    if (!confirm("Restore default list? Edits will be lost.")) return;
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
    if (parsed.length === 0) { modalStat.style.color = "#dc2626"; modalStat.textContent = "❌ No valid entries."; return; }
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

    // Pre-check: waitlist (csv_queue pending + waiting_pool) capacity
    const stats = await import("../modules/supabase.js").then(m => m.getCsvQueueStats(state.accessToken));
    const _csvPending = stats?.pending || 0;
    const _csvWaiting = stats?.waiting_pool || 0;
    const _capacityTotal = CSV_PENDING_CAP + WAITLIST_CAP; // 1000
    const space = _capacityTotal - _csvPending - _csvWaiting;
    if (space <= 0) {
      resEl.innerHTML = `<span style="color:#dc2626">❌ System saturated: ${_csvPending}/${CSV_PENDING_CAP} processing + ${_csvWaiting}/${WAITLIST_CAP} waiting. Wait for worker before adding more.</span>`;
      return;
    }
    const allowed = Math.min(cap, space);

    fetchBtn.disabled = true;
    resEl.innerHTML = `⏳ Fetching ${company.url}...`;
    try {
      const _sellerName = company.name || (company.url || "").split("/")[2] || "";
      const domains = await fetchSellersJson(company.url);
      if (domains.length === 0) {
        logImportAttempt(state.accessToken, {
          userEmail: state.loginEmail, source: "sellers_json",
          sourceDetail: _sellerName, attempted: 0, deduped: 0, inserted: 0,
        });
        resEl.innerHTML = `<span style="color:#d97706">⚠️ No se encontraron PUBLISHER en sellers.json.</span>`;
        return;
      }
      resEl.innerHTML = `🔍 Found ${domains.length}. Chequeando duplicados contra sistema...`;
      const known = await findKnownDomains(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_ANON_KEY, state.accessToken, domains);
      const fresh = domains.filter(d => !known.has(d));
      const knownCount = domains.length - fresh.length;
      if (fresh.length === 0) {
        logImportAttempt(state.accessToken, {
          userEmail: state.loginEmail, source: "sellers_json",
          sourceDetail: _sellerName, attempted: domains.length, deduped: domains.length, inserted: 0,
        });
        resEl.innerHTML = `<span style="color:#d97706">⚠️ ${domains.length} dominios — TODOS ya están en la cola activa o en Prospects. Nada nuevo para encolar.</span>`;
        return;
      }
      // Maxi 2026-06-19: tomar AL AZAR de todo el json (de la primera a la última),
      // no siempre las primeras N. Así, en sucesivas corridas se recorre el json
      // entero. Las que no entran NO se marcan como usadas (no se insertan en ningún
      // lado que el comparador lea) → vuelven a estar disponibles la próxima vez.
      const shuffledFresh = fresh.slice().sort(() => Math.random() - 0.5);
      const slice   = shuffledFresh.slice(0, allowed);
      const skipped = fresh.length - slice.length;
      const { uploadCsvDomains } = await import("../modules/supabase.js");
      const result = await uploadCsvDomains(slice, state.loginEmail, state.accessToken, "sellers_json");
      const ins = result?.inserted || 0;
      const dup = slice.length - ins;
      logImportAttempt(state.accessToken, {
        userEmail: state.loginEmail, source: "sellers_json",
        sourceDetail: _sellerName,
        attempted: domains.length, deduped: knownCount, inserted: ins,
      });
      const r = formatUploadResult(result, slice.length);
      resEl.innerHTML = `<span style="color:${r.color}">${r.msg}</span>`;
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
      if (!confirm(`Open ${N} tabs in browser?\n\nFound: ${domains.length} | Fresh: ${fresh.length} | Cap: ${SELLERS_OPEN_TABS_CAP}/click`)) {
        return;
      }
      // Aleatorio: recorre todo el json en sucesivas aperturas (no siempre los primeros).
      const slice = fresh.slice().sort(() => Math.random() - 0.5).slice(0, N);
      resEl.innerHTML = `🪟 Opening ${N} tabs (400ms delay so Chrome doesn't block them)...`;
      slice.forEach((domain, i) => {
        setTimeout(() => chrome.tabs.create({ url: `https://${domain}`, active: false }), i * 400);
      });
      const left = fresh.length - N;
      resEl.innerHTML = `<span style="color:#16a34a">✅ ${N} tabs opened${left > 0 ? `. ${left} fresh ones not opened (cap ${SELLERS_OPEN_TABS_CAP}/click).` : "."}</span>`;
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
const AUTOPILOT_DURATION_MS = 20 * 60 * 1000; // 20 min max de sesión autopilot — alineado con worker SESSION_LIMIT_MS
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
  // Bonus por caracteres únicos — Maxi 2026-07-08: gate igual que el worker. Los acentos
  // compartidos (á/é/í/ó/ú los usan checo/polaco/húngaro/turco) solo suman si el idioma YA
  // tiene una stopword; solo ñ/¿/¡/ã/õ (realmente distintivos) suman solos.
  if (/[ñ¿¡]/.test(text)) scores.es += 5;
  else if (scores.es > 0 && /[áéíóúü]/.test(text)) scores.es += 5;
  if (/[ãõ]/.test(text)) scores.pt += 5;
  else if (scores.pt > 0 && /[çàáâ]/.test(text)) scores.pt += 5;
  if (scores.it > 0 && /[àèéìòù]/.test(text)) scores.it += 5;

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
/* renderDraftChips removida (B2) — código muerto (0 call-sites) */

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
      listEl.innerHTML = '<div style="color:var(--text-muted);font-style:italic;padding:8px">No drafts. Create the first one below.</div>';
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
    saveBtn.disabled = false; saveBtn.textContent = "💾 Save";

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
    if (!confirm("Delete this draft?")) return;
    await deletePitchDraft(state.accessToken, editingId);
    clearForm();
    await load();
  });
}

// Banner global rojo cuando un flag está ON pero Railway no late hace > 5 min.
// NO auto-apaga el toggle (evita loops). Solo avisa.
/* renderRailwayDeadBanner / pollRailwayDeadBanner removidas (B1) — código muerto
   (pollRailwayDeadBanner nunca se llamaba). renderRailwayHeartbeat SÍ se usa, se conserva. */

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
  // active session and it hasn't exceeded its 20-min window. Force OFF only
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

      // Gate horario operativo (solo al prender)
      if (!(await _checkManualOverrideIfOutside())) return;

      // Mutex: otro user lo tiene activo
      const cur = await getAutopilotState(state.accessToken);
      if (cur.enabled && cur.sessionUser
          && cur.sessionUser.toLowerCase() !== (state.loginEmail || "").toLowerCase()
          && cur.heartbeatAt && (Date.now() - cur.heartbeatAt.getTime()) < 120_000) {
        const elapsed = cur.sessionStart ? Math.round((Date.now() - cur.sessionStart.getTime()) / 60000) : 0;
        const remaining = Math.max(0, 20 - elapsed);
        alert(`🔒 ${cur.sessionUser} already has Autopilot running (started ${elapsed} min ago · ends in ~${remaining} min).\n\nCannot run 2 autopilots at once. Wait for it to finish or ask them to turn it off.`);
        return;
      }
      // Turn on
      setAutopilotUI(btn, true);
      setAutopilotLockBadge(null);
      const res = await setAutopilotEnabled(true, state.accessToken, state.loginEmail);
      if (res && res.ok === false) {
        // Race perdida — otro MB clavó la sesión <400ms antes
        alert(`🔒 ${res.winner.split("@")[0]} just started Autopilot first (race condition). Try again in 20 min when their session ends.`);
        setAutopilotUI(btn, false);
        return;
      }
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
  // Perf fix: guard module-level para no acumular intervals si initAutopilot
  // se llama varias veces (caso real: re-entrar a Prospects tab).
  if (window._autopilotLiveTimer) clearInterval(window._autopilotLiveTimer);
  // Maxi 2026-07-03 perf: el fetch de live-stats (toolbar_config) cada 10s corría SIEMPRE
  // aunque el panel estuviera oculto o el user en otra solapa. Guardamos por visibilidad +
  // tab Prospects activa (mismo patrón que el heartbeat de arriba). Al volver, refresca ≤10s.
  window._autopilotLiveTimer = setInterval(() => {
    if (document.visibilityState === "hidden") return;
    if (!document.getElementById("tab-prospects")?.classList.contains("active")) return;
    refreshAutopilotLiveStats();
  }, 10_000);

  // Refresco periódico del estado mutex (cada 30s) para que cuando el dueño
  // apague desde su browser, los demás vean el lock liberado sin reload manual.
  if (window._autopilotMutexTimer) clearInterval(window._autopilotMutexTimer);
  window._autopilotMutexTimer = setInterval(async () => {
    // Maxi 2026-07-03 perf: no pollear getAutopilotState cuando el panel está oculto o el
    // user no está en Prospects (el botón que se actualiza vive ahí). Al volver refresca ≤30s.
    if (document.visibilityState === "hidden") return;
    if (!document.getElementById("tab-prospects")?.classList.contains("active")) return;
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

// ── GEO filter dropdown: dinámico desde los leads disponibles ─────────────
// ISO 2-letter → emoji flag (regional indicator chars).
function _isoToFlag(iso) {
  if (!iso || iso.length !== 2) return "🌍";
  try {
    return String.fromCodePoint(...[...iso.toUpperCase()].map(c => 0x1F1E6 + c.charCodeAt(0) - 65));
  } catch { return "🌍"; }
}
// Nombres de país comunes. Para los no listados mostramos el ISO solo.
const _GEO_NAMES = {
  AR:"Argentina", BR:"Brasil", ES:"España", MX:"México", CO:"Colombia", CL:"Chile",
  PE:"Perú", UY:"Uruguay", EC:"Ecuador", VE:"Venezuela", DO:"Rep. Dominicana",
  PA:"Panamá", BO:"Bolivia", GT:"Guatemala", CR:"Costa Rica", HN:"Honduras",
  SV:"El Salvador", NI:"Nicaragua", PY:"Paraguay", PR:"Puerto Rico", CU:"Cuba",
  PT:"Portugal", IT:"Italia", FR:"Francia", DE:"Alemania", US:"USA", GB:"Reino Unido",
  CA:"Canadá", NL:"Países Bajos", BE:"Bélgica", CH:"Suiza", AT:"Austria",
  IE:"Irlanda", DK:"Dinamarca", SE:"Suecia", NO:"Noruega", FI:"Finlandia",
  PL:"Polonia", CZ:"Chequia", HU:"Hungría", RO:"Rumania", GR:"Grecia",
  IN:"India", PK:"Pakistán", BD:"Bangladesh", LK:"Sri Lanka", ID:"Indonesia",
  PH:"Filipinas", VN:"Vietnam", TH:"Tailandia", MY:"Malasia", SG:"Singapur",
  SA:"Arabia Saudita", AE:"Emiratos", EG:"Egipto", TR:"Turquía", IL:"Israel",
  NG:"Nigeria", KE:"Kenia", ZA:"Sudáfrica", MA:"Marruecos", DZ:"Argelia",
  JP:"Japón", KR:"Corea", AU:"Australia", NZ:"Nueva Zelanda", CN:"China", TW:"Taiwán",
  RU:"Rusia", UA:"Ucrania", BG:"Bulgaria", HR:"Croacia", SK:"Eslovaquia",
};
// Reconstruye chips de GEO clickeables a partir de los datos disponibles.
// Toolkit user 2026-06-17: chips multi-select para filtrar por varios países a la vez.
// Maxi 2026-06-18 v3: chips de CONTINENTE (preview) + dropdown multi-país.
// Antes mostraba 1 chip por país → con 60+ países era ilegible.
// Ahora 8 chips: EU/AS/NA/SA/CA/AF/OC + "Sin GEO". Para detalle por país,
// botón "🌐 Filtrar países" abre panel con checkboxes de los disponibles.
const ISO_TO_CONTINENT = {
  // North America
  US:"NA", CA:"NA", MX:"NA",
  // Central America + Caribbean
  GT:"CA", HN:"CA", SV:"CA", NI:"CA", CR:"CA", PA:"CA", BZ:"CA",
  CU:"CA", DO:"CA", PR:"CA", JM:"CA", HT:"CA", BS:"CA", TT:"CA", BB:"CA",
  // South America
  AR:"SA", BR:"SA", CL:"SA", CO:"SA", PE:"SA", UY:"SA", VE:"SA",
  EC:"SA", BO:"SA", PY:"SA", GY:"SA", SR:"SA",
  // Europe
  GB:"EU", IE:"EU", FR:"EU", DE:"EU", ES:"EU", PT:"EU", IT:"EU", NL:"EU",
  BE:"EU", CH:"EU", AT:"EU", DK:"EU", SE:"EU", NO:"EU", FI:"EU", PL:"EU",
  CZ:"EU", SK:"EU", HU:"EU", RO:"EU", BG:"EU", GR:"EU", HR:"EU", SI:"EU",
  EE:"EU", LV:"EU", LT:"EU", LU:"EU", MT:"EU", CY:"EU", IS:"EU", UA:"EU",
  BY:"EU", MD:"EU", RS:"EU", AL:"EU", MK:"EU", BA:"EU", ME:"EU", XK:"EU", RU:"EU",
  // Asia
  CN:"AS", JP:"AS", KR:"AS", IN:"AS", PK:"AS", BD:"AS", LK:"AS", ID:"AS",
  PH:"AS", VN:"AS", TH:"AS", MY:"AS", SG:"AS", HK:"AS", TW:"AS", MM:"AS",
  KH:"AS", LA:"AS", BN:"AS", NP:"AS", KZ:"AS", UZ:"AS", AZ:"AS", GE:"AS",
  AM:"AS", AF:"AS", IR:"AS", IQ:"AS", SY:"AS", JO:"AS", LB:"AS", IL:"AS",
  SA:"AS", AE:"AS", QA:"AS", BH:"AS", OM:"AS", KW:"AS", YE:"AS", TR:"AS", PS:"AS",
  // Africa
  EG:"AFR", MA:"AFR", DZ:"AFR", TN:"AFR", LY:"AFR", NG:"AFR", KE:"AFR",
  ZA:"AFR", ET:"AFR", GH:"AFR", UG:"AFR", TZ:"AFR", SN:"AFR", CI:"AFR",
  CM:"AFR", AO:"AFR", MZ:"AFR", ZM:"AFR", ZW:"AFR", BW:"AFR",
  MG:"AFR", RW:"AFR", SD:"AFR", SS:"AFR",
  // Oceania
  AU:"OC", NZ:"OC", FJ:"OC", PG:"OC", SB:"OC",
};
const CONTINENT_INFO = {
  EU:  { name: "Europe",          flag: "🇪🇺" },
  AS:  { name: "Asia",            flag: "🌏" },
  NA:  { name: "North America",   flag: "🌎" },
  SA:  { name: "South America",   flag: "🌎" },
  CA:  { name: "Central America", flag: "🌎" },
  AFR: { name: "Africa",          flag: "🌍" },
  OC:  { name: "Oceania",         flag: "🌏" },
};
// Maxi 2026-06-30: mapas de geo a SCOPE DE MÓDULO (antes vivían dentro de
// _rebuildGeoChips). Así el CONTEO de chips y el FILTRO usan EXACTAMENTE la misma
// lógica → el número del chip coincide siempre con lo que filtra (fix A4: antes el
// filtro usaba un mapa reducido distinto y count ≠ filtrado).
const ISO_TO_NAME_FULL = {
  AR:"Argentina", BR:"Brazil", ES:"Spain", MX:"Mexico", CO:"Colombia",
  CL:"Chile", PE:"Peru", UY:"Uruguay", EC:"Ecuador", VE:"Venezuela",
  DO:"Dominican Republic", PA:"Panama", BO:"Bolivia", GT:"Guatemala",
  CR:"Costa Rica", HN:"Honduras", SV:"El Salvador", NI:"Nicaragua",
  PY:"Paraguay", PR:"Puerto Rico", CU:"Cuba", US:"United States",
  GB:"United Kingdom", CA:"Canada", AU:"Australia", NZ:"New Zealand",
  PT:"Portugal", IT:"Italy", FR:"France", DE:"Germany", NL:"Netherlands",
  BE:"Belgium", CH:"Switzerland", AT:"Austria", IE:"Ireland", DK:"Denmark",
  SE:"Sweden", NO:"Norway", FI:"Finland", PL:"Poland", CZ:"Czech Republic",
  HU:"Hungary", RO:"Romania", GR:"Greece", IN:"India", PK:"Pakistan",
  BD:"Bangladesh", LK:"Sri Lanka", ID:"Indonesia", PH:"Philippines",
  VN:"Vietnam", TH:"Thailand", MY:"Malaysia", SG:"Singapore",
  SA:"Saudi Arabia", AE:"UAE", EG:"Egypt", TR:"Turkey", IL:"Israel",
  NG:"Nigeria", KE:"Kenya", ZA:"South Africa", MA:"Morocco", DZ:"Algeria",
  JP:"Japan", KR:"South Korea", CN:"China", TW:"Taiwan", HK:"Hong Kong",
  RU:"Russia", UA:"Ukraine", BG:"Bulgaria", HR:"Croatia", SK:"Slovakia",
};
const NAME_TO_ISO_FULL = {};
Object.entries(ISO_TO_NAME_FULL).forEach(([iso, name]) => { NAME_TO_ISO_FULL[name.toUpperCase()] = iso; });
// Además, los nombres en español del mapa global _GEO_NAMES (Brasil/España/...).
try { Object.entries(_GEO_NAMES || {}).forEach(([iso, name]) => { NAME_TO_ISO_FULL[String(name).toUpperCase()] = iso; }); } catch {}

// Normaliza un valor de geo (NAME o ISO) → ISO de 2 letras, o "" si no resuelve.
// Maxi 2026-07-01: el fallback slice(0,2) SOLO se aplica cuando el valor parece un
// código ISO (≤3 chars), NUNCA sobre nombres largos: antes "Indonesia".slice(0,2)="IN"
// (India) y "United States"→"UN" mapeaban mal el país mostrado por SimilarWeb.
function _geoToISO(raw) {
  const g = String(raw || "").trim().toUpperCase();
  if (!g) return "";
  if (g.length === 2 && ISO_TO_CONTINENT[g]) return g;
  if (NAME_TO_ISO_FULL[g])                    return NAME_TO_ISO_FULL[g];
  if (g.length <= 3 && ISO_TO_CONTINENT[g.slice(0, 2)]) return g.slice(0, 2);
  return "";
}
// Maxi 2026-07-01 (PUNTO 1): el filtro/conteo de GEO debe respetar EL PAÍS QUE MUESTRA
// LA CARD (el de SimilarWeb = r.geo, "país principal"), NO cualquier geo secundario de
// geos_all. Bug reportado: al elegir "North America" salían Ecuador/Colombia/Chile/India
// porque _rowISOs devolvía TODOS los geos_all → un lead de Ecuador con US en geos_all
// matcheaba NA. Ahora devolvemos SOLO el país primario (r.geo, o geos_all[0] de fallback),
// que es exactamente el que la card renderiza como "🌎 país principal".
function _rowISOs(r) {
  const out = new Set();
  const primary = _geoToISO(r && r.geo)
    || (Array.isArray(r && r.geos_all) && r.geos_all.length ? _geoToISO(r.geos_all[0]) : "");
  if (primary) out.add(primary);
  return out;
}
// Maxi 2026-07-01: ¿el lead tiene al menos un email válido? Usado para (a) ordenar
// los prospects CON email primero en las páginas, y (b) el filtro Type "No Email".
function _hasEmailRow(r) {
  return Array.isArray(r && r.emails) && r.emails.some(e => e && /@/.test(e));
}
// Continentes de un lead (deriva de _rowISOs vía ISO_TO_CONTINENT).
function _rowContinents(r) {
  const conts = new Set();
  for (const iso of _rowISOs(r)) { const c = ISO_TO_CONTINENT[iso]; if (c) conts.add(c); }
  return conts;
}

function _rebuildGeoChips(rows, selected) {
  const wrap = document.getElementById("prospects-geo-chips");
  if (!wrap) return;
  const sel = selected instanceof Set ? selected : new Set();
  const ISO_TO_NAME = ISO_TO_NAME_FULL;
  const _isoOfRow = _rowISOs;
  // Contar por CONTINENTE + por país (para panel multi-select).
  const continentCounts = new Map();
  const countryCounts   = new Map();
  let noGeoCount = 0;
  const seenContByLead  = new Set(); // reset per lead
  for (const r of rows || []) {
    const isos = _isoOfRow(r);
    if (isos.size === 0) { noGeoCount++; continue; }
    const conts = new Set();
    for (const iso of isos) {
      countryCounts.set(iso, (countryCounts.get(iso) || 0) + 1);
      const cont = ISO_TO_CONTINENT[iso];
      if (cont) conts.add(cont);
    }
    for (const c of conts) continentCounts.set(c, (continentCounts.get(c) || 0) + 1);
  }
  // Render chips de CONTINENTE — preview SIEMPRE visible (los 7 continentes, en inglés),
  // tengan o no leads (user 2026-06-19: "los continentes son un preview a la vista").
  // El count aparece entre paréntesis solo si hay leads de ese continente.
  const CONTINENT_ORDER = ["NA", "CA", "SA", "EU", "AS", "AFR", "OC"];
  const allActive = sel.size === 0;
  let html = `<button class="geo-chip" data-code="_ALL_" type="button" style="font-size:10px;padding:2px 7px;border-radius:10px;cursor:pointer;border:1px solid ${allActive ? "#0ea5e9" : "#334155"};background:${allActive ? "#0ea5e9" : "#1e293b"};color:${allActive ? "#fff" : "#cbd5e1"};font-weight:600">🌍 All</button>`;
  for (const cont of CONTINENT_ORDER) {
    const code = `_C_${cont}`;
    const isOn = sel.has(code);
    const info = CONTINENT_INFO[cont] || { name: cont, flag: "🌐" };
    const n = continentCounts.get(cont) || 0;
    html += `<button class="geo-chip" data-code="${code}" type="button" title="${esc(info.name)}" style="font-size:10px;padding:2px 7px;border-radius:10px;cursor:pointer;border:1px solid ${isOn ? "#10b981" : "#334155"};background:${isOn ? "#10b981" : "#1e293b"};color:${isOn ? "#fff" : "#cbd5e1"};font-weight:${isOn ? 600 : 400}">${info.flag} ${info.name}${n ? ` (${n})` : ""}</button>`;
  }
  if (noGeoCount > 0) {
    const isOn = sel.has("_NONE_");
    html += `<button class="geo-chip" data-code="_NONE_" type="button" title="Leads sin país detectado" style="font-size:10px;padding:2px 7px;border-radius:10px;cursor:pointer;border:1px solid ${isOn ? "#10b981" : "#334155"};background:${isOn ? "#10b981" : "#1e293b"};color:${isOn ? "#fff" : "#cbd5e1"};font-weight:${isOn ? 600 : 400}">❓ Sin GEO (${noGeoCount})</button>`;
  }
  // Mostrar chips de país individuales SOLO si hay alguno seleccionado
  // (para indicar visualmente cuáles están filtrando desde el panel).
  const indivSelected = [...sel].filter(c => c.length === 2 && c !== "_C_" && !c.startsWith("_"));
  for (const iso of indivSelected) {
    const name = ISO_TO_NAME[iso] || iso;
    const flag = _isoToFlag(iso);
    html += `<button class="geo-chip" data-code="${iso}" type="button" title="${esc(name)} (click para quitar)" style="font-size:10px;padding:2px 7px;border-radius:10px;cursor:pointer;border:1px solid #10b981;background:#10b981;color:#fff;font-weight:600">${flag} ${iso} ×</button>`;
  }
  wrap.innerHTML = html;
  // Guardar contador de países para el panel
  window._prospectsCountryCounts = countryCounts;
}

// Panel multi-select de países — se abre con "🌐 Filtrar países"
function _renderCountryPanel(filter = "") {
  const list = document.getElementById("prospects-country-list");
  if (!list) return;
  const counts = window._prospectsCountryCounts || new Map();
  const sel = window._selectedGeoChips instanceof Set ? window._selectedGeoChips : new Set();
  const ISO_TO_NAME_LOCAL = {
    AR:"Argentina", BR:"Brazil", ES:"Spain", MX:"Mexico", CO:"Colombia",
    CL:"Chile", PE:"Peru", UY:"Uruguay", EC:"Ecuador", VE:"Venezuela",
    DO:"Dominican Rep", PA:"Panama", BO:"Bolivia", GT:"Guatemala",
    CR:"Costa Rica", HN:"Honduras", SV:"El Salvador", NI:"Nicaragua",
    PY:"Paraguay", PR:"Puerto Rico", CU:"Cuba", US:"United States",
    GB:"United Kingdom", CA:"Canada", AU:"Australia", NZ:"New Zealand",
    PT:"Portugal", IT:"Italy", FR:"France", DE:"Germany", NL:"Netherlands",
    BE:"Belgium", CH:"Switzerland", AT:"Austria", IE:"Ireland", DK:"Denmark",
    SE:"Sweden", NO:"Norway", FI:"Finland", PL:"Poland", CZ:"Czech Rep",
    HU:"Hungary", RO:"Romania", GR:"Greece", IN:"India", PK:"Pakistan",
    BD:"Bangladesh", LK:"Sri Lanka", ID:"Indonesia", PH:"Philippines",
    VN:"Vietnam", TH:"Thailand", MY:"Malaysia", SG:"Singapore",
    SA:"Saudi Arabia", AE:"UAE", EG:"Egypt", TR:"Turkey", IL:"Israel",
    NG:"Nigeria", KE:"Kenya", ZA:"South Africa", MA:"Morocco", DZ:"Algeria",
    JP:"Japan", KR:"South Korea", CN:"China", TW:"Taiwan", HK:"Hong Kong",
    RU:"Russia", UA:"Ukraine", BG:"Bulgaria", HR:"Croatia", SK:"Slovakia",
  };
  // Maxi 2026-06-22: mostrar la MAYORÍA de países SIEMPRE (con (0) si no hay leads),
  // no solo los que tienen prospects. Y si hay un CONTINENTE seleccionado, mostrar
  // SOLO los países de ese continente. Orden alfabético.
  const _selConts = new Set([...sel].filter(c => c.startsWith("_C_")).map(c => c.slice(3)));
  const sorted = Object.keys(ISO_TO_NAME_LOCAL)
    .filter(iso => {
      if (_selConts.size && !_selConts.has(ISO_TO_CONTINENT[iso] || "")) return false;
      if (filter) {
        const f = filter.toLowerCase();
        const name = (ISO_TO_NAME_LOCAL[iso] || iso).toLowerCase();
        if (!(iso.toLowerCase().includes(f) || name.includes(f))) return false;
      }
      return true;
    })
    .map(iso => [iso, counts.get(iso) || 0])
    .sort((a, b) => (ISO_TO_NAME_LOCAL[a[0]] || a[0]).localeCompare(ISO_TO_NAME_LOCAL[b[0]] || b[0]));

  // Maxi 2026-06-19: fila de CONTINENTES (en inglés) SIEMPRE visible — además de
  // los países. North/Central/South America separados + Europe/Asia/Africa/Oceania.
  // Usan los mismos códigos `_C_XX` que ya entiende el filtro multi-GEO, y la misma
  // clase .country-pick (el handler togglea data-code en _selectedGeoChips).
  const CONTINENT_ORDER = ["NA", "CA", "SA", "EU", "AS", "AFR", "OC"];
  let contHtml = "";
  for (const cont of CONTINENT_ORDER) {
    const code = `_C_${cont}`;
    const info = CONTINENT_INFO[cont] || { name: cont, flag: "🌐" };
    const isOn = sel.has(code);
    const cn = [...counts.entries()].reduce((acc, [iso, n]) => acc + ((ISO_TO_CONTINENT[iso] === cont) ? n : 0), 0);
    contHtml += `<button class="country-pick" data-code="${code}" type="button" title="${esc(info.name)}" style="font-size:10px;padding:3px 8px;border-radius:10px;cursor:pointer;border:1px solid ${isOn ? "#10b981" : "#0ea5e9"};background:${isOn ? "#10b981" : "#0b2a3d"};color:${isOn ? "#fff" : "#7dd3fc"};font-weight:600">${info.flag} ${esc(info.name)}${cn ? ` (${cn})` : ""}</button>`;
  }
  const contBlock = `<div style="font-size:9px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.4px;margin:0 0 4px">🌐 Continents</div><div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px;padding-bottom:8px;border-bottom:1px solid #334155">${contHtml}</div>`;

  let html = "";
  for (const [iso, n] of sorted) {
    const isOn = sel.has(iso);
    const name = ISO_TO_NAME_LOCAL[iso] || iso;
    const flag = _isoToFlag(iso);
    html += `<button class="country-pick" data-code="${iso}" type="button" title="${esc(name)}" style="font-size:10px;padding:3px 8px;border-radius:10px;cursor:pointer;border:1px solid ${isOn ? "#10b981" : "#334155"};background:${isOn ? "#10b981" : "#1e293b"};color:${isOn ? "#fff" : "#cbd5e1"};font-weight:${isOn ? 600 : 400}">${flag} ${esc(name)} (${n})</button>`;
  }
  const countriesBlock = html
    ? `<div style="font-size:9px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.4px;margin:0 0 4px">📍 Countries</div><div style="display:flex;flex-wrap:wrap;gap:4px">${html}</div>`
    : `<div style="font-size:11px;color:var(--text-muted);padding:6px">Sin paises ${filter ? "que coincidan" : "disponibles (todavía no hay leads cargados)"}.</div>`;
  // Los CONTINENTES ahora son preview a la vista (en _rebuildGeoChips); el panel
  // desplegable muestra SOLO países (user 2026-06-19).
  list.innerHTML = countriesBlock;
}

/* _rebuildGeoFilterFromRows removida (C1/B2) — código muerto (0 call-sites; los GEO chips
   la reemplazaron con _rebuildGeoChips). */

async function loadProspectsTab(opts = {}) {
  // opts.keepPage: mantener la página actual (lo usa el auto-refresh de 15 min para
  // NO sacar al MB de su lugar). Filtros y ↻ manual arrancan en página 1.
  const listEl  = document.getElementById("prospects-list");
  const statsEl = document.getElementById("prospects-stats");
  if (!listEl) return;
  const _keepPage = opts.keepPage ? (window._prospectsPage || 1) : 1;
  if (!opts.keepPage) listEl.innerHTML = '<div class="cascade-empty">⏳ Loading...</div>';

  const dateFilter    = document.getElementById("prospects-date-filter")?.value    || "";
  const sourceFilter  = document.getElementById("prospects-source-filter")?.value  || "";
  const userFilter    = document.getElementById("prospects-user-filter")?.value    || "";
  const geoFilter     = document.getElementById("prospects-geo-filter")?.value     || "";
  const trafficFilter = document.getElementById("prospects-traffic-filter")?.value || "";
  const nameFilterRaw = document.getElementById("prospects-name-filter")?.value    || "";
  const nameFilter    = nameFilterRaw.trim().toLowerCase();
  // Multi-GEO chips — si hay chips seleccionados, prevalecen sobre el dropdown legacy
  const geoChipsSet = window._selectedGeoChips instanceof Set ? window._selectedGeoChips : new Set();
  // El server-side geoFilter SOLO entiende un país real (ISO 2 letras). Un continente
  // (`_C_EU`) o "Sin GEO" (`_NONE_`) NO existe como valor de `geo` en la tabla → si se
  // mandaba al server (Maxi 2026-06-30 bug) devolvía 0 rows / no filtraba. Ahora esos
  // casos se traen completos y los resuelve el filtro client-side de abajo.
  const _singleChip = geoChipsSet.size === 1 ? [...geoChipsSet][0] : "";
  const _singleIsCountry = _singleChip.length === 2 && !_singleChip.startsWith("_");
  const effectiveGeoForServer = _singleIsCountry ? _singleChip : (geoChipsSet.size === 0 ? geoFilter : "");
  let rows = [];
  let allRowsForChips = [];     // sin filtros client-side, para repoblar chips
  let dailyCount = 0;
  let snoozedSet = new Set();
  try {
    const _gpcMod = await import("../modules/supabase.js");
    const [_rows, _count, _drafts, _snoozedRes, _globalPool] = await Promise.all([
      fetchReviewQueue(state.accessToken, { dateFilter, sourceFilter, userFilter, geoFilter: effectiveGeoForServer }),
      getDailyValidationCount(state.accessToken, state.loginEmail),
      getPitchDrafts(state.accessToken, state.loginEmail),
      // Snoozes activos del MB actual (snooze_until > now). Cada MB tiene su propia bolsa.
      fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_snoozed_prospects?user_email=eq.${encodeURIComponent((state.loginEmail||"").toLowerCase())}&snooze_until=gt.${new Date().toISOString()}&select=domain`,
        { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
      ).then(r => r.ok ? r.json() : []).catch(() => []),
      // Total global del pool (igual para todos los MBs) — Maxi 2026-06-17.
      _gpcMod.getReviewQueuePendingCount(state.accessToken).catch(() => 0),
    ]);
    window._prospectsGlobalPool = _globalPool;
    rows = _rows;
    dailyCount = _count;
    _cachedProspectDrafts = _drafts;
    snoozedSet = new Set((_snoozedRes || []).map(r => (r.domain || "").toLowerCase()));
    // Maxi 2026-06-22: VISTA UNIFICADA — todos los MBs deben ver la MISMA cantidad de
    // prospects (solo cambia el orden). Antes el snooze y el X-learn (firmas rechazadas)
    // se aplicaban POR-MB → Diego/Agus/Maxi veían conteos distintos. Ahora NO se filtran
    // del listado (se mantiene la data por si se reactiva). El pool es el mismo para todos.
    // (snooze + X-learn desactivados del filtro visible — Maxi 2026-06-22)
    allRowsForChips = rows.slice();
    // ── Client-side filters: multi-GEO chips + traffic range + name ──
    // (Estos NO pueden ir en URL PostgREST de forma simple, así que filtramos
    // acá. El server ya nos trajo hasta 3000 rows = bien manejable.)
    // Maxi 2026-06-18: filtro client-side multi-GEO + soporta "_NONE_" (sin geo)
    if (geoChipsSet.size >= 1) {
      const wantsNoGeo = geoChipsSet.has("_NONE_");
      // Maxi 2026-06-30: el filtro usa EXACTAMENTE la misma detección que el conteo de
      // chips (_rowISOs/_rowContinents) → el número del chip coincide con lo filtrado.
      const wantContinents = new Set();
      const wantCountries  = new Set();
      for (const code of geoChipsSet) {
        if (code === "_NONE_") continue;
        if (code.startsWith("_C_")) wantContinents.add(code.slice(3));
        else if (code.length === 2) wantCountries.add(code);
      }
      rows = rows.filter(r => {
        const isos = _rowISOs(r);
        if (wantsNoGeo && isos.size === 0) return true;
        for (const code of wantCountries) if (isos.has(code)) return true;
        if (wantContinents.size) {
          for (const cont of _rowContinents(r)) if (wantContinents.has(cont)) return true;
        }
        return false;
      });
    }
    if (trafficFilter) {
      const [minMStr, maxMStr] = trafficFilter.split("-");
      const minTraffic = parseFloat(minMStr) * 1_000_000;
      const maxTraffic = parseFloat(maxMStr) * 1_000_000;
      rows = rows.filter(r => {
        const t = parseInt(r.traffic || 0, 10);
        return t >= minTraffic && t < maxTraffic;
      });
    }
    if (nameFilter) {
      rows = rows.filter(r => {
        const d = (r.domain || "").toLowerCase();
        const t = (r.page_title || "").toLowerCase();
        return d.includes(nameFilter) || t.includes(nameFilter);
      });
    }
    // Maxi 2026-07-01: filtro TYPE para revisar categorías aparte.
    //   alert   → sospechosas de rechazo (⚠️ suspect_reject)
    //   noemail → sin email (para completarlas/revisarlas)
    const _typeFilter = window._prospectsTypeFilter || "all";
    if (_typeFilter === "alert")        rows = rows.filter(r => !!r.suspect_reject);
    else if (_typeFilter === "noemail") rows = rows.filter(r => !_hasEmailRow(r));
    // Sincronizar la cache global de drafts (la usa la bandera+autocarga de cada card)
    _draftsState.all = _cachedProspectDrafts;
    _rebuildDraftsByLang();
    _draftsState.loaded = true;
  } catch (err) {
    listEl.innerHTML = `<div class="cascade-empty" style="color:#e53e3e">❌ Error loading prospects: ${esc(err.message || String(err))}</div>`;
    return;
  }

  // Repoblar GEO chips desde el universo SIN filtros client-side, así siempre
  // ves los países disponibles.
  _rebuildGeoChips(allRowsForChips, geoChipsSet);

  updateProspectsDailyBar(dailyCount);
  if (statsEl) statsEl.textContent = rows.length ? `${rows.length} pending candidate${rows.length === 1 ? "" : "s"}` : "No pending candidates";
  // Tab counter — vista de pajaro de cuánto hay para revisar.
  const tabCount = document.getElementById("tab-prospects-count");
  if (tabCount) tabCount.textContent = rows.length > 0 ? `(${rows.length})` : "";

  if (!rows.length) {
    // Maxi 2026-07-15 (BUG 2 auditoría filtros): limpiar la barra de paginación al quedar 0 resultados.
    // Antes la nav (sibling de #prospects-list) quedaba con "1000 leads · pág 1/20" del render anterior →
    // "No pending candidates" arriba y "1000 leads" abajo al mismo tiempo.
    const _navEmpty = document.getElementById("prospects-pagination");
    if (_navEmpty) { _navEmpty.innerHTML = ""; _navEmpty.style.display = "none"; }
    window._prospectsSample = [];
    // Empty state inteligente: si hay actividad activa del worker, mostrar qué
    // está procesando; si todo está apagado, mostrar el mensaje genérico.
    listEl.innerHTML = '<div class="cascade-empty">⏳ Verificando actividad del worker...</div>';
    renderProspectsEmptyState(listEl).catch(() => {
      listEl.innerHTML = '<div class="cascade-empty">No pending prospects.</div>';
    });
    return;
  }

  // ── Cap diario por OUTPUT desde PROSPECTS: 30 envios/día per MB ──
  // Solo cuenta validations procesadas desde Prospects (validated_by = MB).
  // Envíos manuales desde Analysis NO cuentan acá — el MB puede seguir
  // mandando desde ahí libremente.
  const DAILY_SEND_CAP = 30;
  const sentFromProspects = dailyCount; // ya viene de getDailyValidationCount arriba
  // Maxi 2026-06-30: ANTES, al llegar a 30, se OCULTABA toda la lista y el MB perdía
  // acceso a Prospects el resto del día (no podía ni revisar/abrir). El envío ya tiene
  // su propio tope en validateProspect (corta a 50), así que el gate de 30 solo molestaba.
  // Ahora la lista SIEMPRE se muestra; el progreso se ve en el stats line de abajo.

  // ── Sample 100 random ROTANDO cada 30 minutos por MB ──
  // Maxi 2026-06-18: SIN cap visible — todos los MBs ven la totalidad de
  // disponibles. El shuffle por slot garantiza orden distinto entre MBs.
  // Antes había VISIBLE_CAP=200, ahora mostramos TODOS los rows tras filtros.
  const SLOT_MIN    = 30;           // rotar cada 30 minutos
  const slotIdx     = Math.floor(Date.now() / (SLOT_MIN * 60 * 1000));
  const userKey     = (state.loginEmail || "anon").toLowerCase();

  // GEO chips ya se reconstruyeron arriba (línea _rebuildGeoChips). Dropdown
  // viejo está hidden — solo lo usamos como storage para compat.
  // Bug fix 2026-05-14: slotKey debe incluir filtros (incluido geo).
  // 2026-06-17: incluir traffic + name + chips para que el slot cache no
  // mezcle resultados entre filtros distintos.
  const geoChipsKey = [...geoChipsSet].sort().join(",");
  const filterHash  = `${dateFilter}|${sourceFilter}|${userFilter}|${geoFilter}|${geoChipsKey}|${trafficFilter}|${nameFilter}`;
  const slotKey     = `_prospects_slot_${userKey}_${filterHash}_${slotIdx}`;

  // user 2026-06-16: dos cambios importantes en esta sección:
  //   A) `mine` (created_by = MB actual) se calcula SIEMPRE fresco — no cae al
  //      caché del slot. Si el MB importa nuevos sitios, aparecen al toque en
  //      el tope, sin esperar 30 min al próximo slot.
  //   B) Para los `other`, GEOs deprioritizados (US/GB/CA/AU/NZ/IE) van al
  //      final. LATAM + Europa continental + resto del mundo arriba.
  const me = (state.loginEmail || "").toLowerCase();
  const DEPRIO_ISO   = new Set(["US","USA","GB","UK","CA","AU","NZ","IE"]);
  const DEPRIO_NAMES = ["united states","usa","united kingdom","uk","britain","england","canada","australia","new zealand","ireland"];
  const _geoKey = (r) => {
    if (Array.isArray(r.geos_all) && r.geos_all.length) return String(r.geos_all[0] || "?").toUpperCase().slice(0,3);
    return String(r.geo || "?").toUpperCase().slice(0,3);
  };
  const _isDeprio = (r) => {
    if (Array.isArray(r.geos_all) && r.geos_all.length) {
      const iso = String(r.geos_all[0] || "").toUpperCase();
      if (DEPRIO_ISO.has(iso)) return true;
    }
    const g = String(r.geo || "").toLowerCase().trim();
    if (!g) return false;
    if (DEPRIO_ISO.has(g.toUpperCase())) return true;
    return DEPRIO_NAMES.some(n => g === n || g.startsWith(n));
  };
  const shuffle = (arr) => {
    for (let i = arr.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
  };

  // Maxi 2026-06-22: orden ESTABLE (por fecha desc, nuevos arriba) en vez de shuffle
  // aleatorio → el listado NO se reordena en cada refresh/click. Antes el shuffle hacía
  // que el orden cambiara solo y se "perdiera" al actualizar.
  const mineAll = rows.filter(r => (r.created_by || "").toLowerCase() === me)
    .sort((a, b) => String(b.created_at || "").localeCompare(String(a.created_at || "")));
  const mineIds = new Set(mineAll.map(r => r.id));

  // Cache solo el sample de los "other" (no-mine) por slot. `mine` se overlay
  // siempre fresco arriba.
  let cachedOtherIds = [];
  try {
    const stored = await chrome.storage.local.get(slotKey);
    if (stored?.[slotKey] && Array.isArray(stored[slotKey])) cachedOtherIds = stored[slotKey];
  } catch {}

  // Validar que los ids cacheados sigan en `rows` y NO sean mine (si el MB
  // tomó ownership de algún row después del slot, sacarlo del bucket "other").
  const rowsById = new Map(rows.map(r => [r.id, r]));
  cachedOtherIds = cachedOtherIds.filter(id => rowsById.has(id) && !mineIds.has(id));

  if (cachedOtherIds.length === 0) {
    const other = rows.filter(r => !mineIds.has(r.id));
    // Separar en prio vs deprio antes de bucketizar.
    const prio   = other.filter(r => !_isDeprio(r));
    const deprio = other.filter(r =>  _isDeprio(r));
    const buildBuckets = (arr) => {
      const m = new Map();
      for (const r of arr) {
        const g = _geoKey(r);
        if (!m.has(g)) m.set(g, []);
        m.get(g).push(r);
      }
      for (const a of m.values()) shuffle(a);
      return m;
    };
    const pickRoundRobin = (buckets, budget) => {
      const out = [];
      const keys = [...buckets.keys()];
      shuffle(keys);
      let safety = 5000;
      while (out.length < budget && safety-- > 0) {
        let progressed = false;
        for (const k of keys) {
          const b = buckets.get(k);
          if (b && b.length) { out.push(b.shift()); progressed = true; }
          if (out.length >= budget) break;
        }
        if (!progressed) break;
      }
      return out;
    };
    // Maxi 2026-06-18: SIN cap — meter TODOS los rows other (no solo VISIBLE_CAP)
    // El round-robin ahora corre con budget = other.length (= todos).
    const totalOther  = other.length;
    const prioPicked  = pickRoundRobin(buildBuckets(prio), totalOther);
    const remaining   = Math.max(0, totalOther - prioPicked.length);
    const deprioPicked = remaining > 0 ? pickRoundRobin(buildBuckets(deprio), remaining) : [];
    const mixed = [...prioPicked, ...deprioPicked];
    cachedOtherIds = mixed.map(r => r.id);
    await chrome.storage.local.set({ [slotKey]: cachedOtherIds }).catch(() => {});
    try {
      const all = await chrome.storage.local.get(null);
      const stale = Object.keys(all).filter(k => k.startsWith(`_prospects_slot_${userKey}_`) && k !== slotKey);
      if (stale.length > 0) await chrome.storage.local.remove(stale);
    } catch {}
  }

  const otherOrdered = cachedOtherIds.map(id => rowsById.get(id)).filter(Boolean);
  // Maxi 2026-06-18: sin slice — todos los disponibles
  // Maxi 2026-07-01: los que TIENEN email van PRIMERO en las páginas (partición estable:
  // conserva el orden random por MB dentro de cada grupo). Así el MB trabaja los accionables
  // arriba y los sin-email quedan al final (o se revisan con el filtro Type "No Email").
  const _sampleRaw = [...mineAll, ...otherOrdered];
  const sample = [..._sampleRaw.filter(_hasEmailRow), ..._sampleRaw.filter(r => !_hasEmailRow(r))];

  // Maxi 2026-06-22: PAGINACIÓN — antes renderizaba TODAS las cards de una y con
  // miles (waitlist llegó a 2000+) la UI se COLGABA. Ahora 50 por página + nav 1·2·3.
  window._prospectsSample = sample;
  if (statsEl) {
    const globalPool = window._prospectsGlobalPool || rows.length;
    statsEl.innerHTML = `<strong>${sample.length}</strong> visibles · <strong>${globalPool}</strong> en pool global · enviaste <strong>${sentFromProspects}/${DAILY_SEND_CAP}</strong> hoy`;
  }
  renderProspectsPage(_keepPage);  // 1 normalmente; la página actual si es auto-refresh
}

const PROSPECTS_PAGE_SIZE = 50;
function renderProspectsPage(page) {
  const listEl = document.getElementById("prospects-list");
  if (!listEl) return;
  const sample = window._prospectsSample || [];
  const pages = Math.max(1, Math.ceil(sample.length / PROSPECTS_PAGE_SIZE));
  window._prospectsPage = Math.min(Math.max(1, page), pages);
  const start = (window._prospectsPage - 1) * PROSPECTS_PAGE_SIZE;
  const slice = sample.slice(start, start + PROSPECTS_PAGE_SIZE);

  // Render solo la página actual (rápido — máx 50 cards)
  let nav = document.getElementById("prospects-pagination");
  listEl.innerHTML = slice.map(r => renderProspectCard(r)).join("");
  listEl.querySelectorAll(".pcard").forEach(card => {
    const id = parseInt(card.dataset.id);
    const data = slice.find(r => r.id === id);
    if (data) initProspectCard(card, data);
  });

  // Barra de páginas (debajo de la lista)
  if (!nav) {
    nav = document.createElement("div");
    nav.id = "prospects-pagination";
    nav.style.cssText = "display:flex;flex-wrap:wrap;gap:4px;justify-content:center;align-items:center;margin:10px 8px";
    listEl.parentNode.insertBefore(nav, listEl.nextSibling);
  }
  if (pages <= 1) { nav.innerHTML = ""; nav.style.display = "none"; return; }
  nav.style.display = "flex";
  const cur = window._prospectsPage;
  const btn = (label, target, disabled, active) =>
    `<button class="prospects-page-btn" data-page="${target}" ${disabled ? "disabled" : ""}
       style="min-width:26px;padding:3px 8px;font-size:11px;border-radius:4px;cursor:${disabled ? "default" : "pointer"};
       border:1px solid ${active ? "#0ea5e9" : "#334155"};background:${active ? "#0ea5e9" : "#1e293b"};
       color:${active ? "#fff" : "#cbd5e1"};font-weight:${active ? 700 : 400};opacity:${disabled ? .4 : 1}">${label}</button>`;
  const from = Math.max(1, cur - 2), to = Math.min(pages, cur + 2);
  let html = btn("‹", cur - 1, cur === 1, false);
  if (from > 1) html += btn("1", 1, false, cur === 1) + (from > 2 ? `<span style="color:#64748b">…</span>` : "");
  for (let p = from; p <= to; p++) html += btn(String(p), p, false, p === cur);
  if (to < pages) html += (to < pages - 1 ? `<span style="color:#64748b">…</span>` : "") + btn(String(pages), pages, false, cur === pages);
  html += btn("›", cur + 1, cur === pages, false);
  html += `<span style="font-size:10px;color:#64748b;margin-left:6px">${sample.length} leads · pág ${cur}/${pages}</span>`;
  nav.innerHTML = html;
  nav.querySelectorAll(".prospects-page-btn").forEach(b => {
    b.addEventListener("click", () => { if (!b.disabled) { renderProspectsPage(parseInt(b.dataset.page, 10)); document.getElementById("prospects-list")?.scrollIntoView({ block: "start", behavior: "smooth" }); } });
  });
}

function updateProspectsDailyBar(count) {
  const countEl = document.getElementById("prospects-daily-count");
  const fillEl  = document.getElementById("prospects-daily-fill");
  if (countEl) countEl.textContent = count;
  if (fillEl)  fillEl.style.width  = Math.min(100, (count / 50) * 100) + "%";
  if (fillEl)  fillEl.style.background = count >= 50 ? "#e53e3e" : count >= 40 ? "#d97706" : "#3b82f6";
}

// Quick score lead — heurística sync para Prospects card cuando r.score=0.
// Mismo mapping conceptual que worker scoreWebsite pero sin gates duros (todos visibles).
function quickScoreLead(r) {
  let s = 0;
  // Traffic (max 30)
  const tr = parseInt(r.traffic || 0, 10);
  if (tr >= 1_000_000)      s += 30;
  else if (tr >= 500_000)   s += 22;
  else if (tr >= 350_000)   s += 18;
  else if (tr >= 100_000)   s += 10;
  else if (tr > 0)          s += 5;
  // GEO bonus (max 20)
  const geo = (r.geo || "").trim();
  const HI_GEOS = new Set(["AR","MX","CO","CL","PE","UY","ES","PT","BR","Argentina","Mexico","Colombia","Chile","Peru","Uruguay","Spain","Portugal","Brazil"]);
  const MID_GEOS = new Set(["IT","DE","FR","Italy","Germany","France","Italia","Alemania","Francia"]);
  if (HI_GEOS.has(geo))       s += 20;
  else if (MID_GEOS.has(geo)) s += 15;
  else if (geo)               s += 5;
  // Has email (max 25)
  const emails = Array.isArray(r.emails) ? r.emails.filter(Boolean) : [];
  if (emails.length >= 1) s += 15;
  if (emails.length >= 2) s += 5;
  if (r.contact_name) s += 5;
  // Ad networks detected (max 10) — más es mejor (más open al outreach)
  const ads = Array.isArray(r.ad_networks) ? r.ad_networks : [];
  if (ads.length === 0) s += 10;       // virgin → ideal
  else if (ads.length <= 2) s += 5;
  // Category bonus (max 10) — news/sports/finance convierten mejor
  const cat = (r.category || "").toLowerCase();
  if (/news|sport|finance|business/.test(cat)) s += 10;
  else if (cat && cat !== "other") s += 5;
  // Language match supported (max 5)
  if (r.language && /^(es|en|pt|it|ar)$/.test(r.language)) s += 5;
  return Math.min(100, s);
}

function renderProspectCard(r) {
  const trafficFmt  = r.traffic ? formatTraffic(r.traffic) : "Sin data";
  // Resolver idioma sincronicamente al render — evita que aparezca "🌐 —"
  // mientras carga el cache de drafts. Cascada robusta: lang→geo(ISO)→geo(label)→
  // category words→title heuristic→TLD→default. Si NADA es claro y el sitio es .com,
  // disparamos detección async vs el sitio (best-effort) que actualiza la card después.
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
    // 4) Heurística sobre title + category — palabras típicas por idioma
    const sample = `${r.page_title || ""} ${r.category || ""} ${r.domain || ""}`.toLowerCase();
    if (sample) {
      // Maxi 2026-07-08: solo chars distintivos + stopwords (ver _detectLangFromText).
      if (/[ñ¿¡]|noticias|últimas|video|fútbol|deport|política|economía|ciudad|provincia|país|noticia/.test(sample)) return "es";
      if (/[ãõ]|notícias|notícia|últimas|últim|esportes|política|economia|cidade|brasileir/.test(sample)) return "pt";
      if (/notizie|ultim|sport|politica|economia|città/.test(sample)) return "it";
      if (/[؀-ۿ]/.test(r.page_title || "")) return "ar";
    }
    // 5) TLD del dominio (.com.ar / .mx / .br / .it / .pt) — fallback obvio
    const tldLang = detectLangFromDomain(r.domain || "");
    if (LANG_FLAG[tldLang]) return tldLang;
    // 6) default
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

  // Maxi 2026-06-17: stars + fire/warm/cold REMOVIDOS — no se usaban para
  // decidir nada y ocupaban espacio visual. La info clave (traffic, GEO,
  // email count, categoria) va ahora directamente en la meta row, más legible.
  // Mantenemos `score` para el ordenamiento interno pero no se renderiza.
  let score = r.score || 0;
  if (!score) score = quickScoreLead(r);
  const scoreBadge = "";
  const tempBadge  = "";
  // Sobre con cantidad de emails — destaca arriba (antes estaba abajo, chiquito).
  const emailCountBadge = hasEmail
    ? `<span title="${emails.length} email(s) encontrados" style="font-size:11px;font-weight:700;color:#fff;background:#0ea5e9;border-radius:4px;padding:1px 6px;flex-shrink:0">✉️ ${emails.length}</span>`
    // Maxi 2026-06-19: sin emails → además del badge rojo, botón verde "SW" para
    // abrir SimilarWeb del dominio (chequear visual si la web vale la pena).
    : `<span title="Sin emails" style="font-size:11px;font-weight:700;color:#fff;background:#dc2626;border-radius:4px;padding:1px 6px;flex-shrink:0">✉️ —</span><a href="https://hypestat.com/info/${esc(r.domain || "")}" target="_blank" rel="noopener" title="Ver tráfico de ${esc(r.domain || "")} (Hypestat — sin límite de sesión)" style="font-size:10px;font-weight:700;color:#fff;background:#10b981;border-radius:4px;padding:1px 6px;flex-shrink:0;text-decoration:none">📊</a>`;

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
            // Source badge — icono + label corto (Auto/CSV/JSON/Monday). Maxi 2026-06-17:
            // pidió que el label de TIPO de búsqueda quede al lado del nombre. Antes era
            // solo icono cuadradito. Ahora icon + categoría.
            const src = r.source || "autopilot";
            const badges = {
              autopilot:      ["🤖", "Auto",     "#6366f1"],
              csv:            ["📥", "CSV",      "#0ea5e9"],
              manual:         ["✋", "Manual",   "#0ea5e9"],
              monday_refresh: ["🔄", "Monday",   "#f59e0b"],
              sellers_json:   ["📋", "JSON",     "#8b5cf6"],
              autogoogle:     ["🔎", "Google",   "#22c55e"],  // Maxi 2026-06-30: faltaba → caía a "Auto"
            };
            const [icon, label, color] = badges[src] || badges.autopilot;
            return `<span title="Tipo de búsqueda: ${label}" style="font-size:10px;font-weight:700;color:#fff;background:${color};border-radius:4px;padding:1px 6px;flex-shrink:0">${icon} ${label}</span>`;
          })()}
          ${(() => {
            // Badge del usuario que generó el item. Mapea email a nombre corto.
            // Maxi 2026-06-17: si created_by está vacío (worker/autopilot sin
            // owner real) → mostrar "Maxi" (no "worker" ni vacío) — convención
            // del equipo: trabajo del worker lo atribuimos a Maxi (admin/owner).
            const email = (r.created_by || "").toLowerCase();
            const userMap = {
              "mgargiulo@adeqmedia.com": ["Maxi",  "#10b981"],
              "dhorovitz@adeqmedia.com": ["Diego", "#a855f7"],
              "sales@adeqmedia.com":     ["Agus",  "#ec4899"],
            };
            // Maxi 2026-06-30: created_by vacío = lo cargó el AGENTE autónomo (no un MB).
            // Antes se atribuía a "Maxi" → ahora se muestra como "🤖 Agent" para distinguir
            // el trabajo automático de las cargas manuales de cada MB.
            let name, color, icon;
            if (!email)                 { name = "Agent"; color = "#0ea5e9"; icon = "🤖"; }
            else if (userMap[email])    { [name, color] = userMap[email]; icon = "👤"; }
            else                        { name = email.split("@")[0]; color = "#64748b"; icon = "👤"; }
            return `<span title="Origen del item: ${esc(email || "agente autónomo (sin owner manual)")}" style="font-size:10px;font-weight:700;color:#fff;background:${color};border-radius:4px;padding:1px 6px;flex-shrink:0">${icon} ${esc(name)}</span>`;
          })()}
          <a class="pcard-domain-link" href="#" data-url="https://www.${esc(r.domain)}"
             style="font-weight:700;font-size:12px;color:var(--primary);text-decoration:none;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.3;flex:1;min-width:0"
             title="${esc(r.domain)}">
            ${esc(r.domain)} ↗
          </a>
          ${emailCountBadge}
        </div>
        ${titleRow}
        <!-- Meta row reordenada Maxi 2026-06-17: GEO + Páginas Vistas + Categoría + Idioma -->
        <div style="font-size:10px;color:var(--text-muted);margin-top:3px;display:flex;flex-wrap:wrap;gap:6px">
          ${r.geo      ? `<span title="País principal">🌎 ${esc(r.geo)}</span>` : ""}
          <span title="Páginas vistas / tráfico mensual">📊 ${trafficFmt}</span>
          ${r.category ? `<span title="Categoría del sitio">📁 ${esc(r.category)}</span>` : ""}
          ${r.language ? `<span title="Idioma">🗣 ${esc(langName)}</span>` : ""}
          ${r.contact_name ? `<span title="Contacto detectado">👤 ${esc(r.contact_name)}</span>` : ""}
          ${adNetRow}
        </div>
      </div>
      <div style="display:flex;gap:3px;flex-shrink:0;align-items:center">
        <button class="btn btn-secondary btn-sm pcard-expand-btn" title="Expandir para revisar datos, email y pitch antes de enviar" style="padding:3px 7px">▼ Revisar</button>
        <!-- Maxi 2026-07-01: alerta "sospechosa de rechazo". El worker (3×/semana) analiza
             los prospects contra los comentarios de descarte y marca suspect_reject=true en
             las que son del MISMO tipo que las rechazadas. Se enciende la ⚠️ al lado de la X. -->
        ${r.suspect_reject
          ? `<span class="pcard-similar-alert" title="⚠️ Sospechosa: parecida a un tipo que descartaste${r.suspect_reason ? ` (${esc(r.suspect_reason)})` : ""}. Revisá si conviene rechazarla." style="color:#f59e0b;font-size:14px;cursor:help;line-height:1">⚠️</span>`
          : ""}
        <button class="btn btn-sm pcard-reject-btn" title="❌ Descartar — no sirve + el agente aprende a evitar tipos similares" style="padding:3px 7px;color:#e53e3e;background:transparent;border:1px solid var(--border)">❌</button>
      </div>
    </div>

    <!-- Maxi 2026-06-30: caja de motivo de rechazo EN EL CUERPO VISIBLE de la card.
         Antes vivía dentro de .pcard-detail (display:none) → al apretar ❌ en la card
         colapsada el textarea se ponía en block pero su contenedor padre seguía oculto
         y el cuadro nunca aparecía. Ahora se revela inline sin tener que expandir. -->
    <textarea class="pcard-dislike-reason" placeholder="¿Por qué NO sirve esta web? Describí el CONTENIDO/TIPO (ej: spam/MFA, contenido autogenerado, agregador sin valor, foro muerto, pocas notas, baja calidad editorial). El agente aprende por TIPO de web — NUNCA descarta por país ni temática." style="display:none;width:100%;margin-top:6px;font-size:11px;padding:5px;border:1px solid #fca5a5;border-radius:4px;min-height:42px;resize:vertical"></textarea>

    <!-- Expandable detail panel -->
    <div class="pcard-detail" style="display:none;border-top:1px solid var(--border);padding:10px">

      <!-- Maxi 2026-06-18: vista alineada con Analysis tab — mismo header
           "✉️ Decision-maker Email" y misma estructura visual. -->
      <div class="sub-title" style="font-size:11px;font-weight:700;color:var(--text);margin-bottom:5px">✉️ Decision-maker Email</div>
      <div class="pcard-email-list email-list">
        ${hasEmail ? "" : '<div style="font-size:11px;color:#e53e3e;margin-bottom:4px">No emails — escribilo manualmente abajo</div>'}
      </div>
      <input type="text" class="form-input pcard-email-manual" placeholder="Enter email manually..." style="margin-top:4px;font-size:11px;padding:4px 7px" />

      <!-- Maxi 2026-06-18: Adicionales — mismo wording que Analysis tab. -->
      <details style="margin-top:6px;font-size:11px">
        <summary style="cursor:pointer;color:var(--text-muted);font-weight:700;letter-spacing:.3px">📨 Contactos Adicionales</summary>
        <div style="display:flex;flex-direction:column;gap:3px;margin-top:5px">
          <input type="email" class="form-input pcard-future-1" placeholder="Adicional 1" style="font-size:11px;padding:3px 6px" />
          <input type="email" class="form-input pcard-future-2" placeholder="Adicional 2" style="font-size:11px;padding:3px 6px" />
          <input type="email" class="form-input pcard-future-3" placeholder="Adicional 3" style="font-size:11px;padding:3px 6px" />
          <div class="pcard-future-status" style="font-size:10px;color:var(--text-muted)"></div>
        </div>
      </details>

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
        <textarea class="pitch-textarea pcard-pitch" rows="5" placeholder="Language draft auto-loads here...">${esc(r.pitch || "")}</textarea>

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
        <input type="text" class="form-input pcard-traffic" value="${esc(r.traffic ? formatTraffic(r.traffic) : "")}" placeholder="e.g. 500K, 1.2M, 3500000" style="font-size:11px;padding:4px 7px" title="Editable — fill manually if SimilarWeb didn't return data. Accepts '500K', '1.2M' or raw number." />
      </div>

      <!-- Action buttons in panel -->
      <div style="display:flex;gap:6px;margin-top:10px;align-items:center">
        <button class="btn btn-success btn-sm pcard-validate-expanded" style="flex:1">✅ Push + Send Email</button>
        <button type="button" class="pcard-like-btn" title="👍 Like — autopilot prioriza este tipo de lead" style="background:#fff;border:1px solid #d1d5db;border-radius:6px;padding:5px 8px;cursor:pointer;font-size:13px">👍</button>
        <button type="button" class="pcard-dislike-btn" title="👎 Dislike — el agente evita este tipo + el RAG aprende del feedback" style="background:#fff;border:1px solid #d1d5db;border-radius:6px;padding:5px 8px;cursor:pointer;font-size:13px">👎</button>
      </div>
      <!-- textarea .pcard-dislike-reason movido al cuerpo visible de la card (arriba) — Maxi 2026-06-30 -->
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

  // Auto-fetch tráfico — antes solo corría on-expand. Ahora dispara también al
  // renderizar (delayed un poco para no bloquear scroll inicial). Cache 90d
  // hace que en la mayoría de los casos sea hit gratis (0 RapidAPI calls).
  function autoFetchTraffic() {
    if (card.dataset._trafficFetched) return;
    card.dataset._trafficFetched = "1";
    const trafficInput = card.querySelector(".pcard-traffic");
    if (!trafficInput || trafficInput.value) return;
    trafficInput.placeholder = "⏳ Fetching traffic…";
    getTraffic(data.domain).then(t => {
      const v = t?.pageViews || t?.rawVisits || 0;
      // Guard: card pudo haberse re-renderizado por enrich antes del timeout.
      // isConnected es true si sigue en el DOM, false si fue reemplazada.
      if (!card.isConnected) return;
      if (v > 0) {
        data.traffic = v;
        if (!trafficInput.value) trafficInput.value = formatTraffic(v);
        trafficInput.placeholder = "e.g. 500K, 1.2M, 3500000";
      } else {
        trafficInput.placeholder = "No data — fill manually";
      }
    }).catch(() => { if (card.isConnected) trafficInput.placeholder = "Error — fill manually"; });
  }
  // Si la card ya viene sin tráfico, intentamos en background (no bloquea).
  // Guardo el handle así si la card es reemplazada, podemos cancelar.
  if (!data.traffic) {
    card._autoFetchTimer = setTimeout(autoFetchTraffic, 500);
  }

  // Domain link → open tab
  card.querySelector(".pcard-domain-link")?.addEventListener("click", e => {
    e.preventDefault();
    chrome.tabs.create({ url: e.currentTarget.dataset.url, active: false });
  });

  // Enriquecer: tráfico fresco + Apollo si faltan datos
  /* handler pcard-enrich-btn ("Completar") removido (B3/M1) — el botón se eliminó; el email
     lo trae el worker automáticamente (re-enrich). Era código muerto (querySelector null). */

  // Expand toggle — al abrir, lock del prospect 30 min para que otros MBs no lo
  // toquen al mismo tiempo. Se libera al cerrar la card o al cerrar la toolbar.
  card.querySelector(".pcard-expand-btn")?.addEventListener("click", async () => {
    const panel = card.querySelector(".pcard-detail");
    const btn   = card.querySelector(".pcard-expand-btn");
    const open  = panel.style.display === "none";
    panel.style.display = open ? "block" : "none";
    btn.textContent     = open ? "▲" : "▼";
    if (open) {
      // PERF: autoVerify ahora se dispara aquí (al expandir) en lugar de
      // en render inicial. Solo verifica emails de ESTA card, no de las 100.
      const listEl = card.querySelector(".pcard-email-list");
      if (listEl && !card.dataset._verifiedOnce) {
        card.dataset._verifiedOnce = "1";
        autoVerifyEmailChips(listEl).then(() => {
          // Re-render del email-list para reflejar grades nuevos
          if (typeof renderProspectEmailList === "function") renderProspectEmailList();
        }).catch(e => console.warn("[pcard autoVerify]", e));
      }
    }
    if (open && data.domain) {
      // Verificar si otro MB ya lo lockeó
      const lock = await getActiveProspectLock(state.accessToken, data.domain);
      if (lock && lock.locked_by.toLowerCase() !== (state.loginEmail || "").toLowerCase()) {
        const minLeft = Math.max(0, Math.round((new Date(lock.expires_at) - Date.now()) / 60000));
        showToast(`🔒 ${lock.locked_by} is reviewing this prospect (${minLeft} min). Coordinate before pushing.`, "warn", 6000);
      } else {
        lockProspect(state.accessToken, data.domain, state.loginEmail).catch(() => {});
      }
      // Auto-fetch tráfico si la card no lo tiene — usa cache 90d → 0 hits
      // si ya fue analizado por cualquier MB. Solo gasta hit si es dominio fresh.
      if (!data.traffic && !card.dataset._trafficFetched) {
        autoFetchTraffic();
      }
      // Maxi 2026-07-01: el auto-fetch de EMAILS al expandir se removió junto con el botón
      // "Completar". El email ahora lo trae el WORKER sí o sí (re-enrich cada 15min: scrape
      // mejorado → Apollo pago si el scrape falla). Si un lead todavía no tiene email, el MB
      // puede tipear uno manual abajo; el worker lo va a completar solo.
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

    // Orden por grade (A > B > C > D > E) — el mejor queda arriba y auto-selecto.
    // Backend prioriza Apollo en el array pero el ranking real es por grade.
    const _gradeRank = { A: 5, B: 4, C: 3, D: 2, E: 1 };
    const sorted = [...emails].sort((a, b) => {
      const cA = _emailVerifyCache.get(a);
      const cB = _emailVerifyCache.get(b);
      const sA = state.emailSources.get(a) || "";
      const sB = state.emailSources.get(b) || "";
      const gA = _gradeRank[_emailGrade(a, cA, sA).grade] || 0;
      const gB = _gradeRank[_emailGrade(b, cB, sB).grade] || 0;
      return gB - gA;
    });

    const VISIBLE = 5;
    const visible = sorted.slice(0, VISIBLE);
    const hidden  = sorted.slice(VISIBLE);

    // Lee source info del row prospect (email_sources). Backward compat:
    // - String legacy "scrape" / "apollo" → solo label
    // - Object nuevo {source, url} → label + URL clickeable (Maxi 2026-06-17)
    const sourceMap = data.email_sources || {};
    const _getEmailSource = (em) => {
      const raw = sourceMap[em.toLowerCase()];
      if (!raw) return { source: "", url: "" };
      if (typeof raw === "string") return { source: raw, url: "" };
      return { source: raw.source || "", url: raw.url || "" };
    };
    const SOURCE_LABEL = {
      apollo:   { txt: "apollo",     color: "#7c3aed" },
      informer: { txt: "informer",   color: "#0ea5e9" },
      scrape:   { txt: "sitio",      color: "#10b981" },
      generic:  { txt: "genérico",   color: "#94a3b8" },
    };
    // Maxi 2026-06-18: botón "+" para asignar email a slot adicional (1/2/3).
    // Misma lógica que en Analysis (renderEmailList).
    const slotIdxByEmail = (em) => {
      const v1 = (card.querySelector(".pcard-future-1")?.value || "").toLowerCase().trim();
      const v2 = (card.querySelector(".pcard-future-2")?.value || "").toLowerCase().trim();
      const v3 = (card.querySelector(".pcard-future-3")?.value || "").toLowerCase().trim();
      const x = em.toLowerCase();
      if (x === v1) return 1;
      if (x === v2) return 2;
      if (x === v3) return 3;
      return 0;
    };
    const chipFor = (e) => {
      const cached = _emailVerifyCache.get(e);
      const cls    = cached ? _verifyClass(cached) : "verify-pending";
      const srcObj = _getEmailSource(e);
      const g      = _emailGrade(e, cached, srcObj.source);
      const gb     = `<span class="email-grade email-grade-${g.grade}" title="${esc(g.label)}">${g.grade}</span>`;
      let srcChip  = "";
      if (srcObj.source) {
        const meta = SOURCE_LABEL[srcObj.source] || { txt: srcObj.source, color: "#64748b" };
        if (srcObj.url) {
          srcChip = `<a href="#" class="email-src-link" data-url="${esc(srcObj.url)}" title="Origen: ${esc(srcObj.url)}" style="font-size:9px;color:${meta.color};text-decoration:underline;margin-left:4px">(${meta.txt})</a>`;
        } else {
          srcChip = `<span title="Origen: ${esc(meta.txt)}" style="font-size:9px;color:${meta.color};margin-left:4px">(${meta.txt})</span>`;
        }
      }
      const curSlot = slotIdxByEmail(e);
      const btnLabel = curSlot ? String(curSlot) : "+";
      const btnTitle = curSlot
        ? `Asignado como Adicional ${curSlot} — click para quitar`
        : "Click para agregar como Contacto Adicional (envío paralelo día 0)";
      const futureBtn = `<button type="button" class="pcard-future-btn ${curSlot ? "assigned" : ""}" data-email-future="${esc(e)}" title="${esc(btnTitle)}" style="margin-left:auto;background:${curSlot?'#dc2626':'#94a3b8'};color:#fff;border:none;border-radius:50%;width:18px;height:18px;font-size:10px;cursor:pointer;font-weight:700">${btnLabel}</button>`;
      return `<div class="email-chip ${cls} ${curSlot ? 'slot-future' : ''}" data-email="${esc(e)}" title="Click = email principal · botón +/N = contacto adicional" style="display:flex;align-items:center;gap:4px">${gb}<span style="flex:1">${esc(e)}${srcChip}</span>${futureBtn}</div>`;
    };

    let html = visible.map(chipFor).join("");
    if (hidden.length > 0) {
      html += `<div class="email-chips-hidden" style="display:none">${hidden.map(chipFor).join("")}</div>`;
      html += `<button class="email-show-more" type="button" style="font-size:10px;background:transparent;border:none;color:#0369a1;cursor:pointer;padding:4px 0;text-decoration:underline">+ show ${hidden.length} more…</button>`;
    }

    // Maxi 2026-06-18: contact forms del worker — chip clickeable separado.
    const cfChips = [];
    Object.keys(sourceMap).forEach(k => {
      if (k.startsWith("__contact_form_")) {
        const v = sourceMap[k];
        const url = typeof v === "string" ? "" : (v?.url || "");
        if (url) cfChips.push(url);
      }
    });
    if (cfChips.length > 0) {
      html += `<div style="margin-top:6px;font-size:10px;color:var(--text-muted);font-weight:700;text-transform:uppercase;letter-spacing:.3px">📝 Contact Form</div>`;
      cfChips.forEach((url, i) => {
        html += `<a href="#" class="pcard-contact-form-chip" data-cf-url="${esc(url)}" title="Abrir formulario de contacto" style="display:inline-flex;align-items:center;gap:4px;padding:3px 9px;background:#10b981;color:#fff;border-radius:4px;font-size:11px;text-decoration:none;margin:2px;font-weight:600">📝 Form ${cfChips.length > 1 ? i + 1 : ""}</a>`;
      });
    }
    listEl.innerHTML = html;

    // Wire contact form chips
    listEl.querySelectorAll(".pcard-contact-form-chip").forEach(el => {
      el.addEventListener("click", (ev) => {
        ev.preventDefault();
        const url = el.dataset.cfUrl;
        if (url) chrome.tabs.create({ url, active: false });
      });
    });

    // Toggle ver más
    listEl.querySelector(".email-show-more")?.addEventListener("click", (e) => {
      const block = listEl.querySelector(".email-chips-hidden");
      if (block) { block.style.display = "block"; e.target.style.display = "none"; }
    });

    // Click chip = seleccionar + sincronizar email a Monday data,
    // PERO si el user ya tipeó algo manual en el input, NO pisar.
    const mondayEmailEl = card.querySelector(".pcard-email-monday");
    const manualEmailEl = card.querySelector(".pcard-email-manual");
    // Marcar el input Monday como "user-typed" cuando el user lo edita
    if (mondayEmailEl) {
      mondayEmailEl.addEventListener("input", () => {
        mondayEmailEl.dataset.userEdited = "1";
      });
    }
    if (manualEmailEl) {
      manualEmailEl.addEventListener("input", () => {
        manualEmailEl.dataset.userEdited = "1";
        // Maxi 2026-07-01: reflejar el email manual en el cuadro de Monday (el que
        // realmente se envía), así no queda uno arriba en "manual" y otro distinto abajo.
        // Si se borra el manual, el cuadro de Monday vuelve a tomar el chip seleccionado.
        const v = manualEmailEl.value.trim();
        if (mondayEmailEl) {
          if (v) { mondayEmailEl.value = v; mondayEmailEl.dataset.userEdited = "1"; }
          else {
            mondayEmailEl.dataset.userEdited = "";
            const sel = listEl.querySelector(".email-chip.selected");
            if (sel) mondayEmailEl.value = sel.dataset.email || "";
          }
        }
      });
    }
    // Maxi 2026-06-18: SINGLE-SELECT (1 email principal = 1 item Monday).
    // Antes multi-select creaba N items en Monday (= duplicaba la URL).
    // Ahora: 1 principal va a Monday; los adicionales se mandan en paralelo
    // pero NO crean items duplicados. Mismo comportamiento que Analysis.
    const _syncSelectedToInput = () => {
      const sel = listEl.querySelector(".email-chip.selected");
      if (mondayEmailEl && mondayEmailEl.dataset.userEdited !== "1" && sel) {
        mondayEmailEl.value = sel.dataset.email;
      }
      // Quitar badge legacy multi-select si quedó
      card.querySelector(".pcard-multi-badge")?.remove();
    };

    listEl.querySelectorAll(".email-chip").forEach(chip => {
      chip.addEventListener("click", (ev) => {
        // Click en link "(origen)" → abrir URL, no togglear
        const srcLink = ev.target.closest(".email-src-link");
        if (srcLink) {
          ev.preventDefault();
          ev.stopPropagation();
          const url = srcLink.dataset.url;
          if (url) chrome.tabs.create({ url, active: false });
          return;
        }
        // Click en botón "+ adicional" → manejado abajo, no togglear principal
        if (ev.target.classList.contains("pcard-future-btn")) return;
        // Single-select: limpiar todos + seleccionar este
        listEl.querySelectorAll(".email-chip").forEach(c => c.classList.remove("selected"));
        chip.classList.add("selected");
        _syncSelectedToInput();
      });
    });
    const first = listEl.querySelector(".email-chip:not(.slot-future)") || listEl.querySelector(".email-chip");
    if (first) first.classList.add("selected");
    _syncSelectedToInput();

    // Maxi 2026-06-18: handler del botón "+/N" para slots adicionales.
    // Misma lógica que en Analysis renderEmailList.
    listEl.querySelectorAll(".pcard-future-btn").forEach(btn => {
      btn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const email = btn.dataset.emailFuture;
        if (!email) return;
        const slots = [
          card.querySelector(".pcard-future-1"),
          card.querySelector(".pcard-future-2"),
          card.querySelector(".pcard-future-3"),
        ].filter(Boolean);
        if (slots.length === 0) return;
        const lowerE = email.toLowerCase().trim();
        const existingIdx = slots.findIndex(s => (s.value || "").toLowerCase().trim() === lowerE);
        if (existingIdx !== -1) {
          slots[existingIdx].value = "";
        } else {
          const freeSlot = slots.find(s => !(s.value || "").trim());
          if (!freeSlot) slots[slots.length - 1].value = email;
          else freeSlot.value = email;
          // Si el email era el principal, liberarlo y elegir otro
          if (mondayEmailEl && mondayEmailEl.value.toLowerCase() === lowerE) {
            mondayEmailEl.value = "";
            mondayEmailEl.dataset.userEdited = "";
            const next = listEl.querySelector(".email-chip:not(.slot-future):not(.selected)");
            if (next) {
              listEl.querySelectorAll(".email-chip").forEach(c => c.classList.remove("selected"));
              next.classList.add("selected");
              if (mondayEmailEl) mondayEmailEl.value = next.dataset.email;
            }
          }
        }
        // Re-render para actualizar etiquetas +/1/2/3 en TODOS los chips
        renderProspectEmailList();
      });
    });

    // PERF FIX 2026-05-13: NO disparar autoVerify en render inicial.
    // Antes: 100 cards × 5 emails = 500 verify fetches en paralelo →
    // hanging. Ahora se verifica SOLO cuando el user expande la card
    // (click en ▼) — ver wireExpandToggle más abajo. El usuario solo
    // necesita verify del lead que va a contactar, no de los 100.
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
        feedbackRules: await _getPitchRulesBlockPopup(),
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

  // 👎 Dislike — toggle textarea de razón. Al 2do click (con/sin texto) confirma
  // el feedback al autopilot + manda el comentario al RAG (Voyage embed) para que
  // futuros pitches eviten ese error.
  card.querySelector(".pcard-dislike-btn")?.addEventListener("click", async (e) => {
    e.stopPropagation();
    const btn = e.currentTarget;
    const reasonEl = card.querySelector(".pcard-dislike-reason");
    // Primer click: revelar textarea, segundo click: confirmar
    if (reasonEl && reasonEl.style.display === "none") {
      reasonEl.style.display = "block";
      reasonEl.focus();
      btn.title = "Click otra vez para confirmar (el comentario es opcional)";
      btn.style.background = "#fef3c7";
      btn.textContent = "👎✓";
      return;
    }
    const reason = (reasonEl?.value || "").trim().substring(0, 500);
    btn.disabled = true; btn.textContent = "⏳";
    try {
      // 1) Feedback al autopilot (categoría/geo/ad_networks)
      await saveAutopilotFeedback(state.accessToken, {
        user_email: state.loginEmail, domain: data.domain, action: "disliked",
        category: data.category, geo: data.geo, ad_networks: data.ad_networks,
        reason: reason || undefined,
      });
      // 2) RAG feedback con el pitch + razón (si tenemos pitch)
      if (data.pitch && typeof ragSavePitchFeedback === "function") {
        ragSavePitchFeedback("disliked", data.pitch, data.pitch_subject || "", {
          domain: data.domain, category: data.category, geo: data.geo,
          language: data.language || "", traffic: data.traffic || 0,
          reason,
        });
      }
    } catch (err) { console.warn("[dislike]", err); }
    if (reasonEl) reasonEl.style.display = "none";
    btn.textContent = "👎"; btn.style.background = "#fee2e2"; btn.style.borderColor = "#fca5a5";
    btn.title = `✓ Dislike${reason ? ` + razón guardada` : " guardado"} — el agente y RAG aprendieron`;
  });

  // Snooze 21d (solo para este MB, otros MBs siguen viendo el prospect)
  /* handler pcard-snooze-btn removido (B3) — el botón no existe en la card (código muerto) */

  // Reject
  // Maxi 2026-06-22: RECHAZO INTELIGENTE — el MB escribe POR QUÉ no sirve, Claude
  // deduce el TIPO de web, y el agente aprende por TIPO (no por geo/categoría).
  //   1er click → revela el comentario.  2do click → confirma (Claude analiza + aprende).
  card.querySelector(".pcard-reject-btn")?.addEventListener("click", async () => {
    const btn = card.querySelector(".pcard-reject-btn");
    const reasonEl = card.querySelector(".pcard-dislike-reason");
    if (reasonEl && reasonEl.style.display === "none") {
      reasonEl.style.display = "block";
      reasonEl.placeholder = "¿Por qué NO sirve esta web? Describí el CONTENIDO/TIPO (spam/MFA, autogenerado, agregador sin valor, foro muerto, baja calidad). El agente aprende por TIPO — NUNCA por país ni temática.";
      reasonEl.focus();
      btn.textContent = "✓"; btn.style.color = "#16a34a";
      btn.title = "Click de nuevo para confirmar el rechazo (el comentario enseña al agente el TIPO de web a evitar)";
      return;
    }
    const reason = (reasonEl?.value || "").trim().substring(0, 500);
    card.style.opacity = "0.4"; btn.disabled = true; btn.textContent = "⏳";
    // Maxi 2026-06-30: INVESTIGAR el sitio para aprender — fetch del HTML en vivo y
    // extraer título + meta description + headings, para que Claude clasifique por
    // CONTENIDO REAL (no solo por los campos guardados). Best-effort, con timeout corto.
    let siteSnippet = "";
    try {
      const resp = await fetch(`https://${data.domain}`, { signal: AbortSignal.timeout(5000) });
      if (resp.ok) {
        const html = await resp.text();
        const pick = (re) => { const m = html.match(re); return m ? m[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, 160) : ""; };
        const title = pick(/<title[^>]*>([\s\S]*?)<\/title>/i);
        const desc  = pick(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i)
                   || pick(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']description["']/i);
        const heads = [...html.matchAll(/<h[12][^>]*>([\s\S]*?)<\/h[12]>/gi)].slice(0, 4)
          .map(m => m[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim()).filter(Boolean).join(" | ").slice(0, 240);
        siteSnippet = [title && `Título real: ${title}`, desc && `Descripción: ${desc}`, heads && `Titulares: ${heads}`].filter(Boolean).join("\n");
      }
    } catch { /* sitio caído/CORS → uso solo los campos guardados */ }
    // Claude deduce el TIPO de web por CONTENIDO (ignora país y temática general).
    let webType = "";
    try {
      const { callClaude, CLAUDE_HAIKU } = await import("../modules/claude.js");
      const r = await callClaude({
        model: CLAUDE_HAIKU, maxTokens: 40,
        system: "Clasificás sitios web para un equipo de monetización publicitaria. Devolvé en 2-6 palabras el TIPO de web a evitar según su CALIDAD y NATURALEZA de CONTENIDO (ej: 'sitio MFA/spam', 'contenido autogenerado', 'agregador sin valor', 'foro muerto', 'blog bajo tráfico', 'directorio', 'web corporativa sin inventario'). REGLA DURA: NO clasifiques por país/idioma ni por temática general (deportes, noticias, autos, etc. son válidos) — solo por la CALIDAD/TIPO del contenido. SOLO el tipo, sin explicación.",
        messages: [{ role: "user", content: `Dominio: ${data.domain}\nTítulo guardado: ${data.page_title || ""}\n${siteSnippet || "(no se pudo leer el sitio en vivo)"}\nMotivo del rechazo del MB: ${reason || "(sin comentario)"}` }],
      });
      webType = (r?.text || "").trim().replace(/^["']|["']$/g, "").slice(0, 60);
    } catch (e) { console.warn("[reject] Claude type err", e?.message); }
    await Promise.all([
      rejectReviewItem(state.accessToken, id, data.domain),
      saveAutopilotFeedback(state.accessToken, {
        user_email: state.loginEmail, domain: data.domain, action: "disliked",
        category: data.category, geo: data.geo, ad_networks: data.ad_networks, traffic: data.traffic,
        reason: [reason, webType ? `[tipo: ${webType}]` : ""].filter(Boolean).join(" ") || undefined,
      }),
    ]);
    if (typeof showToast === "function") showToast(webType ? `🧠 Aprendido — evitar tipo: ${webType}` : "❌ Rechazado", "info", 3500);
    card.remove();
    refreshProspectsStats();
  });

  // Validate (compact) → uses data defaults
  /* handler pcard-validate-btn removido (B3) — el botón no existe en la card (código muerto) */

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
    setResult("❌ GEO required. Fill the GEO field before sending.", false);
    card.querySelector(".pcard-geo")?.focus();
    return;
  }
  // 2. Tráfico (Páginas Vistas) > 0 — leído del input editable
  if (!trafficNum || trafficNum === 0) {
    setResult("❌ Page Views required. Fill the Traffic field (accepts 500K, 1.2M or raw number).", false);
    card.querySelector(".pcard-traffic")?.focus();
    return;
  }
  // 3. Email válido (siempre obligatorio para push, aún si no se manda mail)
  // Maxi 2026-07-01: aviso en inglés — el email lo busca el worker automáticamente;
  // si todavía no está, el MB debe esperar (o tipear uno manual).
  if (!email) {
    setResult("⏳ No email yet — the system is still searching for it. Please wait a moment, or type one manually below.", false);
    card.querySelector(".pcard-email-monday")?.focus();
    return;
  }
  if (!isValidEmail(email)) {
    setResult(`❌ Invalid email: ${email}`, false);
    card.querySelector(".pcard-email-monday")?.focus();
    return;
  }
  // 4. Subject + Pitch (obligatorios para enviar mail)
  if (doSendEmail) {
    if (!subject) {
      setResult("❌ Subject required. Fill it before sending.", false);
      card.querySelector(".pcard-subject")?.focus();
      return;
    }
    if (!pitch) {
      setResult("❌ Email body required.", false);
      card.querySelector(".pcard-pitch")?.focus();
      return;
    }
  }
  // 5. Owner / Status / Language deben estar seteados
  if (!ejecutivo)        { setResult("❌ Owner required.", false);    return; }
  if (!estado)           { setResult("❌ Status required.", false);   return; }
  if (idioma === "" || idioma == null) { setResult("❌ Language required.", false); return; }
  // 6. Date — debe ser DD/MM/YYYY parseable
  if (!dateStr) {
    setResult("❌ Date required.", false);
    card.querySelector(".pcard-date")?.focus();
    return;
  }
  const dateParts = dateStr.split("/");
  if (dateParts.length !== 3 || dateParts[2].length !== 4 || isNaN(parseInt(dateParts[0])) || isNaN(parseInt(dateParts[1]))) {
    setResult("❌ Invalid date. Format DD/MM/YYYY.", false);
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
    // Bump counter para Activity admin panel. Antes solo se hacía desde Analysis,
    // las cargas desde Prospects (validate + push/send) no se contaban.
    incrementUserDailyCounter(state.accessToken, state.loginEmail, "monday").catch(() => {});

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
      incrementUserDailyCounter(state.accessToken, state.loginEmail, "emails").catch(() => {});

      // Maxi 2026-06-18: contactos adicionales se envían EN PARALELO día 0
      // (no se encolan). Si el original rebota/OOO, bounce handler en worker
      // actualiza Monday con el adicional que respondió bien.
      const futSels = [".pcard-future-1", ".pcard-future-2", ".pcard-future-3"];
      const futStatusEl = card.querySelector(".pcard-future-status");
      const sentMsgs = [];
      const failMsgs = [];
      for (const sel of futSels) {
        const fe = card.querySelector(sel)?.value?.trim()?.toLowerCase();
        if (!fe || !fe.includes("@")) continue;
        if (fe === email.toLowerCase()) { failMsgs.push(`⏭️ ${fe} igual al principal`); continue; }
        const bFut = await isEmailBounced(state.accessToken, fe).catch(() => ({ bounced: false }));
        if (bFut.bounced) { failMsgs.push(`🚫 ${fe} bounced`); continue; }
        const altRes = await sendEmail({ to: fe, subject, body: fullBody, expectedFrom: state.loginEmail });
        if (altRes.ok) {
          sentMsgs.push(fe);
          incrementUserDailyCounter(state.accessToken, state.loginEmail, "emails").catch(() => {});
          // Registrar en response_tracking
          fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_response_tracking`, {
            method: "POST",
            headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({
              mb_email:      state.loginEmail.toLowerCase(),
              domain:        data.domain,
              email_sent_to: fe,
              source:        "manual_extra",
              geo:           geo,
              category:      data.category || "",
              sent_at:       new Date().toISOString(),
            }),
          }).catch(() => {});
        } else {
          failMsgs.push(`❌ ${fe}: ${altRes.error || "send fail"}`);
        }
      }
      if (futStatusEl && (sentMsgs.length || failMsgs.length)) {
        const parts = [];
        if (sentMsgs.length) parts.push(`✅ Adicionales día 0: ${sentMsgs.join(", ")}`);
        if (failMsgs.length) parts.push(`Problemas: ${failMsgs.join(" · ")}`);
        futStatusEl.textContent = parts.join(" | ");
        futStatusEl.style.color = failMsgs.length && !sentMsgs.length ? "#dc2626" : sentMsgs.length ? "#16a34a" : "#d97706";
      }
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

    // 5. Update UI — desaparece inmediato (200ms para mostrar el ✅)
    card.style.opacity = "0.3";
    setResult(doSendEmail && email ? "✅ Monday + Email sent!" : "✅ Pushed to Monday");
    setTimeout(() => { card.remove(); refreshProspectsStats(); }, 200);

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
  if (!remaining && listEl) listEl.innerHTML = '<div class="cascade-empty">Queue empty — great work! 🎉</div>';
  const count = await getDailyValidationCount(state.accessToken, state.loginEmail);
  updateProspectsDailyBar(count);
  // Mantener formato del lote (consistente con loadProspectsTab) para que el counter
  // "Enviaste X/30" se actualice on-the-fly tras cada validate sin recargar.
  if (statsEl) {
    const DAILY_SEND_CAP = 30;
    const sent = count;
    const left = Math.max(0, DAILY_SEND_CAP - sent);
    const SLOT_MIN = 30;
    const minsLeft = SLOT_MIN - Math.floor((Date.now() % (SLOT_MIN * 60 * 1000)) / 60000);
    if (remaining > 0) {
      statsEl.innerHTML = `<strong>${remaining}</strong> leads en tu lote · enviaste <strong>${sent}/${DAILY_SEND_CAP}</strong> hoy (te quedan ${left}) · 🔄 nuevo lote en ${minsLeft}min`;
    } else {
      statsEl.innerHTML = `enviaste <strong>${sent}/${DAILY_SEND_CAP}</strong> hoy — ¡buen trabajo! 🎉`;
    }
  }
  // Actualizar tab badge
  const tabCount = document.getElementById("tab-prospects-count");
  if (tabCount) tabCount.textContent = remaining > 0 ? `(${remaining})` : "";
}

async function initProspectsTab() {
  // Restore filtros guardados de sesiones previas
  try {
    // Maxi 2026-06-22: por DEFAULT mostrar TODAS las URLs (sin necesidad de clickear
    // "All"). Antes se restauraban los filtros guardados → al abrir Prospects quedaba
    // un filtro viejo activo y había que tocar "All" para ver todo. Ahora arranca SIEMPRE
    // sin filtros = todas. Si el MB activa un filtro en la sesión, ese se aplica.
    const dateEl   = document.getElementById("prospects-date-filter");
    const sourceEl = document.getElementById("prospects-source-filter");
    const userEl   = document.getElementById("prospects-user-filter");
    const geoEl    = document.getElementById("prospects-geo-filter");
    const trafEl   = document.getElementById("prospects-traffic-filter");
    const nameEl   = document.getElementById("prospects-name-filter");
    if (dateEl)   dateEl.value   = "";
    if (sourceEl) sourceEl.value = "";
    if (userEl)   userEl.value   = "";
    if (geoEl)    geoEl.value    = "";
    if (trafEl)   trafEl.value   = "";
    if (nameEl)   nameEl.value   = "";
    window._selectedGeoChips = new Set();  // sin chips GEO → todas
    window._prospectsTypeFilter = "all";   // Maxi 2026-07-01: filtro Type arranca en All
  } catch {}

  // Maxi 2026-07-01: ATAJOS DE TECLADO sobre la card con hover (menos mouse en 50 cards/día).
  //   E = expandir/cerrar · R = rechazar · Enter = enviar (solo si ya está expandida y revisada).
  // Solo actúa en el tab Prospects visible y si NO se está tipeando en un campo.
  if (!window._prospectsKeyboardBound) {
    window._prospectsKeyboardBound = true;
    document.getElementById("prospects-list")?.addEventListener("mouseover", (e) => {
      const card = e.target.closest?.(".pcard");
      if (card) window._hoveredProspectCard = card;
    });
    document.addEventListener("keydown", (e) => {
      const tag = (e.target.tagName || "").toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select" || e.target.isContentEditable) return;
      const list = document.getElementById("prospects-list");
      if (!list || list.offsetParent === null) return;  // tab Prospects no visible
      const card = window._hoveredProspectCard;
      if (!card || !document.body.contains(card)) return;
      const k = (e.key || "").toLowerCase();
      if (k === "e") { e.preventDefault(); card.querySelector(".pcard-expand-btn")?.click(); }
      else if (k === "r") { e.preventDefault(); card.querySelector(".pcard-reject-btn")?.click(); }
      else if (e.key === "Enter") {
        const detail  = card.querySelector(".pcard-detail");
        const sendBtn = card.querySelector(".pcard-validate-expanded");
        if (detail && detail.style.display !== "none" && sendBtn) { e.preventDefault(); sendBtn.click(); }
        else { e.preventDefault(); card.querySelector(".pcard-expand-btn")?.click(); }  // colapsada → expandir para revisar
      }
    });
  }

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
  document.getElementById("prospects-geo-filter")?.addEventListener("change", async (e) => {
    chrome.storage.local.set({ _prospectsGeoFilter: e.target.value }).catch(() => {});
    await loadProspectsTab();
  });
  document.getElementById("prospects-traffic-filter")?.addEventListener("change", async (e) => {
    chrome.storage.local.set({ _prospectsTrafficFilter: e.target.value }).catch(() => {});
    await loadProspectsTab();
  });
  // Name filter — debounce 300ms para no recargar en cada tecla
  let _nameDebounce = null;
  document.getElementById("prospects-name-filter")?.addEventListener("input", (e) => {
    clearTimeout(_nameDebounce);
    const v = e.target.value;
    _nameDebounce = setTimeout(async () => {
      chrome.storage.local.set({ _prospectsNameFilter: v }).catch(() => {});
      await loadProspectsTab();
    }, 300);
  });
  // Chips delegate handler
  document.getElementById("prospects-geo-chips")?.addEventListener("click", async (e) => {
    const chip = e.target.closest(".geo-chip");
    if (!chip) return;
    const code = chip.dataset.code;
    if (!code) return;
    if (!window._selectedGeoChips) window._selectedGeoChips = new Set();
    if (code === "_ALL_") {
      window._selectedGeoChips.clear();
    } else {
      if (window._selectedGeoChips.has(code)) window._selectedGeoChips.delete(code);
      else window._selectedGeoChips.add(code);
    }
    chrome.storage.local.set({ _prospectsGeoChips: [...window._selectedGeoChips] }).catch(() => {});
    await loadProspectsTab();
  });
  // Panel multi-país (toggle visibilidad + búsqueda + pick)
  document.getElementById("btn-prospects-country-picker")?.addEventListener("click", () => {
    const panel = document.getElementById("prospects-country-panel");
    if (!panel) return;
    const open = panel.style.display === "none" || !panel.style.display;
    panel.style.display = open ? "block" : "none";
    if (open) _renderCountryPanel(document.getElementById("prospects-country-search")?.value || "");
  });
  document.getElementById("prospects-country-search")?.addEventListener("input", (e) => {
    _renderCountryPanel(e.target.value || "");
  });
  document.getElementById("prospects-country-clear")?.addEventListener("click", async () => {
    if (!window._selectedGeoChips) window._selectedGeoChips = new Set();
    // Quitar solo los códigos de país (2 chars) — preservar continentes seleccionados
    for (const code of [...window._selectedGeoChips]) {
      if (code.length === 2 && !code.startsWith("_")) window._selectedGeoChips.delete(code);
    }
    chrome.storage.local.set({ _prospectsGeoChips: [...window._selectedGeoChips] }).catch(() => {});
    await loadProspectsTab();
    _renderCountryPanel(document.getElementById("prospects-country-search")?.value || "");
  });
  document.getElementById("prospects-country-list")?.addEventListener("click", async (e) => {
    const btn = e.target.closest(".country-pick");
    if (!btn) return;
    const code = btn.dataset.code;
    if (!code) return;
    if (!window._selectedGeoChips) window._selectedGeoChips = new Set();
    if (window._selectedGeoChips.has(code)) window._selectedGeoChips.delete(code);
    else window._selectedGeoChips.add(code);
    chrome.storage.local.set({ _prospectsGeoChips: [...window._selectedGeoChips] }).catch(() => {});
    await loadProspectsTab();
    _renderCountryPanel(document.getElementById("prospects-country-search")?.value || "");
  });
  // ── Filter presets por SOURCE ─────
  document.querySelectorAll(".prospects-preset").forEach(btn => {
    btn.addEventListener("click", async () => {
      const source = btn.dataset.source || "";
      const sourceEl = document.getElementById("prospects-source-filter");
      if (sourceEl) sourceEl.value = source;
      chrome.storage.local.set({ _prospectsSourceFilter: source }).catch(() => {});
      document.querySelectorAll(".prospects-preset").forEach(b => {
        if (b.dataset.source === source) {
          b.style.background = "#0ea5e9"; b.style.color = "#fff";
          b.style.border = "none"; b.style.fontWeight = "600";
        } else {
          b.style.background = "#1e293b"; b.style.color = "#cbd5e1";
          b.style.border = "1px solid #334155"; b.style.fontWeight = "400";
        }
      });
      await loadProspectsTab();
    });
  });

  // Maxi 2026-06-18: Filter presets por USUARIO (Maxi/Diego/Agus/Todos)
  document.querySelectorAll(".prospects-user-preset").forEach(btn => {
    btn.addEventListener("click", async () => {
      const user = btn.dataset.user || "";
      const userEl = document.getElementById("prospects-user-filter");
      if (userEl) userEl.value = user;
      chrome.storage.local.set({ _prospectsUserFilter: user }).catch(() => {});
      document.querySelectorAll(".prospects-user-preset").forEach(b => {
        if (b.dataset.user === user) {
          b.style.background = "#0ea5e9"; b.style.color = "#fff";
          b.style.border = "none"; b.style.fontWeight = "600";
        } else {
          b.style.background = "#1e293b"; b.style.color = "#cbd5e1";
          b.style.border = "1px solid #334155"; b.style.fontWeight = "400";
        }
      });
      await loadProspectsTab();
    });
  });

  // Maxi 2026-07-01: filtro TYPE (All / ⚠️ Alert / ✉️ No Email)
  document.querySelectorAll(".prospects-type-preset").forEach(btn => {
    btn.addEventListener("click", async () => {
      const type = btn.dataset.type || "";
      window._prospectsTypeFilter = type || "all";
      chrome.storage.local.set({ _prospectsTypeFilter: window._prospectsTypeFilter }).catch(() => {});
      document.querySelectorAll(".prospects-type-preset").forEach(b => {
        const on = (b.dataset.type || "") === type;
        b.style.background = on ? "#0ea5e9" : "#1e293b";
        b.style.color = on ? "#fff" : "#cbd5e1";
        b.style.border = on ? "none" : "1px solid #334155";
        b.style.fontWeight = on ? "600" : "400";
      });
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
      if (!confirm(`About to open ${checked.length} tabs. Defensive cap is ${HARD_CAP}. Continue anyway?`)) return;
    }
    const domains = checked
      .map(c => c.closest(".pcard")?.dataset?.domain)
      .filter(Boolean);
    domains.forEach((domain, i) => {
      setTimeout(() => chrome.tabs.create({ url: `https://${domain}`, active: false }), i * 400);
    });
    showToast(`🪟 Opening ${domains.length} tabs...`, "info");
  });
  document.getElementById("btn-bulk-reject")?.addEventListener("click", async () => {
    const checked = [...document.querySelectorAll(".pcard-bulk-cbx:checked")];
    if (checked.length === 0) return;
    if (!confirm(`Reject ${checked.length} prospects? They will be marked as permanently blocked.`)) return;
    const ids = checked.map(c => parseInt(c.dataset.id, 10)).filter(Boolean);
    showToast(`⏳ Rejecting ${ids.length} prospects...`, "info");
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
    showToast(`✅ Rejected ${ok}${fail > 0 ? ` (${fail} failed)` : ""}.`, fail > 0 ? "warn" : "info");
    updateBulkBar();
    refreshProspectsStats();
  });

  // Soft refresh agresivo cada 8s: chequea si algún lead que tenemos en pantalla
  // ya fue contactado por otro MB (status != pending) y lo quita inmediatamente
  // del DOM sin re-render full (sin flicker). Cross-MB realtime feel.
  setInterval(async () => {
    if (document.visibilityState === "hidden") return;
    const tab = document.getElementById("tab-prospects");
    if (!tab?.classList.contains("active")) return;
    const cards = document.querySelectorAll(".pcard[data-id]");
    if (cards.length === 0) return;
    const visibleIds = [...cards].map(c => parseInt(c.dataset.id, 10)).filter(Boolean);
    if (visibleIds.length === 0) return;
    try {
      // Pregunta: cuáles de los IDs que veo siguen en status=pending?
      const idList = visibleIds.join(",");
      const res = await fetch(
        `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_review_queue?id=in.(${idList})&status=eq.pending&select=id`,
        { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${state.accessToken}` } }
      );
      if (!res.ok) return;
      const stillPending = new Set((await res.json()).map(r => r.id));
      // Las que YA NO están pending → otro MB (o el agent) las contactó → fade-out
      visibleIds.forEach(id => {
        if (!stillPending.has(id)) {
          const card = document.querySelector(`.pcard[data-id="${id}"]`);
          if (card) {
            card.style.transition = "opacity 0.3s, max-height 0.3s, padding 0.3s, margin 0.3s";
            card.style.opacity = "0.2";
            card.style.maxHeight = "30px";
            card.style.overflow = "hidden";
            setTimeout(() => { card.remove(); refreshProspectsStats(); }, 350);
          }
        }
      });
    } catch {}
  }, 8_000);

  // Reload full cada 30s para traer leads NUEVOS del autopilot/csv/sellers.
  // Maxi 2026-06-22: auto-refresh cada 15 MIN (antes 30s, que reordenaba y molestaba).
  // El orden es estable (por fecha) y se preserva la PÁGINA actual → trae leads nuevos
  // sin sacar al MB de su lugar. El ↻ manual y los filtros sí re-ordenan/van a página 1.
  setInterval(async () => {
    if (document.visibilityState === "hidden") return;
    const tab = document.getElementById("tab-prospects");
    if (!tab?.classList.contains("active")) return;
    await loadProspectsTab({ keepPage: true }).catch(() => {});
  }, 15 * 60_000);
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
