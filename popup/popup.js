// ============================================================
// ADEQ TOOLBAR — Popup v4
// ============================================================

import { checkDuplicate, pushToMonday, updateMonday, getMondayBoardIndex, setFollowUpDates, fetchImportCandidates, fetchMondayForRefresh } from "../modules/monday.js";
import { getTraffic, formatTraffic, passesTrafficFilter, setTrafficAuthToken } from "../modules/traffic.js";
import { scrapeEmailsFromPage, findDecisionMakerViaApollo, quickValidateEmail, revealApolloEmail } from "../modules/scraper.js";
import { runAudit }                                                                            from "../modules/audit.js";
import { generatePitch, generateFollowUp }                                                    from "../modules/gemini.js";
import { searchEmailsWithGemini }                                                              from "../modules/geminiSearch.js";
import { verifyEmail, verifyEmailDeep, isGarbageEmail }                                         from "../modules/emailVerifier.js";
import { runCascade, getSimilarSites }                                                         from "../modules/cascade.js";
import { detectBanners }                                                                       from "../modules/bannerDetector.js";
import { saveHistory, loadHistory, clearHistory, saveSendDate, getSendInfo, markFUSent,
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
import { callProxy, setProxyAuth }                                                              from "../modules/apiProxy.js";

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
  sendInfo: null,
  category: "",
  siteLanguage: "",
  pageTitle: "", pageDescription: "",
  decisionMakerName: "",
  generatedPitches: [],
  loginEmail: "",
  gmailEmail: "",
  accessToken: "",
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
  setProxyAuth(auth.accessToken);

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
          setProxyAuth(r.access_token);
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

  document.getElementById("site-url").textContent = state.domain;
  document.getElementById("cascade-seed").value   = state.domain;

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
  // Load per-user Claude custom prompt into state (no-op if empty). Used by generatePitch.
  getCustomPrompt(auth.accessToken, auth.user).then(p => { state.customPrompt = p || ""; }).catch(() => {});
  // initKeywords, initAutopilot, initProspectsTab, initCsvQueue, loadHistoryTab → lazy on tab click

  // Show the toolbar login email as the Gmail "from" account
  const fromEl = document.getElementById("gmail-from");
  if (fromEl && state.loginEmail) fromEl.textContent = `From: ${state.loginEmail}`;

  // Follow-up check
  getSendInfo(state.domain).then(info => {
    if (info) { state.sendInfo = info; checkFUStatus(info); }
  });

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
    const pvDisplay = document.getElementById("form-pv-display");
    if (pvDisplay) pvDisplay.textContent = formatTraffic(state.traffic);

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
    filterEl.textContent = passesTrafficFilter(trafficForFilter) ? "✅ Supera umbral 500K" : "❌ Bajo umbral 500K";
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
    // Only scrape the page DOM automatically (free + no external leak).
    // Informer/WhoIs are paid third-parties that leak the visited domain — run only on-demand via Apollo/Gemini buttons.
    const pageEmails = await scrapeEmailsFromPage(state.tabId);
    state.emails = []; state.emailSources = new Map();
    addEmailsWithSource((sess?.emails || []).filter(quickValidateEmail), "Cache");
    addEmailsWithSource(pageEmails.filter(quickValidateEmail),           "Page");
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
// FOLLOW-UP
// ============================================================
function checkFUStatus(sendInfo) {
  // Banner FU oculto a pedido del user — confunde y no se está usando.
  // Si querés volver a activarlo, restaurar la lógica del git history (commit anterior).
  const banner = document.getElementById("fu-banner");
  if (banner) banner.style.display = "none";
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
  const rows = people.map((p, i) => {
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
  }).join("");

  resultEl.innerHTML = `
    <details class="apollo-details" open>
      <summary class="apollo-summary">👥 ${esc(summary)} <span class="apollo-toggle">click to toggle</span></summary>
      <div class="apollo-list">${rows}</div>
    </details>
  `;

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
    const ctxStr = buildPitchContext(ctxFields);
    const embedding = await voyageEmbed(ctxStr, "document");
    if (!embedding) return;
    await insertPitchFeedback(state.accessToken, state.loginEmail, {
      ...ctxFields,
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

async function bindButtons() {

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
    await saveDislikePitch(pitch);
    ragSavePitchFeedback("disliked", pitch, subject, {
      domain: state.domain, category: state.category,
      geo: detectGeo?.() || "", language: state.siteLanguage || "", traffic: state.traffic || 0,
    });
    const likeStatus = document.getElementById("pitch-like-status");
    likeStatus.textContent = "✗ Marked to avoid (RAG)";
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

  // Generar Follow-Up
  document.getElementById("btn-generate-fu").addEventListener("click", async () => {
    const btn      = document.getElementById("btn-generate-fu");
    const fuNumber = parseInt(document.getElementById("fu-banner")?.dataset?.fuNumber || "1");
    const pitchEl  = document.getElementById("pitch-text");

    if (!state.sendInfo?.sendDate) {
      alert("Original send date not found. Send the first pitch before generating a follow-up.");
      return;
    }

    btn.disabled   = true; btn.textContent = "⏳...";

    try {
      const days = Math.floor((Date.now() - new Date(state.sendInfo.sendDate)) / 86_400_000);
      const text = await generateFollowUp({
        domain:        state.domain,
        originalPitch: state.sendInfo?.pitch || "",
        fuNumber,
        daysSinceSend: days,
      });
      document.getElementById("pitch-text").value   = text;
      document.getElementById("form-subject").value = `Re: Partnership opportunity — ${state.domain}`;
      document.querySelector('[data-tab="core"]')?.click();
      btn.textContent = "✅ Generated";
    } catch (err) {
      console.error("[Generate FU]", err);
      alert("Follow-up generation failed: " + (err.message || "unknown error"));
      btn.disabled = false; btn.textContent = "✨ Generate FU";
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

  document.getElementById("btn-push-monday").addEventListener("click", async () => {
    const btn    = document.getElementById("btn-push-monday");
    const res    = document.getElementById("push-result");
    const { email, geo, idioma, estado, fecha, pitch, ejecutivo } = getMondayFormValues();

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
    // Guard #3: no dejar pushear a Monday si todavía no se mandó el email
    if (!state.emailSentInSession && !state.duplicate?.found) {
      const msg = "❌ You need to send the email first via Gmail. Click 'Send Gmail' before pushing to Monday.";
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
      } else {
        const item = await pushToMonday({
          domain: state.domain,
          traffic: formatTraffic(state.traffic),
          email, geo, idioma, pitch, estado, fecha, ejecutivo,
          loginEmail: state.loginEmail,
        });
        state.mondayItemId = item?.id;
        res.textContent = `✅ Created: ${item?.name || state.domain}`; res.className = "push-result ok";

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
      const today = new Date().toISOString().split("T")[0];
      const { fu1Date, fu2Date } = await saveSendDate(state.domain, {
        sendDate: today,
        pitch,
        email,
      });
      if (state.mondayItemId) setFollowUpDates(state.mondayItemId, fu1Date, fu2Date);
      const fuNumber = parseInt(document.getElementById("fu-banner")?.dataset?.fuNumber || "0");
      if (fuNumber) {
        markFUSent(state.domain, fuNumber);
        document.getElementById("fu-banner").style.display = "none";
      }
      // Update "From" label with the actual Gmail account used
      const fromEl = document.getElementById("gmail-from");
      if (fromEl) fromEl.textContent = `From: ${state.loginEmail}`;
      const assocEl = document.getElementById("settings-gmail-assoc");
      if (assocEl) assocEl.textContent = state.loginEmail;

      res.textContent = `✅ Email sent · FU1: ${fu1Date} · FU2: ${fu2Date}`;
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
  // Hidratar custom prompt desde state (cargado al boot)
  const promptEl = document.getElementById("settings-custom-prompt");
  if (promptEl) promptEl.value = state.customPrompt || "";
  const statusEl = document.getElementById("custom-prompt-status");
  if (statusEl) statusEl.textContent = state.customPrompt ? `${state.customPrompt.length} chars saved` : "empty";

  // Claude usage counters (today + last 30 days) — non-blocking
  const todayEl = document.getElementById("stats-claude-today");
  const monthEl = document.getElementById("stats-claude-month");
  if (todayEl && monthEl) {
    todayEl.textContent = "…"; monthEl.textContent = "…";
    Promise.all([
      getApiUsageForProvider(state.accessToken, state.loginEmail, "anthropic", 1),
      getApiUsageForProvider(state.accessToken, state.loginEmail, "anthropic", 30),
    ]).then(([today, month]) => {
      todayEl.textContent = String(today.total);
      monthEl.textContent = String(month.total);
    }).catch(() => {
      todayEl.textContent = "–"; monthEl.textContent = "–";
    });
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
    const value = taEl.value.trim();
    saveBtn.disabled = true; saveBtn.textContent = "⏳ Saving...";
    const r = await setCustomPrompt(state.accessToken, state.loginEmail, value);
    saveBtn.disabled = false; saveBtn.textContent = "💾 Save prompt";
    if (!r.ok) { statusEl.textContent = `❌ Save failed (${r.status || r.error})`; statusEl.style.color = "var(--danger)"; return; }
    state.customPrompt = value;
    statusEl.textContent = `✅ Saved · ${value.length} chars`;
    statusEl.style.color = "var(--success-text)";
    setTimeout(() => { statusEl.style.color = "var(--text-muted)"; }, 3000);
  });

  clearBtn?.addEventListener("click", async () => {
    if (!confirm("Clear your custom prompt?")) return;
    taEl.value = "";
    const r = await setCustomPrompt(state.accessToken, state.loginEmail, "");
    if (r.ok) { state.customPrompt = ""; statusEl.textContent = "empty"; }
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
  const countryStr = site.countryCode ? (COUNTRY_NAMES[site.countryCode] || site.countryCode) : "";

  const s    = scoreProspect({ pageViews: site.visits, rawVisits: site.visits });
  const grade = `<span class="score-grade-sm" style="background:${s.color}" title="${s.label}">${s.grade}</span>`;

  item.innerHTML = `
    <input type="checkbox" />
    <img class="cascade-favicon" loading="lazy" src="https://www.google.com/s2/favicons?domain=${esc(site.domain)}&sz=16" onerror="this.style.display='none'" />
    <span class="cascade-domain" title="${esc(site.domain)}">${esc(site.domain)}</span>
    <span class="cascade-visits">${esc(formatTraffic(site.visits))}</span>
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
  const [tMin, tMax] = parseRange(document.getElementById("cascade-min-traffic").value);
  const [rMin, rMax] = parseRange(document.getElementById("cascade-max-rank").value);
  const langFilter   = document.getElementById("cascade-language").value;

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
    if (tMin > 0 && site.visits < tMin) return false;
    if (tMax !== Infinity && site.visits > tMax) return false;
    if (rMin > 0 && site.globalRank && site.globalRank < rMin) return false;
    if (rMax !== Infinity && site.globalRank && site.globalRank > rMax) return false;
    if (langFilter && site.countryCode !== langFilter) return false;
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
    console.error("[auth] No se pudo determinar el usuario logueado. Email:", email);
    // Forzar re-login: borra auth y recarga
    chrome.storage.local.remove("auth").then(() => window.location.reload());
    return;
  }

  state.mediaBuyer   = name;
  state.loginEmail   = email;
  state.accessToken  = auth?.accessToken || "";

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

  // Estado inicial del toggle
  enabledCbx.checked = await getCsvQueueEnabled(state.accessToken);
  if (enabledCbx.checked) startHeartbeat();
  enabledCbx.addEventListener("change", async () => {
    await setCsvQueueEnabled(enabledCbx.checked, state.accessToken);
    if (enabledCbx.checked) startHeartbeat();
    else stopHeartbeat();
  });

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

      // Aviso si pasa el límite diario de 75 — solo se procesarán los primeros 75 hoy
      if (unique.length > 75) {
        const ok = confirm(`⚠️ You uploaded ${unique.length} domains but the limit is 75/day per user.\n\nThe first 75 will be processed today and the rest over following days automatically.\n\nContinue?`);
        if (!ok) { uploadRes.textContent = "Upload canceled."; uploadRes.className = "push-result"; return; }
      }

      uploadRes.textContent = `Uploading ${unique.length} domains...`;

      const result = await uploadCsvDomains(unique, state.loginEmail, state.accessToken);
      const note = unique.length > 75 ? ` 75/day will be processed.` : "";
      uploadRes.textContent = `✅ ${result.inserted} added (${result.attempted - result.inserted} duplicates ignored).${note}`;
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
    const limit    = parseInt(document.getElementById("refresh-limit").value) || 75;

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
      resultEl.textContent = `✅ ${up.inserted} added (${domains.length - up.inserted} already queued). Railway processes 75/day/user.`;
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
    if (!confirm("Delete all processed entries (done/error/skipped) from the queue?")) return;
    await clearCsvQueue(state.accessToken, true);
    await refreshAll();
  });

  clearAll.addEventListener("click", async () => {
    if (!confirm("⚠️ Delete ALL entries from the CSV queue (including pending)?")) return;
    await clearCsvQueue(state.accessToken, false);
    await refreshStats();
  });
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

  if (meSession) {
    setAutopilotUI(btn, true);
    const remaining = Math.max(0, AUTOPILOT_DURATION_MS - sessionAgeMs);
    if (remaining > 0) startAutopilotCountdown(btn, remaining);
  } else {
    setAutopilotUI(btn, false);
    if (enabled) {
      // session expired or someone else's — flip OFF in DB
      await setAutopilotEnabled(false, state.accessToken);
      console.log("[Autopilot] Forced OFF on panel open — session expired or owned by another user");
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
    const isOn = btn.classList.contains("active");
    if (isOn) {
      // Apagar
      clearAutopilotTimer();
      setAutopilotUI(btn, false);
      await setAutopilotEnabled(false, state.accessToken);
    } else {
      // Mutex: si otro user tiene autopilot activo + heartbeat fresco, bloquear
      const cur = await getAutopilotState(state.accessToken);
      if (cur.enabled && cur.sessionUser
          && cur.sessionUser.toLowerCase() !== (state.loginEmail || "").toLowerCase()
          && cur.heartbeatAt && (Date.now() - cur.heartbeatAt.getTime()) < 120_000) {
        const elapsed = cur.sessionStart ? Math.round((Date.now() - cur.sessionStart.getTime()) / 60000) : 0;
        const remaining = Math.max(0, 60 - elapsed);
        alert(`⚠️ ${cur.sessionUser} ya tiene Autopilot corriendo (hace ${elapsed} min · termina en ~${remaining} min).\n\nEsperá a que termine o pedile que lo apague antes de prender el tuyo. Si lo prendés ahora, le robás la sesión y los créditos del día.`);
        return;
      }
      // Turn on for up to AUTOPILOT_DURATION_MS (default 60 min)
      setAutopilotUI(btn, true);
      await setAutopilotEnabled(true, state.accessToken, state.loginEmail);
      startAutopilotCountdown(btn, AUTOPILOT_DURATION_MS);
    }
  });

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

async function loadProspectsTab() {
  const listEl  = document.getElementById("prospects-list");
  const statsEl = document.getElementById("prospects-stats");
  if (!listEl) return;

  listEl.innerHTML = '<div class="cascade-empty">⏳ Loading...</div>';

  const dateFilter   = document.getElementById("prospects-date-filter")?.value   || "";
  const sourceFilter = document.getElementById("prospects-source-filter")?.value || "";
  let rows = [];
  let dailyCount = 0;
  try {
    [rows, dailyCount, _cachedProspectDrafts] = await Promise.all([
      fetchReviewQueue(state.accessToken, { dateFilter, sourceFilter }),
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
    listEl.innerHTML = '<div class="cascade-empty">No pending prospects. The auto-pilot will add candidates here.</div>';
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
  const trafficFmt  = r.traffic ? formatTraffic(r.traffic) : "N/A";
  // Filtrar garbage (whois, abuse, postmaster, etc.) y dedupe.
  // Backend ya guarda Apollo primero (apolloEmails antes que scraperEmails),
  // así que el orden del array preserva la prioridad Apollo.
  const emails      = (Array.isArray(r.emails) ? r.emails : [])
    .map(e => (e || "").trim())
    .filter(Boolean)
    .filter(e => !isGarbageEmail(e))
    .filter((e, i, arr) => arr.indexOf(e) === i);
  const hasEmail    = emails.length > 0;
  const owner       = defaultOwnerForLang(r.language);
  const status      = defaultStatusForOwner(owner);
  const langIdx     = LANG_TO_IDX[r.language] || "0";
  const langName    = LANG_NAMES_PRO[r.language] || r.language || "—";
  const adNetworks  = Array.isArray(r.ad_networks) ? r.ad_networks : [];
  const subjects    = Array.isArray(r.pitch_subjects) ? r.pitch_subjects : [];

  // Score badge color
  const score = r.score || 0;
  const scoreBg = score >= 60 ? "#16a34a" : score >= 35 ? "#d97706" : "#6b7280";
  const scoreBadge = score > 0
    ? `<span style="font-size:10px;font-weight:700;color:#fff;background:${scoreBg};border-radius:4px;padding:1px 5px;flex-shrink:0">${score}</span>`
    : "";

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
  <div class="pcard" data-id="${r.id}" data-source="${esc(r.source || 'autopilot')}" data-monday-id="${r.monday_item_id || ''}" style="background:var(--card-bg);border:1px solid var(--border);border-radius:8px;margin:0 8px 8px;overflow:hidden">

    <!-- Summary row -->
    <div style="display:flex;align-items:center;gap:6px;padding:8px 10px">
      <div style="flex:1;min-width:0">
        <div style="display:flex;align-items:center;gap:5px">
          ${(() => {
            const src = r.source || "autopilot";
            const badges = {
              autopilot:      ["🤖", "Auto",    "#6366f1"],
              csv:            ["📥", "CSV",     "#0ea5e9"],
              monday_refresh: ["🔄", "Refresh", "#f59e0b"],
            };
            const [icon, label, color] = badges[src] || badges.autopilot;
            return `<span title="Origen: ${label}" style="font-size:9px;font-weight:700;color:#fff;background:${color};border-radius:4px;padding:1px 5px;flex-shrink:0">${icon} ${label}</span>`;
          })()}
          <a class="pcard-domain-link" href="#" data-url="https://www.${esc(r.domain)}"
             style="font-weight:700;font-size:12px;color:var(--primary);text-decoration:none;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
            ${esc(r.domain)} ↗
          </a>
          ${scoreBadge}
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
            <option value="en">English</option>
            <option value="es">Spanish</option>
            <option value="it">Italian</option>
            <option value="pt">Portuguese</option>
            <option value="ar">Arabic</option>
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
            <span class="pcard-flag-emoji">🌐</span>
            <span class="pcard-flag-name pitch-tool-name">—</span>
          </button>
          <div class="pitch-toolbar-right">
            <div class="pcard-lang-flags pitch-lang-flags" title="Cambiar idioma del template"></div>
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
        <div style="font-size:12px;font-weight:600;padding:4px 0">${trafficFmt}</div>
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

  // Expand toggle
  card.querySelector(".pcard-expand-btn")?.addEventListener("click", () => {
    const panel = card.querySelector(".pcard-detail");
    const btn   = card.querySelector(".pcard-expand-btn");
    const open  = panel.style.display === "none";
    panel.style.display = open ? "block" : "none";
    btn.textContent     = open ? "▲" : "▼";
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
    if (!r.pitch || !r.pitch.trim()) {
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
  // Prioridad: 1) campo "Email" de Monday data (auto-completado pero editable)
  //            2) input manual
  //            3) chip seleccionado en la lista
  const monday  = card.querySelector(".pcard-email-monday")?.value?.trim();
  if (monday && monday.includes("@")) return monday;
  const manual  = card.querySelector(".pcard-email-manual")?.value?.trim();
  if (manual && manual.includes("@")) return manual;
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
  const traffic   = data.traffic ? formatTraffic(data.traffic) : "";

  // ── VALIDACIONES OBLIGATORIAS ────────────────────────────────
  // Misma lógica que Analysis (btn-push-monday). Si falta cualquier
  // dato esencial, NO se pushea a Monday.

  // 1. GEO
  if (!geo) {
    setResult("❌ GEO obligatorio. Completá el campo GEO antes de enviar.", false);
    card.querySelector(".pcard-geo")?.focus();
    return;
  }
  // 2. Tráfico (Páginas Vistas) > 0
  if (!data.traffic || data.traffic === 0) {
    setResult("❌ Páginas Vistas obligatorio. SimilarWeb no devolvió data — re-prospectá o editá la card.", false);
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

function initProspectsTab() {
  document.getElementById("btn-prospects-refresh")?.addEventListener("click", async () => {
    await loadProspectsTab();
  });
  document.getElementById("prospects-date-filter")?.addEventListener("change", async () => {
    await loadProspectsTab();
  });
  document.getElementById("prospects-source-filter")?.addEventListener("change", async () => {
    await loadProspectsTab();
  });
  document.getElementById("btn-prospects-clear")?.addEventListener("click", async () => {
    if (!confirm("Delete ALL your prospects (pending, rejected, validated, failed)?\n\nThis cannot be undone. Only your own items are affected.")) return;
    const btn = document.getElementById("btn-prospects-clear");
    btn.disabled = true; btn.textContent = "⏳ Deleting...";
    const r = await clearPendingProspects(state.accessToken, state.loginEmail);
    btn.disabled = false; btn.textContent = "🗑 Clear all";
    if (!r.ok) { alert("Delete failed: " + (r.error || "unknown")); return; }
    if (r.deleted === 0) {
      alert("No rows deleted.\n\nPossible causes:\n• Rows belong to other users (RLS blocks them)\n• Rows have no 'created_by' set and RLS won't allow deletion\n\nAsk an admin to clear them via service role if needed.");
    }
    await loadProspectsTab();
  });
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
