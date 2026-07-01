// ============================================================
// ADEQ Toolbar — API Proxy client
// Routes Gemini / Apollo / RapidAPI calls through Supabase Edge Function
// so the API keys never touch the extension bundle.
// ============================================================

import { CONFIG } from "../config.js";

let _sbAuthToken = null;
let _sbUserEmail = null;
export function setProxyAuth(accessToken, email) {
  _sbAuthToken = accessToken || null;
  if (email) _sbUserEmail = email.toLowerCase();
}

function proxyUrl() {
  return `${CONFIG.SUPABASE_URL}/functions/v1/api-proxy`;
}

// ─────────────────────────────────────────────────────────────────
// Hard cap MENSUAL de RapidAPI — protege contra overage.
// Default 40.000 (vs FREE de 500k) para tener margen amplio.
// Configurable en runtime via toolbar_config.rapidapi_monthly_limit.
// Compartido entre todos los MBs (counter en Supabase).
// ─────────────────────────────────────────────────────────────────
const DEFAULT_MONTHLY_LIMIT = 40000;

let _monthState = null;       // { used, limit, period, dirty: bool }
let _monthLastSync = 0;       // timestamp del último flush a Supabase
const FLUSH_EVERY_MS  = 30_000; // persiste como mucho cada 30s
const FLUSH_EVERY_HITS = 10;    // o cada 10 hits, lo que ocurra antes
let _hitsSinceFlush = 0;

// Período de facturación: ciclo del 6 al 6 (Maxi 2026-06-17). RapidAPI cobra
// del día 6 al día 6 del mes siguiente. Si hoy >= día 6 → período empieza este
// mes-06. Si hoy < día 6 → empezó el mes pasado-06. Formato "YYYY-MM-06" para
// que matchee con el worker (también compat con slice(0,7) legacy).
function currentPeriod() {
  const d = new Date();
  const isBeforeDay6 = d.getUTCDate() < 6;
  const month = isBeforeDay6 ? d.getUTCMonth() - 1 : d.getUTCMonth();
  const anchor = new Date(Date.UTC(d.getUTCFullYear(), month, 6));
  return anchor.toISOString().slice(0, 10); // "2026-06-06"
}

async function _readConfigKeys(keys) {
  if (!_sbAuthToken) return {};
  const url = `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=in.(${keys.join(",")})&select=key,value`;
  const res = await fetch(url, {
    headers: {
      "apikey":        CONFIG.SUPABASE_ANON_KEY,
      "Authorization": `Bearer ${_sbAuthToken}`,
    },
  });
  if (!res.ok) return {};
  const rows = await res.json();
  const map  = {};
  if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });
  return map;
}

async function _writeConfigKey(key, value) {
  if (!_sbAuthToken) return;
  const url = `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config?key=eq.${key}`;
  const headers = {
    "Content-Type":  "application/json",
    "apikey":        CONFIG.SUPABASE_ANON_KEY,
    "Authorization": `Bearer ${_sbAuthToken}`,
    "Prefer":        "resolution=merge-duplicates",
  };
  // Try update first
  const upd = await fetch(url, { method: "PATCH", headers, body: JSON.stringify({ value: String(value) }) });
  if (upd.ok) return;
  // Fallback: insert
  await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_config`, {
    method:  "POST",
    headers,
    body:    JSON.stringify({ key, value: String(value) }),
  }).catch(() => {});
}

async function loadMonthlyCounter() {
  const map = await _readConfigKeys([
    "rapidapi_calls_month",
    "rapidapi_calls_month_period",
    "rapidapi_monthly_limit",
  ]);
  const period       = currentPeriod();
  const storedPeriod = map.rapidapi_calls_month_period || "";
  // Compatibilidad con formato viejo (YYYY-MM-06) — comparamos por primeros 7 chars (YYYY-MM).
  // Así si la DB tiene "2026-05-06" y nosotros usamos "2026-05", se reconocen como mismo mes.
  const sameMonth    = storedPeriod.slice(0, 7) === period.slice(0, 7);
  const used         = sameMonth ? parseInt(map.rapidapi_calls_month || "0", 10) : 0;
  const limit        = parseInt(map.rapidapi_monthly_limit || String(DEFAULT_MONTHLY_LIMIT), 10);
  _monthState = { used, limit, period, dirty: false };
  return _monthState;
}

async function flushMonthlyCounter(force = false) {
  if (!_monthState || !_monthState.dirty) return;
  const now = Date.now();
  if (!force && (now - _monthLastSync < FLUSH_EVERY_MS) && _hitsSinceFlush < FLUSH_EVERY_HITS) return;
  _monthLastSync   = now;
  _hitsSinceFlush  = 0;
  _monthState.dirty = false;
  // Re-leer + sumar para no pisar lo que otro MB sumó en paralelo.
  // Compat con formato viejo: comparamos por YYYY-MM (slice 0,7).
  try {
    const map = await _readConfigKeys(["rapidapi_calls_month", "rapidapi_calls_month_period"]);
    const period   = currentPeriod();
    const sameMonth = (map.rapidapi_calls_month_period || "").slice(0, 7) === period.slice(0, 7);
    const stored   = sameMonth ? parseInt(map.rapidapi_calls_month || "0", 10) : 0;
    const newTotal = Math.max(stored, _monthState.used);
    await _writeConfigKey("rapidapi_calls_month", newTotal);
    await _writeConfigKey("rapidapi_calls_month_period", period);
    _monthState.used = newTotal;
  } catch (e) {
    console.warn("[apiProxy] flushMonthlyCounter failed:", e.message);
    _monthState.dirty = true; // reintentar después
  }
}

// Hook UI — popup.js le suscribe un callback para mostrar el banner.
let _onCapReachedCb = null;
export function onRapidApiCapReached(cb) { _onCapReachedCb = cb; }

// Hook adicional que dispara DESPUÉS de cada hit RapidAPI exitoso.
// Permite que el footer counter se refresque al instante en vez de esperar
// los 60s de polling. Decisión user 2026-05-08: monitoreo en tiempo real.
let _onRapidHitCb = null;
export function onRapidApiHit(cb) { _onRapidHitCb = cb; }

export async function getRapidApiMonthlyStatus() {
  if (!_monthState || _monthState.period !== currentPeriod()) await loadMonthlyCounter();
  return { ..._monthState };
}

function _capReached() {
  if (!_monthState) return false;
  return _monthState.used >= _monthState.limit;
}

// ─────────────────────────────────────────────────────────────────
// Cap personal por usuario (toolbar_user_limits.monthly_api_cap).
// Cuando el admin lo setea, el usuario no puede pasar de ese número
// aún si el cap global de 40K tiene espacio. Lee 1 vez al login y
// cachea (cambios del admin se reflejan al siguiente refresh).
// ─────────────────────────────────────────────────────────────────
let _userPersonalCap     = null;   // null = sin cap personal, usa solo el global
let _userPersonalUsed    = 0;
let _userCapLoadedFor    = null;
let _userCapLoadedPeriod = null;

async function loadUserPersonalCap() {
  if (!_sbAuthToken || !_sbUserEmail) return;
  const period = currentPeriod();
  if (_userCapLoadedFor === _sbUserEmail && _userCapLoadedPeriod === period) return;
  try {
    const limitRes = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_limits?user_email=eq.${encodeURIComponent(_sbUserEmail)}&select=monthly_api_cap&limit=1`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${_sbAuthToken}` } }
    );
    if (limitRes.ok) {
      const rows = await limitRes.json();
      _userPersonalCap = rows?.[0]?.monthly_api_cap ? parseInt(rows[0].monthly_api_cap, 10) : null;
    }
    // Sumar todos los hits de ESTE user en el mes corriente desde toolbar_api_usage.
    // period es "YYYY-MM" (mes calendario, decisión user 2026-05-12).
    // Le concatenamos "-01" para que PostgREST acepte el cast a date.
    const usageRes = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage?user_email=eq.${encodeURIComponent(_sbUserEmail)}&day=gte.${period}-01&select=by_provider`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${_sbAuthToken}` } }
    );
    if (usageRes.ok) {
      const rows = await usageRes.json();
      _userPersonalUsed = rows.reduce((acc, r) => acc + parseInt(r.by_provider?.rapidapi || 0, 10), 0);
    }
    _userCapLoadedFor    = _sbUserEmail;
    _userCapLoadedPeriod = period;
  } catch (e) { console.warn("[apiProxy] loadUserPersonalCap:", e.message); }
}

function _userCapReached() {
  if (_userPersonalCap == null) return false;
  return _userPersonalUsed >= _userPersonalCap;
}

/**
 * Call the Edge Function proxy.
 * @param {"gemini"|"apollo"|"rapidapi"} provider
 * @param {string} path - Upstream path (e.g. "/v1/mixed_people/api_search")
 * @param {object} opts - { method, headers, body, query }
 * @returns {Promise<{ ok, status, data, text, quota }>}
 */
// Retry config — SOLO 5xx (server errors NO cuentan en la factura del vendor).
// 429 NO se reintenta: cada retry de 429 es 1 request facturada por nada
// (RapidAPI cobra por hit HTTP, no por response útil). Si el usuario ve "No data"
// por 429, debe esperar — preferimos UX peor a factura de 400 USD por overage.
const MAX_RETRIES        = 2;
const BASE_BACKOFF_MS    = 800;
const RETRYABLE_STATUSES = new Set([500, 502, 503, 504]);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

export async function callProxy(provider, path, opts = {}) {
  if (!_sbAuthToken) return { ok: false, status: 401, error: "No auth token" };

  const { method = "GET", headers = {}, body = null, query = "" } = opts;
  const payload = JSON.stringify({ provider, path, method, headers, body, query });

  // ── Hard cap MENSUAL de RapidAPI ──────────────────────────
  // Pre-check: si ya pasamos el límite del mes, NO hacer la request.
  // Solo aplica a provider === "rapidapi" (Apollo/Gemini/Anthropic tienen sus propios contadores).
  if (provider === "rapidapi") {
    if (!_monthState || _monthState.period !== currentPeriod()) {
      await loadMonthlyCounter();
    }
    // Cap personal del usuario tiene prioridad sobre el global.
    await loadUserPersonalCap();
    if (_userCapReached()) {
      console.warn(`[apiProxy] ⛔ Cap PERSONAL del usuario alcanzado: ${_userPersonalUsed}/${_userPersonalCap}`);
      try { _onCapReachedCb?.({ used: _userPersonalUsed, limit: _userPersonalCap, period: _monthState?.period, scope: "user" }); } catch {}
      return {
        ok:     false,
        status: 429,
        data:   null,
        text:   "user_monthly_cap_reached",
        error:  "user_monthly_cap_reached",
        capReached: true,
      };
    }
    if (_capReached()) {
      console.warn(`[apiProxy] ⛔ RapidAPI cap mensual GLOBAL alcanzado: ${_monthState.used}/${_monthState.limit}`);
      try { _onCapReachedCb?.({ used: _monthState.used, limit: _monthState.limit, period: _monthState.period, scope: "global" }); } catch {}
      return {
        ok:     false,
        status: 429,
        data:   null,
        text:   "monthly_cap_reached",
        error:  "monthly_cap_reached",
        capReached: true,
      };
    }
  }

  let attempt = 0;
  let lastRes = null;

  while (attempt <= MAX_RETRIES) {
    const res = await fetch(proxyUrl(), {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "Authorization": `Bearer ${_sbAuthToken}`,
        "apikey":        CONFIG.SUPABASE_ANON_KEY,
      },
      body: payload,
    });

    if (!RETRYABLE_STATUSES.has(res.status) || attempt === MAX_RETRIES) {
      const text = await res.text();
      let data   = null;
      try { data = JSON.parse(text); } catch {}

      // Incrementar contador mensual (rapidapi y apollo cuentan separados).
      // Bump atomic via RPC bump_api_counter — funciona desde popup/agente/worker.
      const inc = 1 + attempt;
      // BUG FIX 2026-06-17: Apollo solo cobra crédito por /v1/people/match
      // (el unlock real). /v1/mixed_people/api_search es GRATIS. Antes
      // contábamos TODO como crédito → contador inflado 2000+ y Apollo
      // dashboard mostraba 0 reales. Ahora solo bumpamos cuando es unlock.
      const isPaidApolloCall = provider === "apollo" && /\/v1\/people\/match/.test(path);
      const shouldBumpCounter = provider === "rapidapi" || isPaidApolloCall;
      if (shouldBumpCounter && res.ok) {
        // RPC en background, no bloquea. Solo bumpa en respuestas exitosas:
        // Apollo no cobra créditos por errores HTTP 4xx/5xx en /people/match.
        fetch(`${CONFIG.SUPABASE_URL}/rest/v1/rpc/bump_api_counter`, {
          method: "POST",
          headers: {
            "apikey": CONFIG.SUPABASE_ANON_KEY,
            "Authorization": `Bearer ${_sbAuthToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ provider, n: inc }),
        }).catch(() => {});
      }
      // El resto solo aplica a rapidapi (UI cap banner, etc.)
      // Maxi 2026-07-01: gatear en res.ok — antes el contador LOCAL subía aunque la
      // llamada fallara (mientras el RPC de persistencia SÍ estaba gateado en res.ok),
      // así que el local se adelantaba y disparaba el banner de cap antes de tiempo.
      if (provider === "rapidapi" && _monthState && res.ok) {
        _monthState.used    += inc;
        _userPersonalUsed   += inc;
        _monthState.dirty    = true;
        _hitsSinceFlush     += inc;
        flushMonthlyCounter().catch(() => {});
        try { _onRapidHitCb?.({ used: _monthState.used, limit: _monthState.limit, period: _monthState.period }); } catch {}
        if (_userCapReached()) {
          try { _onCapReachedCb?.({ used: _userPersonalUsed, limit: _userPersonalCap, period: _monthState.period, scope: "user" }); } catch {}
        } else if (_capReached()) {
          try { _onCapReachedCb?.({ used: _monthState.used, limit: _monthState.limit, period: _monthState.period, scope: "global" }); } catch {}
        }
      }

      return {
        ok:     res.ok,
        status: res.status,
        data,
        text,
        quota: {
          remaining:         parseInt(res.headers.get("X-Quota-Remaining") || "0"),
          providerRemaining: parseInt(res.headers.get("X-Provider-Remaining") || "0"),
          monthlyUsed:       provider === "rapidapi" ? _monthState?.used : undefined,
          monthlyLimit:      provider === "rapidapi" ? _monthState?.limit : undefined,
        },
        retried: attempt > 0 ? attempt : undefined,
      };
    }

    // Respetar Retry-After del server si viene; sino backoff exponencial con jitter.
    const retryAfterHdr = parseInt(res.headers.get("Retry-After") || "0");
    const backoff       = retryAfterHdr > 0
      ? retryAfterHdr * 1000
      : BASE_BACKOFF_MS * Math.pow(2, attempt) + Math.random() * 250;

    console.warn(`[apiProxy] ${provider}${path} → HTTP ${res.status}, retry ${attempt + 1}/${MAX_RETRIES} en ${Math.round(backoff)}ms`);
    lastRes = res;
    await res.body?.cancel?.().catch(() => {}); // liberar el stream
    await sleep(backoff);
    attempt++;
  }

  // Inalcanzable (el loop retorna en la última iteración) pero por seguridad:
  return { ok: false, status: lastRes?.status || 0, data: null, text: "" };
}
