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

function currentPeriod() {
  return new Date().toISOString().slice(0, 7); // YYYY-MM
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
  const used         = storedPeriod === period ? parseInt(map.rapidapi_calls_month || "0", 10) : 0;
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
  // Re-leer + sumar para no pisar lo que otro MB sumó en paralelo
  try {
    const map = await _readConfigKeys(["rapidapi_calls_month", "rapidapi_calls_month_period"]);
    const period   = currentPeriod();
    const stored   = map.rapidapi_calls_month_period === period ? parseInt(map.rapidapi_calls_month || "0", 10) : 0;
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

export async function getRapidApiMonthlyStatus() {
  if (!_monthState || _monthState.period !== currentPeriod()) await loadMonthlyCounter();
  return { ..._monthState };
}

function _capReached() {
  if (!_monthState) return false;
  return _monthState.used >= _monthState.limit;
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
    if (_capReached()) {
      console.warn(`[apiProxy] ⛔ RapidAPI cap mensual alcanzado: ${_monthState.used}/${_monthState.limit}`);
      try { _onCapReachedCb?.({ used: _monthState.used, limit: _monthState.limit, period: _monthState.period }); } catch {}
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

      // Incrementar contador mensual SOLO para rapidapi (cualquier status, sí o sí
      // es 1 hit facturable — incluso 4xx/5xx cuentan en RapidAPI billing).
      if (provider === "rapidapi" && _monthState) {
        _monthState.used += 1 + attempt; // sumar también los retries de 5xx que se ejecutaron
        _monthState.dirty = true;
        _hitsSinceFlush  += 1 + attempt;
        // Persistir async, sin bloquear
        flushMonthlyCounter().catch(() => {});
        // Si justo cruzamos el límite con esta call, alertar UI
        if (_capReached()) {
          try { _onCapReachedCb?.({ used: _monthState.used, limit: _monthState.limit, period: _monthState.period }); } catch {}
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
