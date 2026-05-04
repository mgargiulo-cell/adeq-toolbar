// ============================================================
// ADEQ Toolbar — API Proxy client
// Routes Gemini / Apollo / RapidAPI calls through Supabase Edge Function
// so the API keys never touch the extension bundle.
// ============================================================

import { CONFIG } from "../config.js";

let _sbAuthToken = null;
export function setProxyAuth(accessToken) { _sbAuthToken = accessToken || null; }

function proxyUrl() {
  return `${CONFIG.SUPABASE_URL}/functions/v1/api-proxy`;
}

/**
 * Call the Edge Function proxy.
 * @param {"gemini"|"apollo"|"rapidapi"} provider
 * @param {string} path - Upstream path (e.g. "/v1/mixed_people/api_search")
 * @param {object} opts - { method, headers, body, query }
 * @returns {Promise<{ ok, status, data, text, quota }>}
 */
// Retry config — comportamiento humano de "abrir 50 tabs y mirar todas" puede
// disparar 429 del proxy/vendor. En vez de tirar error al usuario, hacemos
// backoff exponencial transparente. Solo aplica a 429 (rate limit) y 5xx (server).
const MAX_RETRIES        = 4;
const BASE_BACKOFF_MS    = 600;   // 600 → 1200 → 2400 → 4800 (~9s total worst case)
const RETRYABLE_STATUSES = new Set([429, 500, 502, 503, 504]);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

export async function callProxy(provider, path, opts = {}) {
  if (!_sbAuthToken) return { ok: false, status: 401, error: "No auth token" };

  const { method = "GET", headers = {}, body = null, query = "" } = opts;
  const payload = JSON.stringify({ provider, path, method, headers, body, query });

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
      return {
        ok:     res.ok,
        status: res.status,
        data,
        text,
        quota: {
          remaining:         parseInt(res.headers.get("X-Quota-Remaining") || "0"),
          providerRemaining: parseInt(res.headers.get("X-Provider-Remaining") || "0"),
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
