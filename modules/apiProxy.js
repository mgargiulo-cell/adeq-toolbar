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
export async function callProxy(provider, path, opts = {}) {
  if (!_sbAuthToken) return { ok: false, status: 401, error: "No auth token" };

  const { method = "GET", headers = {}, body = null, query = "" } = opts;

  const res = await fetch(proxyUrl(), {
    method: "POST",
    headers: {
      "Content-Type":  "application/json",
      "Authorization": `Bearer ${_sbAuthToken}`,
      "apikey":        CONFIG.SUPABASE_ANON_KEY,
    },
    body: JSON.stringify({ provider, path, method, headers, body, query }),
  });

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
  };
}
