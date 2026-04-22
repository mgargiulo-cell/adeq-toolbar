// ============================================================
// ADEQ Toolbar — Edge Function api-proxy
// Proxies Gemini / Apollo / RapidAPI calls with:
//   - JWT validation (only authenticated toolbar users)
//   - Per-user per-day quota enforcement
//   - Keys stored as Supabase secrets — never exposed to client
//
// Deploy:
//   supabase functions deploy api-proxy --no-verify-jwt
//   supabase secrets set GEMINI_API_KEY=... APOLLO_API_KEY=... RAPIDAPI_KEY=...
//
// Client calls:
//   POST ${SUPABASE_URL}/functions/v1/api-proxy
//   Headers: Authorization: Bearer <user-jwt>, Content-Type: application/json
//   Body: { provider: "gemini"|"apollo"|"rapidapi", path: "...", method, headers?, body? }
// ============================================================

// @ts-nocheck
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";

// ── Per-user daily quota (total calls across providers) ──────
const DAILY_QUOTA_PER_USER = 500;
// Per-provider per-user daily caps
const PROVIDER_CAPS = {
  gemini:    300,
  apollo:    150,
  rapidapi:  400,
  anthropic: 200,
  voyage:    300,
};

const PROVIDERS = {
  gemini: {
    base: "https://generativelanguage.googleapis.com",
    authMode: "query",                // ?key=<KEY>
    keyEnv: "GEMINI_API_KEY",
    allow: /^\/v1beta\/models\/[a-zA-Z0-9._-]+:(generateContent|streamGenerateContent)$/,
  },
  apollo: {
    base: "https://api.apollo.io",
    authMode: "header-x-api-key",     // x-api-key: <KEY>
    keyEnv: "APOLLO_API_KEY",
    allow: /^\/v1\/(mixed_people\/api_search|people\/match|organizations\/enrich)$/,
  },
  rapidapi: {
    base: "https://similarweb-insights.p.rapidapi.com",
    authMode: "header-rapidapi",
    keyEnv: "RAPIDAPI_KEY",
    hostHeader: "similarweb-insights.p.rapidapi.com",
    allow: /^\/(traffic|engagement|countries|similar|category|description|keywords|general|website-analysis)\b/,
  },
  anthropic: {
    base: "https://api.anthropic.com",
    authMode: "header-anthropic",     // x-api-key + anthropic-version
    keyEnv: "ANTHROPIC_API_KEY",
    apiVersion: "2023-06-01",
    allow: /^\/v1\/messages$/,
  },
  voyage: {
    base: "https://api.voyageai.com",
    authMode: "header-bearer",        // Authorization: Bearer <KEY>
    keyEnv: "VOYAGE_API_KEY",
    allow: /^\/v1\/embeddings$/,
  },
};

const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function json(status: number, data: any) {
  return new Response(JSON.stringify(data), {
    status, headers: { ...CORS, "Content-Type": "application/json" },
  });
}

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  if (req.method !== "POST")    return json(405, { error: "Method not allowed" });

  // ── Validate user JWT ────────────────────────────────────────
  const authHeader = req.headers.get("authorization") || "";
  const jwt = authHeader.replace(/^Bearer\s+/i, "");
  if (!jwt) return json(401, { error: "Missing bearer token" });

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const serviceKey  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const supabase    = createClient(supabaseUrl, serviceKey);

  const { data: userData, error: userErr } = await supabase.auth.getUser(jwt);
  if (userErr || !userData?.user?.email) return json(401, { error: "Invalid token" });
  const userEmail = userData.user.email.toLowerCase();

  // ── Parse body ──────────────────────────────────────────────
  let payload: any;
  try { payload = await req.json(); } catch { return json(400, { error: "Invalid JSON" }); }
  const { provider, path, method = "GET", headers: extraHeaders = {}, body = null, query = "" } = payload || {};

  const cfg = PROVIDERS[provider];
  if (!cfg)               return json(400, { error: "Unknown provider" });
  if (!cfg.allow.test(path || "")) return json(403, { error: "Path not allowed", path });

  // ── Quota check ─────────────────────────────────────────────
  const today = new Date().toISOString().slice(0, 10);
  const { data: quotaRow } = await supabase
    .from("toolbar_api_usage")
    .select("total,by_provider")
    .eq("user_email", userEmail).eq("day", today).maybeSingle();

  const total   = quotaRow?.total || 0;
  const byProv  = quotaRow?.by_provider || {};
  const provCnt = byProv[provider] || 0;

  if (total >= DAILY_QUOTA_PER_USER)
    return json(429, { error: "Daily total quota exceeded", limit: DAILY_QUOTA_PER_USER, used: total });
  if (provCnt >= PROVIDER_CAPS[provider])
    return json(429, { error: `Daily ${provider} quota exceeded`, limit: PROVIDER_CAPS[provider], used: provCnt });

  // ── Build upstream request ──────────────────────────────────
  const keyVal = Deno.env.get(cfg.keyEnv);
  if (!keyVal) return json(500, { error: `Missing ${cfg.keyEnv} secret` });

  let url = cfg.base + path;
  if (query) url += (url.includes("?") ? "&" : "?") + query;

  const upstreamHeaders: Record<string,string> = {
    "Content-Type": "application/json",
    ...extraHeaders,
  };

  if (cfg.authMode === "query") {
    url += (url.includes("?") ? "&" : "?") + "key=" + encodeURIComponent(keyVal);
  } else if (cfg.authMode === "header-x-api-key") {
    upstreamHeaders["X-Api-Key"] = keyVal;
  } else if (cfg.authMode === "header-rapidapi") {
    upstreamHeaders["x-rapidapi-key"]  = keyVal;
    upstreamHeaders["x-rapidapi-host"] = cfg.hostHeader;
  } else if (cfg.authMode === "header-anthropic") {
    upstreamHeaders["x-api-key"]          = keyVal;
    upstreamHeaders["anthropic-version"]  = cfg.apiVersion || "2023-06-01";
  } else if (cfg.authMode === "header-bearer") {
    upstreamHeaders["Authorization"]      = `Bearer ${keyVal}`;
  }

  // ── Fetch upstream ──────────────────────────────────────────
  let upstreamRes: Response;
  try {
    upstreamRes = await fetch(url, {
      method,
      headers: upstreamHeaders,
      body: body != null && method !== "GET" && method !== "HEAD" ? JSON.stringify(body) : undefined,
    });
  } catch (e) {
    return json(502, { error: "Upstream fetch failed", detail: String(e) });
  }

  const upstreamBody = await upstreamRes.text();

  // ── Record usage (fire-and-forget style) ────────────────────
  const newByProv = { ...byProv, [provider]: provCnt + 1 };
  await supabase.from("toolbar_api_usage").upsert({
    user_email: userEmail,
    day: today,
    total: total + 1,
    by_provider: newByProv,
    updated_at: new Date().toISOString(),
  }, { onConflict: "user_email,day" });

  return new Response(upstreamBody, {
    status: upstreamRes.status,
    headers: {
      ...CORS,
      "Content-Type":     upstreamRes.headers.get("content-type") || "application/json",
      "X-Quota-Remaining": String(DAILY_QUOTA_PER_USER - (total + 1)),
      "X-Provider-Remaining": String(PROVIDER_CAPS[provider] - (provCnt + 1)),
    },
  });
});
