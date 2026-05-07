// ============================================================
// ADEQ Toolbar — URL Blocklist
// Combina:
//   1. Hardcoded blocklist (TLDs y dominios gigantes que jamás van a ser prospects)
//   2. Admin blocklist (tabla toolbar_url_blocklist en Supabase)
// Pre-check antes de gastar API en cualquier dominio.
// ============================================================

import { CONFIG } from "../config.js";

// ── 1. TLDs siempre bloqueados ──────────────────────────────
const BLOCKED_TLDS = new Set([
  "gov", "mil", "edu",
  "gob.ar", "gob.mx", "gob.es", "gov.uk", "gov.br", "gov.au", "gov.in",
  "edu.ar", "edu.mx", "edu.br", "edu.au",
]);

// ── 2. Dominios gigantes / corporativos donde jamás vamos a vender ─
const BLOCKED_DOMAINS = new Set([
  // Tech giants
  "google.com", "facebook.com", "youtube.com", "instagram.com", "twitter.com",
  "x.com", "linkedin.com", "amazon.com", "microsoft.com", "apple.com",
  "netflix.com", "tiktok.com", "snapchat.com", "pinterest.com", "reddit.com",
  "wikipedia.org", "github.com", "stackoverflow.com", "openai.com",
  "anthropic.com", "claude.ai", "chatgpt.com", "gemini.google.com",
  // Cloud / SaaS
  "aws.amazon.com", "cloud.google.com", "azure.microsoft.com",
  "salesforce.com", "hubspot.com", "stripe.com", "shopify.com",
  "notion.so", "slack.com", "zoom.us", "dropbox.com",
  // Plataformas que no son publishers
  "monday.com", "atlassian.com", "trello.com", "asana.com",
  // Search engines / portales
  "bing.com", "yahoo.com", "duckduckgo.com", "baidu.com", "yandex.ru",
  // Sites internos de ADEQ / herramientas
  "adeqmedia.com",
]);

// Cache en memoria de la blocklist admin (refresca cada 5 min)
let _adminBlocklistCache = null;
let _adminBlocklistFetchedAt = 0;
const ADMIN_CACHE_TTL = 5 * 60 * 1000;

async function fetchAdminBlocklist(accessToken) {
  if (!accessToken) return new Set();
  const now = Date.now();
  if (_adminBlocklistCache && (now - _adminBlocklistFetchedAt) < ADMIN_CACHE_TTL) {
    return _adminBlocklistCache;
  }
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_url_blocklist?select=domain`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return _adminBlocklistCache || new Set();
    const rows = await res.json();
    _adminBlocklistCache = new Set((rows || []).map(r => (r.domain || "").toLowerCase()));
    _adminBlocklistFetchedAt = now;
    return _adminBlocklistCache;
  } catch {
    return _adminBlocklistCache || new Set();
  }
}

function cleanDomain(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/.*$/, "")
    .trim();
}

// Devuelve { blocked: bool, reason: string } sin gastar API.
export async function checkDomainBlocked(domain, accessToken) {
  const d = cleanDomain(domain);
  if (!d || !d.includes(".")) return { blocked: true, reason: "Dominio inválido" };

  // 1. TLD blocked
  for (const tld of BLOCKED_TLDS) {
    if (d.endsWith("." + tld)) return { blocked: true, reason: `TLD bloqueado (${tld})` };
  }

  // 2. Dominios gigantes hardcoded
  if (BLOCKED_DOMAINS.has(d)) return { blocked: true, reason: "Dominio gigante / no-prospect" };

  // 3. Subdominio de un dominio gigante
  for (const big of BLOCKED_DOMAINS) {
    if (d.endsWith("." + big)) return { blocked: true, reason: `Subdominio de ${big}` };
  }

  // 4. Admin blocklist (Supabase)
  const adminList = await fetchAdminBlocklist(accessToken);
  if (adminList.has(d)) return { blocked: true, reason: "Bloqueado por admin" };
  for (const blocked of adminList) {
    if (d.endsWith("." + blocked)) return { blocked: true, reason: `Subdominio bloqueado (${blocked})` };
  }

  return { blocked: false };
}

// Para invalidar el cache cuando admin actualiza la lista
export function invalidateBlocklistCache() {
  _adminBlocklistCache = null;
  _adminBlocklistFetchedAt = 0;
}
