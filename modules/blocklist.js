// ============================================================
// ADEQ Toolbar — URL Blocklist
// Combina:
//   1. Hardcoded blocklist (TLDs y dominios gigantes que jamás van a ser prospects)
//   2. Admin blocklist (tabla toolbar_url_blocklist en Supabase)
// Pre-check antes de gastar API en cualquier dominio.
// ============================================================

import { CONFIG } from "../config.js";
import { TOP_500_BLOCKED } from "./blockedDomainsTop500.js";

// ── 1. TLDs siempre bloqueados ──────────────────────────────
const BLOCKED_TLDS = new Set([
  "gov", "mil", "edu",
  "gob.ar", "gob.mx", "gob.es", "gov.uk", "gov.br", "gov.au", "gov.in",
  "edu.ar", "edu.mx", "edu.br", "edu.au",
]);

// ── 1b. Categorías auto-bloqueadas (pre-check sobre el dominio antes de gastar API) ─
// Si el dominio matchea uno de estos patrones de subdominio/path típicos, lo bloqueamos
// porque suelen ser sitios institucionales / no-publishers que no convierten en deals.
const BLOCKED_PATTERNS = [
  /^(www\.)?(login|admin|portal|intranet|sso|hr|recruiting)\./,
  /(insurance|seguro|salud|hospital|clinic[ao]|medical|farmacia|pharma)\./,
  /^bank|banco/i,
];

// ── 2. Dominios gigantes / corporativos donde jamás vamos a vender ─
// Combina TOP_500_BLOCKED (lista grande pre-cargada de sitios no-publishers)
// + ADEQ-specific entries que no están en el top 500.
const BLOCKED_DOMAINS = new Set([
  ...TOP_500_BLOCKED,
  // Extras de ADEQ
  "x.com", "tiktok.com", "snapchat.com", "openai.com",
  "anthropic.com", "claude.ai", "chatgpt.com", "gemini.google.com",
  "aws.amazon.com", "cloud.google.com", "azure.microsoft.com",
  "stripe.com", "notion.so", "atlassian.com", "monday.com",
  "adeqmedia.com",
  // WHOIS-privacy proxies — nunca son contactos reales, scrapearlos siempre da bounces
  "protecteddomainservices.com", "domainsbyproxy.com", "whoisguard.com",
  "whoisprivacyprotect.com", "whoisprotectservice.com", "contactprivacy.com",
  "privacyguardian.org", "anonymize.com", "withheldforprivacy.com",
  "perfectprivacy.com", "privacy-link.com", "namecheap.com",
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

  // 3b. Patrones por categoría (insurance/banking/medical/portales corporativos)
  for (const re of BLOCKED_PATTERNS) {
    if (re.test(d)) return { blocked: true, reason: "Categoría auto-bloqueada (no-publisher)" };
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
