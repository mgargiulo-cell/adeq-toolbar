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
// Política user 2026-05-26: rechazar gov / edu / mil / academic / int en TODOS sus variantes.
// No-publishers por definición, jamás van a comprar advertising.
const BLOCKED_TLDS = new Set([
  // Gobierno
  "gov", "mil", "int",
  "gob.ar", "gob.mx", "gob.es", "gob.pe", "gob.cl", "gob.ve", "gob.do", "gob.ec", "gob.gt", "gob.hn", "gob.pa", "gob.sv", "gob.bo",
  "gov.uk", "gov.br", "gov.au", "gov.in", "gov.co", "gov.za", "gov.ca", "gov.ph", "gov.sg", "gov.it",
  "gouv.fr",
  // Educación / academic
  "edu", "ac",
  "edu.ar", "edu.mx", "edu.br", "edu.au", "edu.co", "edu.pe", "edu.cl", "edu.ve", "edu.do", "edu.ec", "edu.gt", "edu.uy",
  "edu.es", "edu.it", "edu.in", "edu.sg", "edu.ph", "edu.my", "edu.pk", "edu.bd", "edu.tw",
  "ac.uk", "ac.jp", "ac.kr", "ac.in", "ac.za", "ac.nz", "ac.il", "ac.cn", "ac.id", "ac.th",
  // Militar
  "mil.ar", "mil.br", "mil.co", "mil.uk", "mil.es", "mil.in",
]);

// ── 1b. Patrones de DOMINIO auto-bloqueados (sin gastar API) ───
// Mata sitios corporativos, marcas, instituciones cuyo nombre los delata.
// Política user 2026-05-26: agresivo — preferimos perder algún publisher con
// nombre raro a llenar el pool de marcas que jamás van a pautar.
const BLOCKED_PATTERNS = [
  // Subdominios técnicos / corporativos
  /^(www\.)?(login|admin|portal|intranet|sso|hr|recruiting|careers|investor|investors|jobs|developer|developers|api|status|docs|support|help|cdn|static|cms|wiki|vendor|partner|partners|mail|webmail)\./,
  // Marcas / industrias regulated (insurance, bank, pharma, healthcare)
  /(insurance|seguro|salud|hospital|clinic[ao]|medical|farmacia|pharma|pharmaceutic|aseguradora|aseguranca|aseguros)\./,
  /^(bank|banco|banque|banca)\b/i,
  /\b(bank|banco|banque|banca)\./i,
  // Corporativos / holdings / grupos
  /\b(corp|inc|holding|holdings|group|grupo|industries|industria|industrias|company|cia|sa|srl|sas|gmbh|ltd|llc|plc|enterprises?|consult(ing|oria))\.(com|net|org|biz|info|co)/i,
  // Sufijos corporativos en la base del dominio
  /-(corp|inc|holding|holdings|group|grupo|industries|company|enterprises?)\.(com|net|org|biz|info|co)/i,
  // Auto / fabricantes / energy / telecom (marcas)
  /^(autos?|motor|motors|automotive|automotriz|vehiculo|vehiculos)\./i,
  /^(energi[ae]|energy|petro|petrol|oil|gas|electric|electrico|nuclear|utilit(y|ies))\./i,
  /^(telecom|telefon|movistar|claro|tigo|entel|att|verizon|vodafone|orange|deutsche|telekom)\./i,
  // Real estate / inmobiliarias / construction
  /\b(realtor|realestate|inmobil|imobil|construct|construccion|construcao|properties|propiedades|propiedad|propriedade)\./i,
  // Manufacturing / industrial / B2B vendors
  /\b(manufactur|industria|industrial|fabrica|factory|plant|planta|wholesale|mayorista)\./i,
  // Travel ops / airlines / hotels (marcas, no publishers de turismo)
  /^(airlines?|aerolinea|aeroporto|airport|airways|hotels?|hoteles)\./i,
  // Retail chains
  /^(walmart|carrefour|tesco|costco|amazon|ebay|mercadolibre|aliexpress|temu|shein)\./i,
  // Government keywords (refuerzo además del TLD)
  /^(www\.)?(gobierno|gobiern|government|ministerio|ministry|alcaldia|municipio|municipal|ayuntamiento|provincia|congreso|senado|parlamento|parliament)\./i,
  // Universidades por nombre (no solo por TLD)
  /^(www\.)?(universidad|universit|university|college|colegio|escuela|school|instituto|facultad|faculty)\./i,
  // ONG / fundaciones (donantes, no compradores)
  /^(www\.)?(ong|ngo|fundacion|fundacao|foundation|charity|caridad)\./i,
];

// ── 1c. Categorías SimilarWeb auto-bloqueadas ─────────────────
// La API devuelve `category` (string tipo "Banking, Credit, and Lending" o
// "Computers Electronics and Technology > Computer Hardware"). Si matchea
// una de estas keywords (case-insensitive), rechazamos. Política user
// 2026-05-26: sitios de marcas/instituciones que NO compran display nunca.
const BLOCKED_CATEGORY_KEYWORDS = [
  // Servicios financieros (proveedores, no media)
  "banking", "credit and lending", "credit cards", "insurance",
  "investing", "stock trading", "asset management",
  // Gobierno
  "government", "law and government", "public administration", "military",
  // Educación
  "universities", "higher education", "school", "academic", "career and education > school",
  // Tech vendors (marcas, no publishers)
  "computer hardware", "computer security", "consumer electronics > brands",
  "telecommunications", "isps", "web hosting", "data center",
  "software > b2b", "saas", "enterprise software", "cloud computing",
  // Fabricantes
  "vehicles > manufacturer", "automotive > manufacturer", "auto parts",
  "consumer goods > manufacturer", "industry", "agriculture", "logistics",
  "manufacturing", "energy and utilities", "oil and gas",
  // Servicios B2B
  "business and consumer services > consulting",
  "advertising and marketing > agencies",
  "marketing and advertising",
  // Pharma
  "pharmaceuticals", "biotechnology", "medical devices",
  // Retail brands
  "ecommerce", "shopping", "marketplace",
  // Travel ops (no media)
  "air travel", "hotels and accommodations", "car rentals", "cruises",
  // Real estate
  "real estate", "property listings",
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

// Chequea si la categoría de SimilarWeb está en la blocklist.
// Se usa DESPUÉS de obtener traffic data (no pre-API). Devuelve
// { blocked, reason } igual que checkDomainBlocked.
export function isCategoryBlocked(category) {
  if (!category) return { blocked: false };
  const cat = String(category).toLowerCase();
  for (const kw of BLOCKED_CATEGORY_KEYWORDS) {
    if (cat.includes(kw)) {
      return { blocked: true, reason: `Categoría auto-bloqueada: "${category}" matchea "${kw}"` };
    }
  }
  return { blocked: false };
}
