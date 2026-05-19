// ============================================================
// ADEQ AUTO-PROSPECTOR — v3
// Cambios v3:
//   - Fuente de dominios: Majestic Million (1M sitios rankeados del mundo)
//   - Los dominios de Monday se usan como EXCLUSIÓN (ya son clientes)
//   - Pool de dominios se descarga una vez al iniciar Railway (en memoria)
// Deploy: Railway
// ============================================================

import fetch from "node-fetch";
import { pickRandomTemplate, fillTemplate, pickPitchSource, getSenderName, getBakedTemplates } from "./templates.js";

const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY         = process.env.SUPABASE_ANON_KEY;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // bypass RLS (backend worker)
const SUPABASE_EMAIL            = process.env.SUPABASE_EMAIL;
const SUPABASE_PASSWORD         = process.env.SUPABASE_PASSWORD;
const CLOUDFLARE_API_TOKEN      = process.env.CLOUDFLARE_API_TOKEN || null; // optional: Radar country-indexed pool

// If a service-role key is set, use it as Bearer — bypasses RLS.
// Otherwise fall back to user JWT (won't see items uploaded by other users with RLS on).
const BACKEND_BEARER = SUPABASE_SERVICE_ROLE_KEY || null;

const SESSION_LIMIT_MS  = 20 * 60 * 1000; // 20 minutos máx por sesión de autopilot — auto-corte
const POLL_INTERVAL_MS  = 20 * 1000;   // durante sesión activa
const IDLE_INTERVAL_MS  = 120 * 1000;  // cuando autopilot está OFF (2 min)
const IDLE_EXIT_MS      = 4 * 60 * 60 * 1000; // 4h sin trabajo → exit (subido de 30min para evitar restarts frecuentes 2026-05-13)
const DOMAIN_DELAY_MS  = 2500;
const MIN_TRAFFIC      = 400_000;  // pageViews mínimos para AUTOPILOT Majestic (descubrimiento)
const REVIEW_QUEUE_MIN_TRAFFIC = 400_000; // Floor absoluto en review_queue. Items debajo se auto-borran (no acumulan basura).

// Fuente de dominios públicos rankeados (Majestic Million — top 1M sitios)
const MAJESTIC_URL = "https://downloads.majesticseo.com/majestic_million.csv";

// Dominios de tech/redes sociales/marcas globales — no son publishers
const EXCLUDE_DOMAINS = new Set([
  // Search & tech
  "google.com","google.co.uk","google.com.br","google.es","google.de","google.com.mx",
  "google.co.jp","google.fr","google.it","google.com.ar",
  "youtube.com","gmail.com","googletagmanager.com","googleapis.com",
  "bing.com","duckduckgo.com","baidu.com","yandex.ru","yandex.com","naver.com","yahoo.com",
  "msn.com","bing.com","ask.com","aol.com",
  // Social
  "facebook.com","instagram.com","twitter.com","x.com","threads.net",
  "tiktok.com","snapchat.com","pinterest.com","linkedin.com","whatsapp.com",
  "reddit.com","tumblr.com","quora.com","vk.com","ok.ru",
  "discord.com","telegram.org","signal.org","wechat.com","line.me",
  // Video/streaming
  "netflix.com","spotify.com","twitch.tv","vimeo.com","dailymotion.com",
  "hulu.com","disneyplus.com","primevideo.com","peacocktv.com","hbomax.com",
  // E-commerce/retail
  "amazon.com","amazon.co.uk","amazon.de","amazon.es","amazon.com.br","amazon.fr",
  "ebay.com","ebay.co.uk","ebay.de","aliexpress.com","alibaba.com","taobao.com",
  "mercadolibre.com","shopify.com","etsy.com","wish.com","rakuten.com",
  "walmart.com","target.com","costco.com","bestbuy.com","homedepot.com",
  // Finance
  "paypal.com","stripe.com","payoneer.com","wise.com","revolut.com",
  "chase.com","bankofamerica.com","wellsfargo.com","citibank.com","hsbc.com",
  "visa.com","mastercard.com","americanexpress.com",
  // Tech/software
  "apple.com","microsoft.com","windows.com","office.com","live.com","outlook.com",
  "zoom.us","slack.com","dropbox.com","github.com","gitlab.com","stackoverflow.com",
  "cloudflare.com","amazonaws.com","azure.microsoft.com","cloud.google.com",
  "oracle.com","sap.com","salesforce.com","hubspot.com","zendesk.com",
  "adobe.com","canva.com","figma.com","notion.so","atlassian.com","jira.com",
  // CMS/blogging (plataformas, no publishers)
  "wp.com","wordpress.com","blogspot.com","wix.com","squarespace.com",
  "weebly.com","medium.com","substack.com","ghost.io","blogger.com",
  "webflow.com","jimdo.com","strikingly.com",
  // Knowledge/encyclopedia
  "wikipedia.org","wikimedia.org","wikihow.com","wikidata.org",
  // Travel booking (no content puro)
  "booking.com","airbnb.com","expedia.com","tripadvisor.com","hotels.com",
  "kayak.com","skyscanner.net","agoda.com","hostelworld.com",
  // Food delivery
  "ubereats.com","doordash.com","grubhub.com","deliveroo.com","justeat.com","rappi.com",
  // Ride/transport
  "uber.com","lyft.com","bolt.eu","cabify.com","grab.com",
  // Misc global brands
  "ikea.com","zara.com","hm.com","uniqlo.com","nike.com","adidas.com",
  "mcdonalds.com","starbucks.com","cocacola.com","pepsi.com",
  "samsung.com","lg.com","sony.com","panasonic.com","philips.com",
  "toyota.com","honda.com","bmw.com","mercedes-benz.com","volkswagen.com",
]);

// Detecta dominios de universidades e institutos académicos
function isUniversityDomain(domain) {
  if (domain.endsWith(".edu")) return true;
  if (domain.endsWith(".ac.uk"))  return true;
  if (domain.endsWith(".edu.au")) return true;
  if (domain.endsWith(".edu.br")) return true;
  if (domain.endsWith(".edu.mx")) return true;
  if (domain.endsWith(".ac.jp"))  return true;
  if (domain.endsWith(".edu.ar")) return true;
  if (domain.endsWith(".edu.co")) return true;
  if (domain.endsWith(".ac.nz"))  return true;
  if (domain.endsWith(".ac.za"))  return true;
  if (domain.endsWith(".sch.uk")) return true;
  if (domain.endsWith(".edu.es")) return true;
  if (domain.endsWith(".edu.it")) return true;
  if (domain.endsWith(".unimi.it")) return true;
  const kw = /\b(university|universidad|universidade|universit[aey]|uni[-.]|college|instituto[-.]tecnol|polytechnic|akademi|hochschule|facultad|school|escuela|colegio)\b/i;
  return kw.test(domain);
}

// Patrones de palabra clave para descartar verticales que NO son publishers.
// Aplica al nombre del dominio antes del primer punto. Más liviano que mantener
// listas exhaustivas — si un dominio matchea alguno de estos, no es prospect ADEQ.
const EXCLUDE_KEYWORDS = [
  // Adulto / porno
  "porn","xxx","sex","adult","cam","escort","fetish","hentai","onlyfans","pornhub","xvideos","xnxx","redtube",
  "youporn","brazzers","chaturbate","stripchat","camsoda","myfreecams","livejasmin","bongacams",
  // Gobierno / instituciones
  "gov","gob","government","municipalidad","ayuntamiento","ministerio","ministry","parliament","congreso",
  // Bancos grandes / fintech enterprise
  "bank","banco","banca","banking","santander","bbva","caixabank","sabadell","unicredit","intesasanpaolo",
  // Seguros enterprise
  "insurance","seguros","mapfre","allianz","axa","zurich","prudential",
  // Pharma enterprise
  "pharma","pfizer","novartis","roche","merck","sanofi","gsk","astrazeneca",
  // Telco enterprise
  "telecom","telefonica","movistar","orange","vodafone","telmex","claro","mts",
  // Energía enterprise
  "petroleum","petrobras","exxon","chevron","totalenergies","shell","bp-",
  // Aerolineas enterprise
  "airlines","aerolineas","iberia","lufthansa","ryanair","emirates","qatarairways","americanairlines",
  // Search / ad networks que se cuelan
  "doubleclick","adservice","adsystem","adnxs","criteo","outbrain","taboola",
  // Hosting / CDN
  "cloudflare","cloudfront","fastly","akamai","jsdelivr","unpkg","cdn",
  // Marketplace gigantes locales
  "olx","craigslist","mercadolivre","gumtree","letgo","wallapop","jiji",
  // Apuestas/gambling enterprise (legales pero gigantes)
  "bet365","williamhill","ladbrokes","betfair","draftkings","fanduel","pokerstars",
];

const EXCLUDE_KEYWORD_RE = new RegExp(`\\b(${EXCLUDE_KEYWORDS.join("|")})\\b`, "i");

function matchesExcludeKeyword(domain) {
  // Solo chequea el nombre antes del primer punto + sufijos relevantes
  return EXCLUDE_KEYWORD_RE.test(domain);
}

function isDomainAllowed(domain) {
  if (!domain || !domain.includes(".")) return false;
  if (EXCLUDE_DOMAINS.has(domain)) return false;
  if (isUniversityDomain(domain)) return false;
  if (matchesExcludeKeyword(domain)) return false;
  return true;
}

// Cap superior de tráfico — sitios con > 40M visits/mes son demasiado grandes
// para que ADEQ pueda venderles. El SDR perdió tiempo persiguiéndolos.
const MAX_TRAFFIC_FOR_PROSPECT = 40_000_000;

// Pool global — se carga una vez al iniciar el proceso
let domainPool = null;

const COUNTRY_CODES = {
  US:"United States", MX:"Mexico", AR:"Argentina", CO:"Colombia", BR:"Brazil",
  CL:"Chile", ES:"Spain", PE:"Peru", EC:"Ecuador", VE:"Venezuela", UY:"Uruguay",
  PY:"Paraguay", BO:"Bolivia", DO:"Dominican Republic", CR:"Costa Rica",
  PA:"Panama", GT:"Guatemala", HN:"Honduras", SV:"El Salvador", NI:"Nicaragua",
  CU:"Cuba", PR:"Puerto Rico",
  GB:"United Kingdom", FR:"France", DE:"Germany", IT:"Italy", PT:"Portugal",
  CA:"Canada", AU:"Australia", JP:"Japan", KR:"South Korea", IN:"India",
  VN:"Vietnam", TH:"Thailand", ID:"Indonesia", PH:"Philippines", TR:"Turkey",
  SA:"Saudi Arabia", AE:"UAE", EG:"Egypt", MA:"Morocco", ZA:"South Africa",
  NG:"Nigeria", RU:"Russia", UA:"Ukraine", PL:"Poland", NL:"Netherlands",
  BE:"Belgium", SE:"Sweden", CH:"Switzerland", AT:"Austria", NO:"Norway",
  DK:"Denmark", FI:"Finland", IL:"Israel", SG:"Singapore", CN:"China",
  MY:"Malaysia", GR:"Greece", HU:"Hungary", CZ:"Czech Republic", RO:"Romania",
  TW:"Taiwan", HK:"Hong Kong", PK:"Pakistan",
  BG:"Bulgaria", HR:"Croatia", SI:"Slovenia", RS:"Serbia", IE:"Ireland",
  BD:"Bangladesh", LK:"Sri Lanka", KE:"Kenya", DZ:"Algeria", TN:"Tunisia",
  JO:"Jordan", LB:"Lebanon", IQ:"Iraq", KW:"Kuwait", QA:"Qatar", OM:"Oman",
  YE:"Yemen", LY:"Libya", SN:"Senegal", CI:"Ivory Coast", GH:"Ghana",
};

// Reverse lookup: country name → ISO code (for Cloudflare Radar API)
const COUNTRY_NAME_TO_CODE = Object.fromEntries(
  Object.entries(COUNTRY_CODES).map(([code, name]) => [name, code])
);

// ── Monday GEO label canonical (Spanish, no accents, first cap) ──
// El campo texto6 "Top Geo" en Monday espera nombres en español SIN tildes.
// Mapeo desde ISO code O nombre inglés → label Monday válido.
// Si no encontramos match → return "" (no insertar valor inventado).
const MONDAY_GEO_LABELS = {
  US:"Estados Unidos", MX:"Mexico", AR:"Argentina", CO:"Colombia", BR:"Brasil",
  CL:"Chile", ES:"Espana", PE:"Peru", EC:"Ecuador", VE:"Venezuela", UY:"Uruguay",
  PY:"Paraguay", BO:"Bolivia", DO:"Republica Dominicana", CR:"Costa Rica",
  PA:"Panama", GT:"Guatemala", HN:"Honduras", SV:"El Salvador", NI:"Nicaragua",
  CU:"Cuba", PR:"Puerto Rico",
  GB:"Reino Unido", FR:"Francia", DE:"Alemania", IT:"Italia", PT:"Portugal",
  CA:"Canada", AU:"Australia", NZ:"Nueva Zelanda",
  JP:"Japon", KR:"Corea del Sur", IN:"India",
  VN:"Vietnam", TH:"Tailandia", ID:"Indonesia", PH:"Filipinas", TR:"Turquia",
  SA:"Arabia Saudita", AE:"Emiratos Arabes", EG:"Egipto", MA:"Marruecos",
  ZA:"Sudafrica", NG:"Nigeria", KE:"Kenia", GH:"Ghana", ET:"Etiopia",
  RU:"Rusia", UA:"Ucrania", PL:"Polonia",
  NL:"Paises Bajos", BE:"Belgica", SE:"Suecia", CH:"Suiza", AT:"Austria",
  NO:"Noruega", DK:"Dinamarca", FI:"Finlandia", IE:"Irlanda", LU:"Luxemburgo",
  IL:"Israel", SG:"Singapur", CN:"China",
  MY:"Malasia", GR:"Grecia", HU:"Hungria", CZ:"Republica Checa", RO:"Rumania",
  TW:"Taiwan", HK:"Hong Kong", PK:"Pakistan", BD:"Bangladesh",
};

// Formatea visits para Monday "Paginas Vistas" (texto7):
// - >= 1M → "N.NM" (548091 → "0.5M" no, mejor 548091 → "540K")
// - 1K-999K → redondeado a 10K más cercano → "NNK" (548091 → "540K", 184500 → "180K")
// - < 1K → as-is
function formatTrafficForMonday(visits) {
  const n = parseInt(visits, 10);
  if (!n || n <= 0) return "";
  if (n >= 1_000_000) {
    // 1 decimal: 2,345,678 → "2.3M", 5,700,000 → "5.7M"
    return (n / 1_000_000).toFixed(1).replace(/\.0$/, "") + "M";
  }
  if (n >= 1_000) {
    // Redondeo a 10K más cercano hacia abajo: 548091 → 540K, 184500 → 180K
    const rounded = Math.floor(n / 10_000) * 10_000;
    return Math.round(rounded / 1000) + "K";
  }
  return String(n);
}

// Normaliza cualquier input geo a label válido para Monday (texto6).
// Acepta: ISO code (PY), nombre inglés (Brazil), nombre español con tildes (México).
// Devuelve "" si no podemos confirmar — mejor vacío que inventar.
function normalizeMondayGeo(raw) {
  if (!raw) return "";
  const s = String(raw).trim();
  if (!s) return "";
  // 1. ISO code directo (2 letras uppercase)
  const upper = s.toUpperCase();
  if (MONDAY_GEO_LABELS[upper]) return MONDAY_GEO_LABELS[upper];
  // 2. Nombre inglés conocido — convertir vía COUNTRY_NAME_TO_CODE
  const fromEnglish = COUNTRY_NAME_TO_CODE[s];
  if (fromEnglish && MONDAY_GEO_LABELS[fromEnglish]) return MONDAY_GEO_LABELS[fromEnglish];
  // 3. Nombre español con tildes — strip y match contra labels existentes
  const stripped = s.normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase();
  for (const [code, label] of Object.entries(MONDAY_GEO_LABELS)) {
    if (label.toLowerCase() === stripped) return label;
  }
  // 4. No match → vacío (no inventar)
  return "";
}

// ── Domain pool (Majestic Million) ───────────────────────────

async function loadDomainPool() {
  if (domainPool !== null) return domainPool;

  log("Descargando Majestic Million (puede tardar ~20s)...");
  try {
    const res = await fetch(MAJESTIC_URL, { signal: AbortSignal.timeout(90_000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const text = await res.text();

    const lines = text.split("\n").slice(1); // saltar header
    const domains = [];
    for (const line of lines) {
      if (!line.trim()) continue;
      const cols = line.split(",");
      const domain = cols[2]?.trim().toLowerCase().replace(/^www\./, "");
      if (isDomainAllowed(domain)) domains.push(domain);
    }
    domainPool = domains;
    log(`Pool cargado: ${domains.length.toLocaleString()} dominios disponibles.`);
    // Memory snapshot — para detectar OOM ANTES que Railway mate al worker
    try {
      const m = process.memoryUsage();
      const mb = (b) => Math.round(b / 1024 / 1024);
      log(`💾 Memoria post-pool: rss=${mb(m.rss)}MB, heapUsed=${mb(m.heapUsed)}MB, heapTotal=${mb(m.heapTotal)}MB, external=${mb(m.external)}MB`);
    } catch {}
  } catch (err) {
    log(`⚠️ Error descargando pool: ${err.message} — se reintentará en la próxima sesión.`);
    domainPool = []; // evitar retry inmediato; se reseteará al reiniciar
  }
  return domainPool;
}

// ── Cloudflare Radar: country-indexed domain pool ────────────
// Returns Map<domain, Set<countryCode>> for each country in countryCodes.
// Requires CLOUDFLARE_API_TOKEN env var. If missing → returns null (fallback to Majestic).
async function loadRadarPoolForCountries(countryCodes) {
  if (!CLOUDFLARE_API_TOKEN) return null;
  if (!countryCodes?.length) return null;

  const headers = { "Authorization": `Bearer ${CLOUDFLARE_API_TOKEN}` };
  const merged = new Map(); // domain → Set<CC>
  const LIMIT  = 1000;

  for (const cc of countryCodes) {
    try {
      // Cloudflare Radar /ranking/top devuelve TOP 100 por país (max).
      // Antes pedíamos limit=1000 → HTTP 400. También faltaba name=top.
      // Token requiere scope "Radar:Read" en Cloudflare dashboard.
      const url = `https://api.cloudflare.com/client/v4/radar/ranking/top?name=top&location=${encodeURIComponent(cc)}&limit=100`;
      const res = await fetch(url, { headers, signal: AbortSignal.timeout(15_000) });
      if (!res.ok) {
        let detail = "";
        try {
          const errBody = await res.json();
          detail = errBody?.errors?.[0]?.message || JSON.stringify(errBody?.errors || errBody).substring(0, 120);
        } catch {}
        log(`  ⚠️ Radar ${cc}: HTTP ${res.status}${detail ? " — " + detail : ""}`);
        continue;
      }
      const data = await res.json();
      const top  = data?.result?.top_0 || data?.result?.top || [];
      let added = 0;
      for (const entry of top) {
        const domain = (entry?.domain || entry?.value || "").toLowerCase().replace(/^www\./, "");
        if (!domain || !isDomainAllowed(domain)) continue;
        if (BLACKLIST_TLDS.some(tld => domain.endsWith(tld))) continue;
        if (!merged.has(domain)) merged.set(domain, new Set());
        merged.get(domain).add(cc);
        added++;
      }
      log(`  🌎 Radar ${cc}: +${added} dominios`);
    } catch (e) {
      log(`  ⚠️ Radar ${cc}: ${e.message}`);
    }
  }
  return merged.size > 0 ? merged : null;
}

// Expand a mix of regions + countries to ISO codes Cloudflare Radar understands
function targetGeosToCountryCodes(targetGeos) {
  const codes = new Set();
  for (const g of targetGeos) {
    if (GEO_REGIONS[g]) {
      for (const country of GEO_REGIONS[g]) {
        const cc = COUNTRY_NAME_TO_CODE[country];
        if (cc) codes.add(cc);
      }
    } else {
      const cc = COUNTRY_NAME_TO_CODE[g];
      if (cc) codes.add(cc);
    }
  }
  return [...codes];
}

// ── Supabase helpers ──────────────────────────────────────────

async function supabaseLogin() {
  // Si tenemos SERVICE_ROLE_KEY (recomendado en backend), saltamos el login con
  // email/password. Las queries usan BACKEND_BEARER directamente y bypassan RLS.
  // Esto evita el punto de falla "Invalid login credentials" cuando alguien
  // cambia la password del usuario admin desde la toolbar.
  if (SUPABASE_SERVICE_ROLE_KEY) {
    return SUPABASE_SERVICE_ROLE_KEY;
  }
  // Fallback: login con email/password si no hay service role configurado
  if (!SUPABASE_EMAIL || !SUPABASE_PASSWORD) {
    throw new Error("Falta SUPABASE_SERVICE_ROLE_KEY (preferido) o SUPABASE_EMAIL+PASSWORD");
  }
  const res = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=password`, {
    method: "POST",
    headers: { "apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ email: SUPABASE_EMAIL, password: SUPABASE_PASSWORD }),
  });
  const data = await res.json();
  if (!data.access_token) throw new Error("Login fallido: " + JSON.stringify(data));
  return data.access_token;
}

async function getConfig(token) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_config?select=key,value`, {
    headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` },
  });
  const rows = await res.json();
  const cfg = {};
  rows.forEach(r => { cfg[r.key] = r.value; });
  return cfg;
}

// Lectura liviana — flags de encendido, para el poll idle
async function getActiveFlags(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(auto_prospecting_enabled,csv_queue_enabled,agent_enabled_users,agent_paused_until)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map = {};
    rows.forEach(r => { map[r.key] = r.value; });
    let agentUsers = [];
    try { agentUsers = JSON.parse(map.agent_enabled_users || "[]"); } catch {}
    // Parse defensivo: si agent_paused_until es "" o invalid date, tratar como 0.
    // Bug previo: new Date("").getTime() = NaN → Date.now() > NaN = false → agent INACTIVE
    let pausedUntil = 0;
    if (map.agent_paused_until && map.agent_paused_until.trim()) {
      const parsed = new Date(map.agent_paused_until).getTime();
      if (!isNaN(parsed)) pausedUntil = parsed;
    }
    const agentActive = agentUsers.length > 0 && Date.now() > pausedUntil;
    return {
      autopilot: map.auto_prospecting_enabled === "true",
      csvQueue:  map.csv_queue_enabled === "true",
      agent:     agentActive,
      agentUsers,
    };
  } catch { return { autopilot: false, csvQueue: false, agent: false, agentUsers: [] }; }
}

async function isAutopilotEnabled(token) {
  const f = await getActiveFlags(token);
  return f.autopilot;
}

async function setConfigValue(token, key, value) {
  await fetch(`${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.${key}`, {
    method: "PATCH",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ value }),
  });
}

async function getProcessedDomains(token) {
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/toolbar_import_queue?select=domain&expires_at=gt.${encodeURIComponent(new Date().toISOString())}`,
    { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
  );
  const rows = await res.json();
  return new Set(rows.map(r => r.domain));
}

// Reasons with PERMANENT block (never re-evaluate)
const PERMANENT_REASONS = new Set([
  "country_blacklist",  // US/CA/RU/AU/NZ — won't change
  "tld_blacklist",      // .us/.ca/.ru/.au/.nz — won't change
  "government",         // .gov/.edu — won't change
  "blocklist",          // static exclude list (google.com, etc.) — won't change
  "rejected_by_user",   // user clicked ❌ Reject in Prospects
  "traffic_too_high",   // > 40M visits/mes — too big for ADEQ, won't change quickly
]);
// Other reasons (traffic_low, page_unreachable, api_error) → 60-day TTL, re-evaluate eventually

async function markProcessed(token, domains, reason = "processed") {
  const isPerm = PERMANENT_REASONS.has(reason);
  // Permanent = expire in 100 years. Temporary = 60 days.
  const expiresAt = new Date(Date.now() + (isPerm ? 100 * 365 : 60) * 24 * 60 * 60 * 1000).toISOString();
  await fetch(`${SUPABASE_URL}/rest/v1/toolbar_import_queue`, {
    method: "POST",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
      "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates",
    },
    body: JSON.stringify(domains.map(d => ({ domain: d, imported_by: reason, expires_at: expiresAt }))),
  });
}

// Bump the per-user/day/reason counter so the UI can show "today: 130 traffic_low / 25 added_to_review / ..."
// ── GEO Cache (toolbar_domain_geo_cache) ──────────────────────
// Cache compartido frontend+backend. Acá se usa para:
//  1. Pre-filtrar pool ANTES de llamar SimilarWeb (gran ahorro)
//  2. Persistir cada GEO obtenido (Radar + SimilarWeb)

async function getCachedGeoBulk(token, domains) {
  if (!Array.isArray(domains) || domains.length === 0) return new Map();
  const result = new Map();
  const CHUNK = 500;
  for (let i = 0; i < domains.length; i += CHUNK) {
    const slice = domains.slice(i, i + CHUNK);
    const list  = slice.map(d => `"${d.replace(/"/g, "")}"`).join(",");
    try {
      const res = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_domain_geo_cache?domain=in.(${encodeURIComponent(list)})&select=domain,country`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      if (!res.ok) continue;
      const rows = await res.json();
      for (const r of rows) result.set(r.domain, r.country);
    } catch {}
  }
  return result;
}

async function setCachedGeo(token, domain, country, source = "unknown", confidence = 5) {
  if (!domain || !country) return;
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_domain_geo_cache?on_conflict=domain`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({
        domain, country: country.toUpperCase().slice(0, 2),
        source, confidence: Math.max(1, Math.min(10, parseInt(confidence) || 5)),
        updated_at: new Date().toISOString(),
      }),
    });
  } catch {}
}

async function bumpStat(token, userEmail, reason, n = 1) {
  if (!userEmail || !reason) return;
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/rpc/bump_autopilot_stat`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ p_user_email: userEmail, p_reason: reason, p_count: n }),
    });
  } catch {} // best-effort, never block the loop
}

// Cap absoluto de la COLA DE PROCESAMIENTO (csv_queue.pending).
// Cuando se alcanza, los nuevos imports van a "waiting_pool" (max 300).
// El worker promueve waiting_pool → pending automáticamente cuando se libera
// espacio (al procesar items y bajar el count de pending).
const CSV_QUEUE_HARD_CAP = 200;
const CSV_WAITING_POOL_CAP = 300;
const WAITLIST_HARD_CAP  = 300;
// Cap diario GLOBAL — protege RapidAPI mensual (40K hits) + Railway billing.
// 1000 csv_queue + 300 autopilot = 1300/día = ~39K/mes.
// Configurable runtime: toolbar_config.csv_queue_daily_cap, autopilot_daily_cap_global
const DEFAULT_CSV_DAILY_GLOBAL_CAP = 1000;
const DEFAULT_AUTOPILOT_DAILY_GLOBAL_CAP = 1000;

// Counters in-process — se persisten al config para que sobrevivan restart
let _csvDailyCounterDate = null;
let _csvDailyCounter = 0;
let _autopilotDailyCounterDate = null;
let _autopilotDailyCounter = 0;

// Devuelve true si HOY es sábado o domingo (España). Operativos Lun-Vie.
function _isWeekendSpain() {
  const fmt = new Intl.DateTimeFormat("en-US", { timeZone: "Europe/Madrid", weekday: "short" });
  const day = fmt.format(new Date()); // "Mon", "Tue", ..., "Sun"
  return day === "Sat" || day === "Sun";
}

async function getDailyGlobalCounters(token) {
  // Día actual España (calendario, no UTC)
  const fmt = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit" });
  const todaySpain = fmt.format(new Date());
  // Reset si cambió el día calendario
  const _dayChanged = _csvDailyCounterDate !== null && _csvDailyCounterDate !== todaySpain;
  if (_csvDailyCounterDate !== todaySpain) { _csvDailyCounterDate = todaySpain; _csvDailyCounter = 0; }
  if (_autopilotDailyCounterDate !== todaySpain) { _autopilotDailyCounterDate = todaySpain; _autopilotDailyCounter = 0; }
  // Rollover next_day → waiting_pool al cambiar el día (regla user: excedente diario espera al siguiente día operativo)
  if (_dayChanged) {
    try {
      const promoteRes = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.next_day`, {
        method: "PATCH",
        headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=representation" },
        body: JSON.stringify({ status: "waiting_pool" }),
      });
      if (promoteRes.ok) {
        const rows = await promoteRes.json().catch(() => []);
        log(`🌅 day rollover ${todaySpain}: promoted ${rows.length} next_day → waiting_pool`);
      }
    } catch (e) { log(`⚠️ next_day rollover: ${e.message}`); }
  }
  // Cargar persisted counters de Supabase si vienen del mismo día
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(csv_daily_count,csv_daily_count_date,autopilot_daily_count,autopilot_daily_count_date,csv_queue_daily_cap,autopilot_daily_cap_global)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (res.ok) {
      const rows = await res.json();
      const map = {};
      rows.forEach(r => { map[r.key] = r.value; });
      if (map.csv_daily_count_date === todaySpain) _csvDailyCounter = Math.max(_csvDailyCounter, parseInt(map.csv_daily_count || "0", 10));
      if (map.autopilot_daily_count_date === todaySpain) _autopilotDailyCounter = Math.max(_autopilotDailyCounter, parseInt(map.autopilot_daily_count || "0", 10));
      return {
        csvCount:        _csvDailyCounter,
        csvCap:          parseInt(map.csv_queue_daily_cap || String(DEFAULT_CSV_DAILY_GLOBAL_CAP), 10),
        autopilotCount:  _autopilotDailyCounter,
        autopilotCap:    parseInt(map.autopilot_daily_cap_global || String(DEFAULT_AUTOPILOT_DAILY_GLOBAL_CAP), 10),
        date:            todaySpain,
      };
    }
  } catch {}
  return {
    csvCount: _csvDailyCounter, csvCap: DEFAULT_CSV_DAILY_GLOBAL_CAP,
    autopilotCount: _autopilotDailyCounter, autopilotCap: DEFAULT_AUTOPILOT_DAILY_GLOBAL_CAP,
    date: todaySpain,
  };
}

async function bumpCsvDailyCounter(token, n = 1) {
  _csvDailyCounter += n;
  await Promise.all([
    setConfigValue(token, "csv_daily_count",      String(_csvDailyCounter)),
    setConfigValue(token, "csv_daily_count_date", _csvDailyCounterDate || ""),
  ]);
}
async function bumpAutopilotDailyCounter(token, n = 1) {
  _autopilotDailyCounter += n;
  await Promise.all([
    setConfigValue(token, "autopilot_daily_count",      String(_autopilotDailyCounter)),
    setConfigValue(token, "autopilot_daily_count_date", _autopilotDailyCounterDate || ""),
  ]);
}

async function getCsvQueuePendingCountServer(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.pending&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("content-range") || res.headers.get("Content-Range") || "";
    const m = range.match(/\/(\d+)$/);
    return m ? parseInt(m[1]) : 0;
  } catch { return 0; }
}

// Promueve items csv_queue.status="waiting_pool" → "pending" si hay espacio
// en la cola de procesamiento (csv_queue.pending < 200).
async function promoteWaitlist(token) {
  try {
    const pendingCount = await getCsvQueuePendingCountServer(token);
    const slots = CSV_QUEUE_HARD_CAP - pendingCount;
    if (slots <= 0) return 0;
    // Trae hasta `slots` items waiting_pool, los promueve a pending (FIFO)
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.waiting_pool&order=uploaded_at.asc&limit=${slots}&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return 0;
    const items = await res.json();
    if (!Array.isArray(items) || items.length === 0) return 0;
    const ids = items.map(i => i.id);
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?id=in.(${ids.join(",")})`, {
      method: "PATCH",
      headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
      body: JSON.stringify({ status: "pending" }),
    });
    log(`📤 Promote: ${items.length} csv_queue items waitlist → pending (csv pending=${pendingCount}/${CSV_QUEUE_HARD_CAP})`);
    return items.length;
  } catch (e) {
    log(`⚠️ promoteWaitlist error: ${e.message}`);
    return 0;
  }
}

async function saveToReviewQueue(token, { domain, traffic, geo, geosAll, language, category, contactName, contactPhone, emails, emailSources = {}, pitch, pitchSubject, pitchSubjects, score, adNetworks, pageTitle, createdBy, source = "autopilot", mondayItemId = null }) {
  // NOTA: el cap de 200 en review_queue se chequea EN LOS CALLERS antes de
  // llamar acá (csv worker → marca waiting_pool, autopilot → skip silent).
  // Esta función solo INSERTA y devuelve boolean.

  const res = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue`, {
    method: "POST",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
      "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal",
    },
    body: JSON.stringify({
      domain,
      traffic:        traffic ? Math.round(traffic) : 0,
      geo:            geo            || "",
      geos_all:       Array.isArray(geosAll) && geosAll.length ? geosAll : null,
      language:       language       || "",
      category:       category       || "",
      contact_name:   contactName    || "",
      contact_phone:  contactPhone   || "",
      emails:         emails         || [],
      email_sources:  emailSources   || {},  // {email: "apollo"|"scrape"|"informer"|"generic"} para pick prioritario
      pitch:          pitch          || "",
      pitch_subject:  pitchSubject   || "",
      pitch_subjects: pitchSubjects  || [],
      score:          score          || 0,
      ad_networks:    adNetworks     || [],
      page_title:     pageTitle      || "",
      created_by:     createdBy      || "",
      source,                                   // "autopilot" | "csv" | "monday_refresh"
      monday_item_id: mondayItemId,             // si source="monday_refresh" el push hace UPDATE en vez de CREATE
      status:         "pending",
    }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    log(`  ❌ saveToReviewQueue ${domain} HTTP ${res.status}: ${txt.substring(0, 200)}`);
    return false;
  }
  return true;
}

// ── Bulk refresh de leads sin traffic ───────────────────────
// Job: cada loop iteration, si toolbar_config.agent_refresh_empty_leads=true,
// pickea hasta REFRESH_BATCH leads con traffic=0/null y los re-fetchea en
// paralelo. Cache 90d ayuda a no quemar RapidAPI. Cuando ya no quedan,
// auto-apaga el flag.
const REFRESH_EMPTY_BATCH = 3;
async function refreshOneEmptyLead(token, cfg) {
  const flag = cfg.agent_refresh_empty_leads === "true";
  if (!flag) return;
  const rapidapi_key = cfg.rapidapi_key;
  if (!rapidapi_key) return;

  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&or=(traffic.eq.0,traffic.is.null)&select=id,domain&order=created_at.asc&limit=${REFRESH_EMPTY_BATCH}`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) {
      log("✅ Refresh empty leads: completado, no quedan leads sin traffic. Apagando flag.");
      await setConfigValue(token, "agent_refresh_empty_leads", "false");
      return;
    }
    log(`🔄 Refresh empty batch: ${rows.length} leads`);
    await Promise.all(rows.map(async (lead) => {
      try {
        const data = await getTrafficData(lead.domain, rapidapi_key);
        const newVisits = data?.visits || 0;
        const newGeo = data?.topCountry || "";
        // Solo consideramos resuelto si hay tráfico real (>0). Sin tráfico el WHERE
        // del job lo vuelve a pickear y entra en loop. Geo solo no alcanza.
        if (newVisits > 0) {
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "PATCH",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({ traffic: newVisits, geo: newGeo || undefined }),
          });
          log(`  ✅ ${lead.domain} → traffic=${newVisits}, geo=${newGeo || "?"}`);
        } else if (data?.error && /429|rate/i.test(data.error)) {
          // 429: NO marcar -1, dejar en 0 para reintentar cuando se libere rate-limit
          log(`  ⏳ ${lead.domain} → 429, dejando para retry`);
        } else {
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "PATCH",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({ traffic: -1 }),
          });
          log(`  ⚠️ ${lead.domain} → sin traffic real (marcado -1)`);
        }
      } catch (e) {
        log(`  ⚠️ ${lead.domain} refresh err: ${e.message}`);
      }
    }));
  } catch (e) {
    log(`⚠️ refreshEmptyBatch error: ${e.message}`);
  }
}

// ── Backfill missing fields ────────────────────────────────────
// Job: si toolbar_config.agent_backfill_missing=true, busca leads pending con
// language/contact_name/category/title/ad_networks/score vacíos y los completa
// usando fetchPageContent + Apollo + detectLanguageRobust + scoreWebsite.
// Cache 90d traffic + 7d Apollo → mayoría son hits gratis.
const BACKFILL_BATCH = 5;
// ════════════════════════════════════════════════════════════════
// AUTO-FEEDER v2 (2026-05-18) — Schedule fijo + 3 fuentes + RapidAPI gate
// ────────────────────────────────────────────────────────────────
// Política user 2026-05-18:
//   • 5 crons/día L-V Madrid: 9, 12, 15, 18, 20
//   • Meta diaria: 150 efectivos sumados a review_queue
//   • Fuentes mixed (1/3 cada una): sellers.json + Monday Ciclo Finalizado + Majestic
//   • RapidAPI gate: skip si usedThisMonth ≥ 95% del limit (sin SimilarWeb no
//     hay enriquecimiento útil)
//   • Saturation: skip si review_queue ya ≥ 500 efectivos pending
//   • Conversion-aware: estima conversion rate de últimos 10 runs
// ════════════════════════════════════════════════════════════════

const FEEDER_SLOTS = [9, 12, 15, 18, 20];          // hora Madrid L-V
const FEEDER_DAILY_TARGET = 150;                    // efectivos sumados a review_queue
const FEEDER_PER_SLOT_TARGET = 30;                  // máx efectivos a meter en 1 slot
const FEEDER_RQ_SATURATION = 500;                   // skip si review_queue ya ≥ esto
const FEEDER_RAPIDAPI_THRESHOLD = 0.95;             // 95% del limit mensual → stop
const FEEDER_MEASURE_DELAY_MIN = 30;                // medir efectivos N min después
const FEEDER_DEFAULT_CONVERSION = 0.15;             // sin histórico, 15%
const FEEDER_FALLBACK_GROSS_CAP = 800;              // techo dur por si conversion se desploma

const FEEDER_SELLERS_SOURCES = [
  "https://improvedigital.com/sellers.json",
  "https://www.truvid.com/sellers.json",
  "https://www.themoneytizer.com/sellers.json",
  "https://triplelift.com/sellers.json",
  "https://www.vidoomy.com/sellers.json",
  "https://teads.tv/sellers.json",
  "https://pubmatic.com/sellers.json",
  "https://ad.plus/sellers.json",
  "https://openx.com/sellers.json",
  "https://sharethrough.com/sellers.json",
  "https://optad360.com/sellers.json",
  "https://setupad.com/sellers.json",
  "https://www.indexexchange.com/sellers.json",
  "https://152media.info/sellers.json",
  "https://mowplayer.com/sellers.json",
  "https://nsightvideo.com/sellers.json",
  "https://rubiconproject.com/sellers.json",
  "https://smartadserver.com/sellers.json",
  "https://www.seedtag.com/sellers.json",
  "https://verve.com/sellers.json",
  // Agregadas user 2026-05-18 — native + popunder
  "https://revcontent.com/sellers.json",
  "https://mgid.com/sellers.json",
  "https://propellerads.com/sellers.json",
  "https://admaven.com/sellers.json",
  // Europa — agregadas user 2026-05-18 (foco EU + LATAM)
  "https://equativ.com/sellers.json",
  "https://adagio.io/sellers.json",
  "https://showheroes.com/sellers.json",
  "https://adyoulike.com/sellers.json",
  "https://smartclip.com/sellers.json",
  "https://yieldbird.com/sellers.json",
  "https://aniview.com/sellers.json",
  "https://anyclip.com/sellers.json",
  "https://vidazoo.com/sellers.json",
  "https://openweb.com/sellers.json",
  "https://mobfox.com/sellers.json",
  "https://adtelligent.com/sellers.json",
  "https://adkernel.com/sellers.json",
  "https://aax.network/sellers.json",
  "https://dailymotion.com/sellers.json",
];

let _feederLastSlot = "";  // "YYYY-MM-DD-HH:00" del último slot disparado

function _normalizeFeederDomain(d) {
  if (!d || typeof d !== "string") return "";
  return d.toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/.*$/, "")
    .trim();
}

async function _findKnownDomainsWorker(token, candidates) {
  if (!Array.isArray(candidates) || candidates.length === 0) return new Set();
  const known = new Set();
  const headers = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  const BATCH = 200;
  const tables = [
    { table: "toolbar_csv_queue",     col: "domain" },
    { table: "toolbar_review_queue",  col: "domain" },
    { table: "toolbar_historial",     col: "domain" },
    { table: "toolbar_sendtrack",     col: "domain" },
    { table: "toolbar_url_blocklist", col: "domain" },
  ];
  for (let i = 0; i < candidates.length; i += BATCH) {
    const slice = candidates.slice(i, i + BATCH);
    const inList = slice.map(d => `"${d.replace(/"/g, '\\"')}"`).join(",");
    await Promise.all(tables.map(async ({ table, col }) => {
      try {
        const res = await fetch(
          `${SUPABASE_URL}/rest/v1/${table}?${col}=in.(${encodeURIComponent(inList)})&select=${col}`,
          { headers }
        );
        if (!res.ok) return;
        const rows = await res.json();
        rows.forEach(r => {
          const v = r[col];
          if (typeof v === "string" && v) known.add(v.toLowerCase());
        });
      } catch {}
    }));
  }
  return known;
}

function _madridNowParts() {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Madrid",
    year: "numeric", month: "2-digit", day: "2-digit",
    weekday: "short", hour: "2-digit", hour12: false,
  });
  const parts = fmt.formatToParts(new Date()).reduce((acc, p) => { acc[p.type] = p.value; return acc; }, {});
  return {
    hour: parseInt(parts.hour, 10),
    weekday: parts.weekday,
    dateISO: `${parts.year}-${parts.month}-${parts.day}`,
  };
}

function _currentFeederSlot() {
  const { hour, weekday, dateISO } = _madridNowParts();
  if (weekday === "Sat" || weekday === "Sun") return null;
  if (!FEEDER_SLOTS.includes(hour)) return null;
  return { slot: hour, slotLabel: `${dateISO}-${String(hour).padStart(2, "0")}:00` };
}

async function _getFeederTodayRuns(token, dateISO) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_feeder_runs?cron_at=gte.${dateISO}T00:00:00&select=*&order=cron_at.asc`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch { return []; }
}

async function _getReviewQueueValidCount(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.${REVIEW_QUEUE_MIN_TRAFFIC}&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("content-range") || "";
    return parseInt(range.match(/\/(\d+)$/)?.[1] || "0", 10);
  } catch { return 0; }
}

async function _getRecentConversionRate(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_feeder_runs?status=eq.ok&effective_added=not.is.null&order=cron_at.desc&limit=10&select=conversion_pct`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) return null;
    const vals = rows.map(r => parseFloat(r.conversion_pct)).filter(v => v > 0);
    if (vals.length === 0) return null;
    return (vals.reduce((a, b) => a + b, 0) / vals.length) / 100;
  } catch { return null; }
}

async function _insertFeederRun(token, slotLabel, data) {
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_feeder_runs`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json", "Prefer": "return=minimal",
      },
      body: JSON.stringify({ slot_label: slotLabel, ...data }),
    });
  } catch (e) { log(`⚠️ feeder run log failed: ${e.message}`); }
}

async function _injectIntoCsvQueue(token, domains, sourceTag) {
  if (!domains || domains.length === 0) return 0;
  const pendingNow = await getCsvQueuePendingCountServer(token).catch(() => 0);
  // Waiting count inline (no hay helper dedicado para waiting_pool)
  let waitingNow = 0;
  try {
    const wr = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.waiting_pool&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    waitingNow = parseInt((wr.headers.get("content-range") || "").match(/\/(\d+)$/)?.[1] || "0", 10);
  } catch {}
  const slotsPending = Math.max(0, CSV_QUEUE_HARD_CAP - pendingNow);
  const slotsWaiting = Math.max(0, 300 - waitingNow);
  let _p = 0, _w = 0;
  const payload = domains.map(domain => {
    let status;
    if (_p < slotsPending) { status = "pending"; _p++; }
    else if (_w < slotsWaiting) { status = "waiting_pool"; _w++; }
    else { status = "next_day"; }
    return { domain, status, source: sourceTag, uploaded_by: "worker@autofeeder", uploaded_at: new Date().toISOString() };
  });
  try {
    const res = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates,return=representation",
      },
      body: JSON.stringify(payload),
    });
    if (!res.ok) return 0;
    const rows = await res.json().catch(() => []);
    return Array.isArray(rows) ? rows.length : 0;
  } catch { return 0; }
}

async function _getMondayApiKeyForFeeder(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(monday_api_key_default,${encodeURIComponent("monday_api_key_mgargiulo@adeqmedia.com")})&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    const map = {}; rows.forEach(r => { map[r.key] = r.value; });
    return (map["monday_api_key_mgargiulo@adeqmedia.com"] || map["monday_api_key_default"] || "").trim() || null;
  } catch { return null; }
}

// FUENTE 1: sellers.json (rotación, insiste hasta llegar al target)
async function _feederPullSellers(token, targetCount, sessionKnown) {
  let inserted = 0;
  const sourcesToTry = [...FEEDER_SELLERS_SOURCES].sort(() => Math.random() - 0.5);
  for (const url of sourcesToTry) {
    if (inserted >= targetCount) break;
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
      if (!res.ok) continue;
      const data = await res.json().catch(() => null);
      if (!data || !Array.isArray(data.sellers)) continue;
      const candidates = [...new Set(
        data.sellers
          .filter(s => (s.seller_type || "").toUpperCase() === "PUBLISHER")
          .map(s => _normalizeFeederDomain(s.domain || ""))
          .filter(Boolean)
      )];
      if (candidates.length === 0) continue;
      const known = await _findKnownDomainsWorker(token, candidates);
      const fresh = candidates.filter(d => !known.has(d) && !sessionKnown.has(d));
      if (fresh.length === 0) continue;
      const slice = fresh.slice(0, targetCount - inserted);
      slice.forEach(d => sessionKnown.add(d));
      const ok = await _injectIntoCsvQueue(token, slice, "auto_feeder_sellers");
      inserted += ok;
      log(`  🌱 sellers ${url.split("/")[2]}: ${candidates.length} pubs → ${fresh.length} frescos → ${ok} insertados`);
    } catch (e) {
      log(`  ⚠️ sellers ${url.split("/")[2]} error: ${e.message}`);
    }
  }
  return inserted;
}

// FUENTE 2: Monday Ciclo Finalizado (pool 1000 + shuffle)
async function _feederPullMonday(token, targetCount, sessionKnown) {
  try {
    const mondayApiKey = await _getMondayApiKeyForFeeder(token);
    if (!mondayApiKey) { log(`  ⚠️ monday: no api key`); return 0; }
    const POOL_SIZE = 1000;
    let cursor = null;
    let items = [];
    do {
      const pageArgs = cursor
        ? `cursor: "${cursor}", limit: 500`
        : `limit: 500, query_params: { rules: [{ column_id: "deal_stage", compare_value: [5], operator: any_of }] }`;
      const query = `{ boards(ids: [1420268379]) { items_page(${pageArgs}) { cursor items { name } } } }`;
      const res = await fetch("https://api.monday.com/v2", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": mondayApiKey, "API-Version": "2024-01" },
        body: JSON.stringify({ query }),
      });
      const data = await res.json().catch(() => null);
      const page = data?.data?.boards?.[0]?.items_page;
      items = [...items, ...(page?.items || [])];
      cursor = page?.cursor || null;
      if (items.length >= POOL_SIZE) break;
    } while (cursor);
    if (items.length === 0) return 0;
    const pool = [...new Set(items.map(it => _normalizeFeederDomain(it.name || "")).filter(Boolean))];
    for (let i = pool.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [pool[i], pool[j]] = [pool[j], pool[i]];
    }
    const known = await _findKnownDomainsWorker(token, pool);
    const fresh = pool.filter(d => !known.has(d) && !sessionKnown.has(d));
    if (fresh.length === 0) return 0;
    const slice = fresh.slice(0, targetCount);
    slice.forEach(d => sessionKnown.add(d));
    const inserted = await _injectIntoCsvQueue(token, slice, "auto_feeder_monday");
    log(`  🌱 monday: pool=${pool.length}, frescos=${fresh.length} → insertados=${inserted}`);
    return inserted;
  } catch (e) {
    log(`  ⚠️ monday feeder error: ${e.message}`);
    return 0;
  }
}

// FUENTE 3: Majestic Million (sample random top 1M)
async function _feederPullMajestic(token, targetCount, sessionKnown) {
  try {
    const pool = await loadDomainPool();
    if (!pool || pool.length === 0) return 0;
    const sampleSize = Math.min(pool.length, targetCount * 5);
    const sample = [];
    const seen = new Set();
    while (sample.length < sampleSize && seen.size < pool.length) {
      const idx = Math.floor(Math.random() * pool.length);
      const d = pool[idx];
      if (!seen.has(d)) { seen.add(d); sample.push(d); }
    }
    const known = await _findKnownDomainsWorker(token, sample);
    const fresh = sample.filter(d => !known.has(d) && !sessionKnown.has(d));
    if (fresh.length === 0) return 0;
    const slice = fresh.slice(0, targetCount);
    slice.forEach(d => sessionKnown.add(d));
    const inserted = await _injectIntoCsvQueue(token, slice, "auto_feeder_majestic");
    log(`  🌱 majestic: sample=${sample.length}, frescos=${fresh.length} → insertados=${inserted}`);
    return inserted;
  } catch (e) {
    log(`  ⚠️ majestic feeder error: ${e.message}`);
    return 0;
  }
}

// ORQUESTADOR: chequea si estamos en slot y si no disparó, dispara
async function maybeRunFeederSlot(token) {
  const slotInfo = _currentFeederSlot();
  if (!slotInfo) return;
  const { slotLabel } = slotInfo;
  if (_feederLastSlot === slotLabel) return;
  // DB recuerda (survives worker restart)
  try {
    const existing = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_feeder_runs?slot_label=eq.${encodeURIComponent(slotLabel)}&select=id&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (existing.ok) {
      const rows = await existing.json();
      if (Array.isArray(rows) && rows.length > 0) {
        _feederLastSlot = slotLabel;
        return;
      }
    }
  } catch {}
  _feederLastSlot = slotLabel;
  await _runFeederSlot(token, slotLabel);
}

async function _runFeederSlot(token, slotLabel) {
  log(`🌱 FEEDER cron ${slotLabel} fired`);

  // 1. Daily target check
  const today = _madridNowParts().dateISO;
  const todayRuns = await _getFeederTodayRuns(token, today);
  const dailyEffective = todayRuns.reduce((sum, r) => sum + (parseInt(r.effective_added, 10) || 0), 0);
  if (dailyEffective >= FEEDER_DAILY_TARGET) {
    log(`🛑 cron ${slotLabel} SKIP: daily target met (${dailyEffective}/${FEEDER_DAILY_TARGET})`);
    await _insertFeederRun(token, slotLabel, { status: "skipped_daily_target", notes: `daily=${dailyEffective}` });
    return;
  }

  // 2. RapidAPI gate (la única protección real de costo)
  const { usedThisMonth, limit: rapidLimit } = await getRapidApiUsageThisMonth(token);
  if (usedThisMonth >= rapidLimit * FEEDER_RAPIDAPI_THRESHOLD) {
    log(`🛑 cron ${slotLabel} SKIP: RapidAPI ${usedThisMonth}/${rapidLimit} (≥${FEEDER_RAPIDAPI_THRESHOLD * 100}%)`);
    await _insertFeederRun(token, slotLabel, {
      status: "skipped_rapidapi", rapidapi_used: usedThisMonth, rapidapi_limit: rapidLimit,
      notes: "RapidAPI near monthly limit",
    });
    return;
  }

  // 3. Saturation check
  const rqValid = await _getReviewQueueValidCount(token);
  if (rqValid >= FEEDER_RQ_SATURATION) {
    log(`🛑 cron ${slotLabel} SKIP: review_queue saturated (${rqValid}/${FEEDER_RQ_SATURATION})`);
    await _insertFeederRun(token, slotLabel, {
      status: "skipped_saturated", rq_valid_before: rqValid,
      rapidapi_used: usedThisMonth, rapidapi_limit: rapidLimit,
      notes: "review_queue full",
    });
    return;
  }

  // 4. Calcular target conversion-aware
  const remaining = FEEDER_DAILY_TARGET - dailyEffective;
  const targetEffective = Math.min(FEEDER_PER_SLOT_TARGET, remaining);
  const conv = await _getRecentConversionRate(token) || FEEDER_DEFAULT_CONVERSION;
  let targetGross = Math.ceil(targetEffective / conv);
  if (targetGross > FEEDER_FALLBACK_GROSS_CAP) targetGross = FEEDER_FALLBACK_GROSS_CAP;
  log(`📊 cron ${slotLabel}: target=${targetEffective} efectivos, conv=${(conv * 100).toFixed(1)}%, inyecta hasta ${targetGross} brutos`);

  // 5. Pull from 3 fuentes (split 1/3)
  const perSource = Math.ceil(targetGross / 3);
  const sessionKnown = new Set();  // evita que 2 fuentes inserten el mismo dominio en este slot
  const fromSellers  = await _feederPullSellers(token, perSource, sessionKnown);
  const fromMonday   = await _feederPullMonday(token, perSource, sessionKnown);
  const fromMajestic = await _feederPullMajestic(token, perSource, sessionKnown);
  const grossTotal = fromSellers + fromMonday + fromMajestic;

  log(`✅ cron ${slotLabel}: sellers=${fromSellers} monday=${fromMonday} majestic=${fromMajestic} = ${grossTotal} brutos`);

  await _insertFeederRun(token, slotLabel, {
    status: grossTotal > 0 ? "ok" : "incomplete",
    gross_sellers: fromSellers, gross_monday: fromMonday, gross_majestic: fromMajestic,
    rapidapi_used: usedThisMonth, rapidapi_limit: rapidLimit,
    rq_valid_before: rqValid,
  });
}

// MEASUREMENT: post-mortem para calcular effective_added después de 30 min.
// Corre 1x por loop iteration. Busca runs OK del día con effective_added=null
// y > 30 min de antigüedad, mide cuántos pasaron a review_queue, actualiza fila.
async function _measureFeederRuns(token) {
  try {
    const today = _madridNowParts().dateISO;
    const cutoffAgo = new Date(Date.now() - FEEDER_MEASURE_DELAY_MIN * 60_000).toISOString();
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_feeder_runs?status=eq.ok&effective_added=is.null&cron_at=lt.${cutoffAgo}&cron_at=gte.${today}T00:00:00&select=id,cron_at,gross_total,rq_valid_before&limit=5`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return;
    const pending = await res.json();
    if (!Array.isArray(pending) || pending.length === 0) return;
    for (const run of pending) {
      const rqValidNow = await _getReviewQueueValidCount(token);
      const delta = Math.max(0, rqValidNow - (parseInt(run.rq_valid_before, 10) || 0));
      const eff = Math.min(delta, parseInt(run.gross_total, 10) || 0);
      const conv = run.gross_total > 0 ? (eff / run.gross_total) * 100 : 0;
      await fetch(`${SUPABASE_URL}/rest/v1/toolbar_feeder_runs?id=eq.${run.id}`, {
        method: "PATCH",
        headers: {
          "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
          "Content-Type": "application/json", "Prefer": "return=minimal",
        },
        body: JSON.stringify({ effective_added: eff, conversion_pct: conv.toFixed(2), rq_valid_after: rqValidNow }),
      }).catch(() => {});
      log(`📏 feeder run id=${run.id}: gross=${run.gross_total} → efectivos=${eff} (${conv.toFixed(1)}%)`);
    }
  } catch (e) { log(`⚠️ measure feeder runs: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════
// AUTOPILOT SLOT — descubrimiento Majestic 1 vez al día (20:00 Madrid L-V).
// Política user 2026-05-18: además del auto-feeder (sellers.json + Monday +
// Majestic vía feeder), correr el autopilot completo 1x/día por la tarde
// para alimentar la cola con leads frescos para el día siguiente.
// El propio worker auto-apaga después de 20min (SESSION_LIMIT_MS).
// ════════════════════════════════════════════════════════════════
const AUTOPILOT_DAILY_HOUR = 20;
let _autopilotLastSlot = "";

async function maybeStartAutopilotSlot(token) {
  const { hour, weekday, dateISO } = _madridNowParts();
  if (weekday === "Sat" || weekday === "Sun") return;
  if (hour !== AUTOPILOT_DAILY_HOUR) return;
  const slotLabel = `autopilot-${dateISO}-${String(AUTOPILOT_DAILY_HOUR).padStart(2, "0")}:00`;
  if (_autopilotLastSlot === slotLabel) return;

  // Race-condition guard: persistir en Supabase para sobrevivir restarts.
  try {
    const cfgRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.autopilot_last_slot&select=value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (cfgRes.ok) {
      const rows = await cfgRes.json();
      if (rows?.[0]?.value === slotLabel) {
        _autopilotLastSlot = slotLabel;
        return;
      }
    }
  } catch {}
  _autopilotLastSlot = slotLabel;
  await setConfigValue(token, "autopilot_last_slot", slotLabel).catch(() => {});

  try {
    const cfg = await getConfig(token);
    if (String(cfg.auto_prospecting_enabled || "").toLowerCase() === "true") {
      log(`🛰️ autopilot ${slotLabel}: ya estaba prendido — skip auto-trigger`);
      return;
    }
    await setConfigValue(token, "auto_prospecting_enabled", "true");
    await setConfigValue(token, "auto_session_user", "worker@autopilot");
    await setConfigValue(token, "auto_session_start", new Date().toISOString());
    log(`🛰️ AUTOPILOT slot ${slotLabel} → ON (worker auto-apaga en ~20min)`);
  } catch (e) { log(`⚠️ maybeStartAutopilotSlot: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════
// AGENT SLOTS — el Agent solo envía en los mismos 5 horarios del feeder.
// 9/12/15/18/20 Madrid L-V × 6 leads/slot = 30 envíos/día max.
// Política user 2026-05-18: pattern más sano para Gmail (menos burst) +
// alineado con el feeder (cuando el cron mete leads, 30min después el
// worker los procesa, y al próximo slot el Agent ya tiene material).
// ════════════════════════════════════════════════════════════════
const AGENT_SLOTS = [9, 12, 15, 18, 20];
let _agentLastSlot = "";

function _currentAgentSlot() {
  const { hour, weekday, dateISO } = _madridNowParts();
  if (weekday === "Sat" || weekday === "Sun") return null;
  if (!AGENT_SLOTS.includes(hour)) return null;
  return { slot: hour, slotLabel: `agent-${dateISO}-${String(hour).padStart(2, "0")}:00` };
}

async function maybeRunAgentSlot(token, allFlags) {
  // En TEST_MODE bypaseamos slot (para pruebas inmediatas)
  try {
    const cfg = await getConfig(token);
    if (String(cfg.agent_test_mode || "").toLowerCase() === "true") {
      return await runAgentCycle(token, allFlags);
    }
  } catch {}
  const slotInfo = _currentAgentSlot();
  if (!slotInfo) return;
  if (_agentLastSlot === slotInfo.slotLabel) return;

  // Race-condition guard: persistir slot fired en Supabase para sobrevivir
  // restarts de Railway. Sin esto, si el worker reinicia entre 9:00 y 9:30,
  // _agentLastSlot se reseteaba en memoria y el slot disparaba 2 veces.
  try {
    const cfgRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.agent_last_slot&select=value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (cfgRes.ok) {
      const rows = await cfgRes.json();
      const dbVal = rows?.[0]?.value || "";
      if (dbVal === slotInfo.slotLabel) {
        _agentLastSlot = dbVal;  // sync memoria con DB
        return;
      }
    }
  } catch {}

  _agentLastSlot = slotInfo.slotLabel;
  await setConfigValue(token, "agent_last_slot", slotInfo.slotLabel).catch(() => {});
  log(`🤖 AGENT slot ${slotInfo.slotLabel} fired — up to 6 leads`);
  return await runAgentCycle(token, allFlags);
}

// ════════════════════════════════════════════════════════════════
// AUTO-PAUSE del Agent si 3 crons seguidos baja de cierto threshold.
// Política user 2026-05-18: si los últimos 3 runs ok dejaron < 5 efectivos
// cada uno (target por slot = 30), algo está mal — calidad de fuentes,
// SimilarWeb degradado, Apollo caído, etc. Pausamos el Agent 2h y loggeamos
// para que el admin chequee. El user puede reanudar manual con "Resume now".
// ════════════════════════════════════════════════════════════════
const AUTOPAUSE_MIN_EFFECTIVE = 5;     // por cron run
const AUTOPAUSE_LOOKBACK_RUNS = 3;     // últimos N runs ok
const AUTOPAUSE_DURATION_MIN  = 120;   // 2h pause si dispara

async function _checkAutoPauseAgent(token) {
  // DISABLED 2026-05-19: política user — el Agent nunca se debe auto-pausar
  // por slots de feeder con 0 efectivos. La lógica anterior castigaba al Agent
  // cuando el feeder se quedaba sin URLs nuevas para descubrir, ignorando que
  // el pool tuviera 200+ leads viejos listos para envío.
  //
  // Si el feeder devuelve 0 efectivos repetidamente, la acción correcta es
  // investigar la causa (fuentes rotas, todos duplicados, threshold mal puesto),
  // NO frenar al Agent. La función queda como no-op por si se quiere reactivar.
  return;
}

// ════════════════════════════════════════════════════════════════
// RE-ENRICH bad leads del review_queue — corre cuando flag activo
// ────────────────────────────────────────────────────────────────
// Política user 2026-05-13: hay ~240 leads en review_queue desde antes
// de los fixes (source-strict, Apollo TLD, etc.). Para que el agent
// los use bien, re-enriquecemos los que tienen 0 emails O solo generics.
// Apollo cuesta — controlamos cap diario (150) y monthly (2400).
// Procesa 5 leads/run, 1 run cada 60 iters (~5h).
// ════════════════════════════════════════════════════════════════
let _lastReenrichRunAt = 0;
const REENRICH_COOLDOWN_MS = 15 * 60 * 1000; // 15min entre runs (era 1h)
const REENRICH_BATCH = 10;                    // procesar 10 por run (era 5)

async function runReenrichBadLeads(token) {
  try {
    const cfg = await getConfig(token);
    if (String(cfg.agent_reenrich_bad_leads || "").toLowerCase() !== "true") return;
    if (Date.now() - _lastReenrichRunAt < REENRICH_COOLDOWN_MS) return;
    _lastReenrichRunAt = Date.now();

    const apollo_api_key = cfg.apollo_api_key;
    if (!apollo_api_key) { log("⚠️ reenrich: sin APOLLO_API_KEY"); return; }

    // Cap check
    const usage = await getApolloUsageToday(token);
    if (usage.usedToday >= usage.limit || (usage.usedThisMonth ?? 0) >= APOLLO_MONTHLY_HARD_CAP) {
      log("⚠️ reenrich: Apollo cap alcanzado, skip");
      return;
    }

    // Leads que necesitan re-enrich:
    // - 0 emails
    // - Solo emails generic (info@/contact@) sin source apollo/informer
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&select=id,domain,emails,email_sources,contact_name,category&order=created_at.asc&limit=50`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return;
    const leads = await res.json();
    if (!Array.isArray(leads)) return;

    // Filtrar los que necesitan re-enrich
    const candidates = leads.filter(l => {
      const emails = Array.isArray(l.emails) ? l.emails : [];
      if (emails.length === 0) return true;
      // Si todos los emails son source='generic' o están vacíos → re-enrich
      const sources = l.email_sources || {};
      const hasGoodSource = emails.some(e => {
        const src = sources[e.toLowerCase()] || "";
        return src === "apollo" || src === "informer";
      });
      return !hasGoodSource;
    }).slice(0, REENRICH_BATCH);

    if (candidates.length === 0) { log("✅ reenrich: todos los leads ya tienen email apollo/informer — flag OFF"); await setConfigValue(token, "agent_reenrich_bad_leads", "false"); return; }

    log(`🔄 reenrich: procesando ${candidates.length} leads malos`);
    for (const lead of candidates) {
      try {
        // 1) Scrape PRIMERO (gratis) — con CF decoder + JSON-LD nuevos
        let foundEmail = null;
        let foundSource = null;
        let foundContactName = "";
        try {
          const scraped = await scrapeEmailsForDomain(lead.domain);
          if (Array.isArray(scraped) && scraped.length > 0) {
            // Pickear el mejor por rank
            const ranked = scraped
              .map(e => ({ email: e, score: rankEmail(e, lead.domain) }))
              .filter(r => r.score > 0)
              .sort((a, b) => b.score - a.score);
            if (ranked.length > 0) {
              foundEmail = ranked[0].email;
              foundSource = "scrape";
            }
          }
        } catch (e) { log(`  ⚠️ scrape ${lead.domain}: ${e.message}`); }

        // 2) Apollo SOLO si scrape no encontró nada (ahorra crédito)
        if (!foundEmail) {
          try {
            const apolloRes = await findBestApolloEmail(lead.domain, apollo_api_key, token, {
              traffic: lead.traffic || 0, allowUnlock: true,
            });
            if (apolloRes?.email) {
              foundEmail = apolloRes.email;
              foundSource = "apollo";
              foundContactName = apolloRes.contact_name || "";
            }
          } catch (e) { log(`  ⚠️ apollo ${lead.domain}: ${e.message}`); }
        }

        if (foundEmail) {
          const existing = Array.isArray(lead.emails) ? lead.emails : [];
          const merged = [foundEmail, ...existing.filter(e => e.toLowerCase() !== foundEmail.toLowerCase())];
          const validated = await validateEmailsBatch(merged);
          const newSources = { ...(lead.email_sources || {}) };
          newSources[foundEmail.toLowerCase()] = foundSource;
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "PATCH",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({
              emails: validated,
              email_sources: newSources,
              contact_name: foundContactName || lead.contact_name || "",
            }),
          });
          log(`  ✅ ${lead.domain}: +${foundSource} ${foundEmail}`);
        } else {
          log(`  ⏭️ ${lead.domain}: scrape+Apollo sin resultados`);
        }
        await sleep(1500); // anti rate-limit
      } catch (e) {
        log(`  ⚠️ reenrich ${lead.domain}: ${e.message}`);
      }
    }
  } catch (e) {
    log(`⚠️ runReenrichBadLeads error: ${e.message}`);
  }
}

// ── Frozen Weekly Report ────────────────────────────────────────
// Cada domingo 20-21hs Madrid, manda email a mgargiulo@adeqmedia.com con CSV
// de dominios en toolbar_csv_queue.status='frozen' para análisis manual.
async function runFrozenWeeklyReport(token) {
  const fmt = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", hour12: false, weekday: "short" });
  const parts = fmt.formatToParts(new Date());
  const partMap = {}; parts.forEach(p => { partMap[p.type] = p.value; });
  const weekday = partMap.weekday; // "Sun"
  const hour = parseInt(partMap.hour, 10);
  if (weekday !== "Sun" || hour < 20 || hour >= 21) return;

  // Una vez por semana — chequear flag con week-of-year
  const today = `${partMap.year}-${partMap.month}-${partMap.day}`;
  try {
    const cfgRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.last_frozen_report_at&select=value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const cfgRows = await cfgRes.json().catch(() => []);
    const lastSent = cfgRows?.[0]?.value || "";
    if (lastSent === today) return; // ya se mandó hoy
  } catch {}

  // Pull frozen rows
  const frozenRes = await fetch(
    `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.frozen&select=domain,uploaded_at,uploaded_by,source,error_message,processed_at&order=uploaded_at.desc&limit=5000`,
    { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
  );
  if (!frozenRes.ok) return;
  const rows = await frozenRes.json();
  if (!Array.isArray(rows) || rows.length === 0) {
    log("📊 frozen weekly: 0 rows, skip email");
    await setConfigValue(token, "last_frozen_report_at", today);
    return;
  }

  // Build CSV
  const csvCell = (v) => {
    if (v == null) return "";
    const s = String(v).replace(/"/g, '""');
    return `"${s}"`;
  };
  const header = "domain,uploaded_at,uploaded_by,source,error_message,processed_at";
  const lines = [header];
  rows.forEach(r => {
    lines.push([r.domain, r.uploaded_at, r.uploaded_by, r.source, r.error_message, r.processed_at].map(csvCell).join(","));
  });
  const csvContent = lines.join("\n");

  // Stats
  const byError = {};
  rows.forEach(r => { const e = r.error_message || "unknown"; byError[e] = (byError[e] || 0) + 1; });
  const topErrors = Object.entries(byError).sort((a, b) => b[1] - a[1]).slice(0, 5)
    .map(([err, n]) => `  - ${err}: ${n}`).join("\n");

  const subject = `[ADEQ Toolbar] Frozen domains weekly — ${today} (${rows.length} leads)`;
  const body = [
    `📊 Frozen domains report — ${today}`,
    ``,
    `Total frozen: ${rows.length}`,
    ``,
    `Top errores:`,
    topErrors,
    ``,
    `Ver CSV adjunto para análisis manual.`,
    ``,
    `--`,
    `ADEQ Toolbar v5.0.33 worker`,
  ].join("\n");

  // Send con attachment via MIME multipart/mixed
  try {
    const userEmail = "mgargiulo@adeqmedia.com";
    const accessToken = await getGmailAccessToken(userEmail);
    const boundary = `----=adeq_frozen_${Date.now().toString(36)}`;
    const csvBase64 = Buffer.from(csvContent, "utf-8").toString("base64");
    const csvBase64Wrapped = csvBase64.match(/.{1,76}/g).join("\r\n");

    const mime = [
      `To: ${userEmail}`,
      `From: ${userEmail}`,
      `Subject: ${subject}`,
      "MIME-Version: 1.0",
      `Content-Type: multipart/mixed; boundary="${boundary}"`,
      "",
      `--${boundary}`,
      "Content-Type: text/plain; charset=utf-8",
      "Content-Transfer-Encoding: 8bit",
      "",
      body,
      "",
      `--${boundary}`,
      `Content-Type: text/csv; charset=utf-8; name="frozen_${today}.csv"`,
      "Content-Transfer-Encoding: base64",
      `Content-Disposition: attachment; filename="frozen_${today}.csv"`,
      "",
      csvBase64Wrapped,
      "",
      `--${boundary}--`,
    ].join("\r\n");

    const raw = Buffer.from(mime, "utf-8").toString("base64")
      .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

    const sendRes = await fetch("https://gmail.googleapis.com/gmail/v1/users/me/messages/send", {
      method: "POST",
      headers: { "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ raw }),
    });
    if (!sendRes.ok) {
      const errText = await sendRes.text();
      log(`⚠️ frozen weekly send failed: ${sendRes.status} ${errText.slice(0,200)}`);
      return;
    }
    await setConfigValue(token, "last_frozen_report_at", today);
    log(`📧 Frozen weekly report enviado: ${rows.length} dominios → ${userEmail}`);
  } catch (e) {
    log(`⚠️ frozen weekly: ${e.message}`);
  }
}

async function backfillMissingFields(token, cfg) {
  const flag = cfg.agent_backfill_missing === "true";
  if (!flag) return;
  const apollo_api_key = cfg.apollo_api_key;

  try {
    // Buscar leads pending con AL MENOS 1 campo crítico vacío
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&or=(language.is.null,language.eq.,contact_name.is.null,contact_name.eq.,category.is.null,category.eq.,page_title.is.null,page_title.eq.,score.eq.0,score.is.null)&select=id,domain,traffic,geo,language,category,page_title,ad_networks,emails,contact_name,score,status&order=created_at.asc&limit=${BACKFILL_BATCH}`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) {
      log("✅ Backfill missing: completado, no quedan leads incompletos. Apagando flag.");
      await setConfigValue(token, "agent_backfill_missing", "false");
      return;
    }
    log(`🔧 Backfill missing batch: ${rows.length} leads`);

    await Promise.all(rows.map(async (lead) => {
      try {
        const patch = {};
        // 1) Page content (title, ad_networks, category, htmlLang, ogLocale, textSample)
        const needsPage = !lead.category || !lead.page_title || !lead.ad_networks || (Array.isArray(lead.ad_networks) && lead.ad_networks.length === 0);
        let pageContent = null;
        if (needsPage) {
          pageContent = await fetchPageContent(lead.domain).catch(() => null);
          if (pageContent) {
            if (!lead.page_title && pageContent.title) { patch.page_title = pageContent.title; lead.page_title = pageContent.title; }
            if (!lead.category && pageContent.category) { patch.category = pageContent.category; lead.category = pageContent.category; }
            if ((!lead.ad_networks || lead.ad_networks.length === 0) && pageContent.adNetworks?.length) {
              patch.ad_networks = pageContent.adNetworks;
              lead.ad_networks = pageContent.adNetworks;
            }
          }
        }
        // 2) Language (usar pageContent si lo trajimos, sino fallback geo/tld)
        if (!lead.language || lead.language === "") {
          const det = await detectLanguageRobust({
            htmlLang:   pageContent?.htmlLang,
            ogLocale:   pageContent?.ogLocale,
            hreflang:   pageContent?.hreflang,
            jsonLdLang: pageContent?.jsonLdLang,
            pathLang:   pageContent?.pathLang,
            textSample: pageContent?.textSample,
            geo:        lead.geo,
            domain:     lead.domain,
          }, { token });
          patch.language = det.lang;
          lead.language = det.lang;
        }
        // 3) Contact name (Apollo cache 7d → gratis si ya existió)
        if ((!lead.contact_name || lead.contact_name === "") && apollo_api_key) {
          try {
            const apolloEmails = await findAllEmails(lead.domain, apollo_api_key, token);
            const name = apolloEmails && apolloEmails.contact_name;
            if (name) { patch.contact_name = name; lead.contact_name = name; }
            // Si trajo emails nuevos que no estaban, sumarlos (validados)
            if (Array.isArray(apolloEmails) && apolloEmails.length > 0) {
              const existing = new Set((lead.emails || []).filter(Boolean));
              const newEmails = apolloEmails.filter(e => !existing.has(e));
              if (newEmails.length) {
                const merged = [...new Set([...apolloEmails, ...(lead.emails || [])])];
                const validated = await validateEmailsBatch(merged);
                // Compara contenido (set), no length — length puede coincidir si
                // validateEmailsBatch quita N basura y agrega N nuevos.
                const oldSet = new Set((lead.emails || []).filter(Boolean));
                const newSet = new Set(validated);
                const sameContent = oldSet.size === newSet.size && [...oldSet].every(e => newSet.has(e));
                if (!sameContent) {
                  patch.emails = validated;
                  lead.emails = validated;
                }
              }
            }
          } catch {}
        }
        // 4) Score (sync, sin API)
        if (!lead.score || lead.score === 0) {
          const sw = scoreWebsite(lead);
          if (sw.score < 0) {
            // gate hit (geo/cat blocked) — marcar como rejected
            patch.status = "rejected";
            patch.validated_by = "agent:backfill";
            patch.validated_at = new Date().toISOString();
            log(`  ❌ ${lead.domain}: rejected (${sw.reasons.join(",")})`);
          } else {
            patch.score = sw.score;
            log(`  ⭐ ${lead.domain}: ${sw.stars}★ score=${sw.score} lang=${lead.language}`);
          }
        }
        if (Object.keys(patch).length > 0) {
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "PATCH",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify(patch),
          });
        }
      } catch (e) {
        log(`  ⚠️ ${lead.domain} backfill err: ${e.message}`);
      }
    }));
  } catch (e) {
    log(`⚠️ backfillMissingFields error: ${e.message}`);
  }
}

// ── Stats en vivo del autopilot, persistidas a toolbar_config ─────
// Los popups de los MBs leen estas keys cada 30s y muestran el progreso.
let _autopilotStatsLocal = { processed: 0, added: 0, filtered: 0, lastDomain: "", lastUpdate: 0, sessionUser: "" };
let _autopilotStatsLastFlush = 0;

async function flushAutopilotStats(token, force = false) {
  const now = Date.now();
  if (!force && (now - _autopilotStatsLastFlush) < 5000) return; // máx 1 vez cada 5s
  _autopilotStatsLastFlush = now;
  try {
    await setConfigValue(token, "auto_session_stats", JSON.stringify({
      ..._autopilotStatsLocal,
      lastUpdate: now,
    }));
  } catch (e) { /* silent */ }
}

function resetAutopilotStats(sessionUser) {
  _autopilotStatsLocal = { processed: 0, added: 0, filtered: 0, lastDomain: "", lastUpdate: Date.now(), sessionUser: sessionUser || "" };
}

function trackAutopilotEvent(kind, domain) {
  // kind: "processed" | "added" | "filtered"
  _autopilotStatsLocal.processed = (kind === "processed" ? _autopilotStatsLocal.processed + 1 : _autopilotStatsLocal.processed);
  if (kind === "added")    _autopilotStatsLocal.added++;
  if (kind === "filtered") _autopilotStatsLocal.filtered++;
  _autopilotStatsLocal.lastDomain = domain || _autopilotStatsLocal.lastDomain;
  _autopilotStatsLocal.lastUpdate = Date.now();
}

// Cuenta items de autopilot creados por un user hoy (para quota 75/día)
async function getUserAutopilotCountToday(token, userEmail) {
  if (!userEmail) return 0;
  try {
    // Argentina local day — quotas reset at 00:00 AR, not 00:00 UTC
    const todayISO = new Date().toLocaleDateString("en-CA", { timeZone: "America/Argentina/Buenos_Aires" });
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?created_by=eq.${encodeURIComponent(userEmail)}&created_at=gte.${todayISO}T00:00:00Z&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0" } }
    );
    const cr = res.headers.get("content-range") || "";
    const m = cr.match(/\/(\d+)$/);
    return m ? parseInt(m[1]) : 0;
  } catch { return 0; }
}

// Carga feedback del user: sets con categorías/geos/dominios que marcó como disliked
async function loadAutopilotFeedback(token, userEmail) {
  const dislikedCategories = new Map();
  const dislikedGeos       = new Map();
  const dislikedDomains    = new Set();
  const likedDomains       = [];  // ordered by recency — use for similar-site seeding
  const likedCategories    = new Map();
  const likedGeos          = new Map();
  if (!userEmail) return { dislikedCategories, dislikedGeos, dislikedDomains, likedDomains, likedCategories, likedGeos };
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_autopilot_feedback?user_email=eq.${encodeURIComponent(userEmail)}&action=in.(liked,disliked)&select=domain,action,category,geo,created_at&order=created_at.desc&limit=2000`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return { dislikedCategories, dislikedGeos, dislikedDomains, likedDomains, likedCategories, likedGeos };
    const rows = await res.json();
    for (const r of rows) {
      if (r.action === "disliked") {
        if (r.domain)   dislikedDomains.add(r.domain.toLowerCase());
        if (r.category) dislikedCategories.set(r.category, (dislikedCategories.get(r.category) || 0) + 1);
        if (r.geo)      dislikedGeos.set(r.geo,             (dislikedGeos.get(r.geo)             || 0) + 1);
      } else if (r.action === "liked") {
        if (r.domain)   likedDomains.push(r.domain.toLowerCase());
        if (r.category) likedCategories.set(r.category,     (likedCategories.get(r.category)    || 0) + 1);
        if (r.geo)      likedGeos.set(r.geo,                (likedGeos.get(r.geo)               || 0) + 1);
      }
    }
  } catch {}
  return { dislikedCategories, dislikedGeos, dislikedDomains, likedDomains, likedCategories, likedGeos };
}

// Lee límites custom del usuario desde toolbar_user_limits (admin panel).
// Defaults si no existe la fila.
async function getUserLimits(token, userEmail) {
  if (!userEmail) return { autopilot_enabled: true, autopilot_daily_minutes: 20, autopilot_daily_prospects: 300, monthly_api_cap: null };
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_user_limits?user_email=eq.${encodeURIComponent(userEmail.toLowerCase())}&select=*&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return { autopilot_enabled: true, autopilot_daily_minutes: 20, autopilot_daily_prospects: 300, monthly_api_cap: null };
    const rows = await res.json();
    if (!rows.length) return { autopilot_enabled: true, autopilot_daily_minutes: 20, autopilot_daily_prospects: 300, monthly_api_cap: null };
    const r = rows[0];
    return {
      autopilot_enabled:         r.autopilot_enabled !== false,
      autopilot_daily_minutes:   parseInt(r.autopilot_daily_minutes || 20, 10),
      autopilot_daily_prospects: parseInt(r.autopilot_daily_prospects || 300, 10),
      monthly_api_cap:           r.monthly_api_cap ? parseInt(r.monthly_api_cap, 10) : null,
    };
  } catch { return { autopilot_enabled: true, autopilot_daily_minutes: 20, autopilot_daily_prospects: 300, monthly_api_cap: null }; }
}

// Cap MENSUAL Apollo (plan = 2,500 credits/mes; cap conservador 2,400 con margen 100).
// Si llega → fallback a scraping/page-emails (no rompe flujos, solo no usa Apollo).
const APOLLO_MONTHLY_HARD_CAP = 2400;

async function getApolloUsageToday(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(apollo_calls_today,apollo_calls_date,apollo_daily_limit,apollo_calls_month,apollo_calls_month_period,apollo_monthly_limit)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const today   = new Date().toISOString().slice(0, 10);
    const storedDate  = map.apollo_calls_date  || "";
    const storedCount = parseInt(map.apollo_calls_today || "0", 10);
    const limit       = parseInt(map.apollo_daily_limit || "150", 10);
    const usedToday = storedDate === today ? storedCount : 0;

    // Mensual — alineado con billing cycle 6→6 (igual que RapidAPI)
    const period       = _billingCyclePeriod();
    const storedPeriod = map.apollo_calls_month_period || "";
    const storedMonth  = parseInt(map.apollo_calls_month || "0", 10);
    const monthLimit   = parseInt(map.apollo_monthly_limit || String(APOLLO_MONTHLY_HARD_CAP), 10);
    const usedThisMonth = storedPeriod === period ? storedMonth : 0;

    return { usedToday, limit, today, usedThisMonth, monthLimit, period };
  } catch { return { usedToday: 0, limit: 50, today: new Date().toISOString().slice(0, 10), usedThisMonth: 0, monthLimit: APOLLO_MONTHLY_HARD_CAP, period: "" }; }
}

// ── Hard cap MENSUAL de RapidAPI ────────────────────────────────────
// Default 40.000/mes (vs FREE de 500k) — protección contra overage.
// Compartido entre worker y toolbar manual (mismas keys en toolbar_config).
async function getRapidApiUsageThisMonth(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(rapidapi_calls_month,rapidapi_calls_month_period,rapidapi_monthly_limit)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const period      = _billingCyclePeriod();
    const storedPer   = map.rapidapi_calls_month_period || "";
    const storedCount = parseInt(map.rapidapi_calls_month || "0", 10);
    const limit       = parseInt(map.rapidapi_monthly_limit || "40000", 10);
    // Compat con formato viejo (YYYY-MM-06): comparar por YYYY-MM.
    const sameMonth   = storedPer.slice(0, 7) === period.slice(0, 7);
    const usedThisMonth = sameMonth ? storedCount : 0;
    return { usedThisMonth, limit, period };
  } catch { return { usedThisMonth: 0, limit: 40000, period: _billingCyclePeriod() }; }
}

// Período mensual = MES CALENDARIO (decisión user 2026-05-12).
// Reset automático día 1. Formato "YYYY-MM" matchea con popup apiProxy.
function _billingCyclePeriod() {
  return new Date().toISOString().slice(0, 7);
}

async function saveRapidApiMonthlyUsage(token, callsThisSession, period) {
  if (callsThisSession <= 0) return;
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(rapidapi_calls_month,rapidapi_calls_month_period)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const storedPer   = map.rapidapi_calls_month_period || "";
    // Compat YYYY-MM-06 vs YYYY-MM
    const sameMonth   = storedPer.slice(0, 7) === period.slice(0, 7);
    const storedCount = sameMonth ? parseInt(map.rapidapi_calls_month || "0", 10) : 0;
    const newCount    = storedCount + callsThisSession;

    await setConfigValue(token, "rapidapi_calls_month",        String(newCount));
    await setConfigValue(token, "rapidapi_calls_month_period", period);
    log(`RapidAPI mensual guardado: ${newCount} hits en ${period} (sumé ${callsThisSession} esta sesión)`);
  } catch {}
}

// ── Hard cap diario de RapidAPI ─────────────────────────────────────
// Sumamos ALL hits (autopilot + similar-sites + countries) y cortamos al límite.
// Default: 5000/día (vs. plan PRO de 25k/mes ≈ 833/día → con margen de seguridad).
// Cambiable en runtime via Supabase: toolbar_config.rapidapi_daily_limit
async function getRapidApiUsageToday(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(rapidapi_calls_today,rapidapi_calls_date,rapidapi_daily_limit)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const today       = new Date().toISOString().slice(0, 10);
    const storedDate  = map.rapidapi_calls_date  || "";
    const storedCount = parseInt(map.rapidapi_calls_today || "0", 10);
    const limit       = parseInt(map.rapidapi_daily_limit || "5000", 10);
    const usedToday   = storedDate === today ? storedCount : 0;
    return { usedToday, limit, today };
  } catch { return { usedToday: 0, limit: 5000, today: new Date().toISOString().slice(0, 10) }; }
}

async function saveRapidApiUsage(token, callsThisSession, today) {
  if (callsThisSession <= 0) return;
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(rapidapi_calls_today,rapidapi_calls_date)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const storedDate  = map.rapidapi_calls_date || "";
    const storedCount = storedDate === today ? parseInt(map.rapidapi_calls_today || "0", 10) : 0;
    const newCount    = storedCount + callsThisSession;

    await setConfigValue(token, "rapidapi_calls_today", String(newCount));
    await setConfigValue(token, "rapidapi_calls_date",  today);
    log(`RapidAPI usage guardado: ${newCount} hits hoy (sumé ${callsThisSession} en esta sesión)`);
  } catch {}
}

async function saveApolloUsage(token, callsThisSession, today) {
  if (callsThisSession === 0) return;
  try {
    // Leer counters actuales (otra sesión pudo haber corrido en paralelo)
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(apollo_calls_today,apollo_calls_date,apollo_calls_month,apollo_calls_month_period)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const storedDate  = map.apollo_calls_date || "";
    const storedCount = storedDate === today ? parseInt(map.apollo_calls_today || "0", 10) : 0;
    const newCount    = storedCount + callsThisSession;

    // Mensual: ciclo 6→6 igual que RapidAPI
    const period         = _billingCyclePeriod();
    const storedPeriod   = map.apollo_calls_month_period || "";
    const storedMonth    = storedPeriod === period ? parseInt(map.apollo_calls_month || "0", 10) : 0;
    const newMonth       = storedMonth + callsThisSession;
    await setConfigValue(token, "apollo_calls_month",        String(newMonth));
    await setConfigValue(token, "apollo_calls_month_period", period);

    await setConfigValue(token, "apollo_calls_today", String(newCount));
    await setConfigValue(token, "apollo_calls_date",  today);
    log(`Apollo usage guardado: ${newCount} calls hoy (sumé ${callsThisSession} en esta sesión)`);
  } catch {}
}

async function getRejectionPatterns(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?select=category,geo&status=eq.rejected&order=created_at.desc&limit=150`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    if (!Array.isArray(rows)) return { categories: {}, geos: {} };
    const categories = {}, geos = {};
    for (const r of rows) {
      if (r.category) categories[r.category] = (categories[r.category] || 0) + 1;
      if (r.geo)      geos[r.geo]             = (geos[r.geo]             || 0) + 1;
    }
    return { categories, geos };
  } catch { return { categories: {}, geos: {} }; }
}

// ── APIs externas ─────────────────────────────────────────────

// Estados Monday que SE PUEDEN re-prospectar (autopilot + CSV pueden traerlos).
// Cualquier otro estado bloquea el dominio (En Negociacion / Propuesta Vigente / etc).
const REPROSPECTABLE_STATES = new Set(["Ciclo Finalizado", "Mail No Enviado"]);

async function fetchMondayDomains(apiKey) {
  // Trae name + estado y devuelve solo dominios cuyo estado NO es re-prospectable.
  // Esos son los que el autopilot debe excluir del pool Majestic.
  const query = `{
    boards(ids: [1420268379]) {
      items_page(limit: 500) {
        items {
          name
          column_values(ids: ["deal_stage"]) { text }
        }
      }
    }
  }`;
  const res = await fetch("https://api.monday.com/v2", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": apiKey, "API-Version": "2024-01" },
    body: JSON.stringify({ query }),
  });
  const data = await res.json();
  const items = data?.data?.boards?.[0]?.items_page?.items || [];
  return items
    .filter(i => {
      const estado = i.column_values?.[0]?.text || "";
      return !REPROSPECTABLE_STATES.has(estado);
    })
    .map(i => cleanDomain(i.name))
    .filter(Boolean);
}

// Retry helper — 3 attempts with exponential backoff for transient RapidAPI errors.
// SOLO retry en 5xx + network (no facturan). 429 = abandonar inmediato porque
// cada retry de 429 es 1 request facturada por RapidAPI (incidente del 02/04
// — overage de 400 USD por retries en cascada en día con vendor degradado).
// 4xx también fail-fast.
// Token global del worker — accesible desde funciones helper como rapidFetchWithRetry
// que no recibe token explícito. Se actualiza en main loop al hacer login/refresh.
let _workerToken = null;

// Bump centralizado del counter via RPC atómico de Supabase.
// Usable desde cualquier path (worker rapidapi, worker apollo, popup, etc.).
// Race-safe: el RPC bump_api_counter usa upsert + cálculo en la DB.
async function bumpApiCounterRPC(provider, n = 1) {
  if (!n || n <= 0) return;
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/rpc/bump_api_counter`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${BACKEND_BEARER || _workerToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ provider, n }),
    });
  } catch {}
}

async function rapidFetchWithRetry(url, headers, timeout = 8000) {
  let lastErr = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      // Pre-check del cap diario global (evita facturar si ya pasamos el límite)
      if (_rapidCapReached) return { __error4xx: "daily_cap_reached" };
      _rapidGlobalCounter++;
      const res = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
      if (res.ok) {
        // Bump RPC atomic — fire-and-forget para no bloquear el response
        bumpApiCounterRPC("rapidapi", 1);
        return await res.json();
      }
      // 4xx (incluido 429) = NO retry. Cada hit cuesta plata, no pagamos por error.
      // Devuelvo __error4xx para que el caller NO active fallback (mismo plan, mismo resultado).
      if (res.status >= 400 && res.status < 500) {
        return { __error4xx: `HTTP ${res.status}${res.status === 429 ? " (rate limited)" : ""}` };
      }
      // Solo 5xx llega acá → retry una vez (los 5xx no facturan en RapidAPI)
      lastErr = `HTTP ${res.status}`;
    } catch (e) {
      lastErr = e.message;
    }
    if (attempt < 1) await sleep(800);
  }
  return { __error: lastErr }; // 5xx exhaustivo o network → caller puede intentar fallback
}

// Counter en memoria de RapidAPI hits de la sesión actual de Railway.
// Se persiste a Supabase periódicamente (toolbar_config.rapidapi_calls_today).
let _rapidGlobalCounter = 0;
let _rapidCapReached    = false;

async function getTrafficData(domain, rapidApiKey) {
  const headers = { "x-rapidapi-key": rapidApiKey, "x-rapidapi-host": "website-insights.p.rapidapi.com" };

  // REGLA DE ORO: cache compartida 90 días en Supabase. Antes de gastar 1 hit
  // a RapidAPI, chequeamos si ya tenemos data fresca de este dominio.
  const cleanD = cleanDomain(domain);
  const cached = await getTrafficCacheServer(cleanD);
  if (cached) {
    log(`  💾 traffic cache HIT ${cleanD} (sin gastar hit)`);
    return {
      visits:        cached.rawVisits || cached.visits || 0,
      pagesPerVisit: cached.pagesPerVisit || null,
      topCountry:    cached.topCountries?.[0]?.code ? COUNTRY_CODES[cached.topCountries[0].code] || cached.topCountries[0].code : null,
      error:         null,
      fromCache:     true,
    };
  }

  try {
    // Primary: /all-insights (devuelve Visits + TopCountries + Category en una sola llamada)
    let data = await rapidFetchWithRetry(
      `https://website-insights.p.rapidapi.com/all-insights?domain=${encodeURIComponent(domain)}`,
      headers
    );
    // Fallback: /traffic SOLO si /all-insights falló por 5xx/network (no factura).
    // Si fue 4xx (404/403/429), NO insistir — el endpoint hermano del mismo plan
    // tirará el mismo error y solo gastamos otra request facturada.
    if (data?.__error && !data.__error4xx) {
      data = await rapidFetchWithRetry(
        `https://website-insights.p.rapidapi.com/traffic?domain=${encodeURIComponent(domain)}`,
        headers
      );
    }
    if (!data) {
      log(`  ⚠️ getTrafficData ${domain}: response null (key vacía o servicio caído)`);
      return { visits: null, topCountry: null, error: "null_response" };
    }
    if (data.__error4xx) {
      log(`  ⚠️ getTrafficData ${domain}: ${data.__error4xx}`);
      return { visits: null, topCountry: null, error: data.__error4xx };
    }
    if (data.__error) {
      log(`  ⚠️ getTrafficData ${domain}: ${data.__error}`);
      return { visits: null, topCountry: null, error: data.__error };
    }

    // ── Adaptador del shape nuevo de website-insights ──────────
    // Visits puede venir como: object {YYYY-MM-DD: n}, array [{date,value}],
    // number directo, o string "1.2M". Y puede estar en data.Traffic.Visits
    // O directamente en data.Visits (shape v3 que apareció 2026-05-12).
    let tv = data.Traffic?.Visits;
    if (tv == null) tv = data.Visits;
    // Si Visits viene como objeto/array crudo, limpiamos data.Visits para
    // re-procesarlo abajo (sino el `data?.Visits` final lo lee como objeto).
    if (tv != null && typeof tv !== "number" && typeof tv !== "string") {
      data.Visits = null;
    }
    if (tv != null) {
      if (typeof tv === "number") {
        data.Visits = tv;
      } else if (typeof tv === "string") {
        // string puede ser "1234567" o "1.2M". parseHumanNumber para formatos.
        const m = tv.match(/^([\d.]+)\s*([KMB]?)/i);
        if (m) {
          const n = parseFloat(m[1]) || 0;
          const mult = ({ K:1e3, M:1e6, B:1e9 })[m[2]?.toUpperCase()] || 1;
          data.Visits = n * mult;
        }
      } else if (Array.isArray(tv) && tv.length) {
        // array [{date,value}] o [{Date,Value}] o [{d,v}] — tomar más reciente
        const sorted = [...tv].sort((a, b) => {
          const da = a.date || a.Date || a.d || "";
          const db = b.date || b.Date || b.d || "";
          return db.localeCompare(da);
        });
        const first = sorted[0];
        data.Visits = parseFloat(first?.value || first?.Value || first?.v || 0) || 0;
      } else if (typeof tv === "object") {
        const dates = Object.keys(tv).sort().reverse();
        if (dates.length) data.Visits = parseFloat(tv[dates[0]]) || 0;
      }
    }
    // TopCountries: convertir TopCountryShares {US: 0.7, ...} → array {CountryCode, Share}
    // Buscamos en orden: data.Traffic.TopCountryShares, data.TopCountryShares,
    // data.TopCountries (top-level shape v3 que ya viene como array OK).
    const tcRaw = data.Traffic?.TopCountryShares || data.TopCountryShares;
    if (tcRaw && typeof tcRaw === "object" && !Array.isArray(tcRaw)) {
      data.TopCountries = Object.entries(tcRaw)
        .map(([code, share]) => ({ CountryCode: code, Share: parseFloat(share) || 0 }))
        .sort((a, b) => b.Share - a.Share);
    }
    // Si data.TopCountries ya viene como array (shape v3), normalizamos shape.
    if (Array.isArray(data.TopCountries)) {
      data.TopCountries = data.TopCountries.map(c => ({
        CountryCode: c?.CountryCode || c?.countryCode || c?.Country || c?.country || c?.code || "",
        Share:       parseFloat(c?.Share || c?.share || c?.value || 0) || 0,
      })).filter(c => c.CountryCode);
    }
    // Category bajo WebsiteDetails
    if (data.WebsiteDetails?.Category && !data.Category) data.Category = data.WebsiteDetails.Category;

    const visits = data?.Visits || data?.visits || data?.pageViews || data?.PageViews || null;

    // Log diagnóstico si el shape de la API cambió y no extrajimos visits
    if (!visits) {
      const sampleKeys = Object.keys(data || {}).slice(0, 8).join(",");
      // Dump del shape REAL de Visits + Traffic para debug
      const visitsShape = JSON.stringify(data.Visits ?? null).substring(0, 300);
      const trafficVisitsShape = JSON.stringify(data.Traffic?.Visits ?? null).substring(0, 300);
      log(`  ⚠️ getTrafficData ${domain}: sin visits. keys=[${sampleKeys}] | data.Visits=${visitsShape} | data.Traffic.Visits=${trafficVisitsShape}`);
    }

    // Pages per visit del nuevo shape (Traffic.Engagement.PagesPerVisit) o legacy (PagePerVisit)
    const pagesPerVisit = data?.Traffic?.Engagement?.PagesPerVisit
                       || data?.PagePerVisit || data?.PagesPerVisit
                       || data?.pagesPerVisit || null;

    // Extract top country del response. NO llamamos /countries fallback — desde
    // el switch a website-insights, el GEO viene siempre inline en /all-insights.
    // Si no viene → el caller usa fallback gratis (TLD + Cloudflare Radar hint).
    let topCountry = null;
    let topCountries3 = []; // ISO 2-letter codes top 3 (para filtro amplio del agente)
    const inlineList = data?.TopCountries || data?.Countries || data?.countries
                    || data?.topCountryShares || data?.CountryShares || [];
    if (Array.isArray(inlineList) && inlineList.length) {
      for (const c of inlineList.slice(0, 3)) {
        const code = (c?.CountryCode || c?.countryCode || c?.Country || c?.country || "").toUpperCase().slice(0, 2);
        if (code) topCountries3.push(code);
      }
      if (topCountries3[0]) topCountry = COUNTRY_CODES[topCountries3[0]] || topCountries3[0];
    }

    // Guardar en cache compartida para próximas consultas (regla de oro 90 días)
    if (visits) {
      saveTrafficCacheServer(cleanD, {
        rawVisits:     visits,
        visits,
        pagesPerVisit,
        pageViews:     pagesPerVisit ? Math.round(visits * pagesPerVisit) : null,
        topCountries:  topCountry ? [{ code: Object.keys(COUNTRY_CODES).find(k => COUNTRY_CODES[k] === topCountry) || topCountry, name: topCountry, share: 0 }] : [],
        category:      data?.WebsiteDetails?.Category || data?.Category || "",
      }).catch(() => {});
    }
    return { visits, pagesPerVisit, topCountry, topCountries3, error: null };
  } catch (e) { return { visits: null, pagesPerVisit: null, topCountry: null, topCountries3: [], error: e.message }; }
}

// Cache helpers de tráfico para el worker (acceso directo a Supabase).
async function getTrafficCacheServer(domain) {
  if (!domain) return null;
  try {
    const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 90);
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_traffic_cache?domain=eq.${encodeURIComponent(domain)}&fetched_at=gte.${cutoff.toISOString()}&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows.length ? rows[0].data : null;
  } catch { return null; }
}

async function saveTrafficCacheServer(domain, data) {
  if (!domain) return;
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_traffic_cache`, {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "apikey":        SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${BACKEND_BEARER}`,
        "Prefer":        "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({ domain, data, fetched_at: new Date().toISOString() }),
    });
  } catch {}
}

async function findSimilarSites(domain, rapidApiKey) {
  // REGLA DE ORO: cache compartida 90 días en Supabase. Antes de gastar 1 hit
  // a RapidAPI, chequeamos si ya tenemos los similar-sites de este dominio.
  const cleanD = cleanDomain(domain);
  const cachedSimilar = await getSimilarSitesCacheServer(cleanD);
  if (cachedSimilar) {
    log(`  💾 similar-sites cache HIT ${cleanD} (${cachedSimilar.length} sitios — sin gastar hit)`);
    return cachedSimilar.filter(d => isDomainAllowed(d));
  }
  const [swSites, ssSites] = await Promise.all([
    (async () => {
      if (_rapidCapReached) return [];
      try {
        _rapidGlobalCounter++;
        const res = await fetch(
          `https://website-insights.p.rapidapi.com/similar-sites?domain=${encodeURIComponent(domain)}`,
          {
            headers: {
              "x-rapidapi-key":  rapidApiKey,
              "x-rapidapi-host": "website-insights.p.rapidapi.com",
            },
            signal: AbortSignal.timeout(8000),
          }
        );
        if (!res.ok) return [];
        const data = await res.json();
        const raw = data?.similar_sites || data?.similarSites || data?.sites
                  || data?.related_sites || data?.data?.similar_sites || data?.data || [];
        const sites = Array.isArray(raw) ? raw : [];
        return sites
          .map(s => cleanDomain(s?.domain || s?.url || s?.name || (typeof s === "string" ? s : "")))
          .filter(d => isDomainAllowed(d));
      } catch { return []; }
    })(),
    (async () => {
      try {
        const clean = domain.replace(/^www\./, "").toLowerCase();
        const res = await fetch(`https://www.similarsites.com/site/${encodeURIComponent(clean)}`, {
          headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" },
          signal: AbortSignal.timeout(10000),
        });
        if (!res.ok) return [];
        const html = await res.text();
        const match = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/);
        if (!match) return [];
        const json = JSON.parse(match[1]);
        const domains = [];
        function search(obj) {
          if (!obj || typeof obj !== "object") return;
          if (Array.isArray(obj)) {
            for (const item of obj) {
              if (item && typeof item === "object") {
                const d = item.domain || item.Domain || item.url || item.site || item.hostname;
                if (d && typeof d === "string" && d.includes(".") && !d.includes("/")) {
                  const cd = d.replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "").toLowerCase();
                  if (cd && cd !== clean) domains.push(cd);
                }
                search(item);
              }
            }
          } else {
            for (const val of Object.values(obj)) search(val);
          }
        }
        search(json);
        return [...new Set(domains)].slice(0, 20).filter(d => isDomainAllowed(d));
      } catch { return []; }
    })(),
  ]);

  const merged = [...new Set([...swSites, ...ssSites])];
  // Persistir en cache para próximas consultas (regla de oro)
  if (merged.length) await saveSimilarSitesCacheServer(cleanD, merged).catch(() => {});
  return merged;
}

// Cache helpers para el worker (acceso directo a Supabase, sin import client-side)
async function getSimilarSitesCacheServer(domain) {
  if (!domain) return null;
  try {
    const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 90);
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_similar_sites_cache?domain=eq.${encodeURIComponent(domain)}&fetched_at=gte.${cutoff.toISOString()}&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows.length && Array.isArray(rows[0].sites) ? rows[0].sites : null;
  } catch { return null; }
}

async function saveSimilarSitesCacheServer(domain, sites) {
  if (!domain) return;
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_similar_sites_cache`, {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "apikey":        SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${BACKEND_BEARER}`,
        "Prefer":        "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({ domain, sites, fetched_at: new Date().toISOString() }),
    });
  } catch {}
}

const APOLLO_GOOD_STATUSES = new Set(["verified", "likely", "guessed"]);

// Apollo cache (TTL 7d) en Supabase — compartido con popup.
// Saves cost: si worker o popup ya lo procesaron en los últimos 7d, esta call cuesta 0.
const APOLLO_CACHE_TTL_DAYS = 7;
async function getApolloCacheServer(token, domain) {
  const d = (domain || "").toLowerCase().replace(/^www\./, "");
  if (!d) return null;
  try {
    const cutoff = new Date(Date.now() - APOLLO_CACHE_TTL_DAYS * 86_400_000).toISOString();
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_apollo_cache?domain=eq.${encodeURIComponent(d)}&fetched_at=gte.${cutoff}&select=data&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows?.[0]?.data || null;
  } catch { return null; }
}
async function saveApolloCacheServer(token, domain, data) {
  const d = (domain || "").toLowerCase().replace(/^www\./, "");
  if (!d || !data) return;
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_apollo_cache`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({ domain: d, data, fetched_at: new Date().toISOString() }),
    });
  } catch {}
}

async function findAllEmails(domain, apolloApiKey, token = null) {
  if (!apolloApiKey) return [];
  // Helper: attach contact_name property al array (caller puede leer arr.contact_name).
  // Mantiene compatibilidad — sites que solo iteran emails siguen funcionando.
  const _attach = (arr, name) => { if (name) arr.contact_name = name; return arr; };

  // Cache check: si popup o worker ya pegó Apollo en los últimos 7d, retornar lo cacheado.
  // Cache puede tener shape worker {emails, contact_name} o popup {email, people:[...]}.
  if (token) {
    const cached = await getApolloCacheServer(token, domain);
    if (cached) {
      if (Array.isArray(cached.emails)) return _attach(cached.emails.slice(), cached.contact_name);
      if (Array.isArray(cached.people)) {
        const goods = cached.people.filter(p => p.email && APOLLO_GOOD_STATUSES.has(p.email_status));
        if (goods.length) {
          const first = goods[0];
          const name  = `${first.first_name || ""} ${first.last_name || ""}`.trim();
          return _attach([...new Set(goods.map(p => p.email))], name);
        }
      }
      if (cached.email) return _attach([cached.email], cached.contact_name);
    }
  }

  const emails = [];
  let firstContactName = "";
  // /v1/people/match was deprecated — now /v1/mixed_people/search is ALSO deprecated.
  // Current endpoint: /v1/mixed_people/api_search (per Apollo docs 2026-04)
  try {
    const res = await fetch("https://api.apollo.io/v1/mixed_people/api_search", {
      method: "POST",
      headers: { "X-Api-Key": apolloApiKey, "Content-Type": "application/json" },
      body: JSON.stringify({
        q_organization_domains_list: [domain],
        person_titles: ["CEO","founder","co-founder","owner","publisher","editor in chief","managing editor","director","head of digital","VP"],
        per_page: 5,
        page: 1,
      }),
      signal: AbortSignal.timeout(12000),
    });
    if (res.ok) {
      const data   = await res.json();
      const people = Array.isArray(data?.people) ? data.people : [];
      for (const p of people) {
        if (p.email && APOLLO_GOOD_STATUSES.has(p.email_status)) {
          emails.push(p.email);
          if (!firstContactName) firstContactName = `${p.first_name || ""} ${p.last_name || ""}`.trim();
        }
      }
    }
  } catch {}

  const unique = [...new Set(emails)];
  // Cache write — incluye contact_name para que próximos lookups lo levanten gratis
  if (token) saveApolloCacheServer(token, domain, { emails: unique, contact_name: firstContactName, source: "worker" }).catch(() => {});
  return _attach(unique, firstContactName);
}

// ── findBestApolloEmail: estrategia balanceada para autoimport/autopilot ──
// Devuelve 1 email max + contact_name. Lógica:
//   1. Cache 7d hit → return (0 credits)
//   2. Free search → si hay verified/likely → return (0 credits)
//   3. Si traffic ≥ APOLLO_UNLOCK_MIN_TRAFFIC y cap < APOLLO_MONTHLY_HARD_CAP
//      → unlock TOP 1 person via /v1/people/match (1 credit)
//   4. Si no aplica unlock → return null (caller usa scraping fallback)
const APOLLO_UNLOCK_MIN_TRAFFIC = 500_000;
async function findBestApolloEmail(domain, apolloKey, token, { traffic = 0, allowUnlock = true } = {}) {
  if (!apolloKey || !domain) return null;
  // Audit P2 fix: Apollo espera dominio limpio sin www. Antes lookups con
  // "www.sitio.com" fallaban silenciosamente.
  domain = domain.replace(/^https?:\/\//, "").replace(/^www\./, "").toLowerCase().trim();

  // 1. Cache 7d
  if (token) {
    const cached = await getApolloCacheServer(token, domain);
    if (cached) {
      const arr = Array.isArray(cached.emails) ? cached.emails : [];
      if (arr.length) return { email: arr[0], contact_name: cached.contact_name || "", source: "cache" };
      if (cached.email) return { email: cached.email, contact_name: cached.contact_name || "", source: "cache" };
    }
  }

  // 2. Free search (no reveal — 0 credits)
  let people = [];
  try {
    const res = await fetch("https://api.apollo.io/v1/mixed_people/api_search", {
      method: "POST",
      headers: { "X-Api-Key": apolloKey, "Content-Type": "application/json" },
      body: JSON.stringify({
        q_organization_domains_list: [domain],
        person_titles: ["CEO","founder","co-founder","owner","publisher","editor in chief","managing editor","director","head of digital","VP","marketing","commercial"],
        per_page: 5, page: 1,
      }),
      signal: AbortSignal.timeout(12000),
    });
    if (res.ok) {
      const data = await res.json();
      people = Array.isArray(data?.people) ? data.people : [];
    }
  } catch { return null; }

  // 3. ¿Verified gratis?
  const verified = people.find(p => p.email && APOLLO_GOOD_STATUSES.has(p.email_status));
  if (verified) {
    const phone = _extractApolloPhone(verified);
    const result = {
      email: verified.email,
      contact_name: `${verified.first_name||""} ${verified.last_name||""}`.trim(),
      phone,
      source: "free_verified",
    };
    if (token) saveApolloCacheServer(token, domain, { emails: [result.email], contact_name: result.contact_name, phone, source: "worker_free" }).catch(() => {});
    return result;
  }

  // 4. ¿Vale la pena unlock? Solo si tráfico alto y allowUnlock y bajo cap mensual
  if (!allowUnlock || traffic < APOLLO_UNLOCK_MIN_TRAFFIC) return null;

  // Hard cap check: leer apollo_calls_month antes de gastar credit
  try {
    const usage = await getApolloUsageToday(token);
    if ((usage.usedThisMonth || 0) >= APOLLO_MONTHLY_HARD_CAP) {
      log(`  ⚠ Apollo cap ${APOLLO_MONTHLY_HARD_CAP} alcanzado (${usage.usedThisMonth}/${usage.monthLimit}) — skip unlock ${domain}`);
      return null;
    }
  } catch {}

  // 5. Unlock top 1
  const target = people[0];
  if (!target?.id) return null;
  try {
    const unlock = await fetch("https://api.apollo.io/v1/people/match", {
      method: "POST",
      headers: { "X-Api-Key": apolloKey, "Content-Type": "application/json" },
      body: JSON.stringify({ id: target.id, reveal_personal_emails: true }),
      signal: AbortSignal.timeout(12000),
    });
    if (!unlock.ok) return null;
    const data = await unlock.json();
    const person = data?.person;
    if (!person?.email) return null;
    // Bump apollo_calls_month +1 (este endpoint cuesta 1 credit)
    bumpApolloUnlocks(token, 1).catch(() => {});
    const phone = _extractApolloPhone(person);
    log(`  💎 Apollo unlock ${domain} → ${person.email}${phone ? " 📞" : ""} (1 credit)`);
    const result = {
      email: person.email,
      contact_name: `${person.first_name||""} ${person.last_name||""}`.trim(),
      phone,
      source: "unlocked",
    };
    if (token) saveApolloCacheServer(token, domain, { emails: [result.email], contact_name: result.contact_name, phone, source: "worker_unlocked" }).catch(() => {});
    return result;
  } catch { return null; }
}

// Extrae el primer teléfono útil de un objeto Apollo person.
// Apollo devuelve phone_numbers, sanitized_phone, mobile_phone, work_direct_phone, etc.
function _extractApolloPhone(person) {
  if (!person) return "";
  const candidates = [
    person.sanitized_phone,
    person.phone,
    person.mobile_phone,
    person.work_direct_phone,
    person.home_phone,
    Array.isArray(person.phone_numbers) ? person.phone_numbers[0]?.sanitized_number || person.phone_numbers[0]?.raw_number : null,
  ].filter(Boolean);
  return (candidates[0] || "").toString().trim();
}

// Increment Apollo monthly counter (reusa apollo_calls_month).
async function bumpApolloUnlocks(token, n = 1) {
  // Delegate al RPC atómico — no más read-modify-write race-prone.
  await bumpApiCounterRPC("apollo", n);
}

// ── Email scraping fallback (server-side HTTP) ────────────────

const EMAIL_REGEX  = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6}(?=\s|$|[^a-zA-Z])/g;
const IGNORE_EMAIL = ["example.com","domain.com","sentry.io","google.com","w3.org","schema.org","cloudflare.com"];

// Limpieza de prefijo "C" pegado al email — artifact común del scrape de HTML
// donde "Contato: foo@bar.com" se captura como "Cfoo@bar.com" porque la regex
// agarró la C de "Contato:" cuando no había espacio. Casos reales 2026-05-13:
//   - Cpublicidade@autoracing.com.br → publicidade@autoracing.com.br
//   - Cbasketball-video.com@whoisprotectservice.net → basketball-video.com@... (queda invalido por TLD, se descarta)
//   - Cluciano@phonecall.com.br → luciano@phonecall.com.br
function _stripScrapePrefix(email) {
  if (!email || typeof email !== "string") return email;
  // Decode URL-encoded chars (%20=space, %09=tab) y trim whitespace al inicio.
  // Caso real 2026-05-14: "%20info@ewdifh.com" salió enviado por el agente.
  try { email = decodeURIComponent(email); } catch {}
  email = email.replace(/^[\s​ ]+/, "").trim();
  const [local, domain] = email.split("@");
  if (!local || !domain) return email;
  // Solo si local-part empieza con 1 letra MAYÚSCULA seguida de letra minúscula:
  // "Cpublicidade", "Cluciano". Y la versión sin la letra debe seguir matcheando
  // formato normal (5+ chars y no es la inicial de "carlos" ej.)
  if (/^[A-Z][a-z]{4,}/.test(local)) {
    const stripped = local.slice(1).toLowerCase();
    // Solo limpiar si la versión strip es una palabra reconocible (rol/keyword común)
    // o tiene patrón firstname normal sin la mayúscula al inicio.
    const KNOWN_LOCAL = /^(publicidade|publicidad|contato|contatto|contact|contacto|info|hola|atendimento|suporte|soporte|prensa|press|imprensa|sales|ventas|marketing|comercial|info|hello|hi|news|press|admin|director|gerente|owner|founder|ceo|cto|editor|redaccion|redacao)/i;
    const FIRSTNAME = /^[a-z]{3,12}$/i; // single lowercase word that could be firstname
    if (KNOWN_LOCAL.test(stripped) || FIRSTNAME.test(stripped)) {
      return `${stripped}@${domain}`;
    }
  }
  return email.toLowerCase();
}

// Cloudflare Email Obfuscation decoder.
// CF Pro reemplaza emails con <a class="__cf_email__" data-cfemail="HEX">[email protected]</a>.
// El HEX es: primer byte = XOR key, resto = caracteres del email XOR'd con el key.
function _decodeCfEmail(hex) {
  try {
    const key = parseInt(hex.slice(0, 2), 16);
    let email = "";
    for (let i = 2; i < hex.length; i += 2) {
      email += String.fromCharCode(parseInt(hex.slice(i, i + 2), 16) ^ key);
    }
    return email;
  } catch { return null; }
}

function extractEmailsFromHtml(html) {
  const clean = html
    .replace(/&#64;|&#x40;/gi, "@").replace(/&#46;|&#x2e;/gi, ".")
    .replace(/\[\s*at\s*\]/gi, "@").replace(/\(\s*at\s*\)/gi, "@")
    .replace(/\barroba\b/gi,   "@").replace(/\bpunto\b/gi,    ".");
  const collected = new Set();
  // 1) Regex tradicional
  (clean.match(EMAIL_REGEX) || []).forEach(e => collected.add(_stripScrapePrefix(e).toLowerCase()));
  // 2) Cloudflare data-cfemail decoder — gap común en sitios con CF Pro
  const cfMatches = html.matchAll(/data-cfemail=["']([a-f0-9]+)["']/gi);
  for (const m of cfMatches) {
    const decoded = _decodeCfEmail(m[1]);
    if (decoded && decoded.includes("@")) collected.add(decoded.toLowerCase());
  }
  // 3) JSON-LD schema.org "email": "x@y"
  const jsonLdMatches = html.matchAll(/"email"\s*:\s*"([^"]+@[^"]+)"/gi);
  for (const m of jsonLdMatches) collected.add(m[1].toLowerCase());
  return [...collected].filter(e => {
    const lower = e.toLowerCase();
    if (IGNORE_EMAIL.some(p => lower.includes(p))) return false;
    const parts = e.split("@");
    if (parts.length !== 2) return false;
    const tld = parts[1].split(".").pop();
    return tld && tld.length >= 2 && tld.length <= 6;
  });
}

async function scrapeEmailsForDomain(domain) {
  // Trae emails de muchas fuentes con concurrencia limitada (4 a la vez).
  // Usa User-Agent real (Chrome) para evitar anti-bot blocks comunes.
  const emails = new Set();
  const base   = `https://${domain}`;
  const cleanDomain = domain.replace(/^www\./, "");
  // Chrome real para evitar bloqueos por User-Agent de bot
  const uaChrome = { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36" };
  const uaMobile = { "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1" };

  const tryFetch = async (url, timeout = 5000, mobile = false) => {
    try {
      const r = await fetch(url, { headers: mobile ? uaMobile : uaChrome, signal: AbortSignal.timeout(timeout) });
      if (!r.ok) return;
      const html = await r.text();
      // 1) regex emails en HTML general
      extractEmailsFromHtml(html).forEach(e => emails.add(e));
      // 2) explicit mailto: hrefs (raramente missed pero a veces hay solo ahí)
      const mailtoMatches = html.matchAll(/mailto:([^"'\s<>?]+@[^"'\s<>?]+)/gi);
      for (const m of mailtoMatches) {
        const clean = m[1].toLowerCase().split("?")[0];
        if (clean.includes("@")) emails.add(clean);
      }
    } catch {}
  };

  // 20 paths potenciales — sites de news/blogs típicos tienen variantes diversas
  const paths = [
    "", // home
    "/contact", "/contact-us", "/contactus", "/contacto", "/contactanos", "/contacto-nos",
    "/about", "/about-us", "/aboutus", "/sobre", "/sobre-nosotros", "/quienes-somos",
    "/team", "/equipo", "/equipe", "/staff", "/nosotros",
    "/advertise", "/advertising", "/publicidad", "/publicidade", "/anunciantes", "/anunciar",
    "/redaccion", "/redacao", "/editorial", "/noticias",
    "/aviso-legal", "/legal", "/impressum", "/politica-privacidad", "/privacy",
    "/footer", "/site-map", "/sitemap",
  ];
  const internalTargets = paths.map(p => `${base}${p}`);

  // Fuentes externas (WHOIS / informer) — pueden estar bloqueadas por rate limit
  const externalTargets = [
    `https://website.informer.com/${cleanDomain}`,
    `https://who.is/whois/${cleanDomain}`,
  ];

  // Procesar en chunks de 4 — concurrencia limitada para no saturar Railway
  const CONCURRENT = 4;
  const all = [...internalTargets, ...externalTargets];
  for (let i = 0; i < all.length; i += CONCURRENT) {
    const chunk = all.slice(i, i + CONCURRENT);
    await Promise.all(chunk.map(url => tryFetch(url, url.match(/informer|who\.is/) ? 6000 : 4000)));
    // Early-stop si ya tenemos varios emails buenos (no garbage)
    if (emails.size >= 10) break;
  }

  // Última chance — si NADA encontrado, probar mobile UA en home (algunos sites cambian)
  if (emails.size === 0) {
    await tryFetch(base, 5000, true);
  }

  return [...emails];
}

// ── Page intelligence ─────────────────────────────────────────

async function fetchPageContent(domain) {
  try {
    const res = await fetch(`https://${domain}`, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" },
      signal: AbortSignal.timeout(9000),
      redirect: "follow",
    });
    if (!res.ok) return null;
    const html = await res.text();

    const title = (html.match(/<title[^>]*>([^<]{1,120})<\/title>/i) || [])[1]?.trim() || "";
    const desc  = (
      html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']{1,300})["']/i) ||
      html.match(/<meta[^>]+content=["']([^"']{1,300})["'][^>]+name=["']description["']/i) ||
      html.match(/<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']{1,300})["']/i) ||
      html.match(/<meta[^>]+content=["']([^"']{1,300})["'][^>]+property=["']og:description["']/i) ||
      [])[1]?.trim() || "";

    // Ad networks detected in page source — ADEQ partner networks only
    const adNetworks = [];
    if (/sparteo\.com/i.test(html))                                        adNetworks.push("Sparteo");
    if (/seedtag\.com/i.test(html))                                        adNetworks.push("Seedtag");
    if (/taboola/i.test(html))                                             adNetworks.push("Taboola");
    if (/missena\.com/i.test(html))                                        adNetworks.push("Missena");
    if (/viads\.com|viads\.io/i.test(html))                               adNetworks.push("Viads");
    if (/mgid\.com/i.test(html))                                           adNetworks.push("MGID");
    if (/clever-advertising\.com|cleveradvertising\.com/i.test(html))     adNetworks.push("Clever Advertising");
    if (/vidoomy\.com/i.test(html))                                        adNetworks.push("Vidoomy");
    if (/vidverto\.com/i.test(html))                                       adNetworks.push("Vidverto");
    if (/ezoic\.com|ezojs\.com|ez\.ai/i.test(html))                      adNetworks.push("Ezoic");
    if (/clickio\.com|clickio\.net/i.test(html))                          adNetworks.push("Clickio");
    if (/360playvid\.com/i.test(html))                                     adNetworks.push("360Playvid");
    if (/truvid\.com/i.test(html))                                         adNetworks.push("Truvid");
    if (/optad360\.com/i.test(html))                                       adNetworks.push("Optad360");
    if (/embimedia\.com|embi\.media/i.test(html))                         adNetworks.push("Embi Media");
    if (/snigel\.com/i.test(html))                                         adNetworks.push("Snigel");

    // Categoría heurística — keywords en title + description + URL (gratis, sin API call)
    // Adult/Streaming PRIMERO porque scoreWebsite los usa como gates duros (descarte total).
    const textForCategory = `${title} ${desc} ${domain}`.toLowerCase();
    let category = "other";
    if      (/\b(porn|xxx|sex|adult|escort|fetish|hentai|onlyfans|pornhub|xvideos|xnxx|redtube|cam[\s-]?girl|webcam|nude|nudes|brazzer)\b|videos?xxx|sexo[\s-]?gratis|chicas[\s-]?desnudas/i.test(textForCategory)) category = "adult";
    else if (/\b(streaming|stream[\s-]?online|cuevana|repelis|pelis24|pelisplus|gnula|magis[\s-]?tv|futbol[\s-]?en[\s-]?vivo|live[\s-]?stream|free[\s-]?movies|watch[\s-]?free|123movies|fmovies|putlocker|soap2day|netflix[\s-]?free|disney[\s-]?free|hbo[\s-]?free)\b|ver[\s-]?(peliculas|series|partidos|futbol)[\s-]?(online|gratis|en[\s-]?vivo)/i.test(textForCategory)) category = "streaming";
    else if (/sport|futbol|futebol|soccer|football|nba|basket|tennis|béisbol|beisbol|liga|mlb|f1|motor|boxeo|boxing/.test(textForCategory)) category = "sports";
    else if (/noticia|news|diario|periódico|periodico|press|journalism|último|ultimo momento|actualidad/.test(textForCategory))            category = "news";
    else if (/finanz|banco|econom|invest|crypto|bolsa|stock|finance|mercad/.test(textForCategory))                                          category = "finance";
    else if (/cine|película|pelicula|movie|film|music|música|música|juego|game|entertain|espectácul|espectacul|farándula|farandula/.test(textForCategory)) category = "entertainment";
    else if (/tech|tecnolog|software|digital|code|program|gadget|computador|hardware/.test(textForCategory))                                category = "technology";
    else if (/salud|health|medic|bienestar|wellness|fitness|diet/.test(textForCategory))                                                    category = "health";
    else if (/viaj|travel|tour|hotel|vacacion|destino|vuel/.test(textForCategory))                                                          category = "travel";
    else if (/casin|apuesta|betting|gambl|poker|bingo|lotería|loteria/.test(textForCategory))                                              category = "gambling";
    else if (/auto|car |coche|vehícul|vehicul|moto|truck|camión|camion/.test(textForCategory))                                             category = "automotive";
    else if (/receta|cocina|comida|food|cook|gastronom/.test(textForCategory))                                                              category = "food";
    else if (/negocio|business|emprend|startup|marketing|seo|empresa/.test(textForCategory))                                                category = "business";

    // Extraer señales de idioma del HTML para detección robusta downstream.
    // Capturamos MULTIPLES señales — más es mejor para cross-validation.
    const htmlLang = (html.match(/<html[^>]+lang=["']([a-z]{2})/i) || [])[1] || "";
    const ogLocale = (html.match(/<meta[^>]+property=["']og:locale["'][^>]+content=["']([a-z]{2})/i) || [])[1] || "";
    // hreflang — sitios multi-idioma lo declaran. Tomamos el "x-default" o el primero.
    const hreflangs = [...html.matchAll(/<link[^>]+rel=["']alternate["'][^>]+hreflang=["']([a-z]{2})/gi)].map(m => m[1].toLowerCase());
    const hreflang = hreflangs[0] || "";
    // JSON-LD schema.org — muchas news/articles ponen inLanguage
    const jsonLdLang = (html.match(/"inLanguage"\s*:\s*"([a-z]{2})/i) || [])[1] || "";
    // URL path pattern: /es/, /pt-BR/, /it_IT/
    const pathLang = (domain.match(/\/(es|pt|it|en|ar|fr|de)([\-_][a-z]{2})?\//i) || [])[1] || "";
    // Sample de texto AMPLIO (10K chars) para análisis heurístico fuerte
    const textSample = (title + " " + desc + " " + html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").substring(0, 10000)).toLowerCase();

    return {
      title: title.slice(0, 100),
      description: desc.slice(0, 280),
      adNetworks, category,
      htmlLang, ogLocale, hreflang, jsonLdLang, pathLang,
      textSample,
    };
  } catch { return null; }
}

// ── DETECCIÓN ROBUSTA DE IDIOMA — mirror del popup _detectLangFromText ──
// Cascada: <html lang> → og:locale → texto heurístico → GEO → TLD → "en"
// Solo retorna idiomas SOPORTADOS por templates: es/en/it/pt/ar.
// Crítico: el agente debe acertar idioma SIEMPRE — un mail en idioma equivocado
// quema el dominio para siempre.
const SUPPORTED_AGENT_LANGS = new Set(["es", "en", "it", "pt", "ar"]);

const TLD_TO_LANG_AGENT = {
  ar:"es", mx:"es", co:"es", cl:"es", pe:"es", uy:"es", py:"es", bo:"es",
  ec:"es", ve:"es", do:"es", cr:"es", pa:"es", gt:"es", hn:"es", sv:"es",
  ni:"es", cu:"es", pr:"es", es:"es",
  br:"pt", pt:"pt",
  it:"it",
  ae:"ar", sa:"ar", eg:"ar", ma:"ar",
  com:"en", net:"en", org:"en", io:"en", uk:"en", us:"en", au:"en", nz:"en",
};

const GEO_TO_LANG_AGENT = {
  Argentina:"es", Mexico:"es", Colombia:"es", Chile:"es", Peru:"es", Uruguay:"es",
  Paraguay:"es", Bolivia:"es", Ecuador:"es", Venezuela:"es", "Dominican Republic":"es",
  "Costa Rica":"es", Panama:"es", Guatemala:"es", Honduras:"es", "El Salvador":"es",
  Nicaragua:"es", Cuba:"es", "Puerto Rico":"es", Spain:"es",
  Brazil:"pt", Portugal:"pt",
  Italy:"it", Switzerland:"it",
  "United Arab Emirates":"ar", "Saudi Arabia":"ar", Egypt:"ar", Morocco:"ar",
  AR:"es", MX:"es", CO:"es", CL:"es", PE:"es", UY:"es", PY:"es", BO:"es",
  EC:"es", VE:"es", DO:"es", CR:"es", PA:"es", GT:"es", HN:"es", SV:"es",
  NI:"es", CU:"es", PR:"es", ES:"es",
  BR:"pt", PT:"pt",
  IT:"it", CH:"it",
  AE:"ar", SA:"ar", EG:"ar", MA:"ar",
};

function _detectLangFromText(text) {
  if (!text || text.length < 30) return { lang: null, confidence: "none", scores: {} };
  const t = text.toLowerCase();
  // Caracteres únicos = señal fuerte
  if (/[؀-ۿ]/.test(text)) return { lang: "ar", confidence: "high", scores: { ar: 999 } };
  // Stopwords + palabras frecuentes — listas EXTENDIDAS para mayor precisión
  const markers = {
    es: /\b(que|los|las|para|por|con|una|del|este|esta|pero|cuando|donde|como|porque|sobre|tambien|también|nuestra|nuestro|hola|gracias|hace|noticias|últimas|video|videos|fútbol|política|economía|deportes|mundo|inicio|contacto|sobre[\s-]?nosotros|aviso|legal|política[\s-]?de[\s-]?privacidad|términos|condiciones|últim|hoy|ayer|mañana|años|días|nuevo|nueva|gran|millones)\b/g,
    pt: /\b(que|não|para|com|uma|por|esse|essa|mas|quando|onde|como|porque|sobre|nossa|nosso|olá|obrigad|dele|dela|você|notícias|notícia|últimas|últim|esportes|política|economia|cidade|brasileir|portuguesa|português|política[\s-]?de[\s-]?privacidade|termos|condições|hoje|ontem|amanhã|anos|dias|nova|grande|milhões)\b/g,
    it: /\b(che|non|per|con|una|del|della|sono|questo|questa|quando|dove|come|perché|sopra|grazie|nostra|nostro|ciao|notizie|ultim|sport|politica|economia|città|italiano|italiana|chi[\s-]?siamo|contatti|privacy|termini|condizioni|oggi|ieri|domani|anni|giorni|nuovo|nuova|grande|milioni)\b/g,
    en: /\b(the|and|that|for|with|this|from|have|been|will|would|could|should|about|which|their|there|where|when|because|hello|thanks|news|latest|video|videos|football|politics|economy|sports|world|home|contact|about[\s-]?us|privacy|terms|conditions|today|yesterday|tomorrow|years|days|new|great|millions)\b/g,
    fr: /\b(que|les|des|pour|avec|une|sur|cette|cet|mais|quand|où|comme|parce|notre|votre|bonjour|merci|nouvelles|aujourd'hui|hier|demain)\b/g,
    de: /\b(der|die|das|und|für|mit|ein|eine|nicht|auch|aber|wenn|wo|wie|weil|über|unsere|unser|hallo|danke|nachrichten|heute|gestern|morgen)\b/g,
  };
  const scores = {};
  let total = 0;
  for (const [lang, re] of Object.entries(markers)) {
    const m = t.match(re);
    scores[lang] = m ? m.length : 0;
    total += scores[lang];
  }
  // Bonus por caracteres únicos (señal muy fuerte)
  if (/[ñáéíóúü¿¡]/.test(text)) scores.es = (scores.es || 0) + 8;
  if (/[ãõçàáâ]/.test(text))    scores.pt = (scores.pt || 0) + 8;
  if (/[àèéìòù]/.test(text))    scores.it = (scores.it || 0) + 5;
  if (/[äöüß]/.test(text))      scores.de = (scores.de || 0) + 5;
  if (total < 5) return { lang: null, confidence: "low", scores };
  const sorted = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  const top = sorted[0];
  const second = sorted[1] || ["", 0];
  const gap = top[1] - second[1];
  // High = gap >= 8 (claro ganador). Medium = gap 3-7. Low = ambiguo.
  const conf = gap >= 8 ? "high" : gap >= 3 ? "medium" : "low";
  return { lang: top[0], confidence: conf, scores, gap };
}

// Cache de detección por dominio — evita re-pagar Claude/re-fetchear HTML
const _domainLangCache = new Map();
const DOMAIN_LANG_CACHE_MAX = 1000;

// Árbitro Claude Haiku — clasificación final cuando heurística es ambigua.
// Cost: ~$0.0005 por call. Cached por dominio.
// Guess de idioma por DOMINIO + GEO cuando no hay HTML disponible. Sólo se usa
// en el path de fallback (fetchPageContent falló). Mucho menos confiable que
// el arbiter normal porque no hay texto, pero supera al "default a EN" puro.
async function _claudeLangByContext(token, domain, geo) {
  if (!domain) return null;
  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/api-proxy`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        provider: "anthropic",
        path: "/v1/messages",
        method: "POST",
        body: {
          model: "claude-haiku-4-5",
          max_tokens: 20,
          system: "Guess the primary content language of a website given ONLY its domain and country code. Reply with a 2-letter ISO code (es/en/pt/it/ar/fr/de/other). If unsure, reply 'other'. No explanation.",
          messages: [{ role: "user", content: `Domain: ${domain}\nCountry: ${geo || "unknown"}` }],
        },
      }),
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) return null;
    const data = await res.json();
    const text = (data?.content?.[0]?.text || "").trim().toLowerCase();
    const m = text.match(/^([a-z]{2})/);
    return m && SUPPORTED_AGENT_LANGS.has(m[1]) ? m[1] : null;
  } catch { return null; }
}

async function _claudeLangArbiter(token, domain, sample) {
  if (!sample || sample.length < 30) return null;
  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/api-proxy`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        provider: "anthropic",
        path: "/v1/messages",
        method: "POST",
        body: {
          model: "claude-haiku-4-5",
          max_tokens: 30,
          system: "You classify the language of website text. Respond ONLY with a 2-letter ISO code (es/en/pt/it/ar/fr/de/other). No explanation.",
          messages: [{ role: "user", content: `Domain: ${domain}\nSample (first 600 chars):\n${sample.substring(0, 600)}` }],
        },
      }),
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) return null;
    const data = await res.json();
    const text = (data?.content?.[0]?.text || "").trim().toLowerCase();
    const m = text.match(/^([a-z]{2})/);
    return m && SUPPORTED_AGENT_LANGS.has(m[1]) ? m[1] : null;
  } catch { return null; }
}

// Sistema de VOTACIÓN — recolecta todos los signals, weighta, decide.
// El text heuristic es la fuente más confiable (lo que el publisher REALMENTE escribió),
// aunque html lang diga otra cosa (sitios mal-declarados son comunes).
// Si la votación es ambigua y tenemos token, llamamos a Claude Haiku como árbitro.
async function detectLanguageRobust({ htmlLang, ogLocale, hreflang, jsonLdLang, pathLang, textSample, geo, domain }, opts = {}) {
  const { token = null, allowClaudeArbiter = true } = opts;
  const cleanDomain = (domain || "").replace(/^www\./, "").toLowerCase();

  // Cache hit SOLO si tenemos textSample (= invocación con datos completos).
  // Si solo tenemos geo/domain (hint cheap), no cache para no envenenar.
  const hasFullData = !!textSample;
  if (cleanDomain && hasFullData && _domainLangCache.has(cleanDomain)) {
    return _domainLangCache.get(cleanDomain);
  }

  // Recolectar votos: cada signal aporta peso al lang detectado.
  const votes = {}; // lang → puntaje
  const reasons = [];
  const addVote = (lang, weight, source) => {
    if (!lang || !SUPPORTED_AGENT_LANGS.has(lang)) return;
    votes[lang] = (votes[lang] || 0) + weight;
    reasons.push(`${source}:${lang}+${weight}`);
  };

  // 1) Texto heurístico (más confiable — lo que el publisher escribió)
  const textRes = _detectLangFromText(textSample || "");
  if (textRes.lang) {
    const weight = textRes.confidence === "high" ? 10 : textRes.confidence === "medium" ? 6 : 3;
    addVote(textRes.lang, weight, `text(${textRes.confidence})`);
  }

  // 2) hreflang del primer link alternate (alta confianza si existe)
  const hl = (hreflang || "").toLowerCase().split("-")[0];
  if (SUPPORTED_AGENT_LANGS.has(hl)) addVote(hl, 8, "hreflang");

  // 3) JSON-LD inLanguage (alta confianza si presente)
  const jl = (jsonLdLang || "").toLowerCase().split("-")[0];
  if (SUPPORTED_AGENT_LANGS.has(jl)) addVote(jl, 8, "jsonld");

  // 4) URL path /es/ /pt-BR/ (cuando aparece, muy confiable)
  const pl = (pathLang || "").toLowerCase().split(/[-_]/)[0];
  if (SUPPORTED_AGENT_LANGS.has(pl)) addVote(pl, 7, "url");

  // 5) <html lang> — peso medio (sitios mal-declarados son comunes)
  const hl2 = (htmlLang || "").toLowerCase().split("-")[0];
  if (SUPPORTED_AGENT_LANGS.has(hl2)) addVote(hl2, 5, "html_lang");

  // 6) og:locale — peso medio
  const og = (ogLocale || "").toLowerCase().split(/[-_]/)[0];
  if (SUPPORTED_AGENT_LANGS.has(og)) addVote(og, 5, "og");

  // 7) GEO → idioma — peso bajo (RapidAPI a veces devuelve geo wrong)
  const geoLang = GEO_TO_LANG_AGENT[geo] || GEO_TO_LANG_AGENT[(geo || "").trim()];
  if (geoLang && SUPPORTED_AGENT_LANGS.has(geoLang)) addVote(geoLang, 3, "geo");

  // 8) TLD — peso bajo (.com domina, poco discriminativo)
  const tld = cleanDomain.split(".").pop() || "";
  const tldLang = TLD_TO_LANG_AGENT[tld];
  if (tldLang && SUPPORTED_AGENT_LANGS.has(tldLang)) addVote(tldLang, 2, "tld");

  // ── Decisión ────────────────────────────────────────────────
  const sorted = Object.entries(votes).sort((a, b) => b[1] - a[1]);
  let result;
  if (sorted.length === 0) {
    result = { lang: "en", source: "default", confidence: "low", reasons: ["no_signals"] };
  } else {
    const winner = sorted[0];
    const runnerUp = sorted[1] || ["", 0];
    const margin = winner[1] - runnerUp[1];
    // Confianza: si winner score >= 10 Y margen >= 5 → high. Si margen < 3 → low (ambiguo).
    const confidence = winner[1] >= 10 && margin >= 5 ? "high" : margin >= 3 ? "medium" : "low";

    // Si confianza baja Y tenemos token + sample → Claude Haiku decide
    if (confidence === "low" && allowClaudeArbiter && token && textSample) {
      const claudeAns = await _claudeLangArbiter(token, cleanDomain, textSample);
      if (claudeAns) {
        result = { lang: claudeAns, source: "claude_arbiter", confidence: "high", reasons: [...reasons, `claude:${claudeAns}`] };
      } else {
        result = { lang: winner[0], source: "voting", confidence, reasons };
      }
    } else {
      result = { lang: winner[0], source: "voting", confidence, reasons };
    }
  }

  // Cache SOLO si fue invocación con full data (textSample) — no las heuristics chea
  if (cleanDomain && hasFullData) {
    if (_domainLangCache.size >= DOMAIN_LANG_CACHE_MAX) {
      const firstKey = _domainLangCache.keys().next().value;
      _domainLangCache.delete(firstKey);
    }
    _domainLangCache.set(cleanDomain, result);
  }
  return result;
}

// ── Blocklist para autopilot — evita dominios que no son targets válidos ───

// Dominios exactos que no queremos prospectar (grandes marcas, tech giants, social media)
const CORPORATE_BLOCKLIST = new Set([
  // Search engines
  "google.com","bing.com","yahoo.com","baidu.com","yandex.ru","duckduckgo.com","ask.com",
  // Social networks
  "facebook.com","twitter.com","x.com","instagram.com","tiktok.com","linkedin.com",
  "reddit.com","pinterest.com","snapchat.com","whatsapp.com","telegram.org","discord.com",
  "tumblr.com","weibo.com","vk.com","quora.com",
  // Tech giants & big brands
  "apple.com","microsoft.com","amazon.com","google.com","alphabet.com","meta.com",
  "netflix.com","spotify.com","adobe.com","ibm.com","oracle.com","sap.com","salesforce.com",
  "hubspot.com","shopify.com","stripe.com","square.com","paypal.com","visa.com","mastercard.com",
  "samsung.com","sony.com","intel.com","amd.com","nvidia.com","dell.com","hp.com","lenovo.com",
  "tesla.com","uber.com","lyft.com","airbnb.com","booking.com","expedia.com","tripadvisor.com",
  // Dev / SaaS
  "github.com","gitlab.com","bitbucket.org","atlassian.com","slack.com","notion.so","figma.com",
  "canva.com","zoom.us","dropbox.com","box.com","wetransfer.com","docusign.com",
  // Content platforms (no publishers independientes)
  "youtube.com","vimeo.com","twitch.tv","medium.com","substack.com","wordpress.com","blogger.com",
  "wikipedia.org","stackoverflow.com","stackexchange.com",
  // Adult (explícito)
  "pornhub.com","xvideos.com","xhamster.com","redtube.com","youporn.com","xnxx.com","brazzers.com",
  "chaturbate.com","onlyfans.com","livejasmin.com","stripchat.com","bongacams.com",
  // E-commerce gigantes
  "ebay.com","alibaba.com","aliexpress.com","mercadolibre.com","mercadolibre.com.ar","mercadolivre.com.br",
  "walmart.com","target.com","bestbuy.com","homedepot.com","costco.com",
  // ChatGPT / AI giants
  "chatgpt.com","openai.com","chat.openai.com","claude.ai","anthropic.com","perplexity.ai","gemini.google.com",
]);

// TLDs o subcadenas que SIEMPRE indican sitios no-prospectables
const BLOCKED_TLDS     = [".edu", ".gov", ".mil", ".ac.uk", ".edu.", ".gov.", ".mil."];
const ADULT_TLDS       = [".xxx", ".adult", ".porn", ".sex"];
const ADULT_KEYWORDS   = ["porn", "xxx", "xvideos", "sexcam", "camgirl", "fuck", "nsfw", "hentai", "escort"];

// ── GEO BLACKLIST — países que NUNCA queremos prospectar (aunque no haya targetGeo) ──
const GEO_BLACKLIST_COUNTRIES = new Set([
  "United States","Canada","Russia","Australia","New Zealand",
]);
// TLDs que corresponden a países blacklisteados — se skipean ANTES de SimilarWeb
const BLACKLIST_TLDS = [".us",".ca",".ru",".au",".nz",".ua"]; // Ukraine también fuera (zona conflicto)

// TLDs por región — usado para pre-filtrar por GEO antes de fetchar SimilarWeb
// Priorizamos LATAM > Europa > MENA > Africa > Asia
const TLD_BY_REGION = {
  LATAM:  [".ar",".mx",".co",".cl",".br",".pe",".ec",".ve",".uy",".py",".bo",".es",".com.ar",".com.mx",".com.co",".com.br",".com.pe"],
  CentralAmerica: [".mx",".cr",".pa",".gt",".hn",".ni",".sv",".cu",".pr",".do"],
  Europe: [".uk",".co.uk",".fr",".de",".it",".pt",".nl",".be",".ch",".at",".pl",".se",".no",".dk",".fi",".gr",".hu",".cz",".ro",".ie",".lu",".rs",".sk",".bg",".hr",".si",".lt",".lv",".ee"],
  Africa: [".za",".ng",".ke",".gh",".ma",".tn",".dz",".eg",".et",".tz"],
  MENA:   [".ae",".sa",".eg",".ma",".tr",".il",".kw",".qa",".dz",".tn"],
  Asia:   [".in",".jp",".kr",".cn",".tw",".hk",".sg",".my",".id",".ph",".th",".vn",".pk",".bd"],
};

// Returns the "organization root" of a domain — collapses regional TLD variants
// so clarin.com / clarin.com.ar / clarin.com.br dedupe to the same org key.
// Heuristic — strips known multi-part TLDs first, then last TLD label, leaving
// the brand part. Not perfect, but cuts >80% of obvious cross-region duplicates.
const MULTI_PART_TLDS = new Set([
  // commercial
  "com.ar","com.br","com.mx","com.co","com.pe","com.uy","com.ec","com.ve","com.bo",
  "com.es","com.au","com.cn","com.tw","com.hk","com.sg","com.my","com.tr","com.eg",
  "com.sa","com.ng","com.za","com.ph","com.vn","com.pk","com.gt","com.do","com.pa",
  "com.gh","com.ke","com.uy","com.bd","com.np","com.lk","com.kh","com.kw","com.qa",
  "com.lb","com.jo","com.om","com.ye","com.ly","com.tn","com.dz",
  "co.uk","co.za","co.in","co.kr","co.jp","co.il","co.nz","co.id","co.cr","co.ve",
  "co.th","co.ke","co.tz","co.ug","co.zw","co.ma","co.ao",
  // organization
  "org.uk","org.ar","org.br","org.mx","org.au","org.za","org.es","org.in","org.pl",
  "org.cn","org.tw","org.kr","org.jp","org.tr","org.sg","org.my","org.ph","org.vn",
  "org.pk","org.bd","org.np","org.lk","org.eg","org.sa","org.ae","org.il",
  // government — todos los .gov.X conocidos
  "gov.ar","gov.br","gov.mx","gov.co","gov.pe","gov.cl","gov.uy","gov.ec","gov.ve",
  "gov.bo","gov.au","gov.in","gov.uk","gov.za","gov.eg","gov.sa","gov.ng",
  "gov.cn","gov.tw","gov.kr","gov.jp","gov.sg","gov.my","gov.ph","gov.vn","gov.pk",
  "gov.bd","gov.np","gov.lk","gov.tr","gov.ae","gov.il","gov.tn","gov.ma",
  // academic
  "ac.uk","ac.in","ac.za","ac.jp","ac.kr","ac.nz","ac.th","ac.id","ac.cn","ac.tw",
  "ac.ir","ac.ae","ac.il","ac.bd","ac.lk","ac.np","ac.tz","ac.ke",
  "edu.ar","edu.br","edu.mx","edu.co","edu.pe","edu.uy","edu.au","edu.in","edu.eg",
  "edu.cn","edu.tw","edu.hk","edu.sg","edu.my","edu.ph","edu.vn","edu.pk","edu.bd",
  "edu.np","edu.lk","edu.tr","edu.sa","edu.ae","edu.jo","edu.lb",
  // network/info per country
  "net.ar","net.br","net.mx","net.au","net.in","net.cn","net.tw","net.kr","net.jp",
  "net.sg","net.my","net.ph","net.vn","net.pk","net.bd","net.np","net.lk","net.eg",
  "net.sa","net.ae","net.tr",
  // .jp specific second-level (not just co.jp)
  "or.jp","ne.jp","ad.jp","ed.jp","gr.jp","lg.jp","go.jp",
  // .kr specific
  "or.kr","ne.kr","go.kr","re.kr","pe.kr","es.kr","sc.kr","hs.kr","ms.kr",
  // .cn specific
  "ah.cn","bj.cn","cq.cn","fj.cn","gd.cn","gs.cn","gz.cn","gx.cn","ha.cn","hb.cn",
  "he.cn","hi.cn","hk.cn","hl.cn","hn.cn","jl.cn","js.cn","jx.cn","ln.cn","mo.cn",
  "nm.cn","nx.cn","qh.cn","sc.cn","sd.cn","sh.cn","sn.cn","sx.cn","tj.cn","tw.cn",
  "xj.cn","xz.cn","yn.cn","zj.cn",
]);
function coreDomain(domain) {
  if (!domain) return "";
  const parts = domain.toLowerCase().replace(/^www\./, "").split(".");
  if (parts.length <= 2) return parts[0]; // foo.com → "foo"
  // Last 2 labels — check if it's a multi-part TLD (e.g. "com.ar", "gov.co")
  const last2 = parts.slice(-2).join(".");
  if (MULTI_PART_TLDS.has(last2) && parts.length >= 3) {
    return parts[parts.length - 3]; // x.gov.co → "x", brand.com.ar → "brand"
  }
  return parts[parts.length - 2]; // sub.foo.com → "foo"
}

function isDomainBlocked(domain) {
  const d = domain.toLowerCase();
  if (CORPORATE_BLOCKLIST.has(d)) return "corporate/brand";
  if (BLOCKED_TLDS.some(t => d.endsWith(t) || d.includes(t))) return "government/education";
  if (ADULT_TLDS.some(t => d.endsWith(t))) return "adult-tld";
  if (ADULT_KEYWORDS.some(k => d.includes(k))) return "adult-keyword";
  // Blacklist geográfica: descarta dominios de USA/CA/RU/AU/NZ/UA por TLD
  if (BLACKLIST_TLDS.some(t => d.endsWith(t))) return "geo-blacklist-tld";
  return null;
}

// Cache admin blocklist (toolbar_url_blocklist) — refresca cada 5min.
// Fix 2026-05-13: antes el worker SOLO chequeaba hardcoded blocklist.
// Los 500+ dominios que admin agrega manualmente no se filtraban en
// autopilot path. Ahora ambos paths (CSV + autopilot + agent send) consultan.
let _adminBlocklistCacheWorker = null;
let _adminBlocklistFetchedAtWorker = 0;
const ADMIN_BLOCKLIST_TTL_MS = 5 * 60 * 1000;
async function getAdminBlocklistWorker(token) {
  const now = Date.now();
  if (_adminBlocklistCacheWorker && (now - _adminBlocklistFetchedAtWorker) < ADMIN_BLOCKLIST_TTL_MS) {
    return _adminBlocklistCacheWorker;
  }
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_url_blocklist?select=domain`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return _adminBlocklistCacheWorker || new Set();
    const rows = await res.json();
    _adminBlocklistCacheWorker = new Set((rows || []).map(r => (r.domain || "").toLowerCase().replace(/^www\./, "")));
    _adminBlocklistFetchedAtWorker = now;
    return _adminBlocklistCacheWorker;
  } catch { return _adminBlocklistCacheWorker || new Set(); }
}

// Chequeo unificado: combina hardcoded + admin blocklist.
// Devuelve string razón si está bloqueado, null si pasa.
async function isDomainBlockedFull(domain, token) {
  const hardcoded = isDomainBlocked(domain);
  if (hardcoded) return hardcoded;
  const d = domain.toLowerCase().replace(/^www\./, "");
  const adminList = await getAdminBlocklistWorker(token);
  if (adminList.has(d)) return "admin-blocklist";
  // Subdominios de un dominio en blocklist
  for (const b of adminList) {
    if (d.endsWith("." + b)) return `admin-blocklist-subdomain (${b})`;
  }
  return null;
}

// Infiere país desde el TLD — fallback cuando SimilarWeb no devuelve topCountry
const TLD_TO_COUNTRY = {
  ".ar": "Argentina", ".mx": "Mexico", ".co": "Colombia", ".cl": "Chile", ".br": "Brazil",
  ".pe": "Peru", ".ec": "Ecuador", ".ve": "Venezuela", ".uy": "Uruguay", ".py": "Paraguay",
  ".bo": "Bolivia", ".es": "Spain",
  ".cr": "Costa Rica", ".pa": "Panama", ".gt": "Guatemala", ".hn": "Honduras",
  ".ni": "Nicaragua", ".sv": "El Salvador", ".cu": "Cuba", ".pr": "Puerto Rico", ".do": "Dominican Republic",
  ".uk": "United Kingdom", ".co.uk": "United Kingdom", ".fr": "France", ".de": "Germany",
  ".it": "Italy", ".pt": "Portugal", ".nl": "Netherlands", ".be": "Belgium",
  ".ch": "Switzerland", ".at": "Austria", ".pl": "Poland", ".se": "Sweden",
  ".no": "Norway", ".dk": "Denmark", ".fi": "Finland", ".gr": "Greece",
  ".hu": "Hungary", ".cz": "Czech Republic", ".ro": "Romania", ".ie": "Ireland",
  ".za": "South Africa", ".ng": "Nigeria", ".ke": "Kenya", ".gh": "Ghana", ".ma": "Morocco",
  ".tn": "Tunisia", ".dz": "Algeria", ".eg": "Egypt",
  ".ae": "UAE", ".sa": "Saudi Arabia", ".tr": "Turkey", ".il": "Israel",
  ".in": "India", ".jp": "Japan", ".kr": "South Korea", ".tw": "Taiwan", ".hk": "Hong Kong",
  ".sg": "Singapore", ".my": "Malaysia", ".id": "Indonesia", ".ph": "Philippines",
  ".th": "Thailand", ".vn": "Vietnam", ".pk": "Pakistan",
};

function inferCountryFromTLD(domain) {
  const d = domain.toLowerCase();
  // Probar TLDs compuestos primero (.co.uk, .com.ar, etc.)
  for (const [tld, country] of Object.entries(TLD_TO_COUNTRY)) {
    if (d.endsWith(tld)) return country;
  }
  return null;
}

// Si hay targetGeo seteado, chequea que el TLD matchee con algún país de la región
// Para .com / .net / .org etc (genéricos) devuelve true (no podemos decidir sin SimilarWeb)
function matchesTargetGeoByTLD(domain, targetGeo) {
  if (!targetGeo) return true;
  const tldList = TLD_BY_REGION[targetGeo];
  if (!tldList) return true; // target es un país específico, no podemos pre-filtrar
  const d = domain.toLowerCase();
  const genericTLDs = [".com", ".net", ".org", ".info", ".biz", ".co", ".io", ".app", ".dev", ".xyz"];
  // Si tiene TLD regional → chequear si pertenece a la región target
  const hasRegionalTLD = tldList.some(t => d.endsWith(t));
  if (hasRegionalTLD) return true;
  // Si tiene TLD de OTRA región → descartar
  for (const [region, tlds] of Object.entries(TLD_BY_REGION)) {
    if (region === targetGeo) continue;
    if (tlds.some(t => d.endsWith(t))) return false;
  }
  // Es genérico (.com etc) — aceptamos y dejamos que SimilarWeb decida con topCountry
  if (genericTLDs.some(t => d.endsWith(t))) return true;
  return true;
}

// ── Scoring ───────────────────────────────────────────────────

const GEO_REGIONS = {
  LATAM:          ["Mexico","Argentina","Colombia","Chile","Brazil","Peru","Ecuador","Venezuela","Uruguay","Paraguay","Bolivia","Spain"],
  CentralAmerica: ["Mexico","Costa Rica","Panama","Guatemala","Honduras","Nicaragua","El Salvador","Dominican Republic","Cuba","Puerto Rico"],
  Europe:         ["United Kingdom","France","Germany","Italy","Portugal","Netherlands","Belgium","Switzerland","Austria","Poland","Sweden","Norway","Denmark","Finland","Greece","Hungary","Czech Republic","Romania","Ireland","Luxembourg","Serbia","Slovakia","Bulgaria","Croatia","Slovenia","Lithuania","Latvia","Estonia"],
  Africa:         ["South Africa","Nigeria","Kenya","Ghana","Morocco","Tunisia","Algeria","Egypt","Ethiopia","Tanzania"],
  MENA:           ["UAE","Saudi Arabia","Egypt","Morocco","Turkey","Israel","Kuwait","Qatar","Algeria","Tunisia"],
  Asia:           ["India","Japan","South Korea","China","Taiwan","Hong Kong","Singapore","Malaysia","Indonesia","Philippines","Thailand","Vietnam","Pakistan"],
};

const HIGH_VALUE_CATS = new Set(["sports","news","entertainment","gambling","finance"]);
const MED_VALUE_CATS  = new Set(["health","travel","automotive","technology","food","business"]);

function scoreCandidate({ visits, category, topCountry, contactName, emails, pageContent, allowedCountries }) {
  let score = 0;

  // Traffic tier (0–40)
  if      (visits >= 20_000_000) score += 40;
  else if (visits >=  5_000_000) score += 32;
  else if (visits >=  1_000_000) score += 22;
  else if (visits >=    500_000) score += 14;
  else                           score +=  6;

  // Category ad-value (0–25)
  if      (HIGH_VALUE_CATS.has(category)) score += 25;
  else if (MED_VALUE_CATS.has(category))  score += 15;
  else                                     score +=  5;

  // Contact & email signals (0–20)
  if (contactName)      score += 10;
  if (emails.length > 0) score += 10;

  // Page accessible (0–5)
  if (pageContent) score += 5;

  // Target geo bonus (+15 if inside the allowed set)
  if (allowedCountries?.size && topCountry && allowedCountries.has(topCountry)) {
    score += 15;
  }

  return Math.max(0, score);
}

// ── Helpers ───────────────────────────────────────────────────

function cleanDomain(str) {
  return (str || "").toLowerCase()
    .replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "")
    // Strip annotations comunes: " (r)", " (refresh)", " (m)", " - foo", "[bar]", etc.
    .replace(/\s+[\(\[\-].*$/, "")
    .replace(/\s+/g, "")
    .trim();
}

// Validador estricto: domain debe tener formato xxx.tld válido.
function _isValidDomainFormat(d) {
  if (!d) return false;
  // No espacios, no paréntesis, no caracteres raros, debe tener al menos un punto
  return /^[a-z0-9][a-z0-9\-\.]*\.[a-z]{2,}$/i.test(d) && !/\s|[\(\)\[\]]/.test(d);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function log(msg) {
  console.log(`[${new Date().toISOString().replace("T", " ").substring(0, 19)}] ${msg}`);
}

function shuffleArray(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
}

// ── CSV Queue — bulk refresh de Monday ────────────────────────

// Sin límite diario por user (antes 75). El costo real lo controla
// Apollo (limit propio) + idle-exit del proceso Railway.
const CSV_DAILY_LIMIT_PER_USER = 300; // 300 dominios/día/usuario para CSV (Monday URL + External CSV)

// Cuenta cuántos items terminó (done) un usuario específico hoy
async function getUserCsvDoneToday(token, userEmail) {
  try {
    // Argentina local day — quotas reset at 00:00 AR, not 00:00 UTC
    const todayISO = new Date().toLocaleDateString("en-CA", { timeZone: "America/Argentina/Buenos_Aires" });
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.done&uploaded_by=eq.${encodeURIComponent(userEmail)}&processed_at=gte.${todayISO}T00:00:00Z&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0" } }
    );
    const contentRange = res.headers.get("content-range") || "";
    const match = contentRange.match(/\/(\d+)$/);
    return match ? parseInt(match[1]) : 0;
  } catch { return 0; }
}

async function getNextCsvItem(token, blockedUsers = new Set()) {
  try {
    let filter = "";
    if (blockedUsers.size > 0) {
      // Supabase REST "not.in" syntax: uploaded_by=not.in.(a,b,c)
      const list = [...blockedUsers].map(u => `"${u}"`).join(",");
      filter = `&uploaded_by=not.in.(${list})`;
    }
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.pending${filter}&order=uploaded_at.asc&limit=1&select=id,domain,uploaded_by,error_message`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) return null;
    const item = rows[0];

    // Marcar como processing (claim atómico)
    const claim = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?id=eq.${item.id}&status=eq.pending`,
      {
        method: "PATCH",
        headers: {
          "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
          "Content-Type": "application/json", "Prefer": "return=representation",
        },
        body: JSON.stringify({ status: "processing" }),
      }
    );
    const claimed = await claim.json().catch(() => []);
    return claimed?.[0] ? item : null; // si otro proceso lo tomó, claimed será []
  } catch { return null; }
}

// Revierte un item claimed a pending (para cuando el usuario alcanzó su quota diaria)
async function revertCsvItemToPending(token, id) {
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?id=eq.${id}`, {
      method: "PATCH",
      headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ status: "pending" }),
    });
  } catch {}
}

// Map login email → Monday user ID para la columna Persona deal_owner
const MONDAY_USER_IDS = {
  "mgargiulo@adeqmedia.com": 56851451, // Maximiliano
  "sales@adeqmedia.com":     60940538, // Agustina
  "dhorovitz@adeqmedia.com": 56938560, // Diego
};

// Elige la Monday API key del usuario que subió el CSV (fallback al default)
function getMondayKeyForUser(cfg, userEmail) {
  if (!userEmail) return cfg.monday_api_key || "";
  const perUser = cfg[`monday_api_key_${userEmail.toLowerCase().trim()}`];
  return perUser || cfg.monday_api_key || "";
}

// Formatea visitas al formato Monday:
//   <1M  → redondea al múltiplo de 50K más cercano → "150K" / "450K" / "950K"
//   >=1M → redondea a 1 decimal → "1.3M" / "5.2M"
function formatVisitsForMonday(visits) {
  if (!visits || visits < 1000) return String(visits || 0);
  if (visits >= 1_000_000) {
    const m = Math.round(visits / 100_000) / 10; // 1 decimal
    return `${m}M`;
  }
  const k = Math.round(visits / 50_000) * 50; // múltiplos de 50
  return `${k}K`;
}

async function markCsvItem(token, id, status, fields = {}) {
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?id=eq.${id}`, {
      method: "PATCH",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ status, processed_at: new Date().toISOString(), ...fields }),
    });
  } catch {}
}

// Busca un item en Monday por nombre de dominio y devuelve id + estado actual
async function findMondayItem(domain, mondayApiKey) {
  const clean = cleanDomain(domain);
  const query = `{
    boards(ids: [1420268379]) {
      items_page(limit: 5, query_params: { rules: [
        { column_id: "name", compare_value: "${clean}", operator: contains_text }
      ]}) {
        items {
          id name
          column_values(ids: ["deal_stage"]) { id text }
        }
      }
    }
  }`;
  try {
    const res = await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": mondayApiKey, "API-Version": "2024-01" },
      body: JSON.stringify({ query }),
    });
    const data  = await res.json();
    const items = data?.data?.boards?.[0]?.items_page?.items || [];
    const match = items.find(it => cleanDomain(it.name) === clean);
    if (!match) return null;
    const estado = match.column_values?.find(c => c.id === "deal_stage")?.text || "";
    return { id: match.id, estado };
  } catch { return null; }
}

async function updateMondayItem(itemId, columnValues, mondayApiKey) {
  const safe = (s) => String(s).replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");
  const mutation = `mutation {
    change_multiple_column_values(
      item_id: ${itemId},
      board_id: 1420268379,
      column_values: "${safe(JSON.stringify(columnValues))}"
    ) { id }
  }`;
  const res = await fetch("https://api.monday.com/v2", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": mondayApiKey, "API-Version": "2024-01" },
    body: JSON.stringify({ query: mutation }),
  });
  const data = await res.json();
  if (data?.errors) throw new Error(JSON.stringify(data.errors).substring(0, 200));
  return data?.data?.change_multiple_column_values;
}

async function processCsvItem(token, item, cfg, apolloUsage, apolloCallsThisSessionRef) {
  const { rapidapi_key, apollo_api_key } = cfg;
  const domain = item.domain;

  // 0. Blocklist check (hardcoded + admin) ANTES de gastar API
  // Fix 2026-05-13: el CSV worker no chequeaba admin blocklist, solo hardcoded.
  const blockReason = await isDomainBlockedFull(domain, token);
  if (blockReason) {
    await markCsvItem(token, item.id, "skipped", { error_message: `blocked: ${blockReason}` });
    log(`  ⊘ ${domain} — ${blockReason}, sin consumir API`);
    return;
  }

  // Buscar en Monday para detectar si es CSV externo (no existe) o Monday Refresh (sí existe + Ciclo Finalizado)
  const mondayApiKey = getMondayKeyForUser(cfg, item.uploaded_by);
  let match = null;
  let source = "csv"; // default: URL externa, no está en Monday todavía
  let mondayItemId = null;

  if (mondayApiKey) {
    match = await findMondayItem(domain, mondayApiKey);
    if (match) {
      // Existe en Monday — solo procesar si está en Ciclo Finalizado
      if (!REPROSPECTABLE_STATES.has(match.estado)) {
        await markCsvItem(token, item.id, "skipped", {
          error_message: `estado=${match.estado || "?"} (solo ${[...REPROSPECTABLE_STATES].join(" / ")} se re-prospecta)`,
          monday_item_id: match.id,
        });
        log(`  ⏭ ${domain} — ya está en Monday con estado "${match.estado}"`);
        return;
      }
      source = "monday_refresh";
      mondayItemId = match.id;
    }
  }

  // 1. Traffic + page content en paralelo
  const [trafficData, pageContent] = await Promise.all([
    getTrafficData(domain, rapidapi_key),
    fetchPageContent(domain),
  ]);
  let { visits, topCountry, topCountries3 } = trafficData;
  if (!topCountry) {
    const inferred = inferCountryFromTLD(domain);
    if (inferred) topCountry = inferred;
  }
  // ISO codes top3 para filtro amplio del agente (geos_all)
  const _isoFromName = (name) => Object.keys(COUNTRY_CODES).find(k => COUNTRY_CODES[k] === name);
  const geosAllIso = Array.isArray(topCountries3) && topCountries3.length
    ? topCountries3
    : (topCountry ? [_isoFromName(topCountry) || topCountry] : []);
  // ── REGLA ESTRICTA + BACKOFF EXPONENCIAL ──
  // - traffic < 400K → skip (no entra)
  // - traffic = 0 / null / error → after 3 failed attempts, FREEZE 15d/30d/60d
  // - traffic ≥ 400K → entra al pool
  if (!visits || visits <= 0) {
    // Tracking de intentos via error_message (no requiere schema change)
    const prevAttempts = parseInt((item.error_message || "").match(/attempt_(\d+)/)?.[1] || "0", 10);
    const newAttempt = prevAttempts + 1;
    if (newAttempt >= 3) {
      // 3 fails → freeze. Backoff: 15d primera vez, 30d segunda, 60d tercera+
      try {
        const existing = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_frozen_leads?domain=eq.${encodeURIComponent(domain)}&select=attempt_count`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
        );
        const rows = await existing.json();
        const prevFreeze = Array.isArray(rows) && rows[0] ? parseInt(rows[0].attempt_count || 1, 10) : 0;
        const days = prevFreeze === 0 ? 15 : prevFreeze === 1 ? 30 : 60;
        // Auto-blocklist permanente tras 3+ freeze cycles sin traffic data.
        // Dominios "inoperativos": están caídos o RapidAPI no los reconoce. No vale gastar más.
        if (prevFreeze >= 2) {
          try {
            await fetch(`${SUPABASE_URL}/rest/v1/toolbar_url_blocklist`, {
              method: "POST",
              headers: {
                "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
                "Content-Type": "application/json", "Prefer": "resolution=ignore-duplicates,return=minimal",
              },
              body: JSON.stringify({
                domain: domain.toLowerCase().replace(/^www\./, ""),
                category: "inoperativo",
                added_by: "worker_auto",
                reason: `3 freeze cycles sin traffic data (último intento ${new Date().toISOString().split("T")[0]})`,
              }),
            });
            log(`  🚫 ${domain} → AUTO-BLOCKLIST 'inoperativo' (3 freeze cycles)`);
          } catch (e) { log(`  ⚠️ auto-blocklist err: ${e.message}`); }
        }
        const frozenUntil = new Date(Date.now() + days * 86400_000).toISOString();
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_frozen_leads`, {
          method: "POST",
          headers: {
            "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
            "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal",
          },
          body: JSON.stringify({
            domain, frozen_until: frozenUntil,
            attempt_count: prevFreeze + 1,
            last_error: "no_traffic_data_after_3_attempts",
            source, uploaded_by: item.uploaded_by || "",
            updated_at: new Date().toISOString(),
          }),
        });
        await markCsvItem(token, item.id, "frozen", {
          error_message: `frozen_until_${frozenUntil} (${days}d backoff)`,
        });
        log(`  🧊 ${domain} — FREEZE ${days}d después de 3 intentos. unfreeze: ${frozenUntil.split("T")[0]}`);
      } catch (e) {
        await markCsvItem(token, item.id, "skipped", { error_message: `freeze_failed: ${e.message}` });
        log(`  ⚠️ ${domain} freeze err: ${e.message}`);
      }
      return;
    }
    // Aún hay intentos — marcar pending con counter incrementado
    await markCsvItem(token, item.id, "pending", {
      error_message: `no_traffic_data — attempt_${newAttempt}/3`,
    });
    log(`  ⏸ ${domain} — sin traffic (intento ${newAttempt}/3). Retry próximo iter.`);
    return;
  }
  if (visits < REVIEW_QUEUE_MIN_TRAFFIC) {
    await markCsvItem(token, item.id, "skipped", {
      error_message: `traffic ${visits} below min ${REVIEW_QUEUE_MIN_TRAFFIC}`,
    });
    log(`  ⏭ ${domain} — traffic ${visits} < ${REVIEW_QUEUE_MIN_TRAFFIC} (no entra a review_queue)`);
    return;
  }
  const category = pageContent?.category || "";
  const adNetworks = pageContent?.adNetworks || [];
  const pageTitle = pageContent?.title || "";

  // ── Detección de IDIOMA al insertar — robusta vía detectLanguageRobust ──
  const langDet = await detectLanguageRobust({
    htmlLang:   pageContent?.htmlLang,
    ogLocale:   pageContent?.ogLocale,
    hreflang:   pageContent?.hreflang,
    jsonLdLang: pageContent?.jsonLdLang,
    pathLang:   pageContent?.pathLang,
    textSample: pageContent?.textSample,
    geo:        topCountry,
    domain,
  }, { token });
  const detectedLang = langDet.lang;

  // 2. Emails — Apollo si visits >= 500K, scraping siempre como fallback.
  // Doble cap: diario (150) + mensual (2400 del plan). Si llega cualquiera,
  // skip Apollo y usa solo scraping. Cero impacto al flow (igual hay emails).
  const apolloMonthRemaining = (apolloUsage.monthLimit ?? APOLLO_MONTHLY_HARD_CAP) - (apolloUsage.usedThisMonth ?? 0);
  const canUseApollo = apollo_api_key
    && (apolloUsage.usedToday + apolloCallsThisSessionRef.count) < apolloUsage.limit
    && apolloMonthRemaining > 0;

  // Estrategia de rotación (user 2026-05-13): no quemar Apollo en cada lead.
  // 50% → scrape primero, si vacío Apollo fallback (ahorra crédito en sitios obvios)
  // 50% → Apollo primero, si vacío scrape fallback (mejor calidad cuando funciona)
  // Resultado: ~50% de Apollo lookups vs antes (100%).
  const useApolloFirst = canUseApollo && Math.random() < 0.5;
  let apolloRes = null;
  let scraperEmails = [];
  if (useApolloFirst) {
    apolloRes = await findBestApolloEmail(domain, apollo_api_key, token, { traffic: visits, allowUnlock: true })
      .then(r => { if (r?.source === "unlocked") apolloCallsThisSessionRef.count += 1; return r; })
      .catch(() => null);
    if (!apolloRes?.email) {
      scraperEmails = await scrapeEmailsForDomain(domain).catch(() => []);
    }
  } else {
    scraperEmails = await scrapeEmailsForDomain(domain).catch(() => []);
    if (scraperEmails.length === 0 && canUseApollo) {
      apolloRes = await findBestApolloEmail(domain, apollo_api_key, token, { traffic: visits, allowUnlock: true })
        .then(r => { if (r?.source === "unlocked") apolloCallsThisSessionRef.count += 1; return r; })
        .catch(() => null);
    }
  }
  const apolloContactName = apolloRes?.contact_name || "";
  const apolloPhone       = apolloRes?.phone || "";
  const apolloEmail = apolloRes?.email ? [apolloRes.email] : [];
  // Apollo va PRIMERO en el array. emailSources mapea cada email a su origen
  // para pick prioritario en agent: apollo > scrape > generic.
  const rawEmails = [...new Set([...apolloEmail, ...scraperEmails])];
  const emailSources = {};
  apolloEmail.forEach(e => { emailSources[e.toLowerCase()] = "apollo"; });
  scraperEmails.forEach(e => {
    const lower = e.toLowerCase();
    if (!emailSources[lower]) {
      // Generic role detection: si el local-part es info/contact/sales/etc → "generic", sino "scrape"
      const local = (lower.split("@")[0] || "");
      const IS_GENERIC = /^(info|contact|contacto|contato|contatto|kontakt|hello|hi|hola|ola|olá|support|soporte|suporte|atendimento|mail|email|inbox|news|press|prensa|imprensa|sales|ventas|marketing|publicidade|publicidad|comercial|admin|general|reception|recepcion|recepcao|webmaster)$/i;
      emailSources[lower] = IS_GENERIC.test(local) ? "generic" : "scrape";
    }
  });
  // Filtrar emails con dominio sin MX records ANTES de guardar
  const emails = await validateEmailsBatch(rawEmails);
  if (rawEmails.length !== emails.length) {
    log(`  📧 ${domain}: ${rawEmails.length} → ${emails.length} emails (apollo:${apolloRes?.source||"none"})`);
  }

  // 3. NO empujar a Monday automáticamente. Escribir a review_queue para que el MB
  //    decida email + draft + push manualmente desde el tab Prospects.
  try {
    await saveToReviewQueue(token, {
      domain,
      traffic:        visits || 0,
      geo:            topCountry || "",
      geosAll:        geosAllIso,
      language:       detectedLang,
      category,
      contactName:    apolloContactName,
      contactPhone:   apolloPhone,
      emails,
      emailSources,                              // source tracking apollo/scrape/generic
      pitch:          "",
      pitchSubject:   "",
      pitchSubjects:  [],
      score:          0,
      adNetworks,
      pageTitle,
      createdBy:      item.uploaded_by || "",
      source,
      mondayItemId,
    });
    await markCsvItem(token, item.id, "done", { monday_item_id: mondayItemId });
    // Bump counter global diario (cap safety net 1000/día)
    bumpCsvDailyCounter(token, 1).catch(() => {});
    const vstr = visits ? formatVisitsForMonday(visits) : "-";
    log(`  ✅ ${domain} → review_queue (source:${source}, visits:${vstr}, geo:${topCountry || "-"}, ${emails.length} email(s))`);
  } catch (e) {
    await markCsvItem(token, item.id, "error", { error_message: e.message.substring(0, 500), monday_item_id: mondayItemId });
    log(`  ❌ ${domain} — ${e.message}`);
  }
}

async function runCsvQueue(token, cfg, maxItems = 100) {
  const apolloUsage   = await getApolloUsageToday(token);
  const rapidUsage    = await getRapidApiUsageToday(token);
  const rapidMonth    = await getRapidApiUsageThisMonth(token);
  const dailyGlobal   = await getDailyGlobalCounters(token);
  const callsRef      = { count: 0 };
  const blockedUsers  = new Set(); // usuarios que alcanzaron el límite diario
  const userCounts    = new Map(); // email → cuántos procesamos en esta tanda
  let processed       = 0;

  log(`▶ CSV queue start (apollo: ${apolloUsage.usedToday}/${apolloUsage.limit} · rapidapi día: ${rapidUsage.usedToday}/${rapidUsage.limit} · mes: ${rapidMonth.usedThisMonth}/${rapidMonth.limit} · csv global hoy: ${dailyGlobal.csvCount}/${dailyGlobal.csvCap})`);

  // Hard cap MENSUAL — no procesar si pasamos el límite del mes
  if (rapidMonth.usedThisMonth >= rapidMonth.limit) {
    log(`⛔ Cap MENSUAL de RapidAPI alcanzado — CSV queue no arranca hasta próximo mes.`);
    return 0;
  }
  // Hard cap DIARIO
  if (rapidUsage.usedToday >= rapidUsage.limit) {
    log(`⛔ Cap diario de RapidAPI alcanzado — CSV queue no arranca. Reset mañana.`);
    return 0;
  }
  // Hard cap GLOBAL diario CSV (safety net — cap por encima del per-user para
  // proteger RapidAPI/Railway si bug en agente o MBs sobre-prospectan)
  if (dailyGlobal.csvCount >= dailyGlobal.csvCap) {
    log(`⛔ Cap diario GLOBAL CSV alcanzado (${dailyGlobal.csvCount}/${dailyGlobal.csvCap}) — pausa hasta próximo día operativo.`);
    return 0;
  }
  // Pausa fin de semana — operativo solo Lun-Vie España (override: test mode)
  const _csvTestMode = String(cfg.agent_test_mode || "").toLowerCase() === "true";
  if (!_csvTestMode && _isWeekendSpain()) {
    log(`⏸ Fin de semana España — CSV queue pausada hasta lunes.`);
    return 0;
  }
  _rapidGlobalCounter = 0;
  _rapidCapReached    = false;
  const _rapidStart      = rapidUsage.usedToday;
  const _rapidMonthStart = rapidMonth.usedThisMonth;

  // Hard cap 20 min — defense en profundidad por si bug deja la cola corriendo
  const _csvSessionStart = Date.now();
  const CSV_SESSION_LIMIT_MS = 20 * 60 * 1000;

  let _csvKillCheckIter = 0;
  while (processed < maxItems) {
    // Hard timeout 20min — apagar y salir aunque queden items pendientes.
    if (Date.now() - _csvSessionStart >= CSV_SESSION_LIMIT_MS) {
      log(`⏱ CSV queue: 20min hard cap alcanzado — auto-apagando (procesados: ${processed}). Re-prender manual si querés seguir.`);
      await setConfigValue(token, "csv_queue_enabled", "false");
      break;
    }
    // Re-check del flag cada 5 items — si el user apagó el toggle, paramos YA.
    // Antes solo se chequeaba al inicio → bug "queue OFF pero sigue procesando".
    if (++_csvKillCheckIter % 5 === 0) {
      try {
        const fresh = await getConfig(token);
        if (String(fresh.csv_queue_enabled || "").toLowerCase() !== "true") {
          log(`🛑 CSV queue: flag apagado por user — deteniendo (procesados: ${processed})`);
          break;
        }
      } catch {}
    }
    // No hay pre-check de cap acá — el cap (200) se aplica al SUBIR items
    // (popup pre-check + promoteWaitlist en main loop). Si llegamos acá,
    // hay items para procesar normalmente.
    const item = await getNextCsvItem(token, blockedUsers);
    if (!item) {
      // Cola vacía. Verificar si TAMBIÉN waitlist está vacía (entonces no hay
      // nada en absoluto). Si es así, apagar el toggle para ahorrar Railway
      // compute. El user lo prende manual cuando suba nuevos imports.
      try {
        const wlRes = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.waiting_pool&select=id`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
        );
        const wlRange = wlRes.headers.get("content-range") || "";
        const wlCount = parseInt(wlRange.split("/")[1] || "0", 10);
        if (wlCount === 0) {
          log("  (cola vacía + waitlist vacía) — apagando csv_queue_enabled para ahorrar Railway");
          await setConfigValue(token, "csv_queue_enabled", "false");
        }
      } catch {}
      break;
    }

    const userEmail = item.uploaded_by?.toLowerCase() || "";

    // Check quota diaria del usuario: suma los ya hechos en DB + los ya procesados en esta tanda
    const alreadyDone = await getUserCsvDoneToday(token, userEmail);
    const inBatch     = userCounts.get(userEmail) || 0;
    const userTotal   = alreadyDone + inBatch;

    if (Number.isFinite(CSV_DAILY_LIMIT_PER_USER) && userTotal >= CSV_DAILY_LIMIT_PER_USER) {
      log(`  ⏸ ${userEmail} alcanzó ${userTotal}/${CSV_DAILY_LIMIT_PER_USER} hoy — se reanuda mañana`);
      blockedUsers.add(userEmail);
      // Persistir cap-reached para que la toolbar muestre alert al user
      await setConfigValue(token, `csv_cap_reached_${userEmail}`, JSON.stringify({
        limit: CSV_DAILY_LIMIT_PER_USER, reachedAt: new Date().toISOString(),
      })).catch(() => {});
      await revertCsvItemToPending(token, item.id);
      continue;
    }

    processed++;
    userCounts.set(userEmail, inBatch + 1);
    log(`→ [${processed}/${maxItems}] ${item.domain} (${userEmail})`);

    try {
      await processCsvItem(token, item, cfg, apolloUsage, callsRef);
    } catch (e) {
      await markCsvItem(token, item.id, "error", { error_message: e.message.substring(0, 500) });
      log(`  ❌ ${item.domain} — uncaught: ${e.message}`);
    }

    // Hard cap MENSUAL mid-queue
    const rapidMonthUsedNow = _rapidMonthStart + _rapidGlobalCounter;
    if (rapidMonthUsedNow >= rapidMonth.limit) {
      _rapidCapReached = true;
      log(`⛔ Cap MENSUAL de RapidAPI alcanzado mid-queue (${rapidMonthUsedNow}/${rapidMonth.limit}) — corte hasta próximo mes.`);
      break;
    }
    // Hard cap diario mid-queue
    const rapidUsedNow = _rapidStart + _rapidGlobalCounter;
    if (rapidUsedNow >= rapidUsage.limit) {
      _rapidCapReached = true;
      log(`⛔ Cap diario de RapidAPI alcanzado mid-queue (${rapidUsedNow}/${rapidUsage.limit}) — corte. Reset mañana.`);
      break;
    }

    await sleep(DOMAIN_DELAY_MS);
  }

  const today = new Date().toISOString().split("T")[0];
  await saveApolloUsage(token, callsRef.count, today);
  await saveRapidApiUsage(token, _rapidGlobalCounter, today);
  await saveRapidApiMonthlyUsage(token, _rapidGlobalCounter, rapidMonth.period);
  log(`◼ CSV queue end — procesados: ${processed}, apollo: ${callsRef.count}, rapidapi: ${_rapidGlobalCounter}`);
  return processed;
}

// ── Sesión de prospección ─────────────────────────────────────

async function runSession(token, cfg, sessionStart) {
  const { monday_api_key, rapidapi_key, apollo_api_key } = cfg;

  // Targets de esta sesión (desde toolbar_config)
  // target_geo can be a comma-separated list of regions and/or countries
  const targetGeoRaw   = cfg.target_geo      || "";
  const targetGeos     = targetGeoRaw.split(",").map(s => s.trim()).filter(Boolean);
  const targetCategory = cfg.target_category || "";
  const minScore       = Number(cfg.min_score)    || 20;
  const sessionMinTraffic = Number(cfg.min_traffic) || MIN_TRAFFIC;
  const sessionUser    = cfg.auto_session_user || "";

  // Per-user limits configurables por el admin (toolbar_user_limits).
  // Defaults: 75 prospectos/día y 20 min/sesión. Si el admin los cambió, los usa.
  let AUTOPILOT_DAILY_LIMIT = 300; // default 300/día/usuario; admin lo puede sobrescribir per-user
  let userSessionLimitMs    = SESSION_LIMIT_MS;
  if (sessionUser) {
    try {
      const userLimits = await getUserLimits(token, sessionUser);
      if (userLimits.autopilot_daily_prospects > 0) AUTOPILOT_DAILY_LIMIT = userLimits.autopilot_daily_prospects;
      // Hard cap a 20 min — el admin puede bajar pero NO superar el SESSION_LIMIT_MS.
      // Evita loops infinitos por configuración mal seteada.
      if (userLimits.autopilot_daily_minutes >= 5) {
        const userMins = Math.min(userLimits.autopilot_daily_minutes, 20);
        userSessionLimitMs = userMins * 60 * 1000;
      }
      if (userLimits.autopilot_enabled === false) {
        log(`⛔ ${sessionUser} tiene autopilot DISABLED por el admin — sesión no arranca`);
        await setConfigValue(token, "auto_prospecting_enabled", "false");
        return;
      }
    } catch (e) { log(`(per-user limits no disponibles: ${e.message} — usando defaults)`); }
  }

  // Expand regions to concrete country lists for matching
  const allowedCountries = new Set();
  for (const g of targetGeos) {
    if (GEO_REGIONS[g]) GEO_REGIONS[g].forEach(c => allowedCountries.add(c));
    else allowedCountries.add(g); // treat as single country
  }
  const hasTargetGeo = targetGeos.length > 0;

  const targetInfo = [targetGeos.join("+"), targetCategory].filter(Boolean).join(" / ") || "sin filtros";
  log(`Sesión iniciada. User: ${sessionUser || "(desconocido)"} | Target: ${targetInfo} | Min score: ${minScore} | Min traffic: ${(sessionMinTraffic/1000).toFixed(0)}K`);
  // Reset stats live para que el popup muestre 0 al arrancar la sesión nueva
  resetAutopilotStats(sessionUser);
  await flushAutopilotStats(token, true).catch(() => {});

  // Quota diaria per-user para el autopilot
  const userTodayCount = await getUserAutopilotCountToday(token, sessionUser);
  if (sessionUser && userTodayCount >= AUTOPILOT_DAILY_LIMIT) {
    log(`⚠️ ${sessionUser} ya alcanzó el límite diario de autopilot (${userTodayCount}/${AUTOPILOT_DAILY_LIMIT}) — sesión no arranca`);
    return;
  }
  log(`Quota del día para ${sessionUser}: ${userTodayCount}/${AUTOPILOT_DAILY_LIMIT}`);

  // GLOBAL safety net + weekend pause (override: agent_test_mode=true)
  const _autoTestMode = String(cfg.agent_test_mode || "").toLowerCase() === "true";
  if (!_autoTestMode && _isWeekendSpain()) {
    log(`⏸ Fin de semana España — autopilot pausado hasta lunes.`);
    return;
  }
  const _dailyGlobalAuto = await getDailyGlobalCounters(token);
  if (_dailyGlobalAuto.autopilotCount >= _dailyGlobalAuto.autopilotCap) {
    log(`⛔ Cap GLOBAL diario autopilot alcanzado (${_dailyGlobalAuto.autopilotCount}/${_dailyGlobalAuto.autopilotCap}) — sesión no arranca.`);
    return;
  }

  // Cargar feedback del user (learning)
  const feedback = await loadAutopilotFeedback(token, sessionUser);
  if (feedback.dislikedDomains.size || feedback.dislikedCategories.size || feedback.dislikedGeos.size) {
    log(`Learning: ${feedback.dislikedDomains.size} dominios, ${feedback.dislikedCategories.size} categorías y ${feedback.dislikedGeos.size} geos bloqueados por feedback previo`);
  }

  // Intentar Cloudflare Radar si hay target_geos + token configurado
  let poolSource = "majestic";
  let radarPool  = null;
  if (CLOUDFLARE_API_TOKEN && hasTargetGeo) {
    const countryCodes = targetGeosToCountryCodes(targetGeos);
    if (countryCodes.length > 0) {
      log(`Cloudflare Radar: consultando top domains para ${countryCodes.join(",")}...`);
      radarPool = await loadRadarPoolForCountries(countryCodes);
      if (radarPool) {
        log(`Pool Radar: ${radarPool.size.toLocaleString()} dominios pre-filtrados por país.`);
      } else {
        log("Radar devolvió vacío.");
      }
    }
  }

  // Carga en paralelo: Majestic, Monday, procesados, rechazos, uso Apollo + RapidAPI (diario + mensual)
  const [majesticFullPool, mondayDomains, processed, rejectionPatterns, apolloUsage, rapidUsage, rapidMonth] = await Promise.all([
    loadDomainPool(),
    fetchMondayDomains(monday_api_key),
    getProcessedDomains(token),
    getRejectionPatterns(token),
    getApolloUsageToday(token),
    getRapidApiUsageToday(token),
    getRapidApiUsageThisMonth(token),
  ]);

  // Hard cap MENSUAL de RapidAPI — protección principal contra overage
  log(`RapidAPI mensual: ${rapidMonth.usedThisMonth}/${rapidMonth.limit} en ${rapidMonth.period}`);
  if (rapidMonth.usedThisMonth >= rapidMonth.limit) {
    log(`⛔ Cap MENSUAL de RapidAPI alcanzado (${rapidMonth.usedThisMonth}/${rapidMonth.limit}) — autopilot no arranca hasta el próximo mes.`);
    await setConfigValue(token, "auto_prospecting_enabled", "false");
    return;
  }

  // Hard cap DIARIO — defensa secundaria
  const rapidRemaining = rapidUsage.limit - rapidUsage.usedToday;
  log(`RapidAPI diario: ${rapidUsage.usedToday}/${rapidUsage.limit} hits hoy — ${rapidRemaining} disponibles`);
  if (rapidRemaining <= 0) {
    log(`⛔ Cap diario de RapidAPI alcanzado (${rapidUsage.usedToday}/${rapidUsage.limit}) — autopilot no arranca. Reset mañana.`);
    await setConfigValue(token, "auto_prospecting_enabled", "false");
    return;
  }
  // Resetear contadores de sesión + arm el cap-watcher
  _rapidGlobalCounter = 0;
  _rapidCapReached    = false;
  const _rapidStart   = rapidUsage.usedToday;
  const _rapidMonthStart = rapidMonth.usedThisMonth;

  // ── ROTACIÓN INTERNA AUTOPILOT con RATIO ADAPTATIVO (user 2026-05-13) ──
  // Mismo botón, 2 ideas: Majestic vs similar-sites discovery.
  // Sistema aprende cuál genera más leads durante el día y ajusta el ratio.
  // Floor 20%/cap 80% para no abandonar al peor (puede mejorar más tarde).
  let _probMajestic = 0.5;
  try {
    const statsRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(autopilot_majestic_leads_today,autopilot_similar_leads_today,autopilot_stats_date)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const statsRows = await statsRes.json();
    const statsMap = {}; statsRows.forEach(r => { statsMap[r.key] = r.value; });
    const fmt2 = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit" });
    const today2 = fmt2.format(new Date());
    if (statsMap.autopilot_stats_date === today2) {
      const majLeads = parseInt(statsMap.autopilot_majestic_leads_today || "0", 10);
      const simLeads = parseInt(statsMap.autopilot_similar_leads_today || "0", 10);
      if (majLeads + simLeads >= 5) {  // mínimo data point antes de adaptarse
        _probMajestic = Math.max(0.2, Math.min(0.8, majLeads / (majLeads + simLeads)));
      }
      log(`📊 Stats hoy — Majestic: ${majLeads} leads, Similar: ${simLeads} leads → P(majestic)=${_probMajestic.toFixed(2)}`);
    }
  } catch {}
  const _autopilotMode = Math.random() < _probMajestic ? "majestic" : "similar_discovery";
  log(`🎲 Autopilot mode: ${_autopilotMode} (P_majestic=${_probMajestic.toFixed(2)})`);
  const _modeStartLeadCount = await (async () => {
    try {
      const r = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_review_queue?source=eq.autopilot&select=id`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
      );
      const range = r.headers.get("content-range") || "";
      return parseInt(range.match(/\/(\d+)$/)?.[1] || "0", 10);
    } catch { return 0; }
  })();

  let pool;
  if (_autopilotMode === "similar_discovery") {
    // Seeds: 10 dominios random de review_queue con traffic>=400K + 10 de Monday activo
    const seedDomains = new Set();
    try {
      const rqSeedRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.400000&select=domain&order=created_at.desc&limit=10`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      const rqSeeds = await rqSeedRes.json();
      if (Array.isArray(rqSeeds)) rqSeeds.forEach(r => r.domain && seedDomains.add(r.domain.toLowerCase().replace(/^www\./, "")));
    } catch {}
    // Monday active: tomar 10 random de mondayDomains (ya cargados arriba)
    const mondaySeeds = mondayDomains.slice(0, 10);
    mondaySeeds.forEach(d => seedDomains.add(d));
    log(`Similar discovery seeds: ${seedDomains.size} dominios (review_queue + Monday)`);
    if (seedDomains.size === 0) {
      log("Sin seeds disponibles — fallback a Majestic global");
      pool = majesticFullPool;
      poolSource = "majestic-global-fallback";
    } else {
      const discoveredSet = new Set();
      // similarsites.com scrape (gratis) por cada seed, hasta 20 dominios/seed
      const seedArray = [...seedDomains];
      for (let i = 0; i < seedArray.length; i += 4) {
        const chunk = seedArray.slice(i, i + 4);
        const results = await Promise.all(chunk.map(d =>
          (async () => {
            try {
              const r = await fetch(`https://www.similarsites.com/site/${encodeURIComponent(d)}`, {
                headers: { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0" },
                signal: AbortSignal.timeout(8000),
              });
              if (!r.ok) return [];
              const html = await r.text();
              const m = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/);
              if (!m) return [];
              const j = JSON.parse(m[1]);
              const found = [];
              const search = (obj) => {
                if (!obj || typeof obj !== "object") return;
                if (Array.isArray(obj)) {
                  for (const it of obj) {
                    if (it && typeof it === "object") {
                      const dd = it.domain || it.Domain || it.hostname;
                      if (dd && typeof dd === "string" && dd.includes(".") && !dd.includes("/")) {
                        found.push(dd.toLowerCase().replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, ""));
                      }
                      search(it);
                    }
                  }
                } else {
                  for (const v of Object.values(obj)) search(v);
                }
              };
              search(j);
              return [...new Set(found)].slice(0, 20);
            } catch { return []; }
          })()
        ));
        results.flat().forEach(d => d && d !== "" && discoveredSet.add(d));
      }
      pool = [...discoveredSet];
      poolSource = `similar-sites-${seedDomains.size}seeds`;
      log(`Similar discovery: ${pool.length.toLocaleString()} dominios descubiertos desde ${seedDomains.size} seeds (gratis)`);
      if (pool.length === 0) {
        log("Similar discovery vacío — fallback a Majestic global");
        pool = majesticFullPool;
        poolSource = "majestic-global-fallback";
      }
    }
  } else if (hasTargetGeo) {
    // Expandir targets a lista de TLDs relevantes
    const targetTLDs = new Set();
    for (const g of targetGeos) {
      if (TLD_BY_REGION[g]) TLD_BY_REGION[g].forEach(t => targetTLDs.add(t));
      else {
        // Buscar TLD del país individual
        for (const [tld, country] of Object.entries(TLD_TO_COUNTRY)) {
          if (country === g) targetTLDs.add(tld);
        }
      }
    }
    const tldList = [...targetTLDs];
    // Filtrar Majestic por TLDs relevantes
    const majesticFiltered = tldList.length
      ? majesticFullPool.filter(d => tldList.some(tld => d.endsWith(tld)))
      : [];
    log(`Majestic filtrado por TLD (${tldList.length} TLDs): ${majesticFiltered.length.toLocaleString()} dominios`);

    // Merge sin duplicar — Radar primero (mejor ranking), Majestic relleno
    const seen = new Set();
    pool = [];
    if (radarPool) {
      for (const d of radarPool.keys()) { seen.add(d); pool.push(d); }
    }
    for (const d of majesticFiltered) { if (!seen.has(d)) { seen.add(d); pool.push(d); } }
    poolSource = radarPool ? "radar+majestic-tld" : "majestic-tld";
  } else {
    pool = majesticFullPool;
    poolSource = "majestic-global";
  }
  log(`Fuente del pool: ${poolSource} (${pool.length.toLocaleString()} dominios totales)`);

  let apolloCallsThisSession = 0;
  const apolloRemaining = apolloUsage.limit - apolloUsage.usedToday;
  log(`Apollo: ${apolloUsage.usedToday}/${apolloUsage.limit} usados hoy — ${apolloRemaining} disponibles`);

  if (rejectionPatterns.categories && Object.keys(rejectionPatterns.categories).length > 0) {
    const topRejected = Object.entries(rejectionPatterns.categories)
      .sort((a, b) => b[1] - a[1]).slice(0, 3)
      .map(([c, n]) => `${c}(×${n})`).join(", ");
    log(`Patrones de rechazo aprendidos — categorías: ${topRejected}`);
  }

  const mondaySet  = new Set(mondayDomains);
  let candidates = pool.filter(d => !mondaySet.has(d) && !processed.has(d));

  // ── PRE-FILTRO POR GEO CACHE ──────────────────────────────────
  // Para cada candidato chequeamos si ya conocemos su país en cache.
  // Si está y NO matchea allowedCountries → descartar SIN llamar SimilarWeb.
  // Si está y SÍ matchea → priorizamos al frente del array.
  // Si no está → queda como candidato normal (vamos a llamar SimilarWeb).
  let geoFilteredOut = 0;
  let geoCacheHits   = 0;
  if (hasTargetGeo && candidates.length > 0) {
    log(`Consultando GEO cache para ${candidates.length.toLocaleString()} candidatos...`);
    const cachedGeos = await getCachedGeoBulk(token, candidates);
    geoCacheHits = cachedGeos.size;
    const matchedFirst = [];
    const unknown      = [];
    for (const d of candidates) {
      const cc = cachedGeos.get(d);
      if (!cc) { unknown.push(d); continue; }
      if (allowedCountries.has(COUNTRY_CODES[cc] || cc)) matchedFirst.push(d);
      else { geoFilteredOut++; }
    }
    candidates = [...matchedFirst, ...unknown];
    log(`GEO cache: ${geoCacheHits} conocidos, ${matchedFirst.length} matchean target, ${geoFilteredOut} descartados sin gastar SimilarWeb, ${unknown.length} desconocidos por consultar.`);
  }

  shuffleArray(candidates);

  log(`Pool: ${pool.length.toLocaleString()} | Excluidos (Monday): ${mondayDomains.length} | Ya procesados: ${processed.size} | Candidatos finales: ${candidates.length.toLocaleString()}`);

  if (candidates.length === 0) {
    if (pool.length === 0) {
      log("Pool vacío (error de descarga) — forzando re-descarga en próxima sesión.");
      domainPool = null;
    } else {
      log("Sin candidatos nuevos — todos los dominios del pool ya fueron procesados.");
    }
    return;
  }

  // ── SEED desde likes: inyectar similares de los últimos 30 dominios que el user 👍 ──
  // Esto crea un feedback loop positivo: te gustó X → próxima sesión busca sitios como X
  // Reducido de 30 → 10 para limitar coste de RapidAPI: 30 likes × 1 call = 30 hits
  // por arranque de sesión. Con 10 son 10 hits/sesión × 5 MBs = 50/día solo en seeds.
  const likedSeeds = feedback.likedDomains.slice(0, 10);
  const likedSimilarDomains = new Set();
  if (likedSeeds.length > 0) {
    log(`Seeding similar-sites desde ${likedSeeds.length} likes recientes...`);
    const simResults = await Promise.all(
      likedSeeds.map(d => findSimilarSites(d, rapidapi_key).catch(() => []))
    );
    for (const list of simResults) {
      for (const sim of list) {
        if (!mondaySet.has(sim) && !processed.has(sim)) likedSimilarDomains.add(sim);
      }
    }
    log(`  🔗 Similares de likes: +${likedSimilarDomains.size} dominios con prioridad`);
  }

  // Cola dinámica: priorizar similares-de-likes primero, después pool random
  const toProcess     = [...likedSimilarDomains, ...candidates];
  const seenInSession = new Set(toProcess);
  // Dedup por organización: si ya vimos clarin.com.ar, saltar clarin.com.br/.es/.co
  const seenOrgs = new Set(toProcess.map(coreDomain).filter(Boolean));
  let count = 0, added = 0, skipped = 0, lowScore = 0, discovered = 0, dupOrg = 0;

  while (toProcess.length > 0) {
    if (Date.now() - sessionStart >= userSessionLimitMs) {
      log(`⏱ ${Math.round(userSessionLimitMs / 60000)} minutos (límite del usuario) — auto-apagando.`);
      await setConfigValue(token, "auto_prospecting_enabled", "false");
      await setConfigValue(token, "auto_session_start", "");
      break;
    }

    // Check quota diaria del user: si ya alcanzó 75, cortar
    if (sessionUser) {
      const userSessionTotal = userTodayCount + added;
      if (userSessionTotal >= AUTOPILOT_DAILY_LIMIT) {
        log(`⏸ ${sessionUser} alcanzó ${userSessionTotal}/${AUTOPILOT_DAILY_LIMIT} autopilot hoy — sesión cortada`);
        await setConfigValue(token, "auto_prospecting_enabled", "false");
        break;
      }
    }

    // Hard cap MENSUAL — corte primario, protege contra overage tipo incidente May 02.
    const rapidMonthUsedNow = _rapidMonthStart + _rapidGlobalCounter;
    if (rapidMonthUsedNow >= rapidMonth.limit) {
      _rapidCapReached = true;
      log(`⛔ Cap MENSUAL de RapidAPI alcanzado (${rapidMonthUsedNow}/${rapidMonth.limit}) — sesión cortada hasta próximo mes.`);
      await setConfigValue(token, "auto_prospecting_enabled", "false");
      break;
    }
    // Hard cap diario — defensa secundaria
    const rapidUsedNow = _rapidStart + _rapidGlobalCounter;
    if (rapidUsedNow >= rapidUsage.limit) {
      _rapidCapReached = true;
      log(`⛔ Cap diario de RapidAPI alcanzado (${rapidUsedNow}/${rapidUsage.limit}) — sesión cortada. Reset mañana.`);
      await setConfigValue(token, "auto_prospecting_enabled", "false");
      break;
    }

    if (count % 10 === 0 && count > 0) {
      const freshCfg = await getConfig(token);
      if (freshCfg.auto_prospecting_enabled !== "true" && freshCfg.auto_prospecting_enabled !== true) {
        log("Autopilot apagado manualmente — deteniendo.");
        break;
      }
    }

    const domain = toProcess.shift();
    log(`→ [${count + 1} | +${discovered} desc | cola: ${toProcess.length}] ${domain}`);

    // Pre-filtro GRATUITO #-1: dedup por organización (clarin.com vs clarin.com.ar)
    const org = coreDomain(domain);
    if (org && seenOrgs.has(org) && !seenInSession.has(`__seed:${domain}`)) {
      // already saw a sibling in this session — skip without consuming any API
      log(`  ⊘ ${domain} — duplicado por org "${org}" (ya procesado)`);
      await markProcessed(token, [domain], "org_duplicate");
      count++; dupOrg++;
      continue;
    }
    if (org) seenOrgs.add(org);

    // Pre-filtro GRATUITO (antes de gastar créditos SimilarWeb / Apollo):
    // 0) Learning: dominio dislikeado directamente por el user
    if (feedback.dislikedDomains.has(domain.toLowerCase())) {
      log(`  ⊘ ${domain} — disliked previamente por ${sessionUser}, skip`);
      await markProcessed(token, [domain], "rejected_by_user");
      count++; skipped++;
      continue;
    }
    // 1) Blocklist corporativa / universidades / adultos / tech giants + ADMIN URLs
    const blockReason = await isDomainBlockedFull(domain, token);
    if (blockReason) {
      log(`  ⊘ ${domain} — ${blockReason}, saltado sin consumir API`);
      // Permanent: these categories won't change
      const perm = blockReason.includes("gov") || blockReason.includes("edu") || blockReason.includes("university")
        ? "government" : "blocklist";
      await markProcessed(token, [domain], perm);
      count++; skipped++;
      continue;
    }
    // 1b) TLD blacklist (permanent — a .us won't stop being .us)
    if (BLACKLIST_TLDS.some(tld => domain.endsWith(tld))) {
      log(`  ⊘ ${domain} — TLD blacklisteado, skip permanente`);
      await markProcessed(token, [domain], "tld_blacklist");
      count++; skipped++;
      continue;
    }
    // 2) GEO pre-filter por TLD si hay targets — pasa si matchea CUALQUIERA
    if (hasTargetGeo && !targetGeos.some(g => matchesTargetGeoByTLD(domain, g))) {
      log(`  ⊘ ${domain} — TLD no coincide con ${targetGeos.join("+")}`);
      // Not permanent — user may change targets in next session
      await markProcessed(token, [domain], "tld_target_mismatch");
      count++; skipped++;
      continue;
    }

    // Paso 1: tráfico + contenido de página en paralelo (sin Gemini)
    const [trafficData, pageContent] = await Promise.all([
      getTrafficData(domain, rapidapi_key),
      fetchPageContent(domain),
    ]);

    let { visits, pagesPerVisit, topCountry, topCountries3, error: rapidError } = trafficData;
    const _isoFromName2 = (name) => Object.keys(COUNTRY_CODES).find(k => COUNTRY_CODES[k] === name);
    const geosAllIsoAuto = Array.isArray(topCountries3) && topCountries3.length
      ? topCountries3
      : (topCountry ? [_isoFromName2(topCountry) || topCountry] : []);

    // Si el RapidAPI devolvió error tras 3 retries, no descartar permanentemente
    if (rapidError) {
      log(`  ⚠ RapidAPI error (${rapidError}) — re-evaluar después`);
      await markProcessed(token, [domain], "api_error");
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Fallback GEO orden: SimilarWeb > Radar hint > TLD
    let geoSource = topCountry ? "similarweb" : null;
    if (!topCountry && radarPool?.has(domain)) {
      const cc = [...radarPool.get(domain)][0];
      if (cc && COUNTRY_CODES[cc]) {
        topCountry = COUNTRY_CODES[cc];
        geoSource  = "radar";
        log(`  ℹ ${domain} — GEO del Radar: ${topCountry}`);
      }
    }
    if (!topCountry) {
      const inferred = inferCountryFromTLD(domain);
      if (inferred) {
        topCountry = inferred;
        geoSource  = "tld";
        log(`  ℹ ${domain} — GEO inferido por TLD: ${topCountry}`);
      }
    }

    // Persistir al cache compartido (alpha-2 code)
    if (topCountry && geoSource) {
      const cc = Object.keys(COUNTRY_CODES).find(k => COUNTRY_CODES[k] === topCountry) || topCountry.slice(0, 2).toUpperCase();
      const conf = geoSource === "similarweb" ? 9 : geoSource === "radar" ? 8 : 3;
      setCachedGeo(token, domain, cc, geoSource, conf).catch(() => {});
    }

    // Blacklist geográfica post-traffic — permanente, el país no va a cambiar
    if (topCountry && GEO_BLACKLIST_COUNTRIES.has(topCountry)) {
      log(`  ⊘ ${domain} — topCountry ${topCountry} blacklisteado, skip permanente`);
      await markProcessed(token, [domain], "country_blacklist");
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Filtro primario: tráfico mínimo. Si visits=null, puede ser API error vs site sin tráfico.
    // Distinguimos: null = api_error (re-evaluable), número bajo = traffic_low (re-evaluable en 60 días)
    if (visits === null || visits === undefined) {
      log(`  ⚠ Tráfico N/A (posible API error) — descartado temporalmente`);
      await markProcessed(token, [domain], "api_error");
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }
    // Threshold REAL del negocio: visits × pagesPerVisit (= pageViews mensuales).
    // Si pagesPerVisit no vino en la respuesta, asumimos 1.0 (conservador) para
    // que solo descarte sitios que ni siquiera con un PPV optimista lleguen al
    // mínimo. Esto evita false-negatives por data faltante.
    const ppvSafe   = (typeof pagesPerVisit === "number" && pagesPerVisit > 0) ? pagesPerVisit : 1.0;
    const pageViews = Math.round(visits * ppvSafe);
    if (pageViews < sessionMinTraffic) {
      log(`  ✗ pageViews (${Math.round(pageViews/1000)}K = ${Math.round(visits/1000)}K visits × ${ppvSafe.toFixed(1)} ppv) < ${(sessionMinTraffic/1000)}K — descartado`);
      await markProcessed(token, [domain], "traffic_low");
      count++; skipped++;
      trackAutopilotEvent("filtered", domain);
      trackAutopilotEvent("processed", domain);
      flushAutopilotStats(token).catch(() => {});
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }
    // Cap superior — sitios mega-grandes no son target ADEQ
    if (visits > MAX_TRAFFIC_FOR_PROSPECT) {
      log(`  ✗ Tráfico (${Math.round(visits/1_000_000)}M) > ${MAX_TRAFFIC_FOR_PROSPECT/1_000_000}M cap — too big for ADEQ`);
      await markProcessed(token, [domain], "traffic_too_high");
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Detectar idioma robustamente (text + html + hreflang + jsonLd + url + geo + tld + Claude arbiter)
    const _autopilotLangDet = await detectLanguageRobust({
      htmlLang:   pageContent?.htmlLang,
      ogLocale:   pageContent?.ogLocale,
      hreflang:   pageContent?.hreflang,
      jsonLdLang: pageContent?.jsonLdLang,
      pathLang:   pageContent?.pathLang,
      textSample: pageContent?.textSample,
      geo:        topCountry,
      domain,
    }, { token });
    const language    = _autopilotLangDet.lang;
    const category    = pageContent?.category || "";
    const contactName = "";
    const adNetworks  = pageContent?.adNetworks || [];
    const pageTitle   = pageContent?.title       || "";

    // Filtro por categoría objetivo (si está configurado) — no permanente
    if (targetCategory && category !== targetCategory) {
      log(`  ✗ Categoría ${category} ≠ objetivo ${targetCategory} — descartado`);
      await markProcessed(token, [domain], "category_mismatch");
      count++; skipped++;
      trackAutopilotEvent("filtered", domain);
      trackAutopilotEvent("processed", domain);
      flushAutopilotStats(token).catch(() => {});
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Filtro duro de GEO — no permanente (user puede cambiar targets)
    if (hasTargetGeo && topCountry && !allowedCountries.has(topCountry)) {
      log(`  ✗ GEO ${topCountry} ∉ {${[...allowedCountries].slice(0,5).join(",")}${allowedCountries.size>5?"...":""}} — descartado`);
      await markProcessed(token, [domain], "geo_target_mismatch");
      count++; skipped++;
      trackAutopilotEvent("filtered", domain);
      trackAutopilotEvent("processed", domain);
      flushAutopilotStats(token).catch(() => {});
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Scoring: tráfico + categoría + contacto + geo target + contenido accesible
    const rawScore = scoreCandidate({ visits, category, topCountry, contactName, emails: [], pageContent, allowedCountries });

    // Penalización por patrones de rechazo aprendidos (globales) — pesa poco
    // para no contaminar el ranking de un user con preferencias de otros.
    const catPenalty = Math.min(10, (rejectionPatterns.categories[category] || 0) * 2);
    const geoPenalty = Math.min(5,  (rejectionPatterns.geos[topCountry]     || 0) * 1);

    // Penalización POR USUARIO (feedback like/dislike explícito) — mucho más fuerte
    const userCatDislikes = feedback.dislikedCategories.get(category) || 0;
    const userGeoDislikes = feedback.dislikedGeos.get(topCountry)     || 0;
    const userCatPenalty  = Math.min(40, userCatDislikes * 10);       // -10 por cada dislike, max -40
    const userGeoPenalty  = Math.min(30, userGeoDislikes * 8);

    // Bonus por LIKES: si la categoría o geo matchea patterns que el user aprobó
    const userCatLikes = feedback.likedCategories.get(category) || 0;
    const userGeoLikes = feedback.likedGeos.get(topCountry)     || 0;
    const userCatBonus = Math.min(30, userCatLikes * 6);  // +6 por like, max +30
    const userGeoBonus = Math.min(20, userGeoLikes * 4);
    // Bonus extra si viene de la inyección de similares de likes
    const fromLikedSeed = likedSimilarDomains.has(domain) ? 15 : 0;

    const finalScore = rawScore - catPenalty - geoPenalty - userCatPenalty - userGeoPenalty
                     + userCatBonus + userGeoBonus + fromLikedSeed;

    // GATE PRINCIPAL: solo el threshold de pageViews (350K). Sin score check —
    // si el sitio supera 350K pageViews ya es candidato. El score se persiste
    // en review_queue por si en futuro el admin quiere ordenar/filtrar por él.
    // (decisión user 2026-05-08)

    // Paso 2: emails + pitch + sitios similares en paralelo
    // Apollo solo si quedan créditos disponibles hoy
    const apolloMonthRem = (apolloUsage.monthLimit ?? APOLLO_MONTHLY_HARD_CAP) - (apolloUsage.usedThisMonth ?? 0);
    const canUseApollo = apollo_api_key
      && (apolloUsage.usedToday + apolloCallsThisSession) < apolloUsage.limit
      && apolloMonthRem > 0;
    if (!canUseApollo && apollo_api_key) {
      const reason = apolloMonthRem <= 0
        ? `cap MENSUAL alcanzado (${apolloUsage.usedThisMonth}/${apolloUsage.monthLimit}) — fallback a scraping`
        : `límite diario (${apolloUsage.limit}/día) alcanzado`;
      log(`  ⚠️ Apollo skip: ${reason}`);
    }

    // Rotación 50/50 Apollo vs scrape (user 2026-05-13) — ahorra créditos Apollo.
    const useApolloFirst = canUseApollo && Math.random() < 0.5;
    const similarPromise = findSimilarSites(domain, rapidapi_key);
    let apolloRes = null;
    let scraperEmails = [];
    if (useApolloFirst) {
      apolloRes = await findBestApolloEmail(domain, apollo_api_key, token, { traffic: visits, allowUnlock: true })
        .then(r => { if (r?.source === "unlocked") apolloCallsThisSession += 1; return r; })
        .catch(() => null);
      if (!apolloRes?.email) scraperEmails = await scrapeEmailsForDomain(domain).catch(() => []);
    } else {
      scraperEmails = await scrapeEmailsForDomain(domain).catch(() => []);
      if (scraperEmails.length === 0 && canUseApollo) {
        apolloRes = await findBestApolloEmail(domain, apollo_api_key, token, { traffic: visits, allowUnlock: true })
          .then(r => { if (r?.source === "unlocked") apolloCallsThisSession += 1; return r; })
          .catch(() => null);
      }
    }
    const similarSites = await similarPromise;
    const apolloContactNameAuto = apolloRes?.contact_name || "";
    const apolloPhoneAuto       = apolloRes?.phone || "";
    const apolloEmailAuto = apolloRes?.email ? [apolloRes.email] : [];
    const rawEmailsAuto = [...new Set([...apolloEmailAuto, ...scraperEmails])];
    const emails = await validateEmailsBatch(rawEmailsAuto);
    if (rawEmailsAuto.length !== emails.length) {
      log(`  📧 ${domain}: ${rawEmailsAuto.length} → ${emails.length} emails (apollo:${apolloRes?.source||"none"})`);
    }

    // Score final con emails encontrados
    const scoreWithEmails = finalScore + (emails.length > 0 ? 10 : 0);

    // Inyectar sitios similares en la cola
    let newFromSimilar = 0;
    for (const sim of similarSites) {
      if (!seenInSession.has(sim) && !processed.has(sim) && !mondaySet.has(sim)) {
        seenInSession.add(sim);
        toProcess.push(sim);
        newFromSimilar++;
        discovered++;
      }
    }
    if (newFromSimilar > 0) log(`  🔗 +${newFromSimilar} sitios similares`);

    if (adNetworks.length > 0) log(`  📡 Ad networks: ${adNetworks.join(", ")}`);

    const saveOk = await saveToReviewQueue(token, {
      domain,
      traffic:       visits,
      geo:           topCountry,
      geosAll:       geosAllIsoAuto,
      language,
      category,
      contactName:   apolloContactNameAuto || contactName,
      contactPhone:  apolloPhoneAuto,
      emails,
      pitch:         "",
      pitchSubject:  "",
      pitchSubjects: [],
      score:         scoreWithEmails,
      adNetworks,
      pageTitle,
      createdBy:     sessionUser,
    });
    if (saveOk) {
      await markProcessed(token, [domain], "added_to_review");
      count++; added++;
      trackAutopilotEvent("added", domain);
      // Bump counter global diario (cap safety net 300/día autopilot)
      bumpAutopilotDailyCounter(token, 1).catch(() => {});
    } else {
      // Save falló — no marcar como added pero sí como processed para no re-evaluar
      await markProcessed(token, [domain], "save_failed");
      count++; skipped++;
      trackAutopilotEvent("filtered", domain);
    }
    trackAutopilotEvent("processed", domain);
    flushAutopilotStats(token).catch(() => {});

    log(`  ✓ score:${scoreWithEmails} | ${Math.round(visits/1000)}K | ${topCountry||"N/A"} | ${language} | ${category} | ${contactName||"—"} | ${emails.length} email(s)`);
    await sleep(DOMAIN_DELAY_MS);
  }

  log(`Sesión completada — ${count} procesados | ${added} guardados | ${skipped} skipped | ${lowScore} bajo score | ${dupOrg} dup-org | ${discovered} descubiertos vía similares`);
  log(`RapidAPI esta sesión: ${_rapidGlobalCounter} hits`);
  await saveApolloUsage(token, apolloCallsThisSession, apolloUsage.today);
  await saveRapidApiUsage(token, _rapidGlobalCounter, rapidUsage.today);
  await saveRapidApiMonthlyUsage(token, _rapidGlobalCounter, rapidMonth.period);
  await flushAutopilotStats(token, true).catch(() => {});

  // ── PERSIST STATS para ratio adaptativo ──
  try {
    const fmtP = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit" });
    const todayP = fmtP.format(new Date());
    const statsPrev = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(autopilot_majestic_leads_today,autopilot_similar_leads_today,autopilot_stats_date)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const prevRows = await statsPrev.json();
    const prevMap = {}; prevRows.forEach(r => { prevMap[r.key] = r.value; });
    const dateMatches = prevMap.autopilot_stats_date === todayP;
    const prevMaj = dateMatches ? parseInt(prevMap.autopilot_majestic_leads_today || "0", 10) : 0;
    const prevSim = dateMatches ? parseInt(prevMap.autopilot_similar_leads_today || "0", 10) : 0;
    const newMaj  = _autopilotMode === "majestic" ? prevMaj + added : prevMaj;
    const newSim  = _autopilotMode === "similar_discovery" ? prevSim + added : prevSim;
    await Promise.all([
      setConfigValue(token, "autopilot_majestic_leads_today", String(newMaj)),
      setConfigValue(token, "autopilot_similar_leads_today",  String(newSim)),
      setConfigValue(token, "autopilot_stats_date",           todayP),
    ]);
    log(`📊 Stats actualizadas: Majestic=${newMaj} · Similar=${newSim} (esta sesión: ${_autopilotMode} +${added})`);
  } catch (e) { log(`⚠️ persist autopilot stats: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════
// 🤖 AGENT MB — Auto end-to-end prospecting (push Monday + send Gmail)
// Activado solo para users en toolbar_config.agent_enabled_users (JSON array).
// Procesa review_queue → quality gates → Claude pitch → Monday push → Gmail send.
// Simula 100% el workflow del MB humano.
// ════════════════════════════════════════════════════════════════

const AGENT_DEFAULTS = {
  threshold_traffic:    400_000,   // único filtro de calidad real (decisión user 2026-05-18)
  max_per_day:          30,        // 5 slots × 6 = 30/día
  active_hours_start:   9,         // 9am España (CET/CEST)
  active_hours_end:     23,        // 23hs España
  active_timezone:      "Europe/Madrid",
  per_cycle_limit:      6,         // max 6 leads por slot del cron 9/12/15/18/20 Madrid L-V
  monday_board_id:      1420268379,
  // DEPRECATED (2026-05-18):
  //   threshold_score: 40    → ya no se filtra por score. Sólo hard gates en scoreWebsite()
  //   cycle_interval_sec     → ya no se polleaba en intervalo, son slots fijos
};

function _agentCfg(cfg) {
  const get = (key, dflt) => {
    const v = cfg[`agent_${key}`];
    if (v == null || v === "") return dflt;
    if (typeof dflt === "number") {
      const n = parseInt(v, 10);
      return Number.isFinite(n) ? n : dflt; // ← 0 es válido (no caer al default)
    }
    return v;
  };
  let focus = { geosPriority: [], geosExcluded: [], categoriesPriority: [], weeklyTarget: 0, dailyOverride: 0 };
  try {
    const raw = cfg.agent_focus_config;
    if (raw) {
      const f = JSON.parse(raw);
      focus = {
        geosPriority:       Array.isArray(f.geos_priority) ? f.geos_priority.map(s => String(s).toUpperCase().trim()).filter(Boolean) : [],
        geosExcluded:       Array.isArray(f.geos_excluded) ? f.geos_excluded.map(s => String(s).toUpperCase().trim()).filter(Boolean) : [],
        categoriesPriority: Array.isArray(f.categories_priority) ? f.categories_priority.map(s => String(s).toLowerCase().trim()).filter(Boolean) : [],
        weeklyTarget:       parseInt(f.weekly_target, 10) || 0,
        dailyOverride:      parseInt(f.daily_override, 10) || 0,
      };
    }
  } catch {}
  return {
    thresholdTraffic: get("threshold_traffic",    AGENT_DEFAULTS.threshold_traffic),
    maxPerDay:        focus.dailyOverride || get("max_per_day", AGENT_DEFAULTS.max_per_day),
    activeStart:      get("active_hours_start",   AGENT_DEFAULTS.active_hours_start),
    activeEnd:        get("active_hours_end",     AGENT_DEFAULTS.active_hours_end),
    perCycleLimit:    get("per_cycle_limit",      AGENT_DEFAULTS.per_cycle_limit),
    focus,
  };
}

// Hora actual España vía Intl (maneja CET/CEST automático).
function _spainHour() {
  const fmt = new Intl.DateTimeFormat("es-ES", {
    timeZone: "Europe/Madrid", hour: "numeric", hour12: false,
  });
  return parseInt(fmt.format(new Date()), 10);
}

// 0=Sun, 1=Mon, ..., 6=Sat — en timezone Madrid (helper para logging)
function _spainWeekday() {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "Europe/Madrid", weekday: "short",
  });
  const map = { Sun:0, Mon:1, Tue:2, Wed:3, Thu:4, Fri:5, Sat:6 };
  return map[fmt.format(new Date())] ?? 0;
}
// _isWeekendSpain ya existe en línea 532

// Devuelve true si AHORA estamos FUERA de las active hours (9-20 España).
function _isOutsideActiveHours(activeStart, activeEnd) {
  const h = _spainHour();
  if (activeStart < activeEnd) return h < activeStart || h >= activeEnd;
  // wrap (raro pero soportado): si start > end (ej. 20-9 → trabaja noche)
  // Activo cuando h>=start O h<end. Fuera de rango: h<start && h>=end.
  return h < activeStart && h >= activeEnd;
  // (mantenido — si activeStart=activeEnd → siempre fuera, deseable como kill switch)
}

// Pickea un draft del MB desde toolbar_pitch_drafts para el idioma del lead.
// Prefiere drafts del user_email impersonado; cae a is_default=true si no hay
// custom. Devuelve { body, subjects } compatible con fillTemplate, o null si
// no encuentra ningún draft (caller cae a templates.js baked).
const _draftsCache = new Map(); // key: `${userEmail}|${lang}` → { drafts, ts }
const DRAFTS_CACHE_TTL = 5 * 60 * 1000; // 5 min — drafts cambian rara vez

async function pickDbDraft(token, userEmail, language) {
  const lang = (language || "en").toLowerCase().slice(0, 2);
  const cacheKey = `${userEmail}|${lang}`;
  let cached = _draftsCache.get(cacheKey);
  if (!cached || Date.now() - cached.ts > DRAFTS_CACHE_TTL) {
    // Fetch: drafts del user impersonado + defaults globales para ese lang
    const url = `${SUPABASE_URL}/rest/v1/toolbar_pitch_drafts?language=eq.${encodeURIComponent(lang)}&or=(user_email.eq.${encodeURIComponent(userEmail)},is_default.eq.true)&select=id,name,subject,body,is_default,priority&order=priority.asc,is_default.desc`;
    const res = await fetch(url, {
      headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` },
    });
    if (!res.ok) {
      _draftsCache.set(cacheKey, { drafts: [], ts: Date.now() });
      return null;
    }
    const rows = await res.json();
    cached = { drafts: Array.isArray(rows) ? rows : [], ts: Date.now() };
    _draftsCache.set(cacheKey, cached);
  }
  const drafts = cached.drafts;
  if (!drafts.length) return null;
  // Pickear random — todos los drafts del MB+defaults son válidos
  const pick = drafts[Math.floor(Math.random() * drafts.length)];
  return {
    body: pick.body || "",
    subjects: pick.subject ? [pick.subject] : ["Pregunta sobre {{domain}}"],
  };
}

// ════════════════════════════════════════════════════════════════
// pickAnyTemplate — combina baked + DB drafts en un solo pool y elige
// uno ponderado por open rate histórico.
//
// Política user 2026-05-18:
//   • Combina templates baked (auto-prospector/templates.js, los 3 nuevos)
//     con los drafts en toolbar_pitch_drafts (3 defaults + custom de cada MB).
//   • Selección random ponderada por open_rate de los últimos 30 días.
//     Smoothing Laplace: (opens+1)/(sends+2) — templates nuevos arrancan en 50%
//     y van bajando/subiendo según resultados reales.
//   • Devuelve { template, templateId } para que el caller loggee qué template usó.
//
// IDs:
//   baked_<lang>_<idx>   → templates.js
//   db_<id>              → toolbar_pitch_drafts
// ════════════════════════════════════════════════════════════════

// Cache de scores (open rates) — refresh cada 10 min. La query agrupa
// agent_actions+email_opens y es relativamente cara, no la queremos hacer
// en cada envío del Agent.
const _templateScoresCache = { map: null, ts: 0 };
const TEMPLATE_SCORES_TTL_MS = 10 * 60 * 1000;

async function _getTemplateScores(token) {
  if (_templateScoresCache.map && Date.now() - _templateScoresCache.ts < TEMPLATE_SCORES_TTL_MS) {
    return _templateScoresCache.map;
  }
  const map = new Map(); // templateId → { sends, opens }
  try {
    const cutoff = new Date(Date.now() - 30 * 86400_000).toISOString();
    // 1. Sends por template (últimos 30d)
    const sentRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=eq.sent&template_id=not.is.null&created_at=gte.${cutoff}&select=id,template_id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (sentRes.ok) {
      const rows = await sentRes.json();
      const actionToTpl = new Map();
      rows.forEach(r => {
        const tid = r.template_id;
        if (!tid) return;
        actionToTpl.set(r.id, tid);
        if (!map.has(tid)) map.set(tid, { sends: 0, opens: 0 });
        map.get(tid).sends++;
      });
      // 2. Opens por action_id (cruzar con template_id)
      const actionIds = [...actionToTpl.keys()];
      if (actionIds.length > 0) {
        const BATCH = 200;
        for (let i = 0; i < actionIds.length; i += BATCH) {
          const slice = actionIds.slice(i, i + BATCH);
          const inList = slice.join(",");
          const opRes = await fetch(
            `${SUPABASE_URL}/rest/v1/toolbar_email_opens?agent_action_id=in.(${inList})&select=agent_action_id`,
            { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
          );
          if (opRes.ok) {
            const opens = await opRes.json();
            const seenAction = new Set();
            opens.forEach(o => {
              const aid = o.agent_action_id;
              if (seenAction.has(aid)) return; // 1 open por action máx (anti-pixel-reload)
              seenAction.add(aid);
              const tid = actionToTpl.get(aid);
              if (tid && map.has(tid)) map.get(tid).opens++;
            });
          }
        }
      }
    }
  } catch (e) { log(`⚠️ _getTemplateScores: ${e.message}`); }
  _templateScoresCache.map = map;
  _templateScoresCache.ts = Date.now();
  return map;
}

async function pickAnyTemplate(token, userEmail, language) {
  const lang = (language || "en").toLowerCase().slice(0, 2);

  // 1. Recolectar pool combinado (baked + DB drafts)
  const baked = getBakedTemplates(lang).map(t => ({
    id: t.id,
    body: t.body,
    subjects: t.subjects,
    source: "baked",
  }));

  // Reutilizamos el cache de pickDbDraft via fetch directo a la misma URL
  // (queremos `id` para identificar cada draft DB).
  const cacheKey = `${userEmail}|${lang}`;
  let cached = _draftsCache.get(cacheKey);
  if (!cached || Date.now() - cached.ts > DRAFTS_CACHE_TTL) {
    try {
      const url = `${SUPABASE_URL}/rest/v1/toolbar_pitch_drafts?language=eq.${encodeURIComponent(lang)}&or=(user_email.eq.${encodeURIComponent(userEmail)},is_default.eq.true)&select=id,name,subject,body,is_default,priority&order=priority.asc,is_default.desc`;
      const res = await fetch(url, {
        headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` },
      });
      cached = { drafts: res.ok ? (await res.json()) : [], ts: Date.now() };
      _draftsCache.set(cacheKey, cached);
    } catch { cached = { drafts: [], ts: Date.now() }; }
  }
  const dbDrafts = (cached.drafts || []).map(d => ({
    id: `db_${d.id}`,
    body: d.body || "",
    subjects: d.subject ? [d.subject] : ["Pregunta sobre {{domain}}"],
    source: "db_draft",
  }));

  const pool = [...baked, ...dbDrafts];
  if (pool.length === 0) return { template: null, templateId: null };

  // 2. Scores (open rate Laplace-smoothed + warmup bonus para templates nuevos)
  // El bonus de exploración hace que templates con <30 envíos reciban más turnos
  // hasta acumular data confiable. Después de 30 envíos, la fórmula Laplace pura
  // toma protagonismo y el peso converge al open rate real.
  const scores = await _getTemplateScores(token).catch(() => new Map());
  const WARMUP_SENDS = 30;
  const weighted = pool.map(t => {
    const s = scores.get(t.id) || { sends: 0, opens: 0 };
    const smoothedRate = (s.opens + 1) / (s.sends + 2);
    // Warmup: si tiene < 30 sends, bonus proporcional. Decae linealmente.
    const warmupBonus = Math.max(0, (WARMUP_SENDS - s.sends) / WARMUP_SENDS) * 0.3;
    return { ...t, weight: smoothedRate + warmupBonus };
  });

  // 3. Weighted random pick
  const totalWeight = weighted.reduce((sum, t) => sum + t.weight, 0);
  let r = Math.random() * totalWeight;
  let picked = weighted[weighted.length - 1];
  for (const t of weighted) {
    r -= t.weight;
    if (r <= 0) { picked = t; break; }
  }

  return {
    template: { body: picked.body, subjects: picked.subjects },
    templateId: picked.id,
  };
}

// ════════════════════════════════════════════════════════════════
// 🔄 RE-ENGAGEMENT — INACTIVE hasta agent_reengagement_enabled=true
// ════════════════════════════════════════════════════════════════
//
// Lógica: detectar emails enviados por el agente que NO se abrieron en N días
// y reenviar al siguiente contacto del array de emails del lead. Update Monday
// item email column para que la app de reply-detection del user mire el nuevo.
//
// Reglas (configurables via toolbar_config):
//   agent_reengagement_enabled       (default false — KILL SWITCH)
//   agent_reengagement_wait_days     (default 5)
//   agent_reengagement_max_attempts  (default 3)
//
// NUNCA dispara si:
//   - El email original ABRIÓ (open_rate pixel hit) → vio el mail, no responder
//     es decisión humana. Mandar a colega sería invasivo.
//   - El item Monday pasó a "Negociación" (alguien respondió por la app externa).
//   - El dominio recibió mail en los últimos 5d (anti-spam intra-dominio).
//   - Ya se hicieron N attempts (default 3) — entonces freeze 60d.

// 1. Query: encontrar SENTs sin opens y sin re_sents recientes
async function findUnopenedSends(token, waitDays = 5) {
  try {
    const cutoff = new Date(Date.now() - waitDays * 86400_000).toISOString();
    // Buscar agent_actions sent del último mes que ya tienen >= waitDays
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=eq.sent&created_at=lt.${cutoff}&created_at=gte.${new Date(Date.now() - 30 * 86400_000).toISOString()}&select=id,domain,user_email,details,created_at&order=created_at.asc&limit=200`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return [];
    const sends = await res.json();
    if (!Array.isArray(sends) || sends.length === 0) return [];

    // Para cada send, verificar: ¿tiene open_rate hit? ¿tiene re_sent ya?
    const candidates = [];
    for (const s of sends) {
      // Check opens — si abrió, NO re-engaging (mandó al lead, decidió no responder)
      const opensRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_email_opens?agent_action_id=eq.${s.id}&select=id&limit=1`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      const opens = opensRes.ok ? await opensRes.json() : [];
      if (Array.isArray(opens) && opens.length > 0) continue; // abrió → skip

      // Check re_sent — ya intentamos re-engaging este envío?
      const reSentRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=eq.re_sent&domain=eq.${encodeURIComponent(s.domain)}&select=id&limit=1`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      const reSents = reSentRes.ok ? await reSentRes.json() : [];
      if (Array.isArray(reSents) && reSents.length > 0) continue; // ya re-engagéd

      candidates.push(s);
    }
    return candidates;
  } catch (e) {
    log(`⚠️ findUnopenedSends error: ${e.message}`);
    return [];
  }
}

// 2. Para un dominio, contar cuántos sends totales (incluyendo re_sent) hubo
async function countAttemptsForDomain(token, domain) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?domain=eq.${encodeURIComponent(domain)}&action=in.(sent,re_sent)&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("content-range") || "";
    return parseInt(range.match(/\/(\d+)$/)?.[1] || "0", 10);
  } catch { return 0; }
}

// 3. Pickea el siguiente email candidato (B/C/D) del array del review_queue.
// Excluye: el email original A, bounced, y los que ya fueron usados en re_sent.
async function pickNextEmailCandidate(token, domain, excludeEmails = []) {
  try {
    // 3.a Lee review_queue para ese dominio
    const rqRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?domain=eq.${encodeURIComponent(domain)}&select=id,emails,language,category,contact_name,contact_phone,monday_item_id&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rq = await rqRes.json();
    if (!Array.isArray(rq) || rq.length === 0) return null;
    const lead = rq[0];
    const emails = Array.isArray(lead.emails) ? lead.emails : [];

    // 3.b Excluir bounced
    await loadBouncedEmails(token);
    const filtered = emails.filter(e => {
      if (!e || excludeEmails.includes(e.toLowerCase())) return false;
      if (isBouncedSync(e)) return false;
      const score = rankEmail(e, domain, lead.category || "");
      return score >= 0;
    });
    if (!filtered.length) return null;
    return { email: filtered[0], lead };
  } catch (e) {
    log(`⚠️ pickNextEmailCandidate ${domain}: ${e.message}`);
    return null;
  }
}

// 4. Update Monday item email column al nuevo email B.
async function updateMondayItemEmail(monday_api_key, itemId, boardId, newEmail) {
  if (!itemId || !newEmail) return false;
  const cols = JSON.stringify({ [MONDAY_COL_EMAIL]: { email: newEmail, text: newEmail } });
  const query = `mutation ($board: ID!, $item: ID!, $cols: JSON!) {
    change_multiple_column_values (board_id: $board, item_id: $item, column_values: $cols) { id }
  }`;
  try {
    const res = await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": monday_api_key, "API-Version": "2024-01" },
      body: JSON.stringify({ query, variables: { board: boardId, item: itemId, cols } }),
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) return false;
    const data = await res.json();
    return !data?.errors;
  } catch { return false; }
}

// 4b. Update completo cuando dispara Email Futuro:
//     - Email column ← future_email
//     - Fecha FU1     ← today + 5 días
//     - Fecha FU2     ← today + 10 días
async function updateMondayReengagementDispatch(monday_api_key, itemId, boardId, newEmail) {
  if (!itemId || !newEmail) return false;
  const today = new Date();
  const isoDate = (d) => d.toISOString().slice(0, 10); // "YYYY-MM-DD"
  const fu1 = new Date(today.getTime() + 5  * 86_400_000);
  const fu2 = new Date(today.getTime() + 10 * 86_400_000);
  const cols = JSON.stringify({
    [MONDAY_COL_EMAIL]: { email: newEmail, text: newEmail },
    [MONDAY_COL_FU1]:   { date: isoDate(fu1) },
    [MONDAY_COL_FU2]:   { date: isoDate(fu2) },
  });
  const query = `mutation ($board: ID!, $item: ID!, $cols: JSON!) {
    change_multiple_column_values (board_id: $board, item_id: $item, column_values: $cols) { id }
  }`;
  try {
    const res = await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": monday_api_key, "API-Version": "2024-01" },
      body: JSON.stringify({ query, variables: { board: boardId, item: itemId, cols } }),
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) {
      log(`⚠️ updateMondayReengagementDispatch HTTP ${res.status}`);
      return false;
    }
    const data = await res.json();
    if (data?.errors) {
      log(`⚠️ updateMondayReengagementDispatch errors: ${JSON.stringify(data.errors).slice(0, 200)}`);
      return false;
    }
    return true;
  } catch (e) {
    log(`⚠️ updateMondayReengagementDispatch exception: ${e.message}`);
    return false;
  }
}

// 5. Main re-engagement cycle — corre del main loop cada 60 iters.
// SOLO se ejecuta si agent_reengagement_enabled=true en toolbar_config.
async function runReengagementCycle(token) {
  try {
    const cfg = await getConfig(token);
    const enabled = String(cfg.agent_reengagement_enabled || "").toLowerCase() === "true";
    if (!enabled) return; // KILL SWITCH

    const waitDays    = parseInt(cfg.agent_reengagement_wait_days || "5", 10);
    const maxAttempts = parseInt(cfg.agent_reengagement_max_attempts || "3", 10);

    log(`🔄 Re-engagement cycle: buscando sends sin opens >= ${waitDays}d...`);
    const candidates = await findUnopenedSends(token, waitDays);
    if (!candidates.length) {
      log(`🔄 Re-engagement: 0 candidatos`);
      return;
    }
    log(`🔄 Re-engagement: ${candidates.length} candidatos a procesar`);

    for (const orig of candidates) {
      const domain = orig.domain;
      const userEmail = orig.user_email;
      const originalEmail = (orig.details?.email || "").toLowerCase();

      // Cap de attempts
      const attempts = await countAttemptsForDomain(token, domain);
      if (attempts >= maxAttempts) {
        log(`  🧊 ${domain}: ${attempts} attempts sin opens — freeze 60d`);
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_frozen_leads`, {
          method: "POST",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" },
          body: JSON.stringify({
            domain, frozen_until: new Date(Date.now() + 60 * 86400_000).toISOString(),
            attempt_count: attempts, last_error: `re_engagement_max_attempts_${maxAttempts}_no_opens`,
            source: "agent_reengagement", uploaded_by: userEmail, updated_at: new Date().toISOString(),
          }),
        }).catch(() => {});
        await logAgentAction(token, userEmail, {
          domain, action: "skipped", reason: "reengagement_max_attempts_reached",
          details: { attempts, max: maxAttempts },
        });
        continue;
      }

      // Pickear siguiente email
      const exclude = [originalEmail];
      const next = await pickNextEmailCandidate(token, domain, exclude);
      if (!next) {
        log(`  ⏭ ${domain}: sin emails alternativos disponibles`);
        await logAgentAction(token, userEmail, {
          domain, action: "skipped", reason: "reengagement_no_alt_email",
          details: { tried: originalEmail },
        });
        continue;
      }
      const { email: newEmail, lead } = next;

      // Pickear template del pool combinado (baked + DB drafts), ponderado
      // por open rate. Misma lógica que el envío normal del Agent.
      let pitch = null;
      try {
        const senderName = getSenderName(userEmail);
        const picked = await pickAnyTemplate(token, userEmail, lead.language || "en");
        if (picked.template) {
          pitch = fillTemplate(picked.template, { domain, geo: "", traffic: 0, senderName });
          pitch._templateId = picked.templateId;
        } else {
          const tpl = pickRandomTemplate(lead.language || "en");
          pitch = fillTemplate(tpl, { domain, geo: "", traffic: 0, senderName });
        }
      } catch (e) {
        log(`  ⚠️ ${domain}: pitch error ${e.message} — skip`);
        continue;
      }
      const subject = pitch.subjects?.[0] || `Pregunta sobre ${domain}`;

      // Pre-flight: bounced + sendtrack 30d
      try {
        const cutoff30 = new Date(Date.now() - 30 * 86400_000).toISOString().split("T")[0];
        const stRes = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_sendtrack?domain=eq.${encodeURIComponent(domain)}&send_date=gte.${cutoff30}&select=email&limit=10`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
        );
        const sentRows = await stRes.json();
        const sentEmails = (Array.isArray(sentRows) ? sentRows : []).map(r => (r.email || "").toLowerCase());
        if (sentEmails.includes(newEmail.toLowerCase())) {
          log(`  ⏭ ${domain}: ${newEmail} ya recibió mail en 30d — skip`);
          continue;
        }
      } catch {}

      // Reservar slot agent_actions ANTES del send (anti-crash)
      let reservedId = null;
      try {
        const reserveRes = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions`, {
          method: "POST",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=representation" },
          body: JSON.stringify({
            user_email: userEmail, domain, action: "reserved",
            pitch_subject: subject,
            details: { email: newEmail, source: "reengagement", original_email: originalEmail, attempt: attempts + 1, language: lead.language },
          }),
        });
        const reserved = await reserveRes.json();
        reservedId = reserved?.[0]?.id || null;
      } catch {}

      // Send Gmail
      try {
        await sendGmailServer(token, userEmail, { to: newEmail, subject, body: pitch.body, agentActionId: reservedId });
      } catch (err) {
        log(`  ❌ ${domain} re-engagement send fail: ${err.message}`);
        if (reservedId) {
          fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions?id=eq.${reservedId}`, {
            method: "PATCH",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({ action: "failed_reserved", reason: "reengagement_send_failed" }),
          }).catch(() => {});
        }
        continue;
      }

      // Confirmar action=re_sent
      if (reservedId) {
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions?id=eq.${reservedId}`, {
          method: "PATCH",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ action: "re_sent" }),
        });
      }

      // Update Monday — email + reset FU1/FU2 para que Monday continúe el ciclo
      const mondayItemId = lead.monday_item_id;
      const mondayApiKey = (cfg[`monday_api_key_${userEmail.toLowerCase()}`] || cfg.monday_api_key || "").trim();
      if (mondayItemId && mondayApiKey) {
        const ok = await updateMondayReengagementDispatch(mondayApiKey, mondayItemId, AGENT_DEFAULTS.monday_board_id, newEmail);
        log(`  ${ok ? "✅" : "⚠️"} ${domain}: Monday email + FU1/FU2 ${ok ? "actualizados" : "FAIL"} → ${newEmail}`);
      }

      // Sendtrack
      await fetch(`${SUPABASE_URL}/rest/v1/toolbar_sendtrack`, {
        method: "POST",
        headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" },
        body: JSON.stringify({ domain, send_date: new Date().toISOString().split("T")[0], email: newEmail, pitch: pitch.body.substring(0, 1000) }),
      }).catch(() => {});

      log(`🔄 Re-engagement: ${domain} — ${originalEmail} sin opens ${waitDays}d, enviado a ${newEmail} (attempt ${attempts + 1}/${maxAttempts})`);

      // Anti-rate limit Monday + Gmail
      await sleep(2000);
    }
  } catch (e) {
    log(`⚠️ runReengagementCycle error: ${e.message}`);
  }
}

// ════════════════════════════════════════════════════════════════
// MANUAL re-engagement queue — pickea de toolbar_reengagement_queue
// Lo que el MB encoló manualmente desde la toolbar (campo Email Futuro).
// Para cada row pendiente cuyo scheduled_for ya pasó:
//   1. Check si original_email tuvo open (toolbar_email_opens.tracking_action_id)
//   2. Si abrió → mark 'skipped_opened' (no molestar)
//   3. Si NO abrió → enviar original_body/subject al future_email
//      + Update Monday: email = future_email, FU1 = today+5, FU2 = today+10
//   4. Mark 'sent'
// ════════════════════════════════════════════════════════════════
async function processManualReengagementQueue(token) {
  try {
    const cfg = await getConfig(token);
    const enabled = String(cfg.agent_reengagement_enabled || "").toLowerCase() === "true";
    if (!enabled) return; // mismo kill switch que el agent

    const monday_api_key = cfg.monday_api_key;
    if (!monday_api_key) { log("⚠️ manualReengage: sin MONDAY_API_KEY"); return; }
    const boardId = cfg.monday_active_board || cfg.monday_board_id || 1420268379;

    // 1. Pickear pending vencidos (limit 20 por iteración)
    const nowIso = new Date().toISOString();
    const url = `${SUPABASE_URL}/rest/v1/toolbar_reengagement_queue?status=eq.pending&scheduled_for=lte.${encodeURIComponent(nowIso)}&select=*&order=scheduled_for.asc&limit=20`;
    const res = await fetch(url, {
      headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` },
    });
    if (!res.ok) { log(`⚠️ manualReengage HTTP ${res.status}`); return; }
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) return;

    log(`📬 manualReengage: ${rows.length} email(s) futuros a despachar`);

    for (const row of rows) {
      const { id, domain, monday_item_id, mb_email, original_email, future_email,
              original_subject, original_body, tracking_action_id } = row;
      let newStatus = "sent";
      let reason = null;

      // 2. ¿El email original fue abierto?
      let wasOpened = false;
      if (tracking_action_id) {
        try {
          const oRes = await fetch(
            `${SUPABASE_URL}/rest/v1/toolbar_email_opens?agent_action_id=eq.${tracking_action_id}&select=id&limit=1`,
            { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
          );
          if (oRes.ok) {
            const opens = await oRes.json();
            wasOpened = Array.isArray(opens) && opens.length > 0;
          }
        } catch {}
      }

      if (wasOpened) {
        newStatus = "skipped_opened";
        reason = "original email already opened";
        log(`⏭️  ${domain}: abierto, skip future`);
      } else if (!future_email || !future_email.includes("@")) {
        newStatus = "failed";
        reason = "invalid future_email";
      } else if (!original_subject || !original_body) {
        newStatus = "failed";
        reason = "missing original subject/body snapshot";
      } else {
        // 3. Enviar mismo subject + body al future_email
        try {
          const sent = await sendGmailServer(token, mb_email, {
            to:      future_email,
            subject: original_subject,
            body:    original_body,
            agentActionId: null, // worker no inyecta pixel acá; el seguimiento del futuro es out of scope v1
          });
          if (!sent?.ok) {
            newStatus = "failed";
            reason = `gmail send failed: ${sent?.error || "unknown"}`;
          } else {
            // 4. Update Monday: email + FU1 (today+5) + FU2 (today+10)
            const upd = await updateMondayReengagementDispatch(monday_api_key, monday_item_id, boardId, future_email);
            if (!upd) {
              // El envío salió pero Monday falló — no es fatal, log y seguir.
              log(`⚠️ ${domain}: gmail OK pero Monday update FALLÓ — revisar IDs FU1/FU2`);
              reason = "sent_ok_monday_update_failed";
            } else {
              log(`✅ ${domain}: future email enviado a ${future_email} + Monday actualizado (FU1+5, FU2+10)`);
            }
          }
        } catch (e) {
          newStatus = "failed";
          reason = `exception: ${e.message}`;
        }
      }

      // 5. Update row status
      await fetch(`${SUPABASE_URL}/rest/v1/toolbar_reengagement_queue?id=eq.${id}`, {
        method: "PATCH",
        headers: {
          "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
          "Content-Type": "application/json", "Prefer": "return=minimal",
        },
        body: JSON.stringify({ status: newStatus, reason, updated_at: new Date().toISOString() }),
      }).catch(() => {});

      // Anti rate-limit Monday + Gmail
      await sleep(1500);
    }
  } catch (e) {
    log(`⚠️ processManualReengagementQueue error: ${e.message}`);
  }
}
// ════════════════════════════════════════════════════════════════

async function logAgentAction(token, userEmail, payload) {
  // Audit P1 fix: 3 retries con backoff. Si toolbar_agent_actions falla,
  // el cap diario (getAgentDailyCount) no cuenta este send → over-send.
  // Antes era fire-and-forget swallowing silencioso.
  const MAX_RETRIES = 3;
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions`, {
        method: "POST",
        headers: {
          "apikey": SUPABASE_ANON_KEY,
          "Authorization": `Bearer ${BACKEND_BEARER || token}`,
          "Content-Type": "application/json",
          "Prefer": "return=minimal",
        },
        body: JSON.stringify({ user_email: userEmail, ...payload }),
      });
      if (res.ok) return; // éxito
      // 4xx no se reintenta (request mal armado), 5xx sí
      if (res.status >= 400 && res.status < 500) {
        log(`⚠️ logAgentAction ${res.status} (no retry): ${await res.text().catch(() => "")}`);
        return;
      }
      throw new Error(`HTTP ${res.status}`);
    } catch (e) {
      if (attempt === MAX_RETRIES - 1) {
        // Último intento — loguear el fail crítico
        log(`🔴 logAgentAction failed después de ${MAX_RETRIES} intentos: ${e.message} — payload: ${JSON.stringify(payload).slice(0, 200)}`);
        return;
      }
      // Exponential backoff: 500ms, 1s, 2s
      await new Promise(r => setTimeout(r, 500 * Math.pow(2, attempt)));
    }
  }
}

// ── BOUNCE DETECTION ────────────────────────────────────────
// Cache local + 1 query a Supabase cuando se necesita refrescar (cada 5min).
const _bouncedCache = { set: new Set(), ts: 0 };
const BOUNCED_CACHE_TTL = 5 * 60 * 1000;

async function loadBouncedEmails(token) {
  if (Date.now() - _bouncedCache.ts < BOUNCED_CACHE_TTL) return _bouncedCache.set;
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_bounced_emails?select=email&limit=10000`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (res.ok) {
      const rows = await res.json();
      _bouncedCache.set = new Set((rows || []).map(r => (r.email || "").toLowerCase()));
      _bouncedCache.ts = Date.now();
    }
  } catch {}
  return _bouncedCache.set;
}

function isBouncedSync(email) {
  return _bouncedCache.set.has((email || "").toLowerCase());
}

async function markEmailBounced(token, { email, reason, originalActionId, originalDomain }) {
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_bounced_emails`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({
        email: (email || "").toLowerCase(),
        reason: (reason || "").substring(0, 300),
        original_action_id: originalActionId || null,
        original_domain: originalDomain || null,
      }),
    });
    _bouncedCache.set.add((email || "").toLowerCase());
    log(`  🚫 BOUNCED: ${email} (${reason || "unknown"}) — agregado al blocklist`);
  } catch (e) {
    log(`⚠️ markEmailBounced failed: ${e.message}`);
  }
}

// Worker job: escanea INBOX por mailer-daemon en últimos 24h, parsea destinatario
// que rebotó, persiste en toolbar_bounced_emails. Corre 1 vez por loop iter.
async function scanBouncesForUser(token, userEmail) {
  try {
    const accessToken = await getGmailAccessToken(userEmail);
    // Query Gmail: from:mailer-daemon OR from:postmaster, últimas 24h
    const q = encodeURIComponent('from:(mailer-daemon@ OR postmaster@ OR noreply@) subject:(undelivered OR "delivery status" OR "returned mail" OR "failure notice") newer_than:1d');
    const listRes = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${q}&maxResults=20`,
      { headers: { "Authorization": `Bearer ${accessToken}` } }
    );
    if (!listRes.ok) {
      // 403 = scope readonly no autorizado en Workspace. Skip silencioso.
      if (listRes.status !== 403) log(`⚠️ scanBounces list ${listRes.status}`);
      return 0;
    }
    const list = await listRes.json();
    const ids = (list.messages || []).map(m => m.id);
    if (!ids.length) return 0;

    let detected = 0;
    for (const id of ids) {
      try {
        const msgRes = await fetch(
          `https://gmail.googleapis.com/gmail/v1/users/me/messages/${id}?format=full`,
          { headers: { "Authorization": `Bearer ${accessToken}` } }
        );
        if (!msgRes.ok) continue;
        const msg = await msgRes.json();
        // Buscar el header X-Failed-Recipients o parsear body
        const headers = msg.payload?.headers || [];
        const failedHeader = headers.find(h => h.name?.toLowerCase() === "x-failed-recipients");
        let failedEmails = [];
        if (failedHeader?.value) {
          failedEmails = failedHeader.value.split(",").map(e => e.trim().toLowerCase()).filter(Boolean);
        } else {
          // Fallback: extraer emails del body
          const body = extractMessageText(msg.payload || {});
          const emailMatches = body.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g) || [];
          // Filtrar el email del propio user y de daemons
          failedEmails = [...new Set(emailMatches
            .map(e => e.toLowerCase())
            .filter(e => !e.includes("mailer-daemon") && !e.includes("postmaster") && e !== userEmail.toLowerCase())
          )].slice(0, 3); // top 3
        }
        // Hard vs soft bounce detection del body — el hard amerita retry inmediato
        const bodyText = extractMessageText(msg.payload || {}).toLowerCase();
        const isHardBounce =
          /address not found|no se ha encontrado la dirección|endereço não encontrado|indirizzo non trovato|n'a pas été trouvée|nicht gefunden/i.test(bodyText) ||
          /550[\s-]?5\.1\.1|550[\s-]?user unknown|user does not exist|no such user|mailbox unavailable|recipient does not exist/i.test(bodyText) ||
          /permanent failure|permanent error/i.test(bodyText);
        const isSoftBounce =
          /4\d\d[\s-]?\d\.\d\.\d|mailbox full|over quota|temporarily|temporary failure|try again later|rate limit/i.test(bodyText) &&
          !isHardBounce;
        const bounceType = isHardBounce ? "hard" : (isSoftBounce ? "soft" : "unknown");

        for (const failed of failedEmails) {
          if (!isBouncedSync(failed)) {
            await markEmailBounced(token, { email: failed, reason: `smtp_bounce_${bounceType}`, originalDomain: failed.split("@")[1] });
            detected++;
            // Trigger bounce retry (solo hard bounces — los soft Gmail los reintenta solo)
            if (bounceType === "hard") {
              queueBounceRetry(token, userEmail, failed, bounceType).catch(e => log(`⚠️ queueBounceRetry: ${e.message}`));
            }
          }
        }
      } catch {}
    }
    if (detected) {
      log(`🚫 scanBounces ${userEmail}: detectó ${detected} email(s) rebotados`);
      // Audit P1 fix: invalidar cache de bounced para que el próximo
      // rankEmail use la lista actualizada. Antes la cache TTL 5min podía
      // dejar pasar un email que JUST bounced en este mismo ciclo.
      _bouncedCache.ts = 0;
    }
    return detected;
  } catch (e) {
    log(`⚠️ scanBounces error: ${e.message}`);
    return 0;
  }
}

// ── Auto-Reply detector — escanea inbox por respuestas automáticas
// (out-of-office, vacation, ticket systems, "responderemos en 48h", etc).
// Si detecta, trata al recipient como "no leerá" y dispara retry a próximo email.
async function scanAutoRepliesForUser(token, userEmail) {
  try {
    const accessToken = await getGmailAccessToken(userEmail);
    // Query Gmail: subject patrones de auto-reply en 7 idiomas + 1d
    // Lista corta de keywords muy distintivos para minimizar falsos positivos.
    const subjectQuery = [
      '"auto-reply"', '"automatic reply"', '"out of office"', '"vacation"',
      '"respuesta automática"', '"ausencia"', '"vacaciones"',
      '"resposta automática"', '"férias"', '"ausência"',
      '"risposta automatica"', '"assenza"',
      '"réponse automatique"', '"absence"',
      '"automatische antwort"', '"abwesend"', '"urlaub"',
      '"رد تلقائي"',
      '"accusons bonne réception"', '"acuse de recibo"', '"acusamos recebimento"',
    ].join(" OR ");
    const q = encodeURIComponent(`subject:(${subjectQuery}) newer_than:1d`);
    const listRes = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${q}&maxResults=15`,
      { headers: { "Authorization": `Bearer ${accessToken}` } }
    );
    if (!listRes.ok) {
      if (listRes.status !== 403) log(`⚠️ scanAutoReplies list ${listRes.status}`);
      return 0;
    }
    const list = await listRes.json();
    const ids = (list.messages || []).map(m => m.id);
    if (!ids.length) return 0;

    let detected = 0;
    for (const id of ids) {
      try {
        const msgRes = await fetch(
          `https://gmail.googleapis.com/gmail/v1/users/me/messages/${id}?format=metadata&metadataHeaders=From&metadataHeaders=Auto-Submitted&metadataHeaders=X-Auto-Response-Suppress`,
          { headers: { "Authorization": `Bearer ${accessToken}` } }
        );
        if (!msgRes.ok) continue;
        const msg = await msgRes.json();
        const headers = msg.payload?.headers || [];
        const fromH = headers.find(h => h.name?.toLowerCase() === "from")?.value || "";
        // Extraer email del FROM (esa fue la dirección que respondió auto)
        const fromMatch = fromH.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
        if (!fromMatch) continue;
        const respondedFrom = fromMatch[0].toLowerCase();
        if (respondedFrom === userEmail.toLowerCase()) continue;
        // Verificar header Auto-Submitted si está presente (más confiable que subject)
        const autoSub = headers.find(h => h.name?.toLowerCase() === "auto-submitted")?.value || "";
        const isAutoSubmitted = /auto-replied|auto-generated/i.test(autoSub);
        // Disparar retry — tratamos el email como "no-reader" igual que un soft bounce
        if (!isBouncedSync(respondedFrom)) {
          await markEmailBounced(token, { email: respondedFrom, reason: "auto_reply_detected", originalDomain: respondedFrom.split("@")[1] });
          queueBounceRetry(token, userEmail, respondedFrom, "auto_reply").catch(e => log(`⚠️ autoReply retry: ${e.message}`));
          detected++;
          log(`🔁 auto-reply detectado: ${respondedFrom}${isAutoSubmitted ? " (header)" : " (subject)"} → retry queued`);
        }
      } catch {}
    }
    if (detected) {
      _bouncedCache.ts = 0;
      log(`🔁 scanAutoReplies ${userEmail}: ${detected} respuestas automáticas → retry`);
    }
    return detected;
  } catch (e) {
    log(`⚠️ scanAutoReplies error: ${e.message}`);
    return 0;
  }
}

// ════════════════════════════════════════════════════════════════
// BOUNCE RETRY — re-envío automático a próximo email cuando bounce
// ────────────────────────────────────────────────────────────────
// Llamada desde scanBouncesForUser cuando detecta hard bounce.
// Pasos:
//   1. Cooldown: no más de 1 retry/domain en 24h
//   2. Max attempts: 2 (A → B → freeze)
//   3. Buscar lead en review_queue por domain del bounce
//   4. Pickear next-best email (source-strict, excluir bounced)
//   5. Si hay candidato: enviar + insert row bounce_retries + update Monday
//   6. Si no: freeze 60d
// ════════════════════════════════════════════════════════════════

async function queueBounceRetry(token, mbEmail, bouncedEmail, bounceType) {
  try {
    const domain = (bouncedEmail.split("@")[1] || "").toLowerCase();
    if (!domain) return;
    log(`🔄 bounceRetry: procesando bounce ${bouncedEmail} (${bounceType}) para ${domain}`);

    // 1. Guard: cooldown 24h por domain
    const cutoff24h = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const recentRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_bounce_retries?domain=eq.${encodeURIComponent(domain)}&created_at=gte.${cutoff24h}&select=id,attempt_number&order=created_at.desc&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (recentRes.ok) {
      const recent = await recentRes.json();
      if (Array.isArray(recent) && recent.length > 0) {
        log(`  ⏭️ ${domain}: ya hay bounce retry en últimas 24h (attempt ${recent[0].attempt_number}) — skip`);
        return;
      }
    }

    // 2. Max attempts global por domain (lifetime): 2 attempts
    const allAttemptsRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_bounce_retries?domain=eq.${encodeURIComponent(domain)}&select=id&limit=10`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-9" } }
    );
    const rangeHdr = allAttemptsRes.headers.get("content-range") || "";
    const totalAttempts = parseInt(rangeHdr.match(/\/(\d+)$/)?.[1] || "0", 10);
    if (totalAttempts >= 2) {
      log(`  🧊 ${domain}: ya ${totalAttempts} bounce retries — FREEZE 60d + clear Monday email`);
      await fetch(`${SUPABASE_URL}/rest/v1/toolbar_frozen_leads`, {
        method: "POST",
        headers: {
          "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
          "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        body: JSON.stringify({
          domain, frozen_until: new Date(Date.now() + 60 * 86400_000).toISOString(),
          attempt_count: totalAttempts, last_error: "max_bounce_retries",
          source: "bounce_retry", uploaded_by: mbEmail, updated_at: new Date().toISOString(),
        }),
      }).catch(() => {});
      // Clear Monday email column — el dominio queda en el board pero sin email
      // para que el equipo lo vea como "sin contacto activo" y decida qué hacer.
      try {
        const cfgClr = await getConfig(token);
        const mondayKey = (cfgClr[`monday_api_key_${mbEmail.toLowerCase()}`] || cfgClr.monday_api_key || "").trim();
        if (mondayKey) {
          const item = await findMondayItem(domain, mondayKey).catch(() => null);
          if (item?.id) {
            await updateMondayItem(item.id, { [MONDAY_COL_EMAIL]: { email: "", text: "" } }, mondayKey).catch(() => {});
            log(`  🧹 ${domain}: Monday email column limpiada (item ${item.id})`);
          }
        }
      } catch (e) { log(`  ⚠️ ${domain}: no se pudo limpiar Monday email: ${e.message}`); }
      return;
    }

    // 3. Encontrar lead en review_queue
    const leadRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?domain=eq.${encodeURIComponent(domain)}&select=*&order=created_at.desc&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!leadRes.ok) { log(`  ⚠️ ${domain}: query lead failed HTTP ${leadRes.status}`); return; }
    const leadRows = await leadRes.json();
    let lead = (Array.isArray(leadRows) && leadRows[0]) || null;

    // 3b. Fallback Monday-only: el lead puede ser pre-toolbar (cargado a Monday
    // manualmente, nunca pasó por review_queue). En ese caso, lookup directo
    // en Monday por dominio y armamos un "lead sintético" para el resto del flujo.
    // Así también esos bounces terminan limpiando la URL en Monday.
    if (!lead) {
      const cfgEarly = await getConfig(token).catch(() => ({}));
      const mondayKeyEarly = (cfgEarly[`monday_api_key_${mbEmail.toLowerCase()}`] || cfgEarly.monday_api_key || "").trim();
      if (mondayKeyEarly) {
        const mondayMatch = await findMondayItem(domain, mondayKeyEarly).catch(() => null);
        if (mondayMatch?.id) {
          log(`  🔎 ${domain}: lead no estaba en review_queue, pero existe en Monday (item ${mondayMatch.id}) — uso fallback Monday-only`);
          lead = {
            id: null,
            monday_item_id: mondayMatch.id,
            domain,
            emails: [],           // sin candidatos: el flujo cae directo a rescate (scrape + Apollo)
            email_sources: {},
            category: "",
            traffic: 0,
            pitch: null,
            pitch_subject: null,
            pitch_subjects: null,
          };
        }
      }
    }
    if (!lead) {
      log(`  ⏭️ ${domain}: lead no encontrado en review_queue ni en Monday — skip retry`);
      return;
    }

    // 4. Pickear next-best email — source-strict, excluir bounced
    // PRIORIDAD #1: future_email manual del MB (toolbar_reengagement_queue).
    // Si el MB tomó la molestia de cargar un email B en la toolbar, esa es
    // la mejor alternativa — más curada que cualquier scrape/Apollo automático.
    let manualFutureEmail = null;
    try {
      const fqRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_reengagement_queue?domain=eq.${encodeURIComponent(domain)}&mb_email=eq.${encodeURIComponent(mbEmail.toLowerCase())}&status=eq.pending&select=future_email&order=created_at.desc&limit=1`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      if (fqRes.ok) {
        const fqRows = await fqRes.json();
        const fe = (fqRows?.[0]?.future_email || "").trim().toLowerCase();
        if (fe && fe.includes("@") && fe !== bouncedEmail.toLowerCase() && !isBouncedSync(fe)) {
          manualFutureEmail = fe;
          log(`  ⭐ ${domain}: usando future_email manual del MB → ${manualFutureEmail}`);
        }
      }
    } catch (e) { log(`  ⚠️ future_email lookup: ${e.message}`); }

    let candidates = (lead.emails || []).filter(e => {
      if (!e || e.toLowerCase() === bouncedEmail.toLowerCase()) return false;
      if (isBouncedSync(e.toLowerCase())) return false; // ya bounced antes
      return true;
    });
    // Si hay future_email manual, lo poneamos al frente del pool
    if (manualFutureEmail) {
      candidates = [manualFutureEmail, ...candidates.filter(e => e.toLowerCase() !== manualFutureEmail)];
    }
    // RESCATE: si no hay alternativas en el lead, intentar re-enrich on-the-fly.
    // Scrape primero (gratis, con CF decoder + JSON-LD) → si vacío, Apollo lookup.
    // Si encuentra algo válido lo agregamos al lead y al pool de candidates.
    if (candidates.length === 0) {
      log(`  🔍 ${domain}: 0 alternativas — intentando rescate (scrape + Apollo)...`);
      const newEmails = new Set();
      try {
        const scraped = await scrapeEmailsForDomain(domain);
        scraped.forEach(e => { if (e && e.toLowerCase() !== bouncedEmail.toLowerCase()) newEmails.add(e.toLowerCase()); });
      } catch {}
      const cfg2 = await getConfig(token).catch(() => ({}));
      const apolloKey = cfg2.apollo_api_key;
      if (apolloKey) {
        try {
          const apolloRes = await findBestApolloEmail(domain, apolloKey, token, { traffic: lead.traffic || 0, allowUnlock: true });
          if (apolloRes?.email && apolloRes.email.toLowerCase() !== bouncedEmail.toLowerCase()) {
            newEmails.add(apolloRes.email.toLowerCase());
          }
        } catch {}
      }
      // Filtrar lo que ya bounced o garbage
      const rescued = [...newEmails].filter(e => !isBouncedSync(e) && rankEmail(e, domain, lead.category || "") >= 0);
      if (rescued.length > 0) {
        log(`  💊 ${domain}: rescate encontró ${rescued.length} email(s) nuevos: ${rescued.join(", ")}`);
        // Persist al lead para que próximas vueltas y otros MBs los vean
        const mergedEmails = [...(lead.emails || []), ...rescued].filter((e, i, arr) => e && arr.indexOf(e) === i);
        const mergedSources = { ...(lead.email_sources || {}) };
        rescued.forEach(e => { if (!mergedSources[e]) mergedSources[e] = "rescue"; });
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
          method: "PATCH",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ emails: mergedEmails, email_sources: mergedSources }),
        }).catch(() => {});
        candidates = rescued;
      } else {
        log(`  ⏭️ ${domain}: rescate sin resultados — marcando failed_all_bounced + freeze 30d`);
        // Marcar bounce retry como skipped
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_bounce_retries`, {
          method: "POST",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({
            domain, mb_email: mbEmail, original_email: bouncedEmail, retry_email: "",
            bounce_type: bounceType, attempt_number: totalAttempts + 1,
            status: "skipped_no_alt", reason: "no_alternative_emails_after_rescue",
          }),
        }).catch(() => {});
        // Freeze 30d (no 60d) para darle chance que el sitio actualice contactos
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_frozen_leads`, {
          method: "POST",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" },
          body: JSON.stringify({
            domain, frozen_until: new Date(Date.now() + 30 * 86400_000).toISOString(),
            attempt_count: totalAttempts + 1, last_error: "all_emails_bounced_no_rescue",
            source: "bounce_retry", uploaded_by: mbEmail, updated_at: new Date().toISOString(),
          }),
        }).catch(() => {});
        return;
      }
    }

    // Dynamic ranking: lee toolbar_source_performance del MB (fallback _global,
    // fallback hardcoded). Si hay ε-greedy explore o sample chico → ranking default.
    // "manual" siempre mantiene rank top — es la decisión explícita del MB.
    const dynRank = await getDynamicSourceRank(token, mbEmail);
    const SOURCE_RANK = { ...dynRank, manual: Math.max(dynRank.manual || 0, 5) };
    const _sourcesMap = lead.email_sources || {};
    const ranked = candidates
      .map(e => ({
        email: e,
        source: (manualFutureEmail && e.toLowerCase() === manualFutureEmail) ? "manual" : (_sourcesMap[e.toLowerCase()] || ""),
        score: rankEmail(e, domain, lead.category || ""),
      }))
      .filter(x => x.score >= 0)
      .sort((a, b) => {
        const sa = SOURCE_RANK[a.source] || 0;
        const sb = SOURCE_RANK[b.source] || 0;
        if (sa !== sb) return sb - sa;
        return b.score - a.score;
      });
    if (ranked.length === 0) {
      log(`  ⏭️ ${domain}: candidatos existen pero todos con score negativo — skip`);
      return;
    }
    const retryEmail  = ranked[0].email;
    const retrySource = ranked[0].source || "scrape";
    log(`  🎯 ${domain}: retry → ${retryEmail} (source=${retrySource}, score=${ranked[0].score})`);

    // 5. Cargar config + send
    const cfg = await getConfig(token);
    const mondayApiKey = (cfg[`monday_api_key_${mbEmail.toLowerCase()}`] || cfg.monday_api_key || "").trim();

    // Reusar el pitch del lead (snapshot) o regenerar — preferimos reusar
    // el pitch original que se generó cuando entró a review_queue
    const subject = lead.pitch_subject || (lead.pitch_subjects?.[0]) || `Sobre ${domain}`;
    const body    = lead.pitch || "Hola, quería ver si te puedo sumar algo desde ADEQ. Avisame si te interesa.";

    let retryActionId = null;
    let sendOk = false;
    let sendErr = "";
    try {
      const sent = await sendGmailServer(token, mbEmail, {
        to: retryEmail, subject, body, agentActionId: null,
      });
      sendOk = sent?.ok === true;
      if (!sendOk) sendErr = sent?.error || "unknown";
    } catch (e) {
      sendErr = e.message;
    }

    if (sendOk) {
      // Log agent action
      try {
        const resAct = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions`, {
          method: "POST",
          headers: {
            "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
            "Content-Type": "application/json", "Prefer": "return=representation",
          },
          body: JSON.stringify({
            user_email: mbEmail, domain, action: "bounce_retry_sent",
            email_to: retryEmail, pitch_subject: subject,
            details: { original_email: bouncedEmail, bounce_type: bounceType, retry_source: retrySource, source: retrySource },
          }),
        });
        if (resAct.ok) {
          const arr = await resAct.json().catch(() => []);
          retryActionId = Array.isArray(arr) ? arr[0]?.id : arr?.id;
        }
      } catch {}

      // 6. Update Monday — email + RESET FU1 (today+5) + FU2 (today+10).
      // Antes solo cambiábamos email, los FU quedaban con fecha vieja y Monday
      // no disparaba follow-ups o los disparaba tarde. Ahora reset completo.
      if (lead.monday_item_id && mondayApiKey) {
        await updateMondayReengagementDispatch(mondayApiKey, lead.monday_item_id, cfg.monday_active_board || cfg.monday_board_id || 1420268379, retryEmail)
          .catch(e => log(`  ⚠️ ${domain}: Monday update FAIL: ${e.message}`));
      }
      // Sendtrack
      await fetch(`${SUPABASE_URL}/rest/v1/toolbar_sendtrack`, {
        method: "POST",
        headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" },
        body: JSON.stringify({ domain, send_date: new Date().toISOString().split("T")[0], email: retryEmail, pitch: body.substring(0, 1000) }),
      }).catch(() => {});

      log(`  ✅ ${domain}: bounce retry enviado a ${retryEmail} + Monday actualizado`);
    } else {
      log(`  ❌ ${domain}: bounce retry FAIL — ${sendErr}`);
    }

    // 7. Insert row bounce_retries (audit trail)
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_bounce_retries`, {
      method: "POST",
      headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
      body: JSON.stringify({
        domain, monday_item_id: lead.monday_item_id,
        mb_email: mbEmail, original_email: bouncedEmail, retry_email: retryEmail,
        retry_source: retrySource, bounce_type: bounceType,
        attempt_number: totalAttempts + 1,
        status: sendOk ? "sent" : "failed",
        retry_action_id: retryActionId,
        reason: sendOk ? null : sendErr.substring(0, 200),
      }),
    }).catch(() => {});

  } catch (e) {
    log(`⚠️ queueBounceRetry ${bouncedEmail}: ${e.message}`);
  }
}

function extractMessageText(payload) {
  if (payload.body?.data) {
    try { return Buffer.from(payload.body.data, "base64").toString("utf-8"); } catch { return ""; }
  }
  if (Array.isArray(payload.parts)) {
    return payload.parts.map(p => extractMessageText(p)).join("\n");
  }
  return "";
}

// Cuenta de envíos del agent en últimos 7 días (weekly target)
async function getAgentWeeklyCount(token, userEmail) {
  try {
    const cutoff = new Date(Date.now() - 7 * 86400_000).toISOString();
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(userEmail)}&action=eq.sent&created_at=gte.${cutoff}&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("content-range") || "";
    const m = range.match(/\/(\d+)$/);
    return m ? parseInt(m[1]) : 0;
  } catch { return 0; }
}

// Cuenta de envíos del agent en el día calendario España (no 24h móvil).
// Cambio: antes era 24h rolling — eso permitía "13/10" si los envíos se
// distribuían en torno a la medianoche. Ahora es estricto día calendario
// España: 00:00 → 23:59 reset.
async function getAgentDailyCount(token, userEmail) {
  try {
    // Calcular medianoche España de HOY.
    const fmt = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit"
    });
    const todaySpain = fmt.format(new Date()); // "YYYY-MM-DD"
    // Madrid timezone offset (CEST = +02 verano, CET = +01 invierno)
    // Construimos el ISO de medianoche Madrid → UTC. Aproximación: usar
    // toLocaleString para verificar offset actual.
    const probe = new Date(`${todaySpain}T00:00:00`);
    const madridStr = probe.toLocaleString("en-US", { timeZone: "Europe/Madrid" });
    const utcStr = probe.toLocaleString("en-US", { timeZone: "UTC" });
    const offsetMs = new Date(madridStr).getTime() - new Date(utcStr).getTime();
    const cutoffUtc = new Date(probe.getTime() - offsetMs).toISOString();
    // Cuenta tanto 'sent' (confirmado) como 'reserved' (pre-send) — la reserva
    // protege contra crashes mid-send: si el worker reinicia entre reserve y
    // confirmación, el slot queda apartado y el next iter no over-sends.
    // Timeout 5s: si Supabase está lento, no colgamos el worker entero por
    // este conteo. El fail-open de abajo se encarga del retry next cycle.
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(userEmail)}&action=in.(sent,reserved)&created_at=gte.${cutoffUtc}&select=id`,
      {
        headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" },
        signal: AbortSignal.timeout(5000),
      }
    );
    if (!res.ok) {
      // Fail-open: si no podemos contar, asumimos 0 y seguimos. El riesgo es
      // sobre-enviar si la query falla repetidamente, pero el sendtrack 30d
      // guard + reserved slots + per_cycle_limit limitan el daño (peor caso:
      // 5-15 envíos extra durante una caída sostenida de Supabase). El costo
      // del fail-closed previo era perder el día entero por un glitch.
      log(`⚠️ getAgentDailyCount HTTP ${res.status} — fail-open (assume 0, retry next cycle)`);
      return 0;
    }
    const range = res.headers.get("content-range") || "";
    const m = range.match(/\/(\d+)$/);
    return m ? parseInt(m[1]) : 0;
  } catch (e) {
    log(`⚠️ getAgentDailyCount error: ${e.message} — fail-open (assume 0)`);
    return 0;
  }
}

// Kill switch: si en la última hora hay > N fails consecutivos, auto-pausa 1h.
async function checkAgentKillSwitch(token, userEmail, cfg) {
  try {
    const cutoff = new Date(Date.now() - 3600_000).toISOString();
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(userEmail)}&created_at=gte.${cutoff}&select=action,reason&order=created_at.desc&limit=20`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length < 5) return false;
    const fails = rows.filter(r => r.action === "failed").length;
    const total = rows.length;
    const failRate = fails / total;
    if (failRate > 0.5) {
      const pauseUntil = new Date(Date.now() + 3600_000).toISOString();
      await setConfigValue(token, "agent_paused_until", pauseUntil);
      log(`🚨 KILL SWITCH AGENT: fail rate ${Math.round(failRate*100)}% (${fails}/${total}) — pausado 1h hasta ${pauseUntil}`);
      await logAgentAction(token, userEmail, {
        domain: "(system)", action: "kill_switch", reason: `fail_rate_${Math.round(failRate*100)}_pct`,
        details: { fails, total, paused_until: pauseUntil },
      });
      return true;
    }
    return false;
  } catch (e) {
    log(`⚠️ checkAgentKillSwitch error: ${e.message} — fail-closed (skip user este ciclo)`);
    return true; // fail-closed: si no podemos chequear, NO seguir mandando
  }
}

// Cache del estilo ADEQ — vive por la lifetime del proceso (refresh cada 30min)
let _adeqStyleCache = null;
let _adeqStyleCacheAt = 0;
const ADEQ_STYLE_TTL = 30 * 60 * 1000;

// IMPORTANTE: este fallback se usa SOLO si la lectura de
// toolbar_user_prompts.__global__ falla. La fuente de verdad es Supabase
// (editable por el admin desde la toolbar). Mantener este string sincronizado
// con el seed de sql/2026-05-13_user_prompts_rls_and_seed.sql.
const ADEQ_STYLE_FALLBACK = `# IDENTIDAD

Sos Claude operando como redactor de cold emails de ADEQ Media.
Escribís en nombre de {{SIGNER_NAME}} ({{SIGNER_ROLE}}).

Sos un vendedor B2B AdTech con voz humana, mínima, conversacional.
Tu objetivo único: generar UNA respuesta del publisher. Punto.

NO sos un asistente neutral. NO sos un equipo de marketing. Sos una
persona apurada que escribe en 2 minutos entre dos llamadas. Si el
mail huele a "campaña de outreach armada" → no responde. Si parece
tipeado a mano → responde.

# CONTEXTO DEL DESTINATARIO

El publisher recibe 10-20 emails al día vendiéndole monetización.
Está cansado, los ignora, los archiva sin leer. Tu único activo es
que tu mail NO se vea como esos otros 19.

Este es el PRIMER toque. El publisher no sabe quién sos ni qué es
ADEQ. El objetivo NO es cerrar, NO es explicar el producto, NO es
proponer call. El objetivo es UNA respuesta. Puede ser:

- "sí, contame más" (ideal)
- "ahora no" (válido, lo retomamos)
- "soy yo, qué me ofrecen" (apertura)
- "no soy el contacto, hablá con X" (puerta nueva)

Cualquiera de esas cuatro es éxito.

# QUÉ HACE ADEQ MEDIA

ADEQ monetiza inventario publicitario de sitios web. Productos
pitcheables en cold:

1. Header bidding — uplift típico 15-30% en eCPM sobre el stack
   actual, sin tocar la integración del publisher.

2. Video in-stream y out-stream — player propio, CPMs y fill altos,
   no pisa la UX del sitio. Bueno para sports, news, entretenimiento.

3. Sticky footer / sticky header — campañas directas que pegan bien
   por CTR alto (queda fijo a la vista durante la sesión). Sin
   competir con tu inventario actual — ocupa una posición que en
   general no estás vendiendo.

4. Slider / corner video — CPM fijo USD 1. Mencionar solo cuando encaja.

5. Display sticky (otros), interstitials — complementarios, no
   protagonistas en cold.

# REGLAS DE VOZ (no negociables)

## Largo
40-80 palabras. Cortar despiadado. Si tenés que elegir entre quitar
una frase o dejar el mail más largo, quitala.

## Apertura — directa, sin "¿cómo estás?"
NO uses "Hola [Nombre], ¿cómo estás?". Demasiado formal para cold.

Usá uno de estos arranques reales:
- "Hola, soy de ADEQ. Vi {{domain}} y..."
- "Hola! Vi {{domain}} y queria preguntarte..."
- "Hola, te escribo de ADEQ Media."
- "Hola [Nombre], soy de ADEQ. Vi {{domain}}..."

Si no hay nombre, NO inventes uno — empezá con "Hola, soy de ADEQ".

## Estructura
2 párrafos cortos máximo. Línea en blanco entre ellos. Estilo
WhatsApp largo, no carta formal. Sin bullets, sin negritas, sin
formato HTML.

## Cierre — informal, no "Saludos."
- "Cualquier cosa avisame."
- "Decime y te mando los detalles."
- "Si te quedan minutos te muestro cómo."
- "Avisame si te interesa."

NO usar "Saludos." / "Quedo a la espera" / "Atentamente".

## Firma
NUNCA firmes con nombre. Gmail agrega la firma sola.
Último renglón = cierre informal. Nada después.

## Datos numéricos permitidos en cold
- Header bidding → "uplift 15-30% del eCPM"
- Slider → "CPM fijo 1 USD"
- Video → "CPMs altos, fill alto" SIN número específico
- Sticky → "CTR alto", "queda fijo durante la navegación", sin número
- Revshare 80-20 → NUNCA en cold (solo si preguntan)
- NET 60 → NUNCA en cold

## Frases PROHIBIDAS
"Espero que te encuentres bien", "Quedo a su disposición",
"Sin más por el momento", "Estimado/a", "Sr./Sra.",
"win-win", "sinergia", "apalancar", "ecosistema",
"OPORTUNIDAD ÚNICA", "le escribo para",
"todo piola", "dale campeón".

## Lo que NO va en COLD
- "sin exclusividad / sin permanencia"
- "solo seguimos si los resultados son buenos"
- Mención de clientes referencia (Footballia, Ciclo21, etc.)
- Pedido directo de call/Meet
- Rangos detallados de CPM para video / sticky
- Comparaciones con AdSense / Taboola / etc.

# CUATRO TÁCTICAS DE COLD

## A — Validación de gatekeeper (default cuando hay duda de contacto)
"Hola! Vi {{domain}} y queria preguntarte si sos vos quien maneja las
pautas publicitarias del sitio, o si me podes pasar el contacto del
que decide.

Soy de ADEQ Media, trabajamos con publishers monetizando inventario.
Quiero ver si te puedo sumar algo.

Cualquier cosa avisame."

## B — Header bidding (señal: tráfico alto, AdSense detectado)
"Hola, soy de ADEQ. Vi que {{domain}} tiene buen tráfico y queria
preguntarte si ya estás corriendo header bidding o si lo manejas
todo via Google directo.

Tenemos un setup que suele levantar 15-30% del eCPM sin tocar la
integración actual. Si te quedan minutos te muestro cómo."

## C — Video (señal: sports, news, entretenimiento)
"Hola, te escribo de ADEQ Media. Tenemos campañas de video activas
(in-stream y out-stream) que andan muy bien con sitios como {{domain}}.

CPMs altos, fill alto, sin pisarte la UX. Si te interesa te paso un
breakdown rápido.

Decime y te mando los detalles."

## D — Sticky (señal: tráfico alto, sin sticky propio, vertical generalista)
"Hola, soy de ADEQ. Vi {{domain}} y queria proponerte algo simple:
tenemos campañas directas para sticky footer (o sticky header) que
suelen rendir muy bien por el CTR alto, ya que queda fijo durante
toda la navegación.

Es una posición que normalmente no compite con tu inventario actual.
Si te interesa te paso los detalles.

Decime y te mando."

# IDIOMA Y REGIÓN

El tono español (mínimo, casual, "tipeado en 2 minutos") es el MOLDE
para TODOS los idiomas. Traducís la VOZ, no solo las palabras.

## Idiomas soportados (escribir en local)
- Español (LATAM + ES)
- Inglés (US/UK/global)
- Portugués (BR + PT)
- Italiano (IT)
- Árabe (MENA)

## ES — voz maestra
Voseo AR/UY: "queria", "decime", "avisame". Tuteo MX/CO/ES: "dime",
"avísame". WhatsApp largo, no carta formal.

## EN
Apertura: "Hi, I'm from ADEQ." / "Hi [Name], saw {{domain}}..."
Cierre: "Let me know." / "Happy to share more if useful."
Sin "I hope this email finds you well", sin "Kind regards".

## PT-BR
"Oi, sou da ADEQ." / "Olá! Vi {{domain}}..."
"Me avisa." / "Qualquer coisa avisa."

## PT-PT
"Olá, sou da ADEQ Media." — un toque más cuidado, pero sin email
corporativo PT clásico.

## IT
"Ciao, sono di ADEQ." / "Ciao [Nome], ho visto {{domain}}..."
Sin "Buongiorno Sig./Sig.ra". Frases cortas, sin "egregio".
Cierre: "Fammi sapere." / "Se ti interessa ti mando i dettagli."

## AR (MENA)
Apertura: "مرحباً، أنا من ADEQ". Mínimo de formalidad pero sin carta
clásica. Frases cortas, 2 párrafos máx.
Cierre: "أخبرني إذا أردت أن أرسل لك التفاصيل".

## Resto Europa Este (PL/BG/RO/CZ/etc.)
Responder en INGLÉS. No improvisar idioma local.

## FR / DE
NO prospectar activamente. Si pedido → alertar antes de generar.

# PERSONALIZACIÓN POR SEÑAL

Si recibís {geo}, {vertical}, {traffic}, {ad_networks}, ajustá la
táctica (NO mencionando datos crudos, solo eligiendo ángulo):

- Sports / news / entretenimiento → C (video) o D (sticky)
- AdSense detectado → B (header bidding)
- Tráfico alto + sin sticky propio → D (sticky)
- Vertical generalista → D (sticky) > C
- Tráfico < 100K → A (validación), tono cercano
- Tráfico > 5M → B o C, tono ligeramente más profesional
- Sin señal clara → A por default

# AUTO-CHECKLIST ANTES DE DEVOLVER

- ¿Es PRIMER toque? Si es follow-up/respuesta/reactivación → NO generes.
- FR/DE → NO generar, alertar primero
- PL/BG/RO/etc. → generar en INGLÉS
- ES / EN / PT / IT / AR → generar en local
- 40-80 palabras
- Apertura directa "Hola, soy de ADEQ" (sin "¿cómo estás?")
- 2 párrafos cortos, sin formato
- Termina con cierre informal
- Sin firma con nombre
- Mencioné {{domain}} explícito
- Sin frases prohibidas
- Sin "sin exclusividad", sin urgencia, sin clientes referencia, sin pedido directo de call

# OUTPUT FORMAT

Asunto: [3-6 palabras, minúscula]

[Cuerpo, 40-80 palabras, cierre informal]

Sin "Acá tenés", sin explicación.

NUNCA: "URGENT", "!!!", "RE:", emojis, "OPORTUNIDAD ÚNICA".`;

// ── RAG con Voyage embeddings (worker-side) ──
// Embed lead context con Voyage, busca top liked/disliked similares en
// toolbar_pitch_feedback (RPC match_pitch_feedback). Inyecta como few-shot
// en pitch generation. Cero costo si la tabla está vacía (skip).
function _buildPitchContextWorker({ domain, category, geo, language, traffic }) {
  const trafficStr = traffic
    ? (traffic >= 1_000_000 ? `${Math.round(traffic / 1_000_000)}M` : `${Math.round(traffic / 1_000)}K`)
    : "unknown";
  return [
    `Site: ${domain || "unknown"}`,
    `Category: ${category || "unknown"}`,
    `Geo: ${geo || "unknown"}`,
    `Language: ${language || "unknown"}`,
    `Traffic: ${trafficStr} visits/mo`,
  ].join(" | ");
}

const _voyageWorkerCache = new Map();
async function _voyageEmbedWorker(token, text) {
  if (!text) return null;
  if (_voyageWorkerCache.has(text)) return _voyageWorkerCache.get(text);
  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/api-proxy`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        provider: "voyage",
        path: "/v1/embeddings",
        method: "POST",
        body: { model: "voyage-3", input: [text], input_type: "query" },
      }),
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) return null;
    const data = await res.json();
    const vec = data?.data?.[0]?.embedding;
    if (Array.isArray(vec)) {
      if (_voyageWorkerCache.size >= 200) {
        const k = _voyageWorkerCache.keys().next().value;
        _voyageWorkerCache.delete(k);
      }
      _voyageWorkerCache.set(text, vec);
    }
    return Array.isArray(vec) ? vec : null;
  } catch { return null; }
}

async function ragRetrieveExamplesAgent(token, userEmail, ctx) {
  try {
    const ctxStr = _buildPitchContextWorker(ctx);
    const embedding = await _voyageEmbedWorker(token, ctxStr);
    if (!embedding) return { likes: [], dislikes: [] };
    // RPC match_pitch_feedback (mismo que usa el popup)
    const callRpc = async (action, count) => {
      try {
        const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/match_pitch_feedback`, {
          method: "POST",
          headers: {
            "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            query_embedding: embedding,
            match_user_email: userEmail,
            match_action: action,
            match_count: count,
          }),
          signal: AbortSignal.timeout(6000),
        });
        if (!res.ok) return [];
        const rows = await res.json();
        return Array.isArray(rows) ? rows : [];
      } catch { return []; }
    };
    const [likes, dislikes] = await Promise.all([callRpc("liked", 3), callRpc("disliked", 2)]);
    return {
      likes: likes.map(r => r.pitch_body).filter(Boolean),
      dislikes: dislikes.map(r => r.pitch_body).filter(Boolean),
    };
  } catch { return { likes: [], dislikes: [] }; }
}

// 2do pass Claude para elegir el mejor email entre ambiguos. Devuelve el email
// elegido o null. Cache 30d en toolbar_config para no re-pagar Claude por el
// mismo dominio. Solo se llama si rankEmail dio scores ambiguos.
const _claudePickCache = new Map();
const CLAUDE_PICK_CACHE_MAX = 500;
async function claudePickBestEmail(token, { domain, category, emails }) {
  if (!Array.isArray(emails) || emails.length < 2) return emails[0] || null;
  const cacheKey = `${domain}:${emails.sort().join(",")}`;
  if (_claudePickCache.has(cacheKey)) return _claudePickCache.get(cacheKey);
  if (_claudePickCache.size >= CLAUDE_PICK_CACHE_MAX) {
    const firstKey = _claudePickCache.keys().next().value;
    _claudePickCache.delete(firstKey);
  }

  const userMsg = `Site: ${domain}
Category: ${category || "unknown"}
Emails candidatos:
${emails.map((e, i) => `${i + 1}. ${e}`).join("\n")}

¿Cuál tiene MÁS probabilidad de ser un decision-maker B2B comercial (publisher/founder/marketing/business dev) que respondería a un cold email sobre monetización publicitaria?
Devolveme JSON: { "email": "<el email exacto>", "reason": "<5 palabras>" }`;

  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/api-proxy`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        provider: "anthropic",
        path: "/v1/messages",
        method: "POST",
        body: {
          model: "claude-haiku-4-5", // Haiku 4.5 = mucho más barato que Sonnet, suficiente para este pick
          max_tokens: 100,
          system: "Sos un experto en B2B AdTech sales. Elegís emails para outreach.",
          messages: [{ role: "user", content: userMsg }],
        },
      }),
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) return null;
    const data = await res.json();
    const text = data?.content?.[0]?.text || "";
    const m = text.match(/\{[\s\S]*\}/);
    if (!m) return null;
    const parsed = JSON.parse(m[0]);
    const picked = parsed?.email && emails.includes(parsed.email) ? parsed.email : null;
    if (picked) _claudePickCache.set(cacheKey, picked);
    return picked;
  } catch { return null; }
}

async function _getAdeqStyle(token) {
  const now = Date.now();
  if (_adeqStyleCache && (now - _adeqStyleCacheAt) < ADEQ_STYLE_TTL) return _adeqStyleCache;
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_user_prompts?user_email=eq.__global__&select=prompt`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (res.ok) {
      const rows = await res.json();
      const prompt = rows?.[0]?.prompt;
      if (prompt && prompt.length > 100) {
        _adeqStyleCache = prompt;
        _adeqStyleCacheAt = now;
        return prompt;
      }
    }
  } catch {}
  _adeqStyleCache = ADEQ_STYLE_FALLBACK;
  _adeqStyleCacheAt = now;
  return ADEQ_STYLE_FALLBACK;
}

// ── Claude pitch generation server-side (calls Anthropic via Edge proxy) ──
async function generatePitchAgent(token, ctx) {
  const { domain, traffic, geo, language, category, contactName, adNetworks, userEmail } = ctx;
  const langName = ({ es:"Spanish", en:"English", pt:"Portuguese", it:"Italian", ar:"Arabic" })[language] || "English";
  const trafficStr = traffic >= 1_000_000 ? `${Math.round(traffic/1_000_000)}M` : `${Math.round(traffic/1_000)}K`;

  // Estilo ADEQ desde DB (toolbar_user_prompts user_email='__global__'), fallback a baked
  const adeqStyle = await _getAdeqStyle(token);

  // RAG few-shot: levantar pitches similares exitosos (liked) y los rechazados (disliked)
  // del MB humano. Best-effort, si falla seguimos sin RAG.
  const ragOwner = userEmail || "mgargiulo@adeqmedia.com";
  const rag = await ragRetrieveExamplesAgent(token, ragOwner, { domain, category, geo, language, traffic });
  const ragLikes = rag.likes.length > 0
    ? `\n\n# EJEMPLOS DE EMAILS QUE FUNCIONARON (replicá el ESTILO, no el contenido):\n${rag.likes.map((p, i) => `Ejemplo ${i + 1}:\n"""${p.substring(0, 500)}"""`).join("\n\n")}`
    : "";
  const ragDislikes = rag.dislikes.length > 0
    ? `\n\n# ESTILO A EVITAR (rechazados por el MB — NO escribir nada que se les parezca):\n${rag.dislikes.map((p, i) => `Rechazado ${i + 1}:\n"""${p.substring(0, 300)}"""`).join("\n\n")}`
    : "";

  const systemMsg = `${adeqStyle}${ragLikes}${ragDislikes}

# OUTPUT REQUIREMENTS — MAIL INICIAL CORTO Y SIMPLE
Este es el PRIMER mail al publisher: NO sobre-analizar, NO largo, NO armado.
Debe parecer escrito en 90 segundos entre dos llamadas.

LANGUAGE: write the ENTIRE email in ${langName}. Do not mix languages.
RETURN JSON ONLY: { "body": string, "subjects": [3 subject lines, 4-8 words each in ${langName}] }
- body: 50-100 palabras MÁXIMO. 2-3 párrafos chiquitos (1-2 líneas c/u). Nada más.
- subjects: variantes A/B/C cortas — usar el dominio del sitio en al menos 1
- NO incluir firma, nombre propio, ni despedida formal al final del body
- Cerrar con UNA pregunta concreta sobre charlar/probar`;

  const userMsg = `Site: ${domain}
Monthly traffic: ${trafficStr} visits
Geo: ${geo || "unknown"}
Category: ${category || "unknown"}
Ad networks detected: ${(adNetworks || []).join(", ") || "none"}
Contact: ${contactName || "(unknown)"}

Write the prospecting email. Return JSON only.`;

  const res = await fetch(`${SUPABASE_URL}/functions/v1/api-proxy`, {
    method: "POST",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      provider: "anthropic",
      path: "/v1/messages",
      method: "POST",
      body: {
        model: "claude-sonnet-4-5",
        max_tokens: 1024,
        system: systemMsg,
        messages: [{ role: "user", content: userMsg }],
      },
    }),
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) throw new Error(`Claude HTTP ${res.status}`);
  const data = await res.json();
  const text = data?.content?.[0]?.text || "";
  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (!jsonMatch) throw new Error("Claude response no contiene JSON");
  const parsed = JSON.parse(jsonMatch[0]);
  if (!parsed.body || !Array.isArray(parsed.subjects) || parsed.subjects.length === 0) {
    throw new Error("Claude response shape inválido");
  }
  return parsed;
}

// ── Email scoring + verify ──────────────────────────────────
// Garantiza que solo mandamos a emails decentes. 3 niveles:
//   🟢 green  : SMTP OK + no garbage + no genérico → manda
//   🟡 yellow : válido formato + genérico (info@/contact@/etc) → manda igual
//   🔴 red    : bounce, garbage (whois@/abuse@/postmaster@), inválido → SKIP
// LOCAL part patterns (antes del @): roles que nunca son decision-makers B2B.
// Lista exhaustiva — ampliada 2026-05-12 por feedback de envíos reales.
// Construido como string concat para mantener legibilidad.
// REGLA: solo bloqueamos emails que NINGÚN humano lee (mailer-daemon, abuse, etc).
// Customer support (info@, contact@, support@) NO se bloquea — algunos publishers
// usan esos como único contacto. Esos van como "yellow" (mandar pero baja confianza)
// vía rankEmail score bajo, no descarte total.
const _GL_LOCAL_PARTS = [
  // Sysadmin / mail infra (sin lectura humana real)
  "abuse","admin","administrator","root","sudo","webmaster","hostmaster","postmaster","nobody","null",
  // Roles que no responden / no son decision-makers (cazados 2026-05-14)
  "feedback","feedbacks","reclamo","reclamos","reclamacao","reclamacoes","quejas","sugerencias","sugestoes",
  "circulation","subscriptions","subs","newsletter","alerts","alerta","alertas",
  "training","capacitacion","capacitacao","cursos",
  "plataforma","plataformas","platform","platforms","sistema","sistemas","tic","tic-admin","tic.adm","tecnologia","tecnologias","tech-admin",
  "foco","servicioalcliente","servicio-al-cliente","servicio-cliente","atencionalcliente","atencion-al-cliente","customerservice","customer-service","customercare","customer-care",
  "office","oficina","secretaria","secretariat","reception",
  "redaction","redazione","redaktion","redactie","editorial",
  "trustandsafety","trust-?and-?safety","trust-?safety","safety","safety-?team","trust-?ops",
  "whois","registrant","registry","registrar","domain-?ops","domain-?abuse","ndomains",
  // Manejo de dominios/DNS (caso real domains@latinregistrar.com.br se coló al agent 2026-05-13)
  "domain","domains","domain-?master","domain-?admin","domain-?contact","domain-?renewal",
  "dns","dns-?master","dns-?host-?master","dns-?admin","nic","nic-?host-?master",
  "ssl","ssl-?cert","ssl-?admin","tls-?cert",
  "noreply","no-reply","donotreply","do-not-reply","do_not_reply","autoreply","auto-?reply","mailer-?daemon","mta","mailserver","mail-?server","mta-?admin",
  "bounce","bounced","mailer",
  "cert","cert-?admin","csirt","soc","noc","sysadmin","sys-?admin","netops","cert-?manager",
  "spam","antispam","fraud","antifraud","phishing","abusedesk","abuse-?report",
  "legal","copyright","dmca","takedown","trademark",
  // Privacy / GDPR (proxies de WHOIS)
  "gdpr","gdpr-?mask","gdpr-?masking","gdpr-?desk","dpo","data-?protection","privacy","masked","masking","anonymous","anon","undisclosed",
  // Billing / finance (no compran ads)
  "billing","invoice","invoices","invoicing","accounting","finance","payable","payables","treasury",
  // Hosting / CDN (infraestructura)
  "hosting","cdn","cloudflare","cloudfront","akamai","fastly","incapsula","sucuri",
  "piracy","pirate","antipiracy","anti-?piracy",
  "dns","dns-?admin","ssl-?admin",
  // HR (no son decision-makers comerciales)
  "careers?","jobs?","recruit","recruiting","recruitment","hire","hiring","talent",
  // Marketing automation opt-out
  "unsubscribe","opt-?out","optout","removeme","remove-?me",
  // Dev/test fakes
  "test","testing","dev","developer","staging","sandbox","example","fake","throwaway",
  // Monitoring
  "monitoring","alerts?","incident","incidents",
];
const GARBAGE_LOCAL = new RegExp("^(?:" + _GL_LOCAL_PARTS.join("|") + ")@", "i");
// Cualquier ocurrencia dentro del local-part también descarta. Captura variantes
// nuevas tipo "trustandsafety", "gdpr-mask-2025", "protect-domain", etc.
const GARBAGE_LOCAL_CONTAINS = new RegExp([
  "abuse","domain[-._]?(?:ops|operations|abuse|admin|manager|owner)","hosting","cloudflare","cloudfront","akamai","fastly",
  "proxy","piracy","pirate","takedown","whois","gdpr","masked?","masking","anonymized?",
  // "protect" anclado a word boundary para no kill "protected-content@" o "protector@editorial.com"
  "(?:^|[._-])protect(?:ed|ion)?[-._]?(?:domain|service|service|email|mail|admin|service)?(?:$|[._-])",
  "trust[-._]?and[-._]?safety","trust[-._]?safety","safety[-._]?team",
  "unsubscribe","opt[-._]?out","removeme",
  "mailer[-._]?daemon","noreply","no[-._]?reply","donotreply","autoreply",
  "dpo","data[-._]?protection",
  // Capa adicional 2026-05-14 — palabras lexicales en local-part que indican infraestructura
  "(?:^|[._-])(?:dns|ssl|tls|cert|smtp|imap|pop3|ftp|sftp|vpn)(?:$|[._-])",
  "(?:^|[._-])(?:registrar|registry|registrant|registered)(?:$|[._-])",
  "(?:^|[._-])(?:privacy|anonym|shield|guard|hidden|undisclosed)(?:$|[._-])",
  "(?:^|[._-])(?:billing|invoice|finance|accounting|payable|treasury|cobranza|facturacion|cobros)(?:$|[._-])",
  "(?:^|[._-])(?:cdn|cloud|hosting|host|server|servers|datacenter|colo)(?:$|[._-])",
  "(?:^|[._-])(?:legal|compliance|dmca|copyright|takedown|trademark)(?:$|[._-])",
].join("|"), "i");

// ── CAPA 3 (defense-in-depth) — palabras-clave en el DOMINIO del email ─────
// Cualquier dominio que contenga estas palabras = infraestructura, NO publisher.
// Caso real 2026-05-14: emails enviados a domains@latinregistrar.com.br,
// ceo@viads.com, trustandsafety@support.aws.com, ayuda@nic.mx, etc.
const _GARBAGE_DOMAIN_KEYWORDS = [
  // Registrars / DNS
  "registrar","registry","registrant","nic\\.","whois","domainsby","domainservice",
  "dominio","dominios","dominiosecuador","jewellaprivacy","cscinfo","cscglobal",
  "n2v","markmonitor","godaddyguard","gandi\\.net","enom","netim","epag",
  "porkbun","namebright","key-systems","onlinenic","ovhcloud","ovh\\.net",
  // Privacy proxies
  "privacyprotect","privacyguard","domainprotect","protecteddomain","whoisprotect",
  "whoisguard","contactprivacy","withheldforprivacy","perfectprivacy",
  "regprivate","redactedforprivacy","registrationprivate",
  // GDPR masks
  "gdpr-mask","gdpr-masked","gdpr-protect","data-protected","redacted-private",
  // Hosting / Cloud
  "amazonaws","amazonses","cloudfront","googlecloud","azure-?microsoft","cloudflare",
  "fastly","akamai","digitalocean","linode","heroku","netlify","vercel","render",
  "cloudways","kinsta","wpengine","siteground","hostinger","bluehost","hostgator",
  // Transactional senders (no humans)
  "sendgrid","mailgun","postmark","mandrill","sparkpost","amazonses","mailtrap",
  // Trust & safety / abuse desks
  "trustandsafety","trust-and-safety","abuse-?desk",
  // Disposable
  "mailinator","guerrillamail","tempmail","throwaway","sharklasers","yopmail","10minutemail","disposable",
];
const GARBAGE_DOMAIN_KEYWORDS = new RegExp(
  // Match keyword en cualquier nivel del dominio (incluyendo TLDs cortos como nic.mx).
  // (?:^|[.@]) → inicio o tras . o @
  // [a-z0-9-]* → prefijo opcional (csc en cscinfo, latin en latinregistrar)
  // (KEYWORDS) → palabra-clave
  // [a-z0-9-]* → sufijo opcional
  // (?=\\.|$) → seguido de . o fin
  "(?:^|[.@])[a-z0-9-]*(?:" + _GARBAGE_DOMAIN_KEYWORDS.join("|") + ")[a-z0-9-]*(?=\\.|$)",
  "i"
);
// Capa 3 extra: AWS / Google Cloud / Azure subdominios — captura "support.aws.com",
// "support.amazonaws.com", "abuse.cloudflare.com", etc. donde la keyword está
// en un subdominio interno (no como dominio raíz).
const GARBAGE_DOMAIN_SUBDOMAIN = /\.(aws|amazonaws|googlecloud|azure|cloudflare|fastly|akamai)\.com$/i;

// ── CAPA 4 — Helper unificado: clasifica email como reject / low / ok ─────
// Devuelve { verdict, reason, score }
//   verdict: "reject" | "low_quality" | "ok"
//   reason:  string descriptivo (audit trail)
//   score:   guidance al ranking (negative = nunca pickear)
function classifyEmail(email, leadDomain = "") {
  if (!email || typeof email !== "string") return { verdict: "reject", reason: "malformed_empty", score: -1 };
  const e = email.toLowerCase().trim();
  if (!e.includes("@") || e.split("@").length !== 2) return { verdict: "reject", reason: "malformed_no_at", score: -1 };
  const [local, dom] = e.split("@");
  if (!local || !dom || local.length < 2) return { verdict: "reject", reason: "malformed_short_local", score: -1 };

  // Capa 1: local-part patterns (anclado al inicio)
  if (GARBAGE_LOCAL.test(local + "@")) return { verdict: "reject", reason: "garbage_local_anchored", score: -1 };
  // Capa 2: local-part contains (cualquier posición)
  if (GARBAGE_LOCAL_CONTAINS.test(local)) return { verdict: "reject", reason: "garbage_local_contains", score: -1 };
  // Capa 3: domain keywords (registrar/privacy/hosting/cloud/etc en el dominio)
  if (GARBAGE_DOMAIN_KEYWORDS.test(dom) || GARBAGE_DOMAIN_SUBDOMAIN.test(dom)) return { verdict: "reject", reason: "garbage_domain_keywords", score: -1 };
  // Capa 3b: domain match exacto contra patterns viejos (defense-in-depth, redundante pero seguro)
  if (typeof GARBAGE_DOMAIN_PATTERN !== "undefined" && GARBAGE_DOMAIN_PATTERN.test(dom)) {
    return { verdict: "reject", reason: "garbage_domain_pattern", score: -1 };
  }
  // Capa 3c: local-part con TLD adentro (caso "site.com@registrar.com")
  if (/\.(com|net|org|io|co|tv|me|info|biz|us|uk|de|es|fr|it|br|ar|mx)$/i.test(local)) {
    return { verdict: "reject", reason: "malformed_tld_in_local", score: -1 };
  }
  // Capa 3d: cross-domain a no-webmail (defense-in-depth)
  if (leadDomain) {
    const _lead = leadDomain.toLowerCase().replace(/^www\./, "");
    const _isWebmail = /^(gmail|hotmail|outlook|live|yahoo|aol|icloud|protonmail|gmx|me)\.com$/.test(dom);
    const _domMatches = dom === _lead || dom.endsWith("." + _lead) || _lead.endsWith("." + dom);
    if (!_domMatches && !_isWebmail) return { verdict: "reject", reason: "cross_domain_recipient", score: -1 };
  }

  // Genéricos: pasa pero baja calidad — solo pickear si no hay opción mejor
  const GENERIC_RE = /^(info|contact|contacto|contato|contatto|kontakt|hello|hi|hey|hola|ola|olá|support|soporte|suporte|atendimento|mail|email|inbox|bonjour|news|press|prensa|imprensa|stampa|presse|noticias|reception|recepcion|recepcao|general|sales|ventas|marketing|publicidade|publicidad|comercial|editor|editorial|redaccion|redacao|jurídico|juridico|juridique)$/i;
  if (GENERIC_RE.test(local)) return { verdict: "low_quality", reason: "generic_role", score: 20 };

  // OK → score guidance basado en shape
  // - person-like (nombre.apellido): 80
  // - single name corto: 50
  // - otro: 40
  if (/^[a-z]+\.[a-z]+$/i.test(local)) return { verdict: "ok", reason: "person_firstname_lastname", score: 80 };
  if (/^[a-z]+_[a-z]+$/i.test(local)) return { verdict: "ok", reason: "person_underscore", score: 75 };
  if (/^[a-z]{3,15}$/i.test(local))   return { verdict: "ok", reason: "single_name", score: 50 };
  return { verdict: "ok", reason: "other", score: 40 };
}

// ── Website composite scoring ──────────────────────────────
// Score 0-100 por lead, con gates duros (return -1 = descartar).
// Basado en: GEO target ADEQ (LATAM/ES/EU prioritario, Tier1/UK/RU descarta),
// Engagement (≥400K + datos completos), Categoría (adult/streaming descartan),
// Ad networks (menos partners detectadas = más open), Idioma (ru/zh penaliza).

const GEO_BUCKETS = {
  // LATAM + España + paises hispanos = +30 (target principal ADEQ)
  hi: new Set([
    "AR","MX","CO","PE","CL","VE","EC","BO","PY","UY","GT","DO","HN","SV","NI","CR","PA","CU","PR","ES",
    "Argentina","Mexico","Colombia","Peru","Chile","Venezuela","Ecuador","Bolivia","Paraguay","Uruguay",
    "Guatemala","Dominican Republic","Honduras","El Salvador","Nicaragua","Costa Rica","Panama","Cuba",
    "Puerto Rico","Spain"
  ]),
  // Europa continental sin UK/RU = +25
  mid: new Set([
    "DE","FR","IT","PT","NL","BE","PL","RO","CZ","SK","HU","BG","GR","SE","NO","DK","FI","IE","AT","CH","HR","SI","RS","UA",
    "Germany","France","Italy","Portugal","Netherlands","Belgium","Poland","Romania","Czech Republic",
    "Slovakia","Hungary","Bulgaria","Greece","Sweden","Norway","Denmark","Finland","Ireland","Austria",
    "Switzerland","Croatia","Slovenia","Serbia","Ukraine"
  ]),
  // África = +15 (puede servir)
  africa: new Set([
    "NG","KE","ZA","EG","MA","DZ","TN","GH","ET","UG","TZ","SN","CM","CI","ZM","ZW","RW","MZ","AO",
    "Nigeria","Kenya","South Africa","Egypt","Morocco","Algeria","Tunisia","Ghana","Ethiopia","Uganda",
    "Tanzania","Senegal","Cameroon","Ivory Coast","Zambia","Zimbabwe","Rwanda","Mozambique","Angola"
  ]),
  // Asia (no India) = +5 baja conversión
  asia: new Set([
    "JP","KR","TH","VN","ID","MY","PH","TW","SG","BD","PK","LK","KH","MM","NP","HK","MN",
    "Japan","South Korea","Thailand","Vietnam","Indonesia","Malaysia","Philippines","Taiwan","Singapore",
    "Bangladesh","Pakistan","Sri Lanka","Cambodia","Myanmar","Nepal","Hong Kong","Mongolia"
  ]),
  // DESCARTE: Tier 1 + UK + Rusia (no encajan al portfolio actual ADEQ)
  blocked: new Set([
    "US","CA","AU","NZ","GB","UK","RU","BY","IL",
    "United States","Canada","Australia","New Zealand","United Kingdom","Russia","Belarus","Israel"
  ]),
};

function scoreGeo(geo) {
  if (!geo) return 5; // sin geo conocido — neutro bajo
  const g = geo.trim();
  if (GEO_BUCKETS.blocked.has(g)) return -1000; // descarte total
  if (GEO_BUCKETS.hi.has(g))      return 30;
  if (GEO_BUCKETS.mid.has(g))     return 25;
  if (GEO_BUCKETS.africa.has(g))  return 15;
  if (GEO_BUCKETS.asia.has(g))    return 5;
  return 10; // desconocido pero no blocked — neutro
}

// Categorías que descartan el lead (gate duro)
const BLOCKED_CATEGORIES = new Set(["adult","streaming","gambling"]);

// Ad networks ADEQ partner — si el sitio YA tiene muchas, hay menos espacio para nosotros
const ADEQ_PARTNER_NETWORKS = new Set([
  "Sparteo","Seedtag","Taboola","Missena","Viads","MGID","Clever Advertising","Vidoomy",
  "Vidverto","Ezoic","Clickio","360Playvid","Truvid","Optad360","Embi Media","Snigel"
]);

// Idiomas que penalizan (Ruso/Chino fuera del portfolio)
const PENALIZED_LANGS = new Set(["ru","zh","zh-cn","zh-tw","ja","ko"]);

function scoreWebsite(lead) {
  const reasons = [];
  let score = 0;

  // ════════════════════════════════════════════════════════════
  // HARD GATES (user 2026-05-18) — únicos motivos de rechazo:
  //   1. Categorías peligrosas (adult / streaming pirata / gambling)
  //   2. Mega-corps (google, amazon, facebook, etc. — no monetizables)
  //   3. Government / educational (.gov, .edu, .mil, .gob, .ac)
  // GEO y categoría-preferencia (News > Sports etc.) YA NO bloquean:
  // si el lead tiene traffic ≥ 400K y no cae en estos gates, el Agent
  // lo manda. El resto del score queda solo informativo (badges/stars UI).
  // ════════════════════════════════════════════════════════════
  const cat    = (lead.category || "").toLowerCase();
  const domain = (lead.domain   || "").toLowerCase();

  if (BLOCKED_CATEGORIES.has(cat)) {
    return { score: -1, color: "red", reasons: [`cat_blocked:${cat}`] };
  }
  // Mega-corps — usa el mismo set de EXCLUDE_DOMAINS que el autopilot
  if (EXCLUDE_DOMAINS.has(domain) || EXCLUDE_DOMAINS.has(domain.replace(/^www\./, ""))) {
    return { score: -1, color: "red", reasons: [`mega_corp:${domain}`] };
  }
  // Government / educational / military / academic
  if (/(^|\.)(gov|edu|mil|gob|ac)(\.|$)/i.test(domain)) {
    return { score: -1, color: "red", reasons: [`gov_edu:${domain}`] };
  }

  // ── A partir de acá: SCORING INFORMATIVO ────────────────────
  // Score se persiste para UI (stars, badges en Prospects tab) pero
  // NO afecta la selección del Agent — el query ya no ordena por score
  // y la única condición de envío es traffic >= 400K + no hard gate.

  // GEO (informativo)
  const geoPts = scoreGeo(lead.geo);
  if (geoPts > 0) { score += geoPts; reasons.push(`geo:${lead.geo || "?"}=${geoPts}`); }

  // Categoría (informativo)
  if (cat && cat !== "other") { score += 5; reasons.push(`cat:${cat}=+5`); }

  // 3. ENGAGEMENT — traffic ≥ 400K + datos completos
  const tr = lead.traffic || 0;
  if (tr >= 1_000_000)      { score += 25; reasons.push("traffic≥1M=+25"); }
  else if (tr >= 500_000)   { score += 20; reasons.push("traffic≥500K=+20"); }
  else if (tr >= 400_000)   { score += 15; reasons.push("traffic≥400K=+15"); }
  else if (tr > 0)          { score += 5;  reasons.push(`traffic=${tr}=+5`); }
  // datos completos = bonus por tener category + geo + emails parseados
  let completeness = 0;
  if (lead.category && lead.category !== "other") completeness++;
  if (lead.geo)                                   completeness++;
  if (lead.language)                              completeness++;
  if (Array.isArray(lead.emails) && lead.emails.length) completeness++;
  if (completeness >= 3) { score += 10; reasons.push(`complete=${completeness}=+10`); }
  else if (completeness >= 2) { score += 5; reasons.push(`complete=${completeness}=+5`); }

  // 4. AD NETWORKS — menos partners ADEQ detectadas = más open al pitch
  const detected = Array.isArray(lead.ad_networks) ? lead.ad_networks : [];
  const adeqDetected = detected.filter(n => ADEQ_PARTNER_NETWORKS.has(n));
  if (adeqDetected.length === 0)      { score += 20; reasons.push("ad_open=+20"); }
  else if (adeqDetected.length === 1) { score += 10; reasons.push(`ad:${adeqDetected.join(",")}=+10`); }
  else if (adeqDetected.length === 2) { score += 0;  reasons.push(`ad:${adeqDetected.join(",")}=0`); }
  else                                { score -= 10; reasons.push(`ad_saturado=-10`); }
  // Seedtag específico = competidor directo (penaliza extra)
  if (adeqDetected.includes("Seedtag")) { score -= 10; reasons.push("seedtag=-10"); }

  // 5. IDIOMA (ru/zh/ja/ko penaliza)
  const lang = (lead.language || "").toLowerCase();
  if (PENALIZED_LANGS.has(lang)) { score -= 15; reasons.push(`lang:${lang}=-15`); }
  else if (lang) { score += 5; reasons.push(`lang:${lang}=+5`); }

  // Stars 1-5 (1=peor, 5=mejor) — mapping del score 0-100
  let stars;
  if      (score >= 80) stars = 5;
  else if (score >= 60) stars = 4;
  else if (score >= 40) stars = 3;
  else if (score >= 20) stars = 2;
  else                  stars = 1;
  // Color buckets (compat con UI existente)
  const color = stars >= 4 ? "green" : stars >= 3 ? "yellow" : "orange";
  return { score, stars, color, reasons };
}

// Rank email por probabilidad de ser un buen contacto B2B. Más alto = mejor.
// Sync, sin red. Llamado desde runAgentCycle para elegir el mejor del array.
// (No confundir con scoreEmail() async que hace SMTP verify y devuelve color/red).
// Categoría → roles ideales del local-part (un MB humano sabe esto intuitivamente)
const CATEGORY_TARGET_ROLES = {
  news:         /^(editor|redacao|redaccion|redazione|writer|periodista|journalist|prensa|press|director|gerente)/,
  sports:       /^(marketing|comercial|sponsorship|patrocin|publicidad|ads|director|gerente|jefe)/,
  finance:      /^(marketing|comercial|business|partnerships|director|cmo|ceo)/,
  entertainment:/^(marketing|comercial|publicidad|partnerships|brand|ads|director)/,
  technology:   /^(marketing|partnerships|business|bd|growth|director)/,
  health:       /^(marketing|comercial|director|gerente|jefe)/,
  travel:       /^(marketing|comercial|partnerships|sales|business|director)/,
  food:         /^(marketing|publicidad|comercial|chef|director|brand)/,
  business:     /^(marketing|partnerships|business|bd|growth|director|cmo)/,
  automotive:   /^(marketing|comercial|publicidad|sales|director|gerente)/,
};

function rankEmail(email, siteDomain, leadCategory = "") {
  if (!email || typeof email !== "string" || !email.includes("@")) return -1;
  const lower = email.toLowerCase();
  if (GARBAGE_LOCAL.test(lower) || GARBAGE_DOMAIN_PATTERN.test(lower)) return -1;
  if (isBouncedSync(lower)) return -1; // hard reject: ya bounceó antes
  const [local, dom] = lower.split("@");
  if (!local || !dom) return -1;
  if (GARBAGE_LOCAL_CONTAINS.test(local)) return -1;
  if (GARBAGE_DOMAIN_KEYWORDS.test(dom) || GARBAGE_DOMAIN_SUBDOMAIN.test(dom)) return -1; // Capa 3: keywords/subdominios garbage

  // Malformed local-part: contiene TLD (.com/.net/.io/etc) → scrape artifact
  // Caso real 2026-05-13: "lindaikejisblog.com@protecteddomainservices.com"
  // donde el scraper agarro "site.com@registrar.com" como un solo email.
  if (/\.(com|net|org|io|co|tv|me|info|biz|us|uk|de|es|fr|it|br|ar|mx)$/i.test(local)) return -1;

  // Hash/random-string detection: emails como "a8f9d2k1@x.com" probablemente auto-gen.
  if (/^[a-z0-9]{8,}$/.test(local) && !/[aeiou]{2}/.test(local)) return -1;

  let score = 0;
  const cleanSite = (siteDomain || "").replace(/^www\./, "");

  // ── DOMAIN MATCH (peso 0-40) ──
  // Email del MISMO dominio del sitio → señal MUY fuerte (es probablemente real)
  const isFreeWebmail = /^(gmail|yahoo|hotmail|outlook|live|aol|icloud|protonmail|gmx|mail\.ru|yandex|me)\./.test(dom);
  if (cleanSite) {
    if (dom === cleanSite) score += 40;
    else if (dom.endsWith("." + cleanSite) || cleanSite.endsWith("." + dom)) score += 35;
    else if (isFreeWebmail) {
      // Webmail cross-domain — penalty intermedio (-15). Es esperado que un
      // contacto B2B use gmail personal, pero corporate email mismo-dominio
      // sigue siendo MEJOR. Esto evita que un john.doe@gmail le gane a
      // marketing@empresa.com.
      score -= 15;
    } else {
      // Cross-domain a OTRO dominio corporativo — penalidad fuerte.
      score -= 50;
    }
  }

  // ── ROLE QUALITY (peso -20 a +90) ──
  // Roles comerciales = target ideal (decision-makers de monetización)
  const COMMERCIAL = /^(marketing|comercial|business|partnerships?|partner|ads?|advertising|publicidad|monetiza|ventas|sales|bd|growth|director|gerente|manager|jefe|head|brand|sponsorship|patrocin)\b/;
  const EDITORIAL  = /^(editor|editor-in-chief|chief-editor|redacao|redaccion|redazione|writer|periodista|journalist|prensa|press|reporter|news-?desk)\b/;
  const EXEC       = /^(ceo|cmo|cto|coo|founder|co-?founder|owner|publisher|presidente|president)\b/;

  // ORDEN: chequear generics PRIMERO (antes que "single name"), sino palabras
  // tipo "contato" se cuelan como single-name con score alto en lugar de role.
  const IS_GENERIC = /^(info|contact|contacto|contato|contatto|contattare|kontakt|kontact|hello|hi|hey|hola|ola|olá|support|soporte|suporte|atendimento|mail|email|inbox|bonjour|news|press|prensa|imprensa|stampa|presse|presseportal|noticias|reception|recepcion|recepcao|general)$/i;

  if (EXEC.test(local))           score += 90;       // CEO/founder = jackpot
  else if (COMMERCIAL.test(local)) score += 80;
  else if (EDITORIAL.test(local))  score += 60;
  // Pattern firstname.lastname (juan.perez@x.com) = persona real
  else if (/^[a-z]{2,}[._-][a-z]{2,}$/.test(local)) score += 70;
  // Pattern firstinitial+lastname (jperez@x.com, mgarcia@x.com) = común corp
  else if (/^[a-z][a-z]{4,14}$/.test(local) && local.length >= 5 && /[aeiou]/.test(local) && !IS_GENERIC.test(local)) score += 55;
  // Generics — OK pero baja conversión. Cobertura multi-idioma (PT/IT/FR/DE/ES).
  // CHEQUEADO ANTES que single-name para que "contato" no se cuele como persona.
  else if (IS_GENERIC.test(local)) score += 15;
  // Single name (juan@x.com) — could be person or generic
  else if (/^[a-z]{3,12}$/.test(local) && /[aeiou]/.test(local)) score += 30;

  // ── CATEGORY-ROLE MATCH (peso 0-25) ──
  // Si el sitio es "sports" y el email es marketing/comercial → bonus extra
  // (un MB humano sabe que sports + comercial es golden)
  const cat = (leadCategory || "").toLowerCase();
  if (CATEGORY_TARGET_ROLES[cat] && CATEGORY_TARGET_ROLES[cat].test(local)) {
    score += 25;
  }

  // ── PENALTIES ──
  // Dígitos largos en local-part (3+) = lista vieja, automated
  if (/\d{3,}/.test(local)) score -= 40;
  // Local muy corto (<3) o muy largo (>30) = sospechoso
  if (local.length < 3) score -= 30;
  if (local.length > 30) score -= 25;
  // Soft penalty para palabras con flavor spam (real estate, sales, etc).
  // No descarta — baja prioridad para que ganen otros candidatos si los hay.
  if (/property|sale|offer|click|freemium|promo|bonus/.test(local)) score -= 15;
  // Free webmail = penalizar pero NO descartar (un MB humano puede mandar)
  if (isFreeWebmail) score -= 20; // antes -35, ahora -20 para que webmail con persona real sobreviva

  // ── LANGUAGE MATCH bonus ──
  // Si el sitio es .br y el email tiene palabras pt (vendas, comercial) → +5
  // Si .ar/.es/.mx y palabras es (ventas, comercial) → +5
  // Pequeño extra que ayuda a desambiguar entre candidatos similares
  const tld = cleanSite.split(".").pop();
  if ((tld === "br" || tld === "pt") && /(vendas|comercial|publicidade|atendimento)/.test(local)) score += 5;
  if (/^(ar|es|mx|cl|co|pe|uy)$/.test(tld) && /(ventas|comercial|publicidad|atencion)/.test(local)) score += 5;

  return score;
}
const GARBAGE_DOMAIN_PATTERN = new RegExp([
  // Keywords del dominio — anclados a inicio o tras @/. para evitar matches
  // dentro de palabras (lawscope.com NO debe matchear "aws", paws.com tampoco).
  "(^|[.@])(?:gdpr|aws|amazonaws|amazonses|cloudfront|cloudflare|fastly|akamai|whois)(?=[.-])",
  // GDPR/protect domains (más laxo — solo si es claramente un proxy)
  "(^|[.@])(?:protect|protected|gdpr-?protect|protect-?service)\\.",
  // Subdominios o dominios raíz de admin/whois/abuse/support (también después de @)
  "(^|[.@])(?:nic|abuse|donuts|godaddy|cert|registry|registrar|hosting|host|hostingpanel|trustandsafety)\\.",
  // Cloud providers - support/abuse desks (NO son publishers)
  "(^|[.@])(?:aws|amazonaws|cloudfront|googlecloud|azure|microsoft|cloudflare|fastly)\\.com",
  // Privacy/proxy services
  "domainsbyproxy\\.com|whoisguard|whoisprivacy|whoisprotect|domainprotect|privacyprotect|contactprivacy|perfectprivacy|namebrightprivacy|withheldforprivacy|protect-?service|protectedmail|protecteddomainservices|panelregister|identity-?protect",
  // Registrars (B2B abuse desks)
  "dropped\\.|internetx\\.com|markmonitor|cscglobal|csc-corp|comlaude|safenames|gandi\\.net|key-systems|1api\\.net|netim\\.com|psi-usa|nameshield|epag\\.de|eurodns|realtimeregister|tld-box|enom\\.|networksolutions|tucows|porkbun\\.com|namecheap.*proxy|hostgator|bluehost|godaddyguard|hostafrica|dominio.*\\.com\\.|dominios?[a-z]+\\.ec|dominios?[a-z]+\\.com",
  // Heurística genérica: cualquier dominio con palabra "registrar/registry/dnshosting/
  // domainsby/domainservices/namehost" — caso real latinregistrar.com.br 2026-05-13
  "(^|[.@])[a-z0-9-]*(?:registrar|registry|dnshosting|domainsby|domainservices|namehost|domainname)[a-z0-9-]*\\.",
  // GDPR masking
  "gdpr-?masked?|gdpr-?mask\\.com|gdpr-?protect|data-?protected|registrant-?private|domains[-._]?by[-._]?proxy|registry-?proxy",
  // Disposable/temp emails
  "mailinator|guerrillamail|tempmail|throwaway|trashmail|sharklasers|yopmail|10minutemail|disposable|fakeinbox|mailtrap",
  // Transactional senders (no humans)
  "mailgun\\.org|sendgrid\\.net|amazonses\\.com|postmarkapp\\.com|mandrillapp\\.com|sparkpostmail",
  // Error trackers / sysmail
  "sentry\\.io|bugsnag\\.com|errorception|raygun\\.io|rollbar\\.com",
  // Mailing list managers
  "list-server|listserv\\.|mailman\\.|maillists?\\.",
  // Test / fake / local
  "example\\.(?:com|org|net)|test\\.(?:com|org|net)|localhost|invalid|local",
].join("|"), "i");
const GENERIC_LOCAL = /^(info|contact|hello|hi|sales|support|ventas|comercial|prensa|press|editor|editorial|redaccion|redacción|mail|email)@/i;

// Filtra emails invalidos / sin MX antes de mostrarlos al MB o al agente.
// Llamado al INSERT en review_queue → previene mostrar/elegir emails que dan
// bounce. Solo chequeos GRATIS: garbage regex + MX records (DoH Cloudflare).
async function validateEmailsBatch(emails) {
  if (!Array.isArray(emails) || emails.length === 0) return [];
  const out = [];
  for (const e of emails) {
    if (!e || typeof e !== "string" || !e.includes("@")) continue;
    const lower = e.toLowerCase().trim();
    if (GARBAGE_LOCAL.test(lower) || GARBAGE_DOMAIN_PATTERN.test(lower)) continue;
    const local = lower.split("@")[0];
    if (GARBAGE_LOCAL_CONTAINS.test(local)) continue;
    const dom = lower.split("@")[1];
    if (GARBAGE_DOMAIN_KEYWORDS.test(dom) || GARBAGE_DOMAIN_SUBDOMAIN.test(dom)) continue;
    const mx = await _hasMxRecords(dom);
    // En el INSERT a review_queue toleramos unknown (null) — el dominio puede
    // ser válido y el DNS estar con blip. La verificación dura ocurre en
    // scoreEmail antes del send. Confirmed false (no MX) sí descarta acá.
    if (mx !== false) out.push(lower);
  }
  return [...new Set(out)];
}

// MX records cache — LRU cap 500 dominios para evitar leak en worker 24/7.
// Valores: true (has MX), false (confirmed no MX), null (unknown — DNS error).
// Antes devolvía true en errores de red, lo que dejaba pasar dominios inexistentes
// durante DNS blips → bounces seguros. Ahora devuelve null y el caller decide.
const _mxCache = new Map();
const MX_CACHE_MAX = 500;
async function _hasMxRecords(domain) {
  if (!domain) return false;
  if (_mxCache.has(domain)) return _mxCache.get(domain);
  if (_mxCache.size >= MX_CACHE_MAX) {
    const firstKey = _mxCache.keys().next().value;
    _mxCache.delete(firstKey);
  }
  // Retry una vez con timeout corto para tolerar blip transitorio.
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const res = await fetch(`https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(domain)}&type=MX`, {
        headers: { "accept": "application/dns-json" },
        signal: AbortSignal.timeout(attempt === 0 ? 3000 : 5000),
      });
      if (!res.ok) continue; // retry
      const data = await res.json();
      const has = Array.isArray(data?.Answer) && data.Answer.length > 0;
      _mxCache.set(domain, has);
      return has;
    } catch { /* retry */ }
  }
  // Ambos intentos fallaron. NO cachear, devolver null = unknown.
  return null;
}

async function scoreEmail(email) {
  if (!email || typeof email !== "string" || !email.includes("@")) {
    return { color: "red", reason: "invalid_format" };
  }
  const lower = email.toLowerCase().trim();
  if (GARBAGE_LOCAL.test(lower) || GARBAGE_DOMAIN_PATTERN.test(lower)) {
    return { color: "red", reason: "garbage_address" };
  }
  const localPart = lower.split("@")[0];
  if (GARBAGE_LOCAL_CONTAINS.test(localPart)) {
    return { color: "red", reason: "garbage_local_contains" };
  }
  const dom = lower.split("@")[1];
  if (GARBAGE_DOMAIN_KEYWORDS.test(dom) || GARBAGE_DOMAIN_SUBDOMAIN.test(dom)) {
    return { color: "red", reason: "garbage_domain_keywords" };
  }
  // 1) MX records — null=unknown, false=confirmado sin MX, true=tiene MX.
  // Confirmado-sin-MX = red (dominio inexistente). Unknown = degrade a yellow
  // (no asumimos green sin confirmación). Antes hi-confidence en blips DNS.
  const hasMX = await _hasMxRecords(dom);
  if (hasMX === false) return { color: "red", reason: "no_mx_records" };
  const mxUnknown = hasMX === null;

  // 2) SMTP verify via eva.pingutil (free, no auth)
  let smtpVerified = false;
  let catchAll = false;
  try {
    const res = await fetch(`https://api.eva.pingutil.com/email?email=${encodeURIComponent(lower)}`, {
      signal: AbortSignal.timeout(8000),
    });
    if (res.ok) {
      const data = await res.json();
      const valid       = data?.status === "success" && data?.data?.valid_syntax === true;
      const deliverable = data?.data?.deliverable === true;
      const isDisposable = data?.data?.disposable === true;
      catchAll = data?.data?.catch_all === true || data?.data?.smtp_catch_all === true;
      if (isDisposable) return { color: "red", reason: "disposable" };
      if (!valid)        return { color: "red", reason: "invalid_syntax_smtp" };
      if (!deliverable)  return { color: "red", reason: "undeliverable_smtp" };
      smtpVerified = true;
    }
  } catch {} // pingutil falló → smtpVerified queda false

  // Catch-all domain: el servidor acepta cualquier email pero suele rebotar
  // después al usuario real → high bounce risk. Degradamos a yellow.
  if (catchAll) return { color: "yellow", reason: "catch_all_domain" };

  // Generic role-based local-part (info@, contact@) → yellow.
  if (GENERIC_LOCAL.test(lower)) return { color: "yellow", reason: "generic_address" };

  // Si MX o SMTP quedaron sin verificar duro, no promovemos a green.
  if (mxUnknown && !smtpVerified) return { color: "yellow", reason: "mx_smtp_unverified" };
  if (!smtpVerified)              return { color: "yellow", reason: "smtp_unverified" };

  return { color: "green", reason: "ok" };
}

// Self-check Claude: verifica que el pitch no contradiga datos de input.
// Bajo costo (max 100 tokens). Si detecta inconsistencia, return false.
async function selfCheckPitch(token, pitch, ctx) {
  const { domain, adsTxtExists, adNetworks, subjects } = ctx;
  if (!pitch || pitch.length < 30) return { ok: false, reason: "pitch_too_short" };
  // Heurística rápida sin Claude: detectar contradicciones obvias.

  // 1. Claim de ausencia de ads.txt (siempre prohibido salvo confirmación explícita)
  if (/no.{1,15}(tienen|hay|tiene|tienes|have).{1,15}ads\.txt/i.test(pitch)) {
    if (adsTxtExists !== false) return { ok: false, reason: "claims_no_ads_txt" };
  }
  // 2. Claim de ausencia de monetización
  if (/no.{1,15}(detect|veo|see|tienen|tiene|have).{1,25}(monetiz|ad.{1,5}network|ad.{1,5}stack|ningún.{1,10}sistema)/i.test(pitch)) {
    if (!(adNetworks?.length === 0)) return { ok: false, reason: "claims_no_ads_but_has_or_unknown" };
  }
  // 3. Meses hardcoded
  const months = /\b(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre|january|february|march|april|may|june|july|august|september|october|november|december)\b/i;
  if (months.test(pitch)) return { ok: false, reason: "mentions_specific_month" };
  // 4. Firma con nombre propio del equipo
  if (/\b(diego|max|maxi|maximiliano|agus|agustina)\s*$/im.test(pitch)) {
    return { ok: false, reason: "signed_with_personal_name" };
  }
  // 5. Frases prohibidas corporate-speak
  const banned = /\b(win[\s-]?win|sinerg|apalanc|ecosistema|estimad[oa]\b|sr\.|sra\.)\b/i;
  if (banned.test(pitch)) return { ok: false, reason: "corporate_speak_phrase" };
  // 6. Length: 50-180 palabras (mail inicial corto)
  const wordCount = pitch.split(/\s+/).filter(Boolean).length;
  if (wordCount > 200) return { ok: false, reason: `too_long_${wordCount}_words` };
  // 7. Subjects validation (cuando vienen)
  if (Array.isArray(subjects)) {
    for (const s of subjects) {
      if (!s) continue;
      const subjWords = s.split(/\s+/).filter(Boolean).length;
      if (subjWords > 10) return { ok: false, reason: `subject_too_long_${subjWords}_words` };
      if (/URGENT|!!!|RE:\s*RE:|OPORTUNIDAD/i.test(s)) {
        return { ok: false, reason: "subject_spam_pattern" };
      }
    }
  }
  return { ok: true };
}

// ── Gmail server-side via Google Service Account (Domain-Wide Delegation) ──
// El admin de Google Workspace configura una vez:
//   1. Crea service account en Google Cloud Console
//   2. Genera JSON key, lo guarda en Railway env GOOGLE_SERVICE_ACCOUNT_JSON
//   3. En Workspace admin: Security → API Controls → Domain-Wide Delegation
//      whitelist el client ID del SA con scope https://www.googleapis.com/auth/gmail.send
// Después: el worker firma JWT con la private key del SA, intercambia por access
// token "impersonating" cualquier user del workspace (mgargiulo@adeqmedia.com),
// y manda Gmail como ese user.

import { createSign } from "node:crypto";

let _saCredentials = null;
function getServiceAccount() {
  if (_saCredentials) return _saCredentials;
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) return null;
  try { _saCredentials = JSON.parse(raw); return _saCredentials; }
  catch { return null; }
}

const _accessTokenCache = new Map(); // userEmail → { token, expiresAt }
const ACCESS_TOKEN_CACHE_MAX = 50; // worker rara vez impersona >50 users distintos
async function getGmailAccessToken(impersonateUser) {
  const cached = _accessTokenCache.get(impersonateUser);
  if (cached && cached.expiresAt > Date.now() + 60_000) return cached.token;

  const sa = getServiceAccount();
  if (!sa) throw new Error("no_service_account_configured");

  // Build JWT for OAuth 2.0 Service Account flow
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss:   sa.client_email,
    sub:   impersonateUser,
    scope: "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.settings.basic https://www.googleapis.com/auth/gmail.readonly",
    aud:   "https://oauth2.googleapis.com/token",
    iat:   now,
    exp:   now + 3600,
  };
  const b64url = (obj) => Buffer.from(JSON.stringify(obj)).toString("base64url");
  const unsigned = `${b64url(header)}.${b64url(payload)}`;
  const sign = createSign("RSA-SHA256");
  sign.update(unsigned);
  const signature = sign.sign(sa.private_key, "base64url");
  const jwt = `${unsigned}.${signature}`;

  // Exchange JWT for access token
  const tokenRes = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion:  jwt,
    }),
  });
  if (!tokenRes.ok) {
    const err = await tokenRes.text();
    throw new Error(`gmail_oauth_failed: ${tokenRes.status} ${err.slice(0,300)}`);
  }
  const data = await tokenRes.json();
  if (!data.access_token) throw new Error("no_access_token_in_response");
  if (_accessTokenCache.size >= ACCESS_TOKEN_CACHE_MAX) {
    const firstKey = _accessTokenCache.keys().next().value;
    _accessTokenCache.delete(firstKey);
  }
  _accessTokenCache.set(impersonateUser, {
    token: data.access_token,
    expiresAt: Date.now() + (data.expires_in * 1000),
  });
  return data.access_token;
}

// Cache de signature HTML por user (TTL 1h) — evita refetch en cada send
const _signatureCache = new Map();
const SIG_TTL_MS = 60 * 60 * 1000;
async function getGmailSignatureHtmlServer(userEmail) {
  const cached = _signatureCache.get(userEmail);
  if (cached && Date.now() - cached.ts < SIG_TTL_MS) return cached.html;
  try {
    const accessToken = await getGmailAccessToken(userEmail);
    const res = await fetch(
      "https://gmail.googleapis.com/gmail/v1/users/me/settings/sendAs",
      { headers: { "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return "";
    const data = await res.json();
    const primary = data.sendAs?.find(s => s.isDefault) || data.sendAs?.[0];
    const html = primary?.signature || "";
    _signatureCache.set(userEmail, { html, ts: Date.now() });
    return html;
  } catch { return ""; }
}

function _textToHtmlServer(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\r\n/g, "\n")
    // Anti-linkify: rompe la auto-detección de URLs en el cliente del
    // destinatario. Reemplaza `.` con `&#46;` dentro de patrones de dominio
    // (alfanuméricos-dot-alfanuméricos). Visual idéntico, sin <a> link.
    // Pedido del user 2026-05-18: URLs como texto plano.
    .replace(/\b([a-z0-9-]{2,}(?:\.[a-z0-9-]{2,})+)\b/gi, (m) => m.replace(/\./g, "&#46;"))
    .split("\n\n")
    .map(p => `<p>${p.replace(/\n/g, "<br/>")}</p>`)
    .join("\n");
}

async function sendGmailServer(_token, userEmail, { to, subject, body, agentActionId = null }) {
  const accessToken = await getGmailAccessToken(userEmail);
  const signatureHtml = await getGmailSignatureHtmlServer(userEmail);
  // Open-rate tracking pixel — solo si tenemos un agent_action_id (envíos del agente)
  const trackingPixel = agentActionId
    ? `<img src="${SUPABASE_URL}/functions/v1/track-open?aid=${agentActionId}" width="1" height="1" alt="" style="display:block;border:0;width:1px;height:1px"/>`
    : "";

  // Subject RFC 2047 encoded para soportar acentos (ej. "monetización")
  const subjectEncoded = /^[\x20-\x7E]*$/.test(subject)
    ? subject
    : `=?UTF-8?B?${Buffer.from(subject, "utf-8").toString("base64")}?=`;

  let mime;
  // Forzar multipart si hay tracking pixel (necesita HTML), aunque no haya signature
  const useMultipart = (signatureHtml && signatureHtml.trim()) || trackingPixel;
  if (useMultipart) {
    // Multipart: text + HTML con signature default del user
    const boundary = `----=adeq_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`;
    const htmlBody = `${_textToHtmlServer(body)}${signatureHtml ? "\n<br/>\n" + signatureHtml : ""}${trackingPixel ? "\n" + trackingPixel : ""}`;
    mime = [
      `To: ${to}`,
      `From: ${userEmail}`,
      `Subject: ${subjectEncoded}`,
      "MIME-Version: 1.0",
      `Content-Type: multipart/alternative; boundary="${boundary}"`,
      "",
      `--${boundary}`,
      "Content-Type: text/plain; charset=utf-8",
      "Content-Transfer-Encoding: 8bit",
      "",
      body,
      "",
      `--${boundary}`,
      "Content-Type: text/html; charset=utf-8",
      "Content-Transfer-Encoding: 8bit",
      "",
      htmlBody,
      "",
      `--${boundary}--`,
    ].join("\r\n");
  } else {
    // Sin signature → text/plain (legacy)
    mime = [
      `To: ${to}`, `From: ${userEmail}`, `Subject: ${subjectEncoded}`,
      "Content-Type: text/plain; charset=utf-8", "MIME-Version: 1.0", "",
      body,
    ].join("\r\n");
  }
  const raw = Buffer.from(mime, "utf-8").toString("base64")
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

  const sendRes = await fetch("https://gmail.googleapis.com/gmail/v1/users/me/messages/send", {
    method: "POST",
    headers: { "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ raw }),
  });
  if (!sendRes.ok) {
    const errText = await sendRes.text();
    throw new Error(`gmail_send_failed: ${sendRes.status} ${errText.slice(0,200)}`);
  }
  return await sendRes.json();
}

// ── Push to Monday (server-side) ──
// Asegura que el dominio empiece con "www." al pushear a Monday.
// - quita protocolo (http(s)://)
// - quita trailing slash
// - si ya tiene www. → no duplica
// - si es subdomain (ej. blog.foo.com) → NO agrega www (sería www.blog.foo.com, raro)
function _ensureWwwPrefix(domain) {
  if (!domain) return "";
  let d = String(domain).trim().toLowerCase();
  d = d.replace(/^https?:\/\//, "").replace(/\/+$/, "");
  if (d.startsWith("www.")) return d;
  // Si ya tiene 3+ niveles (ej. m.foo.com, blog.foo.com), no forzamos www.
  const parts = d.split(".");
  if (parts.length > 2) {
    // Excepción: si el subdominio es ccSLD (.com.ar, .co.uk), sí agregamos www.
    const last2 = parts.slice(-2).join(".");
    if (MULTI_PART_TLDS && MULTI_PART_TLDS.has(last2)) return "www." + d;
    return d;
  }
  return "www." + d;
}

async function pushToMondayServer(monday_api_key, payload, boardId) {
  // Crea item nuevo en Monday con todas las columnas. Usado solo cuando agent
  // decide enviar (no antes). Imita el botón "Push to Monday" del popup.
  // Shapes según config.js MONDAY_COLUMNS:
  // - text columns (texto6/texto7) = string crudo
  // - email = { email, text }
  // - phone = { phone, countryShortName }
  // - status/dropdown (estado/idioma) = { label } o { index }
  // - person = { personsAndTeams }
  // - date = { date }
  // GEO normalizado a label español sin tildes.
  // Tráfico formateado: 540K / 2.3M (no raw 548091).
  const geoNormalized = normalizeMondayGeo(payload.geo);
  // Monday item_name DEBE empezar con www. — regla de la cuenta para que el
  // hipervínculo funcione siempre. Si ya viene con www. o protocolo, no duplica.
  const itemName = _ensureWwwPrefix(payload.domain);
  const cols = {
    [MONDAY_COL_TRAFFIC]: payload.traffic_text || "",
    [MONDAY_COL_GEO]:     geoNormalized,
    [MONDAY_COL_EMAIL]:   { email: payload.email, text: payload.email },
    [MONDAY_COL_DATE]:    { date: new Date().toISOString().split("T")[0] },
    [MONDAY_COL_IDIOMA]:  { index: payload.idioma_idx || 0 },
    [MONDAY_COL_ESTADO]:  { label: "Propuesta Vigente (T)" },
    ...(payload.monday_user_id ? { [MONDAY_COL_OWNER]: { personsAndTeams: [{ id: payload.monday_user_id, kind: "person" }] } } : {}),
    ...(payload.phone ? { [MONDAY_COL_PHONE]: { phone: String(payload.phone), countryShortName: "" } } : {}),
    // NO incluir Comentarios — el user no quiere el pitch ahí.
  };
  const query = `mutation ($board: ID!, $name: String!, $cols: JSON!) {
    create_item (board_id: $board, item_name: $name, column_values: $cols, create_labels_if_missing: true) { id }
  }`;
  const res = await fetch("https://api.monday.com/v2", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": monday_api_key, "API-Version": "2024-01" },
    body: JSON.stringify({
      query,
      variables: { board: boardId, name: itemName, cols: JSON.stringify(cols) },
    }),
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`Monday HTTP ${res.status}`);
  const data = await res.json();
  if (data?.errors) throw new Error(`Monday errors: ${JSON.stringify(data.errors).slice(0,500)}`);
  return data?.data?.create_item?.id || null;
}

// Constantes de columnas Monday — DEBE matchear config.js MONDAY_COLUMNS
// del popup, sino los pushes quedan vacíos en el board real.
const MONDAY_COL_TRAFFIC = "texto7";          // Paginas Vistas (text)
const MONDAY_COL_GEO     = "texto6";          // Top Geo (text)
const MONDAY_COL_EMAIL   = "email_mm2edcd3";  // Email (email column type)
const MONDAY_COL_DATE    = "deal_close_date"; // Fecha Contacto (date)
const MONDAY_COL_IDIOMA  = "estado_12";       // Idioma (status/dropdown)
const MONDAY_COL_ESTADO  = "deal_stage";      // Estado (status/dropdown)
const MONDAY_COL_OWNER   = "deal_owner";      // Ejecutivo (person)
const MONDAY_COL_PITCH   = "texto";           // Comentarios (text)
const MONDAY_COL_PHONE   = "tel_fono_1";      // Telefono (phone column)
// Reengagement / Email Futuro — el agente al disparar el email B actualiza
// estas dos fechas con hoy+5 y hoy+10. Verificar IDs en Monday si el push falla.
const MONDAY_COL_FU1     = "fecha2_8";        // Fecha FU1 (today + 5)
const MONDAY_COL_FU2     = "fecha_1";         // Fecha FU2 (today + 10)

// Update Monday item estado (después de enviar mail)
async function updateMondayEstado(monday_api_key, itemId, boardId, estadoIdx) {
  const cols = JSON.stringify({ [MONDAY_COL_ESTADO]: { index: estadoIdx } });
  const query = `mutation ($board: ID!, $item: ID!, $cols: JSON!) {
    change_multiple_column_values (board_id: $board, item_id: $item, column_values: $cols) { id }
  }`;
  try {
    await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": monday_api_key, "API-Version": "2024-01" },
      body: JSON.stringify({ query, variables: { board: boardId, item: itemId, cols } }),
    });
  } catch {}
}

// ── runAgentCycle: el corazón del agent ──
// Por cada user habilitado, procesa hasta perCycleLimit leads del review_queue
// que cumplan los thresholds. Para cada uno: pitch → quality gate → push Monday → send Gmail.
async function runAgentCycle(token, allFlags) {
  // Pausa fin de semana — operativo solo Lun-Vie España (override: agent_test_mode=true)
  const _agentEarlyCfg = await getConfig(token).catch(() => ({}));
  const _agentTestModeEarly = String(_agentEarlyCfg.agent_test_mode || "").toLowerCase() === "true";
  if (!_agentTestModeEarly && _isWeekendSpain()) {
    log(`🤖 Agent: fin de semana España — sin envíos`);
    return;
  }

  const cfg = await getConfig(token);
  const aCfg = _agentCfg(cfg);
  const monday_api_key_default = cfg.monday_api_key || "";
  const TEST_MODE = String(cfg.agent_test_mode || "").toLowerCase() === "true";

  // Active hours check — fuera de 9-20 España no manda nada (ni Monday, ni mail)
  if (!TEST_MODE && (_isWeekendSpain() || _isOutsideActiveHours(aCfg.activeStart, aCfg.activeEnd))) {
    log(`🤖 Agent: fuera de horario laboral España (lun-vie ${aCfg.activeStart}-${aCfg.activeEnd}, hoy=${_spainWeekday()}, h=${_spainHour()})`);
    return;
  }
  if (TEST_MODE) log(`🧪 Agent TEST MODE ON — bypass active hours + daily cap + weekly target`);
  log(`🤖 Agent: ciclo iniciando (users=${allFlags.agentUsers.length}, threshold=${aCfg.thresholdTraffic}, maxPerDay=${aCfg.maxPerDay})`);

  // Refresh bounced cache + scan INBOX al inicio del ciclo
  await loadBouncedEmails(token);

  // Whitelist de users autorizados a usar el agent (defense-in-depth).
  // Lee toolbar_config.agent_whitelist (CSV) y fallback hardcoded a admin.
  // Para agregar un user nuevo sin redeploy:
  //   update toolbar_config set value='mgargiulo@adeqmedia.com,otro@adeqmedia.com'
  //   where key='agent_whitelist';
  const whitelistRaw = (cfg.agent_whitelist || "mgargiulo@adeqmedia.com").trim();
  const AGENT_WHITELIST = new Set(whitelistRaw.split(",").map(s => s.trim().toLowerCase()).filter(Boolean));

  for (const userEmail of allFlags.agentUsers) {
    if (!AGENT_WHITELIST.has((userEmail || "").toLowerCase())) {
      log(`🚫 Agent: user ${userEmail} no está en whitelist hardcoded — skip`);
      continue;
    }
    // Bounce scan (Gmail INBOX) — fire-and-forget para que no atrase el ciclo
    scanBouncesForUser(token, userEmail).catch(() => {});
    // Auto-reply scan (out-of-office, ticket systems, etc.) — también dispara retry
    scanAutoRepliesForUser(token, userEmail).catch(() => {});
    // Kill switch check
    if (await checkAgentKillSwitch(token, userEmail, aCfg)) continue;
    // Daily cap
    const sentToday = await getAgentDailyCount(token, userEmail);
    if (!TEST_MODE && sentToday >= aCfg.maxPerDay) {
      log(`🤖 Agent ${userEmail}: cap diario ${aCfg.maxPerDay} alcanzado (${sentToday})`);
      continue;
    }
    // Weekly target (si configurado): cuenta sent en últimos 7 días
    if (!TEST_MODE && aCfg.focus.weeklyTarget > 0) {
      const sentWeek = await getAgentWeeklyCount(token, userEmail);
      if (sentWeek >= aCfg.focus.weeklyTarget) {
        log(`🤖 Agent ${userEmail}: weekly target ${aCfg.focus.weeklyTarget} alcanzado (${sentWeek})`);
        continue;
      }
    }
    const remaining = TEST_MODE ? aCfg.perCycleLimit : (aCfg.maxPerDay - sentToday);
    const batchSize = Math.min(aCfg.perCycleLimit, remaining);

    // Aplicar focus filtros al query.
    // geos_priority son ISO 2-letter codes (AR, UY, ES...). Matchea contra
    // `geos_all` (text[] con top 3 ISO codes) usando overlap. Fallback al
    // campo legacy `geo` que guarda el country NAME (Argentina, Uruguay...)
    // para rows pre-migración.
    const focus = aCfg.focus;
    let geoClause = "";
    if (focus.geosPriority.length > 0) {
      const isoCodes = focus.geosPriority;
      const names    = isoCodes.map(c => COUNTRY_CODES[c]).filter(Boolean);
      const ovList   = `{${isoCodes.join(",")}}`;
      const inList   = [...isoCodes, ...names].map(s => `"${s}"`).join(",");
      // PostgREST OR: geos_all overlap ISO codes  OR  geo (legacy) in (codes ∪ names)
      geoClause = `&or=(geos_all.ov.${encodeURIComponent(ovList)},geo.in.(${encodeURIComponent(inList)}))`;
    } else if (focus.geosExcluded.length > 0) {
      const isoCodes = focus.geosExcluded;
      const names    = isoCodes.map(c => COUNTRY_CODES[c]).filter(Boolean);
      const inList   = [...isoCodes, ...names].map(s => `"${s}"`).join(",");
      geoClause = `&geo=not.in.(${encodeURIComponent(inList)})`;
    }
    let categoryClause = "";
    if (focus.categoriesPriority.length > 0) {
      const inList = focus.categoriesPriority.map(c => `"${c}"`).join(",");
      categoryClause = `&category=in.(${encodeURIComponent(inList)})`;
    }

    // Pull candidates: solo filtro de tráfico (mínimo absoluto) + focus.
    // El score se usa SOLO para ordenar (los más prometedores primero).
    // No filtramos por score porque la lógica del score puede cambiar y
    // no queremos perder leads buenos por una heurística inestable.
    const queueRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.${aCfg.thresholdTraffic}${geoClause}${categoryClause}&select=*&order=created_at.desc&limit=${batchSize * 5}`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const candidatesRaw = await queueRes.json();
    if (!Array.isArray(candidatesRaw) || candidatesRaw.length === 0) {
      log(`🤖 Agent ${userEmail}: 0 candidatos (threshold=${aCfg.thresholdTraffic}, geos=${focus.geosPriority.join(",")||"all"}, cat=${focus.categoriesPriority.join(",")||"all"})`);
      // Loggear al feed UI para que el admin vea actividad aunque no haya envíos
      await logAgentAction(token, userEmail, {
        domain: "_cycle_", action: "cycle_no_candidates",
        reason: `threshold=${aCfg.thresholdTraffic}`,
        details: { threshold: aCfg.thresholdTraffic, geos: focus.geosPriority, categories: focus.categoriesPriority },
      });
      continue;
    }

    // ── COMPOSITE SCORING ─────────────────────────────────────
    // scoreWebsite() devuelve {score, stars 1-5, color, reasons}.
    // Filtramos los que cayeron en gates duros (geo blocked, adult/streaming).
    // Persistimos el score a review_queue para que el popup muestre las stars.
    const scored = [];
    for (const c of candidatesRaw) {
      const sw = scoreWebsite(c);
      if (sw.score < 0) {
        // Marcar como rejected — no entran al pool del MB humano tampoco
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${c.id}`, {
          method: "PATCH",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ status: "rejected", validated_by: "agent:auto", validated_at: new Date().toISOString() }),
        }).catch(() => {});
        log(`  ❌ ${c.domain}: ${sw.reasons.join(", ")}`);
        continue;
      }
      // Persistir score si cambió mucho (evita PATCH spam)
      const oldScore = c.score || 0;
      if (Math.abs(oldScore - sw.score) >= 5) {
        fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${c.id}`, {
          method: "PATCH",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ score: sw.score }),
        }).catch(() => {});
        c.score = sw.score;
      }
      scored.push({ ...c, _scoreData: sw });
    }
    if (scored.length === 0) {
      log(`🤖 Agent ${userEmail}: todos los candidatos descartados por gates`);
      continue;
    }
    // Política user 2026-05-18: el score NO se usa para elegir URLs. Si pasó
    // el gate duro (score >= 0 = no NSFW, no blocked geo) y tiene traffic
    // ≥ threshold → es válida. Orden por created_at desc (más fresco primero).
    scored.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
    log(`🤖 Agent ${userEmail}: ${scored.length}/${candidatesRaw.length} candidatos válidos (FIFO por created_at, top: ${scored[0].domain})`);

    // No filtramos por sendtrack acá — la regla real es:
    // "no mandar si el dominio está EN MONDAY EN ESTADO ACTIVO".
    // Eso lo chequeamos PER LEAD abajo (más fresco que cachear sendtrack).
    const fresh = scored;

    let processed = 0;
    for (const lead of fresh) {
      if (processed >= batchSize) break;
      const domain = lead.domain;
      let emails = Array.isArray(lead.emails) ? lead.emails.filter(Boolean) : [];
      let leadTraffic = lead.traffic || 0;
      let leadGeo = lead.geo || "";
      let leadLanguage = (lead.language || "").toLowerCase().split("-")[0];
      let reservedId = null;

      try {
        // ── DETECCIÓN ROBUSTA DE IDIOMA (CRÍTICO) ──
        // El mail DEBE estar en el idioma del sitio. Cross-check obligatorio:
        // si lead.language="en" pero el TLD/GEO sugiere otro idioma → re-detectar.
        // Mail en idioma equivocado quema el dominio para siempre.
        let _pageContent = null;
        // Hint barato basado en TLD + GEO. Si dice "es/pt/it/ar" pero
        // lead.language dice "en", hay desacuerdo → forzamos re-detect.
        const _hintDet = await detectLanguageRobust({ geo: leadGeo, domain }, { allowClaudeArbiter: false });
        const _hintDisagrees = _hintDet.lang !== "en"
          && _hintDet.lang !== leadLanguage
          && SUPPORTED_AGENT_LANGS.has(_hintDet.lang);
        const needLangFetch = !leadLanguage
          || !SUPPORTED_AGENT_LANGS.has(leadLanguage)
          || _hintDisagrees;
        if (needLangFetch) {
          _pageContent = await fetchPageContent(domain).catch(() => null);
          if (_pageContent) {
            const det = await detectLanguageRobust({
              htmlLang:   _pageContent.htmlLang,
              ogLocale:   _pageContent.ogLocale,
              hreflang:   _pageContent.hreflang,
              jsonLdLang: _pageContent.jsonLdLang,
              pathLang:   _pageContent.pathLang,
              textSample: _pageContent.textSample,
              geo: leadGeo,
              domain,
            }, { token });
            leadLanguage = det.lang;
            log(`  🌐 ${domain}: lang=${det.lang} (${det.source}/${det.confidence}) [${det.reasons?.join(",")||""}]`);
            // Persistir para futuros runs
            fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
              method: "PATCH",
              headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
              body: JSON.stringify({ language: leadLanguage }),
            }).catch(() => {});
          } else {
            // No pudimos fetchear página → heurística GEO/TLD primero. Si la
            // confidence es baja (caso típico: .com con GEO ambiguo), pedimos a
            // Claude que adivine por domain+geo. Antes el fallback iba directo a
            // "en" y quemábamos sitios LATAM/europeos. Costo: ~$0.0005/call.
            const det = await detectLanguageRobust({ geo: leadGeo, domain }, { allowClaudeArbiter: false });
            leadLanguage = det.lang;
            if (token && (det.confidence === "low" || det.lang === "en") && !leadGeo) {
              const guessed = await _claudeLangByContext(token, domain, leadGeo);
              if (guessed && guessed !== leadLanguage) {
                log(`  🌐 ${domain}: Claude override ${leadLanguage} → ${guessed} (sin html)`);
                leadLanguage = guessed;
              }
            }
            log(`  🌐 ${domain}: lang=${leadLanguage} (${det.source}/${det.confidence}, sin html)`);
          }
        }
        // Asegurar que llegue siempre algo soportado
        if (!SUPPORTED_AGENT_LANGS.has(leadLanguage)) leadLanguage = "en";
        lead.language = leadLanguage;

        // ── ENRICHMENT ON-THE-FLY (igual que el botón Data del MB humano) ──
        // Si falta data, intentamos traerla AHORA antes de descartar el lead.
        // Cache 90d (traffic) + 7d (Apollo) → la mayoría son hits gratis.

        // 1a. Si NO hay traffic, fetchear getTrafficData
        if (!leadTraffic || leadTraffic <= 0) {
          if (cfg.rapidapi_key) {
            try {
              const td = await getTrafficData(domain, cfg.rapidapi_key);
              if (td?.visits > 0) {
                leadTraffic = td.visits;
                if (!leadGeo && td.topCountry) leadGeo = td.topCountry;
                // Persistir para futuros runs
                await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
                  method: "PATCH",
                  headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
                  body: JSON.stringify({ traffic: leadTraffic, geo: leadGeo || undefined }),
                });
                log(`  🔄 ${domain}: traffic on-the-fly → ${leadTraffic} (${leadGeo})`);
              }
            } catch (e) { log(`  ⚠️ on-the-fly traffic ${domain}: ${e.message}`); }
          }
        }

        // 1b. Si NO hay email DECENTE (rankScore >= 50), dispara enrichment AGRESIVO:
        //     - findBestApolloEmail (free verified, o unlock 1 credit si traffic ≥ 500K)
        //     - scrapeEmailsForDomain como fallback (gratis)
        // Audit fix 2026-05-13: antes "hasGoodEmail" solo chequeaba garbage filters,
        // entonces "contato@filmelier.com" (no-garbage pero generic role) era
        // considerado bueno → Apollo NO se llamaba → agent mandaba al rol genérico.
        // Ahora chequeamos rankScore real: si el mejor < 50 (no commercial-grade)
        // → fuerza Apollo lookup para tratar de conseguir un personal email.
        emails = emails.filter(e => e && /\@/.test(e) && !GARBAGE_LOCAL.test(e) && !GARBAGE_DOMAIN_PATTERN.test(e));
        const currentBestScore = emails.length > 0
          ? Math.max(...emails.map(e => rankEmail(e, domain, lead.category)))
          : -1;
        const hasGoodEmail = currentBestScore >= 50;
        if (!hasGoodEmail) {
          try {
            // PASO 1: Scraping primero (gratis). Si encuentra email decente
            // (rank score >= 50), saltar Apollo unlock para ahorrar credits.
            const scraped = await scrapeEmailsForDomain(domain).catch(() => []);
            const scrapedScores = scraped.map(e => rankEmail(e, domain, lead.category));
            const bestScraped = Math.max(...scrapedScores, -100);
            const skipApollo = bestScraped >= 50; // hay email scraped commercial-grade

            // PASO 2: Apollo solo si NO encontramos email bueno scraping
            let apolloRes = null;
            if (!skipApollo && cfg.apollo_api_key) {
              apolloRes = await findBestApolloEmail(domain, cfg.apollo_api_key, token, {
                traffic: leadTraffic, allowUnlock: true,
              });
            }
            const apolloEmail = apolloRes?.email ? [apolloRes.email] : [];
            const merged = [...new Set([...apolloEmail, ...emails, ...scraped])];
            const validated = await validateEmailsBatch(merged);
            if (validated.length > emails.length) {
              emails = validated;
              const patch = { emails: validated };
              if (apolloRes?.contact_name && !lead.contact_name) patch.contact_name = apolloRes.contact_name;
              await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
                method: "PATCH",
                headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
                body: JSON.stringify(patch),
              });
              log(`  🔄 ${domain}: enrichment → ${validated.length} emails (scraped:${scraped.length}${skipApollo?" — Apollo SKIPPED (scraped suficiente)":`, apollo:${apolloRes?.source||"none"}`})`);
            }
          } catch (e) { log(`  ⚠️ on-the-fly enrich ${domain}: ${e.message}`); }
        }

        // Re-pickear el MEJOR email después del enrichment.
        // Política user 2026-05-19: ranking dinámico por (mb, source) basado en
        // open_rate * (1 - bounce_rate) observado en últimos 30d. Con sample
        // chico o ε-greedy 10%, fallback al hardcoded apollo > informer > scrape.
        // Threshold: solo score >= 0 (positivo); negativo = no mandar.
        const SOURCE_RANK = await getDynamicSourceRank(token, userEmail);
        const _sourcesMap = lead.email_sources || {};
        // Filtra bounced ANTES de rankear: si el #1 ya rebotó, no perdemos el
        // turno descubriéndolo en la validación final.
        const _bouncedSet = _bouncedCache.set || new Set();
        const _emailsClean = emails.filter(e => !_bouncedSet.has(e.toLowerCase()));
        if (_emailsClean.length < emails.length) {
          log(`  🚫 ${domain}: skip ${emails.length - _emailsClean.length} bounced de ${emails.length}`);
        }
        const _rankedAll = _emailsClean.map(e => ({
          email:  e,
          source: _sourcesMap[e.toLowerCase()] || "",
          score:  rankEmail(e, domain, lead.category),
        }));
        const ranked = _rankedAll
          .filter(x => x.score >= 0)
          .sort((a, b) => {
            const sa = SOURCE_RANK[a.source] || 0;
            const sb = SOURCE_RANK[b.source] || 0;
            if (sa !== sb) return sb - sa;     // mejor source primero
            return b.score - a.score;          // dentro del mismo source, mejor rank
          });
        let email = ranked[0]?.email;
        const pickedSource = ranked[0]?.source || "";   // attribution para toolbar_source_performance
        // Log diagnóstico — ver qué pasó con cada candidate
        if (emails.length > 0) {
          const summary = _rankedAll.slice(0, 5).map(x => `${x.email}=${x.score}`).join(", ");
          log(`  🔢 ${domain}: ${emails.length} emails → rankings: ${summary}`);
        } else {
          log(`  ⚠️ ${domain}: emails array vacío después del enrichment`);
        }

        // ── 2do pass Claude para emails ambiguos ──
        // Si el top score < 50 (no claramente verde) Y hay ≥2 candidatos,
        // pedimos a Claude que elija el más probable decision-maker B2B.
        // Costo: ~$0.005 por pick. Toggle vía agent_claude_email_pick=true.
        if (cfg.agent_claude_email_pick === "true" && ranked.length >= 2 && ranked[0].score < 50) {
          try {
            const top = ranked.slice(0, 5).map(r => r.email);
            const picked = await claudePickBestEmail(token, {
              domain, category: lead.category || "", emails: top,
            });
            if (picked && top.includes(picked)) {
              email = picked;
              log(`  🤖 ${domain}: Claude pickeó ${picked} (de ambiguos: ${top.join(",")})`);
            }
          } catch (e) { log(`  ⚠️ Claude pick ${domain}: ${e.message}`); }
        } else if (email && ranked.length > 1) {
          log(`  ✉️ ${domain}: pickeado ${email} (score=${ranked[0].score}) de ${ranked.length} candidatos`);
        }
        if (!email) {
          log(`  ⏭ ${domain}: SKIP — no_email_after_enrichment (emails encontrados: ${emails.length})`);
          await logAgentAction(token, userEmail, {
            domain, action: "skipped", reason: "no_email_after_enrichment",
            details: { traffic: leadTraffic, geo: leadGeo, emails_count: emails.length },
          });
          // Contar fails consecutivos hoy para este dominio — después de 3 → FREEZE
          // (sacar del pool de pending para que el agente no lo re-pickee infinito)
          try {
            const cutoffTodaySpain = new Date(new Date().toLocaleString("en-US", { timeZone: "Europe/Madrid" }));
            cutoffTodaySpain.setHours(0, 0, 0, 0);
            const cntRes = await fetch(
              `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=eq.skipped&reason=eq.no_email_after_enrichment&domain=eq.${encodeURIComponent(domain)}&created_at=gte.${cutoffTodaySpain.toISOString()}&select=id`,
              { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
            );
            const range = cntRes.headers.get("content-range") || "";
            const failsToday = parseInt(range.match(/\/(\d+)$/)?.[1] || "0", 10);
            if (failsToday >= 3) {
              // Mark lead as 'frozen' so agent query (status=pending) lo excluye.
              // Y agregalo a toolbar_frozen_leads para retry en 15d.
              await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
                method: "PATCH",
                headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
                body: JSON.stringify({ status: "frozen" }),
              }).catch(() => {});
              await fetch(`${SUPABASE_URL}/rest/v1/toolbar_frozen_leads`, {
                method: "POST",
                headers: {
                  "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
                  "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                body: JSON.stringify({
                  domain, frozen_until: new Date(Date.now() + 15 * 86400_000).toISOString(),
                  attempt_count: 1, last_error: "no_email_3_attempts",
                  source: "agent", uploaded_by: userEmail,
                  updated_at: new Date().toISOString(),
                }),
              }).catch(() => {});
              log(`  🧊 ${domain}: 3 fails sin email → FREEZE 15d`);
            }
          } catch {}
          continue;
        }

        // Floor 350K: si después del enrichment descubrimos que el lead tiene
        // traffic CONOCIDO menor a 350K → no vale la pena procesarlo, lo BORRAMOS
        // del review_queue para que no acumule basura. Solo aplica a leads con
        // traffic > 0 (los que tienen 0/null seguirán esperando enrichment).
        if (leadTraffic > 0 && leadTraffic < REVIEW_QUEUE_MIN_TRAFFIC) {
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "DELETE",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` },
          }).catch(() => {});
          await logAgentAction(token, userEmail, {
            domain, action: "skipped", reason: "below_min_traffic_deleted",
            details: { traffic: leadTraffic, min: REVIEW_QUEUE_MIN_TRAFFIC },
          });
          log(`  🗑 ${domain}: traffic ${leadTraffic} < ${REVIEW_QUEUE_MIN_TRAFFIC} → DELETE`);
          continue;
        }

        // NOTA: NO chequeamos Monday acá. Los filtros upstream ya lo cubren.

        // 2. EMAIL SCORE — loopear sobre ranked hasta encontrar non-red.
        // Recupera leads buenos donde el #1 estaba undeliverable pero el #2/#3 sí.
        let emailScore = await scoreEmail(email);
        let pickedRedReason = "";
        if (emailScore.color === "red") {
          pickedRedReason = emailScore.reason;
          // Probar siguientes candidatos del ranking
          let alternativeFound = false;
          for (let i = 1; i < ranked.length; i++) {
            const altEmail = ranked[i].email;
            const altScore = await scoreEmail(altEmail);
            if (altScore.color !== "red") {
              log(`  🔁 ${domain}: top ${email} fue red (${pickedRedReason}), fallback a #${i+1} ${altEmail}`);
              email = altEmail;
              emailScore = altScore;
              alternativeFound = true;
              break;
            }
          }
          if (!alternativeFound) {
            log(`  ⏭ ${domain}: SKIP — todos los ${ranked.length} emails red (top: ${pickedRedReason})`);
            await logAgentAction(token, userEmail, {
              domain, action: "skipped",
              reason: `email_red_${pickedRedReason}`,
              details: { email, traffic: leadTraffic, ranked_count: ranked.length },
            });
            continue;
          }
        }
        log(`  📧 ${domain}: email_score=${emailScore.color} → ${email}`);

        // 2. Decidir source: 80% template, 20% Claude (configurable via agent_claude_percent)
        const claudePercent = parseInt(cfg.agent_claude_percent || "20", 10);
        const source = pickPitchSource(claudePercent);
        log(`  🎲 ${domain}: source=${source} (claudePct=${claudePercent}) lang=${lead.language}`);
        let pitch;
        if (source === "claude") {
          // Variedad estilística — A/B test futuro
          pitch = await generatePitchAgent(token, {
            domain, traffic: leadTraffic, geo: leadGeo, language: lead.language || "en",
            category: lead.category, contactName: lead.contact_name,
            adNetworks: lead.ad_networks,
            userEmail, // para RAG retrieval — feedback del propio MB
          });
          // Self-check anti-alucinación solo cuando viene de Claude (templates baked clean).
          // Pasamos undefined para adsTxtExists (= unknown) y subjects para validar largo.
          const check = await selfCheckPitch(token, pitch.body, {
            domain, adsTxtExists: undefined, adNetworks: lead.ad_networks,
            subjects: pitch.subjects,
          });
          if (!check.ok) {
            await logAgentAction(token, userEmail, {
              domain, action: "skipped", reason: `quality_${check.reason}`,
              pitch_subject: pitch.subjects?.[0],
              details: { source, pitch_body_preview: pitch.body.substring(0, 200) },
            });
            continue;
          }
        } else {
          // Template (80% de los casos) — pool combinado baked + DB drafts.
          // Selección ponderada por open_rate de los últimos 30 días
          // (templates con más opens reciben más probabilidad).
          const senderName = getSenderName(userEmail);
          let templateId = null;
          try {
            const picked = await pickAnyTemplate(token, userEmail, lead.language);
            if (picked.template) {
              templateId = picked.templateId;
              pitch = fillTemplate(picked.template, {
                domain, geo: leadGeo, traffic: leadTraffic, senderName,
              });
            }
          } catch (e) { log(`  ⚠️ pickAnyTemplate ${domain}: ${e.message}`); }
          // Fallback duro si todo falla — no debería pasar pero por las dudas
          if (!pitch) {
            const tpl = pickRandomTemplate(lead.language);
            templateId = `baked_${(lead.language || "en").toLowerCase().slice(0,2)}_fallback`;
            pitch = fillTemplate(tpl, {
              domain, geo: leadGeo, traffic: leadTraffic, senderName,
            });
          }
          // Persistir templateId para que el send loggee qué se usó
          pitch._templateId = templateId;
        }

        // ── ORDEN: Gmail PRIMERO, Monday DESPUÉS (igual que MB humano) ──
        // Si el send falla, NO ensuciamos Monday con items "Mail No Enviado".

        // 3. Pre-check Monday config (early exit si falta API key)
        const mondayUserId = (cfg[`monday_user_id_${userEmail.toLowerCase()}`] || "").trim();
        const mondayApiKey = (cfg[`monday_api_key_${userEmail.toLowerCase()}`] || monday_api_key_default).trim();
        if (!mondayApiKey) {
          await logAgentAction(token, userEmail, { domain, action: "failed", reason: "no_monday_api_key" });
          continue;
        }

        // 4. Send Gmail PRIMERO — si falla, no toca Monday
        const subject = pitch.subjects[0];

        // BLOCKLIST GUARD (defense-in-depth) — admin pudo agregar el dominio
        // entre intake y send. Recheck antes de mandar.
        const _blockGuard = await isDomainBlockedFull(domain, token).catch(() => null);
        if (_blockGuard) {
          log(`  ⊘ ${domain}: ABORT send — admin blocklist (${_blockGuard})`);
          await logAgentAction(token, userEmail, { domain, action: "skipped", reason: `blocklist:${_blockGuard}` });
          // Marcar review_queue como rejected para no re-considerar
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "PATCH",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({ status: "rejected", validated_by: `admin_blocklist`, validated_at: new Date().toISOString() }),
          }).catch(() => {});
          continue;
        }

        // reservedId ya está declarado al inicio del loop (scope del for) para que catch lo vea
        // ── SENDTRACK 30d GUARD (último filtro antes del send) ──
        // Aunque upstream filtre, defense-in-depth: chequeamos si el dominio
        // recibió mail en los últimos 30 días por CUALQUIER MB (humano o agente).
        // Cero costo (1 query Supabase con índice). Evita re-contactar.
        try {
          const cutoff = new Date(Date.now() - 30 * 86400_000).toISOString().split("T")[0];
          const stRes = await fetch(
            `${SUPABASE_URL}/rest/v1/toolbar_sendtrack?domain=eq.${encodeURIComponent(domain)}&send_date=gte.${cutoff}&select=send_date,email&limit=1`,
            { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
          );
          if (stRes.ok) {
            const sentRows = await stRes.json();
            if (Array.isArray(sentRows) && sentRows.length > 0) {
              log(`  ⏭ ${domain}: skip — ya contactado ${sentRows[0].send_date} (sendtrack 30d guard)`);
              await logAgentAction(token, userEmail, {
                domain, action: "skipped", reason: "sendtrack_30d",
                details: { last_send: sentRows[0].send_date, last_email: sentRows[0].email },
              });
              continue;
            }
          }
        } catch (e) { log(`  ⚠️ sendtrack guard ${domain}: ${e.message}`); }

        // RESERVA en counter ANTES del send. Si Railway crashea entre send y log,
        // el counter ya tiene el slot reservado → próximo arranque no over-sends.
        // Usamos action='reserved' (no 'sent') para que getAgentDailyCount sume
        // SOLO los confirmados. Tras send OK, hacemos PATCH a 'sent'. Si falla,
        // el catch loggea 'failed' y el reserved queda como audit trail.
        const reserveRes = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions`, {
          method: "POST",
          headers: {
            "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
            "Content-Type": "application/json", "Prefer": "return=representation",
          },
          body: JSON.stringify({
            user_email: userEmail, domain, action: "reserved",
            pitch_subject: subject,
            details: { email, source: pickedSource, traffic: leadTraffic, geo: leadGeo, language: lead.language },
          }),
        }).catch(() => null);
        reservedId = (await reserveRes?.json().catch(() => null))?.[0]?.id || null;

        // Defense-in-depth: validar que el email pertenece a la misma MARCA del lead.
        // Algoritmo brand-match (2026-05-15): strip TLD para comparar nombre comercial.
        // Casos resueltos:
        //   eltribuno.com ↔ eltribuno.com.ar    → match (mismo brand "eltribuno")
        //   midilibre.fr  ↔ midilibre.com        → match
        //   tudogostoso.com.br ↔ webedia-group.com → no match (parent corp distinct)
        //   lafm.com.co ↔ rcnradio.com.co        → no match (parent corp distinct)
        const _recipientDom = (email.split("@")[1] || "").toLowerCase();
        const _leadDom      = domain.toLowerCase().replace(/^www\./, "");
        const _isWebmail    = /^(gmail|hotmail|outlook|live|yahoo|aol|icloud|protonmail|gmx|me)\.com$/.test(_recipientDom);
        // Extraer brand: dominio sin TLD ni public suffix
        const _stripTld = (d) => {
          // Sacar public suffixes comunes (.com.ar, .com.br, .co.uk, .com.mx, etc.)
          const parts = d.split(".");
          if (parts.length <= 2) return parts[0]; // foo.com → foo
          // Public suffixes de 2 niveles
          const last2 = parts.slice(-2).join(".");
          const TWO_LEVEL = new Set([
            "com.ar","com.br","com.mx","com.co","com.pe","com.cl","com.ve","com.ec","com.uy",
            "com.py","com.bo","com.gt","com.sv","com.hn","com.ni","com.cr","com.pa","com.do",
            "co.uk","co.za","co.nz","co.jp","co.kr","co.id","co.in",
            "com.tr","com.tw","com.hk","com.sg","com.my","com.au","com.eg","com.sa","com.ng",
            "org.uk","ac.uk","gov.uk","com.pt","org.br","gov.br","edu.br",
          ]);
          if (TWO_LEVEL.has(last2)) return parts[parts.length - 3] || "";
          return parts[parts.length - 2] || ""; // foo.bar.com → bar
        };
        const _recipientBrand = _stripTld(_recipientDom);
        const _leadBrand      = _stripTld(_leadDom);
        const _domMatches   = _recipientDom === _leadDom
                            || _recipientDom.endsWith("." + _leadDom)
                            || _leadDom.endsWith("." + _recipientDom)
                            || (_recipientBrand && _recipientBrand === _leadBrand && _recipientBrand.length >= 4)
                            || _isWebmail;
        if (!_domMatches) {
          log(`🚫 ${domain}: ABORT send — recipient ${email} brand mismatch (${_recipientBrand} ≠ ${_leadBrand}).`);
          if (reservedId) {
            await fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions?id=eq.${reservedId}`, {
              method: "PATCH",
              headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
              body: JSON.stringify({ action: "skipped", reason: "domain_mismatch_recipient" }),
            }).catch(() => {});
          }
          // FIX LOOP INFINITO 2026-05-15: marcar lead como rejected para que NO se
          // vuelva a pickear en próximos ciclos. Antes el agente intentaba lo mismo
          // cada minuto → 50 skips repetidos por hora del mismo lead.
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "PATCH",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({ status: "rejected", validated_by: "domain_mismatch_loop_guard", validated_at: new Date().toISOString() }),
          }).catch(() => {});
          continue; // próximo lead
        }

        await sendGmailServer(token, userEmail, { to: email, subject, body: pitch.body, agentActionId: reservedId });

        // PATCH reserved → sent ahora que confirmamos el envío
        // Loggeamos también template_id (baked_<lang>_<idx> o db_<id>) para
        // poder calcular open rates por template y ponderar futuros picks.
        if (reservedId) {
          const _patchBody = pitch._templateId
            ? { action: "sent", template_id: pitch._templateId }
            : { action: "sent" };
          fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions?id=eq.${reservedId}`, {
            method: "PATCH",
            headers: {
              "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
              "Content-Type": "application/json", "Prefer": "return=minimal",
            },
            body: JSON.stringify(_patchBody),
          }).catch(() => {});
        }

        // 5. Push to Monday CON estado correcto desde el inicio (Propuesta Vigente T = idx 3)
        let mondayItemId = null;
        try {
          mondayItemId = await pushToMondayServer(mondayApiKey, {
            domain, email, geo: leadGeo, traffic_text: formatTrafficForMonday(leadTraffic),
            phone: lead.contact_phone || "",
            pitch_body: pitch.body, idioma_idx: ({ en:0, es:1, it:2, pt:3, ar:6 })[lead.language] ?? 0,
            monday_user_id: mondayUserId,
            estado_idx: 3, // Propuesta Vigente (T)
          }, AGENT_DEFAULTS.monday_board_id);
        } catch (mondayErr) {
          // Audit P1 fix: Gmail OK + Monday FAIL = lead recibió pitch pero no
          // hay tracking en CRM. Antes solo log; ahora guardamos el payload
          // COMPLETO en details.retry_payload para que admin pueda re-push manual
          // desde un script o el panel admin. Sendtrack + review_queue siguen
          // marcando para no re-enviar.
          log(`🟠 Agent ${userEmail}: Gmail OK pero Monday FAIL ${domain}: ${mondayErr.message} — guardando retry_payload`);
          await logAgentAction(token, userEmail, {
            domain, action: "monday_failed", reason: "monday_push_error",
            pitch_subject: subject,
            details: {
              email, traffic: leadTraffic, geo: leadGeo,
              language: lead.language,
              monday_error: mondayErr.message?.substring(0, 200),
              // Retry payload: todo lo necesario para que admin re-pushee manual
              retry_payload: {
                domain, email, geo: leadGeo, traffic: leadTraffic,
                language: lead.language, contact_name: lead.contact_name || "",
                pitch_subject: subject,
                pitch_body: pitch.body?.substring(0, 5000) || "",
                sent_at: new Date().toISOString(),
                sender_user: userEmail,
              },
            },
          });
        }

        // 6. Track en sendtrack para no re-enviar (siempre, mail YA salió)
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_sendtrack`, {
          method: "POST",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" },
          body: JSON.stringify({ domain, send_date: new Date().toISOString().split("T")[0], email, pitch: pitch.body.substring(0, 1000) }),
        });

        // 7. Marcar el review_queue item como validated_by agent
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
          method: "PATCH",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json" },
          body: JSON.stringify({ status: "validated", validated_by: `agent:${userEmail}`, validated_at: new Date().toISOString() }),
        });

        // 8. Log success completo de Monday (NO duplica action='sent', solo confirma Monday OK)
        if (mondayItemId) {
          await logAgentAction(token, userEmail, {
            domain, action: "monday_ok", reason: "ok",
            pitch_subject: subject,
            monday_item_id: mondayItemId,
            details: {
              email,
              email_score: emailScore.color,
              source,
              traffic: leadTraffic,
              geo: leadGeo,
              language: lead.language,
            },
          });
        }

        log(`🤖 Agent ${userEmail}: SENT to ${email} for ${domain} (subj: "${subject.substring(0,50)}")`);
        processed++;
        // Delay entre envíos para no parecer bot. Bajado de 15-25s → 10-15s
        // para subir throughput diario sin disparar rate limits de Gmail
        // (Gmail tolera ~1 envío/min/cuenta tranquilo).
        await sleep(10000 + Math.random() * 5000);

      } catch (err) {
        // CRITICAL: si el send falló, el slot reservado pre-send queda contado
        // como "consumed" en el cap diario aunque NO se mandó nada. Lo
        // PATCHeamos a 'failed' para que getAgentDailyCount lo descarte.
        if (reservedId) {
          fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions?id=eq.${reservedId}`, {
            method: "PATCH",
            headers: {
              "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
              "Content-Type": "application/json", "Prefer": "return=minimal",
            },
            body: JSON.stringify({ action: "failed_reserved", reason: "send_failed_revert_slot" }),
          }).catch(() => {});
        }
        await logAgentAction(token, userEmail, {
          domain, action: "failed", reason: err.message?.substring(0, 200) || "unknown",
          details: { error: String(err).substring(0, 500) },
        });
        log(`🤖 Agent ${userEmail}: FAILED ${domain}: ${err.message}`);
        // Errores hard (no_refresh_token, monday key) → don't keep trying same cycle
        if (/no_refresh_token|no_monday_api_key|oauth_refresh_failed/.test(err.message || "")) break;
      }
    }
  }
}

// ── Loop principal ────────────────────────────────────────────

// Timestamp del arranque del proceso — usado para detectar force-restart triggers
// que llegaron DESPUÉS del start (vs ya consumidos en runs previos).
const _processStartedAt = Date.now();

// Capturar unhandled rejections — sin esto, una promesa async sin .catch()
// crashea el process node entero (Node 15+ behavior). Loguear y seguir.
process.on("unhandledRejection", (reason, p) => {
  try {
    log(`⚠️ UnhandledRejection: ${reason?.message || reason}`);
    if (reason?.stack) log(reason.stack.split("\n").slice(0, 6).join("\n"));
    console.error("[unhandledRejection]", reason);
  } catch {}
});
process.on("uncaughtException", (err) => {
  try {
    log(`⚠️ UncaughtException: ${err?.message || err}`);
    if (err?.stack) log(err.stack.split("\n").slice(0, 6).join("\n"));
    console.error("[uncaughtException]", err);
  } catch {}
});
// SIGTERM (Railway restart o redeploy) — loggear claro para distinguir de crash real
process.on("SIGTERM", () => {
  log("🛑 SIGTERM recibido — Railway está reiniciando el container. Worker exit gracefully.");
  setTimeout(() => process.exit(0), 1500);
});
process.on("SIGINT", () => {
  log("🛑 SIGINT recibido — exit gracefully.");
  setTimeout(() => process.exit(0), 500);
});

// ════════════════════════════════════════════════════════════════
// SOURCE PERFORMANCE — dynamic ranking de email sources
// ════════════════════════════════════════════════════════════════
// Aggregate rolling 30d de (mb_email, source) → open_rate, bounce_rate, score.
// El job aggregateSourcePerformance() corre diario y upserta a toolbar_source_performance.
// El picker getDynamicSourceRank(mbEmail) reemplaza el SOURCE_RANK hardcoded en runtime,
// con cache 1h en memoria, fallback a default si sample chico, y ε-greedy 10%.
// ════════════════════════════════════════════════════════════════

const SOURCE_RANK_DEFAULT = { manual: 5, apollo: 4, informer: 3, scrape: 2, generic: 1, "": 0 };
const SOURCE_PERF_WINDOW_DAYS = 30;
const SOURCE_PERF_MIN_SENT = 50;       // sample mínimo por (mb, source) para usar dinámico
const SOURCE_PERF_EPSILON = 0.10;      // 10% de las decisiones usan ranking default (exploración)
const SOURCE_PERF_CACHE_TTL = 60 * 60 * 1000;  // 1h
const _sourceRankCache = new Map();    // mb_email → { rank: {...}, ts: number }

// Convierte una tabla {source: score} a un map {source: rank} estable.
// El source con mayor score queda con rank más alto (igual semántica que SOURCE_RANK_DEFAULT).
function _scoresToRank(scoresBySource) {
  const sorted = Object.entries(scoresBySource).sort((a, b) => b[1] - a[1]);
  const rank = { "": 0 };
  // El primero queda con rank = N, el último con rank = 1.
  sorted.forEach(([src], i) => { rank[src] = sorted.length - i; });
  return rank;
}

// Lee de toolbar_source_performance la perf de un MB. Si no hay data suficiente
// para alguna source (sent < threshold), esa source NO entra en el cómputo y
// queda con su rank default. Devuelve un map {source: rank} listo para usar.
async function _fetchDynamicSourceRank(token, mbEmail) {
  try {
    const mb = (mbEmail || "_global").toLowerCase();
    // Buscar primero la fila del MB; si no tiene data suficiente, fallback _global.
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_source_performance?mb_email=in.(${encodeURIComponent(mb)},_global)&window_days=eq.${SOURCE_PERF_WINDOW_DAYS}&select=mb_email,source,sent,score`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json().catch(() => []);
    if (!Array.isArray(rows) || rows.length === 0) return null;
    // Preferimos rows del MB; las _global son fallback por source.
    const scores = {};
    for (const src of Object.keys(SOURCE_RANK_DEFAULT)) {
      if (src === "") continue;
      const mbRow     = rows.find(r => r.mb_email === mb && r.source === src);
      const globalRow = rows.find(r => r.mb_email === "_global" && r.source === src);
      const chosen    = (mbRow && mbRow.sent >= SOURCE_PERF_MIN_SENT) ? mbRow
                      : (globalRow && globalRow.sent >= SOURCE_PERF_MIN_SENT) ? globalRow
                      : null;
      if (chosen) scores[src] = parseFloat(chosen.score) || 0;
    }
    // Si menos de 2 sources tienen data → no podemos rankear → fallback default.
    if (Object.keys(scores).length < 2) return null;
    // Para las sources sin data observada, las dejamos con rank default así no las matamos.
    // Estrategia: primero rankeamos las observed por score, después appendamos las no-observed
    // por debajo en orden default. Esto preserva exploration de sources nuevas.
    const observedRank = _scoresToRank(scores);
    const merged = { ...observedRank };
    Object.entries(SOURCE_RANK_DEFAULT).forEach(([src, defRank]) => {
      if (!(src in merged)) merged[src] = 0.5; // por debajo de cualquier observed (mín 1) pero arriba de "" (0)
    });
    return merged;
  } catch { return null; }
}

// API pública: devuelve un map {source: rank} para usar en el picker.
// ε-greedy: 10% de los calls devuelven el ranking default aunque haya data
// (exploración — evita lock-in si una source empeora con el tiempo).
async function getDynamicSourceRank(token, mbEmail) {
  // ε-greedy: con prob ε, ignorar lo aprendido.
  if (Math.random() < SOURCE_PERF_EPSILON) return SOURCE_RANK_DEFAULT;

  const key = (mbEmail || "_global").toLowerCase();
  const cached = _sourceRankCache.get(key);
  if (cached && (Date.now() - cached.ts) < SOURCE_PERF_CACHE_TTL) return cached.rank;

  const dynamic = await _fetchDynamicSourceRank(token, key);
  const rank = dynamic || SOURCE_RANK_DEFAULT;
  _sourceRankCache.set(key, { rank, ts: Date.now() });
  return rank;
}

// Job diario: agrega toolbar_agent_actions ⨯ toolbar_email_opens ⨯ toolbar_bounce_retries
// y upserta a toolbar_source_performance. Se llama desde el main loop con guard
// "1× por día" usando un flag en toolbar_config (last_source_perf_run).
async function aggregateSourcePerformance(token) {
  try {
    const since = new Date(Date.now() - SOURCE_PERF_WINDOW_DAYS * 86400_000).toISOString();
    log(`📊 Source perf: aggregating window ${SOURCE_PERF_WINDOW_DAYS}d (since ${since.slice(0, 10)})`);

    // 1. Pull todos los sends del período con source poblado en details.
    const sendsRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=in.(sent,re_sent,bounce_retry_sent)&created_at=gte.${since}&select=id,user_email,email_to,details,created_at&limit=10000`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!sendsRes.ok) { log(`  ⚠️ sends fetch ${sendsRes.status}`); return; }
    const sends = await sendsRes.json();
    if (!Array.isArray(sends) || sends.length === 0) { log("  (sin sends en ventana)"); return; }

    // 2. Bulk pull de opens — todas las filas que matchean los action_ids.
    const sendIds = sends.map(s => s.id).filter(Boolean);
    const opensByActionId = new Set();
    // Chunk por 200 para no inflar la URL
    for (let i = 0; i < sendIds.length; i += 200) {
      const chunk = sendIds.slice(i, i + 200);
      const oRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_email_opens?agent_action_id=in.(${chunk.join(",")})&select=agent_action_id`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      if (oRes.ok) {
        const opens = await oRes.json().catch(() => []);
        opens.forEach(o => opensByActionId.add(o.agent_action_id));
      }
    }

    // 3. Bulk pull de bounces — match por email_to (los rebotados son target del send).
    const bouncedEmails = new Set();
    const bRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_bounce_retries?created_at=gte.${since}&select=original_email`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (bRes.ok) {
      const brows = await bRes.json().catch(() => []);
      brows.forEach(b => { if (b.original_email) bouncedEmails.add(b.original_email.toLowerCase()); });
    }

    // 4. Agregar por (mb, source).
    const agg = new Map(); // key "mb|source" → {sent, opens, bounces}
    const bump = (mb, src, field) => {
      const k = `${mb}|${src}`;
      if (!agg.has(k)) agg.set(k, { sent: 0, opens: 0, bounces: 0, mb, src });
      agg.get(k)[field]++;
    };
    for (const s of sends) {
      const mb  = (s.user_email || "").toLowerCase();
      const src = (s.details?.source || "").toLowerCase();
      if (!mb || !src) continue;     // sin atribución → skip
      const isOpen   = opensByActionId.has(s.id);
      const isBounce = s.email_to ? bouncedEmails.has(s.email_to.toLowerCase()) : false;
      bump(mb, src, "sent");
      bump("_global", src, "sent");
      if (isOpen)   { bump(mb, src, "opens");   bump("_global", src, "opens"); }
      if (isBounce) { bump(mb, src, "bounces"); bump("_global", src, "bounces"); }
    }

    // 5. Calcular rates + score y upsert.
    const rows = [];
    for (const { mb, src, sent, opens, bounces } of agg.values()) {
      const openRate   = sent > 0 ? opens / sent   : 0;
      const bounceRate = sent > 0 ? bounces / sent : 0;
      const score      = openRate * (1 - bounceRate);
      rows.push({
        mb_email: mb, source: src, window_days: SOURCE_PERF_WINDOW_DAYS,
        sent, opens, bounces,
        open_rate: openRate.toFixed(4),
        bounce_rate: bounceRate.toFixed(4),
        score: score.toFixed(4),
        computed_at: new Date().toISOString(),
      });
    }
    if (rows.length === 0) { log("  (nada para upsertar)"); return; }

    const upRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_source_performance?on_conflict=mb_email,source,window_days`,
      {
        method: "POST",
        headers: {
          "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
          "Content-Type": "application/json",
          "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        body: JSON.stringify(rows),
      }
    );
    if (!upRes.ok) {
      const txt = await upRes.text().catch(() => "");
      log(`  ⚠️ upsert ${upRes.status}: ${txt.slice(0, 200)}`);
      return;
    }

    // 6. Invalidar cache para que el próximo getDynamicSourceRank lea fresh.
    _sourceRankCache.clear();
    log(`  ✅ source perf upsert OK: ${rows.length} filas (${sends.length} sends procesados)`);
  } catch (e) {
    log(`⚠️ aggregateSourcePerformance: ${e.message}`);
  }
}

// Guard: corre 1× por día (hora Madrid). Persiste last-run en toolbar_config.
async function maybeRunSourcePerformanceAggregate(token) {
  try {
    const cfg = await getConfig(token);
    const enabled = String(cfg.agent_source_perf_enabled ?? "true").toLowerCase() !== "false";
    if (!enabled) return;
    const todayMadrid = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
    const last = cfg.last_source_perf_run || "";
    if (last === todayMadrid) return;
    await aggregateSourcePerformance(token);
    await setConfigValue(token, "last_source_perf_run", todayMadrid).catch(() => {});
  } catch (e) { log(`⚠️ maybeRunSourcePerformanceAggregate: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════
// NOTIFICATIONS — alert scanners para el bell del popup
// ════════════════════════════════════════════════════════════════
// Helper genérico de inserción + 6 scanners que se invocan desde el main loop
// con guards de frecuencia (low_prosp: L y J 18hs, bounce/open: lunes 09hs).
// ════════════════════════════════════════════════════════════════

const LOW_PROSPECTING_THRESHOLD = 30;        // hardcoded: regla de negocio
const BOUNCE_HIGH_FLOOR = 0.10;              // no alertar si bounce < 10% incluso si está sobre el promedio
const OPEN_LOW_GAP_RATIO = 0.6;              // alertar si MB.open < team_avg * 0.6

async function createNotificationWorker(token, payload) {
  // payload: { mb_email, type, severity, title, body, metadata, dedup_key }
  try {
    const res = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_notifications`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({
        mb_email:  (payload.mb_email || "_admin").toLowerCase(),
        type:      payload.type,
        severity:  payload.severity || "info",
        title:     payload.title,
        body:      payload.body || null,
        metadata:  payload.metadata || {},
        dedup_key: payload.dedup_key || null,
      }),
    });
    return res.ok;
  } catch { return false; }
}

// Devuelve los últimos N días hábiles (excluye sáb/dom) como array de YYYY-MM-DD.
function _lastBusinessDays(n) {
  const days = [];
  const d = new Date();
  while (days.length < n) {
    const dow = d.getDay(); // 0=Sun, 6=Sat
    if (dow !== 0 && dow !== 6) {
      days.push(d.toISOString().split("T")[0]);
    }
    d.setDate(d.getDate() - 1);
  }
  return days;
}

// Scanner 1: low prospecting (L/J 18hs Madrid sobre últimos 5 días hábiles)
async function scanLowProspecting(token) {
  const now = new Date();
  const madridDow = parseInt(now.toLocaleString("en-US", { timeZone: "Europe/Madrid", weekday: "long" }).match(/(\d+)/)?.[1] || "0", 10);
  const madridHour = parseInt(now.toLocaleString("en-US", { timeZone: "Europe/Madrid", hour: "2-digit", hour12: false }).replace(/\D/g, "").slice(0, 2), 10);
  // Real day-of-week (0=Sun..6=Sat) sin parsing engendro:
  const madridStr = now.toLocaleString("en-US", { timeZone: "Europe/Madrid", weekday: "short" });
  const isMonOrThu = madridStr.startsWith("Mon") || madridStr.startsWith("Thu");
  if (!isMonOrThu || madridHour < 18) return;

  const businessDays = _lastBusinessDays(5);
  const since = businessDays[businessDays.length - 1] + "T00:00:00Z";
  const dedupKey = `lowprosp-${businessDays[0]}`;

  // Pull TEAM_EMAILS from config — fallback hardcoded
  const teamEmails = ["mgargiulo@adeqmedia.com", "dhorovitz@adeqmedia.com", "sales@adeqmedia.com"];
  for (const mb of teamEmails) {
    try {
      const res = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(mb)}&action=in.(sent,re_sent,bounce_retry_sent)&created_at=gte.${since}&select=id,created_at&limit=1000`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      if (!res.ok) continue;
      const rows = await res.json();
      // Group by day
      const byDay = {};
      rows.forEach(r => { const d = r.created_at.slice(0, 10); byDay[d] = (byDay[d] || 0) + 1; });
      // Calcular promedio sobre días hábiles que efectivamente trabajó (>=1 envío)
      const activeDays = businessDays.filter(d => byDay[d] > 0);
      if (activeDays.length === 0) continue;
      const avgPerDay = activeDays.reduce((s, d) => s + (byDay[d] || 0), 0) / activeDays.length;
      if (avgPerDay < LOW_PROSPECTING_THRESHOLD) {
        await createNotificationWorker(token, {
          mb_email: mb, type: "low_prospecting", severity: "warning",
          title: `Low prospecting: ${avgPerDay.toFixed(1)} emails/day average`,
          body:  `Target is ${LOW_PROSPECTING_THRESHOLD}/day. Last ${activeDays.length} business days: ${activeDays.map(d => byDay[d] || 0).join(" / ")}.`,
          metadata: { avg: avgPerDay, threshold: LOW_PROSPECTING_THRESHOLD, days: activeDays },
          dedup_key: dedupKey,
        });
      }
    } catch (e) { log(`⚠️ scanLowProspecting ${mb}: ${e.message}`); }
  }
}

// Helper: media + std dev simple.
function _meanStd(arr) {
  if (!arr.length) return { mean: 0, std: 0 };
  const mean = arr.reduce((s, x) => s + x, 0) / arr.length;
  const variance = arr.reduce((s, x) => s + (x - mean) ** 2, 0) / arr.length;
  return { mean, std: Math.sqrt(variance) };
}

// Scanner 2 + 3: bounce_high + open_low (semanal, lunes 09hs Madrid).
// Lee toolbar_source_performance (que ya tiene rates por MB) y compara contra
// la distribución del equipo. Auto-calibra: el threshold se mueve con el equipo.
async function scanWeeklyRates(token) {
  const now = new Date();
  const madridStr = now.toLocaleString("en-US", { timeZone: "Europe/Madrid", weekday: "short" });
  const madridHour = parseInt(now.toLocaleString("en-US", { timeZone: "Europe/Madrid", hour: "2-digit", hour12: false }).replace(/\D/g, "").slice(0, 2), 10);
  if (!madridStr.startsWith("Mon") || madridHour < 9) return;

  // Compute ISO week key for dedup
  const weekKey = `${now.getFullYear()}-W${Math.ceil(((now - new Date(now.getFullYear(), 0, 1)) / 86400000 + new Date(now.getFullYear(), 0, 1).getDay() + 1) / 7)}`;

  try {
    // Agregar TOTAL bounce/open rate por MB (sumando todas las sources)
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_source_performance?window_days=eq.30&mb_email=neq._global&select=mb_email,sent,opens,bounces`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    // Sum per MB
    const byMb = {};
    rows.forEach(r => {
      const m = r.mb_email;
      if (!byMb[m]) byMb[m] = { sent: 0, opens: 0, bounces: 0 };
      byMb[m].sent    += r.sent;
      byMb[m].opens   += r.opens;
      byMb[m].bounces += r.bounces;
    });
    const mbStats = Object.entries(byMb)
      .filter(([, v]) => v.sent >= 30)  // mínimo de envíos para tener señal
      .map(([mb, v]) => ({ mb, openRate: v.opens / v.sent, bounceRate: v.bounces / v.sent }));
    if (mbStats.length < 2) return;     // necesitamos al menos 2 MBs para comparar

    const openRates   = mbStats.map(s => s.openRate);
    const bounceRates = mbStats.map(s => s.bounceRate);
    const openStats   = _meanStd(openRates);
    const bounceStats = _meanStd(bounceRates);

    for (const s of mbStats) {
      // Bounce alto: > 1 stddev sobre la media Y sobre floor 10%
      if (s.bounceRate > (bounceStats.mean + bounceStats.std) && s.bounceRate >= BOUNCE_HIGH_FLOOR) {
        await createNotificationWorker(token, {
          mb_email: s.mb, type: "bounce_high", severity: "error",
          title:    `High bounce rate: ${(s.bounceRate * 100).toFixed(1)}%`,
          body:     `Team average is ${(bounceStats.mean * 100).toFixed(1)}%. Check your email sources — too many dead addresses.`,
          metadata: { rate: s.bounceRate, teamMean: bounceStats.mean, teamStd: bounceStats.std },
          dedup_key: `bounce-${weekKey}`,
        });
      }
      // Open bajo: < 1 stddev bajo media Y gap significativo (< 60% del promedio)
      if (s.openRate < (openStats.mean - openStats.std) && s.openRate < (openStats.mean * OPEN_LOW_GAP_RATIO)) {
        await createNotificationWorker(token, {
          mb_email: s.mb, type: "open_low", severity: "warning",
          title:    `Open rate below team: ${(s.openRate * 100).toFixed(1)}%`,
          body:     `Team average is ${(openStats.mean * 100).toFixed(1)}%. Try varying subject lines or warming up Apollo contacts.`,
          metadata: { rate: s.openRate, teamMean: openStats.mean, teamStd: openStats.std },
          dedup_key: `openlow-${weekKey}`,
        });
      }
      // Refuerzo positivo: open > 25% Y > 1 stddev sobre la media
      if (s.openRate >= 0.25 && s.openRate > (openStats.mean + openStats.std)) {
        await createNotificationWorker(token, {
          mb_email: s.mb, type: "positive", severity: "success",
          title:    `🎉 Top open rate this week: ${(s.openRate * 100).toFixed(1)}%`,
          body:     `You're ${((s.openRate / openStats.mean - 1) * 100).toFixed(0)}% above team average. Keep doing what you're doing.`,
          metadata: { rate: s.openRate, teamMean: openStats.mean },
          dedup_key: `positive-${weekKey}`,
        });
      }
    }
  } catch (e) { log(`⚠️ scanWeeklyRates: ${e.message}`); }
}

// Scanner 4: source insight semanal — qué source está rindiendo mejor para el MB.
async function scanSourceInsight(token) {
  const now = new Date();
  const madridStr = now.toLocaleString("en-US", { timeZone: "Europe/Madrid", weekday: "short" });
  const madridHour = parseInt(now.toLocaleString("en-US", { timeZone: "Europe/Madrid", hour: "2-digit", hour12: false }).replace(/\D/g, "").slice(0, 2), 10);
  if (!madridStr.startsWith("Mon") || madridHour < 9) return;

  const weekKey = `${now.getFullYear()}-W${Math.ceil(((now - new Date(now.getFullYear(), 0, 1)) / 86400000 + new Date(now.getFullYear(), 0, 1).getDay() + 1) / 7)}`;

  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_source_performance?window_days=eq.30&mb_email=neq._global&select=mb_email,source,sent,open_rate,bounce_rate,score&order=mb_email.asc,score.desc`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    const byMb = {};
    rows.forEach(r => { if (r.sent >= 20) (byMb[r.mb_email] = byMb[r.mb_email] || []).push(r); });
    for (const [mb, sources] of Object.entries(byMb)) {
      if (sources.length < 2) continue;
      const best = sources[0];
      const worst = sources[sources.length - 1];
      const gap = parseFloat(best.open_rate) - parseFloat(worst.open_rate);
      if (gap < 0.10) continue;   // gap < 10pp → no insight relevante
      await createNotificationWorker(token, {
        mb_email: mb, type: "source_insight", severity: "info",
        title:    `Source insight: ${best.source} is your best lever`,
        body:     `${best.source} open rate ${(parseFloat(best.open_rate) * 100).toFixed(0)}% vs ${worst.source} ${(parseFloat(worst.open_rate) * 100).toFixed(0)}%. Agent auto-prioritizes ${best.source} for you.`,
        metadata: { best: best.source, worst: worst.source, gap },
        dedup_key: `insight-${weekKey}`,
      });
    }
  } catch (e) { log(`⚠️ scanSourceInsight: ${e.message}`); }
}

// Scanner 5: system_failure — agent failed actions concentrados.
// Si un MB tiene > 3 failed actions en la última hora → alerta a él.
// Si el global tiene > 10 failed en la última hora → alerta a admin.
async function scanSystemFailures(token) {
  try {
    const cutoff = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=in.(failed,monday_failed)&created_at=gte.${cutoff}&select=user_email,domain,reason,created_at&limit=200`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) return;
    const byMb = {};
    rows.forEach(r => { const m = (r.user_email || "_admin").toLowerCase(); byMb[m] = (byMb[m] || 0) + 1; });
    const hourKey = new Date().toISOString().slice(0, 13); // hora actual
    for (const [mb, count] of Object.entries(byMb)) {
      if (count > 3 && mb !== "_admin") {
        await createNotificationWorker(token, {
          mb_email: mb, type: "system_failure", severity: "error",
          title:    `${count} failed sends in the last hour`,
          body:     `Most recent: ${rows.find(r => (r.user_email || "").toLowerCase() === mb)?.reason || "unknown"}. Check Gmail token / Monday API key.`,
          metadata: { count, sample: rows.filter(r => (r.user_email || "").toLowerCase() === mb).slice(0, 3) },
          dedup_key: `sysfail-${mb}-${hourKey}`,
        });
      }
    }
    if (rows.length > 10) {
      await createNotificationWorker(token, {
        mb_email: "_admin", type: "system_failure", severity: "error",
        title:    `Global: ${rows.length} failed sends in last hour`,
        body:     `Across ${Object.keys(byMb).length} MBs. Check Railway logs.`,
        metadata: { total: rows.length, byMb },
        dedup_key: `sysfail-global-${hourKey}`,
      });
    }
  } catch (e) { log(`⚠️ scanSystemFailures: ${e.message}`); }
}

// Orchestrator — runs all scanners. Cada uno tiene guard interno de frecuencia.
async function runNotificationScanners(token) {
  await scanLowProspecting(token).catch(e => log(`⚠️ scanLowProsp: ${e.message}`));
  await scanWeeklyRates(token).catch(e => log(`⚠️ scanWeeklyRates: ${e.message}`));
  await scanSourceInsight(token).catch(e => log(`⚠️ scanInsight: ${e.message}`));
  await scanSystemFailures(token).catch(e => log(`⚠️ scanSysFail: ${e.message}`));
}

async function main() {
  log("ADEQ Auto-Prospector v3 iniciado.");
  // Cleanup: resetear items "processing" trabados de un crash anterior.
  // Sin esto, items con status=processing se quedan invisibles para getNextCsvItem.
  try {
    const cleanupRes = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.processing`, {
      method: "PATCH",
      headers: {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${BACKEND_BEARER || ""}`,
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
      },
      body: JSON.stringify({ status: "pending" }),
    });
    if (cleanupRes.ok) log("🧹 Cleanup: items processing → pending (recovery from previous run)");
  } catch (e) {
    log(`⚠️ Cleanup processing items failed: ${e.message}`);
  }

  // ── Cleanup: reserved slots huérfanos (> 5min sin patch a sent/failed) ──
  // Audit P1 fix: si Railway crashea entre el INSERT reserved y el PATCH→sent,
  // el slot queda como 'reserved' permanente y cuenta para el cap diario.
  // Acumula con cada crash → over-count → menos envíos de los permitidos.
  // Acá los marcamos como 'failed' con reason='orphaned_reserve' para que el
  // contador del cap no los cuente (filtra solo action='sent').
  try {
    const cutoff = new Date(Date.now() - 5 * 60 * 1000).toISOString(); // 5min atrás
    const orphanRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=eq.reserved&created_at=lt.${cutoff}`,
      {
        method: "PATCH",
        headers: {
          "apikey": SUPABASE_ANON_KEY,
          "Authorization": `Bearer ${BACKEND_BEARER || ""}`,
          "Content-Type": "application/json",
          "Prefer": "return=minimal",
        },
        body: JSON.stringify({ action: "failed", reason: "orphaned_reserve_crash_recovery" }),
      }
    );
    if (orphanRes.ok) log("🧹 Cleanup: reserved slots huérfanos → failed (cap diario corregido)");
  } catch (e) {
    log(`⚠️ Cleanup orphaned reserves failed: ${e.message}`);
  }


  let token = null;
  let tokenExpiry = 0;

  while (!token) {
    try {
      token = await supabaseLogin();
      _workerToken = token; // expone para helpers globales (bumpApiCounterRPC)
      tokenExpiry = Date.now() + 55 * 60 * 1000;
      log("Login exitoso.");
    } catch (err) {
      log(`⚠️ Login fallido: ${err.message} — reintentando en 60s...`);
      await sleep(60_000);
    }
  }

  // Schema sanity check — si faltan columnas críticas, loggea bien fuerte
  // para que admin corra la migración. NO bloquea boot (worker sigue corriendo
  // por si solo un subset de features necesita la columna).
  try {
    const sanityRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?select=geos_all,contact_phone&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || _workerToken}` } }
    );
    if (!sanityRes.ok) {
      const txt = await sanityRes.text().catch(() => "");
      log(`🚨 SCHEMA CHECK FAIL: ${sanityRes.status} — ${txt.substring(0, 300)}`);
      log(`🚨 CORRER MIGRACIÓN: sql/2026-05-12_geos_all_contact_phone.sql en Supabase`);
    } else {
      log("✅ Schema check OK (geos_all + contact_phone presentes)");
    }
  } catch (e) {
    log(`⚠️ Schema check error: ${e.message}`);
  }

  let idleSince = Date.now();
  log("📍 Entrando al main loop...");
  let iterCount = 0;

  // Tracker de hits ya persistidos — para flush incremental en backfill / refresh
  // (sin esperar el end-of-session de runCsvQueue).
  let _persistedRapidHits = 0;

  while (true) {
    iterCount++;
    if (iterCount === 1 || iterCount % 10 === 0) {
      log(`📍 Loop iter #${iterCount}`);
      // Memory check cada 10 iters — si rss > 700MB log WARN para detectar OOM
      try {
        const m = process.memoryUsage();
        const rssMB = Math.round(m.rss / 1024 / 1024);
        const heapMB = Math.round(m.heapUsed / 1024 / 1024);
        if (rssMB > 700) {
          log(`⚠️ MEMORIA ALTA iter #${iterCount}: rss=${rssMB}MB, heap=${heapMB}MB — riesgo OOM`);
        } else if (iterCount % 30 === 0) {
          log(`💾 Mem iter #${iterCount}: rss=${rssMB}MB, heap=${heapMB}MB`);
        }
      } catch {}
    }

    // ── REGLA DE ORO: lun-vie 9-20 Madrid. Fin de semana o fuera de hora → NADA corre ──
    // Aplica a TODOS los users y TODOS los flows: agent, csv queue, autopilot,
    // backfill, refresh, unfreezer. Override: agent_test_mode=true bypasses para testing.
    try {
      const ghCfg = await getConfig(token);
      const ghTestMode = String(ghCfg.agent_test_mode || "").toLowerCase() === "true";
      const ghStart = parseInt(ghCfg.active_hours_start || "9", 10);
      const ghEnd   = parseInt(ghCfg.active_hours_end   || "23", 10);
      // Manual override del admin: si flag manual_override_until > now, bypass horario.
      // Permite encender autopilot/queue fuera de 9-23 L-V cuando admin lo necesita.
      // Expira solo (2h default) para que no quede prendido olvidado todo el finde.
      const ghOverrideUntil = ghCfg.manual_override_until ? new Date(ghCfg.manual_override_until).getTime() : 0;
      const ghOverrideActive = ghOverrideUntil > Date.now();
      if (!ghTestMode && !ghOverrideActive) {
        if (_isWeekendSpain()) {
          if (iterCount === 1 || iterCount % 30 === 0) {
            log(`💤 Fin de semana en España (weekday=${_spainWeekday()}) — worker dormido hasta lunes`);
          }
          // Sleep largo fuera de active hours — ahorro CPU/Railway. Antes era
          // POLL_INTERVAL_MS (20s) que loopeaba sin trabajo. IDLE_INTERVAL_MS (120s)
          // es suficiente para reaccionar a un toggle ON manual del admin.
          await sleep(IDLE_INTERVAL_MS);
          continue;
        }
        if (_isOutsideActiveHours(ghStart, ghEnd)) {
          if (iterCount === 1 || iterCount % 30 === 0) {
            log(`💤 Fuera de horario España (h=${_spainHour()}, activo=${ghStart}-${ghEnd}) — worker pausado`);
          }
          // Sleep largo fuera de active hours (ver comentario arriba).
          await sleep(IDLE_INTERVAL_MS);
          continue;
        }
      }
    } catch {}


    // ── Re-engagement cada 60 iters (~25min) — INACTIVE por default ──
    // Solo corre si toolbar_config.agent_reengagement_enabled='true'.
    // Sin esto la función early-returns sin tocar nada.
    if (iterCount % 60 === 0) {
      runReengagementCycle(token).catch(e => log(`⚠️ reengagement: ${e.message}`));
      // Cola MANUAL de Email Futuro (toolbar_reengagement_queue) — se procesa
      // siempre que esté el flag general activo (mismo gate que el otro).
      processManualReengagementQueue(token).catch(e => log(`⚠️ manualReengage: ${e.message}`));
      // Re-enrich de leads malos del review_queue (flag agent_reenrich_bad_leads).
      // Sin esto, los 240 leads viejos sin Apollo nunca se actualizan.
      runReenrichBadLeads(token).catch(e => log(`⚠️ reenrich: ${e.message}`));
    }

    // ── Unfreezer cada 60 iters (~25min) ──
    // Mueve toolbar_frozen_leads.frozen_until ≤ now() de vuelta a csv_queue.pending
    if (iterCount % 60 === 0) {
      try {
        const now = new Date().toISOString();
        const res = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_frozen_leads?frozen_until=lte.${encodeURIComponent(now)}&select=domain,source,uploaded_by,attempt_count&limit=20`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
        );
        const rows = await res.json();
        if (Array.isArray(rows) && rows.length) {
          for (const row of rows) {
            // Re-encolar en csv_queue.pending
            await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue`, {
              method: "POST",
              headers: {
                "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
                "Content-Type": "application/json", "Prefer": "return=minimal",
              },
              body: JSON.stringify({
                domain: row.domain, status: "pending",
                source: row.source || "frozen_retry",
                uploaded_by: row.uploaded_by || "",
                error_message: `unfrozen_retry_attempt_${(row.attempt_count || 1) + 1}`,
              }),
            }).catch(() => {});
            // Borrar de frozen — si vuelve a fallar 3 veces, se re-congela con backoff mayor
            await fetch(`${SUPABASE_URL}/rest/v1/toolbar_frozen_leads?domain=eq.${encodeURIComponent(row.domain)}`, {
              method: "DELETE",
              headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` },
            }).catch(() => {});
          }
          log(`🧊 Unfreezer: ${rows.length} leads liberados de frozen → re-encolados en csv_queue`);
        }
      } catch (e) { log(`⚠️ unfreezer: ${e.message}`); }
    }

    // ── Cleanup periódico cada 30 iters ──
    // 1. Borrar leads pending con traffic > 0 AND < 350K (basura que el agente
    //    nunca pickearía pero acumulan en cola).
    // 2. Resetear traffic=-1 (sentinel old) → 0 para que refresh los re-intente.
    if (iterCount % 30 === 0) {
      try {
        // Reset -1 → 0 (re-eligible for refresh)
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=eq.-1`, {
          method: "PATCH",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ traffic: 0 }),
        }).catch(() => {});
        // Delete sub-threshold (0 < traffic < min)
        const delRes = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gt.0&traffic=lt.${REVIEW_QUEUE_MIN_TRAFFIC}&select=id`, {
          method: "GET",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" },
        });
        const range = delRes.headers.get("content-range") || "";
        const m = range.match(/\/(\d+)$/);
        const subCount = m ? parseInt(m[1]) : 0;
        if (subCount > 0) {
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gt.0&traffic=lt.${REVIEW_QUEUE_MIN_TRAFFIC}`, {
            method: "DELETE",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "return=minimal" },
          }).catch(() => {});
          log(`🗑 Cleanup: ${subCount} leads pending con traffic < ${REVIEW_QUEUE_MIN_TRAFFIC} eliminados`);
        }
      } catch (e) { log(`⚠️ cleanup: ${e.message}`); }
    }

    // Flush incremental del counter RapidAPI cada 10 iters — captura hits del
    // backfill / refresh (que no son parte de runCsvQueue/autopilot).
    if (iterCount % 10 === 0 && _rapidGlobalCounter > _persistedRapidHits) {
      const delta = _rapidGlobalCounter - _persistedRapidHits;
      const today = new Date().toISOString().split("T")[0];
      const period = _billingCyclePeriod();
      try {
        await saveRapidApiUsage(token, delta, today);
        await saveRapidApiMonthlyUsage(token, delta, period);
        _persistedRapidHits = _rapidGlobalCounter;
        log(`💾 Persisted +${delta} RapidAPI hits (total session: ${_rapidGlobalCounter})`);
      } catch (e) { log(`⚠️ flush rapidapi: ${e.message}`); }
    }

    try {
      if (Date.now() > tokenExpiry) {
        try {
          token = await supabaseLogin();
          _workerToken = token;
          tokenExpiry = Date.now() + 55 * 60 * 1000;
          log("Token renovado.");
        } catch (err) {
          log(`⚠️ Error renovando token: ${err.message} — reintentando en 60s...`);
          await sleep(60_000);
          continue;
        }
      }

      // Heartbeat — cliente lee esto para mostrar si Railway está vivo
      try { await setConfigValue(token, "auto_heartbeat_at", new Date().toISOString()); } catch {}

      // Force restart triggered desde popup (al prender csv toggle). El popup
      // setea worker_force_restart_at = ISO timestamp. Si es POSTERIOR al inicio
      // de este proceso, exit(0) → Railway re-arranca container limpio.
      try {
        const fres = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.worker_force_restart_at&select=value`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
        );
        const rows = await fres.json();
        const ts = rows?.[0]?.value;
        if (ts && new Date(ts).getTime() > _processStartedAt) {
          log(`🔄 Force restart triggered at ${ts} — exiting (code 1) for Railway auto-restart`);
          process.exit(1); // exit code 1 = Railway interpreta como crash y reinicia automático
        }
      } catch {}

      // Promueve items waiting_pool → pending si hay espacio en review_queue.
      // Cada loop iteration lo intenta — items "preparados" por MBs entran
      // automáticamente cuando se libera lugar.
      try { await promoteWaitlist(token); } catch (e) { log(`⚠️ promoteWaitlist: ${e.message}`); }

      // Refresh job: si admin activó agent_refresh_empty_leads, procesar 1
      // lead vacío por ciclo (cache 90d ahorra hits si ya analizado).
      try {
        const cfgRefresh = await getConfig(token);
        await refreshOneEmptyLead(token, cfgRefresh);
        // Backfill missing fields (language/contact_name/category/score) — corre
        // en paralelo lógico con refresh de tráfico. Ambos terminan rápido (paralelo).
        await backfillMissingFields(token, cfgRefresh);
      } catch (e) { log(`⚠️ refresh+backfill: ${e.message}`); }

      // ── AUTO-FEEDER v2: schedule fijo 9/12/15/18/20 Madrid L-V ───
      // Reemplaza el band-maintainer 120-300. Ahora target diario fijo
      // (150 efectivos) repartido en 5 crons. Ver sección AUTO-FEEDER v2.
      try {
        const rqValid = await _getReviewQueueValidCount(token);
        await setConfigValue(token, "review_queue_band_status",
          JSON.stringify({ valid: rqValid, saturation: FEEDER_RQ_SATURATION, at: new Date().toISOString() })
        );
        await maybeRunFeederSlot(token).catch(e => log(`⚠️ feeder slot: ${e.message}`));
        await maybeStartAutopilotSlot(token).catch(e => log(`⚠️ autopilot slot: ${e.message}`));
        await _measureFeederRuns(token).catch(e => log(`⚠️ feeder measure: ${e.message}`));
        await _checkAutoPauseAgent(token).catch(e => log(`⚠️ autopause: ${e.message}`));
        // Source performance aggregate (1× por día, guard interno)
        await maybeRunSourcePerformanceAggregate(token).catch(e => log(`⚠️ source perf: ${e.message}`));
        // Notification scanners — cada uno tiene su guard de frecuencia interno
        await runNotificationScanners(token).catch(e => log(`⚠️ notif scanners: ${e.message}`));

        // Boot-time guarantee: agent_enabled_users siempre con mgargiulo si vacío.
        // Reemplaza al self-activator viejo (sin chequear horario ni manual_off).
        const flagsRes = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.agent_enabled_users&select=value`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
        );
        const flagsRows = await flagsRes.json().catch(() => []);
        let agentUsers = [];
        try { agentUsers = JSON.parse(flagsRows?.[0]?.value || "[]"); } catch {}
        if (agentUsers.length === 0) {
          await setConfigValue(token, "agent_enabled_users", JSON.stringify(["mgargiulo@adeqmedia.com"]));
          log(`🔛 boot guarantee: agent_enabled_users=[mgargiulo@adeqmedia.com]`);
        }
      } catch (e) { log(`⚠️ band maintainer: ${e.message}`); }

      // Auto-encender csv_queue_enabled si hay items pending y el flag está OFF.
      // Política user 2026-05-13: "siempre que hay urls en cola, procesarlas
      // hasta finalizar". Antes el flag se apagaba al vaciar la cola y había
      // que prenderlo manualmente al subir más items.
      try {
        const pcRes = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.pending&select=id&limit=1`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
        );
        const range = pcRes.headers.get("content-range") || "";
        const pendingCount = parseInt(range.match(/\/(\d+)$/)?.[1] || "0", 10);
        if (pendingCount > 0) {
          // Lee el flag actual y solo lo prende si está apagado (evita writes vacíos)
          const fRes = await fetch(
            `${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.csv_queue_enabled&select=value`,
            { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
          );
          const fRows = await fRes.json().catch(() => []);
          const isOn = (fRows?.[0]?.value || "").toLowerCase() === "true";
          if (!isOn) {
            await setConfigValue(token, "csv_queue_enabled", "true");
            log(`🔛 csv_queue auto-encendida: ${pendingCount} items pending detectados`);
          }
        }
      } catch (e) { log(`⚠️ csvQueue auto-enable: ${e.message}`); }

      // Frozen weekly report — domingo 20-21hs Madrid, 1 vez por semana
      try { await runFrozenWeeklyReport(token); } catch (e) { log(`⚠️ frozenReport: ${e.message}`); }

      // Poll liviano — lee autopilot + csv_queue + agent flags
      const flags = await getActiveFlags(token);
      if (iterCount === 1 || iterCount % 10 === 0) {
        log(`🚦 flags: autopilot=${flags.autopilot} csv=${flags.csvQueue} agent=${flags.agent} (users=${flags.agentUsers.length})`);
      }

      if (!flags.autopilot && !flags.csvQueue && !flags.agent) {
        // Auto-exit si llevamos > IDLE_EXIT_MS sin trabajo (Railway corta billing)
        if (Date.now() - idleSince >= IDLE_EXIT_MS) {
          log(`💤 Idle ${Math.round(IDLE_EXIT_MS / 60000)} min — exiting. Railway re-arranca cuando se prenda autopilot/csv desde el toolbar.`);
          process.exit(0);
        }
        await sleep(IDLE_INTERVAL_MS);
        continue;
      }
      idleSince = Date.now(); // hay trabajo → reset contador idle

      // ── PARALELIZACIÓN ──────────────────────────────────────
      // Agent + CSV queue son INDEPENDIENTES (Gmail send vs RapidAPI/Apollo
      // enrich). Los corremos en paralelo para que el agent no espere.
      // Autopilot también es independiente pero es loop largo (sesión 20min)
      // — lo dejamos secuencial debajo por simplicidad.
      const cfgShared = (flags.csvQueue || flags.agent) ? await getConfig(token) : null;
      const parallelTasks = [];
      let csvProcessed = 0;
      if (flags.csvQueue) {
        parallelTasks.push(
          // Sin batch — procesa TODA la cola hasta vaciarla. Agent corre en
          // paralelo via Promise.all. Si crashea mid-queue, Railway reinicia
          // automático y el worker continúa desde donde quedó (items processing
          // se resetean a pending por el cleanup, pendientes se procesan).
          runCsvQueue(token, cfgShared, Infinity).then(n => { csvProcessed = n; }).catch(e => log(`⚠️ runCsvQueue: ${e.message}`))
        );
      }
      if (flags.agent) {
        parallelTasks.push(
          maybeRunAgentSlot(token, flags).catch(e => log(`⚠️ runAgentCycle: ${e.message}`))
        );
      }
      if (parallelTasks.length > 0) {
        await Promise.all(parallelTasks);
      }
      // Si csv hubo trabajo, no caemos a autopilot — próximo loop sigue csv
      if (csvProcessed > 0) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      // Autopilot (sólo si está prendido) — secuencial porque las sesiones son largas
      if (!flags.autopilot) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      // Encendido — leer config completo una sola vez por sesión
      const cfg = await getConfig(token);

      let sessionStart;
      if (cfg.auto_session_start) {
        sessionStart = new Date(cfg.auto_session_start).getTime();
      } else {
        sessionStart = Date.now();
        await setConfigValue(token, "auto_session_start", new Date(sessionStart).toISOString());
      }

      if (Date.now() - sessionStart >= SESSION_LIMIT_MS) {
        const minutesAgo = Math.round((Date.now() - sessionStart) / 60000);
        log(`⏱ Sesión expirada (start hace ${minutesAgo}min, limit ${SESSION_LIMIT_MS/60000}min) — auto-apagando. El user debe re-prender el toggle.`);
        await setConfigValue(token, "auto_prospecting_enabled", "false");
        await setConfigValue(token, "auto_session_start", "");
        // Marcar en auto_session_stats que la sesión expiró sin trabajar — la
        // toolbar lo lee y muestra mensaje claro al user
        await setConfigValue(token, "auto_session_stats", JSON.stringify({
          processed: 0, added: 0, filtered: 0,
          lastDomain: "(session expired before processing — toggle OFF/ON to restart)",
          lastUpdate: Date.now(),
          sessionUser: cfg.auto_session_user || "",
        })).catch(() => {});
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      await runSession(token, cfg, sessionStart);
      // Auto-disable after each session — user must re-enable manually
      await setConfigValue(token, "auto_prospecting_enabled", "false");
      await setConfigValue(token, "auto_session_start", "");
      log("✅ Sesión completada — autopilot apagado. Activalo manualmente para la próxima sesión.");
      await sleep(POLL_INTERVAL_MS);

    } catch (err) {
      log(`❌ Error: ${err.message}`);
      await sleep(POLL_INTERVAL_MS);
    }
  }
}

main().catch(err => {
  console.error("Error fatal:", err);
  setTimeout(() => main(), 30_000);
});
