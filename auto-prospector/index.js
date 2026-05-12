// ============================================================
// ADEQ AUTO-PROSPECTOR — v3
// Cambios v3:
//   - Fuente de dominios: Majestic Million (1M sitios rankeados del mundo)
//   - Los dominios de Monday se usan como EXCLUSIÓN (ya son clientes)
//   - Pool de dominios se descarga una vez al iniciar Railway (en memoria)
// Deploy: Railway
// ============================================================

import fetch from "node-fetch";
import { pickRandomTemplate, fillTemplate, pickPitchSource } from "./templates.js";

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
const IDLE_EXIT_MS      = 30 * 60 * 1000; // si está idle 30 min seguidos, exit (Railway no factura idle)
const DOMAIN_DELAY_MS  = 2500;
const MIN_TRAFFIC      = 400_000;  // pageViews mínimos para AUTOPILOT Majestic (descubrimiento)
const REVIEW_QUEUE_MIN_TRAFFIC = 350_000; // Floor absoluto en review_queue. Items debajo se auto-borran (no acumulan basura).

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
  GB:"United Kingdom", FR:"France", DE:"Germany", IT:"Italy", PT:"Portugal",
  CA:"Canada", AU:"Australia", JP:"Japan", KR:"South Korea", IN:"India",
  VN:"Vietnam", TH:"Thailand", ID:"Indonesia", PH:"Philippines", TR:"Turkey",
  SA:"Saudi Arabia", AE:"UAE", EG:"Egypt", MA:"Morocco", ZA:"South Africa",
  NG:"Nigeria", RU:"Russia", UA:"Ukraine", PL:"Poland", NL:"Netherlands",
  BE:"Belgium", SE:"Sweden", CH:"Switzerland", AT:"Austria", NO:"Norway",
  DK:"Denmark", FI:"Finland", IL:"Israel", SG:"Singapore", CN:"China",
  MY:"Malaysia", GR:"Greece", HU:"Hungary", CZ:"Czech Republic", RO:"Romania",
  TW:"Taiwan", HK:"Hong Kong", PK:"Pakistan",
};

// Reverse lookup: country name → ISO code (for Cloudflare Radar API)
const COUNTRY_NAME_TO_CODE = Object.fromEntries(
  Object.entries(COUNTRY_CODES).map(([code, name]) => [name, code])
);

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
const WAITLIST_HARD_CAP  = 300;
// Cap diario GLOBAL — protege RapidAPI mensual (40K hits) + Railway billing.
// 1000 csv_queue + 300 autopilot = 1300/día = ~39K/mes.
// Configurable runtime: toolbar_config.csv_queue_daily_cap, autopilot_daily_cap_global
const DEFAULT_CSV_DAILY_GLOBAL_CAP = 1000;
const DEFAULT_AUTOPILOT_DAILY_GLOBAL_CAP = 300;

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
  if (_csvDailyCounterDate !== todaySpain) { _csvDailyCounterDate = todaySpain; _csvDailyCounter = 0; }
  if (_autopilotDailyCounterDate !== todaySpain) { _autopilotDailyCounterDate = todaySpain; _autopilotDailyCounter = 0; }
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

async function saveToReviewQueue(token, { domain, traffic, geo, language, category, contactName, emails, pitch, pitchSubject, pitchSubjects, score, adNetworks, pageTitle, createdBy, source = "autopilot", mondayItemId = null }) {
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
      language:       language       || "",
      category:       category       || "",
      contact_name:   contactName    || "",
      emails:         emails         || [],
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
        if (newVisits > 0 || newGeo) {
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
    const usedThisMonth = storedPer === period ? storedCount : 0;
    return { usedThisMonth, limit, period };
  } catch { return { usedThisMonth: 0, limit: 40000, period: _billingCyclePeriod() }; }
}

// Período de facturación 6→6 (alineado al billing real de RapidAPI plan PRO).
function _billingCyclePeriod() {
  const now = new Date();
  const day = now.getDate();
  const cycleStart = day >= 6
    ? new Date(now.getFullYear(), now.getMonth(), 6)
    : new Date(now.getFullYear(), now.getMonth() - 1, 6);
  const yyyy = cycleStart.getFullYear();
  const mm   = String(cycleStart.getMonth() + 1).padStart(2, "0");
  return `${yyyy}-${mm}-06`;
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
    const storedCount = storedPer === period ? parseInt(map.rapidapi_calls_month || "0", 10) : 0;
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
async function rapidFetchWithRetry(url, headers, timeout = 8000) {
  let lastErr = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      // Pre-check del cap diario global (evita facturar si ya pasamos el límite)
      if (_rapidCapReached) return { __error4xx: "daily_cap_reached" };
      _rapidGlobalCounter++;
      const res = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
      if (res.ok) return await res.json();
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
    // number directo, o string "1.2M". Cubrimos todos los casos.
    const tv = data.Traffic?.Visits;
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
    if (data.Traffic?.TopCountryShares && typeof data.Traffic.TopCountryShares === "object" && !Array.isArray(data.Traffic.TopCountryShares)) {
      data.TopCountries = Object.entries(data.Traffic.TopCountryShares)
        .map(([code, share]) => ({ CountryCode: code, Share: parseFloat(share) || 0 }))
        .sort((a, b) => b.Share - a.Share);
    }
    // Category bajo WebsiteDetails
    if (data.WebsiteDetails?.Category && !data.Category) data.Category = data.WebsiteDetails.Category;

    const visits = data?.Visits || data?.visits || data?.pageViews || data?.PageViews || null;

    // Log diagnóstico si el shape de la API cambió y no extrajimos visits
    if (!visits) {
      const sampleKeys = Object.keys(data || {}).slice(0, 8).join(",");
      log(`  ⚠️ getTrafficData ${domain}: response OK pero sin visits. Top-level keys: [${sampleKeys}]. Posible cambio de shape de la API.`);
    }

    // Pages per visit del nuevo shape (Traffic.Engagement.PagesPerVisit) o legacy (PagePerVisit)
    const pagesPerVisit = data?.Traffic?.Engagement?.PagesPerVisit
                       || data?.PagePerVisit || data?.PagesPerVisit
                       || data?.pagesPerVisit || null;

    // Extract top country del response. NO llamamos /countries fallback — desde
    // el switch a website-insights, el GEO viene siempre inline en /all-insights.
    // Si no viene → el caller usa fallback gratis (TLD + Cloudflare Radar hint).
    let topCountry = null;
    const inlineList = data?.TopCountries || data?.Countries || data?.countries
                    || data?.topCountryShares || data?.CountryShares || [];
    if (Array.isArray(inlineList) && inlineList.length) {
      const c    = inlineList[0];
      const code = (c?.CountryCode || c?.countryCode || c?.Country || c?.country || "").toUpperCase().slice(0, 2);
      if (code) topCountry = COUNTRY_CODES[code] || code;
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
    return { visits, pagesPerVisit, topCountry, error: null };
  } catch (e) { return { visits: null, pagesPerVisit: null, topCountry: null, error: e.message }; }
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

// ── Email scraping fallback (server-side HTTP) ────────────────

const EMAIL_REGEX  = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6}(?=\s|$|[^a-zA-Z])/g;
const IGNORE_EMAIL = ["example.com","domain.com","sentry.io","google.com","w3.org","schema.org","cloudflare.com"];

function extractEmailsFromHtml(html) {
  const clean = html
    .replace(/&#64;|&#x40;/gi, "@").replace(/&#46;|&#x2e;/gi, ".")
    .replace(/\[\s*at\s*\]/gi, "@").replace(/\(\s*at\s*\)/gi, "@")
    .replace(/\barroba\b/gi,   "@").replace(/\bpunto\b/gi,    ".");
  const found = [...new Set((clean.match(EMAIL_REGEX) || []).map(e => e.toLowerCase()))];
  return found.filter(e => {
    const lower = e.toLowerCase();
    if (IGNORE_EMAIL.some(p => lower.includes(p))) return false;
    const parts = e.split("@");
    if (parts.length !== 2) return false;
    const tld = parts[1].split(".").pop();
    return tld && tld.length >= 2 && tld.length <= 6;
  });
}

async function scrapeEmailsForDomain(domain) {
  const emails = new Set();
  const base   = `https://${domain}`;

  // 1. website.informer.com
  try {
    const r = await fetch(`https://website.informer.com/${domain}`, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; bot)" },
      signal: AbortSignal.timeout(7000),
    });
    if (r.ok) extractEmailsFromHtml(await r.text()).forEach(e => emails.add(e));
  } catch {}

  if (emails.size > 0) return [...emails];

  // 2. who.is WHOIS (registrant email)
  try {
    const cleanDomain = domain.replace(/^www\./, "");
    const r = await fetch(`https://who.is/whois/${cleanDomain}`, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; bot)" },
      signal: AbortSignal.timeout(7000),
    });
    if (r.ok) extractEmailsFromHtml(await r.text()).forEach(e => emails.add(e));
  } catch {}

  if (emails.size > 0) return [...emails];

  // 3. Contact pages
  for (const path of ["/contact", "/contact-us", "/about", "/advertise", "/advertising"]) {
    try {
      const r = await fetch(new URL(path, base).href, { signal: AbortSignal.timeout(4000) });
      if (r.ok) extractEmailsFromHtml(await r.text()).forEach(e => emails.add(e));
      if (emails.size > 0) break;
    } catch {}
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

  // Cache hit por dominio (memoria proceso)
  if (cleanDomain && _domainLangCache.has(cleanDomain)) {
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

  // Cache result
  if (cleanDomain) {
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
  "com.gh","com.ke","com.uy",
  "co.uk","co.za","co.in","co.kr","co.jp","co.il","co.nz","co.id","co.cr","co.ve",
  // organization
  "org.uk","org.ar","org.br","org.mx","org.au","org.za","org.es","org.in","org.pl",
  // government — todos los .gov.X conocidos
  "gov.ar","gov.br","gov.mx","gov.co","gov.pe","gov.cl","gov.uy","gov.ec","gov.ve",
  "gov.bo","gov.au","gov.in","gov.uk","gov.za","gov.eg","gov.sa","gov.ng",
  // academic
  "ac.uk","ac.in","ac.za","ac.jp","ac.kr","ac.nz","edu.ar","edu.br","edu.mx",
  "edu.co","edu.pe","edu.uy","edu.au","edu.in","edu.eg",
  // network/info per country
  "net.ar","net.br","net.mx","net.au","net.in",
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
    .replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "").trim();
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
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.pending${filter}&order=uploaded_at.asc&limit=1&select=id,domain,uploaded_by`,
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
  let { visits, topCountry } = trafficData;
  if (!topCountry) {
    const inferred = inferCountryFromTLD(domain);
    if (inferred) topCountry = inferred;
  }
  // Filtro 350K: si tenemos traffic conocido y es bajo, no insertar al pool.
  // Si visits=0 (sin data), igual lo dejamos pasar — el agente lo enriquece después.
  if (visits > 0 && visits < REVIEW_QUEUE_MIN_TRAFFIC) {
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
    && visits >= 500_000
    && (apolloUsage.usedToday + apolloCallsThisSessionRef.count) < apolloUsage.limit
    && apolloMonthRemaining > 0;

  const [apolloEmails, scraperEmails] = await Promise.all([
    canUseApollo
      ? findAllEmails(domain, apollo_api_key, token).then(r => { apolloCallsThisSessionRef.count += 2; return r; })
      : Promise.resolve([]),
    scrapeEmailsForDomain(domain),
  ]);
  const rawEmails = [...new Set([...apolloEmails, ...scraperEmails])];
  // Filtrar emails con dominio sin MX records ANTES de guardar — evita que el
  // MB o el agente elijan después un email que da bounce.
  const emails = await validateEmailsBatch(rawEmails);
  if (rawEmails.length !== emails.length) {
    log(`  📧 ${domain}: ${rawEmails.length} → ${emails.length} emails (filtrados ${rawEmails.length - emails.length} sin MX/garbage)`);
  }
  const apolloContactName = (apolloEmails && apolloEmails.contact_name) || "";

  // 3. NO empujar a Monday automáticamente. Escribir a review_queue para que el MB
  //    decida email + draft + push manualmente desde el tab Prospects.
  try {
    await saveToReviewQueue(token, {
      domain,
      traffic:        visits || 0,
      geo:            topCountry || "",
      language:       detectedLang,
      category,
      contactName:    apolloContactName,
      emails,
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
  // Pausa fin de semana — operativo solo Lun-Vie España
  if (_isWeekendSpain()) {
    log(`⏸ Fin de semana España — CSV queue pausada hasta lunes.`);
    return 0;
  }
  _rapidGlobalCounter = 0;
  _rapidCapReached    = false;
  const _rapidStart      = rapidUsage.usedToday;
  const _rapidMonthStart = rapidMonth.usedThisMonth;

  while (processed < maxItems) {
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
  // Defaults: 75 prospectos/día y 60 min/sesión. Si el admin los cambió, los usa.
  let AUTOPILOT_DAILY_LIMIT = 300; // default 300/día/usuario; admin lo puede sobrescribir per-user
  let userSessionLimitMs    = SESSION_LIMIT_MS;
  if (sessionUser) {
    try {
      const userLimits = await getUserLimits(token, sessionUser);
      if (userLimits.autopilot_daily_prospects > 0) AUTOPILOT_DAILY_LIMIT = userLimits.autopilot_daily_prospects;
      if (userLimits.autopilot_daily_minutes >= 5)  userSessionLimitMs    = userLimits.autopilot_daily_minutes * 60 * 1000;
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

  // GLOBAL safety net + weekend pause
  if (_isWeekendSpain()) {
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

  // ── POOL HÍBRIDO ──
  // Cuando hay target_geos: combinar Radar + Majestic-filtered-by-TLD
  // Sin target_geos: usar Majestic completo
  let pool;
  if (hasTargetGeo) {
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
    // 1) Blocklist corporativa / universidades / adultos / tech giants
    const blockReason = isDomainBlocked(domain);
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

    let { visits, pagesPerVisit, topCountry, error: rapidError } = trafficData;

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

    const [apolloEmails, scraperEmails, similarSites] = await Promise.all([
      canUseApollo
        ? findAllEmails(domain, apollo_api_key, token).then(r => { apolloCallsThisSession += 2; return r; })
        : Promise.resolve([]),
      scrapeEmailsForDomain(domain),
      findSimilarSites(domain, rapidapi_key),
    ]);
    const rawEmailsAuto = [...new Set([...apolloEmails, ...scraperEmails])];
    const emails = await validateEmailsBatch(rawEmailsAuto);
    if (rawEmailsAuto.length !== emails.length) {
      log(`  📧 ${domain}: ${rawEmailsAuto.length} → ${emails.length} emails (filtrados ${rawEmailsAuto.length - emails.length})`);
    }
    const apolloContactNameAuto = (apolloEmails && apolloEmails.contact_name) || "";

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
      language,
      category,
      contactName:   apolloContactNameAuto || contactName,
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
}

// ════════════════════════════════════════════════════════════════
// 🤖 AGENT MB — Auto end-to-end prospecting (push Monday + send Gmail)
// Activado solo para users en toolbar_config.agent_enabled_users (JSON array).
// Procesa review_queue → quality gates → Claude pitch → Monday push → Gmail send.
// Simula 100% el workflow del MB humano.
// ════════════════════════════════════════════════════════════════

const AGENT_DEFAULTS = {
  threshold_traffic:    500_000,
  threshold_score:      40,
  max_per_day:          20,
  active_hours_start:   9,         // 9am España (CET/CEST)
  active_hours_end:     20,        // 20hs España
  active_timezone:      "Europe/Madrid",
  cycle_interval_sec:   300,       // 5 min entre ciclos
  per_cycle_limit:      3,         // max 3 leads procesados por ciclo (rate limit Gmail-friendly)
  monday_board_id:      1420268379,
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
    thresholdScore:   get("threshold_score",      AGENT_DEFAULTS.threshold_score),
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

// Devuelve true si AHORA estamos FUERA de las active hours (9-20 España).
function _isOutsideActiveHours(activeStart, activeEnd) {
  const h = _spainHour();
  if (activeStart < activeEnd) return h < activeStart || h >= activeEnd;
  // wrap (raro pero soportado): si start > end (ej. 20-9 → trabaja noche)
  // Activo cuando h>=start O h<end. Fuera de rango: h<start && h>=end.
  return h < activeStart && h >= activeEnd;
  // (mantenido — si activeStart=activeEnd → siempre fuera, deseable como kill switch)
}

async function logAgentAction(token, userEmail, payload) {
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
      },
      body: JSON.stringify({ user_email: userEmail, ...payload }),
    });
  } catch (e) {
    log(`⚠️ logAgentAction failed: ${e.message}`);
  }
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
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(userEmail)}&action=in.(sent,reserved)&created_at=gte.${cutoffUtc}&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("content-range") || "";
    const m = range.match(/\/(\d+)$/);
    return m ? parseInt(m[1]) : 0;
  } catch { return 0; }
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
  } catch { return false; }
}

// Cache del estilo ADEQ — vive por la lifetime del proceso (refresh cada 30min)
let _adeqStyleCache = null;
let _adeqStyleCacheAt = 0;
const ADEQ_STYLE_TTL = 30 * 60 * 1000;

const ADEQ_STYLE_FALLBACK = `# IDENTIDAD
Sos Claude operando como redactor de emails comerciales de ADEQ Media — escribís en
nombre del equipo de Publishers Relations. NO sos un asistente neutral. Sos un vendedor
B2B AdTech con voz humana, conversacional, auto-consciente del spam.
El mail debe PARECER escrito en 2 minutos entre dos llamadas — NO un email armado por marketing.

# CONTEXTO DESTINATARIO
El publisher recibe 10-20 emails al día vendiéndole monetización. Está cansado, ignora,
archiva sin leer. Si tu mail huele a outreach armado → no responde. Si parece tipeado
entre dos llamadas → sí responde. La AUTO-CONCIENCIA del spam es un anzuelo poderoso:
"sé que te llegan muchos mails", "intento ser breve", "para no perder tiempo ambos".

# PRODUCTO
1. Header bidding interno: 8 demandas compitiendo por slot display → uplift 25-30%
   sobre stack actual. NUNCA dar CPM puntual, solo el uplift %.
2. Video instream: player propio que reproduce ads o previews. CPM 1.5-2.5 USD,
   +50% fillrate. Ideal sports/entretenimiento.
3. Slider/corner video: CPM fijo 1 USD. Mencionar SOLO si pidieron o tras lo principal.
4. Display sticky / interstitials: complementarios.

ARGUMENTOS TRANSVERSALES (en casi todos los mails):
- Sin exclusividad ni permanencia mínima
- Sumamos al stack que ya tenés, no reemplazamos nada
- Solo seguimos si los resultados son buenos
- Revshare 80/20 a favor del publisher (mencionar solo si preguntan)

# TÁCTICAS PARA MAIL INICIAL (cold)

## Cold 1 — primer contacto (más usado)
Engancha con UN dato concreto + pregunta de validación.
Ejemplo:
"Hola [Nombre], ¿cómo estás?
Vi [dominio]. Estamos cargando nuevas campañas y veo que el sitio puede rendir bien
con header bidding o video instream.
¿Sos vos quien maneja la monetización o me conviene hablar con alguien más?
Saludos."

## Cold 2 — pitch directo (lead caliente o post-validación)
1-2 opciones concretas. CPM solo en video o slider.
Ejemplo:
"Hola [Nombre], ¿cómo estás?
Tengo dos opciones con muy buenos resultados.
Por un lado un slider con CPM fijo de 1 USD. Por otro, gestionamos toda la demanda
de display con header bidding interno donde competimos 8 demandas (uplift típico 25-30%).
Sin exclusividad, sin permanencia mínima — solo seguimos si los resultados son buenos.
¿Cómo lo ves?"

## Cold 3 — pidiendo info antes de pitchear
Hacé hablar al lead.
Ejemplo:
"Hola [Nombre], ¿cómo va todo?
Te hago una consulta rápida antes de armar un mail largo.
¿Qué están trabajando hoy? ¿Algún partner les está rindiendo mal o verían bien sumar
otras opciones?
Espero tu aviso."

# REGLAS DE ESCRITURA (no negociables)

## Apertura por idioma
- ES: "Hola [Nombre], ¿cómo estás?" (sin nombre: "Hola, ¿cómo estás?")
- EN: "Hi [Name], how are you?" o "how have you been?" (warm)
- PT-BR: "Olá [Nome], tudo bem?"
- PT-PT: "Olá [Nome], tudo bem?" (más formal)

## Largo
- Cold puro: 60-100 palabras
- Cold con CPM/datos: 80-130 palabras
Foco: lectura en 15 segundos.

## Cierre (SIEMPRE pregunta)
ES: "¿Cómo lo ves?", "¿Qué opinás?", "¿Qué te parece?", "¿Te interesa probar?",
"Espero tu aviso", "Espero tus comentarios".
EN: "What do you think?", "Looking forward to hearing from you."

## Firma — REGLA DURA
NUNCA firmar con nombre. La firma de Gmail se agrega automáticamente.
Último renglón = pregunta + opcional "Saludos." / "Saludos!" suelto. Nada más.
NUNCA escribir nombres propios del equipo ADEQ ("Diego", "Max", "Agus", etc.).

## Estructura
Sin bullets, sin negritas, sin formato. Solo párrafos cortos separados por línea
en blanco — estilo WhatsApp largo.

## CPM — regla estricta
- Slider          → SÍ: "CPM fijo 1 USD"
- Video instream  → SÍ: "1.5-2.5 USD CPM, +50% fillrate"
- Header bidding  → NO mencionar CPM. Solo "uplift 25-30%"
- Display puro    → NO mencionar CPM. Derivar a call.

## Frases PROHIBIDAS
"Espero que te encuentres bien", "Quedo a su disposición",
"Sin más por el momento", "Estimado/a", "Sr./Sra.",
"win-win", "sinergia", "apalancar", "ecosistema",
"todo piola", "dale campeón".

## Frases MAGNÉTICAS (usar libremente)
- "Intento ser breve para no armar un mail extenso."
- "Antes de armarte un mail largo te hago una consulta rápida."
- "Sé que te llegan 10/20 mails al día y debe ser cansador."
- "Para no perder tiempo ambos."
- "Te dejo dos opciones / tres datos."
- "Sin ataduras comerciales."
- "Solo seguimos si los resultados son buenos."

# CLIENTES REFERENCIA (ÚNICOS permitidos — no inventar otros)
- Raialyoum.com — Árabe/MENA
- Ciclo21.com — News ES
- MuchoDeporte.com — Sports ES
- Footballia.net — Sports/fútbol
- ElPilon.com.co — News Colombia
- owngoalnigeria.com / zamusic.co.za / fakazahub.com — África sub-sahariana

Si ninguno encaja al vertical, OMITIR referencia (no inventar).

# PERSONALIZACIÓN POR SEÑAL (si vienen en input)
- Sports → priorizar instream, mencionar Footballia/MuchoDeporte si encaja
- News → header bidding + densidad banners, mencionar Ciclo21/ElPilon
- Árabe/MENA → Raialyoum
- África sub-sahariana → owngoalnigeria/zamusic/fakazahub
- Tráfico < 100K → tono cercano, "no tenemos mínimos"
- Tráfico > 5M → tono más profesional, mencionar dashboard propio
- AdSense detectado → "no lo reemplazamos, sumamos al stack"
- Taboola/Outbrain detectado → "se complementa, no compite por el mismo slot"

# DATOS NUMÉRICOS PERMITIDOS (no inventar otros)
- Slider: CPM fijo 1 USD ✓ libre
- Video instream: 1.5-2.5 USD CPM + 50% fillrate ✓ libre
- Header bidding: 8 demandas compitiendo, uplift 25-30% (NO como CPM)
- Display puro: sin CPM público — derivar a call
- Revshare: 80/20 a favor publisher (solo si preguntan)
- Pago: NET 60 (NUNCA en cold)

# REGLAS DURAS
- NO mencionar meses ni fechas específicas
- NO afirmar ausencia de ads.txt/tech salvo que el input lo CONFIRME ("ads.txt confirmed absent")
- NO inventar clientes referencia fuera de la lista
- NO usar frases prohibidas
- NO firma con nombre propio
- Mencionar el dominio al menos 1 vez en el cuerpo
- Si dudás, terminar más corto que más largo`;

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
const GARBAGE_LOCAL = /^(abuse|admin|administrator|whois|postmaster|noreply|no-reply|donotreply|do-not-reply|bounce|mailer-daemon|root|hostmaster|nobody|webmaster|cert|csirt|soc|noc|ops|operations|security|domain-?abuse|domain|domains|domain-?ops|domain-?operations|dns|ssl|ndomains|registry|registrar|tech|technical|spam|fraud|legal|copyright|dmca|complaint|complaints|billing|accounts?|accounting|finance|info-?legal|privacy|private|gdpr|dpo|hosting|host|cdn|cloudflare|proxy|piracy|pirate|antipiracy|takedown)@/i;
// Cualquier ocurrencia de estas palabras en el local-part también descarta
const GARBAGE_LOCAL_CONTAINS = /(abuse|domain[-._]?ops|domain[-._]?operations|domain[-._]?abuse|hosting|cloudflare|cloudfront|proxy|piracy|pirate|takedown|whois)/i;

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

  // 1. GEO (máx 30, gate si Tier1/UK/RU)
  const geoPts = scoreGeo(lead.geo);
  if (geoPts < 0) return { score: -1, color: "red", reasons: [`geo_blocked:${lead.geo}`] };
  score += geoPts;
  reasons.push(`geo:${lead.geo || "?"}=${geoPts}`);

  // 2. CATEGORÍA (gate duro adult/streaming)
  const cat = (lead.category || "").toLowerCase();
  if (BLOCKED_CATEGORIES.has(cat)) {
    return { score: -1, color: "red", reasons: [`cat_blocked:${cat}`] };
  }
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
function rankEmail(email, siteDomain) {
  if (!email || typeof email !== "string" || !email.includes("@")) return -1;
  const lower = email.toLowerCase();
  if (GARBAGE_LOCAL.test(lower) || GARBAGE_DOMAIN_PATTERN.test(lower)) return -1;
  const [local, dom] = lower.split("@");
  if (!local || !dom) return -1;
  if (GARBAGE_LOCAL_CONTAINS.test(local)) return -1; // abuse/domain/proxy/hosting/etc
  let score = 0;
  // Cross-domain penalty fuerte: si el email es de otro dominio que no matchea
  // el sitio prospectado (ej. cavok.com.br → marcio@nunes.inf.br), penalizar -50.
  // Apollo a veces devuelve emails de empresas relacionadas que no son útiles.
  const cleanSiteEarly = (siteDomain || "").replace(/^www\./, "");
  if (cleanSiteEarly && dom !== cleanSiteEarly && !dom.endsWith("." + cleanSiteEarly) && !cleanSiteEarly.endsWith("." + dom)) {
    score -= 50;
  }
  // Roles comerciales / editoriales (target ideal de outreach)
  if (/^(marketing|comercial|business|partnerships|partner|ads|advertising|publicidad|monetiza|ventas|sales|bd|director|gerente|manager|jefe|head)/.test(local)) score += 80;
  else if (/^(editor|redacao|redaccion|redazione|writer|periodista|journalist|prensa|press)/.test(local)) score += 60;
  // Nombre personal (first.last o first_last) muy probable persona real
  else if (/^[a-z]+[._-][a-z]{2,}$/.test(local)) score += 70;
  // Pattern firstinitial+lastname (jperez@, mgarcia@) — común corp
  else if (/^[a-z][a-z]{4,15}$/.test(local) && local.length >= 5) score += 50;
  // Roles genéricos OK pero menos efectivos
  else if (/^(info|contact|contacto|hello|hi|hola|support|soporte|mail|email)$/.test(local)) score += 20;
  // Penalizar dígitos largos en local (típico de listas viejas)
  if (/\d{3,}/.test(local)) score -= 40;
  // Penalizar local muy corto/largo (probables alias/auto-gen)
  if (local.length < 3) score -= 30;
  if (local.length > 30) score -= 30;
  // Dominio del sitio matchea (no proxy random)
  const cleanSite = (siteDomain || "").replace(/^www\./, "");
  if (cleanSite && (dom === cleanSite || dom.endsWith("." + cleanSite) || cleanSite.endsWith("." + dom))) score += 30;
  // Email free webmail = penalizar (no es corporativo)
  if (/^(gmail|yahoo|hotmail|outlook|live|aol|icloud|protonmail|gmx|mail\.ru|yandex)\./.test(dom)) score -= 30;
  return score;
}
const GARBAGE_DOMAIN_PATTERN = /(^|\.)(nic\.|whois\.|abuse\.|donuts\.|godaddy|cert\.|registry\.|registrar\.)|domainsbyproxy\.com|whoisguard|whoisprivacy|domainprotect|privacyprotect|contactprivacy|perfectprivacy|namebrightprivacy|withheldforprivacy|dropped\.|internetx\.com|markmonitor|cscglobal|csc-corp|comlaude|safenames|gandi\.net|key-systems|1api\.net|netim\.com|psi-usa|nameshield|epag\.de|eurodns|realtimeregister|tld-box|enom\.|networksolutions|tucows|porkbun\.com|namecheap.*proxy/i;
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
    if (GARBAGE_LOCAL_CONTAINS.test(local)) continue; // domain.operations@x.com etc
    const dom = lower.split("@")[1];
    const ok = await _hasMxRecords(dom);
    if (ok) out.push(lower);
  }
  return [...new Set(out)];
}

// MX records cache — LRU cap 500 dominios para evitar leak en worker 24/7.
const _mxCache = new Map();
const MX_CACHE_MAX = 500;
async function _hasMxRecords(domain) {
  if (!domain) return false;
  if (_mxCache.has(domain)) return _mxCache.get(domain);
  if (_mxCache.size >= MX_CACHE_MAX) {
    const firstKey = _mxCache.keys().next().value;
    _mxCache.delete(firstKey);
  }
  try {
    // Cloudflare DNS-over-HTTPS (gratis, no rate-limit razonable)
    const res = await fetch(`https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(domain)}&type=MX`, {
      headers: { "accept": "application/dns-json" },
      signal: AbortSignal.timeout(4000),
    });
    if (!res.ok) { _mxCache.set(domain, true); return true; } // si DNS falla, asumimos válido
    const data = await res.json();
    const has = Array.isArray(data?.Answer) && data.Answer.length > 0;
    _mxCache.set(domain, has);
    return has;
  } catch { _mxCache.set(domain, true); return true; }
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
  // 1) MX records check (gratis vía Cloudflare DoH) — descarta dominios sin mail server
  const dom = lower.split("@")[1];
  const hasMX = await _hasMxRecords(dom);
  if (!hasMX) return { color: "red", reason: "no_mx_records" };
  // 2) SMTP verify via eva.pingutil (free, no auth) — best-effort
  try {
    const res = await fetch(`https://api.eva.pingutil.com/email?email=${encodeURIComponent(lower)}`, {
      signal: AbortSignal.timeout(8000),
    });
    if (res.ok) {
      const data = await res.json();
      const valid = data?.status === "success" && data?.data?.valid_syntax === true;
      const deliverable = data?.data?.deliverable === true;
      const isDisposable = data?.data?.disposable === true;
      if (isDisposable) return { color: "red", reason: "disposable" };
      if (!valid)        return { color: "red", reason: "invalid_syntax_smtp" };
      if (!deliverable)  return { color: "red", reason: "undeliverable_smtp" };
    }
  } catch {} // si pingutil falla, seguimos con el check local
  if (GENERIC_LOCAL.test(lower)) return { color: "yellow", reason: "generic_address" };
  return { color: "green", reason: "ok" };
}

// Self-check Claude: verifica que el pitch no contradiga datos de input.
// Bajo costo (max 100 tokens). Si detecta inconsistencia, return false.
async function selfCheckPitch(token, pitch, ctx) {
  const { domain, adsTxtExists, adNetworks } = ctx;
  if (!pitch || pitch.length < 30) return { ok: false, reason: "pitch_too_short" };
  // Heurística rápida sin Claude: detectar contradicciones obvias.
  const lower = pitch.toLowerCase();
  if (adsTxtExists && /no.{1,10}(tienen|hay|tiene|tienes|have).{1,10}ads\.txt/i.test(pitch)) {
    return { ok: false, reason: "claims_no_ads_txt_but_has" };
  }
  if (adNetworks?.length > 0 && /no.{1,10}(detect|veo|see).{1,20}(monetiz|ad.{1,5}network|ad.{1,5}stack)/i.test(pitch)) {
    return { ok: false, reason: "claims_no_ads_but_has_networks" };
  }
  // Detectar meses hardcoded — usar word boundary para evitar matches dentro de
  // palabras (ej "mayo" en "mayoría", "may" en "maybe"/"mayor"/"essay")
  const months = /\b(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre|january|february|march|april|may|june|july|august|september|october|november|december)\b/i;
  if (months.test(pitch)) {
    return { ok: false, reason: "mentions_specific_month" };
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
    scope: "https://www.googleapis.com/auth/gmail.send",
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
    .split("\n\n")
    .map(p => `<p>${p.replace(/\n/g, "<br/>")}</p>`)
    .join("\n");
}

async function sendGmailServer(_token, userEmail, { to, subject, body }) {
  const accessToken = await getGmailAccessToken(userEmail);
  const signatureHtml = await getGmailSignatureHtmlServer(userEmail);

  // Subject RFC 2047 encoded para soportar acentos (ej. "monetización")
  const subjectEncoded = /^[\x20-\x7E]*$/.test(subject)
    ? subject
    : `=?UTF-8?B?${Buffer.from(subject, "utf-8").toString("base64")}?=`;

  let mime;
  if (signatureHtml && signatureHtml.trim()) {
    // Multipart: text + HTML con signature default del user
    const boundary = `----=adeq_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`;
    const htmlBody = `${_textToHtmlServer(body)}\n<br/>\n${signatureHtml}`;
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
async function pushToMondayServer(monday_api_key, payload, boardId) {
  // Crea item nuevo en Monday con todas las columnas. Usado solo cuando agent
  // decide enviar (no antes). Imita el botón "Push to Monday" del popup.
  // Shapes según config.js: geo/trafico/comentarios = text, no objetos.
  // email = { email, text }. status (estado/idioma) = { index } o { label }.
  // person = { personsAndTeams }. date = { date }.
  const cols = {
    [MONDAY_COL_TRAFFIC]:   payload.traffic_text || "",
    [MONDAY_COL_GEO]:       payload.geo || "",
    [MONDAY_COL_EMAIL]:     { email: payload.email, text: payload.email },
    [MONDAY_COL_DATE]:      { date: new Date().toISOString().split("T")[0] },
    [MONDAY_COL_IDIOMA]:    { index: payload.idioma_idx || 0 },
    [MONDAY_COL_ESTADO]:    { label: "Propuesta Vigente (T)" },
    ...(payload.monday_user_id ? { [MONDAY_COL_OWNER]: { personsAndTeams: [{ id: payload.monday_user_id, kind: "person" }] } } : {}),
    [MONDAY_COL_PITCH]:     payload.pitch_body ? `PITCH IA:\n${payload.pitch_body}` : "",
  };
  const query = `mutation ($board: ID!, $name: String!, $cols: JSON!) {
    create_item (board_id: $board, item_name: $name, column_values: $cols, create_labels_if_missing: true) { id }
  }`;
  const res = await fetch("https://api.monday.com/v2", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": monday_api_key, "API-Version": "2024-01" },
    body: JSON.stringify({
      query,
      variables: { board: boardId, name: payload.domain, cols: JSON.stringify(cols) },
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
  // Pausa fin de semana — operativo solo Lun-Vie España (consistente con csv + autopilot)
  if (_isWeekendSpain()) {
    log(`🤖 Agent: fin de semana España — sin envíos`);
    return;
  }

  const cfg = await getConfig(token);
  const aCfg = _agentCfg(cfg);
  const monday_api_key_default = cfg.monday_api_key || "";

  // Active hours check — fuera de 9-20 España no manda nada (ni Monday, ni mail)
  if (_isOutsideActiveHours(aCfg.activeStart, aCfg.activeEnd)) {
    log(`🤖 Agent: fuera de horario España (h=${_spainHour()}, activo=${aCfg.activeStart}-${aCfg.activeEnd})`);
    return;
  }
  log(`🤖 Agent: ciclo iniciando (users=${allFlags.agentUsers.length}, threshold=${aCfg.thresholdTraffic}, maxPerDay=${aCfg.maxPerDay})`);

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
    // Kill switch check
    if (await checkAgentKillSwitch(token, userEmail, aCfg)) continue;
    // Daily cap
    const sentToday = await getAgentDailyCount(token, userEmail);
    if (sentToday >= aCfg.maxPerDay) {
      log(`🤖 Agent ${userEmail}: cap diario ${aCfg.maxPerDay} alcanzado (${sentToday})`);
      continue;
    }
    // Weekly target (si configurado): cuenta sent en últimos 7 días
    if (aCfg.focus.weeklyTarget > 0) {
      const sentWeek = await getAgentWeeklyCount(token, userEmail);
      if (sentWeek >= aCfg.focus.weeklyTarget) {
        log(`🤖 Agent ${userEmail}: weekly target ${aCfg.focus.weeklyTarget} alcanzado (${sentWeek})`);
        continue;
      }
    }
    const remaining = aCfg.maxPerDay - sentToday;
    const batchSize = Math.min(aCfg.perCycleLimit, remaining);

    // Aplicar focus filtros al query
    const focus = aCfg.focus;
    let geoClause = "";
    if (focus.geosPriority.length > 0) {
      // Postgrest: geo=in.(AR,MX,...) — case insensitive imatch via OR
      const inList = focus.geosPriority.map(g => `"${g}"`).join(",");
      geoClause = `&geo=in.(${encodeURIComponent(inList)})`;
    } else if (focus.geosExcluded.length > 0) {
      const inList = focus.geosExcluded.map(g => `"${g}"`).join(",");
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
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.${aCfg.thresholdTraffic}${geoClause}${categoryClause}&select=*&order=score.desc.nullslast,created_at.desc&limit=${batchSize * 3}`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const candidatesRaw = await queueRes.json();
    if (!Array.isArray(candidatesRaw) || candidatesRaw.length === 0) {
      log(`🤖 Agent ${userEmail}: 0 candidatos (threshold=${aCfg.thresholdTraffic}, geos=${focus.geosPriority.join(",")||"all"}, cat=${focus.categoriesPriority.join(",")||"all"})`);
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
    // Ordenar por score desc (mejor stars primero), luego created_at desc
    scored.sort((a, b) => {
      if (b._scoreData.score !== a._scoreData.score) return b._scoreData.score - a._scoreData.score;
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
    });
    log(`🤖 Agent ${userEmail}: ${scored.length}/${candidatesRaw.length} candidatos válidos (top: ${scored[0].domain} ${scored[0]._scoreData.stars}★ score=${scored[0]._scoreData.score})`);

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
            // No pudimos fetchear página → fallback GEO/TLD
            const det = await detectLanguageRobust({ geo: leadGeo, domain }, { allowClaudeArbiter: false });
            leadLanguage = det.lang;
            log(`  🌐 ${domain}: lang=${det.lang} (${det.source}/${det.confidence}, sin html)`);
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

        // 1b. Si NO hay email decente, dispara Apollo (cache 7d)
        const hasGoodEmail = emails.some(e => e && /\@/.test(e) && !GARBAGE_LOCAL.test(e) && !GARBAGE_DOMAIN_PATTERN.test(e));
        // También filtrar emails garbage del array antes de elegir destino
        emails = emails.filter(e => e && /\@/.test(e) && !GARBAGE_LOCAL.test(e) && !GARBAGE_DOMAIN_PATTERN.test(e));
        if (!hasGoodEmail && cfg.apollo_api_key) {
          try {
            const apolloEmails = await findAllEmails(domain, cfg.apollo_api_key, token);
            if (Array.isArray(apolloEmails) && apolloEmails.length > 0) {
              const merged = [...new Set([...emails, ...apolloEmails])];
              const validated = await validateEmailsBatch(merged);
              emails = validated;
              await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
                method: "PATCH",
                headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
                body: JSON.stringify({ emails: validated }),
              });
              log(`  🔄 ${domain}: Apollo on-the-fly → ${validated.length} emails válidos (de ${merged.length} totales)`);
            }
          } catch (e) { log(`  ⚠️ on-the-fly Apollo ${domain}: ${e.message}`); }
        }

        // Re-pickear el MEJOR email después del enrichment (rankEmail sync, NO la async scoreEmail)
        const ranked = emails
          .map(e => ({ email: e, score: rankEmail(e, domain) }))
          .filter(x => x.score >= 0)
          .sort((a, b) => b.score - a.score);
        let email = ranked[0]?.email;

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
          await logAgentAction(token, userEmail, {
            domain, action: "skipped", reason: "no_email_after_enrichment",
            details: { traffic: leadTraffic, geo: leadGeo },
          });
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

        // 2. EMAIL SCORE — verifica antes de gastar Claude/template
        // 🔴 red = skip, 🟡 yellow = manda igual, 🟢 green = ideal
        const emailScore = await scoreEmail(email);
        if (emailScore.color === "red") {
          await logAgentAction(token, userEmail, {
            domain, action: "skipped",
            reason: `email_red_${emailScore.reason}`,
            details: { email, traffic: leadTraffic },
          });
          continue;
        }

        // 2. Decidir source: 80% template, 20% Claude (configurable via agent_claude_percent)
        const claudePercent = parseInt(cfg.agent_claude_percent || "20", 10);
        const source = pickPitchSource(claudePercent);
        let pitch;
        if (source === "claude") {
          // Variedad estilística — A/B test futuro
          pitch = await generatePitchAgent(token, {
            domain, traffic: leadTraffic, geo: leadGeo, language: lead.language || "en",
            category: lead.category, contactName: lead.contact_name,
            adNetworks: lead.ad_networks,
            userEmail, // para RAG retrieval — feedback del propio MB
          });
          // Self-check anti-alucinación solo cuando viene de Claude (templates están baked clean)
          const check = await selfCheckPitch(token, pitch.body, {
            domain, adsTxtExists: false, adNetworks: lead.ad_networks,
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
          // Template (80% de los casos) — sin costo Claude
          const tpl = pickRandomTemplate(lead.language);
          pitch = fillTemplate(tpl, {
            domain, geo: leadGeo, traffic: leadTraffic,
          });
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
            details: { email, traffic: leadTraffic, geo: leadGeo, language: lead.language },
          }),
        }).catch(() => null);
        const reservedId = (await reserveRes?.json().catch(() => null))?.[0]?.id;

        await sendGmailServer(token, userEmail, { to: email, subject, body: pitch.body });

        // PATCH reserved → sent ahora que confirmamos el envío
        if (reservedId) {
          fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions?id=eq.${reservedId}`, {
            method: "PATCH",
            headers: {
              "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
              "Content-Type": "application/json", "Prefer": "return=minimal",
            },
            body: JSON.stringify({ action: "sent" }),
          }).catch(() => {});
        }

        // 5. Push to Monday CON estado correcto desde el inicio (Propuesta Vigente T = idx 3)
        let mondayItemId = null;
        try {
          mondayItemId = await pushToMondayServer(mondayApiKey, {
            domain, email, geo: leadGeo, traffic_text: String(leadTraffic || ""),
            pitch_body: pitch.body, idioma_idx: ({ en:0, es:1, it:2, pt:3, ar:6 })[lead.language] ?? 0,
            monday_user_id: mondayUserId,
            estado_idx: 3, // Propuesta Vigente (T)
          }, AGENT_DEFAULTS.monday_board_id);
        } catch (mondayErr) {
          // Edge case raro: mail YA se mandó pero Monday falló.
          // Loggeamos como sent_no_monday para que admin pueda crear el item manual.
          // No es crítico — el lead recibió el pitch, solo falta tracking en CRM.
          log(`⚠️ Agent ${userEmail}: Gmail OK pero Monday falló para ${domain}: ${mondayErr.message}`);
          // NO duplicar action='sent' — ya quedó reservado pre-send.
          await logAgentAction(token, userEmail, {
            domain, action: "monday_failed", reason: "monday_push_error",
            pitch_subject: subject,
            details: { email, traffic: leadTraffic, geo: leadGeo, language: lead.language, monday_error: mondayErr.message?.substring(0, 200) },
          });
          // Igual marcamos sendtrack + review_queue para no re-mandar
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
        // Delay entre envíos para no parecer bot
        await sleep(15000 + Math.random() * 10000);

      } catch (err) {
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


  let token = null;
  let tokenExpiry = 0;

  while (!token) {
    try {
      token = await supabaseLogin();
      tokenExpiry = Date.now() + 55 * 60 * 1000;
      log("Login exitoso.");
    } catch (err) {
      log(`⚠️ Login fallido: ${err.message} — reintentando en 60s...`);
      await sleep(60_000);
    }
  }

  let idleSince = Date.now();
  log("📍 Entrando al main loop...");
  let iterCount = 0;

  while (true) {
    iterCount++;
    if (iterCount === 1 || iterCount % 10 === 0) log(`📍 Loop iter #${iterCount}`);
    try {
      if (Date.now() > tokenExpiry) {
        try {
          token = await supabaseLogin();
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
          runAgentCycle(token, flags).catch(e => log(`⚠️ runAgentCycle: ${e.message}`))
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
