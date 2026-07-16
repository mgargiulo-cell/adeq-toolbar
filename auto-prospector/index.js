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
import { KEYWORDS as _AG_KEYWORDS } from "./keywordsData.js";  // 3490 frases (12 idiomas) para AutoGoogle

const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY         = process.env.SUPABASE_ANON_KEY;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // bypass RLS (backend worker)
const SUPABASE_EMAIL            = process.env.SUPABASE_EMAIL;
const SUPABASE_PASSWORD         = process.env.SUPABASE_PASSWORD;
const CLOUDFLARE_API_TOKEN      = process.env.CLOUDFLARE_API_TOKEN || null; // optional: Radar country-indexed pool

// If a service-role key is set, use it as Bearer — bypasses RLS.
// Otherwise fall back to user JWT (won't see items uploaded by other users with RLS on).
const BACKEND_BEARER = SUPABASE_SERVICE_ROLE_KEY || null;
const SERPER_API_KEY = (process.env.SERPER_API_KEY || "").trim() || null;  // AutoGoogle: keyword→Google. Sin key = AutoGoogle apagado (no rompe nada).

const SESSION_LIMIT_MS  = 20 * 60 * 1000; // 20 minutos máx por sesión de autopilot — auto-corte
const POLL_INTERVAL_MS  = 30 * 1000;   // Maxi 2026-07-03 perf: 20s→30s. El loop drena TODA la cola por iteración (runCsvQueue con Infinity) y el agent procesa todo su pool, así que este intervalo es entre lulls, NO entre items — subirlo NO frena el drenado, pero baja ~33% la frecuencia de la maintenance por-iteración (getConfig refresh, counts de promoteWaitlist/feeder, heartbeat) = menos Disk IO sobre Supabase
const IDLE_INTERVAL_MS  = 120 * 1000;  // cuando autopilot está OFF (2 min)
const IDLE_EXIT_MS      = 4 * 60 * 60 * 1000; // 4h sin trabajo → exit (subido de 30min para evitar restarts frecuentes 2026-05-13)
const DOMAIN_DELAY_MS  = 2500;
// Maxi 2026-06-19: umbral = 350K PAGEVIEWS (no visits). El número del negocio
// son páginas vistas: si SimilarWeb da pageViews se usa ese, si solo da visits
// se estima visits × 2 (ppv promedio inventado). La columna `traffic` de
// review_queue ahora guarda PAGEVIEWS (antes guardaba visits crudos → los pisos
// downstream rechazaban/borraban sitios potables de 200-350K visits con buenos
// pageviews; ese era el bug de "queued → 0 to Prospects").
const MIN_TRAFFIC      = 350_000;  // pageViews mínimos para AUTOPILOT Majestic (descubrimiento)
const REVIEW_QUEUE_MIN_TRAFFIC = 350_000; // Floor absoluto en review_queue (en PAGEVIEWS). Items debajo se auto-borran.

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

// Maxi 2026-06-17 (audit #12): pool cacheado a nivel módulo (variable
// domainPool). 1 sola descarga por boot del worker (Railway redeploy ≈ 1x/día
// en práctica). No expira en memoria. Si Railway escala/reinicia, re-descarga.
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

// Maxi 2026-07-03: CACHE de config para bajar Disk IO sobre la base (nano/micro
// se quedaba sin IO budget → 522 recurrente). getConfig leía la tabla ENTERA en
// cada llamada, y se llama varias veces por iteración del loop (regla-de-oro +
// cfgShared + subfunciones). El cache colapsa esos reads redundantes en 1 cada
// CONFIG_TTL_MS. Se invalida al escribir (setConfigValue) → un valor recién seteado
// se ve al instante, sin staleness. TTL corto (30s) → los toggles del admin
// reaccionan en ≤30s, más que suficiente.
const CONFIG_TTL_MS = 30_000;
const FLAGS_TTL_MS  = 15_000;
let _cfgCache   = { at: 0, data: null };
let _flagsCache = { at: 0, data: null };
function _invalidateConfigCache() { _cfgCache = { at: 0, data: null }; _flagsCache = { at: 0, data: null }; }

async function getConfig(token) {
  if (_cfgCache.data && (Date.now() - _cfgCache.at) < CONFIG_TTL_MS) return _cfgCache.data;
  const res = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_config?select=key,value`, {
    headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` },
  });
  const rows = await res.json();
  const cfg = {};
  rows.forEach(r => { cfg[r.key] = r.value; });
  _cfgCache = { at: Date.now(), data: cfg };
  return cfg;
}

// Lectura liviana — flags de encendido, para el poll idle
async function getActiveFlags(token) {
  if (_flagsCache.data && (Date.now() - _flagsCache.at) < FLAGS_TTL_MS) return _flagsCache.data;
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
    const flags = {
      autopilot: map.auto_prospecting_enabled === "true",
      csvQueue:  map.csv_queue_enabled === "true",
      agent:     agentActive,
      agentUsers,
    };
    _flagsCache = { at: Date.now(), data: flags };
    return flags;
  } catch { return { autopilot: false, csvQueue: false, agent: false, agentUsers: [] }; }
}

async function isAutopilotEnabled(token) {
  const f = await getActiveFlags(token);
  return f.autopilot;
}

// Maxi 2026-07-03 perf: keys de las que deriva getActiveFlags — si se escribe una,
// el flags cache debe refrescarse (invalidación quirúrgica, no del config entero).
const _FLAG_CFG_KEYS = new Set(["auto_prospecting_enabled", "csv_queue_enabled", "agent_enabled_users", "agent_paused_until"]);
async function setConfigValue(token, key, value) {
  const _val = String(value);
  const _auth = {
    "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
    "Content-Type": "application/json",
  };
  // Maxi 2026-07-15 (BUG CRÍTICO): antes era SOLO PATCH. Un PATCH con ?key=eq.X sobre una
  // key que NO EXISTE matchea 0 filas y NO crea nada → se perdía en silencio (y encima el
  // cache in-memory de abajo la marcaba "escrita", enmascarando el fallo). Efecto: TODA key
  // nueva que el worker intenta crear en runtime (auto_heartbeat_at, autogoogle_stats,
  // auto_session_stats, last_source_perf_run…) nunca se persistía → el popup veía al worker
  // "muerto" aunque estuviera vivo, y las stats/latido quedaban null para siempre. Ahora:
  // PATCH (caso común: ya existe) y si matcheó 0 filas → INSERT (crear). return=representation
  // nos dice cuántas filas tocó el PATCH sin un round-trip extra.
  let _patched = [];
  try {
    const _r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.${encodeURIComponent(key)}`, {
      method: "PATCH",
      headers: { ..._auth, "Prefer": "return=representation" },
      body: JSON.stringify({ value: _val }),
    });
    if (_r.ok) _patched = await _r.json().catch(() => []);
  } catch {}
  if (!Array.isArray(_patched) || _patched.length === 0) {
    // No existía → crearla. Solo llegamos acá en el primer write de cada key nueva;
    // después el PATCH matchea y este INSERT no corre (writes de misma key son secuenciales
    // en el proceso único del worker, así que no hay carrera de duplicados).
    try {
      await fetch(`${SUPABASE_URL}/rest/v1/toolbar_config`, {
        method: "POST",
        headers: { ..._auth, "Prefer": "return=minimal" },
        body: JSON.stringify({ key, value: _val }),
      });
    } catch {}
  }
  // Maxi 2026-07-03 perf: antes _invalidateConfigCache() borraba el cache ENTERO →
  // el próximo getConfig re-leía toda la tabla toolbar_config. El loop escribe config
  // varias veces por iteración (heartbeat, review_queue_band_status, slots, counters…),
  // así que el cache quedaba SIEMPRE frío = full-table read de config en cada getConfig
  // (varios por iteración). Ahora actualizamos la entrada in-place: el worker ve su
  // propio write al instante SIN re-leer la tabla. El TTL (30s) igual refresca para
  // captar writes externos del admin. Ahorro IO/egress ALTO sobre toolbar_config.
  if (_cfgCache.data) _cfgCache.data[key] = String(value);
  // Los flags derivan de keys puntuales → invalidar SOLO ese cache barato (15s TTL)
  // si tocamos una, para no servir un flag viejo tras un toggle del propio worker.
  if (_FLAG_CFG_KEYS.has(key)) _flagsCache = { at: 0, data: null };
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
  // Rollover next_day → waiting_pool al cambiar el día (excedente diario espera al día
  // siguiente). Maxi 2026-07-01: RESPETAR el tope del waiting_pool (700). Antes promovía
  // TODO sin límite → el pool llegó a 1367. Ahora promueve solo hasta llenar 700; el
  // resto queda en next_day (regulación automática).
  if (_dayChanged) {
    try {
      const _auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
      let _wNow = 0;
      try {
        const wr = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.waiting_pool&select=id`,
          { headers: { ..._auth, "Prefer": "count=exact", "Range": "0-0" } });
        _wNow = parseInt((wr.headers.get("content-range") || "").match(/\/(\d+)$/)?.[1] || "0", 10);
      } catch {}
      const _room = Math.max(0, WAITING_POOL_CAP - _wNow);
      if (_room > 0) {
        const idsRes = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.next_day&order=uploaded_at.asc&limit=${_room}&select=id`, { headers: _auth });
        const ids = idsRes.ok ? (await idsRes.json().catch(() => [])).map(r => r.id) : [];
        if (ids.length) {
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?id=in.(${ids.join(",")})`, {
            method: "PATCH",
            headers: { ..._auth, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({ status: "waiting_pool" }),
          });
          log(`🌅 day rollover ${todaySpain}: promoted ${ids.length} next_day → waiting_pool (tope ${WAITING_POOL_CAP})`);
        }
      } else {
        log(`🌅 day rollover ${todaySpain}: waiting_pool lleno (${_wNow}/${WAITING_POOL_CAP}) — next_day espera`);
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

async function saveToReviewQueue(token, { domain, traffic, geo, geosAll, language, category, contactName, contactNameSource = "", contactPhone, emails, emailSources = {}, pitch, pitchSubject, pitchSubjects, score, adNetworks, pageTitle, createdBy, source = "autopilot", mondayItemId = null }) {
  // Maxi 2026-06-18: GUARD EXPLÍCITO contra leads sin tráfico. Antes saveToReviewQueue
  // confiaba en que el caller filtraba. Pero rows con NaN/0 se colaban. Ahora se
  // valida ACÁ — si traffic no es número >= MIN, no se guarda.
  const trafficNum = Number(traffic);
  // Maxi 2026-07-01: FLOOR DURO PARA TODOS. Se SACÓ la excepción monday_refresh/csv — el user
  // exige "solo pasan los +350K, las de menos ni se analizan", sin importar la fuente (incluidos
  // imports manuales y re-imports de Monday). Si el tráfico no es número ≥ MIN, no se guarda.
  if (!Number.isFinite(trafficNum) || trafficNum < REVIEW_QUEUE_MIN_TRAFFIC) {
    log(`  ⛔ saveToReviewQueue ${domain} REJECTED — traffic ${trafficNum} < ${REVIEW_QUEUE_MIN_TRAFFIC} (source=${source})`);
    return "floor";
  }

  // NOTA: el cap de 200 en review_queue se chequea EN LOS CALLERS antes de
  // llamar acá (csv worker → marca waiting_pool, autopilot → skip silent).
  // Esta función solo INSERTA y devuelve boolean.

  // Maxi 2026-06-17 (audit #7): dedup pre-insert. Antes el merge-duplicates de
  // Supabase dependía de un unique constraint que podría no existir → dups en
  // review_queue. Ahora chequeamos: si ya hay row pending con mismo domain,
  // skip insert (evita doble enriquecimiento + doble envío del agente).
  try {
    const dupRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?domain=eq.${encodeURIComponent(domain)}&status=eq.pending&select=id,source,created_at&order=created_at.desc&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (dupRes.ok) {
      const dupRows = await dupRes.json();
      if (Array.isArray(dupRows) && dupRows.length > 0) {
        log(`  ⏭️ saveToReviewQueue ${domain}: ya existe pending (id=${dupRows[0].id}, source=${dupRows[0].source || "?"}) — skip dup`);
        return "dup";
      }
    }
  } catch {}

  // Payload completo. Campos "core" (domain/traffic/status) son imprescindibles;
  // el resto es metadata que enriquece pero NO debe hacer caer el lead si falta la
  // columna en la base (ej: tras un reset que recreó la tabla sin alguna columna).
  const payload = {
    domain,
    traffic:        traffic ? Math.round(traffic) : 0,
    geo:            geo            || "",
    geos_all:       Array.isArray(geosAll) && geosAll.length ? geosAll : null,
    language:       language       || "",
    category:       category       || "",
    contact_name:   contactName    || "",
    // Maxi 2026-06-17 (audit #11): trackear de dónde viene el contact_name.
    contact_name_source: contactNameSource || (contactName ? "apollo" : ""),
    contact_phone:  contactPhone   || "",
    emails:         emails         || [],
    email_sources:  emailSources   || {},
    pitch:          pitch          || "",
    pitch_subject:  pitchSubject   || "",
    pitch_subjects: pitchSubjects  || [],
    score:          score          || 0,
    ad_networks:    adNetworks     || [],
    page_title:     pageTitle      || "",
    created_by:     createdBy      || "",
    source,
    monday_item_id: mondayItemId,
    status:         "pending",
  };
  // Maxi 2026-06-19 — INSERT AUTO-CURATIVO: si Postgres/PostgREST rechaza por una
  // columna inexistente (PGRST204), se quita ese campo y se reintenta. Así un reset
  // que borre una columna NUNCA más bloquea TODOS los leads en silencio (era el bug
  // que dejó Prospects en 0). Núcleo (domain/traffic/status) jamás se quita.
  const CORE = new Set(["domain", "traffic", "status"]);
  let body = { ...payload };
  for (let attempt = 0; attempt < 6; attempt++) {
    // on_conflict=domain: si el dominio YA existe (validated/rejected/etc.), en vez de
    // chocar con el unique (409/23505) hace UPSERT → re-activa esa fila a pending para
    // re-prospectar. El guard de envío 30d evita re-spamear. (Maxi 2026-06-22: antes
    // merge-duplicates upserteaba sobre la PK id, no sobre domain → 409 → leads perdidos.)
    const res = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?on_conflict=domain`, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
        "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify(body),
    });
    if (res.ok) {
      if (attempt > 0) log(`  ✓ saveToReviewQueue ${domain}: insertado tras saltear columna(s) faltante(s)`);
      return "ok";
    }
    const txt = await res.text().catch(() => "");
    // PGRST204: "Could not find the 'COL' column of '...' in the schema cache"
    const m = txt.match(/Could not find the '([^']+)' column/);
    if (res.status === 400 && m && (m[1] in body) && !CORE.has(m[1])) {
      log(`  ⚠️ saveToReviewQueue ${domain}: columna '${m[1]}' no existe en la base → reintento sin ella (auto-cura)`);
      delete body[m[1]];
      continue;
    }
    log(`  ❌ saveToReviewQueue ${domain} HTTP ${res.status}: ${txt.substring(0, 200)}`);
    return `http_${res.status}:${txt.substring(0, 60)}`;
  }
  return "http_max_retries";
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

  // Maxi 2026-07-03 costo: este job corre CADA iteración del main loop y gasta
  // RapidAPI (getTrafficData → hit facturado si no está cacheado) por lead. Antes
  // NO chequeaba el cap mensual/diario — solo confiaba en _rapidCapReached, que se
  // resetea a false al arrancar cada sesión de csv/autopilot. Con la cola parada
  // (csv/autopilot OFF) el flag quedaba en false → este loop podía gastar RapidAPI
  // pasado el cap MENSUAL sin ningún freno = runaway de costo. Ahora gate duro:
  // si el cap mensual o diario ya está alcanzado, no gasta ni un hit.
  try {
    const [rapidMonth, rapidDay] = await Promise.all([
      getRapidApiUsageThisMonth(token),
      getRapidApiUsageToday(token),
    ]);
    if (rapidMonth.usedThisMonth >= rapidMonth.limit) {
      log(`⛔ refreshEmpty: cap MENSUAL RapidAPI alcanzado (${rapidMonth.usedThisMonth}/${rapidMonth.limit}) — no gasto hits hasta próximo mes.`);
      return;
    }
    if (rapidDay.usedToday >= rapidDay.limit) {
      log(`⛔ refreshEmpty: cap DIARIO RapidAPI alcanzado (${rapidDay.usedToday}/${rapidDay.limit}) — no gasto hits hasta mañana.`);
      return;
    }
  } catch { /* si el chequeo falla, el fusible por-minuto de rapidFetchWithRetry sigue protegiendo */ }

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
const FEEDER_RQ_SATURATION = 3000;                  // Maxi 2026-07-01: 2000→3000. Hoy el conteo está inflado por leads SIN email (71%) que se están reprocesando; darle aire para no frenar el feeder mientras drenan.
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
  // dailymotion.com removido 2026-06-19: su sellers.json está caído (404 confirmado root+www).
  // Agregadas user 2026-05-29 — validadas por HTTP (sellers.json real con publishers).
  // Native/discovery + mobile SDKs + gestión de publishers + LATAM/ES.
  "https://taboola.com/sellers.json",
  "https://outbrain.com/sellers.json",
  "https://nativo.com/sellers.json",
  "https://applovin.com/sellers.json",
  "https://ironsrc.com/sellers.json",
  "https://inmobi.com/sellers.json",
  "https://smaato.com/sellers.json",
  "https://mintegral.com/sellers.json",
  "https://chartboost.com/sellers.json",
  "https://digitalturbine.com/sellers.json",   // cubre Fyber (misma data)
  "https://buysellads.com/sellers.json",
  "https://gourmetads.com/sellers.json",
  "https://e-planning.net/sellers.json",
  "https://mediavine.com/sellers.json",
  "https://adthrive.com/sellers.json",         // = Raptive
  "https://monetizemore.com/sellers.json",
  "https://nitropay.com/sellers.json",
  "https://snack-media.com/sellers.json",
  "https://freestar.io/sellers.json",
  "https://playwire.com/sellers.json",
  "https://publift.com/sellers.json",
  "https://underdogmedia.com/sellers.json",
  "https://venatus.com/sellers.json",
  "https://sunmedia.tv/sellers.json",
  // Agregadas user 2026-05-29 (2da tanda) — LATAM + video. Validadas por HTTP.
  "https://adsmovil.com/sellers.json",        // mobile LATAM
  "https://connatix.com/sellers.json",        // video
  "https://glomex.com/sellers.json",          // video editorial
  "https://primis.tech/sellers.json",         // video nativo (VideoByte)
  "https://unrulymedia.com/sellers.json",     // Unruly — video premium
  // ─────────────────────────────────────────────────────────────────────────
  // TANDA IAB EUROPA + LATAM (user 2026-06-19). 105 fuentes nuevas de directorios
  // IAB (ES/IT/FR/PT/UK/IE/NL/BE/DE/AT/CH/PL/Nórdicos/CEE/GR/TR + LATAM) y lista
  // manual del user. TODAS verificadas por script: sellers.json con >=10 entradas
  // seller_type=PUBLISHER con domain (criterio optad360=2482). # publishers al lado.
  "https://www.reklamstore.com/sellers.json",              // 19954 pub
  "https://static.cdn.admatic.com.tr/sellers/sellers.json",// 19133 pub
  "https://unity.com/sellers.json",                        // 11143 pub
  "https://loopme.com/sellers.json",                       // 2854 pub (curl-only)
  "https://pubgalaxy.com/sellers.json",                    // 3683 pub
  "https://premiumads.com.br/sellers.json",                // 1724 pub
  "https://projectagora.com/sellers.json",                 // 1576 pub
  "https://smartframe.io/sellers.json",                    // 1565 pub
  "https://simpleads.com.br/sellers.json",                 // 1446 pub
  "https://criteo.com/sellers.json",                       // 1209 pub
  "https://adwmg.com/sellers.json",                        // 1189 pub
  "https://holid.io/sellers.json",                         // 1090 pub
  "https://freewheel.com/sellers.json",                    // 1066 pub (curl-only)
  "https://adpone.com/sellers.json",                       // 1021 pub
  "https://www.dazn.com/sellers.json",                     // 981 pub
  "https://adhese.com/sellers.json",                       // 909 pub
  "https://adnimation.com/sellers.json",                   // 837 pub
  "https://richaudience.com/sellers.json",                 // 825 pub
  "https://exte.com/sellers.json",                         // 825 pub
  "https://adops.gr/sellers.json",                         // 744 pub
  "https://ogury.com/sellers.json",                        // 734 pub (curl-only)
  "https://vistarmedia.com/sellers.json",                  // 722 pub
  "https://movingup.it/sellers.json",                      // 661 pub
  "https://odeeo.io/sellers.json",                         // 640 pub
  "https://evolutionadv.it/sellers.json",                  // 626 pub
  "https://footballco.com/sellers.json",                   // 581 pub
  "https://optidigital.com/sellers.json",                  // 537 pub
  "https://audienzz.ch/sellers.json",                      // 502 pub
  "https://clickio.com/sellers.json",                      // 493 pub
  "https://r2b2.cz/sellers.json",                          // 487 pub
  "https://broadsign.com/sellers.json",                    // 484 pub
  "https://harrenmedia.com/sellers.json",                  // 456 pub
  "https://streamlyn.com/sellers.json",                    // 449 pub
  "https://webads.nl/sellers.json",                        // 425 pub
  "https://onlinemediasolutions.com/sellers.json",         // 425 pub
  "https://www.phaistosnetworks.gr/sellers.json",          // 405 pub
  "https://strossle.com/sellers.json",                     // 377 pub
  "https://tappx.com/sellers.json",                        // 375 pub
  "https://dianomi.com/sellers.json",                      // 368 pub
  "https://stroeer.com/sellers.json",                      // 366 pub
  "https://joinads.me/sellers.json",                       // 366 pub
  "https://adsocy.com/sellers.json",                       // 336 pub
  "https://soundstack.com/sellers.json",                   // 317 pub
  "https://voisetech.com/sellers.json",                    // 316 pub
  "https://adweb.gr/sellers.json",                         // 313 pub
  "https://yoc.com/sellers.json",                          // 312 pub
  "https://onetag.com/sellers.json",                       // 310 pub
  "https://refinery89.com/sellers.json",                   // 309 pub
  "https://adswizz.com/sellers.json",                      // 304 pub
  "https://alkimi.org/sellers.json",                       // 298 pub
  "https://amagi.com/sellers.json",                        // 264 pub
  "https://overwolf.com/sellers.json",                     // 259 pub
  "https://iion.io/sellers.json",                          // 258 pub
  "https://adform.com/sellers.json",                       // 245 pub
  "https://connectad.io/sellers.json",                     // 233 pub
  "https://entravision.com/sellers.json",                  // 230 pub
  "https://brightcom.com/sellers.json",                    // 218 pub
  "https://yieldlove.com/sellers.json",                    // 215 pub
  "https://wurl.com/sellers.json",                         // 201 pub (curl-only)
  "https://alright.com.br/sellers.json",                   // 151 pub
  "https://otzads.net/sellers.json",                       // 134 pub
  "https://stroeer.de/sellers.json",                       // 132 pub
  "https://digohispanicmedia.com/sellers.json",            // 121 pub
  "https://seznam.cz/sellers.json",                        // 115 pub
  "https://flower-ads.com/sellers.json",                   // 111 pub
  "https://adverty.com/sellers.json",                      // 105 pub
  "https://undertone.com/sellers.json",                    // 96 pub
  "https://adtonos.com/sellers.json",                      // 88 pub
  "https://adasta.it/sellers.json",                        // 83 pub
  "https://mediasquare.fr/sellers.json",                   // 81 pub
  "https://gazeta.pl/sellers.json",                        // 77 pub
  "https://contentignite.com/sellers.json",                // 73 pub
  "https://iprom.si/sellers.json",                         // 73 pub
  "https://yieldlab.net/sellers.json",                     // 70 pub
  "https://sapo.pt/sellers.json",                          // 66 pub
  "https://next14.com/sellers.json",                       // 65 pub
  "https://massarius.com/sellers.json",                    // 60 pub
  "https://ividence.com/sellers.json",                     // 59 pub
  "https://ozoneproject.com/sellers.json",                 // 51 pub (curl-only)
  "https://wp.pl/sellers.json",                            // 48 pub
  "https://livewrapped.com/sellers.json",                  // 37 pub
  "https://relevant-digital.com/sellers.json",             // 35 pub
  "https://beintoo.com/sellers.json",                      // 32 pub
  "https://cpex.cz/sellers.json",                          // 32 pub
  "https://www.rakuten.tv/sellers.json",                   // 31 pub
  "https://ringier-advertising.ch/sellers.json",           // 28 pub
  "https://hubvisor.io/sellers.json",                      // 27 pub
  "https://russmedia.com/sellers.json",                    // 27 pub
  "https://adtarget.biz/sellers.json",                     // 27 pub
  "https://sibboventures.com/sellers.json",                // 25 pub
  "https://newixmedia.com/sellers.json",                   // 22 pub
  "https://arbomedia.ro/sellers.json",                     // 22 pub
  "https://prismamedia.com/sellers.json",                  // 20 pub
  "https://smartstream.tv/sellers.json",                   // 20 pub
  "https://adssets.com/sellers.json",                      // 20 pub
  "https://proximus.be/sellers.json",                      // 15 pub
  "https://manzoniadvertising.it/sellers.json",            // 14 pub
  "https://bluebillywig.com/sellers.json",                 // 14 pub
  "https://italiaonline.it/sellers.json",                  // 13 pub
  "https://366.fr/sellers.json",                           // 13 pub
  "https://ad-alliance.de/sellers.json",                   // 13 pub
  "https://produpress.be/sellers.json",                    // 12 pub
  "https://canelamedia.com/sellers.json",                  // 11 pub
  "https://emetriq.com/sellers.json",                      // 11 pub
  "https://wemass.com/sellers.json",                       // 10 pub
  // Fuentes "chicas" pero que listan TERCEROS (user 2026-06-19: "todo suma, no publishers
  // que se listan a sí mismos"). Verificadas: >=2 dominios de terceros en su sellers.json.
  "https://cdn.nativery.com/widget/js/sellers.json",       // 131 terceros
  "https://pebblemedia.be/sellers.json",                   // 9 terceros (BE sales house)
  "https://adsanddata.be/sellers.json",                    // 9 terceros (BE)
  "https://seven.one/sellers.json",                        // 8 terceros (DE)
  "https://onedio.com/sellers.json",                       // 7 terceros (TR)
  "https://first-id.fr/sellers.json",                      // 6 terceros (FR)
  "https://logan.ai/sellers.json",                         // 6 terceros (LATAM)
  "https://bonniernews.se/sellers.json",                   // 6 terceros (SE)
  "https://www.mediamond.it/sellers.json",                 // 4 terceros (IT)
  "https://admoai.com/sellers.json",                       // 4 terceros (LATAM)
  "https://dpgmedia.be/sellers.json",                      // 4 terceros (BE)
  "https://sabah.com.tr/sellers.json",                     // 3 terceros (TR)
  "https://invidi.com/sellers.json",                       // 3 terceros (Nórdico/CTV)
  "https://impresa.pt/sellers.json",                       // 3 terceros (PT)
  "https://www.samsung.com/sellers.json",                  // 2 terceros
  "https://rmb.be/sellers.json",                           // 2 terceros (BE)
  "https://dexerto.com/sellers.json",                      // 2 terceros (UK)
  // ═════════════════════════════════════════════════════════════════════════
  // TANDA 2 — BÚSQUEDA GLOBAL (user 2026-06-19). 95 fuentes nuevas de 7 agentes
  // exhaustivos (IAB/LinkedIn/Google/DMEXCO/affbank) por continente. Todas
  // verificadas por script: >=10 entradas PUBLISHER/BOTH con domain de terceros.
  // ── 🌏 ASIA-PACÍFICO ──
  "https://i-mobile.co.jp/sellers.json",
  "https://ucfunnel.com/sellers.json",
  "https://aralego.com/sellers.json",
  "https://auxoads.com/sellers.json",
  "https://greedygame.com/sellers.json",
  "https://vertoz.com/sellers.json",
  "https://adop.cc/sellers.json",
  "https://ad-stir.com/sellers.json",
  "https://microad.co.jp/sellers.json",
  "https://ad-generation.jp/sellers.json",
  "https://vdo.ai/sellers.json",
  "https://adingo.jp/sellers.json",
  "https://yeahmobi.com/sellers.json",
  "https://innity.com/sellers.json",
  "https://playstream.media/sellers.json",
  "https://fout.jp/sellers.json",
  "https://momagic.com/sellers.json",
  "https://admicro.vn/sellers.json",
  "https://pokkt.com/sellers.json",
  "https://xapads.com/sellers.json",
  "https://logly.co.jp/sellers.json",
  "https://adgebra.co/sellers.json",
  "https://vuukle.com/sellers.json",
  "https://adpopcorn.com/sellers.json",
  "https://geniee-ssp.net/sellers.json",
  "https://playground.xyz/sellers.json",
  "https://vidcrunch.com/sellers.json",
  "https://adview.com/sellers.json",
  "https://adpushup.com/sellers.json",
  "https://zmaticoo.com/sellers.json",
  // ── 🌍 MENA + ÁFRICA + ISRAEL ──
  "https://foxpush.com/sellers.json",
  "https://adintop.com/sellers.json",
  "https://arabyads.com/sellers.json",
  "https://adlive.io/sellers.json",
  "https://andbeyond.media/sellers.json",
  "https://dochase.com/sellers.json",
  "https://kueez.com/sellers.json",
  "https://kueezrtb.com/sellers.json",
  "https://mobupps.com/sellers.json",
  // ── 🌎 LATAM ──
  "https://wortise.com/sellers.json",
  "https://denakop.com/sellers.json",
  "https://nobeta.com.br/sellers.json",
  "https://audienciad.com/sellers.json",
  "https://membrana.media/sellers.json",
  "https://juicebarads.com/sellers.json",
  "https://adzep.com.br/sellers.json",
  "https://grumft.com/sellers.json",
  // ── 🇪🇺 EUROPA ──
  "https://rtbhouse.com/sellers.json",
  "https://4wmarketplace.com/sellers.json",
  "https://sublime.xyz/sellers.json",
  "https://justpremium.com/sellers.json",
  "https://quantum-advertising.com/sellers.json",
  "https://cwire.com/sellers.json",
  "https://bidmachine.io/sellers.json",
  "https://insticator.com/sellers.json",
  "https://adapex.io/sellers.json",
  "https://sevio.com/sellers.json",
  "https://stailamedia.com/sellers.json",
  "https://betweendigital.com/sellers.json",
  "https://smartclip.net/sellers.json",
  "https://madvertise.com/sellers.json",
  "https://adverline.com/sellers.json",
  "https://admixer.net/sellers.json",
  "https://admixer.com/sellers.json",
  "https://eskimi.com/sellers.json",
  "https://nativery.com/sellers.json",
  "https://venatusmedia.com/sellers.json",
  "https://themediagrid.com/sellers.json",
  "https://www.targetspot.com/sellers.json",
  // ── 🌎 NORTEAMÉRICA / GLOBAL ──
  "https://33across.com/sellers.json",
  "https://media.net/sellers.json",
  "https://appnexus.com/sellers.json",
  "https://conversantmedia.com/sellers.json",
  "https://adcolony.com/sellers.json",
  "https://fyber.com/sellers.json",
  "https://nextmillennium.io/sellers.json",
  "https://lkqd.com/sellers.json",
  "https://minutemedia.com/sellers.json",
  "https://gumgum.com/sellers.json",
  "https://yieldmo.com/sellers.json",
  "https://beachfront.com/sellers.json",
  "https://kargo.com/sellers.json",
  "https://brid.tv/sellers.json",
  "https://sonobi.com/sellers.json",
  "https://publir.com/sellers.json",
  "https://springserve.com/sellers.json",
  "https://smartyads.com/sellers.json",
  "https://onomagic.com/sellers.json",
  "https://pulsepoint.com/sellers.json",
  "https://adsparc.com/sellers.json",
  "https://pixfuture.com/sellers.json",
  "https://adsyield.com/sellers.json",
  "https://mediafuse.com/sellers.json",
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
  // COBERTURA PROGRESIVA (Maxi 2026-06-22): el feeder también recorre los JSON sin
  // repetir. Bloquea si el dominio YA pasó por el sistema — cualquier fila en la cola
  // (cualquier status) o en Prospects. Así no re-analiza lo ya visto y avanza a lo
  // nuevo. Re-prospect de finalizados = flujo Monday aparte. Tras reset total, todo
  // vuelve a ser nuevo. (Sincronizado con el import manual en modules/sellersJson.js.)
  const tables = [
    { table: "toolbar_csv_queue",     col: "domain", filter: "" },
    { table: "toolbar_review_queue",  col: "domain", filter: "" },
  ];
  for (let i = 0; i < candidates.length; i += BATCH) {
    const slice = candidates.slice(i, i + BATCH);
    const inList = slice.map(d => `"${d.replace(/"/g, '\\"')}"`).join(",");
    await Promise.all(tables.map(async ({ table, col, filter }) => {
      try {
        const res = await fetch(
          `${SUPABASE_URL}/rest/v1/${table}?${col}=in.(${encodeURIComponent(inList)})&select=${col}${filter || ""}`,
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
      `${SUPABASE_URL}/rest/v1/toolbar_feeder_runs?cron_at=gte.${dateISO}T00:00:00&select=effective_added&order=cron_at.asc`, // Maxi 2026-07-03 perf: select=* → solo effective_added (único campo usado por el caller)
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch { return []; }
}

// Maxi 2026-07-03 perf: cache TTL 30s. Es un count=exact sobre toolbar_review_queue
// (tabla grande) que se llamaba en CADA iteración del loop para el band-status + el
// gate de saturación del feeder. Ese count fuerza scan del índice pending cada 20-30s.
// El gate/banda toleran 30s de staleness sin problema (son umbrales, no precisión).
let _rqValidCountCache = { at: 0, n: 0 };
async function _getReviewQueueValidCount(token) {
  if (Date.now() - _rqValidCountCache.at < 30_000) return _rqValidCountCache.n;
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.${REVIEW_QUEUE_MIN_TRAFFIC}&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("content-range") || "";
    const n = parseInt(range.match(/\/(\d+)$/)?.[1] || "0", 10);
    _rqValidCountCache = { at: Date.now(), n };
    return n;
  } catch { return _rqValidCountCache.n || 0; }
}

// Maxi 2026-06-22: BACKLOG de la cola csv (pending+processing+waiting_pool+next_day).
// Gate de saturación: el feeder/autopilot/autogoogle NO cargan más mientras este
// número esté alto → la cola drena antes de volver a llenarse (pedido del user: la
// cola llegó a ~1500 en espera). Resume recién cuando baja del umbral LOW.
const CSV_QUEUE_HALT_HIGH = 250; // Maxi 2026-07-01: 150→250. El backlog activo ahora cuenta solo pending+processing, y pending clava en su tope (200) → con 150 el feeder frenaba SIEMPRE (200>150) aunque hubiera lugar. La regulación real ahora son los carriles por fuente + el tope de waiting_pool (700); este gate solo evita amontonar cuando pending está realmente lleno.
// Maxi 2026-07-01: BUG que frenaba TODOS los motores (feeder + autopilot + autogoogle).
// Antes contaba pending+processing+waiting_pool+next_day. waiting_pool (cap 300) y next_day
// (ILIMITADO) son buffers DIFERIDOS que se promueven solos cuando pending drena — pero al
// apilarse mantenían el backlog >150 PARA SIEMPRE → el intake quedaba bloqueado y ni el
// feeder ni AutoGoogle traían webs nuevas (skipped_saturated 25/25, autogoogle 0 búsquedas).
// Ahora solo cuenta el trabajo ACTIVO (pending+processing): si el worker tiene <150 items
// para procesar, los motores siguen descubriendo. El crecimiento lo limitan el hard cap de
// pending (200), el target diario del feeder y el cap mensual de AutoGoogle.
async function _getCsvQueueBacklog(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=in.(pending,processing)&select=id`,
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

// Maxi 2026-07-01: CARRIL POR FUENTE + tope global del waiting_pool.
// Cada motor tiene su cupo ACTIVO propio (pending+processing+waiting_pool+next_day) para
// que un sellers.json no le tape el lugar a AutoGoogle/autopilot. Si una fuente llenó su
// carril, no inyecta más → regulación automática + podemos analizar qué arrojó cada uno.
const WAITING_POOL_CAP = 700;                       // el pool no debe pasar de esto (user 2026-07-01)
const PER_SOURCE_ACTIVE_CAP = {
  auto_feeder_sellers:  250,
  auto_feeder_majestic: 150,   // autopilot (majestic/similar)
  auto_feeder_adstxt:   120,   // autopilot (ads.txt-graph)
  auto_feeder_monday:   120,
  autogoogle:           180,   // carril RESERVADO — nunca lo tapa un JSON
};
const DEFAULT_SOURCE_CAP = 150;

async function _countActiveCsvBySource(token, sourceTag) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=in.(pending,processing,waiting_pool,next_day)&source=eq.${encodeURIComponent(sourceTag)}&select=id`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    return parseInt((res.headers.get("content-range") || "").match(/\/(\d+)$/)?.[1] || "0", 10);
  } catch { return 0; }
}

async function _injectIntoCsvQueue(token, domains, sourceTag) {
  if (!domains || domains.length === 0) return 0;
  // CARRIL de la fuente: si ya llenó su cupo activo, no inyectar más.
  const laneCap  = PER_SOURCE_ACTIVE_CAP[sourceTag] ?? DEFAULT_SOURCE_CAP;
  const laneUsed = await _countActiveCsvBySource(token, sourceTag);
  const laneRoom = Math.max(0, laneCap - laneUsed);
  if (laneRoom <= 0) { log(`⏸️ ${sourceTag} SKIP inject: carril lleno (${laneUsed}/${laneCap})`); return 0; }
  if (domains.length > laneRoom) domains = domains.slice(0, laneRoom);
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
  const slotsWaiting = Math.max(0, WAITING_POOL_CAP - waitingNow);
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

// ════════════════════════════════════════════════════════════════════════════
// AUTOGOOGLE (user 2026-06-19) — motor SEPARADO del autopilot. Busca keywords en
// Google vía Serper.dev, extrae los dominios de los resultados orgánicos (web
// abierta, plata fresca por TEMA), y los encola con source="autogoogle" → mismo
// dedup canónico + filtro 350K + filtro basura que todo lo demás. El filtro
// "AutoGoogle" en Prospects mide cuántos llegan a Prospects desde este motor vs
// el autopilot. Apagado si no hay SERPER_API_KEY (env de Railway).
const AUTOGOOGLE_SLOTS = [10, 11, 14, 17, 21];  // Maxi 2026-07-15 (F4): 8→10 (el slot 8:00 nunca corría por el gate off-hours 9-23). Offset del feeder (9/12/15/18/20).
const AUTOGOOGLE_MONTHLY_CAP = 2500;           // techo Serper/mes (plan FREE de Serper); el pacing lo reparte por días
let _autoGoogleLastSlot = "";
const _AUTOGOOGLE_KEYWORDS = [
  "breaking news today","football transfer news","stock market today","crypto news today","best gaming laptops","healthy dinner recipes","weight loss tips","travel destinations 2025","movie reviews","celebrity gossip","nba highlights","premier league news","personal finance tips","ai news today","smartphone reviews","home workout routine","car reviews 2025","real estate market news","budget travel tips","best tv series to watch",
  "noticias de hoy","fichajes futbol","precio del bitcoin","recetas faciles","consejos de salud","resultados liga","analisis politico","mejores celulares","horoscopo de hoy","peliculas estreno","tips de viaje","mercado inmobiliario","finanzas personales","noticias deportivas","tutoriales tecnologia",
  "noticias de hoje","futebol ao vivo","preco bitcoin","receitas faceis","dicas de saude","resultados brasileirao","analise politica","melhores celulares","filmes lancamento","dicas de viagem",
  "notizie di oggi","calciomercato","prezzo bitcoin","ricette facili","consigli salute","risultati serie a","recensioni film","migliori smartphone","viaggi economici","oroscopo oggi",
  "actualites du jour","mercato football","prix bitcoin","recettes faciles","conseils sante","resultats ligue 1","critiques films","meilleurs smartphones","idees voyage","horoscope du jour",
  "nachrichten heute","fussball transfers","bitcoin preis","einfache rezepte","gesundheitstipps","bundesliga ergebnisse","filmkritiken","beste smartphones","reisetipps","horoskop heute",
];

// Países de búsqueda (Serper `gl`) para targeting GEO — NO-Anglo (LATAM + EU + TR/GR).
// Simula buscar DESDE ese país → Google prioriza publishers de/para ese país.
const _AUTOGOOGLE_GL = ["mx", "ar", "br", "cl", "co", "pe", "uy", "es", "it", "fr", "de", "nl", "pt", "pl", "tr", "gr", "se", "be", "ch"];

// Pega una keyword a Serper (con país de búsqueda gl) y devuelve los dominios orgánicos.
// Maxi 2026-07-15 (auditoría AutoGoogle): devuelve {domains, ok, status} para que el caller distinga
// éxito de error (antes tragaba todo a [] → "cero silencioso"). + timeout 10s (era el ÚNICO fetch del
// hot-path sin timeout → un cuelgue de Serper bloqueaba el loop y starvaba el heartbeat).
async function _serperSearch(query, num = 20, gl = "") {
  if (!SERPER_API_KEY) return { domains: [], ok: false, status: 0 };
  try {
    const body = { q: query, num };
    if (gl) body.gl = gl;  // país de búsqueda → prioriza publishers de ese país (targeting GEO)
    const res = await fetch("https://google.serper.dev/search", {
      method: "POST",
      headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) { log(`  ⚠️ Serper ${res.status} para "${query}"`); return { domains: [], ok: false, status: res.status }; }
    const data = await res.json();
    const out = [];
    for (const r of (Array.isArray(data.organic) ? data.organic : [])) {
      try {
        const h = new URL(r.link).hostname.toLowerCase().replace(/^www\./, "");
        if (h && h.includes(".")) out.push(h);
      } catch {}
    }
    return { domains: [...new Set(out)], ok: true, status: 200 };
  } catch (e) { log(`  ⚠️ Serper error "${query}": ${e.message}`); return { domains: [], ok: false, status: -1 }; }
}

// Maxi 2026-07-16: FALLBACK de contacto por Google. El user mostró que muchos sitios (BR/GR/…) tienen
// SOLO un formulario "fale conosco"/contato (sin email visible: massa.com.br), pero Google indexa el
// email/teléfono/WhatsApp en los snippets. Cuando el scraping normal NO encontró email, gastamos 1
// búsqueda Serper "<dominio> contato/publicidade/email/telefone" y extraemos email (mismo dominio) +
// teléfono + WhatsApp de los organic/knowledgeGraph/answerBox. Cost-control: SOLO se llama si no hay email.
async function _serperContactSearch(domain) {
  if (!SERPER_API_KEY) return { emails: [], phones: [], whatsapps: [] };
  const clean = String(domain || "").replace(/^www\./, "").toLowerCase();
  const base = clean.split(".")[0];
  try {
    const res = await fetch("https://google.serper.dev/search", {
      method: "POST",
      headers: { "X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ q: `${clean} contato OR contacto OR contact OR publicidade OR email OR telefone OR whatsapp`, num: 10 }),
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) { log(`  ⚠️ Serper contact ${res.status} ${clean}`); return { emails: [], phones: [], whatsapps: [] }; }
    const data = await res.json();
    let text = "";
    for (const r of (Array.isArray(data.organic) ? data.organic : [])) text += ` ${r.title || ""} ${r.snippet || ""} ${r.link || ""}`;
    if (data.knowledgeGraph) { const kg = data.knowledgeGraph; text += ` ${kg.title || ""} ${kg.description || ""} ${kg.phoneNumber || ""} ${kg.website || ""} ${JSON.stringify(kg.attributes || {})}`; }
    if (data.answerBox) text += ` ${data.answerBox.snippet || ""} ${data.answerBox.answer || ""}`;
    // Emails: SOLO del mismo dominio registrable (evita agarrar mails de otros sitios del ranking).
    const emails = extractEmailsFromHtml(text).filter(e => {
      const host = (e.split("@")[1] || "").toLowerCase();
      return host === clean || host.endsWith("." + clean) || host === base + "." + clean.split(".").slice(1).join(".") || (base.length >= 4 && host.split(".")[0] === base);
    });
    const { phones, whatsapps } = extractPhonesFromHtml(text);
    return { emails: [...new Set(emails)], phones, whatsapps };
  } catch (e) { log(`  ⚠️ Serper contact error ${clean}: ${e.message}`); return { emails: [], phones: [], whatsapps: [] }; }
}

async function maybeRunAutoGoogleSlot(token) {
  if (!SERPER_API_KEY) return;  // sin key → AutoGoogle apagado
  const { hour, weekday, dateISO } = _madridNowParts();
  if (weekday === "Sat" || weekday === "Sun") return;  // solo lunes a viernes
  if (!AUTOGOOGLE_SLOTS.includes(hour)) return;
  const slotLabel = `${dateISO}-${hour}:00`;
  if (_autoGoogleLastSlot === slotLabel) return;
  // Maxi 2026-07-01: AutoGoogle tiene VÍA PROPIA más laxa. Trae publishers frescos de
  // Google (no las listas ad-tech del backlog del feeder) y se autolimita con su cap
  // mensual (2500 búsquedas). Antes lo frenaba el mismo gate que el feeder (150) → con
  // el backlog acumulado nunca corría. Ahora solo frena si el trabajo ACTIVO (pending+
  // processing) está MUY alto (400) — que con el hard cap de pending (200) casi no pasa,
  // así corre en paralelo al feeder como pediste.
  const AUTOGOOGLE_HALT = 400;
  const _bl = await _getCsvQueueBacklog(token);
  if (_bl >= AUTOGOOGLE_HALT) { log(`🔎 AutoGoogle SKIP: trabajo activo ${_bl} (>${AUTOGOOGLE_HALT})`); return; }
  _autoGoogleLastSlot = slotLabel;
  try { await _runAutoGoogleSlot(token, slotLabel); }
  catch (e) { log(`⚠️ AutoGoogle slot: ${e.message}`); }
}

async function _runAutoGoogleSlot(token, slotLabel) {
  const { hour, dateISO } = _madridNowParts();
  const period = dateISO.slice(0, 7);  // YYYY-MM
  // ── PACING MENSUAL: reparte el cap (20k búsquedas) entre los días/slots que faltan
  //    del mes, para NO agotar todo en un día (clave para el costo de server). ──
  // Maxi 2026-07-15: cap mensual CONFIGURABLE (autogoogle_monthly_cap) sin deploy — con 50k créditos
  // pagos, el user regula el ritmo de gasto. + freshRate = rolling de dominios NUEVOS por búsqueda,
  // usado abajo para NO gastar créditos cuando el pool está saturado (spend inteligente).
  let used = 0, monthlyCap = AUTOGOOGLE_MONTHLY_CAP, freshRate = 1.0;
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(autogoogle_serper_used,autogoogle_serper_period,autogoogle_monthly_cap,autogoogle_fresh_rate)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (r.ok) {
      const map = {}; (await r.json()).forEach(x => { map[x.key] = x.value; });
      if (map.autogoogle_serper_period === period) used = parseInt(map.autogoogle_serper_used || "0", 10) || 0;
      if (map.autogoogle_monthly_cap) monthlyCap = Math.max(100, parseInt(map.autogoogle_monthly_cap, 10) || AUTOGOOGLE_MONTHLY_CAP);
      if (map.autogoogle_fresh_rate) freshRate = Math.max(0, Math.min(3, parseFloat(map.autogoogle_fresh_rate) || 1.0));
    }
  } catch {}
  const remaining = Math.max(0, monthlyCap - used);
  if (remaining <= 0) { log(`🔎 AutoGoogle: cap mensual ${monthlyCap} alcanzado (${used}) — skip`); return; }
  // Maxi 2026-07-15: PRE-CHECK del carril ANTES de gastar Serper. Bug en logs: se hacían las 200
  // búsquedas (200 créditos) y RECIÉN al inyectar aparecía "carril lleno (180/180) → 0 encolados"
  // = créditos quemados al pedo. Ahora: si el carril autogoogle ya está lleno, NO gasto ni una
  // búsqueda; si queda poco lugar, capo N abajo para no buscar más de lo que puedo inyectar.
  const _laneCap  = PER_SOURCE_ACTIVE_CAP.autogoogle ?? DEFAULT_SOURCE_CAP;
  const _laneUsed = await _countActiveCsvBySource(token, "autogoogle");
  const _laneRoom = Math.max(0, _laneCap - _laneUsed);
  if (_laneRoom <= 0) { log(`🔎 AutoGoogle: carril lleno (${_laneUsed}/${_laneCap}) — NO gasto créditos Serper este slot`); return; }
  const [yy, mm, dd] = dateISO.split("-").map(Number);
  const daysInMonth = new Date(yy, mm, 0).getDate();
  const daysLeft = Math.max(1, daysInMonth - dd + 1);                                  // incluye hoy
  const slotsLeftToday = Math.max(1, AUTOGOOGLE_SLOTS.filter(h => h >= hour).length);  // incluye éste
  const budgetToday = Math.ceil(remaining / daysLeft);
  let N = Math.ceil(budgetToday / slotsLeftToday);
  N = Math.max(1, Math.min(200, N, remaining));                                        // tope de seguridad 200/slot
  // Maxi 2026-07-15 SPEND INTELIGENTE: escalar por rendimiento reciente. freshRate = dominios NUEVOS por
  // búsqueda (rolling). Pool saturado (freshRate bajo) → MENOS búsquedas (no quemar créditos); piso 8 para
  // seguir sondeando cuando el pool se renueve; freshRate alto → hasta el budget completo.
  const _throttle = Math.max(0.15, Math.min(1, freshRate));
  N = Math.min(remaining, 200, Math.max(8, Math.round(N * _throttle)));
  // Cap por lugar en el carril: no buscar más de lo que entra. freshRate = dominios nuevos por
  // búsqueda → para llenar _laneRoom slots alcanzan ~_laneRoom/freshRate búsquedas (piso 8).
  N = Math.min(N, Math.max(8, Math.ceil(_laneRoom / Math.max(0.2, freshRate))));
  // Pool = TODAS las frases de cascade (3490, 12 idiomas) desde keywordsData.js; fallback inline.
  let pool;
  try { pool = Object.values(_AG_KEYWORDS).flat().filter(s => typeof s === "string" && s); } catch { pool = []; }
  if (!pool || pool.length < 50) pool = _AUTOGOOGLE_KEYWORDS;
  // Maxi 2026-07-16: SELECCIÓN POR YIELD (antes 100% random). ~65% de las frases que históricamente
  // traen más dominios FRESCOS (toolbar_keyword_yield) + ~35% exploración random (para seguir descubriendo
  // frases buenas y no encasillarse). Sube la calidad de URLs sin gastar más búsquedas.
  let _topPhrases = [];
  try {
    const _yr = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_keyword_yield?searches=gte.2&order=fresh.desc&select=phrase&limit=600`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (_yr.ok) _topPhrases = (await _yr.json()).map(r => r.phrase).filter(p => typeof p === "string");
  } catch {}
  const _poolSet = new Set(pool);
  const _topInPool = _topPhrases.filter(p => _poolSet.has(p));
  const _pickTop = [..._topInPool].sort(() => Math.random() - 0.5).slice(0, Math.round(N * 0.65));
  const _pickTopSet = new Set(_pickTop);
  const _explore = pool.filter(p => !_pickTopSet.has(p)).sort(() => Math.random() - 0.5).slice(0, N - _pickTop.length);
  const kws = [..._pickTop, ..._explore].sort(() => Math.random() - 0.5);
  log(`🔎 AutoGoogle slot ${slotLabel} — ${kws.length} búsquedas (${_pickTop.length} top-yield + ${_explore.length} explore · mes ${used}/${monthlyCap}, freshRate ${freshRate.toFixed(2)}, ${daysLeft}d)...`);
  const found = new Set();
  const kwDomains = new Map();  // Maxi 2026-07-16: keyword → dominios que trajo (para calcular el yield)
  let queriesDone = 0, errCount = 0, lastStatus = 0;
  for (const kw of kws) {
    // País de búsqueda rotado (targeting GEO no-Anglo) → trae publishers del país objetivo.
    const gl = _AUTOGOOGLE_GL[Math.floor(Math.random() * _AUTOGOOGLE_GL.length)];
    const { domains, ok, status } = await _serperSearch(kw, 20, gl);
    // Maxi 2026-07-15: contar SOLO las búsquedas EXITOSAS. Antes queriesDone++ corría siempre → las
    // fallidas (Serper 403/429 por créditos agotados) igual gastaban el "cap mensual" → se auto-bloqueaba.
    if (ok) { queriesDone++; domains.forEach(d => found.add(d)); kwDomains.set(kw, domains); }
    else { errCount++; lastStatus = status; }
    if (queriesDone > 0 && queriesDone % 20 === 0) {
      try { await setConfigValue(token, "autogoogle_serper_used", String(used + queriesDone)); await setConfigValue(token, "autogoogle_serper_period", period); } catch {}
      try { await setConfigValue(token, "auto_heartbeat_at", new Date().toISOString()); } catch {}  // Maxi 2026-07-15 (F3): heartbeat dentro del loop largo
    }
  }
  try { await setConfigValue(token, "autogoogle_serper_used", String(used + queriesDone)); await setConfigValue(token, "autogoogle_serper_period", period); } catch {}
  // Maxi 2026-07-15: surfacar el error de Serper a config (antes se tragaba → "cero silencioso" por semanas).
  if (errCount > 0) {
    const _emsg = `Serper status=${lastStatus} en ${errCount}/${kws.length} búsquedas`;
    try { await setConfigValue(token, "autogoogle_last_error", _emsg); } catch {}
    log(`🔎 AutoGoogle: ${errCount}/${kws.length} búsquedas FALLARON (${_emsg}) — revisar créditos/key de Serper`);
  } else {
    try { await setConfigValue(token, "autogoogle_last_error", ""); } catch {}
  }
  // Dedup canónico (cola activa + Prospects) + inject, si hubo resultados.
  let freshCount = 0, inserted = 0, _freshSet = new Set();
  if (found.size > 0) {
    const cands = [...found].filter(d => !DEPRIO_TLD_RE.test(d));   // pre-filtro TLD Anglo deprio
    const known = await _findKnownDomainsWorker(token, cands);
    const fresh = cands.filter(d => !known.has(d));
    _freshSet = new Set(fresh);
    freshCount = fresh.length;
    if (fresh.length > 0) inserted = await _injectIntoCsvQueue(token, fresh, "autogoogle");
  }
  // Maxi 2026-07-16: registrar el YIELD por keyword (frescos que trajo cada frase) → la selección futura
  // se sesga hacia las que rinden. RPC atómico bump_keyword_yield (incrementa searches/found/fresh).
  try {
    await Promise.all([...kwDomains].map(([kw, doms]) => {
      const _f = doms.filter(d => _freshSet.has(d)).length;
      return fetch(`${SUPABASE_URL}/rest/v1/rpc/bump_keyword_yield`, {
        method: "POST",
        headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
        body: JSON.stringify({ p_phrase: String(kw).slice(0, 300), p_searches: 1, p_found: doms.length, p_fresh: _f }),
      }).catch(() => {});
    }));
  } catch {}
  // Maxi 2026-07-15 SPEND INTELIGENTE: actualizar el rolling freshRate (dominios NUEVOS por búsqueda) —
  // SIEMPRE que hubo búsquedas exitosas, incluso con 0 nuevos (pool saturado → baja el rate → menos
  // búsquedas el próximo slot). Si todo falló (créditos/key) NO se ajusta (no es señal de saturación).
  if (queriesDone > 0) {
    const _slotRate = freshCount / queriesDone;
    const _newRate = Math.round((0.6 * freshRate + 0.4 * _slotRate) * 1000) / 1000;
    try {
      await setConfigValue(token, "autogoogle_fresh_rate", String(_newRate));
      await setConfigValue(token, "autogoogle_stats", JSON.stringify({ slot: slotLabel, searches: queriesDone, found: found.size, fresh: freshCount, inserted, freshRate: _newRate, at: new Date().toISOString().slice(0, 16) }));
    } catch {}
    log(`🔎 AutoGoogle: ${found.size} dominios de ${queriesDone} búsquedas → ${freshCount} nuevos → ${inserted} encolados · freshRate ${_newRate} (spend inteligente)`);
  } else {
    log(`🔎 AutoGoogle: 0 búsquedas exitosas (revisar autogoogle_last_error) — freshRate sin cambios`);
  }
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
      // Maxi 2026-06-22: recorrido AL AZAR del JSON (no en orden). Mezclamos los frescos
      // antes de cortar, así en sucesivas corridas no caemos siempre sobre los primeros.
      const fresh = candidates.filter(d => !known.has(d) && !sessionKnown.has(d))
        .sort(() => Math.random() - 0.5);
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
// Maxi 2026-06-17 (audit #1): TLDs deprio — skip ANTES de meter al pool
// para no gastar RapidAPI traffic-lookup en sitios obvios de USA/UK/CA/AU/NZ/IE.
// Cubre los casos donde el TLD ya nos dice el país. Sitios .com globales
// siguen pasando (SimilarWeb decide después si son USA-heavy).
const DEPRIO_TLD_RE = /\.(us|uk|co\.uk|ca|au|com\.au|nz|co\.nz|ie)$/i;

// Maxi 2026-07-16: código de país → sufijos de TLD, para SESGAR Majestic hacia los GEOs objetivo
// (worker_discovery_config.geos_priority) en vez de random global (que trae mucho USA/marca).
const _CC_TO_TLD = {
  mx:[".mx"], ar:[".ar"], br:[".br"], cl:[".cl"], co:[".co"], pe:[".pe"], uy:[".uy"], ve:[".ve"], ec:[".ec"], bo:[".bo"], py:[".py"], cr:[".cr"], gt:[".gt"], do:[".do"], pa:[".pa"], hn:[".hn"], sv:[".sv"], ni:[".ni"],
  es:[".es"], it:[".it"], fr:[".fr"], de:[".de"], nl:[".nl"], pt:[".pt"], pl:[".pl"], tr:[".tr"], gr:[".gr"], se:[".se"], be:[".be"], ch:[".ch"], at:[".at"], ro:[".ro"], cz:[".cz"], hu:[".hu"], dk:[".dk"], fi:[".fi"], no:[".no"], sk:[".sk"], bg:[".bg"], hr:[".hr"], rs:[".rs"], si:[".si"],
  id:[".id"], my:[".my"], th:[".th"], vn:[".vn"], ph:[".ph"], jp:[".jp"], kr:[".kr"], in:[".in"], sg:[".sg"], hk:[".hk"], tw:[".tw"],
  za:[".za"], eg:[".eg"], dz:[".dz"], ma:[".ma"], ng:[".ng"], ke:[".ke"], tn:[".tn"],
};
// Pre-filtro por NOMBRE de dominio: descartar no-publishers OBVIOS ANTES de gastar RapidAPI.
// CONSERVADOR (regla de oro: no matar un publisher real) → solo señales muy fuertes: TLDs gov/edu/mil,
// y tokens de comercio/casino/adulto como SEGMENTO del dominio (no substring suelto — "bankrate" no matchea).
const _MAJESTIC_NAME_SKIP_RE = /(\.gov(\.|$)|\.gob(\.|$)|\.gouv\.|\.edu(\.|$)|\.ac\.|\.mil(\.|$)|\.gov\.)|(^|[.\-])(shop|store|tienda|loja|negozio|boutique|webshop|onlineshop|casino|apuestas|betting|sportsbook|onlyfans|escort)([.\-]|$)/i;

async function _feederPullMajestic(token, targetCount, sessionKnown) {
  try {
    const pool = await loadDomainPool();
    if (!pool || pool.length === 0) return 0;
    // Maxi 2026-07-16: CURSOR SECUENCIAL (antes random). Recorre el 1M EN ORDEN (garantiza dominios
    // nuevos sin re-rolear + menos gasto de dedup), persiste el índice (majestic_cursor) y da la vuelta
    // al llegar al final. + SESGO GEO/TLD hacia geos_priority + PRE-FILTRO por nombre (gov/edu/shop/casino)
    // ANTES de gastar RapidAPI.
    const cfg = await getConfig(token);
    let cursor = parseInt(cfg.majestic_cursor || "0", 10) || 0;
    if (cursor < 0 || cursor >= pool.length) cursor = 0;
    // TLDs preferidos según geos_priority del worker_discovery_config (vacío = sin sesgo → puro secuencial).
    let wd = {}; try { wd = JSON.parse(cfg.worker_discovery_config || "{}"); } catch {}
    const preferTlds = (wd.geos_priority || []).flatMap(cc => _CC_TO_TLD[String(cc).toLowerCase()] || []);
    // Ventana secuencial ~12× el target para compensar los filtros de abajo.
    const WINDOW = Math.min(pool.length, Math.max(targetCount * 12, 300));
    const win = [];
    for (let i = 0; i < WINDOW; i++) win.push(pool[(cursor + i) % pool.length]);
    cursor = (cursor + WINDOW) % pool.length;
    // Filtros BARATOS (sin red): TLD deprio Anglo + nombre obvio no-publisher + corporate/brand.
    let cands = win.filter(d =>
      d && !DEPRIO_TLD_RE.test(d) && !_MAJESTIC_NAME_SKIP_RE.test(d) &&
      !isCorporatePattern(d) && !BRAND_BLOCKLIST.has(d)
    );
    // SESGO GEO: poner los dominios de los TLDs objetivo ADELANTE (no exclusivo: si no alcanzan, se
    // completa con el resto de la ventana → nunca frena el feeder por falta de match GEO).
    if (preferTlds.length) {
      const pref = cands.filter(d => preferTlds.some(t => d.endsWith(t)));
      const rest = cands.filter(d => !preferTlds.some(t => d.endsWith(t)));
      cands = [...pref, ...rest];
    }
    // Dedup vs lo ya visto (cola + Prospects) + sesión — sobre el frente de candidatos.
    const checkSet = cands.slice(0, Math.max(targetCount * 6, 120));
    const known = await _findKnownDomainsWorker(token, checkSet);
    const fresh = checkSet.filter(d => !known.has(d) && !sessionKnown.has(d));
    let inserted = 0;
    if (fresh.length > 0) {
      const slice = fresh.slice(0, targetCount);
      slice.forEach(d => sessionKnown.add(d));
      inserted = await _injectIntoCsvQueue(token, slice, "auto_feeder_majestic");
    }
    await setConfigValue(token, "majestic_cursor", String(cursor)).catch(() => {});
    log(`  🌱 majestic[cursor]: ventana=${WINDOW} post-filtros=${cands.length} frescos=${fresh.length} pref=${preferTlds.length ? preferTlds.join("/") : "—"} → insertados=${inserted} (cursor→${cursor}/${pool.length})`);
    return inserted;
  } catch (e) {
    log(`  ⚠️ majestic feeder error: ${e.message}`);
    return 0;
  }
}

// ════════════════════════════════════════════════════════════════
// M3 (Maxi 2026-06-19): ASIGNACIÓN INTELIGENTE DE FUENTES DEL FEEDER.
// Antes el feeder repartía 1/3 FIJO a sellers/monday/majestic sin importar
// cuál producía leads válidos. Ahora reparte por RENDIMIENTO REAL (yield =
// efectivos/brutos de los últimos 7 días por fuente), con piso de exploración
// del 15% para que ninguna fuente muera y pueda recuperarse. Las fuentes
// agotadas (devuelven pocos efectivos o pocos frescos) pierden peso solas
// (P1 yield + P3 agotamiento; el costo P2 queda implícito: bajo yield = caro
// por lead → menos peso). SIN cambios de schema: usa el tag `source` que ya
// llevan los rows de csv_queue + las columnas gross_* de toolbar_feeder_runs.
// ════════════════════════════════════════════════════════════════
// ════════════════════════════════════════════════════════════════
// FUENTE RENOVABLE (Maxi 2026-06-19): ads.txt → sellers.json (graph expander).
// Antídoto a "se acaban los JSON": en vez de una lista FIJA de redes, descubre
// REDES NUEVAS leyendo el ads.txt de publishers que YA validamos, baja su
// sellers.json y saca PUBLISHERS nuevos. Se auto-alimenta y crece solo. Costo $0
// (solo HTTP, sin RapidAPI/Apollo). Las redes nuevas se persisten en config para
// que el descubrimiento compounding crezca con el tiempo.
// ════════════════════════════════════════════════════════════════
function _hostFromSellersUrl(u) {
  try { return new URL(u).hostname.replace(/^www\./, "").toLowerCase(); } catch { return ""; }
}
const _KNOWN_SELLER_HOSTS = new Set(FEEDER_SELLERS_SOURCES.map(_hostFromSellersUrl).filter(Boolean));

async function _adsTxtSystems(domain) {
  try {
    const res = await fetch(`https://${domain}/ads.txt`, { redirect: "follow", signal: AbortSignal.timeout(9000) });
    if (!res.ok) return [];
    const text = await res.text();
    if (/^\s*<!doctype|<html/i.test(text.trim())) return [];
    const systems = new Set();
    for (const line of text.slice(0, 200000).split("\n")) {
      const clean = line.split("#")[0].trim();
      if (!clean) continue;
      const parts = clean.split(",").map(s => s.trim());
      if (parts.length < 3) continue;
      const d = _normalizeFeederDomain(parts[0]);
      if (d && !/^(subdomain|contact|cname|owner)/i.test(d)) systems.add(d);
    }
    return [...systems];
  } catch { return []; }
}

async function _publishersFromSellersJson(networkDomain) {
  for (const url of [`https://${networkDomain}/sellers.json`, `https://www.${networkDomain}/sellers.json`]) {
    try {
      const res = await fetch(url, { redirect: "follow", signal: AbortSignal.timeout(15000), headers: { "Accept": "application/json" } });
      if (!res.ok) continue;
      const text = await res.text();
      if (/^\s*<!doctype|<html/i.test(text.trim())) continue;
      const data = JSON.parse(text);
      const pubs = (data?.sellers || [])
        .filter(s => (s.seller_type || "").toUpperCase() === "PUBLISHER")
        .map(s => _normalizeFeederDomain(s.domain || ""))
        .filter(Boolean);
      if (pubs.length) return [...new Set(pubs)];
    } catch {}
  }
  return [];
}

async function _feederPullAdsTxtGraph(token, maxInject, sessionKnown) {
  if (maxInject <= 0) return 0;
  const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  // 1. Semillas: publishers que YA validamos (los mejores). Fallback a recientes.
  let seeds = [];
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.validated&select=domain&order=validated_at.desc&limit=40`, { headers: auth });
    if (r.ok) seeds = (await r.json()).map(x => _normalizeFeederDomain(x.domain || "")).filter(Boolean);
  } catch {}
  if (seeds.length < 5) {
    try {
      const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&select=domain&order=created_at.desc&limit=40`, { headers: auth });
      if (r.ok) seeds.push(...(await r.json()).map(x => _normalizeFeederDomain(x.domain || "")).filter(Boolean));
    } catch {}
  }
  seeds = [...new Set(seeds)].slice(0, 25);
  if (!seeds.length) { log(`  🌐 adstxt-graph: sin semillas (review_queue vacío)`); return 0; }

  // 2. Redes ya conocidas: las fijas (FEEDER_SELLERS_SOURCES) + las descubiertas antes.
  let discovered = [];
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.discovered_sellers_networks&select=value`, { headers: auth });
    if (r.ok) { const rows = await r.json(); discovered = JSON.parse(rows?.[0]?.value || "[]"); }
  } catch {}
  const knownNetworks = new Set([..._KNOWN_SELLER_HOSTS, ...discovered.map(d => String(d).toLowerCase())]);

  // 3. ads.txt de las semillas → ad-systems → quedarse con los que NO conocíamos.
  const newNetworks = new Set();
  for (let i = 0; i < seeds.length; i += 6) {
    const lists = await Promise.all(seeds.slice(i, i + 6).map(d => _adsTxtSystems(d)));
    for (const list of lists) for (const net of list) if (!knownNetworks.has(net)) newNetworks.add(net);
  }
  if (!newNetworks.size) { log(`  🌐 adstxt-graph: 0 redes nuevas (de ${seeds.length} semillas)`); return 0; }

  // 4. sellers.json de hasta 15 redes nuevas → publishers nuevos → dedup → inject.
  let inserted = 0;
  const confirmedNets = [];
  for (const net of [...newNetworks].slice(0, 15)) {
    if (inserted >= maxInject) break;
    const pubs = await _publishersFromSellersJson(net);
    if (!pubs.length) continue;
    confirmedNets.push(net);
    const known = await _findKnownDomainsWorker(token, pubs);
    const fresh = pubs.filter(d => !known.has(d) && !sessionKnown.has(d));
    if (!fresh.length) continue;
    const slice = fresh.slice(0, maxInject - inserted);
    slice.forEach(d => sessionKnown.add(d));
    const ok = await _injectIntoCsvQueue(token, slice, "auto_feeder_adstxt");
    inserted += ok;
    log(`  🌐 adstxt-graph red NUEVA ${net}: ${pubs.length} pubs → ${fresh.length} frescos → ${ok} insertados`);
  }

  // 5. Persistir las redes nuevas confirmadas (compounding — la base crece sola).
  if (confirmedNets.length) {
    const merged = [...new Set([...discovered, ...confirmedNets])].slice(0, 500);
    await setConfigValue(token, "discovered_sellers_networks", JSON.stringify(merged)).catch(() => {});
    log(`  🌐 adstxt-graph: +${confirmedNets.length} redes nuevas guardadas (total acumulado ${merged.length})`);
  }
  return inserted;
}

const FEEDER_SOURCE_KEYS = [
  { key: "sellers",  tag: "auto_feeder_sellers"  },
  { key: "monday",   tag: "auto_feeder_monday"   },
  { key: "majestic", tag: "auto_feeder_majestic" },
];
const FEEDER_EXPLORE_FLOOR     = 0.15; // cada fuente recibe ≥15% del split (exploración)
const FEEDER_WEIGHTS_MIN_GROSS = 30;   // bajo este total de brutos en 7d → 1/3 fijo (cold start)

// Sube las fuentes por debajo del piso y baja proporcionalmente las de arriba.
function _applyExploreFloor(w, floor) {
  const keys = ["sellers", "monday", "majestic"];
  const out = { ...w };
  let deficit = 0;
  const above = [];
  for (const k of keys) {
    if (out[k] < floor) { deficit += (floor - out[k]); out[k] = floor; }
    else above.push(k);
  }
  if (deficit > 0 && above.length) {
    const aboveSum = above.reduce((s, k) => s + out[k], 0) || 1;
    for (const k of above) out[k] = Math.max(floor, out[k] - deficit * (out[k] / aboveSum));
  }
  // re-normalizar por si el reparto dejó suma != 1
  const s = out.sellers + out.monday + out.majestic;
  return { sellers: out.sellers / s, monday: out.monday / s, majestic: out.majestic / s };
}

async function _getFeederSourceWeights(token) {
  const equal = { sellers: 1 / 3, monday: 1 / 3, majestic: 1 / 3, debug: "equal(coldstart)" };
  try {
    const sinceISO = new Date(Date.now() - 7 * 86400_000).toISOString();
    const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
    // Brutos por fuente (suma de columnas existentes en runs ok últimos 7 días)
    const runsRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_feeder_runs?status=eq.ok&cron_at=gte.${sinceISO}&select=gross_sellers,gross_monday,gross_majestic`,
      { headers: auth }
    );
    if (!runsRes.ok) return equal;
    const runs = await runsRes.json();
    const gross = { sellers: 0, monday: 0, majestic: 0 };
    for (const r of (runs || [])) {
      gross.sellers  += parseInt(r.gross_sellers, 10)  || 0;
      gross.monday   += parseInt(r.gross_monday, 10)   || 0;
      gross.majestic += parseInt(r.gross_majestic, 10) || 0;
    }
    const totalGross = gross.sellers + gross.monday + gross.majestic;
    if (totalGross < FEEDER_WEIGHTS_MIN_GROSS) return equal; // poca data → arrancar parejo
    // Efectivos por fuente: rows que llegaron a status='done' con ese source en la ventana
    const eff = { sellers: 0, monday: 0, majestic: 0 };
    for (const s of FEEDER_SOURCE_KEYS) {
      const r = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.done&source=eq.${s.tag}&uploaded_at=gte.${sinceISO}&select=id`,
        { headers: { ...auth, "Prefer": "count=exact", "Range": "0-0" } }
      );
      eff[s.key] = parseInt((r.headers.get("content-range") || "").match(/\/(\d+)$/)?.[1] || "0", 10);
    }
    // Yield con suavizado Beta(1,1): (eff+1)/(gross+2)
    const yld = {
      sellers:  (eff.sellers  + 1) / (gross.sellers  + 2),
      monday:   (eff.monday   + 1) / (gross.monday   + 2),
      majestic: (eff.majestic + 1) / (gross.majestic + 2),
    };
    const sum = yld.sellers + yld.monday + yld.majestic;
    if (!(sum > 0)) return equal;
    let w = { sellers: yld.sellers / sum, monday: yld.monday / sum, majestic: yld.majestic / sum };
    w = _applyExploreFloor(w, FEEDER_EXPLORE_FLOOR);
    w.debug = `eff s/m/j=${eff.sellers}/${eff.monday}/${eff.majestic} gross=${gross.sellers}/${gross.monday}/${gross.majestic}`;
    return w;
  } catch { return equal; }
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

  // 2. RapidAPI gate + throttle de pacing (Maxi 2026-06-17).
  // Hard gate al 95% (no se mueve). Throttle dinámico: si gastamos al 50% pero
  // solo va 20% del ciclo, pisamos el freno para que el cap dure todo el mes.
  const { usedThisMonth, limit: rapidLimit } = await getRapidApiUsageThisMonth(token);
  if (usedThisMonth >= rapidLimit * FEEDER_RAPIDAPI_THRESHOLD) {
    log(`🛑 cron ${slotLabel} SKIP: RapidAPI ${usedThisMonth}/${rapidLimit} (≥${FEEDER_RAPIDAPI_THRESHOLD * 100}%)`);
    await _insertFeederRun(token, slotLabel, {
      status: "skipped_rapidapi", rapidapi_used: usedThisMonth, rapidapi_limit: rapidLimit,
      notes: "RapidAPI near monthly limit",
    });
    return;
  }
  if (_isAheadOfPace(usedThisMonth, rapidLimit)) {
    const tPct = Math.round(_cycleElapsedRatio() * 100);
    const uPct = Math.round((usedThisMonth / rapidLimit) * 100);
    log(`⏸️ cron ${slotLabel} SKIP throttle: RapidAPI usado ${uPct}% pero ciclo va ${tPct}% (pacing)`);
    await _insertFeederRun(token, slotLabel, {
      status: "skipped_throttle", rapidapi_used: usedThisMonth, rapidapi_limit: rapidLimit,
      notes: `pacing — used ${uPct}% vs cycle ${tPct}%`,
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
  // 3b. Maxi 2026-06-22: GATE de backlog de la COLA csv. Si ya hay muchos en
  // pending/waiting/next_day, NO cargar más → dejar que drene antes de re-llenar
  // (la cola había llegado a ~1500 en espera).
  const _backlog = await _getCsvQueueBacklog(token);
  if (_backlog >= CSV_QUEUE_HALT_HIGH) {
    log(`🛑 cron ${slotLabel} SKIP: cola csv con backlog ${_backlog} (>${CSV_QUEUE_HALT_HIGH}) — esperar a que drene`);
    await _insertFeederRun(token, slotLabel, {
      status: "skipped_saturated", rq_valid_before: rqValid,
      rapidapi_used: usedThisMonth, rapidapi_limit: rapidLimit,
      notes: `csv_queue backlog ${_backlog}`,
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

  // 5. Pull from 3 fuentes — M3: split por RENDIMIENTO (no más 1/3 fijo).
  // Maxi 2026-06-17 (audit #9): sessionKnown ya deduplica entre fuentes — un
  // domain insertado por Sellers no se reinserta por Monday/Majestic. Verificado.
  // El sessionKnown.size final = total únicos insertados en este slot.
  const w = await _getFeederSourceWeights(token);
  const allocSellers  = Math.max(1, Math.round(targetGross * w.sellers));
  const allocMonday   = Math.max(1, Math.round(targetGross * w.monday));
  const allocMajestic = Math.max(1, Math.round(targetGross * w.majestic));
  log(`  ⚖️ feeder weights: sellers=${(w.sellers * 100).toFixed(0)}% monday=${(w.monday * 100).toFixed(0)}% majestic=${(w.majestic * 100).toFixed(0)}% — alloc ${allocSellers}/${allocMonday}/${allocMajestic} [${w.debug}]`);
  const sessionKnown = new Set();
  const fromSellers  = await _feederPullSellers(token, allocSellers, sessionKnown);
  const fromMonday   = await _feederPullMonday(token, allocMonday, sessionKnown);
  const fromMajestic = await _feederPullMajestic(token, allocMajestic, sessionKnown);
  // BONUS renovable: ads.txt → sellers.json (descubre redes/publishers nuevos, $0).
  // Va ENCIMA del split de las 3 (no compite por el yield) — supply extra que
  // crece sola con el tiempo. Cap modesto por slot para acotar el HTTP.
  const fromAdsTxt = await _feederPullAdsTxtGraph(token, Math.min(40, Math.max(15, allocSellers)), sessionKnown).catch(() => 0);
  log(`  🔎 sessionKnown size: ${sessionKnown.size} dominios únicos insertados (sellers=${fromSellers}+monday=${fromMonday}+majestic=${fromMajestic}+adstxt=${fromAdsTxt})`);
  const grossTotal = fromSellers + fromMonday + fromMajestic + fromAdsTxt;

  log(`✅ cron ${slotLabel}: sellers=${fromSellers} monday=${fromMonday} majestic=${fromMajestic} adstxt=${fromAdsTxt} = ${grossTotal} brutos`);

  await _insertFeederRun(token, slotLabel, {
    status: grossTotal > 0 ? "ok" : "incomplete",
    gross_sellers: fromSellers, gross_monday: fromMonday, gross_majestic: fromMajestic,
    rapidapi_used: usedThisMonth, rapidapi_limit: rapidLimit,
    rq_valid_before: rqValid,
    notes: `w s/m/j=${(w.sellers * 100).toFixed(0)}/${(w.monday * 100).toFixed(0)}/${(w.majestic * 100).toFixed(0)} adstxt=${fromAdsTxt}`,
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
      // FIX 2026-05-19: la medición anterior comparaba review_queue.count antes
      // vs después de 30min, lo que daba delta=0 cuando el Agent gastaba leads
      // en paralelo. Ahora contamos directamente cuántos rows de ESTE cron en
      // toolbar_csv_queue terminaron status='done' (= pasaron todos los filtros
      // y llegaron al pool). Eso es el "efectivo" real, independiente del consumo.
      const cronStart = new Date(run.cron_at).toISOString();
      const cronEnd = new Date(new Date(run.cron_at).getTime() + 15 * 60_000).toISOString();
      const doneRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.done&uploaded_by=eq.worker%40autofeeder&uploaded_at=gte.${cronStart}&uploaded_at=lt.${cronEnd}&select=id`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
      );
      const rangeHdr = doneRes.headers.get("content-range") || "";
      const eff = parseInt(rangeHdr.match(/\/(\d+)$/)?.[1] || "0", 10);
      const conv = run.gross_total > 0 ? (eff / run.gross_total) * 100 : 0;
      const rqValidNow = await _getReviewQueueValidCount(token);
      await fetch(`${SUPABASE_URL}/rest/v1/toolbar_feeder_runs?id=eq.${run.id}`, {
        method: "PATCH",
        headers: {
          "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
          "Content-Type": "application/json", "Prefer": "return=minimal",
        },
        body: JSON.stringify({ effective_added: eff, conversion_pct: conv.toFixed(2), rq_valid_after: rqValidNow }),
      }).catch(() => {});
      log(`📏 feeder run id=${run.id}: gross=${run.gross_total} → efectivos=${eff} (${conv.toFixed(1)}%) [csv_queue done count]`);
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
// Ventanas de autopilot (Madrid, L-V). Antes era solo 20:00 (1×/día) → "se prende
// pocas veces". Ahora 4 ventanas alineadas con los slots del agente (9/12/15/18/20)
// para garantizar que el worker esté despierto cuando dispara. El total diario de
// prospects igual lo limita agent_max_per_day / daily_override.
// Maxi 2026-06-19: "prendido casi todo el día". Antes 4 ventanas (12/15/18/20)
// = ~80min/día. Ahora cada hora 9-20 (L-V). NO revienta presupuesto porque cada
// arranque pasa por el freno de pacing (abajo): si vamos gastando RapidAPI más
// rápido que el ritmo del ciclo, ese slot se saltea solo.
const AUTOPILOT_SLOTS = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20];
let _autopilotLastSlot = "";

async function maybeStartAutopilotSlot(token) {
  const { hour, weekday, dateISO } = _madridNowParts();
  if (weekday === "Sat" || weekday === "Sun") return;
  if (!AUTOPILOT_SLOTS.includes(hour)) return;
  const slotLabel = `autopilot-${dateISO}-${String(hour).padStart(2, "0")}:00`;
  if (_autopilotLastSlot === slotLabel) return;
  // Maxi 2026-06-22: gate de backlog — no descubrir más si la cola csv está llena.
  const _apBl = await _getCsvQueueBacklog(token);
  if (_apBl >= CSV_QUEUE_HALT_HIGH) { log(`🤖 Autopilot SKIP: cola csv backlog ${_apBl} (>${CSV_QUEUE_HALT_HIGH})`); return; }

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

  // Maxi 2026-06-19: FRENO DE PACING. Como ahora hay slots cada hora, este guard
  // evita reventar el cupo: si gastamos RapidAPI más rápido que lo que va del
  // ciclo (6-a-6), o estamos cerca del cap, NO arrancamos este slot. Se auto-frena
  // al inicio del ciclo y deja correr al final (igual que el feeder).
  try {
    const { usedThisMonth, limit: rapidLimit } = await getRapidApiUsageThisMonth(token);
    if (usedThisMonth >= rapidLimit * FEEDER_RAPIDAPI_THRESHOLD) {
      log(`🛰️ autopilot ${slotLabel} SKIP: RapidAPI ${usedThisMonth}/${rapidLimit} cerca del cap`);
      return;
    }
    if (_isAheadOfPace(usedThisMonth, rapidLimit)) {
      log(`🛰️ autopilot ${slotLabel} SKIP throttle: RapidAPI adelantado al ritmo del ciclo (pacing)`);
      return;
    }
  } catch {}

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
const REENRICH_COOLDOWN_MS = 60 * 1000;      // Maxi 2026-07-14: 60s (re-análisis rápido on-demand de los sin-email)
const REENRICH_BATCH = 40;                    // Maxi 2026-07-14: 40/run
const REENRICH_CONC = 5;                      // Maxi 2026-07-14: 5 en paralelo (antes secuencial) → ~5x más rápido

async function runReenrichBadLeads(token) {
  try {
    const cfg = await getConfig(token);
    if (String(cfg.agent_reenrich_bad_leads || "").toLowerCase() !== "true") return;
    if (Date.now() - _lastReenrichRunAt < REENRICH_COOLDOWN_MS) return;
    _lastReenrichRunAt = Date.now();

    // Maxi 2026-06-30: el SCRAPE es gratis y no depende de Apollo. Antes, si no había
    // Apollo o el cap estaba lleno, se cortaba TODO el re-enrich → nunca se re-leían las
    // webs con el scraper mejorado. Ahora: el scrape siempre corre; Apollo es opcional
    // (solo como fallback cuando hay cupo).
    const apollo_api_key = cfg.apollo_api_key;
    let apolloAvailable = !!apollo_api_key;
    if (apolloAvailable) {
      const usage = await getApolloUsageToday(token);
      if (usage.usedToday >= usage.limit || (usage.usedThisMonth ?? 0) >= APOLLO_MONTHLY_HARD_CAP) {
        log("⚠️ reenrich: Apollo cap alcanzado → solo scrape (gratis)");
        apolloAvailable = false;
      }
    } else {
      log("ℹ️ reenrich: sin APOLLO_API_KEY → solo scrape (gratis)");
    }

    // Leads que necesitan re-enrich: 0 emails, o solo generic (info@/contact@) sin apollo/informer.
    // Maxi 2026-07-14: BARRIDO COMPLETO por cursor (antes miraba SIEMPRE los 50 más viejos con
    // order=created_at.asc&limit=50 sin cursor → se apagaba antes de recorrer todo el pool y dejaba
    // cientos sin re-analizar). Ahora avanza por reenrich_cursor_ts hasta agotar el pool. +traffic al
    // select (faltaba → Apollo recibía traffic=0).
    const cursor = cfg.reenrich_cursor_ts || "";
    const cursorClause = cursor ? `&created_at=gt.${encodeURIComponent(cursor)}` : "";
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending${cursorClause}&select=id,domain,emails,email_sources,contact_name,category,traffic,created_at&order=created_at.asc&limit=${REENRICH_BATCH}`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return;
    const leads = await res.json();
    if (!Array.isArray(leads) || leads.length === 0) {
      log("✅ reenrich: pool COMPLETO re-analizado → flag OFF");
      await setConfigValue(token, "agent_reenrich_bad_leads", "false").catch(() => {});
      await setConfigValue(token, "reenrich_cursor_ts", "").catch(() => {});
      return;
    }
    // Avanzar cursor al último de la ventana (procesemos o no cada uno → sweep monotónico)
    const _reenrichLastTs = leads[leads.length - 1].created_at;
    const candidates = leads.filter(l => {
      const emails = Array.isArray(l.emails) ? l.emails : [];
      if (emails.length === 0) return true;  // sin email → re-leer
      const sources = l.email_sources || {};
      const hasGood = emails.some(e => {
        const src = (sources[e.toLowerCase()] || "").toLowerCase();
        if (src === "apollo" || src === "informer") return true;
        return !_isGenericLocalPart(e);
      });
      return !hasGood;
    });
    await setConfigValue(token, "reenrich_cursor_ts", _reenrichLastTs).catch(() => {});
    if (candidates.length === 0) { log(`🔄 reenrich: ventana ${leads.length} sin candidatos (cursor→${_reenrichLastTs})`); return; }

    log(`🔄 reenrich: procesando ${candidates.length} leads (conc ${REENRICH_CONC})`);
    for (let _ri = 0; _ri < candidates.length; _ri += REENRICH_CONC) {
     await Promise.all(candidates.slice(_ri, _ri + REENRICH_CONC).map(async (lead) => {
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

        // 2) Apollo SOLO si scrape no encontró nada Y hay cupo. Maxi 2026-07-01:
        // forceUnlock — el user pidió PAGAR Apollo cuando el scrape viene vacío (estos
        // leads ya pasaron el filtro de tráfico). Respeta el cap mensual duro.
        if (!foundEmail && apolloAvailable) {
          try {
            const apolloRes = await findBestApolloEmail(lead.domain, apollo_api_key, token, {
              traffic: lead.traffic || 0, allowUnlock: true, forceUnlock: true,
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
      } catch (e) {
        log(`  ⚠️ reenrich ${lead.domain}: ${e.message}`);
      }
     }));
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
// Maxi 2026-07-15 (F6): medianoche de HOY en Madrid, como ISO UTC. Reemplaza el patrón buggy de mezclar
// la fecha de Buenos Aires con 'Z' (medianoche UTC) → desfase de 3h + inconsistente con el resto del worker
// (que reset en Madrid). Mismo cálculo que getAgentDailyCount.
function _madridDayStartUtc() {
  const todaySpain = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
  const probe = new Date(`${todaySpain}T00:00:00`);
  const offsetMs = new Date(probe.toLocaleString("en-US", { timeZone: "Europe/Madrid" })).getTime()
                 - new Date(probe.toLocaleString("en-US", { timeZone: "UTC" })).getTime();
  return new Date(probe.getTime() - offsetMs).toISOString();
}
async function getUserAutopilotCountToday(token, userEmail) {
  if (!userEmail) return 0;
  try {
    const cutoffUtc = _madridDayStartUtc();  // reset del día en Madrid (F6)
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?created_by=eq.${encodeURIComponent(userEmail)}&created_at=gte.${cutoffUtc}&select=id`,
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
      `${SUPABASE_URL}/rest/v1/toolbar_user_limits?user_email=eq.${encodeURIComponent(userEmail.toLowerCase())}&select=autopilot_enabled,autopilot_daily_minutes,autopilot_daily_prospects,monthly_api_cap&limit=1`, // Maxi 2026-07-03 perf: select=* → solo columnas leídas
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

// Techo de seguridad por día (protege contra rate-limit de Apollo y errores).
const APOLLO_DAILY_BURST_MAX = 250;

// Guard para no re-disparar el reset del contador mensual en cada lectura.
let _apolloPeriodReset = "";

// Días que faltan en el ciclo (mes calendario), incluyendo hoy. Mínimo 1.
function _daysLeftInCycle() {
  const now = new Date();
  const daysInMonth = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 0)).getUTCDate();
  return Math.max(1, daysInMonth - now.getUTCDate() + 1);
}

// Presupuesto diario dinámico: reparte los créditos que quedan del cap mensual
// entre los días que faltan del ciclo (+15% de margen para recuperar días en que
// el cron no corrió). Así no se quema todo a principio de mes ni quedan créditos
// sin usar al final. Nunca supera lo que queda ni el techo de seguridad diario.
function _apolloPacedDailyCap(monthRemaining) {
  if (monthRemaining <= 0) return 0;
  const paced = Math.ceil((monthRemaining / _daysLeftInCycle()) * 1.15);
  return Math.max(1, Math.min(monthRemaining, APOLLO_DAILY_BURST_MAX, paced));
}

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
    const usedToday = storedDate === today ? storedCount : 0;

    // Mensual — alineado con billing cycle 6→6 (igual que RapidAPI)
    const period       = _billingCyclePeriod();
    const storedPeriod = map.apollo_calls_month_period || "";
    const storedMonth  = parseInt(map.apollo_calls_month || "0", 10);
    const monthLimit   = parseInt(map.apollo_monthly_limit || String(APOLLO_MONTHLY_HARD_CAP), 10);
    const usedThisMonth = storedPeriod === period ? storedMonth : 0;

    // Rollover mensual a prueba de fallos: el RPC bump_api_counter (que ahora es
    // la única fuente del contador mensual) no está garantizado que resetee el
    // period al cambiar de mes. Si quedó viejo, lo reseteamos una vez por proceso
    // para que el contador no quede congelado ni el cap deje de funcionar.
    if (storedPeriod !== period && _apolloPeriodReset !== period) {
      _apolloPeriodReset = period;
      setConfigValue(token, "apollo_calls_month", "0").catch(() => {});
      setConfigValue(token, "apollo_calls_month_period", period).catch(() => {});
    }

    // Tope diario = presupuesto dinámico (restante/días que faltan), acotado por
    // un override manual opcional (apollo_daily_limit). Por defecto el override es
    // el techo de seguridad, así no interfiere y manda el pacing mensual.
    const monthRemaining  = Math.max(0, monthLimit - usedThisMonth);
    const configuredDaily = parseInt(map.apollo_daily_limit || String(APOLLO_DAILY_BURST_MAX), 10);
    const limit = Math.min(configuredDaily, _apolloPacedDailyCap(monthRemaining));

    return { usedToday, limit, today, usedThisMonth, monthLimit, period, monthRemaining };
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

// Maxi 2026-06-17: filtro GEO worker-side. Sitios cuyo topCountry o geos_all[0]
// matchee esta lista se SKIPEAN antes de Apollo/scrape. Excepción: Monday
// refresh (re-engage manual del MB siempre se procesa).
//   USA/Canadá/UK/Australia/Nueva Zelanda/Irlanda → demasiado yanqui, no convierte.
//   LATAM/Centroamérica/Europa continental/África/Asia → SI procesamos.
const WORKER_DEPRIO_ISO = new Set([
  "US","USA","GB","UK","CA","AU","NZ","IE",
]);
const WORKER_DEPRIO_NAMES = new Set([
  "united states","usa","united kingdom","britain","england",
  "canada","australia","new zealand","ireland",
]);
// Maxi 2026-06-19: GEO deprio DESACTIVADO. El user nunca pidió bloquear GEOs en el
// descubrimiento — la selección de GEO se hace al ENVIAR (config del agente). Antes
// esto descartaba USA/UK/CA/AU/NZ/IE antes de llegar a Prospects. Ahora pasan todos;
// el único filtro de descubrimiento es el tráfico (<350K). Flag por si se quiere reactivar.
const WORKER_GEO_DEPRIO_ON = false;
function _isWorkerDeprioGeo(topCountryNameOrIso, geosAllArr) {
  if (!WORKER_GEO_DEPRIO_ON) return false;
  const norm = (s) => String(s || "").trim();
  const t = norm(topCountryNameOrIso);
  if (t) {
    if (WORKER_DEPRIO_ISO.has(t.toUpperCase())) return true;
    if (WORKER_DEPRIO_NAMES.has(t.toLowerCase())) return true;
  }
  if (Array.isArray(geosAllArr) && geosAllArr.length) {
    const first = norm(geosAllArr[0]).toUpperCase();
    if (WORKER_DEPRIO_ISO.has(first)) return true;
  }
  return false;
}

// Maxi 2026-06-18: cache distribución GEO del review_queue.pending. Se actualiza
// cada 5 min. Usado por _isGeoOverrepresentedInPool para frenar el feeder
// cuando un país satura el pool de Prospects.
const _geoPoolCache = { ts: 0, totalPending: 0, byGeo: new Map() };
const _GEO_POOL_TTL = 5 * 60 * 1000;
const _GEO_SATURATION_PCT = 0.25; // >25% del pool → saturado
async function _refreshGeoPoolCache(token) {
  if (Date.now() - _geoPoolCache.ts < _GEO_POOL_TTL && _geoPoolCache.totalPending > 0) return;
  try {
    // Traer hasta 2000 rows con solo geo/geos_all (liviano)
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&select=geo,geos_all&limit=2000`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    const byGeo = new Map();
    for (const r of rows) {
      let iso = "";
      if (Array.isArray(r.geos_all) && r.geos_all.length) iso = String(r.geos_all[0] || "").toUpperCase().slice(0, 2);
      else if (r.geo) iso = String(r.geo).trim().toUpperCase().slice(0, 2);
      if (!iso) iso = "UNK";
      byGeo.set(iso, (byGeo.get(iso) || 0) + 1);
    }
    _geoPoolCache.ts = Date.now();
    _geoPoolCache.totalPending = rows.length;
    _geoPoolCache.byGeo = byGeo;
  } catch {}
}
async function _isGeoOverrepresentedInPool(token, topCountryName, geosAll) {
  await _refreshGeoPoolCache(token);
  if (_geoPoolCache.totalPending < 50) return false; // pool chico, no aplicar
  // Determinar ISO del lead actual
  let iso = "";
  if (Array.isArray(geosAll) && geosAll.length) iso = String(geosAll[0] || "").toUpperCase().slice(0, 2);
  if (!iso && topCountryName) {
    // NAME → ISO
    const found = Object.keys(COUNTRY_CODES).find(k => COUNTRY_CODES[k] === topCountryName);
    if (found) iso = found;
    else iso = String(topCountryName).toUpperCase().slice(0, 2);
  }
  if (!iso) return false;
  const count = _geoPoolCache.byGeo.get(iso) || 0;
  const pct = count / _geoPoolCache.totalPending;
  return pct > _GEO_SATURATION_PCT;
}

// Período mensual = ciclo 6 → 6 (Maxi 2026-06-17). El RapidAPI cycle real
// es del día 6 de un mes al día 6 del próximo. Por eso anclamos el período
// al día 6: si hoy >= 6 → este mes-06. Si hoy < 6 → mes anterior-06.
// Formato "YYYY-MM-06" (también compat con el legacy "YYYY-MM" via slice(0,7)).
function _billingCyclePeriod() {
  const d = new Date();
  const isBeforeDay6 = d.getUTCDate() < 6;
  const month = isBeforeDay6 ? d.getUTCMonth() - 1 : d.getUTCMonth();
  const anchor = new Date(Date.UTC(d.getUTCFullYear(), month, 6));
  return anchor.toISOString().slice(0, 10); // "2026-06-06"
}

// Calcula % del ciclo transcurrido — 0 = recién empezó, 1 = está por terminar.
// Usado para throttle dinámico: si gastamos al 80% pero solo va 30% del ciclo,
// pisamos el freno.
function _cycleElapsedRatio() {
  const start = new Date(_billingCyclePeriod() + "T00:00:00Z");
  const end   = new Date(start.getTime());
  end.setUTCMonth(end.getUTCMonth() + 1);
  const total   = end.getTime() - start.getTime();
  const elapsed = Date.now() - start.getTime();
  return Math.max(0, Math.min(1, elapsed / total));
}

// Throttle dinámico: devuelve true si usamos MÁS rápido que el tiempo que pasó.
// Sirve para Apollo (cap 2400/mes) y RapidAPI (cap 40K/mes). Si ratio gastado
// > ratio transcurrido × 1.10 (10% margen), bloqueamos hasta que el tiempo
// se ponga al día. Evita quemar la cuota al día 20 y quedarse sin créditos
// hasta el próximo ciclo (Maxi 2026-06-17).
function _isAheadOfPace(used, limit) {
  if (limit <= 0) return false;
  const spentRatio = used / limit;
  const timeRatio  = _cycleElapsedRatio();
  return spentRatio > (timeRatio + 0.10);
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
    // SOLO contador diario. El mensual (apollo_calls_month) lo incrementa de
    // forma atómica bumpApolloUnlocks() vía RPC en cada unlock — si lo sumáramos
    // también acá habría doble conteo (cada unlock contaría 2x en el mensual).
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(apollo_calls_today,apollo_calls_date)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const storedDate  = map.apollo_calls_date || "";
    const storedCount = storedDate === today ? parseInt(map.apollo_calls_today || "0", 10) : 0;
    const newCount    = storedCount + callsThisSession;

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

// Maxi 2026-06-22: LISTA NEGRA (antes era lista blanca). Regla del user: sumar TODO
// lo disponible; bloquear SOLO los dominios que en Monday están en un deal ACTIVO o
// con dueño trabajándolo. Todo lo demás (Ciclo Finalizado, Mail No Enviado, Descartado,
// o que NO esté en Monday) se re-prospecta. Los nombres deben coincidir EXACTO con las
// labels de la columna deal_stage del board 1420268379.
const MONDAY_BLOCKED_STATES = new Set([
  "LIVE", "En Negociacion", "Propuesta Vigente", "Propuesta Vigente (T)",
  "PAUSADO", "Masivo - Diego", "Masivo - Agus", "Masivo - Max",
]);
// Helper: ¿este estado de Monday bloquea el re-prospect? (true = NO sumar)
function _isMondayBlocked(estado) { return MONDAY_BLOCKED_STATES.has((estado || "").trim()); }

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
      return _isMondayBlocked(estado); // solo los ACTIVOS/con-dueño se excluyen del pool
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
      // Maxi 2026-07-03 costo: FUSIBLE global anti-runaway (independiente de la lógica
      // de negocio). Cuenta hits RapidAPI por ventana de 1 min en memoria. El ritmo
      // normal es ~24/min (loops secuenciales con DOMAIN_DELAY_MS=2.5s) o ~10/min
      // (refresh/backfill 3-5 en paralelo cada 30s). Si supera RAPID_PER_MIN_FUSE
      // (250 = ~10× lo normal) es CLARAMENTE un bug girando en loop → corta TODO
      // (setea _rapidCapReached) + ALERTA. Es un breaker duro: ningún camino de código
      // puede facturar miles de hits/min aunque un cap de negocio falle o se saltee.
      const _fuseNow = Date.now();
      if (_fuseNow - _rapidFuseWindowStart >= 60_000) { _rapidFuseWindowStart = _fuseNow; _rapidFuseCount = 0; }
      _rapidFuseCount++;
      if (_rapidFuseCount > RAPID_PER_MIN_FUSE) {
        _rapidCapReached = true;
        log(`🚨 FUSIBLE RapidAPI DISPARADO: ${_rapidFuseCount} llamadas en <1min (límite ${RAPID_PER_MIN_FUSE}) — posible BUG en loop. Corto RapidAPI hasta el próximo reset de sesión.`);
        return { __error4xx: "per_minute_fuse_tripped" };
      }
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
// Maxi 2026-07-03 costo: fusible por-minuto (ver rapidFetchWithRetry). Umbral
// altísimo (250/min) que solo se alcanza con un bug en loop; ~10× el ritmo normal.
const RAPID_PER_MIN_FUSE   = 250;
let _rapidFuseWindowStart  = 0;
let _rapidFuseCount        = 0;

// Maxi 2026-06-18: FALLBACK de tráfico — si RapidAPI no devuelve, intentar
// scrapear de fuentes públicas que estiman tráfico mensual.
// Fuentes (en orden):
//   1. similarweb.com/website/{d}/   — más confiable cuando no está blocked
//   2. hypestat.com/info/{d}          — estima daily uniques (× 30 = monthly)
//   3. siteworthtraffic.com/report/{d} — alternativa
//
// Nota: estas son páginas PÚBLICAS — el dato ya es visible al user. Limit:
// rate-limit + Cloudflare bot protection. Si bloquea, devolvemos null y el
// flow original (freeze 15-30-60d) sigue.
async function _scrapeTrafficFallback(domain) {
  const headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
  };
  const tryFetch = async (url) => {
    try {
      const r = await fetch(url, { headers, signal: AbortSignal.timeout(12000) });
      if (!r.ok) return null;
      return await r.text();
    } catch { return null; }
  };
  // Helper: parsea "1.5M" / "1,500,000" / "2.3K" → número
  const parseHumanNum = (s) => {
    if (!s) return null;
    const clean = String(s).trim().replace(/,/g, "").toUpperCase();
    const m = clean.match(/^(\d+(?:\.\d+)?)\s*([KMB])?$/);
    if (!m) return null;
    const n = parseFloat(m[1]);
    const mult = m[2] === "K" ? 1e3 : m[2] === "M" ? 1e6 : m[2] === "B" ? 1e9 : 1;
    return Math.round(n * mult);
  };

  // ── 1. similarweb.com ──
  try {
    const html = await tryFetch(`https://www.similarweb.com/website/${domain}/`);
    if (html) {
      // SimilarWeb pública muestra Total Visits en varios formatos. Probamos varios.
      // a) JSON embebido: "totalVisits":1500000 o similar
      let m = html.match(/"(?:totalVisits|visits|monthlyVisits)"\s*:\s*(\d+)/i);
      if (m) {
        const visits = parseInt(m[1], 10);
        if (visits >= 1000) {
          log(`  📊 scrape fallback ${domain}: similarweb JSON → ${visits} visits`);
          return { visits, source: "similarweb_scrape" };
        }
      }
      // b) Texto visible: <p>1.5M</p> cerca de "Total Visits" o "Monthly Visits"
      m = html.match(/Total\s*Visits[^<]*<[^>]*>\s*([\d.,]+\s*[KMB]?)/i)
        || html.match(/Monthly\s*Visits[^<]*<[^>]*>\s*([\d.,]+\s*[KMB]?)/i);
      if (m) {
        const visits = parseHumanNum(m[1]);
        if (visits && visits >= 1000) {
          log(`  📊 scrape fallback ${domain}: similarweb HTML → ${visits} visits`);
          return { visits, source: "similarweb_scrape" };
        }
      }
    }
  } catch {}

  // ── 2. hypestat.com (estima daily uniques → ×30 monthly) ──
  // Maxi 2026-06-19: Hypestat muestra "Daily Pageviews" — que es JUSTO la métrica
  // del negocio (350K pageviews). Antes solo tomábamos visitors y el threshold
  // hacía visits×2 (estimación). Ahora, si hay pageviews directo, los devolvemos.
  try {
    const html = await tryFetch(`https://hypestat.com/info/${domain}`);
    if (html) {
      // Daily Pageviews directo (preferido)
      const pvM = html.match(/Daily\s*Page\s*?views?[^<]*<[^>]*>([\d.,]+\s*[KMB]?)/i);
      const dailyPv = pvM ? parseHumanNum(pvM[1]) : null;
      // "Daily Unique Visitors:" 5,000 → ×30
      let m = html.match(/Daily\s*Unique\s*Visitors[^<]*<[^>]*>([\d.,]+\s*[KMB]?)/i)
        || html.match(/Estimated\s*Worth[\s\S]{0,300}?Visitors[^<]*<[^>]*>([\d.,]+\s*[KMB]?)/i);
      const daily = m ? parseHumanNum(m[1]) : null;
      if (daily && daily >= 100) {
        const visits = daily * 30;
        const pageViews = (dailyPv && dailyPv >= 100) ? dailyPv * 30 : null;
        log(`  📊 scrape fallback ${domain}: hypestat → daily=${daily} ×30 = ${visits}${pageViews ? ` · pageviews=${pageViews}` : ""}`);
        return { visits, pageViews, source: "hypestat_scrape" };
      }
      // Caso: hay pageviews pero no visitors → derivar visits ≈ pageviews/2 para compat
      if (dailyPv && dailyPv >= 100) {
        const pageViews = dailyPv * 30;
        log(`  📊 scrape fallback ${domain}: hypestat → daily pageviews=${dailyPv} ×30 = ${pageViews}`);
        return { visits: Math.round(pageViews / 2), pageViews, source: "hypestat_scrape" };
      }
    }
  } catch {}

  // ── 3. siteworthtraffic.com ──
  try {
    const html = await tryFetch(`https://www.siteworthtraffic.com/report/${domain}`);
    if (html) {
      let m = html.match(/Monthly\s*Visitors[^<]*<[^>]*>([\d.,]+\s*[KMB]?)/i)
        || html.match(/Visitors[\s\S]{0,200}<[^>]*>([\d.,]+)\s*(?:per\s*month|monthly)/i);
      if (m) {
        const visits = parseHumanNum(m[1]);
        if (visits && visits >= 1000) {
          log(`  📊 scrape fallback ${domain}: siteworthtraffic → ${visits}`);
          return { visits, source: "siteworthtraffic_scrape" };
        }
      }
    }
  } catch {}

  return null;
}

async function getTrafficData(domain, rapidApiKey) {
  const headers = { "x-rapidapi-key": rapidApiKey, "x-rapidapi-host": "website-insights.p.rapidapi.com" };

  // REGLA DE ORO: cache compartida 90 días en Supabase. Antes de gastar 1 hit
  // a RapidAPI, chequeamos si ya tenemos data fresca de este dominio.
  const cleanD = cleanDomain(domain);
  const cached = await getTrafficCacheServer(cleanD);
  if (cached) {
    log(`  💾 traffic cache HIT ${cleanD} (sin gastar hit)`);
    // Maxi 2026-06-22 FIX: el HIT devolvía solo visits/ppv/topCountry y PERDÍA
    // swCategory, topCountries3 y pageViews (sí están guardados) → re-filtraba mal
    // (categoría vacía → Haiku innecesario; pageViews null → estimación peor; GEO sin top3).
    const _cc = Array.isArray(cached.topCountries) ? cached.topCountries : [];
    const _top3 = _cc.map(c => String(c?.code || "").toUpperCase().slice(0, 2)).filter(Boolean).slice(0, 3);
    return {
      visits:        cached.rawVisits || cached.visits || 0,
      pagesPerVisit: cached.pagesPerVisit || null,
      pageViews:     cached.pageViews || null,
      topCountry:    _cc[0]?.code ? (COUNTRY_CODES[_cc[0].code] || _cc[0].code) : null,
      topCountries3: _top3,
      swCategory:    cached.category || "",
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
    // Maxi 2026-06-18: si RapidAPI vino vacío/error, intentar fallback de scrape
    // antes de devolver null. Las fuentes públicas (similarweb / hypestat /
    // siteworthtraffic) son visibles sin login y a veces tienen data cuando
    // nuestra API no.
    const _tryScrapeFallback = async (reasonForLog) => {
      const fb = await _scrapeTrafficFallback(domain).catch(() => null);
      if (fb && fb.visits >= 1000) {
        log(`  ✅ getTrafficData ${domain}: ${reasonForLog} → scrape fallback OK (${fb.source})`);
        return { visits: fb.visits, pageViews: fb.pageViews ?? null, topCountry: null, error: null, fromScrape: fb.source };
      }
      return null;
    };
    if (!data) {
      log(`  ⚠️ getTrafficData ${domain}: response null (key vacía o servicio caído)`);
      const fb = await _tryScrapeFallback("rapidapi_null");
      if (fb) return fb;
      return { visits: null, topCountry: null, error: "null_response" };
    }
    if (data.__error4xx) {
      log(`  ⚠️ getTrafficData ${domain}: ${data.__error4xx}`);
      const fb = await _tryScrapeFallback(data.__error4xx);
      if (fb) return fb;
      return { visits: null, topCountry: null, error: data.__error4xx };
    }
    if (data.__error) {
      log(`  ⚠️ getTrafficData ${domain}: ${data.__error}`);
      const fb = await _tryScrapeFallback(data.__error);
      if (fb) return fb;
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
    // FIX 2026-05-26: devolver también la categoría SimilarWeb para que el filtro
    // de categorías auto-bloqueadas (banking, gov, universidades, etc.) la use.
    const swCategory = data?.WebsiteDetails?.Category || data?.Category || "";
    // Maxi 2026-06-18: si RapidAPI devolvió data pero visits=0, intentar scrape.
    // Caso real: APIs vieja vez devuelven shape OK pero visits=0 cuando hay glitch.
    if (!visits || visits === 0) {
      const fb = await _scrapeTrafficFallback(domain).catch(() => null);
      if (fb && fb.visits >= 1000) {
        log(`  ✅ getTrafficData ${domain}: RapidAPI dijo 0 → scrape fallback rescató ${fb.visits} (${fb.source})`);
        return { visits: fb.visits, pageViews: fb.pageViews ?? null, pagesPerVisit, topCountry, topCountries3, swCategory, error: null, fromScrape: fb.source };
      }
    }
    return { visits, pagesPerVisit, topCountry, topCountries3, swCategory, error: null };
  } catch (e) {
    // Si todo explotó, intentar scrape como último recurso
    const fb = await _scrapeTrafficFallback(domain).catch(() => null);
    if (fb && fb.visits >= 1000) {
      log(`  ✅ getTrafficData ${domain}: exception rescatada por scrape fallback (${fb.source})`);
      return { visits: fb.visits, pageViews: fb.pageViews ?? null, pagesPerVisit: null, topCountry: null, topCountries3: [], swCategory: "", error: null, fromScrape: fb.source };
    }
    return { visits: null, pagesPerVisit: null, topCountry: null, topCountries3: [], swCategory: "", error: e.message };
  }
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
      // Maxi 2026-07-15 (Cost#2): rutear por rapidFetchWithRetry → cap pre-check (_rapidCapReached) +
      // fusible por minuto + CONTADOR ATÓMICO (bumpApiCounterRPC). Antes hacía fetch directo con solo
      // _rapidGlobalCounter++ → estas llamadas facturadas NO las veía el meter atómico (subconteo entre
      // sesiones → el cap se podía superar). Ahora se cuentan igual que getTrafficData.
      try {
        const data = await rapidFetchWithRetry(
          `https://website-insights.p.rapidapi.com/similar-sites?domain=${encodeURIComponent(domain)}`,
          { "x-rapidapi-key": rapidApiKey, "x-rapidapi-host": "website-insights.p.rapidapi.com" },
          8000
        );
        if (!data || data.__error4xx || data.__error) return [];
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
        return [...new Set(domains)].slice(0, 60).filter(d => isDomainAllowed(d)); // Maxi 2026-06-19: 20→60
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
const APOLLO_CACHE_TTL_DAYS = 45;  // Maxi 2026-07-15 (Cost#6): 7→45. Un decision-maker revelado no cambia semana a semana; los leads viven semanas en pending → con 7d se re-pagaba el unlock. Los no-email igual se re-intentan GRATIS (polish/reenrich: scrape+social+informer).
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
// Maxi 2026-07-01: bajado 399K→350K para alinear con REVIEW_QUEUE_MIN_TRAFFIC. Antes había
// sitios 350K-399K que pasaban el floor pero NUNCA intentaban unlock de Apollo. Ahora TODO
// prospect que entra (≥350K pageviews) puede desbloquear Apollo si el HTML no dio email.
const APOLLO_UNLOCK_MIN_TRAFFIC = 350_000;

// Local-parts genéricos (no son una persona). Si el verified gratis es uno de
// estos y el sitio califica para unlock, gastamos 1 credit para revelar al
// decision-maker real en vez de quedarnos con el genérico.
const APOLLO_GENERIC_LOCAL = /^(info|contact|contacto|contato|contatto|kontakt|hello|hi|hola|ola|olá|support|soporte|suporte|atendimento|mail|email|inbox|news|press|prensa|imprensa|sales|ventas|marketing|publicidade|publicidad|comercial|admin|general|reception|recepcion|recepcao|webmaster|noreply|no-reply)$/i;
function _isGenericEmail(email) {
  const local = (email || "").split("@")[0] || "";
  return APOLLO_GENERIC_LOCAL.test(local.trim());
}
async function findBestApolloEmail(domain, apolloKey, token, { traffic = 0, allowUnlock = true, forceUnlock = false } = {}) {
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

  // 3. ¿Verified gratis? Lo guardamos como freeResult (0 credits).
  const verified = people.find(p => p.email && APOLLO_GOOD_STATUSES.has(p.email_status));
  let freeResult = null;
  if (verified) {
    freeResult = {
      email: verified.email,
      contact_name: `${verified.first_name||""} ${verified.last_name||""}`.trim(),
      phone: _extractApolloPhone(verified),
      source: "free_verified",
    };
  }

  // 4. ¿Califica para unlock? Tráfico alto, allowUnlock y bajo cap mensual.
  // Maxi 2026-07-01: forceUnlock (re-enrich de leads SIN email) bypassa el umbral de
  // tráfico — el user pidió PAGAR Apollo cuando el scrape no encuentra nada. El cap
  // mensual duro sigue vigente (no se bypassa NUNCA).
  const qualifiesUnlock = allowUnlock && (forceUnlock || traffic >= APOLLO_UNLOCK_MIN_TRAFFIC);

  // Si hay verified gratis y NO califica, o el gratis ya es una persona (no genérico),
  // nos quedamos con el gratis sin gastar crédito.
  if (freeResult && (!qualifiesUnlock || !_isGenericEmail(freeResult.email))) {
    if (token) saveApolloCacheServer(token, domain, { emails: [freeResult.email], contact_name: freeResult.contact_name, phone: freeResult.phone, source: "worker_free" }).catch(() => {});
    return freeResult;
  }

  // Acá: o no hay gratis, o el gratis es genérico y el sitio califica → intentamos unlock.
  if (!qualifiesUnlock) return freeResult; // null si tampoco había gratis

  // Hard cap + throttle de pacing. Si gastamos al X% pero solo va Y% del ciclo
  // (X > Y + 10%), pisamos el freno para que el cap dure todo el mes
  // (Maxi 2026-06-17: evitar quemar Apollo al día 20 y quedarse sin créditos).
  try {
    const usage = await getApolloUsageToday(token);
    const used  = usage.usedThisMonth || 0;
    const cap   = APOLLO_MONTHLY_HARD_CAP;
    if (used >= cap) {
      log(`  ⚠ Apollo cap ${cap} alcanzado (${used}/${usage.monthLimit}) — skip unlock ${domain}`);
      return freeResult;
    }
    if (!forceUnlock && _isAheadOfPace(used, cap)) {
      const tPct = Math.round(_cycleElapsedRatio() * 100);
      const uPct = Math.round((used / cap) * 100);
      log(`  ⏸️ Apollo throttle: usado ${uPct}% pero ciclo va ${tPct}% — skip unlock ${domain} (pacing)`);
      return freeResult;
    }
  } catch {}

  // 5. Unlock — Maxi 2026-07-08 BUG FIX: antes unlockeaba people[0] (el PRIMERO de la
  // lista de Apollo, que suele ser random/junior — ej. "Home & Garden Reporter") y si ese
  // no tenía email se rendía → gastaba crédito en la persona equivocada y devolvía 0 emails
  // aunque el editor/publisher SÍ tuviera. Ahora rankea por CARGO (decision-maker) y prueba
  // hasta 3 empezando por el más senior, parando en el primero que REVELE email. El cap
  // mensual duro corta el gasto igual.
  // Maxi 2026-07-08: el user SOLO quiere revelar emails de PUBLICIDAD/MARKETING/PROGRAMMATIC/
  // TECH/DECISIÓN — NUNCA periodistas, editores, fotógrafos ni ops (ahí se quemaba el crédito
  // en gente sin email ni relevancia). Filtramos ANTES de pagar el reveal.
  const _CONTENT_OPS_RE = /\b(reporter|report[eé]r|journalist|periodist|redactor|redacc|correspondent|columnist|writer|editor|photograph|fot[oó]graf|graphic|designer|copy|clerk|cashier|mechanic|warehouse|driver|pressroom|circulation|imaging|literacy|custodian|delivery)\b/i;
  const _RELEVANT_RE = /\b(ceo|founder|co-?founder|owner|president|chief|cmo|cro|cto|publisher|propietari|due[ñn]|director|vp|vice president|head of|general manager|managing|marketing|advertis|publicidad|comercial|commercial|sales|ventas|revenue|monetiz|ad ?ops|ad ?operations|programmatic|program[aá]tic|media buy|inventory|yield|partnership|business development|bizdev|growth|digital|developer|engineer|programador|desarrollador|webmaster|\btech\b)\b/i;
  const _score = (t) => {
    const s = (t || "").toLowerCase();
    if (_CONTENT_OPS_RE.test(s)) return -1;   // periodista/editor/ops → NUNCA revelar
    if (!_RELEVANT_RE.test(s))   return -1;   // fuera de interés → NUNCA revelar
    if (/\b(advertis|publicidad|marketing|programmatic|program[aá]tic|monetiz|ad ?ops|comercial|commercial|revenue)\b/.test(s)) return 3; // core: publi/marketing/programmatic
    if (/\b(ceo|founder|owner|publisher|president|chief|cmo|cro|director|vp|head)\b/.test(s)) return 2;                                   // decisión
    return 1; // dev/tech/digital/sales
  };
  const ranked = people.filter(p => p?.id && _score(p.title) >= 0).sort((a, b) => _score(b.title) - _score(a.title));
  if (ranked.length === 0) { log(`  ○ Apollo ${domain}: sin roles de publi/marketing/tech → NO gasto crédito`); return freeResult; }
  for (const target of ranked.slice(0, 3)) {
    try {
      // Re-chequear cap antes de CADA reveal pago (no pasarse por probar varios).
      const u = await getApolloUsageToday(token).catch(() => ({ usedThisMonth: 0 }));
      if ((u.usedThisMonth || 0) >= APOLLO_MONTHLY_HARD_CAP) { log(`  ⚠ Apollo cap alcanzado mid-loop ${domain} — stop`); break; }
      const unlock = await fetch("https://api.apollo.io/v1/people/match", {
        method: "POST",
        headers: { "X-Api-Key": apolloKey, "Content-Type": "application/json" },
        body: JSON.stringify({ id: target.id, reveal_personal_emails: true }),
        signal: AbortSignal.timeout(12000),
      });
      if (!unlock.ok) continue;
      const data = await unlock.json();
      const person = data?.person;
      bumpApolloUnlocks(token, 1).catch(() => {}); // cada match cuesta 1 credit (tenga email o no)
      if (!person?.email) { log(`  ○ Apollo ${domain}: "${target.title || "?"}" sin email → probar siguiente`); continue; }
      const phone = _extractApolloPhone(person);
      log(`  💎 Apollo unlock ${domain} → ${person.email} (${target.title || "?"})`);
      const sourceUrl = person.linkedin_url || `https://app.apollo.io/people/${person.id || ""}`;
      const result = {
        email: person.email,
        contact_name: `${person.first_name||""} ${person.last_name||""}`.trim(),
        phone,
        source: "unlocked",
        source_url: sourceUrl,
      };
      if (token) saveApolloCacheServer(token, domain, { emails: [result.email], contact_name: result.contact_name, phone, source_url: sourceUrl, source: "worker_unlocked", title: target.title || "" }).catch(() => {});
      return result;
    } catch { continue; }
  }
  return freeResult;
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

// Maxi 2026-06-30: TLD 2-10 (paridad con el extractor del dashboard/popup). Antes 2-6
// rechazaba .travel/.museum/.online/.agency/.media y emails válidos quedaban afuera.
const EMAIL_REGEX  = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,10}(?=\s|$|[^a-zA-Z])/g;
const IGNORE_EMAIL = ["example.com","domain.com","sentry.io","google.com","w3.org","schema.org","cloudflare.com"];

// Maxi 2026-07-01: email_sources puede guardar un STRING ("scrape") o un OBJETO
// {url, source} según el path de escritura. Normaliza SIEMPRE a string. Antes se
// metía el objeto entero en el ranking y en toolbar_response_tracking.source →
// rompía el análisis por fuente Y el ranking dinámico (cada dominio quedaba como
// una "fuente" distinta, ej: {"url":"...","source":"scrape"}).
function _normSrc(v) { return typeof v === "string" ? v : (v && v.source) || ""; }

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
    // Maxi 2026-07-08: ELIMINADO el branch FIRSTNAME (/^[a-z]{3,12}$/) — era la CAUSA RAÍZ
    // de los locals mutilados vistos en prod (Search→earch, Values→alues, Contact→ontact,
    // Research→esearch): strippeaba la 1ra letra de CUALQUIER palabra capitalizada real, no
    // solo de un stray "C" pegado de "Contato:". Ahora solo se strippea si el remanente es un
    // ROL CONOCIDO (Cpublicidade→publicidade, Cinfo→info), único caso inequívoco de prefijo
    // basura. Un firstname legítimo mal-pegado ("Cluciano") se pierde, pero eso es MUCHO menos
    // dañino que mutilar palabras válidas (regla del dueño: no mutilar emails reales).
    if (KNOWN_LOCAL.test(stripped)) {
      return `${stripped}@${domain}`;
    }
  }
  return email.toLowerCase();
}

// ── Limpieza/filtrado de emails scrapeados (calidad de contactos) ──
const STRICT_EMAIL_RE = /^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$/;
// Dominios de registradores / WHOIS-privacy — nunca son el contacto del medio.
const JUNK_EMAIL_DOMAINS = new Set([
  "networksolutionsprivateregistration.com","whoisprotectservice.net","domainsbyproxy.com",
  "contactprivacy.com","privacyprotect.org","privacy-protect.org","whoisguard.com",
  "resellerid.com","latintld.com","web.com","networksolutions.com","godaddy.com",
  "namecheap.com","name.com","gandi.net","tucows.com","enom.com","registrarsafe.com",
  "1and1.com","ionos.com","cdsfulfillment.com",
]);
const JUNK_LOCAL_RE = /^(dominios?|domainoperations|tldadmin|hostmaster|registrar|registrarcontact|abuse|abusereport|dns-admin|postmaster|noc)$/i;
// Tokens de admin de dominio/DNS en cualquier parte del local-part
// (ej "tenmien.intecom", "admin-domaines-internet", "tdns").
const JUNK_LOCAL_TOKENS = /(^|[._-])(tenmien|tdns|dns|domain|domains|domaines|hostmaster|registrar|tldadmin|abuse|noc|nic)([._-]|$)/i;
// Placeholders de plantilla / buzones técnicos que nunca son contacto real.
// Maxi 2026-07-08: ampliado con placeholders de NOMBRE de plantilla en varios idiomas
// (vistos en prod, ej vorname.name@weltwoche.ch = "nombre.apellido" en alemán). Separador
// "." o "_". Un email con local "vorname.name"/"firstname.lastname"/etc. es una plantilla sin
// rellenar, jamás un contacto real.
const PLACEHOLDER_LOCAL = /(youremail|yourname|tuemail|tucorreo|webmail|noreply|no-reply|^email$|^e-mail$|^name$|^nombre$|^test$|^example$|^ejemplo$|placeholder|^user\d*$|^usuario\d*$|^guest\d*$|^demo$|^sample$|vorname[._]name|nombre[._]apellido|firstname[._]lastname|prenom[._]nom|nome[._]cognome|name[._]surname|ime[._]prezime|nome[._]sobrenome|max[._]mustermann|^mustermann$)/i;
// Dominios de webmail (gmail, hotmail, etc. en cualquier TLD).
const WEBMAIL_RE = /^(gmail|hotmail|outlook|live|yahoo|ymail|icloud|proton|protonmail|gmx|aol|msn|mail)\./i;

// ¿El local-part parece una persona real (nombre)? Usado para decidir si
// aceptamos un webmail scrapeado (juan.perez@gmail sí, info@gmail no).
function _looksLikePerson(local) {
  if (!local) return false;
  if (PLACEHOLDER_LOCAL.test(local) || _isGenericEmail(`${local}@x.com`)) return false;
  if (/^[a-z]+[._-][a-z]+/.test(local)) return true;            // nombre.apellido
  if (/^[a-z]{3,18}$/.test(local) && /[aeiou]/.test(local)) return true; // nombre solo
  return false;
}

// Saca artefactos de extracción (u003e de JSON escapado, backslash/control chars
// pegados, %20, prefijo "C"). Casos reales mayo: "u003enews@...", "news@x.tv\".
function _sanitizeEmail(raw) {
  if (!raw || typeof raw !== "string") return "";
  let e = raw;
  try { e = decodeURIComponent(e); } catch {}
  e = e.replace(/^(u00[0-9a-f]{2})+/i, "");        // u003e, u0022, ...
  e = e.replace(/[\x00-\x1f\x7f\\%]/g, "");        // control chars, backslash y % residual (ej "%hector@")
  e = e.replace(/^[^a-z0-9]+/i, "").trim().toLowerCase();  // cualquier basura al inicio (.>"'<% etc.)
  return _stripScrapePrefix(e);
}

// Filtra emails scrapeados: sanitiza, valida formato, descarta registradores/WHOIS,
// admin de dominio/DNS y placeholders. Acepta emails del dominio del lead; los
// webmail (gmail/hotmail) solo si parecen persona (nombre.apellido). Prioriza
// personas sobre genéricos y capea a 15. NO se aplica a emails de Apollo.
function _cleanScrapedEmails(list, leadDomain) {
  const core = (leadDomain || "").replace(/^www\./, "").toLowerCase().trim();
  const seen = new Set();
  const valid = [];
  for (const raw of Array.isArray(list) ? list : []) {
    const e = _sanitizeEmail(raw);
    if (!e || !STRICT_EMAIL_RE.test(e) || seen.has(e)) continue;
    if (IGNORE_EMAIL.some(p => e.includes(p))) continue;
    const local = e.split("@")[0];
    const dom   = e.split("@")[1];
    // Basura por local-part: roles de registro, admin de dominio/DNS, placeholders
    if (JUNK_LOCAL_RE.test(local) || JUNK_LOCAL_TOKENS.test(local) || PLACEHOLDER_LOCAL.test(local)) continue;
    // Basura por dominio: registradores/WHOIS
    if (JUNK_EMAIL_DOMAINS.has(dom)) continue;
    const isLeadDomain     = dom === core || dom.endsWith("." + core) || core.endsWith("." + dom);
    // Maxi 2026-07-08: gmail/hotmail/outlook/yahoo/etc. PUEDEN ser el contacto real del sitio
    // (el dueño lo aclaró explícitamente) → aceptar CUALQUIER webmail no-placeholder, sin
    // exigir que "parezca persona". Los locals placeholder/junk/registro ya se filtraron arriba
    // (JUNK_LOCAL_RE / PLACEHOLDER_LOCAL). rankEmail después penaliza webmail/genérico pero NO
    // lo descarta — así no perdemos un contacto real solo por venir de un dominio genérico.
    const isPersonalWebmail = WEBMAIL_RE.test(dom);
    // Maxi 2026-07-08: aceptar cross-domain si el local es un ROL de contacto de negocio
    // (publicidade@/comercial@/info@/prensa@...). Los GRUPOS de medios centralizan el contacto
    // de publicidad en el dominio de la editora — ej. publicidade@caras.com.br para
    // aventurasnahistoria.com.br (misma Editora Caras). Antes se descartaba → quedaba SIN email.
    // El junk (noreply/webmaster/registro) ya se filtró arriba; rankEmail después lo puntúa.
    const isBizRole = GENERIC_LOCAL_RE.test(local);
    // Solo: dominio del lead, webmail no-placeholder, o rol de negocio cross-domain. El resto fuera.
    if (core && !isLeadDomain && !isPersonalWebmail && !isBizRole) continue;
    seen.add(e);
    valid.push(e);
  }
  valid.sort((a, b) => (_isGenericEmail(a) ? 1 : 0) - (_isGenericEmail(b) ? 1 : 0));
  return valid.slice(0, 15);
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

// Maxi 2026-06-30: deobfuscador PORTADO VERBATIM del extractor del dashboard
// (modules/scraper.js → deobfuscate). Paridad total: si el MB lo encuentra a mano,
// el worker tiene que encontrarlo. Cubre entidades con ceros, &commat;/&period;,
// [at]/(at)/{at}/[arroba]/[a], [dot]/(dot)/{dot}/[punto]/[d], "user at domain dot com"
// (regex contextual preciso, no blunt), arroba/punto, y reverso ofuscado.
function _deobfuscateEmails(text) {
  if (!text) return "";
  text = text
    .replace(/&#0*64;|&#x0*40;/gi, "@").replace(/&#0*46;|&#x0*2e;/gi, ".")
    .replace(/&commat;/gi, "@").replace(/&period;/gi, ".");
  text = text
    .replace(/\[\s*at\s*\]/gi, "@").replace(/\(\s*at\s*\)/gi, "@").replace(/\{\s*at\s*\}/gi, "@")
    .replace(/\[\s*arroba\s*\]/gi, "@").replace(/\(\s*arroba\s*\)/gi, "@").replace(/\[\s*a\s*\]/gi, "@")
    .replace(/\[\s*dot\s*\]/gi, ".").replace(/\(\s*dot\s*\)/gi, ".").replace(/\{\s*dot\s*\}/gi, ".")
    .replace(/\[\s*punto\s*\]/gi, ".").replace(/\(\s*punto\s*\)/gi, ".").replace(/\[\s*d\s*\]/gi, ".");
  // Espacio AT espacio — contextual (solo cuando se parece a local@domain.tld)
  text = text.replace(
    /([a-zA-Z0-9._%+\-]{2,})\s+(?:at|AT)\s+([a-zA-Z0-9\-]{2,}(?:(?:\s+(?:dot|DOT)\s+|\s*\.\s*)[a-zA-Z]{2,})+)/g,
    (_, local, domain) => `${local}@${domain.replace(/\s+(?:dot|DOT)\s+/g, ".").replace(/\s*\.\s*/g, ".")}`
  );
  text = text.replace(/\barroba\b/gi, "@").replace(/\bpunto\b/gi, ".");
  if (/^[a-z0-9.\-_]{4,}\.(?:moc|gro|ten|ude|vog|moc\.[a-z]{2})@[a-z0-9.\-_]{2,}$/i.test(text.trim())) {
    text = text.split("").reverse().join("");
  }
  return text;
}

function extractEmailsFromHtml(html) {
  if (!html) return [];
  const collected = new Set();
  // 1) Regex sobre HTML deobfuscado (texto + atributos + scripts, todo junto)
  const clean = _deobfuscateEmails(html);
  (clean.match(EMAIL_REGEX) || []).forEach(e => collected.add(_stripScrapePrefix(e).toLowerCase()));
  // 2) Cloudflare data-cfemail decoder — gap común en sitios con CF Pro
  for (const m of html.matchAll(/data-cfemail=["']([a-f0-9]+)["']/gi)) {
    const decoded = _decodeCfEmail(m[1]);
    if (decoded && decoded.includes("@")) collected.add(decoded.toLowerCase());
  }
  // 3) JSON-LD schema.org "email": "x@y"
  for (const m of html.matchAll(/"email"\s*:\s*"([^"]+@[^"]+)"/gi)) collected.add(m[1].toLowerCase());
  // 4) Atributos data-* donde los sitios esconden el mail (paridad popup: + courriel,
  //    email-address, mailtolink). Si el valor parece hex CF, se intenta decodificar.
  for (const m of html.matchAll(/data-(?:email|mail|contact|correo|courriel|emailaddress|email-address|mailtolink)\s*=\s*["']([^"']+)["']/gi)) {
    const v = m[1];
    if (v.includes("@")) _deobfuscateEmails(v).match(EMAIL_REGEX)?.forEach(e => collected.add(e.toLowerCase()));
    else if (/^[a-f0-9]{6,}$/i.test(v)) { const d = _decodeCfEmail(v); if (d && d.includes("@")) collected.add(d.toLowerCase()); }
  }
  // 5) mailto: hrefs (a veces el único lugar con el mail real)
  for (const m of html.matchAll(/mailto:([^"'\s<>?]+@[^"'\s<>?]+)/gi)) collected.add(m[1].split("?")[0].toLowerCase());
  return [...collected].filter(e => {
    const lower = e.toLowerCase();
    if (IGNORE_EMAIL.some(p => lower.includes(p))) return false;
    const parts = e.split("@");
    if (parts.length !== 2) return false;
    const tld = parts[1].split(".").pop();
    return tld && tld.length >= 2 && tld.length <= 10;  // paridad con popup (.travel/.museum/.online)
  });
}

// Maxi 2026-07-16: extrae teléfonos + WhatsApp de HTML/texto. El user pidió capturar tel/WhatsApp
// (muy común en BR/otros: "fale conosco" con form + WhatsApp, ej. massa.com.br; footer con tel, ej.
// trikalaola.gr). Prioridad ALTA precisión: wa.me y tel: son señales fuertes; el número visible es
// más ruidoso → conservador. Devuelve { phones:[], whatsapps:[] } (WhatsApp = solo dígitos con país).
function extractPhonesFromHtml(text) {
  if (!text) return { phones: [], whatsapps: [] };
  const phones = new Set(), whatsapps = new Set();
  // WhatsApp: wa.me/<num>, api.whatsapp.com/send?phone=<num>, whatsapp://send?phone=<num>
  for (const m of text.matchAll(/(?:wa\.me\/|(?:api\.)?whatsapp\.com\/send\/?\?phone=|whatsapp:\/\/send\?phone=)(\+?\d[\d\s\-]{6,17}\d)/gi)) {
    const n = m[1].replace(/[^\d]/g, ""); if (n.length >= 8 && n.length <= 15) whatsapps.add(n);
  }
  // tel: hrefs (alta precisión)
  for (const m of text.matchAll(/tel:(\+?\d[\d\s().\-]{6,18}\d)/gi)) {
    const n = m[1].replace(/[^\d+]/g, ""); if (n.replace(/\D/g, "").length >= 8) phones.add(n);
  }
  // Números visibles con estructura de teléfono: +cc, (área) y separadores. Conservador (bajo ruido).
  for (const m of text.matchAll(/(?:\+\d{1,3}[\s.\-]?)?\(?\d{2,4}\)?[\s.\-]\d{3,4}[\s.\-]?\d{3,4}/g)) {
    const raw = m[0].trim(); const digits = raw.replace(/\D/g, "");
    if (digits.length >= 8 && digits.length <= 14 && !/^\d{4}[.\-]\d{2}[.\-]\d{2}$/.test(raw)) phones.add(raw.replace(/\s+/g, " "));
  }
  return { phones: [...phones].slice(0, 5), whatsapps: [...whatsapps].slice(0, 3) };
}

// Maxi 2026-06-17: Informer-only fetch. CHEAP, sin Apollo credit.
// Maxi 2026-06-18 fix bug thewordfinder.com: timeout 6s→12s, headers más
// realistas, URLs adicionales de WHOIS, deobfuscation reforzada.
async function scrapeInformerOnly(domain) {
  const cleanDomain = domain.replace(/^www\./, "");
  const headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Cache-Control": "no-cache",
  };
  const emails = new Set();
  const urlByEmail = new Map();
  const targets = [
    `https://website.informer.com/${cleanDomain}`,
    `https://website.informer.com/${cleanDomain}/whois`,    // subpath dedicado a contactos
    `https://who.is/whois/${cleanDomain}`,
    `https://www.whois.com/whois/${cleanDomain}`,
  ];
  await Promise.all(targets.map(async (url) => {
    try {
      const r = await fetch(url, { headers, signal: AbortSignal.timeout(12000) });
      if (!r.ok) return;
      const html = await r.text();
      const found = new Set();
      extractEmailsFromHtml(html).forEach(e => found.add(e));
      // Mailto links explícitos
      const mailtoMatches = html.matchAll(/mailto:([^"'\s<>?]+@[^"'\s<>?]+)/gi);
      for (const m of mailtoMatches) {
        const clean = m[1].toLowerCase().split("?")[0];
        if (clean.includes("@")) found.add(clean);
      }
      // Maxi 2026-06-18: Informer a veces presenta el email con espacios o
      // entities raras. Hacemos un segundo pass después de deobfuscar más
      // patrones (espacios, [at], (at), &amp;, &#x40;).
      const deobfuscated = html
        .replace(/&amp;#?64;/gi, "@").replace(/&#?64;|&#x40;/gi, "@")
        .replace(/&#?46;|&#x2e;/gi, ".").replace(/&commat;/gi, "@").replace(/&period;/gi, ".")
        .replace(/\s*\[\s*at\s*\]\s*/gi, "@").replace(/\s*\(\s*at\s*\)\s*/gi, "@")
        .replace(/\s+at\s+/gi, "@").replace(/\s+dot\s+/gi, ".");
      // Re-extract emails del HTML deobfuscado (esto agarra patrones que la
      // regex original puede haber perdido por espacios o entities)
      const passTwoRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,10}/g;
      (deobfuscated.match(passTwoRegex) || []).forEach(e => {
        const low = e.toLowerCase();
        // Filtrar basura típica de WHOIS (proxies y registrar emails)
        if (/abuse|whoisguard|domainsbyproxy|privacy|protection|registrar|noreply/.test(low)) return;
        found.add(low);
      });
      found.forEach(e => {
        const lower = e.toLowerCase();
        emails.add(e);
        if (!urlByEmail.has(lower)) urlByEmail.set(lower, url);
      });
    } catch {}
  }));
  return { emails: _cleanScrapedEmails([...emails], domain), urlByEmail };
}

// Devuelve true si el local-part es genérico (info@, contact@, etc.). Maxi
// quiere usar el primer Informer hit si NO es genérico.
// Maxi 2026-06-17 (audit #2): regex GENERIC_LOCAL_RE como única fuente de
// verdad. Antes _isGenericLocalPart, processCsvItem y autopilot usaban regex
// DIFERENTES → un email podía ser "no-genérico" para Informer pero "genérico"
// para rankEmail. Ahora todos consultan esta constante.
const GENERIC_LOCAL_RE = /^(info|contact|contacto|contato|contatto|contattare|kontakt|kontakte|hello|hi|hola|ola|olá|support|soporte|suporte|atendimento|mail|email|e-mail|inbox|news|press|prensa|imprensa|stampa|presse|sales|ventas|comercial|marketing|publicidade|publicidad|publicite|pubblicita|werbung|admin|general|generale|reception|recepcion|recepcao|webmaster|noreply|no-reply|no_reply|donotreply|do-not-reply|abuse|hostmaster|postmaster|spam|legal|dmca|copyright|takedown|privacy|gdpr|dpo|jobs|career|hr|recruit|talents)$/i;
function _isGenericLocalPart(email) {
  const local = (email || "").split("@")[0].toLowerCase();
  return GENERIC_LOCAL_RE.test(local);
}

// Maxi 2026-07-09: rol de VENTA DE PAUTA/PUBLICIDAD — el buzón IDEAL para ADEQ (que vende
// inventario). El user (Q4) lo eligió como "la mejor opción". Módulo-level para compartir entre
// rankEmail (score +95) y _pickTier (orden de selección). Formas ACOTADAS: nada de `ads?`/`adv`
// sueltos, que matchearían "admin"/"advisor".
// Maxi 2026-07-13 (auditoría): +cobertura del pool europeo — régie(FR), Vermarktung/Anzeigen/Verkauf(DE),
// verkoop/adverteren(NL), vente(FR), raccolta pubblicitaria(IT), auglýsingar(IS), annons(SE). Todos = venta
// de pauta/inventario. 'regie\b'/'regiepub' evita matchear 'regierung'(gobierno DE).
const AD_SALES_LOCAL = /^(?:publicidad|publicidade|publicit[ea]|pubblicit|werbung|vermarkt|advertis|advert\b|\badv\b|ads\b|ad[-_.]?sales|adverten|anunci|anzeigen|reklam|iklan|regiepub|regie\b|comercial|commercial|ventas|vendas|vente|verkauf|verkoop|sales\b|salesteam|marketing|mktg?\b|monetiz|media[-_.]?sales|raccolta|auglys|annons|inventory|programmatic|patrocin|sponsor)/i;

// Maxi 2026-06-17 v4: extrae emails publicados en redes sociales (FB about,
// YT about, Twitter bio via Nitter). Fallback worker — solo se llama cuando
// el scrape normal no encontró email NO-genérico. Devuelve Map<email, "Facebook"|"YouTube"|"Twitter">.
async function _scrapeEmailsFromSocialLinksWorker(socialLinks) {
  const found = new Map();
  if (!Array.isArray(socialLinks) || socialLinks.length === 0) return found;
  const UA_MOBILE = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1";
  const UA_DESKTOP = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";

  const tryFetchSocial = async (url, source, ua = UA_DESKTOP, timeout = 5000) => {
    try {
      const r = await fetch(url, {
        headers: { "User-Agent": ua, "Accept-Language": "en-US,en;q=0.9" },
        signal: AbortSignal.timeout(timeout),
        redirect: "follow",
      });
      if (!r.ok) return;
      const html = await r.text();
      extractEmailsFromHtml(html).forEach(e => {
        if (!found.has(e)) found.set(e, source);
      });
    } catch {}
  };

  const seen = new Set();
  const tasks = [];
  for (const link of socialLinks) {
    const lower = link.toLowerCase();
    if (lower.includes("facebook.com") && !seen.has("fb")) {
      seen.add("fb");
      const m = lower.match(/facebook\.com\/(?:pg\/)?([a-z0-9._-]{2,100})/i);
      const page = m && !["pages","groups","events","sharer","home.php","login","watch"].includes(m[1]) ? m[1] : null;
      if (page) {
        tasks.push(tryFetchSocial(`https://m.facebook.com/${page}/about`,                      "Facebook", UA_MOBILE, 6000));
        tasks.push(tryFetchSocial(`https://m.facebook.com/${page}/about_contact_and_basic_info`, "Facebook", UA_MOBILE, 6000));
      }
    }
    if (lower.includes("youtube.com") && !seen.has("yt")) {
      seen.add("yt");
      const m = lower.match(/youtube\.com\/(@[a-z0-9._-]+|c\/[a-z0-9._-]+|channel\/[a-z0-9._-]+|user\/[a-z0-9._-]+)/i);
      if (m) tasks.push(tryFetchSocial(`https://www.youtube.com/${m[1]}/about`, "YouTube", UA_DESKTOP, 6000));
    }
    // Twitter/X DESACTIVADO (Maxi 2026-07-01, B8): nitter.net caído en 2026 → fetch fallaba
    // siempre gastando 6s. twitter.com/x.com con login wall. Re-agregar si hay Nitter vivo.
  }
  await Promise.all(tasks);
  return found;
}

async function scrapeEmailsForDomain(domain, opts = {}) {
  // Trae emails de muchas fuentes con concurrencia limitada (4 a la vez).
  // Usa User-Agent real (Chrome) para evitar anti-bot blocks comunes.
  // opts.informerOut: Set opcional — recibe los emails que vinieron de
  // website.informer.com / who.is, para que el caller los etiquete "informer".
  // opts.urlByEmail: Map opcional — recibe email→URL de origen para tracking UI
  // (user 2026-06-17: poder mostrar de qué URL salió cada scraped email).
  const informerOut = opts.informerOut || null;
  const urlByEmail  = opts.urlByEmail  || null;
  // Maxi 2026-06-17 v4: socialOut Map recibe emails extraídos de redes
  // sociales (FB business email, YT contact for business). Source: "Facebook",
  // "YouTube", "Twitter".
  const socialOut   = opts.socialOut   || null;
  // Maxi 2026-06-18: contactFormsOut recibe URLs de formularios de contacto.
  // Si el sitio no tiene email pero tiene contact form, el MB puede usarlo.
  const contactFormsOut = opts.contactFormsOut || null;
  const emails = new Set();
  // socialLinks detectados durante el scraping para hacer fetch posterior
  const detectedSocialLinks = new Set();
  // Maxi 2026-07-09: links de contacto/impressum/publicidad REALES cosechados del HTML del home.
  // El user pidió "garantizar que agarra lo que está escrito en la web". En vez de SOLO adivinar
  // rutas, seguimos los links que el sitio realmente publica (contacto/kontakt/impressum/aviso
  // legal/publicidad/media-kit/about/equipo), aunque tengan nombres no estándar.
  const discovered = new Set();
  // Maxi 2026-07-14: cobertura MULTILINGÜE del link de contacto/publicidad/about. El email vive en la
  // página de contacto y su slug depende del idioma (no todos son /contacto). Cubre PT-BR (contato/
  // fale-conosco), TR (iletisim/reklam/hakkimizda), HU (kapcsolat/hirdet), ID (kontak/hubungi/iklan),
  // VN (lien-he/quang-cao), FI (yhtey/maino), GR (epikoin), SE/NO (annons/om-oss), IS (auglys), PL/CZ/RU
  // (kontak/kontakty/reklam/o-nas), RO (despre). Caso testigo: sorteador.com.br → /contato.
  const CONTACT_HINT = /contact|contacto|contato|contatt|fale[-_ ]?conosco|kontak|kontakty|iletis|kapcsolat|hubungi|lien[-_ ]?he|yhtey|epikoin|impress?um|imprint|mentions?-?l[eé]gal|aviso-?legal|note-?legal|\blegal\b|publicidad|publicidade|publicit[eé]|pubblicit|werbung|reklam|hirdet|iklan|quang[-_ ]?cao|annons|auglys|maino|mediadaten|media-?kit|advertis|anunci|about|sobre|qui[eé]n|quem-?somos|chi-?siamo|nosotros|hakkimizda|o-?nas|despre|tentang|om-?oss|equipe?|\bteam\b|\bstaff\b|redac|ueber-?uns|über-?uns|impronta/i;
  const base   = `https://${domain}`;
  const cleanDomain = domain.replace(/^www\./, "");
  // Chrome real para evitar bloqueos por User-Agent de bot
  const uaChrome = { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36" };
  const uaMobile = { "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1" };

  // Maxi 2026-06-30: reintento UNA vez si el fetch se corta por timeout. Los sitios
  // lentos (los que más emails pierden) antes morían al primer AbortSignal de 4s →
  // ahora el 2º intento usa un timeout 1.6× más largo. redirect:"follow" explícito.
  const tryFetch = async (url, timeout = 5000, mobile = false, isInformer = false) => {
   let _attempt = 0;
   while (_attempt < 2) {
    try {
      const r = await fetch(url, { headers: mobile ? uaMobile : uaChrome, redirect: "follow", signal: AbortSignal.timeout(timeout) });
      if (!r.ok) return;
      const html = await r.text();
      const found = new Set();
      // 1) regex emails en HTML general
      extractEmailsFromHtml(html).forEach(e => found.add(e));
      // 2) explicit mailto: hrefs (raramente missed pero a veces hay solo ahí)
      const mailtoMatches = html.matchAll(/mailto:([^"'\s<>?]+@[^"'\s<>?]+)/gi);
      for (const m of mailtoMatches) {
        const clean = m[1].toLowerCase().split("?")[0];
        if (clean.includes("@")) found.add(clean);
      }
      found.forEach(e => {
        const lower = e.toLowerCase();
        emails.add(e);
        if (isInformer && informerOut) informerOut.add(lower);
        // Track origen URL: solo primera vez (URL más temprana = más cerca de home/contact)
        if (urlByEmail && !urlByEmail.has(lower)) urlByEmail.set(lower, url);
      });
      // Maxi 2026-06-17 v4: detectar social media links en el HTML
      const SOCIAL_RE = /(?:facebook\.com|youtube\.com|twitter\.com|x\.com)\/[A-Za-z0-9._@\-]{2,80}/gi;
      const socialMatches = html.match(SOCIAL_RE) || [];
      socialMatches.forEach(s => detectedSocialLinks.add(s.toLowerCase()));
      // Maxi 2026-06-18: detectar contact form URLs en el HTML. Patrones típicos:
      // /contactform, /contact-form, /contact.php, /sendmail, etc.
      if (contactFormsOut) {
        // 1) URLs explícitas que contienen "contactform" o "contact-form" o "contact_form"
        const CF_URL_RE = /\/(?:contactform|contact[-_]form|contact-us-form|formulario[-_]contacto)(?:\.[a-z]+)?(?:[\/?][^"' >]*)?/gi;
        const cfMatches = html.match(CF_URL_RE) || [];
        cfMatches.forEach(p => {
          let abs;
          try { abs = new URL(p.split('"')[0].split("'")[0], base).href; } catch { abs = base + p; }
          contactFormsOut.add(abs);
        });
        // 2) <form action="..."> que termina en algo tipo contact, sendmail, formulario
        const FORM_ACTION_RE = /<form[^>]+action\s*=\s*["']([^"']+(?:contact|formulario|sendmail|send-mail)[^"']*)["']/gi;
        for (const m of html.matchAll(FORM_ACTION_RE)) {
          try {
            const abs = new URL(m[1], base).href;
            contactFormsOut.add(abs);
          } catch {}
        }
      }
      // Maxi 2026-07-09: cosechar links de contacto/impressum/publicidad REALES del HTML
      // (mismo dominio, cap 16) para seguirlos en la fase 2. Así agarramos la página de
      // contacto aunque el sitio la nombre distinto (ej. /en/contact-us-2, /kontakt-impressum).
      if (discovered.size < 16) {
        for (const m of html.matchAll(/<a\b[^>]*href\s*=\s*["']([^"'\s>]+)["'][^>]*>([\s\S]{0,140}?)<\/a>/gi)) {
          if (discovered.size >= 16) break;
          const href = m[1];
          if (!href || /^(mailto:|tel:|javascript:|#)/i.test(href)) continue;
          const anchor = (m[2] || "").replace(/<[^>]+>/g, " ");
          if (!CONTACT_HINT.test(href) && !CONTACT_HINT.test(anchor)) continue;
          let abs; try { abs = new URL(href.split("#")[0], base).href; } catch { continue; }
          try { const h = new URL(abs).hostname.replace(/^www\./, ""); if (h !== cleanDomain && !h.endsWith("." + cleanDomain)) continue; } catch { continue; }
          discovered.add(abs);
        }
      }
      return; // éxito → no reintentar
    } catch (e) {
      const isTimeout = e?.name === "TimeoutError" || e?.name === "AbortError";
      if (_attempt === 0 && isTimeout) { _attempt++; timeout = Math.round(timeout * 1.6); continue; }
      return;
    }
   }
  };

  // Paths de fallback (por si el home no linkeó su página de contacto — SPAs, JS). Cubre
  // EN/ES/PT/IT/FR/DE. Maxi 2026-07-09: reforzado con Impressum/Kontakt (DE), mentions-légales
  // (FR), contatti/pubblicita (IT) y media-kit/mediadaten — donde los sitios EU esconden el email.
  const paths = [
    "", // home
    "/contact", "/contact-us", "/contactus", "/contacto", "/contactanos", "/contacto-nos",
    "/contatti", "/contactez-nous", "/kontakt", "/kontaktformular", "/kontakt-impressum",
    "/contato", "/fale-conosco", "/faleconosco", "/fale-conosco.html", "/fale-connosco", "/anuncie", "/anuncie-conosco", // BR/PT (Maxi 2026-07-14)
    // Maxi 2026-07-14: contacto/publicidad localizados por idioma (el email vive acá y el slug cambia).
    "/iletisim", "/reklam", "/hakkimizda",                 // TR
    "/kapcsolat", "/hirdetes", "/rolunk",                  // HU
    "/kontak", "/hubungi-kami", "/hubungi", "/iklan", "/pasang-iklan", "/tentang-kami", // ID
    "/lien-he", "/lienhe", "/quang-cao", "/gioi-thieu",    // VN
    "/yhteystiedot", "/ota-yhteytta", "/mainonta",         // FI
    "/kontakty", "/reklama", "/o-nas",                     // PL/CZ/RU
    "/contacte", "/despre-noi", "/publicitate",            // RO
    "/epikoinonia",                                        // GR
    "/annonsera", "/om-oss", "/auglysingar",               // SE/NO/IS
    "/about", "/about-us", "/aboutus", "/sobre", "/sobre-nos", "/sobre-nosotros", "/quienes-somos",
    "/chi-siamo", "/qui-sommes-nous", "/ueber-uns",
    "/team", "/equipo", "/equipe", "/staff", "/nosotros",
    "/advertise", "/advertising", "/advertise-with-us", "/publicidad", "/publicidade",
    "/anunciantes", "/anunciar", "/publicite", "/regie-publicitaire", "/pubblicita", "/werbung",
    "/media-kit", "/mediakit", "/mediadaten",
    "/redaccion", "/redacao", "/editorial", "/noticias",
    "/aviso-legal", "/legal", "/impressum", "/impressum.html", "/imprint",
    "/mentions-legales", "/note-legali", "/politica-privacidad", "/privacy",
    "/footer", "/site-map", "/sitemap",
  ];
  const internalTargets = paths.map(p => `${base}${p}`);

  // Fuentes externas (WHOIS / informer) — emails de baja calidad (suelen ser del
  // registrador). Maxi 2026-06-30: van AL FINAL. Con el early-stop por email-real,
  // las páginas internas (home/contacto/publicidad) tienen el decision-maker bueno;
  // si lo encontramos ahí, ni gastamos en WHOIS. Solo corren si internas no dieron nada.
  const externalTargets = [
    `https://website.informer.com/${cleanDomain}`,
    `https://who.is/whois/${cleanDomain}`,
  ];

  // Maxi 2026-07-09: orquestación en 3 FASES para GARANTIZAR captura del email escrito.
  // El user aceptó que tarde unos segundos más a cambio de agarrar sí o sí lo que está en la web.
  const CONCURRENT = 4;
  // FASE 1 — HOME solo. De su HTML cosechamos los links de contacto/impressum/publicidad REALES
  // que el sitio publica (llena `discovered`), además de los emails que ya estén en el home.
  await tryFetch(base, 6000, false, false);

  let _hasReal = [...emails].some(e => !_isGenericLocalPart(e));
  // FASE 2 — seguir los links DESCUBIERTOS primero (el sitio los nombró explícitamente → más
  // precisos), luego las rutas estáticas de fallback. Early-stop al primer email real / buen lote.
  if (!_hasReal) {
    const seenUrl = new Set([base, base + "/"]);
    const queue = [];
    for (const u of [...discovered, ...internalTargets]) {
      if (u === base) continue;
      const norm = u.replace(/\/+$/, "");
      if (seenUrl.has(u) || seenUrl.has(norm)) continue;
      seenUrl.add(u); seenUrl.add(norm);
      queue.push(u);
    }
    for (let i = 0; i < queue.length; i += CONCURRENT) {
      await Promise.all(queue.slice(i, i + CONCURRENT).map(url => tryFetch(url, 6000)));
      _hasReal = [...emails].some(e => !_isGenericLocalPart(e));
      if (_hasReal || emails.size >= 14) break;
    }
  }

  // FASE 3 — WHOIS/informer SOLO si seguimos con CERO emails (baja calidad, último recurso).
  if (emails.size === 0) {
    for (const u of externalTargets) await tryFetch(u, 8000, false, true);
  }

  // Última chance — si NADA encontrado, probar mobile UA en home (algunos sites cambian)
  if (emails.size === 0) {
    await tryFetch(base, 7000, true);
  }

  // Maxi 2026-06-17 v4: si después de todo NO hay email NO-genérico, probar
  // extraer emails de las redes sociales detectadas en el HTML. Solo como
  // fallback para no costar bandwidth en cada lead.
  const hasNonGeneric = [...emails].some(e => !_isGenericLocalPart(e));
  if (!hasNonGeneric && detectedSocialLinks.size > 0) {
    log(`  📱 ${domain}: scraping social media (${detectedSocialLinks.size} links) buscando email…`);
    const socialResults = await _scrapeEmailsFromSocialLinksWorker([...detectedSocialLinks]).catch(() => new Map());
    socialResults.forEach((src, em) => {
      const lower = em.toLowerCase();
      emails.add(em);
      if (socialOut) socialOut.set(lower, src);
      if (urlByEmail && !urlByEmail.has(lower)) {
        // marcar la URL social como origen
        const u = [...detectedSocialLinks].find(l => l.includes(src.toLowerCase())) || src;
        urlByEmail.set(lower, "https://" + u);
      }
    });
    if (socialResults.size > 0) {
      log(`  📱 ${domain}: ${socialResults.size} email(s) extraídos de redes sociales`);
    }
  }

  // Limpieza final: sanitiza, descarta registradores/WHOIS y ajenos al dominio,
  // prioriza personas sobre genéricos y capea a 15. Corta el ruido del scraping.
  return _cleanScrapedEmails([...emails], domain);
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
    if (/outbrain/i.test(html))                                            adNetworks.push("Outbrain");   // Maxi 2026-07-15 (D2): faltaba → un publisher con Outbrain podía purgarse
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
    if (/teads\.tv|teads\.com/i.test(html))                                adNetworks.push("Teads");
    if (/smilewanted\.com/i.test(html))                                    adNetworks.push("SmileWanted");

    // Maxi 2026-06-19 (filtro basura B2): señal de MONETIZACIÓN REAL — AdSense /
    // Google Ad Manager / programmatic. Más universal que ads.txt: si una web
    // corre esto, es un publisher monetizable de verdad. Gov/uni/corp/SaaS casi
    // nunca lo tienen. Es el discriminador positivo más fuerte (más que partner-SSP).
    const PROGRAMMATIC_RE = /(pagead2\.googlesyndication|adsbygoogle|googletagservices|securepubads|securepubads\.g\.doubleclick|googletag\.cmd|div-gpt-ad|data-ad-slot|doubleclick\.net|amazon-adsystem|aps\.amazon|criteo|pubmatic|rubiconproject|magnite|openx\.net|prebid\.js|prebidjs|adnxs\.com|appnexus|33across|sovrn\.com|indexexchange|casalemedia|smartadserver|adform\.net|yieldmo|sharethrough|gumgum|adsrvr\.org|adservice\.google|fundingchoicesmessages)/i;
    const hasProgrammatic = PROGRAMMATIC_RE.test(html);
    const hasDisplayAds   = hasProgrammatic || adNetworks.length > 0;
    // Maxi 2026-07-15 (caso allhiphop.com purgado como ecommerce): ad-tech de PUBLISHER = vende su
    // inventario a TERCEROS (AdSense/GPT/SSP/Taboola). Una TIENDA NO corre esto (mostrar ads de otros en
    // sus product pages mandaría tráfico afuera) — a lo sumo tiene retargeting (criteo/adsrvr/adform), que
    // NO cuenta acá. Un publisher con tienda de merch (allhiphop) SÍ tiene publisher-ads → vetea isStore.
    const PUBLISHER_ADS_RE = /(pagead2\.googlesyndication|adsbygoogle|googletagservices|securepubads|div-gpt-ad|data-ad-slot|googletag\.cmd|amazon-adsystem|aps\.amazon|pubmatic|rubiconproject|magnite|openx\.net|prebid\.js|prebidjs|adnxs\.com|appnexus|33across|sovrn\.com|indexexchange|casalemedia|smartadserver|yieldmo|sharethrough|gumgum|fundingchoicesmessages)/i;
    const hasPublisherAds = PUBLISHER_ADS_RE.test(html) || adNetworks.length > 0;

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

    // ── Maxi 2026-07-08: DETECTOR ESTRUCTURAL de TIPO DE SITIO (no-publisher) ────
    // El user pidió filtrar por CÓMO está construido el sitio (no por categoría/tráfico).
    // Publishers monetizan con ads; TIENDAS, BANCOS, UNIVERSIDADES y EMPRESAS DE SERVICIOS
    // NO son targets aunque tengan pixel de retargeting o ads.txt. Se detecta por schema.org
    // @type (señal FUERTE = 1 basta) + keywords de intención (señal débil = necesita 2).
    const _hits = (arr) => arr.reduce((n, re) => n + (re.test(html) ? 1 : 0), 0);

    // TIENDA / E-COMMERCE — ALTA PRECISIÓN. Maxi 2026-07-09: el detector viejo daba FALSOS
    // POSITIVOS en publishers (clarin/chequeado/kiwilimon) por "@type":Offer suelto y cart/basket/
    // price sueltos ("basket"→basketball, Offer→suscripción, price→precio de nota). Ahora SOLO:
    // firma de PLATAFORMA real, o botón add-to-cart + ruta de checkout JUNTOS, o og:type=product
    // + botón. Validado 0 FP sobre 33 publishers de la lista del user.
    // Solo plataformas DEDICADAS de e-commerce (un sitio de contenido no las carga). WooCommerce/
    // Magento SE SACARON del 1-hit: muchos medios en WordPress usan Woo para paywall/suscripción/
    // merch → se detectan igual por botón-carrito + checkout (abajo), como cualquier tienda.
    // Shopify: exigimos la firma del STOREFRONT (globals JS que solo existen cuando la PÁGINA es la
    // tienda), NO un link a *.myshopify.com ni un asset cdn.shopify.com — muchos PUBLISHERS enlazan o
    // embeben una tienda de merch en Shopify (ej. americasvoice.news → realamericasvoice.myshopify.com)
    // y NO son ecommerce. Shopify.shop/Shopify.theme/window.Shopify/shopify-section = storefront real.
    const storePlatform = /Shopify\.theme|Shopify\.shop|Shopify\.routes|window\.Shopify|id=["']shopify-section|vteximg|vtexassets|vtexcommercestable|portal\.vtex|\/\/[a-z0-9-]+\.vtex\.|nuvemshop|tiendanube|lojaintegrada|prestashop|bigcommerce|demandware|dwstatic|\/on\/demandware/i;
    const storeCartBtn  = /add[\s-]?to[\s-]?cart|a[ñn]adir al carrito|agregar al carr(o|ito)|adicionar ao carrinho|sepete ekle|a[ñn]adir a la (cesta|bolsa)|in den warenkorb/i;
    const storeCheckout = /\/(checkout|cart\/add|onepage|finalizar-compra|finalizar_compra|sepet|carrinho)\b|class=["'][^"']*(add-to-cart|addtocart|btn-cart|buy-button)/i;
    const storeOgProduct = /og:type["'][^>]*content=["']product["']/i;
    const isStore = storePlatform.test(html) || (storeCartBtn.test(html) && storeCheckout.test(html)) || (storeOgProduct.test(html) && storeCartBtn.test(html));

    // Schema.org @type de ENTIDAD COMERCIAL (señal FUERTE, 1 = rechazo). Los publishers NO lo
    // llevan. Banco/seguro, universidad, hotel/viajes/alquiler, ONG, inmobiliaria.
    const bankSchema   = /"@type"\s*:\s*"(BankOrCreditUnion|FinancialService|InsuranceAgency)"/i;
    const eduSchema    = /"@type"\s*:\s*"(CollegeOrUniversity|EducationalOrganization|School|University)"/i;
    const travelSchema = /"@type"\s*:\s*"(Hotel|LodgingBusiness|Resort|TravelAgency|AutoRental|RentACar|Campground|BedAndBreakfast)"/i;
    const npoSchema    = /"@type"\s*:\s*"(NGO|NonprofitOrganization|Charity)"/i;
    const realtySchema = /"@type"\s*:\s*"(RealEstateListing|RealEstateAgent|Residence|Apartment|SingleFamilyResidence)"/i;
    const govSchema    = /"@type"\s*:\s*"(GovernmentOrganization|GovernmentService|GovernmentOffice|GovernmentBuilding)"/i;
    // Maxi 2026-07-14 (auditoría, casos hpc.org.ar/unitedpharmacy.sa): entidades de SALUD físicas —
    // un publisher de salud (netdoktor/medical-news) NO se auto-marca Hospital/Pharmacy/Clinic.
    const healthSchema = /"@type"\s*:\s*"(Hospital|MedicalClinic|Clinic|Pharmacy|Physician|Dentist|DiagnosticLab|MedicalBusiness|MedicalOrganization)"/i;
    // Maxi 2026-07-15 (taxonomía confirmada por el user): servicios profesionales/local-business,
    // instituciones (iglesias/museos/bibliotecas) y SaaS/apps. Un publisher NO se auto-marca así.
    // El user PIDIÓ NO bloquear: herramientas online, marketplaces de assets, developer/técnico.
    const serviceSchema = /"@type"\s*:\s*"(LocalBusiness|ProfessionalService|Attorney|LegalService|Notary|AccountingService|Electrician|Plumber|HVACBusiness|RoofingContractor|HousePainter|Locksmith|MovingCompany|GeneralContractor|HomeAndConstructionBusiness|Restaurant|FoodEstablishment|CafeOrCoffeeShop|BarOrPub|Bakery|Brewery|Winery|AutoDealer|AutoRepair|AutoBodyShop|GasStation|HairSalon|BeautySalon|NailSalon|DaySpa|HealthAndBeautyBusiness|TattooParlor|FuneralHome|ChildCare|DryCleaningOrLaundry|EmploymentAgency|SelfStorage|ExerciseGym|HealthClub|SportsClub|SportsActivityLocation|VeterinaryCare|Optician|NightClub|AmusementPark|EntertainmentBusiness)"/i;
    const instSchema    = /"@type"\s*:\s*"(Church|Mosque|Synagogue|HinduTemple|BuddhistTemple|PlaceOfWorship|Museum|Library|ArchiveOrganization)"/i;
    const saasSchema    = /"@type"\s*:\s*"(SoftwareApplication|WebApplication|MobileApplication)"/i;
    // Maxi 2026-07-15 (user "corregilo"): corporate-brochure y personal/portfolio por schema.
    const corpSchema    = /"@type"\s*:\s*"Corporation"/i;                    // Corporation (NO Organization, que usan publishers)
    const personalSchema = /"@type"\s*:\s*"(ProfilePage|ResumeAction)"/i;
    // Piratería / brand-unsafe: se excluye AUNQUE tenga ads (decisión de brand-safety, no de monetización).
    // Señales fuertes y específicas para no pegar un artículo que MENCIONE el tema.
    const piracyRe = /magnet:\?xt=urn:btih|\.torrent["'\s>]|\b(putlocker|123movies|fmovies|solarmovie|thepiratebay|1337x|rarbg|nyaa\.si)\b|read\s+manga\s+online\s+free/i;

    // Frases inequívocas y ACOTADAS (un publisher jamás las usa como intención propia). GATE por
    // !hasProgrammatic: un banco/hotel/universidad/inmobiliaria/tienda en su PROPIO sitio no corre
    // programmatic display; un publisher (incl. finance-news, travel-blog, education-content) SÍ.
    // Eso evita los FP en bolsamania/expansion/gurudeviaje/mundoprimaria (validado 0 FP).
    const bankKw   = [/online banking|internet banking|home ?banking|banca (digital|en l[íi]nea)|neobank|acesse sua conta|abr[ai] (sua|tu) conta/i, /abrir (tu |una )?cuenta( corriente| de ahorro| bancaria)|open (a |your )?bank account|conta corrente/i];
    const travelKw = [/car (hire|rental)|rent a car|alquiler de (coches?|autos?)|pauschalreise|urlaubsangebote|hotel buchen|best rate guarantee|book your (stay|room)/i, /reserva (tu |una )?(habitaci[óo]n|estancia)|habitaciones disponibles|mejor tarifa garantizada/i];
    const eduKw    = [/proceso de admisi[óo]n|solicita tu (admisi[óo]n|plaza)|admisiones abiertas/i, /oferta acad[ée]mica|vida universitaria|nuestras? (titulaciones|carreras universitarias)|campus universitario/i];
    const svcKw    = [/solicita(r)? (tu |un )?presupuesto|pide presupuesto|request a (demo|quote)|solicita una demo|book a demo/i, /nuestros servicios profesionales|market research|investigaci[óo]n de mercado|consultor[íi]a (empresarial|estrat[ée]gica)/i];
    const npoKw    = [/donate now|make a donation|registered charity|become a volunteer|hacer una donaci[óo]n/i, /recaudaci[óo]n de fondos|fundraising campaign|apoya (nuestra|la) causa/i];
    const realtyKw = [/pisos? en (venta|alquiler)|propiedades? en (venta|alquiler)|casas? en venta|im[óo]veis (para|à) (venda|alugar)/i, /publica(r)? tu (anuncio|propiedad) gratis|m² (construidos|[úu]tiles)|\d+ dormitorios/i];
    // Maxi 2026-07-15 (user "corregilo"): corporate-brochure, transaccionales (hosting/pago/VPN/citas),
    // personal/portfolio. Keywords FP-prone → gate por !hasDisplayAds (abajo). Requieren 2 hits (personal 1, muy específico).
    const corpKw     = [/\b(wholesale|mayorista|distribuidor|distributor|fabricante|manufacturer|OEM|ISO ?900\d)\b/i, /(our (products|solutions)|nuestros productos|nossos produtos|request a (quote|demo)|solicite (una |un )?cotizaci[óo]n|solutions for your business|soluciones para (tu|su) (empresa|negocio))/i];
    const svcPlatKw  = [/\b(web hosting|shared hosting|reseller hosting|dedicated server|cpanel|alojamiento web|hospedagem|payment gateway|pasarela de pago|merchant account|no-logs vpn|vpn service)\b/i, /(register (your |a )?domain|registra tu dominio|ssl certificate|certificado ssl|accept payments online|acept[aá] pagos|hide your ip|unblock (streaming|websites)|servers in \d+ countries)/i];
    const datingKw   = [/\b(dating (site|app|service)|online dating|citas online|encuentra pareja)\b/i, /(find (your )?match|singles (near you|in your area)|create (your )?(free )?profile|crea tu perfil gratis)/i];
    const personalKw = [/(my portfolio|mi portafolio|meu portf[óo]lio|hire me|contrat[aá]me|available for (hire|freelance)|freelance (designer|developer|writer|photographer|consultant))/i, /(my (resume|cv|work)|view my work|book a (session|shoot)|get in touch to work together)/i];
    // Maxi 2026-07-16 (ejemplos del user): cripto compra/venta/exchange (blockchaincenter.net) y
    // dev-tool/librería/docs (vueuse.org) NO son publishers. Gateados por !hasDisplayAds abajo (un
    // medio de cripto/tech CON ads pasa; solo el sitio que ES el exchange/la herramienta sin ads se caza).
    const cryptoKw   = [/\b(buy|sell|trade|comprar|vender|compr[aá]|vend[eé]) (bitcoin|btc|ethereum|eth|crypto|cripto|criptomonedas?|usdt|tokens?)\b/i, /\b(crypto|cripto) ?(exchange|wallet|trading|broker)\b|casa de cambio (de )?cripto|spot trading|futures trading|derivatives exchange|(deposit|withdraw|retir[aá]|deposit[aá]) (crypto|usdt|fondos)|conect[aá] tu wallet|connect wallet/i];
    const devToolKw  = [/\b(npm install|yarn add|pnpm add|pip install|composer require|go get |cargo add)\b/i, /\b(api reference|getting started|read the docs|open[- ]?source (library|framework|tool)|github stars?|available on (npm|pypi|packagist|crates)|sdk for developers|contribute on github)\b/i];
    // Maxi 2026-07-16 (ejemplos del user): APP (landing de app móvil/citas/servicio, ej. babel.com) y
    // TIENDA online de refuerzo (ej. tokyointerior-onlineshop.com, si el detector de plataforma no la agarró).
    // Gateados por !hasDisplayAds → un medio con app propia o merch + ads sigue pasando (regla de oro).
    const appKw      = [/\b(download (the |our |your )?app|descarg[aá] (la |nuestra )?app|baixe (o |nosso )?app|get the app|t[ée]l[ée]charge[rz] (l'?|notre )app)\b/i, /\b(on the app ?store|on google play|download on the app store|get it on google play|disponible en (el )?app ?store|disponible en google play|available for (ios|android)|para (ios|android))\b/i];
    const shopKw     = [/\b(add to (cart|basket|bag)|a[ñn]adir al carrito|agregar al carrito|adicionar ao carrinho|aggiungi al carrello|comprar ahora|buy now|comprar agora|finalizar compra|mi carrito|shopping cart|free shipping|env[íi]o gratis|frete gr[áa]tis)\b/i, /\b(online shop|tienda online|loja online|negozio online|boutique en ligne|our (store|shop)|nuestra tienda|nossa loja|(in|out of) stock|sold out|agotado)\b/i];

    let nonPublisherType = null;
    // Maxi 2026-07-15: isStore ya NO es "aunque tenga ads" — si el sitio corre ad-tech de PUBLISHER
    // (AdSense/GPT/SSP/Taboola) es un medio con tienda de merch (allhiphop.com), NO una tienda. Veto.
    if (isStore && !hasPublisherAds) nonPublisherType = "ecommerce";
    // Maxi 2026-07-13 (auditoría 48h): schema de ENTIDAD que un publisher JAMÁS lleva describiéndose
    // a SÍ MISMO → rechazo AUNQUE tenga ads (un banco/aseguradora/universidad/gobierno mete pixel de
    // retargeting → "monetizado" → antes se colaba: axa/sympany/wizink/adib/adcb/fernuni/univ-biskra/
    // has-sante/rki/bizkaia). Un finance-news NO se auto-marca BankOrCreditUnion; un edu-content NO se
    // auto-marca CollegeOrUniversity → FP casi nulo. El schema DÉBIL (hotel/ONG/inmobiliaria, que un
    // publisher SÍ puede embeber al RESEÑAR) + keywords quedan gateados por !hasDisplayAds (abajo).
    // Brand-safety: piratería fuera aunque tenga ads (no es tema de monetización).
    else if (piracyRe.test(html)) nonPublisherType = "piracy";
    // Maxi 2026-07-15: TODO el schema de entidad no-publisher lo VETA el ad-tech de PUBLISHER
    // (hasPublisherAds: AdSense/GPT/SSP/Taboola). Un banco/servicio/institución/SaaS NO corre publisher-ads;
    // un medio que SÍ vende inventario (aunque tenga schema raro o tienda de merch) queda protegido.
    else if (!hasPublisherAds && bankSchema.test(html)) nonPublisherType = "bank";
    else if (!hasPublisherAds && eduSchema.test(html))  nonPublisherType = "education";
    else if (!hasPublisherAds && govSchema.test(html))  nonPublisherType = "government";
    else if (!hasPublisherAds && healthSchema.test(html)) nonPublisherType = "health";
    else if (!hasPublisherAds && serviceSchema.test(html)) nonPublisherType = "service";
    else if (!hasPublisherAds && instSchema.test(html)) nonPublisherType = "institution";
    else if (!hasPublisherAds && saasSchema.test(html)) nonPublisherType = "saas";
    else if (!hasPublisherAds && corpSchema.test(html)) nonPublisherType = "corporate";
    else if (!hasPublisherAds && personalSchema.test(html)) nonPublisherType = "personal";
    // TODO lo demás (schema Y keywords) SOLO cuenta si el sitio NO muestra ads display (programmatic
    // O red partner de ADEQ: Taboola/MGID/Ezoic/Seedtag/Teads...). Un publisher que monetiza con ads
    // —aunque embeba schema de un hotel/producto que RESEÑA, o mencione vocabulario financiero— NUNCA
    // se rechaza (regla de oro: no perder un publisher). Un banco/hotel/universidad/inmobiliaria/
    // tienda-sin-plataforma en su PROPIO sitio no vende inventario → no corre display → se caza.
    // Esto además arregla: nonprofit-journalism con ads (pasa) vs charity sin ads (se caza).
    else if (!hasDisplayAds) {
      if (travelSchema.test(html)) nonPublisherType = "travel";
      else if (npoSchema.test(html)) nonPublisherType = "nonprofit";
      else if (realtySchema.test(html)) nonPublisherType = "realestate";
      else if (_hits(bankKw) >= 2) nonPublisherType = "bank";
      else if (_hits(travelKw) >= 2) nonPublisherType = "travel";
      else if (_hits(realtyKw) >= 2) nonPublisherType = "realestate";
      else if (_hits(eduKw) >= 2) nonPublisherType = "education";
      else if (_hits(svcKw) >= 2) nonPublisherType = "service";
      else if (_hits(npoKw) >= 2) nonPublisherType = "nonprofit";
      else if (_hits(corpKw) >= 2) nonPublisherType = "corporate";
      else if (_hits(svcPlatKw) >= 2) nonPublisherType = "service";
      else if (_hits(datingKw) >= 2) nonPublisherType = "service";
      else if (_hits(cryptoKw) >= 2) nonPublisherType = "crypto";
      else if (_hits(devToolKw) >= 2) nonPublisherType = "devtool";
      else if (_hits(appKw) >= 2) nonPublisherType = "app";
      else if (_hits(shopKw) >= 2) nonPublisherType = "ecommerce";
      else if (_hits(personalKw) >= 1) nonPublisherType = "personal";
    }

    return {
      title: title.slice(0, 100),
      description: desc.slice(0, 280),
      adNetworks, category,
      hasDisplayAds, hasProgrammatic,   // B2: señales de monetización real
      nonPublisherType,                 // tienda/banco/universidad/servicios → rechazar
      isEcommerce: nonPublisherType === "ecommerce",
      ...extractPhonesFromHtml(html),   // Maxi 2026-07-16: phones[] + whatsapps[] del home (footer, wa.me, tel:)
      htmlLang, ogLocale, hreflang, jsonLdLang, pathLang,
      textSample,
    };
  } catch (e) {
    // Maxi 2026-07-08: distinguir DOMINIO MUERTO (DNS no resuelve) de un bloqueo transitorio.
    // ENOTFOUND/EAI_AGAIN = el dominio no existe/no resuelve → dead:true para skipear downstream.
    // Timeout/403/reset = puede estar VIVO pero bloqueando → null (no lo matamos).
    const code = String(e?.cause?.code || e?.code || e?.message || "");
    // Maxi 2026-07-16: DOMINIO MUERTO/INSERVIBLE = DNS no resuelve, conexión rechazada, o error DURO de
    // TLS/SSL/certificado. El user pasó ejemplos: zd.blog.jp (privacy error), gamepress.gg (can't be
    // reached), eiga.com (ERR_SSL_VERSION_OR_CIPHER_MISMATCH). Esos NO se pueden servir → dead:true → se
    // descartan downstream. Timeout/reset/403 se dejan en null (puede estar VIVO bloqueando un bot → no lo matamos).
    if (/ENOTFOUND|EAI_AGAIN|ERR_NAME_NOT_RESOLVED|getaddrinfo|ECONNREFUSED|CERT|SSL|TLS|EPROTO|SELF_SIGNED|UNABLE_TO_VERIFY|HANDSHAKE|WRONG_VERSION|ALTNAME|unsupported protocol/i.test(code)) return { dead: true, deadReason: code.slice(0, 60) };
    return null;
  }
}

// ════════════════════════════════════════════════════════════════
// FILTRO DE BASURA (Maxi 2026-06-19) — "¿es un publisher monetizable o es
// basura (gobierno/universidad/empresa/SaaS/e-commerce/marca)?". Combina señales
// que YA tenemos (no agrega costo de tráfico): publicidad en la página, ads.txt,
// categoría, y un clasificador Haiku barato SOLO para los casos dudosos.
// ads.txt NO es obligatorio (muchos publishers buenos no lo tienen) — es UNA
// señal más. La señal fuerte es la publicidad real en el HTML.
// ════════════════════════════════════════════════════════════════
const PUBLISHER_CATEGORIES = new Set(["news","sports","entertainment","finance","technology","health","travel","food","automotive","gambling","streaming","business"]);
// Títulos típicos de NO-publisher (empresa/SaaS/login/checkout).
// Maxi 2026-07-09: SE SACARON palabras editoriales comunes que rechazaban diarios reales de la
// lista del user (verificado por revisión adversarial): "precios"/"pricing" (mercados/dólar),
// "gobierno"/"government"/"ministerio"/"municipal"/"ayuntamiento" (política), "universidad"/
// "university"/"facultad" (educación/estudiantil). Ámbito y El Cronista se rechazaban por "Gobierno"
// y "precios" en el <title>. Gov/uni reales igual los cazan isCategoryBlockedWorker + Haiku.
// Quedan SOLO señales inequívocas de app/checkout/panel (un medio jamás las pone en su título).
const NON_PUBLISHER_TITLE_RE = /\b(log ?in|sign ?in|sign ?up|my account|mi cuenta|request a demo|book a demo|get a demo|free trial|prueba gratis|checkout|add to cart|shopping cart|dashboard|control panel|panel de control|webmail|cpanel|plesk|online banking|banca en l[ií]nea)\b/i;

// Chequeo ligero de ads.txt — true si existe y tiene ≥1 línea de seller real.
const _adsTxtCache = new Map();
async function _hasRealAdsTxt(domain) {
  if (_adsTxtCache.has(domain)) return _adsTxtCache.get(domain);
  let ok = false;
  try {
    const res = await fetch(`https://${domain}/ads.txt`, { signal: AbortSignal.timeout(7000), redirect: "follow" });
    if (res.ok) {
      const ct = (res.headers.get("content-type") || "").toLowerCase();
      const txt = (await res.text()).slice(0, 20000);
      // Debe ser texto (no HTML de 404) y tener líneas "dominio, pub-id, DIRECT/RESELLER".
      const looksHtml = /<html|<!doctype/i.test(txt);
      const sellerLines = (txt.match(/^[^\s,#][^,\n]*,[^,\n]+,\s*(DIRECT|RESELLER)/gim) || []).length;
      ok = !looksHtml && !ct.includes("text/html") && sellerLines >= 1;
    }
  } catch {}
  if (_adsTxtCache.size > 2000) _adsTxtCache.clear();
  _adsTxtCache.set(domain, ok);
  return ok;
}

// Clasificador Haiku (barato) SOLO para dudosos. Cache por dominio.
const _publisherClassCache = new Map();
async function _haikuPublisherClass(token, domain, pageContent, swCategory, trashRules = "") {
  // Cache key incluye si hay reglas (para no servir veredictos viejos cuando el MB
  // enseña reglas nuevas vía rechazos).
  const ckey = `${domain}|${trashRules ? "r1" : "r0"}`;
  if (_publisherClassCache.has(ckey)) return _publisherClassCache.get(ckey);
  const title = pageContent?.title || ""; const desc = pageContent?.description || "";
  if (!title && !desc) return null; // sin nada que clasificar
  // Maxi 2026-07-08: taxonomía DESTILADA de los rechazos manuales del MB (los ejemplos que fue
  // pasando: leroymerlin/defacto/beyoung=tienda, n26/bbva=banco, ipsos=market research,
  // carwow/holidayautos=alquiler, andalusiaegypt/urlaubsguru=hotel/viajes, tommys.org=ONG,
  // ouedkniss=marketplace, universidades). Un publisher SOLO es medio de CONTENIDO monetizado
  // con display/programmatic. Todo lo que VENDE/RESERVA/GESTIONA no va.
  const sys = "Clasificás sitios web para un ad-network. Un 'publisher' (ÚNICO target válido) es un "
    + "medio/blog/revista/portal de CONTENIDO editorial que vive de publicidad display/programmatic: "
    + "noticias, deportes, entretenimiento, farándula, estilo de vida, tecnología, gaming, recetas, etc. "
    + "El sitio muestra ARTÍCULOS/NOTAS y coloca avisos alrededor.\n\n"
    + "NO son publishers (descartar SIEMPRE, aunque tengan pixel de retargeting o ads.txt):\n"
    + "- ecommerce: tiendas online, marcas que venden producto, marketplaces, clasificados (carrito/precio/comprar).\n"
    + "- bank: bancos, fintech, financieras, seguros, home-banking, tarjetas, préstamos.\n"
    + "- edu: universidades, colegios, institutos, plataformas de cursos (admisiones/carreras/matrícula).\n"
    + "- travel: hoteles, agencias de viaje, reservas, alquiler de autos, aerolíneas, turismo.\n"
    + "- nonprofit: ONGs, fundaciones, sitios .org benéficos (donaciones/causas/voluntariado).\n"
    + "- saas: software, apps, herramientas, plataformas B2B con demo/pricing/login.\n"
    + "- service: empresas de servicios, consultoras, market research, agencias, estudios profesionales "
    + "(presupuesto/demo/'nuestros servicios'/'contacta con un asesor'). Ipsos, por ejemplo, es service.\n"
    + "- marketplace: portales de LISTADOS/clasificados donde terceros publican para vender/alquilar. "
    + "Inmobiliarias (idealista, fincaraiz, ciencuadras), autos usados, clasificados (encuentra24, "
    + "corotos), bolsas de empleo, comparadores de precio. Aunque tengan ads, su producto son los "
    + "LISTADOS, no contenido editorial → NO es publisher.\n"
    + "- corp: sitios corporativos de marca (institucional, 'quiénes somos', sin contenido editorial).\n"
    + "- gov: gobiernos, entes públicos, municipios.\n\n"
    + "Regla: si el objetivo CLARO del sitio es VENDER, RESERVAR, GESTIONAR CUENTAS, CAPTAR CLIENTES o "
    + "RECAUDAR — NO es publisher. Solo si su producto ES el contenido y monetiza con ads → publisher. "
    + "IMPORTANTE: si NO estás razonablemente seguro de que sea comercial, respondé 'publisher' "
    + "(preferimos dejar pasar y que un humano lo descarte; perder un publisher real es lo peor). "
    + "Solo respondé un tipo comercial cuando sea EVIDENTE."
    + (trashRules ? `\n\nEl media buyer YA rechazó sitios por estos motivos adicionales — si encaja, NO es publisher:\n${trashRules}` : "")
    + "\n\nRespondé SOLO una palabra: publisher | corp | gov | edu | saas | ecommerce | bank | travel | nonprofit | service | marketplace | other.";
  let type = null;
  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/api-proxy`, {
      method: "POST",
      headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        provider: "anthropic", path: "/v1/messages", method: "POST",
        body: {
          model: "claude-haiku-4-5", max_tokens: 20,
          system: sys,
          messages: [{ role: "user", content: `Domain: ${domain}\nTitle: ${title}\nDescription: ${desc}\nSimilarWeb category: ${swCategory || "?"}` }],
        },
      }),
      signal: AbortSignal.timeout(12000),
    });
    if (res.ok) {
      const data = await res.json();
      const t = (data?.content?.[0]?.text || "").trim().toLowerCase().match(/[a-z]+/)?.[0] || "";
      if (["publisher","corp","gov","edu","saas","ecommerce","bank","travel","nonprofit","service","marketplace","realestate","other"].includes(t)) type = t;
    }
  } catch {}
  if (_publisherClassCache.size > 3000) _publisherClassCache.clear();
  _publisherClassCache.set(ckey, type);
  return type;
}

// Carga el contexto de basura aprendido de los rechazos del MB (por CONTENIDO,
// ignora GEO): (a) categorías net-negativas (dislikes ≥5 y > 2× likes), (b) las
// reglas sintetizadas por Haiku desde los motivos de rechazo. Cache 30min.
let _trashCtxCache = { at: 0, ctx: null };
async function _loadProspectTrashContext(token) {
  if (_trashCtxCache.ctx && (Date.now() - _trashCtxCache.at) < 1800_000) return _trashCtxCache.ctx;
  const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  const dislikedCats = new Set();
  let rules = "";
  try {
    const since = new Date(Date.now() - 90 * 86400_000).toISOString();
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_autopilot_feedback?created_at=gte.${since}&select=category,action&limit=5000`, { headers: auth });
    if (r.ok) {
      const tally = new Map();
      for (const x of (await r.json() || [])) {
        const c = (x.category || "").toLowerCase().trim();
        if (!c) continue;
        const t = tally.get(c) || { dis: 0, lik: 0 };
        if (x.action === "disliked") t.dis++; else if (x.action === "liked") t.lik++;
        tally.set(c, t);
      }
      for (const [c, t] of tally) if (t.dis >= 5 && t.dis > t.lik * 2) dislikedCats.add(c);
    }
  } catch {}
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.prospect_trash_rules&select=value`, { headers: auth });
    if (r.ok) { const rows = await r.json(); rules = String(rows?.[0]?.value || "").slice(0, 1500); }
  } catch {}
  const ctx = { dislikedCats, rules };
  _trashCtxCache = { at: Date.now(), ctx };
  return ctx;
}

// Maxi 2026-07-01: ANÁLISIS 3×/semana (L/X/V) de los prospects ACTUALES contra el
// aprendizaje de rechazos (por CONTENIDO/TIPO, ignora geo). Marca suspect_reject=true en
// las que Haiku considera del mismo tipo que las rechazadas → la toolbar enciende una ⚠️
// al lado de la X para que el MB revise si conviene descartarlas. Bounded: 200/run.
let _lastSuspectAnalysisDate = "";
async function runSuspectRejectAnalysis(token) {
  const { weekday, dateISO, hour } = _madridNowParts();
  if (!["Mon", "Wed", "Fri"].includes(weekday)) return;  // 3×/semana
  if (hour < 10) return;                                   // desde las 10 Madrid
  if (_lastSuspectAnalysisDate === dateISO) return;
  try { const cfg = await getConfig(token); if (cfg.last_suspect_analysis_date === dateISO) { _lastSuspectAnalysisDate = dateISO; return; } } catch {}
  _lastSuspectAnalysisDate = dateISO;
  const trash = await _loadProspectTrashContext(token);
  // Maxi 2026-07-01: el aprendizaje de rechazos es por CONTENIDO/TIPO del sitio (reglas destiladas
  // de los comentarios del MB), NUNCA por categoría/temática ni geo (regla dura del user). Si no
  // hay `rules` aún, no hay nada que aprender → skip. dislikedCats YA no gatilla el análisis.
  if (!trash.rules) { log("🔎 suspect-analysis: sin reglas de rechazo por contenido aún — skip"); return; }
  const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  let rows = [];
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&suspect_reject=eq.false&select=id,domain,page_title,category&order=created_at.desc&limit=200`, { headers: auth });
    if (r.ok) rows = await r.json();
  } catch {}
  if (!Array.isArray(rows) || rows.length === 0) return;
  log(`🔎 suspect-analysis (${dateISO}): analizando ${rows.length} prospects contra reglas de rechazo`);
  let flagged = 0;
  const CONC = 4;
  for (let i = 0; i < rows.length; i += CONC) {
    await Promise.all(rows.slice(i, i + CONC).map(async (row) => {
      // Maxi 2026-07-01: SOLO por TIPO/contenido (Haiku + reglas destiladas de comentarios de
      // rechazo). Se sacó el flag por `dislikedCats` (temática) — el user prohíbe descartar por
      // categoría o geo. La ⚠️ aprende del CONTENIDO del sitio, no de su país ni su temática.
      let reason = "";
      const type = await _haikuPublisherClass(token, row.domain, { title: row.page_title || "", description: "" }, row.category || "", trash.rules || "");
      if (type && type !== "publisher") reason = `tipo detectado: ${type}`;
      if (!reason) return;
      try {
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${row.id}`, {
          method: "PATCH",
          headers: { ...auth, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ suspect_reject: true, suspect_reason: reason.slice(0, 200) }),
        });
        flagged++;
      } catch {}
    }));
  }
  await setConfigValue(token, "last_suspect_analysis_date", dateISO).catch(() => {});
  log(`🔎 suspect-analysis: ${flagged}/${rows.length} marcadas como sospechosas (⚠️)`);
}

// ════════════════════════════════════════════════════════════════
// BARRIDO DE PROSPECTS (Maxi 2026-07-13) — el pool VIEJO (pending) entró antes de que
// endureciera los filtros y NO se re-evalúa solo → cientos de no-publishers conocidos
// (pinterest/esselunga/bancos/retailers) siguen ahí. Este job los PURGA de a batches usando
// SOLO señales de ALTA PRECISIÓN (0 falsos positivos): (a) blocklist curada (isDomainBlockedFull),
// (b) detector estructural (nonPublisherType: ecommerce/bank/edu/gov/travel/realestate). NUNCA
// borra por Haiku ni por categoría/tema (eso quema publishers). Un publisher real jamás matchea
// ninguna de las 2 señales → regla de oro intacta. Gated por config purge_blocked_prospects='true';
// avanza por cursor de created_at y se auto-apaga al terminar el pool.
async function sweepBlockedFromProspects(token) {
  const cfg = await getConfig(token);
  if (String(cfg.purge_blocked_prospects || "") !== "true") return;
  const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  const BATCH = 60;
  const cursor = cfg.purge_cursor_ts || "";
  const cursorClause = cursor ? `&created_at=lt.${encodeURIComponent(cursor)}` : "";
  let rows = [];
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending${cursorClause}&select=id,domain,created_at&order=created_at.desc&limit=${BATCH}`, { headers: auth });
    if (r.ok) rows = await r.json();
  } catch {}
  if (!Array.isArray(rows) || rows.length === 0) {
    await setConfigValue(token, "purge_blocked_prospects", "false").catch(() => {});
    await setConfigValue(token, "purge_cursor_ts", "").catch(() => {});
    log(`🧹 purge-prospects: pool barrido COMPLETO → flag OFF`);
    return;
  }
  const toDelete = [];
  const remaining = [];
  // Fase 1: blocklist curada (sin red, instantáneo)
  for (const row of rows) {
    const blocked = await isDomainBlockedFull(row.domain, token);
    if (blocked) toDelete.push({ id: row.id, domain: row.domain, reason: `blocklist:${blocked}` });
    else remaining.push(row);
  }
  // Fase 2: detector estructural (con red, alta precisión, CONC 5)
  const CONC = 5;
  for (let i = 0; i < remaining.length; i += CONC) {
    await Promise.all(remaining.slice(i, i + CONC).map(async (row) => {
      const pc = await fetchPageContent(row.domain).catch(() => null);
      if (pc?.dead) toDelete.push({ id: row.id, domain: row.domain, reason: `unreachable` });  // Maxi 2026-07-16: muerto/SSL/cert
      else if (pc?.nonPublisherType) toDelete.push({ id: row.id, domain: row.domain, reason: `nonpub_${pc.nonPublisherType}` });
    }));
  }
  // NO hard-DELETE: pasamos a status='rejected' + motivo 'purge:...'. Desaparecen de Prospects
  // (el agente solo lee status=pending) pero queda AUDITORÍA — se puede revisar/restaurar si algún
  // publisher se coló (regla de oro). Query de auditoría: status='rejected' AND suspect_reason LIKE 'purge:%'.
  for (const d of toDelete) {
    try {
      await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${d.id}`, {
        method: "PATCH",
        headers: { ...auth, "Content-Type": "application/json", "Prefer": "return=minimal" },
        body: JSON.stringify({ status: "rejected", suspect_reject: true, suspect_reason: `purge: ${d.reason}`.slice(0, 200) }),
      });
      log(`  🗑️ purge ${d.domain} — ${d.reason}`);
    } catch {}
  }
  const lastTs = rows[rows.length - 1].created_at;
  await setConfigValue(token, "purge_cursor_ts", lastTs).catch(() => {});
  log(`🧹 purge-prospects: batch=${rows.length} borrados=${toDelete.length} (blocklist=${rows.length - remaining.length}, estructural=${toDelete.length - (rows.length - remaining.length)}) cursor→${lastTs}`);
}

// ════════════════════════════════════════════════════════════════
// PULIDO DEL POOL (Maxi 2026-07-15) — UN SOLO job que reemplaza a purge + reenrich. Recorre todo el
// pool pending y con UN fetch por dominio: (1) BLOQUEA (soft-reject) los no-publishers (blocklist +
// detector estructural con veto publisher-ads) y (2) para los que QUEDAN sin email bueno, busca email
// (scrape gratis + website-informer + Apollo capado opcional). NO usa RapidAPI. Gated por config
// polish_pool='true'; cursor polish_cursor_ts; se auto-apaga al terminar. Apollo off con polish_use_apollo='false'.
async function _softRejectLead(auth, id, reason) {
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${id}`, {
      method: "PATCH", headers: { ...auth, "Content-Type": "application/json", "Prefer": "return=minimal" },
      body: JSON.stringify({ status: "rejected", suspect_reject: true, suspect_reason: `purge: ${reason}`.slice(0, 200) }),
    });
  } catch {}
}
let _lastPolishRunAt = 0;
// Maxi 2026-07-16: control de gasto del fallback de contacto por Google (Serper). Cap DIARIO + dedup
// para no quemar créditos (el user vio ~1500/día → a ese ritmo 50k no duran). Reset al cambiar el día Madrid.
let _serperContactCount = 0, _serperContactDay = "", _serperContactTried = new Set();
// Maxi 2026-07-15: ritmo subido para pulir el pool grande (1133 pendientes) en horas, no días.
// Seguro ahora que (a) el cursor commitea por wave (sobrevive restarts) y (b) se arregló el OOM.
// Es red-bound (fetch+scrape), no memoria → más concurrencia impacta poco en RSS.
// Maxi 2026-07-16: RITMO MÁXIMO para el re-barrido (el user pidió re-analizar todo lo más rápido).
// Seguro por: (a) guard de memoria (limpia caches si rss sube), (b) commit incremental (si lo reinician
// no pierde progreso). El fallback Google (Serper) agrega ~10s por dominio sin email → más concurrencia
// compensa. Tradeoff: el agente se pacea un poco (polishPool corre antes en el loop) pero sigue enviando.
const POLISH_COOLDOWN_MS = 8 * 1000;   // 20→8s: corre casi cada loop
const POLISH_BATCH = 120;              // 60→120: más dominios por corrida (commit incremental por wave lo hace seguro)
const POLISH_CONC = 12;                // 8→12: más dominios en paralelo por wave
async function polishPool(token) {
  const cfg = await getConfig(token);
  if (String(cfg.polish_pool || "") !== "true") return;
  if (Date.now() - _lastPolishRunAt < POLISH_COOLDOWN_MS) return;
  _lastPolishRunAt = Date.now();
  const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  // Apollo capado, opcional (off si polish_use_apollo='false'). NUNCA RapidAPI.
  const useApollo = String(cfg.polish_use_apollo ?? "true") !== "false";
  const apollo_api_key = cfg.apollo_api_key;
  let apolloAvailable = useApollo && !!apollo_api_key;
  if (apolloAvailable) {
    const usage = await getApolloUsageToday(token);
    if (usage.usedToday >= usage.limit || (usage.usedThisMonth ?? 0) >= APOLLO_MONTHLY_HARD_CAP) apolloAvailable = false;
  }
  const cursor = cfg.polish_cursor_ts || "";
  const cursorClause = cursor ? `&created_at=gt.${encodeURIComponent(cursor)}` : "";
  let leads = [];
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending${cursorClause}&select=id,domain,emails,email_sources,contact_name,contact_phone,category,traffic,created_at&order=created_at.asc&limit=${POLISH_BATCH}`, { headers: auth });
    if (r.ok) leads = await r.json();
  } catch {}
  if (!Array.isArray(leads) || leads.length === 0) {
    await setConfigValue(token, "polish_pool", "false").catch(() => {});
    await setConfigValue(token, "polish_cursor_ts", "").catch(() => {});
    log(`✨ polish: pool COMPLETO pulido → flag OFF`);
    return;
  }
  let blocked = 0, enriched = 0;
  let _committedTs = cursor;
  for (let i = 0; i < leads.length; i += POLISH_CONC) {
    try { await setConfigValue(token, "auto_heartbeat_at", new Date().toISOString()); } catch {}  // Maxi 2026-07-15 (F3): heartbeat por ronda (job largo)
    const _wave = leads.slice(i, i + POLISH_CONC);
    await Promise.all(_wave.map(async (lead) => {
      const domain = lead.domain;
      try {
        // 1) blocklist (sin red)
        const bl = await isDomainBlockedFull(domain, token);
        if (bl) { await _softRejectLead(auth, lead.id, `blocklist:${bl}`); blocked++; return; }
        // 2) clasificar por HTML — UN fetch (detector estructural con veto publisher-ads)
        const pc = await fetchPageContent(domain).catch(() => null);
        if (pc?.dead) { await _softRejectLead(auth, lead.id, `unreachable:${pc.deadReason || "dead"}`); blocked++; return; }  // Maxi 2026-07-16: sitio muerto/SSL/cert → fuera
        if (pc?.nonPublisherType) { await _softRejectLead(auth, lead.id, `nonpub_${pc.nonPublisherType}`); blocked++; return; }
        // 3) keeper: si ya tiene email bueno, listo
        const curEmails = Array.isArray(lead.emails) ? lead.emails.filter(Boolean) : [];
        const srcMap = lead.email_sources || {};
        const hasGood = curEmails.some(e => {
          const raw = srcMap[e.toLowerCase()];
          const src = (typeof raw === "string" ? raw : (raw?.source || "")).toLowerCase();
          if (src === "apollo" || src === "informer") return true;
          return !_isGenericLocalPart(e);
        });
        // Maxi 2026-07-16: teléfono/WhatsApp del home (ya fetcheamos pc → gratis). "wa:" marca WhatsApp.
        let foundPhone = "";
        if (pc?.whatsapps?.length) foundPhone = "wa:" + pc.whatsapps[0];
        else if (pc?.phones?.length) foundPhone = pc.phones[0];
        const _curPhone = String(lead.contact_phone || "").trim();
        const _savePhone = async () => {
          if (foundPhone && !_curPhone) {
            await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
              method: "PATCH", headers: { ...auth, "Content-Type": "application/json", "Prefer": "return=minimal" },
              body: JSON.stringify({ contact_phone: foundPhone }),
            }).catch(() => {});
          }
        };
        // keeper: ya tiene email bueno → solo guardamos el tel si es nuevo, y listo.
        if (hasGood) { await _savePhone(); return; }
        // 4) buscar email: scrape gratis (páginas internas multilingües) + REDES SOCIALES (FB/IG/Twitter)
        //    + website-informer/WHOIS — todo adentro de scrapeEmailsForDomain — y Apollo capado si vacío.
        let foundEmail = null, foundSource = null, foundName = "";
        const _informerOut = new Set(), _socialOut = new Map();
        const scraped = await scrapeEmailsForDomain(domain, { informerOut: _informerOut, socialOut: _socialOut }).catch(() => []);
        if (Array.isArray(scraped) && scraped.length) {
          const ranked = scraped.map(e => ({ email: e, score: rankEmail(e, domain, lead.category) })).filter(r => r.score > 0).sort((a, b) => b.score - a.score);
          if (ranked.length) {
            foundEmail = ranked[0].email;
            const _le = foundEmail.toLowerCase();
            foundSource = _socialOut.has(_le) ? "social" : (_informerOut.has(_le) ? "informer" : "scrape");
          }
        }
        if (!foundEmail && apolloAvailable) {
          const ap = await findBestApolloEmail(domain, apollo_api_key, token, { traffic: lead.traffic || 0, allowUnlock: true, forceUnlock: true }).catch(() => null);
          if (ap?.email) { foundEmail = ap.email; foundSource = "apollo"; foundName = ap.contact_name || ""; }
        }
        // 5) Maxi 2026-07-16 FALLBACK Google (Serper) OPTIMIZADO: gasta 1 crédito SOLO si el dominio quedó
        //    con CERO email (NO en los que ya tienen uno genérico → esos ya son contactables). Con CAP
        //    DIARIO (serper_contact_daily_cap, default 250) + dedup (no re-buscar el mismo dominio en el
        //    día) + metrado en config (serper_contact_used) → el user vio ~1500/día sin control, ahora
        //    ~250/día = junto con AutoGoogle (~150) tus 50k duran ~4 meses. Es un consumidor SEPARADO de
        //    AutoGoogle (que sigue buscando DOMINIOS nuevos para el cascade).
        if (!foundEmail && curEmails.length === 0) {
          const _mDay = _madridNowParts().dateISO;
          if (_serperContactDay !== _mDay) { _serperContactDay = _mDay; _serperContactCount = 0; _serperContactTried.clear(); }
          const _ccap = parseInt(cfg.serper_contact_daily_cap || "250", 10) || 250;
          if (_serperContactCount < _ccap && !_serperContactTried.has(domain)) {
            _serperContactTried.add(domain);
            _serperContactCount++;
            if (_serperContactCount % 10 === 0) setConfigValue(token, "serper_contact_used", `${_mDay}:${_serperContactCount}`).catch(() => {});
            const g = await _serperContactSearch(domain).catch(() => null);
            if (g) {
              if (g.emails.length) {
                const gr = g.emails.map(e => ({ email: e, score: rankEmail(e, domain, lead.category) })).filter(r => r.score > 0).sort((a, b) => b.score - a.score);
                if (gr.length) { foundEmail = gr[0].email; foundSource = "google_contact"; }
              }
              if (!foundPhone) {
                if (g.whatsapps.length) foundPhone = "wa:" + g.whatsapps[0];
                else if (g.phones.length) foundPhone = g.phones[0];
              }
            }
          }
        }
        // Guardar email (si hay) y/o teléfono (si es nuevo) en UN solo PATCH.
        if (foundEmail || (foundPhone && !_curPhone)) {
          const _patch = {};
          if (foundEmail) {
            const merged = [foundEmail, ...curEmails.filter(e => e.toLowerCase() !== foundEmail.toLowerCase())];
            _patch.emails = await validateEmailsBatch(merged);
            const newSources = { ...(lead.email_sources || {}) };
            newSources[foundEmail.toLowerCase()] = foundSource;
            _patch.email_sources = newSources;
            _patch.contact_name = foundName || lead.contact_name || "";
          }
          if (foundPhone && !_curPhone) _patch.contact_phone = foundPhone;
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "PATCH", headers: { ...auth, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify(_patch),
          });
          if (foundEmail) enriched++;
        }
      } catch (e) { log(`  ⚠️ polish ${domain}: ${e.message}`); }
    }));
    // Maxi 2026-07-15: commitear el cursor DESPUÉS DE CADA WAVE (antes solo al final del batch). Si el
    // worker se reinicia a mitad (OOM/redeploy), el progreso queda guardado y el próximo run resume desde
    // acá en vez de re-empezar el MISMO batch → garantiza avance aunque haya restarts. (leads viene
    // ordenado created_at asc → el último de cada wave es el created_at máximo procesado hasta ahora.)
    _committedTs = _wave[_wave.length - 1].created_at;
    await setConfigValue(token, "polish_cursor_ts", _committedTs).catch(() => {});
  }
  log(`✨ polish: batch=${leads.length} bloqueados=${blocked} enriquecidos=${enriched} cursor→${_committedTs}`);
}

// Maxi 2026-07-16: EXPANSIÓN POR SIMILARES desde los Prospects. El user pidió "que busque similares de
// los que están en Prospects" como otra fuente de descubrimiento (además de likes+validados que ya seedea
// el autopilot). Toma los Prospects pending de MÁS tráfico (≥500K = publishers grandes ya calificados →
// sus similares suelen ser también buenos), trae sus similar-sites (SimilarWeb, con cache 90d = muchos sin
// costo), dedup + pre-filtro por nombre, e inyecta los frescos a la cola → pasan la MISMA calificación
// (350K pageviews + detector publisher). Gated por similar_expansion_enabled='true'. Cursor por created_at.
// RapidAPI-gated (no gasta si estamos cerca del cap mensual) + cooldown + lote chico (control de costo).
let _lastSimilarExpAt = 0;
const SIMILAR_EXP_COOLDOWN_MS = 5 * 60 * 1000;
const SIMILAR_EXP_BATCH = 6;
async function runProspectSimilarExpansion(token) {
  const cfg = await getConfig(token);
  if (String(cfg.similar_expansion_enabled || "") !== "true") return;
  if (Date.now() - _lastSimilarExpAt < SIMILAR_EXP_COOLDOWN_MS) return;
  _lastSimilarExpAt = Date.now();
  const rapidapi_key = cfg.rapidapi_key;
  if (!rapidapi_key) return;
  // Gate RapidAPI: SimilarWeb cuesta hits → no expandir si estamos cerca del cap mensual.
  try {
    const { usedThisMonth, limit } = await getRapidApiUsageThisMonth(token);
    if (usedThisMonth >= limit * FEEDER_RAPIDAPI_THRESHOLD) { log(`🛑 similar-exp SKIP: RapidAPI ${usedThisMonth}/${limit} (≥${FEEDER_RAPIDAPI_THRESHOLD * 100}%)`); return; }
  } catch {}
  const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  const cursor = cfg.similar_expansion_cursor || "";
  const cursorClause = cursor ? `&created_at=gt.${encodeURIComponent(cursor)}` : "";
  let seeds = [];
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.500000${cursorClause}&select=domain,created_at&order=created_at.asc&limit=${SIMILAR_EXP_BATCH}`, { headers: auth });
    if (r.ok) seeds = await r.json();
  } catch {}
  if (!Array.isArray(seeds) || seeds.length === 0) {
    await setConfigValue(token, "similar_expansion_cursor", "").catch(() => {});  // fin → reset (los Prospects cambian)
    return;
  }
  const _lastTs = seeds[seeds.length - 1].created_at;
  const sessionKnown = new Set();
  let injected = 0;
  for (const s of seeds) {
    try {
      const sims = await findSimilarSites(s.domain, rapidapi_key).catch(() => []);
      if (!Array.isArray(sims) || sims.length === 0) continue;
      const cands = sims.filter(d => d && !DEPRIO_TLD_RE.test(d) && !_MAJESTIC_NAME_SKIP_RE.test(d) && !isCorporatePattern(d) && !BRAND_BLOCKLIST.has(d));
      const known = await _findKnownDomainsWorker(token, cands);
      const fresh = cands.filter(d => !known.has(d) && !sessionKnown.has(d));
      fresh.forEach(d => sessionKnown.add(d));
      if (fresh.length) injected += await _injectIntoCsvQueue(token, fresh, "auto_feeder_majestic");
    } catch (e) { log(`  ⚠️ similar-exp ${s.domain}: ${e.message}`); }
    try { await setConfigValue(token, "auto_heartbeat_at", new Date().toISOString()); } catch {}
  }
  await setConfigValue(token, "similar_expansion_cursor", _lastTs).catch(() => {});
  log(`🔗 similar-exp: semillas=${seeds.length} → ${injected} similares frescos inyectados (cursor→${_lastTs})`);
}

// Gate principal. Devuelve { ok, reason }. ok=false → no va a Prospects.
async function classifyPublisher(token, domain, pageContent, swCategory) {
  // Si no pudimos bajar la home, NO filtrar acá (podría ser publisher con el sitio
  // caído un momento). El guard de "no_emails_and_site_unreachable" downstream
  // maneja los dominios realmente muertos. Evita falsos rechazos.
  // Maxi 2026-07-16: sitio MUERTO/inservible (DNS no resuelve, conexión rechazada, TLS/SSL/cert roto) →
  // NO pasa (antes con !pageContent devolvía ok:true = "beneficio de la duda" y estos se colaban a Prospects:
  // zd.blog.jp, gamepress.gg, eiga.com). fetchPageContent marca dead:true solo en fallas DURAS (no timeouts).
  if (pageContent?.dead) return { ok: false, reason: `unreachable:${pageContent.deadReason || "dead"}` };
  if (!pageContent) return { ok: true, reason: "no_pagecontent" };
  // Maxi 2026-07-08: TIPO DE SITIO no-publisher estructural (tienda/banco/universidad/viajes/
  // ONG/servicios) → NO es target. Rechazo TEMPRANO, ANTES de la señal positiva de ads, porque
  // estos sitios corren retargeting/ads.txt y se colaban (leroymerlin, defacto, n26, ipsos,
  // carwow, urlaubsguru, tommys, etc.). Detectado por schema.org @type + keywords de intención.
  if (pageContent.nonPublisherType) return { ok: false, reason: `nonpub_${pageContent.nonPublisherType}` };
  const title = (pageContent?.title || "");
  const cat   = (pageContent?.category || "");
  // Maxi 2026-06-19: filtro NO agresivo. Se SACÓ el aprendizaje por contenido
  // (dislikedCats / trash rules): hasta confirmar que todo está 100%, NO rechazamos
  // por "categoría que el MB no quiso". Solo descartamos no-publishers ESTRUCTURALES
  // (empresas/SaaS/gobierno/universidades/adultos/streaming). Ante la duda, PASA y que
  // el MB lo rechace a mano (el botón rojo). Lo estructural lo cubren también
  // isCategoryBlockedWorker (gov/uni/empresas) y scoreWebsite (adult/streaming/gambling).
  // 1. Negativo fuerte por título (empresa/SaaS/login/gov/uni) → estructural, gratis.
  if (NON_PUBLISHER_TITLE_RE.test(title)) return { ok: false, reason: `title_nonpub:"${title.slice(0,40)}"` };
  // 2. Categoría de medios FUERTE (heurística home / SimilarWeb) → publisher directo, sin gastar IA.
  if (PUBLISHER_CATEGORIES.has(cat)) return { ok: true, reason: `pub_cat:${cat}` };
  const swc = (swCategory || "").toLowerCase();
  if (/news|media|sport|entertain|magazine|gossip|lifestyle|gaming|music|tv|film|movie/.test(swc)) return { ok: true, reason: `sw_pub_cat:${swc.slice(0,20)}` };
  // 3. Maxi 2026-07-08: la señal de monetización (display ads / ads.txt) YA NO alcanza por sí
  //    sola — tiendas/bancos/hoteles/servicios corren retargeting y ads.txt y se colaban. Pedimos
  //    veredicto de la IA (Haiku) como VETO: si la IA lo tipifica como comercial (tienda/banco/
  //    edu/viajes/ong/saas/servicio/corp/gov) → rechazo, aunque tenga ads. El user: "si no
  //    analiza la IA se da cuenta que no va". Haiku recibe además las reglas aprendidas del MB.
  const monetized = !!pageContent?.hasDisplayAds || await _hasRealAdsTxt(domain);
  const trash = await _loadProspectTrashContext(token).catch(() => ({ rules: "" }));
  const type = await _haikuPublisherClass(token, domain, pageContent, swCategory, trash.rules || "");
  if (type && type !== "publisher" && type !== "other") return { ok: false, reason: `haiku_${type}` };
  if (type === "publisher") return { ok: true, reason: monetized ? "haiku+ads" : "haiku_publisher" };
  // 4. Haiku no respondió / "other". Si hay monetización real → publisher; si no, PASA (no agresivo)
  //    y que el MB lo rechace a mano.
  if (monetized) return { ok: true, reason: pageContent?.hasProgrammatic ? "programmatic_ads" : "ads_monetized" };
  return { ok: true, reason: "classifier_unavailable_pass" };
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
  // Maxi 2026-07-08: bonus de acento GATEADO — solo REFUERZA una señal de stopword ya
  // existente, NO crea un ganador de la nada. Antes: á/é/í/ó/ú/ü (compartidos por checo,
  // polaco, eslovaco, húngaro, turco, etc.) inflaban es/pt/it falsos y se volvían el voto
  // ganador en sitios de idiomas NO soportados. Chars REALMENTE distintivos de un idioma
  // (ñ/¿/¡ = español; ã/õ = portugués; ß = alemán) mantienen bonus fuerte SIEMPRE porque
  // casi no existen fuera de ese idioma; los compartidos solo suman si YA hay stopwords.
  if (/[ñ¿¡]/.test(text))                          scores.es = (scores.es || 0) + 8; // casi únicos del es
  else if (scores.es > 0 && /[áéíóúü]/.test(text)) scores.es += 8;                    // compartidos: refuerzo
  if (/[ãõ]/.test(text))                           scores.pt = (scores.pt || 0) + 8; // casi únicos del pt
  else if (scores.pt > 0 && /[çàáâ]/.test(text))   scores.pt += 8;                    // compartidos: refuerzo
  if (scores.it > 0 && /[àèéìòù]/.test(text))      scores.it += 5;                    // it no tiene chars únicos
  if (/ß/.test(text))                              scores.de = (scores.de || 0) + 5; // ß casi único del de
  else if (scores.de > 0 && /[äöü]/.test(text))    scores.de += 5;                    // compartidos: refuerzo
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

  // Maxi 2026-07-08: cierre unificado — aplica la REGLA DURA (lang SIEMPRE en
  // SUPPORTED_AGENT_LANGS, ante cualquier duda → en), cachea y devuelve. Evita duplicar
  // el cacheo en los múltiples early-returns (incl. el fallback de idioma no soportado).
  const finish = (result) => {
    if (!result.lang || !SUPPORTED_AGENT_LANGS.has(result.lang)) {
      result = { ...result, lang: "en", source: (result.source || "") + "→en_hardrule" };
    }
    if (cleanDomain && hasFullData) {
      if (_domainLangCache.size >= DOMAIN_LANG_CACHE_MAX) {
        const firstKey = _domainLangCache.keys().next().value;
        _domainLangCache.delete(firstKey);
      }
      _domainLangCache.set(cleanDomain, result);
    }
    return result;
  };

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
  // Maxi 2026-07-08: si el TEXTO detecta CLARAMENTE un idioma NO soportado (de/fr/otro)
  // con confianza media/alta, es señal de que el sitio NO está en uno de nuestros 5 idiomas
  // → EN directo. Antes: un textRes.lang no soportado lo rechazaba addVote() y el resultado
  // caía a señales débiles (geo/tld) o al voto de acento equivocado → es/pt/it falso. Regla
  // del dueño: todo lo que no sea {en,es,pt,ar,it} se manda en INGLÉS.
  if (textRes.lang && !SUPPORTED_AGENT_LANGS.has(textRes.lang) &&
      (textRes.confidence === "high" || textRes.confidence === "medium")) {
    return finish({ lang: "en", source: "unsupported_fallback", confidence: "high",
                    reasons: [`text_unsupported:${textRes.lang}(${textRes.confidence})`] });
  }
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
        // Maxi 2026-07-08: el árbitro devolvió null (Claude dijo un idioma NO soportado —
        // de/fr/other — o hubo error). En baja confianza NO caemos al winner dudoso (voto de
        // acento/geo equivocado): el fallback CORRECTO es EN. Antes esto mandaba pt/es/it falso.
        result = { lang: "en", source: "arbiter_null_fallback", confidence: "low",
                   reasons: [...reasons, "claude_null→en"] };
      }
    } else {
      result = { lang: winner[0], source: "voting", confidence, reasons };
    }
  }

  // Maxi 2026-07-08: cierre unificado (regla dura SUPPORTED-only + cache) vía finish().
  return finish(result);
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
  // Adtech / monetización / SSP / ad networks — Maxi 2026-07-08: NO son publishers,
  // son la infra/competencia de monetización. El user: "no prospectamos empresas de
  // monetización" (ej. applovin). Rechazar todos.
  "applovin.com","ironsource.com","is.com","unity.com","unity3d.com","digitalturbine.com",
  "moloco.com","liftoff.io","vungle.com","adcolony.com","chartboost.com","mopub.com",
  "inmobi.com","smaato.com","fyber.com","tapjoy.com","pangle.io","mintegral.com",
  "taboola.com","outbrain.com","criteo.com","revcontent.com","mgid.com","adnow.com",
  "pubmatic.com","magnite.com","rubiconproject.com","indexexchange.com","openx.com",
  "smartadserver.com","equativ.com","sovrn.com","adform.com","teads.com","unruly.com",
  "sharethrough.com","triplelift.com","gumgum.com","primis.tech","connatix.com",
  "aniview.com","vidoomy.com","coinis.com","verve.com","vervegroup.com","smartyads.com",
  "epom.com","adpushup.com","setupad.com","freestar.com","playwire.com","snigel.com",
  "venatus.com","vidazoo.com","ezoic.com","mediavine.com","adthrive.com","raptive.com",
  "monumetric.com","newor.media","nexx360.io","yieldbird.com","admanager.google.com",
  "pubgalaxy.com","adhese.com","kevel.com","adpone.com","onetag.com","33across.com",
  "adagio.io","truvid.com","aps.amazon.com","liveintent.com","lkqd.com","spotx.tv",
  "spotxchange.com","beachfront.com","telaria.com","springserve.com","publica.tv",
]);

// TLDs o subcadenas que SIEMPRE indican sitios no-prospectables.
// Política user 2026-05-26: rechazar agresivamente gov/edu/military/academic
// en TODOS sus variantes nacionales. Estos sitios jamás compran publicidad.
const BLOCKED_TLDS     = [
  ".edu", ".gov", ".mil", ".int",
  ".edu.", ".gov.", ".mil.", ".gob.", ".ac.", ".gouv.",
  ".ac.uk", ".ac.jp", ".ac.kr", ".ac.in", ".ac.za", ".ac.nz", ".ac.il", ".ac.id", ".ac.th",
];
const ADULT_TLDS       = [".xxx", ".adult", ".porn", ".sex"];
const ADULT_KEYWORDS   = ["porn", "xxx", "xvideos", "sexcam", "camgirl", "fuck", "nsfw", "hentai", "escort"];

// Patrones de nombre de dominio que indican corporativo / marca / institución.
// Política user 2026-05-26: agresivo, preferir false-positive a meter basura.
const CORPORATE_PATTERNS = [
  // Subdominios técnicos / corporativos
  /^(www\.)?(login|admin|portal|intranet|sso|hr|recruiting|careers|investor|investors|jobs|developer|developers|api|status|docs|support|help|cdn|static|cms|wiki|vendor|partner|partners|mail|webmail)\./,
  // Industrias reguladas / institucional
  /(insurance|seguro|salud|hospital|clinic[ao]|medical|farmacia|pharma|pharmaceutic|aseguradora|aseguranca|aseguros)\./i,
  /^(bank|banco|banque|banca)[a-z0-9-]*\./i,
  // Corporativos / holdings / grupos
  /\b(corp|inc|holding|holdings|group|grupo|industries|industria|industrias|company|cia|sa|srl|sas|gmbh|ltd|llc|plc|enterprises?|consult(ing|oria))\.(com|net|org|biz|info|co)/i,
  /-(corp|inc|holding|holdings|group|grupo|industries|company|enterprises?)\.(com|net|org|biz|info|co)/i,
  // Energy / telecom / autos (marcas)
  /^(autos?|motor|motors|automotive|automotriz|vehiculo|vehiculos|energi[ae]|energy|petro|petrol|oil|gas|electric|electrico|nuclear|utilit(y|ies)|telecom|telefon|movistar|claro|tigo|entel|att|verizon|vodafone|orange|deutsche|telekom)\./i,
  // Real estate / construction
  /\b(realtor|realestate|inmobil|imobil|construct|construccion|construcao|properties|propiedades|propiedad|propriedade)\./i,
  // Manufacturing / B2B
  /\b(manufactur|industria|industrial|fabrica|factory|plant|planta|wholesale|mayorista)\./i,
  // Airlines / hotels (no media)
  /^(airlines?|aerolinea|aeroporto|airport|airways|hotels?|hoteles)\./i,
  // Retail chains
  /^(walmart|carrefour|tesco|costco|aliexpress|temu|shein)\./i,
  // Gobierno por nombre (refuerzo)
  /^(www\.)?(gobierno|gobiern|government|ministerio|ministry|alcaldia|municipio|municipal|ayuntamiento|provincia|congreso|senado|parlamento|parliament)\./i,
  // Universidades por nombre
  /^(www\.)?(universidad|universit|university|college|colegio|escuela|school|instituto|facultad|faculty)\./i,
  // ONG / fundaciones
  /^(www\.)?(ong|ngo|fundacion|fundacao|foundation|charity|caridad)\./i,
];

// Categorías de SimilarWeb que indican NO-publisher (marca, institución, B2B).
// Se chequea DESPUÉS de obtener traffic data (no pre-API).
// Maxi 2026-06-19: lista RECORTADA — no agresiva. Solo no-publishers ESTRUCTURALES
// (gobierno, universidades, empresas/SaaS, fabricantes). Se SACARON categorías que
// SÍ pueden ser publishers de contenido: finanzas/inversión (sitios de noticias
// financieras), telecom, ecommerce/shopping (sitios de deals/reviews), viajes
// (blogs/guías), inmobiliaria (portales con contenido), pharma, logística, etc.
// Esas, si no sirven, las rechaza el MB a mano (botón rojo). Adult/streaming/gambling
// los corta scoreWebsite (gate duro). Objetivo: dejar pasar, no sobre-filtrar.
const BLOCKED_CATEGORY_KEYWORDS = [
  "government", "law and government", "public administration", "military",
  "universities", "higher education", "academic",
  "software > b2b", "saas", "enterprise software",
  "vehicles > manufacturer", "automotive > manufacturer",
  "consumer goods > manufacturer",
  // Maxi 2026-07-13 (auditoría 48h): categorías SimilarWeb que NUNCA son publisher de contenido —
  // se colaban a Prospects (subito/idealista/rabobank/hellowork/olx…). Substring match contra la cat.
  // ⚠️ NO incluir 'adult' ni 'price_comparison' ni 'marketing_and_advertising': SW mis-clasificó ahí
  // publishers REALES (fatherly=adult, tweakers=price_comparison, medios de marketing) → los dejamos a
  // Haiku/revisión manual para no perder publishers (regla de oro).
  "real_estate", "banking_credit_and_lending", "/insurance", "accounting_and_auditing",
  "jobs_and_employment", "classifieds", "marketplace",
  "e-commerce_and_shopping/e-commerce_and_shopping",
];

function isCorporatePattern(domain) {
  const d = String(domain || "").toLowerCase();
  for (const re of CORPORATE_PATTERNS) {
    if (re.test(d)) return true;
  }
  return false;
}

function isCategoryBlockedWorker(category) {
  if (!category) return null;
  const cat = String(category).toLowerCase();
  for (const kw of BLOCKED_CATEGORY_KEYWORDS) {
    if (cat.includes(kw)) return kw;
  }
  return null;
}

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

// Maxi 2026-07-08: marcas/plataformas por LABEL de 2do nivel — matchea en CUALQUIER TLD o
// subdominio (news.google.at → "google", rakuten.tv → "rakuten", fr.wix.com → "wix"). El user
// reportó que estas se colaban a Prospects porque el blocklist era match EXACTO.
const BRAND_BLOCKLIST = new Set([
  "google","bing","yahoo","baidu","yandex","duckduckgo","ask",
  "facebook","instagram","twitter","tiktok","linkedin","reddit","pinterest","snapchat",
  "whatsapp","telegram","weibo","vk","quora","tumblr","threads",
  "wix","wixsite","squarespace","weebly","wordpress","blogger","webflow","godaddy","hostgator",
  "amazon","aliexpress","alibaba","ebay","mercadolibre","mercadolivre","walmart","target","rakuten",
  "apple","microsoft","office","live","outlook","windows","meta","alphabet",
  "netflix","spotify","disney","hulu","primevideo","hbomax","paramount","peacock",
  "adobe","oracle","sap","salesforce","hubspot","shopify","stripe","paypal","visa","mastercard",
  "samsung","sony","intel","amd","nvidia","dell","lenovo","huawei","xiaomi",
  "tesla","uber","lyft","airbnb","booking","expedia","tripadvisor","agoda",
  "github","gitlab","atlassian","slack","notion","figma","canva","zoom","dropbox","docusign",
  "youtube","vimeo","twitch","medium","substack","wikipedia","stackoverflow",
  "applovin","ironsource","criteo","taboola","outbrain","pubmatic","magnite","inmobi",
  "unity","digitalturbine","vungle","adcolony","mgid","revcontent","teads","vidoomy",
  // Maxi 2026-07-13 (auditoría): marcas-ANUNCIANTE (no venden inventario, son clientes potenciales
  // de pauta, NO prospects). Aparecieron recibiendo mail del agente. Núcleo de 2do nivel inequívoco.
  "adidas","nike","puma","reebok","realmadrid","fcbarcelona","fcbayern","cocacola","pepsico","mcdonalds","ikea","lego",
  // Maxi 2026-07-13 (barrido pool): plataformas/retailers/bancos/telcos globales que NO son publisher y
  // que el detector estructural puede no ver por estructura. Núcleos inequívocos (evito ambiguos:
  // action/but/orange/pepper/cultura). El detector estructural caza el resto de tiendas por carrito.
  "roblox","dailymotion","deepai","skyscanner","kayak","trivago",
  "esselunga","mytheresa","oriflame","johnlewis","fnac","noon","subito","njuskalo","chotot","buyma","snkrdunk",
  "zalando","asos","shein","temu","allegro","cdiscount","mediamarkt","decathlon","wayfair","etsy","taobao","tmall",
  "bbva","santander","revolut","virginmedia","earthlink","mydealz","promodescuentos","chollometro","nubank","mercadopago",
  "admiralmarkets","etoro","plus500","xtb","interactivebrokers","binance","coinbase","kraken", // brokers/exchanges (auditoría 2026-07-14)
]);

function isDomainBlocked(domain) {
  const d = domain.toLowerCase().replace(/^www\./, "");
  if (CORPORATE_BLOCKLIST.has(d)) return "corporate/brand";
  // Subdominios de un dominio blocklisteado (ej. m.amazon.com, fr.wix.com si wix.com está)
  for (const b of CORPORATE_BLOCKLIST) { if (d.endsWith("." + b)) return "corporate/subdomain"; }
  // Marca por 2do-nivel en cualquier TLD/subdominio (news.google.at, rakuten.tv, fr.wix.com)
  if (BRAND_BLOCKLIST.has(coreDomain(d))) return "corporate/brand-root";
  if (BLOCKED_TLDS.some(t => d.endsWith(t) || d.includes(t))) return "government/education";
  if (ADULT_TLDS.some(t => d.endsWith(t))) return "adult-tld";
  if (ADULT_KEYWORDS.some(k => d.includes(k))) return "adult-keyword";
  // Blacklist geográfica: descarta dominios de USA/CA/RU/AU/NZ/UA por TLD
  if (BLACKLIST_TLDS.some(t => d.endsWith(t))) return "geo-blacklist-tld";
  // Patrones corporativos/marca/institución (política user 2026-05-26)
  if (isCorporatePattern(d)) return "corporate-pattern";
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
    const cutoffUtc = _madridDayStartUtc();  // Maxi 2026-07-15 (F6): reset del día en Madrid (antes BA+Z = -3h e inconsistente)
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.done&uploaded_by=eq.${encodeURIComponent(userEmail)}&processed_at=gte.${cutoffUtc}&select=id`,
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
// Reverse: Monday user ID → email (Maxi 2026-06-18 v2: para que Monday Refresh
// tagee el lead con el MB dueño del item original, no con worker@autofeeder).
const MONDAY_ID_TO_EMAIL = Object.fromEntries(
  Object.entries(MONDAY_USER_IDS).map(([email, id]) => [String(id), email])
);

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
// + dueño (deal_owner — persona asignada). Maxi 2026-06-18 v2: el owner se usa
// para tagear la re-prospección con el MB original, no con worker@autofeeder.
async function findMondayItem(domain, mondayApiKey) {
  const clean = cleanDomain(domain);
  const query = `{
    boards(ids: [1420268379]) {
      items_page(limit: 5, query_params: { rules: [
        { column_id: "name", compare_value: "${clean}", operator: contains_text }
      ]}) {
        items {
          id name
          column_values(ids: ["deal_stage","deal_owner"]) { id text value }
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
    // deal_owner.value es JSON con personsAndTeams: [{ id: 56851451, kind: "person" }]
    let ownerEmail = "";
    const ownerCol = match.column_values?.find(c => c.id === "deal_owner");
    if (ownerCol?.value) {
      try {
        const parsed = JSON.parse(ownerCol.value);
        const personId = parsed?.personsAndTeams?.find(p => p.kind === "person")?.id;
        if (personId) ownerEmail = MONDAY_ID_TO_EMAIL[String(personId)] || "";
      } catch {}
    }
    return { id: match.id, estado, ownerEmail };
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
  // Maxi 2026-07-01: IMPORT MANUAL del MB (uploaded_by = email real, no worker@autofeeder).
  // Regla del user: un import manual SIEMPRE trae la web — NO se re-rechaza por "ya analizada"
  // (pageviews/categoría/geo/publisher/discovery). Lo ÚNICO que lo bloquea es que esté en un
  // deal ACTIVO de Monday (ciclo actual). El auto-feeder sí aplica todos los filtros de calidad.
  const isManualImport = !!item.uploaded_by && !/autofeeder/i.test(String(item.uploaded_by));

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
  // Maxi 2026-06-18: respetar el source original del csv_queue.
  // El feeder inyecta con auto_feeder_sellers / auto_feeder_monday / auto_feeder_majestic.
  // Antes se hardcodeaba "csv" → todo el chip filter quedaba inútil.
  let source;
  switch (item.source) {
    // Alias del feeder automático → source de Prospects
    case "auto_feeder_sellers":  source = "sellers_json"; break;
    case "auto_feeder_majestic": source = "autopilot";    break;
    case "auto_feeder_monday":   source = "monday_refresh"; break;
    case "auto_feeder_adstxt":   source = "autopilot";    break;  // Maxi 2026-07-01: faltaba → caía a "csv" (mal). ads.txt-graph = descubrimiento automático.
    // Maxi 2026-06-22 FIX: los imports MANUALES ya vienen con el source correcto
    // (sellers_json / monday_refresh / csv / autogoogle / autopilot) → PRESERVARLO.
    // Antes caían en default="csv" → el filtro de FUENTE en Prospects no respetaba nada.
    case "sellers_json":
    case "monday_refresh":
    case "csv":
    case "autogoogle":
    case "autopilot":           source = item.source; break;
    // Maxi 2026-07-01: default HONESTO por ORIGEN. El worker NO importa CSV — sus fuentes son
    // discovery automático. "csv"/"manual" solo son válidos si uploaded_by es un MB humano
    // (isManualImport). Un source desconocido de un item del worker/autofeeder NUNCA debe quedar
    // "csv" (mentía: leads del worker aparecían como imports manuales); cae a "autopilot".
    default:
      source = (item.source && String(item.source).trim())
        || (isManualImport ? "csv" : "autopilot");
  }
  let mondayItemId = null;

  if (mondayApiKey) {
    match = await findMondayItem(domain, mondayApiKey);
    if (match) {
      // Existe en Monday — bloquear SOLO si está en un estado ACTIVO/con-dueño
      // (lista negra). Ciclo Finalizado / Mail No Enviado / Descartado → se re-prospecta.
      if (_isMondayBlocked(match.estado)) {
        await markCsvItem(token, item.id, "skipped", {
          error_message: `monday_activo: estado="${match.estado || "?"}" (deal activo/con dueño — no se re-prospecta)`,
          monday_item_id: match.id,
        });
        log(`  ⏭ ${domain} — Monday estado "${match.estado}" es activo/con-dueño → skip`);
        return;
      }
      source = "monday_refresh";
      mondayItemId = match.id;
    }
  }

  // Maxi 2026-07-01: bypass de los filtros de CALIDAD para lo que el MB trae explícitamente
  // (import manual o Monday refresh). Estos NO se re-rechazan por "ya analizada"; solo los
  // frena el deal Monday activo (chequeado arriba) y la blocklist. El auto-feeder sí filtra.
  const bypassFilters = isManualImport || source === "monday_refresh";

  // Maxi 2026-07-01: TRAFFIC PRIMERO, SOLO. El page content (scrape) se difería a
  // después del floor de 350K para NO gastar scrape/API en sitios que se van a rechazar.
  // Antes traffic+content corrían en paralelo → todo sitio scrapeaba aunque fuera de 2K.
  const trafficData = await getTrafficData(domain, rapidapi_key);
  let { visits, pagesPerVisit, topCountry, topCountries3, swCategory } = trafficData;
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
  if ((!visits || visits <= 0) && !bypassFilters) {
    // Maxi 2026-06-22 FIX: NO penalizar/congelar por fallas TRANSITORIAS de la API
    // (429/5xx/timeout/red). Antes 3 timeouts seguidos congelaban un lead bueno 15-60d
    // y hasta lo mandaban a blocklist. Solo se cuenta como intento fallido cuando la API
    // respondió de verdad (0 visitas o 404 "sin data"). Transitorio → reintento limpio.
    const _errStr = String(trafficData.error || "");
    const _transient = /429|timeout|timed out|\b5\d\d\b|network|fetch failed|ETIMEDOUT|ECONNRESET|ECONNREFUSED|EAI_AGAIN|socket|aborted/i.test(_errStr);
    if (_transient) {
      await markCsvItem(token, item.id, "pending", {
        error_message: `traffic_api_transient (reintento sin penalizar): ${_errStr.slice(0, 50)}`,
      });
      log(`  🔁 ${domain} — API tráfico transitorio (${_errStr.slice(0, 40)}) → reintento, NO congela`);
      return;
    }
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
            // Maxi 2026-07-03 perf: invalidación de cache — al bloquear un dominio, agregarlo
            // al set cacheado (si está poblado) para no reprocesarlo hasta el próximo TTL de 5min.
            if (_adminBlocklistCacheWorker) {
              _adminBlocklistCacheWorker.add(domain.toLowerCase().replace(/^www\./, ""));
            }
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
  // FIX 2026-05-19: el threshold real del negocio son PAGE VIEWS, no visits.
  // Política user: si la API trajo pagesPerVisit (97% de los hits), usar
  // pageViews reales (visits × pagesPerVisit). Si no (3%), estimar como
  // visits × 2.0 (conservador vs el promedio observado 2.75 — no queremos
  // dejar entrar sitios de bajo tráfico por overshoot del estimador).
  const PPV_FALLBACK = 2.0;
  const ppvForThreshold = (typeof pagesPerVisit === "number" && pagesPerVisit > 0) ? pagesPerVisit : PPV_FALLBACK;
  // Maxi 2026-06-19: si el 2º chequeo (Hypestat scrape) trajo PÁGINAS VISTAS
  // directas, usarlas (más preciso que el estimado visits×ppv).
  const scrapePV = (typeof trafficData.pageViews === "number" && trafficData.pageViews > 0) ? trafficData.pageViews : null;
  const effectivePageViews = scrapePV != null ? scrapePV : Math.round(visits * ppvForThreshold);
  const usingFallback = scrapePV == null && !(typeof pagesPerVisit === "number" && pagesPerVisit > 0);
  // Maxi 2026-07-01: FLOOR DE 350K SIN EXCEPCIÓN. Antes `!bypassFilters` dejaba pasar imports
  // manuales y monday_refresh de <350K (aparecían prospects de 2K/12K/67K en la cola). El user
  // exige: "solo pasan los +350K, las de menos NI SE ANALIZAN". El floor de tráfico ya NO se
  // bypassa para nadie; los OTROS filtros de calidad (categoría/geo/config) sí siguen bypasseados
  // para lo que el MB trae explícito. Este check corre ANTES de scrape/Apollo → cero API gastada.
  if (effectivePageViews < REVIEW_QUEUE_MIN_TRAFFIC) {
    const label = scrapePV != null ? "hypestat pageviews" : (usingFallback ? `visits×${PPV_FALLBACK} fallback` : `visits×${ppvForThreshold.toFixed(2)}`);
    await markCsvItem(token, item.id, "skipped", {
      error_message: `pageviews ${effectivePageViews} (${label}) below min ${REVIEW_QUEUE_MIN_TRAFFIC}`,
    });
    log(`  ⏭ ${domain} — ${effectivePageViews} pageviews (${visits} visits × ${ppvForThreshold.toFixed(2)}) < ${REVIEW_QUEUE_MIN_TRAFFIC} (floor duro, sin bypass)`);
    return;
  }
  // Floor superado → AHORA sí traemos el HTML (scrape). No se gastó nada en sitios <350K.
  const pageContent = await fetchPageContent(domain).catch(() => null);
  // Maxi 2026-07-08: DOMINIO MUERTO (DNS no resuelve) → skip. El user reportó url caídas que
  // igual terminaban en Prospects sin email (gdpr.tubi.tv, wwwwwwwwwweb.2chblog.jp,
  // webtermin.medatixx.de, fr.wix.com). fetchPageContent devuelve {dead:true} SOLO cuando el DNS
  // falla (ENOTFOUND) — no cuando bloquea/timeout (esos podrían estar vivos). No se gasta Apollo.
  if (pageContent?.dead) {
    await markCsvItem(token, item.id, "skipped", { error_message: "dead_domain_dns_fail" });
    log(`  💀 ${domain} — dominio no resuelve (DNS) → skip, no es prospect`);
    return;
  }
  // FIX 2026-05-26: filtro por categoría SimilarWeb — bloquea marcas/instituciones
  // que pasaron por traffic pero no son publishers. Política user: no quiero ver
  // bancos, universidades, gobierno, marcas de autos, telcos, etc. en Prospects.
  const blockedCat = isCategoryBlockedWorker(swCategory);
  if (blockedCat && !bypassFilters) {
    await markCsvItem(token, item.id, "skipped", {
      error_message: `category-blocked: "${swCategory}" matchea "${blockedCat}"`,
    });
    log(`  ⊘ ${domain} — categoría "${swCategory}" bloqueada (matchea "${blockedCat}")`);
    return;
  }
  // Maxi 2026-06-17: GEO pre-filter. Si el sitio es de USA/Canadá/UK/AU/NZ/IE,
  // skipear ANTES de gastar Apollo + Claude. Hay demasiado USA y no nos sirve.
  // Excepción: si el lead vino de Monday refresh, lo procesamos igual (es un
  // re-engage del usuario, no descubrimiento aleatorio).
  if (!bypassFilters && _isWorkerDeprioGeo(topCountry, geosAllIso)) {
    await markCsvItem(token, item.id, "skipped", {
      error_message: `deprio-geo: ${topCountry || geosAllIso[0] || "?"} (USA/UK/CA/AU/NZ/IE no procesados)`,
    });
    log(`  🌎 ${domain} — GEO ${topCountry || geosAllIso[0]} bloqueado (no prioritario, ahorrando créditos)`);
    return;
  }

  // Maxi 2026-06-18: INTELIGENCIA DE BALANCE GEO. Si en review_queue.pending
  // hay MUCHOS de un mismo país (>25% del pool), skipear este lead para
  // priorizar diversidad. Excepción: monday_refresh + Weekly Focus específico.
  // Esto evita que el feeder llene Prospects con 200 sitios de Brasil.
  if (!bypassFilters) {
    const overrepresented = await _isGeoOverrepresentedInPool(token, topCountry, geosAllIso).catch(() => false);
    if (overrepresented) {
      // Maxi 2026-06-22 FIX: saturación de GEO es TRANSITORIA (el pool se rebalancea).
      // Antes se marcaba "skipped" → el dedup lo bloqueaba para siempre. Ahora "next_day":
      // reintenta cuando el pool baje de ese GEO. No se pierde el lead.
      await markCsvItem(token, item.id, "next_day", {
        error_message: `geo_saturated_in_pool: ${topCountry || "?"} >25% del pool (reintenta mañana)`,
      });
      log(`  ⚖️ ${domain} — GEO ${topCountry} saturado (>25%) → next_day (reintenta, no se pierde)`);
      return;
    }
  }
  // ── FILTRO DE BASURA (Maxi 2026-06-19) — descartar gov/uni/empresa/SaaS antes
  // de gastar Apollo+Claude. Excepción: monday_refresh (re-prospect explícito del MB).
  if (!bypassFilters) {
    const pub = await classifyPublisher(token, domain, pageContent, swCategory);
    if (!pub.ok) {
      await markCsvItem(token, item.id, "skipped", { error_message: `not_publisher: ${pub.reason}` });
      log(`  🗑 ${domain} — no parece publisher (${pub.reason}) → skip`);
      return;
    }
  }
  const category = pageContent?.category || swCategory || "";
  const adNetworks = pageContent?.adNetworks || [];
  const pageTitle = pageContent?.title || "";

  // Maxi 2026-06-19: filtro de DESCUBRIMIENTO configurable por el admin
  // (worker_discovery_config, editable desde el toggle 🏭 Worker). VACÍO = no filtra
  // (default: solo el bajo tráfico descarta). Si el admin setea prioridades/exclusiones
  // de GEO o categoría, el worker las respeta para lo que TRAE a Prospects.
  // monday_refresh se exceptúa (re-prospect explícito del MB).
  if (!bypassFilters) {
    let wd = {};
    try { wd = JSON.parse(cfg.worker_discovery_config || "{}"); } catch {}
    const geoPri = (wd.geos_priority || []).map(s => String(s).toUpperCase());
    const geoExc = (wd.geos_excluded || []).map(s => String(s).toUpperCase());
    const catPri = (wd.categories_priority || []).map(s => String(s).toLowerCase());
    const iso = String(geosAllIso[0] || "").toUpperCase();
    const nm  = String(topCountry || "").toUpperCase();
    const geoMatches = (set) => set.length && set.some(v => v === iso || v === nm);
    if (geoExc.length && geoMatches(geoExc)) {
      await markCsvItem(token, item.id, "skipped", { error_message: `worker_geo_excluded:${topCountry || iso}` });
      log(`  🏭 ${domain} — GEO ${topCountry || iso} EXCLUIDO por config del worker`);
      return;
    }
    if (geoPri.length && !geoMatches(geoPri)) {
      await markCsvItem(token, item.id, "skipped", { error_message: `worker_geo_not_priority:${topCountry || iso}` });
      log(`  🏭 ${domain} — GEO ${topCountry || iso} no está en la prioridad del worker`);
      return;
    }
    if (catPri.length && category && !catPri.some(c => category.toLowerCase().includes(c))) {
      await markCsvItem(token, item.id, "skipped", { error_message: `worker_cat_not_priority:${category}` });
      log(`  🏭 ${domain} — categoría "${category}" no está en la prioridad del worker`);
      return;
    }
  }

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

  // 2. Emails — Apollo unlock si visits >= 399K, scraping siempre como fallback.
  // Doble cap: diario (150) + mensual (2400 del plan). Si llega cualquiera,
  // skip Apollo y usa solo scraping. Cero impacto al flow (igual hay emails).
  const apolloMonthRemaining = (apolloUsage.monthLimit ?? APOLLO_MONTHLY_HARD_CAP) - (apolloUsage.usedThisMonth ?? 0);
  const canUseApollo = apollo_api_key
    && (apolloUsage.usedToday + apolloCallsThisSessionRef.count) < apolloUsage.limit
    && apolloMonthRemaining > 0;

  // Estrategia 2026-06-18 v2 (Maxi): Informer + Scrape gratis SIEMPRE,
  //   Apollo se mete RANDOM forzado por pacing diario para consumir el cupo
  //   mensual. La probabilidad de tirar Apollo escala con cuánto cupo queda
  //   en el día (más temprano del día → más probabilidad).
  //   - Si informer/scrape no encontraron persona → Apollo sí o sí (cubre el caso vacío)
  //   - Si informer/scrape sí encontraron → Apollo con prob = remaining_today / daily_limit
  //     (al inicio del día ~100%, al final ~0%). Cuando dispara aporta un 2do
  //     email decision-maker (Apollo siempre va PRIMERO en el array).
  let apolloRes = null;
  let scraperEmails = [];
  const informerSet = new Set();
  const urlByEmail  = new Map();
  const contactForms = new Set();
  let _socialOutCsvScope = null;
  // ── WATERFALL de captación (user 2026-07-08) ──────────────────────────────────
  // 1) INFORMER completo → 2) si vacío, SCRAPE → 3) si vacío, APOLLO pago (último recurso,
  // regulado por el cap mensual/diario duro). Cada tier corre SOLO si el anterior no encontró
  // NINGÚN email. Prioriza lo gratis; Apollo (que cuesta) solo cuando no hay otra.
  // TIER 1: Informer (gratis) — se QUEDA con TODO lo que encuentre (generic o no; antes perdía
  // los genéricos del informer al saltar a scrape).
  const infRes = await scrapeInformerOnly(domain).catch(() => ({ emails: [], urlByEmail: new Map() }));
  if (infRes.emails.length > 0) {
    scraperEmails = infRes.emails;
    infRes.emails.forEach(e => informerSet.add(e.toLowerCase()));
    infRes.urlByEmail.forEach((url, em) => urlByEmail.set(em, url));
    log(`  ✅ Tier 1 INFORMER ${domain} → ${infRes.emails.length} email(s): ${infRes.emails.slice(0,3).join(", ")}`);
  } else {
    // TIER 2: Scraping del sitio (gratis) — solo si el informer no encontró NADA.
    const socialOut = new Map();
    scraperEmails = await scrapeEmailsForDomain(domain, { informerOut: informerSet, urlByEmail, socialOut, contactFormsOut: contactForms }).catch(() => []);
    _socialOutCsvScope = socialOut;
    if (scraperEmails.length > 0) log(`  ✅ Tier 2 SCRAPE ${domain} → ${scraperEmails.length} email(s)`);
    else log(`  ○ ${domain}: informer + scrape SIN emails → Apollo (último recurso)`);
  }
  // TIER 3: Apollo PAGO — SOLO si informer Y scrape no encontraron NINGÚN email. forceUnlock
  // (paga para conseguir un decision-maker), pero el cap duro dentro de findBestApolloEmail
  // regula el gasto de créditos. Si ya hay CUALQUIER email de las fuentes gratis, NO se paga.
  if (canUseApollo && scraperEmails.length === 0) {
    apolloRes = await findBestApolloEmail(domain, apollo_api_key, token, { traffic: effectivePageViews, allowUnlock: true, forceUnlock: true })
      .then(r => { if (r?.source === "unlocked") { apolloCallsThisSessionRef.count += 1; log(`  💎 Apollo ${domain} → ${r.email}`); } return r; })
      .catch(() => null);
  }
  const apolloContactName = apolloRes?.contact_name || "";
  const apolloPhone       = apolloRes?.phone || "";
  const apolloEmail = apolloRes?.email ? [apolloRes.email] : [];
  // Apollo va PRIMERO en el array. emailSources mapea cada email a su origen
  // para pick prioritario en agent: apollo > informer > scrape > generic.
  // 2026-06-17: format = {source: "apollo|scrape|informer|generic", url?: string}
  // El UI muestra (apollo)/(informer)/(genérico) labels, y para "scrape" hace
  // clickeable la URL si está. Backward-compat: si el value es string, se trata
  // como source plano (rows viejas no tienen URL).
  const rawEmails = [...new Set([...apolloEmail, ...scraperEmails])];
  const emailSources = {};
  apolloEmail.forEach(e => { emailSources[e.toLowerCase()] = { source: "apollo", url: apolloRes?.source_url || "" }; });
  scraperEmails.forEach(e => {
    const lower = e.toLowerCase();
    if (!emailSources[lower]) {
      const local = (lower.split("@")[0] || "");
      const IS_GENERIC = GENERIC_LOCAL_RE; // unified — audit #2
      // Maxi 2026-06-17 v4: si el email vino de una red social, source = "Facebook"/"YouTube"/"Twitter"
      const socialSrc = _socialOutCsvScope?.get(lower);
      if (socialSrc) {
        emailSources[lower] = { source: socialSrc, url: urlByEmail.get(lower) || "" };
        return;
      }
      const url = urlByEmail.get(lower) || "";
      if (informerSet.has(lower) && !IS_GENERIC.test(local)) {
        emailSources[lower] = { source: "informer", url: url || "https://website.informer.com/" + domain };
      } else if (IS_GENERIC.test(local)) {
        emailSources[lower] = { source: "generic", url };
      } else {
        emailSources[lower] = { source: "scrape", url };
      }
    }
  });
  // Maxi 2026-06-18: contact forms detectados → persistir en email_sources
  // con key especial "__contact_form_N__" para que la UI los muestre como chips.
  if (contactForms.size > 0) {
    let i = 1;
    for (const cfUrl of contactForms) {
      emailSources[`__contact_form_${i}__`] = { source: "contact_form", url: cfUrl };
      i++;
      if (i > 3) break; // max 3 forms
    }
  }
  // Filtrar emails con dominio sin MX records ANTES de guardar
  const emails = await validateEmailsBatch(rawEmails);
  if (rawEmails.length !== emails.length) {
    log(`  📧 ${domain}: ${rawEmails.length} → ${emails.length} emails (apollo:${apolloRes?.source||"none"})`);
  }

  // Maxi 2026-06-18: GUARD anti-leads-fantasma. Si después de Apollo + scrape:
  //   1) NO hay emails Y
  //   2) NO pudimos cargar la home del sitio (pageContent === null = fetch falló)
  // → el sitio probablemente está caído / dominio inválido. NO guardar en
  // review_queue (aparecía como "disponible" sin contacto real). Maxi reportó
  // casos como contentiq.com, boonsmedia.com, home.gotsport.com.
  // CRITERIO ESTRICTO: solo si fetch falló totalmente — sites legitimos sin
  // title NO se filtran (pueden ser sitios chicos).
  // Maxi 2026-06-19: SIN EMAILS ya NO descarta. Política del user: si el sitio tiene
  // ≥350K pageviews, entra a Prospects igual (el MB puede conseguir el contacto después).
  // Lo ÚNICO que descarta en el descubrimiento es el bajo tráfico. Antes acá se tiraba
  // todo lo "sin emails + sitio caído" → leads válidos perdidos.
  if (emails.length === 0 && pageContent === null) {
    log(`  ⚠️ ${domain} — sin emails y fetch falló, pero tráfico OK → se agrega igual a Prospects (sin contacto)`);
  }
  // 3. NO empujar a Monday automáticamente. Escribir a review_queue para que el MB
  //    decida email + draft + push manualmente desde el tab Prospects.
  try {
    // Maxi 2026-06-19: guardamos PAGEVIEWS (no visits crudos) — es el número del
    // negocio y el que usan los pisos de saveToReviewQueue/cleanup/saturación.
    const saved = await saveToReviewQueue(token, {
      domain,
      traffic:        effectivePageViews || visits || 0,
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
      // Monday refresh tagea con el MB dueño del item original (deal_owner)
      // para que el filtro USUARIO en Prospects respete quién hizo el pedido.
      createdBy:      (source === "monday_refresh" && match?.ownerEmail) ? match.ownerEmail : (item.uploaded_by || ""),
      source,
      mondayItemId,
    });
    // Maxi 2026-06-19 (fix): respetar el resultado de saveToReviewQueue. Antes se
    // marcaba "done" SIEMPRE, aunque saveToReviewQueue devolviera false (rechazo
    // por dup o por el piso) → el item se contaba como "done" pero NUNCA llegaba
    // a Prospects (el famoso "34 done · 0 to Prospects"). Ahora si no se guardó,
    // se marca "skipped" con razón visible en el diagnóstico.
    if (saved === "ok") {
      await markCsvItem(token, item.id, "done", { monday_item_id: mondayItemId });
      // Bump counter global diario (cap safety net 1000/día)
      bumpCsvDailyCounter(token, 1).catch(() => {});
      const vstr = visits ? formatVisitsForMonday(visits) : "-";
      log(`  ✅ ${domain} → review_queue (source:${source}, visits:${vstr}, geo:${topCountry || "-"}, ${emails.length} email(s))`);
    } else {
      // Maxi 2026-06-19: motivo REAL del fallo (dup / floor / http_NNN) — antes se
      // mezclaba todo en "dup o piso" y ocultaba errores de INSERCIÓN reales.
      await markCsvItem(token, item.id, "skipped", {
        error_message: `review_queue_insert_fail:${saved}`,
        monday_item_id: mondayItemId,
      });
      log(`  ⏭️ ${domain} — saveToReviewQueue: ${saved} (no llegó a Prospects)`);
    }
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
        // Maxi 2026-07-03 perf: solo importa "¿hay ≥1 en waiting_pool?" → limit=1 sin count=exact,
        // evaluado por rows.length (evita el count exacto sobre toda la tabla).
        const wlRes = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.waiting_pool&select=id&limit=1`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
        );
        const wlRows = await wlRes.json();
        if (Array.isArray(wlRows) && wlRows.length === 0) {
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
    // Maxi 2026-07-15 (F3): heartbeat cada 5 items — runCsvQueue puede drenar por hasta 20min y antes no
    // actualizaba el heartbeat → la UI mostraba "worker muerto" mientras trabajaba.
    if (processed % 5 === 0) { try { await setConfigValue(token, "auto_heartbeat_at", new Date().toISOString()); } catch {} }

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
  // Maxi 2026-07-15 (Cost#1): RapidAPI ya NO se flushea acá (lo persiste el RPC atómico por hit) → evita doble-conteo.
  log(`◼ CSV queue end — procesados: ${processed}, apollo: ${callsRef.count}, rapidapi(session): ${_rapidGlobalCounter}`);
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

  // ── ROTACIÓN INTERNA AUTOPILOT con RATIO ADAPTATIVO (user 2026-05-13) ──
  // Mismo botón, 2 ideas: Majestic vs similar-sites discovery.
  // user 2026-05-29: el feeder YA cubre Majestic (su pool principal), así que el
  // autopilot redundaba y producía 0. Lo sesgamos a similar_discovery (su modo
  // único) — baseline 0.15 majestic / 0.85 similar, floor 0.05.
  let _probMajestic = 0.12;  // baseline: Majestic es relleno/exploración, NO protagonista (user 2026-06-19)
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
        // Techo 0.30 (user 2026-06-19): Majestic = lista global ruidosa → nunca domina.
        // Similar-discovery (parecidos a lo validado/enviado) es de mayor calidad y lleva el resto.
        _probMajestic = Math.max(0.05, Math.min(0.30, majLeads / (majLeads + simLeads)));
      }
      log(`📊 Stats hoy — Majestic: ${majLeads} leads, Similar: ${simLeads} leads → P(majestic)=${_probMajestic.toFixed(2)}`);
    }
  } catch {}
  // Maxi 2026-06-19: el autopilot SIEMPRE hace similar_discovery (su fortaleza única:
  // parecidos a lo que enviaste/validaste). Majestic YA lo cubre el feeder → NO replicar;
  // queda solo como fallback automático si no hubiera semillas (lo maneja el flujo de abajo).
  const _autopilotMode = "similar_discovery";
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
    // Seeds (user 2026-06-19, Autopilot v2): priorizar lo que YA funcionó — dominios
    // ENVIADOS (sendtrack: máxima señal de calidad y SOBREVIVE resets de la base) +
    // validados + pending>=350K como relleno + Monday. Antes sólo usaba pending>=400K,
    // que tras el reset quedó en 0 → el autopilot caía siempre a Majestic (baja calidad).
    const seedDomains = new Set();
    const _seedAuth = { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } };
    const _addSeeds = (rows) => { if (Array.isArray(rows)) rows.forEach(r => r.domain && seedDomains.add(r.domain.toLowerCase().replace(/^www\./, ""))); };
    try { // 1) Enviados recientes (sendtrack) — los que de verdad trabajamos
      const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_sendtrack?select=domain&order=send_date.desc&limit=15`, _seedAuth);
      if (r.ok) _addSeeds(await r.json());
    } catch {}
    try { // 2) Validados (aprobados por humano)
      const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.validated&select=domain&order=validated_at.desc&limit=10`, _seedAuth);
      if (r.ok) _addSeeds(await r.json());
    } catch {}
    try { // 3) Pending de alto tráfico (>=350K) como relleno
      const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.350000&select=domain&order=created_at.desc&limit=10`, _seedAuth);
      if (r.ok) _addSeeds(await r.json());
    } catch {}
    // 4) Monday activo
    mondayDomains.slice(0, 10).forEach(d => seedDomains.add(d));
    log(`Similar discovery seeds: ${seedDomains.size} dominios (sendtrack + validated + pending>=350K + Monday)`);
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

  // Maxi 2026-06-19 (ahorro de créditos): descartar TLDs claramente Anglo
  // (.us/.co.uk/.ca/.com.au/.nz/.ie) ANTES de pagar el lookup de tráfico. Antes
  // se pagaba RapidAPI y recién después se descartaban por GEO (desperdicio).
  const _beforeTld = candidates.length;
  candidates = candidates.filter(d => !DEPRIO_TLD_RE.test(d));
  if (_beforeTld !== candidates.length) log(`Pre-filtro TLD Anglo: -${_beforeTld - candidates.length} descartados sin gastar SimilarWeb`);

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

  // ── SEED desde likes + leads VALIDADOS de alto tráfico ──
  // user 2026-05-29: el autopilot producía 0 porque su pool era 99% conocidos.
  // Ahora sembramos con: (a) hasta 20 dominios likeados recientes, (b) hasta 15
  // leads validados (>=500K visitas) del review_queue. Eso son ~35 seeds × ~10
  // similares cada uno = ~350 candidatos frescos por sesión.
  const likedSeeds = feedback.likedDomains.slice(0, 20);
  // (b) Top leads validados recientes — los que ya pasaron por humano = high-signal seeds
  const validatedSeeds = [];
  try {
    const valRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.validated&traffic=gte.350000&select=domain&order=validated_at.desc&limit=15`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (valRes.ok) {
      const rows = await valRes.json();
      (Array.isArray(rows) ? rows : []).forEach(r => r.domain && validatedSeeds.push(r.domain.toLowerCase()));
    }
  } catch {}
  const allSeeds = [...new Set([...likedSeeds, ...validatedSeeds])];
  const likedSimilarDomains = new Set();
  if (allSeeds.length > 0) {
    log(`Seeding similar-sites desde ${allSeeds.length} seeds (${likedSeeds.length} likes + ${validatedSeeds.length} validados ≥500K)...`);
    const simResults = await Promise.all(
      allSeeds.map(d => findSimilarSites(d, rapidapi_key).catch(() => []))
    );
    for (const list of simResults) {
      for (const sim of list) {
        if (!mondaySet.has(sim) && !processed.has(sim)) likedSimilarDomains.add(sim);
      }
    }
    log(`  🔗 Similares de seeds: +${likedSimilarDomains.size} dominios frescos con prioridad`);
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
    // Maxi 2026-06-17: deprio GEO (USA/UK/CA/AU/NZ/IE) — skip antes de Apollo/scrape.
    // Ahorra Apollo credit + Claude tokens + bandwidth en sitios que no nos sirven.
    if (_isWorkerDeprioGeo(topCountry, geosAllIsoAuto)) {
      log(`  🌎 ${domain} — GEO ${topCountry} bloqueado (USA/UK/CA/AU/NZ/IE — no prioritario)`);
      await markProcessed(token, [domain], "country_deprio");
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
    // Si pagesPerVisit no vino en la respuesta (3% de los hits), asumimos 2.0
    // (conservador vs el promedio observado 2.75, política user 2026-05-19).
    const ppvSafe   = (typeof pagesPerVisit === "number" && pagesPerVisit > 0) ? pagesPerVisit : 2.0;
    // Maxi 2026-06-19: usar pageviews directos del 2º chequeo (Hypestat) si vinieron.
    const pageViews = (typeof trafficData.pageViews === "number" && trafficData.pageViews > 0) ? trafficData.pageViews : Math.round(visits * ppvSafe);
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

    // ── FILTRO DE BASURA (Maxi 2026-06-19) — antes el autopilot NO filtraba
    // gov/uni/empresa/SaaS (solo el path CSV lo hacía). Ahora también acá.
    const _pub = await classifyPublisher(token, domain, pageContent, trafficData?.swCategory);
    if (!_pub.ok) {
      log(`  🗑 ${domain} — no parece publisher (${_pub.reason}) → skip`);
      await markProcessed(token, [domain], "not_publisher");
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
    // Maxi 2026-06-30: GEO NEUTRAL. El user no quiere que el país influya en NADA del
    // descubrimiento ni del ranking (no descartar ni deprioritizar por geo). Antes geo
    // penalizaba/bonificaba el score → ahora geoPenalty/userGeoPenalty/userGeoBonus = 0.
    const geoPenalty = 0;

    // Penalización POR USUARIO (feedback like/dislike explícito) — mucho más fuerte
    const userCatDislikes = feedback.dislikedCategories.get(category) || 0;
    const userCatPenalty  = Math.min(40, userCatDislikes * 10);       // -10 por cada dislike, max -40
    const userGeoPenalty  = 0;

    // Bonus por LIKES: solo por categoría/contenido (geo neutral)
    const userCatLikes = feedback.likedCategories.get(category) || 0;
    const userCatBonus = Math.min(30, userCatLikes * 6);  // +6 por like, max +30
    const userGeoBonus = 0;
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

    // Maxi 2026-06-17: 3-tier — Informer FIRST (cheap), después 50/50 Apollo
    // vs HTML scrape. Si Informer trae non-genérico, saltamos Apollo (ahorra
    // crédito) y HTML scrape (ahorra bandwidth).
    const similarPromise = findSimilarSites(domain, rapidapi_key);
    let apolloRes = null;
    let scraperEmails = [];
    const informerSetAuto = new Set();
    const urlByEmailAuto  = new Map();
    const contactFormsAuto = new Set();
    const infResAuto = await scrapeInformerOnly(domain).catch(() => ({ emails: [], urlByEmail: new Map() }));
    const infAutoNonGeneric = infResAuto.emails.find(e => !_isGenericLocalPart(e));
    if (infAutoNonGeneric) {
      scraperEmails = infResAuto.emails;
      infResAuto.emails.forEach(e => { informerSetAuto.add(e.toLowerCase()); });
      infResAuto.urlByEmail.forEach((url, em) => { urlByEmailAuto.set(em, url); });
      log(`  ✅ Informer hit ${domain} (autopilot) → ${infAutoNonGeneric} (skip Apollo + HTML)`);
    } else {
      const useApolloFirst = canUseApollo && Math.random() < 0.5;
      // socialOut: emails de redes sociales (autopilot path)
      var socialOutAuto = new Map();
      if (useApolloFirst) {
        apolloRes = await findBestApolloEmail(domain, apollo_api_key, token, { traffic: visits, allowUnlock: true })
          .then(r => { if (r?.source === "unlocked") apolloCallsThisSession += 1; return r; })
          .catch(() => null);
        if (!apolloRes?.email) scraperEmails = await scrapeEmailsForDomain(domain, { informerOut: informerSetAuto, urlByEmail: urlByEmailAuto, socialOut: socialOutAuto, contactFormsOut: contactFormsAuto }).catch(() => []);
      } else {
        scraperEmails = await scrapeEmailsForDomain(domain, { informerOut: informerSetAuto, urlByEmail: urlByEmailAuto, socialOut: socialOutAuto, contactFormsOut: contactFormsAuto }).catch(() => []);
        if (scraperEmails.length === 0 && canUseApollo) {
          apolloRes = await findBestApolloEmail(domain, apollo_api_key, token, { traffic: visits, allowUnlock: true })
            .then(r => { if (r?.source === "unlocked") apolloCallsThisSession += 1; return r; })
            .catch(() => null);
        }
      }
    }
    const similarSites = await similarPromise;
    const apolloContactNameAuto = apolloRes?.contact_name || "";
    const apolloPhoneAuto       = apolloRes?.phone || "";
    const apolloEmailAuto = apolloRes?.email ? [apolloRes.email] : [];
    const rawEmailsAuto = [...new Set([...apolloEmailAuto, ...scraperEmails])];
    // Source tracking: apollo > informer > scrape > generic. Object format con URL tracking.
    const emailSourcesAuto = {};
    apolloEmailAuto.forEach(e => { emailSourcesAuto[e.toLowerCase()] = { source: "apollo", url: apolloRes?.source_url || "" }; });
    scraperEmails.forEach(e => {
      const lower = e.toLowerCase();
      if (emailSourcesAuto[lower]) return;
      const local = (lower.split("@")[0] || "");
      const IS_GENERIC = GENERIC_LOCAL_RE; // unified — audit #2
      // Maxi 2026-06-17 v4: redes sociales primero (Facebook/YouTube/Twitter)
      const socialSrcA = (typeof socialOutAuto !== "undefined") ? socialOutAuto.get(lower) : null;
      if (socialSrcA) {
        emailSourcesAuto[lower] = { source: socialSrcA, url: urlByEmailAuto.get(lower) || "" };
        return;
      }
      const url = urlByEmailAuto.get(lower) || "";
      if (informerSetAuto.has(lower) && !IS_GENERIC.test(local)) {
        emailSourcesAuto[lower] = { source: "informer", url: url || "https://website.informer.com/" + domain };
      } else if (IS_GENERIC.test(local)) {
        emailSourcesAuto[lower] = { source: "generic", url };
      } else {
        emailSourcesAuto[lower] = { source: "scrape", url };
      }
    });
    // Maxi 2026-06-18: contact forms en autopilot path
    if (contactFormsAuto.size > 0) {
      let i = 1;
      for (const cfUrl of contactFormsAuto) {
        emailSourcesAuto[`__contact_form_${i}__`] = { source: "contact_form", url: cfUrl };
        i++;
        if (i > 3) break;
      }
    }
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

    // Maxi 2026-06-18: mismo guard para autopilot — leads sin email + fetch falló
    if (emails.length === 0 && pageContent === null) {
      log(`  ⛔ ${domain} — sin emails Y fetch del sitio falló → skip autopilot`);
      await markProcessed(token, [domain], "no_emails_unreachable");
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    const saveOk = await saveToReviewQueue(token, {
      domain,
      // Maxi 2026-06-19: PAGEVIEWS (visits × ppv), no visits crudos — consistente
      // con el piso de saveToReviewQueue/cleanup y con lo que muestra la UI.
      traffic:       pageViews || visits,
      geo:           topCountry,
      geosAll:       geosAllIsoAuto,
      language,
      category,
      contactName:   apolloContactNameAuto || contactName,
      contactPhone:  apolloPhoneAuto,
      emails,
      emailSources:  emailSourcesAuto,
      pitch:         "",
      pitchSubject:  "",
      pitchSubjects: [],
      score:         scoreWithEmails,
      adNetworks,
      pageTitle,
      createdBy:     sessionUser,
    });
    if (saveOk === "ok") {
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
  // Maxi 2026-07-15 (Cost#1): RapidAPI ya NO se flushea acá (lo persiste el RPC atómico por hit) → evita doble-conteo.
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
  max_per_day:          10,        // Maxi 2026-06-30: 10/día para cada MB (5 slots × 2)
  active_hours_start:   9,         // 9am España (CET/CEST)
  active_hours_end:     23,        // 23hs España
  active_timezone:      "Europe/Madrid",
  per_cycle_limit:      2,         // max 2 leads por slot 9/12/15/18/20 Madrid L-V (= 10/día)
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

  // Maxi 2026-07-08: el pool del agente = los DB drafts (los MISMOS que el MB ve/edita
  // en Analysis) — así NO se duplica con los baked. Baked queda solo como fallback si la
  // DB no tiene drafts. Selección UNIFORME 33/33/33 entre los 3 borradores (el user pidió
  // reparto parejo, NO ponderado por open-rate). Con 3 defaults → ~33% cada uno.
  const pool = dbDrafts.length > 0 ? dbDrafts : baked;
  if (pool.length === 0) return { template: null, templateId: null };
  const picked = pool[Math.floor(Math.random() * pool.length)];

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

    // Maxi 2026-07-03 perf: antes era N+1 (2 queries por send × hasta 200 sends =
    // ~400 reads secuenciales por ciclo). Ahora se batchea en 2 queries usando in.(...):
    // 1 para los opens de todos los agent_action_id, 1 para los re_sent/exhausted de
    // todos los dominios. Se filtra en memoria. Ahorro IO/egress ALTO en cada ciclo.
    const _auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
    const sendIds = sends.map(s => s.id);
    const sendDomains = [...new Set(sends.map(s => s.domain).filter(Boolean))];

    // Set de agent_action_id que SÍ abrieron (para descartarlos)
    const openedSet = new Set();
    try {
      const oRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_email_opens?agent_action_id=in.(${sendIds.join(",")})&select=agent_action_id`,
        { headers: _auth }
      );
      if (oRes.ok) (await oRes.json()).forEach(r => openedSet.add(String(r.agent_action_id)));
    } catch {}

    // Set de dominios ya re-engagéd o agotados
    const handledSet = new Set();
    if (sendDomains.length) {
      try {
        const rRes = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=in.(re_sent,reengagement_exhausted)&domain=in.(${sendDomains.map(d => encodeURIComponent(d)).join(",")})&select=domain`,
          { headers: _auth }
        );
        if (rRes.ok) (await rRes.json()).forEach(r => handledSet.add(r.domain));
      } catch {}
    }

    const candidates = sends.filter(s =>
      !openedSet.has(String(s.id)) && !handledSet.has(s.domain)
    );
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

    // Maxi 2026-06-22: mismos gates que el envío normal — NO re-engagear fin de semana
    // ni fuera de horario laboral (evita mails de madrugada/sábado y cuida reputación Gmail).
    if (_isWeekendSpain()) { log("🔄 Re-engagement: fin de semana → skip"); return; }
    const _reAStart = parseInt(cfg.agent_active_hours_start || "9", 10);
    const _reAEnd   = parseInt(cfg.agent_active_hours_end || "20", 10);
    if (_isOutsideActiveHours(_reAStart, _reAEnd)) { log("🔄 Re-engagement: fuera de horario → skip"); return; }

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
        // Maxi 2026-06-22 FIX: marcar AGOTADO (acción terminal) en vez de "skipped".
        // Antes findUnopenedSends seguía devolviendo este dominio CADA ciclo (25 min)
        // porque solo excluye los que tienen re_sent → loop infinito (los 1154 casos).
        // Ahora 'reengagement_exhausted' lo excluye para siempre.
        log(`  ⏭ ${domain}: sin emails alternativos → AGOTADO (no se re-evalúa más)`);
        await logAgentAction(token, userEmail, {
          domain, action: "reengagement_exhausted", reason: "no_alt_email",
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
    const url = `${SUPABASE_URL}/rest/v1/toolbar_reengagement_queue?status=eq.pending&scheduled_for=lte.${encodeURIComponent(nowIso)}&select=id,domain,monday_item_id,mb_email,future_email,original_subject,original_body,tracking_action_id,original_email&order=scheduled_for.asc&limit=20`; // Maxi 2026-07-03 perf: select=* → solo columnas destructuradas
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
          if (!sent?.id) {   // Maxi 2026-07-08 BUG FIX: Gmail éxito = {id,...}, no {ok:true}
            newStatus = "failed";
            reason = `gmail send failed: ${sent?.error || "no_message_id"}`;
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
    // Query Gmail AMPLIADA (Maxi 2026-06-17). Antes era muy estrecha — solo
    // capturaba subjects en inglés y 20 mensajes. Maxi reporta 5-10 rebotes/día
    // pero solo veíamos ~0.6/día. Cambios:
    //   - Subjects: agregamos patrones que en realidad llegan (Mail Delivery,
    //     wasn't delivered, address rejected) + español + portugués + italiano
    //   - Sender: además de daemons, sumamos common bounce senders + "bounce@"
    //   - Window: 7 días (era 1d) con dedupe vía isBouncedSync → no reprocesa
    //   - maxResults: 100 (era 20) → cubre el peor día
    //   - in:anywhere → Spam + Papelera incluidos (ya estaba)
    const q = encodeURIComponent(
      'from:(mailer-daemon OR postmaster OR mail-daemon OR bounce OR no-reply-bounce OR returns OR delivery) ' +
      'subject:(' +
        'undelivered OR "delivery status" OR "returned mail" OR "failure notice" OR ' +
        '"mail delivery" OR "delivery failed" OR "wasn\'t delivered" OR "address rejected" OR ' +
        '"could not be delivered" OR "permanent failure" OR "delivery problem" OR ' +
        '"no se ha entregado" OR "mensaje no entregado" OR "devuelto al remitente" OR ' +
        '"correo no entregado" OR "fallo en la entrega" OR ' +
        '"não foi entregue" OR "mensagem não entregue" OR "devolvido" OR ' +
        '"non recapitato" OR "consegna non riuscita" OR ' +
        '"non distribué" OR "n\'a pas pu être remis"' +
      ') newer_than:7d in:anywhere'
    );
    const listRes = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${q}&maxResults=100`,
      { headers: { "Authorization": `Bearer ${accessToken}` } }
    );
    if (!listRes.ok) {
      // Maxi 2026-06-17: 403 ya NO es silencioso. Antes no sabíamos si Gmail
      // estaba devolviendo 403 (= scope readonly no autorizado en Workspace),
      // por eso nunca nos enterábamos que el scan no corría. Ahora logeamos
      // y registramos en toolbar_agent_actions para visibilidad.
      log(`⚠️ scanBounces ${userEmail} HTTP ${listRes.status} — scan NO ejecutado`);
      if (listRes.status === 403) {
        logAgentAction(token, userEmail, {
          domain: "_scan_failed_",
          action: "bounce_scan_failed",
          reason: "gmail_403_scope_missing",
          details: { hint: "agregar gmail.readonly al Workspace OAuth scope", status: 403 },
        }).catch(() => {});
      }
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
            // Maxi 2026-06-22: un SOFT bounce (buzón lleno / temporal / 4xx / rate-limit)
            // es TRANSITORIO → NO blacklistear el email para siempre (la semana que viene
            // puede entrar). Solo hard/unknown matan el contacto. El retry a un alternativo
            // igual se dispara abajo, así tenemos cobertura sin quemar el email soft.
            if (bounceType !== "soft") {
              await markEmailBounced(token, { email: failed, reason: `smtp_bounce_${bounceType}`, originalDomain: failed.split("@")[1] });
            } else {
              log(`  ↩️ soft bounce ${failed} — transitorio, NO se blacklistea (se podrá reintentar)`);
            }
            detected++;
            // user 2026-05-29: TODO rebote detectado (hard, soft/buzón-lleno o unknown)
            // dispara retry → busca email nuevo, reenvía, actualiza Prospect + FU1/FU2.
            queueBounceRetry(token, userEmail, failed, bounceType).catch(e => log(`⚠️ queueBounceRetry: ${e.message}`));
            // Maxi 2026-06-17: log de visibilidad — el MB ve en el panel admin
            // que el sistema detectó el bounce, qué tipo y qué hizo.
            logAgentAction(token, userEmail, {
              domain: failed.split("@")[1] || "_bounce_",
              action: "bounce_detected",
              reason: `${bounceType}_bounce`,
              details: { failed_email: failed, bounce_type: bounceType, scan_source: "gmail_inbox_spam_trash" },
            }).catch(() => {});
          }
        }
      } catch {}
    }
    // Maxi 2026-06-17: log SIEMPRE — visibilidad de cuántos msgs encontró el
    // scan, no solo cuando detecta nuevo. Si ids.length > 0 pero detected = 0,
    // significa que todos ya estaban marcados (bueno) o el parser falló (malo).
    log(`📬 scanBounces ${userEmail}: ${ids.length} msgs Gmail · ${detected} bounces nuevos`);
    if (detected) {
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
        // user 2026-05-29: un auto-reply (out-of-office / vacaciones) PRUEBA que el
        // email es VÁLIDO y hay una persona real — solo está ausente. NO lo
        // blacklisteamos (antes lo hacía y abandonaba decision-makers buenos para
        // siempre). Sí probamos un contacto alternativo por si es cola de tickets,
        // pero el original queda usable para futuros intentos/cadencia.
        await queueBounceRetry(token, userEmail, respondedFrom, "auto_reply").catch(e => log(`⚠️ autoReply retry: ${e.message}`));
        detected++;
        log(`🔁 auto-reply (ausencia) de ${respondedFrom}${isAutoSubmitted ? " (header)" : " (subject)"} → email VÁLIDO (no blacklist), probando alternativa`);
        // Maxi 2026-06-17: log de visibilidad para el panel admin
        logAgentAction(token, userEmail, {
          domain: respondedFrom.split("@")[1] || "_oao_",
          action: "auto_reply_detected",
          reason: isAutoSubmitted ? "header_auto_submitted" : "subject_match",
          details: { responded_from: respondedFrom, scan_source: "gmail_inbox_spam_trash" },
        }).catch(() => {});
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
// SCAN RESPUESTAS REALES — Maxi 2026-06-18
// ────────────────────────────────────────────────────────────────
// Objetivo: detectar cuando un contacto responde REAL (no OOO, no bounce,
// no auto-reply). Cada respuesta real se marca en toolbar_response_tracking
// para alimentar las stats de conversion rate por source/geo/category.
//
// Lógica:
//   1. Leer envíos sin respuesta de últimos 14 días (mb_email)
//   2. Para cada uno, buscar en Gmail si llegó respuesta del email_sent_to
//   3. Si llegó: chequear si es OOO / auto-reply (NO cuenta) o real
//   4. Marcar responded_at + response_type ("real"|"ooo")
// ════════════════════════════════════════════════════════════════
async function scanRealResponsesForUser(token, userEmail) {
  try {
    const accessToken = await getGmailAccessToken(userEmail);
    // Envíos pendientes de respuesta — últimos 14 días, sin responded_at
    const since14d = new Date(Date.now() - 14 * 86400_000).toISOString();
    const trackRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_response_tracking?mb_email=eq.${encodeURIComponent(userEmail.toLowerCase())}&responded_at=is.null&sent_at=gte.${since14d}&select=id,email_sent_to,sent_at,domain&order=sent_at.desc&limit=200`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!trackRes.ok) return 0;
    const pending = await trackRes.json();
    if (!Array.isArray(pending) || pending.length === 0) return 0;

    let detected = 0;
    // Procesar en chunks para no spammear Gmail
    for (const row of pending) {
      try {
        const to = (row.email_sent_to || "").toLowerCase();
        if (!to.includes("@")) continue;
        // Query Gmail: respuestas DEL contact al userEmail después del envío
        const sentDate = new Date(row.sent_at);
        const afterDate = `${sentDate.getUTCFullYear()}/${sentDate.getUTCMonth()+1}/${sentDate.getUTCDate()}`;
        const q = encodeURIComponent(`from:${to} to:${userEmail} after:${afterDate} in:anywhere`);
        const listRes = await fetch(
          `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${q}&maxResults=5`,
          { headers: { "Authorization": `Bearer ${accessToken}` } }
        );
        if (!listRes.ok) continue;
        const list = await listRes.json();
        const ids = (list.messages || []).map(m => m.id);
        if (!ids.length) continue;

        // Hay respuesta — chequear si es OOO / auto-reply
        let isAutoReply = false;
        for (const id of ids) {
          const msgRes = await fetch(
            `https://gmail.googleapis.com/gmail/v1/users/me/messages/${id}?format=metadata&metadataHeaders=Auto-Submitted&metadataHeaders=Subject&metadataHeaders=X-Autoreply&metadataHeaders=X-Auto-Response-Suppress`,
            { headers: { "Authorization": `Bearer ${accessToken}` } }
          );
          if (!msgRes.ok) continue;
          const msg = await msgRes.json();
          const headers = msg.payload?.headers || [];
          const autoSub = headers.find(h => h.name?.toLowerCase() === "auto-submitted")?.value || "";
          const subj = headers.find(h => h.name?.toLowerCase() === "subject")?.value || "";
          const hasAutoReplyHeader = !!headers.find(h => ["x-autoreply","x-auto-response-suppress"].includes(h.name?.toLowerCase()));
          const subjOOO = /\b(out of office|out-of-office|away|ausencia|vacation|vacaciones|férias|ferie|abwesen|auto[-\s]?reply|automatic reply|automatisch|absence|absencia)\b/i.test(subj);
          if (/auto-replied|auto-generated/i.test(autoSub) || hasAutoReplyHeader || subjOOO) {
            isAutoReply = true;
            break;
          }
        }
        const responseType = isAutoReply ? "ooo" : "real";
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_response_tracking?id=eq.${row.id}`, {
          method: "PATCH",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ responded_at: new Date().toISOString(), response_type: responseType }),
        }).catch(() => {});
        if (!isAutoReply) detected++;
      } catch {}
    }
    if (detected > 0) {
      log(`💌 scanRealResponses ${userEmail}: ${detected} respuesta(s) REAL detectada(s)`);
    }
    return detected;
  } catch (e) {
    log(`⚠️ scanRealResponses error: ${e.message}`);
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

    // 0. Maxi 2026-06-17 (audit #3): si el DOMINIO está en blocklist global
    // (inoperativo / dead / NSFW), NO intentar retry — es esfuerzo perdido y
    // el agente puede mandar 2x a dominios ya descartados.
    try {
      const blockReason = await isDomainBlockedFull(domain, token);
      if (blockReason) {
        log(`  ⏭️ ${domain}: ya está en blocklist (${blockReason}) — skip retry`);
        return;
      }
    } catch {}

    // 0a. Maxi 2026-07-15: si el dominio YA está frozen (frozen_until > now), skip TODO.
    // Bug visto en logs: un soft-bounce recurrente (ej. info@evima.gr) que ya llegó a max retries
    // y quedó frozen 60d se re-detectaba en CADA scan de Gmail y volvía a entrar acá → re-freeze +
    // "Monday email column limpiada" en loop (spam a Monday + trabajo al pedo + reclear repetido).
    // Un dominio frozen no se toca hasta que el unfreezer lo libere.
    try {
      const _nowIso = new Date().toISOString();
      const _frRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_frozen_leads?domain=eq.${encodeURIComponent(domain)}&frozen_until=gt.${encodeURIComponent(_nowIso)}&select=domain&limit=1`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      if (_frRes.ok) {
        const _fr = await _frRes.json().catch(() => []);
        if (Array.isArray(_fr) && _fr.length > 0) {
          log(`  🧊 ${domain}: ya frozen — skip bounce retry (no re-freeze ni re-clear Monday)`);
          return;
        }
      }
    } catch {}

    // 0b. Maxi 2026-06-18: AUTO-PROMOTE en Monday.
    // Si el dominio recibió OTROS envíos del mismo MB en últimos 7d (vía
    // response_tracking — los adicionales que mandamos día 0 con el original),
    // buscar el primero que NO rebotó y actualizar Monday SIN re-mandar mail.
    // Esto cubre el caso "manda al original + 3 adicionales → original rebota
    // → Monday queda con email muerto → reemplazar por el adicional que llegó".
    try {
      const cutoff7d = new Date(Date.now() - 7 * 86400_000).toISOString();
      const otherSendsRes = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_response_tracking?mb_email=eq.${encodeURIComponent(mbEmail.toLowerCase())}&domain=eq.${encodeURIComponent(domain)}&sent_at=gte.${cutoff7d}&select=email_sent_to,response_type,sent_at&order=sent_at.desc&limit=20`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
      );
      if (otherSendsRes.ok) {
        const sends = await otherSendsRes.json();
        if (Array.isArray(sends) && sends.length > 1) {
          // Buscar el primer email que NO rebotó (y no es el que acaba de rebotar)
          const alive = sends.find(s => {
            const e = (s.email_sent_to || "").toLowerCase();
            return e !== bouncedEmail.toLowerCase() && !isBouncedSync(e);
          });
          if (alive && alive.email_sent_to) {
            log(`  🔁 ${domain}: auto-promote Monday — bounced=${bouncedEmail} → vivo=${alive.email_sent_to}`);
            // Buscar el lead en review_queue (o Monday item directo)
            try {
              const leadRes = await fetch(
                `${SUPABASE_URL}/rest/v1/toolbar_review_queue?domain=eq.${encodeURIComponent(domain)}&select=monday_item_id&order=created_at.desc&limit=1`,
                { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
              );
              const leadRows = leadRes.ok ? await leadRes.json() : [];
              const mondayItemId = leadRows[0]?.monday_item_id;
              const cfg = await getConfig(token);
              const mondayApiKey = getMondayKeyForUser(cfg, mbEmail) || cfg.monday_api_key;
              if (mondayItemId && mondayApiKey) {
                const boardId = cfg.monday_active_board || cfg.monday_board_id || 1420268379;
                await updateMondayReengagementDispatch(mondayApiKey, mondayItemId, boardId, alive.email_sent_to);
                log(`  ✅ ${domain}: Monday actualizado con email vivo ${alive.email_sent_to}`);
                logAgentAction(token, mbEmail, {
                  domain, action: "auto_promoted",
                  reason: "bounce_swap_to_alive_adicional",
                  details: { bounced: bouncedEmail, promoted: alive.email_sent_to, monday_item_id: mondayItemId },
                }).catch(() => {});
                return; // No hace falta retry — el adicional ya fue enviado el día 0
              }
            } catch (e) { log(`  ⚠️ auto-promote fail ${domain}: ${e.message}`); }
          }
        }
      }
    } catch (e) { log(`  ⚠️ auto-promote lookup ${domain}: ${e.message}`); }

    // 1. Guard: cooldown POR EMAIL bounceado (no por domain) — Maxi 2026-06-17.
    // Antes: 1 retry por domain/24h → si rebotaban 3 emails distintos del mismo
    // domain en el día, solo 1 disparaba retry. Ahora: cooldown solo si el
    // MISMO email ya disparó retry en últimas 6h (evita duplicados de mismo
    // bounce reportado 2 veces por Gmail).
    const cutoff6h = new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString();
    const recentRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_bounce_retries?original_email=eq.${encodeURIComponent(bouncedEmail)}&created_at=gte.${cutoff6h}&select=id&limit=1`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (recentRes.ok) {
      const recent = await recentRes.json();
      if (Array.isArray(recent) && recent.length > 0) {
        log(`  ⏭️ ${bouncedEmail}: ya disparó retry en últimas 6h — skip (dedupe)`);
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
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?domain=eq.${encodeURIComponent(domain)}&select=id,monday_item_id,emails,email_sources,category,traffic,pitch,pitch_subject,pitch_subjects&order=created_at.desc&limit=1`, // Maxi 2026-07-03 perf: select=* → solo columnas usadas en el flujo de bounce
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
          // Maxi 2026-07-08: forceUnlock en el RESCATE por bounce — el email anterior rebotó,
          // así que vale pagar Apollo para conseguir un decision-maker nuevo aunque el tráfico
          // esté bajo el umbral. Sigue capado por APOLLO_MONTHLY_HARD_CAP (2400/mes).
          const apolloRes = await findBestApolloEmail(domain, apolloKey, token, { traffic: lead.traffic || 0, allowUnlock: true, forceUnlock: true });
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
        source: (manualFutureEmail && e.toLowerCase() === manualFutureEmail) ? "manual" : _normSrc(_sourcesMap[e.toLowerCase()]),
        score: rankEmail(e, domain, lead.category || ""),
      }))
      // FIX 2026-05-19: manual future_email del MB nunca se filtra por score.
      // Es decisión explícita del MB — si rankEmail penaliza ("info@" etc), lo
      // dejamos pasar igual con score forzado a 0 para que entre al sort.
      .map(x => (x.source === "manual" && x.score < 0) ? { ...x, score: 0 } : x)
      .filter(x => x.score >= 0)
      .sort((a, b) => {
        // Maxi 2026-07-09: tier DURO primero (_pickTier): apollo/informer nominal > rol
        // comercial/publicidad scrapeado > persona scrapeada > genérico. Honra la regla del
        // dueño (decision-maker verificado manda) Y la elección del user (Q4: rol comercial).
        const ta = _pickTier(a.email, a.source);
        const tb = _pickTier(b.email, b.source);
        if (ta !== tb) return tb - ta;
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
      // Maxi 2026-07-08 BUG FIX: sendGmailServer devuelve la respuesta CRUDA de Gmail
      // en éxito ({id, threadId, labelIds}) — NO tiene campo .ok. El check `sent?.ok===true`
      // daba SIEMPRE false aunque el mail se enviara → 161 re-envíos marcados "failed"
      // (Monday no se actualizaba, se congelaba el lead habiendo enviado). Éxito = hay `id`.
      sendOk = !!(sent && sent.id);
      if (!sendOk) sendErr = sent?.error || "no_message_id";
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

      // 6a. Update Prospect (review_queue): sacar el email rebotado y poner el
      // nuevo PRIMERO, para que la cola refleje el contacto válido (user 2026-05-29).
      try {
        const baseEmails = (lead.emails || []).filter(e => e && e.toLowerCase() !== bouncedEmail.toLowerCase());
        const newEmails  = [retryEmail, ...baseEmails.filter(e => e.toLowerCase() !== retryEmail.toLowerCase())];
        const newSources = { ...(lead.email_sources || {}) };
        delete newSources[bouncedEmail.toLowerCase()];
        if (!newSources[retryEmail.toLowerCase()]) newSources[retryEmail.toLowerCase()] = retrySource || "rescue";
        if (lead.id) {
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
            method: "PATCH",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
            body: JSON.stringify({ emails: newEmails, email_sources: newSources }),
          }).catch(() => {});
        }
      } catch (e) { log(`  ⚠️ ${domain}: review_queue update FAIL: ${e.message}`); }

      // 6b. Update Monday — email + RESET FU1 (today+5) + FU2 (today+10).
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

// ════════════════════════════════════════════════════════════════
// RECONCILIACIÓN Monday — Maxi 2026-07-08 (one-time cleanup)
// ────────────────────────────────────────────────────────────────
// Los bounce_retries con status='failed' y reason='unknown' son FALSOS FALLOS del
// bug del .ok: el re-envío SÍ se mandó al retry_email, pero como el código creyó que
// falló, NUNCA actualizó Monday (quedó con el email viejo/rebotado). Esta función pone
// el retry_email (contacto alternativo válido) en Monday + review_queue y marca la fila
// 'reconciled' para no re-procesarla. Gate: agent_reconcile_monday_bounces=true. Batch
// de 30/run; se auto-apaga (flag OFF) cuando no queda nada.
// ════════════════════════════════════════════════════════════════
async function reconcileMondayBounces(token) {
  try {
    const cfg = await getConfig(token);
    if (String(cfg.agent_reconcile_monday_bounces || "").toLowerCase() !== "true") return;
    const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_bounce_retries?status=eq.failed&reason=eq.unknown&retry_email=neq.&monday_item_id=not.is.null&select=id,domain,monday_item_id,retry_email,mb_email&limit=30`,
      { headers: auth }
    );
    if (!res.ok) return;
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) {
      log("✅ reconcile-monday-bounces: nada más que reconciliar → flag OFF");
      await setConfigValue(token, "agent_reconcile_monday_bounces", "false").catch(() => {});
      return;
    }
    log(`🔧 reconcile-monday-bounces: ${rows.length} items (Monday quedó con email viejo por el bug .ok)`);
    for (const r of rows) {
      try {
        const mondayKey = (cfg[`monday_api_key_${(r.mb_email || "").toLowerCase()}`] || cfg.monday_api_key || "").trim();
        if (mondayKey && r.monday_item_id && r.retry_email) {
          await updateMondayItem(r.monday_item_id, { [MONDAY_COL_EMAIL]: { email: r.retry_email, text: r.retry_email } }, mondayKey).catch(() => {});
        }
        // Reflejar el email correcto también en review_queue (Prospects)
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?domain=eq.${encodeURIComponent(r.domain)}&select=id,emails,email_sources&limit=1`, { headers: auth })
          .then(rq => rq.ok ? rq.json() : [])
          .then(async (lrows) => {
            const lead = Array.isArray(lrows) ? lrows[0] : null;
            if (!lead?.id) return;
            const base = (Array.isArray(lead.emails) ? lead.emails : []).filter(e => e && e.toLowerCase() !== r.retry_email.toLowerCase());
            const newEmails = [r.retry_email, ...base];
            const newSources = { ...(lead.email_sources || {}) };
            if (!newSources[r.retry_email.toLowerCase()]) newSources[r.retry_email.toLowerCase()] = "rescue";
            await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?id=eq.${lead.id}`, {
              method: "PATCH", headers: { ...auth, "Content-Type": "application/json", "Prefer": "return=minimal" },
              body: JSON.stringify({ emails: newEmails, email_sources: newSources }),
            }).catch(() => {});
          }).catch(() => {});
        // Marcar reconciliado para no re-procesar
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_bounce_retries?id=eq.${r.id}`, {
          method: "PATCH", headers: { ...auth, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ status: "reconciled" }),
        }).catch(() => {});
        log(`  ✅ ${r.domain}: Monday actualizado a ${r.retry_email}`);
      } catch (e) { log(`  ⚠️ reconcile ${r.domain}: ${e.message}`); }
    }
  } catch (e) { log(`⚠️ reconcileMondayBounces: ${e.message}`); }
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
      // Maxi 2026-07-15 (F1): +secondary_sent → el 2do email por lead ahora cuenta para el cap diario.
      // Antes solo (sent,reserved) → el agente mandaba ~2x el tope (1 primario + 1 secundario por lead).
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(userEmail)}&action=in.(sent,reserved,secondary_sent)&created_at=gte.${cutoffUtc}&select=id`,
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
  // M2: reglas destiladas del feedback 👍/👎 de este MB (síntesis diaria).
  const rulesBlock = await _getPitchRulesBlock(token, ragOwner);

  const systemMsg = `${adeqStyle}${ragLikes}${ragDislikes}${rulesBlock}

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

  // Maxi 2026-07-13 (auditoría 48h): rechazo DURO SOLO de lo que NUNCA es un contacto real:
  //  a) PLACEHOLDERS/FALSOS que se colaban como "persona" (vorname.name@/firstname.lastname@/
  //     nombre.apellido@ matcheaban firstname.lastname → +70 → se enviaban). PLACEHOLDER_LOCAL y
  //     JUNK_LOCAL_* existían pero NO se llamaban desde rankEmail.
  //  b) WHOIS/gestión de DOMINIO (domainmanagement@axa, dominios.lantik@bizkaia): son del registrar.
  // OJO (feedback user 2026-07-13): sistemas@/system@/IT/gmail NO van acá — en un medio chico pueden
  // ser un contacto real ("sistemas.diariodovale@ no lo veo mal") → van a PENALTY abajo, no a reject.
  if (PLACEHOLDER_LOCAL.test(local) || JUNK_LOCAL_RE.test(local) || JUNK_LOCAL_TOKENS.test(local)) return -1;
  if (/^(domainmanagement|domainadmin|domainname|dominios?)([._-]|$)/i.test(local)) return -1;
  // Maxi 2026-07-14 (auditoría rebotes 11-15/07): "owner@" bare = etiqueta de WHOIS/informer, NO un
  // buzón real → rebotó 4/4 (cnnturk/expansion/arealme/vetogate). El ranking lo tomaba como EXEC (+90)
  // y lo mandaba primero. Un dueño real escribe desde su nombre, no owner@. Reject (source-agnóstico).
  if (/^owner$/.test(local)) return -1;

  let score = 0;
  const cleanSite = (siteDomain || "").replace(/^www\./, "");

  // ── DOMAIN MATCH (peso 0-40) ──
  // Email del MISMO dominio del sitio → señal MUY fuerte (es probablemente real)
  const isFreeWebmail = /^(gmail|yahoo|hotmail|outlook|live|aol|icloud|protonmail|gmx|mail\.ru|yandex|me)\./.test(dom);
  let isCrossDomainCorporate = false;
  if (cleanSite) {
    if (dom === cleanSite) score += 40;
    else if (dom.endsWith("." + cleanSite) || cleanSite.endsWith("." + dom)) score += 35;
    else if (isFreeWebmail) {
      // Webmail cross-domain — penalty intermedio (-15). Es esperado que un
      // contacto B2B use gmail personal. CORRECCIÓN audit #5 (Maxi 2026-06-17):
      // si después detectamos que es EXEC/COMMERCIAL/EDITORIAL, REVERTIMOS el
      // penalty (un founder@gmail NO merece -15 — es una persona real). Por
      // eso aplicamos -15 aquí y abajo lo cancelamos si role matchea.
      score -= 15;
    } else {
      // Cross-domain a OTRO dominio corporativo — penalidad fuerte. Marcamos
      // para revertir parcialmente si es EXEC (founder@otra-empresa puede ser
      // un advisor/board member real).
      score -= 50;
      isCrossDomainCorporate = true;
    }
  }

  // ── ROLE QUALITY (peso -20 a +90) ──
  // Roles comerciales = target ideal (decision-makers de monetización)
  // Roles comerciales/publicidad — cobertura multi-idioma (EN/ES/PT/IT/PL/ID/DE/FR).
  // Stems como prefijo (sin \b) para agarrar "anuncios", "publicidade", "advertising".
  // bd/head requieren boundary (cortos, evitan falsos positivos tipo "headlines").
  // Maxi 2026-07-09: AD_SALES = buzón de VENTA DE PAUTA/PUBLICIDAD. El user eligió "rol
  // comercial/publicidad" como la MEJOR opción (ADEQ vende inventario → este buzón va directo
  // a quien compra/vende espacios). Rankea por ENCIMA de EXEC. Locals específicos de ad-sales
  // (no el "director/manager" genérico, que queda en COMMERCIAL).
  // Regex hoisteado a módulo (AD_SALES_LOCAL) — misma fuente de verdad que _pickTier.
  const AD_SALES   = AD_SALES_LOCAL;
  const COMMERCIAL = /^(?:(?:business|partnership|partner|propaganda|director|gerente|manager|jefe|brand|media)|(?:bd|head)\b)/i;
  const EDITORIAL  = /^(editor|editor-in-chief|chief-editor|redacao|redaccion|redazione|writer|periodista|journalist|prensa|press|reporter|news-?desk)\b/;
  const EXEC       = /^(ceo|cmo|cto|coo|founder|co-?founder|owner|publisher|presidente|president)\b/;

  // ORDEN: chequear generics PRIMERO (antes que "single name"), sino palabras
  // tipo "contato" se cuelan como single-name con score alto en lugar de role.
  // Maxi 2026-06-17 (audit #2): usa GENERIC_LOCAL_RE unificada.
  const IS_GENERIC = GENERIC_LOCAL_RE;

  let matchedRole = "";
  if (AD_SALES.test(local))        { score += 95; matchedRole = "AD_SALES"; }    // publicidad@/comercial@/ads@ = target ideal ADEQ
  else if (EXEC.test(local))       { score += 90; matchedRole = "EXEC"; }       // CEO/founder = jackpot
  else if (COMMERCIAL.test(local)) { score += 80; matchedRole = "COMMERCIAL"; }
  else if (EDITORIAL.test(local))  { score += 60; matchedRole = "EDITORIAL"; }
  // Pattern firstname.lastname (juan.perez@x.com) = persona real
  else if (/^[a-z]{2,}[._-][a-z]{2,}$/.test(local)) { score += 70; matchedRole = "PERSON"; }
  // Pattern firstinitial+lastname (jperez@x.com, mgarcia@x.com) = común corp
  else if (/^[a-z][a-z]{4,14}$/.test(local) && local.length >= 5 && /[aeiou]/.test(local) && !IS_GENERIC.test(local)) { score += 55; matchedRole = "PERSON_LIKELY"; }
  // Generics — OK pero baja conversión. Cobertura multi-idioma (PT/IT/FR/DE/ES).
  // CHEQUEADO ANTES que single-name para que "contato" no se cuele como persona.
  else if (IS_GENERIC.test(local)) score += 15;
  // Single name (juan@x.com) — could be person or generic
  else if (/^[a-z]{3,12}$/.test(local) && /[aeiou]/.test(local)) { score += 30; matchedRole = "SINGLE_NAME"; }

  // Maxi 2026-06-17 (audit #5): si el local-part identificó una PERSONA REAL
  // (EXEC/COMMERCIAL/EDITORIAL/PERSON), REVERTIR el penalty de webmail
  // cross-domain (era -15). Un founder@gmail SÍ es una persona válida.
  // Para cross-corporate (-50): revertir parcialmente (-25) si es EXEC, ya
  // que founder@otra-empresa puede ser advisor/board (real pero menos directo).
  if (matchedRole && (matchedRole === "AD_SALES" || matchedRole === "EXEC" || matchedRole === "COMMERCIAL" || matchedRole === "EDITORIAL" || matchedRole === "PERSON")) {
    if (isFreeWebmail && cleanSite && dom !== cleanSite) {
      score += 15; // cancela el -15 inicial
    }
    if (isCrossDomainCorporate && matchedRole === "EXEC") {
      score += 25; // revierte la mitad del -50
    }
  }

  // ── CATEGORY-ROLE MATCH (peso 0-25) ──
  // Si el sitio es "sports" y el email es marketing/comercial → bonus extra
  // (un MB humano sabe que sports + comercial es golden)
  const cat = (leadCategory || "").toLowerCase();
  if (CATEGORY_TARGET_ROLES[cat] && CATEGORY_TARGET_ROLES[cat].test(local)) {
    score += 25;
  }

  // ── PENALTIES ──
  // Maxi 2026-07-01: roles LEGALES/compliance (datenschutz@/legal@/privacy@/dpo@/gdpr@/
  // dmca@/copyright@/abuse@) — obligatorios por ley (sobre todo en /impressum de sitios DE),
  // NO son comprador y rinden poquísimo. Los dejamos ABAJO de un info@/contact@ normal.
  if (/^(datenschutz|legal|privacy|privacidad|gdpr|dpo|dsb|dmca|copyright|compliance|abuse|recht)/.test(local)) score -= 30;
  // Maxi 2026-07-13 (auditoría): departamentos que NO compran pauta (seguridad/casting/quejas/
  // reclamos/RRHH/soporte). No se descartan del todo (por si es el único contacto), pero van bien abajo.
  if (/^(seguridad|seguranca|security|sicherheit|casting|complaints?|reclam|quejas|reclamacoes|helpdesk|helpline|support.?tech|soporte.?tecnico|suporte.?tecnico)/.test(local)) score -= 45;
  // Roles TÉCNICOS/IT/operaciones/red — rara vez compran pauta, pero en un medio chico pueden ser el
  // ÚNICO contacto (feedback user 2026-07-13: "sistemas.diariodovale@ no lo veo mal") → penalty fuerte,
  // NO hard-reject: pierden contra cualquier otro candidato pero sobreviven como último recurso.
  if (/^(sys|sysadmin|systems?|sistemas?|edv|betrieb|technik|teknik|informatica|infra|infraestructura|infrastructure|netmanage|net-manage)([._-]|$)/.test(local)
      || /^it[._-](einkauf|support|admin|team|abteilung|dept|helpdesk|service)/.test(local)) score -= 55;
  // Placeholders de CMS (user01, user02, usuario3, guest) — no son personas.
  if (/^(user|usuario|guest|nobody|admin)\d*$/.test(local)) score -= 60;
  // Maxi 2026-06-17 (audit #10): penalty solo si dígitos están AL INICIO
  // (ej. "1234email@", "0001foo@") o si parecen un hash sin vocales (ya
  // descartado arriba con /^[a-z0-9]{8,}$/). NO penalizar mid-string
  // (ej. "sales2024@", "team2@") que son patrones legítimos.
  if (/^\d{3,}/.test(local)) score -= 40;
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
  const bounced = await loadBouncedEmails();   // reusa cache existente (TTL 5min)
  const out = [];
  for (const e of emails) {
    if (!e || typeof e !== "string" || !e.includes("@")) continue;
    const lower = e.toLowerCase().trim();
    if (bounced.has(lower)) continue;   // ya rebotó — no re-agregar
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
  // Sanitización final del destinatario: aunque venga del review_queue, de un
  // reintento o de un email viejo/manual, acá lo limpiamos (saca %, control chars,
  // basura al inicio). Si queda inválido, NO enviamos (evita el caso "%hector@...").
  const rawTo = to; // Maxi 2026-06-17 (audit #6): preservamos el original
  to = _sanitizeEmail(to);
  if (!to || !STRICT_EMAIL_RE.test(to)) {
    log(`  ⛔ sendGmailServer: destinatario inválido tras sanitizar (raw:"${rawTo}" → clean:"${to}") — no se envía`);
    // Log a agent_actions con raw_email + sanitized para debugging.
    if (_workerToken && userEmail) {
      logAgentAction(_workerToken, userEmail, {
        domain: (rawTo || "").split("@")[1] || "_invalid_",
        action: "skipped",
        reason: "invalid_recipient_after_sanitize",
        details: { raw_email: rawTo, sanitized: to, agent_action_id: agentActionId },
      }).catch(() => {});
    }
    return { ok: false, error: "invalid_recipient" };
  }
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
const MONDAY_COL_FU1     = "fecha2";          // Fecha FU1 (today + 5) — Maxi 2026-07-15: era "fecha2_8" (id inexistente en board 1420268379 → "This column ID doesn't exist" en cada dispatch); id real verificado = "fecha2"
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
  // Pausa fin de semana — operativo solo Lun-Vie España
  if (_isWeekendSpain()) {
    log(`🤖 Agent: fin de semana España — sin envíos`);
    return;
  }

  const cfg = await getConfig(token);
  const aCfg = _agentCfg(cfg);
  const monday_api_key_default = cfg.monday_api_key || "";

  // Active hours check — fuera de 9-20 España no manda nada (ni Monday, ni mail)
  if (_isWeekendSpain() || _isOutsideActiveHours(aCfg.activeStart, aCfg.activeEnd)) {
    log(`🤖 Agent: fuera de horario laboral España (lun-vie ${aCfg.activeStart}-${aCfg.activeEnd}, hoy=${_spainWeekday()}, h=${_spainHour()})`);
    return;
  }
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

  // Cap diario POR USUARIO (override del maxPerDay global). JSON en config:
  // agent_max_per_day_by_user = {"sales@adeqmedia.com":15,"dhorovitz@adeqmedia.com":15}
  // Si un user no está en el map, usa aCfg.maxPerDay.
  let perUserCap = {};
  try { perUserCap = JSON.parse(cfg.agent_max_per_day_by_user || "{}"); } catch {}

  for (const userEmail of allFlags.agentUsers) {
    if (!AGENT_WHITELIST.has((userEmail || "").toLowerCase())) {
      log(`🚫 Agent: user ${userEmail} no está en whitelist hardcoded — skip`);
      continue;
    }
    // Bounce scan (Gmail INBOX) — fire-and-forget para que no atrase el ciclo
    scanBouncesForUser(token, userEmail).catch(() => {});
    // Maxi 2026-06-18: scan respuestas REALES (no OOO) para nutrir
    // toolbar_response_tracking + ranking dinámico por source.
    scanRealResponsesForUser(token, userEmail).catch(() => {});
    // Auto-reply scan (out-of-office, ticket systems, etc.) — también dispara retry
    scanAutoRepliesForUser(token, userEmail).catch(() => {});
    // Kill switch check
    if (await checkAgentKillSwitch(token, userEmail, aCfg)) continue;
    // Daily cap — por usuario (override) o global
    const userMaxPerDay = Number(perUserCap[(userEmail || "").toLowerCase()]) || aCfg.maxPerDay;
    const sentToday = await getAgentDailyCount(token, userEmail);
    if (sentToday >= userMaxPerDay) {
      log(`🤖 Agent ${userEmail}: cap diario ${userMaxPerDay} alcanzado (${sentToday})`);
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
    const remaining = userMaxPerDay - sentToday;
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

    // Maxi 2026-06-18: pool MUCHO más amplio + sin orden fijo (después
    // bucketeamos por GEO para diversidad). Antes traíamos 5x batchSize por
    // created_at DESC → caía siempre en los más nuevos = poca variedad.
    // Ahora pool grande (300) + el bucket-shuffle por GEO se encarga del orden.
    const POOL_SIZE = 300;
    const queueRes = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gte.${aCfg.thresholdTraffic}${geoClause}${categoryClause}&select=id,domain,score,traffic,geo,geos_all,language,category,emails,email_sources,ad_networks,contact_name,contact_phone&limit=${POOL_SIZE}`, // Maxi 2026-07-03 perf: select=* → solo columnas usadas (scoreWebsite + pool loop + per-lead loop). Egress ALTO: POOL_SIZE filas de tabla ancha
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
        // Maxi 2026-06-19: el AGENTE ya NO marca rejected automáticamente. Antes esto
        // borraba leads del pool del MB humano (Prospects quedaba vacío). Ahora solo
        // saltea el ENVÍO automático de ese lead; sigue PENDING para que el MB lo vea
        // y decida. Solo el botón rojo del MB (o blocklist explícito) rechaza.
        log(`  ⏭️ ${c.domain}: agente no envía (${sw.reasons.join(", ")}) — queda pending para el MB`);
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
    // Maxi 2026-06-18: DIVERSIDAD POR GEO obligatoria.
    //   - Bucketize por GEO (no por categoría)
    //   - Si vos seteaste GEOs en Weekly Focus → se respeta (el query ya filtró)
    //   - Sin Weekly Focus → round-robin entre TODOS los GEOs disponibles
    //   - Resultado: el agente NUNCA manda muchos del mismo país en el día
    //
    // Ejemplo: si pool tiene [50 BR, 30 AR, 20 ES, 10 IT], el agente del día
    // sale con 6 BR + 6 AR + 6 ES + 6 IT + 1 más (= 25 emails balanceados).
    const _geoKey = (c) => {
      if (Array.isArray(c.geos_all) && c.geos_all.length) return String(c.geos_all[0] || "?").toUpperCase().slice(0,3);
      return String(c.geo || "?").toUpperCase().slice(0,3);
    };
    const geoBuckets = new Map();
    for (const c of scored) {
      const g = _geoKey(c);
      if (!geoBuckets.has(g)) geoBuckets.set(g, []);
      geoBuckets.get(g).push(c);
    }
    // Shuffle dentro de cada bucket de GEO (Fisher-Yates)
    for (const arr of geoBuckets.values()) {
      for (let i = arr.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
      }
    }
    // Round-robin entre GEOs — 1 de cada país por vuelta hasta vaciar
    const mixed = [];
    const geoKeys = [...geoBuckets.keys()];
    // Orden inicial de GEOs random también para que no caiga siempre BR primero
    for (let i = geoKeys.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [geoKeys[i], geoKeys[j]] = [geoKeys[j], geoKeys[i]];
    }
    let safety = scored.length * 2;
    while (mixed.length < scored.length && safety-- > 0) {
      let progressed = false;
      for (const k of geoKeys) {
        const b = geoBuckets.get(k);
        if (b && b.length) { mixed.push(b.shift()); progressed = true; }
        if (mixed.length >= scored.length) break;
      }
      if (!progressed) break;
    }
    scored.length = 0;
    scored.push(...mixed);
    // Stats por GEO en el log para auditing
    const geoStats = geoKeys.map(k => `${k}:${geoBuckets.get(k)?.length || 0}`).join(",");
    log(`🤖 Agent ${userEmail}: ${scored.length}/${candidatesRaw.length} candidatos · ${geoKeys.length} GEOs (${geoStats}) · round-robin por país`);

    // No filtramos por sendtrack acá — la regla real es:
    // "no mandar si el dominio está EN MONDAY EN ESTADO ACTIVO".
    // Eso lo chequeamos PER LEAD abajo (más fresco que cachear sendtrack).
    const fresh = scored;

    let processed = 0;
    for (const lead of fresh) {
      if (processed >= batchSize) break;
      const domain = lead.domain;
      // Maxi 2026-07-13 (auditoría 48h): re-chequear blocklist EN EL ENVÍO, no solo al importar.
      // applovin.com (adtech, YA en CORPORATE_BLOCKLIST) recibió mail porque entró a review_queue
      // ANTES de blocklistearse y el ciclo no re-chequeaba. Ahora ningún dominio blocklisteado sale.
      const _blockReason = await isDomainBlockedFull(domain, token);
      if (_blockReason) {
        log(`  ⛔ ${domain}: blocklist (${_blockReason}) — no se envía`);
        continue;
      }
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
        // Maxi 2026-07-13 (DECISIÓN DEL USER): NO se agregan más idiomas de template. CUALQUIER idioma
        // fuera de {es,en,it,pt,ar} se envía en INGLÉS. Antes (2026-06-22) los idiomas foráneos con
        // html lang declarado (de/fr/nl/ja/tr/...) se SALTEABAN y quedaban pending; el user prefiere
        // mandar en inglés a no mandar. Los 5 soportados siguen yendo en su idioma (detección arriba).
        if (!SUPPORTED_AGENT_LANGS.has(leadLanguage)) {
          if (leadLanguage && leadLanguage !== "en") {
            log(`  🌐 ${domain} — idioma "${leadLanguage}" sin template → se envía en INGLÉS (decisión user 2026-07-13)`);
          }
          leadLanguage = "en";
        }
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
        //     - findBestApolloEmail (free verified, o unlock 1 credit si traffic ≥ 399K)
        //     - scrapeEmailsForDomain como fallback (gratis)
        // Audit fix 2026-05-13: antes "hasGoodEmail" solo chequeaba garbage filters,
        // entonces "contato@filmelier.com" (no-garbage pero generic role) era
        // considerado bueno → Apollo NO se llamaba → agent mandaba al rol genérico.
        // Ahora chequeamos rankScore real: si el mejor < 50 (no commercial-grade)
        // → fuerza Apollo lookup para tratar de conseguir un personal email.
        // Maxi 2026-06-17 (audit #13): además chequeamos si TODOS los emails
        // son de source "generic" en email_sources — si sí, force enrichment
        // aunque el rank esté arriba de 50 (puede ser falsa señal por bonus
        // de CATEGORY_TARGET_ROLES). Edge case: review_queue tenía info@ y
        // contact@, ambos rankearon 15+bonus=45 vs cutoff 50 OK, pero después
        // un CATEGORY match los empujó a 65 → hasGoodEmail=true → no enriquece.
        emails = emails.filter(e => e && /\@/.test(e) && !GARBAGE_LOCAL.test(e) && !GARBAGE_DOMAIN_PATTERN.test(e));
        const currentBestScore = emails.length > 0
          ? Math.max(...emails.map(e => rankEmail(e, domain, lead.category)))
          : -1;
        // Detectar si TODOS los emails son source genérico (no apollo/informer/scrape persona).
        // Si sí, force enrichment ignorando el currentBestScore.
        const allGeneric = emails.length > 0 && emails.every(e => {
          const src = lead.email_sources?.[e.toLowerCase()];
          const srcStr = typeof src === "string" ? src : (src?.source || "");
          return srcStr === "generic" || srcStr === "" || srcStr === "cache_unknown";
        });
        const hasGoodEmail = currentBestScore >= 50 && !allGeneric;
        if (allGeneric && currentBestScore >= 50) {
          log(`  🔬 ${domain}: rank ${currentBestScore} OK pero TODOS los emails source=generic → force Apollo lookup (audit #13)`);
        }
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
          source: _normSrc(_sourcesMap[e.toLowerCase()]),
          score:  rankEmail(e, domain, lead.category),
        }));
        const ranked = _rankedAll
          .filter(x => x.score >= 0)
          // Maxi 2026-07-14 (auditoría rebotes): informer (WHOIS) + freemail = email del REGISTRANTE
          // del dominio, NO el contacto comercial → nunca sirve y suele rebotar (caso rudnypc@gmail de
          // baladag4.com.br). Un gmail SCRAPEADO del sitio sí puede ser real → esto solo aplica a informer.
          .filter(x => !(x.source === "informer" && /@(gmail|googlemail|hotmail|outlook|live|yahoo|ymail|aol|icloud|protonmail|gmx|yandex)\.|@mail\.ru\b/i.test(x.email)))
          .sort((a, b) => {
            // Maxi 2026-07-09: tier DURO primero (_pickTier): apollo/informer nominal > rol
            // comercial/publicidad scrapeado > persona scrapeada > genérico. Honra la regla del
            // dueño (decision-maker verificado manda) Y la elección del user (Q4: rol comercial).
            const ta = _pickTier(a.email, a.source);
            const tb = _pickTier(b.email, b.source);
            if (ta !== tb) return tb - ta;     // apollo/informer > publicidad@ > persona > genérico
            const sa = SOURCE_RANK[a.source] || 0;
            const sb = SOURCE_RANK[b.source] || 0;
            if (sa !== sb) return sb - sa;     // dentro del tier: ranking dinámico
            return b.score - a.score;          // desempate final: rankEmail
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
        // Maxi 2026-07-09: BACKUP de TODOS los emails detectados (no solo el elegido) → el MB
        // compara en SQL qué habría elegido él vs el agente y saca estadísticas para decidir.
        // Fire-and-forget: NUNCA frena ni rompe el envío. Tabla toolbar_email_picks.
        if (Array.isArray(emails) && emails.length > 0) {
          try {
            const chosenSrc = email ? _normSrc(_sourcesMap[email.toLowerCase()]) : "";
            const candidates = emails.map(e => {
              const s = _normSrc(_sourcesMap[e.toLowerCase()]);
              return { email: e, source: s || "unknown", score: rankEmail(e, domain, lead.category), tier: _pickTier(e, s), bounced: _bouncedSet.has(e.toLowerCase()) };
            }).sort((a, b) => (b.tier - a.tier) || (b.score - a.score));
            fetch(`${SUPABASE_URL}/rest/v1/toolbar_email_picks`, {
              method: "POST",
              headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
              body: JSON.stringify({
                domain, lead_id: lead.id || null, mb_email: userEmail || null, category: lead.category || null,
                chosen_email: email || null, chosen_source: chosenSrc || null,
                chosen_tier: email ? _pickTier(email, chosenSrc) : null,
                n_candidates: candidates.length, candidates,
              }),
            }).catch(() => {});
          } catch {}
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
            // Maxi 2026-07-03 perf: solo importa "¿≥3 fallas hoy?" → limit=3 sin count=exact,
            // evaluado por rows.length (el número exacto no se usa: log y attempt_count son constantes).
            const cntRes = await fetch(
              `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?action=eq.skipped&reason=eq.no_email_after_enrichment&domain=eq.${encodeURIComponent(domain)}&created_at=gte.${cutoffTodaySpain.toISOString()}&select=id&limit=3`,
              { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
            );
            const cntRows = await cntRes.json();
            const failsToday = Array.isArray(cntRows) ? cntRows.length : 0;
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
            email_to: email,                            // FIX 2026-05-19: necesario para aggregateSourcePerformance
            pitch_subject: subject,
            details: { email, source: pickedSource, traffic: leadTraffic, geo: leadGeo, language: lead.language },
          }),
        }).catch(() => null);
        reservedId = (await reserveRes?.json().catch(() => null))?.[0]?.id || null;
        // Maxi 2026-07-15 (F5): si la reserva NO se creó (Supabase caído), NO enviar — un envío sin reserva
        // NO lo cuenta getAgentDailyCount → bypasea el cap. Mejor saltear y reintentar el próximo ciclo.
        if (!reservedId) {
          log(`  ⏭️ ${domain}: no se pudo reservar el slot de envío (Supabase?) — skip para no enviar sin contar`);
          continue;
        }

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

        // Maxi 2026-06-18: re-check bounce JUSTO antes de enviar. Entre el rank
        // inicial y este send pueden haber pasado minutos; otro MB/scan pudo
        // haber marcado este email como bounced. Si rebotó ahora → skip.
        if (isBouncedSync(email)) {
          log(`  🚫 ${domain}: ${email} marcado bounced ENTRE rank y send — abortar envío`);
          if (reservedId) {
            fetch(`${SUPABASE_URL}/rest/v1/toolbar_agent_actions?id=eq.${reservedId}`, {
              method: "PATCH",
              headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
              body: JSON.stringify({ action: "skipped", reason: "bounced_in_window" }),
            }).catch(() => {});
          }
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

        // Maxi 2026-06-18: registrar envío en response_tracking para medir
        // qué TIPO de email convierte mejor (apollo/informer/scrape/social/generic).
        // responded_at queda NULL hasta que scanRealResponsesForUser detecte un reply real.
        fetch(`${SUPABASE_URL}/rest/v1/toolbar_response_tracking`, {
          method: "POST",
          headers: {
            "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
            "Content-Type": "application/json", "Prefer": "return=minimal,resolution=merge-duplicates",
          },
          body: JSON.stringify({
            agent_action_id: reservedId || null,
            mb_email:        userEmail.toLowerCase(),
            domain,
            email_sent_to:   email,
            source:          pickedSource || "unknown",
            geo:             leadGeo || "",
            category:        lead.category || "",
            sent_at:         new Date().toISOString(),
          }),
        }).catch(() => {});

        // Maxi 2026-06-18: AGENTE TAMBIÉN manda al 2do mejor email del lead
        // (si existe y tiene rank decente). Ganamos el que responda primero.
        // El bounce handler después auto-promueve Monday al que respondió.
        // Reglas: 2do email debe tener score >= 40, NO bounced, diferente del 1ro.
        try {
          const secondCandidate = ranked.find(r =>
            r.email && r.email.toLowerCase() !== email.toLowerCase() &&
            r.score >= 40 && !isBouncedSync(r.email)
          );
          if (secondCandidate) {
            log(`  ➕ ${domain}: agente envía AL 2DO email ${secondCandidate.email} (score=${secondCandidate.score}, source=${secondCandidate.source})`);
            await sendGmailServer(token, userEmail, { to: secondCandidate.email, subject, body: pitch.body, agentActionId: null });
            // Registrar en response_tracking con source del 2do
            fetch(`${SUPABASE_URL}/rest/v1/toolbar_response_tracking`, {
              method: "POST",
              headers: {
                "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
                "Content-Type": "application/json", "Prefer": "return=minimal",
              },
              body: JSON.stringify({
                mb_email:      userEmail.toLowerCase(),
                domain,
                email_sent_to: secondCandidate.email,
                source:        secondCandidate.source || "secondary",
                geo:           leadGeo || "",
                category:      lead.category || "",
                sent_at:       new Date().toISOString(),
              }),
            }).catch(() => {});
            logAgentAction(token, userEmail, {
              domain, action: "secondary_sent",
              reason: "agent_2nd_email_parallel",
              details: { primary: email, secondary: secondCandidate.email, score: secondCandidate.score, source: secondCandidate.source },
            }).catch(() => {});
          }
        } catch (e) { log(`  ⚠️ agente 2do email ${domain}: ${e.message}`); }

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

// Maxi 2026-07-08: PRIORIDAD DURA de fuente. Un contacto NOMINAL (apollo/informer =
// decision-maker real, o manual = elección explícita del MB) SIEMPRE se elige antes que un
// GENÉRICO (info@/contact@), sin importar lo que diga el ranking dinámico aprendido. El 0%
// de respuesta observado era por IDIOMA equivocado (Fix #1), NO por la fuente. Regla del
// dueño: si informer/apollo encontró contacto, usarlo sí o sí por encima de genéricos.
// El ranking dinámico (SOURCE_RANK) sigue operando como DESEMPATE dentro del mismo tier —
// antes podía relegar un informer detrás de un info@ con mejor open-rate observado.
//   tier 2 = nominal decision-maker (apollo/informer/manual)
//   tier 1 = otras fuentes con persona (scrape, redes sociales)
//   tier 0 = genérico (info@/contact@) y desconocido
function _sourceHardTier(source) {
  const s = (source || "").toLowerCase();
  if (s === "apollo" || s === "informer" || s === "manual") return 2;
  if (s === "generic" || s === "") return 0;
  return 1;
}

// Maxi 2026-07-09: tier de SELECCIÓN del email final. Combina la confiabilidad de la SOURCE con
// el ROL comercial del local-part. El user (Q4) eligió "rol comercial/publicidad" como la mejor
// opción, PERO el decision-maker verificado por Apollo sigue mandando (regla del dueño):
//   4 = apollo/manual nominal (decision-maker verificado)
//   3 = publicidad@/comercial@/ventas@ (rol de venta de pauta — buzón ideal ADEQ)
//   2 = otra persona/rol scrapeado (source=scrape)
//   1 = informer NO-comercial (WHOIS/registrar — baja calidad, ver auditoría 2026-07-13)
//   0 = genérico (info@/contacto@)
// Así el orden queda: apollo > publicidad@ > persona scrapeada > informer > genérico.
function _pickTier(email, source) {
  const src = String(source || "").toLowerCase();
  const local = String(email || "").toLowerCase().split("@")[0];
  // Maxi 2026-07-13 (auditoría 48h): informer (website.informer/WHOIS) NO es top-tier — da contactos
  // técnicos/registrar (domainmanagement@, net-manage@…) de baja calidad. Se baja al nivel más bajo
  // salvo que el local sea un rol comercial explícito. apollo/manual siguen tier 4.
  if (src === "informer") return AD_SALES_LOCAL.test(local) ? 3 : 1;
  const st = _sourceHardTier(source);
  if (st === 2) return 4;                                          // apollo/manual nominal
  if (AD_SALES_LOCAL.test(local)) return 3;                        // rol comercial/publicidad
  if (st === 1) return 2;                                          // persona scrapeada
  return 0;                                                        // genérico
}
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
// Maxi 2026-06-18: ranking por TASA DE RESPUESTA REAL.
// Lee toolbar_response_tracking de últimos N días y computa:
//   conversion_rate(source) = (count where response_type='real') / count_sent
// Si hay sample suficiente (≥10 envíos por source en ≥2 sources), devuelve rank.
// Sino devuelve null y el caller cae al ranking viejo (open_rate-based).
async function _fetchResponseRateRank(token, mbEmail) {
  try {
    const mb = (mbEmail || "").toLowerCase();
    const since = new Date(Date.now() - SOURCE_PERF_WINDOW_DAYS * 86400_000).toISOString();
    const filter = mb ? `&mb_email=eq.${encodeURIComponent(mb)}` : "";
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_response_tracking?sent_at=gte.${since}${filter}&select=source,response_type&limit=5000`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json().catch(() => []);
    if (!Array.isArray(rows) || rows.length === 0) return null;
    // Contadores por source
    const counts = new Map(); // source → { sent, real }
    for (const r of rows) {
      const src = (r.source || "unknown").toLowerCase();
      const c = counts.get(src) || { sent: 0, real: 0 };
      c.sent++;
      if (r.response_type === "real") c.real++;
      counts.set(src, c);
    }
    // Filtrar sources con ≥10 envíos (sample mínimo)
    const qualified = [...counts.entries()].filter(([_, c]) => c.sent >= 10);
    if (qualified.length < 2) return null; // necesitamos ≥2 para rankear
    // Score = response_rate (0-1) + smoothing leve
    const scores = {};
    for (const [src, c] of qualified) {
      scores[src] = c.sent > 0 ? (c.real / c.sent) : 0;
    }
    return _scoresToRank(scores);
  } catch { return null; }
}

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
  if (Math.random() < SOURCE_PERF_EPSILON) {
    // Maxi 2026-06-17 (audit #8): log explícito cuando cae a hardcoded por
    // exploration ε-greedy o por no-data. Antes era silencioso → admin no sabía
    // si la ranking dinámica realmente estaba activa.
    return SOURCE_RANK_DEFAULT;
  }

  const key = (mbEmail || "_global").toLowerCase();
  const cached = _sourceRankCache.get(key);
  if (cached && (Date.now() - cached.ts) < SOURCE_PERF_CACHE_TTL) return cached.rank;

  // Maxi 2026-06-18: PRIORIDAD a respuestas REALES (toolbar_response_tracking).
  // Fallback al ranking viejo (open_rate-based) y por último al default hardcoded.
  let rank = await _fetchResponseRateRank(token, key);
  let source = "response_rate";
  if (!rank) {
    rank = await _fetchDynamicSourceRank(token, key);
    source = "open_rate";
  }
  if (!rank) {
    rank = SOURCE_RANK_DEFAULT;
    source = "hardcoded_default";
  }
  log(`  ℹ️ source ranking ${key}: usando ${source} → ${Object.entries(rank).filter(([k])=>k).map(([k,v])=>`${k}:${v.toFixed(2)}`).join(", ")}`);
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
      brows.forEach(b => { if (typeof b.original_email === "string") bouncedEmails.add(b.original_email.toLowerCase()); });
    }

    // 4. Agregar por (mb, source).
    const agg = new Map(); // key "mb|source" → {sent, opens, bounces}
    const bump = (mb, src, field) => {
      const k = `${mb}|${src}`;
      if (!agg.has(k)) agg.set(k, { sent: 0, opens: 0, bounces: 0, mb, src });
      agg.get(k)[field]++;
    };
    for (const s of sends) {
      // Maxi 2026-07-15: guardas de tipo. details es JSONB → source podía venir como objeto/
      // array/número en algún row viejo → `(x||"").toLowerCase()` tiraba "toLowerCase is not a
      // function" y abortaba TODO el aggregate (visto en logs Railway). Ahora no-string → "".
      const mb  = (typeof s.user_email === "string" ? s.user_email : "").toLowerCase();
      const src = (typeof s.details?.source === "string" ? s.details.source : "").toLowerCase();
      if (!mb || !src) continue;     // sin atribución → skip
      const isOpen   = opensByActionId.has(s.id);
      const isBounce = (typeof s.email_to === "string") ? bouncedEmails.has(s.email_to.toLowerCase()) : false;
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
      return false;   // Maxi 2026-07-15 (F8): señalar fallo → no marcar "corrido hoy"
    }

    // 6. Invalidar cache para que el próximo getDynamicSourceRank lea fresh.
    _sourceRankCache.clear();
    log(`  ✅ source perf upsert OK: ${rows.length} filas (${sends.length} sends procesados)`);
    return true;
  } catch (e) {
    log(`⚠️ aggregateSourcePerformance: ${e.message}`);
    return false;
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
    const _ok = await aggregateSourcePerformance(token);
    // Maxi 2026-07-15 (F8): solo marcar "corrido hoy" si NO falló (false). Antes se marcaba siempre →
    // un fallo (Supabase 5xx) quedaba como "hecho" y el ranking dinámico corría sobre data vieja hasta mañana.
    if (_ok !== false) await setConfigValue(token, "last_source_perf_run", todayMadrid).catch(() => {});
  } catch (e) { log(`⚠️ maybeRunSourcePerformanceAggregate: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════
// M2 (Maxi 2026-06-19): SÍNTESIS DE FEEDBACK DEL EMAIL IA.
// Antes el generador solo recuperaba ejemplos sueltos (👍/👎) por similitud.
// Ahora, 1× por día por MB, Claude (Haiku, barato) LEE los emails likeados y
// rechazados (+ el motivo del 👎) y los DESTILA en reglas concretas: "HACÉ ..."
// y "EVITÁ ...". Esas reglas se inyectan en CADA generación (worker + popup),
// así el sistema "aprende qué tener y qué no tener en cuenta". Se guarda en
// toolbar_config key `pitch_rules_<email>`.
// ════════════════════════════════════════════════════════════════
async function aggregatePitchFeedbackRules(token, userEmail) {
  const emailLc = (userEmail || "").toLowerCase();
  if (!emailLc) return;
  const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  const sinceISO = new Date(Date.now() - 60 * 86400_000).toISOString();
  const pull = async (action) => {
    try {
      const r = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_pitch_feedback?user_email=eq.${encodeURIComponent(emailLc)}&action=eq.${action}&created_at=gte.${sinceISO}&select=pitch_body,context,created_at&order=created_at.desc&limit=15`,
        { headers: auth }
      );
      return r.ok ? (await r.json()) : [];
    } catch { return []; }
  };
  const [liked, disliked] = await Promise.all([pull("liked"), pull("disliked")]);
  // Necesitamos señal mínima para que valga la pena destilar.
  if ((liked.length + disliked.length) < 3) return;

  const fmt = (arr) => arr.map((r, i) => {
    // El motivo del 👎 viene embebido en context como "USER_FEEDBACK_REASON: ..."
    const reason = (r.context || "").match(/USER_FEEDBACK_REASON:\s*([^\n]+)/i)?.[1] || "";
    const body = (r.pitch_body || "").substring(0, 400);
    return `${i + 1}. "${body}"${reason ? `\n   [motivo del MB: ${reason.trim()}]` : ""}`;
  }).join("\n");

  const userMsg = `Estos son emails de prospección que un media buyer marcó como BUENOS (👍) y MALOS (👎). Tu tarea: detectar PATRONES recurrentes y destilarlos en reglas cortas y accionables para que la IA genere mejores emails para ESTE media buyer.

EMAILS BUENOS (👍):
${liked.length ? fmt(liked) : "(ninguno aún)"}

EMAILS MALOS (👎):
${disliked.length ? fmt(disliked) : "(ninguno aún)"}

Devolveme SOLO JSON:
{ "do": ["regla corta 1", "..."], "avoid": ["regla corta 1", "..."] }
- Máximo 6 reglas en cada lista, en español, concretas (ej: "evitar palabras como 'sinergia'", "abrir mencionando el dominio", "no más de 2 párrafos").
- Basate en patrones REALES de los ejemplos, no inventes. Si no hay señal suficiente para una lista, devolvela vacía.`;

  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/api-proxy`, {
      method: "POST",
      headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        provider: "anthropic", path: "/v1/messages", method: "POST",
        body: {
          model: "claude-haiku-4-5", max_tokens: 600,
          system: "Sos un analista de copywriting B2B. Destilás feedback en reglas accionables. Devolvés SOLO JSON válido.",
          messages: [{ role: "user", content: userMsg }],
        },
      }),
      signal: AbortSignal.timeout(20000),
    });
    if (!res.ok) return;
    const data = await res.json();
    const text = data?.content?.[0]?.text || "";
    const m = text.match(/\{[\s\S]*\}/);
    if (!m) return;
    const parsed = JSON.parse(m[0]);
    const doRules    = Array.isArray(parsed.do)    ? parsed.do.filter(Boolean).slice(0, 6)    : [];
    const avoidRules = Array.isArray(parsed.avoid) ? parsed.avoid.filter(Boolean).slice(0, 6) : [];
    if (doRules.length === 0 && avoidRules.length === 0) return;
    const todayMadrid = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
    await setConfigValue(token, `pitch_rules_${emailLc}`, JSON.stringify({ do: doRules, avoid: avoidRules, at: todayMadrid, n: liked.length + disliked.length })).catch(() => {});
    log(`🧠 pitch rules synth ${emailLc}: ${doRules.length} HACÉ + ${avoidRules.length} EVITÁ (de ${liked.length}👍/${disliked.length}👎)`);
  } catch (e) { log(`⚠️ aggregatePitchFeedbackRules ${emailLc}: ${e.message}`); }
}

// Guard: corre 1× por día (hora Madrid) para cada MB habilitado.
async function maybeRunPitchRulesSynthesis(token) {
  try {
    const cfg = await getConfig(token);
    const todayMadrid = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
    if (cfg.last_pitch_rules_run === todayMadrid) return;
    let users = [];
    try { users = JSON.parse(cfg.agent_enabled_users || "[]"); } catch {}
    if (!users.length) return;
    for (const u of users) await aggregatePitchFeedbackRules(token, u);
    await setConfigValue(token, "last_pitch_rules_run", todayMadrid).catch(() => {});
  } catch (e) { log(`⚠️ maybeRunPitchRulesSynthesis: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════
// SÍNTESIS DE REGLAS DE BASURA (Maxi 2026-06-19) — el botón ROJO enseña.
// 1× por día: Haiku lee los MOTIVOS de rechazo del MB (por CONTENIDO, ignora GEO)
// y los destila en reglas cortas. classifyPublisher las usa para descartar futuros
// similares → Prospects se mantiene limpio y va aprendiendo solo.
// ════════════════════════════════════════════════════════════════
async function aggregateProspectTrashRules(token) {
  const auth = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };
  const since = new Date(Date.now() - 90 * 86400_000).toISOString();
  let rows = [];
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_autopilot_feedback?action=eq.disliked&reason=not.is.null&created_at=gte.${since}&select=domain,category,reason&order=created_at.desc&limit=100`, { headers: auth });
    if (r.ok) rows = await r.json();
  } catch {}
  rows = (rows || []).filter(x => (x.reason || "").trim().length > 2);
  if (rows.length < 3) return; // poca señal todavía
  const list = rows.slice(0, 60).map((x, i) => `${i + 1}. ${x.domain || "?"} [${x.category || "?"}] — "${(x.reason || "").trim().slice(0, 160)}"`).join("\n");
  const userMsg = `Estos son sitios que el media buyer RECHAZÓ de su lista de prospects de PUBLICIDAD, con el motivo. Detectá PATRONES de CONTENIDO (qué TIPO de sitios NO sirven) y destilá reglas cortas y accionables para descartar futuros similares. IGNORÁ el GEO/país — lo que importa es el CONTENIDO/tipo de sitio.

RECHAZADOS:
${list}

Devolveme SOLO JSON: { "rules": ["regla corta 1", "regla corta 2", ...] }
- Máximo 8 reglas, en español, concretas y por CONTENIDO (ej: "e-commerce / tiendas de venta", "blog corporativo de una empresa de servicios", "portal de gobierno o municipio", "sitio de cursos/universidad", "directorio o agregador sin contenido propio", "landing de un producto/SaaS").
- Basate en patrones REALES de los motivos, no inventes.`;
  try {
    const res = await fetch(`${SUPABASE_URL}/functions/v1/api-proxy`, {
      method: "POST",
      headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        provider: "anthropic", path: "/v1/messages", method: "POST",
        body: { model: "claude-haiku-4-5", max_tokens: 500, system: "Sos un analista que destila feedback en reglas de filtrado accionables por contenido. Devolvés SOLO JSON válido.", messages: [{ role: "user", content: userMsg }] },
      }),
      signal: AbortSignal.timeout(20000),
    });
    if (!res.ok) return;
    const data = await res.json();
    const m = (data?.content?.[0]?.text || "").match(/\{[\s\S]*\}/);
    if (!m) return;
    const parsed = JSON.parse(m[0]);
    const rules = Array.isArray(parsed.rules) ? parsed.rules.filter(Boolean).slice(0, 8) : [];
    if (!rules.length) return;
    const text = rules.map(r => `- ${r}`).join("\n");
    await setConfigValue(token, "prospect_trash_rules", text).catch(() => {});
    _trashCtxCache = { at: 0, ctx: null }; // invalidar cache → toma las reglas nuevas ya
    log(`🧠 prospect trash rules synth: ${rules.length} reglas (de ${rows.length} rechazos con motivo)`);
  } catch (e) { log(`⚠️ aggregateProspectTrashRules: ${e.message}`); }
}

async function maybeRunProspectTrashSynthesis(token) {
  try {
    const cfg = await getConfig(token);
    const todayMadrid = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
    if (cfg.last_prospect_trash_run === todayMadrid) return;
    await aggregateProspectTrashRules(token);
    await setConfigValue(token, "last_prospect_trash_run", todayMadrid).catch(() => {});
  } catch (e) { log(`⚠️ maybeRunProspectTrashSynthesis: ${e.message}`); }
}

// Lee las reglas destiladas del MB y las formatea como bloque para el prompt.
// Cache 10min en memoria para no pegarle a config en cada generación.
const _pitchRulesCache = new Map();
async function _getPitchRulesBlock(token, userEmail) {
  const emailLc = (userEmail || "").toLowerCase();
  if (!emailLc) return "";
  const cached = _pitchRulesCache.get(emailLc);
  if (cached && (Date.now() - cached.at) < 600_000) return cached.block;
  let block = "";
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.${encodeURIComponent(`pitch_rules_${emailLc}`)}&select=value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (r.ok) {
      const rows = await r.json();
      const parsed = rows?.[0]?.value ? JSON.parse(rows[0].value) : null;
      if (parsed && (parsed.do?.length || parsed.avoid?.length)) {
        const doStr    = (parsed.do || []).map(x => `- ${x}`).join("\n");
        const avoidStr = (parsed.avoid || []).map(x => `- ${x}`).join("\n");
        block = `\n\n# REGLAS APRENDIDAS DEL FEEDBACK DE ESTE MB (cumplir SIEMPRE):${doStr ? `\nHACÉ:\n${doStr}` : ""}${avoidStr ? `\nEVITÁ:\n${avoidStr}` : ""}`;
      }
    }
  } catch {}
  _pitchRulesCache.set(emailLc, { block, at: Date.now() });
  return block;
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
// Umbrales ABSOLUTOS de salud de envío — funcionan con 1 solo MB (scanWeeklyRates
// necesita ≥2 MBs para comparar y no sirve acá). Baseline actual ~82% open / 4.8% bounce.
const DELIV_OPEN_FLOOR  = 0.50;              // alertar si open rate global < 50%
const DELIV_BOUNCE_CEIL = 0.08;              // alertar si bounce rate global > 8%
const DELIV_MIN_SENT    = 30;                // sample mínimo para tener señal

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
  const madridHour = parseInt(now.toLocaleString("en-US", { timeZone: "Europe/Madrid", hour: "2-digit", hour12: false }).replace(/\D/g, "").slice(0, 2), 10);
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
// FIX 2026-05-19: throttle in-memory para evitar correr 60+ veces/hora.
// El dedup_key del insert ya evita duplicados a nivel DB, pero igual mandar
// 60 requests con 409 conflict es ruido innecesario en logs Supabase.
let _lastSysFailScanAt = 0;
async function scanSystemFailures(token) {
  if (Date.now() - _lastSysFailScanAt < 15 * 60 * 1000) return; // throttle 15min
  _lastSysFailScanAt = Date.now();
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
// Chequeo ABSOLUTO de salud de envío (1×/día) — avisa si la apertura cae o los
// rebotes suben, sin depender de comparar contra otros MBs (clave con 1 solo MB).
async function scanDeliverabilityHealth(token) {
  try {
    const cfg = await getConfig(token);
    const todayMadrid = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
    if ((cfg.last_deliverability_check || "") === todayMadrid) return;

    // user 2026-05-29: PERSONAL por MB. Antes mandaba al _admin (compartido) y
    // ningún MB podía descartarla. Ahora itera por cada MB con datos en source_performance.
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_source_performance?window_days=eq.30&mb_email=neq._global&select=mb_email,sent,opens,bounces`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    if (!res.ok) return;
    const rows = await res.json();
    await setConfigValue(token, "last_deliverability_check", todayMadrid).catch(() => {});
    if (!Array.isArray(rows) || rows.length === 0) return;

    // Agrupar por MB (sumar todas las sources)
    const byMb = new Map();
    for (const r of rows) {
      const mb = (r.mb_email || "").toLowerCase();
      if (!mb || mb === "_global") continue;
      if (!byMb.has(mb)) byMb.set(mb, { sent: 0, opens: 0, bounces: 0 });
      const t = byMb.get(mb);
      t.sent    += r.sent    || 0;
      t.opens   += r.opens   || 0;
      t.bounces += r.bounces || 0;
    }

    for (const [mb, tot] of byMb.entries()) {
      if (tot.sent < DELIV_MIN_SENT) continue;
      const openRate   = tot.opens   / tot.sent;
      const bounceRate = tot.bounces / tot.sent;
      const problems = [];
      if (openRate   < DELIV_OPEN_FLOOR)   problems.push(`apertura ${(openRate   * 100).toFixed(0)}% (piso ${DELIV_OPEN_FLOOR  * 100}%)`);
      if (bounceRate > DELIV_BOUNCE_CEIL)  problems.push(`rebotes ${(bounceRate * 100).toFixed(1)}% (techo ${DELIV_BOUNCE_CEIL * 100}%)`);
      if (!problems.length) continue;
      await createNotificationWorker(token, {
        mb_email: mb,
        type: "deliverability_health",
        severity: "error",
        title: "⚠️ Salud de envío en baja",
        body: `Últimos 30d (${tot.sent} envíos): ${problems.join(" · ")}. Revisar la calidad de los emails y la reputación del remitente.`,
        metadata: { sent: tot.sent, open_rate: +openRate.toFixed(3), bounce_rate: +bounceRate.toFixed(3) },
        dedup_key: `deliverability-${mb}-${todayMadrid}`,
      });
      log(`🚨 Deliverability alert ${mb}: ${problems.join(", ")} (sent=${tot.sent})`);
    }
  } catch (e) { log(`⚠️ scanDeliverabilityHealth: ${e.message}`); }
}

// Maxi 2026-06-18: SIMPLIFICAR notificaciones — solo dejamos:
//   1. Daily digest por MB (resumen de AYER + tip simple)
//   2. Deliverability health (alerta si bounce>8% o open<50% en 30d)
// Eliminados: scanLowProspecting, scanWeeklyRates, scanSourceInsight,
// scanSystemFailures (decisión Maxi: "no sirven, son noise")
async function runNotificationScanners(token) {
  await generateDailyDigestAllMBs(token).catch(e => log(`⚠️ dailyDigest: ${e.message}`));
  await scanDeliverabilityHealth(token).catch(e => log(`⚠️ scanDeliverability: ${e.message}`));
}

// ════════════════════════════════════════════════════════════════
// DAILY DIGEST — resumen diario por MB (Maxi 2026-06-18)
// Maxi: "Solo enviar notificaciones que indiquen por dia cuantos
// emails/prospectos realizo el media buyer".
// Corre 1 vez por día (guard via last_digest_run = YYYY-MM-DD Madrid).
// Para cada MB activo (mgargiulo / dhorovitz / sales) genera 1 notif personal
// con stats de AYER + comparación vs media semanal + tip.
// ════════════════════════════════════════════════════════════════
async function generateDailyDigestAllMBs(token) {
  const todayMadrid = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
  const cfg = await getConfig(token);
  if ((cfg.last_digest_run || "") === todayMadrid) return;

  const MBS = [
    { email: "mgargiulo@adeqmedia.com", name: "Maxi" },
    { email: "dhorovitz@adeqmedia.com", name: "Diego" },
    { email: "sales@adeqmedia.com",     name: "Agus" },
  ];
  for (const mb of MBS) {
    try { await generateDailyDigestForMB(token, mb.email, mb.name); }
    catch (e) { log(`⚠️ digest ${mb.email}: ${e.message}`); }
  }
  await setConfigValue(token, "last_digest_run", todayMadrid).catch(() => {});
  log(`📬 daily digests generados para ${MBS.length} MBs (${todayMadrid})`);
}

// Mapa país → continente (ISO 2-letter codes). Usado por el digest para
// agrupar prospects por región. Solo los más comunes — el resto cae en "Otros".
const COUNTRY_TO_CONTINENT = {
  // América (latam + norte)
  AR:"América", BR:"América", MX:"América", CO:"América", CL:"América", PE:"América",
  UY:"América", EC:"América", VE:"América", PY:"América", BO:"América", DO:"América",
  PA:"América", GT:"América", HN:"América", SV:"América", NI:"América", CR:"América",
  CU:"América", PR:"América", US:"América", CA:"América",
  // Europa
  ES:"Europa", PT:"Europa", IT:"Europa", FR:"Europa", DE:"Europa", GB:"Europa",
  NL:"Europa", BE:"Europa", CH:"Europa", AT:"Europa", IE:"Europa", DK:"Europa",
  SE:"Europa", NO:"Europa", FI:"Europa", PL:"Europa", CZ:"Europa", HU:"Europa",
  RO:"Europa", GR:"Europa", BG:"Europa", HR:"Europa", SK:"Europa", RU:"Europa", UA:"Europa",
  // Asia
  IN:"Asia", CN:"Asia", JP:"Asia", KR:"Asia", VN:"Asia", TH:"Asia", ID:"Asia",
  PH:"Asia", MY:"Asia", SG:"Asia", PK:"Asia", BD:"Asia", LK:"Asia", TW:"Asia", HK:"Asia",
  // África / Medio Oriente
  EG:"África", MA:"África", DZ:"África", NG:"África", KE:"África", ZA:"África",
  SA:"M.Oriente", AE:"M.Oriente", IL:"M.Oriente", TR:"M.Oriente",
  // Oceanía
  AU:"Oceanía", NZ:"Oceanía",
};
function _countryToContinent(geoStr) {
  if (!geoStr) return "Sin GEO";
  const s = String(geoStr).trim();
  // ISO 2-letter directo
  const upper = s.toUpperCase().slice(0,2);
  if (COUNTRY_TO_CONTINENT[upper]) return COUNTRY_TO_CONTINENT[upper];
  // NAME mapping (mismo que worker)
  const NAME_TO_ISO = {
    "argentina":"AR","brazil":"BR","brasil":"BR","mexico":"MX","colombia":"CO",
    "chile":"CL","peru":"PE","uruguay":"UY","ecuador":"EC","venezuela":"VE",
    "paraguay":"PY","bolivia":"BO","spain":"ES","portugal":"PT","italy":"IT",
    "france":"FR","germany":"DE","united kingdom":"GB","united states":"US",
    "canada":"CA","australia":"AU","new zealand":"NZ","india":"IN","china":"CN",
    "japan":"JP","south korea":"KR","vietnam":"VN","thailand":"TH","indonesia":"ID",
    "philippines":"PH","russia":"RU","ukraine":"UA","turkey":"TR","israel":"IL",
    "saudi arabia":"SA","egypt":"EG","morocco":"MA","nigeria":"NG","south africa":"ZA",
  };
  const iso = NAME_TO_ISO[s.toLowerCase()];
  return iso && COUNTRY_TO_CONTINENT[iso] ? COUNTRY_TO_CONTINENT[iso] : "Otros";
}

async function generateDailyDigestForMB(token, mbEmail, mbName) {
  // Ventana = AYER en horario Madrid (mismo que usamos para slot del agente)
  const now = new Date();
  const todayMadridStr = now.toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
  const yesterday = new Date(now.getTime() - 86400_000);
  const ydayMadridStr = yesterday.toLocaleDateString("en-CA", { timeZone: "Europe/Madrid" });
  const ydayDisplay = yesterday.toLocaleDateString("es-AR", { day: "numeric", month: "long", year: "numeric", timeZone: "Europe/Madrid" });

  // 1. Emails ENVIADOS ayer = response_tracking filtrado por MB + sent_at de ayer
  //    Distinguimos source para "Analysis (manual)" vs "Agente / Prospects" en details.ui_origin
  //    pero como response_tracking no guarda ui_origin, usamos agent_actions.
  const since = `${ydayMadridStr}T00:00:00`;
  const until = `${ydayMadridStr}T23:59:59`;
  const headers = { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` };

  let actionsRows = [];
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_agent_actions?user_email=eq.${encodeURIComponent(mbEmail)}&action=in.(sent,re_sent,bounce_retry_sent,secondary_sent)&created_at=gte.${encodeURIComponent(since)}&created_at=lte.${encodeURIComponent(until)}&select=action,details&limit=2000`,
      { headers }
    );
    actionsRows = r.ok ? await r.json() : [];
  } catch {}
  const isManual = (a) => (a.details?.ui_origin || "") === "toolbar_manual";
  const emailsManual = actionsRows.filter(isManual).length;
  const emailsAgent  = actionsRows.length - emailsManual;
  const emailsTotal  = actionsRows.length;

  // 2. Prospects PROCESADOS ayer = review_queue rows con validated_by=mb O created_by=mb del día
  let queueRows = [];
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?or=(validated_by.eq.${encodeURIComponent(mbEmail)},created_by.eq.${encodeURIComponent(mbEmail)})&or=(validated_at.gte.${encodeURIComponent(since)},created_at.gte.${encodeURIComponent(since)})&validated_at=lte.${encodeURIComponent(until)}&select=geo,geos_all,created_by,validated_by,created_at,validated_at,domain&limit=2000`,
      { headers }
    );
    queueRows = r.ok ? await r.json() : [];
  } catch {}
  // Dedup por domain (alguien puede ser create+validate del mismo)
  const seen = new Set();
  const myProspects = [];
  for (const q of queueRows) {
    const d = (q.domain || "").toLowerCase();
    if (!d || seen.has(d)) continue;
    // Sólo contar si validated_at o created_at cayó ayer (por el OR de arriba puede colarse otra fecha)
    const va = q.validated_at || "";
    const ca = q.created_at || "";
    const validFromY = va.startsWith(ydayMadridStr);
    const createdY   = ca.startsWith(ydayMadridStr);
    if (!validFromY && !createdY) continue;
    seen.add(d);
    myProspects.push(q);
  }
  const prospectsTotal = myProspects.length;

  // 3. Breakdown por continente
  const byContinent = new Map();
  for (const q of myProspects) {
    let iso = "";
    if (Array.isArray(q.geos_all) && q.geos_all.length) iso = q.geos_all[0];
    else if (q.geo) iso = q.geo;
    const cont = _countryToContinent(iso);
    byContinent.set(cont, (byContinent.get(cont) || 0) + 1);
  }
  const contStrs = [...byContinent.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([c, n]) => `${Math.round((n / prospectsTotal) * 100)}% ${c}`)
    .slice(0, 4);

  // País más frecuente
  const byCountry = new Map();
  for (const q of myProspects) {
    let iso = "";
    if (Array.isArray(q.geos_all) && q.geos_all.length) iso = String(q.geos_all[0]).toUpperCase().slice(0,2);
    else if (q.geo) iso = String(q.geo).toUpperCase().slice(0,2);
    if (!iso) continue;
    byCountry.set(iso, (byCountry.get(iso) || 0) + 1);
  }
  const topCountry = [...byCountry.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || "";

  // 4. Promedio semanal del MB (últimos 7 días, exclusive ayer)
  const weekFrom = new Date(now.getTime() - 8 * 86400_000).toISOString();
  const weekTo   = `${ydayMadridStr}T00:00:00`;
  let weekProspects = 0;
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_review_queue?or=(validated_by.eq.${encodeURIComponent(mbEmail)},created_by.eq.${encodeURIComponent(mbEmail)})&created_at=gte.${encodeURIComponent(weekFrom)}&created_at=lt.${encodeURIComponent(weekTo)}&select=id`,
      { headers: { ...headers, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const rangeHdr = r.headers.get("content-range") || "";
    weekProspects = parseInt(rangeHdr.match(/\/(\d+)$/)?.[1] || "0", 10);
  } catch {}
  const weekAvg = weekProspects > 0 ? Math.round(weekProspects / 7) : 0;

  // 5. Construir mensaje rioplatense
  const lines = [];
  lines.push(`👋 ${mbName}, ayer ${ydayDisplay}:`);
  if (prospectsTotal > 0) {
    let geoLine = `📋 Prospectaste ${prospectsTotal} websites.`;
    if (contStrs.length) geoLine += ` ${contStrs.join(" · ")}.`;
    if (topCountry) geoLine += ` Incluyendo varias de "${topCountry}".`;
    lines.push(geoLine);
  } else {
    lines.push(`📋 No prospectaste websites ayer.`);
  }
  if (emailsTotal > 0) {
    lines.push(`✉️ Enviaste ${emailsTotal} emails — ${emailsManual} desde Analysis · ${emailsAgent} desde el agente/Prospects.`);
  } else {
    lines.push(`✉️ No enviaste emails ayer.`);
  }
  // Tip comparativo
  if (weekAvg > 0 && prospectsTotal > 0) {
    if (prospectsTotal < weekAvg * 0.7) {
      lines.push(`📉 Tu media de prospectos es INFERIOR a la de la semana (promedio ${weekAvg}/día). Quizás vale un push hoy.`);
    } else if (prospectsTotal > weekAvg * 1.3) {
      lines.push(`📈 ¡Día arriba del promedio! Tu media semanal es ${weekAvg}/día — ayer la superaste.`);
    }
  }
  // Tip simple basado en patrones
  if (prospectsTotal > 0 && emailsTotal === 0) {
    lines.push(`💡 Tip: prospectaste pero no enviaste mails — pasá por Prospects para empujar.`);
  } else if (prospectsTotal === 0 && emailsTotal > 5) {
    lines.push(`💡 Tip: muchos envíos pero pocos prospects nuevos — chequeá Prospects, hay leads disponibles.`);
  } else if (byContinent.size === 1 && prospectsTotal > 0) {
    lines.push(`💡 Tip: todos tus prospects de ayer fueron de la misma región. Diversificar GEO suele mejorar tasa de respuesta.`);
  }

  await createNotificationWorker(token, {
    mb_email: mbEmail.toLowerCase(),
    type: "daily_digest",
    severity: "info",
    title: `📊 Resumen de ayer — ${ydayDisplay}`,
    body: lines.join("\n"),
    metadata: {
      prospects_total: prospectsTotal,
      emails_total: emailsTotal,
      emails_manual: emailsManual,
      emails_agent: emailsAgent,
      week_avg: weekAvg,
      top_country: topCountry,
    },
    dedup_key: `daily_digest_${mbEmail.toLowerCase()}_${ydayMadridStr}`,
  });
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

  // FIX 2026-05-19: check de las tablas nuevas (source_performance + notifications).
  // Sin esto, si las migraciones no se aplicaron, los scanners explotan silenciosos.
  try {
    const [perfRes, notifRes] = await Promise.all([
      fetch(`${SUPABASE_URL}/rest/v1/toolbar_source_performance?select=mb_email&limit=1`, {
        headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || _workerToken}` },
      }),
      fetch(`${SUPABASE_URL}/rest/v1/toolbar_notifications?select=id&limit=1`, {
        headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || _workerToken}` },
      }),
    ]);
    if (!perfRes.ok) log(`🚨 SCHEMA CHECK: falta toolbar_source_performance → correr sql/2026-05-19_source_performance.sql`);
    if (!notifRes.ok) log(`🚨 SCHEMA CHECK: falta toolbar_notifications → correr sql/2026-05-19_notifications.sql`);
    if (perfRes.ok && notifRes.ok) log("✅ Schema check OK (source_performance + notifications presentes)");
  } catch (e) {
    log(`⚠️ Schema check (new tables) error: ${e.message}`);
  }

  let idleSince = Date.now();
  let _dbDownStreak = 0; // Maxi 2026-07-03: circuit breaker — cuántos chequeos seguidos falló la base
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
        // Maxi 2026-07-15: caches keyed-by-domain crecen 1 entrada por dominio único y NUNCA evictan →
        // en un proceso largo que procesa miles de dominios filtran memoria → alimentan el OOM que
        // reiniciaba el worker cada ~7min (SIGTERM de Railway). Los limpiamos periódicamente / bajo presión.
        // BARATOS (recomputan de un fetch/DNS → sin costo de API): limpiar seguido.
        if (rssMB > 450 || iterCount % 40 === 0) {
          let _freed = 0;
          for (const _c of [_adsTxtCache, _publisherClassCache, _domainLangCache, _mxCache]) {
            try { _freed += _c.size; _c.clear(); } catch {}
          }
          if (_freed > 0) log(`🧹 caches baratos limpiados (${_freed} entradas) — rss=${rssMB}MB`);
        }
        // CAROS (embeddings Voyage + picks de Claude = API paga): SOLO bajo presión real de memoria,
        // para no re-gastar créditos. Se sacrifican solo si de verdad estamos por OOM.
        if (rssMB > 550) {
          let _freedApi = 0;
          for (const _c of [_voyageWorkerCache, _claudePickCache]) {
            try { _freedApi += _c.size; _c.clear(); } catch {}
          }
          if (_freedApi > 0) log(`🧹 caches API (embeddings/claude) limpiados por presión de memoria (${_freedApi}) — rss=${rssMB}MB`);
        }
      } catch {}
    }

    // ── CIRCUIT BREAKER (Maxi 2026-07-03) ──────────────────────────
    // Los logs mostraron al worker MARTILLANDO la base caída: cada ~20s disparaba
    // queries pesadas (dedup in.() de cientos de dominios, counts, config) que tardan
    // 20s en timeoutear con 522 y reintentaba al toque → (a) quema IO al pedo y (b)
    // puede IMPEDIR que la instancia se recupere (retry storm). Ahora un probe barato
    // (limit=1, timeout 8s) chequea si la base responde; si NO, backoff exponencial
    // (20s→40s→80s→160s→5min cap) en vez de martillar. Le da aire para recuperarse.
    let _dbOk = false;
    try {
      const _hp = await fetch(
        `${SUPABASE_URL}/rest/v1/toolbar_config?select=key&limit=1`,
        { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` }, signal: AbortSignal.timeout(8000) }
      );
      _dbOk = _hp.ok;
    } catch { _dbOk = false; }
    if (!_dbOk) {
      _dbDownStreak++;
      const backoff = Math.min(300_000, 20_000 * Math.pow(2, Math.min(_dbDownStreak - 1, 4)));
      if (_dbDownStreak === 1 || _dbDownStreak % 5 === 0)
        log(`🔴 Base no responde (streak ${_dbDownStreak}) — backoff ${Math.round(backoff / 1000)}s, no martillo mientras se recupera`);
      await sleep(backoff);
      continue;
    }
    if (_dbDownStreak > 0) { log(`🟢 Base recuperada tras ${_dbDownStreak} intentos fallidos`); _dbDownStreak = 0; }

    // Maxi 2026-07-15 (F2): el reporte semanal de frozen es Domingo 20-21 Madrid, pero estaba DESPUÉS del
    // gate de fin de semana (que hace continue los sáb/dom) → nunca corría. Lo llamamos ANTES del gate
    // (tiene su propio gate interno Domingo 20-21, así que no molesta el resto de la semana).
    try { await runFrozenWeeklyReport(token); } catch (e) { log(`⚠️ frozenReport: ${e.message}`); }

    // ── REGLA DE ORO: lun-vie 9-20 Madrid. Fin de semana o fuera de hora → NADA corre ──
    // Aplica a TODOS los users y TODOS los flows: agent, csv queue, autopilot,
    // backfill, refresh, unfreezer.
    try {
      const ghCfg = await getConfig(token);
      const ghStart = parseInt(ghCfg.active_hours_start || "9", 10);
      const ghEnd   = parseInt(ghCfg.active_hours_end   || "23", 10);
      // Manual override del admin: si flag manual_override_until > now, bypass horario.
      // Permite encender autopilot/queue fuera de 9-23 L-V cuando admin lo necesita.
      // Expira solo (2h default) para que no quede prendido olvidado todo el finde.
      const ghOverrideUntil = ghCfg.manual_override_until ? new Date(ghCfg.manual_override_until).getTime() : 0;
      const ghOverrideActive = ghOverrideUntil > Date.now();
      if (!ghOverrideActive) {
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
      // Reconciliación one-time de Monday para los 161 falsos-fallos del bug .ok
      // (flag agent_reconcile_monday_bounces; se auto-apaga al terminar).
      reconcileMondayBounces(token).catch(e => log(`⚠️ reconcile monday: ${e.message}`));
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
            // Maxi 2026-06-22 FIX: UPSERT (on_conflict=domain) en vez de INSERT plano.
            // Antes el INSERT chocaba con la fila vieja status=frozen (dominio único) →
            // 409 tragado por .catch → el lead se borraba de frozen pero NO se re-encolaba
            // = PERDIDO. Ahora re-activa la fila existente a pending (o inserta si no hay).
            await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?on_conflict=domain`, {
              method: "POST",
              headers: {
                "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
                "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal",
              },
              body: JSON.stringify({
                domain: row.domain, status: "pending",
                source: row.source || "frozen_retry",
                uploaded_by: row.uploaded_by || "",
                processed_at: null,
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
    // Maxi 2026-06-18: ampliado para incluir leads con traffic=0 / NULL después
    // de 48hs (les damos chance de que el refresh los enriquezca). Excluye
    // monday_refresh (re-prospect explícito del MB, se procesan igual).
    if (iterCount % 30 === 0) {
      try {
        // Reset -1 → 0 (re-eligible for refresh)
        await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=eq.-1`, {
          method: "PATCH",
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
          body: JSON.stringify({ traffic: 0 }),
        }).catch(() => {});

        // 1. DELETE leads con traffic > 0 AND < MIN (basura conocida — el agente nunca los pickearía)
        // Maxi 2026-07-01: se SACÓ la excepción `source=neq.monday_refresh`. El floor de 350K es
        // duro para TODO (incluido monday_refresh) → los sub-350K de cualquier fuente que ya
        // quedaron en la cola (los 2K/12K/67K reportados) se limpian también.
        const subRes = await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gt.0&traffic=lt.${REVIEW_QUEUE_MIN_TRAFFIC}&select=id`, {
          headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" },
        });
        const subCount = parseInt((subRes.headers.get("content-range") || "").match(/\/(\d+)$/)?.[1] || "0", 10);
        if (subCount > 0) {
          await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&traffic=gt.0&traffic=lt.${REVIEW_QUEUE_MIN_TRAFFIC}`, {
            method: "DELETE",
            headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "return=minimal" },
          }).catch(() => {});
          log(`🗑 Cleanup: ${subCount} leads pending con traffic < ${REVIEW_QUEUE_MIN_TRAFFIC} eliminados`);
        }

        // 2. DELETE leads con traffic=0 OR NULL después de 48hs (sin tráfico tras
        //    refresh + re-process — son "fantasma" que ocupan espacio).
        const cutoff48h = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();
        const noTrafficRes = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&source=neq.monday_refresh&or=(traffic.eq.0,traffic.is.null)&created_at=lt.${encodeURIComponent(cutoff48h)}&select=id`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "count=exact", "Range": "0-0" } }
        );
        const noTrafficCount = parseInt((noTrafficRes.headers.get("content-range") || "").match(/\/(\d+)$/)?.[1] || "0", 10);
        if (noTrafficCount > 0) {
          await fetch(
            `${SUPABASE_URL}/rest/v1/toolbar_review_queue?status=eq.pending&source=neq.monday_refresh&or=(traffic.eq.0,traffic.is.null)&created_at=lt.${encodeURIComponent(cutoff48h)}`,
            { method: "DELETE", headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`, "Prefer": "return=minimal" } }
          ).catch(() => {});
          log(`🗑 Cleanup: ${noTrafficCount} leads pending SIN tráfico (> 48hs) eliminados`);
        }
      } catch (e) { log(`⚠️ cleanup: ${e.message}`); }
    }

    // Maxi 2026-07-15 (Cost#1): flush RapidAPI ELIMINADO. La persistencia la hace SOLO el RPC atómico
    // bump_api_counter (por hit, en rapidFetchWithRetry). Este flush read-modify-write DOBLE-CONTABA cada
    // hit (RPC + este RMW) → los caps trip a la mitad del uso real, y el RMW podía PISAR incrementos del
    // RPC (race → subconteo → overspend). _rapidGlobalCounter queda SOLO como gauge en-memoria del cap live.

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
        // Maxi 2026-07-03 perf: era un read dedicado a toolbar_config cada iteración.
        // worker_force_restart_at es una key del config → sale de getConfig (cacheado 30s).
        // Reacción al restart en ≤30s, más que suficiente para el auto-restart de Railway.
        const ts = (await getConfig(token)).worker_force_restart_at;
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
        await maybeRunAutoGoogleSlot(token).catch(e => log(`⚠️ autogoogle slot: ${e.message}`));
        await _measureFeederRuns(token).catch(e => log(`⚠️ feeder measure: ${e.message}`));
        await _checkAutoPauseAgent(token).catch(e => log(`⚠️ autopause: ${e.message}`));
        // Source performance aggregate (1× por día, guard interno)
        await maybeRunSourcePerformanceAggregate(token).catch(e => log(`⚠️ source perf: ${e.message}`));
        // M2: síntesis de reglas del feedback 👍/👎 del email IA (1× por día, guard interno)
        await maybeRunPitchRulesSynthesis(token).catch(e => log(`⚠️ pitch rules synth: ${e.message}`));
        // Síntesis de reglas de basura desde los rechazos (botón rojo enseña al filtro)
        await maybeRunProspectTrashSynthesis(token).catch(e => log(`⚠️ prospect trash synth: ${e.message}`));
        // Maxi 2026-07-01: análisis 3×/semana (L/X/V) — marca prospects sospechosos de rechazo (⚠️)
        await runSuspectRejectAnalysis(token).catch(e => log(`⚠️ suspect analysis: ${e.message}`));
        // Maxi 2026-07-13: barrido on-demand del pool (purga no-publishers viejos por blocklist +
        // detector estructural). Gated por config purge_blocked_prospects='true'; se auto-apaga al terminar.
        await sweepBlockedFromProspects(token).catch(e => log(`⚠️ purge prospects: ${e.message}`));
        // Maxi 2026-07-15: PULIDO UNIFICADO — 1 fetch/dominio: bloquea no-publishers + busca email
        // (scrape+social+informer+Apollo) para los que quedan sin email. Reemplaza purge+reenrich.
        // Gated por config polish_pool='true'; se auto-apaga al terminar. NO usa RapidAPI.
        await polishPool(token).catch(e => log(`⚠️ polish pool: ${e.message}`));
        // Maxi 2026-07-16: expansión por similares desde los Prospects grandes (gated similar_expansion_enabled).
        await runProspectSimilarExpansion(token).catch(e => log(`⚠️ similar-exp: ${e.message}`));
        // Notification scanners — cada uno tiene su guard de frecuencia interno
        await runNotificationScanners(token).catch(e => log(`⚠️ notif scanners: ${e.message}`));

        // Boot-time guarantee: agent_enabled_users siempre con mgargiulo si vacío.
        // Reemplaza al self-activator viejo (sin chequear horario ni manual_off).
        // Maxi 2026-07-03 perf: era un read dedicado a config cada iteración.
        // agent_enabled_users es una key del config → sale de getConfig (cacheado).
        let agentUsers = [];
        try { agentUsers = JSON.parse((await getConfig(token)).agent_enabled_users || "[]"); } catch {}
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
        // Maxi 2026-07-03 perf: solo importa "¿hay ≥1 pending?" → limit=1 sin count=exact
        // (el count exacto forzaba un scan completo del status index cada iteración).
        const pcRes = await fetch(
          `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.pending&select=id&limit=1`,
          { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
        );
        const pcRows = pcRes.ok ? await pcRes.json().catch(() => []) : [];
        const hasPending = Array.isArray(pcRows) && pcRows.length > 0;
        if (hasPending) {
          // Maxi 2026-07-03 perf: flag actual desde getConfig (cacheado), sin read dedicado
          const isOn = String((await getConfig(token)).csv_queue_enabled || "").toLowerCase() === "true";
          if (!isOn) {
            await setConfigValue(token, "csv_queue_enabled", "true");
            log(`🔛 csv_queue auto-encendida: items pending detectados`);
          }
        }
      } catch (e) { log(`⚠️ csvQueue auto-enable: ${e.message}`); }

      // Frozen weekly report → Maxi 2026-07-15 (F2): MOVIDO arriba del gate de fin de semana (esta llamada
      // era inalcanzable los domingos). Ver la llamada en la sección "REGLA DE ORO".

      // Poll liviano — lee autopilot + csv_queue + agent flags
      const flags = await getActiveFlags(token);
      if (iterCount === 1 || iterCount % 10 === 0) {
        log(`🚦 flags: autopilot=${flags.autopilot} csv=${flags.csvQueue} agent=${flags.agent} (users=${flags.agentUsers.length})`);
      }

      if (!flags.autopilot && !flags.csvQueue && !flags.agent) {
        // Maxi 2026-07-02: NO MÁS idle-exit(0). Bug real detectado: el worker hacía
        // exit(0) a las 4h idle, pero Railway tiene restart policy ON_FAILURE → un
        // exit 0 NO reinicia el container. Resultado: al vaciar la cola, el worker se
        // apagaba y quedaba MUERTO; los imports posteriores (sellers.json de los MB)
        // nunca se procesaban porque el auto-enable de csv_queue vive DENTRO de este
        // loop y no corre con el proceso caído. Nada lo despertaba.
        // Ahora el worker queda SIEMPRE vivo poleando cada IDLE_INTERVAL_MS (120s, CPU
        // casi nulo). Apenas entran items pending, el auto-enable de arriba prende la
        // cola y se procesan solos. Off-hours/finde ya duerme en las ramas de arriba
        // (sleep+continue, sin exit). Confiabilidad > el ahorro de billing del exit.
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
