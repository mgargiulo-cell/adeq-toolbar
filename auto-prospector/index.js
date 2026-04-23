// ============================================================
// ADEQ AUTO-PROSPECTOR — v3
// Cambios v3:
//   - Fuente de dominios: Majestic Million (1M sitios rankeados del mundo)
//   - Los dominios de Monday se usan como EXCLUSIÓN (ya son clientes)
//   - Pool de dominios se descarga una vez al iniciar Railway (en memoria)
// Deploy: Railway
// ============================================================

import fetch from "node-fetch";

const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY         = process.env.SUPABASE_ANON_KEY;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // bypass RLS (backend worker)
const SUPABASE_EMAIL            = process.env.SUPABASE_EMAIL;
const SUPABASE_PASSWORD         = process.env.SUPABASE_PASSWORD;
const CLOUDFLARE_API_TOKEN      = process.env.CLOUDFLARE_API_TOKEN || null; // optional: Radar country-indexed pool

// If a service-role key is set, use it as Bearer — bypasses RLS.
// Otherwise fall back to user JWT (won't see items uploaded by other users with RLS on).
const BACKEND_BEARER = SUPABASE_SERVICE_ROLE_KEY || null;

const SESSION_LIMIT_MS  = 60 * 60 * 1000; // 1 hora max por sesión de autopilot
const POLL_INTERVAL_MS  = 20 * 1000;   // durante sesión activa
const IDLE_INTERVAL_MS  = 120 * 1000;  // cuando autopilot está OFF (2 min)
const DOMAIN_DELAY_MS  = 2500;
const MIN_TRAFFIC      = 400_000;

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
  // TLDs académicos
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
  // Palabras clave en el dominio
  const kw = /\b(university|universidad|universidade|universit[aey]|uni[-.]|college|instituto[-.]tecnol|polytechnic|akademi|hochschule|facultad)\b/i;
  return kw.test(domain);
}

function isDomainAllowed(domain) {
  if (!domain || !domain.includes(".")) return false;
  if (EXCLUDE_DOMAINS.has(domain)) return false;
  if (isUniversityDomain(domain)) return false;
  return true;
}

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
      const url = `https://api.cloudflare.com/client/v4/radar/ranking/top?location=${encodeURIComponent(cc)}&limit=${LIMIT}`;
      const res = await fetch(url, { headers, signal: AbortSignal.timeout(15_000) });
      if (!res.ok) {
        log(`  ⚠️ Radar ${cc}: HTTP ${res.status} — saltado`);
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
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(auto_prospecting_enabled,csv_queue_enabled)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map = {};
    rows.forEach(r => { map[r.key] = r.value === "true"; });
    return { autopilot: !!map.auto_prospecting_enabled, csvQueue: !!map.csv_queue_enabled };
  } catch { return { autopilot: false, csvQueue: false }; }
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

async function saveToReviewQueue(token, { domain, traffic, geo, language, category, contactName, emails, pitch, pitchSubject, pitchSubjects, score, adNetworks, pageTitle, createdBy }) {
  await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue`, {
    method: "POST",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}`,
      "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates",
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
      created_by:     createdBy      || "",    // user que inició el autopilot
      status:         "pending",
    }),
  });
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

async function getApolloUsageToday(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(apollo_calls_today,apollo_calls_date,apollo_daily_limit)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${BACKEND_BEARER || token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const today   = new Date().toISOString().slice(0, 10);
    const storedDate  = map.apollo_calls_date  || "";
    const storedCount = parseInt(map.apollo_calls_today || "0", 10);
    const limit       = parseInt(map.apollo_daily_limit || "150", 10);

    // Si cambió el día, el contador empieza de cero
    const usedToday = storedDate === today ? storedCount : 0;
    return { usedToday, limit, today };
  } catch { return { usedToday: 0, limit: 50, today: new Date().toISOString().slice(0, 10) }; }
}

async function saveApolloUsage(token, callsThisSession, today) {
  if (callsThisSession === 0) return;
  try {
    // Leer valor actual primero (otra sesión pudo haber corrido en paralelo)
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

async function fetchMondayDomains(apiKey) {
  const query = `{
    boards(ids: [1420268379]) {
      items_page(limit: 500) {
        items { name }
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
  return items.map(i => cleanDomain(i.name)).filter(Boolean);
}

// Retry helper — 3 attempts with exponential backoff for transient RapidAPI errors.
// 5xx + network errors retry; 4xx (bad domain) fails fast. Returns null on permanent failure.
async function rapidFetchWithRetry(url, headers, timeout = 8000) {
  let lastErr = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(url, { headers, signal: AbortSignal.timeout(timeout) });
      if (res.ok) return await res.json();
      // 4xx = client error, don't retry
      if (res.status >= 400 && res.status < 500 && res.status !== 429) return null;
      // 429 / 5xx = retry
      lastErr = `HTTP ${res.status}`;
    } catch (e) {
      lastErr = e.message;
    }
    if (attempt < 2) await sleep(500 * Math.pow(2, attempt)); // 500ms, 1s
  }
  return { __error: lastErr }; // signal to caller: distinguish from "empty"
}

async function getTrafficData(domain, rapidApiKey) {
  const headers = { "x-rapidapi-key": rapidApiKey, "x-rapidapi-host": "similarweb-insights.p.rapidapi.com" };
  try {
    const data = await rapidFetchWithRetry(
      `https://similarweb-insights.p.rapidapi.com/traffic?domain=${encodeURIComponent(domain)}`,
      headers
    );
    if (!data) return { visits: null, topCountry: null, error: null };             // 4xx = domain not in SW
    if (data.__error) return { visits: null, topCountry: null, error: data.__error }; // retries exhausted
    const visits = data?.Visits || data?.visits || data?.pageViews || null;

    // Extract top country — try inline data first, then separate /countries endpoint
    let topCountry = null;
    const inlineList = data?.TopCountries || data?.Countries || data?.countries
                    || data?.topCountryShares || data?.CountryShares || [];
    if (Array.isArray(inlineList) && inlineList.length) {
      const c    = inlineList[0];
      const code = (c?.CountryCode || c?.countryCode || c?.Country || c?.country || "").toUpperCase().slice(0, 2);
      if (code) topCountry = COUNTRY_CODES[code] || code;
    }

    // Fallback: dedicated /countries endpoint (used by extension too)
    if (!topCountry) {
      try {
        const r2 = await fetch(
          `https://similarweb-insights.p.rapidapi.com/countries?domain=${encodeURIComponent(domain)}`,
          { headers, signal: AbortSignal.timeout(6000) }
        );
        if (r2.ok) {
          const d2   = await r2.json();
          const list = Array.isArray(d2) ? d2 : (d2?.TopCountries || d2?.Countries || d2?.countries || []);
          if (list.length) {
            const c    = list[0];
            const code = (c?.CountryCode || c?.countryCode || c?.Country || c?.country || "").toUpperCase().slice(0, 2);
            if (code) topCountry = COUNTRY_CODES[code] || code;
          }
        }
      } catch {}
    }

    return { visits, topCountry, error: null };
  } catch (e) { return { visits: null, topCountry: null, error: e.message }; }
}

async function findSimilarSites(domain, rapidApiKey) {
  const [swSites, ssSites] = await Promise.all([
    (async () => {
      try {
        const res = await fetch(
          `https://similarweb-insights.p.rapidapi.com/similar-sites?domain=${encodeURIComponent(domain)}`,
          {
            headers: {
              "x-rapidapi-key":  rapidApiKey,
              "x-rapidapi-host": "similarweb-insights.p.rapidapi.com",
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

  return [...new Set([...swSites, ...ssSites])];
}

const APOLLO_GOOD_STATUSES = new Set(["verified", "likely", "guessed"]);

async function findAllEmails(domain, apolloApiKey) {
  if (!apolloApiKey) return [];
  const emails = [];

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
        if (p.email && APOLLO_GOOD_STATUSES.has(p.email_status)) emails.push(p.email);
      }
    }
  } catch {}

  return [...new Set(emails)];
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
    const textForCategory = `${title} ${desc} ${domain}`.toLowerCase();
    let category = "other";
    if      (/sport|futbol|futebol|soccer|football|nba|basket|tennis|béisbol|beisbol|liga|mlb|f1|motor|boxeo|boxing/.test(textForCategory)) category = "sports";
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

    return { title: title.slice(0, 100), description: desc.slice(0, 280), adNetworks, category };
  } catch { return null; }
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
  "com.ar","com.br","com.mx","com.co","com.pe","com.uy","com.ec","com.ve","com.bo",
  "com.es","com.au","com.cn","com.tw","com.hk","com.sg","com.my","com.tr","com.eg",
  "com.sa","com.ng","com.za","com.ph","com.vn","com.pk",
  "co.uk","co.za","co.in","co.kr","co.jp","co.il",
  "org.uk","org.ar","org.br","org.mx","gov.uk","ac.uk","ac.in","gov.ar",
]);
function coreDomain(domain) {
  if (!domain) return "";
  const parts = domain.toLowerCase().replace(/^www\./, "").split(".");
  if (parts.length <= 2) return parts[0]; // foo.com → "foo"
  // Last 2 labels — check if it's a multi-part TLD (e.g. "com.ar")
  const last2 = parts.slice(-2).join(".");
  if (MULTI_PART_TLDS.has(last2)) return parts[parts.length - 3]; // x.com.ar → "x"
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

const CSV_DAILY_LIMIT_PER_USER = 75;

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

  // Usar la Monday API key del usuario que subió el CSV (sus acciones se registran como él)
  const mondayApiKey = getMondayKeyForUser(cfg, item.uploaded_by);
  if (!mondayApiKey) {
    await markCsvItem(token, item.id, "error", { error_message: "No Monday API key for user " + (item.uploaded_by || "unknown") });
    log(`  ❌ ${domain} — sin API key para ${item.uploaded_by}`);
    return;
  }

  // 1. Buscar item en Monday y validar estado
  const match = await findMondayItem(domain, mondayApiKey);
  if (!match) {
    await markCsvItem(token, item.id, "skipped", { error_message: "not in Monday" });
    log(`  ⏭ ${domain} — no está en Monday`);
    return;
  }

  // Solo procesar items cuyo Estado actual es "Ciclo Finalizado"
  if (match.estado !== "Ciclo Finalizado") {
    await markCsvItem(token, item.id, "skipped", { error_message: `estado=${match.estado || "?"} (solo Ciclo Finalizado)`, monday_item_id: match.id });
    log(`  ⏭ ${domain} — estado "${match.estado}" (no es Ciclo Finalizado)`);
    return;
  }

  // 2. Traffic + page content en paralelo
  const [trafficData, pageContent] = await Promise.all([
    getTrafficData(domain, rapidapi_key),
    fetchPageContent(domain),
  ]);
  let { visits, topCountry } = trafficData;

  // Fallback GEO: si SimilarWeb no trajo topCountry, inferir desde TLD
  if (!topCountry) {
    const inferred = inferCountryFromTLD(domain);
    if (inferred) topCountry = inferred;
  }

  // 3. Emails — Apollo si visits >= 500K, scraping siempre como fallback
  const canUseApollo = apollo_api_key
    && visits >= 500_000
    && (apolloUsage.usedToday + apolloCallsThisSessionRef.count) < apolloUsage.limit;

  const [apolloEmails, scraperEmails] = await Promise.all([
    canUseApollo
      ? findAllEmails(domain, apollo_api_key).then(r => { apolloCallsThisSessionRef.count += 2; return r; })
      : Promise.resolve([]),
    scrapeEmailsForDomain(domain),
  ]);
  const emails = [...new Set([...apolloEmails, ...scraperEmails])];
  const primaryEmail = emails[0] || "";

  // 4. Update Monday: traffic + geo + email + fecha + ejecutivo + estado
  const columnValues = {};
  if (visits)       columnValues["texto7"] = formatVisitsForMonday(visits);              // Paginas Vistas formateado (K/M)
  if (topCountry)   columnValues["texto6"] = topCountry;                                  // Top Geo
  if (primaryEmail) columnValues["email_mm2edcd3"] = { email: primaryEmail, text: primaryEmail };

  // Fecha Contacto = hoy
  columnValues["deal_close_date"] = { date: new Date().toISOString().split("T")[0] };

  // Ejecutivo = usuario que subió el CSV (persona column con user ID)
  const userId = MONDAY_USER_IDS[item.uploaded_by?.toLowerCase()];
  if (userId) columnValues["deal_owner"] = { personsAndTeams: [{ id: userId, kind: "person" }] };

  // Estado = "Propuesta Vigente (T)" (index 4)
  columnValues["deal_stage"] = { index: 4 };

  try {
    await updateMondayItem(match.id, columnValues, mondayApiKey);
    await markCsvItem(token, item.id, "done", { monday_item_id: match.id });
    const vstr = visits ? formatVisitsForMonday(visits) : "-";
    log(`  ✅ ${domain} — visits:${vstr} geo:${topCountry || "-"} email:${primaryEmail ? "yes" : "no"} apollo:${canUseApollo ? "yes" : "no"} → Propuesta Vigente (T)`);
  } catch (e) {
    await markCsvItem(token, item.id, "error", { error_message: e.message.substring(0, 500), monday_item_id: match.id });
    log(`  ❌ ${domain} — ${e.message}`);
  }
}

async function runCsvQueue(token, cfg, maxItems = 100) {
  const apolloUsage   = await getApolloUsageToday(token);
  const callsRef      = { count: 0 };
  const blockedUsers  = new Set(); // usuarios que alcanzaron el límite diario
  const userCounts    = new Map(); // email → cuántos procesamos en esta tanda
  let processed       = 0;

  log(`▶ CSV queue start (apollo usados hoy: ${apolloUsage.usedToday}/${apolloUsage.limit}, límite diario por user: ${CSV_DAILY_LIMIT_PER_USER})`);

  while (processed < maxItems) {
    const item = await getNextCsvItem(token, blockedUsers);
    if (!item) { log("  (cola vacía o todos los users alcanzaron su límite diario)"); break; }

    const userEmail = item.uploaded_by?.toLowerCase() || "";

    // Check quota diaria del usuario: suma los ya hechos en DB + los ya procesados en esta tanda
    const alreadyDone = await getUserCsvDoneToday(token, userEmail);
    const inBatch     = userCounts.get(userEmail) || 0;
    const userTotal   = alreadyDone + inBatch;

    if (userTotal >= CSV_DAILY_LIMIT_PER_USER) {
      log(`  ⏸ ${userEmail} alcanzó ${userTotal}/${CSV_DAILY_LIMIT_PER_USER} hoy — se reanuda mañana`);
      blockedUsers.add(userEmail);
      await revertCsvItemToPending(token, item.id); // deja el item para mañana
      continue;
    }

    processed++;
    userCounts.set(userEmail, inBatch + 1);
    log(`→ [${processed}/${maxItems}] ${item.domain} (${userEmail}: ${userTotal + 1}/${CSV_DAILY_LIMIT_PER_USER})`);

    try {
      await processCsvItem(token, item, cfg, apolloUsage, callsRef);
    } catch (e) {
      await markCsvItem(token, item.id, "error", { error_message: e.message.substring(0, 500) });
      log(`  ❌ ${item.domain} — uncaught: ${e.message}`);
    }

    await sleep(DOMAIN_DELAY_MS);
  }

  await saveApolloUsage(token, callsRef.count, new Date().toISOString().split("T")[0]);
  log(`◼ CSV queue end — procesados: ${processed}, apollo: ${callsRef.count}`);
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
  const AUTOPILOT_DAILY_LIMIT = 75;

  // Expand regions to concrete country lists for matching
  const allowedCountries = new Set();
  for (const g of targetGeos) {
    if (GEO_REGIONS[g]) GEO_REGIONS[g].forEach(c => allowedCountries.add(c));
    else allowedCountries.add(g); // treat as single country
  }
  const hasTargetGeo = targetGeos.length > 0;

  const targetInfo = [targetGeos.join("+"), targetCategory].filter(Boolean).join(" / ") || "sin filtros";
  log(`Sesión iniciada. User: ${sessionUser || "(desconocido)"} | Target: ${targetInfo} | Min score: ${minScore} | Min traffic: ${(sessionMinTraffic/1000).toFixed(0)}K`);

  // Quota diaria per-user para el autopilot
  const userTodayCount = await getUserAutopilotCountToday(token, sessionUser);
  if (sessionUser && userTodayCount >= AUTOPILOT_DAILY_LIMIT) {
    log(`⚠️ ${sessionUser} ya alcanzó el límite diario de autopilot (${userTodayCount}/${AUTOPILOT_DAILY_LIMIT}) — sesión no arranca`);
    return;
  }
  log(`Quota del día para ${sessionUser}: ${userTodayCount}/${AUTOPILOT_DAILY_LIMIT}`);

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

  // Carga en paralelo: Majestic, Monday, procesados, rechazos, uso Apollo
  const [majesticFullPool, mondayDomains, processed, rejectionPatterns, apolloUsage] = await Promise.all([
    loadDomainPool(),
    fetchMondayDomains(monday_api_key),
    getProcessedDomains(token),
    getRejectionPatterns(token),
    getApolloUsageToday(token),
  ]);

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
  const candidates = pool.filter(d => !mondaySet.has(d) && !processed.has(d));
  shuffleArray(candidates);

  log(`Pool: ${pool.length.toLocaleString()} | Excluidos (Monday): ${mondayDomains.length} | Ya procesados: ${processed.size} | Candidatos: ${candidates.length.toLocaleString()}`);

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
  const likedSeeds = feedback.likedDomains.slice(0, 30); // últimos 30
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
    if (Date.now() - sessionStart >= SESSION_LIMIT_MS) {
      log("⏱ 60 minutos — auto-apagando.");
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

    let { visits, topCountry, error: rapidError } = trafficData;

    // Si el RapidAPI devolvió error tras 3 retries, no descartar permanentemente
    if (rapidError) {
      log(`  ⚠ RapidAPI error (${rapidError}) — re-evaluar después`);
      await markProcessed(token, [domain], "api_error");
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Fallback GEO orden: SimilarWeb > Radar hint > TLD
    if (!topCountry && radarPool?.has(domain)) {
      const cc = [...radarPool.get(domain)][0];
      if (cc && COUNTRY_CODES[cc]) {
        topCountry = COUNTRY_CODES[cc];
        log(`  ℹ ${domain} — GEO del Radar: ${topCountry}`);
      }
    }
    if (!topCountry) {
      const inferred = inferCountryFromTLD(domain);
      if (inferred) {
        topCountry = inferred;
        log(`  ℹ ${domain} — GEO inferido por TLD: ${topCountry}`);
      }
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
    if (visits < sessionMinTraffic) {
      log(`  ✗ Tráfico (${Math.round(visits/1000)}K) < ${(sessionMinTraffic/1000)}K — descartado`);
      await markProcessed(token, [domain], "traffic_low");
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    const language    = "";
    const category    = pageContent?.category || "";
    const contactName = "";
    const adNetworks  = pageContent?.adNetworks || [];
    const pageTitle   = pageContent?.title       || "";

    // Filtro por categoría objetivo (si está configurado) — no permanente
    if (targetCategory && category !== targetCategory) {
      log(`  ✗ Categoría ${category} ≠ objetivo ${targetCategory} — descartado`);
      await markProcessed(token, [domain], "category_mismatch");
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Filtro duro de GEO — no permanente (user puede cambiar targets)
    if (hasTargetGeo && topCountry && !allowedCountries.has(topCountry)) {
      log(`  ✗ GEO ${topCountry} ∉ {${[...allowedCountries].slice(0,5).join(",")}${allowedCountries.size>5?"...":""}} — descartado`);
      await markProcessed(token, [domain], "geo_target_mismatch");
      count++; skipped++;
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

    if (finalScore < minScore) {
      log(`  ✗ Score ${finalScore} (raw ${rawScore}, -dis cat:${catPenalty+userCatPenalty} geo:${geoPenalty+userGeoPenalty}, +like cat:${userCatBonus} geo:${userGeoBonus} seed:${fromLikedSeed}) — descartado`);
      await markProcessed(token, [domain], "low_score");
      count++; lowScore++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Paso 2: emails + pitch + sitios similares en paralelo
    // Apollo solo si quedan créditos disponibles hoy
    const canUseApollo = apollo_api_key && (apolloUsage.usedToday + apolloCallsThisSession) < apolloUsage.limit;
    if (!canUseApollo && apollo_api_key) {
      log(`  ⚠️ Límite Apollo alcanzado (${apolloUsage.limit}/día) — sin búsqueda de email`);
    }

    const [apolloEmails, scraperEmails, similarSites] = await Promise.all([
      canUseApollo
        ? findAllEmails(domain, apollo_api_key).then(r => { apolloCallsThisSession += 2; return r; })
        : Promise.resolve([]),
      scrapeEmailsForDomain(domain),
      findSimilarSites(domain, rapidapi_key),
    ]);
    const emails = [...new Set([...apolloEmails, ...scraperEmails])];

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

    await saveToReviewQueue(token, {
      domain,
      traffic:       visits,
      geo:           topCountry,
      language,
      category,
      contactName,
      emails,
      pitch:         "",
      pitchSubject:  "",
      pitchSubjects: [],
      score:         scoreWithEmails,
      adNetworks,
      pageTitle,
      createdBy:     sessionUser,
    });
    await markProcessed(token, [domain], "added_to_review");
    count++; added++;

    log(`  ✓ score:${scoreWithEmails} | ${Math.round(visits/1000)}K | ${topCountry||"N/A"} | ${language} | ${category} | ${contactName||"—"} | ${emails.length} email(s)`);
    await sleep(DOMAIN_DELAY_MS);
  }

  log(`Sesión completada — ${count} procesados | ${added} guardados | ${skipped} skipped | ${lowScore} bajo score | ${dupOrg} dup-org | ${discovered} descubiertos vía similares`);
  await saveApolloUsage(token, apolloCallsThisSession, apolloUsage.today);
}

// ── Loop principal ────────────────────────────────────────────

async function main() {
  log("ADEQ Auto-Prospector v3 iniciado.");

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

  while (true) {
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

      // Poll liviano — lee autopilot + csv_queue flags
      const flags = await getActiveFlags(token);
      if (!flags.autopilot && !flags.csvQueue) {
        await sleep(IDLE_INTERVAL_MS);
        continue;
      }

      // CSV queue — procesar primero si está activa y hay items
      // (NO es mutuamente exclusivo con el autopilot; si CSV no tiene items, sigue a autopilot)
      let csvProcessed = 0;
      if (flags.csvQueue) {
        const cfgCsv = await getConfig(token);
        csvProcessed = await runCsvQueue(token, cfgCsv, 50); // batch chico para poder alternar con autopilot
        if (csvProcessed > 0) {
          // Hubo trabajo — volver al loop rápido (próxima iteración sigue con CSV o pasa a autopilot)
          await sleep(POLL_INTERVAL_MS);
          continue;
        }
        // No había items pending → cae a autopilot si está prendido
      }

      // Autopilot (sólo si está prendido)
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
        log("⏱ Sesión expirada (60 min) — auto-apagando.");
        await setConfigValue(token, "auto_prospecting_enabled", "false");
        await setConfigValue(token, "auto_session_start", "");
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
