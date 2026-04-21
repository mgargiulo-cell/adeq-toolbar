// ============================================================
// ADEQ AUTO-PROSPECTOR — v3
// Cambios v3:
//   - Fuente de dominios: Majestic Million (1M sitios rankeados del mundo)
//   - Los dominios de Monday se usan como EXCLUSIÓN (ya son clientes)
//   - Pool de dominios se descarga una vez al iniciar Railway (en memoria)
// Deploy: Railway
// ============================================================

import fetch from "node-fetch";

const SUPABASE_URL      = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_EMAIL    = process.env.SUPABASE_EMAIL;
const SUPABASE_PASSWORD = process.env.SUPABASE_PASSWORD;

const SESSION_LIMIT_MS  = 45 * 60 * 1000;
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
    headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` },
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
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
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
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ value }),
  });
}

async function getProcessedDomains(token) {
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/toolbar_import_queue?select=domain&expires_at=gt.${encodeURIComponent(new Date().toISOString())}`,
    { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
  );
  const rows = await res.json();
  return new Set(rows.map(r => r.domain));
}

async function markProcessed(token, domains) {
  const expiresAt = new Date(Date.now() + 60 * 24 * 60 * 60 * 1000).toISOString();
  await fetch(`${SUPABASE_URL}/rest/v1/toolbar_import_queue`, {
    method: "POST",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates",
    },
    body: JSON.stringify(domains.map(d => ({ domain: d, imported_by: "AUTO", expires_at: expiresAt }))),
  });
}

async function saveToReviewQueue(token, { domain, traffic, geo, language, category, contactName, emails, pitch, pitchSubject, pitchSubjects, score, adNetworks, pageTitle }) {
  await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue`, {
    method: "POST",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}`,
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
      status:         "pending",
    }),
  });
}

async function getApolloUsageToday(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=in.(apollo_calls_today,apollo_calls_date,apollo_daily_limit)&select=key,value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });

    const today   = new Date().toISOString().slice(0, 10);
    const storedDate  = map.apollo_calls_date  || "";
    const storedCount = parseInt(map.apollo_calls_today || "0", 10);
    const limit       = parseInt(map.apollo_daily_limit || "50",  10);

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
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
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
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
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

async function getTrafficData(domain, rapidApiKey) {
  const headers = { "x-rapidapi-key": rapidApiKey, "x-rapidapi-host": "similarweb-insights.p.rapidapi.com" };
  try {
    const res = await fetch(
      `https://similarweb-insights.p.rapidapi.com/traffic?domain=${encodeURIComponent(domain)}`,
      { headers, signal: AbortSignal.timeout(8000) }
    );
    if (!res.ok) return { visits: null, topCountry: null };
    const data = await res.json();
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

    return { visits, topCountry };
  } catch { return { visits: null, topCountry: null }; }
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

  // /v1/people/match is deprecated (HTTP 422) — use only mixed_people/search
  try {
    const res = await fetch("https://api.apollo.io/v1/mixed_people/search", {
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

// ── Scoring ───────────────────────────────────────────────────

const GEO_REGIONS = {
  LATAM:  ["Mexico","Argentina","Colombia","Chile","Brazil","Peru","Ecuador","Venezuela","Uruguay","Paraguay","Bolivia","Spain"],
  Europe: ["United Kingdom","France","Germany","Italy","Portugal","Netherlands","Belgium","Switzerland","Austria","Poland","Sweden","Norway","Denmark","Finland","Greece","Hungary","Czech Republic","Romania","Ukraine","Russia"],
  MENA:   ["UAE","Saudi Arabia","Egypt","Morocco","Turkey","Israel","Kuwait","Qatar","Algeria","Tunisia"],
  Asia:   ["India","Japan","South Korea","China","Taiwan","Hong Kong","Singapore","Malaysia","Indonesia","Philippines","Thailand","Vietnam","Pakistan"],
};

const HIGH_VALUE_CATS = new Set(["sports","news","entertainment","gambling","finance"]);
const MED_VALUE_CATS  = new Set(["health","travel","automotive","technology","food","business"]);

function scoreCandidate({ visits, category, topCountry, contactName, emails, pageContent, targetGeo }) {
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

  // Target geo bonus/penalty (±15)
  if (targetGeo && topCountry) {
    const region = GEO_REGIONS[targetGeo];
    if (region) {
      if (region.includes(topCountry)) score += 15;
      else                              score -= 10;
    } else if (targetGeo === topCountry) {
      score += 15;
    }
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

async function getNextCsvItem(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_csv_queue?status=eq.pending&order=uploaded_at.asc&limit=1&select=id,domain`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
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
          "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}`,
          "Content-Type": "application/json", "Prefer": "return=representation",
        },
        body: JSON.stringify({ status: "processing" }),
      }
    );
    const claimed = await claim.json().catch(() => []);
    return claimed?.[0] ? item : null; // si otro proceso lo tomó, claimed será []
  } catch { return null; }
}

async function markCsvItem(token, id, status, fields = {}) {
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/toolbar_csv_queue?id=eq.${id}`, {
      method: "PATCH",
      headers: {
        "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ status, processed_at: new Date().toISOString(), ...fields }),
    });
  } catch {}
}

// Busca un item en Monday por nombre de dominio (name column contains domain)
async function findMondayItem(domain, mondayApiKey) {
  const clean = cleanDomain(domain);
  const query = `{
    boards(ids: [1420268379]) {
      items_page(limit: 5, query_params: { rules: [
        { column_id: "name", compare_value: "${clean}", operator: contains_text }
      ]}) {
        items { id name }
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
    return match ? match.id : null;
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
  const { monday_api_key, rapidapi_key, apollo_api_key } = cfg;
  const domain = item.domain;

  // 1. Buscar item en Monday
  const itemId = await findMondayItem(domain, monday_api_key);
  if (!itemId) {
    await markCsvItem(token, item.id, "skipped", { error_message: "not in Monday" });
    log(`  ⏭ ${domain} — no está en Monday`);
    return;
  }

  // 2. Traffic + page content en paralelo (sin Gemini)
  const [trafficData, pageContent] = await Promise.all([
    getTrafficData(domain, rapidapi_key),
    fetchPageContent(domain),
  ]);
  const { visits, topCountry } = trafficData;

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

  // 4. Update Monday — solo campos con data (no sobreescribe con vacío)
  const columnValues = {};
  if (visits)      columnValues["texto7"] = String(visits);                         // Paginas Vistas
  if (topCountry)  columnValues["texto6"] = topCountry;                             // Top Geo
  if (primaryEmail) columnValues["email_mm2edcd3"] = { email: primaryEmail, text: primaryEmail };

  try {
    if (Object.keys(columnValues).length > 0) {
      await updateMondayItem(itemId, columnValues, monday_api_key);
      await markCsvItem(token, item.id, "done", { monday_item_id: itemId });
      log(`  ✅ ${domain} — visits:${visits || 0} geo:${topCountry || "-"} email:${primaryEmail ? "yes" : "no"} apollo:${canUseApollo ? "yes" : "no"}`);
    } else {
      await markCsvItem(token, item.id, "done", { monday_item_id: itemId, error_message: "no new data" });
      log(`  ○ ${domain} — sin data nueva`);
    }
  } catch (e) {
    await markCsvItem(token, item.id, "error", { error_message: e.message.substring(0, 500) });
    log(`  ❌ ${domain} — ${e.message}`);
  }
}

async function runCsvQueue(token, cfg, maxItems = 100) {
  const apolloUsage = await getApolloUsage(token, cfg);
  const callsRef    = { count: 0 };
  let processed     = 0;

  log(`▶ CSV queue start (apollo usados hoy: ${apolloUsage.usedToday}/${apolloUsage.limit})`);

  while (processed < maxItems) {
    const item = await getNextCsvItem(token);
    if (!item) { log("  (cola vacía)"); break; }

    processed++;
    log(`→ [${processed}/${maxItems}] ${item.domain}`);

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
}

// ── Sesión de prospección ─────────────────────────────────────

async function runSession(token, cfg, sessionStart) {
  const { monday_api_key, rapidapi_key, apollo_api_key } = cfg;

  // Targets de esta sesión (desde toolbar_config)
  const targetGeo      = cfg.target_geo      || "";
  const targetCategory = cfg.target_category || "";
  const minScore       = Number(cfg.min_score)    || 20;
  const sessionMinTraffic = Number(cfg.min_traffic) || MIN_TRAFFIC;

  const targetInfo = [targetGeo, targetCategory].filter(Boolean).join(" + ") || "sin filtros";
  log(`Sesión iniciada. Target: ${targetInfo} | Min score: ${minScore} | Min traffic: ${(sessionMinTraffic/1000).toFixed(0)}K`);

  // Carga en paralelo: pool global, Monday, procesados, rechazos, uso Apollo
  const [pool, mondayDomains, processed, rejectionPatterns, apolloUsage] = await Promise.all([
    loadDomainPool(),
    fetchMondayDomains(monday_api_key),
    getProcessedDomains(token),
    getRejectionPatterns(token),
    getApolloUsageToday(token),
  ]);

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

  // Cola dinámica: permite inyectar sitios similares durante la sesión
  const toProcess     = [...candidates];
  const seenInSession = new Set(toProcess);
  let count = 0, added = 0, skipped = 0, lowScore = 0, discovered = 0;

  while (toProcess.length > 0) {
    if (Date.now() - sessionStart >= SESSION_LIMIT_MS) {
      log("⏱ 45 minutos — auto-apagando.");
      await setConfigValue(token, "auto_prospecting_enabled", "false");
      await setConfigValue(token, "auto_session_start", "");
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

    // Paso 1: tráfico + contenido de página en paralelo (sin Gemini)
    const [trafficData, pageContent] = await Promise.all([
      getTrafficData(domain, rapidapi_key),
      fetchPageContent(domain),
    ]);

    const { visits, topCountry } = trafficData;

    // Filtro primario: tráfico mínimo
    if (!visits || visits < sessionMinTraffic) {
      log(`  ✗ Tráfico (${visits ? Math.round(visits/1000)+"K" : "N/A"}) — descartado`);
      await markProcessed(token, [domain]);
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    const language    = "";
    const category    = pageContent?.category || "";
    const contactName = "";
    const adNetworks  = pageContent?.adNetworks || [];
    const pageTitle   = pageContent?.title       || "";

    // Filtro por categoría objetivo (si está configurado)
    if (targetCategory && category !== targetCategory) {
      log(`  ✗ Categoría ${category} ≠ objetivo ${targetCategory} — descartado`);
      await markProcessed(token, [domain]);
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    // Filtro duro de GEO — si topCountry está disponible y no pertenece a la región objetivo, descartar
    if (targetGeo && topCountry) {
      const region = GEO_REGIONS[targetGeo];
      const inRegion = region ? region.includes(topCountry) : (targetGeo === topCountry);
      if (!inRegion) {
        log(`  ✗ GEO ${topCountry} ∉ ${targetGeo} — descartado`);
        await markProcessed(token, [domain]);
        count++; skipped++;
        await sleep(DOMAIN_DELAY_MS);
        continue;
      }
    }

    // Scoring: tráfico + categoría + contacto + geo target + contenido accesible
    const rawScore = scoreCandidate({ visits, category, topCountry, contactName, emails: [], pageContent, targetGeo });

    // Penalización por patrones de rechazo aprendidos
    const catPenalty = Math.min(20, (rejectionPatterns.categories[category] || 0) * 4);
    const geoPenalty = Math.min(10, (rejectionPatterns.geos[topCountry]     || 0) * 2);
    const finalScore = rawScore - catPenalty - geoPenalty;

    if (finalScore < minScore) {
      log(`  ✗ Score ${finalScore} (raw ${rawScore}, penalización cat:${catPenalty} geo:${geoPenalty}) — descartado`);
      await markProcessed(token, [domain]);
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
    });
    await markProcessed(token, [domain]);
    count++; added++;

    log(`  ✓ score:${scoreWithEmails} | ${Math.round(visits/1000)}K | ${topCountry||"N/A"} | ${language} | ${category} | ${contactName||"—"} | ${emails.length} email(s)`);
    await sleep(DOMAIN_DELAY_MS);
  }

  log(`Sesión completada — ${count} procesados | ${added} guardados | ${skipped} bajo tráfico | ${lowScore} bajo score | ${discovered} descubiertos vía similares`);
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

      // Poll liviano — lee autopilot + csv_queue flags
      const flags = await getActiveFlags(token);
      if (!flags.autopilot && !flags.csvQueue) {
        await sleep(IDLE_INTERVAL_MS);
        continue;
      }

      // CSV queue tiene prioridad sobre autopilot normal
      if (flags.csvQueue) {
        const cfg = await getConfig(token);
        await runCsvQueue(token, cfg, 100); // procesa hasta 100 por tanda
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
        log("⏱ Sesión expirada (45 min) — auto-apagando.");
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
