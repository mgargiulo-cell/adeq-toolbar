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

const SESSION_LIMIT_MS = 45 * 60 * 1000;
const POLL_INTERVAL_MS = 20 * 1000;
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

async function saveToReviewQueue(token, { domain, traffic, geo, language, category, contactName, emails, pitch, pitchSubject }) {
  await fetch(`${SUPABASE_URL}/rest/v1/toolbar_review_queue`, {
    method: "POST",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates",
    },
    body: JSON.stringify({
      domain,
      traffic:       traffic ? Math.round(traffic) : 0,
      geo:           geo           || "",
      language:      language      || "",
      category:      category      || "",
      contact_name:  contactName   || "",
      emails:        emails        || [],
      pitch:         pitch         || "",
      pitch_subject: pitchSubject  || "",
      status:        "pending",
    }),
  });
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
  try {
    const res = await fetch(
      `https://similarweb-insights.p.rapidapi.com/traffic?domain=${encodeURIComponent(domain)}`,
      {
        headers: {
          "x-rapidapi-key":  rapidApiKey,
          "x-rapidapi-host": "similarweb-insights.p.rapidapi.com",
        },
        signal: AbortSignal.timeout(8000),
      }
    );
    if (!res.ok) return { visits: null, topCountry: null };
    const data = await res.json();
    const visits = data?.visits || data?.Visits || data?.pageViews || null;

    // Extract top country from various possible field names
    const shares = data?.topCountryShares || data?.CountryShares || data?.countries || data?.topCountries || [];
    let countryCode = null;
    if (Array.isArray(shares) && shares.length) {
      countryCode = shares[0]?.country || shares[0]?.countryCode || shares[0]?.Country || null;
    }
    const topCountry = countryCode
      ? (COUNTRY_CODES[String(countryCode).toUpperCase()] || countryCode)
      : null;

    return { visits, topCountry };
  } catch { return { visits: null, topCountry: null }; }
}

async function findSimilarSites(domain, rapidApiKey) {
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
    // Handle various response shapes
    const raw = data?.similar_sites || data?.similarSites || data?.sites
              || data?.related_sites || data?.data?.similar_sites || data?.data || [];
    const sites = Array.isArray(raw) ? raw : [];
    return sites
      .map(s => cleanDomain(s?.domain || s?.url || s?.name || (typeof s === "string" ? s : "")))
      .filter(d => isDomainAllowed(d));
  } catch { return []; }
}

async function findContactAndMeta(domain, geminiKey) {
  try {
    const prompt = `Analyze the website "${domain}" and return ONLY this JSON (no extra text):
{
  "first_name": "",
  "last_name": "",
  "title": "",
  "language": "en",
  "category": "other"
}
Rules:
- first_name / last_name: CEO, founder, or main decision maker (empty string if not found)
- language: 2-letter ISO code of the site's main content language (en/es/pt/it/ar/fr/de)
- category: one of sports / news / finance / technology / entertainment / health / travel / gambling / automotive / food / business / other`;

    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${geminiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          tools: [{ google_search: {} }],
          generationConfig: { temperature: 0.1, maxOutputTokens: 200 },
        }),
        signal: AbortSignal.timeout(15000),
      }
    );
    if (!res.ok) return null;
    const data  = await res.json();
    const text  = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
    const match = text.match(/\{[\s\S]*?\}/);
    if (!match) return null;
    const p = JSON.parse(match[0]);
    return {
      firstName: p.first_name || "",
      lastName:  p.last_name  || "",
      title:     p.title      || "",
      language:  p.language   || "en",
      category:  p.category   || "other",
    };
  } catch { return null; }
}

async function findAllEmails(domain, firstName, lastName, rapidApiKey) {
  const emails = [];
  try {
    const params = new URLSearchParams({ domain, first_name: firstName, last_name: lastName });
    const res = await fetch(
      `https://apollo-io-enrichment-data-scraper.p.rapidapi.com/email-finder.php?${params}`,
      {
        headers: {
          "x-rapidapi-key":  rapidApiKey,
          "x-rapidapi-host": "apollo-io-enrichment-data-scraper.p.rapidapi.com",
        },
        signal: AbortSignal.timeout(10000),
      }
    );
    if (res.ok) {
      const data = await res.json();
      const found = [
        data?.email,
        data?.data?.email,
        ...(Array.isArray(data?.emails) ? data.emails : []),
      ].filter(e => e && typeof e === "string" && e.includes("@") && e.includes("."));
      emails.push(...found);
    }
  } catch {}
  return [...new Set(emails)];
}

async function generatePitchForDomain(domain, visits, geo, language, category, contactName, geminiKey) {
  const langNames = { en:"English", es:"Spanish", pt:"Portuguese", it:"Italian", ar:"Arabic", fr:"French", de:"German" };
  const langName  = langNames[language] || "English";
  const trafficStr = visits ? `${Math.round(visits / 1000)}K monthly visits` : "significant traffic";
  const greeting  = contactName ? `Dear ${contactName}` : "Dear Publisher";

  const prompt = `Write a short outreach email for ADEQ Media (digital advertising network) to the publisher "${domain}".

Site: ${trafficStr} · ${geo || "international"} · ${category || "general content"}
Greeting: ${greeting}

Rules:
- Language: ${langName}
- Tone: professional and direct
- Length: 3-4 short sentences
- Topic: monetization partnership opportunity
- No specific revenue numbers or percentages
- Sign as "ADEQ Media Team"
- Subject: concise, specific to the site

Return ONLY valid JSON: {"subject":"...","body":"..."}`;

  try {
    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${geminiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          generationConfig: { temperature: 0.4, maxOutputTokens: 500 },
        }),
        signal: AbortSignal.timeout(15000),
      }
    );
    if (!res.ok) return null;
    const data  = await res.json();
    const text  = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return null;
    const p = JSON.parse(match[0]);
    return { subject: p.subject || "", body: p.body || "" };
  } catch { return null; }
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

// ── Sesión de prospección ─────────────────────────────────────

async function runSession(token, cfg, sessionStart) {
  const { monday_api_key, rapidapi_key, gemini_api_key } = cfg;

  log("Sesión iniciada. Cargando fuentes...");

  // Cargar pool global + exclusiones en paralelo
  const [pool, mondayDomains, processed] = await Promise.all([
    loadDomainPool(),
    fetchMondayDomains(monday_api_key),  // usados como exclusión (ya son clientes)
    getProcessedDomains(token),
  ]);

  const mondaySet = new Set(mondayDomains);
  const candidates = pool.filter(d => !mondaySet.has(d) && !processed.has(d));

  // Shuffle para rotación — cada sesión procesa en orden distinto
  shuffleArray(candidates);

  log(`Pool: ${pool.length.toLocaleString()} | Monday (excluidos): ${mondayDomains.length} | Ya procesados: ${processed.size} | Candidatos: ${candidates.length.toLocaleString()}`);

  if (candidates.length === 0) {
    if (pool.length === 0) {
      log("Pool vacío (error de descarga) — forzando re-descarga en próxima sesión.");
      domainPool = null;
    } else {
      log("Sin candidatos nuevos — todos los dominios del pool ya fueron procesados.");
    }
    return;
  }

  // Cola dinámica: permite inyectar sitios similares descubiertos durante la sesión
  const toProcess = [...candidates];
  const seenInSession = new Set(toProcess);
  let count = 0; let added = 0; let skipped = 0; let discovered = 0;

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
    log(`→ [${count + 1} proc | +${discovered} desc | cola: ${toProcess.length}] ${domain}`);

    // Paso 1: tráfico + meta en paralelo
    const [trafficData, meta] = await Promise.all([
      getTrafficData(domain, rapidapi_key),
      findContactAndMeta(domain, gemini_api_key),
    ]);

    const { visits, topCountry } = trafficData;

    // Filtro: descartar si < 400K
    if (!visits || visits < MIN_TRAFFIC) {
      log(`  ✗ Tráfico insuficiente (${visits ? Math.round(visits / 1000) + "K" : "N/A"}) — saltando`);
      await markProcessed(token, [domain]);
      count++; skipped++;
      await sleep(DOMAIN_DELAY_MS);
      continue;
    }

    const language    = meta?.language  || "en";
    const category    = meta?.category  || "other";
    const contactName = meta?.firstName ? `${meta.firstName} ${meta.lastName}`.trim() : "";

    // Paso 2: emails + pitch + sitios similares en paralelo
    const [emails, pitchResult, similarSites] = await Promise.all([
      meta?.firstName
        ? findAllEmails(domain, meta.firstName, meta.lastName, rapidapi_key)
        : Promise.resolve([]),
      generatePitchForDomain(domain, visits, topCountry, language, category, contactName, gemini_api_key),
      findSimilarSites(domain, rapidapi_key),
    ]);

    // Inyectar sitios similares en la cola (si no están ya vistos ni procesados)
    let newFromSimilar = 0;
    for (const sim of similarSites) {
      if (!seenInSession.has(sim) && !processed.has(sim) && !mondaySet.has(sim)) {
        seenInSession.add(sim);
        toProcess.push(sim);
        newFromSimilar++;
        discovered++;
      }
    }
    if (newFromSimilar > 0) {
      log(`  🔗 ${newFromSimilar} sitios similares añadidos a la cola`);
    }

    await saveToReviewQueue(token, {
      domain, traffic: visits, geo: topCountry, language, category,
      contactName, emails,
      pitch:        pitchResult?.body    || "",
      pitchSubject: pitchResult?.subject || "",
    });
    await markProcessed(token, [domain]);
    count++; added++;

    log(`  ✓ ${Math.round(visits / 1000)}K | ${topCountry || "N/A"} | ${language} | ${category} | ${contactName || "N/A"} | ${emails.length} email(s)`);
    await sleep(DOMAIN_DELAY_MS);
  }

  log(`Sesión completada — ${count} procesados, ${added} en cola de revisión, ${skipped} saltados (<400K), ${discovered} descubiertos vía similares.`);
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

      const cfg = await getConfig(token);

      if (cfg.auto_prospecting_enabled !== "true" && cfg.auto_prospecting_enabled !== true) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

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
