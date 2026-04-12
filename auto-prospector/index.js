// ============================================================
// ADEQ AUTO-PROSPECTOR — v2
// Cambios:
//   - Escribe a toolbar_review_queue (en lugar de toolbar_historial)
//   - Filtro mínimo 400K visitas — dominios con menos son descartados
//   - Detecta país principal de tráfico (geo)
//   - Detecta idioma y categoría del sitio vía Gemini
//   - Recolecta múltiples emails vía Apollo
//   - Genera pitch completo con Gemini (subject + body)
//   - Shuffle de candidatos para rotación de países por sesión
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

  log("Sesión iniciada. Cargando dominios de Monday...");
  const allDomains = await fetchMondayDomains(monday_api_key);
  const processed  = await getProcessedDomains(token);
  const candidates = allDomains.filter(d => !processed.has(d));

  // Shuffle para rotación de países — cada sesión procesa en orden distinto
  shuffleArray(candidates);

  log(`${allDomains.length} en Monday — ${candidates.length} sin procesar.`);

  let count = 0; let added = 0; let skipped = 0;

  for (const domain of candidates) {
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

    log(`→ [${count + 1}/${candidates.length}] ${domain}`);

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

    // Paso 2: emails + pitch en paralelo
    const [emails, pitchResult] = await Promise.all([
      meta?.firstName
        ? findAllEmails(domain, meta.firstName, meta.lastName, rapidapi_key)
        : Promise.resolve([]),
      generatePitchForDomain(domain, visits, topCountry, language, category, contactName, gemini_api_key),
    ]);

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

  log(`Sesión completada — ${count} procesados, ${added} en cola de revisión, ${skipped} saltados (<400K).`);
}

// ── Loop principal ────────────────────────────────────────────

async function main() {
  log("ADEQ Auto-Prospector v2 iniciado.");

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
