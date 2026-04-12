// ============================================================
// ADEQ AUTO-PROSPECTOR — Servicio Node.js
// Comportamiento:
//   - Cuando AUTO está ON: corre sin parar hasta que se apague
//   - Límite automático de 45 minutos por sesión → se auto-apaga
//   - Sin límite de URLs: procesa todo lo que haya disponible
//   - Pausa entre dominios para respetar rate limits de APIs
// Deploy: Railway / Render (gratis)
// ============================================================

import fetch from "node-fetch";

const SUPABASE_URL      = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_EMAIL    = process.env.SUPABASE_EMAIL;
const SUPABASE_PASSWORD = process.env.SUPABASE_PASSWORD;

const SESSION_LIMIT_MS  = 45 * 60 * 1000; // 45 minutos
const POLL_INTERVAL_MS  = 20 * 1000;       // revisar estado cada 20 seg cuando OFF
const DOMAIN_DELAY_MS   = 2000;            // pausa entre dominios (rate limit)

// ── Supabase helpers ──────────────────────────────────────────
async function supabaseLogin() {
  // Debug: log first/last 6 chars of key to verify it's loading correctly
  const keyPreview = SUPABASE_ANON_KEY
    ? `${SUPABASE_ANON_KEY.slice(0, 6)}...${SUPABASE_ANON_KEY.slice(-6)} (len:${SUPABASE_ANON_KEY.length})`
    : "UNDEFINED";
  log(`Debug — URL: ${SUPABASE_URL} | KEY: ${keyPreview} | EMAIL: ${SUPABASE_EMAIL}`);

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

async function saveToHistory(token, { domain, traffic, contact }) {
  await fetch(`${SUPABASE_URL}/rest/v1/toolbar_historial`, {
    method: "POST",
    headers: {
      "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates",
    },
    body: JSON.stringify({
      domain,
      media_buyer: "AUTO",
      page_views:  traffic ? Math.round(traffic) : 0,
      raw_visits:  traffic ? Math.round(traffic) : 0,
      is_new:      true,
      ejecutivo:   contact?.name || "",
      email:       contact?.email || "",
      partners:    "", geo: "",
      date:        new Date().toISOString().split("T")[0],
      source:      "auto",
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

async function getTraffic(domain, rapidApiKey) {
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
    if (!res.ok) return null;
    const data = await res.json();
    return data?.visits || data?.Visits || data?.pageViews || null;
  } catch { return null; }
}

// Paso 1: Gemini grounding → nombre del decisor
async function findContactName(domain, geminiKey) {
  try {
    const prompt = `Find the CEO, founder, or main decision maker of the website "${domain}".
Return ONLY a JSON object: {"first_name":"John","last_name":"Smith","title":"CEO"}
If not found: {"first_name":"","last_name":"","title":""}`;

    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${geminiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          tools: [{ google_search: {} }],
          generationConfig: { temperature: 0.1, maxOutputTokens: 150 },
        }),
        signal: AbortSignal.timeout(12000),
      }
    );
    if (!res.ok) return null;
    const data  = await res.json();
    const text  = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return null;
    const p = JSON.parse(match[0]);
    if (!p.first_name || !p.last_name) return null;
    return { firstName: p.first_name, lastName: p.last_name, title: p.title || "" };
  } catch { return null; }
}

// Paso 2: Apollo RapidAPI → email real
async function findEmail(domain, firstName, lastName, rapidApiKey) {
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
    if (!res.ok) return null;
    const data = await res.json();
    return data?.email || data?.data?.email || data?.emails?.[0] || null;
  } catch { return null; }
}

// ── Helpers ───────────────────────────────────────────────────
function cleanDomain(str) {
  return (str || "").toLowerCase()
    .replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "").trim();
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg) {
  console.log(`[${new Date().toISOString().replace("T"," ").substring(0,19)}] ${msg}`);
}

// ── Sesión de prospección ─────────────────────────────────────
async function runSession(token, cfg, sessionStart) {
  const { monday_api_key, rapidapi_key, gemini_api_key } = cfg;

  log("Sesión iniciada. Cargando dominios de Monday...");
  const allDomains = await fetchMondayDomains(monday_api_key);
  const processed  = await getProcessedDomains(token);
  const candidates = allDomains.filter(d => !processed.has(d));

  log(`${allDomains.length} dominios en Monday — ${candidates.length} disponibles.`);

  let count = 0;
  for (const domain of candidates) {
    // Verificar límite de 45 minutos
    if (Date.now() - sessionStart >= SESSION_LIMIT_MS) {
      log(`⏱ 45 minutos alcanzados — auto-apagando.`);
      await setConfigValue(token, "auto_prospecting_enabled", "false");
      break;
    }

    // Verificar si fue apagado manualmente (cada 10 dominios)
    if (count % 10 === 0) {
      const freshCfg = await getConfig(token);
      if (freshCfg.auto_prospecting_enabled !== "true") {
        log("Autopilot apagado manualmente — deteniendo sesión.");
        break;
      }
    }

    log(`→ [${count + 1}/${candidates.length}] ${domain}`);

    // Paso 1: tráfico y nombre del decisor en paralelo
    const [traffic, contactInfo] = await Promise.all([
      getTraffic(domain, rapidapi_key),
      findContactName(domain, gemini_api_key),
    ]);

    // Paso 2: email real si tenemos nombre
    let email = null;
    if (contactInfo?.firstName && contactInfo?.lastName) {
      email = await findEmail(domain, contactInfo.firstName, contactInfo.lastName, rapidapi_key);
    }

    const contact = contactInfo
      ? { name: `${contactInfo.firstName} ${contactInfo.lastName}`.trim(), title: contactInfo.title, email }
      : null;

    await saveToHistory(token, { domain, traffic, contact });
    await markProcessed(token, [domain]);
    count++;

    log(`  ✓ Tráfico: ${traffic ? Math.round(traffic/1000)+'K' : 'N/A'} | Contacto: ${contact?.name || 'N/A'} | Email: ${email || 'N/A'}`);
    await sleep(DOMAIN_DELAY_MS);
  }

  log(`Sesión completada — ${count} dominios procesados.`);
}

// ── Loop principal ────────────────────────────────────────────
async function main() {
  log("ADEQ Auto-Prospector iniciado.");

  // Reintentar login indefinidamente en lugar de crashear
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
      // Renovar token si está por vencer
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
      log(`Debug cfg — auto_prospecting_enabled: "${cfg.auto_prospecting_enabled}" | keys: ${Object.keys(cfg).join(",")}`);

      if (cfg.auto_prospecting_enabled !== "true") {
        // Esperar y volver a chequear
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      // Determinar inicio de sesión (si está vacío, usar ahora y guardarlo)
      let sessionStart;
      if (cfg.auto_session_start) {
        sessionStart = new Date(cfg.auto_session_start).getTime();
      } else {
        sessionStart = Date.now();
        await setConfigValue(token, "auto_session_start", new Date(sessionStart).toISOString());
      }

      // Verificar si ya pasaron los 45 minutos desde que se encendió
      if (Date.now() - sessionStart >= SESSION_LIMIT_MS) {
        log("⏱ Sesión expirada (45 min) — auto-apagando.");
        await setConfigValue(token, "auto_prospecting_enabled", "false");
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      // Correr sesión completa
      await runSession(token, cfg, sessionStart);

    } catch (err) {
      log(`❌ Error: ${err.message}`);
      await sleep(POLL_INTERVAL_MS);
    }
  }
}

main().catch(err => {
  console.error("Error fatal inesperado:", err);
  // No hacer process.exit — dejar que Railway decida
  setTimeout(() => main(), 30_000);
});
