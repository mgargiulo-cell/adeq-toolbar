// ============================================================
// ADEQ AUTO-PROSPECTOR — Motor de Descubrimiento Autónomo
// STATUS: HOLD — no conectado a index.js todavía
//
// Qué hace:
//   1. Toma seeds (dominios conocidos) desde Supabase
//   2. Expande vía SimilarWeb similar-sites
//   3. Filtra por tráfico, categoría, país
//   4. Descarta los que ya están en Monday o procesados
//   5. Devuelve lista de candidatos nuevos listos para enriquecer
//
// Para activar: importar runDiscovery() en index.js y llamarlo
// antes de fetchMondayDomains(), o como fuente alternativa.
// ============================================================

import fetch from "node-fetch";

const SUPABASE_URL      = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const RAPIDAPI_KEY      = process.env.RAPIDAPI_KEY; // misma key que usa la toolbar

const SIMILARWEB_HOST   = "similarweb-insights.p.rapidapi.com";
const RAPIDAPI_HEADERS  = () => ({
  "x-rapidapi-key":  RAPIDAPI_KEY,
  "x-rapidapi-host": SIMILARWEB_HOST,
});

// ── Configuración del motor ───────────────────────────────────
const DISCOVERY_CONFIG = {
  maxSeedsPerRun:      10,    // cuántos seeds expandir por sesión
  similarPerSeed:      10,    // cuántos similares pedir por seed
  minTraffic:          500_000, // visitas/mes mínimas
  maxCandidatesPerRun: 50,    // tope de candidatos nuevos por sesión
  delayBetweenSeeds:   3000,  // ms entre llamadas a SimilarWeb
};

// Dominios genéricos a ignorar (igual que en la toolbar)
const BLOCKLIST = new Set([
  "google.com","youtube.com","facebook.com","twitter.com","instagram.com",
  "wikipedia.org","amazon.com","reddit.com","tiktok.com","linkedin.com",
  "yahoo.com","bing.com","microsoft.com","apple.com","netflix.com",
  "ebay.com","chatgpt.com","openai.com","whatsapp.com","telegram.org",
  "pinterest.com","tumblr.com","quora.com","medium.com","wordpress.com",
]);

// ── Helpers ───────────────────────────────────────────────────
function cleanDomain(str) {
  return (str || "").toLowerCase()
    .replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "").trim();
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg) {
  console.log(`[DISCOVERY ${new Date().toISOString().substring(11,19)}] ${msg}`);
}

// ── Supabase: leer seeds configurados ────────────────────────
// Seeds se guardan en toolbar_config con key = "discovery_seeds"
// Valor: JSON array de dominios, ej: ["marca.com","deportes.mx"]
async function getSeeds(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_config?key=eq.discovery_seeds&select=value`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
    );
    const rows = await res.json();
    const raw  = rows?.[0]?.value || "[]";
    const seeds = JSON.parse(raw);
    return Array.isArray(seeds) ? seeds.map(cleanDomain).filter(Boolean) : [];
  } catch {
    return [];
  }
}

// ── Supabase: dominios ya procesados ─────────────────────────
async function getProcessedDomains(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_import_queue?select=domain&expires_at=gt.${encodeURIComponent(new Date().toISOString())}`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
    );
    const rows = await res.json();
    return new Set(rows.map(r => r.domain));
  } catch {
    return new Set();
  }
}

// ── Monday: dominios ya en el board ──────────────────────────
async function getMondayDomains(token) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/toolbar_historial?select=domain`,
      { headers: { "apikey": SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
    );
    const rows = await res.json();
    return new Set(rows.map(r => cleanDomain(r.domain)));
  } catch {
    return new Set();
  }
}

// ── SimilarWeb: sitios similares a un dominio ────────────────
async function getSimilarSites(domain) {
  try {
    const res = await fetch(
      `https://${SIMILARWEB_HOST}/similar-sites?domain=${encodeURIComponent(domain)}`,
      {
        headers: RAPIDAPI_HEADERS(),
        signal: AbortSignal.timeout(8000),
      }
    );
    if (!res.ok) return [];
    const data = await res.json();

    // Normalizar distintos formatos de respuesta
    const sites = data?.similar_sites || data?.SimilarSites || data?.sites || [];
    return sites.map(s => ({
      domain:  cleanDomain(s.domain || s.Domain || s.url || ""),
      visits:  Math.round(s.visits || s.Visits || s.traffic || 0),
    })).filter(s => s.domain && s.domain.length > 3);
  } catch {
    return [];
  }
}

// ── SimilarWeb: tráfico de un dominio ────────────────────────
async function getTraffic(domain) {
  try {
    const res = await fetch(
      `https://${SIMILARWEB_HOST}/traffic?domain=${encodeURIComponent(domain)}`,
      {
        headers: RAPIDAPI_HEADERS(),
        signal: AbortSignal.timeout(8000),
      }
    );
    if (!res.ok) return null;
    const data = await res.json();
    if (data.error || !data.Visits) return null;
    return {
      visits:       Math.round(data.Visits || 0),
      category:     data.Category     || "",
      globalRank:   data.GlobalRank   || null,
      topCountries: (data.TopCountries || []).slice(0, 3).map(c => ({
        code:  (c.CountryCode || c.Country || "").toUpperCase().slice(0, 2),
        share: parseFloat(c.CountryShare || c.Share || 0),
      })),
    };
  } catch {
    return null;
  }
}

// ── Motor principal de descubrimiento ────────────────────────
export async function runDiscovery(token) {
  log("Iniciando motor de descubrimiento...");

  const [seeds, processed, mondayDomains] = await Promise.all([
    getSeeds(token),
    getProcessedDomains(token),
    getMondayDomains(token),
  ]);

  if (!seeds.length) {
    log("No hay seeds configurados. Agregá dominios en toolbar_config con key=discovery_seeds");
    return [];
  }

  log(`${seeds.length} seeds disponibles. Expandiendo...`);

  const candidates  = new Map(); // domain → { visits, category, globalRank, topCountries, sourceSeeed }
  const excluded    = new Set([...processed, ...mondayDomains, ...BLOCKLIST]);
  const seedsToUse  = seeds.slice(0, DISCOVERY_CONFIG.maxSeedsPerRun);

  for (const seed of seedsToUse) {
    log(`  Expandiendo desde: ${seed}`);
    const similars = await getSimilarSites(seed);

    for (const site of similars.slice(0, DISCOVERY_CONFIG.similarPerSeed)) {
      if (!site.domain) continue;
      if (excluded.has(site.domain)) continue;
      if (candidates.has(site.domain)) continue;
      if (site.domain.includes("google") || site.domain.includes("facebook")) continue;

      candidates.set(site.domain, {
        visits:       site.visits,
        sourceSeed:   seed,
        category:     "",
        globalRank:   null,
        topCountries: [],
      });
    }

    await sleep(DISCOVERY_CONFIG.delayBetweenSeeds);
  }

  log(`${candidates.size} candidatos antes de filtrar por tráfico.`);

  // Filtrar por tráfico mínimo — verificar los que no tienen dato de visitas
  const verified = [];
  for (const [domain, data] of candidates) {
    if (verified.length >= DISCOVERY_CONFIG.maxCandidatesPerRun) break;

    let traffic = data;

    // Si el dato de visitas de similar-sites es 0 o no confiable, verificar con /traffic
    if (!data.visits || data.visits < 1000) {
      const fresh = await getTraffic(domain);
      if (!fresh || fresh.visits < DISCOVERY_CONFIG.minTraffic) continue;
      traffic = { ...data, ...fresh };
      await sleep(1500);
    } else if (data.visits < DISCOVERY_CONFIG.minTraffic) {
      continue;
    }

    verified.push({
      domain,
      visits:       traffic.visits,
      category:     traffic.category     || "",
      globalRank:   traffic.globalRank   || null,
      topCountries: traffic.topCountries || [],
      sourceSeed:   data.sourceSeed,
    });

    log(`  ✓ ${domain} — ${Math.round(traffic.visits / 1000)}K visitas (seed: ${data.sourceSeed})`);
  }

  log(`Descubrimiento completo — ${verified.length} candidatos nuevos válidos.`);
  return verified;
}

// ── Cómo conectar a index.js cuando esté listo ────────────────
// En index.js, dentro de runSession(), reemplazar:
//
//   const allDomains = await fetchMondayDomains(monday_api_key);
//
// Por:
//
//   const mondayDomains    = await fetchMondayDomains(monday_api_key);
//   const discoveredDomains = await runDiscovery(token);
//   const allDomains = [...new Set([...mondayDomains, ...discoveredDomains.map(d => d.domain)])];
//
// Y agregar a toolbar_config en Supabase:
//   key = "discovery_seeds"
//   value = '["sitio1.com","sitio2.com","sitio3.com"]'
// ============================================================
