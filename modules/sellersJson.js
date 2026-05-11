// ============================================================
// ADEQ Toolbar — sellers.json import
// ------------------------------------------------------------
// Cada empresa publisher/intermediary publica /sellers.json (estándar IAB)
// listando todos los sitios con los que trabaja. Es una mina de oro de leads:
// scrape el JSON, filtra los seller_type=PUBLISHER, encolá en csv_queue.
// ============================================================

// Lista baked-in de empresas conocidas. El user puede agregar más
// (se persisten en chrome.storage.local + Supabase toolbar_config).
// URLs verificadas 2026-05-11 (pubs = cantidad de seller_type=PUBLISHER al momento del check).
// Ordenadas por cantidad de publishers — los más grandes primero.
export const DEFAULT_SELLERS_COMPANIES = [
  { name: "Truvid (7143 pubs)",      url: "https://www.truvid.com/sellers.json" },
  { name: "Vidoomy (3433 pubs)",     url: "https://www.vidoomy.com/sellers.json" },
  { name: "AdPlus (2938 pubs)",      url: "https://ad.plus/sellers.json" },
  { name: "152Media (1846 pubs)",    url: "https://152media.info/sellers.json" },
  { name: "Mowplayer (1578 pubs)",   url: "https://mowplayer.com/sellers.json" },
  { name: "Seedtag (815 pubs)",      url: "https://www.seedtag.com/sellers.json" },
  { name: "Ogury (739 pubs)",        url: "https://ogury.com/sellers.json" },
  { name: "Teads (3729 pubs)",       url: "https://teads.tv/sellers.json" },
  { name: "VidCrunch (629 pubs)",    url: "https://vidcrunch.com/sellers.json" },
  { name: "EMBI Media (522 pubs)",   url: "https://embi-media.com/sellers.json" },
  { name: "Clickio (492 pubs)",      url: "https://clickio.com/sellers.json" },
  { name: "Vidverto (397 pubs)",     url: "https://vidverto.com/sellers.json" },
  { name: "Primis (231 pubs)",       url: "https://primis.tech/sellers.json" },
  { name: "VIAds (188 pubs)",        url: "https://viads.com/sellers.json" },
  { name: "Missena (165 pubs)",      url: "https://missena.com/sellers.json" },
  { name: "Flower-Ads (111 pubs)",   url: "https://flower-ads.com/sellers.json" },
  { name: "Carambola (27 pubs)",     url: "https://carambola.com/sellers.json" },
  // ── Agregadas user 2026-05-11 ────────────────────────────────
  { name: "The Moneytizer (6105 pubs)", url: "https://www.themoneytizer.com/sellers.json" },
  { name: "OptAd360 (2496 pubs)",       url: "https://optad360.com/sellers.json" },
  { name: "NSightVideo (1208 pubs)",    url: "https://nsightvideo.com/sellers.json" },
  { name: "Verve (635 pubs)",           url: "https://verve.com/sellers.json" },
  { name: "Spacefoot (277 pubs)",       url: "https://spacefoot.com/sellers.json" },
  { name: "360Playvid (213 pubs)",      url: "https://360playvid.com/sellers.json" },
  { name: "Seznam (115 pubs)",          url: "https://www.seznam.cz/sellers.json" },
];

// Check si los dominios ya fueron procesados antes (csv_queue + review_queue + historial).
// Devuelve Set de dominios ya conocidos. Útil para no re-encolar leads que ya pasamos.
export async function findKnownDomains(supabaseUrl, anonKey, accessToken, candidates) {
  if (!Array.isArray(candidates) || candidates.length === 0) return new Set();
  const known = new Set();
  const headers = { "apikey": anonKey, "Authorization": `Bearer ${accessToken}` };
  // PostgREST acepta in.(...) con URL larga, pero por las dudas batcheo 200 por query.
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
          `${supabaseUrl}/rest/v1/${table}?${col}=in.(${encodeURIComponent(inList)})&select=${col}`,
          { headers }
        );
        if (!res.ok) return;
        const rows = await res.json();
        rows.forEach(r => { if (r[col]) known.add(r[col].toLowerCase()); });
      } catch {}
    }));
  }
  return known;
}

// Empresas verificadas SIN sellers.json público (al 2026-05-11):
//   - Ezoic       (https://www.ezoic.com/sellers.json → 404)
//   - The Monetizer (themonetizer.com → 404)
//   - Playvid360  (DNS no resuelve)
// Si conseguís la URL real, agregalo desde el botón ✏️ Edit.

// Fetch sellers.json + extrae solo los PUBLISHER (skip INTERMEDIARY/BOTH del owner).
// Retorna lista de dominios deduplicados y normalizados.
export async function fetchSellersJson(url) {
  const res = await fetch(url, {
    method: "GET",
    redirect: "follow",
    signal: AbortSignal.timeout(15000),
    headers: { "Accept": "application/json" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch { throw new Error(`Response no es JSON válido (¿bloqueado por CORS o HTML 404?)`); }
  const sellers = Array.isArray(data?.sellers) ? data.sellers : [];
  // Solo PUBLISHER — INTERMEDIARY/BOTH son el dueño del archivo o brokers.
  const domains = sellers
    .filter(s => (s.seller_type || "").toUpperCase() === "PUBLISHER")
    .map(s => normalizeDomain(s.domain || ""))
    .filter(Boolean);
  // Dedupe
  return [...new Set(domains)];
}

function normalizeDomain(d) {
  if (!d) return "";
  return d.toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/.*$/, "")
    .trim();
}
