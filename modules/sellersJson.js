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
  // ── SSPs / Ad exchanges descubiertos (well-known.dev + top SSP lists) ──
  // Estos son los GIGANTES — Improve Digital tiene ~20K publishers, casi
  // toda la web europea pasa por ahí.
  { name: "Improve Digital (19537 pubs)", url: "https://improvedigital.com/sellers.json" },
  { name: "TripleLift (3788 pubs)",       url: "https://triplelift.com/sellers.json" },
  { name: "PubMatic (3393 pubs)",         url: "https://pubmatic.com/sellers.json" },
  { name: "OpenX (2827 pubs)",            url: "https://openx.com/sellers.json" },
  { name: "Sharethrough (2722 pubs)",     url: "https://sharethrough.com/sellers.json" },
  { name: "Setupad (1878 pubs)",          url: "https://setupad.com/sellers.json" },
  { name: "Index Exchange (1695 pubs)",   url: "https://www.indexexchange.com/sellers.json" },
  { name: "Rubicon Project (1468 pubs)",  url: "https://rubiconproject.com/sellers.json" },
  { name: "Smart AdServer (1331 pubs)",   url: "https://smartadserver.com/sellers.json" },
  { name: "LKQD (1329 pubs)",             url: "https://lkqd.com/sellers.json" },
  { name: "GumGum (464 pubs)",            url: "https://gumgum.com/sellers.json" },
  { name: "Adform (308 pubs)",            url: "https://adform.com/sellers.json" },
  { name: "OneTag (307 pubs)",            url: "https://onetag.com/sellers.json" },
  { name: "Kargo (302 pubs)",             url: "https://kargo.com/sellers.json" },
  { name: "EMX Digital (167 pubs)",       url: "https://emxdigital.com/sellers.json" },
  { name: "Undertone (139 pubs)",         url: "https://undertone.com/sellers.json" },
  { name: "Yieldlab (70 pubs)",           url: "https://yieldlab.net/sellers.json" },
  { name: "Contextweb (26 pubs)",         url: "https://contextweb.com/sellers.json" },
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
  // SIN headers Accept — algunos servidores rechazan preflight CORS si lo mandamos.
  // Timeout 30s para sellers.json grandes (Truvid 689KB, Vidoomy 592KB).
  let res;
  try {
    res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: AbortSignal.timeout(30000),
    });
  } catch (e) {
    // Errores de red comunes: TypeError "Failed to fetch" (CORS), DNS, timeout.
    throw new Error(`Network error: ${e.message || e.name}. ${e.name === "TimeoutError" ? "Timeout 30s." : "Posible CORS/DNS/SSL — el servidor puede estar bloqueando fetch desde extensions."}`);
  }
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); }
  catch { throw new Error(`Response no es JSON válido (¿HTML 404 o respuesta binaria?)`); }
  const sellers = Array.isArray(data?.sellers) ? data.sellers : [];
  // Filtro flexible: acepta PUBLISHER en cualquier capitalización ("Publisher", "publisher").
  // INTERMEDIARY/BOTH del owner se descartan.
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

// Parsea el ads.txt de un sitio y devuelve dominios únicos de ad systems.
// Formato ads.txt: "domain.com, accountId, RELATIONSHIP, certAuthority"
// Útil para descubrir nuevas empresas con sellers.json.
export async function fetchAdsTxtSystems(siteUrl) {
  const url = (() => {
    try { return new URL("/ads.txt", siteUrl).href; } catch { return null; }
  })();
  if (!url) throw new Error("URL inválida");
  const res = await fetch(url, {
    method: "GET",
    redirect: "follow",
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} en ${url}`);
  const text = await res.text();
  // HTML check (404 page que devuelve 200)
  if (/^\s*<!doctype|<html/i.test(text.trim())) {
    throw new Error("Respuesta no es ads.txt (parece HTML 404)");
  }
  const systems = new Set();
  text.split("\n").forEach(line => {
    const clean = line.split("#")[0].trim();
    if (!clean) return;
    const parts = clean.split(",").map(s => s.trim());
    if (parts.length < 3) return;
    const domain = normalizeDomain(parts[0]);
    // Skip subdomain-style ads.txt (CNAME/subdomain= que no son ad systems)
    if (!domain || /^(subdomain|contact|cname)/i.test(domain)) return;
    systems.add(domain);
  });
  return [...systems].sort();
}

// Probe en paralelo: para cada dominio ad-system, prueba si /sellers.json existe
// y devuelve {domain, url, pubs}. Skip los que fallan.
// Concurrency limitada para no quemar la red (8 simultáneos).
export async function probeSellersJson(domains, onProgress) {
  const results = [];
  const CONCURRENCY = 8;
  let completed = 0;
  const probe = async (domain) => {
    const tryUrls = [
      `https://${domain}/sellers.json`,
      `https://www.${domain}/sellers.json`,
    ];
    for (const url of tryUrls) {
      try {
        const res = await fetch(url, {
          method: "GET", redirect: "follow",
          signal: AbortSignal.timeout(8000),
          headers: { "Accept": "application/json" },
        });
        if (!res.ok) continue;
        const text = await res.text();
        if (/^\s*<!doctype|<html/i.test(text.trim())) continue;
        let data;
        try { data = JSON.parse(text); } catch { continue; }
        const pubs = (data?.sellers || []).filter(s => (s.seller_type || "").toUpperCase() === "PUBLISHER").length;
        if (pubs > 0) return { domain, url: res.url || url, pubs };
      } catch {}
    }
    return null;
  };
  // Worker pool
  const queue = [...domains];
  const workers = Array(Math.min(CONCURRENCY, queue.length)).fill(0).map(async () => {
    while (queue.length > 0) {
      const domain = queue.shift();
      if (!domain) break;
      const r = await probe(domain);
      completed++;
      if (onProgress) onProgress(completed, domains.length, r);
      if (r) results.push(r);
    }
  });
  await Promise.all(workers);
  results.sort((a, b) => b.pubs - a.pubs);
  return results;
}
