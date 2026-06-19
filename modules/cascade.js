// ============================================================
// ADEQ TOOLBAR — Módulo Similar Sites en Cascada
// Caché de 60 días en Supabase para no repetir calls a SimilarWeb.
// ============================================================

import { CONFIG }                              from "../config.js";
import { getSimilarCache, saveSimilarCache }   from "./supabase.js";
import { callProxy }                           from "./apiProxy.js";

/**
 * Obtiene sitios similares a un dominio.
 * Primero consulta caché de 60 días, si no hay hace la llamada real.
 */
// ⚠️ DEPRECATED — esta función gasta 1 hit RapidAPI por dominio. Hoy NO se llama
// desde ningún lado (cascade usa solo getSimilarSitesFromSimilarSites que es
// scrape gratis). Mantengo la función exportada por si en futuro se quiere
// reactivar enrichment opcional, pero NO importarla sin necesidad porque
// si se llama, suma al counter SW.
export async function getSimilarSites(domain) {
  const clean = domain.replace(/^www\./, "").toLowerCase();

  // Caché primero
  const cached = await getSimilarCache(clean);
  if (cached) return cached;

  try {
    const response = await callProxy("rapidapi", `/similar-sites?domain=${encodeURIComponent(clean)}`, { method: "GET" });
    if (!response.ok) return [];
    const data = response.data || {};
    if (data.error || !data.SimilarSites) return [];

    const sites = data.SimilarSites
      .filter(site => (site.Visits || 0) >= CONFIG.MIN_TRAFFIC)
      .map(site => ({
        domain:      site.Domain,
        title:       site.Title || site.Domain,
        visits:      Math.round(site.Visits || 0),
        country:     site.TopCountry?.CountryName || "N/A",
        countryCode: site.TopCountry?.CountryCode || "",
        globalRank:  site.GlobalRank || null,
        description: site.Description || "",
        favicon:     site.Images?.Favicon || "",
      }));

    await saveSimilarCache(clean, sites);
    return sites;

  } catch (err) {
    console.error("Cascade error:", err);
    return [];
  }
}

/**
 * Obtiene similares desde similarsites.com (__NEXT_DATA__ embebido en HTML).
 * Devuelve array de dominios limpios.
 */
export async function getSimilarSitesFromSimilarSites(domain) {
  const clean = domain.replace(/^www\./, "").toLowerCase();
  try {
    const res = await fetch(`https://www.similarsites.com/site/${encodeURIComponent(clean)}`, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) {
      return [];
    }

    const html  = await res.text();
    const match = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/);
    if (!match) {
      return [];
    }

    const json  = JSON.parse(match[1]);

    // Recursive search for arrays containing domain-like objects
    const domains = [];
    function search(obj) {
      if (!obj || typeof obj !== "object") return;
      if (Array.isArray(obj)) {
        for (const item of obj) {
          if (item && typeof item === "object") {
            const d = item.domain || item.Domain || item.url || item.site || item.hostname;
            if (d && typeof d === "string" && d.includes(".") && !d.includes("/")) {
              const cleaned = d.replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "").toLowerCase();
              if (cleaned && cleaned !== clean) domains.push(cleaned);
            }
            search(item);
          }
        }
      } else {
        for (const val of Object.values(obj)) search(val);
      }
    }
    search(json);

    return [...new Set(domains)].slice(0, 60); // Maxi 2026-06-19: 20→60 (más similares, sigue gratis sin RapidAPI)
  } catch {
    // Silent fail — el runCascade aplica fallback automático a RapidAPI
    return [];
  }
}

/**
 * Cascade — descubre webs similares SIN GASTAR HITS DE RAPIDAPI.
 *
 * Decisión user 2026-05-08: no tiene sentido enriquecer visits/geo de sitios
 * similares hasta que el MB decida abrirlos para prospectar. Cascade solo
 * trae la lista de dominios desde el scrape público de similarsites.com
 * (gratis, sin API key). Cuando el MB haga click en uno, la pestaña abre y
 * el toolbar (Analysis) ahí sí dispara el getTraffic que enriquece el
 * dominio elegido.
 *
 * Resultado: 0 hits de RapidAPI por sesión de Cascade.
 */
// Maxi 2026-06-19: Cascade ahora hace 2 SALTOS (antes solo 1). Tras traer los
// similares del seed (nivel 1), expande con los similares-de-los-similares
// (nivel 2) usando el MISMO scrape gratis de similarsites.com → muchos más
// resultados SIN gastar créditos. Concurrencia limitada + tope total para no
// abusar del scrape. Cuando el MB hace click en uno, Analysis enriquece tráfico.
const CASCADE_MAX_TOTAL   = 150; // tope de dominios acumulados (alineado con CASCADE_LIMIT del popup)
const CASCADE_L2_SEEDS    = 18;  // cuántos dominios de nivel 1 se expanden a nivel 2
const CASCADE_L2_CONCURR  = 4;   // fetches simultáneos en nivel 2

export async function runCascade(seedDomain, onProgress) {
  const found = new Map();

  const addDomain = (d, level) => {
    if (!d || d === seedDomain || found.has(d)) return false;
    const stub = {
      domain:      d,
      title:       d,
      visits:      0,           // placeholder — se completa cuando user hace Analysis
      country:     "",
      countryCode: "",
      globalRank:  null,
      description: "",
      favicon:     `https://www.google.com/s2/favicons?domain=${d}&sz=32`,
      level,
    };
    found.set(d, stub);
    onProgress?.({ status: "found", site: stub, level });
    return true;
  };

  // ── Nivel 1: similares directos del seed ──────────────────────────────
  onProgress?.({ status: "searching", domain: seedDomain, level: 1 });
  let ssLevel1 = await getSimilarSitesFromSimilarSites(seedDomain);

  // FALLBACK: si similarsites.com cambió HTML o está caído, intentar RapidAPI.
  // Gasta 1 hit pero permite que el feature siga funcionando.
  if (!ssLevel1 || ssLevel1.length === 0) {
    try {
      const rapidApiSites = await getSimilarSites(seedDomain);
      ssLevel1 = (rapidApiSites || []).map(s => s.domain).filter(Boolean);
    } catch { /* silent — feature degrada naturalmente */ }
  }
  for (const d of ssLevel1) addDomain(d, 1);

  // ── Nivel 2: similares de los primeros N de nivel 1 (mismo scrape gratis) ──
  const l2Seeds = (ssLevel1 || []).slice(0, CASCADE_L2_SEEDS);
  for (let i = 0; i < l2Seeds.length && found.size < CASCADE_MAX_TOTAL; i += CASCADE_L2_CONCURR) {
    const batch = l2Seeds.slice(i, i + CASCADE_L2_CONCURR);
    onProgress?.({ status: "searching", domain: batch[0], level: 2 });
    const results = await Promise.all(
      batch.map(seed => getSimilarSitesFromSimilarSites(seed).catch(() => []))
    );
    for (const list of results) {
      for (const d of (list || [])) {
        if (found.size >= CASCADE_MAX_TOTAL) break;
        addDomain(d, 2);
      }
    }
    await sleep(150); // cortesía con el scrape público
  }

  // Devolvemos sin sort por visits (no las tenemos) — nivel 1 primero, luego alfabético.
  return [...found.values()].sort((a, b) => (a.level - b.level) || a.domain.localeCompare(b.domain));
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
