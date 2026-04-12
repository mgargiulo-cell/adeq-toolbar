// ============================================================
// ADEQ TOOLBAR — Módulo Similar Sites en Cascada
// Caché de 60 días en Supabase para no repetir calls a SimilarWeb.
// ============================================================

import { CONFIG }                              from "../config.js";
import { getSimilarCache, saveSimilarCache }   from "./supabase.js";

const RAPIDAPI_BASE = "https://similarweb-insights.p.rapidapi.com";

/**
 * Obtiene sitios similares a un dominio.
 * Primero consulta caché de 60 días, si no hay hace la llamada real.
 */
export async function getSimilarSites(domain) {
  const clean = domain.replace(/^www\./, "").toLowerCase();

  // Caché primero
  const cached = await getSimilarCache(clean);
  if (cached) return cached;

  try {
    const response = await fetch(`${RAPIDAPI_BASE}/similar-sites?domain=${encodeURIComponent(clean)}`, {
      method: "GET",
      headers: {
        "Content-Type":    "application/json",
        "x-rapidapi-key":  CONFIG.RAPIDAPI_KEY,
        "x-rapidapi-host": CONFIG.RAPIDAPI_TRAFFIC_HOST,
      },
      signal: AbortSignal.timeout(8000),
    });

    if (!response.ok) return [];
    const data = await response.json();
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
    if (!res.ok) return [];

    const html  = await res.text();
    const match = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]*?)<\/script>/);
    if (!match) return [];

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

    return [...new Set(domains)].slice(0, 20);
  } catch { return []; }
}

/**
 * Cascada de 2 niveles: semilla → similares → similares de similares.
 * Usa caché — solo llama a la API si el dominio no fue consultado en 60 días.
 */
export async function runCascade(seedDomain, onProgress) {
  const found   = new Map();
  const checked = new Set();

  onProgress?.({ status: "searching", domain: seedDomain, level: 1 });
  const [swLevel1, ssLevel1] = await Promise.all([
    getSimilarSites(seedDomain),
    getSimilarSitesFromSimilarSites(seedDomain),
  ]);
  // Merge: SimilarWeb objects take priority; add similarsites.com-only domains as minimal stubs
  const swMap = new Map(swLevel1.map(s => [s.domain, s]));
  for (const d of ssLevel1) {
    if (!swMap.has(d)) swMap.set(d, { domain: d, title: d, visits: 0, country: "N/A", countryCode: "", globalRank: null, description: "", favicon: "" });
  }
  const level1 = [...swMap.values()];
  checked.add(seedDomain);

  for (const site of level1) {
    if (!found.has(site.domain)) {
      found.set(site.domain, { ...site, level: 1 });
      onProgress?.({ status: "found", site, level: 1 });
    }
  }

  for (const site of level1.slice(0, 10)) {
    if (checked.has(site.domain)) continue;
    checked.add(site.domain);

    // Solo duerme si no hay caché (para no saturar la API en calls reales)
    const cached = await getSimilarCache(site.domain.replace(/^www\./, "").toLowerCase());
    if (!cached) await sleep(1500);

    onProgress?.({ status: "searching", domain: site.domain, level: 2 });
    const level2 = await getSimilarSites(site.domain);

    for (const s2 of level2) {
      if (!found.has(s2.domain) && s2.domain !== seedDomain) {
        found.set(s2.domain, { ...s2, level: 2 });
        onProgress?.({ status: "found", site: s2, level: 2 });
      }
    }
  }

  return [...found.values()].sort((a, b) => b.visits - a.visits);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
