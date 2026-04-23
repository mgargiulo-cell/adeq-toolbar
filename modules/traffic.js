// ============================================================
// ADEQ TOOLBAR — Módulo de Tráfico v3.2
// Caché de 60 días + contador de requests + límites de RapidAPI
// ============================================================

import { CONFIG }                            from "../config.js";
import { getTrafficCache, saveTrafficCache } from "./supabase.js";
import { callProxy }                         from "./apiProxy.js";

// Every RapidAPI call now goes through the Edge Function proxy (keys stay server-side)
async function rapidFetch(path) {
  const res = await callProxy("rapidapi", path, { method: "GET" });
  return { ok: res.ok, status: res.status, data: res.data };
}

// ISO 3166-1 numeric → alpha-2 (top countries we care about for monetization)
const ISO_NUMERIC_TO_ALPHA2 = {
  4:"AF", 8:"AL", 12:"DZ", 32:"AR", 36:"AU", 40:"AT", 50:"BD", 56:"BE", 68:"BO",
  76:"BR", 100:"BG", 124:"CA", 152:"CL", 156:"CN", 158:"TW", 170:"CO", 188:"CR",
  191:"HR", 192:"CU", 196:"CY", 203:"CZ", 208:"DK", 214:"DO", 218:"EC", 222:"SV",
  233:"EE", 246:"FI", 250:"FR", 268:"GE", 276:"DE", 300:"GR", 320:"GT", 344:"HK",
  348:"HU", 352:"IS", 356:"IN", 360:"ID", 364:"IR", 368:"IQ", 372:"IE", 376:"IL",
  380:"IT", 388:"JM", 392:"JP", 398:"KZ", 404:"KE", 410:"KR", 414:"KW", 422:"LB",
  428:"LV", 440:"LT", 442:"LU", 458:"MY", 484:"MX", 504:"MA", 528:"NL", 554:"NZ",
  566:"NG", 578:"NO", 586:"PK", 591:"PA", 600:"PY", 604:"PE", 608:"PH", 616:"PL",
  620:"PT", 642:"RO", 643:"RU", 682:"SA", 688:"RS", 702:"SG", 703:"SK", 705:"SI",
  710:"ZA", 724:"ES", 752:"SE", 756:"CH", 764:"TH", 780:"TT", 788:"TN", 792:"TR",
  804:"UA", 818:"EG", 826:"GB", 840:"US", 858:"UY", 862:"VE", 704:"VN",
};

const CODE_TO_NAME = {
  AR:"Argentina", BR:"Brazil", MX:"Mexico", CO:"Colombia", CL:"Chile", PE:"Peru",
  EC:"Ecuador", VE:"Venezuela", UY:"Uruguay", PY:"Paraguay", BO:"Bolivia", ES:"Spain",
  US:"United States", CA:"Canada", GB:"United Kingdom", FR:"France", DE:"Germany",
  IT:"Italy", PT:"Portugal", NL:"Netherlands", BE:"Belgium", CH:"Switzerland",
  AT:"Austria", SE:"Sweden", NO:"Norway", DK:"Denmark", FI:"Finland", GR:"Greece",
  PL:"Poland", HU:"Hungary", CZ:"Czech Republic", RO:"Romania", IE:"Ireland",
  IL:"Israel", AE:"UAE", SA:"Saudi Arabia", EG:"Egypt", MA:"Morocco", TR:"Turkey",
  IN:"India", JP:"Japan", KR:"South Korea", CN:"China", SG:"Singapore", MY:"Malaysia",
  ID:"Indonesia", PH:"Philippines", TH:"Thailand", VN:"Vietnam", AU:"Australia",
  NZ:"New Zealand", ZA:"South Africa", NG:"Nigeria", RU:"Russia", UA:"Ukraine",
};

// Normaliza el shape variable que devuelve RapidAPI: a veces { CountryCode: 32 } (numeric ISO),
// a veces { CountryCode: "AR" } (alpha2), a veces { country: "Argentina" } (full name).
function normalizeCountry(c) {
  let raw = c.CountryCode ?? c.country_code ?? c.countryCode ?? c.Country ?? c.country ?? "";
  let code = "";
  if (typeof raw === "number") {
    code = ISO_NUMERIC_TO_ALPHA2[raw] || "";
  } else if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (/^\d+$/.test(trimmed)) {
      code = ISO_NUMERIC_TO_ALPHA2[parseInt(trimmed)] || "";
    } else if (trimmed.length === 2) {
      code = trimmed.toUpperCase();
    }
  }
  if (!code) return null;
  const name = CODE_TO_NAME[code] || c.CountryName || c.country_name || code;
  const shareRaw = c.CountryShare ?? c.Share ?? c.share ?? c.Value ?? c.value ?? 0;
  const share = typeof shareRaw === "number" ? shareRaw : parseFloat(shareRaw) || 0;
  return { code, name, share };
}

// ── fetchTopCountries — endpoint separado ─────────────────────
async function fetchTopCountries(domain) {
  try {
    const res = await rapidFetch(`/countries?domain=${encodeURIComponent(domain)}`);
    if (!res.ok) return [];
    const data = res.data;
    const list = Array.isArray(data)
      ? data
      : (data.TopCountries || data.Countries || data.countries || data.top_countries || []);
    return list.slice(0, 3).map(normalizeCountry).filter(Boolean);
  } catch {
    return [];
  }
}

// ── fetchEngagement — endpoint secundario para PagePerVisit ───
// Muchas cuentas tienen engagement metrics en /engagement aunque no en /traffic
async function fetchEngagement(domain) {
  try {
    const res = await rapidFetch(`/engagement?domain=${encodeURIComponent(domain)}`);
    if (!res.ok) return null;
    const data = res.data || {};
    const ppv = data.PagePerVisit || data.PagesPerVisit || data.pagesPerVisit || data.pages_per_visit || null;
    if (!ppv) return null;
    return parseFloat(ppv);
  } catch { return null; }
}

// Estimación de PagePerVisit según categoría del sitio — último recurso
// Basado en industry benchmarks públicos (SimilarWeb, Statista, ComScore)
const CATEGORY_PAGES_PER_VISIT = {
  news:          4.5,
  sports:        5.0,
  entertainment: 5.5,
  gambling:      6.0,
  finance:       2.8,
  technology:    2.5,
  business:      2.5,
  health:        3.0,
  travel:        4.0,
  automotive:    3.5,
  food:          3.8,
  other:         3.0,
};

function estimatePagesPerVisit(category) {
  const c = (category || "").toLowerCase();
  for (const [key, ppv] of Object.entries(CATEGORY_PAGES_PER_VISIT)) {
    if (c.includes(key)) return ppv;
  }
  return CATEGORY_PAGES_PER_VISIT.other;
}

// ── getTraffic ────────────────────────────────────────────────
export async function getTraffic(domain) {
  const cleanDomain = domain
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/$/, "");

  // Caché primero (60 días)
  const cached = await getTrafficCache(cleanDomain);
  if (cached) {
    // Si el caché no tiene geo, intentar el endpoint /countries una vez y rellenar
    if (!cached.topCountries?.length) {
      const fresh = await fetchTopCountries(cleanDomain);
      if (fresh.length) {
        cached.topCountries = fresh;
        await saveTrafficCache(cleanDomain, cached);
      }
    }
    return cached;
  }

  try {
    // ── Primario: /traffic — tiene Visits + PagePerVisit (engagement metrics) ──
    const response = await rapidFetch(`/traffic?domain=${encodeURIComponent(cleanDomain)}`);

    if (response.ok) {
      const data = response.data || {};
      if (!data.error && data.Visits) {
        const visits    = Math.round(data.Visits || 0);
        let pagesPerVisit  = data.PagePerVisit || data.PagesPerVisit || null;
        let ppvSource      = pagesPerVisit ? "traffic" : null;

        // Fallback 1: endpoint /engagement (suele tener metrics que /traffic omite)
        if (!pagesPerVisit) {
          const engagementPPV = await fetchEngagement(cleanDomain);
          if (engagementPPV) { pagesPerVisit = engagementPPV; ppvSource = "engagement"; }
        }

        // Top countries — desde respuesta principal o endpoint separado
        let topCountries = [];
        const inlineCountries = data.TopCountries || data.Countries || data.countries;
        if (Array.isArray(inlineCountries) && inlineCountries.length) {
          topCountries = inlineCountries.slice(0, 3).map(c => ({
            code:  (c.CountryCode || c.Country || "").toUpperCase().slice(0, 2),
            name:  c.CountryName || c.CountryCode || "",
            share: parseFloat(c.CountryShare || c.Share || 0),
          })).filter(c => c.code.length === 2);
        } else {
          topCountries = await fetchTopCountries(cleanDomain);
        }

        const category = data.Category || "";
        // Fallback 2: estimar por categoría si ningún endpoint nos dió metrics
        if (!pagesPerVisit && category) {
          pagesPerVisit = estimatePagesPerVisit(category);
          ppvSource     = "estimated";
        }

        const pageViews = pagesPerVisit ? Math.round(visits * pagesPerVisit) : null;

        const result = {
          visits,
          pagesPerVisit:  pagesPerVisit ? Math.round(pagesPerVisit * 10) / 10 : null,
          pageViews,
          monthly:        pageViews || visits,
          rawVisits:      visits,
          noPageViewData: !pagesPerVisit,
          ppvSource,                         // "traffic" | "engagement" | "estimated" | null
          estimatedPages: ppvSource === "estimated",
          category,
          categoryRank:   data.CategoryRank || null,
          globalRank:     data.GlobalRank   || null,
          topCountries,
          tags:           data.Tags         || [],
        };
        await saveTrafficCache(cleanDomain, result);
        return result;
      }
    }

    // ── Fallback: /similar-sites ──────────────────────────────
    const fallback = await rapidFetch(`/similar-sites?domain=${encodeURIComponent(cleanDomain)}`);
    if (!fallback.ok) return null;
    const fData = fallback.data || {};
    const visits        = Math.round(fData.Visits || 0);
    let pagesPerVisit   = fData.PagePerVisit || fData.PagesPerVisit || null;
    let ppvSource       = pagesPerVisit ? "similar-sites" : null;

    if (!pagesPerVisit) {
      const engagementPPV = await fetchEngagement(cleanDomain);
      if (engagementPPV) { pagesPerVisit = engagementPPV; ppvSource = "engagement"; }
    }
    const category = fData.Category || "";
    if (!pagesPerVisit && category) {
      pagesPerVisit = estimatePagesPerVisit(category);
      ppvSource     = "estimated";
    }

    const pageViews    = pagesPerVisit ? Math.round(visits * pagesPerVisit) : null;
    const topCountries = await fetchTopCountries(cleanDomain);
    const result = {
      visits,
      pagesPerVisit:  pagesPerVisit ? Math.round(pagesPerVisit * 10) / 10 : null,
      pageViews,
      monthly:        pageViews || visits,
      rawVisits:      visits,
      noPageViewData: !pagesPerVisit,
      ppvSource,
      estimatedPages: ppvSource === "estimated",
      category,
      categoryRank:   fData.CategoryRank || null,
      globalRank:     fData.GlobalRank   || null,
      tags: [], topCountries,
    };
    await saveTrafficCache(cleanDomain, result);
    return result;

  } catch (err) {
    console.error("Error obteniendo tráfico:", err);
    return null;
  }
}

export function formatTraffic(num) {
  if (!num || num === 0) return "0";
  if (num >= 1_000_000_000) return `${Math.round(num / 1_000_000_000)}B`;
  if (num >= 1_000_000)     return `${Math.round(num / 1_000_000)}M`;
  if (num >= 1_000)         return `${Math.round(num / 1_000)}K`;
  return String(num);
}

export function formatVisits(num) { return formatTraffic(num); }

export function passesTrafficFilter(pageViews) {
  return pageViews >= CONFIG.MIN_TRAFFIC;
}
