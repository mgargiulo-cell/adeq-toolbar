// ============================================================
// ADEQ TOOLBAR — Módulo de Tráfico v3.2
// Caché de 60 días + contador de requests + límites de RapidAPI
// ============================================================

import { CONFIG }                            from "../config.js";
import { getTrafficCache, saveTrafficCache } from "./supabase.js";

const RAPIDAPI_BASE = "https://similarweb-insights.p.rapidapi.com";
// Headers como función para leer CONFIG en tiempo de ejecución (ya cargado desde Supabase)
const getHeaders = () => ({
  "Content-Type":    "application/json",
  "x-rapidapi-key":  CONFIG.RAPIDAPI_KEY,
  "x-rapidapi-host": CONFIG.RAPIDAPI_TRAFFIC_HOST,
});

// ── Contador mensual de requests reales a RapidAPI ────────────
async function incrementApiCounter() {
  const key    = `sw_calls_${new Date().toISOString().substring(0, 7)}`;
  const stored = await chrome.storage.local.get(key);
  const count  = (stored[key] || 0) + 1;
  await chrome.storage.local.set({ [key]: count });
  return count;
}

export async function getMonthlyApiCalls() {
  const key    = `sw_calls_${new Date().toISOString().substring(0, 7)}`;
  const stored = await chrome.storage.local.get(key);
  return stored[key] || 0;
}

// Guarda los límites reales del plan tal como los informa RapidAPI
async function saveApiLimits(remaining, limit) {
  if (remaining == null || limit == null) return;
  await chrome.storage.local.set({
    sw_limit:     parseInt(limit),
    sw_remaining: parseInt(remaining),
  });
}

export async function getApiLimits() {
  const stored = await chrome.storage.local.get(["sw_limit", "sw_remaining"]);
  return {
    limit:     stored.sw_limit     ?? null,
    remaining: stored.sw_remaining ?? null,
  };
}

// ── fetchTopCountries — endpoint separado ─────────────────────
async function fetchTopCountries(domain) {
  try {
    const res = await fetch(
      `${RAPIDAPI_BASE}/countries?domain=${encodeURIComponent(domain)}`,
      { method: "GET", headers: getHeaders(), signal: AbortSignal.timeout(5000) }
    );
    if (!res.ok) return [];
    const data = await res.json();

    // Normalizar distintos formatos posibles de la API
    const list = Array.isArray(data)
      ? data
      : (data.TopCountries || data.Countries || data.countries || []);

    return list.slice(0, 3).map(c => ({
      code:  (c.CountryCode || c.Country || c.countryCode || c.country || "").toUpperCase().slice(0, 2),
      name:  c.CountryName  || c.CountryCode || c.Country || "",
      share: parseFloat(c.CountryShare || c.Share || c.share || 0),
    })).filter(c => c.code.length === 2);
  } catch {
    return [];
  }
}

// ── getTraffic ────────────────────────────────────────────────
export async function getTraffic(domain) {
  const cleanDomain = domain
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/$/, "");

  // Caché primero (60 días)
  const cached = await getTrafficCache(cleanDomain);
  if (cached) return cached;

  try {
    // ── Primario: /traffic — tiene Visits + PagePerVisit (engagement metrics) ──
    const response = await fetch(
      `${RAPIDAPI_BASE}/traffic?domain=${encodeURIComponent(cleanDomain)}`,
      { method: "GET", headers: getHeaders(), signal: AbortSignal.timeout(8000) }
    );

    saveApiLimits(
      response.headers.get("X-RateLimit-Requests-Remaining"),
      response.headers.get("X-RateLimit-Requests-Limit")
    );
    incrementApiCounter();

    if (response.ok) {
      const data = await response.json();
      if (!data.error && data.Visits) {
        const visits        = Math.round(data.Visits || 0);
        const pagesPerVisit = data.PagePerVisit || data.PagesPerVisit || null;
        const pageViews     = pagesPerVisit ? Math.round(visits * pagesPerVisit) : null;

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

        const result = {
          visits,
          pagesPerVisit:  pagesPerVisit ? Math.round(pagesPerVisit * 10) / 10 : null,
          pageViews,
          monthly:        pageViews || visits,
          rawVisits:      visits,
          noPageViewData: !pagesPerVisit,
          category:       data.Category     || "",
          categoryRank:   data.CategoryRank || null,
          globalRank:     data.GlobalRank   || null,
          topCountries,
          tags:           data.Tags         || [],
          estimatedPages: false,
        };
        await saveTrafficCache(cleanDomain, result);
        return result;
      }
    }

    // ── Fallback: /similar-sites ──────────────────────────────
    const fallback = await fetch(
      `${RAPIDAPI_BASE}/similar-sites?domain=${encodeURIComponent(cleanDomain)}`,
      { method: "GET", headers: getHeaders(), signal: AbortSignal.timeout(8000) }
    );
    if (!fallback.ok) return null;

    saveApiLimits(
      fallback.headers.get("X-RateLimit-Requests-Remaining"),
      fallback.headers.get("X-RateLimit-Requests-Limit")
    );

    const fData         = await fallback.json();
    const visits        = Math.round(fData.Visits || 0);
    const pagesPerVisit = fData.PagePerVisit || fData.PagesPerVisit || null;
    const pageViews     = pagesPerVisit ? Math.round(visits * pagesPerVisit) : null;
    const topCountries  = await fetchTopCountries(cleanDomain);
    const result = {
      visits,
      pagesPerVisit:  pagesPerVisit ? Math.round(pagesPerVisit * 10) / 10 : null,
      pageViews,
      monthly:        pageViews || visits,
      rawVisits:      visits,
      noPageViewData: !pagesPerVisit,
      category:       fData.Category    || "",
      categoryRank:   fData.CategoryRank || null,
      globalRank:     fData.GlobalRank   || null,
      tags: [], estimatedPages: false, topCountries,
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
