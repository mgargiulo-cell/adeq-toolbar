// ============================================================
// ADEQ TOOLBAR — Módulo de Tráfico v3.2
// Caché de 90 días + contador de requests + límites de RapidAPI
// ============================================================

import { CONFIG }                            from "../config.js";
import { getTrafficCache, saveTrafficCache, getDomainGeo, setDomainGeo } from "./supabase.js";
import { checkDomainBlocked } from "./blocklist.js";
import { callProxy }                         from "./apiProxy.js";

// Token de auth — se setea desde popup.js al iniciar sesión, lo necesitamos
// para llamar al cache GEO en Supabase. Si no está, los lookups silently fallan.
let _authToken = null;
export function setTrafficAuthToken(t) { _authToken = t || null; }

const SOURCE_CONFIDENCE = {
  "similarweb": 9, "radar": 8, "footer-address": 7, "og-locale": 6,
  "lang-region": 5, "phone-code": 5, "currency": 4, "tld": 3,
};

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

// TLD → ISO alpha-2 (último recurso cuando SimilarWeb no devuelve país)
const TLD_TO_ALPHA2 = {
  ar:"AR", mx:"MX", co:"CO", cl:"CL", br:"BR", pe:"PE", ec:"EC", ve:"VE", uy:"UY",
  py:"PY", bo:"BO", es:"ES", us:"US", ca:"CA", uk:"GB", fr:"FR", de:"DE", it:"IT",
  pt:"PT", nl:"NL", be:"BE", ch:"CH", at:"AT", se:"SE", no:"NO", dk:"DK", fi:"FI",
  pl:"PL", gr:"GR", hu:"HU", cz:"CZ", ro:"RO", ie:"IE", lu:"LU", il:"IL", ae:"AE",
  sa:"SA", eg:"EG", ma:"MA", tr:"TR", in:"IN", jp:"JP", kr:"KR", cn:"CN", sg:"SG",
  my:"MY", id:"ID", ph:"PH", th:"TH", vn:"VN", au:"AU", nz:"NZ", za:"ZA", ng:"NG",
  ru:"RU", ua:"UA", tw:"TW", hk:"HK", pk:"PK", bd:"BD",
};

function inferCountryFromTLD(domain) {
  if (!domain) return null;
  const d = domain.toLowerCase();
  // Check 2-part TLDs first (.com.ar, .co.uk, etc.)
  const parts = d.split(".");
  if (parts.length >= 3) {
    const last2 = parts.slice(-2).join(".");
    const map2 = { "com.ar":"AR","com.mx":"MX","com.br":"BR","com.co":"CO","com.pe":"PE",
                   "com.uy":"UY","com.ec":"EC","com.ve":"VE","com.bo":"BO","com.es":"ES",
                   "co.uk":"GB","org.uk":"GB","ac.uk":"GB","co.za":"ZA","com.au":"AU",
                   "com.cn":"CN","co.in":"IN","co.jp":"JP","co.kr":"KR","com.tr":"TR",
                   "com.eg":"EG","com.sa":"SA","com.ng":"NG","com.ph":"PH","com.vn":"VN" };
    if (map2[last2]) return map2[last2];
  }
  const tld = parts[parts.length - 1];
  return TLD_TO_ALPHA2[tld] || null;
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

// ─────────────────────────────────────────────────────────────────
// Adaptador del shape nuevo de website-insights (Mayo 2026 en adelante).
// La nueva API anida todo y cambia tipos vs la vieja similarweb-insights:
//   - Visits: objeto {YYYY-MM-DD: number} → tomamos el mes más reciente
//   - PagePerVisit: vive en data.Traffic.Engagement.PagesPerVisit
//   - TopCountries: TopCountryShares {US: 0.761, ...} → array tradicional
//   - Category: data.WebsiteDetails.Category
//   - GlobalRank/CategoryRank: dentro de data.Rank
// El resto del código sigue usando el shape viejo, así que normalizamos acá.
// ─────────────────────────────────────────────────────────────────
function normalizeWebsiteInsightsResponse(data) {
  if (!data || typeof data !== "object") return data;
  // Detectar shape nueva: tiene Traffic + WebsiteDetails como objetos top-level
  const isNewShape = (data.Traffic && typeof data.Traffic === "object")
                  || (data.WebsiteDetails && typeof data.WebsiteDetails === "object");
  if (!isNewShape) return data; // shape vieja, devolver tal cual

  const out = { ...data };

  // Visits: tomar el valor del mes más reciente del objeto histórico
  const visitsObj = data.Traffic?.Visits;
  if (visitsObj && typeof visitsObj === "object" && !Array.isArray(visitsObj)) {
    const dates = Object.keys(visitsObj).sort().reverse();
    if (dates.length) out.Visits = parseFloat(visitsObj[dates[0]]) || 0;
  } else if (typeof visitsObj === "number") {
    out.Visits = visitsObj;
  }

  // PagesPerVisit (lo que la toolbar mostraba como "estimado conservador")
  const ppv = data.Traffic?.Engagement?.PagesPerVisit;
  if (ppv != null) {
    out.PagePerVisit  = ppv;
    out.PagesPerVisit = ppv;
  }
  // BounceRate y TimeOnSite por si los queremos exponer en futuro
  if (data.Traffic?.Engagement?.BounceRate != null)  out.BounceRate  = data.Traffic.Engagement.BounceRate;
  if (data.Traffic?.Engagement?.TimeOnSite != null)  out.AvgVisitDuration = data.Traffic.Engagement.TimeOnSite;

  // TopCountries: convertir {US: 0.761, CA: 0.045} a [{CountryCode, Share}, ...]
  const tcs = data.Traffic?.TopCountryShares;
  if (tcs && typeof tcs === "object" && !Array.isArray(tcs)) {
    out.TopCountries = Object.entries(tcs)
      .map(([code, share]) => ({ CountryCode: code, Share: parseFloat(share) || 0 }))
      .sort((a, b) => b.Share - a.Share)
      .slice(0, 5);
  }

  // Category — la nueva API la pone dentro de WebsiteDetails
  if (data.WebsiteDetails?.Category && !out.Category) {
    out.Category = data.WebsiteDetails.Category;
  }

  // Ranks
  if (data.Rank?.GlobalRank != null && out.GlobalRank == null) {
    out.GlobalRank = data.Rank.GlobalRank;
  }
  if (data.Rank?.CategoryRank?.Rank != null && out.CategoryRank == null) {
    out.CategoryRank = data.Rank.CategoryRank.Rank;
  }

  return out;
}

// Acepta cualquiera de las variantes que SimilarWeb-Insights ha usado en
// distintas versiones del endpoint para la misma métrica.
function extractPPV(data) {
  if (!data || typeof data !== "object") return null;
  const candidates = [
    // Variantes principales (SimilarWeb classic + website-insights)
    data.PagePerVisit, data.PagesPerVisit, data.pagesPerVisit, data.pages_per_visit,
    data.PageViewsPerVisit, data.AvgPagesPerVisit, data.avg_pages_per_visit,
    data.AvgPageViews, data.AveragePagesPerVisit, data.AveragePageViews,
    data.PagesViewed, data.PageViews,
    // Anidados comunes
    data.Engagement?.PagePerVisit, data.Engagement?.PagesPerVisit, data.Engagement?.AvgPagesPerVisit,
    data.engagement?.pagesPerVisit, data.engagement?.pages_per_visit, data.engagement?.PagePerVisit,
    data.engagements?.pagesPerVisit, data.engagements?.PagePerVisit,
    data.Metrics?.PagePerVisit, data.Metrics?.PagesPerVisit,
    data.metrics?.pagesPerVisit, data.metrics?.PagePerVisit,
    // Algunos planes anidan todo dentro de "data" o "result"
    data.data?.PagePerVisit, data.data?.PagesPerVisit, data.data?.pagesPerVisit,
    data.result?.PagePerVisit, data.result?.PagesPerVisit, data.result?.pagesPerVisit,
    // Snake case
    data.page_per_visit, data.avg_page_views,
  ];
  for (const v of candidates) {
    if (v != null && v !== "") {
      const n = parseFloat(v);
      if (!isNaN(n) && n > 0) return n;
    }
  }
  // Diagnóstico: si no encontramos PPV, guardar el response completo en chrome.storage
  // para inspección posterior. Console.warn también, por si miran ahí.
  console.warn("[Traffic] PPV no encontrado. Top-level keys:", Object.keys(data));
  for (const k of Object.keys(data)) {
    if (data[k] && typeof data[k] === "object" && !Array.isArray(data[k])) {
      console.warn(`[Traffic] keys de "${k}":`, Object.keys(data[k]));
    }
  }
  try {
    chrome.storage.local.set({
      _debug_last_api_response: {
        timestamp: new Date().toISOString(),
        topLevelKeys: Object.keys(data),
        fullResponse: data,
      }
    });
  } catch {}
  return null;
}

// ── fetchEngagement — endpoint secundario para PagePerVisit ───
async function fetchEngagement(domain) {
  try {
    const res = await rapidFetch(`/engagement?domain=${encodeURIComponent(domain)}`);
    if (!res.ok) return null;
    const ppv = extractPPV(res.data);
    if (!ppv) {
      console.log(`[Traffic] /engagement ${domain} sin PPV — keys: ${Object.keys(res.data || {}).join(",")}`);
    }
    return ppv;
  } catch { return null; }
}

// Estimación de PagePerVisit cuando SimilarWeb no devuelve dato real.
// Se usa como ÚLTIMO RECURSO. Cap conservador a 1.5 — sobrestimar páginas
// vistas infla el "qualifies" y termina pusheando basura a Monday. Mejor
// quedarse corto que largo.
const CATEGORY_PAGES_PER_VISIT = {
  news:          1.5,
  sports:        1.5,
  entertainment: 1.5,
  gambling:      1.5,
  finance:       1.3,
  technology:    1.3,
  business:      1.3,
  health:        1.4,
  travel:        1.5,
  automotive:    1.4,
  food:          1.4,
  other:         1.2,
};

function estimatePagesPerVisit(category) {
  const c = (category || "").toLowerCase();
  for (const [key, ppv] of Object.entries(CATEGORY_PAGES_PER_VISIT)) {
    if (c.includes(key)) return ppv;
  }
  return CATEGORY_PAGES_PER_VISIT.other;
}

// ── getTraffic ────────────────────────────────────────────────
export async function getTraffic(domain, opts = {}) {
  const { forceRefresh = false } = opts;
  const cleanDomain = domain
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/$/, "");

  // Pre-check blocklist (gratis, no consume API)
  const block = await checkDomainBlocked(cleanDomain, _authToken);
  if (block.blocked) {
    // Bloqueo silenciado — filtro funcionando, no es error
    return { visits: 0, pagesPerVisit: null, pageViews: 0, monthly: 0, rawVisits: 0, noPageViewData: true, ppvSource: null, estimatedPages: false, category: "", topCountries: [], blocked: true, blockedReason: block.reason };
  }

  // Caché primero (90 días) — salvo forceRefresh
  const cached = forceRefresh ? null : await getTrafficCache(cleanDomain);
  if (cached) {
    // Si el caché no tiene geo, inferir por TLD (sin gastar API)
    if (!cached.topCountries?.length) {
      const inferred = inferCountryFromTLD(cleanDomain);
      if (inferred) {
        cached.topCountries = [{ code: inferred, name: CODE_TO_NAME[inferred] || inferred, share: 0, source: "tld" }];
        await saveTrafficCache(cleanDomain, cached);
      }
    }
    return cached;
  }

  try {
    // ── Primario: /all-insights — devuelve TODO en una sola call ──
    // (Visits, Engagement, TopCountries, Category, GlobalRank, etc.)
    // Mucho más eficiente que /traffic + /countries + /engagement por separado
    // (esos paths NO EXISTEN en este vendor de RapidAPI — devolvían 404).
    let response = await rapidFetch(`/all-insights?domain=${encodeURIComponent(cleanDomain)}`);

    // Fallback: solo si fue 5xx/network. Si fue 4xx (404/403/429), NO insistir
    // — /traffic vive en el mismo plan y vendor: si /all-insights tiró 4xx,
    // /traffic también va a tirar 4xx y solo gastamos otra request facturada.
    let usedFallback = false;
    if (!response.ok && response.status >= 500) {
      console.log(`[Traffic] /all-insights ${cleanDomain} HTTP ${response.status} (5xx), fallback /traffic`);
      response = await rapidFetch(`/traffic?domain=${encodeURIComponent(cleanDomain)}`);
      usedFallback = true;
    } else if (!response.ok) {
      console.log(`[Traffic] /all-insights ${cleanDomain} HTTP ${response.status} — no fallback (4xx, evita gastar request)`);
    }

    if (response.ok) {
      // Normalizar el shape de la nueva API website-insights (Mayo 2026+)
      // que anida todo bajo Traffic/WebsiteDetails/Rank — el resto del código
      // sigue esperando el shape viejo (data.Visits como número, etc.)
      const data = normalizeWebsiteInsightsResponse(response.data || {});
      if (!data.error && data.Visits) {
        const visits    = Math.round(data.Visits || 0);
        let pagesPerVisit  = extractPPV(data);
        let ppvSource      = pagesPerVisit ? "all-insights" : null;
        if (!pagesPerVisit && !usedFallback) {
          console.log(`[Traffic] /all-insights ${cleanDomain} sin PPV — keys: ${Object.keys(data).join(",")}`);
        }

        // Top countries — el response de /all-insights ya las incluye
        let topCountries = [];
        const inlineCountries = data.TopCountries || data.Countries || data.countries;
        if (Array.isArray(inlineCountries) && inlineCountries.length) {
          topCountries = inlineCountries.slice(0, 3).map(normalizeCountry).filter(Boolean);
        }
        // Fallback 1: /countries endpoint (algunos planes lo exponen separado
        // del /all-insights — vale el costo del request extra para asegurar GEO)
        if (topCountries.length === 0) {
          const sep = await fetchTopCountries(cleanDomain);
          if (sep.length) {
            topCountries = sep;
            console.log(`[Traffic] ${cleanDomain} GEO via /countries: ${topCountries.map(c => c.code).join(",")}`);
          }
        }
        // Fallback 2: TLD del dominio (.com.ar → AR, .es → ES)
        if (topCountries.length === 0) {
          const inferred = inferCountryFromTLD(cleanDomain);
          if (inferred) {
            topCountries = [{ code: inferred, name: CODE_TO_NAME[inferred] || inferred, share: 0, source: "tld" }];
            console.log(`[Traffic] ${cleanDomain} GEO inferido por TLD: ${inferred}`);
          }
        }
        // Las señales de página (lang-region, og:locale, footer-address) se aplican
        // después en popup.js → enrichTrafficWithPageSignals(). Acá no las tenemos.

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
        // Persistir GEO al cache compartido (autopilot lo aprovecha también)
        if (topCountries[0]?.code && _authToken) {
          const src = topCountries[0]?.source === "tld" ? "tld" : "similarweb";
          setDomainGeo(_authToken, cleanDomain, topCountries[0].code, src, SOURCE_CONFIDENCE[src]).catch(() => {});
        }
        return result;
      }
    }

    // ── Fallback: /similar-sites — SOLO si los anteriores fueron 5xx/network ──
    // Mismo razonamiento: en 4xx el endpoint hermano va a tirar 4xx también.
    if (response.status >= 400 && response.status < 500) {
      console.log(`[Traffic] ${cleanDomain} sin /similar-sites fallback — primaria fue 4xx`);
      return null;
    }
    const fallback = await rapidFetch(`/similar-sites?domain=${encodeURIComponent(cleanDomain)}`);
    if (!fallback.ok) return null;
    const fData = normalizeWebsiteInsightsResponse(fallback.data || {});
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
    if (topCountries[0]?.code && _authToken) {
      const src = topCountries[0]?.source === "tld" ? "tld" : "similarweb";
      setDomainGeo(_authToken, cleanDomain, topCountries[0].code, src, SOURCE_CONFIDENCE[src]).catch(() => {});
    }
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
