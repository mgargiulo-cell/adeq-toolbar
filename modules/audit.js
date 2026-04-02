// ============================================================
// ADEQ TOOLBAR — Módulo Auditoría Técnica v4
// Detecta los 15 partners de ADEQ Media
// ============================================================

const ALL_PARTNERS = [
  {
    name: "Sparteo",
    sigs: ["sparteo.com", "bid.sparteo.com", "sploadernetworkid", "window.sploader"],
    win:  ["spLoader"],
  },
  {
    name: "Seedtag",
    sigs: ["seedtag.com", "t.seedtag.com", "sdk.seedtag.com"],
    win:  [],
  },
  {
    name: "Taboola",
    sigs: ["taboola.com", "cdn.taboola.com", "_taboola", "tb_loader_script"],
    win:  ["_taboola"],
  },
  {
    name: "Missena",
    sigs: ["missena.io", "missena.com", "ad.missena.io"],
    win:  [],
  },
  {
    name: "Viads",
    sigs: ["viads.com", "viads.net"],
    win:  [],
  },
  {
    name: "MGID",
    sigs: ["mgid.com", "jsc.mgid.com", "lp.mgid.com"],
    win:  [],
  },
  {
    name: "Clever Advertising",
    sigs: ["cleverwebserver.com", "cleveradvertising.com", "clever-core", "clevercoreloader"],
    win:  [],
  },
  {
    name: "Vidoomy",
    sigs: ["vidoomy.com", "ads.vidoomy.com"],
    win:  [],
  },
  {
    name: "Vidverto",
    sigs: ["vidverto.io", "vidverto.com"],
    win:  [],
  },
  {
    name: "Ezoic",
    sigs: ["ezoic.net", "ezoic.com", "ezodn.com", "ezoimgfmt", "__ezoic", "ezoicads"],
    win:  ["__ezoic_head_0", "ezstandalone"],
  },
  {
    name: "Clickio",
    sigs: ["clickio.com", "clickiocdn.com", "s.clickiocdn.com"],
    win:  [],
  },
  {
    name: "360Playvid",
    sigs: ["360playvid.com", "360playvid.info", "slidepleer"],
    win:  [],
  },
  {
    name: "Truvid",
    sigs: ["truvid.com", "trvdp.com", "cnt.trvdp.com"],
    win:  [],
  },
  {
    name: "Optad360",
    sigs: ["optad360.io", "optad360.com", "get.optad360.io"],
    win:  [],
  },
  {
    name: "Embi Media",
    sigs: ["embi.media", "cdn.embi.media"],
    win:  [],
  },
  {
    name: "Snigel",
    sigs: ["snigelweb.com", "cdn.snigelweb.com", "adengine"],
    win:  [],
  },
];

export async function runAudit(baseUrl, monthlyTraffic = 0) {
  const [adsTxt, detected] = await Promise.all([
    checkAdsTxt(baseUrl),
    detectAdsTech(),
  ]);

  const techStack  = detected;
  const revenueGap = estimateRevenueGap(monthlyTraffic, techStack, adsTxt);
  const allPartners = ALL_PARTNERS.map(p => ({ name: p.name, found: techStack.includes(p.name) }));

  return {
    adsTxt,
    techStack,
    revenueGap,
    allPartners,
    summary: buildSummary(adsTxt, techStack, revenueGap),
  };
}

async function checkAdsTxt(baseUrl) {
  try {
    const url      = new URL("/ads.txt", baseUrl).href;
    const response = await fetch(url, { method: "GET", signal: AbortSignal.timeout(5000) });
    if (!response.ok) return { exists: false, entries: 0, hasGoogle: false };

    const text  = await response.text();
    const lines = text.split("\n").filter(l => l.trim() && !l.startsWith("#"));
    return {
      exists:    true,
      entries:   lines.length,
      hasGoogle: text.toLowerCase().includes("google.com/doubleclick") || text.toLowerCase().includes("adx.google.com"),
      hasEzoic:  text.toLowerCase().includes("ezoic"),
      raw:       text.substring(0, 500),
    };
  } catch {
    return { exists: false, entries: 0, hasGoogle: false };
  }
}

async function detectAdsTech() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (!tab?.id) return [];
    const [result] = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      func:   detectAdsFromDOM,
      args:   [ALL_PARTNERS.map(p => ({ name: p.name, sigs: p.sigs, win: p.win || [] }))],
    });
    return result?.result || [];
  } catch {
    return [];
  }
}

// Se inyecta en la página — busca en HTML, recursos cargados y window
function detectAdsFromDOM(partners) {
  const html = document.documentElement.innerHTML.toLowerCase();

  // URLs de todos los recursos cargados (scripts, XHR, fetch, etc.)
  const resources = (performance.getEntriesByType?.("resource") || [])
    .map(r => r.name.toLowerCase());

  // Scripts cargados dinámicamente que ya están en el DOM
  const scriptSrcs = [...document.querySelectorAll("script[src]")]
    .map(s => (s.src || s.getAttribute("src") || "").toLowerCase());

  // Texto completo de scripts inline
  const inlineScripts = [...document.querySelectorAll("script:not([src])")]
    .map(s => s.textContent.toLowerCase())
    .join(" ");

  const allText = [html, inlineScripts].join(" ");
  const allUrls = [...new Set([...resources, ...scriptSrcs])];

  const detected = [];

  for (const p of partners) {
    // 1. Buscar firmas en HTML + scripts inline
    const inHtml = p.sigs.some(s => allText.includes(s.toLowerCase()));

    // 2. Buscar firmas en URLs de recursos cargados
    const inResources = p.sigs.some(sig =>
      allUrls.some(url => url.includes(sig.toLowerCase()))
    );

    // 3. Buscar variables en window (detecta carga dinámica post-render)
    const inWindow = p.win.some(key => {
      try { return typeof window[key] !== "undefined"; } catch { return false; }
    });

    if (inHtml || inResources || inWindow) detected.push(p.name);
  }

  return detected;
}

function estimateRevenueGap(monthlyTraffic, techStack, adsTxt) {
  if (!monthlyTraffic || monthlyTraffic < 10000) return { percent: 0, usd: 0, factors: [] };

  let gapPercent = 0;
  const factors  = [];

  if (!adsTxt.exists) {
    gapPercent += 25; factors.push("Sin ads.txt: ~25% pérdida de fill rate");
  } else if (adsTxt.entries < 5) {
    gapPercent += 15; factors.push("ads.txt incompleto: pocos SSPs");
  }
  if (techStack.length <= 1) {
    gapPercent += 30; factors.push("Sin header bidding: solo 1 red");
  }
  if (techStack.includes("Google AdSense") && !techStack.includes("Google Ad Manager (GAM)")) {
    gapPercent += 20; factors.push("AdSense vs GAM: menor yield por impresión");
  }

  const rpmBase    = 2.5;
  const currentRev = (monthlyTraffic / 1000) * rpmBase;
  const gapUsd     = Math.round((currentRev * gapPercent) / 100);

  return {
    percent:          Math.min(gapPercent, 80),
    usd:              gapUsd,
    factors,
    currentEstimated: Math.round(currentRev),
  };
}

function buildSummary(adsTxt, techStack, revenueGap) {
  const items = [];

  if (!adsTxt.exists) {
    items.push({ status: "error", text: "Sin ads.txt" });
  } else if (adsTxt.entries < 5) {
    items.push({ status: "warn", text: `ads.txt incompleto (${adsTxt.entries} entradas)` });
  } else {
    items.push({ status: "ok", text: "ads.txt OK" });
  }

  if (techStack.length === 0) {
    items.push({ status: "warn", text: "No se detectó tecnología de anuncios" });
  } else {
    items.push({ status: "ok", text: techStack.join(", ") });
  }

  if (revenueGap.percent > 0) {
    items.push({ status: "warn", text: `Revenue Gap: ~${revenueGap.percent}% (≈$${revenueGap.usd}/mes)` });
  }

  return items;
}
