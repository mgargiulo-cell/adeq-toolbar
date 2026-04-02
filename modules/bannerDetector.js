// ============================================================
// ADEQ TOOLBAR — Banner Detector v5
// Detecta los 8 formatos de publicidad que trabaja ADEQ Media:
//   1. In Image        — ads sobre imágenes
//   2. Video Instream  — video player dentro de párrafos
//   3. Video Slider    — video/banner flotante en esquina con close
//   4. In Text Banners — banners entre párrafos del artículo
//   5. Side Rail       — banners en los laterales
//   6. Interstitial    — pop-up entre artículos
//   7. Sticky Header   — banner fijo en la parte superior con X
//   8. Sticky Footer   — banner fijo en la parte inferior con X
// ============================================================

export async function detectBanners(tabId) {
  try {
    const [{ result }] = await chrome.scripting.executeScript({
      target: { tabId },
      func: scanPageForAds,
    });
    return result;
  } catch (err) {
    return { summary: [], total: 0, error: err.message };
  }
}

function scanPageForAds() {

  const detected = {
    inImage:      false,
    videoInstream: false,
    videoSlider:  false,
    inText:       false,
    sideRail:     false,
    interstitial: false,
    stickyHeader: false,
    stickyFooter: false,
  };
  // detalle específico por formato — se setea una sola vez (sin duplicados)
  const details = {};

  // ── Helpers ────────────────────────────────────────────────
  function getStyle(el) {
    try { return window.getComputedStyle(el); } catch { return {}; }
  }
  function rect(el) {
    try { return el.getBoundingClientRect(); } catch { return null; }
  }
  function isVisible(el) {
    const s = getStyle(el);
    if (s.display === "none" || s.visibility === "hidden" || s.opacity === "0") return false;
    const r = rect(el);
    return r && (r.width > 0 || r.height > 0);
  }
  function hasCloseButton(el) {
    // Busca X, ×, close, cerrar dentro del elemento o sus hijos directos
    const CLOSE = /^[×x✕✖close cerrar]$|close|cerrar/i;
    const children = el.querySelectorAll("button, span, div, a, i");
    for (const c of children) {
      const txt = (c.innerText || c.textContent || "").trim();
      const cls = (c.className || "").toLowerCase();
      const id  = (c.id || "").toLowerCase();
      if (CLOSE.test(txt) || cls.includes("close") || cls.includes("cerrar") ||
          id.includes("close") || c.getAttribute("aria-label")?.toLowerCase().includes("close")) {
        return true;
      }
    }
    return false;
  }

  const vw = window.innerWidth  || 1280;
  const vh = window.innerHeight || 800;

  // ── Recursos cargados (performance API) ───────────────────
  const loadedUrls = (performance.getEntriesByType?.("resource") || [])
    .map(r => r.name.toLowerCase());

  function urlContains(...terms) {
    return terms.some(t => loadedUrls.some(u => u.includes(t)));
  }

  // ── HTML completo (lowercase) ──────────────────────────────
  const html = document.documentElement.innerHTML.toLowerCase();

  // ── IAB sizes ─────────────────────────────────────────────
  const IAB = [[728,90],[970,90],[970,250],[320,50],[320,100],
               [300,250],[336,280],[300,600],[160,600],[120,600]];
  function isIabSize(el) {
    const r = rect(el);
    if (!r) return false;
    const w = Math.round(r.width), h = Math.round(r.height);
    return IAB.some(([aw,ah]) => Math.abs(w-aw) <= 5 && Math.abs(h-ah) <= 5);
  }

  // ── AD SELECTORS genéricos ────────────────────────────────
  const AD_SEL = [
    ".adsbygoogle","ins.adsbygoogle","[data-ad-slot]",
    "[id^='div-gpt-ad']","[id^='google_ads']",
    "[id^='taboola']","[class*='taboola']",
    "[id*='mgid']","[class*='mgid']",
    "[class*='ad-unit']","[class*='adunit']","[class*='ad-slot']",
    "[class*='advertisement']","[class*='banner-ad']",
  ];
  const adEls = new Set();
  AD_SEL.forEach(sel => {
    try { document.querySelectorAll(sel).forEach(el => adEls.add(el)); } catch {}
  });

  // ============================================================
  // 1. IN IMAGE — ads superpuestos sobre imágenes
  // ============================================================
  // Proveedores conocidos de in-image
  const IN_IMAGE_SIGS = ["gumgum","inimage","in-image","inimg","visual-revenue","vibrant"];
  const inImageByUrl  = IN_IMAGE_SIGS.some(s => html.includes(s) || urlContains(s));

  if (!inImageByUrl) {
    // Buscar: elemento ad con position:absolute dentro de figure/picture/div con img
    document.querySelectorAll("figure, [class*='image'], [class*='photo'], [class*='img']").forEach(container => {
      if (!isVisible(container)) return;
      container.querySelectorAll("[class*='ad'],[id*='ad'],ins,.adsbygoogle").forEach(adEl => {
        const s = getStyle(adEl);
        if (s.position === "absolute" && isVisible(adEl)) {
          detected.inImage = true;
        }
      });
    });
  } else {
    detected.inImage = true;
  }

  // ============================================================
  // 2. VIDEO INSTREAM — player dentro de párrafos del artículo
  // ============================================================
  const VIDEO_PROVIDERS = [
    "truvid","trvdp","vidoomy","vidverto","360playvid","playvid",
    "sparteo","missena","jwplatform","jwplayer","brightcove","kaltura",
    "dailymotion","vimeo","youtube","flowplayer","videojs",
  ];
  const VIDEO_EL_SEL = [
    "video","[class*='jw-']","[id*='jwplayer']",
    ".video-js","[id*='videojs']",
    "[data-account][data-player]",
    "[class*='vidoomy']","[class*='vidverto']","[class*='truvid']",
    "[src*='trvdp.com']","[src*='vidoomy.com']","[src*='vidverto.io']",
    "[src*='360playvid']","[class*='video-player']","[id*='video-player']",
    "[class*='instream']","[class*='in-stream']",
  ];

  // Detectar si hay un video player dentro del contenido del artículo
  const articleSelectors = [
    "article","[class*='article-body']","[class*='post-content']",
    "[class*='entry-content']","[class*='content-body']","main",
  ];
  let articleEl = null;
  for (const sel of articleSelectors) {
    const el = document.querySelector(sel);
    if (el) { articleEl = el; break; }
  }

  const videoProviderInHtml = VIDEO_PROVIDERS.some(p => html.includes(p)) ||
                               urlContains(...VIDEO_PROVIDERS);

  if (videoProviderInHtml) {
    // Verificar que hay un video element visible (no slider)
    VIDEO_EL_SEL.forEach(sel => {
      try {
        [...document.querySelectorAll(sel)].forEach(el => {
          if (!isVisible(el)) return;
          const s   = getStyle(el);
          const pos = s.position;
          // Instream = NO fijo en esquina
          if (pos !== "fixed" && pos !== "sticky") {
            const r = rect(el);
            if (r && r.width > 100 && r.height > 60) {
              detected.videoInstream = true;
            }
          }
        });
      } catch {}
    });
  }

  // ============================================================
  // 3. VIDEO SLIDER — flotante en esquina con botón close/X
  // ============================================================
  const SLIDER_SEL = [
    "[class*='floating-video']","[class*='sticky-video']",
    "[class*='video-float']","[class*='float-video']",
    "[class*='corner-video']","[class*='slide-in-video']",
    "[class*='pinned-video']","[class*='video-slider']",
    "[class*='video-widget']","[id*='floating-video']",
    "[class*='vdo']","[id*='vdo']",
  ];

  SLIDER_SEL.forEach(sel => {
    try {
      [...document.querySelectorAll(sel)].forEach(el => {
        if (isVisible(el)) detected.videoSlider = true;
      });
    } catch {}
  });

  // Detectar por posición: elemento fijo pequeño en esquina con close button
  if (!detected.videoSlider) {
    document.querySelectorAll("*").forEach(el => {
      const s = getStyle(el);
      if (s.position !== "fixed" && s.position !== "sticky") return;
      if (!isVisible(el)) return;
      const r = rect(el);
      if (!r) return;
      const isSmall  = r.width < vw * 0.45 && r.height < vh * 0.45 && r.width > 80;
      const isCorner = (r.bottom >= vh - 250) &&
                       (r.right >= vw - 600 || r.left <= 200);
      const hasVideo = !!el.querySelector("video, iframe, [class*='video'], [class*='player']");
      const hasClose = hasCloseButton(el);
      if (isSmall && isCorner && (hasVideo || hasClose)) {
        detected.videoSlider = true;
        if (!details.videoSlider) {
          const side = r.right >= vw - 300 ? "derecha" : "izquierda";
          const vert = r.bottom >= vh - 200 ? "inferior" : "superior";
          details.videoSlider = `Esquina ${vert} ${side}`;
        }
      }
    });
  }

  // VDO.AI = siempre slider flotante
  if (/vdo\.ai|powered by vdo/i.test(html) || urlContains("vdo.ai","a.vdo.ai")) {
    detected.videoSlider = true;
  }

  // ============================================================
  // 4. IN TEXT BANNERS — banners entre párrafos
  // ============================================================
  // Buscar elementos de anuncio que estén entre párrafos <p> del artículo
  const checkInText = (container) => {
    if (!container) return;
    const children = [...container.children];
    let pCount  = 0, adCount = 0;
    children.forEach(child => {
      if (child.tagName === "P") pCount++;
      if (adEls.has(child) || isIabSize(child) ||
          (child.tagName === "INS") ||
          /adsbygoogle|ad-unit|advertisement|banner/i.test(child.className + child.id)) {
        if (isVisible(child)) adCount++;
      }
    });
    if (pCount >= 2 && adCount >= 1) detected.inText = true;
  };

  if (articleEl) checkInText(articleEl);
  if (!detected.inText) {
    document.querySelectorAll("article, .post-content, .entry-content, main").forEach(checkInText);
  }

  // Fallback: ins/div ad con IAB size visible y no en header/footer
  if (!detected.inText) {
    document.querySelectorAll("ins.adsbygoogle, [data-ad-slot]").forEach(el => {
      if (!isVisible(el)) return;
      const r = rect(el);
      if (!r) return;
      const notInHeaderFooter = r.top > 150 && r.top < document.body.scrollHeight * 0.9;
      const s = getStyle(el);
      if (notInHeaderFooter && s.position !== "fixed" && s.position !== "sticky") {
        detected.inText = true;
      }
    });
  }

  // ============================================================
  // 5. SIDE RAIL — banners en laterales (fijos o dentro de sidebar)
  // ============================================================
  // a) Elementos fijos/sticky en zona lateral
  document.querySelectorAll("*").forEach(el => {
    const s = getStyle(el);
    if (s.position !== "fixed" && s.position !== "sticky") return;
    if (!isVisible(el)) return;
    const r = rect(el);
    if (!r) return;
    const isOnSide  = r.right < vw * 0.22 || r.left > vw * 0.78;
    const isTall    = r.height > 150;
    const isNarrow  = r.width < vw * 0.25;
    if (isOnSide && isTall && isNarrow) {
      detected.sideRail = true;
      if (!details.sideRail) {
        details.sideRail = r.left > vw * 0.6 ? "Lateral derecho" : "Lateral izquierdo";
      }
    }
  });

  // b) Sidebar con anuncios
  if (!detected.sideRail) {
    const SIDEBAR_SEL = [
      "aside","[class*='sidebar']","[id*='sidebar']",
      "[class*='side-rail']","[class*='side-column']","[id*='side-rail']",
    ];
    SIDEBAR_SEL.forEach(sel => {
      try {
        [...document.querySelectorAll(sel)].forEach(container => {
          if (!isVisible(container)) return;
          const r = rect(container);
          if (!r) return;
          const isOnSide = r.right < vw * 0.3 || r.left > vw * 0.7;
          const hasAd    = adEls.size > 0 &&
            [...adEls].some(ad => container.contains(ad) && isVisible(ad));
          const hasIab   = [...container.querySelectorAll("div, iframe, ins")]
            .some(el => isIabSize(el) && isVisible(el));
          if (isOnSide && (hasAd || hasIab)) {
            detected.sideRail = true;
            if (!details.sideRail) {
              details.sideRail = r.left > vw * 0.6 ? "Lateral derecho" : "Lateral izquierdo";
            }
          }
        });
      } catch {}
    });
  }

  // ============================================================
  // 6. INTERSTITIAL — pop-up/overlay entre artículos
  // ============================================================
  const INTER_SEL = [
    "[id*='interstitial']","[class*='interstitial']",
    "[id*='overlay-ad']","[class*='overlay-ad']",
    "[id*='modal-ad']","[class*='modal-ad']",
    "[id*='popup-ad']","[class*='popup-ad']",
    "[id*='takeover']","[class*='takeover']",
    "[class*='ad-popup']","[id*='ad-popup']",
  ];
  INTER_SEL.forEach(sel => {
    try {
      if ([...document.querySelectorAll(sel)].some(isVisible)) {
        detected.interstitial = true;
      }
    } catch {}
  });

  // Detectar por posición: overlay grande fijo sobre toda la pantalla
  if (!detected.interstitial) {
    document.querySelectorAll("*").forEach(el => {
      const s = getStyle(el);
      if (s.position !== "fixed" && s.position !== "absolute") return;
      if (!isVisible(el)) return;
      const r = rect(el);
      if (!r) return;
      if (r.width >= vw * 0.75 && r.height >= vh * 0.55) {
        const cls = (el.className || "").toLowerCase();
        const id  = (el.id || "").toLowerCase();
        if (/ad|ads|promo|sponsor|popup|modal|overlay/.test(cls + id) ||
            adEls.has(el)) {
          detected.interstitial = true;
        }
      }
    });
  }

  // ============================================================
  // 7. STICKY HEADER — fijo en la parte SUPERIOR, sigue el scroll
  // ============================================================
  document.querySelectorAll("*").forEach(el => {
    const s = getStyle(el);
    if (s.position !== "fixed" && s.position !== "sticky") return;
    if (!isVisible(el)) return;
    const r = rect(el);
    if (!r) return;
    const isWide  = r.width >= vw * 0.5;
    const isAtTop = r.top >= -10 && r.top <= 130;
    const isShort = r.height < 200;
    const cls = (el.className || "").toLowerCase();
    const id  = (el.id || "").toLowerCase();
    const hasAdSig = /ad|ads|banner|advert|sponsor|promo/.test(cls + id) ||
                     adEls.has(el) || isIabSize(el) ||
                     !!el.querySelector(".adsbygoogle,[data-ad-slot],[id^='div-gpt-ad']");
    if (isWide && isAtTop && isShort && hasAdSig) {
      detected.stickyHeader = true;
      if (!details.stickyHeader) {
        details.stickyHeader = `Header fijo · ${Math.round(r.height)}px alto`;
      }
    }
  });

  // ============================================================
  // 8. STICKY FOOTER — fijo en la parte INFERIOR, sigue el scroll
  // ============================================================
  document.querySelectorAll("*").forEach(el => {
    const s = getStyle(el);
    if (s.position !== "fixed" && s.position !== "sticky") return;
    if (!isVisible(el)) return;
    const r = rect(el);
    if (!r) return;
    const isWide   = r.width >= vw * 0.5;
    const isAtBot  = r.bottom >= vh - 150 && r.bottom <= vh + 10;
    const isShort  = r.height < 200;
    const cls = (el.className || "").toLowerCase();
    const id  = (el.id || "").toLowerCase();
    const hasAdSig = /ad|ads|banner|advert|sponsor|promo/.test(cls + id) ||
                     adEls.has(el) || isIabSize(el) ||
                     !!el.querySelector(".adsbygoogle,[data-ad-slot],[id^='div-gpt-ad']");
    if (isWide && isAtBot && isShort && hasAdSig) {
      detected.stickyFooter = true;
      if (!details.stickyFooter) {
        details.stickyFooter = `Footer fijo · ${Math.round(r.height)}px alto`;
      }
    }
  });

  // ── Summary ───────────────────────────────────────────────
  const FORMAT_LABELS = {
    inImage:       { label: "In Image",        fallback: "Sobre imágenes" },
    videoInstream: { label: "Video Instream",  fallback: "Dentro del artículo" },
    videoSlider:   { label: "Video Slider",    fallback: "Flotante en esquina" },
    inText:        { label: "In Text Banners", fallback: "Entre párrafos" },
    sideRail:      { label: "Side Rail",       fallback: "Lateral" },
    interstitial:  { label: "Interstitial",    fallback: "Pop-up / overlay" },
    stickyHeader:  { label: "Sticky Header",   fallback: "Header fijo" },
    stickyFooter:  { label: "Sticky Footer",   fallback: "Footer fijo" },
  };

  const summary = Object.entries(FORMAT_LABELS)
    .filter(([key]) => detected[key])
    .map(([key, { label, fallback }]) => ({
      type:   label,
      detail: details[key] || fallback,
    }));

  return {
    ...detected,
    summary,
    total: summary.length,
    notes: summary.map(s => `${s.type}: ${s.detail}`).join(" · "),
  };
}
