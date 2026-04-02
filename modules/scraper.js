// ============================================================
// ADEQ TOOLBAR — Módulo Email Scraper
// Fuentes (en orden de prioridad):
// 1. DOM de la página actual (footer, contacto, etc.)
// 2. website.informer.com/{domain}
// 3. Apollo.io API (decisor CEO/Owner)
// ============================================================

import { CONFIG } from "../config.js";

const IGNORE_DOMAINS = [
  "example.com","domain.com","yoursite.com","sentry.io",
  "google.com","w3.org","schema.org","cloudflare.com",
  ".png",".jpg",".gif",".svg",".webp",
];

// ============================================================
// 1. Scraping del DOM de la página activa
// ============================================================
export async function scrapeEmailsFromPage(tabId) {
  try {
    const [result] = await chrome.scripting.executeScript({
      target: { tabId },
      func: extractEmailsFromDOM,
    });
    return filterEmails(result?.result || []);
  } catch {
    return [];
  }
}

// Se ejecuta dentro de la página — función inyectada, debe ser self-contained
function extractEmailsFromDOM() {
  const found = new Set();
  // TLD max 6 chars + requiere no-letra después para evitar "comsoccer"
  const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6}(?=\s|$|[^a-zA-Z])/g;

  // ── Desofuscador de emails ─────────────────────────────────
  function deobfuscate(text) {
    if (!text) return "";

    // 1. Entidades HTML
    text = text
      .replace(/&#64;|&#x40;/gi, "@")
      .replace(/&#46;|&#x2e;/gi, ".");

    // 2. Variantes con corchetes / paréntesis / llaves
    text = text
      .replace(/\[\s*at\s*\]/gi,      "@")
      .replace(/\(\s*at\s*\)/gi,      "@")
      .replace(/\{\s*at\s*\}/gi,      "@")
      .replace(/\[\s*arroba\s*\]/gi,  "@")
      .replace(/\(\s*arroba\s*\)/gi,  "@")
      .replace(/\[\s*a\s*\]/gi,       "@")   // [a]
      .replace(/\[\s*dot\s*\]/gi,     ".")
      .replace(/\(\s*dot\s*\)/gi,     ".")
      .replace(/\{\s*dot\s*\}/gi,     ".")
      .replace(/\[\s*punto\s*\]/gi,   ".")
      .replace(/\(\s*punto\s*\)/gi,   ".")
      .replace(/\[\s*d\s*\]/gi,       ".");  // [d]

    // 3. Espacio AT espacio en contexto de email
    //    "nombre AT dominio DOT com" o "nombre AT dominio.com"
    text = text.replace(
      /([a-zA-Z0-9._%+\-]{2,})\s+(?:at|AT)\s+([a-zA-Z0-9\-]{2,}(?:(?:\s+(?:dot|DOT)\s+|\s*\.\s*)[a-zA-Z]{2,})+)/g,
      (_, local, domain) => {
        const cleanDomain = domain
          .replace(/\s+(?:dot|DOT)\s+/g, ".")
          .replace(/\s*\.\s*/g, ".");
        return `${local}@${cleanDomain}`;
      }
    );

    // 4. Guión bajo o "arroba" en español sin corchetes
    text = text.replace(/\barroba\b/gi, "@");
    text = text.replace(/\bpunto\b/gi, ".");

    return text;
  }

  function extractFrom(text) {
    const clean = deobfuscate(text);
    (clean.match(emailRegex) || []).forEach(e => found.add(e.toLowerCase().trim()));
  }

  // ── Fuentes de texto ──────────────────────────────────────
  // Texto visible
  extractFrom(document.body?.innerText || "");
  // HTML completo (captura emails en atributos ocultos, comentarios, data-*)
  extractFrom(document.body?.innerHTML || "");

  // mailto: links
  document.querySelectorAll("a[href^='mailto:']").forEach(a => {
    const mail = a.href.replace("mailto:", "").split("?")[0].trim();
    if (mail) found.add(mail.toLowerCase());
  });

  // Data attributes comunes para ofuscación JS
  document.querySelectorAll("[data-email],[data-mail],[data-contact],[data-correo]").forEach(el => {
    ["data-email","data-mail","data-contact","data-correo"].forEach(attr => {
      extractFrom(el.getAttribute(attr) || "");
    });
  });

  // Atributos title (tooltips con emails)
  document.querySelectorAll("[title]").forEach(el => {
    extractFrom(el.getAttribute("title") || "");
  });

  // Atributos aria-label (accesibilidad con emails)
  document.querySelectorAll("[aria-label]").forEach(el => {
    extractFrom(el.getAttribute("aria-label") || "");
  });

  return [...found];
}

// ============================================================
// 2. website.informer.com — teléfono + email
// ============================================================
export async function scrapeInformer(domain) {
  const cleanDomain = domain.replace(/^www\./, "");
  const url = `https://website.informer.com/${cleanDomain}`;

  try {
    const response = await fetch(url, {
      method: "GET",
      signal: AbortSignal.timeout(7000),
      headers: { "User-Agent": "Mozilla/5.0 (compatible; bot)" },
    });

    if (!response.ok) return { emails: [], phone: null };

    const html   = await response.text();
    const emails = filterEmails(extractEmailsFromText(html));

    // Extrae teléfono (formatos: +1 555..., (555)..., internacional)
    const phoneMatch = html.match(/(?:\+\d{1,3}[\s\-]?)?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4}/);
    const phone      = phoneMatch?.[0]?.trim() || null;

    return { emails, phone, source: "website.informer.com" };

  } catch {
    return { emails: [], phone: null };
  }
}

// ============================================================
// 3. Páginas de contacto del propio sitio
// ============================================================
export async function scrapeContactPages(baseUrl) {
  const paths = ["/contact", "/contact-us", "/contacto", "/about", "/about-us", "/legal", "/privacy", "/advertise", "/advertising"];
  const emails = new Set();

  for (const path of paths) {
    try {
      const url      = new URL(path, baseUrl).href;
      const response = await fetch(url, { method: "GET", signal: AbortSignal.timeout(4000) });
      if (!response.ok) continue;

      const html  = await response.text();
      const found = filterEmails(extractEmailsFromText(html));
      found.forEach(e => emails.add(e));

      if (emails.size > 0) break;
    } catch { /* página no disponible */ }
  }

  return [...emails];
}

// ============================================================
// 4. Verificación básica de email (sin API externa)
// ============================================================
export function validateEmailFormat(email) {
  const regex = /^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$/;
  return regex.test(email);
}

// ============================================================
// 5. Decisor — Apollo free (nombre) + RapidAPI enrichment (email)
// Paso 1: Apollo people search → first_name, last_name, title
// Paso 2: apollo-io-enrichment-data-scraper → email real
// ============================================================
export async function findDecisionMakerViaApollo(domain) {
  const cleanDomain = domain.replace(/^www\./, "");

  // ── Paso 1: Gemini con Google Search → nombre del decisor ────
  let firstName = "", lastName = "", title = "", linkedin = "";

  if (CONFIG.GEMINI_API_KEY) {
    console.log("[Apollo] Paso 1 vacío — intentando Gemini como fallback para:", cleanDomain);
    try {
      const prompt = `Find the CEO, founder, or main decision maker of the website "${cleanDomain}".
Return ONLY a JSON object with no extra text:
{"first_name":"John","last_name":"Smith","title":"CEO","linkedin":"https://linkedin.com/in/..."}
If not found, return: {"first_name":"","last_name":"","title":"","linkedin":""}`;

      const gRes = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${CONFIG.GEMINI_API_KEY}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            contents: [{ role: "user", parts: [{ text: prompt }] }],
            tools: [{ google_search: {} }],
            generationConfig: { temperature: 0.1, maxOutputTokens: 200 },
          }),
          signal: AbortSignal.timeout(12000),
        }
      );
      if (gRes.ok) {
        const gData = await gRes.json();
        const text  = gData?.candidates?.[0]?.content?.parts?.[0]?.text || "";
        const match = text.match(/\{[\s\S]*\}/);
        if (match) {
          const p = JSON.parse(match[0]);
          if (p.first_name && p.last_name) {
            firstName = p.first_name; lastName = p.last_name;
            title     = p.title     || title;
            linkedin  = p.linkedin  || linkedin;
            console.log("[Apollo] Gemini encontró:", firstName, lastName, title);
          }
        }
      }
    } catch (e) {
      console.warn("[Apollo] Gemini error:", e.message);
    }
  }

  if (!firstName || !lastName) {
    console.log("[Apollo] Sin nombre disponible para:", cleanDomain);
    return { error: "No se encontró decisor para " + cleanDomain };
  }

  console.log("[Apollo] Paso 1 OK →", firstName, lastName, title);
  console.log("[Apollo] Paso 2: llamando RapidAPI enrichment para", cleanDomain);

  try {
    const params = new URLSearchParams({ domain: cleanDomain, first_name: firstName, last_name: lastName });
    const res = await fetch(
      `https://apollo-io-enrichment-data-scraper.p.rapidapi.com/email-finder.php?${params}`,
      {
        method: "GET",
        headers: {
          "x-rapidapi-key":  CONFIG.RAPIDAPI_KEY,
          "x-rapidapi-host": "apollo-io-enrichment-data-scraper.p.rapidapi.com",
          "Content-Type":    "application/json",
        },
        signal: AbortSignal.timeout(10000),
      }
    );

    if (!res.ok) {
      return { name: `${firstName} ${lastName}`.trim(), email: null, title, linkedin, error: `Enrichment ${res.status}` };
    }

    const data  = await res.json();
    // La API puede devolver email en distintos campos según la respuesta
    const email = data?.email || data?.data?.email || data?.emails?.[0] || null;

    return {
      name:    `${firstName} ${lastName}`.trim(),
      email,
      title:   data?.title   || data?.data?.title   || title,
      linkedin: data?.linkedin_url || data?.data?.linkedin_url || linkedin,
    };

  } catch (err) {
    return { name: `${firstName} ${lastName}`.trim(), email: null, title, linkedin, error: err.message };
  }
}

// ============================================================
// Helpers internos
// ============================================================
function deobfuscateText(text) {
  if (!text) return "";
  return text
    .replace(/&#64;|&#x40;/gi,     "@")
    .replace(/&#46;|&#x2e;/gi,     ".")
    .replace(/\[\s*at\s*\]/gi,     "@")
    .replace(/\(\s*at\s*\)/gi,     "@")
    .replace(/\{\s*at\s*\}/gi,     "@")
    .replace(/\[\s*arroba\s*\]/gi, "@")
    .replace(/\(\s*arroba\s*\)/gi, "@")
    .replace(/\barroba\b/gi,       "@")
    .replace(/\[\s*dot\s*\]/gi,    ".")
    .replace(/\(\s*dot\s*\)/gi,    ".")
    .replace(/\{\s*dot\s*\}/gi,    ".")
    .replace(/\[\s*punto\s*\]/gi,  ".")
    .replace(/\(\s*punto\s*\)/gi,  ".")
    .replace(/\bpunto\b/gi,        ".")
    .replace(
      /([a-zA-Z0-9._%+\-]{2,})\s+(?:at|AT)\s+([a-zA-Z0-9\-]{2,}(?:(?:\s+(?:dot|DOT)\s+|\s*\.\s*)[a-zA-Z]{2,})+)/g,
      (_, local, domain) => `${local}@${domain.replace(/\s+(?:dot|DOT)\s+/g, ".").replace(/\s*\.\s*/g, ".")}`
    );
}

function extractEmailsFromText(text) {
  // TLD máximo 6 chars para evitar "comsoccer", "comfoo", etc.
  const regex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6}(?=\s|$|[^a-zA-Z])/g;
  const clean = deobfuscateText(text);
  return [...new Set((clean.match(regex) || []).map(e => e.toLowerCase()))];
}

// Validación rápida sincrónica antes de mostrar (sin DNS)
export function quickValidateEmail(email) {
  if (!email || typeof email !== "string") return false;

  const parts = email.split("@");
  if (parts.length !== 2) return false;
  const [local, domain] = parts;

  // Local part no puede ser vacía ni mayor a 64 chars
  if (!local || local.length > 64) return false;

  // TLD entre 2 y 6 caracteres
  const tld = domain.split(".").pop();
  if (!tld || tld.length < 2 || tld.length > 6) return false;

  // Dominio debe tener al menos una parte + TLD
  const domainParts = domain.split(".");
  if (domainParts.length < 2 || domainParts.some(p => p.length === 0)) return false;

  // La parte local NO debe parecer un dominio en sí misma
  // Ej: "owngoalnigeria.com@..." o "site.net@..."
  const COMMON_TLDS = /\.(com|net|org|io|co|ar|es|mx|br|pt|fr|it|de|uk|au|ca|gov|edu|info|biz|us|tv|me|app|dev)$/i;
  if (COMMON_TLDS.test(local)) return false;

  // El dominio no puede ser solo un TLD (ej: "@com")
  if (domainParts.length === 1) return false;

  // Formato general válido
  const emailRegex = /^[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]{1,253}\.[a-zA-Z]{2,6}$/;
  return emailRegex.test(email);
}

function filterEmails(emails) {
  return emails.filter(email => {
    const lower = email.toLowerCase();
    return !IGNORE_DOMAINS.some(p => lower.includes(p)) && validateEmailFormat(email);
  });
}
