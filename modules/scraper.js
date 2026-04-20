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
// 3. who.is — WHOIS registrant email
// ============================================================
export async function scrapeWhoIs(domain) {
  const cleanDomain = domain.replace(/^www\./, "");
  try {
    const response = await fetch(`https://who.is/whois/${cleanDomain}`, {
      method: "GET",
      signal: AbortSignal.timeout(7000),
      headers: { "User-Agent": "Mozilla/5.0 (compatible; bot)" },
    });
    if (!response.ok) return [];
    const html   = await response.text();
    return filterEmails(extractEmailsFromText(html));
  } catch {
    return [];
  }
}

// ============================================================
// 4. Páginas de contacto del propio sitio
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
// 5. Decisor — Gemini (nombre) + Apollo oficial (email)
// Paso 1: Gemini con Google Search → first_name, last_name, title
// Paso 2: Apollo /mixed_people/search por dominio + títulos ejecutivos
// ============================================================

const APOLLO_GOOD_STATUSES = new Set(["verified", "likely", "guessed", "unverified"]);

function isUnlockedEmail(email) {
  if (!email) return false;
  const lower = email.toLowerCase();
  // Apollo devuelve placeholders cuando el email está bloqueado por plan
  if (lower.includes("email_not_unlocked")) return false;
  if (lower.includes("not_unlocked_")) return false;
  if (!lower.includes("@") || !lower.includes(".")) return false;
  return true;
}

async function apolloSearch(domain, apiKey, withTitleFilter) {
  const body = {
    q_organization_domains_list: [domain],
    per_page: 10,
    page: 1,
  };
  if (withTitleFilter) {
    body.person_titles = ["CEO","founder","co-founder","owner","publisher","editor","director","head","VP","manager","sales","marketing","business development"];
  }

  const res = await fetch("https://api.apollo.io/v1/mixed_people/search", {
    method: "POST",
    headers: { "X-Api-Key": apiKey, "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(12000),
  });

  if (!res.ok) {
    const errBody = await res.text().catch(() => "");
    console.warn(`[Apollo] HTTP ${res.status}:`, errBody.substring(0, 300));
    return null;
  }
  return res.json();
}

async function apolloDomainSearch(domain, apiKey) {
  try {
    // Intento 1: con filtro de títulos (más preciso para decisores)
    let data = await apolloSearch(domain, apiKey, true);
    let people = Array.isArray(data?.people) ? data.people : [];
    console.log(`[Apollo] ${domain}: ${people.length} con filtro de títulos`);

    let valid = people
      .filter(p => isUnlockedEmail(p.email) && APOLLO_GOOD_STATUSES.has(p.email_status))
      .map(p => ({
        email: p.email, name: `${p.first_name || ""} ${p.last_name || ""}`.trim(),
        title: p.title || "", linkedin: p.linkedin_url || "", status: p.email_status || "",
      }));

    // Intento 2 (fallback): sin filtro de títulos, trae cualquier persona del dominio
    if (valid.length === 0) {
      data = await apolloSearch(domain, apiKey, false);
      people = Array.isArray(data?.people) ? data.people : [];
      console.log(`[Apollo] ${domain}: ${people.length} sin filtro de títulos (fallback)`);

      valid = people
        .filter(p => isUnlockedEmail(p.email) && APOLLO_GOOD_STATUSES.has(p.email_status))
        .map(p => ({
          email: p.email, name: `${p.first_name || ""} ${p.last_name || ""}`.trim(),
          title: p.title || "", linkedin: p.linkedin_url || "", status: p.email_status || "",
        }));

      if (people.length > 0 && valid.length === 0) {
        const statuses = people.slice(0, 5).map(p => `${p.first_name || "?"}: ${p.email_status || "null"} (${p.email || "no-email"})`).join(" | ");
        console.warn(`[Apollo] ${domain}: todos los emails bloqueados por plan → ${statuses}`);
      }
    }
    return valid;
  } catch (e) {
    console.warn(`[Apollo] error:`, e.message);
    return [];
  }
}

export async function findDecisionMakerViaApollo(domain) {
  const cleanDomain = domain.replace(/^www\./, "");
  const apolloKey   = CONFIG.APOLLO_API_KEY;

  // ── Paso 1: Gemini con Google Search → nombre del decisor ────
  let firstName = "", lastName = "", title = "", linkedin = "";

  if (CONFIG.GEMINI_API_KEY) {
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
          if (p.first_name) {
            firstName = p.first_name; lastName = p.last_name || "";
            title     = p.title     || "";
            linkedin  = p.linkedin  || "";
          }
        }
      }
    } catch {}
  }

  if (!apolloKey) {
    return firstName
      ? { name: `${firstName} ${lastName}`.trim(), email: null, title, linkedin, error: "No Apollo key" }
      : { error: "No Apollo key configured" };
  }

  // ── Paso 2: Apollo mixed_people/search por dominio + títulos ──
  const results = await apolloDomainSearch(cleanDomain, apolloKey);
  if (results.length > 0) return results[0];

  return {
    name:  firstName ? `${firstName} ${lastName}`.trim() : "",
    email: null, title, linkedin,
    error: "No email found via Apollo",
  };
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
