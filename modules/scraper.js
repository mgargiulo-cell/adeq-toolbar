// ============================================================
// ADEQ TOOLBAR — Módulo Email Scraper
// Fuentes (en orden de prioridad):
// 1. DOM de la página actual (footer, contacto, etc.)
// 2. website.informer.com/{domain}
// 3. Apollo.io API (decisor CEO/Owner)
// ============================================================

import { CONFIG }    from "../config.js";
import { callProxy } from "./apiProxy.js";

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
// 2. Páginas de contacto del propio sitio
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

// Valores que Apollo devuelve en email_status — incluimos likely_to_engage (mismatch anterior)
const APOLLO_GOOD_STATUSES = new Set([
  "verified",
  "likely",
  "likely_to_engage",
  "guessed",
  "unverified",
]);

function isUnlockedEmail(email, emailLocked) {
  if (emailLocked === true) return false;         // metadata explícita de Apollo
  if (!email) return false;
  const lower = email.toLowerCase();
  if (lower.includes("email_not_unlocked")) return false;
  if (lower.includes("not_unlocked_")) return false;
  if (!lower.includes("@") || !lower.includes(".")) return false;
  return true;
}

// Una página de la búsqueda Apollo (max 10 personas por page según el endpoint)
async function apolloSearchPage(domain, apiKey, withTitleFilter, page = 1) {
  const body = {
    q_organization_domains_list: [domain],
    per_page: 10,
    page,
    reveal_personal_emails: true,
    contact_email_status: ["verified", "likely_to_engage", "guessed"],
  };
  if (withTitleFilter) {
    body.person_titles = ["CEO","founder","co-founder","owner","publisher","editor","director","head","VP","manager","sales","marketing","business development"];
  }

  const res = await callProxy("apollo", "/v1/mixed_people/api_search", { method: "POST", body });
  if (!res.ok) {
    console.warn(`[Apollo] HTTP ${res.status}:`, (res.text || "").substring(0, 300));
    return null;
  }
  return res.data;
}

// Pagina hasta 3 páginas (30 personas max) parando si partial_results o no hay más páginas
async function apolloSearch(domain, apiKey, withTitleFilter) {
  const allPeople = [];
  let lastPagination = null;
  for (let page = 1; page <= 3; page++) {
    const data = await apolloSearchPage(domain, apiKey, withTitleFilter, page);
    if (!data) break;
    lastPagination = data.pagination || null;
    const people = Array.isArray(data.people) ? data.people : [];
    allPeople.push(...people);
    if (people.length < 10) break;                           // última página
    if (data.partial_results_only === true) break;           // Apollo cortó resultados
    if (lastPagination && lastPagination.has_next_page === false) break;
  }
  return { people: allPeople, pagination: lastPagination };
}

function mapApolloPerson(p) {
  return {
    email:    p.email, name: `${p.first_name || ""} ${p.last_name || ""}`.trim(),
    title:    p.title || "", linkedin: p.linkedin_url || "", status: p.email_status || "",
  };
}

/**
 * Busca en Apollo y devuelve diagnóstico detallado.
 * Return shape: { valid: [...], rawCount, lockedCount, noEmailCount, badStatusCount, sample, httpError }
 */
async function apolloDomainSearch(domain, apiKey) {
  const diag = { valid: [], rawCount: 0, lockedCount: 0, noEmailCount: 0, badStatusCount: 0, sample: [], httpError: null };

  try {
    // Intento 1: con filtro de títulos
    let data = await apolloSearch(domain, apiKey, true);
    let people = Array.isArray(data?.people) ? data.people : [];

    // Intento 2: si no trajo nada útil, retry sin títulos
    let triedFallback = false;
    if (people.length === 0 || !people.some(p => isUnlockedEmail(p.email, p.email_locked) && APOLLO_GOOD_STATUSES.has(p.email_status))) {
      const data2 = await apolloSearch(domain, apiKey, false);
      const people2 = Array.isArray(data2?.people) ? data2.people : [];
      if (people2.length > people.length) people = people2;
      triedFallback = true;
    }

    diag.rawCount = people.length;

    for (const p of people) {
      const hasEmail    = !!p.email;
      const unlocked    = isUnlockedEmail(p.email, p.email_locked);
      const goodStatus  = APOLLO_GOOD_STATUSES.has(p.email_status);

      if (!hasEmail)                     diag.noEmailCount++;
      else if (!unlocked)                diag.lockedCount++;
      else if (!goodStatus)              diag.badStatusCount++;
      else                               diag.valid.push(mapApolloPerson(p));
    }

    // Sample para debugging: primeros 5 con el motivo por el que se filtró (o pasó)
    diag.sample = people.slice(0, 5).map(p => ({
      name:   `${p.first_name || "?"} ${p.last_name || ""}`.trim(),
      title:  p.title || "",
      email:  p.email || null,
      status: p.email_status || null,
    }));

    const fallbackNote = triedFallback ? " (con fallback)" : "";
    console.log(`[Apollo] ${domain}${fallbackNote}: ${diag.rawCount} personas · ${diag.valid.length} válidas · ${diag.lockedCount} bloqueados · ${diag.noEmailCount} sin email · ${diag.badStatusCount} status inválido`);
    if (diag.rawCount > 0 && diag.valid.length === 0) {
      console.warn(`[Apollo] sample:`, diag.sample);
    }
    return diag;
  } catch (e) {
    diag.httpError = e.message;
    console.warn(`[Apollo] error:`, e.message);
    return diag;
  }
}

export async function findDecisionMakerViaApollo(domain) {
  const cleanDomain = domain.replace(/^www\./, "");

  // ── Paso 1: Gemini con Google Search → nombre del decisor ────
  let firstName = "", lastName = "", title = "", linkedin = "";

  {
    try {
      const prompt = `Find the CEO, founder, or main decision maker of the website "${cleanDomain}".
Return ONLY a JSON object with no extra text:
{"first_name":"John","last_name":"Smith","title":"CEO","linkedin":"https://linkedin.com/in/..."}
If not found, return: {"first_name":"","last_name":"","title":"","linkedin":""}`;

      const gRes = await callProxy("gemini", "/v1beta/models/gemini-2.5-flash:generateContent", {
        method: "POST",
        body: {
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          tools: [{ google_search: {} }],
          generationConfig: { temperature: 0.1, maxOutputTokens: 200 },
        },
      });
      if (gRes.ok) {
        const gData = gRes.data || {};
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

  // ── Paso 2: Apollo mixed_people/search por dominio + títulos ──
  const diag = await apolloDomainSearch(cleanDomain, null);
  if (diag.valid.length > 0) return diag.valid[0];

  // Construir mensaje de diagnóstico específico
  let reason;
  if (diag.httpError)           reason = `Apollo API error: ${diag.httpError}`;
  else if (diag.rawCount === 0) reason = `Apollo no tiene personas indexadas para ${cleanDomain}`;
  else if (diag.lockedCount === diag.rawCount) reason = `Apollo encontró ${diag.rawCount} personas pero TODOS los emails están bloqueados por plan. Desbloqueá los contactos en Apollo.app o upgradeá el plan.`;
  else if (diag.noEmailCount === diag.rawCount) reason = `Apollo encontró ${diag.rawCount} personas pero ninguna tiene email registrado`;
  else reason = `Apollo encontró ${diag.rawCount} personas: ${diag.lockedCount} bloqueados · ${diag.noEmailCount} sin email · ${diag.badStatusCount} con status inválido`;

  return {
    name:  firstName ? `${firstName} ${lastName}`.trim() : "",
    email: null, title, linkedin,
    error: reason,
    _apolloDiag: diag, // expuesto para debug en UI
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

// Dominios de proxies/privacidad de WhoIs — nunca son contactos reales
const WHOIS_PROXY_DOMAINS = [
  "markmonitor.com", "whoisguard.com", "whoisprivacy.com", "whoisprivacyservice.org",
  "domainsbyproxy.com", "contactprivacy.com", "privacyprotect.org", "privacy-protect.org",
  "proxy.dreamhost.com", "namebright.com", "namecheap.com", "namesilo.com",
  "registerdomainsafe.com", "registrarsafe.com", "anonymize.com", "onamae.com",
  "withheldforprivacy.com", "withheldforprivacy.email", "perfectprivacy.com",
  "protecteddomainservices.com", "whoisproxy.com", "proxydomain.com",
  "cloudflare.com", "csc-global.com", "redacted-gandi.net",
];

// Local-parts típicos de buzones genéricos de registrars / legal (no contactos)
const WHOIS_PROXY_LOCALS = [
  "whoisrequest", "whoisprivacy", "whoisguard", "domainabuse", "domain-abuse",
  "abusereport", "dns-admin", "hostmaster", "registrar", "registrarcontact",
  "legal-notices", "takedown", "dmca",
];

function isWhoIsProxyEmail(email) {
  const lower = email.toLowerCase();
  const [local, domain] = lower.split("@");
  if (!domain) return false;
  if (WHOIS_PROXY_DOMAINS.some(d => domain === d || domain.endsWith("." + d))) return true;
  if (WHOIS_PROXY_LOCALS.some(l => local === l || local.startsWith(l + "-") || local.startsWith(l + "_"))) return true;
  return false;
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

  // Bloquear proxies de WhoIs — no son contactos reales
  if (isWhoIsProxyEmail(email)) return false;

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
