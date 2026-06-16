// ============================================================
// ADEQ TOOLBAR — Módulo Email Scraper
// Fuentes (en orden de prioridad):
// 1. DOM de la página actual (footer, contacto, etc.)
// 2. Páginas de contacto/directorio del sitio (scrapeContactPages)
// 3. website.informer.com/{domain} + who.is (scrapeWebsiteInformer)
// 4. Apollo.io API (decisor CEO/Owner)
// ============================================================

import { CONFIG }    from "../config.js";
import { callProxy } from "./apiProxy.js";
import { getApolloCache, saveApolloCache, getApolloMonthlyUsage } from "./supabase.js";

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
  const paths = [
    "/contact", "/contact-us", "/contactus", "/contacto", "/contactanos",
    "/contact.html", "/contact.php", "/page/contact", "/pages/contact",
    "/support/contact", "/help/contact", "/support", "/help", "/help-center",
    "/lien-he", "/lienhe", "/kontakt", "/contatti", "/contato", "/contattaci",
    "/about", "/about-us", "/sobre-nosotros", "/quienes-somos", "/nosotros", "/gioi-thieu",
    "/directorio", "/directory", "/team", "/equipo", "/equipe", "/staff",
    "/advertise", "/advertising", "/publicidad", "/publicidade", "/anunciar", "/anunciantes",
    "/quang-cao", "/werbung", "/pubblicita",
    "/legal", "/aviso-legal", "/privacy", "/privacidad", "/redaccion",
  ];
  const emails = new Set();

  const fetchOne = async (path) => {
    try {
      const url      = new URL(path, baseUrl).href;
      const response = await fetch(url, { method: "GET", signal: AbortSignal.timeout(4000) });
      if (!response.ok) return;
      const html = await response.text();
      filterEmails(extractEmailsFromText(html)).forEach(e => emails.add(e));
    } catch { /* página no disponible */ }
  };

  // Concurrencia limitada (chunks de 5) para no disparar 25 fetch a la vez.
  // Cortamos temprano si ya juntamos varios correos buenos.
  const CONCURRENT = 5;
  for (let i = 0; i < paths.length && emails.size < 12; i += CONCURRENT) {
    await Promise.all(paths.slice(i, i + CONCURRENT).map(fetchOne));
  }

  return [...emails];
}

// ============================================================
// 3. Website Informer + who.is (fuentes externas de contacto)
// ============================================================
export async function scrapeWebsiteInformer(domain) {
  const clean = (domain || "").replace(/^https?:\/\//, "").replace(/^www\./, "").toLowerCase().trim();
  if (!clean) return [];
  const emails = new Set();
  const targets = [
    `https://website.informer.com/${clean}`,
    `https://who.is/whois/${clean}`,
  ];
  await Promise.all(targets.map(async (url) => {
    try {
      const res = await fetch(url, { method: "GET", signal: AbortSignal.timeout(6000) });
      if (!res.ok) return;
      const html = await res.text();
      filterEmails(extractEmailsFromText(html)).forEach(e => emails.add(e));
    } catch { /* bloqueado por anti-bot / rate limit */ }
  }));
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
// 5. Decisor — Apollo oficial (nombre + email)
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
    // Priorizamos titles relevantes a AdTech / monetización / publishing /
    // dev / digital marketing. Apollo hace OR de toda la lista. El ranking
    // posterior (rankApolloPerson) se encarga de priorizar adentro.
    body.person_titles = [
      // AdTech / monetización (lo más relevante para ADEQ)
      "ad ops","adops","ad operations","ads","advertising","advertisement",
      "monetization","monetisation","programmatic","yield","revenue",
      "traffic","inventory","media buyer","media buying","media",
      "publisher","publishing","publicidad","publicista","comercial",
      // Digital / online / marketing
      "digital","online","digital marketing","marketing","growth",
      "audience","content","editorial","editor",
      // Tech / dev (publishers chicos suelen tener al dev a cargo de ad stack)
      "developer","web developer","webmaster","cto","tech lead","engineer",
      // Decisor genérico (fallback)
      "CEO","founder","co-founder","owner","director","head","VP","manager",
      "business development","sales",
    ];
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

// Clasifica títulos en 3 buckets para que la UI siempre muestre PRIMERO
// las áreas que ADEQ quiere atacar (publicidad / marketing / online /
// programador), luego un fallback genérico, y descarta del todo lo que
// no decide pauta (HR/legal/finance/support).
//   - "core" → publicidad, marketing, digital/online, dev/tech, ad ops, programmatic
//   - "fallback" → C-suite / manager / business dev sin contexto ad/digital
//   - "drop" → HR, legal, finance, customer support
// Devuelve { bucket: "core"|"fallback"|"drop", score: number }
function _rankApolloTitle(title) {
  const t = (title || "").toLowerCase();
  if (!t) return { bucket: "fallback", score: 1 };

  // Drop — gente que NUNCA decide pauta publicitaria
  if (/\b(hr|human resources|recursos humanos|recruit|talent|payroll|legal|abogad|attorney|counsel|finance|finanzas|accountant|contabil|customer (support|service|success)|soporte|qa|quality assurance)\b/.test(t)) {
    return { bucket: "drop", score: -100 };
  }

  // Core — las 4 áreas que el user pidió (publicidad, marketing, online, programador)
  // + adjacentes obvios (ad ops, programmatic, content/editorial, traffic)
  if (/\b(ad\s?ops|ad operations|adops|monetizat|monetisa|programmatic|yield|revenue (manager|director|lead|ops)|inventory|media (buyer|buying|director|manager))\b/.test(t)) {
    return { bucket: "core", score: 100 };
  }
  if (/\b(publisher|publishing|publicidad|publicista|ads? (manager|director|lead|ops)|advertising|advertisement)\b/.test(t)) {
    return { bucket: "core", score: 95 };
  }
  if (/\b(digital marketing|growth (lead|head|director|manager)|digital (director|manager|lead|head)|online (director|manager|lead))\b/.test(t)) {
    return { bucket: "core", score: 90 };
  }
  if (/\b(marketing|digital|online|growth|audience|content|editorial|editor|traffic|comercial)\b/.test(t)) {
    return { bucket: "core", score: 80 };
  }
  // Programador / dev / tech
  if (/\b(cto|tech (lead|director|head)|web (developer|master)|webmaster|developer|engineer|programador|programmer)\b/.test(t)) {
    return { bucket: "core", score: 70 };
  }

  // Fallback — C-suite y management genérico, solo se usa si no hay nadie de las áreas core
  if (/\b(ceo|founder|co-founder|owner|president|managing director)\b/.test(t)) {
    return { bucket: "fallback", score: 50 };
  }
  if (/\b(vp|vice president|director|head of|head)\b/.test(t)) {
    return { bucket: "fallback", score: 30 };
  }
  if (/\b(manager|lead|business development|sales)\b/.test(t)) {
    return { bucket: "fallback", score: 20 };
  }
  return { bucket: "fallback", score: 5 };
}

function mapApolloPerson(p) {
  return {
    id:       p.id || null,
    email:    p.email || null,
    name:     `${p.first_name || ""} ${p.last_name || ""}`.trim(),
    first_name: p.first_name || "",
    last_name:  p.last_name  || "",
    title:    p.title || "",
    linkedin: p.linkedin_url || "",
    status:   p.email_status || "",
    emailLocked: p.email_locked === true,
    unlocked:   isUnlockedEmail(p.email, p.email_locked) && APOLLO_GOOD_STATUSES.has(p.email_status),
  };
}

/**
 * Busca en Apollo y devuelve diagnóstico detallado.
 * Return shape: { valid: [...], rawCount, lockedCount, noEmailCount, badStatusCount, sample, httpError }
 */
async function apolloDomainSearch(domain, apiKey) {
  const diag = { valid: [], all: [], rawCount: 0, lockedCount: 0, noEmailCount: 0, badStatusCount: 0, sample: [], httpError: null };

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
    diag.all = people.map(mapApolloPerson);

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
    // Log silencioso — solo si DEBUG_APOLLO=true en chrome.storage
    // console.log(`[Apollo] ${domain}${fallbackNote}: ${diag.rawCount} personas · ${diag.valid.length} válidas`);
    if (diag.rawCount > 0 && diag.valid.length === 0) {
      // diag.sample silenciado — info debug, no error
      // console.warn(`[Apollo] sample:`, diag.sample);
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

  // ── Cache check (TTL 7d) — evita pagar Apollo 2× para el mismo dominio ──
  // Worker autopilot y popup escriben/leen la misma cache.
  let _accessToken = null;
  try {
    const { auth } = await chrome.storage.local.get("auth");
    _accessToken = auth?.accessToken || null;
    if (_accessToken) {
      const cached = await getApolloCache(cleanDomain, _accessToken);
      if (cached) {
        // Cache HIT silenciado (info, no error)
        // console.log(`[Apollo] cache HIT ${cleanDomain} (saved ~$0.05 API)`);
        return cached;
      }
    }
  } catch {}

  // ── Apollo cap mensual (plan 2,500; cap 2,400 con margen 100) ──
  // Si llegado, NO llamamos Apollo — fallback al scraping que ya corre paralelo.
  // Toda la toolbar comparte el mismo cap (worker + popup).
  if (_accessToken) {
    try {
      const monthly = await getApolloMonthlyUsage(_accessToken);
      if (monthly.remaining <= 0) {
        console.warn(`[Apollo] cap MENSUAL alcanzado (${monthly.used}/${monthly.limit}) — fallback a scraping`);
        return {
          name: "", email: null, title: "", linkedin: "",
          people: [],
          note: `Apollo llegó al límite mensual de la toolbar (${monthly.used}/${monthly.limit}). Usando scraping como fuente alternativa.`,
          diag: { capped: true, monthly },
        };
      }
    } catch {}
  }

  // ── Apollo mixed_people/search por dominio + títulos ──
  const diag = await apolloDomainSearch(cleanDomain, null);
  // Clasificar válidos por bucket (core / fallback / drop) y ordenar:
  //   1) SIEMPRE primero los del bucket "core" (publicidad/marketing/online/dev)
  //   2) Después fallback (C-suite/manager) — solo se elige como primary si no hay core
  //   3) "drop" se descarta del todo (HR/legal/finance/support nunca se sugieren)
  const classified = diag.valid.map((p, i) => ({ p, ...(_rankApolloTitle(p.title)), idx: i }))
    .filter(x => x.bucket !== "drop");
  const core     = classified.filter(x => x.bucket === "core").sort((a, b) => (b.score - a.score) || (a.idx - b.idx));
  const fallback = classified.filter(x => x.bucket === "fallback").sort((a, b) => (b.score - a.score) || (a.idx - b.idx));
  // Core arriba de todo, fallback debajo. La UI renderiza en este orden.
  diag.valid = [...core, ...fallback].map(x => x.p);
  // Auto-pick: SOLO de las áreas relevantes. Si no hay core, no auto-revelamos
  // un C-suite genérico — dejamos primary en null para que el user decida.
  const primary = core.length > 0 ? core[0].p : null;

  let note = "";
  if (diag.httpError)               note = `Apollo API error: ${diag.httpError}`;
  else if (diag.rawCount === 0)     note = `Apollo has no people indexed for ${cleanDomain}`;
  else if (diag.valid.length === 0) {
    if (diag.lockedCount === diag.rawCount)       note = `${diag.rawCount} people found — all emails locked (click 🔓 Reveal on a row)`;
    else if (diag.noEmailCount === diag.rawCount) note = `${diag.rawCount} people found — no emails on file`;
    else note = `${diag.rawCount} people: ${diag.lockedCount} locked · ${diag.noEmailCount} no email · ${diag.badStatusCount} bad status`;
  } else {
    note = `${diag.valid.length} valid · ${diag.rawCount - diag.valid.length} locked/other`;
  }

  const result = {
    name:     primary?.name     || "",
    email:    primary?.email    || null,
    title:    primary?.title    || "",
    linkedin: primary?.linkedin || "",
    people:   diag.all,           // ALL people (locked + unlocked) for UI rendering
    note,
    diag,
  };
  // Persistir en cache (TTL 7d) — el siguiente lookup en 7 días es gratis,
  // sin importar si vino del worker o del popup.
  if (_accessToken) saveApolloCache(cleanDomain, result, _accessToken).catch(() => {});
  return result;
}

/**
 * Reveal a single Apollo contact via /v1/people/match — costs 1 Apollo credit.
 * Pass { id } (preferred) OR { first_name, last_name, domain }.
 */
export async function revealApolloEmail({ id, first_name, last_name, domain }) {
  const body = { reveal_personal_emails: true, reveal_phone_number: false };
  if (id)         body.id = id;
  if (first_name) body.first_name = first_name;
  if (last_name)  body.last_name  = last_name;
  if (domain)     body.organization_name = domain;
  try {
    const res = await callProxy("apollo", "/v1/people/match", { method: "POST", body });
    if (!res.ok) return { ok: false, error: `HTTP ${res.status}: ${(res.text||"").substring(0,120)}` };
    const p = res.data?.person || res.data;
    if (!p) return { ok: false, error: "No person returned" };
    const mapped = mapApolloPerson(p);
    if (!mapped.unlocked) return { ok: false, error: "Apollo could not unlock — out of credits or not allowed by plan", person: mapped };
    return { ok: true, person: mapped };
  } catch (e) { return { ok: false, error: e.message }; }
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
