// ============================================================
// ADEQ TOOLBAR — Verificación de Emails v2
// Capas de verificación (sin SMTP, que no es posible desde browser):
//   1. Formato (regex)
//   2. Typos comunes en dominios populares
//   3. Email de rol (info@, noreply@, etc.)
//   4. Dominio descartable / temporal
//   5. MX record via DNS over HTTPS (Google DNS)
// Resultado: valid, score (0-100), reason, tags[]
// ============================================================

// ── 1. TYPOS en dominios populares ────────────────────────────
const DOMAIN_TYPOS = {
  "gmial.com":      "gmail.com",
  "gmai.com":       "gmail.com",
  "gmail.co":       "gmail.com",
  "gamil.com":      "gmail.com",
  "gmaill.com":     "gmail.com",
  "gnail.com":      "gmail.com",
  "gmail.con":      "gmail.com",
  "hotmial.com":    "hotmail.com",
  "hotmail.co":     "hotmail.com",
  "hotmai.com":     "hotmail.com",
  "hotmall.com":    "hotmail.com",
  "hotmail.con":    "hotmail.com",
  "yahho.com":      "yahoo.com",
  "yaho.com":       "yahoo.com",
  "yahoo.co":       "yahoo.com",
  "yahooo.com":     "yahoo.com",
  "yahoo.con":      "yahoo.com",
  "outlok.com":     "outlook.com",
  "outloo.com":     "outlook.com",
  "outlook.co":     "outlook.com",
  "iclod.com":      "icloud.com",
  "icloud.co":      "icloud.com",
  "protonmai.com":  "protonmail.com",
  "protonmial.com": "protonmail.com",
};

// ── 2. ROLES — emails de poco valor para prospecting ──────────
const ROLE_PREFIXES = new Set([
  "info","contact","contacto","hello","hola","support","soporte","help","ayuda",
  "noreply","no-reply","no_reply","donotreply","do-not-reply","mailer","mailer-daemon",
  "admin","administrator","administrador","webmaster","webmaster","postmaster",
  "billing","facturacion","finance","contabilidad","accounting","payments","pagos",
  "sales","ventas","marketing","prensa","press","media","comunicacion","comunicaciones",
  "newsletter","news","subscriptions","suscripciones","notifications","notificaciones",
  "team","equipo","office","oficina","reception","recepcion","general","service",
  "customer","clientes","feedback","enquiries","enquiry","infoinfo","abuse","security",
  "legal","privacy","privacidad","compliance","rrhh","hr","careers","empleo","jobs",
  "invest","investors","inversores","ir","shareholder","accionistas",
]);

// ── 3. DOMINIOS DESCARTABLES / TEMPORALES ─────────────────────
const DISPOSABLE = new Set([
  "mailinator.com","tempmail.com","guerrillamail.com","throwaway.email","yopmail.com",
  "sharklasers.com","10minutemail.com","trashmail.com","fakeinbox.com","maildrop.cc",
  "dispostable.com","mailnull.com","spamgourmet.com","trashmail.io","temp-mail.org",
  "tempr.email","discard.email","mailnesia.com","mailnull.com","spamex.com",
  "getairmail.com","filzmail.com","throwam.com","spamthisplease.com","mailscrap.com",
  "mytemp.email","spamfree24.org","mailme24.com","super-mailer.com","spamstack.net",
  "dispomail.eu","objectmail.com","ownmail.net","spamgourmet.net","dodgit.com",
  "tempinbox.com","tempemail.net","yomail.info","meltmail.com","mt2009.com",
]);

// ── 4. FORMATO regex ──────────────────────────────────────────
const EMAIL_REGEX = /^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$/;

// Proxies de WhoIs / buzones de registrars — nunca son contactos reales,
// aunque SMTP responda. Hay que marcarlos como inválidos explícitamente.
const WHOIS_PROXY_DOMAINS = new Set([
  "markmonitor.com","whoisguard.com","whoisprivacy.com","whoisprivacyservice.org",
  "domainsbyproxy.com","contactprivacy.com","privacyprotect.org","privacy-protect.org",
  "proxy.dreamhost.com","namebright.com","namesilo.com","registerdomainsafe.com",
  "registrarsafe.com","anonymize.com","onamae.com","withheldforprivacy.com",
  "withheldforprivacy.email","perfectprivacy.com","protecteddomainservices.com",
  "whoisproxy.com","proxydomain.com","csc-global.com","redacted-gandi.net",
]);

const WHOIS_PROXY_LOCALS = new Set([
  "whoisrequest","whoisprivacy","whoisguard","domainabuse","domain-abuse",
  "abusereport","dns-admin","hostmaster","registrar","registrarcontact",
  "legal-notices","takedown","dmca",
]);

// ── Garbage emails que NUNCA deben llegar a la UI ─────────────
// Filtro sincrónico (sin DNS/red), corre antes del render. Más agresivo que
// ROLE_PREFIXES — acá sólo van los que son INSERVIBLES como contacto comercial.
const GARBAGE_LOCAL_PREFIXES = [
  "abuse","abuse-",
  "postmaster","mailer-daemon","mailer_daemon","mail-daemon","daemon",
  "hostmaster","dns-admin","dnsadmin","webmaster",
  "noreply","no-reply","no_reply","donotreply","do-not-reply","do_not_reply",
  "bounce","bounces","bounced","mailer","mailerbot",
  "registrar","registrarcontact","registrar-contact",
  "whois","whoisprivacy","whoisguard","whoisrequest",
  "takedown","dmca","copyright-claim","copyrightclaim","legal-notices","legalnotices",
  "domainabuse","domain-abuse","abusereport",
];
const GARBAGE_DOMAIN_SUFFIXES = [
  // proxies/registrars
  "markmonitor.com","whoisguard.com","whoisprivacy.com","whoisprivacyservice.org",
  "domainsbyproxy.com","contactprivacy.com","privacyprotect.org","privacy-protect.org",
  "namebright.com","namesilo.com","registerdomainsafe.com","registrarsafe.com",
  "anonymize.com","onamae.com","withheldforprivacy.com","withheldforprivacy.email",
  "perfectprivacy.com","protecteddomainservices.com","whoisproxy.com","proxydomain.com",
  "csc-global.com","redacted-gandi.net",
  // genericos que aparecen scrappeados pero nunca son contactos comerciales
  "sentry.io","sentry-next.wixpress.com","email-od.com",
];

/**
 * Filtro RÁPIDO (sin red) — devuelve true si el email NO debe mostrarse al user.
 * Cubre proxies de WhoIs, buzones administrativos, mailer-daemon, abuse, etc.
 * Más permisivo que verifyEmail — éste sólo bloquea lo que es inservible per se.
 */
export function isGarbageEmail(email) {
  if (!email || typeof email !== "string") return true;
  const e = email.toLowerCase().trim();
  if (!e.includes("@")) return true;
  const [local, domain] = e.split("@");
  if (!local || !domain) return true;

  // 1. Dominio de proxy/whois (exacto o subdominio)
  for (const d of GARBAGE_DOMAIN_SUFFIXES) {
    if (domain === d || domain.endsWith("." + d)) return true;
  }

  // 2. Local-part: prefijo administrativo. Aceptamos "abuse", "abuse-domain", "abuse_2024".
  for (const prefix of GARBAGE_LOCAL_PREFIXES) {
    if (local === prefix) return true;
    if (local.startsWith(prefix) && (local[prefix.length] === "-" || local[prefix.length] === "_" || local[prefix.length] === "." || /\d/.test(local[prefix.length] || ""))) return true;
  }

  // 3. Sufijo "-abuse" / "_abuse"
  if (/[-_.]abuse(\d|$)/.test(local)) return true;

  return false;
}

export async function verifyEmail(email) {
  if (!email || !email.includes("@")) {
    return { valid: false, score: 0, reason: "Formato inválido", tags: ["formato"] };
  }

  const [local, domain] = email.toLowerCase().trim().split("@");
  const tags = [];

  // ── Formato ────────────────────────────────────────────────
  if (!EMAIL_REGEX.test(email)) {
    return { valid: false, score: 0, reason: "Formato inválido", tags: ["formato"] };
  }

  // ── Proxy de WhoIs (markmonitor etc.) — nunca es contacto real ─
  if (WHOIS_PROXY_DOMAINS.has(domain) || [...WHOIS_PROXY_DOMAINS].some(d => domain.endsWith("." + d))) {
    return { valid: false, score: 0, reason: `Proxy de WhoIs (${domain}) — no es un contacto real`, tags: ["proxy-whois"] };
  }
  if (WHOIS_PROXY_LOCALS.has(local) || [...WHOIS_PROXY_LOCALS].some(l => local.startsWith(l + "-") || local.startsWith(l + "_"))) {
    return { valid: false, score: 0, reason: `Buzón de registrar (${local}@) — no es un contacto real`, tags: ["proxy-whois"] };
  }

  // ── Typo en dominio ────────────────────────────────────────
  const typoCorrection = DOMAIN_TYPOS[domain];
  if (typoCorrection) {
    return {
      valid:      false,
      score:      10,
      reason:     `Posible typo — ¿quisiste decir @${typoCorrection}?`,
      suggestion: `${local}@${typoCorrection}`,
      tags:       ["typo"],
    };
  }

  // ── Descartable ────────────────────────────────────────────
  if (DISPOSABLE.has(domain)) {
    return { valid: false, score: 5, reason: "Email temporal / descartable", tags: ["descartable"] };
  }

  // ── Email de rol ───────────────────────────────────────────
  const localBase = local.split("+")[0].replace(/[._-]/g, "");
  const isRole    = ROLE_PREFIXES.has(local) || ROLE_PREFIXES.has(localBase);
  if (isRole) tags.push("rol");

  // ── Extensiones de dominio sospechosas ────────────────────
  const tld = domain.split(".").pop();
  if (["tk","ml","ga","cf","gq","xyz","top","click","loan","win","racing","date"].includes(tld)) {
    tags.push("tld-sospechoso");
  }

  // ── MX Record via DNS over HTTPS ──────────────────────────
  let mxFound   = null;
  let mxRecords = [];
  try {
    const res  = await fetch(
      `https://dns.google/resolve?name=${encodeURIComponent(domain)}&type=MX`,
      { signal: AbortSignal.timeout(5000) }
    );
    const data = await res.json();
    mxFound    = data.Status === 0 && (data.Answer?.length > 0);
    mxRecords  = data.Answer?.map(a => a.data) || [];
  } catch {
    mxFound = null;
  }

  // ── A Record fallback (algunos dominios usan A sin MX) ────
  let aFound = false;
  if (!mxFound) {
    try {
      const res  = await fetch(
        `https://dns.google/resolve?name=${encodeURIComponent(domain)}&type=A`,
        { signal: AbortSignal.timeout(4000) }
      );
      const data = await res.json();
      aFound     = data.Status === 0 && (data.Answer?.length > 0);
    } catch {}
  }

  // ── Catch-all detection ──────────────────────────────────
  // Un dominio "catch-all" acepta emails con cualquier local-part.
  // Verificamos que el MX provider sea "business-grade" y no un providers tipo catch-all genérico.
  // Heurística: si el MX apunta a proveedores que se sabe son catch-all (mailgun, sendgrid, etc.)
  // marcamos el email como unverifiable aunque tenga MX.
  const catchAllProviders = [
    "mailgun.org","mailgun.net","sendgrid.net","sendgrid.com",
    "postmarkapp.com","mandrill","mailjet.com","sparkpost.com",
    "amazonses.com","aws.amazon.com",
    "mxroute","improvmx.com","forwardemail.net",
  ];
  const mxLower = mxRecords.map(m => m.toLowerCase()).join(" ");
  const catchAllSuspected = catchAllProviders.some(p => mxLower.includes(p));
  if (catchAllSuspected) tags.push("catch-all-provider");

  // ── Score ──────────────────────────────────────────────────
  let score = 0;
  if (mxFound)             score += 60;
  else if (aFound)         score += 20;
  if (!isRole)             score += 20;
  if (!tags.includes("tld-sospechoso")) score += 10;
  if (mxFound && !isRole && !catchAllSuspected)  score += 10;
  if (catchAllSuspected)   score -= 30;           // baja mucho el score — catch-all no verificable

  // ── Resultado ──────────────────────────────────────────────
  if (!mxFound && !aFound) {
    return {
      valid:   false,
      score,
      reason:  "Dominio sin registros DNS — probablemente inválido",
      mxFound: false,
      tags:    [...tags, "sin-dns"],
    };
  }

  if (!mxFound && aFound) {
    return {
      valid:   false,
      score,
      reason:  "Dominio existe pero sin MX — no puede recibir emails",
      mxFound: false,
      tags:    [...tags, "sin-mx"],
    };
  }

  // MX encontrado
  const isGenericMX = mxRecords.some(r =>
    r.includes("google") || r.includes("outlook") || r.includes("microsoft") ||
    r.includes("protonmail") || r.includes("zoho")
  );
  if (isGenericMX) tags.push("mx-conocido");

  // Catch-all detectado → no marcamos como valid (sería decir que cualquier email existe)
  if (catchAllSuspected) {
    return {
      valid:   false,
      score,
      reason:  `Dominio usa MX catch-all (${mxRecords[0]?.split(" ")[1] || "?"}) — cualquier email resuelve, no verificable de forma confiable`,
      mxFound: true,
      tags,
    };
  }

  if (isRole) {
    return {
      valid:   true,
      score,
      reason:  "Dominio válido pero email de rol (info@, contact@, etc.) — baja probabilidad de llegar al decisor",
      mxFound: true,
      tags,
    };
  }

  return {
    valid:   true,
    score,
    reason:  mxFound ? "Email con dominio válido y MX activo" : "Email con formato correcto",
    mxFound,
    tags,
  };
}

export async function verifyEmailList(emails) {
  const results = await Promise.all(
    emails.map(async email => ({ email, ...(await verifyEmail(email)) }))
  );
  return results.sort((a, b) => b.score - a.score);
}
