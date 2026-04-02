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

  // ── Score ──────────────────────────────────────────────────
  let score = 0;
  if (mxFound)             score += 60;
  else if (aFound)         score += 20;
  if (!isRole)             score += 20;
  if (!tags.includes("tld-sospechoso")) score += 10;
  if (mxFound && !isRole)  score += 10;

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
    r.includes("protonmail") || r.includes("zoho") || r.includes("mailgun")
  );
  if (isGenericMX) tags.push("mx-conocido");

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
