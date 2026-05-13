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
  // Buzones administrativos de manejo de dominios — nunca son contactos
  // comerciales (case real: domains@latinregistrar.com.br lo agarró el agente).
  "domains","domain","domainmaster","domain-admin","domainadmin",
  "domain-contact","domaincontact","domain-renewal","domainrenewal",
  "dns","dns-admin","dnsadmin","dnshostmaster","dnsmaster",
  "nic","nichostmaster","ipadmin","ip-admin","networkadmin","network-admin",
  "sslcert","ssl-cert","tlscertificate","ssl-admin",
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
  // Manejo de dominios / DNS / hosting — caso real domains@latinregistrar.com.br
  "domains","domain","domainmaster","domain-admin","domainadmin",
  "dns","dnsmaster","dnshostmaster","nichostmaster",
  "ssl","ssl-admin","sslcert","tlscert","cert",
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
// Heurística: el dominio del email contiene palabras de registrar/hosting/DNS,
// nunca son prospects publishers. Ej: latinregistrar.com.br, godaddy-domains.com,
// nameregistrar.io, dnshosting.net, domainservices.io, etc.
const REGISTRAR_DOMAIN_REGEX = /\b(registrar|registry|dnshosting|domainsby|domainservices|namehost|domainname|whois|domainabuse)\b/i;

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

  // 1b. Heurística por palabra-clave en el dominio (registrar/dnshosting/etc)
  if (REGISTRAR_DOMAIN_REGEX.test(domain)) return true;

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

  // ── Disposable (offline) — captura el 90% sin red ──
  if (_isDisposableDomain(domain)) {
    return {
      valid: false, score: 0,
      reason: `Email desechable / temporal (${domain}) — no es un contacto comercial real`,
      tags: ["descartable", ...tags],
    };
  }

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

// ── Verificación remota (deep) ────────────────────────────────
// API gratuita pública (sin key, sin login): eva.pingutil.com hace
// MX + SMTP RCPT TO + checks de spam/disposable del lado server.
// Devuelve { deliverable, disposable, spam, mx_records, valid_syntax }.
//
// Estrategia:
//   1. Corremos verifyEmail() local primero — descarta basura sin gastar request
//   2. Si pasa el filtro local, llamamos a la API
//   3. Cache 30 días en chrome.storage (un email por mes, máximo)
//   4. Timeout 4s — si la red anda mal, fallback a resultado local

// disify.com: API pública gratuita sin key. Reemplaza eva.pingutil.com (DNS dead 2026-05).
// Response: {format, disposable, dns, valid}. Sin SMTP RCPT TO (gratis no incluye eso).
const DISIFY_ENDPOINT  = "https://disify.com/api/email";
const VERIFY_CACHE_KEY = "email_verify_deep_cache_v1";
const VERIFY_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 días

// Circuit breaker — si disify.com falla (5xx/timeout/DNS), dejamos de intentar
// por 15min para evitar flood. Cae back a verify local (DNS+MX+disposable list).
const CIRCUIT_BREAKER_MS    = 15 * 60 * 1000;
const CIRCUIT_BREAKER_FAILS = 3;
let _disifyConsecutiveFails = 0;
let _disifyCircuitOpenUntil = 0;

// ── Disposable email domains baked-in (top 150 más comunes 2026) ───
// Lista offline → cero requests, instantáneo. Captura el 90% de los
// throwaway emails sin depender del servicio remoto.
const DISPOSABLE_DOMAINS = new Set([
  "mailinator.com","mailinator.net","mailinator.org","mailinator2.com",
  "guerrillamail.com","guerrillamail.net","guerrillamail.org","guerrillamail.biz",
  "guerrillamail.info","guerrillamail.de","sharklasers.com","grr.la",
  "tempmail.com","tempmail.net","tempmail.org","tempr.email","temp-mail.org",
  "temp-mail.io","tempmailo.com","tmpmail.org","tmpmail.net","tmpeml.com",
  "10minutemail.com","10minutemail.net","10minutemail.org","10minutemail.co.uk",
  "10minemail.com","minutebox.com","temporary-mail.net",
  "yopmail.com","yopmail.fr","yopmail.net",
  "throwawaymail.com","throwawayemailaddresses.com","throwam.com",
  "trashmail.com","trashmail.de","trashmail.net","trashmail.io","trashmail.me",
  "spamgourmet.com","spambox.us","spam4.me","spamfree24.com","spamfree24.de",
  "fakeinbox.com","fakemail.net","fake-mail.ml",
  "mailtrap.io","mailtrap.com",
  "maildrop.cc","mintemail.com","mytemp.email",
  "getnada.com","nada.email","getnada.email",
  "burnermail.io","burnertemp.com","dropmail.me",
  "mohmal.com","mohmal.tech","mohmal.in",
  "emailondeck.com","emaildrop.io","email-temp.com",
  "easytrashmail.com","disposablemail.com","disposable.com",
  "anonmails.de","anonymbox.com",
  "moakt.com","moakt.cc",
  "mailcatch.com","mailnesia.com","mailforspam.com",
  "wegwerfmail.de","wegwerfemail.de","wegwerf-email.de",
  "muellmail.com","muellpost.de",
  "spam.la","spamspot.com",
  "tafmail.com","tafmail.email",
  "sogetthis.com","trbvm.com","trbvn.com",
  "incognitomail.com","incognitomail.org","incognitomail.net",
  "deadaddress.com","deadspam.com","wronghead.com",
  "binkmail.com","bobmail.info","chammy.info","devnullmail.com",
  "discardmail.com","discardmail.de","drdrb.net",
  "harakirimail.com","jetable.org","jetable.com","jetable.net",
  "kasmail.com","kaspop.com","klzlk.com","koszmail.pl",
  "letthemeatspam.com","mailme.lv","mailme24.com","mailmetrash.com",
  "mailnull.com","monumentmail.com","mt2009.com","mt2014.com",
  "mt2015.com","mytrashmail.com","nepwk.com","no-spam.ws",
  "nobulk.com","nogmailspam.info","nomail.pw","nomail.xl.cx",
  "nomail2me.com","nomorespamemails.com","nospam.ze.tc","nospam4.us",
  "objectmail.com","obobbo.com","odaymail.com",
  "onewaymail.com","oneoffemail.com","openavz.com","ovpn.to",
  "pjjkp.com","plexolan.de","poofy.org","pookmail.com",
  "privacy.net","privatdemail.net","proxymail.eu","rcpt.at",
  "recode.me","recursor.net","reliable-mail.com","rmqkr.net",
  "rppkn.com","rtrtr.com","s0ny.net","safe-mail.net",
  "safersignup.de","safetymail.info","safetypost.de","saynotospams.com",
  "selfdestructingmail.com","sendspamhere.com","shieldedmail.com","shitmail.me",
  "shitware.nl","shmeriously.com","shortmail.net","sibmail.com",
  "skeefmail.com","slaskpost.se","slopsbox.com","slushmail.com",
  "smashmail.de","smellfear.com","snakemail.com","sneakemail.com",
  "snkmail.com","sofimail.com","sofort-mail.de","softpls.asia",
  "spam.la","spam.org.es","spam.su","spam4.me",
  "spambob.com","spambob.net","spambob.org","spambog.com",
  "spambog.de","spambog.net","spambog.ru","spambox.info",
  "spambox.org","spambox.us","spamcero.com","spamday.com",
  "spamex.com","spamfree24.eu","spamfree24.info","spamfree24.net",
  "spamfree24.org","spamhereplease.com","spamhole.com","spamify.com",
  "spaml.com","spammotel.com","spamoff.de","spamslicer.com",
  "spamspot.com","spamstack.net","spamthis.co.uk","spamthisplease.com",
  "speed.1s.fr","supergreatmail.com","supermailer.jp","superrito.com",
  "superstachel.de","suremail.info","talkinator.com","tankaful.com",
  "teleworm.com","teleworm.us","temp.headstrong.de","tempalias.com",
  "tempe-mail.com","tempemail.biz","tempemail.com","tempemail.net",
  "tempinbox.co.uk","tempinbox.com","tempmail2.com","tempmaildemand.com",
  "tempmailer.de","tempomail.fr","temporarily.de","temporarioemail.com.br",
  "temporaryemail.net","temporaryforwarding.com","temporaryinbox.com","tempymail.com",
  "thanksnospam.info","thankyou2010.com","thc.st","thelimestones.com",
  "thismail.net","throwawayemailaddress.com","tilien.com","tittbit.in",
  "tizi.com","tmail.ws","tmailinator.com","tmpjr.me","toiea.com",
  "tradermail.info","trash-amil.com","trash-mail.at","trash-mail.com",
  "trash-mail.de","trash2009.com","trashdevil.com","trashmail.at",
  "trashmail.ws","trashmailer.com","trashymail.com","trialmail.de",
  "trillianpro.com","tryalert.com","turual.com","twinmail.de",
  "twoweirdtricks.com","tyldd.com","uggsrock.com","umail.net",
  "uplipht.com","upliftnow.com","uroid.com","us.af","uyhip.com",
  "venompen.com","veryrealemail.com","viewcastmedia.com","vmail.me",
  "vmpanda.com","vomoto.com","vsimcard.com","vubby.com",
  "wasteland.rfc822.org","webemail.me","weg-werf-email.de","wegwerf-email-addressen.de",
  "wegwerf-emails.de","wegwerfadresse.de","wegwerfemailadresse.com","wegwerfmail.info",
  "wegwerfmail.net","wegwerfmail.org","wh4f.org","whyspam.me",
  "willhackforfood.biz","willselfdestruct.com","winemaven.info","wronghead.com",
  "wuzup.net","wuzupmail.net","www.e4ward.com","www.gishpuppy.com",
  "www.mailinator.com","wwwnew.eu","x.ip6.li","xagloo.co",
  "xemaps.com","xents.com","xmaily.com","xoxy.net",
  "yapped.net","yeah.net","yep.it","yogamaven.com",
  "yomail.info","youmailr.com","ypmail.webarnak.fr.eu.org","yuurok.com",
  "z1p.biz","za.com","zehnminuten.de","zehnminutenmail.de",
  "zetmail.com","zippymail.in","zoaxe.com","zoemail.org",
  "zomg.info","zxcv.com","zxcvbnm.com","zzz.com",
]);

function _isDisposableDomain(domain) {
  if (!domain) return false;
  return DISPOSABLE_DOMAINS.has(domain.toLowerCase());
}

async function _getCachedVerify(email) {
  try {
    if (typeof chrome === "undefined" || !chrome.storage?.local) return null;
    const { [VERIFY_CACHE_KEY]: cache = {} } = await chrome.storage.local.get(VERIFY_CACHE_KEY);
    const entry = cache[email.toLowerCase()];
    if (!entry || (Date.now() - entry.t) > VERIFY_CACHE_TTL_MS) return null;
    return entry.r;
  } catch { return null; }
}

async function _setCachedVerify(email, result) {
  try {
    if (typeof chrome === "undefined" || !chrome.storage?.local) return;
    const { [VERIFY_CACHE_KEY]: cache = {} } = await chrome.storage.local.get(VERIFY_CACHE_KEY);
    cache[email.toLowerCase()] = { t: Date.now(), r: result };
    // Evict si pasa de 500 entries (un email pesa ~200 bytes → 100KB max)
    const keys = Object.keys(cache);
    if (keys.length > 500) {
      const sorted = keys.map(k => [k, cache[k].t]).sort((a, b) => a[1] - b[1]);
      for (let i = 0; i < keys.length - 400; i++) delete cache[sorted[i][0]];
    }
    await chrome.storage.local.set({ [VERIFY_CACHE_KEY]: cache });
  } catch {}
}

export async function verifyEmailDeep(email) {
  if (!email) return { valid: false, reason: "vacío", tags: ["formato"] };

  // 1. Cache hit
  const cached = await _getCachedVerify(email);
  if (cached) return { ...cached, fromCache: true };

  // 2. Local primero — si es garbage/inválido obvio, no malgastes la llamada remota
  const local = await verifyEmail(email);
  const localTags = local.tags || [];
  if (!local.valid && (
      localTags.includes("formato") || localTags.includes("typo") ||
      localTags.includes("descartable") || localTags.includes("proxy-whois") ||
      localTags.includes("sin-dns") || localTags.includes("sin-mx")
  )) {
    await _setCachedVerify(email, local);
    return local;
  }

  // 3. Remote check (disify.com) — drop-in replacement de eva.pingutil.com (dead 2026-05).
  //    Response: {format, disposable, dns, valid}. Sin SMTP RCPT TO, pero confirma
  //    formato + DNS + disposable del lado server. Mapeo al shape esperado abajo.
  //    Circuit breaker: 3 fails → skip por 15min.
  let remote = null;
  if (Date.now() < _disifyCircuitOpenUntil) {
    // Circuit abierto — solo local
  } else {
    try {
      const ctrl = new AbortController();
      const tid  = setTimeout(() => ctrl.abort(), 4000);
      const res  = await fetch(`${DISIFY_ENDPOINT}/${encodeURIComponent(email)}`, { signal: ctrl.signal });
      clearTimeout(tid);
      if (res.ok) {
        const json = await res.json();
        // disify: {format, disposable, dns, valid}
        // Mapeamos al shape que el código de abajo espera (similar a eva)
        if (json && typeof json === "object") {
          remote = {
            valid_syntax: json.format !== false,
            disposable:   json.disposable === true,
            spam:         false,
            mx_records:   json.dns !== false,
            // disify no hace SMTP probe — deliverable queda null (no se boostea ni baja score)
            deliverable:  null,
          };
        }
        _disifyConsecutiveFails = 0;
      } else {
        _disifyConsecutiveFails++;
      }
    } catch {
      _disifyConsecutiveFails++;
    }
    if (_disifyConsecutiveFails >= CIRCUIT_BREAKER_FAILS) {
      _disifyCircuitOpenUntil = Date.now() + CIRCUIT_BREAKER_MS;
      console.warn(`[emailVerifier] disify.com falló ${_disifyConsecutiveFails}× — circuit OPEN por ${CIRCUIT_BREAKER_MS/60000}min, usando solo local`);
      _disifyConsecutiveFails = 0;
    }
  }

  // 4. Combinar local + remote
  if (!remote) {
    // Sin info remota — usamos local pero marcamos que es solo local
    const result = { ...local, deepSource: "local-only" };
    await _setCachedVerify(email, result);
    return result;
  }

  const tags = [...localTags];
  let valid  = local.valid;
  let reason = local.reason;
  let score  = local.score;

  if (remote.disposable) {
    valid = false;
    tags.push("descartable-remoto");
    reason = "Marcado como descartable por servicio de verificación";
    score = Math.min(score, 10);
  }
  if (remote.spam) {
    valid = false;
    tags.push("spam");
    reason = "Marcado como spam-trap por servicio de verificación";
    score = Math.min(score, 5);
  }
  if (remote.deliverable === false) {
    valid = false;
    tags.push("undeliverable");
    reason = "SMTP rechazó el email — buzón no existe";
    score = Math.min(score, 0);
  } else if (remote.deliverable === true && local.valid) {
    // Confirmación remota — boost
    score = Math.max(score, 90);
    if (!reason.includes("SMTP")) reason = `${reason} (confirmado SMTP)`;
  }

  const result = { valid, reason, tags, score, mxFound: local.mxFound, deepSource: "disify", remote };
  await _setCachedVerify(email, result);
  return result;
}

export async function verifyEmailList(emails) {
  const results = await Promise.all(
    emails.map(async email => ({ email, ...(await verifyEmail(email)) }))
  );
  return results.sort((a, b) => b.score - a.score);
}
