// ═══════════════════════════════════════════════════════════════════════════════════════
// EMAIL — elegir A QUIÉN se le escribe
// ═══════════════════════════════════════════════════════════════════════════════════════
// Extraído de index.js el 2026-09-02 SIN cambiarle una coma a la lógica: sólo se movió y se
// exportó. Estaba desparramado entre las líneas 6.400 y 18.400 del archivo grande, mezclado
// con descubrimiento, cola y salud.
//
// Es el área con MÁS reglas de negocio del sistema: acá vive el criterio del media buyer
// sobre qué dirección sirve y cuál no (rankEmail), la basura que se descarta antes de
// gastar una verificación paga, y el freno de entregabilidad. Un cambio acá se ve
// directamente en la tasa de rebote.
//
// Se apoya en `geo.js` (el TLD dice si una dirección es del país del sitio). NO conoce la
// base ni la red: es todo decisión sobre datos que le pasan, por eso se puede testear sin
// levantar nada — ver tests/garbage-filter.test.js.

import { COUNTRY_CODES } from "./geo.js";


// ── MEMORIA DE REBOTE POR DOMINIO (auditoría 2026-08-04) ───────────────────────────────────
// La lista de rebotados bloquea el EMAIL exacto, pero el rebote casi nunca es de la casilla:
// es del dominio (servidor decomisionado, empresa cerrada, MX roto). Si ventas@x.com rebotó,
// info@x.com va a rebotar igual — y hoy se lo mandábamos igual.
// Umbral 2 y no 1 a propósito: con 1 hay falsos positivos reales (una persona que se fue de una
// empresa que sigue viva). Con 2 rebotes distintos, el dominio está quemado.
// ⚠️ ESTADO COMPARTIDO con index.js, a propósito. index.js lo llena (leyendo la tabla de
// rebotes) y acá se consulta para puntuar. Se exporta el OBJETO, no el valor: index.js muta
// su contenido (.clear(), .set(...)) pero nunca reasigna la variable, que es lo único que
// ESM no permite a un importador. Pasarlo por parámetro habría cambiado la firma de
// rankEmail y sus llamadores, y el objetivo de esta extracción era mover código sin
// cambiarle el comportamiento.
export const _rebotesPorDominio = new Map();

// ── BOUNCE DETECTION ────────────────────────────────────────
// Cache local + 1 query a Supabase cuando se necesita refrescar (cada 5min).
// ⚠️ ESTADO COMPARTIDO con index.js (ver nota de _rebotesPorDominio). index.js refresca
// `.set` y `.ts` desde la base; acá sólo se lee en isBouncedSync.
export const _bouncedCache = { set: new Set(), ts: 0 };

// ── CAPA 3 (defense-in-depth) — palabras-clave en el DOMINIO del email ─────
// Cualquier dominio que contenga estas palabras = infraestructura, NO publisher.
// Caso real 2026-05-14: emails enviados a domains@latinregistrar.com.br,
// ceo@viads.com, trustandsafety@support.aws.com, ayuda@nic.mx, etc.
const _GARBAGE_DOMAIN_KEYWORDS = [
  // Registrars / DNS
  "registrar","registry","registrant","nic\\.","whois","domainsby","domainservice",
  "dominio","dominios","dominiosecuador","jewellaprivacy","cscinfo","cscglobal",
  "n2v","markmonitor","godaddyguard","gandi\\.net","enom","netim","epag",
  "porkbun","namebright","key-systems","onlinenic","ovhcloud","ovh\\.net",
  // Maxi 2026-07-28: WHOIS proxies / brand-protection vistos EN EL POOL REAL. Ninguno matcheaba
  // las keywords de arriba y quedaban como email #1 del lead. Casos textuales:
  //   domain-contact.org → pixiv.net, globo.com y sus 4 subdominios, store.playstation.com,
  //                        miro.com, eneba.com, outdooractive.com, parasut.com (8+ leads)
  //   ccireg.com → blog.bancolombia.com · iptwins.com → location.carrefour.fr
  //   istmanagement.com → los 3 blogs de fc2.com · psi-japan.co.jp → au.com
  // No son el publisher: son el agente que le administra el dominio. Escribirles es 100% desperdicio.
  "domain-?contact","ccireg","iptwins","istmanagement","psi-japan",
  "brandprotect","brand-protect","corporatedomains","netnames","safenames","comlaude","ipmirror",
  // Privacy proxies
  "privacyprotect","privacyguard","domainprotect","protecteddomain","whoisprotect",
  "whoisguard","contactprivacy","withheldforprivacy","perfectprivacy",
  "regprivate","redactedforprivacy","registrationprivate",
  // GDPR masks
  "gdpr-mask","gdpr-masked","gdpr-protect","data-protected","redacted-private",
  // Hosting / Cloud
  "amazonaws","amazonses","cloudfront","googlecloud","azure-?microsoft","cloudflare",
  "fastly","akamai","digitalocean","linode","heroku","netlify","vercel","render",
  "cloudways","kinsta","wpengine","siteground","hostinger","bluehost","hostgator",
  // Transactional senders (no humans)
  "sendgrid","mailgun","postmark","mandrill","sparkpost","amazonses","mailtrap",
  // Trust & safety / abuse desks
  "trustandsafety","trust-and-safety","abuse-?desk",
  // Disposable
  "mailinator","guerrillamail","tempmail","throwaway","sharklasers","yopmail","10minutemail","disposable",
];

// ── Email scoring + verify ──────────────────────────────────
// Garantiza que solo mandamos a emails decentes. 3 niveles:
//   🟢 green  : SMTP OK + no garbage + no genérico → manda
//   🟡 yellow : válido formato + genérico (info@/contact@/etc) → manda igual
//   🔴 red    : bounce, garbage (whois@/abuse@/postmaster@), inválido → SKIP
// LOCAL part patterns (antes del @): roles que nunca son decision-makers B2B.
// Lista exhaustiva — ampliada 2026-05-12 por feedback de envíos reales.
// Construido como string concat para mantener legibilidad.
// REGLA: solo bloqueamos emails que NINGÚN humano lee (mailer-daemon, abuse, etc).
// Customer support (info@, contact@, support@) NO se bloquea — algunos publishers
// usan esos como único contacto. Esos van como "yellow" (mandar pero baja confianza)
// vía rankEmail score bajo, no descarte total.
const _GL_LOCAL_PARTS = [
  // Sysadmin / mail infra (sin lectura humana real)
  // ⚠️ `admin` SALIÓ de acá (Maxi 2026-09-04). En un medio chico `admin@` es el dueño —el
  // WordPress lo crea así y nadie lo cambia—. Medido: los media buyers lo usaron y funcionó
  // (multipasko.pl, bangla-kobita.com, forebet.com), y el crawler lo veía y lo tiraba con -1.
  // Queda como "de última" (ver el castigo en rankEmail), no como basura: pierde contra
  // cualquier persona o rol, pero no deja al lead sin ningún email.
  "abuse","administrator","root","sudo","webmaster","hostmaster","postmaster","nobody","null",
  // Roles que no responden / no son decision-makers (cazados 2026-05-14)
  "feedback","feedbacks","reclamo","reclamos","reclamacao","reclamacoes","quejas","sugerencias","sugestoes",
  "circulation","subscriptions","subs","newsletter","alerts","alerta","alertas",
  "training","capacitacion","capacitacao","cursos",
  "plataforma","plataformas","platform","platforms","sistema","sistemas","tic","tic-admin","tic.adm","tecnologia","tecnologias","tech-admin",
  "foco","servicioalcliente","servicio-al-cliente","servicio-cliente","atencionalcliente","atencion-al-cliente","customerservice","customer-service","customercare","customer-care",
  "office","oficina","secretaria","secretariat","reception",
  // ⚠️ ACÁ ESTUVO "redaction","redazione","redaktion","redactie","editorial" (sacado 2026-08-31).
  // Era la MISMA palabra tratada al revés según el idioma: `redaccion@` (es), `redacao@` (pt) y
  // `editor@` puntuaban 115 y se enviaban, mientras `redazione@` (it), `redaktion@` (de),
  // `redaction@` (fr), `redactie@` (nl) y `editorial@` morían en esta lista — y encima 90 líneas
  // más abajo la regla EDITORIAL de esta misma función les da +75, o sea que era código muerto.
  // Medido: 163 leads en 7 días quedaron sin email por ranking; en la muestra, la casilla de
  // redacción EN EL DOMINIO PROPIO del medio era la única que el sitio publicaba
  // (valsusaoggi.it, dueruote.it, varesenews.it, come-on-fc.com, santemagazine.fr, reader.gr).
  // Descartarla es perder el lead entero, y choca de frente con el North Star: garantizar ≥1 email.
  // Pega justo en la Europa no hispana, que es adonde va la cascada GEO después de LATAM/España.
  // El buzón de redacción lo lee una persona del medio: no es infraestructura, que es lo que esta
  // lista existe para cazar. Si alguna vez se decide que la redacción NO es un destino válido, va
  // como PENALTY en el scoring —una decisión de negocio, visible y medible—, no acá.
  "trustandsafety","trust-?and-?safety","trust-?safety","safety","safety-?team","trust-?ops",
  "whois","registrant","registry","registrar","domain-?ops","domain-?abuse","ndomains",
  // Manejo de dominios/DNS (caso real domains@latinregistrar.com.br se coló al agent 2026-05-13)
  "domain","domains","domain-?master","domain-?admin","domain-?contact","domain-?renewal",
  "dns","dns-?master","dns-?host-?master","dns-?admin","nic","nic-?host-?master",
  "ssl","ssl-?cert","ssl-?admin","tls-?cert",
  "noreply","no-reply","donotreply","do-not-reply","do_not_reply","autoreply","auto-?reply","mailer-?daemon","mta","mailserver","mail-?server","mta-?admin",
  "bounce","bounced","mailer",
  "cert","cert-?admin","csirt","soc","noc","sysadmin","sys-?admin","netops","cert-?manager",
  "spam","antispam","fraud","antifraud","phishing","abusedesk","abuse-?report",
  "legal","copyright","dmca","takedown","trademark",
  // Privacy / GDPR (proxies de WHOIS)
  "gdpr","gdpr-?mask","gdpr-?masking","gdpr-?desk","dpo","data-?protection","privacy","masked","masking","anonymous","anon","undisclosed",
  // Las mismas oficinas en los idiomas del pool (Maxi 2026-09-04). `datenschutz@` puntuaba 65 y
  // `rgpd@` 40 —o sea "sirve"— porque el castigo de -30 de abajo no compensaba el +40 del dominio
  // propio y el +55 de "parece persona". Un delegado de protección de datos no compra pauta.
  "rgpd","lopd","datenschutz","datenschutzbeauftragter","proteccion-?de-?datos","protecao-?de-?dados","privacidade","privacidad","ochrona-?danych",
  // Billing / finance (no compran ads)
  "billing","invoice","invoices","invoicing","accounting","finance","payable","payables","treasury",
  // Hosting / CDN (infraestructura)
  "hosting","cdn","cloudflare","cloudfront","akamai","fastly","incapsula","sucuri",
  "piracy","pirate","antipiracy","anti-?piracy",
  "dns","dns-?admin","ssl-?admin",
  // HR (no son decision-makers comerciales)
  "careers?","jobs?","recruit","recruiting","recruitment","hire","hiring","talent",
  // Marketing automation opt-out
  "unsubscribe","opt-?out","optout","removeme","remove-?me",
  // Dev/test fakes
  "test","testing","dev","developer","staging","sandbox","example","fake","throwaway",
  // Monitoring
  "monitoring","alerts?","incident","incidents",
];

// Local-parts genéricos (no son una persona). Si el verified gratis es uno de
// estos y el sitio califica para unlock, gastamos 1 credit para revelar al
// decision-maker real en vez de quedarnos con el genérico.
export const APOLLO_GENERIC_LOCAL = /^(info|contact|contacto|contato|contatto|kontakt|hello|hi|hola|ola|olá|support|soporte|suporte|atendimento|mail|email|inbox|news|press|prensa|imprensa|sales|ventas|marketing|publicidade|publicidad|comercial|admin|general|reception|recepcion|recepcao|webmaster|noreply|no-reply)$/i;

export function _isGenericEmail(email) {
  const local = (email || "").split("@")[0] || "";
  return APOLLO_GENERIC_LOCAL.test(local.trim());
}

export const IGNORE_EMAIL = ["example.com","domain.com","sentry.io","google.com","w3.org","schema.org","cloudflare.com"];

// Maxi 2026-07-01: email_sources puede guardar un STRING ("scrape") o un OBJETO
// {url, source} según el path de escritura. Normaliza SIEMPRE a string. Antes se
// metía el objeto entero en el ranking y en toolbar_response_tracking.source →
// rompía el análisis por fuente Y el ranking dinámico (cada dominio quedaba como
// una "fuente" distinta, ej: {"url":"...","source":"scrape"}).
// ── BRAND-MATCH: ¿el email pertenece a la MARCA del lead? (hoisteado 2026-07-28) ──
// Vivía inline en el loop de envío. Lo saco a módulo para poder usarlo TAMBIÉN en la
// selección de candidato: antes, un email de otra marca abortaba el envío y marcaba el lead
// como 'rejected' para siempre, teniendo otras direcciones válidas en la lista. Ahora ese
// candidato se descarta y se prueba el siguiente, igual que con los no entregables.
// Algoritmo (2026-05-15): strip TLD para comparar nombre comercial.
//   eltribuno.com ↔ eltribuno.com.ar    → match (mismo brand)
//   midilibre.fr  ↔ midilibre.com        → match
//   tudogostoso.com.br ↔ webedia-group.com → no match (matriz distinta)
export function _brandStripTld(d) {
  const parts = String(d || "").split(".");
  if (parts.length <= 2) return parts[0] || "";
  const last2 = parts.slice(-2).join(".");
  const TWO_LEVEL = new Set([
    "com.ar","com.br","com.mx","com.co","com.pe","com.cl","com.ve","com.ec","com.uy",
    "com.py","com.bo","com.gt","com.sv","com.hn","com.ni","com.cr","com.pa","com.do",
    "co.uk","co.za","co.nz","co.jp","co.kr","co.id","co.in",
    "com.tr","com.tw","com.hk","com.sg","com.my","com.au","com.eg","com.sa","com.ng",
    "org.uk","ac.uk","gov.uk","com.pt","org.br","gov.br","edu.br",
  ]);
  if (TWO_LEVEL.has(last2)) return parts[parts.length - 3] || "";
  // ⚠️ LA LISTA SIEMPRE QUEDA CORTA (Maxi 2026-08-25). `addustour.com.jo` recortaba a
  // "com" porque com.jo no estaba, y por eso el enviador lo trataba como otra marca
  // siendo literalmente el MISMO nombre con otro TLD. Regla genérica de respaldo: si el
  // penúltimo trozo es un genérico de segundo nivel, la marca está un lugar más atrás.
  if (parts.length >= 3 && /^(com|net|org|gov|edu|ac|co|or|ne|gob|mil)$/.test(parts[parts.length - 2])) {
    return parts[parts.length - 3] || "";
  }
  return parts[parts.length - 2] || "";
}

// Raíz registrable (eTLD+1) de un dominio: casavogue.globo.com → globo.com
export function _rootDomain(d) {
  const dom = String(d || "").toLowerCase().replace(/^www\./, "").split("/")[0];
  const parts = dom.split(".");
  if (parts.length <= 2) return dom;
  const last2 = parts.slice(-2).join(".");
  const TWO_LEVEL = new Set([
    "com.ar","com.br","com.mx","com.co","com.pe","com.cl","com.ve","com.ec","com.uy",
    "com.py","com.bo","com.gt","com.sv","com.hn","com.ni","com.cr","com.pa","com.do",
    "co.uk","co.za","co.nz","co.jp","co.kr","co.id","co.in",
    "com.tr","com.tw","com.hk","com.sg","com.my","com.au","com.eg","com.sa","com.ng",
    "org.uk","ac.uk","gov.uk","com.pt","org.br","gov.br","edu.br","ne.jp","or.jp","go.jp",
    "com.ph","com.ua","co.il","com.vn","co.th","com.pk","com.bd","com.gh","net.au","org.au","com.pl","com.ru","com.cn","org.mx","com.py","com.bo",
  ]);
  if (TWO_LEVEL.has(last2)) return parts.slice(-3).join(".");
  return last2;
}

/**
 * ⚠️ REESCRITO EL 2026-08-25 — ESTA REGLA FRENABA ~320 ENVÍOS POR DÍA.
 *
 * El registro de motivos que agregué el 24/08 lo dejó a la vista: de 1.215 candidatos
 * descartados en 517 leads, el 100% cayó acá. Y mirando QUÉ rechazaba:
 *   3djuegos.com     → publicidad@webedia-group.com   (Webedia es su casa editora)
 *   4troxoi.gr       → emporiko@kathimerini.gr        (emporiko = comercial)
 *   ad-italia.it     → adstxt@condenast.com           (Condé Nast)
 *   beurs.nl         → sales@iexgroup.nl              (IEX Group)
 *   addustour.com    → advertis@addustour.com.jo      (la MISMA marca, otro TLD)
 *
 * O sea que el sistema buscaba a propósito el contacto de la casa editora —lo agregamos
 * por DNS y por OWNERDOMAIN del ads.txt— y después el enviador lo tiraba. Una parte
 * trabajaba contra la otra.
 *
 * La regla nació para un caso real y distinto: `info@domain-contact.org` como email #1 de
 * pixiv.net, que es el proxy de WHOIS, no el publisher. La diferencia entre ese caso y los
 * de arriba no es el dominio: **es de dónde salió el email**. Si lo scrapeamos DEL PROPIO
 * SITIO, es el contacto que ellos publican, valga el dominio que valga. Si vino de WHOIS o
 * de una hipótesis de patrón, ahí sí hay que exigir que la marca coincida.
 *
 * @param fuente  de dónde salió el email (`cand.source`). Sin fuente se mantiene el
 *                comportamiento estricto de antes.
 */
// Fuentes en las que el email salió del PROPIO sitio (o de un proveedor que consultamos
// nosotros), y por lo tanto se le perdona que el dominio del correo sea el de la casa
// editora. La única que NO entra es `informer` (WHOIS/registrador), que es justo la que
// devuelve al dueño del dominio y no al contacto comercial.
// Maxi 2026-08-25: faltaba `generic` —1.634 emails del pool, los info@/contacto@ que el
// scraper saca de la página de contacto— y las vías nuevas de descubrimiento (MX, DMARC,
// Certificate Transparency, YouTube). Al no estar, se las trataba como origen desconocido
// y hostil, que es exactamente lo contrario de lo que son.
// `rol_mx` (2026-09-04): el rol estándar del idioma en el propio dominio, con MX verificado. Es
// del dominio por construcción (ver _ROLES_POR_IDIOMA en index.js).
export const _FUENTES_DEL_PROPIO_SITIO = /^(scrape|mailto|jsonld|json_ld|sitemap|rss|wordpress|wp|contact_form|social|ads_txt|adstxt|google_contact|play|wayback|apollo|generic|mx|dmarc|ct|cert_transparency|youtube|instagram|facebook|twitter|linkedin|rol_mx)/i;

export function _brandMatches(email, leadDomain, fuente = "") {
  const recipientDom = (String(email || "").split("@")[1] || "").toLowerCase();
  const leadDom      = String(leadDomain || "").toLowerCase().replace(/^www\./, "");
  if (!recipientDom || !leadDom) return false;
  // Publicado por ellos en su propio sitio (o devuelto por Apollo para ESE dominio):
  // es su contacto, aunque el buzón viva en el dominio del grupo editor.
  if (_FUENTES_DEL_PROPIO_SITIO.test(String(fuente || ""))) return true;
  const isWebmail = /^(gmail|hotmail|outlook|live|yahoo|aol|icloud|protonmail|gmx|me)\.com$/.test(recipientDom);
  const rb = _brandStripTld(recipientDom), lb = _brandStripTld(leadDom);
  return recipientDom === leadDom
      || recipientDom.endsWith("." + leadDom)
      || leadDom.endsWith("." + recipientDom)
      || (rb && rb === lb && rb.length >= 4)
      || isWebmail;
}

// Limpieza de prefijo "C" pegado al email — artifact común del scrape de HTML
// donde "Contato: foo@bar.com" se captura como "Cfoo@bar.com" porque la regex
// agarró la C de "Contato:" cuando no había espacio. Casos reales 2026-05-13:
//   - Cpublicidade@autoracing.com.br → publicidade@autoracing.com.br
//   - Cbasketball-video.com@whoisprotectservice.net → basketball-video.com@... (queda invalido por TLD, se descarta)
//   - Cluciano@phonecall.com.br → luciano@phonecall.com.br
export function _stripScrapePrefix(email) {
  if (!email || typeof email !== "string") return email;
  // Decode URL-encoded chars (%20=space, %09=tab) y trim whitespace al inicio.
  // Caso real 2026-05-14: "%20info@ewdifh.com" salió enviado por el agente.
  try { email = decodeURIComponent(email); } catch {}
  // ⚠️ ESCAPES DE HTML/JSON PEGADOS AL PRINCIPIO (Maxi 2026-08-25).
  // El scrape agarra el email de un HTML donde antes viene un ">" codificado y se lo lleva
  // pegado literal: `u003ecommercialquestions@condenast.com`. Medido: 37 direcciones del
  // pool arruinadas así, y varias eran justo los contactos comerciales que buscamos —
  // ads@omnicalculator.com, media@omnicalculator.com, commercialquestions@condenast.com.
  // Mismo tipo de bug que la "C" de "Contato:" que ya se limpiaba abajo.
  email = email.replace(/^(?:\\?u00(?:3e|3c|26)|&gt;|&lt;|&amp;|&#\d+;|[<>"'`])+/i, "")
                 .replace(/(?:\\?u00(?:3e|3c)|&gt;|&lt;|[<>"'`])+$/i, "")
                   // Etiqueta pegada adelante: "email: juan@x.com" — el scrape se lleva el rótulo.
                   .replace(/^\s*(?:e-?mail|mail|correo|contacto|contato)\s*[:：]\s*/i, "")
                   // Barras y comillas de escape que quedan al final del recorte del HTML.
                   .replace(/[\\\/"'`\s]+$/, "");
  email = email.replace(/^[\s​ ]+/, "").trim();
  const [local, domain] = email.split("@");
  if (!local || !domain) return email;
  // Solo si local-part empieza con 1 letra MAYÚSCULA seguida de letra minúscula:
  // "Cpublicidade", "Cluciano". Y la versión sin la letra debe seguir matcheando
  // formato normal (5+ chars y no es la inicial de "carlos" ej.)
  if (/^[A-Z][a-z]{4,}/.test(local)) {
    const stripped = local.slice(1).toLowerCase();
    // Solo limpiar si la versión strip es una palabra reconocible (rol/keyword común)
    // o tiene patrón firstname normal sin la mayúscula al inicio.
    const KNOWN_LOCAL = /^(publicidade|publicidad|contato|contatto|contact|contacto|info|hola|atendimento|suporte|soporte|prensa|press|imprensa|sales|ventas|marketing|comercial|info|hello|hi|news|press|admin|director|gerente|owner|founder|ceo|cto|editor|redaccion|redacao)/i;
    // Maxi 2026-07-08: ELIMINADO el branch FIRSTNAME (/^[a-z]{3,12}$/) — era la CAUSA RAÍZ
    // de los locals mutilados vistos en prod (Search→earch, Values→alues, Contact→ontact,
    // Research→esearch): strippeaba la 1ra letra de CUALQUIER palabra capitalizada real, no
    // solo de un stray "C" pegado de "Contato:". Ahora solo se strippea si el remanente es un
    // ROL CONOCIDO (Cpublicidade→publicidade, Cinfo→info), único caso inequívoco de prefijo
    // basura. Un firstname legítimo mal-pegado ("Cluciano") se pierde, pero eso es MUCHO menos
    // dañino que mutilar palabras válidas (regla del dueño: no mutilar emails reales).
    if (KNOWN_LOCAL.test(stripped)) {
      return `${stripped}@${domain}`;
    }
  }
  return email.toLowerCase();
}

// ── Limpieza/filtrado de emails scrapeados (calidad de contactos) ──
export const STRICT_EMAIL_RE = /^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$/;

// Dominios de registradores / WHOIS-privacy — nunca son el contacto del medio.
export const JUNK_EMAIL_DOMAINS = new Set([
  "networksolutionsprivateregistration.com","whoisprotectservice.net","domainsbyproxy.com",
  "contactprivacy.com","privacyprotect.org","privacy-protect.org","whoisguard.com",
  "resellerid.com","latintld.com","web.com","networksolutions.com","godaddy.com",
  "namecheap.com","name.com","gandi.net","tucows.com","enom.com","registrarsafe.com",
  "1and1.com","ionos.com","cdsfulfillment.com",
]);

export const JUNK_LOCAL_RE = /^(dominios?|domainoperations|tldadmin|hostmaster|registrar|registrarcontact|abuse|abusereport|dns-admin|postmaster|noc)$/i;

// Tokens de admin de dominio/DNS en cualquier parte del local-part
// (ej "tenmien.intecom", "admin-domaines-internet", "tdns").
export const JUNK_LOCAL_TOKENS = /(^|[._-])(tenmien|tdns|dns|domain|domains|domaines|hostmaster|registrar|tldadmin|abuse|noc|nic)([._-]|$)/i;

// Placeholders de plantilla / buzones técnicos que nunca son contacto real.
// Maxi 2026-07-08: ampliado con placeholders de NOMBRE de plantilla en varios idiomas
// (vistos en prod, ej vorname.name@weltwoche.ch = "nombre.apellido" en alemán). Separador
// "." o "_". Un email con local "vorname.name"/"firstname.lastname"/etc. es una plantilla sin
// rellenar, jamás un contacto real.
export const PLACEHOLDER_LOCAL = /(youremail|yourname|tuemail|tucorreo|webmail|noreply|no-reply|^email$|^e-mail$|^name$|^nombre$|^test$|^example$|^ejemplo$|placeholder|^user\d*$|^usuario\d*$|^guest\d*$|^demo$|^sample$|vorname[._]name|nombre[._]apellido|firstname[._]lastname|prenom[._]nom|nome[._]cognome|name[._]surname|ime[._]prezime|nome[._]sobrenome|max[._]mustermann|^mustermann$|^(john|jane)[._]?doe$|^(john|jane)doe$|^doe$|^first$|^firstname$|^second$|^lastname$|^apellido$|^surname$|^prenom$|^vorname$|^insertname$|^changeme$|etunimi[._]sukunimi|imie[._]nazwisko|keresztnev[._]vezeteknev|^cuenta$|^conta$|^alumn[oa]s?$|^alun[oa]s?$|^estudiantes?$|^students?$|^matricula$|^android$|^ios$|^apk$|^app$|^apps$|^appstore$|^playstore$)/i;

// Maxi 2026-07-27 (auditoría de envíos reales 21-27): los términos agregados arriba son casos
// TEXTUALES que recibieron un pitch en esta tanda y que el regex original no cubría:
//   etunimi.sukunimi@sanoma.com → "nombre.apellido" en FINLANDÉS: es el ejemplo que la propia web
//                                 publica para explicar su formato de casilla. La dirección no existe.
//   cuenta@gmail.com            → literalmente eso, copiado de un instructivo del sitio
//   alumno@sportlife.cl         → buzón de alumnado, jamás compra pauta (y encima cross-dominio)
//   android@eurosport.com       → buzón de la app móvil, no es contacto comercial
// Todos entraban como PERSON (+70) o SINGLE_NAME (+30) y le ganaban a un contacto real del sitio.
// + variantes de "nombre.apellido" en polaco (imie.nazwisko) y húngaro, que están en el pool N4.
// Maxi 2026-07-21 (auditoría 5 días): se estaban ENVIANDO placeholders reales que el regex no
// cubría — jane.doe@pacoelchato.com (nombre de ejemplo), first@duic.nl ("first" suelto),
// firstname/lastname/apellido sueltos. Todos rebote seguro → suman a los 68 rebotes del período.
// Dominios de webmail (gmail, hotmail, etc. en cualquier TLD).
export const WEBMAIL_RE = /^(gmail|hotmail|outlook|live|yahoo|ymail|icloud|proton|protonmail|gmx|aol|msn|mail)\./i;

// Saca artefactos de extracción (u003e de JSON escapado, backslash/control chars
// pegados, %20, prefijo "C"). Casos reales mayo: "u003enews@...", "news@x.tv\".
export function _sanitizeEmail(raw) {
  if (!raw || typeof raw !== "string") return "";
  let e = raw;
  try { e = decodeURIComponent(e); } catch {}
  e = e.replace(/^(u00[0-9a-f]{2})+/i, "");        // u003e, u0022, ...
  e = e.replace(/[\x00-\x1f\x7f\\%]/g, "");        // control chars, backslash y % residual (ej "%hector@")
  e = e.replace(/^[^a-z0-9]+/i, "").trim().toLowerCase();  // cualquier basura al inicio (.>"'<% etc.)
  return _stripScrapePrefix(e);
}

// Filtra emails scrapeados: sanitiza, valida formato, descarta registradores/WHOIS,
// admin de dominio/DNS y placeholders. Acepta emails del dominio del lead; los
// webmail (gmail/hotmail) solo si parecen persona (nombre.apellido). Prioriza
// personas sobre genéricos y capea a 15. NO se aplica a emails de Apollo.
// opts.urlByEmail: Map email→URL de origen. Si el email lo publicó el PROPIO sitio del lead,
// es su contacto aunque el dominio del mail sea otro (auditoría empírica 2026-08-04).
// TLD de país (ISO 3166-1 alpha-2) + los que se usan como país sin serlo (uk, eu, su, ac).
// Sirve para descartar artefactos de texto que PARECEN email: `good@all.he` no es una
// dirección, `contacto@medio.io` sí. Solo se consulta para TLDs de dos letras.
export const TLD_PAIS_VALIDOS = new Set(("ad ae af ag ai al am ao aq ar as at au aw ax az ba bb bd be bf bg bh bi bj bl bm bn bo bq br bs bt bv bw by bz "
 + "ca cc cd cf cg ch ci ck cl cm cn co cr cu cv cw cx cy cz de dj dk dm do dz ec ee eg eh er es et fi fj fk fm fo fr "
 + "ga gb gd ge gf gg gh gi gl gm gn gp gq gr gs gt gu gw gy hk hm hn hr ht hu id ie il im in io iq ir is it je jm jo jp "
 + "ke kg kh ki km kn kp kr kw ky kz la lb lc li lk lr ls lt lu lv ly ma mc md me mf mg mh mk ml mm mn mo mp mq mr ms mt "
 + "mu mv mw mx my mz na nc ne nf ng ni nl no np nr nu nz om pa pe pf pg ph pk pl pm pn pr ps pt pw py qa re ro rs ru rw "
 + "sa sb sc sd se sg sh si sj sk sl sm sn so sr ss st sv sx sy sz tc td tf tg th tj tk tl tm tn to tr tt tv tw tz ua ug "
 + "um us uy uz va vc ve vg vi vn vu wf ws ye yt za zm zw uk eu su ac").split(/\s+/));

export function _cleanScrapedEmails(list, leadDomain, opts = {}) {
  const core = (leadDomain || "").replace(/^www\./, "").toLowerCase().trim();
  const urlByEmail = opts.urlByEmail || null;
  // Dominios que PROBAMOS que son la casa editora del lead (OWNERDOMAIN del ads.txt, el MX,
  // el developer de Google Play). No es una corazonada: son tres fuentes independientes que
  // declaran la relación. Sin esto, mobilepub@comercio.com.pe se caía por cross-domain siendo
  // el ÚNICO contacto alcanzable de peru21.pe (Cloudflare bloquea todo lo demás).
  const casasEditoras = new Set([...(opts.casasEditoras || [])].map(x => String(x).toLowerCase().replace(/^www\./, "")));
  // ¿El email salió de una página del propio sitio? (no de WHOIS, informer ni redes sociales)
  const vieneDelPropioSitio = (email) => {
    if (!urlByEmail || !core) return false;
    const u = String(urlByEmail.get?.(email) || urlByEmail[email] || "").toLowerCase();
    if (!u) return false;
    try {
      const h = new URL(u.startsWith("http") ? u : `https://${u}`).hostname.replace(/^www\./, "");
      return h === core || h.endsWith("." + core) || core.endsWith("." + h);
    } catch { return false; }
  };
  const seen = new Set();
  const valid = [];
  for (const raw of Array.isArray(list) ? list : []) {
    const e = _sanitizeEmail(raw);
    if (!e || !STRICT_EMAIL_RE.test(e) || seen.has(e)) continue;
    if (IGNORE_EMAIL.some(p => e.includes(p))) continue;
    const local = e.split("@")[0];
    const dom   = e.split("@")[1];
    // Basura por local-part: roles de registro, admin de dominio/DNS, placeholders
    if (JUNK_LOCAL_RE.test(local) || JUNK_LOCAL_TOKENS.test(local) || PLACEHOLDER_LOCAL.test(local)) continue;
    // Basura por dominio: registradores/WHOIS
    if (JUNK_EMAIL_DOMAINS.has(dom)) continue;
    // El local-part ES el dominio del sitio → artefacto de parseo, no un buzón. Medido:
    // daynight.gr → "daynight.gr@wi.daynight.gr". Sale de textos tipo "daynight.gr @ wi..."
    // o de un mailto mal armado. Se manda y rebota.
    if (core && (local === core || local === _rootDomain(core))) continue;
    const isLeadDomain     = dom === core || dom.endsWith("." + core) || core.endsWith("." + dom);
    // Maxi 2026-07-08: gmail/hotmail/outlook/yahoo/etc. PUEDEN ser el contacto real del sitio
    // (el dueño lo aclaró explícitamente) → aceptar CUALQUIER webmail no-placeholder, sin
    // exigir que "parezca persona". Los locals placeholder/junk/registro ya se filtraron arriba
    // (JUNK_LOCAL_RE / PLACEHOLDER_LOCAL). rankEmail después penaliza webmail/genérico pero NO
    // lo descarta — así no perdemos un contacto real solo por venir de un dominio genérico.
    const isPersonalWebmail = WEBMAIL_RE.test(dom);
    // Maxi 2026-07-08: aceptar cross-domain si el local es un ROL de contacto de negocio
    // (publicidade@/comercial@/info@/prensa@...). Los GRUPOS de medios centralizan el contacto
    // de publicidad en el dominio de la editora — ej. publicidade@caras.com.br para
    // aventurasnahistoria.com.br (misma Editora Caras). Antes se descartaba → quedaba SIN email.
    // El junk (noreply/webmaster/registro) ya se filtró arriba; rankEmail después lo puntúa.
    // Maxi 2026-08-04: faltaba AD_SALES_LOCAL. Solo se miraba GENERIC_LOCAL_RE, así que un
    // `inzercia@` (publicidad en eslovaco) o `hirdetes@` (húngaro) cross-domain se descartaba
    // — justo el buzón de venta de pauta que buscamos.
    const isBizRole = GENERIC_LOCAL_RE.test(local) || AD_SALES_LOCAL.test(local) || AD_SALES_CONTIENE.test(local);
    // Maxi 2026-08-04: LA PROCEDENCIA MANDA SOBRE EL DOMINIO. Un email impreso en la página de
    // contacto del propio sitio ES su contacto, aunque el buzón esté en el dominio de la casa
    // editora. Medido: el scraper ya llegaba a la página buena y extraía el mail, y esta línea
    // lo tiraba. Casos reales: hnonline.sk → inzercia@mafraslovakia.sk (publicidad en eslovaco,
    // 9 emails extraídos y devolvía []), radio1.hu → hirdetes@mediamoment.hu (venta de pauta),
    // radioagricultura.cl → vradnic@agricultura.cl (gerente general),
    // elfinancierocr.com → 7 emails @nacion.com (mismo grupo).
    // ⚠️ LA PROCEDENCIA NO PUEDE RESCATAR UNA DIRECCIÓN QUE NO EXISTE (Maxi 2026-09-01).
    // Al revivir la regla de procedencia aparecieron artefactos que antes tapaba el filtro
    // cross-domain: en ronaldo7.net salieron `1323-a-look@ronaldo-roots-and-early-days.html`
    // (un trozo de URL leído como email) y `good@all.he`. Los dos "vienen del propio sitio",
    // así que la regla los rescataba y se habrían enviado — un rebote garantizado y encima
    // ensuciando la reputación del dominio.
    // Se exige que la parte del dominio termine en algo que pueda ser un TLD de verdad: 2 a 24
    // letras, y nunca una extensión de archivo. Es barato y corta justo esta familia.
    const _tldAparente = (dom.split(".").pop() || "");
    // Un TLD de DOS letras solo es válido si es un código de país de verdad. `good@all.he`
    // salió del texto de una nota y `.he` no existe; `.fr` o `.io` sí. Los de 3+ letras se
    // aceptan salvo que sean una extensión de archivo (el caso `...-early-days.html`).
    // (No se reusa COUNTRY_CODES: tiene 86 entradas y le faltan .uk .io .tv .me .eu, así que
    // rechazaría dominios legítimos — peor que el problema que arregla.)
    const _dominioPlausible = dom.includes(".") && dom.length >= 4
      && (_tldAparente.length === 2
            ? TLD_PAIS_VALIDOS.has(_tldAparente)
            : /^[a-z]{3,24}$/.test(_tldAparente)
              && !/^(html?|php|aspx?|jsp|jpe?g|png|gif|webp|svg|css|js|json|xml|pdf|zip|mp[34]|txt|woff2?|ico|rss|amp)$/.test(_tldAparente));
    const publicadoPorElSitio = _dominioPlausible && vieneDelPropioSitio(e);
    const esCasaEditora = casasEditoras.has(dom) || [...casasEditoras].some(c => dom.endsWith("." + c));
    if (core && !isLeadDomain && !isPersonalWebmail && !isBizRole && !publicadoPorElSitio && !esCasaEditora) continue;
    seen.add(e);
    valid.push(e);
  }
  // El corte de 15 truncaba justo el buzón comercial cuando venía después de una lista de
  // vendedores (hnonline.sk tenía 14 personas y después inzercia@). Ahora el de pauta va primero.
  valid.sort((a, b) => {
    const ad = (x) => { const l = x.split("@")[0]; return (AD_SALES_LOCAL.test(l) || AD_SALES_CONTIENE.test(l)) ? 0 : 1; };
    return (ad(a) - ad(b)) || ((_isGenericEmail(a) ? 1 : 0) - (_isGenericEmail(b) ? 1 : 0));
  });
  return valid.slice(0, 15);
}

// Devuelve true si el local-part es genérico (info@, contact@, etc.). Maxi
// quiere usar el primer Informer hit si NO es genérico.
// Maxi 2026-06-17 (audit #2): regex GENERIC_LOCAL_RE como única fuente de
// verdad. Antes _isGenericLocalPart, processCsvItem y autopilot usaban regex
// DIFERENTES → un email podía ser "no-genérico" para Informer pero "genérico"
// para rankEmail. Ahora todos consultan esta constante.
export const GENERIC_LOCAL_RE = /^(info|geral|contact|contacto|contato|contatto|contattare|kontakt|kontakte|hello|hi|hola|ola|olá|support|soporte|suporte|atendimento|mail|email|e-mail|inbox|news|press|prensa|imprensa|stampa|presse|sales|ventas|comercial|marketing|publicidade|publicidad|publicite|pubblicita|werbung|admin|general|generale|reception|recepcion|recepcao|webmaster|noreply|no-reply|no_reply|donotreply|do-not-reply|abuse|hostmaster|postmaster|spam|legal|dmca|copyright|takedown|privacy|gdpr|dpo|jobs|career|hr|recruit|talents)$/i;

export function _isGenericLocalPart(email) {
  const local = (email || "").split("@")[0].toLowerCase();
  return GENERIC_LOCAL_RE.test(local);
}

// Maxi 2026-07-09: rol de VENTA DE PAUTA/PUBLICIDAD — el buzón IDEAL para ADEQ (que vende
// inventario). El user (Q4) lo eligió como "la mejor opción". Módulo-level para compartir entre
// rankEmail (score +95) y _pickTier (orden de selección). Formas ACOTADAS: nada de `ads?`/`adv`
// sueltos, que matchearían "admin"/"advisor".
// Maxi 2026-07-13 (auditoría): +cobertura del pool europeo — régie(FR), Vermarktung/Anzeigen/Verkauf(DE),
// verkoop/adverteren(NL), vente(FR), raccolta pubblicitaria(IT), auglýsingar(IS), annons(SE). Todos = venta
// de pauta/inventario. 'regie\b'/'regiepub' evita matchear 'regierung'(gobierno DE).
export const AD_SALES_LOCAL = /^(?:publicidad|publicidade|publicit[ea]|pubblicit|werbung|vermarkt|vertrieb|advertis|advert\b|\badv\b|ads\b|ad[-_.]?sales|adverten|anunci|anzeigen|reklam|iklan|regiepub|regie\b|comercial|commercial|ventas|vendas|vente|verkauf|verkoop|sales\b|salesteam|marketing|mktg?\b|monetiz|media[-_.]?sales|raccolta|auglys|annons|inventory|programmatic|patrocin|sponsor|inzerc|inzer[aá]t|hirdet|diafimisi|diafhmish|adverten|adverteren|advertentie|oglas|marknad|myynti)/i;

// Igual que AD_SALES_LOCAL pero SIN anclar al principio: el token comercial puede estar en el
// medio o al final del buzón. Medido: mobilepub@comercio.com.pe es el único contacto alcanzable
// de peru21.pe (Cloudflare bloquea el resto) y el ^ lo dejaba afuera. Lista más corta a propósito:
// sin ancla, tokens de 3 letras como "adv" o "ads" pillan cualquier palabra que los contenga.
export const AD_SALES_CONTIENE = /(?:publicidad|publicidade|pubblicit[aà]?|advertis|ad[-_.]?sales|comercial|commercial|marketing|monetiz|inzerc|hirdet|reklam|patrocin|sponsor|mediakit|media[-_.]?kit|pauta|pub)$|(?:[._-])(?:pub|adv|ads|ventas|sales|comercial|marketing)(?:[._-]|$)/i;

// Maxi 2026-07-27 (auditoría respuestas 23-27): SEGMENTOS de local-part que identifican un buzón
// de IT / infraestructura / dominios / registrar / seguridad. Nunca son contacto de venta de pauta.
// Se testea por segmento (local partido por . _ -) para agarrarlos también al final del local-part
// ("reliancedomains.admin"), no solo como prefijo. Usado por rankEmail → matchedRole="IT_INFRA".
// Deliberadamente NO incluye 2 letras ambiguas (ti, si) ni "media"/"web" (falsos positivos obvios).
export const IT_INFRA_SEGMENT = new Set([
  "it", "itsec", "itsupport", "informatica", "informatique", "informatik", "informatics",
  "sistemas", "sistema", "sistemi", "systemes", "systems", "system", "sysadmin", "sysop",
  "admin", "administrator", "administracion", "administracao",
  "tecnico", "tecnica", "tecnologia", "technique", "technik", "tech", "helpdesk",
  "seguranca", "seguridad", "seguretat", "security", "infosec", "cybersecurity",
  "redes", "network", "networking", "noc", "infra", "infraestructura", "infrastructure",
  "hosting", "hostmaster", "postmaster", "webmaster", "dns", "servidor", "server", "servers",
  "dominios", "dominio", "domeny", "domains", "domain", "domaine", "registrar", "whois",
  "wsparcie", "podpora", "poparcie", "tugi", "destek",
]);

export function isBouncedSync(email) {
  return _bouncedCache.set.has((email || "").toLowerCase());
}

export const GARBAGE_LOCAL = new RegExp("^(?:" + _GL_LOCAL_PARTS.join("|") + ")@", "i");

// Cualquier ocurrencia dentro del local-part también descarta. Captura variantes
// nuevas tipo "trustandsafety", "gdpr-mask-2025", "protect-domain", etc.
export const GARBAGE_LOCAL_CONTAINS = new RegExp([
  "abuse","domain[-._]?(?:ops|operations|abuse|admin|manager|owner)","hosting","cloudflare","cloudfront","akamai","fastly",
  "proxy","piracy","pirate","takedown","whois","gdpr","masked?","masking","anonymized?",
  // "protect" anclado a word boundary para no kill "protected-content@" o "protector@editorial.com"
  "(?:^|[._-])protect(?:ed|ion)?[-._]?(?:domain|service|service|email|mail|admin|service)?(?:$|[._-])",
  "trust[-._]?and[-._]?safety","trust[-._]?safety","safety[-._]?team",
  "unsubscribe","opt[-._]?out","removeme",
  "mailer[-._]?daemon","noreply","no[-._]?reply","donotreply","autoreply",
  "dpo","data[-._]?protection",
  // Capa adicional 2026-05-14 — palabras lexicales en local-part que indican infraestructura
  "(?:^|[._-])(?:dns|ssl|tls|cert|smtp|imap|pop3|ftp|sftp|vpn)(?:$|[._-])",
  "(?:^|[._-])(?:registrar|registry|registrant|registered)(?:$|[._-])",
  "(?:^|[._-])(?:privacy|anonym|shield|guard|hidden|undisclosed)(?:$|[._-])",
  "(?:^|[._-])(?:billing|invoice|finance|accounting|payable|treasury|cobranza|facturacion|cobros)(?:$|[._-])",
  "(?:^|[._-])(?:cdn|cloud|hosting|host|server|servers|datacenter|colo)(?:$|[._-])",
  "(?:^|[._-])(?:legal|compliance|dmca|copyright|takedown|trademark)(?:$|[._-])",
].join("|"), "i");

export const GARBAGE_DOMAIN_KEYWORDS = new RegExp(
  // Match keyword en cualquier nivel del dominio (incluyendo TLDs cortos como nic.mx).
  // (?:^|[.@]) → inicio o tras . o @
  // [a-z0-9-]* → prefijo opcional (csc en cscinfo, latin en latinregistrar)
  // (KEYWORDS) → palabra-clave
  // [a-z0-9-]* → sufijo opcional
  // (?=\\.|$) → seguido de . o fin
  "(?:^|[.@])[a-z0-9-]*(?:" + _GARBAGE_DOMAIN_KEYWORDS.join("|") + ")[a-z0-9-]*(?=\\.|$)",
  "i"
);

// Capa 3 extra: AWS / Google Cloud / Azure subdominios — captura "support.aws.com",
// "support.amazonaws.com", "abuse.cloudflare.com", etc. donde la keyword está
// en un subdominio interno (no como dominio raíz).
export const GARBAGE_DOMAIN_SUBDOMAIN = /\.(aws|amazonaws|googlecloud|azure|cloudflare|fastly|akamai)\.com$/i;

// ── CAPA 4 — Helper unificado: clasifica email como reject / low / ok ─────
// Devuelve { verdict, reason, score }
//   verdict: "reject" | "low_quality" | "ok"
//   reason:  string descriptivo (audit trail)
//   score:   guidance al ranking (negative = nunca pickear)
export function classifyEmail(email, leadDomain = "") {
  if (!email || typeof email !== "string") return { verdict: "reject", reason: "malformed_empty", score: -1 };
  const e = email.toLowerCase().trim();
  if (!e.includes("@") || e.split("@").length !== 2) return { verdict: "reject", reason: "malformed_no_at", score: -1 };
  const [local, dom] = e.split("@");
  if (!local || !dom || local.length < 2) return { verdict: "reject", reason: "malformed_short_local", score: -1 };

  // Capa 1: local-part patterns (anclado al inicio)
  if (GARBAGE_LOCAL.test(local + "@")) return { verdict: "reject", reason: "garbage_local_anchored", score: -1 };
  // Capa 2: local-part contains (cualquier posición)
  if (GARBAGE_LOCAL_CONTAINS.test(local)) return { verdict: "reject", reason: "garbage_local_contains", score: -1 };
  // Capa 3: domain keywords (registrar/privacy/hosting/cloud/etc en el dominio)
  if (GARBAGE_DOMAIN_KEYWORDS.test(dom) || GARBAGE_DOMAIN_SUBDOMAIN.test(dom)) return { verdict: "reject", reason: "garbage_domain_keywords", score: -1 };
  // Capa 3b: domain match exacto contra patterns viejos (defense-in-depth, redundante pero seguro)
  if (typeof GARBAGE_DOMAIN_PATTERN !== "undefined" && GARBAGE_DOMAIN_PATTERN.test(dom)) {
    return { verdict: "reject", reason: "garbage_domain_pattern", score: -1 };
  }
  // Capa 3c: local-part con TLD adentro (caso "site.com@registrar.com")
  if (/\.(com|net|org|io|co|tv|me|info|biz|us|uk|de|es|fr|it|br|ar|mx)$/i.test(local)) {
    return { verdict: "reject", reason: "malformed_tld_in_local", score: -1 };
  }
  // Capa 3d: cross-domain a no-webmail (defense-in-depth)
  if (leadDomain) {
    const _lead = leadDomain.toLowerCase().replace(/^www\./, "");
    const _isWebmail = /^(gmail|hotmail|outlook|live|yahoo|aol|icloud|protonmail|gmx|me)\.com$/.test(dom);
    const _domMatches = dom === _lead || dom.endsWith("." + _lead) || _lead.endsWith("." + dom);
    if (!_domMatches && !_isWebmail) return { verdict: "reject", reason: "cross_domain_recipient", score: -1 };
  }

  // Genéricos: pasa pero baja calidad — solo pickear si no hay opción mejor
  const GENERIC_RE = /^(info|contact|contacto|contato|contatto|kontakt|hello|hi|hey|hola|ola|olá|support|soporte|suporte|atendimento|mail|email|inbox|bonjour|news|press|prensa|imprensa|stampa|presse|noticias|reception|recepcion|recepcao|general|sales|ventas|marketing|publicidade|publicidad|comercial|editor|editorial|redaccion|redacao|jurídico|juridico|juridique)$/i;
  if (GENERIC_RE.test(local)) return { verdict: "low_quality", reason: "generic_role", score: 20 };

  // OK → score guidance basado en shape
  // - person-like (nombre.apellido): 80
  // - single name corto: 50
  // - otro: 40
  if (/^[a-z]+\.[a-z]+$/i.test(local)) return { verdict: "ok", reason: "person_firstname_lastname", score: 80 };
  if (/^[a-z]+_[a-z]+$/i.test(local)) return { verdict: "ok", reason: "person_underscore", score: 75 };
  if (/^[a-z]{3,15}$/i.test(local))   return { verdict: "ok", reason: "single_name", score: 50 };
  return { verdict: "ok", reason: "other", score: 40 };
}

// Rank email por probabilidad de ser un buen contacto B2B. Más alto = mejor.
// Sync, sin red. Llamado desde runAgentCycle para elegir el mejor del array.
// (No confundir con scoreEmail() async que hace SMTP verify y devuelve color/red).
// Categoría → roles ideales del local-part (un MB humano sabe esto intuitivamente)
export const CATEGORY_TARGET_ROLES = {
  news:         /^(editor|redacao|redaccion|redazione|writer|periodista|journalist|prensa|press|director|gerente)/,
  sports:       /^(marketing|comercial|sponsorship|patrocin|publicidad|ads|director|gerente|jefe)/,
  finance:      /^(marketing|comercial|business|partnerships|director|cmo|ceo)/,
  entertainment:/^(marketing|comercial|publicidad|partnerships|brand|ads|director)/,
  technology:   /^(marketing|partnerships|business|bd|growth|director)/,
  health:       /^(marketing|comercial|director|gerente|jefe)/,
  travel:       /^(marketing|comercial|partnerships|sales|business|director)/,
  food:         /^(marketing|publicidad|comercial|chef|director|brand)/,
  business:     /^(marketing|partnerships|business|bd|growth|director|cmo)/,
  automotive:   /^(marketing|comercial|publicidad|sales|director|gerente)/,
};

// ══════════════════════════════════════════════════════════════════════════════════════════
// DETECCIÓN DE EMAILS FALSOS / PHISHING (Maxi 2026-08-04, pedido del user)
// ══════════════════════════════════════════════════════════════════════════════════════════
// Un email scrapeado de una web ajena puede ser una trampa: sitios comprometidos, comentarios
// de spam indexados, o dominios lookalike puestos justamente para que un bot les escriba.
// Escribirle a uno de esos no solo se desperdicia: nos mete en listas de spam y ensucia la
// reputación de los buzones, que es lo que venimos peleando.
// Devuelve "" si está limpio, o el motivo si es sospechoso.
export const _TLD_ALTO_RIESGO = /\.(tk|ml|ga|cf|gq|top|xyz|click|link|work|loan|men|date|racing|win|bid|stream|download|review|country|kim|science|party|gdn|mom|rest|fit|cyou|icu|sbs|buzz|monster|quest)$/i;

export const _MARCAS_SUPLANTADAS = /(paypal|apple|micros0ft|microsofl|goog1e|gooogle|faceb00k|arnazon|amaz0n|netfIix|whatsapp|binance|coinbase|metamask|bancolombia|santander|bbva|mercadopago|correos|dhl|fedex)/i;

export function detectarEmailSospechoso(email, siteDomain = "") {
  const lower = String(email || "").toLowerCase().trim();
  const [local, dom] = lower.split("@");
  if (!local || !dom) return "malformado";

  // 1. TLD de alto riesgo: gratuitos o históricamente abusados por phishing.
  if (_TLD_ALTO_RIESGO.test(dom)) return `tld_riesgoso:${dom.split(".").pop()}`;

  // 2. Suplantación de marca conocida — el dominio IMITA a una marca grande con typos.
  //    Un publisher legítimo nunca tiene un email en un dominio así.
  //    Normalizamos homóglifos ANTES de comparar: "paypaI-secure.com" usa una I mayúscula que al
  //    pasar a minúscula queda "i" y esquivaba el patrón. Lo mismo con 0→o, 1→l, 5→s.
  // Normalización de homóglifos: los pares que el ojo confunde en un renglón de texto.
  //   1/i/| → l   ·   0 → o   ·   rn → m (el clásico)   ·   vv → w   ·   5/$ → s
  // Solo se usa para comparar contra la lista fija de marcas grandes, así que el riesgo de
  // falso positivo es mínimo: un dominio legítimo tendría que normalizar EXACTAMENTE a una marca.
  // Casos que caza: paypaI-secure.com (i mayúscula) y bancolornbia.com (rn por m).
  // Se prueban VARIAS normalizaciones por separado, no una sola encadenada: aplicar todas juntas
  // se pisa a sí misma. Ej: "bancolornbia" → rn→m da "bancolombia" (correcto), pero si después
  // corre i→l queda "bancolombla" y ya no matchea. Cada variante ataca un tipo de homóglifo.
  const _variantes = [
    dom,
    dom.replace(/rn/g, "m").replace(/vv/g, "w"),                       // rn→m, vv→w
    dom.replace(/[1|]/g, "l").replace(/0/g, "o").replace(/5/g, "s")
       .replace(/3/g, "e").replace(/4/g, "a"),                          // dígitos por letras
    dom.replace(/i/g, "l"),                                            // I mayúscula leída como i
    dom.replace(/rn/g, "m").replace(/[1|]/g, "l").replace(/0/g, "o"),  // combinada
  ];
  if (_variantes.some(v => _MARCAS_SUPLANTADAS.test(v))) return `marca_suplantada:${dom}`;

  // 3. Lookalike del PROPIO sitio: el dominio del email se parece al del lead pero no es igual.
  //    Es la trampa clásica: en el sitio real ponen un contacto en un dominio casi idéntico.
  //    Ej: lead=clarin.com, email=@clar1n.com o @clarin-noticias.com
  const site = String(siteDomain || "").toLowerCase().replace(/^www\./, "");
  if (site && dom !== site && !dom.endsWith("." + site) && !site.endsWith("." + dom)) {
    const marcaSitio = (site.split(".")[0] || "");
    const marcaMail  = (dom.split(".")[0] || "");
    if (marcaSitio.length >= 5 && marcaMail.length >= 5 && marcaSitio !== marcaMail) {
      // Distancia de edición chica entre las marcas = imitación, no otra empresa.
      const dist = (a, b) => {
        const m = Array.from({ length: b.length + 1 }, (_, i) => [i, ...Array(a.length).fill(0)]);
        for (let j = 1; j <= a.length; j++) m[0][j] = j;
        for (let i = 1; i <= b.length; i++) for (let j = 1; j <= a.length; j++)
          m[i][j] = Math.min(m[i-1][j] + 1, m[i][j-1] + 1, m[i-1][j-1] + (a[j-1] === b[i-1] ? 0 : 1));
        return m[b.length][a.length];
      };
      const d = dist(marcaSitio, marcaMail);
      if (d > 0 && d <= 2) return `lookalike_del_sitio:${dom}_vs_${site}`;
      // Typosquat por añadido: la marca del sitio está DENTRO del dominio del email junto con una
      // palabra de cebo ("clarin-noticia.com" para clarin.com). Solo se marca si el agregado es
      // una palabra de las que usa el phishing — un "lanacionmas.com" legítimo no cae acá.
      const _cebo = /(noticia|noticias|news|oficial|official|secure|seguro|login|verify|soporte|support|help|account|cuenta|pago|pay|billing|update|alerta|alert|premium|vip|online)/i;
      if ((marcaMail.includes(marcaSitio) || marcaSitio.includes(marcaMail)) && marcaMail !== marcaSitio && _cebo.test(marcaMail))
        return `typosquat_con_cebo:${dom}_vs_${site}`;
    }
  }

  // 4. Caracteres no ASCII en el dominio (homógrafos: cirílico "а" que parece latina "a").
  if (/[^\x00-\x7F]/.test(dom)) return "homografo_no_ascii";

  // 5. Local-part con pinta de cebo automatizado.
  if (/^(abuse|phish|spam|fraud|scam|verify|verification|secure|security-?alert|account-?update|billing-?update|confirm|unlock|suspended)([._-]|$)/i.test(local))
    return `local_cebo:${local.slice(0, 20)}`;

  // 6. Dominio muy largo con muchos guiones: patrón típico de dominio desechable de campaña.
  if (dom.length >= 32 && (dom.match(/-/g) || []).length >= 3) return "dominio_sospechoso_largo";

  return "";
}

export function rankEmail(email, siteDomain, leadCategory = "", casasEditoras = null) {
  if (!email || typeof email !== "string" || !email.includes("@")) return -1;
  const lower = email.toLowerCase();
  if (GARBAGE_LOCAL.test(lower) || GARBAGE_DOMAIN_PATTERN.test(lower)) return -1;
  if (isBouncedSync(lower)) return -1; // hard reject: ya bounceó antes
  // Dominio quemado: 2+ rebotes distintos ahí. El rebote casi nunca es de la casilla sino del
  // dominio, así que insistir con otro buzón del mismo lugar es tirar reputación (2026-08-04).
  const _rd = riesgoRebotePorDominio(lower.split("@")[1] || "");
  if (_rd.bloquear) return -1;
  const [local, dom] = lower.split("@");
  if (!local || !dom) return -1;
  if (GARBAGE_LOCAL_CONTAINS.test(local)) return -1;
  // Veto por phishing / suplantación (pedido del user 2026-08-04). Escribirle a una trampa nos
  // mete en listas de spam, que es justo lo que venimos peleando.
  const _sosp = detectarEmailSospechoso(lower, siteDomain);
  if (_sosp) return -1;
  // Local-part de 1-2 caracteres ("a@olm.vn", "66@manhuaren.com") = artefacto de scrape,
  // nunca un contacto real. Se permiten 2 chars solo si son iniciales con punto (j.p@).
  // Maxi 2026-08-04: la regla original rechazaba TODO local de ≤2 caracteres y se llevaba
  // puestos `hi@thevocket.com` y `tu@fpt.com`, que son direcciones de contacto reales (muy
  // comunes en sitios chicos y startups). Ahora solo cae 1 carácter, o 2 si trae dígitos
  // (`a1@`, `66@` = artefacto de scrape).
  const _localSinSep = local.replace(/[._-]/g, "");
  if (_localSinSep.length <= 1) return -1;
  if (_localSinSep.length === 2 && /\d/.test(_localSinSep)) return -1;
  if (GARBAGE_DOMAIN_KEYWORDS.test(dom) || GARBAGE_DOMAIN_SUBDOMAIN.test(dom)) return -1; // Capa 3: keywords/subdominios garbage

  // Malformed local-part: contiene TLD (.com/.net/.io/etc) → scrape artifact
  // Caso real 2026-05-13: "lindaikejisblog.com@protecteddomainservices.com"
  // donde el scraper agarro "site.com@registrar.com" como un solo email.
  // Maxi 2026-08-04: EXCEPCIÓN — "el dominio del sitio como local-part de un freemail" es un
  // patrón REAL en publishers chicos: brainberries.co@gmail.com, ajnn.net@gmail.com. La regla
  // existe para cazar artefactos de scrape tipo "site.com@registrar.com", así que solo aplica
  // cuando el destinatario NO es un webmail.
  if (/\.(com|net|org|io|co|tv|me|info|biz|us|uk|de|es|fr|it|br|ar|mx)$/i.test(local)
      && !/^(gmail|googlemail|hotmail|outlook|live|yahoo|ymail|aol|icloud|protonmail|gmx|yandex)\./i.test(dom)) return -1;

  // Hash/random-string detection: emails como "a8f9d2k1@x.com" probablemente auto-gen.
  // Maxi 2026-08-04 — ESTA LÍNEA ERA EL BUG MÁS CARO DEL RANKING.
  // Antes: `/^[a-z0-9]{8,}$/ && !/[aeiou]{2}/` → rechazaba cualquier local de 8+ caracteres sin
  // DOS VOCALES SEGUIDAS. La intención era cazar hashes tipo "x7k9m2p4", pero mataba justo los
  // buzones que buscamos, porque en español/italiano/portugués/neerlandés/polaco las palabras
  // alternan consonante-vocal:
  //   publicidad · advertising · advertise · marketing · pubblicita · contacto · privacidad
  //   klantenservice · reklamacje · yrityspalvelu
  // Medido: de 46 leads donde SÍ existía un buzón comercial, el agente solo le escribió a 18.
  // Los otros 28 tenían publicidad@ / advertising@ / marketing@ y se descartaban con score -1,
  // mandando el pitch a info@ o support@ en su lugar.
  // Ahora se mide la PROPORCIÓN de vocales, que es lo que de verdad separa una palabra de un
  // hash: "publicidad" tiene 40% de vocales, "x7k9m2p4" tiene 0%.
  const _soloLetras = local.replace(/[^a-z]/g, "");
  const _vocales = (local.match(/[aeiou]/g) || []).length;
  const _ratioVocal = local.length ? _vocales / local.length : 0;
  if (/^[a-z0-9]{8,}$/.test(local) && _ratioVocal < 0.22 && _soloLetras.length < local.length * 0.8) return -1;
  if (/^[a-z0-9]{8,}$/.test(local) && _ratioVocal < 0.15) return -1;   // solo letras pero sin vocales = hash

  // Maxi 2026-07-13 (auditoría 48h): rechazo DURO SOLO de lo que NUNCA es un contacto real:
  //  a) PLACEHOLDERS/FALSOS que se colaban como "persona" (vorname.name@/firstname.lastname@/
  //     nombre.apellido@ matcheaban firstname.lastname → +70 → se enviaban). PLACEHOLDER_LOCAL y
  //     JUNK_LOCAL_* existían pero NO se llamaban desde rankEmail.
  //  b) WHOIS/gestión de DOMINIO (domainmanagement@axa, dominios.lantik@bizkaia): son del registrar.
  // OJO (feedback user 2026-07-13): sistemas@/system@/IT/gmail NO van acá — en un medio chico pueden
  // ser un contacto real ("sistemas.diariodovale@ no lo veo mal") → van a PENALTY abajo, no a reject.
  if (PLACEHOLDER_LOCAL.test(local) || JUNK_LOCAL_RE.test(local) || JUNK_LOCAL_TOKENS.test(local)) return -1;
  if (/^(domainmanagement|domainadmin|domainname|dominios?)([._-]|$)/i.test(local)) return -1;
  // ── ARTEFACTOS DE ESCAPE HTML/JSON (Maxi 2026-08-31) ──────────────────────────────
  // Caso real: le escribimos a `u003eenquiry@mytvsuper.com` y rebotó. `\u003e` es un ">"
  // escapado en JSON; al leer el sitio se perdió la barra y quedó pegado al local-part.
  // Puntuaba 40 y se enviaba. La dirección de verdad existe (`enquiry@`), así que el
  // scrape no solo mandó a una dirección rota: además quemó la buena, porque el dominio
  // suma un rebote. Cubre \u003e (>), \u0026 (&), \u0027 ('), \u003c (<) y familia.
  if (/^[ux]00[0-9a-f]{2}./i.test(local)) return -1;
  // Buzones de informes DMARC/SPF: los llena un robot con XML todos los días y no los lee
  // nadie. `dmarcreport@opopular.com.br` puntuaba 95 —el ranking lo leía como nombre de
  // persona— y se envió. `dmca` ya estaba en la lista de basura; `dmarc` no.
  if (/(^|[._-])(dmarc|spf|rua|ruf)([._-]|report|rep|$)/i.test(local)) return -1;
  // Maxi 2026-07-14 (auditoría rebotes 11-15/07): "owner@" bare = etiqueta de WHOIS/informer, NO un
  // buzón real → rebotó 4/4 (cnnturk/expansion/arealme/vetogate). El ranking lo tomaba como EXEC (+90)
  // y lo mandaba primero. Un dueño real escribe desde su nombre, no owner@. Reject (source-agnóstico).
  if (/^owner$/.test(local)) return -1;
  // Maxi 2026-07-24 (auditoría 22-24/07): casos basura que SÍ se enviaron.
  //  a) local de UNA sola letra ("a@olm.vn") → scrape roto, nunca un buzón real.
  if (/^[a-z0-9]$/i.test(local)) return -1;
  //  b) roles genéricos que NO son contacto comercial: cuenta/account (corotos.com.do → cuenta@gmail),
  //     alumno/student/estudiante (sportlife → alumno@), socio/member, cliente/customer. Reject duro.
  if (/^(cuenta|account|alumno|alumna|student|estudiante|aluno|socio|member|miembro|cliente|customer|usuario|user|abonado|suscriptor|subscriber|lector|reader|visitante|visitor)s?$/i.test(local)) return -1;
  //  c) FREEMAIL (gmail/hotmail/…) cuyo local es una palabra genérica de contacto — un negocio real
  //     no usa cuenta@gmail/info@gmail para vender. Si es freemail Y el local es genérico → reject.
  //     (Un freemail con NOMBRE de persona —juanperez@gmail— sigue pasando: puede ser un medio chico.)
  //     Regex inline: la const isFreeWebmail se declara más abajo (evita TDZ).
  if (/^(gmail|yahoo|ymail|hotmail|outlook|live|aol|icloud|protonmail|gmx|yandex)\.|^mail\.ru$/i.test(dom) && _isGenericLocalPart(`${local}@x.com`)) return -1;

  let score = 0;
  const cleanSite = (siteDomain || "").replace(/^www\./, "");

  // ── DOMAIN MATCH (peso 0-40) ──
  // Email del MISMO dominio del sitio → señal MUY fuerte (es probablemente real)
  const isFreeWebmail = /^(gmail|yahoo|hotmail|outlook|live|aol|icloud|protonmail|gmx|mail\.ru|yandex|me)\./.test(dom);
  let isCrossDomainCorporate = false;
  if (cleanSite) {
    if (dom === cleanSite) score += 40;
    else if (dom.endsWith("." + cleanSite) || cleanSite.endsWith("." + dom)) score += 35;
    else if (isFreeWebmail) {
      // Webmail cross-domain — penalty intermedio (-15). Es esperado que un
      // contacto B2B use gmail personal. CORRECCIÓN audit #5 (Maxi 2026-06-17):
      // si después detectamos que es EXEC/COMMERCIAL/EDITORIAL, REVERTIMOS el
      // penalty (un founder@gmail NO merece -15 — es una persona real). Por
      // eso aplicamos -15 aquí y abajo lo cancelamos si role matchea.
      score -= 15;
    } else if (_brandMatches(lower, cleanSite)) {
      // Maxi 2026-08-04: MISMA MARCA, otro TLD. No es "otra empresa": es el mismo publisher con
      // su sitio de otro país. Antes se llevaba -50 y quedaba descartado, y en varios casos era
      // el ÚNICO email del lead, así que el lead entero se perdía. Casos reales medidos:
      //   pepper.com        → contato@pepper.com.br
      //   altalex.com       → info@altalex.es
      //   giga.de           → contato@giga.com.br
      //   voetbalprimeur.be → info@voetbalprimeur.nl
      // Usa el mismo _brandMatches que ya valida el envío, así los dos criterios coinciden.
      score += 25;
    } else if (casasEditoras && [...casasEditoras].some(c => {
      const _c = String(c).toLowerCase().replace(/^www\./, "");
      return dom === _c || dom.endsWith("." + _c);
    })) {
      // Maxi 2026-08-07: la CASA EDITORA probada tampoco es "otra empresa". El scraper ya la
      // valida contra tres fuentes independientes (OWNERDOMAIN del ads.txt, el MX, el developer
      // de Google Play) y _cleanScrapedEmails la deja pasar — pero acá se llevaba -50 igual y el
      // lead quedaba en cero, porque polishPool descarta todo lo que puntúa <= 0.
      // Medido: apotheken-umschau.de (la revista de salud más grande de Alemania) devuelve
      // vertrieb@wubv.de e info@wubv.de — Wort & Bild Verlag, su editorial. Los cuatro emails
      // se encontraban y los cuatro se tiraban acá.
      score += 20;
    } else {
      // Cross-domain a OTRO dominio corporativo — penalidad fuerte. Marcamos
      // para revertir parcialmente si es EXEC (founder@otra-empresa puede ser
      // un advisor/board member real).
      score -= 50;
      isCrossDomainCorporate = true;
    }
  }

  // ── ROLE QUALITY (peso -20 a +90) ──
  // Roles comerciales = target ideal (decision-makers de monetización)
  // Roles comerciales/publicidad — cobertura multi-idioma (EN/ES/PT/IT/PL/ID/DE/FR).
  // Stems como prefijo (sin \b) para agarrar "anuncios", "publicidade", "advertising".
  // bd/head requieren boundary (cortos, evitan falsos positivos tipo "headlines").
  // Maxi 2026-07-09: AD_SALES = buzón de VENTA DE PAUTA/PUBLICIDAD. El user eligió "rol
  // comercial/publicidad" como la MEJOR opción (ADEQ vende inventario → este buzón va directo
  // a quien compra/vende espacios). Rankea por ENCIMA de EXEC. Locals específicos de ad-sales
  // (no el "director/manager" genérico, que queda en COMMERCIAL).
  // Regex hoisteado a módulo (AD_SALES_LOCAL) — misma fuente de verdad que _pickTier.
  const AD_SALES   = AD_SALES_LOCAL;
  // El buzón comercial con el departamento adelante (Maxi 2026-09-04). AD_SALES está anclado
  // al principio del local, así que `departamentocomercial@grupocronica.com.ar` —el contacto
  // que el media buyer usó para baenegocios.com— no matcheaba nada, caía en "parece persona"
  // y, por ser de otro dominio, terminaba en -50. Es el mismo buzón de venta de pauta con
  // una palabra adelante.
  // Y con la palabra ATRÁS también: `mcobian.comercial@elespanol.com` es la persona del área
  // comercial del grupo editor de redaccionmedica.com. Se mira cada trozo del local por
  // separado (partiendo por . _ -), así "comercial" cuenta adelante, atrás o en el medio, pero
  // "comerciales" o "noticiascomerciales" no.
  const _SEG_COMERCIAL = /^(?:comercial|commercial|publicidad|publicidade|pubblicita|publicite|ventas|vendas|sales|adsales|marketing|advertising|anuncios|anunciantes|reklama|werbung|pauta)$/i;
  const AD_SALES_DEPTO = {
    test: (l) => /^(?:departamento|depto|dpto|dept|area|setor|sector|equipo|equipe|team|servicio|service|oficina|gerencia|direccion|diretoria)[._-]?(?:comercial|commercial|publicidad|publicidade|publicit|pubblicit|ventas|vendas|sales|marketing|advertis|anunci|reklam|werbung)/i.test(l)
             || (l.includes(".") || l.includes("_") || l.includes("-")) && l.split(/[._-]+/).some(seg => _SEG_COMERCIAL.test(seg)),
  };
  const COMMERCIAL = /^(?:(?:business|partnership|partner|propaganda|director|gerente|manager|jefe|brand|media)|(?:bd|head)\b)/i;
  // Maxi 2026-08-31: faltaban los idiomas europeos. `redazione` (it) ya estaba pero moría en la
  // lista negra; `redaktion` (de), `redaction` (fr), `redactie` (nl), `redakcja` (pl), `redakce`
  // (cz), `szerkesztoseg` (hu) y `syntaxi` (gr) no figuraban en ningún lado. Y `editorial` no
  // entraba por el `\b`: después de "editor" viene una "i", que es carácter de palabra, así que
  // la alternativa fallaba y caía en PERSON_LIKELY (+55) en vez de EDITORIAL (+75).
  // `presse` (fr), `imprensa` (pt), `stampa` (it), `basin` (tr), `sajto` (hu), `tisk` (cz): la
  // casilla de prensa en los idiomas del pool. Estaban en la lista de GENÉRICOS y no acá, así
  // que `press@` valía 115 y `presse@` 55 — la misma palabra, dos puntajes, según el idioma.
  // Es el mismo error que se arregló con `redazione` el 31/08. (Maxi 2026-09-04)
  const EDITORIAL  = /^(editor|editorial|editor-in-chief|chief-editor|redacao|redaccion|redazione|redaktion|redaction|redactie|redakcja|redakce|redaktsiya|szerkesztoseg|syntaxi|toimitus|newsroom|writer|periodista|journalist|prensa|press|presse|imprensa|stampa|basin|sajto|tisk|reporter|news-?desk)\b/;
  const EXEC       = /^(ceo|cmo|cto|coo|founder|co-?founder|owner|publisher|presidente|president)\b/;

  // ORDEN: chequear generics PRIMERO (antes que "single name"), sino palabras
  // tipo "contato" se cuelan como single-name con score alto en lugar de role.
  // Maxi 2026-06-17 (audit #2): usa GENERIC_LOCAL_RE unificada.
  const IS_GENERIC = GENERIC_LOCAL_RE;

  let matchedRole = "";
  if (AD_SALES.test(local) || AD_SALES_DEPTO.test(local)) { score += 95; matchedRole = "AD_SALES"; }    // publicidad@/comercial@/ads@ = target ideal ADEQ
  else if (EXEC.test(local))       { score += 90; matchedRole = "EXEC"; }       // CEO/founder = jackpot
  else if (COMMERCIAL.test(local)) { score += 80; matchedRole = "COMMERCIAL"; }
  // ── ORDEN DEL MEDIA BUYER (Maxi 2026-08-25, textual) ────────────────────────────────
  // "Yo voy por el correo que dice el nombre del espacio con el que me quiero conectar, ya
  //  sea el webmaster, editor, sales, encargado de ventas; o de última, si no hay, info o
  //  contacto, o algún nombre como juan@aliados.com."
  // O sea: primero el rol que NOMBRA el espacio, y recién después el genérico o la persona.
  // EDITORIAL sube de 60 a 75 para quedar por encima de PERSON (70): un `editor@` es un rol
  // nombrado, `juan.perez@` es la alternativa. Y WEBMASTER sale de la bolsa de genéricos,
  // donde valía 15 — el MB lo nombra como objetivo principal y el código lo trataba como
  // `info@`. (Hoy no hay ninguno en el pool, así que esto no cambia nada de inmediato: es
  // para que cuando aparezca uno, se lo trate como lo que es.)
  else if (EDITORIAL.test(local))  { score += 75; matchedRole = "EDITORIAL"; }
  // Redacción/prensa con un prefijo de región o idioma: `lat.press@motorsport.com`,
  // `fr.press@`, `es.redaccion@`. La regla de arriba está anclada al principio y estos caían
  // como "persona" (lat + press) hasta que se dejó de aceptar un genérico como apellido; sin
  // esto quedaban sin rol, en 35. Mismo criterio que el comercial por segmento. (2026-09-04)
  else if (/[._-]/.test(local) && local.split(/[._-]+/).some(seg => seg.length >= 4 && EDITORIAL.test(seg))) { score += 75; matchedRole = "EDITORIAL"; }
  else if (/^webmaster([._-]|$)/i.test(local)) { score += 72; matchedRole = "WEBMASTER"; }
  // Maxi 2026-07-24 (auditoría respuestas 22-24): direcciones de DEPARTAMENTO / atención al
  // cliente que NO son contacto de venta de pauta y ni rebotan ni responden a un pitch de
  // inventario (denuncias@, soporte.epaper@, bok@, cskh@, rrhh@, cobranzas@…). Antes caían en
  // PERSON (+70, por el punto en "soporte.epaper") o PERSON_LIKELY (+55, "denuncias") y le
  // GANABAN a un contacto real. Ahora +8: sendables como ÚLTIMO recurso (North Star: ≥1 email),
  // pero pierden contra cualquier persona/rol comercial. Chequeado ANTES del patrón nombre.apellido.
  // ── MESA DE AYUDA: NO SE LE ESCRIBE (Maxi 2026-08-11) ─────────────────────
  // Medido sobre las 39 "respuestas reales" de junio-agosto: casi TODAS venían de
  // ajuda@, apoyo@, soporte@, support@, service@, abonnements@, bok@, csmtix@,
  // suport@. No eran respuestas comerciales: eran ACUSES DE TICKET automáticos, que
  // el clasificador no cazaba porque no traen cabecera de auto-reply ni asunto de
  // fuera-de-oficina. Es decir, el sistema se estaba auto-reportando éxito mientras
  // el dueño veía cero negociaciones — y tenía razón él.
  // Escribirle al buzón de reclamos no solo no vende: mete un pitch en la cola de
  // atención al cliente del publisher, que es la peor primera impresión posible.
  // Score NEGATIVO = rankEmail lo descarta. Preferimos no mandar antes que mandar acá.
  // `ouvidoria` (la defensoría del lector en Brasil), `ombudsman` y `complaints` son la mesa de
  // reclamos con otro nombre (Maxi 2026-09-04): `ouvidoria@` puntuaba 95 por parecer un nombre.
  else if (/^(soporte|suporte|support|suport|atencion|atenci[oó]n|atendimento|ajuda|apoyo|denuncias?|reclamos?|reclama[cç][õo]es|ouvidoria|ombudsman|complaints?|abonnements?|suscripciones|assinaturas|cobran[zc]as|cobran[çc]a|facturaci[oó]n|faturamento|billing|pedidos|env[ií]os|devoluciones|postvent[ao]|posvent[ao]|\bsac\b|\bbok\b|cskh|helpdesk|help|servicios?|servico|service|tickets?|customer[a-z]*|cliente[a-z]*|servicedesk)([._-]|$)/i.test(local)) { score -= 20; matchedRole = "MESA_DE_AYUDA"; }
  // Otros departamentos que no son mesa de ayuda: no venden pauta, pero tampoco
  // ensucian una cola de soporte. Siguen sendables como último recurso (North Star: ≥1 email).
  // ── ÁREAS QUE LOS MB DESCARTAN SIEMPRE (Maxi 2026-08-25) ────────────────────────────
  // Agus: "descarto las que son de otras áreas: suscripción, noticias, finanzas, atención al
  // cliente". Diego: "descarto info, contacto, soporte, etc."
  // `soporte@` y `suscripciones@` ya caían en MESA_DE_AYUDA (-20). Faltaban estas dos, que
  // estaban en +15 junto a `info@` — o sea que podían ganarle a nada y recibir el pitch.
  // Un buzón de redacción o de contabilidad no compra publicidad NUNCA: no es "peor que un
  // comercial", es que no es el interlocutor. Negativo = descartado.
  // (`prensa@`/`press@` NO entra acá: es un buzón atendido por humanos que sí reenvía.)
  // `redaccion` NO está acá: la agarra antes EDITORIAL (+75), que es lo que Agus quiere.
  else if (/^(noticias?|news|newsletter)([._-]|$)/i.test(local)) { score -= 10; matchedRole = "AREA_EQUIVOCADA"; }
  else if (/^(finan|contab|administracion|administrativo|tesoreria|pagos?)/i.test(local)) { score -= 10; matchedRole = "AREA_EQUIVOCADA"; }
  else if (/^(rrhh|recursoshumanos|empleos?|jobs|careers|trabaj[ao]|legal|privacy|privacidad|privacidade|\bdpo\b|abuse)([._-]|$)/i.test(local)) { score += 8; matchedRole = "DEPARTMENT"; }
  // Maxi 2026-07-27 (auditoría respuestas 23-27): buzones de IT / infraestructura / dominios /
  // registrar. NO son contacto de venta de pauta y jamás responden un pitch de inventario; peor,
  // varios son direcciones técnicas donde el mail se archiva o abre un ticket. Casos reales de
  // esta tanda: reliancedomains.admin@ril.com y drc.seguranca@cuf.pt caían en PERSON (+70, por el
  // punto en el local-part) y informatique@ / domeny@ / wsparcie@ en PERSON_LIKELY (+55) — le
  // GANABAN a cualquier contacto comercial del mismo dominio. Chequeo por SEGMENTO (partiendo por
  // . _ -) para agarrarlos también cuando van al final ("...domains.admin"). Score +5: sendables
  // como último recurso (North Star: ≥1 email), pero pierden contra todo lo demás.
  // Va ANTES del patrón nombre.apellido, igual que DEPARTMENT.
  else if (local.split(/[._-]+/).some(seg => IT_INFRA_SEGMENT.has(seg))) { score += 5; matchedRole = "IT_INFRA"; }
  // Un genérico con sufijo o prefijo de región (`info.lat@`, `contacto.mx@`, `gq.contacto@`)
  // es un genérico, no una persona llamada "info lat". Va antes del patrón nombre.apellido.
  else if (/[._-]/.test(local) && local.split(/[._-]+/).some(seg => IS_GENERIC.test(seg))) score += 15;
  // Pattern firstname.lastname (juan.perez@x.com) = persona real.
  // También inicial.apellido (j.perez@, m.rossi@): es el formato corporativo más común en
  // Italia, Francia y Alemania, y el patrón exigía dos letras antes del punto, así que
  // `f.puglisi@iltempo.it` —el contacto que usó el media buyer— valía 40 y `fpuglisi@` 95.
  // La misma persona, dos puntajes. Se excluye que el segundo trozo sea un genérico
  // (`e.mail`, `i.info`) para no fabricar personas de artefactos. (Maxi 2026-09-04)
  else if (/^[a-z]+[._-][a-z]{2,}$/.test(local) && !IS_GENERIC.test(local.split(/[._-]/)[1])) { score += 70; matchedRole = "PERSON"; }
  // Pattern firstinitial+lastname (jperez@x.com, mgarcia@x.com) = común corp
  else if (/^[a-z][a-z]{4,14}$/.test(local) && local.length >= 5 && /[aeiou]/.test(local) && !IS_GENERIC.test(local)) { score += 55; matchedRole = "PERSON_LIKELY"; }
  // Generics — OK pero baja conversión. Cobertura multi-idioma (PT/IT/FR/DE/ES).
  // CHEQUEADO ANTES que single-name para que "contato" no se cuele como persona.
  else if (IS_GENERIC.test(local)) score += 15;
  // ── LOCALES DE 2-3 LETRAS: 19% DE REBOTE (Maxi 2026-09-02) ────────────────────────────
  // Medido sobre 30 días: los locales de hasta 3 caracteres rebotan al 19,0%, contra 4,5% de
  // los comerciales y 0% de los editoriales. Mirando cuáles son, se entiende: `gp@`, `al@`,
  // `v_t@`, `tld@`, `it@` — iniciales y códigos de departamento raspados del texto, no
  // buzones publicados.
  // NO se rechazan: `ads@shorouknews.com` también es de 3 letras y es exactamente el buzón de
  // venta de pauta que buscamos. Por eso los roles conocidos (AD_SALES, EXEC, COMMERCIAL,
  // EDITORIAL) ya puntuaron arriba y no llegan hasta acá; este castigo solo alcanza al que no
  // se pudo identificar como nada. Queda de último recurso en vez de competir de igual a igual.
  // Single name (juan@x.com) — could be person or generic
  else if (/^[a-z]{3,12}$/.test(local) && /[aeiou]/.test(local)) { score += 30; matchedRole = "SINGLE_NAME"; }

  // Maxi 2026-06-17 (audit #5): si el local-part identificó una PERSONA REAL
  // (EXEC/COMMERCIAL/EDITORIAL/PERSON), REVERTIR el penalty de webmail
  // cross-domain (era -15). Un founder@gmail SÍ es una persona válida.
  // Para cross-corporate (-50): revertir parcialmente (-25) si es EXEC, ya
  // que founder@otra-empresa puede ser advisor/board (real pero menos directo).
  // ── LOCALES DE 2-3 LETRAS: 19% DE REBOTE (Maxi 2026-09-02) ────────────────────────────
  // Medido sobre 30 días: los locales de hasta 3 caracteres rebotan al 19,0%, contra 4,5% de
  // los comerciales y 0% de los editoriales. Mirando cuáles son se entiende por qué: `gp@`,
  // `al@`, `v_t@`, `tld@`, `it@` — iniciales y códigos de departamento raspados del texto,
  // no buzones publicados.
  // Va DESPUÉS de la cadena de roles y no adentro: `tld@` e `it@` los agarraba la rama de
  // genéricos (+15) antes de llegar al castigo, así que dependía de qué rama matcheara primero.
  // Se exceptúan los roles que SÍ identificamos —`ads@` es de 3 letras y es exactamente el
  // buzón de venta de pauta que buscamos, y rebota poco—. El castigo alcanza solo a lo que no
  // se pudo identificar como nada: queda de último recurso en vez de competir de igual a igual.
  // Abreviaturas de rol que SÍ son un contacto real y miden 2-3 letras: `pr@` (prensa /
  // relaciones públicas), `rp@`, `mkt@`, `com@`. Sin esta excepción se perdía `pr@`, que es
  // un buzón atendido por una persona del medio.
  const _ROLES_CORTOS_OK = /^(pr|rp|mkt|com|ads|sac)$/;
  if (_localSinSep.length <= 3 && !_ROLES_CORTOS_OK.test(_localSinSep)
      && !["AD_SALES", "EXEC", "COMMERCIAL", "EDITORIAL"].includes(matchedRole)) {
    score -= 40;
    matchedRole = "INICIALES_SOSPECHOSAS";
  }

  // ── EL DUEÑO CON UN GMAIL RESPONDE EL DOBLE (Maxi 2026-09-04, medido) ──────────────────
  // Sobre 2.597 envíos con resultado: las direcciones de webmail (gmail/hotmail/yahoo)
  // tuvieron 7,7% de respuesta real y 1,8% de rebote; todo lo demás, 3,5% y 7,3%. Es la regla
  // textual de Diego —"si no hay, prefiero los de gmail"— validada con datos. Y en el hueco
  // contra el CRM, el ranking había tirado `rsmaxit@gmail.com`, `adrianofrazao@gmail.com`,
  // `vietnamplus2008@gmail.com`, `agazeta.jornal@gmail.com`: exactamente las direcciones que
  // el media buyer terminó usando.
  // Antes: persona@gmail = 55 − 15 (cross) − 20 (webmail) = 20, debajo de info@ (55). Ahora una
  // PERSONA o un rol con webmail cancela el -15 y suma 10: queda por encima del genérico y
  // por debajo de la misma persona en el dominio propio (95). El orden del MB, medido en sus
  // elecciones: comercial > persona@webmail > info@.
  const _esPersonaORol = ["AD_SALES", "EXEC", "COMMERCIAL", "EDITORIAL", "PERSON", "PERSON_LIKELY", "SINGLE_NAME"].includes(matchedRole);
  // El nombre del MEDIO dentro del local de un webmail (`agazeta.jornal@gmail.com`,
  // `diarioexample@gmail.com`) es el buzón del propio sitio alojado en gmail: en LATAM y en
  // Europa del Este es la norma para un medio chico. Se trata como contacto del sitio.
  const _brandSitio = _brandStripTld(cleanSite).replace(/[^a-z0-9]/g, "");
  const _marcaEnLocal = _brandSitio.length >= 4 && local.replace(/[^a-z0-9]/g, "").includes(_brandSitio);
  const _webmailDelSitio = isFreeWebmail && _marcaEnLocal;
  // Y el mismo caso en el dominio del GRUPO editor: `contacto.topgear@henneomagazines.com` es
  // el buzón de topgear.es alojado en su casa matriz. Llevaba -50 por "otra empresa" y quedaba
  // rechazado; con la marca del sitio en el local no es otra empresa, es su propio buzón.
  if (isCrossDomainCorporate && _marcaEnLocal) {
    score += 60;   // cancela el -50 y suma 10
    isCrossDomainCorporate = false;
    if (!matchedRole) matchedRole = "BUZON_DEL_SITIO_EN_GRUPO";
  }
  if (isFreeWebmail && cleanSite && dom !== cleanSite && (_esPersonaORol || _webmailDelSitio)) {
    if (_webmailDelSitio && !_esPersonaORol) {
      // Sin rol reconocible (`diario-ejemplo.contacto@gmail.com`, `kultivi01@gmail.com`): es el
      // info@ del sitio con otro hosting. Vale lo mismo que info@ en el dominio propio (55):
      // -15 del cross + 70 = 55.
      score += 70;
      matchedRole = "WEBMAIL_DEL_SITIO";
    } else {
      score += 25;   // cancela el -15 inicial y suma 10
    }
  }
  if (isCrossDomainCorporate && matchedRole === "EXEC") {
    score += 25; // revierte la mitad del -50
  }

  // ── CATEGORY-ROLE MATCH (peso 0-25) ──
  // Si el sitio es "sports" y el email es marketing/comercial → bonus extra
  // (un MB humano sabe que sports + comercial es golden)
  const cat = (leadCategory || "").toLowerCase();
  if (CATEGORY_TARGET_ROLES[cat] && CATEGORY_TARGET_ROLES[cat].test(local)) {
    score += 25;
  }

  // ── PENALTIES ──
  // Maxi 2026-07-01: roles LEGALES/compliance (datenschutz@/legal@/privacy@/dpo@/gdpr@/
  // dmca@/copyright@/abuse@) — obligatorios por ley (sobre todo en /impressum de sitios DE),
  // NO son comprador y rinden poquísimo. Los dejamos ABAJO de un info@/contact@ normal.
  if (/^(datenschutz|legal|privacy|privacidad|gdpr|dpo|dsb|dmca|copyright|compliance|abuse|recht)/.test(local)) score -= 30;
  // Maxi 2026-07-13 (auditoría): departamentos que NO compran pauta (seguridad/casting/quejas/
  // reclamos/RRHH/soporte). No se descartan del todo (por si es el único contacto), pero van bien abajo.
  if (/^(seguridad|seguranca|security|sicherheit|casting|complaints?|ouvidoria|ombudsman|reclam|quejas|reclamacoes|helpdesk|helpline|support.?tech|soporte.?tecnico|suporte.?tecnico)/.test(local)) score -= 45;
  // Roles TÉCNICOS/IT/operaciones/red — rara vez compran pauta, pero en un medio chico pueden ser el
  // ÚNICO contacto (feedback user 2026-07-13: "sistemas.diariodovale@ no lo veo mal") → penalty fuerte,
  // NO hard-reject: pierden contra cualquier otro candidato pero sobreviven como último recurso.
  if (/^(sys|sysadmin|systems?|sistemas?|edv|betrieb|technik|teknik|informatica|infra|infraestructura|infrastructure|netmanage|net-manage)([._-]|$)/.test(local)
      || /^it[._-](einkauf|support|admin|team|abteilung|dept|helpdesk|service)/.test(local)) score -= 55;
  // Placeholders de CMS (user01, user02, usuario3, guest) — no son personas.
  if (/^(user|usuario|guest|nobody)\d*$/.test(local) || /^admin\d+$/.test(local)) score -= 60;
  // `admin@` a secas: de última. En un medio chico es el dueño (ver la nota en la lista de
  // basura), pero pierde contra info@ (55) y contra cualquier persona. (Maxi 2026-09-04)
  if (/^admin$/.test(local)) score -= 20;
  // Maxi 2026-06-17 (audit #10): penalty solo si dígitos están AL INICIO
  // (ej. "1234email@", "0001foo@") o si parecen un hash sin vocales (ya
  // descartado arriba con /^[a-z0-9]{8,}$/). NO penalizar mid-string
  // (ej. "sales2024@", "team2@") que son patrones legítimos.
  if (/^\d{3,}/.test(local)) score -= 40;
  // Local muy corto (<3) o muy largo (>30) = sospechoso
  if (local.length < 3) score -= 30;
  if (local.length > 30) score -= 25;
  // Soft penalty para palabras con flavor spam (real estate, sales, etc).
  // No descarta — baja prioridad para que ganen otros candidatos si los hay.
  // ⚠️ "sale" está dentro de "sales" (Maxi 2026-09-04): el castigo anti-spam le pegaba a
  // `sales@`, `adsales@`, `de.adsales@` —el buzón que más buscamos— y por eso valían 120
  // mientras `ventas@` y `comercial@` valían 135. Si ya es AD_SALES, no es spam.
  if (matchedRole !== "AD_SALES" && /property|sale|offer|click|freemium|promo|bonus/.test(local)) score -= 15;
  // Free webmail = penalizar pero NO descartar (un MB humano puede mandar)
  if (_rd.penalidad) score += _rd.penalidad;   // 1 rebote previo en el dominio → -40
  // El castigo al webmail se queda SOLO para lo que no es ni persona ni rol ni el buzón del
  // sitio (un `xyz123@gmail.com` sin forma). A los demás ya se los trató arriba con datos.
  if (isFreeWebmail && !_esPersonaORol && !_webmailDelSitio) score -= 20;

  // ── LANGUAGE MATCH bonus ──
  // Si el sitio es .br y el email tiene palabras pt (vendas, comercial) → +5
  // Si .ar/.es/.mx y palabras es (ventas, comercial) → +5
  // Pequeño extra que ayuda a desambiguar entre candidatos similares
  const tld = cleanSite.split(".").pop();
  if ((tld === "br" || tld === "pt") && /(vendas|comercial|publicidade|atendimento)/.test(local)) score += 5;
  if (/^(ar|es|mx|cl|co|pe|uy)$/.test(tld) && /(ventas|comercial|publicidad|atencion)/.test(local)) score += 5;

  return score;
}

export const GARBAGE_DOMAIN_PATTERN = new RegExp([
  // Keywords del dominio — anclados a inicio o tras @/. para evitar matches
  // dentro de palabras (lawscope.com NO debe matchear "aws", paws.com tampoco).
  "(^|[.@])(?:gdpr|aws|amazonaws|amazonses|cloudfront|cloudflare|fastly|akamai|whois)(?=[.-])",
  // GDPR/protect domains (más laxo — solo si es claramente un proxy)
  "(^|[.@])(?:protect|protected|gdpr-?protect|protect-?service)\\.",
  // Subdominios o dominios raíz de admin/whois/abuse/support (también después de @)
  "(^|[.@])(?:nic|abuse|donuts|godaddy|cert|registry|registrar|hosting|host|hostingpanel|trustandsafety)\\.",
  // Cloud providers - support/abuse desks (NO son publishers)
  "(^|[.@])(?:aws|amazonaws|cloudfront|googlecloud|azure|microsoft|cloudflare|fastly)\\.com",
  // Privacy/proxy services
  "domainsbyproxy\\.com|whoisguard|whoisprivacy|whoisprotect|domainprotect|privacyprotect|contactprivacy|perfectprivacy|namebrightprivacy|withheldforprivacy|protect-?service|protectedmail|protecteddomainservices|panelregister|identity-?protect",
  // Registrars (B2B abuse desks)
  "dropped\\.|internetx\\.com|markmonitor|cscglobal|csc-corp|comlaude|safenames|gandi\\.net|key-systems|1api\\.net|netim\\.com|psi-usa|nameshield|epag\\.de|eurodns|realtimeregister|tld-box|enom\\.|networksolutions|tucows|porkbun\\.com|namecheap.*proxy|hostgator|bluehost|godaddyguard|hostafrica|dominio.*\\.com\\.|dominios?[a-z]+\\.ec|dominios?[a-z]+\\.com",
  // Heurística genérica: cualquier dominio con palabra "registrar/registry/dnshosting/
  // domainsby/domainservices/namehost" — caso real latinregistrar.com.br 2026-05-13
  "(^|[.@])[a-z0-9-]*(?:registrar|registry|dnshosting|domainsby|domainservices|namehost|domainname)[a-z0-9-]*\\.",
  // GDPR masking
  "gdpr-?masked?|gdpr-?mask\\.com|gdpr-?protect|data-?protected|registrant-?private|domains[-._]?by[-._]?proxy|registry-?proxy",
  // Disposable/temp emails
  "mailinator|guerrillamail|tempmail|throwaway|trashmail|sharklasers|yopmail|10minutemail|disposable|fakeinbox|mailtrap",
  // Transactional senders (no humans)
  "mailgun\\.org|sendgrid\\.net|amazonses\\.com|postmarkapp\\.com|mandrillapp\\.com|sparkpostmail",
  // Error trackers / sysmail
  "sentry\\.io|bugsnag\\.com|errorception|raygun\\.io|rollbar\\.com",
  // Mailing list managers
  "list-server|listserv\\.|mailman\\.|maillists?\\.",
  // Test / fake / local
  "example\\.(?:com|org|net)|test\\.(?:com|org|net)|localhost|invalid|local",
].join("|"), "i");

export const GENERIC_LOCAL = /^(info|contact|hello|hi|sales|support|ventas|comercial|prensa|press|editor|editorial|redaccion|redacción|mail|email)@/i;

export function riesgoRebotePorDominio(dominioEmail) {
  const n = _rebotesPorDominio.get(String(dominioEmail || "").toLowerCase())?.size || 0;
  if (n >= 2) return { bloquear: true, motivo: `${n} rebotes previos en ese dominio` };
  if (n === 1) return { bloquear: false, penalidad: -40 };
  return { bloquear: false, penalidad: 0 };
}

// ── EL LINTER DE ENTREGABILIDAD ──────────────────────────────────────────────
export const _MAY_PERMITIDAS = new Set(["ADEQ","CPM","CTR","GEO","IAB","URL","SEO","B2B","IVA","CRM","SSP","DSP","RTB","PDF","ADEQMEDIA"]);

/**
 * @param body    el texto COMPLETO que sale (plantilla + firma + pie) → reglas estructurales
 * @param cuerpo  SOLO la plantilla, lo que escribimos nosotros → reglas de estilo
 *
 * ⚠️ La distinción no es cosmética (Maxi 2026-08-24). Cuando esto linteaba el texto
 * completo, la FIRMA del media buyer entraba a las reglas de estilo — y una firma de
 * empresa dice "ADEQ MEDIA" y "ARGENTINA" en mayúsculas, legítimamente. La regla de
 * "no grites" las leyó como gritos y bloqueó 83 envíos reales en dos días. El estilo
 * se juzga sobre lo que redactamos nosotros; la firma es del dueño del buzón.
 */
export function revisarEntregabilidad({ to, subject, body, cuerpo, html, mime, esProspeccion = true }) {
  const bloqueantes = [], avisos = [];
  const s = String(subject || ""), b = String(body || "");
  // Si no llega la plantilla por separado, NO se juzga el estilo. Bloquear un envío
  // legítimo es peor que dejar pasar un "!" de más: el costo de un falso positivo acá
  // ya se pagó (83 envíos perdidos en dos días por la firma del MB).
  const plantilla = cuerpo != null ? String(cuerpo) : null;
  // El correo INTERNO (alertas de salud, avisos de seguridad a mgargiulo@) no es
  // prospección: no va a un desconocido, no compite por reputación y suele traer
  // SQL, mayúsculas y textos largos. Si le aplicáramos las reglas de estilo, el
  // vigilante se bloquearía sus propias alertas — el peor fallo silencioso posible.
  const _estricto = esProspeccion !== false;

  // Placeholders sin resolver: el bug de {{saludo}} del 11/08 le llegó a un
  // prospecto real. Con esto no habría salido.
  // Estas dos solo en prospección: el correo interno cita SQL, que legítimamente
  // contiene NULL, y puede mostrar un placeholder como ejemplo de lo que se detectó.
  if (_estricto && /\{\{|\}\}|\[\[|%%|__[A-Z]+__/.test(s + " " + b)) bloqueantes.push("placeholder_sin_resolver");
  if (_estricto && /\b(null|undefined|NaN)\b/.test(s + " " + b)) bloqueantes.push("valor_vacio_en_el_texto");

  if (!s.trim()) bloqueantes.push("asunto_vacio");
  if (_estricto && s.length > 78) bloqueantes.push("asunto_muy_largo");
  // El emoji solo importa en un cold email a un desconocido. En el correo INTERNO es
  // al revés: el parte usa 🔴/⚠️/✅ para que se lea de un vistazo. El 21/08 esta regla
  // bloqueó el propio parte diario del sistema — el detector censurándose a sí mismo.
  if (_estricto && /[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u.test(s)) bloqueantes.push("emoji_en_el_asunto");
  if (/[\r\n]/.test(s) || /[\r\n]/.test(String(to || ""))) bloqueantes.push("salto_de_linea_en_cabecera");

  if (_estricto && plantilla != null) {
    // Sobre la PLANTILLA, no sobre el mail entero: la firma no se juzga.
    const palabras = plantilla.trim().split(/\s+/).filter(Boolean).length;
    if (palabras < 15) bloqueantes.push(`cuerpo_muy_corto_${palabras}`);
    if (palabras > 400) bloqueantes.push(`cuerpo_muy_largo_${palabras}`);
    if ((plantilla.match(/!/g) || []).length > 2) bloqueantes.push("exceso_de_exclamaciones");
    // Y dentro de la plantilla, se exigen DOS gritos para bloquear. Una sola palabra en
    // mayúsculas suele ser un nombre propio o una sigla; dos o más ya es tono de spam.
    // ⚠️ ESTA REGLA DEJA DE BLOQUEAR (Maxi 2026-08-25). Lleva DOS incidentes y 147 envíos
    // reales frenados —79 el 18-19/08 y 68+ hoy— y no atajó jamás un mail spammy. Las dos
    // veces fue lo mismo: leyó "ADEQ MEDIA" y "BUENOS AIRES, ARGENTINA" de la firma. La
    // segunda vez ni siquiera pude reproducir CÓMO le llega la firma al cuerpo, y mientras
    // tanto se perdían envíos: un guard que solo produjo falsos positivos no puede seguir
    // frenando el negocio mientras se investiga.
    // Pasa a AVISO, con el contexto alrededor para poder rastrear el origen la próxima vez.
    const grito = (plantilla.match(/\b[A-ZÁÉÍÓÚÑ]{4,}\b/g) || []).filter(w => !_MAY_PERMITIDAS.has(w));
    if (grito.length >= 2) {
      const _i = plantilla.indexOf(grito[0]);
      const _ctx = plantilla.slice(Math.max(0, _i - 45), _i + 60).replace(/\s+/g, " ");
      avisos.push(`mayusculas:${grito.slice(0, 3).join(",")} · contexto: "…${_ctx}…"`);
    }
  }

  // Caracteres que marcan spam o sirven para suplantar
  if (/[​-‍﻿]/.test(s + b)) bloqueantes.push("caracteres_invisibles");
  if (/[‪-‮⁦-⁩]/.test(s + b)) bloqueantes.push("override_bidireccional");
  if (/[a-zA-Z]/.test(b) && /[Ѐ-ӿͰ-Ͽ]/.test(b)) bloqueantes.push("homoglifos_mezclados");
  // Entidades numéricas: atrapa si alguien reintroduce el anti-linkify de los puntos.
  if (/&#\d+;/.test(b) || /&#\d+;/.test(String(html || ""))) bloqueantes.push("entidades_html_ofuscadas");

  // ⚠️ SE JUZGABAN LOS LINKS DE LA FIRMA (Maxi 2026-08-25). `html` es el mail COMPLETO,
  // firma incluida, y la firma de Gmail trae logo, web y redes. Es la misma trampa que ya
  // frenó 79 envíos por "mayúsculas" al leer "ADEQ MEDIA" de la firma. Las reglas de
  // CONTENIDO se juzgan sobre la plantilla; las estructurales (caracteres invisibles,
  // homóglifos, largo de línea) sí van sobre el mail entero, que es lo que ve el filtro.
  const urls = (String(plantilla != null ? plantilla : (html || b)).match(/https?:\/\/[^\s"'<>)]+/gi) || [])
    .filter(u => !/track-open|unsubscribe|\/unsub/i.test(u));
  if (_estricto && urls.length > 3) bloqueantes.push(`demasiados_links_${urls.length}`);

  if (mime) {
    // TextEncoder y no Buffer: este módulo también lo importa la EXTENSIÓN (Fase 5, 2026-09-04)
    // y en el navegador Buffer no existe. Probado equivalente byte a byte con ñ, kanji y emoji.
    if (mime.split("\r\n").some(l => new TextEncoder().encode(l).length > 990)) bloqueantes.push("linea_mime_mayor_a_990");
    if (_estricto && /Content-Disposition:\s*attachment/i.test(mime)) bloqueantes.push("adjunto_en_prospeccion");
    if ((mime.match(/=\?UTF-8\?B\?[^?]+\?=/g) || []).some(w => w.length > 75)) bloqueantes.push("encoded_word_mayor_a_75");
  }

  if (/^(info|contacto|contact|hello|hola)@/i.test(String(to || ""))) avisos.push("destinatario_generico");
  return { ok: bloqueantes.length === 0, bloqueantes, avisos };
}

// Maxi 2026-07-08: PRIORIDAD DURA de fuente. Un contacto NOMINAL (apollo/informer =
// decision-maker real, o manual = elección explícita del MB) SIEMPRE se elige antes que un
// GENÉRICO (info@/contact@), sin importar lo que diga el ranking dinámico aprendido. El 0%
// de respuesta observado era por IDIOMA equivocado (Fix #1), NO por la fuente. Regla del
// dueño: si informer/apollo encontró contacto, usarlo sí o sí por encima de genéricos.
// El ranking dinámico (SOURCE_RANK) sigue operando como DESEMPATE dentro del mismo tier —
// antes podía relegar un informer detrás de un info@ con mejor open-rate observado.
//   tier 2 = nominal decision-maker (apollo/informer/manual)
//   tier 1 = otras fuentes con persona (scrape, redes sociales)
//   tier 0 = genérico (info@/contact@) y desconocido
export function _sourceHardTier(source) {
  const s = (source || "").toLowerCase();
  if (s === "apollo" || s === "informer" || s === "manual") return 2;
  if (s === "generic" || s === "") return 0;
  return 1;
}

// Maxi 2026-07-09: tier de SELECCIÓN del email final. Combina la confiabilidad de la SOURCE con
// el ROL comercial del local-part. El user (Q4) eligió "rol comercial/publicidad" como la mejor
// opción, PERO el decision-maker verificado por Apollo sigue mandando (regla del dueño):
//   4 = apollo/manual nominal (decision-maker verificado)
//   3 = publicidad@/comercial@/ventas@ (rol de venta de pauta — buzón ideal ADEQ)
//   2 = otra persona/rol scrapeado (source=scrape)
//   1 = informer NO-comercial (WHOIS/registrar — baja calidad, ver auditoría 2026-07-13)
//   0 = genérico (info@/contacto@)
// Así el orden queda: apollo > publicidad@ > persona scrapeada > informer > genérico.
// ── EL ORDEN DE LOS TIERS LO DECIDEN LAS RESPUESTAS (Maxi 2026-08-28, pedido del user) ──
// "Dale automáticamente más prioridad a los que más responden, 4 reajustes por mes, uno por
//  semana." Hasta hoy el orden era fijo por decisión: apollo > rol comercial > persona >
// genérico. La medición real le da la razón a Apollo (9,3%) pero el resto baila — y un orden
// clavado no se entera si mañana los roles comerciales empiezan a contestar el doble.
//
// Una vez por semana se mide la respuesta REAL (sin out-of-office) por tipo sobre 90 días
// —con 30 la muestra de Apollo es demasiado chica para decidir— y se reordena. Salvaguardas:
//   · suavizado (ok+1)/(n+10): un tipo con 2 de 3 no le gana a uno con 20 de 200;
//   · mínimo 15 envíos por tipo — sin muestra se queda el orden vigente para ese tipo;
//   · el resultado se guarda en config y se LOGUEA: cada reajuste queda visible en el resumen.
export const _TIER_ORDEN_DEFAULT = ["apollo", "rol", "persona", "generico"];

export function _tipoDeEmailParaRanking(email, source) {
  const src = String(source || "").toLowerCase();
  const local = String(email || "").toLowerCase().split("@")[0];
  // El que eligió una persona a mano sigue arriba de todo.
  if (src === "manual") return "apollo";
  // ── APOLLO COMPITE POR RESULTADO (decisión 2 del user, 04/09) ───────────────────────
  // Medido en 90 días: 169 envíos a emails de Apollo → 3 respuestas reales (1,8%), contra
  // 6,6% de los que el MB agregó a mano y 3,1% del scrape. Ser de Apollo ya no vale por sí
  // solo: una persona de Apollo es una persona, un rol comercial es un rol, y un info@ que
  // Apollo devolvió es un genérico — y se ordena con los de su clase.
  if (src === "apollo") {
    if (AD_SALES_LOCAL.test(local)) return "rol";
    return _isGenericLocalPart(email) ? "generico" : "persona";
  }
  if (AD_SALES_LOCAL.test(local)) return "rol";
  if (_sourceHardTier(source) === 1) return "persona";
  return "generico";
}
