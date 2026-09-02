// ═══════════════════════════════════════════════════════════════════════════════════════
// DOMINIO — normalizar una URL a algo comparable
// ═══════════════════════════════════════════════════════════════════════════════════════
// Extraído de index.js el 2026-09-02 sin cambiarle la lógica.
//
// Parece trivial y no lo es: si dos partes del sistema normalizan distinto, el mismo sitio
// entra dos veces al pool, o un dominio bloqueado no matchea y se le escribe igual. Ya pasó
// —dos normalizadores en conflicto hacían que las fichas no abrieran—, por eso vive en un
// solo lugar.
//
// ⚠️ `coreDomain` tiene que entender los TLD de dos niveles (.com.ar, .co.uk): sin eso,
// "diario.com.ar" y "otro.com.ar" se leen como el mismo sitio.

// Dominios de tech/redes sociales/marcas globales — no son publishers
export const EXCLUDE_DOMAINS = new Set([
  // Search & tech
  "google.com","google.co.uk","google.com.br","google.es","google.de","google.com.mx",
  "google.co.jp","google.fr","google.it","google.com.ar",
  "youtube.com","gmail.com","googletagmanager.com","googleapis.com",
  "bing.com","duckduckgo.com","baidu.com","yandex.ru","yandex.com","naver.com","yahoo.com",
  "msn.com","bing.com","ask.com","aol.com",
  // Social
  "facebook.com","instagram.com","twitter.com","x.com","threads.net",
  "tiktok.com","snapchat.com","pinterest.com","linkedin.com","whatsapp.com",
  "reddit.com","tumblr.com","quora.com","vk.com","ok.ru",
  "discord.com","telegram.org","signal.org","wechat.com","line.me",
  // Video/streaming
  "netflix.com","spotify.com","twitch.tv","vimeo.com","dailymotion.com",
  "hulu.com","disneyplus.com","primevideo.com","peacocktv.com","hbomax.com",
  // E-commerce/retail
  "amazon.com","amazon.co.uk","amazon.de","amazon.es","amazon.com.br","amazon.fr",
  "ebay.com","ebay.co.uk","ebay.de","aliexpress.com","alibaba.com","taobao.com",
  "mercadolibre.com","shopify.com","etsy.com","wish.com","rakuten.com",
  "walmart.com","target.com","costco.com","bestbuy.com","homedepot.com",
  // Finance
  "paypal.com","stripe.com","payoneer.com","wise.com","revolut.com",
  "chase.com","bankofamerica.com","wellsfargo.com","citibank.com","hsbc.com",
  "visa.com","mastercard.com","americanexpress.com",
  // Tech/software
  "apple.com","microsoft.com","windows.com","office.com","live.com","outlook.com",
  "zoom.us","slack.com","dropbox.com","github.com","gitlab.com","stackoverflow.com",
  "cloudflare.com","amazonaws.com","azure.microsoft.com","cloud.google.com",
  "oracle.com","sap.com","salesforce.com","hubspot.com","zendesk.com",
  "adobe.com","canva.com","figma.com","notion.so","atlassian.com","jira.com",
  // CMS/blogging (plataformas, no publishers)
  "wp.com","wordpress.com","blogspot.com","wix.com","squarespace.com",
  "weebly.com","medium.com","substack.com","ghost.io","blogger.com",
  "webflow.com","jimdo.com","strikingly.com",
  // Knowledge/encyclopedia
  "wikipedia.org","wikimedia.org","wikihow.com","wikidata.org",
  // Travel booking (no content puro)
  "booking.com","airbnb.com","expedia.com","tripadvisor.com","hotels.com",
  "kayak.com","skyscanner.net","agoda.com","hostelworld.com",
  // Food delivery
  "ubereats.com","doordash.com","grubhub.com","deliveroo.com","justeat.com","rappi.com",
  // Ride/transport
  "uber.com","lyft.com","bolt.eu","cabify.com","grab.com",
  // Misc global brands
  "ikea.com","zara.com","hm.com","uniqlo.com","nike.com","adidas.com",
  "mcdonalds.com","starbucks.com","cocacola.com","pepsi.com",
  "samsung.com","lg.com","sony.com","panasonic.com","philips.com",
  "toyota.com","honda.com","bmw.com","mercedes-benz.com","volkswagen.com",
]);

// Detecta dominios de universidades e institutos académicos
export function isUniversityDomain(domain) {
  if (domain.endsWith(".edu")) return true;
  if (domain.endsWith(".ac.uk"))  return true;
  if (domain.endsWith(".edu.au")) return true;
  if (domain.endsWith(".edu.br")) return true;
  if (domain.endsWith(".edu.mx")) return true;
  if (domain.endsWith(".ac.jp"))  return true;
  if (domain.endsWith(".edu.ar")) return true;
  if (domain.endsWith(".edu.co")) return true;
  if (domain.endsWith(".ac.nz"))  return true;
  if (domain.endsWith(".ac.za"))  return true;
  if (domain.endsWith(".sch.uk")) return true;
  if (domain.endsWith(".edu.es")) return true;
  if (domain.endsWith(".edu.it")) return true;
  if (domain.endsWith(".unimi.it")) return true;
  const kw = /\b(university|universidad|universidade|universit[aey]|uni[-.]|college|instituto[-.]tecnol|polytechnic|akademi|hochschule|facultad|school|escuela|colegio)\b/i;
  return kw.test(domain);
}

// Patrones de palabra clave para descartar verticales que NO son publishers.
// Aplica al nombre del dominio antes del primer punto. Más liviano que mantener
// listas exhaustivas — si un dominio matchea alguno de estos, no es prospect ADEQ.
export const EXCLUDE_KEYWORDS = [
  // Adulto / porno
  "porn","xxx","sex","adult","cam","escort","fetish","hentai","onlyfans","pornhub","xvideos","xnxx","redtube",
  "youporn","brazzers","chaturbate","stripchat","camsoda","myfreecams","livejasmin","bongacams",
  // Gobierno / instituciones
  "gov","gob","government","municipalidad","ayuntamiento","ministerio","ministry","parliament","congreso",
  // Bancos grandes / fintech enterprise
  "bank","banco","banca","banking","santander","bbva","caixabank","sabadell","unicredit","intesasanpaolo",
  // Seguros enterprise
  "insurance","seguros","mapfre","allianz","axa","zurich","prudential",
  // Pharma enterprise
  "pharma","pfizer","novartis","roche","merck","sanofi","gsk","astrazeneca",
  // Telco enterprise
  "telecom","telefonica","movistar","orange","vodafone","telmex","claro","mts",
  // Energía enterprise
  "petroleum","petrobras","exxon","chevron","totalenergies","shell","bp-",
  // Aerolineas enterprise
  "airlines","aerolineas","iberia","lufthansa","ryanair","emirates","qatarairways","americanairlines",
  // Search / ad networks que se cuelan
  "doubleclick","adservice","adsystem","adnxs","criteo","outbrain","taboola",
  // Hosting / CDN
  "cloudflare","cloudfront","fastly","akamai","jsdelivr","unpkg","cdn",
  // Marketplace gigantes locales
  "olx","craigslist","mercadolivre","gumtree","letgo","wallapop","jiji",
  // Apuestas/gambling enterprise (legales pero gigantes)
  "bet365","williamhill","ladbrokes","betfair","draftkings","fanduel","pokerstars",
];

export const EXCLUDE_KEYWORD_RE = new RegExp(`\\b(${EXCLUDE_KEYWORDS.join("|")})\\b`, "i");

export function matchesExcludeKeyword(domain) {
  // Solo chequea el nombre antes del primer punto + sufijos relevantes
  return EXCLUDE_KEYWORD_RE.test(domain);
}

export function isDomainAllowed(domain) {
  // ⚠️ ESTO TIRABA EL PROCESO (Maxi 2026-09-02). `TypeError: domain.includes is not a
  // function`, 14 veces sin capturar. Le llegaban OBJETOS `{title, domain}` desde el cache
  // de similares: alguna rama guardó la respuesta cruda de la API en vez del dominio, y 117
  // filas del cache quedaron así. Como el error salía por unhandledRejection, no lo agarraba
  // ningún try/catch y se llevaba puesto el ciclo entero.
  // Se acepta lo que venga y se extrae el dominio si es un objeto: una función de filtro
  // NUNCA puede voltear el proceso por un dato mal formado.
  if (domain && typeof domain === "object") domain = domain.domain || domain.url || domain.name || "";
  domain = String(domain || "");
  if (!domain || !domain.includes(".")) return false;
  if (EXCLUDE_DOMAINS.has(domain)) return false;
  if (isUniversityDomain(domain)) return false;
  if (matchesExcludeKeyword(domain)) return false;
  return true;
}

// ── ¿ES UN DOMINIO DE VERDAD? (Maxi 2026-08-18) ──────────────────────────────────────
// Lo que costó no tener esto: `cachalot.k` entró a la cola el 6 de julio. El ".k" no existe
// como terminación, así que SimilarWeb devolvía HTTP 400 en cada intento. Como el 400 estaba
// mal clasificado como error transitorio, se reintentaba sin fin y la cola nunca avanzaba:
// 1.711 dominios detrás sin procesar y los tres motores de descubrimiento frenados por
// saturación. Seis semanas de parálisis por un dominio mal escrito.
// El 400 ya está arreglado, pero la lección de fondo es otra: basura que no debió entrar
// nunca. Éste es el filtro más barato del sistema — sin red, sin API, sin costo.
// Primero LIMPIA (protocolo, www, ruta, puerto) y después juzga. El orden importa: tirar un
// lead bueno por venir escrito como "https://ejemplo.com/noticias" sería cambiar un problema
// por otro. Devuelve el dominio limpio, o "" si no hay dominio posible ahí adentro.
export function _dominioLimpio(d) {
  let s = String(d || "").trim().toLowerCase();
  if (!s) return "";
  s = s.replace(/^[a-z]+:\/\//, "");     // https://
  s = s.split(/[/?#]/)[0];               // ruta, query, ancla
  s = s.split("@").pop();                // user@host
  s = s.split(":")[0];                   // :8080
  s = s.replace(/^www\./, "").replace(/\.$/, "");
  // Dominios con acentos o ñ (olé.com.ar, españa.es): existen y en un mercado hispano son
  // leads reales. Se pasan a punycode, que es como los resuelve internet de verdad, en vez
  // de descartarlos por "caracteres raros".
  if (/[^\x00-\x7F]/.test(s)) {
    try { s = new URL(`http://${s}`).hostname.replace(/^www\./, ""); } catch { return ""; }
  }
  return s;
}

export function _dominioValido(d) {
  const s = _dominioLimpio(d);
  if (!s || s.length > 253 || s.length < 4) return false;
  if (/[\s_\\]/.test(s)) return false;
  if (s.startsWith(".") || s.includes("..")) return false;
  const partes = s.split(".");
  if (partes.length < 2) return false;                // sin punto no es un dominio
  const tld = partes[partes.length - 1];
  if (!/^[a-z]{2,24}$/.test(tld)) return false;       // ← acá muere "cachalot.k"
  return partes.every(p => /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/.test(p) && p.length <= 63);
}

// Returns the "organization root" of a domain — collapses regional TLD variants
// so clarin.com / clarin.com.ar / clarin.com.br dedupe to the same org key.
// Heuristic — strips known multi-part TLDs first, then last TLD label, leaving
// the brand part. Not perfect, but cuts >80% of obvious cross-region duplicates.
export const MULTI_PART_TLDS = new Set([
  // commercial
  "com.ar","com.br","com.mx","com.co","com.pe","com.uy","com.ec","com.ve","com.bo",
  "com.es","com.au","com.cn","com.tw","com.hk","com.sg","com.my","com.tr","com.eg",
  "com.sa","com.ng","com.za","com.ph","com.vn","com.pk","com.gt","com.do","com.pa",
  "com.gh","com.ke","com.uy","com.bd","com.np","com.lk","com.kh","com.kw","com.qa",
  "com.lb","com.jo","com.om","com.ye","com.ly","com.tn","com.dz",
  "co.uk","co.za","co.in","co.kr","co.jp","co.il","co.nz","co.id","co.cr","co.ve",
  "co.th","co.ke","co.tz","co.ug","co.zw","co.ma","co.ao",
  // organization
  "org.uk","org.ar","org.br","org.mx","org.au","org.za","org.es","org.in","org.pl",
  "org.cn","org.tw","org.kr","org.jp","org.tr","org.sg","org.my","org.ph","org.vn",
  "org.pk","org.bd","org.np","org.lk","org.eg","org.sa","org.ae","org.il",
  // government — todos los .gov.X conocidos
  "gov.ar","gov.br","gov.mx","gov.co","gov.pe","gov.cl","gov.uy","gov.ec","gov.ve",
  "gov.bo","gov.au","gov.in","gov.uk","gov.za","gov.eg","gov.sa","gov.ng",
  "gov.cn","gov.tw","gov.kr","gov.jp","gov.sg","gov.my","gov.ph","gov.vn","gov.pk",
  "gov.bd","gov.np","gov.lk","gov.tr","gov.ae","gov.il","gov.tn","gov.ma",
  // academic
  "ac.uk","ac.in","ac.za","ac.jp","ac.kr","ac.nz","ac.th","ac.id","ac.cn","ac.tw",
  "ac.ir","ac.ae","ac.il","ac.bd","ac.lk","ac.np","ac.tz","ac.ke",
  "edu.ar","edu.br","edu.mx","edu.co","edu.pe","edu.uy","edu.au","edu.in","edu.eg",
  "edu.cn","edu.tw","edu.hk","edu.sg","edu.my","edu.ph","edu.vn","edu.pk","edu.bd",
  "edu.np","edu.lk","edu.tr","edu.sa","edu.ae","edu.jo","edu.lb",
  // network/info per country
  "net.ar","net.br","net.mx","net.au","net.in","net.cn","net.tw","net.kr","net.jp",
  "net.sg","net.my","net.ph","net.vn","net.pk","net.bd","net.np","net.lk","net.eg",
  "net.sa","net.ae","net.tr",
  // .jp specific second-level (not just co.jp)
  "or.jp","ne.jp","ad.jp","ed.jp","gr.jp","lg.jp","go.jp",
  // .kr specific
  "or.kr","ne.kr","go.kr","re.kr","pe.kr","es.kr","sc.kr","hs.kr","ms.kr",
  // .cn specific
  "ah.cn","bj.cn","cq.cn","fj.cn","gd.cn","gs.cn","gz.cn","gx.cn","ha.cn","hb.cn",
  "he.cn","hi.cn","hk.cn","hl.cn","hn.cn","jl.cn","js.cn","jx.cn","ln.cn","mo.cn",
  "nm.cn","nx.cn","qh.cn","sc.cn","sd.cn","sh.cn","sn.cn","sx.cn","tj.cn","tw.cn",
  "xj.cn","xz.cn","yn.cn","zj.cn",
]);

export function coreDomain(domain) {
  if (!domain) return "";
  const parts = domain.toLowerCase().replace(/^www\./, "").split(".");
  if (parts.length <= 2) return parts[0]; // foo.com → "foo"
  // Last 2 labels — check if it's a multi-part TLD (e.g. "com.ar", "gov.co")
  const last2 = parts.slice(-2).join(".");
  if (MULTI_PART_TLDS.has(last2) && parts.length >= 3) {
    return parts[parts.length - 3]; // x.gov.co → "x", brand.com.ar → "brand"
  }
  return parts[parts.length - 2]; // sub.foo.com → "foo"
}
