// La extensión, revisión final del 13/09 antes del deploy (cluster extension_final).
//
//   A1  La tarjeta de Prospects mandaba al email principal sin mirar la lista de rebotados: sólo preguntaba por
//       los adicionales. Analysis y el lote sí frenan. Un rebotado nunca se reusa, y "no pude preguntar" nunca
//       es "no rebotó".
//   A2  La caché de sesión de Analysis guardaba los emails sin su fuente. Al volver a abrir el sitio todo era
//       "Cache", el gmail del registrante que trajo website.informer subía a persona y quedaba preseleccionado,
//       cuando la primera apertura y la tarjeta del mismo lead lo descartan.
//   A3  El mail desde Analysis leía los slots de adicionales, el idioma y la fuente del principal después de las
//       esperas (firma, envío, rebote de cada adicional). Si el MB cambiaba de pestaña mientras salía, el adicional
//       de A se encolaba a nombre de B (o no se programaba) y la ficha de B se llevaba el contacto de A.
//   A4  Código muerto en la tarjeta: una lista de radios (emailOptions, _idxPreseleccion, _fuenteDeEmail) que no se
//       dibujaba, y getSelectedEmail buscaba esos radios. Los arreglos del 13/09 habían ido a parar a esa copia.
//
// Cada test corre el código REAL de popup/popup.js (extraído con acorn) con dobles de lo que toca afuera.
//
// Run: npm test
/* eslint-disable no-new-func */
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as acorn from "acorn";
import * as walk from "acorn-walk";
import * as E from "../lib/email.js";
import { isGarbageEmail } from "../../modules/emailVerifier.js";
import {
  adicionalesDeLaTarjeta, contactosDeAdicionales, filaColaDesdeFormulario, fotoCrmAlGuardar, anotarEnvioDeSesion,
} from "../../modules/colaEstado.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(aqui, "..", "..");
const popup = fs.readFileSync(path.join(RAIZ, "popup", "popup.js"), "utf8");
const ast = acorn.parse(popup, { ecmaVersion: "latest", sourceType: "module" });
const texto = (n) => popup.slice(n.start, n.end);
const linea = (n) => popup.slice(0, n.start).split("\n").length;

function funcion(nombre) {
  let hallado = null;
  walk.full(ast, (n) => { if (!hallado && n.type === "FunctionDeclaration" && n.id?.name === nombre) hallado = n; });
  ok(hallado, `no encontré la función ${nombre} en popup.js`);
  return hallado;
}
function llamadas(arbol, nombre) {
  const out = [];
  walk.full(arbol, (n) => { if (n.type === "CallExpression" && n.callee.type === "Identifier" && n.callee.name === nombre) out.push(n); });
  return out;
}
const ejecutar = (deps, fuente) => {
  const nombres = Object.keys(deps);
  return new Function(...nombres, fuente)(...nombres.map(n => deps[n]));
};

// El _veredictoCrm REAL del popup (mismo corte que tests/veredicto-crm.test.js).
const veredictoCrm = (() => {
  const ini = popup.indexOf("const _CRM_LIVE_RE");
  const fin = popup.indexOf("function _pintarVeredictoCrm");
  ok(ini > 0 && fin > ini, "no encontré _veredictoCrm en popup.js");
  return new Function(popup.slice(ini, fin) + "; return _veredictoCrm;")();
})();

// ═══ A1 — la tarjeta no le manda al principal rebotado ═══════════════════════════════════════════
const PRINCIPAL = "publicidad@diario.com.mx";
const ADICIONAL = "ventas@diario.com.mx";

// validateProspect, extraída con acorn y ejecutada con dobles (mismo método que extension_envio-13-09c).
function armarTarjeta({ rebote }) {
  const log = [], pedidos = [], fichas = [], consultas = [];
  const valores = {
    ".pcard-pitch": "Hola, somos ADEQ", ".pcard-subject": "Propuesta", ".pcard-owner": "mb", ".pcard-status": "Propuesta Vigente",
    ".pcard-lang": "1", ".pcard-geo": "Mexico", ".pcard-date": "13/09/2026", ".pcard-traffic": "900K",
    ".pcard-future-1": ADICIONAL,
  };
  const els = new Map();
  const card = {
    style: {},
    querySelector: (sel) => { if (!els.has(sel)) els.set(sel, { value: valores[sel] ?? "", textContent: "", style: {}, focus() {} }); return els.get(sel); },
    botones: [{ classList: { contains: (c) => c === "pcard-validate-expanded" }, disabled: false }, { classList: { contains: () => false }, disabled: false }],
    querySelectorAll(sel) { return sel === "button" ? this.botones : []; },
    remove() {},
  };
  const deps = {
    buscarEnCrm: async () => ({ found: false }), _veredictoCrm: veredictoCrm, getDailyValidationCount: async () => 0,
    state: { accessToken: "tk", loginEmail: "mb@adeqmedia.com" },
    getSelectedEmail: () => PRINCIPAL, defaultOwnerForLang: () => "mb", defaultStatusForOwner: () => "Propuesta Vigente",
    LANG_TO_IDX: { es: "1" }, formatTraffic: (n) => `${Math.round(n / 1000)}K`, isValidEmail: (e) => /@/.test(e),
    ensureFreshToken: async (margen) => { log.push(`token:${margen === Infinity ? "forzado" : "normal"}`); return "tk"; },
    getGmailSignature: async () => "MB · ADEQ", appendClosingIfMissing: (s) => s,
    createManualSendTracking: async () => { log.push("tracking"); return { ok: true, id: 77 }; },
    CONFIG: { SUPABASE_URL: "https://sb", SUPABASE_ANON_KEY: "anon" },
    sendEmail: async ({ to }) => { log.push(`mail:${to}`); return { ok: true }; },
    markManualSendFailed: async () => {}, incrementUserDailyCounter: async () => {},
    saveSendDate: async () => { log.push("sendtrack"); return { ok: true }; },
    markReviewQueueAsContacted: async () => { log.push("contactado"); return { ok: true }; },
    isEmailBounced: async (tk, e, op) => {
      consultas.push({ tk, e, op, botonesApagados: card.botones.every(b => b.disabled) });
      log.push(`rebote:${e}`);
      return rebote(e, op);
    },
    adicionalesDeLaTarjeta, contactosDeAdicionales, filaColaDesdeFormulario, fotoCrmAlGuardar,
    anotarEnvioDeSesion, _enviosDeLaSesion: new Map(),
    fetch: async (url, opts = {}) => {
      const tabla = url.split("/rest/v1/")[1].split("?")[0];
      pedidos.push({ tabla, opts });
      return { ok: true, status: 201, json: async () => [{ id: 42 }] };
    },
    AbortSignal,
    enviarAlBoard: async (p) => { log.push("ficha"); fichas.push(p); return {}; },
    saveHistory: async () => { log.push("historial"); }, validateReviewItem: async () => ({ ok: true }), queuePendingMark: async () => {},
    refreshProspectsStats: () => {}, setTimeout: () => {}, console: { error() {}, warn() {}, log() {} },
    showToast: () => {}, window: {},
  };
  const validateProspect = ejecutar(deps, `${texto(funcion("_tarjetaAPorEnviar"))}\n${texto(funcion("validateProspect"))}\nreturn validateProspect;`);
  const data = { id: 42, domain: "diario.com.mx", traffic: 900000, language: "es", emails: [PRINCIPAL, ADICIONAL], email_sources: {}, contact_phone: "" };
  return {
    correr: () => validateProspect(card, data, true), card, log, pedidos, fichas, consultas,
    resultado: () => card.querySelector(".pcard-result").textContent,
  };
}

// Lo que NO puede haber pasado cuando el principal no sale.
function noSalioNada(t, porque) {
  ok(!t.log.some(l => l.startsWith("mail:")), `${porque}: el mail salió igual → ${t.log.join(" → ")}`);
  ok(!t.log.includes("tracking"), `${porque}: se anotó en agent_actions un envío que no tiene que existir`);
  ok(!t.log.includes("sendtrack") && !t.log.includes("contactado") && !t.log.includes("historial"), `${porque}: se registró algo del envío → ${t.log.join(" → ")}`);
  strictEqual(t.fichas.length, 0, `${porque}: se cargó la ficha del CRM con esa dirección`);
  ok(!t.pedidos.some(p => p.tabla === "toolbar_reengagement_queue"), `${porque}: se programaron los adicionales`);
  ok(t.card.botones.every(b => !b.disabled), `${porque}: los botones quedaron apagados y no se puede elegir otra ni reintentar`);
}

test("A1: la tarjeta no le manda al principal que está en la lista de rebotados, y lo dice", async () => {
  const t = armarTarjeta({ rebote: async (e) => (e === PRINCIPAL ? { bounced: true, reason: "550 user unknown", since: "2026-09-01" } : { bounced: false }) });
  await t.correr();
  ok(t.consultas.some(c => c.e === PRINCIPAL), "la tarjeta nunca le preguntó a la lista de rebotados por el principal");
  noSalioNada(t, "principal rebotado");
  ok(/Cannot send/.test(t.resultado()) && /bounced emails database/.test(t.resultado()) && t.resultado().includes(PRINCIPAL) && /550 user unknown/.test(t.resultado()),
    `el cartel tiene que decir que rebotó y por qué: ${t.resultado()}`);
});

test("A1: si la lista de rebotados no contesta, la tarjeta no manda, dice el motivo y deja reintentar", async () => {
  const t = armarTarjeta({ rebote: async () => ({ bounced: null, indeterminado: true, motivo: "la base no contestó en 8 s" }) });
  await t.correr();
  noSalioNada(t, "lista sin respuesta");
  ok(/couldn't confirm/.test(t.resultado()) && /la base no contestó en 8 s/.test(t.resultado()) && /Try again/.test(t.resultado()),
    `"no pude preguntar" no es "no rebotó": ${t.resultado()}`);

  // Una consulta que tira (en vez de contestar) es lo mismo: no sé.
  const u = armarTarjeta({ rebote: async () => { throw new Error("Failed to fetch"); } });
  await u.correr();
  noSalioNada(u, "consulta que tira");
  ok(/couldn't confirm/.test(u.resultado()) && /Failed to fetch/.test(u.resultado()), u.resultado());
});

test("A1: con el principal limpio sale como siempre; la consulta va antes de todo lo del envío, con los botones apagados y token fresco", async () => {
  const t = armarTarjeta({ rebote: async () => ({ bounced: false }) });
  await t.correr();
  ok(t.log.includes(`mail:${PRINCIPAL}`), t.log.join(" → "));
  strictEqual(t.fichas.length, 1);
  ok(/Cargado en ADEQ \+ mail enviado/.test(t.resultado()), t.resultado());
  const iRebote = t.log.indexOf(`rebote:${PRINCIPAL}`);
  ok(iRebote >= 0 && iRebote < t.log.indexOf("tracking") && iRebote < t.log.indexOf(`mail:${PRINCIPAL}`),
    `el principal se consulta antes del tracking y del mail: ${t.log.join(" → ")}`);
  const consulta = t.consultas.find(c => c.e === PRINCIPAL);
  strictEqual(consulta.botonesApagados, true, "un doble click durante la consulta (hasta 8 s) mandaría dos veces");
  ok(typeof consulta.op?.renovarToken === "function", "sin renovarToken, un token vencido se lee como 'la lista no contesta'");
  await consulta.op.renovarToken();
  ok(t.log.includes("token:forzado"), "renovarToken tiene que forzar la renovación del JWT (ensureFreshToken(Infinity))");
  // El adicional se sigue consultando aparte, y el principal una sola vez.
  deepStrictEqual(t.consultas.map(c => c.e), [PRINCIPAL, ADICIONAL]);
  ok(t.pedidos.some(p => p.tabla === "toolbar_reengagement_queue"), "el adicional limpio se sigue programando");
});

test("A1: las cuatro puertas del mail (Analysis, sus adicionales, el lote y la tarjeta) consultan la lista antes de mandar", () => {
  const vp = funcion("validateProspect");
  const [envio] = llamadas(vp, "sendEmail");
  const consultas = llamadas(vp, "isEmailBounced");
  ok(envio && consultas.length >= 2, "no encontré el envío o las consultas de la tarjeta");
  const principal = consultas.find(c => c.start < envio.start);
  ok(principal, "la tarjeta manda el principal sin consultar la lista de rebotados");
  strictEqual(texto(principal.arguments[1]), "email", "lo que se consulta tiene que ser el principal que se manda");
});

// ═══ A2 — al volver a abrir un sitio, Analysis conserva la fuente de cada email ═══════════════════
const DOM_REG = "baladag4.com.br";
const REGISTRANTE = "rudnypc@gmail.com";
const INFO = `info@${DOM_REG}`;

function fuenteTop(nombre) {
  const n = ast.body.find(s => s.type === "FunctionDeclaration" && s.id?.name === nombre);
  ok(n, `popup.js no tiene la función top-level ${nombre}`);
  return texto(n);
}
// Las escrituras de la caché de sesión que guardan emails, con su objeto tal cual está en popup.js.
function escriturasDeCacheConEmails() {
  const out = [];
  walk.full(ast, (n) => {
    if (n.type !== "CallExpression" || n.callee.type !== "Identifier" || n.callee.name !== "setSessionCache") return;
    const obj = n.arguments[1];
    if (obj?.type !== "ObjectExpression") return;
    const claves = obj.properties.filter(p => p.type === "Property").map(p => p.key?.name ?? p.key?.value);
    if (claves.includes("emails")) out.push({ obj, claves, linea: linea(n) });
  });
  return out;
}
// Una apertura del panel: runEmailScraper, addEmailsWithSource, la caché de sesión y el ranking REALES, con
// chrome.storage.session en memoria. La página publica info@; website.informer devuelve el gmail del registrante.
function abrirAnalisis(almacen, { informer = [REGISTRANTE] } = {}) {
  const state = { domain: DOM_REG, category: "", emails: [], emailSources: new Map(), tabId: 1, duplicate: null, traffic: 0 };
  const cuenta = { informer: 0 };
  const copia = (v) => JSON.parse(JSON.stringify(v));
  const chrome = {
    storage: {
      session: {
        get: async (k) => (k in almacen ? { [k]: copia(almacen[k]) } : {}),
        set: async (o) => { for (const [k, v] of Object.entries(o)) almacen[k] = copia(v); },
      },
      local: { get: async () => ({}), set: async () => {} },
    },
  };
  const nombres = ["getSessionCache", "setSessionCache", "addEmailsWithSource", "runEmailScraper", "_ctxEmailsAnalisis", "_fuenteTextoClient",
                   "_rankClient", "_motivoReboteClient", "_emailPickTierClient", "_ordenarEmailsClient", "_elegirPreseleccionClient", "_emailGradeCompute"];
  const deps = {
    state, chrome, rankEmail: E.rankEmail, vetoDuroEmail: E.vetoDuroEmail, esRegistranteWebmail: E.esRegistranteWebmail, motivoRebote: E.motivoRebote,
    _rebotesExtension: { cache: { set: new Set(), ts: 0 }, porDominio: new Map() }, tierDeEmail: E.tierDeEmail,
    compararCandidatosEmail: E.compararCandidatosEmail, _ordenTiersExtension: { orden: null }, _cleanScrapedEmails: E._cleanScrapedEmails, isGarbageEmail,
    quickValidateEmail: (e) => /^[^@\s]+@[^@\s]+\.[a-z]{2,}$/i.test(e),
    scrapeEmailsFromPage: async () => ({ emails: [INFO], socialLinks: [] }),
    scrapeContactPages: async () => [],
    scrapeWebsiteInformer: async () => { cuenta.informer++; return informer; },
    findDecisionMakerViaApollo: async () => null, scrapeEmailsFromSocialLinks: async () => new Map(),
    showLinkedIn() {}, renderApolloPeople() {}, _apolloAutoPaceReveal: async () => {},
    renderEmailList() {}, autoPushReady: {}, checkAutoPush() {}, updateScore() {}, saveHistory: async () => {},
    detectGeo: () => "", userKey: (k) => k, loadHistoryTab: async () => {},
    document: { getElementById: () => null }, console: { warn() {}, log() {}, error() {} },
  };
  const fns = ejecutar(deps, `${nombres.map(fuenteTop).join("\n")}\nreturn { ${nombres.join(", ")} };`);
  // Lo que deja puesto renderEmailList: sin basura, el orden compartido y la primera elegible.
  const preseleccion = () => fns._elegirPreseleccionClient(fns._ordenarEmailsClient([...new Set(state.emails)].filter(e => !isGarbageEmail(e, state.domain))));
  return { state, fns, cuenta, preseleccion };
}

test("A2: al volver a abrir el sitio en la misma sesión, el gmail del registrante sigue descartado y queda puesto el info@", async () => {
  const escrituras = escriturasDeCacheConEmails();
  strictEqual(escrituras.length, 2, `esperaba dos escrituras de la caché con emails (arranque y push nuevo), hay ${escrituras.length}`);
  for (const esc of escrituras) {
    ok(esc.claves.includes("emailSources"), `popup.js:${esc.linea} guarda los emails en la caché de sesión sin su fuente`);
    const almacen = {};
    const a = abrirAnalisis(almacen);
    await a.fns.runEmailScraper();
    strictEqual(a.state.emailSources.get(REGISTRANTE), "Informer");
    strictEqual(a.fns._emailPickTierClient(REGISTRANTE), -1, "primera apertura: el gmail del registrante no es contacto");
    strictEqual(a.preseleccion(), INFO, "primera apertura");

    // La escritura real, con el objeto tal cual está en popup.js.
    await a.fns.setSessionCache(DOM_REG, new Function("state", `return (${texto(esc.obj)});`)(a.state));

    const b = abrirAnalisis(almacen);
    await b.fns.runEmailScraper();
    strictEqual(b.state.emailSources.get(REGISTRANTE), "Informer", `popup.js:${esc.linea}: al volver a abrir, la fuente del registrante quedó "${b.state.emailSources.get(REGISTRANTE)}"`);
    strictEqual(b.state.emailSources.get(INFO), "Page");
    strictEqual(b.fns._emailPickTierClient(REGISTRANTE), -1, "al volver a abrir, el registrante subía a persona");
    strictEqual(b.fns._emailGradeCompute(REGISTRANTE, null, b.state.emailSources.get(REGISTRANTE)).grade, "E", "la nota A-E mira la misma fuente");
    strictEqual(b.preseleccion(), INFO, "al volver a abrir quedaba preseleccionado el gmail del registrante");

    // Navegar dentro del sitio con la caché ya marcada (contactScraped): informer no se vuelve a pedir, y la fuente sigue.
    const c = abrirAnalisis(almacen);
    await c.fns.runEmailScraper();
    strictEqual(c.cuenta.informer, 0, "con la caché marcada, website.informer no se vuelve a pedir");
    strictEqual(c.state.emailSources.get(REGISTRANTE), "Informer", "navegando dentro del sitio, la fuente se perdía");
    strictEqual(c.preseleccion(), INFO);
  }
});

test("A2: la fuente guardada sólo vuelve a una dirección que entró desde la caché; sin fuentes guardadas queda 'Cache'", async () => {
  const clave = `sess_${DOM_REG}`;
  // Una caché de antes del arreglo (sin emailSources): se comporta como siempre.
  const vieja = { [clave]: { emails: [INFO, REGISTRANTE], contactScraped: true } };
  const v = abrirAnalisis(vieja);
  await v.fns.runEmailScraper();
  deepStrictEqual([v.state.emailSources.get(INFO), v.state.emailSources.get(REGISTRANTE)], ["Cache", "Cache"]);

  // Fuentes de direcciones que no entraron (o que no son texto) no se inventan ni ensucian el Map.
  const rara = { [clave]: { emails: [INFO], emailSources: { [INFO]: "Page", "fantasma@otro-sitio.com": "Informer", [REGISTRANTE]: { source: "informer" } }, contactScraped: true } };
  const r = abrirAnalisis(rara);
  await r.fns.runEmailScraper();
  strictEqual(r.state.emailSources.get(INFO), "Page");
  ok(!r.state.emailSources.has("fantasma@otro-sitio.com") && !r.state.emails.includes("fantasma@otro-sitio.com"), "una fuente guardada no agrega direcciones");
  ok(!r.state.emailSources.has(REGISTRANTE), "una dirección que no está en la caché no aparece por su fuente");
});

// ═══ A3 — el mail desde Analysis anota los adicionales a nombre del sitio del mail ═════════════════
const SITIO_A = "sitio-a.com";
const SITIO_B = "sitio-b.com";
const VENTAS_A = `ventas@${SITIO_A}`;
const JUAN = `juan@${SITIO_A}`;
const MARIA = `maria@${SITIO_A}`;

function handlerGmail() {
  let hallado = null;
  walk.full(ast, (n) => {
    if (hallado || n.type !== "CallExpression") return;
    const c = n.callee;
    if (c.type !== "MemberExpression" || c.property?.name !== "addEventListener" || n.arguments[0]?.value !== "click") return;
    const obj = c.object.type === "ChainExpression" ? c.object.expression : c.object;
    if (obj.type === "CallExpression" && obj.arguments[0]?.value === "btn-send-gmail") hallado = n.arguments[1];
  });
  ok(hallado, "no encontré el handler de click de #btn-send-gmail");
  return hallado;
}

test("A3: después de la primera espera, el botón de Gmail no vuelve a leer del panel el sitio, el idioma, las fuentes ni los slots", () => {
  const h = handlerGmail();
  let primera = null;
  walk.full(h.body, (n) => { if (n.type === "AwaitExpression" && (!primera || n.start < primera.start)) primera = n; });
  ok(primera, "no encontré ninguna espera en el botón de Gmail");
  const tardias = [];
  walk.full(h.body, (n) => {
    if (n.start <= primera.start) return;
    if (n.type === "MemberExpression" && n.object.type === "Identifier" && n.object.name === "state"
        && ["domain", "siteLanguage", "monday", "emailSources"].includes(n.property?.name)) tardias.push(`state.${n.property.name} (popup.js:${linea(n)})`);
    if (n.type === "CallExpression" && n.callee.type === "MemberExpression" && n.callee.property?.name === "getElementById") {
      const a = n.arguments[0];
      if (a?.type !== "Literal" || /^form-email-futuro/.test(String(a.value))) tardias.push(`getElementById(${texto(a)}) (popup.js:${linea(n)})`);
    }
  });
  deepStrictEqual(tardias, [], "mientras sale el mail el panel puede pasar a otro sitio (scheduleRecheck + resetAnalysisUI): lo que se anota se lee al hacer click");
});

// Todo lo que el doble no simula se resuelve a un objeto neutro que acepta cualquier uso.
const NEUTRO = new Proxy(function () {}, {
  get: (_t, k) => (k === "then" ? undefined : k === Symbol.toPrimitive ? () => "" : NEUTRO),
  apply: () => NEUTRO, construct: () => NEUTRO, set: () => true,
});

// El handler REAL de #btn-send-gmail junto con setupAutoRefreshOnUrlChange (su scheduleRecheck, disparado por
// onActivated), resetAnalysisUI y _contactosAdicionales reales. Lo simulado: Chrome, la red y los módulos.
async function mandarDesdeAnalisis({ cambiarEn = null } = {}) {
  const els = {};
  const mk = (id, props = {}) => (els[id] = new Proxy({ id, value: "", textContent: "", className: "x", disabled: false, style: {}, ...props },
    { get: (t, k) => (k in t ? t[k] : NEUTRO), set: (t, k, v) => { t[k] = v; return true; } }));
  const document = { getElementById: (id) => els[id] || mk(id), querySelector: () => null, querySelectorAll: () => [] };
  mk("form-email", { value: VENTAS_A }); mk("pitch-text", { value: "Olá, somos a ADEQ" }); mk("form-subject", { value: "Proposta" });
  mk("form-email-futuro", { value: JUAN }); mk("form-email-futuro-2", { value: MARIA });

  const state = {
    domain: SITIO_A, url: `https://${SITIO_A}/`, tabId: 1, accessToken: "tk", loginEmail: "mb@adeqmedia.com",
    crmVeredicto: { ok: true }, duplicate: null, pitch: "", siteLanguage: "pt", emailSources: new Map([[VENTAS_A, "Page"]]), adicionalesEncolados: null,
  };
  const r = { anotado: [], tracking: [], cierres: [], filas: null };
  const listeners = {};
  const pendientes = [];
  const chrome = {
    tabs: {
      onUpdated: { addListener: (f) => { listeners.onUpdated = f; } },
      onActivated: { addListener: (f) => { listeners.onActivated = f; } },
      get: async (id) => ({ id, url: id === 2 ? `https://${SITIO_B}/nota` : `https://${SITIO_A}/` }),
      query: async () => [{ id: 2, url: `https://${SITIO_B}/nota` }],
    },
  };
  // El MB pasa a la pestaña del sitio B: Chrome dispara onActivated, el listener real corre scheduleRecheck
  // (state.domain = B y resetAnalysisUI en el momento) y, a los 800 ms, arranca la pipeline de B.
  const cambiarDePestana = async () => { await listeners.onActivated({ tabId: 2 }); await Promise.all(pendientes); };
  const vars = {
    document, state, chrome, console: { warn() {}, log() {}, error() {} },
    _autoRefreshWired: false, _autoRefreshLastDomain: null, _autoRefreshTimer: null,
    setTimeout: (fn) => { pendientes.push(Promise.resolve().then(fn)); return 0; }, clearTimeout: () => {},
    extractDomain: (u) => { try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return ""; } },
    runAnalysisPipeline: () => { state.emailSources = new Map([[`contacto@${SITIO_B}`, "Page"]]); state.siteLanguage = "en"; },
    _motivoBloqueoCrm: () => "", fotoCrmAlGuardar: () => null, _esFormularioUrl: () => false, isValidEmail: () => true,
    ensureFreshToken: async () => "tk",
    isEmailBounced: async (_tk, e) => { if (cambiarEn === "el rebote del 1er adicional" && e === JUAN) await cambiarDePestana(); return { bounced: false }; },
    checkUserCanDo: async () => ({ allowed: true }), _rateLimiter: { check: () => true },
    getGmailSignature: async () => { if (cambiarEn === "la firma") await cambiarDePestana(); return ""; },
    appendClosingIfMissing: (b, lang) => { r.cierres.push(lang); return b; },
    createManualSendTracking: async (_tk, o) => { r.tracking.push(o); return { ok: true, id: 77 }; },
    CONFIG: { SUPABASE_URL: "https://sb", SUPABASE_ANON_KEY: "anon" },
    sendEmail: async () => { if (cambiarEn === "el envío") await cambiarDePestana(); return { ok: true }; },
    markManualSendFailed: async () => ({}), incrementUserDailyCounter: async () => ({}),
    saveSendDate: async (d) => { r.anotado.push(`sendtrack:${d}`); return { ok: true }; },
    anotarEnvioDeSesion: (_m, d) => { r.anotado.push(`sesion:${d}`); }, _enviosDeLaSesion: new Map(),
    markReviewQueueAsContacted: async (_tk, d) => { r.anotado.push(`contactado:${d}`); return { ok: true }; },
    fetch: async (url, opts) => { if (String(url).includes("toolbar_reengagement_queue")) r.filas = JSON.parse(opts.body); return { ok: true, status: 201 }; },
  };
  const scope = new Proxy(vars, {
    has: () => true,
    get: (t, k) => (k === Symbol.unscopables ? undefined : k in t ? t[k] : k in globalThis ? globalThis[k] : NEUTRO),
    set: (t, k, v) => { t[k] = v; return true; },
  });
  const api = new Function("scope", `with (scope) {
    ${fuenteTop("setupAutoRefreshOnUrlChange")}
    ${fuenteTop("resetAnalysisUI")}
    ${fuenteTop("_contactosAdicionales")}
    return { setupAutoRefreshOnUrlChange, _contactosAdicionales, handler: (${texto(handlerGmail())}) };
  }`)(scope);
  api.setupAutoRefreshOnUrlChange();
  await api.handler();
  return { ...r, state, contactosAdicionales: () => api._contactosAdicionales(), estado: () => els["email-futuro-status"]?.textContent || "" };
}

test("A3: si el MB cambia de pestaña mientras sale el mail, los adicionales se programan y se anotan a nombre del sitio del mail", async () => {
  for (const cambiarEn of [null, "la firma", "el envío", "el rebote del 1er adicional"]) {
    const t = await mandarDesdeAnalisis({ cambiarEn });
    const cuando = cambiarEn ? `cambio de pestaña durante ${cambiarEn}` : "sin cambio de pestaña";
    strictEqual(t.state.domain, cambiarEn ? SITIO_B : SITIO_A, `${cuando}: el doble no dejó el panel donde correspondía`);
    deepStrictEqual(t.anotado, [`sendtrack:${SITIO_A}`, `sesion:${SITIO_A}`, `contactado:${SITIO_A}`], cuando);
    ok(Array.isArray(t.filas), `${cuando}: no se programó ningún adicional (resetAnalysisUI vació los slots) → "${t.estado()}"`);
    deepStrictEqual(t.filas.map(f => [f.domain, f.future_email, f.original_email]), [[SITIO_A, JUAN, VENTAS_A], [SITIO_A, MARIA, VENTAS_A]],
      `${cuando}: el worker avisaría al CRM del adicional a nombre de otro sitio, o se perdió uno`);
    strictEqual(t.state.adicionalesEncolados?.domain, SITIO_A, `${cuando}: los adicionales encolados quedaron a nombre de ${t.state.adicionalesEncolados?.domain}`);
    deepStrictEqual(t.state.adicionalesEncolados.lista.map(c => c.email), [JUAN, MARIA]);
    deepStrictEqual(t.tracking.map(x => [x.domain, x.language, x.email_source]), [[SITIO_A, "pt", "page"]], `${cuando}: el tracking del envío mezcla datos de otro sitio`);
    deepStrictEqual(t.cierres, ["pt"], `${cuando}: el cierre del mail salió en el idioma de otro sitio`);
    if (cambiarEn) deepStrictEqual(t.contactosAdicionales(), [], `${cuando}: con el panel en B, la ficha de B se llevaría el contacto de A`);
    else deepStrictEqual(t.contactosAdicionales().map(c => c.email), [JUAN, MARIA], "en el mismo sitio, el push lleva los adicionales con su hora");
  }
});

// ═══ A4 — una sola lista de emails en la tarjeta: los chips ═══════════════════════════════════════
test("A4: la tarjeta no arma una segunda lista de emails que no se dibuja, y conserva lo que sí se usa", () => {
  const tarjeta = funcion("renderProspectCard");
  const declaradas = [];
  walk.full(tarjeta, (n) => { if (n.type === "VariableDeclarator" && n.id.type === "Identifier") declaradas.push(n.id.name); });
  for (const muerta of ["emailOptions", "_idxPreseleccion", "_fuenteDeEmail"]) {
    ok(!declaradas.includes(muerta), `renderProspectCard declara ${muerta}: es de la lista de radios que no se dibuja, y un arreglo puede volver a ir a parar ahí`);
  }
  ok(!/pcard-email-radio/.test(popup), "no puede volver la lista de radios");
  // Lo que sí se usa: la preselección arranca el campo Email (lo que se envía), y los chips marcan la adivinada.
  ok(declaradas.includes("_ctxTarjeta") && declaradas.includes("_preseleccion"), "se borró la preselección que usa el campo Email");
  ok(/pcard-email-monday" value="\$\{esc\(_preseleccion\)\}"/.test(texto(tarjeta)), "el campo Email tiene que arrancar con la preselección");
  ok(/rol_mx:\s+\{ txt: "adivinado \(no publicado\)"/.test(popup), "los chips tienen que marcar la dirección adivinada");
});

test("A4: getSelectedEmail elige campo manual > campo Email > chip elegido, y no busca radios que no existen", () => {
  const getSelectedEmail = ejecutar({}, `${texto(funcion("getSelectedEmail"))}\nreturn getSelectedEmail;`);
  const buscados = [];
  const tarjeta = (els) => ({ querySelector: (sel) => { buscados.push(sel); return els[sel] ?? null; } });
  strictEqual(getSelectedEmail(tarjeta({ ".pcard-email-manual": { value: " a@diario.com " }, ".pcard-email-monday": { value: "b@diario.com" } })), "a@diario.com");
  strictEqual(getSelectedEmail(tarjeta({ ".pcard-email-manual": { value: "sin arroba" }, ".pcard-email-monday": { value: "b@diario.com" } })), "b@diario.com");
  strictEqual(getSelectedEmail(tarjeta({ ".pcard-email-list .email-chip.selected": { dataset: { email: "chip@diario.com" } } })), "chip@diario.com");
  strictEqual(getSelectedEmail(tarjeta({})), "", "sin nada elegido no hay email (y validateProspect lo dice)");
  deepStrictEqual(buscados.filter(s => /radio/.test(s)), [], "getSelectedEmail busca radios que la tarjeta no dibuja");
});
