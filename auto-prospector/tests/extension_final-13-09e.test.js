// La extensión, revisión final del 13/09 antes del deploy (cluster extension_final).
//
//   A1  La tarjeta de Prospects mandaba al email principal sin mirar la lista de rebotados: sólo preguntaba por
//       los adicionales. Analysis y el lote sí frenan. Un rebotado nunca se reusa, y "no pude preguntar" nunca
//       es "no rebotó".
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
import {
  adicionalesDeLaTarjeta, contactosDeAdicionales, filaColaDesdeFormulario, fotoCrmAlGuardar, anotarEnvioDeSesion,
} from "../../modules/colaEstado.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(aqui, "..", "..");
const popup = fs.readFileSync(path.join(RAIZ, "popup", "popup.js"), "utf8");
const ast = acorn.parse(popup, { ecmaVersion: "latest", sourceType: "module" });
const texto = (n) => popup.slice(n.start, n.end);

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
