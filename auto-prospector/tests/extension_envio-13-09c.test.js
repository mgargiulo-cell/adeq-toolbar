// El envío desde la extensión, ronda final del 13/09 (cluster extension_envio).
//
//   I21  Después de que el mail salía, la tarjeta de Prospects y Analysis esperaban pedidos a Supabase sin
//        reloj (sendtrack, la lista de rebotados, la marca de contactado, el historial) y la carga al CRM
//        tampoco tenía tope: un pedido colgado dejaba la tarjeta en "Processing…" con los botones apagados y
//        el CRM sin ficha. Y isEmailBounced decía "no rebotó" cuando no había podido preguntar: el lote y los
//        adicionales mandaban igual a una dirección que quizás ya había rebotado.
//   I25  El chequeo del zip (scripts/empaquetar.sh) no veía los imports escritos en varias líneas ni los
//        import() dinámicos: un import hacia un archivo que el zip no copia pasaba con el tilde verde.
//   a    Al volver a abrir un sitio al que este MB le escribió, la "Propuesta Vigente" sin ejecutivo que creó
//        el aviso de sus adicionales bloqueaba "Enviar a ADEQ" y "Guardar en cola" en Analysis. Mismo criterio
//        que el lote (decidirLoteCrm).
//   b    El Guard #3 del botón verde leía una bandera de sesión que nunca volvía a false: después de mandarle a
//        A dejaba cargar B sin mail, enviarAlBoard mandaba mail_ya_enviado=true y el CRM nunca le escribía a B.
//
// Run: npm test
/* eslint-disable no-new-func */
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as acorn from "acorn";
import * as walk from "acorn-walk";
import {
  decidirLoteCrm, fotoCrmAlGuardar, filaColaDesdeFormulario, lecturaDeEnvios, contactadoDeCola,
  adicionalesDeLaTarjeta, contactosDeAdicionales, anotarEnvioDeSesion, envioDeSesion,
  crmBloqueaCarga, veredictoConEnvioPropio, envioPropioGuardado, envioParaCargar,
} from "../../modules/colaEstado.js";
import { isEmailBounced, saveSendDate, createManualSendTracking, markReviewQueueAsContacted } from "../../modules/supabase.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(aqui, "..", "..");
const leer = (rel) => fs.readFileSync(path.join(RAIZ, rel), "utf8");
const parsear = (src) => acorn.parse(src, { ecmaVersion: "latest", sourceType: "module", locations: true });
const popup = leer("popup/popup.js");
const ast = parsear(popup);
const astSupa = parsear(leer("modules/supabase.js"));
const texto = (n) => popup.slice(n.start, n.end);
const sinEspacios = (s) => String(s).replace(/\s+/g, "");

function handlerDe(id) {
  let hallado = null;
  walk.full(ast, (n) => {
    if (hallado || n.type !== "CallExpression") return;
    const c = n.callee;
    if (c.type !== "MemberExpression" || c.property?.name !== "addEventListener") return;
    const obj = c.object.type === "ChainExpression" ? c.object.expression : c.object;
    if (obj.type === "CallExpression" && obj.arguments[0]?.value === id) hallado = n.arguments[1];
  });
  ok(hallado, `no encontré el handler de #${id} en popup.js`);
  return hallado;
}
function funcion(nombre) {
  let hallado = null;
  walk.full(ast, (n) => { if (!hallado && n.type === "FunctionDeclaration" && n.id?.name === nombre) hallado = n; });
  ok(hallado, `no encontré la función ${nombre} en popup.js`);
  return hallado;
}
function llamadas(arbol, nombre) {
  const out = [];
  walk.full(arbol, (n) => { if (n.type === "CallExpression" && n.callee.type === "Identifier" && n.callee.name === nombre) out.push({ nodo: n }); });
  return out;
}
const llamaA = (arbol, nombre) => llamadas(arbol, nombre).length > 0;
function ifCon(arbol, condicion) {
  let hallado = null;
  walk.full(arbol, (n) => { if (!hallado && n.type === "IfStatement" && sinEspacios(texto(n.test)) === sinEspacios(condicion)) hallado = n; });
  return hallado;
}
const contiene = (arbol, tipo) => { let si = false; walk.full(arbol, (m) => { if (m.type === tipo) si = true; }); return si; };
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

const HOY = "2026-09-13";
const AHORA = Date.parse("2026-09-13T12:00:00Z");
const hoyReal = () => new Date().toISOString().slice(0, 10);
const LIBRE = { ok: true, duda: false, found: false, estado: "", ejecutivo: "" };
const PV_PROPIA = { found: true, status: "Propuesta Vigente", ejecutivo: "" };
const PV_AJENA = { found: true, status: "Propuesta Vigente", ejecutivo: "otro@adeqmedia.com" };
const vencido = () => new DOMException("The operation was aborted due to timeout", "TimeoutError");
async function conFetch(falso, fn) {
  const antes = globalThis.fetch;
  globalThis.fetch = falso;
  try { return await fn(); } finally { globalThis.fetch = antes; }
}

// ═══ I21 — lo que se espera después de mandar tiene reloj ═════════════════════════════════════════
// Detector: todo `fetch` alcanzable desde las esperas posteriores a un punto, siguiendo las funciones que
// llama (en popup.js y en modules/supabase.js). Un fetch sin `signal` es uno que puede colgarse para siempre.
function indexar(fuentes) {
  const indice = new Map();
  for (const f of fuentes) {
    walk.full(f.ast, (n) => { if (n.type === "FunctionDeclaration" && n.id && !indice.has(n.id.name)) indice.set(n.id.name, { fuente: f, nodo: n }); });
  }
  return indice;
}
const tieneReloj = (llamada) => {
  const op = llamada.arguments[1];
  return op?.type === "ObjectExpression" && op.properties.some(p => p.type === "Property" && (p.key?.name ?? p.key?.value) === "signal");
};
function fetchSinReloj(raiz, fuente, indice, vistos = new Set(), out = []) {
  walk.full(raiz, (n) => {
    if (n.type !== "CallExpression" || n.callee.type !== "Identifier") return;
    if (n.callee.name === "fetch") { if (!tieneReloj(n)) out.push(`${fuente.nombre}:${n.loc.start.line}`); return; }
    const dec = indice.get(n.callee.name);
    if (!dec || vistos.has(n.callee.name)) return;
    vistos.add(n.callee.name);
    fetchSinReloj(dec.nodo.body, dec.fuente, indice, vistos, out);
  });
  return out;
}
function esperasSinReloj(fn, desde, fuente, indice) {
  const out = [];
  walk.full(fn, (n) => { if (n.type === "AwaitExpression" && n.start > desde) fetchSinReloj(n.argument, fuente, indice, new Set(), out); });
  return [...new Set(out)];
}
function asignacionTrue(arbol, nombre) {
  let hallado = null;
  walk.full(arbol, (n) => {
    if (!hallado && n.type === "AssignmentExpression" && n.left.type === "Identifier" && n.left.name === nombre && n.right.type === "Literal" && n.right.value === true) hallado = n;
  });
  return hallado;
}
const FUENTES = [{ nombre: "popup/popup.js", ast }, { nombre: "modules/supabase.js", ast: astSupa }];
const INDICE = indexar(FUENTES);

test("I21: el detector de esperas sin reloj se prueba a sí mismo", () => {
  const src = [
    "async function colgada(u) { return fetch(u, { method: 'POST' }); }",
    "async function envuelta(u) { return colgada(u); }",
    "async function conReloj(u) { return fetch(u, { signal: AbortSignal.timeout(1) }); }",
    "async function flujo() {",
    "  await colgada('antes');",
    "  let mailSalio = false;",
    "  mailSalio = true;",
    "  await conReloj('a');",
    "  await envuelta('b').catch(() => null);",
    "}",
  ].join("\n");
  const f = { nombre: "mini.js", ast: parsear(src) };
  const indice = indexar([f]);
  const flujo = indice.get("flujo").nodo;
  deepStrictEqual(esperasSinReloj(flujo, asignacionTrue(flujo, "mailSalio").start, f, indice), ["mini.js:1"],
    "tiene que ver el fetch colgado a través de la función que lo envuelve, y sólo lo que se espera después del mail");
});

test("I21: todo lo que la tarjeta y Analysis esperan después de que salió el mail tiene reloj, y la carga al CRM también", () => {
  const tarjeta = funcion("validateProspect");
  const marca = asignacionTrue(tarjeta, "mailSalio");
  ok(marca, "no encontré 'mailSalio = true' en validateProspect");
  deepStrictEqual(esperasSinReloj(tarjeta, marca.start, FUENTES[0], INDICE), [],
    "la tarjeta espera un pedido sin reloj después de mandar: colgado, queda en 'Processing…' y el CRM sin ficha");
  const gmail = handlerDe("btn-send-gmail");
  const [envio] = llamadas(gmail, "sendEmail");
  ok(envio, "no encontré sendEmail en el botón de Gmail");
  deepStrictEqual(esperasSinReloj(gmail, envio.nodo.start, FUENTES[0], INDICE), [], "Analysis espera un pedido sin reloj después de mandar");
  const [carga] = llamadas(funcion("enviarAlBoard"), "fetch");
  ok(carga && tieneReloj(carga.nodo), "la carga al CRM no tiene reloj");
  for (const nombre of ["createManualSendTracking", "saveSendDate", "isEmailBounced", "markReviewQueueAsContacted", "validateReviewItem", "saveHistory", "markManualSendFailed"]) {
    const dec = INDICE.get(nombre);
    ok(dec && dec.fuente.nombre === "modules/supabase.js", `no encontré ${nombre} en modules/supabase.js`);
    deepStrictEqual(fetchSinReloj(dec.nodo.body, dec.fuente, INDICE), [], `${nombre} tiene un fetch sin reloj`);
  }
});

test("I21: isEmailBounced dice 'no sé' (ok:false) cuando no pudo preguntar, y 'no rebotó' sólo con la lista en la mano", async () => {
  const senales = [];
  const pedir = (resp) => async (_u, o) => { senales.push(o?.signal); if (resp instanceof DOMException || resp instanceof Error) throw resp; return resp; };
  const r1 = await conFetch(pedir(vencido()), () => isEmailBounced("tk", "Ventas@Diario.com.mx"));
  deepStrictEqual([r1.bounced, r1.ok, r1.error], [false, false, "no contestó en 8 s"], "un pedido que no contesta no es 'no rebotó'");
  ok(senales[0] instanceof AbortSignal, "sin reloj, el pedido podía colgar la tarjeta y el lote para siempre");
  const r2 = await conFetch(pedir({ ok: false, status: 500 }), () => isEmailBounced("tk", "a@b.com"));
  deepStrictEqual([r2.bounced, r2.ok, r2.status], [false, false, 500]);
  const r3 = await conFetch(pedir({ ok: true, status: 200, json: async () => { throw new Error("html de Cloudflare"); } }), () => isEmailBounced("tk", "a@b.com"));
  deepStrictEqual([r3.bounced, r3.ok], [false, false], "una respuesta ilegible no es una lista vacía");
  const r4 = await conFetch(pedir({ ok: true, status: 200, json: async () => [] }), () => isEmailBounced("tk", "a@b.com"));
  deepStrictEqual([r4.bounced, r4.ok], [false, true]);
  const r5 = await conFetch(pedir({ ok: true, status: 200, json: async () => [{ reason: "550", created_at: "2026-09-01" }] }), () => isEmailBounced("tk", "a@b.com"));
  deepStrictEqual([r5.bounced, r5.ok, r5.reason], [true, true, "550"]);
  const r6 = await isEmailBounced("", "a@b.com");
  deepStrictEqual([r6.bounced, r6.ok], [false, false], "sin sesión tampoco se pudo preguntar");
  const r7 = await isEmailBounced("tk", "https://sitio.com/contacto");
  deepStrictEqual([r7.bounced, r7.ok], [false, true], "una URL de formulario no es un email: no hay nada que haya rebotado");
});

test("I21: sendtrack, el tracking y la marca de contactado llevan reloj y dicen cuando no entraron", async () => {
  const antesChrome = globalThis.chrome;
  const almacen = {};
  globalThis.chrome = { storage: { local: {
    get: async (k) => ({ [k]: almacen[k] }),
    set: async (o) => { Object.assign(almacen, JSON.parse(JSON.stringify(o))); },
  } } };
  try {
    const senales = [];
    const colgado = async (_u, o) => { senales.push(o?.signal); throw vencido(); };
    const st = await conFetch(colgado, () => saveSendDate("diario.com.mx", { sendDate: HOY, pitch: "hola", email: "a@diario.com.mx", mbEmail: "MB@adeqmedia.com", crmAlEnviar: LIBRE }));
    deepStrictEqual([st.ok, st.error], [false, "no contestó en 8 s"]);
    const tr = await conFetch(colgado, () => createManualSendTracking("tk", { user_email: "mb@adeqmedia.com", domain: "diario.com.mx" }));
    strictEqual(tr.ok, false);
    const mk = await conFetch(async (_u, o) => { senales.push(o?.signal); return { ok: false, status: 500 }; },
      () => markReviewQueueAsContacted("tk", "diario.com.mx", "mb@adeqmedia.com"));
    deepStrictEqual([mk.ok, mk.status], [false, 500], "devolvía ok:true aunque la base contestara 500");
    strictEqual(senales.length, 3);
    ok(senales.every(s => s instanceof AbortSignal), "algún pedido del envío salió sin reloj");
    // (punto a) La copia local dice quién mandó y qué decía el CRM antes de mandar.
    deepStrictEqual(almacen.sendtrack["diario.com.mx"], { sendDate: HOY, pitch: "hola", email: "a@diario.com.mx", mbEmail: "mb@adeqmedia.com", crm: LIBRE });
  } finally {
    if (antesChrome === undefined) delete globalThis.chrome; else globalThis.chrome = antesChrome;
  }
});

test("I21: el lote no carga una fila si no pudo preguntar si su email rebotó", () => {
  const h = handlerDe("btn-cola-enviar");
  const [chequeo] = llamadas(h, "isEmailBounced");
  const [carga] = llamadas(h, "enviarAlBoard");
  ok(chequeo && carga, "no encontré el chequeo de rebote o la carga en el lote");
  const corta = ifCon(h, "b.ok === false");
  ok(corta && corta.start > chequeo.nodo.start && corta.start < carga.nodo.start, "'no pude preguntar' contaba como 'no rebotó' y la fila salía");
  ok(contiene(corta.consequent, "ContinueStatement") && /fallaron\.push\(/.test(texto(corta.consequent)), "la fila tiene que figurar como fallada, con el motivo");
  let respaldo = null;
  walk.full(h, (n) => {
    if (!respaldo && n.type === "CallExpression" && n.callee.type === "MemberExpression" && n.callee.object === chequeo.nodo && n.callee.property?.name === "catch") respaldo = n;
  });
  ok(!respaldo || /ok:\s*false/.test(texto(respaldo.arguments[0])), "si el pedido tira, el respaldo tiene que ser 'no sé', no 'no rebotó'");
});

test("I21: en Analysis, sin respuesta de la lista de rebotados no sale el mail ni se programa un adicional", () => {
  const h = handlerDe("btn-send-gmail");
  const [principal, adicional] = llamadas(h, "isEmailBounced");
  const [envio] = llamadas(h, "sendEmail");
  ok(principal && adicional && envio, "no encontré los chequeos de rebote del botón de Gmail");
  ok(principal.nodo.start < envio.nodo.start && adicional.nodo.start > envio.nodo.start);
  ok(/ensureFreshToken\(/.test(texto(principal.nodo.arguments[0])), "con un token vencido la lista 'no contesta': hace falta el token fresco antes");
  const noSale = ifCon(h, "b.ok === false");
  ok(noSale && noSale.start < envio.nodo.start && contiene(noSale.consequent, "ReturnStatement"), "sin saber si rebotó, el mail salía igual");
  const noSePrograma = ifCon(h, "bFut.ok === false");
  ok(noSePrograma && contiene(noSePrograma.consequent, "ContinueStatement"), "sin saber si rebotó, el adicional se programaba igual (el worker no vuelve a mirar)");
});

test("I21: los adicionales de la tarjeta sin respuesta de la lista no se programan, y lo dicen", () => {
  const { filas, avisos } = adicionalesDeLaTarjeta({
    domain: "diario.com.mx", mbEmail: "mb@adeqmedia.com", principal: "publicidad@diario.com.mx",
    candidatos: ["ventas@diario.com.mx", "direccion@diario.com.mx", "prensa@diario.com.mx"],
    rebotados: new Set(["prensa@diario.com.mx"]), sinConfirmar: new Set(["ventas@diario.com.mx"]), ahoraMs: AHORA,
  });
  deepStrictEqual(filas.map(f => [f.future_email, f.sequence]), [["direccion@diario.com.mx", 1]]);
  ok(avisos.some(a => /ventas@diario\.com\.mx: no pude confirmar si rebotó/.test(a)), avisos.join(" · "));
  ok(avisos.some(a => /prensa@diario\.com\.mx bounced/.test(a)), avisos.join(" · "));
});

// validateProspect, extraída con acorn y ejecutada con dobles (mismo método que cola_por_enviar-13-09c).
function armarTarjeta({ rebote = async () => ({ bounced: false, ok: true }) } = {}) {
  const log = [], pedidos = [], fichas = [], sendtrack = [];
  const envios = new Map();
  const valores = {
    ".pcard-pitch": "Hola, somos ADEQ", ".pcard-subject": "Propuesta", ".pcard-owner": "mb", ".pcard-status": "Propuesta Vigente",
    ".pcard-lang": "1", ".pcard-geo": "Mexico", ".pcard-date": "13/09/2026", ".pcard-traffic": "900K",
    ".pcard-future-1": "ventas@diario.com.mx",
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
    getSelectedEmail: () => "publicidad@diario.com.mx", defaultOwnerForLang: () => "mb", defaultStatusForOwner: () => "Propuesta Vigente",
    LANG_TO_IDX: { es: "1" }, formatTraffic: (n) => `${Math.round(n / 1000)}K`, isValidEmail: (e) => /@/.test(e),
    ensureFreshToken: async () => "tk", getGmailSignature: async () => "MB · ADEQ", appendClosingIfMissing: (s) => s,
    createManualSendTracking: async () => ({ ok: true, id: 77 }),
    CONFIG: { SUPABASE_URL: "https://sb", SUPABASE_ANON_KEY: "anon" },
    sendEmail: async ({ to }) => { log.push(`mail:${to}`); return { ok: true }; },
    markManualSendFailed: async () => {}, incrementUserDailyCounter: async () => {},
    saveSendDate: async (d, o) => { sendtrack.push([d, o]); return { ok: true }; },
    markReviewQueueAsContacted: async () => ({ ok: true }),
    isEmailBounced: async (_tk, e) => rebote(e),
    adicionalesDeLaTarjeta, contactosDeAdicionales, filaColaDesdeFormulario, fotoCrmAlGuardar,
    anotarEnvioDeSesion, _enviosDeLaSesion: envios,
    fetch: async (url, opts = {}) => {
      const tabla = url.split("/rest/v1/")[1].split("?")[0];
      pedidos.push({ tabla, opts });
      return { ok: true, status: 201, json: async () => [{ id: 42 }] };
    },
    AbortSignal,
    enviarAlBoard: async (p) => { fichas.push(p); return {}; },
    saveHistory: async () => {}, validateReviewItem: async () => ({ ok: true }), queuePendingMark: async () => {},
    refreshProspectsStats: () => {}, setTimeout: () => {}, console: { error() {}, warn() {}, log() {} },
    showToast: () => {}, window: {},
  };
  const validateProspect = ejecutar(deps, `${texto(funcion("_tarjetaAPorEnviar"))}\n${texto(funcion("validateProspect"))}\nreturn validateProspect;`);
  const data = { id: 42, domain: "diario.com.mx", traffic: 900000, language: "es", emails: ["publicidad@diario.com.mx"], email_sources: {}, contact_phone: "" };
  return { correr: () => validateProspect(card, data, true), card, log, pedidos, fichas, sendtrack, envios };
}

test("I21: la tarjeta manda el principal y no programa el adicional cuya lista de rebotados no contestó", async () => {
  const t = armarTarjeta({ rebote: async (e) => (e === "ventas@diario.com.mx" ? { bounced: false, ok: false, error: "no contestó en 8 s" } : { bounced: false, ok: true }) });
  await t.correr();
  ok(t.log.includes("mail:publicidad@diario.com.mx"), t.log.join(" → "));
  strictEqual(t.fichas.length, 1);
  deepStrictEqual(t.fichas[0].contactos, [], "la ficha no puede anotar un adicional que no se programó");
  ok(!t.pedidos.some(p => p.tabla === "toolbar_reengagement_queue"), "el adicional sin respuesta de la lista se programaba igual");
  ok(/no pude confirmar si rebotó/.test(t.card.querySelector(".pcard-future-status").textContent), t.card.querySelector(".pcard-future-status").textContent);
});

// enviarAlBoard, extraída con su reloj y ejecutada con un fetch falso.
function armarEnviarAlBoard(fetchFalso) {
  const ini = popup.indexOf("const _CRM_CARGA_TIMEOUT_MS");
  const fn = funcion("enviarAlBoard");
  ok(ini > 0 && ini < fn.start, "enviarAlBoard no tiene su reloj declarado");
  return ejecutar({
    CONFIG: { CRM_BOARD_URL: "https://crm/sync-toolbar", CRM_BOARD_SECRET: "s" }, _estadoLabel: (e) => e || "",
    _BOARD_EJEC: {}, _BOARD_IDIOMA: {}, state: { loginEmail: "mb@adeqmedia.com" }, formatTraffic: String,
    _contactosAdicionales: () => [], console: { warn() {} }, fetch: fetchFalso, AbortSignal,
  }, `${popup.slice(ini, fn.end)}\nreturn enviarAlBoard;`);
}

test("I21: la carga al CRM tiene reloj, y un timeout se dice 'el CRM no contestó'", async () => {
  let senal = null;
  const colgado = armarEnviarAlBoard(async (_u, o) => { senal = o?.signal; throw vencido(); });
  let error = null;
  try { await colgado({ domain: "diario.com.mx", email: "a@diario.com.mx" }); } catch (e) { error = e; }
  ok(senal instanceof AbortSignal, "la carga al CRM salía sin reloj");
  ok(error && /el CRM no contestó en 15 s/.test(error.message), String(error?.message));
  const bien = armarEnviarAlBoard(async () => ({ ok: true, status: 200, json: async () => ({ creados: 1 }) }));
  deepStrictEqual(await bien({ domain: "diario.com.mx" }), { creados: 1 });
});

// ═══ I25 — el chequeo del zip ve todos los imports ═══════════════════════════════════════════════
// Se corre el verificador DEL SCRIPT (el bloque de Python de scripts/empaquetar.sh), no una copia: sobre el
// paquete armado igual que el script y sobre casos chicos. Y se compara con lo que cuenta acorn.
const SH = leer("scripts/empaquetar.sh");
const SIN_PYTHON = spawnSync("python3", ["--version"], { encoding: "utf8" }).status !== 0 && "sin python3 en esta máquina";

function queCopiaElScript() {
  const dirs = /cp -R "\$REPO"\/\{([^}]+)\}/.exec(SH);
  const libs = /cp "\$REPO"\/auto-prospector\/lib\/\{([^}]+)\}/.exec(SH);
  ok(dirs && libs, "no encontré qué copia scripts/empaquetar.sh");
  return { raiz: dirs[1].split(","), lib: libs[1].split(",") };
}
function verificadorDelScript() {
  const m = /python3 - "\$ST" <<'PY'\n([\s\S]*?)\nPY\n/.exec(SH);
  ok(m, "no encontré el verificador del paquete en scripts/empaquetar.sh");
  return m[1];
}
function conCarpeta(fn) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "adeq-zip-"));
  try { return fn(base); } finally { fs.rmSync(base, { recursive: true, force: true }); }
}
function verificar(base, paquete) {
  const py = path.join(base, "verificar.py");
  fs.writeFileSync(py, verificadorDelScript());
  const r = spawnSync("python3", [py, paquete], { encoding: "utf8" });
  return { codigo: r.status, salida: `${r.stdout || ""}${r.stderr || ""}` };
}
function importsConAcorn(dir) {
  const est = [], din = [], armados = [];
  const recorrer = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) { recorrer(p); continue; }
      if (!e.name.endsWith(".js")) continue;
      walk.full(acorn.parse(fs.readFileSync(p, "utf8"), { ecmaVersion: "latest", sourceType: "module" }), (n) => {
        const dinamico = n.type === "ImportExpression";
        const estatico = n.type === "ImportDeclaration" || n.type === "ExportAllDeclaration" || (n.type === "ExportNamedDeclaration" && n.source);
        if (!dinamico && !estatico) return;
        if (n.source.type !== "Literal") { armados.push(path.relative(dir, p)); return; }
        if (!String(n.source.value).startsWith(".")) return;
        (dinamico ? din : est).push({ archivo: p, destino: path.normalize(path.join(path.dirname(p), n.source.value)) });
      });
    }
  };
  recorrer(dir);
  return { est, din, armados };
}
function escribir(dir, archivos) {
  for (const [rel, src] of Object.entries(archivos)) {
    const p = path.join(dir, rel);
    fs.mkdirSync(path.dirname(p), { recursive: true });
    fs.writeFileSync(p, src);
  }
}

test("I25: todo import relativo del paquete apunta a un archivo que el zip lleva, y el verificador cuenta lo mismo que acorn", { skip: SIN_PYTHON }, () => {
  conCarpeta((base) => {
    const paquete = path.join(base, "paquete");
    const { raiz, lib } = queCopiaElScript();
    for (const x of raiz) fs.cpSync(path.join(RAIZ, x), path.join(paquete, x), { recursive: true });
    fs.mkdirSync(path.join(paquete, "auto-prospector", "lib"), { recursive: true });
    for (const x of lib) fs.copyFileSync(path.join(RAIZ, "auto-prospector", "lib", x), path.join(paquete, "auto-prospector", "lib", x));
    const manifest = JSON.parse(fs.readFileSync(path.join(paquete, "manifest.json"), "utf8"));
    delete manifest.key;
    fs.writeFileSync(path.join(paquete, "manifest.json"), JSON.stringify(manifest));

    const { est, din, armados } = importsConAcorn(paquete);
    deepStrictEqual(armados, [], "un import() con la ruta armada en tiempo de ejecución no lo puede verificar nadie");
    ok(est.length > 50 && din.length > 0, `acorn contó ${est.length} estáticos y ${din.length} dinámicos: el armado del paquete falló`);
    for (const x of [...est, ...din]) ok(fs.existsSync(x.destino), `${path.relative(paquete, x.archivo)} importa ${path.relative(paquete, x.destino)}, que el zip no lleva`);
    const r = verificar(base, paquete);
    strictEqual(r.codigo, 0, r.salida);
    const cuenta = /estáticos (\d+) · dinámicos (\d+)/.exec(r.salida);
    ok(cuenta, `el verificador no separa estáticos y dinámicos: ${r.salida}`);
    deepStrictEqual([Number(cuenta[1]), Number(cuenta[2])], [est.length, din.length], "el verificador del zip no ve los mismos imports que acorn");
  });
});

test("I25: un import en varias líneas o un import() hacia un archivo que el zip no lleva frena el paquete", { skip: SIN_PYTHON }, () => {
  conCarpeta((base) => {
    const caso = (nombre, archivos) => {
      const dir = path.join(base, nombre);
      escribir(dir, { "manifest.json": "{}", ...archivos });
      return verificar(base, dir);
    };
    const multilinea = caso("multilinea", { "popup/popup.js": `import {\n  a,\n  b,\n} from "../auto-prospector/lib/noEsta.js";\nconsole.log(a, b);\n` });
    strictEqual(multilinea.codigo, 1, `un import en varias líneas hacia un archivo que falta daba el tilde verde: ${multilinea.salida}`);
    ok(/noEsta\.js/.test(multilinea.salida), multilinea.salida);
    const dinamico = caso("dinamico", { "popup/popup.js": `export async function f() { return import("../auto-prospector/lib/noEsta.js"); }\n` });
    strictEqual(dinamico.codigo, 1, `un import() hacia un archivo que falta daba el tilde verde: ${dinamico.salida}`);
    const sano = caso("sano", {
      "popup/popup.js": `import {\n  x,\n} from "./otro.js";\nexport const y = () => import("./otro.js");\n`,
      "popup/otro.js": "export const x = 1;\n",
    });
    strictEqual(sano.codigo, 0, sano.salida);
    ok(/estáticos 1 · dinámicos 1/.test(sano.salida), sano.salida);
  });
});

// ═══ a — la ficha que creó nuestro envío no bloquea cargarla ni guardarla en Analysis ═════════════
test("a: la 'Propuesta Vigente' sin ejecutivo que creó nuestro envío no bloquea cargar ni guardar; el mail sí", () => {
  const envio = { el: HOY, enSendtrack: true, crm: LIBRE };
  const v = veredictoConEnvioPropio(veredictoCrm(PV_PROPIA), { dup: PV_PROPIA, envio });
  strictEqual(v.fichaPropia, true, "bloqueaba 'Enviar a ADEQ' y 'Guardar en cola' en un sitio al que este MB le escribió");
  strictEqual(crmBloqueaCarga(v), false);
  ok(!v.ok && !v.duda, "el botón del mail sigue frenado: repetirlo es un duplicado al mismo contacto");
  deepStrictEqual(v.envioPropio, { el: HOY, enSendtrack: true });
  deepStrictEqual(fotoCrmAlGuardar(v, PV_PROPIA), LIBRE, "'Guardar' anota lo que decía el CRM cuando salió el mail, no la ficha que creó el aviso");

  const casos = [
    // [ficha de hoy, envío, por qué sigue bloqueado]
    [PV_AJENA, envio, "una propuesta con ejecutivo es de alguien"],
    [PV_PROPIA, null, "sin un envío de este MB, nada nuestro explica la ficha"],
    [PV_PROPIA, { ...envio, crm: { ...LIBRE, found: true, estado: "Pausado" } }, "la ficha ya existía cuando se mandó"],
    [PV_PROPIA, { ...envio, crm: { ok: false, duda: false, found: true, estado: "Live", ejecutivo: "x@adeqmedia.com" } }, "el CRM ya decía que no cuando se mandó"],
    [{ found: true, status: "Live", ejecutivo: "" }, envio, "un cliente activo, nunca"],
    [{ found: true, status: "En Negociacion", ejecutivo: "" }, envio, "una negociación la pone una persona"],
    [{ found: false, bloqueado: "TLD vetado" }, envio, "bloqueado por lista"],
  ];
  for (const [dup, env, porque] of casos) {
    const vc = veredictoCrm(dup);
    const r = veredictoConEnvioPropio(vc, { dup, envio: env });
    strictEqual(r, vc, `${porque}: el veredicto no tiene que cambiar`);
    strictEqual(crmBloqueaCarga(r), true, porque);
  }
  const libre = veredictoCrm({ found: false });
  strictEqual(veredictoConEnvioPropio(libre, { dup: { found: false }, envio }), libre);
  strictEqual(crmBloqueaCarga(libre), false);
  strictEqual(crmBloqueaCarga(veredictoCrm({ found: false, indeterminado: true })), false, "la duda avisa, no bloquea (como siempre)");
  strictEqual(crmBloqueaCarga(null), false);
  // Un envío sin foto (anotado antes del arreglo en la misma sesión): decide el envío, como en el lote.
  strictEqual(veredictoConEnvioPropio(veredictoCrm(PV_PROPIA), { dup: PV_PROPIA, envio: { el: HOY } }).fichaPropia, true);
});

test("a: el envío que sobrevive a cerrar la toolbar es el de este MB, a este sitio, dentro de 30 días", () => {
  const local = {
    "www.Diario.com.mx": { sendDate: "2026-09-12", email: "a@diario.com.mx", mbEmail: "mb@adeqmedia.com", crm: LIBRE },
    "diario.com.mx": { sendDate: "2026-09-01", email: "b@diario.com.mx", mbEmail: "mb@adeqmedia.com", crm: null },
    "otro.com": { sendDate: "2026-09-12", mbEmail: "otra@adeqmedia.com", crm: LIBRE },
    "viejo.com": { sendDate: "2026-09-12", email: "x@viejo.com" },
    "lejano.com": { sendDate: "2026-07-01", mbEmail: "mb@adeqmedia.com", crm: LIBRE },
  };
  const op = { mbEmail: "MB@adeqmedia.com", ahoraMs: AHORA };
  deepStrictEqual(envioPropioGuardado(local, "diario.com.mx", op), { el: "2026-09-12", enSendtrack: false, crm: LIBRE }, "el más reciente, sin importar www ni mayúsculas");
  strictEqual(envioPropioGuardado(local, "otro.com", op), null, "lo mandó otro MB desde este navegador");
  strictEqual(envioPropioGuardado(local, "viejo.com", op), null, "una entrada de antes del arreglo no dice quién mandó: no prueba nada");
  strictEqual(envioPropioGuardado(local, "lejano.com", op), null, "fuera de la ventana de la cola");
  strictEqual(envioPropioGuardado(local, "nuevo.com", op), null);
  strictEqual(envioPropioGuardado(local, "diario.com.mx", { ahoraMs: AHORA }), null, "sin saber quién está logueado no se afirma nada");
  strictEqual(envioPropioGuardado(undefined, "diario.com.mx", op), null);
});

test("a: la foto del CRM de la sesión es la del primer mail a ese sitio", () => {
  const envios = new Map();
  anotarEnvioDeSesion(envios, "diario.com.mx", { el: HOY, enSendtrack: true, crm: LIBRE });
  anotarEnvioDeSesion(envios, "www.diario.com.mx", { el: HOY, enSendtrack: false, crm: { ok: false, duda: false, found: true, estado: "Propuesta Vigente", ejecutivo: "" } });
  deepStrictEqual(envioDeSesion(envios, "diario.com.mx"), { el: HOY, enSendtrack: true, crm: LIBRE },
    "el segundo mail ya ve la ficha que creó nuestro aviso: ésa no es la foto de antes");
});

// _aplicarBloqueoCrm, ejecutada con botones falsos.
function armarBloqueo() {
  const boton = (textContent) => {
    const clases = new Set();
    return { clases, classList: { toggle: (c, si) => { if (si) clases.add(c); else clases.delete(c); } }, title: "", disabled: false, textContent, dataset: {} };
  };
  const els = { "btn-push-monday": boton("🚀 Enviar a ADEQ"), "btn-send-gmail": boton("📧 Send via Gmail"), "btn-guardar-cola": boton("📥 Guardar") };
  const borrador = [];
  const aplicar = ejecutar({
    document: { getElementById: (id) => els[id] || null }, crmBloqueaCarga, _bloquearBorradorCrm: (b) => borrador.push(b),
  }, `${texto(funcion("_aplicarBloqueoCrm"))}\nreturn _aplicarBloqueoCrm;`);
  const bloqueado = (id) => els[id].clases.has("btn-bloqueado-crm");
  return { aplicar, els, borrador, bloqueado };
}

test("a: en Analysis la ficha propia deja cargar y guardar; el mail y el borrador siguen frenados", () => {
  const t = armarBloqueo();
  t.aplicar(veredictoConEnvioPropio(veredictoCrm(PV_PROPIA), { dup: PV_PROPIA, envio: { el: HOY, crm: LIBRE } }));
  deepStrictEqual([t.bloqueado("btn-push-monday"), t.bloqueado("btn-guardar-cola"), t.els["btn-guardar-cola"].disabled], [false, false, false],
    "'Enviar a ADEQ' y 'Guardar en cola' quedaban bloqueados sobre la ficha que creó nuestro envío");
  strictEqual(t.els["btn-push-monday"].textContent, "🚀 Enviar a ADEQ");
  strictEqual(t.bloqueado("btn-send-gmail"), true, "el mail ya salió: no se vuelve a ofrecer");
  deepStrictEqual(t.borrador, [true]);

  const u = armarBloqueo();
  u.aplicar(veredictoConEnvioPropio(veredictoCrm(PV_AJENA), { dup: PV_AJENA, envio: { el: HOY, crm: LIBRE } }));
  deepStrictEqual(["btn-push-monday", "btn-guardar-cola", "btn-send-gmail"].map(u.bloqueado), [true, true, true], "una propuesta ajena bloquea todo, como siempre");
  ok(u.els["btn-guardar-cola"].disabled && /No prospectable/.test(u.els["btn-push-monday"].textContent));
});

// _veredictoAnalisis y _envioPropio, ejecutadas con un chrome.storage falso.
function armarVeredicto({ sendtrack, sesion = new Map(), loginEmail = "mb@adeqmedia.com", storageFalla = false } = {}) {
  let lecturas = 0;
  const veredicto = ejecutar({
    _veredictoCrm: veredictoCrm, veredictoConEnvioPropio, envioDeSesion, envioPropioGuardado, _enviosDeLaSesion: sesion,
    state: { loginEmail },
    chrome: { storage: { local: { get: async () => { lecturas++; if (storageFalla) throw new Error("storage"); return { sendtrack }; } } } },
  }, `${texto(funcion("_veredictoAnalisis"))}\n${texto(funcion("_envioPropio"))}\nreturn _veredictoAnalisis;`);
  return { veredicto, lecturas: () => lecturas };
}

test("a: al volver a abrir el sitio, Analysis reconoce la ficha que creó el envío de este MB", async () => {
  const local = { "diario.com.mx": { sendDate: hoyReal(), mbEmail: "mb@adeqmedia.com", crm: LIBRE } };
  const v = await armarVeredicto({ sendtrack: local }).veredicto(PV_PROPIA, "diario.com.mx");
  strictEqual(v.fichaPropia, true, "con la toolbar cerrada y vuelta a abrir, la ficha de nuestro aviso bloqueaba el sitio");
  strictEqual(crmBloqueaCarga(v), false);
  strictEqual(crmBloqueaCarga(await armarVeredicto({ sendtrack: local, loginEmail: "otra@adeqmedia.com" }).veredicto(PV_PROPIA, "diario.com.mx")), true,
    "otro MB en el mismo navegador no hereda el envío");
  strictEqual((await armarVeredicto({ sendtrack: local, loginEmail: "" }).veredicto(PV_PROPIA, "diario.com.mx", "mb@adeqmedia.com")).fichaPropia, true,
    "el chequeo temprano pasa el login a mano (todavía no está en state)");
  strictEqual(crmBloqueaCarga(await armarVeredicto({ sendtrack: local, storageFalla: true }).veredicto(PV_PROPIA, "diario.com.mx")), true,
    "sin poder leer el envío, el bloqueo queda como estaba");
  const sesion = new Map();
  anotarEnvioDeSesion(sesion, "diario.com.mx", { el: hoyReal(), enSendtrack: true, crm: LIBRE });
  const s = armarVeredicto({ sesion });
  strictEqual((await s.veredicto(PV_PROPIA, "www.diario.com.mx")).fichaPropia, true, "en la misma sesión, al volver al sitio");
  strictEqual(s.lecturas(), 0, "con el envío de la sesión no hace falta leer el storage");
  const libre = armarVeredicto({ sendtrack: local });
  strictEqual((await libre.veredicto({ found: false }, "diario.com.mx")).ok, true);
  strictEqual(libre.lecturas(), 0, "el envío se busca sólo cuando el CRM dice que no con una ficha");
});

test("a: las tres consultas del veredicto de Analysis (arranque, pipeline y Reintentar) usan la misma regla", () => {
  let temprano = null;
  walk.full(ast, (n) => {
    if (temprano || n.type !== "CallExpression" || n.callee.type !== "MemberExpression" || n.callee.property?.name !== "addEventListener") return;
    if (texto(n.callee.object) !== "document" || n.arguments[0]?.value !== "DOMContentLoaded") return;
    if (llamaA(n.arguments[1], "_crmConsultar") && llamaA(n.arguments[1], "_armarWatchdogCrm")) temprano = n.arguments[1];
  });
  ok(temprano, "no encontré el chequeo temprano del CRM");
  for (const [nombre, fn] of [["el chequeo temprano", temprano], ["runDuplicateCheck", funcion("runDuplicateCheck")], ["_reintentarVeredictoCrm", funcion("_reintentarVeredictoCrm")]]) {
    ok(llamaA(fn, "_veredictoAnalisis"), `${nombre} no mira si la ficha la creó nuestro envío`);
    ok(!llamaA(fn, "_veredictoCrm"), `${nombre} calcula el veredicto por su cuenta`);
  }
  const [consulta] = llamadas(temprano, "_veredictoAnalisis");
  const yaPinto = ifCon(temprano, "state.crmVeredicto && !state.crmVeredicto.provisional");
  ok(yaPinto && consulta.nodo.start < yaPinto.start, "la espera del envío va antes del chequeo de 'ya pintó la pipeline': entre medio pisaría ese veredicto");
});

// _validarProspectoMonday ("Guardar para enviar después"), ejecutada con dobles.
function validarGuardar({ dominio, veredicto, envios = new Map() }) {
  const fn = ejecutar({
    _motivoBloqueoCrm: () => "⛔", document: { getElementById: () => null },
    getMondayFormValues: () => ({ email: "ventas@diario.com.mx", geo: "Mexico", idioma: "1", estado: "Propuesta Vigente", fecha: HOY, ejecutivo: "mb", pitch: "" }),
    isValidEmail: () => true, _esFormularioUrl: () => false,
    state: { domain: dominio, traffic: 900000, visits: 0, crmVeredicto: veredicto },
    _enviosDeLaSesion: envios, envioParaCargar, crmBloqueaCarga,
  }, `${texto(funcion("_validarProspectoMonday"))}\nreturn _validarProspectoMonday;`);
  return fn({ textContent: "", className: "" });
}

test("a: la ficha propia se guarda con el envío de verdad, y el lote después la carga", () => {
  const propia = veredictoConEnvioPropio(veredictoCrm(PV_PROPIA), { dup: PV_PROPIA, envio: { el: HOY, enSendtrack: false, crm: LIBRE } });
  const v = validarGuardar({ dominio: "diario.com.mx", veredicto: propia });
  ok(v, "'Guardar en cola' frenaba el sitio al que este MB le escribió");
  deepStrictEqual([v.mailEnviado, v.mailEnviadoEl], [true, HOY], "con la toolbar recién abierta, la fila tiene que decir que el mail ya salió");
  const fila = filaColaDesdeFormulario(v, { prev: null, domain: "diario.com.mx", crmAlGuardar: fotoCrmAlGuardar(propia, PV_PROPIA) });
  deepStrictEqual(fila.monday_payload.crm_al_guardar, LIBRE);
  const contactado = contactadoDeCola(fila, lecturaDeEnvios({ ok: true, dominios: new Set() }, [fila], { ahoraMs: AHORA }));
  strictEqual(contactado, true);
  const dec = decidirLoteCrm({ dup: PV_PROPIA, veredicto: veredictoCrm(PV_PROPIA), alGuardar: fila.monday_payload.crm_al_guardar, contactado });
  deepStrictEqual([dec.enviar, dec.fichaPropia], [true, true], `el lote la salteaba por 'el CRM ya decía que no al guardar': ${dec.motivo}`);
  strictEqual(validarGuardar({ dominio: "diario.com.mx", veredicto: veredictoCrm(PV_AJENA) }), null, "una propuesta ajena sigue frenando 'Guardar'");
});

test("a: el botón de Gmail y la tarjeta guardan qué decía el CRM antes de mandar, junto al envío", async () => {
  const h = handlerDe("btn-send-gmail");
  let foto = null;
  walk.full(h, (n) => {
    if (!foto && n.type === "VariableDeclarator" && n.init && sinEspacios(texto(n.init)) === sinEspacios("fotoCrmAlGuardar(state.crmVeredicto, state.duplicate)")) foto = n;
  });
  ok(foto, "el botón de Gmail no toma la foto del CRM");
  let esperaAntes = false;
  walk.full(h, (n) => { if (n.type === "AwaitExpression" && n.start < foto.start) esperaAntes = true; });
  ok(!esperaAntes, "la foto se toma antes de la primera espera, junto con el sitio del mail");
  const [st] = llamadas(h, "saveSendDate");
  ok(texto(st.nodo.arguments[1]).includes(`crmAlEnviar: ${foto.id.name}`), "la copia local de sendtrack no guarda la foto");
  const [an] = llamadas(h, "anotarEnvioDeSesion");
  ok(texto(an.nodo.arguments[2]).includes(`crm: ${foto.id.name}`), "el envío de la sesión no guarda la foto");

  const t = armarTarjeta();
  await t.correr();
  deepStrictEqual(t.sendtrack[0][1].crmAlEnviar, LIBRE, "la tarjeta no guarda la foto en la copia local");
  deepStrictEqual(envioDeSesion(t.envios, "diario.com.mx").crm, LIBRE, "la tarjeta no guarda la foto en la sesión");
});

// ═══ b — el Guard #3 exige un mail a ESTE sitio ═══════════════════════════════════════════════════
test("b: el botón verde no carga un sitio al que no se le escribió, aunque en la sesión haya salido otro mail", () => {
  const h = handlerDe("btn-push-monday");
  let guard = null;
  walk.full(h, (n) => { if (!guard && n.type === "IfStatement" && /Mandá primero el mail/.test(texto(n.consequent))) guard = n; });
  ok(guard, "no encontré el Guard #3 del botón verde");
  const [carga] = llamadas(h, "enviarAlBoard");
  ok(carga && guard.start < carga.nodo.start, "el Guard #3 tiene que ir antes de cargar");
  const bloquea = new Function("envioParaCargar", "_enviosDeLaSesion", "state", "esFormulario", `return (${texto(guard.test)});`);
  const sesion = new Map();
  anotarEnvioDeSesion(sesion, "a.com", { el: HOY, enSendtrack: true });
  // `emailSentInSession: true` es la bandera vieja, prendida por el mail a A: con ella el guard dejaba pasar B.
  const st = (domain, crmVeredicto = null) => ({ domain, crmVeredicto, emailSentInSession: true });
  strictEqual(bloquea(envioParaCargar, sesion, st("b.com"), false), true,
    "mandarle a A dejaba cargar B sin mail, con mail_ya_enviado=true: el CRM nunca le escribía a B");
  strictEqual(bloquea(envioParaCargar, sesion, st("www.a.com"), false), false, "a A sí se le escribió");
  strictEqual(bloquea(envioParaCargar, sesion, st("b.com"), true), false, "un formulario de contacto no lleva mail");
  const propia = veredictoConEnvioPropio(veredictoCrm(PV_PROPIA), { dup: PV_PROPIA, envio: { el: HOY, crm: LIBRE } });
  strictEqual(bloquea(envioParaCargar, new Map(), st("diario.com.mx", propia), false), false, "(a) la ficha que creó nuestro envío se carga sin repetir el mail");
  strictEqual(bloquea(envioParaCargar, new Map(), st("diario.com.mx", veredictoCrm({ found: false })), false), true, "sin mail a este sitio y sin ficha propia, no");
});

test("b: la bandera de sesión se retiró (nadie la lee ni la escribe) y envioParaCargar es por sitio", () => {
  const usos = [];
  walk.full(ast, (n) => {
    if ((n.type === "MemberExpression" && n.property?.name === "emailSentInSession") || (n.type === "Property" && n.key?.name === "emailSentInSession")) usos.push(n.loc.start.line);
  });
  deepStrictEqual(usos, [], "la bandera volvería a decir 'enviado' para cualquier sitio después del primer mail");
  const sesion = new Map();
  anotarEnvioDeSesion(sesion, "a.com", { el: HOY, enSendtrack: true });
  strictEqual(envioParaCargar(sesion, "b.com", null), null);
  deepStrictEqual(envioParaCargar(sesion, "A.com", null), { el: HOY, enSendtrack: true });
  const propia = veredictoConEnvioPropio(veredictoCrm(PV_PROPIA), { dup: PV_PROPIA, envio: { el: HOY, crm: LIBRE } });
  deepStrictEqual(envioParaCargar(new Map(), "diario.com.mx", propia), { el: HOY, enSendtrack: false });
  strictEqual(envioParaCargar(new Map(), "diario.com.mx", veredictoCrm(PV_PROPIA)), null, "un 'no' del CRM sin envío nuestro no habilita nada");
});
