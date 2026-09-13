// La cola "Por enviar" y la tarjeta de Prospects, segunda vuelta de la revisión del 13/09.
//
//   B1  El lote "Enviar a ADEQ" salteaba para siempre las filas del flujo normal. Al mandar con
//       adicionales, el worker los despacha y le avisa al CRM (`adicional_enviado`, sólo dominio +
//       contactos): en un dominio nuevo eso crea la ficha con el default de la tabla, "Propuesta
//       Vigente", sin ejecutivo. El lote la leía como "propuesta en curso" y no la cargaba nunca.
//   M1  Si el CRM fallaba después del mail, la tarjeta decía "cargalo desde Analysis", y en Analysis
//       el botón verde exige haber mandado el mail en esa sesión: no había salida sin repetir el
//       mail. Y si lo que fallaba era el historial, el cartel igual culpaba al CRM.
//   M2  La tarjeta encolaba los adicionales ANTES de la ficha: con el CRM caído salían igual y su
//       aviso creaba una ficha incompleta.
//   M3  Sin lectura de sendtrack (un 401/403, o una RLS que le esconde al MB los envíos ajenos),
//       "Quitar" y "Enviar" quedaban inutilizables; y una lectura vacía no se distinguía de "nadie
//       le escribió".
//   M6  Con la base caída el lote seguía fila por fila (300 × 8 s).
//   M8  Dos tests del 13/09 buscaban texto: un comentario los dejaba en verde. Acá se mira el árbol.
//   B2  (tercera revisión) "Guardar" anotaba mail_enviado con una bandera de sesión que nunca vuelve a
//       false: después del primer mail, todo lo guardado quedaba "enviado". Con eso lecturaDeEnvios
//       desconfiaba de todo sendtrack y apagaba el lote, "Quitar" cerraba como contactados sitios a
//       los que nunca se les escribió y el lote tomaba por nuestra una Propuesta Vigente ajena.
//       Ahora el envío se anota por sitio y la fila lo escribe con su sitio (envioAnotado).
//   M9  flushPendingMarks escribía la lista que había leído al empezar: una marca que el lote
//       encolaba mientras tanto se perdía y la fila, ya en el CRM, volvía a verse en "Por enviar".
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
  decidirLoteCrm, fotoCrmAlGuardar, lecturaDeEnvios, contactadoDeCola, estadoAlSacarDeCola, planSacarDeCola,
  textoConfirmarSacar, filaColaDesdeFormulario, adicionalesDeLaTarjeta, contactosDeAdicionales,
  anotarEnvioDeSesion, envioDeSesion, envioAnotado,
} from "../../modules/colaEstado.js";
import { dominiosConEnvioReciente } from "../../modules/supabase.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(aqui, "..", "..");
const popup = fs.readFileSync(path.join(RAIZ, "popup", "popup.js"), "utf8");
const parsear = (src) => acorn.parse(src, { ecmaVersion: "latest", sourceType: "module", locations: true });
const ast = parsear(popup);
const texto = (n) => popup.slice(n.start, n.end);

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
// Cada llamada a `nombre(…)` de un árbol, con la función que la contiene (la más cercana).
function llamadas(arbol, nombre) {
  const out = [];
  walk.fullAncestor(arbol, (n, _st, anc) => {
    if (n.type !== "CallExpression" || n.callee.type !== "Identifier" || n.callee.name !== nombre) return;
    out.push({ nodo: n, fn: [...anc].reverse().find(a => a !== n && /Function/.test(a.type)) });
  });
  return out;
}
const llamaA = (nodo, nombre, cond = () => true) => {
  let si = false;
  walk.full(nodo, (n) => { if (!si && n.type === "CallExpression" && n.callee.type === "Identifier" && n.callee.name === nombre && cond(n)) si = true; });
  return si;
};

// El _veredictoCrm REAL del popup (mismo corte que tests/veredicto-crm.test.js): la regla del lote
// tiene que funcionar con el vocabulario verdadero del CRM, no con uno inventado en el test.
const veredictoCrm = (() => {
  const ini = popup.indexOf("const _CRM_LIVE_RE");
  const fin = popup.indexOf("function _pintarVeredictoCrm");
  ok(ini > 0 && fin > ini, "no encontré _veredictoCrm en popup.js");
  return new Function(popup.slice(ini, fin) + "; return _veredictoCrm;")();
})();
const decidir = (dup, extra = {}) => decidirLoteCrm({ dup, veredicto: veredictoCrm(dup), ...extra });
const LIBRE_AL_GUARDAR = { ok: true, duda: false, found: false, estado: "", ejecutivo: "" };

// ═══ B1 — la ficha que crea nuestro propio envío no bloquea el lote ═════════════════════════
test("B1: los tres casos del reporte, con el veredicto real del CRM", () => {
  const propia = decidir({ found: true, status: "Propuesta Vigente", ejecutivo: "" }, { contactado: true });
  strictEqual(propia.enviar, true, "la ficha 'Propuesta Vigente' sin ejecutivo que dejó el aviso de adicionales se salteaba para siempre");
  strictEqual(propia.fichaPropia, true);

  const live = decidir({ found: true, status: "Live", ejecutivo: "otro@adeqmedia.com" }, { contactado: true, alGuardar: LIBRE_AL_GUARDAR });
  strictEqual(live.enviar, false, "un cliente activo nunca se carga desde el lote");
  ok(/Cliente activo/.test(live.motivo), live.motivo);

  const ajena = decidir({ found: true, status: "Propuesta Vigente", ejecutivo: "otro@adeqmedia.com" }, { contactado: true, alGuardar: LIBRE_AL_GUARDAR });
  strictEqual(ajena.enviar, false, "una propuesta con ejecutivo que apareció después de guardar es de otro");
  ok(/otro@adeqmedia\.com/.test(ajena.motivo), ajena.motivo);
});

test("B1: el resto de los casos — sólo la Propuesta Vigente sin dueño posterior a nuestro envío pasa", () => {
  const casos = [
    // [ficha de hoy, extra, ¿se manda?, por qué]
    [{ found: false }, {}, true, "nunca estuvo en el CRM"],
    [{ found: true, status: "Ciclo Finalizado", ejecutivo: "x@adeqmedia.com" }, {}, true, "ciclo finalizado es prospectable"],
    [{ found: true, status: "Pausado" }, {}, true, "pausado es prospectable"],
    [{ found: true, status: "En Negociacion", ejecutivo: "" }, { contactado: true }, false, "una negociación la pone una persona"],
    [{ found: true, status: "Personalizado", ejecutivo: "" }, { contactado: true }, false, "personalizado dispara el mail del MB"],
    [{ found: false, bloqueado: "TLD vetado" }, { contactado: true }, false, "bloqueado por lista"],
    [{ found: true, status: "Estado Nuevo", ejecutivo: "" }, { contactado: true }, false, "un estado que no se reconoce no se declara libre"],
    [{ indeterminado: true }, { contactado: true }, false, "no pude preguntar nunca es 'libre'"],
    [{ found: true, status: "Propuesta Vigente", ejecutivo: "" }, { contactado: false }, false, "sin envío nuestro, esa ficha no la explica nuestro aviso"],
    [{ found: true, status: "propuesta vigente", ejecutivo: "  " }, { contactado: true, alGuardar: null }, true, "fila de antes del 13/09: sin foto, decide el envío"],
    [{ found: true, status: "Propuesta Vigente", ejecutivo: "" }, { contactado: true, alGuardar: { ...LIBRE_AL_GUARDAR, found: true, estado: "Pausado" } }, false, "la ficha ya existía al guardar: no la creó nuestro aviso"],
    [{ found: true, status: "Propuesta Vigente", ejecutivo: "" }, { contactado: true, alGuardar: { ok: false, duda: false, found: false, estado: "", ejecutivo: "" } }, false, "el CRM ya decía que no al guardar"],
  ];
  for (const [dup, extra, enviar, porque] of casos) {
    const d = decidir(dup, extra);
    strictEqual(d.enviar, enviar, `${porque}: ${JSON.stringify(dup)} → ${JSON.stringify(d)}`);
    if (!enviar) ok(d.motivo, `${porque}: un salteo sin motivo es un salteo en silencio`);
  }
  strictEqual(decidirLoteCrm({ dup: { found: true, status: "Propuesta Vigente" } }).enviar, false, "sin veredicto no se manda");
});

test("B1: 'Guardar' anota la foto del CRM y el lote decide con ella", () => {
  deepStrictEqual(fotoCrmAlGuardar(veredictoCrm({ found: false }), { found: false }), LIBRE_AL_GUARDAR);
  deepStrictEqual(fotoCrmAlGuardar(veredictoCrm({ found: true, status: "Pausado", ejecutivo: "MB@adeqmedia.com " }), { found: true, status: "Pausado", ejecutivo: "MB@adeqmedia.com " }),
    { ok: true, duda: false, found: true, estado: "Pausado", ejecutivo: "mb@adeqmedia.com" });
  strictEqual(fotoCrmAlGuardar(null, null), null, "sin veredicto todavía no se inventa uno");

  const v = { email: "a@s.com", geo: "Mexico", idioma: "1", traffic: 900000, mailEnviado: true };
  deepStrictEqual(filaColaDesdeFormulario(v, { prev: { id: 1, status: "validated", traffic: 900000 }, crmAlGuardar: LIBRE_AL_GUARDAR }).monday_payload.crm_al_guardar, LIBRE_AL_GUARDAR);
  ok(!("crm_al_guardar" in filaColaDesdeFormulario(v, { prev: null }).monday_payload));

  const guardar = handlerDe("btn-guardar-cola");
  ok(llamaA(guardar, "filaColaDesdeFormulario", (n) => {
    const p = n.arguments[1]?.properties?.find(x => x.key?.name === "crmAlGuardar");
    return !!p && texto(p.value) === "fotoCrmAlGuardar(state.crmVeredicto, state.duplicate)";
  }), "'Guardar' tiene que anotar lo que decía el CRM en ese momento");

  const enviar = handlerDe("btn-cola-enviar");
  const dec = llamadas(enviar, "decidirLoteCrm")[0];
  const carga = llamadas(enviar, "enviarAlBoard")[0];
  ok(dec && carga && dec.nodo.start < carga.nodo.start, "el lote tiene que decidir con decidirLoteCrm antes de cargar");
  const props = Object.fromEntries(dec.nodo.arguments[0].properties.map(p => [p.key.name, texto(p.value)]));
  strictEqual(props.alGuardar, "mp.crm_al_guardar || null", "el lote tiene que usar la foto guardada");
  strictEqual(props.contactado, "contactado");
  const mailYa = carga.nodo.arguments[0].properties.find(p => p.key?.name === "mailYaEnviado");
  strictEqual(texto(mailYa.value), "contactado", "mail_ya_enviado sale del mismo dato con el que se decidió");
});

// ═══ M3 — sin una lectura creíble de sendtrack no se adivina, pero los botones siguen andando ═
test("M3: lecturaDeEnvios distingue 'no hay envíos' de 'no pude saber'", () => {
  const ahora = Date.parse("2026-09-13T12:00:00Z");
  const op = { ahoraMs: ahora };
  const falla = lecturaDeEnvios({ ok: false, status: 403, error: "HTTP 403" }, [], op);
  strictEqual(falla.conocida, false);
  ok(/HTTP 403/.test(falla.motivo), falla.motivo);

  // (2026-09-13, B2) "Sabemos que salió" es un envío anotado con su sitio y aceptado por sendtrack al
  // mandarlo; la bandera sola ya no alcanza (ver los tests B2 más abajo).
  const enviada = { id: 1, domain: "Diario.com.mx", monday_payload: {
    mail_enviado: true, mail_enviado_dominio: "diario.com.mx", mail_enviado_el: "2026-09-10", mail_enviado_en_sendtrack: true, fecha: "2026-09-10" } };
  const otra = { id: 2, domain: "otro.com", monday_payload: { status_previo: "pending" } };
  strictEqual(lecturaDeEnvios({ ok: true, dominios: new Set(["diario.com.mx"]) }, [enviada, otra], op).conocida, true);
  const vacia = lecturaDeEnvios({ ok: true, dominios: new Set() }, [enviada, otra], op);
  strictEqual(vacia.conocida, false, "si la base no muestra un envío que sabemos que salió, tampoco se le puede creer el resto");
  ok(/diario\.com\.mx/.test(vacia.motivo), vacia.motivo);
  const vieja = { ...enviada, monday_payload: { ...enviada.monday_payload, mail_enviado_el: "2026-07-01", fecha: "2026-07-01" } };
  strictEqual(lecturaDeEnvios({ ok: true, dominios: new Set() }, [vieja, otra], op).conocida, true, "un envío de hace más de 30 días no tiene por qué estar en la ventana");

  strictEqual(contactadoDeCola(enviada, falla), true, "lo anotado al guardar no depende de sendtrack");
  strictEqual(contactadoDeCola(otra, falla), null);
  strictEqual(contactadoDeCola(otra, { conocida: true, dominios: new Set(["otro.com"]) }), true);
  strictEqual(contactadoDeCola(otra, { conocida: true, dominios: new Set() }), false);
});

test("M3: 'Quitar' sin lectura creíble no devuelve nada al pool, y saca lo que no depende de sendtrack", () => {
  strictEqual(estadoAlSacarDeCola({ status_previo: "pending", contactado_sendtrack: null }).status, null);
  strictEqual(estadoAlSacarDeCola({ status_previo: "pending", contactado_sendtrack: null }).grupo, "sin_confirmar");
  const filas = [
    { id: 1, domain: "a.com", source: "autogoogle", traffic: 900000, monday_payload: { status_previo: "pending" } },
    // (2026-09-13, B2) El envío anotado con su sitio; la bandera sola queda sin confirmar (tests B2).
    { id: 2, domain: "b.com", source: "autogoogle", traffic: 900000, monday_payload: { status_previo: "pending", mail_enviado: true, mail_enviado_dominio: "b.com" } },
    { id: 3, domain: "c.com", source: "csv", traffic: 900000, monday_payload: { status_previo: "rejected" } },
    { id: 4, domain: "d.com", source: "manual_cola", traffic: 900000, monday_payload: {} },
    { id: 5, domain: "e.com", source: "autogoogle", traffic: 120000, monday_payload: { status_previo: "pending" } },
    { id: 6, domain: "f.com", source: "autogoogle", traffic: 900000, monday_payload: { status_previo: "validated" } },
    { id: 7, domain: "g.com", source: "autogoogle", traffic: 900000, monday_payload: { status_previo: "frozen" } },
  ];
  const plan = planSacarDeCola(filas, { contactados: null, minTraffic: 350000, loginEmail: "mb@x.com", ahoraIso: "2026-09-13T10:00:00Z" });
  deepStrictEqual(plan.sinConfirmar, ["1", "5", "7"], "lo que volvería al pool (pending, bajo el piso o restaurado) se queda en la cola");
  const destinos = Object.fromEntries(plan.lotes.flatMap(l => l.ids.map(id => [id, l.body.status])));
  deepStrictEqual(destinos, { 2: "validated", 3: "rejected", 4: "rejected", 6: "validated" });
  for (const l of plan.lotes) ok(l.body.status !== "pending", "ningún PATCH devuelve algo a pending sin saber si se le escribió");
  const conf = textoConfirmarSacar(plan, { minTraffic: 350000, motivoSinConfirmar: "no pude leer los envíos (HTTP 403)" });
  ok(/3 se quedan en la cola/.test(conf) && /HTTP 403/.test(conf), conf);
  // Con lectura buena, nada cambia respecto de la primera vuelta.
  strictEqual(planSacarDeCola(filas, { contactados: new Set(), minTraffic: 350000 }).sinConfirmar.length, 0);
});

test("M3: dominiosConEnvioReciente renueva el token UNA vez ante 401/403 y dice el status si sigue fallando", async () => {
  const antes = globalThis.fetch;
  try {
    const auths = [];
    let renovaciones = 0;
    const respuestas = [{ ok: false, status: 401 }, { ok: true, status: 200, json: async () => [{ domain: "a.com" }] }];
    globalThis.fetch = async (_u, o) => { auths.push(o.headers.Authorization); return respuestas.shift(); };
    const r = await dominiosConEnvioReciente("viejo", ["a.com"], { renovarToken: async () => { renovaciones++; return "nuevo"; } });
    strictEqual(r.ok, true);
    deepStrictEqual([...r.dominios], ["a.com"]);
    deepStrictEqual(auths, ["Bearer viejo", "Bearer nuevo"]);
    strictEqual(renovaciones, 1);

    let pedidos = 0; renovaciones = 0;
    globalThis.fetch = async () => { pedidos++; return { ok: false, status: 403 }; };
    const muchos = Array.from({ length: 250 }, (_, i) => `s${i}.com`);
    const r2 = await dominiosConEnvioReciente("tk", muchos, { renovarToken: async () => { renovaciones++; return "otro"; } });
    deepStrictEqual([r2.ok, r2.status], [false, 403]);
    strictEqual(renovaciones, 1, "no se renueva una vez por lote");
    strictEqual(pedidos, 2, "un 403 que sigue después de renovar corta la consulta");

    pedidos = 0;
    const r3 = await dominiosConEnvioReciente("tk", ["a.com"]);
    deepStrictEqual([r3.ok, r3.status, pedidos], [false, 403, 1], "sin renovarToken no hay reintento");
  } finally { globalThis.fetch = antes; }
});

test("M3: 'Quitar' y 'Enviar' ya no se apagan enteros cuando sendtrack no contesta", () => {
  for (const id of ["btn-cola-enviar", "btn-cola-borrar"]) {
    const h = handlerDe(id);
    const lectura = llamadas(h, "dominiosConEnvioReciente")[0];
    ok(lectura, `#${id} no consulta sendtrack`);
    ok(lectura.nodo.arguments[2]?.properties?.some(p => p.key?.name === "renovarToken"), `#${id}: un token vencido tiene que renovarse`);
    ok(llamaA(h, "lecturaDeEnvios"), `#${id} tiene que evaluar si la lectura es creíble`);
    ok(!/if \(!_env\.ok\)/.test(texto(h)), `#${id} volvió a apagarse entero ante un fallo de sendtrack`);
  }
  const enviar = handlerDe("btn-cola-enviar");
  let saltaSinSaber = false;
  walk.full(enviar, (n) => {
    if (n.type === "IfStatement" && texto(n.test) === "contactado === null") walk.full(n.consequent, (m) => { if (m.type === "ContinueStatement") saltaSinSaber = true; });
  });
  ok(saltaSinSaber, "sin saber si ya se le escribió, el lote saltea la fila (el CRM le mandaría el inicial)");
  ok(/!plan\.lotes\.length/.test(texto(handlerDe("btn-cola-borrar"))), "'Quitar' tiene que avisar cuando todo quedó sin confirmar");
});

// ═══ M6 — con la base caída el lote se corta ════════════════════════════════════════════
test("M6: si la base no confirma que la fila siga en la cola, el lote se frena", () => {
  let condicion = null;
  walk.full(handlerDe("btn-cola-enviar"), (n) => { if (!condicion && n.type === "IfStatement" && texto(n.test) === "sigue === null") condicion = n; });
  ok(condicion, "no encontré el chequeo de 'sigue === null' en el lote");
  let corta = false, sigue = false;
  walk.full(condicion.consequent, (n) => { if (n.type === "BreakStatement") corta = true; if (n.type === "ContinueStatement") sigue = true; });
  ok(corta && !sigue, "con la base caída seguía fila por fila: hasta 300 × 8 s");
});

// ═══ M1 y M2 — la tarjeta, ejecutada con dobles ═════════════════════════════════════════
// validateProspect vive en popup.js (arranca el DOM entero y no se puede importar). Se extrae con
// acorn y se ejecuta con dobles de todo lo que toca: así se prueba el ORDEN real de las llamadas y
// el cartel que ve el MB, no un texto.
function armarTarjeta({ crmFalla = false, historialFalla = false, colaResponde = null, sendtrackFalla = false } = {}) {
  const log = [], pedidos = [], fichas = [], toasts = [];
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
    buscarEnCrm: async () => ({ found: false }),
    _veredictoCrm: veredictoCrm,
    getDailyValidationCount: async () => 0,
    state: { accessToken: "tk", loginEmail: "mb@adeqmedia.com" },
    getSelectedEmail: () => "publicidad@diario.com.mx",
    defaultOwnerForLang: () => "mb", defaultStatusForOwner: () => "Propuesta Vigente",
    LANG_TO_IDX: { es: "1" },
    formatTraffic: (n) => `${Math.round(n / 1000)}K`,
    isValidEmail: (e) => /@/.test(e),
    ensureFreshToken: async () => "tk",
    getGmailSignature: async () => "MB · ADEQ",
    appendClosingIfMissing: (s) => s,
    createManualSendTracking: async () => { log.push("tracking"); return { ok: true, id: 77 }; },
    CONFIG: { SUPABASE_URL: "https://sb", SUPABASE_ANON_KEY: "anon" },
    sendEmail: async ({ to }) => { log.push(`mail:${to}`); return { ok: true }; },
    markManualSendFailed: async () => {},
    incrementUserDailyCounter: async () => {},
    saveSendDate: async () => { log.push("sendtrack"); return sendtrackFalla ? { ok: false, status: 500 } : { ok: true }; },
    markReviewQueueAsContacted: async () => ({ ok: true }),
    isEmailBounced: async () => ({ bounced: false }),
    adicionalesDeLaTarjeta, contactosDeAdicionales, filaColaDesdeFormulario, fotoCrmAlGuardar,
    anotarEnvioDeSesion, _enviosDeLaSesion: envios,
    fetch: async (url, opts = {}) => {
      const tabla = url.split("/rest/v1/")[1].split("?")[0];
      pedidos.push({ url, opts, tabla, cuerpo: opts.body ? JSON.parse(opts.body) : null });
      log.push(`${opts.method || "GET"} ${tabla}`);
      if (tabla === "toolbar_review_queue") {
        return colaResponde ? colaResponde() : { ok: true, status: 200, json: async () => [{ id: 42 }] };
      }
      return { ok: true, status: 201, json: async () => [] };
    },
    AbortSignal,
    enviarAlBoard: async (p) => { log.push("ficha"); fichas.push(p); if (crmFalla) throw new Error("el CRM respondió HTTP 500"); return {}; },
    saveHistory: async () => { log.push("historial"); if (historialFalla) throw new Error("storage lleno"); },
    validateReviewItem: async () => ({ ok: true }),
    queuePendingMark: async (id) => { log.push(`marca:${id}`); },
    refreshProspectsStats: () => {},
    setTimeout: () => {},
    console: { error() {}, warn() {}, log() {} },
    showToast: (m) => { toasts.push(m); },
    window: {},
  };
  const nombres = Object.keys(deps);
  const fuente = `${texto(funcion("_tarjetaAPorEnviar"))}\n${texto(funcion("validateProspect"))}\nreturn validateProspect;`;
  const validateProspect = new Function(...nombres, fuente)(...nombres.map(n => deps[n]));
  const data = { id: 42, domain: "diario.com.mx", traffic: 900000, language: "es", emails: ["publicidad@diario.com.mx", "ventas@diario.com.mx"], email_sources: {}, contact_phone: "" };
  return { correr: () => validateProspect(card, data, true), card, log, pedidos, fichas, toasts, envios, resultado: () => card.querySelector(".pcard-result").textContent };
}

test("M1 y M2: con el CRM caído la tarjeta no programa adicionales y deja el sitio en 'Por enviar'", async () => {
  const t = armarTarjeta({ crmFalla: true });
  await t.correr();
  ok(t.log.includes("mail:publicidad@diario.com.mx") && t.log.includes("ficha"), t.log.join(" → "));
  ok(!t.log.includes("POST toolbar_reengagement_queue"), `con la ficha afuera, los adicionales salían igual: ${t.log.join(" → ")}`);

  const patch = t.pedidos.find(p => p.tabla === "toolbar_review_queue" && p.opts.method === "PATCH");
  ok(patch, "el sitio no quedó en 'Por enviar': no hay forma de cargarlo sin repetir el mail");
  ok(patch.url.includes("id=eq.42&status=in.(pending,validated)&select=id"), patch.url);
  strictEqual(patch.opts.headers.Prefer, "return=representation");
  ok(patch.opts.signal, "sin reloj");
  strictEqual(patch.cuerpo.status, "por_enviar");
  const mp = patch.cuerpo.monday_payload;
  strictEqual(mp.mail_enviado, true, "el lote tiene que avisarle al CRM que el mail ya salió");
  strictEqual(mp.status_previo, "validated", "si lo sacan de la cola, vuelve a contactado");
  strictEqual(mp.email, "publicidad@diario.com.mx");
  deepStrictEqual([mp.fecha, mp.geo_form, mp.idioma, mp.estado, mp.ejecutivo], ["2026-09-13", "Mexico", "1", "Propuesta Vigente", "mb"]);
  deepStrictEqual(mp.crm_al_guardar, LIBRE_AL_GUARDAR, "la foto del CRM es la que tomó la tarjeta antes de mandar");
  deepStrictEqual(mp.contactos, [{ email: "ventas@diario.com.mx", tipo: "adicional", orden: 1 }], "los adicionales van a la ficha sin hora: no salieron");

  const txt = t.resultado();
  ok(/NO lo vuelvas a mandar/.test(txt) && /Por enviar/.test(txt), txt);
  ok(!/cargalo desde Analysis\./.test(txt), `el cartel mandaba a un botón que exige repetir el mail: ${txt}`);
  strictEqual(t.toasts.length, 1, "el refresco saca la tarjeta en segundos: el aviso tiene que quedar a la vista");
  deepStrictEqual(t.card.botones.map(b => b.disabled), [true, false], "el botón de enviar queda apagado");
  ok(/NO programados/.test(t.card.querySelector(".pcard-future-status").textContent));
});

test("M2: con el CRM bien, primero la ficha (con la hora de los adicionales) y después la cola", async () => {
  const t = armarTarjeta();
  await t.correr();
  const iFicha = t.log.indexOf("ficha");
  const iCola = t.log.indexOf("POST toolbar_reengagement_queue");
  ok(iFicha >= 0 && iCola > iFicha, `los adicionales se encolaban antes de que la ficha entrara: ${t.log.join(" → ")}`);
  strictEqual(t.fichas.length, 1);
  deepStrictEqual(t.fichas[0].contactos.map(c => [c.email, typeof c.enviado_at]), [["ventas@diario.com.mx", "string"]],
    "la ficha lleva los contactos con su hora programada (regla del 07/09)");
  const cola = t.pedidos.find(p => p.tabla === "toolbar_reengagement_queue");
  deepStrictEqual(cola.cuerpo.map(f => [f.future_email, f.reason]), [["ventas@diario.com.mx", "adicional_manual"]]);
  ok(!t.pedidos.some(p => p.tabla === "toolbar_review_queue"), "sin falla del CRM no se toca la cola 'Por enviar'");
  ok(/Cargado en ADEQ \+ mail enviado/.test(t.resultado()), t.resultado());
});

test("M1: si falla el historial después de cargar la ficha, el cartel no dice que falló el CRM", async () => {
  const t = armarTarjeta({ historialFalla: true });
  await t.correr();
  const txt = t.resultado();
  ok(/cargado en ADEQ/i.test(txt), txt);
  ok(!/carga al CRM falló/.test(txt), `el MB iba a cargar otra vez algo que ya estaba: ${txt}`);
  ok(t.log.includes("marca:42"), "la marca de contactado se reintenta sola");
  ok(!t.pedidos.some(p => p.tabla === "toolbar_review_queue"), "una ficha que entró no va a 'Por enviar'");
  deepStrictEqual(t.card.botones.map(b => b.disabled), [true, false]);
});

test("M1: si tampoco se puede dejar en 'Por enviar', el cartel ofrece un camino que existe", async () => {
  const t = armarTarjeta({ crmFalla: true, colaResponde: () => ({ ok: true, status: 200, json: async () => [] }) });
  await t.correr();
  const txt = t.resultado();
  ok(/no pude dejarlo en Por enviar \(la fila ya no estaba en Prospects\)/.test(txt), txt);
  ok(/Guardar para enviar después/.test(txt), txt);
  // Y ese camino existe: "Guardar para enviar después" no exige haber mandado el mail en la sesión.
  const validar = funcion("_validarProspectoMonday");
  let exigeMail = false;
  walk.full(validar, (n) => { if (n.type === "IfStatement" && /emailSentInSession/.test(texto(n.test))) exigeMail = true; });
  strictEqual(exigeMail, false, "'Guardar para enviar después' pasó a exigir el mail: el cartel volvería a mentir");
});

// ═══ M8 — los chequeos del 13/09, sobre el árbol y no sobre el texto ═════════════════════
// Un comentario que nombra la función no es una llamada. El detector se prueba primero a sí mismo.
const VEREDICTO = new Set(["_veredictoCrm", "_crmBloquea", "decidirLoteCrm"]);
function miraElVeredictoAntes(fn, hasta) {
  let visto = false;
  walk.full(fn, (n) => {
    if (visto || n.start >= hasta) return;
    if (n.type === "CallExpression" && n.callee.type === "Identifier" && VEREDICTO.has(n.callee.name)) visto = true;
    if (n.type === "MemberExpression" && n.object.type === "Identifier" && n.object.name === "state" && n.property?.name === "crmVeredicto") visto = true;
  });
  return visto;
}

test("M8: el detector no se deja engañar por un comentario", () => {
  const trampa = parsear(`async function f(dup) {\n  // _veredictoCrm(dup) y state.crmVeredicto\n  await enviarAlBoard({ contactos: [] });\n}`);
  const [c] = llamadas(trampa, "enviarAlBoard");
  strictEqual(miraElVeredictoAntes(c.fn, c.nodo.start), false);
  const bien = parsear(`async function f(dup) { const v = _veredictoCrm(dup); if (!v.ok) return; await enviarAlBoard({ contactos: [] }); }`);
  const [c2] = llamadas(bien, "enviarAlBoard");
  strictEqual(miraElVeredictoAntes(c2.fn, c2.nodo.start), true);
});

test("M8: toda carga al CRM mira el veredicto en código, y todo envío de la tarjeta queda registrado en código", () => {
  const cargas = llamadas(ast, "enviarAlBoard");
  ok(cargas.length >= 3, `esperaba el botón verde, la cola y la tarjeta; encontré ${cargas.length}`);
  for (const { nodo, fn } of cargas) {
    ok(miraElVeredictoAntes(fn, nodo.start), `enviarAlBoard en L${nodo.loc.start.line} carga sin mirar el veredicto del CRM`);
  }
  for (const { nodo, fn } of llamadas(ast, "sendEmail")) {
    const antes = llamadas(fn, "createManualSendTracking").some(c => c.nodo.start < nodo.start);
    const despues = llamadas(fn, "saveSendDate").some(c => c.nodo.start > nodo.start);
    ok(antes && despues, `sendEmail en L${nodo.loc.start.line}: tiene que registrar el envío (tracking antes, sendtrack después)`);
  }
});

// ═══ B2 — "mail enviado" es de un sitio, no de la sesión ═══════════════════════════════════
// La repro de la tercera revisión: el MB le manda a A y en la misma sesión guarda B sin escribirle.
// B quedó con mail_enviado=true (la bandera de sesión, que nunca vuelve a false) y sin sitio anotado.
// C y D son filas normales. sendtrack contestó bien y vacío.
const HOY = "2026-09-13";
const AHORA = Date.parse("2026-09-13T12:00:00Z");
const B_VIEJA = { id: 2, domain: "b.com", source: "autogoogle", traffic: 900000, monday_payload: { mail_enviado: true, fecha: HOY, status_previo: "pending" } };
const C_NORMAL = { id: 3, domain: "c.com", source: "autogoogle", traffic: 900000, monday_payload: { mail_enviado: false, fecha: HOY, status_previo: "pending" } };
const D_NORMAL = { id: 4, domain: "d.com", source: "import", traffic: 900000, monday_payload: { mail_enviado: false, fecha: HOY, status_previo: "pending" } };
const leerVacio = (filas) => lecturaDeEnvios({ ok: true, dominios: new Set() }, filas, { ahoraMs: AHORA });

test("B2: una fila con la bandera de sesión vieja no apaga el lote", () => {
  const L = leerVacio([B_VIEJA, C_NORMAL, D_NORMAL]);
  strictEqual(L.conocida, true, `una bandera sin sitio hacía desconfiar de todo sendtrack y saltear C y D: ${L.motivo}`);
  deepStrictEqual([B_VIEJA, C_NORMAL, D_NORMAL].map(f => contactadoDeCola(f, L)), [false, false, false],
    "sendtrack contestó bien y no tiene a ninguno: a ninguno se le escribió");
  strictEqual(envioAnotado(B_VIEJA), false, "la bandera sin sitio no prueba un envío");
});

test("B2: 'Quitar' no cierra como contactada una fila cuyo único respaldo es la bandera", () => {
  const filas = [B_VIEJA, C_NORMAL, D_NORMAL];
  const L = leerVacio(filas);
  const plan = planSacarDeCola(filas, { contactados: L.conocida ? L.dominios : null, minTraffic: 350000, loginEmail: "mb@x.com", ahoraIso: "2026-09-13T12:00:00Z" });
  const destinos = Object.fromEntries(plan.lotes.flatMap(l => l.ids.map(id => [id, l.body.status])));
  deepStrictEqual(destinos, { 2: "pending", 3: "pending", 4: "pending" }, "B pasaba a validated con sello de contactado y salía del pool para siempre");
  deepStrictEqual(plan.sinConfirmar, []);
  // Sin lectura de sendtrack tampoco se cierra: "no se sabe" deja la fila en la cola.
  const sinLectura = planSacarDeCola([B_VIEJA], { contactados: null });
  deepStrictEqual([sinLectura.lotes.length, sinLectura.sinConfirmar], [0, ["2"]]);
  strictEqual(contactadoDeCola(B_VIEJA, { conocida: false, dominios: new Set() }), null, "sin sendtrack, la bandera vieja es 'no se sabe', no 'sí'");
  // Si sendtrack sí la tiene (el mail salió de verdad), se cierra como contactada igual que siempre.
  strictEqual(planSacarDeCola([B_VIEJA], { contactados: new Set(["b.com"]), loginEmail: "mb@x.com", ahoraIso: "x" }).lotes[0].body.status, "validated");
});

test("B2: el lote no toma por nuestra una Propuesta Vigente por la bandera vieja", () => {
  const L = leerVacio([B_VIEJA]);
  const pv = { found: true, status: "Propuesta Vigente", ejecutivo: "" };
  strictEqual(decidir(pv, { contactado: contactadoDeCola(B_VIEJA, L), alGuardar: null }).enviar, false,
    "con la bandera vieja, una propuesta sin ejecutivo pasaba como la ficha de nuestros adicionales");
  const anotada = { domain: "b.com", monday_payload: { mail_enviado: true, mail_enviado_dominio: "b.com" } };
  strictEqual(decidir(pv, { contactado: contactadoDeCola(anotada, L), alGuardar: null }).enviar, true, "B1 sigue igual para un envío anotado con su sitio");
});

test("B2: sólo un envío anotado con su sitio, y aceptado por sendtrack al mandarlo, hace desconfiar de la lectura", () => {
  const mp = { mail_enviado: true, mail_enviado_dominio: "diario.com.mx", mail_enviado_el: "2026-09-12", mail_enviado_en_sendtrack: true };
  const anotada = { id: 1, domain: "diario.com.mx", monday_payload: mp };
  strictEqual(leerVacio([anotada, C_NORMAL]).conocida, false, "un envío que sendtrack aceptó y ya no muestra sí es sospechoso");
  const sinInsert = { ...anotada, monday_payload: { ...mp, mail_enviado_en_sendtrack: false } };
  strictEqual(leerVacio([sinInsert, C_NORMAL]).conocida, true, "si sendtrack no lo aceptó al mandar, que no esté no dice nada de la lectura");
  strictEqual(contactadoDeCola(sinInsert, { conocida: false, dominios: new Set() }), true, "pero el mail salió: sigue contactado");
  strictEqual(envioAnotado({ ...anotada, domain: "otro.com" }), false, "un envío anotado para otro sitio no prueba nada de esta fila");
  strictEqual(envioAnotado({ domain: "WWW.Diario.com.mx", monday_payload: mp }), true);
});

test("B2: 'Guardar' escribe de qué sitio y qué día salió el mail", () => {
  const v = { email: "a@diario.com.mx", geo: "Mexico", idioma: "1", traffic: 900000, mailEnviado: true, mailEnviadoEl: "2026-09-13", mailEnviadoEnSendtrack: true };
  const mp = filaColaDesdeFormulario(v, { prev: null, domain: "WWW.Diario.com.mx" }).monday_payload;
  deepStrictEqual([mp.mail_enviado, mp.mail_enviado_dominio, mp.mail_enviado_el, mp.mail_enviado_en_sendtrack], [true, "diario.com.mx", "2026-09-13", true]);
  strictEqual(envioAnotado({ domain: "diario.com.mx", monday_payload: mp }), true, "la fila que escribe 'Guardar' tiene que servir de prueba");
  const sin = filaColaDesdeFormulario({ ...v, mailEnviado: false }, { prev: { id: 1, status: "pending", traffic: 900000 }, domain: "diario.com.mx" }).monday_payload;
  deepStrictEqual([sin.mail_enviado, "mail_enviado_dominio" in sin], [false, false]);
  strictEqual(filaColaDesdeFormulario({ ...v, mailEnviadoEl: "13/09/2026" }, { domain: "diario.com.mx" }).monday_payload.mail_enviado_el, "",
    "una fecha que no es ISO no entra a la ventana de sendtrack");
});

test("B2: los envíos de la sesión se anotan por sitio", () => {
  const envios = new Map();
  anotarEnvioDeSesion(envios, "WWW.A.com", { el: "2026-09-13", enSendtrack: true });
  anotarEnvioDeSesion(envios, "b.com", { el: "2026-09-13", enSendtrack: false });
  deepStrictEqual(envioDeSesion(envios, "a.com"), { el: "2026-09-13", enSendtrack: true });
  deepStrictEqual(envioDeSesion(envios, "B.com"), { el: "2026-09-13", enSendtrack: false });
  strictEqual(envioDeSesion(envios, "c.com"), null, "a C no se le escribió: guardar C no dice 'enviado'");
  anotarEnvioDeSesion(envios, "a.com", { el: "2026-09-14", enSendtrack: false });
  deepStrictEqual(envioDeSesion(envios, "a.com"), { el: "2026-09-14", enSendtrack: true }, "si sendtrack aceptó un envío a ese sitio, lo sigue teniendo");
  anotarEnvioDeSesion(envios, "", {});
  anotarEnvioDeSesion(null, "x.com", {});
  strictEqual(envios.size, 2);
  strictEqual(envioDeSesion(null, "a.com"), null);
});

// _validarProspectoMonday, ejecutado con dobles en una sesión en la que ya salió un mail (la bandera
// vieja está prendida): el valor de mailEnviado tiene que depender de state.domain.
function validarGuardar({ dominio, envios }) {
  const deps = {
    _crmBloquea: () => false, _motivoBloqueoCrm: () => "",
    document: { getElementById: () => null },
    getMondayFormValues: () => ({ email: "ventas@sitio.com", geo: "Mexico", idioma: "1", estado: "Propuesta Vigente", fecha: HOY, ejecutivo: "mb", pitch: "" }),
    isValidEmail: () => true, _esFormularioUrl: () => false,
    state: { domain: dominio, traffic: 900000, visits: 0, emailSentInSession: true },
    _enviosDeLaSesion: envios, envioDeSesion,
  };
  const nombres = Object.keys(deps);
  const fn = new Function(...nombres, `${texto(funcion("_validarProspectoMonday"))}\nreturn _validarProspectoMonday;`)(...nombres.map(n => deps[n]));
  return fn({ textContent: "", className: "" });
}

test("B2: 'Guardar para enviar después' dice 'enviado' sólo para el sitio al que le salió el mail", () => {
  const envios = new Map();
  anotarEnvioDeSesion(envios, "a.com", { el: HOY, enSendtrack: true });
  const b = validarGuardar({ dominio: "b.com", envios });
  strictEqual(b.mailEnviado, false, "mandarle a A dejaba 'enviado' todo lo que se guardaba después en la sesión");
  const a = validarGuardar({ dominio: "www.a.com", envios });
  deepStrictEqual([a.mailEnviado, a.mailEnviadoEl, a.mailEnviadoEnSendtrack], [true, HOY, true]);
  strictEqual(envioAnotado({ domain: "b.com", monday_payload: filaColaDesdeFormulario(b, { domain: "b.com" }).monday_payload }), false);
  strictEqual(envioAnotado({ domain: "a.com", monday_payload: filaColaDesdeFormulario(a, { domain: "a.com" }).monday_payload }), true);
});

test("B2: el botón de Gmail anota el envío para el sitio tomado antes de mandar, y el Guard #3 no cambia", () => {
  const h = handlerDe("btn-send-gmail");
  let decl = null;
  walk.full(h, (n) => { if (!decl && n.type === "VariableDeclarator" && n.init?.type === "MemberExpression" && texto(n.init) === "state.domain") decl = n; });
  ok(decl, "el handler tiene que tomar el sitio del mail en una constante");
  let esperaAntes = false;
  walk.full(h, (n) => { if (n.type === "AwaitExpression" && n.start < decl.start) esperaAntes = true; });
  ok(!esperaAntes, "el sitio se toma antes de la primera espera: después el panel puede estar en otro dominio");
  const sitio = decl.id.name;
  const [envio] = llamadas(h, "sendEmail");
  const anota = llamadas(h, "anotarEnvioDeSesion");
  strictEqual(anota.length, 1, "el envío de Analysis tiene que quedar anotado por sitio");
  ok(anota[0].nodo.start > envio.nodo.start, "se anota después de que el mail salió");
  deepStrictEqual(anota[0].nodo.arguments.slice(0, 2).map(texto), ["_enviosDeLaSesion", sitio]);
  strictEqual(texto(llamadas(h, "saveSendDate")[0].nodo.arguments[0]), sitio, "sendtrack y la anotación tienen que hablar del mismo sitio");
  const enSendtrack = anota[0].nodo.arguments[2]?.properties?.find(p => p.key?.name === "enSendtrack");
  ok(enSendtrack && /\.ok === true/.test(texto(enSendtrack.value)), "si sendtrack no aceptó el envío, no se anota como aceptado");
  ok(/state\.emailSentInSession = true/.test(texto(h)), "el Guard #3 del botón verde no se toca en este arreglo");
});

test("B2: la tarjeta anota su envío por sitio y lo deja escrito en 'Por enviar' cuando el CRM falla", async () => {
  const t = armarTarjeta({ crmFalla: true });
  await t.correr();
  const envio = envioDeSesion(t.envios, "diario.com.mx");
  ok(envio && envio.enSendtrack === true && /^\d{4}-\d{2}-\d{2}$/.test(envio.el), JSON.stringify(envio));
  const patch = (x) => x.pedidos.find(p => p.tabla === "toolbar_review_queue" && p.opts.method === "PATCH").cuerpo.monday_payload;
  const mp = patch(t);
  deepStrictEqual([mp.mail_enviado_dominio, mp.mail_enviado_el, mp.mail_enviado_en_sendtrack], ["diario.com.mx", envio.el, true]);
  strictEqual(envioAnotado({ domain: "diario.com.mx", monday_payload: mp }), true, "el lote tiene que poder cargarla aunque sendtrack no conteste");
  const t2 = armarTarjeta({ crmFalla: true, sendtrackFalla: true });
  await t2.correr();
  strictEqual(patch(t2).mail_enviado_en_sendtrack, false, "si sendtrack rechazó el envío, la fila no lo afirma");
});

// ═══ M9 — las marcas pendientes no se pisan entre sí ═══════════════════════════════════════
// queuePendingMark y flushPendingMarks, extraídas del popup y ejecutadas con un chrome.storage falso.
function armarMarcas(validar) {
  const almacen = {};
  const copia = (x) => (x === undefined ? undefined : JSON.parse(JSON.stringify(x)));
  const deps = {
    chrome: { storage: { local: {
      get: async (k) => ({ [k]: copia(almacen[k]) }),
      set: async (o) => { for (const [k, v] of Object.entries(o)) almacen[k] = copia(v); },
    } } },
    ensureFreshToken: async () => "tk",
    state: { loginEmail: "mb@adeqmedia.com" },
    console: { log() {}, warn() {} },
    validateReviewItem: (...a) => validar(...a),
  };
  const ini = popup.indexOf("const PENDING_MARKS_KEY");
  const fin = funcion("flushPendingMarks").end;
  ok(ini > 0 && fin > ini, "no encontré las marcas pendientes en popup.js");
  const nombres = Object.keys(deps);
  const api = new Function(...nombres, `${popup.slice(ini, fin)}\nreturn { PENDING_MARKS_KEY, queuePendingMark, flushPendingMarks };`)(...nombres.map(n => deps[n]));
  return { ...api, almacen };
}

test("M9: una marca que el lote encola mientras se reintentan las otras no se pierde", async () => {
  let m = null;
  m = armarMarcas(async (_tk, id) => {
    // El lote de la cola, a mitad del reintento: una fila entró al CRM y su marca falló.
    if (String(id) === "1") await m.queuePendingMark(99, "mb@adeqmedia.com");
    return String(id) === "1" ? { ok: true } : { ok: false, error: "HTTP 500" };
  });
  const ahora = Date.now();
  m.almacen[m.PENDING_MARKS_KEY] = [{ id: 1, by: "mb", at: ahora }, { id: 2, by: "mb", at: ahora }, { id: 3, by: "mb", at: ahora - 8 * 86_400_000 }];
  await m.flushPendingMarks();
  deepStrictEqual(m.almacen[m.PENDING_MARKS_KEY].map(x => x.id), [2, 99],
    "la 1 entró, la 2 se reintenta, la 3 lleva más de 7 días; la 99 se perdía y su fila volvía a verse en 'Por enviar'");
});
