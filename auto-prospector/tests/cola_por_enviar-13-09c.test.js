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

  const enviada = { id: 1, domain: "Diario.com.mx", monday_payload: { mail_enviado: true, fecha: "2026-09-10" } };
  const otra = { id: 2, domain: "otro.com", monday_payload: { status_previo: "pending" } };
  strictEqual(lecturaDeEnvios({ ok: true, dominios: new Set(["diario.com.mx"]) }, [enviada, otra], op).conocida, true);
  const vacia = lecturaDeEnvios({ ok: true, dominios: new Set() }, [enviada, otra], op);
  strictEqual(vacia.conocida, false, "si la base no muestra un envío que sabemos que salió, tampoco se le puede creer el resto");
  ok(/diario\.com\.mx/.test(vacia.motivo), vacia.motivo);
  const vieja = { ...enviada, monday_payload: { mail_enviado: true, fecha: "2026-07-01" } };
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
    { id: 2, domain: "b.com", source: "autogoogle", traffic: 900000, monday_payload: { status_previo: "pending", mail_enviado: true } },
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
function armarTarjeta({ crmFalla = false, historialFalla = false, colaResponde = null } = {}) {
  const log = [], pedidos = [], fichas = [], toasts = [];
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
    saveSendDate: async () => { log.push("sendtrack"); return { ok: true }; },
    markReviewQueueAsContacted: async () => ({ ok: true }),
    isEmailBounced: async () => ({ bounced: false }),
    adicionalesDeLaTarjeta, contactosDeAdicionales, filaColaDesdeFormulario, fotoCrmAlGuardar,
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
  return { correr: () => validateProspect(card, data, true), card, log, pedidos, fichas, toasts, resultado: () => card.querySelector(".pcard-result").textContent };
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
