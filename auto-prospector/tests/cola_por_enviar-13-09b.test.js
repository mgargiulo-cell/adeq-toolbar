// La cola "Por enviar" y los envíos a mano desde la tarjeta de Prospects. (2026-09-13)
//
// La auditoría del 13/09 encontró siete agujeros en las puertas manuales al CRM. Todos tenían la
// misma forma: la regla vivía adentro de un handler de popup.js, donde ningún test llega.
//   C44  La tarjeta mandaba el mail sin registrarlo: los rebotes de Exchange se descartaban y el
//        candado de 30 días no lo veía. Los adicionales salían los cuatro juntos.
//   C45  "Guardar" pisaba el estado y "Quitar" lo dejaba en pending: contactados, descartados y
//        sitios sin filtro volvían al pool como nuevos.
//   C46  El lote de la cola no preguntaba al CRM: podía empujar a un cliente activo.
//   C47  "Guardar" escribía idioma "1" y país "España" sobre filas que usan ISO e inglés.
//   C48  "Guardar" reemplazaba la lista de emails por uno solo (o por nada, o por una URL).
//   C50  Los PATCH de la cola no miraban la respuesta: un 401 contaba como hecho.
//   C52  "Quitar" prometía "no se borran" y lo de menos de 350K se borraba a los 15 minutos.
//
// Las reglas ahora son funciones puras en modules/colaEstado.js (se prueban directo). Lo que
// sigue en popup.js se prueba por su árbol sintáctico (acorn), no por texto suelto: así un
// comentario que viaja no puede dejar un test en verde sin el código.
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
  IDIOMA_DE_INDICE_MONDAY, idiomaIsoDelFormulario, isoDeEtiquetaMonday, fusionarEmailsCola, emailDeCola,
  statusPrevioAlGuardar, avisoAlGuardarEnCola, filaColaDesdeFormulario, estadoAlSacarDeCola, planSacarDeCola,
  contarGrupos, textoConfirmarSacar, textoResultadoSacar, adicionalesDeLaTarjeta, contactosDeAdicionales,
} from "../../modules/colaEstado.js";
import { createManualSendTracking, dominiosConEnvioReciente } from "../../modules/supabase.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(aqui, "..", "..");
const popup = fs.readFileSync(path.join(RAIZ, "popup", "popup.js"), "utf8");
const html = fs.readFileSync(path.join(RAIZ, "popup", "popup.html"), "utf8");
const ast = acorn.parse(popup, { ecmaVersion: "latest", sourceType: "module", locations: true });
const texto = (n) => popup.slice(n.start, n.end);
const MIN = 350000;

// El handler de `document.getElementById(id)…addEventListener("click", fn)`.
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
// Cada llamada a `nombre(…)` con la función que la contiene (la más cercana).
function llamadas(nombre) {
  const out = [];
  walk.fullAncestor(ast, (n, _st, anc) => {
    if (n.type !== "CallExpression" || n.callee.type !== "Identifier" || n.callee.name !== nombre) return;
    const fn = [...anc].reverse().find(a => a !== n && /Function/.test(a.type));
    out.push({ nodo: n, fn });
  });
  return out;
}
const propiedades = (obj) => (obj?.type === "ObjectExpression" ? obj.properties : [])
  .filter(p => p.type === "Property").map(p => p.key.name || p.key.value);

// Literales de popup.js que las reglas tienen que respetar.
const literal = (re) => { const m = re.exec(popup); ok(m, `no encontré ${re} en popup.js`); return new Function(`return ${m[1]};`)(); };
const MONDAY_COUNTRIES = literal(/const MONDAY_COUNTRIES = (\[[\s\S]*?\]);/);
const GEO_LABEL = literal(/const GEO_LABEL = (\{[\s\S]*?\});/);
const LANG_TO_IDX = literal(/const LANG_TO_IDX\s*=\s*(\{[^}]*\});/);

const conFetchFalso = async (falso, fn) => {
  const antes = globalThis.fetch;
  globalThis.fetch = falso;
  try { return await fn(); } finally { globalThis.fetch = antes; }
};

// ═══ C44 — el envío desde la tarjeta queda registrado ═══════════════════════════════════
test("C44: toda función del popup que manda con sendEmail registra el envío: tracking antes, sendtrack después", () => {
  const envios = llamadas("sendEmail");
  ok(envios.length >= 2, `esperaba al menos Analysis y la tarjeta, encontré ${envios.length}`);
  for (const { nodo, fn } of envios) {
    const cuerpo = texto(fn);
    const donde = `${fn.id?.name || "handler"} (L${nodo.loc.start.line})`;
    const iTrack = cuerpo.indexOf("createManualSendTracking(");
    const iEnvio = nodo.start - fn.start;
    const iSend = cuerpo.indexOf("saveSendDate(", iEnvio);
    ok(iTrack >= 0 && iTrack < iEnvio, `${donde}: manda sin crear antes la fila de agent_actions (el lector de rebotes no la reconoce)`);
    ok(iSend > iEnvio, `${donde}: manda sin escribir sendtrack después (el candado de 30 días no lo ve)`);
    ok(/markManualSendFailed\(/.test(cuerpo), `${donde}: si Gmail rechaza, la fila de tracking queda diciendo "sent"`);
  }
});

test("C44: la tarjeta manda UN mail desde el popup; los adicionales van a la cola del worker, sin el píxel del principal", () => {
  const vp = texto(funcion("validateProspect"));
  strictEqual((vp.match(/\bsendEmail\(/g) || []).length, 1, "los adicionales volvieron a salir desde el popup, los cuatro juntos");
  ok(!/toolbar_response_tracking/.test(vp), "el worker ya escribe response_tracking para adicional_manual: escribirla acá cuenta doble");
  ok(/toolbar_reengagement_queue/.test(vp), "los adicionales tienen que ir a toolbar_reengagement_queue, como en Analysis");
  ok(/adicionalesDeLaTarjeta\(\{[\s\S]{0,200}?body: fullBody/.test(vp), "el cuerpo de los adicionales es fullBody, no el que lleva el píxel del principal");
  ok(/toolbar_reengagement_queue[\s\S]{0,400}?signal: AbortSignal\.timeout\(/.test(vp), "el POST a la cola de adicionales va con reloj");
  const cierre = /catch \(err\) \{[\s\S]*$/.exec(vp)[0];
  ok(/mailSalio/.test(cierre) && /pcard-validate-expanded/.test(cierre),
     "si el CRM falla después de mandar el mail, el botón de enviar no puede volver a habilitarse (reenvío al mismo contacto)");
});

test("C44: adicionalesDeLaTarjeta arma la cola de a uno por minuto, sin repetir, sin rebotados", () => {
  const ahora = Date.parse("2026-09-13T10:00:00Z");
  const { filas, avisos } = adicionalesDeLaTarjeta({
    domain: "diario.com.mx", mbEmail: "MB@adeqmedia.com", principal: "Publicidad@diario.com.mx",
    candidatos: ["publicidad@diario.com.mx", " Ventas@diario.com.mx ", "ventas@diario.com.mx", "muerto@diario.com.mx", "", "editor@diario.com.mx"],
    rebotados: new Set(["muerto@diario.com.mx"]), subject: "Hola", body: "cuerpo con firma", ahoraMs: ahora,
  });
  deepStrictEqual(filas.map(f => f.future_email), ["ventas@diario.com.mx", "editor@diario.com.mx"]);
  deepStrictEqual(filas.map(f => f.scheduled_for), ["2026-09-13T10:01:00.000Z", "2026-09-13T10:02:00.000Z"]);
  for (const f of filas) {
    strictEqual(f.reason, "adicional_manual");
    strictEqual(f.tracking_action_id, null, "con tracking_action_id el worker saltea el adicional si el principal se abrió");
    strictEqual(f.original_body, "cuerpo con firma");
    strictEqual(f.original_email, "publicidad@diario.com.mx");
    strictEqual(f.mb_email, "mb@adeqmedia.com");
  }
  strictEqual(avisos.length, 3, avisos.join(" | "));
  deepStrictEqual(contactosDeAdicionales(filas).map(c => [c.email, c.orden, c.enviado_at]),
    [["ventas@diario.com.mx", 1, "2026-09-13T10:01:00.000Z"], ["editor@diario.com.mx", 2, "2026-09-13T10:02:00.000Z"]]);
  ok(contactosDeAdicionales(filas, { programados: false }).every(c => !("enviado_at" in c)), "si la cola falló no se afirma que el mail salió");
});

test("C44 y C46: toda llamada a enviarAlBoard fuera del botón verde lleva los contactos de SU sitio", () => {
  const cargas = llamadas("enviarAlBoard");
  const conPlantilla = cargas.filter(c => propiedades(c.nodo.arguments[0]).includes("plantilla"));
  strictEqual(conPlantilla.length, 1, "sólo el botón verde de Analysis manda la plantilla y lee los adicionales de su pantalla");
  const resto = cargas.filter(c => !conPlantilla.includes(c));
  ok(resto.length >= 2, "esperaba al menos la cola y la tarjeta");
  for (const { nodo } of resto) {
    ok(propiedades(nodo.arguments[0]).includes("contactos"),
       `enviarAlBoard en L${nodo.loc.start.line} no pasa contactos: lee los de la pantalla de Analysis, que es otro dominio`);
  }
});

test("C44: createManualSendTracking guarda la fuente como texto aunque llegue el objeto {source, url}", async () => {
  const cuerpos = [];
  const falso = async (_url, opts) => { cuerpos.push(JSON.parse(opts.body)); return { ok: true, json: async () => [{ id: 7 }] }; };
  await conFetchFalso(falso, async () => {
    const r = await createManualSendTracking("tk", { user_email: "MB@x.com", domain: "Sitio.com", email_to: "a@sitio.com", email_source: { source: "Apollo", url: "https://x" } });
    deepStrictEqual(r, { ok: true, id: 7 });
    await createManualSendTracking("tk", { user_email: "mb@x.com", domain: "sitio.com", email_source: "Scrape" });
    await createManualSendTracking("tk", { user_email: "mb@x.com", domain: "sitio.com" });
  });
  deepStrictEqual(cuerpos.map(c => c.details.source), ["apollo", "scrape", "manual"]);
  strictEqual(cuerpos[0].action, "sent");
});

// ═══ C45 — cada fila vuelve a donde estaba ══════════════════════════════════════════════
test("C45: estadoAlSacarDeCola devuelve cada fila a su estado anterior", () => {
  const casos = [
    [{ status_previo: "pending", mail_enviado: true, traffic: 900000 }, "validated", "contactado"],
    [{ status_previo: "validated", traffic: 900000 }, "validated", "contactado"],
    [{ status_previo: "rejected", traffic: 900000 }, "rejected", "descartado"],
    [{ status_previo: "pending", traffic: 900000 }, "pending", "prospects"],
    [{ status_previo: null, source: "manual_cola", traffic: 900000 }, "rejected", "sin_filtro"],
    [{ status_previo: null, source: "autogoogle", traffic: 900000 }, "pending", "prospects"],
    // Los dos agregados: el mail salió después de guardar (sólo sendtrack lo sabe) y un descartado al que igual se le escribió.
    [{ status_previo: "pending", mail_enviado: false, contactado_sendtrack: true, traffic: 900000 }, "validated", "contactado"],
    [{ status_previo: "rejected", mail_enviado: true, traffic: 900000 }, "validated", "contactado"],
  ];
  for (const [entrada, status, grupo] of casos) {
    const d = estadoAlSacarDeCola({ ...entrada, minTraffic: MIN });
    strictEqual(d.status, status, JSON.stringify(entrada));
    strictEqual(d.grupo, grupo, JSON.stringify(entrada));
  }
  const sinFiltro = estadoAlSacarDeCola({ source: "manual_cola" });
  strictEqual(sinFiltro.suspect_reject, true);
  // (2026-09-13) Era `mb: `. Ese prefijo es el de un rechazo a mano y el worker lo conserva al reactivar:
  // si un feeder traía el sitio y pasaba el filtro, volvía con suspect_reject=true y el agente no lo
  // tomaba. `cola:` es una marca automática (tests/estados_cola-13-09c.test.js ata las dos puntas).
  strictEqual(sinFiltro.suspect_reason, "cola: sacada_sin_filtro", "no es un juicio del MB: si después pasa el filtro, vuelve limpio");
  strictEqual(estadoAlSacarDeCola({ status_previo: "rejected" }).suspect_reject, undefined, "restaurar un descartado no toca su motivo original");
  strictEqual(estadoAlSacarDeCola({ status_previo: "validated", mail_enviado: true }).sello, false, "un validated viejo conserva quién y cuándo");
  strictEqual(estadoAlSacarDeCola({ status_previo: "pending", mail_enviado: true }).sello, true);
});

test("C45: el estado anterior se anota al guardar y se hereda si se guarda dos veces", () => {
  strictEqual(statusPrevioAlGuardar(null), null);
  strictEqual(statusPrevioAlGuardar({ status: "validated" }), "validated");
  strictEqual(statusPrevioAlGuardar({ status: "por_enviar", monday_payload: { status_previo: "rejected" } }), "rejected");
  strictEqual(statusPrevioAlGuardar({ status: "por_enviar", monday_payload: {} }), null, "una fila vieja en la cola no tiene estado anterior: no se inventa");
  const v = { email: "", geo: "España", idioma: "1", traffic: 500000, mailEnviado: false };
  strictEqual(filaColaDesdeFormulario(v, { prev: { id: 1, status: "validated", traffic: 800000 } }).monday_payload.status_previo, "validated");
  strictEqual(filaColaDesdeFormulario(v, { prev: { id: 1, status: "por_enviar", traffic: 800000, monday_payload: { status_previo: "pending" } } }).monday_payload.status_previo, "pending");
  ok(!("status_previo" in filaColaDesdeFormulario(v, { prev: null }).monday_payload), "una fila nueva queda marcada como manual_cola, sin estado anterior");
});

test("C45: el cartel 'YA figura contactado' no salta por el contacto que el MB acaba de hacer", () => {
  const ahora = Date.parse("2026-09-13T15:00:00Z");
  const op = { loginEmail: "mb@adeqmedia.com", ahoraMs: ahora };
  strictEqual(avisoAlGuardarEnCola(null, op), "nuevo");
  strictEqual(avisoAlGuardarEnCola({ status: "validated", validated_by: "MB@adeqmedia.com", validated_at: "2026-09-13T14:59:00Z" }, op), "contactado_por_vos");
  strictEqual(avisoAlGuardarEnCola({ status: "validated", validated_by: "otro@adeqmedia.com", validated_at: "2026-09-13T14:59:00Z" }, op), "contactado");
  strictEqual(avisoAlGuardarEnCola({ status: "validated", validated_by: "mb@adeqmedia.com", validated_at: "2026-09-11T10:00:00Z" }, op), "contactado");
  strictEqual(avisoAlGuardarEnCola({ status: "pending", monday_item_id: "123" }, op), "contactado");
  strictEqual(avisoAlGuardarEnCola({ status: "rejected", suspect_reason: "purge: gobierno" }, op), "descartado");
  strictEqual(avisoAlGuardarEnCola({ status: "por_enviar", monday_payload: { status_previo: "rejected" } }, op), "descartado");
  strictEqual(avisoAlGuardarEnCola({ status: "por_enviar", monday_payload: {} }, op), "ya_en_cola");
  strictEqual(avisoAlGuardarEnCola({ status: "pending" }, op), "en_pool");
});

test("C45 y C52: 'Quitar' arma un PATCH por destino y el cartel dice la verdad antes de confirmar", () => {
  const filas = [
    { id: 1, domain: "a.com", source: "autogoogle", traffic: 900000, monday_payload: { status_previo: "pending" } },
    { id: 2, domain: "B.com", source: "autogoogle", traffic: 900000, monday_payload: { status_previo: "pending", mail_enviado: false } },
    { id: 3, domain: "c.com", source: "csv", traffic: 900000, monday_payload: { status_previo: "rejected" } },
    { id: 4, domain: "d.com", source: "manual_cola", traffic: 900000, monday_payload: {} },
    { id: 5, domain: "e.com", source: "autogoogle", traffic: 120000, monday_payload: { status_previo: "pending" } },
    { id: 6, domain: "f.com", source: "autogoogle", traffic: 900000, monday_payload: { status_previo: "validated" } },
  ];
  const plan = planSacarDeCola(filas, { contactados: new Set(["b.com"]), minTraffic: MIN, loginEmail: "mb@x.com", ahoraIso: "2026-09-13T10:00:00Z" });
  const porIds = Object.fromEntries(plan.lotes.map(l => [l.ids.join(","), l.body]));
  deepStrictEqual(porIds["1"], { status: "pending" });
  // (2026-09-13) Iba a pending "porque cleanup_pool la borraba". La limpieza ya no borra: rechaza con
  // rejected_at, y el parte la contaba como purga del pool. Ahora se descarta en el mismo PATCH, con el
  // motivo de la limpieza y sin rejected_at.
  deepStrictEqual(porIds["5"], { status: "rejected", suspect_reject: true, suspect_reason: "cleanup: trafico_bajo" }, "menos de 350K: descartada en el mismo PATCH");
  deepStrictEqual(porIds["2"], { status: "validated", validated_by: "mb@x.com", validated_at: "2026-09-13T10:00:00Z" });
  deepStrictEqual(porIds["3"], { status: "rejected" });
  deepStrictEqual(porIds["4"], { status: "rejected", suspect_reject: true, suspect_reason: "cola: sacada_sin_filtro" });
  deepStrictEqual(porIds["6"], { status: "validated" });
  for (const l of plan.lotes) ok(!("rejected_at" in l.body), "rejected_at haría que el parte cuente como purga lo que sacó un MB de su cola");
  deepStrictEqual(contarGrupos(["1", "2", "3", "4", "5", "6"], plan.grupoPorId),
    { prospects: 1, bajo_piso: 1, contactado: 2, descartado: 1, sin_filtro: 1, restaurado: 0 });
  const conf = textoConfirmarSacar(plan, { minTraffic: MIN });
  ok(!/no se borran/i.test(conf), conf);
  ok(/1 vuelven a Prospects/.test(conf) && /2 ya estaban contactados/.test(conf) && /menos de 350K y se descartan/.test(conf), conf);
  ok(!/se eliminan|se borran/i.test(conf), `nada se borra: quedan descartados y se pueden revertir. ${conf}`);
  const fin = textoResultadoSacar(["1", "3"], 6, plan.grupoPorId, { minTraffic: MIN, fallas: ["4 no se pudieron sacar (HTTP 401)"] });
  ok(/Se sacaron 2 de 6/.test(fin) && /HTTP 401/.test(fin), fin);
});

test("C45: dominiosConEnvioReciente mira 30 días de sendtrack y un fallo nunca es 'no hay envíos'", async () => {
  const urls = [];
  const ahora = Date.parse("2026-09-13T12:00:00Z");
  const r = await conFetchFalso(async (url, opts) => {
    urls.push(url); ok(opts.signal, "sin reloj");
    return { ok: true, status: 200, json: async () => [{ domain: "A.com" }] };
  }, () => dominiosConEnvioReciente("tk", ["a.com", "A.com", "b.com"], { ahoraMs: ahora }));
  strictEqual(r.ok, true);
  deepStrictEqual([...r.dominios], ["a.com"]);
  strictEqual(urls.length, 1);
  ok(urls[0].includes("send_date=gte.2026-08-14"), urls[0]);
  ok(decodeURIComponent(urls[0]).includes('domain=in.("a.com","b.com")'), urls[0]);

  const muchos = Array.from({ length: 250 }, (_, i) => `s${i}.com`);
  let pedidos = 0;
  await conFetchFalso(async () => { pedidos++; return { ok: true, json: async () => [] }; }, () => dominiosConEnvioReciente("tk", muchos));
  strictEqual(pedidos, 3, "de a 100 dominios por consulta, para no pasarse del largo de URL");

  strictEqual((await conFetchFalso(async () => ({ ok: false, status: 500 }), () => dominiosConEnvioReciente("tk", ["a.com"]))).ok, false);
  strictEqual((await conFetchFalso(async () => ({ ok: true, json: async () => ({ message: "x" }) }), () => dominiosConEnvioReciente("tk", ["a.com"]))).ok, false);
  strictEqual((await conFetchFalso(async () => { throw new Error("red"); }, () => dominiosConEnvioReciente("tk", ["a.com"]))).ok, false);
  let llamado = false;
  const vacio = await conFetchFalso(async () => { llamado = true; return { ok: true, json: async () => [] }; }, () => dominiosConEnvioReciente("tk", []));
  strictEqual(vacio.ok, true); strictEqual(llamado, false);
});

test("C45 y C52: los botones de la cola usan las reglas y ya no prometen lo que no pasa", () => {
  const sacar = texto(handlerDe("btn-cola-borrar"));
  ok(/planSacarDeCola\(/.test(sacar) && /textoConfirmarSacar\(/.test(sacar), "'Quitar' decide con planSacarDeCola y lo dice antes de confirmar");
  ok(/dominiosConEnvioReciente\(/.test(sacar), "'Quitar' tiene que saber si el mail salió después de guardar");
  ok(/CONFIG\.MIN_TRAFFIC/.test(sacar), "el piso sale de CONFIG, no de un número copiado");
  ok(!/status:\s*"pending"/.test(sacar) && !/no se borran/i.test(sacar), "'Quitar' volvió a mandar todo a pending");
  // (2026-09-13, menor 3) Sin la consulta a sendtrack 'Quitar' sigue sin adivinar, pero ya no se
  // apaga entero: lo que volvería al pool queda en la cola (planSacarDeCola con contactados null).
  ok(/lecturaDeEnvios\(_env, filas\)/.test(sacar) && /contactados: _lectura\.conocida \? _lectura\.dominios : null/.test(sacar),
     "sin la consulta a sendtrack, 'Quitar' no adivina");
  ok(!/no los borra de Prospects/.test(html), "el title del botón Quitar prometía algo falso");

  const guardar = texto(handlerDe("btn-guardar-cola"));
  const select = /toolbar_review_queue\?domain=eq\.[^`]*select=([^`&]+)/.exec(guardar);
  ok(select, "no encontré la lectura previa de 'Guardar'");
  for (const col of ["status", "monday_item_id", "monday_payload", "validated_by", "validated_at", "suspect_reason", "traffic", "emails", "email_sources"]) {
    ok(select[1].split(",").includes(col), `la lectura previa de 'Guardar' no trae ${col}`);
  }
  ok(/filaColaDesdeFormulario\(/.test(guardar) && /avisoAlGuardarEnCola\(/.test(guardar), "'Guardar' arma la fila con la regla compartida");
  ok(!/\.catch\(\(\) => \[\]\)/.test(guardar), "una lectura fallida se trataba como 'no existe'");

  const cargar = texto(funcion("_colaCargar"));
  ok(/select=[^`]*\bsource\b/.test(cargar), "_colaCargar no trae source: 'Quitar' no puede reconocer una fila manual_cola");
});

// ═══ C46 — el lote pregunta al CRM ══════════════════════════════════════════════════════
test("C46: toda carga al CRM mira antes el veredicto del CRM", () => {
  for (const { nodo, fn } of llamadas("enviarAlBoard")) {
    const antes = popup.slice(fn.start, nodo.start);
    ok(/_veredictoCrm\(|crmVeredicto|_crmBloquea\(/.test(antes), `enviarAlBoard en L${nodo.loc.start.line} carga sin mirar el veredicto del CRM`);
  }
  const verde = texto(handlerDe("btn-push-monday"));
  ok(verde.indexOf("state.crmVeredicto") >= 0 && verde.indexOf("state.crmVeredicto") < verde.indexOf("enviarAlBoard("),
     "el Guard #0 tiene que seguir adentro del botón verde, antes de cargar");
  const validar = texto(funcion("_validarProspectoMonday"));
  ok(validar.indexOf("_crmBloquea()") >= 0 && validar.indexOf("_crmBloquea()") < validar.indexOf("getMondayFormValues()"),
     "'Guardar para enviar después' tiene que frenar a un sitio que el CRM marca como no prospectable");
  const bloqueo = texto(funcion("_aplicarBloqueoCrm"));
  ok(/"btn-guardar-cola"/.test(bloqueo) && /cola\.disabled = /.test(bloqueo), "el botón de la cola tiene que verse bloqueado, como los otros dos");
});

test("C46: el lote pregunta por cada sitio en el momento, corta si el CRM no contesta y no reusa un rebotado", () => {
  const enviar = texto(handlerDe("btn-cola-enviar"));
  const iCarga = enviar.indexOf("enviarAlBoard(");
  ok(iCarga > 0);
  ok(enviar.indexOf("buscarEnCrm(") >= 0 && enviar.indexOf("buscarEnCrm(") < iCarga, "el lote no consulta la ficha antes de cargar");
  ok(!/_crmConsultar\(/.test(enviar), "_crmConsultar comparte la consulta en vuelo de Analysis: el lote usa buscarEnCrm");
  ok(/indeterminado\)[\s\S]{0,120}?break;/.test(enviar), "si el CRM no contesta se corta el lote (300 filas × 8 s)");
  // (2026-09-13, B1) El salteo lo decide decidirLoteCrm: un NO o una duda se saltean con motivo,
  // salvo la ficha "Propuesta Vigente" que crea nuestro propio aviso de adicionales.
  ok(/decidirLoteCrm\(\{[\s\S]{0,160}?\}\);\s*if \(!dec\.enviar\)[\s\S]{0,120}?continue;/.test(enviar), "un NO o una duda del CRM se saltean con motivo");
  ok(enviar.indexOf("isEmailBounced(") >= 0 && enviar.indexOf("isEmailBounced(") < iCarga, "un email rebotado nunca se reusa");
});

// ═══ C47 — idioma y país en el vocabulario del pool ═════════════════════════════════════
test("C47: ninguna opción del selector de idioma se escribe como dígito en el pool", () => {
  const select = /<select id="form-idioma"[\s\S]*?<\/select>/.exec(html)[0];
  const valores = [...select.matchAll(/<option value="([^"]*)"/g)].map(m => m[1]);
  ok(valores.length > 10, "no encontré las opciones de #form-idioma");
  for (const val of valores) {
    const iso = idiomaIsoDelFormulario(val);
    ok(iso === "" || /^[a-z]{2}$/.test(iso), `la opción "${val}" dio "${iso}"`);
    strictEqual(filaColaDesdeFormulario({ idioma: val, geo: "Mexico", traffic: 1 }, {}).language, iso);
  }
  strictEqual(idiomaIsoDelFormulario("1"), "es");
  strictEqual(idiomaIsoDelFormulario("5"), "", "'Language?' no es un idioma");
  const inverso = Object.fromEntries(Object.entries(LANG_TO_IDX).map(([iso, idx]) => [idx, iso]));
  deepStrictEqual(IDIOMA_DE_INDICE_MONDAY, inverso, "el mapa de índices tiene que ser el inverso exacto de LANG_TO_IDX");
});

test("C47: todo país del desplegable de Monday se traduce a ISO, y la fila nueva nunca guarda la etiqueta en español", () => {
  const sinIso = MONDAY_COUNTRIES.filter(c => !isoDeEtiquetaMonday(c, GEO_LABEL));
  deepStrictEqual(sinIso, [], `sin ISO: ${sinIso.join(", ")}`);
  const caso = (label) => { const f = filaColaDesdeFormulario({ geo: label, idioma: "1", traffic: 1 }, { geoLabel: GEO_LABEL }); return [f.geo, f.geos_all]; };
  deepStrictEqual(caso("España"), ["Spain", ["ES"]]);
  deepStrictEqual(caso("Alemania"), ["Germany", ["DE"]], "_leadIso leía 'Alemania' como AL (Albania)");
  deepStrictEqual(caso("Estados Unidos"), ["United States", ["US"]], "_leadIso leía 'Estados Unidos' como ES");
  deepStrictEqual(caso("Holanda"), ["Netherlands", ["NL"]]);
  deepStrictEqual(caso("Afghanistan"), ["AF", ["AF"]], "sin nombre en inglés conocido va el ISO, que el filtro de Prospects entiende");
  deepStrictEqual(caso("Narnia"), ["", null], "lo que no se puede traducir queda vacío, nunca la etiqueta");

  const existente = filaColaDesdeFormulario({ geo: "Brasil", idioma: "3", traffic: 1 }, { prev: { id: 9, status: "pending", traffic: 700000 }, geoLabel: GEO_LABEL });
  for (const k of ["geo", "language", "geos_all", "source", "created_by", "traffic"]) ok(!(k in existente), `el PATCH de una fila existente no puede llevar ${k}`);
  strictEqual(existente.monday_payload.geo_form, "Brasil", "lo que eligió el MB viaja en monday_payload para el CRM");
  strictEqual(existente.monday_payload.idioma, "3");

  const enviar = texto(handlerDe("btn-cola-enviar"));
  ok(/geo: mp\.geo_form \|\| f\.geo/.test(enviar), "el CRM tiene que seguir recibiendo la etiqueta del board");
  ok(/mp\.geo_form \|\| f\.geo/.test(texto(funcion("_colaPintar"))));
});

// ═══ C48 — los emails del pool no se pierden ════════════════════════════════════════════
test("C48: fusionarEmailsCola pone primero la elección del MB y conserva el resto", () => {
  const prev = { emails: ["publicidad@revista.com.ar", "Redaccion@revista.com.ar", "info@revista.com.ar"],
                 email_sources: { "publicidad@revista.com.ar": "scrape", "redaccion@revista.com.ar": { source: "apollo", url: "x" }, "info@revista.com.ar": "generic" } };
  const a = fusionarEmailsCola(prev, "REDACCION@revista.com.ar", { loginEmail: "MB@x.com" });
  deepStrictEqual(a.emails, ["redaccion@revista.com.ar", "publicidad@revista.com.ar", "info@revista.com.ar"]);
  deepStrictEqual(a.email_sources["redaccion@revista.com.ar"], { source: "apollo", url: "x" }, "una fuente que ya existía no se pisa");
  strictEqual(a.payloadEmail, "redaccion@revista.com.ar");

  const nuevaFuente = fusionarEmailsCola(prev, "ventas@revista.com.ar", { loginEmail: "MB@x.com" });
  deepStrictEqual(nuevaFuente.email_sources["ventas@revista.com.ar"], { source: "manual_mb", by: "mb@x.com" });
  strictEqual(nuevaFuente.emails.length, 4);

  const vacio = fusionarEmailsCola(prev, "");
  strictEqual(vacio.cambia, false); strictEqual(vacio.payloadEmail, ""); ok(!("emails" in vacio));

  const form = fusionarEmailsCola(prev, "https://revista.com.ar/contacto", { esFormulario: true });
  strictEqual(form.cambia, false); strictEqual(form.contactoFormulario, "https://revista.com.ar/contacto");

  strictEqual(fusionarEmailsCola({ emails: ["publicidad@revista.com.ar"], email_sources: { "publicidad@revista.com.ar": "scrape" } }, "Publicidad@Revista.com.ar").cambia, false,
    "la misma dirección en otra capitalización no es un cambio");

  const nueva = fusionarEmailsCola(null, "hola@nuevo.com", { loginEmail: "mb@x.com" });
  deepStrictEqual(nueva.emails, ["hola@nuevo.com"]);
});

test("C48: la fila de 'Guardar' no borra emails con un campo vacío ni mete una URL en la lista", () => {
  const prev = { id: 3, status: "pending", traffic: 800000, emails: ["a@s.com", "b@s.com"], email_sources: {} };
  const vacio = filaColaDesdeFormulario({ email: "", geo: "Mexico", traffic: 1 }, { prev });
  ok(!("emails" in vacio) && !("email_sources" in vacio), "un campo vacío borraba la lista");
  strictEqual(vacio.monday_payload.email, "", "la clave va presente: el lote manda vacío, como hoy");
  const url = filaColaDesdeFormulario({ email: "https://s.com/contacto", geo: "Mexico", traffic: 1 }, { prev, esFormulario: true });
  ok(!("emails" in url), "una URL de formulario terminaba en el array de emails");
  strictEqual(url.monday_payload.contacto_formulario, "https://s.com/contacto");
  deepStrictEqual(filaColaDesdeFormulario({ email: "", geo: "Mexico", traffic: 1 }, {}).emails, [], "una fila nueva sin email sigue naciendo con []");
  ok(!("pitch" in vacio), "un pitch vacío en el formulario no borra el pitch del pool");
});

test("C48: el lote manda la dirección que eligió el MB aunque la auditoría reordene la lista", () => {
  strictEqual(emailDeCola({ emails: ["x@s.com"], monday_payload: { contacto_formulario: "https://s.com/c", email: "" } }), "https://s.com/c");
  strictEqual(emailDeCola({ emails: ["x@s.com"], monday_payload: { email: "" } }), "", "campo vacío al guardar: se manda vacío, como hasta hoy");
  strictEqual(emailDeCola({ emails: ["x@s.com", "y@s.com"], monday_payload: { estado: "1" } }), "x@s.com", "una fila de antes del arreglo sigue usando la primera");
  strictEqual(emailDeCola({ emails: ["publicidad@s.com", "redaccion@s.com"], monday_payload: { email: "redaccion@s.com" } }), "redaccion@s.com");
  const enviar = texto(handlerDe("btn-cola-enviar"));
  ok(/emailDeCola\(f\)/.test(enviar) && !/f\.emails\[0\]/.test(enviar), "el lote volvió a mandar emails[0]");
  ok(/emailDeCola\(f\)/.test(texto(funcion("_colaPintar"))), "la lista tiene que mostrar lo mismo que se va a mandar");
});

// ═══ C50 — las escrituras de la cola miran la respuesta ═════════════════════════════════
test("C50: los botones de la cola no escriben a ciegas", () => {
  for (const id of ["btn-cola-enviar", "btn-cola-borrar"]) {
    const h = texto(handlerDe(id));
    ok(!/method: "PATCH"/.test(h), `#${id} volvió a tener un PATCH propio: tiene que pasar por _colaPatchPorEnviar`);
    ok(/_colaPatchPorEnviar\(/.test(h), `#${id} no usa _colaPatchPorEnviar`);
    ok(!/\.catch\(\(\) => \{\}\)/.test(h), `#${id} se traga un error con .catch(() => {})`);
  }
  ok(/queuePendingMark\(/.test(texto(handlerDe("btn-cola-enviar"))), "si la marca falla después de cargar en el CRM, se encola para reintentar");
  const cargar = texto(funcion("_colaCargar"));
  ok(/signal: AbortSignal\.timeout\(/.test(cargar) && /getPendingMarkIds\(/.test(cargar), "la lectura de la cola tiene reloj y esconde las marcas pendientes");
});

test("C50: _colaPatchPorEnviar sólo toca lo que sigue en la cola, renueva el token una vez por lote y dice cuántas tocó", async () => {
  const fn = funcion("_colaPatchPorEnviar");
  const pedidos = [];
  let refrescos = 0;
  const respuestas = [];
  const fetchFalso = async (url, opts) => { pedidos.push({ url, opts }); return respuestas.shift(); };
  const crear = new Function("CONFIG", "state", "ensureFreshToken", "fetch", "AbortSignal", `${texto(fn)}; return _colaPatchPorEnviar;`);
  const patch = crear({ SUPABASE_URL: "https://sb", SUPABASE_ANON_KEY: "anon" }, { accessToken: "viejo" },
    async (margen) => { if (margen === Infinity) { refrescos++; return "nuevo"; } return "viejo"; }, fetchFalso, AbortSignal);

  const ctx = { refrescado: false };
  respuestas.push({ ok: false, status: 401 }, { ok: true, status: 200, json: async () => [{ id: 5 }] });
  deepStrictEqual(await patch("id=eq.5", { status: "validated" }, ctx), { ok: true, ids: ["5"] });
  ok(pedidos[0].url.includes("id=eq.5&status=eq.por_enviar&select=id"), pedidos[0].url);
  ok(pedidos[0].opts.signal, "sin reloj");
  strictEqual(pedidos[0].opts.headers.Prefer, "return=representation");
  strictEqual(pedidos[1].opts.headers.Authorization, "Bearer nuevo");

  respuestas.push({ ok: false, status: 401 });
  deepStrictEqual(await patch("id=eq.6", { status: "validated" }, ctx), { ok: false, error: "HTTP 401" }, "el segundo 401 del mismo lote no vuelve a renovar");
  strictEqual(refrescos, 1);

  respuestas.push({ ok: true, status: 200, json: async () => [] });
  deepStrictEqual(await patch("id=eq.7", { status: "pending" }), { ok: true, ids: [] }, "cero filas = otro MB ya la sacó de la cola");

  const lento = crear({ SUPABASE_URL: "https://sb", SUPABASE_ANON_KEY: "anon" }, { accessToken: "t" }, async () => "t",
    async () => { const e = new Error("aborted"); e.name = "TimeoutError"; throw e; }, AbortSignal);
  deepStrictEqual(await lento("id=eq.8", {}), { ok: false, error: "no contestó en 8 s" });
});

// ═══ C52 — el tráfico que pasó el piso no se reescribe ══════════════════════════════════
test("C52: 'Guardar' no pisa el tráfico de un lead que ya estaba en Prospects", () => {
  const v = { email: "", geo: "Mexico", idioma: "1", traffic: 300000 };
  ok(!("traffic" in filaColaDesdeFormulario(v, { prev: { id: 1, status: "pending", traffic: 750000 } })),
     "300K de visitas encima de 750K de páginas vistas: al sacarlo de la cola, cleanup_pool lo borraba");
  strictEqual(filaColaDesdeFormulario(v, { prev: { id: 1, status: "pending", traffic: 0 } }).traffic, 300000, "sin tráfico guardado, se usa el del MB");
  strictEqual(filaColaDesdeFormulario(v, { prev: { id: 1, status: "pending", traffic: null } }).traffic, 300000);
  strictEqual(filaColaDesdeFormulario(v, {}).traffic, 300000);
  strictEqual(estadoAlSacarDeCola({ status_previo: "pending", traffic: 300000, minTraffic: MIN }).grupo, "bajo_piso");
  strictEqual(estadoAlSacarDeCola({ status_previo: "pending", traffic: 300000, minTraffic: MIN }).status, "rejected", "se descarta en el mismo PATCH (2026-09-13)");
  strictEqual(estadoAlSacarDeCola({ status_previo: "pending", traffic: 0, minTraffic: MIN }).grupo, "prospects", "la regla trafico_bajo de cleanup_pool sólo mira tráfico mayor que 0");
});
