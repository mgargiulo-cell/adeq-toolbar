// El agente: cinco defectos sueltos que otros grupos marcaron fuera de su zona. (2026-09-13, cuarta
// vuelta de la auditoría del pool pedida por el dueño: "Prospects limpio para los MB, Prospects igual
// a Análisis, base estable sin fallas".)
//
// Cada test es una regla que estaba rota en runAgentCycle o en los lectores del rebote:
//   S1. El enriquecimiento del agente le daba el PRIMER email a un lead y no marcaba email_found_at.
//   S2. El chequeo de URL al enviar rechazaba con 'envio:' sin la puerta grande que aplica la entrada.
//   S3. Filas con el idioma como índice del formulario de Monday ('1') en vez de ISO ('es').
//   S4. El piso de 350K del agente BORRABA el lead de Prospects.
//   S5. Los tres lectores del % de rebote dejaban afuera el 2º email (secondary_sent).
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { IDIOMA_DE_INDICE_MONDAY as MAPA_DE_LA_COLA, idiomaIsoDelFormulario } from "../../modules/colaEstado.js";
import { IDIOMA_DE_INDICE_MONDAY, idiomaIsoDelLead } from "../lib/idioma.js";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const entre = (texto, desde, hasta) => {
  const i = texto.indexOf(desde);
  ok(i >= 0, `no encontré ${desde}`);
  const j = texto.indexOf(hasta, i + desde.length);
  ok(j > i, `no encontré ${hasta} después de ${desde}`);
  return texto.slice(i, j);
};
const cuerpoDe = (firma) => {
  const i = worker.indexOf(firma);
  ok(i >= 0, `no encontré ${firma}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const agente = cuerpoDe("async function runAgentCycle(");
const respuesta = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: () => null },
  json: async () => body, text: async () => JSON.stringify(body),
});

// ── S1. El rescate del agente cuenta igual que el del pulido ────────────────────────────
test("S1: el primer email que encuentra el enriquecimiento del agente marca email_found_at, con la regla del pulido", async () => {
  const guardado = entre(agente, "const patch = { emails: validated };", "log(`  🔄 ${domain}: enrichment →");
  ok(/_marcarRescate\(patch, lead\.emails\);/.test(guardado),
     "antes el PATCH guardaba la lista sin email_found_at: el parte no contaba el rescate");
  ok(guardado.indexOf("_marcarRescate(patch, lead.emails)") < guardado.indexOf("body: JSON.stringify(patch)"), "la marca va antes de guardar");
  const codigo = guardado.split("\n").filter(l => !l.trim().startsWith("//")).join("\n");
  ok(!/email_found_at\s*[:=]/.test(codigo), "la condición vive en _marcarRescate, no escrita a mano");

  // Con las entradas del agente: la fila tal como estaba (lead.emails) y la lista validada.
  const { _marcarRescate } = await cargarWorker(["_marcarRescate"]);
  const ahora = "2026-09-13T12:00:00.000Z";
  deepStrictEqual(_marcarRescate({ emails: ["comercial@diario.com.ar"] }, [], ahora),
    { emails: ["comercial@diario.com.ar"], email_found_at: ahora }, "no tenía ninguno: rescate");
  deepStrictEqual(_marcarRescate({ emails: ["comercial@diario.com.ar"] }, null, ahora),
    { emails: ["comercial@diario.com.ar"], email_found_at: ahora }, "emails null en la fila también es 'ninguno'");
  deepStrictEqual(_marcarRescate({ emails: ["comercial@diario.com.ar", "info@diario.com.ar"] }, ["info@diario.com.ar"], ahora),
    { emails: ["comercial@diario.com.ar", "info@diario.com.ar"] }, "sumar una persona sobre un info@ es una mejora, no un rescate");
});

// ── S2. La puerta grande también al enviar ──────────────────────────────────────────────
const YES = { state: "yes", lines: 40 };
const NO = { state: "no", lines: 0 };
const ILEGIBLE = { state: "unknown", lines: 0, why: "http_403" };
const PV = 2_000_000;
let _url = null;
const cargarUrl = () => (_url ??= cargarWorker([
  "_veredictoUrlAlEnviar", "_urlRubroPerdonable", "_apruebaPorAdsTxtYTrafico", "_VETO_ESTRUCTURAL",
  "URL_REJECT_RULES", "classifyByUrlOnly", "REVIEW_QUEUE_MIN_TRAFFIC", "REVIEW_QUEUE_MAX_TRAFFIC",
]));

test("S2: al enviar, un rubro por URL con ads.txt y tráfico pasa; lo estructural, el techo y el piso siguen rechazando", async () => {
  const w = await cargarUrl();
  const v = (dominio, ads, pv = PV) => w._veredictoUrlAlEnviar(w.classifyByUrlOnly(dominio, "", pv), ads, pv);
  strictEqual(v("comparabien.com.pe", YES), "enviar", "antes salía de Prospects con 'envio: url_comparador' aunque entró por la puerta grande");
  strictEqual(v("inkabet.pe", YES), "enviar");
  strictEqual(v("comparabien.com.pe", NO), "rechazar", "sin ads.txt no hay puerta grande");
  strictEqual(v("comparabien.com.pe", { state: "adsense", lines: 0 }), "rechazar", "la excepción de AdSense no perdona el rubro (decisión 1 del 04/09)");
  strictEqual(v("comparabien.com.pe", null), "rechazar");
  strictEqual(v("comparabien.com.pe", ILEGIBLE), "reintentar", "ilegible no es 'no tiene': hoy no sale, pero no sale de Prospects");
  strictEqual(v("comparabien.com.pe", YES, w.REVIEW_QUEUE_MIN_TRAFFIC - 1), "rechazar", "el piso no es un rubro");
  strictEqual(v("diario-grande.com", YES, w.REVIEW_QUEUE_MAX_TRAFFIC + 1), "rechazar", "el techo de 40M no es un rubro");
  strictEqual(v("argentina.gob.ar", YES), "rechazar", "gobierno: veto estructural, con o sin ads.txt");
  strictEqual(v("argentina.gob.ar", ILEGIBLE), "rechazar", "lo estructural no espera a leer el ads.txt");
  strictEqual(v("posgrados.udp.cl", YES), "rechazar", "universidad");
  strictEqual(w._veredictoUrlAlEnviar({ ok: true, reason: "url_ok" }, null, PV), "enviar");
  strictEqual(w._veredictoUrlAlEnviar({ ok: false, reason: "dominio_vacio" }, YES, PV), "rechazar");
  strictEqual(w._urlRubroPerdonable("gigante_45M_pageviews"), false);
  strictEqual(w._urlRubroPerdonable("bajo_trafico_120000"), false);
});

test("S2: para cada tipo que classifyByUrlOnly puede devolver, el envío decide lo mismo que la entrada", async () => {
  const w = await cargarUrl();
  // La regla de la entrada, tal cual está en el pre-filtro por URL de processCsvItem. Si cambia allá,
  // tiene que cambiar _veredictoUrlAlEnviar en el mismo commit.
  ok(worker.includes('if (!_urlV.ok && !_VETO_ESTRUCTURAL.test(String(_urlV.reason || "")) && _apruebaPorAdsTxtYTrafico(_ads, effectivePageViews)) {'),
     "la regla de la entrada cambió: revisar _veredictoUrlAlEnviar");
  const tipos = [...new Set(w.URL_REJECT_RULES.map(([, tipo]) => tipo))];
  ok(tipos.length >= 20, `tipos: ${tipos.join(",")}`);
  for (const tipo of tipos) {
    const reason = `url_${tipo}`;
    const entra = !w._VETO_ESTRUCTURAL.test(reason) && w._apruebaPorAdsTxtYTrafico(YES, PV);
    strictEqual(w._veredictoUrlAlEnviar({ ok: false, reason }, YES, PV) === "enviar", entra, `${reason}: al enviar decide distinto que al entrar`);
  }
});

test("S2: runAgentCycle decide la URL con _veredictoUrlAlEnviar, lee el ads.txt sólo para un rubro y no toca la fila si es ilegible", () => {
  const url = entre(agente, "const _urlChk = classifyByUrlOnly(domain,", "// ── ORDEN: Gmail PRIMERO");
  ok(/const _adsUrl = !_urlChk\.ok && _urlRubroPerdonable\(_urlChk\.reason\)\s*\?\s*await checkAdsTxt\(domain\)/.test(url),
     "el ads.txt se lee sólo cuando la URL marcó un rubro perdonable");
  ok(/const _urlDecision = _veredictoUrlAlEnviar\(_urlChk, _adsUrl, lead\.traffic \|\| 0\);/.test(url));
  ok(!/if \(!_urlChk\.ok\) \{/.test(url), "el rechazo viejo, por cualquier motivo de URL, no puede volver");
  const reintento = entre(url, 'if (_urlDecision === "reintentar")', 'if (_urlDecision === "rechazar")');
  ok(/continue;/.test(reintento), "ilegible: el lead no sale hoy");
  ok(!/status: "rejected"/.test(reintento) && !/method: "PATCH"/.test(reintento), "ilegible: la fila no se toca");
  const rechazo = entre(url, 'if (_urlDecision === "rechazar")', "continue; // próximo lead");
  ok(/suspect_reason: `envio: \$\{_urlChk\.reason\}`/.test(rechazo), "lo que se rechaza sigue con la marca 'envio:' de siempre");
});

// ── S3. El idioma guardado como índice del formulario de Monday ─────────────────────────
test("S3: el índice del formulario de Monday se traduce a ISO con el mismo mapa que la cola", () => {
  deepStrictEqual(IDIOMA_DE_INDICE_MONDAY, MAPA_DE_LA_COLA, "el mapa del worker y el de modules/colaEstado.js tienen que ser el mismo");
  for (let d = 0; d <= 9; d++) strictEqual(idiomaIsoDelLead(String(d)), idiomaIsoDelFormulario(String(d)), `índice ${d}`);
  strictEqual(idiomaIsoDelLead("1"), "es");
  strictEqual(idiomaIsoDelLead(" 3 "), "pt");
  strictEqual(idiomaIsoDelLead(6), "ar");
  strictEqual(idiomaIsoDelLead("0"), "en");
  strictEqual(idiomaIsoDelLead("5"), "", "'Language?' no es un idioma: el agente lo detecta");
  strictEqual(idiomaIsoDelLead("es-AR"), "es", "lo que no es un dígito sigue como antes");
  strictEqual(idiomaIsoDelLead("PT"), "pt");
  strictEqual(idiomaIsoDelLead("fr"), "fr", "un idioma sin plantilla lo resuelve el agente (sale en inglés)");
  strictEqual(idiomaIsoDelLead(null), "");
  strictEqual(idiomaIsoDelLead(undefined), "");
});

test("S3: runAgentCycle arma leadLanguage con idiomaIsoDelLead, y cada índice cae en un idioma con plantilla", async () => {
  ok(/let leadLanguage = idiomaIsoDelLead\(lead\.language\);/.test(agente), "antes '1' se leía como un idioma desconocido");
  ok(!/\(lead\.language \|\| ""\)\.toLowerCase\(\)\.split\("-"\)\[0\]/.test(agente));
  const { idiomaIsoDelLead: delWorker, SUPPORTED_AGENT_LANGS } = await cargarWorker(["idiomaIsoDelLead", "SUPPORTED_AGENT_LANGS"]);
  for (const indice of Object.keys(IDIOMA_DE_INDICE_MONDAY)) {
    ok(SUPPORTED_AGENT_LANGS.has(delWorker(indice)), `índice ${indice} → ${delWorker(indice)} tiene plantilla`);
  }
});

// ── S4. El piso de 350K del agente rechaza, no borra ────────────────────────────────────
test("S4: el piso del agente rechaza con 'cleanup: trafico_bajo' y rejected_at, sólo si sigue pending", async () => {
  const { _rechazarPorTraficoBajo, _motivoCleanup } = await cargarWorker(["_rechazarPorTraficoBajo", "_motivoCleanup"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => { pedidos.push({ url: String(url), opts }); return respuesta(null, { status: 204 }); };
  const ahora = "2026-09-14T12:00:00.000Z";
  strictEqual(await _rechazarPorTraficoBajo({ apikey: "k", Authorization: "Bearer t" }, 77, ahora), true);
  strictEqual(pedidos.length, 1);
  const [p] = pedidos;
  strictEqual(p.opts.method, "PATCH", "nunca DELETE");
  ok(p.url.includes("/rest/v1/toolbar_review_queue?id=eq.77&status=eq.pending"), `si un MB lo movió a su cola, no se toca: ${p.url}`);
  ok(p.opts.signal, "todo fetch con reloj");
  strictEqual(p.opts.headers.Authorization, "Bearer t");
  const body = JSON.parse(p.opts.body);
  deepStrictEqual(body, { status: "rejected", suspect_reject: true, suspect_reason: "cleanup: trafico_bajo", rejected_at: ahora },
    "sin rejected_at no entra en el renglón de purgas del parte");
  strictEqual(body.suspect_reason, _motivoCleanup({ status: "pending", traffic: 120000, created_at: ahora }, Date.parse(ahora)),
    "el mismo motivo que cleanup_pool para la misma regla: el autopilot no aprende rubros de él");
  ok(worker.includes("toolbar_review_queue?rejected_at=gte.${desdeHoy}&select=id"), "el renglón de purgas del parte cuenta por rejected_at");

  globalThis.__fetchFalso = async () => respuesta({ message: "boom" }, { status: 500 });
  strictEqual(await _rechazarPorTraficoBajo({}, 78, ahora), false, "un 500 no se cuenta como sacado");
});

test("S4: runAgentCycle no borra filas de Prospects; el piso usa _rechazarPorTraficoBajo", () => {
  ok(!/method:\s*"DELETE"/.test(agente), "ningún DELETE en el agente");
  const piso = entre(agente, "if (leadTraffic > 0 && leadTraffic < REVIEW_QUEUE_MIN_TRAFFIC) {", "continue;");
  ok(/await _rechazarPorTraficoBajo\(_authRq, lead\.id\)/.test(piso));
  ok(!/below_min_traffic_deleted/.test(agente), "el motivo ya no dice 'deleted'");
});

// ── S5. El 2º email en el % de rebote ───────────────────────────────────────────────────
// La base simulada filtra por `action` como PostgREST (eq.x o in.(x,y)): con el filtro viejo, el 2º
// email no aparece ni en el denominador ni en el cruce con los rebotes.
test("S5: vigilarReputacion suma el 2º email en envíos y en rebotes, y cada rebote cae en su buzón", async () => {
  const { vigilarReputacion } = await cargarWorker(["vigilarReputacion"], { fetchFalso: true });
  const SALES = "sales@adeqmedia.com", DH = "dhorovitz@adeqmedia.com";
  const envios = [
    ...Array.from({ length: 60 }, (_, i) => ({ action: "sent", user_email: SALES, email_to: `ventas${i}@diario${i}.com` })),
    ...Array.from({ length: 20 }, (_, i) => ({ action: "secondary_sent", user_email: SALES, email_to: `publicidad${i}@diario${i}.com` })),
    ...Array.from({ length: 300 }, (_, i) => ({ action: "sent", user_email: DH, email_to: `redaccion${i}@medio${i}.it` })),
  ];
  const rebotes = [
    ...Array.from({ length: 8 }, (_, i) => ({ email: `publicidad${i}@diario${i}.com`, evidencia: "rebote_smtp" })),
    { email: "ventas5@diario5.com", evidencia: "verificador" },   // MillionVerifier: no es un rebote, no cuenta
  ];
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = decodeURIComponent(String(url)), m = (opts.method || "GET").toUpperCase();
    reg.push({ u, m, b: opts.body ? String(opts.body) : "" });
    if (u.includes("/toolbar_agent_actions?") && m === "GET") {
      const f = u.match(/[?&]action=([^&]+)/)?.[1] || "";
      const acciones = f.startsWith("eq.") ? [f.slice(3)] : f.startsWith("in.(") ? f.slice(4, -1).split(",") : [];
      return respuesta(envios.filter(e => acciones.includes(e.action)).map(({ user_email, email_to }) => ({ user_email, email_to })));
    }
    if (u.includes("/toolbar_bounced_emails?")) return respuesta(rebotes);
    return respuesta([]);
  };
  await vigilarReputacion("t");

  const ping = reg.filter(x => x.m === "POST" && x.u.includes("/toolbar_health")).map(x => JSON.parse(x.b)).find(x => x.job === "reputacion");
  ok(ping, "vigilarReputacion tiene que latir");
  ok(/sobre 380 envíos/.test(ping.last_detail), `antes contaba 360, sin los 20 del 2º email: ${ping.last_detail}`);
  const avisos = reg.filter(x => x.m === "POST" && x.u.includes("/toolbar_notifications")).map(x => JSON.parse(x.b));
  const deSales = avisos.find(a => a.dedup_key?.startsWith(`salud-reputacion-${SALES}-`));
  ok(deSales, `el buzón de sales rebota 8 de 80 con su 2º email; antes figuraba 0 de 60 y no avisaba: ${JSON.stringify(avisos.map(a => a.title))}`);
  ok(deSales.body.startsWith("8 de 80 envíos"), deSales.body);
  ok(!avisos.some(a => a.dedup_key?.includes(DH)), "los rebotes del 2º email de sales no se le imputan a otro buzón");
});

test("S5: el aviso por buzón del agente y el bloque REBOTES del boletín cuentan sent + secondary_sent en los dos lados", () => {
  strictEqual((worker.match(/const ACCION_ENVIO_PARA_REBOTE = "in\.\(sent,secondary_sent\)";/g) || []).length, 1, "un solo filtro para los tres lectores");

  const aviso = entre(agente, "const _cuenta = async (filtroAccion)", "if (_rebotes < 0)");
  ok(/action=\$\{filtroAccion\}/.test(aviso));
  ok(/const _enviados = await _cuenta\(ACCION_ENVIO_PARA_REBOTE\);/.test(aviso), "denominador");
  ok(/toolbar_agent_actions\?user_email=eq\.\$\{encodeURIComponent\(userEmail\)\}&action=\$\{ACCION_ENVIO_PARA_REBOTE\}&created_at=gte\.\$\{_desde7\}&select=email_to/.test(aviso),
     "numerador: los destinatarios que se cruzan con la lista de rebotes");
  ok(!/action=eq\.sent/.test(aviso));

  const boletin = entre(cuerpoDe("async function _boletinPorSeccion("), "REBOTES POR BUZÓN", "EL PLAN, DÍA A DÍA");
  ok(/toolbar_agent_actions\?action=\$\{ACCION_ENVIO_PARA_REBOTE\}&details->>ui_origin=is\.null/.test(boletin), "sigue siendo sólo el agente, ahora con su 2º email");
  ok(!/action=eq\.sent/.test(boletin));

  const vigilante = cuerpoDe("async function vigilarReputacion(");
  ok(/toolbar_agent_actions\?action=\$\{ACCION_ENVIO_PARA_REBOTE\}&/.test(vigilante));
  ok(!/action=eq\.sent/.test(vigilante));
});

test("S5: el 2º email se anota con su destinatario y sólo si salió, así el cruce con los rebotes lo encuentra", () => {
  const segundo = entre(agente, "AGENTE TAMBIÉN manda al 2do mejor email", "// 5. Push al CRM.");
  const salio = entre(segundo, "if (_dec2.ok) {", "} catch (e) { log(`  ⚠️ agente 2do email");
  ok(/action: "secondary_sent",[\s\S]*?email_to: secondCandidate\.email,/.test(salio), "sin email_to el rebote del 2º no se puede imputar");
  ok(salio.indexOf("sendGmailServer(") < salio.indexOf('action: "secondary_sent"'), "se anota después de mandarlo");
  strictEqual((agente.match(/action: "secondary_sent"/g) || []).length, 1, "un solo lugar lo anota: el candidato que MV quemó no suma un envío");
});
