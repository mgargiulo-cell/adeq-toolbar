// Los estados de una fila de Prospects cuando se juntan los grupos del 13/09. (2026-09-13, ronda final)
//
// La revisión de la integración encontró defectos que no estaban en ningún grupo por separado: cada rama
// escribió un estado o una marca que la otra no conocía. Cada test es una de esas reglas, con el código real:
//   E1 (I1/I15)  saveToReviewQueue devuelve 'en_cola_de_envio' (la fila está en la tanda 'Por enviar' de un
//                MB) y processCsvItem lo leía como "base rota": la fila iba a 'error', salía la alerta roja
//                y, como ni 'error' ni 'por_enviar' contaban para el dedup, el reciclado lo volvía a pagar.
//   E2 (I2/I20/I22/I26)  La limpieza dejó de borrar y rechaza con 'cleanup:'. Ese prefijo no estaba entre las
//                marcas automáticas: el lead que volvía con tráfico quedaba pending con suspect_reject=true y
//                el agente no lo tomaba nunca.
//   E3 (I3)      El agente marcaba 'validated' por id sin mirar el estado: pisaba la cola 'Por enviar' de un
//                MB que guardó el sitio mientras el agente lo trabajaba.
//   E4 (I4/I23/I28)  "Quitar" soltaba a pending lo de menos de 350K "porque la limpieza lo borraba": ya no
//                borra, rechaza con rejected_at, y el parte lo contaba como purga del pool. El cartel decía
//                "se eliminan".
//   E5 (I5)      La extensión marcaba 'mb: sacada_de_cola_sin_filtro', el prefijo de un rechazo a mano: el
//                sitio que después pasaba el filtro volvía a Alert para siempre.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { findKnownDomains } from "../../modules/sellersJson.js";
import { estadoAlSacarDeCola, planSacarDeCola, textoConfirmarSacar, textoResultadoSacar } from "../../modules/colaEstado.js";

process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (firma) => {
  const i = worker.indexOf(firma);
  ok(i >= 0, `no encontré ${firma}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
// Sin comentarios: un comentario que viaja no puede dejar un test en verde sin el código.
const sinComentarios = (s) => s.split("\n").filter(l => !l.trim().startsWith("//")).join("\n");

const resp = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: () => null },
  json: async () => body,
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
globalThis.__fetchFalso = async () => resp([]);

const W = await cargarWorker([
  "saveToReviewQueue", "_ajustesDeReactivacion", "_motivoCleanup", "_estadoTrasGuardar", "processCsvItem",
  "_dominiosPendientesEnProspects",
], { fetchFalso: true });

// ── Un PostgREST mínimo: filtros eq / in / is.null / not.is.true sobre una tabla en memoria ─────────────
function cumple(fila, query) {
  for (const [k, v] of new URLSearchParams(query)) {
    if (["select", "order", "limit", "on_conflict"].includes(k)) continue;
    const actual = fila[k];
    if (v.startsWith("eq.")) { if (actual == null || String(actual) !== v.slice(3)) return false; }
    else if (v.startsWith("in.(")) {
      const lista = v.slice(4, -1).split(",").map(x => x.replace(/^"|"$/g, ""));
      if (actual == null || !lista.includes(String(actual))) return false;
    } else if (v === "is.null") { if (actual != null) return false; }
    else if (v === "not.is.true") { if (actual === true) return false; }
    else throw new Error(`filtro no simulado: ${k}=${v}`);
  }
  return true;
}

// ═══ E1 — 'en_cola_de_envio' es un veredicto, y el dedup ve la tanda 'Por enviar' ═══════════════════════
test("E1: 'en_cola_de_envio' es un veredicto (skipped, sin alerta), no la base rota", () => {
  deepStrictEqual(W._estadoTrasGuardar("en_cola_de_envio"),
    { status: "skipped", error_message: "review_queue_insert_fail:en_cola_de_envio", alerta: false },
    "iba a 'error' con la alerta 'revisar columnas o permisos' por un lead que sólo esperaba al MB");
  strictEqual(W._estadoTrasGuardar("algo_raro").alerta, true, "lo desconocido sigue siendo la base rota");
});

// Arma la base de un dominio que un MB tiene en su tanda 'Por enviar'. Las lecturas de toolbar_review_queue
// se contestan aplicando el filtro de la URL de verdad: si el filtro no admite por_enviar, no la ve.
const ADS_TXT = Array.from({ length: 25 }, (_, i) => `google.com, pub-${2000 + i}, DIRECT, f08c47fec0942fa0`).join("\n");
const HOME = `<!doctype html><html lang="es"><head><title>La Gaceta de Prueba</title></head><body><article><h1>Hoy</h1><p>${"Nota del dia con texto. ".repeat(80)}</p></article></body></html>`;
function baseConPorEnviar(domain) {
  const fila = { id: 9, domain, status: "por_enviar", source: "csv", created_by: "sales@adeqmedia.com", suspect_reject: false, suspect_reason: null };
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase();
    reg.push({ u, m, b: String(opts.body || "") });
    if (u.includes("/api/crm/ficha?domain=")) return resp({ found: false });
    if (/\/app-ads\.txt$/.test(u)) return resp("", { status: 404 });
    if (/\/ads\.txt$/.test(u)) return resp(ADS_TXT);
    if (u.includes("toolbar_traffic_cache?domain=eq.")) {
      return resp([{ data: { visits: 2_000_000, rawVisits: 2_000_000, pagesPerVisit: 2, topCountries: [{ code: "PE" }], category: "News and Media" }, fetched_at: new Date().toISOString() }]);
    }
    if (m === "GET" && u.includes("/rest/v1/toolbar_review_queue?") && u.includes("domain=eq.")) {
      const q = decodeURIComponent(u.slice(u.indexOf("?") + 1));
      return resp(cumple(fila, q) ? [fila] : []);
    }
    if (/^https?:\/\/[^/]+\/?$/.test(u)) return resp(HOME);
    return resp([]);
  };
  return reg;
}
const patchesDeCola = (reg, id) => reg.filter(r => r.m === "PATCH" && r.u.includes(`toolbar_csv_queue?id=eq.${id}`)).map(r => JSON.parse(r.b));
const USO_APOLLO = { usedToday: 0, limit: 0, monthLimit: 0, usedThisMonth: 0 };
const alertaDeBaseRota = (reg) => reg.some(r => r.m === "POST" && r.u.includes("toolbar_notifications") && /prospects-insert-roto/.test(r.b));

test("E1 processCsvItem de verdad: un item del worker cuyo dominio está en 'Por enviar' se saltea al principio, sin pagar nada", async () => {
  const domain = "lagacetadeprueba.com.pe";
  const reg = baseConPorEnviar(domain);
  await W.processCsvItem("t", { id: 301, domain, source: "auto_feeder_monday", uploaded_by: "worker@autofeeder", error_message: "" },
    { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  deepStrictEqual(patchesDeCola(reg, 301).map(x => [x.status, x.error_message]), [["skipped", "ya_estaba_en_prospects"]],
    "con sólo status=eq.pending seguía y pagaba tráfico, Haiku y quizá Apollo");
  ok(!reg.some(x => /ads\.txt|toolbar_traffic_cache|api-proxy|anthropic|apollo/i.test(x.u)), "no se gasta nada");
  ok(!alertaDeBaseRota(reg), "sin alerta roja");
});

test("E1 processCsvItem de verdad: un import del MB (que no tiene chequeo previo) termina 'skipped' y sin alerta", async () => {
  const domain = "otragacetadeprueba.com.pe";
  const reg = baseConPorEnviar(domain);
  await W.processCsvItem("t", { id: 302, domain, source: "csv", uploaded_by: "diego@adeqmedia.com", error_message: "" },
    { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  const p = patchesDeCola(reg, 302);
  ok(reg.some(x => x.m === "GET" && x.u.includes("toolbar_review_queue?domain=eq.") && x.u.includes("select=id,status,")),
    `el import tiene que llegar a saveToReviewQueue: ${JSON.stringify(p)}`);
  deepStrictEqual(p.map(x => [x.status, x.error_message]), [["skipped", "review_queue_insert_fail:en_cola_de_envio"]],
    "quedaba 'error': no cuenta como cola activa y el sitio se volvía a encolar y pagar");
  ok(!reg.some(x => x.m === "POST" && x.u.includes("toolbar_review_queue?on_conflict=domain")), "la tanda del MB no se toca");
  ok(!alertaDeBaseRota(reg), "la alerta 'revisar columnas o permisos' era falsa");
});

test("E1: el reciclado del worker y el de la extensión cuentan 'por_enviar' como Prospects, con el mismo filtro", async () => {
  const tabla = [
    { domain: "pendiente.com", status: "pending" },
    { domain: "enviar.com", status: "por_enviar" },
    { domain: "rechazado.com", status: "rejected" },
    { domain: "validado.com", status: "validated" },
  ];
  const candidatos = ["pendiente.com", "enviar.com", "rechazado.com", "validado.com", "nuevo.com"];
  const contestar = (url) => {
    const q = decodeURIComponent(String(url).slice(String(url).indexOf("?") + 1));
    return resp(tabla.filter(f => cumple(f, q)).map(f => ({ domain: f.domain })));
  };
  const urlsWorker = [];
  globalThis.__fetchFalso = async (url) => { urlsWorker.push(String(url)); return contestar(url); };
  deepStrictEqual([...await W._dominiosPendientesEnProspects("t", candidatos)].sort(), ["enviar.com", "pendiente.com"],
    "un rechazado o un validado viejo no bloquean el reciclado; la tanda de un MB sí");

  const fetchViejo = globalThis.fetch;
  const urlsExt = [];
  globalThis.fetch = async (url) => {
    const u = String(url);
    urlsExt.push(u);
    return u.includes("toolbar_review_queue") ? contestar(u) : resp([]);
  };
  try {
    const known = await findKnownDomains("https://x.supabase.co", "anon", "tok", candidatos, { mode: "reciclable" });
    deepStrictEqual([...known].sort(), ["enviar.com", "pendiente.com"], "los botones de reciclables del popup, igual que el worker");
  } finally {
    globalThis.fetch = fetchViejo;
  }
  const filtro = (u) => decodeURIComponent(u).match(/status=[^&]+/)?.[0];
  strictEqual(filtro(urlsExt.find(u => u.includes("toolbar_review_queue"))), filtro(urlsWorker[0]), "el mismo texto de filtro en los dos lados");
  ok(/status=in\.\(pending,por_enviar\)/.test(cuerpoDe("async function processCsvItem(").slice(0, 4000)),
    "el chequeo previo de processCsvItem usa la misma regla");
});

// ═══ E2 y E5 — las marcas automáticas se limpian al volver, las de una persona no ═══════════════════════
function simularUpsert(prev) {
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = opts.method || "GET";
    pedidos.push({ u, m, body: opts.body ? JSON.parse(opts.body) : null });
    if (m === "GET" && u.includes("toolbar_review_queue?domain=eq.")) return resp(prev ? [prev] : []);
    if (m === "POST" && u.includes("toolbar_review_queue?on_conflict=domain")) return resp(null, { status: 201 });
    return resp([]);
  };
  return () => pedidos.find(p => p.m === "POST" && p.u.includes("toolbar_review_queue?on_conflict=domain"))?.body;
}
const LEAD = (extra = {}) => ({ domain: "diario-de-vuelta.com.ar", traffic: 900000, geo: "AR", emails: ["publicidad@diario-de-vuelta.com.ar"], createdBy: "worker@autofeeder", source: "similar", ...extra });
const LIMPIA = { suspect_reject: false, suspect_reason: null, suspect_checked_at: null };
const limpiaLaMarca = (extra) => extra.suspect_reject === false && extra.suspect_reason === null && extra.suspect_checked_at === null;

test("E2: los motivos de la limpieza (sacados de _motivoCleanup, no escritos a mano) se limpian al volver", async () => {
  const AHORA = Date.parse("2026-09-13T12:00:00Z");
  const motivos = [
    W._motivoCleanup({ status: "pending", traffic: 120000, source: "csv" }, AHORA),
    W._motivoCleanup({ status: "pending", traffic: null, source: "similar", created_at: new Date(AHORA - 72 * 3600_000).toISOString() }, AHORA),
  ];
  deepStrictEqual(motivos, ["cleanup: trafico_bajo", "cleanup: sin_trafico"]);
  for (const motivo of motivos) {
    for (const source of ["similar", "csv", "monday_refresh"]) {
      const { extra } = W._ajustesDeReactivacion({ status: "rejected", suspect_reject: true, suspect_reason: motivo }, { source });
      ok(limpiaLaMarca(extra), `${motivo} (${source}) volvía pending con suspect_reject=true: ${JSON.stringify(extra)}`);
    }
  }
  const upsert = simularUpsert({ id: 9, status: "rejected", source: "similar", suspect_reject: true, suspect_reason: "cleanup: sin_trafico" });
  strictEqual(await W.saveToReviewQueue("t", LEAD()), "ok");
  const b = upsert();
  strictEqual(b.status, "pending");
  deepStrictEqual({ suspect_reject: b.suspect_reject, suspect_reason: b.suspect_reason, suspect_checked_at: b.suspect_checked_at }, LIMPIA,
    "el agente (suspect_reject=not.is.true) no la tomaba nunca");
});

test("E2 paridad: todo prefijo con el que el worker rechaza en el mismo paso se limpia; lo de una persona se queda", () => {
  const prefijos = new Set();
  for (const m of worker.matchAll(/status: "rejected", suspect_reject: true, suspect_reason: `([a-z_]+):/g)) prefijos.add(m[1]);
  const cleanup = worker.match(/const CLEANUP_PREFIJO = "([a-z_]+):";/);
  ok(cleanup, "no encontré CLEANUP_PREFIJO");
  ok(/body: JSON\.stringify\(\{ status: "rejected", suspect_reject: true, suspect_reason: motivo,/.test(cuerpoDe("async function _cleanupPool(")) &&
     /const motivo = `\$\{CLEANUP_PREFIJO\} \$\{clave\}`;/.test(cuerpoDe("async function _cleanupPool(")), "la limpieza escribe su motivo con CLEANUP_PREFIJO");
  prefijos.add(cleanup[1]);
  for (const p of ["purge", "urlpurge", "envio", "descongelado", "cleanup"]) ok(prefijos.has(p), `la búsqueda no encontró ${p}: ${[...prefijos]}`);
  for (const p of prefijos) {
    const { extra } = W._ajustesDeReactivacion({ status: "rejected", suspect_reject: true, suspect_reason: `${p}: algo` }, { source: "csv" });
    ok(limpiaLaMarca(extra), `'${p}:' rechaza en el mismo paso y no está en _MARCA_AUTOMATICA_RE`);
  }
  // Las marcas que escribe la extensión al sacar de la cola.
  for (const entrada of [{ source: "manual_cola" }, { status_previo: "pending", traffic: 300000 }]) {
    const r = estadoAlSacarDeCola(entrada).suspect_reason;
    const { extra } = W._ajustesDeReactivacion({ status: "rejected", suspect_reject: true, suspect_reason: r }, { source: "sellers_json" });
    ok(limpiaLaMarca(extra), `'${r}' (colaEstado) tiene que ser una marca automática del worker`);
  }
  // Lo de una persona sigue en Alert.
  for (const r of ["mb: no es un medio", null, "barrido: tienda", "Parece un banco (Haiku)"]) {
    const { extra } = W._ajustesDeReactivacion({ status: "rejected", suspect_reject: true, suspect_reason: r }, { source: "csv" });
    ok(!("suspect_reject" in extra), `${r} lo decidió una persona: vuelve a Alert`);
  }
});

test("E5: un sitio que el MB sacó de su cola sin filtrar vuelve limpio si después pasa el filtro", async () => {
  const marca = estadoAlSacarDeCola({ source: "manual_cola" });
  deepStrictEqual([marca.status, marca.suspect_reject, marca.suspect_reason], ["rejected", true, "cola: sacada_sin_filtro"],
    "sigue rechazada: sin filtro no entra a Prospects");
  ok(!/^mb:/i.test(marca.suspect_reason), "'mb:' es el prefijo de un rechazo a mano y el worker lo conserva");
  const upsert = simularUpsert({ id: 11, status: "rejected", source: "manual_cola", created_by: "mb@adeqmedia.com", suspect_reject: true, suspect_reason: marca.suspect_reason });
  strictEqual(await W.saveToReviewQueue("t", LEAD({ source: "sellers_json" })), "ok");
  const b = upsert();
  deepStrictEqual({ suspect_reject: b.suspect_reject, suspect_reason: b.suspect_reason, suspect_checked_at: b.suspect_checked_at }, LIMPIA,
    "quedaba en Alert y el agente no la tomaba");
});

// ═══ E4 — "Quitar" descarta lo de menos de 350K en el mismo PATCH, sin rejected_at ═══════════════════════
test("E4: lo de menos de 350K sale descartado con el motivo de la limpieza, no vuelve a Prospects y no cuenta como purga", () => {
  const MIN = 350000;
  const filas = [
    { id: 1, domain: "a.com", source: "monday_refresh", traffic: 900000, monday_payload: { status_previo: "pending" } },
    { id: 2, domain: "b.com", source: "monday_refresh", traffic: 200000, monday_payload: { status_previo: "pending" } },
  ];
  const plan = planSacarDeCola(filas, { contactados: new Set(), minTraffic: MIN });
  const cuerpo = Object.fromEntries(plan.lotes.flatMap(l => l.ids.map(id => [id, l.body])));
  deepStrictEqual(cuerpo["1"], { status: "pending" });
  deepStrictEqual(cuerpo["2"], { status: "rejected", suspect_reject: true, suspect_reason: "cleanup: trafico_bajo" },
    "volvía a pending: 15 minutos en Prospects al alcance del pulido y el barrido, y después la limpieza la contaba como purga");
  // El motivo es EXACTAMENTE el de la limpieza del worker, para esa misma fila.
  strictEqual(cuerpo["2"].suspect_reason, W._motivoCleanup({ status: "pending", traffic: 200000, source: "monday_refresh" }));
  for (const l of plan.lotes) ok(!("rejected_at" in l.body), "el parte cuenta rejected_at como purga del pool");
  // Ya rechazada, la limpieza no la vuelve a tocar: el renglón de purgas del parte no la ve nunca.
  strictEqual(W._motivoCleanup({ status: "rejected", traffic: 200000, source: "monday_refresh" }), null);
  const conf = textoConfirmarSacar(plan, { minTraffic: MIN });
  ok(/1 tienen menos de 350K y se descartan/.test(conf) && !/se eliminan|se borran/i.test(conf), conf);
  const fin = textoResultadoSacar(["1", "2"], 2, plan.grupoPorId, { minTraffic: MIN });
  ok(/se descartan/.test(fin) && !/se eliminan/.test(fin), fin);
});

// ═══ E3 — el agente no pisa la cola 'Por enviar' ═════════════════════════════════════════════════════════
const agente = cuerpoDe("async function runAgentCycle(");
const tramoAgente = (desde, hasta) => {
  const i = agente.indexOf(desde);
  ok(i >= 0, `no encontré ${desde}`);
  const j = agente.indexOf(hasta, i);
  ok(j > i, `no encontré ${hasta} después de ${desde}`);
  return agente.slice(i, j);
};

test("E3: el agente relee el estado de la fila justo antes de mandar y no manda si no está pending (ni si no pudo leerlo)", () => {
  const antes = sinComentarios(tramoAgente("const _mvEstado = await _verifyEmailMV(token, cfg, email)", "sendGmailServer(token, userEmail, { to: email,"));
  const i = antes.indexOf("toolbar_review_queue?id=eq.${lead.id}&select=status");
  ok(i >= 0, "entre la reserva y el envío nadie miraba si un MB ya había guardado el sitio en su cola");
  const bloque = antes.slice(i);
  ok(/signal: AbortSignal\.timeout\(\d+\)/.test(bloque.slice(0, 400)), "con reloj");
  ok(/let _estadoFila = null;/.test(antes) && /if \(_estadoFila !== "pending"\) \{/.test(bloque),
    "falla cerrado: una lectura que no se pudo hacer deja _estadoFila en null y tampoco se manda");
  const salteo = bloque.slice(bloque.indexOf('if (_estadoFila !== "pending") {'));
  const fin = salteo.indexOf("continue;");
  ok(fin > 0, "el salteo tiene que cortar el lead");
  const cuerpoSalteo = salteo.slice(0, fin);
  ok(/toolbar_agent_actions\?id=eq\.\$\{reservedId\}/.test(cuerpoSalteo) && /action: "skipped"/.test(cuerpoSalteo), "cierra la reserva del cupo");
  ok(!/toolbar_review_queue/.test(cuerpoSalteo), "no toca la fila: es del MB (o del lote)");
  ok(agente.indexOf('action: "reserved"') < agente.indexOf("toolbar_review_queue?id=eq.${lead.id}&select=status"), "la reserva ya existe: el salteo la cierra");
});

test("E3: el paso 7 marca validated SÓLO si la fila sigue pending; una 'Por enviar' de un MB queda como estaba", () => {
  const paso7 = sinComentarios(tramoAgente("// 7. Marcar el review_queue item", "// 8. Log del resultado"));
  const m = paso7.match(/fetch\(`\$\{SUPABASE_URL\}\/rest\/v1\/(toolbar_review_queue\?[^`]+)`, \{\s*method: "PATCH"/);
  ok(m, `no encontré el PATCH del paso 7: ${paso7.slice(0, 300)}`);
  ok(/"Prefer": "return=representation"/.test(paso7) && /signal: AbortSignal\.timeout\(\d+\)/.test(paso7), "devuelve lo que tocó, con reloj");
  ok(/\.catch\(\(\) => null\)/.test(paso7), "una excepción después del envío no puede caer en el catch que marca 'failed'");
  const tabla = [{ id: 42, status: "por_enviar" }, { id: 43, status: "pending" }];
  for (const id of [42, 43]) {
    const query = m[1].replace("${lead.id}", String(id));
    const body = { status: "validated", validated_by: "agent:mb@adeqmedia.com" };
    for (const f of tabla) if (cumple(f, query.slice(query.indexOf("?") + 1))) Object.assign(f, body);
  }
  strictEqual(tabla[0].status, "por_enviar", "la fila desaparecía de la cola del MB sin aviso");
  strictEqual(tabla[1].status, "validated");
});
