// La entrada de la cola, ronda final antes del deploy (2026-09-13). Lo que dejaron abierto la verificación
// de la entrada y la revisión del informe:
//   1. Dos dominios seguidos al frente de la cola que el CRM no contesta la frenaban para siempre, con la
//      alerta "el CRM no respondió" aunque el CRM contestaba para el resto. Se corre runCsvQueue DE VERDAD.
//   2. La alerta del CRM usaba la misma clave para dos severidades, y un texto hablaba de "30 min".
//   3. Los reintentos de "no pude congelar" se gastaban en segundos, sin darle tiempo a la base.
//   4. `_fichaFallos`: un contador que sólo se sumaba.
//   5. El informe: páginas vistas con decimales, motivos en mayúscula, MillionVerifier medido sobre otra
//      población que "N email(s)", y la cola o las altas sin leer pintadas como cero.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual, match, doesNotMatch } from "node:assert";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};

// Igual que cargarWorker (tests/_worker-exportado.mjs, con fetchFalso) más tres cambios que sólo necesita
// runCsvQueue: sin la pausa de fin de semana (los tests corren cualquier día), sin los 2,5 s entre
// dominios, y con el techo de 4 minutos por dominio en un reloj que no retiene al proceso de los tests.
// No se toca _worker-exportado.mjs: lo usan todos los tests.
async function cargarWorkerDeLaCola(funciones) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "adeq-cola-"));
  for (const f of ["node_modules", "lib", "discovery.js", "templates.js", "keywordsData.js", "package.json"]) {
    fs.symlinkSync(path.join(RAIZ, f), path.join(tmp, f));
  }
  let s = worker;
  const cambiar = (de, a) => {
    const n = typeof de === "string" ? s.split(de).length - 1
      : (s.match(new RegExp(de.source, de.flags.includes("g") ? de.flags : `${de.flags}g`)) || []).length;
    if (n !== 1) throw new Error(`esperaba encontrar una sola vez ${de} en index.js, hay ${n}`);
    s = s.replace(de, a);
  };
  cambiar("main().catch(", "false && main().catch(");
  cambiar(/^import fetchNodo from "node-fetch";/m, "const fetchNodo = (...a) => globalThis.__fetchFalso(...a);");
  cambiar("function _isWeekendSpain() {", "function _isWeekendSpain() { return false;");
  cambiar(/const DOMAIN_DELAY_MS\s*=\s*2500;/, "const DOMAIN_DELAY_MS = 0;");
  cambiar('rej(new Error("item_timeout_4min")), _TECHO_ITEM_MS)', 'rej(new Error("item_timeout_4min")), _TECHO_ITEM_MS).unref()');
  s += `\nexport { ${funciones.join(", ")} };\n`;
  fs.writeFileSync(path.join(tmp, "index.js"), s);
  return import(pathToFileURL(path.join(tmp, "index.js")).href);
}

const resp = (body, { status = 200, headers = {} } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: (k) => headers[String(k).toLowerCase()] ?? null },
  json: async () => body,
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
globalThis.__fetchFalso = async () => resp([]);

const W = await cargarWorkerDeLaCola([
  "runCsvQueue", "_estadoTrasCrmSinRespuesta", "_filasEnEspera", "_filasColaEnEspera", "ESPERA_FILA_COLA_MS",
  "CRM_DOMINIO_DE_PRUEBA", "_motivoCanonicoCola", "_viasDeEmailInforme", "_boletinPorSeccion",
]);

const ADS_TXT = Array.from({ length: 25 }, (_, i) => `google.com, pub-${1000 + i}, DIRECT, f08c47fec0942fa0`).join("\n");
const HOME = `<!doctype html><html lang="es"><head><title>El Diario de Prueba</title></head><body><article><h1>Ultima hora</h1><p>${"Texto de la nota del dia. ".repeat(80)}</p></article></body></html>`;
const MB = "agustina@adeqmedia.com";
const AGENTE = "worker@autofeeder";
const fila = (id, domain, { uploaded_by = AGENTE, error_message = "" } = {}) => ({
  id, domain, status: "pending", uploaded_by, source: uploaded_by === AGENTE ? "auto_feeder_sellers" : "csv",
  error_message, uploaded_at: new Date(Date.UTC(2026, 8, 1, 0, id)).toISOString(),
});
const CRM_DESCANSA = () => resp({ found: true, descansando: true, diasParaReintentar: 10, estado: "", board: "" });

// Una cola en memoria (uploaded_at ascendente = orden de llegada) y un CRM que contesta por dominio.
// Anota cada pedido, las alertas del CRM y los latidos de csv_queue.
function colaFalsa(filas, crm) {
  const reg = [], alertas = [], latidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase(), b = String(opts.body || "");
    reg.push({ u, m, b });
    if (u.includes("/ficha?domain=")) return crm(decodeURIComponent(u.split("domain=")[1]));
    if (u.includes("/rest/v1/toolbar_notifications") && m === "POST") {
      const j = JSON.parse(b);
      if (String(j.dedup_key || "").startsWith("salud-ficha-crm")) alertas.push(j);
      return resp(null, { status: 201 });
    }
    if (u.includes("/rest/v1/toolbar_health") && m !== "GET") {
      try { for (const j of [].concat(JSON.parse(b))) if (j?.job === "csv_queue") latidos.push(j); } catch {}
      return resp(null, { status: 201 });
    }
    if (u.includes("/rest/v1/toolbar_config")) {
      return m === "GET" ? resp([{ key: "csv_queue_enabled", value: "true" }]) : resp([], { status: 201 });
    }
    if (u.includes("/rest/v1/toolbar_csv_queue")) {
      const qs = new URLSearchParams(u.split("?")[1] || "");
      if (m === "GET") {
        if (qs.get("status") !== "eq.pending") return resp([], { headers: { "content-range": "*/0" } });
        let r = filas.filter(f => f.status === "pending");
        for (const ub of qs.getAll("uploaded_by")) {
          if (ub.startsWith("neq.")) r = r.filter(f => f.uploaded_by !== ub.slice(4));
          if (ub.startsWith("not.in.(")) { const l = ub.slice(8, -1).split(",").map(x => x.replace(/"/g, "")); r = r.filter(f => !l.includes(f.uploaded_by)); }
        }
        for (const idf of qs.getAll("id")) {
          if (idf.startsWith("not.in.(")) { const l = idf.slice(8, -1).split(",").map(Number); r = r.filter(f => !l.includes(f.id)); }
        }
        r.sort((a, c) => a.uploaded_at.localeCompare(c.uploaded_at));
        return resp(r.slice(0, Number(qs.get("limit") || 1000)).map(f => ({ ...f })));
      }
      if (m === "PATCH") {
        const idq = qs.get("id");
        if (!idq || !idq.startsWith("eq.")) return resp(null, { status: 204 });
        const f = filas.find(x => x.id === Number(idq.slice(3)));
        const stq = qs.get("status");
        if (!f || (stq && f.status !== stq.slice(3))) return resp([]);
        Object.assign(f, JSON.parse(b || "{}"));
        return resp([{ ...f }]);
      }
      return resp([], { status: 201 });
    }
    if (/\/app-ads\.txt$/.test(u)) return resp("", { status: 404 });
    if (/\/ads\.txt$/.test(u)) return resp(ADS_TXT);
    if (u.includes("toolbar_traffic_cache?domain=eq.")) return resp([{ data: { noData: true }, fetched_at: new Date().toISOString() }]);
    if (u.includes("toolbar_frozen_leads") && m === "POST") return resp({ message: "upstream" }, { status: 503 });
    if (/^https?:\/\/[^/]+\/?$/.test(u)) return resp(HOME);
    return resp([]);
  };
  const reclamos = (id) => reg.filter(r => r.m === "PATCH" && r.u.includes(`toolbar_csv_queue?id=eq.${id}&status=eq.pending`)).length;
  const pedidosAlCrm = () => reg.filter(r => r.u.includes("/ficha?domain=")).map(r => decodeURIComponent(r.u.split("domain=")[1]));
  return { reg, alertas, latidos, reclamos, pedidosAlCrm };
}

// El reloj de las esperas: se adelanta entre vueltas, nunca adentro de una.
const _ahoraReal = Date.now;
let _adelanto = 0;
const adelantar = (ms) => { _adelanto += ms; Date.now = () => _ahoraReal() + _adelanto; };
const relojReal = () => { _adelanto = 0; Date.now = _ahoraReal; };
const CFG = { rapidapi_key: "", apollo_api_key: "" };

// ── 1. La regla ─────────────────────────────────────────────────────────────────────────────
test("el CRM sin respuesta: si el caído es el CRM no se suma nada; si es el dominio, crm_N conserva la memoria de la fila y al tercero va a next_day", () => {
  const f = W._estadoTrasCrmSinRespuesta;
  deepStrictEqual(f({ mensajePrevio: "crm_sin_respuesta crm_2", crmContesta: false }), { status: "pending", error_message: null, cortarTanda: true, intento: 0 },
    "con el CRM caído la fila vuelve tal cual: un corte de 10 minutos no manda a mañana los imports de los MB");
  deepStrictEqual(f({ mensajePrevio: "", crmContesta: true }), { status: "pending", error_message: "crm_sin_respuesta crm_1", cortarTanda: false, intento: 1 });
  deepStrictEqual(f({ mensajePrevio: "crm_sin_respuesta crm_1", crmContesta: true }), { status: "pending", error_message: "crm_sin_respuesta crm_2", cortarTanda: false, intento: 2 });
  deepStrictEqual(f({ mensajePrevio: "crm_sin_respuesta crm_2", crmContesta: true }), { status: "next_day", error_message: "crm_sin_respuesta crm_3", cortarTanda: false, intento: 3 });
  strictEqual(f({ mensajePrevio: "crm_sin_respuesta crm_3", crmContesta: true }).status, "next_day", "de vuelta de next_day, un intento y otra vez a esperar");

  // Las marcas de las otras salidas se leen igual que antes.
  const desc = f({ mensajePrevio: "unfrozen_retry_attempt_2 freeze_1", crmContesta: true }).error_message;
  strictEqual(desc, "unfrozen_retry_attempt_2 freeze_1 crm_1");
  strictEqual(desc.match(/attempt_(\d+)/)[1], "2");
  strictEqual(desc.match(/\bfreeze_(\d+)\b/)[1], "1");
  const trans = f({ mensajePrevio: "traffic_api_transient retry_5 (reintento sin penalizar): timeout crm_1", crmContesta: true }).error_message;
  strictEqual(trans, "traffic_api_transient retry_5 (reintento sin penalizar): timeout crm_2");
  strictEqual(trans.match(/retry_(\d+)/)[1], "5");
  const anglo = f({ mensajePrevio: "anglo_daily_quota: United States — cupo del día", crmContesta: true }).error_message;
  strictEqual((anglo.match(/anglo_daily_quota:\s*([^—]+?)\s*—/) || [])[1], "United States", "el país del cupo anglo sigue ahí: no se vuelve a pagar RapidAPI para saberlo");
});

// ── 2. runCsvQueue de verdad: dos dominios que el CRM no contesta al frente de la cola ─────────
test("runCsvQueue de verdad: dos imports que el CRM no contesta al frente no frenan a los tres de atrás, esperan, y al tercer intento pasan a next_day con aviso", async () => {
  W._filasColaEnEspera.clear();
  const filas = [
    fila(1, "primerodeprueba.com.ar", { uploaded_by: MB, error_message: "unfrozen_retry_attempt_2 freeze_1" }),
    fila(2, "segundodeprueba.com.ar", { uploaded_by: MB }),
    fila(3, "tercerodeprueba.com.ar"), fila(4, "cuartodeprueba.com.ar"), fila(5, "quintodeprueba.com.ar"),
  ];
  const MALOS = new Set(["primerodeprueba.com.ar", "segundodeprueba.com.ar"]);
  const c = colaFalsa(filas, (d) => (MALOS.has(d) ? resp({ error: "boom" }, { status: 500 }) : CRM_DESCANSA()));
  try {
    const n1 = await W.runCsvQueue("t", CFG, 50);
    strictEqual(n1, 3, `con el código anterior la vuelta procesaba 0: ${filas.map(f => `${f.id}:${f.status}`).join(" ")}`);
    deepStrictEqual(filas.map(f => f.status), ["pending", "pending", "skipped", "skipped", "skipped"]);
    strictEqual(filas[0].error_message, "unfrozen_retry_attempt_2 freeze_1 crm_1", "la fila guarda cuántas veces el CRM no la contestó, sin perder su memoria");
    strictEqual(filas[1].error_message, "crm_sin_respuesta crm_1");
    ok(c.pedidosAlCrm().includes(W.CRM_DOMINIO_DE_PRUEBA), "la culpa la decide el dominio de prueba");
    deepStrictEqual(c.alertas, [], "el CRM contesta: ni 'CRM caído' ni aviso por dos intentos");
    ok(!c.latidos.some(l => l.last_status === "warn"), "un dominio que el CRM no contesta no es la cola rindiendo poco");

    // Enseguida: las dos esperan fuera de la cola, y la cola no se apaga.
    const n2 = await W.runCsvQueue("t", CFG, 50);
    strictEqual(n2, 0);
    strictEqual(c.reclamos(1), 1, "esperan: no se reclaman otra vez en segundos");
    strictEqual(c.reclamos(2), 1);
    ok(!c.reg.some(r => r.m !== "GET" && r.u.includes("toolbar_config") && /csv_queue_enabled/.test(r.b) && /false/.test(r.b)),
      "lo que espera sigue en pending: la cola no está vacía y no se apaga");

    adelantar(W.ESPERA_FILA_COLA_MS + 1000);
    await W.runCsvQueue("t", CFG, 50);
    deepStrictEqual(filas.slice(0, 2).map(f => [f.status, f.error_message]),
      [["pending", "unfrozen_retry_attempt_2 freeze_1 crm_2"], ["pending", "crm_sin_respuesta crm_2"]]);
    deepStrictEqual(c.alertas, []);

    adelantar(W.ESPERA_FILA_COLA_MS + 1000);
    await W.runCsvQueue("t", CFG, 50);
    deepStrictEqual(filas.slice(0, 2).map(f => [f.status, f.error_message]),
      [["next_day", "unfrozen_retry_attempt_2 freeze_1 crm_3"], ["next_day", "crm_sin_respuesta crm_3"]]);
    strictEqual(c.alertas.length, 1, JSON.stringify(c.alertas));
    const a = c.alertas[0];
    strictEqual(a.severity, "warning");
    match(a.dedup_key, /^salud-ficha-crm-no-responde-dominio-/, "un dominio puntual tiene su propia clave: no pisa el 'CRM caído' del día");
    match(a.body, /primerodeprueba\.com\.ar, segundodeprueba\.com\.ar/, "el aviso nombra los dominios");
    ok(!c.alertas.some(x => x.severity === "error"));
  } finally { relojReal(); W._filasColaEnEspera.clear(); }
});

test("runCsvQueue de verdad: con el CRM caído se corta al primer dominio, ninguno suma intentos (ningún import pasa a next_day) y la alerta es de error con su clave", async () => {
  W._filasColaEnEspera.clear();
  const filas = [fila(11, "unodeprueba.com.ar", { uploaded_by: MB }), fila(12, "dosdeprueba.com.ar", { uploaded_by: MB }), fila(13, "tresdeprueba.com.ar")];
  const c = colaFalsa(filas, () => resp({ error: "down" }, { status: 503 }));
  try {
    for (let vuelta = 1; vuelta <= 4; vuelta++) {
      strictEqual(await W.runCsvQueue("t", CFG, 50), 0);
      adelantar(W.ESPERA_FILA_COLA_MS + 1000);
    }
    deepStrictEqual(filas.map(f => [f.status, f.error_message]), [["pending", ""], ["pending", ""], ["pending", ""]],
      "un CRM caído no le suma intentos a nadie");
    strictEqual(c.reclamos(11), 4, "cada vuelta prueba el primero");
    strictEqual(c.reclamos(12), 0, "y corta: no hace esperar 15 s a cada dominio detrás de un CRM caído");
    strictEqual(c.pedidosAlCrm().filter(d => d === W.CRM_DOMINIO_DE_PRUEBA).length, 4);
    ok(c.alertas.length >= 1);
    for (const a of c.alertas) {
      strictEqual(a.severity, "error");
      match(a.dedup_key, /^salud-ficha-crm-no-responde-\d{4}-\d{2}-\d{2}$/);
      match(a.body, /unodeprueba\.com\.ar/);
      doesNotMatch(a.body, /30 min/, "el worker vuelve a probar en unos 30 segundos, no en 30 minutos");
    }
    ok(c.latidos.some(l => l.last_status === "warn"), "el CRM caído sí deja el latido en warn");
  } finally { relojReal(); W._filasColaEnEspera.clear(); }
});

// ── 3. No pude congelar: el reintento espera ───────────────────────────────────────────────────
test("runCsvQueue de verdad: si no se pudo congelar, la fila vuelve a la cola pero espera; no gasta sus tres reintentos en la misma tanda", async () => {
  W._filasColaEnEspera.clear();
  const filas = [fila(21, "otrodiariodeprueba.com.pe", { error_message: "unfrozen_retry_attempt_2 freeze_2" }), fila(22, "sextodeprueba.com.ar")];
  const c = colaFalsa(filas, (d) => (d === "otrodiariodeprueba.com.pe" ? resp({ found: false }) : CRM_DESCANSA()));
  try {
    strictEqual(await W.runCsvQueue("t", CFG, 50), 2);
    strictEqual(c.reclamos(21), 1, "con el código anterior se reclamaba 4 veces en segundos y terminaba en error");
    deepStrictEqual([filas[0].status, filas[0].error_message], ["pending", "no_traffic_data — attempt_2/3 freeze_fail_1 freeze_2"]);
    strictEqual(filas[1].status, "skipped", "la de atrás se procesa igual");
    ok(W._filasEnEspera().has(21));
    ok(!W._filasEnEspera(Date.now() + W.ESPERA_FILA_COLA_MS + 1).has(21), "la espera es de unos minutos, no para siempre");
  } finally { relojReal(); W._filasColaEnEspera.clear(); }
});

// ── 4. Textos y contador muerto ─────────────────────────────────────────────────────────────────
test("sin contador muerto de la ficha, sin '30 min' en la cola y sin la regla 'dos seguidos'", () => {
  ok(!/let _fichaFallos\b|_fichaFallos\+\+/.test(worker), "_fichaFallos sólo se sumaba: nadie lo leía");
  const tanda = cuerpoDe("runCsvQueue");
  doesNotMatch(tanda, /\(30 min\)/, "el loop principal vuelve a llamar a la cola cada 30 s");
  ok(!/_crmCaidoCortaLaTanda|CRM_FALLOS_SEGUIDOS_PARA_CORTAR/.test(worker), "la regla 'dos seguidos' frenaba la cola con dos dominios malos al frente");
  ok(tanda.includes('clave: "ficha-crm-no-responde", severidad: "error"') && tanda.includes('clave: "ficha-crm-no-responde-dominio", severidad: "warning"'),
    "dos severidades, dos claves");
  ok(!/const auth =\{/.test(worker));
});

// ── 5. El informe ───────────────────────────────────────────────────────────────────────────────
test("el motivo canónico: páginas vistas con decimales y motivos en mayúscula caen en la misma clave", () => {
  const m = W._motivoCanonicoCola;
  strictEqual(m("pageviews 212345.5 (hypestat pageviews) below min 350000"), "trafico_bajo_piso");
  strictEqual(m("pageviews 51000000.25 above max 50000000 (gigante — venta directa, no prospección)"), "gigante_sobre_techo");
  strictEqual(m("pageviews 212345 (visits×2.31) below min 350000"), "trafico_bajo_piso", "los enteros siguen igual");
  strictEqual(m("not_publisher: Gigante_120M"), "gigante");
  strictEqual(m("reintentar: Bajo_Trafico_99"), "bajo_trafico");
  strictEqual(m("not_publisher: Haiku_Corp"), "haiku_corp");
});

test("MillionVerifier se mide sobre la misma cohorte que 'N email(s)': una alta de hace 20 días verificada esta semana no suma", () => {
  // vieja.pe es un alta de hace 20 días (sólo en el respaldo); MV quemó su dirección esta semana.
  const VIEJA = { domain: "vieja.pe", emails: [], email_sources: { "ventas@vieja.pe": "rol_mx" } };
  // nueva.pe es de estos 7 días: a@ sigue, b@ la quemó MV.
  const NUEVA = { domain: "nueva.pe", emails: ["a@nueva.pe"], email_sources: { "a@nueva.pe": "rol_mx", "b@nueva.pe": "rol_mx" } };
  const r = W._viasDeEmailInforme({
    cohorte: [NUEVA], respaldo: [VIEJA, NUEVA],
    enviados: [{ email_to: "ventas@vieja.pe", details: {} }],
    malos: [{ email: "ventas@vieja.pe", evidencia: "verificador" }, { email: "b@nueva.pe", evidencia: "verificador" }, { email: "ventas@vieja.pe", evidencia: "rebote_smtp" }],
    verificados: [{ email: "ventas@vieja.pe" }, { email: "a@nueva.pe" }, { email: "b@nueva.pe" }],
  });
  deepStrictEqual(r.via.rol_mx, { n: 1, rebotes: 1, env: 1, ver: 2, mvNo: 1 },
    "con el código anterior: ver 3 y mvNo 2, mezclando la alta vieja; el rebote sí se atribuye con el respaldo");
  match(r.lineas.join("\n"), /rol_mx\s+1 email\(s\) · rebotaron\s+1 \(100%\) de 1 enviados · MV descartó 1 de 2 verificados/);
});

function boletinFalso({ colaFalla = false, altasFalla = false } = {}) {
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url);
    reg.push({ u, m: String(opts.method || "GET").toUpperCase() });
    if (u.includes("toolbar_csv_queue?processed_at=gte.")) {
      return colaFalla ? resp({ message: "timeout" }, { status: 500 })
        : resp([{ domain: "reac.com", source: "auto_feeder_monday", status: "done", uploaded_by: AGENTE, error_message: null }]);
    }
    if (u.includes("toolbar_review_queue?created_at=gte.") && u.includes("select=source,domain")) {
      return altasFalla ? resp({ message: "timeout" }, { status: 500 }) : resp([]);
    }
    return resp([]);
  };
  return reg;
}

test("el boletín: la cola sin leer no se pinta como 'procesó 0', y las altas sin leer no vuelven reactivadas a todas", async () => {
  boletinFalso({ colaFalla: true });
  const t1 = (await W._boletinPorSeccion("token-falso")).join("\n");
  ok(!/boletín por sección falló/.test(t1), t1);
  match(t1, /No se pudo leer la cola/);
  doesNotMatch(t1, /La cola procesó/, "con el código anterior decía 'La cola procesó 0 … llegaron 0' debajo del aviso");

  const reg = boletinFalso({ altasFalla: true });
  const t2 = (await W._boletinPorSeccion("token-falso")).join("\n");
  ok(!/boletín por sección falló/.test(t2), t2);
  match(t2, /\? alta\(s\) en Prospects: no se pudieron leer/);
  doesNotMatch(t2, /\b0 alta\(s\)/);
  match(t2, /llegaron a Prospects 1 \(no se pudieron leer las altas/);
  doesNotMatch(t2, /ya estaban y se reactivaron/);
  ok(!reg.some(r => r.u.includes("toolbar_review_queue?domain=in.(")), "sin las altas no se buscan 'reactivadas' en Prospects");
});
