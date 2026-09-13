// Las lecturas de más de 1.000 filas (ronda final del 13/09, grupo lecturas_1000).
//
// PostgREST devuelve como máximo 1.000 filas por pedido, pida lo que pida el `limit=`. El 04/09 se creó
// `_traerTodo` para paginar, pero quedaban 37 lecturas en el worker y 8 en la extensión con límites de
// 1.500 a 50.000 que creían traer todo y traían un pedazo sin orden. Las que protegen algo fallaban
// ABIERTO sin saberlo:
//   · el agente y los reciclables leían "contactados" cortados (I16) → se re-trabajaba a contactados;
//   · reabrirLeadsRebotados leía 1.000 rebotes y 1.000 leads (I11) → un lead con todos sus emails
//     rebotados seguía "con email" en el pool, y una lectura caída latía verde;
//   · la lista de rebotes ya procesados (bounce_seen) → un rebote visto dos veces reenvía dos veces;
//   · los destinatarios reales de 90 días del scan de rebotes → una dirección muerta quedaba libre;
//   · Prospects en la extensión → con más de 1.000 pendientes los MBs veían los 1.000 más nuevos.
// Y los informes (parte, resumen del MB, métricas, panel admin) mostraban un 0 o un 1.000 donde había
// una consulta caída o un corte.
//
// Cada test fija la regla con el código real: el falso de la base contesta como PostgREST (con Range,
// esa página; sin Range, las primeras 1.000), así que con el código anterior todos estos fallan.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as acorn from "acorn";
import * as walk from "acorn-walk";
import { cargarWorker } from "./_worker-exportado.mjs";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(AQUI, "..", "..");
const leer = (rel) => fs.readFileSync(path.join(RAIZ, rel), "utf8");
const worker = leer("auto-prospector/index.js");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};

const resp = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: () => null },
  json: async () => body,
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
// Como PostgREST: con Range, esa página (1.000 como mucho); sin Range, las primeras 1.000 — el tope.
const pagina = (filas, opts, { romperDesde = null } = {}) => {
  const rango = String(opts?.headers?.Range || "");
  const desde = rango ? parseInt(rango.split("-")[0], 10) : 0;
  if (romperDesde != null && desde >= romperDesde) return resp({ message: "caído" }, { status: 503 });
  const hasta = rango ? parseInt(rango.split("-")[1], 10) : desde + 999;
  return resp(filas.slice(desde, Math.min(hasta + 1, desde + 1000)));
};
// Anota cada pedido y contesta por la primera regla que coincide; lo que no reconoce, lista vacía.
const base = (pedidos, reglas) => async (url, opts = {}) => {
  const u = String(url), m = (opts.method || "GET").toUpperCase();
  pedidos.push({ u, m, b: String(opts.body || ""), rango: String(opts.headers?.Range || "") });
  for (const [cond, contestar] of reglas) if (cond(u, m)) return contestar(u, m, opts);
  return resp([]);
};
const latidoDe = (pedidos, job) => pedidos
  .filter(p => p.m === "POST" && p.u.includes("/rest/v1/toolbar_health"))
  .map(p => JSON.parse(p.b)).filter(b => b.job === job).pop();

// ── 1. _traerTodo: una ventana más grande que el tope no vuelve con cara de entera ─────────────────
test("_traerTodo con `entero` devuelve null si llega al tope sin ver el final de la tabla", async () => {
  const w = await cargarWorker(["_traerTodo"], { fetchFalso: true });
  const filas = Array.from({ length: 2500 }, (_, i) => ({ id: i }));
  globalThis.__fetchFalso = base([], [[(u) => u.includes("toolbar_x"), (_u, _m, o) => pagina(filas, o)]]);
  const url = "https://base/rest/v1/toolbar_x?select=id&order=id";
  strictEqual((await w._traerTodo(url, {}, { max: 2000 })).length, 2000, "sin `entero` sigue siendo un tope, como antes");
  strictEqual(await w._traerTodo(url, {}, { max: 2000, entero: true }), null, "2.000 de 2.500 no es la lista");
  strictEqual((await w._traerTodo(url, {}, { max: 5000, entero: true })).length, 2500);
});

// ── 2. reabrirLeadsRebotados (I11) ─────────────────────────────────────────────────────────────────
const rebotes = (n) => Array.from({ length: n }, (_, i) => ({ email: `persona${i}@medio${i}.com`, evidencia: "rebote_smtp", fuente: "scrape" }));
const esReabrir = (p, id) => p.m === "PATCH" && p.u.includes(`toolbar_review_queue?id=eq.${id}`) && p.b.includes("todos_los_emails_rebotaron");
const correrReabrir = async ({ listaRebotes, leads, romperRebotesDesde = null, romperLeadsDesde = null }) => {
  const w = await cargarWorker(["reabrirLeadsRebotados", "_CADENCIA_MEM", "_bouncedCache"], { fetchFalso: true });
  // lib/email.js es UN solo módulo para todas las copias del worker: la lista de rebotes que cargó el
  // escenario anterior sigue fresca 5 minutos y taparía la lectura caída de éste.
  w._bouncedCache.ts = 0;
  w._bouncedCache.set = new Set();
  const pedidos = [];
  globalThis.__fetchFalso = base(pedidos, [
    [(u, m) => m === "GET" && u.includes("toolbar_bounced_emails"), (_u, _m, o) => pagina(listaRebotes, o, { romperDesde: romperRebotesDesde })],
    [(u, m) => m === "GET" && u.includes("toolbar_review_queue?status=eq.pending&emails=neq."), (_u, _m, o) => pagina(leads, o, { romperDesde: romperLeadsDesde })],
  ]);
  await w.reabrirLeadsRebotados("t");
  return { w, pedidos };
};

test("I11: el lead cuyo único email rebotó en la fila 1.003 de rebotes se reabre", async () => {
  const { pedidos } = await correrReabrir({
    listaRebotes: rebotes(1005),
    leads: [{ id: 77, domain: "medio1003.com", emails: ["persona1003@medio1003.com"], email_sources: {} }],
  });
  ok(pedidos.some(p => esReabrir(p, 77)), "con un solo pedido la base devolvía 1.000 rebotes y este no se reabría");
  ok(pedidos.filter(p => p.u.includes("toolbar_bounced_emails")).every(p => !/limit=/.test(p.u)));
  strictEqual(latidoDe(pedidos, "reabrir_rebotados")?.last_status, "ok");
});

test("I11: el lead en el puesto 1.100 del pool también se mira", async () => {
  const leads = Array.from({ length: 1200 }, (_, i) => ({ id: i + 1, domain: `lead${i}.com`, emails: [`vivo${i}@lead${i}.com`], email_sources: {} }));
  leads[1100].emails = ["persona5@medio5.com"];
  const { pedidos } = await correrReabrir({ listaRebotes: rebotes(10), leads });
  ok(pedidos.some(p => esReabrir(p, 1101)), "los pendientes se leían con limit=1500 sin orden: el 1.101 no llegaba");
  strictEqual(pedidos.filter(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue?id=eq.")).length, 1, "a los que tienen un email vivo no se los toca");
  ok(pedidos.filter(p => p.u.includes("toolbar_review_queue?status=eq.pending")).every(p => /order=id/.test(p.u) && !/limit=/.test(p.u)));
});

test("I11: sin la lista de rebotes o sin el pool enteros no se juzga, late en rojo y devuelve el turno", async () => {
  {
    const { w, pedidos } = await correrReabrir({
      listaRebotes: rebotes(1005), romperRebotesDesde: 1000,
      leads: [{ id: 5, domain: "medio2.com", emails: ["persona2@medio2.com"], email_sources: {} }],
    });
    ok(!pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue?id=eq.")), "con media lista no se escribe nada");
    const l = latidoDe(pedidos, "reabrir_rebotados");
    deepStrictEqual([l?.last_status, /rebotes/.test(l?.last_detail || "")], ["fail", true], `una lectura caída latía 'no hay rebotes registrados' en verde: ${JSON.stringify(l)}`);
    strictEqual(w._CADENCIA_MEM.has("reabrir_rebotados"), false, "el turno se devuelve: reintenta en la vuelta siguiente, no en 6 horas");
  }
  {
    const leads = Array.from({ length: 1200 }, (_, i) => ({ id: i + 1, domain: `lead${i}.com`, emails: [`persona${i % 10}@medio${i % 10}.com`], email_sources: {} }));
    const { w, pedidos } = await correrReabrir({ listaRebotes: rebotes(10), leads, romperLeadsDesde: 1000 });
    ok(!pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue?id=eq.")), "con medio pool no se reabre a nadie");
    const l = latidoDe(pedidos, "reabrir_rebotados");
    deepStrictEqual([l?.last_status, /pool/.test(l?.last_detail || "")], ["fail", true], JSON.stringify(l));
    strictEqual(w._CADENCIA_MEM.has("reabrir_rebotados"), false);
  }
});

// ── 3. Los rebotes ya procesados: enteros o no se procesa ──────────────────────────────────────────
test("bounce_seen se lee de a páginas y, si no se pudo, el scan no procesa la tanda", async () => {
  const vistos = Array.from({ length: 1500 }, (_, i) => ({ msg_id: `m${String(i).padStart(5, "0")}` }));
  {
    const w = await cargarWorker(["loadSeenBounceMsgs"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = base(pedidos, [[(u) => u.includes("toolbar_bounce_seen"), (_u, _m, o) => pagina(vistos, o)]]);
    const set = await w.loadSeenBounceMsgs("t");
    ok(set && set.has("m01200"), "con limit=20000 la base devolvía 1.000 y el mensaje 1.201 se volvía a procesar (y a reenviar)");
    ok(pedidos.every(p => /seen_at=gte\./.test(p.u) && /order=msg_id/.test(p.u) && !/limit=/.test(p.u)), pedidos[0]?.u);
  }
  {
    const w = await cargarWorker(["loadSeenBounceMsgs"], { fetchFalso: true });
    globalThis.__fetchFalso = base([], [[(u) => u.includes("toolbar_bounce_seen"), (_u, _m, o) => pagina(vistos, o, { romperDesde: 1000 })]]);
    strictEqual(await w.loadSeenBounceMsgs("t"), null, "media lista no es 'estos ya se procesaron'");
  }
  const scan = cuerpoDe("scanBouncesForUser");
  const iCarga = scan.indexOf("await loadSeenBounceMsgs(token)");
  const iCorte = scan.search(/if \(!seen\) \{[^\n]*return 0; \}/);
  const iFiltro = scan.indexOf("allIds.filter(id => !seen.has(id))");
  ok(iCarga >= 0 && iCorte > iCarga && iFiltro > iCorte, "sin la lista de vistos, scanBouncesForUser corta antes de filtrar y procesar");
});

// ── 4. Drenajes que no avanzaban: lo que no se toca queda adelante para siempre ────────────────────
test("revisarDescartesCondicionales libera al condicional que está detrás de 1.000 descartes definitivos", async () => {
  const w = await cargarWorker(["revisarDescartesCondicionales"], { fetchFalso: true });
  const filas = Array.from({ length: 1200 }, (_, i) => ({ id: i + 1, error_message: i === 1150 ? "geo_not_priority: IN" : "not_publisher: sin_ads_txt" }));
  const pedidos = [];
  globalThis.__fetchFalso = base(pedidos, [[(u, m) => m === "GET" && u.includes("toolbar_csv_queue?status=eq.skipped"), (_u, _m, o) => pagina(filas, o)]]);
  await w.revisarDescartesCondicionales("t");
  ok(pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_csv_queue?id=in.(1151)") && p.b.includes("expired")),
    "con limit=1500 la base devolvía los 1.000 más viejos, todos sin ads.txt, y la revisión no liberaba nunca a nadie");
});

test("purgarColaVieja borra la fila con respaldo que cae después de la 1.000, y una lectura caída no late 'nada viejo'", async () => {
  const filas = Array.from({ length: 1200 }, (_, i) => ({ id: i + 1, domain: `viejo${i}.com` }));
  {
    const w = await cargarWorker(["purgarColaVieja"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = base(pedidos, [
      [(u, m) => m === "GET" && u.includes("toolbar_csv_queue?status=eq.done"), (_u, _m, o) => pagina(filas, o)],
      [(u, m) => m === "GET" && u.includes("toolbar_review_queue?domain=in.("), (u) => resp(decodeURIComponent(u).includes('"viejo1100.com"') ? [{ domain: "viejo1100.com" }] : [])],
    ]);
    await w.purgarColaVieja("t");
    ok(pedidos.some(p => p.m === "DELETE" && p.u.includes("toolbar_csv_queue?id=in.(1101)")), "con limit=2000 la fila 1.101 no llegaba y la purga se estancaba");
  }
  {
    const w = await cargarWorker(["purgarColaVieja"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = base(pedidos, [[(u, m) => m === "GET" && u.includes("toolbar_csv_queue?status=eq.done"), () => resp({ message: "caído" }, { status: 500 })]]);
    await w.purgarColaVieja("t");
    ok(!/nada viejo/.test(latidoDe(pedidos, "purga_cola")?.last_detail || ""), "un 500 se latía como 'nada viejo para purgar', en verde");
  }
});

test("la atribución de AutoGoogle ve la frase que calificó aunque su dominio esté detrás de 1.000 que todavía no", async () => {
  const w = await cargarWorker(["_reconcileAutogoogleAttribution"], { fetchFalso: true });
  const ahora = new Date().toISOString();
  const filas = Array.from({ length: 1200 }, (_, i) => ({ domain: `ag${i}.com`, phrase: `frase ${i}`, injected_at: ahora }));
  const pedidos = [];
  globalThis.__fetchFalso = base(pedidos, [
    [(u, m) => m === "GET" && u.includes("toolbar_autogoogle_attribution?select="), (_u, _m, o) => pagina(filas, o)],
    [(u, m) => m === "GET" && u.includes("toolbar_review_queue?domain=in.("), (u) => resp(decodeURIComponent(u).includes('"ag1100.com"') ? [{ domain: "ag1100.com" }] : [])],
  ]);
  await w._reconcileAutogoogleAttribution("t");
  ok(pedidos.some(p => p.m === "POST" && p.u.includes("rpc/bump_keyword_yield") && p.b.includes('"p_phrase":"frase 1100"')),
    "la frase que trajo un publisher se contaba hasta 10 días tarde y podía darse por muerta");
});

test("la muestra de keywords en español trae las 1.200 de su cupo, de a tramos de 1.000", async () => {
  const w = await cargarWorker(["_muestraKeywordsDeLaBase"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = base(pedidos, [[(u) => u.includes("toolbar_keywords?lang=eq.es"), (u) => {
    const off = Number(/[?&]offset=(\d+)/.exec(u)?.[1] || 0), lim = Number(/[?&]limit=(\d+)/.exec(u)?.[1] || 1000);
    return resp(Array.from({ length: Math.min(lim, 1000) }, (_, k) => ({ phrase: `palabra${off + k}` })));
  }]]);
  const out = await w._muestraKeywordsDeLaBase("t", true);
  strictEqual(new Set(out.es).size, 1200, "con limit=1200 la base devolvía 1.000");
  ok(pedidos.every(p => Number(/[?&]limit=(\d+)/.exec(p.u)?.[1]) <= 1000));
});

// ── 5. Informes: ni 1.000 clavado ni ceros inventados ──────────────────────────────────────────────
test("el resumen diario del MB cuenta los 1.200 envíos, y si no pudo leer no le manda 'no enviaste nada'", async () => {
  const acciones = Array.from({ length: 1200 }, () => ({ action: "sent", details: {} }));
  {
    const w = await cargarWorker(["generateDailyDigestForMB"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = base(pedidos, [[(u, m) => m === "GET" && u.includes("toolbar_agent_actions?user_email=eq."), (_u, _m, o) => pagina(acciones, o)]]);
    await w.generateDailyDigestForMB("t", "sales@adeqmedia.com", "Agus");
    const notif = pedidos.find(p => p.m === "POST" && p.u.includes("toolbar_notifications"));
    strictEqual(JSON.parse(notif.b).metadata.emails_total, 1200, "con limit=2000 decía 1.000");
  }
  {
    const w = await cargarWorker(["generateDailyDigestForMB"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = base(pedidos, [[(u, m) => m === "GET" && u.includes("toolbar_agent_actions?user_email=eq."), () => resp({ message: "caído" }, { status: 503 })]]);
    const err = await w.generateDailyDigestForMB("t", "sales@adeqmedia.com", "Agus").then(() => null, e => e);
    ok(err, "una lectura caída tiene que llegar al llamador como error");
    ok(!pedidos.some(p => p.m === "POST" && p.u.includes("toolbar_notifications")), "no se manda 'No enviaste emails ayer' por un 503");
  }
});

// ── 6. El agente: contactados y rebotes de la cohorte ─────────────────────────────────────────────
test("el agente lee los contactados de 30 días enteros o no los usa, y la cohorte cruza contra la lista entera de rebotes", () => {
  const ciclo = cuerpoDe("runAgentCycle");
  ok(/_traerTodo\(\s*`\$\{SUPABASE_URL\}\/rest\/v1\/toolbar_sendtrack\?send_date=gte\.\$\{_corte\}&select=domain&order=domain`[\s\S]{0,200}?entero: true/.test(ciclo),
    "contactados de 30 días: de a páginas, con orden, y null si no se pudo leer entera (decide el guard por dominio)");
  ok(/if \(Array\.isArray\(_f\)\) _contactados30d = new Set/.test(ciclo), "sólo una lectura entera arma el filtro en memoria");
  ok(!/toolbar_bounced_emails\?select=email&\$\{EVIDENCIA_BLOQUEA\}/.test(ciclo), "la cohorte ya no lee su propio pedazo de rebotes");
  ok(/if \(!_rs \|\| !\(await _rebotesListosParaJuzgar\(token\)\)\) throw new Error\("no pude medir"\)/.test(ciclo)
    && /_dest\.filter\(e => isBouncedSync\(e\)\)/.test(ciclo), "sin lista de rebotes buena, 'no pude medir' (avisa, no frena)");
});

// ── 7. La extensión ────────────────────────────────────────────────────────────────────────────────
test("extensión: traerTodo pagina con reloj por página y devuelve null ante una página caída o un tope", async () => {
  const S = await import("../../modules/supabase.js");
  strictEqual(typeof S.traerTodo, "function", "la extensión necesita su equivalente de _traerTodo");
  const filas = Array.from({ length: 2500 }, (_, i) => ({ id: i }));
  const pedidos = [];
  const fetchImpl = async (u, o) => { pedidos.push({ u, o }); return pagina(filas, o); };
  const todo = await S.traerTodo("https://base/rest/v1/t?select=id&order=id&limit=5000", { apikey: "k" }, { fetchImpl });
  strictEqual(todo.length, 2500);
  ok(pedidos.every(p => p.o.signal && p.o.headers.Range && p.o.headers.apikey === "k" && !/limit=/.test(p.u)), "cada página con reloj y sin el limit engañoso");
  strictEqual(await S.traerTodo("https://base/rest/v1/t?select=id", {}, { fetchImpl: async (u, o) => pagina(filas, o, { romperDesde: 1000 }) }), null);
  strictEqual(await S.traerTodo("https://base/rest/v1/t?select=id", {}, { fetchImpl, max: 2000, entero: true }), null);
});

test("extensión: Prospects trae el pool entero, no los 1.000 más nuevos", async () => {
  const S = await import("../../modules/supabase.js");
  const pool = Array.from({ length: 1500 }, (_, i) => ({ id: i + 1, domain: `lead${i}.com`, status: "pending" }));
  const antes = globalThis.fetch;
  const pedidos = [];
  try {
    globalThis.fetch = async (u, o) => { pedidos.push(String(u)); return pagina(pool, o); };
    const rows = await S.fetchReviewQueue("tk");
    strictEqual(rows.length, 1500, "con limit=3000 la base devolvía 1.000 y 500 leads no aparecían nunca en Prospects");
    strictEqual(rows.total, 1500);
    ok(pedidos.every(u => /order=created_at\.desc,id\.desc/.test(u) && !/limit=/.test(u)), pedidos[0]);
  } finally { globalThis.fetch = antes; }
});

// ── 8. La regla, para todo el código: ningún pedido a PostgREST pide más de 1.000 ─────────────────
// Un `limit=` fijo mayor a 1.000 en cualquier literal es un error: la base corta en 1.000 y el código
// cree que leyó todo. Lo que necesita más va por `_traerTodo` (worker) o `traerTodo` (extensión), que
// además tiran el `limit`. Un `limit=${expresión}` hacia PostgREST tiene que estar en esta lista, con
// la cota verificada contra el código: si alguien cambia la declaración, el test lo obliga a revisar.
const constante = (src, nombre) => {
  const m = new RegExp(`const ${nombre}\\s*=\\s*([\\d_]+);`).exec(src);
  ok(m, `no encontré const ${nombre}`);
  return Number(m[1].replace(/_/g, ""));
};
const capturado = (fn, re, que) => { const m = re.exec(fn); ok(m, `${que}: la declaración cambió, revisar la cota`); return Number(m[1]); };
const DINAMICOS = {
  "auto-prospector/index.js|rellenarWaitingPool|room": ({ src, fn }) => (ok(/const room = Math\.max\(0, WAITING_POOL_CAP - /.test(fn)), constante(src, "WAITING_POOL_CAP")),
  "auto-prospector/index.js|getDailyGlobalCounters|_room": ({ src, fn }) => (ok(/const _room = Math\.max\(0, WAITING_POOL_CAP - /.test(fn)), constante(src, "WAITING_POOL_CAP")),
  "auto-prospector/index.js|promoteWaitlist|limit": ({ src, fn }) => (ok(/const slots = CSV_QUEUE_HARD_CAP - pendingCount;/.test(fn) && /traer\([^\n]*, slots\)/.test(fn)), constante(src, "CSV_QUEUE_HARD_CAP")),
  "auto-prospector/index.js|_drainBacklog|Math.min(room * 3, 600)": () => 600,
  "auto-prospector/index.js|_muestraKeywordsDeLaBase|Math.min(1000, cupo - hecho)": () => 1000,
  "auto-prospector/index.js|barridoNoPublisher|LOTE": ({ fn }) => capturado(fn, /const LOTE = Math\.min\((\d+),/, "barridoNoPublisher"),
  "auto-prospector/index.js|purgeByUrlOnly|BATCH": ({ fn }) => capturado(fn, /const BATCH = (\d+);/, "purgeByUrlOnly"),
  "auto-prospector/index.js|sweepBlockedFromProspects|BATCH": ({ fn }) => capturado(fn, /const BATCH = (\d+);/, "sweepBlockedFromProspects"),
  "auto-prospector/index.js|auditarEmailsDelPool|AUDITORIA_EMAILS_LOTE": ({ src }) => constante(src, "AUDITORIA_EMAILS_LOTE"),
  "auto-prospector/index.js|polishPool|POLISH_BATCH": ({ src }) => constante(src, "POLISH_BATCH"),
  "auto-prospector/index.js|runProspectSimilarExpansion|SIMILAR_EXP_BATCH": ({ src }) => constante(src, "SIMILAR_EXP_BATCH"),
  "auto-prospector/index.js|getNextCsvItem|lim": ({ fn }) => {
    ok(/const pedir = async \(extra, lim = 1\)/.test(fn));
    return Math.max(1, ...[...fn.matchAll(/pedir\([^\n]*?, (\d+)\)/g)].map(m => Number(m[1])));
  },
  "auto-prospector/index.js|runSession|_NSEM": ({ fn }) => capturado(fn, /const _NSEM = (\d+);/, "runSession"),
  "auto-prospector/index.js|reconciliarHuerfanosFrozen|HUERFANOS_FROZEN_LOTE": ({ src }) => constante(src, "HUERFANOS_FROZEN_LOTE"),
  "auto-prospector/index.js|runAgentCycle|POOL_SIZE": ({ fn }) => capturado(fn, /const POOL_SIZE = (\d+);/, "runAgentCycle"),
  "auto-prospector/index.js|_cleanupPool|CLEANUP_LOTE": ({ src }) => constante(src, "CLEANUP_LOTE"),
  // Integración (13/09, reintento_crm R5): pide los ids de a `lote` dominios con domain=in.(...), y domain es
  // único en toolbar_review_queue, así que nunca vuelven más filas que `lote`. Cota: el valor por defecto y
  // cualquier `lote:` que pase un llamador.
  "auto-prospector/index.js|_sacarBloqueadosDeProspects|lote": ({ src, fn }) => Math.max(
    capturado(fn, /lote = (\d+) \} = \{\}\)/, "_sacarBloqueadosDeProspects"),
    ...[...src.matchAll(/_sacarBloqueadosDeProspects\([^\n]*\blote: (\d+)/g)].map(m => Number(m[1]))),
  "modules/auditLog.js|fetchAuditLog|limit": ({ fn }) => capturado(fn, /\{ limit = (\d+) \}/, "fetchAuditLog"),
  "modules/supabase.js|fetchNotifications|limit": ({ fn }) => Math.max(capturado(fn, /\{ limit = (\d+) \}/, "fetchNotifications"),
    ...[...leer("popup/popup.js").matchAll(/fetchNotifications\([^\n]*\{ limit: (\d+) \}/g)].map(m => Number(m[1]))),
  "modules/supabase.js|_demoteAgentPending|howMany": ({ src }) => {
    ok(/_demoteAgentPending\(accessToken, quiereEnPending - pendingSlots\)/.test(src)
      && /const quiereEnPending = Math\.min\(domains\.length, mbLaneFree\);/.test(src)
      && /const mbLaneFree = Math\.max\(0, MB_PRIORITY_SLOTS - /.test(src), "_demoteAgentPending: la cota cambió");
    return Number(/const MB_PRIORITY_SLOTS\s*=\s*(\d+);/.exec(src)[1]);
  },
  "modules/supabase.js|getCsvQueueHistory|limit": ({ fn }) => capturado(fn, /getCsvQueueHistory\(accessToken, limit = (\d+),/, "getCsvQueueHistory"),
};

test("ningún pedido a la base pide más de 1.000 filas en un solo pedido (worker, lib, módulos, popup, background)", () => {
  const dir = (rel, filtro = (f) => f.endsWith(".js")) => fs.readdirSync(path.join(RAIZ, rel)).filter(filtro).map(f => `${rel}/${f}`);
  const archivos = ["auto-prospector/index.js", "auto-prospector/discovery.js", "auto-prospector/templates.js",
    ...dir("auto-prospector/lib"), ...dir("modules"), "popup/popup.js", ...dir("background")];
  const fuera = [];
  const vistos = new Set();
  for (const rel of archivos) {
    const src = leer(rel);
    const ast = acorn.parse(src, { ecmaVersion: "latest", sourceType: "module", locations: true });
    walk.fullAncestor(ast, (node, _s, anc) => {
      const donde = `${rel}:${node.loc.start.line}`;
      if (node.type === "Literal" && typeof node.value === "string") {
        const m = /[?&]limit=(\d+)/.exec(node.value);
        if (m && Number(m[1]) > 1000) fuera.push(`${donde} limit=${m[1]}`);
        return;
      }
      if (node.type !== "TemplateLiteral") return;
      const quasis = node.quasis.map(q => q.value.cooked ?? q.value.raw);
      const aLaBase = quasis.join("").includes("/rest/v1/");
      quasis.forEach((q, i) => {
        for (const m of q.matchAll(/[?&]limit=(\d+)/g)) if (Number(m[1]) > 1000) fuera.push(`${donde} limit=${m[1]}`);
        if (!aLaBase || !/[?&]limit=$/.test(q) || !node.expressions[i]) return;
        const decl = [...anc].reverse().find(a => /Function/.test(a.type) && a.id);
        const expr = src.slice(node.expressions[i].start, node.expressions[i].end);
        const clave = `${rel}|${decl?.id?.name}|${expr}`;
        const cota = DINAMICOS[clave];
        if (!cota) { fuera.push(`${donde} limit=\${${expr}} (sin cota conocida: ${clave})`); return; }
        vistos.add(clave);
        const n = cota({ src, fn: decl ? src.slice(decl.start, decl.end) : src });
        if (!(n <= 1000)) fuera.push(`${donde} limit=\${${expr}} puede valer ${n}`);
      });
    });
  }
  deepStrictEqual(fuera, [], "PostgREST corta en 1.000: el pedido cree que leyó todo y leyó un pedazo. Usar _traerTodo / traerTodo con orden");
  deepStrictEqual(Object.keys(DINAMICOS).filter(k => !vistos.has(k)), [], "una entrada de la lista ya no existe en el código: sacarla para que la lista siga siendo la verdad");
});

// ── 9. Una columna que la tabla no tiene es una lectura caída ─────────────────────────────────────
// Toda lectura de a páginas lleva `order=` para que las páginas no repitan ni salteen filas. Pero la
// columna tiene que existir: PostgREST contesta 400 (42703) a una desconocida, `_traerTodo` devuelve null
// y lo que dependía de la lista no corre. vigilarReputacion ordenaba toolbar_bounced_emails por `id`, que
// esa tabla no tiene (su clave es `email`, sql/2026-05-12_bounced_emails.sql): se cortaba antes de la
// alerta de rebote y del latido 'reputacion', y el parte lo mostraba como job atrasado. npm test daba
// verde porque ningún test corría la función y la regla de arriba sólo mira `limit=`.
const esquemaSql = () => {
  const creadas = new Set();
  const columnas = new Map();
  const agregar = (t, c) => { if (!columnas.has(t)) columnas.set(t, new Set()); columnas.get(t).add(c); };
  const dirSql = path.join(RAIZ, "sql");
  for (const f of fs.readdirSync(dirSql).filter(f => f.endsWith(".sql"))) {
    const sql = fs.readFileSync(path.join(dirSql, f), "utf8").replace(/--[^\n]*/g, "");
    for (const m of sql.matchAll(/create\s+table\s+(?:if\s+not\s+exists\s+)?(?:public\.)?"?(\w+)"?\s*\(/gi)) {
      const tabla = m[1].toLowerCase();
      creadas.add(tabla);
      if (!columnas.has(tabla)) columnas.set(tabla, new Set());
      const items = [];
      let prof = 1, item = "";
      for (let j = m.index + m[0].length; j < sql.length && prof; j++) {
        const ch = sql[j];
        if (ch === "(") prof++;
        if (ch === ")") prof--;
        if (!prof || (ch === "," && prof === 1)) { items.push(item); item = ""; } else item += ch;
      }
      for (const it of items) {
        const nombre = /^\s*"?(\w+)"?/.exec(it)?.[1]?.toLowerCase();
        if (nombre && !/^(constraint|primary|unique|foreign|check|exclude|like)$/.test(nombre)) agregar(tabla, nombre);
      }
    }
    for (const m of sql.matchAll(/alter\s+table\s+(?:if\s+exists\s+)?(?:only\s+)?(?:public\.)?"?(\w+)"?([^;]*);/gi)) {
      const tabla = m[1].toLowerCase();
      for (const a of m[2].matchAll(/\badd\s+(?:column\s+)?(?:if\s+not\s+exists\s+)?"?(\w+)"?/gi)) {
        if (!/^(constraint|primary|unique|foreign|check)$/i.test(a[1])) agregar(tabla, a[1].toLowerCase());
      }
      for (const a of m[2].matchAll(/\brename\s+column\s+"?\w+"?\s+to\s+"?(\w+)"?/gi)) agregar(tabla, a[1].toLowerCase());
    }
  }
  return { creadas, columnas };
};
const columnasDeOrden = (valor) => String(valor).split(",").map(x => x.split(/->|\./)[0].trim().toLowerCase()).filter(Boolean);
// Las columnas que nombra un pedido: select, order y los filtros (`col=op.valor`).
const columnasPedidas = (u) => {
  const out = [];
  for (const par of String(u).slice(String(u).indexOf("?") + 1).split("&")) {
    const i = par.indexOf("=");
    if (i < 0) continue;
    const k = par.slice(0, i), v = decodeURIComponent(par.slice(i + 1));
    if (k === "select") out.push(...v.split(",").map(c => c.split("->")[0].trim().toLowerCase()));
    else if (k === "order") out.push(...columnasDeOrden(v));
    else if (!/^(limit|offset|and|or|on_conflict|columns)$/.test(k)) out.push(k.split("->")[0].toLowerCase());
  }
  return out.filter(Boolean);
};

test("vigilarReputacion avisa el rebote y late aunque los rebotes de verdad estén pasando la fila 1.000", async () => {
  const { columnas } = esquemaSql();
  // Las columnas de la tabla según sql/ más `detalle`, que se agregó desde el panel (GET a la base, 13/09).
  const cols = new Set([...columnas.get("toolbar_bounced_emails"), "detalle"]);
  const w = await cargarWorker(["vigilarReputacion"], { fetchFalso: true });
  const envios = Array.from({ length: 1500 }, (_, i) => ({ user_email: "sales@adeqmedia.com", email_to: `c${i}@medio${i}.com` }));
  // 1.050 veredictos de MillionVerifier (envíos evitados, no rebotes) y detrás 150 rebotes del SMTP.
  const tabla = [
    ...Array.from({ length: 1050 }, (_, i) => ({ email: `mv${i}@otro${i}.com`, reason: "mv_undeliverable", detalle: "", evidencia: "verificador" })),
    ...Array.from({ length: 150 }, (_, i) => ({ email: `c${i}@medio${i}.com`, reason: "550 No such user", detalle: "", evidencia: "rebote_smtp" })),
  ];
  const pedidos = [];
  globalThis.__fetchFalso = base(pedidos, [
    // Integración (13/09): desde agente_sueltos los envíos que miden el rebote incluyen el 2º email
    // (ACCION_ENVIO_PARA_REBOTE = in.(sent,secondary_sent)); el ruteador acepta las dos formas.
    [(u, m) => m === "GET" && /toolbar_agent_actions\?action=(eq\.sent|in\.\(sent,secondary_sent\))/.test(u), (_u, _m, o) => pagina(envios, o)],
    [(u, m) => m === "GET" && u.includes("toolbar_bounced_emails?"), (u, _m, o) => {
      const falta = columnasPedidas(u).find(c => !cols.has(c));
      if (falta) return resp({ code: "42703", message: `column toolbar_bounced_emails.${falta} does not exist` }, { status: 400 });
      return pagina(tabla, o);
    }],
  ]);
  await w.vigilarReputacion("t");

  const lecturas = pedidos.filter(p => p.u.includes("toolbar_bounced_emails"));
  ok(lecturas.length >= 2 && lecturas.every(p => /[?&]order=email\b/.test(p.u) && !/limit=/.test(p.u)), `los rebotes, de a páginas y por su clave: ${lecturas[0]?.u}`);
  const alerta = pedidos.filter(p => p.m === "POST" && p.u.includes("toolbar_notifications")).map(p => JSON.parse(p.b)).find(b => /reputacion-dominio/.test(b.dedup_key || ""));
  ok(alerta && /Rebote del 10\.0% en 7 días/.test(alerta.title), `con order=id la base contestaba 400 y el rebote del 10% no se avisaba: ${JSON.stringify(alerta)}`);
  const l = latidoDe(pedidos, "reputacion");
  deepStrictEqual([l?.last_status, /rebote 10\.00% sobre 1500 envíos/.test(l?.last_detail || "")], ["ok", true], `sin latido el parte lo muestra atrasado: ${JSON.stringify(l)}`);
});

test("ninguna lectura ordena por una columna que su tabla no tiene, y los rebotes de a páginas van por su clave", () => {
  const { creadas, columnas } = esquemaSql();
  const reb = columnas.get("toolbar_bounced_emails");
  ok(reb?.has("email") && reb.has("evidencia") && reb.has("bounced_at") && !reb.has("id"), `sql/ no se leyó como se esperaba: ${[...(reb || [])]}`);
  const dir = (rel) => fs.readdirSync(path.join(RAIZ, rel)).filter(f => f.endsWith(".js")).map(f => `${rel}/${f}`);
  const archivos = ["auto-prospector/index.js", "auto-prospector/discovery.js", "auto-prospector/templates.js",
    ...dir("auto-prospector/lib"), ...dir("modules"), "popup/popup.js", ...dir("background")];
  const fuera = [];
  let revisadas = 0;
  for (const rel of archivos) {
    const ast = acorn.parse(leer(rel), { ecmaVersion: "latest", sourceType: "module", locations: true });
    walk.fullAncestor(ast, (node, _s, anc) => {
      let txt;
      if (node.type === "Literal" && typeof node.value === "string") txt = node.value;
      else if (node.type === "TemplateLiteral") txt = node.quasis.map(q => q.value.cooked ?? q.value.raw).join(" ");
      else return;
      const m = /\/rest\/v1\/(\w+)\?([\s\S]*)$/.exec(txt);
      if (!m) return;
      const donde = `${rel}:${node.loc.start.line}`;
      const tablaUrl = m[1].toLowerCase();
      const orden = /(?:^|&)order=([^& \s#]+)/.exec(m[2])?.[1];
      const padre = anc[anc.length - 2];
      const deAPaginas = padre?.type === "CallExpression" && padre.arguments[0] === node
        && /^_?traerTodo$/.test(padre.callee.name || padre.callee.property?.name || "");
      if (tablaUrl === "toolbar_bounced_emails" && deAPaginas && !(orden && columnasDeOrden(orden)[0] === "email")) {
        fuera.push(`${donde} toolbar_bounced_emails de a páginas sin order=email (la tabla no tiene id: su clave es email)`);
      }
      if (!orden || !creadas.has(tablaUrl)) return;
      revisadas++;
      const faltan = columnasDeOrden(orden).filter(c => !columnas.get(tablaUrl).has(c));
      if (faltan.length) fuera.push(`${donde} ${tablaUrl} order=${orden}: la tabla no tiene ${faltan.join(", ")}`);
    });
  }
  ok(revisadas >= 20, `se revisaron ${revisadas} lecturas con orden a tablas de sql/: el scan dejó de ver el código`);
  deepStrictEqual(fuera, [], "PostgREST contesta 400 a una columna que no existe: _traerTodo devuelve null y lo que dependía de la lista no corre. Si la columna se agregó desde el panel, dejar su ALTER en sql/");
});

// ── 10. Las columnas REALES de cada tabla: select, order y filtros ─────────────────────────────────
// isEmailBounced (la extensión) le pedía a toolbar_bounced_emails `created_at`, que no existe (la fecha
// es `bounced_at`). La base contestaba 400 a cada consulta y la función decía "no rebotó": el guard de
// Análisis, el del lote de "Por enviar" y el de los adicionales no frenaron a nadie desde el 03/09. La
// regla de la sección 9 no lo veía: sólo mira `order=`, sólo en tablas creadas en sql/, y lee la parte de
// la URL que sigue a una interpolación pegada a lo anterior.
//
// COLUMNAS_REALES: las columnas que el código nombra en cada tabla, con su fuente. "GET 13/09" es un pedido
// de SÓLO LECTURA `?select=<columnas>&limit=0` a la base: no trae filas; contesta 200 [] si todas existen y
// 400 42703 si una no. Una columna nueva se agrega acá DESPUÉS de verificarla así (o con su CREATE/ALTER en
// sql/). `noTiene`: nombres que parecen obvios, no existen, y ya rompieron una lectura.
const COLUMNAS_REALES = {
  toolbar_agent_actions:          { cols: "id action user_email domain email_to reason template_id details created_at", fuente: "GET 13/09" },
  toolbar_autogoogle_attribution: { cols: "domain phrase injected_at", fuente: "sql/2026-07-16_autogoogle_qualified_yield.sql" },
  toolbar_autopilot_feedback:     { cols: "id user_email domain action category geo traffic_bucket reason created_at", fuente: "GET 13/09" },
  toolbar_bounce_retries:         { cols: "id domain monday_item_id mb_email original_email retry_email retry_source bounce_type attempt_number status original_action_id retry_action_id reason created_at updated_at", fuente: "sql/2026-05-13_bounce_retries.sql" },
  toolbar_bounce_seen:            { cols: "msg_id seen_at", fuente: "sql/2026-07-17_bounce_seen.sql" },
  toolbar_bounced_emails:         { cols: "email bounced_at reason original_action_id original_domain retry_attempted evidencia tipo detalle fuente", noTiene: "id created_at",
                                    fuente: "sql/2026-05-12_bounced_emails.sql y sql/2026-09-03_evidencia_rebotes.sql; tipo, detalle y fuente los escribe markEmailBounced (GET 13/09)" },
  toolbar_csv_queue:              { cols: "id domain status source uploaded_at uploaded_by processed_at error_message monday_item_id", noTiene: "updated_at", fuente: "GET 13/09" },
  toolbar_diag_descartes:         { cols: "id domain motivo comentario created_at", fuente: "GET 13/09" },
  toolbar_diag_sin_email:         { cols: "id domain motivo comentario fase created_at", fuente: "GET 13/09" },
  toolbar_historial:              { cols: "id domain media_buyer email geo is_new page_views source date created_at", fuente: "GET 13/09" },
  toolbar_keyword_yield:          { cols: "phrase searches found fresh qualified updated_at", fuente: "sql/2026-07-16_autogoogle_keyword_yield.sql; qualified: GET 13/09" },
  toolbar_keywords:               { cols: "id phrase lang", fuente: "GET 13/09" },
  toolbar_mv_results:             { cols: "id email result blocked created_at", fuente: "GET 13/09" },
  toolbar_response_tracking:      { cols: "id agent_action_id mb_email domain email_sent_to source geo category sent_at responded_at response_type created_at", fuente: "sql/2026-06-18_response_tracking.sql" },
  toolbar_review_queue:           { cols: "id domain status source traffic geo geos_all language category page_title ad_networks score emails email_sources email_found_at email_intentos email_ultimo_intento email_ultimo_motivo contact_name contact_phone pitch pitch_subject pitch_subjects monday_item_id monday_payload created_at created_by validated_at validated_by rejected_at suspect_checked_at suspect_reason suspect_reject", fuente: "GET 13/09" },
  toolbar_sendtrack:              { cols: "domain email send_date", noTiene: "id", fuente: "GET 13/09" },
  toolbar_traffic_cache:          { cols: "domain data fetched_at", noTiene: "id", fuente: "GET 13/09" },
  toolbar_usage_sessions:         { cols: "id user_email started_at ended_at duration_sec", fuente: "GET 13/09" },
};
const columnasDe = (tabla) => new Set(COLUMNAS_REALES[tabla].cols.split(/\s+/));

// Por qué alcanza cada orden de las lecturas de a páginas. Con Range, PostgREST pagina con LIMIT/OFFSET: si
// el orden no es total, dos páginas pueden repetir o saltear filas. Alcanza con una clave única (sola o
// al final), o con que quien llama use sólo el CONJUNTO de valores de la columna de orden: un empate entre
// filas con el mismo valor no puede sacar ese valor del conjunto.
const ORDEN_DE_A_PAGINAS = {
  "toolbar_agent_actions|id":                         "id: clave (toolbar_bounced_emails.original_action_id la referencia)",
  "toolbar_autogoogle_attribution|injected_at.asc,domain": "domain: clave primaria (sql/2026-07-16_autogoogle_qualified_yield.sql) desempata injected_at",
  "toolbar_autopilot_feedback|id":                    "id: clave",
  "toolbar_autopilot_feedback|created_at.desc,id":    "id desempata created_at",
  "toolbar_bounce_retries|id":                        "id: identity primary key (sql/2026-05-13_bounce_retries.sql)",
  "toolbar_bounce_seen|msg_id":                       "msg_id: clave primaria (sql/2026-07-17_bounce_seen.sql)",
  "toolbar_bounced_emails|email":                     "email: clave primaria; la tabla NO tiene id (sql/2026-05-12_bounced_emails.sql)",
  "toolbar_csv_queue|id":                             "id: clave (los PATCH y DELETE van por id=in.(...))",
  "toolbar_csv_queue|uploaded_at.asc,id":             "id desempata uploaded_at",
  "toolbar_csv_queue|uploaded_at.desc,id":            "id desempata uploaded_at",
  "toolbar_diag_descartes|created_at.desc":           "informe: sólo busca un ejemplo con comentario; un empate exacto de created_at (microsegundos) cambia el ejemplo, no una decisión",
  "toolbar_diag_sin_email|created_at.desc":           "informe: agrupa por motivo y muestra un ejemplo; mismo caso que diag_descartes",
  "toolbar_historial|id":                             "id: clave",
  "toolbar_historial|created_at.asc,id":              "id desempata created_at",
  "toolbar_keyword_yield|phrase":                     "phrase: clave primaria (sql/2026-07-16_autogoogle_keyword_yield.sql)",
  "toolbar_mv_results|id":                            "id: clave",
  "toolbar_response_tracking|id":                     "id: bigserial primary key (sql/2026-06-18_response_tracking.sql)",
  "toolbar_review_queue|id":                          "id: clave (los PATCH van por id=eq.)",
  "toolbar_review_queue|created_at.desc,id.desc":     "id desempata created_at",
  "toolbar_sendtrack|domain":                         "sin id; se escribe con merge-duplicates (una fila por dominio) y quien lee usa el conjunto de dominios",
  "toolbar_traffic_cache|domain":                     "sin id; caché por dominio y quien lee usa el conjunto de dominios",
  "toolbar_usage_sessions|id":                        "id: clave",
};

// Las columnas que nombra la parte de la URL que sigue al `?`: select (sin recursos embebidos ni alias),
// order, filtros `col=op.valor` y las columnas dentro de or=/and=. PH marca una interpolación: lo que la
// toca no se puede leer y se saltea, pero lo que sigue sí.
const PH = String.fromCharCode(1);
const _partirSelect = (s) => {
  const out = []; let prof = 0, cur = "";
  for (const ch of s) { if (ch === "(") prof++; if (ch === ")") prof--; if (ch === "," && prof === 0) { out.push(cur); cur = ""; } else cur += ch; }
  out.push(cur);
  return out;
};
const _OPERADORES = "eq|neq|gt|gte|lt|lte|like|ilike|is|in|cs|cd|ov|fts|plfts|phfts|wfts|match|imatch|isdistinct";
const columnasDeQuery = (q) => {
  const cols = [];
  for (const par of String(q).split(/[&#\s]/)) {
    const i = par.indexOf("=");
    if (i < 0) continue;
    const k = par.slice(0, i);
    let v = par.slice(i + 1);
    try { v = decodeURIComponent(v); } catch {}
    if (!k || k.includes(PH)) continue;
    if (k === "select") {
      for (const it of _partirSelect(v)) {
        if (/[()*]/.test(it) || it.includes(PH)) continue;
        const c = it.replace(/^\s*\w+:(?!:)/, "").split(/->|::/)[0].trim();
        if (c) cols.push(c);
      }
    } else if (k === "order") {
      for (const it of v.split(",")) { if (it.includes(PH)) continue; const c = it.split(/->|\./)[0].trim(); if (c) cols.push(c); }
    } else if (/^(not\.)?(or|and)$/.test(k)) {
      for (const m of v.matchAll(new RegExp(`(?:^|[(,])(\\w+)(?:->>?\\w+)*\\.(?:not\\.)?(?:${_OPERADORES})\\.`, "g"))) cols.push(m[1]);
    } else if (!/^(limit|offset|on_conflict|columns)$/.test(k)) {
      const c = k.split("->")[0].trim();
      if (/^\w+$/.test(c)) cols.push(c);
    }
  }
  return [...new Set(cols.map(c => c.toLowerCase()))];
};
const urlsDelCodigo = () => {
  const dir = (rel) => fs.readdirSync(path.join(RAIZ, rel)).filter(f => f.endsWith(".js")).map(f => `${rel}/${f}`);
  const archivos = ["auto-prospector/index.js", "auto-prospector/discovery.js", "auto-prospector/templates.js",
    ...dir("auto-prospector/lib"), ...dir("modules"), "popup/popup.js", ...dir("background")];
  const out = [];
  for (const rel of archivos) {
    const ast = acorn.parse(leer(rel), { ecmaVersion: "latest", sourceType: "module", locations: true });
    walk.fullAncestor(ast, (node, _s, anc) => {
      let txt;
      if (node.type === "Literal" && typeof node.value === "string") txt = node.value;
      else if (node.type === "TemplateLiteral") txt = node.quasis.map(q => q.value.cooked ?? q.value.raw).join(PH);
      else return;
      const m = /\/rest\/v1\/(\w+)\?([\s\S]*)$/.exec(txt);
      if (!m) return;
      const padre = anc[anc.length - 2];
      const deAPaginas = padre?.type === "CallExpression" && padre.arguments[0] === node
        && /^_?traerTodo$/.test(padre.callee.name || padre.callee.property?.name || "");
      out.push({ donde: `${rel}:${node.loc.start.line}`, tabla: m[1], query: m[2], deAPaginas,
        orden: /(?:^|&)order=([^&\s#]+)/.exec(m[2])?.[1] || "" });
    });
  }
  return out;
};
// Una base falsa que contesta como PostgREST a una columna que la tabla no tiene: 400 42703.
const baseEstricta = (pedidos, contestar) => async (url, opts = {}) => {
  const u = String(url);
  pedidos.push({ u, auth: String(opts.headers?.Authorization || "") });
  const m = /\/rest\/v1\/(\w+)\?(.*)$/.exec(u);
  if (m && COLUMNAS_REALES[m[1]]) {
    const falta = columnasDeQuery(m[2]).find(c => !columnasDe(m[1]).has(c));
    if (falta) return resp({ code: "42703", message: `column ${m[1]}.${falta} does not exist` }, { status: 400 });
  }
  return contestar(u, opts);
};

test("extensión: isEmailBounced encuentra al rebotado contra una base que rechaza columnas inexistentes, y 'no pude preguntar' no es 'no rebotó'", async () => {
  const S = await import("../../modules/supabase.js");
  const antes = globalThis.fetch;
  const rebotada = { email: "muerto@diario.com", reason: "550 No such user", bounced_at: "2026-09-10T10:00:00Z" };
  const pedidos = [];
  try {
    globalThis.fetch = baseEstricta(pedidos, (u) => resp(decodeURIComponent(u).includes("email=eq.muerto@diario.com") ? [rebotada] : []));
    const b = await S.isEmailBounced("tk", " Muerto@Diario.com ");
    deepStrictEqual([b.bounced, b.since, b.reason], [true, rebotada.bounced_at, rebotada.reason],
      `con select=...,created_at la base contestaba 400 y la función respondía "no rebotó": ${JSON.stringify(b)}`);
    const v = await S.isEmailBounced("tk", "vivo@diario.com");
    deepStrictEqual([v.bounced, Boolean(v.indeterminado)], [false, false], "se preguntó y no está: se puede usar");

    // Un error de la base, un reloj vencido o una respuesta ilegible: no se sabe, y quien llama no manda.
    for (const [que, falso] of [
      ["HTTP 503", async () => resp({ message: "caído" }, { status: 503 })],
      ["columna inexistente", async () => resp({ code: "42703" }, { status: 400 })],
      ["reloj", async () => { throw Object.assign(new Error("The operation timed out"), { name: "TimeoutError" }); }],
      ["respuesta ilegible", async () => resp({ no: "es una lista" })],
    ]) {
      globalThis.fetch = falso;
      const r = await S.isEmailBounced("tk", "x@diario.com");
      ok(r.indeterminado === true && r.bounced !== false && r.bounced !== true && r.motivo, `${que}: ${JSON.stringify(r)}`);
    }

    // Un token vencido se renueva una vez; sin sesión, tampoco se sabe.
    const tokens = [];
    let n = 0;
    globalThis.fetch = async (_u, o) => { tokens.push(o.headers.Authorization); return ++n === 1 ? resp({ message: "JWT expired" }, { status: 401 }) : resp([]); };
    const r = await S.isEmailBounced("viejo", "x@diario.com", { renovarToken: async () => "nuevo" });
    deepStrictEqual([r.bounced, tokens], [false, ["Bearer viejo", "Bearer nuevo"]]);
    ok((await S.isEmailBounced("", "x@diario.com")).indeterminado, "sin token no se puede preguntar");
  } finally { globalThis.fetch = antes; }
});

test("toda URL a una tabla de la lista nombra columnas que la tabla tiene, y toda lectura de a páginas va a una tabla de la lista", () => {
  const urls = urlsDelCodigo();
  const fuera = [];
  let revisadas = 0;
  for (const u of urls) {
    const t = COLUMNAS_REALES[u.tabla];
    if (!t) {
      if (u.deAPaginas) fuera.push(`${u.donde} ${u.tabla}: lectura de a páginas a una tabla sin lista de columnas — agregarla a COLUMNAS_REALES con su fuente`);
      continue;
    }
    revisadas++;
    const faltan = columnasDeQuery(u.query).filter(c => !columnasDe(u.tabla).has(c));
    if (faltan.length) fuera.push(`${u.donde} ${u.tabla}: ${faltan.join(", ")} no está entre sus columnas (${t.fuente})`);
  }
  ok(revisadas >= 150, `se revisaron ${revisadas} URLs: el scan dejó de ver el código`);
  for (const [tabla, t] of Object.entries(COLUMNAS_REALES)) {
    for (const c of String(t.noTiene || "").split(/\s+/).filter(Boolean)) ok(!columnasDe(tabla).has(c), `${tabla} no tiene ${c}`);
  }
  deepStrictEqual(fuera, [], "PostgREST contesta 400 a una columna que no existe y la lectura se cae entera. Verificar la columna con un GET limit=0 (o su SQL) y recién ahí agregarla a COLUMNAS_REALES");
});

test("cada orden de las lecturas de a páginas tiene su justificación en ORDEN_DE_A_PAGINAS, y la tabla no tiene entradas muertas", () => {
  const fuera = [];
  const usadas = new Set();
  for (const u of urlsDelCodigo().filter(x => x.deAPaginas && x.orden)) {
    const clave = `${u.tabla}|${u.orden}`;
    if (ORDEN_DE_A_PAGINAS[clave]) usadas.add(clave);
    else fuera.push(`${u.donde} ${clave}: sin justificación (¿clave única al final, o sólo se usa el conjunto?)`);
  }
  deepStrictEqual(fuera, []);
  deepStrictEqual(Object.keys(ORDEN_DE_A_PAGINAS).filter(k => !usadas.has(k)), [], "una entrada ya no se usa: sacarla");
});

test("todo llamador de isEmailBounced frena si no se pudo preguntar, y el lote de 'Por enviar' se corta", () => {
  const dir = (rel) => fs.readdirSync(path.join(RAIZ, rel)).filter(f => f.endsWith(".js")).map(f => `${rel}/${f}`);
  const fuera = [];
  let llamadas = 0;
  for (const rel of ["popup/popup.js", ...dir("modules"), ...dir("background")]) {
    const src = leer(rel);
    for (const m of src.matchAll(/\bisEmailBounced\(/g)) {
      const linea = src.slice(src.lastIndexOf("\n", m.index) + 1, src.indexOf("\n", m.index));
      if (/function isEmailBounced\(/.test(linea)) continue;
      llamadas++;
      const donde = `${rel}:${src.slice(0, m.index).split("\n").length}`;
      const asig = /(?:const|let)\s+(\w+)\s*=\s*await isEmailBounced\(/.exec(linea);
      if (!asig) { fuera.push(`${donde}: el resultado no se guarda en una variable`); continue; }
      if (/\.catch\(/.test(linea)) fuera.push(`${donde}: un .catch que inventa una respuesta`);
      const despues = src.slice(m.index, m.index + 700);
      const iInd = despues.indexOf(`${asig[1]}.indeterminado`), iReb = despues.indexOf(`${asig[1]}.bounced`);
      if (iInd < 0 || (iReb >= 0 && iReb < iInd)) fuera.push(`${donde}: no mira ${asig[1]}.indeterminado antes de ${asig[1]}.bounced`);
    }
  }
  ok(llamadas >= 4, `encontré ${llamadas} llamadas`);
  deepStrictEqual(fuera, [], "isEmailBounced devuelve indeterminado cuando no pudo preguntar: tratarlo como 'no rebotó' manda a direcciones muertas");
  const popup = leer("popup/popup.js");
  const iLote = popup.indexOf('getElementById("btn-cola-enviar")?.addEventListener');
  const iCorte = popup.search(/if \(b\.indeterminado\) \{ corte = [^\n]*break; \}/);
  ok(iLote > 0 && iCorte > iLote && iCorte < popup.indexOf("enviarAlBoard(", iLote), "sin respuesta de la lista de rebotados el lote se corta antes de cargar");
});

test("tarjeta: un adicional cuya consulta de rebote falló no se programa y el aviso dice por qué", async () => {
  const { adicionalesDeLaTarjeta } = await import("../../modules/colaEstado.js");
  const { filas, avisos } = adicionalesDeLaTarjeta({
    domain: "diario.com", mbEmail: "mb@adeqmedia.com", principal: "a@diario.com",
    candidatos: ["b@diario.com", "c@diario.com", "d@diario.com"], rebotados: new Set(["c@diario.com"]),
    sinConfirmar: new Map([["d@diario.com", "la base contestó HTTP 503"]]), subject: "s", body: "b", ahoraMs: 0,
  });
  deepStrictEqual(filas.map(f => f.future_email), ["b@diario.com"], "ni el rebotado ni el que no se pudo confirmar se programan");
  ok(avisos.some(a => /d@diario\.com: no pude confirmar que no rebotó \(la base contestó HTTP 503\)/.test(a)), avisos.join(" | "));
});

// ── 11. La lista de rebotes más larga que el tope ──────────────────────────────────────────────────
// loadBouncedEmails y _cargarDominiosQueRechazan leían con `max: 50000` sin mirar si llegaban al final:
// con más filas cargaban las primeras 50.000 como si fueran la lista entera, y _rebotesListosParaJuzgar
// daba luz verde. Es el mismo error que `entero` arregló en las demás lecturas que protegen.
test("una lista de rebotes que llega al tope no se da por entera: reabrir no juzga y late en rojo, y lo leído sigue frenando", async () => {
  const m = /const REBOTES_MAX_FILAS\s*=\s*([\d_]+);/.exec(worker);
  const tope = Number(String(m?.[1] || "50000").replace(/_/g, ""));   // antes del arreglo, el 50.000 fijo
  const direccion = (i) => `p${i}@medio${i % 50}.com`;
  const lista = Array.from({ length: tope + 5 }, (_, i) => ({ email: direccion(i), evidencia: "rebote_smtp", fuente: "scrape" }));
  const { w, pedidos } = await correrReabrir({
    listaRebotes: lista,
    leads: [{ id: 9, domain: "lead9.com", emails: [direccion(tope + 3)], email_sources: {} }],
  });
  const l = latidoDe(pedidos, "reabrir_rebotados");
  deepStrictEqual([l?.last_status, /rebotes/.test(l?.last_detail || "")], ["fail", true],
    `con más filas que el tope se juzgaba con las primeras como si fueran todas: ${JSON.stringify(l)}`);
  ok(!pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue?id=eq.")), "con media lista no se escribe nada");
  strictEqual(w._CADENCIA_MEM.has("reabrir_rebotados"), false, "el turno se devuelve");
  ok(w._bouncedCache.set.has(direccion(tope - 1)), "lo que sí se leyó se sigue usando para frenar envíos");
  ok(/toolbar_bounced_emails\?select=email,tipo,evidencia,fuente&\$\{EVIDENCIA_BLOQUEA\}&order=email`,[^\n]*\n[^\n]*\n\s*\{ max: REBOTES_MAX_FILAS \}\);/.test(worker),
    "_cargarDominiosQueRechazan usa el mismo tope");
});
