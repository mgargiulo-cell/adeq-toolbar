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
