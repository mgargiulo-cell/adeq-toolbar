// El feeder: cuánto se descubre, de dónde, y cuándo se gasta para descubrirlo. (2026-09-13, segunda
// auditoría pedida por el dueño)
//
// "Dejar una base de conceptos, lógicas y funcionamientos automáticos sin fallas: lo que arreglás un
// día no lo rompas al otro." Cada test fija una regla que la auditoría encontró rota con el código real:
//   1. AutoGoogle y similar decidían GASTAR (Serper, RapidAPI) con el carril fijo de la tabla, y la
//      inyección cortaba con el dinámico: búsquedas pagadas para dejar todo estacionado, y la fuente
//      que mejor convierte frenada en 250 con un carril asignado de ~500.
//   2. Los dos reciclados del CRM (barrido diario y slot) filtraban cada uno con su copia: el slot
//      re-bajaba lo descartado sin ads.txt y borraba la marca de email en dominios que no entraban.
//   3. El reparto del feeder "por rendimiento" dividía filas distintas arriba y abajo y quedaba
//      clavado en 70/15/15 sin importar lo que rindiera cada fuente.
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

process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const resp = (body, { status = 200, total = null } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: (k) => (String(k).toLowerCase() === "content-range" && total != null ? `0-0/${total}` : null) },
  json: async () => body,
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
// Un enrutador que anota cada pedido y contesta según la URL; lo que no reconoce, lista vacía.
const enrutar = (pedidos, reglas) => async (url, opts = {}) => {
  const u = String(url), m = (opts.method || "GET").toUpperCase();
  pedidos.push({ u, m, b: String(opts.body || "") });
  for (const [cond, contestar] of reglas) if (cond(u, m)) return contestar(u, m);
  return resp([]);
};
const latidoDe = (pedidos, job) => pedidos
  .filter(p => p.m === "POST" && p.u.includes("/rest/v1/toolbar_health"))
  .map(p => JSON.parse(p.b)).filter(b => b.job === job).pop();
// Entre la marca de una salida y su `return;` tiene que haber un latido del job.
const lateAntesDeSalir = (cuerpo, marca, job) => {
  const i = cuerpo.indexOf(marca);
  ok(i >= 0, `no encontré la salida "${marca}"`);
  const tramo = cuerpo.slice(i, cuerpo.indexOf("return;", i));
  return tramo.includes(`saludPing(token, "${job}", { status: "ok"`);
};

// ── 1. Un solo número de carril para decidir el gasto y para inyectar ──────────────────
test("el carril fijo de la tabla sólo lo leen el recálculo y _capDeFuente: nadie decide gastar con otro número", () => {
  const ast = acorn.parse(worker, { ecmaVersion: "latest", sourceType: "module" });
  const permitidas = new Set(["recalcularCarrilesPorRendimiento", "_capDeFuente"]);
  const fuera = [];
  walk.fullAncestor(ast, (node, _s, anc) => {
    if (node.type !== "MemberExpression" || node.object?.name !== "PER_SOURCE_ACTIVE_CAP") return;
    const fn = [...anc].reverse().find(a => a.type === "FunctionDeclaration")?.id?.name || "(módulo)";
    if (!permitidas.has(fn)) fuera.push(`${fn}: ${worker.slice(node.start, node.end)}`);
  });
  deepStrictEqual(fuera, [], "AutoGoogle pagaba Serper con 180 de lugar y la inyección cortaba en 72 (el 10/09: 54 frescos y CERO encolados)");
});

test("el lugar en el carril sale del carril DINÁMICO, y una cuenta ilegible es 'sin lugar', no 'vacío'", async () => {
  const w = await cargarWorker(["recalcularCarrilesPorRendimiento", "_lugarEnCarril", "_capDeFuente"], { fetchFalso: true });
  let conteo = () => resp([], { total: 100 });
  globalThis.__fetchFalso = enrutar([], [
    [(u) => u.includes("toolbar_config?select=key,value"), () => resp([{ key: "carriles_dinamicos", value: JSON.stringify({ autogoogle: 72, auto_feeder_similar: 450 }) }])],
    [(u) => u.includes("toolbar_csv_queue?status=in.(pending,processing,waiting_pool)"), () => conteo()],
  ]);
  await w.recalcularCarrilesPorRendimiento("t");   // carga el reparto guardado: desde afuera no se le puede asignar
  strictEqual(w._capDeFuente("autogoogle"), 72);
  deepStrictEqual(await w._lugarEnCarril("t", "autogoogle"), { cap: 72, usados: 100, lugar: 0, error: false },
    "con el fijo (180) daba 80 de lugar y se pagaban las búsquedas");
  deepStrictEqual(await w._lugarEnCarril("t", "auto_feeder_similar"), { cap: 450, usados: 100, lugar: 350, error: false });
  conteo = () => resp({ message: "JWT expired" }, { status: 401 });
  deepStrictEqual(await w._lugarEnCarril("t", "autogoogle"), { cap: 72, usados: null, lugar: 0, error: true }, "un 401 no es un carril vacío");
  conteo = () => resp([]);   // 200 sin content-range
  strictEqual((await w._lugarEnCarril("t", "autogoogle")).error, true, "sin total no hay cuenta");
});

test("AutoGoogle con el carril dinámico lleno no gasta Serper y late 'ok'; con la cuenta ilegible tampoco gasta", async () => {
  for (const caso of ["lleno", "ilegible"]) {
    const w = await cargarWorker(["recalcularCarrilesPorRendimiento", "_runAutoGoogleSlot"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = enrutar(pedidos, [
      [(u) => u.includes("toolbar_config?select=key,value"), () => resp([{ key: "carriles_dinamicos", value: '{"autogoogle":72}' }])],
      [(u) => u.includes("toolbar_csv_queue?status=in.(pending,processing,waiting_pool)&source=eq.autogoogle"),
        () => (caso === "lleno" ? resp([], { total: 100 }) : resp({ message: "boom" }, { status: 500 }))],
    ]);
    await w.recalcularCarrilesPorRendimiento("t");
    await w._runAutoGoogleSlot("t", "2026-09-14-10:00");
    ok(!pedidos.some(p => /^https:\/\/[^/]*serper\.dev\//i.test(p.u)), `${caso}: no puede salir ni una búsqueda a Serper`);
    ok(!pedidos.some(p => p.u.includes("toolbar_discovery_backlog")), `${caso}: sin lugar tampoco se drena el pre-listado`);
    const latido = latidoDe(pedidos, "autogoogle");
    ok(latido, `${caso}: tiene que latir`);
    strictEqual(latido.last_status, caso === "lleno" ? "ok" : "fail", latido.last_detail);
    ok(caso === "lleno" ? /carril lleno \(100\/72\)/.test(latido.last_detail) : /no pude contar el carril/.test(latido.last_detail), latido.last_detail);
  }
});

test("las otras salidas de AutoGoogle sin gastar también laten: pre-listado que llena el carril y tope mensual", () => {
  const ag = cuerpoDe("_runAutoGoogleSlot");
  ok(lateAntesDeSalir(ag, "carril llenado con el pre-listado", "autogoogle"), "con el carril real (~80) el pre-listado lo llena seguido: callarse era parecer caído");
  ok(/if \(remaining <= 0\) \{[\s\S]{0,300}saludPing\(token, "autogoogle", \{ status: "off"/.test(ag), "el tope mensual es 'apagado por tope', igual que el diario");
  ok(lateAntesDeSalir(cuerpoDe("runProspectSimilarExpansion"), "carril llenado con el pre-listado", "similar_expansion"), "similar sale en silencio si el pre-listado llena el carril");
});

test("similar se expande hasta su carril dinámico (450), no hasta el fijo (250); con la cuenta ilegible no gasta RapidAPI", async () => {
  {
    const w = await cargarWorker(["recalcularCarrilesPorRendimiento", "runProspectSimilarExpansion"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = enrutar(pedidos, [
      [(u) => u.includes("toolbar_config?select=key,value"), () => resp([{ key: "carriles_dinamicos", value: '{"auto_feeder_similar":450}' }, { key: "rapidapi_key", value: "clave-falsa" }])],
      [(u) => u.includes("toolbar_csv_queue?status=in.(pending,processing,waiting_pool)&source=eq.auto_feeder_similar"), () => resp([], { total: 260 })],
    ]);
    await w.recalcularCarrilesPorRendimiento("t");
    await w.runProspectSimilarExpansion("t");
    const drenaje = pedidos.find(p => p.m === "GET" && p.u.includes("toolbar_discovery_backlog?source=eq.auto_feeder_similar"));
    ok(drenaje, "con 260 activos y carril 450 hay lugar: con el fijo salteaba porque 260 > 250");
    ok(/limit=570/.test(drenaje.u), `el drenaje pide 3× los 190 lugares libres: ${drenaje.u}`);
  }
  {
    const w = await cargarWorker(["runProspectSimilarExpansion"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = enrutar(pedidos, [
      [(u) => u.includes("toolbar_config?select=key,value"), () => resp([{ key: "rapidapi_key", value: "clave-falsa" }])],
      [(u) => u.includes("toolbar_csv_queue?status=in.(pending,processing,waiting_pool)&source=eq.auto_feeder_similar"), () => resp({ message: "caído" }, { status: 503 })],
    ]);
    await w.runProspectSimilarExpansion("t");
    ok(!pedidos.some(p => /toolbar_discovery_backlog|toolbar_response_tracking|toolbar_review_queue/.test(p.u)),
      "sin poder contar el carril no se drena ni se buscan semillas: un 503 no es 'carril vacío, gastá'");
    const latido = latidoDe(pedidos, "similar_expansion");
    ok(latido && latido.last_status === "fail" && /no pude contar el carril/.test(latido.last_detail), JSON.stringify(latido));
  }
});

// ── 2. Los reciclables pasan por una sola regla ─────────────────────────────────────────
test("los reciclables pasan por UNA regla: contactados, cola, Prospects y sin ads.txt en 30 días", async () => {
  const { _filtrarReciclables } = await cargarWorker(["_filtrarReciclables"], { fetchFalso: true });
  let sendtrackOk = true;
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [(u) => u.includes("toolbar_sendtrack?"), () => (sendtrackOk ? resp([{ domain: "contactado.com" }]) : resp({ message: "boom" }, { status: 500 }))],
    [(u) => u.includes("toolbar_csv_queue?domain=in."), () => resp([{ domain: "encola.com" }])],
    [(u) => u.includes("toolbar_review_queue?status=eq.pending"), () => resp([{ domain: "Pendiente.com" }])],
    [(u) => u.includes("toolbar_adstxt_audit?verdict=eq.no&last_checked_at=gte."), () => resp([{ domain: "sinads.com" }])],
  ]);
  const lista = ["libre2.com", "contactado.com", "encola.com", "pendiente.com", "sinads.com", "libre1.com"];
  const f = await _filtrarReciclables("t", lista, 90);
  deepStrictEqual(f.elegibles, ["libre2.com", "libre1.com"], "y conserva el orden: el slot mezcla la lista antes de filtrar");
  deepStrictEqual([...f.yaEnProspects], ["pendiente.com"]);
  deepStrictEqual([...f.sinAds30d], ["sinads.com"]);
  sendtrackOk = false;
  strictEqual(await _filtrarReciclables("t", lista, 90), null, "sin poder leer a quién se le escribió, no se re-prospecta a ciegas");
});

test("el feeder por slot no re-baja lo descartado sin ads.txt, borra la marca sólo en lo que entró, y un descarte por ads.txt no es rendir poco", async () => {
  const { _feederPullMonday } = await cargarWorker(["_feederPullMonday"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [(u) => u.includes("/reciclables"), () => resp({ domains: ["pendiente.com", "sinads.com", "nuevo1.com", "nuevo2.com"] })],
    [(u) => u.includes("toolbar_review_queue?status=eq.pending"), () => resp([{ domain: "pendiente.com" }])],
    [(u) => u.includes("toolbar_adstxt_audit?verdict=eq.no&last_checked_at=gte."), () => resp([{ domain: "sinads.com" }])],
    [(u) => u.includes("toolbar_csv_queue?status=in.(pending,processing,waiting_pool)"), () => resp([], { total: 0 })],
    [(u) => /^https?:\/\/[a-z0-9.-]+\/(app-)?ads\.txt$/.test(u), () => resp("", { status: 404 })],
  ]);
  const inserted = await _feederPullMonday("t", 50, new Set());
  strictEqual(inserted, 0, "nuevo1 y nuevo2 no tienen ads.txt: no entra ninguno");
  const adsTxt = pedidos.filter(p => /\/(app-)?ads\.txt$/.test(p.u)).map(p => p.u);
  ok(adsTxt.some(u => u.includes("nuevo1.com")), "los nuevos sí pasan por la puerta de ads.txt");
  ok(!adsTxt.some(u => u.includes("sinads.com")), `lo descartado hace menos de 30 días no se vuelve a bajar en cada slot: ${adsTxt.join(", ")}`);
  ok(!adsTxt.some(u => u.includes("pendiente.com")), "lo que espera en Prospects tampoco");
  ok(!pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue")), "no entró ninguno: no hay marca de email que borrar");
  const latido = latidoDe(pedidos, "feeder_monday");
  ok(latido, "tiene que latir");
  strictEqual(latido.real_ultimo, 2, "dos descartes correctos por ads.txt son dos procesados, no 'rindió 0 de 2'");
  strictEqual(latido.esperado_ultimo, 2);
});

test("con el carril de monday lleno el slot no le pide la lista al CRM y late 'no hacía falta'", async () => {
  const { _feederPullMonday } = await cargarWorker(["_feederPullMonday"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, []);
  strictEqual(await _feederPullMonday("t", 0, new Set()), 0);
  ok(!pedidos.some(p => p.u.includes("/reciclables")), "sin lugar, pedir 1.000 reciclables y filtrarlos es trabajo tirado");
  const latido = latidoDe(pedidos, "feeder_monday");
  ok(latido && latido.last_status === "ok" && /carril lleno/.test(latido.last_detail), JSON.stringify(latido));
});

test("los dos reciclados llaman a la misma regla, ninguno tiene su copia, y la marca de email se borra sólo en lo que entró", () => {
  for (const fn of ["sincronizarFinalizadosDeMonday", "_feederPullMonday"]) {
    const c = cuerpoDe(fn);
    ok(/_filtrarReciclables\(token, /.test(c), `${fn} tiene que usar _filtrarReciclables`);
    ok(!/_dominiosContactadosDesde\(|_dominiosActivosEnCola\(|_dominiosPendientesEnProspects\(|toolbar_adstxt_audit/.test(c),
      `${fn} no puede volver a filtrar por su cuenta: así un filtro nuevo quedó dos veces afuera del otro camino`);
    ok(/returnDomains: true/.test(c) && /_limpiarMarcaDeEmail\(token, _entraron\)/.test(c), `${fn}: la marca de email se borra sólo en los que entraron`);
  }
});

test("la inyección devuelve una LISTA cuando se le pide la lista, aunque el lote quede vacío al limpiarlo", async () => {
  const { _injectIntoCsvQueue } = await cargarWorker(["_injectIntoCsvQueue"], { fetchFalso: true });
  globalThis.__fetchFalso = enrutar([], []);
  deepStrictEqual(await _injectIntoCsvQueue("t", ["", "cachalot.k"], "auto_feeder_monday", { returnDomains: true }), [],
    "devolvía el número 0 y el llamador leía `.length` de un número");
  strictEqual(await _injectIntoCsvQueue("t", ["cachalot.k"], "auto_feeder_monday"), 0, "sin returnDomains sigue siendo un número");
});

// ── 3. El reparto por rendimiento mide lo que dice medir ────────────────────────────────
test("el reparto por rendimiento cuenta filas de la misma tabla, deja afuera a monday y respeta el piso", async () => {
  const { _pesosFeeder } = await cargarWorker(["_pesosFeeder"]);
  const filas = (tag, llegaron, n) => Array.from({ length: n }, (_, i) => ({
    source: tag, status: i < llegaron ? "done" : "skipped", error_message: i < llegaron ? null : "not_publisher: sin_ads_txt",
  }));
  const cerca = (a, b) => Math.abs(a - b) < 1e-9;
  const base = [...filas("auto_feeder_sellers", 0, 200), ...filas("auto_feeder_majestic", 50, 200)];
  const w = _pesosFeeder(base, 0.15);
  ok(cerca(w.sellers, 0.15), `sellers 0 de 200 queda exactamente en el piso: ${w.sellers}`);
  ok(cerca(w.sellers + w.majestic, 1), "la suma da 1");
  // Las done del barrido diario tienen la misma etiqueta que el slot: no pueden mover el reparto.
  const conMonday = _pesosFeeder([...base, ...filas("auto_feeder_monday", 600, 600)], 0.15);
  deepStrictEqual([conMonday.sellers, conMonday.majestic], [w.sellers, w.majestic], "monday no altera el reparto");
  const chica = _pesosFeeder([...filas("auto_feeder_sellers", 10, 40), ...filas("auto_feeder_majestic", 0, 400)], 0.15);
  deepStrictEqual([chica.sellers, chica.majestic], [0.5, 0.5], "con menos de 50 procesadas no se decide con ruido");
  const yaEstaban = Array.from({ length: 30 }, () => ({ source: "auto_feeder_sellers", status: "skipped", error_message: "ya_estaba_en_prospects" }));
  const d = _pesosFeeder([...filas("auto_feeder_sellers", 10, 40), ...yaEstaban, ...filas("auto_feeder_majestic", 10, 100)], 0.15);
  deepStrictEqual([d.sellers, d.majestic], [0.5, 0.5], "40 procesadas + 30 que ya estaban en Prospects son 40, no 70");
  ok(/sellers 10\/40/.test(d.debug), d.debug);
  for (const [s, ns, j, nj] of [[0, 50, 0, 50], [50, 50, 0, 900], [0, 900, 50, 50], [30, 100, 31, 100], [1, 500, 499, 500]]) {
    const r = _pesosFeeder([...filas("auto_feeder_sellers", s, ns), ...filas("auto_feeder_majestic", j, nj)], 0.15);
    ok(r.sellers >= 0.15 - 1e-9 && r.majestic >= 0.15 - 1e-9 && r.sellers <= 0.85 + 1e-9 && r.majestic <= 0.85 + 1e-9 && cerca(r.sellers + r.majestic, 1),
      `${s}/${ns} contra ${j}/${nj}: ${JSON.stringify(r)}`);
  }
});

test("el slot le da a monday lo que falta de su carril, a sellers+majestic la misma parte de antes, y el reparto no lee brutos de otra tabla", () => {
  const slot = worker.slice(worker.indexOf("async function _runFeederSlot("), worker.indexOf("async function _measureFeederRuns("));
  ok(!/w\.monday/.test(slot), "monday ya no sale de un peso por rendimiento");
  ok(/const allocMonday\s+= Math\.min\(_libreMonday, _techoMonday\)/.test(slot) && /_capDeFuente\("auto_feeder_monday"\)/.test(slot),
    "monday: el lugar libre de su carril (el mismo cap que usa la inyección), con el techo diario del barrido");
  ok(/_parteSM\s+= Math\.round\(targetGross \* FEEDER_PARTE_SELLERS_MAJESTIC\)/.test(slot));
  strictEqual(worker.match(/const FEEDER_PARTE_SELLERS_MAJESTIC = ([\d.]+);/)?.[1], "0.30", "la parte conjunta de sellers + majestic es la que tenían (15 + 15): subirla es decisión del dueño");
  ok(!/_feederPullAdsTxtGraph\(token, [^)]*alloc/.test(slot) && !/_feederPullGeo\(token, [^)]*alloc/.test(slot),
    "adstxt y GEO van encima del reparto: su cupo no puede cambiar porque cambie el reparto");
  ok(/gross_monday: fromMonday/.test(slot), "el popup sigue leyendo los brutos por fuente");
  const pesos = cuerpoDe("_getFeederSourceWeights");
  ok(!/toolbar_feeder_runs/.test(pesos), "los brutos del slot contra las done de toda la base eran poblaciones distintas");
  ok(/_pesosFeeder\(filas, FEEDER_EXPLORE_FLOOR\)/.test(pesos));
});
