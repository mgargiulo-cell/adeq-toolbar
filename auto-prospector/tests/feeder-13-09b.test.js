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
//   4. Tres lecturas del feeder pedían 14 o 90 días en un solo pedido con `limit=20000` y PostgREST
//      corta en 1.000: el reparto y los carriles se medían con un pedazo sin orden de la ventana, y
//      los contactados que caían fuera de esas 1.000 se re-prospectaban.
//   5. Monday contaba su carril fallando abierto y sin la espera (next_day), así que los 5 slots
//      apilaban reciclados afuera del carril; un techo "del día" que restaba sólo el slot hacía depender
//      el volumen del orden de los jobs; la fila del slot se escribía al final (un reinicio a mitad lo
//      re-disparaba); y sus 0-400 filas entraban en la conversión que decide cuánto traen sellers y majestic.
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
  // Desde la corrección del 13/09 el lugar se cuenta con _lugarEnCarril (el mismo _capDeFuente que usa la
  // inyección, fallando cerrado) y el tope de cada pasada es el techo del barrido, con la MISMA regla que
  // el barrido (ver la sección 5): lo que encoló hoy el otro camino no entra en la cuenta.
  ok(/const allocMonday\s+= _asigMonday\.alloc/.test(slot) && /_lugarEnCarril\(token, "auto_feeder_monday"\)/.test(slot)
    && /_asignacionMonday\(\{ lugar: _carrilMonday\.error \? null : _carrilMonday\.lugar, techo: _techoMonday \}\)/.test(slot)
    && /return _capDeFuente|const cap = _capDeFuente\(sourceTag\)/.test(cuerpoDe("_lugarEnCarril")),
    "monday: el lugar libre de su carril (el mismo cap que usa la inyección), con el techo del barrido como tope");
  ok(!/_contarEncoladosHoy|uploaded_at=gte/.test(slot),
    "el slot no puede depender de si el barrido ya corrió hoy: con el techo compartido de un solo lado, los slots reciclaban 0 o el día sumaba 2 techos según el orden");
  ok(/_parteSM\s+= Math\.round\(targetGross \* FEEDER_PARTE_SELLERS_MAJESTIC\)/.test(slot));
  strictEqual(worker.match(/const FEEDER_PARTE_SELLERS_MAJESTIC = ([\d.]+);/)?.[1], "0.30", "la parte conjunta de sellers + majestic es la que tenían (15 + 15): subirla es decisión del dueño");
  ok(!/_feederPullAdsTxtGraph\(token, [^)]*alloc/.test(slot) && !/_feederPullGeo\(token, [^)]*alloc/.test(slot),
    "adstxt y GEO van encima del reparto: su cupo no puede cambiar porque cambie el reparto");
  ok(/gross_monday: fromMonday/.test(slot), "el popup sigue leyendo los brutos por fuente");
  const pesos = cuerpoDe("_getFeederSourceWeights");
  ok(!/toolbar_feeder_runs/.test(pesos), "los brutos del slot contra las done de toda la base eran poblaciones distintas");
  ok(/_pesosFeeder\(filas, FEEDER_EXPLORE_FLOOR\)/.test(pesos));
});

// ── 4. Lo que mide o filtra una ventana la lee ENTERA ───────────────────────────────────
// PostgREST devuelve 1.000 filas como máximo por pedido, pida lo que pida el `limit=`. Este
// enrutador contesta como la base: sin Range, las primeras 1.000; con Range, esa página. Con
// `romperDesde`, las páginas desde esa fila contestan 503.
const paginado = (pedidos, tablas, { romperDesde = null } = {}) => async (url, opts = {}) => {
  const u = String(url), m = (opts.method || "GET").toUpperCase();
  const rango = String(opts.headers?.Range || "");
  pedidos.push({ u, m, rango, b: String(opts.body || "") });
  for (const [marca, filas] of tablas) {
    if (m !== "GET" || !u.includes(marca)) continue;
    const desde = parseInt(rango.split("-")[0], 10) || 0;
    if (romperDesde != null && desde >= romperDesde) return resp({ message: "caído" }, { status: 503 });
    const hasta = rango ? parseInt(rango.split("-")[1], 10) : desde + 999;
    return resp(filas.slice(desde, Math.min(hasta + 1, desde + 1000)), { status: rango ? 206 : 200 });
  }
  return resp([]);
};
const filasDe = (tag, llegaron, n) => Array.from({ length: n }, (_, i) => ({
  source: tag, status: i < llegaron ? "done" : "skipped", error_message: i < llegaron ? null : "not_publisher: sin_ads_txt",
}));

test("el reparto sellers/majestic lee los 14 días de a páginas: la fuente que cae en la segunda página cuenta", async () => {
  const { _getFeederSourceWeights } = await cargarWorker(["_getFeederSourceWeights"], { fetchFalso: true });
  const filas = [...filasDe("auto_feeder_sellers", 100, 1000), ...filasDe("auto_feeder_majestic", 60, 300)];
  const pedidos = [];
  globalThis.__fetchFalso = paginado(pedidos, [["toolbar_csv_queue?processed_at=gte.", filas]]);
  const w = await _getFeederSourceWeights("t");
  const lecturas = pedidos.filter(p => p.u.includes("toolbar_csv_queue?processed_at=gte."));
  deepStrictEqual(lecturas.map(p => p.rango), ["0-999", "1000-1999"], "pide la segunda página, y con 300 filas no pide una tercera");
  ok(lecturas.every(p => /&order=id/.test(p.u) && !/limit=/.test(p.u)), `paginar sin orden repite y saltea filas: ${lecturas[0]?.u}`);
  ok(/sellers 100\/1000 · majestic 60\/300 \(14d\)/.test(w.debug), `con un solo pedido daba majestic 0/0 y 'muestra chica: mitad y mitad': ${w.debug}`);
  ok(w.majestic > w.sellers, `majestic rinde 20% y sellers 10%: ${JSON.stringify(w)}`);
  globalThis.__fetchFalso = paginado([], [["toolbar_csv_queue?processed_at=gte.", filas]], { romperDesde: 1000 });
  const roto = await _getFeederSourceWeights("t");
  deepStrictEqual([roto.sellers, roto.majestic], [0.5, 0.5]);
  ok(/lectura incompleta/.test(roto.debug), `una página caída no es la muestra entera: ${roto.debug}`);
});

test("el recálculo de carriles lee los 14 días de a páginas, y con una página caída deja el reparto que había", async () => {
  // sellers llena la primera página; similar y adstxt están en la segunda. Con un solo pedido había
  // una fuente sola, "sin con qué comparar", y los carriles no se recalculaban nunca.
  const filas = [...filasDe("auto_feeder_sellers", 80, 1000), ...filasDe("auto_feeder_similar", 150, 200), ...filasDe("auto_feeder_adstxt", 20, 200)];
  const cambioElReparto = (pedidos) => pedidos.some(p => p.m !== "GET" && `${p.u} ${p.b}`.includes("carriles_dinamicos"));
  {
    const w = await cargarWorker(["recalcularCarrilesPorRendimiento", "_capDeFuente"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = paginado(pedidos, [["toolbar_csv_queue?processed_at=gte.", filas]]);
    await w.recalcularCarrilesPorRendimiento("t");
    const lecturas = pedidos.filter(p => p.u.includes("toolbar_csv_queue?processed_at=gte."));
    deepStrictEqual(lecturas.map(p => p.rango), ["0-999", "1000-1999"]);
    ok(lecturas.every(p => /&order=id/.test(p.u) && !/limit=/.test(p.u)), lecturas[0]?.u);
    // similar 151/210, adstxt 21/210, sellers 81/1010 sobre 520 de carril total (250 + 120 + 150).
    strictEqual(w._capDeFuente("auto_feeder_similar"), 416);
    strictEqual(w._capDeFuente("auto_feeder_adstxt"), 58);
    strictEqual(w._capDeFuente("auto_feeder_sellers"), 60, "sellers al 8% queda en el piso del 40% de su carril");
    ok(cambioElReparto(pedidos), "el reparto nuevo se guarda y late");
  }
  {
    const w = await cargarWorker(["recalcularCarrilesPorRendimiento", "_capDeFuente"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = paginado(pedidos, [["toolbar_csv_queue?processed_at=gte.", filas]], { romperDesde: 1000 });
    await w.recalcularCarrilesPorRendimiento("t");
    strictEqual(w._capDeFuente("auto_feeder_similar"), 250, "sin la ventana entera queda el carril que había");
    ok(!cambioElReparto(pedidos), "no se guarda un reparto medido con media ventana");
  }
});

test("los contactados de 90 días se leen de a páginas: el que cae después del envío 1.000 no se re-prospecta", async () => {
  const { _filtrarReciclables } = await cargarWorker(["_filtrarReciclables"], { fetchFalso: true });
  const envios = [...Array.from({ length: 1000 }, (_, i) => ({ domain: `enviado${i}.com` })), { domain: "contactado-tarde.com" }];
  const pedidos = [];
  globalThis.__fetchFalso = paginado(pedidos, [["toolbar_sendtrack?", envios]]);
  const f = await _filtrarReciclables("t", ["contactado-tarde.com", "libre.com"], 90);
  deepStrictEqual(f.elegibles, ["libre.com"], "con un solo pedido la base cortaba en 1.000 y contactado-tarde.com volvía a la cola");
  const lecturas = pedidos.filter(p => p.u.includes("toolbar_sendtrack?"));
  deepStrictEqual(lecturas.map(p => p.rango), ["0-999", "1000-1999"]);
  ok(lecturas.every(p => /&order=domain/.test(p.u) && !/limit=/.test(p.u)), lecturas[0]?.u);
  globalThis.__fetchFalso = paginado([], [["toolbar_sendtrack?", envios]], { romperDesde: 1000 });
  strictEqual(await _filtrarReciclables("t", ["contactado-tarde.com", "libre.com"], 90), null,
    "media lista de contactados no es la lista: no se re-prospecta a ciegas");
});

test("ninguna lectura del feeder que mide o filtra una ventana pide más de 1.000 filas en un solo pedido", () => {
  // `_reconcileAutogoogleAttribution` queda afuera a propósito: es un drenaje ordenado de las más
  // viejas que borra lo que procesa, así que 1.000 por vuelta avanza igual.
  const zona = ["recalcularCarrilesPorRendimiento", "_getFeederSourceWeights", "_dominiosContactadosDesde", "_filtrarReciclables",
    "_runAutoGoogleSlot", "runProspectSimilarExpansion", "_feederPullMonday", "sincronizarFinalizadosDeMonday",
    "_injectIntoCsvQueue", "_lugarEnCarril", "_dominiosActivosEnCola", "_runFeederSlot"];
  for (const n of zona) ok(worker.includes(`function ${n}(`), `no encontré ${n}: si cambió de nombre, actualizar la lista`);
  const ast = acorn.parse(worker, { ecmaVersion: "latest", sourceType: "module" });
  const fuera = [];
  walk.fullAncestor(ast, (node, _s, anc) => {
    if (node.type !== "TemplateLiteral") return;
    const m = /[?&]limit=(\d+)/.exec(worker.slice(node.start, node.end));
    if (!m || Number(m[1]) <= 1000) return;
    const fn = [...anc].reverse().find(a => a.type === "FunctionDeclaration")?.id?.name;
    if (zona.includes(fn)) fuera.push(`${fn}: limit=${m[1]}`);
  });
  deepStrictEqual(fuera, [], "PostgREST corta en 1.000: el pedido cree que leyó todo y leyó un pedazo. Usar _traerTodo con orden");
});

// ── 5. Monday: una regla para las dos pasadas, la espera dentro del carril, slot anotado al empezar ──
test("monday recicla en cada pasada min(lugar, techo), sin restar lo que encoló el otro camino, y sin poder contar no recicla", async () => {
  const { _asignacionMonday: a } = await cargarWorker(["_asignacionMonday"]);
  strictEqual(a({ lugar: 700, techo: 400 }).alloc, 400);
  strictEqual(a({ lugar: 300, techo: 400 }).alloc, 300, "nunca más que el lugar del carril");
  strictEqual(a({ lugar: 0, techo: 400 }).alloc, 0);
  // El 13/09 a la mañana la regla restaba "lo encolado hoy" y sólo la aplicaba el slot: con el barrido
  // corriendo primero, los 5 slots del día reciclaban 0. Lo que ya está en la cola lo cuenta el carril.
  strictEqual(a({ lugar: 300, techo: 400, encoladosHoy: 400 }).alloc, 300, "lo que encoló hoy el barrido no le resta al slot");
  for (const roto of [{ lugar: null, techo: 400 }, { lugar: NaN, techo: 400 }]) {
    const r = a(roto);
    ok(r.error && r.alloc === 0, `una cuenta ilegible es 'no reciclo', no 'carril vacío': ${JSON.stringify(roto)} → ${JSON.stringify(r)}`);
  }
});

// La cuenta del carril como la base: suma las filas de los estados que pide la URL (`status=in.(…)`).
const estadosDe = (u) => (/status=in\.\(([^)]*)\)/.exec(u)?.[1] || "").split(",").filter(Boolean);
const carrilPorEstado = (porEstado) => (u) => resp([], { total: estadosDe(u).reduce((s, e) => s + (porEstado[e] || 0), 0) });
const esCuentaDeMonday = (u) => u.includes("toolbar_csv_queue?status=in.(") && u.includes("&source=eq.auto_feeder_monday");
const reciclables = (n) => () => resp({ domains: Array.from({ length: n }, (_, i) => `reciclado${i}.com`) });
const reciclablesDelCrm = reciclables(100);
const chequeadosDe = (pedidos) => new Set(pedidos.filter(p => /\/(app-)?ads\.txt$/.test(p.u) && /reciclado\d+\.com/.test(p.u)).map(p => p.u.match(/reciclado\d+\.com/)[0]));

// El slot entero, con la base, el CRM y los ads.txt falsos. `crm` decide qué contesta /reciclables.
// `hoy` contesta cuántas filas de monday se encolaron hoy: la corrección de la mañana del 13/09 lo
// preguntaba en el slot; hoy nadie lo pregunta, y el test verifica que siga así.
const correrSlot = async ({ crm, carril = carrilPorEstado({ pending: 100 }), hoy = () => resp([], { total: 360 }), cerrar = () => resp(null, { status: 204 }), anotar = () => resp([{ id: 77 }], { status: 201 }) }) => {
  const { _runFeederSlot } = await cargarWorker(["_runFeederSlot"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [(u, m) => m === "POST" && /\/rest\/v1\/toolbar_feeder_runs$/.test(u), () => anotar()],
    [(u, m) => m === "PATCH" && u.includes("/rest/v1/toolbar_feeder_runs?id=eq."), () => cerrar()],
    [esCuentaDeMonday, (u) => carril(u)],
    [(u) => u.includes("toolbar_csv_queue?source=eq.auto_feeder_monday&uploaded_at=gte."), () => hoy()],
    [(u) => u.includes("/reciclables"), () => crm()],
    [(u) => /^https?:\/\/[a-z0-9.-]+\/(app-)?ads\.txt$/.test(u), () => resp("", { status: 404 })],
  ]);
  return { corrida: _runFeederSlot("t", "2026-09-14-09:00"), pedidos };
};

// El barrido diario entero, con la base, el CRM y los ads.txt falsos.
const correrBarrido = async ({ carril, crm = reciclables(400) }) => {
  const { sincronizarFinalizadosDeMonday } = await cargarWorker(["sincronizarFinalizadosDeMonday"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [esCuentaDeMonday, (u) => carril(u)],
    [(u) => u.includes("/reciclables"), () => crm()],
    [(u) => /^https?:\/\/[a-z0-9.-]+\/(app-)?ads\.txt$/.test(u), () => resp("", { status: 404 })],
  ]);
  const encolados = await sincronizarFinalizadosDeMonday("t");
  return { encolados, pedidos };
};

test("con 400 encolados hoy por el barrido y 300 de lugar, el slot recicla 300: no depende de qué job corrió primero", async () => {
  const { corrida, pedidos } = await correrSlot({
    crm: reciclables(400), carril: carrilPorEstado({ pending: 150, waiting_pool: 250 }), hoy: () => resp([], { total: 400 }),
  });
  await corrida;
  const latido = latidoDe(pedidos, "feeder_monday");
  ok(latido, "tiene que latir");
  strictEqual(latido.esperado_ultimo, 300,
    `restando el techo del día sólo en el slot, un barrido temprano de 400 dejaba los 5 slots en 0: ${JSON.stringify(latido)}`);
  ok(!pedidos.some(p => p.u.includes("uploaded_at=gte.")), "el slot no pregunta cuánto encoló hoy el barrido");
  const chequeados = chequeadosDe(pedidos);
  ok(chequeados.size > 0 && chequeados.size <= 300, `chequeó el ads.txt de ${chequeados.size} reciclados`);
});

test("para monday la espera ocupa el carril: con 500 en next_day y carril de 700 el lugar es 200 en la inyección, el slot y el barrido", async () => {
  {
    const w = await cargarWorker(["_lugarEnCarril", "_injectIntoCsvQueue"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = enrutar(pedidos, [
      [(u) => u.includes("toolbar_csv_queue?status=in.("), carrilPorEstado({ next_day: 500 })],
      [(u) => /^https?:\/\/[a-z0-9.-]+\/(app-)?ads\.txt$/.test(u), () => resp("", { status: 404 })],
    ]);
    deepStrictEqual(await w._lugarEnCarril("t", "auto_feeder_monday"), { cap: 700, usados: 500, lugar: 200, error: false },
      "sin contar la espera daba 700 de lugar: 5 slots × 400 se apilaban en next_day afuera del carril");
    deepStrictEqual(await w._lugarEnCarril("t", "auto_feeder_similar"), { cap: 250, usados: 0, lugar: 250, error: false },
      "las demás fuentes no cambian: 117 en next_day frenaban a similar, la que mejor convierte");
    await w._injectIntoCsvQueue("t", Array.from({ length: 300 }, (_, i) => `reciclado${i}.com`), "auto_feeder_monday", { returnDomains: true });
    strictEqual(chequeadosDe(pedidos).size, 200, "la inyección corta con el mismo número que los pre-chequeos");
  }
  {
    const { corrida, pedidos } = await correrSlot({ crm: reciclables(400), carril: carrilPorEstado({ next_day: 500 }), hoy: () => resp([], { total: 0 }) });
    await corrida;
    strictEqual(latidoDe(pedidos, "feeder_monday")?.esperado_ultimo, 200, "slot");
  }
  {
    const { pedidos } = await correrBarrido({ carril: carrilPorEstado({ next_day: 500 }) });
    const latido = latidoDe(pedidos, "monday_sync");
    ok(latido && latido.esperado_ultimo === 200 && /carril libre 200/.test(latido.last_detail), `barrido: ${JSON.stringify(latido)}`);
    strictEqual(chequeadosDe(pedidos).size, 200);
  }
});

test("el barrido no recicla a ciegas: sin poder contar el carril no chequea ni encola, y late 'fail'", async () => {
  for (const carril of [() => resp({ message: "caído" }, { status: 503 }), () => resp([])]) {
    const { encolados, pedidos } = await correrBarrido({ carril });
    strictEqual(encolados, 0);
    strictEqual(chequeadosDe(pedidos).size, 0, "con la cuenta fallando abierta, un 503 era 0 activos → 700 libres → 400 chequeos de ads.txt");
    ok(!pedidos.some(p => p.m === "POST" && p.u.includes("toolbar_csv_queue?on_conflict")), "no se encola nada");
    const latido = latidoDe(pedidos, "monday_sync");
    ok(latido && latido.last_status === "fail" && /no pude contar el carril/.test(latido.last_detail), JSON.stringify(latido));
  }
});

test("sin poder contar el carril de monday, el slot no le pide la lista al CRM y late 'fail'", async () => {
  for (const caso of [{ carril: () => resp({ message: "caído" }, { status: 503 }) }, { carril: () => resp([]) }]) {
    const { corrida, pedidos } = await correrSlot({ crm: reciclablesDelCrm, ...caso });
    await corrida;
    ok(!pedidos.some(p => p.u.includes("/reciclables")), `un 503 contaba como carril vacío → 400 reciclados al CRM: ${caso.carril}`);
    const latido = latidoDe(pedidos, "feeder_monday");
    ok(latido && latido.last_status === "fail" && /no pude contar/.test(latido.last_detail), JSON.stringify(latido));
    ok(pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_feeder_runs?id=eq.77")), "el resto del slot sigue y la fila se cierra");
  }
});

test("el slot se anota ANTES del trabajo largo: un reinicio a mitad no lo vuelve a disparar, y al terminar se cierra esa misma fila", async () => {
  // El CRM no contesta nunca: es el reinicio de Railway cayendo en el medio del slot.
  {
    const { pedidos } = await correrSlot({ crm: () => new Promise(() => {}) });
    for (let i = 0; i < 200 && !pedidos.some(p => p.u.includes("/reciclables")); i++) await new Promise(r => setTimeout(r, 25));
    const iCrm = pedidos.findIndex(p => p.u.includes("/reciclables"));
    ok(iCrm >= 0, "el slot tiene que llegar a pedir los reciclables");
    const anotado = pedidos.findIndex(p => p.m === "POST" && /\/rest\/v1\/toolbar_feeder_runs$/.test(p.u) && JSON.parse(p.b).status === "en_curso");
    ok(anotado >= 0 && anotado < iCrm,
      "la fila se escribía al final: con el worker muerto a mitad, maybeRunFeederSlot no la encontraba y rehacía el slot en la vuelta siguiente");
    strictEqual(JSON.parse(pedidos[anotado].b).slot_label, "2026-09-14-09:00", "maybeRunFeederSlot reconoce el slot por slot_label");
  }
  {
    const { corrida, pedidos } = await correrSlot({ crm: reciclablesDelCrm });
    await corrida;
    const escrituras = pedidos.filter(p => p.u.includes("/rest/v1/toolbar_feeder_runs") && p.m !== "GET");
    deepStrictEqual(escrituras.map(p => p.m), ["POST", "PATCH"], "una sola fila por slot: se anota y se cierra");
    const cierre = JSON.parse(escrituras[1].b);
    ok(/id=eq\.77/.test(escrituras[1].u) && ["ok", "incomplete"].includes(cierre.status) && "gross_monday" in cierre && cierre.cron_at,
      `el cierre lleva los números y re-escribe cron_at (la medición mide desde ahí, como antes): ${escrituras[1].b}`);
  }
  {
    // Si no se pudo anotar, o no se puede cerrar, la fila se escribe al final como antes: nunca se pierde.
    const sinAnotar = await correrSlot({ crm: reciclablesDelCrm, anotar: () => resp({ message: "no" }, { status: 400 }) });
    await sinAnotar.corrida;
    const e1 = sinAnotar.pedidos.filter(p => p.u.includes("/rest/v1/toolbar_feeder_runs") && p.m !== "GET");
    deepStrictEqual(e1.map(p => [p.m, JSON.parse(p.b).status === "en_curso"]), [["POST", true], ["POST", false]]);
    const sinCerrar = await correrSlot({ crm: reciclablesDelCrm, cerrar: () => resp({ message: "no" }, { status: 500 }) });
    await sinCerrar.corrida;
    const e2 = sinCerrar.pedidos.filter(p => p.u.includes("/rest/v1/toolbar_feeder_runs") && p.m !== "GET");
    deepStrictEqual(e2.map(p => p.m), ["POST", "PATCH", "POST"]);
  }
});

test("la conversión del slot no la mueve el volumen de monday, y con la mezcla de antes da lo mismo que antes", async () => {
  const { _measureFeederRuns } = await cargarWorker(["_measureFeederRuns"], { fetchFalso: true });
  const hace1h = new Date(Date.now() - 60 * 60_000).toISOString();
  const runs = [
    { id: 1, cron_at: hace1h, gross_sellers: 20, gross_majestic: 10, gross_total: 430 },   // monday recicló 400
    { id: 2, cron_at: hace1h, gross_sellers: 20, gross_majestic: 10, gross_total: 30 },    // monday con el carril lleno
    { id: 3, cron_at: hace1h, gross_sellers: 30, gross_majestic: 30, gross_total: 200 },   // la mezcla de antes: monday 70%
  ];
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [(u, m) => m === "GET" && u.includes("toolbar_feeder_runs?status=eq.ok&conversion_pct=is.null"), () => resp(runs)],
    [(u) => u.includes("toolbar_csv_queue?status=eq.done"), () => resp([], { total: 6 })],
  ]);
  await _measureFeederRuns("t");
  const conv = Object.fromEntries(pedidos.filter(p => p.m === "PATCH" && p.u.includes("toolbar_feeder_runs?id=eq."))
    .map(p => [p.u.match(/id=eq\.(\d+)/)[1], JSON.parse(p.b)]));
  deepStrictEqual([conv[1]?.conversion_pct, conv[2]?.conversion_pct], ["6.00", "6.00"],
    `con gross_total abajo, 400 reciclados de monday la bajaban de 20% a 1,4% y el objetivo del slot subía hacia 800: ${JSON.stringify(conv)}`);
  strictEqual(conv[3]?.conversion_pct, "3.00", "con monday en el 70% da efectivos / gross_total, lo mismo que se medía");
  ok(Object.values(conv).every(c => c.effective_added === 6), "los efectivos no cambian");
});
