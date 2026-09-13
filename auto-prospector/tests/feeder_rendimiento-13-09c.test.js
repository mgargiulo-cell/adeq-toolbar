// El feeder mide lo que dice medir, el re-trabajo no se disfraza de descubrimiento y la caché de tráfico
// no esconde un tipo de negocio. (2026-09-13, ronda final de la integración antes del deploy)
//
// "Pool de Prospects limpio, Prospects igual a Análisis, base estable sin fallas." Cada test fija una
// regla que la revisión integrada encontró rota en el código de main (a99f382):
//   I17. El reparto sellers/majestic contaba como "no llegó" las filas pospuestas (next_day por cuota
//        anglo, pending por tráfico transitorio): majestic perdía reparto por filas sin veredicto.
//   I18. El descongelador, el revivir de Prospects-2 y el re-chequeo de ads.txt re-encolaban con la
//        etiqueta del carril: el re-trabajo sumaba al rendimiento del feeder y ocupaba su carril.
//   I19. La fila de caché guardada con RapidAPI caído no tiene categoría ni país y se leía 90 días como
//        una respuesta de SimilarWeb: un sitio de apuestas que volvía a la cola pasaba el veto por tipo.
//   Y los pendientes de la revisión del feeder: frases muertas de AutoGoogle leídas "fallando abierto"
//   (gasto de Serper), el latido 'ok' de monday sin CRM_SYNC_SECRET, una constante sin uso, y la
//   conversión del slot que no entra en su columna y trababa la medición del día.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual, match } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";

process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";
delete process.env.SERPER_API_KEY;   // AutoGoogle nunca sale a Serper en un test

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const resp = (body, { status = 200, total = null, texto = null } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: (k) => (String(k).toLowerCase() === "content-range" && total != null ? `0-0/${total}` : null) },
  json: async () => body,
  text: async () => texto ?? (typeof body === "string" ? body : JSON.stringify(body)),
});
// Un enrutador que anota cada pedido y contesta según la URL; lo que no reconoce, lista vacía.
const enrutar = (pedidos, reglas) => async (url, opts = {}) => {
  const u = String(url), m = (opts.method || "GET").toUpperCase();
  pedidos.push({ u, m, b: String(opts.body || "") });
  for (const [cond, contestar] of reglas) if (cond(u, m, opts)) return contestar(u, m, opts);
  return resp([]);
};
const tick = () => new Promise(r => setImmediate(r));
const ADS_TXT = Array.from({ length: 25 }, (_, i) => `google.com, pub-${1000 + i}, DIRECT, f08c47fec0942fa0`).join("\n");
const HOME = `<!doctype html><html lang="es"><head><title>El Noticiero de Prueba</title></head><body><article><h1>Ultima hora</h1><p>${"Texto de la nota del dia. ".repeat(80)}</p></article></body></html>`;
const USO_APOLLO = { usedToday: 0, limit: 0, monthLimit: 0, usedThisMonth: 0 };
const patchesDeCola = (pedidos, id) => pedidos.filter(p => p.m === "PATCH" && p.u.includes(`toolbar_csv_queue?id=eq.${id}`)).map(p => JSON.parse(p.b));
const postsA = (pedidos, tabla) => pedidos.filter(p => p.m === "POST" && p.u.includes(tabla)).map(p => JSON.parse(p.b));

// ── I17. Pospuestas afuera, congeladas adentro ─────────────────────────────────────────────
test("I17: el reparto sellers/majestic no cuenta las pospuestas como procesadas; las congeladas sí", async () => {
  const { _pesosFeeder } = await cargarWorker(["_pesosFeeder"]);
  const filas = (tag, status, n, error_message = null) => Array.from({ length: n }, () => ({ source: tag, status, error_message }));
  const base = [
    ...filas("auto_feeder_sellers", "done", 30), ...filas("auto_feeder_sellers", "skipped", 70, "not_publisher: sin_ads_txt"),
    ...filas("auto_feeder_majestic", "done", 30), ...filas("auto_feeder_majestic", "skipped", 70, "not_publisher: sin_ads_txt"),
  ];
  const pospuestas = [
    ...filas("auto_feeder_majestic", "next_day", 120, "anglo_daily_quota: United States — vuelve mañana"),
    ...filas("auto_feeder_majestic", "pending", 30, "traffic_api_transient retry_1 (reintento sin penalizar): HTTP 503"),
    ...filas("auto_feeder_majestic", "waiting_pool", 20),
  ];
  const w = _pesosFeeder([...base, ...pospuestas], 0.15);
  deepStrictEqual([w.sellers, w.majestic], [0.5, 0.5],
    `las dos convierten 30 de 100: 170 filas que siguen en la cola no son 170 fracasos de majestic (${w.debug})`);
  match(w.debug, /majestic 30\/100/);
  const conCongeladas = _pesosFeeder([...base, ...pospuestas, ...filas("auto_feeder_majestic", "frozen", 40, "frozen_until_2026-09-28 (15d backoff)")], 0.15);
  match(conCongeladas.debug, /majestic 30\/140/,
    "una congelada sí es una procesada que no llegó: el descongelador la re-encola con otra etiqueta y nunca vuelve a esta fuente");
  ok(conCongeladas.majestic < 0.5, JSON.stringify(conCongeladas));
});

// ── I18. El re-trabajo vuelve como `retry:<fuente>` ─────────────────────────────────────────
test("I18: el re-trabajo se etiqueta retry:<fuente> una sola vez; las de reintento y el import del MB quedan como están", async () => {
  const w = await cargarWorker(["_etiquetaRetrabajo", "_fuenteSinRetrabajo", "_claveFuenteCola", "_origenParaCongelar", "PER_SOURCE_ACTIVE_CAP"]);
  const et = w._etiquetaRetrabajo;
  // Un lead de AutoGoogle que el agente congeló: _origenParaCongelar guarda "autogoogle" y el descongelador lo re-encola.
  const origen = w._origenParaCongelar({ source: "autogoogle", created_by: "worker@autofeeder" });
  deepStrictEqual(w._claveFuenteCola({ source: et(origen.source, { uploadedBy: origen.uploadedBy }), uploaded_by: origen.uploadedBy }),
    { grupo: "retrabajo", fuente: "autogoogle" }, "antes daba grupo 'feeder' e inflaba el rendimiento de Serper");
  strictEqual(et("auto_feeder_sellers"), "retry:auto_feeder_sellers");
  deepStrictEqual(w._claveFuenteCola({ source: et("auto_feeder_sellers"), uploaded_by: "worker@autofeeder" }), { grupo: "retrabajo", fuente: "sellers" });
  strictEqual(et("retry:autogoogle"), "retry:autogoogle", "nunca retry:retry:");
  strictEqual(et(" RETRY:auto_feeder_monday "), "retry:auto_feeder_monday");
  for (const r of ["bounce_retry", "agent_reengagement", "agent", "frozen_retry", "prospects_offline"]) {
    strictEqual(et(r), r, `${r} es del grupo envío o ya dice 'reintento': no lleva prefijo`);
  }
  strictEqual(et(""), "frozen_retry");
  strictEqual(et(null), "frozen_retry");
  strictEqual(et("csv", { uploadedBy: "agustina@adeqmedia.com" }), "csv", "el congelado de un import del MB lo cuenta el informe como import, por la firma");
  strictEqual(et("sellers_json", { uploadedBy: "worker@autofeeder" }), "retry:sellers_json");
  for (const k of Object.keys(w.PER_SOURCE_ACTIVE_CAP)) ok(!/^retry:/.test(k), `el re-trabajo no puede tener carril del feeder: ${k}`);
  strictEqual(w._fuenteSinRetrabajo("retry:auto_feeder_sellers"), "auto_feeder_sellers");
  strictEqual(w._fuenteSinRetrabajo(undefined), "");
});

test("I18: revivir Prospects-2 de verdad encola con retry:, y el carril que mira es el del re-trabajo, no el de sellers", async () => {
  const w = await cargarWorker(["revivirProspectsOffline"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [(u, m) => m === "GET" && u.includes("toolbar_prospects_offline?revived_at=is.null"),
      () => resp([{ id: 7, domain: "revividodeprueba.com.ar", geo: "AR", source: "auto_feeder_sellers" }])],
    [(u) => /\/app-ads\.txt$/.test(u), () => resp("", { status: 404 })],
    [(u) => /\/ads\.txt$/.test(u), () => resp(ADS_TXT)],
    [(u, m) => m === "GET" && u.includes("toolbar_csv_queue?status="), () => resp([], { total: 0 })],
    [(u, m) => m === "POST" && u.includes("toolbar_csv_queue?on_conflict=domain"), (u, m, o) => resp(JSON.parse(o.body))],
  ]);
  await w.revivirProspectsOffline("t");
  const encolado = postsA(pedidos, "toolbar_csv_queue?on_conflict=domain").flat();
  strictEqual(encolado.length, 1, JSON.stringify(pedidos.map(p => p.u)));
  strictEqual(encolado[0].source, "retry:auto_feeder_sellers", "con auto_feeder_sellers ocupaba el carril de sellers y sumaba a su rendimiento");
  ok(pedidos.some(p => p.u.includes("toolbar_csv_queue?status=in.(pending,processing,waiting_pool)&source=eq.retry%3Aauto_feeder_sellers")),
    "el lugar se cuenta en el carril del re-trabajo");
  ok(pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_prospects_offline?id=eq.7")), "y se marca revivido");
});

test("I18: el re-chequeo de ads.txt de verdad devuelve los recuperados con retry:, no con la etiqueta del carril de monday", async () => {
  const w = await cargarWorker(["recheckAdsTxtUnknowns"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [(u) => u.includes("toolbar_adstxt_audit?verdict=eq.unknown"), () => resp([
      { domain: "recuperadodeprueba.com.ar", checks: 1, source: "auto_feeder_monday" },
      { domain: "otrorecuperado.com.ar", checks: 2, source: "sellers_json" },
    ])],
    [(u) => /\/app-ads\.txt$/.test(u), () => resp("", { status: 404 })],
    [(u) => /\/ads\.txt$/.test(u), () => resp(ADS_TXT)],
    [(u, m) => m === "POST" && u.includes("toolbar_csv_queue"), () => resp(null, { status: 201 })],
  ]);
  await w.recheckAdsTxtUnknowns("t");
  const porDominio = Object.fromEntries(postsA(pedidos, "toolbar_csv_queue").flat().map(f => [f.domain, f]));
  strictEqual(porDominio["recuperadodeprueba.com.ar"]?.source, "retry:auto_feeder_monday",
    `el recuperado ocupaba el lugar del reciclado de monday y contaba como feeder: ${JSON.stringify(porDominio)}`);
  strictEqual(porDominio["otrorecuperado.com.ar"]?.source, "retry:sellers_json");
  strictEqual(porDominio["recuperadodeprueba.com.ar"]?.uploaded_by, "worker@autofeeder");
});

// processCsvItem entero con un ítem de re-trabajo. Mismo armado que tests/entrada-13-09b.test.js.
function ruteadorCola(opc = {}) {
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [(u) => u.includes("/api/crm/ficha?domain="), () => resp({ found: false })],
    [(u) => /\/app-ads\.txt$/.test(u), () => resp("", { status: 404 })],
    [(u) => /\/ads\.txt$/.test(u), () => opc.adsTxt || resp(ADS_TXT)],
    [(u, m) => m === "GET" && u.includes("toolbar_traffic_cache?domain=eq."), () => resp(opc.cache || [])],
    [(u) => u.includes("toolbar_review_queue?domain=eq.") && u.includes("status=in.(pending,por_enviar)"), () => resp(opc.enPool ? [{ id: 9 }] : [])],
    [(u) => u.includes("rapidapi.com"), () => (opc.insights ? opc.insights() : resp({ message: "forbidden" }, { status: 403 }))],
    [(u) => /^https?:\/\/[^/]+\/?$/.test(u), () => resp(HOME)],
  ]);
  return pedidos;
}
const itemRetry = (id, domain) => ({ id, domain, source: "retry:auto_feeder_sellers", uploaded_by: "worker@autofeeder", error_message: "unfrozen_retry_attempt_2" });

test("I18: processCsvItem de verdad quita retry: antes de traducir: la auditoría dice sellers_json y el aparcado guarda la etiqueta sin prefijo", async () => {
  const w = await cargarWorker(["processCsvItem"], { fetchFalso: true });
  // a) sin ads.txt: la auditoría anota la etiqueta de Prospects.
  let pedidos = ruteadorCola({ adsTxt: resp("", { status: 404 }) });
  await w.processCsvItem("t", itemRetry(301, "sinadsdeprueba.com.pe"), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  deepStrictEqual(patchesDeCola(pedidos, 301).map(x => x.error_message), ["not_publisher: sin_ads_txt"]);
  const audit = postsA(pedidos, "toolbar_adstxt_audit");
  strictEqual(audit.length, 1);
  strictEqual(audit[0].source, "sellers_json", "con retry: en el switch caía en el default y Prospects/auditoría guardaban 'retry:auto_feeder_sellers'");
  // b) país excluido: se aparca en Prospects-2 con la etiqueta de la cola SIN prefijo (el revivir lo pone una vez).
  pedidos = ruteadorCola({ cache: [{ data: { visits: 2_000_000, rawVisits: 2_000_000, pagesPerVisit: 2, topCountries: [{ code: "PE" }], category: "News and Media" }, fetched_at: new Date().toISOString() }] });
  await w.processCsvItem("t", itemRetry(302, "aparcadodeprueba.com.pe"), { rapidapi_key: "", worker_discovery_config: JSON.stringify({ geos_excluded: ["PE"] }) }, USO_APOLLO, { count: 0 });
  match(patchesDeCola(pedidos, 302)[0]?.error_message || "", /^worker_geo_excluded:/);
  const aparcado = postsA(pedidos, "toolbar_prospects_offline");
  strictEqual(aparcado.length, 1);
  strictEqual(aparcado[0].source, "auto_feeder_sellers");
});

test("I18: el descongelador re-encola con _etiquetaRetrabajo, y processCsvItem traduce la etiqueta sin el prefijo", () => {
  const i = worker.indexOf("toolbar_frozen_leads?frozen_until=lte.");
  ok(i > 0, "no encontré el descongelador");
  const tramo = worker.slice(i, worker.indexOf("toolbar_frozen_leads?domain=eq.", i));
  ok(/source: _etiquetaRetrabajo\(row\.source, \{ uploadedBy: row\.uploaded_by \}\)/.test(tramo), "el descongelador tiene que encolar el re-trabajo con retry:");
  ok(!/source: row\.source \|\| "frozen_retry"/.test(tramo), "la etiqueta cruda volvía a contar como feeder");
  const pci = cuerpoDe("processCsvItem");
  ok(/const _srcCola = _fuenteSinRetrabajo\(item\.source\);\s+switch \(_srcCola\)/.test(pci), "el prefijo se quita ANTES del switch");
  ok(!/switch \(item\.source\)/.test(pci));
});

// ── I19. La fila de caché sin respuesta de la API se completa una vez ─────────────────────────
const HYPESTAT = (unicosDiarios, pvDiarias) =>
  `<html><p>Daily Unique Visitors: <strong>${unicosDiarios}</strong></p><p>Daily Pageviews: <strong>${pvDiarias}</strong></p></html>`;
function redTrafico({ cache = () => [], insights, hypestat = null }) {
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase();
    pedidos.push({ u, m, body: opts.body ? JSON.parse(opts.body) : null });
    if (u.includes("toolbar_traffic_cache")) return m === "POST" ? resp(null, { status: 201 }) : resp(cache());
    if (u.includes("rapidapi.com")) return insights(u);
    if (u.includes("hypestat.com")) return hypestat ? resp(null, { texto: hypestat }) : resp(null, { status: 404 });
    return resp(null, { status: 404 });
  };
  return pedidos;
}
const guardados = (pedidos) => pedidos.filter(p => p.m === "POST" && p.u.includes("toolbar_traffic_cache")).map(p => p.body);
const pagos = (pedidos) => pedidos.filter(p => p.u.includes("rapidapi.com")).length;
const CAIDA = () => resp({ message: "forbidden" }, { status: 403 });
const APUESTAS = () => resp({ Visits: 900_000, WebsiteDetails: { Category: "Gambling/Casinos" }, TopCountryShares: { US: 0.7, MX: 0.2 } });

test("I19: con RapidAPI caído la fila lleva la marca; cuando la API contesta se completa UNA vez, con el mismo número y la misma fecha", async () => {
  const { getTrafficData, _categoriaNuncaProspectable } = await cargarWorker(["getTrafficData", "_categoriaNuncaProspectable"], { fetchFalso: true });
  // 1. API caída, Hypestat rescata 1,5M: se guarda con la marca.
  let pedidos = redTrafico({ insights: CAIDA, hypestat: HYPESTAT("20,000", "50,000") });
  const r1 = await getTrafficData("apuestas-ejemplo.pl", "k");
  await tick();
  strictEqual(r1.pageViews, 1_500_000);
  const fila = guardados(pedidos)[0]?.data;
  ok(fila, "la fila del scrape se guarda (supera el piso)");
  strictEqual(fila.sinRespuestaApi, true, "sin marca, 90 días se leía como una respuesta de SimilarWeb sin categoría");
  strictEqual(fila.category, "");

  // 2. Re-evaluación con la API funcionando y permiso del llamador: un hit, número de la caché, categoría real.
  const FECHA = new Date(Date.now() - 20 * 86400_000).toISOString();
  let cache = [{ data: fila, fetched_at: FECHA }];
  pedidos = redTrafico({ cache: () => cache, insights: APUESTAS });
  const r2 = await getTrafficData("apuestas-ejemplo.pl", "k", { puedeCompletarCategoria: async () => true });
  await tick();
  strictEqual(pagos(pedidos), 1, "un solo hit");
  strictEqual(r2.pageViews, 1_500_000, "el número con el que entró no cambia");
  strictEqual(r2.visits, fila.rawVisits);
  strictEqual(r2.swCategory, "Gambling/Casinos");
  strictEqual(_categoriaNuncaProspectable(r2.swCategory), "gambling", "ahora el veto por tipo lo ve");
  deepStrictEqual(r2.topCountries3, ["US", "MX"]);
  strictEqual(r2.categoriaDesconocida, undefined);
  ok(!pedidos.some(p => p.u.includes("hypestat.com")), "no se vuelve a scrapear");
  const resguardo = guardados(pedidos);
  strictEqual(resguardo.length, 1);
  strictEqual(resguardo[0].data.sinRespuestaApi, undefined, "la marca se quita: no se vuelve a preguntar");
  strictEqual(resguardo[0].data.category, "Gambling/Casinos");
  strictEqual(resguardo[0].data.pageViews, 1_500_000);
  strictEqual(resguardo[0].data.topCountries[0]?.code, "US");
  strictEqual(resguardo[0].fetched_at, FECHA, "con la fecha original: el número del scrape no vive otros 90 días");

  // 3. La tercera vez sale de la caché completa, sin pagar.
  cache = [{ data: resguardo[0].data, fetched_at: FECHA }];
  pedidos = redTrafico({ cache: () => cache, insights: APUESTAS });
  const r3 = await getTrafficData("apuestas-ejemplo.pl", "k", { puedeCompletarCategoria: async () => true });
  strictEqual(pagos(pedidos), 0);
  strictEqual(r3.swCategory, "Gambling/Casinos");
});

test("I19: sin permiso, en Prospects, o con la API todavía caída, no se paga dos veces ni se re-scrapea", async () => {
  const { getTrafficData } = await cargarWorker(["getTrafficData"], { fetchFalso: true });
  const marcada = { rawVisits: 600_000, visits: 600_000, pageViews: 1_500_000, pagesPerVisit: 2.5, ppvSource: "hypestat", topCountries: [], category: "", source: "hypestat_scrape", sinRespuestaApi: true };
  const cache = () => [{ data: marcada, fetched_at: new Date().toISOString() }];

  // Sin la opción (el agente y el autopilot): nunca completa.
  let pedidos = redTrafico({ cache, insights: APUESTAS });
  let r = await getTrafficData("sitio-ejemplo.pl", "k");
  strictEqual(pagos(pedidos), 0, "el agente sólo llama por leads del pool: ahí el tráfico nunca se vuelve a consultar");
  strictEqual(r.categoriaDesconocida, true);
  strictEqual(r.pageViews, 1_500_000);

  // El llamador dice que está en Prospects (false) o el chequeo explota: no se paga.
  for (const puede of [async () => false, async () => { throw new Error("base caída"); }, async () => null]) {
    pedidos = redTrafico({ cache, insights: APUESTAS });
    r = await getTrafficData("sitio-ejemplo.pl", "k", { puedeCompletarCategoria: puede });
    strictEqual(pagos(pedidos), 0);
    strictEqual(r.categoriaDesconocida, true);
  }

  // La API sigue caída: un intento, la caché como antes, sin re-scrapear ni guardar.
  pedidos = redTrafico({ cache, insights: CAIDA, hypestat: HYPESTAT("20,000", "50,000") });
  r = await getTrafficData("sitio-ejemplo.pl", "k", { puedeCompletarCategoria: async () => true });
  await tick();
  strictEqual(pagos(pedidos), 1);
  strictEqual(r.categoriaDesconocida, true);
  strictEqual(r.pageViews, 1_500_000);
  ok(!pedidos.some(p => p.u.includes("hypestat.com")), "no se re-scrapea");
  strictEqual(guardados(pedidos).length, 0);

  // Control: una fila en la que la API sí contestó (sin marca) sale de la caché sin pagar.
  const { sinRespuestaApi, ...contesto } = marcada;
  pedidos = redTrafico({ cache: () => [{ data: contesto, fetched_at: new Date().toISOString() }], insights: APUESTAS });
  r = await getTrafficData("sitio-ejemplo.pl", "k", { puedeCompletarCategoria: async () => true });
  strictEqual(pagos(pedidos), 0);
  strictEqual(r.categoriaDesconocida, undefined);
});

test("I19: processCsvItem de verdad: un sitio de apuestas guardado con la API caída ya no pasa el veto por tipo; si está en Prospects no se paga", async () => {
  const w = await cargarWorker(["processCsvItem"], { fetchFalso: true });
  const cache = [{ data: { rawVisits: 600_000, visits: 600_000, pageViews: 1_500_000, pagesPerVisit: 2.5, ppvSource: "hypestat", topCountries: [], category: "", source: "hypestat_scrape", sinRespuestaApi: true }, fetched_at: new Date().toISOString() }];
  const cfg = { rapidapi_key: "k", worker_discovery_config: JSON.stringify({ geos_excluded: ["PE"] }) };
  const item = (id) => ({ id, domain: "portaldeprueba.com.pe", source: "auto_feeder_sellers", uploaded_by: "worker@autofeeder", error_message: "unfrozen_retry_attempt_2" });

  let pedidos = ruteadorCola({ cache, insights: APUESTAS });
  await w.processCsvItem("t", item(401), cfg, USO_APOLLO, { count: 0 });
  strictEqual(pedidos.filter(p => p.u.includes("rapidapi.com")).length, 1, "un hit para completar la categoría");
  const p = patchesDeCola(pedidos, 401);
  strictEqual(p.length, 1, JSON.stringify(p));
  match(p[0].error_message, /^no_prospectable_tipo: "Gambling\/Casinos" es gambling/,
    "con la categoría vacía el sitio seguía de largo (acá, aparcado por país excluido, y sin país podía entrar)");
  strictEqual(postsA(pedidos, "toolbar_prospects_offline").length, 0);

  // El mismo dominio en la tanda 'Por enviar' de un MB: el tráfico de un lead de Prospects no se vuelve a consultar.
  pedidos = ruteadorCola({ cache, insights: APUESTAS, enPool: true });
  await w.processCsvItem("t", item(402), cfg, USO_APOLLO, { count: 0 });
  strictEqual(pedidos.filter(p => p.u.includes("rapidapi.com")).length, 0);
  // Integración (13/09): desde estados_cola el chequeo previo de processCsvItem también ve `por_enviar`,
  // así que el lead ni llega al tráfico ni a la GEO: sale antes, sin pagar nada. Lo que este test cuida
  // (cero RapidAPI para un lead de Prospects) sigue igual; cambia sólo en qué puerta se detiene.
  match(patchesDeCola(pedidos, 402)[0]?.error_message || "", /^ya_estaba_en_prospects/);
});

// ── Pendientes de la revisión del feeder ───────────────────────────────────────────────────
test("AutoGoogle: sin poder leer las frases retiradas la exploración no sale (Serper es pago); con la lista, sí", async () => {
  // El slot entero primero: la línea del log dice cuántas frases de exploración salieron.
  for (const caso of ["caida", "leida"]) {
    const w = await cargarWorker(["_runAutoGoogleSlot"], { fetchFalso: true });
    const pedidos = [];
    globalThis.__fetchFalso = enrutar(pedidos, [
      [(u) => u.includes("toolbar_csv_queue?status=in.(pending,processing,waiting_pool)&source=eq.autogoogle"), () => resp([], { total: 0 })],
      [(u) => u.includes("toolbar_keyword_yield?searches=gte.10"), () => (caso === "caida" ? resp({ message: "boom" }, { status: 500 }) : resp([]))],
    ]);
    const lineas = [];
    const logReal = console.log;
    console.log = (...a) => { lineas.push(a.join(" ")); };
    try { await w._runAutoGoogleSlot("t", "2026-09-14-10:00"); } finally { console.log = logReal; }
    const l = lineas.find(x => /AutoGoogle slot .* búsquedas \(\d+ top-yield \+ \d+ explore/.test(x));
    ok(l, `no encontré la línea del slot: ${lineas.slice(-5).join(" | ")}`);
    const explore = Number(/\+ (\d+) explore/.exec(l)[1]);
    if (caso === "caida") strictEqual(explore, 0, `con la lista de muertas ilegible volvía a explorar frases retiradas: ${l}`);
    else ok(explore > 0, `control: con la lista leída la exploración sale: ${l}`);
  }
  // La regla pura que usa el slot.
  const { _frasesDeExploracion } = await cargarWorker(["_frasesDeExploracion"]);
  deepStrictEqual(_frasesDeExploracion(["a", "b", "c", "d"], { elegidas: new Set(["a"]), muertas: new Set(["c"]) }), ["b", "d"]);
  deepStrictEqual(_frasesDeExploracion(["a", "b"], { elegidas: new Set(), muertas: null }), [], "lista ilegible = no explorar, nunca 'no hay muertas'");
  deepStrictEqual(_frasesDeExploracion(["a", "b"], { elegidas: new Set(), muertas: new Set() }), ["a", "b"], "lista leída y vacía = explorar todo");
});

test("monday: sin CRM_SYNC_SECRET el slot late 'fail' antes de mirar el carril; y FEEDER_SOURCE_KEYS ya no existe", () => {
  const c = cuerpoDe("_feederPullMonday");
  const iSecreto = c.indexOf("!CRM_SYNC_SECRET");
  const iLleno = c.indexOf('detalle: "carril lleno: no hacía falta');   // el latido, no un comentario
  ok(iSecreto > 0 && iLleno > 0 && iSecreto < iLleno, "con el carril lleno latía 'ok, no hacía falta' aunque no pudiera reciclar nunca");
  ok(/!CRM_SYNC_SECRET\) \{[\s\S]{0,300}saludPing\(token, "feeder_monday", \{ status: "fail"/.test(c), "sin secreto tiene que latir fail, como el barrido diario");
  ok(!/const FEEDER_SOURCE_KEYS\b/.test(worker), "constante sin uso desde que el reparto es de dos fuentes");
});

test("la conversión que no entra en numeric(5,2) no traba la medición: queda sin conversión, con sus efectivos, y no se re-mide", async () => {
  const { _measureFeederRuns } = await cargarWorker(["_measureFeederRuns"], { fetchFalso: true });
  const hace1h = new Date(Date.now() - 60 * 60_000).toISOString();
  const runs = [
    { id: 11, cron_at: hace1h, gross_sellers: 1, gross_majestic: 1, gross_total: 2 },     // 70 / (2 / 0,30) = 1050%
    { id: 12, cron_at: hace1h, gross_sellers: 20, gross_majestic: 10, gross_total: 30 },  // 70 / 100 = 70%
  ];
  const pedidos = [];
  globalThis.__fetchFalso = enrutar(pedidos, [
    [(u, m) => m === "GET" && u.includes("toolbar_feeder_runs?status=eq.ok&conversion_pct=is.null"), () => resp(runs)],
    [(u) => u.includes("toolbar_csv_queue?status=eq.done"), () => resp([], { total: 70 })],
    [(u, m) => m === "PATCH", () => resp(null, { status: 204 })],
  ]);
  await _measureFeederRuns("t");
  const lectura = pedidos.find(p => p.m === "GET" && p.u.includes("toolbar_feeder_runs?"));
  ok(/effective_added=eq\.0/.test(lectura.u), "una corrida ya medida sin conversión no se vuelve a pedir en cada vuelta");
  const cuerpo = Object.fromEntries(pedidos.filter(p => p.m === "PATCH" && p.u.includes("toolbar_feeder_runs?id=eq."))
    .map(p => [p.u.match(/id=eq\.(\d+)/)[1], JSON.parse(p.b)]));
  strictEqual(cuerpo[11]?.conversion_pct, null, `"1050.00" no entra en numeric(5,2): el PATCH fallaba con 400 y la corrida se re-medía todo el día (${JSON.stringify(cuerpo)})`);
  strictEqual(cuerpo[11]?.effective_added, 70);
  strictEqual(cuerpo[12]?.conversion_pct, "70.00", "lo que entra se guarda igual que siempre");
  // La regla pura.
  const { _conversionParaColumna } = await cargarWorker(["_conversionParaColumna"]);
  strictEqual(_conversionParaColumna(1050), null);
  strictEqual(_conversionParaColumna(999.99), "999.99");
  strictEqual(_conversionParaColumna(6), "6.00");
  strictEqual(_conversionParaColumna(NaN), null);
});
