// La entrada de la cola: una falla NUESTRA no puede enterrar un lead, y las puertas van en el orden
// que gasta menos. (2026-09-13, revisión de la entrada pedida por el dueño)
//
// "Dejar una base de conceptos, lógicas y funcionamientos automáticos sin fallas: lo que arreglás
// un día no lo rompas al otro." Cada test es una regla que la revisión encontró rota en el código
// real de processCsvItem y runCsvQueue:
//   1. La marca freeze_N del descongelador se borraba en tres reescrituras: el castigo volvía a 15 días.
//   2. Un ex cliente del CRM sin datos de SimilarWeb terminaba en la blocklist permanente.
//   3. Guardar en Prospects con la base caída dejaba 'skipped' (final) un lead que ya había pagado todo.
//   4. Congelar con la base caída dejaba 'skipped' sin contador; un 5xx ni siquiera se veía.
//   5. El techo de 4 minutos marcaba 'error' al primer desborde y podía pisar un 'done'.
//   6. El CRM que no contesta dejaba la fila en 'processing' y el aviso decía que "entraron".
//   7. La GEO excluida pagaba Haiku antes de aparcarse; los vetos estructurales pagaban RapidAPI.
//   8. Textos que mentían (el reintento de sin_ads_txt, "90 días") y descartes sin comentario.
//   9. Inventario de las salidas 'skipped': una nueva sin prefijo conocido rompe acá.
//
// Los tests "de verdad" corren processCsvItem entero contra respuestas inventadas (sin red).
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual, match, doesNotMatch } from "node:assert";
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

const resp = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: () => null },
  json: async () => body,
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
globalThis.__fetchFalso = async () => resp([]);

const W = await cargarWorker([
  "_conMarcaFreeze", "_vaABlocklistInoperativo", "_estadoTrasGuardar", "_guardadoFallidoPorRed",
  "_estadoTrasFreezeFallido", "_estadoTrasTimeout", "_marcarCsvSiSigueProcesando", "_backoffCongelado",
  "processCsvItem",
], { fetchFalso: true });

// Un ads.txt de 25 líneas, una home con nota y título, y el CRM que dice "no está".
const ADS_TXT = Array.from({ length: 25 }, (_, i) => `google.com, pub-${1000 + i}, DIRECT, f08c47fec0942fa0`).join("\n");
const HOME = `<!doctype html><html lang="es"><head><title>El Noticiero de Prueba</title></head><body><article><h1>Ultima hora</h1><p>${"Texto de la nota del dia. ".repeat(80)}</p></article></body></html>`;
function ruteador(opc = {}) {
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase();
    reg.push({ u, m, b: String(opts.body || "") });
    if (u.includes("/api/crm/ficha?domain=")) return opc.crm || resp({ found: false });
    if (/\/app-ads\.txt$/.test(u)) return resp("", { status: 404 });
    if (/\/ads\.txt$/.test(u)) return opc.adsTxt || resp(ADS_TXT);
    if (u.includes("toolbar_traffic_cache?domain=eq.")) return resp(opc.cache || []);
    if (u.includes("toolbar_frozen_leads") && m === "POST") return opc.frozenPost || resp(null, { status: 201 });
    if (/^https?:\/\/[^/]+\/?$/.test(u)) return resp(opc.home || HOME);
    return resp([]);
  };
  return reg;
}
const patchesDeCola = (reg, id) => reg.filter(r => r.m === "PATCH" && r.u.includes(`toolbar_csv_queue?id=eq.${id}`)).map(r => JSON.parse(r.b));
const USO_APOLLO = { usedToday: 0, limit: 0, monthLimit: 0, usedThisMonth: 0 };
const itemDe = (id, domain, error_message = "") => ({ id, domain, source: "auto_feeder_sellers", uploaded_by: "worker@autofeeder", error_message });
const sinDatosEnCache = () => [{ data: { noData: true }, fetched_at: new Date().toISOString() }];

// Todas las llamadas markCsvItem(token, item.id, status, { error_message }) de una función, leídas
// del árbol de sintaxis: el estado, el comienzo literal del mensaje y la función que lo envuelve.
const AST = acorn.parse(worker, { ecmaVersion: "latest", sourceType: "module" });
function marcasDeCola(nombre) {
  let fn = null;
  walk.simple(AST, { FunctionDeclaration(n) { if (n.id?.name === nombre) fn = n; } });
  ok(fn, `no encontré ${nombre} en el árbol`);
  const out = [];
  walk.simple(fn, {
    CallExpression(n) {
      if (n.callee.type !== "Identifier" || n.callee.name !== "markCsvItem") return;
      const [, , st, campos] = n.arguments;
      const prop = campos?.type === "ObjectExpression" ? campos.properties.find(p => p.key?.name === "error_message") : null;
      let v = prop?.value, envuelto = "";
      if (v?.type === "CallExpression" && v.callee.type === "Identifier") { envuelto = v.callee.name; v = v.arguments[0]; }
      const prefijo = v?.type === "Literal" ? String(v.value) : v?.type === "TemplateLiteral" ? v.quasis[0].value.cooked : null;
      out.push({ literal: st?.type === "Literal", status: st?.type === "Literal" ? st.value : worker.slice(st.start, st.end), prefijo, envuelto, fuente: worker.slice(n.start, n.end) });
    },
  });
  return out;
}

// ── 1. La marca freeze_N ────────────────────────────────────────────────────────────────
test("la marca freeze_N del descongelador sobrevive a las tres reescrituras por falta de tráfico", () => {
  const { _conMarcaFreeze, _backoffCongelado } = W;
  strictEqual(_conMarcaFreeze("no_traffic_data — attempt_2/3", "unfrozen_retry_attempt_1 freeze_1"), "no_traffic_data — attempt_2/3 freeze_1");
  strictEqual(_conMarcaFreeze("x", ""), "x", "sin marca previa no inventa nada");
  strictEqual(_conMarcaFreeze("x freeze_2", "unfrozen_retry_attempt_2 freeze_1"), "x freeze_2", "no duplica ni pisa");
  strictEqual(_conMarcaFreeze("y", "no_traffic_data — attempt_2/3 freeze_fail_1"), "y", "freeze_fail_N es otro contador, no el ciclo");

  const msg = "traffic_api_transient retry_1 (reintento sin penalizar): timeout freeze_1";
  strictEqual(_backoffCongelado({ attemptFila: 0, errorMessage: msg }).dias, 30, "con la marca, el segundo congelado es de 30 días");
  strictEqual(parseInt(msg.match(/retry_(\d+)/)?.[1] || "0", 10), 1, "el contador de reintentos se lee igual");
  strictEqual(msg.match(/attempt_(\d+)/), null, "y no se confunde con un intento");
  const m2 = _conMarcaFreeze("no_traffic_data — attempt_2/3", "unfrozen_retry_attempt_1 freeze_1");
  strictEqual(parseInt(m2.match(/attempt_(\d+)/)?.[1] || "0", 10), 2, "el contador de intentos se lee igual");

  const salidas = marcasDeCola("processCsvItem");
  for (const pre of ["sin_cuota_de_api", "traffic_api_transient retry_", "no_traffic_data — attempt_"]) {
    const s = salidas.filter(x => x.prefijo?.startsWith(pre));
    strictEqual(s.length, 1, `esperaba una sola salida "${pre}"`);
    strictEqual(s[0].envuelto, "_conMarcaFreeze", `la salida "${pre}" reescribe el mensaje sin conservar freeze_N`);
  }
});

test("processCsvItem de verdad: un descongelado sin datos vuelve a la cola con attempt_2 y su freeze_1", async () => {
  const reg = ruteador({ cache: sinDatosEnCache() });
  await W.processCsvItem("t", itemDe(101, "elnoticierodeprueba.com.pe", "unfrozen_retry_attempt_1 freeze_1"), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  deepStrictEqual(patchesDeCola(reg, 101).map(x => [x.status, x.error_message]), [["pending", "no_traffic_data — attempt_2/3 freeze_1"]]);
});

// ── 2. El CRM y la blocklist permanente ─────────────────────────────────────────────────
test("un ex cliente del CRM sin datos de SimilarWeb no termina en la blocklist permanente (el congelado de 60 días sigue)", () => {
  const f = W._vaABlocklistInoperativo;
  strictEqual(f({ prevFreeze: 2, deCache: false, source: "sellers_json" }), true, "tercer congelado de un dominio descubierto: blocklist, como antes");
  strictEqual(f({ prevFreeze: 2, deCache: false, source: "monday_refresh" }), false, "del CRM: nunca");
  strictEqual(f({ prevFreeze: 3, deCache: true, source: "similar" }), false, "con un 'sin datos' de la caché: nunca (36fb125)");
  strictEqual(f({ prevFreeze: 1, deCache: false, source: "sellers_json" }), false, "antes del tercer congelado: nunca");

  const cuerpo = cuerpoDe("processCsvItem");
  match(cuerpo, /case "auto_feeder_monday":\s+source = "monday_refresh";/, "el reciclado del CRM llega como monday_refresh");
  match(cuerpo, /source = "monday_refresh";\s+mondayItemId = null;/, "y todo lo que tiene ficha en el CRM también");
  match(cuerpo, /if \(_vaABlocklistInoperativo\(\{ prevFreeze, deCache: !!trafficData\.fromCache, source \}\)\) \{/);
  ok(cuerpo.indexOf("_vaABlocklistInoperativo(") < cuerpo.indexOf("rest/v1/toolbar_frozen_leads`, {"), "el congelado se escribe después y fuera de esa condición");
});

// ── 3. Guardar en Prospects ─────────────────────────────────────────────────────────────
test("guardar en Prospects: los veredictos se descartan, la falla pasajera vuelve hasta 3 veces y la base rota queda en error con aviso", () => {
  const f = W._estadoTrasGuardar;
  deepStrictEqual(f("ok"), { status: "done", error_message: null, alerta: false });
  for (const v of ["dup", "floor", "contactado_hace_poco"]) {
    deepStrictEqual(f(v), { status: "skipped", error_message: `review_queue_insert_fail:${v}`, alerta: false }, `${v} es un veredicto`);
  }
  deepStrictEqual(f('http_503:{"message":"upstream"}'), { status: "next_day", error_message: "reintentar: insercion http_503 ins_1", alerta: false });
  strictEqual(f("http_503:x", "reintentar: insercion http_503 ins_2").error_message, "reintentar: insercion http_503 ins_3");
  const agotado = f("http_503:x", "reintentar: insercion http_503 ins_3");
  strictEqual(agotado.status, "error"); strictEqual(agotado.alerta, true);
  for (const v of ["http_408:x", "http_429:x", "http_502:x", "http_net:socket hang up"]) strictEqual(f(v).status, "next_day", `${v} es pasajero`);
  for (const v of ["http_400:bad", "http_401:jwt", "http_404:x", "http_max_retries", "algo_raro"]) {
    const r = f(v);
    deepStrictEqual([r.status, r.alerta], ["error", true], `${v} es la base rota: nunca next_day, nunca skipped`);
  }
  strictEqual(f("http_429:x", "traffic_api_transient retry_15 (reintento sin penalizar): timeout").error_message, "reintentar: insercion http_429 ins_1",
    "el retry_15 del tráfico no es el contador de la inserción");
  const conTexto = f('http_503:{"hint":"retry_5 attempt_2 freeze_3"}').error_message;
  strictEqual(conTexto.match(/retry_(\d+)|attempt_(\d+)|\bfreeze_\d+/), null, "el texto de la base no se copia: confundiría a los otros contadores");
  // El parte cuenta como "vuelven mañana" los next_day que matchean esto (parteDelDia).
  match(worker, /if \(\/reintentar\|no_verificable\|sin_cuota\/i\.test\(m\)\) _reintentables\+\+/);
  match(f("http_502:x").error_message, /reintentar|no_verificable|sin_cuota/i);
});

test("un corte de red al guardar vuelve mañana como un 503; un bug sigue yendo a error", () => {
  const timeout = Object.assign(new Error("The operation was aborted due to timeout"), { name: "TimeoutError" });
  match(W._guardadoFallidoPorRed(timeout), /^http_net:/);
  match(W._guardadoFallidoPorRed(new TypeError("fetch failed")), /^http_net:/);
  strictEqual(W._guardadoFallidoPorRed(new TypeError("Cannot read properties of undefined (reading 'ok')")), null);
  strictEqual(W._estadoTrasGuardar(W._guardadoFallidoPorRed(timeout)).status, "next_day");

  const cuerpo = cuerpoDe("processCsvItem");
  match(cuerpo, /\}\)\.catch\(\(e\) => \{ const _red = _guardadoFallidoPorRed\(e\); if \(_red\) return _red; throw e; \}\);/, "el guardado traduce el corte de red");
  match(cuerpo, /const _est = _estadoTrasGuardar\(saved, item\.error_message\);/);
  match(cuerpo, /markCsvItem\(token, item\.id, _est\.status, \{/);
  match(cuerpo, /clave: "prospects-insert-roto", severidad: "error"/, "la base rota avisa");
});

// ── 4. Congelar ─────────────────────────────────────────────────────────────────────────
test("processCsvItem de verdad: congelar con la base caída no entierra el lead; vuelve con su contador y a la cuarta es error", async () => {
  const reg = ruteador({ cache: sinDatosEnCache(), frozenPost: resp({ message: "upstream" }, { status: 503 }) });
  await W.processCsvItem("t", itemDe(102, "otrodiariodeprueba.com.pe", "unfrozen_retry_attempt_2 freeze_2"), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  const p = patchesDeCola(reg, 102);
  deepStrictEqual(p.map(x => x.status), ["pending"], "un 503 al congelar dejaba la fila 'frozen' sin fila de congelado, y el descongelador no la veía nunca");
  strictEqual(p[0].error_message, "no_traffic_data — attempt_2/3 freeze_fail_1 freeze_2");
  ok(!reg.some(r => r.m === "POST" && r.u.includes("toolbar_url_blocklist")), "con un 'sin datos' de la caché nunca va a la blocklist");
  // La vuelta siguiente llega directo al congelado, con el castigo que corresponde.
  strictEqual(parseInt(p[0].error_message.match(/attempt_(\d+)/)[1], 10) + 1, 3);
  strictEqual(W._backoffCongelado({ attemptFila: 0, errorMessage: p[0].error_message }).dias, 60);

  deepStrictEqual(W._estadoTrasFreezeFallido({ mensajePrevio: "no_traffic_data — attempt_2/3 freeze_fail_2 freeze_1", prevAttempts: 2, error: "x" }),
    { status: "pending", error_message: "no_traffic_data — attempt_2/3 freeze_fail_3 freeze_1" });
  const cuarta = W._estadoTrasFreezeFallido({ mensajePrevio: "no_traffic_data — attempt_2/3 freeze_fail_3", prevAttempts: 2, error: "HTTP 503" });
  strictEqual(cuarta.status, "error");
  match(cuarta.error_message, /^freeze_failed: /, "el informe lo sigue agrupando como freeze_failed");
});

// ── 5. El techo de 4 minutos ────────────────────────────────────────────────────────────
test("el techo de 4 minutos: la primera vez vuelve mañana, la segunda es error, y nunca pisa un estado ya escrito", async () => {
  deepStrictEqual(W._estadoTrasTimeout(""), { status: "next_day", error_message: "reintentar: item_timeout_4min tmo_1" });
  strictEqual(W._estadoTrasTimeout("reintentar: item_timeout_4min tmo_1").status, "error");
  strictEqual(W._estadoTrasTimeout("unfrozen_retry_attempt_2 freeze_1").error_message, "reintentar: item_timeout_4min tmo_1 freeze_1");

  const reg = ruteador();
  await W._marcarCsvSiSigueProcesando("t", 55, "next_day", { error_message: "x" });
  const p = reg.find(r => r.m === "PATCH");
  ok(p && p.u.includes("toolbar_csv_queue?id=eq.55&status=eq.processing"), `el PATCH tiene que exigir processing: ${p?.u}`);

  const cuerpo = cuerpoDe("runCsvQueue");
  match(cuerpo, /const _t = _estadoTrasTimeout\(item\.error_message\);/);
  match(cuerpo, /await _marcarCsvSiSigueProcesando\(token, item\.id, _t\.status, \{ error_message: _t\.error_message \}\);/);
  doesNotMatch(cuerpo, /markCsvItem\(token, item\.id, "error", \{ error_message: "item_timeout_4min/);
});

// ── 6. El CRM que no contesta ───────────────────────────────────────────────────────────
test("processCsvItem de verdad: el CRM que no contesta devuelve la fila a pending sin gastar nada, y runCsvQueue corta la tanda", async () => {
  const reg = ruteador({ crm: resp({ error: "down" }, { status: 503 }) });
  const r = await W.processCsvItem("t", itemDe(103, "tercerdiariodeprueba.com.pe"), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  strictEqual(r, "crm_indeterminado");
  deepStrictEqual(patchesDeCola(reg, 103), [{ status: "pending" }], "quedaba en 'processing' hasta el próximo reinicio");
  ok(!reg.some(x => /ads\.txt|toolbar_traffic_cache/.test(x.u)), "corre antes de ads.txt y del tráfico: no se gastó nada");

  const cuerpo = cuerpoDe("runCsvQueue");
  match(cuerpo, /_resultadoItem = await Promise\.race\(/);
  match(cuerpo, /if \(_resultadoItem === "crm_indeterminado"\) \{[\s\S]{0,500}?break;/);
  const iAviso = cuerpo.indexOf('clave: "ficha-crm-no-responde"');
  ok(iAviso >= 0, "falta el aviso del CRM");
  const aviso = cuerpo.slice(cuerpo.lastIndexOf("if (", iAviso), cuerpo.indexOf("}).catch(", iAviso));
  doesNotMatch(aviso, /entraron/i, "esos dominios no entraron: el aviso y el latido tienen que decir lo que pasó");
  match(aviso, /volvi(ó|eron) a la cola|vuelven a la cola/);
});

// ── 7. El orden de las puertas ──────────────────────────────────────────────────────────
test("orden de las puertas: vetos estructurales antes de pagar el tráfico, GEO excluida antes de las cuotas y del detector", () => {
  const f = cuerpoDe("processCsvItem");
  const idx = (k) => { const i = f.indexOf(k); ok(i >= 0, `no encontré ${k}`); return i; };
  ok(idx("_VETO_ESTRUCTURAL.test") < idx("await getTrafficData(domain"), "el veto estructural pagaba RapidAPI");
  strictEqual((f.match(/const category =/g) || []).length, 1);
  ok(idx("if (pageContent?.dead)") < idx("const category ="), "category necesita pageContent");
  ok(idx("const category =") < idx("worker_discovery_config"));
  ok(idx("worker_discovery_config") < idx("_isWorkerDeprioGeo("));
  ok(idx("worker_discovery_config") < idx("_isGeoOverrepresentedInPool(token"), "un país excluido no puede girar en next_day por la saturación");
  ok(idx("worker_discovery_config") < idx("_isAngloOverDailyQuota(token, topCountry"), "ni por la cuota Anglo");
  ok(idx("_isGeoOverrepresentedInPool(token") < idx("pub = await classifyPublisher("));
  for (const k of ["const category =", "const pageTitle =", "const adNetworks ="]) {
    ok(idx(k) < idx("aparcarProspectOffline(token"), `${k} tiene que existir antes de aparcar: si no, "Cannot access before initialization"`);
  }
});

test("processCsvItem de verdad: una plataforma de cursos con ads.txt se descarta por la URL sin preguntar el tráfico", async () => {
  const reg = ruteador();
  await W.processCsvItem("t", itemDe(104, "cursos.edutin.com"), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  deepStrictEqual(patchesDeCola(reg, 104).map(x => [x.status, x.error_message]), [["skipped", "not_publisher: url_elearning"]]);
  ok(!reg.some(x => x.u.includes("toolbar_traffic_cache") || x.u.includes("rapidapi")), "no se consulta el tráfico de un veto estructural");
});

test("processCsvItem de verdad: un medio de un país excluido se aparca en Prospects-2 con su título, y el comentario no promete lo que no se miró", async () => {
  const reg = ruteador({ cache: [{ data: { visits: 2_000_000, rawVisits: 2_000_000, pagesPerVisit: 2, topCountries: [{ code: "PE" }], category: "News and Media" }, fetched_at: new Date().toISOString() }] });
  const cfg = { rapidapi_key: "", worker_discovery_config: JSON.stringify({ geos_excluded: ["PE"] }) };
  await W.processCsvItem("t", itemDe(105, "cuartodiariodeprueba.com.pe"), cfg, USO_APOLLO, { count: 0 });
  const p = patchesDeCola(reg, 105);
  strictEqual(p.length, 1, JSON.stringify(p));
  match(p[0].error_message, /^worker_geo_excluded:/);
  const aparcado = reg.find(x => x.m === "POST" && x.u.includes("toolbar_prospects_offline"));
  ok(aparcado, "no se aparcó");
  const b = JSON.parse(aparcado.b);
  strictEqual(b.page_title, "El Noticiero de Prueba");
  match(b.motivo, /^geo_excluida:/);
  const diag = reg.find(x => x.m === "POST" && x.u.includes("toolbar_diag_descartes"));
  ok(diag, "el aparcado deja su diagnóstico");
  match(JSON.parse(diag.b).comentario, /el detector de no-publisher corre recién cuando se reviva/);
});

// ── 8. Textos y comentarios ─────────────────────────────────────────────────────────────
test("processCsvItem de verdad: sin ads.txt, el comentario ya no promete un reintento que no existe", async () => {
  const reg = ruteador({ adsTxt: resp("", { status: 404 }), home: "<html><head><title>Sitio</title></head><body>hola</body></html>" });
  await W.processCsvItem("t", itemDe(106, "quintodiariodeprueba.com.pe"), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  deepStrictEqual(patchesDeCola(reg, 106).map(x => [x.status, x.error_message]), [["skipped", "not_publisher: sin_ads_txt"]]);
  const diag = reg.find(x => x.m === "POST" && x.u.includes("toolbar_diag_descartes"));
  ok(diag, "sin diagnóstico");
  const c = JSON.parse(diag.b).comentario;
  doesNotMatch(c, /recheckAdsTxtUnknowns/, "ese job sólo toma los ilegibles, nunca este 'no'");
  match(c, /no se reintenta solo/);
});

test("categoría bloqueada y detector de no-publisher dejan comentario sin columnas nuevas, y la revisión periódica dice los días reales", () => {
  const f = cuerpoDe("processCsvItem");
  for (const pre of ["error_message: `category-blocked:", "error_message: `not_publisher: ${pub.reason}`"]) {
    const i = f.indexOf(pre);
    ok(i >= 0, `no encontré ${pre}`);
    match(f.slice(i, f.indexOf("return;", i)), /registrarDiagDescarte\(token, \{/, `${pre} no dejaba comentario para el resumen de salud`);
  }
  // Una columna nueva sin su ALTER TABLE haría fallar TODOS los inserts en silencio (el catch vacío).
  const cols = [...cuerpoDe("registrarDiagDescarte").matchAll(/(\w+): d\./g)].map(m => m[1]).sort();
  deepStrictEqual(cols, ["categoria", "comentario", "decidido_por", "domain", "etapa", "geo", "motivo", "tenia_ads_txt", "traffic"]);

  const rev = cuerpoDe("revisarDescartesCondicionales");
  doesNotMatch(rev, /ya pasaron 90 días|\+90 días/, "el corte es de 45 días");
  match(rev, /ya pasaron \$\{REVISAR_CONDICIONALES_ANTIGUEDAD\} días/);
});

// ── 9. Inventario de salidas ────────────────────────────────────────────────────────────
test("inventario: toda salida 'skipped' de processCsvItem tiene un prefijo conocido, y las fallas nuestras salen de una regla probada", () => {
  // Si agregás una salida, sumá su prefijo acá Y al agrupador de motivos del informe: así el mail
  // no muestra un "?" ni la esconde.
  const PREFIJOS = [
    "ya_estaba_en_prospects", "blocked: ", "en descanso: ", "crm_activo: ", "not_publisher: ",
    "traffic_api_transient: 20 reintentos", "pageviews ", "duplicate_subdomain_of:", "dead_domain_dns_fail",
    "no_prospectable_tipo: ", "category-blocked: ", "worker_geo_excluded:", "worker_cat_not_priority:", "deprio-geo: ",
  ];
  const salidas = marcasDeCola("processCsvItem");
  const skipped = salidas.filter(s => s.literal && s.status === "skipped");
  ok(skipped.length >= 15, `esperaba al menos 15 salidas skipped, hay ${skipped.length}`);
  const desconocidas = skipped.filter(s => !s.prefijo || !PREFIJOS.some(p => s.prefijo.startsWith(p)));
  deepStrictEqual(desconocidas.map(s => s.fuente.slice(0, 140)), [], "salida 'skipped' nueva o sin prefijo literal");
  ok(!skipped.some(s => /^(review_queue_insert_fail|freeze_failed)/.test(s.prefijo || "")), "una falla nuestra no puede quedar 'skipped' escrita a mano");
  deepStrictEqual(salidas.filter(s => !s.literal).map(s => s.status).sort(), ["_est.status", "_ff.status"],
    "un estado calculado tiene que salir de _estadoTrasGuardar o _estadoTrasFreezeFallido, que tienen test");
});
