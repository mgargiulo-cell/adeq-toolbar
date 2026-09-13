// Tarjeta de Prospects sin gasto de RapidAPI y orden de tipos de email sin muestra. (2026-09-13)
//
// Dos defectos que marcaron los agentes de la auditoría del pool fuera de su zona:
//   X1. Regla del dueño (18/08): el tráfico de un lead de Prospects no se vuelve a consultar. La
//       tarjeta llamaba a getTraffic al dibujarse (y al expandirse) cuando su fila venía con tráfico
//       0 o vacío. La fila del pool sólo cuenta con traffic >= 1000, así que sin caché —o con el
//       negativo del worker, que para el MB no es caché— se pagaba RapidAPI en cada dibujo.
//   X2. reajustarPrioridadTiposEmail mandaba al final a todo tipo con menos de 15 envíos, aunque el
//       comentario decía que conservaba su lugar: "apollo" (lo que eligió un MB a mano) quedaba
//       debajo de info@.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { getTraffic } from "../../modules/traffic.js";
import { setProxyAuth } from "../../modules/apiProxy.js";

const aqui  = path.dirname(fileURLToPath(import.meta.url));
const RAIZ  = path.join(aqui, "..", "..");
const popup = fs.readFileSync(path.join(RAIZ, "popup", "popup.js"), "utf8");
const cuerpo = (src, firma) => {
  const i = src.indexOf(firma);
  ok(i >= 0, `no encontré ${firma}`);
  return { i, texto: src.slice(i, src.indexOf("\n}\n", i)) };
};
const respuesta = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status, headers: { get: () => null },
  json: async () => body, text: async () => JSON.stringify(body),
});

// ── X1: getTraffic con soloGratis ─────────────────────────────────────────────────────────
// Red inventada de la extensión. El proxy contesta 200 para que, si el código llama a RapidAPI,
// el pedido quede anotado y no se esconda detrás de un 401 por falta de sesión.
async function conRedExtension({ cache = [] }, fn) {
  const fetchOriginal = globalThis.fetch;
  const pagos = [];
  globalThis.fetch = async (url) => {
    const u = String(url);
    if (/functions\/v1\/api-proxy|rapidapi/.test(u)) { pagos.push(u); return respuesta({ Visits: 250_000 }); }
    if (u.includes("toolbar_review_queue")) return respuesta([]);   // la fila tiene tráfico 0: no pasa traffic=gte.1000
    if (u.includes("toolbar_traffic_cache")) return respuesta(cache);
    if (u.includes("bump_api_counter")) return respuesta(null);
    return respuesta([], { status: 500 });
  };
  setProxyAuth("token-de-prueba", "mb@ejemplo.com");
  try {
    return await fn(pagos);
  } finally {
    setProxyAuth(null);
    globalThis.fetch = fetchOriginal;
  }
}

test("X1 getTraffic soloGratis: fila de Prospects con tráfico 0 y sin caché no llama a RapidAPI, ni con forceRefresh", async () => {
  await conRedExtension({ cache: [] }, async (pagos) => {
    for (const forceRefresh of [false, true]) {
      pagos.length = 0;
      const t = await getTraffic("diario-sin-trafico.com.ar", { soloGratis: true, forceRefresh });
      strictEqual(t, null, `forceRefresh=${forceRefresh}: sin dato vuelve null y la tarjeta lo muestra como faltante`);
      strictEqual(pagos.length, 0, `antes pagaba RapidAPI al dibujar la tarjeta: ${pagos.join(" | ")}`);
    }
  });
});

test("X1 getTraffic soloGratis: el negativo del worker tampoco dispara el pago", async () => {
  const negativo = [{ data: { noData: true, visits: 0 }, fetched_at: new Date().toISOString() }];
  await conRedExtension({ cache: negativo }, async (pagos) => {
    strictEqual(await getTraffic("diario-sin-trafico.com.ar", { soloGratis: true }), null);
    strictEqual(pagos.length, 0, "getTrafficCache ignora noData: sin soloGratis eso era un pago por tarjeta");
  });
});

test("X1 getTraffic soloGratis: con caché devuelve el dato gratis, igual que antes", async () => {
  const fila = [{ data: { visits: 300_000, rawVisits: 300_000, pageViews: 750_000, monthly: 750_000, topCountries: [{ code: "AR", name: "Argentina", share: 0.9 }] }, fetched_at: new Date().toISOString() }];
  await conRedExtension({ cache: fila }, async (pagos) => {
    const t = await getTraffic("diario-con-cache.com.ar", { soloGratis: true });
    strictEqual(t?.pageViews, 750_000, "la tarjeta se completa con la caché, que es gratis");
    strictEqual(t?.fromCache, true);
    strictEqual(pagos.length, 0);
  });
});

test("X1 getTraffic sin soloGratis (Análisis) sigue igual: sin fila ni caché, consulta", async () => {
  await conRedExtension({ cache: [] }, async (pagos) => {
    await getTraffic("sitio-nuevo-ejemplo.com.ar");
    ok(pagos.length >= 1, "el camino de Análisis para un dominio fuera de Prospects no cambia");
  });
});

// ── X1: el popup ──────────────────────────────────────────────────────────────────────────
test("X1 popup: ninguna tarjeta de Prospects pide tráfico pagable; sólo Análisis llama getTraffic sin soloGratis", () => {
  const card = cuerpo(popup, "function initProspectCard(card, data) {");
  const enTarjeta = [...card.texto.matchAll(/getTraffic\(([^)]*)\)/g)];
  ok(enTarjeta.length >= 1, "initProspectCard completa el tráfico faltante desde pool y caché");
  for (const m of enTarjeta) ok(/soloGratis:\s*true/.test(m[1]), `llamada pagable en la tarjeta de Prospects: getTraffic(${m[1]})`);
  ok(/No data — fill manually/.test(card.texto), "si no hay dato, la tarjeta lo muestra como faltante");

  // La clase entera: fuera de runTrafficCheck (Análisis), toda llamada lleva soloGratis.
  const analisis = cuerpo(popup, "async function runTrafficCheck(opts = {}) {");
  for (const m of popup.matchAll(/getTraffic\(([^)]*)\)/g)) {
    const dentroDeAnalisis = m.index > analisis.i && m.index < analisis.i + analisis.texto.length;
    if (dentroDeAnalisis) continue;
    ok(/soloGratis:\s*true/.test(m[1]), `getTraffic(${m[1]}) fuera de Análisis puede pagar RapidAPI`);
  }
});

// ── X2: el orden de los tipos de email ────────────────────────────────────────────────────
test("X2 _ordenTiposEmailPorRespuesta: un tipo sin muestra conserva su casillero del orden por defecto", async () => {
  const { _ordenTiposEmailPorRespuesta, _TIER_ORDEN_DEFAULT } = await cargarWorker(["_ordenTiposEmailPorRespuesta", "_TIER_ORDEN_DEFAULT"]);
  deepStrictEqual(_TIER_ORDEN_DEFAULT, ["apollo", "rol", "persona", "generico"], "el test asume este orden por defecto");

  // apollo (elegido a mano) con 5 envíos: sin muestra. info@ contesta mucho mejor que rol y persona.
  const agg = { apollo: { n: 5, ok: 2 }, rol: { n: 40, ok: 1 }, persona: { n: 40, ok: 2 }, generico: { n: 60, ok: 12 } };
  const orden = _ordenTiposEmailPorRespuesta(agg);
  deepStrictEqual(orden, ["apollo", "generico", "persona", "rol"], "antes: generico > persona > rol > apollo");
  ok(orden.indexOf("apollo") < orden.indexOf("generico"), "lo elegido a mano no puede quedar debajo de info@ por falta de muestra");

  // Un tipo sin muestra en el medio se queda en su casillero; los medidos se mueven alrededor.
  const medio = _ordenTiposEmailPorRespuesta({ apollo: { n: 50, ok: 5 }, rol: { n: 100, ok: 3 }, persona: { n: 10, ok: 0 }, generico: { n: 200, ok: 20 } });
  strictEqual(medio[2], "persona", `persona sin muestra sigue tercera: ${medio.join(" > ")}`);
  deepStrictEqual(medio, ["apollo", "generico", "persona", "rol"]);

  // Sin datos, o con todo empatado dentro del 15%, el orden por defecto.
  deepStrictEqual(_ordenTiposEmailPorRespuesta({}), _TIER_ORDEN_DEFAULT);
  deepStrictEqual(_ordenTiposEmailPorRespuesta(null), _TIER_ORDEN_DEFAULT);
  const empate = { apollo: { n: 100, ok: 6 }, rol: { n: 100, ok: 6 }, persona: { n: 100, ok: 6 }, generico: { n: 100, ok: 7 } };
  deepStrictEqual(_ordenTiposEmailPorRespuesta(empate), _TIER_ORDEN_DEFAULT, "un decimal no reordena");

  // Todos medidos: el reordenamiento por respuestas sigue funcionando como antes.
  const todos = { apollo: { n: 100, ok: 1 }, rol: { n: 100, ok: 20 }, persona: { n: 100, ok: 10 }, generico: { n: 100, ok: 1 } };
  deepStrictEqual(_ordenTiposEmailPorRespuesta(todos), ["rol", "persona", "apollo", "generico"]);
});

test("X2 reajustarPrioridadTiposEmail guarda el orden con el tipo sin muestra en su lugar", async () => {
  const w = await cargarWorker(["reajustarPrioridadTiposEmail", "_tipoDeEmailParaRanking"], { fetchFalso: true });
  const filas = [];
  const agregar = (n, respondidas, email, source) => {
    for (let k = 0; k < n; k++) filas.push({ email_sent_to: email, source, responded_at: k < respondidas ? "2026-09-01T10:00:00Z" : null, response_type: k < respondidas ? "real" : null });
  };
  agregar(5, 2, "juan.perez@diario-ejemplo.com", "manual");
  agregar(40, 1, "publicidad@diario-ejemplo.com", "apollo");
  agregar(40, 2, "maria.gomez@diario-ejemplo.com", "apollo");
  agregar(60, 12, "info@diario-ejemplo.com", "");
  const tipos = [...new Set(filas.map(f => w._tipoDeEmailParaRanking(f.email_sent_to, f.source)))];
  deepStrictEqual(tipos, ["apollo", "rol", "persona", "generico"], "las filas de prueba cubren los cuatro tipos");

  const escrituras = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url);
    const m = String(opts.method || "GET").toUpperCase();
    if (u.includes("toolbar_response_tracking")) return respuesta(filas);
    if (u.includes("toolbar_config") && m === "GET") return respuesta([]);
    if (u.includes("toolbar_config")) {
      const body = opts.body ? JSON.parse(opts.body) : {};
      const key = body.key || decodeURIComponent((/key=eq\.([^&]+)/.exec(u) || [])[1] || "");
      escrituras.push({ key, value: body.value });
      return respuesta([]);
    }
    return respuesta([], { status: 404 });
  };
  try {
    await w.reajustarPrioridadTiposEmail("tok");
  } finally {
    delete globalThis.__fetchFalso;
  }
  const guardado = escrituras.filter(e => e.key === "email_tier_ranking").at(-1);
  ok(guardado, `el reajuste tiene que guardar email_tier_ranking: ${JSON.stringify(escrituras.map(e => e.key))}`);
  deepStrictEqual(JSON.parse(guardado.value).orden, ["apollo", "generico", "persona", "rol"],
    "antes guardaba apollo último, debajo de info@");
});
