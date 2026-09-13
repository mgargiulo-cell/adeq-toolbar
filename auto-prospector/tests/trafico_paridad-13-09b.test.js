// Tráfico: la tarjeta de Prospects y Análisis dicen lo mismo, y un lead del pool no se vuelve a pagar. (2026-09-13)
//
// Pedido del dueño: "que la información de Prospects sea la misma que aparece en Análisis". Regla
// del 18/08: "el crédito se gasta solo en la primera consulta, por única vez". La auditoría del
// 13/09 encontró tres caminos que rompían las dos cosas a la vez:
//   C18. Abrir en Análisis un lead de Prospects que está en el CRM con caché de más de 30 días
//        forzaba RapidAPI: un hit pagado y un número distinto al de la tarjeta.
//   C19a. Si el número del lead salió del respaldo por scrape (Hypestat), el worker no lo guardaba:
//        cada MB que abría el lead pagaba otra vez y leía "Sin tráfico" sobre una tarjeta de 900K.
//   C19b. Sin páginas por visita, el piso usaba visitas × 2.0 pero la caché guardaba pageViews null:
//        Análisis mostraba 200K y "Bajo umbral" sobre una tarjeta de 400K.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { decidirFuenteTrafico, armarTraficoDePool, getTraffic } from "../../modules/traffic.js";
import { getFilaPoolTrafico, getTrafficCacheSinVencer } from "../../modules/supabase.js";

const aqui   = path.dirname(fileURLToPath(import.meta.url));
const RAIZ   = path.join(aqui, "..", "..");
const worker = fs.readFileSync(path.join(RAIZ, "auto-prospector", "index.js"), "utf8");
const popup  = fs.readFileSync(path.join(RAIZ, "popup", "popup.js"), "utf8");
const cuerpo = (src, firma) => {
  const i = src.indexOf(firma);
  ok(i >= 0, `no encontré ${firma}`);
  return src.slice(i, src.indexOf("\n}\n", i));
};
const respuesta = (body, { status = 200, texto = null } = {}) => ({
  ok: status >= 200 && status < 300, status, headers: { get: () => null },
  json: async () => body, text: async () => texto ?? JSON.stringify(body),
});
const tick = () => new Promise(r => setImmediate(r));
const HYPESTAT = (unicosDiarios, pvDiarias) =>
  `<html><p>Daily Unique Visitors: <strong>${unicosDiarios}</strong></p><p>Daily Pageviews: <strong>${pvDiarias}</strong></p></html>`;

// Red inventada del worker: caché, RapidAPI, las tres fuentes públicas y el contador.
function armarRedWorker({ cache = () => [], insights, hypestat = null }) {
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url);
    const m = String(opts.method || "GET").toUpperCase();
    pedidos.push({ u, m, body: opts.body ? JSON.parse(opts.body) : null });
    if (u.includes("toolbar_traffic_cache")) return m === "POST" ? respuesta(null, { status: 201 }) : respuesta(cache());
    if (u.includes("rapidapi.com")) return insights(u);
    if (u.includes("hypestat.com")) return hypestat ? respuesta(null, { texto: hypestat }) : respuesta(null, { status: 404 });
    if (u.includes("bump_api_counter")) return respuesta(null);
    return respuesta(null, { status: 404 });   // similarweb.com y siteworthtraffic: no ayudan
  };
  return pedidos;
}
const guardados = (pedidos) => pedidos.filter(p => p.m === "POST" && p.u.includes("toolbar_traffic_cache")).map(p => p.body.data);
const pagos     = (pedidos) => pedidos.filter(p => p.u.includes("rapidapi.com")).length;

// ── WORKER: la fila de caché ────────────────────────────────────────────────────────────
test("la fila de caché lleva el mismo número que decide el piso, y nunca noData", async () => {
  const { _filaCacheTrafico, PPV_ESTIMADO } = await cargarWorker(["_filaCacheTrafico", "PPV_ESTIMADO"]);
  const sinPpv = _filaCacheTrafico(200_000, {});
  strictEqual(sinPpv.pageViews, 200_000 * PPV_ESTIMADO, "sin páginas por visita: visitas × PPV_ESTIMADO, como el piso de processCsvItem");
  strictEqual(sinPpv.estimatedPages, true, "y marcado como estimado, para que la extensión lo pinte ~X (est.)");
  strictEqual(sinPpv.ppvSource, "estimated");

  const api = _filaCacheTrafico(300_000, { pagesPerVisit: 2.7, topCountry: null, category: "News" });
  strictEqual(api.pageViews, Math.round(300_000 * 2.7));
  strictEqual(api.ppvSource, "api");
  strictEqual(api.estimatedPages, false);
  strictEqual(api.rawVisits, 300_000);

  const hyp = _filaCacheTrafico(600_000, { pageViews: 1_500_000, source: "hypestat_scrape" });
  strictEqual(hyp.pageViews, 1_500_000, "páginas vistas directas de Hypestat, sin tocar");
  strictEqual(hyp.pagesPerVisit, 2.5);
  strictEqual(hyp.ppvSource, "hypestat");
  strictEqual(hyp.source, "hypestat_scrape");

  for (const f of [sinPpv, api, hyp]) {
    strictEqual(f.noData, undefined, "una fila con datos no puede llevar noData: la extensión la ignoraría");
    strictEqual(f.monthly, f.pageViews);
  }
});

test("el número del scrape se guarda siempre si la API contestó, y con la API caída sólo si supera el piso", async () => {
  const { _debeGuardarScrapeEnCache, REVIEW_QUEUE_MIN_TRAFFIC } = await cargarWorker(["_debeGuardarScrapeEnCache", "REVIEW_QUEUE_MIN_TRAFFIC"]);
  strictEqual(_debeGuardarScrapeEnCache({ pageViews: 10_000 }, { apiContesto: true }), true, "RapidAPI contestó 0 y se pagó: preguntar de nuevo es gasto seguro");
  strictEqual(_debeGuardarScrapeEnCache({ pageViews: REVIEW_QUEUE_MIN_TRAFFIC }, { apiContesto: false }), true, "API caída y el lead entra a Prospects: se guarda");
  strictEqual(_debeGuardarScrapeEnCache({ pageViews: REVIEW_QUEUE_MIN_TRAFFIC - 1 }, { apiContesto: false }), false,
    "API caída y número bajo: no se graba 90 días, así se mide con SimilarWeb cuando vuelva el cupo");
});

// ── WORKER: getTrafficData de punta a punta ─────────────────────────────────────────────
test("RapidAPI dice 0 y Hypestat rescata: se guarda en la caché y la segunda consulta sale de ahí, con el mismo número", async () => {
  const { getTrafficData } = await cargarWorker(["getTrafficData"], { fetchFalso: true });
  let cache = [];
  const pedidos = armarRedWorker({
    cache: () => cache,
    insights: () => respuesta({ Visits: 0, WebsiteDetails: { Category: "News_and_Media" } }),
    hypestat: HYPESTAT("20,000", "50,000"),
  });
  const r = await getTrafficData("sitio-ejemplo.pl", "k");
  await tick();
  strictEqual(r.pageViews, 1_500_000, "processCsvItem usa estos pageViews para el piso y para la tarjeta");
  const filas = guardados(pedidos);
  strictEqual(filas.length, 1, "antes volvía sin guardar: el lead entraba a Prospects sin fila en la caché");
  strictEqual(filas[0].pageViews, 1_500_000);
  strictEqual(filas[0].source, "hypestat_scrape");
  strictEqual(filas[0].noData, undefined);

  cache = [{ data: filas[0], fetched_at: new Date().toISOString() }];
  const antes = pagos(pedidos);
  const r2 = await getTrafficData("sitio-ejemplo.pl", "k");
  strictEqual(pagos(pedidos), antes, "la segunda vez no se paga RapidAPI");
  strictEqual(r2.pageViews, r.pageViews, "y el número es el mismo que decidió la entrada");
});

test("con la API caída, el número de Hypestat se guarda sólo si el lead supera el piso", async () => {
  const { getTrafficData } = await cargarWorker(["getTrafficData"], { fetchFalso: true });
  let pedidos = armarRedWorker({ insights: () => respuesta({ message: "forbidden" }, { status: 403 }), hypestat: HYPESTAT("1,000", "3,000") });
  const bajo = await getTrafficData("chico-ejemplo.pl", "k");
  await tick();
  strictEqual(bajo.pageViews, 90_000);
  strictEqual(guardados(pedidos).length, 0, "90K con la API caída no se graba");

  pedidos = armarRedWorker({ insights: () => respuesta({ message: "forbidden" }, { status: 403 }), hypestat: HYPESTAT("20,000", "50,000") });
  const alto = await getTrafficData("grande-ejemplo.pl", "k");
  await tick();
  strictEqual(alto.pageViews, 1_500_000);
  const filas = guardados(pedidos);
  strictEqual(filas.length, 1, "1,5M entra a Prospects: sin fila, cada MB que lo abre paga");
  strictEqual(filas[0].pageViews, 1_500_000);
});

test("RapidAPI sin páginas por visita: la caché guarda visitas × PPV_ESTIMADO, igual que el piso", async () => {
  const { getTrafficData } = await cargarWorker(["getTrafficData"], { fetchFalso: true });
  let cache = [];
  const pedidos = armarRedWorker({ cache: () => cache, insights: () => respuesta({ Visits: 200_000 }) });
  const r = await getTrafficData("medio-ejemplo.pl", "k");
  await tick();
  strictEqual(r.pagesPerVisit, null, "processCsvItem calcula 200K × 2.0 = 400K para la tarjeta");
  const filas = guardados(pedidos);
  strictEqual(filas.length, 1);
  strictEqual(filas[0].pageViews, 400_000, "antes guardaba null y Análisis mostraba 200K con 'Bajo umbral'");
  strictEqual(filas[0].estimatedPages, true);

  cache = [{ data: filas[0], fetched_at: new Date().toISOString() }];
  const r2 = await getTrafficData("medio-ejemplo.pl", "k");
  strictEqual(r2.pageViews, 400_000, "desde la caché, el piso ve el mismo número");
});

test("PPV_ESTIMADO vale lo mismo que el 2.0 del piso de la cola y del autopilot", () => {
  const est = Number(/const PPV_ESTIMADO = ([\d.]+);/.exec(worker)?.[1]);
  ok(est > 0, "index.js tiene que declarar PPV_ESTIMADO");
  const cola = /const PPV_FALLBACK = ([\w.]+);/.exec(worker)?.[1];
  ok(cola === "PPV_ESTIMADO" || Number(cola) === est, `processCsvItem usa ${cola} y la caché ${est}: vuelven a ser dos números`);
  const auto = /const ppvSafe\s*=\s*\(typeof pagesPerVisit === "number" && pagesPerVisit > 0\) \? pagesPerVisit : ([\w.]+);/.exec(worker)?.[1];
  ok(auto === "PPV_ESTIMADO" || Number(auto) === est, `el autopilot usa ${auto} y la caché ${est}`);
});

// ── EXTENSIÓN: de dónde sale el número ──────────────────────────────────────────────────
test("decidirFuenteTrafico: un lead de Prospects nunca paga ni se fuerza; sin fila, lo de siempre", () => {
  const cacheVieja = { fromCache: true, cachedDaysAgo: 60, rawVisits: 300_000 };
  deepStrictEqual(decidirFuenteTrafico({ filaPool: { traffic: 820_000 }, cache: cacheVieja, veredictoOk: true, cachedDaysAgo: 60 }),
    { fuente: "pool", forzar: false, pagar: false }, "C18: CRM reciclable y caché de 60 días, pero está en Prospects");
  deepStrictEqual(decidirFuenteTrafico({ filaPool: { traffic: 820_000 }, cache: null }),
    { fuente: "pool", forzar: false, pagar: false }, "C19: está en Prospects y no hay caché (entró por Hypestat)");
  deepStrictEqual(decidirFuenteTrafico({ filaPool: { traffic: 820_000 }, pedidoForzar: true }),
    { fuente: "pool", forzar: false, pagar: false }, "ni forzado: el timer del CRM puede llegar antes que la fila");

  deepStrictEqual(decidirFuenteTrafico({ cache: { fromCache: true, cachedDaysAgo: 45 }, veredictoOk: true, cachedDaysAgo: 45 }),
    { fuente: "api", forzar: true, pagar: true }, "fuera de Prospects, CRM reciclable y caché de 45 días: se refresca como siempre");
  deepStrictEqual(decidirFuenteTrafico({ cache: { fromCache: true, cachedDaysAgo: 10 }, veredictoOk: true, cachedDaysAgo: 10 }),
    { fuente: "cache", forzar: false, pagar: false });
  deepStrictEqual(decidirFuenteTrafico({ cache: null }), { fuente: "api", forzar: false, pagar: true }, "sin fila y sin caché se paga");
  deepStrictEqual(decidirFuenteTrafico({ filaPool: { traffic: 0 }, cache: null }), { fuente: "api", forzar: false, pagar: true },
    "una fila sin tráfico no es dato de Prospects");
});

test("armarTraficoDePool: el número principal es el de la tarjeta; la caché sólo desglosa", () => {
  const conCache = armarTraficoDePool(
    { traffic: 820_000, geo: "", category: "news" },
    { rawVisits: 300_000, pagesPerVisit: 2.5, pageViews: 610_000, topCountries: [{ code: "DO", name: "DO", share: 0.8 }], category: "News_and_Media", fromCache: true, cachedDaysAgo: 60 },
    "elcaribe.com.do");
  strictEqual(conCache.pageViews, 820_000, "Análisis mostraba 610K (caché) y la tarjeta 820K");
  strictEqual(conCache.monthly, 820_000);
  strictEqual(conCache.rawVisits, 300_000);
  strictEqual(conCache.fromPool, true);
  strictEqual(conCache.fromCache, false, "no es caché: el timer del CRM no la tiene que refrescar");
  strictEqual(conCache.topCountries[0].code, "DO");
  strictEqual(decidirFuenteTrafico({ filaPool: { traffic: conCache.pageViews }, cache: conCache, veredictoOk: true, cachedDaysAgo: 60 }).forzar, false);

  const soloFila = armarTraficoDePool({ traffic: 900_000, geo: "Argentina", category: "news" }, null, "diario-ejemplo.com");
  strictEqual(soloFila.pageViews, 900_000);
  strictEqual(soloFila.rawVisits, null);
  strictEqual(soloFila.category, "news");
  strictEqual(soloFila.topCountries[0].code, "AR", "el país sale de la fila");

  strictEqual(armarTraficoDePool({ traffic: 900_000, geo: "" }, null, "diario-ejemplo.com.mx").topCountries[0].code, "MX", "si la fila no tiene país, el TLD");
  strictEqual(armarTraficoDePool({ traffic: 900_000 }, { noData: true, rawVisits: 0 }, "x.com").rawVisits, null, "un negativo del worker no desglosa nada");
});

// getTraffic real, con la red inventada: fila en Prospects, sin caché, ni una llamada a la API.
test("getTraffic: un lead de Prospects sale del pool, sin pagar, ni con forceRefresh", async () => {
  const fetchOriginal = globalThis.fetch;
  const urls = [];
  globalThis.fetch = async (url) => {
    const u = String(url);
    urls.push(u);
    if (u.includes("toolbar_review_queue")) return respuesta([{ id: 1, traffic: 820_000, geo: "Dominican Republic", category: "news", status: "pending" }]);
    if (u.includes("toolbar_traffic_cache")) return respuesta([]);
    return respuesta({}, { status: 500 });
  };
  try {
    for (const forceRefresh of [false, true]) {
      urls.length = 0;
      const t = await getTraffic("elcaribe.com.do", { forceRefresh });
      ok(t && !t.blocked, `el dominio de prueba no puede estar bloqueado: ${JSON.stringify(t)}`);
      strictEqual(t.fromPool, true, `forceRefresh=${forceRefresh}: antes iba a RapidAPI`);
      strictEqual(t.pageViews, 820_000);
      strictEqual(urls.filter(u => /api-proxy|rapidapi|functions\/v1/.test(u)).length, 0, `no se llama al proxy de RapidAPI: ${urls.join(" | ")}`);
    }
  } finally {
    globalThis.fetch = fetchOriginal;
  }
});

test("las dos lecturas nuevas de la extensión llevan reloj y respetan la regla de estados y de negativos", async () => {
  const fetchOriginal = globalThis.fetch;
  const pedidos = [];
  let filas = [];
  globalThis.fetch = async (url, opts = {}) => { pedidos.push({ u: String(url), opts }); return respuesta(filas); };
  try {
    filas = [{ id: 7, traffic: 500_000 }];
    const fila = await getFilaPoolTrafico("WWW-no.Diario.com");
    strictEqual(fila?.traffic, 500_000);
    const u = pedidos.at(-1).u;
    ok(u.includes("status=in.(pending,por_enviar)"), `sólo Prospects y la cola del MB: ${u}`);
    ok(u.includes("traffic=gte.1000"), "una fila sin tráfico no cuenta");
    ok(pedidos.at(-1).opts.signal instanceof AbortSignal, "el GET al pool tiene que llevar timeout");

    filas = [{ data: { noData: true, visits: 0 }, fetched_at: new Date().toISOString() }];
    strictEqual(await getTrafficCacheSinVencer("x.com"), null, "un negativo del worker no es caché para el MB");
    ok(!pedidos.at(-1).u.includes("fetched_at=gte"), "sin vencimiento: sólo desglosa, el número ya salió del pool");
    ok(pedidos.at(-1).opts.signal instanceof AbortSignal);

    globalThis.fetch = async () => { throw new Error("red caída"); };
    strictEqual(await getFilaPoolTrafico("x.com"), null, "si falla, null: la toolbar sigue el camino de siempre");
  } finally {
    globalThis.fetch = fetchOriginal;
  }
});

test("popup: el refresco del CRM pasa por decidirFuenteTrafico y Análisis pinta el número del pool", () => {
  const dup = cuerpo(popup, "async function runDuplicateCheck() {");
  ok(/decidirFuenteTrafico\(\{ filaPool: state\.filaPool/.test(dup), "el timer del CRM tiene que preguntarle a la regla con la fila del pool");
  ok(/if \(fuente\.forzar\)/.test(dup), "y forzar sólo si la regla lo dice");
  ok(!/state\.trafficData\?\.fromCache && \(state\.trafficData\?\.cachedDaysAgo \|\| 0\) > 30/.test(dup), "volvió la condición cruda que ignoraba Prospects");

  const tr = cuerpo(popup, "async function runTrafficCheck(opts = {}) {");
  ok(/state\.filaPool = data\?\.fromPool/.test(tr), "runTrafficCheck deja la fila del pool para el timer del CRM");
  ok(/if \(!state\.traffic && !state\.visits\)/.test(tr), "'Sin tráfico' sólo si no hay ni páginas vistas ni visitas");
  const iPool = tr.indexOf("} else if (data.fromPool) {");
  ok(iPool > 0 && iPool < tr.indexOf("} else if (data.noPageViewData) {"), "la rama del pool va antes que las de la caché");
  ok(/Dato de Prospects/.test(tr.slice(iPool, tr.indexOf("} else if (data.noPageViewData) {"))), "y dice de dónde sale el número");
  ok(!/btn-traffic-recheck/.test(tr.slice(iPool, tr.indexOf("} else if (data.noPageViewData) {"))), "sin Re-verificar en un lead del pool: pagaría y habría dos números");
  ok(/state\.filaPool\s+= null;/.test(popup), "al cambiar de web se limpia la fila del pool");
});
