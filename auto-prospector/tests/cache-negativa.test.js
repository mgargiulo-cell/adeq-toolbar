// "No hay datos" también es un dato comprado. (2026-09-08, parte del 07-08/09)
//
// La caché de tráfico guardaba sólo los positivos. Un dominio del que SimilarWeb no sabe nada se
// le preguntaba TRES veces facturadas (los 3 intentos antes del freeze) más tres rondas de
// scraping, y al descongelarse a los 15/30/60 días, otras tres. Con el negativo guardado, los
// reintentos salen de la caché a costo cero. TTL corto (14 días) porque un "no" envejece.
// La extensión ignora esas filas: para el MB sigue valiendo la regla del 17/06 (no mostrar
// "sin tráfico" por un 0 guardado).
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const worker = fs.readFileSync(path.join(RAIZ, "auto-prospector", "index.js"), "utf8");
const supabaseExt = fs.readFileSync(path.join(RAIZ, "modules", "supabase.js"), "utf8");

const hace = (dias) => new Date(Date.now() - dias * 86400_000).toISOString();

test("un negativo fresco se sirve de la caché; uno vencido se vuelve a preguntar", async () => {
  const mod = await cargarWorker(["getTrafficCacheServer"], { fetchFalso: true });
  let filas = [];
  globalThis.__fetchFalso = async (url) => {
    ok(/select=data,fetched_at/.test(String(url)), "la lectura tiene que traer fetched_at para poder vencer el negativo");
    return { ok: true, json: async () => filas };
  };
  filas = [{ data: { visits: 0, noData: true }, fetched_at: hace(3) }];
  const fresco = await mod.getTrafficCacheServer("sin-datos.com");
  ok(fresco && fresco.noData === true, "a los 3 días el negativo vale: no se paga otra consulta");

  filas = [{ data: { visits: 0, noData: true }, fetched_at: hace(20) }];
  strictEqual(await mod.getTrafficCacheServer("sin-datos.com"), null, "a los 20 días el negativo venció: se pregunta de nuevo");

  filas = [{ data: { visits: 500000, rawVisits: 500000 }, fetched_at: hace(20) }];
  const positivo = await mod.getTrafficCacheServer("con-datos.com");
  ok(positivo && positivo.visits === 500000, "un positivo de 20 días sigue valiendo (TTL de 90)");
});

test("getTrafficData guarda el negativo SÓLO cuando la API contestó y nadie tenía datos", () => {
  const ini = worker.indexOf("async function getTrafficData(domain, rapidApiKey) {");
  const fin = worker.indexOf("\n}\n", ini);
  const fn  = worker.slice(ini, fin);
  const iGuardaNeg = fn.indexOf("noData: true");
  ok(iGuardaNeg > 0, "tiene que guardar el negativo");
  // El guardado va DESPUÉS de las tres ramas de error (null_response / 4xx / __error), que
  // devuelven antes: un timeout o un 5xx nunca puede quedar grabado como "sin datos".
  for (const rama of ['error: "null_response"', "error: data.__error4xx", "error: data.__error"]) {
    ok(fn.indexOf(rama) < iGuardaNeg, `la rama ${rama} tiene que salir antes de llegar al guardado del negativo`);
  }
  // Y sólo si el scrape de respaldo tampoco rescató nada.
  ok(fn.lastIndexOf("_scrapeTrafficFallback", iGuardaNeg) > 0, "primero se intenta el scrape público; el negativo es el último recurso");
});

test("la extensión ignora las filas noData: para el MB no son caché", () => {
  const ini = supabaseExt.indexOf("export async function getTrafficCache(domain) {");
  const fn  = supabaseExt.slice(ini, supabaseExt.indexOf("\n}\n", ini));
  ok(/data\?\.noData\) return null/.test(fn),
     "sin esto la toolbar mostraría 'sin tráfico' por un negativo del worker — la regla del 17/06");
});

test("el TTL del negativo es configurable y más corto que el positivo", () => {
  ok(/traffic_neg_cache_dias/.test(worker), "se lee de toolbar_config como traffic_cache_dias");
  const neg = Number(/let _trafficNegCacheDias = (\d+);/.exec(worker)?.[1]);
  const pos = Number(/let _trafficCacheDias = (\d+);/.exec(worker)?.[1]);
  ok(neg > 0 && pos > 0 && neg < pos, `negativo ${neg}d tiene que ser menor que positivo ${pos}d: un "no" envejece más rápido que un dato`);
});
