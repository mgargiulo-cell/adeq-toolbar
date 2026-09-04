// fetchPageContent tiene que DEVOLVER ALGO para un sitio que responde. (2026-09-04)
//
// Entre el 02/09 y el 04/09 devolvió null para todos los sitios: una variable que no existía
// (`geo`) tiraba ReferenceError adentro del try, y el catch —pensado para errores de red— lo
// convertía en "no pude bajar la página". El clasificador se quedó ciego dos días sin que
// ningún test ni alerta lo dijera. Este test baja UNA página real y exige un objeto con título.
// Si no hay red, se saltea (no falla): lo que se prueba es el código, no la conexión.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import { cargarWorker } from "./_worker-exportado.mjs";

test("fetchPageContent devuelve la página, no null, para un sitio que responde", async (t) => {
  const { fetchPageContent } = await cargarWorker(["fetchPageContent"]);
  // Algunos entornos no resuelven example.com (visto el 04/09: ENOTFOUND en el sandbox);
  // se usa el primer sitio estable que responda. Sin ninguno, no hay red: se saltea.
  let sitio = "";
  for (const s of ["example.com", "www.iana.org", "developer.mozilla.org", "www.wikipedia.org"]) {
    try { const r = await fetch(`https://${s}/`, { signal: AbortSignal.timeout(6000) }); if (r.ok) { sitio = s; break; } } catch {}
  }
  if (!sitio) { t.skip("sin red"); return; }
  const pc = await fetchPageContent(sitio);
  ok(pc !== null && typeof pc === "object", `devolvió null para ${sitio}: algo tira adentro del try y el catch lo esconde`);
  strictEqual(pc.dead, undefined, `${sitio} no está muerto`);
  ok((pc.title || "").length > 0, `sin título para ${sitio}`);
  // Los campos que el clasificador lee tienen que existir (no undefined), aunque sean false/null.
  for (const k of ["nonPublisherType", "nonPublisherFuerte", "sinSenalEditorial", "hasDisplayAds", "hasProgrammatic"]) {
    ok(k in pc, `falta el campo ${k} en el resultado`);
  }
});
