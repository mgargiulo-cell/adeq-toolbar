// El ciclo de RapidAPI tiene UN día de renovación, y lo comparten el worker y la extensión. (2026-09-13)
//
// El worker pasó al 18 el 24/08 (el plan renueva el 18); la extensión quedó en el 7 hasta el 13/09.
// Entre el 7 y el 17 de cada mes cada lado contaba un ciclo distinto: el panel de la extensión
// arrancaba un período nuevo once días antes que el worker. Maxi confirmó el 18 el 13/09.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const leer = (rel) => fs.readFileSync(path.join(RAIZ, rel), "utf8");
const ancla = (src, que) => {
  const m = /const RAPIDAPI_CYCLE_ANCHOR_DAY = (\d+);/.exec(src);
  ok(m, `${que}: no encontré RAPIDAPI_CYCLE_ANCHOR_DAY`);
  return Number(m[1]);
};

test("el worker y la extensión renuevan el ciclo de RapidAPI el mismo día (el 18, confirmado el 13/09)", () => {
  const worker = ancla(leer("auto-prospector/index.js"), "worker");
  const extension = ancla(leer("modules/apiProxy.js"), "extensión");
  strictEqual(worker, 18);
  strictEqual(extension, worker, "la extensión contaba desde el 7 y el worker desde el 18: dos ciclos distintos");
  const fn = leer("modules/apiProxy.js").slice(leer("modules/apiProxy.js").indexOf("function currentPeriod()"));
  ok(!/\b7\b/.test(fn.slice(0, fn.indexOf("\n}"))), "quedó un 7 literal en currentPeriod");
  ok(/RAPIDAPI_CYCLE_ANCHOR_DAY/.test(fn.slice(0, fn.indexOf("\n}"))), "currentPeriod tiene que usar la constante, no un número suelto");
});
