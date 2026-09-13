// Nada declarado ADENTRO de una función puede usarse desde AFUERA. (2026-09-08, error de consola del user)
//
// Lo que pasó: `bindButtons()` en popup.js es una función de ~1.300 líneas escrita sin indentar.
// Desde el corte de Monday (02/09) se le fueron pegando adentro, creyendo que eran top-level, el
// emisor al CRM (`enviarAlBoard`), la consulta de la ficha (`buscarEnCrm`), el veredicto
// (`_veredictoCrm`, `_crmBloquea`…), las constantes del CRM y un `addEventListener("DOMContentLoaded")`.
// Los módulos son strict: una `function` declarada dentro de otra sólo existe ahí. Desde los botones
// (adentro) funcionaba todo; desde `runDuplicateCheck`, `autofillDraftOnLoad`, `resetAnalysisUI` y
// `validateProspect` (afuera) era `ReferenceError`. Seis días, todas las versiones, y `node --check`
// contento porque las llaves suman bien.
//
// Este test parsea los archivos de la extensión con acorn y falla si un identificador libre de un
// statement top-level resuelve a algo declarado en el cuerpo de OTRA función top-level — o a nada.
// El analizador vive en tests/_alcance.mjs desde el 13/09, porque el worker necesitaba el mismo
// chequeo (tests/alcance-worker.test.js).
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { analizarAlcance, GLOBALS_NAVEGADOR } from "./_alcance.mjs";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(aqui, "..", "..");

// El detector se prueba a sí mismo con el caso real en miniatura: si algún día deja de ver esto,
// el resto de los tests de este archivo pasan sin mirar nada.
test("el detector ve una función declarada adentro de otra y usada desde afuera", () => {
  const { violaciones } = analizarAlcance(`
    function bindButtons() {
      document.getElementById("x").addEventListener("click", () => {});
    function buscarEnCrm() { return 1; }
    }
    async function runDuplicateCheck() { return await buscarEnCrm(); }
  `, GLOBALS_NAVEGADOR);
  strictEqual(violaciones.length, 1, `esperaba 1 violación, vinieron ${violaciones.length}`);
  ok(/runDuplicateCheck .* usa "buscarEnCrm", que está declarado ADENTRO de bindButtons/.test(violaciones[0]), violaciones[0]);
});

test("y no confunde destructuring, defaults ni parámetros con referencias libres", () => {
  const { violaciones, sinResolver } = analizarAlcance(`
    async function a({ auth, tab = null } = {}) { const [x, y] = [1, 2]; for (const [k, v] of Object.entries({})) console.log(k, v, x, y, auth, tab); }
    function b(el) { try { el.value = ""; } catch (e) { console.warn(e); } }
  `, GLOBALS_NAVEGADOR);
  strictEqual(violaciones.length, 0, violaciones.join("\n"));
  strictEqual(sinResolver.size, 0, [...sinResolver.keys()].join(", "));
});

const ARCHIVOS = [
  "popup/popup.js",
  "background/service-worker.js",
  ...fs.readdirSync(path.join(RAIZ, "modules")).filter(f => f.endsWith(".js")).map(f => `modules/${f}`),
  // La librería de emails va dentro del zip de la extensión: se chequea con las globales del navegador.
  "auto-prospector/lib/email.js",
];

for (const rel of ARCHIVOS) {
  test(`${rel}: nada declarado adentro de una función se usa desde afuera, y nada queda sin resolver`, () => {
    const src = fs.readFileSync(path.join(RAIZ, rel), "utf8");
    const { violaciones, sinResolver } = analizarAlcance(src, GLOBALS_NAVEGADOR);
    strictEqual(violaciones.length, 0,
      `${violaciones.length} referencia(s) a algo declarado adentro de otra función — es el bug que dejó el cartel del CRM ` +
      `en "Checking..." seis días:\n   ${violaciones.slice(0, 15).join("\n   ")}`);
    strictEqual(sinResolver.size, 0,
      `${sinResolver.size} nombre(s) que no existen ni en el archivo ni como global del navegador (ReferenceError en cuanto se ejecuten):\n   ` +
      [...sinResolver].slice(0, 15).map(([nm, us]) => `${nm} ← ${us.slice(0, 3).join(", ")}`).join("\n   ") +
      `\n   (si es un global real de Chrome, agregarlo a GLOBALS_NAVEGADOR en tests/_alcance.mjs)`);
  });
}
