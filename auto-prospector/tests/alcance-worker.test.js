// El worker tampoco puede usar un nombre que no existe. (2026-09-13, auditoría de las señales del 10/09)
//
// "google_contact 0" en todos los mails. La búsqueda de contacto en Google llamaba
// extractPhonesFromHtml(text, geo) sin tener `geo` (02/09) y filtraba URLs con `CONTACT_HINT`, que
// estaba declarada adentro de otra función (04/08). En un módulo strict las dos son ReferenceError;
// un try/catch las tragaba y la función devolvía vacío DESPUÉS de pagarle a Serper. Había dos más:
// `cfg` en el rollover de next_day (la comparación contra ayer era siempre contra 0) y `domain` en
// el catch de la conversión del feeder (un corte de red tiraba el slot entero).
// Ninguna la ve `node --check`, y alcance-popup.test.js sólo miraba la extensión.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { analizarAlcance, GLOBALS_NODE } from "./_alcance.mjs";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

test("el detector ve un nombre que no existe y uno declarado en otra función (los casos geo y CONTACT_HINT)", () => {
  const { violaciones, sinResolver } = analizarAlcance(`
    async function scrape() { const CONTACT_HINT = /contacto/i; return CONTACT_HINT.test("x"); }
    function extractPhones(text, geo = "") { return [text, geo]; }
    async function buscarContacto(domain) {
      try { const urls = ["a"].filter(u => CONTACT_HINT.test(u)); return extractPhones(domain, geo); } catch { return []; }
    }
  `, GLOBALS_NODE);
  ok(violaciones.some(v => /buscarContacto .* usa "CONTACT_HINT", que está declarado ADENTRO de scrape/.test(v)), violaciones.join("\n"));
  ok(sinResolver.has("geo"), `geo tenía que salir sin resolver: ${[...sinResolver.keys()].join(", ")}`);
});

const ARCHIVOS = [
  "index.js",
  ...fs.readdirSync(path.join(RAIZ, "lib")).filter(f => f.endsWith(".js")).map(f => `lib/${f}`),
  ...["discovery.js", "templates.js", "keywordsData.js"].filter(f => fs.existsSync(path.join(RAIZ, f))),
];

for (const rel of ARCHIVOS) {
  test(`worker ${rel}: nada declarado adentro de una función se usa desde afuera, y nada queda sin resolver`, () => {
    const src = fs.readFileSync(path.join(RAIZ, rel), "utf8");
    const { violaciones, sinResolver } = analizarAlcance(src, GLOBALS_NODE);
    strictEqual(violaciones.length, 0,
      `${violaciones.length} referencia(s) a algo declarado adentro de otra función:\n   ${violaciones.slice(0, 15).join("\n   ")}`);
    strictEqual(sinResolver.size, 0,
      `${sinResolver.size} nombre(s) que no existen (ReferenceError en cuanto se ejecuten, y un catch lo esconde):\n   ` +
      [...sinResolver].slice(0, 15).map(([nm, us]) => `${nm} ← ${us.slice(0, 3).join(", ")}`).join("\n   ") +
      `\n   (si es un global real de Node, agregarlo a GLOBALS_NODE en tests/_alcance.mjs)`);
  });
}
