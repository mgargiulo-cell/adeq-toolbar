// La extensión y el worker tienen que decidir IGUAL a quién se le escribe. (Fase 5, 2026-09-04)
//
// Hasta hoy el popup tenía copias propias del criterio y ya habían discrepado dos veces. Este
// test no prueba el DOM (no se puede desde Node): prueba lo que SÍ puede volver a pasar —
// que alguien vuelva a escribir una regex de ranking dentro de popup.js en vez de importarla
// del módulo compartido. Si eso pasa, este test lo dice antes de que llegue a un media buyer.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const aqui  = path.dirname(fileURLToPath(import.meta.url));
const popup = fs.readFileSync(path.join(aqui, "..", "..", "popup", "popup.js"), "utf8");

test("el popup importa el ranking del módulo compartido", () => {
  ok(/import \{[^}]*\brankEmail\b[^}]*\} from "\.\.\/auto-prospector\/lib\/email\.js"/.test(popup),
     "popup.js tiene que importar rankEmail de ../auto-prospector/lib/email.js");
});

test("el popup no define una regex comercial propia", () => {
  // La regex vivía acá como `const _AD_SALES_LOCAL_RE = /^(?:publicidad|…/`. Ahora tiene que
  // ser un alias del export compartido, no una literal.
  const propia = /const _AD_SALES_LOCAL_RE\s*=\s*\/\^/.test(popup);
  strictEqual(propia, false, "_AD_SALES_LOCAL_RE volvió a ser una regex literal en popup.js");
  ok(/const _AD_SALES_LOCAL_RE\s*=\s*AD_SALES_LOCAL;/.test(popup), "_AD_SALES_LOCAL_RE tiene que ser alias de AD_SALES_LOCAL");
});

test("el popup no tiene su propia lista de locales genéricos", () => {
  // Antes: `return /^(info|contact|hello|…)$/.test(local)` dentro de _isGenericEmailLocal.
  const bloque = popup.slice(popup.indexOf("function _isGenericEmailLocal"), popup.indexOf("function _isGenericEmailLocal") + 400);
  ok(!/\/\^\(info\|/.test(bloque), "_isGenericEmailLocal volvió a tener una lista propia de genéricos");
  ok(/_isGenericLocalPart\(/.test(bloque), "_isGenericEmailLocal tiene que delegar en _isGenericLocalPart");
});

test("el módulo compartido no depende de Node (tiene que correr en el navegador)", () => {
  const email = fs.readFileSync(path.join(aqui, "..", "lib", "email.js"), "utf8");
  const geo   = fs.readFileSync(path.join(aqui, "..", "lib", "geo.js"), "utf8");
  for (const [nombre, src] of [["email.js", email], ["geo.js", geo]]) {
    ok(!/\bBuffer\./.test(src), `${nombre} usa Buffer, que no existe en el navegador`);
    ok(!/\bprocess\.env\b/.test(src), `${nombre} usa process.env`);
    ok(!/from "node:|require\(/.test(src), `${nombre} importa algo de Node`);
  }
});

test("el empaquetado incluye el módulo compartido", () => {
  const sh = fs.readFileSync(path.join(aqui, "..", "..", "scripts", "empaquetar.sh"), "utf8");
  ok(/auto-prospector\/lib\/\{email\.js,geo\.js\}/.test(sh), "scripts/empaquetar.sh tiene que copiar lib/email.js y lib/geo.js al zip");
});
