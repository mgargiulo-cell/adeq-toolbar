// Un elemento que se prende y apaga con `hidden` NO puede tener un `display` propio sin su
// guarda `[hidden]`. (2026-09-07)
//
// El atributo `hidden` del HTML oculta a través de la hoja de estilos del NAVEGADOR, que pierde
// contra cualquier `display` declarado en la hoja de la extensión. El panel de traducción tenía
// `display:flex` y por eso `panelEl.hidden = true` no ocultaba nada: estuvo tapando el borrador
// de forma permanente desde que se creó, hasta que el user lo reportó ("el cajón siempre se ve
// traducido por más que yo no pase el cursor"). Es un bug invisible en code review y que ningún
// test de lógica encuentra, porque el JS está bien.
//
// Este test cruza el HTML/JS (quién usa `hidden`) contra el CSS (quién declara `display`).
//
// Run: npm test
import { test } from "node:test";
import { ok } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const raiz = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const css  = fs.readFileSync(path.join(raiz, "popup", "popup.css"), "utf8");
const html = fs.readFileSync(path.join(raiz, "popup", "popup.html"), "utf8");
const js   = fs.readFileSync(path.join(raiz, "popup", "popup.js"), "utf8");

// Clases de elementos que el código apaga con `hidden` (atributo en el HTML, o `.hidden = ` en JS).
function clasesQueUsanHidden() {
  const clases = new Set();
  // <div class="a b" hidden>
  for (const m of html.matchAll(/<[^>]*class="([^"]+)"[^>]*\shidden[\s>]/g)) {
    for (const c of m[1].split(/\s+/)) if (c) clases.add(c);
  }
  for (const m of js.matchAll(/<[^>]*class="([^"]+)"[^>]*\shidden[\s>]/g)) {
    for (const c of m[1].split(/\s+/)) if (c) clases.add(c);
  }
  return clases;
}

test("ninguna clase apagada con `hidden` declara `display` sin su guarda [hidden]", () => {
  const clases = clasesQueUsanHidden();
  ok(clases.size > 0, "no encontré ningún elemento con `hidden`: revisá el extractor");
  const rotas = [];
  for (const clase of clases) {
    // ¿La hoja declara un `display` para esa clase, en una regla que NO es la guarda?
    const reglas = [...css.matchAll(new RegExp(`^\\.${clase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\\w-])([^{]*)\\{([^}]*)\\}`, "gm"))];
    const declaraDisplay = reglas.some(r => !r[1].includes("[hidden]") && /(^|;|\s)display\s*:/.test(r[2]));
    if (!declaraDisplay) continue;
    const tieneGuarda = new RegExp(`\\.${clase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\[hidden\\]`).test(css);
    if (!tieneGuarda) rotas.push(clase);
  }
  ok(rotas.length === 0,
     `estas clases se apagan con \`hidden\` pero su \`display\` del CSS gana y las deja visibles: ${rotas.join(", ")}. ` +
     `Agregá \`.<clase>[hidden] { display: none !important; }\`.`);
});

test("el panel de traducción tiene su guarda (el caso que originó esto)", () => {
  ok(/\.pitch-es\[hidden\]\s*\{[^}]*display\s*:\s*none/.test(css),
     "falta `.pitch-es[hidden] { display: none !important; }`: el panel volvería a taparse siempre");
});
