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
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as acorn from "acorn";
import * as walk from "acorn-walk";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(aqui, "..", "..");

// Globales del navegador y del service worker. Si un día se usa uno nuevo y este test lo marca
// como "sin resolver", se agrega acá — pero primero hay que estar seguro de que existe en Chrome.
// `Buffer` es de Node: en keywords.js está detrás de `typeof btoa === "function"`, así que en el
// navegador nunca se evalúa; se lista para no dar falsa alarma.
const GLOBALS = new Set(`document window chrome fetch console setTimeout clearTimeout setInterval
clearInterval Promise Date Math JSON Object Array String Number Boolean RegExp Error TypeError
RangeError SyntaxError ReferenceError Map Set WeakMap WeakSet URL URLSearchParams
encodeURIComponent decodeURIComponent encodeURI decodeURI AbortSignal AbortController TextEncoder
TextDecoder DOMParser XMLSerializer Blob File FileReader navigator location localStorage
sessionStorage crypto btoa atob parseInt parseFloat isNaN isFinite Intl Symbol BigInt undefined NaN
Infinity requestAnimationFrame cancelAnimationFrame structuredClone alert confirm prompt Event
CustomEvent KeyboardEvent MouseEvent MutationObserver IntersectionObserver ResizeObserver
performance queueMicrotask globalThis Node NodeList HTMLElement Element Reflect Proxy arguments
eval escape unescape Uint8Array Uint16Array Uint32Array Int8Array Float32Array ArrayBuffer DataView
Function Response Request Headers FormData Image Audio open close getComputedStyle self WebSocket
Worker history screen devicePixelRatio innerWidth innerHeight scrollTo scrollBy getSelection Range
Option HTMLInputElement HTMLTextAreaElement HTMLSelectElement Text Document CSS matchMedia
ClipboardItem clients importScripts registration caches Buffer`.split(/\s+/).filter(Boolean));

// Identificadores declarados por un patrón. ⚠️ acorn-walk visita los de destructuring y defaults
// como `VariablePattern`, no como `Identifier`: sin ese visitor, `let { auth } = …` no se contaba
// como declaración y todo el análisis era ruido.
const decl = (pat, into) => walk.simple(pat, {
  Identifier(i) { into.add(i.name); },
  VariablePattern(i) { into.add(i.name); },
}, walk.base);

/** Analiza un fuente ESM. Devuelve las referencias cruzadas entre funciones y los nombres sin dueño. */
export function analizarAlcance(src) {
  const ast = acorn.parse(src, { ecmaVersion: "latest", sourceType: "module", locations: true });

  // Nombres del scope de módulo.
  const modulo = new Set();
  for (const n of ast.body) {
    if (n.type === "FunctionDeclaration" || n.type === "ClassDeclaration") modulo.add(n.id.name);
    else if (n.type === "VariableDeclaration") for (const d of n.declarations) decl(d.id, modulo);
    else if (n.type === "ImportDeclaration") for (const s of n.specifiers) modulo.add(s.local.name);
    else if (n.type === "ExportNamedDeclaration" && n.declaration) {
      const d = n.declaration;
      if (d.id) modulo.add(d.id.name);
      if (d.declarations) for (const x of d.declarations) decl(x.id, modulo);
    }
  }

  // Lo que cada función top-level declara directamente en su cuerpo: los candidatos a "tragados".
  const dentroDe = new Map();
  for (const F of ast.body.filter(n => n.type === "FunctionDeclaration")) {
    for (const s of F.body.body) {
      const names = new Set();
      if (s.type === "FunctionDeclaration") names.add(s.id.name);
      if (s.type === "VariableDeclaration") for (const d of s.declarations) decl(d.id, names);
      for (const nm of names) {
        if (!dentroDe.has(nm)) dentroDe.set(nm, []);
        dentroDe.get(nm).push(`${F.id.name}@L${s.loc.start.line}`);
      }
    }
  }

  // Todo lo declarado en cualquier lugar adentro de un statement (params, vars, catch, clases).
  const declaradosDentro = (node) => {
    const s = new Set();
    walk.fullAncestor(node, (n) => {
      if (n.type === "VariableDeclarator") decl(n.id, s);
      if (n.type === "FunctionDeclaration" || n.type === "FunctionExpression" || n.type === "ArrowFunctionExpression") {
        if (n.id) s.add(n.id.name);
        for (const p of n.params) decl(p, s);
      }
      if (n.type === "ClassDeclaration" || n.type === "ClassExpression") { if (n.id) s.add(n.id.name); }
      if (n.type === "CatchClause" && n.param) decl(n.param, s);
      if (n.type === "ImportDeclaration") for (const sp of n.specifiers) s.add(sp.local.name);
    });
    return s;
  };

  const violaciones = [], sinResolver = new Map();
  for (const n of ast.body) {
    const local = declaradosDentro(n);
    walk.fullAncestor(n, (id, _st, anc) => {
      if (id.type !== "Identifier") return;
      const p = anc[anc.length - 2];
      if (!p) return;
      // No son referencias: propiedades, claves, etiquetas, specifiers, y las propias declaraciones.
      if (p.type === "MemberExpression" && p.property === id && !p.computed) return;
      if ((p.type === "Property" || p.type === "PropertyDefinition" || p.type === "MethodDefinition") && p.key === id && !p.computed && !p.shorthand) return;
      if (p.type === "LabeledStatement" || p.type === "BreakStatement" || p.type === "ContinueStatement") return;
      if (/Specifier$/.test(p.type)) return;
      if ((p.type === "VariableDeclarator" && p.id === id) || (/^(Function|Class)(Declaration|Expression)$/.test(p.type) && p.id === id)) return;
      if (p.type === "Property" && p.value === id && anc[anc.length - 3]?.type === "ObjectPattern") return;
      const nm = id.name;
      if (local.has(nm) || modulo.has(nm) || GLOBALS.has(nm)) return;
      const quien = n.type === "FunctionDeclaration" ? n.id.name : `${n.type}@L${n.loc.start.line}`;
      if (dentroDe.has(nm)) violaciones.push(`${quien} (L${id.loc.start.line}) usa "${nm}", que está declarado ADENTRO de ${dentroDe.get(nm).join(" / ")}`);
      else {
        if (!sinResolver.has(nm)) sinResolver.set(nm, []);
        sinResolver.get(nm).push(`${quien}:L${id.loc.start.line}`);
      }
    });
  }
  return { violaciones, sinResolver };
}

// El detector se prueba a sí mismo con el caso real en miniatura: si algún día deja de ver esto,
// el resto de los tests de este archivo pasan sin mirar nada.
test("el detector ve una función declarada adentro de otra y usada desde afuera", () => {
  const { violaciones } = analizarAlcance(`
    function bindButtons() {
      document.getElementById("x").addEventListener("click", () => {});
    function buscarEnCrm() { return 1; }
    }
    async function runDuplicateCheck() { return await buscarEnCrm(); }
  `);
  strictEqual(violaciones.length, 1, `esperaba 1 violación, vinieron ${violaciones.length}`);
  ok(/runDuplicateCheck .* usa "buscarEnCrm", que está declarado ADENTRO de bindButtons/.test(violaciones[0]), violaciones[0]);
});

test("y no confunde destructuring, defaults ni parámetros con referencias libres", () => {
  const { violaciones, sinResolver } = analizarAlcance(`
    async function a({ auth, tab = null } = {}) { const [x, y] = [1, 2]; for (const [k, v] of Object.entries({})) console.log(k, v, x, y, auth, tab); }
    function b(el) { try { el.value = ""; } catch (e) { console.warn(e); } }
  `);
  strictEqual(violaciones.length, 0, violaciones.join("\n"));
  strictEqual(sinResolver.size, 0, [...sinResolver.keys()].join(", "));
});

const ARCHIVOS = [
  "popup/popup.js",
  "background/service-worker.js",
  ...fs.readdirSync(path.join(RAIZ, "modules")).filter(f => f.endsWith(".js")).map(f => `modules/${f}`),
];

for (const rel of ARCHIVOS) {
  test(`${rel}: nada declarado adentro de una función se usa desde afuera, y nada queda sin resolver`, () => {
    const src = fs.readFileSync(path.join(RAIZ, rel), "utf8");
    const { violaciones, sinResolver } = analizarAlcance(src);
    strictEqual(violaciones.length, 0,
      `${violaciones.length} referencia(s) a algo declarado adentro de otra función — es el bug que dejó el cartel del CRM ` +
      `en "Checking..." seis días:\n   ${violaciones.slice(0, 15).join("\n   ")}`);
    strictEqual(sinResolver.size, 0,
      `${sinResolver.size} nombre(s) que no existen ni en el archivo ni como global del navegador (ReferenceError en cuanto se ejecuten):\n   ` +
      [...sinResolver].slice(0, 15).map(([nm, us]) => `${nm} ← ${us.slice(0, 3).join(", ")}`).join("\n   ") +
      `\n   (si es un global real de Chrome, agregarlo a GLOBALS en este test)`);
  });
}
