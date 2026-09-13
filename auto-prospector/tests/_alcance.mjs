// El analizador de alcance, compartido por los tests de la extensión y del worker. (2026-09-13)
//
// Nació el 08/09 en alcance-popup.test.js para la extensión. El 13/09 apareció la misma clase de
// falla en el worker, donde nadie la miraba: `_serperContactSearch` usaba `geo` (no existía) y
// `CONTACT_HINT` (declarada adentro de otra función). En un módulo strict eso es ReferenceError, y
// como estaba dentro de un try/catch, la búsqueda de contacto en Google pagaba Serper y devolvía
// vacío desde el 02/09 sin un solo aviso. `node --check` y los 233 tests pasaban.
//
// No es un test (el glob `tests/*.test.js` no lo toma): lo importan los dos.
import * as acorn from "acorn";
import * as walk from "acorn-walk";

// Globales del navegador y del service worker. Si un día se usa uno nuevo y el test lo marca como
// "sin resolver", se agrega acá — pero primero hay que estar seguro de que existe en Chrome.
// `Buffer` es de Node: en keywords.js está detrás de `typeof btoa === "function"`, así que en el
// navegador nunca se evalúa; se lista para no dar falsa alarma.
export const GLOBALS_NAVEGADOR = new Set(`document window chrome fetch console setTimeout clearTimeout setInterval
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

// El worker corre en Node: se suman sus globales.
export const GLOBALS_NODE = new Set([...GLOBALS_NAVEGADOR, "process", "Buffer", "setImmediate", "clearImmediate", "global"]);

// Identificadores declarados por un patrón. ⚠️ acorn-walk visita los de destructuring y defaults
// como `VariablePattern`, no como `Identifier`: sin ese visitor, `let { auth } = …` no se contaba
// como declaración y todo el análisis era ruido.
const decl = (pat, into) => walk.simple(pat, {
  Identifier(i) { into.add(i.name); },
  VariablePattern(i) { into.add(i.name); },
}, walk.base);

/** Analiza un fuente ESM. Devuelve las referencias cruzadas entre funciones y los nombres sin dueño. */
export function analizarAlcance(src, globals = GLOBALS_NAVEGADOR) {
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
      if (local.has(nm) || modulo.has(nm) || globals.has(nm)) return;
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
