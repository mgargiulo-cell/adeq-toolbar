// Lo que encontró la auditoría de las señales del mail y el parte del 10/09. (2026-09-13)
//
// El dueño pidió revisar cómo se interpretan los emails y por qué hay tantas URLs sin email.
// Tres fallas de causa raíz, reproducidas con el código real antes de tocar nada:
//   1. El desofuscador del worker pegaba la oración siguiente al email:
//      "info@psycho-test.org. The methodology" → info@psycho-test.org.the (parte del 10/09).
//   2. El patrón de dominios basura no estaba anclado: todo medio con "local" o "test" en el
//      nombre (diariolocal.com, speedtest.net, psycho-test.org) daba -1 y la auditoría lo borraba.
//   3. La búsqueda de contacto en Google pagaba Serper y devolvía vacío desde el 02/09 (`geo` sin
//      declarar) y nunca devolvió páginas de contacto desde el 04/08 (`CONTACT_HINT` en otra
//      función). Una quinta vía la llamaba sin tope.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { rankEmail } from "../lib/email.js";
import { cargarWorker } from "./_worker-exportado.mjs";

process.env.SERPER_API_KEY = process.env.SERPER_API_KEY || "clave-de-prueba";   // se lee al importar el worker

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const respuesta = (body) => ({ ok: true, status: 200, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) });

// ── 1. El desofuscador no pega la oración siguiente ────────────────────────────────────
test("un email que cierra una oración se extrae sin la palabra que sigue", async () => {
  const { extractEmailsFromHtml } = await cargarWorker(["extractEmailsFromHtml"]);
  const casos = [
    ["sending the method to the address info@psycho-test.org. The methodology will be reviewed", "info@psycho-test.org"],
    ["Escribinos a info@diario.com.ar. Para pautar llamanos", "info@diario.com.ar"],
    ["sales@medio.fr. Tel +33 1 23 45", "sales@medio.fr"],
    ["publicidad@site.es. De lunes a viernes", "publicidad@site.es"],
    ["contact@site.co. Il y a une équipe", "contact@site.co"],
  ];
  for (const [texto, esperado] of casos) {
    const r = extractEmailsFromHtml(texto).map(e => String(e).toLowerCase());
    ok(r.includes(esperado), `"${texto}" tenía que dar ${esperado} y dio ${JSON.stringify(r)}`);
    ok(!r.some(e => e.startsWith(esperado + ".")), `"${texto}" pegó la palabra siguiente: ${JSON.stringify(r)}`);
  }
});

test("y sigue uniendo el dominio partido que motivó el arreglo original (crnobelo, 04/08)", async () => {
  const { extractEmailsFromHtml } = await cargarWorker(["extractEmailsFromHtml"]);
  const r = extractEmailsFromHtml('<meta content="Kontakt (igor@crnobelo. com) i (emi@crnobelo. com)">').map(e => String(e).toLowerCase());
  ok(r.includes("igor@crnobelo.com") && r.includes("emi@crnobelo.com"), JSON.stringify(r));
  const r2 = extractEmailsFromHtml("mail: ventas@empresa. com.ar").map(e => String(e).toLowerCase());
  ok(r2.includes("ventas@empresa.com.ar"), JSON.stringify(r2));
});

// ── 2. El patrón de basura sólo marca dominios reservados ──────────────────────────────
test("un medio con 'local' o 'test' en el dominio no es basura", () => {
  for (const [email, dominio] of [
    ["publicidad@psycho-test.org", "psycho-test.org"], ["publicidad@speedtest.net", "speedtest.net"],
    ["publicidad@latest.com", "latest.com"], ["publicidad@diariolocal.com", "diariolocal.com"],
    ["publicidad@noticiaslocales.es", "noticiaslocales.es"], ["publicidad@localnews.com.ar", "localnews.com.ar"],
    ["publicidadlocal@diario.com", "diario.com"],
  ]) ok(rankEmail(email, dominio) > 0, `${email} dio ${rankEmail(email, dominio)}: la auditoría lo borraba de Prospects`);
});

test("pero los dominios reservados siguen siendo basura, con o sin espacios o punto final", () => {
  for (const email of ["x@example.com", "x@test.com", "x@host.local", "x@a.invalid", "x@localhost", "x@foo.test",
                       " publicidad@example.com ", "publicidad@example.com."]) {
    strictEqual(rankEmail(email, "diario.com"), -1, `${JSON.stringify(email)} tenía que seguir en -1`);
  }
});

// ── 3. La búsqueda de contacto en Google devuelve lo que pagó ──────────────────────────
test("la búsqueda de contacto devuelve el email, las páginas del sitio y el teléfono (antes: vacío con el crédito pagado)", async () => {
  const { _serperContactSearch } = await cargarWorker(["_serperContactSearch"], { fetchFalso: true });
  let pedidos = 0;
  globalThis.__fetchFalso = async () => { pedidos++; return respuesta({ organic: [
    { title: "Contacto", snippet: "publicidad@ejemplo.com.ar Tel: +54 11 4555-1234", link: "https://www.ejemplo.com.ar/contacto" },
    { title: "Quiénes somos", snippet: "", link: "https://ejemplo.com.ar/quienes-somos" },
    { title: "Otro", snippet: "ventas@otrositio.com", link: "https://otrositio.com/contacto" },
  ] }); };
  const g = await _serperContactSearch("ejemplo.com.ar");
  strictEqual(pedidos, 1);
  deepStrictEqual(g.emails, ["publicidad@ejemplo.com.ar"], "sólo el email del propio dominio");
  deepStrictEqual(g.urlsContacto.sort(), ["https://ejemplo.com.ar/quienes-somos", "https://www.ejemplo.com.ar/contacto"], "las páginas institucionales del sitio, sin el otro dominio");
  ok(g.phones.length > 0 || g.whatsapps.length > 0, `el teléfono del snippet se perdía también: ${JSON.stringify(g)}`);
});

test("un solo guardián controla el tope de Serper contacto: tope diario, sin repetir dominio, y sin config no se gasta", async () => {
  const { _serperContactoPermitido } = await cargarWorker(["_serperContactoPermitido"], { fetchFalso: true });
  globalThis.__fetchFalso = async () => respuesta([]);
  const cfg = { serper_contact_daily_cap: "2", serper_contact_used: "" };
  strictEqual(_serperContactoPermitido(null, "t", "a.com"), false, "sin config: fail closed");
  strictEqual(_serperContactoPermitido(cfg, "t", "a.com"), true);
  strictEqual(_serperContactoPermitido(cfg, "t", "www.a.com"), false, "el mismo dominio con www no se vuelve a pagar");
  strictEqual(_serperContactoPermitido(cfg, "t", "b.com"), true);
  strictEqual(_serperContactoPermitido(cfg, "t", "c.com"), false, "tope diario alcanzado");
});

test("ninguna llamada a Serper contacto se saltea el guardián, y el contador se suma en un solo lugar", () => {
  const lineas = worker.split("\n");
  const sinGuardian = [];
  lineas.forEach((l, i) => {
    if (!/_serperContactSearch\(/.test(l) || /async function _serperContactSearch\(/.test(l)) return;
    const contexto = lineas.slice(Math.max(0, i - 3), i + 1).join("\n");
    if (!/_serperContactoPermitido\(/.test(contexto)) sinGuardian.push(`L${i + 1}: ${l.trim().slice(0, 100)}`);
  });
  deepStrictEqual(sinGuardian, [], "una vía sin tope fue exactamente el bug de scrapeEmailsForDomain");
  strictEqual((worker.match(/_serperContactCount\+\+/g) || []).length, 1, "si el contador se suma en dos lugares, cada búsqueda cuenta doble");
});

test("CONTACT_HINT existe una sola vez, a nivel de módulo", () => {
  const decl = worker.match(/^.*const CONTACT_HINT = /gm) || [];
  strictEqual(decl.length, 1, `hay ${decl.length} declaraciones: dos copias se desalinean`);
  ok(decl[0].startsWith("const CONTACT_HINT"), "tiene que estar en la columna 0: adentro de una función no la ve _serperContactSearch");
  const fn = worker.slice(worker.indexOf("async function scrapeEmailsForDomain("), worker.indexOf("// ── ads.txt COMO FUENTE DE CONTACTO"));
  ok(/\.filter\(_esRutaInstitucional\)\.slice\(0, 4\)/.test(fn), "las páginas que da Google pasan el mismo filtro institucional que la fase 2");
  ok(/_tenemosContactoBueno\(emails, cleanDomain\) \|\| !_hayTiempo\(\)\) break;/.test(fn), "y el bucle respeta el reloj");
});
