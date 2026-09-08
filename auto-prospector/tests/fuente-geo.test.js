// Descubrir por país, omitiendo a USA, con fuentes que nacen filtradas. (2026-09-08, plan del user)
//
// *"El feeder descubre mucho USA… debería poder descubrir webs de geos al azar que uno ponga,
// omitiendo USA, Canadá, UK y Oceanía."* Dos fuentes nuevas, gratis y medidas en vivo: el top de
// Chrome por país (CrUX, 261.811 orígenes para Colombia) y los medios con sitio oficial de
// Wikidata (Colombia 389, Polonia 1.395). Este archivo fija el cableado y la regla de país.
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

test("un host es 'del país' por su TLD, y nunca una IP ni un puerto raro", async () => {
  const mod = await cargarWorker(["_hostEsDelPais"]);
  strictEqual(mod._hostEsDelPais("eltiempo.com", "co"), false, "global, aunque se visite desde Colombia");
  strictEqual(mod._hostEsDelPais("elcolombiano.com.co", "co"), true);
  strictEqual(mod._hostEsDelPais("bluradio.co", "co"), true);
  strictEqual(mod._hostEsDelPais("192.168.1.1", "co"), false);
  strictEqual(mod._hostEsDelPais("bbc.co.uk", "gb"), true, "el único país cuyo TLD no es su código");
});

test("los anglo quedan afuera aunque alguien los ponga en la config", () => {
  const m = /const _GEO_FEEDER_ANGLO = new Set\(\[([^\]]+)\]\)/.exec(worker);
  ok(m, "tiene que existir la lista de exclusión");
  for (const cc of ["us", "ca", "gb", "uk", "au", "nz", "ie"]) ok(m[1].includes(`"${cc}"`), `${cc} tiene que estar excluido siempre`);
  const def = /const _GEO_FEEDER_PAISES_DEFAULT = "([^"]+)"/.exec(worker)[1].split(",");
  for (const cc of ["us", "ca", "gb", "au", "nz", "ie"]) ok(!def.includes(cc), `${cc} no puede estar en el default`);
  ok(def.length >= 30, `el default tiene ${def.length} países: LATAM + Europa + África + Asia`);
  for (const cc of ["co", "pe", "pl", "ng", "ke", "id", "ma"]) ok(def.includes(cc), `falta ${cc} en el default`);
});

test("la fuente GEO está enchufada al slot del feeder y se mide por separado", () => {
  const slot = worker.slice(worker.indexOf("async function _runFeederSlot("), worker.indexOf("async function _measureFeederRuns("));
  ok(/await _feederPullGeo\(token/.test(slot), "el slot tiene que llamar a la fuente GEO");
  ok(/fromGeoTotal = \(fromGeo\.crux \|\| 0\) \+ \(fromGeo\.wikidata \|\| 0\) \+ \(fromGeo\.directorio \|\| 0\)/.test(slot) && /\+ fromGeoTotal;/.test(slot),
     "sus brutos (crux + wikidata + directorio) entran al total del slot");
  ok(/geo=\$\{fromGeo\.pais\}/.test(slot), "las notas del run dicen qué país tocó");
  // Cada fuente con su etiqueta: si una no rinde, el parte lo muestra y se baja sola.
  ok(/"auto_feeder_wikidata"\)/.test(worker) && /"auto_feeder_crux"\)/.test(worker), "las dos etiquetas tienen que inyectarse por separado");
  ok(/case "auto_feeder_crux":\s+source = "crux"/.test(worker) && /case "auto_feeder_wikidata":\s+source = "wikidata"/.test(worker),
     "la etiqueta se traduce al source de Prospects, o caería en 'origen_desconocido'");
  ok(/auto_feeder_crux:\s+120/.test(worker) && /auto_feeder_wikidata:\s+120/.test(worker), "cada carril con su cupo activo");
});

test("CrUX pide sólo rank ≤ 100K y filtra gov/edu y hosts técnicos", () => {
  const fn = worker.slice(worker.indexOf("async function _cruxDominiosDelPais("), worker.indexOf("async function _wikidataMediosDelPais("));
  ok(/_GEO_FEEDER_RANK_MAX/.test(fn) && /const _GEO_FEEDER_RANK_MAX = 100_000;/.test(worker));
  ok(/\(gov\|gob\|edu\|mil\|ac\)/.test(fn), "gobierno y universidades no son publishers");
  ok(/_hostEsDelPais\(host, cc\)/.test(fn), "sin el TLD entra lo global que se visita desde el país (healthline, fandom)");
});

test("Wikidata busca medios por código ISO del país, sin tabla de QIDs a mantener", () => {
  const fn = worker.slice(worker.indexOf("async function _wikidataMediosDelPais("), worker.indexOf("async function _feederPullGeo("));
  ok(/wdt:P297/.test(fn), "P297 = ISO-2: así no hay que mapear cada país a su QID");
  ok(/wd:Q11032/.test(fn) && /wd:Q1153191/.test(fn), "diario y diario online, como mínimo");
  ok(/wdt:P856/.test(fn), "sólo los que tienen sitio oficial");
});

// ── PDF de media kits ───────────────────────────────────────────────────────────────────
test("el crawl cosecha PDFs comerciales y los lee al final, sólo si hace falta", () => {
  const fn = worker.slice(worker.indexOf("async function scrapeEmailsForDomain("), worker.indexOf("\n}\n", worker.indexOf("async function scrapeEmailsForDomain(")));
  ok(/const pdfLinks = new Set\(\)/.test(fn), "tiene que existir la cosecha de PDFs");
  ok(/media\[-_ \]\?kit\|mediakit\|mediadaten\|tarif/.test(fn), "sólo PDFs que por nombre o ancla parecen comerciales");
  ok(/import\("pdf-parse"\)/.test(fn), "pdf-parse se carga perezoso: si falta el binario en Railway, la fase se saltea");
  ok(/!_tenemosContactoBueno\(emails, cleanDomain\)\) \{\s*let PDFParse/.test(fn), "no se lee ningún PDF si ya hay un contacto bueno");
  ok(/6_000_000/.test(fn), "hay un tope de tamaño");
  ok(/urlByEmail\.set\(lower, pdfUrl\)/.test(fn), "el PDF es del propio sitio: la procedencia se guarda para _cleanScrapedEmails");
  const pkg = JSON.parse(fs.readFileSync(path.join(RAIZ, "auto-prospector", "package.json"), "utf8"));
  ok(pkg.dependencies["pdf-parse"], "pdf-parse tiene que estar en package.json");
});

// ── AutoGoogle: África anglófona ────────────────────────────────────────────────────────
test("Sudáfrica, Kenia, Nigeria y Ghana se buscan en inglés con gl local", () => {
  const idioma = worker.slice(worker.indexOf("const _IDIOMA_DE_PAIS = {"), worker.indexOf("};", worker.indexOf("const _IDIOMA_DE_PAIS = {")));
  for (const cc of ["za", "ke", "ng", "gh"]) ok(new RegExp(`\\b${cc}: "en"`).test(idioma), `${cc} tiene que mapear a inglés — sin esto sus ciudades se buscan con plantillas en español`);
  const ciudades = worker.slice(worker.indexOf("const _CIUDADES = {"), worker.indexOf("};", worker.indexOf("const _CIUDADES = {")));
  for (const cc of ["ke", "ng", "gh"]) ok(new RegExp(`\\n\\s+${cc}: \\[`).test(ciudades), `faltan las ciudades de ${cc}`);
  ok(/en: \["za", "ke", "ng", "gh", "ph"\]/.test(worker), "_PAISES_POR_IDIOMA.en tiene que listar los cuatro africanos y Filipinas");
  ok(/en: \[".co.za", ".za", ".co.ke", ".ke", ".ng", ".com.ng", ".gh", ".com.gh", ".ph", ".com.ph"\]/.test(worker), "y sus TLDs para el sesgo de búsqueda");
});
