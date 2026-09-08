// La extensión y el worker deciden IGUAL qué string es un email. (2026-09-08, parte del día)
//
// Caso real: peopledaily.digital. El popup topeaba el TLD en 6 letras (`.digital` tiene 7):
// sus correos jamás se extraían ni se aceptaban, y el MB terminó mandando el pitch a
// `marketing@bulawayo24.com`. El worker había subido su tope a 10 el 30/06 "por paridad con el
// popup" — y el popup seguía en 6. Ahora hay UNA función (`_dominioEmailPlausible` /
// `esEmailPlausible` en lib/email.js) y los dos lados la importan.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { _dominioEmailPlausible, esEmailPlausible, _cleanScrapedEmails } from "../lib/email.js";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const scraper = fs.readFileSync(path.join(RAIZ, "modules", "scraper.js"), "utf8");
const popup   = fs.readFileSync(path.join(RAIZ, "popup", "popup.js"), "utf8");
const worker  = fs.readFileSync(path.join(RAIZ, "auto-prospector", "index.js"), "utf8");

/** Recorta una función del fuente (salta la lista de parámetros antes de contar llaves). */
function extraer(fuente, firma) {
  const i = fuente.indexOf(firma);
  if (i < 0) throw new Error(`no encontré ${firma}`);
  let p = fuente.indexOf("(", i), prof = 0, j = -1;
  for (let k = p; k < fuente.length; k++) {
    if (fuente[k] === "(") prof++;
    else if (fuente[k] === ")" && --prof === 0) { j = fuente.indexOf("{", k); break; }
  }
  let n = 0;
  for (let k = j; k < fuente.length; k++) {
    if (fuente[k] === "{") n++;
    else if (fuente[k] === "}" && --n === 0) return fuente.slice(i, k + 1);
  }
  throw new Error(`no cerró ${firma}`);
}

// ── La regla compartida ─────────────────────────────────────────────────────────────────
test("los TLD nuevos existen: .digital, .online, .agency, .network, .marketing", () => {
  for (const d of ["peopledaily.digital", "medio.online", "grupo.agency", "red.network", "casa.marketing", "diario.international"]) {
    ok(_dominioEmailPlausible(d), `${d} es un dominio real y se rechazaba`);
  }
});

test("un TLD de dos letras sólo vale si es un país; tres o más nunca si es extensión de archivo", () => {
  ok(_dominioEmailPlausible("medio.io"), ".io existe");
  ok(_dominioEmailPlausible("diario.com.ar"), ".ar existe");
  ok(!_dominioEmailPlausible("all.he"), "`good@all.he` salió del texto de una nota: .he no existe");
  ok(!_dominioEmailPlausible("ronaldo-roots-and-early-days.html"), "un trozo de URL no es un dominio");
  ok(!_dominioEmailPlausible("foto.png"), "una imagen no es un dominio");
  ok(!_dominioEmailPlausible("x.y"), "demasiado corto");
});

test("esEmailPlausible: formato + basura, sin política de dominio cruzado", () => {
  strictEqual(esEmailPlausible("contact@peopledaily.digital"), true, "el caso del parte del 08/09");
  strictEqual(esEmailPlausible("Cpublicidade@autoracing.com.br"), true, "el prefijo C pegado se sanea, no se descarta");
  strictEqual(esEmailPlausible("good@all.he"), false);
  strictEqual(esEmailPlausible("hostmaster@medio.com"), false, "rol de registro");
  strictEqual(esEmailPlausible("vorname.name@weltwoche.ch"), false, "placeholder de plantilla");
  strictEqual(esEmailPlausible("abuse@godaddy.com"), false, "dominio de registrador");
  strictEqual(esEmailPlausible("info@sentry.io"), false, "dominio ignorado");
  strictEqual(esEmailPlausible("no es un email"), false);
});

test("_cleanScrapedEmails también corta un dominio imposible aunque el rol sea de negocio", () => {
  // Antes `info@all.he` pasaba por `isBizRole` cross-domain aunque `.he` no exista.
  deepStrictEqual(_cleanScrapedEmails(["info@all.he", "publicidad@medio.digital"], "medio.digital"), ["publicidad@medio.digital"]);
});

// ── El popup usa la regla compartida, no la suya ────────────────────────────────────────
test("scraper.js ya no topea el TLD en 6 letras en ninguna de sus tres regex", () => {
  const topes = scraper.match(/\\\.\[a-zA-Z\]\{2,(\d+)\}/g) || [];
  ok(topes.length >= 2, `esperaba al menos 2 regex de email en scraper.js, hay ${topes.length}`);
  for (const t of topes) {
    const n = Number(/\{2,(\d+)\}/.exec(t)[1]);
    ok(n >= 24, `una regex de scraper.js sigue topeando el TLD en ${n} letras: ${t}`);
  }
  ok(/import \{ esEmailPlausible \} from "\.\.\/auto-prospector\/lib\/email\.js"/.test(scraper),
     "scraper.js tiene que importar la regla compartida");
});

test("quickValidateEmail (extensión) acepta .digital y sigue rechazando el local que parece dominio", () => {
  const src = extraer(scraper, "export function quickValidateEmail(email) {").replace(/^export /, "");
  const fn = new Function("esEmailPlausible", "isWhoIsProxyEmail", `${src}; return quickValidateEmail;`)(esEmailPlausible, () => false);
  strictEqual(fn("contact@peopledaily.digital"), true);
  strictEqual(fn("owngoalnigeria.com@whoisprotectservice.net"), false, "local-part que es un dominio = artefacto");
  strictEqual(fn("good@all.he"), false);
});

test("extractEmailsFromText (extensión) extrae un correo en .digital y decodifica &#064; / &commat; / ＠", () => {
  const src = extraer(scraper, "function deobfuscateText(text) {") + "\n" + extraer(scraper, "function extractEmailsFromText(text) {");
  const fn = new Function(`${src}; return extractEmailsFromText;`)();
  deepStrictEqual(fn("Escribinos a contact@peopledaily.digital hoy"), ["contact@peopledaily.digital"]);
  deepStrictEqual(fn("mail: ventas&#064;diario.com.ar"), ["ventas@diario.com.ar"]);
  deepStrictEqual(fn("mail: ventas&commat;diario.com.ar"), ["ventas@diario.com.ar"]);
  deepStrictEqual(fn("mail: ventas＠diario.com.ar"), ["ventas@diario.com.ar"]);
});

test("el worker tampoco topea en 10: sus dos regex van a 24", () => {
  const topes = (worker.match(/@\[a-zA-Z0-9\.\\-\]\+\\\.\[a-zA-Z\]\{2,(\d+)\}/g) || []).map(t => Number(/\{2,(\d+)\}/.exec(t)[1]));
  ok(topes.length >= 2, `esperaba las 2 regex del worker, encontré ${topes.length}`);
  for (const n of topes) ok(n >= 24, `una regex del worker sigue en ${n}`);
});

// ── El popup acepta lo que el worker acepta ─────────────────────────────────────────────
test("addEmailsWithSource pasa por _cleanScrapedEmails, con procedencia para Page/Scrape", () => {
  const src = extraer(popup, "function addEmailsWithSource(emails, source, domainGuard = null) {");
  ok(/_cleanScrapedEmails\(lista, currentSite, \{ urlByEmail \}\)/.test(src),
     "la extensión tenía su propio criterio (dominio del lead o webmail) y tiraba el buzón de la casa editora impreso en el sitio");
  ok(/source === "Page" \|\| source === "Scrape"/.test(src),
     "lo que se encontró en el propio sitio lleva URL de origen: es su contacto aunque el dominio sea otro");
  ok(/source === "Apollo" \|\| source === "Cache"/.test(src),
     "Apollo y Cache conservan el filtro viejo: ya vienen acotados al dominio");
  ok(/import \{[^}]*_cleanScrapedEmails[^}]*\} from "\.\.\/auto-prospector\/lib\/email\.js"/.test(popup),
     "popup.js tiene que importar _cleanScrapedEmails de lib/email.js");
});
