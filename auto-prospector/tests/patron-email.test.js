// Los pendientes del plan del 08/09, todos juntos: inferencia por patrón (la técnica de Hunter
// que faltaba), nombres cosechados del sitio, email-en-imagen, Instagram/Telegram, directorio de
// prensa como tercera fuente GEO, ciudades de Asia y la extensión con la misma regla de basura.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generarHipotesisDePatron, _partesDeNombre, _patronDeEmailsConocidos } from "../lib/email.js";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const worker = fs.readFileSync(path.join(RAIZ, "auto-prospector", "index.js"), "utf8");
const verifier = fs.readFileSync(path.join(RAIZ, "modules", "emailVerifier.js"), "utf8");

// ── E1 · inferencia por patrón (puro: no verifica, sólo propone) ─────────────────────────
test("de un nombre real salen las tres formas más comunes, sin acentos y en orden", () => {
  deepStrictEqual(generarHipotesisDePatron({ nombre: "María José Pérez", dominio: "diario.com.ar" }),
    ["maria.perez@diario.com.ar", "mperez@diario.com.ar", "maria@diario.com.ar"]);
});

test("si el dominio ya tiene un email personal, se copia SU patrón y sale UNA sola hipótesis", () => {
  deepStrictEqual(generarHipotesisDePatron({ nombre: "Juan Gómez", dominio: "medio.pe", emailsConocidos: ["ana.lopez@medio.pe", "info@medio.pe"] }),
    ["juan.gomez@medio.pe"], "un crédito de MillionVerifier, no tres");
  deepStrictEqual(generarHipotesisDePatron({ nombre: "Juan Gómez", dominio: "medio.pe", emailsConocidos: ["a.lopez@medio.pe"] }),
    ["j.gomez@medio.pe"]);
  strictEqual(_patronDeEmailsConocidos(["publicidad@medio.pe", "info@medio.pe"], "medio.pe"), null, "info@ y publicidad@ no dicen nada del patrón");
  strictEqual(_patronDeEmailsConocidos(["jperez@medio.pe"], "medio.pe"), null, "'jperez' es ambiguo (¿inicial+apellido? ¿apodo?): no se adivina");
});

test("un rol, un genérico o una sola palabra NO son un nombre: no se inventa nada", () => {
  for (const n of ["Redacción", "Equipo Comercial", "Director General", "Staff", "info", "Sra."]) {
    strictEqual(_partesDeNombre(n), null, `"${n}" no es una persona`);
    deepStrictEqual(generarHipotesisDePatron({ nombre: n, dominio: "medio.com" }), []);
  }
  deepStrictEqual(generarHipotesisDePatron({ nombre: "Pedro", dominio: "medio.com" }), [], "sin apellido no hay patrón");
});

test("con un dominio imposible no hay hipótesis", () => {
  deepStrictEqual(generarHipotesisDePatron({ nombre: "Ana Ruiz", dominio: "all.he" }), []);
  deepStrictEqual(generarHipotesisDePatron({ nombre: "Ana Ruiz", dominio: "" }), []);
});

test("el pulido tiene el paso 5c y sólo acepta lo que MillionVerifier confirma 'ok'", () => {
  const i = worker.indexOf("// 5c) INFERENCIA POR PATRÓN");
  ok(i > 0, "tiene que existir el paso 5c en polishPool");
  const paso = worker.slice(i, worker.indexOf("// 6) ÚLTIMO RECURSO", i));
  ok(/generarHipotesisDePatron\(\{ nombre: _nom, dominio: domain, emailsConocidos/.test(paso), "usa el generador compartido con los emails ya conocidos");
  ok(/decidirVerificacionMV\(_e, "pattern"\)/.test(paso), "pregunta primero si el dominio es catch-all: ahí no se gasta ni se adivina");
  ok(/if \(_estado === "ok"\)/.test(paso), "sólo entra la hipótesis que MV confirma 'ok' — ni 'riesgo' ni 'dudoso'");
  ok(/MILLIONVERIFIER_API_KEY \|\| cfg\.millionverifier_api_key/.test(paso), "sin clave de MV la fase no existe: nunca se manda una hipótesis sin verificar");
  ok(/polish_patron_daily_cap/.test(paso) && /_patronHoy/.test(paso), "tope diario propio");
  ok(/foundSource = "pattern"/.test(paso), "la fuente queda marcada como patrón para el ranking y la ficha");
  ok(/_nombresOut/.test(paso) && /nombresOut: _nombresOut/.test(worker), "los nombres cosechados del sitio llegan al paso");
});

test("el crawl cosecha nombres de personas (JSON-LD / meta author) y detecta el mail en imagen", () => {
  const fn = worker.slice(worker.indexOf("async function scrapeEmailsForDomain("), worker.indexOf("\n}\n", worker.indexOf("async function scrapeEmailsForDomain(")));
  ok(/const nombresOut = opts\.nombresOut \|\| null;/.test(fn));
  ok(/"@type"\\s\*:\\s\*"Person"/.test(fn), "autores del JSON-LD");
  ok(/name=\["'\]author\["'\]/.test(fn), "<meta name=author>");
  ok(/_stats\.emailEnImagen = true/.test(fn), "página de contacto sin texto de email pero con imagen 'email/correo': se marca");
  const motivo = worker.slice(worker.indexOf("function _motivoSinEmail(diag, stats) {"), worker.indexOf("\n}\n", worker.indexOf("function _motivoSinEmail(diag, stats) {")));
  ok(/return "email_en_imagen"/.test(motivo), "y el motivo lo dice, para no reintentar eternamente");
});

test("Instagram, LinkedIn y Telegram se detectan; Instagram y Telegram se leen", () => {
  ok(/instagram\\\.com\|linkedin\\\.com\\\/company\|t\\\.me/.test(worker), "SOCIAL_RE tiene que incluir las tres redes");
  const fn = worker.slice(worker.indexOf("async function _scrapeEmailsFromSocialLinksWorker("), worker.indexOf("async function scrapeEmailsForDomain("));
  ok(/https:\/\/www\.instagram\.com\/\$\{user\}\//.test(fn), "perfil público de Instagram");
  ok(/https:\/\/t\.me\/s\/\$\{m\[1\]\}/.test(fn), "preview pública del canal de Telegram");
  ok(!/linkedin\.com\/company\/\$\{/.test(fn), "LinkedIn no se lee (sesión obligatoria): el link queda para el MB");
});

// ── Feeder: tercera fuente GEO ───────────────────────────────────────────────────────────
test("el directorio de prensa es la tercera fuente GEO, con su etiqueta y su cupo", () => {
  ok(/async function _directorioMediosDelPais\(cc\)/.test(worker));
  ok(/onlinenewspapers\.com\/\$\{slug\}\.shtml/.test(worker));
  const geo = worker.slice(worker.indexOf("async function _feederPullGeo("), worker.indexOf("const FEEDER_SOURCE_KEYS = ["));
  ok(/"auto_feeder_directorio"\)/.test(geo), "inyecta con su propia etiqueta");
  ok(/out\.wikidata < Math\.ceil\(maxInject \/ 2\)/.test(geo), "sólo si Wikidata no llenó la mitad: es la fuente chica");
  ok(/case "auto_feeder_directorio":\s+source = "directorio"/.test(worker), "se traduce al source de Prospects");
  ok(/auto_feeder_directorio: 120/.test(worker), "cupo activo propio");
  ok(/fromGeo\.directorio/.test(worker.slice(worker.indexOf("async function _runFeederSlot("))), "sus brutos entran al total del slot");
  const slugs = /const _DIRECTORIO_SLUG = \{([\s\S]*?)\};/.exec(worker)[1];
  for (const cc of ["co", "pe", "pl", "ng", "id", "ar"]) ok(new RegExp(`\\b${cc}: "`).test(slugs), `falta el slug de ${cc} (verificado en vivo que responde 200)`);
});

// ── AutoGoogle: Asia ────────────────────────────────────────────────────────────────────
test("Japón, Corea, Vietnam, Tailandia, Malasia y Filipinas tienen ciudades, y Filipinas va en inglés", () => {
  const ciudades = worker.slice(worker.indexOf("const _CIUDADES = {"), worker.indexOf("};", worker.indexOf("const _CIUDADES = {")));
  for (const cc of ["jp", "kr", "vn", "th", "my", "ph"]) ok(new RegExp(`\\n\\s+${cc}: \\[`).test(ciudades), `faltan las ciudades de ${cc}`);
  const idioma = worker.slice(worker.indexOf("const _IDIOMA_DE_PAIS = {"), worker.indexOf("};", worker.indexOf("const _IDIOMA_DE_PAIS = {")));
  ok(/\bph: "en"/.test(idioma), "Filipinas en inglés");
  for (const cc of ["jp", "kr", "vn", "th", "my"]) ok(new RegExp(`\\b${cc}: "`).test(idioma), `${cc} tiene que tener idioma (ya lo tenía; que no se pierda)`);
  ok(/en: \["za", "ke", "ng", "gh", "ph"\]/.test(worker));
  ok(/"\.ph", "\.com\.ph"/.test(worker), "TLDs de Filipinas para el sesgo de búsqueda");
});

// ── Extensión: una sola regla de basura ─────────────────────────────────────────────────
test("isGarbageEmail (extensión) consulta primero la regla compartida del worker", () => {
  ok(/import \{ esEmailPlausible \} from "\.\.\/auto-prospector\/lib\/email\.js"/.test(verifier));
  const fn = verifier.slice(verifier.indexOf("export function isGarbageEmail(email) {"), verifier.indexOf("\n}\n", verifier.indexOf("export function isGarbageEmail(email) {")));
  ok(/if \(!esEmailPlausible\(e\)\) return true;/.test(fn), "lo que el worker considera imposible, la extensión tampoco lo muestra");
});
