// El ranking de emails, la misma regla en el agente y en la extensión. (2026-09-13)
//
// El parte del 10/09 mostró a los media buyers escribiendo a mano, desde la extensión, a
// download@pixelmonmod.com, advent@bisafans.de, series@racingnews365.com, copyright@pokecommunity.com
// y owner@pokexperto.net. La auditoría encontró dos causas en lib/email.js, que usan los dos lados:
//   1. Cualquier palabra de 5 a 15 letras puntuaba como persona: download@ valía 95, info@ 55.
//   2. Los vetos duros (-1) sólo mandaban la dirección al final en la extensión; si era la única,
//      quedaba preseleccionada. Ahora son una función (vetoDuroEmail) que la extensión también usa.
// Y el agente clasificaba el tipo de email por la FUENTE, mientras la extensión mira la DIRECCIÓN.
//
// Mover los vetos a una función no puede cambiar ningún puntaje: el primer test lo exige contra
// los valores medidos antes del cambio, con la única excepción buscada (los buzones funcionales).
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { rankEmail, vetoDuroEmail, esBuzonFuncional, _tipoDeEmailParaRanking } from "../lib/email.js";
import { isGarbageEmail } from "../../modules/emailVerifier.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const popup = fs.readFileSync(path.join(aqui, "..", "..", "popup", "popup.js"), "utf8");
const verifier = fs.readFileSync(path.join(aqui, "..", "..", "modules", "emailVerifier.js"), "utf8");
const D = "diario-ejemplo.com";

// Medido con lib/email.js ANTES de extraer los vetos (2026-09-13).
const ANTES = [
  ["publicidad@"+D, D, 135], ["ventas@"+D, D, 135], ["comercial@"+D, D, 135], ["marketing@"+D, D, 135], ["sales@"+D, D, 135], ["advertising@"+D, D, 135],
  ["info@"+D, D, 55], ["contacto@"+D, D, 55], ["contact@"+D, D, 55], ["press@"+D, D, 115], ["prensa@"+D, D, 115], ["redaccion@"+D, D, 115], ["editor@"+D, D, 115],
  ["juan.perez@"+D, D, 110], ["jperez@"+D, D, 95], ["guillermo@"+D, D, 95], ["ayesha@"+D, D, 95],
  // webmaster@ valía -1 (veto duro) cuando se midió esta tabla. Decisión del dueño del 13/09 ("webmaster@
  // SÍ, office@ NO"): es un contacto válido de puntaje bajo, 45 como informatique@, debajo de info@ (55).
  // Ver tests/webmaster-13-09f.test.js. Los demás valores de la tabla no cambian.
  ["support@"+D, D, 20], ["admin@"+D, D, 25], ["webmaster@"+D, D, 45], ["noreply@"+D, D, -1], ["dpo@"+D, D, -1], ["privacy@"+D, D, -1],
  ["copyright@pokecommunity.com", "pokecommunity.com", -1], ["owner@pokexperto.net", "pokexperto.net", -1], ["jobs@"+D, D, -1], ["careers@"+D, D, -1], ["billing@"+D, D, -1], ["customer@"+D, D, -1],
  ["info@gmail.com", D, -1], ["juanperez@gmail.com", D, 65], ["cuenta@gmail.com", D, -1],
  ["a@"+D, D, -1], ["66@"+D, D, -1], ["hi@thevocket.com", "thevocket.com", -15], ["x7k9m2p4@"+D, D, -1], ["u003eenquiry@mytvsuper.com", "mytvsuper.com", -1], ["dmarcreport@"+D, D, -1],
  ["domainmanagement@axa.com", "axa.com", -1], ["site.com@registrar.com", D, -1], ["brainberries.co@gmail.com", "brainberries.co", 80], ["firstname.lastname@"+D, D, -1],
  ["info@prowell.media", "atomix.vg", -35], ["guillermo.cruz@prowell.media", "atomix.vg", 20], ["osiris.peinado@oprjobs.com", "elmanana.com.mx", 20], ["tarreo@buscatodo.com", "tarreo.com", 65],
  ["download@pixelmonmod.com", "pixelmonmod.com", 95], ["advent@bisafans.de", "bisafans.de", 95], ["series@racingnews365.com", "racingnews365.com", 95], ["techsupport@networld.hk", "discuss.com.hk", 5],
  ["rewards@best.enchantedmc.net", "best-minecraft-servers.co", 5], ["store@"+D, D, 95], ["events@"+D, D, 95], ["x@example.com", D, -1], ["publicidad@psycho-test.org", "psycho-test.org", 135],
];
// El único cambio buscado: un buzón funcional del dominio propio deja de valer como persona.
const FUNCIONAL_PROPIO = new Set(["download@pixelmonmod.com", "advent@bisafans.de", "series@racingnews365.com", "store@"+D, "events@"+D]);

test("extraer los vetos no cambió ningún puntaje (salvo los buzones funcionales, a propósito)", () => {
  const distintos = [];
  for (const [e, d, antes] of ANTES) {
    const ahora = rankEmail(e, d, "");
    if (FUNCIONAL_PROPIO.has(e)) { if (ahora !== 48) distintos.push(`${e}: esperaba 48 (40 de dominio + 8), dio ${ahora}`); }
    else if (ahora !== antes) distintos.push(`${e} (${d}): antes ${antes}, ahora ${ahora}`);
  }
  strictEqual(distintos.length, 0, distintos.join("\n"));
});

test("todo veto duro da -1 en rankEmail, y un puntaje negativo NO es un veto", () => {
  for (const [e, d] of ANTES) if (vetoDuroEmail(e, d)) strictEqual(rankEmail(e, d, ""), -1, `${e} está vetado (${vetoDuroEmail(e, d)}) y no da -1`);
  strictEqual(vetoDuroEmail("info@prowell.media", "atomix.vg"), "", "un buzón del grupo editor vale -35 sin la casa editora, pero no está vetado");
  strictEqual(vetoDuroEmail("hi@thevocket.com", "thevocket.com"), "");
  for (const e of ["copyright@pokecommunity.com", "owner@pokexperto.net", "jobs@"+D, "billing@"+D, "customer@"+D, "info@gmail.com"]) ok(vetoDuroEmail(e, e.split("@")[1]), `${e} tiene que estar vetado`);
});

// ── La extensión esconde lo mismo que el worker descarta ────────────────────────────────
test("la extensión no muestra lo que el worker nunca usaría, y sí muestra los buzones del grupo editor", () => {
  for (const [e, sitio] of [["copyright@pokecommunity.com", "pokecommunity.com"], ["owner@pokexperto.net", "pokexperto.net"], ["jobs@diario.com", "diario.com"],
                            ["careers@diario.com", "diario.com"], ["billing@diario.com", "diario.com"], ["customer@diario.com", "diario.com"], ["info@gmail.com", "diario.com"]]) {
    strictEqual(isGarbageEmail(e, sitio), true, `${e} no puede aparecer como chip en la extensión`);
  }
  for (const [e, sitio] of [["info@prowell.media", "atomix.vg"], ["contacto@buscatodo.com", "tarreo.com"], ["info@networld.hk", "discuss.com.hk"],
                            ["admin@diario.com", "diario.com"], ["redaccion@diario.com", "diario.com"], ["support@diario.com", "diario.com"], ["juan.perez@gmail.com", "diario.com"]]) {
    strictEqual(isGarbageEmail(e, sitio), false, `${e} es un contacto posible y tiene que verse`);
  }
});

test("la extensión usa los vetos y el criterio de buzón funcional del módulo compartido, y pasa el dominio siempre", () => {
  // Desde el 13/09 (T1 de ranking_extension-13-09b) el criterio de buzón funcional le llega al popup
  // adentro de claseDeEmail, la misma función que usa el agente, y no importa esBuzonFuncional suelto.
  ok(/import \{[^}]*\bvetoDuroEmail\b[^}]*\bclaseDeEmail\b[^}]*\} from "\.\.\/auto-prospector\/lib\/email\.js"/.test(popup), "popup.js importa vetoDuroEmail y claseDeEmail");
  const tier = popup.slice(popup.indexOf("function _emailPickTierClient"), popup.indexOf("function _ordenarEmailsClient"));
  // `ctx.domain` desde el 13/09 (ranking_extension-13-09b): la tarjeta de Prospects pasa su propio dominio.
  ok(/if \(vetoDuroEmail\(email, (?:state|ctx)\.domain \|\| ""\)\) return -1;/.test(tier), "el tier -1 sale del veto, no del puntaje");
  ok(!/_rankClient\(email\) < 0/.test(tier), "un puntaje negativo hundía a los buzones del grupo editor");
  // Un buzón funcional va con los genéricos: lo decide claseDeEmail (que incluye esBuzonFuncional), la
  // misma función del agente; la paridad de comportamiento la fija el test T1 de ranking_extension-13-09b.
  // (2026-09-13, cierre de eleccion_paridad) El tipo sale de tierDeEmail, que clasifica con claseDeEmail: la
  // misma función del _pickTier del agente. Lo que se protege es lo mismo: ninguna lista propia en el popup.
  ok(/tierDeEmail\(email, src, ctx\.ordenTiers\)/.test(tier) && !/esBuzonFuncional\(|_isGenericEmailLocal\(/.test(tier), "un buzón funcional va con los genéricos");
  const llamadas = popup.match(/isGarbageEmail\([^)]*\)/g) || [];
  ok(llamadas.length >= 5, `esperaba al menos 5 llamadas, hay ${llamadas.length}`);
  for (const c of llamadas) ok(c.includes(","), `${c} tiene que pasar el dominio del sitio`);
  ok(/import \{ vetoDuroEmail \} from "\.\.\/auto-prospector\/lib\/email\.js";/.test(verifier), "emailVerifier importa el veto en su propia línea");
  ok(/export function isGarbageEmail\(email, siteDomain = ""\)/.test(verifier) && /if \(vetoDuroEmail\(e, siteDomain\)\) return true;/.test(verifier));
});

// ── Buzones funcionales ─────────────────────────────────────────────────────────────────
test("un buzón funcional no es una persona: queda por debajo de info@ pero sigue siendo usable", () => {
  for (const l of ["download", "rewards", "advent", "series", "techsupport", "store", "shop", "events", "eventos", "descargas", "mods"]) {
    ok(esBuzonFuncional(`${l}@${D}`), `${l}@ es un buzón funcional`);
    const s = rankEmail(`${l}@${D}`, D, "");
    ok(s >= 0 && s < rankEmail(`info@${D}`, D, ""), `${l}@ vale ${s}: tiene que ser >= 0 (si no, la auditoría lo borra) y < info@`);
  }
  for (const l of ["donatella", "storey", "reportero", "guillermo", "ayesha"]) ok(!esBuzonFuncional(`${l}@${D}`), `${l} es un nombre, no un buzón funcional`);
  strictEqual(rankEmail("techsupport@networld.hk", "discuss.com.hk", ""), 5, "de otro dominio no se toca: ya vale poco y bajarlo lo borraría");
});

// Revisión del 13/09: la primera versión de esBuzonFuncional miraba un solo trozo y pisaba al rol
// comercial. Estos valores son los de lib/email.js en c0a6b82, antes de los buzones funcionales.
test("un trozo comercial o de decisor le gana al buzón funcional: eventos.comercial@ sigue siendo comercial", () => {
  for (const [l, antes] of [["eventos.comercial", 135], ["events.marketing", 135], ["events.sales", 135], ["eventi.pubblicita", 135], ["shop.advertising", 135],
                            ["tienda.ventas", 135], ["eventos.patrocinio", 110], ["events.sponsorship", 110], ["awards.partnerships", 110], ["events.partner", 110], ["events.director", 110]]) {
    strictEqual(esBuzonFuncional(`${l}@${D}`), false, `${l}@ es el área que vende o arma acuerdos, no un buzón funcional`);
    strictEqual(rankEmail(`${l}@${D}`, D, ""), antes, `${l}@ tiene que valer lo mismo que antes de los buzones funcionales`);
    strictEqual(_tipoDeEmailParaRanking(`${l}@${D}`, "scrape"), "persona", `${l}@ no puede bajar al tier de los genéricos`);
  }
  ok(rankEmail(`eventos.patrocinio@${D}`, D, "") >= 50, "debajo de 50 el worker lo trata como lead sin buen email y paga enriquecimiento");
});

test("los TLD reservados (.test, .example, .local) sólo cuentan en el dominio, no antes de la arroba", () => {
  for (const e of [`maria.test@${D}`, `equipo.example@${D}`, `info.test@${D}`, `ana.local@${D}`]) strictEqual(vetoDuroEmail(e, D), "", `${e} es una dirección real del sitio`);
  strictEqual(rankEmail(`maria.test@${D}`, D, ""), 110);
  strictEqual(vetoDuroEmail("maria@lab.test.io", "lab.test.io"), "", "un subdominio que se llama test no es un TLD reservado");
  for (const e of ["maria@foo.test", "maria@example.com", "maria@example.com.", "maria@site.local", "maria@site.invalid", "maria@localhost"]) strictEqual(vetoDuroEmail(e, D), "basura", `${e} es un dominio reservado`);
});

test("el agente clasifica el tipo de email por la dirección, como la extensión", () => {
  for (const src of ["scrape", "Facebook", "unknown", "cache", "rol_mx"]) strictEqual(_tipoDeEmailParaRanking(`info@${D}`, src), "generico", `info@ con fuente ${src}`);
  strictEqual(_tipoDeEmailParaRanking(`download@${D}`, "scrape"), "generico", "un buzón funcional va con los genéricos");
  strictEqual(_tipoDeEmailParaRanking(`marketing@${D}`, "Instagram"), "rol");
  strictEqual(_tipoDeEmailParaRanking(`juan.perez@${D}`, "scrape"), "persona");
  strictEqual(_tipoDeEmailParaRanking(`info@${D}`, "manual"), "apollo", "lo que el MB eligió a mano sigue arriba");
});
