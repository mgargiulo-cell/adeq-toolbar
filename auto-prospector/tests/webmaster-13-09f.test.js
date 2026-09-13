// webmaster@ SÍ, office@ NO. Decisión del dueño del 13/09. (2026-09-13)
//
// Hasta hoy webmaster@ era veto duro: estaba en la lista de basura de lib/email.js (GARBAGE_LOCAL) y en
// la de la extensión (GARBAGE_LOCAL_PREFIXES). La extensión lo escondía, el agente nunca le escribía, el
// pulido lo rechazaba (rechazados_por_ranking) y la auditoría lo sacaba del pool. Y sus variantes estaban
// al revés: `webmaster.diario@` valía 112 y era "persona" en el worker (arriba de info@) mientras la
// extensión la escondía.
//
// Ahora es un contacto válido de puntaje BAJO, con una sola regla en lib/email.js (rankEmail +
// claseDeEmail), que usan el worker y la extensión:
//   · 45 en el dominio propio, lo mismo que informatique@ (IT_INFRA): debajo de info@ (55) y de
//     cualquier rol comercial, editorial o persona; >= 0 para que la auditoría y el pulido lo dejen.
//   · clase "generico": no se preselecciona ni se elige antes que info@ ni que un comercial.
//   · de otro dominio o en un webmail, las reglas generales (webmaster@gmail.com vetado como info@gmail.com).
// office@, oficina@, secretaria@, secretariat@, reception@ y sistemas@ siguen vetados (otra decisión pendiente).
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as acorn from "acorn";
import { cargarWorker } from "./_worker-exportado.mjs";
import * as E from "../lib/email.js";
import { isGarbageEmail } from "../../modules/emailVerifier.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const popup = fs.readFileSync(path.join(aqui, "..", "..", "popup", "popup.js"), "utf8");
const indexJs = fs.readFileSync(path.join(aqui, "..", "index.js"), "utf8");
const D = "diario-ejemplo.com";
const AJENO = "otro-grupo.com";
const W = `webmaster@${D}`;
const INFO = `info@${D}`;
const VARIANTES = ["webmaster1", "webmaster2", "webmaster.diario", "webmaster-es", "webmaster_es"];
const SIGUEN_VETADOS = ["office", "oficina", "secretaria", "secretariat", "reception", "sistemas"];

// El worker entero, una sola vez, sin red.
let _w = null;
const worker = () => {
  globalThis.__fetchFalso ||= async () => { throw new Error("sin red en este test"); };
  return (_w ||= cargarWorker(["_pickTier", "rankEmail", "_planAuditoriaLead"], { fetchFalso: true }));
};

// Las funciones del popup con su código EXACTO (acorn) y las dependencias reales de lib/email.js, igual
// que tests/ranking_extension-13-09b.test.js.
const _arbolPopup = acorn.parse(popup, { ecmaVersion: "latest", sourceType: "module" });
function fuenteDe(nombre) {
  const n = _arbolPopup.body.find(s => s.type === "FunctionDeclaration" && s.id?.name === nombre);
  ok(n, `popup.js no tiene la función top-level ${nombre}`);
  return popup.slice(n.start, n.end);
}
function rankingDelPopup(state = { domain: "", category: "", emailSources: new Map(), accessToken: "" }) {
  const nombres = ["_isGenericEmailLocal", "_ctxEmailsAnalisis", "_ctxEmailsProspecto", "_fuenteTextoClient", "_rankClient", "_motivoReboteClient",
                   "_emailPickTierClient", "_ordenarEmailsClient", "_elegirPreseleccionClient", "_bestEmailByTier"];
  const codigo = nombres.map(fuenteDe).join("\n");
  const rebotes = { cache: { set: new Set(), ts: 0 }, porDominio: new Map() };
  const fabrica = new Function("state", "rankEmail", "vetoDuroEmail", "claseDeEmail", "esRegistranteWebmail", "motivoRebote", "_isGenericLocalPart", "AD_SALES_LOCAL", "_rebotesExtension",
    "tierDeEmail", "compararCandidatosEmail", "_ordenTiersExtension",
    `const _AD_SALES_LOCAL_RE = AD_SALES_LOCAL;\n${codigo}\nreturn { ${nombres.join(", ")} };`);
  return fabrica(state, E.rankEmail, E.vetoDuroEmail, E.claseDeEmail, E.esRegistranteWebmail, E.motivoRebote, E._isGenericLocalPart, E.AD_SALES_LOCAL, rebotes,
                 E.tierDeEmail, E.compararCandidatosEmail, { orden: null });
}

// ── El puntaje ──────────────────────────────────────────────────────────────────────────────────
test("webmaster@ del sitio es un contacto válido de puntaje bajo: > 0, debajo de info@ y de todo rol o persona", () => {
  strictEqual(E.vetoDuroEmail(W, D), "", "webmaster@ ya no es veto duro");
  const s = E.rankEmail(W, D, "");
  strictEqual(s, 45, "el mismo puntaje que informatique@ (IT_INFRA)");
  strictEqual(s, E.rankEmail(`informatique@${D}`, D, ""));
  ok(s > 0, "> 0: el pulido descarta lo que puntúa <= 0");
  ok(s < E.rankEmail(INFO, D, ""), `webmaster@ (${s}) tiene que quedar debajo de info@ (55)`);
  for (const l of ["publicidad", "ventas", "comercial", "marketing", "sales", "redaccion", "prensa", "editor", "redazione", "redaktion",
                   "juan.perez", "jperez", "guillermo", "director", "gerente", "ceo"]) {
    ok(s < E.rankEmail(`${l}@${D}`, D, ""), `webmaster@ (${s}) tiene que quedar debajo de ${l}@ (${E.rankEmail(`${l}@${D}`, D, "")})`);
  }
  // Las variantes que el código ya trataba como webmaster valen lo mismo.
  for (const l of VARIANTES) strictEqual(E.rankEmail(`${l}@${D}`, D, ""), 45, `${l}@`);
  // Un trozo comercial o editorial le sigue ganando al webmaster, como con eventos.comercial@.
  strictEqual(E.rankEmail(`webmaster.comercial@${D}`, D, ""), 135);
  strictEqual(E.rankEmail(`webmaster.redaccion@${D}`, D, ""), 115);
});

test("de otro dominio o en un webmail, webmaster@ sigue las reglas generales (como info@)", () => {
  strictEqual(E.vetoDuroEmail(`webmaster@gmail.com`, D), "freemail_generico", "webmaster@gmail.com se veta como info@gmail.com");
  strictEqual(E.rankEmail(`webmaster@gmail.com`, D, ""), -1);
  strictEqual(E.vetoDuroEmail(`webmaster@${AJENO}`, D), "", "de otra empresa no es veto: vale negativo, como info@ de otra empresa");
  ok(E.rankEmail(`webmaster@${AJENO}`, D, "") < 0 && E.rankEmail(`info@${AJENO}`, D, "") < 0);
  strictEqual(E.vetoDuroEmail(`webmaster.es@${D}`, D), "local_con_tld", "un local que termina en TLD sigue siendo artefacto de scrape");
});

// ── La clase ────────────────────────────────────────────────────────────────────────────────────
test("webmaster@ es 'generico' para el agente y la extensión, venga de donde venga", () => {
  for (const e of [W, ...VARIANTES.map(l => `${l}@${D}`)]) {
    strictEqual(E.claseDeEmail(e), "generico", e);
    strictEqual(E.esDecisor(e), false, `${e} no cuenta como decisor: el lead sigue buscando a alguien mejor`);
    for (const src of ["scrape", "apollo", "Facebook", "rol_mx", ""]) strictEqual(E._tipoDeEmailParaRanking(e, src), "generico", `${e} con fuente ${src}`);
    strictEqual(E.tierDeEmail(e, "scrape"), E.tierDeEmail(INFO, "scrape"), `${e} va en el mismo tier que info@`);
  }
});

// ── La extensión ────────────────────────────────────────────────────────────────────────────────
test("la extensión muestra webmaster@ del sitio (isGarbageEmail false) y sigue escondiendo webmaster@gmail.com", () => {
  strictEqual(isGarbageEmail(W, D), false, "webmaster@ del sitio tiene que verse como chip");
  for (const l of VARIANTES) strictEqual(isGarbageEmail(`${l}@${D}`, D), false, `${l}@`);
  strictEqual(isGarbageEmail(`webmaster@${AJENO}`, D), false, "de otro dominio se ve, como info@ de otro dominio");
  strictEqual(isGarbageEmail("webmaster@gmail.com", D), true, "webmaster@gmail.com es freemail genérico");
  // La extensión no tiene una lista propia que lo vuelva a esconder.
  const prefijos = fs.readFileSync(path.join(aqui, "..", "..", "modules", "emailVerifier.js"), "utf8");
  const bloque = prefijos.slice(prefijos.indexOf("const GARBAGE_LOCAL_PREFIXES"), prefijos.indexOf("const GARBAGE_DOMAIN_SUFFIXES"));
  ok(bloque.length > 0 && !/"webmaster"/.test(bloque), "GARBAGE_LOCAL_PREFIXES no puede volver a tener webmaster");
});

test("la extensión no preselecciona webmaster@ sobre info@ ni sobre un comercial, y lo deja puesto si es lo único", () => {
  const p = rankingDelPopup();
  const tarjeta = (emails) => {
    const ctx = p._ctxEmailsProspecto({ domain: D, category: "", email_sources: Object.fromEntries(emails.map(e => [e, "scrape"])) });
    const orden = p._ordenarEmailsClient(emails, ctx);
    return { orden, puesto: p._elegirPreseleccionClient(orden, ctx) };
  };
  for (const [otro, gana] of [[INFO, INFO], [`ventas@${D}`, `ventas@${D}`], [`publicidad@${D}`, `publicidad@${D}`], [`juan.perez@${D}`, `juan.perez@${D}`],
                              [`redaccion@${D}`, `redaccion@${D}`], [`office@${D}`, W], [`secretaria@${D}`, W]]) {
    for (const lista of [[W, otro], [otro, W]]) {
      const { orden, puesto } = tarjeta(lista);
      strictEqual(orden[0], gana, `tarjeta con ${lista.join(", ")}: ordenó ${orden.join(", ")}`);
      strictEqual(puesto, gana, `tarjeta con ${lista.join(", ")}: dejó puesto ${puesto}`);
    }
  }
  strictEqual(tarjeta([W]).puesto, W, "si es la única dirección, webmaster@ queda puesto (ya no es tier -1)");
  deepStrictEqual(tarjeta([W, INFO, `ventas@${D}`, `juan.perez@${D}`]).orden, [`ventas@${D}`, `juan.perez@${D}`, INFO, W],
    "rol comercial > persona > info@ > webmaster@");
  // Análisis (fuente "Page") elige lo mismo que la tarjeta.
  const analisis = (emails) => rankingDelPopup({ domain: D, category: "", emailSources: new Map(emails.map(e => [e, "Page"])), accessToken: "" })._bestEmailByTier(emails);
  strictEqual(analisis([W, INFO]), INFO);
  strictEqual(analisis([W, `ventas@${D}`]), `ventas@${D}`);
  strictEqual(analisis([`office@${D}`, W]), W);
});

// ── El agente ───────────────────────────────────────────────────────────────────────────────────
test("el agente le escribe a webmaster@ solo si no hay nada mejor", async () => {
  const w = await worker();
  // runAgentCycle: descarta lo que puntúa < 0 y ordena con compararCandidatosEmail y el tier de _pickTier.
  ok(/\.sort\(\(a, b\) => compararCandidatosEmail\(a, b, \{ sourceRank: SOURCE_RANK, orden: _tierOrdenCache\.orden \}\)\)/.test(indexJs), "el agente ordena con el comparador compartido");
  const elige = (emails, src) => emails.map(e => ({ email: e, source: src, score: w.rankEmail(e, D, ""), tier: w._pickTier(e, src) }))
    .filter(c => c.score >= 0).sort((a, b) => E.compararCandidatosEmail(a, b)).map(c => c.email);
  for (const src of ["scrape", "apollo", "Facebook", "rol_mx"]) {
    strictEqual(w.rankEmail(W, D, ""), 45, "el worker usa la misma regla");
    for (const otro of [INFO, `ventas@${D}`, `juan.perez@${D}`, `redaccion@${D}`]) {
      strictEqual(elige([W, otro], src)[0], otro, `con ${otro} (fuente ${src}) el agente no elige webmaster@`);
    }
    deepStrictEqual(elige([W], src), [W], `si es lo único (fuente ${src}), el agente le escribe`);
    deepStrictEqual(elige([`office@${D}`, W, `sistemas@${D}`], src), [W], `office@ y sistemas@ siguen fuera (fuente ${src})`);
  }
});

// ── La auditoría del pool, el pulido y la entrada ───────────────────────────────────────────────
test("la auditoría del pool no saca a webmaster@, y sí a office@", async () => {
  const w = await worker();
  const solo = w._planAuditoriaLead({ id: 1, domain: D, status: "pending", emails: [W], email_sources: { [W]: "scrape" } });
  deepStrictEqual(solo.malos, [], "webmaster@ no es basura");
  strictEqual(solo.plan, null, "un lead con sólo webmaster@ no se toca");

  const conInfo = w._planAuditoriaLead({ id: 2, domain: D, status: "validated", emails: [W, INFO], email_sources: {} });
  deepStrictEqual(conInfo.malos, []);
  deepStrictEqual(conInfo.plan?.buenos, [INFO, W], "se reordena: info@ primero, webmaster@ queda");

  const conOffice = w._planAuditoriaLead({ id: 3, domain: D, status: "pending", emails: [`office@${D}`, W], email_sources: {} });
  deepStrictEqual(conOffice.malos.map(m => [m.email, m.motivo]), [[`office@${D}`, "basura_o_departamento"]]);
  deepStrictEqual(conOffice.plan?.buenos, [W]);
  strictEqual(conOffice.plan?.vaciaria, false, "el lead no se queda sin email");

  const cola = w._planAuditoriaLead({ id: 4, domain: D, status: "por_enviar", emails: [W], email_sources: {}, monday_payload: { email: W } });
  strictEqual(cola.aviso, null, "una fila por enviar a webmaster@ no se avisa como muerta");
});

test("el pulido y la entrada al pool dejan a webmaster@ (score > 0 y >= 0)", () => {
  ok(/const ranked = _todos\.filter\(r => r\.score > 0 /.test(indexJs), "el pulido guarda lo que puntúa > 0 (rechazados_por_ranking es <= 0)");
  ok(/\.map\(e => \(\{ e, s: rankEmail\(e, domain, category \|\| ""\) \}\)\)\s*\.filter\(x => x\.s >= 0\)/.test(indexJs), "la entrada al pool guarda lo que puntúa >= 0");
  ok(E.rankEmail(W, D, "") > 0);
  strictEqual(E.GARBAGE_LOCAL.test(W), false, "validateEmailsBatch y scoreEmail (GARBAGE_LOCAL) ya no lo descartan");
  strictEqual(E.classifyEmail(W, D).verdict, "low_quality", "classifyEmail lo trata como un genérico, igual que info@");
});

// ── office@ y compañía: sin cambios ─────────────────────────────────────────────────────────────
test("office@, oficina@, secretaria@, secretariat@, reception@ y sistemas@ siguen vetados en el worker y en la extensión", () => {
  for (const l of SIGUEN_VETADOS) {
    const e = `${l}@${D}`;
    strictEqual(E.vetoDuroEmail(e, D), "basura", `${l}@ sigue siendo veto duro`);
    strictEqual(E.rankEmail(e, D, ""), -1, `${l}@`);
    strictEqual(isGarbageEmail(e, D), true, `${l}@ sigue escondido en la extensión`);
    strictEqual(E.GARBAGE_LOCAL.test(e), true, `${l}@ sigue en la lista de basura`);
  }
});

// ── Antes / después sobre una batería amplia ────────────────────────────────────────────────────
// Medido con lib/email.js y modules/emailVerifier.js en 0268101 (v705), antes del cambio, sobre
// diario-ejemplo.com. Columnas: [local, puntaje en el dominio propio, puntaje @otro-grupo.com,
// puntaje @gmail.com, clase (g=generico, p=persona, r=rol), 1 si la extensión lo escondía en el sitio].
// Todo esto tiene que seguir IGUAL: el único cambio buscado es la familia webmaster de más abajo.
// Quedan fijadas a propósito webmasters@, web.master@, webmasterteam@ y webmasteres@: no son las
// variantes de webmaster que el código ya reconocía (separador o número) y no cambian.
const ANTES_SIN_CAMBIO = {
  es: [
    ["info", 55, -35, -1, "g", 0], ["contacto", 55, -35, -1, "g", 0], ["hola", 55, -35, -1, "g", 0], ["general", 55, -35, -1, "g", 0],
    ["publicidad", 135, 45, -1, "r", 0], ["ventas", 135, 45, -1, "r", 0], ["comercial", 135, 45, -1, "r", 0], ["marketing", 135, 45, -1, "r", 0],
    ["redaccion", 115, 25, 85, "g", 0], ["prensa", 115, 25, -1, "g", 0], ["editor", 115, 25, 85, "g", 0], ["director", 120, 30, 90, "p", 0],
    ["gerente", 120, 30, 90, "p", 0], ["juan.perez", 110, 20, 80, "p", 0], ["jperez", 95, 5, 65, "p", 0], ["guillermo", 95, 5, 65, "p", 0],
    ["admin", 25, -65, -1, "g", 0], ["soporte", 20, -70, -1, "g", 0], ["atencion", 20, -70, -55, "g", 0], ["denuncias", 20, -70, -55, "g", 0],
    ["suscripciones", 20, -70, -55, "g", 0], ["noticias", 30, -60, -45, "g", 0], ["finanzas", 30, -60, -45, "g", 0], ["contabilidad", 30, -60, -45, "g", 0],
    ["administracion", 30, -60, -45, "g", 0], ["rrhh", 48, -42, -27, "g", 0], ["empleos", 48, -42, -27, "g", 0], ["trabajo", 48, -42, -27, "g", 0],
    ["sistemas", -1, -1, -1, "g", 1], ["secretaria", -1, -1, -1, "p", 1], ["oficina", -1, -1, -1, "p", 1], ["recepcion", 55, -35, -1, "g", 0],
    ["tecnico", 45, -45, -30, "g", 0], ["informatica", -10, -100, -85, "g", 0], ["webmaestro", 95, 5, 65, "p", 0], ["legal", -1, -1, -1, "g", 1],
    ["privacidad", -1, -1, -1, "g", 1], ["compras", 48, 5, 65, "g", 0], ["eventos", 48, 5, 65, "g", 0], ["tienda", 48, 5, 65, "g", 0],
    ["cartas", 48, 5, 65, "g", 0], ["lectores", 48, 5, 65, "g", 0],
  ],
  pt: [
    ["contato", 55, -35, -1, "g", 0], ["publicidade", 135, 45, -1, "r", 0], ["vendas", 135, 45, 105, "r", 0], ["redacao", 115, 25, 85, "g", 0],
    ["imprensa", 115, 25, -1, "g", 0], ["atendimento", 20, -70, -1, "g", 0], ["suporte", 20, -70, -1, "g", 0], ["ajuda", 20, -70, -55, "g", 0],
    ["ouvidoria", -25, -115, -100, "g", 0], ["assinaturas", 20, -70, -55, "g", 0], ["financeiro", 30, -60, -45, "g", 0], ["rh", -30, -120, -105, "g", 0],
    ["recrutamento", 48, -42, -27, "g", 0], ["emprego", 48, -42, -27, "g", 0], ["recepcao", 55, -35, -1, "g", 0], ["geral", 55, -35, -1, "g", 0],
    ["faleconosco", 95, 5, 65, "p", 0],
  ],
  it: [
    ["redazione", 115, 25, 85, "g", 0], ["pubblicita", 135, 45, -1, "r", 0], ["commerciale", 135, 45, 105, "r", 0], ["stampa", 115, 25, -1, "g", 0],
    ["amministrazione", 95, 5, 65, "p", 0], ["segreteria", 95, 5, 65, "p", 0], ["contatti", 95, 5, 65, "p", 0], ["lavoro", 48, -42, -27, "g", 0],
    ["ufficio", 95, 5, 65, "p", 0], ["direzione", 95, 5, 65, "p", 0], ["ufficiostampa", 95, 5, 65, "p", 0],
  ],
  fr: [
    ["contact", 55, -35, -1, "g", 0], ["redaction", 115, 25, 85, "g", 0], ["presse", 115, 25, -1, "g", 0], ["publicite", 135, 45, -1, "r", 0],
    ["regie", 135, 45, 105, "r", 0], ["service.client", 20, -70, -55, "g", 0], ["secretariat", -1, -1, -1, "p", 1], ["accueil", 95, 5, 65, "p", 0],
    ["recrutement", 48, -42, -27, "g", 0], ["emploi", 48, -42, -27, "g", 0], ["informatique", 45, -45, -30, "g", 0], ["webmestre", 95, 5, 65, "p", 0],
    ["direction", 95, 5, 65, "p", 0], ["abonnements", 20, -70, -55, "g", 0],
  ],
  de: [
    ["kontakt", 55, -35, -1, "g", 0], ["redaktion", 115, 25, 85, "g", 0], ["werbung", 135, 45, -1, "r", 0], ["anzeigen", 135, 45, 105, "r", 0],
    ["vertrieb", 135, 45, 105, "r", 0], ["verkauf", 135, 45, 105, "r", 0], ["sekretariat", 95, 5, 65, "p", 0], ["empfang", 95, 5, 65, "p", 0],
    ["buero", 95, 5, 65, "p", 0], ["datenschutz", -1, -1, -1, "p", 1], ["impressum", 95, 5, 65, "p", 0], ["karriere", 48, -42, -27, "g", 0],
    ["technik", -10, -100, -85, "g", 0], ["edv", -25, -115, -100, "g", 0], ["geschaeftsfuehrer", 130, 65, 100, "p", 0], ["verlag", 95, 5, 65, "p", 0],
  ],
  pl: [
    ["redakcja", 115, 25, 85, "g", 0], ["reklama", 135, 45, 105, "r", 0], ["biuro", 95, 5, 65, "p", 0], ["reklamacje", -25, -115, -100, "g", 0],
    ["kadry", 48, -42, -27, "g", 0], ["praca", 48, -42, -27, "g", 0], ["wsparcie", 45, -45, -30, "g", 0], ["prenumerata", 95, 5, 65, "p", 0],
    ["informatyka", 95, 5, 65, "p", 0],
  ],
  en: [
    ["office", -1, -1, -1, "p", 1], ["reception", -1, -1, -1, "g", 1], ["support", 20, -70, -1, "g", 0], ["hello", 55, -35, -1, "g", 0],
    ["sales", 135, 45, -1, "r", 0], ["ads", 135, 45, 105, "r", 0], ["advertising", 135, 45, 105, "r", 0], ["press", 115, 25, -1, "g", 0],
    ["newsroom", 115, 25, 85, "g", 0], ["careers", -1, -1, -1, "g", 1], ["jobs", -1, -1, -1, "g", 1], ["postmaster", -1, -1, -1, "g", 1],
    ["hostmaster", -1, -1, -1, "g", 1], ["abuse", -1, -1, -1, "g", 1], ["noreply", -1, -1, -1, "g", 1], ["root", -1, -1, -1, "p", 1],
    ["it", -25, -115, -100, "g", 0], ["helpdesk", -25, -115, -100, "g", 0], ["security", 0, -90, -75, "g", 0], ["team", 70, -20, 40, "p", 0],
    ["editorial", 115, 25, 85, "g", 0], ["partnerships", 120, 30, 90, "p", 0],
  ],
  "no-son-webmaster": [
    ["webmasters", 95, 5, 65, "p", 0], ["juan.webmaster", 45, -45, -30, "g", 0], ["web.master", 110, 20, 80, "p", 0], ["web-master", 110, 20, 80, "p", 0],
    ["webmasterteam", 95, 5, 65, "p", 0], ["webmasteres", 95, 5, 65, "p", 0],
  ],
};

// La familia webmaster: [local, antes (0268101), ahora]. Mismas columnas sin el local.
//   webmaster@            veto duro (-1, escondido)                 → 45, generico, visible
//   webmaster1/2/123@     40 "persona" en el worker, escondido      → 45, generico, visible
//   webmaster.diario@ y webmaster-es@ / _es@ / .soporte@: 112 "persona" (arriba de info@) en el worker
//                         y escondido en la extensión               → 45, generico, visible
//   webmaster.comercial@ / .redaccion@: el worker ya las puntuaba por el trozo comercial/editorial; sólo
//                         cambia que la extensión ahora las muestra, como el worker las usa.
const WEBMASTER_ANTES_AHORA = [
  ["webmaster",           [-1, -1, -1, "g", 1],     [45, -45, -1, "g", 0]],
  ["webmaster1",          [40, -50, -35, "p", 1],   [45, -45, -30, "g", 0]],
  ["webmaster2",          [40, -50, -35, "p", 1],   [45, -45, -30, "g", 0]],
  ["webmaster123",        [40, -50, -35, "p", 1],   [45, -45, -30, "g", 0]],
  ["webmaster.diario",    [112, 22, 37, "p", 1],    [45, -45, -30, "g", 0]],
  ["webmaster-es",        [112, 22, 37, "p", 1],    [45, -45, -30, "g", 0]],
  ["webmaster_es",        [112, 22, 37, "p", 1],    [45, -45, -30, "g", 0]],
  ["webmaster.soporte",   [112, 22, 37, "p", 1],    [45, -45, -30, "g", 0]],
  ["webmaster.comercial", [135, 45, 105, "p", 1],   [135, 45, 105, "p", 0]],
  ["webmaster.redaccion", [115, 25, 85, "g", 1],    [115, 25, 85, "g", 0]],
];

const CLASE = { generico: "g", persona: "p", rol: "r" };
const medir = (l) => [E.rankEmail(`${l}@${D}`, D, ""), E.rankEmail(`${l}@${AJENO}`, D, ""), E.rankEmail(`${l}@gmail.com`, D, ""),
                      CLASE[E.claseDeEmail(`${l}@${D}`)], isGarbageEmail(`${l}@${D}`, D) ? 1 : 0];

test("antes/después (es/pt/it/fr/de/pl/en): sólo cambió la familia webmaster", () => {
  const distintos = [];
  let n = 0;
  for (const [idioma, filas] of Object.entries(ANTES_SIN_CAMBIO)) {
    for (const [l, ...antes] of filas) {
      n++;
      const ahora = medir(l);
      if (JSON.stringify(ahora) !== JSON.stringify(antes)) distintos.push(`${idioma} ${l}@: antes ${JSON.stringify(antes)}, ahora ${JSON.stringify(ahora)}`);
    }
  }
  ok(n >= 130, `la batería tiene que ser amplia (hay ${n} locales)`);
  strictEqual(distintos.length, 0, distintos.join("\n"));
});

test("antes/después: la familia webmaster queda como decidió el dueño", () => {
  const distintos = [];
  for (const [l, antes, esperado] of WEBMASTER_ANTES_AHORA) {
    const ahora = medir(l);
    if (JSON.stringify(ahora) !== JSON.stringify(esperado)) distintos.push(`${l}@: esperaba ${JSON.stringify(esperado)}, dio ${JSON.stringify(ahora)} (antes ${JSON.stringify(antes)})`);
    ok(JSON.stringify(antes) !== JSON.stringify(esperado), `${l}@ figura en la familia que cambia pero no cambió nada`);
  }
  strictEqual(distintos.length, 0, distintos.join("\n"));
});
