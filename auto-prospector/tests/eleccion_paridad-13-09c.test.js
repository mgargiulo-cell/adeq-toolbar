// A quién se le escribe: la misma regla en el agente, el reintento, la tarjeta y Análisis. (2026-09-13, cierre)
//
// "Que Prospects muestre lo mismo que se manda, y lo que arreglás un día no lo rompas al otro." La revisión
// integrada del 13/09 encontró que cada pantalla y cada camino del worker elegían con su propia copia:
//   I12  La tarjeta y Análisis preseleccionaban otra dirección que el agente para el mismo lead: una
//        persona sin fuente era "genérica" en el agente, el desempate por fuente sólo existía en el worker
//        y el orden semanal de tipos no llegaba a la extensión.
//   I13  Dos copias del bucle de elección (agente y reintento) que ya elegían distinto al gastar las 3
//        consultas de MillionVerifier, y tres copias de la regex del registrante.
//   I14  El aviso de la auditoría para la cola "Por enviar" miraba emails[0] y nadie lo mostraba.
//   I24  Con direcciones pero ninguna preseleccionable, la tarjeta decía "el sistema todavía está buscando".
//   I27  "No pude verificar" (tope, error, timeout de MV) era "riesgo", igual que un catch-all, y el
//        reintento lo mandaba al dominio que acababa de rebotar.
//   Extras  "Hay decisor" se calculaba con la lista de genéricos; reklamacje@ (reclamos) era rol comercial;
//        compras@/proveedores@/lectores@/cartas@ y recrutement@/kadry@ valían como personas; prensa@ y
//        redaccion@ (el mismo rol) caían en clases distintas; pr@ y geschaeftsfuehrer@ sin rol.
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
import { emailDeCola, avisoDeCola } from "../../modules/colaEstado.js";
import { isGarbageEmail } from "../../modules/emailVerifier.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const popup = fs.readFileSync(path.join(aqui, "..", "..", "popup", "popup.js"), "utf8");
const indexJs = fs.readFileSync(path.join(aqui, "..", "index.js"), "utf8");
const DT = "diario.com.ar";
const resp = (body, { status = 200 } = {}) => ({ ok: status >= 200 && status < 300, status, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) });
const cuerpoWorker = (firma) => {
  const i = indexJs.indexOf(firma);
  ok(i >= 0, `no encontré ${firma}`);
  return indexJs.slice(i, indexJs.indexOf("\n}\n", i));
};

// ── Las funciones del popup, con su código EXACTO (acorn) y las dependencias reales de lib/email.js ──
const _arbolPopup = acorn.parse(popup, { ecmaVersion: "latest", sourceType: "module" });
function fuenteDe(nombre) {
  const n = _arbolPopup.body.find(s => s.type === "FunctionDeclaration" && s.id?.name === nombre);
  ok(n, `popup.js no tiene la función top-level ${nombre}`);
  return popup.slice(n.start, n.end);
}
function rankingDelPopup(state = { domain: "", category: "", emailSources: new Map(), accessToken: "" }) {
  const nombres = ["_ctxEmailsAnalisis", "_ctxEmailsProspecto", "_fuenteTextoClient", "_rankClient", "_motivoReboteClient",
                   "_emailPickTierClient", "_ordenarEmailsClient", "_elegirPreseleccionClient", "_bestEmailByTier"];
  const codigo = nombres.map(fuenteDe).join("\n");
  const rebotes = { cache: { set: new Set(), ts: 0 }, porDominio: new Map() };
  const ordenTiers = { orden: null };
  const fabrica = new Function("state", "rankEmail", "vetoDuroEmail", "esRegistranteWebmail", "motivoRebote", "_rebotesExtension",
    "tierDeEmail", "compararCandidatosEmail", "_ordenTiersExtension", `${codigo}\nreturn { ${nombres.join(", ")} };`);
  return { ...fabrica(state, E.rankEmail, E.vetoDuroEmail, E.esRegistranteWebmail, E.motivoRebote, rebotes, E.tierDeEmail, E.compararCandidatosEmail, ordenTiers), rebotes, ordenTiers };
}

let _w = null;
const worker = () => (_w ||= cargarWorker(["_pickTier", "rankEmail", "_decidirCandidato", "_elegirDireccion", "_elegirEnviable", "_accionPorVeredictoMV",
                                           "_tenemosContactoBueno", "_esMejoraDeContacto", "_fuentesDelEnriquecimiento", "_planAuditoriaLead",
                                           "_emailQueMandaLaCola"]));

// ════════════════════════════════════════════════════════════════════════════════════════════════
// PUNTAJES Y CLASES: TABLA ANTES / DESPUÉS
// ════════════════════════════════════════════════════════════════════════════════════════════════
// "antes" medido con lib/email.js de a99f382 (el main integrado), dominio propio diario.com.ar, sin
// categoría. Cada fila dice por qué cambió. Todo lo que no está en la tabla no se movió (control abajo).
const TABLA = [
  // local                puntaje antes → después   clase antes → después   por qué
  // reklam(?!ac|ati): reklamacje es "reclamos" en polaco; salía como buzón de venta de pauta por el prefijo.
  ["reklamacje",          135, -25, "rol",     "generico", "reclamos (pl): mesa de reclamos, como ouvidoria@ (-25)"],
  ["reklamacja",          135, -25, "rol",     "generico", "reclamo (pl)"],
  ["reklamace",           135, -25, "rol",     "generico", "reclamos (cz)"],
  ["reklamacie",          135, -25, "rol",     "generico", "reclamos (sk)"],
  ["reklamation",         135, -25, "rol",     "generico", "reclamo (de)"],
  // RRHH / empleo en otros idiomas: el mismo DEPARTMENT que rrhh@/empleos@ (48).
  ["recrutement",          95,  48, "persona", "generico", "reclutamiento (fr)"],
  ["emploi",               95,  48, "persona", "generico", "empleo (fr)"],
  ["emplois",              95,  48, "persona", "generico", "empleos (fr)"],
  ["kadry",                95,  48, "persona", "generico", "RRHH (pl)"],
  ["praca",                95,  48, "persona", "generico", "trabajo (pl)"],
  ["rekrutacja",           95,  48, "persona", "generico", "reclutamiento (pl)"],
  ["recrutamento",         95,  48, "persona", "generico", "reclutamiento (pt)"],
  ["emprego",              95,  48, "persona", "generico", "empleo (pt)"],
  ["empregos",             95,  48, "persona", "generico", "empleos (pt)"],
  ["reclutamiento",        95,  48, "persona", "generico", "reclutamiento (es)"],
  ["karriere",             95,  48, "persona", "generico", "carrera/empleos (de)"],
  ["lavoro",               95,  48, "persona", "generico", "trabajo (it)"],
  // Buzones funcionales: el área de compras, proveedores o cartas de lectores, como tienda@/eventos@ (48).
  ["compras",              95,  48, "persona", "generico", "compras (es/pt)"],
  ["proveedores",          95,  48, "persona", "generico", "proveedores (es)"],
  ["lectores",             95,  48, "persona", "generico", "cartas de lectores (es)"],
  ["cartas",               95,  48, "persona", "generico", "cartas de lectores (es/pt)"],
  ["fornecedores",         95,  48, "persona", "generico", "proveedores (pt)"],
  ["leitores",             95,  48, "persona", "generico", "lectores (pt)"],
  ["acquisti",             95,  48, "persona", "generico", "compras (it)"],
  ["fornitori",            95,  48, "persona", "generico", "proveedores (it)"],
  ["lettori",              95,  48, "persona", "generico", "lectores (it)"],
  ["achats",               95,  48, "persona", "generico", "compras (fr)"],
  ["fournisseurs",         95,  48, "persona", "generico", "proveedores (fr)"],
  ["lecteurs",             95,  48, "persona", "generico", "lectores (fr)"],
  ["einkauf",              95,  48, "persona", "generico", "compras (de)"],
  ["lieferanten",          95,  48, "persona", "generico", "proveedores (de)"],
  ["leserbriefe",          95,  48, "persona", "generico", "cartas de lectores (de)"],
  ["zakupy",               95,  48, "persona", "generico", "compras (pl)"],
  ["dostawcy",             95,  48, "persona", "generico", "proveedores (pl)"],
  ["czytelnicy",           95,  48, "persona", "generico", "lectores (pl)"],
  ["purchasing",           95,  48, "persona", "generico", "compras (en)"],
  ["procurement",          95,  48, "persona", "generico", "compras (en)"],
  ["suppliers",            95,  48, "persona", "generico", "proveedores (en)"],
  // EXEC: el gerente general alemán valía 40 (sin rol) siendo "persona": arriba de info@ por tier y debajo por puntaje.
  ["geschaeftsfuehrer",    40, 130, "persona", "persona",  "gerente general (de) = EXEC, como ceo@ (130)"],
  ["geschaftsfuhrer",      95, 130, "persona", "persona",  "la misma palabra sin diéresis"],
  // Una sola clase para el rol EDITORIAL: la de prensa@/presse@/stampa@ (lista de genéricos). Puntaje igual.
  ["redaccion",           115, 115, "persona", "generico", "EDITORIAL, como prensa@ (115, generico)"],
  ["redazione",           115, 115, "persona", "generico", "EDITORIAL (it), como stampa@"],
  ["redaktion",           115, 115, "persona", "generico", "EDITORIAL (de), como presse@"],
  ["redaction",           115, 115, "persona", "generico", "EDITORIAL (fr), como presse@"],
  ["redakcja",            115, 115, "persona", "generico", "EDITORIAL (pl)"],
  ["newsroom",            115, 115, "persona", "generico", "EDITORIAL (en), como press@"],
  ["editor",              115, 115, "persona", "generico", "EDITORIAL"],
  ["periodista",          115, 115, "persona", "generico", "EDITORIAL"],
  ["es.redaccion",        115, 115, "persona", "generico", "EDITORIAL con prefijo de idioma"],
  ["lat.press",           115, 115, "persona", "generico", "press@ con prefijo de región: ya era genérico sin el prefijo"],
  // pr@ / rp@: la sigla de prensa / relaciones públicas, sin rol en rankEmail y "persona" arriba de info@.
  ["pr",                   10,  10, "persona", "generico", "sigla del buzón de prensa: con los genéricos, debajo de info@ (55)"],
  ["rp",                   10,  10, "persona", "generico", "relaciones públicas"],
];

test("tabla antes/después: cada cambio de puntaje o de clase está acá, medido y explicado", () => {
  const mal = [];
  for (const [l, antes, despues, claseAntes, claseDespues, porque] of TABLA) {
    ok(porque && (antes !== despues || claseAntes !== claseDespues), `${l}@: una fila sin cambio o sin motivo no va en la tabla`);
    const e = `${l}@${DT}`;
    const p = E.rankEmail(e, DT, ""), c = E.claseDeEmail(e);
    if (p !== despues || c !== claseDespues) mal.push(`${l}@: esperaba ${despues}/${claseDespues}, da ${p}/${c} (antes ${antes}/${claseAntes})`);
  }
  strictEqual(mal.length, 0, mal.join("\n"));
});

test("control: lo que no está en la tabla no se movió (puntaje y clase de a99f382)", () => {
  const CONTROL = [
    ["publicidad", 140, "rol"], ["ventas", 140, "rol"], ["reklama", 135, "rol"], ["reklamy", 135, "rol"], ["reklamowa", 135, "rol"], ["mkt", 135, "rol"],
    ["juan.perez", 110, "persona"], ["maria", 95, "persona"], ["krzysztof", 95, "persona"], ["gerente", 120, "persona"], ["com", 70, "persona"],
    ["jan.kadry", 110, "persona"], ["k.praca", 110, "persona"],
    ["info", 55, "generico"], ["contacto", 55, "generico"], ["prensa", 115, "generico"], ["presse", 115, "generico"], ["press", 115, "generico"],
    ["rrhh", 48, "generico"], ["empleos", 48, "generico"], ["tienda", 48, "generico"], ["eventos", 48, "generico"], ["soporte", 20, "generico"],
    ["ouvidoria", -25, "generico"], ["informatique", 45, "generico"], ["tld", 0, "generico"],
    ["eventos.comercial", 140, "persona"], ["compras.publicidad", 140, "persona"],
  ];
  const mal = CONTROL.filter(([l, p, c]) => E.rankEmail(`${l}@${DT}`, DT, "") !== p || E.claseDeEmail(`${l}@${DT}`) !== c)
    .map(([l, p, c]) => `${l}@: esperaba ${p}/${c}, da ${E.rankEmail(`${l}@${DT}`, DT, "")}/${E.claseDeEmail(`${l}@${DT}`)}`);
  strictEqual(mal.length, 0, mal.join("\n"));
  for (const l of ["reklama", "reklamy", "reklamowa", "reklamodawca"]) ok(E.AD_SALES_LOCAL.test(l), `${l} sigue siendo publicidad`);
  for (const l of ["reklamacje", "reklamacja", "reklamace", "reklamation"]) ok(!E.AD_SALES_LOCAL.test(l), `${l} es reclamos, no publicidad`);
});

test("un buzón de compras o de cartas de lectores ya no es una 'mejora' frente a info@", async () => {
  const w = await worker();
  for (const l of ["compras", "proveedores", "lectores", "cartas"]) {
    const e = `${l}@${DT}`;
    strictEqual(w._esMejoraDeContacto(e, E.rankEmail(e, DT, ""), [`info@${DT}`], {}, DT), false, `${l}@ (${E.rankEmail(e, DT, "")}) no mejora a info@`);
  }
  ok(w._esMejoraDeContacto(`juan.perez@${DT}`, 110, [`info@${DT}`], {}, DT), "una persona sí");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// "HAY DECISOR": LA CLASE DEL AGENTE, NO LA LISTA DE GENÉRICOS
// ════════════════════════════════════════════════════════════════════════════════════════════════
test("esDecisor: rol comercial o persona; rrhh@, soporte@, informatique@, redaccion@ o reklamacje@ no", () => {
  for (const l of ["publicidad", "juan.perez", "maria", "ceo", "geschaeftsfuehrer", "gerente"]) strictEqual(E.esDecisor(`${l}@${DT}`), true, `${l}@`);
  for (const l of ["info", "rrhh", "soporte", "soporte.web", "informatique", "redaccion", "prensa", "reklamacje", "compras", "kadry", "pr", "download"]) {
    strictEqual(E.esDecisor(`${l}@${DT}`), false, `${l}@`);
    strictEqual(E._isGenericLocalPart(`${l}@${DT}`) && !E.esDecisor(`${l}@${DT}`), E._isGenericLocalPart(`${l}@${DT}`), "todo genérico de la lista sigue sin ser decisor");
  }
});

test("el rastreo del sitio no para en un rrhh@ o un redaccion@: sólo un decisor corta la búsqueda", async () => {
  const w = await worker();
  for (const l of ["rrhh", "informatique", "redaccion", "soporte.web", "compras"]) strictEqual(w._tenemosContactoBueno([`${l}@${DT}`], DT), false, `${l}@ cortaba el rastreo`);
  for (const l of ["juan.perez", "publicidad"]) strictEqual(w._tenemosContactoBueno([`${l}@${DT}`], DT), true, `${l}@ sí corta`);
  strictEqual(w._tenemosContactoBueno(["juanperez@gmail.com"], DT), false, "un gmail suelto sigue sin cortar");
});

test("ningún clasificador de 'hay decisor' usa la lista de genéricos (worker y extensión)", () => {
  const arbol = acorn.parse(indexJs, { ecmaVersion: "latest", sourceType: "module" });
  const nombreTop = (pos) => {
    const n = arbol.body.find(s => s.start <= pos && pos < s.end);
    if (!n) return "?";
    if (n.type === "FunctionDeclaration") return n.id?.name || "?";
    if (n.type === "VariableDeclaration") return n.declarations[0]?.id?.name || "?";
    return n.type;
  };
  const usos = new Set();
  const visitar = (nodo) => {
    if (!nodo || typeof nodo.type !== "string") return;
    if (nodo.type === "CallExpression" && nodo.callee?.type === "Identifier" && nodo.callee.name === "_isGenericLocalPart") usos.add(nombreTop(nodo.start));
    for (const v of Object.values(nodo)) {
      if (Array.isArray(v)) v.forEach(visitar);
      else if (v && typeof v === "object" && typeof v.type === "string") visitar(v);
    }
  };
  visitar(arbol);
  // Permitidos, y por qué:
  //   _esEmailDeUltima   el criterio del MB "email de última" (genérico no comercial), no "hay decisor".
  //   apolloQuemarCiclo  su filtro "sin persona" es la misma clase de regla, pero es zona del grupo que
  //                      toca Apollo (apollo_marcas): queda anotado como pendiente para que use esDecisor.
  const PERMITIDOS = new Set(["_esEmailDeUltima", "apolloQuemarCiclo"]);
  const fuera = [...usos].filter(n => !PERMITIDOS.has(n));
  deepStrictEqual(fuera, [], `usan la lista de genéricos para decidir: ${fuera.join(", ")}`);
  for (const [trozo, nombre] of [["return esDecisor(e);", "hasGood de polishPool"], ["curUtiles.every(e => !esDecisor(e))\n          && (!foundEmail || !esDecisor(foundEmail))", "_soloGenericos (unlock de Apollo)"],
                                 ["curUtiles.every(e => !esDecisor(e))) && String(cfg.polish_serper_personas", "búsqueda de personas en Google"],
                                 ["const infAutoNonGeneric = infResAuto.emails.find(e => esDecisor(e));", "Informer del autopilot"],
                                 ["if (!esDecisor(e)) continue;", "_tenemosContactoBueno"]]) {
    ok(indexJs.includes(trozo), `${nombre} no usa esDecisor`);
  }
  ok(/const haveDM = \(state\.emails \|\| \[\]\)\.some\(e => esDecisor\(e\)\);/.test(popup), "el reveal automático de Análisis decide con esDecisor");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// I12. LA TARJETA, ANÁLISIS, EL AGENTE Y EL REINTENTO ELIGEN LA MISMA DIRECCIÓN
// ════════════════════════════════════════════════════════════════════════════════════════════════
const D = "diario.com";
// El camino de runAgentCycle, paso por paso: puntaje >= 0, sin el gmail del registrante, y el comparador.
const primeroDelAgente = (w, lista, fuentes) => lista
  .map(e => ({ email: e, source: typeof fuentes[e] === "string" ? fuentes[e] : (fuentes[e]?.source || ""), score: w.rankEmail(e, D, "") }))
  .filter(x => x.score >= 0)
  .filter(x => !E.esRegistranteWebmail(x.email, x.source))
  .sort((a, b) => E.compararCandidatosEmail(a, b, { sourceRank: E.SOURCE_RANK_DEFAULT, orden: null }))[0]?.email || "";
const primeroDeLaTarjeta = (p, lista, fuentes) => {
  const ctx = p._ctxEmailsProspecto({ domain: D, category: "", email_sources: fuentes });
  return p._elegirPreseleccionClient(p._ordenarEmailsClient(lista, ctx), ctx);
};

test("I12: una persona sin fuente (o con fuente 'generic') es persona en el agente, como en la extensión; informer no", () => {
  for (const f of ["", "generic", null, undefined, "cache_unknown"]) strictEqual(E._tipoDeEmailParaRanking(`juan.perez@${D}`, f), "persona", `fuente ${f}`);
  strictEqual(E._tipoDeEmailParaRanking(`juan.perez@${D}`, "informer"), "generico", "el WHOIS sigue abajo");
  strictEqual(E._tipoDeEmailParaRanking(`juan.perez@${D}`, { source: "apollo" }), "persona", "la fuente guardada como objeto");
  strictEqual(E._tipoDeEmailParaRanking(`info@${D}`, ""), "generico");
});

test("I12: los casos del auditor — el agente y la tarjeta eligen la misma dirección", async () => {
  const w = await worker();
  const p = rankingDelPopup();
  const casos = [
    ["A: info@ raspado + persona sin fuente", [`info@${D}`, `juan.perez@${D}`], { [`info@${D}`]: "scrape" }, `juan.perez@${D}`],
    ["A': info@ 'generic' + persona sin fuente", [`info@${D}`, `juan.perez@${D}`], { [`info@${D}`]: "generic" }, `juan.perez@${D}`],
    ["B: dos personas de 110, google_contact y scrape", [`ana.gomez@${D}`, `pedro.ruiz@${D}`], { [`ana.gomez@${D}`]: "google_contact", [`pedro.ruiz@${D}`]: "scrape" }, `pedro.ruiz@${D}`],
    ["C: persona raspada + persona sin fuente", [`maria@${D}`, `juan.perez@${D}`], { [`maria@${D}`]: "scrape" }, `maria@${D}`],
  ];
  for (const [nombre, lista, fuentes, esperado] of casos) {
    strictEqual(primeroDelAgente(w, lista, fuentes), esperado, `${nombre}: agente`);
    strictEqual(primeroDeLaTarjeta(p, lista, fuentes), esperado, `${nombre}: tarjeta`);
  }
});

test("I12: el PRIMER elegido coincide en listas mezcladas de 2 y 3 direcciones × fuentes (incluidas '' y 'generic')", async () => {
  const w = await worker();
  const p = rankingDelPopup();
  const emails = ["publicidad", "juan.perez", "maria", "info", "redaccion", "rrhh", "contacto", "ana.gomez"].map(l => `${l}@${D}`);
  const todas = ["", "generic", "scrape", "apollo", "google_contact", "informer", "manual", "Facebook"];
  const cortas = ["", "generic", "scrape", "apollo"];
  const distintos = [];
  let n = 0;
  const probar = (lista, fuentesLista) => {
    const fuentes = Object.fromEntries(lista.map((e, i) => [e, fuentesLista[i]]).filter(([, f]) => f !== ""));
    const a = primeroDelAgente(w, lista, fuentes), t = primeroDeLaTarjeta(p, lista, fuentes);
    n++;
    if (a !== t) distintos.push(`${lista.map((e, i) => `${e.split("@")[0]}(${fuentesLista[i]})`).join(", ")} → agente ${a}, tarjeta ${t}`);
  };
  for (let i = 0; i < emails.length; i++) for (let j = i + 1; j < emails.length; j++)
    for (const fi of todas) for (const fj of todas) probar([emails[i], emails[j]], [fi, fj]);
  for (let i = 0; i < emails.length; i++) for (let j = i + 1; j < emails.length; j++) for (let k = j + 1; k < emails.length; k++)
    for (const fi of cortas) for (const fj of cortas) for (const fk of cortas) probar([emails[i], emails[j], emails[k]], [fi, fj, fk]);
  ok(n > 5000, `esperaba la matriz entera, hubo ${n}`);
  strictEqual(distintos.length, 0, distintos.slice(0, 10).join("\n"));
  // Y el tier es el mismo número de los dos lados (no sólo el mismo primero).
  for (const e of emails) for (const f of todas) {
    const ctx = p._ctxEmailsProspecto({ domain: D, email_sources: { [e]: f } });
    strictEqual(p._emailPickTierClient(e, ctx), w._pickTier(e, f), `${e} (${f})`);
  }
});

test("I12: el orden semanal de tipos llega a la extensión y se aplica igual que en el agente", async () => {
  const w = await cargarWorker(["_pickTier", "_cargarTierOrden"]);
  const orden = ["generico", "persona", "rol", "apollo"];
  await w._cargarTierOrden({ email_tier_ranking: JSON.stringify({ orden, medido: {}, fecha: "2026-09-13" }) });
  const p = rankingDelPopup();
  p.ordenTiers.orden = orden;
  for (const l of ["publicidad", "juan.perez", "info", "redaccion"]) for (const f of ["scrape", "apollo", "manual", "informer", ""]) {
    const e = `${l}@${D}`;
    strictEqual(p._emailPickTierClient(e, p._ctxEmailsProspecto({ domain: D, email_sources: { [e]: f } })), w._pickTier(e, f), `${l}@ (${f}) con el orden ${orden.join(">")}`);
  }
  strictEqual(E.ordenDeTiersValido(["apollo", "rol", "persona"]), null, "tres tipos no es un orden");
  strictEqual(E.ordenDeTiersValido(["apollo", "rol", "persona", "persona"]), null, "repetido tampoco");
  deepStrictEqual(E.ordenDeTiersValido(orden), orden);
  const cargador = fuenteDe("_asegurarOrdenTiersExtension");
  ok(/key=eq\.email_tier_ranking/.test(cargador) && /AbortSignal\.timeout\(/.test(cargador) && /ordenDeTiersValido\(/.test(cargador), "la extensión lee el orden con reloj y con la misma validación");
  ok(/_asegurarOrdenTiersExtension\(_reordenarAnalisisTrasOrden\)/.test(fuenteDe("renderEmailList")), "Análisis lo pide y se reordena si cambia");
  ok(/_asegurarOrdenTiersExtension\(\(\) => \{ if \(card\.isConnected\) renderProspectEmailList\(\); \}\)/.test(popup), "la tarjeta también");
  ok(/const orden = ordenDeTiersValido\(g\?\.orden\);/.test(cuerpoWorker("async function _cargarTierOrden(")), "el worker valida igual");
});

// ── I12 (ronda final): el orden semanal llega a TODAS las tarjetas y cambia la preselección ─────────
// La revisión integrada lo reprodujo con el código real: la lectura del orden guardaba un solo aviso (el de
// la primera tarjeta; la página dibuja 50 de un tirón) y, aun en esa tarjeta, el redibujo conservaba la
// preselección AUTOMÁTICA hecha con el orden por defecto. El test de arriba ponía el orden a mano antes de
// ordenar y buscaba el aviso con una regex: no lo veía. Acá corre el código EXACTO de las dos listas
// (renderEmailList y la de la tarjeta) sobre un DOM mínimo, con la lectura en curso mientras se dibujan.
function _coincideSelector(el, sel) {
  const m = /^((?:\.[\w-]+)+)((?::not\(\.[\w-]+\))*)$/.exec(String(sel).trim());
  if (!m) throw new Error(`selector no soportado por el DOM de prueba: ${sel}`);
  const si = m[1].split(".").filter(Boolean), no = [...m[2].matchAll(/:not\(\.([\w-]+)\)/g)].map(x => x[1]);
  return si.every(c => el._clases.has(c)) && !no.some(c => el._clases.has(c));
}
function elementoFalso(clases = "", datos = {}) {
  const el = { isConnected: true, style: {}, value: "", textContent: "", title: "", dataset: { ...datos }, _hijos: [], _oyentes: {},
               _clases: new Set(String(clases).split(/\s+/).filter(Boolean)) };
  el.classList = { add: (...c) => c.forEach(x => el._clases.add(x)), remove: (...c) => c.forEach(x => el._clases.delete(x)), contains: (c) => el._clases.has(c) };
  el.addEventListener = (tipo, fn) => { (el._oyentes[tipo] ||= []).push(fn); };
  el.querySelectorAll = (sel) => el._hijos.filter(h => _coincideSelector(h, sel));
  el.querySelector = (sel) => el.querySelectorAll(sel)[0] || null;
  el.closest = (sel) => (_coincideSelector(el, sel) ? el : null);
  el.remove = () => {};
  Object.defineProperty(el, "className", { get: () => [...el._clases].join(" "), set: (v) => { el._clases = new Set(String(v).split(/\s+/).filter(Boolean)); } });
  // innerHTML: cada etiqueta de apertura es un hijo con sus clases y sus data-* (los selectores del popup son todos por clase).
  Object.defineProperty(el, "innerHTML", { get: () => "", set: (html) => {
    el._hijos = [...String(html).matchAll(/<[a-z]+\b([^>]*)>/gi)].map(([, attrs]) => elementoFalso(/\bclass="([^"]*)"/.exec(attrs)?.[1] || "",
      Object.fromEntries([...attrs.matchAll(/\bdata-([\w-]+)="([^"]*)"/g)].map(([, k, v]) => [k.replace(/-(\w)/g, (_, l) => l.toUpperCase()), v]))));
  } });
  return el;
}
const clickFalso = (el) => (el._oyentes.click || []).forEach(fn => fn({ target: el, preventDefault() {}, stopPropagation() {} }));
const seleccionada = (listEl) => listEl.querySelector(".email-chip.selected")?.dataset.email || "";

function declaracionDe(nombre) {
  const n = _arbolPopup.body.find(s => s.type === "VariableDeclaration" && s.declarations.some(d => d.id?.name === nombre));
  ok(n, `popup.js no tiene la declaración top-level ${nombre}`);
  return popup.slice(n.start, n.end);
}
function trozoDelPopup(desde, hasta) {
  const i = popup.indexOf(desde);
  ok(i >= 0, `no encontré "${desde}"`);
  const j = popup.indexOf(hasta, i);
  ok(j > i, `no encontré "${hasta}" después de "${desde}"`);
  return popup.slice(i, j);
}
// Las dos listas con su código exacto. La lectura de toolbar_config queda colgada hasta `soltarOrden()`.
function listasDelPopup({ orden, state }) {
  let soltar = null;
  const lecturas = [];
  const fetchFalso = (url) => { lecturas.push(url); return new Promise(r => { soltar = () => r(resp([{ value: JSON.stringify({ orden, medido: {}, fecha: "2026-09-13" }) }])); }); };
  const ids = Object.fromEntries(["email-result", "email-list", "email-verify-badge", "form-email", "form-email-futuro", "form-email-futuro-2", "form-email-futuro-3"].map(id => [id, elementoFalso(id)]));
  const document = { getElementById: (id) => ids[id] || null };
  const esc = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  const nombres = ["_ctxEmailsAnalisis", "_ctxEmailsProspecto", "_fuenteTextoClient", "_rankClient", "_motivoReboteClient", "_emailPickTierClient",
                   "_ordenarEmailsClient", "_elegirPreseleccionClient", "_asegurarOrdenTiersExtension", "_ordenCambioDesdeElDibujo",
                   "_reordenarAnalisisTrasOrden", "_redibujarAnalisisConservandoEleccion", "renderEmailList"];
  const codigo = [declaracionDe("_ordenTiersExtension"), declaracionDe("_alCambiarOrdenTiers"), ...nombres.map(fuenteDe)].join("\n");
  // renderProspectEmailList es un const adentro de initProspectCard: se arma con su card, sus emails y su lead.
  const tarjeta = trozoDelPopup("const renderProspectEmailList = () => {", "renderProspectEmailList();\n\n");
  const fabrica = new Function("state", "document", "fetch", "CONFIG", "rankEmail", "vetoDuroEmail", "esRegistranteWebmail", "motivoRebote", "_rebotesExtension",
    "tierDeEmail", "compararCandidatosEmail", "ordenDeTiersValido", "isGarbageEmail", "esc", "_emailVerifyCache", "_verifyClass", "_emailGrade",
    "_renderVerifyBadge", "autoVerifyEmailChips", "chrome", "_asegurarRebotesExtension", "_reordenarAnalisisTrasRebotes",
    `${codigo}\nfunction crearTarjeta(card, data) { const emails = data.emails; ${tarjeta} return renderProspectEmailList; }\nreturn { ${nombres.join(", ")}, _ordenTiersExtension, crearTarjeta };`);
  const p = fabrica(state, document, fetchFalso, { SUPABASE_URL: "https://x.supabase.co", SUPABASE_ANON_KEY: "anon" }, E.rankEmail, E.vetoDuroEmail, E.esRegistranteWebmail,
    E.motivoRebote, { cache: { set: new Set(), ts: 0 }, porDominio: new Map() }, E.tierDeEmail, E.compararCandidatosEmail, E.ordenDeTiersValido, isGarbageEmail, esc,
    new Map(), () => "verify-pending", () => ({ grade: "B", label: "" }), () => {}, async () => {}, { tabs: { create() {} } }, () => {}, () => {});
  // Una tarjeta de Prospects: el campo "Email" arranca con la preselección de renderProspectCard (mismo orden que el primer dibujo).
  const tarjetaFalsa = (lead) => {
    const partes = Object.fromEntries([".pcard-email-list", ".pcard-email-monday", ".pcard-email-manual", ".pcard-future-1", ".pcard-future-2", ".pcard-future-3"]
      .map(s => [s, elementoFalso(s.slice(1))]));
    const card = { isConnected: true, querySelector: (s) => partes[s] || null };
    const ctx = p._ctxEmailsProspecto(lead);
    partes[".pcard-email-monday"].value = p._elegirPreseleccionClient(p._ordenarEmailsClient(lead.emails, ctx), ctx);
    return { lista: partes[".pcard-email-list"], campo: partes[".pcard-email-monday"], dibujar: p.crearTarjeta(card, lead) };
  };
  return { p, ids, lecturas, tarjetaFalsa, soltarOrden: async () => { ok(soltar, "la lectura del orden no arrancó"); soltar(); await new Promise(r => setTimeout(r, 10)); } };
}
const ORDEN_GUARDADO = ["persona", "rol", "apollo", "generico"];
const agenteConOrden = (lista, fuentes, orden) => lista
  .map(e => ({ email: e, source: fuentes[e] || "", score: E.rankEmail(e, D, "") }))
  .filter(x => x.score >= 0).filter(x => !E.esRegistranteWebmail(x.email, x.source))
  .sort((a, b) => E.compararCandidatosEmail(a, b, { sourceRank: E.SOURCE_RANK_DEFAULT, orden }))[0]?.email || "";
const leadDe = (id, locales) => {
  const emails = locales.map(l => `${l}@${D}`);
  return { id, domain: D, category: "", emails, email_sources: Object.fromEntries(emails.map(e => [e, "scrape"])) };
};

test("I12: las tarjetas dibujadas con la lectura del orden en curso terminan con la dirección del agente; la elección a mano se conserva", async () => {
  const { lecturas, tarjetaFalsa, soltarOrden } = listasDelPopup({ orden: ORDEN_GUARDADO, state: { accessToken: "tok" } });
  const L1 = leadDe(1, ["publicidad", "juan.perez", "info"]), L2 = leadDe(2, ["ventas", "maria.lopez"]);
  const t1 = tarjetaFalsa(L1), t2 = tarjetaFalsa(L2), t3 = tarjetaFalsa(L1), t4 = tarjetaFalsa(L1), t5 = tarjetaFalsa(L1);
  for (const t of [t1, t2, t3, t4, t5]) t.dibujar();   // renderProspectsPage: un forEach sincrónico
  strictEqual(lecturas.length, 1, "una sola lectura para toda la página");
  for (const [t, lead] of [[t1, L1], [t2, L2]]) {
    const porDefecto = agenteConOrden(lead.emails, lead.email_sources, null), delAgente = agenteConOrden(lead.emails, lead.email_sources, ORDEN_GUARDADO);
    strictEqual(seleccionada(t.lista), porDefecto, "el primer dibujo sale con el orden por defecto");
    ok(porDefecto !== delAgente, `el caso tiene que distinguir: ${porDefecto} vs ${delAgente}`);
  }
  // t3: el MB elige a mano otra dirección; t4: elige a mano justo la preseleccionada; t5: edita el campo "Email".
  clickFalso(t3.lista.querySelectorAll(".email-chip").find(c => c.dataset.email === `info@${D}`));
  clickFalso(t4.lista.querySelector(".email-chip.selected"));
  t5.campo.dataset.userEdited = "1"; t5.campo.value = `otra.persona@${D}`;

  await soltarOrden();

  for (const [nombre, t, lead] of [["tarjeta 1", t1, L1], ["tarjeta 2", t2, L2]]) {
    const delAgente = agenteConOrden(lead.emails, lead.email_sources, ORDEN_GUARDADO);
    strictEqual(seleccionada(t.lista), delAgente, `${nombre}: la preselección es la del agente con el orden guardado`);
    strictEqual(t.campo.value, delAgente, `${nombre}: y el campo "Email" (lo que se envía) la sigue`);
  }
  strictEqual(seleccionada(t3.lista), `info@${D}`, "la elección a mano sobrevive al cambio de orden");
  strictEqual(t3.campo.value, `info@${D}`);
  strictEqual(seleccionada(t4.lista), `publicidad@${D}`, "aunque coincida con la preselección vieja, la eligió el MB");
  strictEqual(t4.campo.value, `publicidad@${D}`);
  strictEqual(t5.campo.value, `otra.persona@${D}`, "lo que el MB escribió en el campo no se pisa");

  // Un redibujo sin cambio de orden (verificar, el botón +/N) conserva lo elegido, como antes.
  t1.dibujar();
  strictEqual(seleccionada(t1.lista), agenteConOrden(L1.emails, L1.email_sources, ORDEN_GUARDADO));
});

test("I12: Análisis cambia a la dirección del agente cuando llega el orden, salvo que el MB haya tocado un chip", async () => {
  for (const tocaElMb of [false, true]) {
    const emails = ["publicidad", "juan.perez", "info"].map(l => `${l}@${D}`);
    const state = { accessToken: "tok", domain: D, category: "", emails: [...emails], emailSources: new Map(emails.map(e => [e, "Scrape"])), duplicate: null, pageSocialLinks: [] };
    const { p, ids, soltarOrden } = listasDelPopup({ orden: ORDEN_GUARDADO, state });
    p.renderEmailList(state.emails);
    const fuentes = Object.fromEntries(emails.map(e => [e, "scrape"]));
    strictEqual(ids["form-email"].value, agenteConOrden(emails, fuentes, null), "primer dibujo con el orden por defecto");
    if (tocaElMb) clickFalso(ids["email-list"].querySelector(".email-chip.selected"));
    await soltarOrden();
    const esperado = tocaElMb ? `publicidad@${D}` : agenteConOrden(emails, fuentes, ORDEN_GUARDADO);
    strictEqual(ids["form-email"].value, esperado, tocaElMb ? "la eligió el MB: se conserva" : "la había puesto el dibujo: se recalcula con el orden nuevo");
    strictEqual(seleccionada(ids["email-list"]), esperado);
    // La marca del MB es de ESTE dominio: si otra web tuviera la misma dirección puesta, no la hereda.
    if (tocaElMb) {
      state.domain = "otro-diario.com";
      ids["email-list"].dataset.ordenDelDibujo = "null";   // como si el orden hubiera cambiado desde el último dibujo
      p._redibujarAnalisisConservandoEleccion();
      strictEqual(ids["form-email"].value, agenteConOrden(emails, fuentes, ORDEN_GUARDADO), "otra web no hereda la elección a mano");
    }
  }
});

test("I12: el agente y el reintento ordenan con el comparador compartido, sin copias", () => {
  const usos = indexJs.match(/\.sort\(\(a, b\) => compararCandidatosEmail\(a, b, \{ sourceRank: SOURCE_RANK, orden: _tierOrdenCache\.orden \}\)\)/g) || [];
  strictEqual(usos.length, 2, "runAgentCycle y queueBounceRetry");
  ok(/compararCandidatosEmail\(a, b, \{ sourceRank: SOURCE_RANK/.test(cuerpoWorker("async function runAgentCycle(")));
  ok(/compararCandidatosEmail\(a, b, \{ sourceRank: SOURCE_RANK/.test(cuerpoWorker("async function queueBounceRetry(")));
  ok(!/const sa = SOURCE_RANK\[a\.source\]/.test(indexJs), "volvió una copia del comparador");
  ok(!/^const SOURCE_RANK_DEFAULT = /m.test(indexJs), "la tabla por defecto vive en lib/email.js");
  ok(/\.sort\(\(a, b\) => compararCandidatosEmail\(a, b\)\)/.test(fuenteDe("_ordenarEmailsClient")), "la extensión usa el mismo comparador");
});

test("I12: el enriquecimiento del agente anota la fuente de todo lo nuevo, sin pisar la que ya estaba", async () => {
  const w = await worker();
  const r = w._fuentesDelEnriquecimiento({ "info@x.com": "scrape", "viejo@x.com": { source: "informer", url: "u" } }, {
    apollo: ["Juan.Perez@x.com"], scrape: ["info@x.com", "contacto@x.com", "maria@x.com", "juan.perez@x.com"], google_contact: ["ventas@x.com", "maria@x.com"],
  });
  deepStrictEqual(r, {
    "info@x.com": "scrape", "viejo@x.com": { source: "informer", url: "u" },
    "juan.perez@x.com": "apollo", "contacto@x.com": "generic", "maria@x.com": "scrape", "ventas@x.com": "google_contact",
  });
  deepStrictEqual(w._fuentesDelEnriquecimiento(null, {}), {});
  const agente = cuerpoWorker("async function runAgentCycle(");
  const i = agente.indexOf("patch.email_sources = _fuentesDelEnriquecimiento(lead.email_sources, { apollo: apolloEmail, scrape: scraped, google_contact: serperEmails });");
  const j = agente.indexOf("lead.email_sources = patch.email_sources;");
  const k = agente.indexOf("const _sourcesMap = lead.email_sources || {};");
  ok(i > 0 && j > i && k > j, "la fuente se anota y el MISMO ciclo ordena con ella");
  const reenganche = cuerpoWorker("async function pickNextEmailCandidate(");
  ok(/select=id,emails,email_sources,/.test(reenganche), "el reenganche lee las fuentes para no pisarlas");
  ok(/const _fuentes = _fuentesDelEnriquecimiento\(lead\.email_sources, \{ scrape: _utiles \}\);/.test(reenganche)
     && /body: JSON\.stringify\(\{ emails: _todos, email_sources: _fuentes \}\)/.test(reenganche), "y anota las del raspado");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// I13. UNA SOLA ELECCIÓN: EL REINTENTO ES EL AGENTE CON OTRA POLÍTICA
// ════════════════════════════════════════════════════════════════════════════════════════════════
const cand = (x) => ({ email: `${x}@sitio.it`, source: "scrape", score: 80 });
const conVeredictos = (tabla) => {
  const consultas = [];
  return { consultas, fn: async (e) => { consultas.push(e); return tabla[e.split("@")[0]]; } };
};

test("I13: con la misma política, _elegirDireccion y el reintento eligen lo mismo y pagan lo mismo", async () => {
  const w = await worker();
  const listas = {
    "[riesgo, dudoso, dudoso, ok]": { a: "riesgo", b: "dudoso", c: "dudoso", d: "ok" },
    "[no, no, no, ok]": { a: "no", b: "no", c: "no", d: "ok" },
    "[dudoso, dudoso, dudoso, ok]": { a: "dudoso", b: "dudoso", c: "dudoso", d: "ok" },
    "[riesgo × 4]": { a: "riesgo", b: "riesgo", c: "riesgo", d: "riesgo" },
    "[sin_verificar, ok]": { a: "sin_verificar", b: "ok", c: "ok", d: "ok" },
  };
  for (const [nombre, tabla] of Object.entries(listas)) {
    const orden = ["a", "b", "c", "d"].map(cand);
    const v1 = conVeredictos(tabla);
    const d = await w._elegirDireccion(orden, {
      rebotado: () => false, noEscribir: () => "", marcaOk: () => true, ruta: async () => ({ verificar: true, enviar: true }),
      verificar: v1.fn, quemar: () => {}, alAgotarMv: "saltear", sinVerificar: "saltear", manualManda: true,
    });
    const v2 = conVeredictos(tabla);
    const r = await w._elegirEnviable("t", {}, "sitio.it", orden, {
      rutaMV: async () => ({ verificar: true, enviar: true }), verificarMV: (_t, _c, e) => v2.fn(e), quemar: async () => {},
    });
    strictEqual(r.chosen?.email || null, d.chosen?.email || null, `${nombre}: la dirección`);
    strictEqual(r.deReserva, d.deReserva, `${nombre}: de reserva`);
    deepStrictEqual(v2.consultas, v1.consultas, `${nombre}: las consultas pagas`);
    strictEqual(r.mvUsados, d.mvUsed);
  }
});

test("I13: la política de cada uno queda como estaba (qué hacer al gastar las consultas lo decide el dueño)", async () => {
  const w = await worker();
  const orden = ["a", "b", "c", "d"].map(cand);
  const opts = (v) => ({ rebotado: () => false, noEscribir: () => "", marcaOk: () => true, ruta: async () => ({ verificar: true, enviar: true }), verificar: v.fn });
  let v = conVeredictos({ a: "riesgo", b: "dudoso", c: "dudoso", d: "ok" });
  let r = await w._elegirDireccion(orden, opts(v));
  strictEqual(r.chosen?.email, "d@sitio.it", "el agente: la cuarta sale sin verificar y la mira la red final");
  strictEqual(v.consultas.length, 3);
  v = conVeredictos({ a: "riesgo", b: "dudoso", c: "dudoso", d: "ok" });
  r = await w._elegirEnviable("t", {}, "sitio.it", orden, { rutaMV: async () => ({ verificar: true, enviar: true }), verificarMV: (_t, _c, e) => v.fn(e), quemar: async () => {} });
  strictEqual(r.chosen?.email, "a@sitio.it", "el reintento: la reserva catch-all");
  ok(r.deReserva && r.motivos.includes("tope_mv"));
  ok(/_elegirDireccion\(lista, \{/.test(cuerpoWorker("async function _elegirEnviable(")) && !/for \(const cand of/.test(cuerpoWorker("async function _elegirEnviable(")), "el reintento no tiene su propio bucle");
  ok(/_decidirCandidato\(cand, hechos, politica\)/.test(cuerpoWorker("async function _elegirDireccion(")), "el agente decide cada candidato con la regla del reintento");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// I27. "NO PUDE VERIFICAR" NO ES UN CATCH-ALL
// ════════════════════════════════════════════════════════════════════════════════════════════════
test("I27: sin_verificar es reserva en el primer contacto y se saltea en el reintento; el catch-all real no cambia", async () => {
  const w = await worker();
  strictEqual(w._accionPorVeredictoMV("sin_verificar"), "reserva");
  strictEqual(w._accionPorVeredictoMV("riesgo"), "reserva");
  const c = { email: "ventas@sitio.it", source: "scrape" };
  const ciega = { verificar: false, enviar: true }, decide = { verificar: true, enviar: true };
  strictEqual(w._decidirCandidato(c, { ruta: decide, mv: "sin_verificar" }), "saltear:mv_sin_verificar", "reintento: sin verificar no sale al dominio que rebotó");
  strictEqual(w._decidirCandidato(c, { ruta: ciega, mv: "sin_verificar" }), "elegir", "con la ruta ciega (M365) la consulta no aportaba nada: como siempre");
  strictEqual(w._decidirCandidato(c, { ruta: decide, mv: "sin_verificar" }, { manualManda: false, sinVerificar: "reserva" }), "reserva", "agente: igual que hasta hoy");
  strictEqual(w._decidirCandidato(c, { ruta: decide, mv: "riesgo" }), "reserva", "el catch-all real sigue de reserva (lo decide el dueño)");
  strictEqual(w._decidirCandidato({ email: "nuevo@sitio.it", source: "manual" }, { mv: "sin_verificar" }), "elegir", "la dirección del MB sólo la frena un 'no'");
});

async function reintentoConMv(modo) {
  const w = await cargarWorker(["_elegirEnviable", "_verifyEmailMV", "_madridNowParts"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = async (url) => {
    const u = String(url);
    pedidos.push(u);
    if (u.includes("toolbar_mv_results?email=eq.")) return resp([]);
    if (u.includes("api.millionverifier.com")) {
      if (modo === "503") return resp({ error: "boom" }, { status: 503 });
      if (modo === "timeout") { const e = new Error("The operation was aborted due to timeout"); e.name = "TimeoutError"; throw e; }
      if (modo === "catch_all") return resp({ result: "catch_all", quality: "risky" });
      return resp({ result: "ok" });
    }
    return resp([], { status: 200 });
  };
  const hoy = w._madridNowParts().dateISO;
  const cfg = modo === "tope" ? { millionverifier_used: `${hoy}:500` } : {};
  const r = await w._elegirEnviable("t", cfg, "diarioejemplo.com.ar", [{ email: "ventas@diarioejemplo.com.ar", source: "scrape", score: 140 }],
    { rutaMV: async () => ({ verificar: true, enviar: true }) });
  return { r, pagadas: pedidos.filter(u => u.includes("api.millionverifier.com")).length, w, cfg };
}

test("I27: con el cupo diario gastado, un error 503 o un timeout de MV, el reintento no manda sin verificar", async () => {
  const clave = process.env.MILLIONVERIFIER_API_KEY;
  process.env.MILLIONVERIFIER_API_KEY = "clave-falsa";
  try {
    const tope = await reintentoConMv("tope");
    strictEqual(tope.r.chosen, null, `cupo gastado: ${JSON.stringify(tope.r)}`);
    ok(tope.r.motivos.includes("mv_sin_verificar"));
    strictEqual(tope.pagadas, 0, "con el cupo gastado no se llama a MillionVerifier");
    strictEqual(await tope.w._verifyEmailMV("t", tope.cfg, "otra@diarioejemplo.com.ar"), "sin_verificar");
    for (const modo of ["503", "timeout"]) {
      const x = await reintentoConMv(modo);
      strictEqual(x.r.chosen, null, `${modo}: ${JSON.stringify(x.r)}`);
      ok(x.r.motivos.includes("mv_sin_verificar"), modo);
      strictEqual(x.pagadas, 1, `${modo}: una consulta`);
    }
    const catchAll = await reintentoConMv("catch_all");
    strictEqual(catchAll.r.chosen?.email, "ventas@diarioejemplo.com.ar", "un catch-all real sigue saliendo de reserva: si tiene que exigir 'ok' lo decide el dueño");
    ok(catchAll.r.deReserva);
  } finally {
    globalThis.__fetchFalso = undefined;
    if (clave === undefined) delete process.env.MILLIONVERIFIER_API_KEY; else process.env.MILLIONVERIFIER_API_KEY = clave;
  }
  const mv = cuerpoWorker("async function _verifyEmailMV(");
  strictEqual((mv.match(/return "riesgo"/g) || []).length, 0, "ningún fallo nuestro vuelve a devolver 'riesgo'");
  strictEqual((mv.match(/return "sin_verificar"/g) || []).length, 3, "tope, error de la API y timeout");
  ok(!/\.catch\(\(\) => "riesgo"\)/.test(indexJs), "tampoco una excepción se disfraza de catch-all");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// I14. EL AVISO DE LA COLA "POR ENVIAR": SOBRE LO QUE MANDA EL LOTE, Y A LA VISTA
// ════════════════════════════════════════════════════════════════════════════════════════════════
const FILAS_COLA = [
  { emails: ["x@s.com"], monday_payload: { contacto_formulario: "https://s.com/c", email: "" } },
  { emails: ["x@s.com"], monday_payload: { email: "" } },
  { emails: ["x@s.com", "y@s.com"], monday_payload: { estado: "1" } },
  { emails: ["publicidad@s.com", "redaccion@s.com"], monday_payload: { email: "redaccion@s.com" } },
  { emails: [], monday_payload: null },
  { emails: ["a@s.com"] },
  {},
];

test("I14: el worker juzga la misma dirección que manda el lote (paridad con emailDeCola)", async () => {
  const w = await worker();
  for (const f of FILAS_COLA) strictEqual(w._emailQueMandaLaCola(f), emailDeCola(f), JSON.stringify(f));
});

test("I14: la auditoría avisa sobre monday_payload.email, no sobre emails[0], y no avisa en una fila de formulario", async () => {
  const w = await worker();
  const antes = E._bouncedCache.set;
  try {
    E._bouncedCache.set = new Set(["contacto@portal.pe", "info@diariouno.com.ar"]);
    const form = w._planAuditoriaLead({ id: 1, domain: "portal.pe", status: "por_enviar", emails: ["contacto@portal.pe"], email_sources: {},
      monday_payload: { contacto_formulario: "https://portal.pe/contacto", email: "" } });
    strictEqual(form.aviso, null, "el lote manda la URL del formulario: contacto@ rebotado no es lo que se manda");
    const otra = w._planAuditoriaLead({ id: 2, domain: "diariouno.com.ar", status: "por_enviar", emails: ["info@diariouno.com.ar", "ventas@clarin.com"],
      email_sources: { "ventas@clarin.com": { source: "manual_mb", by: "mb@adeqmedia.com" } }, monday_payload: { email: "ventas@clarin.com" } });
    deepStrictEqual(otra.aviso, { email: "ventas@clarin.com", motivo: "otra_marca" }, "la dirección que eligió el MB es de otra marca aunque no sea la primera");
    strictEqual(otra.plan, null);
    const vacio = w._planAuditoriaLead({ id: 3, domain: "portal.pe", status: "por_enviar", emails: ["contacto@portal.pe"], email_sources: {}, monday_payload: { email: "" } });
    strictEqual(vacio.aviso, null, "campo vacío al guardar: el lote manda vacío, no contacto@");
    const vieja = w._planAuditoriaLead({ id: 4, domain: "portal.pe", status: "por_enviar", emails: ["contacto@portal.pe"], email_sources: {} });
    deepStrictEqual(vieja.aviso, { email: "contacto@portal.pe", motivo: "ya_reboto" }, "una fila de antes del arreglo sigue con la primera");
  } finally {
    E._bouncedCache.set = antes;
  }
  const auditoria = cuerpoWorker("async function auditarEmailsDelPool(");
  ok(/const manda = fila \? String\(_emailQueMandaLaCola\(fila\) \|\| ""\) : "";/.test(auditoria), "la relectura compara con lo que manda el lote");
  ok(!/fila\.emails\[0\]/.test(auditoria), "la relectura volvió a mirar emails[0]");
});

test("I14: la lista de la cola y la confirmación del lote muestran el aviso cuando es sobre lo que se manda", () => {
  const conAviso = (mp, av) => ({ domain: "diariouno.com.ar", emails: ["info@diariouno.com.ar", "ventas@clarin.com"], monday_payload: { ...mp, aviso_email: av } });
  const a = avisoDeCola(conAviso({ email: "ventas@clarin.com" }, { email: "Ventas@Clarin.com", motivo: "otra_marca", fecha: "2026-09-13" }));
  deepStrictEqual([a?.email, a?.motivo], ["ventas@clarin.com", "otra_marca"]);
  ok(/otra marca/.test(a.texto));
  strictEqual(avisoDeCola(conAviso({ email: "info@diariouno.com.ar" }, { email: "ventas@clarin.com", motivo: "otra_marca" })), null, "el MB volvió a guardar con otra dirección: el aviso viejo no aplica");
  strictEqual(avisoDeCola(conAviso({ contacto_formulario: "https://diariouno.com.ar/c" }, { email: "ventas@clarin.com", motivo: "otra_marca" })), null, "fila de formulario");
  strictEqual(avisoDeCola({ monday_payload: { email: "x@y.com" } }), null, "sin aviso");
  ok(/basura/.test(avisoDeCola(conAviso({ email: "ventas@clarin.com" }, { email: "ventas@clarin.com", motivo: "basura_o_departamento" })).texto));

  const trozo = (desde, hasta) => { const i = popup.indexOf(desde); ok(i >= 0, desde); return popup.slice(i, popup.indexOf(hasta, i)); };
  const pintar = trozo("function _colaPintar() {", "function _colaMarcados()");
  ok(/const _av = avisoDeCola\(f\);/.test(pintar) && /_av\.texto/.test(pintar), "la lista muestra el aviso con el motivo");
  const lote = trozo('document.getElementById("btn-cola-enviar")?.addEventListener("click"', "const btn = document.getElementById(\"btn-cola-enviar\");");
  ok(/avisoDeCola\(f\)/.test(lote) && /confirm\(`Se van a crear \$\{ids\.length\} ficha\(s\) en ADEQ\. ¿Confirmás\?\$\{_txtAvisos\}`\)/.test(lote), "la confirmación del lote lista los avisos");
});

// ════════════════════════════════════════════════════════════════════════════════════════════════
// I24. HAY DIRECCIONES PERO NINGUNA QUEDÓ ELEGIDA
// ════════════════════════════════════════════════════════════════════════════════════════════════
test("I24: si la tarjeta tiene direcciones, el aviso no dice que el sistema todavía está buscando", () => {
  const cuerpo = fuenteDe("validateProspect");
  const i = cuerpo.indexOf("  if (!email) {"), j = cuerpo.indexOf("  if (!isValidEmail(email)) {");
  ok(i > 0 && j > i, "no encontré el bloque del email vacío");
  const bloque = new Function("card", "setResult", "email", cuerpo.slice(i, j) + "\nreturn 'siguió';");
  const correr = (chips) => {
    let msg = "";
    const card = { querySelectorAll: (sel) => (sel === ".pcard-email-list .email-chip[data-email]" ? Array.from({ length: chips }) : []), querySelector: () => null };
    const salida = bloque(card, (m) => { msg = m; }, "");
    return { msg, salida };
  };
  const con = correr(2), sin = correr(0);
  ok(!/still searching/.test(con.msg) && /No address selected/.test(con.msg), `con chips: ${con.msg}`);
  ok(/still searching/.test(sin.msg), `sin chips: ${sin.msg}`);
  strictEqual(con.salida, undefined, "y no sigue sin email");
  strictEqual(sin.salida, undefined);
});
