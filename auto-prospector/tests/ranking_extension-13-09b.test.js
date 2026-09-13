// El ranking de emails y la extensión, segunda tanda del 13/09. (2026-09-13)
//
// "Que Prospects muestre lo mismo que Análisis, y lo que arreglás un día no lo rompas al otro."
// Cada test es una regla que la auditoría encontró rota con el código real:
//   R1  La extensión escondía direcciones reales: el veto por lookalike leía "elcomercio" contra
//       "comercio" como imitación, y krzysztof@ caía como hash. Y una dirección vetada podía quedar
//       preseleccionada.
//   C7  Dos gmail rebotados quemaban gmail.com entero: toda persona@gmail daba -1 en el worker y el
//       agente la descartaba como "dominio_ya_rechazo".
//   C17 La tarjeta de Prospects ordenaba por la nota A-E (castiga publicidad@) y con las fuentes de la
//       pestaña de Análisis: elegía otro email que Análisis y que el agente.
//   C26 La lista de rebotados se leía cortada en 1.000 filas, y la extensión no la cargaba nunca.
//   C27 (ya arreglado en d4881a1) El agente y la extensión clasifican igual info@ venga de donde venga.
//   C28 El gmail del registrante (informer) sólo lo vetaba el agente.
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
import { isGarbageEmail, traerRebotados, EVIDENCIA_BLOQUEA_EXTENSION } from "../../modules/emailVerifier.js";

const aqui = path.dirname(fileURLToPath(import.meta.url));
const popup = fs.readFileSync(path.join(aqui, "..", "..", "popup", "popup.js"), "utf8");
const indexJs = fs.readFileSync(path.join(aqui, "..", "index.js"), "utf8");
const D = "diario-ejemplo.com";
const respuesta = (body, { status = 200 } = {}) => ({ ok: status >= 200 && status < 300, status, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) });

// El worker entero, una sola vez, con el fetch reemplazado por globalThis.__fetchFalso.
let _w = null;
const worker = () => (_w ||= cargarWorker(["_porQueNoEscribirA", "_DOMINIOS_QUE_RECHAZAN", "_recontarRebotesPorDominio", "rankEmail", "loadBouncedEmails",
                                           "_cargarDominiosQueRechazan", "_pickTier", "_bouncedCache"], { fetchFalso: true }));

// ── Las funciones del popup, con su código EXACTO (acorn) y las dependencias reales de lib/email.js ──
const _arbolPopup = acorn.parse(popup, { ecmaVersion: "latest", sourceType: "module" });
function fuenteDe(nombre) {
  const n = _arbolPopup.body.find(s => s.type === "FunctionDeclaration" && s.id?.name === nombre);
  ok(n, `popup.js no tiene la función top-level ${nombre}`);
  return popup.slice(n.start, n.end);
}
const trozo = (desde, hasta) => {
  const i = popup.indexOf(desde);
  ok(i >= 0, `no encontré "${desde}"`);
  const j = popup.indexOf(hasta, i);
  ok(j > i, `no encontré "${hasta}" después de "${desde}"`);
  return popup.slice(i, j);
};
// renderProspectEmailList es un const adentro de initProspectCard.
const listaTarjeta = () => trozo("const renderProspectEmailList = () => {", "renderProspectEmailList();\n\n");

function rankingDelPopup(state = { domain: "", category: "", emailSources: new Map(), accessToken: "" }) {
  const nombres = ["_isGenericEmailLocal", "_ctxEmailsAnalisis", "_ctxEmailsProspecto", "_fuenteTextoClient", "_rankClient", "_motivoReboteClient",
                   "_emailPickTierClient", "_ordenarEmailsClient", "_elegirPreseleccionClient", "_bestEmailByTier", "_emailGradeCompute"];
  const codigo = nombres.map(fuenteDe).join("\n");
  const rebotes = { cache: { set: new Set(), ts: 0 }, porDominio: new Map() };
  const fabrica = new Function("state", "rankEmail", "vetoDuroEmail", "esBuzonFuncional", "esRegistranteWebmail", "motivoRebote", "_isGenericLocalPart", "AD_SALES_LOCAL", "_rebotesExtension",
    `const _AD_SALES_LOCAL_RE = AD_SALES_LOCAL;\n${codigo}\nreturn { ${nombres.join(", ")} };`);
  return { ...fabrica(state, E.rankEmail, E.vetoDuroEmail, E.esBuzonFuncional, E.esRegistranteWebmail, E.motivoRebote, E._isGenericLocalPart, E.AD_SALES_LOCAL, rebotes), rebotes };
}

// ── R1. La extensión no puede esconder direcciones reales ───────────────────────────────────────
test("R1: el artículo no es una imitación: elcomercio.pe publica @comercio.com.pe y la extensión lo tiene que mostrar", () => {
  for (const [e, sitio] of [["roger.zuzunaga@comercio.com.pe", "elcomercio.pe"], ["piero.hatto@comercio.com.pe", "elcomercio.pe"],
                            ["ventas@universal.com.mx", "eluniversal.com.mx"], ["redaccion@diario.es", "eldiario.es"]]) {
    strictEqual(E.vetoDuroEmail(e, sitio), "", `${e} es del mismo medio que ${sitio}`);
    strictEqual(isGarbageEmail(e, sitio), false, `${e} tiene que verse como chip en la extensión`);
  }
  ok(E.detectarEmailSospechoso("info@clar1n.com", "clarin.com").startsWith("lookalike_del_sitio"), "una imitación con typo sigue cayendo");
  ok(E.detectarEmailSospechoso("info@clarin-noticias.com", "clarin.com").startsWith("typosquat_con_cebo"), "y un typosquat con cebo también");
  ok(E.detectarEmailSospechoso("info@elcomerc1o.pe", "elcomercio.pe").startsWith("lookalike_del_sitio"), "el artículo no perdona un typo en la marca");
  strictEqual(E._mismaMarcaSinArticulo("elpais", "pais"), false, "sin 5 letras de marca no se saca el artículo");
});

test("R1: la y cuenta como vocal en la regla del hash sin vocales, y los hashes siguen cayendo", () => {
  strictEqual(E.vetoDuroEmail("krzysztof@onet.pl", "onet.pl"), "", "krzysztof es un nombre");
  ok(E.rankEmail("krzysztof@onet.pl", "onet.pl", "") > 0);
  strictEqual(E.vetoDuroEmail(`x7k9m2p4@${D}`, D), "hash");
  strictEqual(E.vetoDuroEmail(`bcdfghjkl@${D}`, D), "hash");
});

test("R1: la extensión nunca deja preseleccionada una dirección de tier -1 (Análisis, tarjeta y campo de envío)", () => {
  const p = rankingDelPopup();
  const registrante = p._ctxEmailsProspecto({ domain: "baladag4.com.br", category: "", email_sources: { "rudnypc@gmail.com": "informer" } });
  strictEqual(p._emailPickTierClient("rudnypc@gmail.com", registrante), -1);
  strictEqual(p._elegirPreseleccionClient(["rudnypc@gmail.com"], registrante), "", "si es la única, se ve pero no queda puesta sola");
  E.cargarRebotados([{ email: `ventas@${D}`, evidencia: "rebote_smtp", fuente: "scrape" }], p.rebotes);
  const ctx = p._ctxEmailsProspecto({ domain: D, email_sources: {} });
  const orden = p._ordenarEmailsClient([`ventas@${D}`, `info@${D}`], ctx);
  deepStrictEqual(orden, [`info@${D}`, `ventas@${D}`], "el rebotado va al final");
  strictEqual(p._elegirPreseleccionClient(orden, ctx), `info@${D}`);
  const adivinada = p._ctxEmailsProspecto({ domain: D, email_sources: { [`publicidad@${D}`]: "rol_mx", [`juan.perez@${D}`]: "scrape" } });
  strictEqual(p._elegirPreseleccionClient(p._ordenarEmailsClient([`publicidad@${D}`, `juan.perez@${D}`], adivinada), adivinada), `juan.perez@${D}`,
    "una adivinada no queda puesta si hay una publicada (36fb125)");

  const lista = fuenteDe("renderEmailList");
  ok(/_elegirPreseleccionClient\(_frescos\.map\(c => c\.dataset\.email\), _ctxLista\)/.test(lista) && /\.find\(_elegible\)/.test(lista), "Análisis elige con la regla compartida");
  ok(!/const preferredChip = listEl\.querySelector\(/.test(lista), "el preferredChip viejo tomaba el primer chip sin mirar el tier");
  const tarjeta = fuenteDe("renderProspectCard");
  ok(/pcard-email-monday" value="\$\{esc\(_preseleccion\)\}"/.test(tarjeta), "el campo de envío arranca con la preselección, no con emails[0]");
  ok(/const _preseleccion = _elegirPreseleccionClient\(_ordenarEmailsClient\(emails, _ctxTarjeta\), _ctxTarjeta\);/.test(tarjeta));
  const lp = listaTarjeta();
  ok(/_elegirPreseleccionClient\(_chipsPrincipales\.map\(c => c\.dataset\.email\), _ctxCard\)/.test(lp), "la lista de la tarjeta elige con la regla compartida");
  ok(!/const first = listEl\.querySelector\(/.test(lp), "el primer chip ya no se elige a ciegas");
});

// ── C7. Un proveedor de casillas no es un dominio que rechaza ────────────────────────────────────
test("C7: qué es un proveedor de casillas y qué no", () => {
  for (const d of ["gmail.com", "googlemail.com", "hotmail.com.ar", "outlook.es", "yahoo.co.uk", "gmx.de", "proton.me", "icloud.com", "msn.com",
                   "libero.it", "onet.pl", "uol.com.br", "naver.com", "mail.ru"]) ok(E.esBuzonCompartido(d), `${d} es un proveedor de casillas`);
  for (const d of ["diario.com", "mail.diario.com", "live.diario.com", "gmail-noticias.com", "clarin.com", "", null]) ok(!E.esBuzonCompartido(d), `${d} no lo es`);
});

test("C7: dos gmail rebotados no queman gmail.com; la casilla exacta sí sigue vetada y un dominio de empresa se quema como antes", () => {
  const sinRebotes = E.rankEmail("juanperez@gmail.com", D, ""), info = E.rankEmail(`info@${D}`, D, "");
  const libero = E.rankEmail("mario.rossi@libero.it", "corriere-ejemplo.it", "");
  try {
    E._rebotesPorDominio.set("gmail.com", new Set(["a1@gmail.com", "b2@gmail.com"]));
    E._rebotesPorDominio.set("libero.it", new Set(["x@libero.it", "y@libero.it"]));
    E._rebotesPorDominio.set("hotmail.com", new Set(["z@hotmail.com"]));
    strictEqual(E.rankEmail("juanperez@gmail.com", D, ""), sinRebotes, "persona@gmail vale lo mismo con o sin gmails rebotados");
    ok(sinRebotes > info);
    strictEqual(E.rankEmail("adrianofrazao@gmail.com", "significados.com.br", ""), 65, "el único contacto de significados.com.br");
    strictEqual(E.rankEmail("mario.rossi@libero.it", "corriere-ejemplo.it", ""), libero);
    strictEqual(E.rankEmail("maria.lopez@hotmail.com", D, ""), E.rankEmail("maria.lopez@gmail.com", D, ""), "un rebote en hotmail no le resta 40 a nadie");
    E._bouncedCache.set.add("rebotado@gmail.com");
    strictEqual(E.rankEmail("rebotado@gmail.com", D, ""), -1, "la casilla exacta que rebotó sigue vetada");
    E._rebotesPorDominio.set(D, new Set([`a@${D}`, `b@${D}`]));
    strictEqual(E.rankEmail(`ventas@${D}`, D, ""), -1, "dos rebotes en un dominio de empresa lo siguen quemando");
    E._rebotesPorDominio.set(D, new Set([`a@${D}`]));
    strictEqual(E.rankEmail(`ventas@${D}`, D, ""), 135 - 40, "y uno solo sigue restando 40");
  } finally {
    E._rebotesPorDominio.clear();
    E._bouncedCache.set.delete("rebotado@gmail.com");
  }
});

test("C7: el agente no descarta a toda persona@gmail porque dos gmail no existían", async () => {
  strictEqual(E.motivoNoEscribirDominio("gmail.com", { usuarios: 5, dominioMuerto: false }), "");
  ok(E.motivoNoEscribirDominio(D, { usuarios: 2 }));
  ok(E.motivoNoEscribirDominio(D, { dominioMuerto: true }));
  strictEqual(E.motivoNoEscribirDominio(D, { usuarios: 1 }), "");
  const w = await worker();
  try {
    w._DOMINIOS_QUE_RECHAZAN.set("gmail.com", { usuarios: 5, dominioMuerto: false });
    w._DOMINIOS_QUE_RECHAZAN.set(D, { usuarios: 2, dominioMuerto: false });
    strictEqual(w._porQueNoEscribirA("juanperez@gmail.com"), "", "dominio_ya_rechazo no aplica a un proveedor de casillas");
    ok(w._porQueNoEscribirA(`otra@${D}`), "a un dominio de empresa sí");
    w._recontarRebotesPorDominio([{ email: "a1@gmail.com", evidencia: "verificador", fuente: "scrape" }, { email: "b2@gmail.com", evidencia: "rebote_smtp", fuente: "scrape" }]);
    strictEqual(w.rankEmail("tvcherpak@gmail.com", "zvezdev.com", ""), 65, "el worker, con dos gmail en la tabla, sigue puntuando la persona");
  } finally {
    w._DOMINIOS_QUE_RECHAZAN.clear();
    w._recontarRebotesPorDominio([]);
  }
});

// ── C26. La lista de rebotados, entera, en el worker y en la extensión ─────────────────────────────
const paginaDeRebotes = (desde, n, extra = {}) => Array.from({ length: n }, (_, i) => ({ email: `persona${desde + i}@medio${desde + i}.com`, evidencia: "rebote_smtp", fuente: "scrape", ...extra }));
const desdeDe = (opts) => parseInt(String(opts?.headers?.Range || "0-999").split("-")[0], 10);

test("C26: el worker lee TODA la lista de rebotados de a páginas, y una página caída no deja una lista parcial", async () => {
  const w = await worker();
  const pedidos = [];
  try {
    globalThis.__fetchFalso = async (url, opts = {}) => {
      pedidos.push(String(url));
      const desde = desdeDe(opts);
      return respuesta(desde === 0 ? paginaDeRebotes(0, 1000) : desde === 1000 ? paginaDeRebotes(1000, 5) : []);
    };
    w._bouncedCache.ts = 0;
    await w.loadBouncedEmails("t");
    strictEqual(w._bouncedCache.set.size, 1005, "PostgREST devuelve 1.000 por pedido: con limit=10000 se perdían las demás");
    strictEqual(w.rankEmail("persona1004@medio1004.com", "medio1004.com", ""), -1, "una dirección de la segunda página tiene que estar vetada");
    ok(pedidos.every(u => !/limit=/.test(u)) && pedidos.every(u => u.includes("evidencia=in.(rebote_smtp,verificador,sin_clasificar)")));

    globalThis.__fetchFalso = async (url, opts = {}) => (desdeDe(opts) === 1000 ? respuesta({ message: "boom" }, { status: 500 }) : respuesta(paginaDeRebotes(0, 1000)));
    w._bouncedCache.ts = 0;
    await w.loadBouncedEmails("t");
    strictEqual(w._bouncedCache.set.size, 1005, "si una página falla, queda la lista anterior entera");
    strictEqual(w._bouncedCache.ts, 0, "y el ts viejo, para reintentar en la próxima vuelta");
  } finally {
    w._bouncedCache.set = new Set();
    w._bouncedCache.ts = 0;
    w._recontarRebotesPorDominio([]);
  }
});

test("C26: los dominios que rechazan direcciones (el filtro del envío) también se leen enteros", async () => {
  const w = await worker();
  try {
    globalThis.__fetchFalso = async (url, opts = {}) => {
      const desde = desdeDe(opts);
      if (desde === 0) return respuesta(paginaDeRebotes(0, 1000, { tipo: "usuario_inexistente" }));
      if (desde === 1000) return respuesta([{ email: "a@quemado-ejemplo.com", tipo: "usuario_inexistente", evidencia: "rebote_smtp" },
                                            { email: "b@quemado-ejemplo.com", tipo: "usuario_inexistente", evidencia: "rebote_smtp" }]);
      return respuesta([]);
    };
    await w._cargarDominiosQueRechazan("t");
    ok(w._porQueNoEscribirA("c@quemado-ejemplo.com"), "el dominio de la segunda página tiene que contar");
  } finally {
    w._DOMINIOS_QUE_RECHAZAN.clear();
  }
});

test("C26: la extensión trae la lista con el mismo filtro que el worker, de a páginas, con reloj, y null si algo falla", async () => {
  strictEqual(EVIDENCIA_BLOQUEA_EXTENSION, indexJs.match(/const EVIDENCIA_BLOQUEA = "([^"]+)"/)?.[1], "el mismo filtro de evidencia que EVIDENCIA_BLOQUEA");
  const pedidos = [];
  const fetchImpl = async (url, opts) => {
    pedidos.push(opts);
    return respuesta(desdeDe(opts) === 0
      ? Array.from({ length: 1000 }, (_, i) => ({ email: `X${i}@a.com`, evidencia: "verificador", fuente: { source: "rol_mx", url: "https://a.com" } }))
      : [{ email: "ultimo@b.com", evidencia: "rebote_smtp", fuente: "scrape" }]);
  };
  const filas = await traerRebotados("tok", { fetchImpl });
  strictEqual(filas.length, 1001);
  deepStrictEqual(filas[0], { email: "x0@a.com", evidencia: "verificador", fuente: "rol_mx" }, "minúsculas y la fuente como texto");
  ok(pedidos.every(o => o.signal) && pedidos.every(o => o.headers.Authorization === "Bearer tok"), "cada página con reloj y con el token del MB");
  strictEqual(await traerRebotados("tok", { fetchImpl: async () => respuesta({}, { status: 500 }) }), null);
  strictEqual(await traerRebotados("tok", { fetchImpl: async () => { throw new Error("red"); } }), null);
  strictEqual(await traerRebotados("", { fetchImpl }), null);
});

test("C26: los rebotados de la extensión ordenan pero no esconden: la lista compartida del módulo no se toca", () => {
  const p = rankingDelPopup();
  E.cargarRebotados([
    { email: `ventas@${D}`, evidencia: "rebote_smtp" },
    { email: "a@quemado-ejemplo.com", evidencia: "rebote_smtp" }, { email: "b@quemado-ejemplo.com", evidencia: "rebote_smtp" },
    { email: "x@gmail.com", evidencia: "rebote_smtp" }, { email: "y@gmail.com", evidencia: "rebote_smtp" },
    { email: "contacto@adivinado-ejemplo.com", evidencia: "verificador", fuente: "rol_mx" }, { email: "redaccion@adivinado-ejemplo.com", evidencia: "verificador", fuente: "pattern" },
  ], p.rebotes);
  strictEqual(E.motivoRebote(`ventas@${D}`, p.rebotes), "ya_reboto");
  strictEqual(E.motivoRebote("comercial@quemado-ejemplo.com", p.rebotes), "dominio_quemado");
  strictEqual(E.motivoRebote("juanperez@gmail.com", p.rebotes), "", "dos gmail tampoco queman gmail en la extensión");
  strictEqual(E.motivoRebote("publicidad@adivinado-ejemplo.com", p.rebotes), "", "dos adivinanzas rechazadas por MV no queman el dominio");
  strictEqual(E.vetoDuroEmail(`ventas@${D}`, D), "", "la lista compartida del módulo sigue vacía");
  strictEqual(isGarbageEmail(`ventas@${D}`, D), false, "la dirección se sigue viendo: esconderla o bloquearla lo decide el dueño");
  strictEqual(p._emailPickTierClient(`ventas@${D}`, p._ctxEmailsProspecto({ domain: D, email_sources: {} })), -1, "pero va al final");

  ok(/cargarRebotados\(filas, _rebotesExtension\)/.test(popup) && !/cargarRebotados\(filas\)/.test(popup),
     "cargar la lista COMPARTIDA en la extensión haría que isGarbageEmail esconda los rebotados");
  ok(/traerRebotados\(state\.accessToken\)/.test(fuenteDe("_asegurarRebotesExtension")));
  ok(/_asegurarRebotesExtension\(_reordenarAnalisisTrasRebotes\)/.test(fuenteDe("renderEmailList")), "Análisis pide la lista y se reordena al llegar");
  ok(/_asegurarRebotesExtension\(\(\) => \{ if \(card\.isConnected/.test(listaTarjeta()), "la tarjeta también");
});

// ── C17. La tarjeta de Prospects ordena como Análisis ──────────────────────────────────────────────
test("C17: la tarjeta ordena con la regla de Análisis (rol comercial > persona > genérico), no con la nota A-E", () => {
  const lp = listaTarjeta();
  const antesDeLosChips = lp.slice(0, lp.indexOf("const chipFor"));
  ok(!/\.sort\(/.test(antesDeLosChips) && !/_gradeRank/.test(antesDeLosChips), "la nota A-E no puede volver a ordenar la tarjeta");
  ok(/const sorted = _ordenarEmailsClient\(emails, _ctxCard\);/.test(lp), "la tarjeta usa el orden compartido con su propio contexto");
  ok(!/state\.emailSources/.test(lp), "la tarjeta no puede leer las fuentes de la pestaña de Análisis");

  const lead = { domain: "diario.com", category: "", email_sources: { "juan.perez@diario.com": "scrape", "publicidad@diario.com": "scrape", "info@diario.com": "generic" } };
  const emails = ["juan.perez@diario.com", "info@diario.com", "publicidad@diario.com"];
  const p = rankingDelPopup();
  const enTarjeta = p._ordenarEmailsClient(emails, p._ctxEmailsProspecto(lead));
  deepStrictEqual(enTarjeta, ["publicidad@diario.com", "juan.perez@diario.com", "info@diario.com"]);
  const gP = p._emailGradeCompute("publicidad@diario.com", null, "scrape").grade, gJ = p._emailGradeCompute("juan.perez@diario.com", null, "scrape").grade;
  ok(gP > gJ, `la nota sigue poniendo a publicidad@ (${gP}) debajo de juan.perez@ (${gJ}): por eso no puede ordenar`);
  const analisis = rankingDelPopup({ domain: "diario.com", category: "", emailSources: new Map([["juan.perez@diario.com", "Page"], ["publicidad@diario.com", "Scrape"], ["info@diario.com", "Page"]]) });
  deepStrictEqual(analisis._ordenarEmailsClient(emails), enTarjeta, "el mismo lead en Análisis y en Prospects, el mismo orden");
  strictEqual(analisis._bestEmailByTier(emails), p._elegirPreseleccionClient(enTarjeta, p._ctxEmailsProspecto(lead)), "y la misma dirección puesta");
});

test("C17: verificar no reordena ni pisa la elección, y la nota del chip usa la fuente de su propio lead", () => {
  const lp = listaTarjeta();
  ok(/listEl\.dataset\.eleccionMb = chip\.dataset\.email/.test(lp) && /_chipDe\(listEl\.dataset\.eleccionMb \|\| ""\)/.test(lp), "la elección a mano sobrevive a los redibujos");
  ok(/const _selAntes = listEl\.querySelector\("\.email-chip\.selected"\)\?\.dataset\.email/.test(lp), "lo elegido antes de redibujar se conserva");
  ok(/data-src="\$\{esc\(srcObj\.source\)\}"/.test(lp) && /data-src="\$\{esc\(src\)\}"/.test(fuenteDe("renderEmailList")), "cada chip lleva su fuente");
  ok(/const src = chip\.dataset\.src !== undefined \? chip\.dataset\.src : /.test(fuenteDe("autoVerifyEmailChips")), "la nota verificada usa la fuente del chip");
});

// ── C27. El agente y la extensión nunca ordenan al revés ──────────────────────────────────────────
test("C27: para cada par email × fuente, el agente (_pickTier) y la extensión nunca ordenan al revés", async () => {
  const w = await worker();
  const p = rankingDelPopup();
  const emails = ["publicidad", "marketing", "comercial", "juan.perez", "maria", "redaccion", "info", "contacto", "press", "news", "download"].map(l => `${l}@diario.com`);
  const fuentes = ["scrape", "rol_mx", "serper", "google_contact", "[object Object]", "apollo", "manual", "informer", "generic", "", "Facebook"];
  const items = [];
  for (const e of emails) for (const f of fuentes) {
    const tp = p._emailPickTierClient(e, p._ctxEmailsProspecto({ domain: "diario.com", email_sources: { [e]: f } }));
    if (tp >= 0) items.push({ e, f, tp, tw: w._pickTier(e, f) });
  }
  ok(items.length >= 100, `esperaba la matriz casi entera, hay ${items.length}`);
  const alReves = [];
  for (const a of items) for (const b of items) if (a.tw > b.tw && a.tp < b.tp) alReves.push(`${a.e} (${a.f}) vs ${b.e} (${b.f}): agente ${a.tw}>${b.tw}, extensión ${a.tp}<${b.tp}`);
  strictEqual(alReves.length, 0, alReves.slice(0, 10).join("\n"));
  for (const f of ["scrape", "rol_mx", "serper", "google_contact", "[object Object]"]) {
    strictEqual(E._tipoDeEmailParaRanking("info@diario.com", f), "generico", `info@ con ${f}`);
    strictEqual(E._tipoDeEmailParaRanking("contacto@diario.com", f), "generico", `contacto@ con ${f}`);
  }
  for (const l of ["press", "news"]) strictEqual(E._tipoDeEmailParaRanking(`${l}@diario.com`, "scrape"), "generico");
  for (const l of ["publicidad", "marketing", "comercial"]) for (const f of ["scrape", "rol_mx"]) strictEqual(E._tipoDeEmailParaRanking(`${l}@diario.com`, f), "rol", `${l}@ con ${f}`);
});

// ── C28. El gmail del registrante, la misma regla en los dos lados ─────────────────────────────────
test("C28: el gmail del registrante (informer) es una regla compartida, y la extensión lo manda al final con nota E", () => {
  ok(E.esRegistranteWebmail("x@gmail.com", "informer"));
  ok(E.esRegistranteWebmail("x@gmail.com", "Informer"), "la extensión guarda la fuente con mayúscula");
  ok(E.esRegistranteWebmail("x@gmail.com", { source: "informer" }), "la base la guarda a veces como objeto");
  ok(!E.esRegistranteWebmail("x@gmail.com", "scrape"), "un gmail publicado en el sitio sí puede ser el contacto");
  ok(E.esRegistranteWebmail("ivan@mail.ru", "informer"));
  ok(!E.esRegistranteWebmail("ventas@diario.com.ar", "informer"));
  // Mientras el agente tenga la regex propia, tiene que ser la misma lista.
  const literal = indexJs.match(/x\.source === "informer" && (\/@\(gmail[^\n]*?\/i)\.test/);
  if (literal) strictEqual(literal[1], String(E.FREEMAIL_REGISTRANTE_RE), "la lista del agente y la de lib/email.js no pueden separarse");

  const p = rankingDelPopup({ domain: "baladag4.com.br", category: "", emailSources: new Map([["rudnypc@gmail.com", "Informer"]]) });
  strictEqual(p._emailPickTierClient("rudnypc@gmail.com"), -1);
  strictEqual(p._bestEmailByTier(["rudnypc@gmail.com"]), "", "no queda puesto en el campo de envío");
  strictEqual(p._emailGradeCompute("rudnypc@gmail.com", null, "informer").grade, "E");
});
