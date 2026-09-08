// El recuadro del CRM tiene que contestar SIEMPRE, y rápido. (2026-09-07, reporte del user)
//
// Lo que pasó: *"📋 CRM — Checking… y nunca arroja resultado"*. El endpoint no tenía la culpa
// —`/api/crm/ficha` es un `.eq('domain', …)` con índice, medido en 0,5-0,75 s—: el `fetch` del
// popup iba **sin timeout**. Un pedido colgado no rechaza nunca, así que la promesa no se
// resolvía, nadie pintaba el veredicto y el recuadro se quedaba con el "Checking..." del HTML
// para siempre: sin error, sin aviso y sin manera de reintentar.
//
// Este test corre el `buscarEnCrm` REAL, extraído de popup.js — no una copia. Si alguien le
// saca el timeout o los dos intentos, falla acá y no en la pantalla de un media buyer.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const aqui  = path.dirname(fileURLToPath(import.meta.url));
const RUTA  = path.join(aqui, "..", "..", "popup", "popup.js");
const popup = fs.readFileSync(RUTA, "utf8");

/** Recorta una función del fuente contando llaves, para poder correrla de verdad. */
function extraer(fuente, firma) {
  const i = fuente.indexOf(firma);
  if (i < 0) throw new Error(`no encontré ${firma} en popup.js`);
  // ⚠️ El cuerpo NO empieza en la primera `{` después del nombre: si la firma desestructura un
  // parámetro —`applyDraftToPitch(d, { silent = false } = {})`— esa llave es del parámetro, y
  // el conteo cerraba ahí mismo devolviendo tres palabras en vez de la función. Un test que
  // buscaba algo dentro daba falso negativo, y uno que verificaba una AUSENCIA pasaba sin
  // mirar nada. Se salta la lista de parámetros contando paréntesis y recién después se abre.
  let p = fuente.indexOf("(", i), prof = 0, j = -1;
  for (let k = p; k < fuente.length; k++) {
    if (fuente[k] === "(") prof++;
    else if (fuente[k] === ")" && --prof === 0) { j = fuente.indexOf("{", k); break; }
  }
  if (j < 0) throw new Error(`no encontré el cuerpo de ${firma}`);
  let n = 0;
  for (let k = j; k < fuente.length; k++) {
    if (fuente[k] === "{") n++;
    else if (fuente[k] === "}" && --n === 0) return fuente.slice(i, k + 1);
  }
  throw new Error(`no cerró la llave de ${firma}`);
}

// El helper tiene que probarse a sí mismo: si vuelve a cortar de más, los tests que dependen
// de él mienten en silencio.
test("el extractor no se corta en una llave de la firma", () => {
  const f = extraer("function f(d, { s = 1 } = {}) { const x = 2; return x; }\nlisto", "function f(");
  ok(/return x;/.test(f), `el extractor devolvió ${JSON.stringify(f)}: se cortó en el parámetro`);
});

const FUENTE_TIMEOUT = /const _CRM_FICHA_TIMEOUT_MS = (\d+);/.exec(popup);
ok(FUENTE_TIMEOUT, "popup.js tiene que declarar _CRM_FICHA_TIMEOUT_MS");
const TIMEOUT_MS = Number(FUENTE_TIMEOUT[1]);

// El watchdog se lee del fuente y no se copia acá: el número exacto puede cambiar, lo que no
// puede cambiar es que le entren los dos intentos. Está escrito como fórmula
// (`2 * _CRM_FICHA_TIMEOUT_MS + 1000`), así que se evalúa en vez de leerse literal.
const FUENTE_WD = /const _CRM_WATCHDOG_MS = ([^;]+);/.exec(popup);
ok(FUENTE_WD, "popup.js tiene que declarar _CRM_WATCHDOG_MS");
const WATCHDOG_MS = Number(new Function("_CRM_FICHA_TIMEOUT_MS", `return ${FUENTE_WD[1]}`)(TIMEOUT_MS));

/** Devuelve el buscarEnCrm real, con sus dependencias inyectadas. */
function cargarBuscarEnCrm(fetchFalso, { bloqueo = async () => ({ blocked: false }) } = {}) {
  const cuerpo = extraer(popup, "function _dominioRaiz(host) {") + "\n" +
                 extraer(popup, "async function buscarEnCrm(domain) {");
  const fab = new Function(
    "fetch", "AbortSignal", "console", "crmUrl", "CONFIG", "_CRM_FICHA_TIMEOUT_MS", "checkDomainBlocked", "state",
    `${cuerpo}; return buscarEnCrm;`,
  );
  return fab(
    fetchFalso, AbortSignal, { warn() {} },
    () => "https://console.adeqmedia.com/api/crm/ficha",
    { CRM_BOARD_SECRET: "x" },
    TIMEOUT_MS,
    bloqueo, { accessToken: "" },
  );
}

/** El _veredictoCrm real, con sus cuatro regex. */
function cargarVeredicto() {
  const ini = popup.indexOf("const _CRM_LIVE_RE");
  const fin = popup.indexOf("\n}\n", popup.indexOf("function _veredictoCrm(dup) {")) + 3;
  return new Function(popup.slice(ini, fin) + "; return _veredictoCrm;")();
}

// ── La lista de bloqueados se consulta ANTES que el CRM (2026-09-08) ────────────────────
// Parte del 08/09: Diego, parado en mail.google.com, le mandó el pitch a cto@arise.tv y la
// ficha quedó registrada bajo `mail.google.com`. El veredicto sólo preguntaba al CRM, y "no
// está en el CRM" se pintaba como "Web prospectable · Nunca fue contactada".
test("un dominio bloqueado da NO prospectable sin llegar a preguntarle al CRM", async () => {
  let llamadasAlCrm = 0;
  const buscar = cargarBuscarEnCrm(async () => { llamadasAlCrm++; return respuesta({ found: false }); },
                                   { bloqueo: async (d) => d.endsWith("google.com") ? { blocked: true, reason: "Subdominio de google.com" } : { blocked: false } });
  const r = await buscar("mail.google.com");
  strictEqual(r.found, false);
  strictEqual(r.bloqueado, "Subdominio de google.com");
  strictEqual(llamadasAlCrm, 0, "bloqueado es bloqueado: no hace falta gastar la consulta");
  const v = cargarVeredicto()(r);
  strictEqual(v.ok, false);
  ok(!v.duda, "no es una duda: es un no");
  ok(/Web NO prospectable/.test(v.titulo) && /google\.com/.test(v.detalle), `${v.titulo} — ${v.detalle}`);
  // Y uno que no está bloqueado sigue el camino normal.
  const libre = await buscar("diario-nuevo.com");
  strictEqual(libre.bloqueado, undefined);
  strictEqual(llamadasAlCrm, 1);
});

const respuesta = (json) => ({ ok: true, json: async () => json });

test("el timeout deja pasar el arranque en frío, y los dos intentos entran en el watchdog", () => {
  // Medido contra producción el 08/09: 0,40-0,66 s en caliente y **3,15 s** en el primer pedido
  // del día, cuando Vercel levanta la función. Con el techo en 2,5 s ese primer pedido —el que
  // hace el MB al abrir la toolbar a la mañana— se abortaba siempre. El piso protege de que
  // alguien lo vuelva a bajar "para que sea más rápido" y rompa justo la primera apertura.
  ok(TIMEOUT_MS >= 3500, `el timeout es ${TIMEOUT_MS}ms: no le entra el arranque en frío de Vercel (3,15s medidos)`);
  ok(TIMEOUT_MS <= 5000, `el timeout es ${TIMEOUT_MS}ms: el user pidió enterarse enseguida del estado de la web`);
  // La invariante que de verdad importa: si el watchdog dispara antes de que termine el segundo
  // intento, el MB ve "no pude consultar" sobre una consulta que iba a contestar bien.
  ok(TIMEOUT_MS * 2 < WATCHDOG_MS,
     `dos intentos (${TIMEOUT_MS * 2}ms) tienen que caber en el watchdog (${WATCHDOG_MS}ms)`);
});

// ── EL MATCHEO CON EL CRM ───────────────────────────────────────────────────────────────
// *"Me tenés que garantizar que el cartel del CRM funcione. Qué tan difícil es matchear con
// el CRM y decir si es o no prospectable."* (user, 08/09) · *"Omití el www, el http y todo
// eso: que sea dominio real, ole.com y listo. En el CRM están todos sin www ni https."*
//
// Medido contra producción el 08/09: el endpoint tolera `www.`, MAYÚSCULAS y la ruta, pero
// NO los subdominios — `m.elpais.com` daba `found:false` mientras `elpais.com` daba "Ciclo
// Finalizado". Y ese fallo cae para el lado peligroso: "no encontrado" se pinta como
// **"Web prospectable"**, invitando a escribirle a un cliente activo.
test("un subdominio encuentra la ficha del dominio raíz", async () => {
  const vistos = [];
  const buscar = cargarBuscarEnCrm(async (url) => {
    const dom = decodeURIComponent(new URL(url).searchParams.get("domain"));
    vistos.push(dom);
    return respuesta(dom === "elpais.com" ? { found: true, estado: "Live" } : { found: false });
  });
  const r = await buscar("m.elpais.com");
  strictEqual(r.found, true, "entrar por la versión móvil no puede decir que el cliente está libre");
  strictEqual(r.status, "Live");
  strictEqual(r.dominioConsultado, "elpais.com");
  deepStrictEqual(vistos, ["m.elpais.com", "elpais.com"], "primero el host tal cual, después el raíz");
});

test("un sufijo compuesto no se corta de más", async () => {
  const vistos = [];
  const buscar = cargarBuscarEnCrm(async (url) => {
    vistos.push(decodeURIComponent(new URL(url).searchParams.get("domain")));
    return respuesta({ found: false });
  });
  await buscar("deportes.clarin.com.ar");
  // `clarin.com.ar`, nunca `com.ar`: cortar de más preguntaría por un dominio que no es nadie.
  ok(vistos.includes("clarin.com.ar"), `probó ${vistos.join(", ")}: falta el raíz con sufijo compuesto`);
  ok(!vistos.includes("com.ar"), `probó "com.ar", que no es un dominio`);
});

test("un dominio sin subdominio se pregunta UNA sola vez", async () => {
  let n = 0;
  const buscar = cargarBuscarEnCrm(async () => { n++; return respuesta({ found: false }); });
  await buscar("elpais.com");
  strictEqual(n, 1, "preguntar dos veces lo mismo duplica la espera del cartel sin ganar nada");
});

test('"no está en el CRM" NO es "no pude consultar"', async () => {
  // Los dos terminan en `found:false` y el cartel los pinta al revés: uno es "Web prospectable"
  // (verde, adelante) y el otro "No pude consultar" (⚠️, verificá a mano). Confundirlos
  // convertiría cada web nueva en una alarma falsa, y el MB dejaría de mirar el cartel.
  const buscar = cargarBuscarEnCrm(async () => respuesta({ found: false }));
  const r = await buscar("sitio-nuevo-nunca-visto.com");
  strictEqual(r.found, false);
  ok(!r.indeterminado, "el CRM contestó: que no esté es una respuesta, no una falla");
});

test("una respuesta normal devuelve el estado y cuánto tardó", async () => {
  const buscarEnCrm = cargarBuscarEnCrm(async () => respuesta({ found: true, estado: "Live", ejecutivo: "a@b.com" }));
  const r = await buscarEnCrm("ejemplo.com");
  strictEqual(r.found, true);
  strictEqual(r.status, "Live");
  strictEqual(typeof r.ms, "number", "tiene que informar los milisegundos para poder mostrarlos");
});

test("un pedido que se cuelga NO deja la promesa colgada: corta y avisa", async () => {
  // El fetch que reproduce el bug: nunca resuelve por su cuenta. Sólo termina si alguien
  // aborta la señal — que es exactamente lo que el timeout tiene que hacer.
  const colgado = (_url, opts) => new Promise((_, rej) => {
    opts.signal.addEventListener("abort", () => rej(Object.assign(new Error("timeout"), { name: "TimeoutError" })));
  });
  const buscarEnCrm = cargarBuscarEnCrm(colgado);
  const t0 = Date.now();
  const r  = await buscarEnCrm("ejemplo.com");
  const ms = Date.now() - t0;
  strictEqual(r.indeterminado, true, "colgarse NUNCA puede leerse como 'la web está libre'");
  strictEqual(r.found, false);
  ok(/no contestó/.test(r.motivo || ""), `el motivo tiene que ser legible, vino "${r.motivo}"`);
  ok(ms < WATCHDOG_MS, `tardó ${ms}ms: tiene que cortar antes del watchdog (${WATCHDOG_MS}ms)`);
});

test("si el primer intento falla, reintenta y contesta bien", async () => {
  let n = 0;
  const buscarEnCrm = cargarBuscarEnCrm(async () => {
    if (++n === 1) throw new Error("network");
    return respuesta({ found: false });
  });
  const r = await buscarEnCrm("ejemplo.com");
  strictEqual(n, 2, "tiene que haber dos intentos");
  strictEqual(r.found, false);
  strictEqual(r.indeterminado, undefined, "un 'no está en el CRM' legítimo no es indeterminado");
});

test("un HTTP 500 no se confunde con 'no está en el CRM'", async () => {
  const buscarEnCrm = cargarBuscarEnCrm(async () => ({ ok: false, status: 500, json: async () => ({}) }));
  const r = await buscarEnCrm("ejemplo.com");
  strictEqual(r.indeterminado, true);
});

// ── Las tres piezas que hacen que el cartel no se pueda quedar mudo ─────────────────────
test("el veredicto se pide en un listener propio, no colgado del arranque grande", () => {
  const listeners = popup.match(/document\.addEventListener\("DOMContentLoaded"/g) || [];
  ok(listeners.length >= 2,
     "el chequeo del CRM tiene que ir en su propio DOMContentLoaded: si va en el grande, un throw previo se lo lleva puesto");
  ok(/_armarWatchdogCrm/.test(popup), "tiene que existir el watchdog que pinta 'no pude consultar' si nadie contestó");
  ok(/btn-crm-reintentar/.test(popup), "cuando falla tiene que haber un botón de reintentar");
});

test("una sola consulta por dominio: el chequeo temprano y la pipeline la comparten", () => {
  const cuerpo = extraer(popup, "async function runDuplicateCheck() {");
  ok(/_crmConsultar\(state\.domain\)/.test(cuerpo),
     "runDuplicateCheck tiene que usar _crmConsultar, no buscarEnCrm suelto: si no, son dos GET por apertura");
});

// ── Cambiar de URL tiene que dejar todo limpio y volver a llenar ────────────────────────
test("al cambiar de URL se desbloquea el borrador y se limpia el veredicto viejo", () => {
  const cuerpo = extraer(popup, "function resetAnalysisUI() {");
  ok(/state\.pitchTemplate\s*=\s*null/.test(cuerpo),
     "el borrador de la web anterior no puede seguir puesto");
  ok(/_desbloquearPitch\(\)/.test(cuerpo),
     "el textarea quedaba readOnly de la plantilla anterior: vacío Y trabado");
  ok(/state\.crmVeredicto\s*=\s*null/.test(cuerpo),
     "el veredicto de la web anterior no puede quedar en pantalla");
  ok(/_crmVuelo\s*=\s*null/.test(cuerpo),
     "la consulta cacheada es de otro dominio");
});

test("al cambiar de URL se vuelve a llenar el formulario y el borrador", () => {
  const cuerpo = extraer(popup, "function runAnalysisPipeline() {");
  ok(/autofillDraftOnLoad\(\)/.test(cuerpo),
     "faltaba: la pipeline re-corría los chequeos y no recargaba el borrador del nuevo idioma");
  ok(/runAutoFill\(\)/.test(cuerpo),
     "faltaba: el formulario del CRM se quedaba con los datos de la web anterior");
  ok(/_pitchIntacto\(\)/.test(cuerpo),
     "la segunda pasada no puede pisarle el texto que el MB escribió mientras corría el análisis");
});

// ── El botón de la plantilla del CRM ────────────────────────────────────────────────────
test("la plantilla del CRM se compara CRUDA, no con el dominio ya sustituido", () => {
  const cuerpo = extraer(popup, "function rotatePitchTemplate() {");
  ok(/t\.bodyRaw/.test(cuerpo),
     "comparar contra el body sustituido daba SIEMPRE distinto: el botón decía '✅ se actualizó' en cada click y el texto no cambiaba");
  ok(/applyCrmTemplate/.test(popup) && /bodyRaw:\s*String\(t\.body/.test(popup),
     "applyCrmTemplate tiene que guardar el body crudo además del resuelto");
});

// ── Si el CRM dice que no: no hay borrador, pero los datos SÍ se ven ─────────────────────
// La regla del 07/09 ("si no es prospectable, el borrador ni se carga") sigue en pie. Lo que
// cambió el 08/09 es el ALCANCE: bloqueaba 17 campos, incluidos los que sólo muestran datos y
// los que servían para salir del bloqueo (`pitch-country`, `btn-pitch-clear`). El user lo
// separó en dos frases: *"la carga de los datos de la tool tiene que ser de inmediato, sea que
// es una web prospectable o no"* y *"no debe cargar el borrador, y que en el campo aparezca que
// no carga porque es una web no prospectable"*.
test("una web no prospectable bloquea el borrador, no los datos", () => {
  const bloqueo = extraer(popup, "function _aplicarBloqueoCrm(v) {");
  ok(/_bloquearBorradorCrm\(/.test(bloqueo),
     "apagar los dos botones no alcanza: el MB veía el mail redactado listo para un cliente activo");
  const campos = extraer(popup, "function _bloquearBorradorCrm(bloquear) {");
  ok(/el\.disabled = bloquear/.test(campos), "los campos del mail se deshabilitan de verdad, no sólo se pintan");
  ok(/el\.value = ""/.test(campos), "el borrador que ya se había cargado hay que borrarlo");

  const lista = /const _CAMPOS_BORRADOR = \[([\s\S]*?)\];/.exec(popup);
  ok(lista, "tiene que existir la lista _CAMPOS_BORRADOR");
  for (const id of ["form-subject", "pitch-text", "btn-generate-pitch"]) {
    ok(lista[1].includes(`"${id}"`), `${id} es parte del mail: tiene que bloquearse`);
  }
  // Lo que NO puede volver a entrar en la lista: campos que sólo MUESTRAN el dato del CRM.
  // Bloquearlos fue el bug — el MB abría una web y no veía ni idioma ni país ni correo.
  for (const id of ["form-idioma", "form-geo", "form-ejecutivo", "form-estado", "form-email-search"]) {
    ok(!lista[1].includes(`"${id}"`),
       `${id} sólo muestra un dato: bloquearlo deja al MB sin ver el estado de la web que está mirando`);
  }
  // Y el motivo va en el placeholder, no en el value: un texto puesto como valor es un texto
  // que se puede terminar mandando.
  ok(/placeholder = /.test(campos), "el campo tiene que DECIR por qué está vacío");
  ok(/No se carga borrador: web no prospectable/.test(popup),
     "el cartel tiene que nombrar el motivo con las palabras del user");
});

test("el borrador no se carga para una web que el CRM frena", () => {
  const cuerpo = extraer(popup, "async function autofillDraftOnLoad() {");
  ok(/_crmBloquea\(\)/.test(cuerpo),
     "autofillDraftOnLoad tiene que cortar si el veredicto dice que no");
  const auto = extraer(popup, "function runAutoFill() {");
  ok(!/_crmBloquea\(\)/.test(auto),
     "runAutoFill NO puede cortar por el veredicto: los datos del CRM se muestran igual (regla del user, 08/09)");
  const cb = extraer(popup, "function _crmBloquea() {");
  ok(/!v\.ok && !v\.duda/.test(cb), "una duda avisa pero NO bloquea: sólo bloquea un 'no' del CRM");
});

// ── Los datos del CRM llegan al formulario ──────────────────────────────────────────────
// *"No carga los datos del CRM en idioma, país y correo."* (user, 08/09) Era literal: la ficha
// los devuelve y `runAutoFill` los ignoraba. Este test corre el `runAutoFill` REAL contra la
// respuesta REAL de `/api/crm/ficha?domain=elpais.com`, copiada de producción el 08/09.
const FICHA_ELPAIS = {
  found: true, status: "Ciclo Finalizado", ejecutivo: "dhorovitz@adeqmedia.com",
  email: "nsasser7@elpais.com", geo: "España", idioma: "Español",
  fecha: "2025-07-08", trafico: "300K", rebotado: false,
};

/** Un <select>/<input> de mentira, con lo justo que toca runAutoFill. */
function campo(opciones = []) {
  return { value: "", options: opciones.map(o => (typeof o === "string" ? { value: o, text: o } : o)) };
}
function correrAutoFill(dup, extra = {}) {
  const els = {
    "form-idioma": campo([{ value: "", text: "— select —" }, { value: "0", text: "English" },
                          { value: "1", text: "Spanish" }, { value: "de", text: "German" }]),
    "form-geo":    campo([{ value: "", text: "" }, { value: "España", text: "España" },
                          { value: "Alemania", text: "Alemania" }]),
    "form-email":  campo(),
    ...extra,
  };
  const cuerpo = extraer(popup, "function runAutoFill() {") + "\n" +
                 extraer(popup, "function _isoDeEtiquetaCrm(etiqueta) {");
  const fab = new Function(
    "document", "state", "IDIOMA_CRM", "LANG_TO_IDIOMA", "GEO_LABEL",
    "detectLangFromDomain", "_bestEmailByTier", "_etiquetaCrmAIso",
    `${cuerpo}; return runAutoFill;`,
  );
  fab(
    { getElementById: (id) => els[id] || null, querySelector: () => null },
    { duplicate: dup, domain: "elpais.com", siteLanguage: "", trafficData: null, emails: [] },
    { en: "Ingles", es: "Español", it: "Italiano", de: "Aleman" },
    { en: "0", es: "1", it: "2", pt: "3", ar: "6" },
    { ES: "España", DE: "Alemania" },
    () => "", () => "", null,
  )();
  return els;
}

test("la ficha del CRM llena idioma, país y correo", () => {
  const els = correrAutoFill(FICHA_ELPAIS);
  strictEqual(els["form-idioma"].value, "1", 'el CRM dijo "Español" y el select quedó vacío');
  strictEqual(els["form-geo"].value, "España", 'el CRM dijo top_geo "España" y el select quedó vacío');
  strictEqual(els["form-email"].value, "nsasser7@elpais.com", "el correo del CRM tiene que llegar al formulario");
});

test("un idioma sin índice de Monday igual se completa, por ISO", () => {
  // `LANG_TO_IDIOMA` sólo conoce los 5 heredados de Monday. Los 18 que se agregaron el 04/09
  // van por ISO en el <select>: sin probar el ISO, un sitio alemán no completaba nada.
  const els = correrAutoFill({ ...FICHA_ELPAIS, idioma: "Aleman", geo: "Alemania" });
  strictEqual(els["form-idioma"].value, "de", "el alemán no tiene índice viejo: tiene que entrar por su ISO");
});

test("una web contactada anteayer también muestra sus datos", () => {
  // El portón `shouldFill` sólo dejaba pasar sitios nuevos o de hace más de 30 días — justo al
  // revés de donde el CRM tiene el dato bueno.
  const els = correrAutoFill({ ...FICHA_ELPAIS, fecha: new Date().toISOString().slice(0, 10) });
  strictEqual(els["form-idioma"].value, "1", "un contacto reciente no puede dejar el formulario vacío");
  strictEqual(els["form-email"].value, "nsasser7@elpais.com");
});

test("un correo que rebotó no se autocompleta", () => {
  const els = correrAutoFill({ ...FICHA_ELPAIS, rebotado: true });
  strictEqual(els["form-email"].value, "", "una dirección muerta no se ofrece por más registrada que esté");
});

test("lo que el MB ya eligió no se pisa", () => {
  const mio = campo([{ value: "0", text: "English" }, { value: "1", text: "Spanish" }]);
  mio.value = "0";
  const els = correrAutoFill(FICHA_ELPAIS, { "form-idioma": mio });
  strictEqual(els["form-idioma"].value, "0", "si el MB eligió, manda el MB");
});

// ── El idioma del borrador ──────────────────────────────────────────────────────────────
test("el idioma sale del TLD antes de caer al inglés", () => {
  // El commit del 07/09 justificó cargar el borrador antes del análisis diciendo que "el
  // idioma sale del TLD". No era cierto: `_resolvePitchLang` nunca miró el dominio, así que en
  // la primera pasada —sin <html lang>, sin og:locale, sin texto y sin GEO— caía al default y
  // el MB veía un borrador EN INGLÉS sobre un diario español.
  const cuerpo = extraer(popup, "function _resolvePitchLang() {");
  const iTld = cuerpo.indexOf("detectLangFromDomain");
  const iEn  = cuerpo.lastIndexOf('return "en"');
  ok(iTld > 0, "_resolvePitchLang tiene que consultar el TLD");
  ok(iTld < iEn, "el TLD se consulta ANTES del default inglés, o no sirve de nada");
});

test("el TLD conoce más de tres idiomas", () => {
  const cuerpo = extraer(popup, "function detectLangFromDomain(domain) {");
  ok(/GEO_TO_LANG/.test(cuerpo),
     "TLD_TO_LANG sólo lista es/pt/it: un .de o un .fr no daban idioma. GEO_TO_LANG ya tiene los 23");
});

// ── Las dos vías del mail, y que no se mezclen ──────────────────────────────────────────
// La regla completa del user (08/09), textual:
//   · *"Por default siempre se carga al azar 1 de los 3 del CRM en el idioma local detectado.
//      Eso no se puede modificar."*
//   · *"Si el MB no quiere enviar eso, le tiene que dar a Limpiar y luego redactar a mano su
//      mail o elegir un país del filtro que carga los pitch draft."*
//   · *"Es una opción u otra."*
test("por default va una de las 3 del CRM en el idioma local, y bloqueada", () => {
  const auto = extraer(popup, "async function autofillDraftOnLoad() {");
  const iCrm = auto.indexOf("_crmTpl.byLang.get(lang)");
  const iPropios = auto.indexOf("_draftsState.byLang.get(lang)");
  ok(iCrm > 0, "la carga automática tiene que salir de las plantillas del CRM");
  ok(iCrm < iPropios, "el CRM manda por default; el borrador propio es sólo el respaldo si no hay plantilla");
  ok(/_semillaRotacion\(crm\.length\)/.test(auto),
     "una de las 3 al azar, no siempre la primera");
  const aplicar = extraer(popup, "function applyCrmTemplate(t, lang) {");
  ok(/_bloquearPitch\(\)/.test(aplicar), '"Eso no se puede modificar": la plantilla del CRM se aplica bloqueada');
});

test("Limpiar vacía el mail Y el asunto: es rechazar la propuesta del CRM", () => {
  const boot = popup.slice(popup.indexOf("function initPitchInlineControls() {"));
  const clear = boot.slice(boot.indexOf("clearBtn?.addEventListener"), boot.indexOf("pitchEl?.addEventListener"));
  ok(/pitchEl\.value = ""/.test(clear), "Limpiar tiene que vaciar el cuerpo");
  ok(/subjEl\.value = ""/.test(clear),
     "y el asunto: reponer el del CRM sería devolver lo que el MB acaba de rechazar");
  ok(/_desbloquearPitch\(\)/.test(clear), "y desbloquear, que es el único camino para escribir el propio");
});

// ── Elegir país ─────────────────────────────────────────────────────────────────────────
// *"Elegir un país del filtro que carga los pitch draft."* Son 3 por idioma, en
// `toolbar_pitch_drafts`. Antes no aparecían porque `pitch-country` y `btn-pitch-flag` estaban
// deshabilitados por el candado del veredicto.
test("elegir país carga TUS pitch drafts, y NUNCA una del CRM", () => {
  const desde = popup.indexOf("// ── ELEGIR PAÍS = TUS PITCH DRAFTS");
  ok(desde > 0, "tiene que existir el bloque del filtro de país");
  const handler = popup.slice(desde, popup.indexOf("});", desde));
  ok(/_draftsState\.byLang\.get\(l\)/.test(handler), "la fuente son los pitch drafts propios");
  // El orden de las dos vías, textual: *"Primero los mails del CRM siempre, y si el MB no
  // quiere, pone sus template."* Se llega acá DESPUÉS de rechazar la propuesta del CRM con
  // 🗑️ Limpiar: devolver una plantilla del CRM sería deshacer esa decisión sin avisar, y
  // encima bloqueada, que es como se aplican las del CRM.
  ok(!/applyCrmTemplate/.test(handler),
     "es una opción u otra: el filtro de país no puede devolver una plantilla del CRM");
  // Y sin borrador propio no se calla: el bug original no era el vacío, era el silencio.
  ok(/No tenés pitch draft en/.test(handler),
     "sin pitch draft en ese idioma hay que decirlo y dejar el recuadro libre para escribir");
  ok(/_desbloquearPitch\(\)/.test(handler), "y desbloquear, para poder escribirlo a mano");
});

test("una vía carga el asunto o la otra, pero el campo no queda vacío por accidente", () => {
  ok(/function _asuntoPorDefecto\(lang\) {/.test(popup), "tiene que existir el asunto por default");
  const crm = extraer(popup, "function applyCrmTemplate(t, lang) {");
  ok(/subjectEl\.value = subject \|\| _asuntoPorDefecto/.test(crm),
     "una plantilla del CRM sin asunto no puede dejar el campo vacío");
  const draft = extraer(popup, "function applyDraftToPitch(d, { silent = false } = {}) {");
  ok(/subjectEl\.value = subject \|\| _asuntoPorDefecto/.test(draft),
     "un pitch draft sin asunto no puede dejar el campo vacío");
  // El de emergencia no se inventa: sale de una plantilla real, y prioriza la del MB.
  const def = extraer(popup, "function _asuntoPorDefecto(lang) {");
  const iPropios = def.indexOf("_draftsState.byLang.get(lang)");
  const iCrm     = def.indexOf("_crmTpl.byLang.get(lang)");
  ok(iPropios > 0 && iCrm > 0 && iPropios < iCrm,
     "el respaldo prioriza el borrador del MB: se usa en la vía que arranca con Limpiar");
});

// ── Los campos no esperan al análisis ───────────────────────────────────────────────────
// *"Los campos se completan todos rápidamente."* (user, 08/09) La ficha contesta en 0,4-0,66s;
// el análisis completo tarda mucho más. Llenar el formulario recién al final era la diferencia
// entre medio segundo y varios.
test("el formulario se llena en cuanto contesta el CRM, no al final del análisis", () => {
  const cuerpo = extraer(popup, "function runAnalysisPipeline() {");
  const iChequeo = cuerpo.indexOf("const chequeoCrm = runDuplicateCheck()");
  ok(iChequeo > 0, "el chequeo del CRM tiene que quedar referenciado para poder colgarse de él");
  const colgado = cuerpo.slice(iChequeo, cuerpo.indexOf("const tasks = ["));
  ok(/runAutoFill\(\)/.test(colgado) && /autofillDraftOnLoad\(\)/.test(colgado),
     "los campos y el borrador se llenan al contestar el CRM, no después de SimilarWeb y el scraper");
  ok(/state\.domain !== startedDomain/.test(colgado),
     "si el MB ya navegó a otra web, ese resultado no puede escribir el formulario");
});

// ── El estado de la web se sabe enseguida, siempre ──────────────────────────────────────
// *"El MB debe saber enseguida el status de la web que está viendo."* (user, 08/09)
test("el recuadro del CRM nunca se queda con el 'Checking...' del HTML", () => {
  const temprano = popup.slice(popup.indexOf("// ── EL VEREDICTO ARRANCA PRIMERO Y POR SU CUENTA"));
  const listener = temprano.slice(temprano.indexOf('document.addEventListener("DOMContentLoaded"'));
  const hastaPrimerAwait = listener.slice(0, listener.indexOf("await "));
  ok(/_pintarEsperaCrm\(\)/.test(hastaPrimerAwait) && /_armarWatchdogCrm\(\)/.test(hastaPrimerAwait),
     "el cartel y el watchdog van ANTES del primer await: con ellos después, cualquier salida temprana deja el 'Checking...' muerto del HTML");
  // Las dos respuestas empiezan igual para que la diferencia salte a la vista.
  ok(/titulo: "Web prospectable"/.test(popup) && /titulo: "Web NO prospectable"/.test(popup),
     'el veredicto se dice con todas las letras: "Web prospectable" / "Web NO prospectable"');
});

// ── Un solo camino de análisis ──────────────────────────────────────────────────────────
// El bug que hizo que el user reportara *"todas estas correcciones no están funcionando"*: el
// arranque tenía su PROPIA copia del pipeline, así que lo que se arreglaba en
// `runAnalysisPipeline` sólo corría al navegar de una URL a otra, nunca al abrir la toolbar.
test("el arranque usa runAnalysisPipeline, no una copia propia", () => {
  const boot = popup.slice(popup.indexOf("// ── UN SOLO CAMINO DE ANÁLISIS"));
  ok(/runAnalysisPipeline\(\)\.then\(/.test(boot.slice(0, 2000)),
     "el arranque tiene que llamar al pipeline compartido y esperarlo");
  // Si vuelve a aparecer un Promise.all con los seis chequeos, es la copia otra vez.
  const copias = popup.match(/runDuplicateCheck\(\)\s*\.?\s*catch/g) || [];
  ok(copias.length <= 1,
     `hay ${copias.length} lugares que arrancan runDuplicateCheck: tiene que haber uno solo, o los arreglos se aplican a un camino y no al otro`);
  ok(/return Promise\.all\(tasks\)/.test(popup),
     "runAnalysisPipeline tiene que devolver la promesa para que el arranque pueda encadenar la caché de sesión");
});

// ── El rotador de borradores sólo lo abre el país ───────────────────────────────────────
test("sin país elegido el botón no rota nada: el mail del CRM lo decide el sistema", () => {
  const cuerpo = extraer(popup, "function rotatePitchTemplate() {");
  ok(/!_draftsState\.paisElegido/.test(cuerpo),
     "regla del user: ni Limpiar habilita pasar entre 1/3, 2/3 — sólo elegir un país");
  ok(/paisElegido: false/.test(popup), "_draftsState tiene que arrancar sin país elegido");
  ok(/_draftsState\.paisElegido = true/.test(popup),
     "elegir un país es lo único que habilita el rotador");
  const reset = extraer(popup, "function resetAnalysisUI() {");
  ok(/paisElegido = false/.test(reset), "al cambiar de web se vuelve a lo que manda el CRM");
});

test("el mensaje del botón dice qué hacer, no sólo que no se puede", () => {
  const cuerpo = extraer(popup, "function rotatePitchTemplate() {");
  ok(/no se cambia a mano/.test(cuerpo), "tiene que decir que la plantilla del CRM no se cambia");
  ok(/Limpiar/.test(cuerpo) && /país/.test(cuerpo),
     "tiene que ofrecer las dos salidas: 🗑️ Limpiar para escribir, o elegir país para el borrador propio");
});
