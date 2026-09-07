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
import { ok, strictEqual } from "node:assert";
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
  let n = 0, j = fuente.indexOf("{", i);
  for (let k = j; k < fuente.length; k++) {
    if (fuente[k] === "{") n++;
    else if (fuente[k] === "}" && --n === 0) return fuente.slice(i, k + 1);
  }
  throw new Error(`no cerró la llave de ${firma}`);
}

const FUENTE_TIMEOUT = /const _CRM_FICHA_TIMEOUT_MS = (\d+);/.exec(popup);
ok(FUENTE_TIMEOUT, "popup.js tiene que declarar _CRM_FICHA_TIMEOUT_MS");
const TIMEOUT_MS = Number(FUENTE_TIMEOUT[1]);

/** Devuelve el buscarEnCrm real, con sus dependencias inyectadas. */
function cargarBuscarEnCrm(fetchFalso) {
  const cuerpo = extraer(popup, "async function buscarEnCrm(domain) {");
  const fab = new Function(
    "fetch", "AbortSignal", "console", "crmUrl", "CONFIG", "_CRM_FICHA_TIMEOUT_MS",
    `${cuerpo}; return buscarEnCrm;`,
  );
  return fab(
    fetchFalso, AbortSignal, { warn() {} },
    () => "https://console.adeqmedia.com/api/crm/ficha",
    { CRM_BOARD_SECRET: "x" },
    TIMEOUT_MS,
  );
}

const respuesta = (json) => ({ ok: true, json: async () => json });

test("el timeout está en 2,5s: dos intentos entran holgados en los 6s del watchdog", () => {
  ok(TIMEOUT_MS <= 3000, `el timeout es ${TIMEOUT_MS}ms y el user pidió 2-3 segundos como máximo`);
  ok(TIMEOUT_MS * 2 < 6000, "dos intentos tienen que caber en el watchdog de 6s");
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
  ok(ms < 6000, `tardó ${ms}ms: tiene que cortar antes del watchdog de 6s`);
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

test("el mensaje del botón dice qué hacer, no sólo que no se puede", () => {
  const cuerpo = extraer(popup, "function rotatePitchTemplate() {");
  ok(/no se cambia a mano/.test(cuerpo), "tiene que decir que la plantilla del CRM no se cambia");
  ok(/Limpiar/.test(cuerpo) && /país/.test(cuerpo),
     "tiene que ofrecer las dos salidas: 🗑️ Limpiar para escribir, o elegir país para el borrador propio");
});
