// Lo que corrigieron los tres mails del 09, 10 y 11 de septiembre. (2026-09-11, pedido del user)
//
// "Te paso los emails recibidos para que hagas las correcciones que tengas que hacer". Cada
// test de acá es un renglón de esos mails que era falso, ilegible o un gasto repetido:
//   · un pitch en japonés frenado como `cuerpo_muy_corto_6` (no hay espacios entre palabras),
//   · "[object object]" como la vía de email más grande del pool (parte-embudo.test.js),
//   · el barrido de ciclos cerrados pidiéndole a Monday, muerto desde el 02/09,
//   · AutoGoogle gritando "se paga Serper para nada" cuando el lote quedó estacionado,
//   · 269 envíos salteados por `mv_dudoso` re-pagando la misma verificación tras cada reinicio,
//   · dos latidos que sólo se mandaban al fallar y se leían como "trabajo sin correr".
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { revisarEntregabilidad } from "../lib/email.js";
import { cargarWorker } from "./_worker-exportado.mjs";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const entre = (desde, hasta) => worker.slice(worker.indexOf(desde), worker.indexOf(hasta, worker.indexOf(desde)));

// ── 1. El linter y las escrituras sin espacios ──────────────────────────────────────────
test("un pitch en japonés de largo normal no es 'cuerpo muy corto'", () => {
  // 6 "palabras" para el split por espacios, ~190 caracteres reales. Es el mail de herseyshiga.com.
  const jp = "はじめまして。ADEQ Mediaの担当者です。貴サイトを拝見し、広告収益の改善についてご提案できることがあると感じました。"
           + "私たちは欧州と中南米のパブリッシャー向けに、プログラマティック広告の収益最適化を行っています。"
           + "貴サイトの現在の広告構成を拝見したところ、いくつか改善の余地があると考えております。ぜひ一度お話しさせていただけないでしょうか。";
  const r = revisarEntregabilidad({ to: "info@herseyshiga.com", subject: "サイトを拝見して気づいた点があります", body: jp, cuerpo: jp });
  ok(!r.bloqueantes.some(b => b.startsWith("cuerpo_muy_corto")), `se frenó: ${r.bloqueantes.join(", ")}`);
});

test("un cuerpo realmente corto sigue frenado, en cualquier idioma", () => {
  const es = revisarEntregabilidad({ to: "a@b.com", subject: "Hola", body: "Hola, ¿hablamos mañana?", cuerpo: "Hola, ¿hablamos mañana?" });
  ok(es.bloqueantes.some(b => b.startsWith("cuerpo_muy_corto")), "seis palabras en castellano son pocas");
  const jp = revisarEntregabilidad({ to: "a@b.com", subject: "件名", body: "明日お話しできますか。", cuerpo: "明日お話しできますか。" });
  ok(jp.bloqueantes.some(b => b.startsWith("cuerpo_muy_corto")), "once caracteres en japonés también son pocos");
});

// ── 2. El barrido diario de ciclos cerrados lee el CRM ──────────────────────────────────
test("sincronizarFinalizadosDeMonday ya no le pide nada a Monday: lee /api/crm/reciclables", () => {
  const fn = entre("async function sincronizarFinalizadosDeMonday(", "async function _dominiosContactadosDesde(");
  ok(!/api\.monday\.com/.test(fn), "Monday está cortado desde el 02/09: pedirle al board dio 'sin api key' nueve días seguidos");
  ok(!/_getMondayApiKeyForFeeder/.test(fn), "sin api key tampoco: la fuente es el CRM");
  ok(/_urlCrm\("\/reciclables"\)/.test(fn) && /x-toolbar-secret/.test(fn), "tiene que leer el endpoint del CRM con el secreto");
  ok(/AbortSignal\.timeout\(/.test(fn), "y con reloj: un fetch sin timeout es EL bug recurrente");
  ok(/setConfigValue\(token, "monday_sync_ultimo"/.test(fn), "sigue escribiendo el resumen que lee el parte y el resumen de salud");
  ok(/monday_enabled/.test(fn) === false, "monday_enabled ya no lo apaga: el CRM es la fuente aunque Monday esté muerto");
});

test("el barrido respeta el carril que comparte con el feeder, y lo deja escrito", () => {
  const fn = entre("async function sincronizarFinalizadosDeMonday(", "async function _dominiosContactadosDesde(");
  ok(/_capDeFuente\("auto_feeder_monday"\)/.test(fn) && /_countActiveCsvBySource\(token, "auto_feeder_monday"\)/.test(fn),
     "si el carril está lleno, encolar 0 es lo esperado y no una falla");
  ok(/Math\.min\(_techoDia, _libre\)/.test(fn), "los candidatos se cortan por el techo Y por el cupo libre");
  ok(/libre: _libre/.test(fn), "el cupo libre queda en monday_sync_ultimo para que el resumen de salud no pinte amarillo por diseño");
  const salud = entre("// ── MONDAY (reciclado hacia cero)", "// ── RE-TRABAJO");
  ok(/_mSync\.libre/.test(salud), "el resumen de salud tiene que mirar `libre` al decidir si el barrido cumplió");
});

// ── 3. AutoGoogle: "cero encolados" sólo alarma si se perdió algo ───────────────────────
test("la inyección a la cola cuenta qué pasó con lo que no entró", async () => {
  const { _injectIntoCsvQueue } = await cargarWorker(["_injectIntoCsvQueue"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url);
    pedidos.push({ u, m: (opts.method || "GET").toUpperCase() });
    // El carril de autogoogle (180) está lleno: 999 activos.
    if (u.includes("toolbar_csv_queue?status=in.(pending,processing,waiting_pool)&source=eq.autogoogle")) {
      return { ok: true, status: 200, headers: { get: (k) => k.toLowerCase() === "content-range" ? "0-0/999" : null }, json: async () => [], text: async () => "[]" };
    }
    return { ok: true, status: 201, headers: { get: () => null }, json: async () => [], text: async () => "[]" };
  };
  const resumen = {};
  const n = await _injectIntoCsvQueue("token-falso", ["diario-nuevo.pe", "radio-nueva.co"], "autogoogle", { parkOverflow: true, resumen });
  strictEqual(n, 0, "no entró ninguno (carril lleno)");
  deepStrictEqual(resumen, { estacionados: 2, sinAds: 0, encolados: 0, carrilLleno: true, rechazadoPorCola: false },
    "los dos quedaron ESTACIONADOS: no se perdió nada, y el llamador tiene que poder saberlo");
  ok(pedidos.some(p => p.u.includes("toolbar_discovery_backlog") && p.m === "POST"), "se estacionaron en el pre-listado");
});

test("AutoGoogle alarma sólo si hay dominios perdidos o la cola rechazó el lote", () => {
  const fn = entre("const _resInj = {};", "async function _getMondayApiKeyForFeeder(");
  ok(/resumen: _resInj/.test(fn), "la inyección recibe el resumen");
  ok(/const _perdidos = Math\.max\(0, freshCount - inserted - _estacionados - _sinAds\)/.test(fn), "perdido = fresco − encolado − estacionado − sin ads.txt");
  ok(/inserted === 0 && \(_resInj\.rechazadoPorCola \|\| _perdidos > 0\)/.test(fn),
     "el 10/09 gritó tres veces con 54 frescos estacionados: eso no es 'pagar Serper para nada'");
  ok(/real: inserted \+ _estacionados/.test(fn), "lo estacionado es dominio pagado que NO se perdió: cuenta como entregado");
});

// ── 4. MillionVerifier: el veredicto se guarda y se reusa ───────────────────────────────
test("el estado de MillionVerifier sale de una sola tabla", async () => {
  const { _mvEstadoDe } = await cargarWorker(["_mvEstadoDe"]);
  strictEqual(_mvEstadoDe("invalid"), "no");
  strictEqual(_mvEstadoDe("disposable"), "no");
  strictEqual(_mvEstadoDe("unknown"), "dudoso");
  strictEqual(_mvEstadoDe("risky"), "dudoso");
  strictEqual(_mvEstadoDe("catch_all"), "riesgo");
  strictEqual(_mvEstadoDe("ok"), "ok");
  strictEqual(_mvEstadoDe(""), null, "sin resultado guardado no hay veredicto: se pregunta");
});

test("antes de pagarle a MillionVerifier se mira lo que ya contestó (el caché en memoria muere con cada reinicio)", async () => {
  const fn = entre("async function _verifyEmailMV(", "async function _mvResultadoGuardado(") + entre("async function _mvResultadoGuardado(", "\n}\n");
  ok(/_mvResultadoGuardado\(token, lower\)/.test(entre("async function _verifyEmailMV(", "const day = _madridNowParts().dateISO;")),
     "se consulta ANTES de contar contra el tope diario y antes del fetch a la API");
  ok(/toolbar_mv_results\?email=eq\./.test(fn) && /order=id\.desc&limit=1/.test(fn), "el último veredicto de esa dirección");
  ok(/MV_RESULTADO_VALIDO_DIAS = 30/.test(worker), "un veredicto vale un mes");
  // Y funcional: con un resultado guardado, NO se llama a millionverifier.com.
  const { _verifyEmailMV } = await cargarWorker(["_verifyEmailMV"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = async (url) => {
    const u = String(url); pedidos.push(u);
    if (u.includes("toolbar_mv_results?email=eq.")) return { ok: true, status: 200, headers: { get: () => null }, json: async () => [{ result: "unknown" }], text: async () => "" };
    return { ok: true, status: 200, headers: { get: () => null }, json: async () => ({}), text: async () => "" };
  };
  const estado = await _verifyEmailMV("token-falso", { millionverifier_api_key: "clave-falsa" }, "editor@nerdin.com.br");
  strictEqual(estado, "dudoso", "el veredicto guardado manda");
  ok(!pedidos.some(u => u.includes("millionverifier.com")), `se volvió a pagar la verificación: ${pedidos.join(" | ")}`);
});

// ── 5. Dos latidos que sólo salían al fallar ────────────────────────────────────────────
test("lista_no_recontactar y stock_envios laten también cuando están bien (o apagados)", () => {
  const bloq = entre("async function _bloqueadosDelCrm(", "\n}\n");
  ok(/saludPing\(token, "lista_no_recontactar", \{\s*status: "ok"/.test(bloq), "trece días sanos se leían como 'trabajo sin correr'");
  ok(/_bloqCrmUltimoLatidoOk/.test(bloq), "y no en cada lectura (cada 10 min): como mucho cada 6 h");
  const stock = entre("let _recorteStock = null;", "// ── EL ÚLTIMO MB YA NO SE MUERE DE HAMBRE");
  ok(/saludPing\(token, "stock_envios", \{ status: "fail"/.test(stock), "si no pudo contar el stock, lo dice en vez de callarse");
  ok(/saludApagado\(token, "stock_envios", "agent_regular_por_stock=false"\)/.test(stock), "apagado a propósito ≠ sin correr");
  ok(/AbortSignal\.timeout\(10000\)/.test(stock), "y el conteo tiene reloj");
});

// ── 6. Apollo: el job "atrasado" tiene de dónde elegir ──────────────────────────────────
test("apolloQuemarCiclo mira 600 pendientes, no 120", () => {
  const fn = entre("async function apolloQuemarCiclo(", "// BARRIDO DE PROSPECTS");
  ok(/order=traffic\.desc\.nullslast&limit=600/.test(fn), "con 120 los de más tráfico ya tenían persona y el job decía 'candidatos 0' estando atrasado");
});

// ── 7. Lo que apareció al releer los "reales" ───────────────────────────────────────────
test("el agente no vuelve a recorrer cada día los leads que ya salteó por MV dudoso", () => {
  const fn = entre("async function runAgentCycle(", "const _conEmail = fresh.filter(_tieneEmail).length;");
  ok(/reason=eq\.mv_dudoso&created_at=gte\./.test(fn), "se leen los salteados por mv_dudoso de la última semana");
  ok(/fresh = fresh\.filter\(l => !_saltadosMv7d\.has\(/.test(fn), "y se sacan del lote antes de empezar, como los contactados en 30 días");
  ok(/7 \* 86400_000/.test(fn), "siete días: el veredicto de MV vale un mes, pero el re-enrich puede traer otra dirección");
});

test("el barrido de no-publishers mide 'revisé el lote', no 'marqué todo lo que revisé'", () => {
  const fn = entre("async function barridoNoPublisher(", "// QUEMAR EL CICLO DE APOLLO");
  ok(/real: revisados, esperado: rows\.length/.test(fn), "con real=marcados/esperado=revisados estuvo 13 días 'rindiendo por debajo' sin que nada fallara");
});
