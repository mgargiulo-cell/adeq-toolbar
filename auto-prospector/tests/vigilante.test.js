// El vigilante: las reglas que deciden qué sale en el resumen de salud. (2026-09-13, pedido del user)
//
// "A diario van llegando los resultados y a diario estamos haciendo cambios… lo que arreglás un
// día lo rompés al otro. La idea es optimizar, pero dejar ya una base de todos los conceptos,
// lógicas y funcionamientos automáticos que no tengan fallas."
//
// Los mails de salud del 09 al 12/09 trajeron un aviso falso distinto cada día, y casi todos
// salían de las mismas reglas del vigilante. Este archivo las fija con escenarios de hora
// concretos, para que el próximo cambio que las rompa falle ACÁ y no en el mail de mañana:
//   1. un job se juzga contra el horario en que PUEDE correr (slots del agente, fin de semana),
//   2. un job que sólo late al fallar no "nunca corrió",
//   3. los números de un latido describen ESA corrida: un latido sin números los borra,
//   4. cada job tiene su propia alerta, así lo nuevo se ve como nuevo,
//   5. la cadencia declarada alcanza para el hueco entre slots, y cada job declara una sola,
//   6. "no pude leer" nunca es "cero", y un contador se lee con una sola función.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as acorn from "acorn";
import * as walk from "acorn-walk";
import { cargarWorker } from "./_worker-exportado.mjs";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const entre = (desde, hasta) => worker.slice(worker.indexOf(desde), worker.indexOf(hasta, worker.indexOf(desde)));
const ast = acorn.parse(worker, { ecmaVersion: "latest", sourceType: "module" });

const { _clasificarLatidos } = await cargarWorker(["_clasificarLatidos"]);
// Septiembre: Madrid = UTC+2.
const madrid = (s) => Date.parse(`${s}:00+02:00`);
const iso = (s) => new Date(madrid(s)).toISOString();
const diaMadrid = (ts) => new Intl.DateTimeFormat("en-US", { timeZone: "Europe/Madrid", weekday: "short" }).format(new Date(ts));
const nombres = (lista) => lista.map(x => x.job);

test("las fechas de los escenarios caen en el día de la semana que dicen", () => {
  strictEqual(diaMadrid(madrid("2026-09-11T17:30")), "Fri");
  strictEqual(diaMadrid(madrid("2026-09-12T15:00")), "Sat");
  strictEqual(diaMadrid(madrid("2026-09-14T12:30")), "Mon");
  strictEqual(diaMadrid(madrid("2026-09-15T12:30")), "Tue");
});

// ── 1. El horario en que el job PUEDE correr ────────────────────────────────────────────
const delAgente = (okEn, cadencia = 240) => ({ job: "stock_envios", esperado_cada_min: cadencia, last_ok_at: iso(okEn), last_status: "ok" });

test("un job del agente (slots 13-17) NO está atrasado a la mañana siguiente, antes del primer slot", () => {
  const r = _clasificarLatidos([delAgente("2026-09-14T17:30")], { ahora: madrid("2026-09-15T12:30") });
  deepStrictEqual(nombres(r.atrasados), [], JSON.stringify(r.atrasados));
});

test("con la cadencia vieja (60) ese mismo caso SÍ salía atrasado: es el falso de 14 días de stock_envios", () => {
  const r = _clasificarLatidos([delAgente("2026-09-14T17:30", 60)], { ahora: madrid("2026-09-15T12:30") });
  deepStrictEqual(nombres(r.atrasados), ["stock_envios"]);
});

test("pero un job del agente SÍ está atrasado si pasó un día hábil entero sin correr", () => {
  const r = _clasificarLatidos([delAgente("2026-09-14T17:30")], { ahora: madrid("2026-09-15T22:00") });
  deepStrictEqual(nombres(r.atrasados), ["stock_envios"]);
});

test("el fin de semana no cuenta: viernes 17:30 → lunes 12:30 está al día", () => {
  const r = _clasificarLatidos([delAgente("2026-09-11T17:30")], { ahora: madrid("2026-09-14T12:30") });
  deepStrictEqual(nombres(r.atrasados), []);
});

test("un sábado no suma minutos de 'hoy', ni para un job de cada 30 minutos", () => {
  const cola = { job: "csv_queue", esperado_cada_min: 30, last_ok_at: iso("2026-09-11T22:50"), last_status: "ok" };
  deepStrictEqual(nombres(_clasificarLatidos([cola], { ahora: madrid("2026-09-12T15:00") }).atrasados), []);
});

test("un job diario caído se ve a los tres días hábiles, no a la semana", () => {
  const diario = { job: "purga_cola", esperado_cada_min: 1440, last_ok_at: iso("2026-09-14T10:00"), last_status: "ok" };
  deepStrictEqual(nombres(_clasificarLatidos([diario], { ahora: madrid("2026-09-16T22:00") }).atrasados), [], "miércoles a la noche: todavía no");
  deepStrictEqual(nombres(_clasificarLatidos([diario], { ahora: madrid("2026-09-17T22:00") }).atrasados), ["purga_cola"], "jueves a la noche: sí");
});

// ── 2. Un job que sólo late cuando falla ────────────────────────────────────────────────
test("un job que sólo late al fallar no 'nunca corrió': se juzga por sus fallos seguidos", () => {
  const soloFalla = (n) => ({ job: "guardar_rebote", esperado_cada_min: 60, last_ok_at: null, last_status: "fail", fails_consecutivos: n, last_detail: "timeout" });
  const uno = _clasificarLatidos([soloFalla(1)], { ahora: madrid("2026-09-15T15:00") });
  deepStrictEqual([nombres(uno.atrasados), nombres(uno.fallando)], [[], []], "un fallo aislado no es noticia");
  const tres = _clasificarLatidos([soloFalla(3)], { ahora: madrid("2026-09-15T15:00") });
  deepStrictEqual([nombres(tres.atrasados), nombres(tres.fallando)], [[], ["guardar_rebote"]], "tres seguidos sí, como fallando y no como 'nunca corrió'");
});

test("un job con horario que nunca latió se reporta, pero sólo con la ventana abierta hace más de 2 h", () => {
  const nuevo = { job: "job_nuevo", esperado_cada_min: 240, last_ok_at: null, last_status: null };
  deepStrictEqual(nombres(_clasificarLatidos([nuevo], { ahora: madrid("2026-09-15T10:30") }).atrasados), [], "al arrancar, todo está sin correr por definición");
  deepStrictEqual(nombres(_clasificarLatidos([nuevo], { ahora: madrid("2026-09-15T15:00") }).atrasados), ["job_nuevo"]);
});

test("apagado a propósito no está roto, por viejo que sea su último latido", () => {
  const off = { job: "similar_expansion", esperado_cada_min: 60, last_ok_at: "2026-08-01T10:00:00Z", last_status: "off", real_ultimo: 0, esperado_ultimo: 9, fails_consecutivos: 5 };
  const r = _clasificarLatidos([off], { ahora: madrid("2026-09-15T15:00") });
  deepStrictEqual([r.atrasados.length, r.fallando.length, r.rindenPoco.length], [0, 0, 0]);
});

// ── 3. Los números describen la corrida del último latido ───────────────────────────────
test("rinde poco sólo si el último latido trajo números y real < la mitad", () => {
  const base = { esperado_cada_min: 240, last_ok_at: iso("2026-09-15T14:00"), last_status: "ok" };
  const r = _clasificarLatidos([
    { ...base, job: "barrido_no_publisher", real_ultimo: null, esperado_ultimo: null },   // "tope diario, sigue mañana"
    { ...base, job: "autogoogle", real_ultimo: 0, esperado_ultimo: 3 },
    { ...base, job: "polish_pool", real_ultimo: 12, esperado_ultimo: 20 },
  ], { ahora: madrid("2026-09-15T15:00") });
  deepStrictEqual(nombres(r.rindenPoco), ["autogoogle"]);
});

test("saludPing borra los números cuando el latido no los trae (el barrido estuvo 14 días 'por debajo' con cifras viejas)", async () => {
  const { saludPing } = await cargarWorker(["saludPing"], { fetchFalso: true });
  const cuerpos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    if (String(url).includes("/rest/v1/toolbar_health") && (opts.method || "GET") === "POST") cuerpos.push(JSON.parse(opts.body));
    return { ok: true, status: 201, headers: { get: () => null }, json: async () => [], text: async () => "" };
  };
  await saludPing("token-falso", "job_de_prueba", { status: "ok", cadenciaMin: 240, real: 3, esperado: 25 });
  await saludPing("token-falso", "job_de_prueba", { status: "warn", cadenciaMin: 240, detalle: "tope diario — sigue mañana" });
  strictEqual(cuerpos.length, 2, "se tienen que escribir los dos latidos");
  strictEqual(cuerpos[0].real_ultimo, 3);
  strictEqual(cuerpos[0].esperado_ultimo, 25);
  ok("real_ultimo" in cuerpos[1] && cuerpos[1].real_ultimo === null, `el segundo latido no trajo números y dejó: ${JSON.stringify(cuerpos[1])}`);
  ok("esperado_ultimo" in cuerpos[1] && cuerpos[1].esperado_ultimo === null);
});

// ── 4. Una alerta por job ───────────────────────────────────────────────────────────────
test("cada job tiene su propia alerta: uno nuevo no se esconde bajo 'sigue igual desde hace 14 días'", () => {
  const fn = entre("async function saludWatchdog(", "// ── 2. El KPI del negocio");
  ok(/_clasificarLatidos\(filas,/.test(fn), "el vigilante usa la regla que se prueba acá, no una copia");
  for (const k of ["jobs-atrasados", "jobs-fallando", "jobs-rinden-poco"]) {
    ok(fn.includes(`clave: \`${k}-\${a.job}\``), `${k} tiene que llevar el nombre del job en la clave`);
    ok(!fn.includes(`clave: "${k}"`), `${k} no puede volver a ser una clave de grupo`);
  }
});

// ── 5. Cadencias ────────────────────────────────────────────────────────────────────────
const _cadencia = (node) => {
  const p = node.arguments[2]?.properties?.find(x => x.key?.name === "cadenciaMin");
  if (!p) return null;
  const expr = worker.slice(p.value.start, p.value.end);
  return /^[\d\s*+]+$/.test(expr) ? Function(`return (${expr})`)() : null;
};

test("todo latido de un job por slots declara una cadencia que cubre el hueco más largo entre slots", () => {
  const slots = (nombre) => JSON.parse(worker.match(new RegExp(`const ${nombre} = (\\[[^\\]]+\\])`))[1]);
  const [INI, FIN] = [9, 23];
  const huecoMax = (s) => Math.max(...s.slice(1).map((h, i) => (h - s[i]) * 60), (FIN - s[s.length - 1]) * 60 + (s[0] - INI) * 60);
  const porFuncion = { runAgentCycle: "AGENT_SLOTS", maybeRunAgentSlot: "AGENT_SLOTS", _runAutoGoogleSlot: "AUTOGOOGLE_SLOTS", maybeRunAutoGoogleSlot: "AUTOGOOGLE_SLOTS" };
  const problemas = [];
  let revisados = 0;
  walk.fullAncestor(ast, (node, _s, anc) => {
    if (node.type !== "CallExpression" || node.callee.name !== "saludPing") return;
    const fn = [...anc].reverse().find(a => a.type === "FunctionDeclaration")?.id?.name;
    if (!porFuncion[fn]) return;
    const cad = _cadencia(node);
    if (!cad) return;
    revisados++;
    const hueco = huecoMax(slots(porFuncion[fn]));
    const alcance = cad * (cad >= 720 ? 1.5 : 3);
    // +90 min: un slot puede disparar tarde ("RECUPERADO, iba atrasado").
    if (alcance <= hueco + 90) problemas.push(`${node.arguments[1].value} en ${fn}: cadencia ${cad} cubre ${alcance} min activos y el hueco entre slots es ${hueco}`);
  });
  ok(revisados >= 3, `revisé ${revisados} latidos de jobs por slots: la búsqueda no está encontrando nada`);
  deepStrictEqual(problemas, []);
});

test("un mismo job no declara dos cadencias distintas", () => {
  const cads = {};
  walk.simple(ast, { CallExpression(node) {
    if (node.callee.name !== "saludPing") return;
    const job = node.arguments[1]?.value, cad = _cadencia(node);
    if (typeof job !== "string" || !cad) return;   // 0 = "a pedido", a propósito
    (cads[job] ||= new Set()).add(cad);
  } });
  const dobles = Object.entries(cads).filter(([, s]) => s.size > 1).map(([j, s]) => `${j}: ${[...s].join(" y ")}`);
  deepStrictEqual(dobles, [], "lista_no_recontactar latía 'ok' cada 24 h y 'fail' cada 60 min: el horario de la fila dependía de cuál había llegado último");
});

// ── 6. "No pude leer" nunca es "cero" ───────────────────────────────────────────────────
test("si no puede leer el uso de Apollo, el cupo figura agotado y lo dice", async () => {
  const { getApolloUsageToday } = await cargarWorker(["getApolloUsageToday"], { fetchFalso: true });
  globalThis.__fetchFalso = async () => ({ ok: false, status: 503, headers: { get: () => null }, json: async () => ({}), text: async () => "caído" });
  const u = await getApolloUsageToday("token-falso");
  strictEqual(u.sinDatos, true);
  ok(u.usedToday >= u.limit, `con la base caída nadie puede desbloquear: ${JSON.stringify(u)}`);
  ok(u.usedThisMonth >= u.monthLimit, "y el mensual figura lleno");
});

const _configApollo = (periodo, escrito) => async (url, opts = {}) => {
  const u = String(url);
  if ((opts.method || "GET") !== "GET") escrito.push(`${u} ${opts.body || ""}`);
  if (u.includes("toolbar_config?key=in.(apollo_calls_today")) {
    return { ok: true, status: 200, headers: { get: () => null }, text: async () => "", json: async () => [
      { key: "apollo_calls_month", value: "412" }, { key: "apollo_calls_month_period", value: periodo }, { key: "apollo_monthly_limit", value: "2500" },
    ] };
  }
  return { ok: true, status: 200, headers: { get: () => null }, json: async () => [], text: async () => "" };
};

test("un período guardado que no es el ciclo anterior avisa en vez de reiniciar el contador en silencio", async () => {
  const { getApolloUsageToday } = await cargarWorker(["getApolloUsageToday"], { fetchFalso: true });
  const escrito = [];
  globalThis.__fetchFalso = _configApollo("2026-09", escrito);
  await getApolloUsageToday("token-falso");
  await new Promise(r => setTimeout(r, 150));   // la alerta sale sin await
  ok(escrito.some(x => x.includes("apollo-contador-periodo")), `no avisó. Escrituras: ${escrito.map(x => x.slice(0, 90)).join(" | ")}`);
});

test("el día que cambia el ciclo (lo guardado es el ciclo anterior) reinicia sin avisar", async () => {
  const { getApolloUsageToday } = await cargarWorker(["getApolloUsageToday"], { fetchFalso: true });
  const hoy = new Date();
  const inicio = new Date(Date.UTC(hoy.getUTCFullYear(), hoy.getUTCDate() < 12 ? hoy.getUTCMonth() - 1 : hoy.getUTCMonth(), 12));
  const anterior = new Date(inicio); anterior.setUTCMonth(anterior.getUTCMonth() - 1);
  const escrito = [];
  globalThis.__fetchFalso = _configApollo(anterior.toISOString().slice(0, 10), escrito);
  const u = await getApolloUsageToday("token-falso");
  await new Promise(r => setTimeout(r, 150));
  strictEqual(u.usedThisMonth, 0, "ciclo nuevo: arranca en cero");
  ok(escrito.some(x => x.includes("apollo_calls_month")), "y reinicia el contador");
  ok(!escrito.some(x => x.includes("apollo-contador-periodo")), "sin alerta: es el cambio de ciclo normal");
});

test("aprovechamiento de Apollo y quemar el ciclo leen el contador con la MISMA función", () => {
  const aprov = entre("async function vigilarAprovechamientoDeApollo(", "// ── CHEQUEO DEL PROPIO DNS");
  ok(/getApolloUsageToday\(token\)/.test(aprov), "el 11/09 uno dijo '0 de 2.500' y el otro 'va a usar 717': dos reglas para un contador");
  ok(!/cfg\.apollo_calls_month\b/.test(aprov), "no puede volver a leer la config con su propia regla");
  ok(!/real: proyectado/.test(aprov), "sin números: su alerta 💸 ya lo dice, y con números salía dos veces en el mismo mail");
  const quemar = entre("async function apolloQuemarCiclo(", "// BARRIDO DE PROSPECTS");
  ok(/usage\.sinDatos/.test(quemar), "sin datos es una falla de lectura, no 'atrasado con 0 usados'");
});

// ── 7. Los otros renglones del mail del 12/09 ───────────────────────────────────────────
test("email_en_imagen tiene su propia explicación", async () => {
  const { _comentarioSinEmail } = await cargarWorker(["_comentarioSinEmail"]);
  const t = _comentarioSinEmail({ ok: 5, fail: 0, paginas: 5, crudos: 0, emailEnImagen: true });
  ok(/IMAGEN/.test(t) && !/solo tiene formulario/.test(t), t);
  ok(/solo tiene formulario/.test(_comentarioSinEmail({ ok: 6, fail: 0, paginas: 6, crudos: 0 })), "el caso sin imagen conserva su explicación");
});

test("los reciclables sin ads.txt no se re-eligen cada día, y cuentan como procesados", () => {
  const fn = entre("async function sincronizarFinalizadosDeMonday(", "async function _dominiosContactadosDesde(");
  ok(/toolbar_adstxt_audit\?verdict=eq\.no&last_checked_at=gte\./.test(fn), "se lee la auditoría de ads.txt");
  ok(/!_sinAdsReciente\.has\(d\)/.test(fn), "y lo descartado en 30 días no se elige");
  ok(/resumen: _resRec/.test(fn) && /sin_ads: _resRec\.sinAds/.test(fn), "los descartados de hoy quedan guardados");
  const salud = entre("// ── MONDAY (reciclado hacia cero)", "// ── RE-TRABAJO");
  ok(/_procesados = \(_mSync\.encolados \|\| 0\) \+ _sinAdsHoy/.test(salud), "602 encolados + 93 sin ads.txt = 695 de 695: cumplió");
  ok(/Number\(_mSync\.libre\) < _techo \* 0\.9/.test(salud), "el carril se nombra sólo si limitó");
});

test("con el carril de sellers lleno, la fuente de Google late 'no hacía falta' en vez de callarse", () => {
  const fn = entre("async function _feederPullSellers(", "inserted += await _feederPullSellersGoogle(");
  ok(/_usadoCarril >= _cupoCarril\) \{[\s\S]*saludPing\(token, "sellers_google", \{ status: "ok"[\s\S]*return 0;/.test(fn),
     "el 12/09 llevaba 71 h sin latir por diseño: salteada por carril lleno, no caída");
});

// ── 8. Ningún fetch sin reloj ───────────────────────────────────────────────────────────
test("el fetch del worker trae reloj por defecto: nadie puede volver a colgar el loop para siempre", () => {
  ok(/^import fetchNodo from "node-fetch";/m.test(worker), "el import de node-fetch tiene que pasar por el envoltorio");
  ok(!/^import fetch from "node-fetch";/m.test(worker), "si vuelve el import directo, 345 de 419 llamadas quedan sin reloj");
  ok(/const fetch = \(url, opts\) => fetchNodo\(url, opts\?\.signal \? opts : /.test(worker), "el envoltorio agrega el reloj sólo si falta");
  ok(/process\.on\("unhandledRejection"/.test(worker), "un fetch que corta en vez de colgar no puede tirar el proceso");
});

test("el envoltorio agrega el reloj sólo si el llamador no trajo uno, y conserva el resto de las opciones", async () => {
  const mod = await cargarWorker(["fetch"], { fetchFalso: true });
  const vistos = [];
  globalThis.__fetchFalso = async (_url, opts) => { vistos.push(opts); return { ok: true, status: 200, headers: { get: () => null }, json: async () => ({}), text: async () => "" }; };
  await mod.fetch("https://x.test/a");
  await mod.fetch("https://x.test/b", { method: "POST", body: "{}" });
  const propio = AbortSignal.timeout(5000);
  await mod.fetch("https://x.test/c", { signal: propio });
  ok(vistos[0]?.signal instanceof AbortSignal, "sin opciones: reloj por defecto");
  ok(vistos[1]?.signal instanceof AbortSignal && vistos[1].method === "POST" && vistos[1].body === "{}", "con opciones: se conservan y se agrega el reloj");
  strictEqual(vistos[2].signal, propio, "con reloj propio: se respeta el del llamador");
});
