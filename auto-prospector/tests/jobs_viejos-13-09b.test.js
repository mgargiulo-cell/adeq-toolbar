// Los jobs viejos del pool: los que borraban sin dejar rastro, los que daban el día por hecho sin
// mirar, y los que prometían un trabajo que no hacían. (2026-09-13, auditoría pedida por el dueño)
//
// "Dejar una base de conceptos, lógicas y funcionamientos automáticos sin fallas: lo que arreglás un
// día no lo rompas al otro." Cada bloque de este archivo fija una regla que la auditoría encontró rota:
//   1. La limpieza del pool (cleanup_pool) BORRABA filas de Prospects: sin motivo, sin latido, fuera
//      del renglón de purgas del parte, y con "eliminados" en el log aunque el borrado fallara.
//      Ahora rechaza con motivo 'cleanup:', late, y el autopilot no aprende rubros de esos rechazos.
//   2. La revisión de sospechosos sellaba el día aunque la IA no hubiera contestado nada.
//   3. El re-análisis de emails nació roto (email_sources como objeto → TypeError antes del cursor):
//      se retira, y ningún lector de email_sources puede volver a usar la fuente cruda como texto.
//   4. El backfill de campos rechazaba leads por un score informativo y no terminaba nunca: se retira.
//   5. El botón "Activar refresh" prometía re-leer tráfico de Prospects (prohibido): se retira con su job.
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
const popup = fs.readFileSync(path.join(RAIZ, "..", "popup", "popup.js"), "utf8");
const popupHtml = fs.readFileSync(path.join(RAIZ, "..", "popup", "popup.html"), "utf8");
const ast = acorn.parse(worker, { ecmaVersion: "latest", sourceType: "module" });
const entre = (desde, hasta) => {
  const i = worker.indexOf(desde);
  ok(i >= 0, `no encontré "${desde}"`);
  return worker.slice(i, worker.indexOf(hasta, i));
};
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const respuesta = (body, { status = 200, total } = {}) => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: (k) => (String(k).toLowerCase() === "content-range" && total != null ? `0-0/${total}` : null) },
  json: async () => body, text: async () => JSON.stringify(body),
});

// ── 1. Limpieza del pool ────────────────────────────────────────────────────────────────
const AHORA = Date.parse("2026-09-14T12:00:00+02:00");
const haceDias = (d) => new Date(AHORA - d * 86400_000).toISOString();

test("la regla de la limpieza: debajo de 350K sale para toda fuente, sin tráfico sale a las 48 h salvo monday_refresh", async () => {
  const { _motivoCleanup } = await cargarWorker(["_motivoCleanup"]);
  const fila = (x) => ({ status: "pending", source: "auto_feeder_sellers", created_at: haceDias(3), ...x });
  strictEqual(_motivoCleanup(fila({ traffic: 120000, source: "autopilot" }), AHORA), "cleanup: trafico_bajo");
  strictEqual(_motivoCleanup(fila({ traffic: 120000, source: "monday_refresh" }), AHORA), "cleanup: trafico_bajo", "el piso de 350K no tiene excepción de fuente (01/07)");
  strictEqual(_motivoCleanup(fila({ traffic: 120000, created_at: haceDias(0.01) }), AHORA), "cleanup: trafico_bajo", "el tráfico bajo no espera 48 h");
  strictEqual(_motivoCleanup(fila({ traffic: 350000 }), AHORA), null, "350K justo pasa el piso");
  strictEqual(_motivoCleanup(fila({ traffic: 2_000_000 }), AHORA), null);
  for (const t of [0, null, -1]) {
    strictEqual(_motivoCleanup(fila({ traffic: t }), AHORA), "cleanup: sin_trafico", `traffic ${t} de hace 3 días`);
    strictEqual(_motivoCleanup(fila({ traffic: t, created_at: haceDias(1) }), AHORA), null, `traffic ${t} de hace 1 día: todavía en el margen`);
  }
  strictEqual(_motivoCleanup(fila({ traffic: 0, source: "monday_refresh" }), AHORA), null, "monday_refresh sin tráfico se queda");
  strictEqual(_motivoCleanup(fila({ traffic: 0, source: null }), AHORA), "cleanup: sin_trafico", "sin fuente no es monday_refresh");
  strictEqual(_motivoCleanup({ status: "por_enviar", source: "manual_cola", traffic: 120000, created_at: haceDias(3) }, AHORA), null, "lo que está en la cola del MB no se toca");
  strictEqual(_motivoCleanup({ status: "validated", traffic: 0, created_at: haceDias(30) }, AHORA), null);
});

test("el freno de la limpieza es relativo al pool: más de 50 o más del 10% de lo pendiente", async () => {
  const { _cleanupDeGolpe } = await cargarWorker(["_cleanupDeGolpe"]);
  strictEqual(_cleanupDeGolpe(3, 3000), false);
  strictEqual(_cleanupDeGolpe(50, 100), false, "50 es el piso del umbral");
  strictEqual(_cleanupDeGolpe(51, 100), true);
  strictEqual(_cleanupDeGolpe(300, 3000), false, "el 10% de 3.000 es 300");
  strictEqual(_cleanupDeGolpe(301, 3000), true);
  strictEqual(_cleanupDeGolpe(60, NaN), true, "si no se pudo contar el pool, vale el piso de 50");
});

function enrutadorCleanup(registro, { filas = {}, totales = {}, patchStatus = 200, pool = 3000 } = {}) {
  return async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase();
    registro.push({ u, m, b: opts.body ? String(opts.body) : "" });
    if (u.includes("/toolbar_health")) return respuesta([], { status: 201 });
    if (!u.includes("toolbar_review_queue")) return respuesta([]);
    if (m === "GET" && u.includes("status=eq.pending&select=id")) return respuesta([], { total: pool });
    const clave = u.includes("traffic=gt.0") ? "trafico_bajo" : "sin_trafico";
    if (m === "GET") return respuesta(filas[clave] || [], { total: totales[clave] ?? (filas[clave] || []).length });
    if (m === "PATCH") {
      if (patchStatus !== 200) return respuesta({ message: "boom" }, { status: patchStatus });
      const ids = (u.match(/id=in\.\(([^)]*)\)/)?.[1] || "").split(",").filter(Boolean);
      return respuesta(ids.map(id => ({ id })));
    }
    return respuesta([]);
  };
}

test("la limpieza rechaza con motivo y rejected_at, nunca borra, y late con lo que sacó de verdad", async () => {
  const { _cleanupPool } = await cargarWorker(["_cleanupPool"], { fetchFalso: true });
  const reg = [];
  globalThis.__fetchFalso = enrutadorCleanup(reg, {
    filas: {
      trafico_bajo: [
        { id: "a1", status: "pending", traffic: 120000, source: "manual_cola", created_at: haceDias(0.01) },
        { id: "a2", status: "pending", traffic: 80000, source: "monday_refresh", created_at: haceDias(5) },
      ],
      sin_trafico: [
        { id: "b1", status: "pending", traffic: -1, source: "autopilot", created_at: haceDias(3) },
        { id: "b2", status: "pending", traffic: null, source: null, created_at: haceDias(4) },
      ],
    },
  });
  const r = await _cleanupPool("t", AHORA);
  deepStrictEqual(r.sacadas, { trafico_bajo: 2, sin_trafico: 2 });
  deepStrictEqual(r.problemas, []);
  strictEqual(reg.filter(x => x.m === "DELETE").length, 0, "la limpieza no puede borrar filas de Prospects");
  ok(!reg.some(x => x.u.includes("traffic=eq.-1")), "el reseteo -1→0 para un refresh que no existe se fue");
  const patches = reg.filter(x => x.m === "PATCH");
  strictEqual(patches.length, 2);
  for (const p of patches) {
    const b = JSON.parse(p.b);
    strictEqual(b.status, "rejected");
    strictEqual(b.suspect_reject, true);
    ok(/^cleanup: (trafico_bajo|sin_trafico)$/.test(b.suspect_reason), b.suspect_reason);
    strictEqual(b.rejected_at, new Date(AHORA).toISOString(), "sin rejected_at no entra en el renglón de purgas del parte");
    ok(p.u.includes("status=eq.pending"), "si un MB la movió a la cola mientras tanto, no se toca");
  }
  const lecturaSinTrafico = reg.find(x => x.m === "GET" && x.u.includes("traffic.lt.0"));
  ok(lecturaSinTrafico, "el -1 entra en la regla de sin tráfico");
  ok(lecturaSinTrafico.u.includes("source.is.null"), "una fila sin fuente no es monday_refresh: `source=neq` sola la dejaba afuera");
  const ping = reg.filter(x => x.u.includes("/toolbar_health")).map(x => JSON.parse(x.b)).find(x => x.job === "cleanup_pool");
  ok(ping, "la limpieza tiene que latir");
  strictEqual(ping.last_status, "ok");
  strictEqual(ping.real_ultimo, 4);
  strictEqual(ping.esperado_ultimo, null, "sin esperado: no hay un número deseado de filas para sacar");
  strictEqual(ping.esperado_cada_min, 15);
});

test("si el rechazo falla o quiere sacar demasiado de golpe, el latido sale en fail y no cuenta lo que no hizo", async () => {
  const { _cleanupPool } = await cargarWorker(["_cleanupPool"], { fetchFalso: true });
  const reg = [];
  const filas = { trafico_bajo: [{ id: "a1", status: "pending", traffic: 1000, source: "autopilot", created_at: haceDias(1) }] };
  globalThis.__fetchFalso = enrutadorCleanup(reg, { filas, patchStatus: 500 });
  const r = await _cleanupPool("t", AHORA);
  strictEqual(r.real, 0, "un PATCH que dio 500 no sacó nada, aunque el log viejo dijera 'eliminados'");
  ok(r.problemas.some(p => /rechazo falló \(HTTP 500\)/.test(p)), r.problemas.join("; "));
  let ping = pingDe(reg, "cleanup_pool");
  strictEqual(ping.last_status, "fail");

  const { _cleanupPool: otra } = await cargarWorker(["_cleanupPool"], { fetchFalso: true });
  const reg2 = [];
  globalThis.__fetchFalso = enrutadorCleanup(reg2, { filas, totales: { trafico_bajo: 1400 }, pool: 3000 });
  const r2 = await otra("t", AHORA);
  strictEqual(r2.real, 1, "igual procesa su lote: rechazar es reversible");
  ok(r2.problemas.some(p => /quiere sacar 1400 de golpe/.test(p)), r2.problemas.join("; "));
  ping = pingDe(reg2, "cleanup_pool");
  strictEqual(ping.last_status, "fail");

  const { _cleanupPool: tercera } = await cargarWorker(["_cleanupPool"], { fetchFalso: true });
  const reg3 = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    reg3.push({ u: String(url), m: (opts.method || "GET").toUpperCase(), b: opts.body ? String(opts.body) : "" });
    return String(url).includes("/toolbar_health") ? respuesta([], { status: 201 }) : respuesta({ message: "caído" }, { status: 503 });
  };
  const r3 = await tercera("t", AHORA);
  ok(r3.problemas.length === 2, `no pude leer ≠ no había nada: ${r3.problemas.join("; ")}`);
  strictEqual(pingDe(reg3, "cleanup_pool").last_status, "fail");
});

test("el loop principal limpia con _cleanupPool y no queda ningún DELETE sobre Prospects en la limpieza", () => {
  const bloque = entre('_tocaCorrer(token, "cleanup_pool", 15)', "_cleanupPool(token)");
  ok(bloque.length < 200, "la limpieza del loop es sólo la llamada");
  const fn = cuerpoDe("_cleanupPool");
  ok(!/method:\s*"DELETE"/.test(fn), "la limpieza no borra");
  ok(!/traffic:\s*0\b/.test(fn), "ni resetea el -1");
});

test("el autopilot no aprende rubros ni países de los rechazos de la limpieza", async () => {
  const { getRejectionPatterns } = await cargarWorker(["getRejectionPatterns"], { fetchFalso: true });
  const urls = [];
  globalThis.__fetchFalso = async (url) => { urls.push(String(url)); return respuesta([{ category: "News", geo: "AR" }]); };
  const r = await getRejectionPatterns("t");
  deepStrictEqual(r.categories, { News: 1 });
  ok(urls[0].includes("status=eq.rejected"), urls[0]);
  ok(urls[0].includes("suspect_reason.not.like.%22cleanup:*%22") && urls[0].includes("suspect_reason.is.null"),
     `los rechazos 'cleanup:' son por tráfico, no por rubro: ${urls[0]}`);
});

// ── 2. Revisión de sospechosos ──────────────────────────────────────────────────────────
test("la revisión de sospechosos sólo cierra el día si la IA dictaminó algo, o si ya lo intentó 3 veces", async () => {
  const { _resultadoCorridaSuspect } = await cargarWorker(["_resultadoCorridaSuspect"]);
  const todosNull = _resultadoCorridaSuspect({ consultables: 180, veredictos: 0, flagged: 0, intentoN: 1 });
  strictEqual(todosNull.cerrarDia, false, "techo de Claude, 5xx o timeout en todos los lotes: no se da el día por hecho");
  strictEqual(todosNull.status, "warn");
  strictEqual(todosNull.real, null);
  strictEqual(todosNull.esperado, null, "sin números: un warn de 'IA sin respuesta' no puede disparar 'rinde poco'");
  const tercero = _resultadoCorridaSuspect({ consultables: 180, veredictos: 0, intentoN: 3 });
  deepStrictEqual([tercero.cerrarDia, tercero.status], [true, "warn"], "al tercer intento se cierra: sigue la próxima corrida L/X/V");
  const sinTitulo = _resultadoCorridaSuspect({ consultables: 0, veredictos: 0, intentoN: 1 });
  deepStrictEqual([sinTitulo.cerrarDia, sinTitulo.status, sinTitulo.esperado], [true, "ok", null], "filas sin título no son una falla de la IA");
  const parcial = _resultadoCorridaSuspect({ consultables: 180, veredictos: 60, flagged: 4, intentoN: 1 });
  deepStrictEqual([parcial.cerrarDia, parcial.status, parcial.real, parcial.esperado], [true, "ok", 60, 180], "esperado = las que se le preguntaron, nunca rows.length");
  ok(/4 marcadas de 60 dictaminadas/.test(parcial.detalle), parcial.detalle);
});

test("el freno de reintentos vive en la config: 3 por día y 120 min entre uno y otro", async () => {
  const { _intentoSuspect } = await cargarWorker(["_intentoSuspect"]);
  const dateISO = "2026-09-14";
  const ahora = Date.parse("2026-09-14T14:00:00+02:00");
  const hace = (min) => new Date(ahora - min * 60_000).toISOString();
  deepStrictEqual(_intentoSuspect("", { dateISO, ahora }), { puede: true, intentoN: 1 });
  deepStrictEqual(_intentoSuspect(`2026-09-11|3|${hace(4000)}`, { dateISO, ahora }), { puede: true, intentoN: 1 }, "la marca de otro día no cuenta");
  strictEqual(_intentoSuspect(`${dateISO}|1|${hace(30)}`, { dateISO, ahora }).puede, false, "el worker reinicia cada ~7 min: sin espera, reintentaría en cada vuelta");
  deepStrictEqual(_intentoSuspect(`${dateISO}|1|${hace(121)}`, { dateISO, ahora }), { puede: true, intentoN: 2 });
  strictEqual(_intentoSuspect(`${dateISO}|3|${hace(500)}`, { dateISO, ahora }).puede, false, "tope de 3 por día");
});

const LUNES = { weekday: "Mon", dateISO: "2026-09-14", hour: 11 };
const LUNES_TS = Date.parse("2026-09-14T11:00:00+02:00");
const PENDIENTES = [
  ...Array.from({ length: 45 }, (_, i) => ({ id: `r${i}`, domain: `sitio${i}.com`, page_title: `Diario ${i}`, category: "News" })),
  ...Array.from({ length: 5 }, (_, i) => ({ id: `s${i}`, domain: `sintitulo${i}.com`, page_title: "", category: "" })),
];

function enrutadorSuspect(reg, { claude = "caido", marca = "", ultimaFecha = "2026-09-11" } = {}) {
  return async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase();
    const b = opts.body ? String(opts.body) : "";
    reg.push({ u, m, b });
    if (u.includes("toolbar_config?select=key,value")) return respuesta([
      { key: "last_suspect_analysis_date", value: ultimaFecha }, { key: "suspect_analysis_intento", value: marca },
    ]);
    if (u.includes("toolbar_config?key=eq.prospect_trash_rules")) return respuesta([{ value: "tiendas online y turnos médicos" }]);
    if (u.includes("toolbar_config?key=eq.") && m === "PATCH") return respuesta([{ key: "x" }]);
    if (u.includes("toolbar_review_queue?status=eq.pending&suspect_checked_at=is.null")) return respuesta(PENDIENTES);
    if (u.includes("/functions/v1/api-proxy")) {
      if (claude === "caido") return respuesta({ error: "overloaded" }, { status: 529 });
      const lista = JSON.parse(b).body.messages[0].content.split("\n").map(l => l.split(" | ")[0]);
      const out = Object.fromEntries(lista.map(d => [d, d === "sitio3.com" ? "ecommerce" : "publisher"]));
      return respuesta({ content: [{ text: JSON.stringify(out) }], usage: { input_tokens: 10, output_tokens: 10 } });
    }
    if (u.includes("/toolbar_health")) return respuesta([], { status: 201 });
    return respuesta([]);
  };
}
const configEscrita = (reg, key) => reg.filter(x => x.m === "PATCH" && x.u.includes(`toolbar_config?key=eq.${key}`)).map(x => JSON.parse(x.b).value);
const pingDe = (reg, job) => reg.filter(x => x.u.includes("/toolbar_health") && x.m === "POST").map(x => JSON.parse(x.b)).filter(x => x.job === job).pop();

test("con la IA caída: una sola llamada de sonda, el día NO se sella, queda la marca del intento y late warn", async () => {
  const { runSuspectRejectAnalysis } = await cargarWorker(["runSuspectRejectAnalysis"], { fetchFalso: true });
  const reg = [];
  globalThis.__fetchFalso = enrutadorSuspect(reg, { claude: "caido" });
  await runSuspectRejectAnalysis("t", { partes: LUNES, ahora: LUNES_TS });
  strictEqual(reg.filter(x => x.u.includes("/functions/v1/api-proxy")).length, 1, "si la sonda de 20 no trae nada, no se pagan los otros lotes");
  deepStrictEqual(configEscrita(reg, "last_suspect_analysis_date"), [], "no pude mirar ≠ corrida limpia: el día no se sella");
  deepStrictEqual(configEscrita(reg, "suspect_analysis_intento"), [`2026-09-14|1|${new Date(LUNES_TS).toISOString()}`]);
  strictEqual(reg.filter(x => x.m === "PATCH" && x.u.includes("toolbar_review_queue")).length, 0, "sin veredicto no se escribe nada en las filas");
  const ping = pingDe(reg, "suspect_analysis");
  ok(ping, "la revisión tiene que latir");
  strictEqual(ping.last_status, "warn");
  ok(/IA sin respuesta — reintento 1\/3/.test(ping.last_detail), ping.last_detail);
  ok(!("esperado_cada_min" in ping), "corre L/X/V con salidas mudas: una cadencia la mostraría 'atrasada' en falso");

  // Misma mañana, 30 min después (el worker reinició): la marca frena sin leer ni pagar nada.
  const { runSuspectRejectAnalysis: otraVuelta } = await cargarWorker(["runSuspectRejectAnalysis"], { fetchFalso: true });
  const reg2 = [];
  globalThis.__fetchFalso = enrutadorSuspect(reg2, { claude: "caido", marca: `2026-09-14|1|${new Date(LUNES_TS).toISOString()}` });
  await otraVuelta("t", { partes: { ...LUNES, hour: 11 }, ahora: LUNES_TS + 30 * 60_000 });
  strictEqual(reg2.filter(x => x.u.includes("/functions/v1/api-proxy")).length, 0);
  strictEqual(reg2.filter(x => x.u.includes("suspect_checked_at=is.null")).length, 0);
});

test("con la IA contestando: clasifica todo, marca la sospechosa, sella el día y late ok con esperado = las que tenían título", async () => {
  const { runSuspectRejectAnalysis } = await cargarWorker(["runSuspectRejectAnalysis"], { fetchFalso: true });
  const reg = [];
  globalThis.__fetchFalso = enrutadorSuspect(reg, { claude: "ok" });
  await runSuspectRejectAnalysis("t", { partes: LUNES, ahora: LUNES_TS });
  strictEqual(reg.filter(x => x.u.includes("/functions/v1/api-proxy")).length, 3, "45 con título → lotes de 20, 20 y 5");
  deepStrictEqual(configEscrita(reg, "last_suspect_analysis_date"), ["2026-09-14"]);
  const marcadas = reg.filter(x => x.m === "PATCH" && x.u.includes("toolbar_review_queue") && x.b.includes("suspect_reject"));
  strictEqual(marcadas.length, 1);
  ok(marcadas[0].u.includes("id=eq.r3"), marcadas[0].u);
  const ping = pingDe(reg, "suspect_analysis");
  deepStrictEqual([ping.last_status, ping.real_ultimo, ping.esperado_ultimo], ["ok", 45, 45]);
});

test("en el código: el día se sella sólo con cerrarDia, y el latido de la revisión no declara cadencia", () => {
  const fn = cuerpoDe("runSuspectRejectAnalysis");
  ok(/if \(res\.cerrarDia\) await setConfigValue\(token, "last_suspect_analysis_date"/.test(fn), "el sello del día tiene que depender del resultado");
  strictEqual((fn.match(/"last_suspect_analysis_date", dateISO/g) || []).length, 1, "un solo lugar sella el día");
  walk.simple(ast, { CallExpression(node) {
    if (node.callee.name !== "saludPing" || node.arguments[1]?.value !== "suspect_analysis") return;
    const props = node.arguments[2]?.properties || [];
    ok(!props.some(p => p.key?.name === "cadenciaMin"), "suspect_analysis sin cadenciaMin");
  } });
});

// ── 3. Re-análisis de emails retirado, y la clase de bug que lo rompió ──────────────────
// Busca `(x.email_sources[k] || "").toLowerCase()` y parecidos: un método de texto aplicado a la
// fuente cruda de un email. Desde el 17/06 la fuente puede ser un objeto {source, url}.
const METODOS_DE_TEXTO = new Set(["toLowerCase", "toUpperCase", "trim", "startsWith", "endsWith", "includes", "replace", "split", "match"]);
function fuenteCrudaComoTexto(codigo) {
  const arbol = acorn.parse(codigo, { ecmaVersion: "latest", sourceType: "module" });
  const hallados = [];
  const esSources = (obj) => {
    const nombre = obj?.type === "Identifier" ? obj.name : obj?.type === "MemberExpression" && !obj.computed ? obj.property.name : "";
    return /^(email_?sources|sources|emailSources)$/i.test(nombre);
  };
  walk.fullAncestor(arbol, (node, _s, anc) => {
    if (node.type !== "MemberExpression" || !node.computed || !esSources(node.object)) return;
    let hijo = node;
    for (let i = anc.length - 2; i >= 0; i--) {
      const padre = anc[i];
      if (padre.type === "LogicalExpression" || padre.type === "ChainExpression") { hijo = padre; continue; }
      if (padre.type === "MemberExpression" && padre.object === hijo && !padre.computed && METODOS_DE_TEXTO.has(padre.property.name)) {
        hallados.push(codigo.slice(node.start, padre.end));
      }
      break;
    }
  });
  return hallados;
}

test("el detector ve la línea exacta que rompió el re-análisis, y no confunde la lectura con _normSrc", () => {
  deepStrictEqual(fuenteCrudaComoTexto(`const src = (sources[e.toLowerCase()] || "").toLowerCase();`), ['sources[e.toLowerCase()] || "").toLowerCase']);
  deepStrictEqual(fuenteCrudaComoTexto(`const s = lead.email_sources?.[e].trim();`).length, 1);
  deepStrictEqual(fuenteCrudaComoTexto(`const src = _normSrc(sources[e.toLowerCase()]).toLowerCase();`), []);
  deepStrictEqual(fuenteCrudaComoTexto(`const raw = l.email_sources[k]; const s = typeof raw === "string" ? raw.toLowerCase() : "";`), []);
});

test("ningún lector de email_sources del worker usa la fuente cruda como texto, y el re-análisis roto no vuelve", () => {
  deepStrictEqual(fuenteCrudaComoTexto(worker), [], "usar _normSrc (o un typeof) antes de tratar la fuente como texto");
  for (const f of fs.readdirSync(path.join(RAIZ, "lib")).filter(x => x.endsWith(".js"))) {
    deepStrictEqual(fuenteCrudaComoTexto(fs.readFileSync(path.join(RAIZ, "lib", f), "utf8")), [], `lib/${f}`);
  }
  ok(!/\brunReenrichBadLeads\s*\(/.test(worker), "el re-análisis quedó retirado: lo cubren polishPool, auditarEmailsDelPool y apolloQuemarCiclo");
  ok(!/\bREENRICH_(BATCH|CONC|COOLDOWN_MS)\b/.test(worker));
});

// ── 4. Backfill retirado, y la clase: un score informativo no rechaza ───────────────────
test("ningún bloque que calcula scoreWebsite rechaza el lead: el filtro corre una vez, al entrar (v638)", () => {
  const problemas = [];
  let llamadas = 0;
  walk.fullAncestor(ast, (node, _s, anc) => {
    if (node.type !== "CallExpression" || node.callee.name !== "scoreWebsite") return;
    llamadas++;
    const bloque = [...anc].reverse().find(a => a.type === "BlockStatement");
    const texto = worker.slice(bloque.start, bloque.end);
    if (/status\s*[:=]\s*["']rejected["']/.test(texto)) problemas.push(texto.slice(0, 160));
  });
  ok(llamadas >= 1, "la búsqueda no encontró ninguna llamada a scoreWebsite");
  deepStrictEqual(problemas, []);
  ok(!/\bbackfillMissingFields\s*\(/.test(worker), "el backfill quedó retirado");
  ok(!/\bBACKFILL_BATCH\b/.test(worker));
});

// ── 5. "Activar refresh" retirado: ninguna llave re-compra el tráfico de Prospects ──────
test("no hay botón, job ni llave que vuelva a comprar el tráfico de un lead que ya está en Prospects (regla del 18/08)", () => {
  for (const [nombre, texto] of [["index.js", worker], ["popup.js", popup], ["popup.html", popupHtml]]) {
    ok(!texto.includes("agent_refresh_empty_leads"), `${nombre} menciona agent_refresh_empty_leads`);
    ok(!texto.includes("permitir_recompra_de_trafico"), `${nombre} menciona permitir_recompra_de_trafico`);
  }
  ok(!/\brefreshOneEmptyLead\s*\(/.test(worker));
  ok(!popupHtml.includes('id="agent-refresh-toggle"') && !popupHtml.includes('id="agent-refresh-status"'), "el botón y su cartel de estado");
  ok(!/\btoggleRefreshEmptyLeads\b/.test(popup), "ni la función ni su listener");
});
