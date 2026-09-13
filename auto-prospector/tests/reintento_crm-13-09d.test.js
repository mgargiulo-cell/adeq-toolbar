// Rebotes, reintento y lista del CRM: lo que otros grupos marcaron fuera de su zona el 13/09.
//
//   R1. Un adicional que rebota no encontraba su sitio: `future_sent` sólo queda en agent_actions,
//       nunca en sendtrack, así que el aviso al CRM no salía y el reintento usaba el dominio del correo.
//   R2. El reintento borraba de `email_sources` la vía de la dirección que rebotó: se perdía quién la
//       había encontrado, justo el registro que el parte usa de respaldo.
//   R3. El ranking de fuentes contaba un fuera de oficina como rebote de la fuente.
//   R4. El re-engagement mandaba en inglés a un sitio hispano con el idioma vacío.
//   R5. La lista del CRM sólo se miraba al mandar: un bloqueado seguía días en Prospects.
//
// Run: npm test
import { test, mock } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";

// lib/config.js se evalúa UNA vez por proceso: el secreto del CRM tiene que estar antes de la primera carga.
process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";
// Sin key de MillionVerifier la verificación devuelve true: el re-engagement (que exige "ok") corta antes de Gmail.
delete process.env.MILLIONVERIFIER_API_KEY;

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const resp = (body, { status = 200, total } = {}) => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: (k) => (String(k).toLowerCase() === "content-range" && total != null ? `0-0/${total}` : null) },
  json: async () => body, text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
const MB = "sales@adeqmedia.com";

// ── R1. El sitio de una dirección ───────────────────────────────────────────────────────
function enrutadorSitio(reg, o = {}) {
  return async (url) => {
    const u = String(url);
    reg.push(u);
    if (u.includes("toolbar_sendtrack?email=eq.")) return o.st === "falla" ? resp({ message: "boom" }, { status: 500 }) : resp(o.st || []);
    if (u.includes("toolbar_agent_actions?email_to=eq.")) return o.aa === "falla" ? resp({ message: "boom" }, { status: 500 }) : resp(o.aa || []);
    return resp([]);
  };
}

test("R1: un adicional que rebota encuentra su sitio en agent_actions cuando sendtrack no lo tiene", async () => {
  const { _sitioDeLaDireccion } = await cargarWorker(["_sitioDeLaDireccion"], { fetchFalso: true });
  const reg = [];
  globalThis.__fetchFalso = enrutadorSitio(reg, { st: [], aa: [{ domain: "Telexpresse.com" }] });
  strictEqual(await _sitioDeLaDireccion("t", "SReleve@yahoo.fr"), "telexpresse.com", "sin esto el rebote no se avisa al CRM");
  const q = reg.find(u => u.includes("toolbar_agent_actions?email_to=eq."));
  ok(q, "tiene que consultar el registro de envíos del agente");
  ok(q.includes(`email_to=eq.${encodeURIComponent("sreleve@yahoo.fr")}`), "en minúsculas, como la cola de adicionales guarda la dirección");
  ok(/action=in\.\([^)]*future_sent[^)]*\)/.test(q), "future_sent es justamente el envío que no está en sendtrack");
  ok(/created_at=gte\./.test(q) && /[?&]limit=1(&|$)/.test(q), `acotada en tiempo y en filas: ${q}`);
});

test("R1: sendtrack manda si lo tiene; si falla se usa el respaldo; un marcador no es un sitio; sin datos, vacío", async () => {
  const { _sitioDeLaDireccion } = await cargarWorker(["_sitioDeLaDireccion"], { fetchFalso: true });
  let reg = [];
  globalThis.__fetchFalso = enrutadorSitio(reg, { st: [{ domain: "sitio.it" }], aa: [{ domain: "otro.it" }] });
  strictEqual(await _sitioDeLaDireccion("t", "mario@gmail.com"), "sitio.it");
  ok(!reg.some(u => u.includes("toolbar_agent_actions")), "con sendtrack alcanza: no se consulta nada más");

  reg = [];
  globalThis.__fetchFalso = enrutadorSitio(reg, { st: "falla", aa: [{ domain: "sitio.it" }] });
  strictEqual(await _sitioDeLaDireccion("t", "mario@gmail.com"), "sitio.it", "un sendtrack caído no apaga el aviso si el envío está registrado");

  globalThis.__fetchFalso = enrutadorSitio(reg, { st: [], aa: [{ domain: "_bounce_" }] });
  strictEqual(await _sitioDeLaDireccion("t", "mario@gmail.com"), "", "un marcador nunca se reporta como sitio");
  globalThis.__fetchFalso = enrutadorSitio(reg, { st: [], aa: "falla" });
  strictEqual(await _sitioDeLaDireccion("t", "mario@gmail.com"), "", "sin saber el sitio no se inventa (crearía una ficha falsa)");
  globalThis.__fetchFalso = enrutadorSitio(reg, {});
  strictEqual(await _sitioDeLaDireccion("t", "mario@gmail.com"), "");
});

test("R1: el scan de rebotes resuelve el sitio una vez y lo usa para la lista y para el aviso al CRM", () => {
  const fn = cuerpoDe("scanBouncesForUser");
  const i = fn.indexOf("if (_esRebote) {");
  ok(i > 0, "no encontré la rama del rebote");
  const tramo = fn.slice(i, fn.indexOf("queueBounceRetry(token, userEmail, failed", i));
  strictEqual((tramo.match(/_sitioDeLaDireccion\(/g) || []).length, 1, "con el respaldo son hasta dos consultas por llamada: se pedía dos veces");
  ok(/originalDomain: _sitio \|\| failed\.split\("@"\)\[1\]/.test(tramo));
  ok(/reportarReboteAlCrm\(token, \{ email: failed, originalDomain: _sitio,/.test(tramo));
});

// ── R2. La vía de la dirección que rebotó ───────────────────────────────────────────────
const fichaDelDirector = (w) => w._fichaTrasReintento({
  emails: ["mario.rossi@sitio.it", "info@sitio.it"],
  sources: { "mario.rossi@sitio.it": "apollo", "info@sitio.it": "scrape" },
  bouncedEmail: "Mario.Rossi@sitio.it", retryEmail: "info@sitio.it", retrySource: "scrape",
});

test("R2: la dirección que rebotó sale de `emails` y su vía queda en `email_sources`", async () => {
  const w = await cargarWorker(["_fichaTrasReintento"]);
  const f = fichaDelDirector(w);
  deepStrictEqual(f.emails, ["info@sitio.it"], "la rebotada no vuelve a la lista de contactos");
  strictEqual(f.email_sources["mario.rossi@sitio.it"], "apollo", "quién la encontró queda como registro");
  strictEqual(f.email_sources["info@sitio.it"], "scrape");
});

test("R2: con la clave conservada, el renglón de vías no cuenta la rebotada y sí le atribuye el rebote", async () => {
  const w = await cargarWorker(["_fichaTrasReintento", "_viasDeEmailInforme"]);
  const f = fichaDelDirector(w);
  const v = w._viasDeEmailInforme({
    cohorte: [{ domain: "sitio.it", emails: f.emails, email_sources: f.email_sources }],
    enviados: [{ email_to: "mario.rossi@sitio.it", details: {} }],   // un envío viejo, sin la vía en details
    malos: [{ email: "mario.rossi@sitio.it", evidencia: "rebote_smtp" }],
  });
  strictEqual(v.via.apollo?.n || 0, 0, "la rebotada ya no es un email del pool: no suma");
  strictEqual(v.via.scrape?.n, 1);
  strictEqual(v.via.apollo?.rebotes, 1, "el rebote se atribuye a la vía que la encontró");
  strictEqual(v.rebotesSinVia, 0);
});

test("R2: el reintento arma la ficha con _fichaTrasReintento y nada rearma `emails` desde las claves", () => {
  ok(/_fichaTrasReintento\(\{[\s\S]{0,200}conservarRebotado: _esAusencia/.test(cuerpoDe("queueBounceRetry")));
  ok(!/delete email_sources\[/.test(cuerpoDe("_fichaTrasReintento")), "la clave de la rebotada no se borra");
  ok(!/emails\s*[:=]\s*Object\.keys\([^)]*email_sources/.test(worker), "si algo armara `emails` desde las claves, la rebotada volvería");
});

// ── R3. Un fuera de oficina no es un rebote de la fuente ───────────────────────────────────
test("R3: el ranking de fuentes no cuenta un fuera de oficina como rebote", async () => {
  const { aggregateSourcePerformance } = await cargarWorker(["aggregateSourcePerformance"], { fetchFalso: true });
  const reg = [];
  const reintentos = [
    { original_email: "director@medio.es", bounce_type: "auto_reply" },
    { original_email: "ventas@medio.es", bounce_type: "hard" },
    { original_email: "viejo@medio.es", bounce_type: null },
  ];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase();
    reg.push({ u, m, b: String(opts.body || "") });
    if (u.includes("toolbar_agent_actions?action=in.(sent,re_sent,bounce_retry_sent)")) return resp([
      { id: 1, user_email: MB, email_to: "director@medio.es", details: { source: "apollo" } },
      { id: 2, user_email: MB, email_to: "ventas@medio.es", details: { source: "apollo" } },
      { id: 3, user_email: MB, email_to: "viejo@medio.es", details: { source: "scrape" } },
    ]);
    if (u.includes("toolbar_email_opens")) return resp([]);
    if (u.includes("toolbar_bounce_retries?")) {
      // PostgREST aplica el filtro: sin él vuelven todas las filas.
      const sinAusencias = u.includes("or=(bounce_type.is.null,bounce_type.neq.auto_reply)");
      return resp(reintentos.filter(r => !sinAusencias || r.bounce_type !== "auto_reply"));
    }
    if (u.includes("toolbar_source_performance") && m === "POST") return resp(null, { status: 201 });
    return resp([]);
  };
  strictEqual(await aggregateSourcePerformance("t"), true);
  const up = reg.find(r => r.m === "POST" && r.u.includes("toolbar_source_performance"));
  ok(up, "tiene que escribir el ranking");
  const filas = JSON.parse(up.b);
  const apollo = filas.find(f => f.mb_email === "_global" && f.source === "apollo");
  strictEqual(apollo.sent, 2);
  strictEqual(apollo.bounces, 1, "el director de vacaciones existe: no es un rebote de Apollo");
  strictEqual(filas.find(f => f.mb_email === "_global" && f.source === "scrape").bounces, 1, "una fila vieja sin tipo sí cuenta");
});

// ── R4. El idioma del re-engagement ─────────────────────────────────────────────────────
test("R4: el re-engagement elige el idioma con la regla del agente, no `language || \"en\"`", async (t) => {
  const w = await cargarWorker(["runReengagementCycle"], { fetchFalso: true });
  const DOM = "diario-ejemplo.com.ar";
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase();
    reg.push({ u, m, b: String(opts.body || "") });
    if (!u.includes("/rest/v1/")) return resp({ message: "sin red" }, { status: 500 });   // la página del sitio, Claude
    if (u.includes("toolbar_config?select=key,value")) return resp([
      { key: "agent_reengagement_enabled", value: "true" },
      { key: "agent_active_hours_start", value: "0" }, { key: "agent_active_hours_end", value: "24" },
    ]);
    if (u.includes("toolbar_agent_actions?action=eq.sent&created_at=lt.")) {
      return resp([{ id: 7, domain: DOM, user_email: MB, details: { email: `info@${DOM}` }, created_at: "2026-09-01T10:00:00Z" }]);
    }
    if (u.includes("toolbar_agent_actions?domain=eq.") && u.includes("action=in.(sent,re_sent)")) return resp([], { total: 1 });
    if (u.includes("toolbar_review_queue?domain=eq.")) {
      return resp([{ id: 5, emails: [`info@${DOM}`, `publicidad@${DOM}`], language: "", geo: "", category: "" }]);
    }
    if (u.includes("toolbar_agent_actions") && m === "POST") return resp([{ id: 99 }], { status: 201 });
    return resp([]);
  };
  // Un martes: el re-engagement no corre en fin de semana.
  t.mock.timers.enable({ apis: ["Date"], now: Date.parse("2026-09-15T10:00:00Z") });
  await w.runReengagementCycle("t");
  t.mock.timers.reset();
  const borradores = reg.filter(r => r.u.includes("toolbar_pitch_drafts?language=eq."));
  ok(borradores.length, `tiene que llegar a elegir la plantilla: ${reg.map(r => r.u.split("?")[0]).join(" | ")}`);
  ok(borradores.every(r => r.u.includes("language=eq.es")), `un .com.ar sin idioma guardado es castellano: ${borradores.map(r => r.u).join(" | ")}`);
  ok(!reg.some(r => r.u.includes("gmail.googleapis.com")), "sin MV 'ok' no sale nada (el camino exige ok)");
});

test("R4: el lead trae `geo` y el respaldo de plantillas usa el mismo idioma", () => {
  ok(/select=id,emails,language,geo,/.test(cuerpoDe("pickNextEmailCandidate")), "`_idiomaParaEnvio` mira el GEO del lead");
  const fn = cuerpoDe("runReengagementCycle");
  const codigo = fn.split("\n").filter(l => !l.trim().startsWith("//")).join("\n");
  ok(/_idioma = await _idiomaParaEnvio\(\{ lead, domain, token \}\)/.test(codigo));
  ok(/pickAnyTemplate\(token, userEmail, _idioma\)/.test(codigo));
  ok(/pickRandomTemplate\(_idioma\)/.test(codigo));
  ok(!/lead\.language \|\| "en"/.test(codigo), "el inglés por defecto no puede volver");
});

// ── R5. Los bloqueados del CRM salen de Prospects ──────────────────────────────────────────
const bloqueados = (n) => Array.from({ length: n }, (_, i) => `bloq${i}.com`);
function enrutadorCrm(reg, o = {}) {
  return async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase(), b = String(opts.body || "");
    reg.push({ u, m, b });
    if (u.includes("toolbar_config?select=key,value")) return resp([]);
    if (u.includes("/dominios-activos")) {
      return o.crm === "falla" ? resp({ message: "caído" }, { status: 503 }) : resp({ domains: o.dominios || [], clientesActivos: 3 });
    }
    if (u.includes("toolbar_review_queue?status=eq.pending&domain=in.(")) {
      if (o.lecturaFalla) return resp({ message: "boom" }, { status: 500 });
      const pedidos = decodeURIComponent(u.match(/domain=in\.\(([^)]*)\)/)[1]).split(",").map(x => x.replace(/"/g, ""));
      return resp(pedidos.filter(d => o.pool?.has(d)).map(d => ({ id: o.idDe(d) })));
    }
    if (u.includes("toolbar_review_queue?id=in.(") && m === "PATCH") {
      return resp(u.match(/id=in\.\(([^)]*)\)/)[1].split(",").map(id => ({ id: Number(id) })));
    }
    return resp([]);
  };
}
const idsDe = (reg) => reg.filter(r => r.m === "PATCH" && r.u.includes("toolbar_review_queue?id=in."))
  .flatMap(p => p.u.match(/id=in\.\(([^)]*)\)/)[1].split(",").map(Number));

test("R5: con la lista del CRM leída bien, los bloqueados que esperan en Prospects salen rechazados y reversibles", async () => {
  const w = await cargarWorker(["guardarBloqueadosDeMonday", "_ajustesDeReactivacion"], { fetchFalso: true });
  const reg = [];
  const pool = new Set(["bloq3.com", "bloq250.com", "bloq449.com"]);
  const ids = new Map([...pool].map((d, i) => [d, 100 + i]));
  globalThis.__fetchFalso = enrutadorCrm(reg, { dominios: [...bloqueados(450), "WWW.Bloq3.com"], pool, idDe: d => ids.get(d) });
  await w.guardarBloqueadosDeMonday("t");

  ok(reg.some(r => r.u.includes("toolbar_config") && r.b.includes("bloq449.com")), "la lista se sigue guardando");
  deepStrictEqual(idsDe(reg).sort((a, b) => a - b), [100, 101, 102]);
  const patches = reg.filter(r => r.m === "PATCH" && r.u.includes("toolbar_review_queue?id=in."));
  for (const p of patches) {
    ok(p.u.includes("status=eq.pending"), "lo que un MB pasó a su cola mientras tanto no se toca");
    const body = JSON.parse(p.b);
    strictEqual(body.status, "rejected", "sale con status, nunca con DELETE");
    strictEqual(body.suspect_reject, true);
    ok(!Number.isNaN(Date.parse(body.rejected_at)), "con rejected_at: lo cuenta el renglón SACADAS DE PROSPECTS del parte");
    ok(/^purge: crm_no_recontactar/.test(body.suspect_reason), body.suspect_reason);
    strictEqual(w._ajustesDeReactivacion({ status: "rejected", suspect_reject: true, suspect_reason: body.suspect_reason }).extra.suspect_reject, false,
      "si el CRM lo recicla más adelante, vuelve limpio: la marca es automática");
  }
  const lecturas = reg.filter(r => r.u.includes("toolbar_review_queue?status=eq.pending&domain=in.("));
  strictEqual(lecturas.length, 3, "450 dominios (el www. es el mismo), de a 200");
  ok(lecturas.every(r => Number((r.u.match(/[?&]limit=(\d+)/) || [])[1] || 0) <= 1000), "ninguna lectura pide más de 1.000 filas");
  ok(reg.some(r => r.b.includes("sacado(s) de Prospects")), "el latido dice cuántos salieron");
});

test("R5: si la lectura del CRM falla o viene corta, Prospects no se toca", async () => {
  for (const o of [{ crm: "falla" }, { dominios: bloqueados(40) }]) {
    const w = await cargarWorker(["guardarBloqueadosDeMonday"], { fetchFalso: true });
    const reg = [];
    globalThis.__fetchFalso = enrutadorCrm(reg, { ...o, pool: new Set(["bloq3.com"]), idDe: () => 1 });
    await w.guardarBloqueadosDeMonday("t");
    ok(reg.some(r => r.u.includes("/dominios-activos")), "tiene que haber leído el CRM");
    ok(!reg.some(r => r.u.includes("toolbar_review_queue")), `no se toca Prospects con ${JSON.stringify(o).slice(0, 30)}`);
  }
});

test("R5: tope por corrida, y un lote que no se puede leer se saltea sin tocarlo", async () => {
  const { _sacarBloqueadosDeProspects } = await cargarWorker(["_sacarBloqueadosDeProspects"], { fetchFalso: true });
  const doms = bloqueados(1000);
  let reg = [];
  globalThis.__fetchFalso = enrutadorCrm(reg, { pool: new Set(doms), idDe: d => Number(d.replace(/\D/g, "")) + 1 });
  const r = await _sacarBloqueadosDeProspects("t", doms, { tope: 250 });
  strictEqual(r.sacados, 250);
  ok(r.topeAlcanzado);
  strictEqual(idsDe(reg).length, 250, "nunca más que el tope");
  strictEqual(reg.filter(x => x.u.includes("status=eq.pending&domain=in.(")).length, 2, "alcanzado el tope no se sigue leyendo");

  reg = [];
  globalThis.__fetchFalso = enrutadorCrm(reg, { lecturaFalla: true, pool: new Set(doms), idDe: () => 1 });
  const r2 = await _sacarBloqueadosDeProspects("t", doms.slice(0, 300));
  strictEqual(r2.sacados, 0);
  strictEqual(r2.problemas.length, 2, "cada lote que falla queda dicho");
  ok(!reg.some(x => x.m === "PATCH"), "sin leer no se rechaza nada");
});
