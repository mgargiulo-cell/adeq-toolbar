// Apollo y las marcas del pool: lo que la revisión integrada del 13/09 encontró al juntar los grupos. (2026-09-13c)
//
// "Base estable, no parches diarios": cada test es una regla, y los que corren el job entero (base, Apollo y
// sitios inventados) fallan con el código anterior (a99f382).
//   I6.  La quema leía UNA ventana de 300 sin email: con esos 300 anotados en el registro no probaba a nadie más.
//   I7/I29. Sólo la quema y el pulido miraban "Apollo ya revisó sin email": la entrada, el rescate por rebote,
//        el agente y el autopilot repagaban reveals y no anotaban lo que pagaban sin email.
//   I8.  El boletín y el resumen sumaban las filas de Apollo a los intentos de la búsqueda de email.
//   I9.  La auditoría vaciaba un lead sin motivo: en el stock sin email figuraba "todavía no buscado".
//   I10. La marca vieja apollo_sin_contacto de la columna no vencía nunca.
//   Extras: el freno del 30% salteaba los avisos de la cola por enviar; el pulido sin lista de rebotes la
//   releía en cada vuelta; el pulido no pagaba "a ciegas" sólo en la quema; la rama "descartado" del pulido
//   no dejaba diagnóstico; la quema contaba como mejora un email que el lead ya tenía; import a mitad de archivo.
//
// Run: npm test
import { test, mock } from "node:test";
import { ok, strictEqual, deepStrictEqual, match } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateKeyPairSync } from "node:crypto";
import * as acorn from "acorn";
import * as walk from "acorn-walk";
import { cargarWorker } from "./_worker-exportado.mjs";
import { _bouncedCache } from "../lib/email.js";

// La cuenta de servicio de Gmail tiene que estar antes de la primera carga del worker (lib/config.js).
if (!process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
  const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON = JSON.stringify({
    client_email: "apollo-marcas-test@falso.iam.gserviceaccount.com",
    private_key: privateKey.export({ type: "pkcs8", format: "pem" }),
  });
}

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
function limpiarRebotes() { _bouncedCache.set = new Set(); _bouncedCache.ts = 0; }

const respuesta = (body, { status = 200, tipo = "application/json", total = null } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "", redirected: false, body: null,
  headers: { get: (k) => { const kk = String(k).toLowerCase(); if (kk === "content-type") return tipo; if (kk === "content-range") return `0-0/${total ?? (Array.isArray(body) ? body.length : 0)}`; return null; } },
  json: async () => (typeof body === "string" ? JSON.parse(body) : body),
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
  arrayBuffer: async () => new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body)).buffer,
});
const html = (cuerpo) => respuesta(`<!doctype html><html><head><title>Diario regional</title></head><body><header>Noticias</header>${cuerpo}<div class="ad-slot" id="div-gpt-ad-1"></div><script src="https://securepubads.g.doubleclick.net/tag/js/gpt.js"></script></body></html>`, { tipo: "text/html; charset=utf-8" });
const ADS = "google.com, pub-1234567890123456, DIRECT, f08c47fec0942fa0\nappnexus.com, 1234, RESELLER\n";
const sitio = (cuerpo) => (ruta) => ruta === "/ads.txt" ? respuesta(ADS, { tipo: "text/plain" }) : (ruta === "/" ? html(cuerpo) : null);
const TRES_DEL_AREA = { people: [{ id: "a", title: "Advertising Director" }, { id: "b", title: "Marketing Director" }, { id: "c", title: "CEO" }] };
const SIN_EMAIL = { search: () => respuesta(TRES_DEL_AREA), match: () => respuesta({ person: { id: "x", first_name: "Ana" } }) };
const SIN_ROLES = { search: () => respuesta({ people: [{ id: "r", title: "Sports Reporter" }] }), match: () => respuesta({}) };

// Una base que respeta lo que la consulta pide: estado, marca, sin email, orden por tráfico, limit y offset
// (con el tope de 1.000 filas de PostgREST), y un PATCH que aplica el body sólo a las filas de su filtro.
// El registro de Apollo (toolbar_diag_sin_email fase apollo) crece con cada alta que contesta bien.
function base({ config = {}, leads = [], diag = [], diagLectura = 200, diagAlta = 201, rebotes = [], rebotesStatus = 200, apollo = SIN_ROLES, sitios = {}, dnsVacio = [], extra = () => null } = {}) {
  const pedidos = [];
  const registro = new Set(diag);
  let reveals = 0;
  const fn = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase(), b = typeof opts.body === "string" ? opts.body : "";
    pedidos.push({ u, m, b });
    const x = await extra(u, m, b);
    if (x) return x;
    const cfg = typeof config === "function" ? config(reveals) : config;
    if (u.includes("api.apollo.io/v1/mixed_people/api_search")) return apollo.search(b);
    if (u.includes("api.apollo.io/v1/people/match")) { reveals++; return apollo.match(b); }
    if (u.includes("toolbar_diag_sin_email?fase=eq.apollo")) {
      if (diagLectura !== 200) return respuesta({ message: "boom" }, { status: diagLectura });
      const dentro = decodeURIComponent((u.match(/domain=in\.\(([^)]*)\)/) || [])[1] || "").split(",");
      return respuesta(dentro.filter(d => registro.has(d)).map(domain => ({ domain })));
    }
    if (/toolbar_diag_sin_email$/.test(u) && m === "POST") {
      if (diagAlta < 300 && JSON.parse(b).fase === "apollo") registro.add(JSON.parse(b).domain);
      return respuesta("", { status: diagAlta });
    }
    if (u.includes("/rest/v1/toolbar_config?select=key,value") || u.includes("/rest/v1/toolbar_config?key=in.")) return respuesta(Object.entries(cfg).map(([key, value]) => ({ key, value })));
    if (u.includes("/rest/v1/toolbar_config?key=eq.") && m === "PATCH") return respuesta([{ key: "x" }]);
    if (u.includes("/rest/v1/toolbar_bounced_emails")) return rebotesStatus === 200 ? respuesta(rebotes) : respuesta({ message: "boom" }, { status: rebotesStatus });
    if (u.includes("/rest/v1/toolbar_review_queue?") && m === "PATCH") {
      const q = decodeURIComponent(u);
      const id = (q.match(/[?&]id=eq\.([^&]+)/) || [])[1];
      const marca = (q.match(/[?&]email_ultimo_motivo=eq\.([^&]+)/) || [])[1];
      for (const l of leads) if (String(l.id) === id && (!marca || l.email_ultimo_motivo === marca)) Object.assign(l, JSON.parse(b));
      return respuesta("", { status: 204 });
    }
    if (u.includes("/rest/v1/toolbar_review_queue?status=") && m === "GET") {
      const q = decodeURIComponent(u);
      let filas = leads.filter(l => q.includes(`status=eq.${l.status || "pending"}`) || (q.includes("status=in.(") && q.includes(l.status || "pending")));
      const marca = (q.match(/[?&]email_ultimo_motivo=eq\.([^&]+)/) || [])[1];
      if (marca) filas = filas.filter(l => l.email_ultimo_motivo === marca);
      if (q.includes("or=(email_ultimo_motivo.is.null,email_ultimo_motivo.neq.apollo_sin_contacto)")) filas = filas.filter(l => l.email_ultimo_motivo !== "apollo_sin_contacto");
      if (q.includes("&emails=eq.[]")) filas = filas.filter(l => !(l.emails || []).length);
      if (q.includes("&emails=neq.[]")) filas = filas.filter(l => (l.emails || []).length);
      if (q.includes("order=traffic.desc")) filas = [...filas].sort((a, c) => (c.traffic || 0) - (a.traffic || 0) || a.id - c.id);
      const off = Number((q.match(/[?&]offset=(\d+)/) || [])[1] || 0);
      const lim = Math.min(1000, Number((q.match(/[?&]limit=(\d+)/) || [])[1] || 1000));
      return respuesta(filas.slice(off, off + lim).map(l => ({ ...l })));
    }
    if (u.includes("/rest/v1/")) return respuesta([]);
    if (/dns-query|dns\.google/.test(u)) {
      const nombre = decodeURIComponent((u.match(/name=([^&]+)/) || [])[1] || "");
      return dnsVacio.includes(nombre) ? respuesta({ Status: 3 }) : respuesta({ Status: 0, Answer: [{ data: "10 mx.correo.net." }] });
    }
    let host = "", ruta = "";
    try { const p = new URL(u); host = p.hostname.replace(/^www\./, ""); ruta = p.pathname; } catch {}
    if (typeof sitios[host] === "function") { const r = await sitios[host](ruta, u); if (r) return r; }
    return respuesta("not found", { status: 404, tipo: "text/html" });
  };
  return { pedidos, fn, registro };
}
const buscados = (pedidos) => pedidos.filter(p => p.u.includes("mixed_people/api_search")).map(p => JSON.parse(p.b).q_organization_domains_list[0]);
const altasDiag = (pedidos) => pedidos.filter(p => p.m === "POST" && /toolbar_diag_sin_email$/.test(p.u)).map(p => JSON.parse(p.b));
const latido = (pedidos, job) => pedidos.filter(p => p.m === "POST" && p.u.includes("/rest/v1/toolbar_health") && p.b.includes(`"${job}"`)).map(p => p.b).pop() || "";
const patches = (pedidos, id) => pedidos.filter(p => p.m === "PATCH" && p.u.includes(`toolbar_review_queue?id=eq.${id}`));
// Mitad del ciclo 12→12: con cero gastado la quema está ATRASADA; con 2.000 va al ritmo.
const cfgApollo = (usados, dia) => ({ apollo_api_key: "clave", apollo_calls_month: String(usados), apollo_calls_month_period: "2026-09-12", apollo_monthly_limit: "2500", apollo_calls_today: "0", apollo_calls_date: dia });

// ── I6. La quema pasa la ventana de 300 ──────────────────────────────────────────────────────────
test("la quema pasa la ventana: con los 300 sin email de más tráfico ya revisados, prueba los siguientes", async () => {
  limpiarRebotes();
  const { apolloQuemarCiclo } = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
  try {
    const leads = Array.from({ length: 400 }, (_, i) => ({ id: i + 1, domain: `diario${i + 1}.com.ar`, traffic: 2_000_000 - i, emails: [], email_sources: {}, contact_name: "", email_ultimo_motivo: null }));
    const r = base({ config: cfgApollo(0, "2026-09-28"), leads, diag: leads.slice(0, 300).map(l => l.domain) });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    const lat = latido(r.pedidos, "apollo_quemar_ciclo");
    const presupuesto = Number(JSON.parse(lat || "{}").esperado_ultimo);   // min(presupuesto, candidatos): acá sobran candidatos
    ok(presupuesto > 0, lat);
    const esperados = Array.from({ length: presupuesto }, (_, i) => `diario${301 + i}.com.ar`);
    deepStrictEqual(buscados(r.pedidos), esperados, "antes: 0 pedidos, 'candidatos 300 (300 ya revisados)' y los 100 de atrás nunca se probaban");
    strictEqual(JSON.parse(lat).real_ultimo, presupuesto, "gastó lo que podía");
    ok(!lat.includes('"warn"') && !lat.includes('"fail"'), `con candidatos detrás no es 'sin candidatos': ${lat}`);
    ok(/300 ya revisados sin email, 2 pág\./.test(lat), lat);
    ok(r.pedidos.filter(p => p.u.includes("&emails=eq.%5B%5D&limit=300&offset=")).length <= 10, "con techo de páginas");
  } finally { mock.timers.reset(); limpiarRebotes(); }
});

test("la quema: si la lectura del registro falla sin candidatos juntados, no gasta y lo dice en rojo", async () => {
  limpiarRebotes();
  const { apolloQuemarCiclo } = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
  try {
    const leads = [{ id: 1, domain: "uno.com.ar", traffic: 900000, emails: [], email_sources: {}, contact_name: "" }];
    const r = base({ config: cfgApollo(0, "2026-09-28"), leads, diagLectura: 500 });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    ok(!r.pedidos.some(p => p.u.includes("api.apollo.io")), "sin poder leer los intentos previos no se gasta a ciegas");
    ok(latido(r.pedidos, "apollo_quemar_ciclo").includes('"fail"'));
  } finally { mock.timers.reset(); limpiarRebotes(); }
});

// ── I7 / I29. El registro lo respeta todo camino que paga ────────────────────────────────────────
test("findBestApolloEmail por defecto no repaga lo ya revisado, no paga sin poder leer el registro, y anota lo que contestó sin email", async () => {
  const { findBestApolloEmail, _apolloCyclePeriod } = await cargarWorker(["findBestApolloEmail", "_apolloCyclePeriod"], { fetchFalso: true });
  const periodo = _apolloCyclePeriod();
  const hoy = new Date().toISOString().slice(0, 10);
  const DOM = "rebotado.com.ar";
  const correr = async ({ apollo = SIN_EMAIL, diag = [], diagLectura = 200, usados = 0, opciones = {} } = {}) => {
    const r = base({ apollo, diag, diagLectura, config: (n) => ({ apollo_calls_month: String(usados + n), apollo_calls_month_period: periodo, apollo_monthly_limit: "2500", apollo_calls_today: "0", apollo_calls_date: hoy }) });
    globalThis.__fetchFalso = r.fn;
    const res = {};
    const out = await findBestApolloEmail(DOM, "clave", "t", { traffic: 100, allowUnlock: true, forceUnlock: true, resumenOut: res, ...opciones });
    return { out, res, busquedas: buscados(r.pedidos).length, reveals: r.pedidos.filter(p => p.u.includes("people/match")).length, altas: altasDiag(r.pedidos), leyo: r.pedidos.some(p => p.u.includes("toolbar_diag_sin_email?fase=eq.apollo")) };
  };

  let c = await correr({ diag: [DOM], opciones: { origen: "rescate por rebote" } });
  deepStrictEqual([c.out, c.res.salida, c.busquedas, c.reveals, c.altas.length], [null, "ya_revisado", 1, 0, 0], "ya revisado en 45 días: sólo la búsqueda gratis (antes pagaba 3 reveals)");
  c = await correr({ opciones: { origen: "rescate por rebote" } });
  deepStrictEqual([c.res.salida, c.reveals], ["pagado_sin_email", 3]);
  deepStrictEqual(c.altas.map(a => [a.domain, a.fase, a.motivo]), [[DOM, "apollo", "apollo_sin_contacto"]], "lo pagado sin email queda anotado: el siguiente no lo repaga");
  ok(/reveló 3 persona.*\(rescate por rebote\)/.test(c.altas[0].comentario), c.altas[0].comentario);
  c = await correr({ diagLectura: 500 });
  deepStrictEqual([c.res.salida, c.reveals, c.altas.length], ["registro_ilegible", 0, 0], "'no pude leer' no es 'nadie lo probó'");
  c = await correr({ apollo: SIN_ROLES });
  deepStrictEqual([c.res.salida, c.reveals, c.altas.map(a => a.domain)], ["sin_roles", 0, [DOM]], "Apollo contestó que no hay nadie del área: también se anota");
  c = await correr({ apollo: { search: () => respuesta({ error: "invalid api key" }, { status: 401 }), match: () => respuesta({}) } });
  deepStrictEqual([c.res.salida, c.altas.length], ["busqueda_http_401", 0], "un 401 no se anota");
  c = await correr({ usados: 2499 });
  deepStrictEqual([c.res.salida, c.altas.length], ["tope", 0], "el tope no se anota");
  c = await correr({ opciones: { evitarRepago: false, anotarSinEmail: false } });
  deepStrictEqual([c.leyo, c.reveals, c.altas.length], [false, 3, 0], "la quema y el pulido leen por tanda y anotan por su cuenta");
  c = await correr({ diag: [DOM], opciones: { allowUnlock: false, forceUnlock: false } });
  deepStrictEqual([c.leyo, c.res.salida], [false, "no_califica"], "si no iba a pagar, no lee el registro");
});

test("todo camino que paga Apollo pasa por el registro: sólo la quema y el pulido lo leen y anotan por su cuenta", () => {
  const ast = acorn.parse(worker, { ecmaVersion: "latest", sourceType: "module" });
  const llamadas = [];
  walk.ancestor(ast, {
    CallExpression(n, _s, anc) {
      if (n.callee.type !== "Identifier" || n.callee.name !== "findBestApolloEmail") return;
      const fn = [...anc].reverse().find(a => a.type === "FunctionDeclaration");
      const props = Object.fromEntries((n.arguments[3]?.properties || []).filter(p => p.key).map(p => [p.key.name, worker.slice(p.value.start, p.value.end)]));
      llamadas.push({ fn: fn?.id?.name, props });
    },
  });
  ok(llamadas.length >= 7, JSON.stringify(llamadas.map(l => l.fn)));
  for (const l of llamadas) {
    if (["apolloQuemarCiclo", "polishPool"].includes(l.fn)) {
      deepStrictEqual([l.props.evitarRepago, l.props.anotarSinEmail], ["false", "false"], `${l.fn} lee el registro por tanda y anota por su cuenta`);
    } else {
      ok(!("evitarRepago" in l.props) && !("anotarSinEmail" in l.props), `${l.fn} no puede saltearse el registro de Apollo: ${JSON.stringify(l.props)}`);
    }
  }
  for (const f of ["processCsvItem", "queueBounceRetry", "runAgentCycle"]) ok(llamadas.some(l => l.fn === f), `falta el llamador ${f}`);
});

test("el pulido no paga Apollo a ciegas: si no pudo leer el registro, sólo la búsqueda gratis", async () => {
  limpiarRebotes();
  const { polishPool, _apolloCyclePeriod } = await cargarWorker(["polishPool", "_apolloCyclePeriod"], { fetchFalso: true });
  const lead = { id: 51, domain: "mudodiario.com.ar", emails: [], email_sources: {}, contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: "2026-09-01T00:00:00Z", language: "es", email_intentos: 0 };
  const r = base({
    config: { polish_pool: "true", polish_serper_personas: "false", polish_rol_mx: "false", polish_patron: "false", ...cfgApollo(0, new Date().toISOString().slice(0, 10)), apollo_calls_month_period: _apolloCyclePeriod() },
    leads: [lead], sitios: { [lead.domain]: sitio("<p>Sin contacto publicado</p>") }, apollo: SIN_EMAIL, diagLectura: 500,
  });
  globalThis.__fetchFalso = r.fn;
  await polishPool("t");
  deepStrictEqual([buscados(r.pedidos).length, r.pedidos.filter(p => p.u.includes("people/match")).length], [1, 0], "antes pagaba 3 reveals con la lectura caída");
  limpiarRebotes();
});

// ── I8. El informe separa Apollo del rastreo ─────────────────────────────────────────────────────
const DIAG_MEZCLA = [
  { domain: "coninfo1.com", motivo: "apollo_sin_contacto", fase: "apollo", comentario: "Apollo reveló 3 persona(s)" },
  { domain: "coninfo2.com", motivo: "apollo_sin_contacto", fase: "apollo", comentario: "Apollo reveló 3 persona(s)" },
  { domain: "coninfo3.com", motivo: "apollo_sin_contacto", fase: "apollo", comentario: "Apollo reveló 3 persona(s)" },
  { domain: "w1.com", motivo: "waf_nos_bloqueo", fase: "pulido", comentario: "El sitio está detrás de un WAF" },
  { domain: "w2.com", motivo: "waf_nos_bloqueo", fase: null, comentario: "El sitio está detrás de un WAF" },
];
const W = await cargarWorker(["_separarDiagSinEmail", "_boletinPorSeccion", "enviarResumenSalud", "_pedidosVaciadoAuditoria", "_armarPatchDeRescate", "_agruparStockSinEmail", "_FILTRO_MOTIVO_QUE_EL_AGENTE_PUEDE_PISAR"], { fetchFalso: true });
const baseInforme = (registro) => async (url, opts = {}) => {
  const u = String(url);
  registro.push({ u, m: (opts.method || "GET").toUpperCase(), b: String(opts.body || "") });
  if (u.includes("toolbar_diag_sin_email?created_at=gte.")) {
    const cols = decodeURIComponent((u.match(/select=([^&]+)/) || [])[1] || "").split(",");
    return respuesta(DIAG_MEZCLA.map(d => Object.fromEntries(cols.map(c => [c, d[c]]))));
  }
  // Sin nada pendiente el resumen no se manda: un hallazgo cualquiera para que salga.
  if (u.includes("toolbar_config?select=key,value")) return respuesta([{ key: "salud_resumen_pendiente", value: JSON.stringify([{ clave: "x", titulo: "algo para mirar", severidad: "warn", cuerpo: "detalle" }]) }]);
  if (u.includes("oauth2.googleapis.com")) return respuesta({ access_token: "falso", expires_in: 3600 });
  if (u.includes("gmail.googleapis.com")) return respuesta({ id: "msg-falso" });
  return respuesta([]);
};

test("'Apollo revisó sin email' no es un intento de la búsqueda: se separa por fase, y una fase vacía es del rastreo", async () => {
  const s = W._separarDiagSinEmail(DIAG_MEZCLA);
  deepStrictEqual([s.rastreo.map(d => d.domain), s.apollo], [["w1.com", "w2.com"], 3]);
  deepStrictEqual(W._separarDiagSinEmail(null), { rastreo: [], apollo: 0 }, "una lectura fallida no rompe el informe");

  const registro = [];
  globalThis.__fetchFalso = baseInforme(registro);
  const t = (await W._boletinPorSeccion("token-falso")).join("\n");
  match(t, /Por qué fallan \(2 intentos\): waf_nos_bloqueo 2/, "antes: 'Por qué fallan (5 intentos): apollo_sin_contacto 3 · waf_nos_bloqueo 2'");
  ok(!/Por qué fallan[^\n]*apollo_sin_contacto/.test(t), t);
  match(t, /Apollo revisó sin email 3 dominio\(s\)/);
  ok(registro.some(r => r.u.includes("toolbar_diag_sin_email?created_at=gte.") && r.u.includes("select=motivo,fase")), "el boletín lee la fase");
});

test("el resumen de salud: 'los N intentos' y el caso para verificar son del rastreo, no un dominio con info@ que Apollo revisó", async () => {
  const registro = [];
  globalThis.__fetchFalso = baseInforme(registro);
  await W.enviarResumenSalud("token-falso");
  const envio = registro.find(r => /gmail\.googleapis\.com/.test(r.u) && r.m === "POST" && /"raw"/.test(r.b));
  ok(envio, `el resumen no llegó a Gmail: ${registro.slice(-3).map(r => r.u.slice(0, 90)).join(" | ")}`);
  const mime = Buffer.from(JSON.parse(envio.b).raw.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
  const qp = mime.replace(/=\r?\n/g, "");
  const bytes = [];
  for (let i = 0; i < qp.length; i++) {
    if (qp[i] === "=" && /^[0-9A-F]{2}$/i.test(qp.slice(i + 1, i + 3))) { bytes.push(parseInt(qp.slice(i + 1, i + 3), 16)); i += 2; }
    else bytes.push(...Buffer.from(qp[i], "utf8"));
  }
  const texto = Buffer.from(bytes).toString("utf8");
  match(texto, /SIN EMAIL — los 2 intentos de las últimas 24h/, "antes: 'los 5 intentos', con apollo_sin_contacto primero");
  ok(!texto.includes("coninfo1.com"), "el ejemplo no puede ser un dominio que Apollo revisó (puede tener info@)");
});

// ── I9. La auditoría que vacía deja su motivo ────────────────────────────────────────────────────
test("vaciar un lead deja 'auditoria_vacio:<por qué>': un rescatado que la auditoría vació no figura 'todavía no buscado'", () => {
  const [vaciar, motivo] = W._pedidosVaciadoAuditoria({ id: 5, malos: [{ email: "a@x.com", motivo: "otra_marca" }, { email: "b@x.com", motivo: "ya_reboto" }] });
  deepStrictEqual(vaciar, { ruta: "/rest/v1/toolbar_review_queue?id=eq.5", body: { emails: [], email_found_at: null } });
  deepStrictEqual(motivo.body, { email_ultimo_motivo: "auditoria_vacio:ya_reboto" }, "ya_reboto le gana a otra_marca");
  ok(motivo.ruta.endsWith(`&${W._FILTRO_MOTIVO_QUE_EL_AGENTE_PUEDE_PISAR}`), "no pisa apollo_sin_contacto: la quema volvería a pagar");
  for (const p of [vaciar, motivo]) ok(!("email_intentos" in p.body) && !("email_ultimo_intento" in p.body), "la espera del pulido no se toca");
  strictEqual(W._pedidosVaciadoAuditoria({ id: 6, malos: [{ motivo: "basura_o_departamento" }] })[1].body.email_ultimo_motivo, "auditoria_vacio:basura_o_departamento");

  // El caso del informe: 4 intentos, rescatado por el pulido (intentos 0, motivo null), después la dirección rebota.
  const lead = { id: 7, domain: "x.com", emails: [], email_sources: {}, contact_name: "", contact_phone: "", email_intentos: 4, email_ultimo_motivo: "la_web_no_publica_ningun_email" };
  const { patch } = W._armarPatchDeRescate({ lead, curEmails: [], foundEmail: "ventas@x.com", foundSource: "scrape", validados: ["ventas@x.com"] });
  Object.assign(lead, patch);
  for (const p of W._pedidosVaciadoAuditoria({ id: 7, malos: [{ email: "ventas@x.com", motivo: "ya_reboto" }] })) Object.assign(lead, p.body);
  const g = Object.fromEntries(W._agruparStockSinEmail([lead], 50).grupos.map(([k, v]) => [k, v.n]));
  deepStrictEqual(g, { auditoria_vacio: 1 }, "antes caía en sin_motivo_todavia_no_buscado, junto con un import que nunca se buscó");
});

test("la auditoría escribe el motivo sólo si el vaciado se aplicó, y con el filtro en la URL", async () => {
  for (const vaciadoOk of [true, false]) {
    limpiarRebotes();
    const w = await cargarWorker(["auditarEmailsDelPool", "_recontarRebotesPorDominio"], { fetchFalso: true });
    const leads = [{ id: 7, domain: "radiovalle.com.ar", emails: ["ventas@radiovalle.com.ar"], email_sources: {}, category: "", created_at: "2026-09-01T00:00:00Z", status: "pending" }];
    const r = base({
      config: { auditoria_emails_busca_mejor: "false" }, leads,
      rebotes: [{ email: "ventas@radiovalle.com.ar", evidencia: "rebote_smtp", fuente: "scrape" }],
      extra: (u, m) => (!vaciadoOk && m === "PATCH" && u.endsWith("toolbar_review_queue?id=eq.7")) ? respuesta({ message: "boom" }, { status: 500 }) : null,
    });
    globalThis.__fetchFalso = r.fn;
    await w.auditarEmailsDelPool("t");
    const p = patches(r.pedidos, 7);
    deepStrictEqual(JSON.parse(p[0].b), { emails: [], email_found_at: null });
    if (vaciadoOk) {
      strictEqual(p.length, 2, "vaciado + motivo");
      ok(decodeURIComponent(p[1].u).includes("or=(email_ultimo_motivo.is.null,email_ultimo_motivo.neq.apollo_sin_contacto)"), p[1].u);
      deepStrictEqual(JSON.parse(p[1].b), { email_ultimo_motivo: "auditoria_vacio:ya_reboto" });
    } else {
      strictEqual(p.length, 1, "si el vaciado falló, el motivo no se escribe: el lead sigue con su email");
    }
    w._recontarRebotesPorDominio([]); limpiarRebotes();
  }
});

// ── I10. La marca vieja de la columna vence ──────────────────────────────────────────────────────
test("la marca vieja apollo_sin_contacto pasa al registro con fecha de hoy y se limpia de la columna, vaya la quema al ritmo o no", async () => {
  const leads = () => [
    { id: 1, domain: "viejamarca.com.ar", traffic: 900000, emails: ["info@viejamarca.com.ar"], email_sources: { "info@viejamarca.com.ar": "scrape" }, contact_name: "", email_ultimo_motivo: "apollo_sin_contacto" },
    { id: 2, domain: "sinmarca.com.ar", traffic: 800000, emails: ["contacto@sinmarca.com.ar"], email_sources: { "contacto@sinmarca.com.ar": "scrape" }, contact_name: "", email_ultimo_motivo: null },
  ];
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
  try {
    // Atrasada: convierte, y en la misma vuelta el lead convertido queda afuera por el registro.
    limpiarRebotes();
    let w = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
    let tabla = leads();
    let r = base({ config: cfgApollo(0, "2026-09-28"), leads: tabla });
    globalThis.__fetchFalso = r.fn;
    await w.apolloQuemarCiclo("t");
    const alta = altasDiag(r.pedidos).find(a => a.domain === "viejamarca.com.ar");
    ok(alta && alta.fase === "apollo" && /Marca vieja/.test(alta.comentario), JSON.stringify(altasDiag(r.pedidos)));
    const limpia = patches(r.pedidos, 1);
    strictEqual(limpia.length, 1);
    ok(limpia[0].u.includes("email_ultimo_motivo=eq.apollo_sin_contacto"), "con la marca en la URL, por si otro job la cambió");
    deepStrictEqual(JSON.parse(limpia[0].b), { email_ultimo_motivo: null });
    const iAlta = r.pedidos.findIndex(p => p.m === "POST" && /toolbar_diag_sin_email$/.test(p.u) && p.b.includes("viejamarca"));
    ok(iAlta >= 0 && iAlta < r.pedidos.indexOf(limpia[0]), "el alta va antes de limpiar la columna");
    deepStrictEqual(buscados(r.pedidos), ["sinmarca.com.ar"], "el convertido no se paga en la misma vuelta: lo excluye el registro");

    // Si el alta falla, la marca queda.
    limpiarRebotes();
    w = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
    tabla = leads();
    r = base({ config: cfgApollo(0, "2026-09-28"), leads: tabla, diagAlta: 400 });
    globalThis.__fetchFalso = r.fn;
    await w.apolloQuemarCiclo("t");
    ok(!patches(r.pedidos, 1).length, "sin el alta no se limpia");
    strictEqual(tabla[0].email_ultimo_motivo, "apollo_sin_contacto");
    ok(!buscados(r.pedidos).includes("viejamarca.com.ar"));

    // Al ritmo: la quema no gasta, pero la marca vence igual (también frena al pulido).
    limpiarRebotes();
    w = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
    tabla = leads();
    r = base({ config: cfgApollo(2000, "2026-09-28"), leads: tabla });
    globalThis.__fetchFalso = r.fn;
    await w.apolloQuemarCiclo("t");
    ok(/al ritmo/.test(latido(r.pedidos, "apollo_quemar_ciclo")), latido(r.pedidos, "apollo_quemar_ciclo"));
    deepStrictEqual([altasDiag(r.pedidos).map(a => a.domain), tabla[0].email_ultimo_motivo, buscados(r.pedidos)], [["viejamarca.com.ar"], null, []]);
    // Y no vuelve a leer en cada vuelta.
    const antes = r.pedidos.filter(p => p.u.includes("email_ultimo_motivo=eq.apollo_sin_contacto&select=")).length;
    await w.apolloQuemarCiclo("t");
    strictEqual(r.pedidos.filter(p => p.u.includes("email_ultimo_motivo=eq.apollo_sin_contacto&select=")).length, antes, "cada 6 h, no en cada vuelta del bucle");
  } finally { mock.timers.reset(); limpiarRebotes(); }
});

// ── Extras de la revisión del pool ───────────────────────────────────────────────────────────────
test("el freno del 30% frena los planes, no los avisos de la cola por enviar", async () => {
  limpiarRebotes();
  const w = await cargarWorker(["auditarEmailsDelPool", "_recontarRebotesPorDominio"], { fetchFalso: true });
  const mp = { estado: "Propuesta Vigente", fecha: "2026-09-10", ejecutivo: "Agus", idioma: 1, mail_enviado: true };
  const muertos = Array.from({ length: 7 }, (_, i) => ({ id: 100 + i, domain: `muerto${i}.com.ar`, emails: [`ventas@muerto${i}.com.ar`], email_sources: {}, category: "", created_at: `2026-09-01T00:00:0${i}Z`, status: "pending" }));
  const sanos = Array.from({ length: 13 }, (_, i) => ({ id: 200 + i, domain: `sano${i}.com.ar`, emails: [`publicidad@sano${i}.com.ar`], email_sources: {}, category: "", created_at: `2026-09-02T00:00:${String(i).padStart(2, "0")}Z`, status: "validated" }));
  const cola = { id: 300, domain: "portalandino.pe", emails: ["ventas@portalandino.pe"], email_sources: {}, category: "", created_at: "2026-09-03T00:00:00Z", status: "por_enviar", monday_payload: mp };
  const r = base({
    config: { auditoria_emails_busca_mejor: "false" },
    leads: [...muertos, ...sanos, cola],
    rebotes: [...muertos.map(l => ({ email: l.emails[0], evidencia: "rebote_smtp", fuente: "scrape" })), { email: "ventas@portalandino.pe", evidencia: "rebote_smtp", fuente: "manual" }],
    extra: (u, m) => (m === "GET" && u.includes("toolbar_review_queue?id=eq.300&status=eq.por_enviar")) ? respuesta([{ emails: ["ventas@portalandino.pe"], monday_payload: mp }]) : null,
  });
  globalThis.__fetchFalso = r.fn;
  await w.auditarEmailsDelPool("t");
  ok(/freno de seguridad: vaciaría 7\/21/.test(latido(r.pedidos, "auditoria_emails")), latido(r.pedidos, "auditoria_emails"));
  ok(!r.pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue") && "emails" in JSON.parse(p.b)), "el freno no aplica ningún plan");
  const aviso = patches(r.pedidos, 300)[0];
  ok(aviso && aviso.u.includes("status=eq.por_enviar"), "antes el return del freno salteaba el aviso al MB");
  deepStrictEqual(JSON.parse(aviso.b).monday_payload.aviso_email.email, "ventas@portalandino.pe");
  w._recontarRebotesPorDominio([]); limpiarRebotes();
});

test("el pulido sin lista de rebotes espera a que venza la caché en vez de releerla en cada vuelta", async () => {
  limpiarRebotes();
  const { polishPool } = await cargarWorker(["polishPool"], { fetchFalso: true });
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-14T10:00:00Z") });
  try {
    const r = base({ config: { polish_pool: "true", polish_use_apollo: "false" }, rebotesStatus: 500 });
    globalThis.__fetchFalso = r.fn;
    const lecturas = () => r.pedidos.filter(p => p.u.includes("toolbar_bounced_emails")).length;
    await polishPool("t");
    const n1 = lecturas();
    ok(n1 > 0 && latido(r.pedidos, "polish_pool").includes('"fail"'));
    mock.timers.tick(60_000);
    await polishPool("t");
    strictEqual(lecturas(), n1, "un minuto después (el bucle llama casi en cada vuelta): no se relee");
    mock.timers.tick(5 * 60_000);
    await polishPool("t");
    ok(lecturas() > n1, "vencida la caché de rebotes, reintenta");
    ok(!r.pedidos.some(p => p.u.includes("toolbar_review_queue")), "y sin lista nunca lee ni escribe el pool");
  } finally { mock.timers.reset(); limpiarRebotes(); }
});

test("el pulido deja el diagnóstico cuando encontró un email y la validación lo tiró", async () => {
  limpiarRebotes();
  const { polishPool } = await cargarWorker(["polishPool"], { fetchFalso: true });
  const DOM = "sinmxdiario.com.ar";
  const r = base({
    config: { polish_pool: "true", polish_use_apollo: "false", polish_serper_personas: "false", polish_rol_mx: "false", polish_patron: "false" },
    leads: [{ id: 81, domain: DOM, emails: [], email_sources: {}, contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: "2026-09-01T00:00:00Z", language: "es", email_intentos: 2 }],
    sitios: { [DOM]: sitio(`<p>Escribinos: <a href="mailto:contacto@${DOM}">contacto@${DOM}</a></p>`) },
    dnsVacio: [DOM],   // el dominio no recibe correo: validateEmailsBatch lo descarta
  });
  globalThis.__fetchFalso = r.fn;
  await polishPool("t");
  // Desde la revisión final del 13/09 el motivo va en su propio PATCH, con el filtro que no pisa apollo_sin_contacto
  // (tests/worker_final-13-09e.test.js, B4): se juntan los pedidos para leer lo que quedó en la fila.
  const p = patches(r.pedidos, 81).map(x => JSON.parse(x.b));
  const fila = Object.assign({}, ...p);
  ok(fila.email_ultimo_motivo === "validacion_descarto:scrape" && fila.email_intentos === 3, JSON.stringify(p));
  const diag = altasDiag(r.pedidos).filter(a => a.domain === DOM);
  strictEqual(diag.length, 1, "antes: la columna decía validacion_descarto y la tabla de diagnóstico no tenía nada");
  deepStrictEqual([diag[0].fase, diag[0].motivo, diag[0].intento], ["pulido", "validacion_descarto", 3], "el mismo motivo que la columna");
  ok(diag[0].comentario.includes(`contacto@${DOM}`), diag[0].comentario);
  limpiarRebotes();
});

test("la quema no cuenta como mejora un email que el lead ya tenía (el mismo criterio que el pulido)", async () => {
  limpiarRebotes();
  const { apolloQuemarCiclo } = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
  try {
    const r = base({
      config: cfgApollo(0, "2026-09-28"),
      leads: [{ id: 91, domain: "repetido.com.ar", traffic: 900000, emails: ["info@repetido.com.ar"], email_sources: { "info@repetido.com.ar": "scrape" }, contact_name: "", email_ultimo_motivo: null }],
      apollo: { search: () => respuesta({ people: [{ id: "a", title: "Advertising Director" }] }), match: () => respuesta({ person: { id: "a", email: "info@repetido.com.ar", first_name: "Ana" } }) },
    });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    const lat = latido(r.pedidos, "apollo_quemar_ciclo");
    match(lat, /intentos 1 → 0 con email \(0 rescates, 0 mejoras de genérico a persona, 1 ya estaban en la ficha\)/, "antes: '1 con email (0 rescates, 1 mejoras de genérico a persona)'");
  } finally { mock.timers.reset(); limpiarRebotes(); }
});

test("esBuzonFuncional se importa con el resto de lib/email.js, no a mitad del worker", () => {
  const cabecera = worker.slice(0, worker.indexOf('} from "./lib/email.js";'));
  ok(/\n\s+esBuzonFuncional,\n/.test(cabecera.slice(cabecera.lastIndexOf("import {"))), "en la lista de imports de arriba");
  strictEqual((worker.match(/^import \{ esBuzonFuncional \}/gm) || []).length, 0, "sin el import suelto de la línea ~12400");
});
