// El pool que ve el media buyer: lo que la revisión de la segunda tanda del 13/09 encontró todavía roto.
//
//   1. Apollo: "no pude preguntar" (401 con la clave vencida, 429, red, tope mensual) se anotaba como
//      "Apollo revisó sin email" por 45 días, y esa fila además apagaba los desbloqueos pagos del pulido.
//   2. Un lead que el agente marca `sin_direccion_enviable:<motivo>` (ninguna de sus direcciones se puede
//      mandar) nunca recibía búsqueda de otra dirección, y si pasaba por el pulido se le borraba la marca
//      con cero emails nuevos.
//
// Los que corren el job entero (base, Apollo y sitios inventados) fallan con el código anterior.
// Run: npm test
import { test, mock } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import { cargarWorker } from "./_worker-exportado.mjs";
import { _bouncedCache } from "../lib/email.js";

function limpiarRebotes() { _bouncedCache.set = new Set(); _bouncedCache.ts = 0; }

const respuesta = (body, { status = 200, tipo = "application/json" } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "", redirected: false, body: null,
  headers: { get: (k) => (String(k).toLowerCase() === "content-type" ? tipo : null) },
  json: async () => (typeof body === "string" ? JSON.parse(body) : body),
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
  arrayBuffer: async () => new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body)).buffer,
});
// Una home de medio con publicidad (pasa la puerta del pulido) y lo que se le quiera agregar.
const home = (cuerpo) => respuesta(`<!doctype html><html><head><title>Diario regional</title></head><body><header>Noticias</header>${cuerpo}<div class="ad-slot" id="div-gpt-ad-1"></div><script src="https://securepubads.g.doubleclick.net/tag/js/gpt.js"></script></body></html>`, { tipo: "text/html; charset=utf-8" });
const ADS = "google.com, pub-1234567890123456, DIRECT, f08c47fec0942fa0\nappnexus.com, 1234, RESELLER\n";
const sitio = (cuerpo) => (ruta) => ruta === "/ads.txt" ? respuesta(ADS, { tipo: "text/plain" }) : (ruta === "/" ? home(cuerpo) : null);

// Base, Apollo y sitios inventados. `config` puede depender de cuántos reveals se pagaron (el contador sube).
function enrutador({ config = {}, leads = [], sitios = {}, apollo = null, diagApollo = [], mv = {} } = {}) {
  const pedidos = [];
  let reveals = 0;
  const fn = async (url, opts = {}) => {
    const u = String(url);
    const m = String(opts.method || "GET").toUpperCase();
    const b = typeof opts.body === "string" ? opts.body : "";
    pedidos.push({ u, m, b });
    const cfg = typeof config === "function" ? config(reveals) : config;
    if (u.includes("api.apollo.io/v1/mixed_people/api_search")) return apollo ? apollo.search(b) : respuesta({ people: [] });
    if (u.includes("api.apollo.io/v1/people/match")) { reveals++; return apollo ? apollo.match(b) : respuesta({}); }
    if (u.includes("toolbar_diag_sin_email?fase=eq.apollo")) return respuesta(diagApollo.map(domain => ({ domain })));
    if (/toolbar_diag_sin_email$/.test(u) && m === "POST") return respuesta("", { status: 201 });
    if (u.includes("/rest/v1/toolbar_mv_results?email=eq.")) {
      const e = decodeURIComponent(u.split("email=eq.")[1].split("&")[0]);
      return respuesta(mv[e] ? [{ result: mv[e] }] : []);
    }
    if (u.includes("/rest/v1/toolbar_config?select=key,value") || u.includes("/rest/v1/toolbar_config?key=in.")) return respuesta(Object.entries(cfg).map(([key, value]) => ({ key, value })));
    if (u.includes("/rest/v1/toolbar_bounced_emails")) return respuesta([]);
    if (u.includes("/rest/v1/toolbar_review_queue?status=") && m === "GET") return respuesta(leads);
    if (u.includes("/rest/v1/")) return respuesta([]);
    if (/dns-query|dns\.google/.test(u)) return respuesta({ Status: 0, Answer: [{ data: "10 mx.correo.net." }] });
    let host = "", ruta = "";
    try { const p = new URL(u); host = p.hostname.replace(/^www\./, ""); ruta = p.pathname; } catch {}
    if (typeof sitios[host] === "function") { const r = await sitios[host](ruta, u); if (r) return r; }
    return respuesta("not found", { status: 404, tipo: "text/html" });
  };
  return { pedidos, fn };
}
const altasApollo = (pedidos) => pedidos.filter(p => p.m === "POST" && /toolbar_diag_sin_email$/.test(p.u)).map(p => JSON.parse(p.b));
const patchesA = (pedidos, id) => pedidos.filter(p => p.m === "PATCH" && p.u.includes(`toolbar_review_queue?id=eq.${id}`)).map(p => JSON.parse(p.b));
const latido = (pedidos, job) => pedidos.filter(p => p.m === "POST" && p.u.includes("/rest/v1/toolbar_health") && p.b.includes(`"${job}"`)).map(p => p.b).pop() || "";
const TRES_DEL_AREA = { people: [{ id: "a", title: "Advertising Director" }, { id: "b", title: "Marketing Director" }, { id: "c", title: "CEO" }] };

// ── 1. Apollo: sólo se anota lo que Apollo contestó ──────────────────────────────────────────────
test("'Apollo revisó sin email' es sólo lo que Apollo contestó: nunca un 401, un 429, la red o el tope", async () => {
  const { _apolloContestoSinEmail } = await cargarWorker(["_apolloContestoSinEmail"]);
  for (const s of ["pagado_sin_email", "sin_roles"]) strictEqual(_apolloContestoSinEmail({ salida: s }), true, s);
  for (const s of ["busqueda_http_401", "busqueda_http_429", "busqueda_http_500", "busqueda_sin_respuesta", "tope", "ritmo", "match_fallido", "no_califica", "gratis", "revelado", ""]) {
    strictEqual(_apolloContestoSinEmail({ salida: s }), false, s);
  }
  strictEqual(_apolloContestoSinEmail(null), false);
});

test("findBestApolloEmail dice por qué no trajo email, y devuelve lo mismo que antes", async () => {
  const { findBestApolloEmail, _apolloCyclePeriod } = await cargarWorker(["findBestApolloEmail", "_apolloCyclePeriod"], { fetchFalso: true });
  const periodo = _apolloCyclePeriod();
  const hoy = new Date().toISOString().slice(0, 10);
  const correr = async (apollo, usados = 0) => {
    const r = enrutador({ apollo, config: (n) => ({ apollo_calls_month: String(usados + n), apollo_calls_month_period: periodo, apollo_monthly_limit: "2500", apollo_calls_today: "0", apollo_calls_date: hoy }) });
    globalThis.__fetchFalso = r.fn;
    const res = {};
    const out = await findBestApolloEmail("diariodelsur.com.ar", "clave", "t", { forceUnlock: true, resumenOut: res });
    return { out, res, reveals: r.pedidos.filter(p => p.u.includes("people/match")).length };
  };
  const sinEmail = { search: () => respuesta(TRES_DEL_AREA), match: () => respuesta({ person: { id: "x", first_name: "Ana" } }) };

  let c = await correr({ search: () => respuesta({ error: "invalid api key" }, { status: 401 }), match: () => respuesta({}) });
  deepStrictEqual([c.out, c.res.salida, c.reveals], [null, "busqueda_http_401", 0], "clave vencida: no pudo preguntar");
  c = await correr(sinEmail);
  deepStrictEqual([c.out, c.res.salida, c.res.pagados], [null, "pagado_sin_email", 3], "reveló a los tres y ninguno tenía email");
  c = await correr(sinEmail, 2499);
  deepStrictEqual([c.out, c.res.salida, c.res.pagados], [null, "tope", 1], "el tope cortó a mitad: a los otros dos no se les preguntó");
  c = await correr({ ...sinEmail, match: (b) => JSON.parse(b).id === "b" ? respuesta({ error: "rate limited" }, { status: 429 }) : respuesta({ person: { id: "x" } }) });
  deepStrictEqual([c.res.salida, c.res.pagados], ["match_fallido", 2], "un 429 en un reveal no es 'no hay nadie'");
  c = await correr({ search: () => respuesta({ people: [{ id: "r", title: "Reporter" }] }), match: () => respuesta({}) });
  deepStrictEqual([c.out, c.res.salida, c.reveals], [null, "sin_roles", 0], "contestó: no hay nadie del área");
  c = await correr({ ...sinEmail, match: () => respuesta({ person: { id: "x", email: "ana.ruiz@diariodelsur.com.ar", first_name: "Ana" } }) });
  deepStrictEqual([c.out?.email, c.res.salida], ["ana.ruiz@diariodelsur.com.ar", "revelado"]);
});

test("la quema de Apollo no anota lo que no pudo preguntar: la clave vencida y el tope cortan la vuelta, lo pagado sí se anota", async () => {
  limpiarRebotes();
  const { apolloQuemarCiclo } = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
  const leads = ["uno", "dos", "tres", "cuatro"].map((n, i) => ({ id: 100 + i, domain: `${n}diario.com.ar`, traffic: 900000 - i, emails: [], email_sources: {}, contact_name: "" }));
  const config = (usados, dia) => ({ apollo_api_key: "clave", apollo_calls_month: String(usados), apollo_calls_month_period: "2026-09-12", apollo_monthly_limit: "2500", apollo_calls_today: "0", apollo_calls_date: dia });
  const buscados = (pedidos) => pedidos.filter(p => p.u.includes("mixed_people/api_search")).map(p => JSON.parse(p.b).q_organization_domains_list[0]);
  try {
    // A mitad del ciclo con cero gastado (atrasado) y la clave vencida.
    mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
    let r = enrutador({ leads, config: () => config(0, "2026-09-28"), apollo: { search: () => respuesta({ error: "invalid api key" }, { status: 401 }), match: () => respuesta({}) } });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    deepStrictEqual(altasApollo(r.pedidos), [], "un 401 no es 'Apollo no tiene a nadie': antes quedaban los 4 anotados por 45 días");
    deepStrictEqual(buscados(r.pedidos), ["unodiario.com.ar"], "con la clave vencida la vuelta termina en el primero");
    ok(latido(r.pedidos, "apollo_quemar_ciclo").includes('"fail"'), "y el job lo dice en rojo");

    // Último día del ciclo, 2494 de 2500: dos dominios pagan 3 reveals cada uno y el tope corta.
    mock.timers.reset();
    mock.timers.enable({ apis: ["Date"], now: new Date("2026-10-11T12:00:00Z") });
    r = enrutador({ leads, config: (n) => config(2494 + n, "2026-10-11"), apollo: { search: () => respuesta(TRES_DEL_AREA), match: () => respuesta({ person: { id: "x", first_name: "Ana" } }) } });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    deepStrictEqual(altasApollo(r.pedidos).map(a => `${a.domain}/${a.fase}`), ["unodiario.com.ar/apollo", "dosdiario.com.ar/apollo"], "sólo los que pagaron sus reveals");
    ok(altasApollo(r.pedidos).every(a => /reveló 3 persona/.test(a.comentario)));
    deepStrictEqual(buscados(r.pedidos), ["unodiario.com.ar", "dosdiario.com.ar", "tresdiario.com.ar"], "tres llegó al tope: no se anota y la vuelta termina");
    ok(!latido(r.pedidos, "apollo_quemar_ciclo").includes('"fail"'), "gastar el ciclo entero no es una falla");

    // Apollo contesta que no hay nadie del área: es una respuesta, se anota sin pagar.
    mock.timers.reset();
    mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
    r = enrutador({ leads: leads.slice(0, 2), config: () => config(0, "2026-09-28"), apollo: { search: () => respuesta({ people: [{ id: "r", title: "Sports Reporter" }] }), match: () => respuesta({}) } });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    deepStrictEqual(altasApollo(r.pedidos).map(a => a.domain), ["unodiario.com.ar", "dosdiario.com.ar"]);
    ok(altasApollo(r.pedidos).every(a => /no tiene a nadie/.test(a.comentario)));
    strictEqual(r.pedidos.filter(p => p.u.includes("people/match")).length, 0);
  } finally {
    mock.timers.reset();
    limpiarRebotes();
  }
});

test("el pulido paga Apollo donde la quema no pudo preguntar, anota lo que pagó sin email, y no paga la marca vieja apollo_sin_contacto", async () => {
  limpiarRebotes();
  const leadVacio = (id, domain, extra = {}) => ({ id, domain, emails: [], email_sources: {}, contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: "2026-09-01T00:00:00Z", language: "es", email_intentos: 0, ...extra });
  const correr = async (lead, apollo) => {
    const { polishPool, _apolloCyclePeriod } = await cargarWorker(["polishPool", "_apolloCyclePeriod"], { fetchFalso: true });
    const r = enrutador({
      config: { polish_pool: "true", polish_serper_personas: "false", polish_rol_mx: "false", polish_patron: "false", apollo_api_key: "clave", apollo_calls_month: "0", apollo_calls_month_period: _apolloCyclePeriod(), apollo_monthly_limit: "2500", apollo_calls_today: "0", apollo_calls_date: new Date().toISOString().slice(0, 10) },
      leads: [lead], sitios: { [lead.domain]: sitio("<p>Sin contacto publicado</p>") }, apollo,
    });
    globalThis.__fetchFalso = r.fn;
    await polishPool("t");
    return { reveals: r.pedidos.filter(p => p.u.includes("people/match")).length, busquedas: r.pedidos.filter(p => p.u.includes("mixed_people/api_search")).length, altas: altasApollo(r.pedidos).filter(a => a.fase === "apollo") };
  };
  const sinEmail = { search: () => respuesta(TRES_DEL_AREA), match: () => respuesta({ person: { id: "x", first_name: "Ana" } }) };

  let c = await correr(leadVacio(51, "pagadiario.com.ar"), sinEmail);
  strictEqual(c.reveals, 3, "sin fila de 'revisado' el pulido paga");
  deepStrictEqual(c.altas.map(a => a.domain), ["pagadiario.com.ar"], "y lo que pagó sin email queda anotado (fase apollo, aparte del intento del rastreo): la próxima espera no lo repaga");
  ok(/reveló 3 persona.*\(pulido\)/.test(c.altas[0].comentario));
  c = await correr(leadVacio(52, "claveviejadiario.com.ar"), { search: () => respuesta({ error: "invalid api key" }, { status: 401 }), match: () => respuesta({}) });
  deepStrictEqual(c.altas, [], "un 401 en el pulido tampoco se anota");
  c = await correr(leadVacio(53, "marcaviejadiario.com.ar", { email_ultimo_motivo: "apollo_sin_contacto" }), sinEmail);
  deepStrictEqual([c.busquedas, c.reveals], [1, 0], "la marca vieja de la quema: sólo la búsqueda gratis");
  limpiarRebotes();
});

// ── 2. Leads marcados sin dirección enviable ─────────────────────────────────────────────────────
test("la marca del agente se reconoce por su texto, y sólo una dirección NUEVA la borra", async () => {
  const { _leadSinDireccionEnviable, _armarPatchDeRescate } = await cargarWorker(["_leadSinDireccionEnviable", "_armarPatchDeRescate"]);
  strictEqual(_leadSinDireccionEnviable({ email_ultimo_motivo: "sin_direccion_enviable:mv_dudoso", emails: ["juan.perez@x.com"] }), true);
  strictEqual(_leadSinDireccionEnviable({ email_ultimo_motivo: "sin_direccion_enviable:all_candidates_undeliverable" }), true);
  strictEqual(_leadSinDireccionEnviable({ email_ultimo_motivo: "sin_direccion_enviable" }), true);
  strictEqual(_leadSinDireccionEnviable({ email_ultimo_motivo: "solo_hipotesis_de_patron" }), true);
  strictEqual(_leadSinDireccionEnviable({ email_ultimo_motivo: "apollo_sin_contacto" }), false, "esa es la marca de la quema de Apollo");
  for (const m of [null, undefined, "", "waf_nos_bloqueo", "validacion_descarto:scrape"]) strictEqual(_leadSinDireccionEnviable({ email_ultimo_motivo: m }), false, String(m));
  strictEqual(_leadSinDireccionEnviable(null), false);

  const T = "2026-09-13T10:00:00.000Z";
  const marcado = { id: 1, contact_name: "", contact_phone: "", email_sources: { "info@x.com": "scrape" }, email_intentos: 2, email_ultimo_motivo: "sin_direccion_enviable:mv_dudoso" };
  let r = _armarPatchDeRescate({ lead: marcado, curEmails: ["info@x.com"], foundEmail: "INFO@x.com", foundSource: "scrape", validados: ["info@x.com"], ahoraISO: T });
  strictEqual(r.resultado, "sin_novedad");
  deepStrictEqual(r.patch, { emails: ["info@x.com"], email_intentos: 3, email_ultimo_intento: T }, "la misma dirección de antes: cuenta el intento y la marca queda");
  r = _armarPatchDeRescate({ lead: marcado, curEmails: ["info@x.com"], foundEmail: "publicidad@x.com", foundSource: "scrape", validados: ["publicidad@x.com", "info@x.com"], ahoraISO: T });
  strictEqual(r.resultado, "enriquecido");
  deepStrictEqual([r.patch.email_ultimo_motivo, r.patch.email_intentos, r.elegido], [null, 0, "publicidad@x.com"], "una dirección nueva levanta la marca");
  r = _armarPatchDeRescate({ lead: marcado, curEmails: ["info@x.com"], foundEmail: null, foundPhone: "+54 11 5555-5555", ahoraISO: T });
  deepStrictEqual(r.patch, { contact_phone: "+54 11 5555-5555", email_intentos: 3, email_ultimo_intento: T }, "sólo un teléfono: también cuenta el intento");
  r = _armarPatchDeRescate({ lead: { ...marcado, email_ultimo_motivo: "apollo_sin_contacto" }, curEmails: ["info@x.com"], foundEmail: "info@x.com", foundSource: "scrape", validados: ["info@x.com"], ahoraISO: T });
  strictEqual(r.resultado, "sin_novedad");
  deepStrictEqual(r.patch, { emails: ["info@x.com"] }, "sin la marca: ni borra apollo_sin_contacto, ni cuenta un enriquecido, ni toca la fecha que ordena el pool del agente");
});

test("el pulido pide a los leads marcados sin dirección enviable, con la misma espera, aunque tengan email", async () => {
  limpiarRebotes();
  const { polishPool } = await cargarWorker(["polishPool"], { fetchFalso: true });
  const r = enrutador({ config: { polish_pool: "true", polish_use_apollo: "false" }, leads: [] });
  globalThis.__fetchFalso = r.fn;
  await polishPool("t");
  const q = decodeURIComponent(r.pedidos.find(p => p.m === "GET" && p.u.includes("toolbar_review_queue?status=eq.pending"))?.u || "");
  ok(q.includes("or(emails.is.null,emails.eq.[],email_ultimo_motivo.like.sin_direccion_enviable*,email_ultimo_motivo.eq.solo_hipotesis_de_patron)"), `los marcados entran a la pasada de mudos: ${q}`);
  ok(q.includes("and(or(email_intentos.is.null,email_intentos.lte.3),email_ultimo_intento.lt."), "el agente pone la fecha y no los intentos: NULL cuenta como 0, con su espera de 3 días");
  ok(/select=[^&]*email_ultimo_motivo/.test(q), "y se lee la marca");
  limpiarRebotes();
});

test("el pulido le busca OTRA dirección a un lead marcado: lo que ya tenía o MV ya dio dudoso no cuenta, y sin nada nuevo la marca queda con su intento", async () => {
  limpiarRebotes();
  const { polishPool } = await cargarWorker(["polishPool"], { fetchFalso: true });
  const lead = (id, domain, emails, motivo) => ({ id, domain, emails, email_sources: Object.fromEntries(emails.map(e => [e, "scrape"])), contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: `2026-09-0${id % 9 + 1}T00:00:00Z`, language: "es", email_intentos: 0, email_ultimo_motivo: motivo });
  const leads = [
    lead(61, "radiosur.com.ar", ["juan.perez@radiosur.com.ar"], "sin_direccion_enviable:mv_dudoso"),
    lead(62, "diarionorte.com.ar", ["info@diarionorte.com.ar"], "sin_direccion_enviable:mv_dudoso"),
    lead(63, "litoralhoy.com.ar", ["info@litoralhoy.com.ar"], "sin_direccion_enviable:all_candidates_undeliverable"),
    lead(64, "cronicasur.com.ar", ["info@cronicasur.com.ar"], "solo_hipotesis_de_patron"),
    lead(65, "ecosdelvalle.com.ar", ["juan.gomez@ecosdelvalle.com.ar"], null),
  ];
  const r = enrutador({
    config: { polish_pool: "true", polish_use_apollo: "false", polish_serper_personas: "false", polish_rol_mx: "false", polish_patron: "false" },
    leads,
    mv: { "contacto@cronicasur.com.ar": "unknown" },
    sitios: {
      "radiosur.com.ar": sitio(`<p>Contacto: <a href="mailto:juan.perez@radiosur.com.ar">juan.perez@radiosur.com.ar</a></p>`),
      "diarionorte.com.ar": sitio(`<p>Escribinos: <a href="mailto:info@diarionorte.com.ar">info@diarionorte.com.ar</a></p>`),
      "litoralhoy.com.ar": sitio(`<p>Publicidad: <a href="mailto:publicidad@litoralhoy.com.ar">publicidad@litoralhoy.com.ar</a> · <a href="mailto:info@litoralhoy.com.ar">info@litoralhoy.com.ar</a></p>`),
      "cronicasur.com.ar": sitio(`<p><a href="mailto:contacto@cronicasur.com.ar">contacto@cronicasur.com.ar</a> · <a href="mailto:info@cronicasur.com.ar">info@cronicasur.com.ar</a></p>`),
      "ecosdelvalle.com.ar": sitio(`<p><a href="mailto:juan.gomez@ecosdelvalle.com.ar">juan.gomez@ecosdelvalle.com.ar</a></p>`),
    },
  });
  globalThis.__fetchFalso = r.fn;
  await polishPool("t");
  const rutas = (dom) => [...new Set(r.pedidos.filter(p => p.u.includes(dom) && !p.u.includes("/rest/v1/")).map(p => new URL(p.u).pathname))];
  const intentoSinNovedad = (id) => {
    const p = patchesA(r.pedidos, id);
    strictEqual(p.length, 1, `lead ${id}: un PATCH (${JSON.stringify(p)})`);
    ok(!("email_ultimo_motivo" in p[0]), `lead ${id}: la marca queda (${JSON.stringify(p[0])})`);
    ok(!("emails" in p[0]) || p[0].emails.length > 0, `lead ${id}: no se vacía la lista`);
    strictEqual(p[0].email_intentos, 1, `lead ${id}: cuenta el intento`);
    ok(Date.parse(p[0].email_ultimo_intento) > Date.now() - 60_000, `lead ${id}: con la fecha, que es su espera`);
  };

  ok(rutas("radiosur.com.ar").length > 2, `con una persona dudosa en la ficha, igual se rastrea el sitio (antes sólo "/" y ads.txt): ${rutas("radiosur.com.ar")}`);
  intentoSinNovedad(61);
  intentoSinNovedad(62);   // antes: {"email_intentos":0,"email_ultimo_motivo":null} con el mismo info@
  const p63 = patchesA(r.pedidos, 63);
  strictEqual(p63.length, 1);
  deepStrictEqual(p63[0].emails, ["publicidad@litoralhoy.com.ar", "info@litoralhoy.com.ar"], "la dirección nueva va primera");
  deepStrictEqual([p63[0].email_ultimo_motivo, p63[0].email_intentos], [null, 0], "y levanta la marca");
  ok(r.pedidos.some(p => p.u.includes("toolbar_mv_results?email=eq.contacto%40cronicasur.com.ar")), "se miró el veredicto guardado de MV (lectura gratis)");
  intentoSinNovedad(64);   // contacto@ ya fue dudoso para MV: no es una dirección enviable nueva
  deepStrictEqual(rutas("ecosdelvalle.com.ar").sort(), ["/", "/ads.txt"], "sin marca, un lead con una persona sigue siendo 'ya resuelto'");
  deepStrictEqual(patchesA(r.pedidos, 65), []);
  limpiarRebotes();
});

test("la quema de Apollo levanta la marca del agente cuando agrega una dirección nueva", async () => {
  limpiarRebotes();
  const { apolloQuemarCiclo } = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
  let actual = "";
  try {
    const leads = [
      { id: 71, domain: "marcadodiario.com.ar", traffic: 900000, emails: ["info@marcadodiario.com.ar"], email_sources: { "info@marcadodiario.com.ar": "scrape" }, contact_name: "", email_ultimo_motivo: "sin_direccion_enviable:mv_dudoso" },
      { id: 72, domain: "comundiario.com.ar", traffic: 800000, emails: ["info@comundiario.com.ar"], email_sources: { "info@comundiario.com.ar": "scrape" }, contact_name: "", email_ultimo_motivo: null },
    ];
    const r = enrutador({
      leads,
      config: { apollo_api_key: "clave", apollo_calls_month: "0", apollo_calls_month_period: "2026-09-12", apollo_monthly_limit: "2500", apollo_calls_today: "0", apollo_calls_date: "2026-09-28" },
      apollo: {
        search: (b) => { actual = JSON.parse(b).q_organization_domains_list[0]; return respuesta({ people: [{ id: "a", title: "Advertising Director" }] }); },
        match: () => respuesta({ person: { id: "a", email: `ana.ruiz@${actual}`, first_name: "Ana", last_name: "Ruiz" } }),   // cada dominio, su persona
      },
    });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    const p71 = patchesA(r.pedidos, 71);
    strictEqual(p71.length, 1);
    deepStrictEqual(p71[0].emails, ["ana.ruiz@marcadodiario.com.ar", "info@marcadodiario.com.ar"]);
    deepStrictEqual([p71[0].email_ultimo_motivo, p71[0].email_intentos], [null, 0], "dirección nueva: el pulido ya no tiene que buscarle otra");
    const p72 = patchesA(r.pedidos, 72);
    strictEqual(p72.length, 1);
    ok(!("email_ultimo_motivo" in p72[0]) && !("email_intentos" in p72[0]), "sin marca no se toca el motivo");
  } finally {
    mock.timers.reset();
    limpiarRebotes();
  }
});
