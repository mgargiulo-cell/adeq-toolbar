// La revisión final del 13/09, grupo worker_final: lo que quedaba desincronizado entre el pulido, el
// rescate del reintento por rebote, la auditoría del pool y lo que aprende el autopilot. (2026-09-13e)
//
// "Base estable, no parches diarios": cada test es una regla, y falla con el código anterior (ed79ea1).
//   B1. El pulido y el rescate del reintento guardaban el gmail del registrante (informer + webmail) que la
//       entrada y la auditoría sacan: un rescate falso en el parte, la espera vuelta a cero, y la auditoría
//       lo vaciaba otra vez en cada pasada. Y en la pasada general ese gmail contaba como contacto bueno.
//   B2. El vaciado de la auditoría no conocía registrante_webmail: lo anotaba sin_email_valido y lo
//       contaba como basura en la alerta del freno.
//   B3. El autopilot aprendía rubros de rechazos que no dicen nada del rubro: tráfico, CRM, GEO, sitio
//       caído, subdominio duplicado y "sacada sin filtro".
//   B4. El pulido pisaba la marca de respaldo apollo_sin_contacto sin el filtro que el agente y la
//       auditoría sí respetan, y la quema siguiente volvía a pagar reveals por el mismo dominio.
//
// Run: npm test
import { test, mock } from "node:test";
import { ok, strictEqual, deepStrictEqual, notStrictEqual, match } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateKeyPairSync } from "node:crypto";
import { cargarWorker } from "./_worker-exportado.mjs";
import { _bouncedCache } from "../lib/email.js";

// lib/config.js se evalúa una vez por proceso: el secreto del CRM y la cuenta de servicio de Gmail van
// antes de la primera carga. Sin Serper ni MillionVerifier: ningún camino de estos tests puede gastar.
process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";
delete process.env.SERPER_API_KEY;
delete process.env.MILLIONVERIFIER_API_KEY;
if (!process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
  const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON = JSON.stringify({
    client_email: "worker-final-test@falso.iam.gserviceaccount.com",
    private_key: privateKey.export({ type: "pkcs8", format: "pem" }),
  });
}

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
function limpiarRebotes() { _bouncedCache.set = new Set(); _bouncedCache.ts = 0; }
const DIA = 86_400_000;

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
// La página de website.informer.com de un dominio: el WHOIS con el gmail de quien lo registró.
const informer = (porDominio) => (ruta) => porDominio[ruta.slice(1)]
  ? respuesta(`<!doctype html><html><body><h1>Whois</h1><p>Registrant email: <a href="mailto:${porDominio[ruta.slice(1)]}">${porDominio[ruta.slice(1)]}</a></p></body></html>`, { tipo: "text/html; charset=utf-8" })
  : null;
const TRES_DEL_AREA = { people: [{ id: "a", title: "Advertising Director" }, { id: "b", title: "Marketing Director" }, { id: "c", title: "CEO" }] };
const SIN_EMAIL = { search: () => respuesta(TRES_DEL_AREA), match: () => respuesta({ person: { id: "x", first_name: "Ana" } }) };

// La espera de la consulta del pulido (3, 10 y 30 días según los intentos), para que la base de mentira
// devuelva lo mismo que la de verdad vuelta tras vuelta.
const pulidoLoPide = (l) => {
  const sinEmail = !(l.emails || []).length || String(l.email_ultimo_motivo || "").startsWith("sin_direccion_enviable") || l.email_ultimo_motivo === "solo_hipotesis_de_patron";
  const ui = l.email_ultimo_intento ? Date.parse(l.email_ultimo_intento) : null, n = l.email_intentos;
  const antesDe = (dias) => ui < Date.now() - dias * DIA;
  return sinEmail && (ui == null || ((n == null || n <= 3) && antesDe(3)) || (n >= 4 && n <= 7 && antesDe(10)) || (n >= 8 && antesDe(30)));
};

// Una base que respeta lo que cada pedido pide: estado, marca, la espera del pulido, "con emails" de la
// auditoría y el filtro que no pisa apollo_sin_contacto, también en los PATCH. Apollo y los sitios inventados.
function base({ config = {}, leads = [], diagAlta = 201, apollo = SIN_EMAIL, sitios = {}, dnsVacio = [] } = {}) {
  const pedidos = [];
  const registro = new Set();
  const fn = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase(), b = typeof opts.body === "string" ? opts.body : "";
    pedidos.push({ u, m, b });
    if (u.includes("api.apollo.io/v1/mixed_people/api_search")) return apollo.search(b);
    if (u.includes("api.apollo.io/v1/people/match")) return apollo.match(b);
    if (u.includes("toolbar_diag_sin_email?fase=eq.apollo")) {
      const dentro = decodeURIComponent((u.match(/domain=in\.\(([^)]*)\)/) || [])[1] || "").split(",");
      return respuesta(dentro.filter(d => registro.has(d)).map(domain => ({ domain })));
    }
    if (/toolbar_diag_sin_email$/.test(u) && m === "POST") {
      if (diagAlta < 300 && JSON.parse(b).fase === "apollo") registro.add(JSON.parse(b).domain);
      return respuesta("", { status: diagAlta });
    }
    if (u.includes("/rest/v1/toolbar_config?select=key,value") || u.includes("/rest/v1/toolbar_config?key=in.")) return respuesta(Object.entries(config).map(([key, value]) => ({ key, value })));
    if (u.includes("/rest/v1/toolbar_config?key=eq.") && m === "PATCH") return respuesta([{ key: "x" }]);
    if (u.includes("/rest/v1/toolbar_bounced_emails")) return respuesta([]);
    if (u.includes("/rest/v1/toolbar_review_queue?") && m === "PATCH") {
      const q = decodeURIComponent(u);
      const id = (q.match(/[?&]id=eq\.([^&]+)/) || [])[1];
      const marca = (q.match(/[?&]email_ultimo_motivo=eq\.([^&]+)/) || [])[1];
      const filtroAgente = q.includes("or=(email_ultimo_motivo.is.null,email_ultimo_motivo.neq.apollo_sin_contacto)");
      for (const l of leads) {
        if (String(l.id) !== id || (marca && l.email_ultimo_motivo !== marca)) continue;
        if (filtroAgente && l.email_ultimo_motivo === "apollo_sin_contacto") continue;
        Object.assign(l, JSON.parse(b));
      }
      return respuesta("", { status: 204 });
    }
    if (u.includes("/rest/v1/toolbar_review_queue?status=") && m === "GET") {
      const q = decodeURIComponent(u);
      let filas = leads.filter(l => q.includes(`status=eq.${l.status || "pending"}`) || (q.includes("status=in.(") && q.includes(l.status || "pending")));
      const marca = (q.match(/[?&]email_ultimo_motivo=eq\.([^&]+)/) || [])[1];
      if (marca) filas = filas.filter(l => l.email_ultimo_motivo === marca);
      if (q.includes("or=(email_ultimo_motivo.is.null,email_ultimo_motivo.neq.apollo_sin_contacto)")) filas = filas.filter(l => l.email_ultimo_motivo !== "apollo_sin_contacto");
      if (q.includes("or(emails.is.null,emails.eq.[],")) filas = filas.filter(pulidoLoPide);
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
const patchesA = (pedidos, id) => pedidos.filter(p => p.m === "PATCH" && p.u.includes(`toolbar_review_queue?id=eq.${id}`));
const altasDiag = (pedidos) => pedidos.filter(p => p.m === "POST" && /toolbar_diag_sin_email$/.test(p.u)).map(p => JSON.parse(p.b));
const reveals = (pedidos) => pedidos.filter(p => p.u.includes("people/match")).length;
const PULIDO_GRATIS = { polish_pool: "true", polish_use_apollo: "false", polish_serper_personas: "false", polish_rol_mx: "false", polish_patron: "false", auditoria_emails_busca_mejor: "false" };

// ── B1. El webmail del registrante no es un rescate ─────────────────────────────────────────────
const DOM = "baladaregional.com.br";
const GMAIL = "rudnypc@gmail.com";

test("B1: el gmail del registrante que trae informer no es un rescate: el pulido cuenta el intento, la espera crece y la auditoría no tiene nada que vaciar, vuelta tras vuelta", async () => {
  limpiarRebotes();
  const w = await cargarWorker(["polishPool", "auditarEmailsDelPool", "_recontarRebotesPorDominio"], { fetchFalso: true });
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
  try {
    const lead = { id: 11, domain: DOM, status: "pending", emails: [], email_sources: {}, contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: "2026-09-01T00:00:00Z", language: "pt", email_intentos: 2, email_ultimo_intento: "2026-09-20T00:00:00.000Z", email_ultimo_motivo: "la_web_no_publica_ningun_email" };
    const r = base({ config: PULIDO_GRATIS, leads: [lead], sitios: { [DOM]: sitio("<p>Sin contacto publicado</p>"), "website.informer.com": informer({ [DOM]: GMAIL }) } });
    globalThis.__fetchFalso = r.fn;
    for (let vuelta = 1; vuelta <= 2; vuelta++) {
      const antes = r.pedidos.length;
      await w.polishPool("t");
      ok(r.pedidos.slice(antes).some(p => p.u === `https://website.informer.com/${DOM}`), `vuelta ${vuelta}: el test sólo prueba algo si el rastreo llegó a informer`);
      deepStrictEqual(lead.emails, [], `vuelta ${vuelta}: antes quedaba ${GMAIL} en la ficha, con fuente informer`);
      ok(!lead.email_found_at, `vuelta ${vuelta}: antes marcaba email_found_at, el rescate que suma el parte`);
      strictEqual(lead.email_intentos, 2 + vuelta, `vuelta ${vuelta}: cuenta el intento; antes volvía a 0 y el lead se saltaba la espera`);
      ok(String(lead.email_ultimo_motivo).includes(GMAIL), `el motivo dice qué se descartó: ${lead.email_ultimo_motivo}`);
      await w.auditarEmailsDelPool("t");
      mock.timers.tick(4 * DIA);   // vence la espera de 3 días del pulido y la cadencia de 84 h de la auditoría
    }
    ok(!r.pedidos.some(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue") && p.b.includes("email_found_at")), "ningún rescate en ninguna vuelta");
    ok(!r.pedidos.some(p => (p.u + p.b).includes("polish_enriquecidos_hoy")), "el parte no suma 'emails encontrados a leads que no tenían'");
    ok(!r.pedidos.some(p => p.b.includes("auditoria_vacio")), "la auditoría no tuvo que vaciar a nadie");
    w._recontarRebotesPorDominio([]);
  } finally { mock.timers.reset(); limpiarRebotes(); }
});

test("B1: en la pasada general el gmail del registrante no es 'ya tiene contacto'; un gmail publicado en el propio sitio sí se guarda", async () => {
  limpiarRebotes();
  const { polishPool } = await cargarWorker(["polishPool"], { fetchFalso: true });
  const DOM_A = "vozdointerior.com.br", DOM_B = "anuncieregional.com.br", GMAIL_SITIO = "anuncieregional@gmail.com";
  const stock = { id: 21, domain: DOM_A, status: "pending", emails: ["dono.registro@gmail.com"], email_sources: { "dono.registro@gmail.com": { source: "informer" } }, contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: "2026-09-01T00:00:00Z", language: "pt", email_intentos: 0 };
  const control = { id: 22, domain: DOM_B, status: "pending", emails: [], email_sources: {}, contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: "2026-09-02T00:00:00Z", language: "pt", email_intentos: 0 };
  const r = base({
    config: { ...PULIDO_GRATIS, polish_only_missing: "false" },
    leads: [stock, control],
    sitios: {
      [DOM_A]: sitio(`<p>Anuncie: <a href="mailto:publicidade@${DOM_A}">publicidade@${DOM_A}</a></p>`),
      [DOM_B]: sitio(`<p>Anuncie: <a href="mailto:${GMAIL_SITIO}">${GMAIL_SITIO}</a></p>`),
    },
  });
  globalThis.__fetchFalso = r.fn;
  await polishPool("t");
  strictEqual(stock.emails[0], `publicidade@${DOM_A}`, `antes el pulido lo daba por resuelto (hasGood) y no le buscaba nada: ${JSON.stringify(patchesA(r.pedidos, 21).map(p => p.b))}`);
  deepStrictEqual(control.emails, [GMAIL_SITIO], "la regla sólo mira informer: un gmail que el sitio publica es su contacto");
  strictEqual(String(control.email_sources[GMAIL_SITIO]), "scrape");
  limpiarRebotes();
});

test("B1: el rescate del reintento por rebote no guarda el gmail del registrante: sin otra dirección, es 'sin alternativa'", async () => {
  limpiarRebotes();
  const { queueBounceRetry } = await cargarWorker(["queueBounceRetry"], { fetchFalso: true });
  const DOM_R = "gazetaregional.com.br", REBOTADA = `comercial@${DOM_R}`, MB = "sales@adeqmedia.com";
  const r = base({ sitios: { [DOM_R]: sitio("<p>Sin contacto publicado</p>"), "website.informer.com": informer({ [DOM_R]: GMAIL }) } });
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase(), b = typeof opts.body === "string" ? opts.body : "";
    if (u.includes("toolbar_sendtrack?email=eq.")) { reg.push({ u, m, b }); return respuesta([{ domain: DOM_R }]); }
    if (u.includes("/ficha?domain=")) { reg.push({ u, m, b }); return respuesta({ found: false }); }
    if (u.includes("toolbar_review_queue?domain=eq.") && m === "GET") {
      reg.push({ u, m, b });
      return respuesta([{ id: 5, monday_item_id: null, emails: [REBOTADA], email_sources: { [REBOTADA]: "scrape" }, category: "", traffic: 900000, pitch: "Hola", pitch_subject: "Publicidade", language: "pt", geo: "BR" }]);
    }
    if (u.includes("oauth2.googleapis.com")) { reg.push({ u, m, b }); return respuesta({ access_token: "falso", expires_in: 3600 }); }
    if (u.includes("gmail.googleapis.com")) { reg.push({ u, m, b }); return respuesta({ id: "enviado" }); }
    const x = await r.fn(url, opts);
    reg.push({ u, m, b });
    return x;
  };
  await queueBounceRetry("t", MB, REBOTADA, "hard");
  ok(reg.some(p => p.u === `https://website.informer.com/${DOM_R}`), "el test sólo prueba algo si el rescate llegó a informer");
  deepStrictEqual(patchesA(reg, 5).map(p => JSON.parse(p.b)), [], "antes guardaba {emails:[gmail], email_found_at}: un rescate que el agente nunca manda (informer_webmail)");
  // El gmail del WHOIS tampoco cuenta como "algo encontrado": el respaldo de Google se pide como cuando el sitio no dio
  // nada (sin SERPER_API_KEY en el test no sale a la red; queda anotado el uso del tope diario).
  ok(r.pedidos.some(p => (p.u + p.b).includes("serper_contact_used")), "antes el gmail apagaba el respaldo de Google (viaDe.size === 0)");
  const salto = reg.find(p => p.m === "POST" && p.u.includes("toolbar_bounce_retries"));
  deepStrictEqual([JSON.parse(salto?.b || "{}").status, JSON.parse(salto?.b || "{}").reason], ["skipped_no_alt", "no_alternative_emails_after_rescue"]);
  ok(!reg.some(p => p.m === "POST" && p.u.includes("/messages/send")), "no sale ningún mail");
  limpiarRebotes();
});

test("B1: los dos caminos que guardan lo rastreado pasan por esRegistranteWebmail con la vía real", () => {
  const pulido = cuerpoDe("polishPool");
  match(pulido, /const ranked = _todos\.filter\(r => r\.score > 0 && !_esDelRegistrante\(r\.email\)\)/);
  match(pulido, /const _esDelRegistrante = \(e\) => esRegistranteWebmail\(e, _viaCrawl\(e\)\);/);
  match(pulido, /foundSource = _viaCrawl\(foundEmail\);/);
  match(pulido, /if \(esRegistranteWebmail\(e, src\)\) return false;\s*\n\s*if \(src === "apollo" \|\| src === "informer"\) return true;/);
  // En el reintento sale al anotar cada dirección, antes del respaldo de Google y del filtro de `rescued`.
  const rebote = cuerpoDe("queueBounceRetry");
  match(rebote, /if \(esRegistranteWebmail\(l, via\)\) \{[^\n]*return; \}\s*\n\s*viaDe\.set\(l, via\);/);
  ok(rebote.indexOf("esRegistranteWebmail(l, via)") < rebote.indexOf("if (viaDe.size === 0)"), "antes del respaldo de Google");
});

// ── B2. El vaciado de la auditoría conoce registrante_webmail ───────────────────────────────────
test("B2: el vaciado anota registrante_webmail con su precedencia, y ningún motivo de la auditoría cae en el genérico", async () => {
  const W = await cargarWorker(["_pedidosVaciadoAuditoria", "_planAuditoriaLead"], { fetchFalso: true });
  const motivoDe = (malos) => W._pedidosVaciadoAuditoria({ id: 1, malos: malos.map(motivo => ({ email: "x@x.com", motivo })) })[1].body.email_ultimo_motivo;
  const tabla = [
    [["registrante_webmail"], "auditoria_vacio:registrante_webmail", "antes: sin_email_valido"],
    [["otra_marca", "registrante_webmail"], "auditoria_vacio:registrante_webmail", "antes: otra_marca"],
    [["basura_o_departamento", "registrante_webmail"], "auditoria_vacio:registrante_webmail", "antes: basura_o_departamento"],
    [["registrante_webmail", "ya_reboto"], "auditoria_vacio:ya_reboto", "ya_reboto le sigue ganando a todo"],
    [["basura_o_departamento", "otra_marca"], "auditoria_vacio:otra_marca", "el orden de siempre"],
    [[], "auditoria_vacio:sin_email_valido", "sin motivos, el genérico"],
  ];
  for (const [malos, esperado, porque] of tabla) strictEqual(motivoDe(malos), esperado, `${JSON.stringify(malos)}: ${porque}`);

  // La clase entera: cada motivo que _planAuditoriaLead puede dar, leído del código, tiene su lugar en la
  // precedencia. Un motivo nuevo que caiga en sin_email_valido hace fallar este test.
  const motivos = [...cuerpoDe("_planAuditoriaLead").matchAll(/motivo = "([a-z_]+)"/g)].map(x => x[1]);
  deepStrictEqual([...motivos].sort(), ["basura_o_departamento", "otra_marca", "registrante_webmail", "ya_reboto"]);
  for (const m of motivos) notStrictEqual(motivoDe([m]), "auditoria_vacio:sin_email_valido", m);

  // Y de punta a punta: un lead real que cae en cada motivo deja escrito ese motivo.
  const D = "baladaregional.com.br";
  _bouncedCache.set = new Set([`ventas@${D}`]); _bouncedCache.ts = Date.now();
  try {
    const casos = {
      ya_reboto: [`ventas@${D}`, "scrape"],
      registrante_webmail: [GMAIL, { source: "informer" }],
      basura_o_departamento: [`noreply@${D}`, "scrape"],
      otra_marca: ["ventas@diarioajeno.com", "manual"],
    };
    for (const [motivo, [email, fuente]] of Object.entries(casos)) {
      const { plan } = W._planAuditoriaLead({ id: 9, domain: D, emails: [email], email_sources: { [email]: fuente }, status: "pending", category: "" });
      ok(plan?.vaciaria, `${motivo}: el lead tiene que quedar vacío (${JSON.stringify(plan)})`);
      strictEqual(W._pedidosVaciadoAuditoria(plan)[1].body.email_ultimo_motivo, `auditoria_vacio:${motivo}`, motivo);
    }
  } finally { limpiarRebotes(); }
});

test("B2: la auditoría cuenta el webmail del registrante aparte: en la alerta del freno, en su latido y en su resumen", async () => {
  // Freno: 7 de 20 leads quedarían vacíos sólo por el gmail del WHOIS.
  limpiarRebotes();
  let w = await cargarWorker(["auditarEmailsDelPool", "_recontarRebotesPorDominio"], { fetchFalso: true });
  const registrantes = Array.from({ length: 7 }, (_, i) => ({ id: 100 + i, domain: `registro${i}.com.br`, emails: [`dono${i}@gmail.com`], email_sources: { [`dono${i}@gmail.com`]: { source: "informer" } }, category: "", created_at: `2026-09-01T00:00:0${i}Z`, status: "pending" }));
  const sanos = Array.from({ length: 13 }, (_, i) => ({ id: 200 + i, domain: `sano${i}.com.ar`, emails: [`publicidad@sano${i}.com.ar`], email_sources: {}, category: "", created_at: `2026-09-02T00:00:${String(i).padStart(2, "0")}Z`, status: "validated" }));
  let r = base({ config: { auditoria_emails_busca_mejor: "false" }, leads: [...registrantes, ...sanos] });
  globalThis.__fetchFalso = r.fn;
  await w.auditarEmailsDelPool("t");
  const alerta = r.pedidos.find(p => p.m === "POST" && p.u.includes("toolbar_notifications") && p.b.includes("auditoría de emails"));
  ok(alerta, "el freno tiene que avisar");
  const md = JSON.parse(alerta.b).metadata;
  deepStrictEqual([md.registranteWebmail, md.basura, md.vaciarian], [7, 0, 7], `antes los 7 figuraban como basura: ${JSON.stringify(md)}`);
  w._recontarRebotesPorDominio([]);

  // Sin freno: se aplica, y el latido y el resumen lo dicen con su nombre.
  limpiarRebotes();
  w = await cargarWorker(["auditarEmailsDelPool", "_recontarRebotesPorDominio"], { fetchFalso: true });
  const uno = [{ ...registrantes[0], emails: [...registrantes[0].emails] }];
  r = base({ config: { auditoria_emails_busca_mejor: "false" }, leads: uno });
  globalThis.__fetchFalso = r.fn;
  await w.auditarEmailsDelPool("t");
  strictEqual(uno[0].email_ultimo_motivo, "auditoria_vacio:registrante_webmail");
  const resumen = r.pedidos.find(p => p.m === "PATCH" && p.u.includes("toolbar_config?key=eq.auditoria_emails_ultimo"));
  deepStrictEqual([JSON.parse(JSON.parse(resumen.b).value).registranteWebmail, JSON.parse(JSON.parse(resumen.b).value).basura], [1, 0]);
  const latido = r.pedidos.filter(p => p.m === "POST" && p.u.includes("/rest/v1/toolbar_health") && p.b.includes('"auditoria_emails"')).map(p => p.b).pop() || "";
  match(latido, /1 del registrante/);
  w._recontarRebotesPorDominio([]); limpiarRebotes();
});

// ── B3. Lo que el autopilot aprende de los rechazos ─────────────────────────────────────────────
// [suspect_reason, ¿dice algo del rubro?, por qué]. Los formatos son los que escriben hoy el worker, la
// cola (vía el descongelador), la extensión y colaEstado.js.
const TABLA_RECHAZOS = [
  [null, true, "el ❌ del MB sin motivo"],
  ["", true, "el ❌ del MB sin motivo, vacío"],
  ["mb: es un cine, no un medio", true, "el ❌ del MB con motivo"],
  ["barrido: parecido a un tipo que descartaste", true, "la marca quedó y la fila la rechazó una persona"],
  ["tipo detectado: ecommerce", true, "la revisión de sospechosos marcó el tipo y el MB la rechazó sin motivo: por eso no es una lista blanca"],
  ["purge: nonpub_ecommerce", true, "el pulido juzgó el tipo de sitio"],
  ["purge: sin_ads_txt", true, "no vende inventario"],
  ["purge: sin_evidencia_monetizacion (ads.txt unknown)", true, "no monetiza"],
  ["purge: blocklist:adult-keyword", true, "la lista dura juzga el rubro"],
  ["purge: blocklist:corporate/brand", true, "la lista dura juzga el tipo"],
  ["purge: blocklist:geo-blacklist-tld", false, "GEO por TLD"],
  ["descongelado: blocked: government/education", true, "la misma lista dura, por la cola"],
  ["descongelado: blocked: adult-keyword", true, "la misma lista dura, por la cola"],
  ["descongelado: blocked: geo-blacklist-tld", false, "GEO por TLD, por la cola"],
  ["descongelado: not_publisher: unreachable:timeout", false, "sitio caído (classifyPublisher sin retry)"],
  ["descongelado: worker_cat_not_priority:Shopping", false, "la prioridad de categorías de la config, no un juicio"],
  ["descongelado: ya_estaba_en_prospects", false, "duplicado"],
  ["urlpurge: url_casa_apuestas", true, "el rubro leído en la URL"],
  ["envio: url_tienda", true, "el rubro leído en la URL al mandar"],
  ["descongelado: not_publisher: sin_ads_txt", true, "la cola juzgó el tipo"],
  ['descongelado: no_prospectable_tipo: "Banks" es banco (ni con ads.txt ni con tráfico)', true, "tipo no prospectable"],
  ['descongelado: category-blocked: "Gambling" matchea "gambling"', true, "categoría bloqueada"],
  ["cleanup: trafico_bajo", false, "tráfico"],
  ["cleanup: sin_trafico", false, "tráfico"],
  ["cola: sacada_sin_filtro", false, "no es un juicio sobre el sitio"],
  ["descongelado: pageviews 212345 (visits×2.31) below min 350000", false, "tráfico"],
  ["descongelado: pageviews 90000000 above max 60000000 (gigante — venta directa, no prospección)", false, "tráfico"],
  ['descongelado: crm_activo: estado="LIVE" en "Clientes" (deal activo/con dueño — no se re-prospecta)', false, "CRM"],
  ["descongelado: en descanso: cerró hace poco, faltan 20 días para reintentar", false, "CRM"],
  ["descongelado: deprio-geo: US (USA/UK/CA/AU/NZ/IE no procesados)", false, "GEO"],
  ["descongelado: worker_geo_excluded:US", false, "GEO"],
  ["descongelado: blocked: crm-no-recontactar", false, "CRM"],
  ["descongelado: duplicate_subdomain_of:diario.com", false, "duplicado"],
  ["descongelado: dead_domain_dns_fail", false, "sitio caído"],
  ["descongelado: not_publisher: gigante_120M_pageviews", false, "not_publisher por tráfico"],
  ["descongelado: not_publisher: Gigante_120M", false, "por tráfico, con mayúscula"],
  ["descongelado: not_publisher: bajo_trafico_90000", false, "not_publisher por tráfico"],
  ["purge: crm_no_recontactar_diario", false, "CRM"],
  ["purge: blocklist:crm-no-recontactar", false, "CRM"],
  ["purge: gigante_90000000pv", false, "tráfico"],
  ["purge: bajo_trafico_120000", false, "tráfico"],
  ["purge: unreachable", false, "sitio caído"],
  ["purge: unreachable:ENOTFOUND", false, "sitio caído"],
  ["purge: subdominio_duplicado_de:diario.com", false, "duplicado"],
  ["urlpurge: gigante_120M_pageviews", false, "tráfico"],
  ["urlpurge: bajo_trafico_90000", false, "tráfico"],
  ["envio: bajo_trafico_90000", false, "tráfico"],
];

// Lo justo de PostgREST para evaluar un or=(...) con is.null, like, ilike, and y or anidados. `*` y `%` son
// cualquier cosa, `_` un carácter; null en un like no es verdadero.
function filtroPostgrest(expr) {
  expr = expr.trim();
  const m = expr.match(/^(and|or)\((.*)\)$/s);
  if (m) {
    const partes = []; let prof = 0, cur = "", comillas = false;
    for (const ch of m[2]) {
      if (ch === '"') comillas = !comillas;
      if (!comillas && ch === "(") prof++;
      if (!comillas && ch === ")") prof--;
      if (!comillas && prof === 0 && ch === ",") { partes.push(cur); cur = ""; continue; }
      cur += ch;
    }
    partes.push(cur);
    const hijos = partes.map(filtroPostgrest);
    return m[1] === "and" ? (f) => hijos.every(h => h(f) === true) : (f) => hijos.some(h => h(f) === true);
  }
  const i = expr.indexOf(".");
  const campo = expr.slice(0, i), op = expr.slice(i + 1);
  if (op === "is.null") return (f) => f[campo] == null;
  const lk = op.match(/^(not\.)?(i?like)\."(.*)"$/s);
  if (!lk) throw new Error(`operador que el test no conoce: ${expr}`);
  const re = new RegExp("^" + lk[3].replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/[*%]/g, ".*").replace(/_/g, ".") + "$", lk[2] === "ilike" ? "is" : "s");
  return (f) => (f[campo] == null ? null : (lk[1] ? !re.test(f[campo]) : re.test(f[campo])));
}

test("B3: la consulta del autopilot a la base cuenta sólo los rechazos que juzgan el rubro, fila por fila de la tabla", async () => {
  // Sólo getRejectionPatterns: con el código anterior también carga, y el mensaje dice qué contaba de más.
  const { getRejectionPatterns } = await cargarWorker(["getRejectionPatterns"], { fetchFalso: true });
  // La base de mentira aplica el or=(...) de la URL.
  const filas = TABLA_RECHAZOS.map(([motivo], i) => ({ category: `rubro${i}`, geo: "AR", suspect_reason: motivo }));
  let url = "";
  globalThis.__fetchFalso = async (u) => {
    url = String(u);
    const or = new URLSearchParams(url.split("?")[1] || "").get("or");
    const f = or ? filtroPostgrest(`or${or}`) : () => true;
    return respuesta(filas.filter(x => f(x) === true).map(({ category, geo }) => ({ category, geo })));
  };
  const r = await getRejectionPatterns("t");
  const esperado = Object.fromEntries(TABLA_RECHAZOS.map(([, cuenta], i) => [`rubro${i}`, cuenta]).filter(([, c]) => c).map(([k]) => [k, 1]));
  const deMas = Object.keys(r.categories || {}).filter(k => !(k in esperado)).map(k => TABLA_RECHAZOS[Number(k.slice(5))]?.[0]);
  deepStrictEqual(r.categories, esperado, `antes contaban también ${JSON.stringify(deMas)} (${url})`);
  ok(url.includes("status=eq.rejected") && /[?&]limit=150(&|$)/.test(url), url);
});

test("B3: qué rechazos le enseñan un rubro al autopilot, en tabla, y los formatos de la tabla son los que el código escribe", async () => {
  const W = await cargarWorker(["_rechazoJuzgaElRubro", "_RECHAZOS_Y_RUBRO", "_destinoHuerfanoFrozen", "CLEANUP_PREFIJO", "CRM_BLOQUEADOS_MOTIVO"], { fetchFalso: true });
  for (const [motivo, cuenta, porque] of TABLA_RECHAZOS) strictEqual(W._rechazoJuzgaElRubro(motivo), cuenta, `${JSON.stringify(motivo)}: ${porque}`);

  // Los formatos de la tabla son los reales.
  strictEqual(W._rechazoJuzgaElRubro(`${W.CLEANUP_PREFIJO} trafico_bajo`), false, "la limpieza escribe con CLEANUP_PREFIJO");
  strictEqual(W._rechazoJuzgaElRubro(W.CRM_BLOQUEADOS_MOTIVO), false, "la limpieza diaria de los bloqueados del CRM");
  // Los rechazos que escribe colaEstado.js: status "rejected" con el motivo en la misma línea (un comentario que
  // nombra suspect_reason no cuenta). Al menos los dos de hoy, sacada sin filtro y bajo el piso.
  const colaEstado = fs.readFileSync(path.join(RAIZ, "..", "modules", "colaEstado.js"), "utf8");
  const deLaCola = [...colaEstado.matchAll(/status: "rejected"[^\n]*suspect_reason: "([^"]+)"/g)].map(m => m[1]);
  ok(deLaCola.length >= 2, `colaEstado.js: ${JSON.stringify(deLaCola)}`);
  for (const m of deLaCola) strictEqual(W._rechazoJuzgaElRubro(m), false, `colaEstado.js: ${m}`);
  // La blocklist, la cola, classifyPublisher y la revisión de sospechosos escriben esto hoy: si cambia, la tabla miente.
  for (const lit of ['return "crm-no-recontactar"', 'return "geo-blacklist-tld"', 'return "adult-keyword"', 'return "government/education"',
    "reason = `tipo detectado: ${type}`", "error_message: `blocked: ${blockReason}`", "reason: `unreachable:${pageContent.deadReason || \"dead\"}`"]) {
    ok(worker.includes(lit), `el worker ya no escribe ${lit}`);
  }
  ok(/suspect_reason: `descongelado: \$\{c\.error_message\}`/.test(cuerpoDe("reconciliarHuerfanosFrozen")), "el descongelador escribe 'descongelado: <error de la cola>'");
  for (const [motivo] of TABLA_RECHAZOS.filter(([m]) => String(m || "").startsWith("descongelado: "))) {
    strictEqual(W._destinoHuerfanoFrozen({ csvStatus: "skipped", csvError: motivo.slice("descongelado: ".length) }), "rejected", `el descongelador rechaza con ${motivo}`);
  }
  for (const [prefijo, cuenta] of W._RECHAZOS_Y_RUBRO) {
    strictEqual(prefijo, prefijo.toLowerCase(), "los prefijos van en minúscula: la regla y el ilike comparan sin mayúsculas");
    strictEqual(typeof cuenta, "boolean");
  }
});

// ── B4. El pulido no pisa apollo_sin_contacto ───────────────────────────────────────────────────
test("B4: los PATCH del pulido separan el motivo, con el filtro que no pisa apollo_sin_contacto", async () => {
  const W = await cargarWorker(["_pedidosPatchDelPulido", "_FILTRO_MOTIVO_QUE_EL_AGENTE_PUEDE_PISAR"], { fetchFalso: true });
  const T = "2026-09-28T10:00:00.000Z";
  const ruta = "/rest/v1/toolbar_review_queue?id=eq.7", filtrada = `${ruta}&${W._FILTRO_MOTIVO_QUE_EL_AGENTE_PUEDE_PISAR}`;
  deepStrictEqual(W._pedidosPatchDelPulido(7, { email_intentos: 3, email_ultimo_intento: T, email_ultimo_motivo: "la_web_no_publica_ningun_email" }),
    [{ ruta, body: { email_intentos: 3, email_ultimo_intento: T } }, { ruta: filtrada, body: { email_ultimo_motivo: "la_web_no_publica_ningun_email" } }],
    "la espera va siempre; el motivo, sólo donde no está la marca de Apollo");
  deepStrictEqual(W._pedidosPatchDelPulido(7, { emails: [], email_intentos: 1, email_ultimo_intento: T, email_ultimo_motivo: "validacion_descarto:scrape" }).map(p => p.ruta), [ruta, filtrada]);
  deepStrictEqual(W._pedidosPatchDelPulido(7, { emails: ["publicidad@x.com"], email_intentos: 0, email_ultimo_motivo: null, email_found_at: T }),
    [{ ruta, body: { emails: ["publicidad@x.com"], email_intentos: 0, email_ultimo_motivo: null, email_found_at: T } }],
    "una dirección NUEVA levanta la marca: un solo PATCH, como antes");
  deepStrictEqual(W._pedidosPatchDelPulido(7, { contact_phone: "+55 11 5555-5555" }), [{ ruta, body: { contact_phone: "+55 11 5555-5555" } }]);
  deepStrictEqual(W._pedidosPatchDelPulido(7, { email_ultimo_motivo: "x" }), [{ ruta: filtrada, body: { email_ultimo_motivo: "x" } }]);
  deepStrictEqual(W._pedidosPatchDelPulido(7, {}), []);
});

test("B4: quema con el alta al registro caída → el pulido pasa por el lead → la quema siguiente no vuelve a pagar", async () => {
  limpiarRebotes();
  const W = await cargarWorker(["apolloQuemarCiclo", "polishPool"], { fetchFalso: true });
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
  try {
    const D = "mudodiario.com.ar";
    const leads = [{ id: 7, domain: D, status: "pending", traffic: 900000, emails: [], email_sources: {}, contact_name: "", contact_phone: "", category: "", created_at: "2026-09-01T00:00:00Z", language: "es", email_intentos: 0, email_ultimo_motivo: null }];
    const cfg = { apollo_api_key: "clave", apollo_calls_month: "0", apollo_calls_month_period: "2026-09-12", apollo_monthly_limit: "2500", apollo_calls_today: "0", apollo_calls_date: "2026-09-28", polish_pool: "true", polish_serper_personas: "false", polish_rol_mx: "false", polish_patron: "false" };
    const sitios = { [D]: sitio("<p>Sin contacto publicado</p>") };
    let r = base({ config: cfg, leads, diagAlta: 500, sitios });
    globalThis.__fetchFalso = r.fn;
    await W.apolloQuemarCiclo("t");
    deepStrictEqual([reveals(r.pedidos), leads[0].email_ultimo_motivo], [3, "apollo_sin_contacto"], "Apollo contestó sin email y el registro no lo anotó: queda la marca de respaldo");

    r = base({ config: cfg, leads, sitios });
    globalThis.__fetchFalso = r.fn;
    await W.polishPool("t");
    strictEqual(reveals(r.pedidos), 0, "el pulido respeta la marca para pagar");
    strictEqual(leads[0].email_intentos, 1, "cuenta el intento");
    strictEqual(leads[0].email_ultimo_motivo, "apollo_sin_contacto", "antes la pisaba con la_web_no_publica_ningun_email");
    const conMotivo = patchesA(r.pedidos, 7).filter(p => "email_ultimo_motivo" in JSON.parse(p.b));
    ok(conMotivo.length && conMotivo.every(p => decodeURIComponent(p.u).includes("or=(email_ultimo_motivo.is.null,email_ultimo_motivo.neq.apollo_sin_contacto)")), `el motivo va con el filtro en la URL: ${conMotivo.map(p => p.u).join(" | ")}`);

    r = base({ config: cfg, leads, sitios });
    globalThis.__fetchFalso = r.fn;
    await W.apolloQuemarCiclo("t");
    strictEqual(reveals(r.pedidos), 0, "antes: 3 reveals más por un dominio que Apollo ya contestó sin contacto");
  } finally { mock.timers.reset(); limpiarRebotes(); }
});

test("B4: si el pulido encuentra un email y la validación lo tira, la marca apollo_sin_contacto queda y el diagnóstico se anota igual", async () => {
  limpiarRebotes();
  const { polishPool } = await cargarWorker(["polishPool"], { fetchFalso: true });
  const D = "sinmxmarcado.com.ar";
  const lead = { id: 8, domain: D, status: "pending", emails: [], email_sources: {}, contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: "2026-09-01T00:00:00Z", language: "es", email_intentos: 2, email_ultimo_motivo: "apollo_sin_contacto" };
  const r = base({ config: PULIDO_GRATIS, leads: [lead], sitios: { [D]: sitio(`<p>Escribinos: <a href="mailto:contacto@${D}">contacto@${D}</a></p>`) }, dnsVacio: [D] });
  globalThis.__fetchFalso = r.fn;
  await polishPool("t");
  deepStrictEqual([lead.email_ultimo_motivo, lead.email_intentos], ["apollo_sin_contacto", 3], "antes: validacion_descarto:scrape, y la quema lo volvía a tomar");
  const diag = altasDiag(r.pedidos).filter(a => a.domain === D && a.fase === "pulido");
  deepStrictEqual(diag.map(a => [a.motivo, a.intento]), [["validacion_descarto", 3]], "la historia del intento queda en toolbar_diag_sin_email");
  limpiarRebotes();
});
