// El pool que ve el media buyer, segunda tanda del 13/09: lo que la revisión encontró todavía roto
// en los jobs que lo mantienen (pulido, auditoría de emails, quema del ciclo de Apollo).
//
// "Lo que arreglás un día no lo rompas al otro." Cada test es una regla, y los que corren el job
// entero (con la base y los sitios inventados) fallan con el código de antes:
//   1. La auditoría y el pulido juzgaban con la lista de rebotes VACÍA después de cada reinicio.
//   2. La mejora de contacto de la auditoría anteponía rrhh@, eventos@ o tienda@ a info@.
//   3. La cola "por enviar" (lo que un MB ya trabajó) se reescribía: ahora sólo se avisa.
//   4. El pulido purgaba de Prospects lo que NO PUDO LEER (ads.txt y home ilegibles, DNS temporal).
//   5. La ficha de rescate se marcaba aunque la validación hubiera tirado el email encontrado.
//   6. Apollo: el intento sin resultado se anotaba en una columna que el pulido pisa, y se repagaba.
//
// Run: npm test
import { test, mock } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { _bouncedCache, rankEmail } from "../lib/email.js";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const sinComentarios = (s) => s.split("\n").map(l => l.replace(/\/\/.*$/, "")).join("\n");

const respuesta = (body, { status = 200, tipo = "application/json" } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "", redirected: false, body: null,
  headers: { get: (k) => (String(k).toLowerCase() === "content-type" ? tipo : null) },
  json: async () => (typeof body === "string" ? JSON.parse(body) : body),
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
  arrayBuffer: async () => new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body)).buffer,
});
const html = (cuerpo) => respuesta(`<!doctype html><html><head><title>Diario</title></head><body>${cuerpo}</body></html>`, { tipo: "text/html; charset=utf-8" });

// La lista de rebotes vive en lib/email.js y la comparten todos los workers que carga este archivo.
function limpiarRebotes() { _bouncedCache.set = new Set(); _bouncedCache.ts = 0; }

// Base y sitios inventados. `sitios[dominio](pathname)` contesta un sitio; lo demás es 404.
function enrutador({ config = {}, rebotes = [], rebotesStatus = 200, leads = [], sitios = {}, extra = () => null } = {}) {
  const pedidos = [];
  const filasConfig = () => Object.entries(config).map(([key, value]) => ({ key, value }));
  const fn = async (url, opts = {}) => {
    const u = String(url);
    const m = String(opts.method || "GET").toUpperCase();
    const b = typeof opts.body === "string" ? opts.body : "";
    pedidos.push({ u, m, b });
    const x = await extra(u, m, b);
    if (x) return x;
    if (u.includes("/rest/v1/toolbar_config?select=key,value") || u.includes("/rest/v1/toolbar_config?key=in.")) return respuesta(filasConfig());
    if (u.includes("/rest/v1/toolbar_config?key=eq.") && m === "PATCH") return respuesta([{ key: "x" }]);
    if (u.includes("/rest/v1/toolbar_bounced_emails")) return rebotesStatus === 200 ? respuesta(rebotes) : respuesta({ message: "boom" }, { status: rebotesStatus });
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
const patchesA = (pedidos, id) => pedidos.filter(p => p.m === "PATCH" && p.u.includes(`toolbar_review_queue?id=eq.${id}`));

// ── 1. La lista de rebotes ──────────────────────────────────────────────────────────────
test("'lista de rebotes leída' es una lectura buena en este proceso, no ts ni el tamaño del set", async () => {
  limpiarRebotes();
  const w = await cargarWorker(["_rebotesListosParaJuzgar", "_recontarRebotesPorDominio"], { fetchFalso: true });
  globalThis.__fetchFalso = enrutador({ rebotesStatus: 500 }).fn;
  strictEqual(await w._rebotesListosParaJuzgar("t"), false, "proceso nuevo y la lectura falló: no se juzga");
  globalThis.__fetchFalso = enrutador({ rebotes: [{ email: "ventas@radiovalle.com.ar", evidencia: "rebote_smtp", fuente: "scrape" }] }).fn;
  strictEqual(await w._rebotesListosParaJuzgar("t"), true);
  ok(_bouncedCache.set.has("ventas@radiovalle.com.ar"));
  _bouncedCache.ts = 0;   // lo que hace el escaneo de rebotes para forzar la recarga
  globalThis.__fetchFalso = enrutador({ rebotesStatus: 500 }).fn;
  strictEqual(await w._rebotesListosParaJuzgar("t"), true, "la recarga forzada falló, pero la lista vieja (y lo que agregó markEmailBounced) sigue sirviendo");
  w._recontarRebotesPorDominio([]); limpiarRebotes();
});

const LEAD_RADIO = { id: 7, domain: "radiovalle.com.ar", emails: ["ventas@radiovalle.com.ar", "info@radiovalle.com.ar"], email_sources: {}, category: "", created_at: "2026-09-01T00:00:00Z", status: "validated" };

test("la auditoría no audita ni mueve el cursor sin la lista de rebotes, y devuelve el turno en vez de esperar 84 h", async () => {
  limpiarRebotes();
  const { auditarEmailsDelPool } = await cargarWorker(["auditarEmailsDelPool"], { fetchFalso: true });
  const r = enrutador({ rebotesStatus: 500, leads: [LEAD_RADIO] });
  globalThis.__fetchFalso = r.fn;
  await auditarEmailsDelPool("t");
  const escrituras = r.pedidos.filter(p => p.u.includes("toolbar_config") && p.m !== "GET");
  ok(!escrituras.some(p => p.u.includes("auditoria_emails_cursor") || p.b.includes("auditoria_emails_cursor")), "el cursor no se mueve: esos 750 leads no quedaron auditados");
  ok(!r.pedidos.some(p => p.u.includes("toolbar_review_queue?status=in.")), "ni siquiera lee el lote");
  ok(escrituras.some(p => p.u.includes("key=eq.cadencia_auditoria_emails") && p.b === JSON.stringify({ value: "" })), "devuelve el turno: la vuelta siguiente reintenta");
  ok(r.pedidos.some(p => p.m === "POST" && p.u.includes("/rest/v1/toolbar_health") && p.b.includes('"auditoria_emails"') && p.b.includes('"fail"')), "y lo dice en rojo");
  limpiarRebotes();
});

test("con la lista leída, el email rebotado sale del lead (con la lista vacía de un reinicio se quedaba)", async () => {
  limpiarRebotes();
  const w = await cargarWorker(["auditarEmailsDelPool", "_recontarRebotesPorDominio"], { fetchFalso: true });
  const r = enrutador({ config: { auditoria_emails_busca_mejor: "false" }, rebotes: [{ email: "ventas@radiovalle.com.ar", evidencia: "rebote_smtp", fuente: "scrape" }], leads: [LEAD_RADIO] });
  globalThis.__fetchFalso = r.fn;
  await w.auditarEmailsDelPool("t");
  const p = patchesA(r.pedidos, 7);
  strictEqual(p.length, 1, "un PATCH para el lead");
  deepStrictEqual(JSON.parse(p[0].b).emails, ["info@radiovalle.com.ar"]);
  ok(r.pedidos.some(x => x.u.includes("key=eq.auditoria_emails_cursor")), "y el cursor avanza porque de verdad auditó");
  w._recontarRebotesPorDominio([]); limpiarRebotes();
});

test("el pulido no escribe sin la lista de rebotes, y el bucle la lee antes de la cadena que juzga emails", async () => {
  limpiarRebotes();
  const { polishPool } = await cargarWorker(["polishPool"], { fetchFalso: true });
  const r = enrutador({ config: { polish_pool: "true", polish_use_apollo: "false" }, rebotesStatus: 500, leads: [{ id: 9, domain: "gacetadelnorte.com.ar", emails: [], email_sources: {}, created_at: "2026-09-01T00:00:00Z" }] });
  globalThis.__fetchFalso = r.fn;
  await polishPool("t");
  ok(!r.pedidos.some(p => p.u.includes("toolbar_review_queue")), "no lee ni escribe el pool");
  ok(r.pedidos.some(p => p.m === "POST" && p.u.includes("/rest/v1/toolbar_health") && p.b.includes('"polish_pool"') && p.b.includes('"fail"')), "y lo dice");
  const i = worker.indexOf("const _hayTiempo = () => Date.now() < _LIMITE_MANTENIMIENTO;");
  const carga = worker.indexOf("await loadBouncedEmails(token).catch(() => {});", i);
  ok(i > 0 && carga > i && carga < worker.indexOf("EL PULIDO VA PRIMERO CUANDO HAY COLA", i), "la lectura va antes de las dos llamadas al pulido y a la auditoría");
  limpiarRebotes();
});

// ── 2. La mejora de contacto de la auditoría ───────────────────────────────────────────
test("una mejora le gana a info@ del mismo sitio y a lo publicado; nunca es departamento, buzón funcional ni infraestructura", async () => {
  limpiarRebotes();
  const w = await cargarWorker(["_esMejoraDeContacto", "_ordenarPorPuntaje"]);
  const d = "diario.com.ar";
  const mejora = (local, base = [`info@${d}`], fuentes = {}) => w._esMejoraDeContacto(`${local}@${d}`, rankEmail(`${local}@${d}`, d, ""), base, fuentes, d, "");
  for (const l of ["rrhh", "empleos", "eventos", "events", "store", "tienda", "reservas", "informatique", "noticias"]) {
    ok(!mejora(l), `${l}@ (${rankEmail(`${l}@${d}`, d, "")}) no mejora a info@ (${rankEmail(`info@${d}`, d, "")})`);
  }
  for (const l of ["publicidad", "comercial", "juan.perez", "editor"]) ok(mejora(l), `${l}@ sí es una mejora`);
  ok(!mejora("rrhh", []), "sin nada publicado, la vara es el info@ del sitio: un departamento sigue sin ser mejora");
  ok(mejora("juan.perez", [`redaccion@${d}`], { [`redaccion@${d}`]: "rol_mx" }), "una dirección ADIVINADA no fija la vara: una persona publicada vale más");
  ok(!mejora("juan.perez", [`redaccion@${d}`], { [`redaccion@${d}`]: "scrape" }), "pero la misma dirección publicada sí");
  deepStrictEqual(w._ordenarPorPuntaje([`info@${d}`, `publicidad@${d}`, `INFO@${d}`], d), [`publicidad@${d}`, `info@${d}`], "mismo orden que la primera pasada, sin duplicados");
});

test("la auditoría no antepone rrhh@/eventos@/tienda@ a info@, y sí guarda publicidad@ primero", async () => {
  limpiarRebotes();
  const w = await cargarWorker(["auditarEmailsDelPool", "scrapeEmailsForDomain"], { fetchFalso: true });
  const leads = [
    { id: 11, domain: "elvallediario.com.ar", emails: ["info@elvallediario.com.ar"], email_sources: { "info@elvallediario.com.ar": "scrape" }, category: "", created_at: "2026-09-01T00:00:00Z", status: "pending" },
    { id: 12, domain: "litoralhoy.com.ar", emails: ["info@litoralhoy.com.ar"], email_sources: { "info@litoralhoy.com.ar": "scrape" }, category: "", created_at: "2026-09-02T00:00:00Z", status: "validated" },
  ];
  const sitios = {
    "elvallediario.com.ar": (ruta) => ruta === "/" ? html(`<p>Trabajá con nosotros: <a href="mailto:rrhh@elvallediario.com.ar">rrhh@elvallediario.com.ar</a></p><p>Eventos: <a href="mailto:eventos@elvallediario.com.ar">eventos@elvallediario.com.ar</a></p><p>Tienda: <a href="mailto:tienda@elvallediario.com.ar">tienda@elvallediario.com.ar</a></p>`) : null,
    "litoralhoy.com.ar": (ruta) => ruta === "/" ? html(`<p>Publicidad: <a href="mailto:publicidad@litoralhoy.com.ar">publicidad@litoralhoy.com.ar</a></p>`) : null,
  };
  const r = enrutador({ leads, sitios });
  globalThis.__fetchFalso = r.fn;
  // El test sólo prueba algo si el crawl de verdad ve esas direcciones.
  const vistos = await w.scrapeEmailsForDomain("elvallediario.com.ar", { sinSerper: true });
  ok(["rrhh@elvallediario.com.ar", "eventos@elvallediario.com.ar"].every(e => vistos.includes(e)), `el crawl tiene que encontrarlas: ${vistos}`);
  await w.auditarEmailsDelPool("t");
  deepStrictEqual(patchesA(r.pedidos, 11).map(p => p.b), [], "rrhh@ (48), eventos@ y tienda@ no son mejores que info@ (55)");
  const p12 = patchesA(r.pedidos, 12);
  strictEqual(p12.length, 1, "publicidad@ sí es una mejora");
  deepStrictEqual(JSON.parse(p12[0].b).emails, ["publicidad@litoralhoy.com.ar", "info@litoralhoy.com.ar"]);
});

// ── 3. La cola "por enviar" ─────────────────────────────────────────────────────────────
test("la primera pasada deja intacta una fila 'por enviar' y avisa si la dirección que se va a mandar está muerta", async () => {
  limpiarRebotes();
  const w = await cargarWorker(["_planAuditoriaLead", "_payloadConAviso"]);
  _bouncedCache.set = new Set(["ventas@portal.pe"]);
  const pend = w._planAuditoriaLead({ id: 1, domain: "portal.pe", emails: ["ventas@portal.pe", "info@portal.pe"], email_sources: {}, status: "validated" });
  deepStrictEqual(pend.plan?.buenos, ["info@portal.pe"], "fuera de la cola se limpia como siempre");
  const cola = w._planAuditoriaLead({ id: 2, domain: "portal.pe", emails: ["ventas@portal.pe"], email_sources: {}, status: "por_enviar" });
  strictEqual(cola.plan, null, "no entra en planes: no cuenta para el freno ni para real/esperado");
  deepStrictEqual(cola.malos, []);
  deepStrictEqual(cola.aviso, { email: "ventas@portal.pe", motivo: "ya_reboto" });
  deepStrictEqual(w._planAuditoriaLead({ id: 3, domain: "portal.pe", emails: ["info@portal.pe"], email_sources: {}, status: "por_enviar" }), { plan: null, malos: [], cambioOrden: false, aviso: null });
  const mp = { estado: "Propuesta Vigente", fecha: "2026-09-10", ejecutivo: "Agus", idioma: 1, mail_enviado: true };
  deepStrictEqual(w._payloadConAviso(mp, { email: "x@y.pe", motivo: "ya_reboto" }, "2026-09-13"), { ...mp, aviso_email: { email: "x@y.pe", motivo: "ya_reboto", fecha: "2026-09-13" } }, "perder mail_enviado haría que el CRM mande un inicial duplicado");
  deepStrictEqual(w._payloadConAviso(null, { email: "x@y.pe", motivo: "otra_marca" }, "d"), { aviso_email: { email: "x@y.pe", motivo: "otra_marca", fecha: "d" } });
  limpiarRebotes();
});

test("la auditoría no cambia los emails de la cola por enviar: avisa fusionando monday_payload y no le busca 'uno mejor'", async () => {
  limpiarRebotes();
  const w = await cargarWorker(["auditarEmailsDelPool", "_recontarRebotesPorDominio"], { fetchFalso: true });
  const mp = { estado: "Propuesta Vigente", fecha: "2026-09-10", ejecutivo: "Agus", idioma: 1, mail_enviado: true };
  const leads = [
    { id: 21, domain: "portalandino.pe", emails: ["ventas@portalandino.pe"], email_sources: {}, category: "", created_at: "2026-09-02T00:00:00Z", status: "por_enviar", monday_payload: mp },
    { id: 22, domain: "cronicasur.pe", emails: ["info@cronicasur.pe"], email_sources: {}, category: "", created_at: "2026-09-03T00:00:00Z", status: "por_enviar", monday_payload: mp },
  ];
  const r = enrutador({
    rebotes: [{ email: "ventas@portalandino.pe", evidencia: "rebote_smtp", fuente: "manual" }],
    leads,
    sitios: { "cronicasur.pe": (ruta) => ruta === "/" ? html(`<a href="mailto:publicidad@cronicasur.pe">publicidad@cronicasur.pe</a>`) : null },
    extra: (u, m) => (m === "GET" && u.includes("toolbar_review_queue?id=eq.21&status=eq.por_enviar")) ? respuesta([{ emails: ["ventas@portalandino.pe"], monday_payload: mp }]) : null,
  });
  globalThis.__fetchFalso = r.fn;
  await w.auditarEmailsDelPool("t");
  const patches = r.pedidos.filter(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue"));
  ok(!patches.some(p => "emails" in JSON.parse(p.b)), `ninguna fila de la cola cambia de emails: ${patches.map(p => p.b).join(" | ")}`);
  const aviso = patchesA(r.pedidos, 21)[0];
  ok(aviso && aviso.u.includes("status=eq.por_enviar"), "el aviso sólo se escribe si la fila sigue en la cola");
  const cuerpo = JSON.parse(aviso.b).monday_payload;
  strictEqual(cuerpo.mail_enviado, true); strictEqual(cuerpo.ejecutivo, "Agus"); strictEqual(cuerpo.estado, "Propuesta Vigente");
  deepStrictEqual([cuerpo.aviso_email.email, cuerpo.aviso_email.motivo], ["ventas@portalandino.pe", "ya_reboto"]);
  ok(!r.pedidos.some(p => /^https?:\/\/(www\.)?cronicasur\.pe/.test(p.u)), "la búsqueda de uno mejor no raspa filas que eligió un MB");
  w._recontarRebotesPorDominio([]); limpiarRebotes();
});

// ── 4. El pulido no purga lo que no pudo leer ───────────────────────────────────────────
test("'no pude verificar' es la misma regla en el pulido y en la entrada", async () => {
  const w = await cargarWorker(["_sinDatosParaJuzgar", "scoreProspectable"]);
  strictEqual(w._sinDatosParaJuzgar({ state: "unknown" }, null), true);
  strictEqual(w._sinDatosParaJuzgar({ state: "unknown" }, { bloqueado: true }), true);
  strictEqual(w._sinDatosParaJuzgar({ state: "unknown" }, { hasDisplayAds: false }), false, "home legible sin publicidad: eso sí se juzga");
  strictEqual(w._sinDatosParaJuzgar({ state: "yes" }, null), false);
  strictEqual(w._sinDatosParaJuzgar({ state: "no" }, null), false, "un 404 del ads.txt es un 'no', no un 'no pude'");
  const casos = [["unknown", null], ["unknown", { bloqueado: true }], ["unknown", { hasDisplayAds: false, title: "x" }], ["yes", null], ["no", null], ["unknown", { hasDisplayAds: true, adNetworks: ["gam"] }]];
  for (const [state, pc] of casos) {
    const v = w.scoreProspectable({ domain: "diario.com.ar", urlVerdict: null, adsTxt: { state, lines: state === "yes" ? 40 : 0 }, pageContent: pc, swCategory: "News & Media", haikuType: null, traffic: 500000 });
    strictEqual(!!v.retry, w._sinDatosParaJuzgar({ state }, pc), `ads.txt ${state} + ${JSON.stringify(pc)}: la entrada dice retry=${!!v.retry}`);
  }
});

test("el pulido deja en Prospects un sitio con ads.txt y home ilegibles, y uno con un tropiezo de DNS (EAI_AGAIN)", async () => {
  limpiarRebotes();
  const { polishPool } = await cargarWorker(["polishPool"], { fetchFalso: true });
  const lead = (id, domain) => ({ id, domain, emails: [], email_sources: {}, contact_name: "", contact_phone: "", category: "", traffic: 900000, created_at: `2026-09-0${id % 9}T00:00:00Z`, language: "es", email_intentos: 0 });
  const prohibido = () => respuesta("<html><head><title>Forbidden</title></head></html>", { status: 403, tipo: "text/html" });
  const r = enrutador({
    config: { polish_pool: "true", polish_use_apollo: "false", polish_serper_personas: "false", polish_rol_mx: "false", polish_patron: "false" },
    leads: [lead(31, "gacetadelnorte.com.ar"), lead(32, "ecosdelsur.com.ar")],
    sitios: {
      "gacetadelnorte.com.ar": prohibido,
      "ecosdelsur.com.ar": (ruta) => {
        if (/ads\.txt$/.test(ruta)) return prohibido();
        throw Object.assign(new Error("getaddrinfo EAI_AGAIN ecosdelsur.com.ar"), { code: "EAI_AGAIN" });
      },
    },
  });
  globalThis.__fetchFalso = r.fn;
  await polishPool("t");
  const rechazos = r.pedidos.filter(p => p.m === "PATCH" && p.u.includes("toolbar_review_queue") && p.b.includes('"rejected"'));
  deepStrictEqual(rechazos.map(p => `${p.u} ${p.b}`), [], "ilegible = reintento, nunca descarte");
  ok(r.pedidos.some(p => p.u.includes("gacetadelnorte.com.ar/ads.txt")) && r.pedidos.some(p => p.u.includes("ecosdelsur.com.ar/ads.txt")), "llegó a leer el ads.txt de los dos");
  ok(/sinDatosPolish\+\+/.test(cuerpoDe("polishPool")) && /sin_datos=\$\{sinDatosPolish\}/.test(cuerpoDe("polishPool")), "y se cuentan aparte en el log");
  limpiarRebotes();
});

// ── 5. La ficha de rescate ──────────────────────────────────────────────────────────────
test("la marca de rescate: sólo si el lead no tenía ningún email y la lista nueva no quedó vacía", async () => {
  const { _marcarRescate } = await cargarWorker(["_marcarRescate"]);
  strictEqual(_marcarRescate({ emails: ["maria@x.com"] }, [], "T").email_found_at, "T");
  strictEqual(_marcarRescate({ emails: ["maria@x.com"] }, ["info@x.com"], "T").email_found_at, undefined, "sumar una persona sobre un info@ es una mejora");
  strictEqual(_marcarRescate({ emails: [] }, [], "T").email_found_at, undefined, "la validación sacó el encontrado: el lead sigue vacío");
  strictEqual(_marcarRescate({ emails: ["maria@x.com"] }, null, "T").email_found_at, "T");
  ok(!("email_found_at" in _marcarRescate({ emails: [] }, ["a@x.com"], "T")), "nunca pone null: borrarla es cosa de la auditoría");
});

test("la ficha sale de lo que sobrevivió a la validación, no de lo que se encontró", async () => {
  const { _armarPatchDeRescate } = await cargarWorker(["_armarPatchDeRescate"]);
  const T = "2026-09-13T10:00:00.000Z";
  const virgen = { id: 1, contact_name: "", contact_phone: "", email_sources: {}, email_intentos: 2 };

  let r = _armarPatchDeRescate({ lead: virgen, curEmails: [], foundEmail: "juan@sinmx.com", foundSource: "scrape", validados: [], ahoraISO: T });
  strictEqual(r.resultado, "descartado");
  deepStrictEqual(r.patch, { emails: [], email_intentos: 3, email_ultimo_intento: T, email_ultimo_motivo: "validacion_descarto:scrape" }, "sin fecha de rescate, sin nombre, y cuenta como intento (respeta la espera)");

  r = _armarPatchDeRescate({ lead: { ...virgen, email_sources: { "info@x.com": "scrape" } }, curEmails: ["info@x.com"], foundEmail: "Ana@x.com", foundSource: "apollo", foundName: "Ana Ruiz", validados: ["info@x.com"], ahoraISO: T });
  strictEqual(r.resultado, "descartado");
  deepStrictEqual(r.patch, { emails: ["info@x.com"] }, "con genéricos: ni nombre de una persona sin email, ni fecha, ni intentos");

  r = _armarPatchDeRescate({ lead: virgen, curEmails: [], foundEmail: "Publicidad@x.com", foundSource: "scrape", validados: ["publicidad@x.com"], ahoraISO: T });
  strictEqual(r.resultado, "rescatado");
  strictEqual(r.patch.email_found_at, T); strictEqual(r.patch.email_intentos, 0); strictEqual(r.patch.email_ultimo_motivo, null);
  deepStrictEqual(r.patch.email_sources, { "publicidad@x.com": "scrape" });

  r = _armarPatchDeRescate({ lead: virgen, curEmails: ["info@x.com"], foundEmail: "ana.ruiz@x.com", foundSource: "apollo", foundName: "Ana Ruiz", validados: ["ana.ruiz@x.com", "info@x.com"], ahoraISO: T });
  strictEqual(r.resultado, "enriquecido");
  ok(!("email_found_at" in r.patch), "ya tenía email: no es un rescate");
  strictEqual(r.patch.contact_name, "Ana Ruiz");

  r = _armarPatchDeRescate({ lead: virgen, curEmails: [], foundEmail: "contacto@x.com", foundSource: "rol_mx", extraRol: ["redaccion@x.com"], validados: ["redaccion@x.com"], ahoraISO: T });
  strictEqual(r.resultado, "rescatado"); strictEqual(r.elegido, "redaccion@x.com");
  deepStrictEqual(r.patch.email_sources, { "redaccion@x.com": "rol_mx" }, "la fuente sólo para lo que quedó");

  r = _armarPatchDeRescate({ lead: virgen, curEmails: [], foundEmail: "juan@sinmx.com", foundSource: "scrape", validados: [], foundPhone: "+54 11 5555-5555", ahoraISO: T });
  strictEqual(r.patch.contact_phone, "+54 11 5555-5555", "el teléfono nuevo se guarda igual");
  deepStrictEqual(_armarPatchDeRescate({ lead: virgen, curEmails: [], foundEmail: null, foundPhone: "wa:5491155555555" }), { patch: { contact_phone: "wa:5491155555555" }, resultado: "telefono", elegido: "" });
});

test("el pulido y Apollo marcan el rescate sólo con la regla compartida", () => {
  for (const nombre of ["polishPool", "apolloQuemarCiclo"]) {
    const fn = sinComentarios(cuerpoDe(nombre));
    ok(!/email_found_at\s*[:=]/.test(fn), `${nombre} no puede volver a escribir email_found_at a mano`);
  }
  ok(/_armarPatchDeRescate\(\{ lead, curEmails, foundEmail, foundSource, foundName, extraRol: _extraRol, validados, foundPhone \}\)/.test(cuerpoDe("polishPool")));
  ok(/if \(validados\.includes\(apLower\)\) \{/.test(cuerpoDe("apolloQuemarCiclo")), "Apollo también mira si su email sobrevivió");
});

// ── 6. Apollo: el intento sin resultado ─────────────────────────────────────────────────
test("la quema de Apollo no repaga lo que ya revisó sin email, anota el intento aparte y no pisa el motivo del crawl", async () => {
  limpiarRebotes();
  const { apolloQuemarCiclo } = await cargarWorker(["apolloQuemarCiclo"], { fetchFalso: true });
  // Mitad del ciclo 12→12 con cero gastado: el job está ATRASADO y tiene que gastar.
  mock.timers.enable({ apis: ["Date"], now: new Date("2026-09-28T10:00:00Z") });
  try {
    const config = { apollo_api_key: "clave-falsa", apollo_calls_month: "0", apollo_calls_month_period: "2026-09-12", apollo_monthly_limit: "2500", apollo_calls_today: "0", apollo_calls_date: "2026-09-28" };
    const leads = [
      { id: 41, domain: "yarevisado.com.ar", traffic: 900000, emails: [], email_sources: {}, contact_name: "" },
      { id: 42, domain: "nuevoapollo.com.ar", traffic: 800000, emails: [], email_sources: {}, contact_name: "" },
    ];
    const armar = ({ diagLectura = 200, diagAlta = 201 } = {}) => enrutador({ config, leads, extra: (u, m) => {
      if (u.includes("toolbar_diag_sin_email?fase=eq.apollo")) return diagLectura === 200 ? respuesta([{ domain: "yarevisado.com.ar" }]) : respuesta({ message: "boom" }, { status: diagLectura });
      if (/toolbar_diag_sin_email$/.test(u) && m === "POST") return respuesta("", { status: diagAlta });
      if (u.includes("api.apollo.io/v1/mixed_people/api_search")) return respuesta({ people: [{ id: "p1", title: "Advertising Director" }] });
      if (u.includes("api.apollo.io/v1/people/match")) return respuesta({ person: { id: "p1", first_name: "Ana" } });   // revelada, sin email
      return null;
    } });

    let r = armar();
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    const busquedas = r.pedidos.filter(p => p.u.includes("mixed_people/api_search")).map(p => p.b);
    ok(busquedas.some(b => b.includes("nuevoapollo.com.ar")), "el lead nuevo sí se prueba (el job no quedó sin trabajo)");
    ok(!busquedas.some(b => b.includes("yarevisado.com.ar")), "el ya revisado sin email en 45 días no se vuelve a pagar");
    const alta = r.pedidos.find(p => p.m === "POST" && /toolbar_diag_sin_email$/.test(p.u));
    ok(alta && JSON.parse(alta.b).fase === "apollo" && JSON.parse(alta.b).domain === "nuevoapollo.com.ar", "el intento queda en la tabla de diagnóstico, fase apollo");
    ok(!r.pedidos.some(p => p.m === "PATCH" && p.b.includes("email_ultimo_motivo")), "la columna queda para el motivo del crawl");

    r = armar({ diagAlta: 400 });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    deepStrictEqual(patchesA(r.pedidos, 42).map(p => JSON.parse(p.b)), [{ email_ultimo_motivo: "apollo_sin_contacto" }], "si no se pudo anotar, la marca vieja evita repagarlo");

    r = armar({ diagLectura: 500 });
    globalThis.__fetchFalso = r.fn;
    await apolloQuemarCiclo("t");
    ok(!r.pedidos.some(p => p.u.includes("api.apollo.io")), "sin poder leer los intentos previos no se gasta a ciegas");
    ok(r.pedidos.some(p => p.m === "POST" && p.u.includes("/rest/v1/toolbar_health") && p.b.includes('"apollo_quemar_ciclo"') && p.b.includes('"fail"')), "y lo dice en rojo");
  } finally {
    mock.timers.reset();
    limpiarRebotes();
  }
});

test("el pulido consulta Apollo sólo gratis donde la quema ya no encontró a nadie", () => {
  const fn = cuerpoDe("polishPool");
  ok(/_dominiosConApolloSinEmail\(token, leads\.map\(l => l\.domain\)\)/.test(fn), "una lectura por tanda, no por lead");
  ok(/allowUnlock: _pagarApollo, forceUnlock: _pagarApollo/.test(fn));
  ok(!/NO se escribe en toolbar_apollo_cache/.test(cuerpoDe("polishPool")) && !/toolbar_apollo_cache/.test(sinComentarios(cuerpoDe("_registrarApolloSinEmail"))),
     "el intento vacío nunca va a toolbar_apollo_cache: la extensión mostraría 'No Apollo data' sin el botón de revelar");
});
