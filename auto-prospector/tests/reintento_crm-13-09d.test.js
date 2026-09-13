// Rebotes, reintento y lista del CRM: lo que otros grupos marcaron fuera de su zona el 13/09.
//
//   R1. Un adicional que rebota no encontraba su sitio: `future_sent` sólo queda en agent_actions,
//       nunca en sendtrack, así que el aviso al CRM no salía y el reintento usaba el dominio del correo.
//  R1b. Ese rebote no puede llegar al CRM como `bounced_email`: le vaciaba el Email al principal vivo.
//  R1c. El reintento que sale por esa dirección queda igual en sendtrack: si no, el re-engagement le
//       repetía el pitch a los pocos días.
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
import { generateKeyPairSync } from "node:crypto";
import { cargarWorker } from "./_worker-exportado.mjs";

// lib/config.js se evalúa UNA vez por proceso: el secreto del CRM y la cuenta de servicio de Gmail (R1b:
// el scan de rebotes y el reintento) tienen que estar antes de la primera carga.
process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";
if (!process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
  const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON = JSON.stringify({
    client_email: "reintento-test@falso.iam.gserviceaccount.com",
    private_key: privateKey.export({ type: "pkcs8", format: "pem" }),
  });
}
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
  // Desde R1b (abajo) el scan pide el ORIGEN (sitio + si es la principal); `_sitioDeLaDireccion` es su envoltorio.
  strictEqual((tramo.match(/_(?:sitio|origen)DeLaDireccion\(/g) || []).length, 1, "con el respaldo son hasta dos consultas por llamada: se pedía dos veces");
  ok(/originalDomain: _sitio \|\| failed\.split\("@"\)\[1\]/.test(tramo));
  ok(/reportarReboteAlCrm\(token, \{ email: failed, originalDomain: _sitio,/.test(tramo));
});

// ── R1b. El rebote de un adicional o del 2º email no le vacía el Email al principal ─────────────────
// El CRM (sync-toolbar) no compara `bounced_email` con la dirección cargada: vacía el Email, borra los
// follow-ups y sube otro contacto como principal, o deja la ficha del agente sin email. Con el respaldo
// de R1 esos rebotes encontraban su sitio y el aviso salía. La dirección se quema igual; el aviso al CRM
// y el "email nuevo" del reintento son sólo para la principal.
const CONFIG_CRM = [
  { key: "crm_propio_enabled", value: "true" },
  { key: "monday_bloqueados", value: JSON.stringify(Array.from({ length: 120 }, (_, i) => `bloqueado${i}.com`)) },
  { key: "monday_bloqueados_at", value: new Date().toISOString() },
];
const esperar = (ms) => new Promise(r => setTimeout(r, ms));

test("R1b: el origen separa 'de qué sitio es' de 'es la principal'", async () => {
  const { _origenDeLaDireccion, _sitioDeLaDireccion } = await cargarWorker(["_origenDeLaDireccion", "_sitioDeLaDireccion"], { fetchFalso: true });
  const reg = [];
  const origen = async (o) => { globalThis.__fetchFalso = enrutadorSitio(reg, o); return _origenDeLaDireccion("t", "x@sitio.it"); };
  deepStrictEqual(await origen({ st: [{ domain: "Sitio.it" }] }), { sitio: "sitio.it", via: "sendtrack", accion: null, principal: true },
    "sendtrack lo escriben el agente, el popup y el reintento que reemplaza: siempre la principal");
  for (const accion of ["future_sent", "secondary_sent"]) {
    deepStrictEqual(await origen({ aa: [{ domain: "sitio.it", action: accion, details: {} }] }),
      { sitio: "sitio.it", via: "agent_actions", accion, principal: false }, `${accion} sale a otra persona del sitio`);
  }
  for (const accion of ["sent", "re_sent", "bounce_retry_sent"]) {
    strictEqual((await origen({ aa: [{ domain: "sitio.it", action: accion, details: {} }] })).principal, true, accion);
  }
  strictEqual((await origen({ aa: [{ domain: "sitio.it", action: "bounce_retry_sent", details: { principal: false } }] })).principal, false,
    "el reintento que salió por un adicional tampoco es la dirección de la ficha");
  deepStrictEqual(await origen({ st: "falla", aa: "falla" }), { sitio: "", via: "", accion: null, principal: null });
  const q = reg.filter(u => u.includes("toolbar_agent_actions?email_to=eq.")).pop();
  ok(/select=domain,action,details&/.test(q) && /[?&]limit=1(&|$)/.test(q), q);
  globalThis.__fetchFalso = enrutadorSitio(reg, { aa: [{ domain: "sitio.it", action: "future_sent" }] });
  strictEqual(await _sitioDeLaDireccion("t", "x@sitio.it"), "sitio.it", "quemar la dirección sigue necesitando sólo el sitio");
});

/** Un rebote en Gmail (con X-Failed-Recipients) y la base contestando el origen de la dirección. */
function enrutadorScan(reg, o) {
  return async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase(), b = String(opts.body || "");
    reg.push({ u, m, b });
    if (u.includes("oauth2.googleapis.com")) return resp({ access_token: "falso", expires_in: 3600 });
    if (u.includes("gmail.googleapis.com") && u.includes("messages?q=")) return resp({ messages: [{ id: o.msg }] });
    if (u.includes("gmail.googleapis.com") && u.includes(`messages/${o.msg}?format=full`)) return resp({ payload: {
      headers: [{ name: "X-Failed-Recipients", value: o.direccion }, { name: "Content-Type", value: "multipart/report; report-type=delivery-status" }],
      body: { data: Buffer.from(`Delivery has failed to these recipients or groups:\n${o.direccion}\nRemote server returned '550 5.1.1 User unknown'`).toString("base64") },
    } });
    if (u.includes("toolbar_config")) return resp(CONFIG_CRM);
    if (u.includes("toolbar_sendtrack?email=eq.")) return resp(o.st || []);
    if (u.includes("toolbar_agent_actions?email_to=eq.")) return resp(o.aa || []);
    if (u.includes("toolbar_frozen_leads?domain=eq.")) return resp([{ domain: "sitio.it" }]);   // el reintento corta ahí
    if (m === "POST" && b.includes('"prospects"')) return resp({ ok: 1, errores: [], avisos: [] });
    return resp([]);
  };
}

test("R1b: en el scan, el rebote de un future_sent o un secondary_sent se quema sin aviso al CRM; el de la principal sí se avisa", async () => {
  const casos = [
    { nombre: "adicional del MB", direccion: "redaccion2@sitio.it", aa: [{ domain: "sitio.it", action: "future_sent", details: {} }], avisa: false },
    { nombre: "2º email del agente", direccion: "mario.rossi@gmail.com", aa: [{ domain: "sitio.it", action: "secondary_sent", details: {} }], avisa: false },
    { nombre: "principal del agente sin fila en sendtrack", direccion: "publicidad@sitio.it", aa: [{ domain: "sitio.it", action: "sent", details: {} }], avisa: true },
    { nombre: "principal en sendtrack", direccion: "ventas@sitio.it", st: [{ domain: "sitio.it" }], avisa: true },
  ];
  for (const [i, c] of casos.entries()) {
    const { scanBouncesForUser } = await cargarWorker(["scanBouncesForUser"], { fetchFalso: true });
    const reg = [];
    globalThis.__fetchFalso = enrutadorScan(reg, { ...c, msg: `rebote${i}` });
    strictEqual(await scanBouncesForUser("t", MB), 1, c.nombre);
    await esperar(100);   // el reintento y la acción salen sin await
    const quemada = reg.find(r => r.m === "POST" && r.u.includes("toolbar_bounced_emails"));
    ok(quemada && JSON.parse(quemada.b).email === c.direccion && JSON.parse(quemada.b).original_domain === "sitio.it",
      `${c.nombre}: la dirección se quema con el sitio, sea principal o no`);
    const alCrm = reg.filter(r => r.m === "POST" && r.b.includes('"prospects"'));
    const detectado = reg.find(r => r.m === "POST" && r.u.includes("toolbar_agent_actions") && r.b.includes("bounce_detected"));
    strictEqual(JSON.parse(detectado.b).details.principal, c.avisa, `${c.nombre}: queda dicho en la acción`);
    if (c.avisa) {
      strictEqual(alCrm.length, 1, c.nombre);
      deepStrictEqual(JSON.parse(alCrm[0].b).prospects.map(p => [p.domain, p.bounced_email]), [["sitio.it", c.direccion]]);
    } else {
      // Ningún push: un `bounced_email` vacía el Email del principal, y uno sin email ni bounced_email
      // marca la ficha como contacto por formulario.
      strictEqual(alCrm.length, 0, `${c.nombre}: ${alCrm.map(r => r.b).join(" | ")}`);
    }
  }
});

/** Lo que el reintento necesita para llegar a mandar: el lead, la dirección que cargó el MB y Gmail. */
const DOM_R = "medio-ejemplo.es";
const PITCH = "Hola, te escribo porque vi el sitio y me pareció un medio con muy buena audiencia. Trabajamos con editores de toda la región ayudándolos a sumar ingresos por publicidad sin tocar la experiencia del lector. Si te interesa, te cuento en dos líneas cómo lo hacemos y qué resultados vienen teniendo sitios parecidos.\n\nSaludos";
function enrutadorReintento(reg, o) {
  return async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase(), b = String(opts.body || "");
    reg.push({ u, m, b });
    if (u.includes("oauth2.googleapis.com")) return resp({ access_token: "falso", expires_in: 3600 });
    if (u.includes("gmail.googleapis.com") && u.includes("/messages/send")) return resp({ id: "enviado" });
    if (u.includes("gmail.googleapis.com")) return resp({});
    if (u.includes("toolbar_config")) return resp(CONFIG_CRM);
    if (u.includes("toolbar_sendtrack?email=eq.")) return resp(o.st || []);
    if (u.includes("toolbar_agent_actions?email_to=eq.")) return resp(o.aa || []);
    if (u.includes("/ficha?domain=")) return resp({ found: false });
    if (u.includes("toolbar_review_queue?domain=eq.")) return resp([{ id: 5, monday_item_id: null, emails: [o.rebotada], email_sources: {},
      category: "", traffic: 0, pitch: PITCH, pitch_subject: "Una consulta sobre publicidad", language: "es", geo: "" }]);
    if (u.includes("toolbar_reengagement_queue?domain=eq.")) return resp(o.nueva ? [{ future_email: o.nueva }] : []);
    if (m === "POST" && b.includes('"prospects"')) return resp({ ok: 1, errores: [], avisos: [] });
    if (m === "POST" && u.includes("toolbar_agent_actions")) return resp([{ id: 77 }], { status: 201 });
    return resp([]);
  };
}

test("R1b: el reintento por el rebote de un adicional manda, pero no le cuenta al CRM la dirección nueva como si muriera el principal", async () => {
  const casos = [
    { nombre: "adicional", rebotada: `redaccion2@${DOM_R}`, nueva: `director@${DOM_R}`, aa: [{ domain: DOM_R, action: "future_sent", details: {} }], principal: false },
    { nombre: "principal", rebotada: `info@${DOM_R}`, nueva: `publicidad@${DOM_R}`, st: [{ domain: DOM_R }], principal: true },
  ];
  for (const c of casos) {
    const { queueBounceRetry } = await cargarWorker(["queueBounceRetry"], { fetchFalso: true });
    const reg = [];
    globalThis.__fetchFalso = enrutadorReintento(reg, c);
    await queueBounceRetry("t", MB, c.rebotada, "hard");
    ok(reg.some(r => r.m === "POST" && r.u.includes("/messages/send")), `${c.nombre}: el reintento sale igual: ${reg.map(r => r.u.split("?")[0].slice(-40)).join(" | ")}`);
    const accion = reg.find(r => r.m === "POST" && r.u.includes("toolbar_agent_actions") && r.b.includes("bounce_retry_sent"));
    ok(accion, `${c.nombre}: falta la acción del reintento`);
    strictEqual(JSON.parse(accion.b).details.principal, c.principal, `${c.nombre}: si esta dirección rebota después, no se la toma por la principal`);
    const alCrm = reg.filter(r => r.m === "POST" && r.b.includes('"prospects"')).flatMap(r => JSON.parse(r.b).prospects);
    const sendtrack = reg.filter(r => r.m === "POST" && r.u.includes("toolbar_sendtrack"));
    if (c.principal) {
      deepStrictEqual(alCrm.map(p => [p.domain, p.email]), [[DOM_R, c.nueva]], "murió la principal: el CRM se entera del reemplazo");
      strictEqual(sendtrack.length, 1);
      strictEqual(JSON.parse(sendtrack[0].b).email, c.nueva);
    } else {
      deepStrictEqual(alCrm, [], "el principal sigue vivo: ni `email` nuevo (lo reemplaza) ni un push vacío (marca formulario)");
      // Desde R1c (abajo) el reintento queda en sendtrack igual: es lo que leen el re-engagement y los candados
      // de 30 días. Que su próximo rebote no vacíe la ficha lo decide el último envío (`principal: false`).
      strictEqual(sendtrack.length, 1, "el mail salió: sin esta fila el re-engagement le repite el pitch a esa dirección");
      strictEqual(JSON.parse(sendtrack[0].b).email, c.nueva);
    }
  }
});

test("R1b: sin alternativa, el rebote de un adicional no avisa 'contacto agotado' (el principal está vivo)", () => {
  const fn = cuerpoDe("queueBounceRetry");
  ok(/const _origen = await _origenDeLaDireccion\(token, bouncedEmail\);/.test(fn));
  ok(/const _eraPrincipal = _origen\.principal !== false;/.test(fn), "sin saber el origen se conserva lo de antes; sólo un 'no era la principal' cambia algo");
  ok(/if \(_eraPrincipal\) \{\s*await pushToCrmPropio\(token, \{ domain, contacto_agotado: true \}/.test(fn), "contacto_agotado sólo cuando murió la principal");
  ok(/if \(_eraPrincipal\) \{\s*await pushToCrmPropio\(token, \{\s*domain,\s*email: retryEmail,/.test(fn), "el email nuevo sólo reemplaza a la principal");
  for (const m of fn.matchAll(/pushToCrmPropio\(/g)) {
    ok(/if \(_eraPrincipal\) \{\s*await $/.test(fn.slice(Math.max(0, m.index - 60), m.index)), `un push al CRM sin la condición: ${fn.slice(m.index, m.index + 80)}`);
  }
});

// ── R1c. El reintento por un adicional o un 2º email queda en sendtrack ─────────────────────────────
// R1b dejó de escribir toolbar_sendtrack cuando el reintento salía por una dirección que no era la
// principal, para que "está en sendtrack" siguiera queriendo decir "es la principal". Pero sendtrack es
// lo que leen el re-engagement (el freno de 30 días por dirección) y los candados de 30 días: la dirección
// del reintento recibía otro pitch a los 6 días. Es un camino de punta a punta sobre una base con estado:
// rebota → sale el reintento → corre el re-engagement → rebota la dirección del reintento.
// Dominio y direcciones propios: la lista de rebotados vive en lib/, que se carga una vez por proceso, y los
// tests de arriba ya quemaron direcciones de sitio.it.
const DOM_C = "giornale-r1c.it";
const PRINCIPAL_C = `info@${DOM_C}`, OTRA_C = `commerciale@${DOM_C}`;
const ENVIOS_C = ["sent", "secondary_sent", "re_sent", "bounce_retry_sent", "future_sent"];
function baseConEstado({ rebotada, accion, emails = [rebotada], emailFuturo = OTRA_C }) {
  const db = {
    sendtrack: [{ domain: DOM_C, send_date: "2026-09-14", email: PRINCIPAL_C }],
    // Por defecto sin info@ en la ficha: con él adentro, `_elegirEnviable` consulta el DNS real y el orden
    // depende de la máquina. La única alternativa es el Email Futuro del MB.
    emails,
    emailFuturo,
    actions: [
      { id: 1, domain: DOM_C, user_email: MB, action: "sent", email_to: PRINCIPAL_C, details: { email: PRINCIPAL_C }, created_at: "2026-09-14T10:00:00.000Z" },
      { id: 2, domain: DOM_C, user_email: MB, action: accion, email_to: rebotada, details: {}, created_at: "2026-09-14T10:00:01.000Z" },
    ],
    gmail: [], crm: [], congelado: false, rebote: null,
  };
  let nextId = 100;
  const router = async (url, opts = {}) => {
    const u = decodeURIComponent(String(url)), m = (opts.method || "GET").toUpperCase(), b = String(opts.body || "");
    if (u.includes("oauth2.googleapis.com")) return resp({ access_token: "falso", expires_in: 3600 });
    if (u.includes("gmail.googleapis.com") && u.includes("/messages/send")) {
      const txt = Buffer.from(String(JSON.parse(b).raw || "").replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
      db.gmail.push((txt.match(/^To:\s*(.+)$/mi) || [])[1]?.trim() || "?");
      return resp({ id: `enviado${db.gmail.length}` });
    }
    if (u.includes("gmail.googleapis.com") && u.includes("messages?q=")) return resp({ messages: db.rebote ? [{ id: db.rebote.msg }] : [] });
    if (u.includes("gmail.googleapis.com") && db.rebote && u.includes(`messages/${db.rebote.msg}?format=full`)) return resp({ payload: {
      headers: [{ name: "X-Failed-Recipients", value: db.rebote.direccion }, { name: "Content-Type", value: "multipart/report; report-type=delivery-status" }],
      body: { data: Buffer.from(`Delivery has failed to these recipients or groups:\n${db.rebote.direccion}\nRemote server returned '550 5.1.1 User unknown'`).toString("base64") },
    } });
    if (u.includes("gmail.googleapis.com")) return resp({});
    if (u.includes("toolbar_config")) return resp([
      { key: "crm_propio_enabled", value: "true" },
      { key: "monday_bloqueados", value: CONFIG_CRM[1].value },
      { key: "monday_bloqueados_at", value: new Date().toISOString() },
      { key: "agent_reengagement_enabled", value: "true" },
      { key: "agent_active_hours_start", value: "0" }, { key: "agent_active_hours_end", value: "24" },
    ]);
    if (m === "POST" && b.includes('"prospects"')) { db.crm.push(...JSON.parse(b).prospects); return resp({ ok: 1, errores: [], avisos: [] }); }
    if (u.includes("toolbar_sendtrack")) {
      if (m === "POST") { db.sendtrack.push(JSON.parse(b)); return resp(null, { status: 201 }); }
      const em = (u.match(/[?&]email=eq\.([^&]+)/) || [])[1];
      if (em) return resp(db.sendtrack.filter(r => r.email.toLowerCase() === em.toLowerCase()).map(r => ({ domain: r.domain })));
      const desde = (u.match(/send_date=gte\.([^&]+)/) || [])[1] || "";
      return resp(db.sendtrack.filter(r => u.includes(`domain=eq.${r.domain}&`) && r.send_date >= desde).map(r => ({ email: r.email, send_date: r.send_date })));
    }
    if (u.includes("toolbar_frozen_leads?domain=eq.")) return resp(db.congelado ? [{ domain: DOM_C }] : []);
    // La dirección nueva la cargó el MB (Email Futuro): así la elección no depende del DNS de la máquina
    // que corre el test (sin eso, el orden de `_elegirEnviable` consulta el proveedor de correo real).
    if (u.includes("toolbar_reengagement_queue?domain=eq.")) return resp(db.emailFuturo ? [{ future_email: db.emailFuturo }] : []);
    if (u.includes("toolbar_agent_actions")) {
      if (m === "POST") { const fila = { id: nextId++, ...JSON.parse(b) }; db.actions.push(fila); return resp([fila], { status: 201 }); }
      if (m === "PATCH") {
        const fila = db.actions.find(a => a.id === Number((u.match(/id=eq\.(\d+)/) || [])[1]));
        if (fila) Object.assign(fila, JSON.parse(b));
        return resp([]);
      }
      const em = (u.match(/email_to=eq\.([^&]+)/) || [])[1];
      if (em && !u.includes("user_email=eq.")) return resp(db.actions.filter(a => a.email_to === em && ENVIOS_C.includes(a.action)).slice(-1));
      if (u.includes("domain=eq.") && u.includes("select=email_to")) {
        return resp(db.actions.filter(a => u.includes(`domain=eq.${a.domain}&`) && ENVIOS_C.includes(a.action)).map(a => ({ email_to: a.email_to })));
      }
      if (u.includes("action=eq.sent&")) return resp(db.actions.filter(a => a.action === "sent"));
      if (u.includes("action=in.(re_sent,reengagement_exhausted)")) return resp(db.actions.filter(a => ["re_sent", "reengagement_exhausted"].includes(a.action)));
      if (u.includes("action=in.(sent,re_sent)&select=id")) return resp([], { total: db.actions.filter(a => ["sent", "re_sent"].includes(a.action)).length });
      return resp([]);
    }
    if (u.includes("toolbar_review_queue")) {
      if (m === "PATCH") { const p = JSON.parse(b); if (p.emails) db.emails = p.emails; return resp([]); }
      if (u.includes("id=eq.")) return resp([{ emails: db.emails, email_sources: {} }]);
      return resp([{ id: 5, monday_item_id: null, emails: db.emails, email_sources: {}, category: "", traffic: 0, pitch: PITCH,
        pitch_subject: "Una consulta sobre publicidad", language: "it", geo: "IT", contact_name: "" }]);
    }
    if (u.includes("/ficha?domain=")) return resp({ found: false });
    if (u.includes("toolbar_bounce_retries") && m === "GET") return resp([], { total: 0 });
    return resp([]);
  };
  return { db, router };
}

test("R1c: el reintento por un 2º email o un adicional queda en sendtrack; el re-engagement no le repite el pitch y su rebote no vacía la ficha", async (t) => {
  const casos = [
    // Una dirección nueva por caso: el paso 3 la quema, y la lista de rebotados se comparte entre cargas.
    { nombre: "2º email del agente", rebotada: `mario.rossi@${DOM_C}`, accion: "secondary_sent", otra: OTRA_C },
    { nombre: "adicional del MB", rebotada: `redaccion2@${DOM_C}`, accion: "future_sent", otra: `pubblicita@${DOM_C}` },
  ];
  for (const c of casos) {
    const w = await cargarWorker(["queueBounceRetry", "runReengagementCycle", "scanBouncesForUser", "_origenDeLaDireccion"], { fetchFalso: true });
    const { db, router } = baseConEstado({ ...c, emailFuturo: c.otra });
    globalThis.__fetchFalso = router;
    t.mock.timers.enable({ apis: ["Date"], now: Date.parse("2026-09-15T10:00:00Z") });   // martes
    try {
      // 1. Rebota la dirección que no es la principal y sale el reintento.
      await w.queueBounceRetry("t", MB, c.rebotada, "hard");
      deepStrictEqual(db.gmail, [c.otra], `${c.nombre}: el reintento sale`);
      strictEqual(db.actions.find(a => a.action === "bounce_retry_sent")?.details?.principal, false, c.nombre);
      deepStrictEqual(db.crm, [], `${c.nombre}: el principal sigue vivo, el CRM no se entera de un reemplazo`);
      ok(db.sendtrack.some(r => r.domain === DOM_C && r.email === c.otra),
        `${c.nombre}: el mail salió y tiene que quedar donde lo ven el re-engagement y los candados de 30 días: ${JSON.stringify(db.sendtrack.map(r => r.email))}`);

      // 2. Seis días después info@ no abrió y corre el re-engagement. Sin key de MillionVerifier el camino corta
      //    antes de Gmail, así que lo que prueba que no le iba a escribir es que ni siquiera reserva el envío.
      t.mock.timers.setTime(Date.parse("2026-09-21T10:00:00Z"));   // lunes
      db.gmail.length = 0;
      await w.runReengagementCycle("t");
      const aLaOtra = db.actions.filter(a => a.email_to === c.otra && a.action !== "bounce_retry_sent");
      deepStrictEqual(aLaOtra.map(a => `${a.action}:${a.reason || ""}`), [], `${c.nombre}: ${c.otra} recibió el reintento hace 6 días — el freno de 30 días tiene que verlo`);
      deepStrictEqual(db.gmail, []);

      // 3. Rebota la dirección del reintento. Está en sendtrack, pero el último envío a ella es un reintento
      //    con `principal: false`: se quema sin avisarle al CRM, que vaciaría el Email de info@.
      db.congelado = true;   // el reintento que dispara el scan corta ahí
      db.rebote = { msg: `rebote-${c.accion}`, direccion: c.otra };
      strictEqual(await w.scanBouncesForUser("t", MB), 1, c.nombre);
      await esperar(100);   // el reintento y la acción salen sin await
      deepStrictEqual(db.crm, [], `${c.nombre}: ${JSON.stringify(db.crm)}`);
      deepStrictEqual(await w._origenDeLaDireccion("t", c.otra), { sitio: DOM_C, via: "sendtrack", accion: "bounce_retry_sent", principal: false });
      // La principal está en sendtrack y su último envío es `sent`: sigue siendo la principal.
      deepStrictEqual(await w._origenDeLaDireccion("t", PRINCIPAL_C), { sitio: DOM_C, via: "sendtrack", accion: "sent", principal: true });
    } finally {
      t.mock.timers.reset();
    }
  }
});

test("R1c: con la dirección en sendtrack, el último envío decide si es la principal; `_sitioDeLaDireccion` no consulta de más", async () => {
  const { _origenDeLaDireccion, _sitioDeLaDireccion } = await cargarWorker(["_origenDeLaDireccion", "_sitioDeLaDireccion"], { fetchFalso: true });
  let reg = [];
  const origen = async (o) => { globalThis.__fetchFalso = enrutadorSitio(reg, o); return _origenDeLaDireccion("t", "x@sitio.it"); };
  const st = [{ domain: "sitio.it" }];
  for (const accion of ["future_sent", "secondary_sent"]) {
    strictEqual((await origen({ st, aa: [{ domain: "sitio.it", action: accion, details: {} }] })).principal, false,
      `${accion}: en un ciclo anterior pudo haber sido la principal, hoy le escribimos como adicional`);
  }
  strictEqual((await origen({ st, aa: [{ domain: "sitio.it", action: "bounce_retry_sent", details: { principal: false } }] })).principal, false);
  strictEqual((await origen({ st, aa: [{ domain: "sitio.it", action: "bounce_retry_sent", details: { principal: true } }] })).principal, true);
  deepStrictEqual(await origen({ st, aa: [] }), { sitio: "sitio.it", via: "sendtrack", accion: null, principal: true },
    "sin envío registrado en 90 días, sendtrack alcanza (un envío viejo del popup)");
  deepStrictEqual(await origen({ st, aa: [{ domain: "otro.it", action: "future_sent", details: {} }] }), { sitio: "sitio.it", via: "sendtrack", accion: null, principal: true },
    "un envío a la misma dirección para OTRO sitio no dice nada de la ficha de este");
  deepStrictEqual(await origen({ st, aa: "falla" }), { sitio: "sitio.it", via: "sendtrack", accion: null, principal: true },
    "si no se puede leer el último envío queda lo de siempre: sendtrack es la principal");
  reg = [];
  globalThis.__fetchFalso = enrutadorSitio(reg, { st, aa: [{ domain: "sitio.it", action: "future_sent", details: {} }] });
  strictEqual(await _sitioDeLaDireccion("t", "x@sitio.it"), "sitio.it");
  ok(!reg.some(u => u.includes("toolbar_agent_actions")), "para el sitio sólo, sendtrack alcanza");
});

// Caso G de la revisión: rebota el 2º email (de webmail) y la única alternativa de la ficha es el principal,
// que recibió el pitch ayer. Se le mandaba otra vez, y ese reintento (`principal: false`, ahora también en
// sendtrack) hacía que `_origenDeLaDireccion` leyera al principal como adicional: su rebote después ya no se
// le avisaba al CRM.
test("R1c: con el rebote de un 2º email, el reintento no le repite el pitch a quien ya le escribimos (el principal vivo)", async () => {
  const w = await cargarWorker(["queueBounceRetry", "_origenDeLaDireccion"], { fetchFalso: true });
  const rebotada = "mario.rossi.r1c@gmail.com";
  const { db, router } = baseConEstado({ rebotada, accion: "secondary_sent", emails: [PRINCIPAL_C, rebotada], emailFuturo: null });
  globalThis.__fetchFalso = router;
  await w.queueBounceRetry("t", MB, rebotada, "hard");
  await esperar(50);   // el salto se anota sin await
  deepStrictEqual(db.gmail, [], `${PRINCIPAL_C} recibió el pitch ayer y sigue vivo`);
  ok(!db.actions.some(a => a.action === "bounce_retry_sent"), "no hay reintento que anotar");
  deepStrictEqual(db.sendtrack.map(r => r.email), [PRINCIPAL_C]);
  deepStrictEqual(db.crm, []);
  const salto = db.actions.find(a => a.action === "bounce_retry_skipped");
  strictEqual(salto?.reason, "alternativas_ya_contactadas", `el salto queda dicho: ${JSON.stringify(db.actions.map(a => a.action))}`);
  strictEqual((await w._origenDeLaDireccion("t", PRINCIPAL_C)).principal, true, "y el principal sigue siendo el principal");

  // Si no se puede saber a quién le escribimos, no se reintenta: el principal está vivo, no se pierde nada.
  const w2 = await cargarWorker(["queueBounceRetry"], { fetchFalso: true });
  const b2 = baseConEstado({ rebotada, accion: "secondary_sent", emails: [PRINCIPAL_C, rebotada], emailFuturo: `direzione@${DOM_C}` });
  globalThis.__fetchFalso = async (url, opts) => (String(url).includes("toolbar_sendtrack?domain=eq.")
    ? resp({ message: "boom" }, { status: 500 }) : b2.router(url, opts));
  await w2.queueBounceRetry("t", MB, rebotada, "hard");
  await esperar(50);
  deepStrictEqual(b2.db.gmail, [], "ante la duda, no");
  strictEqual(b2.db.actions.find(a => a.action === "bounce_retry_skipped")?.reason, "ya_escritas_no_verificable");
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
