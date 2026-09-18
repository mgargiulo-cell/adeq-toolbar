// El reintento por rebote manda con las MISMAS reglas que el primer contacto. (2026-09-13, auditoría)
//
// "Lo que arreglás un día no lo rompas al otro." El reintento —el mail que sale a otra dirección
// cuando la primera rebota o contesta un fuera de oficina— tenía reglas propias, más flojas que
// las del agente, y justo en el dominio que acababa de rebotar:
//   C36. Mandaba a la primera del orden sin MillionVerifier: un rol_mx sin verificar, el gmail del
//        registrante o un `dudoso` que el agente había salteado.
//   C37. No le preguntaba al CRM si el lead estaba en Prospects (el caso normal): un pitch en frío
//        a quien había contestado esa misma mañana.
//   C38. Toda plantilla salía en castellano: `language` no estaba en el select.
//   C39. Un fuera de oficina disparaba dos reintentos (el mensaje nunca quedaba visto), borraba de
//        Prospects la dirección que contestó y podía congelar el dominio 60 días.
//   C40. La ficha se reescribía con la lista vieja: se perdían los rescatados y lo de Apollo
//        quedaba anotado como "scrape".
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateKeyPairSync } from "node:crypto";
import { cargarWorker } from "./_worker-exportado.mjs";

// lib/config.js se evalúa UNA vez por proceso: el secreto del CRM y la cuenta de servicio de Gmail
// tienen que estar puestos antes de la primera carga del worker.
process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";
if (!process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
  const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON = JSON.stringify({
    client_email: "reintento-test@falso.iam.gserviceaccount.com",
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
const resp = (body, { status = 200, total } = {}) => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: (k) => (String(k).toLowerCase() === "content-range" && total != null ? `0-0/${total}` : null) },
  json: async () => body, text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});

const MB = "sales@adeqmedia.com";
const DOM = "medio-ejemplo.es";
// Un snapshot sano de la lista de no-recontactar, para que el chequeo de bloqueo no alerte.
const SNAPSHOT_BLOQUEADOS = JSON.stringify(Array.from({ length: 120 }, (_, i) => `bloqueado${i}.com`));

/** Contesta la base, el CRM y Gmail con datos inventados y anota cada pedido. */
function enrutador(reg, o = {}) {
  return async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase(), b = String(opts.body || "");
    reg.push({ u, m, b });
    if (u.includes("toolbar_config?select=key,value")) return resp([
      { key: "monday_bloqueados", value: SNAPSHOT_BLOQUEADOS }, { key: "monday_bloqueados_at", value: new Date().toISOString() },
    ]);
    if (u.includes("oauth2.googleapis.com")) return resp({ access_token: "falso", expires_in: 3600 });
    if (u.includes("gmail.googleapis.com") && u.includes("format=metadata")) {
      const id = (u.match(/messages\/([^?]+)\?/) || [])[1];
      return resp({ payload: { headers: [{ name: "From", value: o.from?.[id] || `Juan <juan@${DOM}>` }, { name: "Auto-Submitted", value: "auto-replied" }] } });
    }
    if (u.includes("gmail.googleapis.com") && u.includes("messages?q=")) return resp({ messages: (o.ids || []).map(id => ({ id })) });
    if (u.includes("gmail.googleapis.com")) return resp({ id: "enviado" });
    if (u.includes("toolbar_bounce_seen?msg_id=in.")) {
      return o.vistosFalla ? resp({ message: "boom" }, { status: 500 }) : resp([...(o.vistos || [])].map(msg_id => ({ msg_id })));
    }
    if (u.includes("toolbar_bounce_seen") && m === "POST") { o.vistos?.add(JSON.parse(b).msg_id); return resp(null, { status: 201 }); }
    if (u.includes("toolbar_sendtrack?email=eq.")) return resp(o.sitio === "" ? [] : [{ domain: o.sitio || DOM }]);
    if (u.includes("toolbar_agent_actions?user_email=") && u.includes("email_to=eq.")) return resp(o.leEscribimos ? [{ id: 1 }] : []);
    if (u.includes("toolbar_agent_actions?user_email=") && u.includes("action=eq.bounce_retry_sent")) return resp(o.reintentosAus || []);
    if (u.includes("toolbar_frozen_leads?domain=eq.")) return resp(o.congelado ? [{ domain: DOM }] : []);
    if (u.includes("toolbar_bounce_retries?domain=eq.")) return resp([], { total: o.intentos || 0 });
    if (u.includes("/ficha?domain=")) return resp(o.ficha ?? { found: false });
    return resp([]);
  };
}

// ── C36. A quién se le manda ────────────────────────────────────────────────────────────
test("C36: la decisión por candidato es la del agente, y un dudoso se saltea sin quemarlo", async () => {
  const { _decidirCandidato } = await cargarWorker(["_decidirCandidato"]);
  const d = (email, source, hechos = {}) => _decidirCandidato({ email, source }, hechos);
  strictEqual(d("mario.rossi@gmail.com", "informer"), "saltear:informer_webmail", "el gmail del registrante (WHOIS) nunca");
  strictEqual(d("mario.rossi@gmail.com", "scrape"), "elegir", "un gmail publicado en el sitio sí puede ser real (regla del 14/07)");
  strictEqual(d("juan@sitio.it", "scrape", { rebotado: true }), "saltear:ya_reboto");
  strictEqual(d("juan@sitio.it", "scrape", { noEscribir: "ya rechazó 2 direcciones" }), "saltear:dominio_ya_rechazo");
  strictEqual(d("info@otra.com", "scrape", { marcaOk: false }), "saltear:otra_marca");
  strictEqual(d("m.rossi@sitio.it", "pattern", { ruta: { verificar: false, enviar: false } }), "saltear:catch_all_y_patron", "patrón en M365: MV no puede resolverlo");
  // El dueño decidió el 18/09 ("soluciona todos los problemas", con rol_mx rebotando 23 de 46): una dirección
  // adivinada sólo sale con un "ok" de MillionVerifier. Hasta ese día acá se esperaba "elegir".
  strictEqual(d("contacto@sitio.it", "rol_mx", { ruta: { verificar: false, enviar: true }, mv: "riesgo" }), "saltear:hipotesis_sin_ok", "rol_mx sin un ok no sale (decisión del dueño, 18/09)");
  strictEqual(d("contacto@sitio.it", "rol_mx", { ruta: { verificar: true, enviar: true }, mv: "ok" }), "elegir", "rol_mx con ok sale");
  strictEqual(_decidirCandidato({ email: "contacto@sitio.it", source: "rol_mx" }, { ruta: { verificar: true, enviar: true }, mv: "sin_verificar" }, { manualManda: false, sinVerificar: "reserva" }), "saltear:hipotesis_sin_ok", "con la política del agente (sin_verificar = reserva) tampoco: un no-sé no alcanza para una adivinanza");
  strictEqual(d("contacto@sitio.it", "rol_mx"), "elegir", "sin veredicto todavía sigue de largo: la verifica _elegirDireccion");
  strictEqual(d("publicidad@sitio.it", "rol_mx", { ruta: { verificar: true, enviar: true }, mv: "dudoso" }), "saltear:mv_dudoso");
  strictEqual(d("publicidad@sitio.it", "scrape", { ruta: { verificar: true, enviar: true }, mv: "no" }), "saltear:mv_no");
  strictEqual(d("publicidad@sitio.it", "scrape", { ruta: { verificar: true, enviar: true }, mv: "riesgo" }), "reserva");
  strictEqual(d("publicidad@sitio.it", "scrape", { ruta: { verificar: true, enviar: true }, mv: "ok" }), "elegir");
  strictEqual(d("publicidad@sitio.it", "scrape", { ruta: { verificar: true, enviar: true }, mv: true }), "elegir", "sin key de MV la verificación devuelve true");
  strictEqual(d("nuevo@sitio.it", "manual", { mv: "dudoso", marcaOk: false }), "elegir", "la dirección que cargó el MB es su elección");
  strictEqual(d("nuevo@sitio.it", "manual", { mv: "no" }), "saltear:mv_no", "salvo que no exista");
});

test("C36: el escenario del auditor — rol_mx dudoso, gmail de informer y un scrape verificado → sale el scrape", async () => {
  const { _elegirEnviable } = await cargarWorker(["_elegirEnviable"]);
  const consultas = [], quemadas = [];
  const mvs = { "publicidad@sitio.it": "dudoso", "redaccion2@sitio.it": "ok" };
  const r = await _elegirEnviable("t", {}, "sitio.it", [
    { email: "publicidad@sitio.it", source: "rol_mx", score: 60 },
    { email: "mario.rossi@gmail.com", source: "informer", score: 40 },
    { email: "redaccion2@sitio.it", source: "scrape", score: 30 },
  ], {
    rutaMV: async () => ({ verificar: true, enviar: true }),
    verificarMV: async (_t, _c, e) => { consultas.push(e); return mvs[e]; },
    quemar: async (_t, x) => { quemadas.push(x); },
  });
  strictEqual(r.chosen?.email, "redaccion2@sitio.it");
  deepStrictEqual(r.motivos, ["mv_dudoso", "informer_webmail"]);
  deepStrictEqual(consultas, ["publicidad@sitio.it", "redaccion2@sitio.it"], "el gmail del registrante ni se consulta");
  deepStrictEqual(quemadas, [], "un dudoso no se quema: sigue disponible si después no aparece nada mejor");
});

test("C36: un 'no' se quema y se prueba la siguiente; el tope de MV corta; la reserva sale sólo si no hay uno limpio", async () => {
  const { _elegirEnviable } = await cargarWorker(["_elegirEnviable"]);
  const ruta = async () => ({ verificar: true, enviar: true });
  let quemadas = [];
  const quemar = async (_t, x) => { quemadas.push(x); };
  let r = await _elegirEnviable("t", {}, "sitio.it", [{ email: "a@sitio.it", source: "scrape" }, { email: "b@sitio.it", source: "scrape" }],
    { rutaMV: ruta, verificarMV: async (_t, _c, e) => (e === "a@sitio.it" ? "no" : true), quemar });
  strictEqual(r.chosen?.email, "b@sitio.it");
  strictEqual(quemadas.length, 1);
  strictEqual(quemadas[0].email, "a@sitio.it");
  strictEqual(quemadas[0].evidencia, "verificador");

  let n = 0;
  r = await _elegirEnviable("t", {}, "sitio.it", ["a", "b", "c", "d"].map(x => ({ email: `${x}@sitio.it`, source: "scrape" })),
    { rutaMV: ruta, verificarMV: async () => { n++; return "riesgo"; }, quemar });
  strictEqual(n, 3, "a lo sumo 3 consultas pagas por reintento");
  strictEqual(r.chosen?.email, "a@sitio.it", "ninguno limpio: sale la mejor reserva");
  ok(r.deReserva && r.motivos.includes("tope_mv"), JSON.stringify(r));

  n = 0;
  r = await _elegirEnviable("t", {}, "sitio.it", [{ email: "m.rossi@sitio.it", source: "pattern" }],
    { rutaMV: async () => ({ verificar: false, enviar: false }), verificarMV: async () => { n++; return true; }, quemar });
  strictEqual(r.chosen, null);
  strictEqual(n, 0, "patrón en un proveedor que acepta todo: ni se paga la consulta");

  r = await _elegirEnviable("t", {}, "sitio.it", [{ email: "nuevo@sitio.it", source: "manual" }],
    { rutaMV: ruta, verificarMV: async () => "dudoso", quemar });
  strictEqual(r.chosen?.email, "nuevo@sitio.it", "la dirección del MB sale aunque MV dude");
});

test("C36: el veto de informer+webmail es la misma expresión en el reintento y en el agente", async () => {
  // (2026-09-13, cierre de eleccion_paridad) Este test comparaba dos copias de la regex (la del reintento,
  // _WEBMAIL_DE_REGISTRANTE, y el literal del agente). Ya no hay copias: los dos llaman a
  // esRegistranteWebmail de lib/email.js, la misma de la extensión. Se exige eso y que no vuelvan.
  ok(!/const _WEBMAIL_DE_REGISTRANTE\s*=/.test(worker), "volvió la copia local de la regex del registrante");
  ok(!/x\.source === "informer" && \/@\(gmail/.test(worker), "volvió el literal del registrante en el agente");
  ok(/esRegistranteWebmail\(email, fuente\)/.test(cuerpoDe("_decidirCandidato")), "el reintento no usa la regla compartida");
  ok(/\.filter\(x => !esRegistranteWebmail\(x\.email, x\.source\)\)/.test(cuerpoDe("runAgentCycle")), "el agente perdió el filtro o no usa la regla compartida");
  const { _decidirCandidato } = await cargarWorker(["_decidirCandidato"]);
  strictEqual(_decidirCandidato({ email: "mario.rossi@gmail.com", source: "informer" }, {}), "saltear:informer_webmail");
  strictEqual(_decidirCandidato({ email: "ivan@mail.ru", source: { source: "informer" } }, {}), "saltear:informer_webmail", "la fuente guardada como objeto también");
});

test("C36: el reintento elige con _elegirEnviable y, sin candidato, no congela ni avisa 'contacto agotado'", () => {
  const fn = cuerpoDe("queueBounceRetry");
  ok(/_elegirEnviable\(token, cfg, domain, ranked, \{ maxMv: 3 \}\)/.test(fn), "falta la elección con las reglas del agente");
  ok(!/ranked\[0\]\.email/.test(fn), "`ranked[0]` sin pasar por las reglas es exactamente el bug");
  const sinCandidato = fn.slice(fn.indexOf("if (!_eleccion.chosen)"), fn.indexOf("const retryEmail"));
  ok(/if \(!_esAusencia\)/.test(sinCandidato) && /status: "skipped_no_alt"/.test(sinCandidato), "un rebote real deja la fila con un status que la tabla acepta");
  ok(!/toolbar_frozen_leads|contacto_agotado/.test(sinCandidato), "un dudoso no es 'contacto agotado'");
});

// ── C37. La ficha fresca del CRM ────────────────────────────────────────────────────────
test("C37: la puerta del CRM es la del agente", async () => {
  const { _motivoNoMandarPorCrm } = await cargarWorker(["_motivoNoMandarPorCrm"]);
  strictEqual(_motivoNoMandarPorCrm(null), null, "no está en el CRM: libre");
  strictEqual(_motivoNoMandarPorCrm({ estado: "Propuesta", board: "Prospectos ADEQ" }), null);
  strictEqual(_motivoNoMandarPorCrm({ indeterminado: true }), "crm:no_verificable", "ante la duda, no");
  strictEqual(_motivoNoMandarPorCrm({ enNegociacion: true, board: "En Negociacion" }), "crm:en_negociacion");
  strictEqual(_motivoNoMandarPorCrm({ descansando: true, diasParaReintentar: 20 }), "crm:descansando");
});

test("C37: con el lead en Prospects y la ficha En Negociacion, el reintento no busca el lead, no rescata y no manda", async () => {
  const w = await cargarWorker(["queueBounceRetry"], { fetchFalso: true });
  const reg = [];
  globalThis.__fetchFalso = enrutador(reg, { ficha: { found: true, board: "En Negociacion", estado: "Propuesta" } });
  await w.queueBounceRetry("t", MB, `info@${DOM}`, "hard");
  ok(reg.some(r => r.u.includes(`/ficha?domain=${DOM}`)), "tiene que preguntarle al CRM");
  ok(!reg.some(r => r.u.includes("select=id,monday_item_id,emails")), "no puede llegar a buscar el lead: de ahí sigue el rescate pago y el envío");
  ok(!reg.some(r => r.u.includes("gmail.googleapis.com")), "no sale ningún mail");
  const salto = reg.find(r => r.m === "POST" && r.u.includes("toolbar_agent_actions") && r.b.includes("bounce_retry_skipped"));
  ok(salto, "el salto tiene que quedar registrado");
  strictEqual(JSON.parse(salto.b).reason, "crm:en_negociacion");
  ok(!reg.some(r => r.m === "POST" && r.u.includes("toolbar_bounce_retries")), "un salto por el CRM no cuenta para el tope de intentos");
});

// `_fichaFallos` se borró (ronda final de la entrada, 2026-09-13): nadie lo leía desde que runCsvQueue
// cuenta sus propios dominios, y un contador que sólo se suma invita a volver a leerlo mal. Lo que este
// test cuidaba queda en pie por construcción: la alerta de la importación cuenta sólo lo que pasó en su
// tanda, y la ficha del CRM no tiene un contador compartido que un reintento pueda ensuciar.
test("C37: consultar el CRM desde el reintento no suma a la alerta de la importación", async () => {
  const w = await cargarWorker(["_fichaDelCrm"], { fetchFalso: true });
  globalThis.__fetchFalso = async () => resp({ message: "caído" }, { status: 503 });
  deepStrictEqual(await w._fichaDelCrm("a.com", { contarFallo: false }), { indeterminado: true });
  deepStrictEqual(await w._fichaDelCrm("a.com"), { indeterminado: true });
  ok(!/\+\+/.test(cuerpoDe("_fichaDelCrm")), "la ficha del CRM no puede sumar a un contador compartido");
  ok(!/let _fichaFallos\b|_fichaFallos\+\+/.test(worker), "el contador de la importación no puede volver");
  ok(cuerpoDe("runCsvQueue").includes('clave: "ficha-crm-no-responde"'), "la alerta de la importación vive en runCsvQueue");
});

test("C37: una sola consulta al CRM, para todo lead y antes de cualquier gasto o envío", () => {
  const fn = cuerpoDe("queueBounceRetry");
  strictEqual((fn.match(/_fichaDelCrm\(/g) || []).length, 1, "dos consultas con dos veredictos terminan discrepando");
  const i = fn.indexOf("_fichaDelCrm(domain, { contarFallo: false })");
  ok(i > 0, "falta la consulta con contarFallo: false");
  ok(i < fn.indexOf("select=id,monday_item_id,emails"), "tiene que ir ANTES de buscar el lead, no sólo cuando no está en Prospects");
  for (const gasto of ["scrapeEmailsForDomain(", "findBestApolloEmail(", "_serperContactSearch(", "sendGmailServer(", "pushToCrmPropio("]) {
    ok(i < fn.indexOf(gasto), `la ficha tiene que consultarse antes de ${gasto}`);
  }
  ok(/_motivoNoMandarPorCrm\(ficha\)/.test(fn));
  ok(!fn.includes("skipped_crm"), "ese status viola el CHECK de toolbar_bounce_retries y además contaría para el tope");
});

// ── C38. El idioma ──────────────────────────────────────────────────────────────────────
test("C38: la normalización del idioma nunca deja salir uno sin plantilla, y re-detecta cuando el país no respalda", async () => {
  const { _hayQueMirarLaPagina, _idiomaEnviable } = await cargarWorker(["_hayQueMirarLaPagina", "_idiomaEnviable"]);
  strictEqual(_idiomaEnviable("es-AR"), "es");
  strictEqual(_idiomaEnviable("PT"), "pt");
  strictEqual(_idiomaEnviable("de"), "en", "fuera de los 5: inglés (decisión del 13/07), nunca 'no reintentar'");
  strictEqual(_idiomaEnviable(""), "en");
  ok(_hayQueMirarLaPagina("", "it"), "sin idioma guardado se mira la página");
  ok(!_hayQueMirarLaPagina("it", "it"), "guardado y respaldado por TLD/GEO: no hace falta bajar nada");
  ok(!_hayQueMirarLaPagina("es-AR", "es"));
  ok(_hayQueMirarLaPagina("pt", "en"), "el caso de Rumania: 'pt' guardado sin ninguna señal que lo respalde");
  ok(_hayQueMirarLaPagina("en", "es"), "el TLD dice otra cosa");
  ok(!_hayQueMirarLaPagina("en", "en"));
  ok(_hayQueMirarLaPagina("de", "en"), "idioma sin plantilla");
});

test("C38: _idiomaParaEnvio — italiano respaldado no baja nada; 'pt' en un .ro sale en inglés; sin idioma ni página, el TLD", async () => {
  const { _idiomaParaEnvio } = await cargarWorker(["_idiomaParaEnvio"], { fetchFalso: true });
  const reg = [];
  globalThis.__fetchFalso = async (url) => { reg.push(String(url)); return resp({ message: "sin red" }, { status: 500 }); };
  strictEqual(await _idiomaParaEnvio({ lead: { id: null, language: "it", geo: "" }, domain: "corriere.it", token: "t" }), "it");
  strictEqual(reg.length, 0, `no hacía falta ninguna consulta: ${reg.join(" | ")}`);
  strictEqual(await _idiomaParaEnvio({ lead: { id: null, language: "pt", geo: "" }, domain: "pieseauto.ro", token: "t" }), "en");
  ok(reg.some(u => u.startsWith("https://pieseauto.ro")), "el idioma guardado no estaba respaldado: se mira la página");
  strictEqual(await _idiomaParaEnvio({ lead: { id: null, language: "", geo: "" }, domain: "folha.com.br", token: "t" }), "pt");
});

test("C38: el reintento trae el idioma, usa la regla y los borradores del agente, y no saluda a quien rebotó", () => {
  const fn = cuerpoDe("queueBounceRetry");
  const codigo = fn.split("\n").filter(l => !l.trim().startsWith("//")).join("\n");   // el comentario que explica el bug lo cita
  ok(/select=id,monday_item_id,emails,email_sources,category,traffic,pitch,pitch_subject,pitch_subjects,language,geo,geos_all/.test(fn), "sin `language` en el select todo sale en castellano");
  ok(!/pickRandomTemplate\(lead\.language \|\| "es"\)/.test(codigo), "el castellano por defecto no puede volver");
  ok(/_idiomaEnvio = await _idiomaParaEnvio\(\{ lead, domain, token \}\)/.test(fn));
  ok(/pickAnyTemplate\(token, mbEmail, _idiomaEnvio\)/.test(fn), "el mismo pool que el agente: los borradores que el MB ve en Análisis");
  ok(/contactName: "",/.test(fn), "el contact_name guardado suele ser el de la persona que rebotó");
  ok(/template_id: _templateId, language: _idiomaEnvio/.test(fn), "en details, no en la columna que lee el ranking de plantillas");
});

test("C38: la regla de idioma del reintento sigue siendo la del agente", () => {
  const agente = cuerpoDe("runAgentCycle");
  const usaLaFuncion = /_idiomaParaEnvio\(/.test(agente);
  const mismaRegla =
    /_hintDet\.lang !== "en"\s*&& _hintDet\.lang !== leadLanguage\s*&& SUPPORTED_AGENT_LANGS\.has\(_hintDet\.lang\)/.test(agente) &&
    /leadLanguage\s*&& leadLanguage !== "en"\s*&& SUPPORTED_AGENT_LANGS\.has\(leadLanguage\)\s*&& _hintDet\.lang !== leadLanguage/.test(agente) &&
    /!leadLanguage\s*\|\| !SUPPORTED_AGENT_LANGS\.has\(leadLanguage\)\s*\|\| _hintDisagrees\s*\|\| _storedNotBackedByGeo/.test(agente);
  ok(usaLaFuncion || mismaRegla, "cambió la regla de idioma del agente: _hayQueMirarLaPagina tiene que cambiar igual (o el agente usar _idiomaParaEnvio)");
});

// ── C39. Fuera de oficina ───────────────────────────────────────────────────────────────
test("C39: el scan de ausencias procesa cada mensaje una vez, sólo si le escribimos, y si no puede saberlo no procesa", async () => {
  const w = await cargarWorker(["scanAutoRepliesForUser"], { fetchFalso: true });
  const vistos = new Set();
  let reg = [];
  globalThis.__fetchFalso = enrutador(reg, { ids: ["m1"], vistos, congelado: true });
  strictEqual(await w.scanAutoRepliesForUser("t", MB), 1);
  ok(reg.some(r => r.u.includes("toolbar_frozen_leads?domain=eq.")), "la primera vez llega al reintento");
  ok(vistos.has("ar:m1"), "queda vista, con prefijo propio");
  ok(!vistos.has("m1"), "nunca con el id pelado: es la clave del scan de rebotes, que corre en paralelo");

  reg = [];
  globalThis.__fetchFalso = enrutador(reg, { ids: ["m1"], vistos, congelado: true });
  strictEqual(await w.scanAutoRepliesForUser("t", MB), 0);
  ok(!reg.some(r => r.u.includes("messages/m1?format=metadata")), "al día siguiente (newer_than:1d) el mismo mensaje no se vuelve a abrir");
  ok(!reg.some(r => r.u.includes("toolbar_frozen_leads")), "ni dispara un segundo reintento");

  reg = [];
  globalThis.__fetchFalso = enrutador(reg, { ids: ["m2"], vistos, sitio: "", leEscribimos: false, from: { m2: "Hans <urlaub@firma.de>" } });
  strictEqual(await w.scanAutoRepliesForUser("t", MB), 0);
  ok(!reg.some(r => r.u.includes("toolbar_frozen_leads")), "un 'urlaub' de alguien a quien nunca le escribimos no dispara nada");
  ok(vistos.has("ar:m2"), "y queda visto para no volver a preguntarlo");

  reg = [];
  globalThis.__fetchFalso = enrutador(reg, { ids: ["m3"], vistosFalla: true });
  strictEqual(await w.scanAutoRepliesForUser("t", MB), 0);
  ok(!reg.some(r => r.u.includes("format=metadata")), "sin poder leer qué ya se procesó, no se procesa nada");
});

test("C39: una ausencia no repite reintento en 7 días, no cuenta para el tope y nunca congela; un rebote real sí", async () => {
  const w = await cargarWorker(["queueBounceRetry"], { fetchFalso: true });
  let reg = [];
  globalThis.__fetchFalso = enrutador(reg, { reintentosAus: [{ id: 9 }] });
  await w.queueBounceRetry("t", MB, `director@${DOM}`, "auto_reply");
  ok(reg.some(r => r.u.includes("action=eq.bounce_retry_sent")), "tiene que mirar si ya hubo un reintento de este MB");
  ok(!reg.some(r => r.u.includes("toolbar_bounce_retries?domain=eq.") || r.u.includes("/ficha?")), "con uno en 7 días corta ahí");

  reg = [];
  globalThis.__fetchFalso = enrutador(reg, { intentos: 2 });
  await w.queueBounceRetry("t", MB, `director@${DOM}`, "auto_reply");
  const tope = reg.find(r => r.u.includes("toolbar_bounce_retries?domain=eq."));
  ok(tope && tope.u.includes("bounce_type.neq.auto_reply") && tope.u.includes("bounce_type.is.null"), `las ausencias no cuentan para el tope: ${tope?.u}`);
  ok(!reg.some(r => r.m === "POST" && r.u.includes("toolbar_frozen_leads")), "un fuera de oficina no congela 60 días");

  reg = [];
  globalThis.__fetchFalso = enrutador(reg, { intentos: 2 });
  await w.queueBounceRetry("t", MB, `info@${DOM}`, "hard");
  ok(reg.some(r => r.m === "POST" && r.u.includes("toolbar_frozen_leads")), "regresión: un rebote real con 2 reintentos sigue congelando");
});

test("C39: sin alternativa, una ausencia no deja fila, no avisa 'contacto agotado' ni congela; y la dirección viva queda en la ficha", async () => {
  const fn = cuerpoDe("queueBounceRetry");
  ok(/\} else if \(_esAusencia\) \{[\s\S]{0,400}?_registrarSalto\("sin_alternativa"\);\s*return;\s*\} else \{/.test(fn),
     "la rama de ausencia tiene que salir ANTES de la fila skipped_no_alt, el contacto_agotado y el congelado de 30 días");
  const { _fichaTrasReintento } = await cargarWorker(["_fichaTrasReintento"]);
  const f = _fichaTrasReintento({
    emails: ["director@m.es", "publicidad@m.es"], sources: { "director@m.es": "apollo", "publicidad@m.es": "scrape" },
    bouncedEmail: "director@m.es", retryEmail: "publicidad@m.es", retrySource: "scrape", conservarRebotado: true,
  });
  deepStrictEqual(f.emails, ["publicidad@m.es", "director@m.es"], "el decisor que estaba de vacaciones sigue en la tarjeta");
  strictEqual(f.email_sources["director@m.es"], "apollo");
});

// ── C40. La ficha después del reintento ─────────────────────────────────────────────────
test("C40: el rescate suma cada dirección con su vía y la ficha final conserva los rescatados que no se usaron", async () => {
  const { _fichaTrasReintento, _fusionarRescate } = await cargarWorker(["_fichaTrasReintento", "_fusionarRescate"]);
  const fusion = _fusionarRescate({
    emails: ["ventas@sitio.com.br"], sources: { "ventas@sitio.com.br": "scrape" },
    vias: new Map([["contato@sitio.com.br", "scrape"], ["joao.silva@sitio.com.br", "apollo"], ["VENTAS@sitio.com.br", "apollo"]]),
  });
  deepStrictEqual(fusion.emails, ["ventas@sitio.com.br", "contato@sitio.com.br", "joao.silva@sitio.com.br"], "sin duplicar por mayúsculas");
  deepStrictEqual(fusion.email_sources, { "ventas@sitio.com.br": "scrape", "contato@sitio.com.br": "scrape", "joao.silva@sitio.com.br": "apollo" },
    "la vía real, nunca 'rescue', y sin pisar la que ya estaba");
  const f = _fichaTrasReintento({ emails: fusion.emails, sources: fusion.email_sources, bouncedEmail: "ventas@sitio.com.br", retryEmail: "joao.silva@sitio.com.br", retrySource: "apollo" });
  deepStrictEqual(f.emails, ["joao.silva@sitio.com.br", "contato@sitio.com.br"], "contato@ desaparecía de Prospects");
  // La rebotada (ventas@) conserva su vía como registro desde el 13/09 (reintento_crm-13-09d, R2): sale de
  // `emails`, no de `email_sources`.
  deepStrictEqual(f.email_sources, { "ventas@sitio.com.br": "scrape", "contato@sitio.com.br": "scrape", "joao.silva@sitio.com.br": "apollo" }, "el pago a Apollo figuraba como scrape");
  strictEqual(_fichaTrasReintento({ emails: ["a@x.com"], sources: {}, bouncedEmail: "a@x.com", retryEmail: "b@x.com", retrySource: "google_contact" }).email_sources["b@x.com"], "google_contact");
  deepStrictEqual(_fichaTrasReintento({ emails: ["B@x.com", "c@x.com", "A@X.com"], sources: {}, bouncedEmail: "a@x.com", retryEmail: "b@x.com", retrySource: "scrape" }).emails,
    ["b@x.com", "c@x.com"], "ni duplica la nueva ni deja la rebotada por diferencia de mayúsculas");
});

test("C40: el rescate anota la vía real, no escribe a id=null, y la ficha final se arma sobre la fila releída", () => {
  const fn = cuerpoDe("queueBounceRetry");
  // `(?:, [^}]*)?`: desde el 13/09 la llamada suma googleOut (tests/entrada_sueltos-13-09d.test.js); la regla sigue siendo separar informer y redes.
  ok(/scrapeEmailsForDomain\(domain, \{ informerOut: _informer, socialOut: _redes(?:, [^}]*)? \}\)/.test(fn), "sin separar informer y redes, un WHOIS pasa a 'persona'");
  ok(/_anotar\(apolloRes\.email, "apollo"\)/.test(fn) && /_anotar\(e, "google_contact"\)/.test(fn));
  ok(!/= "rescue"/.test(fn), "ninguna dirección rescatada se etiqueta 'rescue'");
  ok(/if \(lead\.id\) \{\s*const _patchRescate/.test(fn), "el lead armado desde el CRM no tiene fila");
  ok(/lead\.email_sources = _fusion\.email_sources;/.test(fn), "el orden y la ficha final tienen que ver la vía real en memoria");
  ok(/toolbar_review_queue\?id=eq\.\$\{lead\.id\}&select=emails,email_sources/.test(fn), "relee la fila: otro job pudo cambiarla mientras se mandaba");
  ok(/_fichaTrasReintento\(\{[\s\S]{0,200}conservarRebotado: _esAusencia/.test(fn));
});
