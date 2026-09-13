// ══════════════════════════════════════════════════════════════════════════════════════
// LA COLA "POR ENVIAR" Y LOS ENVÍOS A MANO DESDE LA TARJETA — las reglas, sin DOM (2026-09-13)
// ══════════════════════════════════════════════════════════════════════════════════════
// Por qué existe este archivo. La auditoría del 13/09 encontró que los botones de la cola
// (Guardar, Quitar, Enviar) y el envío desde la tarjeta de Prospects decidían cosas caras
// adentro de un handler de popup.js, donde ningún test llega:
//   · "Guardar" pisaba el status sin anotar el anterior y "Quitar" lo dejaba siempre en
//     pending: contactados, descartados y sitios que nunca pasaron el filtro volvían al pool.
//   · "Guardar" reescribía idioma y país con el vocabulario del formulario de Monday ("1",
//     "España") sobre filas que usan ISO e inglés, reemplazaba la lista de emails por uno solo
//     y pisaba el tráfico que ya había pasado el piso.
//   · Los adicionales de la tarjeta salían los cuatro juntos y sin registro.
// Acá quedan las reglas como funciones puras: el popup las llama y los tests las prueban sin
// abrir Chrome. Lo único que importa es lib/geo.js, que ya viaja en el zip de la extensión.
import { COUNTRY_CODES } from "../auto-prospector/lib/geo.js";

// ── Idioma ──────────────────────────────────────────────────────────────────────────────
// El <select id="form-idioma"> guarda el ÍNDICE de la columna de Monday para los cinco
// idiomas viejos y el código ISO para los nuevos. El pool (y el agente) usan ISO. Es el
// inverso exacto de LANG_TO_IDX de popup.js: el test lo exige, para que no se desincronicen.
// "5" es "Language?": el MB no sabe el idioma, y "no sé" no se escribe como un idioma.
export const IDIOMA_DE_INDICE_MONDAY = { "0": "en", "1": "es", "2": "it", "3": "pt", "6": "ar" };

export function idiomaIsoDelFormulario(valor) {
  const s = String(valor ?? "").trim().toLowerCase();
  if (IDIOMA_DE_INDICE_MONDAY[s]) return IDIOMA_DE_INDICE_MONDAY[s];
  return /^[a-z]{2}$/.test(s) ? s : "";
}

// ── País ────────────────────────────────────────────────────────────────────────────────
// MONDAY_COUNTRIES (el desplegable) tiene duplicados que GEO_LABEL (ISO → etiqueta) no cubre,
// porque GEO_LABEL guarda una sola etiqueta por país. Sin estos, "Holanda" o "Dubai" no
// resolverían a ningún ISO y el país se perdería.
const ETIQUETAS_MONDAY_EXTRA = {
  "Holanda": "NL", "Netherlands": "NL", "Gran Bretaña": "GB", "Emiratos Arabes": "AE", "Dubai": "AE",
  "Jordan": "JO", "Catar": "QA", "Argelia": "DZ", "Bosnia": "BA", "Corea": "KR", "Republica de Corea": "KR",
};
const _normEtiqueta = (s) => String(s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim().toLowerCase();

/** Etiqueta del desplegable de Monday → ISO de 2 letras, o "" si no la conocemos. */
export function isoDeEtiquetaMonday(etiqueta, geoLabel = {}) {
  const buscada = _normEtiqueta(etiqueta);
  if (!buscada) return "";
  for (const [iso, label] of Object.entries(geoLabel || {})) if (_normEtiqueta(label) === buscada) return iso;
  for (const [label, iso] of Object.entries(ETIQUETAS_MONDAY_EXTRA)) if (_normEtiqueta(label) === buscada) return iso;
  // Un ISO tal cual también vale (una fila que ya venía con "ES").
  const up = String(etiqueta).trim().toUpperCase();
  return /^[A-Z]{2}$/.test(up) && (COUNTRY_CODES[up] || Object.prototype.hasOwnProperty.call(geoLabel || {}, up)) ? up : "";
}

// ── Emails ──────────────────────────────────────────────────────────────────────────────
// "Guardar" escribía `emails: [lo del campo]`: se perdían las demás direcciones que el pool
// ya tenía (pagadas en Apollo, raspadas, verificadas), y un campo vacío o una URL de formulario
// dejaban la fila sin email, así que el pulido volvía a gastar en buscarlas.
// Regla: la elección del MB va PRIMERA y las demás se conservan; un campo vacío no borra nada;
// una URL de formulario no es un email y no entra a la lista.
export function fusionarEmailsCola(prev, emailForm, { loginEmail = "", esFormulario = false } = {}) {
  const prevEmails = Array.isArray(prev?.emails) ? prev.emails.filter(e => typeof e === "string" && e.trim()) : [];
  const prevSources = prev?.email_sources && typeof prev.email_sources === "object" && !Array.isArray(prev.email_sources)
    ? prev.email_sources : {};
  const valor = String(emailForm || "").trim();
  if (!valor) return { cambia: false, payloadEmail: "", contactoFormulario: "" };
  if (esFormulario) return { cambia: false, payloadEmail: "", contactoFormulario: valor };
  const e = valor.toLowerCase();
  const vistos = new Set([e]);
  const resto = [];
  for (const x of prevEmails) { const k = x.trim().toLowerCase(); if (!vistos.has(k)) { vistos.add(k); resto.push(x); } }
  const emails = [e, ...resto];
  const email_sources = { ...prevSources };
  if (!email_sources[e]) email_sources[e] = { source: "manual_mb", by: String(loginEmail || "").toLowerCase() };
  const cambia = !prevSources[e] || emails.length !== prevEmails.length || emails.some((x, i) => x !== prevEmails[i]);
  return { cambia, emails, email_sources, payloadEmail: e, contactoFormulario: "" };
}

// A quién le escribe el CRM cuando se manda el lote. La auditoría de emails del worker también
// recorre las filas `por_enviar` y reordena la lista por puntaje: mandar `emails[0]` podía
// terminar en una casilla distinta de la que eligió el MB (y a la que quizás ya le escribió).
// Manda lo que el MB eligió al guardar; las filas de antes del arreglo no tienen esa clave y
// siguen usando la primera de la lista, como hasta hoy.
export function emailDeCola(f) {
  const mp = (f && f.monday_payload) || {};
  if (mp.contacto_formulario) return String(mp.contacto_formulario);
  if (Object.prototype.hasOwnProperty.call(mp, "email")) return String(mp.email || "");
  return (Array.isArray(f?.emails) && f.emails[0]) || "";
}

// ── Estado anterior ─────────────────────────────────────────────────────────────────────
// Lo que la fila era ANTES de entrar a la cola. Si se guarda dos veces, la segunda lectura ya
// ve `por_enviar`: el estado verdadero es el que quedó anotado la primera vez.
export function statusPrevioAlGuardar(prev) {
  if (!prev) return null;
  const previo = prev.status === "por_enviar" ? (prev.monday_payload?.status_previo ?? null) : (prev.status || null);
  return previo === "por_enviar" ? null : previo;
}

// Qué decirle al MB al guardar. El cartel "YA figura contactado" saltaba en casi todos los
// guardados, porque el flujo normal es mandar el mail desde Analysis y después guardar: el
// contacto era el suyo de hace un minuto, y el MB aprendía a ignorar el único aviso que importa.
export function avisoAlGuardarEnCola(prev, { loginEmail = "", ahoraMs = Date.now() } = {}) {
  if (!prev) return "nuevo";
  const previo = statusPrevioAlGuardar(prev);
  if (previo === "rejected") return "descartado";
  if (prev.monday_item_id) return "contactado";
  if (previo === "validated") {
    const mio = !!loginEmail && String(prev.validated_by || "").toLowerCase() === String(loginEmail).toLowerCase();
    const t = Date.parse(prev.validated_at || "");
    const reciente = Number.isFinite(t) && ahoraMs - t <= 12 * 3600 * 1000;
    return mio && reciente ? "contactado_por_vos" : "contactado";
  }
  if (prev.status === "por_enviar") return "ya_en_cola";
  return "en_pool";
}

// ── La fila que escribe "Guardar" ───────────────────────────────────────────────────────
// Fila NUEVA (el sitio no estaba en la tabla): se escribe todo, traducido al vocabulario del
// pool — idioma ISO, país en inglés o ISO y `geos_all` — nunca la etiqueta del formulario.
// Fila EXISTENTE: el pool conserva lo que midió. No se tocan idioma, país ni geos_all; el
// tráfico sólo si no tenía (el que ya pasó el piso de 350K no se reescribe: si el MB lo tipeaba
// más bajo y después sacaba la fila de la cola, cleanup_pool la borraba en 15 minutos); los
// emails se fusionan; el pitch sólo si el MB escribió uno.
// Lo que el formulario eligió viaja entero en `monday_payload`, que es lo que lee el lote.
export function filaColaDesdeFormulario(v, {
  prev = null, domain = "", loginEmail = "", esFormulario = false, geoLabel = {}, contactos = [], trafficTexto = "",
  crmAlGuardar = null,
} = {}) {
  const em = fusionarEmailsCola(prev, v.email, { loginEmail, esFormulario });
  const previo = statusPrevioAlGuardar(prev);
  const monday_payload = {
    estado: v.estado, fecha: v.fecha, ejecutivo: v.ejecutivo, idioma: v.idioma,
    traffic_text: trafficTexto, mail_enviado: v.mailEnviado === true,
    // (2026-09-13) De qué sitio y qué día salió el mail, y si sendtrack lo aceptó. `mail_enviado`
    // sola salía de una bandera de sesión que nunca volvía a false: después del primer mail, todo lo
    // que el MB guardaba en esa sesión quedaba "enviado". Una fila sin estas claves (las de antes)
    // ya no se toma como prueba de envío: decide sendtrack (ver envioAnotado).
    ...(v.mailEnviado === true ? {
      mail_enviado_dominio: normDominioCola(domain),
      mail_enviado_el: /^\d{4}-\d{2}-\d{2}$/.test(String(v.mailEnviadoEl || "")) ? String(v.mailEnviadoEl) : "",
      mail_enviado_en_sendtrack: v.mailEnviadoEnSendtrack === true,
    } : {}),
    geo_form: v.geo || "",
    email: em.payloadEmail,
    ...(em.contactoFormulario ? { contacto_formulario: em.contactoFormulario } : {}),
    ...(previo ? { status_previo: previo } : {}),
    contactos: Array.isArray(contactos) ? contactos : [],
    // Lo que decía el CRM en el momento de guardar (ver fotoCrmAlGuardar y decidirLoteCrm).
    ...(crmAlGuardar ? { crm_al_guardar: crmAlGuardar } : {}),
  };
  const fila = { domain, status: "por_enviar", monday_payload };
  if (!prev) {
    const iso = isoDeEtiquetaMonday(v.geo, geoLabel);
    Object.assign(fila, {
      traffic: v.traffic,
      geo: iso ? (COUNTRY_CODES[iso] || iso) : "",
      geos_all: iso ? [iso] : null,
      language: idiomaIsoDelFormulario(v.idioma),
      emails: em.cambia ? em.emails : [],
      ...(em.cambia ? { email_sources: em.email_sources } : {}),
      pitch: v.pitch || "",
      source: "manual_cola",
      created_by: loginEmail,
    });
  } else {
    if (!(Number(prev.traffic) > 0)) fila.traffic = v.traffic;
    if (em.cambia) { fila.emails = em.emails; fila.email_sources = em.email_sources; }
    if (v.pitch) fila.pitch = v.pitch;
  }
  return fila;
}

// ── "Quitar de la cola" ─────────────────────────────────────────────────────────────────
// Cada fila vuelve a donde estaba, no a pending fijo. En este orden:
//   1. Si el mail salió (anotado al guardar CON SU SITIO —envioAnotado—, o el dominio figura en
//      sendtrack de los últimos 30 días) → validated. Un contactado nunca vuelve a Prospects como nuevo.
//   2. Estaba validated → validated.   3. Estaba rejected → rejected (el motivo original sigue
//      en suspect_reason: "Guardar" nunca lo borró).
//   4. Sin estado anterior y creada por la cola (`manual_cola`): nunca pasó el filtro de entrada
//      (ads.txt, tipo de negocio, GEO). Soltarla en pending la ponía en Prospects y en manos del
//      agente sin filtro. Queda rejected con motivo `mb:`.
//   5. Otro estado conocido → se restaura tal cual.
//   6. Pending (o una fila de antes del arreglo) → pending; si su tráfico está por debajo del
//      piso, cleanup_pool la va a borrar, y el MB lo tiene que saber ANTES de confirmar.
// `contactado_sendtrack: null` = no se pudo saber (sendtrack no contestó o su respuesta no es
// creíble, ver lecturaDeEnvios). Lo que no depende de eso (1-4) sale igual; lo que volvería al
// pool (5-6) se queda en la cola, con status null y grupo "sin_confirmar" (2026-09-13).
export function estadoAlSacarDeCola({ status_previo = null, source = "", mail_enviado = false,
  contactado_sendtrack = false, traffic = null, minTraffic = 350000 } = {}) {
  const previo = status_previo === "por_enviar" ? null : status_previo;
  if (mail_enviado === true || contactado_sendtrack === true) {
    return { status: "validated", grupo: "contactado", sello: previo !== "validated" };
  }
  if (previo === "validated") return { status: "validated", grupo: "contactado", sello: false };
  if (previo === "rejected") return { status: "rejected", grupo: "descartado" };
  if (!previo && source === "manual_cola") {
    return { status: "rejected", grupo: "sin_filtro", suspect_reject: true, suspect_reason: "mb: sacada_de_cola_sin_filtro" };
  }
  // Devolverla al pool sin saber si ya se le escribió es justo lo que "Quitar" hacía mal: un
  // contactado de otro MB volvía a Prospects como nuevo. "No sé" no se trata como "no".
  if (contactado_sendtrack === null) return { status: null, grupo: "sin_confirmar" };
  if (previo && previo !== "pending") return { status: previo, grupo: "restaurado" };
  const t = Number(traffic);
  if (t > 0 && t < minTraffic) return { status: "pending", grupo: "bajo_piso" };
  return { status: "pending", grupo: "prospects" };
}

// Agrupa las filas marcadas en un PATCH por cuerpo distinto (como mucho cinco).
// ⚠️ `rejected_at` NO se escribe: el parte cuenta como "purgadas hoy" toda fila con
// rejected_at de hoy (index.js, sección 4 de parteDelDia), y una fila que el MB saca de su cola
// no es una purga del pool. `status_previo` tampoco se limpia: en cuanto la fila sale de
// `por_enviar`, statusPrevioAlGuardar lo ignora, y limpiarlo obligaría a un PATCH por fila.
// `contactados: null` = sendtrack no se pudo leer (o no es creíble): las filas que volverían al
// pool quedan en `sinConfirmar` y no entran a ningún PATCH.
export function planSacarDeCola(filas, { contactados = new Set(), minTraffic = 350000, loginEmail = "", ahoraIso = new Date().toISOString() } = {}) {
  const lotes = new Map();
  const grupoPorId = new Map();
  const sinConfirmar = [];
  for (const f of filas || []) {
    if (!f || f.id == null) continue;
    const mp = f.monday_payload || {};
    // (2026-09-13) La bandera sola no cierra una fila como contactada: tiene que nombrar a este sitio.
    const d = estadoAlSacarDeCola({
      status_previo: mp.status_previo ?? null, source: f.source || "", mail_enviado: envioAnotado(f),
      contactado_sendtrack: contactados ? contactados.has(String(f.domain || "").toLowerCase()) : null, traffic: f.traffic, minTraffic,
    });
    if (d.status == null) { sinConfirmar.push(String(f.id)); grupoPorId.set(String(f.id), d.grupo); continue; }
    const body = { status: d.status };
    if (d.sello) Object.assign(body, { validated_by: loginEmail, validated_at: ahoraIso });
    if (d.suspect_reject) Object.assign(body, { suspect_reject: true, suspect_reason: d.suspect_reason });
    const clave = JSON.stringify(body);
    if (!lotes.has(clave)) lotes.set(clave, { body, ids: [] });
    lotes.get(clave).ids.push(String(f.id));
    grupoPorId.set(String(f.id), d.grupo);
  }
  return { lotes: [...lotes.values()], grupoPorId, sinConfirmar };
}

export function contarGrupos(ids, grupoPorId) {
  const c = { prospects: 0, bajo_piso: 0, contactado: 0, descartado: 0, sin_filtro: 0, restaurado: 0 };
  for (const id of ids || []) { const g = grupoPorId.get(String(id)); if (g in c) c[g]++; }
  return c;
}

const _partes = (c, minTraffic) => [
  c.prospects && `${c.prospects} vuelven a Prospects`,
  c.contactado && `${c.contactado} ya estaban contactados y quedan cerrados`,
  c.descartado && `${c.descartado} vuelven a descartados`,
  c.sin_filtro && `${c.sin_filtro} se descartan (se guardaron sin pasar el filtro de entrada)`,
  c.restaurado && `${c.restaurado} vuelven a su estado anterior`,
  c.bajo_piso && `${c.bajo_piso} tienen menos de ${Math.round(minTraffic / 1000)}K y se eliminan (Prospects no admite menos de ${Math.round(minTraffic / 1000)}K)`,
].filter(Boolean);

// El confirm decía "Quedan en Prospects como pendientes, no se borran", y no era verdad ni
// para los contactados ni para los de menos de 350K.
export function textoConfirmarSacar(plan, { minTraffic = 350000, motivoSinConfirmar = "" } = {}) {
  const ids = plan.lotes.flatMap(l => l.ids);
  const c = contarGrupos(ids, plan.grupoPorId);
  const quedan = (plan.sinConfirmar || []).length;
  const nota = quedan ? ` ${quedan} se quedan en la cola: no pude confirmar si ya se les escribió${motivoSinConfirmar ? ` (${motivoSinConfirmar})` : ""}.` : "";
  return `¿Sacar ${ids.length} de la cola? Cada uno vuelve a su estado anterior: ${_partes(c, minTraffic).join("; ")}.${nota}`;
}

export function textoResultadoSacar(hechos, total, grupoPorId, { minTraffic = 350000, fallas = [] } = {}) {
  const c = contarGrupos(hechos, grupoPorId);
  const partes = _partes(c, minTraffic);
  let txt = `Se sacaron ${hechos.length} de ${total}${partes.length ? ` (${partes.join("; ")})` : ""}.`;
  if (fallas.length) txt += ` ⚠️ ${fallas.join(" · ")}`;
  return txt;
}

// ── Los adicionales de la tarjeta de Prospects ──────────────────────────────────────────
// Salían los cuatro en el mismo minuto desde el popup (principal + 3), sin fila en
// agent_actions ni en sendtrack: el lector de rebotes no los reconocía como nuestros y el
// rebote de Exchange se descartaba entero. Ahora van a la misma cola que usa Analysis
// (toolbar_reengagement_queue, reason 'adicional_manual'): los despacha el worker de a uno por
// minuto, escribe `future_sent`, la fila de response_tracking y le avisa al CRM.
// `body` es el cuerpo con firma y SIN el píxel del principal: un píxel copiado a otro buzón
// contaría como apertura del principal.
export function adicionalesDeLaTarjeta({ domain = "", mbEmail = "", principal = "", candidatos = [], rebotados = new Set(),
  subject = "", body = "", ahoraMs = Date.now() } = {}) {
  const ppal = String(principal || "").trim().toLowerCase();
  const vistos = new Set([ppal]);
  const filas = [], avisos = [];
  for (const c of candidatos || []) {
    const fe = String(c || "").trim().toLowerCase();
    if (!fe || !fe.includes("@")) continue;
    if (fe === ppal) { avisos.push(`⏭️ ${fe} igual al principal`); continue; }
    if (vistos.has(fe)) { avisos.push(`⏭️ ${fe} repetido`); continue; }
    vistos.add(fe);
    if (rebotados && rebotados.has(fe)) { avisos.push(`🚫 ${fe} bounced`); continue; }
    const orden = filas.length + 1;
    filas.push({
      domain, mb_email: String(mbEmail || "").toLowerCase(), original_email: ppal, future_email: fe,
      original_subject: subject, original_body: body, original_sent_at: new Date(ahoraMs).toISOString(),
      scheduled_for: new Date(ahoraMs + orden * 60_000).toISOString(),
      status: "pending", reason: "adicional_manual", sequence: orden, tracking_action_id: null,
    });
  }
  return { filas, avisos };
}

// Los contactos que viajan al CRM con la ficha. Si la cola de envío falló, van sin hora: no se
// afirma que recibieron un mail que no salió.
export function contactosDeAdicionales(filas, { programados = true } = {}) {
  return (filas || []).map((f, i) => ({
    email: f.future_email, tipo: "adicional", orden: i + 1, ...(programados ? { enviado_at: f.scheduled_for } : {}),
  }));
}

// ── EL LOTE Y LA FICHA QUE CREA NUESTRO PROPIO ENVÍO (2026-09-13) ─────────────────────────
// El lote vuelve a preguntarle al CRM por cada sitio antes de cargarlo, y hace bien: lo guardado
// puede tener días y el sitio pudo volverse cliente. Pero salteaba TODO lo que no fuera
// prospectable, y en el flujo normal la ficha la crea el propio sistema: el MB manda el mail con
// adicionales y guarda; a los pocos minutos el worker despacha los adicionales y le avisa al CRM
// (`adicional_enviado`, sólo dominio + contactos). En un dominio que el CRM no tenía, ese aviso
// crea la ficha con el estado por defecto de la tabla —"Propuesta Vigente"— y sin ejecutivo
// (adeq-dashboard: sql-crm-board-2026-08.sql y api/crm/sync-toolbar). Días después el lote la
// leía como "Propuesta en curso" y la fila quedaba salteada para siempre, sin llegar nunca al
// CRM con los datos del MB.
// Para distinguir esa ficha de una propuesta ajena hace falta saber qué decía el CRM AL GUARDAR.

/** Foto del veredicto del CRM en el momento de guardar. null si todavía no había veredicto. */
export function fotoCrmAlGuardar(veredicto, dup) {
  if (!veredicto) return null;
  return {
    ok: veredicto.ok === true, duda: veredicto.duda === true, found: dup?.found === true,
    estado: String(dup?.status || ""), ejecutivo: String(dup?.ejecutivo || "").trim().toLowerCase(),
  };
}

// "Propuesta Vigente" es el único estado "en curso" que aparece solo, como default de la tabla.
// En Negociación, Personalizado y Live los pone una persona. Mismo vocabulario que
// _CRM_EN_CURSO_RE de popup.js (el test lo compara con el _veredictoCrm real).
const _PROPUESTA_VIGENTE_RE = /propuesta\s*vigente/i;

/**
 * ¿El lote carga esta fila en el CRM? → { enviar, motivo, fichaPropia? }
 *   dup        la ficha de hoy (buscarEnCrm). "El CRM no contesta" lo corta quien llama.
 *   veredicto  _veredictoCrm(dup): la misma regla que el recuadro de Analysis.
 *   alGuardar  monday_payload.crm_al_guardar (null en las filas guardadas antes del 13/09).
 *   contactado el mail ya salió: anotado al guardar con su sitio o visto en sendtrack (contactadoDeCola;
 *              la bandera de sesión sola no cuenta, 2026-09-13).
 * Se saltea: dominio bloqueado; Live / En Negociación / Personalizado; estado que no se reconoce;
 * el CRM ya decía que no al guardar; una Propuesta Vigente con ejecutivo, o que ya existía al
 * guardar, o sin un envío nuestro que la explique. Se manda: lo prospectable, y la Propuesta
 * Vigente sin ejecutivo que apareció después de nuestro envío.
 */
export function decidirLoteCrm({ dup = null, veredicto = null, alGuardar = null, contactado = false } = {}) {
  const v = veredicto || { ok: false, duda: true, titulo: "No pude consultar el CRM", detalle: "" };
  if (v.ok === true) return { enviar: true, motivo: "" };
  const motivo = `${v.titulo || "Web NO prospectable"}${v.detalle ? `: ${v.detalle}` : ""}`;
  if (dup?.bloqueado || v.duda === true || !dup?.found) return { enviar: false, motivo };
  if (alGuardar && alGuardar.ok === false && alGuardar.duda !== true) {
    return { enviar: false, motivo: `${motivo} (el CRM ya decía que no al guardar)` };
  }
  if (!_PROPUESTA_VIGENTE_RE.test(String(dup.status || ""))) return { enviar: false, motivo };
  const ejecutivo = String(dup.ejecutivo || "").trim();
  if (ejecutivo) return { enviar: false, motivo: `${motivo} (a nombre de ${ejecutivo})` };
  if (alGuardar?.found === true) return { enviar: false, motivo: `${motivo} (la ficha ya existía al guardar)` };
  if (contactado !== true) return { enviar: false, motivo: `${motivo} (y no hay un envío nuestro que la explique)` };
  return { enviar: true, motivo: "", fichaPropia: true };
}

// ── ¿SE PUEDE CREER LO QUE DIJO SENDTRACK? (2026-09-13) ──────────────────────────────────
// "Quitar" y "Enviar" leen toolbar_sendtrack con el token del MB. Si la lectura falla, o si una
// política de RLS le esconde al MB los envíos de otros (sql/rls_hardening.sql define una por
// dueño; desde el repo no se puede saber si está aplicada en la base), "no hay envío" no es un
// dato: con eso "Quitar" devolvía a Prospects un sitio contactado.
// Sospechosa: una fila anotó al guardar que el mail salió A ESTE SITIO, sendtrack lo aceptó en ese
// momento, la fecha del envío cae dentro de la ventana, y ahora sendtrack no lo muestra. Si no ve
// ése, no se puede creer que no haya otros.
// (2026-09-13) Antes alcanzaba con `mail_enviado` y la fecha del formulario. Esa bandera salía de la
// sesión, no del sitio: una sola fila guardada después de mandarle a otro apagaba el lote entero
// ("puede que no vea los de otros MB", falso). Y si el insert en sendtrack había fallado al mandar,
// su ausencia no dice nada de la lectura.
export function lecturaDeEnvios(env, filas, { dias = 30, ahoraMs = Date.now() } = {}) {
  if (!env || env.ok !== true) {
    return { conocida: false, dominios: new Set(), motivo: `no pude leer los envíos (${env?.error || "sin respuesta"})` };
  }
  const dominios = env.dominios instanceof Set ? env.dominios : new Set();
  const corte = new Date(ahoraMs - dias * 86_400_000).toISOString().slice(0, 10);
  const faltan = [];
  for (const f of filas || []) {
    const mp = f?.monday_payload || {};
    const el = String(mp.mail_enviado_el || "");
    const d = String(f?.domain || "").toLowerCase();
    if (envioAnotado(f) && mp.mail_enviado_en_sendtrack === true && /^\d{4}-\d{2}-\d{2}$/.test(el) && el >= corte && !dominios.has(d)) faltan.push(d);
  }
  if (faltan.length) {
    return { conocida: false, dominios,
             motivo: `la base no muestra envíos que sí salieron (${faltan.slice(0, 3).join(", ")}): puede que no vea los de otros MB` };
  }
  return { conocida: true, dominios, motivo: "" };
}

/** ¿Ya se le escribió? true / false / null (no se puede saber). Un envío anotado con su sitio no depende de sendtrack. */
export function contactadoDeCola(f, lectura) {
  if (envioAnotado(f)) return true;
  if (!lectura || lectura.conocida !== true) return null;
  return lectura.dominios.has(String(f?.domain || "").toLowerCase());
}

// ── "MAIL ENVIADO" ES DE UN SITIO, NO DE LA SESIÓN (2026-09-13) ─────────────────────────────
// "Guardar" anotaba `mail_enviado: !!state.emailSentInSession`, y esa bandera sólo se prende: el
// panel lateral cambia de dominio sin recargar (scheduleRecheck) y resetAnalysisUI no la baja.
// Mandarle a A y después guardar B, C y D sin escribirles dejaba a los tres "enviados". Con eso
// "Quitar" los cerraba como contactados (salían del pool para siempre), el lote le decía al CRM
// que no mandara el inicial y tomaba por nuestra una "Propuesta Vigente" ajena.
// Ahora el envío se anota por sitio en el momento en que sale (anotarEnvioDeSesion), "Guardar" lo
// lee para el sitio que guarda (envioDeSesion), y la fila lo escribe con el sitio (filaColaDesdeFormulario).
// La bandera de sesión queda sólo para el Guard #3 del botón verde, que no cambia acá.

/** Mismo dominio escrito de dos formas ("WWW.Sitio.com" y "sitio.com") es el mismo sitio. */
export function normDominioCola(d) {
  return String(d || "").trim().toLowerCase().replace(/^www\./, "");
}

/** Anota que a `dominio` le salió un mail en esta sesión. Si sendtrack lo aceptó alguna vez, queda aceptado. */
export function anotarEnvioDeSesion(envios, dominio, { el = "", enSendtrack = false } = {}) {
  const d = normDominioCola(dominio);
  if (!(envios instanceof Map) || !d) return;
  envios.set(d, { el: String(el || ""), enSendtrack: enSendtrack === true || envios.get(d)?.enSendtrack === true });
}

/** El envío de esta sesión a `dominio`, o null si a ese sitio no se le escribió. */
export function envioDeSesion(envios, dominio) {
  const d = normDominioCola(dominio);
  return (envios instanceof Map && d && envios.get(d)) || null;
}

/**
 * ¿Lo anotado al guardar prueba que a este sitio se le escribió? Sólo si nombra al sitio de la fila.
 * Las filas guardadas antes del arreglo tienen `mail_enviado` sin sitio, que puede ser la bandera
 * vieja: para ellas decide sendtrack, y si sendtrack no se pudo leer, "no se sabe" (null en
 * contactadoDeCola), nunca "sí". La cola existe desde el 02/09 y sendtrack se lee con 30 días.
 */
export function envioAnotado(f) {
  const mp = f?.monday_payload || {};
  const d = normDominioCola(f?.domain);
  return mp.mail_enviado === true && !!d && normDominioCola(mp.mail_enviado_dominio) === d;
}
