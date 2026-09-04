// ═══════════════════════════════════════════════════════════════════════════════════════
// ADEQ TOOLBAR — el puente con el CRM Board
// ═══════════════════════════════════════════════════════════════════════════════════════
// Se llamaba `monday.js` hasta el 2026-09-02. Ya no queda una sola línea que hable con
// Monday: todo el código muerto (mondayRequest, checkDuplicate, pushToMonday, updateMonday,
// MONDAY_STATES, RECYCLABLE_STATES…) se borró — 344 líneas quedaron en 150.
//
// Los NOMBRES de las funciones se conservan a propósito (getMondayBoardIndex,
// fetchMondayForRefresh…): las consume el popup desde hace meses y renombrarlas sería un
// diff enorme sin ganancia. Lo que cambió es a dónde preguntan, no cómo se llaman.
//
// ⚠️ `MONDAY_STATES` listaba etiquetas que el CRM ya no acepta. Se borró porque volver a
// usarla reproduce el bug que costó que todo push manual entrara como "Rebotado".
// La única fuente de verdad de los estados es el <select> de popup.html, que a su vez tiene
// que coincidir con ESTADOS en crm-board-schema.ts del dashboard.

import { CONFIG } from "../config.js";

// ── El CRM, que reemplaza a Monday ──────────────────────────────────────────────────────
// El formulario y los filtros manejan el idioma como ÍNDICE (herencia de Monday); el CRM
// guarda la etiqueta. Un índice vacío significa "todos" y no debe filtrar nada.
const _IDIOMA_LABEL = { 0: "Ingles", 1: "Español", 2: "Italiano", 3: "Portugues", 6: "Arabe" };
const _idiomaLabel = (v) => (v === "" || v == null ? "" : (_IDIOMA_LABEL[Number(v)] || ""));

// La base del CRM, en un solo lugar y EXPORTADA: popup.js armaba su propia URL con otro
// replace, así que había dos formas de llegar al mismo servidor y sólo una se mantenía.
// El replace va anclado al final (`$`): sin ancla matchea en cualquier parte de la URL.
export const crmUrl = (ruta = "") => `${(CONFIG.CRM_BOARD_URL || "").replace(/\/sync-toolbar\/?$/, "")}${ruta}`;
const CRM_BASE = () => crmUrl();
async function crmGet(ruta, params = {}) {
  const qs = new URLSearchParams(Object.entries(params).filter(([, v]) => v !== "" && v != null));
  const r = await fetch(`${CRM_BASE()}${ruta}${qs.toString() ? `?${qs}` : ""}`,
    { headers: { "x-toolbar-secret": CONFIG.CRM_BOARD_SECRET } });
  if (!r.ok) throw new Error(`CRM ${ruta} → HTTP ${r.status}`);
  return r.json();
}

// ── LAS PLANTILLAS DEL MAIL INICIAL, LAS MISMAS QUE USA EL CRM (2026-09-04, pedido del user) ──
// "En la toolbar tienen que aparecer, en el espacio de los emails a enviar, los mismos mails
// que están en el CRM como mail inicial." El CRM tiene 3 variantes por idioma en 23 idiomas
// (`crm_board_templates`, step_key='inicial'). Se leen por HTTP con el mismo secreto que la
// ficha: la extensión no tiene —ni debe tener— credenciales de la base del CRM.
//
// Contrato, ya vivo y verificado contra producción el 04/09 (69 plantillas, 23 idiomas,
// 3 variantes cada uno, y 401 sin el secreto):
//   GET /api/crm/plantillas?step=inicial   header x-toolbar-secret
//   → { ok, plantillas: [{ id, idioma, variant, nombre, subject, body, ejecutivo, activa }] }
// `idioma` viene con la etiqueta del CRM ("Aleman", no "de"); la traduce `IDIOMA_CRM` del popup.
//
// El fallback al borrador propio se mantiene aunque el endpoint ande: si algún día devuelve
// 500 o se cae el deploy, el media buyer sigue teniendo con qué escribir y ve POR QUÉ no está
// la plantilla del CRM. Quedarse mudo mostrando otra cosa sería peor que no mostrar nada.
//
// Caché de 6 horas en chrome.storage: las plantillas cambian poco y el popup se abre cientos
// de veces por día. `force` la saltea (el botón de recargar).
const _PLANTILLAS_CACHE_KEY = "crm_plantillas_inicial_v1";
const _PLANTILLAS_TTL_MS    = 6 * 60 * 60 * 1000;
export async function getPlantillasIniciales({ force = false } = {}) {
  try {
    if (!force && chrome?.storage?.local) {
      const { [_PLANTILLAS_CACHE_KEY]: c } = await chrome.storage.local.get(_PLANTILLAS_CACHE_KEY);
      if (c && Array.isArray(c.plantillas) && Date.now() - (c.ts || 0) < _PLANTILLAS_TTL_MS) {
        return { ok: true, plantillas: c.plantillas, cache: true };
      }
    }
  } catch {}
  try {
    const j = await crmGet("/plantillas", { step: "inicial" });
    const plantillas = (j.plantillas || [])
      .filter(t => t && t.body && t.activa !== false)
      .map(t => ({
        id: String(t.id || ""), idioma: String(t.idioma || ""), variant: Number(t.variant) || 1,
        nombre: String(t.nombre || ""), subject: String(t.subject || ""), body: String(t.body || ""),
        ejecutivo: String(t.ejecutivo || ""),
      }));
    try { await chrome.storage.local.set({ [_PLANTILLAS_CACHE_KEY]: { ts: Date.now(), plantillas } }); } catch {}
    return { ok: true, plantillas };
  } catch (e) {
    // ⚠️ No se devuelve una lista vacía como si fuera "no hay plantillas": el popup tiene que
    // saber que NO PUDO preguntar para avisarlo y caer al borrador propio.
    return { ok: false, plantillas: [], motivo: e.message };
  }
}

// ── Board index para filtrado en cascada ──────────────────────
// Devuelve Map<domainClean, { ejecutivo, fecha }>
export async function getMondayBoardIndex() {
  // Sólo se usa para responder "¿este dominio está bloqueado?" en la cascada de similares.
  // Eso lo contesta /api/crm/dominios-activos, que además sabe lo que Monday no sabía: los
  // clientes que facturan, lo que está en un tablero de negociación y los cerrados dentro
  // de sus 60 días de descanso.
  // Se mantiene la forma Map(dominio → {estado}) para no tocar al consumidor.
  try {
    const j = await crmGet("/dominios-activos");
    const index = new Map();
    for (const d of (j.domains || [])) index.set(cleanDomain(d), { ejecutivo: "", fecha: "", estado: "bloqueado" });
    return index;
  } catch (e) {
    // ⚠️ NO se devuelve un Map vacío: eso significa "no hay nadie bloqueado" y la cascada
    // mostraría clientes activos y dominios en negociación como si estuvieran libres, sin
    // un solo aviso. Se RELANZA para que el consumidor —que ya tiene un catch que pinta el
    // cartel en pantalla— se entere. Tragarme el error acá anulaba ese cartel: la promesa
    // resolvía "bien" con un Map vacío y el catch del popup no corría nunca.
    console.warn("[getMondayBoardIndex] ADEQ no contestó:", e.message);
    throw e;
  }
}

function cleanDomain(str) {
  return (str || "").toLowerCase()
    .replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "").trim();
}

// Parsea strings de tráfico de Monday a número entero.
// Acepta sufijos K/M/B (mayúsc o minúsc), decimales con `.` o `,` (estilo europeo),
// separadores de miles, espacios y texto extra (ej: "500 vistas", "1,5 M", "2.5m").
export function parseTrafficText(str) {
  if (str == null) return 0;
  let s = String(str).trim();
  if (!s) return 0;

  // Detectar sufijo K/M/B en cualquier posición, ignorando case
  const upper = s.toUpperCase();
  let mult = 1;
  if (/[MB]/.test(upper))      mult = upper.includes("B") ? 1_000_000_000 : 1_000_000;
  else if (/K/.test(upper))    mult = 1_000;

  if (mult > 1) {
    // Con sufijo: extraer el primer número (decimal con . o ,)
    const m = upper.match(/(\d+(?:[.,]\d+)?)/);
    if (!m) return 0;
    const n = parseFloat(m[1].replace(",", "."));
    return isNaN(n) ? 0 : Math.round(n * mult);
  }

  // Sin sufijo: tratar . y , como separadores de miles → quedarse solo con dígitos
  const digits = s.replace(/[^\d]/g, "");
  return digits ? parseInt(digits, 10) : 0;
}

// ── fetchMondayForRefresh — para MONDAY CSV AUTO PROSPECTOR ─────
// Trae dominios ya en Monday con estado "Ciclo Finalizado" filtrado por geo/idioma,
// para re-prospectarlos en batch sin necesidad de CSV
export async function fetchMondayForRefresh({ geo = "", idioma = "", limit = 75 } = {}) {
  // Los ciclos cerrados salen del CRM. `idioma` llegaba como ÍNDICE de Monday; el CRM guarda
  // la etiqueta, así que se traduce acá y no en el llamador.
  const j = await crmGet("/reciclables", { limit, geo, idioma: _idiomaLabel(idioma), full: 1 });
  return (j.items || []).map(it => it.domain);
}

// ── fetchImportCandidates — para el tab Import ────────────────
// Trae ítems del board con estado "Ciclo Finalizado" filtrados por geo/idioma
export async function fetchImportCandidates({ geo = "", idioma = "", minTraffic = 0, maxTraffic = 0 } = {}) {
  // El botón Import: mismos filtros que en Monday, contra el CRM.
  const j = await crmGet("/reciclables", {
    limit: 500, geo, idioma: _idiomaLabel(idioma), minTraffic, maxTraffic, full: 1,
  });
  return (j.items || []).map(it => ({
    domain: it.domain,
    // `url` la usa el botón "Import N URLs" para abrir cada sitio en una pestaña. Al migrar
    // me la comí y el botón abría N pestañas EN BLANCO (chrome.tabs.create con undefined).
    url: `https://www.${it.domain}`,
    traffic: it.pageviews || "",
    trafficNum: it.pageviews_num || 0,
    geo: it.top_geo || "",
    idioma: it.idioma || "",
    estado: it.estado || "",
    ejecutivo: it.ejecutivo || "",
    email: it.email || "",
  }));
}

// ── LO QUE SE MANDÓ A MANO, SEGÚN EL BOARD (Maxi 2026-08-26) ────────────────────────────
// El panel de Activity mostraba "Emails manual: 0" para los tres MB mientras Monday tenía 18
// de Agustina en un solo día. Contaba desde `toolbar_api_usage`, que se llena desde el popup
// y falla en silencio; el push a Monday, en cambio, es el trabajo principal y siempre ocurre.
//
// Es el MISMO cambio que se hizo en el parte diario del worker, y por la misma razón: la
// fuente de verdad de lo manual es el board, no un contador interno que nadie mira cuando
// falla. Si estos dos números vuelven a divergir, el equivocado es el interno.
//
// Devuelve { ok, items: [{owner, dominio, email, geo, fecha}] }. `ok:false` significa que
// Monday no contestó — quien llame TIENE que decirlo en pantalla en vez de mostrar cero.
export async function fetchManualSendsFromMonday({ desde, hasta } = {}) {
  // ⚠️ Devuelve { ok, items }, NO un array pelado. Se lo cambié a array y el panel de
  // Activity se rompió entero: `monday.ok` daba undefined, así que mostraba el cartel
  // "no se pudo leer" SIEMPRE y los tres MB aparecían con "👤 ? a mano" aunque el CRM
  // respondiera perfecto. El `ok` es lo que separa "no mandó nadie" de "no pude preguntar",
  // y sin él el panel acusa de caído a un sistema que anda.
  try {
    const j = await crmGet("/manuales", { desde, hasta });
    return {
      ok: true,
      items: (j.manuales || []).map(m => ({
        dominio: m.dominio, owner: m.owner, email: m.email, geo: m.geo, fecha: m.fecha,
      })),
    };
  } catch (e) {
    console.warn("[fetchManualSendsFromMonday] ADEQ no contestó:", e.message);
    return { ok: false, items: [], motivo: e.message };
  }
}
