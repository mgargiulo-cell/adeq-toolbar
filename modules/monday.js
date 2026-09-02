// ============================================================
// ADEQ TOOLBAR — Módulo Monday.com v3
// ============================================================

import { CONFIG } from "../config.js";

const MONDAY_API = "https://api.monday.com/v2";

// Maxi 2026-07-01 (B7): columna de índice (estado/idioma) SOLO si el valor es un entero
// válido. Antes parseInt(x) sobre un string no-numérico daba NaN → {index:null} → Monday
// podía blanquear/rechazar la columna. Ahora si no es entero, se omite la columna entera.
function _mondayIndexCol(val) {
  if (val === undefined || val === null || val === "") return null;
  const n = parseInt(val, 10);
  return Number.isInteger(n) ? { index: n } : null;
}

// ⚠️ MONDAY ESTÁ APAGADO (Maxi 2026-09-02, corte pedido por el user).
// Todo el tráfico de la extensión hacia Monday pasaba por acá, así que éste es el lugar
// donde se corta: una sola puerta en vez de quince parches. Si alguna función quedó sin
// migrar, revienta acá con un mensaje que dice QUÉ falta — que es justo lo que se quiere.
// Fallar ruidoso es lo correcto: si esto devolviera vacío en silencio, una pantalla se
// vería "sin resultados" y nadie sabría que en realidad no se consultó nada.
const MONDAY_APAGADO = true;

async function mondayRequest(query, { timeoutMs = 15000 } = {}) {
  if (MONDAY_APAGADO) {
    throw new Error("Monday está apagado: esta pantalla todavía no se migró al CRM. Avisá para migrarla.");
  }
  if (!CONFIG.MONDAY_API_KEY) {
    throw new Error("Monday API key no cargada (sesión expirada o sin permisos)");
  }
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const response = await fetch(MONDAY_API, {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "Authorization": CONFIG.MONDAY_API_KEY,
        "API-Version":   "2024-01",
      },
      body: JSON.stringify({ query }),
      signal: ctrl.signal,
    });
    if (!response.ok) throw new Error(`Monday API error: ${response.status}`);
    const json = await response.json();
    if (json.errors) throw new Error(json.errors[0]?.message || "Error en Monday API");
    return json.data;
  } catch (e) {
    if (e.name === "AbortError") throw new Error(`Monday timeout (${timeoutMs}ms)`);
    throw e;
  } finally {
    clearTimeout(timer);
  }
}


// ── El CRM, que reemplaza a Monday ──────────────────────────────────────────────────────
// El formulario y los filtros manejan el idioma como ÍNDICE (herencia de Monday); el CRM
// guarda la etiqueta. Un índice vacío significa "todos" y no debe filtrar nada.
const _IDIOMA_LABEL = { 0: "Ingles", 1: "Español", 2: "Italiano", 3: "Portugues", 6: "Arabe" };
const _idiomaLabel = (v) => (v === "" || v == null ? "" : (_IDIOMA_LABEL[Number(v)] || ""));

const CRM_BASE = () => (CONFIG.CRM_BOARD_URL || "").replace("/sync-toolbar", "");
async function crmGet(ruta, params = {}) {
  const qs = new URLSearchParams(Object.entries(params).filter(([, v]) => v !== "" && v != null));
  const r = await fetch(`${CRM_BASE()}${ruta}${qs.toString() ? `?${qs}` : ""}`,
    { headers: { "x-toolbar-secret": CONFIG.CRM_BOARD_SECRET } });
  if (!r.ok) throw new Error(`CRM ${ruta} → HTTP ${r.status}`);
  return r.json();
}

export async function checkDuplicate(domain) {
  const domainClean = cleanDomain(domain).replace(/"/g, "").replace(/\\/g, "");
  const query = `{
    boards(ids: [${CONFIG.MONDAY_ACTIVE_BOARD}]) {
      items_page(limit: 500, query_params: {
        rules: [{ column_id: "name", compare_value: ["${domainClean}"], operator: contains_text }]
      }) {
        items {
          id name
          column_values(ids: [
            "${CONFIG.MONDAY_COLUMNS.estado}",
            "${CONFIG.MONDAY_COLUMNS.ejecutivo}",
            "${CONFIG.MONDAY_COLUMNS.trafico}",
            "${CONFIG.MONDAY_COLUMNS.email}",
            "${CONFIG.MONDAY_COLUMNS.geo}",
            "${CONFIG.MONDAY_COLUMNS.fecha_contacto}",
            "${CONFIG.MONDAY_COLUMNS.idioma}"
          ]) { id text }
        }
      }
    }
  }`;

  const data  = await mondayRequest(query);
  const items = data?.boards?.[0]?.items_page?.items || [];
  const match = items.find(item => cleanDomain(item.name) === domainClean);
  if (!match) return { found: false };

  const col = (id) => match.column_values.find(c => c.id === id)?.text || "";
  return {
    found:     true,
    itemId:    match.id,
    status:    col(CONFIG.MONDAY_COLUMNS.estado),
    ejecutivo: col(CONFIG.MONDAY_COLUMNS.ejecutivo),
    trafico:   col(CONFIG.MONDAY_COLUMNS.trafico),
    email:     col(CONFIG.MONDAY_COLUMNS.email),
    geo:       col(CONFIG.MONDAY_COLUMNS.geo),
    fecha:     col(CONFIG.MONDAY_COLUMNS.fecha_contacto),
    idioma:    col(CONFIG.MONDAY_COLUMNS.idioma),
  };
}

// Resuelve el user ID de Monday a partir del login email o el nombre corto
function resolveMondayPerson(ejecutivo, loginEmail) {
  if (loginEmail) {
    const id = CONFIG.MONDAY_USER_IDS?.[loginEmail.toLowerCase()];
    if (id) return id;
  }
  const shortToEmail = {
    "Max":   "mgargiulo@adeqmedia.com",
    "Agus":  "sales@adeqmedia.com",
    "Diego": "dhorovitz@adeqmedia.com",
  };
  if (ejecutivo && shortToEmail[ejecutivo]) {
    const id = CONFIG.MONDAY_USER_IDS?.[shortToEmail[ejecutivo]];
    if (id) return id;
  }
  return null;
}

export async function pushToMonday(data) {
  const { domain, traffic, email, geo, estado, fecha, idioma, ejecutivo, loginEmail } = data;

  const safe     = (str) => (str || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\r/g, "\\r").replace(/\n/g, "\\n").replace(/\t/g, "\\t");
  // Normaliza: quita protocolo/trailing slash, fuerza www. prefix
  const cleaned  = String(domain || "").trim().toLowerCase().replace(/^https?:\/\//, "").replace(/\/+$/, "");
  const itemName = cleaned.startsWith("www.") ? cleaned : `www.${cleaned}`;

  const personId = resolveMondayPerson(ejecutivo, loginEmail);

  // Comentarios = ORIGEN del lead (Maxi 2026-07-21, pedido del user): push MANUAL del MB → "Manual".
  // El push del AGENTE (auto-prospector: pushToMondayServer) escribe "Agente". Desde hoy TODO item
  // lleva identificación en Comentarios. NO es el pitch (eso el user no lo quiere ahí).
  const columnValues = {
    // Maxi 2026-08-10: la marca de origen vivía SOLO en Comentarios, que es texto libre — el
    // primer MB que anota algo ahí la borra. De ahí el "a veces figura y a veces no". Si hay una
    // columna dedicada configurada, va ahí; Comentarios se sigue completando por compatibilidad.
    ...(CONFIG.MONDAY_COLUMNS.origen ? { [CONFIG.MONDAY_COLUMNS.origen]: "Manual" } : {}),
    [CONFIG.MONDAY_COLUMNS.comentarios]:   "Manual",
    [CONFIG.MONDAY_COLUMNS.trafico]:       safe(String(traffic || "")),
    [CONFIG.MONDAY_COLUMNS.geo]:           safe(geo || ""),
    ...(email                                                             ? { [CONFIG.MONDAY_COLUMNS.email]:         { email: email, text: email } } : {}),
    ...(personId                                                          ? { [CONFIG.MONDAY_COLUMNS.ejecutivo]:     { personsAndTeams: [{ id: personId, kind: "person" }] } } : {}),
    ...(_mondayIndexCol(estado) ? { [CONFIG.MONDAY_COLUMNS.estado]: _mondayIndexCol(estado) } : {}),
    ...(fecha                               ? { [CONFIG.MONDAY_COLUMNS.fecha_contacto]: { date: fecha } }             : {}),
    ...(_mondayIndexCol(idioma) ? { [CONFIG.MONDAY_COLUMNS.idioma]: _mondayIndexCol(idioma) } : {}),
  };

  const mutation = `mutation {
    create_item(
      board_id: ${CONFIG.MONDAY_ACTIVE_BOARD},
      item_name: "${safe(itemName)}",
      column_values: "${safe(JSON.stringify(columnValues))}"
    ) { id name }
  }`;

  const result = await mondayRequest(mutation);
  return result?.create_item;
}

export async function updateMonday({ itemId, traffic, email, geo, estado, fecha, idioma, ejecutivo, loginEmail }) {
  const safe = (str) => (str || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");

  const personId = resolveMondayPerson(ejecutivo, loginEmail);

  // Comentarios: NO se incluye pitch — decisión user 2026-05-12.
  // Maxi 2026-08-10: la marca de origen se escribía SOLO al CREAR el item. Los dominios que ya
  // estaban en el board —miles, desde 2024— pasan por acá y se quedaban sin marca. Ese era el
  // "a veces sí y a veces no": no es aleatorio, es si el item es nuevo o ya existía.
  // Verificado en el board con tres items de la misma marca:
  //   fr.besoccer.com      creado hoy   → "Agente"
  //   www.es.besoccer.com  creado 2024  → ""
  //   company.besoccer.com creado 06/26 → null
  // Un push manual siempre lo hace un MB, así que la marca es correcta también al actualizar.
  const columnValues = {
    ...(CONFIG.MONDAY_COLUMNS.origen ? { [CONFIG.MONDAY_COLUMNS.origen]: "Manual" } : {}),
    [CONFIG.MONDAY_COLUMNS.comentarios]:   "Manual",
    [CONFIG.MONDAY_COLUMNS.trafico]:       safe(String(traffic || "")),
    [CONFIG.MONDAY_COLUMNS.geo]:           safe(geo || ""),
    ...(email                                                             ? { [CONFIG.MONDAY_COLUMNS.email]:         { email: email, text: email } } : {}),
    ...(personId                                                          ? { [CONFIG.MONDAY_COLUMNS.ejecutivo]:     { personsAndTeams: [{ id: personId, kind: "person" }] } } : {}),
    ...(_mondayIndexCol(estado) ? { [CONFIG.MONDAY_COLUMNS.estado]: _mondayIndexCol(estado) } : {}),
    ...(fecha                               ? { [CONFIG.MONDAY_COLUMNS.fecha_contacto]: { date: fecha } }             : {}),
    ...(_mondayIndexCol(idioma) ? { [CONFIG.MONDAY_COLUMNS.idioma]: _mondayIndexCol(idioma) } : {}),
  };

  const mutation = `mutation {
    change_multiple_column_values(
      item_id: ${itemId},
      board_id: ${CONFIG.MONDAY_ACTIVE_BOARD},
      column_values: "${safe(JSON.stringify(columnValues))}"
    ) { id name }
  }`;

  return mondayRequest(mutation);
}

export const MONDAY_STATES = {
  LIVE:              { index: "0",  label: "LIVE" },
  EN_NEGOCIACION:    { index: "1",  label: "En Negociacion" },
  DESCARTADO:        { index: "2",  label: "Descartado" },
  PROPUESTA_VIGENTE: { index: "3",  label: "Propuesta Vigente" },
  REBOTADO:          { index: "4",  label: "Rebotado" },
  CICLO_FINALIZADO:  { index: "5",  label: "Ciclo Finalizado" },
  MASIVO_DIEGO:      { index: "6",  label: "Masivo - Diego" },
  AVANZADO:          { index: "7",  label: "Avanzado" },
  MAIL_NO_ENVIADO:   { index: "8",  label: "Mail No Enviado" },
  MASIVO_AGUS:       { index: "9",  label: "Masivo - Agus" },
  MASIVO_MAX:        { index: "10", label: "Masivo - Max" },
};

export const RECYCLABLE_STATES = ["Ciclo Finalizado", "Rebotado", "Descartado"];

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
    // Un Map vacío significa "no hay nadie bloqueado" y la cascada mostraría de todo. Es el
    // mismo comportamiento que ya tenía cuando Monday fallaba, y el consumidor tiene un
    // Promise.race con timeout que también cae acá — se conserva, pero se avisa fuerte.
    console.warn("[getMondayBoardIndex] el CRM no contestó, la cascada NO filtra por bloqueados:", e.message);
    return new Map();
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
  // Los envíos cargados a mano en el rango. Antes salía de la columna de fecha de contacto
  // del board de Monday; ahora del CRM, que separa por ORIGEN en vez de por una marca de
  // texto que había que acordarse de escribir.
  try {
    const j = await crmGet("/manuales", { desde, hasta });
    return (j.manuales || []).map(m => ({
      dominio: m.dominio, owner: m.owner, email: m.email, geo: m.geo, fecha: m.fecha,
    }));
  } catch (e) {
    console.warn("[fetchManualSendsFromMonday] el CRM no contestó:", e.message);
    return [];
  }
}
