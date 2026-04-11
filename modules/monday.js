// ============================================================
// ADEQ TOOLBAR — Módulo Monday.com v3
// ============================================================

import { CONFIG } from "../config.js";

const MONDAY_API = "https://api.monday.com/v2";

async function mondayRequest(query) {
  const response = await fetch(MONDAY_API, {
    method: "POST",
    headers: {
      "Content-Type":  "application/json",
      "Authorization": CONFIG.MONDAY_API_KEY,
      "API-Version":   "2024-01",
    },
    body: JSON.stringify({ query }),
  });
  if (!response.ok) throw new Error(`Monday API error: ${response.status}`);
  const json = await response.json();
  if (json.errors) throw new Error(json.errors[0]?.message || "Error en Monday API");
  return json.data;
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

export async function pushToMonday(data) {
  const { domain, traffic, email, geo, pitch, estado, fecha, idioma, ejecutivo } = data;
  const comentario = pitch ? `PITCH IA:\n${pitch}` : "";

  const safe     = (str) => (str || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\r/g, "\\r").replace(/\n/g, "\\n").replace(/\t/g, "\\t");
  const itemName = domain.startsWith("www.") ? domain : `www.${domain}`;

  const columnValues = {
    [CONFIG.MONDAY_COLUMNS.trafico]:       safe(String(traffic || "")),
    [CONFIG.MONDAY_COLUMNS.email]:         safe(email || ""),
    [CONFIG.MONDAY_COLUMNS.geo]:           safe(geo || ""),
    [CONFIG.MONDAY_COLUMNS.comentarios]:   safe(comentario),
    ...(ejecutivo                                                         ? { [CONFIG.MONDAY_COLUMNS.ejecutivo_txt]: safe(ejecutivo) } : {}),
    ...(estado !== undefined && estado !== "" ? { [CONFIG.MONDAY_COLUMNS.estado]:        { index: parseInt(estado) } } : {}),
    ...(fecha                               ? { [CONFIG.MONDAY_COLUMNS.fecha_contacto]: { date: fecha } }             : {}),
    ...(idioma !== "" && idioma !== undefined ? { [CONFIG.MONDAY_COLUMNS.idioma]:        { index: parseInt(idioma) } } : {}),
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

export async function updateMonday({ itemId, traffic, email, geo, pitch, estado, fecha, idioma, ejecutivo }) {
  const comentario = pitch ? `PITCH IA:\n${pitch}` : "";

  const safe = (str) => (str || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");

  const columnValues = {
    [CONFIG.MONDAY_COLUMNS.trafico]:       safe(String(traffic || "")),
    [CONFIG.MONDAY_COLUMNS.email]:         safe(email || ""),
    [CONFIG.MONDAY_COLUMNS.geo]:           safe(geo || ""),
    [CONFIG.MONDAY_COLUMNS.comentarios]:   safe(comentario),
    ...(ejecutivo                                                         ? { [CONFIG.MONDAY_COLUMNS.ejecutivo_txt]: safe(ejecutivo) } : {}),
    ...(estado !== undefined && estado !== "" ? { [CONFIG.MONDAY_COLUMNS.estado]:        { index: parseInt(estado) } } : {}),
    ...(fecha                               ? { [CONFIG.MONDAY_COLUMNS.fecha_contacto]: { date: fecha } }             : {}),
    ...(idioma !== "" && idioma !== undefined ? { [CONFIG.MONDAY_COLUMNS.idioma]:        { index: parseInt(idioma) } } : {}),
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

export async function recycleProspect({ itemId, traffic, email, techDetected, pitch, newStatusIndex }) {
  return updateMonday({
    itemId, traffic, email, techDetected,
    pitch:  `♻️ Reciclado: ${new Date().toLocaleDateString("es-AR")}\n${pitch || ""}`,
    estado: newStatusIndex,
  });
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
  const query = `{
    boards(ids: [${CONFIG.MONDAY_ACTIVE_BOARD}]) {
      items_page(limit: 500) {
        items {
          name
          column_values(ids: [
            "${CONFIG.MONDAY_COLUMNS.ejecutivo}",
            "${CONFIG.MONDAY_COLUMNS.fecha_contacto}"
          ]) { id text }
        }
      }
    }
  }`;

  try {
    const data  = await mondayRequest(query);
    const items = data?.boards?.[0]?.items_page?.items || [];
    const index = new Map();
    for (const item of items) {
      const col = (id) => item.column_values.find(c => c.id === id)?.text || "";
      index.set(cleanDomain(item.name), {
        ejecutivo: col(CONFIG.MONDAY_COLUMNS.ejecutivo),
        fecha:     col(CONFIG.MONDAY_COLUMNS.fecha_contacto),
      });
    }
    return index;
  } catch {
    return new Map();
  }
}

// ── Setear fechas de Follow Up en Monday ──────────────────────
export async function setFollowUpDates(itemId, fu1Date, fu2Date) {
  const safe = (str) => (str || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  const columnValues = {
    [CONFIG.MONDAY_COLUMNS.fecha_fu1]: JSON.stringify({ date: fu1Date }),
    [CONFIG.MONDAY_COLUMNS.fecha_fu2]: JSON.stringify({ date: fu2Date }),
  };
  const mutation = `mutation {
    change_multiple_column_values(
      item_id: ${itemId},
      board_id: ${CONFIG.MONDAY_ACTIVE_BOARD},
      column_values: "${safe(JSON.stringify(columnValues))}"
    ) { id }
  }`;
  return mondayRequest(mutation).catch(err => console.warn("setFollowUpDates:", err.message));
}

function cleanDomain(str) {
  return (str || "").toLowerCase()
    .replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "").trim();
}

// Parsea strings de tráfico con sufijo K/M (ej: "500K", "1.5M") a número entero
function parseTrafficText(str) {
  if (!str) return 0;
  const s = str.trim().toUpperCase().replace(/[,\s]/g, "");
  const n = parseFloat(s);
  if (isNaN(n)) return 0;
  if (s.includes("M")) return Math.round(n * 1_000_000);
  if (s.includes("K")) return Math.round(n * 1_000);
  return parseInt(s.replace(/[^0-9]/g, "")) || 0;
}

// ── fetchImportCandidates — para el tab Import ────────────────
// Trae ítems del board filtrados por geo/idioma, filtra tráfico client-side
export async function fetchImportCandidates({ geo = "", idioma = "", minTraffic = 0 } = {}) {
  const rules = [];
  if (idioma) rules.push(`{ column_id: "${CONFIG.MONDAY_COLUMNS.idioma}", compare_value: ["${idioma}"], operator: any_of }`);
  if (geo)    rules.push(`{ column_id: "${CONFIG.MONDAY_COLUMNS.geo}", compare_value: ["${geo}"], operator: contains_text }`);

  const queryParams = rules.length ? `, query_params: { rules: [${rules.join(",")}] }` : "";

  const query = `{
    boards(ids: [${CONFIG.MONDAY_ACTIVE_BOARD}]) {
      items_page(limit: 150${queryParams}) {
        items {
          name
          column_values(ids: [
            "${CONFIG.MONDAY_COLUMNS.trafico}",
            "${CONFIG.MONDAY_COLUMNS.geo}",
            "${CONFIG.MONDAY_COLUMNS.idioma}"
          ]) { id text }
        }
      }
    }
  }`;

  try {
    const data  = await mondayRequest(query);
    const items = data?.boards?.[0]?.items_page?.items || [];

    return items
      .map(item => {
        const col     = (id) => item.column_values.find(c => c.id === id)?.text || "";
        const traffic = parseTrafficText(col(CONFIG.MONDAY_COLUMNS.trafico));
        const domain  = cleanDomain(item.name);
        return { domain, url: `https://www.${domain}`, traffic, geo: col(CONFIG.MONDAY_COLUMNS.geo), idioma: col(CONFIG.MONDAY_COLUMNS.idioma) };
      })
      .filter(item => item.domain && (!minTraffic || item.traffic >= minTraffic));
  } catch (e) {
    console.error("[Import] fetchImportCandidates error:", e.message);
    return [];
  }
}
