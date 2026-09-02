// ═══════════════════════════════════════════════════════════════════════════════════════
// TELÉFONO — sacar un número de contacto de una página, sin inventarlo
// ═══════════════════════════════════════════════════════════════════════════════════════
// Escrito el 2026-09-02 para reemplazar a `extractPhonesFromHtml`, que capturaba cualquier
// cosa con separadores. Medido sobre los 2.497 "teléfonos" que había guardados:
//
//     482  decimales sueltos       226.359375
//     260  tipo IP o versión       255-255-255
//      75  empezaban con un año    2026-20007253
//     198  teléfonos de verdad
//
// O sea: 1 de cada 3 no era un teléfono, y de los que quedaban casi ninguno traía código de
// país, así que el CRM los rechazaba. Resultado: 59 teléfonos sobre 10.006 fichas.
//
// LAS DOS CAUSAS, y cómo se corrigen acá:
//
//  1. El patrón de "número visible" era `\d{2,4}[\s.-]\d{3,4}[\s.-]?\d{3,4}` — eso matchea
//     un precio, una coordenada, un id o una fecha. Ahora un número suelto NO alcanza:
//     tiene que venir de una fuente que lo declare teléfono (tel:, WhatsApp, schema.org) o
//     estar pegado a una palabra que lo anuncie ("Tel:", "Teléfono", "Llamanos"…).
//
//  2. El email se buscaba en 65 rutas y el teléfono sólo en la home. Por eso esta función
//     recibe TEXTO, no una URL: se la llama con el HTML que el scraper de contacto YA
//     descargó para buscar emails. Sale gratis y a la misma velocidad.
//
// ⚠️ Ante la duda NO devuelve nada. Un teléfono inventado se lo damos a un media buyer que
// va a llamar a un número equivocado; un hueco sólo cuesta no llamar.

import { prefijoDe } from "./geo.js";

// ⚠️ Países donde el 0 inicial es PARTE DEL NÚMERO, no un prefijo de larga distancia.
// Italia es el caso clásico: sus fijos se marcan +39 0332 873094, con el cero. Sacárselo
// —como corresponde en casi todos los demás— da un número que no existe.
const CONSERVAN_EL_CERO = new Set(["39"]);   // Italia

/** Formas que NO son un teléfono aunque tengan dígitos y separadores. */
function esBasura(s) {
  const t = String(s || "").trim();
  const d = t.replace(/\D/g, "");
  if (/^\d{1,4}[.,]\d{4,}$/.test(t)) return "decimal";              // 226.359375
  // ⚠️ NO se descarta por "parecer una IP" (255-255-255). Se probó y rechazaba teléfonos
  // españoles reales escritos 91.234.56.78 — misma forma, mismos largos. Lo que separa un
  // teléfono de una versión no es la forma, es que ALGUIEN LO ANUNCIE como teléfono, y de
  // eso se encarga `extraerTelefonos`: un número suelto no entra por ningún camino.
  if (/^(19|20)\d{2}[-.\/]/.test(t)) return "empieza con año";       // 2026-20007253
  if (/^\d{4}-\d{2}-\d{2}/.test(t)) return "fecha ISO";
  if (/^0+$/.test(d)) return "todo ceros";
  if (/^(\d)\1{6,}$/.test(d)) return "dígito repetido";              // 1111111111
  if (/^123456|^987654/.test(d)) return "secuencia";
  // Un ÚNICO punto seguido de exactamente 3 dígitos al final es separador de miles o
  // decimal, no un teléfono: "145 163.225" tenía los 9 dígitos que usa Polonia y pasaba.
  // Los teléfonos europeos que usan punto lo repiten (91.234.56.78), no lo usan una sola vez.
  if ((t.match(/\./g) || []).length === 1 && /\.\d{3}$/.test(t) && /\s/.test(t)) return "separador de miles";
  return null;
}

/** Un teléfono en E.164, o null. `geo` es el país del sitio (nombre de SimilarWeb). */
export function normalizarTelefono(bruto, geo) {
  const t = String(bruto || "").trim();
  if (!t || esBasura(t)) return null;

  if (t.startsWith("+") || t.startsWith("00")) {
    let d = t.replace(/^00/, "").replace(/\D/g, "");
    // ⚠️ El 0 de larga distancia NACIONAL no va después del prefijo internacional. Se escribe
    // "+54 0810 333 0365" en muchos sitios, pero marcar +540810... no llega a ningún lado.
    // Sólo se saca cuando se sabe el país y sin ese 0 el largo cierra: si no, se deja como
    // está, porque en algunos países el 0 SÍ es parte del número nacional.
    const inf = prefijoDe(geo);
    if (inf && !CONSERVAN_EL_CERO.has(String(inf[0]))) {
      const cc = String(inf[0]);
      if (d.startsWith(cc + "0") && inf[1].includes(d.length - cc.length - 1)) {
        d = cc + d.slice(cc.length + 1);
      }
    }
    return d.length >= 8 && d.length <= 15 ? "+" + d : null;
  }

  // Sin prefijo internacional hace falta saber de qué país es. Adivinarlo sería inventar.
  const info = prefijoDe(geo);
  if (!info) return null;
  const [cc, largos] = info;
  let d = t.replace(/\D/g, "");
  // Mismo cuidado que arriba: en Italia el 0 va, en el resto es larga distancia y sale.
  if (d.startsWith("0") && !CONSERVAN_EL_CERO.has(String(cc))) d = d.replace(/^0+/, "");
  const ccs = String(cc);
  if (d.startsWith(ccs) && largos.includes(d.length - ccs.length)) return "+" + d;
  // El largo tiene que ser el que ese país usa. Es lo que descarta los ids y las coordenadas
  // que sobrevivieron al filtro de forma.
  if (!largos.includes(d.length)) return null;
  return `+${ccs}${d}`;
}

/** Sin país no se puede pasar a E.164, pero el candidato igual se conserva limpio. */
function _soloCandidato(bruto) {
  const t = String(bruto || "").trim();
  if (esBasura(t)) return null;
  const d = t.replace(/\D/g, "");
  if (d.length < 8 || d.length > 15) return null;
  return t.startsWith("+") ? "+" + d : t.replace(/\s+/g, " ");
}

/** Palabras que anuncian un teléfono, en los idiomas donde prospectamos. */
const ANUNCIA = [
  "tel", "telefono", "teléfono", "telefone", "telefon", "téléphone", "phone", "call",
  "llamanos", "llámanos", "llamar", "contacto", "contact", "kontakt", "contatto", "contato",
  "whatsapp", "wsp", "celular", "movil", "móvil", "mobile", "cel", "fono", "hotline",
  "τηλέφωνο", "телефон", "هاتف", "ligue", "chame",
].join("|");

const RE_NUM = "\\+?\\d[\\d\\s().\\-]{6,18}\\d";

/**
 * Devuelve { telefonos: [], whatsapps: [] } en E.164, ordenados por confianza.
 * `html` es el de una página que ya se descargó (no hace pedidos nuevos).
 */
export function extraerTelefonos(html, geo = "") {
  if (!html) return { telefonos: [], whatsapps: [] };
  const tel = new Map();       // e164 → confianza (mayor = mejor)
  const wa = new Set();
  // Si se conoce el país se normaliza acá; si no, se devuelve el candidato LIMPIO para que
  // lo normalice quien sí lo sepa (saveToReviewQueue). Lo que NO se hace nunca es adivinar
  // el prefijo: un teléfono con el país equivocado es peor que no tener teléfono.
  const guardar = (bruto, conf) => {
    const n = normalizarTelefono(bruto, geo);
    const val = n || (geo ? null : _soloCandidato(bruto));
    if (val && (tel.get(val) ?? 0) < conf) tel.set(val, conf);
  };

  // ── 1 · WhatsApp: el número viene con país por definición del link ────────────────────
  for (const m of html.matchAll(/(?:wa\.me\/|(?:api\.)?whatsapp\.com\/send\/?\?phone=|whatsapp:\/\/send\?phone=)(\+?\d{8,15})/gi)) {
    const d = m[1].replace(/\D/g, "");
    if (d.length >= 8 && d.length <= 15) { wa.add("+" + d); guardar("+" + d, 100); }
  }

  // ── 2 · tel: — el sitio lo declara teléfono, no hay ambigüedad ────────────────────────
  for (const m of html.matchAll(/(?:href=["']|\s)tel:([+\d][\d\s().\-]{6,20})/gi)) guardar(m[1], 95);

  // ── 3 · schema.org: telephone en JSON-LD o microdata ─────────────────────────────────
  for (const m of html.matchAll(/"telephone"\s*:\s*"([^"]{7,25})"/gi)) guardar(m[1], 90);
  for (const m of html.matchAll(/itemprop=["']telephone["'][^>]*content=["']([^"']{7,25})["']/gi)) guardar(m[1], 90);
  for (const m of html.matchAll(/itemprop=["']telephone["'][^>]*>([^<]{7,30})</gi)) guardar(m[1], 85);

  // ── 4 · Anunciado por una palabra ("Tel: …", "Teléfono …") ───────────────────────────
  // Es lo que reemplaza al viejo "cualquier número con separadores": el número solo no
  // alcanza, alguien tiene que estar diciendo que es un teléfono.
  const texto = html.replace(/<[^>]+>/g, " ").replace(/&nbsp;?/gi, " ");
  for (const m of texto.matchAll(new RegExp(`(?:${ANUNCIA})\\s*[:.\\-–]?\\s*(${RE_NUM})`, "gi"))) {
    guardar(m[1], 70);
  }

  const telefonos = [...tel.entries()].sort((a, b) => b[1] - a[1]).map(([n]) => n);
  return { telefonos: telefonos.slice(0, 5), whatsapps: [...wa].slice(0, 3) };
}
