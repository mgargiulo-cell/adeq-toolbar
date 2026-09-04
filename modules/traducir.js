// ═══════════════════════════════════════════════════════════════════════════════════════
// TRADUCIR — para que el media buyer ENTIENDA lo que está por mandar
// ═══════════════════════════════════════════════════════════════════════════════════════
// Pedido del dueño (2026-09-04): "Que todos los mails tengan la traducción de Google
// gratuita y aparezcan en castellano para que el media buyer entienda lo que se está
// enviando, más allá de que lo real que sale es otra cosa. Que no gaste Claude nunca,
// sólo el gratis de Google."
//
// Y la corrección que define el diseño: "Puede ser que al pasar el cursor por encima te
// muestre la traducción, para no cambiar lo que ves en el recuadro."
//
// Eso es lo importante de acá: **el texto que se manda NUNCA se toca**. La traducción vive
// en un panel aparte que aparece al pasar el mouse. Cambiar el contenido del textarea habría
// abierto la puerta a mandar la traducción por accidente — el peor error posible en esta
// herramienta, porque saldría un mail en castellano a un publisher polaco y nadie se enteraría
// hasta ver el rebote (o peor, la respuesta).
//
// ⚠️ CERO CLAUDE, CERO API PAGA. Se usa el endpoint público de Google Translate (el mismo que
// usa la barra de traducción del navegador). No lleva key, no tiene costo y no pasa por el
// proxy. Si algún día deja de responder, la función devuelve `ok:false` y el panel lo dice —
// no se cae de nuevo en Claude ni en nada que cobre.

// Caché en memoria (la sesión del popup) + en disco (entre aperturas). Las plantillas son
// pocas y fijas: 3 variantes × 23 idiomas. Sin caché le pegaríamos a Google en cada hover.
const _mem = new Map();
const _CLAVE_DISCO = "traducciones_es_v1";
const _TTL_MS = 30 * 24 * 60 * 60 * 1000;   // 30 días: el texto de una plantilla no cambia
let _disco = null;

async function _cargarDisco() {
  if (_disco) return _disco;
  try {
    const { [_CLAVE_DISCO]: g } = await chrome.storage.local.get(_CLAVE_DISCO);
    _disco = (g && typeof g === "object") ? g : {};
  } catch { _disco = {}; }
  return _disco;
}

async function _guardarDisco() {
  try {
    // Se poda antes de guardar: sin esto el storage crece para siempre con textos que el MB
    // editó una vez y no volvió a ver.
    const ahora = Date.now();
    const vivas = Object.entries(_disco).filter(([, v]) => ahora - (v.ts || 0) < _TTL_MS);
    const podado = Object.fromEntries(vivas.slice(-400));
    _disco = podado;
    await chrome.storage.local.set({ [_CLAVE_DISCO]: podado });
  } catch {}
}

// Hash corto y estable del texto — la clave de caché. Se usa el texto y no el id de la
// plantilla a propósito: si alguien edita una plantilla del CRM, el hash cambia y la
// traducción vieja no se muestra como si fuera la nueva.
function _clave(texto, desde) {
  const s = `${desde || "auto"}|${texto}`;
  let h1 = 0x811c9dc5, h2 = 0x01000193;
  for (let i = 0; i < s.length; i++) {
    h1 = ((h1 ^ s.charCodeAt(i)) * 0x01000193) >>> 0;
    h2 = ((h2 + s.charCodeAt(i) * (i + 1)) * 0x85ebca6b) >>> 0;
  }
  return `${h1.toString(36)}${h2.toString(36)}|${s.length}`;
}

/**
 * Traduce a castellano con el endpoint gratuito de Google. Nunca lanza: devuelve
 * `{ ok:false, motivo }` para que quien llama pueda DECIR por qué no hay traducción en vez
 * de mostrar un panel vacío.
 *
 * @param texto  el original, tal cual se va a enviar
 * @param desde  ISO del idioma de origen ("pl", "ja"…). "" = que lo detecte Google.
 * @returns { ok, texto, cache, motivo }
 */
export async function traducirAlCastellano(texto, desde = "") {
  const original = String(texto || "").trim();
  if (!original) return { ok: true, texto: "", cache: true };
  // Ya está en castellano: no hay nada que traducir y no se gasta un request.
  if (String(desde || "").toLowerCase().startsWith("es")) {
    return { ok: true, texto: original, cache: true, mismoIdioma: true };
  }
  // Google corta cerca de los 5.000 caracteres por request. Un pitch nunca llega, pero si
  // llegara se avisa en vez de mandar una traducción cortada sin decirlo.
  if (original.length > 4500) {
    return { ok: false, motivo: "el texto es demasiado largo para el traductor gratuito" };
  }

  const k = _clave(original, desde);
  if (_mem.has(k)) return { ok: true, texto: _mem.get(k), cache: true };
  const disco = await _cargarDisco();
  if (disco[k] && Date.now() - (disco[k].ts || 0) < _TTL_MS) {
    _mem.set(k, disco[k].t);
    return { ok: true, texto: disco[k].t, cache: true };
  }

  // Cascada de tres proveedores, TODOS gratis y sin key. Se probaron los tres en vivo con
  // pitches reales (2026-09-04) antes de elegir el orden:
  //
  //  1. clients5 — el endpoint que usa la EXTENSIÓN OFICIAL de Google Translate en Chrome.
  //     Es el más tolerante: contestó 200 desde una IP donde el clásico devuelve 429, y
  //     conserva los saltos de línea del pitch tal cual.
  //  2. translate_a/single — el clásico (`client=gtx`). Va segundo justamente porque bloquea
  //     con "Sorry..." cuando la IP le parece automatizada.
  //  3. MyMemory — ni siquiera es Google. Último recurso para que un bloqueo de Google no
  //     deje al media buyer sin entender lo que manda. Gratis, sin key, ~5.000 palabras/día.
  //
  // Ninguno cuesta un centavo y ninguno pasa por Claude. Si los tres fallan se devuelve
  // `ok:false` con el motivo, y el panel lo muestra: nunca se cae a algo que cobre.
  const sl = desde || "auto";
  // Aplana la respuesta de Google, que según el endpoint viene como string, como array de
  // strings o como array de arrays. Un `join` ingenuo sobre la forma equivocada devuelve
  // "[object Object]" y eso se le mostraría al MB como si fuera la traducción.
  const _plano = (x) => typeof x === "string" ? x : (Array.isArray(x) ? x.map(_plano).join("") : "");

  const proveedores = [
    {
      nombre: "Google (Chrome)",
      url: `https://clients5.google.com/translate_a/t?client=dict-chrome-ex&sl=${encodeURIComponent(sl)}&tl=es&q=${encodeURIComponent(original)}`,
      leer: (j) => _plano(j),
    },
    {
      nombre: "Google",
      url: `https://translate.googleapis.com/translate_a/single?client=gtx&sl=${encodeURIComponent(sl)}&tl=es&dt=t&q=${encodeURIComponent(original)}`,
      leer: (j) => (Array.isArray(j?.[0]) ? j[0] : []).map(p => (Array.isArray(p) ? p[0] : "")).join(""),
    },
    {
      nombre: "MyMemory",
      // MyMemory necesita el par exacto: sin idioma de origen no se puede pedir. Se saltea.
      url: sl === "auto" ? null : `https://api.mymemory.translated.net/get?q=${encodeURIComponent(original)}&langpair=${encodeURIComponent(sl)}|es`,
      leer: (j) => String(j?.responseData?.translatedText || ""),
    },
  ];

  const fallos = [];
  for (const p of proveedores) {
    if (!p.url) continue;
    try {
      const r = await fetch(p.url, { signal: AbortSignal.timeout(8000) });
      if (!r.ok) { fallos.push(`${p.nombre} ${r.status}`); continue; }
      const salida = p.leer(await r.json());
      if (!salida || !salida.trim()) { fallos.push(`${p.nombre} vacío`); continue; }
      _mem.set(k, salida);
      disco[k] = { t: salida, ts: Date.now() };
      _guardarDisco();
      return { ok: true, texto: salida, cache: false, via: p.nombre };
    } catch (e) {
      const esTimeout = e?.name === "TimeoutError" || e?.name === "AbortError";
      fallos.push(`${p.nombre} ${esTimeout ? "timeout" : (e.message || "error").slice(0, 30)}`);
    }
  }
  return { ok: false, motivo: fallos.join(" · ") || "no se pudo traducir" };
}
