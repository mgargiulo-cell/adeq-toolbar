// ═══════════════════════════════════════════════════════════════════════════════════════
// IDIOMA — en qué idioma está el sitio
// ═══════════════════════════════════════════════════════════════════════════════════════
// Extraído de index.js el 2026-09-02 sin cambiarle la lógica.
//
// Decide en qué idioma sale el pitch, así que un error acá se ve directo en la respuesta:
// mandarle un mail en inglés a un diario mexicano.
//
// ⚠️ Manda el TEXTO, no el TLD ni la cabecera `lang`. Un .com puede ser de cualquier lado y
// muchos sitios declaran `lang="en"` por copiar una plantilla. Reconoce 20 idiomas; cuando el
// alfabeto no es latino (griego, árabe, cirílico, CJK) se resuelve por script.
// El pitch existe en 5 idiomas: lo que no cae en esos sale en inglés, a propósito.
//
// ⚠️ NO es puro: cuando el texto es ambiguo le pregunta a Claude por el proxy. Por eso importa
// la config — es el único módulo de `lib/` que hace red.

import { SUPABASE_URL, SUPABASE_ANON_KEY, BACKEND_BEARER } from "./config.js";

// ── DETECCIÓN ROBUSTA DE IDIOMA — mirror del popup _detectLangFromText ──
// Cascada: <html lang> → og:locale → texto heurístico → GEO → TLD → "en"
// Solo retorna idiomas SOPORTADOS por templates: es/en/it/pt/ar.
// Crítico: el agente debe acertar idioma SIEMPRE — un mail en idioma equivocado
// quema el dominio para siempre.
export const SUPPORTED_AGENT_LANGS = new Set(["es", "en", "it", "pt", "ar"]);

export const TLD_TO_LANG_AGENT = {
  ar:"es", mx:"es", co:"es", cl:"es", pe:"es", uy:"es", py:"es", bo:"es",
  ec:"es", ve:"es", do:"es", cr:"es", pa:"es", gt:"es", hn:"es", sv:"es",
  ni:"es", cu:"es", pr:"es", es:"es",
  br:"pt", pt:"pt",
  it:"it",
  ae:"ar", sa:"ar", eg:"ar", ma:"ar",
  com:"en", net:"en", org:"en", io:"en", uk:"en", us:"en", au:"en", nz:"en",
};

export const GEO_TO_LANG_AGENT = {
  Argentina:"es", Mexico:"es", Colombia:"es", Chile:"es", Peru:"es", Uruguay:"es",
  Paraguay:"es", Bolivia:"es", Ecuador:"es", Venezuela:"es", "Dominican Republic":"es",
  "Costa Rica":"es", Panama:"es", Guatemala:"es", Honduras:"es", "El Salvador":"es",
  Nicaragua:"es", Cuba:"es", "Puerto Rico":"es", Spain:"es",
  Brazil:"pt", Portugal:"pt",
  Italy:"it", Switzerland:"it",
  "United Arab Emirates":"ar", "Saudi Arabia":"ar", Egypt:"ar", Morocco:"ar",
  AR:"es", MX:"es", CO:"es", CL:"es", PE:"es", UY:"es", PY:"es", BO:"es",
  EC:"es", VE:"es", DO:"es", CR:"es", PA:"es", GT:"es", HN:"es", SV:"es",
  NI:"es", CU:"es", PR:"es", ES:"es",
  BR:"pt", PT:"pt",
  IT:"it", CH:"it",
  AE:"ar", SA:"ar", EG:"ar", MA:"ar",
};

// ── ALFABETO DEL SITIO (Maxi 2026-07-28) ──────────────────────────────────────────────────
// Caso real reportado por el user: wanfangdata.com.cn (título 万方数据知识服务平台…, GEO China)
// quedó guardado con idioma "pt". El sistema de votos está pensado para idiomas de alfabeto
// LATINO: ante un sitio en chino ninguna señal aplica, y basta un voto suelto (bonus de acento
// por mojibake, un geo/tld mal mapeado, el árbitro adivinando) para que gane con 1-2 puntos.
// La regla del user es determinista y no depende de adivinar: si el sitio NO está en alfabeto
// latino y no es árabe (el único no-latino con template), el mail sale en INGLÉS.
// Cubre: chino, japonés, coreano, cirílico, griego, hebreo, tailandés, devanagari (hindi),
// bengalí, tamil, armenio, georgiano.
export function _scriptNoLatino(text) {
  const t = String(text || "");
  if (!t) return null;
  if (/[\u0600-\u06FF\u0750-\u077F]/.test(t)) return "ar";       // árabe → SÍ soportado
  if (/[\u4E00-\u9FFF\u3400-\u4DBF]/.test(t)) return "zh";
  if (/[\u3040-\u309F\u30A0-\u30FF]/.test(t)) return "ja";
  if (/[\uAC00-\uD7AF\u1100-\u11FF]/.test(t)) return "ko";
  if (/[\u0400-\u04FF]/.test(t))                 return "cyrillic";
  if (/[\u0370-\u03FF\u1F00-\u1FFF]/.test(t))  return "el";
  if (/[\u0590-\u05FF]/.test(t))                 return "he";
  if (/[\u0E00-\u0E7F]/.test(t))                 return "th";
  if (/[\u0900-\u097F]/.test(t))                 return "hi";
  if (/[\u0980-\u09FF]/.test(t))                 return "bn";
  if (/[\u0B80-\u0BFF]/.test(t))                 return "ta";
  if (/[\u0530-\u058F]/.test(t))                 return "hy";
  if (/[\u10A0-\u10FF]/.test(t))                 return "ka";
  return null;
}

// TLDs de países cuyo idioma NO usa alfabeto latino y NO tenemos template → inglés directo.
// Sirve cuando no pudimos bajar la página y no hay texto que analizar.
export const TLD_NO_LATINO = new Set(["cn","jp","kr","tw","hk","th","ru","ua","by","kz","gr","il","in","bd","lk","np","mm","kh","la","ge","am","rs","bg","mk"]);

// ── DETECTOR DE IDIOMA POR TEXTO (reescrito 2026-07-28, planteo del user) ─────────────────
// El user: "no hay nada más fácil que coger 200 palabras del texto de una web y detectar el
// idioma". Tiene razón, y el diseño anterior lo hacía al revés: el texto era UN voto (peso 10)
// entre ocho señales, compitiendo con el TLD (peso 2) y el GEO (peso 3). Un sitio rumano con
// GEO mal leído terminaba con pitch en portugués.
// AHORA: el texto MANDA. Y para que pueda mandar, tiene que saber reconocer los idiomas que NO
// soportamos — antes solo conocía es/pt/it/en/fr/de, así que ante un sitio polaco o rumano
// encontraba 2 stopwords sueltas y devolvía cualquier cosa. Ahora reconoce 20 idiomas: los 5
// que enviamos y 15 más que sirven para decir "no es ninguno de los nuestros → inglés".
export const LANG_MARKERS = {
  // ── Los 5 que SÍ enviamos ──
  es: /\b(que|los|las|para|por|con|una|del|este|esta|pero|cuando|donde|como|porque|sobre|también|nuestra|nuestro|hola|gracias|hace|noticias|últimas|más|años|días|nuevo|nueva|desde|entre|hasta|muy|todo|puede)\b/gi,
  pt: /\b(que|não|para|com|uma|por|esse|essa|mas|quando|onde|como|porque|sobre|nossa|nosso|você|notícias|últimas|mais|anos|dias|nova|desde|entre|até|muito|tudo|pode|são|está|fazer)\b/gi,
  it: /\b(che|non|per|con|una|del|della|sono|questo|questa|quando|dove|come|perché|grazie|nostra|nostro|notizie|ultim|anni|giorni|nuovo|nuova|più|molto|tutto|essere|anche|dopo)\b/gi,
  en: /\b(the|and|that|for|with|this|from|have|been|will|would|could|should|about|which|their|there|where|when|because|news|latest|more|years|days|new|very|all|can|has|not|are)\b/gi,
  ar: /[\u0600-\u06FF]{3,}/g,
  // ── Los que NO enviamos: reconocerlos es lo que evita el falso es/pt/it ──
  fr: /\b(que|les|des|pour|avec|une|sur|cette|mais|quand|où|comme|parce|notre|votre|bonjour|merci|nouvelles|plus|ans|jours|nouveau|très|tout|être|dans|sont|aussi)\b/gi,
  de: /\b(der|die|das|und|für|mit|ein|eine|nicht|auch|aber|wenn|wie|weil|über|unsere|unser|nachrichten|mehr|jahre|tage|neue|sehr|alle|sind|haben|wird|noch|nach)\b/gi,
  nl: /\b(de|het|een|van|voor|met|niet|maar|ook|wanneer|waar|hoe|omdat|onze|nieuws|meer|jaar|dagen|nieuwe|zeer|alle|zijn|hebben|wordt|naar|door|over)\b/gi,
  pl: /\b(nie|się|jest|dla|przez|jako|który|która|ale|kiedy|gdzie|jak|ponieważ|nasze|nasza|wiadomości|więcej|lata|dni|nowe|bardzo|wszystko|są|mają|oraz|tylko)\b/gi,
  ro: /\b(nu|este|pentru|prin|care|dar|când|unde|cum|pentru că|nostru|noastră|știri|mai|ani|zile|nou|nouă|foarte|toate|sunt|au|și|din|cu|despre)\b/gi,
  tr: /\b(bir|bu|için|ile|ve|değil|ama|zaman|nerede|nasıl|çünkü|bizim|haber|daha|yıl|gün|yeni|çok|tüm|var|olan|olarak|sonra|kadar)\b/gi,
  cs: /\b(není|je|pro|přes|který|ale|když|kde|jak|protože|naše|náš|zprávy|více|roky|dny|nové|velmi|všechno|jsou|mají|nebo|také|což)\b/gi,
  sv: /\b(inte|är|för|med|som|men|när|var|hur|eftersom|vår|våra|nyheter|mer|år|dagar|nya|mycket|alla|har|kan|och|det|att|den)\b/gi,
  da: /\b(ikke|er|for|med|som|men|når|hvor|hvordan|fordi|vores|nyheder|mere|år|dage|nye|meget|alle|har|kan|og|det|at|den)\b/gi,
  no: /\b(ikke|er|for|med|som|men|når|hvor|hvordan|fordi|vår|våre|nyheter|mer|år|dager|nye|mye|alle|har|kan|og|det|som)\b/gi,
  fi: /\b(ei|on|että|kuin|mutta|kun|missä|miten|koska|meidän|uutiset|lisää|vuotta|päivää|uusi|hyvin|kaikki|ovat|voi|ja|se|tämä)\b/gi,
  hu: /\b(nem|van|hogy|mint|de|amikor|hol|hogyan|mert|mi|hírek|több|év|nap|új|nagyon|minden|vannak|lehet|és|ez|az|egy)\b/gi,
  id: /\b(tidak|adalah|untuk|dengan|yang|tetapi|ketika|di mana|bagaimana|karena|kami|berita|lebih|tahun|hari|baru|sangat|semua|dan|ini|itu)\b/gi,
  vi: /\b(không|là|cho|với|mà|nhưng|khi|ở đâu|thế nào|bởi vì|chúng tôi|tin tức|hơn|năm|ngày|mới|rất|tất cả|và|này|đó)\b/gi,
  ca: /\b(que|els|les|per|amb|una|del|aquest|aquesta|però|quan|on|com|perquè|nostra|nostre|notícies|més|anys|dies|nou|molt|tot|són)\b/gi,
};

export const LANGS_ENVIABLES = new Set(["es", "pt", "it", "en", "ar"]);

export function _detectLangFromText(text) {
  const raw = String(text || "");
  // El alfabeto se chequea PRIMERO, antes del mínimo de longitud: en chino/japonés/coreano 30
  // caracteres ya son un párrafo entero, y el umbral pensado para texto latino los descartaba
  // como "muy corto" (caso wanfangdata.com.cn, título de 35 chars).
  const script = _scriptNoLatino(raw);
  if (script === "ar") return { lang: "ar", confidence: "high", scores: { ar: 999 } };
  if (script)          return { lang: script, confidence: "high", scores: {}, noLatino: true };
  if (raw.length < 40) return { lang: null, confidence: "none", scores: {} };

  // Muestra amplia: ~200 palabras es más que suficiente y evita decidir sobre un menú de 5 items.
  const t = raw.slice(0, 4000).toLowerCase();
  const palabras = (t.match(/[\p{L}]+/gu) || []).length;
  if (palabras < 8) return { lang: null, confidence: "low", scores: {} };

  const scores = {};
  for (const [lang, re] of Object.entries(LANG_MARKERS)) {
    scores[lang] = (t.match(re) || []).length;
  }
  // Caracteres muy distintivos: sólo desempatan, no crean un ganador de la nada.
  if (/[ñ¿¡]/.test(raw) && scores.es > 0) scores.es += 5;
  if (/[ãõ]/.test(raw)  && scores.pt > 0) scores.pt += 5;
  if (/[ăâîșț]/.test(raw) && scores.ro > 0) scores.ro += 5;
  if (/[ąćęłńóśźż]/.test(raw) && scores.pl > 0) scores.pl += 5;
  if (/[ğışçö]/.test(raw) && scores.tr > 0) scores.tr += 5;
  if (/ß/.test(raw) && scores.de > 0) scores.de += 5;

  const orden = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  const [top, segundo] = [orden[0], orden[1] || ["", 0]];
  // Densidad: cuántas de las palabras del texto son stopwords del ganador.
  const densidad = top[1] / Math.max(palabras, 1);
  if (top[1] < 4 || densidad < 0.01) return { lang: null, confidence: "low", scores };
  const margen = top[1] - segundo[1];
  const confidence = margen >= 6 ? "high" : margen >= 3 ? "medium" : "low";
  return { lang: top[0], confidence, scores, gap: margen, densidad };
}

// Cache de detección por dominio — evita re-pagar Claude/re-fetchear HTML
export const _domainLangCache = new Map();

export const DOMAIN_LANG_CACHE_MAX = 1000;


// La puerta a Claude vive en index.js (necesita getConfig/setConfigValue/log/saludPing, que
// arrastrarían medio archivo hasta acá). Se inyecta el MÉTODO sobre este objeto exportado —
// el mismo patrón que `_bouncedCache`: se exporta el objeto y index.js muta su contenido,
// que es lo único que ESM le permite a un importador.
export const puertaClaude = { llamar: null };

export async function _claudeLangArbiter(token, domain, sample) {
  if (!sample || sample.length < 30) return null;
  try {
    const data = await puertaClaude.llamar?.(token, "idioma_deteccion", {
model: "claude-haiku-4-5",
max_tokens: 30,
system: "You classify the language of website text. Respond ONLY with a 2-letter ISO code (es/en/pt/it/ar/fr/de/other). No explanation.",
messages: [{ role: "user", content: `Domain: ${domain}\nSample (first 600 chars):\n${sample.substring(0, 600)}` }],
        }, { timeout: 10000 });
    if (!data) return null;
    const text = (data?.content?.[0]?.text || "").trim().toLowerCase();
    const m = text.match(/^([a-z]{2})/);
    return m && SUPPORTED_AGENT_LANGS.has(m[1]) ? m[1] : null;
  } catch { return null; }
}

// Sistema de VOTACIÓN — recolecta todos los signals, weighta, decide.
// El text heuristic es la fuente más confiable (lo que el publisher REALMENTE escribió),
// aunque html lang diga otra cosa (sitios mal-declarados son comunes).
// Si la votación es ambigua y tenemos token, llamamos a Claude Haiku como árbitro.
export async function detectLanguageRobust({ htmlLang, ogLocale, hreflang, jsonLdLang, pathLang, textSample, geo, domain }, opts = {}) {
  const { token = null, allowClaudeArbiter = true } = opts;
  const cleanDomain = (domain || "").replace(/^www\./, "").toLowerCase();

  // Cache hit SOLO si tenemos textSample (= invocación con datos completos).
  // Si solo tenemos geo/domain (hint cheap), no cache para no envenenar.
  const hasFullData = !!textSample;
  if (cleanDomain && hasFullData && _domainLangCache.has(cleanDomain)) {
    return _domainLangCache.get(cleanDomain);
  }

  // Maxi 2026-07-08: cierre unificado — aplica la REGLA DURA (lang SIEMPRE en
  // SUPPORTED_AGENT_LANGS, ante cualquier duda → en), cachea y devuelve. Evita duplicar
  // el cacheo en los múltiples early-returns (incl. el fallback de idioma no soportado).
  const finish = (result) => {
    if (!result.lang || !SUPPORTED_AGENT_LANGS.has(result.lang)) {
      result = { ...result, lang: "en", source: (result.source || "") + "→en_hardrule" };
    }
    if (cleanDomain && hasFullData) {
      if (_domainLangCache.size >= DOMAIN_LANG_CACHE_MAX) {
        const firstKey = _domainLangCache.keys().next().value;
        _domainLangCache.delete(firstKey);
      }
      _domainLangCache.set(cleanDomain, result);
    }
    return result;
  };

  // Recolectar votos: cada signal aporta peso al lang detectado.
  const votes = {}; // lang → puntaje
  const reasons = [];
  const addVote = (lang, weight, source) => {
    if (!lang || !SUPPORTED_AGENT_LANGS.has(lang)) return;
    votes[lang] = (votes[lang] || 0) + weight;
    reasons.push(`${source}:${lang}+${weight}`);
  };

  // ── REGLA DURA DEL ALFABETO (Maxi 2026-07-28) — va ANTES de cualquier voto ──
  // Si el sitio está escrito en un alfabeto que no es latino, ningún voto de stopwords/acentos
  // tiene sentido. Árabe es la única excepción (tenemos template); todo el resto → INGLÉS.
  // Esto cierra de raíz el caso wanfangdata.com.cn → "pt".
  const _script = _scriptNoLatino(textSample || "");
  if (_script === "ar") {
    return finish({ lang: "ar", source: "script_arabe", confidence: "high", reasons: ["alfabeto árabe"] });
  }
  if (_script) {
    return finish({ lang: "en", source: `script_${_script}_→en`, confidence: "high",
                    reasons: [`alfabeto ${_script} sin template → inglés (regla del user)`] });
  }
  // Sin texto para analizar: el TLD del país ya nos dice que no es un idioma que manejemos.
  if (!textSample && TLD_NO_LATINO.has((cleanDomain.split(".").pop() || ""))) {
    return finish({ lang: "en", source: "tld_no_latino_→en", confidence: "medium",
                    reasons: [`TLD .${cleanDomain.split(".").pop()} sin template → inglés`] });
  }

  // ── EL TEXTO DECIDE (Maxi 2026-07-28, planteo del user) ───────────────────────────────
  // Antes el texto era UN voto entre ocho, y el TLD/GEO podían darlo vuelta. Ahora, si tenemos
  // texto y el detector está seguro, ESE es el idioma y no se vota nada más. Es la única señal
  // que mira lo que el publisher realmente escribió.
  //   · detecta uno de los 5 que enviamos  → ese
  //   · detecta otro idioma (pl, ro, tr, nl, de, fr…) → INGLÉS (regla del user: fuera de
  //     {es,en,it,pt,ar} se manda en inglés)
  // Solo si el texto NO alcanza para decidir se cae al sistema de votos de abajo.
  // REGLA DEL USER (2026-07-28), textual:
  //   web hispanohablante → español · italiana → italiano · Portugal/Brasil → portugués
  //   árabe → árabe · cualquier otra cosa → INGLÉS
  // Y: "no te bases siempre en SimilarWeb que puede fallar; toma el idioma del TEXTO +
  // SimilarWeb y compará ambos".
  const _txt = _detectLangFromText(textSample || "");
  const _geoLang = GEO_TO_LANG_AGENT[geo] || GEO_TO_LANG_AGENT[(geo || "").trim()] || null;
  const _tldLang = TLD_TO_LANG_AGENT[cleanDomain.split(".").pop() || ""] || null;

  if (_txt.lang && (_txt.confidence === "high" || _txt.confidence === "medium")) {
    const _final = LANGS_ENVIABLES.has(_txt.lang) ? _txt.lang : "en";
    // CRUCE explícito texto ⨯ GEO. El texto manda siempre (es lo que el publisher realmente
    // escribió; el GEO de SimilarWeb viene mal seguido: windguru.cz figuraba como Argentina).
    // Pero cuando discrepan lo dejamos asentado en el log, que es como se detectan los casos
    // raros sin tener que salir a buscarlos.
    const _cruce = !_geoLang ? "geo sin dato"
                 : _geoLang === _final ? `geo coincide (${geo})`
                 : `⚠️ geo dice ${_geoLang} (${geo}) y el texto dice ${_txt.lang} → mando ${_final} (manda el texto)`;
    return finish({
      lang: _final,
      source: LANGS_ENVIABLES.has(_txt.lang) ? `texto(${_txt.confidence})` : `texto_${_txt.lang}_→en`,
      confidence: _txt.confidence,
      reasons: [`texto→${_txt.lang}`, _cruce],
    });
  }

  // El texto NO alcanzó para decidir (poca cantidad, o empate). Ahí SÍ usamos GEO y TLD, que
  // es exactamente para lo que sirven: desempatar cuando no hay contenido que leer.
  if (_geoLang && _tldLang && _geoLang === _tldLang) {
    return finish({ lang: _geoLang, source: "geo+tld_coinciden", confidence: "medium",
                    reasons: [`sin texto útil; geo(${geo}) y tld coinciden en ${_geoLang}`] });
  }

  // 1) Texto heurístico (más confiable — lo que el publisher escribió)
  const textRes = _detectLangFromText(textSample || "");
  // Maxi 2026-07-08: si el TEXTO detecta CLARAMENTE un idioma NO soportado (de/fr/otro)
  // con confianza media/alta, es señal de que el sitio NO está en uno de nuestros 5 idiomas
  // → EN directo. Antes: un textRes.lang no soportado lo rechazaba addVote() y el resultado
  // caía a señales débiles (geo/tld) o al voto de acento equivocado → es/pt/it falso. Regla
  // del dueño: todo lo que no sea {en,es,pt,ar,it} se manda en INGLÉS.
  if (textRes.lang && !SUPPORTED_AGENT_LANGS.has(textRes.lang) &&
      (textRes.confidence === "high" || textRes.confidence === "medium")) {
    return finish({ lang: "en", source: "unsupported_fallback", confidence: "high",
                    reasons: [`text_unsupported:${textRes.lang}(${textRes.confidence})`] });
  }
  if (textRes.lang) {
    const weight = textRes.confidence === "high" ? 10 : textRes.confidence === "medium" ? 6 : 3;
    addVote(textRes.lang, weight, `text(${textRes.confidence})`);
  }

  // 2) hreflang del primer link alternate (alta confianza si existe)
  const hl = (hreflang || "").toLowerCase().split("-")[0];
  if (SUPPORTED_AGENT_LANGS.has(hl)) addVote(hl, 8, "hreflang");

  // 3) JSON-LD inLanguage (alta confianza si presente)
  const jl = (jsonLdLang || "").toLowerCase().split("-")[0];
  if (SUPPORTED_AGENT_LANGS.has(jl)) addVote(jl, 8, "jsonld");

  // 4) URL path /es/ /pt-BR/ (cuando aparece, muy confiable)
  const pl = (pathLang || "").toLowerCase().split(/[-_]/)[0];
  if (SUPPORTED_AGENT_LANGS.has(pl)) addVote(pl, 7, "url");

  // 5) <html lang> — peso medio (sitios mal-declarados son comunes)
  const hl2 = (htmlLang || "").toLowerCase().split("-")[0];
  if (SUPPORTED_AGENT_LANGS.has(hl2)) addVote(hl2, 5, "html_lang");

  // 6) og:locale — peso medio
  const og = (ogLocale || "").toLowerCase().split(/[-_]/)[0];
  if (SUPPORTED_AGENT_LANGS.has(og)) addVote(og, 5, "og");

  // 7) GEO → idioma — peso bajo (RapidAPI a veces devuelve geo wrong)
  const geoLang = GEO_TO_LANG_AGENT[geo] || GEO_TO_LANG_AGENT[(geo || "").trim()];
  if (geoLang && SUPPORTED_AGENT_LANGS.has(geoLang)) addVote(geoLang, 3, "geo");

  // 8) TLD — peso bajo (.com domina, poco discriminativo)
  const tld = cleanDomain.split(".").pop() || "";
  const tldLang = TLD_TO_LANG_AGENT[tld];
  if (tldLang && SUPPORTED_AGENT_LANGS.has(tldLang)) addVote(tldLang, 2, "tld");

  // ── Decisión ────────────────────────────────────────────────
  const sorted = Object.entries(votes).sort((a, b) => b[1] - a[1]);
  let result;
  if (sorted.length === 0) {
    result = { lang: "en", source: "default", confidence: "low", reasons: ["no_signals"] };
  } else {
    const winner = sorted[0];
    const runnerUp = sorted[1] || ["", 0];
    const margin = winner[1] - runnerUp[1];
    // Confianza: si winner score >= 10 Y margen >= 5 → high. Si margen < 3 → low (ambiguo).
    const confidence = winner[1] >= 10 && margin >= 5 ? "high" : margin >= 3 ? "medium" : "low";

    // Si confianza baja Y tenemos token + sample → Claude Haiku decide
    if (confidence === "low" && allowClaudeArbiter && token && textSample) {
      const claudeAns = await _claudeLangArbiter(token, cleanDomain, textSample);
      if (claudeAns) {
        result = { lang: claudeAns, source: "claude_arbiter", confidence: "high", reasons: [...reasons, `claude:${claudeAns}`] };
      } else {
        // Maxi 2026-07-08: el árbitro devolvió null (Claude dijo un idioma NO soportado —
        // de/fr/other — o hubo error). En baja confianza NO caemos al winner dudoso (voto de
        // acento/geo equivocado): el fallback CORRECTO es EN. Antes esto mandaba pt/es/it falso.
        result = { lang: "en", source: "arbiter_null_fallback", confidence: "low",
                   reasons: [...reasons, "claude_null→en"] };
      }
    } else {
      result = { lang: winner[0], source: "voting", confidence, reasons };
    }
  }

  // Maxi 2026-07-08: cierre unificado (regla dura SUPPORTED-only + cache) vía finish().
  return finish(result);
}
