// ============================================================
// ADEQ TOOLBAR — Módulo Gemini IA
// generatePitch    — pitch de ventas con contexto de categoría
// generateFollowUp — follow-up corto referenciando el pitch original
// analyzeRevenueGap
// ============================================================

import { CONFIG }    from "../config.js";
import { callProxy } from "./apiProxy.js";

const GEMINI_FLASH = "gemini-2.5-flash";
const GEMINI_PRO   = "gemini-2.5-pro";
const GEMINI_BASE  = "https://generativelanguage.googleapis.com/v1beta/models";

// ── Contextos de categoría ────────────────────────────────────
const CATEGORY_CONTEXTS = {
  sports:
    "El sitio es de deportes. Los anunciantes premium son marcas deportivas, apuestas, energéticas y streaming. El tráfico tiene picos en eventos en vivo — mencioná la oportunidad de monetizar spikes con header bidding y formatos de video instream.",
  news:
    "El sitio es de noticias/media. El tráfico es de alta frecuencia. Los mejores formatos son sticky ads y native content. Los anunciantes premium son finance, insurance y automotive. Mencioná la oportunidad de aumentar RPM con real-time bidding.",
  finance:
    "El sitio es financiero. La audiencia tiene alto poder adquisitivo. Los CPMs en finanzas personales, seguros e inversiones son de los más altos del mercado. Mencioná el premium CPM alcanzable con los socios correctos.",
  technology:
    "El sitio es de tecnología. La audiencia es profesional. Los anunciantes B2B pagan CPMs elevados para llegar a este perfil. Mencioná la oportunidad con tecnología de ads orientada a audiencias tech.",
  entertainment:
    "El sitio es de entretenimiento. El formato más efectivo es video pre-roll e instream. Los anunciantes de consumer goods y streaming pagan bien. Mencioná video y branded content como formatos clave.",
  health:
    "El sitio es de salud y bienestar. Los anunciantes de pharma, suplementos y wellness pagan CPMs premium. Mencioná la importancia de un stack de ads que cumpla las regulaciones del sector.",
  travel:
    "El sitio es de viajes y turismo. OTAs, aerolíneas y hoteles son anunciantes de alto valor. Mencioná la oportunidad con programmatic y targeting de intención de viaje.",
  gambling:
    "El sitio es de gambling/apuestas deportivas. Es uno de los verticales con CPMs más altos del mercado digital. Los anunciantes son casinos online, sportsbooks y plataformas de fantasy sports. Mencioná la oportunidad de maximizar revenue con socios especializados en este vertical y formatos de alto impacto como interstitials y rich media.",
  automotive:
    "El sitio es de automoción. Los anunciantes de concesionarias, comparadores de seguros de auto y marcas OEM pagan CPMs premium para alcanzar audiencias con intención de compra. Mencioná la oportunidad con targeting contextual y formatos de display de alto impacto.",
  food:
    "El sitio es de gastronomía, recetas o delivery. Los anunciantes de consumer goods, supermercados, apps de delivery y bebidas pagan bien para llegar a audiencias con alta frecuencia de consumo. Mencioná oportunidades con native ads y formatos visuales que encajan naturalmente en el contenido.",
  realestate:
    "El sitio es de bienes raíces/inmobiliario. Los anunciantes de portales inmobiliarios, bancos con hipotecas y servicios de mudanzas tienen CPMs elevados por la alta intención de compra de la audiencia. Mencioná la oportunidad con programmatic orientado a intención y formatos de lead generation.",
  business:
    "El sitio es de negocios, marketing o servicios B2B. La audiencia son profesionales y tomadores de decisión. Los anunciantes de SaaS, servicios financieros y consultoría pagan CPMs premium para este perfil. Mencioná la oportunidad con targeting de audiencias profesionales y formatos de native content.",
};

function getCategoryContext(category) {
  if (!category) return "";
  const lower = category.toLowerCase();
  for (const [key, ctx] of Object.entries(CATEGORY_CONTEXTS)) {
    if (lower.includes(key)) return ctx;
  }
  return "";
}

// ── Variedad: frases de apertura prohibidas para evitar repetición ───
const AVOID_OPENERS = [
  "I noticed", "I came across", "I was browsing", "I recently visited",
  "Hope this finds you", "I hope you", "I wanted to reach out",
  "My name is", "I'm reaching out", "Quick question",
];

// ── generatePitch ─────────────────────────────────────────────
// Params adicionales:
//   tone:    "informal" | "formal"
//   length:  "short" | "long"
//   focus:   "analysis" | "nodataanalysis"
//   opening: "problem" | "praise" | "direct"
//   favExamples: string[]  — pitches marcados como "Me gusta" para esta config
// ── Idiomas soportados para el pitch ─────────────────────────
const LANG_NAMES = {
  en: "English", es: "Spanish", pt: "Portuguese",
  it: "Italian", ar: "Arabic", fr: "French", de: "German",
};

// ── generatePitch ─────────────────────────────────────────────
// Retorna { body, subjects[] } — body es el email completo, subjects son 3 asuntos sugeridos
export async function generatePitch(ctx) {
  const {
    domain, traffic, techStack, adsTxt, revenueGap, banners, category,
    siteLanguage = "",
    pageTitle = "", pageDescription = "",
    decisionMakerName = "",
    previousPitches = [],
    dislikes = [],
    tone = "informal", length = "short", focus = "analysis", opening = "direct",
    favExamples = [],
  } = ctx;

  const trafficStr = traffic >= 1_000_000
    ? `${Math.round(traffic / 1_000_000)}M`
    : `${Math.round(traffic / 1_000)}K`;

  const techStr      = techStack?.length > 0 ? techStack.join(", ") : "no ad tech detected";
  const adsTxtStr    = adsTxt?.exists ? `ads.txt present (${adsTxt.entries} entries)` : "no ads.txt";
  const gapStr       = revenueGap?.percent > 0 ? `Estimated revenue gap: ${revenueGap.percent}% (~$${revenueGap.usd}/mo).` : "";
  const bannerStr    = banners ? `Ad formats detected: ${banners}.` : "";
  const categoryCtx  = getCategoryContext(category);
  const pageCtxStr   = [pageTitle, pageDescription].filter(Boolean).join(" — ");

  // ── Idioma ────────────────────────────────────────────────
  const langName  = LANG_NAMES[siteLanguage] || "English";
  const langInstr = `Write the ENTIRE email in ${langName}. The website is in ${langName} — the publisher speaks ${langName}. Do not mix languages under any circumstances.`;

  // ── Instrucciones según parámetros ────────────────────────
  const toneInstr = tone === "formal"
    ? "Use a formal, executive tone. Address the recipient professionally."
    : "Use a friendly, conversational tone. Keep it human and approachable.";

  const bodyLengthInstr = length === "long"
    ? "The body should be 4-5 sentences. Develop the context and include a specific proposal."
    : "The body should be 2-3 sentences. Be concise and punchy.";

  const focusInstr = focus === "nodataanalysis"
    ? "Do NOT mention traffic numbers, ad tech, or technical data. Focus purely on the business partnership opportunity."
    : "Use the technical data provided to anchor your argument. Mention at least one specific insight.";

  const openingInstr = opening === "problem"
    ? "Start with a single sentence that identifies a specific gap or missed opportunity on their site."
    : opening === "praise"
    ? "Start with a single sentence — a genuine, specific compliment about the site's reach, content quality, or audience."
    : "Start with a single sentence that goes straight to your value proposition — no pleasantries.";

  // ── Nombre del decisor ────────────────────────────────────
  const recipientInstr = decisionMakerName
    ? `The recipient's name is ${decisionMakerName}. Address them by first name naturally in the opening line (e.g. "Hi ${decisionMakerName},").`
    : `Do not address anyone by name. Always open with a language-appropriate greeting: "Hola," for Spanish, "Hi," for English, "Ciao," for Italian, "Olá," for Portuguese, "Bonjour," for French, "Hallo," for German, "مرحباً," for Arabic.`;

  // ── Anti-repetición: seed aleatorio ───────────────────────
  const ANGLES = [
    "Lead with a revenue angle.", "Lead with an audience quality angle.",
    "Lead with a competitive angle (what similar sites are doing).",
    "Lead with a missed opportunity angle.", "Lead with a tech gap angle.",
    "Lead with a partnership growth angle.", "Lead with a market timing angle.",
  ];
  const angle = ANGLES[Math.floor(Math.random() * ANGLES.length)];
  const avoid = AVOID_OPENERS.sort(() => Math.random() - 0.5).slice(0, 4).join('", "');

  // ── Ángulos ya usados en esta sesión ─────────────────────
  const previousSection = previousPitches.length > 0
    ? `\nANGLES ALREADY USED — these approaches were already sent to this domain, do NOT repeat them:\n${previousPitches.map((p, i) => `${i + 1}. "${p.substring(0, 180)}..."`).join("\n")}\n`
    : "";

  // ── Ejemplos favoritos ────────────────────────────────────
  const favSection = favExamples.length > 0
    ? `\nSTYLE REFERENCE (emails the media buyer liked — replicate the style, NOT the content):\n${favExamples.map((e, i) => `Example ${i + 1}:\n"${e.substring(0, 300)}"`).join("\n\n")}\n`
    : "";

  // ── Anti-ejemplos (No me gusta) ───────────────────────────
  const dislikeSection = dislikes.length > 0
    ? `\nSTYLE TO AVOID — emails the media buyer rejected, do NOT write anything resembling these:\n${dislikes.map((d, i) => `Rejected ${i + 1}:\n"${d.substring(0, 200)}"`).join("\n\n")}\n`
    : "";

  const systemPrompt = `You are a senior Ad Ops consultant at ADEQ Media writing a cold outreach email to the owner or publisher of a website.

LANGUAGE: ${langInstr}

RECIPIENT: ${recipientInstr}
${categoryCtx ? `\nSITE VERTICAL CONTEXT:\n${categoryCtx}\n` : ""}
EMAIL STRUCTURE — follow this exact order:
1. OPENING LINE: ${openingInstr}
2. BODY: ${bodyLengthInstr} ${focusInstr}
3. CLOSING QUESTION: One short, specific question tied to this site — not a generic "would you be open to a call?".
4. SIGN-OFF: Do NOT add any closing phrase, farewell, or signature (no "Best,", no "Regards,", no "Saludos,", no "Cheers,"). End the email with the closing question. A signature is appended automatically.

TONE: ${toneInstr}
ANGLE FOR THIS EMAIL: ${angle}
${previousSection}${favSection}${dislikeSection}
STYLE REFERENCE — real emails sent by ADEQ Media team. Study the tone, structure, and phrasing. Write in this same voice:

Email 1 (Spanish, informal, two options pitch):
"Hola, ¿cómo estás?
Tengo dos opciones con las que estamos obteniendo muy buenos resultados. Por un lado, un slider con CPM fijo de 1 USD, y por otro gestionamos la demanda de display a través de un header bidding interno, donde ponemos a competir 8 diferentes demandas para obtener el mejor precio posible por cada anuncio.
Hemos hablado algunas veces en el pasado, pero nunca llegamos a implementar. Me gustaría que nos puedan evaluar en base a los resultados. No tenemos cláusulas de exclusividad ni períodos mínimos de permanencia: seguimos trabajando juntos solo si generamos buenos resultados.
¿Cómo lo ves?"

Email 2 (Spanish, geo-specific traffic angle):
"Hola, espero que estes muy bien. Te cuento que estamos buscando tráfico de nigeria porque venimos obteniendo muy buenos resultados con distintas demandas, tanto en display como en video. Viendo tu sitio creo que podemos replicar el mismo esquema y sacar un extra en la monetización. Trabajamos con un revshare 80-20 a tu favor y no tenemos ataduras comerciales. Me gustaría que nos puedan probar y sacar sus conclusiones sabiendo que estamos sumando nuevas campañas en el comienzo de Abril. ¿Qué te parece?"

Email 3 (Spanish, follow-up / no response):
"Hola, ¿cómo estás? ¿Tuviste la oportunidad de ver mi correo? Me gustaría recibir tu feedback. Te escribo nuevamente porque estamos obteniendo muy buenos resultados y me gustaría que puedan probarlo, teniendo en cuenta que no tenemos ataduras comerciales y que nuestro objetivo es generar ingresos adicionales para el sitio. Avisame si tenés alguna reserva sobre algún formato o si tenés prioridad en algunas posiciones que te gustaría reemplazar porque no están rindiendo como esperabas."

Email 4 (Spanish, handling objection about other agencies):
"Lo sé, ya he analizado tu sitio. Y lo que comentás es muy común: en la mayoría de los sitios con los que trabajamos también hay otras agencias además de Adsense. Dicho esto, también es cierto que una competencia sana entre dos agencias puede ayudarte a aprovechar de una mejor manera las ubicaciones y obtener mejores resultados. Me gustaría entender qué posiciones podríamos reemplazar, especialmente aquellas que hoy considerás que no están rindiendo bien. Al final, serán los resultados los que hablen. ¿Qué opinás?"

Email 5 (Spanish, SimilarWeb traffic insight + video formats):
"Hola Erick, sí, revisando SimilarWeb noté lo mismo; hay una gran variedad de tráfico de distintos países, pero Suecia es el principal. Contamos con campañas en la mayoría de los países europeos y, además, tenemos presencia en todos los continentes. ¿Ves viable probar con video además de los banners? Tengo opciones tanto para instream como para outstream. Con la primera, podemos aprovechar el reproductor en los momentos en que no se esté sirviendo contenido. La segunda solo se vuelve visible cuando hay una campaña para mostrar; de lo contrario, los lectores no notarán absolutamente nada. ¿Tenés preferencia por alguno de ellos?"

Email 6 (English, reconnect + header bidding + video):
"Hi Michal, how have you been? It's been a while. I'd really like us to reconnect and test the alternatives we're currently working with. The company has grown significantly, and I believe you'll notice the difference. We've implemented an internal header bidding setup that allows all our demand sources to compete for display inventory. For video, we offer both slider and instream options. How does that sound to you? Let me know what your current priorities are on your side."

Email 7 (Spanish, video instream + display header bidding, format preference question):
"Hola Fernando, ¿cómo estás? Dispongo de dos opciones que están teniendo un desempeño muy bueno. Por un lado, un video instream que, en los momentos en que no está reproduciendo publicidad, se puede configurar para mostrar avances de otras noticias de tu sitio; esto ayuda a la recirculación de contenido y aumenta el tiempo de permanencia del usuario. Por otro lado, en todo lo que respecta a display, trabajamos con un header bidding interno con el objetivo de maximizar las ganancias en cada ubicación. ¿Tenés preferencia por algún formato? Estamos cargando nuevas campañas para el mes de abril, por lo que es un buen momento para avanzar."

KEY STYLE TRAITS TO REPLICATE:
- Conversational, direct, no corporate jargon
- Short paragraphs, one idea per paragraph
- Always mention: no exclusivity / no minimum commitment / results-based
- Specific technical knowledge (CPM, header bidding, instream, outstream, revshare)
- End with a specific question, not a generic CTA
- Use site-specific data (traffic, geo, tech stack) to show you've done research

RULES:
- Do NOT start with: "${avoid}"
- No subject line in the body. No sender name. No job title. No sign-off or farewell phrase.
- Every sentence must be complete. Never cut off mid-thought.
- The email must read as a cohesive whole — not a list of disconnected facts.
- Every email must feel freshly written — vary sentence structure, word choice, and angle.

CRITICAL LANGUAGE RULE: The ENTIRE response (body AND all 3 subject lines) MUST be written in ${langName}. Do NOT mix languages. Do NOT include any English if the target is not English. Every word, every subject line, every sentence — in ${langName}.

OUTPUT: Respond with a valid JSON object — no markdown, no extra text:
{
  "body": "the complete email body in ${langName} (no sign-off at the end)",
  "subjects": ["subject 1 in ${langName}", "subject 2 in ${langName}", "subject 3 in ${langName}"]
}
All 3 subject lines must be in ${langName}, compelling, 6-10 words each, and varied in angle.`;

  const userMessage = `Site: ${domain}
Monthly traffic: ${trafficStr} visits
Ad tech: ${techStr}
ads.txt: ${adsTxtStr}
${gapStr}
${bannerStr}
${pageCtxStr ? `Site context: ${pageCtxStr}` : ""}

Write the prospecting email.`;

  const raw = await callGemini(systemPrompt, userMessage, {
    temperature: 0.88,
    maxOutputTokens: length === "long" ? 4000 : 3000,
    responseMimeType: "application/json",
    thinkingBudget: 512,
  }, GEMINI_PRO);

  // Parsear JSON — con fallback progresivo ante respuestas mal formadas
  try {
    // Limpiar posibles wrappers de markdown
    let cleaned = raw
      .replace(/^```json\s*/i, "")
      .replace(/^```\s*/i, "")
      .replace(/```\s*$/, "")
      .trim();
    // Extraer el objeto JSON si hay texto extra alrededor
    const objMatch = cleaned.match(/\{[\s\S]*\}/);
    if (objMatch) cleaned = objMatch[0];
    const parsed = JSON.parse(cleaned);
    return {
      body:     parsed.body     || raw,
      subjects: Array.isArray(parsed.subjects) ? parsed.subjects.filter(Boolean) : [],
    };
  } catch {
    // Último recurso: extraer el body manualmente con regex
    const bodyMatch = raw.match(/"body"\s*:\s*"([\s\S]+?)(?:"\s*,\s*"subjects"|"\s*\}\s*$)/);
    if (bodyMatch) {
      const body = bodyMatch[1].replace(/\\n/g, "\n").replace(/\\"/g, '"').replace(/\\t/g, "\t");
      return { body, subjects: [] };
    }
    return { body: raw, subjects: [] };
  }
}

// ── generateFollowUp ─────────────────────────────────────────
export async function generateFollowUp({ domain, originalPitch, fuNumber, daysSinceSend }) {
  const wordLimit = fuNumber === 1 ? "60 palabras" : "40 palabras";
  const prompt = `Sos un consultor de ADEQ Media. Enviamos un pitch hace ${daysSinceSend} días a ${domain} y no respondieron.

El pitch original fue:
"${(originalPitch || "").substring(0, 300)}"

Escribí un follow-up ${fuNumber === 1 ? "breve" : "muy breve"} (máx ${wordLimit}) para retomar el contacto.

REGLAS:
1. No seas insistente ni agresivo.
2. Referenciá el email anterior sin repetirlo.
3. Preguntá si tuvieron tiempo de revisarlo o si hay alguien más indicado.
4. En inglés. Solo el cuerpo del email, sin asunto ni firma.`;

  return callGemini("", prompt, { temperature: 0.7, maxOutputTokens: 150 });
}

// ── analyzeRevenueGap ────────────────────────────────────────
export async function analyzeRevenueGap(ctx) {
  const { domain, traffic, techStack } = ctx;
  const prompt = `Analiza brevemente el potencial de monetización de este sitio web para ADEQ Media:
- Dominio: ${domain}
- Tráfico: ${traffic?.toLocaleString()} visitas/mes
- Ad tech actual: ${techStack?.join(", ") || "Ninguna detectada"}

En 2-3 líneas, estima:
1. RPM potencial con optimización profesional
2. El cambio técnico más impactante
Responde en español. Sé directo y usa números.`;

  try {
    return await callGemini("", prompt, { temperature: 0.4, maxOutputTokens: 150 });
  } catch {
    return "";
  }
}

// ── Helper interno ────────────────────────────────────────────
async function callGemini(systemPrompt, userMessage, generationConfig = {}, model = GEMINI_FLASH) {
  const gc = {
    temperature:     generationConfig.temperature     ?? 0.7,
    maxOutputTokens: generationConfig.maxOutputTokens ?? 300,
    topP:            generationConfig.topP            ?? 0.9,
  };
  if (generationConfig.responseMimeType) gc.responseMimeType = generationConfig.responseMimeType;
  if (generationConfig.thinkingBudget !== undefined) {
    gc.thinkingConfig = { thinkingBudget: generationConfig.thinkingBudget };
  }

  const body = {
    contents: [{ role: "user", parts: [{ text: userMessage }] }],
    generationConfig: gc,
  };
  if (systemPrompt) {
    body.system_instruction = { parts: [{ text: systemPrompt }] };
  }

  const tryModel = async (m) => {
    const path = `/v1beta/models/${m}:generateContent`;
    const response = await callProxy("gemini", path, { method: "POST", body });
    if (!response.ok) {
      const status = response.status;
      const msg    = response.data?.error?.message || response.text || "sin detalle";
      throw Object.assign(new Error(`Gemini error ${status}: ${msg}`), { status });
    }
    const data = response.data || {};
    const candidate = data?.candidates?.[0];
    const text = candidate?.content?.parts?.[0]?.text;
    if (!text) {
      const reason = candidate?.finishReason || "sin candidatos";
      const blocked = data?.promptFeedback?.blockReason;
      console.error("[Gemini] Respuesta sin texto:", JSON.stringify(data).substring(0, 400));
      throw new Error(`Gemini sin respuesta — razón: ${blocked ? "bloqueado por " + blocked : reason}`);
    }
    return text.trim();
  };

  try {
    return await tryModel(model);
  } catch (err) {
    // Si el modelo solicitado es Pro y devuelve 429 (quota) o 400 (free tier sin billing),
    // reintentar automáticamente con Flash.
    if (model === GEMINI_PRO && (err.status === 429 || err.status === 400)) {
      console.warn("[Gemini] Pro sin cuota, usando Flash como fallback");
      return await tryModel(GEMINI_FLASH);
    }
    throw err;
  }
}
