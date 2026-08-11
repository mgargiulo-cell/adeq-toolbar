// ============================================================
// ADEQ Toolbar Agent — Templates de outreach por idioma
// REESCRITOS 2026-08-11 tras la auditoría de "por qué nadie contesta".
//
// Qué estaba mal en los anteriores (3 textos por idioma, ~200 envíos/semana,
// CERO respuestas reales más allá de algún fuera-de-oficina):
//
//  1. Los tres PEDÍAN UN FAVOR y no ofrecían nada. Dos de los tres pedían
//     "pasame el contacto del encargado" — y el ranking de emails (rankEmail +
//     _pickTier, ~250 líneas) está construido justamente para llegar AL encargado.
//     Al target ideal se le pedía el contacto de sí mismo. Cuanto mejor funcionaba
//     el ranking, más absurdo llegaba el mensaje.
//  2. Personalización real = el dominio, y solo en 1 de 3 textos. {{geo}},
//     {{traffic}} y {{sender_name}} estaban implementados y NO SE USABAN en ninguno.
//  3. No decían qué es ADEQ Media, así que desde el otro lado era indistinguible
//     de un arbitrajista. Un publisher no deja entrar a nadie a su ads.txt a ciegas.
//  4. Sin nombre humano y sin despedida: se delegaba todo a la firma de Gmail.
//
// Reglas del copy nuevo:
//  - Saludar por nombre cuando lo tenemos ({{saludo}} resuelve solo).
//  - Nombrar UNA señal concreta del sitio (su ad stack o su tráfico) para que se
//    note que alguien lo miró. Si no la tenemos, la frase desaparece sola.
//  - Decir qué somos en una línea y ofrecer verificación (sellers.json), que es
//    como se genera confianza en programática.
//  - Pedir UNA cosa concreta y chica, no un favor administrativo.
//  - Firmar con nombre. La firma de Gmail se sigue agregando abajo.
//  - Sin CPMs ni casos inventados: eso lo carga Maxi cuando lo defina.
//
// Placeholders disponibles: {{domain}} {{traffic}} {{geo}} {{sender_name}}
//                           {{saludo}} {{senal}}
// Los que quedan vacíos se limpian solos en fillTemplate (incluida la línea entera
// si queda huérfana), así que ningún mail sale con un hueco raro.
// ============================================================
const TEMPLATES = {
  // ── ESPAÑOL ────────────────────────────────────────────────
  es: [
    {
      subjects: ["Inventario de {{domain}}", "Consulta sobre {{domain}}", "{{domain}} — display y video"],
      body: `{{saludo}}

Soy {{sender_name}}, de ADEQ Media. Compramos inventario de display y video para anunciantes, y trabajamos directo con publishers.

{{senal}}Nos interesa sumar a {{domain}} como partner.

Podés verificarnos en nuestro sellers.json antes de cualquier cosa. Si te sirve, te paso los formatos que estamos comprando y los volúmenes para tu mercado, y vemos si tiene sentido.

¿Te va bien que te escriba con eso?

{{sender_name}}`,
    },
    {
      subjects: ["Video para {{domain}}", "Presupuesto de video — {{domain}}", "{{domain}}: in-stream y out-stream"],
      body: `{{saludo}}

Te escribo de ADEQ Media. Tenemos presupuesto de video (in-stream y out-stream) sin colocar para {{geo}} este trimestre.

{{senal}}Por el perfil de {{domain}}, entra en lo que estamos buscando.

No te pido que hagas nada todavía: si me decís que sí, te mando los formatos y el volumen estimado y lo mirás con calma.

{{sender_name}}
ADEQ Media`,
    },
    {
      subjects: ["{{domain}} — 15 minutos", "Propuesta para {{domain}}", "Monetización de {{domain}}"],
      body: `{{saludo}}

{{sender_name}} de ADEQ Media. Trabajamos con publishers de {{geo}} conectando su inventario con anunciantes, en display y video.

{{senal}}Me gustaría proponerte una prueba chica en {{domain}}: un formato, un mes, y medimos.

Si te interesa, contestame y coordinamos 15 minutos esta semana o la que viene.

Gracias,
{{sender_name}}`,
    },
  ],

  // ── INGLÉS ─────────────────────────────────────────────────
  en: [
    {
      subjects: ["{{domain}} inventory", "Question about {{domain}}", "{{domain}} — display and video"],
      body: `{{saludo}}

I'm {{sender_name}}, from ADEQ Media. We buy display and video inventory for advertisers and work directly with publishers.

{{senal}}We'd like to add {{domain}} as a partner.

You can verify us in our sellers.json before anything else. If it's useful, I'll send over the formats we're buying and the volumes for your market, and we can see if it makes sense.

Would that be alright?

{{sender_name}}`,
    },
    {
      subjects: ["Video budget for {{domain}}", "{{domain}}: in-stream and out-stream", "Video campaigns — {{domain}}"],
      body: `{{saludo}}

I'm writing from ADEQ Media. We have unplaced video budget (in-stream and out-stream) for {{geo}} this quarter.

{{senal}}Given the profile of {{domain}}, it fits what we're looking for.

I'm not asking you to do anything yet: if you say yes, I'll send the formats and estimated volume and you can look at it whenever.

{{sender_name}}
ADEQ Media`,
    },
    {
      subjects: ["{{domain}} — 15 minutes", "A proposal for {{domain}}", "Monetizing {{domain}}"],
      body: `{{saludo}}

{{sender_name}} here, from ADEQ Media. We work with publishers in {{geo}}, connecting their inventory to advertisers across display and video.

{{senal}}I'd like to propose a small test on {{domain}}: one format, one month, and we measure.

If that's interesting, reply and we'll find 15 minutes this week or next.

Thanks,
{{sender_name}}`,
    },
  ],

  // ── PORTUGUÉS ──────────────────────────────────────────────
  pt: [
    {
      subjects: ["Inventário de {{domain}}", "Sobre {{domain}}", "{{domain}} — display e vídeo"],
      body: `{{saludo}}

Sou {{sender_name}}, da ADEQ Media. Compramos inventário de display e vídeo para anunciantes e trabalhamos direto com publishers.

{{senal}}Temos interesse em somar {{domain}} como parceiro.

Você pode nos verificar no nosso sellers.json antes de qualquer coisa. Se for útil, mando os formatos que estamos comprando e os volumes para o seu mercado, e vemos se faz sentido.

Posso te escrever com isso?

{{sender_name}}`,
    },
    {
      subjects: ["Vídeo para {{domain}}", "Verba de vídeo — {{domain}}", "{{domain}}: in-stream e out-stream"],
      body: `{{saludo}}

Escrevo da ADEQ Media. Temos verba de vídeo (in-stream e out-stream) sem alocar para {{geo}} neste trimestre.

{{senal}}Pelo perfil de {{domain}}, entra no que estamos buscando.

Não peço nada ainda: se você disser que sim, mando os formatos e o volume estimado e você olha com calma.

{{sender_name}}
ADEQ Media`,
    },
    {
      subjects: ["{{domain}} — 15 minutos", "Proposta para {{domain}}", "Monetização de {{domain}}"],
      body: `{{saludo}}

{{sender_name}}, da ADEQ Media. Trabalhamos com publishers de {{geo}} conectando o inventário deles a anunciantes, em display e vídeo.

{{senal}}Gostaria de propor um teste pequeno em {{domain}}: um formato, um mês, e medimos.

Se tiver interesse, responda e marcamos 15 minutos esta semana ou na próxima.

Obrigado,
{{sender_name}}`,
    },
  ],

  // ── ITALIANO ───────────────────────────────────────────────
  it: [
    {
      subjects: ["Inventory di {{domain}}", "Una domanda su {{domain}}", "{{domain}} — display e video"],
      body: `{{saludo}}

Sono {{sender_name}}, di ADEQ Media. Compriamo inventory display e video per gli inserzionisti e lavoriamo direttamente con i publisher.

{{senal}}Ci interesserebbe aggiungere {{domain}} tra i nostri partner.

Puoi verificarci nel nostro sellers.json prima di qualsiasi cosa. Se ti è utile, ti mando i formati che stiamo comprando e i volumi per il tuo mercato, e vediamo se ha senso.

Ti va se ti scrivo con quei dati?

{{sender_name}}`,
    },
    {
      subjects: ["Budget video per {{domain}}", "{{domain}}: in-stream e out-stream", "Campagne video — {{domain}}"],
      body: `{{saludo}}

Ti scrivo da ADEQ Media. Abbiamo budget video (in-stream e out-stream) non ancora allocato per {{geo}} in questo trimestre.

{{senal}}Per il profilo di {{domain}}, rientra in quello che stiamo cercando.

Non ti chiedo niente per ora: se mi dici di sì, ti mando i formati e il volume stimato e lo guardi con calma.

{{sender_name}}
ADEQ Media`,
    },
    {
      subjects: ["{{domain}} — 15 minuti", "Una proposta per {{domain}}", "Monetizzazione di {{domain}}"],
      body: `{{saludo}}

{{sender_name}} di ADEQ Media. Lavoriamo con publisher in {{geo}}, collegando il loro inventory agli inserzionisti, su display e video.

{{senal}}Vorrei proporti un test piccolo su {{domain}}: un formato, un mese, e misuriamo.

Se ti interessa, rispondimi e troviamo 15 minuti questa settimana o la prossima.

Grazie,
{{sender_name}}`,
    },
  ],

  // ── ÁRABE ──────────────────────────────────────────────────
  ar: [
    {
      subjects: ["مساحات {{domain}} الإعلانية", "استفسار بخصوص {{domain}}", "{{domain}} — العرض والفيديو"],
      body: `{{saludo}}

أنا {{sender_name}} من ADEQ Media. نشتري مساحات إعلانية للعرض والفيديو لصالح المعلنين، ونعمل مباشرة مع الناشرين.

{{senal}}يهمنا إضافة {{domain}} كشريك.

يمكنك التحقق منا عبر ملف sellers.json الخاص بنا قبل أي شيء. وإذا كان مفيداً، أرسل لك الصيغ التي نشتريها والأحجام المتاحة لسوقك، ونرى إن كان الأمر مناسباً.

هل يناسبك أن أرسل لك هذه التفاصيل؟

{{sender_name}}`,
    },
    {
      subjects: ["ميزانية فيديو لموقع {{domain}}", "{{domain}}: in-stream و out-stream", "حملات فيديو — {{domain}}"],
      body: `{{saludo}}

أكتب إليك من ADEQ Media. لدينا ميزانية فيديو (in-stream و out-stream) غير مخصصة بعد لمنطقة {{geo}} هذا الربع.

{{senal}}بالنظر إلى طبيعة {{domain}}، فهو ضمن ما نبحث عنه.

لا أطلب منك شيئاً الآن: إن وافقت، أرسل لك الصيغ والحجم التقديري وتطّلع عليها براحتك.

{{sender_name}}
ADEQ Media`,
    },
    {
      subjects: ["{{domain}} — 15 دقيقة", "مقترح لموقع {{domain}}", "تحقيق الدخل من {{domain}}"],
      body: `{{saludo}}

{{sender_name}} من ADEQ Media. نعمل مع ناشرين في {{geo}} ونربط مساحاتهم الإعلانية بالمعلنين، في العرض والفيديو.

{{senal}}أود أن أقترح تجربة صغيرة على {{domain}}: صيغة واحدة، لمدة شهر، ونقيس النتائج.

إن كان الأمر يهمك، ردّ عليّ ونحدد 15 دقيقة هذا الأسبوع أو الذي يليه.

شكراً،
{{sender_name}}`,
    },
  ],
};

/**
 * Selecciona un template random para el idioma dado.
 * Si no hay templates para ese idioma, usa inglés como fallback.
 * @param {string} language - código ISO (es/en/pt/it/ar)
 * @returns {{ body: string, subjects: string[] }}
 */
export function pickRandomTemplate(language) {
  const lang = (language || "en").toLowerCase();
  const list = TEMPLATES[lang] || TEMPLATES.en;
  return list[Math.floor(Math.random() * list.length)];
}

/**
 * Aplica placeholders al template: {{domain}}, {{geo}}, {{traffic}}.
 * @param {{ body: string, subjects: string[] }} tpl
 * @param {{ domain: string, geo: string, traffic: number }} vars
 * @returns {{ body: string, subjects: string[] }}
 */
// Mapping email → nombre que firma en los templates ({{sender_name}}).
// Si se agrega un MB nuevo al equipo, sumarlo acá.
const SENDER_NAMES = {
  "mgargiulo@adeqmedia.com": "Maxi",
  "sales@adeqmedia.com":     "Agus",
  "dhorovitz@adeqmedia.com": "Diego",
};

export function getSenderName(userEmail) {
  if (!userEmail) return "";
  return SENDER_NAMES[String(userEmail).toLowerCase().trim()] || "";
}

export function fillTemplate(tpl, vars) {
  // Sanitize: trim + null-safe + collapse any leftover "{{...}}" placeholders
  // que no aplicamos (evita que algo tipo "about [null]" o "{{niche}}" salga
  // al mail real).
  const clean = (v) => {
    if (v == null) return "";
    const s = String(v).trim();
    return s === "null" || s === "undefined" ? "" : s;
  };
  const domain      = clean(vars.domain);
  const geo         = clean(vars.geo);
  const senderName  = clean(vars.senderName);
  const traffic = parseInt(vars.traffic, 10) || 0;
  const trafficStr = traffic > 0
    ? (traffic >= 1_000_000 ? `${Math.round(traffic / 1_000_000)}M` : `${Math.round(traffic / 1_000)}K`)
    : "";

  // ── {{saludo}} — saludar por nombre cuando lo tenemos ────────────────────
  // `contact_name` ya existía en toolbar_review_queue, se le pasaba a Claude y al
  // payload de Monday, pero NUNCA llegaba hasta acá: nadie recibía su nombre en el
  // saludo. Un cold email B2B sin un nombre humano no se contesta.
  // Se toma solo el primer nombre y se descartan valores basura (todo mayúsculas,
  // con arroba, con dígitos o demasiado largos: suelen ser el nombre de la empresa).
  const nombreCrudo = clean(vars.contactName);
  const primerNombre = (() => {
    if (!nombreCrudo || nombreCrudo.length > 40) return "";
    if (/[@\d]/.test(nombreCrudo)) return "";
    const p = nombreCrudo.split(/\s+/)[0].replace(/[^\p{L}'-]/gu, "");
    if (p.length < 2 || p.length > 20) return "";
    if (p === p.toUpperCase() && p.length > 3) return "";        // "REDACCION", "VENTAS"
    return p.charAt(0).toUpperCase() + p.slice(1).toLowerCase();
  })();
  const saludo = primerNombre ? `Hola ${primerNombre},` : "Hola,";

  // ── {{senal}} — la prueba de que alguien miró el sitio ───────────────────
  // Una frase corta con un dato REAL y verificable del sitio. Si no tenemos ningún
  // dato, queda vacía y la frase siguiente se lee igual de bien. Nunca se inventa.
  const redes = Array.isArray(vars.adNetworks) ? vars.adNetworks.filter(Boolean) : [];
  const senal = (() => {
    if (redes.length) {
      const lista = redes.slice(0, 2).join(" y ");
      return `Vi que ya trabajan con ${lista}. `;
    }
    if (trafficStr) return `Vi que están en el orden de ${trafficStr} visitas al mes. `;
    return "";
  })();

  const apply = (s) => String(s || "")
    .replace(/\{\{domain\}\}/g, domain)
    .replace(/\{\{geo\}\}/g, geo)
    .replace(/\{\{traffic\}\}/g, trafficStr)
    .replace(/\{\{sender_name\}\}/g, senderName)
    .replace(/\{\{saludo\}\}/g, saludo)
    .replace(/\{\{senal\}\}/g, senal)
    // Cualquier placeholder restante {{xxx}} → vacío (no leak al user).
    .replace(/\{\{[a-z_]+\}\}/gi, "")
    // Limpieza de artefactos comunes de placeholders vacíos.
    .replace(/[ \t]+([,.;:!?])/g, "$1")           // espacio antes de puntuación
    .replace(/\(\s*\)/g, "")                       // paréntesis vacíos
    // Si un placeholder vacío dejó una preposición colgada ("para  este trimestre",
    // "publishers de , conectando"), se limpia. Sin esto, un lead sin GEO mandaba un
    // mail con un agujero visible y eso solo se nota cuando ya salió.
    .replace(/\s+(para|de|en|for|in|of|per|em|da)\s+([,.])/gi, "$2")
    .replace(/\s+,/g, ",")
    .replace(/[ \t]{2,}/g, " ")                    // espacios dobles
    .replace(/\n{3,}/g, "\n\n")                    // saltos de línea triples
    .trim();
  return {
    body:     apply(tpl.body),
    subjects: tpl.subjects.map(apply).filter(Boolean),
  };
}

/**
 * Decide si usar template (80%) o Claude (20%). Random uniforme.
 * Configurable via toolbar_config.agent_claude_percent (0-100).
 * @param {number} claudePercent - 0-100, default 20
 * @returns {"template"|"claude"}
 */
export function pickPitchSource(claudePercent = 20) {
  const pct = Math.max(0, Math.min(100, claudePercent));
  return Math.random() * 100 < pct ? "claude" : "template";
}

/**
 * Devuelve los templates baked para un idioma con su ID sintético, para
 * que el caller pueda combinarlos con los DB drafts en un solo pool.
 * El ID es `baked_<lang>_<index>` (estable mientras no reordenes el array).
 * @param {string} language - código ISO (es/en/pt/it/ar). Default "en".
 * @returns {Array<{ id: string, body: string, subjects: string[] }>}
 */
export function getBakedTemplates(language) {
  const lang = (language || "en").toLowerCase();
  const list = TEMPLATES[lang] || TEMPLATES.en;
  return list.map((tpl, idx) => ({
    id: `baked_${lang}_${idx}`,
    body: tpl.body,
    subjects: tpl.subjects,
  }));
}
