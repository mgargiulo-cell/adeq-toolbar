// ============================================================
// ADEQ Toolbar Agent — Templates de outreach por idioma
// 80% de los emails del agente usan template (cost-effective).
// 20% van a Claude para variedad estilística + A/B test futuro.
//
// Cada template:
//  - Es voz Diego/ADEQ (revshare 80/20, no exclusividad, sin firma).
//  - Tiene placeholders {{domain}} {{traffic}} {{geo}}.
//  - Termina con pregunta (no con CTA genérico).
//  - SIN signature/closing (Gmail aplica firma default).
// ============================================================

// ============================================================
// Templates v2 (2026-05-18) — los 3 templates fijos validados con el user.
//   T1: identificar decision-maker ("¿sos vos?") — usa {{sender_name}}
//   T2: pitch como partner ("sumarnos al stack") — sin nombre
//   T3: sondeo de pain points ("¿qué no rinde?")  — sin nombre
// Cada template repetido en ES/EN/PT/IT/AR con la misma estructura.
// ============================================================
const TEMPLATES = {
  // ── ESPAÑOL ────────────────────────────────────────────────
  es: [
    {
      subjects: [
        "Campañas de Video - ADEQ",
        "Campañas de Video activas - ADEQ",
        "Video para {{domain}} - ADEQ",
      ],
      body: `Hola, ¿cómo estás?

Vi {{domain}} y antes de armarte un mail largo te hago una consulta corta — ¿sos vos quien maneja la monetización del sitio o me conviene hablar con alguien más?

Soy "{{sender_name}}" de ADEQ, y como trabajamos con publishers ayudando a mejorar la monetizacion me queria poner en contacto con la persona responsable.

Espero tu aviso,`,
    },
    {
      subjects: [
        "Dueño de la monetizacion? Conversar sobre ads",
        "Sos el dueño de la monetizacion? Hablamos de ads",
        "Responsable de monetizacion? Conversar sobre ads",
      ],
      body: `Buenas.

Sé que te llegan muchos mails así que intento ser breve. Queria conversar con la persona encargada de publicidad para poder sumarnos como partner de monetizacion.

La idea es sumamos al stack actual, no reemplazamos nada. Sin exclusividad, sin permanencia mínima.

¿Cómo lo ves? ¿Te parece que conversemos sobre formatos que podrian llegar a ir para tu website?

Si te interesa, espero tu feedback y conversamos.

Saludos,`,
    },
    {
      subjects: [
        "Acuerdos anuales de campañas - ADEQ",
        "Acuerdo anual de campañas - ADEQ",
        "Acuerdos anuales para campañas publicitarias - ADEQ",
      ],
      body: `Hola,

Antes de mandarte un mensaje largo te hago una pregunta, ¿hay algún partner de publicidad o formato hoy en {{domain}} que no esté rindiendo como esperaban, o que ya estén pensando en cambiar?

Me gustaria poder conversar para analizar opciones de negocio, realmente creo que podemos hacer buen dinero encontrando entre ambos el formato correcto.

Saludos,`,
    },
  ],

  // ── INGLÉS ─────────────────────────────────────────────────
  en: [
    {
      subjects: [
        "Video Campaigns - ADEQ",
        "Active Video Campaigns - ADEQ",
        "Video for {{domain}} - ADEQ",
      ],
      body: `Hi, how are you?

Saw {{domain}} and before sending you a long email, just a quick question — are you the one handling monetization for the site, or should I reach out to someone else?

I'm "{{sender_name}}" from ADEQ, and since we work with publishers helping improve monetization, I wanted to get in touch with whoever's in charge.

Let me know,`,
    },
    {
      subjects: [
        "Monetization owner? Let's talk ads",
        "Are you the monetization owner? Let's talk ads",
        "Monetization manager? Let's chat about ads",
      ],
      body: `Hi.

I know you get a lot of emails so I'll keep this short. I wanted to reach out to whoever handles ad ops to see if we can join as a monetization partner.

The idea is to add to your current stack, not replace anything. No exclusivity, no minimum commitment.

How does that sound? Could we chat about formats that might work for your site?

If you're interested, let me know and we'll talk.

Best,`,
    },
    {
      subjects: [
        "Annual Campaign Agreements - ADEQ",
        "Annual Campaign Agreement - ADEQ",
        "Annual agreements for ad campaigns - ADEQ",
      ],
      body: `Hi,

Before sending you a long message, just one question — is there any ad partner or format on {{domain}} today that's not performing as expected, or that you're already thinking of changing?

I'd love to chat to look at business options, I really believe we can make good money together finding the right format.

Best,`,
    },
  ],

  // ── PORTUGUÉS ──────────────────────────────────────────────
  pt: [
    {
      subjects: [
        "Campanhas de Vídeo - ADEQ",
        "Campanhas de Vídeo ativas - ADEQ",
        "Vídeo para {{domain}} - ADEQ",
      ],
      body: `Olá, tudo bem?

Vi {{domain}} e antes de te mandar um email longo, faço uma pergunta rápida — você é quem cuida da monetização do site, ou converso com outra pessoa?

Sou o "{{sender_name}}" da ADEQ, e como trabalhamos com publishers ajudando a melhorar a monetização, queria entrar em contato com a pessoa responsável.

Aguardo seu retorno,`,
    },
    {
      subjects: [
        "Responsável pela monetização? Vamos falar sobre ads",
        "Você é o responsável pela monetização? Vamos falar de ads",
        "Quem cuida da monetização? Vamos conversar sobre ads",
      ],
      body: `Olá.

Sei que você recebe muitos emails, então vou ser breve. Queria falar com quem cuida da parte publicitária para podermos nos somar como parceiro de monetização.

A ideia é somar ao stack atual, não substituir nada. Sem exclusividade, sem permanência mínima.

O que acha? Podemos conversar sobre formatos que poderiam funcionar no seu site?

Se tiver interesse, aguardo seu retorno e conversamos.

Abraços,`,
    },
    {
      subjects: [
        "Acordos anuais de campanhas - ADEQ",
        "Acordo anual de campanhas - ADEQ",
        "Acordos anuais para campanhas publicitárias - ADEQ",
      ],
      body: `Olá,

Antes de te mandar uma mensagem longa, uma pergunta — tem algum parceiro de publicidade ou formato hoje em {{domain}} que não está rendendo como esperavam, ou que já estão pensando em mudar?

Gostaria muito de conversar para analisar opções de negócio, acredito que podemos fazer um bom dinheiro juntos encontrando o formato certo.

Abraços,`,
    },
  ],

  // ── ITALIANO ───────────────────────────────────────────────
  it: [
    {
      subjects: [
        "Campagne Video - ADEQ",
        "Campagne Video attive - ADEQ",
        "Video per {{domain}} - ADEQ",
      ],
      body: `Ciao, come stai?

Ho visto {{domain}} e prima di scriverti una mail lunga, ti faccio una domanda rapida — sei tu chi gestisce la monetizzazione del sito, o conviene parlare con qualcun altro?

Sono "{{sender_name}}" di ADEQ, e visto che lavoriamo con publisher aiutando a migliorare la monetizzazione, volevo mettermi in contatto con la persona responsabile.

Aspetto un tuo riscontro,`,
    },
    {
      subjects: [
        "Responsabile della monetizzazione? Parliamo di ads",
        "Sei il responsabile della monetizzazione? Parliamo di ads",
        "Chi gestisce la monetizzazione? Parliamone",
      ],
      body: `Ciao.

So che ricevi tante mail, quindi cerco di essere breve. Volevo parlare con chi gestisce la parte pubblicitaria per poterci unire come partner di monetizzazione.

L'idea è di aggiungerci allo stack attuale, senza sostituire nulla. Senza esclusiva, senza permanenza minima.

Cosa ne pensi? Possiamo parlare di formati che potrebbero andare bene per il tuo sito?

Se ti interessa, resto in attesa di un tuo riscontro e ne parliamo.

Saluti,`,
    },
    {
      subjects: [
        "Accordi annuali per campagne - ADEQ",
        "Accordo annuale per campagne - ADEQ",
        "Accordi annuali per campagne pubblicitarie - ADEQ",
      ],
      body: `Ciao,

Prima di scriverti un messaggio lungo, una domanda — c'è qualche partner pubblicitario o formato oggi su {{domain}} che non sta rendendo come si aspettavano, o che state già pensando di cambiare?

Mi piacerebbe parlare per analizzare opzioni di business, credo davvero che possiamo fare buoni soldi insieme trovando il formato giusto.

Saluti,`,
    },
  ],

  // ── ÁRABE ──────────────────────────────────────────────────
  ar: [
    {
      subjects: [
        "حملات فيديو - ADEQ",
        "حملات فيديو نشطة - ADEQ",
        "فيديو لـ {{domain}} - ADEQ",
      ],
      body: `مرحباً، كيف حالك؟

شاهدت موقع {{domain}}، وقبل أن أرسل لك بريداً مطولاً، لدي سؤال سريع — هل أنت المسؤول عن تحقيق الدخل من الموقع، أم يجدر بي التواصل مع شخص آخر؟

أنا "{{sender_name}}" من شركة ADEQ. نظراً لأننا نعمل مع الناشرين لمساعدتهم في تحسين تحقيق الدخل، أردت التواصل مع الشخص المسؤول.

في انتظار ردك،`,
    },
    {
      subjects: [
        "المسؤول عن تحقيق الدخل؟ لنتحدث عن الإعلانات",
        "هل أنت المسؤول عن تحقيق الدخل؟ لنتحدث عن الإعلانات",
        "من يدير تحقيق الدخل؟ لنتحدث عن الإعلانات",
      ],
      body: `مرحباً.

أعلم أنك تتلقى الكثير من الرسائل، لذا سأكون مختصراً. أردت التحدث مع الشخص المسؤول عن الإعلانات لكي ننضم كشريك في تحقيق الدخل.

الفكرة هي أن نضيف قيمة إلى المنظومة الحالية، دون استبدال أي شيء. بدون حصرية، بدون التزام بمدة أدنى.

ما رأيك؟ هل نتحدث حول الصيغ التي قد تكون مناسبة لموقعك؟

إذا كان الأمر يهمك، أنتظر ردك ونتحدث.

تحياتي،`,
    },
    {
      subjects: [
        "اتفاقيات سنوية للحملات - ADEQ",
        "اتفاقية سنوية للحملات - ADEQ",
        "اتفاقيات سنوية للحملات الإعلانية - ADEQ",
      ],
      body: `مرحباً،

قبل أن أرسل لك رسالة مطولة، لدي سؤال — هل يوجد لديك شريك إعلاني أو صيغة إعلانية على {{domain}} حالياً لا تحقق النتائج المتوقعة، أو تفكرون في تغييرها؟

أود التحدث معك لاستكشاف الفرص التجارية، أعتقد حقاً أننا يمكن أن نحقق نتائج جيدة معاً إذا وجدنا الصيغة المناسبة.

تحياتي،`,
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
  const apply = (s) => String(s || "")
    .replace(/\{\{domain\}\}/g, domain)
    .replace(/\{\{geo\}\}/g, geo)
    .replace(/\{\{traffic\}\}/g, trafficStr)
    .replace(/\{\{sender_name\}\}/g, senderName)
    // Cualquier placeholder restante {{xxx}} → vacío (no leak al user).
    .replace(/\{\{[a-z_]+\}\}/gi, "")
    // Limpieza de artefactos comunes de placeholders vacíos.
    .replace(/[ \t]+([,.;:!?])/g, "$1")           // espacio antes de puntuación
    .replace(/\(\s*\)/g, "")                       // paréntesis vacíos
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
