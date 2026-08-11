// ============================================================
// ADEQ Toolbar Agent — Templates de outreach por idioma
// Los 3 borradores fijos, VALIDADOS POR EL USER.
//
// Maxi 2026-08-11: se probó reemplazarlos por un copy más largo (partner
// directo / presupuesto de video / prueba de 15 min) y el user lo rechazó:
// "los borradores tienen que ser cortos". Vuelven los originales. El copy que
// no se aprobó quedó guardado en la base bajo user_email
// '_nuevo_sin_aprobar_2026_08_11_' por si alguna parte llega a servir.
//
// ⚠️ NO AGREGAR PLACEHOLDERS NUEVOS ACÁ SIN TOCAR TAMBIÉN LA EXTENSIÓN.
// Estos mismos textos los usa el MB para los envíos MANUALES desde el popup, y
// esa ruta solo sabe reemplazar {{domain}}. Cualquier otro placeholder le llega
// CRUDO al destinatario — pasó el 11/08 con {{saludo}} y {{sender_name}}.
//
//   B1: identificar al encargado de los anuncios
//   B2: campañas de video (in-stream/out-stream)
//   B3: campañas de display y video
// ============================================================
const TEMPLATES = {
  // ── ESPAÑOL ────────────────────────────────────────────────
  es: [
    {
      subjects: ["Publicidad en {{domain}} - ADEQ", "¿Quién maneja los anuncios? - ADEQ", "Contacto de publicidad - ADEQ"],
      body: `Hola!

Este es el correo de quien maneja los anuncios de la web de {{domain}}?

Soy del área de ventas de ADEQ Media, y me quería poner en contacto con la persona encargada para ofrecerles unas campañas que nos gustaría monetizar con ustedes.

Cualquier cosa avisame.`,
    },
    {
      subjects: ["Campañas de video activas - ADEQ", "Video para {{domain}} - ADEQ", "Campañas de video - ADEQ"],
      body: `Hola, te escribo de ADEQ Media. Tenemos campañas de video activas (in-stream y out-stream) que están funcionando muy bien en websites como el tuyo.

¿Me podrían pasar el whatsapp o contacto de la persona que se encarga de manejar las implementaciones o monetización?

Muchas gracias, espero el dato para escribirles.

Saludos.`,
    },
    {
      subjects: ["Campañas de display y video - ADEQ", "Contacto del encargado - ADEQ", "Display y video para {{domain}} - ADEQ"],
      body: `Buen día.

¿Me podrían pasar el contacto del encargado del website?

Es para poder conversar sobre unas campañas de display y video, que nos interesaría poder incluirlas en su sitio web.

Gracias`,
    },
  ],

  // ── INGLÉS ─────────────────────────────────────────────────
  en: [
    {
      subjects: ["Advertising on {{domain}} - ADEQ", "Who handles the ads? - ADEQ", "Advertising contact - ADEQ"],
      body: `Hi!

Is this the email of the person who handles advertising on {{domain}}?

I'm from the sales team at ADEQ Media, and I wanted to get in touch with the person in charge to offer you some campaigns we'd love to monetize with you.

Let me know.`,
    },
    {
      subjects: ["Active video campaigns - ADEQ", "Video for {{domain}} - ADEQ", "Video campaigns - ADEQ"],
      body: `Hi, I'm reaching out from ADEQ Media. We have active video campaigns (in-stream and out-stream) that are performing really well on sites like yours.

Could you pass me the WhatsApp or contact of the person who handles implementations or monetization?

Thanks a lot, I'll wait for the details to get in touch.

Best.`,
    },
    {
      subjects: ["Display and video campaigns - ADEQ", "Site manager contact - ADEQ", "Display & video for {{domain}} - ADEQ"],
      body: `Good morning.

Could you pass me the contact of the person in charge of the website?

It's to discuss some display and video campaigns that we'd be interested in including on your site.

Thanks`,
    },
  ],

  // ── PORTUGUÉS ──────────────────────────────────────────────
  pt: [
    {
      subjects: ["Publicidade em {{domain}} - ADEQ", "Quem cuida dos anúncios? - ADEQ", "Contato de publicidade - ADEQ"],
      body: `Olá!

Este é o email de quem cuida da publicidade do site {{domain}}?

Sou da área de vendas da ADEQ Media, e queria entrar em contato com a pessoa responsável para oferecer algumas campanhas que gostaríamos de monetizar com vocês.

Qualquer coisa, me avisa.`,
    },
    {
      subjects: ["Campanhas de vídeo ativas - ADEQ", "Vídeo para {{domain}} - ADEQ", "Campanhas de vídeo - ADEQ"],
      body: `Olá, escrevo da ADEQ Media. Temos campanhas de vídeo ativas (in-stream e out-stream) que estão funcionando muito bem em sites como o seu.

Poderiam me passar o WhatsApp ou contato da pessoa que cuida das implementações ou monetização?

Muito obrigado, aguardo o dado para entrar em contato.

Abraços.`,
    },
    {
      subjects: ["Campanhas de display e vídeo - ADEQ", "Contato do responsável - ADEQ", "Display e vídeo para {{domain}} - ADEQ"],
      body: `Bom dia.

Poderiam me passar o contato do responsável pelo site?

É para conversar sobre algumas campanhas de display e vídeo que teríamos interesse em incluir no seu site.

Obrigado`,
    },
  ],

  // ── ITALIANO ───────────────────────────────────────────────
  it: [
    {
      subjects: ["Pubblicità su {{domain}} - ADEQ", "Chi gestisce gli annunci? - ADEQ", "Contatto pubblicità - ADEQ"],
      body: `Ciao!

È questa l'email di chi gestisce la pubblicità del sito {{domain}}?

Sono dell'area vendite di ADEQ Media, e volevo mettermi in contatto con la persona responsabile per proporvi alcune campagne che ci piacerebbe monetizzare con voi.

Fammi sapere.`,
    },
    {
      subjects: ["Campagne video attive - ADEQ", "Video per {{domain}} - ADEQ", "Campagne video - ADEQ"],
      body: `Ciao, ti scrivo da ADEQ Media. Abbiamo campagne video attive (in-stream e out-stream) che stanno funzionando molto bene su siti come il tuo.

Potresti passarmi il WhatsApp o il contatto della persona che si occupa delle implementazioni o della monetizzazione?

Grazie mille, aspetto il contatto per scrivervi.

Saluti.`,
    },
    {
      subjects: ["Campagne display e video - ADEQ", "Contatto del responsabile - ADEQ", "Display e video per {{domain}} - ADEQ"],
      body: `Buongiorno.

Potreste passarmi il contatto del responsabile del sito?

È per parlare di alcune campagne display e video che ci interesserebbe includere sul vostro sito.

Grazie`,
    },
  ],

  // ── ÁRABE ──────────────────────────────────────────────────
  ar: [
    {
      subjects: ["الإعلانات على {{domain}} - ADEQ", "من يدير الإعلانات؟ - ADEQ", "جهة اتصال الإعلانات - ADEQ"],
      body: `مرحباً!

هل هذا هو بريد الشخص المسؤول عن الإعلانات في موقع {{domain}}؟

أنا من قسم المبيعات في ADEQ Media، وأردت التواصل مع الشخص المسؤول لأعرض عليكم بعض الحملات التي يسعدنا تحقيق الدخل منها معكم.

في انتظار ردك.`,
    },
    {
      subjects: ["حملات فيديو نشطة - ADEQ", "فيديو لـ {{domain}} - ADEQ", "حملات فيديو - ADEQ"],
      body: `مرحباً، أكتب إليك من ADEQ Media. لدينا حملات فيديو نشطة (in-stream و out-stream) تحقق نتائج ممتازة على مواقع مثل موقعك.

هل يمكنكم تزويدي برقم واتساب أو بيانات التواصل مع الشخص المسؤول عن التنفيذ أو تحقيق الدخل؟

شكراً جزيلاً، بانتظار المعلومات للتواصل معكم.

مع التحية.`,
    },
    {
      subjects: ["حملات عرض وفيديو - ADEQ", "التواصل مع المسؤول - ADEQ", "عرض وفيديو لـ {{domain}} - ADEQ"],
      body: `صباح الخير.

هل يمكنكم تزويدي ببيانات التواصل مع المسؤول عن الموقع؟

الأمر يتعلق بالتحدث حول بعض حملات العرض (display) والفيديو التي نود تضمينها في موقعكم.

شكراً`,
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
