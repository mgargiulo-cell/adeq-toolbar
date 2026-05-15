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

const TEMPLATES = {
  // ── ESPAÑOL ────────────────────────────────────────────────
  // Estilo casual conversacional — drafts aprobados por el user 2026-05-12.
  es: [
    {
      subjects: [
        "Pregunta sobre {{domain}}",
        "Quien maneja pautas en {{domain}}?",
        "Contacto comercial de {{domain}}",
      ],
      body: `Hola! Vi {{domain}} y queria preguntarte si sos vos quien maneja las pautas publicitarias del sitio, o si me podes pasar el contacto del que decide.

Soy de ADEQ Media, trabajamos con publishers monetizando inventario. Quiero ver si te puedo sumar algo.

Cualquier cosa avisame.`,
    },
    {
      subjects: [
        "Video activo para {{domain}}",
        "Campañas in-stream + out-stream para {{domain}}",
        "Sumar video a {{domain}}",
      ],
      body: `Hola, te escribo de ADEQ Media. Tenemos campañas de video activas (in-stream y out-stream) que andan muy bien con sitios como {{domain}}.

CPMs decentes, fill alto, sin pisarte la UX. Si te interesa te paso un breakdown rápido.

Decime y te mando los detalles.`,
    },
    {
      subjects: [
        "Header bidding en {{domain}}?",
        "Levantar eCPM en {{domain}} sin tocar la integración",
        "Setup HB para {{domain}}",
        "Estás monetizando todo el inventario de {{domain}}?",
        "Una idea para {{domain}} (toma 2 min leer)",
        "{{domain}} + demand stack ADEQ",
        "Te suma demanda extra en {{domain}}?",
      ],
      body: `Hola, soy de ADEQ. Vi que {{domain}} tiene buen tráfico y queria preguntarte si ya estás corriendo header bidding o si lo manejas todo via Google directo.

Tenemos un setup que suele levantar 15-30% del eCPM sin tocar la integración actual. Si te quedan minutos te muestro como.`,
    },
  ],

  // ── INGLÉS ─────────────────────────────────────────────────
  en: [
    {
      subjects: [
        "Quick question about {{domain}}",
        "Who handles ads on {{domain}}?",
        "Commercial contact for {{domain}}",
      ],
      body: `Hi! Saw {{domain}} and wanted to ask if you're the one handling ad partnerships, or if you can point me to whoever decides.

I'm with ADEQ Media, we work with publishers monetizing inventory. Want to see if I can add something on our end.

Let me know either way.`,
    },
    {
      subjects: [
        "Active video for {{domain}}",
        "In-stream + out-stream campaigns for {{domain}}",
        "Adding video to {{domain}}",
      ],
      body: `Hi, this is ADEQ Media. We have video campaigns running (in-stream and out-stream) that are doing really well on sites like {{domain}}.

Decent CPMs, high fill, doesn't break UX. If you want I'll send a quick breakdown.

Let me know and I'll send the details.`,
    },
    {
      subjects: [
        "Header bidding on {{domain}}?",
        "Lift eCPM on {{domain}} without touching integration",
        "HB setup for {{domain}}",
        "Are you monetizing all {{domain}} inventory?",
        "An idea for {{domain}} (2 min read)",
        "{{domain}} + ADEQ demand stack",
        "Extra demand for {{domain}}?",
      ],
      body: `Hi, ADEQ here. Saw {{domain}} has solid traffic and wanted to ask if you're already running header bidding or handling everything through Google directly.

We have a setup that usually lifts eCPM 15-30% without touching the current integration. If you have a few minutes I'll show you how.`,
    },
  ],

  // ── PORTUGUÉS ──────────────────────────────────────────────
  pt: [
    {
      subjects: [
        "Header bidding + vídeo para {{domain}}",
        "Trabalhando com publishers em {{geo}}",
        "Uma proposta para {{domain}}",
      ],
      body: `Olá, tudo bem?

Tenho duas opções que estão dando ótimos resultados. Por um lado, gerenciamos demanda de display via header bidding interno onde 8+ demand sources competem por impressão para conseguir o melhor CPM. Por outro, vídeo instream configurável para mostrar previews do próprio conteúdo quando não há ad — ajuda na recirculação.

Sem exclusividade, sem mínimos. Você testa, se der resultado, continuamos.

Faz sentido uma call rápida para compartilhar números concretos?`,
    },
    {
      subjects: [
        "Publishers em {{geo}}",
        "Revshare 80/20 — sem amarras",
        "Testando nova demanda em {{domain}}",
      ],
      body: `Olá, espero que esteja bem.

Estamos onboardando publishers em {{geo}} com bons resultados em display e vídeo. Vendo {{domain}} acredito que podemos replicar o esquema e adicionar uma camada extra de monetização.

Trabalhamos com revshare 80/20 a favor do publisher, sem amarras. Avaliação puramente por resultados.

O que acha?`,
    },
    {
      subjects: [
        "{{domain}} + nosso demand stack",
        "Adicionando demanda em {{domain}}",
        "Ideia rápida para {{domain}}",
      ],
      body: `Olá,

Estou no lado de monetização e vi {{domain}}. Queria compartilhar que rodamos header bidding interno com múltiplas demand sources competindo, e opções de vídeo (instream e outstream) que integram sem afetar a UX.

Sem exclusividade, sem mínimos. Posso compartilhar números concretos se for útil.

Vale conversar?`,
    },
  ],

  // ── ITALIANO ───────────────────────────────────────────────
  it: [
    {
      subjects: [
        "Header bidding + video per {{domain}}",
        "Una proposta per {{domain}}",
        "Lavoriamo con publisher in {{geo}}",
      ],
      body: `Ciao, come va?

Ho due opzioni che stanno dando ottimi risultati. Da un lato, gestiamo la domanda display tramite header bidding interno dove 8+ demand sources competono per ogni impression. Dall'altro, video instream che si può configurare per mostrare anteprime di contenuti propri quando non c'è pubblicità — aiuta la ricircolazione.

Senza esclusività, senza minimi. Testi, se i risultati arrivano, continuiamo.

Avrebbe senso una call veloce per condividere numeri concreti?`,
    },
    {
      subjects: [
        "Publisher in {{geo}}",
        "Revshare 80/20 — senza vincoli",
        "Provando nuova domanda su {{domain}}",
      ],
      body: `Ciao, spero stia bene.

Stiamo onboardando publisher in {{geo}} con risultati molto buoni in display e video. Guardando {{domain}} credo possiamo replicare lo schema e aggiungere un livello extra di monetizzazione.

Lavoriamo con revshare 80/20 a favore del publisher, senza vincoli commerciali. Valutazione purely sui risultati.

Cosa ne pensa?`,
    },
    {
      subjects: [
        "{{domain}} + il nostro demand stack",
        "Aggiungere domanda su {{domain}}",
        "Idea rapida per {{domain}}",
      ],
      body: `Ciao,

Sono del lato monetizzazione e ho visto {{domain}}. Volevo condividere che gestiamo un setup di header bidding interno con multiple demand sources che competono, e opzioni video (instream e outstream) che integrano senza alterare l'UX.

Senza esclusività, senza minimi. Posso condividere numeri concreti se utile.

Vale una chiacchierata?`,
    },
  ],

  // ── ÁRABE ──────────────────────────────────────────────────
  ar: [
    {
      subjects: [
        "اقتراح لتحسين تحقيق الدخل من {{domain}}",
        "Header bidding وفيديو لموقع {{domain}}",
        "نعمل مع ناشرين في {{geo}}",
      ],
      body: `مرحباً،

لدي خياران يحققان نتائج ممتازة حالياً. من جهة، نُدير طلب الإعلانات عبر header bidding داخلي حيث تتنافس أكثر من 8 demand sources للحصول على أفضل سعر CPM. من جهة أخرى، فيديو instream يمكن تكوينه لعرض محتوى الموقع عند عدم وجود إعلانات.

بدون حصرية، بدون التزامات. تختبر، إذا جاءت النتائج، نستمر.

هل تودّ مكالمة قصيرة لمشاركة أرقام محددة؟`,
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
export function fillTemplate(tpl, vars) {
  const trafficStr = vars.traffic
    ? (vars.traffic >= 1_000_000 ? `${Math.round(vars.traffic / 1_000_000)}M` : `${Math.round(vars.traffic / 1_000)}K`)
    : "—";
  const apply = (s) => String(s || "")
    .replace(/\{\{domain\}\}/g, vars.domain || "")
    .replace(/\{\{geo\}\}/g, vars.geo || "")
    .replace(/\{\{traffic\}\}/g, trafficStr);
  return {
    body:     apply(tpl.body),
    subjects: tpl.subjects.map(apply),
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
