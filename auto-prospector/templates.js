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
  es: [
    {
      subjects: [
        "Una propuesta para potenciar la monetización de {{domain}}",
        "Trabajando con sitios como {{domain}} en {{geo}}",
        "Header bidding + video para {{domain}}",
      ],
      body: `Hola, ¿cómo estás?

Tengo dos opciones con las que estamos obteniendo muy buenos resultados. Por un lado, gestionamos la demanda de display a través de un header bidding interno donde ponemos a competir 8 demandas distintas para sacar el mejor precio en cada anuncio. Por otro lado, video instream que se puede configurar para mostrar avances de otras notas cuando no hay publicidad — ayuda a recirculación de contenido.

No tenemos cláusulas de exclusividad ni mínimos. Probás, si los resultados dan, seguimos.

¿Te interesa que te muestre números concretos en una llamada corta?`,
    },
    {
      subjects: [
        "Buenos resultados con publishers de {{geo}}",
        "Probar nuevas demandas en {{domain}}",
        "Revshare 80/20 — sin compromisos",
      ],
      body: `Hola, espero que estés bien.

Estamos sumando publishers en {{geo}} con muy buenos resultados, tanto en display como en video. Viendo {{domain}} creo que podemos replicar el mismo esquema y sacar un extra en monetización.

Trabajamos con revshare 80-20 a favor del publisher y sin ataduras comerciales. Me gustaría que nos puedan evaluar en base a resultados.

¿Qué te parece?`,
    },
    {
      subjects: [
        "{{domain}} + nuestro stack de demand",
        "Cómo podemos sumar ingresos a {{domain}}",
        "Probemos sin compromiso en {{domain}}",
      ],
      body: `Hola,

Vengo del lado de la monetización y vi {{domain}}. Quería compartirte que estamos trabajando con un header bidding interno que pone a competir múltiples demand sources por cada impresión + opciones de video (instream y outstream) que se pueden integrar sin afectar la UX actual del sitio.

Sin exclusividad, sin mínimos. Te paso números concretos si te interesa.

¿Charlamos?`,
    },
    {
      subjects: [
        "Optimización de inventario en {{domain}}",
        "Idea concreta para {{domain}}",
        "Sumar demand a {{domain}}",
      ],
      body: `Hola, ¿cómo va?

Estoy revisando sitios con audiencia en {{geo}} y noté que {{domain}} tiene un perfil que encaja bien con lo que estamos buscando. Podemos competir con la demanda actual sin reemplazar nada — corremos en paralelo y el publisher se queda con el mejor CPM por subasta.

Sin períodos mínimos, revshare 80-20. ¿Te interesa probar?`,
    },
    {
      subjects: [
        "Pregunta sobre {{domain}}",
        "Probar formato video en {{domain}}",
        "Demanda extra para {{domain}}",
      ],
      body: `Hola,

¿Tenés video implementado en {{domain}}? Si no es así, podemos integrar un player instream que muestra publicidad cuando está disponible y avances de notas cuando no hay ad — sin afectar la lectura. Y si ya tenés video, podemos sumar demand competitiva.

Trabajamos sin exclusividad. ¿Vemos números juntos?`,
    },
  ],

  // ── INGLÉS ─────────────────────────────────────────────────
  en: [
    {
      subjects: [
        "Header bidding + video for {{domain}}",
        "A proposal to boost {{domain}} monetization",
        "Working with publishers in {{geo}}",
      ],
      body: `Hi, how have you been?

I have two options that are performing really well right now. On one hand, we manage display demand through an internal header bidding setup where 8+ demand sources compete for each impression to get the best CPM. On the other, instream video that can be configured to show your own content previews when no ad is available — boosts recirculation.

No exclusivity, no minimum commitment. You test, if results show up, we keep going.

Would a quick call to share concrete numbers make sense?`,
    },
    {
      subjects: [
        "{{geo}} publishers we're working with",
        "Revshare 80/20 — no strings attached",
        "Trying new demand on {{domain}}",
      ],
      body: `Hi, hope you're well.

We're onboarding publishers in {{geo}} with strong results in both display and video. Looking at {{domain}} I think we can replicate the same setup and add an extra layer of monetization.

We work on an 80/20 revshare in the publisher's favor, no commercial ties. Happy to be evaluated purely on results.

What do you think?`,
    },
    {
      subjects: [
        "Adding demand to {{domain}}",
        "Quick idea for {{domain}}",
        "{{domain}} + our demand stack",
      ],
      body: `Hi,

I'm on the monetization side and came across {{domain}}. Wanted to share that we run an internal header bidding setup with multiple competing demand sources, plus video options (instream and outstream) that integrate without disrupting current UX.

No exclusivity, no minimums. I can share concrete numbers if it's useful.

Worth a chat?`,
    },
    {
      subjects: [
        "Question about {{domain}}",
        "Trying video format on {{domain}}",
        "Extra demand for {{domain}}",
      ],
      body: `Hi,

Do you have video monetization on {{domain}}? If not, we can integrate an instream player that serves ads when available and shows your own content previews when no ad is up — non-intrusive. And if you already have video, we can add competing demand.

We work without exclusivity. Want to see numbers together?`,
    },
    {
      subjects: [
        "Inventory optimization on {{domain}}",
        "{{domain}} + our SSP stack",
        "Test without commitment on {{domain}}",
      ],
      body: `Hi, how's it going?

I've been looking at sites with audiences in {{geo}} and {{domain}} caught my attention — fits well with what we're targeting. We can compete with your current demand without replacing anything — we run in parallel and you keep the highest CPM per auction.

No minimum commitment, 80/20 revshare. Interested in giving it a try?`,
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
