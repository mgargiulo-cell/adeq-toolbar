-- Seed de borradores default (3 × 5 idiomas = 15 templates).
-- user_email='_default_' los marca como compartidos para todos los users.
-- Cada usuario los ve en getPitchDrafts() y al editarlos crea su propia copia
-- privada (porque el INSERT/UPDATE va con user_email=su email).
--
-- Tono: informal, directo, sin formalidades. {{domain}} se reemplaza al cargar.
-- Priority 1 = el más corto (preguntando por el dueño) → se autocarga.
-- Priority 2 = video.
-- Priority 3 = header bidding.

-- Limpieza previa (idempotente — corré este script las veces que quieras)
DELETE FROM toolbar_pitch_drafts WHERE user_email = '_default_';

INSERT INTO toolbar_pitch_drafts (user_email, name, language, subject, body, priority, is_default, updated_at) VALUES

-- ─── ESPAÑOL ─────────────────────────────────────────────────
('_default_', 'ES · Owner directo',  'es', 'Hablamos?',
 'Hola! Vi {{domain}} y queria preguntarte si sos vos quien maneja las pautas publicitarias del sitio, o si me podes pasar el contacto del que decide.

Soy de ADEQ Media, trabajamos con publishers monetizando inventario. Quiero ver si te puedo sumar algo.

Cualquier cosa avisame.',
 1, true, NOW()),

('_default_', 'ES · Video',          'es', 'Campañas de video para {{domain}}',
 'Hola, te escribo de ADEQ Media. Tenemos campañas de video activas (in-stream y out-stream) que andan muy bien con sitios como {{domain}}.

CPMs decentes, fill alto, sin pisarte la UX. Si te interesa te paso un breakdown rápido.

Decime y te mando los detalles.',
 2, true, NOW()),

('_default_', 'ES · Header Bidding', 'es', 'Header bidding en {{domain}}',
 'Hola, soy de ADEQ. Vi que {{domain}} tiene buen tráfico y queria preguntarte si ya estás corriendo header bidding o si lo manejas todo via Google directo.

Tenemos un setup que suele levantar 15-30% del eCPM sin tocar la integración actual. Si te quedan minutos te muestro como.',
 3, true, NOW()),

-- ─── ENGLISH ─────────────────────────────────────────────────
('_default_', 'EN · Owner directo',  'en', 'Quick question about {{domain}}',
 'Hi! Quick one — are you the person handling ad sales / monetization at {{domain}}, or can you point me to whoever does?

I''m with ADEQ Media, we work with publishers on yield. Want to see if there''s a fit.

Let me know!',
 1, true, NOW()),

('_default_', 'EN · Video',          'en', 'Video demand for {{domain}}',
 'Hey, reaching out from ADEQ Media. We''ve got live video campaigns (in-stream + out-stream) running well on sites like {{domain}}.

Solid CPMs, high fill, no impact on your UX. Happy to share a quick breakdown if it''s useful.

Just say the word.',
 2, true, NOW()),

('_default_', 'EN · Header Bidding', 'en', 'Header bidding on {{domain}}',
 'Hi — ADEQ here. Saw {{domain}} pulls solid traffic, was curious whether you already have header bidding set up or if it''s mostly Google direct.

We have a stack that usually lifts eCPM 15-30% without touching the existing integration. Can walk you through it in 10 min if relevant.',
 3, true, NOW()),

-- ─── ITALIANO ────────────────────────────────────────────────
('_default_', 'IT · Owner diretto',  'it', 'Una domanda veloce su {{domain}}',
 'Ciao! Domanda veloce — sei tu che gestisci la monetizzazione pubblicitaria di {{domain}}, o puoi passarmi il contatto giusto?

Sono di ADEQ Media, lavoriamo con publisher sul rendimento dell''inventario. Vorrei capire se c''è spazio per collaborare.

Fammi sapere!',
 1, true, NOW()),

('_default_', 'IT · Video',          'it', 'Campagne video per {{domain}}',
 'Ciao, ti scrivo da ADEQ Media. Abbiamo campagne video attive (in-stream e out-stream) che funzionano bene su siti come {{domain}}.

CPM decenti, fill alto, senza impatto sulla UX. Se ti interessa ti mando un breakdown veloce.

Dimmi tu.',
 2, true, NOW()),

('_default_', 'IT · Header Bidding', 'it', 'Header bidding su {{domain}}',
 'Ciao, sono di ADEQ. Ho visto che {{domain}} ha buon traffico, ti volevo chiedere se hai già header bidding attivo o se vai principalmente con Google diretto.

Abbiamo un setup che alza l''eCPM del 15-30% senza toccare l''integrazione attuale. Se hai 10 minuti te lo spiego.',
 3, true, NOW()),

-- ─── PORTUGUÊS ───────────────────────────────────────────────
('_default_', 'PT · Owner direto',   'pt', 'Pergunta rápida sobre {{domain}}',
 'Oi! Pergunta rápida — você é quem gerencia a monetização de anúncios em {{domain}}, ou pode me passar o contato certo?

Sou da ADEQ Media, trabalhamos com publishers em yield. Quero ver se faz sentido.

Me avisa!',
 1, true, NOW()),

('_default_', 'PT · Video',          'pt', 'Campanhas de vídeo para {{domain}}',
 'Oi, te escrevo da ADEQ Media. Temos campanhas de vídeo ativas (in-stream e out-stream) rodando bem em sites como {{domain}}.

CPMs decentes, fill alto, sem prejudicar a UX. Se te interessar mando um breakdown rápido.

Me fala.',
 2, true, NOW()),

('_default_', 'PT · Header Bidding', 'pt', 'Header bidding em {{domain}}',
 'Oi, ADEQ aqui. Vi que {{domain}} tem bom tráfego, queria perguntar se você já tem header bidding rodando ou se vai principalmente via Google direto.

Temos um setup que costuma subir 15-30% do eCPM sem mexer na integração atual. Se tiver 10 min te mostro como.',
 3, true, NOW()),

-- ─── ARABIC ──────────────────────────────────────────────────
('_default_', 'AR · Owner directo',  'ar', 'سؤال سريع عن {{domain}}',
 'مرحبا! سؤال سريع — هل أنت المسؤول عن تحقيق الدخل من الإعلانات في {{domain}}، أو يمكنك توجيهي للشخص المناسب؟

أنا من ADEQ Media، نعمل مع الناشرين على تحسين العائد. أريد أن أرى إن كان هناك توافق.

أخبرني!',
 1, true, NOW()),

('_default_', 'AR · Video',          'ar', 'حملات فيديو لموقع {{domain}}',
 'مرحبا، أكتب لك من ADEQ Media. لدينا حملات فيديو نشطة (in-stream و out-stream) تعمل بشكل جيد مع مواقع مثل {{domain}}.

CPM جيدة، fill عالٍ، بدون تأثير على تجربة المستخدم. إذا كان يهمك أرسل لك تفاصيل سريعة.

أخبرني.',
 2, true, NOW()),

('_default_', 'AR · Header Bidding', 'ar', 'Header bidding في {{domain}}',
 'مرحبا، ADEQ هنا. لاحظت أن {{domain}} يحقق ترافيك جيد، أردت أن أسألك إذا كان لديك header bidding مفعل أم تعتمد بشكل أساسي على Google المباشر.

لدينا إعداد يرفع eCPM عادة بنسبة 15-30% دون المساس بالتكامل الحالي. إذا كان لديك 10 دقائق أوضح لك كيف.',
 3, true, NOW());
