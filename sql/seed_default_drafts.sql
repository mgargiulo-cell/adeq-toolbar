-- Seed de borradores default (3 × 5 idiomas = 15 templates) — Maxi 2026-07-08.
-- user_email='_default_' los marca como compartidos para todos los users.
-- MISMO contenido que auto-prospector/templates.js (baked del agente). Sincronizar ambos.
--
-- B1 (priority 1): identificar al encargado de los anuncios ("¿este es el correo de...?")
-- B2 (priority 2): campañas de video (in-stream/out-stream) + pedir contacto/whatsapp
-- B3 (priority 3): campañas de display y video + pedir contacto del encargado
-- {{domain}} se reemplaza al cargar. Priority 1 = se autocarga.

-- Limpieza previa (idempotente — corré este script las veces que quieras)
DELETE FROM toolbar_pitch_drafts WHERE user_email = '_default_';

INSERT INTO toolbar_pitch_drafts (user_email, name, language, subject, body, priority, is_default, updated_at) VALUES

-- ─── ESPAÑOL ─────────────────────────────────────────────────
('_default_', 'ES · Encargado anuncios', 'es', 'Publicidad en {{domain}} - ADEQ',
 'Hola!

Este es el correo de quien maneja los anuncios de la web de {{domain}}?

Soy del área de ventas de ADEQ Media, y me quería poner en contacto con la persona encargada para ofrecerles unas campañas que nos gustaría monetizar con ustedes.

Cualquier cosa avisame.',
 1, true, NOW()),

('_default_', 'ES · Video', 'es', 'Campañas de video para {{domain}} - ADEQ',
 'Hola, te escribo de ADEQ Media. Tenemos campañas de video activas (in-stream y out-stream) que están funcionando muy bien en websites como el tuyo.

¿Me podrían pasar el whatsapp o contacto de la persona que se encarga de manejar las implementaciones o monetización?

Muchas gracias, espero el dato para escribirles.

Saludos.',
 2, true, NOW()),

('_default_', 'ES · Display y Video', 'es', 'Campañas de display y video - ADEQ',
 'Buen día.

¿Me podrían pasar el contacto del encargado del website?

Es para poder conversar sobre unas campañas de display y video, que nos interesaría poder incluirlas en su sitio web.

Gracias',
 3, true, NOW()),

-- ─── ENGLISH ─────────────────────────────────────────────────
('_default_', 'EN · Ad manager', 'en', 'Advertising on {{domain}} - ADEQ',
 'Hi!

Is this the email of the person who handles advertising on {{domain}}?

I''m from the sales team at ADEQ Media, and I wanted to get in touch with the person in charge to offer you some campaigns we''d love to monetize with you.

Let me know.',
 1, true, NOW()),

('_default_', 'EN · Video', 'en', 'Active video campaigns for {{domain}} - ADEQ',
 'Hi, I''m reaching out from ADEQ Media. We have active video campaigns (in-stream and out-stream) that are performing really well on sites like yours.

Could you pass me the WhatsApp or contact of the person who handles implementations or monetization?

Thanks a lot, I''ll wait for the details to get in touch.

Best.',
 2, true, NOW()),

('_default_', 'EN · Display & Video', 'en', 'Display and video campaigns - ADEQ',
 'Good morning.

Could you pass me the contact of the person in charge of the website?

It''s to discuss some display and video campaigns that we''d be interested in including on your site.

Thanks',
 3, true, NOW()),

-- ─── PORTUGUÊS ───────────────────────────────────────────────
('_default_', 'PT · Responsável anúncios', 'pt', 'Publicidade em {{domain}} - ADEQ',
 'Olá!

Este é o email de quem cuida da publicidade do site {{domain}}?

Sou da área de vendas da ADEQ Media, e queria entrar em contato com a pessoa responsável para oferecer algumas campanhas que gostaríamos de monetizar com vocês.

Qualquer coisa, me avisa.',
 1, true, NOW()),

('_default_', 'PT · Video', 'pt', 'Campanhas de vídeo para {{domain}} - ADEQ',
 'Olá, escrevo da ADEQ Media. Temos campanhas de vídeo ativas (in-stream e out-stream) que estão funcionando muito bem em sites como o seu.

Poderiam me passar o WhatsApp ou contato da pessoa que cuida das implementações ou monetização?

Muito obrigado, aguardo o dado para entrar em contato.

Abraços.',
 2, true, NOW()),

('_default_', 'PT · Display e Video', 'pt', 'Campanhas de display e vídeo - ADEQ',
 'Bom dia.

Poderiam me passar o contato do responsável pelo site?

É para conversar sobre algumas campanhas de display e vídeo que teríamos interesse em incluir no seu site.

Obrigado',
 3, true, NOW()),

-- ─── ITALIANO ────────────────────────────────────────────────
('_default_', 'IT · Responsabile ads', 'it', 'Pubblicità su {{domain}} - ADEQ',
 'Ciao!

È questa l''email di chi gestisce la pubblicità del sito {{domain}}?

Sono dell''area vendite di ADEQ Media, e volevo mettermi in contatto con la persona responsabile per proporvi alcune campagne che ci piacerebbe monetizzare con voi.

Fammi sapere.',
 1, true, NOW()),

('_default_', 'IT · Video', 'it', 'Campagne video per {{domain}} - ADEQ',
 'Ciao, ti scrivo da ADEQ Media. Abbiamo campagne video attive (in-stream e out-stream) che stanno funzionando molto bene su siti come il tuo.

Potresti passarmi il WhatsApp o il contatto della persona che si occupa delle implementazioni o della monetizzazione?

Grazie mille, aspetto il contatto per scrivervi.

Saluti.',
 2, true, NOW()),

('_default_', 'IT · Display e Video', 'it', 'Campagne display e video - ADEQ',
 'Buongiorno.

Potreste passarmi il contatto del responsabile del sito?

È per parlare di alcune campagne display e video che ci interesserebbe includere sul vostro sito.

Grazie',
 3, true, NOW()),

-- ─── ARABIC ──────────────────────────────────────────────────
('_default_', 'AR · Ad manager', 'ar', 'الإعلانات على {{domain}} - ADEQ',
 'مرحباً!

هل هذا هو بريد الشخص المسؤول عن الإعلانات في موقع {{domain}}؟

أنا من قسم المبيعات في ADEQ Media، وأردت التواصل مع الشخص المسؤول لأعرض عليكم بعض الحملات التي يسعدنا تحقيق الدخل منها معكم.

في انتظار ردك.',
 1, true, NOW()),

('_default_', 'AR · Video', 'ar', 'حملات فيديو لموقع {{domain}} - ADEQ',
 'مرحباً، أكتب إليك من ADEQ Media. لدينا حملات فيديو نشطة (in-stream و out-stream) تحقق نتائج ممتازة على مواقع مثل موقعك.

هل يمكنكم تزويدي برقم واتساب أو بيانات التواصل مع الشخص المسؤول عن التنفيذ أو تحقيق الدخل؟

شكراً جزيلاً، بانتظار المعلومات للتواصل معكم.

مع التحية.',
 2, true, NOW()),

('_default_', 'AR · Display y Video', 'ar', 'حملات عرض وفيديو - ADEQ',
 'صباح الخير.

هل يمكنكم تزويدي ببيانات التواصل مع المسؤول عن الموقع؟

الأمر يتعلق بالتحدث حول بعض حملات العرض (display) والفيديو التي نود تضمينها في موقعكم.

شكراً',
 3, true, NOW());

-- ── 33/33/33 puro: que el agente mande SOLO los 3 borradores, sin el 20% Claude.
--    (Poné 20 de nuevo si querés reactivar la variedad con IA.)
UPDATE toolbar_config SET value = '0' WHERE key = 'agent_claude_percent';
INSERT INTO toolbar_config (key, value)
  SELECT 'agent_claude_percent', '0'
  WHERE NOT EXISTS (SELECT 1 FROM toolbar_config WHERE key = 'agent_claude_percent');
