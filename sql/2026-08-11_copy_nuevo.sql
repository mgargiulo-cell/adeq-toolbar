-- ==============================================================================
-- COPY NUEVO DEL AGENTE - 2026-08-11
--
-- IMPORTANTE: los drafts de esta tabla PISAN a los templates del codigo.
-- pickAnyTemplate usa toolbar_pitch_drafts si existen; los baked de
-- auto-prospector/templates.js son solo fallback. O sea: sin correr este archivo,
-- el agente sigue mandando el copy VIEJO aunque el codigo este deployado.
--
-- Que cambia y por que (auditoria "por que nadie contesta", 2026-08-11):
--   * Los 3 textos viejos PEDIAN UN FAVOR y no ofrecian nada. Dos de los tres
--     pedian "pasame el contacto del encargado" - y se los mandabamos justo al
--     encargado, porque para eso esta todo el ranking de emails.
--   * No decian que es ADEQ Media: desde el otro lado era indistinguible de un
--     arbitrajista, y un publisher no deja entrar a nadie a su ads.txt a ciegas.
--   * Personalizacion real: el dominio, y solo en 1 de 3. Ahora saludan por nombre
--     ({{saludo}}) y nombran una senal concreta del sitio ({{senal}}: su ad stack
--     o su trafico). Si no tenemos el dato, la frase desaparece sola.
--   * Piden UNA cosa chica y concreta, y ofrecen verificacion via sellers.json.
--
-- Generado desde auto-prospector/templates.js para que las dos fuentes no se
-- desincronicen. Es idempotente: se puede correr las veces que haga falta.
-- ==============================================================================

DELETE FROM toolbar_pitch_drafts WHERE user_email = '_default_';

INSERT INTO toolbar_pitch_drafts (user_email, name, language, subject, body, priority, is_default, updated_at) VALUES
('_default_', 'ES · Partner directo', 'es', 'Inventario de {{domain}}',
 '{{saludo}}

Soy {{sender_name}}, de ADEQ Media. Compramos inventario de display y video para anunciantes, y trabajamos directo con publishers.

{{senal}}Nos interesa sumar a {{domain}} como partner.

Podés verificarnos en nuestro sellers.json antes de cualquier cosa. Si te sirve, te paso los formatos que estamos comprando y los volúmenes para tu mercado, y vemos si tiene sentido.

¿Te va bien que te escriba con eso?

{{sender_name}}', 1, true, NOW()),
('_default_', 'ES · Presupuesto video', 'es', 'Video para {{domain}}',
 '{{saludo}}

Te escribo de ADEQ Media. Tenemos presupuesto de video (in-stream y out-stream) sin colocar para {{geo}} este trimestre.

{{senal}}Por el perfil de {{domain}}, entra en lo que estamos buscando.

No te pido que hagas nada todavía: si me decís que sí, te mando los formatos y el volumen estimado y lo mirás con calma.

{{sender_name}}
ADEQ Media', 2, true, NOW()),
('_default_', 'ES · Prueba 15 min', 'es', '{{domain}} — 15 minutos',
 '{{saludo}}

{{sender_name}} de ADEQ Media. Trabajamos con publishers de {{geo}} conectando su inventario con anunciantes, en display y video.

{{senal}}Me gustaría proponerte una prueba chica en {{domain}}: un formato, un mes, y medimos.

Si te interesa, contestame y coordinamos 15 minutos esta semana o la que viene.

Gracias,
{{sender_name}}', 3, true, NOW()),
('_default_', 'EN · Partner directo', 'en', '{{domain}} inventory',
 '{{saludo}}

I''m {{sender_name}}, from ADEQ Media. We buy display and video inventory for advertisers and work directly with publishers.

{{senal}}We''d like to add {{domain}} as a partner.

You can verify us in our sellers.json before anything else. If it''s useful, I''ll send over the formats we''re buying and the volumes for your market, and we can see if it makes sense.

Would that be alright?

{{sender_name}}', 1, true, NOW()),
('_default_', 'EN · Presupuesto video', 'en', 'Video budget for {{domain}}',
 '{{saludo}}

I''m writing from ADEQ Media. We have unplaced video budget (in-stream and out-stream) for {{geo}} this quarter.

{{senal}}Given the profile of {{domain}}, it fits what we''re looking for.

I''m not asking you to do anything yet: if you say yes, I''ll send the formats and estimated volume and you can look at it whenever.

{{sender_name}}
ADEQ Media', 2, true, NOW()),
('_default_', 'EN · Prueba 15 min', 'en', '{{domain}} — 15 minutes',
 '{{saludo}}

{{sender_name}} here, from ADEQ Media. We work with publishers in {{geo}}, connecting their inventory to advertisers across display and video.

{{senal}}I''d like to propose a small test on {{domain}}: one format, one month, and we measure.

If that''s interesting, reply and we''ll find 15 minutes this week or next.

Thanks,
{{sender_name}}', 3, true, NOW()),
('_default_', 'PT · Partner directo', 'pt', 'Inventário de {{domain}}',
 '{{saludo}}

Sou {{sender_name}}, da ADEQ Media. Compramos inventário de display e vídeo para anunciantes e trabalhamos direto com publishers.

{{senal}}Temos interesse em somar {{domain}} como parceiro.

Você pode nos verificar no nosso sellers.json antes de qualquer coisa. Se for útil, mando os formatos que estamos comprando e os volumes para o seu mercado, e vemos se faz sentido.

Posso te escrever com isso?

{{sender_name}}', 1, true, NOW()),
('_default_', 'PT · Presupuesto video', 'pt', 'Vídeo para {{domain}}',
 '{{saludo}}

Escrevo da ADEQ Media. Temos verba de vídeo (in-stream e out-stream) sem alocar para {{geo}} neste trimestre.

{{senal}}Pelo perfil de {{domain}}, entra no que estamos buscando.

Não peço nada ainda: se você disser que sim, mando os formatos e o volume estimado e você olha com calma.

{{sender_name}}
ADEQ Media', 2, true, NOW()),
('_default_', 'PT · Prueba 15 min', 'pt', '{{domain}} — 15 minutos',
 '{{saludo}}

{{sender_name}}, da ADEQ Media. Trabalhamos com publishers de {{geo}} conectando o inventário deles a anunciantes, em display e vídeo.

{{senal}}Gostaria de propor um teste pequeno em {{domain}}: um formato, um mês, e medimos.

Se tiver interesse, responda e marcamos 15 minutos esta semana ou na próxima.

Obrigado,
{{sender_name}}', 3, true, NOW()),
('_default_', 'IT · Partner directo', 'it', 'Inventory di {{domain}}',
 '{{saludo}}

Sono {{sender_name}}, di ADEQ Media. Compriamo inventory display e video per gli inserzionisti e lavoriamo direttamente con i publisher.

{{senal}}Ci interesserebbe aggiungere {{domain}} tra i nostri partner.

Puoi verificarci nel nostro sellers.json prima di qualsiasi cosa. Se ti è utile, ti mando i formati che stiamo comprando e i volumi per il tuo mercato, e vediamo se ha senso.

Ti va se ti scrivo con quei dati?

{{sender_name}}', 1, true, NOW()),
('_default_', 'IT · Presupuesto video', 'it', 'Budget video per {{domain}}',
 '{{saludo}}

Ti scrivo da ADEQ Media. Abbiamo budget video (in-stream e out-stream) non ancora allocato per {{geo}} in questo trimestre.

{{senal}}Per il profilo di {{domain}}, rientra in quello che stiamo cercando.

Non ti chiedo niente per ora: se mi dici di sì, ti mando i formati e il volume stimato e lo guardi con calma.

{{sender_name}}
ADEQ Media', 2, true, NOW()),
('_default_', 'IT · Prueba 15 min', 'it', '{{domain}} — 15 minuti',
 '{{saludo}}

{{sender_name}} di ADEQ Media. Lavoriamo con publisher in {{geo}}, collegando il loro inventory agli inserzionisti, su display e video.

{{senal}}Vorrei proporti un test piccolo su {{domain}}: un formato, un mese, e misuriamo.

Se ti interessa, rispondimi e troviamo 15 minuti questa settimana o la prossima.

Grazie,
{{sender_name}}', 3, true, NOW()),
('_default_', 'AR · Partner directo', 'ar', 'مساحات {{domain}} الإعلانية',
 '{{saludo}}

أنا {{sender_name}} من ADEQ Media. نشتري مساحات إعلانية للعرض والفيديو لصالح المعلنين، ونعمل مباشرة مع الناشرين.

{{senal}}يهمنا إضافة {{domain}} كشريك.

يمكنك التحقق منا عبر ملف sellers.json الخاص بنا قبل أي شيء. وإذا كان مفيداً، أرسل لك الصيغ التي نشتريها والأحجام المتاحة لسوقك، ونرى إن كان الأمر مناسباً.

هل يناسبك أن أرسل لك هذه التفاصيل؟

{{sender_name}}', 1, true, NOW()),
('_default_', 'AR · Presupuesto video', 'ar', 'ميزانية فيديو لموقع {{domain}}',
 '{{saludo}}

أكتب إليك من ADEQ Media. لدينا ميزانية فيديو (in-stream و out-stream) غير مخصصة بعد لمنطقة {{geo}} هذا الربع.

{{senal}}بالنظر إلى طبيعة {{domain}}، فهو ضمن ما نبحث عنه.

لا أطلب منك شيئاً الآن: إن وافقت، أرسل لك الصيغ والحجم التقديري وتطّلع عليها براحتك.

{{sender_name}}
ADEQ Media', 2, true, NOW()),
('_default_', 'AR · Prueba 15 min', 'ar', '{{domain}} — 15 دقيقة',
 '{{saludo}}

{{sender_name}} من ADEQ Media. نعمل مع ناشرين في {{geo}} ونربط مساحاتهم الإعلانية بالمعلنين، في العرض والفيديو.

{{senal}}أود أن أقترح تجربة صغيرة على {{domain}}: صيغة واحدة، لمدة شهر، ونقيس النتائج.

إن كان الأمر يهمك، ردّ عليّ ونحدد 15 دقيقة هذا الأسبوع أو الذي يليه.

شكراً،
{{sender_name}}', 3, true, NOW());

-- El agente manda 100% templates (decision del user: "el agente debe enviar 100%
-- templates rotando"). Se deja explicito para que no quede en el default de 20%.
INSERT INTO toolbar_config (key, value) VALUES ('agent_claude_percent', '0')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Chequeo
SELECT language, priority, subject, left(body, 60) AS arranque
  FROM toolbar_pitch_drafts WHERE user_email = '_default_'
 ORDER BY language, priority;
