-- ══════════════════════════════════════════════════════════════════════════════
-- CORRER TODO — 2026-08-11 (commit f0a9e8d)
-- Pegá este archivo entero en Supabase y ejecutalo de una. Está en orden.
-- No borra prospects. Lo único que borra son los 15 drafts default viejos
-- (user_email='_default_'), que se reemplazan por el copy nuevo más abajo.
--
-- PARTE 1 · Tabla de salud + vista de chequeo + columnas de intentos de email
-- PARTE 2 · Copy nuevo del agente (15 templates, 5 idiomas)
-- PARTE 3 · Diagnóstico (solo SELECT — pegame las salidas)
-- ══════════════════════════════════════════════════════════════════════════════


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ PARTE 1 · SALUD Y ALERTAS                                                ║
-- ╚══════════════════════════════════════════════════════════════════════════╝
-- ══════════════════════════════════════════════════════════════════════════════


-- ── 1. LATIDO POR TRABAJO ────────────────────────────────────────────────────
-- Cada job estampa acá cuándo corrió y si le fue bien. El vigilante compara
-- contra `esperado_cada_min` y avisa cuando algo lleva demasiado sin correr.
CREATE TABLE IF NOT EXISTS public.toolbar_health (
  job                 TEXT PRIMARY KEY,
  last_run_at         TIMESTAMPTZ,
  last_ok_at          TIMESTAMPTZ,
  last_status         TEXT,                       -- ok | fail | skipped
  last_detail         TEXT,
  fails_consecutivos  INT           NOT NULL DEFAULT 0,
  esperado_cada_min   INT,                        -- cadencia esperada; NULL = no se vigila el latido
  real_ultimo         NUMERIC,                    -- rendimiento medido
  esperado_ultimo     NUMERIC,                    -- rendimiento deseado
  updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

ALTER TABLE public.toolbar_health ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "health_service_all" ON public.toolbar_health;
CREATE POLICY "health_service_all" ON public.toolbar_health
  FOR ALL USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');

-- Los MB pueden LEER la salud (para el panel), no escribirla.
DROP POLICY IF EXISTS "health_read_authenticated" ON public.toolbar_health;
CREATE POLICY "health_read_authenticated" ON public.toolbar_health
  FOR SELECT USING (auth.role() IN ('authenticated', 'service_role'));


-- ── 2. LA CONSULTA ÚNICA DE SALUD ────────────────────────────────────────────
-- Esta es la vista que hay que mirar al empezar cualquier sesión.
-- Reemplaza a los seis SQL sueltos que veníamos corriendo a mano.
CREATE OR REPLACE VIEW public.toolbar_health_check AS
SELECT
  job,
  CASE
    WHEN last_ok_at IS NULL                                              THEN '🔴 NUNCA CORRIÓ'
    WHEN esperado_cada_min IS NOT NULL
     AND NOW() - last_ok_at > (esperado_cada_min * 3) * INTERVAL '1 min' THEN '🔴 ATRASADO'
    WHEN esperado_cada_min IS NOT NULL
     AND NOW() - last_ok_at > (esperado_cada_min * 1.5) * INTERVAL '1 min' THEN '🟡 demorado'
    WHEN fails_consecutivos >= 3                                          THEN '🔴 FALLANDO'
    WHEN esperado_ultimo IS NOT NULL AND real_ultimo IS NOT NULL
     AND real_ultimo < esperado_ultimo * 0.5                              THEN '🟡 rinde poco'
    ELSE '🟢 ok'
  END                                                        AS estado,
  ROUND(EXTRACT(EPOCH FROM (NOW() - last_ok_at)) / 60)::int  AS hace_min,
  esperado_cada_min                                          AS esperado_cada,
  real_ultimo, esperado_ultimo, fails_consecutivos,
  last_status, last_detail, last_run_at
FROM public.toolbar_health;

COMMENT ON VIEW public.toolbar_health_check IS
  'Estado de salud de todos los jobs. SELECT * FROM toolbar_health_check ORDER BY estado; — Maxi 2026-08-11.';


-- ── 3. REGISTRO DE BÚSQUEDA DE EMAIL ─────────────────────────────────────────
-- Hoy no existe forma de distinguir "ya lo resolví" de "busqué y no encontré".
-- Sin esto no se puede reintentar de forma inteligente: el cursor pasa y no
-- vuelve nunca. Es la causa directa de los ~310 leads sin email estancados.
ALTER TABLE public.toolbar_review_queue
  ADD COLUMN IF NOT EXISTS email_intentos      INT,
  ADD COLUMN IF NOT EXISTS email_ultimo_intento TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS email_ultimo_motivo  TEXT;

CREATE INDEX IF NOT EXISTS idx_rq_sin_email_reintento
  ON public.toolbar_review_queue (email_ultimo_intento NULLS FIRST)
  WHERE status = 'pending';


-- ── 4. CHEQUEO ───────────────────────────────────────────────────────────────
-- Correr esto cuando el worker lleve unos minutos con el código nuevo:
--   SELECT * FROM toolbar_health_check ORDER BY estado, job;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ PARTE 2 · COPY NUEVO DEL AGENTE                                          ║
-- ║ ⚠️ SIN ESTO EL AGENTE SIGUE MANDANDO EL TEXTO VIEJO, aunque deployes.    ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

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


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ PARTE 3 · DIAGNÓSTICO — solo SELECT, pegame las salidas                  ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- 3.1 · LAS CLAVES QUE DECIDEN SI SALEN 8, 16 O 20 ENVÍOS
-- Si `agent_per_cycle_limit` dice 2, ahí está el motivo de los 8/día.
SELECT key, value FROM toolbar_config
 WHERE key IN ('agent_per_cycle_limit','per_cycle_limit',
               'agent_active_hours_end','active_hours_end',
               'agent_active_hours_start','active_hours_start',
               'agent_max_per_day','agent_enabled_users','agent_whitelist',
               'agent_claude_percent','agent_slots_done')
 ORDER BY key;

-- 3.2 · DE QUÉ ESTÁ HECHO EL POOL, POR FUENTE
SELECT coalesce(source,'(sin fuente)') AS fuente,
       count(*) AS total,
       count(*) FILTER (WHERE status='pending') AS pendientes,
       count(*) FILTER (WHERE status='pending'
                          AND jsonb_array_length(coalesce(emails,'[]'::jsonb))>0
                          AND coalesce(suspect_reject,false)=false) AS listos_para_enviar,
       count(*) FILTER (WHERE status='pending'
                          AND jsonb_array_length(coalesce(emails,'[]'::jsonb))=0) AS sin_email
  FROM toolbar_review_queue
 GROUP BY 1 ORDER BY listos_para_enviar DESC NULLS LAST;

-- 3.3 · ¿SE ESTÁ ALIMENTANDO EL TERMÓMETRO?
SELECT 'respuestas registradas' AS que, count(*)::text AS cuanto FROM toolbar_response_tracking
UNION ALL SELECT 'respuestas REALES (no OOO)', count(*)::text FROM toolbar_response_tracking WHERE response_type='real'
UNION ALL SELECT 'rechazos X del media buyer', count(*)::text FROM toolbar_autopilot_feedback WHERE action='disliked'
UNION ALL SELECT 'leads congelados esperando',  count(*)::text FROM toolbar_frozen_leads
UNION ALL SELECT 'atascados en next_day',       count(*)::text FROM toolbar_csv_queue WHERE status='next_day';

-- 3.4 · ENVÍOS POR MEDIA BUYER, ÚLTIMOS 7 DÍAS (el número que hay que mover)
SELECT created_at::date AS dia, user_email,
       count(*) FILTER (WHERE action='sent') AS enviados
  FROM toolbar_agent_actions
 WHERE created_at >= CURRENT_DATE - 7 AND action='sent'
 GROUP BY 1,2 ORDER BY 1 DESC, 2;
