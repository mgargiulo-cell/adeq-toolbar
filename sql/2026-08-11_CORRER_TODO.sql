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
-- ║ PARTE 2 · ELIMINADA                                                      ║
-- ╚══════════════════════════════════════════════════════════════════════════╝
-- Acá iba un copy nuevo para el agente que el user NO aprobó ("los mails tienen
-- que ser simples") y que además le llegó a un prospecto real con los
-- placeholders crudos, porque la extensión solo resuelve {{domain}}.
-- Se sacó de este archivo para que volver a correrlo no lo reinstale.
-- Los borradores los escribe el user; ver sql/2026-08-11_volver_borradores_viejos.sql.


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
