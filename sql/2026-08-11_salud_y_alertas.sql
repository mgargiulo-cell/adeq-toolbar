-- ══════════════════════════════════════════════════════════════════════════════
-- SALUD Y ALERTAS — 2026-08-11
-- REGLA DE ORO: nada se entrega sin su detector. Se alerta sobre lo que NO pasó,
-- no solo sobre lo que tiró error. Los 5 problemas de la auditoría de hoy eran
-- todos silenciosos: ninguno lanzó una excepción.
--
-- Corré este archivo entero una sola vez. No borra nada.
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
