-- Maxi 2026-06-17: X-learn — agente aprende qué tipos de webs rechaza el MB.
-- Cuando el MB pega X 3+ veces en leads con la MISMA firma (categoría +
-- traffic_bucket + geo), la toolbar filtra futuros leads similares.
--
-- Esta columna estaba implícita en `details` antes — ahora la materializamos
-- para indexar y agregar más rápido.

ALTER TABLE public.toolbar_autopilot_feedback
  ADD COLUMN IF NOT EXISTS traffic_bucket TEXT;

-- Index opcional para queries del fetchRejectedSignatures.
CREATE INDEX IF NOT EXISTS idx_autopilot_feedback_signature
  ON public.toolbar_autopilot_feedback (user_email, action, created_at DESC)
  WHERE action = 'disliked';
