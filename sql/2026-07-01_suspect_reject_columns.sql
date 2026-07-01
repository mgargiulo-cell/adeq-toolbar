-- Maxi 2026-07-01: marca de "sospechosa de rechazo" en Prospects.
-- El worker corre 3×/semana (L/X/V), analiza los prospects pendientes contra los
-- comentarios de descarte aprendidos (toolbar_autopilot_feedback, por TIPO/contenido)
-- y marca suspect_reject=true en las que son del mismo tipo que las rechazadas.
-- La toolbar enciende una ⚠️ al lado de la X en esas cards.
-- Idempotente. Correr 1 vez en Supabase SQL Editor.

ALTER TABLE public.toolbar_review_queue
  ADD COLUMN IF NOT EXISTS suspect_reject boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS suspect_reason text;

-- Índice para que el worker filtre rápido las no-analizadas.
CREATE INDEX IF NOT EXISTS idx_review_queue_suspect
  ON public.toolbar_review_queue (status, suspect_reject)
  WHERE status = 'pending';

-- Verificación
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'toolbar_review_queue' AND column_name IN ('suspect_reject','suspect_reason');
