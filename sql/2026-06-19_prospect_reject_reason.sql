-- ════════════════════════════════════════════════════════════════
-- MOTIVO DE RECHAZO en feedback de prospects — Maxi 2026-06-19
-- ────────────────────────────────────────────────────────────────
-- Agrega la columna `reason` a toolbar_autopilot_feedback para guardar el
-- comentario del MB cuando rechaza un prospect ("¿por qué lo rechazás?").
-- El worker sintetiza esos motivos (por CONTENIDO, ignorando GEO) en reglas de
-- basura y descarta futuros similares → Prospects 100% limpio.
-- Idempotente. Correr 1 vez en Supabase SQL Editor.
-- ════════════════════════════════════════════════════════════════

ALTER TABLE public.toolbar_autopilot_feedback
  ADD COLUMN IF NOT EXISTS reason text;

-- Verificación
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'toolbar_autopilot_feedback' AND column_name = 'reason';
