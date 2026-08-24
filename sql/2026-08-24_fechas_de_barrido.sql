-- ══════════════════════════════════════════════════════════════════════════════
-- FECHAS DEL BARRIDO AUTOMÁTICO — 2026-08-24
--
-- Por qué: dos de las cinco métricas que el user quiere revisar no se podían
-- responder POR DÍA. Cuando un barrido rechazaba un lead o le encontraba un email,
-- no dejaba NINGUNA marca de tiempo: `toolbar_review_queue` solo tiene `created_at`.
-- Se podía saber cuántos hay rechazados en total, nunca cuántos se rechazaron ayer.
--
-- Ojo: esto NO reconstruye el pasado. Empieza a medir desde que se aplique.
-- ══════════════════════════════════════════════════════════════════════════════

ALTER TABLE public.toolbar_review_queue
  -- Cuándo lo descartó el barrido automático (purge / urlpurge / sweep / envío).
  ADD COLUMN IF NOT EXISTS rejected_at    TIMESTAMPTZ,
  -- Cuándo se le encontró el PRIMER email a un lead que entró sin ninguno.
  -- Es la métrica de "rescate": mide el trabajo del barrido, no el del descubrimiento.
  ADD COLUMN IF NOT EXISTS email_found_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_rq_rejected_at
  ON public.toolbar_review_queue (rejected_at DESC) WHERE rejected_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_rq_email_found_at
  ON public.toolbar_review_queue (email_found_at DESC) WHERE email_found_at IS NOT NULL;


-- Chequeo: las dos tienen que aparecer.
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name = 'toolbar_review_queue'
   AND column_name IN ('rejected_at', 'email_found_at', 'email_intentos', 'email_ultimo_intento')
 ORDER BY column_name;
