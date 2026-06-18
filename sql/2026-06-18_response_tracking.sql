-- Maxi 2026-06-18: Tracking de respuestas reales por source/geo/category.
-- Objetivo: medir qué TIPO de email convierte mejor (apollo / informer / scrape
-- / Facebook / YouTube / Twitter / generic) y ajustar el ranking dinámico.
--
-- IMPORTANTE: solo cuenta como respuesta REAL cuando hay reply humano.
-- OOO, vacation, auto-reply, bounce → NO cuentan.

CREATE TABLE IF NOT EXISTS public.toolbar_response_tracking (
  id              BIGSERIAL PRIMARY KEY,
  agent_action_id BIGINT       REFERENCES public.toolbar_agent_actions(id) ON DELETE CASCADE,
  mb_email        TEXT         NOT NULL,
  domain          TEXT         NOT NULL,
  email_sent_to   TEXT         NOT NULL,
  source          TEXT         NOT NULL,                    -- "apollo" | "informer" | "scrape" | "generic" | "Facebook" | "YouTube" | "Twitter"
  geo             TEXT,                                     -- ISO-2 o NAME del país
  category        TEXT,
  sent_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  responded_at    TIMESTAMPTZ,                              -- NULL = no contestó (todavía)
  response_type   TEXT,                                     -- "real" | "ooo" | "bounce" | NULL
  created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Index para agregados rápidos por source/geo/categoria
CREATE INDEX IF NOT EXISTS idx_response_tracking_source_geo
  ON public.toolbar_response_tracking (source, geo, category, sent_at DESC);

-- Index para scanner de respuestas (buscar envíos sin respuesta de últimos 14d)
CREATE INDEX IF NOT EXISTS idx_response_tracking_pending
  ON public.toolbar_response_tracking (mb_email, sent_at DESC)
  WHERE responded_at IS NULL;

-- Unique guard: no duplicar tracking por agent_action_id
CREATE UNIQUE INDEX IF NOT EXISTS uq_response_tracking_action
  ON public.toolbar_response_tracking (agent_action_id)
  WHERE agent_action_id IS NOT NULL;

-- RLS — solo el MB dueño puede leer sus propias respuestas.
ALTER TABLE public.toolbar_response_tracking ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "response_tracking_select_own" ON public.toolbar_response_tracking;
CREATE POLICY "response_tracking_select_own"
  ON public.toolbar_response_tracking FOR SELECT
  USING (auth.jwt() ->> 'email' = mb_email OR auth.role() = 'service_role');

DROP POLICY IF EXISTS "response_tracking_insert_any" ON public.toolbar_response_tracking;
CREATE POLICY "response_tracking_insert_any"
  ON public.toolbar_response_tracking FOR INSERT
  WITH CHECK (auth.role() = 'service_role' OR auth.jwt() ->> 'email' = mb_email);

DROP POLICY IF EXISTS "response_tracking_update_own" ON public.toolbar_response_tracking;
CREATE POLICY "response_tracking_update_own"
  ON public.toolbar_response_tracking FOR UPDATE
  USING (auth.jwt() ->> 'email' = mb_email OR auth.role() = 'service_role');

COMMENT ON TABLE public.toolbar_response_tracking IS
  'Tracking de respuestas REALES (no OOO/bounce) por source/geo/category. Maxi 2026-06-18.';
