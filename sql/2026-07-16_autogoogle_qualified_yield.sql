-- ════════════════════════════════════════════════════════════════════════
-- AutoGoogle YIELD por CALIFICADOS (2026-07-16, refinamiento)
-- Antes el yield medía "dominios frescos". Ahora también "qualified" = los que realmente
-- llegaron a Prospects (pasaron el piso 350K + detector publisher). La selección prioriza
-- por qualified → AutoGoogle busca con las frases que traen publishers GRANDES, no solo nuevos.
-- Atribución vía tabla aparte (NO toca el pipeline core): al inyectar se guarda domain→phrase;
-- en cada slot se reconcilia (¿el dominio llegó a review_queue con source=autogoogle? → bump qualified).
-- ════════════════════════════════════════════════════════════════════════

-- 1) Columna qualified en el yield.
ALTER TABLE toolbar_keyword_yield ADD COLUMN IF NOT EXISTS qualified integer NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_keyword_yield_qualified ON toolbar_keyword_yield (qualified DESC);

-- 2) Tabla de atribución domain→phrase (pendientes de reconciliar). RLS ON = solo worker.
CREATE TABLE IF NOT EXISTS toolbar_autogoogle_attribution (
  domain      text PRIMARY KEY,
  phrase      text NOT NULL,
  injected_at timestamptz DEFAULT now()
);
ALTER TABLE toolbar_autogoogle_attribution ENABLE ROW LEVEL SECURITY;

-- 3) RPC extendido con p_qualified (default 0 → los calls viejos siguen andando).
CREATE OR REPLACE FUNCTION bump_keyword_yield(p_phrase text, p_searches int, p_found int, p_fresh int, p_qualified int DEFAULT 0)
RETURNS void
LANGUAGE sql
AS $$
  INSERT INTO toolbar_keyword_yield (phrase, searches, found, fresh, qualified, updated_at)
  VALUES (p_phrase, p_searches, p_found, p_fresh, p_qualified, now())
  ON CONFLICT (phrase) DO UPDATE SET
    searches   = toolbar_keyword_yield.searches  + EXCLUDED.searches,
    found      = toolbar_keyword_yield.found     + EXCLUDED.found,
    fresh      = toolbar_keyword_yield.fresh     + EXCLUDED.fresh,
    qualified  = toolbar_keyword_yield.qualified + EXCLUDED.qualified,
    updated_at = now();
$$;
