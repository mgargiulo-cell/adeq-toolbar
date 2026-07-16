-- ════════════════════════════════════════════════════════════════════════
-- AutoGoogle YIELD POR KEYWORD (2026-07-16)
-- Trackea qué frases del cascade traen más dominios FRESCOS → el slot de AutoGoogle
-- sesga la selección hacia las que rinden (65% top-yield + 35% exploración random).
-- El worker escribe con service_role (bypasea RLS). RLS ON sin policies = solo el worker accede.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS toolbar_keyword_yield (
  phrase     text PRIMARY KEY,
  searches   integer NOT NULL DEFAULT 0,   -- veces que se buscó esta frase
  found      integer NOT NULL DEFAULT 0,   -- dominios totales que devolvió Google
  fresh      integer NOT NULL DEFAULT 0,   -- de esos, cuántos eran NUEVOS (no conocidos)
  updated_at timestamptz DEFAULT now()
);

-- Índice para el ORDER BY fresh DESC del slot (top-yield).
CREATE INDEX IF NOT EXISTS idx_keyword_yield_fresh ON toolbar_keyword_yield (fresh DESC);

-- RLS ON sin policies → solo service_role (worker) lee/escribe. Los stats no son sensibles,
-- pero así no quedan expuestos al anon key.
ALTER TABLE toolbar_keyword_yield ENABLE ROW LEVEL SECURITY;

-- RPC atómico para incrementar (upsert con suma). El worker lo llama 1× por keyword por slot.
CREATE OR REPLACE FUNCTION bump_keyword_yield(p_phrase text, p_searches int, p_found int, p_fresh int)
RETURNS void
LANGUAGE sql
AS $$
  INSERT INTO toolbar_keyword_yield (phrase, searches, found, fresh, updated_at)
  VALUES (p_phrase, p_searches, p_found, p_fresh, now())
  ON CONFLICT (phrase) DO UPDATE SET
    searches   = toolbar_keyword_yield.searches + EXCLUDED.searches,
    found      = toolbar_keyword_yield.found    + EXCLUDED.found,
    fresh      = toolbar_keyword_yield.fresh    + EXCLUDED.fresh,
    updated_at = now();
$$;
