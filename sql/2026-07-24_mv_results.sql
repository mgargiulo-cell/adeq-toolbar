-- ════════════════════════════════════════════════════════════════════════
-- RESULTADOS DE MILLIONVERIFIER (2026-07-24) — para medir el ROI de la verificación.
-- Cada verificación registra su resultado acá. Con esto se sabe si MV agarra basura REAL
-- (invalid/disposable = rebotes evitados = plata bien gastada) o si casi todo vuelve "ok"
-- (= la calidad de emails ya era buena y MV aporta poco).
-- El worker escribe con service_role. RLS ON sin policies = solo el worker.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS toolbar_mv_results (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  domain     text,
  email      text,
  result     text,        -- ok | catch_all | unknown | invalid | disposable | error
  quality    text,        -- good | risky | bad
  blocked    boolean,     -- true = NO se envió (invalid/disposable)
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_mv_results_created ON toolbar_mv_results (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mv_results_result  ON toolbar_mv_results (result);

ALTER TABLE toolbar_mv_results ENABLE ROW LEVEL SECURITY;
