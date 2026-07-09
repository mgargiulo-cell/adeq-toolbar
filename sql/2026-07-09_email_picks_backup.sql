-- ════════════════════════════════════════════════════════════════════════
-- BACKUP de TODOS los emails detectados por el agente, por cada lead + cuál eligió.
-- Objetivo (pedido del user 2026-07-09): poder comparar en SQL "qué habría elegido
-- YO vs qué eligió el agente", marcar aciertos/errores y sacar estadísticas para
-- tomar decisiones sobre la lógica de selección.
--
-- El worker inserta 1 fila cada vez que elige email para un lead (fire-and-forget,
-- nunca frena el envío). `candidates` (jsonb) guarda TODOS los emails encontrados con
-- su source, score (rankEmail), tier (_pickTier) y si venía bounced.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS toolbar_email_picks (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  domain        TEXT,
  lead_id       TEXT,              -- id del review_queue (TEXT: sirve para bigint o uuid)
  mb_email      TEXT,              -- media buyer dueño del lead
  category      TEXT,
  chosen_email  TEXT,              -- el que el agente eligió (NULL si no eligió ninguno)
  chosen_source TEXT,              -- apollo | informer | scrape | generic | ...
  chosen_tier   INT,              -- 4 apollo/informer · 3 publicidad@ · 2 persona · 0 genérico
  n_candidates  INT,
  candidates    JSONB              -- [{email, source, score, tier, bounced}] ordenado best-first
);

CREATE INDEX IF NOT EXISTS toolbar_email_picks_created_idx ON toolbar_email_picks (created_at DESC);
CREATE INDEX IF NOT EXISTS toolbar_email_picks_domain_idx  ON toolbar_email_picks (domain);

-- RLS: lectura/escritura abierta a authenticated (no es info sensible; mismo patrón que geo_cache).
ALTER TABLE toolbar_email_picks ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "email_picks_select" ON toolbar_email_picks;
CREATE POLICY "email_picks_select" ON toolbar_email_picks
  FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "email_picks_insert" ON toolbar_email_picks;
CREATE POLICY "email_picks_insert" ON toolbar_email_picks
  FOR INSERT TO authenticated WITH CHECK (true);

-- ════════════════════════════════════════════════════════════════════════
-- CONSULTAS ÚTILES (correr cuando ya haya data)
-- ════════════════════════════════════════════════════════════════════════

-- 1) Últimos 50 leads: qué encontró vs qué eligió (candidatos expandidos como texto).
-- SELECT created_at, domain, chosen_email, chosen_source, chosen_tier,
--        jsonb_agg(c->>'email' ORDER BY (c->>'tier')::int DESC) AS todos_los_emails
-- FROM toolbar_email_picks, jsonb_array_elements(candidates) c
-- GROUP BY id ORDER BY created_at DESC LIMIT 50;

-- 2) Distribución del tier elegido (¿está eligiendo comercial/publicidad o genéricos?).
-- SELECT chosen_tier, chosen_source, COUNT(*)
-- FROM toolbar_email_picks GROUP BY chosen_tier, chosen_source ORDER BY 3 DESC;

-- 3) Casos donde HABÍA un candidato de tier más alto que el elegido (revisar la lógica).
-- SELECT domain, chosen_email, chosen_tier,
--        MAX((c->>'tier')::int) AS mejor_tier_disponible
-- FROM toolbar_email_picks, jsonb_array_elements(candidates) c
-- GROUP BY id HAVING MAX((c->>'tier')::int) > COALESCE(chosen_tier, -1)
-- ORDER BY created_at DESC;

-- 4) Leads con varios candidatos (los interesantes para que el MB marque acuerdo/desacuerdo).
-- SELECT created_at, domain, chosen_email, n_candidates, candidates
-- FROM toolbar_email_picks WHERE n_candidates >= 2 ORDER BY created_at DESC LIMIT 100;
