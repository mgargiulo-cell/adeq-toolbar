-- Migración 2026-05-12
-- 1. contact_phone en toolbar_review_queue (popup Apollo phone storage).
-- 2. geos_all top3 ISO codes para filtro amplio del agente.
-- 3. NOTIFY pgrst para que PostgREST recargue el schema cache sin esperar 5-10min.

ALTER TABLE toolbar_review_queue
  ADD COLUMN IF NOT EXISTS contact_phone text,
  ADD COLUMN IF NOT EXISTS geos_all text[];

CREATE INDEX IF NOT EXISTS idx_review_queue_geos_all
  ON toolbar_review_queue USING GIN (geos_all);

NOTIFY pgrst, 'reload schema';
