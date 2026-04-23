-- Agregar columnas source + monday_item_id a toolbar_review_queue
-- source: "autopilot" | "csv" | "monday_refresh"
-- monday_item_id: si está seteado, el push hace UPDATE en vez de CREATE

ALTER TABLE toolbar_review_queue
  ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'autopilot',
  ADD COLUMN IF NOT EXISTS monday_item_id TEXT;

CREATE INDEX IF NOT EXISTS toolbar_review_queue_source_idx
  ON toolbar_review_queue (source);
