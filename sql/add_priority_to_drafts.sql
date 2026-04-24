-- Agregar campo priority (1-5) a toolbar_pitch_drafts
-- 1 = se muestra por default cuando matchea el idioma de GEO
-- La bandera al lado del pitch rota templates del mismo idioma ordenados por priority asc

ALTER TABLE toolbar_pitch_drafts
  ADD COLUMN IF NOT EXISTS priority SMALLINT DEFAULT 3
    CHECK (priority BETWEEN 1 AND 5);

CREATE INDEX IF NOT EXISTS toolbar_pitch_drafts_lang_prio_idx
  ON toolbar_pitch_drafts (language, priority);
