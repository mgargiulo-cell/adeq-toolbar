-- Maxi 2026-06-17: 3 emails futuros (FU2/FU3/FU4) en cascada.
-- Cada uno se encola con su propio scheduled_for (+11/+22/+33d) y un label
-- de sequence ("FU2", "FU3", "FU4"). El worker procesa cada uno
-- verificando si el ORIGINAL fue abierto; si sí → cancela los pendientes.

ALTER TABLE public.toolbar_reengagement_queue
  ADD COLUMN IF NOT EXISTS sequence TEXT DEFAULT 'FU2';

-- Index para querys del worker (status + scheduled_for + sequence).
CREATE INDEX IF NOT EXISTS idx_reengagement_pending
  ON public.toolbar_reengagement_queue (status, scheduled_for)
  WHERE status = 'pending';
