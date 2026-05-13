-- ============================================================
-- toolbar_review_queue.email_sources — tracking del origen de cada email
-- ============================================================
-- Política user 2026-05-13: el agent debe priorizar por SOURCE estrictamente,
-- no solo por rank score:
--   1. apollo  (verified, decision-maker)
--   2. informer (website informer scrape)
--   3. scrape  (page scraping, persona)
--   4. generic (page scraping, rol info@/contact@)
-- Antes el rank score podía dejar que un generic con buen formato le gane a un
-- Apollo personal en casos edge. Con esta columna el orden es deterministico.
-- ============================================================

alter table public.toolbar_review_queue
  add column if not exists email_sources jsonb not null default '{}'::jsonb;

-- Índice no necesario (column es accessory, no se filtra por ella)
comment on column public.toolbar_review_queue.email_sources is
  'Map {email: source} donde source in (apollo|informer|scrape|generic). Usado por agent para pick prioritario.';
