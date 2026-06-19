-- ════════════════════════════════════════════════════════════════
-- RESET LISTA DE BASURA (blocklist) — Maxi 2026-06-19
-- ────────────────────────────────────────────────────────────────
-- Vacía toolbar_url_blocklist para que el sistema re-aprenda qué es basura
-- DESDE CERO, con el filtro de publisher (AdSense/ads.txt/Haiku) + el
-- reject-learning del botón rojo. El dedup ya NO usa blocklist como bloqueo
-- duro: la fuente de verdad pasa a ser Monday (con estado) + Prospects.
-- Correr 1 vez en Supabase SQL Editor.
-- ════════════════════════════════════════════════════════════════

-- Antes:
SELECT count(*) AS blocklist_antes FROM public.toolbar_url_blocklist;

-- Vaciar (AUTO + MANUAL):
DELETE FROM public.toolbar_url_blocklist;

-- Verificación:
SELECT count(*) AS blocklist_despues FROM public.toolbar_url_blocklist;
