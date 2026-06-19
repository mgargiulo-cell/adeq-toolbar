-- ════════════════════════════════════════════════════════════════
-- RESET TASA DE RESPUESTA — Maxi 2026-06-19
-- ────────────────────────────────────────────────────────────────
-- Vacía toolbar_response_tracking para arrancar de cero el contador de
-- "Conversión por fuente de email" (apollo/informer/scrape/generic).
-- OJO: el agente usa estos números para rankear qué fuente de email priorizar
-- → tras el reset re-aprende desde cero (unos días con poca data).
-- Correr 1 vez en Supabase SQL Editor.
-- ════════════════════════════════════════════════════════════════

SELECT count(*) AS antes FROM public.toolbar_response_tracking;

DELETE FROM public.toolbar_response_tracking;

SELECT count(*) AS despues FROM public.toolbar_response_tracking;
