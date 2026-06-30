-- Maxi 2026-06-30: RE-LEER todas las webs de Prospects con el scraper mejorado.
-- Prende el flag del worker `agent_reenrich_bad_leads`. El worker, cada 15 min,
-- toma 10 leads sin email (o con email genérico) y los re-scrapea GRATIS con la
-- lectura de HTML nueva (timeouts más largos + reintento + más deobfuscación +
-- data-attrs + mailto), cayendo a Apollo solo si hay cupo. Cuando ya no quedan
-- leads sin resolver, el worker apaga el flag solo.
--
-- ⚠️ Correr DESPUÉS de deployar el worker nuevo a Railway. Si no, re-lee con el
-- código viejo. Correr 1 vez en Supabase SQL Editor.

INSERT INTO public.toolbar_config (key, value)
VALUES ('agent_reenrich_bad_leads', 'true')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Verificación
SELECT key, value FROM public.toolbar_config WHERE key = 'agent_reenrich_bad_leads';

-- Para ver el progreso (cuántos pendientes siguen sin email):
SELECT
  COUNT(*)                                                            AS total_pending,
  COUNT(*) FILTER (WHERE COALESCE(array_length(emails, 1), 0) = 0)    AS sin_email
FROM public.toolbar_review_queue
WHERE status = 'pending';
