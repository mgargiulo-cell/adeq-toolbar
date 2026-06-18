-- Maxi 2026-06-18: Limpiar leads sin tráfico que se acumularon en Prospects.
-- Estos son los "fantasmas" que Maxi reporta — entraron antes del guard nuevo.
--
-- Borra rows pending con:
--   - traffic = 0 o NULL (SimilarWeb no devolvió data en los reintentos), O
--   - traffic > 0 pero < 400K (basura del threshold)
-- Excluye monday_refresh (re-prospect explícito del MB).

-- Conteo previo (opcional — para saber cuánto vamos a borrar)
SELECT
  COUNT(*) FILTER (WHERE traffic = 0 OR traffic IS NULL) AS sin_traffic,
  COUNT(*) FILTER (WHERE traffic > 0 AND traffic < 400000) AS bajo_threshold
FROM public.toolbar_review_queue
WHERE status = 'pending'
  AND (source IS NULL OR source != 'monday_refresh');

-- Borrar fantasmas (sin tráfico)
DELETE FROM public.toolbar_review_queue
WHERE status = 'pending'
  AND (source IS NULL OR source != 'monday_refresh')
  AND (traffic = 0 OR traffic IS NULL);

-- Borrar bajo-threshold
DELETE FROM public.toolbar_review_queue
WHERE status = 'pending'
  AND (source IS NULL OR source != 'monday_refresh')
  AND traffic > 0
  AND traffic < 400000;

-- Verificación post-cleanup
SELECT
  COUNT(*) AS pendientes_validos
FROM public.toolbar_review_queue
WHERE status = 'pending'
  AND traffic >= 400000;
