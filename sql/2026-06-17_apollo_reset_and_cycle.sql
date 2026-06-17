-- Maxi 2026-06-17: Reset contadores + cambio de ciclo a "6-a-6".
--
-- 1) Apollo (apollo_calls_month): contador inflado por bug en apiProxy.js
--    (contaba búsquedas gratis como créditos). Bug arreglado. Reset a 0
--    para sincronizar con Apollo dashboard real.
--
-- 2) RapidAPI/SimilarWeb (rapidapi_calls_month): cycle real es del día 6 al
--    día 6 del mes siguiente. Al día 17 vamos en 9K reales según Maxi.
--    Cambiamos el period anchor a "2026-06-06" y dejamos el contador en 9000.

-- ── Apollo ───────────────────────────────────────────────────────
UPDATE public.toolbar_config
SET value = '0'
WHERE key = 'apollo_calls_month';

UPDATE public.toolbar_config
SET value = (CASE
  WHEN EXTRACT(DAY FROM current_date)::int < 6
    THEN to_char((current_date - interval '1 month')::date, 'YYYY-MM') || '-06'
  ELSE to_char(current_date, 'YYYY-MM') || '-06'
END)
WHERE key = 'apollo_calls_month_period';

-- Si la fila no existía, la creamos:
INSERT INTO public.toolbar_config (key, value)
SELECT 'apollo_calls_month', '0'
WHERE NOT EXISTS (SELECT 1 FROM public.toolbar_config WHERE key = 'apollo_calls_month');

INSERT INTO public.toolbar_config (key, value)
SELECT 'apollo_calls_month_period', (CASE
  WHEN EXTRACT(DAY FROM current_date)::int < 6
    THEN to_char((current_date - interval '1 month')::date, 'YYYY-MM') || '-06'
  ELSE to_char(current_date, 'YYYY-MM') || '-06'
END)
WHERE NOT EXISTS (SELECT 1 FROM public.toolbar_config WHERE key = 'apollo_calls_month_period');

-- ── RapidAPI / SimilarWeb (reset a 9000 + period 6-a-6) ─────────
UPDATE public.toolbar_config
SET value = '9000'
WHERE key = 'rapidapi_calls_month';

UPDATE public.toolbar_config
SET value = (CASE
  WHEN EXTRACT(DAY FROM current_date)::int < 6
    THEN to_char((current_date - interval '1 month')::date, 'YYYY-MM') || '-06'
  ELSE to_char(current_date, 'YYYY-MM') || '-06'
END)
WHERE key = 'rapidapi_calls_month_period';

INSERT INTO public.toolbar_config (key, value)
SELECT 'rapidapi_calls_month', '9000'
WHERE NOT EXISTS (SELECT 1 FROM public.toolbar_config WHERE key = 'rapidapi_calls_month');

INSERT INTO public.toolbar_config (key, value)
SELECT 'rapidapi_calls_month_period', (CASE
  WHEN EXTRACT(DAY FROM current_date)::int < 6
    THEN to_char((current_date - interval '1 month')::date, 'YYYY-MM') || '-06'
  ELSE to_char(current_date, 'YYYY-MM') || '-06'
END)
WHERE NOT EXISTS (SELECT 1 FROM public.toolbar_config WHERE key = 'rapidapi_calls_month_period');

-- Verificación (opcional):
-- SELECT key, value FROM public.toolbar_config
-- WHERE key IN ('apollo_calls_month','apollo_calls_month_period',
--               'rapidapi_calls_month','rapidapi_calls_month_period');
