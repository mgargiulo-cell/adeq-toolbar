-- APLICADO en producción el 2026-09-18 (CLI de Supabase). Copia de registro: el RPC vive en la base.
--
-- Mismo bug que el de Apollo del 13/09, que ese día quedó sin tocar para RapidAPI ("RapidAPI y el
-- resto siguen por mes calendario"). El ciclo de RapidAPI renueva el 18 (RAPIDAPI_CYCLE_ANCHOR_DAY
-- en el worker, confirmado por Maxi el 13/09), pero el RPC guardaba el período como mes calendario
-- ("2026-09") y reseteaba el contador el día 1. El worker compara por YYYY-MM contra el INICIO DEL
-- CICLO, así que las dos mitades del mes salían mal:
--   · del 1 al 17: ciclo "2026-08-18" → "2026-08" ≠ "2026-09" → el worker leía 0 usados (sin tope
--     mensual ni freno de ritmo) y el RPC ya había tirado lo gastado del 18 al 31 del mes anterior;
--   · del 18 a fin de mes: ciclo "2026-09-18" → "2026-09" = "2026-09" → el worker leía como gasto del
--     ciclo NUEVO lo acumulado del 1 al 17, que era del ciclo VIEJO.
-- Visto el 18/09: toolbar_feeder_runs anotó "skipped_throttle — pacing: used 40% vs cycle 1%" con
-- 15.914 llamadas que eran del ciclo anterior. El freno de ritmo habría durado ~12 días.
-- Ahora RapidAPI guarda el inicio de su ciclo 18→18 (YYYY-MM-DD) y compara completo, igual que Apollo.
-- ⚠️ El 18 tiene que ser el mismo que RAPIDAPI_CYCLE_ANCHOR_DAY (auto-prospector/index.js): hay un
-- test que lo exige (tests/ciclo-rapidapi-18-09.test.js).
CREATE OR REPLACE FUNCTION public.bump_api_counter(provider text, n integer DEFAULT 1)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  hoy date := (now() AT TIME ZONE 'UTC')::date;
  cur_period text := CASE
    WHEN provider = 'apollo' THEN
      CASE WHEN extract(day from hoy) >= 12
           THEN to_char(date_trunc('month', hoy) + interval '11 days', 'YYYY-MM-DD')
           ELSE to_char(date_trunc('month', hoy - interval '1 month') + interval '11 days', 'YYYY-MM-DD')
      END
    WHEN provider = 'rapidapi' THEN
      CASE WHEN extract(day from hoy) >= 18
           THEN to_char(date_trunc('month', hoy) + interval '17 days', 'YYYY-MM-DD')
           ELSE to_char(date_trunc('month', hoy - interval '1 month') + interval '17 days', 'YYYY-MM-DD')
      END
    ELSE to_char(hoy, 'YYYY-MM')
  END;
  k_count text := provider || '_calls_month';
  k_period text := provider || '_calls_month_period';
  stored_period text;
  stored_count int;
  new_count int;
  cambio boolean;
BEGIN
  SELECT value INTO stored_period FROM toolbar_config WHERE key = k_period;
  cambio := stored_period IS NULL
         OR (provider IN ('apollo', 'rapidapi') AND stored_period != cur_period)
         OR (provider NOT IN ('apollo', 'rapidapi') AND substr(stored_period, 1, 7) != cur_period);
  IF cambio THEN
    INSERT INTO toolbar_config (key, value) VALUES (k_period, cur_period)
    ON CONFLICT (key) DO UPDATE SET value = excluded.value;
    INSERT INTO toolbar_config (key, value) VALUES (k_count, n::text)
    ON CONFLICT (key) DO UPDATE SET value = n::text;
    new_count := n;
  ELSE
    SELECT COALESCE(value::int, 0) INTO stored_count FROM toolbar_config WHERE key = k_count;
    new_count := stored_count + n;
    INSERT INTO toolbar_config (key, value) VALUES (k_count, new_count::text)
    ON CONFLICT (key) DO UPDATE SET value = excluded.value;
  END IF;
  RETURN json_build_object('provider', provider, 'period', cur_period, 'count', new_count);
END;
$function$;
