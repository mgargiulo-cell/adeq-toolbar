-- APLICADO en producción el 2026-09-13 (Maxi, SQL Editor). Copia de registro: el RPC vive en la base.
--
-- Causa raíz del "Apollo 0 de 2.500" cuando ya iban 717: el RPC guardaba el período como mes calendario
-- UTC ("2026-09") y reseteaba el 1 de cada mes, mientras el worker (getApolloUsageToday, _apolloCyclePeriod,
-- ancla 12) compara contra el inicio del ciclo real de Apollo ("2026-09-12"). Nunca coincidían: el worker
-- daba el contador por viejo, lo ponía en 0 y cada reinicio lo volvía a pisar.
-- Sólo cambia Apollo: escribe el inicio del ciclo 12→12 (YYYY-MM-DD) y compara completo. RapidAPI y el
-- resto siguen por mes calendario (substr 1..7), como antes.
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
         OR (provider = 'apollo' AND stored_period != cur_period)
         OR (provider != 'apollo' AND substr(stored_period, 1, 7) != cur_period);
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
