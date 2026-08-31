-- ═══════════════════════════════════════════════════════════════════════════════════════
-- EL MONITOR MEDÍA HORAS DE RELOJ CONTRA UN SISTEMA QUE DUERME (Maxi 2026-08-31)
-- ═══════════════════════════════════════════════════════════════════════════════════════
-- El worker trabaja lunes a viernes de 9 a 23 en Madrid: 70 horas activas por semana, de
-- 168 de reloj. La vista comparaba `now() - last_ok_at` —tiempo de reloj, que corre igual
-- de noche y el domingo— contra la cadencia esperada. El fin de semana solo agrega horas
-- muertas al numerador, así que TODO trabajo diario amanece en rojo el lunes sin que haya
-- pasado nada. `monday_llegada` (cada 12h) es rojo garantizado todos los lunes.
--
-- No es teórico: hoy 31/08, lunes, estaban en rojo o amarillo purga_cola, metricas_diarias,
-- monday_llegada, monday_sync y aprovechamiento_apollo. Ninguno había fallado; el worker
-- había estado durmiendo, que es exactamente lo que se le pidió.
--
-- Es el mismo error que venimos arrastrando: el número que alarma tiene que medir lo mismo
-- que el número contra el que se compara. Acá el numerador contaba horas de calendario y el
-- denominador expresaba vueltas de trabajo.
--
-- Y era una bomba con fecha: el cambio de hoy (agente solo de lunes a viernes) garantizaba
-- un mail de alarma cada lunes a la mañana, para siempre. Un monitor que grita todos los
-- lunes deja de leerse, y ese día deja de servir para el rojo de verdad.

-- Minutos ACTIVOS entre dos momentos: solo lunes a viernes, solo de 9 a 23 hora de Madrid.
-- Es la misma ventana que decide en index.js (_isWeekendSpain / _isOutsideActiveHours), así
-- que si algún día se cambia el horario del worker hay que cambiar las dos.
CREATE OR REPLACE FUNCTION toolbar_minutos_activos(desde timestamptz, hasta timestamptz)
RETURNS numeric
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(SUM(
    GREATEST(0, EXTRACT(EPOCH FROM (
        LEAST(hasta,  ((d::date + time '23:00') AT TIME ZONE 'Europe/Madrid'))
      - GREATEST(desde, ((d::date + time '09:00') AT TIME ZONE 'Europe/Madrid'))
    )) / 60)
  ), 0)
  FROM generate_series(
    (desde AT TIME ZONE 'Europe/Madrid')::date,
    (hasta AT TIME ZONE 'Europe/Madrid')::date,
    interval '1 day'
  ) d
  WHERE EXTRACT(isodow FROM d) <= 5   -- 1=lunes … 5=viernes
    AND hasta > desde;
$$;

COMMENT ON FUNCTION toolbar_minutos_activos IS
  'Minutos en los que el worker estuvo despierto (L-V 9-23 Madrid) entre dos momentos. '
  'Sirve para juzgar si un job está atrasado sin contar como atraso las noches ni el finde.';

-- La vista, ahora midiendo lo mismo que expresa la cadencia.
CREATE OR REPLACE VIEW toolbar_health_check AS
SELECT
  job,
  CASE
    WHEN last_ok_at IS NULL THEN '🔴 NUNCA CORRIÓ'
    -- esperado_cada_min NULL = trabajo a pedido (lo dispara una persona). No tiene horario
    -- que incumplir, así que no se lo juzga por el reloj sino por su último resultado.
    WHEN esperado_cada_min IS NOT NULL
     AND toolbar_minutos_activos(last_ok_at, now()) > (esperado_cada_min * 3) THEN '🔴 ATRASADO'
    WHEN esperado_cada_min IS NOT NULL
     AND toolbar_minutos_activos(last_ok_at, now()) > (esperado_cada_min * 1.5) THEN '🟡 demorado'
    WHEN fails_consecutivos >= 3 THEN '🔴 FALLANDO'
    WHEN esperado_ultimo IS NOT NULL AND real_ultimo IS NOT NULL
     AND real_ultimo < (esperado_ultimo * 0.5) THEN '🟡 rinde poco'
    ELSE '🟢 ok'
  END AS estado,
  ROUND(EXTRACT(EPOCH FROM now() - last_ok_at) / 60)::int AS hace_min,
  esperado_cada_min AS esperado_cada,
  real_ultimo,
  esperado_ultimo,
  fails_consecutivos,
  last_status,
  last_detail,
  last_run_at,
  -- Va al final porque CREATE OR REPLACE VIEW no deja insertar columnas en el medio.
  -- `hace_min` es lo que ve un humano mirando el reloj; `hace_min_activos` es lo que de
  -- verdad decide el estado. Estar los dos evita el "dice 68 horas pero figura en verde".
  ROUND(toolbar_minutos_activos(last_ok_at, now()))::int AS hace_min_activos
FROM toolbar_health;
