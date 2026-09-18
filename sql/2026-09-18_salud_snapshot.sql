-- APLICADO en producción el 2026-09-18 (CLI de Supabase). Copia de registro.
--
-- Para qué: la revisión automática de la toolbar (rutina en la nube, martes y viernes) necesita ver
-- la salud del sistema y NO tiene acceso a la base: el CLI de Supabase vive en la Mac del dueño.
-- Esto le da una sola cosa: una FOTO de sólo lectura, con agregados.
--
-- Qué NO expone, a propósito: ninguna API key (la config va por LISTA BLANCA de claves, no por lista
-- negra), ningún email de un lead, ningún pitch. Dominios sí (son sitios públicos).
-- Cómo se protege: la función es SECURITY DEFINER y exige una clave de 32 bytes al azar; en la base
-- sólo vive su SHA-256 (`salud_snapshot_sha256` en toolbar_config), así que ni leyendo la config se
-- puede reconstruir. Para revocarla: cambiar ese hash. No escribe nada, en ningún caso.
CREATE OR REPLACE FUNCTION public.salud_snapshot(k text)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = public
AS $function$
DECLARE
  esperado text;
BEGIN
  SELECT value INTO esperado FROM toolbar_config WHERE key = 'salud_snapshot_sha256';
  IF esperado IS NULL OR length(coalesce(k, '')) < 32
     OR encode(sha256(convert_to(k, 'utf8')), 'hex') <> esperado THEN
    RETURN json_build_object('error', 'no autorizado');
  END IF;

  RETURN json_build_object(
    'generado_at', now(),
    'nota', 'Foto de solo lectura. Horas en UTC. Ver docs/REVISION-SEMANAL.md para leerla.',

    'config', (SELECT coalesce(json_object_agg(key, left(value, 400)), '{}'::json) FROM toolbar_config
      WHERE key IN ('worker_commit','auto_prospecting_enabled','csv_queue_enabled','kill_switch','agent_paused_until',
                    'agent_enabled_users','agent_max_per_day','agent_active_hours_start','agent_active_hours_end',
                    'agent_slots_done','agent_last_slot','autogoogle_slots_done','autopilot_last_slot',
                    'rapidapi_calls_month','rapidapi_calls_month_period','rapidapi_monthly_limit','rapidapi_daily_limit',
                    'apollo_calls_month','apollo_calls_month_period','apollo_monthly_limit',
                    'claude_daily_cap','millionverifier_daily_cap','millionverifier_used',
                    'polish_rol_mx_used','polish_rol_mx_daily_cap','polish_patron_used','polish_patron_daily_cap',
                    'feeder_daily_target','feeder_geo_cursor','majestic_cursor','sellers_google_cursor',
                    'monday_sync_ultimo','autogoogle_stats','parte_diario_ultimo','barrido_np_cursor','barrido_np_daily_cap')),

    'health', (SELECT coalesce(json_agg(to_jsonb(h) ORDER BY h.last_run_at), '[]'::json) FROM toolbar_health h),

    'feeder_runs', (SELECT coalesce(json_agg(x), '[]'::json) FROM (
        SELECT slot_label, status, gross_total, effective_added, rapidapi_used, rapidapi_limit, rq_valid_before, left(coalesce(notes,''), 160) AS notes
        FROM toolbar_feeder_runs ORDER BY slot_label DESC LIMIT 15) x),

    'cola_por_estado', (SELECT coalesce(json_object_agg(status, n), '{}'::json) FROM (
        SELECT coalesce(status,'?') AS status, count(*) AS n FROM toolbar_csv_queue GROUP BY 1) x),

    'cola_motivos_24h', (SELECT coalesce(json_agg(x), '[]'::json) FROM (
        SELECT coalesce(status,'?') AS status, coalesce(source,'?') AS source,
               left(regexp_replace(coalesce(error_message,''), '[0-9]{4}-[0-9]{2}-[0-9]{2}[0-9T:.\-Z]*', '#', 'g'), 110) AS motivo,
               count(*) AS n, min(domain) AS ejemplo
        FROM toolbar_csv_queue WHERE processed_at >= now() - interval '24 hours'
        GROUP BY 1,2,3 ORDER BY n DESC LIMIT 40) x),

    'cola_en_error', (SELECT coalesce(json_agg(x), '[]'::json) FROM (
        SELECT left(coalesce(error_message,''), 160) AS motivo, count(*) AS n, min(domain) AS ejemplo, max(processed_at) AS ultimo
        FROM toolbar_csv_queue WHERE status = 'error' GROUP BY 1 ORDER BY n DESC LIMIT 15) x),

    'agente_7d', (SELECT coalesce(json_agg(x), '[]'::json) FROM (
        SELECT (created_at AT TIME ZONE 'Europe/Madrid')::date AS dia, coalesce(user_email,'?') AS buzon, action,
               split_part(coalesce(reason,''), ':', 1) AS motivo, ((details->>'ui_origin') IS NOT NULL) AS a_mano, count(*) AS n
        FROM toolbar_agent_actions WHERE created_at >= now() - interval '7 days'
        GROUP BY 1,2,3,4,5 ORDER BY 1 DESC, n DESC LIMIT 300) x),

    'prospects_por_estado', (SELECT coalesce(json_object_agg(status, n), '{}'::json) FROM (
        SELECT coalesce(status,'?') AS status, count(*) AS n FROM toolbar_review_queue GROUP BY 1) x),

    'altas_7d', (SELECT coalesce(json_agg(x), '[]'::json) FROM (
        SELECT coalesce(source,'?') AS source, count(*) AS n,
               count(*) FILTER (WHERE emails IS NOT NULL AND emails::text NOT IN ('[]','null','')) AS con_email,
               max(created_at) AS ultima
        FROM toolbar_review_queue WHERE created_at >= now() - interval '7 days' GROUP BY 1 ORDER BY n DESC) x),

    'ultima_alta_por_fuente', (SELECT coalesce(json_object_agg(source, ultima), '{}'::json) FROM (
        SELECT coalesce(source,'?') AS source, max(created_at) AS ultima FROM toolbar_review_queue GROUP BY 1) x),

    'metricas_diarias', (SELECT coalesce(json_agg(to_jsonb(m) ORDER BY m.dia DESC), '[]'::json) FROM (
        SELECT * FROM toolbar_metricas_diarias ORDER BY dia DESC LIMIT 14) m),

    'claude_gasto_3d', (SELECT coalesce(json_agg(x), '[]'::json) FROM (
        SELECT dia, fuente, motivo, sum(llamadas) AS llamadas, sum(tokens_in) AS tokens_in, sum(tokens_out) AS tokens_out
        FROM toolbar_claude_gasto WHERE dia >= current_date - 3 GROUP BY 1,2,3 ORDER BY 1 DESC, llamadas DESC) x),

    'millionverifier_7d', (SELECT coalesce(json_object_agg(result, n), '{}'::json) FROM (
        SELECT coalesce(result,'?') AS result, count(*) AS n FROM toolbar_mv_results
        WHERE created_at >= now() - interval '7 days' GROUP BY 1) x)
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.salud_snapshot(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.salud_snapshot(text) TO anon, authenticated;
