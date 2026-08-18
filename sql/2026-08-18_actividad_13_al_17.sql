-- ══════════════════════════════════════════════════════════════════════════════════════
-- ACTIVIDAD DEL 13 AL 17 DE AGOSTO 2026 — pedido de Maxi
--
--   1. Envíos por día y por media buyer
--   2. Altas a Prospects: cuántas y por qué método
--   3. Embudo por motor: qué encoló cada uno y qué sobrevivió
--   4. Autopilot y AutoGoogle en detalle
--   5. Limpieza: qué se descartó y por qué
--   6. Calidad: a quién se le escribió y si rebotó
--   7. Seguridad en el período
--
-- Corré las secciones de a una: el editor de Supabase muestra solo el ÚLTIMO resultado.
-- Las fechas son UTC (igual criterio que el reporte diario que ya existe).
-- ══════════════════════════════════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 0 · LA FOTO RÁPIDA (si el worker ya venía guardando la historia)
-- ══════════════════════════════════════════════════════════════════════════════════════
-- La tabla se creó el 12/08, así que debería tener los 5 días. Si sale vacía, la escritura
-- diaria no está corriendo — y las secciones de abajo calculan todo igual, en vivo.
SELECT dia, enviados, altas, limpiados, emails_hallados,
       pool_con_email, pool_sin_email, congelados,
       enviados_por_mb, altas_por_fuente
  FROM toolbar_metricas_diarias
 WHERE dia BETWEEN '2026-08-13' AND '2026-08-17'
 ORDER BY dia;


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 1 · ENVÍOS POR DÍA Y POR MEDIA BUYER
-- ══════════════════════════════════════════════════════════════════════════════════════
-- `sent` = primer contacto (el que tiene tope diario).
-- `secondary_sent` = 2da dirección del mismo dominio, NO consume cupo.
-- `re_sent` / `bounce_retry_sent` / `future_sent` = re-trabajo, sin tope.
SELECT created_at::date                                          AS dia,
       user_email                                                AS media_buyer,
       count(*) FILTER (WHERE action = 'sent')                   AS primer_contacto,
       count(*) FILTER (WHERE action = 'secondary_sent')         AS segundo_email,
       count(*) FILTER (WHERE action IN ('re_sent','bounce_retry_sent','future_sent')) AS re_trabajo,
       count(*) FILTER (WHERE action = 'skipped')                AS salteados,
       count(*) FILTER (WHERE action = 'failed')                 AS fallidos
  FROM toolbar_agent_actions
 WHERE created_at::date BETWEEN '2026-08-13' AND '2026-08-17'
   AND user_email IS NOT NULL
 GROUP BY 1, 2
 ORDER BY dia, primer_contacto DESC;

-- El total del período por MB (para ver quién trabajó más)
SELECT user_email AS media_buyer,
       count(*) FILTER (WHERE action = 'sent') AS primer_contacto_total,
       round(count(*) FILTER (WHERE action = 'sent') / 5.0, 1) AS promedio_por_dia
  FROM toolbar_agent_actions
 WHERE created_at::date BETWEEN '2026-08-13' AND '2026-08-17'
   AND user_email IS NOT NULL
 GROUP BY 1 ORDER BY primer_contacto_total DESC;

-- ⚠️ SI ALGÚN DÍA ENVIÓ MENOS DE LO ESPERADO, LA RESPUESTA ESTÁ ACÁ:
SELECT created_at::date AS dia, reason AS motivo, count(*) AS veces
  FROM toolbar_agent_actions
 WHERE created_at::date BETWEEN '2026-08-13' AND '2026-08-17'
   AND action = 'skipped'
 GROUP BY 1, 2 ORDER BY dia, veces DESC;


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 2 · ALTAS A PROSPECTS: CUÁNTAS Y POR QUÉ MÉTODO
-- ══════════════════════════════════════════════════════════════════════════════════════
-- `source` lo escribe cada motor: autopilot (similares + Radar), autogoogle, majestic,
-- sellers_json, adstxt, similar, monday_refresh, csv/manual.
SELECT created_at::date                    AS dia,
       coalesce(source, '(sin fuente)')    AS metodo,
       count(*)                            AS entraron,
       count(*) FILTER (WHERE jsonb_array_length(coalesce(emails,'[]'::jsonb)) > 0) AS con_email,
       round(avg(nullif(traffic, 0)))      AS trafico_promedio
  FROM toolbar_review_queue
 WHERE created_at::date BETWEEN '2026-08-13' AND '2026-08-17'
 GROUP BY 1, 2
 ORDER BY dia, entraron DESC;

-- El total del período por método, ordenado por volumen
SELECT coalesce(source, '(sin fuente)') AS metodo,
       count(*)                          AS entraron,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_del_total,
       count(*) FILTER (WHERE jsonb_array_length(coalesce(emails,'[]'::jsonb)) > 0) AS con_email,
       count(*) FILTER (WHERE status = 'rejected') AS ya_descartados
  FROM toolbar_review_queue
 WHERE created_at::date BETWEEN '2026-08-13' AND '2026-08-17'
 GROUP BY 1 ORDER BY entraron DESC;


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 3 · EMBUDO POR MOTOR — lo que encoló cada uno vs lo que sobrevivió
-- ══════════════════════════════════════════════════════════════════════════════════════
-- Acá se ve el desperdicio: un motor que encola 500 y deja 5 gastó créditos y llenó el
-- carril para nada. `csv_queue` es la cola de entrada; `review_queue` es lo que llegó a
-- Prospects. Comparar los dos es lo que dice qué motor conviene.
SELECT coalesce(source, '(sin fuente)') AS motor,
       count(*)                                              AS encolados,
       count(*) FILTER (WHERE status = 'done')               AS procesados,
       count(*) FILTER (WHERE status = 'skipped')            AS descartados,
       count(*) FILTER (WHERE status = 'pending')            AS aun_esperando,
       count(*) FILTER (WHERE status = 'frozen')             AS congelados,
       round(100.0 * count(*) FILTER (WHERE status='skipped') / nullif(count(*),0), 1) AS pct_descarte
  FROM toolbar_csv_queue
 WHERE uploaded_at::date BETWEEN '2026-08-13' AND '2026-08-17'
 GROUP BY 1 ORDER BY encolados DESC;

-- POR QUÉ se descartó cada uno (el motivo real, agrupado)
SELECT coalesce(source,'(sin fuente)') AS motor,
       split_part(coalesce(error_message,'(sin motivo)'), ':', 1) AS motivo,
       count(*) AS cuantos
  FROM toolbar_csv_queue
 WHERE uploaded_at::date BETWEEN '2026-08-13' AND '2026-08-17'
   AND status = 'skipped'
 GROUP BY 1, 2 ORDER BY cuantos DESC LIMIT 30;


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 4 · AUTOPILOT Y AUTOGOOGLE EN DETALLE
-- ══════════════════════════════════════════════════════════════════════════════════════
-- Día a día de los dos motores automáticos, de punta a punta.
SELECT uploaded_at::date AS dia,
       coalesce(source,'?') AS motor,
       count(*) AS encolo,
       count(*) FILTER (WHERE status='done')    AS proceso,
       count(*) FILTER (WHERE status='skipped') AS tiro
  FROM toolbar_csv_queue
 WHERE uploaded_at::date BETWEEN '2026-08-13' AND '2026-08-17'
   AND (source ILIKE '%autogoogle%' OR source ILIKE '%autopilot%' OR source ILIKE '%auto_feeder%')
 GROUP BY 1, 2 ORDER BY dia, motor;

-- Cuántos de esos llegaron REALMENTE a Prospects
SELECT created_at::date AS dia, source AS motor, count(*) AS llegaron_a_prospects
  FROM toolbar_review_queue
 WHERE created_at::date BETWEEN '2026-08-13' AND '2026-08-17'
   AND (source ILIKE '%autogoogle%' OR source ILIKE '%autopilot%' OR source ILIKE '%similar%')
 GROUP BY 1, 2 ORDER BY dia, motor;

-- Estado de los contadores y topes de los motores (foto de HOY, no del período)
SELECT key, value FROM toolbar_config
 WHERE key ILIKE '%autogoogle%' OR key ILIKE '%autopilot%'
    OR key ILIKE '%feeder%'     OR key ILIKE '%serper%'
 ORDER BY key;


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 5 · LIMPIEZA: QUÉ SE SACÓ DE PROSPECTS Y POR QUÉ
-- ══════════════════════════════════════════════════════════════════════════════════════
-- Prefijos: 'purge:' (barrido y pulido) · 'urlpurge:' (barrido por URL) · 'mb:' (la X del MB).
SELECT updated_at::date AS dia,
       split_part(coalesce(suspect_reason,'(sin motivo)'), ':', 1) AS quien,
       split_part(coalesce(suspect_reason,'(sin motivo)'), ':', 2) AS motivo,
       count(*) AS cuantos
  FROM toolbar_review_queue
 WHERE status = 'rejected'
   AND updated_at::date BETWEEN '2026-08-13' AND '2026-08-17'
 GROUP BY 1, 2, 3 ORDER BY dia, cuantos DESC;


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 6 · CALIDAD DEL ENVÍO — no alcanza con contar mails
-- ══════════════════════════════════════════════════════════════════════════════════════
-- chosen_tier: 4 = persona verificada · 3 = buzón comercial (publicidad@, ventas@)
--              2 = persona scrapeada · 1 = WHOIS · 0 = genérico (info@, contact@)
SELECT created_at::date AS dia, chosen_tier,
       count(*) AS envios,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY created_at::date), 1) AS pct_del_dia
  FROM toolbar_email_picks
 WHERE created_at::date BETWEEN '2026-08-13' AND '2026-08-17'
 GROUP BY 1, 2 ORDER BY dia, chosen_tier DESC;

-- Rebotes: un envío que rebota es un envío perdido Y reputación quemada
SELECT count(*) FILTER (WHERE a.action = 'sent')  AS enviados,
       count(DISTINCT b.email)                    AS rebotaron,
       round(100.0 * count(DISTINCT b.email) / nullif(count(*) FILTER (WHERE a.action='sent'),0), 1) AS pct_rebote
  FROM toolbar_agent_actions a
  LEFT JOIN toolbar_bounced_emails b
         ON lower(b.email) = lower(a.email_to) AND b.bounced_at >= a.created_at
 WHERE a.created_at::date BETWEEN '2026-08-13' AND '2026-08-17';


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 7 · SEGURIDAD DEL PERÍODO
-- ══════════════════════════════════════════════════════════════════════════════════════
-- Todo lo registrado, por tipo y día
SELECT created_at::date AS dia, kind AS tipo, severity, count(*) AS eventos
  FROM toolbar_security_events
 WHERE created_at::date BETWEEN '2026-08-13' AND '2026-08-18'
 GROUP BY 1, 2, 3 ORDER BY dia, eventos DESC;

-- ⚠️ LA QUE DECIDE SI HAY ATAQUE REAL: el ORIGEN de los jwt_invalido.
--   origen = chrome-extension://jgbacjjjohjaiojjecgnejcalepkjclm  → nuestra toolbar
--            con la sesión vencida. BENIGNO (arreglado el 18/08: el proxy ahora
--            renueva el token y reintenta solo).
--   origen vacío o distinto → llamada directa sin navegador (curl/script) = sondeo real.
SELECT actor AS ip,
       coalesce(detail->>'origin', '(sin origen)') AS origen,
       count(*) AS intentos,
       min(created_at) AS desde,
       max(created_at) AS hasta
  FROM toolbar_security_events
 WHERE kind = 'jwt_invalido'
   AND created_at > now() - interval '7 days'
 GROUP BY 1, 2 ORDER BY intentos DESC;

-- Gasto del proxy por usuario (para descartar uso pirata de una cuenta)
SELECT day, user_email, total
  FROM toolbar_api_usage
 WHERE day BETWEEN '2026-08-13' AND '2026-08-17'
 ORDER BY day, total DESC;
