-- ════════════════════════════════════════════════════════════════
-- DIAGNÓSTICO COMPLETO DEL AGENTE — Maxi 2026-06-18
-- ────────────────────────────────────────────────────────────────
-- Correr en Supabase SQL Editor 4-5 días después del deploy para ver
-- todo lo que hizo el agente y poder ajustar.
-- Pegame el resultado y yo te sugiero mejoras.
-- ════════════════════════════════════════════════════════════════

-- ════════════════════════════════════════════════════════════════
-- 1. ABASTECIMIENTO DE PROSPECTS — qué leads se agregaron por día
-- ════════════════════════════════════════════════════════════════
SELECT
  DATE(created_at) AS dia,
  source,
  COUNT(*) AS leads_agregados,
  ROUND(AVG(traffic)/1000) AS traffic_promedio_K,
  COUNT(*) FILTER (WHERE status = 'pending')   AS aun_pending,
  COUNT(*) FILTER (WHERE status = 'validated') AS ya_contactados,
  COUNT(*) FILTER (WHERE status = 'rejected')  AS rechazados
FROM toolbar_review_queue
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY 1 DESC, leads_agregados DESC;

-- ════════════════════════════════════════════════════════════════
-- 2. DISTRIBUCIÓN POR GEO de leads agregados (¿hay balance?)
-- ════════════════════════════════════════════════════════════════
SELECT
  COALESCE(geos_all[1], geo, '?') AS geo,
  COUNT(*) AS cantidad,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS porcentaje
FROM toolbar_review_queue
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY cantidad DESC
LIMIT 20;

-- ════════════════════════════════════════════════════════════════
-- 3. ENVÍOS POR MEDIA BUYER por día (manual vs agente)
-- ════════════════════════════════════════════════════════════════
SELECT
  DATE(created_at) AS dia,
  user_email AS mb,
  COUNT(*) FILTER (WHERE action = 'sent' AND COALESCE(details->>'ui_origin','') = 'toolbar_manual') AS manual,
  COUNT(*) FILTER (WHERE action = 'sent' AND COALESCE(details->>'ui_origin','') != 'toolbar_manual') AS agente,
  COUNT(*) FILTER (WHERE action = 'secondary_sent') AS segundo_email,
  COUNT(*) FILTER (WHERE action = 're_sent') AS re_envios,
  COUNT(*) FILTER (WHERE action = 'bounce_retry_sent') AS bounce_retry,
  COUNT(*) AS total
FROM toolbar_agent_actions
WHERE created_at >= NOW() - INTERVAL '7 days'
  AND action IN ('sent','secondary_sent','re_sent','bounce_retry_sent')
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- ════════════════════════════════════════════════════════════════
-- 4. CÓMO DECIDIÓ EL AGENTE QUÉ EMAIL ENVIAR (source picked)
-- ════════════════════════════════════════════════════════════════
SELECT
  source,
  COUNT(*) AS envios,
  COUNT(*) FILTER (WHERE response_type = 'real') AS respuestas_reales,
  COUNT(*) FILTER (WHERE response_type = 'ooo')  AS ooo,
  ROUND(100.0 * COUNT(*) FILTER (WHERE response_type = 'real') / NULLIF(COUNT(*), 0), 1) AS conv_pct_real
FROM toolbar_response_tracking
WHERE sent_at >= NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY envios DESC;

-- ════════════════════════════════════════════════════════════════
-- 5. CÓMO DECIDIÓ EL FEEDER QUÉ URLs PROSPECTAR (source del lead)
-- ════════════════════════════════════════════════════════════════
SELECT
  DATE(cron_at) AS dia,
  status,
  SUM(gross_sellers)  AS sellers_json,
  SUM(gross_monday)   AS monday_refresh,
  SUM(gross_majestic) AS autopilot_majestic,
  SUM(gross_total)    AS total_brutos,
  SUM(effective_added) AS efectivos_a_prospects
FROM toolbar_feeder_runs
WHERE cron_at >= NOW() - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- ════════════════════════════════════════════════════════════════
-- 6. RAZONES DE DESCARTE (por qué no llegaron a Prospects)
-- ════════════════════════════════════════════════════════════════
SELECT
  status,
  COALESCE(SUBSTRING(error_message FROM '^[^:(]+'), 'sin_razon') AS razon_corta,
  COUNT(*) AS cantidad
FROM toolbar_csv_queue
WHERE processed_at >= NOW() - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY cantidad DESC
LIMIT 30;

-- ════════════════════════════════════════════════════════════════
-- 7. BOUNCE + OOO + RETRY ACTIVITY
-- ════════════════════════════════════════════════════════════════
SELECT
  DATE(created_at) AS dia,
  user_email AS mb,
  action,
  COUNT(*) AS cant
FROM toolbar_agent_actions
WHERE action IN ('bounce_detected','auto_reply_detected','bounce_retry_sent','re_sent','auto_promoted','bounce_scan_failed','secondary_sent')
  AND created_at >= NOW() - INTERVAL '7 days'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

-- ════════════════════════════════════════════════════════════════
-- 8. X-LEARN ACTIVITY (rechazos del MB → agente aprende)
-- ════════════════════════════════════════════════════════════════
SELECT
  category,
  traffic_bucket,
  geo,
  COUNT(*) AS dislikes,
  ARRAY_AGG(DISTINCT user_email) AS quienes_rechazaron
FROM toolbar_autopilot_feedback
WHERE action = 'disliked'
  AND created_at >= NOW() - INTERVAL '30 days'
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 2
ORDER BY dislikes DESC
LIMIT 20;

-- ════════════════════════════════════════════════════════════════
-- 9. APOLLO + RAPIDAPI USAGE (cuotas)
-- ════════════════════════════════════════════════════════════════
SELECT key, value
FROM toolbar_config
WHERE key IN (
  'apollo_calls_month','apollo_calls_month_period',
  'rapidapi_calls_month','rapidapi_calls_month_period'
);

-- ════════════════════════════════════════════════════════════════
-- 10. POOL ACTUAL DE PROSPECTS (estado de la cola)
-- ════════════════════════════════════════════════════════════════
SELECT
  status,
  COUNT(*) AS cantidad,
  ROUND(AVG(traffic)/1000) AS traffic_promedio_K,
  COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 day')  AS ultimas_24h,
  COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') AS ultima_semana
FROM toolbar_review_queue
GROUP BY 1
ORDER BY 2 DESC;

-- ════════════════════════════════════════════════════════════════
-- 11. PERFORMANCE POR SOURCE (ranking dinámico vivo)
-- ════════════════════════════════════════════════════════════════
SELECT
  mb_email,
  source,
  sent,
  opens,
  bounces,
  ROUND(100.0 * opens   / NULLIF(sent, 0), 1) AS open_pct,
  ROUND(100.0 * bounces / NULLIF(sent, 0), 1) AS bounce_pct,
  score
FROM toolbar_source_performance
WHERE window_days = 30
ORDER BY mb_email, score DESC;

-- ════════════════════════════════════════════════════════════════
-- 12. TOP 10 DOMINIOS PROSPECTADOS RECIENTEMENTE (sanity check)
-- ════════════════════════════════════════════════════════════════
SELECT
  domain,
  source,
  traffic,
  geo,
  category,
  array_length(emails, 1) AS num_emails,
  status,
  created_at
FROM toolbar_review_queue
WHERE created_at >= NOW() - INTERVAL '2 days'
ORDER BY created_at DESC
LIMIT 30;
