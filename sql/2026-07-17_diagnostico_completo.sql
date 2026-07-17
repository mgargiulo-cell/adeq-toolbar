-- ════════════════════════════════════════════════════════════════════════
-- DIAGNÓSTICO COMPLETO (2026-07-17) — una sola query, una sola tabla.
-- Cubre: salud del worker, agente del 16, error exacto de Monday, rebotes,
-- pools por fuente, Prospects, yield de AutoGoogle, presupuestos de APIs.
-- Hora de Buenos Aires (-03) para que "el 16" sea el 16 de verdad.
-- ════════════════════════════════════════════════════════════════════════

WITH v AS (
  SELECT '2026-07-16 00:00-03'::timestamptz AS ini,
         '2026-07-17 00:00-03'::timestamptz AS fin
),

-- 1. SALUD DEL WORKER ────────────────────────────────────────────────────
salud AS (
  SELECT 1 AS orden, '1. SALUD' AS seccion, key AS metrica, value AS valor,
         COALESCE(round(EXTRACT(epoch FROM (now() - updated_at))/60)::text || ' min atrás', '—') AS detalle
  FROM toolbar_config
  WHERE key IN ('auto_heartbeat_at','polish_cursor_ts','majestic_cursor',
                'apollo_calls_month','apollo_calls_month_period','apollo_daily_limit',
                'autogoogle_serper_used','autogoogle_serper_period','autogoogle_last_error',
                'autogoogle_fresh_rate','serper_contact_used','rapidapi_calls_month',
                'similar_expansion_enabled','autogoogle_fresh_keywords_enabled')
),

-- 2. AGENTE DEL 16: qué hizo ────────────────────────────────────────────
acciones AS (
  SELECT 2 AS orden, '2. AGENTE 16/07' AS seccion, action AS metrica,
         count(*)::text AS valor,
         count(DISTINCT domain)::text || ' dominios únicos' AS detalle
  FROM toolbar_agent_actions, v
  WHERE created_at >= v.ini AND created_at < v.fin
  GROUP BY action
),

-- 3. EL ERROR EXACTO DE MONDAY (los 15 fallos: mail enviado, CRM sin cargar) ──
monday_err AS (
  SELECT 3 AS orden, '3. MONDAY ERROR' AS seccion,
         COALESCE(left(details->>'monday_error', 90), '(sin detalle)') AS metrica,
         count(*)::text AS valor,
         'últ: ' || to_char(max(created_at) AT TIME ZONE 'America/Argentina/Buenos_Aires', 'DD/MM HH24:MI')
           || ' · ej: ' || min(domain) AS detalle
  FROM toolbar_agent_actions
  WHERE action = 'monday_failed' AND created_at >= now() - interval '7 days'
  GROUP BY 2
),

-- 4. REBOTES: 38 vs 24 enviados — ¿de dónde vienen? ─────────────────────
rebotes AS (
  SELECT 4 AS orden, '4. REBOTES 7d' AS seccion,
         action AS metrica, count(*)::text AS valor,
         count(DISTINCT domain)::text || ' dominios' AS detalle
  FROM toolbar_agent_actions
  WHERE action IN ('bounce_detected','bounce_retry_sent','no_alt_email','bounced_in_window',
                   'no_email_after_enrichment','auto_reply_detected')
    AND created_at >= now() - interval '7 days'
  GROUP BY action
),

-- 5. POOLS: el embudo por fuente ────────────────────────────────────────
pools AS (
  SELECT 5 AS orden, '5. POOL ' || source AS seccion, status AS metrica,
         count(*)::text AS valor,
         'último: ' || to_char(max(uploaded_at) AT TIME ZONE 'America/Argentina/Buenos_Aires', 'DD/MM HH24:MI') AS detalle
  FROM toolbar_csv_queue
  WHERE status IN ('pending','processing','waiting_pool','next_day')
  GROUP BY source, status
),

-- 6. EFICIENCIA POR FUENTE: ¿cuánto descarta cada feeder? ───────────────
eficiencia AS (
  SELECT 6 AS orden, '6. EFICIENCIA' AS seccion, source AS metrica,
         round(100.0 * count(*) FILTER (WHERE status = 'done')
               / NULLIF(count(*) FILTER (WHERE status IN ('done','skipped','frozen')), 0), 1)::text || '% pasa' AS valor,
         count(*) FILTER (WHERE status = 'done')::text || ' done · ' ||
         count(*) FILTER (WHERE status = 'skipped')::text || ' skip · ' ||
         count(*) FILTER (WHERE status = 'frozen')::text || ' frozen' AS detalle
  FROM toolbar_csv_queue
  GROUP BY source
),

-- 7. POR QUÉ SE CONGELAN (razones reales de descarte, últimos 3 días) ───
frozen_why AS (
  SELECT 7 AS orden, '7. DESCARTES 3d' AS seccion,
         COALESCE(left(error_message, 60), '(sin razón)') AS metrica,
         count(*)::text AS valor, '' AS detalle
  FROM toolbar_csv_queue
  WHERE status = 'frozen' AND uploaded_at >= now() - interval '3 days'
  GROUP BY 2
  ORDER BY count(*) DESC
  LIMIT 12
),

-- 8. PROSPECTS: qué fuente trae publishers de verdad (7 días) ───────────
prospects AS (
  SELECT 8 AS orden, '8. PROSPECTS 7d' AS seccion, source AS metrica,
         count(*)::text AS valor,
         count(*) FILTER (WHERE emails IS NOT NULL AND emails <> '{}')::text || ' c/email · ' ||
         count(*) FILTER (WHERE contact_phone IS NOT NULL)::text || ' c/tel · ' ||
         COALESCE(round(avg(traffic))::text, '0') || ' PV prom' AS detalle
  FROM toolbar_review_queue
  WHERE created_at >= now() - interval '7 days'
  GROUP BY source
),

-- 9. GEO: ¿cuánto de LATAM/España estamos trayendo hoy? (baseline) ──────
geo AS (
  SELECT 9 AS orden, '9. GEO 7d' AS seccion,
         CASE WHEN upper(COALESCE(geo,'??')) IN ('AR','MX','CL','CO','PE','UY','ES','VE','EC','BO','PY','CR','GT','DO','PA','HN','SV','NI')
              THEN 'HISPANO' ELSE 'resto: ' || upper(COALESCE(geo,'??')) END AS metrica,
         count(*)::text AS valor, '' AS detalle
  FROM toolbar_review_queue
  WHERE created_at >= now() - interval '7 days'
  GROUP BY 2
  ORDER BY count(*) DESC
  LIMIT 12
),

-- 10. YIELD de AutoGoogle (debe tener filas DESPUÉS del fix de la RPC) ──
yield_kw AS (
  SELECT 10 AS orden, '10. YIELD' AS seccion, 'filas en toolbar_keyword_yield' AS metrica,
         count(*)::text AS valor,
         'si es 0 y AutoGoogle ya corrió post-deploy → la RPC sigue rota' AS detalle
  FROM toolbar_keyword_yield
),
yield_top AS (
  SELECT 11 AS orden, '11. TOP KEYWORDS' AS seccion, left(phrase, 50) AS metrica,
         qualified::text || ' calificados' AS valor,
         searches::text || ' búsq · ' || fresh::text || ' frescos' AS detalle
  FROM toolbar_keyword_yield
  WHERE searches > 0
  ORDER BY qualified DESC, fresh DESC
  LIMIT 10
),

-- 12. ¿La RPC duplicada sigue viva? (debe devolver UNA sola firma) ──────
rpc_dup AS (
  SELECT 12 AS orden, '12. RPC bump_keyword_yield' AS seccion,
         p.oid::regprocedure::text AS metrica,
         'existe' AS valor,
         'si hay MÁS DE UNA fila → correr el DROP del fix' AS detalle
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE p.proname = 'bump_keyword_yield' AND n.nspname = 'public'
),

-- 13. ¿Existe ya el pre-listado? ───────────────────────────────────────
backlog AS (
  SELECT 13 AS orden, '13. PRE-LISTADO' AS seccion,
         'tabla toolbar_discovery_backlog' AS metrica,
         CASE WHEN to_regclass('public.toolbar_discovery_backlog') IS NULL
              THEN 'NO EXISTE → correr la migración' ELSE 'ok' END AS valor,
         '' AS detalle
)

SELECT seccion, metrica, valor, detalle FROM (
  SELECT * FROM salud      UNION ALL SELECT * FROM acciones   UNION ALL
  SELECT * FROM monday_err UNION ALL SELECT * FROM rebotes    UNION ALL
  SELECT * FROM pools      UNION ALL SELECT * FROM eficiencia UNION ALL
  SELECT * FROM frozen_why UNION ALL SELECT * FROM prospects  UNION ALL
  SELECT * FROM geo        UNION ALL SELECT * FROM yield_kw   UNION ALL
  SELECT * FROM yield_top  UNION ALL SELECT * FROM rpc_dup    UNION ALL
  SELECT * FROM backlog
) t
ORDER BY orden, valor DESC;
