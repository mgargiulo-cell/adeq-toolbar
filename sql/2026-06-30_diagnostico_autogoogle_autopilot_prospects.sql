-- ════════════════════════════════════════════════════════════════
-- DIAGNÓSTICO: AutoGoogle + Autopilot + estado de Prospects — Maxi 2026-06-30
-- ────────────────────────────────────────────────────────────────
-- Solo SELECTs (no modifica nada). Correr todo el bloque en Supabase SQL
-- Editor y pegarme el resultado. Cada query tiene su título.
-- ════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════
-- 0. ¿AUTOGOOGLE ESTÁ PRENDIDO? — contador de búsquedas Serper
--    Si no devuelve filas o used=0 → falta SERPER_API_KEY en Railway
--    (AutoGoogle nunca hizo una búsqueda).
-- ════════════════════════════════════════════════════════════════
SELECT key, value
FROM public.toolbar_config
WHERE key IN ('autogoogle_serper_used', 'autogoogle_serper_period');


-- ════════════════════════════════════════════════════════════════
-- 1. EMBUDO POR FUENTE — qué puso cada motor en Prospects, cuántos
--    tienen email, y cuántos terminaron enviados (toolbar_sendtrack).
--    Mira los últimos 30 días. autogoogle / autopilot son los 2 que querés ver.
-- ════════════════════════════════════════════════════════════════
SELECT
  COALESCE(rq.source, '(sin source)')                                  AS fuente,
  COUNT(*)                                                             AS total_en_prospects,
  COUNT(*) FILTER (WHERE (jsonb_typeof(rq.emails)='array' AND jsonb_array_length(rq.emails) > 0))  AS con_email,
  ROUND(100.0 * COUNT(*) FILTER (WHERE (jsonb_typeof(rq.emails)='array' AND jsonb_array_length(rq.emails) > 0))
        / NULLIF(COUNT(*), 0), 1)                                      AS pct_con_email,
  COUNT(*) FILTER (WHERE rq.status = 'pending')                        AS pendientes,
  COUNT(DISTINCT st.domain)                                            AS enviados,
  ROUND(AVG(rq.traffic) / 1000)                                        AS traffic_prom_K
FROM public.toolbar_review_queue rq
LEFT JOIN public.toolbar_sendtrack st ON st.domain = rq.domain
WHERE rq.created_at >= now() - interval '30 days'
GROUP BY COALESCE(rq.source, '(sin source)')
ORDER BY total_en_prospects DESC;


-- ════════════════════════════════════════════════════════════════
-- 2. AUTOGOOGLE en detalle — leads agregados por día (últimos 30d)
-- ════════════════════════════════════════════════════════════════
SELECT
  DATE(created_at)                                                    AS dia,
  COUNT(*)                                                            AS leads,
  COUNT(*) FILTER (WHERE (jsonb_typeof(emails)='array' AND jsonb_array_length(emails) > 0))    AS con_email,
  ROUND(AVG(traffic) / 1000)                                          AS traffic_prom_K
FROM public.toolbar_review_queue
WHERE source = 'autogoogle'
  AND created_at >= now() - interval '30 days'
GROUP BY DATE(created_at)
ORDER BY dia DESC;


-- ════════════════════════════════════════════════════════════════
-- 3. AUTOPILOT en detalle — leads agregados por día (últimos 30d)
-- ════════════════════════════════════════════════════════════════
SELECT
  DATE(created_at)                                                    AS dia,
  COUNT(*)                                                            AS leads,
  COUNT(*) FILTER (WHERE (jsonb_typeof(emails)='array' AND jsonb_array_length(emails) > 0))    AS con_email,
  ROUND(AVG(traffic) / 1000)                                          AS traffic_prom_K
FROM public.toolbar_review_queue
WHERE source = 'autopilot'
  AND created_at >= now() - interval '30 days'
GROUP BY DATE(created_at)
ORDER BY dia DESC;


-- ════════════════════════════════════════════════════════════════
-- 4. ESTADO ACTUAL DE PROSPECTS (status=pending) por OWNER
--    Agent = lo cargó el agente autónomo (created_by vacío/null).
--    Email real = carga manual de ese MB.
-- ════════════════════════════════════════════════════════════════
SELECT
  CASE WHEN COALESCE(NULLIF(TRIM(created_by), ''), '') = ''
       THEN '🤖 Agent (autónomo)'
       ELSE created_by END                                            AS owner,
  COUNT(*)                                                            AS pendientes,
  COUNT(*) FILTER (WHERE (jsonb_typeof(emails)='array' AND jsonb_array_length(emails) > 0))    AS con_email,
  ROUND(100.0 * COUNT(*) FILTER (WHERE (jsonb_typeof(emails)='array' AND jsonb_array_length(emails) > 0))
        / NULLIF(COUNT(*), 0), 1)                                     AS pct_con_email
FROM public.toolbar_review_queue
WHERE status = 'pending'
GROUP BY 1
ORDER BY pendientes DESC;


-- ════════════════════════════════════════════════════════════════
-- 5. PROSPECTS pendientes por GEO (top 20) — para ver el balance
--    y detectar cuántos quedan "sin geo".
-- ════════════════════════════════════════════════════════════════
SELECT
  COALESCE(NULLIF(TRIM(geo), ''), '(sin geo)')                        AS geo,
  COUNT(*)                                                            AS pendientes,
  COUNT(*) FILTER (WHERE (jsonb_typeof(emails)='array' AND jsonb_array_length(emails) > 0))    AS con_email
FROM public.toolbar_review_queue
WHERE status = 'pending'
GROUP BY 1
ORDER BY pendientes DESC
LIMIT 20;


-- ════════════════════════════════════════════════════════════════
-- 6. SALUD GENERAL DE PROSPECTS — el número grande de un vistazo
-- ════════════════════════════════════════════════════════════════
SELECT
  COUNT(*)                                                            AS total_pending,
  COUNT(*) FILTER (WHERE (jsonb_typeof(emails)='array' AND jsonb_array_length(emails) > 0))    AS con_email,
  COUNT(*) FILTER (WHERE NOT (jsonb_typeof(emails)='array' AND jsonb_array_length(emails) > 0))    AS sin_email,
  ROUND(100.0 * COUNT(*) FILTER (WHERE (jsonb_typeof(emails)='array' AND jsonb_array_length(emails) > 0))
        / NULLIF(COUNT(*), 0), 1)                                     AS pct_con_email,
  COUNT(*) FILTER (WHERE COALESCE(traffic, 0) = 0)                    AS sin_traffic
FROM public.toolbar_review_queue
WHERE status = 'pending';
