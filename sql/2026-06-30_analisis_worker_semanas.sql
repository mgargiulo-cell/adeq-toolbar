-- ════════════════════════════════════════════════════════════════
-- ANÁLISIS EXHAUSTIVO DEL WORKER (últimas semanas) — Maxi 2026-06-30
-- ────────────────────────────────────────────────────────────────
-- Solo SELECTs. Responde: cuánto se envió, por quién, de qué fuente salió
-- el dato (scrape vs apollo vs informer/social), cuántos rebotaron, cuántos
-- respondieron, y qué se hizo con los rebotados. Pegame los resultados.
-- Ventana por defecto: últimos 28 días (cambiá el interval si querés).
-- ════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════
-- 1. ENVÍOS POR MB / AGENTE (últimas 4 semanas)
--    'sent' = primer email; los retry/secondary/re_sent son envíos extra.
-- ════════════════════════════════════════════════════════════════
SELECT
  user_email                                                          AS mb,
  COUNT(*) FILTER (WHERE action = 'sent')                             AS enviados_1er,
  COUNT(*) FILTER (WHERE action = 'secondary_sent')                  AS enviados_2do,
  COUNT(*) FILTER (WHERE action = 'bounce_retry_sent')              AS reenvios_por_rebote,
  COUNT(*) FILTER (WHERE action = 're_sent')                         AS re_engagement,
  COUNT(*) FILTER (WHERE action = 'skipped')                         AS saltados,
  COUNT(*) FILTER (WHERE action = 'failed')                          AS fallidos
FROM public.toolbar_agent_actions
WHERE created_at >= now() - interval '28 days'
GROUP BY user_email
ORDER BY enviados_1er DESC;


-- ════════════════════════════════════════════════════════════════
-- 2. ENVÍOS POR DÍA — para ver el ritmo y si el cap (ahora 10/MB) se respeta
-- ════════════════════════════════════════════════════════════════
SELECT
  DATE(created_at)                                                    AS dia,
  COUNT(*) FILTER (WHERE action = 'sent')                             AS enviados
FROM public.toolbar_agent_actions
WHERE created_at >= now() - interval '28 days'
GROUP BY DATE(created_at)
ORDER BY dia DESC;


-- ════════════════════════════════════════════════════════════════
-- 3. FUENTE DEL DATO (scrape vs apollo vs informer/social/generic)
--    + tasa de respuesta REAL por fuente. Fuente canónica:
--    toolbar_response_tracking.source. response_type='real' = reply humano.
-- ════════════════════════════════════════════════════════════════
SELECT
  COALESCE(NULLIF(source, ''), 'unknown')                            AS fuente_del_dato,
  COUNT(*)                                                            AS enviados,
  COUNT(*) FILTER (WHERE response_type = 'real')                     AS respondieron_real,
  COUNT(*) FILTER (WHERE response_type = 'ooo')                      AS auto_reply_ooo,
  ROUND(100.0 * COUNT(*) FILTER (WHERE response_type = 'real')
        / NULLIF(COUNT(*), 0), 1)                                    AS pct_respuesta_real
FROM public.toolbar_response_tracking
WHERE sent_at >= now() - interval '28 days'
GROUP BY 1
ORDER BY enviados DESC;


-- ════════════════════════════════════════════════════════════════
-- 4. REBOTES — cuántos, hard vs soft, y QUÉ SE HIZO con cada uno
--    status: sent = se reenvió a un email alternativo; skipped_no_alt = no
--    había alternativa → el dominio se congeló (freeze); failed = error.
-- ════════════════════════════════════════════════════════════════
SELECT
  bounce_type,
  status,
  COUNT(*)                                                            AS cantidad
FROM public.toolbar_bounce_retries
WHERE created_at >= now() - interval '28 days'
GROUP BY bounce_type, status
ORDER BY cantidad DESC;


-- ════════════════════════════════════════════════════════════════
-- 5. ¿DE QUÉ FUENTE SALÍA EL EMAIL REENVIADO tras un rebote?
--    (sirve para ver si el scrape/apollo de rescate funciona)
-- ════════════════════════════════════════════════════════════════
SELECT
  COALESCE(NULLIF(retry_source, ''), 'unknown')                     AS fuente_reenvio,
  COUNT(*)                                                            AS reintentos,
  COUNT(*) FILTER (WHERE status = 'sent')                            AS reenviados_ok
FROM public.toolbar_bounce_retries
WHERE created_at >= now() - interval '28 days'
GROUP BY 1
ORDER BY reintentos DESC;


-- ════════════════════════════════════════════════════════════════
-- 6. EMBUDO GLOBAL (los números grandes, últimas 4 semanas)
-- ════════════════════════════════════════════════════════════════
SELECT
  (SELECT COUNT(*) FROM public.toolbar_agent_actions
     WHERE action = 'sent' AND created_at >= now() - interval '28 days')        AS total_enviados,
  (SELECT COUNT(*) FROM public.toolbar_response_tracking
     WHERE response_type = 'real' AND sent_at >= now() - interval '28 days')    AS respondieron_real,
  (SELECT COUNT(*) FROM public.toolbar_bounce_retries
     WHERE created_at >= now() - interval '28 days')                            AS rebotes_totales,
  (SELECT COUNT(*) FROM public.toolbar_bounce_retries
     WHERE status = 'sent' AND created_at >= now() - interval '28 days')        AS rebotes_rescatados,
  (SELECT COUNT(*) FROM public.toolbar_bounce_retries
     WHERE status = 'skipped_no_alt' AND created_at >= now() - interval '28 days') AS rebotes_sin_alternativa;


-- ════════════════════════════════════════════════════════════════
-- 7. TASA DE RESPUESTA POR MB (para comparar performance entre MBs/agente)
-- ════════════════════════════════════════════════════════════════
SELECT
  mb_email                                                            AS mb,
  COUNT(*)                                                            AS enviados,
  COUNT(*) FILTER (WHERE response_type = 'real')                     AS respondieron,
  ROUND(100.0 * COUNT(*) FILTER (WHERE response_type = 'real')
        / NULLIF(COUNT(*), 0), 1)                                    AS pct_respuesta
FROM public.toolbar_response_tracking
WHERE sent_at >= now() - interval '28 days'
GROUP BY mb_email
ORDER BY enviados DESC;
