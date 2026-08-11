-- ══════════════════════════════════════════════════════════════════════════════
-- DIAGNÓSTICO BASE — 2026-08-11
-- Números reales para anclar el plan de la mega-evolución.
-- Corré los 3 bloques y pegame las salidas. No modifica nada: todo es SELECT.
-- ══════════════════════════════════════════════════════════════════════════════


-- ── BLOQUE A · QUÉ COLUMNAS EXISTEN DE VERDAD ────────────────────────────────
-- (para no volver a escribir SQL contra columnas adivinadas)
SELECT table_name, string_agg(column_name, ', ' ORDER BY ordinal_position) AS columnas
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name IN (
     'toolbar_review_queue', 'toolbar_agent_actions', 'toolbar_response_tracking',
     'toolbar_autopilot_feedback', 'toolbar_feeder_runs', 'toolbar_csv_queue',
     'toolbar_bounced_emails', 'toolbar_mv_results', 'toolbar_keyword_yield',
     'toolbar_email_opens', 'toolbar_source_performance'
   )
 GROUP BY table_name
 ORDER BY table_name;


-- ── BLOQUE B · DE QUÉ ESTÁ HECHO EL POOL ─────────────────────────────────────
-- Cuántos candidatos hay realmente listos para enviar, y de dónde vinieron.
SELECT coalesce(source, '(sin fuente)')                                  AS fuente,
       count(*)                                                          AS total,
       count(*) FILTER (WHERE status = 'pending')                        AS pendientes,
       count(*) FILTER (WHERE status = 'pending'
                          AND jsonb_array_length(coalesce(emails,'[]'::jsonb)) > 0
                          AND coalesce(suspect_reject, false) = false)   AS listos_para_enviar,
       count(*) FILTER (WHERE status = 'pending'
                          AND jsonb_array_length(coalesce(emails,'[]'::jsonb)) = 0) AS pendientes_sin_email,
       count(*) FILTER (WHERE coalesce(suspect_reject, false))           AS rechazados
  FROM toolbar_review_queue
 GROUP BY 1
 ORDER BY listos_para_enviar DESC NULLS LAST;


-- ── BLOQUE C · EL TERMÓMETRO QUE NADIE MIRA ──────────────────────────────────
-- ¿Se está alimentando el tracking de respuestas? ¿Y el aprendizaje de las X?
SELECT 'respuestas registradas'          AS que, count(*)::text AS cuanto FROM toolbar_response_tracking
UNION ALL
SELECT 'respuestas REALES (no OOO)',     count(*)::text FROM toolbar_response_tracking WHERE response_type = 'real'
UNION ALL
SELECT 'rechazos X del media buyer',     count(*)::text FROM toolbar_autopilot_feedback WHERE action = 'disliked'
UNION ALL
SELECT 'firmas X con 3+ rechazos',       count(*)::text FROM (
         SELECT 1 FROM toolbar_autopilot_feedback
          WHERE action = 'disliked'
          GROUP BY user_email, traffic_bucket
         HAVING count(*) >= 3) s;
