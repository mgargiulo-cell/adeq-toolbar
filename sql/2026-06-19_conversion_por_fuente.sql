-- ════════════════════════════════════════════════════════════════
-- CONVERSIÓN POR FUENTE — Maxi 2026-06-19
-- ────────────────────────────────────────────────────────────────
-- ¿Qué motor rinde mejor? Cruza lo que cada fuente PUSO en Prospects
-- (toolbar_review_queue.source) con lo que efectivamente se ENVIÓ
-- (toolbar_sendtrack, por dominio). Mayor % = mejor convierte.
-- Fuentes típicas: autopilot, autogoogle, csv, sellers_json, monday_refresh.
-- Correr en Supabase SQL Editor. Recién tiene sentido con varios días de data.
-- ════════════════════════════════════════════════════════════════

SELECT
  COALESCE(rq.source, '(sin source)')                 AS fuente,
  COUNT(DISTINCT rq.domain)                            AS puestos_en_prospects,
  COUNT(DISTINCT st.domain)                            AS enviados,
  ROUND(100.0 * COUNT(DISTINCT st.domain)
        / NULLIF(COUNT(DISTINCT rq.domain), 0), 1)     AS pct_enviados
FROM public.toolbar_review_queue rq
LEFT JOIN public.toolbar_sendtrack st
       ON lower(regexp_replace(st.domain, '^www\.', '')) = lower(regexp_replace(rq.domain, '^www\.', ''))
GROUP BY COALESCE(rq.source, '(sin source)')
ORDER BY pct_enviados DESC NULLS LAST;

-- Opcional: acotá a un rango de fechas agregando antes del GROUP BY:
--   WHERE rq.created_at >= '2026-06-19'
