-- ════════════════════════════════════════════════════════════════
-- BACKFILL source en toolbar_review_queue — Maxi 2026-06-18
-- ────────────────────────────────────────────────────────────────
-- Bug: processCsvItem hardcodeaba source="csv" para TODO lo que venía
-- del csv_queue. Por eso 693 leads aparecen como CSV cuando en realidad
-- son sellers_json, autopilot (majestic) o monday_refresh.
--
-- Este SQL toma el source REAL del csv_queue y lo aplica al review_queue.
-- Solo toca rows con source="csv" (no pisa los que ya están correctos).
-- ════════════════════════════════════════════════════════════════

-- 1. Preview de qué se va a cambiar
SELECT
  cq.source AS csv_queue_source,
  COUNT(DISTINCT rq.id) AS leads_a_actualizar
FROM toolbar_review_queue rq
JOIN LATERAL (
  SELECT source FROM toolbar_csv_queue
  WHERE domain = rq.domain
  ORDER BY uploaded_at DESC NULLS LAST
  LIMIT 1
) cq ON true
WHERE rq.source = 'csv'
  AND cq.source LIKE 'auto_feeder_%'
GROUP BY cq.source;

-- 2. UPDATE — corre esto después de revisar el preview
UPDATE toolbar_review_queue rq
SET source = CASE last_cq.source
  WHEN 'auto_feeder_sellers'  THEN 'sellers_json'
  WHEN 'auto_feeder_majestic' THEN 'autopilot'
  WHEN 'auto_feeder_monday'   THEN 'monday_refresh'
  ELSE rq.source
END
FROM (
  SELECT DISTINCT ON (domain) domain, source
  FROM toolbar_csv_queue
  WHERE source LIKE 'auto_feeder_%'
  ORDER BY domain, uploaded_at DESC NULLS LAST
) last_cq
WHERE rq.domain = last_cq.domain
  AND rq.source = 'csv';

-- 3. Verificación — distribución después del update
SELECT source, COUNT(*) AS leads
FROM toolbar_review_queue
WHERE status = 'pending'
GROUP BY source
ORDER BY leads DESC;
