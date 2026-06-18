-- ════════════════════════════════════════════════════════════════
-- WIPE COMPLETO de PROSPECTS — Maxi 2026-06-18
-- ────────────────────────────────────────────────────────────────
-- Borra TODO lo que está disponible para prospectar (review_queue pending)
-- y limpia la cola del worker (csv_queue pending/waiting) para arrancar
-- desde cero. NO toca historial, sent, bounces, blocklist, ni configs.
-- ════════════════════════════════════════════════════════════════

-- 0. PREVIEW — qué va a borrar. Correr SOLO esto primero.
SELECT
  'review_queue pending' AS tabla,
  COUNT(*) AS rows_a_borrar
FROM toolbar_review_queue
WHERE status = 'pending'
UNION ALL
SELECT
  'csv_queue pendiente/waiting/processing',
  COUNT(*)
FROM toolbar_csv_queue
WHERE status IN ('pending','waiting_pool','next_day','processing');

-- ────────────────────────────────────────────────────────────────
-- LO QUE SE PRESERVA (NO se toca):
-- ✓ toolbar_send_track          — emails enviados / bounces / opens
-- ✓ toolbar_historial           — leads ya prospectados (cualquier estado)
-- ✓ toolbar_review_queue        — leads validados o rechazados (status != pending)
-- ✓ toolbar_response_tracking   — respuestas reales / OOO
-- ✓ toolbar_blocklist           — dominios bloqueados
-- ✓ toolbar_frozen_leads        — leads en backoff por traffic=0
-- ✓ toolbar_autopilot_feedback  — X-learn dislikes
-- ✓ toolbar_user_snoozed_prospects — snoozes activos
-- ✓ toolbar_config              — caps, Apollo, RapidAPI, todo
-- ✓ toolbar_csv_queue status=done/skipped/error — historial de la cola
-- ────────────────────────────────────────────────────────────────

-- 1. DELETE — corre esto DESPUÉS de revisar el preview
DELETE FROM toolbar_review_queue
WHERE status = 'pending';

DELETE FROM toolbar_csv_queue
WHERE status IN ('pending','waiting_pool','next_day','processing');

-- 2. VERIFICACIÓN — ambos deben dar 0
SELECT COUNT(*) AS prospects_disponibles FROM toolbar_review_queue WHERE status = 'pending';
SELECT COUNT(*) AS cola_worker            FROM toolbar_csv_queue   WHERE status IN ('pending','waiting_pool','next_day','processing');

-- 3. (OPCIONAL) Resetear el contador diario del feeder para que arranque ya
-- mismo (sin esperar al próximo slot horario):
-- DELETE FROM toolbar_config WHERE key = 'feeder_last_cron_at';
