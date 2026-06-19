-- ════════════════════════════════════════════════════════════════
-- RE-ABRIR DOMINIOS PARA RE-DESCUBRIMIENTO — Maxi 2026-06-19
-- ────────────────────────────────────────────────────────────────
-- PROBLEMA: cuando borrás webs de Prospects (review_queue), el dominio SIGUE
-- en csv_queue (done/skipped/error) y en historial → el dedup del worker lo ve
-- como "ya analizado" y NO lo vuelve a traer. Esto NO lo arregla wipe_prospects.sql.
--
-- ESTE SCRIPT: quita la huella de "ya conocido" en csv_queue + historial SOLO para
-- dominios que NUNCA se contactaron (no están en sendtrack) y NO están blocklisteados.
-- Así el feeder puede re-evaluarlos (ahora con el código nuevo: páginas vistas
-- correctas + filtro de basura + 2º chequeo de tráfico).
--
-- PRESERVA: sendtrack (emails enviados), url_blocklist (bloqueados a mano),
-- review_queue validados/rechazados, configs, Apollo/RapidAPI.
--
-- ⚠️ Correr DESPUÉS de haber deployado el código nuevo, si no, re-evalúa con los
--    bugs viejos. Y correr el PREVIEW (paso 0) ANTES del DELETE.
-- ════════════════════════════════════════════════════════════════

-- Normalizador de dominio (quita www.) para comparar parejo.
-- (Postgres: usamos lower(regexp_replace(domain,'^www\.','')) inline.)

-- 0. PREVIEW — cuántas filas se re-abrirían. Correr SOLO esto primero.
SELECT 'csv_queue done/skipped/error (re-abribles)' AS tabla, COUNT(*) AS rows
FROM toolbar_csv_queue q
WHERE q.status IN ('done','skipped','error')
  AND lower(regexp_replace(q.domain,'^www\.','')) NOT IN (
        SELECT lower(regexp_replace(domain,'^www\.','')) FROM toolbar_sendtrack)
  AND lower(regexp_replace(q.domain,'^www\.','')) NOT IN (
        SELECT lower(regexp_replace(domain,'^www\.','')) FROM toolbar_url_blocklist)
UNION ALL
SELECT 'historial (re-abribles)', COUNT(*)
FROM toolbar_historial h
WHERE lower(regexp_replace(h.domain,'^www\.','')) NOT IN (
        SELECT lower(regexp_replace(domain,'^www\.','')) FROM toolbar_sendtrack)
  AND lower(regexp_replace(h.domain,'^www\.','')) NOT IN (
        SELECT lower(regexp_replace(domain,'^www\.','')) FROM toolbar_url_blocklist);

-- ────────────────────────────────────────────────────────────────
-- 1. DELETE en csv_queue — quita la huella del worker (lo principal).
--    Corré esto DESPUÉS de revisar el preview.
DELETE FROM toolbar_csv_queue q
WHERE q.status IN ('done','skipped','error')
  AND lower(regexp_replace(q.domain,'^www\.','')) NOT IN (
        SELECT lower(regexp_replace(domain,'^www\.','')) FROM toolbar_sendtrack)
  AND lower(regexp_replace(q.domain,'^www\.','')) NOT IN (
        SELECT lower(regexp_replace(domain,'^www\.','')) FROM toolbar_url_blocklist);

-- 2. (OPCIONAL — más agresivo) DELETE en historial. Solo si querés que dominios
--    analizados MANUALMENTE alguna vez también puedan re-entrar. Descomentá si lo querés.
-- DELETE FROM toolbar_historial h
-- WHERE lower(regexp_replace(h.domain,'^www\.','')) NOT IN (
--         SELECT lower(regexp_replace(domain,'^www\.','')) FROM toolbar_sendtrack)
--   AND lower(regexp_replace(h.domain,'^www\.','')) NOT IN (
--         SELECT lower(regexp_replace(domain,'^www\.','')) FROM toolbar_url_blocklist);

-- 3. (OPCIONAL) También liberar los frozen (sin tráfico) para que se re-evalúen
--    con el 2º chequeo de tráfico nuevo:
-- DELETE FROM toolbar_frozen_leads;

-- 4. VERIFICACIÓN
SELECT COUNT(*) AS csv_queue_procesados_restantes
FROM toolbar_csv_queue WHERE status IN ('done','skipped','error');
