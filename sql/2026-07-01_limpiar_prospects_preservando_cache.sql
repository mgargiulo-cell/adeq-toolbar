-- ════════════════════════════════════════════════════════════════
-- LIMPIAR PROSPECTS + DESTAPAR COLA, SIN REPROCESAR — Maxi 2026-07-01
-- ────────────────────────────────────────────────────────────────
-- OBJETIVO (pedido del user): dejar Prospects limpio y la cola destapada, SIN reprocesar
-- los dominios actuales. Solo entran cargas NUEVAS que haga el feeder o los MB.
-- Los caches se preservan → si una web nueva ya fue vista antes (≤90 días), usa el
-- tráfico cacheado (no re-gasta RapidAPI), busca el email y se re-clasifica bien la fuente.
--
-- SE PRESERVA (NO se toca):
--   ✓ toolbar_traffic_cache   → visitas reales cacheadas (evita re-gasto RapidAPI)
--   ✓ toolbar_apollo_cache    → contactos Apollo cacheados
--   ✓ toolbar_sendtrack       → emails YA enviados (no se re-emailea)
--   ✓ toolbar_url_blocklist   → basura bloqueada a mano
--   ✓ toolbar_config, response_tracking, agent_actions, bounces
--   ✓ review_queue validated + rejected → enviados + aprendizaje de descartes
--   ✓ csv_queue done/skipped/error + historial → HUELLA de "ya conocido": así los
--     dominios viejos NO se re-descubren ni se reprocesan (justo lo que pediste).
--
-- SE LIMPIA (solo esto):
--   • review_queue PENDING → la lista de Prospects arranca vacía
--   • csv_queue backlog (pending/processing/waiting_pool/next_day/frozen) → destapa la
--     cola (el clog de ~1886). Estos NO estaban procesados; al borrarlos no se pierde
--     trabajo hecho, y al mantener la huella done/skipped no vuelven a entrar solos.
--
-- ⚠️ Correr el PREVIEW (paso 0) PRIMERO. Después los 2 DELETE.
-- ════════════════════════════════════════════════════════════════


-- ── 0. PREVIEW (no borra nada) ──────────────────────────────────
SELECT 'review_queue PENDING (se borra)'         AS que, COUNT(*) AS filas FROM toolbar_review_queue WHERE status = 'pending'
UNION ALL SELECT 'csv_queue backlog (se borra)',  COUNT(*) FROM toolbar_csv_queue WHERE status IN ('pending','processing','waiting_pool','next_day','frozen')
UNION ALL SELECT 'csv_queue done/skipped (SE PRESERVA = huella)', COUNT(*) FROM toolbar_csv_queue WHERE status IN ('done','skipped','error')
UNION ALL SELECT 'traffic_cache (SE PRESERVA)',   COUNT(*) FROM toolbar_traffic_cache
UNION ALL SELECT 'sendtrack (SE PRESERVA)',        COUNT(*) FROM toolbar_sendtrack;


-- ── 1. Limpiar Prospects (solo pending; NO toca validated/rejected) ──
DELETE FROM toolbar_review_queue WHERE status = 'pending';


-- ── 2. Destapar la cola: borrar SOLO el backlog no procesado ────
--     (NO se tocan done/skipped/error → su huella evita re-descubrimiento)
DELETE FROM toolbar_csv_queue
WHERE status IN ('pending','processing','waiting_pool','next_day','frozen');


-- ── 3. Verificación final ───────────────────────────────────────
SELECT 'review_queue pending' AS tabla, COUNT(*) AS quedan FROM toolbar_review_queue WHERE status = 'pending'
UNION ALL SELECT 'csv_queue backlog', COUNT(*) FROM toolbar_csv_queue WHERE status IN ('pending','processing','waiting_pool','next_day','frozen')
UNION ALL SELECT 'csv_queue huella (done/skipped)', COUNT(*) FROM toolbar_csv_queue WHERE status IN ('done','skipped','error')
UNION ALL SELECT 'traffic_cache (intacto)', COUNT(*) FROM toolbar_traffic_cache;
