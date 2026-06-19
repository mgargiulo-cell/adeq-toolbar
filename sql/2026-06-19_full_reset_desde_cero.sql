-- ════════════════════════════════════════════════════════════════
-- RESET TOTAL "DESDE CERO" — Maxi 2026-06-19
-- ────────────────────────────────────────────────────────────────
-- Borra TODO lo analizado/encolado para que los imports nuevos funcionen y el
-- sistema arranque limpio. Cada dominio se vuelve a evaluar con el CÓDIGO NUEVO
-- (páginas vistas correctas + filtro de basura + 2º chequeo de tráfico).
--
-- ⚠️ CORRER DESPUÉS de deployar el código nuevo. Si no, re-evalúa con los bugs viejos.
-- ⚠️ Correr el PREVIEW (paso 0) ANTES de los DELETE.
--
-- NO filtramos por 350K a propósito: una web rechazada antes por "350K VISITAS"
-- puede tener 350K+ PÁGINAS VISTAS y ahora sí calificar. Borramos todo y el código
-- nuevo decide de nuevo. El cache de tráfico NO se toca (guarda visitas reales = ok;
-- el bug estaba en cómo se comparaba/guardaba después, no en el cache).
-- ════════════════════════════════════════════════════════════════

-- 0. PREVIEW — qué se va a borrar. Correr SOLO esto primero.
SELECT 'csv_queue (TODOS los status)'              AS tabla, COUNT(*) AS rows FROM toolbar_csv_queue
UNION ALL SELECT 'review_queue pending (prospects de hoy)', COUNT(*) FROM toolbar_review_queue WHERE status = 'pending'
UNION ALL SELECT 'historial (análisis desde el inicio)',    COUNT(*) FROM toolbar_historial
UNION ALL SELECT 'frozen_leads (sin tráfico)',              COUNT(*) FROM toolbar_frozen_leads
UNION ALL SELECT 'blocklist AUTO (inoperativo) — se borra', COUNT(*) FROM toolbar_url_blocklist WHERE category = 'inoperativo' OR added_by = 'worker_auto'
UNION ALL SELECT 'sendtrack (SE PRESERVA — emails enviados)', COUNT(*) FROM toolbar_sendtrack
UNION ALL SELECT 'blocklist MANUAL (SE PRESERVA)',          COUNT(*) FROM toolbar_url_blocklist WHERE NOT (category = 'inoperativo' OR added_by = 'worker_auto');

-- ────────────────────────────────────────────────────────────────
-- 1. Cola del worker entera → nada queda "conocido" acá (esto es lo que hacía
--    que las webs borradas se vieran como "ya analizadas").
DELETE FROM toolbar_csv_queue;

-- 2. Prospects disponibles de hoy (pending).
DELETE FROM toolbar_review_queue WHERE status = 'pending';

-- 3. Historial de análisis desde el inicio (la otra fuente del "ya analizado").
DELETE FROM toolbar_historial;

-- 4. Liberar frozen (sin tráfico) → se re-evalúan con el 2º chequeo nuevo (Hypestat).
DELETE FROM toolbar_frozen_leads;

-- 5. Quitar SOLO el auto-blocklist 'inoperativo' (lo que el bug viejo bloqueó por
--    no poder medir tráfico). PRESERVA los bloqueos hechos a mano.
DELETE FROM toolbar_url_blocklist WHERE category = 'inoperativo' OR added_by = 'worker_auto';

-- ────────────────────────────────────────────────────────────────
-- SE PRESERVA A PROPÓSITO:
-- ✓ toolbar_sendtrack          — emails YA enviados. Si lo borrás, el sistema
--   podría RE-EMAILEAR a contactos ya tocados (malo para reputación). Como sigue
--   en el dedup, los dominios ya emaileados NO se re-importan (correcto).
-- ✓ toolbar_traffic_cache      — guarda VISITAS reales (correcto). No hace falta
--   borrarlo; borrarlo solo gastaría créditos RapidAPI para reconstruirlo.
-- ✓ review_queue validated/rejected — el registro de lo ya decidido.
-- ✓ Apollo/RapidAPI counters, configs, feedback.
-- ────────────────────────────────────────────────────────────────

-- 6. (OPCIONAL) Resetear contadores del feeder para que arranque YA mismo:
-- DELETE FROM toolbar_config WHERE key IN ('feeder_last_cron_at','last_source_perf_run','autopilot_last_slot');

-- 7. (OPCIONAL Y AGRESIVO — NO recomendado) Si querés borrar TAMBIÉN los prospects
--    validados/rechazados y el registro de emails enviados (arranque 100% en blanco,
--    con riesgo de re-emailear). Descomentá SOLO si estás seguro:
-- DELETE FROM toolbar_review_queue;       -- borra validados + rechazados también
-- DELETE FROM toolbar_sendtrack;          -- ⚠️ permite re-emailear contactos viejos

-- 8. VERIFICACIÓN — los 4 deben dar 0.
SELECT 'csv_queue'            AS tabla, COUNT(*) AS quedan FROM toolbar_csv_queue
UNION ALL SELECT 'prospects pending', COUNT(*) FROM toolbar_review_queue WHERE status = 'pending'
UNION ALL SELECT 'historial',         COUNT(*) FROM toolbar_historial
UNION ALL SELECT 'frozen',            COUNT(*) FROM toolbar_frozen_leads;
