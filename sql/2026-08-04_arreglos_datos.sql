-- ══════════════════════════════════════════════════════════════════════════════════════════
-- ARREGLOS DE DATOS — 2026-08-04
-- Corré los 3 bloques de una. Ninguno borra nada: solo devuelven leads a 'pending' para que
-- el worker los vuelva a juzgar con el código nuevo.
-- ══════════════════════════════════════════════════════════════════════════════════════════


-- ── 1. MOTIVOS DE PURGA MAL ETIQUETADOS ───────────────────────────────────────────────────
-- El bug del lote (commit 5e0d4df) escribía el motivo de la PRIMERA fila a las 50 del lote.
-- La DECISIÓN de purgar cada dominio fue individual y correcta; la ETIQUETA quedó mezclada,
-- por eso fandom.com figura bajo "casa de apuestas" y zeit.de bajo "placeholder".
-- Se devuelven a pending y el barrido por URL (gratis, sin red) los vuelve a juzgar y esta vez
-- escribe el motivo de a uno.
UPDATE toolbar_review_queue
   SET status = 'pending', suspect_reject = false, suspect_reason = NULL
 WHERE suspect_reason LIKE 'urlpurge:%';

-- Ver cuántos volvieron
SELECT count(*) AS devueltos_a_pending FROM toolbar_review_queue
 WHERE status = 'pending' AND suspect_reason IS NULL;


-- ── 2. LOS 120 DESCARTADOS POR "ads.txt no verificable" ───────────────────────────────────
-- Se juzgaron ANTES de que existiera el veredicto por SimilarWeb. Que no hayamos podido LEER
-- el ads.txt (Cloudflare, timeout) no prueba que el sitio no monetice — es una falla nuestra,
-- el mismo error que congeló 2.011 leads por cuota de API.
UPDATE toolbar_review_queue
   SET status = 'pending', suspect_reject = false, suspect_reason = NULL
 WHERE suspect_reason LIKE '%ads.txt no verificable%'
    OR suspect_reason LIKE '%sin_evidencia_monetizacion%';


-- ── 3. PRENDER LOS DOS JOBS ───────────────────────────────────────────────────────────────
-- url_purge_enabled  → barrido por URL, gratis, sin red, sin créditos. Se auto-apaga.
-- adstxt_recheck_enabled → reintenta 1×/día los ads.txt que no se pudieron leer.
INSERT INTO toolbar_config (key, value) VALUES
  ('url_purge_enabled',      'true'),
  ('adstxt_recheck_enabled', 'true')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;


-- ══════════════════════════════════════════════════════════════════════════════════════════
-- CHEQUEO — correlo mañana para ver cómo quedó
-- ══════════════════════════════════════════════════════════════════════════════════════════
-- SELECT status, split_part(coalesce(suspect_reason,'(sin motivo)'), ' ', 1) AS motivo, count(*)
--   FROM toolbar_review_queue GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 30;
--
-- Leads pendientes CON y SIN email (el repaso ataca los "sin"):
-- SELECT jsonb_array_length(coalesce(emails,'[]'::jsonb)) > 0 AS tiene_email, count(*)
--   FROM toolbar_review_queue WHERE status = 'pending' GROUP BY 1;
