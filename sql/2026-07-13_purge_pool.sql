-- ════════════════════════════════════════════════════════════════════════
-- BARRIDO DEL POOL (2026-07-13) — purga automática de no-publishers viejos de Prospects.
-- El worker (sweepBlockedFromProspects) borra de a 60/ciclo usando SOLO señales de ALTA
-- PRECISIÓN (0 falsos positivos): blocklist curada + detector estructural (ecommerce/banco/
-- edu/gov/travel/realestate). NO usa Haiku ni categoría/tema → no toca publishers reales.
-- No hace hard-DELETE: pasa a status='rejected' + suspect_reason='purge:...' (auditable/reversible).
-- ════════════════════════════════════════════════════════════════════════

-- ── 1) ENCENDER el barrido (el worker lo agarra en el próximo ciclo, ~1 min) ──
INSERT INTO toolbar_config (key, value) VALUES ('purge_blocked_prospects', 'true')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
-- Se AUTO-APAGA solo cuando termina de barrer todo el pool. Para pararlo antes:
-- UPDATE toolbar_config SET value='false' WHERE key='purge_blocked_prospects';


-- ── 2) MONITOREAR el avance (correr cada tanto) ──
-- Cuántos prospects pending quedan (debería ir bajando):
SELECT COUNT(*) AS pending_restantes FROM toolbar_review_queue WHERE status='pending';

-- Cuántos purgó el barrido y por qué motivo:
SELECT split_part(suspect_reason, ' ', 2) AS motivo, COUNT(*) AS n
FROM toolbar_review_queue
WHERE status='rejected' AND suspect_reason LIKE 'purge:%'
GROUP BY 1 ORDER BY 2 DESC;


-- ── 3) AUDITAR lo purgado (verificar que NO se voló ningún publisher real) ──
SELECT domain, suspect_reason, category, traffic
FROM toolbar_review_queue
WHERE status='rejected' AND suspect_reason LIKE 'purge:%'
ORDER BY traffic DESC NULLS LAST
LIMIT 500;


-- ── 4) RESTAURAR (si en la auditoría ves un publisher real mal purgado) ──
-- UPDATE toolbar_review_queue SET status='pending', suspect_reject=false, suspect_reason=NULL
-- WHERE domain IN ('el-que-se-coló.com');   -- ← poné el/los dominios a rescatar
