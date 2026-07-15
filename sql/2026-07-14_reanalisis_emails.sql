-- ════════════════════════════════════════════════════════════════════════
-- RE-ANÁLISIS DE EMAILS del pool (2026-07-14) — pedido del user: re-leer TODOS los
-- prospects sin email (o solo genéricos) con scrape + informer + Apollo, ahora que:
--   • se arregló la cobertura de contacto portugués BR (/contato, fale-conosco) — caso sorteador.com.br
--   • el re-enrich barre TODO el pool por cursor (antes se apagaba antes de terminar)
--   • más rápido: 20 leads/run cada 2 min (scrape gratis siempre; Apollo forzado si el scrape viene vacío,
--     respetando el cap diario 150/mensual 2400).
-- ════════════════════════════════════════════════════════════════════════

-- ── 1) ENCENDER el re-análisis (empieza de cero y barre todo el pool) ──
INSERT INTO toolbar_config (key, value) VALUES ('reenrich_cursor_ts', '')
ON CONFLICT (key) DO UPDATE SET value = '';
INSERT INTO toolbar_config (key, value) VALUES ('agent_reenrich_bad_leads', 'true')
ON CONFLICT (key) DO UPDATE SET value = 'true';
-- Se AUTO-APAGA solo al terminar el pool. Para pararlo antes:
-- UPDATE toolbar_config SET value='false' WHERE key='agent_reenrich_bad_leads';


-- ── 2) MONITOREAR (correr cada tanto — el número debería BAJAR) ──
-- Prospects pending SIN email (los que el re-análisis está tratando de resolver):
SELECT COUNT(*) AS pending_sin_email
FROM toolbar_review_queue
WHERE status='pending' AND (emails IS NULL OR emails::text IN ('{}', '[]', 'null', ''));

-- ¿Hasta dónde avanzó el barrido? (timestamp del cursor; vacío = terminó o no arrancó)
SELECT value AS reenrich_cursor FROM toolbar_config WHERE key='reenrich_cursor_ts';


-- ── 3) VER qué emails NUEVOS encontró (últimos resueltos, con su fuente) ──
-- Los que ahora tienen email y de qué fuente (scrape/apollo/informer):
SELECT domain, emails, email_sources, traffic
FROM toolbar_review_queue
WHERE status='pending'
  AND emails IS NOT NULL AND emails::text NOT IN ('{}', '[]', 'null', '')
ORDER BY updated_at DESC NULLS LAST
LIMIT 100;
