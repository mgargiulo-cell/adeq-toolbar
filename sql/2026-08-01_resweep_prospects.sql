-- ════════════════════════════════════════════════════════════════════════════════════
-- RE-BARRIDO COMPLETO DE PROSPECTS (2026-08-01) — pedido de Maxi:
-- "siguen habiendo varias url que no tienen ads.txt y que son de bancos, ong,
--  universidades, y el filtro no es del todo certero"
--
-- Qué cambió en el worker (deploy de hoy, scoreProspectable):
--   1. VETO POR CATEGORÍA, sin puntuar. Si SimilarWeb dice banca, seguros, e-commerce,
--      educación, gobierno, religión o beneficencia → se va ahí mismo. Antes eso solo se
--      miraba para los sitios que nos bloqueaban el scraper; el resto llegaba al score.
--   2. La "categoría de medios" que sumaba +25 la calculaba el scraper buscando PALABRAS en
--      la home → la de un banco caía en "finance", la de una clínica en "health". Se sacaron
--      finance/technology/health/travel/automotive/business/gambling de esa lista.
--   3. IA dice "other" + sin ads.txt → afuera, sin puntuar (antes "other" se dejaba pasar).
--   4. Sin ads.txt confirmado hace falta veredicto POSITIVO de medio. Antes alcanzaba con UN
--      script publicitario en la home (display ads = 30 pts = el mínimo justo).
--   Lo que no se pudo verificar NO se descarta: se reintenta (nunca tiramos por falla nuestra).
--
-- Este script vuelve a pasar TODO el pool por el filtro nuevo. Los barridos ya existen en el
-- worker pero se auto-apagan al terminar y guardan un cursor: hay que resetear el cursor,
-- si no arrancan donde quedaron y no re-leen lo viejo.
-- Nada se borra: lo que cae va a status='rejected' con el motivo, auditable y reversible.
-- ════════════════════════════════════════════════════════════════════════════════════


-- ── 0) FOTO ANTES (para comparar después) ────────────────────────────────────────────
SELECT COUNT(*) AS pending_ahora FROM toolbar_review_queue WHERE status = 'pending';

-- Sospechosos institucionales que HOY están en Prospects (esto es lo que Maxi ve).
-- Es un rastreo por nombre: no es el filtro, es para dimensionar el problema.
SELECT
  CASE
    WHEN domain ~* '(^|\.)(edu|ac)(\.|$)|\.edu\.|\.ac\.|universi|college|escuela|hochschule|facultad' THEN 'universidad/edu'
    WHEN domain ~* '(^|\.)(gov|gob|mil|gouv)(\.|$)|\.gov\.|\.gob\.'                                    THEN 'gobierno'
    WHEN domain ~* 'bank|banco|banca|caixa|credit|seguros|insurance|assurance|versicherung'            THEN 'banco/seguros'
    WHEN domain ~* '\.org$|\.ong$|fundacion|foundation|charity|ngo'                                    THEN 'ong/fundación'
    ELSE 'otro'
  END AS tipo,
  COUNT(*) AS n
FROM toolbar_review_queue
WHERE status = 'pending'
GROUP BY 1
ORDER BY 2 DESC;

-- El detalle, para mirar a ojo antes de barrer:
SELECT domain, category, traffic, geo, source, created_at
FROM toolbar_review_queue
WHERE status = 'pending'
  AND (domain ~* '(^|\.)(edu|ac)(\.|$)|\.edu\.|\.ac\.|universi|college|facultad'
    OR domain ~* '(^|\.)(gov|gob|mil|gouv)(\.|$)|\.gov\.|\.gob\.'
    OR domain ~* 'bank|banco|banca|caixa|seguros|insurance|assurance'
    OR domain ~* '\.org$|fundacion|foundation|charity')
ORDER BY traffic DESC NULLS LAST
LIMIT 300;


-- ── 0.bis) SEGURO DE GASTO ───────────────────────────────────────────────────────────
-- Pedido de Maxi: "no quiero que gaste Apollo ni SimilarWeb, solo que recategorice y
-- elimine lo que no cumple".
-- Los dos barridos de abajo NO gastan Apollo ni SimilarWeb — bajan la home y el ads.txt
-- (fetch directo, gratis) y usan la categoría de SimilarWeb YA GUARDADA en la fila. Lo
-- único que cuesta es Claude Haiku, y solo para los dudosos: ~USD 0,001 por dominio,
-- o sea menos de USD 2 por todo el pool.
-- El ÚNICO job que puede gastar Apollo es polishPool (el que busca emails). No lo prende
-- este script, pero si quedó encendido de antes correría en paralelo — así que lo apagamos
-- explícitamente antes de arrancar. Volvé a prenderlo cuando quieras buscar emails.
INSERT INTO toolbar_config (key, value) VALUES
  ('polish_pool',        'false'),   -- apaga el buscador de emails (es el que usa Apollo)
  ('polish_use_apollo',  'false')    -- y por las dudas, le saca Apollo aunque lo prendan
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Verificar que quedó apagado ANTES de seguir:
SELECT key, value FROM toolbar_config
WHERE key IN ('polish_pool', 'polish_use_apollo');


-- ── 1) RESETEAR CURSORES Y ENCENDER LOS DOS BARRIDOS ─────────────────────────────────
-- Orden en que corren dentro del worker:
--   a) url_purge_enabled       → barrido GRATIS por URL (sin red, sin créditos, miles/segundo)
--   b) purge_blocked_prospects → barrido CARO: 1 scrape por dominio + ads.txt + IA solo si hace
--                                falta. Es el que aplica el filtro nuevo. NO gasta SimilarWeb
--                                ni Apollo (usa la categoría ya guardada en la fila).
-- Los cursores en '' obligan a empezar desde el lead más nuevo y recorrer TODO otra vez.
INSERT INTO toolbar_config (key, value) VALUES
  ('url_purge_cursor',          ''),
  ('url_purge_enabled',         'true'),
  ('purge_cursor_ts',           ''),
  ('purge_blocked_prospects',   'true')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Para frenarlo en cualquier momento:
--   UPDATE toolbar_config SET value='false'
--   WHERE key IN ('url_purge_enabled','purge_blocked_prospects');


-- ── 2) MONITOREO (correr cada tanto mientras barre) ──────────────────────────────────
-- El barrido caro va de a 60 leads por ciclo → con ~1.600 pending son unas pocas horas.
SELECT
  (SELECT value FROM toolbar_config WHERE key = 'purge_blocked_prospects') AS barrido_caro_on,
  (SELECT value FROM toolbar_config WHERE key = 'purge_cursor_ts')         AS cursor_caro,
  (SELECT value FROM toolbar_config WHERE key = 'url_purge_enabled')       AS barrido_url_on,
  (SELECT COUNT(*) FROM toolbar_review_queue WHERE status = 'pending')     AS pending_restantes;

-- Motivos de descarte, agrupados (acá se ve si el filtro nuevo está pegando):
SELECT
  regexp_replace(suspect_reason, ':.*$', '') AS motivo,
  COUNT(*) AS n
FROM toolbar_review_queue
WHERE status = 'rejected'
  AND (suspect_reason LIKE 'purge:%' OR suspect_reason LIKE 'urlpurge:%')
  AND created_at > now() - interval '7 days'
GROUP BY 1
ORDER BY 2 DESC;


-- ── 3) AUDITORÍA — lo más importante: ¿se voló algún medio real? ─────────────────────
-- Mirar sobre todo los de tráfico alto descartados por el motivo NUEVO.
SELECT domain, suspect_reason, category, traffic, geo
FROM toolbar_review_queue
WHERE status = 'rejected'
  AND (suspect_reason LIKE '%categoria_no_publisher%'          -- veto por categoría de SimilarWeb
    OR suspect_reason LIKE '%ia_other_sin_ads_txt%'            -- la IA no lo ve como medio y no hay ads.txt
    OR suspect_reason LIKE '%sin_ads_txt_ni_evidencia_de_medio%')
ORDER BY traffic DESC NULLS LAST
LIMIT 200;


-- ── 4) RESCATE (si en la auditoría aparece un medio real mal descartado) ─────────────
-- UPDATE toolbar_review_queue
-- SET status='pending', suspect_reject=false, suspect_reason=NULL
-- WHERE domain IN ('el-que-se-colo.com');


-- ══════════════════════════════════════════════════════════════════════════════════════
-- 5) DESPUÉS DEL BARRIDO: BUSCAR EMAILS DE LOS QUE SOBREVIVIERON
-- ══════════════════════════════════════════════════════════════════════════════════════
-- ⚠️ CORRER ESTO RECIÉN CUANDO EL BARRIDO TERMINÓ (paso 2 muestra barrido_caro_on='false').
-- Orden importante: primero limpiar, después buscar emails. Al revés se gastan créditos
-- buscándole el mail a bancos y universidades que después vamos a tirar igual.
--
-- Este job (polishPool) SÍ gasta:
--   · scrape del sitio + website-informer  → gratis
--   · Google (Serper) como fallback         → cuenta contra el cap mensual de Serper
--   · Apollo                                → créditos, OPCIONAL (dejalo en 'false' si no querés)
-- Nunca usa RapidAPI/SimilarWeb.

-- Cuántos quedaron sin email (para saber si vale la pena):
SELECT
  COUNT(*) FILTER (WHERE emails IS NULL OR emails::text IN ('[]','null','')) AS sin_email,
  COUNT(*)                                                                   AS total_pending
FROM toolbar_review_queue WHERE status = 'pending';

-- Encender la búsqueda de emails (se auto-apaga al terminar):
-- INSERT INTO toolbar_config (key, value) VALUES
--   ('polish_cursor_ts',    ''),
--   ('polish_use_apollo',   'false'),   -- 'true' si querés permitir Apollo (gasta créditos)
--   ('polish_pool',         'true')
-- ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Monitoreo mientras busca emails:
-- SELECT
--   (SELECT value FROM toolbar_config WHERE key='polish_pool')      AS buscando_emails,
--   (SELECT value FROM toolbar_config WHERE key='polish_cursor_ts') AS cursor,
--   COUNT(*) FILTER (WHERE emails IS NULL OR emails::text IN ('[]','null','')) AS sin_email_aun
-- FROM toolbar_review_queue WHERE status='pending';
