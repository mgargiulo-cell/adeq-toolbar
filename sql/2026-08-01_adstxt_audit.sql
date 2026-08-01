-- ════════════════════════════════════════════════════════════════════════════════════
-- AUDITORÍA DE ads.txt (2026-08-01, pedido de Maxi)
--
-- "quisiera poder auditar los rechazados por error de lectura de txt por un lado, y los
--  que no tienen txt por otro, para poder revisar el listado cada x semanas"
--
-- El problema que resuelve:
--   1. Los dominios que se descartaban en la PUERTA DE ENTRADA (antes de ocupar un slot de
--      cola) no dejaban ninguna fila — solo una línea de log que se pierde con el deploy.
--      No había forma de auditar qué tiramos por ads.txt.
--   2. Los dos motivos estaban mezclados conceptualmente. Son casos MUY distintos:
--        · "no tiene"  → decisión correcta, el sitio no monetiza. Revisar cada tanto por si
--                        alguno publicó su ads.txt después.
--        · "no pude leerlo" → NO es una decisión, es una falla NUESTRA (Cloudflare, timeout,
--                        DNS). Estos nunca se descartan, pero hasta ahora nadie los volvía a
--                        intentar: quedaban en el limbo para siempre.
--
-- Esta tabla separa los dos y le da al worker de dónde agarrarse para re-chequear.
-- No guarda nada sensible: dominio, veredicto y timestamps.
--
-- El worker escribe con service_role (bypasea RLS). Se agrega policy de lectura para el
-- equipo, así el listado se puede mirar desde el panel si algún día lo llevamos ahí.
-- ════════════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS toolbar_adstxt_audit (
  domain          text PRIMARY KEY,
  verdict         text        NOT NULL,            -- 'no' = no tiene | 'unknown' = no pudimos leerlo
  reason          text,                            -- detalle: host chequeado / tipo de fallo
  source          text,                            -- motor que lo trajo (autogoogle, majestic, csv, ...)
  gate            text,                            -- 'entrada' (antes de la cola) | 'proceso' (al analizarlo)
  checks          int         NOT NULL DEFAULT 1,  -- cuántas veces lo intentamos
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  last_checked_at timestamptz NOT NULL DEFAULT now(),
  resolved_at     timestamptz                      -- se llenó → en un re-chequeo apareció el ads.txt
);

-- El re-chequeo pide: verdict='unknown' AND resolved_at IS NULL, los más viejos primero.
CREATE INDEX IF NOT EXISTS idx_adstxt_audit_recheck
  ON toolbar_adstxt_audit (verdict, last_checked_at ASC)
  WHERE resolved_at IS NULL;

-- Revisión manual: los "no tiene" más recientes.
CREATE INDEX IF NOT EXISTS idx_adstxt_audit_verdict_seen
  ON toolbar_adstxt_audit (verdict, first_seen_at DESC);

ALTER TABLE toolbar_adstxt_audit ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "adstxt_audit_select_team" ON toolbar_adstxt_audit;
CREATE POLICY "adstxt_audit_select_team"
  ON toolbar_adstxt_audit FOR SELECT
  USING (auth.role() IN ('authenticated', 'service_role'));


-- ── ENCENDER EL RE-CHEQUEO AUTOMÁTICO DE LOS "NO PUDE LEERLO" ───────────────────────
-- Corre en el worker, 1 vez por día, de a 200 dominios. Cuesta $0 (fetch directo al sitio).
-- Si el sitio dejó de bloquearnos y ahora sí tiene ads.txt → se marca resuelto y el dominio
-- vuelve a la cola para analizarse. Si resulta que efectivamente no tiene → pasa a 'no'.
INSERT INTO toolbar_config (key, value) VALUES
  ('adstxt_recheck_enabled',  'true'),
  ('adstxt_recheck_min_days', '7')     -- no re-chequear un dominio antes de N días
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;


-- ════════════════════════════════════════════════════════════════════════════════════
-- LOS DOS LISTADOS PARA REVISAR CADA X SEMANAS
-- ════════════════════════════════════════════════════════════════════════════════════

-- ── RESUMEN: cómo viene la mano ─────────────────────────────────────────────────────
SELECT
  CASE verdict
    WHEN 'no'      THEN '1. NO TIENE ads.txt (descartado, decisión correcta)'
    WHEN 'unknown' THEN '2. NO PUDIMOS LEERLO (falla nuestra — se reintenta)'
  END                                              AS caso,
  COUNT(*)                                         AS dominios,
  COUNT(*) FILTER (WHERE resolved_at IS NOT NULL)  AS recuperados,
  MIN(first_seen_at)::date                         AS el_mas_viejo,
  MAX(last_checked_at)::date                       AS ultimo_chequeo
FROM toolbar_adstxt_audit
GROUP BY verdict
ORDER BY verdict DESC;


-- ── LISTADO 1: LOS QUE NO TIENEN ads.txt ────────────────────────────────────────────
-- Decisión correcta. Se revisa cada tanto por si alguno publicó su ads.txt después de que
-- lo miramos (pasa: un sitio empieza a monetizar y recién ahí lo sube).
SELECT domain, source, gate, checks, first_seen_at::date AS visto, last_checked_at::date AS chequeado
FROM toolbar_adstxt_audit
WHERE verdict = 'no' AND resolved_at IS NULL
ORDER BY first_seen_at DESC
LIMIT 500;


-- ── LISTADO 2: LOS QUE NO PUDIMOS LEER ──────────────────────────────────────────────
-- ⚠️ ESTE es el que importa: NO son un "no", son un "no sé". Nunca se descartaron, pero
-- si alguno acá es un medio real que nos bloquea el scraper, lo estamos dejando pasar de largo.
-- La columna `checks` dice cuántas veces lo intentamos; `reason` dice por qué falló.
SELECT domain, reason, source, gate, checks,
       first_seen_at::date AS visto, last_checked_at::date AS ultimo_intento
FROM toolbar_adstxt_audit
WHERE verdict = 'unknown' AND resolved_at IS NULL
ORDER BY checks DESC, first_seen_at ASC
LIMIT 500;

-- Agrupado por TIPO de falla — para ver si hay un patrón (todos Cloudflare, todos timeout...):
SELECT reason, COUNT(*) AS n
FROM toolbar_adstxt_audit
WHERE verdict = 'unknown' AND resolved_at IS NULL
GROUP BY 1 ORDER BY 2 DESC;


-- ── RECUPERADOS: los que en un re-chequeo SÍ tenían ads.txt ─────────────────────────
-- Estos volvieron solos a la cola. Sirve para medir si el re-chequeo vale la pena.
SELECT domain, verdict AS veredicto_original, checks, resolved_at::date AS recuperado_el
FROM toolbar_adstxt_audit
WHERE resolved_at IS NOT NULL
ORDER BY resolved_at DESC
LIMIT 200;


-- ── RESCATE MANUAL (si en la revisión ves uno que querés forzar a la cola) ──────────
-- INSERT INTO toolbar_csv_queue (domain, status, uploaded_by, source)
-- VALUES ('el-que-quiero.com', 'pending', 'mgargiulo@adeqmedia.com', 'manual')
-- ON CONFLICT (domain) DO UPDATE SET status = 'pending';
-- DELETE FROM toolbar_adstxt_audit WHERE domain = 'el-que-quiero.com';
