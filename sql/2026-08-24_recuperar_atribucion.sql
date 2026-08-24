-- ══════════════════════════════════════════════════════════════════════════════
-- RECUPERAR LA ATRIBUCIÓN PERDIDA — 2026-08-24
--
-- `getNextCsvItem` no pedía la columna `source`, así que todo lo que entró por la
-- cola quedó etiquetado "autopilot" en Prospects. Pero la cola SÍ guardó el origen
-- real de cada dominio: se puede reconstruir cruzando por dominio.
--
-- Conservador a propósito: solo toca filas que hoy dicen 'autopilot' y que tienen
-- una fila `done` en la cola con una fuente de feeder conocida. El autopilot de
-- verdad (runSession) escribe DIRECTO a Prospects sin pasar por la cola, así que no
-- tiene contraparte ahí y queda intacto.
--
-- Corré el paso 1, mirá los números, y recién después el 2.
-- ══════════════════════════════════════════════════════════════════════════════


-- ── 1 · PREVIEW: qué se va a reetiquetar y a qué ──────────────────────────────
SELECT CASE q.source
         WHEN 'auto_feeder_sellers'  THEN 'sellers_json'
         WHEN 'auto_feeder_majestic' THEN 'majestic'
         WHEN 'auto_feeder_monday'   THEN 'monday_refresh'
         WHEN 'auto_feeder_adstxt'   THEN 'adstxt'
         WHEN 'auto_feeder_similar'  THEN 'similar'
         WHEN 'autogoogle'           THEN 'autogoogle'
       END                                   AS fuente_real,
       count(*)                              AS filas,
       min(r.created_at)::date               AS desde,
       max(r.created_at)::date               AS hasta
  FROM toolbar_review_queue r
  JOIN toolbar_csv_queue    q ON q.domain = r.domain
 WHERE r.source = 'autopilot'
   AND q.status = 'done'
   AND q.source IN ('auto_feeder_sellers','auto_feeder_majestic','auto_feeder_monday',
                    'auto_feeder_adstxt','auto_feeder_similar','autogoogle')
 GROUP BY 1
 ORDER BY filas DESC;

-- Cuántos quedan como autopilot legítimo (sin contraparte en la cola)
SELECT count(*) AS autopilot_de_verdad
  FROM toolbar_review_queue r
 WHERE r.source = 'autopilot'
   AND NOT EXISTS (SELECT 1 FROM toolbar_csv_queue q
                    WHERE q.domain = r.domain AND q.status = 'done');


-- ── 2 · APLICAR ──────────────────────────────────────────────────────────────
-- Un dominio puede tener varias filas en la cola (reintentos). Se toma la más
-- reciente que esté en `done`, que es la que efectivamente lo guardó.
UPDATE toolbar_review_queue r
   SET source = m.fuente_real
  FROM (
        SELECT DISTINCT ON (q.domain)
               q.domain,
               CASE q.source
                 WHEN 'auto_feeder_sellers'  THEN 'sellers_json'
                 WHEN 'auto_feeder_majestic' THEN 'majestic'
                 WHEN 'auto_feeder_monday'   THEN 'monday_refresh'
                 WHEN 'auto_feeder_adstxt'   THEN 'adstxt'
                 WHEN 'auto_feeder_similar'  THEN 'similar'
                 WHEN 'autogoogle'           THEN 'autogoogle'
               END AS fuente_real
          FROM toolbar_csv_queue q
         WHERE q.status = 'done'
           AND q.source IN ('auto_feeder_sellers','auto_feeder_majestic','auto_feeder_monday',
                            'auto_feeder_adstxt','auto_feeder_similar','autogoogle')
         ORDER BY q.domain, q.uploaded_at DESC
       ) m
 WHERE r.domain = m.domain
   AND r.source = 'autopilot'
   AND m.fuente_real IS NOT NULL;


-- ── 3 · CHEQUEO ──────────────────────────────────────────────────────────────
SELECT source, count(*) AS total, max(created_at)::date AS ultimo
  FROM toolbar_review_queue
 GROUP BY source
 ORDER BY total DESC;
