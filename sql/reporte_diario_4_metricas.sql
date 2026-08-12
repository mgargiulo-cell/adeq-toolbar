-- ══════════════════════════════════════════════════════════════════════════════
-- LAS 4 MÉTRICAS — reporte periódico
--
-- Cambiá la fecha en la primera línea y corré todo. Por defecto: AYER.
-- Para un rango, reemplazá `= _dia` por `BETWEEN _desde AND _hasta`.
--
--   1. Emails enviados por media buyer
--   2. URLs que entraron a Prospects, por método de descubrimiento
--   3. URLs que se limpiaron de Prospects por no cumplir requisitos
--   4. Sitios que no tenían email y se les encontró uno
-- ══════════════════════════════════════════════════════════════════════════════


-- ── 1 · EMAILS ENVIADOS POR MEDIA BUYER ───────────────────────────────────────
-- `sent` = primer contacto (el que tiene tope de 20/día).
-- `secondary_sent` = la 2da dirección del mismo dominio, NO consume cupo.
-- `re_sent` / `bounce_retry_sent` = re-trabajo, sin tope.
SELECT user_email,
       count(*) FILTER (WHERE action = 'sent')                AS primer_contacto,
       count(*) FILTER (WHERE action = 'secondary_sent')      AS segundo_email,
       count(*) FILTER (WHERE action IN ('re_sent','bounce_retry_sent','future_sent')) AS re_trabajo,
       count(*) FILTER (WHERE action = 'skipped')             AS salteados,
       count(*) FILTER (WHERE action = 'failed')              AS fallidos
  FROM toolbar_agent_actions
 WHERE created_at::date = CURRENT_DATE - 1
   AND user_email IS NOT NULL
 GROUP BY user_email
 ORDER BY primer_contacto DESC;

-- Por qué se saltearon (si primer_contacto < 20, la respuesta está acá)
SELECT reason, count(*) AS veces
  FROM toolbar_agent_actions
 WHERE created_at::date = CURRENT_DATE - 1 AND action = 'skipped'
 GROUP BY reason ORDER BY veces DESC LIMIT 15;


-- ── 2 · URLS QUE ENTRARON A PROSPECTS, POR MÉTODO ─────────────────────────────
-- `source` lo escribe cada feeder: autopilot (similares+Radar), autogoogle,
-- majestic, sellers_json, adstxt, similar, monday_refresh, csv/manual.
SELECT coalesce(source, '(sin fuente)') AS metodo,
       count(*)                                                   AS entraron,
       count(*) FILTER (WHERE jsonb_array_length(coalesce(emails,'[]'::jsonb)) > 0) AS con_email,
       count(*) FILTER (WHERE jsonb_array_length(coalesce(emails,'[]'::jsonb)) = 0) AS sin_email,
       round(avg(nullif(traffic, 0)))                             AS trafico_promedio
  FROM toolbar_review_queue
 WHERE created_at::date = CURRENT_DATE - 1
 GROUP BY 1
 ORDER BY entraron DESC;


-- ── 3 · URLS LIMPIADAS DE PROSPECTS POR NO CUMPLIR ────────────────────────────
-- Los barridos escriben el motivo con prefijo: 'purge: X' (polish y sweep),
-- 'urlpurge: X' (barrido por URL), 'mb: X' (rechazo humano con la X).
SELECT split_part(coalesce(suspect_reason, '(sin motivo)'), ':', 1) AS quien_lo_descarto,
       split_part(coalesce(suspect_reason, '(sin motivo)'), ':', 2) AS motivo,
       count(*) AS cuantos
  FROM toolbar_review_queue
 WHERE status = 'rejected'
   AND updated_at::date = CURRENT_DATE - 1
 GROUP BY 1, 2
 ORDER BY cuantos DESC
 LIMIT 25;

-- Y el total del día, en una línea
SELECT count(*) AS limpiados_ayer
  FROM toolbar_review_queue
 WHERE status = 'rejected' AND updated_at::date = CURRENT_DATE - 1;


-- ── 4 · SITIOS SIN EMAIL A LOS QUE SE LES ENCONTRÓ UNO ────────────────────────
-- ⚠️ EXACTO SOLO DESDE EL 2026-08-12. Las columnas email_intentos /
-- email_ultimo_intento se crearon ese día; antes NO existía forma de distinguir
-- "ya lo tenía" de "se lo encontramos hoy", que es justo el agujero que tenía el
-- sistema. Para fechas anteriores esta consulta subestima.
SELECT count(*) AS emails_encontrados
  FROM toolbar_review_queue
 WHERE jsonb_array_length(coalesce(emails,'[]'::jsonb)) > 0
   AND updated_at::date = CURRENT_DATE - 1
   AND created_at::date < CURRENT_DATE - 1;      -- el lead ya existía: el email es nuevo

-- Por dónde aparecieron (el scraper guarda la vía en email_sources)
SELECT src.value AS via, count(*) AS cuantos
  FROM toolbar_review_queue q,
       LATERAL jsonb_each_text(coalesce(q.email_sources, '{}'::jsonb)) AS src
 WHERE q.updated_at::date = CURRENT_DATE - 1
   AND q.created_at::date < CURRENT_DATE - 1
 GROUP BY 1 ORDER BY cuantos DESC LIMIT 15;

-- Cuántos siguen sin email y hace cuánto que no se intenta (el trabajo que queda)
SELECT count(*) FILTER (WHERE email_ultimo_intento IS NULL)                       AS nunca_intentados,
       count(*) FILTER (WHERE email_ultimo_intento < now() - interval '7 days')   AS listos_para_reintentar,
       count(*)                                                                    AS total_sin_email
  FROM toolbar_review_queue
 WHERE status = 'pending'
   AND jsonb_array_length(coalesce(emails,'[]'::jsonb)) = 0;
