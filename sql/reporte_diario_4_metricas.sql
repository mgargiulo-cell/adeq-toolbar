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


-- ══════════════════════════════════════════════════════════════════════════════
-- LO QUE LAS 4 MÉTRICAS NO CUENTAN — agregado 2026-08-12
-- Las 4 miden CANTIDAD. Estas tres miden si esa cantidad sirve.
-- ══════════════════════════════════════════════════════════════════════════════


-- ── 5 · SUPERVIVENCIA POR FUENTE (la que decide dónde invertir) ───────────────
-- Contar cuántas URLs trajo cada feeder no dice nada: una fuente que mete 100 y se
-- purgan todas es PEOR que una que mete 10 y sobreviven 8, porque además gastó
-- créditos y llenó el carril. Esto mira una cohorte vieja (lo que entró hace 7-21
-- días, ya con tiempo de ser juzgado) y pregunta qué quedó vivo.
SELECT coalesce(source,'(sin fuente)')                                          AS fuente,
       count(*)                                                                  AS entraron,
       round(100.0 * count(*) FILTER (WHERE status <> 'rejected') / count(*), 1) AS pct_sobrevive,
       round(100.0 * count(*) FILTER (WHERE jsonb_array_length(coalesce(emails,'[]'::jsonb)) > 0) / count(*), 1) AS pct_con_email,
       count(*) FILTER (WHERE status = 'validated')                              AS validados,
       count(*) FILTER (WHERE domain IN (SELECT domain FROM toolbar_sendtrack))  AS se_les_escribio
  FROM toolbar_review_queue
 WHERE created_at::date BETWEEN CURRENT_DATE - 21 AND CURRENT_DATE - 7
 GROUP BY 1
HAVING count(*) >= 5
 ORDER BY pct_sobrevive DESC;


-- ── 6 · CALIDAD DE LOS ENVÍOS, NO SOLO CANTIDAD ───────────────────────────────
-- 20 mails a info@ no valen lo mismo que 20 al director comercial. `chosen_tier`:
-- 4 = persona verificada · 3 = buzón comercial (publicidad@, ventas@) · 2 = persona
-- scrapeada · 1 = WHOIS · 0 = genérico (info@, contact@).
SELECT chosen_tier,
       count(*) AS envios,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
  FROM toolbar_email_picks
 WHERE created_at::date = CURRENT_DATE - 1
 GROUP BY chosen_tier
 ORDER BY chosen_tier DESC;

-- Y si rebotaron: un envío que rebota es un envío perdido y reputación quemada.
SELECT count(*) FILTER (WHERE a.action = 'sent')                        AS enviados,
       count(DISTINCT b.email)                                          AS rebotaron,
       round(100.0 * count(DISTINCT b.email) / nullif(count(*) FILTER (WHERE a.action='sent'),0), 1) AS pct_rebote
  FROM toolbar_agent_actions a
  LEFT JOIN toolbar_bounced_emails b
         ON lower(b.email) = lower(a.email_to) AND b.bounced_at >= a.created_at
 WHERE a.created_at::date = CURRENT_DATE - 1;


-- ── 7 · LO ÚNICO QUE IMPORTA AL FINAL: RESPUESTAS REALES ──────────────────────
-- Ojo: desde el 2026-08-11 los acuses de ticket de mesas de ayuda ya NO cuentan
-- como respuesta real. Antes inflaban este número (39 "respuestas" que no lo eran).
SELECT date_trunc('week', sent_at)::date AS semana,
       count(*)                                       AS enviados,
       count(*) FILTER (WHERE response_type = 'real') AS respuestas_reales,
       count(*) FILTER (WHERE response_type = 'ooo')  AS automaticas,
       round(100.0 * count(*) FILTER (WHERE response_type = 'real') / nullif(count(*),0), 2) AS pct_respuesta
  FROM toolbar_response_tracking
 WHERE sent_at >= CURRENT_DATE - 42
 GROUP BY 1 ORDER BY 1 DESC;


-- ── 8 · LA TENDENCIA (necesita el snapshot diario del worker) ─────────────────
-- Las consultas de arriba son una FOTO. Sin historia no se sabe si mejora o empeora,
-- y ese es el punto de medir cada 48h. El worker guarda un snapshot por día.
SELECT dia, enviados, altas, limpiados, emails_hallados, pool_con_email
  FROM toolbar_metricas_diarias
 ORDER BY dia DESC
 LIMIT 21;
