-- ════════════════════════════════════════════════════════════════════════
-- ANÁLISIS DE ACTIVIDAD DEL AGENTE 11–15 JULIO 2026 — identificar errores.
-- Correr sección por sección (Supabase muestra solo el ÚLTIMO result set).
-- Fechas en hora Madrid. Los envíos filtran source<>'manual_extra' = SOLO agente
-- (para incluir tus manuales, sacá esa línea).
-- ════════════════════════════════════════════════════════════════════════

-- ── A) Resumen por MB: cuántos envió el agente en total (+ urls/emails distintos) ──
SELECT mb_email,
       COUNT(*)                        AS envios,
       COUNT(DISTINCT domain)          AS urls_distintas,
       COUNT(DISTINCT email_sent_to)   AS emails_distintos
FROM toolbar_response_tracking
WHERE (sent_at AT TIME ZONE 'Europe/Madrid')::date BETWEEN '2026-07-11' AND '2026-07-15'
  AND source IS DISTINCT FROM 'manual_extra'
GROUP BY 1 ORDER BY 2 DESC;


-- ── B) Por día y por MB (para ver el ritmo / cap 10) ──
SELECT (sent_at AT TIME ZONE 'Europe/Madrid')::date AS dia, mb_email, COUNT(*) AS enviados
FROM toolbar_response_tracking
WHERE (sent_at AT TIME ZONE 'Europe/Madrid')::date BETWEEN '2026-07-11' AND '2026-07-15'
  AND source IS DISTINCT FROM 'manual_extra'
GROUP BY 1, 2 ORDER BY 1, 3 DESC;


-- ── C) DETALLE: qué URL y a qué email envió cada MB (+ fuente) ──
SELECT (sent_at AT TIME ZONE 'Europe/Madrid') AS cuando, mb_email, domain, email_sent_to, source
FROM toolbar_response_tracking
WHERE (sent_at AT TIME ZONE 'Europe/Madrid')::date BETWEEN '2026-07-11' AND '2026-07-15'
  AND source IS DISTINCT FROM 'manual_extra'
ORDER BY mb_email, sent_at;


-- ── D) REBOTES por MB: cuántos y QUÉ HIZO con cada uno ──
--   sent            = reenviado OK a un email nuevo
--   failed          = intento de reenvío falló
--   skipped_no_alt  = no había email alternativo → congeló el dominio 30d
--   reconciled      = corregido a posteriori (reconcileMondayBounces)
SELECT mb_email,
       COUNT(*)                                        AS rebotes,
       COUNT(*) FILTER (WHERE status='sent')           AS reenviado_ok,
       COUNT(*) FILTER (WHERE status='failed')         AS reenvio_fallo,
       COUNT(*) FILTER (WHERE status='skipped_no_alt') AS sin_alt_congelado,
       COUNT(*) FILTER (WHERE status='reconciled')     AS reconciliado,
       COUNT(*) FILTER (WHERE bounce_type='hard')      AS hard,
       COUNT(*) FILTER (WHERE bounce_type='soft')      AS soft
FROM toolbar_bounce_retries
WHERE (created_at AT TIME ZONE 'Europe/Madrid')::date BETWEEN '2026-07-11' AND '2026-07-15'
GROUP BY 1 ORDER BY 2 DESC;


-- ── E) DETALLE de rebotes: qué mail rebotó, a cuál se reenvió y en qué terminó ──
SELECT (created_at AT TIME ZONE 'Europe/Madrid') AS cuando, mb_email, domain,
       original_email AS reboto, retry_email AS reenviado_a, retry_source,
       bounce_type, status, reason
FROM toolbar_bounce_retries
WHERE (created_at AT TIME ZONE 'Europe/Madrid')::date BETWEEN '2026-07-11' AND '2026-07-15'
ORDER BY mb_email, created_at;


-- ── F) POSIBLES ERRORES: emails "malos" que el agente llegó a enviar en el período ──
-- (técnicos/WHOIS/placeholder/departamento equivocado/freemail personal) — para cuantificar
-- cuánto de esto pasó ANTES de los fixes de hoy y ver si sigue apareciendo del 14/07 en adelante.
SELECT (sent_at AT TIME ZONE 'Europe/Madrid') AS cuando, mb_email, domain, email_sent_to, source,
  CASE
    WHEN split_part(email_sent_to,'@',1) ~* '^(domainmanagement|dominios|hostmaster|postmaster|noc|sys|system|sistemas|it[-_.]|edv|betrieb|technik|net[-_.]?manage|infra|webmaster)' THEN 'tecnico/whois'
    WHEN email_sent_to ~* '(vorname[._]name|firstname[._]lastname|nombre[._]apellido|name[._]surname)' THEN 'placeholder'
    WHEN split_part(email_sent_to,'@',1) ~* '^(casting|complaints?|reclam|quejas|seguridad|seguranca|security|helpdesk)' THEN 'depto_equivocado'
    WHEN split_part(email_sent_to,'@',2) IN ('gmail.com','hotmail.com','yahoo.com','outlook.com','live.com') THEN 'freemail_personal'
    ELSE 'otro'
  END AS tipo_error
FROM toolbar_response_tracking
WHERE (sent_at AT TIME ZONE 'Europe/Madrid')::date BETWEEN '2026-07-11' AND '2026-07-15'
  AND source IS DISTINCT FROM 'manual_extra'
  AND (
    split_part(email_sent_to,'@',1) ~* '^(domainmanagement|dominios|hostmaster|postmaster|noc|sys|system|sistemas|it[-_.]|edv|betrieb|technik|net[-_.]?manage|infra|webmaster|casting|complaints?|reclam|quejas|seguridad|seguranca|security|helpdesk)'
    OR email_sent_to ~* '(vorname[._]name|firstname[._]lastname|nombre[._]apellido|name[._]surname)'
    OR split_part(email_sent_to,'@',2) IN ('gmail.com','hotmail.com','yahoo.com','outlook.com','live.com')
  )
ORDER BY tipo_error, cuando;
