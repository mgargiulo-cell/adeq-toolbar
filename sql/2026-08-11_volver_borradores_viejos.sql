-- ══════════════════════════════════════════════════════════════════════════════
-- RECUPERAR LOS BORRADORES DEL USER Y BORRAR LOS MÍOS — 2026-08-11
--
-- Los tuyos los archivé hoy bajo user_email = '_viejo_2026_08_11_'. Vuelven al pool.
-- Los míos se borran: no estaban aprobados y encima le llegaron a un prospecto real
-- con los placeholders crudos ({{saludo}}, {{sender_name}}), porque la extensión
-- solo sabe resolver {{domain}}.
--
-- Cómo se distinguen sin riesgo: los míos son los ÚNICOS que contienen {{saludo}}
-- o {{senal}} en el cuerpo. No hace falta confiar en el nombre ni en el asunto.
--
-- Corré los bloques EN ORDEN. Los pasos 1 y 2 son solo lectura: mirá lo que
-- devuelven antes de seguir con el 3.
-- ══════════════════════════════════════════════════════════════════════════════


-- ── 1. QUÉ HAY HOY EN LA TABLA, AGRUPADO ──────────────────────────────────────
-- Acá vas a ver dónde quedó cada juego y cuántos hay por idioma.
SELECT user_email,
       language,
       count(*)                     AS cuantos,
       string_agg(subject, ' · ' ORDER BY priority) AS asuntos
  FROM toolbar_pitch_drafts
 GROUP BY user_email, language
 ORDER BY user_email, language;


-- ── 2. QUÉ SE VA A BORRAR (los míos) ──────────────────────────────────────────
SELECT user_email, language, priority, subject
  FROM toolbar_pitch_drafts
 WHERE body LIKE '%{{saludo}}%' OR body LIKE '%{{senal}}%'
 ORDER BY language, priority;


-- ── 3. RECUPERAR LOS TUYOS ────────────────────────────────────────────────────
UPDATE toolbar_pitch_drafts
   SET user_email = '_default_'
 WHERE user_email = '_viejo_2026_08_11_';


-- ── 4. BORRAR LOS MÍOS, DEFINITIVO ────────────────────────────────────────────
DELETE FROM toolbar_pitch_drafts
 WHERE body LIKE '%{{saludo}}%' OR body LIKE '%{{senal}}%';


-- ── 5. CHEQUEO FINAL ──────────────────────────────────────────────────────────
-- Cuántos quedaron por idioma. Tienen que estar es / en / it / pt / ar.
SELECT language, count(*) AS cuantos
  FROM toolbar_pitch_drafts
 WHERE user_email = '_default_'
 GROUP BY language
 ORDER BY language;

-- El listado completo, para que los revises uno por uno.
SELECT language, priority, subject, body
  FROM toolbar_pitch_drafts
 WHERE user_email = '_default_'
 ORDER BY language, priority;

-- Y el control que importa: NINGÚN borrador puede tener un placeholder distinto
-- de {{domain}}, porque la extensión no sabe resolverlos y le llegan crudos al
-- destinatario. Esta consulta tiene que devolver CERO filas.
SELECT language, subject, body
  FROM toolbar_pitch_drafts
 WHERE body ~ '\{\{(?!domain\}\})' OR subject ~ '\{\{(?!domain\}\})';
