-- ══════════════════════════════════════════════════════════════════════════════
-- VOLVER A LOS BORRADORES DEL USER Y BORRAR LOS MÍOS — 2026-08-11
--
-- El copy que escribí no estaba aprobado y encima le llegó a un prospecto real con
-- los placeholders CRUDOS ({{saludo}}, {{sender_name}}), porque la extensión solo
-- sabe resolver {{domain}}. Se eliminan.
--
-- Cómo se distinguen sin riesgo: los míos son los ÚNICOS que contienen {{saludo}}
-- o {{senal}} en el cuerpo. Ningún borrador del user los tiene. No hace falta
-- confiar en el nombre ni en el asunto.
--
-- Corré los 4 bloques de una. Es idempotente y funciona sin importar si ya
-- corriste el archivo anterior o no.
-- ══════════════════════════════════════════════════════════════════════════════


-- ── 1. VER QUÉ SE VA A BORRAR (mirá esto antes de seguir) ─────────────────────
SELECT user_email, language, subject
  FROM toolbar_pitch_drafts
 WHERE body LIKE '%{{saludo}}%' OR body LIKE '%{{senal}}%'
 ORDER BY language, priority;


-- ── 2. DEVOLVER LOS DEL USER AL POOL ──────────────────────────────────────────
-- (si ya los devolviste, esto no hace nada)
UPDATE toolbar_pitch_drafts
   SET user_email = '_default_'
 WHERE user_email IN ('_viejo_2026_08_11_');


-- ── 3. BORRAR LOS MÍOS, DEFINITIVO ────────────────────────────────────────────
DELETE FROM toolbar_pitch_drafts
 WHERE body LIKE '%{{saludo}}%' OR body LIKE '%{{senal}}%';


-- ── 4. CHEQUEO ────────────────────────────────────────────────────────────────
-- Tienen que quedar 15 (3 × es/en/pt/it/ar), todos cortos y con asunto "- ADEQ".
SELECT language, priority, subject, left(body, 45) AS arranque
  FROM toolbar_pitch_drafts
 WHERE user_email = '_default_'
 ORDER BY language, priority;

-- Y que NINGUNO tenga un placeholder que la extensión no sepa resolver.
-- La toolbar solo reemplaza {{domain}}: cualquier otro llega crudo al destinatario.
-- Esta consulta tiene que devolver CERO filas.
SELECT language, subject, body
  FROM toolbar_pitch_drafts
 WHERE body ~ '\{\{(?!domain\}\})' OR subject ~ '\{\{(?!domain\}\})';
