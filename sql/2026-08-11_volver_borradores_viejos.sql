-- ══════════════════════════════════════════════════════════════════════════════
-- VOLVER A LOS BORRADORES DE ANTES — 2026-08-11
--
-- El user no aprobó el copy nuevo y pidió volver a los suyos para trabajarlos él.
-- Esto es exactamente el inverso del archivado de hoy: no se pierde nada, y el copy
-- nuevo queda guardado por si alguna parte llega a servir más adelante.
--
-- Corré los 3 bloques de una.
-- ══════════════════════════════════════════════════════════════════════════════


-- ── 1. GUARDAR EL COPY NUEVO (sale del pool, no se borra) ─────────────────────
UPDATE toolbar_pitch_drafts
   SET user_email = '_nuevo_sin_aprobar_2026_08_11_'
 WHERE user_email = '_default_';


-- ── 2. DEVOLVER LOS DE ANTES AL POOL ──────────────────────────────────────────
UPDATE toolbar_pitch_drafts
   SET user_email = '_default_'
 WHERE user_email = '_viejo_2026_08_11_';


-- ── 3. CHEQUEO — tienen que salir 15 y con los asuntos de siempre ─────────────
SELECT language, priority, subject, left(body, 50) AS arranque
  FROM toolbar_pitch_drafts
 WHERE user_email = '_default_'
 ORDER BY language, priority;

-- Y que no haya quedado ningún placeholder que la extensión no sepa reemplazar.
-- La toolbar solo resuelve {{domain}}; cualquier otro llegaría CRUDO al destinatario.
SELECT language, subject, body
  FROM toolbar_pitch_drafts
 WHERE user_email = '_default_'
   AND (body ~ '\{\{(?!domain\}\})' OR subject ~ '\{\{(?!domain\}\})');
