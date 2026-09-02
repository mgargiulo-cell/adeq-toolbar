
-- ═══════════════════════════════════════════════════════════════════════════════════════
-- COLA "POR ENVIAR A MONDAY" (Maxi 2026-09-02, pedido del user)
-- ═══════════════════════════════════════════════════════════════════════════════════════
-- Durante la migración de CRM los MB no pueden empujar a Monday, y si esperan pierden horas
-- de prospección. Con esto siguen trabajando igual: analizan, mandan el mail, y en vez de
-- empujar guardan el prospecto en una cola. Después los mandan todos juntos.
--
-- Se usa `toolbar_review_queue` con un estado nuevo en vez de una tabla aparte, porque el
-- user los quiere ver en la pestaña Prospects filtrando por un valor. Una tabla nueva
-- obligaría a que esa vista consultara dos lados.
--
-- ⚠️ Verificado que el estado nuevo no se le cuela al agente: filtra con `status=eq.pending`
-- en 44 lugares y el único filtro negado es `status=neq.rejected`, que se usa para deduplicar
-- — y ahí conviene que un dominio ya encolado NO se vuelva a descubrir.
ALTER TABLE toolbar_review_queue
  ADD COLUMN IF NOT EXISTS monday_payload jsonb;

COMMENT ON COLUMN toolbar_review_queue.monday_payload IS
  'Los campos del formulario de Monday que esta tabla no tiene (estado, fecha, ejecutivo, '
  'pitch). Solo se llena en las filas con status = por_enviar; el resto queda en NULL.';

-- Índice parcial: la cola es chica y se consulta por estado. Parcial para no pesar sobre
-- las 8.400 filas del resto de la tabla.
CREATE INDEX IF NOT EXISTS idx_review_queue_por_enviar
  ON toolbar_review_queue (created_at DESC)
  WHERE status = 'por_enviar';
