-- ════════════════════════════════════════════════════════════════════════
-- PRE-LISTADO DE DESCUBRIMIENTO (2026-07-17, pedido del user)
--
-- Problema que resuelve: en _injectIntoCsvQueue el excedente del carril se DESCARTABA
-- (`domains.slice(0, laneRoom)`) — dominios frescos ya PAGADOS con créditos de Serper que
-- se tiraban a la basura y había que re-descubrir (= pagar de nuevo).
--
-- Con el TURNO HISPANO (50% de los crons priorizan LATAM/Centroamérica/España) el problema
-- se agravaba: un slot hispano encuentra webs buenas de otros países y las perdía.
--
-- Ahora: el excedente se ESTACIONA acá y los slots NO hispanos lo drenan GRATIS antes de
-- gastar créditos nuevos. Si el pre-listado llena el carril, el slot no gasta ni una búsqueda.
--
-- El worker escribe con service_role (bypasea RLS). RLS ON sin policies = solo el worker.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS toolbar_discovery_backlog (
  domain   text PRIMARY KEY,
  source   text NOT NULL,              -- autogoogle | similar | ...
  phrase   text,                       -- keyword que lo trajo (para el yield al recuperarlo)
  found_at timestamptz NOT NULL DEFAULT now()
);

-- Drain: source=eq.X & order=found_at.asc (FIFO — lo más viejo primero, antes de que expire).
CREATE INDEX IF NOT EXISTS idx_discovery_backlog_source_found
  ON toolbar_discovery_backlog (source, found_at ASC);

-- TTL: el worker borra found_at < now()-30d (ya no es "fresco").
CREATE INDEX IF NOT EXISTS idx_discovery_backlog_found_at
  ON toolbar_discovery_backlog (found_at);

ALTER TABLE toolbar_discovery_backlog ENABLE ROW LEVEL SECURITY;
