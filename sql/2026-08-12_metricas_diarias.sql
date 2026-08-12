-- ══════════════════════════════════════════════════════════════════════════════
-- HISTORIA DE LAS 4 MÉTRICAS — 2026-08-12
--
-- Por qué hace falta: las consultas del reporte son una FOTO del día. Sin historia
-- no se puede responder la pregunta que importa —"¿esto mejora o empeora?"— que es
-- justo el sentido de medir cada 48 horas. El worker guarda una fila por día y
-- después la tendencia se lee de un vistazo.
--
-- Corré este archivo una sola vez.
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS public.toolbar_metricas_diarias (
  dia               DATE PRIMARY KEY,
  enviados          INT,        -- primer contacto, todos los MB sumados
  enviados_por_mb   JSONB,      -- {"mgargiulo@...": 20, "sales@...": 18}
  altas             INT,        -- URLs nuevas en Prospects
  altas_por_fuente  JSONB,      -- {"autopilot": 40, "autogoogle": 12}
  limpiados         INT,        -- descartados por no cumplir
  emails_hallados   INT,        -- sitios sin email a los que se les encontró uno
  pool_con_email    INT,        -- cuántos quedan listos para enviar
  pool_sin_email    INT,
  congelados        INT,
  creado_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.toolbar_metricas_diarias ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "metricas_service_all" ON public.toolbar_metricas_diarias;
CREATE POLICY "metricas_service_all" ON public.toolbar_metricas_diarias
  FOR ALL USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');

DROP POLICY IF EXISTS "metricas_read" ON public.toolbar_metricas_diarias;
CREATE POLICY "metricas_read" ON public.toolbar_metricas_diarias
  FOR SELECT USING (auth.role() IN ('authenticated', 'service_role'));


-- La vista que se mira: últimos 21 días con la variación contra el día anterior,
-- para ver la tendencia sin hacer la cuenta a mano.
CREATE OR REPLACE VIEW public.toolbar_metricas_tendencia AS
SELECT dia,
       enviados,
       enviados - lag(enviados)  OVER (ORDER BY dia) AS dif_enviados,
       altas,
       altas    - lag(altas)     OVER (ORDER BY dia) AS dif_altas,
       limpiados,
       emails_hallados,
       pool_con_email,
       pool_con_email - lag(pool_con_email) OVER (ORDER BY dia) AS dif_pool,
       congelados,
       altas_por_fuente,
       enviados_por_mb
  FROM public.toolbar_metricas_diarias
 ORDER BY dia DESC;

COMMENT ON VIEW public.toolbar_metricas_tendencia IS
  'Las 4 métricas día a día con su variación. SELECT * FROM toolbar_metricas_tendencia LIMIT 21;';
