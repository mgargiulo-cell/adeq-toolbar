-- ══════════════════════════════════════════════════════════════════════════════
-- SEMBRAR EL CICLO NUEVO DE RAPIDAPI — 2026-08-24
--
-- El plan cambió: `custom-40k-hard` (USD 25/mes) que arrancó el 18 de agosto de 2026
-- y renueva cada 30 días. El ancla del código estaba en el día 7, así que el worker
-- daba por empezado un ciclo nuevo once días antes de tiempo.
--
-- Regla del proyecto: al mover un ancla hay que SEMBRAR el contador, si no el worker
-- arranca creyendo que gastó cero y se pasa del plan real.
-- Consumo real medido en el panel de RapidAPI: 7,77% de 40.000 = ~3.108.
-- ══════════════════════════════════════════════════════════════════════════════

-- ── 1 · VER CÓMO ESTÁ AHORA ──────────────────────────────────────────────────
SELECT key, value FROM toolbar_config
 WHERE key LIKE 'rapidapi%' ORDER BY key;


-- ── 2 · SEMBRAR ──────────────────────────────────────────────────────────────
INSERT INTO toolbar_config (key, value) VALUES
  ('rapidapi_calls_month',        '3108'),        -- 7,77% de 40.000, medido en el panel
  ('rapidapi_calls_month_period', '2026-08-18'),  -- inicio del ciclo real
  ('rapidapi_monthly_limit',      '40000')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;


-- ── 3 · CHEQUEO ──────────────────────────────────────────────────────────────
-- El período tiene que decir 2026-08-18 y el consumo ~3.108 de 40.000.
SELECT key, value FROM toolbar_config
 WHERE key IN ('rapidapi_calls_month','rapidapi_calls_month_period',
               'rapidapi_monthly_limit','rapidapi_daily_limit')
 ORDER BY key;

-- Referencia de ritmo: 40.000 / 30 días = 1.333 por día de presupuesto parejo.
-- Al 24/08 el consumo real iba a 518/día, o sea MUY por debajo. Sobra cuota.
