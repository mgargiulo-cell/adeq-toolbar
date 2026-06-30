-- Maxi 2026-06-30: bajar el cap del agente a 10 envíos/día PARA CADA MB
-- (antes 25). Aplica a Maxi/Diego/Agus por igual. Sin override per-user.
-- Correr 1 vez en Supabase SQL Editor.

-- 1. Cap diario global del agente = 10
INSERT INTO public.toolbar_config (key, value)
VALUES ('agent_max_per_day', '10')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- 2. Mantener vacío el override per-MB (todos usan el global de 10)
INSERT INTO public.toolbar_config (key, value)
VALUES ('agent_max_per_day_by_user', '{}')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- 3. per_cycle_limit a 2 por consistencia (5 slots 9/12/15/18/20 × 2 = 10/día)
INSERT INTO public.toolbar_config (key, value)
VALUES ('agent_per_cycle_limit', '2')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Verificación
SELECT key, value FROM public.toolbar_config
WHERE key IN ('agent_max_per_day', 'agent_max_per_day_by_user', 'agent_per_cycle_limit');
