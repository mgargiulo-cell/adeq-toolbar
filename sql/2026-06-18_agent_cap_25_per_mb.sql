-- Maxi 2026-06-18: cambiar cap del agente a 25 envíos/día PARA CADA MB
-- (Maxi/Diego/Agus por igual). Eliminar el override per-user que estaba
-- con valores distintos (Maxi 30, Diego 15, Agus 15).

-- 1. Setear el cap global del agente a 25
INSERT INTO public.toolbar_config (key, value)
VALUES ('agent_max_per_day', '25')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- 2. Limpiar el override per-MB (queda vacío → todos usan el global de 25)
INSERT INTO public.toolbar_config (key, value)
VALUES ('agent_max_per_day_by_user', '{}')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- 3. Subir el per_cycle_limit por consistencia (5 slots × 5 = 25)
INSERT INTO public.toolbar_config (key, value)
VALUES ('agent_per_cycle_limit', '5')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Verificación
SELECT key, value
FROM public.toolbar_config
WHERE key IN ('agent_max_per_day','agent_max_per_day_by_user','agent_per_cycle_limit');
