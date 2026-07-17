-- ════════════════════════════════════════════════════════════════════════
-- FIX: bump_keyword_yield duplicada (2026-07-17)
-- La migración del 16 (qualified_yield) hizo CREATE OR REPLACE agregando p_qualified.
-- Cambiar la cantidad de parámetros CAMBIA LA FIRMA → Postgres NO reemplaza: crea una
-- SOBRECARGA. Quedaron vivas bump_keyword_yield(text,int,int,int) y (text,int,int,int,int).
-- El worker llamaba con 4 args → ambas candidatas matcheaban (la de 5 vía su DEFAULT) →
-- "function bump_keyword_yield(...) is not unique" → 500 → .catch() silencioso →
-- toolbar_keyword_yield SIEMPRE vacío → AutoGoogle elegía keywords 100% random.
-- Dropeamos la vieja de 4 args. La de 5 (con p_qualified DEFAULT 0) queda como única.
-- ════════════════════════════════════════════════════════════════════════

DROP FUNCTION IF EXISTS bump_keyword_yield(text, int, int, int);

-- Verificación: debe devolver UNA sola fila (la de 5 argumentos).
SELECT p.oid::regprocedure AS firma
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE p.proname = 'bump_keyword_yield' AND n.nspname = 'public';
