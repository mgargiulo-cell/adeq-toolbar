-- 2026-07-03 — Fix warning "SECURITY DEFINER view" del linter de Supabase
-- Una view SECURITY DEFINER corre con los permisos/RLS del CREADOR, no del que
-- consulta → puede saltear RLS. La recomendación de Supabase es usar security_invoker
-- (Postgres 15+), que hace que la view respete el RLS del usuario que la consulta.
--
-- ⚠️ OJO: si la toolbar leía esta view apoyándose en los permisos elevados del creador,
-- al pasar a invoker podría devolver 0 filas para roles sin acceso a las tablas base.
-- Después de correr esto, VERIFICÁ que la toolbar siga mostrando el open-rate. Si se
-- rompe, o revertís (set security_invoker = off) o agregás una RLS policy adecuada.

alter view public.toolbar_agent_open_rate set (security_invoker = on);

-- Si el linter marca OTRAS views con el mismo warning, aplicá lo mismo a cada una:
-- alter view public.<nombre_de_la_view> set (security_invoker = on);

-- Para listar todas las views SECURITY DEFINER del schema public y detectarlas de una:
-- (correr el SELECT; por cada resultado, correr el ALTER de arriba con ese nombre)
select c.relname as view_name
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
  and c.relkind = 'v'
  and c.reloptions is distinct from array['security_invoker=on']  -- las que NO tienen invoker on
order by 1;
