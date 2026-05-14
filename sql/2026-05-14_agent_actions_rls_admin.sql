-- v5.0.33 fix: admin mgargiulo@adeqmedia.com debe poder leer TODAS las rows
-- de toolbar_agent_actions desde la toolbar (no solo las suyas).
-- Sin esto el feed "Last 50 actions" queda vacío aunque el agente esté mandando.

-- Drop policy vieja si existe (puede estar filtrando por user)
drop policy if exists toolbar_agent_actions_admin_read on public.toolbar_agent_actions;
drop policy if exists toolbar_agent_actions_select on public.toolbar_agent_actions;
drop policy if exists agent_actions_user_read on public.toolbar_agent_actions;

-- Admin (mgargiulo) puede leer TODO. Resto: solo sus propias filas.
create policy toolbar_agent_actions_admin_read
  on public.toolbar_agent_actions
  for select
  to authenticated
  using (
    (auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com'
    or user_email = (auth.jwt() ->> 'email')
  );

-- Service role bypass (worker Railway)
alter table public.toolbar_agent_actions force row level security;

-- Verificación: cuántas rows accesibles
select 'total_rows' as label, count(*) as n from public.toolbar_agent_actions;
