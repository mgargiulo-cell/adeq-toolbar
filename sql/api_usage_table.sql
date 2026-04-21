-- Tracking diario de uso de APIs por user (Edge Function api-proxy escribe acá)
create table if not exists public.toolbar_api_usage (
  user_email  text not null,
  day         date not null,
  total       int  not null default 0,
  by_provider jsonb not null default '{}'::jsonb,
  updated_at  timestamptz not null default now(),
  primary key (user_email, day)
);

create index if not exists toolbar_api_usage_day_idx on public.toolbar_api_usage(day);

alter table public.toolbar_api_usage enable row level security;

-- Authenticated: solo lee sus propias filas (para mostrar quota en UI)
drop policy if exists "usage_select_own" on public.toolbar_api_usage;
create policy "usage_select_own" on public.toolbar_api_usage for select
  to authenticated using (user_email = auth.jwt() ->> 'email');

-- Escritura solo service_role (Edge Function)
revoke insert, update, delete on public.toolbar_api_usage from anon, authenticated;
