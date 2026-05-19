-- ============================================================
-- toolbar_source_performance — rolling 30d aggregate de engagement
-- por (mb_email, source). Source-of-truth: toolbar_agent_actions +
-- toolbar_email_opens + toolbar_bounce_retries. Esta tabla es derivada
-- y se recomputa diario via aggregateSourcePerformance() en el worker.
-- ============================================================
-- Política user 2026-05-19: ranking dinámico de email sources basado
-- en open_rate y bounce_rate observados por MB. Reemplaza el ranking
-- hardcoded {apollo:4, informer:3, scrape:2, generic:1} para que el
-- sistema aprenda qué source engagea más para cada usuario.
--
-- Window default: 30d rolling. Score = open_rate * (1 - bounce_rate).
-- Si una (mb, source) tiene sent < 50, fallback al ranking default
-- (sample chico → no decidir, seguir explorando).
-- ε-greedy 10%: aún con datos, 1 de cada 10 envíos usa ranking default.
-- ============================================================

create table if not exists public.toolbar_source_performance (
  id           bigserial primary key,
  mb_email     text   not null,                         -- "_global" para el agregado del equipo
  source       text   not null,                         -- apollo|informer|scrape|generic|manual
  window_days  int    not null default 30,
  sent         int    not null default 0,
  opens        int    not null default 0,
  bounces      int    not null default 0,
  open_rate    numeric(5,4) not null default 0,         -- opens/sent
  bounce_rate  numeric(5,4) not null default 0,         -- bounces/sent
  score        numeric(5,4) not null default 0,         -- open_rate * (1 - bounce_rate)
  computed_at  timestamptz not null default now(),
  constraint uq_source_perf unique (mb_email, source, window_days)
);

create index if not exists idx_source_perf_mb on public.toolbar_source_performance(mb_email);
create index if not exists idx_source_perf_computed on public.toolbar_source_performance(computed_at desc);

comment on table public.toolbar_source_performance is
  'Aggregate rolling 30d de engagement por (mb_email, source). Derivado; recomputado diario.';
comment on column public.toolbar_source_performance.mb_email is
  'Email del MB que envió. "_global" = agregado del equipo (fallback cuando MB tiene poco data).';
comment on column public.toolbar_source_performance.score is
  'Score combinado: open_rate * (1 - bounce_rate). Mayor = source con más engagement neto.';

-- RLS: admins leen todo. MBs leen su propia fila + _global.
alter table public.toolbar_source_performance enable row level security;

drop policy if exists "source_perf_admin_all" on public.toolbar_source_performance;
create policy "source_perf_admin_all" on public.toolbar_source_performance
  for all using (auth.jwt() ->> 'email' in (
    'mgargiulo@adeqmedia.com', 'dhorovitz@adeqmedia.com', 'sales@adeqmedia.com'
  ));

drop policy if exists "source_perf_mb_read_own" on public.toolbar_source_performance;
create policy "source_perf_mb_read_own" on public.toolbar_source_performance
  for select using (
    mb_email = lower(auth.jwt() ->> 'email') or mb_email = '_global'
  );
