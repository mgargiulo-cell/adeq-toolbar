-- ============================================================
-- Auto-feeder runs log
--
-- Cada cron (L-V 9/12/15/18/20 Madrid) loggea su resultado:
--   • cuántos brutos inyectó por fuente
--   • cuántos terminaron efectivos en review_queue (post-enrichment)
--   • conversion rate del run
--   • status: ok | skipped_rapidapi | skipped_saturated | skipped_hours | incomplete
--
-- Sirve para:
--   1. Calcular conversion_rate adaptativo (próximo cron sabe cuánto inyectar)
--   2. Tabla "Auto-feeder last 20 runs" en Settings → Admin
-- ============================================================

create table if not exists public.toolbar_feeder_runs (
  id                serial primary key,
  cron_at           timestamptz not null default now(),
  slot_label        text not null,                        -- "09:00" | "12:00" | ...
  status            text not null,                        -- ok | skipped_* | incomplete
  gross_sellers     int  not null default 0,
  gross_monday      int  not null default 0,
  gross_majestic    int  not null default 0,
  gross_total       int  generated always as (gross_sellers + gross_monday + gross_majestic) stored,
  effective_added   int  not null default 0,              -- medido ~30 min después del run
  conversion_pct    numeric(5,2),                          -- effective_added / gross_total * 100
  rapidapi_used     int,                                   -- snapshot al disparo
  rapidapi_limit    int,
  rq_valid_before   int,                                   -- review_queue pending+traffic>=400K AL DISPARO
  rq_valid_after    int,                                   -- mismo, medido al evaluar effective_added
  notes             text
);

create index if not exists idx_feeder_runs_cron_at
  on public.toolbar_feeder_runs(cron_at desc);

alter table public.toolbar_feeder_runs enable row level security;
revoke insert, update, delete on public.toolbar_feeder_runs from anon, authenticated;

-- Admins pueden leer todo
drop policy if exists "feeder_runs_select_admin" on public.toolbar_feeder_runs;
create policy "feeder_runs_select_admin" on public.toolbar_feeder_runs for select
  to authenticated using (
    (auth.jwt() ->> 'email') in ('mgargiulo@adeqmedia.com')
  );

-- El worker (service_role) inserta y actualiza
grant select, insert, update on public.toolbar_feeder_runs to service_role;
grant usage, select on sequence public.toolbar_feeder_runs_id_seq to service_role;
