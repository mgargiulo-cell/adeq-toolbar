-- v5.0.33: agregar status 'next_day' a toolbar_csv_queue
-- Permite que uploads que excedan budget diario (1000/día) queden en cola para mañana.
-- Rollover automático en worker al cambiar el día Madrid.

-- Si hay un CHECK constraint en status, lo recreamos. Si no, este SQL es no-op.
do $$
declare
  cname text;
begin
  select conname into cname
  from pg_constraint
  where conrelid = 'public.toolbar_csv_queue'::regclass
    and contype = 'c'
    and pg_get_constraintdef(oid) ilike '%status%';

  if cname is not null then
    execute format('alter table public.toolbar_csv_queue drop constraint %I', cname);
  end if;
end $$;

alter table public.toolbar_csv_queue
  add constraint toolbar_csv_queue_status_check
  check (status in ('pending', 'processing', 'done', 'error', 'skipped', 'waiting_pool', 'next_day', 'frozen'));

-- Index para rollover rápido
create index if not exists idx_csv_queue_next_day on public.toolbar_csv_queue(status) where status = 'next_day';
