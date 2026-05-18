-- ============================================================
-- Import attempts log
--
-- Cada vez que un MB clickea Upload CSV / Queue sellers.json / Fetch Monday,
-- se loggea el intento — incluso si ningún dominio terminó entrando a la cola
-- por dedup. Sirve para mostrar "Diego trabajó hoy" aunque su trabajo haya sido
-- 75 intentos todos repetidos.
--
-- attempted_count   = cuántos dominios trajo la fuente
-- deduped_count     = cuántos se descartaron por dedup contra sistema
-- inserted_count    = cuántos efectivamente entraron a toolbar_csv_queue
--                     (NOTA: NO es lo mismo que "efectivos a Prospects" — esos
--                      requieren post-enrichment del worker y el filtro de tráfico)
-- ============================================================

create table if not exists public.toolbar_import_attempts (
  id                serial primary key,
  user_email        text not null,
  source            text not null,                  -- csv | sellers_json | monday
  source_detail     text,                            -- "Truvid" | "Peru/ES" | filename, opcional
  attempted_count   int  not null default 0,
  deduped_count     int  not null default 0,
  inserted_count    int  not null default 0,
  at                timestamptz not null default now()
);

create index if not exists idx_import_attempts_user_at
  on public.toolbar_import_attempts(user_email, at desc);

create index if not exists idx_import_attempts_at
  on public.toolbar_import_attempts(at desc);

alter table public.toolbar_import_attempts enable row level security;

-- Todos los users autenticados pueden insertar sus propios intentos
drop policy if exists "import_attempts_insert_own" on public.toolbar_import_attempts;
create policy "import_attempts_insert_own" on public.toolbar_import_attempts for insert
  to authenticated with check (lower(user_email) = lower(auth.jwt() ->> 'email'));

-- Todos los users autenticados pueden leer todos los intentos (para ver actividad del equipo)
drop policy if exists "import_attempts_select_team" on public.toolbar_import_attempts;
create policy "import_attempts_select_team" on public.toolbar_import_attempts for select
  to authenticated using (true);

-- Service role acceso total (worker no lo usa pero por consistencia)
grant select, insert on public.toolbar_import_attempts to service_role;
grant usage, select on sequence public.toolbar_import_attempts_id_seq to service_role;
