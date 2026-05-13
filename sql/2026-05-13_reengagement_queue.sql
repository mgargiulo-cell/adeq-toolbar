-- ============================================================
-- toolbar_reengagement_queue — cola manual de email futuro
-- ============================================================
-- Cuando un MB hace "Send to Monday" y rellenó el campo EMAIL FUTURO,
-- se inserta una fila acá. El agente:
--   - A los 11 días lee filas con status='pending'
--   - Si el email inicial NO fue abierto (toolbar_email_opens)
--   - Envía el future_email
--   - Updates Monday: email = future_email, Fecha FU1 = today+5, Fecha FU2 = today+10
--   - Marca status='sent' (o 'skipped' si fue abierto)
-- ============================================================

create table if not exists public.toolbar_reengagement_queue (
  id                bigint generated always as identity primary key,
  domain            text not null,
  monday_item_id    bigint,
  mb_email          text not null,          -- quién lo prospectó
  original_email    text not null,          -- el A que se envió primero
  future_email      text not null,          -- el B que se enviará a los 11d
  -- Snapshot del email original para que el worker pueda reenviar idéntico
  -- (mismo cuerpo + asunto) al future_email a los 11d.
  original_subject  text,
  original_body     text,
  original_sent_at  timestamptz not null default now(),
  scheduled_for     timestamptz not null,   -- original_sent_at + 11 días
  status            text not null default 'pending'
                    check (status in ('pending','sent','skipped_opened','skipped_bounced','failed','cancelled')),
  -- Trazabilidad post-envío:
  -- - tracking_action_id: action_id del envío ORIGINAL (para chequear opens en toolbar_email_opens)
  -- - agent_action_id:    action_id del envío del FUTURE (cuando se dispara)
  tracking_action_id bigint references public.toolbar_agent_actions(id) on delete set null,
  agent_action_id   bigint references public.toolbar_agent_actions(id) on delete set null,
  reason            text,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

create index if not exists toolbar_reengagement_queue_pending_idx
  on public.toolbar_reengagement_queue (status, scheduled_for)
  where status = 'pending';

create index if not exists toolbar_reengagement_queue_domain_idx
  on public.toolbar_reengagement_queue (domain, status);

-- ── RLS ──────────────────────────────────────────────────────
alter table public.toolbar_reengagement_queue enable row level security;
revoke insert, update, delete on public.toolbar_reengagement_queue from anon;

drop policy if exists "rq_select_team"  on public.toolbar_reengagement_queue;
drop policy if exists "rq_insert_own"   on public.toolbar_reengagement_queue;
drop policy if exists "rq_update_own"   on public.toolbar_reengagement_queue;

-- SELECT abierto al equipo (consistente con feedback team-wide y agent worker)
create policy "rq_select_team" on public.toolbar_reengagement_queue for select
  to authenticated using (true);

-- INSERT: solo el propio MB. (El agent corre con service_role y bypassea RLS)
create policy "rq_insert_own" on public.toolbar_reengagement_queue for insert
  to authenticated with check (mb_email = (auth.jwt() ->> 'email'));

-- UPDATE: el propio MB puede cancelar (cambiar status a 'cancelled')
create policy "rq_update_own" on public.toolbar_reengagement_queue for update
  to authenticated using (mb_email = (auth.jwt() ->> 'email'));

-- ============================================================
-- Activar flag y setear wait_days a 11 (antes era 5 por default)
-- ============================================================
insert into public.toolbar_config (key, value) values
  ('agent_reengagement_enabled',   'true'),
  ('agent_reengagement_wait_days', '11')
on conflict (key) do update set value = excluded.value;
