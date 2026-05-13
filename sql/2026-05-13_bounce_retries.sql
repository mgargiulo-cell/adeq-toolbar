-- ============================================================
-- toolbar_bounce_retries — auto-retry a email alternativo cuando bounce
-- ============================================================
-- Cuando scanBouncesForUser detecta un bounce de un email enviado por el
-- agent, el worker (processBounceRetries) pickea el siguiente mejor email
-- del lead.emails (source-strict: apollo > informer > scrape > generic)
-- excluyendo los ya bounced. Si no hay alternativa → freeze 60d.
--
-- Guards:
--   - Max 2 retries por dominio (A → B → C, después freeze)
--   - Solo hard bounces (5xx) — soft (4xx) los reintenta Gmail solo
--   - Cooldown 24h entre intentos al mismo dominio
--   - Skip si dominio ya frozen
-- ============================================================

create table if not exists public.toolbar_bounce_retries (
  id                bigint generated always as identity primary key,
  domain            text not null,
  monday_item_id    bigint,
  mb_email          text not null,
  original_email    text not null,         -- el A que bounced
  retry_email       text not null,         -- el B que se intenta
  retry_source      text,                  -- source del retry email: apollo/informer/scrape/generic
  bounce_type       text check (bounce_type in ('hard','soft','unknown')) default 'unknown',
  attempt_number    int not null default 1,
  status            text not null default 'pending'
                    check (status in ('pending','sent','skipped_frozen','skipped_no_alt','skipped_cooldown','failed','cancelled')),
  -- Trazabilidad
  original_action_id bigint references public.toolbar_agent_actions(id) on delete set null,
  retry_action_id    bigint references public.toolbar_agent_actions(id) on delete set null,
  reason            text,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);

create index if not exists toolbar_bounce_retries_pending_idx
  on public.toolbar_bounce_retries (status, created_at)
  where status = 'pending';

create index if not exists toolbar_bounce_retries_domain_idx
  on public.toolbar_bounce_retries (domain, created_at desc);

-- ── RLS ──────────────────────────────────────────────────────
alter table public.toolbar_bounce_retries enable row level security;
revoke insert, update, delete on public.toolbar_bounce_retries from anon;

drop policy if exists "br_select_team" on public.toolbar_bounce_retries;

-- SELECT team-wide (consistente con feedback/reengagement)
create policy "br_select_team" on public.toolbar_bounce_retries for select
  to authenticated using (true);

-- INSERT/UPDATE solo el worker (service_role bypass) — no exponemos al MB.

-- ============================================================
-- Activación: flag agent_bounce_retry_enabled (default true)
-- ============================================================
insert into public.toolbar_config (key, value) values
  ('agent_bounce_retry_enabled', 'true'),
  ('agent_bounce_retry_max_attempts', '2'),  -- A → B → freeze (no llegamos a C)
  ('agent_bounce_retry_cooldown_h', '24')
on conflict (key) do nothing;
