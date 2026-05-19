-- ============================================================
-- toolbar_notifications — bell/notification center per MB
-- ============================================================
-- Política user 2026-05-19: cada MB tiene su propio inbox de alertas.
-- Generadas por scanners en el worker (low_prospecting, bounce_high,
-- open_low, source_insight, positive, system_failure) y por triggers
-- del popup (load_error). El MB las marca como read/dismissed desde
-- el bell UI del popup.
--
-- Tipos esperados (type):
--   low_prospecting   — < 30 emails/día evaluado L/J sobre últimos 5 días hábiles
--   bounce_high       — bounce rate semanal > 20%
--   open_low          — open rate semanal < 15%
--   source_insight    — comparativa entre sources del MB (info, no warning)
--   positive          — refuerzo positivo (open > 25%, racha de 5 días con >=30 emails)
--   system_failure    — agent worker exception / feeder fail / load fail
--   load_error        — error puntual al cargar un lead (lo dispara el popup)
--
-- Severities: info | success | warning | error
-- ============================================================

create table if not exists public.toolbar_notifications (
  id            bigserial primary key,
  mb_email      text   not null,                        -- destinatario (o "_admin" para alertas globales)
  type          text   not null,
  severity      text   not null default 'info' check (severity in ('info','success','warning','error')),
  title         text   not null,
  body          text,
  metadata      jsonb  not null default '{}'::jsonb,    -- ej {"rate": 0.22, "threshold": 0.20, "period_days": 7}
  read_at       timestamptz,
  dismissed_at  timestamptz,
  created_at    timestamptz not null default now(),
  -- Dedup: para evitar 50 alertas del mismo tipo, un mismo (mb, type, dedup_key)
  -- dentro del mismo día reemplaza la anterior en vez de duplicar.
  -- (uniqueness se garantiza via índice expression-based abajo — Postgres no
  -- soporta expresiones en CONSTRAINT UNIQUE.)
  dedup_key     text
);

-- created_at AT TIME ZONE 'UTC' es IMMUTABLE (TZ hardcoded); date(timestamptz)
-- solo no porque depende del TZ de sesión.
create unique index if not exists uq_notif_dedup
  on public.toolbar_notifications (
    mb_email, type, dedup_key, ((created_at AT TIME ZONE 'UTC')::date)
  );

create index if not exists idx_notif_mb_unread on public.toolbar_notifications(mb_email, read_at) where read_at is null;
create index if not exists idx_notif_created on public.toolbar_notifications(created_at desc);

comment on table public.toolbar_notifications is
  'Inbox de alertas por MB. Generadas por scanners en el worker y triggers del popup.';
comment on column public.toolbar_notifications.dedup_key is
  'Key opcional para deduplicar (ej "week-2026-W20"). Misma (mb, type, dedup_key, día) → reemplaza.';

-- RLS: cada MB lee/escribe SOLO sus propias notificaciones. Admins leen todas.
alter table public.toolbar_notifications enable row level security;

drop policy if exists "notif_admin_all" on public.toolbar_notifications;
create policy "notif_admin_all" on public.toolbar_notifications
  for all using (auth.jwt() ->> 'email' in (
    'mgargiulo@adeqmedia.com', 'dhorovitz@adeqmedia.com', 'sales@adeqmedia.com'
  ));

drop policy if exists "notif_mb_own" on public.toolbar_notifications;
create policy "notif_mb_own" on public.toolbar_notifications
  for select using (mb_email = lower(auth.jwt() ->> 'email') or mb_email = '_admin');

drop policy if exists "notif_mb_update_own" on public.toolbar_notifications;
create policy "notif_mb_update_own" on public.toolbar_notifications
  for update using (mb_email = lower(auth.jwt() ->> 'email'));
