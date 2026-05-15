-- v5.0.34: snooze 21d por user (cada MB tiene su propia "bolsa" de descarte temporal)
-- Cuando un MB hace click en 💤 sobre un prospect, NO le aparece más por 21 días.
-- A los otros MBs igual les aparece (no afecta su pool).

create table if not exists public.toolbar_user_snoozed_prospects (
  id            bigserial primary key,
  user_email    text not null,
  domain        text not null,
  snooze_until  timestamptz not null,
  created_at    timestamptz default now(),
  unique (user_email, domain)
);

create index if not exists idx_user_snoozed_user_until
  on public.toolbar_user_snoozed_prospects(user_email, snooze_until);

alter table public.toolbar_user_snoozed_prospects enable row level security;

drop policy if exists user_snoozed_own_read on public.toolbar_user_snoozed_prospects;
create policy user_snoozed_own_read on public.toolbar_user_snoozed_prospects
  for select to authenticated
  using (user_email = (auth.jwt() ->> 'email') or (auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com');

drop policy if exists user_snoozed_own_insert on public.toolbar_user_snoozed_prospects;
create policy user_snoozed_own_insert on public.toolbar_user_snoozed_prospects
  for insert to authenticated
  with check (user_email = (auth.jwt() ->> 'email'));

drop policy if exists user_snoozed_own_delete on public.toolbar_user_snoozed_prospects;
create policy user_snoozed_own_delete on public.toolbar_user_snoozed_prospects
  for delete to authenticated
  using (user_email = (auth.jwt() ->> 'email') or (auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com');
