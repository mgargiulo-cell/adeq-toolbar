-- ============================================================
-- Autopilot daily stats — by user / day / reason
-- Each row is an aggregated counter; Railway upserts +1 per discard.
-- ============================================================
create table if not exists public.toolbar_autopilot_stats (
  user_email text not null,
  day        date not null default current_date,
  reason     text not null,                 -- traffic_low, country_blacklist, low_score, added_to_review, ...
  count      int  not null default 0,
  primary key (user_email, day, reason)
);

create index if not exists toolbar_autopilot_stats_day_idx
  on public.toolbar_autopilot_stats(day desc, user_email);

alter table public.toolbar_autopilot_stats enable row level security;
revoke insert, update, delete on public.toolbar_autopilot_stats from anon, authenticated;

-- Authenticated can read their own stats
drop policy if exists "stats_select_own" on public.toolbar_autopilot_stats;
create policy "stats_select_own" on public.toolbar_autopilot_stats for select
  to authenticated using (user_email = auth.jwt() ->> 'email');

-- Increment helper — Railway calls this via service role
create or replace function public.bump_autopilot_stat(
  p_user_email text,
  p_reason text,
  p_count int default 1
) returns void
language sql security definer as $$
  insert into public.toolbar_autopilot_stats(user_email, day, reason, count)
  values (p_user_email, current_date, p_reason, p_count)
  on conflict (user_email, day, reason)
  do update set count = toolbar_autopilot_stats.count + excluded.count;
$$;

grant execute on function public.bump_autopilot_stat(text, text, int) to service_role;
