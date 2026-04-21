-- ============================================================
-- RLS HARDENING — ADEQ Toolbar
-- Run in Supabase SQL editor. Review before applying.
-- ============================================================
-- Goal:
--   1. Every toolbar_* table has RLS ON.
--   2. anon role cannot DELETE/UPDATE rows (only read where explicitly allowed).
--   3. authenticated role can read/write only their own rows (by media_buyer / user_email / created_by).
--   4. toolbar_config readable by authenticated (needed to boot), writable only by service_role.
-- ============================================================

-- --- toolbar_config: read-only for clients, writable only by service_role ---
alter table public.toolbar_config enable row level security;
drop policy if exists "toolbar_config_select_auth" on public.toolbar_config;
create policy "toolbar_config_select_auth"
  on public.toolbar_config for select
  to authenticated using (true);
-- (no insert/update/delete policy for anon/authenticated → denied by default)

-- --- toolbar_historial: user scoped by media_buyer ---
alter table public.toolbar_historial enable row level security;
drop policy if exists "hist_select_own"   on public.toolbar_historial;
drop policy if exists "hist_insert_own"   on public.toolbar_historial;
drop policy if exists "hist_update_own"   on public.toolbar_historial;
drop policy if exists "hist_delete_own"   on public.toolbar_historial;
create policy "hist_select_own" on public.toolbar_historial for select
  to authenticated using (media_buyer = auth.jwt() ->> 'email');
create policy "hist_insert_own" on public.toolbar_historial for insert
  to authenticated with check (media_buyer = auth.jwt() ->> 'email');
create policy "hist_update_own" on public.toolbar_historial for update
  to authenticated using (media_buyer = auth.jwt() ->> 'email');
create policy "hist_delete_own" on public.toolbar_historial for delete
  to authenticated using (media_buyer = auth.jwt() ->> 'email');

-- --- toolbar_keywords: shared DB (read-all), insert/delete authenticated only ---
alter table public.toolbar_keywords enable row level security;
drop policy if exists "kw_select_auth" on public.toolbar_keywords;
drop policy if exists "kw_insert_auth" on public.toolbar_keywords;
drop policy if exists "kw_delete_auth" on public.toolbar_keywords;
create policy "kw_select_auth" on public.toolbar_keywords for select
  to authenticated using (true);
create policy "kw_insert_auth" on public.toolbar_keywords for insert
  to authenticated with check (true);
create policy "kw_delete_auth" on public.toolbar_keywords for delete
  to authenticated using (true);

-- --- toolbar_sendtrack: user scoped ---
alter table public.toolbar_sendtrack enable row level security;
drop policy if exists "send_select_own" on public.toolbar_sendtrack;
drop policy if exists "send_insert_own" on public.toolbar_sendtrack;
drop policy if exists "send_update_own" on public.toolbar_sendtrack;
create policy "send_select_own" on public.toolbar_sendtrack for select
  to authenticated using (media_buyer = auth.jwt() ->> 'email' or media_buyer is null);
create policy "send_insert_own" on public.toolbar_sendtrack for insert
  to authenticated with check (true);
create policy "send_update_own" on public.toolbar_sendtrack for update
  to authenticated using (true);

-- --- toolbar_traffic_cache / toolbar_similar_cache: shared read, authenticated write ---
alter table public.toolbar_traffic_cache enable row level security;
drop policy if exists "tcache_select" on public.toolbar_traffic_cache;
drop policy if exists "tcache_upsert" on public.toolbar_traffic_cache;
create policy "tcache_select" on public.toolbar_traffic_cache for select
  to authenticated using (true);
create policy "tcache_upsert" on public.toolbar_traffic_cache for insert
  to authenticated with check (true);

alter table public.toolbar_similar_cache enable row level security;
drop policy if exists "scache_select" on public.toolbar_similar_cache;
drop policy if exists "scache_upsert" on public.toolbar_similar_cache;
create policy "scache_select" on public.toolbar_similar_cache for select
  to authenticated using (true);
create policy "scache_upsert" on public.toolbar_similar_cache for insert
  to authenticated with check (true);

-- --- toolbar_autopilot_feedback: user scoped ---
alter table public.toolbar_autopilot_feedback enable row level security;
drop policy if exists "fb_select_own" on public.toolbar_autopilot_feedback;
drop policy if exists "fb_insert_own" on public.toolbar_autopilot_feedback;
create policy "fb_select_own" on public.toolbar_autopilot_feedback for select
  to authenticated using (user_email = auth.jwt() ->> 'email');
create policy "fb_insert_own" on public.toolbar_autopilot_feedback for insert
  to authenticated with check (user_email = auth.jwt() ->> 'email');

-- --- toolbar_csv_queue / toolbar_review_queue: user scoped by uploaded_by / assigned_to ---
-- Adjust column names if your schema uses different ones.
alter table public.toolbar_csv_queue enable row level security;
drop policy if exists "csv_select_own" on public.toolbar_csv_queue;
drop policy if exists "csv_insert_own" on public.toolbar_csv_queue;
create policy "csv_select_own" on public.toolbar_csv_queue for select
  to authenticated using (uploaded_by = auth.jwt() ->> 'email');
create policy "csv_insert_own" on public.toolbar_csv_queue for insert
  to authenticated with check (uploaded_by = auth.jwt() ->> 'email');

alter table public.toolbar_review_queue enable row level security;
drop policy if exists "rev_select_own"   on public.toolbar_review_queue;
drop policy if exists "rev_update_own"   on public.toolbar_review_queue;
create policy "rev_select_own" on public.toolbar_review_queue for select
  to authenticated using (assigned_to = auth.jwt() ->> 'email');
create policy "rev_update_own" on public.toolbar_review_queue for update
  to authenticated using (assigned_to = auth.jwt() ->> 'email');

-- --- toolbar_pitch_drafts: user scoped ---
alter table public.toolbar_pitch_drafts enable row level security;
drop policy if exists "pd_select_own" on public.toolbar_pitch_drafts;
drop policy if exists "pd_insert_own" on public.toolbar_pitch_drafts;
drop policy if exists "pd_update_own" on public.toolbar_pitch_drafts;
drop policy if exists "pd_delete_own" on public.toolbar_pitch_drafts;
create policy "pd_select_own" on public.toolbar_pitch_drafts for select
  to authenticated using (user_email = auth.jwt() ->> 'email');
create policy "pd_insert_own" on public.toolbar_pitch_drafts for insert
  to authenticated with check (user_email = auth.jwt() ->> 'email');
create policy "pd_update_own" on public.toolbar_pitch_drafts for update
  to authenticated using (user_email = auth.jwt() ->> 'email');
create policy "pd_delete_own" on public.toolbar_pitch_drafts for delete
  to authenticated using (user_email = auth.jwt() ->> 'email');

-- --- Revoke anon role from write operations on all toolbar_* tables ---
do $$
declare r record;
begin
  for r in select tablename from pg_tables where schemaname='public' and tablename like 'toolbar_%' loop
    execute format('revoke insert, update, delete on public.%I from anon', r.tablename);
  end loop;
end $$;
