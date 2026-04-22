-- ============================================================
-- Pitch feedback + Voyage RAG
-- Run once in Supabase SQL editor.
-- ============================================================

create extension if not exists vector;

create table if not exists public.toolbar_pitch_feedback (
  id            bigint generated always as identity primary key,
  user_email    text not null,
  domain        text,
  category      text,
  geo           text,
  language      text,
  traffic       bigint,
  pitch_body    text not null,
  pitch_subject text,
  context       text not null,                    -- the literal text that was embedded
  embedding     vector(1024) not null,            -- voyage-3 / voyage-3-large dimension
  action        text not null check (action in ('liked','disliked')),
  created_at    timestamptz not null default now()
);

create index if not exists toolbar_pitch_feedback_user_idx
  on public.toolbar_pitch_feedback(user_email, action, created_at desc);

-- HNSW index for fast cosine-similarity search
create index if not exists toolbar_pitch_feedback_embedding_idx
  on public.toolbar_pitch_feedback
  using hnsw (embedding vector_cosine_ops);

-- ── RLS ────────────────────────────────────────────────────────
alter table public.toolbar_pitch_feedback enable row level security;
revoke insert, update, delete on public.toolbar_pitch_feedback from anon;

drop policy if exists "pf_select_own"  on public.toolbar_pitch_feedback;
drop policy if exists "pf_insert_own"  on public.toolbar_pitch_feedback;
drop policy if exists "pf_delete_own"  on public.toolbar_pitch_feedback;

create policy "pf_select_own" on public.toolbar_pitch_feedback for select
  to authenticated using (user_email = auth.jwt() ->> 'email');
create policy "pf_insert_own" on public.toolbar_pitch_feedback for insert
  to authenticated with check (user_email = auth.jwt() ->> 'email');
create policy "pf_delete_own" on public.toolbar_pitch_feedback for delete
  to authenticated using (user_email = auth.jwt() ->> 'email');

-- ── RPC for similarity search ──────────────────────────────────
create or replace function public.match_pitch_feedback(
  query_embedding vector(1024),
  match_user_email text,
  match_action text,
  match_count int default 3
)
returns table (
  id bigint,
  domain text,
  category text,
  geo text,
  pitch_body text,
  pitch_subject text,
  similarity float
)
language sql stable security invoker as $$
  select
    f.id,
    f.domain,
    f.category,
    f.geo,
    f.pitch_body,
    f.pitch_subject,
    1 - (f.embedding <=> query_embedding) as similarity
  from public.toolbar_pitch_feedback f
  where f.user_email = match_user_email
    and f.action = match_action
  order by f.embedding <=> query_embedding
  limit match_count;
$$;

-- Allow authenticated users to call the RPC; RLS in the SELECT enforces row-level scope
grant execute on function public.match_pitch_feedback(vector, text, text, int) to authenticated;
