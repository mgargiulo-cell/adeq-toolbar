-- v5.0.33: agregar categorías a toolbar_url_blocklist
-- Permite distinguir entre bloqueos manuales, corporativos, y auto-detectados (inoperativos).

alter table public.toolbar_url_blocklist
  add column if not exists category text default 'manual',
  add column if not exists added_by text,
  add column if not exists reason text,
  add column if not exists created_at timestamptz default now();

-- Backfill: lo que ya estaba se marca como manual (los agregó admin)
update public.toolbar_url_blocklist
  set category = 'manual'
  where category is null;

-- Check constraint para categorías válidas
do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.toolbar_url_blocklist'::regclass
      and conname = 'toolbar_url_blocklist_category_check'
  ) then
    alter table public.toolbar_url_blocklist
      add constraint toolbar_url_blocklist_category_check
      check (category in ('manual', 'corporate', 'inoperativo'));
  end if;
end $$;

create index if not exists idx_url_blocklist_category on public.toolbar_url_blocklist(category);

-- Report
select category, count(*) from public.toolbar_url_blocklist group by category;
