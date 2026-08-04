-- ═══════════════════════════════════════════════════════════════════════════════════════
-- BLINDAJE DE SEGURIDAD — Maxi 2026-07-28
-- Correr UNA sola vez en el SQL editor de Supabase.
-- ═══════════════════════════════════════════════════════════════════════════════════════

-- ── 1. CUOTA ATÓMICA ───────────────────────────────────────────────────────────────────
-- El api-proxy leía el contador y después lo escribía (read-then-write). Con requests
-- concurrentes todas leen el mismo valor y el tope se puede pasar por lejos: 50 llamadas en
-- paralelo con la cuota en 499 pasan las 50. Esta función incrementa y devuelve el valor nuevo
-- en UNA operación atómica, así el límite es real.
create or replace function public.bump_api_usage(p_email text, p_provider text)
returns table (total int, prov int)
language plpgsql security definer set search_path = public as $$
declare v_total int; v_by jsonb;
begin
  insert into toolbar_api_usage (user_email, day, total, by_provider, updated_at)
  values (lower(p_email), current_date, 1, jsonb_build_object(p_provider, 1), now())
  on conflict (user_email, day) do update
    set total = toolbar_api_usage.total + 1,
        by_provider = jsonb_set(
          coalesce(toolbar_api_usage.by_provider, '{}'::jsonb),
          array[p_provider],
          to_jsonb(coalesce((toolbar_api_usage.by_provider ->> p_provider)::int, 0) + 1)
        ),
        updated_at = now()
  returning toolbar_api_usage.total, toolbar_api_usage.by_provider into v_total, v_by;
  return query select v_total, coalesce((v_by ->> p_provider)::int, 0);
end $$;
revoke all on function public.bump_api_usage(text,text) from public, anon, authenticated;

-- ── 2. TABLA DE INCIDENTES DE SEGURIDAD ────────────────────────────────────────────────
-- Todo lo anómalo se registra acá. Es la fuente del alertado por mail y del panel.
create table if not exists public.toolbar_security_events (
  id          bigserial primary key,
  kind        text not null,               -- unauthorized_proxy | quota_burst | pixel_flood | ...
  severity    text not null default 'warn',-- info | warn | critical
  actor       text,                        -- email, hash de IP o "anon"
  detail      jsonb not null default '{}'::jsonb,
  created_at  timestamptz not null default now(),
  notified_at timestamptz
);
create index if not exists idx_secev_created on public.toolbar_security_events (created_at desc);
create index if not exists idx_secev_kind    on public.toolbar_security_events (kind, created_at desc);
alter table public.toolbar_security_events enable row level security;
-- Sin políticas = nadie con anon/authenticated puede leerla ni escribirla. Solo service_role.

-- ── 3. RATE LIMIT DEL PIXEL ────────────────────────────────────────────────────────────
-- track-open es público por diseño (los clientes de mail piden la imagen sin autenticarse).
-- Sin cota, cualquiera puede pedirla en loop y llenar toolbar_email_opens: costo de Supabase,
-- métricas envenenadas y el re-engagement decidiendo sobre datos falsos.
create table if not exists public.toolbar_pixel_ratelimit (
  bucket     text primary key,             -- '<ip_hash>:<yyyy-mm-dd-hh>'
  hits       int not null default 1,
  updated_at timestamptz not null default now()
);
create index if not exists idx_pixelrl_updated on public.toolbar_pixel_ratelimit (updated_at);
alter table public.toolbar_pixel_ratelimit enable row level security;

create or replace function public.bump_pixel_hits(p_bucket text)
returns int language plpgsql security definer set search_path = public as $$
declare v int;
begin
  insert into toolbar_pixel_ratelimit (bucket, hits, updated_at)
  values (p_bucket, 1, now())
  on conflict (bucket) do update set hits = toolbar_pixel_ratelimit.hits + 1, updated_at = now()
  returning hits into v;
  return v;
end $$;
revoke all on function public.bump_pixel_hits(text) from public, anon, authenticated;

-- ── 4. INTERRUPTOR DE EMERGENCIA ───────────────────────────────────────────────────────
-- Un solo valor que apaga TODO el gasto externo. Lo lee el api-proxy en cada request y el
-- worker en cada ciclo. Es el botón de pánico de 1 clic.
insert into toolbar_config (key, value) values ('kill_switch','false')
on conflict (key) do nothing;
insert into toolbar_config (key, value) values ('security_alert_email','mgargiulo@adeqmedia.com')
on conflict (key) do nothing;

-- ── 5. RLS: verificar qué tablas quedaron expuestas ────────────────────────────────────
-- Este SELECT no cambia nada: lista las toolbar_* SIN row level security. Cualquiera con la
-- anon key (que viaja dentro de la extensión y es pública) puede leerlas y escribirlas.
select tablename,
       case when rowsecurity then 'RLS ON' else '⚠️ RLS OFF — EXPUESTA' end as estado,
       (select count(*) from pg_policies p where p.tablename = t.tablename) as politicas
from pg_tables t
where schemaname = 'public' and tablename like 'toolbar_%'
order by rowsecurity asc, tablename;
