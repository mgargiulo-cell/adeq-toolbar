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

-- ═══════════════════════════════════════════════════════════════════════════════════════
-- PARTE 2 — LO CRÍTICO (auditoría 2026-08-04). Correr TODO junto.
-- ═══════════════════════════════════════════════════════════════════════════════════════

-- ── 6. LAS API KEYS ERAN LEGIBLES POR CUALQUIER USUARIO AUTENTICADO ────────────────────
-- La cadena verificada: (a) el registro del proyecto está ABIERTO, (b) el allowlist de login
-- vive en popup.js —JS público, se saltea llamando a Supabase directo—, y (c) la policy de
-- SELECT sobre toolbar_config era `using (true)` para authenticated.
-- Resultado: cualquiera se crea una cuenta con un Gmail, confirma el mail, hace UN
-- `GET /rest/v1/toolbar_config?select=key,value` y se lleva Apollo + RapidAPI + Gemini +
-- Monday + MillionVerifier. El api-proxy queda bypasseado: usa las keys crudas.
drop policy if exists toolbar_config_select_auth on public.toolbar_config;
drop policy if exists cfg_read_safe             on public.toolbar_config;
create policy cfg_read_safe on public.toolbar_config
  for select to authenticated
  using (
    key not like '%api_key%' and key not like '%_key' and key not like '%secret%'
    and key not like '%token%' and key not like '%password%'
  );

-- ── 7. LA ALLOWLIST DEL PROXY ERA AUTO-SERVICIO ────────────────────────────────────────
-- El proxy lee la allowlist de agent_enabled_users / agent_whitelist / proxy_extra_users,
-- pero el popup ESCRIBE agent_enabled_users con el JWT del usuario, y la policy RESTRICTIVE
-- del 15/07 solo protegía apollo_api_key, rapidapi_key, gemini_api_key y dos caps.
-- O sea: cualquier autenticado se agregaba solo a la lista y entraba al proxy.
drop policy if exists cfg_no_write_sensitive on public.toolbar_config;
create policy cfg_no_write_sensitive on public.toolbar_config
  as restrictive for all to authenticated
  using (
    key not in (
      'apollo_api_key','rapidapi_key','gemini_api_key','anthropic_api_key','voyage_api_key',
      'monday_api_key','millionverifier_api_key','serper_api_key',
      'agent_enabled_users','agent_whitelist','proxy_extra_users',
      'kill_switch','kill_switch_reason','security_alert_email','security_watchdog_enabled',
      'csv_queue_daily_cap','autopilot_daily_cap_global','rapidapi_daily_limit',
      'apollo_daily_limit','serper_contact_daily_cap','agent_max_total_sends_per_day',
      'millionverifier_daily_cap','threshold_traffic_max'
    )
  )
  with check (
    key not in (
      'apollo_api_key','rapidapi_key','gemini_api_key','anthropic_api_key','voyage_api_key',
      'monday_api_key','millionverifier_api_key','serper_api_key',
      'agent_enabled_users','agent_whitelist','proxy_extra_users',
      'kill_switch','kill_switch_reason','security_alert_email','security_watchdog_enabled',
      'csv_queue_daily_cap','autopilot_daily_cap_global','rapidapi_daily_limit',
      'apollo_daily_limit','serper_contact_daily_cap','agent_max_total_sends_per_day',
      'millionverifier_daily_cap','threshold_traffic_max'
    )
  );

-- ── 8. HISTORIAL DE CAMBIOS EN LA CONFIG ───────────────────────────────────────────────
-- Hoy no hay rastro: si alguien se agrega a agent_enabled_users, no queda registro de nada.
create or replace function public.log_config_change()
returns trigger language plpgsql security definer set search_path = public as $$
begin
  if new.key ~ '(api_key|token|secret|enabled_users|whitelist|kill_switch|daily_cap|daily_limit)' then
    insert into toolbar_security_events (kind, severity, actor, detail)
    values ('config_cambiada', 'warn', coalesce(current_setting('request.jwt.claim.email', true), 'service'),
            jsonb_build_object('key', new.key, 'antes', left(coalesce(old.value,''),80), 'ahora', left(coalesce(new.value,''),80)));
  end if;
  return new;
end $$;
drop trigger if exists trg_log_config_change on public.toolbar_config;
create trigger trg_log_config_change after update on public.toolbar_config
  for each row execute function public.log_config_change();

-- ── 10. BOTÓN DE PÁNICO DESDE EL PANEL ─────────────────────────────────────────────────
-- Tras el punto 7, `kill_switch` ya no es escribible por usuarios autenticados — que es lo
-- correcto, pero deja al botón del panel sin poder tocarlo. Este RPC es la puerta controlada:
-- corre como security definer (privilegios de la función, no del que llama) y valida que el
-- mail de quien invoca esté en la allowlist. Así el botón funciona para los 3 MBs y para nadie más.
create or replace function public.toggle_kill_switch(p_on boolean, p_motivo text default '')
returns table (activo boolean, motivo text)
language plpgsql security definer set search_path = public as $$
declare v_email text; v_permitidos jsonb; v_motivo text;
begin
  v_email := lower(coalesce(current_setting('request.jwt.claim.email', true), ''));
  if v_email = '' then raise exception 'sin identidad'; end if;

  select value::jsonb into v_permitidos from toolbar_config where key = 'agent_enabled_users';
  if v_permitidos is null
     or not exists (select 1 from jsonb_array_elements_text(v_permitidos) e where lower(e) = v_email)
  then
    insert into toolbar_security_events (kind, severity, actor, detail)
    values ('panic_intento_no_autorizado','critical', v_email, jsonb_build_object('accion', p_on));
    raise exception 'usuario no autorizado';
  end if;

  v_motivo := case when p_on
    then to_char(now(),'YYYY-MM-DD HH24:MI') || ' — activado a mano por ' || v_email ||
         case when coalesce(p_motivo,'') <> '' then ': ' || left(p_motivo, 120) else '' end
    else '' end;

  update toolbar_config set value = case when p_on then 'true' else 'false' end where key = 'kill_switch';
  update toolbar_config set value = v_motivo where key = 'kill_switch_reason';
  -- Al soltarlo a mano se limpia la marca de automático, para que el vigilante no lo re-suelte solo.
  update toolbar_config set value = '' where key = 'kill_switch_auto_at';

  insert into toolbar_security_events (kind, severity, actor, detail)
  values (case when p_on then 'panic_activado_panel' else 'panic_desactivado_panel' end,
          'warn', v_email, jsonb_build_object('motivo', v_motivo));

  return query select p_on, v_motivo;
end $$;
grant execute on function public.toggle_kill_switch(boolean, text) to authenticated;

-- Estado del freno, legible desde el panel sin exponer nada más.
create or replace function public.kill_switch_estado()
returns table (activo boolean, motivo text, auto boolean)
language sql security definer set search_path = public as $$
  select
    coalesce((select value from toolbar_config where key='kill_switch'), 'false') = 'true',
    coalesce((select value from toolbar_config where key='kill_switch_reason'), ''),
    coalesce((select value from toolbar_config where key='kill_switch_auto_at'), '') <> ''
$$;
grant execute on function public.kill_switch_estado() to authenticated;

insert into toolbar_config (key, value) values ('kill_switch_auto_at','') on conflict (key) do nothing;

-- ═══════════════════════════════════════════════════════════════════════════════════════
-- DIAGNÓSTICO — no cambian nada, van al final para poder ver el resultado
-- ═══════════════════════════════════════════════════════════════════════════════════════

-- ── 5. RLS: verificar qué tablas quedaron expuestas ────────────────────────────────────
-- Este SELECT no cambia nada: lista las toolbar_* SIN row level security. Cualquiera con la
-- anon key (que viaja dentro de la extensión y es pública) puede leerlas y escribirlas.
select tablename,
       case when rowsecurity then 'RLS ON' else '⚠️ RLS OFF — EXPUESTA' end as estado,
       (select count(*) from pg_policies p where p.tablename = t.tablename) as politicas
from pg_tables t
where schemaname = 'public' and tablename like 'toolbar_%'
order by rowsecurity asc, tablename;

-- ── 9. CHEQUEO: ¿quién usó el proxy hoy y está fuera de la allowlist? ──────────────────
-- Detecta el abuso al PRIMER uso, no al llegar a un umbral de volumen.
select u.user_email, u.total, u.by_provider
from toolbar_api_usage u
where u.day = current_date
  and u.user_email not in (
    select lower(jsonb_array_elements_text(value::jsonb))
    from toolbar_config where key = 'agent_enabled_users'
  );

-- ── 11. REPORTE DE SEGURIDAD PARA EL PANEL ─────────────────────────────────────────────
-- toolbar_security_events queda SIN políticas a propósito: nadie con anon/authenticated la
-- toca. Pero el botón "Copiar reporte" del panel necesita leerla. Misma solución que el botón
-- de pánico: un RPC security definer que valida que el mail esté en la allowlist.
create or replace function public.security_report(p_limit int default 60)
returns table (created_at timestamptz, kind text, severity text, actor text, detail jsonb)
language plpgsql security definer set search_path = public as $$
declare v_email text; v_permitidos jsonb;
begin
  v_email := lower(coalesce(current_setting('request.jwt.claim.email', true), ''));
  select value::jsonb into v_permitidos from toolbar_config where key = 'agent_enabled_users';
  if v_email = '' or v_permitidos is null
     or not exists (select 1 from jsonb_array_elements_text(v_permitidos) e where lower(e) = v_email)
  then raise exception 'no autorizado'; end if;

  return query
    select e.created_at, e.kind, e.severity, e.actor, e.detail
    from toolbar_security_events e
    order by e.created_at desc
    limit least(coalesce(p_limit, 60), 200);
end $$;
grant execute on function public.security_report(int) to authenticated;

-- ── 12. BLOQUEO DE REGISTROS NO AUTORIZADOS ────────────────────────────────────────────
-- El toggle "Allow new users to sign up" del dashboard no aparecía en la versión del panel del
-- user, y además es frágil: una casilla que se mueve entre versiones y que alguien puede
-- desmarcar sin dejar rastro. Esto es más fuerte: valida contra la propia allowlist del
-- proyecto y deja registrado CADA intento, así el vigilante avisa si alguien está probando.
create or replace function public.bloquear_signups_no_autorizados()
returns trigger language plpgsql security definer set search_path = public as $$
declare v_permitidos jsonb;
begin
  select value::jsonb into v_permitidos from toolbar_config where key = 'agent_enabled_users';
  -- Falla PERMISIVO a propósito: sin lista cargada no bloquea. Prefiero eso a dejar afuera al
  -- dueño por un error de configuración.
  if v_permitidos is null or jsonb_array_length(v_permitidos) = 0 then return new; end if;
  if not exists (
    select 1 from jsonb_array_elements_text(v_permitidos) e
    where lower(e) = lower(new.email)
  ) then
    insert into toolbar_security_events (kind, severity, actor, detail)
    values ('signup_bloqueado', 'critical', lower(coalesce(new.email, '?')),
            jsonb_build_object('cuando', now()));
    raise exception 'registro no permitido';
  end if;
  return new;
end $$;
drop trigger if exists trg_bloquear_signups on auth.users;
create trigger trg_bloquear_signups
  before insert on auth.users
  for each row execute function public.bloquear_signups_no_autorizados();
-- OJO a futuro: si sumás un MB nuevo, agregalo PRIMERO a agent_enabled_users y después que se
-- registre. Si no, el trigger le rechaza el alta.

-- ── 13. PROBAR EL ALERTADO ─────────────────────────────────────────────────────────────
-- El mail de alerta solo se dispara ante un incidente real. Esta bandera fuerza uno de prueba
-- en el próximo ciclo del worker (~1-2 min) y se apaga sola.
insert into toolbar_config (key, value) values ('security_alert_test','false')
on conflict (key) do nothing;
-- Para probar:  UPDATE toolbar_config SET value='true' WHERE key='security_alert_test';
