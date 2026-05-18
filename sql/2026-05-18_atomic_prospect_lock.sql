-- ============================================================
-- Atomic prospect lock — elimina race condition entre MBs.
--
-- Antes: popup hacía SELECT (getActiveProspectLock) + POST upsert
-- separadamente, con gap entre ambos. Dos MBs podían pisarse.
--
-- Ahora: RPC con pg_advisory_xact_lock(hash(domain)) que serializa
-- por dominio dentro de la transacción. Sólo una llamada por dominio
-- corre a la vez; el resto espera o ve el lock ya tomado.
-- ============================================================

create or replace function public.lock_prospect(
  p_domain  text,
  p_email   text,
  p_minutes int default 30
) returns table (
  ok          boolean,
  locked_by   text,
  expires_at  timestamptz
)
language plpgsql
security definer
as $$
declare
  v_lock_key  bigint      := hashtext('prospect_lock:' || p_domain);
  v_existing  public.toolbar_prospect_locks%rowtype;
  v_expires   timestamptz := now() + (p_minutes || ' minutes')::interval;
  v_email     text        := lower(p_email);
begin
  if p_domain is null or p_email is null then
    return query select false, null::text, null::timestamptz;
    return;
  end if;

  -- Serializa concurrentes para el MISMO dominio (transaction-scoped).
  perform pg_advisory_xact_lock(v_lock_key);

  select * into v_existing
    from public.toolbar_prospect_locks
   where domain = p_domain;

  -- Lock activo de OTRO MB → rechazar sin tocar nada.
  if found
     and v_existing.expires_at > now()
     and lower(v_existing.locked_by) <> v_email then
    return query select false, v_existing.locked_by, v_existing.expires_at;
    return;
  end if;

  -- Libre, expirado, o nuestro → upsert.
  insert into public.toolbar_prospect_locks(domain, locked_by, locked_at, expires_at)
  values (p_domain, v_email, now(), v_expires)
  on conflict (domain) do update
    set locked_by  = excluded.locked_by,
        locked_at  = excluded.locked_at,
        expires_at = excluded.expires_at;

  return query select true, v_email, v_expires;
end;
$$;

grant execute on function public.lock_prospect(text, text, int) to authenticated;
