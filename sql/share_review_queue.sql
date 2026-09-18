-- ============================================================
-- Compartir review_queue entre los 3 MBs
-- Antes: cada user veía solo lo creado por él (created_by = jwt email)
-- Ahora: SELECT/UPDATE compartido (descubrimiento + claim cooperativo)
--        INSERT/DELETE siguen scoped al user (no podés borrar lo de otro)
-- ============================================================

drop policy if exists "rev_select_own" on public.toolbar_review_queue;
drop policy if exists "rev_update_own" on public.toolbar_review_queue;
drop policy if exists "rev_delete_own" on public.toolbar_review_queue;
drop policy if exists "rev_select_any" on public.toolbar_review_queue;
drop policy if exists "rev_insert_own" on public.toolbar_review_queue;
drop policy if exists "rev_update_any" on public.toolbar_review_queue;
drop policy if exists "rev_delete_own_only" on public.toolbar_review_queue;

-- Cualquier authenticated VE todos los prospects descubiertos por el autopilot
create policy "rev_select_any" on public.toolbar_review_queue for select
  to authenticated using (true);

-- INSERT solo si created_by coincide (Railway con service_role bypass siempre puede)
create policy "rev_insert_own" on public.toolbar_review_queue for insert
  to authenticated with check (created_by = auth.jwt() ->> 'email');

-- Cualquier user puede validar/rechazar/cambiar status (claim cooperativo)
create policy "rev_update_any" on public.toolbar_review_queue for update
  to authenticated using (true);

-- DELETE solo lo creado por uno (Clear all sigue siendo per-user — protege a otros)
create policy "rev_delete_own_only" on public.toolbar_review_queue for delete
  to authenticated using (created_by = auth.jwt() ->> 'email');
