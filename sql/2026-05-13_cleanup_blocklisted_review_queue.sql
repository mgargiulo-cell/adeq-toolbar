-- v5.0.33: limpiar review_queue de dominios que están en toolbar_url_blocklist.
-- Antes del fix, el worker autopilot no chequeaba la admin blocklist → entraban igual.
-- Después del fix, estos quedaron flotando. Los marcamos como rejected.

update public.toolbar_review_queue rq
  set status = 'rejected',
      validated_by = 'admin_blocklist_cleanup',
      validated_at = now()
  where status = 'pending'
    and lower(regexp_replace(rq.domain, '^www\.', '')) in (
      select lower(regexp_replace(domain, '^www\.', '')) from public.toolbar_url_blocklist
    );

-- Same para csv_queue.pending — no procesar
update public.toolbar_csv_queue cq
  set status = 'skipped',
      error_message = 'admin_blocklist_cleanup',
      processed_at = now()
  where status in ('pending', 'waiting_pool', 'next_day')
    and lower(regexp_replace(cq.domain, '^www\.', '')) in (
      select lower(regexp_replace(domain, '^www\.', '')) from public.toolbar_url_blocklist
    );

-- Report
select 'review_queue rejected' as type, count(*) as n from public.toolbar_review_queue where validated_by = 'admin_blocklist_cleanup'
union all
select 'csv_queue skipped' as type, count(*) as n from public.toolbar_csv_queue where error_message = 'admin_blocklist_cleanup';
