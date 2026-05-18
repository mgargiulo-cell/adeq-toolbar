-- ============================================================
-- Índices faltantes para queries frecuentes.
--
-- Origen: AUDIT_2026-05-11.md item HIGH #3. El doc afirma haberlos
-- creado pero no quedaron migrados en sql/. Los recreo con
-- IF NOT EXISTS para que sea idempotente — si ya están en prod,
-- el statement es no-op.
--
-- Patrones cubiertos (verificados contra popup.js + auto-prospector):
--   • toolbar_api_usage:   filtro por user_email + day (apiProxy.js)
--   • toolbar_historial:   filtro por media_buyer + created_at (admin)
--   • toolbar_review_queue: status + created_at, status + traffic,
--                           created_by + status (worker + popup)
--   • toolbar_sendtrack:    domain + send_date (dedup de envíos)
-- ============================================================

create index if not exists idx_api_usage_user_day
  on public.toolbar_api_usage(user_email, day);

create index if not exists idx_historial_buyer_created
  on public.toolbar_historial(media_buyer, created_at desc);

create index if not exists idx_review_queue_status_created
  on public.toolbar_review_queue(status, created_at desc);

create index if not exists idx_review_queue_status_traffic
  on public.toolbar_review_queue(status, traffic);

create index if not exists idx_review_queue_created_by_status
  on public.toolbar_review_queue(created_by, status);

create index if not exists idx_sendtrack_domain_date
  on public.toolbar_sendtrack(domain, send_date desc);
