-- ════════════════════════════════════════════════════════════════════════
-- HARDENING RLS (2026-07-15) — cierra escrituras/lecturas de INSIDERS a keys/caps/límites.
-- SEGURO: usa policies RESTRICTIVE (se SUMAN a las existentes y las restringen, no las borran).
-- El worker usa service_role (BACKEND_BEARER) → bypasea RLS, no se afecta.
-- NO toca las lecturas de config que la extensión necesita (Monday) ni las escrituras runtime.
-- Admin = mgargiulo@adeqmedia.com (igual que las policies que ya existen).
-- ════════════════════════════════════════════════════════════════════════

-- 1) toolbar_config: SOLO el admin puede ESCRIBIR las keys del worker (apollo/rapidapi/gemini) y los
--    caps globales. Antes cualquier MB autenticado podía sobrescribirlas (redirigir gasto / romper worker
--    / subir caps). Las keys de Monday NO se protegen acá por si cada MB setea la suya desde la UI.
DROP POLICY IF EXISTS cfg_protect_ins ON toolbar_config;
CREATE POLICY cfg_protect_ins ON toolbar_config AS RESTRICTIVE FOR INSERT TO authenticated
  WITH CHECK (
    key NOT IN ('apollo_api_key','rapidapi_key','gemini_api_key','csv_queue_daily_cap','autopilot_daily_cap_global')
    OR lower(auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com'
  );
DROP POLICY IF EXISTS cfg_protect_upd ON toolbar_config;
CREATE POLICY cfg_protect_upd ON toolbar_config AS RESTRICTIVE FOR UPDATE TO authenticated
  USING (
    key NOT IN ('apollo_api_key','rapidapi_key','gemini_api_key','csv_queue_daily_cap','autopilot_daily_cap_global')
    OR lower(auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com'
  )
  WITH CHECK (
    key NOT IN ('apollo_api_key','rapidapi_key','gemini_api_key','csv_queue_daily_cap','autopilot_daily_cap_global')
    OR lower(auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com'
  );

-- 2) toolbar_user_limits: cada MB lee SOLO lo suyo (o el admin todo); ESCRIBIR solo el admin.
--    Antes la policy `auth write`/`auth read` (USING true) dejaba a cualquier MB leer/escribir los
--    límites de TODOS (ej. subirse su propio cap). El worker (service_role) sigue leyendo/escribiendo.
DROP POLICY IF EXISTS ul_read_self_or_admin ON toolbar_user_limits;
CREATE POLICY ul_read_self_or_admin ON toolbar_user_limits AS RESTRICTIVE FOR SELECT TO authenticated
  USING (lower(user_email) = lower(auth.jwt() ->> 'email') OR lower(auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com');
DROP POLICY IF EXISTS ul_write_admin_upd ON toolbar_user_limits;
CREATE POLICY ul_write_admin_upd ON toolbar_user_limits AS RESTRICTIVE FOR UPDATE TO authenticated
  USING (lower(auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com');
DROP POLICY IF EXISTS ul_write_admin_ins ON toolbar_user_limits;
CREATE POLICY ul_write_admin_ins ON toolbar_user_limits AS RESTRICTIVE FOR INSERT TO authenticated
  WITH CHECK (lower(auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com');
DROP POLICY IF EXISTS ul_write_admin_del ON toolbar_user_limits;
CREATE POLICY ul_write_admin_del ON toolbar_user_limits AS RESTRICTIVE FOR DELETE TO authenticated
  USING (lower(auth.jwt() ->> 'email') = 'mgargiulo@adeqmedia.com');

-- ── QUÉ NO se hace acá (a propósito) ──
-- • Las LECTURAS de config siguen abiertas a los 3 MBs (la extensión lee la key de Monday). Cerrar eso
--   requiere el cambio de código (rutear Monday por el Edge Function + sacar fetchApiKeys) → lo hago yo
--   en el repo. Con signups deshabilitados esto ya es solo-insider (bajo riesgo).
-- • toolbar_keywords / toolbar_sendtrack quedan como están (bajo riesgo; el worker escribe con service_role).
