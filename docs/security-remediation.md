# Remediación de seguridad — ADEQ Toolbar (auditoría 2026-07-15)

⚠️ Estos NO se auto-aplicaron: son cambios de Supabase (RLS / Auth / rotación de keys) que pueden romper
producción si se hacen mal (el worker + la extensión leen keys de `toolbar_config`). Aplicar con cuidado,
en orden, verificando que el worker (Railway) y la toolbar sigan andando después de cada paso.

## 🔴 CRÍTICO 1 — API keys en `toolbar_config` legibles por cualquier usuario autenticado + shippeadas al cliente
La RLS de `toolbar_config` es `for select to authenticated using (true)` → cualquiera que se autentique
puede leer con UN request las keys de Monday/RapidAPI/Apollo/Gemini en texto plano. Además `fetchApiKeys()`
las baja a cada instancia de la extensión (quien descomprima el .zip y loguee las obtiene).

**Amplificador (esto lo vuelve internet-facing):** el anon key es público (config.js, docs/reset-password.html,
dentro del .zip). Si los **signups de Supabase Auth están habilitados** (default), CUALQUIERA en internet
puede `POST /auth/v1/signup` con el anon key, volverse `authenticated`, y leer las keys.

**Pasos (en orden):**
1. **YA MISMO**: en el dashboard de Supabase → Authentication → Providers/Settings → **DESHABILITAR signups**
   (o restringir a dominio @adeqmedia.com). Convierte el riesgo internet-facing en solo-insider.
2. Mover TODAS las provider keys fuera de `toolbar_config` → servirlas solo por el Edge Function `api-proxy`
   (ya lo hace para Gemini/Apollo/RapidAPI/Anthropic/Voyage). Rutear **Monday** también por el proxy (hoy
   `modules/monday.js` usa el token directo client-side) y borrar `fetchApiKeys()` + los slots `CONFIG.*_KEY`.
3. Mientras tanto: restringir el SELECT de `toolbar_config` a keys NO secretas (mover los secretos a otra
   tabla sin policy de cliente, o una vista/RPC con filtro de columnas). Nunca `select=*`/`select=key,value`
   de toda la tabla desde el cliente.
   ⚠️ El worker usa `BACKEND_BEARER` (service_role → bypasea RLS) así que tightening la RLS NO lo rompe;
   PERO la extensión lee Monday con anon+JWT (authenticated) → hay que rutearla por el proxy ANTES (paso 2).
4. **Rotar** las keys de Monday/RapidAPI/Apollo/Gemini expuestas, después de la remediación.

## 🔴 ALTO 2 — Acciones de admin gateadas SOLO en el cliente
`ADMIN_EMAILS` (roles.js) + triple-click son cosméticos. Los writes privilegiados (caps globales, límites
por usuario, flags de agente/autopilot, flags de purga) van a Supabase con el JWT del usuario. Si
`toolbar_config`/`toolbar_user_limits` aceptan writes de `authenticated` (el cliente lo requiere), cualquier
MB logueado puede subir su propio cap, cambiar caps globales, borrar límites de otros, o setear flags de purga
destructivos.

**Fix:** mover las mutaciones privilegiadas detrás de RPCs `SECURITY DEFINER` / Edge Functions que chequeen
el email del caller contra una lista de admin server-side, y RLS que impida a `authenticated` INSERT/UPDATE/
DELETE arbitrario en `toolbar_config`/`toolbar_user_limits`.

## 🟠 MEDIO 3 — RLS incompleta / no versionada
`toolbar_user_limits` no tiene policy NI CREATE TABLE en el repo; `fetchAllUserLimits` lee todos los MBs.
`toolbar_sendtrack` UPDATE es `using(true)` (cualquiera actualiza cualquier row). Los caches de tráfico y
`toolbar_keywords` son `with check(true)`. → commitear RLS explícita least-privilege para CADA tabla
`toolbar_*` y reconciliar `sql/rls_hardening.sql` con la DB viva (dump de `pg_policies`).

## 🟡 BAJO 4 — Defense-in-depth (manifest)
- `<all_urls>` host permission + scripting: máximo blast radius (justificable para una tool de análisis, pero
  scopear a los sitios scrapeados si se puede).
- CSP `connect-src 'self' https:` → apretar a los hosts de API específicos (*.supabase.co, api.monday.com,
  googleapis.com).
- Scope Gmail `gmail.settings.basic` es más amplio de lo necesario (solo se lee la firma) — da write a
  filtros/forwarding. Bajar a un scope de lectura si alcanza.

## ✅ Verificado OK (no requiere acción)
Inyección PostgREST (todo encodeURIComponent), XSS (esc() + CSP script-src 'self'), secretos del worker
(env-only), sin logging de secretos, tokens en chrome.storage aislado, el Edge Function api-proxy es el
modelo correcto (valida JWT + allowlist de paths + quotas + keys como secrets de Deno).
