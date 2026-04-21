# 🚀 Pasos de deploy — Sesión nocturna 2026-04-21

Todo esto se hace UNA SOLA VEZ. Después queda andando para siempre.

**Tiempo estimado total:** 15 minutos.

---

## Paso 1 — SQL en Supabase (1 min)

1. Abrí [supabase.com/dashboard/project/ticjpwimhtfkbccchfyp/sql](https://supabase.com/dashboard/project/ticjpwimhtfkbccchfyp/sql)
2. Pegá este SQL y dale **Run**:

```sql
create table if not exists public.toolbar_api_usage (
  user_email  text not null,
  day         date not null,
  total       int  not null default 0,
  by_provider jsonb not null default '{}'::jsonb,
  updated_at  timestamptz not null default now(),
  primary key (user_email, day)
);

create index if not exists toolbar_api_usage_day_idx on public.toolbar_api_usage(day);

alter table public.toolbar_api_usage enable row level security;

drop policy if exists "usage_select_own" on public.toolbar_api_usage;
create policy "usage_select_own" on public.toolbar_api_usage for select
  to authenticated using (user_email = auth.jwt() ->> 'email');

revoke insert, update, delete on public.toolbar_api_usage from anon, authenticated;
```

Tiene que decir **Success. No rows returned**.

---

## Paso 2 — Obtener las API keys actuales (1 min)

En el mismo SQL editor, corré:

```sql
select key, value from toolbar_config
where key in ('gemini_api_key','apollo_api_key','rapidapi_key');
```

Copiá los 3 valores a un lugar seguro (bloc de notas). Los vas a usar en el próximo paso.

---

## Paso 3 — Deploy del Edge Function (5 min)

Abrí Terminal.app (el CLI ya está instalado + logged in + linked).

**Setear secrets** (reemplazá con las keys del Paso 2):

```bash
cd /Users/maximilianogargiulo/Desktop/adeq-toolbar
supabase secrets set \
  GEMINI_API_KEY=PEGAR_GEMINI_KEY_ACA \
  APOLLO_API_KEY=PEGAR_APOLLO_KEY_ACA \
  RAPIDAPI_KEY=PEGAR_RAPIDAPI_KEY_ACA
```

Tiene que responder `Finished supabase secrets set.`

**Deploy del function:**

```bash
supabase functions deploy api-proxy --no-verify-jwt
```

Tiene que terminar con algo tipo `Deployed Functions on project ticjpwimhtfkbccchfyp: api-proxy`.

El `--no-verify-jwt` es obligatorio (la función valida el JWT manualmente).

---

## Paso 4 — Redeploy de Railway (2 min)

El backend `auto-prospector/` tiene cambios nuevos (heartbeat + timezone AR).

Si Railway está conectado a tu repo GitHub:

```bash
cd /Users/maximilianogargiulo/Desktop/adeq-toolbar
git push origin main
```

Railway detecta el push y redeploya solo.

Si NO está conectado: andá al dashboard de Railway → proyecto auto-prospector → click en **Redeploy** (botón en la esquina superior).

---

## Paso 5 — Recargar la extensión (30 seg)

1. Abrí `chrome://extensions` en Chrome
2. Buscá "ADEQ Toolbar"
3. Click en el ícono ↻ **Reload**

La extensión ahora usa el nuevo código.

---

## Paso 6 — Testear end-to-end (3 min)

Abrí una web cualquiera (ej: `ole.com.ar`) y click en el ícono de la toolbar.

**Verificá:**

- [ ] Tab Analysis: se muestra traffic (proxy RapidAPI OK)
- [ ] Botón 🤖 Apollo: encuentra emails (proxy Apollo OK)
- [ ] Botón ✨ Generate Pitch: genera pitch (proxy Gemini OK)
- [ ] Tab Prospects: el hero card muestra "🟢 Railway alive" (si apagaste Railway vas a ver 🔴 o ⚫)
- [ ] Footer inferior: muestra "API today: N (G:X/A:Y/R:Z)" (color gris si < 250 calls/día)

Si todo funciona → seguí al Paso 7.

Si algo falla → abrí DevTools (F12) → pestaña Console → mirás el error y me lo pasás.

---

## Paso 7 — Hardening final (opcional pero recomendado, 30 seg)

Una vez verificado que el proxy anda, **borrá las keys de la tabla** para que ya ni viajen al cliente:

```sql
delete from toolbar_config where key in ('gemini_api_key','apollo_api_key','rapidapi_key');
```

Desde este momento:
- Si alguien extrae el bundle de la extensión → las keys no están ahí
- Único path para llamar a Gemini/Apollo/RapidAPI: el proxy con JWT válido
- Quota server-side: máximo 500 calls/día/user

---

## 🆘 Si algo se rompe

**Error "api-proxy not found"** → Paso 3 no corrió bien, redeploy.

**Error "Missing GEMINI_API_KEY secret"** → Paso 3 `secrets set` no corrió, hacelo de nuevo.

**Error 429 "Daily quota exceeded"** → normal, alcanzaste el límite. Esperá al día siguiente o subí los caps en `supabase/functions/api-proxy/index.ts` líneas 22–28 y redeploya.

**Todo 401 "Invalid token"** → el user no está autenticado. Logout + login en la toolbar.

**Railway muestra "🔴 stale"** → chequeá que Railway esté corriendo (dashboard), que tenga el código actualizado (Paso 4).

**Las keys no las tengo guardadas** → si ya las borraste de `toolbar_config`, sacalas de donde las habías puesto originalmente (Supabase secrets console, email viejo, password manager, provider's dashboard — Gemini: AI Studio; Apollo: apollo.io settings; RapidAPI: rapidapi.com/hub).

---

## ✅ Cuando termines

El setup de seguridad queda profesional completo:
- Keys protegidas del lado servidor
- RLS scoped por user
- Quota per-user enforced
- No leaks de privacy
- UI en inglés consistente
- Heartbeat de Railway visible
- Auto-refresh de sesión

**No hay nada más que hacer** hasta que aparezca un bug nuevo o quieras una feature nueva.
