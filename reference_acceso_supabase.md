---
name: reference-acceso-supabase
description: SÍ hay acceso directo a la base y a los deploys con el CLI de Supabase — no pedirle al user que copie y pegue SQL
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-24T18:13:13.148Z
---

**El CLI de Supabase está instalado y linkeado al proyecto `Toolbar-Adeq` (`ticjpwimhtfkbccchfyp`).**
O sea que se puede consultar la base y deployar sin pasar por el user.

```bash
# Consultar / modificar la base (--linked apunta al proyecto remoto, NO al local)
supabase db query "SELECT ..." --linked

# Deployar la Edge Function del proxy (NO sale con el push a GitHub)
supabase functions deploy api-proxy --project-ref ticjpwimhtfkbccchfyp

# Ver / cargar secrets del proxy (los valores no se leen, solo el digest)
supabase secrets list --project-ref ticjpwimhtfkbccchfyp
supabase secrets set NOMBRE='valor' --project-ref ticjpwimhtfkbccchfyp
```

**Why:** el 2026-08-24 asumí que no tenía acceso —encontré solo la clave anónima en el repo, que no puede leer nada por RLS— y no revisé esa conclusión en toda la sesión. Resultado: le hice al user copiar y pegar decenas de consultas y devolver las tablas a mano durante horas, y encima chocó tres veces con errores de columna inexistente porque yo escribía SQL de memoria en vez de mirar el esquema.

**How to apply:**
- Cuando el user pida "¿cómo venimos?" ([[reference_4_metricas]]), correr las consultas y traer el ANÁLISIS, no el SQL para copiar.
- Antes de afirmar algo sobre los datos, verificarlo. Y antes de escribir SQL, mirar el esquema real: `SELECT column_name FROM information_schema.columns WHERE table_name='x'`.
- **Los cambios de datos se siguen proponiendo antes de aplicarlos.** Poder correr un UPDATE no autoriza a hacerlo solo; el user decide. Ver [[feedback_no_pedir_permiso]] — esa regla es para cambios de CÓDIGO, no para tocar datos de producción sin avisar.

## ⚠️ Tres configuraciones que se separan y nadie cruza
El proxy (Edge Function) y el worker (Railway) se configuran POR SEPARADO. Ya se desincronizaron tres veces:
1. El proxy apuntaba a `similarweb-insights` mientras el worker usaba `website-insights`.
2. El proxy validaba los paths con un contrato que la extensión nunca respetó (la query va pegada al path) → **bloqueó todas las llamadas de los MB a SimilarWeb durante 3 semanas**, desde que agregué la lista blanca el 04/08.
3. La clave de Apollo vivía en dos lugares con valores distintos: `toolbar_config.apollo_api_key` (worker) y el secret `APOLLO_API_KEY` (proxy).

**Al tocar cualquiera de los dos, verificar el otro.** Y vale la pena que el chequeo de salud compare las dos configuraciones, como ya compara el DNS.
