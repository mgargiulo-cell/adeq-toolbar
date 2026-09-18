---
name: reference-dos-macs
description: "El user trabaja desde DOS Macs: qué acceso tiene que tener cada una, cómo se comparte esta memoria (rama `memoria` + enlace) y el ritual de empezar/cerrar sesión para no pisarse"
metadata:
  type: reference
---

Pedido del user (2026-09-18): *"a veces uso esta Mac y a veces otra, necesito poder acceder al proyecto
desde ambas… quiero que las dos tengan full acceso, para insertar SQL, para ajustar lo que sea necesario
de todo el proyecto."*

## Qué es "full acceso" (lo que tiene que tener CADA Mac)
| Para qué | Qué es | Cómo se consigue |
|---|---|---|
| Código y deploy | `git push` a `github.com/mgargiulo-cell/adeq-toolbar` (HTTPS + llavero de macOS). Railway despliega solo cada push a `main`: no hace falta su CLI | ya lo tienen las dos |
| Base de la toolbar (leer y **aplicar SQL**) | Supabase CLI logueado + `supabase link --project-ref ticjpwimhtfkbccchfyp` en la raíz del repo. Se usa `supabase db query "…" --linked` | `supabase login` lo corre EL USER (abre el navegador); el link después |
| Base del CRM, **sólo lectura** | repo `adeq-dashboard` clonado y linkeado a `kacmcymcuvmkqvgctvkn` | opcional. Regla: no se toca, ni el repo ni la base |
| Publicar la extensión | `~/.adeq-cws.json` (OAuth del Chrome Web Store) + `scripts/empaquetar.sh` + `scripts/cws-publish.py <zip> --publicar` | copiar el archivo (AirDrop), chmod 600 |
| La foto de salud / la rutina | `~/.adeq-salud-snapshot.key` | idem |
| La rutina de martes y viernes | va con la CUENTA de Claude, no con la Mac: `RemoteTrigger` la ve desde las dos | nada |
| Esta memoria | rama `memoria` (abajo) | worktree + enlace |
Vercel es del CRM, no de la toolbar: no hace falta. El CLI de Railway tampoco.

## Cómo se comparte la memoria
Rama **huérfana `memoria`** del mismo repo (no es código, no se mergea, Railway no la despliega).
En cada Mac: `git worktree add ../adeq-toolbar-memoria memoria` desde el repo, y la carpeta
`~/.claude/projects/<slug-del-proyecto>/memory` es un **enlace simbólico** a ese worktree (el slug
depende de la ruta del repo en ESA Mac). En la Mac original quedó un respaldo:
`memory.respaldo-2026-09-18`. Se eligió git y no iCloud porque iCloud puede desalojar archivos y
la memoria se lee al arrancar cada sesión; y no una carpeta dentro de `main` porque cada push a
`main` reinicia el worker.

## El ritual (si se saltea, una Mac pisa a la otra)
- **Al empezar:** `git pull --ff-only` en el repo **y** en `../adeq-toolbar-memoria`. Si el repo está
  *detrás*, traer antes de tocar nada (el 15/09 esta Mac estaba 79 commits atrás). Si *divergió*,
  frenar y avisar al user: nunca `--force`.
- **Al cerrar** ("DEPLOY+GITHUB+MEMORIA"): push de `main` y, en la carpeta de memoria,
  `git add -A && git commit -m "…" && git push`.
- Nunca las dos Macs trabajando a la vez sobre lo mismo.
- `npm install` con un npm viejo le saca los campos `libc` a `auto-prospector/package-lock.json`:
  no commitear eso (`git checkout -- auto-prospector/package-lock.json`).

## Las credenciales no viajan por chat
Se pasan por AirDrop (`~/Downloads/adeq-credenciales.zip`, armado el 18/09 con los dos archivos) y
se borra el zip en las dos Macs después. Nunca en un prompt, un commit ni esta memoria.

Relacionado: [[reference_acceso_supabase]], [[reference_chrome_web_store]], [[reference_revision_automatica]]
