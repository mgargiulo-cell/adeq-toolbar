---
name: project-crm-propio
description: "Reemplazar Monday por el CRM propio: la decisión de arquitectura, qué quedó preparado y apagado en la rama crm-propio, y los 3 pasos para ponerlo live"
metadata: 
  node_type: memory
  type: project
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-31T18:49:49.063Z
---

Proyecto arrancado el 2026-08-31. Objetivo: reemplazar Monday.com por un CRM propio en
`console.adeqmedia.com` / `crm.adeqmedia.com`.

## La decisión, ya tomada y verificada
**Va en `adeq-dashboard`** (Next 16 · React 19 · Tailwind 4 · Vercel), como una sección más.
NO en la toolbar (extensión MV3 sin build step: no puede servir un dominio ni tiene login
multiusuario) y NO como proyecto nuevo (habría que rehacer login, RLS, auditoría, alertas,
OAuth de Gmail e ingesta de hilos, que ya corren).

**El hallazgo que ordenó todo:** Monday no era el CRM, era el CABLE entre los dos sistemas.
`app/api/crm/sync-monday` chupaba del board 1420268379 hacia `crm_prospects`. El dashboard ya
sabía consumir un feed de prospectos; sólo había que cambiarle la fuente.

⚠️ **Son DOS Supabase distintas y siguen separadas a propósito:**
toolbar `ticjpwimhtfkbccchfyp` · dashboard `kacmcymcuvmkqvgctvkn`. Migrar la toolbar sería
tocar por dentro el agente que manda 40 mails/día para conseguir algo que el feed ya resuelve.

## Estado al 2026-08-31
El otro lado (adeq-dashboard) terminó y probó en producción: endpoint
`POST console.adeqmedia.com/api/crm/sync-toolbar`, header `X-Toolbar-Secret`, escribe en
**`crm_board_prospects`** (no en `crm_prospects`, para no tocar el Inbox actual), devuelve
`{ok, recibidos, errores[], avisos[]}`, idempotente y canonicalizando dominio. También hizo
`crm_boards` (multi-board), la acción `move_to_board`, y `/p/<domain>` ya redirige.

**Lado toolbar: TODO PREPARADO Y APAGADO en la rama `crm-propio`** (commits `8ab275d` y
`057ecaa`, pusheados a GitHub). `main` sigue en `089a8f9`. Tres cerrojos en serie:
1. Está en una rama — Railway despliega desde `main`.
2. `crm_propio_enabled` en `toolbar_config` = `"false"` (la fila ya existe, para que el
   interruptor esté a la vista). Prender/apagar NO necesita deploy.
3. `CRM_SYNC_SECRET` no está en Railway. Sin secreto no sale ni un request.

## Para ponerlo live — 3 pasos en este orden
1. Mergear `crm-propio` a `main` (llega a Railway, sigue apagado).
2. Cargar `CRM_SYNC_SECRET` en Railway.
3. `crm_propio_enabled` → `true`. Para apagar, sólo este paso al revés.

## Lo verificado contra el endpoint real (no asumido)
- El payload exacto del emisor pasa limpio: 1 ok, 0 errores, 0 avisos.
- Mandar el ISO `"en"` en vez de `"Ingles"` dispara aviso de idioma no soportado → **la
  traducción del emisor es necesaria**. El idioma del CRM es el del PITCH, no el del sitio:
  la toolbar detecta 20+ idiomas pero el pitch sale en 5 (`{en:0,es:1,it:2,pt:3,ar:6}` con
  `?? 0`), por eso un sitio húngaro o griego figura como "Ingles" y está bien.
- `"Árabe"` con tilde entra: el validador normaliza acentos.
- Idempotencia real: se reenvió el mismo dominio con `www.` y siguieron 2 filas, no 3.
- Quedaron 3 filas de prueba (`prueba-emisor-toolbar.test`, `prueba-idioma-toolbar.test`,
  `prueba-arabe-toolbar.test`) para borrar del lado del dashboard.

## Las tres trampas del backfill (encontradas probando, no teorizando)
1. **"Tiene `monday_item_id`" NO es "fue contactado".** De los 2.491 que lo tienen: 187
   RECHAZADOS, 255 congelados, 653 pendientes. El filtro correcto es `status=validated`
   → **1.396 filas**. Sin eso se cargan descartes al board como propuestas vigentes.
2. **Las fechas de follow-up vencidas van en NULL.** Un prospecto contactado hace tres semanas
   tiene FU1/FU2 en el pasado; cargarlas tal cual haría que la primera corrida de
   automatizaciones dispare ~1.400 mails a gente que ya recibió su secuencia entera.
   Irreversible y hacia afuera. `scripts/crm-backfill.mjs` ya lo protege — no sacar esa regla
   sin apagar antes las automatizaciones del CRM.
3. **El `deal_stage` real la toolbar no lo sabe.** Registra que empujó a Monday, no si después
   lo movieron a "Ciclo Finalizado". **Por eso el backfill del script es el PLAN B**: lo
   correcto es sembrar `crm_board_prospects` desde `crm_prospects` (1.994 filas con el stage
   real), que es una copia entre tablas de la misma base.

## Detector
`pushToCrmPropio` pinga `crm_sync` en el sistema de salud que ya existe (no se inventó otro):
prendido, `real` vs `esperado` por día; apagado, late con `cadenciaMin: 0` y el detalle dice
POR QUÉ está apagado. Más un renglón CRM PROPIO en el boletín diario que sólo aparece con el
flag encendido — es el que permite ver si las dos columnas cuadran ANTES de apagar Monday.

⚠️ El secreto del endpoint se pegó en el chat el 31/08. Rotarlo antes de ir a live.

Relacionado: [[project_pending]], [[reference_informe_salud]], [[feedback_alertas_automaticas]]
