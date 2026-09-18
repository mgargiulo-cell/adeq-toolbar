---
name: email-futuro-feature-arquitectura
description: Feature manual de reengagement implementado v5.0.2-v5.0.3. Tabla toolbar_reengagement_queue + pixel tracking + worker cron. Detalle de wiring por si hay que extender.
metadata: 
  node_type: memory
  type: project
  originSessionId: c82727d5-cdda-45a3-bdbf-1b1b3cc8246e
---

## Flujo end-to-end

1. **UI Analysis**: el grade A (Apollo + área core) queda en slot 1 (form-email). MB hace click "→ 2" en otro chip para asignar slot 2 (form-email-futuro). Slot 2 es opcional.
2. **Send Gmail handler** (`btn-send-gmail` en popup.js):
   - Antes de `sendEmail`: `createManualSendTracking` inserta row en `toolbar_agent_actions` con `action='sent'` → devuelve `id`.
   - Inyecta `<img src="${SUPABASE_URL}/functions/v1/track-open?aid=${id}" width="1" height="1" style="display:none" />` al final del body.
   - `sendEmail` manda via Gmail.
   - Si OK + slot 2 lleno: `queueReengagement` inserta en `toolbar_reengagement_queue` con `original_subject`, `original_body` (snapshot), `tracking_action_id=id`, `scheduled_for=now+11d`, `status='pending'`.
3. **Destinatario abre email** → Gmail carga pixel → Edge Function `track-open` → INSERT en `toolbar_email_opens(agent_action_id=id)`.
4. **Worker (Railway, auto-prospector/index.js)**:
   - Main loop cada 5 min, `iterCount % 60 === 0` (cada ~25 min) llama `processManualReengagementQueue(token)`.
   - SELECT pending vencidos (`scheduled_for <= now`).
   - Para cada row: SELECT toolbar_email_opens WHERE agent_action_id=tracking_action_id.
     - Tiene filas → `wasOpened=true` → mark `status='skipped_opened'`.
     - Sin filas → `sendGmailServer(token, mb_email, {to: future_email, subject: original_subject, body: original_body})` (DWD impersona al MB original).
   - On send OK: `updateMondayReengagementDispatch` con cols `email=future_email`, `fecha2_8=today+5`, `fecha_1=today+10`.
   - Mark `status='sent'`.

## Archivos clave
- `sql/2026-05-13_reengagement_queue.sql` — schema + RLS + flag activation.
- `modules/supabase.js` — `createManualSendTracking`, `queueReengagement`.
- `popup/popup.js` `btn-send-gmail` handler — pixel + queue.
- `popup/popup.js` `chipFor` / `renderEmailList` — botón "→ 2" + estado `.slot-future`.
- `popup/popup.css` — `.email-future-btn` (default gris, slot-future = círculo rojo).
- `auto-prospector/index.js` — `processManualReengagementQueue`, `updateMondayReengagementDispatch`, `MONDAY_COL_FU1/FU2`.

## Limitaciones conocidas
- **Open tracking depende del cliente del destinatario**: Gmail bloquea pixels por default a veces. Si nunca se carga → wasOpened=false → se envía future igual. No detecta "leído pero no respondió", solo "pixel cargado".
- **`agent_reengagement_enabled` es global**: si el flag se apaga, también pausa el processManualReengagementQueue (mismo gate). Para tenerlo independiente habría que separar flags.
- **No In-Reply-To header**: el future email llega como nuevo thread, no como reply. Para que aparezca en el mismo hilo de Gmail habría que guardar el `messageId` del send original (Gmail API devuelve `id` y `threadId` en la response del send).

## Para extender o debugear
- Ver queue vivo: `select * from toolbar_reengagement_queue order by created_at desc limit 20`.
- Filtrar pendientes vencidos: `... where status='pending' and scheduled_for <= now()`.
- Ver opens detectados: `select agent_action_id, count(*) from toolbar_email_opens group by agent_action_id`.
- Forzar ejecución inmediata en testing: cambiar `iterCount % 60` a `% 12` (cada hora) o `% 1` (cada iter, no recomendado en prod).
