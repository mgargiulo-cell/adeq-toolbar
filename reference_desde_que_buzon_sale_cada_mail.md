---
name: reference-desde-que-buzon-sale-cada-mail
description: "Auditoría del remitente: cómo se garantiza que cada mail salga del Gmail del MB que corresponde (manual y agente), y las tres pruebas que lo confirman"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-07T15:38:13.904Z
---

Auditado el 2026-09-07 a pedido del user (*"revisar que el botón de enviar a ADEQ esté enviando
los mails desde el Gmail de la cuenta que corresponda y no todos desde una cuenta"*).
**Veredicto: correcto en las dos rutas.** No se tocó código; esto es la evidencia.

## 1. Manual (botón "Send via Gmail" del popup)
`modules/gmail.js → sendEmail({to, subject, body, expectedFrom})`. Usa el token de
`chrome.identity` del MB y llama a `users/me/messages/send`, así que sale de esa casilla.
**Candado**: con `expectedFrom` (siempre `state.loginEmail`) consulta
`oauth2.googleapis.com/tokeninfo` y, si el Gmail de Chrome ≠ el login de la toolbar,
**tira y NO envía**. También renueva el token si viene sin email y distingue 401 de fallo de red
(nunca manda "por las dudas").

## 2. Agente (worker)
`getGmailAccessToken(impersonateUser)` firma un JWT por buzón (`sub: <mail del MB>`) contra la
cuenta de servicio con delegación, con `_accessTokenCache` **por usuario**. Si la delegación
falla, lanza — no hay fallback a otra cuenta. Las cinco rutas de envío pasan el buzón correcto:
- `index.js` ~16009 / ~22174 / ~22259 → `userEmail` del ciclo de usuarios habilitados
- ~16130 → `mb_email` de `toolbar_reengagement_queue` (el MB que contactó originalmente)
- ~17533 → `mbEmail` de `queueBounceRetry`, llamado con el `userEmail` cuyo envío rebotó
Los tres `sendGmailServer(token, dest, …)` restantes (parte diario, resumen de salud, alertas)
mandan **de mgargiulo a mgargiulo**: son informes internos, no prospección.

## 3. La prueba que no depende del código
Una respuesta sólo vuelve al buzón que mandó. En 90 días (`toolbar_response_tracking`):
sales 1.188 env / 93 resp · dhorovitz 765 / 54 · mgargiulo 700 / 51. **Los tres reciben** →
los tres mandaron con su propio `From:`.

## ⚠️ Dato para el user (no es un bug)
`agent_enabled_users` = `["sales@adeqmedia.com","dhorovitz@adeqmedia.com"]`: **mgargiulo NO está
en la rotación del agente**. Venía con ~50 envíos/semana hasta el 24/08 (120), 16 la semana del
31/08 y 0 automáticos desde entonces. Para volver a repartir entre tres, agregar el mail a esa
clave de `toolbar_config`. Se le informó el 07/09 y quedó como está.

Relacionado: [[reference_entregabilidad]], [[reference_parte_diario]], [[project_security]]
