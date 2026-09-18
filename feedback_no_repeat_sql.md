---
name: No repetir SQLs ya corridos
description: Una vez que el user confirma haber corrido un SQL (o cuando lo doy como instrucción única), no volver a mencionarlo en respuestas siguientes
type: feedback
originSessionId: b9fd9a24-e6b6-4c02-b4f9-21155b7bf17e
---
No repetir bloques SQL en respuestas posteriores una vez que ya se entregaron o el user confirmó haberlos corrido. Específicamente: SQLs de migración (ALTER TABLE, CREATE INDEX, NOTIFY pgrst) van UNA sola vez en el mensaje de la feature, nunca en resúmenes ni mensajes de seguimiento.

**Why:** El user lo marcó como ruido — "aparece siempre no tiene ningún sentido". Ver SQL repetido cuando ya está aplicado le hace ruido y le hace pensar que tiene algo pendiente.

**How to apply:** Cuando estoy resumiendo features o respondiendo follow-ups sobre una feature que requirió SQL, NO volver a citar el SQL. Asumir que ya está aplicado. Si genuinamente sospecho que el SQL no se corrió (ej. error PGRST204 column not found), señalar el síntoma y pedir confirmar, no repegar el bloque entero.
