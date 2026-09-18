---
name: No reply/bounce tracking en la toolbar
description: El user ya tiene tracking de respuestas/bounces por OTRO sistema externo. No proponer ni implementar reply-detection, open-rate, bounce-detection ni features Gmail readonly en la toolbar.
type: feedback
originSessionId: b9fd9a24-e6b6-4c02-b4f9-21155b7bf17e
---
**Reply detection**: NO. El user ya tiene tracking externo de respuestas. No leer threads, no UI de reply.

**Bounce detection**: SÍ. Si un email bounceó, debe (1) bloquearse para futuros contactos en toda la toolbar, y (2) si el lead tenía otros emails alternativos en `review_queue.emails[]`, reintentar con el siguiente. Requiere scope `gmail.readonly` para escanear INBOX por mensajes de mailer-daemon. Tabla `toolbar_bounced_emails` (email PK + reason + bounced_at) consultada por rankEmail/scrapeEmailsForDomain como filtro adicional.

**Open-rate tracking**: SÍ pero solo como *input para scoring/análisis* del agente. NO como feature de UI ni reporting visible al MB. Implementar con tracking pixel propio (Edge Function Supabase) + tabla `toolbar_email_opens` + integrar al ranking de templates/subjects.

**Why:** El user tiene reply/bounce afuera (CRM o tool dedicada). Open-rate sí lo quiere ADENTRO porque le sirve a la lógica del agente para aprender qué subjects/templates funcionan mejor y elevar su prioridad.

**How to apply:** Cuando aparezcan ideas de tracking, evaluar: ¿es para el MB o para el algoritmo? Si es para mejorar el scoring → implementar. Si es para mostrar al MB en la UI → skip, ya está afuera.
