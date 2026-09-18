---
name: feedback-pedir-logs-railway
description: "Cuando algo se frena SIN dejar error en la base, pedir los logs de Railway antes de inferir"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-25T16:46:42.423Z
---

**Cuando un job se frena y la base no muestra ningún error, pedir los logs de Railway.**

El 2026-08-25 la cola de prospectos dejó de procesar: 200 pendientes, 0 en proceso, cero
errores en `toolbar_csv_queue`, el worker vivo y escribiendo otras tablas. Gasté cerca de
una hora descartando hipótesis desde la base (topes de RapidAPI, cupo por usuario, flags,
zona horaria, deploy fallido). Los logs lo resolvieron en una línea:

    🌱 sellers i-mobile.co.jp: 18275 pubs → 17993 frescos → 0 insertados
    ⏸️ auto_feeder_sellers SKIP inject: carril lleno (250/250)

**Why:** la base solo guarda RESULTADOS. Un job que decide no hacer nada —o que trabaja para
descartar todo— no deja fila. Ese vacío es indistinguible de "no le tocaba correr", y ahí es
donde se esconden los bugs caros de este proyecto.

**How to apply:**
- Síntoma "está frenado y no hay error" → pedirle al user el export de logs de Railway
  (los baja a `~/Downloads/logs.*.csv`, columnas `message,severity,timestamp`).
- **Descontar los deploys propios antes de culpar a la memoria.** Ese día vi SIGTERM cada
  2,5 min y casi concluyo que el worker moría por RAM; eran mis propios 17 pushes.
- Al arreglar, dejar el rastro que faltaba: cada salida temprana de un job importante tiene
  que escribir en `toolbar_health` con su motivo. Ya están `loop_reparto` (¿el bucle llega al
  reparto de trabajo?) y `csv_queue` (¿arrancó y contra qué topes?).
- Regla de diseño que salió de acá: **un guard de cupo va ANTES del trabajo, no después.**
  `_injectIntoCsvQueue` miraba el carril al final, así que el feeder gastaba minutos bajando
  archivos para descartarlos enteros.

Relacionado: [[feedback_alertas_automaticas]], [[project_pending]]
