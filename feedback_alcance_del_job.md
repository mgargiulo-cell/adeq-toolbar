---
name: feedback-alcance-del-job
description: "Un job de limpieza puede correr en verde e informar la verdad mientras es ciego al 99% del problema, porque su propio filtro excluye a la población que existe para arreglar"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-04T10:48:55.417Z
---

**2026-09-04.** El user venía diciendo hacía días *"sigo sin poder borrar definitivamente
ese mail que ya no está activo"*. La causa: `auditarEmailsDelPool` sacaba las direcciones
rebotadas leyendo **`status=eq.pending`**.

Medido contra producción: de los leads con un email principal ya rebotado, **172 estaban en
`validated` y 1 en `pending`**.

Lo peligroso no es que fallara. Es que **informaba la verdad**. Su último parte decía
`250 leads · 213 emails malos fuera (1 rebotados, ...)`: exactamente lo que da mirar sólo
`pending`, donde había uno. Verde, coherente, y ciego al 99% del problema.

**Why:** el filtro que define el ALCANCE de un job es invisible en su reporte. Un job que
mira el 3% de la población y limpia ese 3% entero se ve idéntico a uno que funciona. Es
primo del patrón *"no pude preguntar" leído como "la respuesta es no"* que ya lleva 8 casos
([[project_pending]]), pero peor: acá ni siquiera hay un error que loguear.

**How to apply:**
- Cuando un arreglo automático "ya existe pero el problema sigue", **la primera sospecha es
  el WHERE del job, no su lógica**. Leer el filtro antes que el algoritmo.
- El parte de un job de limpieza tiene que decir **sobre cuántos podría haber actuado**, no
  sólo cuántos tocó. `213 de 250 pending` y `213 de 5.781 del pool` son el mismo número
  contando cosas distintas.
- Y la población que se limpia tiene que ser **la misma que el usuario ve**. `validated` es
  el estado de lo que ya está en el CRM: si el job no lo mira, para el MB no existe.
- Verificar el alcance con SQL antes de tocar la lógica. Acá el `group by status` resolvió
  el caso en una consulta.

Relacionado: [[feedback_alertas_automaticas]], [[feedback_revision_03_09]],
[[feedback_lecciones_migracion]]
