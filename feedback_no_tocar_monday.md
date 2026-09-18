---
name: feedback-no-tocar-monday-sin-permiso
description: "No modificar el board de Monday sin confirmación explícita, y el estado Agente/Manual va en Comentarios"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-25T18:22:47.592Z
---

**El board de Monday no se toca sin que el user lo confirme, aunque el cambio sea aditivo.**

El 2026-08-25 creé una columna de tipo Estado ("Origen", Agente/Manual) en el board
`1420268379` (Prospectos ADEQ, 10.400+ deals) porque un auditor lo había marcado como
pendiente. El user preguntó "¿para qué es la columna? ya había una para marcar manual o
agente" y decidió eliminarla. Se borró y se revirtió todo.

**Why:** un "corrige todo" sobre una lista de código no autoriza a modificar la estructura de
un CRM de producción que usan tres personas. La confirmación tenía que ser explícita para ESE
cambio, no heredada de la tarea anterior.

**How to apply:**
- **El estado Agente/Manual va en `texto` (Comentarios).** `CONFIG.MONDAY_COLUMNS.origen` y
  `toolbar_config.monday_col_origen` quedan VACÍOS a propósito. No proponer la columna otra vez.
- Antes de cualquier `create_column` / `delete_column` / cambio de labels en el board: preguntar,
  aunque sea aditivo y aunque parezca obvio.
- Escribir DATOS en items existentes (lo que hace la toolbar todos los días) sí está autorizado;
  cambiar la ESTRUCTURA del board no.

## Dato que quedó medido y sigue siendo cierto
`Comentarios` hace tres trabajos a la vez y hay **692 deals** donde el origen no figura porque
lo ocupa otra cosa: `Formulario`, notas de personas (`who is - Mica`,
`Guido Piccapietra | Jefe Comercial`) o el pitch entero volcado (`PITCH IA: ...`).
Es un costo conocido y aceptado por el user, NO un bug para arreglar.

Relacionado: [[project_pending]], [[reference_parte_diario]]
