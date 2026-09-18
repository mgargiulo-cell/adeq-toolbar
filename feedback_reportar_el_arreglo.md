---
name: feedback-reportar-el-arreglo
description: "Al cerrar un bug, contar QUÉ quedó arreglado y qué cambia para el MB — no la autopsia de por qué se rompió"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-07T18:04:31.686Z
---

Textual del user (2026-09-07, después de que le contestara con `git log`, commits y fechas de
cuándo nació cada bug): **"Listo, no me interesa saber por qué se ocasionó el error."**

**Why:** el reporte útil para él es el que puede accionar: qué anda ahora, qué tiene que probar,
qué falta. La arqueología —qué commit lo introdujo, qué patrón se repite, cuántos días estuvo
roto— es trabajo mío para no repetirlo, y va a la memoria, no a la pantalla. Cuando la pongo en
la respuesta le hago leer un post-mortem para enterarse de una línea. Ojo: esa vez él **había
preguntado** ("qué era lo que ocurría y cuándo se rompieron"); igual le sobró. O sea que ni
siquiera preguntar habilita el largo: la pregunta pedía una frase, no una investigación.

**How to apply:**
- Cerrar con: **qué quedó arreglado · qué cambia para el media buyer · qué falta o hay que probar.**
- La causa, en **una frase como mucho**, y sólo si cambia lo que él tiene que hacer (ej.: "no toques
  X hasta la próxima zip"). Nada de commits, fechas de nacimiento del bug ni "el patrón es el mismo".
- Si insiste o pregunta de nuevo con detalle, ahí sí se abre. No antes.
- La lección técnica **sí** se escribe — en la memoria ([[feedback_alertas_automaticas]],
  [[feedback_alcance_del_job]]), que es donde sirve.
- No aplica a los avisos que él necesita para decidir: si algo quedó a medias, si hay que esperar
  una revisión de Google, si un dato no se pudo verificar. Eso se dice siempre.

Relacionado: [[feedback_formato_prompt]], [[feedback_no_pedir_permiso]]
