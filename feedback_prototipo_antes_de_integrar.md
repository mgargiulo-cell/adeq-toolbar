---
name: feedback-prototipo-antes-de-integrar
description: Una vía nueva se prueba con un prototipo contra SU propia vara medida antes de tocar el worker; el 04/09 eso descartó el vector de personas con 0 aciertos y evitó deployar código que habría inferido donald.trump@ como contacto
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-04T14:42:17.366Z
---

**2026-09-04.** El plan proponía un "vector de personas" para el crawler: cosechar nombres con
cargo del sitio, deducir el patrón del dominio, armar `nombre.apellido@`. Sonaba obvio y era lo
que el media buyer parecía hacer (416 de los 735 emails del hueco eran personas).

Se escribió como módulo puro con 7 tests, y **antes de engancharlo** se corrió en vivo contra
la vara que el propio plan había fijado: "de 7 a ≥20 recuperados sobre los 48 del hueco".

- Set de 48 (donde no teníamos nada): **1 acierto**.
- Set justo de 70 (donde el patrón sí era deducible): **0 aciertos exactos**.
- Y el prototipo infirió `donald.trump@dagsavisen.no` y `vladimir.putin@` como "presidente":
  **la portada de un medio está llena de nombres con cargo que son los de la noticia, no del
  staff.** Un detector de personas para un medio es un problema distinto que para una empresa.

El módulo no se commiteó. Lo que sí entró fue lo que el mismo set mostró que hace el humano:
adivinar el rol estándar del idioma (`redazione@`, `contacto@`, `info@`) — 32% de los 4.014
emails humanos del CRM, 28% de los dominios donde no teníamos nada.

**Why:** "no hay que romper nada" y "avanzá hasta el final" conviven sólo si cada vía nueva
demuestra su valor con el mismo rigor con que se midió el problema. Integrar primero y medir
después habría metido en producción una fuente de direcciones inventadas con nombre de
político, y el rebote lo habría pagado la casilla del media buyer.

**How to apply:**
- Toda vía nueva de descubrimiento: **prototipo en scratch → correr sobre el set que definió
  el problema → comparar contra la vara escrita en el plan → recién entonces integrar** (con
  fuente propia etiquetada para medirle rebote y respuesta aparte).
- Probar en el subconjunto hostil Y en el favorable. Si falla en los dos, es la idea; si sólo
  en el hostil, es el alcance.
- Guardar el resultado negativo con números en el plan y en memoria: un "no funciona, medido"
  ahorra volver a intentarlo dentro de tres meses.

Relacionado: [[project_plan_categoria]], [[feedback_alcance_del_job]], [[feedback_revision_03_09]]
