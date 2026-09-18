---
name: feedback-borradores-del-user
description: Los borradores de email los escribe el user. No reescribirlos ni agregarles placeholders nuevos.
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-11T18:05:32.995Z
---

**Los borradores de mail los escribe Max, no yo.** Tienen que ser CORTOS y simples (los suyos son de 4 líneas). Si creo que el copy es el problema, lo digo y le paso el diagnóstico — pero no lo reemplazo.

**Why:** el 2026-08-11 reescribí los 15 borradores (3 × es/en/pt/it/ar) por un copy más largo y "mejor argumentado", sin que estuviera aprobado. Dos cosas salieron mal: no le gustó ("los mails tienen que ser simples") y, peor, **le llegó a un prospecto real (ole.com.ar) con los placeholders CRUDOS**: `{{saludo}}`, `{{sender_name}}` y `{{senal}}` sin reemplazar. Se eliminaron sin respaldo.

**How to apply:**
- ⚠️ **NUNCA agregar un placeholder nuevo a los templates sin tocar también la extensión.** Los mismos drafts de `toolbar_pitch_drafts` los usa el MB para los envíos MANUALES desde el popup, y esa ruta **solo sabe resolver `{{domain}}`**. Cualquier otro llega crudo al destinatario. El worker (`fillTemplate` en auto-prospector/templates.js) resuelve más, la extensión no — y comparten la tabla.
- Control para verificarlo después de tocar cualquier draft (tiene que dar 0 filas):
  `SELECT language, subject FROM toolbar_pitch_drafts WHERE body ~ '\{\{(?!domain\}\})' OR subject ~ '\{\{(?!domain\}\})';`
- Los drafts de la base **PISAN** a los baked de `auto-prospector/templates.js` (`pickAnyTemplate` usa los baked solo como fallback). Cambiar el código no cambia lo que se manda.
- El diagnóstico del copy sigue siendo válido y está en [[project_pending]] (los textos piden un favor, no ofrecen nada, y dos de tres piden el contacto de quien ya es el destinatario). Cuando Max quiera trabajarlos, ese análisis sirve — la maquinaria de `{{saludo}}`/`{{senal}}` quedó en `fillTemplate`, sin usarse.
- Se conecta con [[feedback_alertas_automaticas]]: este error fue silencioso hasta que un humano vio el mail.
