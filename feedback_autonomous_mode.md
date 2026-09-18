---
name: Autorización total — modo autónomo
description: When user says "Autorización total", work autonomously without asking confirmation — make decisions, edit freely, commit, deploy local changes.
type: feedback
originSessionId: c09e269f-e226-42ef-8293-d9448f31fa9c
---
Cuando el user escribe **"Autorización total"** (o "autorización total", cualquier variante):

- **No preguntar** si seguir, si autoriza, si avanza, etc.
- **Tomar decisiones** de arquitectura/UX/priorización sin consultar.
- **Editar/commitear** libremente en el repo.
- **Saltar** pasos de confirmación para acciones reversibles (edits, commits, builds locales).
- **NO** ejecutar acciones irreversibles externas que requieran credenciales del user (deploys a infra compartida que necesitan su password, push force a main, borrado de data en prod).
- Al finalizar la sesión autónoma, dejar un **resumen consolidado** con: qué se hizo, qué queda pendiente, y qué requiere acción manual del user (credenciales, deploys, etc).

**Why:** User no va a estar presente por varias horas (ej. 12hs overnight) y quiere que avance el trabajo sin bloqueos por prompts de confirmación. Ya dio consentimiento amplio una vez; no hace falta volver a pedirlo durante esa ventana.

**How to apply:** Desde el mensaje que dice "Autorización total" hasta que el user vuelve a interactuar, todo lo razonable va directo a acción. Si algo es realmente destructivo o requiere un secret que solo tiene él, dejarlo documentado en el resumen final como "pendiente manual".
