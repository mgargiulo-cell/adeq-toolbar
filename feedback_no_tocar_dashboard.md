---
name: feedback-no-tocar-dashboard
description: "No tocar nada del proyecto adeq-dashboard: ni el repo ni su base. Lo que haga falta de ese lado va por prompt para su equipo"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-04T08:35:18.603Z
---

Instrucción del user, 2026-09-04: **"No toques mas nada del proyecto adeq-dashboard."**

Alcance: el repo `/Users/maximilianogargiulo/Desktop/adeq-dashboard` **y su base de Supabase**
(`kacmcymcuvmkqvgctvkn`, tablas `crm_*`, `system_config`, `casilla_envios`). Sin escrituras de
ningún tipo: ni flags, ni backfills, ni filas de prueba.

**Why:** ese lado tiene su propio equipo y su propio criterio. Yo venía escribiendo en su base
—prendí `crm_board_autosend_enabled`, marqué 146 fichas con rebote, limpié 77 `comentarios`—
y aunque cada cosa estaba justificada, son cambios en un sistema que no es mío y sobre los que
su equipo no tuvo voz. El reparto ya está escrito: ellos son dueños del registro.

**How to apply:**
- Lo que haga falta de ese lado va **por prompt**, como los nueve pedidos: ver
  [[feedback_formato_prompt]] y `REGLA-FINAL-CRM.md`.
- Leer para diagnosticar sólo si el user lo pide explícitamente. Ante la duda, preguntar.
- Si un arreglo mío necesita algo de ellos, se implementa mi mitad, se deja lista, y se
  entrega la spec exacta del contrato. Así se hizo con `casilla-envios` y el registro del
  `inicial`, y funcionó.

Relacionado: [[reference_crm_board_modelo]], [[feedback_lecciones_migracion]]
