---
name: reference-crm-board-modelo
description: "Cómo está modelado el CRM propio: una URL una fila, Prospectos ADEQ como madre, los tableros por media buyer, el vocabulario de 6 estados y el descanso de 40 días"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-02T18:51:56.540Z
---

Reemplazó a Monday el 2026-09-02. Vive en `console.adeqmedia.com` (proyecto `adeq-dashboard`,
Supabase `kacmcymcuvmkqvgctvkn`). La toolbar y el worker lo consumen por endpoints con
`x-toolbar-secret`. Ver [[reference_monday_apagado]].

## El modelo (regla del user, textual)
*"dejamos urls únicas en todo el crm, y directamente de prospect adeq van a negociaciones de
cada media buyer, y de ahí al darlas por finalizada vuelven al board principal"*.

**Un dominio = UNA fila. El tablero dice DÓNDE ESTÁ, no es una copia.** `crm_board_prospects`
tiene un índice único global sobre `lower(domain)`, y con este modelo eso deja de ser un
obstáculo y pasa a ser la regla — **no hubo que tocar el esquema**.

⚠️ Sin esa definición la migración era imposible: de los 931 dominios de los boards de
negociaciones, **865 ya existían** en Prospectos ADEQ. Insertarlos reventaba por clave
duplicada; "arreglarlo" con un upsert habría PISADO las filas de la madre.

## Los 5 estados (el 02/09 pasaron de 15 a 6 y después a 5)
`Propuesta Vigente` · `En Negociacion` · `Ciclo Finalizado` · `Live` · `Personalizado`.
**Ya no existen** los `Masivo - *`, `Mail No Enviado`, `Perdido`, `Descartado` ni
**`Propuesta Vigente (T)`** — que era el valor por DEFECTO del formulario de la toolbar.
Fuente de verdad: `app/lib/crm-board-schema.ts` → `ESTADOS`.

⚠️ **El vocabulario cambió TRES veces en un día** y las tres el `<select>` de la toolbar quedó
desincronizado. Chequearlo contra `ESTADOS` cada vez que se toque el board: `Live` va así, no
`LIVE`.

- **`Live`** = el sitio está corriendo con nosotros. Se sincroniza solo con la facturación:
  `marcarLive` promueve Y degrada, así que el tablero y el revenue no pueden discrepar.
- **`Personalizado`** es un **DISPARADOR**: ponerlo manda el mail personalizado del MB.
- ⚠️ Un estado que el CRM no conoce **no da error**: `sync-toolbar` lo guarda como "Propuesta
  Vigente" con un aviso que nadie ve. Por eso el `<select>` de la toolbar ofrece exactamente
  los que el CRM acepta, y `_estadoLabel` lee la etiqueta **del propio select**.

## El descanso de 40 días (regla final del user, 02/09)
*"solo los que se negocia y se descartan que vayan a finalizados por 40 días. Eso no me frena
nada con los otros miles de ciclo finalizado que ya se les puede escribir"*.

El trigger `crm_board_sellar_descarte` sella `descartado_at` **sólo en la transición
`En Negociacion → Ciclo Finalizado`**. Probado contra producción:

    En Negociacion    → Ciclo Finalizado  → sella, descansando=True, 40 días
    Propuesta Vigente → Ciclo Finalizado  → NO sella, prospectable al instante

⚠️ **Sellar en cualquier entrada a un cierre está MAL** (fue mi primera versión): habría
frenado el pool entero. Y borrar la fecha "al salir de un estado de cierre" tampoco sirve,
porque Ciclo Finalizado TAMBIÉN es cierre — esa regla borró la fecha de 188 filas.
El borde es **estricto**: al día 40 ya está libre, no al 41.

## Quién bloquea el re-contacto
`/api/crm/dominios-activos` es la autoridad. Bloquea por **prefijo de estado**
(`propuesta vigente`, `en negociacion`, `personalizado`), por **estar en un tablero de
negociación** (`is_default=false`, no por lista de slugs), por **cliente activo**, por
**bloqueado a mano** (`crm_blocked_domains`) y por **descanso de 40 días**.
`/api/crm/reciclables` es su complemento exacto — se verifica que la intersección sea 0.

⚠️ `crm_board_clientes_activos` **es una VISTA calculada** (facturación 90 días +
`sitios_config` activos), no una tabla: no se puede insertar y no se debe.
⚠️ `replied_at` está en **NULL en las 10.000 filas** (sólo lo sellaba el espejo de
scan-replies, que casi no corrió). Importa porque `date_arrives` filtra por él: hoy no
descarta a nadie y la cadencia se apoya entera en el log `crm_board_automation_runs`.

## El mail inicial
La regla es `status_is` sobre Propuesta Vigente (antes era `status_changes_to`, que **sólo
corría cuando una persona tocaba la celda**: nada que entrara por API lo disparaba).
En Analysis la toolbar **exige mandar el mail antes de cargar**, así que por ese camino ya
salió; los que entran sin mail son los validados desde Prospects y los de la cola.
Se sembraron 2.682 como "ya enviado" para no re-contactar a quien ya recibió.

Relacionado: [[reference_monday_apagado]], [[feedback_lecciones_migracion]],
[[reference_cola_por_enviar]], [[project_crm_propio]]

## ⏰ Cuándo salen los mails (regla del user, 02/09)
**Ni sábado ni domingo**, y sólo en el solapamiento de las dos regiones: `AGENT_SLOTS` pasó de
`[9,12,15,18,20,21]` a **`[13,14,15,16,17]`** hora de Madrid, con la ventana activa en 13-19.
13 h Madrid = 8 de la mañana en Argentina, 7 en Colombia/Perú, 6 en México. Los turnos viejos
tenían tres mal: las 9 de Madrid son las 4 AM en Buenos Aires y a las 20-21 no trabaja nadie.
⚠️ **Son CINCO turnos y no uno solo a propósito**: el cupo es 20 por casilla y mandarlos juntos
desde la misma dirección es la huella que Gmail lee como spam.

## 📞 Teléfonos: por qué había 59 sobre 10.006
El extractor viejo capturaba **cualquier** número con separadores. De 2.497 guardados: 482
decimales, 260 tipo IP/versión, 75 empezados con un año — **1 de cada 3 no era un teléfono** —
y sólo 198 traían código de país, sin el cual el CRM los rechaza y el board no muestra bandera.
Además el email se busca en 65 rutas y el teléfono sólo en la home.
**No se rescataron por formato**: crucé 68 contra los sitios reales y **18 estaban corrompidos**
(varesenews.it tenía +39714163519 cuando el real es +390332873094). Se re-rasparon los 2.497 con
`lib/telefono.js` → **727 verificados, 406 cargados**, todos en E.164.
⚠️ El board guarda los países **en español** ("México", "España") y SimilarWeb en inglés: sin los
alias, 22 teléfonos válidos quedaban sin prefijo.

## 📧 Una celda, un email
`email` NO admite dos direcciones separadas por `;` o `,` — existe `email_secondary`. Rompe el
envío, la verificación y el registro de rebotes (no se sabría cuál rebotó). `sync-toolbar` ahora
las parte solo. La toolbar nunca genera el caso: manda una sola.

## 🔀 Quién escribe qué (cerrado el 2026-09-03, con el dashboard)
La línea: **antes del primer mail = toolbar + worker. Después = CRM.** Y el reparto se define
por **qué puede ver cada pieza que las otras no**, no por quién lo hace hoy.

    worker/extensión →  domain · email · teléfono · geo · pageviews · idioma ·
                        fecha_contacto · source · comments · ejecutivo_name ·
                        bounced_email/bounce_reason
    extensión además →  deal_stage, PERO sólo cuando una PERSONA lo eligió en el formulario
    CRM             →  estado · replied_at · TODAS las fechas de cadencia · movida de tablero

⚠️ Tres cosas que el worker le estaba pisando al CRM, todas del mismo tipo — **un default
disfrazado de decisión**:
- **`deal_stage` en cada push del agente.** `sync-toolbar` lo escribe sin mirar si la fila ya
  existe, así que alguien que contestaba a las 10:00 (el CRM lo pasa a En Negociacion en 3 min)
  volvía a Propuesta Vigente a la tarde, porque la lista de bloqueados del agente tiene hasta
  24 h. La ficha nueva no queda huérfana: `estado` tiene default `'Propuesta Vigente'` NOT NULL.
- **`fecha_fu1`/`fecha_fu2` clavadas en +5/+10** (copiadas de Monday). El CRM tiene **DIEZ**
  pasos en `crm_board_cadence_steps` y `sync-toolbar` dice textual *"las que manda la toolbar
  mandan"*: cambiar la cadencia no habría afectado a los ~60 diarios del agente.
- **El reintento tras rebote era INVISIBLE.** El CRM vacía `email` al recibir el rebote; el
  worker mandaba a otra dirección y no avisaba → ficha sin email, sin follow-ups, y una
  respuesta que la detección (que matchea por dirección) no habría reconocido.

## Detección de respuestas: la hace el CRM
`cron/respuestas-rapidas` cada 3 min. Se decidió así **contra** mi recomendación inicial: su
filtro de auto-respuestas (cabeceras RFC + asuntos en 11 idiomas) no existe del lado del worker,
y moverlo significaba reescribirlo — con 27 falsos positivos en un día como evidencia del costo.
**El worker nunca escribió `replied_at` ni movió fichas de tablero** (verificado).

## Rebotes: los lee el worker, el CRM aplica el efecto
El worker usa service account con domain-wide delegation (ve TODAS las casillas sin que nadie
autorice); el CRM depende de `crm_gmail_tokens`, que cada MB conecta a mano y se vence.
`crm_board_bounces_enabled=false`. Reparto por **verbo**: buscar la dirección = worker (es el
único con descubrimiento), decidir si se le escribe = CRM (`/dominios-activos`), mandar = worker
(un mail a una dirección nueva es un PRIMER contacto, y el inicial nunca sale del CRM).

## 📭 Fichas sin contacto: la etiqueta, no un estado (decidido 03/09)
Cuando una dirección rebota, el worker busca otra. **Tasa real: 324 encontradas / 272
agotadas (54%)**, así que la mitad se resuelve sola y nunca llega al MB.

Cuando se agota, la ficha va **a la vista del MB** (decisión del user) con `email` VACÍO —
meter texto tipo "rebotado" ahí corrompe el campo por el que se manda y encima pierde cuál
murió, que ya lo guarda `email_rebotado`.

**No hace falta ni estado ni columna nueva**, las dos piezas ya existen y juntas lo dicen:

    contacto_formulario = true   → "no hay mail, entrarle por el formulario web" (666 fichas
                                    ya viven ahí, con filtro y celda propia)
    email_rebotado != null       → "y la que teníamos MURIÓ" → de acá sale la etiqueta

⚠️ El worker **no puede avisarlo hoy**: `contacto_formulario` lo deriva el CRM de "quedó sin
email", con guard `&& !esRebote`. Hace falta que acepte `contacto_agotado:true`. "Todavía no
tengo mail" y "busqué y no hay" son cosas distintas: la primera no necesita a nadie.

## ⚠️ Encontrar otra dirección NO es "En Negociacion"
El user lo propuso y se le explicó por qué no: ese estado significa que alguien **contestó**.
Con una dirección nueva nadie contestó — mudaría la ficha al tablero de negociaciones (el MB
abre y encuentra gente con la que nunca habló) e **inflaría el número con el que se mide el
negocio**. Va a **Propuesta Vigente**, que es literalmente "le mandamos y esperamos".
Aceptado por el user el 03/09. Dato que lo respalda: de las 130 en limbo, **60 ya estaban en
Propuesta Vigente** — sólo las 70 en Ciclo Finalizado tienen que volver.

## 🔴 Revivir una ficha no alcanza sin arreglar el dedupe
La cadencia deduplica por `(automation_id, prospect_id, status='sent')` — **por prospecto, no
por ciclo de contacto**. Una ficha que revive con dirección nueva recibe el primer mail y
**ningún follow-up**, porque los pasos figuran como enviados a la dirección muerta.
**545 prospectos expuestos, 8 ya rebotados.** Sin eso, arreglar las 130 sirve a medias.
