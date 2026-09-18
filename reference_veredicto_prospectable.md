---
name: reference-veredicto-prospectable
description: "Cuándo la toolbar deja prospectar una web: los 5 estados del CRM, qué dice el recuadro de Analysis, y las tres puertas donde se aplica (Analysis, Prospects, cascada)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-15T11:24:10.842Z
---

Regla del user (2026-09-07, textual) implementada en el commit `468ab69`, manifest 679.

## La regla
Decide **la columna `estado` de `/api/crm/ficha` y NADA MÁS** — regla del user, textual:
*"vos tenés que matchear con el CRM por la columna estado, no sacar conclusiones"*.

| `estado` | Veredicto | ¿Deja? |
|---|---|---|
| no está en el CRM | Web prospectable. **Nunca fue contactada.** | sí |
| `Ciclo Finalizado` | Web prospectable. **Ya tiene ciclo finalizado.** | sí |
| `Pausado` | Web prospectable. **Cliente Antiguo Pausado.** | sí |
| `Propuesta Vigente` · `En Negociacion` · `Personalizado` | No prospectable. **Propuesta en curso.** | **no** |
| `Live` | No prospectable. **Cliente activo.** | **no** |
| Ciclo Finalizado con `descansando` (lo calcula el CRM en la misma ficha) | No prospectable todavía (faltan N días) | **no** |
| estado desconocido o no se pudo preguntar | Revisalo a mano | avisa, no frena |

## ⚠️ Error MÍO del 07/09, corregido — no repetirlo
Le avisé al user que "los 62 `Pausado` facturan" porque están en `crm_board_clientes_activos`.
**Era una inferencia sobre cómo se calcula esa vista, no un dato.** Al medir el motivo real:
- `Live`: **45 de 45 facturan** (motivo `factura`).
- `Pausado`: **0 de 62 facturan** — 54 son `ex cliente: en la base y sin revenue` y 8
  `en la base, marcado activo` pero sin revenue en 90 días.

O sea: **el CRM ya es consistente** (si factura → `Live`) y el que sacaba conclusiones era yo.
La vista tiene cuatro motivos con prioridad (`factura` › `en la base, marcado activo` ›
`alias de un sitio de la base` › `ex cliente: en la base y sin revenue`): estar en ella **no**
significa facturar. Si hace falta saber si un dominio factura, mirar `c.motivo = 'factura'`.
Lección: [[feedback_alcance_del_job]] otra vez — afirmar sobre una vista sin leer su definición.

## ⚠️ Actualizado desde OTRA máquina (08–13/09, v696→v707; visto al hacer pull el 15/09)
Los números de abajo cambiaron: `_CRM_FICHA_TIMEOUT_MS = 4000` (el arranque en frío de Vercel
mide 3,15 s, con 2,5 s el primer pedido del día se abortaba siempre) y `_CRM_WATCHDOG_MS = 2 ×
timeout + 1000` (9 s). `buscarEnCrm` consulta la **blocklist ANTES que el CRM** (Diego, parado en
mail.google.com, mandó el pitch a cto@arise.tv y quedó registrado bajo mail.google.com) y matchea
por **dominio raíz** (`_dominioRaiz`: `m.elpais.com` → `elpais.com`; regla del user 08/09 "omití
www/http, que sea dominio real"). El bloque del CRM vivía adentro de `bindButtons()` (v701).
Tests: `crm-rapido.test.js` (ahora ~40 tests). Detalle en `git log 8881060..51dec04`.

## El recuadro tiene reloj (07/09, v690)
El endpoint NO es el lento: `/api/crm/ficha` es un `.eq('domain', …)` con índice sobre
`crm_board_prospects` + una lectura del nombre del board. **Medido: 0,5-0,75 s** — es el número
de referencia para comparar si algún día se arrastra. Es exactamente "filtrar la url y leer la
columna estado", que es lo que el user pide.
Lo que fallaba era el cliente: `buscarEnCrm` iba **sin timeout**, y un fetch colgado no rechaza
nunca → la promesa no se resolvía, nadie pintaba y el recuadro quedaba en el "Checking..." del
HTML para siempre. Ahora, en popup.js:
- `_CRM_FICHA_TIMEOUT_MS = 2500`, **dos intentos** → techo real 5 s. Devuelve `ms` y se muestra.
- La consulta va en un **`DOMContentLoaded` propio**, no en el grande: un throw en cualquier
  línea previa del handler grande se llevaba puesta la consulta. Dos listeners no se matan
  entre sí. `_crmConsultar` la comparte con la pipeline → sigue siendo UN pedido.
- `_armarWatchdogCrm` a los 6 s pinta "no pude consultar" (marcado `provisional`, se pisa si
  llega la respuesta real) + botón **🔄 Reintentar**.
- `resetAnalysisUI` limpia `state.crmVeredicto`, `_crmVuelo` y el bloqueo del botón.
Tests: `auto-prospector/tests/crm-rapido.test.js` — uno corre el `buscarEnCrm` REAL extraído del
fuente contra un fetch que nunca resuelve.

## El pitch, desde el 07/09 (v689)
Sin selectores: el **idioma** sale de `_resolvePitchLang()` (el mismo que elige la plantilla del
CRM) y la **categoría** del sitio. El **tono es fijo** (`ESTILO_PITCH`: informal · short ·
analysis · direct) — no se eligió midiendo: `toolbar_pitch_feedback` tiene 4 filas, la última del
27/05, y ni guarda el tono. La variante del CRM la elige el sistema (hash dominio+día, reparto
medido 33/34/33%); el click en ese botón **refresca** las plantillas salteando la caché de 6 h.
El botón sólo rota **los borradores propios** (3 por idioma en los 23, `toolbar_pitch_drafts`).
Traducción: panel a los **2 s con el cursor quieto**, Google gratis, nunca Claude.

⚠️ v690: `state.pitchTemplate` guarda **`body` (con el dominio ya sustituido) Y `bodyRaw`**. Sin
`bodyRaw`, comparar la plantilla guardada contra la que devuelve el CRM daba siempre distinto y
el botón anunciaba "✅ se actualizó" en cada click sin cambiar una letra. **Comparar crudo con
crudo.** El mensaje del botón dice las DOS salidas (redacción del user): *"La plantilla del CRM
no se cambia a mano: la variante la elige el sistema. ¿Querés mandar otra cosa? 🗑️ Limpiar y
escribís el mail, o elegís un país y usás tu borrador propio (1-3)."*

Al **cambiar de URL** (v690): `resetAnalysisUI` limpia `state.pitchTemplate` y llama a
`_desbloquearPitch()` —quedaba vacío Y trabado—, y `runAnalysisPipeline` llena el borrador
**antes** del análisis (el idioma sale del TLD y las 69 plantillas ya están en memoria) y lo
reajusta al final sólo si `_pitchIntacto()`. Ese `runAutoFill` al final **nunca había estado**
en la pipeline: navegar a otra web re-corría los seis chequeos y no llenaba nada.

## No prospectable = no se prepara NADA (07/09, v691)
Regla del user: *"Si es un Live o alguien que no es prospectable, el borrador ni se debe cargar,
esos campos deben estar bloqueados."* Apagar los dos botones no alcanzaba: quedaba el mail
redactado, el email elegido y el formulario lleno, listo para un cliente activo.
`_bloquearFormularioCrm()` deshabilita los 13 campos + los radios de `#email-result`, vacía
borrador y asunto, y `autofillDraftOnLoad`/`runAutoFill` cortan con `_crmBloquea()`. **Una duda
NO bloquea**, avisa: sólo bloquea un "no" explícito de la columna `estado`.

⚠️ El caso que lo destapó: `diariotextual.com` (estado `Live`, ejecutivo dhorovitz) mostraba
**Owner=Max / Estado=Propuesta Vigente**. No eran datos del CRM: eran los **defaults del
formulario**. Con el veredicto colgado, `fillMondayFormFromDuplicate` nunca corría y el
formulario vacío se leía como información real. Lección: un formulario con defaults que parecen
datos miente peor que uno vacío.

**El rotador de borradores sólo lo abre el país** (`_draftsState.paisElegido`): el mail del CRM
no se cambia con la bandera **ni después de Limpiar** — Limpiar es para ESCRIBIR. Elegir un país
habilita pasar entre los borradores PROPIOS (1/3, 2/3, 3/3), que no son las variantes del CRM.
Cambiar de web vuelve a `false`.

## Las columnas matchean (verificado 07/09)
Los **10 campos** que manda la toolbar (`domain, email, deal_stage, ejecutivo_name,
fecha_contacto, top_geo, pageviews, language, phone, source, mail_ya_enviado`) son exactamente
los que lee `sync-toolbar`. `crm_board_contactos` existe en vivo (8.969 filas / 8.946 fichas).
⚠️ **El checkout local de `adeq-dashboard` está VIEJO** — no tiene el commit `775c9dd`, así que
su `route.ts` no muestra `contactos` aunque el endpoint en vivo sí lo acepte. Verificar contra
la base o el endpoint, nunca contra ese archivo.

## Dónde vive (popup/popup.js)
`_veredictoCrm(dup)` → `{ok, duda, titulo, detalle, clase}` · `_pintarVeredictoCrm`
· `_aplicarBloqueoCrm` (apaga el botón y le pone "⛔ No prospectable") · `_motivoBloqueoCrm`.
Se aplica en: **Analysis** (`runDuplicateCheck`, guard #0 del push y del botón de Gmail),
**Prospects** (`validateProspect`, consulta `/ficha` antes de mandar) y **cascada**
(`isBlockedByExec`, usa la lista del CRM y es a propósito más conservadora).
`_aplicarBloqueoCrm` va **al final** de runDuplicateCheck: la rama de duplicado escribe
"🔄 Actualizar en ADEQ" y le pisaba el texto de bloqueo.

## Estados reales en `crm_board_prospects` (07/09)
Ciclo Finalizado 7.236 · Propuesta Vigente 2.599 · En Negociacion 86 · **Pausado 62** · Live 45.
⚠️ `Pausado` **no está** en `ESTADOS` de `app/lib/crm-board-schema.ts` (que lista cinco sin él) y
`sync-toolbar` lo mapea a "Ciclo Finalizado" al escribir — pero en la base existe y `/ficha` lo
devuelve. Por eso el veredicto matchea por regex y no por lista cerrada.

Relacionado: [[reference_crm_board_modelo]], [[reference_monday_apagado]], [[project_crm_propio]]
