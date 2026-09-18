---
name: reference-gasto-anthropic
description: "Dónde consume la key de Anthropic, por qué saltó el gasto el 26/08, la puerta única con techo, y cómo ver en qué se gasta (por motivo y en tokens)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-03T00:28:25.200Z
---

## El salto del 26/08 NO fue una regresión
`toolbar_api_usage` (contador del proxy, por `user_email` y día) lo muestra limpio:

    hasta el 25/08   worker@backend =    0/día     humanos 70-250/día
    26/08 en adelante              =  250→1.777    humanos IGUAL

`worker@backend` **aparece por primera vez el 26/08**. Todos los llamadores de Claude existen
desde junio/julio — no nació código nuevo. Lo que cambió es que **el worker se destrabó**: los
commits del 26-27/08 son todos de pool/feeder/autopilot. Antes no gastaba porque no trabajaba.
⚠️ Conclusión incómoda pero correcta: **el gasto es trabajo real**. Lo que faltaba no era
frenarlo, era ponerle techo y poder medirlo.

Patrón semanal: **Lun/Mié/Vie ≈ 1.300-1.800, Mar/Jue ≈ 460.** El delta es
`runSuspectRejectAnalysis`, que corre esos tres días.

## Todo lo del worker es Haiku — el caro lo gastan los humanos
`agent_claude_percent = "0"` → **0 pitches por Sonnet en el agente** (verificado: 0 de 2.000).
El agente manda **plantillas**; los pitches salen sólo al tocar "generar" (regla del user).
⚠️ Ojo con `cfg.agent_claude_percent || "20"`: `"0"` es string truthy y aguanta, pero un 0
numérico o un string vacío **caería al default de 20** y prendería Sonnet sin que nadie lo pida.

**Los ~150-250/día de `sales@`/`dhorovitz@`/`mgargiulo@` son la extensión**, y van del popup al
proxy **sin pasar por Railway**: el techo del worker no los toca, que es lo correcto — un tope
de infraestructura no puede frenarle un click a un media buyer.

## La puerta única (02/09)
Había **nueve `fetch` al proxy escritos a mano**: ni techo, ni contador, ni idea de quién
gastaba. Ahora todo pasa por `llamarClaude(token, motivo, cuerpo, {timeout})` en `index.js`,
con `claude_daily_cap` (700) y el gasto persistido (sobrevive a un redeploy).

El techo **no corta lo que va en el camino de un envío** —`CLAUDE_MOTIVOS_ESENCIALES` =
`pitch`, `email_pick`, `idioma_envio`— porque cortarlos cuesta un lead; el enriquecimiento
espera a mañana. Al cruzarlo **avisa una vez** (log + `saludPing` warn).

⚠️ `lib/idioma.js` no puede importar la puerta sin ciclo. Se resolvió con el patrón de
`_bouncedCache`: idioma.js exporta `puertaClaude = { llamar: null }` e index.js le inyecta.

## 👁️ Cómo ver en qué se gasta
**Tabla `toolbar_claude_gasto`** (dia, fuente, motivo, modelo, usuario, llamadas, tokens_in,
tokens_out) + **vista `toolbar_gasto_claude_vista`**, que agrega y estima el USD.

    select * from toolbar_gasto_claude_vista where dia = current_date - 1 order by usd_aprox desc;

Y sin consultar nada: **el boletín de salud trae una sección de gasto** del día anterior,
encabezada por el motivo **MÁS CARO y no el más frecuente** — para optimizar no es lo mismo.

⚠️ **Contar llamadas no alcanza: lo que se factura son tokens.** Medido: 8 llamadas de Sonnet
salen **8,5 veces más** que 12 de Haiku. Un conteo de llamadas puede bajar mientras la factura
sube. Los precios viven en la **vista**, no en el código: un precio hardcodeado mal es un
informe que miente con total confianza.

Motivos: worker → `clasificar_sitio` · `clasificar_sitio_lote` · `sitio_bloqueado` ·
`idioma_envio` · `idioma_deteccion` · `email_pick` · `pitch` · `reglas_pitch` ·
`reglas_descarte` · `keywords`. Extensión → `pitch_a_pedido` · `analisis_revenue` ·
`tipo_de_web` (los dos primeros son **Sonnet**, o sea lo caro).

## Las dos fugas que se taparon
**1. Preguntar de a uno.** El prompt que explica qué es un publisher son ~700 tokens y la
respuesta 3: **el 97% de lo que se paga es la misma instrucción repetida**. Ahora se pregunta
de a 20 (`_haikuPublisherClassLote`). Probado en vivo: **6/6 correctos, 268 tokens de entrada
donde de a uno habrían sido ~4.200**.
⚠️ **Prompt caching de Anthropic NO sirve acá**: el mínimo cacheable son 2048 tokens y estos
prompts no llegan (~700 y ~150). Agrupar es mejor igual — elimina la repetición en vez de
abaratarla. El criterio y la lista de tipos quedaron en UN lugar (`_SYS_CLASIFICA_SITIO`,
`_TIPOS_SITIO`): dos listas paralelas divergen y ahí las dos versiones clasificarían distinto.

**2. Re-pagar un dictamen que ya se tenía.** `runSuspectRejectAnalysis` pedía los 200 `pending`
más nuevos y **sólo marcaba a los que rechazaba**. El que salía limpio volvía a caer en los
mismos 200 la corrida siguiente, para siempre. Medido: **200 de 200 limpios**, y los 2.256 de
atrás sin mirarse nunca. Se agregó `toolbar_review_queue.suspect_checked_at`.
**Un dictamen negativo es un resultado igual que el positivo.** Guardar sólo los hallazgos
convierte cualquier job de revisión en un bucle que re-paga lo que ya sabe.

## Lo que se midió y NO era fuga (para no volver a mirarlo)
- **Recalificar dominios entre días**: 1.188 descartes sobre **1.167 dominios distintos**, sólo
  15 repetidos. La caché en RAM se pierde en cada deploy y **no importa**: el trabajo es casi
  todo sobre dominios nuevos. No hace falta persistirla.
- **Re-atribución**: los humanos no bajaron cuando apareció el worker — es gasto nuevo, no
  gasto que cambió de casilla.

Relacionado: [[feedback_cost_awareness]], [[feedback_alertas_automaticas]],
[[reference_arquitectura]], [[reference_billing_cycles]], [[reference_informe_salud]]
