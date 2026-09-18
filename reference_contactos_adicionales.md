---
name: reference-contactos-adicionales
description: "Los 3 Contactos Adicionales del MB: qué hace la toolbar, qué hace el worker, qué guarda el CRM en crm_board_contactos, y por qué salen de a uno por minuto"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-07T16:37:34.095Z
---

Los tres campos "📨 Contactos Adicionales" de Analysis (y de la card de Prospects). Rehecho el
2026-09-07 con el equipo del CRM (su commit `775c9dd`, el nuestro `c76ca59`, extensión v689).

## Por qué importan: son la MEJOR fuente que tenemos
Medido el 07/09 sobre 90 días: **499 envíos · 341 dominios · 6,6% de respuesta real**
(`toolbar_response_tracking.source='manual_extra'`). El scrape del sitio está en 3,1% y Apollo
en 1,8%. Cualquier cosa que los rompa cuesta caro.

## El flujo, punta a punta
1. El MB carga hasta 3 direcciones y aprieta **Send via Gmail**.
2. El principal sale **ya**, desde su casilla (`chrome.identity` + guarda `expectedFrom`).
3. Los adicionales **NO salen juntos**: se encolan en `toolbar_reengagement_queue` a **+1, +2 y
   +3 minutos** (`reason='adicional_manual'`, `tracking_action_id: null`).
   ⚠️ Antes salían los cuatro en el mismo minuto, mismo asunto, mismo cuerpo, mismo remitente:
   si los adicionales son del mismo dominio, es la firma de una difusión, y el rebote lo pagan
   los tres buzones (la reputación en Gmail es por DOMINIO). Regla del user (07/09).
   El asunto y el cuerpo **no se varían** — decisión suya: "no compliquemos".
4. El worker despacha desde la casilla del MB y escribe la fila `manual_extra` en
   `toolbar_response_tracking` (antes la escribía el popup: al mudar el envío había que mudar
   la medición o se cortaba la serie de 90 días).
5. El **push** al CRM manda los tres de una en `contactos` (ver contrato abajo).

⚠️ El despacho (`processManualReengagementQueue`) está **arriba del portón horario** y fuera del
throttle de 25 min: estaba detrás de ambos, así que un adicional programado a las 22:59 salía a
las 9 de la mañana siguiente. Son envíos que una persona ya disparó, no trabajo del agente.

## El contrato con el CRM (`sync-toolbar`, dentro de cada prospecto)
    "contactos": [{ "email": "...", "tipo": "adicional", "orden": 1,
                    "enviado_at": "ISO" }]
`email` obligatorio; el resto opcional. También acepta la forma corta
`"contactos_adicionales": ["a@x.com"]`. **Es idempotente** y un push sin `enviado_at` nunca
borra el guardado. El principal puede ir o no en la lista: lo reconoce por `email`.

**Se manda TODO en el push, con la hora PROGRAMADA** (regla del user: *"que al momento del push
se encolen todos, a pesar de que nuestro envío tarde 3 minutos, para evitar errores"*). Depender
de que el worker informe cada uno dejaría al CRM sin enterarse si el worker está caído. El
worker igual confirma la hora exacta al despachar. Si el mail todavía no salió, van sin hora.

## Qué hace el CRM (lo construyó su equipo)
- Tabla hija **`crm_board_contactos`** (no `email_secondary`: son hasta 4 y hace falta el
  cuándo). Sembraron los 8.946 principales que ya existían.
- **`scan-replies` matchea contra TODOS los contactos**, y el espejo que mueve la ficha también.
  Era el agujero grande: las respuestas de los adicionales quedaban huérfanas.
- Follow-ups: **sólo al principal** (no multiplicar por 4 contra el mismo medio).
- Si el principal rebota, **promueve un adicional** (prioridad: el que contestó › el que recibió
  y no rebotó › orden de carga) y reengancha la cadencia desde el `enviado_at` de ESA persona.
- Se ven en la ficha con su estado: contestó / rebotó / enviado / sin envío registrado.

## Los `avisos` del CRM ahora se VEN
`sync-toolbar` contesta `avisos` cuando algo entró a medias (idioma que no reconoce → la cadencia
no encuentra plantilla; GEO que no parece país; fecha ignorada; contacto que no pudo guardar).
Iban a `console.warn` mientras el cartel decía "✅ Cargado": fallo silencioso. Ahora se muestran
pegados al resultado, en amarillo. Ver [[feedback_alertas_automaticas]].

Relacionado: [[reference_crm_board_modelo]], [[reference_desde_que_buzon_sale_cada_mail]],
[[reference_entregabilidad]]
