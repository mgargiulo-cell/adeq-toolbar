---
name: reference-monday-apagado
description: "Monday quedó apagado del todo el 2026-09-02: qué endpoint reemplaza a cada cosa que hacía, cómo se cortó y qué queda inalcanzable"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-02T18:52:28.211Z
---

Corte completo el 2026-09-02 por pedido del user (*"ninguna lectura ya debe ir a monday, todo
se debe migrar al crm board"*). **Monday queda intacto como respaldo de sólo lectura.**
El modelo del destino está en [[reference_crm_board_modelo]].

## Los interruptores
`monday_enabled=false` · `monday_rescue_enabled=false` ·
`agent_reconcile_monday_bounces=false` · `crm_propio_enabled=true` (en `toolbar_config`).
**Las credenciales están ARCHIVADAS**: `monday_api_key*` → `ARCHIVADA_monday_api_key*`.
Sin credencial no hay conexión posible ni por un flag mal puesto. Volver = renombrar.

## Tip a tip — qué reemplaza a qué (todo probado en vivo)
    botón enviar / ficha de Prospects / cola  → POST /api/crm/sync-toolbar (idempotente)
    duplicado (era checkDuplicate)            → /api/crm/ficha?domain=
    índice del board (cascada de similares)   → /api/crm/dominios-activos
    Import y Refresh                          → /api/crm/reciclables?full=1&geo=&idioma=&minTraffic=
    envíos manuales (panel y parte diario)    → /api/crm/manuales (?dia= o ?desde=&hasta=)
    feeder de ciclos cerrados                 → /api/crm/reciclables
    lista de no-recontactar                   → /api/crm/dominios-activos
    push del agente                           → pushToCrmPropio

## Dónde se cortó
**La extensión tenía UNA sola puerta**, `mondayRequest` en `modules/monday.js`: ahí se corta
con `MONDAY_APAGADO = true` y **tira error con mensaje**, no devuelve vacío. Una pantalla sin
migrar se ve rota, que es lo que se quiere — vacío en silencio diría "sin resultados" y nadie
sabría que no se consultó nada. El módulo conserva el nombre `monday.js` y las funciones sus
nombres: cambió a dónde preguntan, no cómo se llaman (así no se tocan ~30 llamadores).

En el worker el código quedó **inalcanzable pero presente** (un `return` antes), para que
volver sea cambiar una fila de config y no revertir un deploy.

## Cómo se comporta cada cosa cuando el CRM NO responde
Es lo que más importa y lo que se diseñó con cuidado:
- **feeder**: no recicla. Una lista incompleta = escribirle a alguien en negociación.
- **`fetchDominiosBloqueados`**: devuelve **null, no lista vacía**, y el autopilot **no
  arranca**. Vacío se lee como "no hay nadie bloqueado" y manda a prospectar todo el pipeline.
- **lista de no-recontactar**: conserva la anterior y **pinguea FALLA**. Si se congela callada,
  nadie se entera hasta ver un pitch en frío a un cliente.
- **manuales**: `ok=false` y el parte dice "NO SE PUDO TRAER" — también en la versión HTML,
  donde el dato desaparecía sin decir nada.
- **`_fichaDelCrm`**: loguea, cuenta y al terminar la corrida pinguea warn + alerta.

## Lo que hubo que soltar ANTES de archivar las credenciales
- **El agente frenaba el ENVÍO por falta de API key de Monday** (`continue` sin mirar nada).
  Habría dejado de mandar mails por una credencial que ya no usa.
- **`processManualReengagementQueue`** cortaba antes de mandar. Ese proceso MANDA MAILS.
- `updateMondayReengagementDispatch` y hermanas devuelven **true** sin key, no false: hay un
  llamador que corta el envío con false, y el mail sí salió.

## Nada se perdió
Los **114 ítems de Monday del 1-2/09 están los 114 en el CRM**. Y los 38 dominios que estaban
con Mica (su tablero se borró, ya no trabaja) volvieron enteros a Prospectos ADEQ.

Relacionado: [[reference_crm_board_modelo]], [[feedback_lecciones_migracion]]
