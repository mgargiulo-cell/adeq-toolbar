---
name: reference-alertas-falsos-positivos
description: "Los falsos positivos del Vigilante ya vistos, su causa real, y la regla para no volver a alarmar por el sistema funcionando bien"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-26T12:44:15.606Z
---

El 2026-08-26 el Vigilante mandó **tres alertas críticas en una mañana y activó el kill
switch solo**. Las tres eran falsas. El agente estuvo frenado 3 horas por nada.

## Falso positivo 1 — "33 intentos de acceso NO AUTORIZADO"
Eran **nuestro propio worker**. Supabase migró el proyecto al formato nuevo de claves
(`sb_secret_…`) y lo que la Edge Function recibe en `SUPABASE_SERVICE_ROLE_KEY` dejó de ser
el JWT legacy que el worker manda. La igualdad `jwt === serviceKey` falló y cada llamada del
worker se registró como intento de intrusión.

**No era solo ruido:** durante ~8 días TODO lo que el worker pedía por el proxy —Haiku,
Apollo, embeddings— venía fallando en silencio.

Arreglo: `WORKER_KEY`, un secreto nuestro que **nadie más rota**, aceptado además de
`SUPABASE_SERVICE_ROLE_KEY`. Si Supabase vuelve a cambiar el formato, esto sigue andando.
⚠️ Comparar contra UNA sola cadena que rota un proveedor es frágil por diseño.

## Falso positivo 2 — el contador no miraba lo mismo que la lista
`noAutorizados` era un COUNT crudo sobre la tabla; la lista de "Quién" leía 10 filas y recién
ahí descartaba nuestras IPs. El mail decía **"33 intentos" y abajo no mostraba a nadie**.
Ahora se lee y se filtra UNA vez: si no hay a quién señalar, no hay alerta.

## Falso positivo 3 — el techo de envíos quedó viejo el día que arreglamos algo
`envios_dia_max: 150` contaba TODO junto. El 25/08 revivimos los cuatro caminos de re-trabajo
que llevaban meses muertos → 62 primeros contactos + 87 de re-trabajo = 149. **El arreglo de
ayer disparó la alarma de hoy.**

Ahora son dos umbrales, porque son dos reglas de negocio distintas:
- **primer contacto**: techo DERIVADO del cupo configurado (`agent_focus_config.daily_override`
  → `agent_max_per_day` → 20), ×3 buzones, +50%. Si el user cambia el cupo, el umbral se mueve solo.
- **re-trabajo**: `retrabajo_dia_max: 400`, alto a propósito — no tiene cupo por regla del user
  ("re-trabajo aparte, sin límite"), solo detecta loops.

## Falso positivo 4 — 24h corridas contra un cupo POR DÍA (2026-08-27)
Mi propio arreglo del 26/08 derivó bien el techo y dejó mal la ventana. El 26 el kill switch le
comió la mañana al agente, mandó sus 60 juntos a la tarde, y sumado al 27 la ventana móvil vio
**102 sobre un techo de 90 → kill switch solo otra vez**. El cap nunca se rompió: 20/20/20 los
dos días, exacto.
Ahora cuenta **desde la misma medianoche de Madrid que usa el cap, y POR BUZÓN** — un total
agregado esconde justamente el caso que la alarma existe para detectar.

## Falso positivo 5 — vigilar una fuente retirada
`autopilot` dejó de ser una fuente el 24/08, cuando cada motor pasó a escribir su nombre propio.
El monitor la seguía vigilando y avisaba "29 días sin producir" todos los días. Una fuente que
no existe no puede producir.
**REGLA: si se renombra o retira una fuente/job, sacarla del monitor EN EL MISMO COMMIT.**

## Un mail de alerta por día, de verdad (2026-08-27)
El freno era de 60 min POR TIPO, y el camino "crítico" ni entraba al presupuesto diario que ya
existía. Llegaron dos mails de la misma alarma con 73 min de diferencia.
Ahora: presupuesto **global y por día calendario**, con **dedupe por contenido** (lo que se
repite treinta veces no es una alerta, es un estado → va al resumen). Solo rompe el presupuesto
lo que DE VERDAD frenó el sistema (`forzar: freno`). El resumen diario no gasta ese presupuesto.

## Falso positivo 6 — "no lo anoté" leído como "no pasó" (2026-08-27)
81 de 238 envíos figuraban sin `monday_ok` y arranqué una auditoría creyendo que no habían
llegado al CRM. **Fui a verificar contra el board y los 12 que revisé estaban TODOS.** El log
estaba gateado en `if (mondayItemId)`: cuando el item ya existía, el push lo actualiza sin
devolver id nuevo y no se registraba nada. No faltaba el lead, faltaba el renglón.
**Antes de auditar un agujero, verificar contra la fuente externa.** Y ojo con mi propio
método: la primera consulta a Monday que usé (`items_page_by_column_values` con
`column_id:"name"`) devolvía "no está" para TODOS, incluidos los que sí estaban. Lo correcto
es `items_page` con `query_params.rules` y `operator: contains_text`.

## Falso positivo 7 — DORMIDO leído como MUERTO (2026-08-28)
Fuera del horario de España (9-23) y los fines de semana el worker hace `continue` antes de
escribir nada: `auto_heartbeat_at` y `worker_commit` quedan CONGELADOS en el último momento
activo. **Me confundió a mí**: vi el latido de las 21:01 a las 00:25 y di el worker por
muerto — estaba perfecto, dormido por horario (lo confirmaron los logs de Railway).
Arreglado: `_latirDormido()` late igual mientras duerme y actualiza la versión.
⚠️ **Antes de declarar muerto al worker, mirar la hora de Madrid.** Y los logs de Railway
sí se pueden leer: `curl` a la GraphQL con `RAILWAY_TOKEN` funciona (urllib de Python NO —
falla por SSL/403; usar curl).

## Falso positivo 8 — un trabajo A PEDIDO vigilado con horario (2026-08-31)
`autopilot_similares` llevaba **10 días en rojo con su última corrida exitosa** (847 similares
desde 62 semillas). Lo dispara un MB prendiendo el toggle y se apaga solo al terminar: no tiene
horario que incumplir, pero el monitor le exigía correr cada 120 min.
Arreglado: `saludPing` acepta **`cadenciaMin: 0` = trabajo a pedido** → guarda NULL, y la vista
solo marca ATRASADO cuando hay cadencia esperada. Sigue en el panel, juzgado por su último
resultado. **Un job que nadie pidió no está atrasado, está esperando.**

## Falso positivo 9 — horas de RELOJ contra un sistema que DUERME (2026-08-31)
El más caro de todos, porque era estructural. El worker trabaja **L-V de 9 a 23 en Madrid**: 70
horas activas por semana de 168 de reloj. La vista comparaba `now() - last_ok_at` contra la
cadencia esperada, y el fin de semana solo agrega horas muertas al numerador. Ese lunes estaban
en rojo o amarillo `purga_cola`, `metricas_diarias`, `monday_llegada`, `monday_sync` y
`aprovechamiento_apollo`: **ninguno había fallado**, el worker había dormido.

Y era una **bomba con fecha puesta**: el cambio de ese mismo día (agente solo de lunes a
viernes) garantizaba un mail de alarma **cada lunes, para siempre**. Un monitor que grita todos
los lunes deja de leerse, y ese día deja de servir para el rojo de verdad.

Arreglado con `toolbar_minutos_activos(desde, hasta)` — cuenta solo L-V 9-23 Madrid, **la misma
ventana que decide en index.js** (`_isWeekendSpain` / `_isOutsideActiveHours`); si se cambia el
horario del worker hay que cambiar las dos. `monday_llegada` pasó de 4.092 minutos de reloj a
612 activos y quedó en verde. La vista muestra `hace_min` y `hace_min_activos` juntos, para que
no haya discusión de "dice 68 horas pero figura en verde".
SQL en `sql/2026-08-31_salud_en_minutos_activos.sql`.

## Lo que el rojo del finde estaba TAPANDO (2026-08-31)
Al quedar todo en verde saltaron tres nombres de job corrompidos:
`"pool_listo, cadenciaMin: 120s"`, `"url_pur, cadenciaMin: 4320ge"`,
`"purge_block, cadenciaMin: 4320ed"`. Un reemplazo mal anclado había metido el texto de las
opciones DENTRO del literal.
**No era cosmético: el ping de ÉXITO se escribía bajo el nombre roto**, así que la fila real de
`pool_listos`, `url_purge` y `purge_blocked` no recibía nunca un OK y solo podía juntar fallos.
Un job que anda parecía uno que nunca anduvo. Llevaba así desde el 28/08.
`saludPing` ahora rechaza y logea cualquier nombre que no sea `^[a-z0-9_]{2,60}$`.
⚠️ **Un panel lleno de rojos falsos no es solo ruido: es camuflaje.**

## LA REGLA
**Un umbral fijo es una bomba de tiempo.** Cada vez que se arregla algo que estaba muerto, el
volumen legítimo sube y los techos viejos se convierten en falsos positivos. Antes de dejar un
número clavado, preguntarse de qué config se puede derivar.

**Y: el número que alarma tiene que ser EXACTAMENTE el mismo conjunto que se muestra.** Si el
mail no puede señalar a nadie concreto, no hay alerta que mandar.

El user lo dijo así: *"En vez de alertar intentá solucionar vos directamente estas cuestiones."*
Ver [[feedback_alertas_automaticas]] y [[feedback_no_pedir_permiso]].

## IPs propias conocidas
`152.55.` y `162.220.` son Railway (verificado por whois). Se configuran en
`toolbar_config.ips_propias` sin tocar código. Las de AWS (`13.52.`, `54.215.`, `204.236.`)
son de Supabase.

Relacionado: [[project_security]], [[project_pending]], [[reference_informe_salud]], [[reference_lectura_rebotes]]


## 📊 El worker NO se queda sin memoria y NO se reinicia cada 7 min (medido 2026-09-01)
Leído de los logs de Railway (130 mediciones): **mínimo 55 MB · promedio 311 MB · pico 594 MB**,
contra un umbral de alerta de 700 y limpieza de cachés a los 450. **Cero OOM reales.** Se
estabiliza en ~280 MB durante horas: la limpieza automática funciona.
→ **Pagar un plan más grande de Railway no cambiaría nada.** Si vuelve a surgir la pregunta,
esta es la respuesta, sin necesidad de volver a mirar.

⚠️ **Y el contador de iteraciones va de #30 a #360 sin resetearse (~12 h seguidas): el worker
corre CONTINUO, no se reinicia cada ~7 min.** Los comentarios del código todavía dicen lo
contrario y varias decisiones de reparto de tiempo se justificaron con ese supuesto viejo.
Cada vuelta del loop toma ~2,2 min. Los SIGTERM de los logs son reinicios por DEPLOY.

🔎 Al filtrar logs por "OOM" salen 399 falsos positivos: matchea **vidOOMy.com** (una red
publicitaria) y **newsrOOM**. Filtrar por `MEMORIA ALTA` o `rss=`, que son las que el worker
escribe de verdad.

Acceso: `curl` a `https://backboard.railway.app/graphql/v2` con
`Authorization: Bearer <token de CUENTA>`. El de proyecto no alcanza para listar nada.
`environmentId` del entorno production: `3df30fb3-ac4c-47c4-bbb2-ba682ca382ac`
(proyecto `vigilant-essence`, servicio `adeq-toolbar`).


## 🚨 EL INFORME TE MANDA AL LUGAR EQUIVOCADO — ANDÁ A LOS LOGS (2026-09-02)
El agente estuvo **DOS DÍAS EN CERO** y el informe decía *"ENVÍO 0 de 40 — mirá los skips en
ERRORES CONCRETOS"*. No había skips: **no había NADA**, el ciclo no corría. La causa estaba en
los logs de Railway desde el primer minuto:

    ❌ Error: domain.includes is not a function            x7   ← el que lo frenó
    ❌ Error: Cannot access 'cfg' before initialization     x19  ← mío, al arreglarlo
    ❌ Error: Cannot access '_hayTiempo' before init        x22  ← mío otra vez

**REGLA: si un job figura ATRASADO pero su última corrida fue EXITOSA (`real >= esperado`), no
falló — DEJÓ DE SER LLAMADO.** Eso es una excepción aguas arriba. El primer lugar es filtrar
`❌ Error` en los logs de Railway, NO el informe.
Y ojo con `unhandledRejection`: no lo agarra ningún try/catch de los que rodean la llamada.
`isDomainAllowed` recibía objetos `{title, domain}` del cache de similares (117 filas mal
guardadas) y volteaba el ciclo entero. **Una función de filtro nunca puede tirar el proceso.**

## Falso positivo 10 — "NADA QUE HACER" leído como "NO CORRÍ" (2026-09-02)
`purga_cola` figuraba con última corrida el 27/08 y `reabrir_rebotados` el 31/08. Los dos
habían corrido **ESE MISMO DÍA** (lo confirma `cadencia_*` en config): corren, no encuentran
trabajo, y se van sin pingear. Tres días de rojo por dos jobs sanos — y encima el "no trabajo"
era buena noticia (0 filas para purgar, 6 leads rebotados esperando).
Tercero de la misma familia, con [[7 dormido≠muerto]] y [[8 trabajo a pedido]]. Ya avisan.
