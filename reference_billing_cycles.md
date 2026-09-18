---
name: reference-billing-cycles
description: "Fechas REALES de reset de las APIs pagas (Apollo día 12, SimilarWeb/RapidAPI día 7) — confirmadas por el user"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 63cc8b14-2ceb-446a-9c44-9c75690d5923
  modified: 2026-09-18T11:06:55.386Z
---

## 18/09: el RPC `bump_api_counter` ahora guarda el CICLO de RapidAPI (18→18), no el mes calendario
Aplicado en producción por CLI; copia en `sql/2026-09-18_bump_api_counter_ciclo_rapidapi.sql`
(⚠️ `supabase db query` se atraganta con el encabezado de comentarios: mandar desde `CREATE OR
REPLACE`). Antes: del 1 al 17 el worker leía **0 usados** y del 18 en adelante leía como gasto
del ciclo nuevo lo del 1 al 17 → el 18/09 el feeder anotó `skipped_throttle — pacing: used 40%
vs cycle 1%` (15.914 llamadas del ciclo viejo; habría durado ~12 días). Worker:
`_mismoCicloRapidApi(guardado, periodo)` compara el período ENTERO. La extensión sigue comparando
YYYY-MM y funciona igual porque el valor guardado ya es el inicio del ciclo. Test
`tests/parte-17-09.test.js` exige el mismo día en el SQL y en `RAPIDAPI_CYCLE_ANCHOR_DAY`.
Ojo al probar el RPC: llamarlo con un provider nuevo CREA sus dos claves en `toolbar_config`.

## 13/09: la EXTENSIÓN también renueva RapidAPI el 18 (commit 06f19a1) — con test que exige el
mismo día en los dos lados. Y Apollo cuenta por su ciclo 12→12 en el RPC `bump_api_counter`
(copia del SQL aplicado en `sql/`, commit bf05ddc).

## ⚠️ ACTUALIZADO 2026-08-24 — RapidAPI cambió de plan

**RapidAPI / SimilarWeb: ancla 7 → 18.** El plan viejo se reemplazó por un
`custom-40k-hard` de USD 25/mes que arrancó el **18 de agosto de 2026** y renueva cada 30
días (próximo: 17 de septiembre). Con el ancla en 7 el worker daba por empezado un ciclo
nuevo once días antes de tiempo. Ya corregido en el código (`RAPIDAPI_CYCLE_ANCHOR_DAY`).

**Consumo real al 24/08: 7,77% de 40.000 = ~3.108 en 6 días** (518/día). El presupuesto
parejo sería 1.333/día, así que va MUY por debajo: **sobra cuota, no falta**.

⚠️ **El cartel "0 quota left" del panel NO era RapidAPI.** El proxy lo calcula como su
propio cap diario (400/usuario) menos lo usado. Se leía como "se acabó el plan" teniendo
37.000 disponibles. Texto corregido en popup.js.


Ciclos de facturación reales, confirmados por Max el 2026-07-17 (antes el worker los tenía mal y desperdiciaba cuota):

- **Apollo**: renueva el **día 12** de cada mes. Plan 2,500 créditos. Panel: "Credits will renew on Aug 12, 2026".
  Cap del worker = 2,250 (margen 10% que pidió el user para cortar antes).
- **SimilarWeb / RapidAPI**: repone el **día 7** de cada mes. Cap del worker 40,000.
- **Serper (AutoGoogle)**: cap mensual por mes CALENDARIO (`autogoogle_serper_period` = "YYYY-MM"), no por ancla.
  Aparte, `serper_contact_used` es un cap DIARIO de 250 con formato "YYYY-MM-DD:N".

**Por qué importa:** cada proveedor tiene su propio ancla — no compartir la función de período entre
ellos. El worker tenía TODO anclado al día 6: Apollo reseteaba 6 días antes y calculaba el pacing
sobre días de mes calendario (el 17/07 creía que quedaban 15 días cuando faltaban 26 al 12/08 →
repartía el doble y se quedaba seco ~2 semanas). SimilarWeb reseteaba el contador 24h antes de que
la cuota real se repusiera → ese día gastaba sobre el cupo viejo.

**Si cambia un ancla:** hay que tocar los DOS lados — worker (`auto-prospector/index.js`:
`_cycleStartForAnchor`, `_billingCyclePeriod`, `_apolloCyclePeriod`) y cliente
(`modules/apiProxy.js` `currentPeriod`, `modules/supabase.js` `getApolloMonthlyUsage`,
`popup/popup.js` `apolloCyclePeriod`). Todo en UTC. Si el cliente calcula distinto al worker,
el contador del footer muestra 0 y la extensión cree que hay cuota libre.

**Al cambiar un ancla hay que SEMBRAR el contador**: el worker resetea a 0 cuando el período
guardado no matchea, así que hay que escribir el consumo real (ej. `apollo_calls_month` = 1064,
`apollo_calls_month_period` = '2026-07-12') DESPUÉS de que el deploy termine, o el worker viejo lo pisa.

**MillionVerifier (verificación de entregabilidad, activado 2026-07-24, zip v624):** verifica buzones
ANTES de enviar (mata el bounce que quema reputación; el envío tenía bounce 17% por local-parts
equivocados que el MX check no agarra). API real-time 1 email: `GET api/v3/?api=KEY&email=EMAIL`. NO
usa webhook (eso es bulk). Bloquea `result`=invalid/disposable; fail-open en catch_all/unknown/timeout/
error/cap. `_verifyEmailMV` en el envío fresco del agente (solo ahí). DORMIDA sin key.
**KEY: en RAILWAY ENV `MILLIONVERIFIER_API_KEY` (NO en la DB — se leyó de env primero y se borró de
toolbar_config el 2026-07-24, patrón seguro igual que SERPER_API_KEY).**
**PRESUPUESTO: 10.500 créditos deben durar 90 días → break-even 116/día. Cap = min(config, TECHO
HARDCODED 100). `millionverifier_daily_cap` arrancó en 20 (prueba) → subir a 100. Subir el techo
requiere DEPLOY (MV_ABS_DAILY_MAX).** Envíos reales ~60/día → sobra.
**ANTI-LEAK: el cap es DURABLE — al reiniciar el worker re-siembra `_mvCount` desde el contador
persistido `millionverifier_used`="YYYY-MM-DD:N" (antes vivía en memoria y cada restart lo reseteaba
a 0 → se reventaba el cap). Persiste en cada verificación.** Bloqueados: action=skipped
reason=mv_undeliverable + markEmailBounced local. **ROI: `toolbar_mv_results`(result,quality,blocked)
registra CADA verificación — SQL de ROI: si invalid+disposable 10-20% vale la pena, si ~todo ok es
plata al pedo → apagar.** Sub-request codes: ok/catch_all/unknown/invalid/disposable.

Relacionado: [[project-pending]], [[feedback-cost-awareness]]
