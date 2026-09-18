---
name: reference-4-metricas
description: DISPARADOR "¿cómo venimos?" / "SQL DE ANALISIS" — las 5 métricas fijas del AGENTE AUTOMÁTICO que hay que analizar siempre
metadata:
  type: reference
---

## 🔑 DISPARADOR: **"¿cómo venimos?"** o **"SQL DE ANALISIS"**

No preguntar cuáles son. Son SIEMPRE estas cinco, y **SIEMPRE del AGENTE AUTOMÁTICO,
nunca del trabajo manual del media buyer** (definición precisada por el user 2026-08-24):

1. **Cuántos emails envió por día el agente, para cada media buyer.**
   `toolbar_agent_actions` con `action='sent'`, cortado por día y por `user_email`.
   Objetivo: 20 de PRIMER CONTACTO por MB. `secondary_sent` y el re-trabajo van aparte.
2. **Cuántas URLs se agregaron por día a Prospects, POR FUENTE de agregado.**
   autogoogle · autopilot · monday_refresh · sellers_json · adstxt · similar · majestic · csv/manual.
3. **De las agregadas, cuántas tienen email y cuántas no.** Mismo corte por día y fuente.
4. **Cuántas URLs por día se ELIMINARON de Prospects** por no cumplir requisitos, una vez
   que ya habían sido agregadas — o sea, por el barrido automático. Usa `rejected_at`.
5. **Cuántas URLs por día se les ENCONTRÓ email** habiendo entrado sin ninguno, por el
   barrido automático. Usa `email_found_at`, que se marca SOLO si el lead no tenía email
   (si ya tenía y se le suma otro, no es un rescate).

**SQL:** `sql/reporte_diario_4_metricas.sql`. Correrlo sobre los últimos 7 días.

## ⚠️ Desde cuándo es exacta cada una
- **1, 2 y 3** son exactas desde siempre (`created_at` existe).
- **4 y 5 solo desde el 2026-08-24.** Hasta ese día `toolbar_review_queue` tenía ÚNICAMENTE
  `created_at`: no había forma de saber CUÁNDO se rechazó un lead ni cuándo se le encontró
  el email. Se agregaron `rejected_at` y `email_found_at`
  (`sql/2026-08-24_fechas_de_barrido.sql`) y los cuatro puntos de rechazo automático más el
  rescate del polish las escriben. **No reconstruye el pasado.**

## Al leer los resultados
- Si el primer contacto da menos de 20, el corte por `reason` de los `skipped` dice por qué.
  Ahí aparecen también los frenos MÍOS (`linter:*`, `rebote_alto_*`): ver [[project_pending]].
- La métrica 2 revela si el pool sigue dependiendo del import.
- La 4 alta NO es mala señal: significa que los barridos automáticos funcionan.
- La 5 es la que mide el rescate de leads mudos, que era el agujero de los ~310 sin email.

## 🪫 El agente regula su ritmo por el stock (regla del user, 2026-08-25)
"Idealmente debería tener 10 días (dos semanas de 5 días laborables) por delante de stock de
envíos. Si ve que está bajando, debe regular a menos el envío hasta que Prospects vuelva a subir."

Implementado en `runAgentCycle`:

| Stock (contactables ÷ objetivo pleno) | Cupo por MB |
|---|---|
| ≥ 10 días | 20 (pleno) |
| 7-10 días | 15 |
| 4-7 días  | 10 |
| < 4 días  | 5 (piso, nunca cero) |

Tres cosas que NO hay que "corregir" porque son deliberadas:
- La autonomía se mide contra el objetivo COMPLETO (20 × MBs), no contra el cupo ya recortado:
  si no, bajar el cupo subiría la autonomía y el freno se soltaría en un bucle.
- El piso es 5 y no 0: sin envíos no hay respuestas, y el pool tarda días en recuperarse igual.
- Si NO se puede medir el pool, no se recorta nada.

Config: `agent_dias_stock_objetivo` (10), `agent_regular_por_stock` (true).
Avisa con la clave `stock-bajo-freno-envio`, late en `stock_envios`, y sale en el parte diario.

**Al leer las métricas:** si los envíos bajan de 20/MB, mirar PRIMERO si fue este freno antes
de buscar un bug. Y si el freno se repite varios días seguidos, el problema son las FUENTES.


## ⭐ LOS CUATRO CRITERIOS DEL USER (textual, 2026-09-01)
*"Como siempre digo lo importante a medir es"* — cuando pregunta cómo venimos, responder ESTO
primero, día por día (no el promedio: lo que le importa es que pase **todos los días**):

1. **Que cumpla con los envíos programados** — cupo × buzones habilitados, L-V.
2. **Que el feeder traiga webs nuevas todos los días** — altas en `toolbar_review_queue`.
3. **Que se revisen las URLs sin mail y se les encuentre mail** — `email_found_at`.
4. **Que se revisen las URLs erróneas y se eliminen de Prospects** — `rejected_at`.

Consulta de una línea por día (las cuatro juntas) — armar el resultado como UNA sola columna
concatenada: ⚠️ el CLI de Supabase devuelve las claves del JSON en orden ALFABÉTICO, y leer
varias columnas con `paste` desalinea los valores. Ya me hizo reportar mal los números una vez.

## 🅿️ Prospects-2: las bloqueadas por GEO no se pierden (verificado 2026-09-01)
Pregunta recurrente del user. Verificado contra la base, no contra el informe:
`toolbar_prospects_offline` tenía 875 aparcadas (834 United States · 34 United Kingdom ·
4 Russia · 3 Australia), con el motivo `geo_excluida:<país>` y `revived_at` en null.
⚠️ **La config guarda ISO (`US, AU, RU, NZ, GB`) y lo aparcado guarda el NOMBRE
("United States")**. `revivirProspectsOffline` traduce entre las dos formas; probado con los
cuatro países: quedan aparcados, y simulando destildar US vuelven SOLO los 834 de EE.UU.
Vuelven a la COLA, no directo a Prospects, así re-pasan las puertas de hoy (ads.txt, idioma,
detector) sin gastar crédito de RapidAPI, porque el tráfico ya está cacheado.
