---
name: feedback-alertas-automaticas
description: REGLA DE ORO — todo lo que se construya debe traer alerta automática que avise cuando se rompe o se desvía de lo esperado
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-07T18:04:12.854Z
---

Regla de oro del user (2026-08-11): **toda función, job o flujo que armemos tiene que venir con su alerta automática**, que permita detectar rápido cuando algo está roto o no funciona según los parámetros deseados. Nada se entrega sin su detector.

**Why:** los 5 problemas grandes de la auditoría del 2026-08-11 eran todos SILENCIOSOS — ninguno tiró error. El autopilot logueaba "fallback a Majestic global" como operación normal mientras llevaba semanas sin descubrir nada; 4 jobs de limpieza se apagaban solos diciendo "pool completo" cuando en realidad la query había fallado; el slot de las 20:00 no existía por la ventana horaria y nadie se enteró; las X del media buyer se guardaban y no las leía ningún código. El costo no fue el bug: fue el tiempo hasta descubrirlo. Esto se conecta con [[project_pending]], donde el patrón "un 'no pude ahora' tratado como 'ya está'" ya lleva 11 casos.

**How to apply:**
- Alertar sobre **lo que NO pasó**, no solo sobre errores. La mayoría de las fallas acá son no-ops silenciosos: un job que no corrió, un feeder que devolvió 0, un slot que no disparó, un contador que no se movió. Un `try/catch` que no explota NO es señal de salud.
- Cada job nuevo declara su **parámetro esperado** (cuántos por día, cada cuánto corre, qué rinde) y algo compara lo real contra eso.
- Prohibido el **fallback silencioso**: si algo degrada a un camino secundario, avisa. Si una fuente devuelve 0 cuando debería devolver N, avisa.
- Persistir `last_run_at` / `last_ok_at` por job, para poder detectar "hace X días que esto no corre".
- El destino de la alerta tiene que ser algo que se pueda revisar al empezar una sesión (tabla de salud + un SQL de chequeo), no solo un log de Railway que nadie mira.
- Ver [[reference_agent_audit_sql]] para el SQL de auditoría manual que esto debería volver innecesario.
- **Un `catch` pensado para la red también atrapa los bugs propios** (caso 04/09): `fetchPageContent`
  devolvió `null` para TODOS los sitios durante dos días (02→04/09) porque un `ReferenceError`
  (`geo is not defined`, commit 30c0daec) caía en el catch de "no pude bajar la página". El
  clasificador quedó ciego, sin un solo error visible. Regla: en un catch de red, separar
  `ReferenceError/TypeError/SyntaxError/RangeError` → log 🚨 + `saludPing` en rojo. Y el detector
  que de verdad lo encuentra es **un test que llama al código exacto** (`tests/_worker-exportado.mjs`
  → `cargarWorker(["fetchPageContent"])`), no una copia de las regex: el replay con el código real
  lo mostró en un minuto (687 sitios → 687 null); un replay con regex copiadas no lo habría visto.
- **"Esperar para siempre" es un fallback silencioso** (caso 07/09, v690). Un `fetch` sin
  `AbortSignal.timeout` no falla: **no vuelve nunca**. La promesa queda viva, el `catch` no se
  ejecuta, y la pantalla se queda con el texto inicial del HTML — que el usuario lee como
  "está pensando" durante horas. Es el peor de todos porque no hay error que buscar.
  Regla: **todo fetch de UI lleva timeout, y toda espera visible lleva watchdog** — si a los N
  segundos nadie pintó, se pinta "no pude" con un botón de reintentar. Un cartel que se puede
  quedar mudo es un cartel roto. Se aplicó en `buscarEnCrm` (2,5 s × 2) + `_armarWatchdogCrm`
  (6 s) + `btn-crm-reintentar`. Test: `tests/crm-rapido.test.js` corre el código real contra
  un fetch que nunca resuelve.
