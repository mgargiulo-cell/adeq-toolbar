---
name: project-pendientes-auditoria
description: "Lo que el user pidió el 2026-08-27 y todavía NO está hecho: auditoría profunda y funciones nuevas, con lo ya verificado de cada punto"
metadata: 
  node_type: memory
  type: project
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-27T18:23:50.224Z
---

El 2026-08-27 el user pidió una auditoría profunda + una lista larga de mejoras. Se hizo la
parte urgente (alertas, linter, cupos por MB). **Esto es lo que QUEDA**, con lo ya averiguado
de cada punto para no volver a investigarlo desde cero.

## Ya verificado — no re-investigar
- **RapidAPI y Apollo FUNCIONAN.** Probados contra el proxy el 27/08: los dos responden bien
  (`/all-insights?domain=clarin.com` y `/v1/organizations/enrich`). El "Failed to fetch" que
  ve el user es del lado del navegador. El mensaje "Configure your RapidAPI key" salía ante
  CUALQUIER fallo y mandaba a arreglar lo que no estaba roto — ya corregido.
- **El sellers.json desde ads.txt no está roto: se REMOVIÓ el 2026-05-11 a pedido del user**
  ("feature poco útil"). `probeSellersJson` y `fetchAdsTxtSystems` siguen exportadas en
  `modules/sellersJson.js` sin que nadie las llame. **El user cambió de opinión y lo quiere de
  vuelta** — es reconstruir, no arreglar.
- **Los saves de países SÍ guardan.** Verificado en `toolbar_config`: `agent_focus_config` y
  `worker_discovery_config` tienen las exclusiones puestas. Si el user insiste, el problema
  está en cómo la UI muestra lo guardado al reabrir, no en la escritura.
- **`toolbar_config`**: PK = `key`, y las policies permiten escribir todo salvo keys sensibles;
  las `agent_%` solo las escribe mgargiulo@.

## ✅ HECHO el 2026-08-27 (v648, worker en e0bf82c)
1. **RapidAPI "Failed to fetch"** — no era la API ni la key. En `apiProxy.js` el `fetch` estaba
   FUERA del try: un corte de red es un TypeError, no un status, así que salía disparado sin
   pasar por el bucle de reintentos. El retry cubría los 5xx (raros) y dejaba pasar el corte de
   red (lo que de verdad ocurre cuando el service worker se duerme).
2. **Prospects-2** — `toolbar_prospects_offline` + `revivirProspectsOffline` (cada hora).
   Vuelve por la COLA, no directo a Prospects, para re-pasar las puertas con las reglas de hoy.
3. **Bloqueos antes de buscar** — `_GL_BLOQUEADOS_USER` saca los países excluidos ANTES de
   elegir el `gl`. Ojo: el TLD no es el ISO (`.uk` → GB), mapeado en `_TLD_A_ISO`.
4. **Filtros de Review** — el prefix match del ISO pescaba países ajenos: **CH devolvía Chile y
   China**, MA devolvía Malaysia, SE devolvía Serbia. Sacado el prefijo del ISO; queda el exacto.
5. **Queue Status** — el pool se rellenaba UNA VEZ POR DÍA, así que el tope de 700 funcionaba
   como techo diario. Ahora `rellenarWaitingPool` en cada ciclo. + purga diaria que **NO toca
   `skipped`** (son la memoria del dedupe; borrarlos haría redescubrir 14.136 dominios).
6. **Cupo por persona** — `agent_max_per_day_by_user` ya lo leía el worker; el popup lo BORRABA
   en cada save. Ahora se edita en el panel. Vacío ≠ cero.
7. **Similar sites** — semillas desde los 128 dominios que YA CONTESTARON, no por puntaje.
8. **sellers.json desde ads.txt** — `toolbar_adsystem_seen`; cada análisis manual aporta redes.
9. **11 idiomas nuevos** — 440 frases (el ro hu cs bg sr vi th ko zh ms). Grecia era el 8º país
   del pool y no había UNA keyword en griego.
10. **La cadena de mantenimiento ROTA** — `similar_expansion` estuvo 26h sin correr (no fallaba,
    no lo llamaban: era el 7º de la cadena). `autopilot_similares`, 146h.
11. **Email discovery** — el pulido hacía la pasada general ANTES que los mudos; 237 leads con
    `email_intentos=0` esperando. Invertido: los mudos primero.
12. **Dedupe antes de pagar** — el chequeo vivía al final de `saveToReviewQueue`, después de
    gastar tráfico + email + Apollo + pitch. 176 dominios en 14 días.
13. **Seguridad** — la extensión pedía la config ENTERA y usaba 2 valores. Y un MB podía leer la
    key de Monday de OTRO. Cerrado. Las keys sensibles ya estaban bien protegidas por la policy
    RESTRICTIVA `cfg_no_write_sensitive` (las permisivas no las exponían).

## Pendiente — pedido explícito del user
1. **Prospects-2 (buzón offline)**: cuando una URL se descubre, se paga el crédito y CUMPLE los
   requisitos pero cae en una GEO bloqueada, hoy se descarta y se pierde el dato comprado.
   Debe ir a una tabla oculta y **volver a Prospects sola si se destilda ese país**.
2. **Bloqueos que informen la BÚSQUEDA, no solo el resultado**: hoy AutoGoogle/autopilot
   descubren y recién después se filtra por GEO/idioma. Hay que empujar el filtro un paso
   antes —`gl`/`hl`/keywords— para no gastar el crédito en algo que se va a tirar.
3. **Review Prospects**: los filtros no responden a cada clic ni filtran lo correcto.
4. **Queue Status**: se traba; debe purgar solo lo que ya procesaron los MB y el agente.
5. **Similar sites**: mejorar la lógica.
6. ~~Frases en varios idiomas~~ — **DESCARTADO por el user (27/08): "no quiero pitchs en otros
   idiomas".** Las frases de BÚSQUEDA sí se ampliaron (440 en 11 idiomas); los pitch NO se tocan.
7. **Monday en Analysis**: revisar errores de envío y la cascada.
8. ~~Apollo alternativo~~ — **DESCARTADO por el user (27/08): "de momento no".** No volver a
   proponerlo salvo que él lo pida. Para cuando vuelva el tema, lo medido: 541 unlocks en 30
   días → 47 emails (8,7%), BAJO volumen pero la MEJOR calidad (9,1% de respuesta real contra
   0% del scrape del sitio, 0/106).
9. **Auditoría profunda**: botones, seguridad, abuso de recursos, y sobre todo las lógicas de
   **descubrimiento de URL** y **búsqueda de email**.

## ✅ Segunda tanda HECHA (27/08 noche, worker en 0d3ca50, zip v652)
14. **Backoff escalonado del pulido** — era fijo 3 días; 225 leads con 8+ intentos se llevaban
    el 87,4% de los 3.339 intentos con CERO emails. Ahora 3/10/30 días según fallos.
15. **El agente ya no manda a los `suspect_reject`** — la marca sale de los comentarios del MB
    y solo pintaba un ⚠️; salieron 31 mails a sitios marcados. Fuera del pool del agente.
16. **Tablas de diagnóstico** — `toolbar_diag_sin_email` y `toolbar_diag_descartes`, con
    comentarios en castellano (no códigos). El mail muestra un caso de cada tipo.
17. **Presupuesto de mantenimiento ALTERNADO** — la rotación de la tarde no alcanzó: el techo
    son 3 min y el pulido consume 2, así que la cola de atrás seguía sin turno
    (`auditoria_emails` 50h, `similar_expansion` 27h). Vueltas pares = pulido+auditor;
    impares = mantenimiento entero. Configurable: `mantenimiento_techo_min` (2-5).
18. **Boletín por sección en el resumen diario** — `_boletinPorSeccion`: veredicto ✅/🟡/🔴
    por sección contra lo que DEBÍA hacer. Sigue la regla: un resumen + una alerta por día.
19. **Un lead contactado no resucita** — el dup-check solo miraba `pending` y el upsert pisaba
    los `validated` de vuelta a `pending` (placar.com.br: 9 salteos en un día). 105 corregidos.
20. **Claves verificadas en vivo**: similarsites.com scrape OK (20 similares, parser cubre el
    shape), RapidAPI fallback OK, páginas vistas OK punta a punta (clarin: 71,3M × 2,101 =
    149,8M; extensión y worker leen `Traffic.Engagement.PagesPerVisit`), envío manual OK (52
    en 7 días, 0 bloqueados por linter).

## ✅ Tercera tanda HECHA (28/08 madrugada, zip v657)
21. **Lupa global en Prospects** — busca en el 100% del pool en el SERVIDOR (dominio + título),
    ignorando filtros. Debounce 350ms, tope 100. No la pisa el auto-refresh.
22. **Chips de Review arreglados** — `Agent` daba lista VACÍA (buscaba `created_by` nulo; el
    worker escribe `worker@autofeeder` → 1.009 filas). `Autopilot` daba 1 fila (etiqueta
    retirada) → ahora es la familia similar+adstxt+majestic (559).
23. **Alert con las dos mitades** — botón ✓ que aprueba, limpia la marca y manda el
    contraejemplo al destilador (antes solo aprendía a rechazar, nunca a aflojar).
24. **Contactos Adicionales** — solo se limpiaba el slot 1: los slots 2 y 3 retenían los
    emails de la web ANTERIOR. + dedupe entre slots (mismo email 2 veces = 2 mails).
    ⚠️ La promesa "si el original rebota Monday se actualiza con el adicional" NUNCA se
    implementó — no re-prometerla.
25. **Google Keywords: 23 idiomas** (11 nuevos con 40 frases c/u).
26. **Prioridad por tipo de email AUTO-REAJUSTADA semanalmente** — medido sobre 90d, con
    suavizado, mínimo 15 envíos y **margen del 15%** para que un empate no reordene.
    Datos actuales: apollo 9,3% · manual-adicional 7,7% · genérico 6,5% · persona 6,3% ·
    rol 6,3%. Con el margen, el orden actual se CONSERVA.
27. **La purga borraba los `done` el mismo día** (cortaba por `uploaded_at`, no
    `processed_at`) — se llevó 429 filas, las que miden qué fuente convierte.
28. **`_latirDormido`** — ver [[reference_alertas_falsos_positivos]] falso positivo 7.

## ✅ Cuarta tanda (28/08) — prioridad que se auto-ajusta + tablas de control
29. **Prioridad por TIPO DE EMAIL, reajuste semanal** (`reajustarPrioridadTiposEmail`): mide
    90d, suavizado, mínimo 15 envíos y **margen del 15%** para que un empate no reordene.
    Sin el margen, genérico (6,5%) habría pasado a persona (6,3%) — un decimal poniendo un
    `info@` sobre una persona, justo la regla del user al revés.
30. **`toolbar_template_perf`**: foto semanal por idioma (enviados, respuestas, %, puesto).
    ⚠️ **CORRECCIÓN a lo que le dije al user**: las plantillas NO se eligen al azar.
    `pickAnyTemplate` YA pondera por respuestas reales con piso de participación, y su pool ya
    viene por idioma. `pickRandomTemplate` es SOLO el fallback sin borradores. Escribí un
    segundo ponderador y lo saqué: habría multiplicado sesgos sin poder saber cuál manda.
    Línea de base 28/08 — en: db_20 13,6% vs db_10 2,7% · es: db_30 12,5% vs **db_22 0% en 25
    envíos** · pt: db_36 10,9% vs db_35 3,9%. **Revisar a los 20 días (≈17/09)** y reescribir
    las peores. El resumen diario trae mejor/peor por idioma.

## 🔎 Hallazgos del repaso general (27/08, noche) — SIN tocar todavía
Medidos sobre `toolbar_response_tracking` (3.951 envíos históricos, 131 respuestas reales):
- **Argentina: 0 respuestas en 105 envíos** (re-verificado 28/08: sigue 0/105, último de la
  tabla, debajo de México 2,6% y Grecia 1,7%). De 64 envíos a `.ar` rebotaron 7 (11%): los
  otros 57 LLEGARON y nadie contestó. Dos hipótesis que solo se separan con una prueba —
  spam-foldering en proveedores argentinos, o el mensaje no funciona ahí. **Pendiente: que el
  user se mande un mail de prueba desde la toolbar a una casilla suya en un proveedor
  argentino y vea si entra a Recibidos o Spam.**
  ✅ **PROBADO 28/08: EL MAIL LLEGA OK.** No es entregabilidad. Descartado además, con datos:
   · no es la ventana de medición — AR y ES tienen el MISMO reparto semanal de envíos;
   · no es un split de etiqueta (como "Brasil"/"Brazil") — solo existe "Argentina", 105/0;
   · no son emails genéricos — AR 32,4% vs España 33,3%, y España responde 8,5%;
   · no son sitios malos — rionegro.com.ar, americatv.com.ar, elliberal.com.ar, con las
     direcciones correctas (`publicidad@`, `publicidadonline@`);
   · no es tamaño — los alemanes que responden promedian 37M de tráfico contra 16M de los
     argentinos, o sea que AR apunta a sitios MÁS CHICOS y aun así no contesta;
   · no es un buzón — los TRES MB mandan a AR y los tres tienen 0, con 7,4% / 6,2% / 6,4%
     en el resto.
  **Queda solo la hipótesis comercial: el mensaje o el mercado.** Eso lo decide el user, que
  es el media buyer. Estadísticamente 0 de 105 con una tasa base del 8% tiene probabilidad
  ~0,02% — no es azar. Brasil 6,2%, España 8,8%, **Alemania 14,6%**. AR es
  el tope de la cascada GEO y no contesta NADIE — o hay un problema de entregabilidad hacia
  .ar (¿spam?), o el mensaje no funciona ahí. Es un posible BUG, no solo una preferencia.
- ~~Las plantillas se eligen al azar~~ — **ERA FALSO, ya se ponderan.** Ver punto 30.
- **Horario**: 11h Madrid rinde 12% (muestra chica), 18-20h ~8-9%, 12h 4,8%. El reparto de
  slots no mira esto.
- 639 envíos figuran "(sin template)" en el cruce → el template_id no se registró en esos.

## Números del 27/08 que enmarcan el trabajo
Motivos de salteo en 3 días: `sendtrack_30d` 752 (dedupe normal) · `reengagement_mv_riesgo` 413 ·
`all_candidates_undeliverable` 292 · `no_email_after_enrichment` 143 · **`linter:mayusculas` 88
(ya arreglado)**. Pool: 922 contactables / 505 sin email.

Relacionado: [[reference_alertas_falsos_positivos]], [[reference_entregabilidad]],
[[reference_autogoogle]], [[project_pending]]
