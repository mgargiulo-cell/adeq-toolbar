---
name: reference-digest-diario
description: "El resumen de salud diario (enviarResumenSalud + _boletinPorSeccion) y el parte del día (parteDelDia): de dónde sale cada número, ventanas, el tope de 1.000 filas de PostgREST, y cómo se usa para medir la tool"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-18T11:28:17.102Z
---

Certificado el 2026-09-04 (pedido del user: *"revisá los conceptos, cómo está armado y si los datos
son correctamente tomados… reármalo para que cuando te pase los informes puedas analizar y mejorar la
performance de todos los conceptos"*). Commit de la reestructura: ver git log "El parte del dia no
salia desde el 25/08".

**Fin de semana (arreglado 06/09, commit 5ad7423)**: el loop hace `continue` sáb/dom antes de la
cadena; el resumen quedó ANTES del portón (desde las 8 Madrid) y sale todos los días. El parte
(21h) sigue sólo lun–vie. El resumen del sábado 05/09 y el parte del viernes 04/09 no salieron
(el parte porque el fix del TDZ entró recién el sábado a la madrugada).

## Son TRES mails, no uno
- **Resumen de salud** (`enviarResumenSalud`, cada 24h, sale ~10:17 Madrid): titular + boletín por
  sección (`_boletinPorSeccion`) + SE ARREGLÓ SOLO / NUEVO / SIGUE IGUAL + ERRORES CONCRETOS. Es el
  que el user pega para que lo analice.
- **Parte del día** (`parteDelDia`, 21h Madrid, al dueño): envíos por MB, altas, stock, PARTE 2 con
  el trabajo a mano de cada MB. `parte_forzar_ahora=true` en config lo manda ya.
- **Digest por MB** (`generateDailyDigestAllMBs`): notificación personal, no revisado.

## Regla que lo cambia todo: PostgREST corta en 1.000 filas
Cualquier `limit=N` con N>1000 devuelve 1.000 en silencio. Había 44 consultas así en index.js.
`_traerTodo(url, headers, {max, pagina})` pagina con `Range` y devuelve `null` si una página falla
(nunca lista parcial). Aplicado al parte (caché de tráfico 36k, sendtrack 4k) y al resumen.
**RESUELTO DE RAÍZ el 04/09 desde el CLI**: `alter role authenticator set pgrst.db_max_rows = '20000';
notify pgrst, 'reload config';` — PostgREST lee su config del rol. Verificado por REST con la
service_role (`supabase projects api-keys --project-ref ticjpwimhtfkbccchfyp -o json`): `limit=2000`
devolvió 2.000. Las 44 consultas quedan bien hasta 20.000; por encima, `_traerTodo`.

## Qué dice cada sección del resumen y de dónde sale (post 04/09)
- ENVÍO (ayer completo + hoy hasta hh:mm) — `toolbar_agent_actions` sent, `ui_origin is null`,
  ventana día Madrid. Antes "hoy" a las 10 → siempre 0 de 40.
- DESCUBRIMIENTO (24h) — altas por `source` de `toolbar_review_queue` + embudo procesados→done de
  `toolbar_csv_queue` (dos poblaciones, dos renglones).
- BÚSQUEDA DE EMAILS (24h) — `email_found_at`, quién los encontró (última fuente de
  `email_sources`), por qué fallan (`toolbar_diag_sin_email`).
- COLA — un `count` por estado; drenado = procesados 24h.
- CICLOS FINALIZADOS → PROSPECTS (CRM) — `monday_sync_ultimo`; 🔴 sólo si el dato tiene ≥2 días.
  Cumplir el techo diario (700) es ✅, no un problema.
- RE-TRABAJO, REBOTES (7d, sólo agente, matcheado con `toolbar_bounced_emails`; NO frena nada).
- EL PLAN DEL 04/09, DÍA A DÍA — pings de `apollo_quemar_ciclo`, `barrido_no_publisher`,
  `sellers_google`, `fetch_page_content`; entradas por AdSense (marca en `ad_networks`), `.za`,
  marcas del barrido, emails por vía (apollo/serper_persona/rol_mx/google_contact/scrape).
- TIPOS DE EMAIL (lunes, 30d) — con `_tipoDeEmailParaRanking`, el mismo clasificador del envío.
- GASTO DE CLAUDE — proxy (`toolbar_gasto_claude_cruce`) + desglose por motivo (existe desde 03/09).
- ERRORES CONCRETOS — SIN EMAIL es STOCK por último motivo (dicho así); intentos de 24h aparte;
  descartes por motivo real (`geo_excluida:United States`, no "geo_bloqueada"); skips 24h; ejemplos
  sin duplicar. Alertas crónicas con el nombre del job en el título.

## Revisión del parte del 07/09 (commit 0fc36e6): siete renglones mentían
Método que funcionó: pegar el mail, y para CADA número ir a la tabla de origen. Lo que salió:
- **"agente 1/40 · NO hay descartes — no llegó a intentarlo"** → FALSO. `agent_slots_done`
  decía 13,14,15,16,17 y `agente_envios` "0 de 8 en este slot". El pool tenía 2.427 elegibles.
  Causa (medida en `casilla_envios` del CRM): **el CRM manda 25/h por buzón de 13 a 17 h**, y 25
  es `CUPO_CASILLA_HORA`; el agente entra, ve la casilla llena y hace `break` — antes sin registro.
  Ahora deja `cycle_cupo_casilla`, el parte lo lee (`action=in.(skipped,cycle_*)`) y el vigilante
  (`vigilarAgenteFrenado`) alarma con <25% del objetivo mostrando el cupo por casilla.
  **RESUELTO con el CRM (su commit c54cbad, el nuestro del 07/09 a la noche)**: la mesa común
  pasa a informar `tope: 100` (red) y `restantes`; el CRM mantiene su freno propio de 25/h; la
  toolbar mantiene el suyo: `CUPO_CASILLA_HORA = 25` medido sobre LO NUESTRO (`toolbar_agent_actions`
  sent de la última hora, agente + manual). `cupoDisponibleCasilla` → `{hay, usados, tope,
  restantes, propios, motivo}`; sin `tope` en la respuesta vale 25 (compatible con el CRM viejo).
  **Regla de ellos, correcta: el 100 es una red, no un cupo a gastar** — el ritmo lo ponen el cap
  de 20/día y el batch ≤8 por turno. El popup ahora también registra los envíos manuales en
  `casilla_envios` (`_registrarEnMesaComun`, modules/gmail.js): el 07/09 Agustina mandó 27 y la
  mesa decía 1.
  **Segunda vuelta del CRM (07/09 noche): sacaron TODOS sus topes** (cadencia y manual). El GET
  manda `sin_tope: true` + `tope` inalcanzable (a propósito, para no caer en nuestro 25).
  `cupoDisponibleCasilla` lo lee → motivo `sin_tope_crm`, la mesa nunca frena. **Queda un solo
  freno: el nuestro** (25/h propio + 20/día). ⚠️ La reputación de las tres casillas depende
  ahora sólo del volumen que ELLOS manden: el `count` de `casilla_envios` es la única medida
  real por buzón — mirarlo con ellos esa semana, y si los rebotes suben, es lo primero a revisar.
  **Cierre del CRM (07/09, tarde)**: pusieron un cortacircuitos, no un cupo: **300/h y 1.000/24h
  móviles por casilla** (Workspace bloquea 24 h a las 2.000), contado sobre `casilla_envios`
  (incluye nuestros manuales desde v695), en `sendColdEmail` (las cuatro puertas), **falla
  abierto**, lo frenado se reintenta solo, avisa una vez por buzón en la campana del board.
  Normal ~65/h y 100-150/día; peor día legítimo ~150/h y ~365/día (sales@ drenando vencidos).
  Nuestro freno (25/h + 20/día) queda muy por debajo: nada que cambiar de este lado.
  **El parte muestra "Volumen de cada buzón"** (tarjeta después de Envíos): lee `ultimas_24h` y
  `por_origen` del GET `/casilla-envios` por casilla (`agent_whitelist`), y los umbrales
  `corte_dia`/`corte_hora` que manda el CRM (1.000/300 si no vienen). Si los conteos de 24 h
  fallan, vuelven en `null` y el parte dice "sin dato del CRM" (cubierto por test).
  ⚠️ El 07/09 a la noche el CRM dijo "ya están los campos" pero **el GET en vivo no los traía**
  (verificado con curl a las tres casillas): lo tenían en local. Verificar con curl antes de
  dar por bueno cualquier "ya está" del otro lado. Test en `parte-07-09.test.js`.
- **"Las dio un MB a mano: sales 105…"** → las 160 filas `monday_refresh` llevan `created_by` =
  ejecutivo de la ficha (feeder, línea `createdBy:`). Ahora la fuente reciclada cuenta como agente.
- **"Emails encontrados (no tenían) 544"** → `polish_enriquecidos_hoy` suma `enriched++` por
  cualquier dirección nueva; `email_found_at` (sólo si no tenía ninguna) decía 14. Se lee la columna.
- **"Ya contactados (30 días) 20"** → eran los envíos del día de la misma MB. `send_date >= hoy`
  no cuenta.
- **"Fuera del foco (anglo) 68-85%" y "US · 34"** → `detectGeo()` del popup devolvía "US" para
  todo TLD fuera de un mapa de 15 (wielerrevue.nl, cyclingpro.net…), y el historial contaba
  Gmail/YouTube/claude.ai/console como "URLs abiertas". Popup: SimilarWeb › TLD › "" (v693).
  Parte: `_NO_ES_WEB_PARTE` excluye esos hosts. **Los datos anteriores al 07/09 tienen ese GEO
  contaminado: no comparar "anglo" hacia atrás.**
- **"autopilot trajo 1411 · pasaron 0 (0%) — gasta créditos para nada"** → 1.368 filas de
  julio-agosto congeladas por el barrido del 02-04/09 (15d backoff); RapidAPI 9-49 llamadas/día
  esa semana. `frozen` va aparte con 🧊. `monday`→`crm_reciclado`, `sellers_json`→`sellers`.
- **"Monday finalizados reciclados" / "deal vivo en Monday"** → dice CRM.
- AutoGoogle: 93 frases con ≥10 búsquedas y 0 leads (1.538 búsquedas de Serper) — el top
  exige `qualified>0` y `_muertas` no vuelven a la exploración.
- "Toolbar abierta ~15 min · 54 sesiones" → relabel "Panel en primer plano (piso)": es la suma de
  ratos con el panel al frente, no tiempo de trabajo (eso es "Cobertura").
Test: `tests/parte-07-09.test.js` arma el parte real con esos datos (cuenta de servicio falsa
con RSA generada → llega al MIME), decodifica el quoted-printable y lee el HTML.
El CLI de Railway de la Mac está ROTO (shim sin binario): los logs hay que pedírselos al user.

## Revisión de los mails del 16-17/09 (commit 6ab7b80, v708)
Pedido del user: *"fijate si algún proceso está trabado, pausado o no se ejecuta solo, y reparalo"*.
- **RapidAPI no reinició el ciclo el 18** → ver [[reference_billing_cycles]]. Era lo único que
  estaba frenando procesos ese día.
- **`freeze_failed … HTTP 409` (~30/día en "COLA — N en error")**: `Prefer: resolution=merge-duplicates`
  SIN `?on_conflict=` resuelve contra la PK. `toolbar_frozen_leads`, `toolbar_csv_queue` y
  `toolbar_pitch_drafts` tienen PK `id` y UNIQUE aparte → 409 al repetir. Arreglado en los 5 POST
  del worker y en 2 de la extensión (el import de CSV tiraba el lote ENTERO de 500 en silencio).
  Test estructural en `parte-17-09.test.js`. Las demás tablas con upsert tienen la clave natural de PK.
- **"🔌 N fuentes sin producir" diario desde el 09/09 = falso**: adstxt/sellers_json/majestic sólo
  las inyecta `_runFeederSlot`, que se saltea con Prospects ≥ `FEEDER_RQ_SATURATION` (3.000; había
  4.314 = 108 días de envíos). `_frenoDelFeeder(runs)`: saturado/meta cumplida → "en pausa", sin
  alerta; cualquier otro salteo → alarma y dice el motivo. `toolbar_feeder_runs` es donde mirar.
  ⚠️ similar, autogoogle y el reciclado del CRM NO respetan esa saturación: el pool sigue creciendo
  ~150/día. Decisión de negocio, no tocada.
- **Linter `homoglifos_mezclados`** miraba el mail entero (latín + cirílico/griego en cualquier
  lado): frenó 3 veces el resumen del dueño y 2 un pitch a xsport.ua. Ahora exige dos alfabetos en
  la MISMA palabra (`lib/email.js`).
- **PENDIENTE, DECISIÓN DEL DUEÑO — `rol_mx` rebota 50% (23 de 46 en 7d)**; scrape 1%, Apollo 7%.
  Se guarda sin verificar (sólo MX) y sale de `reserva` con catch-all/sin_verificar porque el lead
  no tiene otra dirección. Propuesta: exigir MV "ok" como ya hace `pattern`. Lo implementé y lo
  REVERTÍ: el test C36 de `reintento_rebote-13-09b` dice textual "cambiarlo es decisión del dueño".
- **Segunda pasada el mismo día (commit 3d4cebe)**: (a) `_filtrarReciclables` no miraba congelados
  VIGENTES → 42 de 680 encolados/día se re-congelaban (y con el 409 arreglado escalaban 15→30→60 por
  vuelta): quinto filtro, falla abierto. (b) 462 de 4.490 "contactables" (10%) sólo tenían
  direcciones adivinadas → renglón nuevo "De ellos, sólo con dirección adivinada"
  (`_soloDireccionesAdivinadas`; sin fuente anotada NO cuenta como adivinada). (c) la tabla de 7
  días pintaba 🔴 los renglones "(re-trabajo)"/"(envío)": no son fuentes → ↻ / 🧊.
  ⚠️ `npm install` con el npm de esta Mac le SACA los campos `libc` al package-lock: no commitearlo
  (`git checkout -- auto-prospector/package-lock.json`); el lock del repo ya tiene acorn.
- Por diseño, no son fallas: `next_day` con `reintentar: ads_txt_no_verificable…` (cap de 30 días),
  `claude_techo 700/700` todos los días a las ~17 h (US$0,75/día), `sellers_discovery 0 de 1`.
- **Rutina semanal automática**: el user la pidió. Las rutinas en la nube tienen el repo pero NO
  la base (el CLI de Supabase y `~/.adeq-cws.json` son de esta Mac) ni conectores. Opciones
  planteadas el 18/09, sin crear nada todavía.

## Cómo se lee para mejorar la tool
Cada sección tiene su medida de éxito: envíos vs cupo (día completo), % contactable de las altas,
emails rescatados por vía, rebote por buzón, respuesta por tipo. Lo que no cambia en 30 días se
saca del mail; lo nuevo (decisiones del 04/09) va en EL PLAN hasta que se estabilice.

Relacionado: [[reference_informe_salud]], [[reference_parte_diario]], [[feedback_alertas_automaticas]]
