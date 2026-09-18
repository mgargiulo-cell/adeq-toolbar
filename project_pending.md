---
name: adeq-toolbar-estado-y-pendientes
description: "Estado al 2026-07-17 — zip v614 (subida al Store). Rescate de Monday CERRADO: 18/18 leads recuperados al CRM, 0 duplicados. Jornada de 5 bugs silenciosos que costaban plata y leads: Monday mataba items por countryShortName vacío (15 leads recibieron mail sin quedar en el CRM), rebotes reenviados en loop, ciclos de Apollo/SimilarWeb mal anclados, RPC de yield duplicada (AutoGoogle elegía keywords 100% random). + turno hispano 50/50, pre-listado anti-desperdicio, GEO sin exclusiones con rotación día a día."
metadata: 
  node_type: memory
  type: project
  originSessionId: 63cc8b14-2ceb-446a-9c44-9c75690d5923
  modified: 2026-07-27T17:00:28.872Z
---

**2026-09-03 (tarde) — los 9 pedidos del CRM, cerrados.**
Ver `PEDIDOS-DASHBOARD-2026-09-03.md` y `REGLA-FINAL-CRM.md`.
⚠️ **Dos de los tres 🔴 tenían el diagnóstico equivocado, y verificarlo antes de codear lo
evitó**: el pedido 3 decía "el bounce_retry no corre" y corre (156 `sent`) — la "única ficha
con reemplazo" ERA el retry de las 15:25, con el circuito cerrado y `ciclo_contacto=2`. Un
dato no es una tendencia. Y el pedido 2 pedía un `template_id` **imposible**: es uuid con FK
al catálogo del CRM (705 filas) y las nuestras son baked (`baked_es_2`) o borradores de
`toolbar_pitch_drafts`. Se resolvió con `template_ref` + `variant` + `idioma`.
⚠️ **`crm_board_template_stats` es el BANDIT con el que el CRM elige variante**, no un
reporte: sin `template_ref` sus 'inicial' habrían elegido mirando el rendimiento de un texto
nuestro. La vista excluye lo que lo tenga.

🔴 **El hallazgo más caro fue el pedido 6**: la extensión mandaba el pitch en `comments`, y
`sync-toolbar` hace `row.comentarios = nota` en cada push → **borraba la nota del media buyer
en CADA carga**, desde agosto. 77 de 199 filas tenían el pitch en vez de una nota (826 chars
la peor). Limpiadas: quedan 122 notas reales. El pitch no se perdió (`toolbar_sendtrack.pitch`,
4.169/4.169). El CRM además encontró que su import de CSV pisaba igual.

Hecho y verificado CONTRA PRODUCCIÓN (no sólo compilado): cupo compartido `casilla_envios`
(GET/POST probados, fila de prueba borrada) · registro del `inicial` (fila sembrada, bandit
confirmado ciego, prueba borrada) · `contacto_agotado` · `email_secondary` · `_fichaDelCrm`
que distingue "libre" de "no pude consultar" · `mail_ya_enviado` explícito · `CRM_BASE_URL`
único con el replace ANCLADO (el viejo rompía con un host que contuviera "sync-toolbar").

⏳ **Zip v676 armado y verificado en el Escritorio, NO subido**: la v675 está en revisión y
la tienda devuelve `ITEM_NOT_UPDATABLE`. Hay que cancelarla desde el panel. Conviene que
entre: la 675 todavía pisa las notas del MB.
⚠️ La API reporta el `crxVersion` viejo hasta que entra un paquete nuevo, así que ese número
NO sirve para saber si se desbloqueó — lo único que lo dice es reintentar la subida.

**2026-09-03 — el gasto de Anthropic: medirlo antes de recortarlo.**
El salto del 26/08 **no era una regresión**: el worker gastaba 0 porque estaba TRABADO, se
destrabó y empezó a trabajar. Todo el detalle en [[reference_gasto_anthropic]].
⚠️ **Corrección a lo que dije primero**: los ~200/día de las personas eran Apollo y RapidAPI,
no Claude. Las personas usaron Claude **175 veces desde abril** y la última fue el 26/08 (hoy
se mandan plantillas, el botón "generar" casi no se toca). **Casi el 100% es el worker.**
Se armó: puerta única con techo (`claude_daily_cap=700`, no corta el camino de un envío),
registro por motivo **en tokens y no sólo en llamadas**, historia en `toolbar_claude_gasto`,
cruce contra el contador del proxy (`toolbar_gasto_claude_cruce`) para saber si entiendo el
100%, y `vigilarGastoClaude` con umbral derivado de la mediana de cada motivo.
Guía de consulta en `GASTO-CLAUDE.md`.
Dos fugas tapadas: preguntar de a uno (el 97% de lo pagado era el mismo prompt repetido → de a
20, probado 6/6) y re-pagar un dictamen ya emitido (200 de 200 limpios cada corrida).

También: el agente **le escribía en frío a clientes que ya nos pagan** — 22 casos, la lista de
no-recontactar se guardaba a diario y sólo se leía para un informe. Y **backfill de rebotes al
CRM: de 3 a 247 fichas**, sin tocar 65 que ya tenían otro email vivo.
El reparto toolbar ↔ CRM quedó escrito en `PROMPT-DASHBOARD.md` (para la sesión del dashboard).

⏳ **PENDIENTE AHORA MISMO**: el zip **v673** está armado y verificado en el Escritorio pero
**NO se pudo subir**: la tienda devuelve `ITEM_NOT_UPDATABLE` porque **v672 está en revisión**.
El user tiene que **cancelarla desde el panel** (la API no expone ese endpoint) y ahí se sube.

⏳ Pendientes menores: `toolbar_bounced_emails` mezcla rebotes SMTP con veredictos de
MillionVerifier (568 filas sin `fuente`); `toolbar_sendtrack.fu1_*/fu2_*` son columnas muertas
(0 de 4.142) porque los follow-ups los manda el CRM.

**2026-09-02 — jornada larga, dos frentes.**
**(a) El agente estuvo DOS DÍAS EN CERO** por un `TypeError` sin capturar que volteaba el ciclo,
y el informe mandaba a mirar los skips. Todo el detalle y la regla de método —ir a los logs de
Railway, no al mail— en [[reference_alertas_falsos_positivos]]. Se sacaron los frenos por rebote
(regla del user), se redefinió qué es un rebote en 15 idiomas ([[reference_lectura_rebotes]]) y
el kill switch pasó a vigilar Apollo/RapidAPI por separado con techos derivados.
⚠️ De 15 commits, **4 fueron arreglar cosas que rompí yo mismo ese día** metiendo cambios de a
muchos. La disciplina de un cambio → medir → siguiente vale más que la velocidad.

**(b) Cola "Por enviar a Monday"** ([[reference_cola_por_enviar]]) + botón Restablecer Operación
+ escaneo de contacto 60% más rápido. **Publicado en la tienda como v664.**


**Proyecto nuevo abierto el 2026-08-31 — CRM propio.** Reemplazar Monday por el CRM que ya
vive en `adeq-dashboard`. El lado del dashboard está terminado y probado; el lado de la
toolbar está **preparado y APAGADO** en la rama `crm-propio` (`main` intacto en `089a8f9`).
Todo el detalle —la decisión, los 3 pasos para ponerlo live y las 3 trampas del backfill—
en [[project_crm_propio]]. **No poner live sin leer eso**: cargar las fechas de follow-up
vencidas dispararía ~1.400 mails a gente que ya recibió su secuencia entera.


**Estado al 2026-08-31 (Railway en `089a8f9`, 8 commits — TODO worker, la extensión no se tocó).**

**Cambio de operación:** el agente queda con **dos buzones** (`sales@`, `dhorovitz@`) — se sacó
a Maxi del envío diario — y corre **solo de lunes a viernes**. Objetivo 40/día. Verificado
empíricamente que el finde ya no enviaba: cero envíos sábado y domingo en 30 días.

**El hallazgo grande: el scan de rebotes no veía los de Microsoft 365**, que es medio internet
publisher. Query vieja 2 vs nueva 8 en la misma ventana. 492 de 523 rebotes de 90 días tenían
`tipo` NULL. Todo en [[reference_lectura_rebotes]] — incluida la respuesta a *"¿dónde dejo los
rechazados?"* (donde caigan: lee Inbox + Spam + Papelera, 7 días).

**El otro grande: la lista negra del ranking mataba `redazione@`/`redaktion@`/`redaction@`/
`editorial@` mientras dejaba pasar `redaccion@`.** 163 leads/7d sin email por ranking. Ver
[[reference_criterio_email]].

**Falsos positivos 8 y 9** en [[reference_alertas_falsos_positivos]]: job a pedido vigilado con
horario, y —el estructural— el monitor midiendo horas de reloj contra un worker que duerme, que
iba a gritar todos los lunes para siempre. Al destaparlo aparecieron 3 nombres de job
corrompidos que escribían el OK bajo un nombre roto desde el 28/08.

**Bug latente cazado de casualidad:** `_madridMidnightUtcISO()` dependía de la zona horaria de
la máquina (`new Date("...T00:00:00")` sin Z se interpreta en hora local). Correcto en UTC —por
eso Railway nunca falló— pero de él dependen **el cupo diario, el kill switch y el informe**: el
día que Railway defina `TZ` se rompen los tres a la vez y en silencio. Ahora prueba los dos
offsets de Madrid y VERIFICA cuál cae a las 00:00 allá. Probado en 5 zonas.

**El informe diario cambió de forma:** titular arriba, rebotes por buzón adentro, y las tres
secciones de 30 días pasan a los lunes. Ver [[reference_informe_salud]] — y ojo que son DOS
mails distintos (`parteDelDia` vs `enviarResumenSalud`).

**Números al cierre:** pool 1.956 pendientes (1.163 con email, 793 sin) · 930 altas en 7 días ·
rebote sales@ 6,5% / mgargiulo 3,1% / dhorovitz 1,0%.

**Lo que sigue abierto:** de los leads sin email, **321 son "no se pudo leer el sitio" contra
163 "rechazados por ranking"** — el primero es ahora el doble de grande y es el próximo lugar
donde buscar. Argentina 0/105 respuestas sigue siendo decisión comercial del user. Revisar
`toolbar_template_perf` cerca del 17/09 (`db_22` en es va 0/25, primer candidato a reescribir).

---
[Histórico anterior]


## 🌙 CIERRE REAL DE LA JORNADA 2026-08-25 (19:20 UTC)

**Estado medido, no supuesto:** 60/60 envíos (los TRES buzones en 20 — a la mañana Agus estaba
en 8 y Diego en 6) · cola procesando ~44 cada 45 min · 767 contactables · 0 fallos del agente ·
repo limpio · **zip v642** (verificado byte a byte contra el código).

**~33 commits en el día.** Auditoría de 24 agentes (91 hallazgos, 6 refutados, 85 aplicados) +
todo lo que pidió el user después.

### Lo que se construyó DESPUÉS de la auditoría
- **Freno por stock**: el agente baja el ritmo si tiene menos de 10 días de envíos por delante
  (20→15→10→5 por MB). Ver [[reference_4_metricas]].
- **`auditarEmailsDelPool`** (2×/semana): limpia emails malos, reordena, y a los que solo
  tienen genérico les busca uno mejor gratis. Ver [[reference_criterio_email]].
- **El criterio de los MB aplicado en la ENTRADA** (processCsvItem + autopilot), no solo al
  enviar. Antes 25 leads entraban con un `soporte@` y contaban como contactables.
- **Aprendizaje de rebotes**: se clasifica el porqué (5 tipos), se congela la fuente en el
  momento (388 de 443 la habían perdido), y si un dominio ya rechazó 2 direcciones se deja de
  probar ahí. 73 leads del pool apuntaban a un dominio así.
- **1 solo mail de alertas por día** + resumen diario (era 1 por tipo cada 60 min = avalancha).
- **80 emails del pool rotos** por escapes del scrape (`u003e`, barras, "email:") recuperados —
  varios eran los comerciales que buscamos.

### ⚠️ Cabos sueltos (ninguno sangrando)
1. **`linter:mayusculas` sin explicación.** Frenó 147 envíos reales en dos incidentes leyendo
   "ADEQ MEDIA"/"BUENOS AIRES, ARGENTINA" de la firma. Verifiqué que NINGUNA de las 12
   plantillas la contiene, que los nombres de firma son Agus/Maxi/Diego, y que la llamada pasa
   la plantilla cruda — nunca encontré cómo la firma llega al cuerpo. **Pasó a AVISO** (ya no
   bloquea, 40 min verificados sin bloqueos) y ahora adjunta el CONTEXTO alrededor de las
   palabras. Cuando vuelva a aparecer en el resumen, ahí está la pista.
2. **Autopilot 0%**: de 449 procesados, 312 congelados (RapidAPI no puede medirlos) y 97 sin
   ads.txt. Trae dominios que no son webs medibles. Las otras 5 fuentes alimentan bien.
3. **El user tiene que cargar el zip v642** — sin eso la PARTE 2 del parte (trabajo manual por
   MB) sale vacía, porque el registro del envío manual vive en la extensión.

### Qué mirar en el parte de mañana
Es el primero con TODO medido bien. Si las altas siguen bajas con la cola sana, el problema
pasa a ser las FUENTES y no el transporte.

## 🔬 2026-08-25 (noche) — AUDITORÍA INTEGRAL CON 24 AGENTES

El user pidió una revisión código-por-código: "no puede ser que tras meses la toolbar siga con
errores en las tareas del agente". Se lanzó un workflow de **12 áreas × 2 agentes** (uno audita
contra la BASE REAL, otro intenta refutarlo). 91 hallazgos, 85 sobrevivieron a la verificación.
Aplicados en 15 commits (fb31ba7 → 79aef15).

### Los que paraban el negocio
1. **`pub is not defined`** — regresión MÍA del 24/08: moví `classifyPublisher` a un bloque y
   dejé el `const` adentro, pero se usa 200 líneas después. **238 dominios muertos y CERO altas
   a Prospects en 51 horas.**
2. **El autopilot descartaba el 100% de lo que descubría, 4 meses.** `seenOrgs` precargado con
   la organización de TODOS los dominios de la cola → cada uno se descartaba a sí mismo.
3. **`_verifyEmailMV(token, cfg, email)` llamada con 2 argumentos, en DOS lugares.** Cero
   re-engagements desde el 12/08 y cero segundos emails. Y el `continue` dejaba la reserva
   colgada, que **le comía el cupo de primer contacto** → por eso sales@ y dhorovitz@ se
   clavaban en 6-8 mientras mgargiulo@ llegaba a 20.
4. **El linter frenaba el mail y el lead se marcaba `sent` igual** (`sendGmailServer` devolvía
   `{ok:false}` y nadie lo miraba). 71 leads quemados el 18-19/08 sin recibir nada.
5. **Métricas 4 y 5 filtraban por `updated_at`, que NO existe** en review_queue → PostgREST 400
   → el contador devolvía 0. Dos de las cinco métricas en cero desde siempre.
6. **Un teléfono mal formateado costaba el lead entero.** 143 en 14 días recibieron el pitch y
   quedaron fuera del CRM. Ahora se sanea + se reintenta sin las columnas que Monday rechaza.

### Patrones que se repitieron (el de siempre, 8 veces más)
- guards que fallaban ABIERTO: el dedup de dominios, el guard de "ya lo contactamos hace 30
  días", el re-chequeo de ads.txt, el des-congelador, el `_contar` del parte diario.
- topes que se re-armaban en cada reinicio (Serper: 3 de 4 vías).
- turnos que se consumían sin hacer el trabajo (caza de emails: NUNCA corrió; autopilot).
- **la lista negra de rebotes tenía 606 de 883 direcciones a las que nunca escribimos** (el
  escáner blacklisteaba cualquier dirección del cuerpo del rebote). 392 liberadas.
- **el Email Futuro no mandó UN SOLO mail en su vida**: 125 filas en 'failed' con motivo
  "gmail send failed: unknown", ninguna reintentada.

### Config corregida
`target_category='food'` (el autopilot descartaba todo lo que no fuera comida),
`polish_use_apollo=false` con 349 leads sin email, `monday_rescue_enabled=false`.
**Apollo pedía cargos de REDACCIÓN que su propio ranking rechaza** → ahora pide publicidad,
comercial, marketing y dueños (regla del user, textual).

#### 📌 CIERRE DE JORNADA 2026-08-25 — QUÉ QUEDÓ FUNCIONANDO

**Estado al cerrar (18:23 UTC), medido, no supuesto:**
- Cola procesando ~120 dominios/hora · 0 errores de código desde las 15:47
- **45 envíos del agente hoy** (ayer 33): Max 16 · **Agus 12 (venía de 8)** · **Diego 11 (venía de 6)**
- Pool: 834 contactables · 353 sin email
- Extensión en **zip v642** — el user tiene que cargarlo para que se registren los envíos
  manuales, que es lo que alimenta la PARTE 2 del parte diario.

**26 commits.** Auditoría de 24 agentes: 91 hallazgos, 6 refutados, 85 aplicados.

### Lo que hay que mirar en el parte de mañana (21h)
1. **Las altas del día.** Hoy 21, pero el número está viciado: la cola estuvo caída medio día
   por el `pub is not defined`. Si mañana con la cola sana siguen en ese orden, **el problema
   pasa a ser de las FUENTES y no del transporte** → mirar AutoGoogle y autopilot, que hoy no
   aportaron nada.
2. **Si los tres buzones llegan a 20.** Ya se despegaron; el día completo con todo arreglado
   es mañana.
3. La PARTE 2 (trabajo manual por MB) sale completa recién cuando los MB tengan el v642.

### Decisiones del user en esta jornada
- **NO hay columna dedicada de origen en Monday.** Se creó, se probó y se borró. El estado
  Agente/Manual va en Comentarios. Ver [[feedback_no_tocar_monday]].
- Apollo: publicidad, dueños, marketing. **NO redacción** (textual del user).

## ✅ LAS 39 MEJORAS MEDIAS Y BAJAS — APLICADAS (commits a511f87 → f86b258)
Además de los críticos y altos, se cerraron TODAS las medias y bajas. Las que más valen:
- **Los carriles estaban al revés del rendimiento**: `similar` (76% con email, la que mejor
  convierte) tenía 150 y `sellers` (pasa el 8%) tenía 250. Invertidos.
- **El linter juzgaba los LINKS sobre el mail completo, firma incluida** — misma trampa que las
  mayúsculas de "ADEQ MEDIA". Las reglas de CONTENIDO van sobre la plantilla; las estructurales
  (invisibles, homóglifos, largo de línea) sobre el mail entero.
- **Apollo sin cuota devolvía lo mismo que un dominio sin contactos**: nada, sin log.
- **El reciclado de Monday comparaba el techo contra sí mismo** → el parte siempre decía "entró
  todo lo que se podía" con 5.790 elegibles y techo 400. Y no miraba Prospects: 88 duplicados.
- **La métrica 3 no se calculaba**: el parte mostraba el stock del pool, no el corte del DÍA.
- **SSRF**: `_feederPullSellers` bajaba con fetch pelado URLs que salen de ads.txt de terceros,
  y sin techo de tamaño (vienen archivos de 18.000 publishers). + guardia + 8 MB.
- **218 dominios anglo re-pagaban su hit de RapidAPI cada día** para redescubrir el mismo país.
- **El "gasto inteligente" de AutoGoogle medía frescos y no encolados** → durante el apagón le
  SUBIÓ el presupuesto de Serper mientras no traía nada (llegó a 3,58, el tope). Sembrado 0,35.
- **Majestic cacheaba el fallo de descarga como pool VACÍO** (`[]` con caché `!== null`).
- **`agent_active_hours_start` vs `active_hours_start`**: el watchdog leía la clave equivocada
  (verificado en la base) y usaba el default.
- **El parte se marcaba enviado ANTES de armarse** → si fallaba, ese día no había parte.
- **Pesos NEGATIVOS** en la ponderación de plantillas con 7+ borradores por idioma.
- **El tope diario de Serper se vigilaba pero ninguna línea lo aplicaba.**
- Tres jobs pingueaban sin cadencia → el vigilante de latidos no los miraba.

## 🔒 EL APAGÓN DE LA COLA — RESUELTO (lo destrabaron los logs de Railway)
La cola dejó de procesar a las 15:47 y no volvía: 200 pendientes, 0 en proceso, **cero errores**.
Desde la base era indiagnosticable. Los logs lo mostraron en una línea:

    🌱 sellers i-mobile.co.jp: 18275 pubs → 17993 frescos → 0 insertados
    ⏸️ auto_feeder_sellers SKIP inject: carril lleno (250/250)

**`_injectIntoCsvQueue` mira el cupo del carril al FINAL**, después de todo el trabajo. Con el
carril lleno, el feeder igual bajaba cientos de sellers.json —uno con 18.275 publishers— para
tirar todo. Y es el job **30 de una cadena de 37**: se comía la vida del worker y la COLA, que va
ÚLTIMA, no corría nunca. En 13 min de logs no hay una sola línea "CSV queue start".

**Bloqueo circular:** la cola no drena el carril → carril lleno → sellers quema el worker → la
cola no corre. Roto por los dos lados: sellers mira el carril ANTES de bajar nada, y el bloque de
mantenimiento tiene presupuesto de 3 min y le cede el turno a la cola.

⚠️ **Los SIGTERM cada 2,5 min NO eran el tope de memoria: eran mis propios deploys** (17 ese día).
Al diagnosticar, descontar los reinicios propios antes de culpar a la memoria.

**Lección para la próxima:** cuando algo se frena sin dejar error, PEDIR LOS LOGS DE RAILWAY antes
de gastar una hora infiriendo desde la base. Y quedaron dos latidos nuevos que ahora lo dicen
solos: `loop_reparto` (¿el bucle llega al reparto de trabajo?) y `csv_queue` (¿arrancó y con qué
topes?). Consultarlos PRIMERO:
`SELECT job, last_status, last_detail FROM toolbar_health WHERE job IN ('loop_reparto','csv_queue');`

Lo que arrancó tras los fixes: 259 leads recuperados al CRM, 10 re_sent, 4 future_sent (los tres
estaban en CERO), y la cola volvió a drenar.

## 🛑 2026-08-25 (tarde) — EL AGENTE SE FRENABA SOLO (commits 4e0227c, 7a653d6)

Revisión de las 5 métricas ([[reference_4_metricas]]). **El 20 y el 21 de agosto el agente mandó
CERO mails los dos días enteros**, y el patrón del resto: Max llega a 19-20, Agus y Diego se
clavan en 6-8.

**Causa: el patrón de siempre otra vez.** Antes de mandar se crea una fila `reserved`. El worker
reinicia cada ~7 min, quedan reservas colgadas, y el cleanup las marcaba **`failed`**. Medido:
468 "fallos" sobre **26 dominios distintos** —el mismo lead reservado 4 veces— y todas con otra
fila del mismo día, o sea el lead SÍ se había resuelto. Ninguna en `sendtrack`: no eran envíos
fallidos, era contabilidad colgada. `checkAgentKillSwitch` veía 100% de fallos y pausaba 1 hora.
Los números cierran: Diego 6 enviados + 14 huérfanas = Agus 8 + 12 = **20, el cupo exacto.**
→ pasan a `reserve_expired`; el freno las ignora y exige 5 acciones REALES; **y la pausa deja de
ser global** (escribía `agent_paused_until`, que frena a los TRES buzones).

**Segunda regla de marca duplicada.** Había una copia propia en el momento del ENVÍO, congelada
desde julio. Al aflojar `_brandMatches` esa mañana, los candidatos pasaban la primera puerta y
esta segunda los abortaba **marcando el lead `rejected` PARA SIEMPRE**. El arreglo, sin esto,
salía peor que el bug. Ahora hay UNA sola implementación.

**Métrica 5 (rescate de emails).** 349 mudos, 1-4 rescates/día. El pulido enriquecía 20/día pero
19 ya tenían email: al terminar una pasada apagaba `polish_only_missing` y volvía a repasar el
pool ENTERO. Ahora al terminar re-apunta SOLO a los mudos. **Apollo estaba en 235/2.500 (9,4%) y
el pulido lo tenía APAGADO** con 349 leads sin email → encendido (topes 2.250/mes, 250/día).

**El reintento por rebote NUNCA salía**: sin pitch guardado caía en un texto genérico escrito a
mano de exactamente 14 palabras, y el linter bloquea bajo 15. Ahora usa plantilla real del user.

**Detector nuevo:** `vigilarAgenteFrenado` — dos días de silencio comercial no pueden ser un
renglón del parte de las 21h. A las 15h Madrid, si el total va en cero y hay pool, alerta grave.

### Medido y NO tocado (a propósito)
- **1.819 leads congelados**, 1.698 por `no_traffic_data_after_3_attempts`. Revisé una muestra:
  son basura real (`zee7suchs.top`, `ads.colorfuljigsawstudio.org`, `approidzone.com`) que
  SimilarWeb no mide porque no son webs. **El congelamiento está bien.** Un filtro por patrón
  solo agarraría el 16%, no vale la pena.
- **El pool se repone al ritmo justo**: 401 altas en 7 días = 57/día contra un objetivo de 60
  envíos/día. Si los envíos se recuperan a 60, el pool empieza a drenarse.

## 🕳️ 2026-08-25 — AUTOGOOGLE PAGABA SERPER 7 DÍAS PARA NO ENCOLAR NADA (commits 60abd2c → dcfcf4e)

**Entre el 18 y el 25 de agosto AutoGoogle no metió UNA sola web en la cola**, mientras seguía
buscando y escribiendo atribución todos los días (532 dominios frescos el día 25). Desde afuera
parecía viva. Dos causas encadenadas:

1. **`skipped` estaba en `_ESTADOS_REINTENTABLES`.** Esa lista era para dominios caídos por un
   timeout o un 500. Pero medido sobre la cola real: **14.136 de los 14.167 `skipped` son
   VEREDICTOS** (gobierno, e-commerce, GEO, dominio muerto) y solo 31 fueron falla nuestra.
   O sea que el 99,8% volvía a darse por fresco cada día. Es **el patrón de siempre AL REVÉS:
   un "no" tratado como "no sé"** (van 9 casos del patrón, contando las dos direcciones).
2. **El POST a la cola iba sin `on_conflict`** y con `ignore-duplicates`, que PostgREST no puede
   aplicar sin saber contra qué constraint. Con `UNIQUE(domain)`, **un solo dominio repetido
   devolvía 409 y `if (!res.ok) return _empty()` tiraba los otros 179 sin una línea de log.**

Y al arreglar (1) apareció el error espejo: dejar `skipped` afuera **enterraba 10.127 dominios
cuyo rechazo era CONDICIONAL** (9.414 por "tráfico bajo" — ¡y nuestro umbral se mueve solo entre
350K y 800K! — 713 por GEO, 31 por cuota). → `revisarDescartesCondicionales`: semanal, 500 por
pasada, +45 días, y solo los que estaban a menos de la mitad del umbral de hoy (531 valen otra
mirada, 3.897 quedan afuera con razón).

**Detector nuevo:** `vigilarEmbudoDeDescubrimiento`. El vigilante viejo mira el FINAL de la
cadena (¿llegó un lead a Prospects?) y por eso no vio romperse el MEDIO. Este compara
descubrimiento contra encolado. + alerta `cola_rechaza_lote` cuando la cola rechaza un lote.

## 📬 2026-08-25 — EL REGISTRO DE ENVÍOS MANUALES NUNCA FUNCIONÓ (commit 37213e0, zip v639)

⚠️ **El ENVÍO siempre funcionó. Lo que no funcionaba era el REGISTRO de ese envío.** El mail sale
directo de la extensión a la API de Gmail (`modules/gmail.js`) y el código no frena si el
tracking falla — un `console.warn` y sigue. Por eso nadie se enteró en meses.

Pedido del user: que el parte diario diga también qué hizo cada MB **a mano**. Al ir a buscar el
dato apareció que **no existía**: `toolbar_agent_actions` tenía UNA sola policy, la de SELECT.
Ninguna de INSERT. `createManualSendTracking` venía siendo rechazada por RLS, el popup se comía
el error con un `console.warn` y mandaba el mail igual. **Y eso rompía el píxel de open → el
"Email Futuro" nunca se disparó en envíos manuales.** Arreglado con dos policies estrechas
(cada quien solo escribe a su propio nombre), ya aplicadas. Ver [[reference_parte_diario]].

El parte pasa a HTML (`htmlPropio`, solo correo interno) con dos partes: el agente y cada MB.

## 🔌 2026-08-24 (cierre) — EL PROXY BLOQUEABA A LOS MB HACE 3 SEMANAS (commit cb3d6c4)

**El panel decía "SimilarWeb ❌ 403 · 0 quota left" y "Apollo ❌ 401". Ninguno era culpa de
las APIs.**

1. **El 403 era MÍO.** La lista blanca de paths que agregué el 04/08 en la auditoría de
   seguridad (`8d64aa9`) valida `path` contra regex que terminan en `$`. Pero la extensión
   SIEMPRE mandó la query pegada: `/all-insights?domain=x`. Nunca matcheó. **Cada llamada de
   un MB a SimilarWeb devolvió 403 desde el 4 de agosto.** El worker no pasa por el proxy
   (tiene su propia clave y llama directo), así que el agente siguió andando mientras el
   trabajo manual estaba muerto — por eso nadie lo notó. Y el propio proxy lo registraba en
   `toolbar_security_events` todos los días. Arreglado: el proxy separa la query del path
   antes de validar. Deployado como api-proxy v20.
2. **El "0 quota left" tampoco era RapidAPI**: lo calcula el proxy como su propio cap diario
   (400/usuario) menos lo usado. El plan real estaba al 7,77% de 40.000. Texto corregido.
3. **El 401 de Apollo eran DOS CLAVES DISTINTAS**: `toolbar_config.apollo_api_key` (worker) y
   el secret `APOLLO_API_KEY` (proxy). Se sincronizaron con una key nueva.

**Ciclo de RapidAPI corregido: ancla 7 → 18.** El plan cambió a `custom-40k-hard` que arrancó
el 18/08 y renueva el 17/09. Contador sembrado en 3.108. Y los items que fallan por cuota
NUESTRA ya no queman reintentos: van a `next_day`.

**APOLLO — se usaba el 5,5% de un plan pago.** 137 llamadas de 2.500 en 12 días, y los
créditos no se acumulan. Causa: Apollo era el último recurso, solo se llamaba si el rastreo
gratis no encontraba NADA — y como el gratis quedó muy bueno, casi siempre encuentra un
`info@` y Apollo no entraba. Ahora también se llama cuando el lead tiene **solo genéricos**:
351 sin email + 127 con solo genéricos = 478 donde aporta. Es la única vía que da una PERSONA
con nombre y cargo. Ver [[feedback_cost_awareness]], que quedó actualizado.

**Detector nuevo:** `vigilarAprovechamientoDeApollo` avisa si la proyección del ciclo queda
bajo el 50% del plan. **Es el primero que vigila que NO desperdiciemos** — todos los demás
vigilan que no nos pasemos.

⚠️ **Del user:** rotar la API key de Apollo (`IVpaKceM...`) — quedó escrita en el historial
de la conversación del 24/08.



## 🔧 2026-08-24 — LA ATRIBUCIÓN ERA FALSA, Y CON ELLA TODO EL ANÁLISIS (commit 87d2d52)

**EL HALLAZGO DEL DÍA:** `getNextCsvItem` pedía `select=id,domain,uploaded_by,error_message`
— **sin `source`**. Así que `item.source` llegaba undefined SIEMPRE, el switch que traduce las
etiquetas del feeder caía al `default`, y todo quedaba etiquetado **"autopilot"**. Solo Monday
se salvaba porque un bloque más abajo lo fuerza. Durante meses las métricas por fuente
mostraron exactamente dos fuentes y todo el análisis de "qué motor rinde" fue sobre datos falsos.

**El cuadro REAL, tras recuperar la atribución** (`sql/2026-08-24_recuperar_atribucion.sql`):
monday_refresh 2222 · **autogoogle 1499** · sellers_json 976 · majestic 538 · adstxt 483 ·
similar 146 · **autopilot 180, último lead 27 de JULIO**.
→ AutoGoogle NO era el motor muerto: es el 2º más productivo. El autopilot sí llevaba un mes
muerto, por DOS frenos: `if (csvProcessed > 0) continue` en el loop (la cola SIEMPRE tiene
trabajo) y el gate de backlog >250 en su slot. Ninguno de los dos tenía sentido: el autopilot
escribe DIRECTO a Prospects, no toca la cola.

**Otros 15 bugs de las fuentes** (auditoría de agente): la regla de la "puerta grande"
(ads.txt + 400k gana a cualquier veto de rubro) **nunca se aplicó a un lead** porque las dos
llamadas de entrada a `classifyPublisher` no pasaban el 5º argumento · `bypassGeoPrefs`
apagaba el retry de tráfico y por eso Monday es la única fuente con 0 frozen y 0 next_day ·
`next_day` ocupaba carril y estrangulaba a similar-expansion · AutoGoogle usaba 3 de sus 11
plantillas por un índice que dividía por la mezcla de IDIOMAS, y desde el 19/08 la única que
usaba devolvía ad-networks · `score: 0` fijo rompía la expansión por similares · techo de 40M
del autopilot medía visitas y no pageviews · saturación GEO al 25% peleaba con LATAM-first.

**Tres bugs MÍOS de la semana pasada que frenaron producción:** el freno por rebotes pausó el
envío con 3 de 80 (ahora Wilson + muestra 150) · el linter bloqueó **79 envíos** porque la
firma dice "ADEQ MEDIA" en mayúsculas (el estilo ahora se juzga solo sobre la PLANTILLA, no
sobre la firma) · el chequeo de DNS reportó que faltaban SPF/DKIM/DMARC sobre un dominio que
los tiene (le pedía `.data` a un string).

**Los 327 `all_candidates_undeliverable`:** NO era MillionVerifier (6 usos de 200). Es
`decidirVerificacionMV` descartando cuando el proveedor acepta todo (M365/gateways, ~41%) Y el
email es hipótesis de patrón. Decisión correcta, pero no se registraba el motivo y el lead
quedaba atascado para siempre. Ahora se guardan los motivos y el lead va a buscar un email REAL.

**DETECTORES NUEVOS:** `vigilarFuentesDeDescubrimiento` mira fuente por fuente (el vigilante
contaba el TOTAL, por eso AutoGoogle mudo 4 días y el autopilot un mes fueron invisibles) ·
`rindenPoco` pasa de warning a error, o sea al mail.

**Pendiente de revisar:** los 269 congelados (no son sitios chicos: son sitios que RapidAPI no
pudo medir, y a los 3 ciclos van a blocklist permanente — destruye al publisher regional chico
de LATAM, que es el objetivo) · Majestic quema posiciones del pool sin mirarlas · sellers y
adstxt trabajan con el carril lleno · dedupe de subdominios sin anclar (`as.com` matchea
`casas.com`) · `discovered_sellers_networks` se trunca de 500 a 200.

**Del user:** el DMARC está en `p=none` y **le falta `rua=`** (tiene solo `ruf`, que casi nadie
manda). Sin reportes agregados no se puede pasar a quarantine sin riesgo. Paso 1 sin riesgo:
agregar `rua` manteniendo `p=none`. Ver [[reference_entregabilidad]].



## 🎛️ 2026-08-12 — ALERTAS ARREGLADAS + AUTOAJUSTE (commit 6011b15)

**Las alertas del vigilante que armé el 11 eran casi todas FALSAS.** Tres errores míos:
comparaba contra el reloj de pared (el worker duerme 23-9 y findes, así que cada mañana
todo parecía caído: "hace 914 min" a las 4 AM); un job apagado por flag figuraba "atrasado"
para siempre (`similar_expansion` arranca en false); y mandaba un mail por alerta.

**Ahora:** el atraso se mide en minutos de ventana ACTIVA · estado `last_status='off'` para
lo apagado a propósito · **UN mail cada 72h** con todo agrupado y deduplicado
(`enviarResumenSalud`), dividido en "se arregló solo" / "necesita una mano" / "para mirar".

**AUTOCURACIÓN + AUTOAJUSTE** (pedido: "que seas autónomo"). Le dije al user que YO no puedo
serlo —solo existo en sesión y no puedo consultar la base—; el que corre 24/7 es el worker.
- `_CURAS_CONOCIDAS`: re-enciende barridos apagados, prende similar_expansion, limpia
  marcadores de slot viejos. Lo curado NO llega como problema.
- `autoAjustarSegunMetricas`: con ≥3 días de historia, sube/baja `feeder_daily_target`
  según los días de autonomía del pool (<5 sube, >20 baja) y sube el piso de tráfico si el
  pool se llena de leads mudos. **Límites duros** (feeder 80-400, tráfico 350K-800K).
  Se apaga con `autoajuste_enabled=false`.
- **Lo que a propósito NO se autoajusta:** envíos bajos CON pool de sobra → se reporta, no
  se toca. Tocar el enviador a ciegas puede empeorarlo.

**Métricas con historia:** `toolbar_metricas_diarias` (snapshot cada noche a las 21h) +
vista `toolbar_metricas_tendencia`. Ver [[reference_4_metricas]] — disparador "SQL DE ANALISIS".

**Pendiente del user:** deploy de Railway de `6011b15`. La base ya está toda corrida.
Primer día limpio de medición: 2026-08-13.



## ✅ CIERRE DEL 2026-08-11 — QUÉ QUEDÓ Y QUÉ FALTA (commit 086a8db)

**Del lado del user ya está TODO hecho salvo una cosa: el DEPLOY DE RAILWAY de `086a8db`.**
El SQL está corrido y **el zip NO hace falta**: hoy no se tocó un solo archivo de la
extensión (verificado: los 6 archivos del zip v636 del Escritorio son idénticos al repo).

### Los tres arreglos que revierten trabajo mío del mismo día
1. **Los borradores volvieron a los del user.** Reescribí los 15 sin aprobación y encima
   le llegaron a ole.com.ar con los placeholders CRUDOS. Ver [[feedback_borradores_del_user]].
   El copy nuevo se ELIMINÓ, sin respaldo, a pedido del user.
2. **El ads.txt decide, no el rubro** (regla nueva del user): "toda url con txt y +400k
   sirve, no importa si es un medio de noticias, una web de loterías". Los vetos por rubro
   ya no matan si hay ads.txt confirmado + tráfico. Esto DESHACE el filtro de casas de
   apuestas que había agregado horas antes. Siguen vetando gobierno/universidad/muerto/
   acortador/CDN y el techo de 40M.
3. **Mi archivo SQL consolidado perdió la línea del DELETE** (la corté con un `sed`) y dejó
   30 templates conviviendo. Lección: no armar SQL con `sed` sobre otro archivo.

### Dos diagnósticos míos que estaban MAL (corregidos con datos del user)
- **El slot de las 20 nunca estuvo muerto**: `agent_active_hours_end`=23. Lo único que
  frenaba los envíos era `per_cycle_limit` en 2. Los 16 de ese día eran 4 slots corridos y
  el 5º todavía por venir.
- **`next_day` daba 0**: el agujero negro era real como mecanismo pero NO era la causa.

### Números medidos ese día
1.938 congelados (1.735 ya vencidos, el más viejo del 27/07) · 548 listos para enviar ·
autopilot es la fuente dominante (3.132 leads) · envíos 8→10→16 por MB en la semana.

## 🔎 2026-08-11 — AUTOGOOGLE REACONDICIONADO (commits 9e13348, 07e9287, 81605f7)

Auditoría dedicada con la premisa del user: **"debe simular ser un media buyer en
Google"**. Buscaba TEMAS y esperaba que aparecieran medios.

**Clave para entender el motor: el cuello de botella NO es la cuota de Serper (2.500/mes,
nunca se acerca) sino el CARRIL de 180.** La pregunta correcta no es cuántos dominios
traigo, es CUÁLES 180. Toda mejora tiene que ser de precisión, no de volumen.

- **Se tiraba todo el contexto**: de la respuesta de Serper se leía solo el hostname.
  title/snippet/date/position/sitelinks vienen gratis en el mismo crédito. Ahora
  descartan tiendas antes de gastar RapidAPI y ordenan por probabilidad de ser publisher.
  `relatedSearches`/`peopleAlsoAsk` = expansión de keywords validada por Google, gratis,
  mientras se le pagaba a Haiku para inventar frases.
- **Era el ÚNICO feeder sin los filtros gratis** (isDomainAllowed, isCorporatePattern,
  BRAND_BLOCKLIST, classifyByUrlOnly). Wikipedia/amazon/.gob se comían carril + RapidAPI.
  OJO: `classifyByUrlOnly(d,"",0)` es seguro — el piso de tráfico está guardado con `pv > 0`.
- **Guardián de slot solo en memoria** con worker que reinicia cada 7 min → el mismo slot
  se re-disparaba hasta 8×/hora. Ahora persistido + recupera el pendiente más viejo.
- **Sin paginación**: cada frase devolvía SIEMPRE los mismos 20, y como el ranking repite
  las que más rindieron, **el motor leía su propia ceguera como saturación del mercado**.
  Otro caso del patrón.
- **`gl` sorteado sin mirar el idioma** de la frase. El idioma ahora NO se adivina: se
  anota al armar el pool (`_IDIOMA_DE_FRASE`), porque ahí se sabe con certeza — la
  heurística falla con palabras sin marcas ("calciomercato").
- **Idiomas**: 0% inglés, ~65% español, 80%+ latinas. Antes 716 frases inglesas (9,5%)
  buscadas con `gl` de países NO anglo.
- **GEO**: 41 países (era 19). **Asia y África no tenían NI UN país** pese a estar en el
  foco. Fuera Norteamérica y Oceanía. México ADENTRO (es el mercado hispano principal).
- **Reparto del slot**: 35% huella comercial (inurl:ads.txt, media kits, tarifas) · 20%
  ciudades secundarias (105 ciudades de 17 países — "noticias Argentina" devuelve los 4
  gigantes que rozan el techo de 40M; el long tail aparece con Neuquén o Arequipa) · 20%
  endpoint /news · resto temas como exploración.
- **Similares**: ahora se disparan sobre los hallazgos FRESCOS y bien puntuados ("el MB
  googlea y cuando encuentra uno bueno busca sus pares"). Antes el cursor ascendente hacía
  que un hallazgo de hoy esperara 1.600 leads = nunca.
- **Monday finalizados**: job diario que barre el 100% del board. Ver commit affa4e0.

**Pendiente del informe, no implementado** (por si vale la pena después): paginación con
columna propia en keyword_yield, `inurl:sellers.json` para descubrir redes nuevas, pares
vía SSP regional del ads.txt de un validado, media kits en PDF (traen el email comercial),
páginas-hub de directorios de medios, TTL de atribución 10d→45d y ranking por tasa
suavizada en vez de acumulado.

## 🔬 2026-08-11 (cierre) — LAS 39 "RESPUESTAS" ERAN ACUSES DE TICKET (commit 8e3e691)

Al mirar una por una las 39 que el tracker daba por reales: casi todas de ajuda@,
apoyo@, soporte@, support@, service@, abonnements@, bok@, csmtix@. **Acuses automáticos
de ticket**, no respuestas comerciales. No traen Auto-Submitted ni asunto de OOO, así que
pasaban como reales. **El sistema se auto-reportaba éxito y el user tenía razón: cero
negociaciones.** Yo predije que darían 0 y di por buena la cifra al verla — el error fue
no mirar las direcciones antes de opinar.

- `rankEmail`: mesas de ayuda de +8 a **-20** (descartadas). Separadas de rrhh/legal/abuse,
  que siguen en +8. Escribirle al buzón de reclamos no vende y mete el pitch en la cola de
  atención al cliente.
- `scanRealResponsesForUser`: detecta acuses por asunto (ticket/caso/folio/protocolo/#1234/
  "hemos recibido su solicitud") y por remitente. **Faltaba pedir la cabecera From.**
- **Se estaba prospectando a no-publishers**: standvirtual, quoka (clasificados), otto.de,
  coolshop (ecommerce), plus.pl (telco), newhome.ch, 21cineplex y **inkabet.pe, casa de
  apuestas** (categoría bloqueada). La regla pedía "betting" entero y las marcas en -bet se
  escapaban. Agregado con excepciones (beteve.cat es un MEDIO catalán) + TLD .bet +
  comparadores + clasificados. Probado: 0 falsos positivos sobre 18 publishers reales.

**Números reales medidos ese día:** 1.938 leads congelados (1.735 ya vencidos, el más viejo
del 27/07) que el descongelador roto no liberaba · 548 listos para enviar · `next_day` en 0
(mi hipótesis del agujero negro NO era la causa hoy) · `agent_active_hours_end`=23, o sea
**el slot de las 20 SÍ podía disparar y mi diagnóstico del techo 16 estaba mal**: lo que
frenaba era solo `per_cycle_limit` en 2.

⚠️ El archivo consolidado que le pasé perdió la línea del DELETE (la corté con un `sed`) →
quedaron 30 templates conviviendo. Se resolvió archivando los viejos a
`user_email='_viejo_2026_08_11_'` (reversible).

## 🚀 2026-08-11 — LA MEGA-EVOLUCIÓN (commit f0a9e8d)

Auditoría de 4 frentes (envíos / feeders / filtros / respuestas) y reescritura grande.
**Solo worker + templates. La extensión NO cambió → NO hay zip nuevo.**

### Lo que tiene que hacer el user
1. **Deploy de Railway** del commit `f0a9e8d`.
2. **`sql/2026-08-11_salud_y_alertas.sql`** — crea `toolbar_health`, la vista
   `toolbar_health_check` y las 3 columnas de intentos de email.
3. **`sql/2026-08-11_copy_nuevo.sql`** — ⚠️ CRÍTICO: los drafts de `toolbar_pitch_drafts`
   PISAN a los templates del código. Sin este SQL el agente sigue mandando el copy VIEJO
   aunque el código esté deployado.
4. **`sql/2026-08-11_diagnostico_base.sql`** bloque D — confirmar si `agent_per_cycle_limit`
   había quedado en 2 (explicaría los 8/día).

### El hallazgo de fondo
**Los 5 problemas grandes eran SILENCIOSOS: ninguno tiraba excepción.** Por eso meses de
cambios no se sentían — muchas veces ni llegaban a correr. De acá sale la nueva regla de
oro: [[feedback_alertas_automaticas]].

- 4 barridos leían "la query falló" como "terminé el pool" → se apagaban solos Y borraban
  el cursor. Cada vez que le pedí al user prender el repaso, un 500 podía matarlo callado.
- 6 jobs detrás de `iterCount % 60` (30 min de proceso sin reiniciar, con worker que
  reinicia cada 7) **no corrían casi nunca**. Entre ellos el DESCONGELADOR → ahí estaban
  estancados los ~310 leads sin email.
- El parser de similares buscaba `domain` y la clave real es `Site` → 0 siempre, tapado por
  un fallback silencioso: **el autopilot venía tirando Majestic al azar**. Esa era la "baja
  calidad" del pool. Yo lo había arreglado en modules/cascade.js y no lo porté al worker.
- El slot de las 20:00 no podía disparar (`active_hours_end=20` lo dejaba fuera de ventana)
  → **techo real 16, no 20**. Y un parche mío del 10/08 acopló el horario del agente al global.
- El rollover de `next_day` exigía que el mismo proceso viera dos días → nunca pasaba, y
  esas filas consumían cupo de carril para siempre, apagando AutoGoogle sin gastar créditos.

**El patrón "no sé / no pude ahora" tratado como "no" o "terminé" ya lleva 13 casos.**
Es EL bug recurrente del proyecto: buscarlo SIEMPRE primero.

### Lo que se construyó
- **Salud/alertas**: `saludPing` / `saludAlerta` / `saludWatchdog` + `toolbar_health`.
  Una sola consulta reemplaza los 6 SQL manuales:
  `SELECT * FROM toolbar_health_check ORDER BY estado, job;`
  El vigilante corre PRIMERO en el loop a propósito (si vive dentro de lo que vigila, muere con eso).
- **Perseguir los 20**: el batch es "lo que falta / slots que quedan", con techo por slot.
  A 75% de rendimiento da 19 en vez de 15; a 50%, 15 en vez de 10.
- **Auto-pulido cada 10 días** (escalonado) y **caza de emails cada 3 días**, automáticos.
- **Copy nuevo en 5 idiomas** con `{{saludo}}` (nombre de pila) y `{{senal}}` (su ad stack o
  su tráfico). Se sacó la ofuscación `&#46;` que puntuaba SpamAssassin y hacía divergir
  text/plain de text/html.
- **Cerebro**: la moneda pasa a ser la RESPUESTA REAL, no la apertura (única métrica que un
  cold email malo puede ganar). Piso de 15% por template para seguir explorando.
- **Calidad de ads.txt por QUIÉN está adentro**: diario regional con 6 exchanges premium
  puntúa 60; content farm con 40 redes de popunder, 35. Antes era al revés.

### Pendiente / a medir
- **¿Aparecen respuestas reales?** La pregunta original, todavía sin responder. Mirar
  `toolbar_response_tracking WHERE response_type='real'` en 2 semanas.
- **El copy necesita datos del user**: qué es ADEQ exactamente para un publisher
  (SSP / reseller / network), qué formato y piso de CPM se puede ofrecer, y si hay algún
  caso nombrable. El copy actual NO inventa nada de eso a propósito.
- Backfill de Monday de items viejos sin marca Agente/Manual (bloqueado: falta saber cuáles).
- Columna "Origen" dedicada en Monday + `monday_col_origen`.
- Buzones comerciales en 1,5% — sin tocar; hipótesis: se corrige cuando fluyan leads hispanos.


## 📊 2026-08-10 — AUDITORÍA DE ENVÍOS: 6 FRENTES, 6 CAUSAS DISTINTAS

Disparador: "analizá los envíos del bot a ver si mejoró". Mejoró parte, y aparecieron 6 problemas.
**Tres los había causado yo en arreglos anteriores.** Y el patrón del día siguió sumando casos.

| Problema | Causa real | Commit |
|---|---|---|
| **0% LATAM, 56% Anglo** (tope 10%) | mi `fresh.sort()` del 04/08 por calidad de email BORRABA la cascada GEO armada el 21/07. Ahora GEO manda y el email desempata adentro del nivel | `3b1583f` |
| **10 de 20 envíos/día** | `_agentCfg` lee `agent_<key>`; yo hice poner `per_cycle_limit` SIN prefijo → se ignoró 3 días. El real estaba en 2 | `f109806` |
| **26 rebotes duros** | MV solo bloqueaba invalid/disposable; **catch_all pasaba como bueno**. Ahora 3 estados: ok / riesgo(reserva) / no | `aeecb23` |
| **Monday sin "Agente"/"Manual"** | la marca se escribía SOLO en `create_item`. Los dominios que ya estaban en el board (miles desde 2024) van por `updateMonday` y quedaban sin marca | `07fa2d0` |
| **Cola: 4 items en 3 días con 1.616 esperando** | el reclamo devolvía null si el PATCH no traía cuerpo → el llamador lo lee como "cola vacía" y frena. Y el item quedaba en `processing` colgado | `7e57076` |
| Aviso de seguridad alarmista | mostraba "cómo revertir el kill switch" aunque no hubiera frenado nada | `6f74ddd` |

### 🔴 EL PATRÓN, ahora con 8 casos en 4 días
**Un estado de "no sé / no pude ahora" tratado como "no" o como "terminé".**
slot del agente · slot del feeder · TLS→sitio muerto · cola CSV→apagado manual · categoría de
SimilarWeb→veredicto · ads.txt ilegible→sin evidencia · **catch_all→entregable** ·
**respuesta HTTP sin cuerpo→reclamo fallido**.
**REGLA DE REVISIÓN: ante cualquier código, preguntar si distingue "no sé" de "no".**

### ⚠️ Dos diagnósticos MÍOS que resultaron falsos (corregidos en el mismo día)
1. "Los rebotes son porque MV se queda sin cupo" → el cap está en 200 y van 32/día. Era catch_all.
2. "Lo de Monday es histórico, el código está bien" → había un bug vivo en el camino de update.
**Verificar antes de afirmar. Los datos del user desmintieron las dos.**

### Datos duros de la auditoría (07→10/08, 68 envíos)
- Buzón elegido: 67,6% persona · 30,9% genérico · **1,5% rol comercial**. Sin tocar; puede
  mejorar solo al arreglarse el GEO (los hispanos tienen más `publicidad@`).
- `agent_per_cycle_limit` estaba en 2; el user lo puso en 4 el 10/08.
- Slot de las 9: **se recupera** (el 10/08 hay envíos a las 11h = el de las 9 atrasado). ✅
- Cola: pending 1.616, `done` +4 en 3 días. waiting_pool 136→236.

### Verificación de columnas de Monday (board 1420268379, vía MCP)
`texto`=Comentarios · `deal_stage`=Estado · `deal_owner`=Ejecutivo · `email_mm2edcd3`=Email ·
`texto6`=Geo · `texto7`=Tráfico · `estado_12`=Idioma · `tel_fono_1`=Teléfono ·
`deal_close_date`/`fecha2`/`fecha_1`=fechas. Se agregó soporte para columna dedicada de origen
(`monday_col_origen` en config del worker, `MONDAY_COLUMNS.origen` en config.js) — vacías = todo
sigue como está. **Pendiente: crear la columna tipo Estado en el board + backfill de items viejos.**

## 🩺 2026-08-07 — 9 COMMITS. EL MISMO PATRÓN, CUATRO VECES

**EL PATRÓN DEL DÍA, y ya es sistémico: un estado de "NO PUDE AHORA" tratado como "TERMINÉ" o
como "NO SIRVE".** Cuatro instancias independientes en un día:

| Dónde | "No pude" | Se convirtió en | Commit |
|---|---|---|---|
| Slot del agente | el worker estaba ocupado a las 9 | slot quemado, **nunca** corría el de las 9 | `e71322d` |
| Slot del feeder | ídem, exigía la hora EXACTA | slot perdido todo el día | `1114172` |
| `fetchPageContent` | handshake TLS que NUESTRO cliente rechaza | "sitio muerto" → purgaba sport.es | `4fd6416` |
| Cola CSV | techo de 20min por tanda | apagaba la cola, "re-prender manual" | `3c00311` |

**Al revisar cualquier cosa, preguntar siempre: ¿este código distingue "no sé" de "no"?**

### 🔴 El pitch del agente estaba ROTO desde el 04/08 (`b0d55c4`)
Cuando puse la lista blanca de modelos en el api-proxy (`claude-haiku-4-5`, `claude-sonnet-5`),
el código pedía `claude-sonnet-4-5` (worker) y `claude-sonnet-4-6` (extensión). **Toda llamada a
Sonnet devolvía 400 "modelo no permitido" durante 3 días** — el 20% de los envíos que usa Claude
y el generador de pitch de la extensión entero. Haiku nunca se rompió (sí estaba en la lista).
**IDs vigentes: `claude-sonnet-5` y `claude-haiku-4-5`.** Sonnet 5 PIENSA por defecto y
`max_tokens` topea pensamiento+respuesta juntos → hubo que subir 1024→4000 y 200→1500.
**Decisión del user: el agente pasa a 100% templates rotando (`agent_claude_percent=0`).**

### 🔴 EL BLOQUEO EN CÍRCULO que secaba el pool (`3c00311`)
Síntoma: 4 leads nuevos en 3 días. **No eran los feeders ni la puerta del ads.txt** (los 5 slots
disparaban bien; `sin_ads_txt` no aparecía en los descartes). Era:
`csv_queue_enabled=false` → nadie procesa los 1.716 pendientes → backlog >250 → el feeder deja
de descubrir (a propósito) → el pool se drena. La cola se apagaba sola al llegar a los 20min de
tanda y **pedía reinicio manual**. Ahora corta la tanda, no la cola.
**Circuito a recordar:** feeders → `toolbar_csv_queue` → enriquecimiento → `toolbar_review_queue`.
El feeder frena solo si el backlog de csv_queue supera **250** (`CSV_QUEUE_HALT_HIGH`).

### El scraper encontraba los emails; se perdían en el ranking (`05e4960`)
`rankEmail` castigaba con −50 a la casa editora, que `_cleanScrapedEmails` sí acepta. Como
polishPool descarta todo lo que puntúa ≤0, el lead quedaba en cero teniendo el contacto.
apotheken-umschau.de daba 4 emails de Wort & Bild Verlag y se tiraban los 4. + faltaba
"vertrieb" (ventas en alemán). + **el `CONTACT=` del ads.txt suele ser del PROVEEDOR de ads**:
lexpress.mu devolvía `contact@setupad.com` — le íbamos a ofrecer inventario a un competidor.
Lista de 49 dominios ad-tech para descartarlos.

### La categoría de SimilarWeb es el TEMA, no el modelo de negocio (`056b9dd`)
ccn.com (cripto-NOTICIAS, que el user marcó como SÍ) descartado como "no es medio" — y tiene 73
líneas de ads.txt. `finance/investing` es la misma etiqueta para un medio de cripto y un bróker;
`heavy_industry` para una revista de arquitectura y una metalúrgica. Ya existía un juez de IA
para sitios bloqueados pero el veto por categoría cortaba antes. Ahora en las categorías
ambiguas decide la IA. **OJO: la categoría llega CORTADA A 30 CARACTERES desde la base** — los
patrones van contra el prefijo (`educatio`, no `education`).

### Estado al cerrar
- Railway en `3c00311`. Zip **v632** armado en el Escritorio (único cambio vs v631:
  `claude-sonnet-5` en modules/claude.js + maxTokens 200→1500 en gemini.js → revive el
  generador de pitch manual del MB, caído desde el 04/08).
- **Cursor del pulido reseteado a ''** al cerrar: los primeros 149 leads se habían procesado
  ANTES del arreglo del ranking (`05e4960`), así que se rehacen. Los que ya tienen email se
  saltean rápido.
- Corriendo en paralelo: repaso del pool (14% al último chequeo, 149/1058), drenaje de los 1.716
  de csv_queue, agente con 100% templates.
- Pool: 1.059 pendientes / 341 sin email. De los ya revisados, 40% sigue sin email — pero es el
  tramo MÁS VIEJO (mayo-julio) y con los arreglos de `05e4960` 3 de 4 ahora sí dan contacto.

### ⏭️ Al retomar — las 3 consultas de control
```sql
-- ¿bajó de 341 sin email?
SELECT count(*) FILTER (WHERE jsonb_array_length(coalesce(emails,'[]'::jsonb))=0) AS sin_email,
       count(*) AS pendientes FROM toolbar_review_queue WHERE status='pending';
-- ¿drenó de 1.716?
SELECT status, count(*) FROM toolbar_csv_queue GROUP BY 1 ORDER BY 2 DESC;
-- ¿mandó 20 por MB? (el 07/08 NO cuenta: los slots de la mañana corrieron con el tope viejo)
SELECT coalesce(user_email,'?') mb, count(*) FROM toolbar_agent_actions
 WHERE action='sent' AND created_at >= current_date GROUP BY 1;
```
1. ¿Bajó `pending` de csv_queue de 1.716? ¿El feeder salió de `skipped_saturated`?
2. **Sigue sin medirse si algo de esto se convirtió en RESPUESTAS.** Es la pregunta original del
   user, de hace dos semanas. Rebote 5,4%, pero nadie miró el inbox todavía.
3. Techo de 40M pageviews: se lleva >250 publishers de primera línea. Decisión del user, no bug.

## 🔎 2026-08-04 (tarde) — DESCUBRIMIENTO DE CONTACTOS: 3 VÍAS QUE NO NECESITAN CRAWLEAR

Worker-only (Railway) — **el zip de la extensión NO cambia**. Commit `10df868`, pusheado.

El techo que quedaba eran los leads que se van en cero. No es que no tengan contacto: o no está
donde mirábamos, o el sitio no nos deja mirar. Tres vías nuevas, todas **fuera del dominio**, y
solo corren si el lead iba a quedar sin email (costo cero en el caso normal):

| Vía | Qué da | Caso real medido |
|---|---|---|
| **MX + DMARC (DoH)** | la casa editora y a veces una persona | hnonline.sk → mafra.cz → `inzercia@mafraslovakia.sk` · radio1.hu → `hirdetes@mediamoment.hu` |
| **Certificate Transparency** (certspotter, NO crt.sh) | subdominios comerciales que el home no linkea | clarin.com (403 en el home) → comercial.clarin.com |
| **Google Play** | email del developer (obligatorio) + casa editora | peru21.pe (403 a todo) → `mobilepub@comercio.com.pe` · thepeninsulaqatar.com → daralsharq.net |

**Regla del email de Play:** casi siempre es soporte técnico (`soporteapps@`, `it@`, `app@`).
Mandarle el pitch quema el contacto. Primero se usa para deducir la casa editora y correr el
pipeline contra ELLA; el mail entra solo si su nombre es comercial, o de último recurso.

**4 bugs que salieron de probarlo contra dominios reales:**
- `cf-mitigated: challenge` → cortar el crawl. Un 403 del WAF no es "esta página no existe", es
  "no vas a leer NADA de acá". Pedíamos 40 rutas al pedo antes de rendirnos.
- Casa editora PROBADA ya no cuenta como cross-domain (era lo que tiraba `mobilepub@comercio.com.pe`).
- `AD_SALES_CONTIENE`: el token comercial puede no ir al principio (`mobilepub@`, `maria.ads@`).
- La lista de proveedores de correo estaba corta: livenation.nl tiene MX en `pphosted.com`
  (Proofpoint) y el worker se puso a buscar el contacto de pauta **EN PROOFPOINT**. Ampliada +
  heurística por nombre de marca (`mailX`, `smtp-`, `*hosting`).

Más: JSON-LD del home (gratis, el HTML ya estaba) y facetas `?cat=obchod` — mafra.cz publica el
contacto POR DEPARTAMENTO en la query string y el dedupe por path se comía justo la de ventas.

**Descartado a propósito:** índice inverso de sellers.json. No hay endpoint por seller_id, hay
que bajar el archivo entero (decenas de MB en Pubmatic/Index) y el worker ya reinicia cada ~7min
por OOM. El riesgo no compensa lo que aporta arriba de las 3 vías que ya están.

**Dos bugs que salieron de probarlo contra 45 dominios reales (commit `f6dba61`):**
- **Nadie miraba el tiempo TOTAL por dominio.** baccredomatic.com tardaba **451s**, astroawani
  160s, nestlefamilynes 158s. El worker reinicia cada ~7min → UN lead pegajoso se lleva puesto
  el turno de envío. Es la otra mitad de por qué mandaba 8 de 20. Presupuesto de 60s
  (`SCRAPE_MAX_MS`): pasado el deadline no se abren fases nuevas. 451s → 64s, mismos emails.
- **Un gmail suelto cortaba la búsqueda.** peru21.pe devolvía `nestorces@gmail.com` (de una red
  social) y por eso nunca llegaba a `mobilepub@comercio.com.pe`. Mismo patrón que el bug de los
  assets retina: un falso positivo apaga el camino bueno. `_tenemosContactoBueno()` exige buzón
  del dominio del lead / de la casa editora / con rol comercial. El webmail se conserva pero no
  frena la búsqueda.

**🐛 El panel de seguridad del 2026-08-04 (mañana) estaba ROTO** y no se había notado: las 6
llamadas usaban `auth.accessToken`, pero `auth` es una variable LOCAL de otra función →
ReferenceError. El botón de pánico, el reporte y el toggle de defensa NO hacían nada. Arreglado
en `3e38243` (zip **v631**). Si alguna vez el panel "no responde", mirar primero esto.

**Para el user (v631):** botón **🔁 Repasar el pool** en Agent, arriba de las stats. Prende
`polish_pool` y limpia el cursor → repasa el pool entero con el scraper nuevo. Los 419 sin email
pasan por ahí. No gasta RapidAPI; Apollo solo si `polish_use_apollo=true` y hay cuota.

**Medición del scraper nuevo:** 34 de 45 dominios con email (76%) sobre una muestra hostil (con
bancos, universidades y sitios muertos adentro).

**⚠️ El botón de repaso habría dejado al agente SIN ENVIAR** (encontrado antes de que el user lo
apretara, commit `5bc5c00`). `polishPool` corre ANTES de `maybeRunAgentSlot` en la misma vuelta
del loop: con 60s por dominio, un batch de 120 con concurrencia 12 son 10 min, y el worker
reinicia a los ~7. Ahora corta a los 2 min y sigue en la vuelta siguiente. + flag
`polish_only_missing`: el chequeo de "ya tiene email" se movió ANTES de salir a la red, así que
los 591 ya resueltos ni se tocan. **Regla general: cualquier job que corra antes del envío en el
loop necesita techo de tiempo propio.**

**SQL de arreglos de datos:** `sql/2026-08-04_arreglos_datos.sql` — devuelve a pending los
`urlpurge:%` (etiquetas rotas del bug del lote) y los `%ads.txt no verificable%` (los 120), y
prende `url_purge_enabled` + `adstxt_recheck_enabled`. **Dado al user el 2026-08-04.**

**Descartado por el user (2026-08-04):** IDs de ADEQ en ads.txt para no prospectar clientes
propios — "no me interesa". No volver a proponerlo.

### 🔴 EL BUG QUE MÁS SE REPITIÓ HOY: jobs sin techo de tiempo en cadena

**Tres veces el mismo patrón en un día.** Los jobs de pool corren EN FILA, uno detrás del otro,
y el worker reinicia cada ~7min por memoria. **Lo que va último puede no ejecutarse NUNCA.**

1. `scrapeEmailsForDomain` sin presupuesto → baccredomatic.com tardaba 451s y se comía el ciclo.
2. `polishPool` delante del envío → un batch de 120 × 60s = 10 min > los 7 del restart.
3. `recheckAdsTxtUnknowns` delante del pulido, **con el marcador "ya corrí hoy" EN MEMORIA** →
   en cada restart empezaba sus 200 dominios de cero, nunca terminaba, y el pulido quedaba en 0.
   Se detectó porque `ya_procesados` no se movía de 0 con el repaso prendido.

**REGLA: todo job que corra antes de otro necesita (a) techo de tiempo propio y (b) marcador de
progreso PERSISTIDO, no en memoria.** El orden de la cadena no es cosmético.

Orden actual (commit `c849d56`): `purgeByUrlOnly` (gratis, segundos) → `polishPool` (2 min) →
`recheckAdsTxtUnknowns` (1 min) → `sweepBlockedFromProspects`.

### 🔍 Cómo saber qué código corre en Railway (commit `c2fd298`)
El worker escribe `worker_commit` y `worker_boot_at` en `toolbar_config` al arrancar (usa
`RAILWAY_GIT_COMMIT_SHA`). Un SELECT alcanza — ya no hay que abrir el dashboard. `worker_boot_at`
además dice hace cuánto arrancó el container (reinicia cada ~7min).

### Estado al cerrar el 2026-08-04
- Railway deployado por el user con `c849d56` (el desbloqueo del pulido). `c2fd298` (el sello de
  versión) queda para el próximo deploy.
- Zip **v631** subido al Chrome Web Store por el user (~1h en impactar).
- SQL de arreglos corrido. **El pool pasó de 1.026 a 1.623 pendientes** y los sin email de 419 a
  **625** — porque volvieron ~600 leads descartados con etiqueta rota o por "ads.txt no
  verificable". **625 es el nuevo número a batir.**
- Repaso corriendo: `polish_pool=true`, `polish_only_missing=true`.

### ⏭️ Lo primero al retomar
1. Chequear que `ya_procesados` se haya movido de 0 y que `sin_email` baje de 625.
2. El número exacto de recuperados está en el log `✨ polish (solo sin email): … enriquecidos=N`.
3. **Lo que sigue pendiente de verdad: medir RESPUESTAS.** El rebote cayó a 5,4% pero la pregunta
   original del user —por qué no hay negociaciones en el inbox— sigue sin datos. Con 3-4 días de
   envío completo, correr el SQL del embudo. Si no hay respuestas, el problema ya no es técnico:
   es el pitch o el segmento.
4. Sigue abierto (decisión del user, NO bug): el techo de 40M pageviews se lleva >250 publishers
   de primera línea. Revisar con datos de respuesta.

## 📈 2026-08-04 — POR QUÉ MANDABA 8 DE 20 + URLs QUE NO ABREN (zip v630, deployado)

**RESULTADO DEL 03/08: el rebote se desplomó de 63% a 5,4%** (2 de 37 envíos). El diagnóstico de
MillionVerifier era correcto. El idioma también quedó bien: it→Italia, es→España/Argentina, y
todo lo que no tiene template sale en inglés. Ni un portugués a Rumania.

**El agente mandaba 8 por MB en vez de 20. Dos causas, las dos arregladas:**
1. **El pool se probaba SIN ORDENAR.** Como ~2 de cada 3 intentos mueren en
   `no_email_after_enrichment` —y cada uno gasta scrape + Apollo + Serper + MV ANTES de fallar—
   el ciclo se comía los ~7 min que dura el worker antes de reiniciar por OOM, y alcanzaba a
   mandar 2 de los 4 del slot. 4 slots × 2 = 8, los números cerraban exacto. Ahora el pool se
   ordena por probabilidad de éxito (primero los que ya tienen email con rankEmail ≥ 50).
2. **El slot de las 9 Madrid no corría** (medido: corrían 12, 15, 18 y 20). `_currentAgentSlot`
   exigía hora EXACTA; si el worker estaba ocupado esa hora, el slot se perdía para siempre.
   Ahora toma el último slot vencido del día → se recupera. El label lleva la hora DEL SLOT y se
   persiste, así que no duplica.

**URLs que no abrían — eran dos problemas:**
- **Al abrir:** 6 lugares distintos armaban el link. La ficha de Prospects hacía
  `https://www.${domain}` y eso ROMPE subdominios (`www.mafraslovakia.hnonline.sk` no existe).
  Todos unificados en `urlDeDominio()`.
- **Al guardar:** había DOS normalizadores (`_normalizeFeederDomain` y `cleanDomain`) con
  criterios distintos. Casos reales encontrados: una LISTA entera guardada como un dominio
  (`footmercato.net, www.fussballtransfers.com, ...`), `losandes.com.ar (r)`, `pctipp.ch.`,
  `fullmatchsports.cc/?tab=fullmatch`. Ahora los dos delegan en `normalizarDominio()`, que
  devuelve "" si no es un hostname válido. Base limpiada (0 filas malformadas).
  **OJO: hay UNIQUE sobre `domain`** — antes un dominio sucio que ya existía limpio se perdía en
  silencio al chocar con la constraint.

**Estado del pool tras el barrido:** pending 1.026 (591 con email) · validated 2.488 (= YA
contactados, NO es una cola sin usar, verificado en el código) · rejected 2.230 · frozen 917.
Purga: `sin_ads_txt` 701 — verificados techpowerup.com, xkcd.com, wuxiaworld.com, rsi.ch y
lolesports.com: **ninguno tiene ads.txt, el descarte fue correcto**.

**PENDIENTE:** los motivos de purga con exactamente 50/61/51 son las etiquetas rotas del bug del
lote (commit 5e0d4df) — rehacer solo el barrido por URL para que queden legibles. Y rescatar los
120 `sin_evidencia_monetizacion (ads.txt no verificable)`, juzgados antes del veredicto por
SimilarWeb.

## 🧹 2026-07-28 — CALIDAD DEL POOL (24 commits, zip v626, TODO DEPLOYADO)
Disparador: "el llenado de prospects está bien en cantidad pero la calidad falla". Confirmado.

**REGLA NUEVA DEL USER: sin ads.txt no entra.** Es el estándar IAB: sin ese archivo no se puede
vender programática. Va de PUERTA 0, antes de gastar un hit de RapidAPI, en los feeders
(autogoogle/autopilot/import/similar) Y en processCsvItem. `checkAdsTxt()` con TRES estados
(yes/no/**unknown**) — el unknown (Cloudflare 403, timeout) NUNCA descarta, reintenta.
Medido: ole.com.ar 1562 líneas/200 exchanges, clarin 1506/196; banistmo y nodejs.org 0.

**Auditoría adversarial de ads.txt (agente, ~370 medios reales) — 11 hallazgos:**
- 🔴 el fallback http:// aceptaba cualquier veredicto → un fallo TLS transitorio en https + 404
  en http daba "no" DEFINITIVO (tiempodesanjuan.com). Ahora http solo PROMUEVE a yes.
- 🔴 redirect cross-domain: /ads.txt redirige a la HOME de otro dominio → HTML → "no tiene".
  Recuperados y verificados: pulso.cl y paula.cl→latercera.com (787 líneas), laprensa.com.pa→
  prensa.com (186), alertapaisa.com→paisa.alerta.com.co (569). Un salto, como el spec IAB.
- BOM UTF-8 mataba la 1ª línea (en JS `\s` incluye U+FEFF) · líneas indentadas (12 dominios) ·
  muro anti-adblock `data-adblockkey` (correo.pe) · el test `<html` sobre 200KB (7/370 ads.txt
  válidos traen "<" en un campo de seller) · 200 con cuerpo vacío → ahora unknown · límite 200KB
  truncaba el parseo y subcontaba exchanges (punchng.com 4486 de 11220) · app-ads.txt por host
  (paginasiete.bo tardaba 48s) · Content-Type ya no vetea · PSL completada.

**IDIOMA reescrito** (casos del user: wanfangdata.com.cn chino→"pt", medyafaresi.com turco→"pt").
Dos problemas de DISEÑO: (1) el texto era UN voto entre ocho, compitiendo con TLD(2) y GEO(3);
(2) el detector solo conocía 6 idiomas y ante un sitio polaco/rumano devolvía cualquier cosa.
Ahora: **el TEXTO decide** si está seguro; reconoce **20 idiomas** para poder decir "no es ninguno
de los nuestros → inglés"; alfabeto no latino (zh/ja/ko/cirílico/griego/hebreo/tailandés/hindi)
→ inglés directo, chequeado ANTES del mínimo de longitud. Árabe es la excepción.
Regla del user: es/it/pt/ar → ese idioma; **cualquier otro → INGLÉS**. GEO y TLD solo deciden si
no hay texto útil, y la discrepancia texto⨯GEO queda logueada. 13/13 verificado.

**Otros filtros nuevos:** `classifyByUrlOnly()` (gratis, sin red — gobierno/universidad/apuestas/
banco/SaaS/acortador/CDN/ecommerce/streaming/ligas/marcas; 0 errores sobre 36 medios reales) ·
techo **40M pageviews** (decisión del user) · dedupe de subdominios a la raíz (globo.com tenía 5
leads con el mismo tráfico) · `scoreProspectable()` con umbral 30 que REEMPLAZA la cadena de ifs
donde "no sé"="sí" (era el bug de diseño de fondo: `!pageContent` y `classifier_unavailable_pass`
devolvían ok:true) · WHOIS proxies como email #1 (info@domain-contact.org en 8 leads) · marca
distinta ahora descarta el EMAIL, no el lead.

**Jobs de limpieza (auto-apagan, NO borran: rejected + suspect_reason):**
`url_purge_enabled` (gratis, segundos) → `purge_blocked_prospects` (ads.txt + clasificación +
**corrige el idioma** de los que sobreviven, aprovechando la página ya bajada).

**PENDIENTE DE REVISAR (user 2026-07-28): las bajas por TECHO DE TRÁFICO.** El techo quedó en
40M pageviews porque "40M es mucho para mi capacidad de trabajo" — decisión consciente del user,
NO un bug. Pero el barrido mostró que se lleva >250 dominios y muchos son publishers de primera
línea en GEOs prioritarias: folha.uol.com.br, zeit.de, ilpost.it, record.pt, gazzetta.it,
lavanguardia.com, reuters.com, latimes.com, ilsole24ore.com, orf.at, sky.it, 20min.ch, novinky.cz,
express.co.uk, euronews.com, as.com (chile/colombia/mexico), elpais (brasil/cincodias),
elperiodico, elmundo, marca. **Revisarlo cuando haya capacidad de trabajo o datos de respuesta.**
Recuperarlos es: `UPDATE toolbar_review_queue SET status='pending', suspect_reject=false,
suspect_reason=NULL WHERE suspect_reason LIKE 'urlpurge: gigante_%';` + subir
`threshold_traffic_max` (0 = sin techo).

**BUG ENCONTRADO Y ARREGLADO (commit 5e0d4df):** `purgeByUrlOnly` escribía el motivo de la PRIMERA
fila del lote a las 50 del lote (`slice[0].reason`). La DECISIÓN de purgar cada dominio era
individual y correcta, pero la etiqueta quedaba mezclada: aparecían fandom.com, zeit.de y
latimes.com bajo "url_casa_apuestas", y globo.com y bloomberg.com bajo "url_placeholder".
`sweepBlockedFromProspects` NO tenía el bug (escribe de a uno). Por eso se reprocesa todo de cero.

**PENDIENTE:** correr los 4 SQL de limpieza y revisar el veredicto por motivo · IDs de ADEQ en
ads.txt para no prospectar clientes propios · detección de video (SSPs de video en ads.txt) ·
frescura vía sitemap lastmod · mostrar `managerdomain=` en la ficha (dice quién le maneja hoy
la monetización: ole.com.ar → semseoymas.com).

## ✅ 2026-07-27 DEPLOYADO Y APLICADO (zip v625) — estado real, no pendiente
**Código:** 12 commits `a28d585..01a0091` pusheados a GitHub + `b1651a3` (bump manifest 625).
Edge function `track-open` deployada aparte (`supabase functions deploy track-open --no-verify-jwt`)
— smoke test OK (200/image/png). **La extensión NO se tocó**: todo fue backend
(`auto-prospector/index.js` + `supabase/functions/track-open`). El zip 625 en Desktop es solo bump.
✅ **Railway deployó `b1651a3` (hijo de 01a0091) el 27/07 ~14:05** → el worker corre TODOS los arreglos.

**Config aplicada por el user:** `millionverifier_daily_cap=200` · `agent_max_per_day=20` ·
`agent_focus_config` con `daily_override=0` y `weekly_target=0` (antes 20 y 100, ambos pisaban).
**Descongelados los 2.011** (`frozen_until=now()` + csv_queue a pending). El descongelamiento quedó cubierto: Railway ya tenía el fix cuando se soltaron.

**Los 12 arreglos:** agujero de logging `future_sent` · buzones IT/dominios puntuaban +70 ·
PATCH reserved→sent fire-and-forget · Serper por calidad no cantidad · enrichment descartado por
comparar largo de array · idioma pt→Rumania/Hungría/Taiwán · placeholders (`etunimi.sukunimi`) y
locals ≤2 chars · MV techo 100→500 · freeze por cuota de API · pixel con `&amp;t=` anti-prefetch ·
cap = primer contacto + re-trabajo sin límite · detector de publisher con 2 puertas abiertas ·
salto instantáneo a otra dirección (movido después del guard 30d, con tope de 3 MV/lead).

**PENDIENTE al cerrar la sesión:**
1. A los 3-4 días: `pct_rebote` debe bajar de 63% a <10%. Si no baja, MV no era el problema.
3. Los **1.194 leads ya en el pool con categoría "other"** — el detector arreglado frena a los
   NUEVOS, los viejos siguen ahí. Falta re-clasificarlos.
4. Normalizar `details->'source'` cuando es objeto (data pre-2026-07-01, fragmenta el ranking).

## ⚙️ REGLAS DE NEGOCIO CONFIRMADAS POR EL USER (2026-07-27) — no volver a preguntarlas
- **20/día por media buyer = 20 PRIMEROS CONTACTOS.** MBs: **mgargiulo = Max · sales@adeqmedia.com = Agustina · dhorovitz = Diego** (confirmado 27/07). El re-trabajo (2ª dirección, re-engagement, reintento por rebote, future email) va
  **APARTE y SIN LÍMITE** — es re-trabajo de algo que salió mal, no debe robar cupo.
- **NO hay follow-ups en la toolbar**: los FU1/FU2 a la misma persona los hace **Monday**.
  Lo único que hace el agente es **re-envío a una dirección NUEVA** si (a) el mail rebota o da
  inválido → salto **instantáneo**, o (b) no se abrió tras N días (`agent_reengagement_wait_days`, 5).
- **TRAMPA DEL CAP (config real al 27/07):** `agent_max_per_day=10` pero
  `agent_focus_config.daily_override=20` → **daily_override PISA al campo del admin**. Precedencia:
  `agent_max_per_day_by_user` > `daily_override` > `agent_max_per_day`. Para que el panel de admin
  mande, poner `daily_override=0`. Ojo también con `weekly_target=100`: a 20/día frena al 5º día.

## 🧨 AUDITORÍA 2026-07-27 PARTE 2 — LAS CAUSAS REALES (commits a5bcbd4, 0836190; NO deployado)
Con SQL sobre 14-30d. **Los 3 hallazgos que explican el silencio del inbox:**

1. **63% de BOUNCE DURO** (250 rebotes / ~398 envíos en 14d). Los rebotes son REALES (sospeché
   falsos positivos: me equivoqué). Causa: `MV_ABS_DAILY_MAX=100` en código y
   `millionverifier_daily_cap=20` en config, con ~40 envíos/día → solo se verificaba la mitad, y
   el cap hace **fail-open** (agotado el cupo, ENVÍA sin verificar). Techo subido a 500.
   **PENDIENTE DEL USER: `UPDATE toolbar_config SET value='200' WHERE key='millionverifier_daily_cap';`**
   Hay 10.000 créditos comprados = ~250 días verificando el 100%.
2. **2.011 leads congelados por CUOTA DE API, no por calidad** (91% de todo lo frozen).
   `rapidFetchWithRetry` devuelve `daily_cap_reached`/`per_minute_fuse_tripped`/`HTTP 40x` cuando
   el sin-cupo somos nosotros; el regex de "transitorio" solo cubría 429 y 5xx → cada lead
   procesado sin cuota sumaba intento fallido → 3 intentos = freeze 15/30/60d → 3 ciclos =
   **blocklist PERMANENTE**. Arreglado. Falta descongelar los 2.011 ya afectados.
3. **Open rate 80% era falso**: doble conteo (eventos, no mails únicos) + 23,2% de las aperturas
   llegan a <2 min del envío = prefetch de Google/Outlook y escáneres. Real: 412 únicos/1.008 = 41%,
   ~33% sin prefetch. El pixel ahora se firma con `&t=<epoch>` y la edge function descarta <2 min.
   **Filtrar por user-agent NO sirve**: 24,8% es GoogleImageProxy y ahí hay aperturas reales.
   Importa porque `getDynamicSourceRank` rankea fuentes por open rate y el re-engagement no
   insiste si cree que abrieron.

**Por qué rebota un email que el prospector aprobó:** el prospector encuentra CADENAS de texto
(scrape del HTML, WHOIS vía informer, Apollo, Google) y `rankEmail` puntúa la FORMA del address
(¿parece persona? ¿rol comercial?), no su existencia. `ffabre@carrefoursa.com` puntúa alto y la
persona ya no trabaja ahí. Apollo tiene data vieja. El ÚNICO componente que comprueba que el buzón
exista es MillionVerifier — y estaba capado al 50% con fail-open. Ver punto 1.

**Estado del pool (3.030 pending):** 36,5% SIN email · solo 1,8% con contacto comercial/exec como
email #1 · categoría "other" 39,4% (ahí viven los no-publishers: bancos, hospitales, Akamai,
self-storage) · publishers reales ~29%. Skips por "sin email": 72,6% encontró CERO emails (el fix
de Serper del commit 23970f7 solo ataca el 27% restante). 77% de los dominios recibe 1 solo toque
(sano). Envíos: 3 buzones (mgargiulo, dhorovitz, sales), ~18-40/día.

## 🔍 AUDITORÍA 2026-07-27 — POR QUÉ NO HAY RESPUESTAS (commit fd5133a, NO deployado aún)
Disparador: el user no recibe negociaciones pese al volumen. Cruce SQL (4d) ⨯ Gmail MCP.

**Diagnóstico honesto: no es entregabilidad, es volumen útil.** ~120-150 envíos reales en 4 días,
tasa típica cold email 1-3% → 1-4 respuestas esperables. Hubo 1 (negativa, de stateofmind.it). Cero
rebotes mailer-daemon en Gmail = MillionVerifier funcionando. N5 Anglo: 0 envíos con 844 en pool =
el fix de GEO anda. Monday 43 ok / 5 failed.

**Cuello de botella real:** por cada mail que sale, ~4,7 prospectos se caen sin email
(`no_email_after_enrichment` 173 + `no_alt_email` 51 vs 48 `sent` en 4d). Y 25% de lo enviado es
`reengagement` → prospección NUEVA real ≈ 29 en 4 días. **Ese es el próximo trabajo y el de mayor ROI.**

3 bugs arreglados (commit fd5133a):
1. **AGUJERO DE LOGGING** — `processManualReengagementQueue` (path "future email") enviaba por Gmail
   y no escribía NUNCA en `toolbar_agent_actions`. 25 y 26/07: 0 filas de envío en la base, ~76 mails
   reales en Gmail. **Toda métrica de GEO/tipo-email/fuente estaba calculada sobre menos de la mitad
   de los envíos, y sobre la mitad buena.** Ahora loguea `action='future_sent'` (NO consume cap;
   si se quiere que consuma, agregarlo a la query de `getAgentDailyCount`).
2. **rankEmail: buzones IT/dominios puntuaban como PERSONA.** `reliancedomains.admin@ril.com` y
   `drc.seguranca@cuf.pt` daban +70 por el patrón nombre.apellido; `informatique@`/`domeny@`/
   `wsparcie@` +55; `itsec@` +30. Nuevo rol `IT_INFRA` (+5) con `IT_INFRA_SEGMENT` chequeado por
   SEGMENTO del local-part (agarra el `.admin` final, no solo prefijos).
3. **PATCH reserved→sent era fire-and-forget.** Worker reinicia cada ~7min; reinicio entre el send
   y el PATCH → fila queda 'reserved' → el cleanup de arranque la pasa a 'failed'. Ahora await +
   2 reintentos + log 🔴.

**Pendiente de esta auditoría:** (a) atacar `no_email_after_enrichment`, (b) frenar dominios que no
son publisher — se enviaron mails a aerolíneas (air-austral, jetstar), bancos (labanquepostale),
hospitales (cuf.pt, hirslanden), ministerios (mesrs.dz), ONGs (fundacionaquae), (c) `angelina.jolie@gmail.com`
pasó el filtro de basura, (d) 3 casos `en → ARGENTINA/SPAIN` (revisar, puede ser legítimo si la web
está en inglés), (e) `bounce_detected`=18 en la DB pero 0 mailer-daemon en Gmail → posible falso positivo
que dispara `bounce_retry_sent`.

## 🩸 JORNADA 2026-07-17 — 5 BUGS QUE COSTABAN PLATA Y LEADS (zip v611, todo deployado)
Sesión de diagnóstico por SQL. Todo lo de abajo estaba ROTO EN SILENCIO (los `.catch(()=>{})` tapaban todo).
Ciclos de API: ver [[reference-billing-cycles]].

1. **Monday mataba leads (15 perdidos el 16/07).** `pushToMondayServer` mandaba `countryShortName:""`
   a la columna phone (exige ISO2 válido) → Monday rechaza el ITEM ENTERO. Como Gmail se manda ANTES
   del push, el lead recibía el pitch y quedaba invisible en el CRM. **Lo introduje YO el 16/07 con la
   captura de teléfono.** Fix: ISO derivado del geo, validado contra COUNTRY_CODES ("UK" NO es ISO2 → es
   GB; sin validar reintroducía el bug). Si no resuelve → omite la columna, el lead entra.
   + `rescueFailedMondayPushes` (flag `monday_rescue_enabled`) re-pushea desde `details.retry_payload`
   con CLAIM ATÓMICO (PATCH CAS `action=eq.monday_failed`, exige 1 fila) — el CRM tiene 9.457 items,
   duplicar es peor que no rescatar. `findMondayItem` NO sirve como única defensa (devuelve null tanto
   si no existe como si Monday erroró). Si el push falla NO revierte → queda `monday_rescue_failed`.
2. **Rebotes reenviados en loop.** El dedup era `!isBouncedSync(failed)` = tabla bounced_emails, pero los
   SOFT a propósito NO se marcan ahí → cada pasada redescubría el mismo msg (evima.gr ×64 en 7d; 233
   detecciones para 46 dominios) y **re-disparaba queueBounceRetry** = reenvíos que queman reputación.
   Fix: dedup por ID de mensaje (`toolbar_bounce_seen`). + el parser del body tomaba direcciones de la
   INFRA de Gmail como el publisher rebotado (mail.gmail.com ×61) → `BOUNCE_INFRA_DOMAINS`, solo en el
   fallback del body (X-Failed-Recipients queda intacto → un contacto real @gmail.com se sigue detectando).
3. **Apollo ancla 6 + días de mes calendario** → el 17/07 creía que quedaban 15 días cuando faltaban 26
   al 12/08 → repartía el doble y se secaba ~2 semanas. Cap 2500→2250 (margen 10% que pidió el user).
4. **SimilarWeb ancla 6, resetea el 7** → reseteaba el contador 24h antes de que repusieran la cuota.
5. **`bump_keyword_yield` DUPLICADA.** La migración del 16 agregó un param con CREATE OR REPLACE: cambiar
   la firma NO reemplaza, crea SOBRECARGA → llamada con 4 args = "function is not unique" → 500 tragado
   por `.catch()` → **toolbar_keyword_yield SIEMPRE vacío → AutoGoogle elegía keywords 100% random**.

**Pedidos del user implementados:** turno hispano 50/50 ROTATIVO (`_isHispanicSlot` alterna y rota por
día → 50.0% exacto, 0 días sin barrido; AutoGoogle es-only + gl hispano, Majestic TLDs hispanos) +
**pre-listado** (`toolbar_discovery_backlog`): `_injectIntoCsvQueue` DESCARTABA el excedente del carril
(`slice(0, laneRoom)`) = dominios ya pagados con Serper a la basura; ahora se estacionan y los slots
no-hispanos los drenan gratis antes de gastar. + **GEO sin exclusiones + rotación día a día**: el user
pidió NO excluir países (tenía `geos_excluded:["US","NZ","NL","CA","GB"]` → 788/1825 del pool, 43%, eran
leads que nunca se enviarían). El round-robin por GEO ya existía pero sin memoria entre ciclos →
`_recentGeoSendCounts` pone adelante los GEOs menos enviados en 7d (spread 15/10 → 13/12: mejora modesta).

**Método que funcionó:** revisión adversarial por subagente del propio diff ANTES de commitear — encontró
4 bugs míos en la primera tanda (uno habría borrado dominios pagados del pre-listado) y el `"UK"` de
`_mondayPhoneIso`. Hacerlo siempre en cambios al hot path.

**GEO (pedido del user, zip v612):** descubrimiento debe ser ≥50% LATAM y Anglo (US/UK/CA/Oceanía)
<10% del día a día. Envío: NO excluir países pero que USA no encabece (es el que menos trabaja y más
URLs tiene). Implementado: `_isAngloOverDailyQuota` (cuota dura sobre lo insertado en las ÚLTIMAS 24h,
no sobre el pool histórico ya contaminado; se chequea tras conocer el país y ANTES de Claude; marca
next_day, no skipped; falla suave) + Anglo al final del round-robin del agente (`_isAngloGeoKey`; sort
estable → respeta la rotación dentro de cada tramo). **LATAM ≥50% NO se puede garantizar** — depende de
que Majestic/Serper tengan el material; las palancas son el turno hispano 50/50 + la cuota Anglo.
**BASELINE 2026-07-17 (pre-fix, últimas 24h): 🔴 Anglo 77.7% (383) · resto 19.9% · 🟢 Hispano 2.4% (12).**
Comparar contra esto. Si el hispano no sube bastante, el problema es el DESCUBRIMIENTO (fuentes sin
material latino), no el filtro → atacar ahí, no bajar más el cap.

**RESCATE: ✅ CERRADO (2026-07-17, zip v614).** 18 recuperados / 18 reclamados / 0 fallados / 0 duplicados
(el claim atómico funcionó: cada lead reclamado 1 vez y pusheado 1 vez). Flag apagado.
Costó 4 intentos por bugs MÍOS encadenados, todos del mismo tipo — **el job no corría y no había forma
de saberlo**: (1) `iterCount % 60`, y cada deploy resetea iterCount → en una tarde de pushes no corre
nunca; (2) bajado a `% 15` = 7,5 min con POLL_INTERVAL_MS=30s (no 5 min: leer la constante antes de
prometer tiempos); (3) `_getMondayApiKeyForFeeder` NO cae a `cfg.monday_api_key` (el agente usa
`getMondayKeyForUser`: perUser || cfg.monday_api_key) → null → `return` MUDO; (4) el cooldown se marcaba
ANTES del flag → una corrida con el flag apagado se lo llevaba puesto, y encima el cooldown (10min) era
> el intervalo (7,5min) → una de cada dos llamadas se descartaba.
**Solución final: llamarlo en CADA iteración** (el flag + cooldown de 5 min lo pacean; con el flag
apagado sale en microsegundos) + loguea SIEMPRE discriminando el motivo.
**REGLA:** un job de recuperación NUNCA debe depender de sobrevivir N iteraciones sin deploys.

**También pendiente:** que `toolbar_keyword_yield` deje de estar vacío tras un slot de AutoGoogle
post-deploy (slots 10/11/14/17/21 Madrid). Zip v614 YA SUBIDA al Chrome Web Store (borrador).

## 📬 OPTIMIZACIÓN DE RESPUESTAS + AUDITORÍA 22-24 (2026-07-24, zip v623)
Análisis de 4 SQL (agente/worker/webs malas/deliverability). WINS: North Star cumplido (0 prospects
nuevos SIN email), GEO por niveles funcionando en entrada (55% hispano) Y salida (53% hispano, 0% anglo).
FIXES: (v621) geos_priority se usaba como FILTRO DURO en qualify → mataba tier-4 Asia/África (699 skips)
que el user SÍ quiere → quitado (la cuota nivel-5 + cascada manejan prioridad). + rankEmail rechaza
basura (a@ 1 letra, cuenta@gmail, alumno@, freemail genérico). + Serper agregado a runReenrichBadLeads
(las 4 vías de email ya usan scrape+apollo+serper). (v622) tier DEPARTMENT en rankEmail: soporte@/
denuncias@/rrhh@/bok@/cskh@ bajan a +8 (antes +70 como persona) → mejor targeting. (v623) MillionVerifier
verificación de entregabilidad ANTES de enviar (ver [[reference-billing-cycles]]) — ACTIVADO en cap 20
(prueba), subir a 300. **BOUNCE: cayó de 190% (pre-fix del loop) a 17% el 24/07 y bajando** — el >100%
histórico era el loop de rebotes re-detectados (v609). 17% real sigue 2-3x el techo sano (5-8%); las
causas: local-parts equivocados (MillionVerifier los mata) + 20% info@ genérico. SMTP self-hosted
descartado (arriesga IP de Railway). Limpieza puntual del pool: DELETE de aerolíneas/universidades
reales corrido por user (dejó afuera flight-trackers/medios financieros = falsos positivos). Monday:
columna Comentarios marca "Agente"/"Manual" (v619/v620) para comparar quién trabaja mejor.

## 🔎 AUDITORÍA 5 DÍAS + PIPELINE END-TO-END (2026-07-21, zip v616)
El user pidió: (a) tranquilidad de que el agente elige la URL correcta de Prospects, manda al mail
correcto, y si falla busca OTRO mail para reenviar; (b) que la ALIMENTACIÓN use todas las fuentes y
foque LATAM+Europa, no tanto USA/Canadá/Oceanía, preconfigurado. Es una CADENA: si un eslabón falla,
lo de abajo cae al vacío.
**Cadena verificada eslabón por eslabón (toda OK):** ① alimentación = las 6 fuentes corren (sellers,
autogoogle, majestic, adstxt, monday, similar + autopilot/runSession). ② calificación (350K + detector
+ cuota Anglo). ③ el agente elige URL por scoring + round-robin GEO (rotación 7d, Anglo al final).
④ elige email por tier (decisor/apollo > rol comercial > persona > genérico) + filtro placeholders.
⑤ si rebota busca otro email del lead y si no hay, rescata.
**Bugs del análisis, RESUELTOS (v615+v616):** (1) se enviaban placeholders reales (jane.doe@, first@)
→ PLACEHOLDER_LOCAL extendido. (2) el 74% se caía sin email (no_email_after_enrichment=210/5d) porque
Serper/google_contact (la MEJOR fuente, 32.6%) SOLO corría en el qualify, no al enviar NI en el rescate
del rebote → agregado a las 2 (ahora las 3 vías —qualify/envío/rescate— usan scrape+apollo+serper con
cap 250/día compartido). (3) atribución: 875 Prospects TODOS "autopilot" (runSession no pasaba source)
→ etiqueta real similar/radar/majestic + fix del bucket manual/autopilot del popup.
**PRIORIDAD GEO POR NIVELES (v617, pedido del user 2026-07-21):** una sola fuente de verdad `_geoTier(iso)`
→ 1..5, usada en búsqueda Y envío. Cascada: 1 LATAM (Sudamérica+México) → 2 Centroamérica → 3 España →
4 Europa(incl UK)/Asia/África/MENA → 5 Oceanía/USA/Canadá (cap 10%). ENVÍO: round-robin ordena buckets
por nivel, dentro del nivel desempata rotación 7d; si un nivel se agota sigue con el siguiente (fallback).
DESCUBRIMIENTO: cuota "nivel 5" <10%. `_geoKey`+`_recentGeoSendCounts` pasaron a ISO2 canónico (_leadIso).
Eliminados ANGLO_ISO/_isAngloGeoKey. UK/IE salieron del cap (ahora nivel 4). Revisado adversarial: limpio.
**ENVÍO 20/día por MB (v618):** AGENT_DEFAULTS max_per_day 10→20, per_cycle_limit 2→4.

**CONFIG (DB) que el user tiene que dejar seteada:**
- `agent_focus_config.daily_override` = 20 (estaba 30 → pisa el default nuevo de 20). geos_priority/excluded = [].
- `target_geo` — el user lo puso "LATAM,CentralAmerica,Europe" pero eso EXCLUYE Asia/África de runSession,
  que son nivel 4 (queridos). Ampliar a "LATAM,CentralAmerica,Europe,MENA,Asia,Africa" (todo no-anglo) o
  vaciarlo (runSession global + reorden hispano-primero + cuota nivel-5). Recomendado: ampliarlo.
- `worker_discovery_config.geos_priority` = [30 ISO LATAM+Europa] (ya seteado, sesga Majestic + turno hispano).

**Techo real de LATAM = cuánto dominio latino exista en las fuentes.** Si tras todo esto el hispano no
sube, el próximo paso es SUMAR UNA FUENTE NUEVA de dominios latinos (ningún filtro genera lo que no está).

**LECCIÓN (repetida 2 veces hoy):** un job que falla en silencio es indistinguible de un flag apagado.
Todo job nuevo LOGUEA SIEMPRE, aunque no haga nada, y dice POR QUÉ. Los `.catch(()=>{})` de este
repo tapan bugs por semanas (fue la causa raíz de 3 de los 5 bugs de hoy).

## 🚀 OPTIMIZACIÓN DESCUBRIMIENTO + CONTACTO + DETECTOR 2026-07-16 (zip v606, worker deployado)
Sesión larga de mejoras (todo deployado). Contexto: [[reference-no-prospectable-types]].
**Contacto (North Star ≥1 email):** agregado descubrimiento de tel/WhatsApp (extractPhonesFromHtml del
footer + `fetchPageContent` devuelve phones/whatsapps) + fallback Google `_serperContactSearch` (busca
"<dom> contato" en Serper cuando el dominio tiene CERO email → email+tel+WhatsApp de los snippets).
OPTIMIZADO por costo: solo si curEmails.length===0, cap diario `serper_contact_daily_cap`=250, dedup,
metrado en `serper_contact_used`. Rinde ~4% emails pero ~$1.2 → se deja capeado (50k Serper duran ~4 meses).
La toolbar muestra Tel/WhatsApp en cada card (contact_phone; "wa:" = link wa.me).
**Majestic feeder optimizado:** cursor SECUENCIAL (`majestic_cursor`, antes random) + sesgo GEO/TLD
(`_CC_TO_TLD` según worker_discovery_config.geos_priority) + pre-filtro por nombre (`_MAJESTIC_NAME_SKIP_RE`:
gov/edu/shop/casino) antes de gastar RapidAPI.
**AutoGoogle yield:** selección 65% frases top-yield + 35% exploración (antes 100% random). Tabla
`toolbar_keyword_yield` (phrase/searches/found/fresh) + RPC `bump_keyword_yield` — migración
sql/2026-07-16_autogoogle_keyword_yield.sql APLICADA. AutoGoogle = descubre dominios nuevos (Serper,
keywords del cascade); Autopilot = califica (tráfico 350K + detector publisher); son consumidores Serper
SEPARADOS. Feeder Majestic 1M sigue vivo (auto_feeder_majestic, 1 de 3 fuentes con sellers.json+Monday).
**Fixes UI (v606):** contador Prospects muestra el REAL (count=exact/Content-Range, antes se clavaba en
1000); contador "CSV" filtra source=csv (antes contaba autogoogle/feeders); contadores footer Apollo/
AutoGoogle andaban (state.accessToken no se seteaba al login); AutoGoogle cap real 10000.
**Detector nuevo (worker+popup):** dead/SSL/cert/DNS descarta; cripto/dev-tool/app/tienda bloquea. Ver
[[reference-no-prospectable-types]]. Monday FU1 id `fecha2` (era `fecha2_8` inexistente).
**Expansión por similares (nuevo, ACTIVO):** `runProspectSimilarExpansion` — toma Prospects pending ≥500K,
trae sus similar-sites (SimilarWeb cache 90d), inyecta frescos → misma calificación. Flag
`similar_expansion_enabled='true'` PRENDIDO 2026-07-16. RapidAPI-gated + cursor `similar_expansion_cursor`
+ cooldown 5min + lote 6. Inyecta como auto_feeder_majestic. Complementa el seeding de likes+validados
que ya hace el autopilot. Verificado: piso 350K = pageViews (o visits×pagesPerVisit, o visits×2.0), NO visitas.
Fuentes de descubrimiento activas: AutoGoogle (Serper/keywords), feeder Majestic-1M (cursor)/sellers/monday,
similar-expansion, CSV manual. Contador Serper contacto = serper_contact_used (cap serper_contact_daily_cap=250).
RESUELTO 2026-07-16 (zip v607): (1) MÉTRICA POR FEEDER — majestic/adstxt/similar tienen source propio en
review_queue (antes lumped en 'autopilot'); badges en popup. (2) KEYWORDS FRESCAS por Claude (Haiku) 1×/sem
→ config autogoogle_fresh_keywords, gated `autogoogle_fresh_keywords_enabled` (OFF default). (3) REBOTES —
Apollo ordena verified>likely>guessed (guessed=fallback) → baja rebotes sin perder cobertura. El ranking
dinámico por source (aggregateSourcePerformance, arreglado el 15) ya penaliza fuentes que rebotan.
AutoGoogle yield por CALIFICADOS: HECHO 2026-07-16 (migración sql/2026-07-16_autogoogle_qualified_yield.sql).
Columna `qualified` + tabla `toolbar_autogoogle_attribution` (domain→phrase) + reconciliación al inicio de
cada slot (¿llegó a review_queue source=autogoogle? → bump qualified). Selección ordena qualified DESC,
fresh DESC. Aparte del pipeline core (0 riesgo).
DESCARTADOS por decisión del user (2026-07-16): SMTP verification (no funciona desde Railway — puerto 25
bloqueado — + riesgo de descartar válidos); seguridad Monday→Edge Function + rotar keys (el user NO quiere
cambios manuales de keys). NO re-proponer salvo que el user lo pida.

## 🔴→🟢 WORKER OOM RESUELTO 2026-07-15 (por qué "nada se reprocesaba")
El worker Railway se REINICIABA cada ~7min (SIGTERM/OOM) → los jobs largos (polishPool) morían a mitad
→ cursor congelado + heartbeat null (parecía "worker muerto", pero estaba vivo mandando mails).
CAUSA RAÍZ + fixes (commits 7c93b16, 6e8c2c9, 7ea7295, 161f997, + speed-up):
1. **setConfigValue era PATCH-only** → NO creaba keys nuevas (un PATCH sobre key inexistente matchea 0
   filas y no crea nada; encima el cache in-memory la marcaba "escrita"). Efecto cascada: los guards
   "1×/día" (source-perf aggregate = pull 10k filas, digests, alertas) nunca persistían su last-run →
   corrían en CADA loop → pico de memoria. FIX: PATCH y si 0 filas → INSERT.
2. **aggregateSourcePerformance** crasheaba con `(x||"").toLowerCase is not a function` (details.source
   JSONB no-string). FIX: guardas typeof string.
3. **Caches keyed-by-domain sin evicción** (_publisherClassCache/_domainLangCache/_mxCache/_adsTxtCache/
   _voyageWorkerCache/_claudePickCache) filtraban memoria. FIX: limpieza periódica/bajo-presión en el loop
   (los de API paga solo si rss>550, para no re-gastar créditos).
4. **polishPool solo commiteaba el cursor al FINAL del batch** → restart lo perdía y re-empezaba el mismo.
   FIX: commit incremental por wave → avance garantizado aunque haya restarts. VERIFICADO: cursor avanzó.
Otros fixes del mismo día (logs Railway): AutoGoogle pre-check del carril (no gasta Serper si carril lleno
180/180), bounce skip si dominio ya frozen (cortó loop evima.gr + spam Monday), Monday FU1 id `fecha2_8`→
`fecha2` (id real verificado vía MCP contra board 1420268379; también en config.js de la extensión).
DIAGNÓSTICO reusable: worker vivo = `auto_heartbeat_at` fresco (<2min); si polish avanza = `polish_cursor_ts`
se mueve; si faltan keys de vida en toolbar_config = worker no completa el loop. Zip actual = **v601**.
Pendiente monitoreo user: que el cursor siga avanzando + Railway sin Start/Stop (si sigue, subir RAM del plan).
Rebotes altos (10-12% vs techo 8%) NO tocado — es tuning de calidad de emails, requiere OK (regla de oro).

## 📍 AUDITORÍA 48H ejecutada 2026-07-13 → 4 fixes deployados (commits 60a9465, 5d34b20)
Se corrió `sql/2026-07-11_auditoria_48h.sql` (versión "todo-en-una-query" con UNION al final del archivo,
porque Supabase Editor solo muestra el ÚLTIMO result set). Hallazgos sobre ~180 envíos del AGENTE en 7 días:
- **Cap 10/día: OK.** Días normales 4–8/MB (ni llega a 10). Único pico 07-08 (17/21/18) = reproceso de
  bounces de ese día, que SALTA el cap (los re-envíos de bounce/reproceso no cuentan). No es fuga diaria.
  Los números "feos" (30/38) de toolbar_response_tracking eran envíos MANUALES (source='manual_extra').
- **Selección de URL**: pool 300 con traffic≥min → scoreWebsite (dropa gates) → shuffle por GEO +
  round-robin entre países. NI primeros-de-fila NI puro azar → azar balanceado por país. Correcto.
- **~1 de cada 3 envíos iba a NO-publisher** (banco/seguro/broker/marketplace/comparador/marca/uni/gov/
  salud/ecommerce que corren pixel de ads → se colaban). Peor caso: applovin.com (adtech, YA en blocklist).
- **Emails**: placeholders (vorname.name@) se enviaban como "persona"; informer daba WHOIS/IT basura
  (domainmanagement@axa, net-manage@, it-einkauf@); gmail/hotmail personales; deptos equivocados (casting@).

**FIXES (user eligió filtro CONSERVADOR, FP casi nulo):**
1. Detector: schema banco/seguro/universidad/gobierno (+GovernmentOrganization) → rechaza AUNQUE tenga
   ads (un publisher jamás se auto-marca así). travel/ONG/inmobiliaria + keywords SIGUEN gateados por
   !hasDisplayAds. + FALSO POSITIVO americasvoice.news arreglado (Shopify: exige firma de STOREFRONT
   Shopify.shop/.theme/window.Shopify/shopify-section, NO un link *.myshopify.com de merch).
2. Blocklist re-chequeada EN EL ENVÍO (runAgentCycle, isDomainBlockedFull por lead) — antes solo al
   importar → applovin se colaba. + marcas al BRAND_BLOCKLIST (adidas/realmadrid/nike/puma/cocacola/...).
3. rankEmail: rechazo DURO de PLACEHOLDER_LOCAL/JUNK_LOCAL_*/técnicos (domainmanagement/net-manage/
   it-einkauf/betrieb/sistemas/dominios/edv/infra) + penalty -45 deptos no-comerciales. _pickTier:
   informer de tier 4 → 1 (WHOIS baja calidad). Rechazar el EMAIL no pierde el DOMINIO (queda pending).
- Fuente `unknown` en response_tracking = hueco de ETIQUETADO (emails viejos sin email_sources), no
  afecta envío — NO se tocó.

**RONDA 2 de fixes (commits ae82cc3, 86349e7, f4b962a, 9f8ee44) — sobre feedback del user:**
4. Hard-block por CATEGORÍA SimilarWeb (isCategoryBlockedWorker): real_estate/banking_credit/insurance/
   accounting/jobs_and_employment/classifieds/marketplace/e-commerce puro → NO entran a Prospects.
   ⚠️ NO se bloqueó adult/price_comparison/marketing porque SW mal-clasificó publishers REALES ahí
   (fatherly=adult, tweakers=price_comparison) → esos van a Haiku/manual (regla de oro).
5. rankEmail HARD-REJECT relajado (feedback user "los gmail no están mal, sistemas.diariodovale@ no lo
   veo mal"): hard-reject SOLO whois/dominio (domainmanagement/dominios) + placeholders/falsos
   (vorname.name/celebridades). IT/sistema/system/sys/betrieb/net-manage/it-* → PENALTY -55 (no reject:
   en medio chico pueden ser el único contacto). gmail/hotmail NUNCA se bloquean (solo -20 soft, revert
   si es rol/persona). +penalty -45 deptos no-comerciales (casting/quejas/seguridad).
6. AD_SALES_LOCAL EXPANDIDO multilingüe (mejor elección de email): +régie(FR), Vermarktung/Anzeigen/
   Verkauf(DE), verkoop/adverteren(NL), vente(FR), raccolta(IT), auglýsingar(IS), annons(SE). Espejo en
   popup (_AD_SALES_LOCAL_RE) también. Celebridades falsas (kylian.mbappe@gmail) = NO detectables auto.

**IDIOMA (auditoría: 100/179 envíos = 56% salieron en INGLÉS por fallback):** templates SOLO en
ES/EN/IT/PT/AR. **DECISIÓN DEL USER 2026-07-13: NO agregar más idiomas** — todo lo que no sea esos 5 →
enviar en INGLÉS. Se QUITÓ el skip de idiomas foráneos (antes de/fr/nl/ja/tr con html lang se salteaban
y quedaban pending; ahora se mandan en inglés). `db?`=22 envíos usaron draft de DB sin idioma resoluble
(draft borrado o language NULL) — pendiente revisar si molesta.

**BARRIDO SISTÉMICO DEL POOL (commits 53e6e7a, 0e5a630) — feedback user "hay CIENTOS que ya se sabe no van":**
El pool VIEJO (pending) entró antes de endurecer filtros y no se re-evalúa → cientos de no-publishers
conocidos (pinterest/esselunga/bancos/retailers) seguían en Prospects. Buscarlos a mano NO escala.
`sweepBlockedFromProspects(token)` (enganchado en el loop tras runSuspectRejectAnalysis): gated por config
`purge_blocked_prospects='true'`, barre pending de a 60/ciclo por cursor `purge_cursor_ts`, se AUTO-APAGA
al terminar. Borra SOLO por 2 señales de ALTA PRECISIÓN (0 FP): (a) blocklist curada isDomainBlockedFull,
(b) detector estructural nonPublisherType. NUNCA por Haiku/categoría → regla de oro. NO hard-DELETE:
status='rejected' + suspect_reason='purge:...' (auditable/reversible). +40 marcas/plataformas/retailers/
bancos globales al BRAND_BLOCKLIST (roblox/esselunga/mytheresa/zalando/shein/bbva/santander/skyscanner/...).
SQL para encender/monitorear/auditar/restaurar: `sql/2026-07-13_purge_pool.sql`.

- **PENDIENTE user**: (1) correr `sql/2026-07-13_purge_pool.sql` → encender el barrido (INSERT config
  purge_blocked_prospects='true'), monitorear pending bajando, auditar lo purgado. El DELETE manual de
  sql/2026-07-13_prospects_cleanup.sql YA NO hace falta (el barrido lo supersede, salvo borderline como
  tradingview/bible/seznam que no son estructurales ni blocklist). (2) cargar zip **v574** (Desktop) al Store.
- **ZIP v574** recompilado en Desktop (commit 1ad7261): popup con fix Shopify + AD_SALES multilingüe.
  Manifest entero 574 (>573; Store publicó 572). Próximo = 575+.

**ESTADO AL CERRAR 2026-07-13 (noche):** user confirmó — INSERT `purge_blocked_prospects='true'` CORRIDO
(barrido andando, pool era 1252 pending) + zip **v574 SUBIDO al Chrome Web Store**. Todos los fixes de la
auditoría 48h están vivos (worker en Railway) desde la tarde del 13/07.
**RETOMAR A PARTIR DEL 2026-07-14** (pedido del user "analizar lo NUEVO que trae el agente para seguir
sacando conclusiones"):
**FIX EMAIL DISCOVERY BR + RE-ANÁLISIS RÁPIDO (commit 9b26d74, 2026-07-14):** caso sorteador.com.br —
aparecía SIN email en Prospects pero el popup encontraba contato@sorteador.com.br. Causa: en .br el email
vive en /contato, pero CONTACT_HINT (cosecha de links del home) no matcheaba "contato" (solo contact/
contacto/contatt) y /contato NO estaba en las rutas estáticas → el worker nunca visitaba la página de
contacto BR. Fix: +contato/fale-conosco/anuncie en CONTACT_HINT y en `paths`. runReenrichBadLeads
acelerado (batch 10→20, cooldown 15min→2min, sleep 1500→700) y con BARRIDO COMPLETO por cursor
`reenrich_cursor_ts` (antes miraba siempre los 50 más viejos y se apagaba antes de terminar) + traffic al
select. Disparador/monitoreo: `sql/2026-07-14_reanalisis_emails.sql` (flag agent_reenrich_bad_leads='true').

**AUDITORÍA COMPLETA DE LA TOOLBAR (5 agentes paralelos 2026-07-15) — ~26 fixes aplicados:**
Worker (commit 55fc740, 11 fixes): F1 2do-email cuenta para el cap (era ~2x envíos); F5 no enviar si la
reserva falla; F2 reporte frozen movido arriba del gate de finde (nunca corría); F4 AutoGoogle slot 8→10
(el 8 no corría por el gate 9-23); F3 heartbeat dentro de jobs largos (AutoGoogle/polishPool/runCsvQueue);
F6 counters de user en Madrid (eran BuenosAires+Z, -3h); F8 source-perf solo marca hecho si no falló;
Cost#1 RapidAPI ya no se DOBLE-CONTABA (flush RMW eliminado, RPC atómico único writer); Cost#2 findSimilarSites
ruteado por rapidFetchWithRetry (facturaba sin contar); Cost#6 Apollo cache 7→45d; D2 +Outbrain a adNetworks.
Popup (zip v577 commit 234ae81, 15 fixes): D1 informer tier 4→1 en _emailPickTierClient; D2 +partner networks
a los regex; D3 detección idioma ampliada; UI#1 form-status→form-estado (estado Monday se arrastraba); UI#3A
vista 7d usaba conteo de hoy; UI#3B/C labels import; Admin#3 _populateHourSelects TypeError abortaba el panel
del agente; Admin#1 comparador mostraba 0 emails/Monday; Admin#2 'Guardar TODO' no salvaba worker config y
corrompía el del agente; Admin#4 doble render pisaba contenedor; Admin#5 filtro user duplicaba; Admin#6 %bounce
mal denominador; C1 default tráfico 400K→350K.
NO aplicado (bajo impacto/riesgo, anotado): Cost#3 Apollo daily undercount (cap mensual ya protege), Cost#4
orden Haiku autopilot, Cost#5 cache Haiku in-memory, UI#2 código muerto Cascade (intencional), DUP1 7 mapas ISO,
DUP2/D4 inconsistencias inertes.
🔴 SEGURIDAD (docs/security-remediation.md) — NO auto-aplicado (son cambios de Supabase que pueden romper prod):
CRÍTICO: api keys en toolbar_config legibles por cualquier authenticated + signups Auth abiertos (anon key
público → internet-facing). ESTADO 2026-07-15: (1) ✅ SIGNUPS DESHABILITADOS por el user (cortó lo internet-
facing → ahora solo-insider, riesgo bajo). (2) ✅ RLS HARDENING aplicado (sql/2026-07-15_rls_hardening.sql):
config keys-del-worker (apollo/rapidapi/gemini) + caps globales = solo admin escribe; user_limits = cada MB
lee/escribe solo lo suyo (admin todo). Policies RESTRICTIVE aditivas, worker usa service_role (bypasea).
PENDIENTE (código, cuando el user quiera, bajo riesgo): (3) rutear Monday por el Edge Function api-proxy +
sacar fetchApiKeys → cierra la LECTURA de las 6 keys por los MBs (hoy la extensión lee la de Monday directo).
(4) rotar keys. Secretos actuales en config: apollo_api_key, rapidapi_key, monday_api_key(+3 per-user). Gemini
ya limpiado. Ver docs/security-remediation.md.

**AUTOGOOGLE nunca arroja portales — DIAGNÓSTICO (agente 2026-07-15, commit c6de812):** el código está
intacto y corre cada ciclo (maybeRunAutoGoogleSlot @ loop, slots 8/11/14/17/21 Madrid L-V, halt 400). ROOT
CAUSE: **Serper devuelve 403/429** (plan FREE = 2500 créditos DE POR VIDA, no mensuales — casi seguro
agotados) y el error se tragaba a [] → "cero silencioso" por semanas. Encima contaba las fallidas contra el
"cap mensual" (AUTOGOOGLE_MONTHLY_CAP=2500) → se auto-bloqueaba. FIXES deployados: _serperSearch devuelve
{domains,ok,status}+timeout 10s; cuenta SOLO éxitos; escribe `autogoogle_last_error` en config. **ACCIÓN
USER (única forma de reactivarlo): revisar dashboard serper.dev / logs Railway y recargar créditos o rotar
SERPER_API_KEY.** Ver error: `SELECT key,value FROM toolbar_config WHERE key LIKE 'autogoogle%'`.
ALERTA "worker sin actividad 5 min" = FALSA ALARMA en gran parte: lee auto_session_stats (progreso de
sesión autopilot), NO el heartbeat real (auto_heartbeat_at @ loop top). Se prende cuando autopilot flag=true
pero el worker está ocupado en CSV-queue/polish/etc en vez de una sesión. FIX (zip v576): el popup ahora
cruza auto_heartbeat_at → si el worker está vivo muestra "en cola detrás de otro trabajo" en vez de alarma.
SERPER CONFIRMADO AGOTADO (user: "credits left -3"). User compró 50k créditos ($50). AUTOGOOGLE SPEND
INTELIGENTE deployado (commit 0fef13e): cap mensual CONFIGURABLE (config `autogoogle_monthly_cap`, default
2500) + THROTTLE por rendimiento (`autogoogle_fresh_rate` rolling = dominios nuevos/búsqueda → pool saturado
= menos búsquedas, no quema créditos; piso 8) + `autogoogle_stats` visible. Cuenta SOLO éxitos. Ver:
`SELECT key,value FROM toolbar_config WHERE key LIKE 'autogoogle%'`. NOTA: aún con Serper OK, el yield a
Prospects era ~0 (los dominios encontrados se filtran downstream por el piso de tráfico 350K) — evaluar con
autogoogle_stats si conviene. FILTROS PROSPECTS: BUG 3 NO era bug (hay normalización auto_feeder_*→sellers_json/
autopilot/monday_refresh en index.js:5865). BUG 4/5/6 arreglados (zip v576): chips geo-unfiltered, cache append,
fecha offset AR -03:00. DETECTOR POPUP sincronizado con worker (veto publisher-ads + todos los schemas). Zip = v576.

**FILTROS PROSPECTS auditados (agente 2026-07-15, zip v575 commit 9e6fa90) — 2 de 6 bugs arreglados:**
BUG1🔴 filtro por USUARIO (Maxi/Diego/Agus/Agent) NO filtraba (el <select> oculto no tenía las options →
.value no pegaba → pool completo siempre) → FIX: agregadas las options. BUG2🔴 "No pending candidates" +
"1000 leads·pág1/20" a la vez (la nav de paginación, sibling, no se limpiaba en el path de 0) → FIX. Los 4
que quedan (menor prio, decidir): BUG3 verificar `select distinct source from toolbar_review_queue` matchea
los botones (autopilot/monday_refresh ✓); BUG4 chips de continente van a 0 al elegir 1 país + truncado si
pool>3000; BUG5 contador del tab vs "X leads" (cache de orden por slot 30min); BUG6 filtro de fecha ~3h de
desfase TZ (timestamp naive en supabase.js). Zip v575 = 575 (>574; Store publicó 572). Popup detector AÚN
sin sincronizar (veto publisher-ads/schemas nuevos) — el aviso "not recommended" del toolbar manual está atrás.

**VETO PUBLISHER-ADS + TAXONOMÍA (commits 0916d8f, 2741a8b — 2026-07-15):** FALSO POSITIVO allhiphop.com
(medio hip-hop con tienda de merch) purgado como nonpub_ecommerce → causa: `isStore` se disparaba "aunque
tenga ads" porque criteo (retargeting de TIENDAS) contaba como ad. FIX ARQUITECTÓNICO (idea del brainstorm):
`hasPublisherAds` = AdSense/GPT/SSP/prebid/Taboola (ad-tech que un PUBLISHER corre para vender su inventario;
EXCLUYE retargeting criteo/adsrvr/adform que usan tiendas) → VETA toda la detección estructural. Ahora TODO
el schema no-publisher (bank/edu/gov/health/service/inst/saas) se rechaza SOLO si `!hasPublisherAds` → un
medio que vende inventario NUNCA se excluye (regla de oro uniforme). Taxonomía en `docs/exclusion-taxonomy.md`
(brainstorm de agente). User confirmó sumar TODOS los grupos 'excluir' EXCEPTO: **herramientas online**
(calc/conversores/PDF/acortadores), **marketplaces de assets** (stock/fonts/templates/NFT) — esos SÍ sirven —
y **developer/técnico** (sensible, muchos corren ads). Agregados por schema: serviceSchema (LocalBusiness*),
instSchema (iglesias/museos/bibliotecas), saasSchema (SoftwareApplication/WebApp/MobileApp), piracyRe
(magnet/.torrent/putlocker — brand-safety, aunque tenga ads). PENDIENTE si el user quiere más: corporate-
brochure y transaccionales-por-keyword (payment/hosting/VPN) → hoy los deja a Haiku (FP-prone). El BARRIDO
re-corriendo (cursor reseteado) levanta service/inst/saas/piracy al reiniciar el worker.

**AUDITORÍA 11-15/07 (hecha 15/07, SQL en sql/2026-07-15_analisis_agente_11-15.sql) — VALIDÓ los fixes:**
75 envíos/5d (~5/día/MB, cap OK). Basura de email (whois/técnico/placeholder/depto) = CERO después del
13/07 (13/07 fue día de transición, ahí aún cayeron axa/stepstone/compo/admiralmarkets/wizink/bbva). Del
14/07 en adelante target = casi todo medios reales. Rol comercial MULTILINGÜE ya pega: reklam@paraanaliz(TR),
salg@dagens.no, onlineadvt@vikatan, comercial@catracalivre, ventas@pauta. Rebotes: 22 (11 reenviados a alt,
11 congelados sin-alt, 0 fallidos → bug .ok muerto). ERROR hallado y arreglado (commit 89ec7cf): "owner@"
bare (WHOIS/informer) rebotó 4/4 (cnnturk/expansion/arealme/vetogate) — EXEC lo puntuaba +90 → ahora reject.
Fuente `informer` = casi toda basura (owner@/domainmanagement@/bancos) — ya en tier 1; si molesta en data
nueva, evaluar DESACTIVAR informer como fuente de envío. PENDIENTE menor: informer+freemail→reject (caso
baladag4 rudnypc@gmail); +fcbayern al BRAND_BLOCKLIST; leaks ecommerce (beck-shop/justspices/ruparupa) los
levanta el barrido.

RETOMAR 14/07:
  1. Auditar el BARRIDO: `sql/2026-07-13_purge_pool.sql` query #3 (status='rejected' AND suspect_reason
     LIKE 'purge:%') → verificar que NO se voló ningún medio real; contar cuánto bajó pending desde 1252.
  2. Analizar los ENVÍOS NUEVOS del agente (created_at >= 2026-07-14) con TODOS los fixes activos: ¿mejoró
     la calidad de target (menos bancos/marketplaces/marcas) y de email (menos WHOIS/IT/placeholder,
     mejor rol comercial)? Comparar contra la tanda vieja (que tenía ~1/3 no-publishers y varios emails malos).
  3. Emails elegidos: ahora SÍ hay data en toolbar_email_picks (tabla creada 13/07) → auditar chosen vs
     descartados con `sql/2026-07-11_auditoria_48h.sql` sección 2.

## ⚠️ toolbar_email_picks — CREADA RECIÉN el 2026-07-13 (NO antes)
El resumen viejo decía que el user corrió el CREATE ("SUCES") pero la tabla NO existía en la base
`ticjpwimhtfkbccchfyp` (la query de auditoría tiró `relation "toolbar_email_picks" does not exist`).
Se creó de verdad el **2026-07-13**. Consecuencia: **el backup de emails elegidos arranca 2026-07-13**;
todo lo que el agente mandó ANTES no quedó registrado (los inserts fire-and-forget fallaban en silencio,
sin romper envíos). Para auditar "qué email eligió vs descartó" hay que esperar ~2 días de data nueva.
Las otras 2 auditorías (canceladas por contenido / envíos por día) SÍ tienen histórico completo.

## ⚠️ VERSIÓN DEL MANIFEST — ahora es ENTERO (573), NO `5.x`
El Chrome Web Store tiene publicada la versión **572** (esquema entero, de una subida vieja del user).
Rechaza cualquier `5.1.x` porque lo lee como **menor** que 572. El 2026-07-09 se subió el zip como
`573` (commit `d182d0c`, `manifest.json` version="573"). **La PRÓXIMA subida debe ser 574 o mayor**
(entero puro), nunca volver a `5.x`. El zip vive en `~/Desktop/adeq-toolbar-chrome.zip` (42 archivos:
manifest/config/background/docs/icons/modules/popup — SIN auto-prospector/sql/.git). El nº de versión
del manifest NO afecta al worker (Railway), solo a la extensión manual.

## 📍 Estado al 2026-07-09 (tarde) — v573 / detector no-publisher ALTA PRECISIÓN, 0 FP validado

Commit `4e82272`. El user dio una lista de ~500 sitios "que SÍ sirven" (publishers reales, LATAM/ES,
en `A.rtf`) como calibración. Testeando el detector contra ella se detectaron **falsos positivos graves**
(bloqueaba clarin/chequeado/kiwilimon como ecommerce por `"@type":Offer` suelto y `cart/basket/price`
sueltos). Se endureció + un **agente revisor adversarial** encontró 6 issues más (todos aplicados):
1. `NON_PUBLISHER_TITLE_RE`: sacadas palabras editoriales (precios/gobierno/ministerio/municipal/
   universidad/facultad) que rechazaban diarios por el `<title>` (Ámbito por "Gobierno", El Cronista por
   "precios"). 2/4/6. Detector: schema.org @type Y keywords SOLO cuentan si `!hasDisplayAds` (programmatic
   O red partner Taboola/MGID/Ezoic/Seedtag/Teads) → un publisher con ads NUNCA se rechaza aunque embeba
   schema de hotel/producto que reseña. 5. WooCommerce/Magento salen del 1-hit (medios WP usan Woo para
   paywall) → se cazan por carrito+checkout. 3. Prompt Haiku: tie-break invertido a "ante la duda,
   publisher". + Haiku: tipo `marketplace/realestate` (idealista/fincaraiz/encuentra24/corotos).
- **isStore** (plataforma dedicada Shopify/VTEX/Nuvemshop/PrestaShop/etc. O botón add-to-cart+checkout)
  sigue SIN gatear → tiendas se atrapan igual (dafiti/leroymerlin/defacto/beyoung ✓).
- **Validado en vivo: 37 publishers de la lista → 0 falsos positivos** (finance-news, travel-blogs,
  education, recetas, deportes, gaming, música, autos-contenido). idealista/banorte/armas → los caza Haiku.
- El user avisó que su lista "que SÍ sirve" tiene ruido (incluyó falabella/banorte/dak/dafiti por error —
  "puedo fallar"). Y que idealista/fincaraiz NO sirven (inmobiliarias) → van a Haiku marketplace.
- Popup v5.1.1: mismo detector endurecido. Zip v5.1.1 en `~/Desktop/adeq-toolbar-chrome.zip`.

### 🔍 AUDITAR EN 48H — "cancelados automáticamente" (pedido del user para afinar)
```sql
SELECT domain, error_message, updated_at FROM toolbar_csv_queue
WHERE status='skipped' AND error_message LIKE 'not_publisher:%'
ORDER BY updated_at DESC LIMIT 300;
```
Motivos: `not_publisher: nonpub_<tipo>` (regex estructural) | `haiku_<tipo>` (IA) | `title_nonpub`.
Si aparece un publisher real ahí → ajustar. La lista `A.rtf` de sitios-OK quedó como referencia de calibración.

---

## 📍 Estado al 2026-07-09 — v5.1.0 (blocking + email discovery + paridad extensión + backup)

Commits `e7f8f39` + `12aa5cc` + `cc8d386` pusheados a main (Railway auto-deploy). El grueso vive en `auto-prospector/index.js`; el port a la extensión en `popup/popup.js` + `popup/popup.html` (zip **v5.1.0**, copia dejada en `~/Desktop/adeq-toolbar-chrome.zip`).

**PORT A LA EXTENSIÓN (toolbar manual, v5.1.0):**
- **Aviso NO bloqueante** "⚠️ Website not recommended for prospecting (\<tipo\>) — not a content publisher" cuando el MB analiza un sitio no-publisher (tienda/banco/uni/viajes/ONG). Detección en el content-script (`runPageContext` → schema.org @type + carrito/home-banking/admisiones/reservas/donaciones). Contenedor `#nonpub-notice` en popup.html. El user lo pidió: "aunque el MB nunca va a abrir bancos, que le diga sitio web no recomendado".
- **Prioridad email comercial** (paridad worker): `_emailPickTierClient` + `_bestEmailByTier` → apollo/informer > publicidad@/comercial@/ventas@ > persona > genérico. Aplica al orden de `renderEmailList` y al autofill de Monday (antes era solo "Apollo primero"). Regex `_AD_SALES_LOCAL_RE`. (Apollo ya tenía `_rankApolloTitle` por rol — sin cambios.)

**BACKUP DE DECISIONES DE EMAIL (pedido del user):** el worker, cada vez que elige email, guarda en `toolbar_email_picks` TODOS los candidatos detectados (`{email, source, score, tier, bounced}`) + cuál eligió. Fire-and-forget (nunca frena el envío). Permite al MB comparar en SQL su elección vs la del agente y sacar estadísticas. SQL: `sql/2026-07-09_email_picks_backup.sql` (CREATE TABLE + RLS + queries de análisis). **⚠️ El user tiene que correr ese CREATE TABLE una vez** o los inserts fallan silenciosamente (fire-and-forget, no rompe nada).

**Filtro estructural de descarte (user: "bloquear URLs por TIPO de web, cómo está construido, no geo/tráfico/categoría").** Decisiones del user (AskUserQuestion): rigor = **Balanceado** (rechaza con señal comercial clara o veto IA; ante duda real, pasa); grupos always-block = **Comercio + Instituciones + Servicios/viajes** (NO forzó foros/agregadores). Defensa en 3 capas:
1. `fetchPageContent` → detector `nonPublisherType` por schema.org @type (fuerte=1 hit) + keywords de intención (débil=2): ecommerce/bank/education/travel/nonprofit/service. También devuelve `{dead:true}` si el DNS no resuelve (ENOTFOUND) — distinto de bloqueo/timeout.
2. `classifyPublisher` → rechazo TEMPRANO de `nonPublisherType` ANTES de la señal de ads (tiendas/bancos corren retargeting+ads.txt y se colaban). La monetización YA NO basta sola: **Haiku actúa como VETO** sobre lo monetizado (user: "si no analiza la IA se da cuenta que no va"). Categoría de medios FUERTE (PUBLISHER_CATEGORIES/SW news) sigue siendo fast-path sin gastar IA.
3. `_haikuPublisherClass` → taxonomía destilada de TODOS los ejemplos del user horneada en el prompt + vocabulario ampliado (bank/travel/nonprofit/service). Ejemplos: leroymerlin/defacto/beyoung=tienda, n26/bbva=banco, ipsos=service, carwow/holidayautos=alquiler, andalusiaegypt/urlaubsguru=hotel, tommys.org=ONG, ouedkniss=marketplace. Validado 12/13 casos, **0 falsos positivos** sobre publishers reales (meganoticias/clarín pasan).
- Dominio muerto (DNS) → skip en `processCsvItem` (no termina en Prospects sin email): gdpr.tubi.tv, 2chblog, medatixx, fr.wix.

**Email discovery (user: "garantizar que agarra sí o sí lo escrito en la web", +latencia OK). Fuentes elegidas: páginas internas + datos estructurados.**
- `scrapeEmailsForDomain` ahora en 3 FASES: (1) home → **COSECHA los links de contacto/impressum/publicidad/about REALES del HTML** (mismo dominio, cap 16) aunque el sitio los nombre distinto; (2) sigue descubiertos PRIMERO + rutas estáticas ampliadas (Impressum/Kontakt DE, mentions-legales FR, contatti/pubblicita IT, media-kit) con early-stop; (3) WHOIS/informer solo si CERO emails. (JSON-LD/Cloudflare/mailto/de-ofuscación ya estaban en extractEmailsFromHtml.)

**Mejor email = ROL COMERCIAL/PUBLICIDAD (elección user Q4; ADEQ vende pauta).**
- `rankEmail`: nuevo tier `AD_SALES` (+95, por encima de EXEC) para publicidad@/comercial@/ventas@/marketing@/ads@. Regex `AD_SALES_LOCAL` hoisteado a módulo (acotado: NO matchea admin/advisor).
- `_pickTier(email,source)` reemplaza a `_sourceHardTier` en los 2 sorts de selección final: apollo/informer nominal (4) > **publicidad@ scrapeado (3)** > persona scrapeada (2) > genérico (0). Honra regla del dueño (decision-maker verificado manda) + elección del user.

### ⏳ PENDIENTE USER
1. **Correr `sql/2026-07-09_email_picks_backup.sql`** (CREATE TABLE toolbar_email_picks) en Supabase (proyecto ticjpwimhtfkbccchfyp) — SIN esto el backup de emails no se guarda (falla silencioso, no rompe envíos).
2. **Cargar zip v5.1.0** (`~/Desktop/adeq-toolbar-chrome.zip`) en Chrome — trae el aviso "not recommended" + prioridad email comercial en la toolbar manual. (Los fixes de BLOCKING/EMAIL del agente ya están vivos por deploy, no dependen del zip.)
3. **Reenviar la lista de sitios que SÍ sirven** (las 2 capturas NO cargaron: >8000px de alto). Pegar dominios como texto o cortar en 2-3 pedazos. Sirve para calibrar umbral y evitar falsos rechazos. (El user dijo "WEBSITES EJEMPLOS QUE SI SIRVEN" pero las imágenes no llegaron.)
4. Verificaciones SQL 48h del 2026-07-08 (Apollo/bounce/idioma) siguen vigentes — ver abajo.
5. Cap agente 10/día: los 21 de Maxi son casi seguro manuales+agente o un override en `agent_max_per_day_by_user`, NO bug (el cap está enforced). Confirmar si molesta.

---

## 📍 Estado al 2026-07-08 — v5.0.99 (sesión larga: calidad del AGENTE de emailing)

Antes: recuperación de caída Supabase (Micro se quedaba sin Disk IO → 522; restart lo levantó) + optimización de consumo (cache config, circuit breaker anti-retry-storm, worker ya no hace idle-exit) + blindaje anti-costo (fusible RapidAPI 250/min, eliminado agent_test_mode). Ver [[project_supabase_infra]].

**Análisis de 98 envíos de julio → 3 problemas de calidad, todos arreglados:**
- **Idioma ~50% mal** (pt a checo, es a polaco, en a alemán). Raíz: bonus de acentos compartidos disparaba es/pt/it para idiomas no soportados. Fix: acento solo cuenta si hay stopword; no-soportado → INGLÉS. Soportados = {en,es,pt,ar,it}. Fix en worker Y popup (3 detectores).
- **Elección de email**: prioridad por tier duro apollo/informer > scrape > generic (antes el ranking dinámico podía poner un genérico arriba).
- **Emails rotos**: `_stripScrapePrefix` cortaba la 1ra letra de palabras capitalizadas (Search→earch). Eliminado. gmail/hotmail YA NO se descartan.

**Bounce-retry — bug CRÍTICO del `.ok`:** sendGmailServer devuelve `{id,...}` en éxito (sin `.ok`), pero queueBounceRetry chequeaba `sent?.ok===true` → marcaba TODOS los re-envíos como "failed" aunque se enviaban. 310 intentos, 0 `sent`. Consecuencia: Monday NO se actualizaba + se congelaba el lead. Fix: éxito = `sent.id`. + rescate con Apollo forzado + reconcileMondayBounces (flag `agent_reconcile_monday_bounces`) para corregir los 161 históricos.

**Apollo — bug CRÍTICO ($60/mes, 0 emails en 240 lookups):** findBestApolloEmail pagaba el reveal de `people[0]` (random/junior, ej. "Home & Garden Reporter") y se rendía. Fix: rankea por CARGO y prueba hasta 3 empezando por senior. + FILTRO de rol (user 2026-07-08): SOLO revela publicidad/marketing/programmatic/comercial/dev/tech/decisión; NUNCA periodistas/editores/ops. Guarda `title` en cache para medir.

**Borradores:** 3 nuevos (B1 encargado anuncios / B2 video / B3 display+video) × 5 idiomas, sincronizados en templates.js (baked) + seed_default_drafts.sql (DB que ve el MB en Analysis). Agente los manda **33/33/33 uniforme** (pickAnyTemplate usa solo DB drafts, sin ponderar) + `agent_claude_percent=0`.

**Otros:** blocklist +70 adtech/monetización (applovin, taboola, criteo, ezoic, vidoomy, revcontent...); botón "🔍 Escanear esta página" en Analysis (captura emails JS-renderizados del DOM, ej. livestly.com/contact — el worker por fetch NO los ve). Re-engagement por no-respuesta REVERTIDO (Monday hace el follow-up).

### ⏳ VERIFICAR EN 48H (2026-07-10) — pack de queries en el chat
1. **Idioma** de los envíos (¿respeta local?), muestra de a qué emails, fuente del contacto.
2. **Apollo ¿sirve o se cancela $60?**: `apollo_calls_month` (pagos) vs `toolbar_apollo_cache` source=worker_unlocked con email + title. Si gasta y saca ~0 emails → cancelar (Apollo no tiene data del segmento).
3. **Bounce**: `toolbar_bounce_retries` status='sent'>0 (antes 0). Reconciliación: failed→reconciled.
4. **Reproceso 76** (bounce-frozen descongelados + reenrich): emails nuevos.

### ⏳ ACCIONES USER
1. **Cargar zip v5.0.99** (botón escanear + fixes idioma popup). Worker ya en Railway.
2. **Reactivar spend cap** de Supabase (seguridad de costo; no era la causa de las caídas).

### ⚠️ Límites honestos (no bugs)
- Emails JS-renderizados: el agente autónomo no los ve (fetch, sin headless browser). El popup sí con el botón.
- Apollo en publishers chicos/regionales/no-ingleses: puede no tener data (lo dirá la medición 48h).

---

## 📍 Estado al 2026-07-01 — v5.0.90 (sesión muy larga, v5.0.73→v5.0.90)

Commits pusheados a main. Highlights de esta sesión (2026-06-30 noche → 2026-07-01):

**Worker — motores desbloqueados (causa raíz encontrada):** el gate de saturación contaba
buffers diferidos (waiting_pool+next_day) → backlog "permanente" >150 → feeder skipped_saturated
25/25 y AutoGoogle 0 búsquedas. Fixes: `_getCsvQueueBacklog` cuenta solo pending+processing;
CSV_QUEUE_HALT_HIGH 150→250 (pending clava en 200); AutoGoogle con vía propia (halt 400);
carriles por fuente (PER_SOURCE_ACTIVE_CAP: sellers 250/majestic 150/adstxt 120/monday 120/
autogoogle 180) + WAITING_POOL_CAP 700 con rollover capeado. AutoGoogle SÍ tiene SERPER_API_KEY
en Railway (confirmado) — lo bloqueaba el gate, no la key.

**Emails — paridad worker=dashboard + Apollo forzado:** extractEmailsFromHtml porta el deobfuscador
completo del popup (TLD 2→10, data-attrs, mailto). Re-enrich: scrape gratis SIEMPRE + Apollo
PAGO forzado (forceUnlock) cuando el scrape viene vacío. Penalty -30 a emails legales
(datenschutz/legal/privacy/dpo). Fix bug fuente del dato (_normSrc: email_sources objeto→string
rompía el ranking dinámico). NO había ni un envío Apollo en semanas; ahora sí.

**Prospects (UI):** emails PRIMERO en las páginas + filtro Type (All/⚠️Alert/✉️No Email); ⚠️ de
sospechosos (worker analiza 3×/sem L/X/V contra rechazos → suspect_reject); filtro 🤖Agent;
GEO count↔filtro unificado (_rowISOs); caja de descartar visible + aprende por CONTENIDO (fetch
del sitio en vivo, NUNCA geo/temática); sacado botón "Completar" (worker trae email); fix email
manual→Monday; atajos teclado E/R/Enter; no ocultar lista al cap; source honesto (adstxt→autopilot).

**Import manual (v5.0.86):** un import del MB (uploaded_by≠worker@autofeeder) SIEMPRE trae la web
aunque se haya borrado de prospects. Los 6 filtros de calidad (pageviews/categoría/geo/publisher/
discovery/freeze) SALTAN para import manual. Solo lo bloquea deal Monday ACTIVO (_isMondayBlocked)
+ blocklist. bypassFilters = isManualImport || monday_refresh.

**Cap agente:** 25→10/día por MB.

**3 AUDITORÍAS exhaustivas (agentes) — TODOS los bugs cerrados:** A1 (crash Cascade), A2 (race
contador RapidAPI: flush ya no hace read-max-write, el RPC atómico es la fuente de verdad), A3,
M1/M2/M4/M5, B5/B6/B7 (Monday NaN blanqueaba columnas)/B8 (nitter caído). Código muerto B1/B2/B3/C1
removido (13 funciones + 3 handlers). ÚNICO no-tocado: M3 (tráfico mín Autopilot fijo 400K sin
select UI — NO es bug, guardado con ?.). 

### ⏳ ACCIONES PENDIENTES DEL USER
1. **Subir zip v5.0.90** a Chrome (UI). Railway ya deployó (confirmado).
2. **Verificar que los motores se destrabaron** (correr post-deploy):
   - `SELECT status,COUNT(*) FROM toolbar_csv_queue GROUP BY status;` (waiting bajando hacia 700)
   - `SELECT cron_at,status,effective_added FROM toolbar_feeder_runs ORDER BY cron_at DESC LIMIT 5;` (debería salir `ok`, no `skipped_saturated`)
   - `SELECT value FROM toolbar_config WHERE key='autogoogle_serper_used';` (>0 = AutoGoogle arrancó)
3. SQLs ya corridos: cap_10, reread flag, suspect_reject columns, limpiar_prospects. Análisis SQLs opcionales (analisis_worker_semanas, diagnostico) → correr y pegar para conclusiones.

### 📊 Diagnóstico observado (2026-07-01)
- Prospects tenía 1721 pending, 71% SIN email (498 con email). Respuestas ~1.5% (8/538 en 28d).
- Fuentes: csv 2165 (mal clasificado, ahora honesto), monday 824, sellers 331, autopilot 110, autogoogle 0.
- csv_queue: backlog 1886 (waiting 1367 + next_day 319 + pending 200) = el clog que frenaba todo.
- Se corrió limpieza de Prospects (borró 1670 pending + 2346 backlog) preservando caches (17498 traffic_cache).

---

## 📍 Estado al 2026-06-30 (sesión nocturna) — v5.0.75

Sesión larga 2026-06-30. Commits `b4ac35c` (v5.0.74) y `56da87c` (v5.0.75) pusheados a main. Cambios:
- **GEO filtro**: un continente ya no manda `_C_XX` al server (devolvía 0). Conteo de chips y filtro unificados con `_rowISOs/_rowContinents` (mapas a scope módulo) → count == filtrado.
- **Caja de descartar**: el textarea estaba en panel oculto → movido al cuerpo visible. Aprende por TIPO/CONTENIDO (NUNCA geo ni temática), y ahora INVESTIGA el sitio en vivo (fetch título/meta/headings → Claude). Worker: geoPenalty/userGeoPenalty/userGeoBonus = 0 (geo neutral).
- **Filtro Agent**: botón 🤖 Agent (created_by vacío = agente autónomo) + badge distingue Agent vs cargas manuales. Badge AutoGoogle agregado (faltaba).
- **Cap envío**: 25 → 10/día por CADA MB. Al llegar al cap ya NO se oculta la lista de Prospects (envío topeado en validateProspect=50).
- **Email hit-rate (worker)**: extractEmailsFromHtml = paridad con el extractor del dashboard (deobfuscación completa, TLD 2→10, data-attrs, mailto). Scraper: reintento en timeout, timeouts más largos, follow redirects, early-stop por email real, internas antes que WHOIS. Re-enrich: scrape gratis SIEMPRE (antes cortaba sin Apollo) + sweep completo.

### ⏳ ACCIONES PENDIENTES DEL USER (las dejé listas, faltan ejecutar)
1. **Recargar la extensión** (Terminal + Reload) para los fixes de UI.
2. **Redeploy worker a Railway** para hit-rate + paridad emails + geo-neutral. Para AutoGoogle: agregar env var `SERPER_API_KEY` (es lo único que le falta).
3. **Correr en Supabase** (post-redeploy): `2026-06-30_agent_cap_10_per_mb.sql`, `2026-06-30_reread_prospects_emails.sql` (dispara re-lectura de TODAS las webs sin email), y los de análisis cuando quiera: `2026-06-30_analisis_worker_semanas.sql` + `2026-06-30_diagnostico_autogoogle_autopilot_prospects.sql` (pegar resultado para sacar conclusiones).

### 🧠 Auditoría Prospects — hallazgos NO resueltos (dead code inofensivo, dejados a propósito)
- Snooze handler apunta a botón inexistente (`.pcard-snooze-btn`) → no-op. `snoozedSet` se fetchea y no se usa (1 HTTP extra/carga).
- `emailOptions`/`.pcard-email-radio` se construyen pero no se renderizan; fallback de getSelectedEmail nunca acierta (cubierto por chips).
- `_rebuildGeoFilterFromRows` declarada y nunca llamada. `quickScoreLead` corre por card pero el score no se renderiza ni ordena.
- 5 mapas ISO↔nombre duplicados (parcialmente unificados con `_rowISOs`). Límite server 3000 filas: con pool>3000 el filtro client opera sobre subconjunto truncado por created_at.
- Comentarios que contradicen el código: orden "estable por fecha" sigue usando shuffle para los "other"; "Sample 100 random" obsoleto.
- BLOCKED_CATEGORY (gov/uni/bank) sigue activo = filtro publisher-quality. El user dijo "no descartar por temática" pero esos no son publishers; lo dejé y lo anoté para confirmar si quiere quitarlo.

---

## 📍 Estado al 2026-06-30 (tarde) — v5.0.73

- **Versión actual:** v5.0.73 (saltó de v5.0.33; 40 versiones de avance hechas en otra PC)
- **Git:** working tree limpio, sincronizado con origin/main. Último commit `1ceede4` (Prospects paginado + orden estable + rechazo por tipo + gate cola)
- **Supabase:** TODOS los SQL ya corridos (confirmado por user 2026-06-30). La base es compartida entre PCs → no hay migraciones pendientes. NO re-correr nada.
- **Backend worker:** corre en Railway (proyecto Supabase ticjpwimhtfkbccchfyp)

## 🔭 Qué cambió v5.0.33 → v5.0.73 (por frente)

**Agente de envío (worker):**
- Cron-scheduled, multi-MB con cap 25/día PARA CADA MB (Maxi/Diego/Agus igual; se eliminó override per-user)
- 80/20 template/IA (config `agent_claude_percent=20`); template tracking por open-rate
- Pacing diario Apollo (random forzado consume cupo), rotación 50/50 Apollo/scrape
- Ranking dinámico de fuentes de email por open_rate×(1-bounce), ε-greedy 10%, fallback al default si sent<50 (`toolbar_source_performance`)
- Secuencia re-engagement FU2/FU3/FU4 (+11/+22/+33d), cancela si abrieron el original
- Bounce-resilient: hard/soft retry con email nuevo, scanBounces incluye Spam/Papelera, auto-reply OOO ya no blacklistea

**Calidad de leads / filtro de basura:**
- Botón rojo enseña al filtro: rechazo con motivo (`toolbar_autopilot_feedback.reason`) → reglas por CONTENIDO (ignora GEO)
- X-learn: agente aprende firmas rechazadas (categoría+traffic_bucket+geo)
- Filtro publisher (AdSense/ads.txt/Haiku), bloqueo marcas/gov/instituciones/placeholders/webmails persona
- Umbral de tráfico usa pageViews (visits×PPV) NO visits crudo; 2º chequeo de tráfico

**Detección de email:**
- Roles publicidad multi-idioma, redes sociales como fuente (Facebook/YouTube/Twitter), contact-form chip
- Cloudflare decoder + JSON-LD (ya venía de v5.0.33)

**Fuentes / discovery:**
- +307 sellers.json acumulados por continente (LATAM+video: Adsmovil, Connatix, Glomex, Primis, Unruly, Teads, SmileWanted...)
- AutoGoogle (Serper), ads.txt-graph, dedup canónico
- Keywords 12 idiomas +7.5k frases, país vía uule, cola hasta 1000
- Feeder stock subido 500 → 2000; autopilot always-on sesgado a similar-discovery

**UI / Admin:**
- Panel admin unificado (2 bloques: MB Manual vs Agente), quickview + daily digest por MB
- Notificaciones personales por MB (`toolbar_notifications`, bell UI)
- Filtros GEO dinámicos: continentes + multi-país + bandera + count; Prospects paginado, orden estable
- Response tracking real (`toolbar_response_tracking`, solo reply humano cuenta), panel rendimiento por motor
- Import: attempts log (`toolbar_import_attempts`), Monday lista negra + owner, on_conflict=domain, import progresivo
- Badge update apunta a Chrome Web Store (no GitHub)

**Infra/correctness:**
- RPC `lock_prospect()` con pg_advisory_xact_lock — elimina race entre MBs
- `toolbar_feeder_runs` log de cada cron con conversion_pct adaptativo
- Índices faltantes agregados; bug Apollo counter (contaba búsquedas gratis) arreglado

## ⏳ Pendientes externos
- Aprobación Chrome Web Store (estado a confirmar)

## 🎯 Posibles próximos pasos (sin confirmar con user)
- Verificar en producción que el ranking dinámico de fuentes esté aprendiendo (correr `2026-06-19_conversion_por_fuente.sql` / `2026-06-18_agent_diagnostic.sql` — son SELECT-only)
- Revisar a fondo el último commit v5.0.73 si hay que retomar ese hilo
- Pendientes viejos que pueden seguir abiertos: `pullMondayRecyclables()` real, degradación si RapidAPI >75%, script doble-click `Actualizar-toolbar.command`

## 🐛 Lecciones acumuladas (siguen vigentes)
- requestAnimationFrame NO dispara en side panel hidden → no usar para yields async
- Chunks recursivos sin await crean concurrencia oculta — await el primer call
- Saves blocklist con DELETE * nukean filas auto-pobladas → DELETE WHERE category=X
- `let` dentro de try crashea en catch (ReferenceError) — hoist al scope del loop
- Update en chrome://extensions NO actualiza unpacked — Terminal + Reload
- Autopilot = Majestic/discovery, no es navegación del MB (flag `auto_prospecting_enabled`)
