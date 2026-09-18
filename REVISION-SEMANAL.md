# Revisión automática de la ADEQ Toolbar

Este archivo lo lee una rutina programada (martes y viernes) que arranca **sin contexto**. Todo lo que
necesita saber está acá. Si sos esa rutina: leelo entero antes de tocar nada.

El dueño es Maxi (mgargiulo@adeqmedia.com). Pidió esto el 18/09/2026, textual: *"¿no podemos hacer que
vos leas 1 o 2 veces a la semana este análisis y hagas las mejoras por tu cuenta? Porque si no cada
semana tengo yo que estar escribiendo los cambios."* Y en el mismo mensaje: *"procurá no romper nada y
solamente optimizar si hace falta, haciendo una revisión previa."* Las dos frases mandan por igual.

## 1. Qué es el sistema

Tres piezas, un solo repo:

- **La extensión de Chrome** (`popup/`, `modules/`, `background/`, `manifest.json`): la usan tres media
  buyers para analizar una web, ver si se puede prospectar y mandarle un mail.
- **El worker** (`auto-prospector/index.js`, ~30.000 líneas, corre en Railway y se despliega solo con
  cada push a `main`): descubre webs, les busca email, y el **agente** manda 20 primeros contactos por
  día por buzón (`sales@` y `dhorovitz@`).
- **La base** (Supabase, tablas `toolbar_*`). Hay un CRM aparte (`adeq-dashboard`, otro repo y otra
  base): **no es tuyo, no se toca**.

`auto-prospector/lib/email.js` y `lib/geo.js` los importan el worker **y** la extensión.
`ARQUITECTURA.md` tiene el mapa por áreas.

## 2. Qué podés y qué no podés hacer

Tenés una copia del repo. **No tenés** la base (salvo la foto de la sección 3), ni Railway, ni la
credencial del Chrome Web Store. Por lo tanto:

| Podés | No podés (dejalo anotado en el pedido de cambio) |
|---|---|
| Leer la foto de salud | Escribir en la base ni aplicar SQL |
| Leer y cambiar código, con tests | Publicar el zip de la extensión |
| Correr la suite (`cd auto-prospector && npm install && npm test`) | Verificar en producción después del deploy |
| Abrir un pedido de cambio (PR) contra `main` | **Empujar a `main`**: nunca, bajo ningún motivo |

Si un arreglo necesita SQL, escribí el archivo en `sql/AAAA-MM-DD_nombre.sql` y decí en el PR que hay
que aplicarlo a mano. Si tocaste algo de la extensión, decí **"requiere zip"**; no subas la versión
del `manifest.json`.

## 3. La foto de salud

```bash
URL=$(grep -o 'SUPABASE_URL: *"[^"]*"' config.js | cut -d'"' -f2)
ANON=$(grep -o 'SUPABASE_ANON_KEY: *"[^"]*"' config.js | cut -d'"' -f2)
curl -s -X POST "$URL/rest/v1/rpc/salud_snapshot" -H "apikey: $ANON" -H "Authorization: Bearer $ANON" \
  -H "Content-Type: application/json" -d "{\"k\":\"$SALUD_KEY\"}" -o /tmp/snap.json
```

`SALUD_KEY` viene en el mensaje que te disparó. Es de sólo lectura. **No la escribas en ningún archivo
del repo ni en el PR.** Si la respuesta es `{"error":"no autorizado"}` o no hay red, no adivines:
terminá diciendo exactamente eso.

Qué trae (`sql/2026-09-18_salud_snapshot.sql` es la definición): `health` (un renglón por job, con
cuándo corrió, su detalle y real/esperado), `feeder_runs`, la cola por estado con sus motivos de 24 h y
lo que está en error, `agente_7d` (acciones del agente por día, buzón y motivo; `a_mano` separa lo que
mandó una persona), `altas_7d` y `ultima_alta_por_fuente`, `metricas_diarias` (14 días), el gasto de
Claude, los veredictos de MillionVerifier y una lista blanca de la config. Horas en UTC.

## 4. Cómo se revisa

El método que funcionó las dos veces que se hizo a mano (07/09 y 18/09): **para cada número raro, ir al
código que lo produce y a la tabla de donde sale, antes de opinar.** Buscá, en este orden:

1. **Procesos parados.** En `health`, un job cuyo `last_run_at` es mucho más viejo que su
   `esperado_cada_min`. Ojo: `sellers_google`, `feeder_monday` y `autopilot_similares` dependen del
   feeder; mirá `feeder_runs` antes de declararlos muertos.
2. **El agente.** ¿Mandó 20/20 por buzón cada día hábil? Si no, `agente_7d` dice por qué
   (`skipped` + motivo, o `cycle_*`). Un día bajo sin ningún motivo anotado es un corte mudo: eso es un bug.
3. **La cola.** `cola_en_error` tendría que estar casi vacía. Un mismo motivo repetido todos los días
   es una falla de código, no mala suerte.
4. **Alarmas que se acusan solas.** Un job en rojo por un freno que el propio sistema puso a propósito.
5. **Números que no cierran** entre dos secciones que miden lo mismo.

Cosas que **parecen** fallas y no lo son (no las "arregles"):

- Feeder en `skipped_saturated`: Prospects pasó de 3.000 y frena a propósito. Entonces `adstxt`,
  `sellers_json` y `majestic` no producen: es una pausa, no una fuente muerta.
- `claude_techo` en 700/700 todos los días a la tarde: es el techo de gasto que fijó el dueño.
- Filas en `next_day` con `reintentar: ads_txt_no_verificable…`: tienen tope de 30 días.
- `sellers_discovery` con 0 de 1: informativo.
- Sábados y domingos el worker no trabaja (sólo sale el resumen de salud).
- `agent_enabled_users` no incluye a mgargiulo@: es a propósito.

## 5. Reglas del dueño. No son negociables

1. **Nunca se pausa el envío por rebotes.** Se informa, no se frena.
2. **No se toca `adeq-dashboard`** ni su base. Lo que haga falta de ese lado va como texto en el PR.
3. **Las decisiones de política son del dueño, no tuyas**: a quién se le escribe y a quién no, qué
   fuentes se prenden o apagan, topes diarios, qué estados del CRM bloquean, cuánto se gasta. Si los
   datos piden un cambio así, **proponelo en el PR con el número al lado y no lo implementes**. Hay tests
   que lo dicen en su mensaje ("es decisión del dueño"): si tu cambio rompe uno de ésos, frená ahí.
4. **Nada de jobs ni crons nuevos.**
5. **Claude no gasta más de lo que ya gasta.** La traducción del pitch es gratis (Google) y sigue así.
6. **"No pude averiguarlo" nunca es "no".** Un fallo de red, un timeout o una consulta que devolvió
   error no es un cero ni un veredicto. Este patrón ya costó más de diez bugs acá.
7. **Nada se entrega sin su detector.** Cada arreglo lleva un test que falla sin él. Si lo que arreglaste
   era silencioso, además tiene que quedar registrado o alarmar la próxima vez.
8. Los mails que escribe el dueño (plantillas, borradores) no se reescriben.
9. **Decisiones ya tomadas — no las vuelvas a proponer** (18/09/2026, textual: *"No freno ningún pool, que
   sume miles. No subo techo de Claude, no quiero gastar de más."*): el pool de Prospects crece sin tope
   aunque haya meses de stock (similar, AutoGoogle y el reciclado del CRM siguen inyectando con el feeder
   saturado: es lo que quiere), y el techo diario de Claude queda en 700 aunque se alcance a la tarde.

## 6. Cómo se entrega

1. Rama `revision/AAAA-MM-DD`. Commits chicos, en castellano, que expliquen el **porqué** con el dato.
2. **La suite entera en verde** antes de abrir el PR. Si un test viejo falla por tu cambio, leé su
   mensaje: muchas veces te está diciendo que no te corresponde.
3. PR contra `main`, en castellano, con esta forma y nada más:
   - **Qué estaba mal** — con el número de la foto que lo prueba.
   - **Qué cambié** — una línea por arreglo.
   - **Qué NO toqué y por qué** — decisiones del dueño, con tu propuesta y el dato.
   - **Qué hace falta desde la Mac** — SQL para aplicar, zip para publicar, algo para verificar.
4. Al dueño no le interesa la autopsia ("no me interesa saber por qué se ocasionó el error"): qué anda
   ahora, qué cambia, qué falta. La causa va en el commit y en el comentario del código.
5. **Si está todo bien, decilo en dos renglones y no abras nada.** No inventes trabajo: un PR de
   "mejoras" sin una falla medida detrás es ruido, y acá cada cambio al worker sale a producción.
6. Si no pudiste empujar la rama o abrir el PR, dejá el resumen y el diff completo en tu mensaje final.

Conservador siempre: ante la duda entre arreglar y proponer, proponé.
