# Reparto de responsabilidades toolbar ↔ CRM, y 5 cosas rotas del lado del CRM

Trabajo en el repo `adeq-toolbar` (extensión Chrome + worker de Railway). Vos trabajás en
`adeq-dashboard` (el CRM en console.adeqmedia.com). Auditamos qué hace cada uno y encontramos
trabajo duplicado y trabajo que no se hace. Esto es lo acordado y lo que te toca.

---

## 1. La línea que separa los dos sistemas

**Antes del primer mail = toolbar + worker. Después del primer mail = CRM.**

El criterio no es "quién lo hace hoy" sino **qué puede ver cada pieza que las otras no**:

| Pieza | Lo único que ella puede ver | Le pertenece |
|---|---|---|
| **Extensión** | el sitio abierto y un humano mirándolo | analizar el sitio, elegir el email, buscar teléfonos, generar el pitch a pedido, **mandar el PRIMER mail**, cargar la ficha |
| **Worker** (Railway) | miles de dominios que nadie mira, 24/7 | descubrimiento, calificación masiva, búsqueda de email/teléfono, primer contacto automático con plantillas, **leer los rebotes de Gmail** |
| **CRM** (vos) | **el registro completo y su historia** | estado y transiciones, cadencia y follow-ups, detección de respuestas, la consecuencia del rebote sobre la ficha, a quién no recontactar, los reportes del tablero |

Dos aclaraciones que cambian decisiones:

- **El mail inicial NUNCA sale del CRM.** Sale de la extensión al tocar "Enviar a ADEQ", o del
  worker cuando es el agente automático. El CRM manda follow-ups, el personalizado y el masivo.
- **Los rebotes los lee el worker, no vos.** No es por costumbre: el worker usa service account
  con domain-wide delegation y ve **todas** las casillas sin que nadie autorice nada, mientras
  que vos dependés de `crm_gmail_tokens`, que cada MB tiene que conectar y se puede vencer.
  El worker te reporta el rebote por `POST /api/crm/sync-toolbar` con `bounced_email` +
  `bounce_reason`, y vos aplicás la consecuencia sobre la ficha. **Vos sos el dueño del efecto,
  él es el dueño de la lectura.**
  Las respuestas sí las detectás vos (`respuestas-rapidas`), y funciona: 104 detectadas.

---

## 2. Lo que ya arreglé de mi lado (no lo repitas)

- El agente **le escribía en frío a clientes que ya nos pagan**: la lista de no-recontactar se
  guardaba a diario y sólo se leía para un informe. 22 casos, el último a `meteored.com` el
  02/09 09:17. Ahora `isDomainBlockedFull` la consulta antes de mandar.
- **Apagué `agent_reengagement_enabled`**: el worker duplicaba tu cadencia de follow-ups
  (37 mandados, 35 pisando FUs que vos ya tenías agendados).
- El worker reportaba el rebote **con el dominio del mail y no el del sitio** — creaba fichas
  falsas (`yahoo.fr`) y dejaba al sitio real sin marca. Arreglado; si no puede resolver el sitio
  ahora **no reporta** en vez de reportar mal.
- **Backfill de rebotes históricos: pasé de 3 a 247 fichas marcadas.** De 640 rebotes, 312 eran
  atribuibles a un sitio; de esos marqué 244. **No toqué 65 que ya tenían otro email vivo** en
  el CRM — pisarlos habría destruido un contacto bueno. Los 328 no atribuibles quedaron fuera.
- Puse techo diario y contador por motivo al gasto de Anthropic del worker.

---

## 3. Lo que te toca (en orden de riesgo)

### A) `crm_board_template_sends` tiene 0 filas — no hay registro de qué se mandó
```sql
select count(*) from crm_board_template_sends;              -- 0
select count(*) from crm_board_automation_runs
  where created_at > now() - interval '7 days';             -- 3.282
```
3.282 corridas de automatización en 7 días y **cero envíos registrados**. La tabla se escribe
desde `app/api/_utils/crm-board-mirror.ts`, `crm-board-engine.ts`, `crm-board/mail-masivo` y se
lee en `crm-board/stats` y `crm-board/detail`.

Encontrá por qué el insert no llega. **Sospecha principal: un error tragado.** `supabase-js` NO
tira excepción — `shouldThrowOnError` es false por defecto, así que un `try/catch` alrededor de
un insert es código muerto y un fallo vuelve como `{ error }` que nadie mira. Hay que
desestructurar `error` y, como mínimo, loguearlo.

Importa porque sin ese log **no se puede saber qué plantilla se le mandó a quién**: no hay forma
de medir cuál funciona, ni de evitar repetirle la misma a alguien, ni de reconstruir qué pasó
cuando alguien contesta.

### B) `mark-live-monday` está agendado pero Vercel no lo puede disparar
`vercel.json` lo agenda a las `0 10 * * *`. `app/api/cron/mark-live-monday/route.ts` exporta
**`POST` y no `GET`** (línea 106). El cron de Vercel pega con GET → 405 → **nunca corrió**.

Es el que sincroniza el estado `Live` con la facturación, y `marcarLive` **promueve y degrada**
justamente para que el tablero y el revenue no puedan discrepar. Si no corre, discrepan en
silencio y nadie se entera.

Agregá el `export async function GET` (que valide el secreto de cron igual que los otros 17) o
sacalo de `vercel.json` si ya no va. **Lo que no puede quedar es agendado y muerto.**
(`poll-finance-inbox` también exporta sólo POST, pero NO está en `vercel.json`: ese está bien.)

### C) El cron de rebotes falla ABIERTO
`app/api/cron/crm-board-bounces/route.ts:75-82`:
```ts
const { data: cfg } = await supabase
  .from('system_config').select('value').eq('key','crm_board_bounces_enabled').maybeSingle();
if (cfg) {
  const v = ...;
  if (String(v).toLowerCase().includes('false')) return ...skipped...;
}
// ↓ si cfg es null, SIGUE Y CORRE
```
El comentario dice *"por defecto queda encendido"*. Pero si la fila se borra **o si la consulta
falla** (que vuelve `{data:null}` sin tirar excepción), `cfg` queda null y el cron **corre** —
produciendo exactamente la doble lectura de las mismas casillas que se desactivó para evitar.
Hoy `crm_board_bounces_enabled = false` y por eso hay 247 rebotes marcados y no un lío.

Invertilo: **por defecto apagado**, y si no se puede leer la config, no correr. Un cron que ante
la duda hace trabajo de más sobre la casilla de otro es el peor default posible.

### D) Ningún cron tiene vigilante
Hay **19 crons** en `vercel.json` y nada chequea que hayan corrido. `mark-live-monday` lleva
quién sabe cuánto muerto y se descubrió leyendo el código, no por una alerta.

Del lado del worker esto ya existe y funciona: cada job llama a `saludPing(job, {status,
cadenciaMin})` al terminar —**también cuando no encontró nada que hacer**, que es la parte que
importa: un job que corre y no encuentra trabajo está sano, y si no late es indistinguible de
uno roto. Un barrido compara el último latido contra la cadencia esperada y avisa.

Armá el equivalente: una tabla `crm_cron_health(job, last_run_at, cadencia_min, status, detalle)`,
que cada cron escriba al terminar **con éxito o sin trabajo**, y un barrido que avise a
mgargiulo@ los que no latieron. Ojo con el umbral: derivalo de la cadencia declarada de cada job,
no un número fijo — un umbral fijo con crons de 3 minutos y de 15 días es una bomba de tiempo.

### E) Escribí la frontera de los rebotes donde se vea
Hoy la decisión "los rebotes los maneja la toolbar" vive **solamente** en el string de un
`return` adentro del cron. Que esté en el README o en un comentario de cabecera, y que el cron
diga en su primera línea quién es el dueño de esa lectura. Si no, en tres semanas alguien
prende el flag "porque parecía apagado por error".

---

## 4. Reglas que valen para todo lo de arriba

1. **`supabase-js` no tira excepción.** Desestructurá `error` en cada query y actuá. Un
   `try/catch` sobre una query es código muerto. Nos costó que `dominios-activos` devolviera 200
   con la lista sin filtrar cuando la vista de clientes fallaba: 129 clientes que facturan dados
   por libres, y la toolbar no podía notarlo.
2. **Ante la duda, no.** Si no podés leer la lista que decide a quién escribirle, cortá con 503.
   Devolver una lista vacía se lee como "no hay nadie bloqueado" y manda a prospectar todo.
3. **`.order()` en todo loop paginado.** Sin orden explícito Postgres no garantiza el mismo
   orden entre dos consultas, y mientras paginás alguien escribe: se repiten filas o **se
   saltean**. Una fila salteada en `dominios-activos` es un dominio que debía estar bloqueado.
4. **Verificá que cada reemplazo haya matcheado** antes de dar un arreglo por hecho. Reporté un
   fix que no estaba en el diff porque un `replace` no encontró el texto y no avisó.
5. **Probá de punta a punta.** Arreglé que el worker mandara `source:'agente'` y el endpoint lo
   tiraba en la puerta porque estaba hardcodeado: el bug siguió vivo con las dos mitades "listas".
6. **Nada se entrega sin su detector**, y el detector tiene que alertar sobre **lo que NO pasó**,
   no sólo sobre errores. Los tres casos de arriba (B, C, D) son todos el mismo patrón: algo
   dejó de pasar y no había nada que lo dijera.

## 5. Datos de contexto

- El board: **una URL = una fila**, índice único global sobre `lower(domain)`. Los tableros de
  negociación dicen dónde está, no son copias.
- 5 estados: `Propuesta Vigente` · `En Negociacion` · `Ciclo Finalizado` · `Live` ·
  `Personalizado`. `Personalizado` es un **disparador** (manda el mail del MB).
- Descanso de 40 días: sólo en la transición `En Negociacion → Ciclo Finalizado`.
- Mails: **ni sábado ni domingo**, y 13 h Madrid (= 8 AM Argentina, 6 AM México).
- Estado actual del board: 10.003 filas · 92 en negociación · 104 con respuesta detectada ·
  247 con rebote marcado.
