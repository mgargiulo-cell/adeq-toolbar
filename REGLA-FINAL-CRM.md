# Regla final del reparto toolbar ↔ CRM, y 4 cosas del lado del CRM

Trabajo en `adeq-toolbar` (extensión Chrome + worker de Railway). Vos en `adeq-dashboard`.
Hicimos una auditoría con 5 agentes probando en vivo. Esto cierra el reparto y lista lo tuyo.

---

## 🔴 URGENTE, YA PASÓ: 353 follow-ups salieron a las 5 de la mañana

```sql
select count(*), min(sent_at), max(sent_at) from crm_board_template_sends;
-- 353 | 2026-09-03 03:14:58+00 | 2026-09-03 03:18:56+00   → 05:14 a 05:18 MADRID
```

Dos reglas rotas a la vez, las dos explícitas del user:

1. **La ventana es 13-17 hora de Madrid**, ni sábado ni domingo. Se eligió así porque 13 h
   Madrid = 8 AM Argentina, 7 en Colombia/Perú, 6 en México: es el único solapamiento entre las
   dos regiones. A las 05:14 no hay nadie despierto en ninguna de las dos.
2. **353 mails en 4 minutos desde 3 casillas** son ~88 por casilla en 4 minutos. Eso es huella
   de spam. En el worker el cupo es 20 por casilla y salen en **cinco turnos separados a
   propósito** (`AGENT_SLOTS = [13,14,15,16,17]`): mandar el cupo junto desde la misma dirección
   es exactamente lo que Gmail lee como envío masivo.

Los crons de Vercel corren en **UTC**. Para 13 h Madrid en horario de verano (CEST, UTC+2) el
schedule es `0 11 * * 1-5`, no `0 13`. Revisá todos los que mandan mail.

Y hace falta un **tope por corrida** (sugerencia: 20-25 por casilla, con los sobrantes al turno
siguiente). Quedan 503 `would_send` pendientes: si salen juntos se repite.

---

## LA REGLA FINAL — quién hace qué

**Antes del primer mail = toolbar + worker. Después del primer mail = CRM.**
El criterio no es quién lo hace hoy, sino **qué puede ver cada pieza que las otras no**.

### Lo que hago YO (no lo implementes de tu lado)

| | Por qué es mío |
|---|---|
| Descubrir, calificar y buscar email/teléfono | Es lo único que corre 24/7 sobre miles de dominios que nadie mira |
| Analizar el sitio abierto, elegir el email, generar el pitch a pedido | Es lo único con un navegador y un humano delante |
| **Mandar el PRIMER mail** (extensión y agente) | El inicial NUNCA sale del CRM |
| **Leer los rebotes de Gmail** | Service account con delegación: veo las 3 casillas sin que nadie autorice nada. Vos dependés de `crm_gmail_tokens`, que cada MB conecta a mano y se vence |
| **Buscar la dirección de reemplazo tras un rebote y mandarle** | Soy el único con descubrimiento de emails. Y un mail a una dirección nueva es un **primer contacto** |
| Aprender qué plantilla funciona, por ahora | Ver la nota al final |

### Lo que hacés VOS

| | Por qué es tuyo |
|---|---|
| **Estado y transiciones** | Sos el registro |
| **Detección de respuestas** | Tu filtro de auto-respuestas (cabeceras RFC + asuntos en 11 idiomas) no existe de mi lado |
| **Cadencia y follow-ups** | Los 10 pasos de `crm_board_cadence_steps` son tuyos |
| **La consecuencia del rebote sobre la ficha** | Yo leo el hecho, vos aplicás el efecto |
| **A quién no recontactar** | `/api/crm/dominios-activos` es la autoridad |
| **El mail personalizado y el masivo** | |

### Lo que NO se toca de cada lado

Yo **no escribo** `estado`, `replied_at`, `fecha_fu1`, `fecha_fu2` ni muevo fichas de tablero.
Verificado en el código: `replied_at` no aparece una sola vez en mi repo.
La **única excepción**: la extensión manda `deal_stage` cuando una **persona** lo eligió en el
formulario. Si alguna vez ves `deal_stage` llegando con `source:"agente"`, es un bug mío.

---

## LO QUE YA ARREGLÉ DE MI LADO (no lo repitas)

- **El agente le escribía en frío a clientes que nos facturan** — 22 casos. La lista de
  no-recontactar se guardaba a diario y sólo se leía para un informe.
- **El agente te pisaba el `estado`** en cada push, y **te pisaba la cadencia** con `+5/+10`
  hardcodeados copiados de Monday. Los dos, eliminados del payload.
- **El puente de rebotes nunca funcionó**: yo escribía `order=sent_at` sobre una tabla sin esa
  columna → HTTP 400 en el 100% de las llamadas. Por eso te llegaban tan pocos rebotes.
- **El reintento tras rebote usaba el dominio del CORREO, no el del sitio** — el bug que creó la
  ficha falsa de `yahoo.fr`. Ahora resuelve el sitio real y **te avisa la dirección nueva**.
- **El reintento pedía tu ficha y tiraba tu veredicto**: ahora respeta `enNegociacion` y
  `descansando`, así que ya no le escribe a alguien que vos moviste hace 3 minutos.
- **Reportar un rebote re-etiquetaba el envío del agente como manual** (no mandaba `source`).
- **Backfill**: pasé las fichas con rebote marcado de 3 a 247, sin tocar 65 que ya tenían otro
  email vivo.
- Y de la auditoría: un clasificador que le daba a cada sitio el veredicto del siguiente, dos
  agujeros de RLS, el botón OFF del agente que no apagaba, y el parte diario 9 días mudo.

---

## LAS 4 COSAS QUE TE TOCAN

### 1. La ventana y el tope de envío (lo de arriba). Es lo único con reloj.

### 2. Un push parcial no puede pisar cuatro columnas
`app/api/crm/sync-toolbar/route.ts`. Cuando te reporto un rebote mando sólo
`{domain, bounced_email, bounce_reason, source}`. Ese push cae en el mismo upsert que un alta:

| línea | qué hace con un rebote | consecuencia |
|---|---|---|
| `:322` | `row.fecha_contacto = fc ?? hoy` — **incondicional** | el "primer contacto" pasa a ser el día del rebote |
| `:344-346` | `recalcFollowUps(...)` sobre esa fecha | la cadencia se re-ancla al rebote |
| `:294` | `if (!row.email) row.contacto_formulario = true` | el rebote queda como "este sitio se contacta por formulario", y no vuelve a bajar cuando aparece un email nuevo |

Ya hay 2 fichas con `fecha_contacto` pisada así. **Un rebote es un EVENTO, no un alta.** Esos
tres campos sólo deberían escribirse cuando el push declara un envío.

### 3. Un rebote tiene que limpiar las fechas de follow-up
```sql
select count(*) from crm_board_prospects where email_rebotado is not null and fecha_fu1 is not null;
-- 247 de 247
```
Hoy la cadencia no las frena el rebote: las frena *"el prospecto no tiene email"*. El día que un
MB cargue otra dirección a mano, arranca sobre fechas ancladas al rebote. `runBoardEvent('email_bounced')`
ya se dispara en `sync-toolbar:421` — que limpie FU1/FU2 como hace `prospect_replied`.

### 4. La ficha tiene que devolver `replied_at` y `email_rebotado`
`app/api/crm/ficha/route.ts:56` devuelve estado, ejecutivo, email, geo, idioma, fecha_contacto,
descartado_at, grupo, board_id, pageviews. **No devuelve `replied_at`, `email_rebotado`,
`rebotado_at` ni `rebote_motivo`.**

Hoy es un canal de una sola vía: yo te escribo el rebote y no lo puedo leer de vuelta, y vos
detectás la respuesta —tu trabajo exclusivo— sin que yo pueda enterarme salvo por
`dominios-activos`, que se refresca cada 24 h y transporta *estado*, no *el hecho*. El MB ve la
ficha sin saber que ese prospecto ya contestó. Es una línea de tu `select`.

---

## REGLAS QUE VALEN PARA TODO ESTO

1. **`supabase-js` NO tira excepción.** Un `try/catch` sobre una query es código muerto.
   Desestructurá `error` y actuá. Nos costó que `dominios-activos` devolviera 200 con la lista
   sin filtrar: 129 clientes que facturan dados por libres.
2. **Ante la duda, no.** Si no podés leer la lista que decide a quién escribirle, cortá con 503.
   Una lista vacía se lee como "no hay nadie bloqueado" y manda a prospectar todo.
3. **Una lista de nombres exactos se rompe con cualquier renombre**, y el que renombra no se
   entera. Hoy archivé unas credenciales renombrándolas y quedaron legibles para cualquier
   usuario logueado, porque la policy que las tapaba comparaba por nombre exacto.
4. **Un dictamen negativo es un resultado.** Guardar sólo los hallazgos convierte cualquier job
   de revisión en un bucle que re-paga lo que ya sabe.
5. **Nada se entrega sin su detector**, y el detector avisa sobre **lo que NO pasó**, no sólo
   sobre errores. Un job que corre y no encuentra trabajo está sano — pero si no late, es
   indistinguible de uno roto.
6. **Latir DESPUÉS del trabajo, no antes.** Tus crons llaman a `latir()` apenas pasan la
   autorización. Eso caza el 405 (Vercel no lo llamó) pero no el 500 (reventó en la línea
   siguiente): un cron que muere haciendo su trabajo late en verde.
7. **Registrar que algo pasó va DESPUÉS de que pase.** Hoy encontré que la extensión creaba la
   ficha *antes* de mandar el mail: si Gmail fallaba, el prospecto quedaba marcado como
   contactado y no le escribía nadie nunca.

---

## UNA NOTA SOBRE EL APRENDIZAJE DE PLANTILLAS

El análisis decía que era tuyo y estoy de acuerdo **en el destino**, pero no todavía: cuando lo
miré, `crm_board_template_sends` tenía 0 filas y mi señal (`toolbar_response_tracking`) era la
única que funcionaba. Ahora tenés 353 filas, así que la balanza se está dando vuelta.

Mi lectura del inbox para aprender **no escribe estado en tu board** —verificado, `replied_at`
no lo toco— así que no te corrompe nada; es sólo una lectura de más. Cuando tengas unas semanas
de datos, la apago y te pido la señal a vos. Avisame cuando lo veas maduro.

---

Cuando termines, avisá y lo verifico contra la base, como la vez pasada.
