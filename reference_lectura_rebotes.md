---
name: reference-lectura-rebotes
description: "Cómo la toolbar lee los rebotes de Gmail: qué carpetas, cuántos días, y el punto ciego de Microsoft 365 que estuvo escondiendo el 75% de los rebotes"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-31T16:19:01.549Z
---

`scanBouncesForUser` en `auto-prospector/index.js`. Corre una vez por vuelta del ciclo, con la
cuenta de cada MB (service account + domain-wide delegation).

## La respuesta corta, para cuando el user vuelva a preguntar
**Lee Inbox, Spam, Papelera y cualquier etiqueta** — la query termina en `in:anywhere`.
Verificado empíricamente el 31/08: una búsqueda con `in:anywhere` devolvió un mensaje con
`labelIds: ["TRASH"]` sin pedir la papelera.

**El user no tiene que mover ni etiquetar nada.** Lo único que vuelve un rebote invisible es
**borrarlo definitivamente** (vaciar la papelera) antes de que pase una semana.

Dos ventanas distintas, no confundirlas:
- **7 días** (`newer_than:7d`) es cuánto Gmail mira hacia atrás. Hay dedupe por ID de mensaje.
- **90 días** es cuánto se mira NUESTRO registro de envíos para saber a quién le rebotó.

## El punto ciego que costó el 75% de los rebotes (arreglado 2026-08-31, commit cdd81a8)
La query exigía `from:(daemons)` **Y ADEMÁS** `subject:(lista de frases)`. Con que fallara una
de las dos, el rebote no existía — y fallaba SIEMPRE con **Microsoft 365 / Exchange**, que es
medio internet publisher.

Exchange titula el rebote **`Undeliverable: <asunto original>`**. La lista tenía `undelivered`,
que es OTRA palabra (Gmail no las une), y el texto reconocible —"couldn't be delivered"— va en
el **cuerpo**, no en el asunto.

Medido contra el buzón de Maxi, misma ventana de 7 días: **query vieja 2, query nueva 8**.
Ninguno de esos 8 figuraba en `toolbar_bounced_emails`. Y en la tabla, **492 de 523 rebotes de
90 días tenían `tipo` NULL**: los que sí entraban tampoco se clasificaban.

### Las cuatro capas del arreglo
1. Las dos condiciones pasan a **OR**. Se sumaron los prefijos que Exchange traduce
   (`unzustellbar`, `nicht zugestellt`, `onbestelbaar`, `niedostarczone`).
2. **Verificación por mensaje** (`_pareceRebote`), para que ensanchar no traiga respuestas de
   personas reales. No es otra lista de frases: se apoya en lo que un rebote tiene POR NORMA
   (RFC 3464) — `multipart/report`, `X-Failed-Recipients`, `Auto-Submitted`, o texto de rebote
   en el cuerpo.
3. **El fraseo de Exchange en el cuerpo**: `wasn't found at` = rebote duro; `isn't set up to
   receive` / `only accepts messages from` / `mail flow rule` = **bloqueado**. Antes caían en
   "desconocido" y el agente reintentaba contra el mismo lugar.
4. **Listas de distribución**: `comercial@` y `redazione@` —las que más buscamos— suelen ser
   grupos, y Exchange rebota nombrando a los MIEMBROS, no al grupo (`noviny@joj.sk` volvió como
   `dinkova@`, `vlkova@`, `mako@`). Ninguno figuraba entre los que escribimos y el filtro los
   tiraba todos. Ahora, si falla en un dominio donde escribimos a UNA sola dirección, se le
   imputa a esa. **Exigir que sea una sola es la clave**: adivinar con dos quema un contacto
   bueno para siempre (el error de las 606 direcciones de agosto).

## Quién se escanea ≠ quién envía (commit 79184f4)
El scan vivía dentro del bucle de `agent_enabled_users`. Al sacar a mgargiulo del agente el
31/08, su buzón dejó de escanearse **en el mismo movimiento** — y él sigue enviando a mano.
El daño no es perder un número: **la lista de rebotes es COMPARTIDA**, así que una dirección
que rebota en un envío manual y no se anota queda habilitada para que el agente le escriba
desde otro buzón, y se paga el rebote dos veces con dos reputaciones.
Ahora se escanea a **cualquiera que haya enviado en los últimos 30 días**.

⚠️ Y hay envíos que la toolbar NO registra: `riosvictor@gallito.com.uy` figura en la base con un
solo envío el 26/08, pero Gmail muestra otro el 31/08 que no está en `toolbar_agent_actions`
(a mano desde Gmail, o el follow-up que dispara Monday). Por eso la ventana de matcheo es de
90 días y no de 30.

## La regla
**Un `AND` entre dos listas heurísticas es un filtro que falla en silencio.** Cada lista tiene
sus huecos y el AND los multiplica. Si hay que ensanchar, ensanchar la búsqueda y verificar
DESPUÉS contra algo normado (una cabecera RFC), no contra otra lista de palabras.

Relacionado: [[reference_entregabilidad]], [[reference_criterio_email]],
[[reference_alertas_falsos_positivos]]


## ⭐ QUÉ ES UN REBOTE (regla del user, 2026-09-02)
**Rebote = UNDELIVERABLE: el mensaje no se pudo entregar.** Nada más. Cuatro formas reales que
NO comparten ni una palabra entre sí:
  Gmail EN   "wasn't delivered ... the address couldn't be found"  (la que más llega)
  Gmail ES   "No se ha encontrado la dirección"                    (550 5.1.1)
  transcript "permanent fatal errors ... User unknown"             (550-5.1.1 CON GUION)
  Postfix    "could not be delivered to one or more recipients"    (SIN código SMTP)
⚠️ **Buzón lleno es el ÚNICO transitorio**: la casilla existe, la semana que viene entra → NO se
quema. Todo lo demás (no existe, bloqueado por política) es permanente.
La pregunta que decide no es "por qué rebotó" sino **"¿tiene sentido volver a escribirle?"**.

**Los avisos llegan en el idioma del servidor del destinatario.** Hay UN set multilingüe
(`REBOTE_NO_EXISTE` 56 patrones · `REBOTE_INDELIVERABLE` 29 · `REBOTE_BUZON_LLENO` 18) que usan
la búsqueda de Gmail, la detección y la clasificación. Antes estaba escrito en los tres lugares
con listas distintas. Si aparece un idioma nuevo, se agrega ahí y vale para todo.
⚠️ Los tres bugs que aparecieron probando eran **una palabra en el medio**: "adresse EST
introuvable", "endereço não FOI encontrado", "mailbox IS full". Las regex tienen que tolerarla.

## 🚫 EL REBOTE NO FRENA NADA (regla del user, 2026-09-02)
Textual: *"No hagas pausa por rebote nunca, es normal el rebote hasta que acomodemos la
identificación correcta de scraping"*. Se sacaron los DOS frenos (pausa del dominio 12 h y freno
por buzón al 2%); quedan solo los avisos.
El motivo técnico le da la razón: la tasa se mide sobre 7 días, así que **frenar vacía el
denominador y la empuja para arriba** — la pausa se re-disparaba sola y dejó al agente 2 días en
cero. Y el techo del 3% se calibró cuando veíamos la MITAD de los rebotes: la tasa real es
5,5-6,5% estable. Un techo por debajo del piso real no es un umbral, es un candado.
El rebote se corrige **eligiendo mejor la dirección**, no apagando el buzón.

## Qué formas de email rebotan (medido, 30 días)
    local de 2-3 letras   19,0%   ← gp@ al@ v_t@ tld@ it@ = iniciales raspadas del texto
    nombre suelto          8,5%
    nombre.apellido        7,9%
    genérico info@         7,3%
    comercial              4,5%
    **editorial            0,0%** ← valida sacar redazione@/redaktion@ de la lista negra

## ⚠️ Cuatro cosas distintas en la misma tabla (corregido 2026-09-03)
`toolbar_bounced_emails` mezclaba, y **estar ahí bloquea una dirección para siempre**:

    509  rebote_smtp      el correo VOLVIÓ — la dirección no existe o nos bloquea
    124  verificador      MillionVerifier antes de mandar: nunca salió, es un envío EVITADO
      5  rebote_temporal  4xx: greylisting, buzón lleno → "ahora no", NO "nunca"
      2  autorespuesta    un "estoy de vacaciones" PRUEBA que el buzón está vivo

Las últimas 7 eran direcciones sanas bloqueadas de por vida, **una era `sales@adeqmedia.com`**
—nuestra propia casilla—. Ahora hay columna `evidencia` y **sólo `rebote_smtp` + `verificador`
bloquean**. `loadBouncedEmails` filtra por columna.

La distinción ya existía en el vigilante de reputación pero **por regex sobre el texto del
motivo**: cualquier `reason` nuevo volvía a colarse como rebote sin que nadie se enterara.
**Una condición que depende de que nadie invente un texto nuevo no es una condición.**

Dos trampas del clasificador, las dos encontradas probando (10/10 después):
- `\bsoft\b` **NO matchea** `smtp_bounce_soft`: el guion bajo es carácter de palabra, no hay
  borde antes de "soft". Va `soft(?![a-z])`, que además no se come "software".
- el código 4xx llega **dentro del detalle**, no al principio: `^4\d\d` no lo agarra.

## 🗑️ `toolbar_sendtrack.fu1_*/fu2_*` borradas (2026-09-03)
4.142 filas, **cero enviados en cinco meses**, y 3 de las 9 con fecha la tenían ANTES del
envío (un upsert reusaba la fila). Los follow-ups los manda el CRM
(`crm_board_automation_runs`). **Una copia en la sombra de algo que no es tuyo sólo sirve para
divergir**, y mientras existe invita a que alguien escriba lógica contra ella.
