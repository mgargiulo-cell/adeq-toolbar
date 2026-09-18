---
name: reference-entregabilidad
description: "Blindaje anti-spam del envío (MIME, linter, reputación) y los pasos de DNS/Workspace que solo puede hacer el user"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-26T12:03:11.554Z
---

Auditoría de entregabilidad del 2026-08-12 (commit 0766ba0). Consultar ANTES de tocar `sendGmailServer`, los templates o cualquier ruta de envío.

## Lo que ya está bien y NO hay que romper
Mandan por la **API de Gmail con impersonación** (service account + domain-wide delegation): salen por las MTAs de Google, con su IP y su DKIM. Es lo mejor posible. Y los templates están limpios de contenido spammy: cero emojis, cero `$`, cero "gratis", cero mayúsculas, un solo `!`, cero links.

## El blindaje que se construyó
- **`revisarEntregabilidad`** — el linter. Vive DENTRO de `sendGmailServer`, a propósito: si estuviera en los callers, una ruta nueva se lo saltearía por olvido. Frena placeholders sin resolver, emojis en asunto, gritos, caracteres invisibles, homóglifos, entidades HTML numéricas, líneas >990 octetos, adjuntos. **El correo interno (`esProspeccion: false`) queda exento de las reglas de estilo** — si no, el vigilante bloquearía sus propias alertas.
- **MIME**: `List-Unsubscribe` + `List-Unsubscribe-Post`, quoted-printable (la firma de Gmail viene en una línea de varios KB y violaba el RFC 5322), `From` con nombre, RFC 2047 partido en varios encoded-words (el árabe daba ~90 chars, el límite es 75), partes plain y HTML coherentes.
- **`_sanitizarFirma`** — saca scripts, iframes, handlers y `<img>` de terceros o de 1-2px. OJO con el regex del píxel: tiene que exigir que el dígito sea el valor COMPLETO, si no `width="120"` matchea como 1px y borra el logo legítimo.
- **El píxel de tracking está APAGADO** por default (`tracking_pixel_enabled`). Era el único recurso remoto del mail y su señal estaba contaminada (Gmail proxea todo; 23% era prefetch).
- **`vigilarReputacion`** — rebote >2% por buzón alerta, >3% del dominio PAUSA 12h. La reputación en Gmail es por DOMINIO: los tres buzones y el correo con clientes comparten adeqmedia.com.
- **`chequearAutenticacionPropia`** — chequeo diario de nuestro SPF/DKIM/DMARC por DoH.

## 🚫 El pie de baja VISIBLE está sacado — no reponerlo (2026-08-25, commit 31f4bba)
Existía un `_pieBaja` que agregaba al cuerpo *"Si preferís que no volvamos a escribirte,
respondé baja..."*. El user lo sacó: **llegaba DESPUÉS de la firma**, así que lo último que leía
la persona no era quién escribía sino un formulario de baja. *"Lo hace parecer no profesional ni
personalizado"* — un primer contacto que trae su propio opt-out se lee como envío masivo, y el
costo comercial pesa más que la comodidad del enlace.

**El opt-out NO se perdió, se volvió invisible:** sigue en la cabecera `List-Unsubscribe` +
`List-Unsubscribe-Post`, que es lo que Gmail y Yahoo exigen (RFC 8058) y lo que muestran como su
propio botón "Cancelar suscripción" arriba del mail. Cumplimiento intacto, cuerpo limpio.
`_pieBaja = ""` con el comentario puesto en el código. **No proponer volver a escribirlo en el
cuerpo**, ni "más cortito", ni "en gris chiquito". Se conecta con [[feedback_borradores_del_user]]:
el mail lo escribe Max y no se le agregan renglones automáticos.

## ⚠️ Lo que el user tiene que hacer y NO se arregla desde el código
**VERIFICADO el 2026-08-24 contra el DNS real: SPF, DKIM y DMARC EXISTEN y están bien puestos.**
El chequeo automático los reportó como faltantes por un bug propio (le pedía `.data` a un string
que ya era el dato) — ya arreglado. Lo único realmente pendiente es el punto 3.
1. **DKIM**: consola → Apps → Gmail → Autenticar correo → clave 2048 → publicar TXT → **y volver a apretar "Start authentication"**. Sin ese último paso Google genera la clave pero no firma. Es el error más común.
2. **SPF**: un ÚNICO TXT `v=spf1 include:_spf.google.com ~all`. Dos registros SPF se invalidan entre sí.
3. **DMARC — el `rua=` YA ESTÁ PUESTO.** Verificado contra el DNS real el 2026-08-26:
   `v=DMARC1;p=none; sp=none; rua=mailto:dmarc@adeqmedia.com; ruf=mailto:dmarc@adeqmedia.com`
   El user lo agregó y yo seguía pidiéndoselo con una nota vieja. **Antes de pedir un cambio de
   DNS, hacer `dig` — no confiar en lo anotado acá.**
   **El DMARC ESTÁ OK y el user lo dio por cerrado (27/08): "dijimos que ya estaba ok".**
   `p=none` NO es una falla: es la primera etapa deliberada de un despliegue de DMARC. El
   chequeo automático lo listaba como problema y avisaba TODOS LOS DÍAS — ya se sacó; ahora
   solo alerta si falta el registro o si no tiene `rua=` (sin reportes no hay con qué decidir).
   Endurecer a `p=quarantine` es decisión del user. **NO volver a pedírselo.**
   **SPF también verificado y correcto**, un solo registro y más estricto de lo anotado:
   `v=spf1 a mx ip4:67.217.62.48 include:relay.mailbaby.net include:_spf.google.com -all`
4. **Google Postmaster Tools** — el único lugar donde se ve la reputación real.
5. **Las tres firmas de Gmail**: sacar imágenes de terceros, agregar dirección postal.

## Decisión pendiente del user
Un **dominio separado para el cold outreach**. Hoy adeqmedia.com carga el riesgo de la prospección Y las conversaciones con clientes reales. Aislarlo protege lo segundo; en contra, parte de la credibilidad es escribir desde el dominio real. Se conecta con [[feedback_borradores_del_user]].

## ✅ LA TRAMPA DE LA FIRMA — RESUELTA DE RAÍZ (2026-08-27, commit 22c524b)
Frenó envíos reales TRES veces: 79 el 18-19/08, 68 el 25/08 y **88 más entre el 24 y el 27/08**.
Se venía arreglando pidiéndole a cada caller que no pasara `cuerpo` — y no funcionó, porque son
varios caminos de re-envío y basta que uno se olvide.

**Ahora se recorta dentro de `sendGmailServer`**, el único lugar por el que pasan todos: si el
texto del caller ya contiene la firma (la generamos nosotros, así que se detecta), se la saca
antes de aplicar las reglas de estilo. Con fallback a la primera línea de la firma por si Gmail
la re-renderizó. Probado: recorta la firma exacta y la re-renderizada, y **sigue detectando un
grito real en la plantilla** — el linter no perdió los dientes.

⚠️ Si vuelve a aparecer `linter:mayusculas:...`, mirar si la firma cambió de formato.

## Historia de la trampa (por qué el arreglo tiene que ser estructural)
Ya frenó envíos reales DOS veces (79 el 18-19/08, 68 el 25/08). Siempre igual:
`linter:mayusculas:MEDIA,ARGENTINA` — son "ADEQ MEDIA" y "BUENOS AIRES, ARGENTINA" de la firma
de Gmail, leídas como gritos de la plantilla.

**La regla:** `revisarEntregabilidad` recibe DOS textos y no son intercambiables.
- `body` = el mail COMPLETO (plantilla + firma + pie) → reglas ESTRUCTURALES
  (placeholders sin resolver, adjuntos, líneas >990 octetos, homóglifos).
- `cuerpo` = SOLO la plantilla que escribimos → reglas de ESTILO (mayúsculas, exclamaciones,
  largo, links).

**Antes de pasar `cuerpo`, preguntarse: ¿este texto lo armé yo recién, o es un mail que YA
SALIÓ?** Si es lo segundo, NO pasar `cuerpo`: el linter saltea el estilo (fail-safe) y conserva
lo estructural. Caso real: el Email Futuro reenvía `original_body`, y 124 de 125 filas de esa
cola tienen la firma adentro.

Ojo con los bugs LATENTES: ese camino no mandaba un mail desde que existía, así que el bug
estuvo dormido hasta que se arregló otra cosa y volvió a enviar. **Al revivir un camino de
envío, revisar qué le pasa al linter.**

Los bloqueos del linter ahora avisan EN EL MOMENTO (clave `linter-` en `_ES_PARO`), no en el
resumen de 72h.
