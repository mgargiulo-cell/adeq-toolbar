---
name: reference-parte-diario
description: "Cómo está armado el mail del parte diario: las dos partes, de qué tablas sale cada dato y qué NO se puede medir"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-25T14:14:04.696Z
---

`parteDelDia` en `auto-prospector/index.js`. Sale 21h Madrid, uno por día, a
`security_alert_email`. Desde el 2026-08-25 va en **HTML de verdad** (`htmlPropio`
en `sendGmailServer`) además del texto plano.

⚠️ `htmlPropio` SOLO funciona con `esProspeccion: false`. En prospección las dos
partes del multipart tienen que decir lo mismo palabra por palabra — que difieran
es heurística de spam. Ver [[reference_entregabilidad]].

## Parte 1 — el agente
Las 5 métricas de [[reference_4_metricas]]. El conteo de envíos filtra
`details->>ui_origin=is.null` para dejar afuera lo manual: **son métricas del
AGENTE**, regla del user.

## Parte 2 — el trabajo a mano de cada MB
Tres registros distintos, y ninguno cuenta las tres cosas:

| Qué | Tabla | Ojo |
|---|---|---|
| el MAIL que salió, y a quién | **MONDAY** (board 1420268379, marca "Manual" en Comentarios) | ⚠️ el registro interno NO sirve, ver abajo |
| el SITIO que miró, con GEO | `toolbar_historial` con `source='manual'` | `media_buyer` es 'Agus'/'Max'/'Diego', NO el email |
| las cargas masivas | `toolbar_import_attempts` | por `user_email` |

**Por qué estaba vacío el primero:** `toolbar_agent_actions` tenía UNA sola policy,
la de SELECT. Ninguna de INSERT. `createManualSendTracking` venía siendo rechazada
por RLS desde siempre, el popup se comía el error con un `console.warn` y mandaba
el mail igual. Las 2.064 filas `action='sent'` que había eran TODAS del worker.
Y eso rompía además el píxel de open → el "Email Futuro" nunca se disparaba en
envíos manuales. Arreglado con dos policies estrechas (cada quien solo escribe a su
propio nombre) en `sql/2026-08-25_registro_envios_manuales.sql`, ya aplicado.

**El envío manual NO pasa por el worker**: sale directo de la extensión a la API de
Gmail (`modules/gmail.js`). Por eso no hay forma de medirlo del lado del servidor y
todo depende de que ese registro del popup funcione.

## 🔴 LOS ENVÍOS A MANO SALEN DE MONDAY, NO DE LA BASE (2026-08-26)
El parte del 25/08 le contó **2 envíos a Agustina cuando Monday tenía 18** suyos ese día. El
user lo detectó comparando. El envío manual NO pasa por el worker: lo hace la extensión, y
escribe en dos lados con confiabilidad muy distinta —
- **Monday siempre funciona** (es el trabajo principal; si falla, se nota).
- `createManualSendTracking` **falla en silencio** (el popup se come el error con un
  `console.warn` y manda el mail igual) → `toolbar_agent_actions` ve una fracción.

`_manualesDeMonday(token, dia)` lee el board una vez y lo usan LOS DOS bloques: los envíos del
día (agente vs mano) y la PARTE 2. Si Monday no contesta, el parte **dice** que el número es
parcial. **No volver a contar lo manual desde `toolbar_agent_actions`.**

El dueño en Monday viene a veces como mail y a veces como nombre ("Maximiliano Gargiulo") →
`_mismoMb` compara por las dos vías.

## Quién dio el alta: `created_by`
La columna ya existía y nadie la leía. `worker@autofeeder` = agente; el mail de la persona =
carga manual. En 7 días: 335 del agente, 130 de los tres MB.

## Títulos que pidió el user (2026-08-26)
"Buzón Prospects — Altas del día" · "Publishers Disponibles — Prospects" ·
"Envíos de hoy — agente N/M · a mano N".

## PARTE 2 — qué muestra y por qué (rehecha el 2026-08-25)
El titular son los ENVÍOS (web + email de cada uno). Lo demás es contexto, y cada número tiene
una razón de existir:

| Renglón | Para qué |
|---|---|
| URLs abiertas + su promedio de 14 días | "50" no dice nada solo; contra su ritmo sí |
| nuevas / ya conocidas | si trae material fresco o repasa lo mismo |
| +500k / -500k / sin dato | cuántas servían de verdad (`parte_piso_trafico`) |
| Aprovechamiento | de las que servían, ¿a cuántas escribió? |
| Sin contactar / retomables / deal vivo / ya contactados | el trabajo pendiente REAL |
| Toolbar abierta (min) | esfuerzo, con su límite dicho |
| **Cobertura de la jornada** | el titular de actividad |
| Pausas dentro de la ventana | la forma del día |
| Fuera del foco geográfico | DÓNDE prospecta, no cuánto |

### ⚠️ Tres trampas que ya se cayeron una vez — no repetirlas
1. **"Sitios mirados" NO es una decisión del MB.** La fila de `toolbar_historial` la escribe
   `runEmailScraper`, que corre al ABRIR la toolbar en una pestaña. Por eso Agustina figuraba
   con 50 sitios y 37 de EE.UU.: es navegación, no prospección. Se llama "URLs abiertas".
2. **La inactividad se mide contra la JORNADA, no contra la propia ventana.** Con el ancla
   vieja, quien trabajaba 20 min sin pausas salía "de corrido" y quien trabajaba 4h con dos
   pausas salía peor. Ahora el titular es la cobertura (`parte_jornada_horas`, 9h).
3. **"Listo para escribir" descuenta lo ya contactado.** De 25 que se le contaban a Agustina,
   11 ya estaban en `sendtrack`. Contarlos es acusarla de no hacer algo que hizo bien.

### Re-contacto: la regla, verificada 11/11 contra el board
Permite: **Ciclo Finalizado · Mail No Enviado · Descartado**. Bloquea todo lo demás (LIVE,
En Negociacion, Propuesta Vigente (T), PAUSADO, los tres Masivo). Ya estaba bien en
`MONDAY_BLOCKED_STATES`. Lo que faltaba era PERSISTIRLO: `guardarBloqueadosDeMonday` (1×/día)
pagina el board y guarda los dominios con deal activo en `monday_bloqueados`. Si esa lista no
está, el parte NO afirma que algo se puede retomar — dice que le falta el estado del board.

**Hora: la PARTE 2 va en hora de Buenos Aires** (son personas en Argentina); el resto del
parte va en Madrid (es la ventana del agente). Está etiquetado en el mail para no confundir.


## El PANEL de la extensión muestra lo mismo que el parte (2026-08-26)
La pestaña **Activity** se reescribió para reflejar el mail. Motivo del user: *"muchos datos,
poca claridad, realmente no la uso"* — y además **mentía**: "Emails manual 0" para los tres
el mismo día que Monday tenía 18 de Agustina, porque contaba desde `toolbar_api_usage`.

**La regla: una sola forma de leer el trabajo del equipo.** Si el panel y el mail dijeran
cosas distintas, ninguno de los dos sirve. Los dos leen lo manual de Monday
(`fetchManualSendsFromMonday` en `modules/monday.js` · `_manualesDeMonday` en el worker).

Se BORRÓ (443 líneas): conversión por fuente de email, comparador lado a lado, resumen
narrativo para 1:1, gráfico por día, export CSV del comparador. **No reponerlos sin que el
user los pida.**

`Agent` quedó ordenada por frecuencia de uso: encender/frenar arriba, tráfico mínimo y
envíos/día en el medio, GEOs y diagnóstico plegados en `<details>`.


## Los DOS mails diarios, definidos por el user (2026-08-27)
1. **El parte de los MB** (21h) — cómo trabajó cada persona + el agente. Ya existía.
2. **El resumen de salud** — ahora abre con el **BOLETÍN POR SECCIÓN** (`_boletinPorSeccion`):
   veredicto ✅/🟡/🔴 por sección (ENVÍO, DESCUBRIMIENTO por fuente, BÚSQUEDA DE EMAILS con
   sus motivos de fallo, COLA, MONDAY→cero, RE-TRABAJO, APIS) + "qué mirar" cuando está mal.
   El veredicto compara contra lo que la sección DEBÍA hacer (cupo, carriles, cola), nunca
   contra un umbral inventado. Después vienen las alertas nuevas, lo crónico en un renglón,
   y los ERRORES CONCRETOS con ejemplos de las tablas de diagnóstico.
**La regla sigue: UN resumen + UNA alerta por día, no tres mails.** El boletín vive DENTRO
del resumen, no aparte.
