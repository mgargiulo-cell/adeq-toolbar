---
name: reference-cola-por-enviar
description: "La cola Por enviar a Monday: guardar un prospecto en un click y mandarlos todos juntos. Cómo funciona y qué se verificó contra Monday real"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-02T12:27:38.471Z
---

Pedido del user el 2026-09-02: durante la migración de CRM los MB no pueden empujar a Monday y
si esperan **pierden horas de prospección**. Con esto siguen igual y guardan para después.

## Cómo funciona
**Guardar** — botón chico abajo a la derecha del verde "Send to Monday". UN CLICK.
Valida GEO, tráfico y formato del email (lo mismo que el botón verde) y guarda TODO lo que
Monday necesita: las columnas normales de `toolbar_review_queue` más `monday_payload` (jsonb)
con `estado`, `fecha`, `ejecutivo`, `idioma`, `traffic_text`, `mail_enviado`.

**Ver y mandar** — en Prospects, botón `📥 Por enviar (N)`. Lista con URL · email · GEO ·
idioma, tilde por fila y "Seleccionar todo". `🚀 Enviar a Monday` los crea; `Quitar` los
devuelve a `pending` (no borra).

Estado `por_enviar` en `toolbar_review_queue`. ⚠️ Verificado que **el agente no lo ve**: filtra
con `status=eq.pending` en 44 lugares y el único negado es `status=neq.rejected` (dedupe), donde
conviene que un dominio encolado no se re-descubra.

## ⚠️ NO exigir que el mail esté mandado
Lo puse al principio asumiendo que el mail sale igual y solo se pospone el CRM. **El user lo
corrigió**: *"lo mismo que enviar a Monday pero dejar todo pre-grabado en hold"*, en un click.
Pedir el mail antes rompe justo lo que esto resuelve.
Se anota `mail_enviado` y la lista marca con **✉︎?** los guardados sin contactar — que sea de un
click no significa que entren al CRM como contactados sin verse.

## Lo verificado contra Monday REAL (item creado y borrado)
Los 7 campos llegaron exactos: Deal, Email, Top Geo, Páginas Vistas, Idioma, Fecha Contacto,
Estado. Y **`ejecutivo` se guarda como nombre corto ("Max") y `resolveMondayPerson` lo traduce
al usuario de Monday EN EL MOMENTO DEL ENVÍO** — por eso no queda un ID viejo pegado.

⚠️ Dos formatos que hay que respetar (los dos me los comí en el test, no el código):
- `fecha` va en **ISO `YYYY-MM-DD`**. El formulario ya convierte con `toIsoDate`.
- `estado` e `idioma` van como **ÍNDICE** (`"4"` = Propuesta Vigente (T)), no como etiqueta.
  `_mondayIndexCol` hace `parseInt`; con texto da NaN, **omite la columna en silencio y Monday
  pone su default "Ciclo Finalizado"**. Un prospecto puede entrar como cerrado sin que nada avise.

## ⚡ El escaneo de contacto: 5,2s → 2,1s
`scrapeContactPages` prueba 65 rutas. Estaba de a 5 (13 olas) con timeout 4000ms, y
`Promise.all` hace que **cada ola espere a la más lenta**: una ruta colgada le cuesta 4s a las
otras cuatro. Ahora de a 14 (5 olas) con timeout 2500ms. **Ni una ruta agregada ni sacada.**
Medido: lafranceagricole.fr 5,2s→2,1s (las mismas 3 páginas), ronaldo7.net 2,3s→0,9s.
El **GEO no hace llamada propia**: sale de `topCountries[0]` del dato del tráfico.

## 🔁 Un cambio en la extensión NO llega solo
Pasó dos veces el 02/09: el user veía un cartel ya borrado y no encontraba un botón ya hecho.
El worker despliega solo por Railway; **la extensión no**. Hasta que la tienda apruebe, la única
forma de usar el código nuevo es `chrome://extensions` → Modo desarrollador → Cargar
descomprimida desde `~/Desktop/adeq-toolbar`, y desactivar la de la tienda para no mezclar.
Se confirma mirando el número de versión abajo en la toolbar.
No existe recarga remota: el badge de versión solo AVISA, no aplica.

Relacionado: [[project_crm_propio]], [[reference_chrome_web_store]]

## ⚠️ El dominio es ÚNICO en `toolbar_review_queue` (2026-09-02)
Guardar reventaba con `duplicate key ... toolbar_review_queue_domain_key` cada vez que el
sitio YA estaba en Prospects — que es el caso NORMAL: 2.395 pendientes + 712 congelados, casi
todo lo que el MB analiza ya pasó por el pool. Veía un error de Postgres crudo.

**No alcanza con un upsert.** `merge-duplicates` pisa todas las columnas del payload y dos no
son nuestras para pisar: `source` (la atribución por fuente con la que se decide en qué feeder
invertir — qlife.jp vino de "similar") y `created_at` (la antigüedad real en el pool). Se mira
primero y se actualiza sólo lo que el MB cargó.

Si el dominio ya figura contactado, lo dice con estilo propio (`.push-result.warn`): lo que
sale del otro lado es un primer contacto. No se bloquea —puede estar re-trabajándolo— pero un
aviso que se ve igual que un éxito no avisa nada.
