---
name: reference-arquitectura
description: "Cómo está organizado el código por áreas, qué se extrajo del worker y el criterio y los 4 pasos para extraer más sin romper nada"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-02T19:07:05.300Z
---

El mapa completo vive en **`ARQUITECTURA.md`** en la raíz del repo, y una versión corta arriba
de `auto-prospector/index.js`. Acá va lo que no conviene volver a deducir.

## Las tres piezas
- **Extensión** (`popup/`, `modules/`) — Chrome del MB: analiza el sitio abierto, elige email,
  manda el pitch, carga en el CRM.
- **Worker** (`auto-prospector/`) — Railway 24/7: descubre, califica, busca email, manda el
  primer contacto, lee rebotes. **34 jobs.**
- **CRM** (`adeq-dashboard`, repo aparte) — el registro. Ver [[reference_crm_board_modelo]].

## El worker: por qué `index.js` sigue teniendo 25.000 líneas
Tiene **44 variables mutables a nivel módulo**: contadores de gasto de APIs, cachés, fusibles,
marcas de slot. **Partirlo de golpe obligaría a repartir ese estado, y un contador duplicado es
un tope de gasto que deja de funcionar sin avisar.** Se extrae por área, de a una.

⚠️ Las áreas están **ENTREMEZCLADAS**, no en bloques: las funciones de email iban de la línea
6.400 a la 18.400. No hay secciones que marcar — hay que agrupar. Por eso el mapa lista
**funciones ancla** por área, no rangos de líneas.

**Criterio para extraer:** se puede mover lo que **no toca estado mutable de módulo ni hace
red**. Medido: 155 de 381 funciones (4.775 líneas) cumplen.

## Ya extraído (2026-09-02) — 1.888 líneas en 5 módulos
    lib/config.js    21 L  credenciales y URLs — LA BASE, no depende de nada
    lib/dominio.js  233 L  normalizar una URL a algo comparable
    lib/email.js  1.199 L  a QUIÉN se le escribe (rankEmail y su cierre)
    lib/geo.js       66 L  país por TLD y prioridad geográfica
    lib/idioma.js   369 L  en qué idioma está el sitio

Dependencias en un solo sentido, sin ciclos: `config ← idioma`, `geo ← email`;
`dominio` y `geo` no dependen de nadie; `index.js` importa de todos.

⚠️ **`config.js` va PRIMERO.** El primer intento de extraer `idioma.js` se llevó la config de
Supabase entera, porque `detectLanguageRobust` tiene un fallback de red (le pregunta a Claude
cuando el texto es ambiguo) y el cierre transitivo la arrastró. Hubo que revertir todo.
**Extraer sin la base abajo produce módulos que arrastran medio archivo.**

Quedan medidas para seguir: calidad del sitio (~460 L), ads.txt (~172 L), fechas/turnos (~248 L).

⚠️ **`_bouncedCache` y `_rebotesPorDominio` quedaron COMPARTIDAS** a propósito: index.js las
llena desde la base, email.js las lee. Se exporta el **objeto**, no el valor — index.js muta su
contenido pero nunca reasigna la variable, que es lo único que ESM prohíbe a un importador.
Pasarlas por parámetro habría cambiado la firma de `rankEmail` y sus llamadores.

## Los 4 pasos para verificar una extracción (en orden)
1. `node --check` en los dos archivos
2. `npm test` — los 20 tests de `auto-prospector/tests/`
3. cargar el módulo nuevo y **ejecutar sus funciones con casos conocidos**
4. ⭐ **cargar `index.js` ENTERO** con `main()` cortocircuitado
   (`s.replace('main().catch(', 'false && main().catch(')` en una copia temporal)

**El paso 4 es el que importa**: `node --check` sólo mira sintaxis y NO detecta una referencia
colgada. En esta extracción encontró cuatro que el análisis estático se había comido
(`_GL_LOCAL_PARTS`, `_GARBAGE_DOMAIN_KEYWORDS` y las dos cachés). Un chequeo por regex falla con
las concatenaciones de strings y con las variables locales dentro de funciones.

## Referencia de que `rankEmail` sigue igual tras mover
    comercial@ / publicidad@   140      info@ (genérico)    55
    redaccion@ (editorial)     115      local de 2 letras  -30
    dmarcreport@ / noreply@     -1

## ⚠️ Consultas paginadas: dos bugs reales encontrados auditando (02/09)
- **`_refreshGeoPoolCache`** leía `limit=2000` de una sola pasada sobre 2.401 pendientes —el
  83%— y **sin `order`**, así que CUÁLES 2.000 cambiaba entre llamadas. Ese número decide si un
  país satura el pool (>25%) y frena el feeder: se calculaba un porcentaje sobre una muestra
  parcial y movediza, con numerador y denominador de poblaciones distintas. Ahora pagina con
  orden, y si falla **conserva el caché viejo** — uno a medias sub-cuenta y DESFRENA un GEO que
  sí está saturando.
- La **caché de tráfico** se lee con `limit=40000` sobre 33.956 filas. No trunca todavía; se le
  puso `order` y un aviso al acercarse, porque si lo pasa el informe diría que se midieron de
  verdad URLs que salieron de la caché.

**Cómo auditarlo:** listar los `limit=N` con N alto y cruzarlos contra el `count(*)` real de esa
tabla. Un `limit=50000` sobre 4.000 filas no es un bug; lo que importa es la distancia al tope.

## Verificar que una extracción no cambió nada
Además de los 4 pasos: **comparar la misma función ANTES y DESPUÉS con los mismos inputs**.
Se hizo con el detector de idioma — 5/5 idénticos. El `"en"` que devuelve con textos cortos
parecía una regresión y NO lo era: la versión vieja hacía lo mismo. Comprobarlo contra el
original antes de dar por bueno un síntoma raro.

Relacionado: [[reference_criterio_email]], [[feedback_lecciones_migracion]],
[[project_architecture]]
