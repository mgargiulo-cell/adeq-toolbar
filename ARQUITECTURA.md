# Cómo está organizado esto

Mapa por áreas para no tener que leer 25.000 líneas cada vez. Actualizado 2026-09-02.

## Las tres piezas y qué hace cada una

| Pieza | Dónde corre | De qué es dueña |
|---|---|---|
| **Extensión** (`popup/`, `modules/`) | Chrome, del media buyer | Analizar el sitio que tiene abierto, elegir el email, mandar el pitch y cargar en el CRM |
| **Worker** (`auto-prospector/`) | Railway, 24/7 | Descubrir sitios, calificarlos, buscarles email, mandar el primer contacto, leer rebotes |
| **CRM** (`adeq-dashboard`, repo aparte) | Vercel | El registro: estados, tableros por MB, cadencia de follow-ups, respuestas |

Se hablan por **endpoints con `x-toolbar-secret`** (ver `reference_monday_apagado` en memoria).
Son **dos Supabase distintas a propósito**: la del pool (toolbar) y la del CRM (dashboard).

## Worker — `auto-prospector/`

```
index.js          el ciclo y todo lo que toca estado o red   (24.684 L)
lib/config.js     credenciales y URLs — la BASE, no depende de nada  (21 L)
lib/dominio.js    normalizar una URL a algo comparable       (233 L)
lib/email.js      A QUIÉN se le escribe                    (1.199 L)
lib/geo.js        país por TLD y prioridad geográfica         (66 L)
lib/idioma.js     en qué idioma está el sitio                (369 L)
templates.js      los pitches por idioma
discovery.js      fuentes de descubrimiento
keywordsData.js   3.490 frases para AutoGoogle
tests/            20 tests · `npm test`
```

El orden de dependencias es en un solo sentido, sin ciclos:

    config ← idioma
    geo    ← email
    (dominio y geo no dependen de nadie)
    index.js importa de todos

### Las áreas dentro de `index.js`

Están **entremezcladas**, no en bloques: por eso conviene buscar por nombre de función.

| Área | Qué resuelve | Funciones ancla |
|---|---|---|
| Descubrimiento | de dónde salen los dominios nuevos | `_feederPullMajestic`, `_feederPullAdsTxtGraph`, `descubrirRedesPorSellersJson`, AutoGoogle |
| Cola y pool | qué se analiza y en qué orden | `_injectIntoCsvQueue`, `_parkInBacklog`, `_drainBacklog`, `processCsvItem` |
| Calidad del sitio | ¿es un publisher que nos sirve? | `checkAdsTxt`, `classifyPublisher`, `scoreWebsite`, `detectLanguageRobust` |
| Bloqueos | a quién NO tocar | `isDomainBlockedFull`, `getAdminBlocklistWorker`, `guardarBloqueadosDeMonday` |
| **Email** | **a quién escribirle** | **`lib/email.js`** |
| Idioma | en qué idioma sale el pitch | **`lib/idioma.js`** |
| Dominio | normalizar URLs | **`lib/dominio.js`** |
| Envío / agente | cuándo y desde qué casilla | `runAgentForUser`, `pickDbDraft`, `_isOutsideActiveHours` |
| Rebotes | quién no existe y qué se reintenta | `scanBouncesForUser`, `queueBounceRetry`, `reconcileMondayBounces` |
| CRM | el puente con el dashboard | `pushToCrmPropio`, `_fichaDelCrm`, `fetchDominiosBloqueados` |
| Salud | qué se rompió y avisarlo | `saludPing`, `saludWatchdog`, `enviarResumenSalud`, `parteDelDia` |

### Por qué `index.js` sigue siendo grande

Tiene **44 variables mutables a nivel módulo** — contadores de gasto de APIs, cachés, fusibles,
marcas de slot. Partirlo entero obligaría a repartir ese estado entre módulos, y un contador
duplicado significa **un tope de gasto que deja de funcionar sin avisar**. Se extrae por área,
de a una, verificando cada paso.

**Criterio para extraer:** una función se puede mover si no toca estado mutable de módulo ni
hace red. Al 2026-09-02 se extrajeron **1.888 líneas** en 5 módulos. Quedan candidatas medidas:
calidad del sitio (~460 L), ads.txt (~172 L), fechas y turnos (~248 L).

**Cómo verificar una extracción** (los cuatro pasos, en orden):
1. `node --check` en los dos archivos
2. `npm test` — los 20 tests
3. cargar el módulo nuevo y ejecutar sus funciones con casos reales
4. **cargar `index.js` entero** con `main()` cortocircuitado — es el único que detecta una
   referencia colgada, porque `node --check` sólo mira sintaxis

## Extensión — `popup/` y `modules/`

```
popup/popup.js       11.598 líneas · la UI y sus flujos
modules/supabase.js   1.857 · toda la lectura/escritura a la base
modules/scraper.js      812 · buscar contactos en el sitio (65 rutas, de a 14)
modules/monday.js       334 · ⚠️ el nombre quedó: ya NO habla con Monday, consulta el CRM
modules/emailVerifier.js 597 · MillionVerifier
modules/traffic.js      494 · RapidAPI / SimilarWeb
modules/sellersJson.js  521 · sellers.json y dominios conocidos
modules/bannerDetector.js 431 · ¿tiene publicidad display?
```

`modules/monday.js` tiene **una sola puerta de salida** (`mondayRequest`) y está cerrada con
`MONDAY_APAGADO = true`: si alguna pantalla quedara sin migrar, **falla con un mensaje** en vez
de devolver vacío.

## Reglas de este proyecto que valen más que el código

1. **"No sé" nunca puede valer "no".** Si no se pudo verificar algo, se dice — no se asume que
   está libre. Ya costó caro nueve veces.
2. **Prohibido el fallback silencioso.** Un cero se lee como dato; un dato que falta tiene que
   verse como dato que falta.
3. **Nada se entrega sin su detector.** Y el detector avisa también de lo que NO pasó.
4. `supabase-js` **no lanza excepciones**: un `try/catch` sobre una query es código muerto.
   Siempre desestructurar `error`.
5. **`.order()` en todo loop paginado**, o se saltean filas mientras el cron escribe.
