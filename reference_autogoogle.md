---
name: reference-autogoogle
description: "De dónde saca AutoGoogle sus keywords, el foco geográfico que fijó el user y el tope de páginas de Google"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-26T13:03:38.819Z
---

`autoGoogleSlot` en `auto-prospector/index.js`. Busca en Google vía Serper y mete los
dominios nuevos en la cola.

## 🔴 LA TABLA DEL USER: 99.888 keywords que nadie leía (arreglado 2026-08-26)
El pool salía SOLO de `keywordsData.js` — **7.549 frases** en el código. Mientras tanto
`toolbar_keywords` tenía **99.888 cargadas a mano por el user** en seis idiomas (44.941 en
español) y AutoGoogle **no abría esa tabla nunca**. El user lo vio en el parte: *"veo que ayer
usaste solo esas cuatro, y tenés miles de keywords en distintos idiomas"*.

`_muestraKeywordsDeLaBase(token, soloEspanol)` trae una **muestra al azar**, no la tabla:
100k frases en memoria en un worker que ya reinicia por RAM sería cambiar un problema por
otro. Offset aleatorio → cada vuelta lee una zona distinta y a lo largo del día se recorre
todo. Caché de 30 min. Cupos: es 1200 · pt 400 · it 300 · fr 300 · ar 200. **`en` queda
afuera a propósito** (regla vieja del user: sin inglés).

**Mezcla cortas y largas** antes de devolver. No es cosmético: una frase de 1-2 palabras trae
los portales grandes del tema; una de 4+ cae en el long tail, que es donde viven los medios
regionales que sí contestan. Si la muestra sale ordenada por id, una carga entera de frases
largas tapa a las cortas y el slot queda sesgado sin que se note.

Fuentes en la tabla: `import` (99.888) y `max_2026_08_26` (300 categorías que pasó el user).

## Tope: PÁGINA 2 de Google, no más
Llegaba hasta la 4. El user: *"siempre agarrar máximo pág 1 y 2, no más, porque no sirven"*.
Lo que Google manda a la página 3 de una búsqueda temática ya no son medios que monetizan.
`page: 1 + (yield % 2)`.

## Foco geográfico (textual del user, 2026-08-26)
> "Focalizar en países de América Central, Sur, Europa y Asia. Oceanía, USA, Canadá, UK y
> Rusia no."

`_GL_FUERA_DE_FOCO` = `us ca gb uk au nz ru by ie`, aplicado como filtro sobre
`_PAISES_POR_IDIOMA` al cargar. Es **lista NEGRA y no blanca a propósito**: una blanca habría
que actualizarla cada vez que se suma un idioma, y el olvido se traduce en un país que deja de
buscarse en silencio.

## Reparto del slot
45% huella comercial (`inurl:ads.txt` y similares — prueba directa de que monetiza) ·
20% ciudades secundarias · 10% pares por SSP regional · el resto temas del pool (exploración).
Selección: 65% por yield histórico (`toolbar_keyword_yield`) + 35% random.

El `hl` de la búsqueda se aparea con el IDIOMA de la frase (`_paisParaFrase`) — buscar una
frase en portugués con `gl=pl` es tirar el crédito.

Relacionado: [[reference_parte_diario]], [[project_pending]]
