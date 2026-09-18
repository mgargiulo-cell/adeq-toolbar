---
name: project-plan-categoria
description: "El plan 'De buzones a personas' (2026-09-04): estado por fase, las 4 decisiones que esperan al user, y los números medidos que lo sostienen"
metadata: 
  node_type: memory
  type: project
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-05T00:27:58.307Z
---

Plan publicado: https://claude.ai/code/artifact/72fbf6a7-6ae3-4299-89ce-0ab2ac977340
Pedido del user (2026-09-04): *"actuá como analista en sistemas… armar un plan de subir de
categoría… está funcionando y no hay que romper nada… AVANZA HACIA EL FINAL."*

## La conclusión en una línea
**La máquina busca buzones y el media buyer busca personas — y los datos le dan la razón al MB.**

## Números que lo sostienen (todos medidos contra producción el 04/09)
- Hueco: **735 de 2.347** dominios en común (31%) donde el humano tiene email y nosotros no ese.
  **416 son personas con nombre.** 243 nunca se intentaron. 82 los vio el crawler y el ranking los tiró.
- En vivo, 48 del hueco con 33 rutas: el email humano está en el sitio en **7**; en **28 (58%)
  no hay ningún email publicado** → LinkedIn / patrón nombre.apellido@ / rol adivinado. 10 WAF.
- El crawler es bueno; **no tiene vector de personas** (autores, `/wp-json/wp/v2/users`, masthead)
  ni "rol adivinado + MX". polishPool rescata **1 de cada 11**; 1.174 esperando.
- Descubrimiento: **377 de 699** sitios de marzo nunca fueron vistos por ninguna fuente (54%).
  Polonia 86, Brasil 85, Sudáfrica 35, Japón 30, Corea 15, mediana 1M pv. AutoGoogle: 1.041
  frases, **97% con cero validated**; las que rinden son "medio + ciudad" y las huellas.
- Prospectabilidad: reglas por URL **0,4% FN** ✓. Puerta ads.txt: **56 de 699 (8%) publishers
  reales rechazados** + 71 ilegibles. **25–30% del pool no es publisher** (tiendas, turnos
  médicos, directorios, corporativos) — entran con ads.txt por todas las fuentes.
- Línea base: 960/2.811 pendientes sin email (34%) · 3,8% respuesta real · ≈15% rebote SMTP
  (el "26%" mezcla 85 veredictos del verificador) · Apollo 1,8% / 11,8%.

## Estado por fase
- **FASE 1 (ranking) — HECHA y deployada** (commit `13556d0`). Ver [[reference_criterio_email]].
  Falta sólo "bajar Apollo del tier 4" = decisión 2.
- **FASE 2 (personas) — PARCIAL, deployada** (commit "Tres vías nuevas para no perder el lead").
  ⚠️ **El vector de personas se probó y NO entró**: prototipo sobre 48 dominios del hueco (1
  acierto) y sobre 70 donde el patrón era deducible (0 aciertos). Lección: **la portada de un
  medio está llena de nombres con cargo que son los de la NOTICIA, no del staff** — infirió
  `donald.trump@dagsavisen.no` como "presidente". Cosechar nombres de un medio necesita
  páginas de equipo/masthead explícitas, y aun así el patrón del dominio rara vez se deduce.
  Lo que SÍ entró, medido: **rol adivinado + MX** (`_ROLES_POR_IDIOMA`, fuente `rol_mx`, flag
  `polish_rol_mx`, tope `polish_rol_mx_daily_cap`=60): el 32% de los 4.014 emails humanos del CRM
  es un rol estándar del propio dominio, y el 28% de los dominios donde no teníamos nada el
  humano los resolvió así. Y **casa editora deducida de dónde se imprimió el email** en polishPool.
  Pendiente: Serper para personas = decisión 3.
- **FASE 3 (prospectabilidad) — HECHA salvo decisión 1, deployada**: tres rechazos duros con
  **0 FN sobre 624 publishers reales** (schema Store sin `Product`, turnos médicos en 9 idiomas,
  título de tienda sin "oficial"), cazan 7 de 25 negativos. ⚠️ **"Sin señal editorial" NO puede
  ser rechazo: rompe al 4,8% de los publishers reales** (calendarios, códigos postales, recetas,
  juegos) — entró como DISPARADOR de Haiku (commit `bee17af`): `fetchPageContent` expone
  `sinSenalEditorial` (6 señales: `<article>`≥3, secciones de noticias≥4, fechas≥5, RSS, schema
  de medio, autor) y `classifyPublisher` pide Haiku aunque haya "categoría de medio + ads.txt
  ≥20" (el atajo por el que pasaban firmy.cz, houzz.es, sony.co.jp). Gasto bajo `llamarClaude`
  (puerta única con techo). Segunda puerta ads.txt = decisión 1.
  Commit `c234164` (mismo día, más tarde): (a) **la puerta grande no perdona al marcado fuerte**
  — `nonPublisherFuerte` (schema de entidad/título de tienda/turnos; NO el carrito: flamengo
  con VTEX era el único FN de 687) veta aunque haya ads.txt + tráfico; (b) **barrido del pool**
  `barridoNoPublisher`: cursor por created_at (`barrido_np_cursor`), 25 por vuelta, 75 s, tope
  `barrido_np_daily_cap`=300 (compartido con el techo de Claude), marca `suspect_reject` +
  `suspect_reason 'barrido: …'`, NO toca sin_ads_txt, unreachable ni `haiku_*`/`ia_*` (marcó
  moneyweb.co.za, diario financiero, como "bank" → revertido y excluido), flag `barrido_no_publisher`,
  ping `barrido_no_publisher`. Revertir: `suspect_reason LIKE 'barrido:%'`.
  ⚠️ **Bug encontrado por el replay**: `fetchPageContent` devolvía null para TODOS los sitios
  desde el 02/09 (`geo` inexistente, 30c0daec) — ver [[feedback_alertas_automaticas]]. Arreglado
  + detector `fetch_page_content` + test `tests/fetch-page-content.test.js` con `cargarWorker`.
- **FASE 4 (fuentes) — PARCIAL, deployada** (commit "AutoGoogle busca por huella comercial en
  13 idiomas"). ⚠️ Hallazgo: **no era la dieta, era la cobertura** — el motor ya repartía 85% a
  plantillas desde el 24-27/08; las frases de contenido SEO (0,2% validados) son sólo el 15%
  de exploración. Lo que faltaba: huellas comerciales sólo en es/pt/it/fr → ahora 13 idiomas;
  ciudades de br/pl/jp/kr de 9/6/6/5 a 35/25/20/16; sorteo de ciudad por ciudad y no por país.
  Semillas (commit `bee17af`): `similar_expansion` YA sembraba desde los que respondieron desde
  el 27/08 (no lo había visto); lo de hoy lo extiende a `_construirBusquedasPorSspRegional`
  (79 respondedores reales, 33 geos, Brasil 26, mezclados + validados de relleno) y a
  `discovery.js getSeeds` (módulo sigue SIN conectarse al worker). Majestic NO tiene semillas:
  `_feederPullMajestic` recorre el Majestic Million CSV por cursor (`majestic_cursor`) con filtros
  de TLD/nombre — "sembrarlo" no aplica. **sellers.json**: el feeder YA existía
  (`_feederPullSellers`, ~280 redes + descubiertas, 8 MB de tope por archivo, 231 leads/30d, 15
  validados); lo que faltaba era GOOGLE (108 MB, acepta Range) → `_feederPullSellersGoogle`
  (commit `c234164`): ventanas de 1 MB, cursor `sellers_google_cursor`, sólo ccTLD objetivo
  (`_TLDS_OBJETIVO_GOOGLE`, sin .za), pre-filtro `checkAdsTxt` ≥10 líneas (60 por corrida), cola
  `sellers_google_pendientes`, flag `sellers_google_enabled`, ping `sellers_google`, etiqueta
  `auto_feeder_sellers`. Medido: 95% desconocidos, 13% con ads.txt grande. Pendiente:
  `sellers_google_incluir_gtld`; Sudáfrica = decisión 4.
- **FASE 5 (extensión) — HECHA en el repo (commit `bee17af`, manifest 678), SIN publicar**:
  `popup.js` importa `rankEmail`, `_isGenericLocalPart`, `AD_SALES_LOCAL` de
  `../auto-prospector/lib/email.js` (una sola implementación; el tier por FUENTE sigue en el
  popup porque rankEmail no sabe la fuente; dentro del tier manda el puntaje; basura <0 va
  última). `lib/email.js` sin `Buffer` (TextEncoder, probado con el linter MIME sin Buffer).
  0 cambios de puntaje sobre 10.968 emails vs foto post-Fase 1. `tests/paridad-popup.test.js`
  (5 tests; 42 en total). **Zip reproducible: `scripts/empaquetar.sh`** (incluye lib/email.js +
  lib/geo.js, saca `key`, verifica 46 imports; `SALIDA=` para probar sin pisar el zip subido).
  Queda con copia propia el detector no-publisher del popup (aviso visual, no decide envíos).
  v677 publicada el 04/09 a la mañana; **v678 subida y publicada el 04/09 a la noche por orden
  del user** ("subí el nuevo zip a chrome") con `scripts/empaquetar.sh` + `scripts/cws-publish.py --publicar`.
  Las 2 verificaciones del CRM (formulario como email, template_id) → el user confirmó hechas.

## Las 4 decisiones — RESPONDIDAS el 04/09 (textual)
1. ads.txt: *"Si no tiene ads txt y tiene tags de Adsense activos puede entrar, la única regla. El resto no entra."*
2. Apollo: *"Pongamos a competirlo, pero usar sí o sí el 100% de las request, quemarlas sí o sí antes de terminar el mes. Por ahí estamos fallando en la elección del Apollo, por eso no son buenos los resultados. Revisar."*
3. Serper personas: sí, cupo 60/día.
4. Sudáfrica: sí, ZA con frases en inglés.

**IMPLEMENTADAS en commit `933a75e` (04/09, deployado):**
1. `hasAdSense` en fetchPageContent (`adsbygoogle.js|pagead2.googlesyndication|ca-pub-`); estado
   `adsTxt.state="adsense"` en puerta 0 (cola), classifyPublisher, scoreProspectable y pulido;
   sin puerta grande; marca visible `"⚠️ sin ads.txt · AdSense activo"` en `ad_networks`.
   Tests `tests/adsense-excepcion.test.js` (scoreProspectable real vía `cargarWorker`).
2. `_tipoDeEmailParaRanking`: apollo → rol/persona/generico (manual sigue "apollo"); popup igual.
   `APOLLO_SAFETY_MARGIN=0` (cap 2500). **`apolloQuemarCiclo`** (maintenance, después del barrido):
   si `used/cap < ritmo−0.10` o quedan ≤5 días → forceUnlock a pendientes por tráfico desc sin
   persona (sin email o sólo genéricos, sin fuente apollo, sin cache 7d), 30 por vuelta dentro de
   `getApolloUsageToday().limit`; marca `email_ultimo_motivo='apollo_sin_contacto'` a los que no
   dan nada. Flag `apollo_quemar_ciclo`, ping `apollo_quemar_ciclo`. Elección: `_JEFE_REDACCION_RE`
   → 2; dev/engineer/webmaster/digital/tech fuera de `_RELEVANT_RE`; `person_titles` + chief
   editor; `per_page` 8; `title` guardado en el resultado del unlock.
3. `_serperPersonaSearch(domain)` (2 consultas `"@dominio"…` fuera del sitio, sólo emails del
   dominio); en polishPool paso 5b antes de rol_mx, cuando no hay nada o sólo genéricos; cap
   `serper_personas_daily_cap`=60, `serper_personas_hoy`, `serper_personas_hechos` (≤600) en
   config; flag `polish_serper_personas`; fuente `serper_persona`.
4. `_PAISES_POR_IDIOMA.en=["za"]`, `_TLDS_POR_IDIOMA.en`, `_HUELLAS_EN`, mezcla + "en",
   `_CIUDADES.za` (10), frases `en` de ciudad, `.za` en `_TLDS_OBJETIVO_GOOGLE`.
   `_GL_FUERA_DE_FOCO` sin cambios (za nunca estuvo).

## (histórico) Las 4 decisiones que esperaban al user
1. ads.txt: ¿segunda oportunidad (tags de display / sellers.json) cuando falta? Cuesta el 8%.
2. Apollo: ¿deja de imponerse por ser Apollo y compite por resultado?
3. Serper: ¿cupo diario propio (~60) para buscar personas en Google/LinkedIn?
4. Sudáfrica: ¿país ZA permitido con frases en inglés? 35 de 699 positivos.

## Herramientas que quedaron (scratchpad de la sesión d0a1004c)
`harness/cargar-worker.sh` (carga index.js entero, 45 ms) · `harness/foto-rankemail.mjs` (foto y
diff sobre el pool) · A2/`vivo-hueco.mjs` + `hueco.json` (el set de 48 y los 735) ·
A3/`adstxt-marzo.mjs`, `url-rules-marzo.mjs`, `fp-muestra.mjs` (los 699 y la muestra del pool) ·
A1/`analisis2.mjs`, `sondas.mjs`, `picks-patron.mjs`. Si el scratchpad se pierde, todo se
reconstruye desde las dos bases en ~20 minutos.

Relacionado: [[feedback_alcance_del_job]], [[reference_apollo_alternativas]], [[project_north_star]],
[[reference_arquitectura]]
