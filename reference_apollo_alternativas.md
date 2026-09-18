---
name: reference-apollo-alternativas
description: "Evaluación de las 21 APIs de RapidAPI como reemplazo de Apollo (2026-09-04): todas son Apollo raspado, y el problema real no es el precio sino que Apollo no tiene datos de nuestro pool"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-04T10:49:19.430Z
---

**2026-09-04.** El user pidió reemplazar Apollo (*"es muy caro"*) probando 21 APIs de
RapidAPI. Informe completo: https://claude.ai/code/artifact/a464f207-3098-4778-8d01-e36f2b29772b

## Lo que hay que saber para no repetir el trabajo

**Apollo cuesta $65/mes** y entrega ~134 emails útiles/mes (536 en May–Ago) → **$0,49 por
email**, con una varianza enorme: mayo $6,50, julio $0,16. **Su producción sigue el caudal
del pool, no su propia calidad.** Consumo: 12–235 créditos de 2.250 por ciclo.

**El hallazgo que importa más que el precio:** sobre 10 dominios reales del pool Apollo
acertó **3 de 10**. Repetido **sin filtro de cargo** para descartar que fuera nuestra
consulta: en 5 de los 8 que fallaron **Apollo no tiene una sola persona cargada**, aunque sí
tenga la ficha de la empresa. Y cruzó mal la empresa en 2 casos (`apotheken-umschau.de` →
una farmacia de barrio). Su cobertura sobre publishers europeos medianos está vacía.

**Las 21 son todas Apollo raspado** — exponen sus endpoints (`mixed_people/search`,
`people/match`), varias se venden como *"no cookies required"*. **El techo de datos es el
mismo**: ninguna cierra el agujero de cobertura. Cambian empaquetado y precio, no
información.

## Triage de las 21 (verificado llamando, no leyendo)

- **11 son la misma API republicada** por vendedores distintos: mismo `/domain-search_v1.php`
  y `/email-finder.php`. Probar una es probar las once.
- **Caídas (502):** `apollo26`, `apollo-io-leads-scraper`, `apollo-api3`.
- **Proxies BYOK** (piden tu propia key de Apollo → gastan tus créditos *y* cobran RapidAPI):
  `apollo-enrichment`, `apollo-prospect-searcher`. Ésta última reenvía a un endpoint que
  Apollo ya dio de baja.
- **Falsa:** `apollo-leads-from-website` devuelve **siempre la misma persona** (Michelle
  Tindale, una velería australiana) para cualquier dominio.
- **No es Apollo:** `apollo-ai-developer-suite` es un resumidor de textos.
- **Única candidata viva:** `apollo-lead-finder-api`. Devuelve email **ya desbloqueado**,
  Verified, con teléfono corporativo. Pero devolvió **cero** en `deia.eus`, donde Apollo
  encontró al Jefe de Publicidad → puede ser un SUBCONJUNTO de Apollo.

**Los planes gratis son inservibles para probar:** BASIC = **3 requests/mes** en la familia
PHP, ~50 en `no-cookies-required`. Para una prueba real hace falta PRO ($14,99 = 2.500 req).

## Economía, para no confundir unidades
Un crédito de Apollo ≠ un request. **En Apollo la búsqueda es gratis**, sólo cobra el
desbloqueo (~0,5 créditos por dominio consultado). En `lead-finder` **cada consulta cuesta**,
encuentre o no. Aun así: **$0,013/dominio Apollo vs $0,006/dominio lead-finder**. Pero
lead-finder quema el plan **3× más rápido** cuando el pool acelera, porque escala con
intentos y no con aciertos. Demanda real: **570–1.420 consultas pagas/mes**.

## La conclusión que le di al user
A $65/mes, **Apollo no es el problema caro**. El problema es que **7 de cada 10 dominios se
quedan sin ningún email** y ninguna de las 21 lo arregla. Optimizar $50/mes mientras el 70%
queda vacío es acomodar los muebles. Ver [[project_north_star]].

**Pendiente de decisión del user:** correr Apollo y `lead-finder` **en paralelo un mes**
($80 total) midiendo sobre ~600 dominios de producción, sin riesgo para el pool. El banco de
pruebas está escrito y corrido.

Relacionado: [[project_north_star]], [[reference_criterio_email]], [[reference_billing_cycles]], [[project_apis]]
