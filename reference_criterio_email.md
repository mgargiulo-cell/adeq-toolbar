---
name: reference-criterio-email
description: Cómo elige un email el media buyer (textual) y cómo está implementado en rankEmail
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-15T11:24:21.717Z
---

**Textual del media buyer (2026-08-25):**
> "Yo voy por el correo que dice el nombre del espacio con el que me quiero conectar, ya sea el
> webmaster, editor, sales, encargado de ventas; o de última si no hay, info o contacto, o algún
> nombre como juan@aliados.com."

O sea: **primero el rol que NOMBRA el espacio**, y recién después el genérico o la persona.

## La escala en `rankEmail` (alineada a eso)
| Puntos | Qué |
|---|---|
| 95 | `publicidad@` `ventas@` `sales@` `ads@` `marketing@` — el ideal |
| 90 | `ceo@` `founder@` |
| 80 | `comercial@` |
| 75 | `editor@` `redaccion@` — subió de 60 |
| 72 | `webmaster@` — **subió de 15**, estaba tratado como genérico |
| 70 | `juan.perez@` |
| 55 | `juan@` |
| 15 | `info@` `contacto@` — el "de última" |
| ≤8 | departamentos, mesa de ayuda |

**El umbral 40** separa "sirve" de "de última". Lo usa el auditor para decidir si sale a
buscar algo mejor.

## Estado del pool cuando se midió (762 leads con email)
- 122 con el comercial ideal
- 453 con un rol o una persona
- **187 (uno de cada cuatro) SOLO con el "de última"** ← ahí está el trabajo

## Quién aplica esto
- `rankEmail` — la única fuente de verdad del puntaje. **No escribir otra:** dos
  implementaciones de la misma regla ya discreparon dos veces en este proyecto
  (la regla de marca duplicada y el linter).
- `auditarEmailsDelPool` (1×/día) — limpia los malos, reordena para que el mejor quede
  primero, y a los que solo tienen genérico les hace un scrape GRATIS buscando uno mejor.
  Techo: 20 upgrades por pasada, corte a los 90s.

Relacionado: [[project_pending]], [[reference_4_metricas]]

## Decisión del dueño 13/09 (commit e792953, desde otra máquina): **webmaster@ SÍ, office@ NO**
`webmaster` salió de la lista de genéricos de `rankEmail` (lib/email.js ~línea 96) y pasa a
**contacto válido de puntaje bajo**; sigue en `APOLLO_GENERIC_LOCAL`/`GENERIC_LOCAL_RE` para el
dedupe de fuentes. También: `rrhh@`, `empleos@`, `informatique@` dejan de ganarle a `info@`
(7d90d0d), y la extensión aplica los mismos vetos del worker (d4881a1). Ver `git log` 08–13/09.

## Los tres criterios y cómo se sintetizaron (2026-08-25)
**Agus:** el rol que NOMBRA el espacio (webmaster, editor, sales, encargado de ventas); los
nombres propios de última; descarta suscripción, noticias, finanzas, atención al cliente; y no
escribe a otro dominio *"porque te mandan a hablarle a una agencia"*.
**Diego:** mismo dominio que el sitio; prioridad `publicidad`/`anuncios`; si no hay, prefiere
los de gmail; descarta info, contacto, soporte.
**Max:** *"depende de cuántos correos tenga la web y de qué tipo; los info no sirven tanto, es
mejor un mail concreto"*.

**Núcleo común (lo que pesa):** mismo dominio · comercial primero · un contacto concreto le
gana a un buzón genérico.

**La observación de Max resuelve el desacuerdo aparente:** no es una escala absoluta, es elegir
el MEJOR DE LO QUE HAY en cada sitio. Si solo existe `info@`, se usa; si además hay un `juan@`,
gana el concreto. El orden por puntaje ya hace eso — solo hay que tener el orden bien.

## Puntaje por DOMINIO (ya estaba bien, NO tocar)
| | |
|---|---|
| mismo dominio | +40 |
| subdominio / mismo brand otro TLD | +35 |
| casa editora conocida | +20 |
| webmail cross-domain (gmail personal) | −15, y se REVIERTE si el rol es EXEC/COMMERCIAL/EDITORIAL |
| otra empresa | **−50** ← esto es el "no le hablo a una agencia" |

## Áreas DESCARTADAS (score negativo)
`soporte` `atencion` `suscripciones` `facturacion` (MESA_DE_AYUDA −20) ·
`noticias` `news` `newsletter` · `finan*` `contab*` `administracion` `tesoreria` (−10).
**NO descartar:** `prensa`/`press` (humano que reenvía) ni `redaccion` (la agarra EDITORIAL +75).


## 🔴 La lista negra mataba a la redacción en TODO idioma menos español (2026-08-31, commit 52bef33)
El error más caro encontrado en `rankEmail`. `GARBAGE_LOCAL` tenía esta línea:
`"redaction","redazione","redaktion","redactie","editorial"`.
Corre ANTES del puntaje, así que la regla EDITORIAL (+75) era **código muerto** para esas
palabras. Probado con la función real — la misma palabra, veredicto opuesto según el idioma:

| local | antes |
|---|---|
| `redaccion@` (es) · `redacao@` (pt) · `editor@` | **115, se enviaba** |
| `redazione@` (it) · `redaktion@` (de) · `redaction@` (fr) · `redactie@` (nl) · `editorial@` | **lista negra, se perdía el lead** |

No era un criterio, era una inconsistencia — y pegaba justo en la **Europa no hispana**, adonde
apunta la cascada GEO después de LATAM y España. 163 leads en 7 días quedaban sin email por
ranking, y en la muestra (`valsusaoggi.it`, `dueruote.it`, `varesenews.it`, `come-on-fc.com`,
`santemagazine.fr`, `reader.gr`) esa casilla era **la única que el sitio publicaba**:
descartarla era perder el lead entero y chocaba de frente con el [[project_north_star]].
`editorial@` además no entraba por el `\b` (después de "editor" viene una "i").
Ahora EDITORIAL conoce los 8 idiomas y `publicidad@` (135) sigue por encima de redacción (115).

⚠️ **Si dos partes de la misma función se contradicen, gana la que corre primero — y en
silencio.** Antes de agregar algo a una lista negra, chequear que el scoring no lo trate como
target.

## Dos basuras que sí había que frenar (2026-08-31, commit 63761b2)
Salieron tirando del 6,5% de rebote de sales@ (contra 1,0% de dhorovitz, mismo mix de fuentes):
- **`u003eenquiry@mytvsuper.com`** puntuaba 40 y se envió. `\u003e` es un `>` escapado en JSON al
  que se le perdió la barra. Daño doble: se manda a una dirección rota **y se quema la buena**,
  porque `enquiry@` existe y el dominio se lleva el rebote igual. Ahora cae todo `u00xx`/`x00xx`.
- **`dmarcreport@opopular.com.br`** puntuaba **95** — el ranking lo leía como nombre de persona
  (PERSON_LIKELY + dominio propio). Lo llena un robot con XML. `dmca` estaba en la lista, `dmarc`
  no. Regex anclada para no pisar nombres reales: `ruben@`, `ruano.perez@`, `spfeiffer@` pasan.

`patriots@patsfans.com` da 95 y pasa bien: su rechazo en el informe venía del estado de rebotes
del dominio, no del ranking.

**El umbral que frena un buzón por rebotes YA es estadístico** (`_wilsonLimiteInferior`), no un
número clavado. Ahí no hay nada que arreglar: el 6,5% de sales@ era una alerta correcta.

Relacionado: [[reference_lectura_rebotes]], [[reference_entregabilidad]]

## 🔴 La escala estaba INVERTIDA respecto al resultado (2026-09-04, medido sobre 2.597 envíos)
Con respuesta REAL (sin `ooo`, que era la mitad de las "respuestas" del tracking):

| bucket rankEmail | envíos | respuesta real | rebote SMTP |
|---|---|---|---|
| 0-39 «de última» | 332 | **6,6%** | **3,6%** |
| 100+ | 663 | 2,9% | 6,8% |

- **Webmail del dueño (gmail/hotmail): 7,7% / 1,8%** contra 3,5% / 7,3% del resto. La regla de
  Diego validada con datos. Ahora persona@gmail = 65 (antes 20), gmail-con-la-marca = 55.
- **MB vs máquina**: en los 465 desacuerdos de `toolbar_email_picks`, los pares con resultado dan
  **22 a 0 para el MB**. Su orden: persona del propio dominio > redacción ≈ comercial >
  persona@webmail > info@. Prefiere persona sobre genérico (61) y sobre comercial (57).
- **Apollo es la PEOR fuente**: 1,8% respuesta real, 11,8% rebote (el 9% de antes eran
  autorespuestas). Informer 2,9% / 12,9% y publica `owner@` inventados. `manual_extra` 6,8% /
  2,3% es lo mejor. **Bajar Apollo del tier 4 es decisión pendiente del user.**
- Arreglos que salieron de sondear la función: `presse/imprensa/stampa` = 115 (era 55, mismo
  bug de `redazione`); `sales@` 135 (la penalidad "sale" le pegaba); `j.perez@` = persona;
  comercial y redacción **por segmento** (`departamentocomercial@`, `de.adsales@`, `lat.press@`);
  `datenschutz/rgpd/lopd` = −1; `ouvidoria/complaints` = mesa de reclamos; `geral@` = info@;
  `admin@` = 25 (de última, los MB lo usaron); `info.lat@` = genérico, no persona; el buzón del
  sitio en el dominio del grupo (`contacto.topgear@henneomagazines.com`) ya no es "otra empresa".
- **Cómo verificar un cambio de escala**: `harness/foto-rankemail.mjs` en el scratchpad de la
  sesión toma la foto sobre los 10.968 emails del pool y diffea contra `rank-antes.json`. Cada
  puntaje que cambia se revisa uno por uno; el diff cazó 3 regresiones antes de deployar.
- 17 tests en `tests/rank-email.test.js` fijan cada regla con su evidencia.
