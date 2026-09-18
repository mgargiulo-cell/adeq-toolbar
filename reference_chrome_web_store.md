---
name: reference-chrome-web-store
description: "Cómo se arma el zip y la ficha de la extensión para el Chrome Web Store: el campo key, la política de privacidad, y por qué va Privada"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-25T20:36:23.791Z
---

Aprendido publicando la v642 el 2026-08-25. Consultar ANTES de armar un zip para la tienda.

## El zip: hay que SACAR el campo `key`
`manifest.json` tiene un `"key"` que fija el ID de la extensión al cargarla a mano en modo
desarrollador. **La tienda lo rechaza**: *"No se admite el campo key en el archivo de
manifiesto"*. Se saca SOLO del paquete, nunca del repo (si no, la carga local cambia de ID).

Receta: copiar a un staging, borrar la clave con un script, zipear desde ahí.
Contenido del paquete: `manifest.json config.js popup modules background icons docs`
(44 archivos, ~480 KB). NO va `auto-prospector/` — eso es el worker de Railway.

## La política de privacidad
**URL válida: `https://mgargiulo-cell.github.io/adeq-toolbar/docs/privacy.html`**
El repo `mgargiulo-cell/adeq-toolbar` es PÚBLICO y Pages ya sirve ese archivo.
(Escaneado el 2026-08-25: no hay claves expuestas, solo la `anon` de Supabase, que es pública
por diseño.)

⚠️ **NO usar `adeqmedia.com/politica-de-privacidad/`**. Existe y carga, pero es la política del
SITIO — RGPD, formularios, cookies. No menciona la extensión, ni el token de Gmail, ni el
scraping, ni Apollo/Monday. Google rechaza la versión cuando la política no cubre los permisos
declarados.

## Visibilidad: **Privada**, con testers de confianza
El permiso de host amplio dispara "revisión en profundidad" (semanas). En Privada se saltea.
Hay que cargar las 3 cuentas de Google (Max, Agustina, Diego) en la página de configuración de
la cuenta de desarrollador — sin eso, al tester le da 404.

## El permiso `<all_urls>` es REAL, no se puede recortar
Un auditor va a proponer cambiarlo por `activeTab`. **No se puede**: la extensión hace fetch
cross-origin a dominios que no se conocen de antemano —
`modules/scraper.js` (perfiles sociales, páginas de contacto),
`modules/sellersJson.js` (ads.txt / sellers.json),
`modules/audit.js` (la página cuando la inyección falla).
Recortarlo rompe la detección de emails y la lectura de ads.txt, que es la puerta 0 del pool.

## Código remoto: NO
Verificado: cero `eval`, cero `new Function`, cero scripts externos. CSP `script-src 'self'`.

## Datos que se declaran en el formulario
Tildar: identificación personal · autenticación · comunicaciones personales · historial web ·
actividad del usuario · contenido del sitio web.
Dejar sin tildar: sanitaria · financiera · ubicación.
Y las **tres** certificaciones del final — el formulario no habilita publicar hasta que están
las tres, aunque una ya venga marcada.

## Capturas
1280x800 exacto, sin alfa. Las inventadas se rechazan: tiene que ser la toolbar funcionando.
Las dos promocionales (440x280 y 1400x560) son OPCIONALES.

Relacionado: [[project_overview]], [[project_security]]


## 🤖 Subida por API — configurado el 2026-08-28
Instructivo completo en `scripts/README-chrome-web-store.md`. Dos scripts:
- `scripts/cws-token.py` — canje ÚNICO del código OAuth por un `refresh_token` (no vence).
- `scripts/cws-publish.py <zip>` — sube. Con `--publicar` además manda a revisión.
  Verifica solo que el zip no traiga el campo `key` antes de subir.

Credenciales en `~/.adeq-cws.json` (chmod 600, FUERA del repo; regla agregada al `.gitignore`).
Necesita: proyecto en Google Cloud + API `chromewebstore` habilitada + cliente OAuth tipo
**App de escritorio** (con otro tipo NO funciona) + `prompt=consent` en la URL de autorización
(sin eso Google no devuelve refresh_token).

Extension ID: `jgbacjjjohjaiojjecgnejcalepkjclm` (derivado del `key` del manifest local).

### Estado: FUNCIONANDO (probado 2026-08-28)
Credenciales creadas y guardadas. Verificado contra la tienda real: auth ✓, proyecto ✓,
permisos ✓. La subida llega al store.

⚠️ **Dos trampas que costaron la primera vuelta:**
1. La API hay que habilitarla **en el MISMO proyecto** que creó el cliente OAuth (nº 290252864383).
   Estaba habilitada en otro y daba 403 con un mensaje confuso.
2. Los scripts usan **`curl`, NO `urllib`**: en esta Mac urllib no encuentra los certificados
   raíz (CERTIFICATE_VERIFY_FAILED) — el mismo problema que con la API de Railway.

**`ITEM_NOT_UPDATABLE` NO es un error del código**: significa que hay un paquete en revisión
(`pending review`, `ready to publish`). La tienda no deja reemplazar el borrador hasta que
Google termine. Se consulta el estado con
`GET /chromewebstore/v1.1/items/<id>?projection=DRAFT` — devuelve `crxVersion` y `uploadState`.

### 🔑 AUTORIZACIÓN PERMANENTE DEL USER (2026-08-28)
Textual: *"la próxima zip la subís vos directamente"* + *"siempre que subís debes publicar"*.

**MÁXIMO UNA PUBLICACIÓN POR DÍA**, y la regla completa es:
1. **NO subir durante la jornada.** Se acumulan todos los cambios del día.
2. Al **terminar todo**, PREGUNTAR: "¿subo y publico?" con el resumen de lo que va.
3. Con el OK, un solo comando: `python3 scripts/cws-publish.py <zip> --publicar`
   (subir y publicar van SIEMPRE juntos — nunca subir sin publicar).

El motivo: cada publicación pasa por revisión de Google y mientras una está pendiente la
tienda devuelve `ITEM_NOT_UPDATABLE` y bloquea la siguiente. Subir de a poco durante el día
se traba solo. **Un zip por día, al final, con el OK del user.**

Lo que SÍ hay que hacer siempre:
· **decirle qué versión se sube y qué trae** — se entera por acá, no por el panel;
· si falla, decir el motivo real (`ITEM_NOT_UPDATABLE` = hay uno en revisión, NO es un bug)
  en vez de reintentar en silencio.

⚠️ Esto NO se extiende a otras acciones hacia afuera (mails, Monday, DNS): la autorización es
para el Chrome Web Store y nada más.
La revisión de Google sigue existiendo — la API no la saltea.
