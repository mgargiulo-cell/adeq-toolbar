# Subir el zip al Chrome Web Store sin entrar al panel

Se configura **una sola vez**. Después cada subida es un comando.

Todo lo que hay que hacer está del lado de Google: yo no puedo crear proyectos ni dar
consentimiento OAuth por vos. Los pasos 1 a 5 son tuyos, el 6 lo hacemos juntos, y del 7 en
adelante lo hago yo.

---

## Paso 1 — Crear el proyecto en Google Cloud

1. Entrá a <https://console.cloud.google.com/projectcreate>
2. Nombre: `adeq-toolbar-cws` (o el que quieras)
3. **Crear**

## Paso 2 — Habilitar la API de la tienda

1. Con ese proyecto seleccionado, entrá a
   <https://console.cloud.google.com/apis/library/chromewebstore.googleapis.com>
2. **Habilitar**

## Paso 3 — Crear las credenciales OAuth

1. <https://console.cloud.google.com/apis/credentials/consent>
   - Tipo: **Externo** → Crear
   - Nombre de la app: `ADEQ Toolbar Publisher`
   - Correo de asistencia y de contacto: tu mail
   - Guardar y continuar hasta el final (no hace falta agregar permisos acá)
   - En **Usuarios de prueba**, agregá tu propio mail (el dueño de la cuenta de desarrollador)
2. <https://console.cloud.google.com/apis/credentials>
   - **Crear credenciales → ID de cliente de OAuth**
   - Tipo de aplicación: **App de escritorio** ← importante, con otro tipo no funciona
   - Nombre: `adeq-cli`
   - **Crear**
3. Copiá el **ID de cliente** y el **Secreto**. Los vas a usar en los pasos 4 y 5.

## Paso 4 — Autorizar (una vez)

Pegá esto en el navegador **reemplazando `TU_CLIENT_ID`**, con la sesión de Google que es
dueña de la cuenta de desarrollador:

```
https://accounts.google.com/o/oauth2/auth?response_type=code&access_type=offline&prompt=consent&redirect_uri=urn:ietf:wg:oauth:2.0:oob&scope=https://www.googleapis.com/auth/chromewebstore&client_id=TU_CLIENT_ID
```

Google va a avisar que la app "no está verificada" — es tuya y sin publicar, así que está
bien: **Configuración avanzada → Ir a ADEQ Toolbar Publisher**.

Aceptá y **copiá el código** que aparece en pantalla.

> El código dura pocos minutos y es de un solo uso. Si se vence, recargá la URL.

## Paso 5 — Canjear el código por el token permanente

```bash
cd ~/Desktop/adeq-toolbar
python3 scripts/cws-token.py "TU_CLIENT_ID" "TU_SECRET" "EL_CODIGO_DEL_PASO_4"
```

Te imprime un bloque JSON con el `refresh_token`. **Ese token no vence** salvo que lo
revoques o cambies la contraseña de Google.

## Paso 6 — Guardar las credenciales

```bash
nano ~/.adeq-cws.json     # pegá el JSON del paso 5, guardá con Ctrl+O y salí con Ctrl+X
chmod 600 ~/.adeq-cws.json
```

`chmod 600` deja el archivo legible solo por tu usuario. **No va al repo** — está en
`.gitignore` por estar fuera de la carpeta del proyecto, pero igual: nunca lo pegues en un
chat ni lo copies adentro de `adeq-toolbar/`.

---

## Paso 7 en adelante — ya lo hago yo

```bash
python3 scripts/cws-publish.py ~/Desktop/adeq-toolbar-v658.zip              # sube, no publica
python3 scripts/cws-publish.py ~/Desktop/adeq-toolbar-v658.zip --publicar   # sube y publica
```

Antes de subir, el script **verifica solo** que el zip no traiga el campo `key` — es el error
que ya rechazó una versión.

---

## Lo que NO cambia

**Publicar sigue necesitando tu OK.** Subir reemplaza el borrador de la ficha; publicar lo
manda a revisión y, al aprobarse, les llega a los tres MB. Es una acción hacia afuera y no la
voy a disparar por mi cuenta: te aviso que el zip está listo y publico cuando me lo pedís.

Si preferís, quedate solo con el paso 7 sin `--publicar`: yo dejo el borrador actualizado y
vos apretás Publicar en el panel. Es lo más conservador y funciona igual.

**La revisión de Google sigue existiendo.** Con visibilidad Privada suele ser rápida, pero no
es instantánea: la API no la saltea.

## Si algo falla

| Mensaje | Qué pasó |
|---|---|
| `invalid_grant` al canjear | El código ya se usó o venció. Repetí el paso 4. |
| No devuelve `refresh_token` | Falta `prompt=consent` en la URL del paso 4. |
| `HTTP 401` al subir | El refresh token fue revocado. Repetí pasos 4 a 6. |
| `HTTP 403` | La cuenta no es dueña de la extensión, o falta habilitar la API (paso 2). |
| Rechazo por `key` | El zip se armó mal: hay que sacar ese campo del manifest del paquete. |
