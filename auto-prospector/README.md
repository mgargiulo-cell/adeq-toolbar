# ADEQ Auto-Prospector

Servicio Node.js que corre continuamente cuando el botón AUTO está ON.
Toma URLs del board de Monday, busca el decisor con Gemini + su email con Apollo, guarda en historial.
Se auto-apaga a los 45 minutos por sesión.

## Deploy en Railway

1. Creá cuenta en [railway.app](https://railway.app) con GitHub
2. **New Project → Deploy from GitHub repo**
3. Seleccioná el repo → en **Settings → Root Directory** poné `auto-prospector`
4. En **Variables** agregá:

```
SUPABASE_URL=https://ticjpwimhtfkbccchfyp.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRpY2pwd2ltaHRma2JjY2NoZnlwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ5MDE1MTksImV4cCI6MjA5MDQ3NzUxOX0.85xb7q52QHFsUZIqgOsogexMml--Ag1K3LY-a7cstyU
SUPABASE_EMAIL=mgargiulo@adeqmedia.com
SUPABASE_PASSWORD=TU_PASSWORD_SUPABASE
```

5. Deploy — el servicio arranca solo y queda corriendo 24/7.

## Flujo por dominio

1. **Gemini + Google Search** → encuentra CEO/founder (nombre y cargo)
2. **SimilarWeb** → obtiene tráfico mensual
3. **Apollo RapidAPI** → busca email con el nombre encontrado
4. Guarda en `toolbar_historial` con `source = 'auto'`
5. Marca el dominio en `toolbar_import_queue` (bloqueado 60 días)

## Control desde la toolbar

- Botón **AUTO** en el header → click para ON (verde) / OFF (gris)
- Al encender: el servicio detecta el cambio en máx 20 segundos y empieza
- Límite: 45 minutos de trabajo continuo → se apaga solo
- Resultados visibles en el tab **Historial** con badge verde **AUTO**

## Lo que NO puede hacer (necesita navegador)

- Scraping de emails desde el HTML del sitio
- Detección de banners y ad tech
- Análisis de ads.txt detallado
