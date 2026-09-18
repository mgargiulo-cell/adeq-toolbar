---
name: ADEQ Toolbar — Capabilities Map
description: Análisis exhaustivo de qué hace la toolbar y qué cubre del workflow de ADEQ Media. Base para planear nuevas funciones.
type: project
originSessionId: b9fd9a24-e6b6-4c02-b4f9-21155b7bf17e
---
# ESENCIA DE ADEQ MEDIA

ADEQ Media es una **agencia de monetización programática** (ad tech). Vende a publishers web (sitios con tráfico) acceso a su demand stack:

- **Display**: header bidding interno con 8+ demand sources compitiendo por cada impresión
- **Video instream**: reproductor + cuando no hay ad → muestra contenido del propio sitio (recirculación)
- **Video outstream**: solo visible cuando hay ad disponible (no molesta UX)
- **Slider**: CPM fijo en USD

Modelo: **revshare 80/20 a favor del publisher**, sin exclusividad ni mínimos. Estrategia comercial = "probános sin compromiso, los resultados hablan".

Equipo: 3 media buyers (MBs) prospectando publishers
- **Maxi** (mgargiulo@adeqmedia.com) = admin de la toolbar
- **Diego** (dhorovitz@adeqmedia.com)
- **Agus** (sales@adeqmedia.com)

# ETAPAS DEL WORKFLOW DEL MB (cubiertas por la toolbar)

## 1. DESCUBRIR leads (sourcing)

| Fuente | Cómo |
|---|---|
| **Sitio que ya estás visitando** | Toolbar Analysis tab analiza el dominio del active tab |
| **Cascade — sitios similares** | Tab Cascade scrapea similarsites.com (gratis, 0 RapidAPI hits) |
| **Autopilot Majestic** | Worker Railway 24/7 descubre del top Majestic, filtra +400K visits + GEO target, encola en Prospects |
| **Import CSV externo** | Subir lista de URLs → worker procesa una por una |
| **Monday URL refresh** | Buscar en Monday items en estado "Ciclo Finalizado" o "Mail No Enviado" → re-prospectar |
| **sellers.json importer** | 42 ad-tech companies (Truvid 7K, Vidoomy 3.4K, AdPlus 2.9K, etc) → ~80K publishers descubribles. Dedup contra sistema antes de encolar |
| **Keywords search** | Tab Keywords con rotación |

Filtros de descubrimiento:
- Threshold +400K pageViews (visits × pagesPerVisit)
- GEO target configurable
- Categoría
- Excluye dominios ya en Monday (excepto Ciclo Finalizado / Mail No Enviado)
- Excluye 500 dominios baked-in no-publishers (Google, Facebook, etc)
- Excluye admin custom blocklist

## 2. ANALIZAR el sitio

Tab **Analysis (Core)** orquesta:
- **Tráfico**: pageViews, rawVisits, top countries, category — vía RapidAPI website-insights (cache 90d compartida en Supabase)
- **Ad tech detection**: scripts de partners cargados en la página (Adsense, Adx, header bidding stack, video players, etc)
- **ads.txt parsing**: presencia + entries + Google/Ezoic flags
- **Banners detected**: formatos de ad activos
- **Revenue gap estimate**: % perdido por falta de ads.txt / pocos SSPs
- **Audit summary**: bullets en lenguaje natural sobre la oportunidad
- **Duplicate check**: si ya está en Monday → muestra status + ejecutivo

## 3. ENCONTRAR el decision maker

- **Apollo direct**: API call con título=[CEO, founder, owner, publisher, editor in chief, ...]. Cache 7d Supabase compartida con worker.
- **Email scraping**: footer, página /contact, /about, etc.
- **Email verification**: api.eva.pingutil.com (SMTP check)
- **Garbage filter**: skip whois/abuse/postmaster/contact@nic
- **Gemini search**: nombre del decisor vía Google Search grounding

UI:
- Lista de chips con status color (verde/amarillo/rojo)
- Apollo "ver más" para emails locked (revealing cuesta)
- Input manual SIEMPRE gana sobre auto-detectado

## 4. GENERAR el pitch

- **Anthropic Claude Sonnet 4.6** vía proxy Edge Function (prompt caching activo)
- **Diego voice prompt** bakeado (~6KB) — tono real, no corporativo, "carga conciencia spam", revshare 80/20, sin firma con nombre
- **Custom prompt admin** override absoluto si lo configurás
- **Few-shot examples** dentro del system prompt
- **RAG retrieval**: busca pitches que el MB marcó 👍 (Voyage embeddings) → influye estilo
- **Anti-repetition**: no repite ángulos ya usados para el mismo dominio
- **Output JSON**: body + 3 subjects sugeridos
- **Idioma auto**: detect TLD + geo + r.language → español/inglés/italiano/portugués/árabe
- Reglas: no inventar meses, no contradecir ads.txt, no firma

## 5. PUSHEAR a Monday CRM

- Push: crea item en board 1420268379 con todas las cols (domain, traffic, geo, idioma, email, fecha, ejecutivo, status, pitch)
- Update: si ya existe (vino de Monday refresh) → UPDATE en vez de CREATE
- Estados Monday: En Negociación, Propuesta Vigente, Propuesta Vigente (T), Masivo-Agus/Max/Diego, Ciclo Finalizado, Mail No Enviado
- Owner = el MB logueado actualmente
- Cap diario per-user configurable (default 100)

## 6. ENVIAR el mail

- Gmail OAuth (`gmail.send` scope)
- RFC 2047 encoding en Subject (UTF-8 acentuados → mojibake-free)
- Append signature/closing automático según idioma
- Track en `toolbar_sendtrack` (domain + send_date)
- Cap diario per-user configurable

## 7. REVISAR la cola (Prospects tab)

- Lee `toolbar_review_queue` (RLS abierta entre MBs — todos ven todo)
- Filtros: date / source / user
- Filter presets: Mis 7 días / Hoy todos / All / Solo Autopilot
- Bulk actions: ❌ Reject + 🪟 Open tabs (selección con checkboxes)
- Lead temperature badges: 🔥 hot / ☀️ warm / ❄️ cold
- Card expand → auto-fetch traffic si falta + lock 30 min al dominio
- Validate (push Monday + send mail) per-card
- Auto-refresh cada 30s si autopilot/csv_queue ON
- Drafts auto-save del pitch (debounce 3s, TTL 7d)

## 8. COORDINAR entre MBs

- **Prospect locks**: 30 min cuando un MB expande una card. Otros ven 🔒 toast.
- **Mutex autopilot**: solo un MB puede prender autopilot a la vez. Otros ven badge "🔒 Locked by X".
- **Mutex AUTO IMPORT**: idem para procesar la cola de imports.
- **Handoffs**: pasar leads entre MBs con nota. TTL 7d sin aceptar → expira.
- **Vacation toggle**: status per-user (ausente/disponible).
- **User badges en cards**: cada prospect muestra quién lo trajo (Maxi/Diego/Agus + color).
- **Banner Railway dead**: si flag ON pero worker no late hace +5min → warning visible.

## 9. APRENDER del feedback (RAG)

- 👍 **Like**: pitch + context se guarda con embedding (Voyage voyage-3, 1024 dims). Próximas generaciones replican estilo.
- 👎 **Dislike**: con razón opcional ("inventó un mes", "tono muy formal", etc). Razón entra al embedding → la IA evita ese patrón.
- Búsqueda por similitud: matching domain/category/geo/language/traffic.

## 10. ADMIN (solo Maxi, triple-click logo)

- **Activity tab**: stat tiles + chart sites/día + comparador horizontal MB (4 grupos: VOLUMEN, CALIDAD, OUTREACH, EFICIENCIA) + resumen narrativo per MB
- **Limits tab**: per-user caps (autopilot enabled, daily prospects/minutes, monthly API cap)
- **Blocklist tab**: 500 baked-in + custom URLs admin
- **Settings**: prompt global de Diego's voice (todos los MBs lo usan, no editable por ellos)
- **Audit log**: cada acción admin se loggea en `toolbar_audit_log`

# CAPAS TÉCNICAS

## Frontend
Chrome Extension MV3 vanilla JS (sin build step). `popup/popup.html` + `popup/popup.js` (~7K líneas) + `popup/popup.css`. Modules en `modules/*.js`.

## Backend
**Supabase**:
- 18+ tablas con RLS
- Auth (email/password)
- Edge Function `api-proxy` enrutando RapidAPI/Apollo/Anthropic/Voyage/Gemini (keys nunca tocan el bundle)

**Railway worker** (`auto-prospector/index.js` ~2300 líneas):
- 24/7 con auto-exit a 30 min idle
- SUPABASE_SERVICE_ROLE_KEY para bypass RLS
- Procesa autopilot Majestic + CSV/Monday queue
- Heartbeat cada loop iteration

## APIs externas
| API | Uso | Costo |
|---|---|---|
| RapidAPI website-insights (DataLoom) | Tráfico | Plan PRO $25/mo, cap 40K/mes |
| Apollo.io | Decision maker email | $0.05/call, cache 7d |
| Anthropic Claude Sonnet 4.6 | Pitch generation | Prompt caching activo |
| Voyage voyage-3 | RAG embeddings | $0.06/M tokens, cache in-memory |
| Google Gemini | Search grounding decisor | Bajo |
| Monday GraphQL | CRM | Plan team |
| Gmail OAuth | Send | Free |
| api.eva.pingutil.com | SMTP verify | Free |

## Anti-overage hardening
- Cap mensual RapidAPI 40K (cycle 6→6, alineado billing)
- Cap diario RapidAPI 5K (defensa secundaria)
- Cache 90d en TODAS las solapas (regla de oro)
- Cache 7d Apollo
- Cache 1h Voyage embeddings
- 75 max por tirada de import
- 300 max pending en queue
- 300 max prospects/día/user
- 20 min sesión max autopilot
- No retry en 429 (cuesta plata)
- Counter SW preciso compartido entre MBs
- Banner alerta a 50%/80%/100% del cap

# TABLAS SUPABASE (al 2026-05-11)

Operacionales:
- `toolbar_review_queue` — prospects pending para revisar
- `toolbar_csv_queue` — cola de imports a procesar por worker
- `toolbar_sendtrack` — mails enviados (1 row por dominio)
- `toolbar_historial` — análisis manuales con toolbar
- `toolbar_handoffs` — pasaje de leads entre MBs
- `toolbar_prospect_locks` — claims de 30 min

Cache (compartida entre MBs):
- `toolbar_traffic_cache` — RapidAPI all-insights (TTL 90d)
- `toolbar_similar_sites_cache` — similar-sites scrape (TTL 90d)
- `toolbar_apollo_cache` — Apollo people (TTL 7d)
- `toolbar_domain_geo_cache` — geo lookup
- `toolbar_url_blocklist` — admin custom + 500 baked-in

Admin / config:
- `toolbar_config` — kv store (heartbeats, mutex, counters)
- `toolbar_user_limits` — caps per-user
- `toolbar_user_status` — vacation/activo
- `toolbar_user_prompts` — Diego voice + custom per-user
- `toolbar_audit_log` — eventos admin

RAG / aprendizaje:
- `toolbar_pitch_feedback` — likes/dislikes con embeddings (1024 dims pgvector)
- `toolbar_pitch_drafts` — drafts guardados por user

Stats:
- `toolbar_api_usage` — hits per provider per user per day
- `toolbar_usage_sessions` — duración popup/autopilot per user
- `toolbar_autopilot_stats` — métricas worker per día

# QUÉ NO CUBRE (boundaries del producto)

- ❌ Follow-up automatizado (CRM externo lo maneja, fue removido)
- ❌ Notificaciones push (user no quiere)
- ❌ Onboarding/contracting una vez que el lead respondió (Monday lo maneja)
- ❌ Reporting financiero / billing
- ❌ Dashboard analytics avanzado (solo Activity tab admin)
- ❌ Auto-archive Monday cold leads +90d (riesgoso, pendiente)
- ❌ Mobile app

# DISTRIBUCIÓN

- Chrome Web Store (en review desde 2026-05-07)
- ZIPs en `~/Desktop/adeq-toolbar-{chrome,mbs}.zip` para load unpacked
- Privacy policy en GitHub Pages: https://mgargiulo-cell.github.io/adeq-toolbar/docs/privacy.html
- Repo: https://github.com/mgargiulo-cell/adeq-toolbar
