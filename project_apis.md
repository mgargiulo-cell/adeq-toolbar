---
name: ADEQ Toolbar — External APIs & Integrations
description: All external services + Supabase tables + key locations (al 2026-05-07)
type: project
originSessionId: b9fd9a24-e6b6-4c02-b4f9-21155b7bf17e
---
## APIs Used

| API | Purpose | Key location |
|-----|---------|-------------|
| Monday.com GraphQL | CRM — create/update prospect items | Supabase secrets (Edge Function) |
| RapidAPI Website Insights (DataLoom) | Traffic + GEO + PPV + similar sites | Supabase secrets (RAPIDAPI_KEY) |
| Anthropic Claude Sonnet 4.6 | Pitch generation con prompt global Diego's voice | Supabase secrets (ANTHROPIC_API_KEY) |
| Apollo.io Official API | Email enrichment | Supabase secrets (APOLLO_API_KEY) |
| Supabase | Persistence + Auth + Edge Function proxy | CONFIG.SUPABASE_URL + ANON_KEY (extensión) |
| Voyage voyage-3 | Embeddings RAG | Supabase secrets (VOYAGE_API_KEY) |
| eva.pingutil.com | Email verify SMTP gratis (CSP whitelisted) | hardcoded en emailVerifier.js |
| Gmail API (OAuth) | Send emails from user's Gmail | OAuth via chrome.identity |

## Edge Function api-proxy

Único endpoint que el cliente llama. Vive en Supabase Edge Functions. Routea provider → upstream con keys server-side.

```
POST {SUPABASE_URL}/functions/v1/api-proxy
Body: { provider, path, method, headers, body, query }
provider: gemini | apollo | rapidapi | anthropic | voyage
```

Per-user daily quota (toolbar_api_usage): 500 total, by provider 300/150/400/200/300.

## RapidAPI Website Insights (DataLoom)

- **Host:** `website-insights.p.rapidapi.com` (switched 2026-05-07 desde similarweb-insights después del overage de $451)
- **Plan:** PRO $25/mo (subscripto el 2026-05-07)
- **Endpoint primary:** `/all-insights?domain=X` — devuelve TODO en una call
  - `Traffic.Visits` (objeto histórico mensual `{YYYY-MM-DD: number}`) → tomar mes más reciente
  - `Traffic.Engagement.PagesPerVisit` → para calcular pageViews
  - `Traffic.TopCountryShares` (objeto `{US: 0.7, ...}`) → top GEO
  - `WebsiteDetails.Category` → categoría
  - `Rank.GlobalRank` → ranking
- **Endpoint secondary:** `/similar-sites?domain=X` — snowball discovery (autopilot)
- **NO usados:** `/traffic`, `/countries`, `/engagement` — los datos ya vienen en `/all-insights`
- **Cap mensual:** 40K hits hard limit (configurable). Cap diario: 5K (defensa).
- **Cache:** 90 días Supabase (toolbar_traffic_cache) + 90 días local chrome.storage
- **Threshold de calidad:** 300K pageViews (visits × pagesPerVisit) — sub = no enriquecer

## Apollo.io

- Endpoint 1: POST `/v1/people/match` (name + domain → verified email)
- Endpoint 2: POST `/v1/mixed_people/api_search` (domain + titles → contactos)
- **Decisión user 2026-05-07:** emails siempre se buscan en flujo manual, sin gate de threshold. En autopilot sí gateado por threshold 300K.
- Daily cap: 150 calls/día (toolbar_config.apollo_daily_limit)

## Anthropic Claude

- Model pitch: `claude-sonnet-4-6`
- Model FU (DEPRECATED 2026-05-07): se eliminó el feature de follow-up entero. CRM externo lo maneja.
- **Prompt global "Diego's voice"**: bakeado en [modules/diegoVoicePrompt.js](modules/diegoVoicePrompt.js) + sincronizado vía toolbar_user_prompts con email = `__global__`. Solo admin lo edita.

## Monday.com

- GraphQL endpoint: `https://api.monday.com/v2`, API-Version: 2024-01
- Board: 1420268379 (CONFIG.MONDAY_ACTIVE_BOARD)
- Columnas: deal_stage, deal_owner (persons), deal_close_date, texto6 (geo), texto7 (traffic), estado_12 (idioma), email_mm2edcd3
- Per-user keys disponibles: monday_api_key_{email}

## Supabase Tables (8 tablas, todas con RLS auth)

```
1. toolbar_traffic_cache       — cache shared 90 días (domain PK, data JSONB, fetched_at)
2. toolbar_sendtrack           — historial de envíos (domain PK, send_date, pitch, email)
3. toolbar_historial           — log de análisis del MB
4. toolbar_review_queue        — prospects pendientes de validación (autopilot output)
5. toolbar_config              — API keys + flags + targets
6. toolbar_api_usage           — daily quotas per-user
7. toolbar_user_prompts        — custom prompts (key __global__ = prompt activo)
8. toolbar_pitch_drafts        — drafts persistidos
9. toolbar_user_limits         — caps per-user (autopilot_enabled, monthly_api_cap, autopilot_daily_minutes, autopilot_daily_prospects)
10. toolbar_url_blocklist      — admin blocklist
11. toolbar_usage_sessions     — tracking real tiempo (popup | autopilot)
12. toolbar_prospect_locks     — claim 30 min por dominio
13. toolbar_handoffs           — pasar leads entre MBs
14. toolbar_user_status        — vacation toggle
15. toolbar_audit_log          — eventos de admin
16. toolbar_domain_geo_cache   — GEO cache compartida
17. toolbar_import_queue       — domains imported (60 días TTL)
18. toolbar_csv_queue          — CSV queue del autopilot
```

## Gmail

- OAuth scopes: `gmail.send` + `gmail.settings.basic` + `userinfo.email`
- Client ID: 1006462691161-6uicvg6urcco0a50534c46l4jiclfm70
- Consent screen: Internal (@adeqmedia.com)

## Hosting / Distribución

- **GitHub repo:** https://github.com/mgargiulo-cell/adeq-toolbar (público)
- **GitHub Pages:** https://mgargiulo-cell.github.io/adeq-toolbar/ — sirve docs/privacy.html y docs/reset-password.html
- **Railway:** worker auto-prospector. Vars necesarias: SUPABASE_URL, SUPABASE_ANON_KEY, **SUPABASE_SERVICE_ROLE_KEY** (crítico, evita login fail), RAPIDAPI_KEY, MONDAY_API_KEY, APOLLO_API_KEY, optional CLOUDFLARE_API_TOKEN.
- **Chrome Web Store:** v3.2.0 enviada a revisión 2026-05-07.
