# ADEQ Auto-Prospector

Worker Node.js (Railway) que automatiza prospecting end-to-end:
- **CSV queue**: procesa imports CSV/Monday/sellers.json → enriquece (RapidAPI + Apollo + scraping) → guarda en `review_queue`
- **Autopilot**: descubrimiento desde Majestic Million / Cloudflare Radar → mismo enrichment
- **AI Agent MB**: para usuarios whitelisted, manda mails reales (Claude pitch + Gmail + Monday push)

## Deploy en Railway

1. Cuenta en [railway.app](https://railway.app) con GitHub
2. **New Project → Deploy from GitHub repo**
3. **Settings → Root Directory** = `auto-prospector`
4. **Variables**:

```bash
# ── Supabase (REQUERIDO) ─────────────────────────────────────
SUPABASE_URL=https://ticjpwimhtfkbccchfyp.supabase.co
SUPABASE_ANON_KEY=<anon JWT>
SUPABASE_EMAIL=mgargiulo@adeqmedia.com
SUPABASE_PASSWORD=<password supabase>

# ── Backend bearer (REQUERIDO para Edge Functions y RLS bypass) ──
# Service role key. Bypassa RLS para que worker pueda PATCH/POST.
# NUNCA exponer en cliente.
BACKEND_BEARER=<service_role JWT>

# ── Gmail Service Account (REQUERIDO si usás el agente) ──────
# JSON entero del SA con Domain-Wide Delegation habilitada en Google
# Workspace Admin Console (scope https://www.googleapis.com/auth/gmail.send).
# Pegado como string single-line.
GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account",...}

# ── Cloudflare DNS (OPCIONAL, mejora discovery autopilot) ────
CLOUDFLARE_API_TOKEN=<api token>
```

5. Deploy → arranca solo, corre 24/7.

## Configurables runtime via Supabase (sin redeploy)

Editables vía SQL en `toolbar_config`:

| Key | Default | Descripción |
|---|---|---|
| `agent_enabled_users` | `[]` | JSON array de emails con agente habilitado |
| `agent_whitelist` | `mgargiulo@adeqmedia.com` | CSV defense-in-depth de users permitidos |
| `agent_max_per_day` | 20 | Cap diario de envíos por user |
| `agent_threshold_traffic` | 500000 | Min tráfico del lead para enviar |
| `agent_active_hours_start` | 9 | Hora START España (0-24) |
| `agent_active_hours_end` | 20 | Hora END España |
| `agent_paused_until` | "" | ISO timestamp; si futuro pausa el agente |
| `agent_claude_email_pick` | false | Opt-in 2do pass Claude para emails ambiguos |
| `agent_refresh_empty_leads` | false | Toggle backfill traffic |
| `agent_backfill_missing` | false | Toggle backfill missing fields (lang, contact, score, etc.) |
| `csv_queue_enabled` | false | Toggle CSV worker |
| `auto_prospecting_enabled` | false | Toggle autopilot worker |
| `csv_queue_daily_cap` | 1000 | Safety net global CSV/día |
| `autopilot_daily_cap_global` | 300 | Safety net global autopilot/día |
| `rapidapi_key` | — | Key del plan RapidAPI website-insights |
| `apollo_api_key` | — | Key Apollo |
| `worker_force_restart_at` | — | ISO timestamp; si > process_start, exit(1) → Railway redeploy |

## Operativa

- **Lun-Vie España**: agente, csv_queue y autopilot procesan normalmente
- **Sábado/Domingo**: pausa automática (consistente con horario operativo ADEQ)
- **Caps globales** se resetean al cambiar día calendario España

## Lo que NO puede hacer (necesita navegador)

- Scraping de emails desde el HTML JS-rendered del sitio (usa fetch puro)
- Análisis de banners visuales

Para esos casos, el MB humano usa la toolbar Chrome desde Analysis tab.
