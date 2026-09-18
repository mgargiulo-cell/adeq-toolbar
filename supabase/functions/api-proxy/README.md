# api-proxy Edge Function

Proxies Gemini / Apollo / RapidAPI calls through Supabase so the API keys never ship to the client extension bundle. Validates user JWT, enforces per-user daily quota (total + per-provider), writes usage to `toolbar_api_usage`.

## Deploy

```bash
# 1. Install Supabase CLI once
brew install supabase/tap/supabase

# 2. Log in and link the project
supabase login
supabase link --project-ref <YOUR_PROJECT_REF>  # from Supabase dashboard URL

# 3. Set secrets (one-time; replace with real keys)
supabase secrets set \
  GEMINI_API_KEY=AIza... \
  APOLLO_API_KEY=... \
  RAPIDAPI_KEY=...

# 4. Deploy
supabase functions deploy api-proxy --no-verify-jwt
```

`--no-verify-jwt` is required because the function validates the JWT itself (so it can read the user email from it). Without the flag, Supabase rejects requests before our code runs.

## Create usage tracking table

Run `sql/api_usage_table.sql` in Supabase SQL editor (once).

## Remove client-side keys

After confirming the proxy works end-to-end:

```sql
delete from toolbar_config where key in ('gemini_api_key','apollo_api_key','rapidapi_key');
```

Once removed, client `CONFIG.GEMINI_API_KEY` etc. will be empty strings — harmless, the proxy is the only path now.

## Quotas

Edit constants at the top of `index.ts`:
- `DAILY_QUOTA_PER_USER` — total calls per user per day (default 500)
- `PROVIDER_CAPS` — per-provider caps (default: gemini 300, apollo 150, rapidapi 400)

After editing, redeploy: `supabase functions deploy api-proxy --no-verify-jwt`.

## Monitoring

```sql
-- Today's usage by user
select user_email, total, by_provider
from toolbar_api_usage
where day = current_date
order by total desc;

-- Weekly totals
select user_email, sum(total) as calls_7d
from toolbar_api_usage
where day >= current_date - 7
group by user_email
order by calls_7d desc;
```
