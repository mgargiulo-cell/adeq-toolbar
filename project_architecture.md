---
name: ADEQ Toolbar — Module Architecture & Functions
description: Complete breakdown of all modules, their exports, and what each function does
type: project
---

## File Structure

```
adeq-toolbar/
├── manifest.json
├── config.js                  ← All API keys + CONFIG object
├── popup/
│   ├── popup.html
│   ├── popup.js               ← Main orchestrator (~1100 lines)
│   └── popup.css
├── background/service-worker.js
├── content/content-script.js
└── modules/
    ├── monday.js
    ├── gemini.js
    ├── geminiSearch.js
    ├── traffic.js
    ├── scraper.js
    ├── audit.js
    ├── bannerDetector.js
    ├── scoring.js
    ├── keywords.js
    ├── cascade.js
    ├── emailVerifier.js
    ├── supabase.js
    └── gmail.js
```

---

## modules/monday.js

- `checkDuplicate(domain)` → Searches Monday.com board for existing item by domain. Returns `{ found, itemId, status, ejecutivo, trafico, email, geo, fecha }`.
- `pushToMonday(data)` → Creates a new item on the Monday board. Fields: domain, traffic, email, geo, pitch, techDetected, estado, fecha, idioma.
- `updateMonday({itemId, ...})` → Updates existing item via `change_multiple_column_values`.
- `recycleProspect({itemId, ...})` → Wraps `updateMonday` with "♻️ Reciclado" prefix in pitch.
- `getMondayBoardIndex()` → Fetches up to 500 items and returns a `Map<domain, { ejecutivo, fecha }>` — used by Cascade to filter prospects already owned by other executives.
- `setFollowUpDates(itemId, fu1Date, fu2Date)` → Sets FU1/FU2 date columns on a Monday item.
- `MONDAY_STATES` → Enum of deal stages (LIVE=0, EN_NEGOCIACION=1, ..., MASIVO_MAX=10).
- `RECYCLABLE_STATES` → ["Ciclo Finalizado", "Rebotado", "Descartado"]
- `cleanDomain(str)` → Internal helper: strips http/www/trailing slash.

**Monday Board ID:** 1420268379 (prospectos)  
**Key columns:** deal_stage, deal_owner, deal_close_date, text_mkrwahsz (texto), fecha2 (fu1), fecha_1 (fu2), texto6 (geo), texto7 (trafico), texto1 (plataforma), estado_12 (idioma), tel_fono_1, texto (comentarios), text_mksvsz9x (email_secundario), text_mksva60r (email), email_mksvdag (correo), text_mksnnqxj (ejecutivo_txt)

---

## modules/gemini.js

Uses **gemini-2.0-flash-001**. All generation goes through `callGemini(systemPrompt, userMessage, config)`.

- `generatePitch(ctx)` → Generates a 120-word prospecting email in English. Takes `{ domain, traffic, techStack, adsTxt, revenueGap, category }`. Has category-aware system prompts for: sports, news, finance, technology, entertainment, health, travel.
- `generateFollowUp({ domain, originalPitch, fuNumber, daysSinceSend })` → Short follow-up (60 words for FU1, 40 for FU2) referencing original pitch.
- `analyzeRevenueGap(ctx)` → 2-3 line RPM potential estimate in Spanish.
- `getCategoryContext(category)` → Internal: maps category string to sales context prompt.

---

## modules/geminiSearch.js

Uses **gemini-2.0-flash** (no -001 suffix).

- `searchEmailsWithGemini(domain)` → Asks Gemini for public contact info. Returns `{ emails[], owner, linkedin, note }`.
- `batchSearchEmails(domains[])` → Batch search for up to 20 domains. Returns `[{ domain, email, name }]`.

---

## modules/traffic.js

Uses **RapidAPI SimilarWeb Insights**. Primary endpoint: `/traffic`. Fallback: `/similar-sites`.

- `getTraffic(domain)` → Fetches traffic. First checks Supabase 60-day cache. Returns `{ visits, pagesPerVisit, pageViews, rawVisits, noPageViewData, category, categoryRank, globalRank, topCountries[], tags }`. `pageViews = visits × pagesPerVisit` (solo si la API devuelve ambos, nunca inventado). `pagesPerVisit: null` si no viene en la API — no usa default.
- `fetchTopCountries(domain)` → Internal. Llama `/countries?domain=`. Returns `[{ code, name, share }]` top 3.
- `getMonthlyApiCalls()` → Reads Chrome storage counter for current month's API calls.
- `getApiLimits()` → Returns `{ limit, remaining }` from last response headers.
- `formatTraffic(num)` → Formats number to "1.2M", "500K", etc.
- `passesTrafficFilter(pageViews)` → Returns `pageViews >= 500000`.

---

## modules/scraper.js

- `scrapeEmailsFromPage(tabId)` → Injects `extractEmailsFromDOM` into active page. Searches body text, innerHTML, and mailto links.
- `scrapeInformer(domain)` → Fetches website.informer.com/{domain}, extracts emails and phone numbers.
- `scrapeContactPages(baseUrl)` → Tries /contact, /about, /advertise, etc. paths.
- `validateEmailFormat(email)` → Regex validation.
- `findDecisionMakerViaApollo(domain)` → Calls Apollo.io mixed_people search for owner/founder/c_suite/vp/director. Returns `{ name, email, title, linkedin }`.
- `filterEmails(emails)` → Removes emails from ignored domains (google.com, sentry.io, etc.).

---

## modules/audit.js

Detects 24 ad tech partners by injecting script into the page.

- `runAudit(baseUrl, monthlyTraffic)` → Parallel: checks ads.txt + detects ad tech from DOM. Returns `{ adsTxt, techStack, revenueGap, allPartners, summary }`.
- `checkAdsTxt(baseUrl)` → Fetches /ads.txt. Returns `{ exists, entries, hasGoogle, hasEzoic, raw }`.
- `detectAdsTech()` → Injects `detectAdsFromDOM` into page. Returns array of detected partner names.
- `estimateRevenueGap(traffic, techStack, adsTxt)` → Calculates revenue gap %. Factors: no ads.txt (+25%), ads.txt < 5 entries (+15%), no header bidding (+30%), AdSense without GAM (+20%). Caps at 80%.
- `buildSummary(adsTxt, techStack, revenueGap)` → Returns `[{ status: "ok|warn|error", text }]` items.

**Partners detected (24):** Google AdSense, Google Ad Manager, Amazon Ads, Criteo, Taboola, Outbrain, MGID, Mediavine, AdThrive, Raptive, Freestar, Setupad, Publift, AdPushup, Monumetric, Ezoic, Seedtag, Clickio, Truvid, Sparteo, Vidoomy, Refinery89, Vidverto, Optad360.

---

## modules/scoring.js

- `scoreProspect({ pageViews, rawVisits, partners, emailFound })` → Returns `{ grade: "A|B|C|D", color, label }`.
  - Traffic: 0-50pts (50M+=50, 10M+=42, 5M+=35, 2M+=28, 1M+=20, 500K+=12, else 4)
  - Partners found: 0-30pts (0 found=30pts best, 4+ found=0pts — more partners = less opportunity)
  - Email found: 0-20pts
  - A≥75%, B≥55%, C≥35%, D<35%

---

## modules/supabase.js

Three Supabase tables. Falls back to Chrome local storage if Supabase not configured.

**Tables:**
- `toolbar_historial` — history of analyzed sites
- `toolbar_traffic_cache` — 60-day traffic cache (domain PK)
- `toolbar_sendtrack` — email send tracking + FU dates (domain PK)

**Functions:**
- `saveHistory(entry)` / `loadHistory()` / `getMonthStats()` — history CRUD
- `getTrafficCache(domain)` / `saveTrafficCache(domain, data)` — 60-day cache
- `saveSendDate(domain, { sendDate, pitch, email })` → Creates sendtrack row, auto-sets FU1=+7days, FU2=+14days. Returns `{ fu1Date, fu2Date }`.
- `getSendInfo(domain)` → Returns sendtrack row if exists.
- `markFUSent(domain, fuNumber)` → Sets fu1_sent or fu2_sent = true.

---

## modules/gmail.js

- `getGmailProfile()` → Non-interactive OAuth token → fetches Gmail profile. Returns `{ email }` or null.
- `sendEmail({ to, subject, body })` → Gets interactive OAuth token → builds RFC 2822 → sends via Gmail API. Returns `{ ok, error? }`.
- `buildRaw({ to, subject, body })` → Internal: base64url encodes RFC 2822 message.

---

## modules/cascade.js

- `getSimilarSites(domain)` → Fetches `/similar-sites` for a domain. Filters by MIN_TRAFFIC (500K). Returns array with `{ domain, title, visits, country, countryCode, globalRank, description, favicon }`.
- `runCascade(seedDomain, onProgress)` → 2-level cascade: seed → 10 similar → each of those → 10 more. 1.5s delay between level-2 requests. Reports results via `onProgress({ status, site, level })` callback.

---

## popup/popup.js — Main Orchestrator

**Global state object:**
```js
state = { domain, url, tabId, traffic, visits, pagesPerVisit, trafficData,
          emails[], techStack[], partners[], banners, adsTxt, revenueGap,
          pitch, duplicate, mediaBuyer, score, mondayItemId, mondaySnapshot, sendInfo, category }
```
`pagesPerVisit` defaults to `null` (not 1) — never invented.

**Auto-push flags:** `autoPushReady = { traffic, notDup, email }` — shows banner when all 3 true.

**Init flow (DOMContentLoaded):**
1. Check auth (30-day session from Chrome storage)
2. Get active tab URL, extract domain
3. prefillMondayForm, initTabs, bindButtons, initKeywords
4. Load history tab
5. Get Gmail profile (non-blocking)
6. Check follow-up status (non-blocking)
7. Update API footer
8. `Promise.all([runDuplicateCheck, runTrafficCheck, runAuditCheck, runEmailScraper])` — all in parallel

**Key functions:**
- `runDuplicateCheck()` → calls `checkDuplicate`, sets autoPushReady.notDup
- `runTrafficCheck()` → calls `getTraffic`, maps category, sets autoPushReady.traffic
- `runAuditCheck()` → calls `runAudit`, renders partners chips
- `runEmailScraper()` → parallel page scraping + informer scraping, saves history
- `updateScore()` → calls `scoreProspect`, renders score badge
- `checkAutoPush()` → shows auto-push banner when all 3 conditions met
- `checkFUStatus(sendInfo)` → shows FU banner if FU1 or FU2 date has passed
- `detectGeo()` → infers geo from TLD (.es→ES, .mx→MX, etc.)
- `mapCategory(category)` → maps SimilarWeb category to pitch category enum
- `countryFlag(code)` → converts ISO 2-letter code to flag emoji (🇦🇷, 🇺🇸, etc.)
- `startCascade()` → runs cascade with filters (min traffic, max rank, language, depth 1 or 2), filters out prospects from other executives (last 45 days from Monday index)
- `initLoginScreen()` → Email must be @adeqmedia.com, password = "AdeqmediaT!"
- `applyUserFromAuth(auth)` → derives mediaBuyer name from email prefix

---

## config.js

Single `CONFIG` export with all API keys:
- Monday.com API key + board ID (1420268379)
- RapidAPI key (SimilarWeb)
- Gemini API key
- Apollo.io API key
- Supabase URL + anon key
- Gmail OAuth Client ID
- MEDIA_BUYER: "Max"
- MIN_TRAFFIC: 500000
