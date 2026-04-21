// ============================================================
// ADEQ TOOLBAR — Módulo Supabase
// Tablas:
//   toolbar_historial       — historial de sitios analizados
//   toolbar_traffic_cache   — caché de tráfico (60 días)
//   toolbar_sendtrack       — seguimiento de envíos y follow-ups
//   toolbar_config          — API keys protegidas por RLS (solo auth users)
// ============================================================

import { CONFIG } from "../config.js";

async function getConfig() {
  return {
    url: CONFIG.SUPABASE_URL || "",
    key: CONFIG.SUPABASE_ANON_KEY || "",
  };
}

// ── Autenticación Supabase ────────────────────────────────────

export async function supabaseSignIn(email, password) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(`${url}/auth/v1/token?grant_type=password`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "apikey": key },
      body: JSON.stringify({ email, password }),
    });
    const data = await res.json();
    if (!res.ok) return { error: data?.error_description || data?.msg || "Credenciales incorrectas" };
    return {
      access_token:  data.access_token,
      refresh_token: data.refresh_token,
      expires_in:    data.expires_in,
    };
  } catch (e) {
    return { error: "Error de conexión: " + e.message };
  }
}

// Envía email de recovery. Supabase dispara el correo con el link de reset.
export async function supabaseResetPassword(email) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(`${url}/auth/v1/recover`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "apikey": key },
      body: JSON.stringify({ email }),
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      return { error: data?.error_description || data?.msg || "No se pudo enviar el email" };
    }
    return { ok: true };
  } catch (e) {
    return { error: "Error de conexión: " + e.message };
  }
}

export async function supabaseRefresh(refreshToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(`${url}/auth/v1/token?grant_type=refresh_token`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "apikey": key },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });
    const data = await res.json();
    if (!res.ok) return { error: data?.error_description || "Sesión expirada" };
    return {
      access_token:  data.access_token,
      refresh_token: data.refresh_token,
      expires_in:    data.expires_in,
    };
  } catch (e) {
    return { error: e.message };
  }
}

// Carga las API keys desde toolbar_config (requiere JWT de usuario autenticado)
export async function fetchApiKeys(accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(`${url}/rest/v1/toolbar_config?select=key,value`, {
      headers: {
        "apikey":        key,
        "Authorization": `Bearer ${accessToken}`,
      },
    });
    if (!res.ok) return null;
    const rows = await res.json();
    if (!Array.isArray(rows)) return null;
    const result = {};
    rows.forEach(r => { result[r.key] = r.value; });
    return result;
  } catch { return null; }
}

// ── Autopilot toggle ──────────────────────────────────────────
export async function getAutopilotEnabled(accessToken) {
  const { enabled } = await getAutopilotState(accessToken);
  return enabled;
}

// Returns { enabled: bool, sessionStart: Date|null }
export async function getAutopilotState(accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res  = await fetch(
      `${url}/rest/v1/toolbar_config?key=in.(auto_prospecting_enabled,auto_session_start)&select=key,value`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });
    const enabled      = map.auto_prospecting_enabled === "true";
    const sessionStart = map.auto_session_start ? new Date(map.auto_session_start) : null;
    return { enabled, sessionStart };
  } catch { return { enabled: false, sessionStart: null }; }
}

export async function setAutopilotEnabled(enabled, accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const headers = {
    "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json",
  };
  try {
    await fetch(`${url}/rest/v1/toolbar_config?key=eq.auto_prospecting_enabled`, {
      method: "PATCH", headers,
      body: JSON.stringify({ value: enabled ? "true" : "false" }),
    });
    // Si se enciende, guardar timestamp de inicio de sesión
    if (enabled) {
      const sessionStart = new Date().toISOString();
      const check = await fetch(`${url}/rest/v1/toolbar_config?key=eq.auto_session_start&select=key`, { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } });
      const exists = (await check.json())?.length > 0;
      await fetch(`${url}/rest/v1/toolbar_config${exists ? "?key=eq.auto_session_start" : ""}`, {
        method: exists ? "PATCH" : "POST", headers,
        body: JSON.stringify(exists ? { value: sessionStart } : { key: "auto_session_start", value: sessionStart }),
      });
    }
  } catch {}
}

export async function getAutopilotTarget(accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_config?key=in.(target_geo,target_category,min_traffic)&select=key,value`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value || ""; });
    return { geo: map.target_geo || "", category: map.target_category || "", minTraffic: map.min_traffic || "400000" };
  } catch { return { geo: "", category: "", minTraffic: "400000" }; }
}

export async function setAutopilotTarget(geo, category, minTraffic, accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const headers = { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates" };
  try {
    await fetch(`${url}/rest/v1/toolbar_config`, {
      method: "POST", headers,
      body: JSON.stringify([
        { key: "target_geo",      value: geo },
        { key: "target_category", value: category },
        { key: "min_traffic",     value: String(minTraffic || "400000") },
      ]),
    });
  } catch {}
}

// ── Review Queue — candidatos del auto-prospector para validación ─────
export async function fetchReviewQueue(accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  // Excluir columna pitch (texto largo) — se carga on-demand al generar
  const cols = "id,domain,traffic,geo,language,category,contact_name,emails,pitch_subject,pitch_subjects,score,ad_networks,page_title,status,validated_by,validated_at,created_at";
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_review_queue?status=eq.pending&order=score.desc,created_at.desc&limit=100&select=${cols}`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return [];
    const rows = await res.json();
    return Array.isArray(rows) ? rows : [];
  } catch { return []; }
}

export async function validateReviewItem(accessToken, id, validatedBy) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    await fetch(`${url}/rest/v1/toolbar_review_queue?id=eq.${id}`, {
      method: "PATCH",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ status: "validated", validated_by: validatedBy, validated_at: new Date().toISOString() }),
    });
  } catch {}
}

export async function rejectReviewItem(accessToken, id, domain) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const expiresAt = new Date(Date.now() + 10 * 365 * 24 * 60 * 60 * 1000).toISOString();
  try {
    await fetch(`${url}/rest/v1/toolbar_review_queue?id=eq.${id}`, {
      method: "PATCH",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ status: "rejected" }),
    });
    await fetch(`${url}/rest/v1/toolbar_import_queue`, {
      method: "POST",
      headers: {
        "apikey": key, "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates",
      },
      body: JSON.stringify([{ domain, imported_by: "REJECTED", expires_at: expiresAt }]),
    });
  } catch {}
}

export async function updateReviewItem(accessToken, id, data) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    await fetch(`${url}/rest/v1/toolbar_review_queue?id=eq.${id}`, {
      method: "PATCH",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
  } catch {}
}

export async function getDailyValidationCount(accessToken, userEmail) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const today = new Date().toISOString().split("T")[0];
    const res = await fetch(
      `${url}/rest/v1/toolbar_review_queue?validated_by=eq.${encodeURIComponent(userEmail)}&validated_at=gte.${today}T00:00:00Z&select=id`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("Content-Range") || "";
    return parseInt(range.split("/")[1]) || 0;
  } catch { return 0; }
}

// ── Import Queue — evitar que dos usuarios importen las mismas URLs ──
export async function getImportedDomains(accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const now = new Date().toISOString();
    const res = await fetch(
      `${url}/rest/v1/toolbar_import_queue?select=domain&expires_at=gt.${encodeURIComponent(now)}`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return new Set();
    const rows = await res.json();
    return new Set(Array.isArray(rows) ? rows.map(r => r.domain) : []);
  } catch { return new Set(); }
}

export async function markDomainsImported(domains, importedBy, accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const expiresAt = new Date(Date.now() + 60 * 24 * 60 * 60 * 1000).toISOString();
  const rows = domains.map(domain => ({ domain, imported_by: importedBy, expires_at: expiresAt }));
  try {
    await fetch(`${url}/rest/v1/toolbar_import_queue`, {
      method: "POST",
      headers: {
        "apikey": key,
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
      },
      body: JSON.stringify(rows),
    });
  } catch {}
}

function addDays(dateStr, days) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() + days);
  return d.toISOString().split("T")[0];
}

// ============================================================
// HISTORIAL
// ============================================================
export async function saveHistory(entry) {
  // Key de storage específica por usuario
  const buyer   = (entry.mediaBuyer || "shared").toLowerCase();
  const histKey = `history_${buyer}`;
  const { [histKey]: history = [] } = await chrome.storage.local.get(histKey);
  const filtered = history.filter(h => h.domain !== entry.domain);
  await chrome.storage.local.set({ [histKey]: [entry, ...filtered].slice(0, 50) });

  const { url, key } = await getConfig();
  if (!url || !key) return;

  try {
    await fetch(`${url}/rest/v1/toolbar_historial`, {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "apikey":        key,
        "Authorization": `Bearer ${key}`,
        "Prefer":        "return=minimal",
      },
      body: JSON.stringify({
        domain:      entry.domain,
        media_buyer: entry.mediaBuyer || "",
        page_views:  entry.pageViews  || 0,
        raw_visits:  entry.rawVisits  || 0,
        is_new:      entry.isNew,
        ejecutivo:   entry.ejecutivo  || "",
        email:       entry.email      || "",
        partners:    (entry.partners  || []).join(", "),
        geo:         entry.geo        || "",
        date:        entry.date       || new Date().toISOString().split("T")[0],
      }),
    });
  } catch (err) {
    console.warn("Supabase saveHistory failed:", err.message);
  }
}

export async function loadHistory() {
  const { url, key } = await getConfig();

  if (url && key) {
    try {
      const res = await fetch(
        `${url}/rest/v1/toolbar_historial?order=created_at.desc&limit=50`,
        { headers: { "apikey": key, "Authorization": `Bearer ${key}` } }
      );
      if (res.ok) {
        const rows = await res.json();
        return rows.map(r => ({
          domain:     r.domain,
          mediaBuyer: r.media_buyer,
          pageViews:  r.page_views,
          rawVisits:  r.raw_visits,
          isNew:      r.is_new,
          ejecutivo:  r.ejecutivo,
          email:      r.email,
          partners:   r.partners,
          geo:        r.geo,
          date:       r.date,
        }));
      }
    } catch {}
  }

  const { history = [] } = await chrome.storage.local.get("history");
  return history;
}

// ============================================================
// BASE DE KEYWORDS
// Tabla: toolbar_keywords (id, phrase, lang, source)
// ============================================================
export async function loadKeywordsFromDB() {
  const { url, key } = await getConfig();
  if (!url || !key) return [];
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_keywords?select=phrase,lang&order=id.asc&limit=5000`,
      { headers: { "apikey": key, "Authorization": `Bearer ${key}` } }
    );
    if (!res.ok) return [];
    return await res.json(); // [{ phrase, lang }]
  } catch { return []; }
}

// Búsqueda directa en Supabase (ilike) — siempre busca en todos los idiomas,
// el lang preferido se usa para ordenar (primero), no para filtrar.
// Retorna { rows: [], error: null|string }
export async function searchKeywordsInDB(term, preferLang = "") {
  const { url, key } = await getConfig();
  if (!url || !key) return { rows: [], error: "Sin configuración de Supabase" };
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_keywords?select=phrase,lang&phrase=ilike.*${encodeURIComponent(term)}*&order=phrase.asc&limit=200`,
      { headers: { "apikey": key, "Authorization": `Bearer ${key}` } }
    );
    if (!res.ok) {
      const errBody = await res.json().catch(() => ({}));
      const msg = errBody?.message || errBody?.hint || `HTTP ${res.status}`;
      return { rows: [], error: msg };
    }
    let rows = await res.json();
    if (!Array.isArray(rows)) return { rows: [], error: null };

    // Ordenar: idioma preferido primero, el resto después
    if (preferLang) {
      rows = [
        ...rows.filter(r => r.lang === preferLang),
        ...rows.filter(r => r.lang !== preferLang),
      ];
    }
    return { rows, error: null };
  } catch (e) {
    return { rows: [], error: e.message };
  }
}

export async function importKeywordsToDB(phrases, source = "import") {
  const { url, key } = await getConfig();
  if (!url || !key) return { count: 0 };
  // Insertar en batches de 200
  const rows = phrases.map(p => ({ phrase: p.phrase, lang: p.lang || "en", source }));
  let inserted = 0;
  for (let i = 0; i < rows.length; i += 200) {
    const batch = rows.slice(i, i + 200);
    try {
      await fetch(`${url}/rest/v1/toolbar_keywords`, {
        method: "POST",
        headers: {
          "Content-Type":  "application/json",
          "apikey":        key,
          "Authorization": `Bearer ${key}`,
          "Prefer":        "resolution=ignore-duplicates,return=minimal",
        },
        body: JSON.stringify(batch),
      });
      inserted += batch.length;
    } catch {}
  }
  return { count: inserted };
}

export async function clearKeywordsDB() {
  const { url, key } = await getConfig();
  if (!url || !key) return;
  try {
    await fetch(`${url}/rest/v1/toolbar_keywords?id=gte.0`, {
      method: "DELETE",
      headers: { "apikey": key, "Authorization": `Bearer ${key}` },
    });
  } catch {}
}

export async function countKeywordsDB() {
  const { url, key } = await getConfig();
  if (!url || !key) return 0;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_keywords?select=id`,
      { headers: { "apikey": key, "Authorization": `Bearer ${key}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("Content-Range") || "";
    return parseInt(range.split("/")[1]) || 0;
  } catch { return 0; }
}

export async function clearHistory() {
  const { url, key } = await getConfig();
  if (!url || !key) return;
  try {
    await fetch(`${url}/rest/v1/toolbar_historial?id=gte.0`, {
      method: "DELETE",
      headers: { "apikey": key, "Authorization": `Bearer ${key}` },
    });
  } catch (err) {
    console.warn("clearHistory failed:", err.message);
  }
}

export async function getMonthStats() {
  const history   = await loadHistory();
  const thisMonth = new Date().toISOString().substring(0, 7);
  const monthly   = history.filter(h => (h.date || "").startsWith(thisMonth));
  return {
    total:     history.length,
    thisMonth: monthly.length,
    nuevos:    monthly.filter(h => h.isNew).length,
    dups:      monthly.filter(h => !h.isNew).length,
  };
}

// ============================================================
// CACHÉ DE SIMILARES — 60 días
// Tabla: toolbar_similar_cache
//   domain TEXT PRIMARY KEY, sites JSONB, fetched_at TIMESTAMPTZ DEFAULT NOW()
// ============================================================
export async function getSimilarCache(domain) {
  const { url, key } = await getConfig();
  if (!url || !key) return null;
  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 60);
    const res = await fetch(
      `${url}/rest/v1/toolbar_similar_cache?domain=eq.${encodeURIComponent(domain)}&fetched_at=gte.${cutoff.toISOString()}&limit=1`,
      { headers: { "apikey": key, "Authorization": `Bearer ${key}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows.length ? rows[0].sites : null;
  } catch { return null; }
}

export async function saveSimilarCache(domain, sites) {
  const { url, key } = await getConfig();
  if (!url || !key) return;
  try {
    await fetch(`${url}/rest/v1/toolbar_similar_cache`, {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "apikey":        key,
        "Authorization": `Bearer ${key}`,
        "Prefer":        "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({ domain, sites, fetched_at: new Date().toISOString() }),
    });
  } catch (err) { console.warn("saveSimilarCache failed:", err.message); }
}

// ============================================================
// CACHÉ DE TRÁFICO — 60 días
// Tabla: toolbar_traffic_cache
//   domain TEXT PRIMARY KEY, data JSONB, fetched_at TIMESTAMPTZ DEFAULT NOW()
// ============================================================
export async function getTrafficCache(domain) {
  const { url, key } = await getConfig();
  if (!url || !key) return null;

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 60);

    const res = await fetch(
      `${url}/rest/v1/toolbar_traffic_cache?domain=eq.${encodeURIComponent(domain)}&fetched_at=gte.${cutoff.toISOString()}&limit=1`,
      { headers: { "apikey": key, "Authorization": `Bearer ${key}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    if (!rows.length) return null;

    const daysAgo = Math.floor((Date.now() - new Date(rows[0].fetched_at)) / 86_400_000);
    return { ...rows[0].data, fromCache: true, cachedDaysAgo: daysAgo };
  } catch {
    return null;
  }
}

export async function saveTrafficCache(domain, data) {
  const { url, key } = await getConfig();
  if (!url || !key) return;

  try {
    const clean = { ...data };
    delete clean.fromCache;
    delete clean.cachedDaysAgo;

    await fetch(`${url}/rest/v1/toolbar_traffic_cache`, {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "apikey":        key,
        "Authorization": `Bearer ${key}`,
        "Prefer":        "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({ domain, data: clean, fetched_at: new Date().toISOString() }),
    });
  } catch (err) {
    console.warn("Cache save failed:", err.message);
  }
}

// ============================================================
// SEGUIMIENTO DE ENVÍOS Y FOLLOW-UPS
// Tabla: toolbar_sendtrack
//   domain    TEXT PRIMARY KEY
//   send_date DATE
//   fu1_date  DATE
//   fu2_date  DATE
//   fu1_sent  BOOLEAN DEFAULT FALSE
//   fu2_sent  BOOLEAN DEFAULT FALSE
//   pitch     TEXT
//   email     TEXT
// ============================================================
export async function saveSendDate(domain, { sendDate, pitch, email }) {
  const fu1Date = addDays(sendDate, 7);
  const fu2Date = addDays(sendDate, 14);

  // Chrome storage fallback
  const { sendtrack = {} } = await chrome.storage.local.get("sendtrack");
  sendtrack[domain] = { sendDate, fu1Date, fu2Date, fu1Sent: false, fu2Sent: false, pitch, email };
  await chrome.storage.local.set({ sendtrack });

  const { url, key } = await getConfig();
  if (url && key) {
    try {
      await fetch(`${url}/rest/v1/toolbar_sendtrack`, {
        method: "POST",
        headers: {
          "Content-Type":  "application/json",
          "apikey":        key,
          "Authorization": `Bearer ${key}`,
          "Prefer":        "resolution=merge-duplicates,return=minimal",
        },
        body: JSON.stringify({
          domain,
          send_date: sendDate,
          fu1_date:  fu1Date,
          fu2_date:  fu2Date,
          fu1_sent:  false,
          fu2_sent:  false,
          pitch:     pitch || "",
          email:     email || "",
        }),
      });
    } catch (err) {
      console.warn("SendTrack save failed:", err.message);
    }
  }

  return { fu1Date, fu2Date };
}

export async function getSendInfo(domain) {
  const { url, key } = await getConfig();

  if (url && key) {
    try {
      const res = await fetch(
        `${url}/rest/v1/toolbar_sendtrack?domain=eq.${encodeURIComponent(domain)}&limit=1`,
        { headers: { "apikey": key, "Authorization": `Bearer ${key}` } }
      );
      if (res.ok) {
        const rows = await res.json();
        if (rows.length) {
          const r = rows[0];
          return {
            sendDate: r.send_date,
            fu1Date:  r.fu1_date,
            fu2Date:  r.fu2_date,
            fu1Sent:  r.fu1_sent,
            fu2Sent:  r.fu2_sent,
            pitch:    r.pitch,
            email:    r.email,
          };
        }
      }
    } catch {}
  }

  const { sendtrack = {} } = await chrome.storage.local.get("sendtrack");
  const t = sendtrack[domain];
  if (!t) return null;
  return {
    sendDate: t.sendDate,
    fu1Date:  t.fu1Date,
    fu2Date:  t.fu2Date,
    fu1Sent:  t.fu1Sent  || false,
    fu2Sent:  t.fu2Sent  || false,
    pitch:    t.pitch    || "",
    email:    t.email    || "",
  };
}

export async function markFUSent(domain, fuNumber) {
  const { sendtrack = {} } = await chrome.storage.local.get("sendtrack");
  if (sendtrack[domain]) {
    sendtrack[domain][`fu${fuNumber}Sent`] = true;
    await chrome.storage.local.set({ sendtrack });
  }

  const { url, key } = await getConfig();
  if (!url || !key) return;

  try {
    await fetch(
      `${url}/rest/v1/toolbar_sendtrack?domain=eq.${encodeURIComponent(domain)}`,
      {
        method: "PATCH",
        headers: {
          "Content-Type":  "application/json",
          "apikey":        key,
          "Authorization": `Bearer ${key}`,
          "Prefer":        "return=minimal",
        },
        body: JSON.stringify({ [`fu${fuNumber}_sent`]: true }),
      }
    );
  } catch (err) {
    console.warn("markFUSent failed:", err.message);
  }
}

// ── CSV Queue — batch de dominios a procesar por el auto-prospector ───
// source: "csv" (External URLs) o "monday" (Monday URL Auto Prospector)
export async function uploadCsvDomains(domains, userEmail, accessToken, source = "csv") {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!Array.isArray(domains) || domains.length === 0) return { inserted: 0 };

  // Batch de 500 (límite seguro de Supabase REST)
  const BATCH = 500;
  let inserted = 0;
  for (let i = 0; i < domains.length; i += BATCH) {
    const slice = domains.slice(i, i + BATCH).map(d => ({
      domain: d, status: "pending", uploaded_by: userEmail, source,
    }));
    try {
      const res = await fetch(`${url}/rest/v1/toolbar_csv_queue`, {
        method: "POST",
        headers: {
          "apikey": key, "Authorization": `Bearer ${accessToken}`,
          "Content-Type": "application/json",
          "Prefer": "resolution=ignore-duplicates,return=representation",
        },
        body: JSON.stringify(slice),
      });
      if (res.ok) {
        const rows = await res.json().catch(() => []);
        inserted += Array.isArray(rows) ? rows.length : 0;
      }
    } catch (e) {
      console.warn("uploadCsvDomains batch failed:", e.message);
    }
  }
  return { inserted, attempted: domains.length };
}

// ── Pitch Drafts — borradores guardados por usuario y idioma ──
export async function getPitchDrafts(accessToken, userEmail, language = null) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const langFilter = language ? `&language=eq.${language}` : "";
  // Trae borradores del user + los defaults (_default_)
  const userFilter = `user_email=in.(${encodeURIComponent(userEmail)},_default_)`;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_pitch_drafts?${userFilter}${langFilter}&order=is_default.desc,updated_at.desc&select=id,user_email,name,language,subject,body,is_default,updated_at`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch (e) { console.warn("getPitchDrafts:", e.message); return []; }
}

export async function savePitchDraft(accessToken, { id, user_email, name, language, subject, body }) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const payload = { user_email, name, language, subject: subject || "", body, updated_at: new Date().toISOString() };
  try {
    if (id) {
      const res = await fetch(`${url}/rest/v1/toolbar_pitch_drafts?id=eq.${id}`, {
        method: "PATCH",
        headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json", "Prefer": "return=representation" },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => []);
      return { ok: res.ok, data: data?.[0] || null };
    }
    const res = await fetch(`${url}/rest/v1/toolbar_pitch_drafts`, {
      method: "POST",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=representation" },
      body: JSON.stringify([payload]),
    });
    const data = await res.json().catch(() => []);
    return { ok: res.ok, data: data?.[0] || null, error: res.ok ? null : (data?.message || `HTTP ${res.status}`) };
  } catch (e) { return { ok: false, error: e.message }; }
}

export async function deletePitchDraft(accessToken, id) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    await fetch(`${url}/rest/v1/toolbar_pitch_drafts?id=eq.${id}`, {
      method: "DELETE",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` },
    });
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// Últimos N dominios procesados (done/error/skipped) ordenados por fecha
// sourceFilter: "csv" | "monday" | null (todos)
export async function getCsvQueueHistory(accessToken, limit = 30, sourceFilter = null) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const sourceQuery = sourceFilter ? `&source=eq.${sourceFilter}` : "";
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_csv_queue?status=in.(done,error,skipped)${sourceQuery}&order=processed_at.desc.nullslast&limit=${limit}&select=domain,status,processed_at,error_message,monday_item_id,source`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return [];
    const rows = await res.json();
    return Array.isArray(rows) ? rows : [];
  } catch (e) {
    console.warn("getCsvQueueHistory failed:", e.message);
    return [];
  }
}

export async function getCsvQueueStats(accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const stats = { total: 0, pending: 0, processing: 0, done: 0, error: 0, skipped: 0 };
  try {
    // Usamos HEAD + Prefer: count=exact para obtener totales por status
    const statuses = Object.keys(stats).filter(k => k !== "total");
    const results = await Promise.all(statuses.map(async (s) => {
      const res = await fetch(
        `${url}/rest/v1/toolbar_csv_queue?status=eq.${s}&select=id`,
        { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0" } }
      );
      const contentRange = res.headers.get("content-range") || "";
      const match = contentRange.match(/\/(\d+)$/);
      return { status: s, count: match ? parseInt(match[1]) : 0 };
    }));
    results.forEach(r => { stats[r.status] = r.count; stats.total += r.count; });
  } catch (e) {
    console.warn("getCsvQueueStats failed:", e.message);
  }
  return stats;
}

export async function clearCsvQueue(accessToken, onlyProcessed = false) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const filter = onlyProcessed ? "?status=in.(done,error,skipped)" : "?id=gte.0";
  try {
    await fetch(`${url}/rest/v1/toolbar_csv_queue${filter}`, {
      method: "DELETE",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` },
    });
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

export async function getCsvQueueEnabled(accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_config?key=eq.csv_queue_enabled&select=value`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    const rows = await res.json();
    return rows?.[0]?.value === "true";
  } catch { return false; }
}

export async function setCsvQueueEnabled(enabled, accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    await fetch(`${url}/rest/v1/toolbar_config?key=eq.csv_queue_enabled`, {
      method: "PATCH",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ value: enabled ? "true" : "false" }),
    });
  } catch (e) {
    console.warn("setCsvQueueEnabled failed:", e.message);
  }
}
