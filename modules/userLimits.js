// ============================================================
// ADEQ Toolbar — User Limits
// Tabla Supabase: toolbar_user_limits
//   user_email          TEXT PRIMARY KEY
//   autopilot_enabled   BOOLEAN  DEFAULT TRUE
//   monthly_api_cap     INTEGER  -- NULL = sin cap individual
//   daily_emails_cap    INTEGER  DEFAULT 100
//   daily_monday_cap    INTEGER  DEFAULT 100
//   updated_at          TIMESTAMPTZ DEFAULT NOW()
// ============================================================

import { CONFIG } from "../config.js";

const DEFAULTS = {
  autopilot_enabled:       true,
  monthly_api_cap:         null,   // null = solo aplica el cap global de 40k
  daily_emails_cap:        100,
  daily_monday_cap:        100,
  autopilot_daily_minutes: 20,    // 20 min/sesión por default (auto-corte)
  autopilot_daily_prospects: 300, // 300 prospectos/día/user
};

function _headers(token) {
  return {
    "apikey":        CONFIG.SUPABASE_ANON_KEY,
    "Authorization": `Bearer ${token}`,
    "Content-Type":  "application/json",
  };
}

export async function fetchAllUserLimits(accessToken) {
  if (!accessToken) return [];
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_limits?select=*&order=user_email`,
      { headers: _headers(accessToken) }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch { return []; }
}

export async function fetchUserLimit(accessToken, email) {
  if (!accessToken || !email) return { ...DEFAULTS, user_email: email };
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_limits?user_email=eq.${encodeURIComponent(email)}&select=*&limit=1`,
      { headers: _headers(accessToken) }
    );
    if (!res.ok) return { ...DEFAULTS, user_email: email };
    const rows = await res.json();
    if (!rows.length) return { ...DEFAULTS, user_email: email };
    return { ...DEFAULTS, ...rows[0] };
  } catch { return { ...DEFAULTS, user_email: email }; }
}

export async function upsertUserLimit(accessToken, limit) {
  if (!accessToken || !limit?.user_email) return false;
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_limits`,
      {
        method: "POST",
        headers: { ..._headers(accessToken), "Prefer": "resolution=merge-duplicates,return=minimal" },
        body: JSON.stringify({
          user_email:                limit.user_email.toLowerCase(),
          autopilot_enabled:         !!limit.autopilot_enabled,
          monthly_api_cap:           limit.monthly_api_cap || null,
          daily_emails_cap:          parseInt(limit.daily_emails_cap || 100, 10),
          daily_monday_cap:          parseInt(limit.daily_monday_cap || 100, 10),
          autopilot_daily_minutes:   parseInt(limit.autopilot_daily_minutes || 60, 10),
          autopilot_daily_prospects: parseInt(limit.autopilot_daily_prospects || 75, 10),
          updated_at:                new Date().toISOString(),
        }),
      }
    );
    return res.ok;
  } catch { return false; }
}

export async function deleteUserLimit(accessToken, email) {
  if (!accessToken || !email) return false;
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_limits?user_email=eq.${encodeURIComponent(email)}`,
      { method: "DELETE", headers: _headers(accessToken) }
    );
    return res.ok;
  } catch { return false; }
}

// ── Per-user daily counters ────────────────────────────────────
// Reusamos la tabla toolbar_api_usage que ya existe — tiene by_provider.
// Para emails y monday agregamos campos custom: by_provider.emails / by_provider.monday_pushes
export async function getUserDailyUsage(accessToken, email) {
  if (!accessToken || !email) return { emails: 0, monday: 0 };
  const today = new Date().toISOString().slice(0, 10);
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage?user_email=eq.${encodeURIComponent(email)}&day=eq.${today}&select=by_provider`,
      { headers: _headers(accessToken) }
    );
    if (!res.ok) return { emails: 0, monday: 0 };
    const rows = await res.json();
    const by = rows[0]?.by_provider || {};
    return {
      emails: parseInt(by._emails_sent || 0, 10),
      monday: parseInt(by._monday_pushes || 0, 10),
    };
  } catch { return { emails: 0, monday: 0 }; }
}

export async function incrementUserDailyCounter(accessToken, email, kind) {
  // kind: "emails" | "monday" | "opens" | "sites"
  const FIELDS = { emails: "_emails_sent", monday: "_monday_pushes", opens: "_popup_opens", sites: "_sites_analyzed" };
  const field = FIELDS[kind];
  if (!accessToken || !email || !field) return;
  const today = new Date().toISOString().slice(0, 10);
  try {
    // Leer estado actual
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage?user_email=eq.${encodeURIComponent(email)}&day=eq.${today}&select=total,by_provider`,
      { headers: _headers(accessToken) }
    );
    const rows = res.ok ? await res.json() : [];
    const row  = rows[0] || { total: 0, by_provider: {} };
    const nextBy = { ...row.by_provider, [field]: (parseInt(row.by_provider[field] || 0, 10) + 1) };
    await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_api_usage`, {
      method:  "POST",
      headers: { ..._headers(accessToken), "Prefer": "resolution=merge-duplicates,return=minimal" },
      body:    JSON.stringify({
        user_email:  email.toLowerCase(),
        day:         today,
        total:       row.total || 0,
        by_provider: nextBy,
        updated_at:  new Date().toISOString(),
      }),
    });
  } catch (e) { console.warn("[userLimits] increment failed:", e.message); }
}

// Devuelve { allowed: bool, reason?: string } con el contexto actual del usuario.
export async function checkUserCanDo(accessToken, email, action) {
  // action: "send_email" | "push_monday" | "autopilot_on"
  const limit = await fetchUserLimit(accessToken, email);
  if (action === "autopilot_on" && !limit.autopilot_enabled) {
    return { allowed: false, reason: "Autopilot desactivado para tu usuario por el admin." };
  }
  if (action === "send_email" || action === "push_monday") {
    const usage = await getUserDailyUsage(accessToken, email);
    if (action === "send_email" && usage.emails >= limit.daily_emails_cap) {
      return { allowed: false, reason: `Límite diario de emails alcanzado (${usage.emails}/${limit.daily_emails_cap}).` };
    }
    if (action === "push_monday" && usage.monday >= limit.daily_monday_cap) {
      return { allowed: false, reason: `Límite diario de pushes Monday alcanzado (${usage.monday}/${limit.daily_monday_cap}).` };
    }
  }
  return { allowed: true };
}
