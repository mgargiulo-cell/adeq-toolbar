// ============================================================
// ADEQ Toolbar — Usage Tracking
// Mide tiempo real que cada user pasa con el popup abierto y con
// autopilot ON. Persiste en toolbar_usage_sessions.
//
// Tabla esperada:
//   id            UUID PRIMARY KEY DEFAULT gen_random_uuid()
//   user_email    TEXT NOT NULL
//   kind          TEXT NOT NULL  -- "popup" | "autopilot"
//   started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
//   ended_at      TIMESTAMPTZ
//   duration_sec  INTEGER         -- llena al cerrar
// ============================================================

import { CONFIG } from "../config.js";

let _currentSessionId = null;
let _currentKind = null;
let _heartbeatTimer = null;

function _headers(token) {
  return {
    "apikey":        CONFIG.SUPABASE_ANON_KEY,
    "Authorization": `Bearer ${token}`,
    "Content-Type":  "application/json",
  };
}

export async function startUsageSession(token, email, kind = "popup") {
  if (!token || !email) return null;
  if (_currentSessionId) await endUsageSession(token); // cerrar la previa
  try {
    const res = await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_usage_sessions`, {
      method: "POST",
      headers: { ..._headers(token), "Prefer": "return=representation" },
      body: JSON.stringify({
        user_email: email.toLowerCase(),
        kind,
        started_at: new Date().toISOString(),
      }),
    });
    if (!res.ok) return null;
    const rows = await res.json();
    _currentSessionId = rows?.[0]?.id || null;
    _currentKind     = kind;
    // Heartbeat: cada 60s actualizamos ended_at por si el browser muere sin onbeforeunload
    if (_heartbeatTimer) clearInterval(_heartbeatTimer);
    _heartbeatTimer = setInterval(() => {
      _patchSession(token, { ended_at: new Date().toISOString() }).catch(() => {});
    }, 60_000);
    return _currentSessionId;
  } catch { return null; }
}

async function _patchSession(token, body) {
  if (!_currentSessionId) return;
  return fetch(
    `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_usage_sessions?id=eq.${_currentSessionId}`,
    { method: "PATCH", headers: { ..._headers(token), "Prefer": "return=minimal" }, body: JSON.stringify(body) }
  );
}

export async function endUsageSession(token) {
  if (!_currentSessionId) return;
  if (_heartbeatTimer) { clearInterval(_heartbeatTimer); _heartbeatTimer = null; }
  const endedAt = new Date().toISOString();
  try {
    // Calcular duración aprox cliente-side (server podría tener trigger pero no asumimos)
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_usage_sessions?id=eq.${_currentSessionId}&select=started_at`,
      { headers: _headers(token) }
    );
    let durationSec = null;
    if (res.ok) {
      const rows = await res.json();
      const startedAt = rows?.[0]?.started_at;
      if (startedAt) durationSec = Math.round((new Date(endedAt) - new Date(startedAt)) / 1000);
    }
    await _patchSession(token, { ended_at: endedAt, duration_sec: durationSec });
  } catch {}
  _currentSessionId = null;
  _currentKind = null;
}

// Stats agregados para el admin dashboard
export async function fetchUsageStats(token, { from, to, userEmail = null } = {}) {
  if (!token) return [];
  const userClause = userEmail ? `&user_email=eq.${encodeURIComponent(userEmail)}` : "";
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_usage_sessions?started_at=gte.${from}&started_at=lte.${to}T23:59:59${userClause}&select=*`,
      { headers: _headers(token) }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch { return []; }
}
