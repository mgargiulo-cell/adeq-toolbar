// ============================================================
// ADEQ Toolbar — Coordinación entre MBs
// - Lock de prospect (un MB lo "claima" X minutos para evitar trabajo doble)
// - Hand-off entre MBs (transferir lead sin tocar Monday manualmente)
// - "En vacaciones" toggle (otros MBs ven al usuario inactivo)
//
// Tablas Supabase necesarias:
//
//   toolbar_prospect_locks
//     domain         TEXT PRIMARY KEY
//     locked_by      TEXT NOT NULL
//     locked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
//     expires_at     TIMESTAMPTZ NOT NULL
//
//   toolbar_handoffs
//     id             UUID PRIMARY KEY DEFAULT gen_random_uuid()
//     domain         TEXT NOT NULL
//     monday_item_id TEXT
//     from_email     TEXT NOT NULL
//     to_email       TEXT NOT NULL
//     note           TEXT
//     created_at     TIMESTAMPTZ DEFAULT NOW()
//     status         TEXT DEFAULT 'pending'   -- 'pending' | 'accepted' | 'rejected'
//
//   toolbar_user_status
//     user_email     TEXT PRIMARY KEY
//     vacation_until DATE                     -- NULL = activo
//     updated_at     TIMESTAMPTZ DEFAULT NOW()
// ============================================================

import { CONFIG } from "../config.js";

const LOCK_DURATION_MIN = 30; // 30 min de lock por defecto

function _headers(token) {
  return {
    "apikey":        CONFIG.SUPABASE_ANON_KEY,
    "Authorization": `Bearer ${token}`,
    "Content-Type":  "application/json",
  };
}

// ── Locks ──────────────────────────────────────────────────
export async function lockProspect(token, domain, email) {
  if (!token || !domain || !email) return { ok: false };
  const expiresAt = new Date(Date.now() + LOCK_DURATION_MIN * 60_000).toISOString();
  try {
    // Si existe lock activo de OTRO usuario, no overwritear
    const cur = await getActiveProspectLock(token, domain);
    if (cur && cur.locked_by.toLowerCase() !== email.toLowerCase()) {
      return { ok: false, owned_by: cur.locked_by, expires_at: cur.expires_at };
    }
    const res = await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_prospect_locks`, {
      method: "POST",
      headers: { ..._headers(token), "Prefer": "resolution=merge-duplicates,return=minimal" },
      body: JSON.stringify({ domain, locked_by: email.toLowerCase(), locked_at: new Date().toISOString(), expires_at: expiresAt }),
    });
    return { ok: res.ok, expires_at: expiresAt };
  } catch { return { ok: false }; }
}

export async function getActiveProspectLock(token, domain) {
  if (!token || !domain) return null;
  try {
    const now = new Date().toISOString();
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_prospect_locks?domain=eq.${encodeURIComponent(domain)}&expires_at=gt.${now}&select=*&limit=1`,
      { headers: _headers(token) }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows?.[0] || null;
  } catch { return null; }
}

export async function unlockProspect(token, domain, email) {
  if (!token || !domain) return;
  try {
    await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_prospect_locks?domain=eq.${encodeURIComponent(domain)}&locked_by=eq.${encodeURIComponent(email)}`,
      { method: "DELETE", headers: _headers(token) }
    );
  } catch {}
}

// ── Hand-offs ──────────────────────────────────────────────
export async function createHandoff(token, { domain, monday_item_id, from_email, to_email, note }) {
  if (!token) return { ok: false };
  try {
    const res = await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_handoffs`, {
      method: "POST",
      headers: { ..._headers(token), "Prefer": "return=minimal" },
      body: JSON.stringify({
        domain, monday_item_id, from_email, to_email,
        note: note || "",
        status: "pending",
        created_at: new Date().toISOString(),
      }),
    });
    return { ok: res.ok };
  } catch { return { ok: false }; }
}

export async function fetchPendingHandoffsForUser(token, email) {
  if (!token || !email) return [];
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_handoffs?to_email=eq.${encodeURIComponent(email)}&status=eq.pending&select=*&order=created_at.desc`,
      { headers: _headers(token) }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch { return []; }
}

export async function updateHandoffStatus(token, id, status) {
  if (!token || !id) return;
  try {
    await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_handoffs?id=eq.${id}`,
      { method: "PATCH", headers: { ..._headers(token), "Prefer": "return=minimal" }, body: JSON.stringify({ status }) }
    );
  } catch {}
}

// ── Vacation status ────────────────────────────────────────
export async function setVacationStatus(token, email, until) {
  if (!token || !email) return;
  try {
    await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_status`, {
      method: "POST",
      headers: { ..._headers(token), "Prefer": "resolution=merge-duplicates,return=minimal" },
      body: JSON.stringify({ user_email: email.toLowerCase(), vacation_until: until || null, updated_at: new Date().toISOString() }),
    });
  } catch {}
}

export async function getUserStatus(token, email) {
  if (!token || !email) return null;
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_status?user_email=eq.${encodeURIComponent(email)}&select=*&limit=1`,
      { headers: _headers(token) }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows?.[0] || null;
  } catch { return null; }
}

export async function getActiveUsers(token) {
  // Devuelve lista de usuarios NO en vacaciones (vacation_until null o pasado)
  if (!token) return [];
  try {
    const today = new Date().toISOString().slice(0, 10);
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_user_status?or=(vacation_until.is.null,vacation_until.lt.${today})&select=user_email`,
      { headers: _headers(token) }
    );
    if (!res.ok) return [];
    const rows = await res.json();
    return rows.map(r => r.user_email);
  } catch { return []; }
}
