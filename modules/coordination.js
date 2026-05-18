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
// Atómico via RPC lock_prospect (advisory lock por dominio). Ver
// sql/2026-05-18_atomic_prospect_lock.sql. El check-then-write previo
// tenía una race window que dejaba que dos MBs se pisaran el lock.
export async function lockProspect(token, domain, email) {
  if (!token || !domain || !email) return { ok: false };
  try {
    const res = await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/rpc/lock_prospect`, {
      method: "POST",
      headers: _headers(token),
      body: JSON.stringify({ p_domain: domain, p_email: email, p_minutes: LOCK_DURATION_MIN }),
    });
    if (!res.ok) return { ok: false };
    const rows = await res.json();
    const row = Array.isArray(rows) ? rows[0] : rows;
    if (!row) return { ok: false };
    if (row.ok) return { ok: true, expires_at: row.expires_at };
    return { ok: false, owned_by: row.locked_by, expires_at: row.expires_at };
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

// TTL de 7 días — handoffs que nadie aceptó en 7 días se descartan del feed.
// Evita que se acumulen pendings de gente que no usó la toolbar en mucho tiempo.
const HANDOFF_TTL_DAYS = 7;
export async function fetchPendingHandoffsForUser(token, email) {
  if (!token || !email) return [];
  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - HANDOFF_TTL_DAYS);
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_handoffs?to_email=eq.${encodeURIComponent(email)}&status=eq.pending&created_at=gte.${cutoff.toISOString()}&select=*&order=created_at.desc`,
      { headers: _headers(token) }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch { return []; }
}

// Marca como expired los handoffs viejos (admin-callable, opcional).
export async function expireOldHandoffs(token) {
  if (!token) return;
  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - HANDOFF_TTL_DAYS);
    await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_handoffs?status=eq.pending&created_at=lt.${cutoff.toISOString()}`,
      { method: "PATCH", headers: { ..._headers(token), "Prefer": "return=minimal" }, body: JSON.stringify({ status: "expired" }) }
    );
  } catch {}
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
