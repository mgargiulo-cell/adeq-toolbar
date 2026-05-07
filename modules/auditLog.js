// ============================================================
// ADEQ Toolbar — Audit Log
// Tabla esperada:
//   id          UUID PRIMARY KEY DEFAULT gen_random_uuid()
//   user_email  TEXT NOT NULL
//   action      TEXT NOT NULL    -- ej: "set_user_limit", "edit_blocklist", "vacation_on"
//   target      TEXT             -- email/dominio afectado, opcional
//   details     JSONB            -- diff, valor anterior, etc.
//   created_at  TIMESTAMPTZ DEFAULT NOW()
// ============================================================

import { CONFIG } from "../config.js";

export async function logAuditEvent(token, { user_email, action, target = null, details = null }) {
  if (!token || !user_email || !action) return;
  try {
    await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_audit_log`, {
      method: "POST",
      headers: {
        "apikey":        CONFIG.SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${token}`,
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
      },
      body: JSON.stringify({
        user_email: user_email.toLowerCase(),
        action,
        target,
        details,
        created_at: new Date().toISOString(),
      }),
    });
  } catch {}
}

export async function fetchAuditLog(token, { limit = 100 } = {}) {
  if (!token) return [];
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_audit_log?select=*&order=created_at.desc&limit=${limit}`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}` } }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch { return []; }
}
