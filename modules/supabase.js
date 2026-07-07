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

// Sum API usage over the last N days for a single provider (e.g. "anthropic").
// Returns { total: number, days: number } — convenient for "pitches this month".
export async function getApiUsageForProvider(accessToken, userEmail, provider, days = 30) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!accessToken || !userEmail || !provider) return { total: 0, days: 0 };
  try {
    // Local day boundary (Argentina tz) so "today" and "this month" match the user's clock
    const startLocal = new Date();
    startLocal.setDate(startLocal.getDate() - (days - 1));
    const startStr = startLocal.toLocaleDateString("en-CA");
    const res = await fetch(
      `${url}/rest/v1/toolbar_api_usage?user_email=eq.${encodeURIComponent(userEmail)}&day=gte.${startStr}&select=by_provider`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return { total: 0, days: 0 };
    const rows = await res.json();
    if (!Array.isArray(rows)) return { total: 0, days: 0 };
    let total = 0;
    for (const r of rows) total += Number(r.by_provider?.[provider] || 0);
    return { total, days: rows.length };
  } catch { return { total: 0, days: 0 }; }
}

// Today's API usage (total + by_provider) for the current user
export async function getApiUsageToday(accessToken, userEmail) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!accessToken || !userEmail) return { total: 0, byProvider: {} };
  try {
    const today = new Date().toLocaleDateString("en-CA");
    const res = await fetch(
      `${url}/rest/v1/toolbar_api_usage?user_email=eq.${encodeURIComponent(userEmail)}&day=eq.${today}&select=total,by_provider`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return { total: 0, byProvider: {} };
    const rows = await res.json();
    const r = Array.isArray(rows) && rows[0];
    return r ? { total: r.total || 0, byProvider: r.by_provider || {} } : { total: 0, byProvider: {} };
  } catch { return { total: 0, byProvider: {} }; }
}

// ── Auth token cache — set once after login; used as Bearer by every request ──
let _sbAuthToken = null;
export function setSupabaseAuth(accessToken) { _sbAuthToken = accessToken || null; }
function bearer(key) { return `Bearer ${_sbAuthToken || key}`; }

// ── Autenticación Supabase ────────────────────────────────────

// Maxi 2026-07-03: helper de fetch a Supabase Auth con REINTENTO + parseo seguro.
// Bug real detectado: cuando Supabase se sobrecarga, Cloudflare devuelve una página
// HTML (HTTP 522/503) y `res.json()` explotaba con "Unexpected token '<'" — mensaje
// críptico que hacía pensar en la contraseña o la extensión. Ahora:
//   1) Distingue caídas del server (5xx/522/HTML) de credenciales incorrectas (4xx JSON).
//   2) Reintenta los errores TRANSITORIOS (el 522 de Supabase es intermitente: en las
//      pruebas el auth volvía en el 2º intento). 3 intentos con backoff 0.6s/1.5s.
//   3) Mensaje CLARO: "servidor temporalmente caído, no es tu contraseña".
async function _supabaseAuthFetch(path, body) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const delays = [600, 1500, 0]; // 3 intentos
  let lastServerErr = "";
  for (let attempt = 0; attempt < delays.length; attempt++) {
    try {
      const res = await fetch(`${url}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "apikey": key },
        body: JSON.stringify(body),
      });
      const ct = res.headers.get("content-type") || "";
      const isJson = ct.includes("application/json");
      if (isJson) {
        const data = await res.json().catch(() => null);
        // 4xx con JSON = respuesta legítima del auth (credenciales, etc.) → NO reintentar.
        if (data) return { ok: res.ok, status: res.status, data };
      }
      // No-JSON (HTML de Cloudflare) o 5xx = caída del server → transitorio, reintentar.
      lastServerErr = `HTTP ${res.status}`;
    } catch (e) {
      lastServerErr = e.message || "network"; // timeout / red caída → reintentar
    }
    if (delays[attempt]) await new Promise(r => setTimeout(r, delays[attempt]));
  }
  return {
    ok: false, status: 0, data: null,
    serverDown: `El servidor está temporalmente caído (${lastServerErr}). No es tu contraseña ni la extensión — reintentá en 1-2 minutos.`,
  };
}

export async function supabaseSignIn(email, password) {
  const r = await _supabaseAuthFetch(`/auth/v1/token?grant_type=password`, { email, password });
  if (r.serverDown) return { error: r.serverDown };
  const data = r.data || {};
  if (!r.ok) return { error: data?.error_description || data?.msg || "Credenciales incorrectas" };
  // Devolvemos también el email REAL que Supabase autenticó. El caller debe
  // verificar que matche con lo tipeado — si no, hubo un mix-up de sesión
  // o de credenciales y NO debemos asumir la identidad tipeada.
  return {
    access_token:  data.access_token,
    refresh_token: data.refresh_token,
    expires_in:    data.expires_in,
    authenticated_email: (data?.user?.email || "").toLowerCase(),
  };
}

// Envía email de recovery. Supabase dispara el correo con el link de reset
// que redirige a nuestra página de reset hosteada en GitHub Pages.
const RESET_PASSWORD_REDIRECT = "https://mgargiulo-cell.github.io/adeq-toolbar/docs/reset-password.html";

export async function supabaseResetPassword(email) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    // redirect_to en el query string + body — Supabase espera ambos en algunos endpoints
    const res = await fetch(`${url}/auth/v1/recover?redirect_to=${encodeURIComponent(RESET_PASSWORD_REDIRECT)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "apikey": key },
      body: JSON.stringify({ email, redirect_to: RESET_PASSWORD_REDIRECT }),
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
  const r = await _supabaseAuthFetch(`/auth/v1/token?grant_type=refresh_token`, { refresh_token: refreshToken });
  if (r.serverDown) return { error: r.serverDown };
  const data = r.data || {};
  if (!r.ok) return { error: data?.error_description || "Sesión expirada" };
  return {
    access_token:  data.access_token,
    refresh_token: data.refresh_token,
    expires_in:    data.expires_in,
  };
}

// ── Pitch feedback RAG (Voyage embeddings + pgvector) ─────────
export async function insertPitchFeedback(accessToken, userEmail, payload) {
  // payload: { domain, category, geo, language, traffic, pitch_body, pitch_subject, context, embedding, action }
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!accessToken || !userEmail) return { ok: false, error: "auth required" };
  try {
    const res = await fetch(`${url}/rest/v1/toolbar_pitch_feedback`, {
      method: "POST",
      headers: {
        "apikey": key, "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json", "Prefer": "return=minimal",
      },
      body: JSON.stringify({ user_email: userEmail, ...payload }),
    });
    return { ok: res.ok, status: res.status };
  } catch (e) { return { ok: false, error: e.message }; }
}

// Returns top-N pitches matching the query embedding, scoped to user + action.
// Uses the SQL function match_pitch_feedback (see sql/pitch_feedback_rag.sql).
export async function matchPitchFeedback(accessToken, userEmail, embedding, action = "liked", count = 3) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!accessToken || !userEmail || !Array.isArray(embedding)) return [];
  try {
    const res = await fetch(`${url}/rest/v1/rpc/match_pitch_feedback`, {
      method: "POST",
      headers: {
        "apikey": key, "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query_embedding:  embedding,
        match_user_email: userEmail,
        match_action:     action,
        match_count:      count,
      }),
    });
    if (!res.ok) {
      console.warn("[matchPitchFeedback] HTTP", res.status, (await res.text()).slice(0, 200));
      return [];
    }
    const rows = await res.json();
    return Array.isArray(rows) ? rows : [];
  } catch (e) {
    console.warn("[matchPitchFeedback] exception", e.message);
    return [];
  }
}

// ── Per-user custom Claude prompt (own table, simple per-user RLS) ──
export async function getCustomPrompt(accessToken, userEmail) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!accessToken || !userEmail) return "";
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_user_prompts?user_email=eq.${encodeURIComponent(userEmail.toLowerCase())}&select=prompt`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return "";
    const rows = await res.json();
    return (Array.isArray(rows) && rows[0]?.prompt) || "";
  } catch { return ""; }
}

export async function setCustomPrompt(accessToken, userEmail, value) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!accessToken || !userEmail) return { ok: false, error: "auth required" };
  const headers = {
    "apikey": key, "Authorization": `Bearer ${accessToken}`,
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
  };
  try {
    const res = await fetch(`${url}/rest/v1/toolbar_user_prompts`, {
      method: "POST", headers,
      body: JSON.stringify([{
        user_email: userEmail.toLowerCase(),
        prompt:     value || "",
        updated_at: new Date().toISOString(),
      }]),
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      return { ok: false, status: res.status, error: txt.slice(0, 200) };
    }
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── Manual send tracking — inserta una fila en toolbar_agent_actions
// ANTES de mandar el email, así obtenemos el id y lo embedeamos en un
// pixel <img src="track-open?aid=ID"/> dentro del body. Cuando el
// destinatario abre, la Edge Function track-open inserta en
// toolbar_email_opens(agent_action_id=ID), y el worker de reengagement
// puede detectar "no abierto" comparando ambas tablas.
export async function createManualSendTracking(accessToken, payload) {
  // payload: { user_email, domain, email_to, pitch_subject, language, email_source? }
  // email_source: apollo|informer|scrape|generic|manual — usado por toolbar_source_performance.
  // Si el popup no lo pasa, queda "manual" (el MB tipeó el email a mano).
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!accessToken || !payload?.user_email) return { ok: false, error: "auth required" };
  try {
    const res = await fetch(`${url}/rest/v1/toolbar_agent_actions`, {
      method: "POST",
      headers: {
        "apikey": key, "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json", "Prefer": "return=representation",
      },
      body: JSON.stringify({
        user_email:    (payload.user_email || "").toLowerCase(),
        domain:        (payload.domain || "").toLowerCase(),
        action:        "sent",
        email_to:      payload.email_to || null,
        pitch_subject: payload.pitch_subject || null,
        details:       {
          source:    payload.email_source || "manual",
          ui_origin: "toolbar_manual",
          language:  payload.language || null,
        },
      }),
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      return { ok: false, status: res.status, error: txt.slice(0, 200) };
    }
    const rows = await res.json().catch(() => []);
    const id = Array.isArray(rows) ? rows[0]?.id : rows?.id;
    return { ok: true, id };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── Email Futuro / Reengagement queue ─────────────────────────
// Encola un email "futuro" para que el agente lo envíe a los 11d si el
// primer email no fue abierto. Update Monday columns: email + FU1 + FU2.
export async function queueReengagement(accessToken, payload) {
  // payload: { domain, monday_item_id, mb_email, original_email, future_email,
  //   delay_days?: 11, sequence?: "FU2" }
  // Maxi 2026-06-17: delay_days + sequence permiten encolar FU2/FU3/FU4 con
  // distinto offset. Worker procesa cada uno verificando si el ORIGINAL fue
  // abierto; si sí → cancela el resto.
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!accessToken || !payload?.future_email) return { ok: false, error: "missing data" };
  const days = parseInt(payload.delay_days || 11, 10);
  const scheduled = new Date(Date.now() + days * 86_400_000).toISOString();
  try {
    const res = await fetch(`${url}/rest/v1/toolbar_reengagement_queue`, {
      method: "POST",
      headers: {
        "apikey": key, "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json", "Prefer": "return=minimal",
      },
      body: JSON.stringify({
        domain:           (payload.domain || "").toLowerCase(),
        monday_item_id:   payload.monday_item_id || null,
        mb_email:         (payload.mb_email || "").toLowerCase(),
        original_email:   payload.original_email,
        future_email:     payload.future_email,
        original_subject: payload.original_subject || null,
        original_body:    payload.original_body || null,
        tracking_action_id: payload.tracking_action_id || null,
        scheduled_for:    scheduled,
        status:           "pending",
        sequence:         payload.sequence || "FU2",
      }),
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      return { ok: false, status: res.status, error: txt.slice(0, 200) };
    }
    return { ok: true, scheduled_for: scheduled };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── Bounce check ──────────────────────────────────────────────
// Chequea si un email está en toolbar_bounced_emails (lista global de
// emails que ya rebotaron en cualquier MB). Si está, no se debe permitir
// usarlo como destinatario — ni manual ni como future_email.
export async function isEmailBounced(accessToken, email) {
  if (!accessToken || !email) return { bounced: false };
  const clean = String(email).trim().toLowerCase();
  if (!clean.includes("@")) return { bounced: false };
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_bounced_emails?email=eq.${encodeURIComponent(clean)}&select=email,reason,created_at&limit=1`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return { bounced: false };
    const rows = await res.json().catch(() => []);
    if (Array.isArray(rows) && rows.length > 0) {
      return { bounced: true, reason: rows[0].reason || "bounced", since: rows[0].created_at };
    }
    return { bounced: false };
  } catch { return { bounced: false }; }
}

// ── Notifications ─────────────────────────────────────────────
// Lee notificaciones del MB (unread + recent read) para renderizar el bell.
// Worker side: las crea via createNotification() en auto-prospector/index.js.
// Popup side: aca solo READ + UPDATE (mark read / dismiss).
export async function fetchNotifications(accessToken, mbEmail, { limit = 30 } = {}) {
  if (!accessToken || !mbEmail) return [];
  try {
    const mb = mbEmail.toLowerCase();
    // user 2026-05-29: notificaciones PERSONALES por MB (no más _admin compartido).
    // Mostramos solo no descartadas — al hacer "Mark all" se descartan y desaparecen.
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_notifications?mb_email=eq.${encodeURIComponent(mb)}&dismissed_at=is.null&select=*&order=created_at.desc&limit=${limit}`,
      { headers: { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return [];
    return await res.json().catch(() => []);
  } catch { return []; }
}

export async function markNotificationRead(accessToken, id) {
  if (!accessToken || !id) return false;
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_notifications?id=eq.${id}`,
      {
        method: "PATCH",
        headers: {
          "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${accessToken}`,
          "Content-Type": "application/json", "Prefer": "return=minimal",
        },
        body: JSON.stringify({ read_at: new Date().toISOString() }),
      }
    );
    return res.ok;
  } catch { return false; }
}

export async function markAllNotificationsRead(accessToken, mbEmail) {
  if (!accessToken || !mbEmail) return false;
  try {
    // user 2026-05-29: "Mark all" ahora también descarta (dismissed_at) → desaparecen
    // del listado. Antes solo seteaba read_at pero el fetch seguía mostrándolas.
    const now = new Date().toISOString();
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_notifications?mb_email=eq.${encodeURIComponent(mbEmail.toLowerCase())}&dismissed_at=is.null`,
      {
        method: "PATCH",
        headers: {
          "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${accessToken}`,
          "Content-Type": "application/json", "Prefer": "return=minimal",
        },
        body: JSON.stringify({ read_at: now, dismissed_at: now }),
      }
    );
    return res.ok;
  } catch { return false; }
}

// Crea una notificación client-side (ej: load_error en el popup).
// Worker side usa la misma tabla via REST directo.
export async function createNotification(accessToken, payload) {
  // payload: { mb_email, type, severity, title, body?, metadata?, dedup_key? }
  if (!accessToken || !payload?.mb_email || !payload?.type || !payload?.title) return false;
  try {
    const res = await fetch(
      `${CONFIG.SUPABASE_URL}/rest/v1/toolbar_notifications`,
      {
        method: "POST",
        headers: {
          "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${accessToken}`,
          "Content-Type": "application/json",
          "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        body: JSON.stringify({
          mb_email:  payload.mb_email.toLowerCase(),
          type:      payload.type,
          severity:  payload.severity || "info",
          title:     payload.title,
          body:      payload.body || null,
          metadata:  payload.metadata || {},
          dedup_key: payload.dedup_key || null,
        }),
      }
    );
    return res.ok;
  } catch { return false; }
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
      `${url}/rest/v1/toolbar_config?key=in.(auto_prospecting_enabled,auto_session_start,auto_session_user,auto_heartbeat_at)&select=key,value`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });
    const enabled      = map.auto_prospecting_enabled === "true";
    const sessionStart = map.auto_session_start ? new Date(map.auto_session_start) : null;
    const heartbeatAt  = map.auto_heartbeat_at  ? new Date(map.auto_heartbeat_at)  : null;
    const sessionUser  = map.auto_session_user  || "";
    return { enabled, sessionStart, heartbeatAt, sessionUser };
  } catch { return { enabled: false, sessionStart: null, heartbeatAt: null, sessionUser: "" }; }
}

async function upsertConfig(url, key, headers, configKey, value) {
  // Maxi 2026-07-01 (M4): guardas — antes check.json() sin res.ok podía tirar sobre una
  // página de error y el toggle reportaba éxito sin persistir. Ahora lanza si falla.
  const check = await fetch(`${url}/rest/v1/toolbar_config?key=eq.${configKey}&select=key`, { headers });
  if (!check.ok) throw new Error(`upsertConfig check ${configKey}: HTTP ${check.status}`);
  let rows = [];
  try { rows = await check.json(); } catch {}
  const exists = Array.isArray(rows) && rows.length > 0;
  const res = await fetch(`${url}/rest/v1/toolbar_config${exists ? `?key=eq.${configKey}` : ""}`, {
    method: exists ? "PATCH" : "POST",
    headers,
    body: JSON.stringify(exists ? { value } : { key: configKey, value }),
  });
  if (!res.ok) throw new Error(`upsertConfig write ${configKey}: HTTP ${res.status}`);
}

export async function setAutopilotEnabled(enabled, accessToken, userEmail = "") {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const headers = {
    "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json",
  };
  try {
    // Mitigación de race: si 2 MBs toggle ON al mismo tiempo, el último PATCH
    // gana el session_user. Hacemos read-after-write: si tras 400ms el dueño no
    // soy yo, devolvemos false para que la UI haga rollback.
    await fetch(`${url}/rest/v1/toolbar_config?key=eq.auto_prospecting_enabled`, {
      method: "PATCH", headers,
      body: JSON.stringify({ value: enabled ? "true" : "false" }),
    });
    if (enabled) {
      const sessionStart = new Date().toISOString();
      await upsertConfig(url, key, headers, "auto_session_start", sessionStart);
      if (userEmail) await upsertConfig(url, key, headers, "auto_session_user", userEmail);
      // Verificación post-write — race detection
      await new Promise(r => setTimeout(r, 400));
      const verifyRes = await fetch(`${url}/rest/v1/toolbar_config?key=eq.auto_session_user&select=value`, { headers });
      const verifyRows = await verifyRes.json().catch(() => []);
      const winner = (verifyRows?.[0]?.value || "").toLowerCase();
      if (userEmail && winner && winner !== userEmail.toLowerCase()) {
        // Race perdida — otro MB clavó la sesión. NO desactivamos (lo hizo él).
        return { ok: false, winner };
      }
    }
    return { ok: true };
  } catch { return { ok: true }; /* swallow para no romper UI */ }
}

// ── Autopilot feedback (learning from user like/dislike) ────
export async function saveAutopilotFeedback(accessToken, { user_email, domain, action, category, geo, ad_networks, traffic, reason }) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  // Bucket de tráfico para detectar tipos rejected. Maxi 2026-06-17:
  // el agente aprende qué COMBO (categoria + tráfico bucket + geo) no sirve.
  const trafficBucket = trafficBucketLabel(traffic);
  // Maxi 2026-06-19: guardar el MOTIVO del rechazo (antes se descartaba). El worker
  // lo sintetiza en reglas de basura por CONTENIDO para descartar futuros similares.
  const row = { user_email, domain, action, category: category || "", geo: geo || "", ad_networks: ad_networks || [], traffic_bucket: trafficBucket };
  if (reason) row.reason = String(reason).substring(0, 500);
  try {
    const res = await fetch(`${url}/rest/v1/toolbar_autopilot_feedback`, {
      method: "POST",
      headers: {
        "apikey": key, "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json", "Prefer": "return=minimal",
      },
      body: JSON.stringify([row]),
    });
    return { ok: res.ok };
  } catch (e) { return { ok: false, error: e.message }; }
}

// Etiqueta bucket de tráfico — usada por reject-learning para agrupar dislikes
// por orden de magnitud. Si pegan 3+ dislikes en el mismo bucket+categoria+geo,
// el worker skipea futuros leads de ese tipo.
export function trafficBucketLabel(traffic) {
  const t = parseInt(traffic || 0, 10);
  if (!t) return "unknown";
  if (t < 100_000)    return "<100K";
  if (t < 500_000)    return "100K-500K";
  if (t < 1_000_000)  return "500K-1M";
  if (t < 5_000_000)  return "1M-5M";
  if (t < 15_000_000) return "5M-15M";
  return "15M+";
}

// Fetch signatures (categoria + traffic_bucket + geo) que el MB rechazó 3+ veces.
// Worker usa esto para skipear leads que matchean. Sin RPC para simplicidad —
// agregamos client-side.
export async function fetchRejectedSignatures(accessToken, userEmail, threshold = 3) {
  if (!accessToken) return [];
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    // Solo últimos 90 días — historial reciente, no perpetuo
    const since = new Date(Date.now() - 90 * 24 * 3600 * 1000).toISOString();
    const u = userEmail ? `&user_email=eq.${encodeURIComponent(userEmail)}` : "";
    const res = await fetch(
      `${url}/rest/v1/toolbar_autopilot_feedback?action=eq.disliked&created_at=gte.${since}${u}&select=category,geo,traffic_bucket&limit=2000`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return [];
    const rows = await res.json();
    // Cuenta por signatura
    const counts = new Map();
    for (const r of (Array.isArray(rows) ? rows : [])) {
      const sig = `${(r.category||"").toLowerCase().trim()}|${(r.traffic_bucket||"unknown")}|${(r.geo||"").toLowerCase().trim()}`;
      counts.set(sig, (counts.get(sig) || 0) + 1);
    }
    // Devolver signatures que pasaron el threshold
    const blocked = [];
    for (const [sig, n] of counts.entries()) {
      if (n >= threshold) {
        const [category, traffic_bucket, geo] = sig.split("|");
        blocked.push({ category, traffic_bucket, geo, count: n });
      }
    }
    return blocked;
  } catch { return []; }
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
export async function fetchReviewQueue(accessToken, { dateFilter = "", sourceFilter = "", userFilter = "", geoFilter = "" } = {}) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const cols = "id,domain,traffic,geo,geos_all,language,category,contact_name,emails,email_sources,pitch_subject,pitch_subjects,score,ad_networks,page_title,status,validated_by,validated_at,created_at,source,monday_item_id,created_by,suspect_reject,suspect_reason";
  // Date filter (Maxi 2026-06-18 v2): "today" | "yesterday" |
  //   "this_week" | "last_week" | "this_month" | "last_month" |
  //   (legacy) "last7" | "last30"
  let dateClause = "";
  if (dateFilter) {
    const tzDay = (d) => d.toLocaleDateString("en-CA", { timeZone: "America/Argentina/Buenos_Aires" });
    const now = new Date();
    // Lunes como inicio de semana (ISO)
    const startOfWeek = (d) => {
      const day = d.getDay(); // 0=dom..6=sáb
      const diff = day === 0 ? 6 : day - 1;
      return new Date(d.getTime() - diff * 86400000);
    };
    if (dateFilter === "today") {
      dateClause = `&created_at=gte.${tzDay(now)}T00:00:00`;
    } else if (dateFilter === "yesterday") {
      const y = new Date(now.getTime() - 86400000);
      dateClause = `&created_at=gte.${tzDay(y)}T00:00:00&created_at=lt.${tzDay(now)}T00:00:00`;
    } else if (dateFilter === "this_week") {
      const sow = startOfWeek(now);
      dateClause = `&created_at=gte.${tzDay(sow)}T00:00:00`;
    } else if (dateFilter === "last_week") {
      const sow = startOfWeek(now);
      const sowLast = new Date(sow.getTime() - 7 * 86400000);
      dateClause = `&created_at=gte.${tzDay(sowLast)}T00:00:00&created_at=lt.${tzDay(sow)}T00:00:00`;
    } else if (dateFilter === "this_month") {
      const som = new Date(now.getFullYear(), now.getMonth(), 1);
      dateClause = `&created_at=gte.${tzDay(som)}T00:00:00`;
    } else if (dateFilter === "last_month") {
      const som = new Date(now.getFullYear(), now.getMonth(), 1);
      const somLast = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      dateClause = `&created_at=gte.${tzDay(somLast)}T00:00:00&created_at=lt.${tzDay(som)}T00:00:00`;
    } else if (dateFilter === "last7") {
      const d7 = new Date(now.getTime() - 7 * 86400000);
      dateClause = `&created_at=gte.${tzDay(d7)}T00:00:00`;
    } else if (dateFilter === "last30") {
      const d30 = new Date(now.getTime() - 30 * 86400000);
      dateClause = `&created_at=gte.${tzDay(d30)}T00:00:00`;
    }
  }
  const sourceClause = sourceFilter ? `&source=eq.${encodeURIComponent(sourceFilter)}` : "";
  // Maxi 2026-06-30: filtro por owner. "_AGENT_" = lo que cargó el agente autónomo
  // (created_by vacío/null, sin owner manual). Un email real = carga manual de ese MB
  // (los imports/CSV setean created_by con el email; el worker autónomo lo deja vacío).
  let userClause = "";
  if (userFilter === "_AGENT_")   userClause = `&or=(created_by.is.null,created_by.eq.)`;
  else if (userFilter)            userClause = `&created_by=eq.${encodeURIComponent(userFilter)}`;
  // Geo filter: matchea contra `geo` (NAME en inglés, legacy) o `geos_all`
  // (ISO array). Acepta ISO 2-letter (AR, BR, ES, MX, etc.). El worker guarda
  // `geo` como NAME (ej "Vietnam") NO como ISO — para legacy hay que mapear.
  let geoClause = "";
  if (geoFilter) {
    const g = geoFilter.trim().toUpperCase();
    const ISO_TO_NAME = {
      AR:"Argentina", BR:"Brazil", ES:"Spain", MX:"Mexico", CO:"Colombia",
      CL:"Chile", PE:"Peru", UY:"Uruguay", EC:"Ecuador", VE:"Venezuela",
      DO:"Dominican Republic", PA:"Panama", BO:"Bolivia", GT:"Guatemala",
      CR:"Costa Rica", HN:"Honduras", SV:"El Salvador", NI:"Nicaragua",
      PY:"Paraguay", PR:"Puerto Rico", CU:"Cuba", US:"United States",
      GB:"United Kingdom", CA:"Canada", AU:"Australia", NZ:"New Zealand",
      PT:"Portugal", IT:"Italy", FR:"France", DE:"Germany", NL:"Netherlands",
      BE:"Belgium", CH:"Switzerland", AT:"Austria", IE:"Ireland", DK:"Denmark",
      SE:"Sweden", NO:"Norway", FI:"Finland", PL:"Poland", CZ:"Czech Republic",
      HU:"Hungary", RO:"Romania", GR:"Greece", IN:"India", PK:"Pakistan",
      BD:"Bangladesh", LK:"Sri Lanka", ID:"Indonesia", PH:"Philippines",
      VN:"Vietnam", TH:"Thailand", MY:"Malaysia", SG:"Singapore",
      SA:"Saudi Arabia", AE:"UAE", EG:"Egypt", TR:"Turkey", IL:"Israel",
      NG:"Nigeria", KE:"Kenya", ZA:"South Africa", MA:"Morocco", DZ:"Algeria",
      JP:"Japan", KR:"South Korea", CN:"China", TW:"Taiwan", HK:"Hong Kong",
      RU:"Russia", UA:"Ukraine", BG:"Bulgaria", HR:"Croatia", SK:"Slovakia",
    };
    const name = ISO_TO_NAME[g] || g;
    const enc = encodeURIComponent(name);
    // Match: ISO en geos_all (array contains) OR geo (legacy) == name OR
    // geo (legacy) prefix-match con ISO/name (cubre ambos formatos).
    geoClause = `&or=(geos_all.cs.{${g}},geo.eq.${enc},geo.ilike.${enc}*,geo.eq.${encodeURIComponent(g)},geo.ilike.${encodeURIComponent(g)}*)`;
  }
  try {
    // Política user 2026-05-18: NO ordenar por score — el score no se usa para
    // seleccionar URLs (solo rankEmail decide para emails). Cualquier lead con
    // traffic ≥ 400K + status=pending es válido. Orden FIFO por created_at desc
    // → primero los más frescos (mejor para que vean los recién agregados).
    const res = await fetch(
      `${url}/rest/v1/toolbar_review_queue?status=eq.pending${dateClause}${sourceClause}${userClause}${geoClause}&order=created_at.desc&limit=3000&select=${cols}`,
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
    const res = await fetch(`${url}/rest/v1/toolbar_review_queue?id=eq.${id}`, {
      method: "PATCH",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ status: "validated", validated_by: validatedBy, validated_at: new Date().toISOString() }),
    });
    if (!res.ok) return { ok: false, error: `HTTP ${res.status}` };
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// Apollo monthly cap (plan = 2,500; cap conservador 2,400 con margen 100).
// Si llegado, callers (Apollo en popup) deben skipear y caer a scraping.
export const APOLLO_MONTHLY_HARD_CAP = 2400;

// Caps de la cola de procesamiento (csv_queue):
// - pending: max 200 items esperando ser procesados por el worker (throttle del worker)
// - waiting_pool: max 800 items en hold (Maxi 2026-06-19: 300→800, total 1000 con pending)
// - review_queue (Prospects) NO tiene cap — puede crecer indefinido (es mejor tener variedad)
// El excedente va a next_day (sin cap) y rolea a waiting_pool a medianoche Madrid → nada se pierde.
export const CSV_QUEUE_HARD_CAP = 200;
export const CSV_WAITING_POOL_CAP = 800;
export const CSV_UPLOAD_MAX = 1000;
export const WAITLIST_HARD_CAP  = 800;

export async function getApolloMonthlyUsage(accessToken) {
  if (!accessToken) return { used: 0, limit: APOLLO_MONTHLY_HARD_CAP, remaining: APOLLO_MONTHLY_HARD_CAP };
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  // Billing cycle 6→6 (igual que RapidAPI). Calcula período actual.
  const now = new Date();
  let year = now.getFullYear(), month = now.getMonth() + 1;
  if (now.getDate() < 6) { month -= 1; if (month === 0) { month = 12; year -= 1; } }
  const period = `${year}-${String(month).padStart(2, "0")}-06`;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_config?key=in.(apollo_calls_month,apollo_calls_month_period,apollo_monthly_limit)&select=key,value`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    const rows = await res.json();
    const map = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });
    const storedPeriod = map.apollo_calls_month_period || "";
    const storedCount  = parseInt(map.apollo_calls_month || "0", 10);
    const limit        = parseInt(map.apollo_monthly_limit || String(APOLLO_MONTHLY_HARD_CAP), 10);
    const used = storedPeriod === period ? storedCount : 0;
    return { used, limit, remaining: Math.max(0, limit - used), period };
  } catch { return { used: 0, limit: APOLLO_MONTHLY_HARD_CAP, remaining: APOLLO_MONTHLY_HARD_CAP }; }
}

// Cuenta de items pending en review_queue (la "cola de Prospects").
// Cap absoluto del pool: 200. Si se alcanza, no se aceptan más imports
// hasta que el equipo procese. Defensa contra cola gigante.
export const REVIEW_QUEUE_HARD_CAP = 200;

export async function getReviewQueuePendingCount(accessToken) {
  if (!accessToken) return 0;
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_review_queue?status=eq.pending&select=id`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("content-range") || res.headers.get("Content-Range") || "";
    const m = range.match(/\/(\d+)$/);
    return m ? parseInt(m[1]) : 0;
  } catch { return 0; }
}

// Marca COMO VALIDATED todos los items pending de este dominio en review_queue.
// Se llama cuando un MB manda mail manual desde Analysis (o el agent procesa).
// Asegura que el lead desaparezca inmediato de la cola de Prospects, sin importar
// si el push a Monday ocurrió o no — el contacto YA se hizo.
export async function markReviewQueueAsContacted(accessToken, domain, validatedBy) {
  if (!domain || !accessToken) return { ok: false };
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const cleanDomain = (domain || "").toLowerCase().replace(/^www\./, "").trim();
  try {
    await fetch(`${url}/rest/v1/toolbar_review_queue?domain=eq.${encodeURIComponent(cleanDomain)}&status=eq.pending`, {
      method: "PATCH",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json", "Prefer": "return=minimal" },
      body: JSON.stringify({ status: "validated", validated_by: validatedBy, validated_at: new Date().toISOString() }),
    });
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
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
    // Local day (browser timezone) — matches what the user thinks of as "today"
    const today = new Date().toLocaleDateString("en-CA");
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
        "Authorization": bearer(key),
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
        { headers: { "apikey": key, "Authorization": bearer(key) } }
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
      { headers: { "apikey": key, "Authorization": bearer(key) } }
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
      { headers: { "apikey": key, "Authorization": bearer(key) } }
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
          "Authorization": bearer(key),
          "Prefer":        "resolution=ignore-duplicates,return=minimal",
        },
        body: JSON.stringify(batch),
      });
      inserted += batch.length;
    } catch {}
  }
  return { count: inserted };
}

export async function clearKeywordsDB(accessToken) {
  const { url, key } = await getConfig();
  if (!url || !key || !accessToken) return;
  try {
    await fetch(`${url}/rest/v1/toolbar_keywords?id=gte.0`, {
      method: "DELETE",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` },
    });
  } catch {}
}

export async function countKeywordsDB() {
  const { url, key } = await getConfig();
  if (!url || !key) return 0;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_keywords?select=id`,
      { headers: { "apikey": key, "Authorization": bearer(key), "Prefer": "count=exact", "Range": "0-0" } }
    );
    const range = res.headers.get("Content-Range") || "";
    return parseInt(range.split("/")[1]) || 0;
  } catch { return 0; }
}

export async function clearHistory(accessToken, userEmail) {
  const { url, key } = await getConfig();
  if (!url || !key || !accessToken || !userEmail) return;
  try {
    // Only delete the caller's own rows — never wipe other users' history
    const q = `media_buyer=eq.${encodeURIComponent(userEmail)}`;
    await fetch(`${url}/rest/v1/toolbar_historial?${q}`, {
      method: "DELETE",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` },
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
// Tabla: toolbar_similar_sites_cache (90 días, regla de oro)
//   domain TEXT PRIMARY KEY, sites JSONB, fetched_at TIMESTAMPTZ DEFAULT NOW()
// ============================================================
const SIMILAR_CACHE_DAYS_OLD = 90;
export async function getSimilarCache(domain) {
  const { url, key } = await getConfig();
  if (!url || !key || !domain) return null;
  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - SIMILAR_CACHE_DAYS_OLD);
    const res = await fetch(
      `${url}/rest/v1/toolbar_similar_sites_cache?domain=eq.${encodeURIComponent(domain)}&fetched_at=gte.${cutoff.toISOString()}&limit=1`,
      { headers: { "apikey": key, "Authorization": bearer(key) } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows.length ? rows[0].sites : null;
  } catch { return null; }
}

export async function saveSimilarCache(domain, sites) {
  const { url, key } = await getConfig();
  if (!url || !key || !domain) return;
  try {
    await fetch(`${url}/rest/v1/toolbar_similar_sites_cache`, {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "apikey":        key,
        "Authorization": bearer(key),
        "Prefer":        "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({ domain, sites: Array.isArray(sites) ? sites : [], fetched_at: new Date().toISOString() }),
    });
  } catch (err) { console.warn("saveSimilarCache failed:", err.message); }
}

// ============================================================
// CACHÉ DE TRÁFICO — 90 días
// Tabla: toolbar_traffic_cache
//   domain TEXT PRIMARY KEY, data JSONB, fetched_at TIMESTAMPTZ DEFAULT NOW()
// ============================================================
const TRAFFIC_CACHE_DAYS = 90;
export async function getTrafficCache(domain) {
  const { url, key } = await getConfig();
  if (!url || !key) return null;

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - TRAFFIC_CACHE_DAYS);

    const res = await fetch(
      `${url}/rest/v1/toolbar_traffic_cache?domain=eq.${encodeURIComponent(domain)}&fetched_at=gte.${cutoff.toISOString()}&limit=1`,
      { headers: { "apikey": key, "Authorization": bearer(key) } }
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
        "Authorization": bearer(key),
        "Prefer":        "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({ domain, data: clean, fetched_at: new Date().toISOString() }),
    });
  } catch (err) {
    console.warn("Cache save failed:", err.message);
  }
}

// ============================================================
// SEGUIMIENTO DE ENVÍOS
// Tabla: toolbar_sendtrack — historial de pitches enviados por dominio.
// (Los follow-ups los maneja el CRM externo, no la toolbar.)
// ============================================================
export async function saveSendDate(domain, { sendDate, pitch, email }) {
  const { sendtrack = {} } = await chrome.storage.local.get("sendtrack");
  sendtrack[domain] = { sendDate, pitch, email };
  await chrome.storage.local.set({ sendtrack });

  const { url, key } = await getConfig();
  if (url && key) {
    try {
      await fetch(`${url}/rest/v1/toolbar_sendtrack`, {
        method: "POST",
        headers: {
          "Content-Type":  "application/json",
          "apikey":        key,
          "Authorization": bearer(key),
          "Prefer":        "resolution=merge-duplicates,return=minimal",
        },
        body: JSON.stringify({
          domain,
          send_date: sendDate,
          pitch:     pitch || "",
          email:     email || "",
        }),
      });
    } catch (err) {
      console.warn("SendTrack save failed:", err.message);
    }
  }
}

// ── CSV Queue — batch de dominios a procesar por el auto-prospector ───
// source: "csv" (External URLs) o "monday" (Monday URL Auto Prospector)
// ── Apollo cache (TTL 7 días) ───────────────────────────────
// Evita pagar Apollo 2× para el mismo dominio (worker + popup comparten).
const APOLLO_CACHE_TTL_DAYS = 7;

export async function getApolloCache(domain, accessToken) {
  if (!domain || !accessToken) return null;
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const d   = (domain || "").toLowerCase().replace(/^www\./, "").trim();
  if (!d) return null;
  try {
    const cutoff = new Date(Date.now() - APOLLO_CACHE_TTL_DAYS * 86_400_000).toISOString();
    const res = await fetch(
      `${url}/rest/v1/toolbar_apollo_cache?domain=eq.${encodeURIComponent(d)}&fetched_at=gte.${cutoff}&select=data&limit=1`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows?.[0]?.data || null;
  } catch { return null; }
}

export async function saveApolloCache(domain, data, accessToken) {
  if (!domain || !accessToken || !data) return;
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const d   = (domain || "").toLowerCase().replace(/^www\./, "").trim();
  if (!d) return;
  try {
    await fetch(`${url}/rest/v1/toolbar_apollo_cache`, {
      method: "POST",
      headers: {
        "apikey": key, "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({ domain: d, data, fetched_at: new Date().toISOString() }),
    });
  } catch {}
}

// Log de intento de import. Se llama desde popup tras cada acción de upload
// (CSV / sellers.json / Monday refresh). Persiste incluso si attempted = 75 y
// inserted = 0 (todos dedupados) — así se ve "Diego trabajó hoy" en el listado.
// Fire-and-forget: nunca rompe el flow del caller.
export async function logImportAttempt(accessToken, { userEmail, source, sourceDetail = "", attempted = 0, deduped = 0, inserted = 0 }) {
  if (!accessToken || !userEmail) return;
  try {
    await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_import_attempts`, {
      method: "POST",
      headers: {
        "apikey": CONFIG.SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
      },
      body: JSON.stringify({
        user_email:      userEmail.toLowerCase(),
        source,
        source_detail:   sourceDetail || null,
        attempted_count: attempted,
        deduped_count:   deduped,
        inserted_count:  inserted,
      }),
    });
  } catch {}
}

export async function uploadCsvDomains(domains, userEmail, accessToken, source = "csv") {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!Array.isArray(domains) || domains.length === 0) return { inserted: 0 };

  // Pre-check capacidades:
  // - pending (cap 200): cola activa, worker procesa de acá
  // - waiting_pool (cap 300): cola intermedia, promote a pending cuando libera
  // - next_day: excedente del budget diario (1000/día total), rollover medianoche Madrid
  let pendingNow = 0, waitingNow = 0, dailyCount = 0, dailyCap = 1000;
  try {
    const [rPending, rWaiting, rCfg] = await Promise.all([
      fetch(`${url}/rest/v1/toolbar_csv_queue?status=eq.pending&select=id`,
        { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${url}/rest/v1/toolbar_csv_queue?status=eq.waiting_pool&select=id`,
        { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Prefer": "count=exact", "Range": "0-0" } }),
      fetch(`${url}/rest/v1/toolbar_config?key=in.(csv_daily_count,csv_daily_count_date,csv_queue_daily_cap)&select=key,value`,
        { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }),
    ]);
    const parseCount = (r) => {
      const range = r.headers.get("content-range") || r.headers.get("Content-Range") || "";
      const m = range.match(/\/(\d+)$/);
      return m ? parseInt(m[1]) : 0;
    };
    pendingNow = parseCount(rPending);
    waitingNow = parseCount(rWaiting);
    const cfgRows = await rCfg.json().catch(() => []);
    const cfgMap = {}; cfgRows.forEach(r => { cfgMap[r.key] = r.value; });
    const todaySpain = new Date(new Date().toLocaleString("en-US", { timeZone: "Europe/Madrid" })).toISOString().split("T")[0];
    if (cfgMap.csv_daily_count_date === todaySpain) {
      dailyCount = parseInt(cfgMap.csv_daily_count || "0", 10);
    }
    dailyCap = parseInt(cfgMap.csv_queue_daily_cap || "1000", 10);
  } catch {}

  const pendingSlots  = Math.max(0, CSV_QUEUE_HARD_CAP - pendingNow);
  const waitingSlots  = Math.max(0, CSV_WAITING_POOL_CAP - waitingNow);
  const todayBudget   = Math.max(0, dailyCap - dailyCount); // cuántos más caben en el budget de hoy

  // Distribución: pending (limitado por pendingSlots Y todayBudget) → waiting_pool (limitado por waitingSlots Y todayBudget) → next_day (sin cap)
  const BATCH = 500;
  let inserted = 0, intoPending = 0, intoWaiting = 0, intoNextDay = 0;
  let consumedPending = 0, consumedWaiting = 0;
  let consumedTodayBudget = 0; // cuántos slots consumimos del budget diario (pending + waiting cuentan)
  for (let i = 0; i < domains.length; i += BATCH) {
    const slice = domains.slice(i, i + BATCH).map(d => {
      let status;
      if (consumedPending < pendingSlots && consumedTodayBudget < todayBudget) {
        status = "pending"; consumedPending++; consumedTodayBudget++; intoPending++;
      } else if (consumedWaiting < waitingSlots && consumedTodayBudget < todayBudget) {
        status = "waiting_pool"; consumedWaiting++; consumedTodayBudget++; intoWaiting++;
      } else {
        status = "next_day"; intoNextDay++;
      }
      return { domain: d, status, uploaded_by: userEmail, source };
    });
    try {
      // Maxi 2026-06-22 FIX: antes "ignore-duplicates" tiraba en silencio cualquier
      // dominio que ya tuviera una fila vieja (done/skipped/frozen/next_day) → "ya
      // estaban, 0 nuevos" hasta en el Monday refresh (que existe para re-prospectar).
      // Ahora "merge-duplicates": re-activa la fila existente al nuevo status (pending/
      // waiting) → el dominio se re-encola y el worker lo re-procesa (chequea Monday +
      // tráfico igual, no re-spamea). Sincroniza el insert con el dedup canónico.
      const res = await fetch(`${url}/rest/v1/toolbar_csv_queue`, {
        method: "POST",
        headers: {
          "apikey": key, "Authorization": `Bearer ${accessToken}`,
          "Content-Type": "application/json",
          "Prefer": "resolution=merge-duplicates,return=representation",
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
  return { inserted, attempted: domains.length, intoPending, intoWaiting, intoNextDay };
}

// Re-export for callers
export const CSV_QUEUE_HARD_CAP_EXPORT = CSV_QUEUE_HARD_CAP;

// Clear prospects: deletes rows where (created_by=user) OR rows with NULL/empty created_by (legacy).
// Returns { ok, deleted, status }.
export async function clearPendingProspects(accessToken, userEmail = null) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  if (!userEmail) return { ok: false, error: "userEmail required" };
  const headers = {
    "apikey": key,
    "Authorization": `Bearer ${accessToken}`,
    "Prefer": "return=representation",
  };
  let deleted = 0;
  try {
    // 1) Rows owned by the user
    const resOwn = await fetch(
      `${url}/rest/v1/toolbar_review_queue?created_by=eq.${encodeURIComponent(userEmail)}&select=id`,
      { method: "DELETE", headers }
    );
    if (resOwn.ok) {
      const rows = await resOwn.json().catch(() => []);
      deleted += Array.isArray(rows) ? rows.length : 0;
    }
    // 2) Legacy rows without created_by (pre-RLS). RLS may block this depending on policy; harmless if it does.
    const resLegacy = await fetch(
      `${url}/rest/v1/toolbar_review_queue?or=(created_by.is.null,created_by.eq.)&select=id`,
      { method: "DELETE", headers }
    );
    if (resLegacy.ok) {
      const rows = await resLegacy.json().catch(() => []);
      deleted += Array.isArray(rows) ? rows.length : 0;
    }
    return { ok: true, deleted };
  } catch (e) { return { ok: false, error: e.message }; }
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
      `${url}/rest/v1/toolbar_pitch_drafts?${userFilter}${langFilter}&order=priority.asc,is_default.desc,updated_at.desc&select=id,user_email,name,language,subject,body,is_default,priority,updated_at`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return [];
    return await res.json();
  } catch (e) { console.warn("getPitchDrafts:", e.message); return []; }
}

export async function savePitchDraft(accessToken, { id, user_email, name, language, subject, body, priority }) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  const prio = Math.max(1, Math.min(5, parseInt(priority) || 3));
  const payload = { user_email, name, language, subject: subject || "", body, priority: prio, updated_at: new Date().toISOString() };
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
  const stats = { total: 0, pending: 0, processing: 0, done: 0, error: 0, skipped: 0, waiting_pool: 0 };
  try {
    // Usamos HEAD + Prefer: count=exact para obtener totales por status
    // Maxi 2026-07-03 perf: los 3 callers (refreshStats heartbeat 10s, pre-check CSV, pre-check
    // sellers.json) SOLO leen pending/processing/waiting_pool. Antes hacíamos 6 count-scans
    // (incluyendo done/error/skipped que nadie usa) → mitad de scans a toolbar_csv_queue por llamada.
    const statuses = ["pending", "processing", "waiting_pool"];
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

export async function getCsvQueueState(accessToken) {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_config?key=in.(csv_queue_enabled,csv_queue_session_user,csv_queue_session_start)&select=key,value`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    const rows = await res.json();
    const map  = {};
    if (Array.isArray(rows)) rows.forEach(r => { map[r.key] = r.value; });
    return {
      enabled:      map.csv_queue_enabled === "true",
      sessionUser:  map.csv_queue_session_user || "",
      sessionStart: map.csv_queue_session_start ? new Date(map.csv_queue_session_start) : null,
    };
  } catch { return { enabled: false, sessionUser: "", sessionStart: null }; }
}

// Backwards-compat: muchas partes del código solo necesitan el bool.
export async function getCsvQueueEnabled(accessToken) {
  const st = await getCsvQueueState(accessToken);
  return st.enabled;
}

export async function setCsvQueueEnabled(enabled, accessToken, userEmail = "") {
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  // Antes esta función swallow-eaba todos los errores con console.warn. Si la
  // PATCH fallaba (red, 4xx, RLS, etc.) el checkbox local quedaba ON pero la DB
  // seguía en OFF → al siguiente poll (30s) el toggle se destildaba solo, sin
  // explicación al user. Ahora devolvemos boolean para que el caller pueda
  // revertir + mostrar error inmediatamente.
  try {
    const res = await fetch(`${url}/rest/v1/toolbar_config?key=eq.csv_queue_enabled`, {
      method: "PATCH",
      headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ value: enabled ? "true" : "false" }),
    });
    if (!res.ok) {
      console.warn(`setCsvQueueEnabled PATCH failed: HTTP ${res.status}`);
      return false;
    }
    if (enabled && userEmail) {
      const upsert = (k, v) => fetch(`${url}/rest/v1/toolbar_config`, {
        method: "POST",
        headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal" },
        body: JSON.stringify({ key: k, value: v }),
      });
      await upsert("csv_queue_session_user", userEmail);
      await upsert("csv_queue_session_start", new Date().toISOString());
      // Force-restart del worker Railway: lee el timestamp y hace exit(0).
      await upsert("worker_force_restart_at", new Date().toISOString());
    } else if (!enabled) {
      await fetch(`${url}/rest/v1/toolbar_config?key=eq.csv_queue_session_user`, {
        method: "PATCH",
        headers: { "apikey": key, "Authorization": `Bearer ${accessToken}`, "Content-Type": "application/json" },
        body: JSON.stringify({ value: "" }),
      });
    }
    return true;
  } catch (e) {
    console.warn("setCsvQueueEnabled failed:", e.message);
    return false;
  }
}

// ── Domain GEO cache ────────────────────────────────────────────
// Cache permanente por dominio. Se llena oportunamente desde cualquier flow
// que ya obtiene el país (SimilarWeb, Radar, footer, page-signal, TLD).
// Uso típico:
//   const cached = await getDomainGeo(token, "site.com");
//   if (cached) → skip SimilarWeb call (gratis)
//   if (!cached + SimilarWeb returned country) → setDomainGeo(token, "site.com", "AR", "similarweb")

export async function getDomainGeo(accessToken, domain) {
  if (!domain || !accessToken) return null;
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    const res = await fetch(
      `${url}/rest/v1/toolbar_domain_geo_cache?domain=eq.${encodeURIComponent(domain)}&select=domain,country,source,confidence,updated_at&limit=1`,
      { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows?.[0] || null;
  } catch { return null; }
}

// Bulk: trae GEO de varios dominios en 1 request. Usado por autopilot para
// pre-filtrar pool sin llamar SimilarWeb.
export async function getDomainGeosBulk(accessToken, domains) {
  if (!Array.isArray(domains) || domains.length === 0 || !accessToken) return new Map();
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  // PostgREST in.() acepta hasta unos 1000 elementos en URL; chunk si pasamos
  const CHUNK = 500;
  const result = new Map();
  for (let i = 0; i < domains.length; i += CHUNK) {
    const slice = domains.slice(i, i + CHUNK);
    const list  = slice.map(d => `"${d.replace(/"/g, "")}"`).join(",");
    try {
      const res = await fetch(
        `${url}/rest/v1/toolbar_domain_geo_cache?domain=in.(${encodeURIComponent(list)})&select=domain,country,source`,
        { headers: { "apikey": key, "Authorization": `Bearer ${accessToken}` } }
      );
      if (!res.ok) continue;
      const rows = await res.json();
      for (const r of rows) result.set(r.domain, { country: r.country, source: r.source });
    } catch {}
  }
  return result;
}

// Guarda (upsert). Confidence: 1-10 según fuente.
//   similarweb: 9, radar: 8, footer-address: 7, og-locale: 6, lang-region: 5,
//   phone-code: 5, currency: 4, tld: 3
export async function setDomainGeo(accessToken, domain, country, source = "unknown", confidence = 5) {
  if (!domain || !country || !accessToken) return false;
  const url = CONFIG.SUPABASE_URL;
  const key = CONFIG.SUPABASE_ANON_KEY;
  try {
    await fetch(`${url}/rest/v1/toolbar_domain_geo_cache?on_conflict=domain`, {
      method: "POST",
      headers: {
        "apikey": key, "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
      },
      body: JSON.stringify({
        domain,
        country: country.toUpperCase().slice(0, 2),
        source,
        confidence: Math.max(1, Math.min(10, parseInt(confidence) || 5)),
        updated_at: new Date().toISOString(),
      }),
    });
    return true;
  } catch { return false; }
}
