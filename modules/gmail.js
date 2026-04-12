// ============================================================
// ADEQ TOOLBAR — Gmail OAuth + Send  v3
//
// Uses chrome.identity.getAuthToken — the native Chrome Extension
// OAuth API. Works correctly for Internal Google Workspace apps
// (all @adeqmedia.com accounts authorized automatically).
//
// Token is cached by Chrome automatically. On 401, the cached
// token is removed and a fresh one is requested.
// ============================================================

const GMAIL_SCOPES = [
  "https://www.googleapis.com/auth/gmail.send",
  "https://www.googleapis.com/auth/gmail.settings.basic",
];

// ── Core token fetch ──────────────────────────────────────────

function fetchToken(interactive) {
  return new Promise((resolve) => {
    chrome.identity.getAuthToken({ interactive, scopes: GMAIL_SCOPES }, (token) => {
      if (chrome.runtime.lastError || !token) {
        console.warn("[Gmail] getAuthToken:", chrome.runtime.lastError?.message);
        resolve(null);
      } else {
        resolve(token);
      }
    });
  });
}

function removeCachedToken(token) {
  return new Promise((resolve) => {
    chrome.identity.removeCachedAuthToken({ token }, resolve);
  });
}

// ── Public API ────────────────────────────────────────────────

/**
 * Returns a valid Gmail access token.
 * @param {boolean} interactive - show OAuth window if needed
 * @returns {string|null}
 */
export async function getGmailToken(interactive = true) {
  return fetchToken(interactive);
}

/**
 * Returns the Gmail profile of the authenticated user.
 * @param {boolean} interactive
 * @returns {{ email: string } | null}
 */
export async function getGmailProfile(interactive = false) {
  const token = await fetchToken(interactive);
  if (!token) return null;
  return { email: "authenticated" };
}

/**
 * Returns the Gmail signature (plain text) of the authenticated user.
 * @returns {string}
 */
export async function getGmailSignature() {
  try {
    const token = await fetchToken(true);
    if (!token) return "";

    const res = await fetch(
      "https://www.googleapis.com/gmail/v1/users/me/settings/sendAs",
      { headers: { "Authorization": `Bearer ${token}` } }
    );

    if (!res.ok) {
      if (res.status === 401) await removeCachedToken(token);
      return "";
    }

    const data    = await res.json();
    const primary = data.sendAs?.find(s => s.isDefault) || data.sendAs?.[0];
    const html    = primary?.signature || "";
    if (!html) return "";

    return html
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<\/div>/gi, "\n")
      .replace(/<\/tr>/gi, "\n")
      .replace(/<\/li>/gi, "\n")
      .replace(/<[^>]+>/g, "")
      .replace(/&nbsp;/gi, " ")
      .replace(/&amp;/gi, "&")
      .replace(/&lt;/gi, "<")
      .replace(/&gt;/gi, ">")
      .replace(/&quot;/gi, '"')
      .replace(/\n{3,}/g, "\n\n")
      .trim();
  } catch { return ""; }
}

/**
 * Sends an email via Gmail API.
 * @param {{ to: string, subject: string, body: string, loginEmail?: string }} params
 * @returns {{ ok: boolean, error?: string }}
 */
export async function sendEmail({ to, subject, body }) {
  try {
    let token = await fetchToken(true);
    if (!token) throw new Error("Could not obtain Gmail token. Make sure Chrome is signed in with your @adeqmedia.com account.");

    const raw = buildRaw({ to, subject, body });

    let res = await fetch("https://www.googleapis.com/gmail/v1/users/me/messages/send", {
      method:  "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type":  "application/json",
      },
      body: JSON.stringify({ raw }),
    });

    // Token expired — remove and retry once with a fresh token
    if (res.status === 401) {
      await removeCachedToken(token);
      token = await fetchToken(true);
      if (!token) throw new Error("Authentication expired. Please try again.");

      res = await fetch("https://www.googleapis.com/gmail/v1/users/me/messages/send", {
        method:  "POST",
        headers: {
          "Authorization": `Bearer ${token}`,
          "Content-Type":  "application/json",
        },
        body: JSON.stringify({ raw }),
      });
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err?.error?.message || `Gmail error ${res.status}`);
    }

    return { ok: true };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

// ── Internal helpers ──────────────────────────────────────────

function buildRaw({ to, subject, body }) {
  const lines = [
    `To: ${to}`,
    `Subject: ${subject}`,
    "Content-Type: text/plain; charset=utf-8",
    "MIME-Version: 1.0",
    "",
    body,
  ].join("\r\n");

  const bytes  = new TextEncoder().encode(lines);
  const binary = String.fromCharCode(...bytes);
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}
