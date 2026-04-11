// ============================================================
// ADEQ TOOLBAR — Gmail OAuth + Send  v2
//
// Uses chrome.identity.launchWebAuthFlow (implicit flow) so that
// ANY Google account (Agus, Diego, Max) can authenticate on any
// machine, regardless of the extension's unpacked ID.
//
// Token is cached in chrome.storage.local per toolbar login email
// to avoid asking for OAuth on every send.
// ============================================================

const GMAIL_SCOPES = [
  "https://www.googleapis.com/auth/gmail.send",
  "https://www.googleapis.com/auth/gmail.settings.basic",
].join(" ");

// ── Token cache helpers ───────────────────────────────────────
// Key: "gmail_token_<loginEmail>"  →  { token, expiresAt }

async function getCachedToken(loginEmail) {
  try {
    const key = `gmail_token_${loginEmail || "shared"}`;
    const { [key]: cached } = await chrome.storage.local.get(key);
    if (!cached) return null;
    if (Date.now() > cached.expiresAt - 120_000) return null; // expire 2 min early
    return cached.token;
  } catch { return null; }
}

async function setCachedToken(loginEmail, token, expiresInSec) {
  try {
    const key = `gmail_token_${loginEmail || "shared"}`;
    await chrome.storage.local.set({
      [key]: { token, expiresAt: Date.now() + expiresInSec * 1000 },
    });
  } catch {}
}

async function clearCachedToken(loginEmail) {
  try {
    const key = `gmail_token_${loginEmail || "shared"}`;
    await chrome.storage.local.remove(key);
  } catch {}
}

// ── Core OAuth via launchWebAuthFlow ─────────────────────────
//
// Works for any Google account on any machine.
// Google treats *.chromiumapp.org redirects as trusted for
// Chrome-App–type OAuth clients, so no exact extension-ID
// registration is needed.
//
// If the OAuth app is in "Testing" mode in Google Cloud Console,
// Agus and Diego must be added as test users by the admin (Max).

async function fetchTokenInteractive(loginEmail) {
  const clientId   = "1006462691161-6uicvg6urcco0a50534c46l4jiclfm70.apps.googleusercontent.com";
  const redirectUri = `https://${chrome.runtime.id}.chromiumapp.org/`;

  const authUrl = new URL("https://accounts.google.com/o/oauth2/auth");
  authUrl.searchParams.set("client_id",     clientId);
  authUrl.searchParams.set("response_type", "token");
  authUrl.searchParams.set("redirect_uri",  redirectUri);
  authUrl.searchParams.set("scope",         GMAIL_SCOPES);
  authUrl.searchParams.set("prompt",        "select_account");
  // Pre-fill the login hint so users don't have to pick the wrong account
  if (loginEmail) authUrl.searchParams.set("login_hint", loginEmail);

  return new Promise((resolve) => {
    chrome.identity.launchWebAuthFlow(
      { url: authUrl.toString(), interactive: true },
      async (redirectUrl) => {
        if (chrome.runtime.lastError || !redirectUrl) {
          const msg = chrome.runtime.lastError?.message || "OAuth window closed";
          console.error("[Gmail] launchWebAuthFlow error:", msg);
          resolve(null);
          return;
        }
        try {
          const hash   = new URL(redirectUrl).hash.slice(1);
          const params = new URLSearchParams(hash);
          const token  = params.get("access_token");
          const expiresIn = parseInt(params.get("expires_in") || "3600");
          if (token) {
            await setCachedToken(loginEmail, token, expiresIn);
          }
          resolve(token || null);
        } catch {
          resolve(null);
        }
      }
    );
  });
}

// ── Public API ────────────────────────────────────────────────

/**
 * Returns a valid Gmail access token for the given toolbar loginEmail.
 * Uses the cache first; falls back to interactive OAuth.
 *
 * @param {boolean} interactive  - true = show OAuth window if needed
 * @param {string}  [loginEmail] - toolbar login email (used as cache key + login_hint)
 * @returns {string|null}
 */
export async function getGmailToken(interactive = true, loginEmail = "") {
  const cached = await getCachedToken(loginEmail);
  if (cached) return cached;
  if (!interactive) return null;
  return fetchTokenInteractive(loginEmail);
}

/**
 * Returns the Gmail profile (email address) for the currently active token.
 * Non-interactive: returns null if no cached token exists.
 *
 * @param {boolean} [interactive]
 * @param {string}  [loginEmail]
 * @returns {{ email: string } | null}
 */
export async function getGmailProfile(interactive = false, loginEmail = "") {
  const token = await getGmailToken(interactive, loginEmail);
  if (!token) return null;

  try {
    const res = await fetch(
      "https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=" + encodeURIComponent(token)
    );
    if (!res.ok) return { email: loginEmail || "authenticated" };
    const data = await res.json();
    return { email: data.email || loginEmail || "authenticated" };
  } catch {
    return { email: loginEmail || "authenticated" };
  }
}

/**
 * Returns the Gmail signature (plain text) for the logged-in account.
 *
 * @param {string} [loginEmail]
 * @returns {string}
 */
export async function getGmailSignature(loginEmail = "") {
  try {
    const token = await getGmailToken(true, loginEmail);
    if (!token) return "";

    const res = await fetch(
      "https://www.googleapis.com/gmail/v1/users/me/settings/sendAs",
      { headers: { "Authorization": `Bearer ${token}` } }
    );
    if (!res.ok) {
      if (res.status === 401) await clearCachedToken(loginEmail); // invalidate
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
 *
 * @param {{ to: string, subject: string, body: string, loginEmail?: string }} params
 * @returns {{ ok: boolean, error?: string }}
 */
export async function sendEmail({ to, subject, body, loginEmail = "" }) {
  try {
    let token = await getGmailToken(false, loginEmail);

    // If no cached token, ask interactively
    if (!token) token = await getGmailToken(true, loginEmail);
    if (!token) throw new Error("Could not obtain Gmail token. Make sure your Google account is authorized.");

    const raw = buildRaw({ to, subject, body });

    const res = await fetch("https://www.googleapis.com/gmail/v1/users/me/messages/send", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Content-Type":  "application/json",
      },
      body: JSON.stringify({ raw }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      if (res.status === 401) {
        await clearCachedToken(loginEmail); // force re-auth next time
        throw new Error("Gmail token expired — please try again to re-authenticate.");
      }
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
