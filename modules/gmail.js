// ============================================================
// ADEQ TOOLBAR — Gmail OAuth + Send
// Usa chrome.identity para obtener token y Gmail API para enviar.
// Requiere manifest.json con permission "identity" y oauth2.client_id
// ============================================================

/**
 * Obtiene el perfil del usuario autenticado en Gmail.
 * @param {boolean} interactive - true para mostrar flujo OAuth al usuario
 * @returns {{ email: string } | null}
 */
export async function getGmailProfile(interactive = false) {
  return new Promise((resolve) => {
    chrome.identity.getAuthToken({ interactive }, (token) => {
      if (chrome.runtime.lastError || !token) {
        console.error("[Gmail] getAuthToken error:", chrome.runtime.lastError?.message);
        resolve(null); return;
      }
      // Leer el email del perfil de Chrome sin llamada extra a la API (evita 403 por scope)
      chrome.identity.getProfileUserInfo({ accountStatus: "ANY" }, (info) => {
        resolve(info?.email ? { email: info.email } : { email: "authenticated" });
      });
    });
  });
}

/**
 * Obtiene la firma por defecto de Gmail del usuario autenticado.
 * Devuelve texto plano (HTML stripeado). Vacío si no hay firma o no hay token.
 * @param {string} [existingToken] - token ya obtenido (evita segunda llamada OAuth)
 * @returns {string}
 */
export async function getGmailSignature(existingToken) {
  const getToken = () => new Promise((resolve) => {
    // Si ya tenemos token, reutilizarlo; si no, pedir interactivo para incluir settings scope
    if (existingToken) { resolve(existingToken); return; }
    chrome.identity.getAuthToken({ interactive: true }, (token) => {
      if (chrome.runtime.lastError || !token) { resolve(null); return; }
      resolve(token);
    });
  });

  try {
    const token = await getToken();
    if (!token) return "";

    const res  = await fetch(
      "https://www.googleapis.com/gmail/v1/users/me/settings/sendAs",
      { headers: { "Authorization": `Bearer ${token}` } }
    );
    if (!res.ok) return "";
    const data = await res.json();
    const primary = data.sendAs?.find(s => s.isDefault) || data.sendAs?.[0];
    const html    = primary?.signature || "";
    if (!html) return "";

    // Convertir HTML a texto plano preservando saltos de línea
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
 * Envía un email via Gmail API usando OAuth.
 * @param {Object} params - { to, subject, body }
 * @returns {{ ok: boolean, error?: string }}
 */
export async function sendEmail({ to, subject, body }) {
  try {
    const token = await getAuthToken();

    // RFC 2822 → base64url
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
      throw new Error(err?.error?.message || `Gmail error ${res.status}`);
    }

    return { ok: true };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

// ---- Helpers ----

/**
 * Obtiene el auth token de Gmail (interactivo = muestra OAuth si es necesario).
 * @param {boolean} interactive
 * @returns {string|null}
 */
export function getGmailToken(interactive = true) {
  return new Promise((resolve) => {
    chrome.identity.getAuthToken({ interactive }, (token) => {
      if (chrome.runtime.lastError || !token) { resolve(null); return; }
      resolve(token);
    });
  });
}

function getAuthToken() {
  return getGmailToken(true);
}

function buildRaw({ to, subject, body }) {
  const lines = [
    `To: ${to}`,
    `Subject: ${subject}`,
    "Content-Type: text/plain; charset=utf-8",
    "MIME-Version: 1.0",
    "",
    body,
  ].join("\r\n");

  // base64url encoding
  const bytes = new TextEncoder().encode(lines);
  const binary = String.fromCharCode(...bytes);
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}
