// Edge Function: track-open
// Devuelve un PNG 1x1 transparente y graba el open en toolbar_email_opens.
// URL: https://<project>.supabase.co/functions/v1/track-open?aid=<agent_action_id>
//
// Deploy:
//   supabase functions deploy track-open --no-verify-jwt
//
// El --no-verify-jwt es importante: este endpoint debe ser público (los Gmail
// clients hacen GET sin auth headers).

const SUPABASE_URL          = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// PNG transparente 1x1 (43 bytes)
const PIXEL = Uint8Array.from([
  0x89,0x50,0x4E,0x47,0x0D,0x0A,0x1A,0x0A,0x00,0x00,0x00,0x0D,
  0x49,0x48,0x44,0x52,0x00,0x00,0x00,0x01,0x00,0x00,0x00,0x01,
  0x08,0x06,0x00,0x00,0x00,0x1F,0x15,0xC4,0x89,0x00,0x00,0x00,
  0x0D,0x49,0x44,0x41,0x54,0x78,0x9C,0x63,0x00,0x01,0x00,0x00,
  0x05,0x00,0x01,0x0D,0x0A,0x2D,0xB4,0x00,0x00,0x00,0x00,0x49,
  0x45,0x4E,0x44,0xAE,0x42,0x60,0x82
]);

async function sha256(s: string): Promise<string> {
  const buf = new TextEncoder().encode(s);
  const hash = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, "0")).join("").slice(0, 16);
}

Deno.serve(async (req) => {
  const headers = {
    "Content-Type": "image/png",
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
  };
  try {
    const url = new URL(req.url);
    const aid = url.searchParams.get("aid");
    // Maxi 2026-07-27 (auditoría de aperturas): descartar el PREFETCH.
    // El worker ahora firma el pixel con &t=<epoch del envío>. Medición sobre 30 días:
    //   · 19,8% de las "aperturas" llegaban a menos de 30 seg del envío
    //   ·  3,4% más entre 30 seg y 2 min
    // Nadie abre un cold email en 30 segundos: eso es Google/Outlook precargando la imagen y
    // los escáneres de seguridad corporativos (Proofpoint, Mimecast) abriendo todo lo que entra.
    // Inflaba el open rate y, peor, el agente lo usa para decidir: getDynamicSourceRank rankea
    // fuentes por open rate y el re-engagement NO vuelve a insistir si cree que ya lo abrieron.
    // Filtramos por TIEMPO y no por user-agent a propósito: el 24,8% de los hits viene de
    // GoogleImageProxy y ahí adentro hay aperturas reales (Gmail siempre proxea imágenes),
    // así que descartar ese UA borraría el dato bueno junto con el malo.
    const sentAt = parseInt(url.searchParams.get("t") || "0", 10);
    const isPrefetch = sentAt > 0 && (Date.now() - sentAt) < 120_000;
    if (aid && /^\d+$/.test(aid) && !isPrefetch) {
      const ua = req.headers.get("user-agent") || "";
      const ip = req.headers.get("x-forwarded-for")?.split(",")[0].trim()
              || req.headers.get("x-real-ip") || "";
      const ipHash = ip ? await sha256(ip) : null;
      // Fire-and-forget log (no esperamos a Supabase para devolver el pixel)
      fetch(`${SUPABASE_URL}/rest/v1/toolbar_email_opens`, {
        method: "POST",
        headers: {
          "apikey": SUPABASE_SERVICE_ROLE,
          "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE}`,
          "Content-Type": "application/json",
          "Prefer": "return=minimal",
        },
        body: JSON.stringify({ agent_action_id: parseInt(aid, 10), user_agent: ua.slice(0, 500), ip_hash: ipHash }),
      }).catch(() => {});
    }
  } catch {}
  return new Response(PIXEL, { headers });
});
