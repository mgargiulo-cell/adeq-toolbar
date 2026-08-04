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

// ── BLINDAJE 2026-07-28 ──────────────────────────────────────────────────────────────────
// Este endpoint es público POR DISEÑO: los clientes de correo piden la imagen sin autenticarse,
// así que no se le puede exigir JWT. Pero sin ninguna cota, cualquiera con la URL puede pedirla
// en loop y: (a) llenar toolbar_email_opens y pagarlo nosotros, (b) envenenar las métricas, y
// (c) —lo peor— hacer que el re-engagement crea que abrieron y deje de insistir.
// Tres defensas, en orden de costo:
//   1. el aid tiene que ser un entero razonable (rechaza basura sin tocar la DB)
//   2. tope por IP y por hora, contado de forma atómica en Postgres
//   3. el prefetch (<2 min del envío) ya se descartaba antes
const MAX_HITS_POR_IP_HORA = 120;

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
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cross-Origin-Resource-Policy": "cross-origin",
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
    // Defensa 1: el aid tiene que ser un entero plausible. Cualquier basura se rechaza sin
    // gastar una sola consulta a la base.
    const aidNum = aid && /^\d{1,12}$/.test(aid) ? parseInt(aid, 10) : 0;
    if (aidNum > 0 && !isPrefetch) {
      const ua = req.headers.get("user-agent") || "";
      const ip = req.headers.get("x-forwarded-for")?.split(",")[0].trim()
              || req.headers.get("x-real-ip") || "";
      const ipHash = ip ? await sha256(ip) : null;

      // Defensa 2: tope por IP y hora, atómico. Pasado el tope se sigue devolviendo el pixel
      // (para no delatar la defensa ni romper la vista del mail) pero NO se registra nada.
      if (ipHash) {
        const bucket = `${ipHash}:${new Date().toISOString().slice(0, 13)}`;
        try {
          const rl = await fetch(`${SUPABASE_URL}/rest/v1/rpc/bump_pixel_hits`, {
            method: "POST",
            headers: {
              "apikey": SUPABASE_SERVICE_ROLE,
              "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ p_bucket: bucket }),
          });
          const hits = rl.ok ? Number(await rl.json()) : 0;
          if (hits > MAX_HITS_POR_IP_HORA) {
            // Se deja constancia UNA sola vez por bucket, en el hit exacto que cruza el tope.
            if (hits === MAX_HITS_POR_IP_HORA + 1) {
              fetch(`${SUPABASE_URL}/rest/v1/toolbar_security_events`, {
                method: "POST",
                headers: {
                  "apikey": SUPABASE_SERVICE_ROLE,
                  "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE}`,
                  "Content-Type": "application/json", "Prefer": "return=minimal",
                },
                body: JSON.stringify({
                  kind: "pixel_flood", severity: "critical", actor: ipHash,
                  detail: { hits, ua: ua.slice(0, 200), limite: MAX_HITS_POR_IP_HORA },
                }),
              }).catch(() => {});
            }
            return new Response(PIXEL, { headers });
          }
        } catch {}
      }
      // Fire-and-forget log (no esperamos a Supabase para devolver el pixel)
      fetch(`${SUPABASE_URL}/rest/v1/toolbar_email_opens`, {
        method: "POST",
        headers: {
          "apikey": SUPABASE_SERVICE_ROLE,
          "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE}`,
          "Content-Type": "application/json",
          "Prefer": "return=minimal",
        },
        body: JSON.stringify({ agent_action_id: aidNum, user_agent: ua.slice(0, 500), ip_hash: ipHash }),
      }).catch(() => {});
    }
  } catch {}
  return new Response(PIXEL, { headers });
});
