// ══════════════════════════════════════════════════════════════════════════════════════════
// ADEQ Toolbar — MODO PÁNICO por URL secreta (Maxi 2026-08-04)
// ══════════════════════════════════════════════════════════════════════════════════════════
// El freno de emergencia ya existía como bandera en la base, pero solo se podía tocar desde el
// editor SQL de Supabase. Esto lo vuelve un clic desde cualquier lado — el teléfono, un
// marcador del navegador, un atajo — que es lo que hace falta cuando el problema pasa un sábado.
//
// Uso (guardalo como marcador):
//   FRENAR:  https://<proj>.supabase.co/functions/v1/panic?k=<SECRETO>&a=on
//   SOLTAR:  https://<proj>.supabase.co/functions/v1/panic?k=<SECRETO>&a=off
//   VER:     https://<proj>.supabase.co/functions/v1/panic?k=<SECRETO>
//
// Deploy:
//   supabase secrets set PANIC_SECRET=<algo largo y aleatorio>
//   supabase functions deploy panic --no-verify-jwt
//
// Seguridad de este endpoint (es público por necesidad — tiene que andar sin login):
//   · el secreto se compara en tiempo constante, para que no se pueda adivinar midiendo demoras
//   · máximo 10 intentos por IP por hora; pasado eso responde 429 aunque el secreto sea correcto
//   · todo intento fallido queda registrado como incidente crítico y dispara la alerta por mail
//   · nunca devuelve datos: solo el estado del freno
// @ts-nocheck
const SUPABASE_URL  = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const PANIC_SECRET  = Deno.env.get("PANIC_SECRET") || "";
const MAX_INTENTOS_HORA = 10;

const H = {
  "Content-Type": "text/html; charset=utf-8",
  "Cache-Control": "no-store",
  "X-Content-Type-Options": "nosniff",
  "Referrer-Policy": "no-referrer",
};

// Comparación en tiempo constante: un `===` normal corta en el primer carácter distinto y deja
// medir el secreto byte por byte cronometrando las respuestas.
function igualSeguro(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let dif = 0;
  for (let i = 0; i < a.length; i++) dif |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return dif === 0;
}

async function sha(s: string): Promise<string> {
  const h = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return Array.from(new Uint8Array(h)).map(b => b.toString(16).padStart(2, "0")).join("").slice(0, 16);
}

const api = (path: string, init: RequestInit = {}) => fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
  ...init,
  headers: {
    "apikey": SERVICE_ROLE, "Authorization": `Bearer ${SERVICE_ROLE}`,
    "Content-Type": "application/json", ...(init.headers || {}),
  },
});

async function incidente(kind: string, actor: string, detail: any) {
  try {
    await api("toolbar_security_events", {
      method: "POST", headers: { "Prefer": "return=minimal" },
      body: JSON.stringify({ kind, severity: "critical", actor, detail }),
    });
  } catch {}
}

function pagina(estado: string, motivo: string, secreto: string) {
  const activo = estado === "true";
  return `<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{font:16px/1.5 system-ui;margin:0;padding:24px;background:${activo ? "#2b0d0d" : "#0d2b12"};color:#fff}
h1{font-size:22px;margin:0 0 4px}p{opacity:.85;margin:4px 0}
a{display:block;margin-top:20px;padding:16px;border-radius:12px;text-align:center;font-weight:700;text-decoration:none;color:#fff}
.on{background:#b3261e}.off{background:#1e7d32}small{opacity:.6;display:block;margin-top:24px}</style>
<h1>${activo ? "🛑 FRENO ACTIVO" : "✅ Operando normal"}</h1>
<p>${activo ? "El gasto en APIs externas está cortado y el agente no envía." : "Todo funcionando."}</p>
${motivo ? `<p><b>Motivo:</b> ${motivo.replace(/[<>&]/g, "")}</p>` : ""}
${activo
  ? `<a class="off" href="?k=${encodeURIComponent(secreto)}&a=off">SOLTAR EL FRENO</a>`
  : `<a class="on"  href="?k=${encodeURIComponent(secreto)}&a=on">🛑 FRENAR TODO AHORA</a>`}
<small>ADEQ Toolbar · ${new Date().toISOString()}</small>`;
}

Deno.serve(async (req) => {
  const url = new URL(req.url);
  const k = url.searchParams.get("k") || "";
  const a = (url.searchParams.get("a") || "").toLowerCase();
  const ip = (req.headers.get("x-forwarded-for") || "").split(",")[0].trim();
  const ipHash = await sha(ip || "sin-ip");

  if (!PANIC_SECRET) return new Response("PANIC_SECRET no configurado", { status: 500, headers: H });

  // Rate limit ANTES de comparar el secreto: sin esto se puede probar a fuerza bruta.
  try {
    const bucket = `panic:${ipHash}:${new Date().toISOString().slice(0, 13)}`;
    const r = await fetch(`${SUPABASE_URL}/rest/v1/rpc/bump_pixel_hits`, {
      method: "POST",
      headers: { "apikey": SERVICE_ROLE, "Authorization": `Bearer ${SERVICE_ROLE}`, "Content-Type": "application/json" },
      body: JSON.stringify({ p_bucket: bucket }),
    });
    const hits = r.ok ? Number(await r.json()) : 0;
    if (hits > MAX_INTENTOS_HORA) {
      await incidente("panic_fuerza_bruta", ipHash, { hits });
      return new Response("Demasiados intentos", { status: 429, headers: H });
    }
  } catch {}

  if (!igualSeguro(k, PANIC_SECRET)) {
    await incidente("panic_secreto_invalido", ipHash, { largo_recibido: k.length });
    return new Response("No", { status: 404, headers: H });   // 404 y no 403: no confirma que exista
  }

  // Cambiar el estado si vino la acción.
  if (a === "on" || a === "off") {
    const valor = a === "on" ? "true" : "false";
    const motivo = a === "on"
      ? `${new Date().toISOString()} — activado a mano desde la URL de pánico`
      : "";
    for (const [key, value] of [["kill_switch", valor], ["kill_switch_reason", motivo]]) {
      await api(`toolbar_config?key=eq.${encodeURIComponent(key)}`, {
        method: "PATCH", headers: { "Prefer": "return=minimal" }, body: JSON.stringify({ value }),
      });
    }
    await incidente(a === "on" ? "panic_activado_manual" : "panic_desactivado_manual", ipHash, { ip_hash: ipHash });
  }

  // Leer y mostrar el estado.
  let estado = "false", motivo = "";
  try {
    const r = await api("toolbar_config?key=in.(kill_switch,kill_switch_reason)&select=key,value");
    for (const row of (r.ok ? await r.json() : [])) {
      if (row.key === "kill_switch") estado = row.value;
      if (row.key === "kill_switch_reason") motivo = row.value || "";
    }
  } catch {}

  return new Response(pagina(estado, motivo, k), { headers: H });
});
