// ============================================================
// ADEQ Toolbar — Edge Function api-proxy
// Proxies Gemini / Apollo / RapidAPI calls with:
//   - JWT validation (only authenticated toolbar users)
//   - Per-user per-day quota enforcement
//   - Keys stored as Supabase secrets — never exposed to client
//
// Deploy:
//   supabase functions deploy api-proxy --no-verify-jwt
//   supabase secrets set GEMINI_API_KEY=... APOLLO_API_KEY=... RAPIDAPI_KEY=...
//
// Client calls:
//   POST ${SUPABASE_URL}/functions/v1/api-proxy
//   Headers: Authorization: Bearer <user-jwt>, Content-Type: application/json
//   Body: { provider: "gemini"|"apollo"|"rapidapi", path: "...", method, headers?, body? }
// ============================================================

// @ts-nocheck
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";

// ── Per-user daily quota (total calls across providers) ──────
const DAILY_QUOTA_PER_USER = 500;
// Per-provider per-user daily caps
const PROVIDER_CAPS = {
  gemini:    300,
  apollo:    150,
  rapidapi:  400,
  anthropic: 200,
  voyage:    300,
};

const PROVIDERS = {
  gemini: {
    base: "https://generativelanguage.googleapis.com",
    authMode: "query",                // ?key=<KEY>
    keyEnv: "GEMINI_API_KEY",
    allow: /^\/v1beta\/models\/[a-zA-Z0-9._-]+:(generateContent|streamGenerateContent)$/,
  },
  apollo: {
    base: "https://api.apollo.io",
    authMode: "header-x-api-key",     // x-api-key: <KEY>
    keyEnv: "APOLLO_API_KEY",
    allow: /^\/v1\/(mixed_people\/api_search|people\/match|organizations\/enrich)$/,
  },
  rapidapi: {
    // Maxi 2026-08-19: era `similarweb-insights`, que quedó viejo. El worker consulta
    // `website-insights` y es el que está contratado (plan de 40.000/mes); la lista de paths
    // de acá abajo también es la de website-insights (website-details, country-metadata,
    // ai-traffic). O sea que el proxy mandaba a los MBs a una API distinta de la del worker
    // —y probablemente sin suscripción—, mientras traffic.js esperaba el shape de
    // website-insights. Los dos caminos tienen que pegarle a la misma API.
    base: "https://website-insights.p.rapidapi.com",
    authMode: "header-rapidapi",
    keyEnv: "RAPIDAPI_KEY",
    hostHeader: "website-insights.p.rapidapi.com",
    allow: /^\/(all-insights|traffic|ai-traffic|rank|seo|website-details|similar-sites|country-metadata|engagement|countries|similar|category|description|keywords|general|website-analysis)$/,
  },
  anthropic: {
    base: "https://api.anthropic.com",
    authMode: "header-anthropic",     // x-api-key + anthropic-version
    keyEnv: "ANTHROPIC_API_KEY",
    apiVersion: "2023-06-01",
    allow: /^\/v1\/messages$/,
  },
  voyage: {
    base: "https://api.voyageai.com",
    authMode: "header-bearer",        // Authorization: Bearer <KEY>
    keyEnv: "VOYAGE_API_KEY",
    allow: /^\/v1\/embeddings$/,
  },
};

// Maxi 2026-07-28 (blindaje): CORS ya no es "*". Solo la extensión y el dashboard. Con "*"
// cualquier página web podía invocar el proxy desde el navegador de un usuario logueado.
// Maxi 2026-08-18: se fijó el ID REAL. Antes el patrón aceptaba CUALQUIER extensión de Chrome
// ([a-p]{32}), así que una extensión cualquiera instalada en el navegador de un MB podía llamar
// al proxy con la sesión de ese MB. El ID sale de la `key` fija del manifest, así que es estable
// en todas las instalaciones (Web Store y carga manual del zip).
const EXTENSION_ID = "jgbacjjjohjaiojjecgnejcalepkjclm";
const ORIGENES_OK = [
  new RegExp(`^chrome-extension://${EXTENSION_ID}$`),
  /^https:\/\/([a-z0-9-]+\.)*adeqmedia\.com$/, // dashboards propios
  /^https:\/\/([a-z0-9-]+\.)*vercel\.app$/,    // previews de Vercel
];
function corsFor(origin: string) {
  const ok = !origin || ORIGENES_OK.some(re => re.test(origin));
  return {
    "Access-Control-Allow-Origin":  ok ? (origin || "*") : "null",
    "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Vary": "Origin",
    // Cabeceras de endurecimiento — no cuestan nada y cierran vectores tontos.
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cross-Origin-Resource-Policy": "same-site",
  };
}
// Métodos permitidos por proveedor: el path estaba en allowlist pero el método no, así que se
// podía mandar un DELETE a un endpoint que solo debería recibir GET/POST.
const METODOS_OK: Record<string, string[]> = {
  gemini: ["POST"], apollo: ["POST", "GET"], rapidapi: ["GET"],
  anthropic: ["POST"], voyage: ["POST"],
};
// Tope GLOBAL diario, sumando todos los usuarios. Las cuotas por usuario no alcanzan: si
// alguien consigue N cuentas, N×500 llamadas. Esto es el techo duro del gasto.
const CUOTA_GLOBAL_DIA = 2000;

function json(status: number, data: any, origin = "") {
  return new Response(JSON.stringify(data), {
    status, headers: { ...corsFor(origin), "Content-Type": "application/json" },
  });
}

// Deja constancia del incidente. La alerta por mail la dispara el worker leyendo esta tabla.
async function registrarIncidente(sb: any, kind: string, severity: string, actor: string, detail: any) {
  try { await sb.from("toolbar_security_events").insert({ kind, severity, actor, detail }); } catch {}
}

serve(async (req) => {
  const origin = req.headers.get("origin") || "";
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsFor(origin) });
  if (req.method !== "POST")    return json(405, { error: "Method not allowed" }, origin);

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const serviceKey  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const supabase    = createClient(supabaseUrl, serviceKey);
  const ipHash = (req.headers.get("x-forwarded-for") || "").split(",")[0].trim().slice(0, 45);

  // ── Validate user JWT ────────────────────────────────────────
  const authHeader = req.headers.get("authorization") || "";
  const jwt = authHeader.replace(/^Bearer\s+/i, "");
  if (!jwt) return json(401, { error: "Missing bearer token" }, origin);

  // ══════════════════════════════════════════════════════════════════════════════════════
  // EL WORKER ES UN LLAMADOR LEGÍTIMO (Maxi 2026-08-19)
  // ══════════════════════════════════════════════════════════════════════════════════════
  // Durante semanas esto generó mails de "intentos de acceso NO AUTORIZADO" y estuvimos a
  // punto de bloquear la IP. Eran de Railway: NUESTRO PROPIO WORKER.
  //
  // El worker manda `Authorization: Bearer <SERVICE_ROLE_KEY>` (su supabaseLogin() devuelve
  // la service key cuando está configurada — no tiene ningún token de usuario). Acá abajo se
  // validaba con `auth.getUser(jwt)`, que espera un token de USUARIO: una service key no lo
  // es, así que devolvía 401 y registraba `jwt_invalido`. Cada llamada del worker al proxy
  // fallaba. Eso es el "4xx Rate: 100%" del dashboard.
  //
  // Consecuencia real: todo lo que el worker pide por el proxy —clasificación con Haiku,
  // Apollo, embeddings— venía fallando entero, en silencio, y encima se disfrazaba de ataque.
  //
  // La comparación es contra la key exacta que esta misma función tiene en su entorno. No
  // agrega riesgo: quien tenga esa key ya tiene acceso total a la base, el proxy es lo de
  // menos.
  const _esWorker = !!serviceKey && jwt === serviceKey;

  let userEmail: string;
  if (_esWorker) {
    userEmail = "worker@backend";        // identidad propia para las cuotas
  } else {
    const { data: userData, error: userErr } = await supabase.auth.getUser(jwt);
    if (userErr || !userData?.user?.email) {
      // Maxi 2026-08-18: se clasifica el incidente por ORIGEN. Un panel con la sesión vencida
      // generaba decenas de 401 por hora y el Vigilante mandaba "64 intentos de acceso NO
      // AUTORIZADO" — alarma real por una causa benigna. Si el origen es NUESTRO queda como
      // `info` (se registra para auditar, pero el Vigilante no lo cuenta como ataque); si viene
      // de afuera sigue siendo `warn` y sí alerta.
      const _esOrigenPropio = ORIGENES_OK.some((re) => re.test(origin));
      await registrarIncidente(supabase, "jwt_invalido", _esOrigenPropio ? "info" : "warn", ipHash, {
        origin: origin || "(sin origen)",
        propio: _esOrigenPropio,
        motivo: userErr?.message?.slice(0, 120) || "token no válido",
      });
      return json(401, { error: "Invalid token" }, origin);
    }
    userEmail = userData.user.email.toLowerCase();
  }

  // ── BLINDAJE 2026-07-28 ──────────────────────────────────────────────────────────────
  // Leemos config una sola vez: kill switch + allowlist de usuarios.
  // FAIL-CLOSED (auditoría 2026-08-04): antes el error de lectura no se chequeaba. Si Supabase
  // fallaba, cfg quedaba vacío → el kill switch quedaba inerte JUSTO cuando más se lo necesita, y
  // la allowlist se saltaba entera. Ahora un fallo de config corta el servicio.
  const { data: cfgRows, error: cfgErr } = await supabase.from("toolbar_config")
    .select("key,value").in("key", ["kill_switch", "agent_whitelist", "agent_enabled_users", "proxy_extra_users"]);
  if (cfgErr) return json(503, { error: "config no disponible — se corta por precaución" }, origin);
  const cfg: Record<string,string> = {};
  for (const r of (cfgRows || [])) cfg[r.key] = r.value;

  // 1. INTERRUPTOR DE EMERGENCIA: corta TODO el gasto externo de un saque.
  if (String(cfg.kill_switch || "") === "true") {
    return json(503, { error: "kill_switch activo — gasto externo suspendido" }, origin);
  }

  // 2. ALLOWLIST DE USUARIOS. Antes alcanzaba con estar autenticado en el proyecto Supabase:
  // si el registro está abierto, cualquiera se crea una cuenta y gasta 500 llamadas/día
  // (200 de Anthropic) con NUESTRAS keys. Ahora el mail tiene que estar explícitamente listado.
  const permitidos = new Set<string>();
  for (const k of ["agent_whitelist", "agent_enabled_users", "proxy_extra_users"]) {
    const raw = (cfg[k] || "").trim();
    if (!raw) continue;
    try {
      const arr = JSON.parse(raw);
      if (Array.isArray(arr)) arr.forEach((e: string) => permitidos.add(String(e).toLowerCase().trim()));
      else raw.split(/[,;\s]+/).forEach(e => e && permitidos.add(e.toLowerCase()));
    } catch {
      raw.split(/[,;\s]+/).forEach(e => e && permitidos.add(e.toLowerCase()));
    }
  }
  // Sin el `size > 0`: si las tres keys están vacías, la lista vacía tiene que RECHAZAR a todos,
  // no dejar pasar a todos. Fallar abierto acá equivale a no tener allowlist.
  // El worker no está en la allowlist de mails ni tiene por qué estarlo: se identifica con la
  // service key, que es una credencial más fuerte que cualquier lista. El kill switch de arriba
  // SÍ lo frena, que es lo que se quiere de un interruptor de emergencia.
  if (!_esWorker && !permitidos.has(userEmail)) {
    await registrarIncidente(supabase, "proxy_usuario_no_autorizado", "critical", userEmail, { origin, ip: ipHash });
    return json(403, { error: "Usuario no autorizado para el proxy" }, origin);
  }

  // 3. ORIGEN. Si vino de una web y no está en la lista, se rechaza (los clientes sin origin
  // —el worker, curl— pasan: ahí manda el JWT).
  if (origin && !ORIGENES_OK.some(re => re.test(origin))) {
    await registrarIncidente(supabase, "proxy_origen_no_permitido", "critical", userEmail, { origin, ip: ipHash });
    return json(403, { error: "Origen no permitido" }, origin);
  }

  // ── Parse body ──────────────────────────────────────────────
  let payload: any;
  try { payload = await req.json(); } catch { return json(400, { error: "Invalid JSON" }, origin); }
  const { provider, path, method = "GET", headers: extraHeaders = {}, body = null, query = "" } = payload || {};

  const pcfg = PROVIDERS[provider];
  if (!pcfg)               return json(400, { error: "Unknown provider" }, origin);
  if (!pcfg.allow.test(path || "")) {
    await registrarIncidente(supabase, "proxy_path_no_permitido", "warn", userEmail, { provider, path });
    return json(403, { error: "Path not allowed", path }, origin);
  }
  // Método permitido por proveedor (antes solo se validaba el path).
  if (!(METODOS_OK[provider] || []).includes(String(method).toUpperCase()))
    return json(405, { error: `Método ${method} no permitido para ${provider}` }, origin);
  // El querystring lo arma el cliente: acotarlo evita que metan parámetros caros o rarezas.
  if (String(query || "").length > 500) return json(400, { error: "query demasiado larga" }, origin);

  // El BODY también lo elegía el cliente: podía pedir el modelo más caro con max_tokens enorme.
  // Con el techo global de 2000 llamadas/día, 8k tokens de salida en el modelo grande son ~US$400
  // por día. Acá se acota a los modelos que el proyecto realmente usa.
  const MODELOS_OK = new Set(["claude-haiku-4-5", "claude-sonnet-5"]);
  if (provider === "anthropic") {
    if (!MODELOS_OK.has(String(body?.model || ""))) {
      await registrarIncidente(supabase, "proxy_modelo_no_permitido", "critical", userEmail, { modelo: body?.model });
      return json(400, { error: "modelo no permitido", permitidos: [...MODELOS_OK] }, origin);
    }
    if (Number(body?.max_tokens || 0) > 4096) return json(400, { error: "max_tokens > 4096" }, origin);
  }
  if (provider === "voyage" && JSON.stringify(body || {}).length > 100_000)
    return json(400, { error: "body demasiado grande" }, origin);

  // ── CUOTA ATÓMICA (Maxi 2026-07-28) ─────────────────────────────────────────────────
  // Antes era leer-y-después-escribir: 50 requests en paralelo leían el mismo contador y
  // pasaban las 50 aunque quedara 1 de cuota. Ahora se incrementa y se lee en una sola
  // operación en Postgres, así el tope es real. Y se cuenta ANTES de llamar al upstream:
  // un intento fallido igual consume cuota, que es lo correcto contra un atacante.
  const today = new Date().toISOString().slice(0, 10);
  const { data: bumped, error: bumpErr } = await supabase
    .rpc("bump_api_usage", { p_email: userEmail, p_provider: provider });
  if (bumpErr) return json(500, { error: "No se pudo registrar el uso" }, origin);
  const total   = bumped?.[0]?.total ?? 0;
  const provCnt = bumped?.[0]?.prov  ?? 0;

  // ── EL WORKER TIENE SU PROPIA ESCALA (Maxi 2026-08-19) ──────────────────────────────
  // Las cuotas de arriba están pensadas para una PERSONA: si a un MB le roban la sesión, que
  // no pueda gastar más de 500 llamadas. El worker es otra cosa — clasifica con Haiku UNA VEZ
  // POR DOMINIO, y hoy tiene 1.711 en cola más ~750 en Prospects que la revisión repasa a
  // diario. Con el tope de 200 tardaría más de una semana en drenar la cola.
  // El costo real es bajo: cada clasificación son ~200 tokens de entrada y 20 de salida en
  // Haiku, o sea unos USD 0,0003. 5.000 por día son ~USD 1,50.
  // Configurable con `proxy_cuota_worker_<proveedor>` por si hay que ajustarlo sin deployar.
  if (_esWorker) {
    const _capW = parseInt(cfg[`proxy_cuota_worker_${provider}`] || "", 10)
      || ({ anthropic: 5000, voyage: 5000, apollo: 500, rapidapi: 2000, gemini: 500 }[provider] ?? 2000);
    if (provCnt > _capW) {
      await registrarIncidente(supabase, "proxy_cuota_worker", "warn", userEmail, { provider, provCnt, limite: _capW });
      return json(429, { error: `Cuota diaria del worker para ${provider} alcanzada`, limit: _capW, used: provCnt }, origin);
    }
    // El tope global de 2000 es la suma de los usuarios humanos; al worker no le aplica porque
    // ya tiene el suyo, más alto y a propósito. Si le aplicara, se frenaría solo a media cola.
  } else {
  if (total > DAILY_QUOTA_PER_USER) {
    await registrarIncidente(supabase, "proxy_cuota_usuario", "warn", userEmail, { total, limite: DAILY_QUOTA_PER_USER });
    return json(429, { error: "Daily total quota exceeded", limit: DAILY_QUOTA_PER_USER, used: total }, origin);
  }
  if (provCnt > PROVIDER_CAPS[provider]) {
    await registrarIncidente(supabase, "proxy_cuota_proveedor", "warn", userEmail, { provider, provCnt });
    return json(429, { error: `Daily ${provider} quota exceeded`, limit: PROVIDER_CAPS[provider], used: provCnt }, origin);
  }

  // TOPE GLOBAL del día, sumando a todos los usuarios. Las cuotas por usuario no alcanzan:
  // con N cuentas son N×500 llamadas. Este es el techo duro del gasto diario.
  const { data: globalRows } = await supabase
    .from("toolbar_api_usage").select("total").eq("day", today);
  const globalTotal = (globalRows || []).reduce((a: number, r: any) => a + (r.total || 0), 0);
  if (globalTotal > CUOTA_GLOBAL_DIA) {
    await registrarIncidente(supabase, "proxy_cuota_global", "critical", userEmail, { globalTotal, limite: CUOTA_GLOBAL_DIA });
    return json(429, { error: "Tope global diario alcanzado", used: globalTotal, limit: CUOTA_GLOBAL_DIA }, origin);
  }
  }

  // ── Build upstream request ──────────────────────────────────
  const keyVal = Deno.env.get(pcfg.keyEnv);
  if (!keyVal) return json(500, { error: `Missing ${pcfg.keyEnv} secret` }, origin);

  let url = pcfg.base + path;
  if (query) url += (url.includes("?") ? "&" : "?") + query;

  // extraHeaders viene del cliente: filtramos cualquier cabecera de autenticación para que no
  // pueda pisar —ni filtrar— nuestras keys. Las de auth se setean después, abajo.
  const PROHIBIDAS = /^(authorization|x-api-key|x-rapidapi-key|x-rapidapi-host|anthropic-version|cookie|host)$/i;
  const upstreamHeaders: Record<string,string> = { "Content-Type": "application/json" };
  for (const [k, v] of Object.entries(extraHeaders || {})) {
    if (!PROHIBIDAS.test(k) && typeof v === "string" && v.length < 500) upstreamHeaders[k] = v;
  }

  if (pcfg.authMode === "query") {
    url += (url.includes("?") ? "&" : "?") + "key=" + encodeURIComponent(keyVal);
  } else if (pcfg.authMode === "header-x-api-key") {
    upstreamHeaders["X-Api-Key"] = keyVal;
  } else if (pcfg.authMode === "header-rapidapi") {
    upstreamHeaders["x-rapidapi-key"]  = keyVal;
    upstreamHeaders["x-rapidapi-host"] = pcfg.hostHeader;
  } else if (pcfg.authMode === "header-anthropic") {
    upstreamHeaders["x-api-key"]          = keyVal;
    upstreamHeaders["anthropic-version"]  = pcfg.apiVersion || "2023-06-01";
  } else if (pcfg.authMode === "header-bearer") {
    upstreamHeaders["Authorization"]      = `Bearer ${keyVal}`;
  }

  // ── Fetch upstream ──────────────────────────────────────────
  let upstreamRes: Response;
  try {
    upstreamRes = await fetch(url, {
      method,
      headers: upstreamHeaders,
      body: body != null && method !== "GET" && method !== "HEAD" ? JSON.stringify(body) : undefined,
    });
  } catch (e) {
    return json(502, { error: "Upstream fetch failed", detail: String(e) }, origin);
  }

  const upstreamBody = await upstreamRes.text();

  // El uso ya quedó contado arriba de forma atómica (bump_api_usage), antes de llamar al
  // upstream. No hace falta un segundo upsert acá.

  return new Response(upstreamBody, {
    status: upstreamRes.status,
    headers: {
      ...corsFor(origin),
      "Content-Type":     upstreamRes.headers.get("content-type") || "application/json",
      "X-Quota-Remaining": String(DAILY_QUOTA_PER_USER - (total + 1)),
      "X-Provider-Remaining": String(PROVIDER_CAPS[provider] - (provCnt + 1)),
    },
  });
});
