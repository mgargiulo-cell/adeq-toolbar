---
name: adeq-toolbar-seguridad
description: "Blindaje de seguridad 2026-08-04: qué se cerró, cómo funciona el modo pánico, la URL secreta y el vigilante. Consultar antes de tocar RLS, edge functions o config sensible."
metadata: 
  node_type: memory
  type: project
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-04T09:37:29.868Z
---

# Blindaje de seguridad — 2026-08-04 (DEPLOYADO Y VERIFICADO)

## 🔴 El agujero que había (verificado en vivo por un agente auditor)
**Cualquier persona de internet podía llevarse TODAS las API keys.** Cadena de tres piezas:
1. El registro del proyecto Supabase estaba ABIERTO (`disable_signup: false`).
2. El allowlist de login vivía en `popup.js` → JS público dentro de la extensión, se saltea
   llamando a Supabase directo.
3. La policy de SELECT de `toolbar_config` era `using (true)` para `authenticated`, y ahí viven
   monday / rapidapi / gemini / apollo / millionverifier.

Un `GET /rest/v1/toolbar_config?select=key,value` y se las llevaba todas. El api-proxy quedaba
bypasseado (usaban las keys crudas). El token de Monday es de cuenta completa → borrado del CRM.
**+ la allowlist del proxy era AUTO-SERVICIO**: el popup escribe `agent_enabled_users` con el JWT
del usuario y esa key no estaba protegida.

## Otros hallazgos aplicados
- **SSRF**: el worker fetchea hosts que salen del ads.txt de terceros (para su sellers.json).
  Un sitio del pool podía poner `169.254.169.254, 1, DIRECT` y hacernos pegar a la red interna de
  Railway. → `hostSeguroParaFetch()` + `fetchExternoSeguro()`, verificado 21/21.
- **Proxy fallaba ABIERTO**: el error al leer config no se chequeaba → kill switch inerte y
  allowlist salteada. Y `permitidos.size > 0 &&` dejaba pasar a todos con lista vacía.
- **El body del proxy lo elegía el cliente**: modelo caro + max_tokens enorme ≈ US$400/día dentro
  del techo global. → allowlist de modelos (haiku/sonnet) + tope 4096.
- **XSS almacenado** en el panel (`email_sources`, `feeder_runs.status`): el CSP bloquea
  `<script>` pero NO `onerror=`, y corría donde vive el token de Monday.
- **Cap de Serper se reseteaba en cada restart** (worker reinicia cada ~7min): tope de 250/día
  re-armado ~200 veces = ~1500 llamadas reales. Costaba plata. → re-siembra desde el persistido.
- **Cuota del proxy era read-then-write** → 50 requests en paralelo pasaban las 50. → RPC atómico.
- `CONFIG.GEMINI_API_KEY` se descargaba al navegador y NUNCA se usaba (gemini.js no tiene fetch).

## Cómo funciona el modo pánico
`kill_switch` en `toolbar_config`. Lo leen **api-proxy** (cada request) y el **worker** (cada
ciclo). Tres formas de apretarlo:
1. **Botón en el panel** (Agent → 🛡️ Seguridad) vía RPC `toggle_kill_switch` (valida el mail
   contra la allowlist; la key está protegida por RLS así que NO se escribe directo).
2. **URL secreta** (edge function `panic`) — para el teléfono, sin abrir Supabase.
   `https://ticjpwimhtfkbccchfyp.supabase.co/functions/v1/panic?k=<PANIC_SECRET>`
   El secreto está en `supabase secrets`. Comparación en tiempo constante, 10 intentos/IP/hora,
   404 (no 403) para no confirmar que existe.
3. **Solo**, cuando el vigilante detecta gasto anómalo.

**Auto-recuperación:** si lo puso el vigilante y pasan 30 min sin anomalías, lo suelta y avisa.
Un freno puesto A MANO nunca se auto-suelta. Configurable: `auto_reapertura='false'`.

## Vigilante (`securityWatchdog`, cada ciclo del worker)
Detecta: gasto del proxy disparado, accesos no autorizados, **flood distribuido** (≥40 excesos de
límite en 1 min), **ataque coordinado** (≥3 orígenes en 5 min), envíos desbocados, rebote
descontrolado, MV sin key. Alerta a **mgargiulo@adeqmedia.com** (máx 1 por tipo cada 60 min).
Toggle `defensa_automatica` para que detecte y avise sin frenar.

**REGLA IMPORTANTE:** el freno automático se dispara SOLO por señales de GASTO REAL. Los intentos
de auth fallidos avisan pero NO frenan — si no, cualquiera apagaría el agente martillando el proxy
con tokens inválidos (DoS gratis contra nosotros). Mismo criterio que excluir el rebote alto.

## Estado verificado
- 45 tablas `toolbar_*` con RLS ON, ninguna expuesta.
- 6 funciones, 2 policies, trigger de config, **trigger de signup activo** (bloquea altas fuera de
  la allowlist y registra cada intento como incidente crítico).
- 3 edge functions deployadas (`panic`, `api-proxy`, `track-open`), smoke tests 4/4.
- SQL completo en `sql/2026-07-28_security_hardening.sql` (12 secciones).

## ⚠️ Cosas a recordar
- **Sumar un MB nuevo:** agregarlo PRIMERO a `agent_enabled_users`, después que se registre. Si no,
  el trigger le rechaza el alta y parece que algo se rompió.
- La config sensible ya NO es escribible desde el popup (policy RESTRICTIVE). Si un toggle del
  panel deja de funcionar, es eso: hay que moverlo a un RPC `security definer` con validación.
- **El mail de alerta NO se probó en vivo.** Usa el mismo Gmail que los pitches. Buscar en logs de
  Railway la línea `🚨 alerta de seguridad enviada`.
- Pendiente menor: pinear el ID real de la extensión en `ORIGENES_OK` del api-proxy (hoy acepta
  cualquier `chrome-extension://[a-p]{32}`).

Relacionado: [[adeq-toolbar-estado-y-pendientes]]
