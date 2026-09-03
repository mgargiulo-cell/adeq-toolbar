// ============================================================
// ADEQ TOOLBAR — Módulo Claude (Anthropic API via Edge Function proxy)
// Modelos usados:
//   - Sonnet 4.6 → pitch + revenue gap (creative + analytical)
//   - Haiku 4.5  → follow-up (short, simple)
// ============================================================

import { callProxy } from "./apiProxy.js";

// Maxi 2026-08-07: era claude-sonnet-4-6, que NO está en la lista blanca de modelos del
// api-proxy (la puse el 04/08 con el blindaje). Todas las llamadas a Sonnet venían fallando
// con 400 "modelo no permitido" desde entonces. claude-sonnet-5 es el vigente y el que el
// proxy permite. Haiku ya estaba bien.
export const CLAUDE_SONNET = "claude-sonnet-5";
export const CLAUDE_HAIKU  = "claude-haiku-4-5";

/**
 * Low-level helper for Anthropic's /v1/messages endpoint.
 *
 * @param {object} opts
 * @param {string} opts.model                   - "claude-sonnet-5" | "claude-haiku-4-5"
 * @param {number} [opts.maxTokens=1024]
 * @param {string|Array} [opts.system]          - String, or array of {type:"text", text, cache_control?}
 * @param {Array} opts.messages                 - [{role, content}]
 * @param {object} [opts.thinking]              - e.g. {type:"adaptive"} or {type:"disabled"}
 * @param {object} [opts.outputConfig]          - e.g. {format: {type:"json_schema", schema}, effort: "low"}
 * @returns {Promise<{text, usage, stop_reason, parsed?}>}
 */
export async function callClaude({ model, maxTokens = 1024, system, messages, thinking, outputConfig, motivo = "sin_etiquetar" }) {
  const body = { model, max_tokens: maxTokens, messages };
  if (system)        body.system = system;
  if (thinking)      body.thinking = thinking;
  if (outputConfig)  body.output_config = outputConfig;

  const res = await callProxy("anthropic", "/v1/messages", { method: "POST", body });
  if (!res.ok) {
    const msg = res.data?.error?.message || res.text || "no detail";
    throw Object.assign(new Error(`Claude error ${res.status}: ${msg}`), { status: res.status });
  }

  const data = res.data || {};
  // Response content is an array of typed blocks (text, thinking, ...).
  // Keep only `text` blocks — thinking blocks have empty text on Opus 4.7 default
  // and are never what we want to surface.
  const textBlocks = (data.content || []).filter(b => b?.type === "text");
  const text = textBlocks.map(b => b.text || "").join("\n").trim();

  if (!text) {
    const stop = data.stop_reason || "no content";
    console.error("[Claude] Empty response:", JSON.stringify(data).substring(0, 400));
    throw new Error(`Claude returned no text — stop_reason: ${stop}`);
  }

  // Se anota para qué se gastó. Contar llamadas no alcanza: lo que se factura son tokens, y
  // una de Sonnet con pensamiento cuesta un orden de magnitud más que una de Haiku. Sin el
  // motivo, un mes caro no se puede explicar ni optimizar — sólo pagar.
  _anotarGasto(motivo, model, data.usage).catch(() => {});

  return {
    text,
    usage:       data.usage,
    stop_reason: data.stop_reason,
  };
}

// ── Registro de gasto por motivo ──────────────────────────────────────────────
// Nunca hace fallar la llamada: si no se puede anotar, se pierde el dato, no el trabajo.
async function _anotarGasto(motivo, modelo, usage) {
  try {
    const { CONFIG } = await import("../config.js");
    const { quienEstaAutenticado } = await import("./apiProxy.js");
    const { token, email } = quienEstaAutenticado();
    if (!token) return;
    const dia = new Date().toISOString().slice(0, 10);
    const H = { "apikey": CONFIG.SUPABASE_ANON_KEY, "Authorization": `Bearer ${token}`, "Content-Type": "application/json" };
    const q = `dia=eq.${dia}&fuente=eq.extension&motivo=eq.${encodeURIComponent(motivo)}`
            + `&modelo=eq.${encodeURIComponent(modelo)}&usuario=eq.${encodeURIComponent(email || "")}`;
    const r = await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_claude_gasto?${q}&select=llamadas,tokens_in,tokens_out,cache_in`, { headers: H });
    const prev = r.ok ? (await r.json())[0] : null;
    await fetch(`${CONFIG.SUPABASE_URL}/rest/v1/toolbar_claude_gasto`, {
      method: "POST",
      headers: { ...H, "Prefer": "resolution=merge-duplicates" },
      body: JSON.stringify({
        dia, fuente: "extension", motivo, modelo, usuario: email || "",
        llamadas:   (prev?.llamadas   || 0) + 1,
        tokens_in:  (prev?.tokens_in  || 0) + (usage?.input_tokens  || 0),
        tokens_out: (prev?.tokens_out || 0) + (usage?.output_tokens || 0),
        cache_in:   (prev?.cache_in   || 0) + (usage?.cache_read_input_tokens || 0) + (usage?.cache_creation_input_tokens || 0),
        actualizado: new Date().toISOString(),
      }),
    });
  } catch {}
}
