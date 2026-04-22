// ============================================================
// ADEQ TOOLBAR — Módulo Claude (Anthropic API via Edge Function proxy)
// Modelos usados:
//   - Sonnet 4.6 → pitch + revenue gap (creative + analytical)
//   - Haiku 4.5  → follow-up (short, simple)
// ============================================================

import { callProxy } from "./apiProxy.js";

export const CLAUDE_SONNET = "claude-sonnet-4-6";
export const CLAUDE_HAIKU  = "claude-haiku-4-5";

/**
 * Low-level helper for Anthropic's /v1/messages endpoint.
 *
 * @param {object} opts
 * @param {string} opts.model                   - "claude-sonnet-4-6" | "claude-haiku-4-5"
 * @param {number} [opts.maxTokens=1024]
 * @param {string|Array} [opts.system]          - String, or array of {type:"text", text, cache_control?}
 * @param {Array} opts.messages                 - [{role, content}]
 * @param {object} [opts.thinking]              - e.g. {type:"adaptive"} or {type:"disabled"}
 * @param {object} [opts.outputConfig]          - e.g. {format: {type:"json_schema", schema}, effort: "low"}
 * @returns {Promise<{text, usage, stop_reason, parsed?}>}
 */
export async function callClaude({ model, maxTokens = 1024, system, messages, thinking, outputConfig }) {
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

  return {
    text,
    usage:       data.usage,
    stop_reason: data.stop_reason,
  };
}
