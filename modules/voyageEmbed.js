// ============================================================
// ADEQ TOOLBAR — Voyage embeddings (RAG retrieval for pitch feedback)
// Calls go through the Supabase Edge Function api-proxy.
// ============================================================

import { callProxy } from "./apiProxy.js";

const VOYAGE_MODEL = "voyage-3"; // 1024 dims, $0.06/M tokens

/**
 * Embed a single string with Voyage.
 * @param {string} text
 * @param {"document"|"query"} inputType - "document" when storing, "query" when retrieving
 * @returns {Promise<number[]|null>}  1024-dim vector, or null on failure
 */
export async function voyageEmbed(text, inputType = "query") {
  if (!text || typeof text !== "string") return null;
  try {
    const res = await callProxy("voyage", "/v1/embeddings", {
      method: "POST",
      body: {
        model: VOYAGE_MODEL,
        input: [text.slice(0, 8000)],
        input_type: inputType,
      },
    });
    if (!res.ok) {
      console.warn("[Voyage] embed failed:", res.status, (res.text || "").slice(0, 200));
      return null;
    }
    const vec = res.data?.data?.[0]?.embedding;
    return Array.isArray(vec) ? vec : null;
  } catch (e) {
    console.warn("[Voyage] embed exception:", e.message);
    return null;
  }
}

/**
 * Build the canonical context string we embed for a site.
 * Same shape used at insert time and at retrieval time so similarity makes sense.
 */
export function buildPitchContext({ domain, category, geo, language, traffic }) {
  const trafficStr = traffic
    ? (traffic >= 1_000_000 ? `${Math.round(traffic / 1_000_000)}M` : `${Math.round(traffic / 1_000)}K`)
    : "unknown";
  return [
    `Site: ${domain || "unknown"}`,
    `Category: ${category || "unknown"}`,
    `Geo: ${geo || "unknown"}`,
    `Language: ${language || "unknown"}`,
    `Traffic: ${trafficStr} visits/mo`,
  ].join(" | ");
}
