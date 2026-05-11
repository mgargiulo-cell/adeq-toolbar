// ============================================================
// ADEQ TOOLBAR — Voyage embeddings (RAG retrieval for pitch feedback)
// Calls go through the Supabase Edge Function api-proxy.
// ============================================================

import { callProxy } from "./apiProxy.js";

const VOYAGE_MODEL = "voyage-3"; // 1024 dims, $0.06/M tokens

// ── Cache in-memory (TTL 1h, max 200 entries) ──────────────
// Evita re-embeber el mismo input string en una sesión. Útil cuando el user
// re-genera pitches con la misma config: cada like/dislike re-embebe el mismo
// context. Cache key = hash simple del input.
const _voyageCache = new Map(); // key → { vec, ts }
const VOYAGE_CACHE_TTL_MS = 60 * 60 * 1000;
const VOYAGE_CACHE_MAX    = 200;
function _voyageCacheKey(text, inputType) {
  // Hash simple djb2 (suficiente para detectar duplicados exactos)
  let h = 5381;
  const s = `${inputType}::${text}`;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) | 0;
  return String(h);
}

/**
 * Embed a single string with Voyage.
 * @param {string} text
 * @param {"document"|"query"} inputType - "document" when storing, "query" when retrieving
 * @returns {Promise<number[]|null>}  1024-dim vector, or null on failure
 */
export async function voyageEmbed(text, inputType = "query") {
  if (!text || typeof text !== "string") return null;
  const cacheKey = _voyageCacheKey(text, inputType);
  const cached = _voyageCache.get(cacheKey);
  if (cached && (Date.now() - cached.ts) < VOYAGE_CACHE_TTL_MS) {
    return cached.vec;
  }
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
    if (Array.isArray(vec)) {
      // Eviction simple: si el Map está lleno, borrar la entry más vieja
      if (_voyageCache.size >= VOYAGE_CACHE_MAX) {
        const firstKey = _voyageCache.keys().next().value;
        _voyageCache.delete(firstKey);
      }
      _voyageCache.set(cacheKey, { vec, ts: Date.now() });
      return vec;
    }
    return null;
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
