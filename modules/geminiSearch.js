// ============================================================
// ADEQ TOOLBAR — Búsqueda de Emails vía Gemini + Google Search Grounding
// Usa grounding para buscar en la web real en lugar de memoria entrenada.
// ============================================================

import { CONFIG }    from "../config.js";
import { callProxy } from "./apiProxy.js";

// Gemini 2.5 Flash con Google Search grounding — busca en la web real (via Edge Function proxy)
const GEMINI_GROUNDING_PATH = "/v1beta/models/gemini-2.5-flash:generateContent";

/**
 * Busca emails/contactos de un sitio usando Gemini + Google Search grounding.
 * Grounding hace que Gemini busque en la web en tiempo real en lugar de usar memoria entrenada.
 * @param {string} domain
 * @returns {{ emails: string[], owner: string, linkedin: string, note: string }}
 */
export async function searchEmailsWithGemini(domain) {
  // Prompt reforzado: fuerza el uso del web search, obliga JSON limpio
  const prompt = `You are an email researcher. You MUST use Google Search to look up real information on the web — DO NOT rely on training data.

Target website: "${domain}"

Search the web for the official CEO, founder, owner, or publisher's BUSINESS email of this domain. Check:
1. The site's own "Contact", "About", "Staff", "Team", or "Advertise" pages (try ${domain}/contact, ${domain}/about, ${domain}/team, ${domain}/advertise)
2. Press releases or news articles mentioning the CEO/founder and any email
3. LinkedIn profiles of executives linked to ${domain}
4. Public WHOIS records (only if not privacy-protected)

RULES:
- Never fabricate an email. If you cannot verify it from a web source, return an empty emails array.
- Prefer personal emails (ceo@, founder@, firstname@) over role-based (info@, contact@).
- Do NOT return email_not_unlocked@*, noreply@*, do-not-reply@*, press-release@*, or privacy-proxy emails (markmonitor, whoisguard, domainsbyproxy, contactprivacy).

Return ONLY a raw JSON object (no markdown, no code fence, no prose):
{"emails":["<email1>","<email2>"],"owner":"<name>","linkedin":"<url>","note":"<source or reason>"}`;

  const attempt = async (retryNum = 0) => {
    try {
      const response = await callProxy("gemini", GEMINI_GROUNDING_PATH, {
        method: "POST",
        body: {
          contents: [{ role: "user", parts: [{ text: prompt }] }],
          tools: [{ google_search: {} }],
          generationConfig: { temperature: 0.1, maxOutputTokens: 800 },
        },
      });

      if (response.status === 429 && retryNum < 2) {
        console.warn(`[Gemini] ${domain}: rate limit, retry in ${(retryNum + 1) * 2}s`);
        await new Promise(r => setTimeout(r, (retryNum + 1) * 2000));
        return attempt(retryNum + 1);
      }

      if (!response.ok) {
        const msg = response.data?.error?.message || `HTTP ${response.status}`;
        console.warn(`[Gemini] ${domain}: request failed — ${msg}`);
        return { emails: [], owner: "", linkedin: "", note: `Error: ${msg}` };
      }

      const data = response.data || {};
      const candidate = data?.candidates?.[0];
      const text = candidate?.content?.parts?.[0]?.text || "";

      // Log grounding metadata para debug
      const groundingMetadata = candidate?.groundingMetadata;
      const searchesDone = groundingMetadata?.webSearchQueries?.length || 0;
      const sourcesFound = groundingMetadata?.groundingChunks?.length || 0;
      console.log(`[Gemini] ${domain}: searches=${searchesDone}, sources=${sourcesFound}, text_length=${text.length}`);

      if (!text) {
        const blockReason = data?.promptFeedback?.blockReason || candidate?.finishReason || "empty";
        console.warn(`[Gemini] ${domain}: empty response, reason=${blockReason}`);
        return { emails: [], owner: "", linkedin: "", note: `Respuesta vacía (${blockReason})` };
      }

      // Primero intentar parsear todo el texto como JSON (responseMimeType lo fuerza)
      let parsed = null;
      try {
        parsed = JSON.parse(text);
      } catch {
        // Si falla, buscar el primer {...} embebido
        const jsonMatch = text.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
          try { parsed = JSON.parse(jsonMatch[0]); } catch {}
        }
      }

      if (parsed) {
        const emails = Array.isArray(parsed.emails)
          ? parsed.emails.filter(e => e && typeof e === "string" && e.includes("@") && !/email_not_unlocked|noreply|no-reply|markmonitor|whoisguard/i.test(e))
          : [];
        return {
          emails,
          owner:    parsed.owner    || "",
          linkedin: parsed.linkedin || "",
          note:     emails.length ? (parsed.note || `Gemini encontró ${emails.length} email(s)`) : (parsed.note || "Gemini no encontró emails"),
        };
      }

      // Fallback: regex sobre texto libre (si parsing falló)
      const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6}/g;
      const found = [...new Set((text.match(emailRegex) || []).map(e => e.toLowerCase()))]
        .filter(e => !/email_not_unlocked|noreply|no-reply|markmonitor|whoisguard/i.test(e));
      return {
        emails:   found,
        owner:    "",
        linkedin: "",
        note:     found.length ? "Parseado via regex del texto" : "JSON inválido y sin emails en texto libre",
      };

    } catch (err) {
      console.warn(`[Gemini] ${domain}: exception — ${err.message}`);
      return { emails: [], owner: "", linkedin: "", note: `Error: ${err.message}` };
    }
  };

  return attempt();
}

/**
 * Busca emails para una lista de dominios usando Gemini en lote.
 * @param {string[]} domains
 */
export async function batchSearchEmails(domains) {
  const list = domains.slice(0, 20).join("\n");

  const prompt = `Para cada uno de los siguientes sitios web, busca su email de contacto público más relevante (preferentemente del CEO, owner o editor).

Sitios:
${list}

Responde SOLO con un JSON array:
[
  { "domain": "sitio.com", "email": "contacto@sitio.com", "name": "Nombre si se conoce" },
  ...
]
Si no tienes email confiable para un sitio, pon "email": null.`;

  try {
    const response = await callProxy("gemini", GEMINI_GROUNDING_PATH, {
      method: "POST",
      body: {
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        tools: [{ google_search: {} }],
        generationConfig: { temperature: 0.1, maxOutputTokens: 1000 },
      },
    });

    const data = response.data || {};
    const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";

    const jsonMatch = text.match(/\[[\s\S]*\]/);
    if (jsonMatch) return JSON.parse(jsonMatch[0]);
    return [];

  } catch {
    return [];
  }
}
