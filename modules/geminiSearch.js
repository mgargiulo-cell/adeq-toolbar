// ============================================================
// ADEQ TOOLBAR — Búsqueda de Emails vía Gemini + Google Search Grounding
// Usa grounding para buscar en la web real en lugar de memoria entrenada.
// ============================================================

import { CONFIG } from "../config.js";

// Usa Flash con Google Search grounding — busca en la web real
const GEMINI_GROUNDING_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent";

/**
 * Busca emails/contactos de un sitio usando Gemini + Google Search grounding.
 * Grounding hace que Gemini busque en la web en tiempo real en lugar de usar memoria entrenada.
 * @param {string} domain
 * @returns {{ emails: string[], owner: string, linkedin: string, note: string }}
 */
export async function searchEmailsWithGemini(domain) {
  const prompt = `Busca el email de contacto o del CEO/owner/editor del sitio web "${domain}".

Buscá en:
- La página de contacto del sitio (${domain}/contact, ${domain}/contacto, ${domain}/about)
- LinkedIn de la empresa o su CEO
- Registros WHOIS públicos
- Artículos de prensa o entrevistas donde aparezca el email

Devolvé SOLO un JSON con este formato exacto:
{
  "emails": ["email@dominio.com"],
  "owner": "Nombre del CEO/owner si lo encontrás",
  "linkedin": "URL de LinkedIn si existe",
  "note": "fuente o nota breve"
}

Si no encontrás emails reales, devolvé emails como array vacío []. No inventes.`;

  try {
    const response = await fetch(`${GEMINI_GROUNDING_URL}?key=${CONFIG.GEMINI_API_KEY}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        tools: [{ google_search: {} }],
        generationConfig: {
          temperature:     0.1,
          maxOutputTokens: 600,
        },
      }),
    });

    if (!response.ok) {
      const errBody = await response.json().catch(() => ({}));
      throw new Error(`Gemini error ${response.status}: ${errBody?.error?.message || "sin detalle"}`);
    }

    const data = await response.json();
    const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";

    // Extraer JSON de la respuesta (puede venir con texto alrededor)
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      try {
        const parsed = JSON.parse(jsonMatch[0]);
        return {
          emails:   Array.isArray(parsed.emails) ? parsed.emails.filter(e => e && e.includes("@")) : [],
          owner:    parsed.owner    || "",
          linkedin: parsed.linkedin || "",
          note:     parsed.note     || "",
        };
      } catch { /* caer al regex */ }
    }

    // Fallback: buscar emails en el texto libre
    const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6}/g;
    const found = [...new Set((text.match(emailRegex) || []).map(e => e.toLowerCase()))];
    return {
      emails:   found,
      owner:    "",
      linkedin: "",
      note:     found.length ? "Extraído del texto de Gemini" : "No se encontraron emails públicos",
    };

  } catch (err) {
    return { emails: [], owner: "", linkedin: "", note: `Error: ${err.message}` };
  }
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
    const response = await fetch(`${GEMINI_GROUNDING_URL}?key=${CONFIG.GEMINI_API_KEY}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        tools: [{ google_search: {} }],
        generationConfig: { temperature: 0.1, maxOutputTokens: 1000 },
      }),
    });

    const data = await response.json();
    const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";

    const jsonMatch = text.match(/\[[\s\S]*\]/);
    if (jsonMatch) return JSON.parse(jsonMatch[0]);
    return [];

  } catch {
    return [];
  }
}
