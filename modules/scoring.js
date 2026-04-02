// ============================================================
// ADEQ TOOLBAR — Scoring de Prospectos
// Escala A-D basada en: tráfico, partners detectados, email encontrado.
// Más tráfico = mejor. Más partners = peor (menos oportunidad).
// ============================================================

/**
 * @param {Object} p
 * @param {number}  p.pageViews   - Páginas vistas mensuales
 * @param {number}  p.rawVisits   - Visitas mensuales (fallback si no hay pageViews)
 * @param {Array}   p.partners    - Array de {name, found}. null = datos no disponibles (cascada)
 * @param {boolean} p.emailFound  - Si se encontró un email. null = desconocido
 * @returns {{ grade: string, color: string, label: string }}
 */
export function scoreProspect({ pageViews, rawVisits, partners = null, emailFound = null }) {
  let score    = 0;
  let maxScore = 0;

  // ── Tráfico (0-50 pts) — a más tráfico, mejor ──────────────
  maxScore += 50;
  const traffic = pageViews || rawVisits || 0;
  if      (traffic >= 50_000_000) score += 50;
  else if (traffic >= 10_000_000) score += 42;
  else if (traffic >=  5_000_000) score += 35;
  else if (traffic >=  2_000_000) score += 28;
  else if (traffic >=  1_000_000) score += 20;
  else if (traffic >=    500_000) score += 12;
  else                            score +=  4;

  // ── Partners detectados (0-30 pts) — más partners = peor ───
  if (partners !== null) {
    maxScore += 30;
    const found = partners.filter(p => p.found).length;
    if      (found === 0) score += 30;
    else if (found === 1) score += 22;
    else if (found === 2) score += 14;
    else if (found === 3) score +=  6;
    // 4+ partners = completamente saturado → 0 pts
  }

  // ── Email encontrado (0-20 pts) ─────────────────────────────
  if (emailFound !== null) {
    maxScore += 20;
    if (emailFound) score += 20;
  }

  const pct = maxScore > 0 ? (score / maxScore) * 100 : 0;

  if      (pct >= 75) return { grade: "A", color: "#16a34a", label: "Muy bueno" };
  else if (pct >= 55) return { grade: "B", color: "#ca8a04", label: "Bueno"     };
  else if (pct >= 35) return { grade: "C", color: "#ea580c", label: "Regular"   };
  else                return { grade: "D", color: "#dc2626", label: "Bajo"       };
}
