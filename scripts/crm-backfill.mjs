#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════════════
// BACKFILL DEL CRM PROPIO — se corre A MANO, nunca desde el worker
// ═══════════════════════════════════════════════════════════════════════════════════════
//
//   node scripts/crm-backfill.mjs                 → SECO: muestra qué mandaría, no manda nada
//   node scripts/crm-backfill.mjs --aplicar       → manda de verdad
//   node scripts/crm-backfill.mjs --limite 50     → probar con pocos primero
//
// Variables necesarias:
//   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY   (base de la toolbar)
//   CRM_SYNC_SECRET                            (secreto del endpoint del CRM)
//
// ⚠️ EL PELIGRO DE ESTE SCRIPT, LEER ANTES DE CORRERLO CON --aplicar
// Un prospecto contactado hace tres semanas tiene su FU1 y su FU2 EN EL PASADO. Si se cargan
// esas fechas tal cual, la primera corrida de automatizaciones del CRM las ve vencidas y
// dispara el follow-up de golpe: 2.000 mails a gente que ya recibió su secuencia entera.
// Sería el peor error posible — irreversible, hacia afuera, y sobre la reputación de las
// cuentas que venimos cuidando.
// Por eso este script MANDA LAS FECHAS DE FOLLOW-UP SOLO SI TODAVÍA NO PASARON. Las vencidas
// van en null: ese prospecto ya cumplió su ciclo y no le corresponde ningún toque nuevo.
// No sacar esta regla sin apagar antes las automatizaciones del CRM.

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CRM_URL      = process.env.CRM_SYNC_URL || "https://console.adeqmedia.com/api/crm/sync-toolbar";
const CRM_SECRET   = process.env.CRM_SYNC_SECRET || "";

const args    = process.argv.slice(2);
const APLICAR = args.includes("--aplicar");
const LIMITE  = parseInt((args.find(a => a.startsWith("--limite")) || "").split(/[= ]/)[1] || "0", 10)
                || parseInt(args[args.indexOf("--limite") + 1] || "0", 10) || 0;

if (!SUPABASE_URL || !SERVICE_KEY) { console.error("Faltan SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY"); process.exit(1); }
if (APLICAR && !CRM_SECRET)        { console.error("Falta CRM_SYNC_SECRET y se pidió --aplicar"); process.exit(1); }

const HOY = new Date().toISOString().split("T")[0];

// Las mismas conversiones que usa el worker. Si alguna vez divergen, las dos columnas dejan
// de ser comparables y la verificación en paralelo de la fase 5 pierde sentido.
const IDIOMA = { en: "Ingles", es: "Español", it: "Italiano", pt: "Portugues", ar: "Árabe" };
const idioma = (l) => IDIOMA[String(l || "").toLowerCase()] || "Ingles";

// El worker manda el GEO en español sin tildes (`normalizeMondayGeo`). Si acá se mandara el
// nombre en inglés, el mismo país entraría con dos escrituras distintas —"Brazil" del backfill
// y "Brasil" del feed— y cualquier filtro o agrupado por país quedaría partido en dos.
const GEO_ES = {
  Brazil:"Brasil", Spain:"España", Mexico:"Mexico", Argentina:"Argentina", Colombia:"Colombia",
  Chile:"Chile", Peru:"Peru", Uruguay:"Uruguay", Ecuador:"Ecuador", Venezuela:"Venezuela",
  Bolivia:"Bolivia", Paraguay:"Paraguay", "Costa Rica":"Costa Rica", Panama:"Panama",
  Guatemala:"Guatemala", Honduras:"Honduras", Nicaragua:"Nicaragua", "El Salvador":"El Salvador",
  "Dominican Republic":"Republica Dominicana", "Puerto Rico":"Puerto Rico", Cuba:"Cuba",
  Portugal:"Portugal", Italy:"Italia", France:"Francia", Germany:"Alemania", Greece:"Grecia",
  Poland:"Polonia", Hungary:"Hungria", Turkey:"Turquia", Romania:"Rumania", Netherlands:"Paises Bajos",
  Belgium:"Belgica", Sweden:"Suecia", Norway:"Noruega", Denmark:"Dinamarca", Finland:"Finlandia",
  Switzerland:"Suiza", Austria:"Austria", "Czech Republic":"Republica Checa", Bulgaria:"Bulgaria",
  Serbia:"Serbia", Croatia:"Croacia", Ukraine:"Ucrania", Russia:"Rusia", India:"India",
  Indonesia:"Indonesia", Japan:"Japon", China:"China", "South Korea":"Corea del Sur",
  Thailand:"Tailandia", Vietnam:"Vietnam", Malaysia:"Malasia", Philippines:"Filipinas",
  Egypt:"Egipto", Morocco:"Marruecos", Nigeria:"Nigeria", "South Africa":"Sudafrica",
  "United States":"Estados Unidos", "United Kingdom":"Reino Unido", Canada:"Canada",
  Australia:"Australia", Israel:"Israel", "Saudi Arabia":"Arabia Saudita",
  "United Arab Emirates":"Emiratos Arabes Unidos",
};
const geoEs = (g) => GEO_ES[String(g || "").trim()] || String(g || "").trim();

function trafico(n) {
  const v = Number(n) || 0;
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
  if (v >= 1_000)     return `${Math.round(v / 1_000)}K`;
  return String(v);
}
const masDias = (iso, d) => {
  if (!iso) return null;
  const t = Date.parse(iso); if (!Number.isFinite(t)) return null;
  return new Date(t + d * 86_400_000).toISOString().split("T")[0];
};
// La regla de arriba, en una línea: una fecha ya vencida no se manda.
const soloFuturo = (f) => (f && f >= HOY ? f : null);

async function leerTodos() {
  const filas = [];
  const auth = { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` };
  for (let desde = 0; ; desde += 1000) {
    const url = `${SUPABASE_URL}/rest/v1/toolbar_review_queue`
      // ⚠️ NO alcanza con "tiene item de Monday" (Maxi 2026-08-31). De los 2.491 que lo tienen,
      // 187 fueron RECHAZADOS después, 255 están congelados y 653 siguen pendientes: cargarlos
      // metería descartes al board como propuestas vigentes. Los realmente contactados son los
      // 1.396 `validated`, y son los únicos que le corresponde ver al CRM.
      + `?monday_item_id=not.is.null&status=eq.validated`
      + `&select=domain,emails,geo,traffic,language,contact_phone,created_by,validated_at,created_at`
      + `&order=created_at.asc&limit=1000&offset=${desde}`;
    const r = await fetch(url, { headers: auth });
    if (!r.ok) throw new Error(`Supabase HTTP ${r.status}`);
    const lote = await r.json();
    filas.push(...lote);
    if (lote.length < 1000) break;
    if (LIMITE && filas.length >= LIMITE) break;
  }
  return LIMITE ? filas.slice(0, LIMITE) : filas;
}

function aProspecto(f) {
  const contacto = (f.validated_at || f.created_at || "").split("T")[0] || null;
  const emails = Array.isArray(f.emails) ? f.emails : [];
  const email = typeof emails[0] === "string" ? emails[0] : (emails[0]?.email || "");
  return {
    domain: f.domain,
    email,
    // ⚠️ EL ESTADO ES LO ÚNICO QUE ESTE SCRIPT NO PUEDE SABER.
    // La toolbar registra que empujó el prospecto a Monday, pero no qué pasó DESPUÉS: si el MB
    // lo movió a "Ciclo Finalizado" o "En Negociacion", eso vive en Monday y nunca vuelve.
    // Cargar 1.396 prospectos como vigentes cuando muchos están cerrados llenaría el board de
    // pipeline falso.
    // POR ESO ESTE BACKFILL ES EL PLAN B, no el principal: `crm_prospects` del dashboard ya
    // tiene 1.994 filas con el deal_stage REAL sincronizado desde Monday. Sembrar
    // `crm_board_prospects` desde ahí es una copia entre tablas de la misma base — exacta, sin
    // HTTP y sin adivinar. Este script sirve para el hueco: lo que la toolbar contactó y nunca
    // llegó a `crm_prospects`.
    deal_stage: process.env.CRM_BACKFILL_STAGE || "Propuesta Vigente (T)",
    ejecutivo_name: f.created_by || "",
    fecha_contacto: contacto,
    fecha_fu1: soloFuturo(masDias(contacto, 5)),
    fecha_fu2: soloFuturo(masDias(contacto, 10)),
    top_geo: geoEs(f.geo),
    pageviews: trafico(f.traffic),
    language: idioma(f.language),
    phone: f.contact_phone || "",
    comments: "Agente",
    source: "toolbar-backfill",   // separado de "toolbar" para poder aislarlo si sale mal
  };
}

(async () => {
  const filas = await leerTodos();
  const prospectos = filas.map(aProspecto).filter(p => p.domain);

  const conFu = prospectos.filter(p => p.fecha_fu1 || p.fecha_fu2).length;
  console.log(`\n  ${prospectos.length} prospectos con item de Monday`);
  console.log(`  ${conFu} con follow-up todavía pendiente · ${prospectos.length - conFu} con el ciclo cumplido (van sin fechas)`);
  console.log(`  destino: ${CRM_URL}\n`);

  if (!APLICAR) {
    console.log("  MODO SECO — no se mandó nada. Muestra de 3:\n");
    console.log(JSON.stringify(prospectos.slice(0, 3), null, 2));
    console.log(`\n  Para mandarlo de verdad: node scripts/crm-backfill.mjs --aplicar\n`);
    return;
  }

  let ok = 0, err = 0;
  for (let i = 0; i < prospectos.length; i += 100) {
    const lote = prospectos.slice(i, i + 100);
    const r = await fetch(CRM_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Toolbar-Secret": CRM_SECRET },
      body: JSON.stringify({ prospects: lote }),
    });
    const j = await r.json().catch(() => null);
    if (!r.ok || !j) { console.log(`  lote ${i}-${i + lote.length}: HTTP ${r.status}`); err += lote.length; continue; }
    ok += j.ok || 0;
    for (const e of (j.errores || [])) { err++; console.log(`  🔴 ${e.domain || "(sin dominio)"}: ${e.motivo}`); }
    for (const a of (j.avisos  || []))          console.log(`  🟡 ${a.domain}: ${a.motivo}`);
    console.log(`  lote ${i}-${i + lote.length}: ${j.ok} ok`);
  }
  console.log(`\n  TOTAL: ${ok} entraron · ${err} fallaron\n`);
})().catch(e => { console.error("Falló:", e.message); process.exit(1); });
