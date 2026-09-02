#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════════════
// MIGRAR LOS ACTIVOS DE MONDAY AL CRM BOARD (Maxi 2026-09-02)
// ═══════════════════════════════════════════════════════════════════════════════════════
//   node scripts/migrar-monday-al-board.mjs              → SECO, no manda nada
//   node scripts/migrar-monday-al-board.mjs --aplicar    → manda de verdad
//
// Migra SOLO los activos (3.362 de 10.562). Los 7.200 "Ciclo Finalizado" no van a propósito:
// en la lógica de la toolbar un ciclo cerrado vuelve a ser prospectable, así que copiarlos
// bloquearía leads que hoy están disponibles.
//
// NO TOCA MONDAY. Solo lee. El board queda con una copia; Monday se conserva intacto.
import fs from "fs";

const APLICAR = process.argv.includes("--aplicar");
const SECRET  = process.env.CRM_SYNC_SECRET || "";
const URL     = "https://console.adeqmedia.com/api/crm/sync-toolbar";
const items   = JSON.parse(fs.readFileSync("/tmp/monday_items.json", "utf8"));

const col = (it, cid) => (it.column_values.find(c => c.id === cid)?.text || "").trim();

// El board pide EMAIL en ejecutivo_name: de ahí sale de qué casilla se manda el follow-up.
// Monday devuelve email para Agus y Diego, pero "Maximiliano Gargiulo" como nombre (557 items).
// Si vienen dos dueños se toma el primero: la cadencia sale de una sola casilla.
const EJEC = { "Maximiliano Gargiulo": "mgargiulo@adeqmedia.com" };
function ejecutivo(v) {
  const primero = String(v || "").split(",")[0].trim();
  if (!primero) return "";
  return primero.includes("@") ? primero : (EJEC[primero] || "");
}

// Monday da las fechas ya en ISO. Se valida igual: una fecha rota haría que el follow-up
// salga cuando no corresponde, y eso es peor que no tener fecha.
const fecha = (v) => (/^\d{4}-\d{2}-\d{2}$/.test(String(v || "").trim()) ? v.trim() : null);

// Mismo criterio que el emisor: solo un número que pueda ser E.164 de verdad.
function telefono(t) {
  const s = String(t || "").trim();
  if (!s.startsWith("+")) return "";
  const d = s.replace(/[^\d]/g, "");
  return (d.length >= 8 && d.length <= 15) ? s : "";
}

const IDIOMAS = new Set(["Español", "Ingles", "Portugues", "Italiano", "Arabe"]);

// --todos incluye los "Ciclo Finalizado" (regla del user: no se pierde ninguna URL).
// Sin el flag migra solo los activos.
// ⚠️ Los finalizados van con su estado REAL, no disfrazados de activos: en la lógica de la
// toolbar un ciclo cerrado vuelve a ser prospectable, así que el board tiene que poder
// distinguirlos para no bloquear leads que hoy están disponibles.
const TODOS = process.argv.includes("--todos");
const activos = items.filter(i => {
  const e = col(i, "deal_stage");
  if (TODOS) return e !== "";
  return !["Ciclo Finalizado", "Descartado", ""].includes(e);
});

const filas = activos.map(i => {
  const dom = i.name.replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/.*$/, "").toLowerCase().trim();
  const idi = col(i, "estado_12");
  return {
    domain: dom,
    email: col(i, "email_mm2edcd3") || col(i, "text_mkrwahsz") || "",
    deal_stage: col(i, "deal_stage"),
    ejecutivo_name: ejecutivo(col(i, "deal_owner")),
    fecha_contacto: fecha(col(i, "deal_close_date")),
    fecha_fu1: fecha(col(i, "fecha2")),
    fecha_fu2: fecha(col(i, "fecha_1")),
    top_geo: col(i, "texto6"),
    pageviews: col(i, "texto7"),
    language: IDIOMAS.has(idi) ? idi : "Ingles",   // el board avisa igual si no matchea
    phone: telefono(col(i, "tel_fono_1")),
    comments: col(i, "texto"),
    source: col(i, "deal_stage") === "Ciclo Finalizado" ? "monday-historico" : "monday-migracion",
  };
}).filter(f => f.domain && f.domain.includes("."));

console.log(`\n  ${items.length} items en Monday · ${activos.length} activos · ${filas.length} con dominio válido`);
const sinEjec = filas.filter(f => !f.ejecutivo_name).length;
const sinMail = filas.filter(f => !f.email).length;
const conFu   = filas.filter(f => f.fecha_fu1 || f.fecha_fu2).length;
console.log(`  sin ejecutivo: ${sinEjec} · sin email: ${sinMail} · con follow-up: ${conFu}\n`);

if (!APLICAR) {
  console.log("  MODO SECO — no se mandó nada. Muestra:\n");
  console.log(JSON.stringify(filas.slice(0, 2), null, 2));
  console.log(`\n  Para migrar de verdad: CRM_SYNC_SECRET=… node scripts/migrar-monday-al-board.mjs --aplicar\n`);
  process.exit(0);
}
if (!SECRET) { console.error("  Falta CRM_SYNC_SECRET"); process.exit(1); }

let ok = 0; const errores = [], avisos = [];
for (let i = 0; i < filas.length; i += 100) {
  const lote = filas.slice(i, i + 100);
  try {
    const r = await fetch(URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-toolbar-secret": SECRET },
      body: JSON.stringify({ prospects: lote }),
    });
    const j = await r.json().catch(() => null);
    if (!r.ok || !j) { console.log(`  lote ${i}: HTTP ${r.status}`); continue; }
    ok += j.ok || 0;
    (j.errores || []).forEach(e => errores.push(`${e.domain}: ${e.motivo}`));
    (j.avisos  || []).forEach(a => avisos.push(`${a.domain}: ${a.motivo}`));
    console.log(`  lote ${i}-${i + lote.length}: ${j.ok} ok${j.errores?.length ? ` · ${j.errores.length} err` : ""}`);
  } catch (e) { console.log(`  lote ${i}: ${e.message}`); }
}
console.log(`\n  MIGRADOS: ${ok} · errores: ${errores.length} · avisos: ${avisos.length}`);
errores.slice(0, 8).forEach(e => console.log(`   🔴 ${e}`));
[...new Set(avisos.map(a => a.split(":").slice(1).join(":").trim().slice(0, 70)))].slice(0, 5)
  .forEach(a => console.log(`   🟡 ${a}`));
