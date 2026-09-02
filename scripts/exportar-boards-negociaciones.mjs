#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════════════
// COPIA FIEL DE LOS BOARDS DE NEGOCIACIONES (Maxi 2026-09-02, pedido del user)
// ═══════════════════════════════════════════════════════════════════════════════════════
// SOLO LEE. No escribe ni borra nada en Monday — la regla del user es explícita:
// "NO BORRES DE MONDAY NINGÚN ÍTEM SOLO MIGRA DUPLICANDO".
//
// Se guarda TODO tal cual viene: cada board con sus grupos (id y título), cada ítem con el
// grupo al que pertenece, todas las columnas con `text` Y `value` crudo, los subitems y las
// actualizaciones. El `value` crudo importa: `text` es la representación linda y pierde cosas
// (un status guarda el índice, un people guarda los IDs). Si después hace falta un dato que
// hoy no estoy mirando, está en el archivo y no hay que volver a Monday.
//   node scripts/exportar-boards-negociaciones.mjs
import fs from "fs";

const MK = process.env.MONDAY_TOKEN;
if (!MK) { console.error("falta MONDAY_TOKEN"); process.exit(1); }

// Los 18 boards de negociaciones. Prospectos ADEQ (1420268379) NO va: ya está migrado.
const BOARDS = [
  5095078896, 5095078900, 5095078739, 5095078740, 5093819630, 5093819631,
  5093819426, 5093819430, 5093819361, 5093819366, 5093819272, 5093819271,
  5093819096, 5093819094, 5093819061, 5093819057, 5093818924, 5093818922,
  5093818890, 5093818887, 5093816950, 5093816946, 5093816904, 5093816901,
  5093812695, 5093812693, 1605415147, 1528152883, 1510114558, 1490667289,
  1490667287, 1490667286, 1439129161, 1430286025, 1430281763,
];

async function gql(query, variables = {}) {
  for (let intento = 1; intento <= 4; intento++) {
    const r = await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: { Authorization: MK, "Content-Type": "application/json", "API-Version": "2024-10" },
      body: JSON.stringify({ query, variables }),
    });
    const j = await r.json().catch(() => null);
    // Monday tira 429 y "Complexity budget exhausted" seguido. Reintentar con espera es
    // parte del camino normal, no una excepción: sin esto la copia sale incompleta y
    // "incompleta" es justo lo que no se puede permitir acá.
    if (r.ok && j && !j.errors) return j.data;
    const msg = JSON.stringify(j?.errors || j || r.status);
    if (intento === 4) throw new Error(msg.slice(0, 300));
    const espera = /complexity|429|budget/i.test(msg) ? 32000 : 3000 * intento;
    console.log(`   ⏳ reintento ${intento} en ${espera / 1000}s — ${msg.slice(0, 90)}`);
    await new Promise(s => setTimeout(s, espera));
  }
}

const Q_META = `query($id:[ID!]) { boards(ids:$id) {
  id name description state board_kind items_count
  workspace { id name }
  groups { id title color position }
  columns { id title type settings_str }
} }`;

const Q_ITEMS = `query($id:ID!, $cursor:String) {
  boards(ids:[$id]) { items_page(limit:100, cursor:$cursor) {
    cursor
    items {
      id name state created_at updated_at
      group { id title }
      creator { id name email }
      column_values { id type text value }
      subitems { id name column_values { id type text value } }
      updates { id body created_at creator { name email } }
    }
  } }
}`;

const salida = { exportado_el: new Date().toISOString(), boards: [] };

for (const id of BOARDS) {
  const meta = (await gql(Q_META, { id: [String(id)] })).boards[0];
  if (!meta) { console.log(`⚠️ ${id} no accesible`); continue; }
  const items = [];
  let cursor = null;
  do {
    const page = (await gql(Q_ITEMS, { id: String(id), cursor })).boards[0].items_page;
    items.push(...page.items);
    cursor = page.cursor;
  } while (cursor);

  // El contador del board vs lo que realmente bajó. Sólo es un problema en UNA dirección:
  // si bajó MENOS de lo declarado falta algo y hay que saberlo ACÁ, no descubrirlo cuando
  // falte un negocio del otro lado. Bajar de MÁS es el contador de Monday desactualizado
  // (pasa en los boards de subelementos, que declaran 0 y tienen 1) — no se pierde nada.
  const declarados = meta.items_count ?? null;
  const ok = declarados === null || items.length >= declarados;
  const deMas = declarados !== null && items.length > declarados;
  console.log(`${ok ? "✅" : "❌"} ${String(id).padEnd(11)} ${String(items.length).padStart(5)} ítems  ${meta.name}`
    + (deMas ? `  (el contador de Monday dice ${declarados}, está viejo)` : "")
    + (ok ? "" : `  ⚠️ FALTAN: el board declara ${declarados}`));
  salida.boards.push({ ...meta, items, bajados: items.length, declarados, completo: ok });
}

fs.writeFileSync("/tmp/monday_negociaciones.json", JSON.stringify(salida, null, 2));
const tot = salida.boards.reduce((a, b) => a + b.bajados, 0);
const sub = salida.boards.reduce((a, b) => a + b.items.reduce((x, i) => x + (i.subitems?.length || 0), 0), 0);
const upd = salida.boards.reduce((a, b) => a + b.items.reduce((x, i) => x + (i.updates?.length || 0), 0), 0);
const incompletos = salida.boards.filter(b => !b.completo);
console.log(`\n${salida.boards.length} boards · ${tot} ítems · ${sub} subitems · ${upd} updates → /tmp/monday_negociaciones.json`);
if (incompletos.length) { console.log(`❌ FALTAN ÍTEMS EN: ${incompletos.map(b => `${b.name} (${b.bajados}/${b.declarados})`).join(", ")}`); process.exit(1); }
console.log("✅ todos los boards bajaron completos");
