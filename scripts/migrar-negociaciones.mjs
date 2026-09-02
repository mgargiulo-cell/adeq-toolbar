#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════════════
// MIGRAR LOS BOARDS DE NEGOCIACIONES AL CRM (Maxi 2026-09-02, pedido del user)
// ═══════════════════════════════════════════════════════════════════════════════════════
//   node scripts/migrar-negociaciones.mjs              → SECO: dice qué haría, no toca nada
//   node scripts/migrar-negociaciones.mjs --aplicar    → escribe
//
// EL MODELO, dictado por el user:
//   "dejamos urls únicas en todo el crm, y directamente de prospect adeq van a
//    negociaciones de cada media buyer, y de ahí al darlas por finalizada vuelven al
//    board principal"  +  "luego de 60 días se autopase a ciclo finalizado".
//
// Eso cambia la migración de raíz y para mejor: un dominio es UNA fila y el board dice
// DÓNDE ESTÁ, no una copia. El índice único global `crm_board_prospects_domain_key` deja de
// ser un obstáculo y pasa a ser la regla — no se toca el esquema.
//
// Sin esa regla la migración era imposible: de los 931 dominios distintos de negociaciones,
// 865 YA existen en Prospectos ADEQ. Insertarlos habría reventado por clave duplicada, y
// "arreglarlo" con un upsert habría PISADO las filas de la madre.
//
// ⚠️ LOS 53 "NEGOCIOS SATISFACTORIOS" NO SON DESCARTES: son negocios GANADOS, o sea
// clientes. Mandarlos a "Descartado" y dejar que a los 60 días vuelvan a "Ciclo Finalizado"
// haría que el agente le mande captación en frío a nuestros propios clientes. Van a
// `crm_board_clientes_activos`, que el motor ya respeta ("nunca reciben captación").
//
// NO TOCA MONDAY. Solo lee el export. Monday queda intacto como respaldo.
import fs from "fs";

const APLICAR = process.argv.includes("--aplicar");
const U = process.env.DASH_URL, K = process.env.DASH_KEY;
if (!U || !K) { console.error("faltan DASH_URL / DASH_KEY"); process.exit(1); }

const H = { apikey: K, Authorization: `Bearer ${K}`, "Content-Type": "application/json" };
const api = async (p, opt = {}) => {
  const r = await fetch(`${U}/rest/v1/${p}`, { ...opt, headers: { ...H, ...(opt.headers || {}) } });
  if (!r.ok) throw new Error(`${p} → ${r.status} ${(await r.text()).slice(0, 200)}`);
  // Con `Prefer: return=minimal` PostgREST contesta 201/204 SIN cuerpo. Pedirle .json() a
  // eso tira "Unexpected end of JSON input" y hace parecer fallada una escritura que salió
  // bien — 25 fichas y los 53 clientes se reportaron como error habiéndose guardado.
  const cuerpo = await r.text();
  return cuerpo ? JSON.parse(cuerpo) : null;
};

// ── Qué board de Monday va a dónde ─────────────────────────────────────────────────────
// ACTIVOS: el prospecto está EN negociación → va al tablero de su media buyer.
// FINALIZADOS: el negocio terminó → vuelve al board principal (regla del user).
const ACTIVOS = {
  1430281763: "max", 5095078896: "max", 1430286025: "max",
  5095078739: "agus", 5093819361: "agus",
  5093819057: "die",
  1490667286: "mica", 1490667289: "mica", 1605415147: "mica",
};
const FINALIZADOS = new Set(["5093819630", "5093819426", "5093816946", "1490667287", "1439129161"]);
// Plantillas de Monday con datos de mentira (google.com, juan.com, luis.com, "Tarea 2").
// Migrarlas metería prospectos falsos en el CRM.
const PLANTILLAS = new Set(["5093818887", "5093816901", "5093812693", "5093812695"]);

const MB = {
  max:  { nombre: "Negociaciones Max",  slug: "oportunidades-max",  email: "mgargiulo@adeqmedia.com", color: "blue" },
  agus: { nombre: "Negociaciones Agus", slug: "oportunidades-agus", email: "sales@adeqmedia.com",     color: "purple" },
  die:  { nombre: "Negociaciones Die",  slug: "oportunidades-die",  email: "dhorovitz@adeqmedia.com", color: "orange" },
  mica: { nombre: "Negociaciones Mica", slug: "oportunidades-mica", email: "",                        color: "pink" },
};

const canon = (s) => String(s || "").trim().toLowerCase()
  .replace(/^https?:\/\//, "").replace(/^www\./, "").split("/")[0].trim();
const esDominio = (s) => /^[a-z0-9][a-z0-9.\-]*\.[a-z]{2,}$/.test(s);
const iso = (v) => (/^\d{4}-\d{2}-\d{2}$/.test(String(v || "").trim()) ? String(v).trim() : null);

const dump = JSON.parse(fs.readFileSync("/tmp/monday_negociaciones.json", "utf8"));
const porId = Object.fromEntries(dump.boards.map(b => [String(b.id), b]));

// El valor de una columna por TÍTULO. Mapear por título y no por id es lo que permite que
// las tres familias de columnas distintas (las viejas de Negociaciones, las nuevas de
// Negocios Finalizados y las de Llamados) se lean con el mismo código.
const val = (item, ...titulos) => {
  for (const t of titulos) {
    const c = item._cols[t.toLowerCase()];
    if (c && String(c.text || "").trim()) return String(c.text).trim();
  }
  return "";
};

// ── Se junta todo en un solo registro por dominio ───────────────────────────────────────
// Un mismo sitio aparece 392 veces en más de un tablero de Monday (pasó por negociación y
// después se cerró). Con URLs únicas hay que elegir UNO, y el criterio es el TIEMPO: manda
// la última actividad. Si el último movimiento fue cerrarlo, está cerrado; si volvió a
// negociación después, está en negociación.
const porDominio = new Map();
let saltados = { plantilla: 0, sinDominio: 0, subitems: 0 };

for (const b of dump.boards) {
  const bid = String(b.id);
  if (PLANTILLAS.has(bid)) { saltados.plantilla += b.items.length; continue; }
  const destino = ACTIVOS[bid] ? "activo" : FINALIZADOS.has(bid) ? "finalizado" : null;
  if (!destino) { saltados.subitems += b.items.length; continue; }

  for (const it of b.items) {
    const dom = canon(it.name);
    if (!esDominio(dom)) { saltados.sinDominio++; continue; }
    it._cols = {};
    for (const c of it.column_values) {
      const t = (b.columns.find(x => x.id === c.id)?.title || "").toLowerCase();
      if (t) it._cols[t] = c;
    }
    const grupo = (it.group || {}).title || "";
    const cuando = it.updated_at || it.created_at || "1970-01-01";
    const reg = {
      dominio: dom, destino, mb: ACTIVOS[bid] || null, grupo, cuando,
      boardMonday: b.name, mondayId: it.id,
      ganado: /satisfactorio/i.test(grupo),
      estado: val(it, "Estado"),
      ejecutivo: val(it, "Ejecutivo", "Media Buyer", "Persona"),
      email: val(it, "Email"),
      email2: val(it, "Email Secundario"),
      fechaContacto: iso(val(it, "Fecha Contacto", "Fecha", "Fecha Respuesta")),
      fu1: iso(val(it, "Fecha FU1")), fu2: iso(val(it, "Fecha FU2")),
      geo: val(it, "Top Geo", "Geo", "Pais"),
      pv: val(it, "Paginas Vistas", "Trafico"),
      idioma: val(it, "Idioma"),
      tel: val(it, "Teléfono", "Telefono"),
      comentarios: val(it, "Comentarios"),
      plataforma: val(it, "Plataforma"),
      followup: val(it, "Follow Up"),
      resumen: val(it, "Resumen Emails", "Extract info"),
    };
    const prev = porDominio.get(dom);
    // Gana el más reciente. Pero "ganado" no lo pisa nadie: que un negocio se haya cerrado
    // bien es un hecho que no se borra porque después alguien tocó otra ficha.
    if (!prev || (prev.cuando < reg.cuando && !prev.ganado) || (reg.ganado && !prev.ganado)) {
      porDominio.set(dom, prev ? { ...reg, tambienEn: [...(prev.tambienEn || []), prev.boardMonday] } : reg);
    } else {
      prev.tambienEn = [...(prev.tambienEn || []), b.name];
    }
  }
}

const regs = [...porDominio.values()];
const activos = regs.filter(r => r.destino === "activo");
const finalizados = regs.filter(r => r.destino === "finalizado" && !r.ganado);
const ganados = regs.filter(r => r.ganado);

// ── Los 60 días aplicados a la historia ────────────────────────────────────────────────
// La regla del user es "60 días de descanso y después se vuelve a intentar". Estos
// descartes NO son de hoy: 870 de 1.075 cerraron hace más de 60 días (hay de 2024). Marcar
// a todos "Descartado" para que el cron los dé vuelta mañana sería 870 filas parpadeando
// por nada. Se les aplica la regla con la fecha que YA tienen: el que ya cumplió el
// descanso entra directo a Ciclo Finalizado (prospectable de nuevo), y sólo el que está
// dentro de la ventana queda Descartado con su reloj corriendo.
const DIAS_DESCANSO = 60;
const diasDesde = (iso) => Math.floor((Date.now() - Date.parse(iso)) / 86400000);
for (const r of finalizados) r.yaDescansó = diasDesde(r.cuando) >= DIAS_DESCANSO;
const reabiertos = finalizados.filter(r => r.yaDescansó);
const enDescanso = finalizados.filter(r => !r.yaDescansó);

console.log(`\n${"═".repeat(78)}\nPLAN  ${APLICAR ? "— APLICANDO" : "— SECO, no escribe nada"}\n${"═".repeat(78)}`);
console.log(`dominios únicos a migrar: ${regs.length}`);
console.log(`  · en negociación → al tablero de su MB : ${activos.length}`);
for (const k of Object.keys(MB)) {
  const n = activos.filter(r => r.mb === k).length;
  if (n) console.log(`        ${MB[k].nombre.padEnd(22)} ${n}`);
}
console.log(`  · finalizados → vuelven al principal   : ${finalizados.length}`);
console.log(`        ya cumplieron los 60 días → Ciclo Finalizado : ${reabiertos.length}  (vuelven a ser prospectables)`);
console.log(`        dentro de los 60 días     → Descartado       : ${enDescanso.length}  (el cron los libera al cumplirse)`);
console.log(`  · GANADOS → lista de clientes          : ${ganados.length}  (nunca reciben captación)`);
console.log(`saltados: ${saltados.plantilla} de plantillas Monday · ${saltados.sinDominio} sin dominio válido · ${saltados.subitems} de tableros de subelementos`);

if (!APLICAR) {
  console.log(`\nejemplos en negociación:`);
  for (const r of activos.slice(0, 5)) console.log(`   ${r.dominio.padEnd(30)} ${MB[r.mb].nombre.padEnd(20)} grupo «${r.grupo}»`);
  console.log(`\nejemplos que vuelven como Descartado:`);
  for (const r of finalizados.slice(0, 5)) console.log(`   ${r.dominio.padEnd(30)} desde «${r.boardMonday}»  últ. act. ${r.cuando.slice(0,10)}`);
  console.log(`\nclientes (ganados):`);
  for (const r of ganados.slice(0, 5)) console.log(`   ${r.dominio.padEnd(30)} desde «${r.boardMonday}»`);
  console.log(`\n→ con --aplicar se escribe.\n`);
  process.exit(0);
}

// ── 1) Tableros ────────────────────────────────────────────────────────────────────────
// Los grupos se toman TAL CUAL de Monday, con el nombre exacto y en el orden en que están
// ahí. Es lo que el user pidió: los MB abren el tablero y ven lo mismo que veían.
const gruposDe = (mbKey) => {
  const vistos = [];
  for (const [bid, k] of Object.entries(ACTIVOS)) {
    if (k !== mbKey) continue;
    for (const g of (porId[bid]?.groups || [])) if (!vistos.includes(g.title)) vistos.push(g.title);
  }
  return vistos;
};

const existentes = await api("crm_boards?select=id,name,slug,position");
const idPorSlug = Object.fromEntries(existentes.map(b => [b.slug, b.id]));
const boardPrincipal = existentes.find(b => b.slug === "prospectos")?.id;
if (!boardPrincipal) throw new Error("no encuentro el board principal (slug 'prospectos')");

const boardIdDe = {};
let pos = Math.max(...existentes.map(b => b.position || 0)) + 1;
for (const [k, m] of Object.entries(MB)) {
  const grupos = gruposDe(k);
  const cfg = {
    order: ["domain","estado","ejecutivo","grupo","fecha_contacto","email","top_geo","pageviews","idioma","telefono","comentarios"],
    groupBy: "grupo", custom: [],
    options: { grupo: grupos.map(g => ({ name: g, color: "#579bfc" })) },
  };
  if (idPorSlug[m.slug]) {
    boardIdDe[k] = idPorSlug[m.slug];
    await api(`crm_boards?id=eq.${boardIdDe[k]}`, { method: "PATCH", headers: { Prefer: "return=minimal" },
      body: JSON.stringify({ name: m.nombre, column_config: cfg }) });
    console.log(`↻ tablero actualizado: ${m.nombre}  (grupos: ${grupos.join(" · ")})`);
  } else {
    // El tablero vacío "NEGOCIACIONES MAXI" ya existía y nada en el código lo referencia:
    // se reusa para Max en vez de dejar dos tableros para lo mismo.
    const viejo = existentes.find(b => b.slug === "negociaciones-maxi");
    if (k === "max" && viejo) {
      boardIdDe[k] = viejo.id;
      await api(`crm_boards?id=eq.${viejo.id}`, { method: "PATCH", headers: { Prefer: "return=minimal" },
        body: JSON.stringify({ name: m.nombre, slug: m.slug, column_config: cfg }) });
      console.log(`↻ reusado el tablero vacío "NEGOCIACIONES MAXI" → ${m.nombre}`);
    } else {
      const [nuevo] = await api("crm_boards", { method: "POST", headers: { Prefer: "return=representation" },
        body: JSON.stringify({ name: m.nombre, slug: m.slug, color: m.color, position: pos++, is_default: false, column_config: cfg }) });
      boardIdDe[k] = nuevo.id;
      console.log(`✚ tablero creado: ${m.nombre}  (grupos: ${grupos.join(" · ")})`);
    }
  }
}

// ── 2) Las fichas ──────────────────────────────────────────────────────────────────────
const yaEn = new Map();
for (let off = 0; ; off += 1000) {
  const p = await api(`crm_board_prospects?select=id,domain,estado,board_id&limit=1000&offset=${off}`);
  if (!p.length) break;
  for (const r of p) yaEn.set(String(r.domain).toLowerCase(), r);
}
console.log(`\nfichas ya en el CRM: ${yaEn.size}`);

const hoy = new Date().toISOString().slice(0, 10);
let act = 0, cre = 0, err = [];
// De a uno son 928 requests en serie y tarda más de lo que dura cualquier terminal. De a 20
// termina en menos de un minuto. Es idempotente: si se corta, se vuelve a correr y las que
// ya estaban se re-leen del CRM y se actualizan igual.
const enTandas = async (lista, n, fn) => {
  for (let i = 0; i < lista.length; i += n) await Promise.all(lista.slice(i, i + n).map(fn));
};
await enTandas(regs, 20, async (r) => {
  if (r.ganado) return;                   // los ganados se tratan aparte, no como fichas de captación
  const esActivo = r.destino === "activo";
  const patch = {
    board_id: esActivo ? boardIdDe[r.mb] : boardPrincipal,
    grupo: esActivo ? r.grupo : "Prospectos Vigentes",
    estado: esActivo ? (r.estado || "En Negociacion")
                     : (r.yaDescansó ? "Ciclo Finalizado" : "Descartado"),
  };
  // La fecha se anota SIEMPRE, también en los ya reabiertos: es el registro de cuándo se
  // cerró el negocio y sin ella no hay forma de auditar por qué quedó prospectable.
  if (!esActivo) patch.descartado_at = (r.cuando || hoy).slice(0, 10);
  // Los datos del prospecto sólo se escriben si Monday los tiene: nunca se pisa con vacío
  // lo que la ficha del CRM ya sabe.
  const opt = (k, v) => { if (v) patch[k] = v; };
  opt("email", r.email); opt("email_secondary", r.email2);
  opt("ejecutivo", r.ejecutivo.includes("@") ? r.ejecutivo : (MB[r.mb]?.email || ""));
  opt("fecha_contacto", r.fechaContacto); opt("fecha_fu1", r.fu1); opt("fecha_fu2", r.fu2);
  opt("top_geo", r.geo); opt("pageviews", r.pv); opt("idioma", r.idioma);
  opt("telefono", r.tel); opt("comentarios", r.comentarios);
  patch.extra = { monday_board: r.boardMonday, monday_item_id: r.mondayId,
                  ...(r.plataforma && { c_plataforma: r.plataforma }),
                  ...(r.followup && { c_follow_up: r.followup }),
                  ...(r.resumen && { c_resumen_emails: r.resumen }),
                  ...(r.tambienEn?.length && { c_tambien_estuvo_en: r.tambienEn.join(" · ") }) };
  try {
    const ex = yaEn.get(r.dominio);
    if (ex) { await api(`crm_board_prospects?id=eq.${ex.id}`, { method: "PATCH", headers: { Prefer: "return=minimal" }, body: JSON.stringify(patch) }); act++; }
    else    { await api("crm_board_prospects", { method: "POST", headers: { Prefer: "return=minimal" }, body: JSON.stringify({ domain: r.dominio, source: "monday-negociaciones", ...patch }) }); cre++; }
  } catch (e) { err.push(`${r.dominio}: ${String(e.message).slice(0, 90)}`); }
});
console.log(`fichas actualizadas: ${act} · creadas: ${cre}`);

// ── 3) Los negocios GANADOS ────────────────────────────────────────────────────────────
// ⚠️ `crm_board_clientes_activos` es una VISTA calculada (facturación de 90 días +
// sitios_config activos), no una tabla: no se puede insertar y no se debe: inventar un
// cliente ahí sería mentirle al dato que decide a quién NO escribirle.
// De los 53: 23 ya figuran activos (la vista los bloquea sola), 20 fueron clientes y se
// dieron de baja, 10 nunca llegaron a estar de alta. Los últimos 30 son una decisión de
// negocio (¿se les vuelve a escribir, y con qué mensaje?), no algo que deba resolver una
// migración. Se deja la marca en la ficha para que en el tablero se vea qué son, y se
// informa el número.
let cl = 0;
await enTandas(ganados, 20, async (r) => {
  try {
    const ex = yaEn.get(r.dominio);
    const marca = { c_negocio_ganado: `${r.boardMonday} · ${String(r.cuando).slice(0, 10)}`,
                    monday_board: r.boardMonday, monday_item_id: r.mondayId };
    if (ex) await api(`crm_board_prospects?id=eq.${ex.id}`, { method: "PATCH",
      headers: { Prefer: "return=minimal" }, body: JSON.stringify({ extra: marca }) });
    else    await api("crm_board_prospects", { method: "POST", headers: { Prefer: "return=minimal" },
      body: JSON.stringify({ domain: r.dominio, board_id: boardPrincipal, grupo: "Prospectos Vigentes",
                             source: "monday-negociaciones", extra: marca }) });
    cl++;
  } catch (e) { err.push(`ganado ${r.dominio}: ${String(e.message).slice(0, 80)}`); }
});
console.log(`negocios ganados marcados en su ficha: ${cl}`);
// Los errores se imprimen ACÁ, después de todo. Antes se imprimían a mitad de camino y los
// 53 fallos de los ganados no se veían: el script decía "0" y seguía como si nada.
if (err.length) { console.log(`\n❌ ${err.length} errores:`); err.slice(0, 12).forEach(e => console.log(`   ${e}`)); }
console.log(`\n✅ listo. Monday quedó intacto.\n`);
