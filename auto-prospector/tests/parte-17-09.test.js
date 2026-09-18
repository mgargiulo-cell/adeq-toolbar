// Los mails del 16 y 17/09, revisados contra la base: cuatro procesos trabados o que se acusaban solos.
//
//  1. RapidAPI renovó el 18 y el contador NO se reinició: el feeder anotó "pacing — used 40% vs cycle
//     1%" con 15.914 llamadas que eran del ciclo anterior. El RPC guardaba mes calendario ("2026-09") y
//     el worker comparaba por YYYY-MM contra un ciclo que va del 18 al 18: las dos mitades del mes
//     salían mal (0 usados del 1 al 17; el gasto viejo contado como nuevo del 18 en adelante).
//  2. "COLA — 33 en error: freeze_failed … HTTP 409", todos los días: `merge-duplicates` sin
//     `on_conflict` resuelve contra la clave primaria (`id`), no contra `domain`. Re-congelar un dominio
//     que ya tenía fila chocaba siempre. La misma trampa estaba en el import de CSV de la extensión
//     (un repetido tiraba el lote entero de 500, en silencio) y en guardar un borrador.
//  3. "🛑 2 fuentes sin producir" todos los días desde el 09/09: adstxt, sellers y majestic sólo las
//     inyecta el feeder, y el feeder se saltea a propósito con Prospects lleno (4.314 ≥ 3.000).
//  4. El linter frenó tres veces el resumen de salud del dueño y dos veces un pitch a xsport.ua por
//     "homóglifos": miraba el mail entero, no la palabra.
//
// NO se tocó: `rol_mx` rebotó 23 de 46 (50%), pero el test C36 de reintento_rebote-13-09b dice que
// cambiar cómo sale es decisión del dueño. Quedó planteado, no hecho.
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { revisarEntregabilidad } from "../lib/email.js";

const aqui    = path.dirname(fileURLToPath(import.meta.url));
const raiz    = path.join(aqui, "..", "..");
const indexJs = fs.readFileSync(path.join(aqui, "..", "index.js"), "utf8");

const W = await cargarWorker(["_mismoCicloRapidApi", "_frenoDelFeeder", "_billingCyclePeriod"]);

// ── 1. El ciclo de RapidAPI ───────────────────────────────────────────────────────────────
test("el contador de RapidAPI sólo vale si es de ESTE ciclo, comparando el período entero", () => {
  const f = W._mismoCicloRapidApi;
  strictEqual(f("2026-09-18", "2026-09-18"), true);
  strictEqual(f("2026-09", "2026-09-18"), false, "el mes calendario viejo no es de ningún ciclo: se lee 0, no 15.914");
  strictEqual(f("2026-08-18", "2026-09-18"), false, "el ciclo anterior no se arrastra");
  strictEqual(f("2026-09-18", "2026-08-18"), false);
  strictEqual(f("", "2026-09-18"), false);
  ok(/^\d{4}-\d{2}-\d{2}$/.test(W._billingCyclePeriod()), "el período del worker es el inicio del ciclo, con día");
});

test("los dos lectores del worker usan esa regla, no el YYYY-MM", () => {
  ok(!/storedPer\.slice\(0, 7\) === period\.slice\(0, 7\)/.test(indexJs), "volvió la comparación por mes calendario");
  strictEqual((indexJs.match(/_mismoCicloRapidApi\(storedPer, period\)/g) || []).length, 2, "la leen getRapidApiUsageThisMonth y saveRapidApiMonthlyUsage");
});

test("el RPC de la base y el worker renuevan RapidAPI el MISMO día", () => {
  const sql = fs.readFileSync(path.join(raiz, "sql", "2026-09-18_bump_api_counter_ciclo_rapidapi.sql"), "utf8");
  const rama = sql.slice(sql.indexOf("WHEN provider = 'rapidapi'"), sql.indexOf("ELSE to_char(hoy, 'YYYY-MM')"));
  const diaSql = Number(/extract\(day from hoy\) >= (\d+)/.exec(rama)?.[1]);
  const masDias = [...rama.matchAll(/interval '(\d+) days'/g)].map(m => Number(m[1]));
  const diaJs = Number(/const RAPIDAPI_CYCLE_ANCHOR_DAY = (\d+);/.exec(indexJs)?.[1]);
  ok(diaJs > 0, "no encontré RAPIDAPI_CYCLE_ANCHOR_DAY");
  strictEqual(diaSql, diaJs, `el RPC renueva el ${diaSql} y el worker el ${diaJs}: si cambia el ancla hay que cambiar los dos`);
  ok(masDias.length === 2 && masDias.every(d => d === diaJs - 1), `el inicio del ciclo en el RPC es "día 1 + ${diaJs - 1}", vino ${masDias}`);
  ok(/provider IN \('apollo', 'rapidapi'\) AND stored_period != cur_period/.test(sql), "RapidAPI compara el período entero, como Apollo");
});

// ── 2. merge-duplicates sin on_conflict ───────────────────────────────────────────────────
// Tablas cuya clave primaria NO es la columna por la que se quiere fusionar (medido en pg_constraint).
const _PK_NO_ES_LA_CLAVE = { toolbar_frozen_leads: "domain", toolbar_csv_queue: "domain", toolbar_pitch_drafts: "user_email,name,language",
  toolbar_review_queue: "domain", toolbar_prospects_offline: "domain", toolbar_user_snoozed_prospects: "user_email,domain" };
test("ningún upsert a una tabla con PK=id va sin on_conflict", () => {
  const malos = [];
  for (const rel of ["auto-prospector/index.js", "modules/supabase.js", "popup/popup.js", "modules/apiProxy.js"]) {
    const L = fs.readFileSync(path.join(raiz, rel), "utf8").split("\n");
    L.forEach((l, i) => {
      const m = /rest\/v1\/(toolbar_[a-z_]+)(\?[^`]*)?`/.exec(l);
      if (!m || !_PK_NO_ES_LA_CLAVE[m[1]]) return;
      const ventana = L.slice(i, i + 9).join("\n");
      if (!/method:\s*"POST"/.test(ventana) || !/merge-duplicates/.test(ventana)) return;
      if (!/on_conflict=/.test(m[2] || "")) malos.push(`${rel}:${i + 1} ${m[1]} (falta ?on_conflict=${_PK_NO_ES_LA_CLAVE[m[1]]})`);
    });
  }
  strictEqual(malos.length, 0, `merge-duplicates resuelve contra la PK (id): sin on_conflict da 409 al repetir.\n  ${malos.join("\n  ")}`);
});

test("los cinco congelados del worker fusionan por dominio", () => {
  strictEqual((indexJs.match(/rest\/v1\/toolbar_frozen_leads\?on_conflict=domain`, \{/g) || []).length, 5);
  strictEqual((indexJs.match(/rest\/v1\/toolbar_frozen_leads`, \{/g) || []).length, 0);
});

// ── 3. El vigilante de fuentes ────────────────────────────────────────────────────────────
const corrida = (status, extra = {}) => ({ slot_label: "2026-09-17-20:00", status, notes: "", rq_valid_before: null, ...extra });
test("el feeder salteado por pool lleno es una pausa, no una fuente muda", () => {
  const v = W._frenoDelFeeder(Array.from({ length: 5 }, () => corrida("skipped_saturated", { rq_valid_before: 4314, notes: "review_queue full" })));
  ok(v.pausa && /4314/.test(v.pausa) && /3000/.test(v.pausa), `tiene que decir el número y el umbral, vino: ${v.pausa}`);
  strictEqual(v.raro, "");
  ok(W._frenoDelFeeder(Array.from({ length: 5 }, () => corrida("skipped_daily_target"))).pausa, "la meta diaria cumplida también es por diseño");
});

test("salteado por el ritmo de RapidAPI NO es una pausa sana: alarma y dice el motivo", () => {
  const v = W._frenoDelFeeder(Array.from({ length: 5 }, () => corrida("skipped_throttle", { notes: "pacing — used 40% vs cycle 1%" })));
  strictEqual(v.pausa, null, "el 18/09 ese freno era un contador mal leído: taparlo como 'pausa' lo habría escondido");
  ok(/skipped_throttle/.test(v.raro) && /used 40%/.test(v.raro), v.raro);
});

test("si algún turno corrió, o hay pocos datos, no se afirma nada", () => {
  const mezcla = [corrida("ok"), ...Array.from({ length: 4 }, () => corrida("skipped_saturated"))];
  strictEqual(W._frenoDelFeeder(mezcla).pausa, null);
  strictEqual(W._frenoDelFeeder([corrida("skipped_saturated")]).pausa, null, "con un solo turno no alcanza");
  strictEqual(W._frenoDelFeeder(null).pausa, null);
});

test("sólo las fuentes que inyecta el feeder pueden ir como 'en pausa'", () => {
  ok(/const _FUENTES_DEL_FEEDER = new Set\(\["adstxt", "sellers_json", "majestic"\]\);/.test(indexJs),
     "similar, autogoogle y monday_refresh tienen su propio job: si callan, es una falla");
  ok(/_frenoFeeder && _FUENTES_DEL_FEEDER\.has\(fuente\)/.test(indexJs));
});

// ── 4. El linter y los alfabetos ──────────────────────────────────────────────────────────
const lint = (body) => revisarEntregabilidad({ to: "a@b.com", subject: "Propuesta", body, esProspeccion: false }).bloqueantes;
test("un mail en ucraniano o griego que nombra a ADEQ Media no es un homóglifo", () => {
  ok(!lint("Добрий день! Ми з ADEQ Media, працюємо з xsport.ua та іншими виданнями.").includes("homoglifos_mezclados"));
  ok(!lint("Καλημέρα, είμαστε η ADEQ Media και δουλεύουμε με το mixanitouxronou.gr").includes("homoglifos_mezclados"));
  ok(!lint("Resumen: el sitio Ελλάδα-news.gr rindió bien").includes("homoglifos_mezclados"), "separados por un guion son dos palabras");
});

test("dos alfabetos dentro de UNA palabra sí se frena", () => {
  ok(lint("Entrá a tu cuenta de pаypal ahora").includes("homoglifos_mezclados"), "la «а» de pаypal es cirílica");
  ok(lint("Visitá gοogle.com").includes("homoglifos_mezclados"), "la «ο» de gοogle es griega");
  ok(lint("Оffer para vos").includes("homoglifos_mezclados"), "la «О» inicial es cirílica");
});
