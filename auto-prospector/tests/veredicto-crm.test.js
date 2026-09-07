// ¿Se puede prospectar esta web? — la regla del user, 2026-09-07, textual:
//
//   · no está en el CRM  → "Web prospectable. Nunca fue contactada."
//   · Ciclo Finalizado   → "Web prospectable. Ya tiene ciclo finalizado."
//   · Pausado            → "Web prospectable. Cliente Antiguo Pausado."
//   · Propuesta Vigente / En Negociacion / Personalizado → NO deja prospectar. "Propuesta en curso."
//   · Live               → NO deja prospectar. "Cliente activo."
//
// Los cinco estados de la izquierda son los que EXISTEN hoy en `crm_board_prospects`
// (medido el 07/09: Ciclo Finalizado 7.236 · Propuesta Vigente 2.599 · En Negociacion 86 ·
// Pausado 62 · Live 45). El vocabulario del CRM ya cambió tres veces en un día, así que este
// test es también el detector: si mañana aparece un estado nuevo, el veredicto tiene que decir
// "no lo reconozco" y NO dejar escribir — nunca inventar que se puede.
//
// `_veredictoCrm` vive en popup/popup.js, que no se puede importar (arranca el DOM entero):
// se extrae el bloque por texto, igual que hace tests/paridad-popup.test.js.
//
// Run: npm test
import { test } from "node:test";
import { strictEqual, ok, match } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const POPUP = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "popup", "popup.js");
const src = fs.readFileSync(POPUP, "utf8");
const ini = src.indexOf("const _CRM_LIVE_RE");
const fin = src.indexOf("function _pintarVeredictoCrm");
ok(ini > 0 && fin > ini, "no encontré _veredictoCrm en popup.js — ¿lo renombraron?");
// Se saca `_dominiosBloqueadosCrm`, que usa chrome.storage y no hace falta para la regla.
const cuerpo = src.slice(ini, fin).replace(/const _BLOQ_CACHE_KEY[\s\S]*?^}/m, "");
const veredicto = new Function(cuerpo + "; return _veredictoCrm;")();

test("una web que nunca fue contactada se puede prospectar", () => {
  const v = veredicto({ found: false });
  strictEqual(v.ok, true);
  match(v.titulo, /prospectable/i);
  match(v.detalle, /nunca fue contactada/i);
});

test("Ciclo Finalizado se puede volver a prospectar", () => {
  const v = veredicto({ found: true, status: "Ciclo Finalizado", descansando: false });
  strictEqual(v.ok, true);
  match(v.detalle, /ciclo finalizado/i);
});

test("Pausado se puede prospectar y se llama Cliente Antiguo Pausado", () => {
  const v = veredicto({ found: true, status: "Pausado" });
  strictEqual(v.ok, true);
  match(v.detalle, /cliente antiguo pausado/i);
});

test("Propuesta Vigente, En Negociacion y Personalizado NO se prospectan", () => {
  for (const estado of ["Propuesta Vigente", "En Negociacion", "En Negociación", "Personalizado"]) {
    const v = veredicto({ found: true, status: estado });
    strictEqual(v.ok, false, `${estado} tendría que bloquear`);
    strictEqual(v.duda, undefined, `${estado} es un NO firme, no una duda`);
    match(v.detalle, /propuesta en curso/i);
  }
});

test("Live NO se prospecta: es un cliente activo", () => {
  const v = veredicto({ found: true, status: "Live" });
  strictEqual(v.ok, false);
  match(v.detalle, /cliente activo/i);
});

test("el descanso de 40 días del CRM se respeta", () => {
  const v = veredicto({ found: true, status: "Ciclo Finalizado", descansando: true, diasParaReintentar: 12 });
  strictEqual(v.ok, false);
  match(v.detalle, /12 día/);
});

test("si el CRM lo bloquea por otra razón (cliente que factura, bloqueado a mano), tampoco", () => {
  const v = veredicto({ found: false }, { bloqueadoPorCrm: true });
  strictEqual(v.ok, false);
  match(v.detalle, /bloqueado/i);
});

test("no haber podido preguntar NUNCA es 'está libre'", () => {
  const v = veredicto({ indeterminado: true });
  strictEqual(v.ok, false);
  strictEqual(v.duda, true);
});

test("un estado que no conocemos no se declara prospectable", () => {
  const v = veredicto({ found: true, status: "Estado Que No Existia Ayer" });
  strictEqual(v.ok, false);
  strictEqual(v.duda, true);
});

test("el envío y la carga se niegan cuando el veredicto dice que no", () => {
  // El user fue explícito: "tiene que no dejar prospectar". Avisar no alcanza.
  for (const guard of [/Guard #0[\s\S]{0,400}?crmVeredicto/, /btn-send-gmail[\s\S]{0,1200}?crmVeredicto/]) {
    ok(guard.test(src), `falta el freno: ${guard}`);
  }
  ok(/function _aplicarBloqueoCrm/.test(src), "falta _aplicarBloqueoCrm, que apaga el botón");
  ok(/validateProspect[\s\S]{0,1400}?_veredictoCrm/.test(src), "Prospects manda sin consultar el CRM");
});
