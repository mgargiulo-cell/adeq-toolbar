// "Tiene ads.txt: ¿es prospectable o no?" — medido sobre el CRM real. (2026-09-08)
//
// 271 clientes reales vs 210 rechazados, mismas señales, sin gastar un crédito:
//   ads.txt real          88% de los clientes ·  13% de los rechazados
//   ads.txt >= 10 líneas  75%                 ·  10%   ← y casi todos esos 10% eran medios mal clasificados
//   GPT/DoubleClick       90%                 ·  63%   ← NO separa: los corporativos también lo usan
//   AdSense               33%                 ·   2%   ← la excepción del user (sin ads.txt + AdSense) está bien calibrada
//   señales de tienda     35%                 ·  46%   ← tampoco separa
//
// Dos correcciones a `scoreProspectable` salen de ahí, y este archivo las fija:
//   1. El marcado fuerte (schema de tienda/hospital) no puede matar un ads.txt de publisher grande
//      (≥15 exchanges): nordest24.it (190) salió como "saas", psvfans.nl (60) como "service".
//   2. Si el ads.txt no se pudo LEER, un veto blando es reintento, no descarte: fxstreet.com
//      (601 líneas) murió como "finanzas" el día que su ads.txt dio 403.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import { cargarWorker } from "./_worker-exportado.mjs";

const mod = await cargarWorker(["scoreProspectable", "_apruebaPorAdsTxtYTrafico"]);
const TRAFICO_OK = 1_200_000;
const adsGrande = { state: "yes", lines: 1159, systems: 190 };
const adsChico  = { state: "yes", lines: 3, systems: 2 };
const adsDudoso = { state: "unknown", why: "http_403" };

test("un ads.txt de publisher grande le gana al marcado fuerte (nordest24.it)", () => {
  const v = mod.scoreProspectable({ domain: "nordest24.it", adsTxt: adsGrande, traffic: TRAFICO_OK,
    pageContent: { nonPublisherType: "saas", nonPublisherFuerte: true, hasDisplayAds: true, title: "Nordest24" }, haikuType: "publisher" });
  strictEqual(v.ok, true, `${v.reason}: ${(v.señales || []).join(" · ")}`);
  ok(v.señales.some(s => /perdonado: ads\.txt de publisher grande/.test(s)), "tiene que decir por qué lo perdonó");
});

test("pero un ads.txt chico NO salva a una tienda con schema de Store (la regla del 04/09 sigue)", () => {
  const v = mod.scoreProspectable({ domain: "tienda.com", adsTxt: adsChico, traffic: TRAFICO_OK,
    pageContent: { nonPublisherType: "ecommerce", nonPublisherFuerte: true, hasDisplayAds: true } });
  strictEqual(v.ok, false);
  strictEqual(v.reason, "nonpub_ecommerce");
  ok(!v.retry, "es un no definitivo: el marcado es del propio sitio y el ads.txt es de agencia");
});

test("ads.txt ilegible + veto blando de la IA = reintento, no descarte (fxstreet.com)", () => {
  const v = mod.scoreProspectable({ domain: "fxstreet.com", adsTxt: adsDudoso, traffic: TRAFICO_OK,
    pageContent: { title: "FXStreet" }, haikuType: "bank" });
  strictEqual(v.ok, false);
  strictEqual(v.retry, true, "no se pudo leer el ads.txt: no se puede sentenciar por el rubro");
  ok(/ads_txt_no_verificable_y_rubro_dudoso:haiku_bank/.test(v.reason), v.reason);
});

test("ads.txt ilegible + categoría de SimilarWeb no-publisher = reintento", () => {
  const v = mod.scoreProspectable({ domain: "x.com", adsTxt: adsDudoso, traffic: TRAFICO_OK, pageContent: {}, swCategory: "finance/investing" });
  strictEqual(v.retry, true, v.reason);
});

test("ads.txt ilegible + veto ESTRUCTURAL sigue matando (gobierno no se reintenta)", () => {
  const v = mod.scoreProspectable({ domain: "algo.gob.ar", adsTxt: adsDudoso, traffic: TRAFICO_OK,
    urlVerdict: { ok: false, reason: "url_gobierno" }, pageContent: {} });
  strictEqual(v.ok, false);
  ok(!v.retry, "un gobierno es un gobierno aunque no leamos su ads.txt");
});

test("sin ads.txt confirmado ('no') sigue siendo un no, aunque la IA diga publisher", () => {
  const v = mod.scoreProspectable({ domain: "sin.com", adsTxt: { state: "no" }, traffic: TRAFICO_OK, pageContent: {}, haikuType: "publisher" });
  strictEqual(v.reason, "sin_ads_txt");
  ok(!v.retry);
});

test("la puerta grande sigue exigiendo ads.txt CONFIRMADO y tráfico", () => {
  strictEqual(mod._apruebaPorAdsTxtYTrafico(adsGrande, TRAFICO_OK), true);
  strictEqual(mod._apruebaPorAdsTxtYTrafico(adsDudoso, TRAFICO_OK), false, "'unknown' no abre la puerta: abre el reintento");
  strictEqual(mod._apruebaPorAdsTxtYTrafico(adsGrande, 100_000), false);
});
