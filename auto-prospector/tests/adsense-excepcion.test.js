// La única excepción a "sin ads.txt no entra": AdSense activo (decisión 1 del user, 2026-09-04).
//   "Si no tiene ads txt y tiene tags de Adsense activos puede entrar, la única regla. El resto no entra."
// Se prueba con el scoreProspectable REAL del worker (cargado entero), no con una copia.
//
// Run: npm test
import { test } from "node:test";
import { strictEqual, ok } from "node:assert";
import { cargarWorker } from "./_worker-exportado.mjs";

const { scoreProspectable } = await cargarWorker(["scoreProspectable"]);
const medio = { title: "Diario del Sur", category: "news", hasDisplayAds: true, hasProgrammatic: true, adNetworks: [], nonPublisherType: null, hasAdSense: true };
const base = { domain: "diariodelsur.co.za", urlVerdict: { ok: true, reason: "url_ok" }, swCategory: "news_and_media", haikuType: "publisher", traffic: 800_000 };

test("sin ads.txt y sin AdSense: no entra", () => {
  const v = scoreProspectable({ ...base, adsTxt: { state: "no", lines: 0 }, pageContent: { ...medio, hasAdSense: false, hasProgrammatic: false, hasDisplayAds: false } });
  strictEqual(v.ok, false);
  strictEqual(v.reason, "sin_ads_txt");
});

test("sin ads.txt pero con AdSense activo (estado 'adsense'): entra, y lo dice en las señales", () => {
  const v = scoreProspectable({ ...base, adsTxt: { state: "adsense", lines: 0, excepcion: "adsense" }, pageContent: medio });
  strictEqual(v.ok, true, `rechazado: ${v.reason} · ${(v.señales || []).join(" · ")}`);
  ok((v.señales || []).some(s => /AdSense/.test(s)), "falta la señal de la excepción");
});

test("la excepción no trae la puerta grande: una tienda con AdSense y sin ads.txt sigue afuera", () => {
  const v = scoreProspectable({ ...base, adsTxt: { state: "adsense", lines: 0 }, pageContent: { ...medio, nonPublisherType: "ecommerce", nonPublisherFuerte: true } });
  strictEqual(v.ok, false);
  strictEqual(v.reason, "nonpub_ecommerce");
});

test("con ads.txt todo sigue igual que antes", () => {
  const v = scoreProspectable({ ...base, adsTxt: { state: "yes", lines: 40, systems: 12 }, pageContent: medio });
  strictEqual(v.ok, true, `rechazado: ${v.reason}`);
});
