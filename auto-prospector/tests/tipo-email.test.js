// Apollo compite por resultado (decisión 2 del user, 2026-09-04).
//
// Medido en 90 días: 169 envíos a emails de Apollo → 3 respuestas (1,8%), contra 6,6% de los
// que el MB agregó a mano. Ser de Apollo ya no es un tier propio: el email vale lo que es.
// Lo manual sí sigue arriba: lo eligió una persona.
//
// Run: npm test
import { test } from "node:test";
import { strictEqual } from "node:assert";
import { _tipoDeEmailParaRanking } from "../lib/email.js";

test("una persona de Apollo es una persona, no 'apollo'", () => {
  strictEqual(_tipoDeEmailParaRanking("maria.lopez@diario.com", "apollo"), "persona");
});

test("un rol comercial de Apollo es un rol", () => {
  strictEqual(_tipoDeEmailParaRanking("publicidad@diario.com", "apollo"), "rol");
});

test("un info@ que devolvió Apollo es un genérico", () => {
  strictEqual(_tipoDeEmailParaRanking("info@diario.com", "apollo"), "generico");
});

test("lo que el MB eligió a mano sigue en el tier más alto", () => {
  strictEqual(_tipoDeEmailParaRanking("info@diario.com", "manual"), "apollo");
});

test("el scrape sigue como antes: persona y genérico", () => {
  strictEqual(_tipoDeEmailParaRanking("juan.perez@diario.com", "scrape"), "persona");
  strictEqual(_tipoDeEmailParaRanking("contacto@diario.com", "generic"), "generico");
});
