// Integración de las rondas del 13/09: reglas que sólo se ven cuando los arreglos de varios grupos
// están juntos en main.
//
// Run: npm test
import { test } from "node:test";
import { strictEqual, deepStrictEqual } from "node:assert";
import { cargarWorker } from "./_worker-exportado.mjs";

process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";

const resp = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: () => null },
  json: async () => body,
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
globalThis.__fetchFalso = async () => resp([]);
const W = await cargarWorker(["processCsvItem"], { fetchFalso: true });
const USO_APOLLO = { usedToday: 0, limit: 0, monthLimit: 0, usedThisMonth: 0 };

// El chequeo previo de Prospects se salteaba para los imports manuales: un MB que importaba un sitio ya
// pendiente pagaba tráfico, scrape, Haiku y quizá Apollo, y al final saveToReviewQueue decía "dup".
test("un import manual de un sitio que ya está en Prospects sale como ya_estaba_en_prospects sin pagar nada", async () => {
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase();
    reg.push({ u, m, b: String(opts.body || "") });
    if (m === "GET" && u.includes("toolbar_review_queue?domain=eq.") && u.includes("status=in.(pending,por_enviar)")) return resp([{ id: 7 }]);
    return resp([]);
  };
  const item = { id: 900, domain: "diariodeprueba.com.ar", source: "manual", uploaded_by: "dhorovitz@adeqmedia.com", error_message: "" };
  await W.processCsvItem("t", item, { rapidapi_key: "k" }, USO_APOLLO, { count: 0 });

  const marcas = reg.filter(r => r.m === "PATCH" && r.u.includes("toolbar_csv_queue?id=eq.900")).map(r => JSON.parse(r.b));
  deepStrictEqual(marcas.map(p => [p.status, p.error_message]), [["skipped", "ya_estaba_en_prospects"]]);
  strictEqual(reg.filter(r => /rapidapi|anthropic|apollo\.io|\/ads\.txt|\/api\/crm\//i.test(r.u)).length, 0, "no se paga ni se consulta nada más");
});
