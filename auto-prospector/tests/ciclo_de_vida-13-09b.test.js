// El ciclo de vida de un lead: cómo entra, cómo se congela, cómo vuelve. (2026-09-13, auditoría)
//
// "Lo que arreglás un día no lo rompas al otro." Cada test es una regla que la auditoría encontró
// rota con el código real:
//   C41. Una fila que VUELVE a Prospects (upsert por dominio) traía las marcas de su vida anterior:
//        suspect_reject de una purga vieja (el agente no la mandaba nunca), la etiqueta "agent" y la
//        firma del MB en un lead de AutoGoogle, y pisaba la tanda 'Por enviar' de un MB.
//   C42. El congelado del agente por falta de email se disfrazaba de import manual (uploaded_by = MB)
//        y, si al reprocesarse la cola lo descartaba, la fila quedaba 'frozen' para siempre: nadie la
//        ve, nadie la cuenta.
//   C43. Los botones de reciclables del popup encolaban sin cruzar contra Prospects: la cola pagaba
//        ads.txt, scrape y email de dominios que ya estaban para terminar en "dup".
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { findKnownDomains } from "../../modules/sellersJson.js";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const popup = fs.readFileSync(path.join(RAIZ, "..", "popup", "popup.js"), "utf8");
const entre = (desde, hasta) => {
  const i = worker.indexOf(desde);
  ok(i >= 0, `no encontré "${desde}"`);
  return worker.slice(i, worker.indexOf(hasta, i));
};
const cuerpoDe = (texto, nombre) => {
  const i = texto.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return texto.slice(i, texto.indexOf("\n}\n", i));
};
const respuesta = (body, { status = 200 } = {}) => ({ ok: status >= 200 && status < 300, status, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) });

// ── C41. La fila que vuelve ─────────────────────────────────────────────────────────────
const W41 = await cargarWorker(["saveToReviewQueue", "_ajustesDeReactivacion"], { fetchFalso: true });

// Simula la base: la fila previa del dominio (o ninguna) y captura el upsert.
function simularBase(prev, { consultaNuevaFalla = false, pendingViejo = null } = {}) {
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = opts.method || "GET";
    pedidos.push({ u, m, body: opts.body ? JSON.parse(opts.body) : null });
    if (m === "GET" && u.includes("toolbar_review_queue?domain=eq.") && u.includes("select=id,status,")) {
      return consultaNuevaFalla ? respuesta({ message: "boom" }, { status: 500 }) : respuesta(prev ? [prev] : []);
    }
    if (m === "GET" && u.includes("toolbar_review_queue?domain=eq.") && u.includes("status=eq.pending")) return respuesta(pendingViejo ? [pendingViejo] : []);
    if (u.includes("toolbar_sendtrack")) return respuesta([]);
    if (m === "POST" && u.includes("toolbar_review_queue?on_conflict=domain")) return respuesta(null, { status: 201 });
    return respuesta([]);
  };
  return pedidos;
}
const LEAD = (extra = {}) => ({ domain: "diario-ejemplo.com.ar", traffic: 900000, geo: "AR", emails: ["publicidad@diario-ejemplo.com.ar"], createdBy: "worker@autofeeder", source: "similar", ...extra });
const upsert = (pedidos) => pedidos.find(p => p.m === "POST" && p.u.includes("toolbar_review_queue?on_conflict=domain"));

test("C41: una purga vieja no viaja con la fila reactivada, y la historia (validated/rejected_at) no se borra", async () => {
  const pedidos = simularBase({ id: 7, status: "rejected", source: "similar", created_by: "worker@autofeeder", suspect_reject: true, suspect_reason: "purge: tipo banco" });
  strictEqual(await W41.saveToReviewQueue("t", LEAD()), "ok");
  const b = upsert(pedidos)?.body;
  ok(b, "tiene que haber upsert");
  strictEqual(b.status, "pending");
  strictEqual(b.suspect_reject, false, "con suspect_reject=true el agente (suspect_reject=not.is.true) no la mandaba nunca");
  strictEqual(b.suspect_reason, null);
  strictEqual(b.suspect_checked_at, null, "el análisis la vuelve a mirar como a una fila nueva");
  for (const k of ["validated_by", "validated_at", "rejected_at", "created_at"]) ok(!(k in b), `${k} es historia: no se toca`);
});

test("C41: un rechazo de una persona se queda (con motivo 'mb:' o SIN motivo) y un barrido rechazado después también", async () => {
  for (const prev of [
    { status: "rejected", suspect_reject: true, suspect_reason: "mb: no es un medio" },
    { status: "rejected", suspect_reject: true, suspect_reason: null },            // ❌ sin motivo: rejectReviewItem no escribe razón
    { status: "rejected", suspect_reject: true, suspect_reason: "barrido: tienda" }, // el barrido marca; el rechazo lo hizo alguien
    { status: "rejected", suspect_reject: true, suspect_reason: "Parece un banco (Haiku)" },
  ]) {
    const pedidos = simularBase({ id: 1, source: "csv", created_by: "diego@adeqmedia.com", ...prev });
    await W41.saveToReviewQueue("t", LEAD());
    const b = upsert(pedidos).body;
    ok(!("suspect_reject" in b) && !("suspect_reason" in b) && !("suspect_checked_at" in b), `${JSON.stringify(prev)} vuelve a Alert para que el MB decida`);
  }
  // Un barrido sobre una fila que NO terminó rechazada (se validó a mano) sí se limpia.
  const pedidos = simularBase({ id: 2, status: "validated", source: "csv", suspect_reject: true, suspect_reason: "barrido: tienda" });
  await W41.saveToReviewQueue("t", LEAD());
  strictEqual(upsert(pedidos).body.suspect_reject, false);
  // Las otras marcas automáticas que rechazan en el mismo paso.
  for (const r of ["urlpurge: /tienda/", "envio: gobierno", "descongelado: not_publisher: sin_ads_txt"]) {
    strictEqual(W41._ajustesDeReactivacion({ status: "rejected", suspect_reject: true, suspect_reason: r }, { source: "similar" }).extra.suspect_reject, false, r);
  }
});

test("C41: un reintento conserva la fuente y la firma originales; un reciclado del CRM las pisa a propósito", async () => {
  let pedidos = simularBase({ id: 3, status: "frozen", source: "autogoogle", created_by: "worker@autofeeder", suspect_reject: false });
  await W41.saveToReviewQueue("t", LEAD({ source: "agent", createdBy: "sales@adeqmedia.com" }));
  let b = upsert(pedidos).body;
  strictEqual(b.source, "autogoogle", "'altas por fuente' tiene que seguir contando AutoGoogle");
  strictEqual(b.created_by, "worker@autofeeder", "el filtro USUARIO no puede mostrarlo como carga del MB");

  for (const etiqueta of ["frozen_retry", "prospects_offline", "origen_desconocido", "bounce_retry", "agent_reengagement"]) {
    pedidos = simularBase({ id: 4, status: "validated", source: "sellers_json", created_by: "worker@autofeeder" });
    await W41.saveToReviewQueue("t", LEAD({ source: etiqueta, createdBy: "mb@adeqmedia.com" }));
    strictEqual(upsert(pedidos).body.source, "sellers_json", etiqueta);
  }

  pedidos = simularBase({ id: 5, status: "validated", source: "csv", created_by: "diego@adeqmedia.com" });
  await W41.saveToReviewQueue("t", LEAD({ source: "monday_refresh", createdBy: "ejecutivo@adeqmedia.com" }));
  b = upsert(pedidos).body;
  strictEqual(b.source, "monday_refresh");
  strictEqual(b.created_by, "ejecutivo@adeqmedia.com", "el reciclado se muestra al dueño del negocio");
  deepStrictEqual([b.email_intentos, b.email_ultimo_intento, b.email_ultimo_motivo], [0, null, null], "reciclar busca un contacto NUEVO");

  // Un import de una persona sobre una fila vieja: pisa como siempre y no toca la búsqueda de email.
  pedidos = simularBase({ id: 6, status: "rejected", source: "similar", created_by: "worker@autofeeder" });
  await W41.saveToReviewQueue("t", LEAD({ source: "manual", createdBy: "diego@adeqmedia.com" }));
  b = upsert(pedidos).body;
  strictEqual(b.source, "manual");
  strictEqual(b.created_by, "diego@adeqmedia.com");
  ok(!("email_intentos" in b));
});

test("C41: la tanda 'Por enviar' de un MB no se toca, y pending sigue siendo dup", async () => {
  let pedidos = simularBase({ id: 8, status: "por_enviar", source: "csv", created_by: "sales@adeqmedia.com" });
  strictEqual(await W41.saveToReviewQueue("t", LEAD()), "en_cola_de_envio");
  strictEqual(upsert(pedidos), undefined, "pasarla a pending la sacaba de la cola del MB y el agente la podía mandar dos veces");
  pedidos = simularBase({ id: 9, status: "pending", source: "csv" });
  strictEqual(await W41.saveToReviewQueue("t", LEAD()), "dup");
  strictEqual(upsert(pedidos), undefined);
});

test("C41: si la consulta nueva falla, el chequeo de duplicado de siempre sigue y el upsert sale sin ajustes", async () => {
  let pedidos = simularBase(null, { consultaNuevaFalla: true, pendingViejo: { id: 10, source: "csv" } });
  strictEqual(await W41.saveToReviewQueue("t", LEAD()), "dup", "perder el chequeo de dup por una columna faltante pisaría leads pending");
  strictEqual(upsert(pedidos), undefined);

  pedidos = simularBase(null, { consultaNuevaFalla: true });
  strictEqual(await W41.saveToReviewQueue("t", LEAD()), "ok");
  const b = upsert(pedidos).body;
  for (const k of ["suspect_reject", "suspect_reason", "suspect_checked_at", "email_intentos"]) ok(!(k in b), `sin fila leída no se ajusta ${k}`);
  strictEqual(b.source, "similar");

  // Dominio nuevo: el payload es el de siempre.
  pedidos = simularBase(null);
  strictEqual(await W41.saveToReviewQueue("t", LEAD({ source: "agent" })), "ok");
  ok(!("suspect_reject" in upsert(pedidos).body));
});

// ── C42. El congelado del agente ────────────────────────────────────────────────────────
const W42 = await cargarWorker(["_origenParaCongelar", "_destinoHuerfanoFrozen", "reconciliarHuerfanosFrozen"], { fetchFalso: true });

test("C42: el congelado guarda el origen real del lead, no el mail del MB que corría el agente", () => {
  deepStrictEqual(W42._origenParaCongelar({ source: "autogoogle", created_by: "worker@autofeeder" }), { source: "autogoogle", uploadedBy: "worker@autofeeder" },
    "con uploaded_by = MB el descongelado pasaba por import manual: se salteaba la despriorización GEO y el cupo anglo");
  deepStrictEqual(W42._origenParaCongelar({ source: "csv", created_by: "diego@adeqmedia.com" }), { source: "csv", uploadedBy: "diego@adeqmedia.com" },
    "lo que un MB importó a mano vuelve como import manual de ese MB");
  deepStrictEqual(W42._origenParaCongelar({ source: "monday_refresh", created_by: "ejecutivo@adeqmedia.com" }), { source: "monday_refresh", uploadedBy: "worker@autofeeder" },
    "en el reciclado la firma es el ejecutivo de la ficha, no quien lo cargó");
  deepStrictEqual(W42._origenParaCongelar(null), { source: "frozen_retry", uploadedBy: "worker@autofeeder" }, "sin datos: etiqueta de reintento honesta");
  deepStrictEqual(W42._origenParaCongelar({ source: "agent", created_by: "" }), { source: "frozen_retry", uploadedBy: "worker@autofeeder" }, "'agent' no es un origen");
});

test("C42: el bloque de congelado usa esa regla y anota el congelado ANTES de cambiar el estado", () => {
  const bloque = entre("if (failsToday >= 3) {", "3 fails sin email → FREEZE");
  ok(!/uploaded_by:\s*userEmail/.test(bloque), "volvió uploaded_by: userEmail");
  ok(/_origenParaCongelar\(/.test(bloque), "tiene que usar _origenParaCongelar");
  ok(/last_error: "no_email_3_attempts"/.test(bloque));
  const post = bloque.indexOf("toolbar_frozen_leads`"), patch = bloque.indexOf('status: "frozen"');
  ok(post > 0 && patch > 0 && post < patch, "si el estado cambia primero, el reconciliador puede verla huérfana en el medio");
});

test("C42: el destino de una fila 'frozen' sin pareja en congelados sale del veredicto de la cola", () => {
  const d = W42._destinoHuerfanoFrozen;
  strictEqual(d({ enFrozenLeads: true, csvStatus: "skipped", csvError: "not_publisher: sin_ads_txt" }), "dejar", "sigue congelado de verdad");
  for (const st of ["pending", "processing", "waiting_pool", "next_day"]) strictEqual(d({ csvStatus: st }), "dejar", `${st}: todavía se está evaluando`);
  for (const e of ["not_publisher: sin_ads_txt", "crm_activo: estado=\"Negociando\"", "dead_domain_dns_fail", "review_queue_insert_fail:floor", "blocked: inoperativo"]) {
    strictEqual(d({ csvStatus: "skipped", csvError: e }), "rejected", e);
  }
  for (const e of ["traffic_api_transient: 20 reintentos sin éxito", "sin_cuota_de_api (no cuenta como intento)", "freeze_failed: timeout", "review_queue_insert_fail:http_500:boom", "review_queue_insert_fail:http_max_retries", ""]) {
    strictEqual(d({ csvStatus: "skipped", csvError: e }), "pending", `culpa nuestra, no del dominio: ${e || "(vacío)"}`);
  }
  for (const st of ["error", "expired", "done", "frozen", null]) strictEqual(d({ csvStatus: st }), "pending", `${st}: nadie la va a reintentar, vuelve a verse`);
});

test("C42: el reconciliador aplica el veredicto, sólo sobre filas que siguen 'frozen', y 'no pude leer' no es 'cero'", async () => {
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = opts.method || "GET";
    pedidos.push({ u, m, body: opts.body ? JSON.parse(opts.body) : null });
    if (m === "GET" && u.includes("toolbar_review_queue?status=eq.frozen")) {
      return respuesta([{ id: 1, domain: "a.com" }, { id: 2, domain: "b.com" }, { id: 3, domain: "c.com" }, { id: 4, domain: "d.com" }, { id: 5, domain: "e.com" }]);
    }
    if (m === "GET" && u.includes("toolbar_frozen_leads?domain=in.")) return respuesta([{ domain: "a.com" }]);
    if (m === "GET" && u.includes("toolbar_csv_queue?domain=in.")) {
      return respuesta([
        { domain: "b.com", status: "pending", error_message: "unfrozen_retry_attempt_2" },
        { domain: "c.com", status: "skipped", error_message: "not_publisher: sin_ads_txt" },
        { domain: "d.com", status: "skipped", error_message: "traffic_api_transient: 20 reintentos sin éxito" },
      ]);
    }
    return respuesta(null, { status: 204 });
  };
  const r = await W42.reconciliarHuerfanosFrozen("t");
  deepStrictEqual(r, { revisados: 5, aPending: 2, aRejected: 1 });
  const patches = pedidos.filter(p => p.m === "PATCH");
  strictEqual(patches.length, 3, "a (congelado) y b (en la cola) no se tocan");
  ok(patches.every(p => p.u.includes("status=eq.frozen")), "el PATCH se condiciona a que siga frozen: si en el medio volvió a pending, no se pisa");
  const porId = Object.fromEntries(patches.map(p => [p.u.match(/id=eq\.(\d+)/)[1], p.body]));
  strictEqual(porId[3].status, "rejected");
  strictEqual(porId[3].suspect_reject, true);
  strictEqual(porId[3].suspect_reason, "descongelado: not_publisher: sin_ads_txt");
  ok(porId[3].rejected_at, "rejected_at: los contadores de purgas cuentan por esa fecha");
  deepStrictEqual(porId[4], { status: "pending" });
  deepStrictEqual(porId[5], { status: "pending" });

  globalThis.__fetchFalso = async () => respuesta({ message: "caído" }, { status: 503 });
  let tiro = false;
  try { await W42.reconciliarHuerfanosFrozen("t"); } catch { tiro = true; }
  ok(tiro, "si no puede leer tiene que tirar, para que el latido quede en fail y no en 'ok, 0'");
});

test("C42: el descongelador corre el reconciliador en su vuelta, con latido propio que falla si no pudo leer", () => {
  const tick = entre('if (await _tocaCorrer(token, "unfreezer", 15)) {', '// ── Cleanup periódico cada 30 iters ──');
  ok(/reconciliarHuerfanosFrozen\(token\)/.test(tick), "el descongelador tiene que llamar al reconciliador");
  ok(/saludPing\(token, "huerfanos_frozen", \{ status: "ok", cadenciaMin: 15/.test(tick));
  ok(/saludPing\(token, "huerfanos_frozen", \{ status: "fail", cadenciaMin: 15/.test(tick));
  ok(/select=domain,source,uploaded_by,attempt_count,last_error&limit=20/.test(tick), "el descongelador de siempre sigue igual");
});

// ── C43. Los botones de reciclables del popup ───────────────────────────────────────────
test("C43: el modo 'reciclable' de la extensión cruza lo mismo que el reciclado del worker", async () => {
  const pedidos = [];
  const fetchViejo = globalThis.fetch;
  globalThis.fetch = async (url, opts = {}) => {
    const u = String(url);
    pedidos.push({ u, signal: opts.signal });
    if (u.includes("toolbar_review_queue")) return respuesta([{ domain: "Ya-En-Prospects.com" }]);
    if (u.includes("toolbar_csv_queue")) return respuesta([{ domain: "en-cola.com" }]);
    if (u.includes("toolbar_url_blocklist")) return respuesta({ message: "caída" }, { status: 500 });
    return respuesta([]);
  };
  try {
    const known = await findKnownDomains("https://x.supabase.co", "anon", "tok", ["ya-en-prospects.com", "en-cola.com", "libre.com"], { mode: "reciclable" });
    deepStrictEqual([...known].sort(), ["en-cola.com", "ya-en-prospects.com"], "una tabla caída se saltea, no rompe el botón");
    const cola = pedidos.find(p => p.u.includes("toolbar_csv_queue")).u;
    const rq = pedidos.find(p => p.u.includes("toolbar_review_queue")).u;
    ok(pedidos.some(p => p.u.includes("toolbar_url_blocklist")));
    // Los filtros tienen que ser los del worker, textuales.
    const filtroColaWorker = cuerpoDe(worker, "_dominiosActivosEnCola").match(/status=in\.\([^)]+\)/)[0];
    const filtroProspectsWorker = cuerpoDe(worker, "_dominiosPendientesEnProspects").match(/status=eq\.pending/)[0];
    ok(cola.includes(filtroColaWorker), `cola: ${cola} no trae ${filtroColaWorker}`);
    ok(rq.includes(filtroProspectsWorker), `Prospects: ${rq} no trae ${filtroProspectsWorker}`);
    ok(pedidos.every(p => p.signal), "todo fetch con reloj");
    // El alias viejo no puede caer en el modo "all" (que bloquea cualquier fila histórica).
    pedidos.length = 0;
    await findKnownDomains("https://x.supabase.co", "anon", "tok", ["libre.com"], { mode: "monday_refresh" });
    ok(pedidos.find(p => p.u.includes("toolbar_review_queue")).u.includes("status=eq.pending"));
  } finally {
    globalThis.fetch = fetchViejo;
  }
});

test("C43: los dos botones de reciclables cruzan en modo 'reciclable' ANTES de subir a la cola", () => {
  const tramo = (desde, hasta) => {
    const i = popup.indexOf(desde);
    ok(i >= 0, `no encontré ${desde} en popup.js`);
    return popup.slice(i, popup.indexOf(hasta, i));
  };
  const botones = {
    "Send to Queue": tramo('getElementById("btn-import-queue").addEventListener', "// initCsvQueue()"),
    "Traer y encolar desde ADEQ": tramo('getElementById("btn-refresh-from-monday")?.addEventListener', "clearProc.addEventListener"),
  };
  for (const [nombre, h] of Object.entries(botones)) {
    const cruce = h.search(/findKnownDomains\([^;]*\{ mode: "reciclable" \}\)/);
    const subida = h.indexOf("uploadCsvDomains(");
    ok(cruce > 0, `${nombre}: tiene que cruzar con findKnownDomains en modo "reciclable"`);
    ok(subida > cruce, `${nombre}: el cruce va antes de uploadCsvDomains`);
  }
  ok(/shuffled = \[\.\.\._libres\]/.test(botones["Send to Queue"]), "los 15 se eligen entre los libres, no entre todos");
});
