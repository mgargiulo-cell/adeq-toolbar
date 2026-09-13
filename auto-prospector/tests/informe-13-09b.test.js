// Los renglones del informe que se contradecían con otro renglón del mismo mail. (2026-09-13, segunda revisión)
//
// "Dejar una base de conceptos, lógicas y funcionamientos automáticos sin fallas: lo que arreglás un
// día no lo rompas al otro." El mail del 12/09 decía:
//   · "sellers 3→0 · sellers_json 105→0 · majestic 53→0": el feeder, el import del MB y el re-trabajo
//     (congelados que vuelven con la etiqueta de Prospects) sumados como si fueran la misma fuente.
//   · "La cola procesó 1227" y "Drenó 1227": incluían filas que volvieron a pending/next_day.
//   · "Quién los encontró: … generic 25": una vía que ningún rescate escribe (orden de claves jsonb).
//   · "Emails nuevos por vía" sumaba 128 contra 159 rescatados (lista fija sin pattern/informer/social).
//   · "DESCARTES DEL DESCUBRIMIENTO (74)" con ~950 procesados que no pasaron.
//   · "quedan 438 sin email" arriba y los motivos sumando 314 abajo, sin renglón para los sin motivo.
//   · "De dónde salen los emails" contaba claves de email_sources (formularios, direcciones ya quemadas).
//   · "mv_dudoso → 195" sin decir si eran 195 leads o 30 repetidos.
// Cada regla vive ahora en una función pura del worker; acá se prueba la regla y el mail armado.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual, match } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateKeyPairSync } from "node:crypto";
import { cargarWorker } from "./_worker-exportado.mjs";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};

const W = await cargarWorker([
  "_claveFuenteCola", "_estadoColaInforme", "_embudoColaInforme", "_lineasEmbudoCola",
  "_viaDelRescate", "_lineaEmailsPorVia", "_motivoCanonicoCola", "_agruparRechazosCola",
  "_cortarComentario", "_agruparStockSinEmail", "_agruparSalteados", "_viasDeEmailInforme",
  "_boletinPorSeccion", "enviarResumenSalud", "FILTRO_SIN_EMAIL_INFORME",
], { fetchFalso: true });

// ── C1 / C30: de quién es una fila de la cola ───────────────────────────────────────────────
test("el feeder, el import del MB, el re-trabajo y el agente no suman en la misma fuente", () => {
  const c = W._claveFuenteCola;
  deepStrictEqual(c({ source: "auto_feeder_majestic", uploaded_by: "worker@autofeeder" }), { grupo: "feeder", fuente: "majestic" });
  // La misma etiqueta, pero escrita por el descongelador/re-chequeo con la etiqueta de Prospects.
  deepStrictEqual(c({ source: "majestic", uploaded_by: "worker@autofeeder" }), { grupo: "retrabajo", fuente: "majestic" });
  deepStrictEqual(c({ source: "sellers_json", uploaded_by: "" }), { grupo: "retrabajo", fuente: "sellers" });
  // El import de sellers.json desde la extensión (popup.js uploadCsvDomains(..., "sellers_json")).
  deepStrictEqual(c({ source: "sellers_json", uploaded_by: "agustina@adeqmedia.com" }), { grupo: "import_mb", fuente: "sellers" });
  deepStrictEqual(c({ source: "auto_feeder_sellers", uploaded_by: "worker@autofeeder" }), { grupo: "feeder", fuente: "sellers" });
  deepStrictEqual(c({ source: "autogoogle", uploaded_by: "worker@autofeeder" }), { grupo: "feeder", fuente: "autogoogle" });
  deepStrictEqual(c({ source: "auto_feeder_monday", uploaded_by: "worker@autofeeder" }), { grupo: "feeder", fuente: "crm_reciclado" });
  // El agente re-encola a nombre del MB: no es un import.
  deepStrictEqual(c({ source: "agent", uploaded_by: "sales@adeqmedia.com" }), { grupo: "envio", fuente: "agent" });
  deepStrictEqual(c({ source: "bounce_retry", uploaded_by: "sales@adeqmedia.com" }), { grupo: "envio", fuente: "bounce_retry" });
  deepStrictEqual(c({ source: "retry:auto_feeder_sellers", uploaded_by: "worker@autofeeder" }), { grupo: "retrabajo", fuente: "sellers" });
});

test("pending y next_day no son 'procesadas': vuelven a la cola", () => {
  strictEqual(W._estadoColaInforme("done"), "procesada");
  strictEqual(W._estadoColaInforme("skipped"), "procesada");
  strictEqual(W._estadoColaInforme("error"), "procesada");
  strictEqual(W._estadoColaInforme("frozen"), "congelada");
  strictEqual(W._estadoColaInforme("pending"), "pospuesta");
  strictEqual(W._estadoColaInforme("next_day"), "pospuesta");
});

// El fixture del 12/09 en chiquito. Se usa para la regla pura y para el mail armado.
const COLA = [
  { domain: "reac.com", source: "auto_feeder_monday", status: "done", uploaded_by: "worker@autofeeder", error_message: null },
  { domain: "nueva.com", source: "auto_feeder_similar", status: "done", uploaded_by: "worker@autofeeder", error_message: null },
  { domain: "pos.com", source: "auto_feeder_similar", status: "pending", uploaded_by: "worker@autofeeder", error_message: "traffic_api_transient retry_1 (reintento sin penalizar): HTTP 503" },
  { domain: "nd.com", source: "auto_feeder_similar", status: "next_day", uploaded_by: "worker@autofeeder", error_message: "sin_cuota_de_api (no cuenta como intento): quota" },
  { domain: "mb1.com", source: "sellers_json", status: "skipped", uploaded_by: "agustina@adeqmedia.com", error_message: "pageviews 212345 (visits×2.31) below min 350000" },
  { domain: "mb2.com", source: "sellers_json", status: "skipped", uploaded_by: "agustina@adeqmedia.com", error_message: "pageviews 99 (visits×2.31) below min 350000" },
  { domain: "ret.com", source: "sellers_json", status: "frozen", uploaded_by: "worker@autofeeder", error_message: "frozen_until_2026-09-28 (15d backoff)" },
  { domain: "us.com", source: "auto_feeder_sellers", status: "skipped", uploaded_by: "worker@autofeeder", error_message: "deprio-geo: United States (USA/UK/CA/AU/NZ/IE no procesados)" },
  { domain: "ya.com", source: "auto_feeder_similar", status: "skipped", uploaded_by: "worker@autofeeder", error_message: "ya_estaba_en_prospects" },
  { domain: "ag.com", source: "agent", status: "done", uploaded_by: "sales@adeqmedia.com", error_message: null },
];
// nueva.com tenía ficha en el CRM: se guardó como monday_refresh. suelta.com no pasó por la cola.
const ALTAS24 = [{ domain: "nueva.com", source: "monday_refresh" }, { domain: "suelta.com", source: "csv" }];
const PROS_REACT = [{ domain: "reac.com", source: "monday_refresh" }, { domain: "ag.com", source: "crux" }];

test("el embudo de la cola: denominador sin pospuestas, nuevas vs reactivadas, y la etiqueta con que quedó en Prospects", () => {
  const e = W._embudoColaInforme(COLA, { altas: ALTAS24, prospects: PROS_REACT });
  strictEqual(e.procesadas, 7, "done + skipped + error");
  strictEqual(e.pospuestas, 2);
  strictEqual(e.congeladas, 1);
  strictEqual(e.llegaron, 3);
  strictEqual(e.nuevas, 1);
  strictEqual(e.reactivadas, 2);
  strictEqual(e.procesadas + e.pospuestas + e.congeladas, COLA.length, "toda fila cae en exactamente un balde");
  deepStrictEqual(e.grupos.feeder.similar, { procesadas: 2, llegaron: 1, nuevas: 1, reactivadas: 0, congeladas: 0, pospuestas: 2 });
  deepStrictEqual(e.grupos.import_mb.sellers, { procesadas: 2, llegaron: 0, nuevas: 0, reactivadas: 0, congeladas: 0, pospuestas: 0 });
  strictEqual(e.grupos.retrabajo.sellers.congeladas, 1);
  strictEqual(e.grupos.feeder.sellers.procesadas, 1, "el feeder de sellers no se mezcla con el import ni con el re-trabajo");
  deepStrictEqual(e.otraEtiqueta, { "similar→crm_reciclado": 1 }, "el agente no cuenta como 'otra etiqueta': re-encola leads que ya tenían la suya");
  strictEqual(e.altasSinCola, 1);
  const l = W._lineasEmbudoCola(e).join("\n");
  match(l, /La cola procesó 7 \(sin contar 2 pospuestas, que vuelven a la cola, ni 1 congeladas\) · llegaron a Prospects 3 \(1 nuevas, 2 ya estaban/);
  match(l, /feeders: similar 2→1 \(2 pospuestas\)/);
  match(l, /imports de MBs: sellers 2→0/);
  match(l, /re-trabajo[^:]*: sellers 0→0 \(1 congeladas\)/);
  match(l, /similar→crm_reciclado 1/);
});

// ── C29: quién encontró el email del rescate ────────────────────────────────────────────────
test("la vía del rescate sale del primer email vigente, no del orden de las claves del jsonb", () => {
  // Postgres guarda la clave más larga al final: el "último valor" era generic.
  strictEqual(W._viaDelRescate({ emails: ["ads@diario.com.ar"], email_sources: { "ads@diario.com.ar": "rol_mx", "info@diario.com.ar": { source: "generic" } } }), "rol_mx");
  strictEqual(W._viaDelRescate({ emails: ["juan@x.com", "info@x.com"], email_sources: { "info@x.com": "generic", "juan@x.com": "apollo" } }), "apollo");
  strictEqual(W._viaDelRescate({ emails: ["a@x.com"], email_sources: { "a@x.com": { source: "scrape", url: "https://x.com/contacto" } } }), "scrape");
  strictEqual(W._viaDelRescate({ emails: ["Ventas@X.com"], email_sources: { "ventas@x.com": "pattern" } }), "pattern", "mayúsculas en una de las dos listas");
  strictEqual(W._viaDelRescate({ emails: [], email_sources: { "viejo@x.com": "scrape" } }), "sin_email_vigente");
  strictEqual(W._viaDelRescate({ emails: ["a@x.com"], email_sources: {} }), "sin_fuente");
});

test("ningún informe vuelve a tomar 'la última clave' de email_sources", () => {
  ok(!/Object\.values\([^)]*email_sources[^)]*\)[\s\S]{0,200}\[\s*\w+\.length\s*-\s*1\s*\]/.test(worker),
    "el orden de las claves de un jsonb no es el orden de escritura");
});

test("'Emails nuevos por vía' muestra las vías del plan en cero y suma todo lo rescatado", () => {
  const p = { scrape: 74, rol_mx: 50, generic: 25, informer: 6, apollo: 4 };
  const l = W._lineaEmailsPorVia(p);
  for (const v of ["apollo 4", "serper_persona 0", "rol_mx 50", "pattern 0", "google_contact 0", "scrape 74", "informer 6", "social 0", "generic 25"]) {
    ok(l.includes(v), `falta "${v}" en: ${l}`);
  }
  const suma = [...l.matchAll(/ (\d+)/g)].reduce((s, m) => s + Number(m[1]), 0);
  strictEqual(suma, 159, "el renglón tiene que sumar lo mismo que 'N email(s) encontrados'");
});

// ── C31: descartes de la cola ───────────────────────────────────────────────────────────────
test("el motivo de un descarte es una clave estable: el mismo motivo con números distintos cae en UNA fila", () => {
  const m = W._motivoCanonicoCola;
  const pares = [
    ["pageviews 212345 (visits×2.31) below min 350000", "pageviews 99 (visits×1.00) below min 350000", "trafico_bajo_piso"],
    ["en descanso: cerró hace poco, faltan 45 días para reintentar", "en descanso: cerró hace poco, faltan 3 días para reintentar", "crm_en_descanso"],
    ['crm_activo: estado="Cliente" en "Ventas" (deal activo/con dueño — no se re-prospecta)', 'crm_activo: estado="Negociación" (deal activo/con dueño — no se re-prospecta)', "crm_activo"],
    ['no_prospectable_tipo: "Gambling" es gambling (ni con ads.txt ni con tráfico)', 'no_prospectable_tipo: "Casinos" es gambling (ni con ads.txt ni con tráfico)', "tipo_no_prospectable:gambling"],
    ['category-blocked: "Adult/Dating" matchea "adult"', 'category-blocked: "Adult" matchea "adult"', "categoria_bloqueada:adult"],
    ["duplicate_subdomain_of:clarin.com", "duplicate_subdomain_of:lanacion.com.ar", "subdominio_duplicado"],
    ["traffic_api_transient: 20 reintentos sin éxito — se libera la cola (HTTP 503)", "traffic_api_transient: 20 reintentos sin éxito — se libera la cola (timeout)", "trafico_api_reintentos_agotados"],
    ["not_publisher: bajo_trafico_12345", "not_publisher: bajo_trafico_99", "bajo_trafico"],
    ["not_publisher: gigante_120M", "not_publisher: gigante_45M", "gigante"],
    ["pageviews 99000000 above max 50000000 (gigante — venta directa, no prospección)", "pageviews 51000000 above max 50000000 (gigante — venta directa, no prospección)", "gigante_sobre_techo"],
    ["review_queue_insert_fail:http_409", "review_queue_insert_fail:http_500", "insert_fail:http"],
    ["blocked: admin-blocklist-subdomain (a.com)", "blocked: admin-blocklist-subdomain (b.com)", "blocklist:admin-blocklist-subdomain"],
  ];
  for (const [a, b, clave] of pares) {
    strictEqual(m(a), clave, a);
    strictEqual(m(b), clave, b);
  }
  strictEqual(m("deprio-geo: United States (USA/UK/CA/AU/NZ/IE no procesados)"), "deprio-geo:United States");
  strictEqual(m("worker_geo_excluded:Russia"), "geo_excluida:Russia", "el mismo nombre que toolbar_diag_descartes");
  strictEqual(m("not_publisher: haiku_corp"), "haiku_corp");
  strictEqual(m("not_publisher: sin_ads_txt"), "sin_ads_txt");
  strictEqual(m("not_publisher: url_no_prospectable:gob.ar"), "url_no_prospectable");
  strictEqual(m("ya_estaba_en_prospects"), "ya_estaba_en_prospects");
  strictEqual(m(""), "(sin motivo)");
  // La clase queda cerrada: un return nuevo con números no vuelve a partir la fila.
  for (const [a, b] of pares) for (const msg of [a, b]) ok(!/\d/.test(m(msg)), `clave con dígitos: ${m(msg)} ← ${msg}`);
  ok(!/\d/.test(m("motivo_nuevo_77: detalle 12")), "lo desconocido cambia los dígitos por N");
});

test("todo lo que no pasó cae en un solo balde, y deprio-geo y los descartes por tráfico aparecen", () => {
  const r = W._agruparRechazosCola(COLA);
  strictEqual(r.totalNoPasados, COLA.filter(f => f.status !== "done").length);
  strictEqual(r.totalDescartes + r.yaEstaban + r.errores + r.reintentables + r.reencolados + r.congelados, r.totalNoPasados);
  strictEqual(r.descartes.trafico_bajo_piso.n, 2, "dos 'pageviews N below min' distintos son UN motivo");
  deepStrictEqual(r.descartes.trafico_bajo_piso.ej, ["mb1.com", "mb2.com"]);
  deepStrictEqual(r.descartes.trafico_bajo_piso.porFuente, { "sellers (MB)": 2 });
  strictEqual(r.descartes["deprio-geo:United States"].n, 1);
  strictEqual(r.yaEstaban, 1);
  ok(!r.descartes.ya_estaba_en_prospects, "ya estaba en Prospects no es un descarte");
  strictEqual(r.reintentables, 1);
  strictEqual(r.reencolados, 1);
  strictEqual(r.congelados, 1);
  // Los procesados cuadran con el embudo del boletín: llegaron + descartes + ya estaban + errores.
  const e = W._embudoColaInforme(COLA, { altas: ALTAS24, prospects: PROS_REACT });
  strictEqual(e.llegaron + r.totalDescartes + r.yaEstaban + r.errores, e.procesadas);
});

test("el comentario largo se corta en un punto, no a la mitad de una frase", () => {
  const largo = "Primera oración con el motivo. " + "Regla del user (27/08): aunque tenga tráfico no entra. ".repeat(12);
  const c = W._cortarComentario(largo, 400);
  ok(c.length <= 400, `largo ${c.length}`);
  ok(c.endsWith("."), `tiene que terminar en un punto: …${c.slice(-30)}`);
  const sinPuntos = "palabra ".repeat(80);
  const d = W._cortarComentario(sinPuntos, 400);
  ok(d.endsWith("…") && d.length <= 401, `sin puntos corta en una palabra con …: ${d.slice(-20)}`);
  strictEqual(W._cortarComentario("corto", 400), "corto");
});

// ── C32: el stock sin email, todo ───────────────────────────────────────────────────────────
test("el stock sin email: todo lead cae en un grupo y la suma es el encabezado", () => {
  const filas = [
    { domain: "s1.com", email_ultimo_motivo: "waf_nos_bloqueo:cloudflare", email_intentos: 2 },
    { domain: "s2.com", email_ultimo_motivo: "waf_nos_bloqueo:akamai", email_intentos: 1 },
    { domain: "s3.com", email_ultimo_motivo: null, email_intentos: 0 },
    { domain: "s4.com", email_ultimo_motivo: null, email_intentos: 3 },
    { domain: "s5.com", email_ultimo_motivo: "no_publica_email", email_intentos: 1 },
    { domain: "s6.com", email_ultimo_motivo: "apollo_sin_contacto", email_intentos: 3 },
    { domain: "s7.com", email_ultimo_motivo: "email_en_imagen", email_intentos: 1 },
    { domain: "s8.com", email_ultimo_motivo: "", email_intentos: null },
  ];
  const g = W._agruparStockSinEmail(filas, 5);
  strictEqual(g.total, 8);
  strictEqual(g.conMotivo, 5);
  strictEqual(g.grupos.reduce((s, [, v]) => s + v.n, 0) + g.otros, filas.length, "grupos + otros = todos");
  strictEqual(g.grupos[0][0], "waf_nos_bloqueo", "el detalle después de ':' no parte el motivo");
  strictEqual(g.grupos[0][1].n, 2);
  const todos = W._agruparStockSinEmail(filas, 50);
  const nombres = Object.fromEntries(todos.grupos.map(([k, v]) => [k, v.n]));
  strictEqual(nombres.sin_motivo_todavia_no_buscado, 2, "motivo vacío y sin intentos: todavía no se buscó");
  strictEqual(nombres.sin_motivo_marca_borrada, 1, "motivo vacío con intentos: alguien le borró la marca");
  deepStrictEqual(W._agruparStockSinEmail(null), { total: 0, conMotivo: 0, grupos: [], otros: 0 });
});

test("'sin email' se cuenta con UN filtro en el parte, el boletín y el resumen", () => {
  strictEqual(W.FILTRO_SIN_EMAIL_INFORME, "and=(or(emails.is.null,emails.eq.%5B%5D))", "and=(...) para no pisar otro or= de la misma URL");
  for (const fn of ["parteDelDia", "_boletinPorSeccion", "enviarResumenSalud"]) {
    const c = cuerpoDe(fn);
    ok(c.includes("status=eq.pending&${FILTRO_SIN_EMAIL_INFORME}"), `${fn} no usa el filtro común`);
    ok(!c.includes("status=eq.pending&emails=eq.%5B%5D"), `${fn} tiene una copia del filtro`);
  }
});

// ── C33: de dónde salen los emails ──────────────────────────────────────────────────────────
test("las vías cuentan emails vigentes, no claves: formularios, quemados y claves viejas no suman", () => {
  const L1 = { domain: "x.pe", emails: ["ok@x.pe"], email_sources: { "__contact_form_1__": { source: "contact_form", url: "https://x.pe/contacto" }, "muerta@x.pe": "rol_mx", "ok@x.pe": "rol_mx" } };
  // Un rescate sobre un lead viejo: su clave generic vieja no es de estos 7 días.
  const L2 = { domain: "viejo.pe", emails: ["ventas@viejo.pe"], email_sources: { "info@viejo.pe": "generic", "ventas@viejo.pe": "rol_mx" } };
  const L3 = { domain: "y.pe", emails: ["a@y.pe"], email_sources: { "a@y.pe": "pattern" } };
  const r = W._viasDeEmailInforme({
    cohorte: [L1, L3, L2, L1],   // L1 llega como alta y como rescate: se cuenta una vez
    respaldo: [L1],
    // muerta@x.pe ya no está en `emails` (rebotó) y su envío no trae la vía: la da el respaldo.
    enviados: [{ email_to: "muerta@x.pe", details: {} }, { email_to: "ok@x.pe", details: { source: "rol_mx" } }],
    malos: [{ email: "muerta@x.pe", evidencia: "rebote_smtp" }],
    verificados: [],
  });
  strictEqual(r.via.rol_mx.n, 2, "ok@x.pe y ventas@viejo.pe; la quemada no cuenta como encontrada");
  strictEqual(r.via.rol_mx.env, 2);
  strictEqual(r.via.rol_mx.rebotes, 1, "el rebote se atribuye aunque la dirección ya no esté en emails");
  ok(!r.via.contact_form, "un formulario de contacto no es un email");
  ok(!r.via.generic, "la clave vieja de un lead rescatado no es un email de estos 7 días");
  strictEqual(r.formularios, 1);
  const t = r.lineas.join("\n");
  match(t, /🔴 rol_mx\s+2 email\(s\) · rebotaron\s+1 \(50%\) de 2 enviados/);
  match(t, /· pattern\s+1 email\(s\) · rebotaron\s+0 \(0%\) de 0 enviados/);
  ok(!/✅ pattern/.test(t), "una vía sin envíos no se midió: no lleva ✅");
});

// ── C35: envíos salteados ───────────────────────────────────────────────────────────────────
test("los salteados cuentan eventos y dominios distintos", () => {
  const g = W._agruparSalteados([
    { reason: "mv_dudoso", domain: "m.com" }, { reason: "mv_dudoso", domain: "M.com" }, { reason: "mv_dudoso", domain: "m.com" },
    { reason: "blocklist:crm-no-recontactar:x", domain: "b.com" },
    { reason: "no_email_after_enrichment", domain: "" }, { reason: "no_email_after_enrichment", domain: null },
  ]);
  strictEqual(g.total, 6);
  strictEqual(g.dominios, 2);
  deepStrictEqual(g.top[0], { motivo: "mv_dudoso", n: 3, dominios: 1, ej: ["m.com"] });
  ok(g.top.some(x => x.motivo === "blocklist:crm-no-recontactar"), JSON.stringify(g.top));
  const sinDom = g.top.find(x => x.motivo === "no_email_after_enrichment");
  strictEqual(sinDom.n, 2);
  strictEqual(sinDom.dominios, 0, "una fila sin dominio cuenta como evento, no como dominio");
  deepStrictEqual(W._agruparSalteados(null), { total: 0, dominios: 0, top: [] });
  ok(/select=action,reason,domain/.test(cuerpoDe("parteDelDia")), "el parte también tiene que poder contar dominios en 'Los descartes de hoy'");
});

// ── El cableado: el parte usa las mismas reglas ─────────────────────────────────────────────
test("el parte usa las mismas reglas que el resumen (ninguna copia adentro de un bucle)", () => {
  const p = cuerpoDe("parteDelDia");
  ok(p.includes("_claveFuenteCola(f)") && p.includes("_estadoColaInforme(f.status)"), "3b tiene que separar grupos y pospuestas");
  ok(p.includes("toolbar_csv_queue?processed_at=gte.${_sem}&select=source,status,uploaded_by&order=id") && /_traerTodo\(\s*`\$\{SUPABASE_URL\}\/rest\/v1\/toolbar_csv_queue\?processed_at=gte\.\$\{_sem\}/.test(p),
    "3b paginado: un fetch suelto se corta en 1.000 filas");
  ok(!p.includes('.replace(/^sellers_json$/, "sellers")'), "el parte no puede tener su propia copia del nombre de fuente");
  ok(p.includes("_agruparRechazosCola(_rech)"), "'Por qué se rechaza' agrupa con la regla común");
  ok(p.includes("_viasDeEmailInforme(") && p.includes("toolbar_review_queue?email_found_at=gte.${_ultimos7}"), "las vías leen los rescates sin el filtro de 30 días");
});

// ── El mail armado de punta a punta ─────────────────────────────────────────────────────────
const resp = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: (k) => (k.toLowerCase() === "content-range" ? `0-0/${Array.isArray(body) ? body.length : 0}` : null) },
  json: async () => body, text: async () => JSON.stringify(body),
});
const STOCK = [
  { domain: "s1.com", email_ultimo_motivo: "waf_nos_bloqueo:cloudflare", email_intentos: 2 },
  { domain: "s2.com", email_ultimo_motivo: "waf_nos_bloqueo:akamai", email_intentos: 1 },
  { domain: "s3.com", email_ultimo_motivo: null, email_intentos: 0 },
  { domain: "s4.com", email_ultimo_motivo: null, email_intentos: 3 },
  { domain: "s5.com", email_ultimo_motivo: "a", email_intentos: 1 },
  { domain: "s6.com", email_ultimo_motivo: "b", email_intentos: 1 },
  { domain: "s7.com", email_ultimo_motivo: "c", email_intentos: 1 },
  { domain: "s8.com", email_ultimo_motivo: "d", email_intentos: 1 },
];
const COMENTARIO = "El sitio figura con tráfico principal en Estados Unidos. " + "Regla del user (27/08): aunque tenga ads.txt y buen tráfico, lo anglo no se procesa. ".repeat(8);
const CONFIG = [
  { key: "agent_enabled_users", value: '["sales@adeqmedia.com","dhorovitz@adeqmedia.com"]' },
  { key: "agent_max_per_day", value: "20" },
  { key: "salud_resumen_pendiente", value: JSON.stringify([{ clave: "x", titulo: "algo para mirar", severidad: "warn", cuerpo: "detalle" }]) },
];
function enrutador(registro) {
  return async (url, opts = {}) => {
    const u = String(url);
    registro.push({ u, m: (opts.method || "GET").toUpperCase(), b: String(opts.body || "") });
    if (u.includes("toolbar_config?select=key,value")) return resp(CONFIG);
    if (u.includes("toolbar_csv_queue?processed_at=gte.")) return resp(COLA);
    if (u.includes("toolbar_review_queue?created_at=gte.") && u.includes("select=source,domain")) return resp(ALTAS24);
    if (u.includes("toolbar_review_queue?domain=in.(")) return resp(PROS_REACT.filter(p => u.includes(p.domain)));
    if (u.includes("toolbar_review_queue?email_found_at=gte.") && u.includes("select=emails,email_sources")) return resp([
      { emails: ["ads@diario.com.ar"], email_sources: { "ads@diario.com.ar": "rol_mx", "info@diario.com.ar": { source: "generic" } } },
      { emails: ["juan@x.com", "info@x.com"], email_sources: { "info@x.com": "generic", "juan@x.com": "pattern" } },
    ]);
    if (u.includes("toolbar_review_queue?email_found_at=gte.")) return resp([{ id: 1 }, { id: 2 }]);
    if (u.includes("toolbar_review_queue?status=eq.pending&and=(or(emails.is.null,emails.eq.%5B%5D))")) return resp(STOCK);
    if (u.includes("toolbar_diag_descartes?created_at=gte.")) return resp([
      { domain: "otro.com", motivo: "tipo_no_prospectable:banco", comentario: "no es de los ejemplos" },
      { domain: "us.com", motivo: "geo_excluida:United States", comentario: COMENTARIO },
    ]);
    if (u.includes("toolbar_agent_actions?action=eq.skipped")) return resp([
      { reason: "mv_dudoso", domain: "m.com" }, { reason: "mv_dudoso", domain: "m.com" }, { reason: "mv_dudoso", domain: "m.com" },
      { reason: "blocklist:crm-no-recontactar:x", domain: "b.com" },
    ]);
    if (u.includes("googleapis.com/oauth2") || u.includes("oauth2.googleapis.com")) return resp({ access_token: "falso" });
    if (u.includes("gmail.googleapis.com")) return resp({ id: "msg-falso" });
    return resp([]);
  };
}

test("el boletín: embudo por grupo, cola que pospone, vía del rescate, y las filas compartidas con el resumen", async () => {
  const registro = [];
  globalThis.__fetchFalso = enrutador(registro);
  const compartido = {};
  const lineas = await W._boletinPorSeccion("token-falso", { compartido });
  const t = lineas.join("\n");
  ok(!/boletín por sección falló/.test(t), t);
  match(t, /La cola procesó 7 \(sin contar 2 pospuestas/);
  match(t, /feeders: similar 2→1 \(2 pospuestas\)/);
  match(t, /imports de MBs: sellers 2→0/);
  match(t, /re-trabajo[^:]*: sellers 0→0 \(1 congeladas\)/);
  match(t, /del agente[^:]*: agent 1→1/);
  match(t, /similar→crm_reciclado 1/);
  match(t, /Drenó 8 en 24h \(y pospuso 2: vuelven a la cola, no salieron\)/);
  match(t, /la cola corre pero pospone/);
  ok(!/el ciclo de la cola no está corriendo/.test(t), "una cola que pospone no es un ciclo parado");
  match(t, /DESCARTES DE LA COLA, abajo/);
  match(t, /Quién los encontró: rol_mx 1 · pattern 1/);
  ok(!/Quién los encontró:[^\n]*generic/.test(t), "generic no es una vía de rescate");
  match(t, /Emails nuevos por vía \(24h\): apollo 0 · serper_persona 0 · rol_mx 1 · pattern 1 · google_contact 0 · scrape 0 · informer 0 · social 0/);
  match(t, /quedan 8 sin email/);
  deepStrictEqual(compartido.cola, COLA, "el resumen tiene que recibir exactamente las filas que contó el boletín");
  // Las reactivadas se buscan en Prospects en un solo lote.
  ok(registro.some(r => r.u.includes("toolbar_review_queue?domain=in.(reac.com,ag.com)")), registro.map(r => r.u).filter(u => u.includes("domain=in")).join(" | "));
});

test("con la cola vacía no se pide un domain=in.() vacío", async () => {
  const registro = [];
  const base = enrutador(registro);
  globalThis.__fetchFalso = async (url, opts) => (String(url).includes("toolbar_csv_queue?processed_at=gte.") ? resp([]) : base(url, opts));
  await W._boletinPorSeccion("token-falso");
  ok(!registro.some(r => r.u.includes("domain=in.()")), "un in.() vacío es un 400 de PostgREST");
});

test("el resumen de salud: descartes de la cola que cuadran, stock sin email entero y salteados con dominios", async () => {
  const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON = JSON.stringify({
    client_email: "resumen-test@falso.iam.gserviceaccount.com",
    private_key: privateKey.export({ type: "pkcs8", format: "pem" }),
  });
  const registro = [];
  globalThis.__fetchFalso = enrutador(registro);
  await W.enviarResumenSalud("token-falso");
  const envio = registro.find(r => /gmail\.googleapis\.com/.test(r.u) && r.m === "POST" && /"raw"/.test(r.b));
  ok(envio, `el resumen no llegó a Gmail. Últimos pedidos: ${registro.slice(-3).map(r => r.u.slice(0, 90)).join(" | ")}`);
  const mime = Buffer.from(JSON.parse(envio.b).raw.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
  const qp = mime.replace(/=\r?\n/g, "");
  const bytes = [];
  for (let i = 0; i < qp.length; i++) {
    if (qp[i] === "=" && /^[0-9A-F]{2}$/i.test(qp.slice(i + 1, i + 3))) { bytes.push(parseInt(qp.slice(i + 1, i + 3), 16)); i += 2; }
    else bytes.push(...Buffer.from(qp[i], "utf8"));
  }
  const texto = Buffer.from(bytes).toString("utf8");
  const plano = texto.slice(0, texto.indexOf("Content-Type: text/html"));
  ok(!/No se pudo armar el detalle de errores/.test(plano), plano.slice(plano.indexOf("ERRORES CONCRETOS"), plano.indexOf("ERRORES CONCRETOS") + 600));
  ok(!/DESCARTES DEL DESCUBRIMIENTO/.test(plano), "el bloque viejo leía 3 motivos de ~20");
  // Cuadra con "La cola procesó 7 · llegaron 3" del boletín: 3 descartes + 1 ya estaba + 0 errores.
  match(plano, /DESCARTES DE LA COLA \(24h\) — de 7 procesados, 3 llegaron a Prospects, 3 se descartaron, 1 ya estaban y 0 dieron error/);
  match(plano, /· trafico_bajo_piso → 2\. Ej: mb1\.com, mb2\.com/);
  match(plano, /· deprio-geo:United States → 1\. Ej: us\.com/);
  match(plano, /1 vuelven mañana · 1 vuelven a la cola · 1 congelados/);
  // El comentario del caso es el de un dominio del top (no el primero de la tabla), cortado en un punto.
  const mEj = plano.match(/\(por ejemplo, us\.com \[geo_excluida:United States\]: ([^\r\n]*)\)\r?\n/);
  ok(mEj, "el comentario tiene que ser de un dominio de los ejemplos, no el primero de la tabla");
  const comentario = mEj[1];
  ok(comentario.length <= 400 && comentario.length > 200, `largo ${comentario.length}`);
  ok(comentario.endsWith("."), `cortado a la mitad: …${comentario.slice(-40)}`);
  // Stock sin email: el mismo número que "quedan N" y todos los leads con renglón.
  match(plano, /quedan 8 sin email/);
  match(plano, /SIN EMAIL — hay 8 pendientes sin email \(lo acumulado, no es de hoy\): 6 ya tienen motivo del barrido de emails y 2 todavía no/);
  match(plano, /· waf_nos_bloqueo → 2/);
  match(plano, /· otros motivos → 2/);
  // Salteados: eventos y dominios.
  match(plano, /ENVÍOS SALTEADOS \(4 en 24h, 2 dominios\)/);
  match(plano, /· mv_dudoso → 3 \(1 dominio\)\. Ej: m\.com/);
  match(plano, /· blocklist:crm-no-recontactar → 1\. Ej: b\.com/);
  // Una sola lectura de la cola en 24h: el resumen reusa la del boletín.
  strictEqual(registro.filter(r => r.u.includes("toolbar_csv_queue?processed_at=gte.")).length, 1, "DESCARTES DE LA COLA tiene que contar las mismas filas que el boletín");
});
