// Los renglones que dicen si las mejoras del 08/09 funcionan. (2026-09-08, pedido del user)
//
// "¿Cómo analizamos si estas mejoras funcionan en los próximos días?" — el parte medía si una
// fuente PASA EL FILTRO, que no es lo mismo que traer una web buena. Una web buena recorre
// cuatro pasos: entra a Prospects, consigue email, se le escribe, no rebota. Este test arma el
// parte REAL con un fixture y verifica que los seis bloques nuevos salgan con los números
// correctos — si alguien los rompe, falla acá y no dentro de tres semanas mirando un mail.
//
// Run: npm test
import { test } from "node:test";
import { ok, match } from "node:assert";
import { generateKeyPairSync } from "node:crypto";
import { cargarWorker } from "./_worker-exportado.mjs";

process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";
const { parteDelDia } = await cargarWorker(["parteDelDia"], { fetchFalso: true });

const hoy = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Madrid", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
const ahora = new Date().toISOString();
const resp = (body, { status = 200, total } = {}) => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: (k) => (k.toLowerCase() === "content-range" ? `0-0/${total ?? (Array.isArray(body) ? body.length : 0)}` : null) },
  json: async () => body, text: async () => JSON.stringify(body),
});

// El fixture: dos fuentes con destinos opuestos. `wikidata` trae poco y se contacta todo;
// `crux` trae mucho, consigue email en la mitad y no se contacta nada. Es exactamente la
// decisión que el user tiene que poder tomar leyendo el mail.
const ALTAS = [
  // wikidata: 2 altas, las 2 con email, las 2 contactadas → 100%
  { domain: "larepublica.pe", source: "auto_feeder_wikidata", emails: ["dir@larepublica.pe"], email_sources: { "dir@larepublica.pe": "pattern" }, geo: "Perú", geos_all: ["PE"], created_at: ahora },
  { domain: "atv.pe",         source: "auto_feeder_wikidata", emails: ["info@atv.pe"],        email_sources: { "info@atv.pe": "scrape" },        geo: "Perú", geos_all: ["PE"], created_at: ahora },
  // crux: 4 altas, 2 con email, 0 contactadas → 0%
  { domain: "tienda1.pe", source: "auto_feeder_crux", emails: ["a@tienda1.pe"], email_sources: { "a@tienda1.pe": "rol_mx" }, geo: "Perú", geos_all: ["PE"], created_at: ahora },
  { domain: "tienda2.pe", source: "auto_feeder_crux", emails: ["b@tienda2.pe"], email_sources: { "b@tienda2.pe": "pattern" }, geo: "Perú", geos_all: ["PE"], created_at: ahora },
  { domain: "tienda3.pe", source: "auto_feeder_crux", emails: [], email_sources: {}, geo: "Perú", geos_all: ["PE"], created_at: ahora },
  { domain: "tienda4.pe", source: "auto_feeder_crux", emails: [], email_sources: {}, geo: "Perú", geos_all: ["PE"], created_at: ahora },
  // Una anglo, para que el bloque de GEO tenga algo que denunciar
  { domain: "usasite.com", source: "auto_feeder_sellers", emails: [], email_sources: {}, geo: "Estados Unidos", geos_all: ["US"], created_at: ahora },
];

function enrutador(registro) {
  return async (url, opts = {}) => {
    const u = String(url);
    registro.push({ u, m: (opts.method || "GET").toUpperCase(), b: String(opts.body || "") });
    if (u.includes("toolbar_config?select=key,value")) return resp([{ key: "agent_enabled_users", value: '["sales@adeqmedia.com"]' }]);
    // El embudo y la GEO leen esta consulta (trae source/emails/geo); el conteo de altas del
    // día usa la misma URL, así que el fixture sirve a los dos.
    if (u.includes("toolbar_review_queue?created_at=gte.")) return resp(ALTAS);
    // Sólo las dos de wikidata fueron contactadas.
    if (u.includes("toolbar_sendtrack?send_date=gte.")) return resp([{ domain: "larepublica.pe" }, { domain: "atv.pe" }]);
    // Un rebote, de la dirección que encontró el PATRÓN en crux → tiene que atribuirse a `pattern`.
    if (u.includes("action=eq.bounce_detected")) return resp([{ details: { failed_email: "b@tienda2.pe" } }]);
    if (u.includes("toolbar_csv_queue?processed_at=gte.") && u.includes("status=in.(skipped,next_day)")) return resp([
      { status: "skipped", error_message: "not_publisher: sin_ads_txt" },
      { status: "skipped", error_message: "not_publisher: sin_ads_txt" },
      { status: "skipped", error_message: "not_publisher: haiku_corp" },
      { status: "next_day", error_message: "reintentar: ads_txt_no_verificable_y_rubro_dudoso:haiku_bank" },
    ]);
    if (u.includes("toolbar_csv_queue?processed_at=gte.")) return resp([{ source: "auto_feeder_crux", status: "done" }]);
    if (u.includes("toolbar_traffic_cache") && u.includes("noData")) return resp([], { total: u.includes("fetched_at") ? 7 : 1415 });
    if (u.includes("toolbar_mv_results?created_at=gte.")) return resp([
      { result: "ok" }, { result: "ok" }, { result: "catch_all" }, { result: "invalid" },
    ]);
    if (u.includes("googleapis.com/oauth2") || u.includes("oauth2.googleapis.com")) return resp({ access_token: "falso" });
    if (u.includes("gmail.googleapis.com")) return resp({ id: "msg-falso" });
    return resp([]);
  };
}

const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
process.env.GOOGLE_SERVICE_ACCOUNT_JSON = JSON.stringify({
  client_email: "parte-test@falso.iam.gserviceaccount.com",
  private_key: privateKey.export({ type: "pkcs8", format: "pem" }),
});
const registro = [];
globalThis.__fetchFalso = enrutador(registro);
await parteDelDia("token-falso", { forzar: true });
const envio = registro.find(r => /gmail\.googleapis\.com/.test(r.u) && r.m === "POST" && /"raw"/.test(r.b));
ok(envio, `el parte no llegó a Gmail. Últimos pedidos: ${registro.slice(-3).map(r => r.u.slice(0, 90)).join(" | ")}`);
const raw = JSON.parse(envio.b).raw;
const mime = Buffer.from(raw.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
const qp = mime.replace(/=\r?\n/g, "");
const bytes = [];
for (let i = 0; i < qp.length; i++) {
  if (qp[i] === "=" && /^[0-9A-F]{2}$/i.test(qp.slice(i + 1, i + 3))) { bytes.push(parseInt(qp.slice(i + 1, i + 3), 16)); i += 2; }
  else bytes.push(...Buffer.from(qp[i], "utf8"));
}
const html = Buffer.from(bytes).toString("utf8");
/** El texto que sigue a una etiqueta, sin HTML. */
const despuesDe = (etiqueta, n = 320) => {
  const i = html.indexOf(etiqueta);
  ok(i >= 0, `no encontré "${etiqueta}" en el mail`);
  return html.slice(i + etiqueta.length, i + etiqueta.length + n).replace(/<[^>]+>/g, " ").replace(/&nbsp;/g, " ").replace(/\s+/g, " ");
};

test("el embudo separa la fuente que se contacta de la que sólo entra", () => {
  const t = despuesDe("El embudo a 30 días", 700);
  // wikidata: 2 entraron, 2 con email (100%), 2 contactadas (100%) → verde
  match(t, /wikidata\s+entraron\s+2 · con email\s+2 \(100%\) · contactadas\s+2 \(100%\)/, t);
  // crux: 4 entraron, 2 con email (50%), 0 contactadas → rojo. Es la fuente a revisar.
  match(t, /crux\s+entraron\s+4 · con email\s+2 \( 50%\) · contactadas\s+0 \(0%\)/, t);
  ok(t.includes("✅") && t.includes("🔴"), `las señales tienen que distinguir las dos: ${t}`);
});

test("la GEO de las altas dice cuánto anglo entra, en porcentaje", () => {
  const t = despuesDe("GEO de las altas", 900);
  match(t, /foco \(LATAM\/EU\/África\/Asia\)\s+6 \(86%\)/, t);
  match(t, /anglo \(US\/CA\/UK\/AU\/NZ\/IE\)\s+1 \(14%\)/, t);
  ok(/poco y bajando/.test(t), "tiene que aclarar que la meta no es cero: el filtro anglo es blando a propósito");
});

test("cada vía de email muestra sus rebotes, atribuidos a la dirección exacta que falló", () => {
  const t = despuesDe("y cuáles rebotan", 520);
  // `pattern` encontró 2 direcciones y una rebotó → 50%, en rojo. Es la vigilancia de E1.
  match(t, /pattern\s+2 email\(s\) · rebotaron\s+1 \(50%\)/, t);
  match(t, /scrape\s+1 email\(s\) · rebotaron\s+0 \(0%\)/, t);
  match(t, /rol_mx\s+1 email\(s\) · rebotaron\s+0 \(0%\)/, t);
});

test("los motivos de rechazo se agrupan, y los reintentables NO se cuentan como descarte", () => {
  const t = despuesDe("Por qué se rechaza", 420);
  match(t, /sin_ads_txt\s+2 \(67%\)/, t);
  match(t, /haiku_corp\s+1 \(33%\)/, t);
  match(t, /↻ 1 NO se descartaron: vuelven mañana/, t);
});

test("la caché negativa informa el stock y lo de hoy", () => {
  const t = despuesDe("lo que ya no se vuelve a pagar", 900);
  match(t, /Dominios sin datos guardados\s+1415/, t);
  match(t, /Nuevos hoy\s+7/, t);
});

test("MillionVerifier informa qué contestó, para vigilar la inferencia por patrón", () => {
  const t = despuesDe("qué contestó hoy", 300);
  match(t, /ok\s+2/, t);
  match(t, /catch_all\s+1/, t);
  match(t, /invalid\s+1/, t);
});
