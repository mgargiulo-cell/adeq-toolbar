// El parte del 07/09, revisado contra la base: siete renglones decían algo que no era.
//
//   · "Las dio un MB a mano: sales 105 · dhorovitz 42 · mgargiulo 13" — eran las 160 filas del
//     reciclador de ciclos finalizados, que firma con el ejecutivo de la ficha.
//   · "Emails encontrados hoy (no tenían): 544" — la columna `email_found_at` decía 14; el
//     contador sumaba direcciones extra sobre leads que ya tenían.
//   · "Ya contactados (30 días): 20" — eran los envíos de ese mismo día del mismo MB.
//   · "Fuera del foco (anglo): 68-85%" — el popup guardaba GEO=US para todo lo que no estaba en
//     un mapa de 15 TLDs, y contaba Gmail, YouTube y claude.ai como "URLs abiertas".
//   · "autopilot trajo 1411 · pasaron 0 (0%) … gasta créditos para nada" — 1.368 congeladas del
//     barrido, sin una sola llamada a RapidAPI.
//   · "NO hay descartes registrados — el agente no llegó a intentarlo" — llegó cinco veces y
//     cinco veces cortó el turno en silencio porque el CRM había llenado los 25/h del buzón.
//   · "Monday finalizados reciclados" — Monday está apagado desde el 02/09.
//
// Este test arma el parte REAL con esos mismos datos y lee el mail que saldría.
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateKeyPairSync } from "node:crypto";
import { cargarWorker } from "./_worker-exportado.mjs";

const aqui   = path.dirname(fileURLToPath(import.meta.url));
const indexJs = fs.readFileSync(path.join(aqui, "..", "index.js"), "utf8");
const popupJs = fs.readFileSync(path.join(aqui, "..", "..", "popup", "popup.js"), "utf8");

const { parteDelDia } = await cargarWorker(["parteDelDia"], { fetchFalso: true });

const hoy = new Date().toISOString().slice(0, 10);
const ahora = new Date().toISOString();
const resp = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: (k) => (k.toLowerCase() === "content-range" ? `0-0/${Array.isArray(body) ? body.length : 0}` : null) },
  json: async () => body, text: async () => JSON.stringify(body),
});
const CONFIG = [
  { key: "agent_enabled_users", value: '["sales@adeqmedia.com","dhorovitz@adeqmedia.com"]' },
  { key: "agent_max_per_day", value: "20" },
  { key: "polish_enriquecidos_hoy", value: `${hoy}:574` },   // el contador inflado del 07/09
];

/** El mail tal como lo vería el user: base64url → MIME → quoted-printable → texto. */
function decodificarMail(cuerpoJson) {
  const raw = JSON.parse(cuerpoJson).raw;
  const mime = Buffer.from(raw.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
  const qp = mime.replace(/=\r?\n/g, "");
  const bytes = [];
  for (let i = 0; i < qp.length; i++) {
    if (qp[i] === "=" && /^[0-9A-F]{2}$/i.test(qp.slice(i + 1, i + 3))) { bytes.push(parseInt(qp.slice(i + 1, i + 3), 16)); i += 2; }
    else bytes.push(...Buffer.from(qp[i], "utf8"));
  }
  return Buffer.from(bytes).toString("utf8");
}

function enrutador(registro) {
  return async (url, opts = {}) => {
    const u = String(url);
    registro.push({ u, m: (opts.method || "GET").toUpperCase(), b: String(opts.body || "") });
    if (u.includes("toolbar_config?select=key,value")) return resp(CONFIG);
    // Lo que abrió Agustina: una web real (NL, con email, escrita HOY) y Gmail con el GEO=US
    // que el popup viejo le ponía a todo.
    if (u.includes("toolbar_historial?source=eq.manual&date=eq.")) return resp([
      { domain: "wielerrevue.nl", media_buyer: "sales@adeqmedia.com", email: "redactie@wielerrevue.nl", geo: "NL", is_new: true, page_views: 2442128, created_at: ahora },
      { domain: "mail.google.com", media_buyer: "sales@adeqmedia.com", email: "", geo: "US", is_new: true, page_views: 0, created_at: ahora },
    ]);
    if (u.includes("toolbar_sendtrack?select=domain,send_date")) return resp([{ domain: "wielerrevue.nl", send_date: hoy }]);
    if (u.includes("toolbar_agent_actions") && u.includes("action=eq.sent")) return resp([{ user_email: "sales@adeqmedia.com" }]);
    if (u.includes("toolbar_agent_actions") && u.includes("action=in.(skipped")) return resp([
      { action: "cycle_cupo_casilla", reason: "casilla_llena:25/25" }, { action: "cycle_cupo_casilla", reason: "casilla_llena:25/25" },
    ]);
    // Las altas: 160 del reciclador firmadas por el ejecutivo, y una del feeder.
    if (u.includes("toolbar_review_queue?created_at=gte.")) return resp([
      ...Array.from({ length: 3 }, () => ({ source: "monday_refresh", emails: ["a@b.com"], created_by: "sales@adeqmedia.com" })),
      { source: "similar", emails: [], created_by: "worker@autofeeder" },
    ]);
    if (u.includes("toolbar_review_queue?email_found_at=gte.")) return resp(Array.from({ length: 14 }, (_, i) => ({ id: i })));
    if (u.includes("toolbar_csv_queue?processed_at=gte.")) return resp([
      { source: "autopilot", status: "frozen" }, { source: "autopilot", status: "frozen" }, { source: "autopilot", status: "frozen" },
      { source: "auto_feeder_similar", status: "done" }, { source: "auto_feeder_similar", status: "skipped" },
      { source: "auto_feeder_monday", status: "done" },
    ]);
    if (u.includes("toolbar_health?job=in.")) return resp([]);
    if (u.includes("googleapis.com/oauth2") || u.includes("oauth2.googleapis.com")) return resp({ access_token: "falso" });
    if (u.includes("gmail.googleapis.com")) return resp({ id: "msg-falso" });
    return resp([]);
  };
}

// Una cuenta de servicio falsa con una clave RSA de verdad: el enviador firma el JWT antes de
// pedir el token, y sin clave se cae antes de armar el MIME — que es justo lo que queremos leer.
const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
process.env.GOOGLE_SERVICE_ACCOUNT_JSON = JSON.stringify({
  client_email: "parte-test@falso.iam.gserviceaccount.com",
  private_key: privateKey.export({ type: "pkcs8", format: "pem" }),
});
const registro = [];
globalThis.__fetchFalso = enrutador(registro);
await parteDelDia("token-falso", { forzar: true });
const envio = registro.find(r => /gmail\.googleapis\.com/.test(r.u) && r.m === "POST" && /"raw"/.test(r.b));
const _ping = registro.find(r => /toolbar_health/.test(r.u) && /"job":"parte_diario"/.test(r.b));
ok(envio, `el parte no llegó a Gmail. Ping: ${_ping ? _ping.b.slice(0, 400) : "(sin ping)"}. Últimos pedidos: ${registro.slice(-3).map(r => r.u.slice(0, 90)).join(" | ")}`);
const html = decodificarMail(envio.b);
/** El texto que sigue a una etiqueta, con el HTML quitado. */
const despuesDe = (etiqueta, n = 220) => {
  const i = html.indexOf(etiqueta);
  ok(i >= 0, `no encontré "${etiqueta}" en el mail`);
  return html.slice(i + etiqueta.length, i + etiqueta.length + n).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
};

test("el reciclado del CRM no se le atribuye a un MB como carga a mano", () => {
  const t = despuesDe("Las dio un MB a mano");
  ok(/ninguna/.test(t), `esperaba "ninguna", vino: ${t}`);
  ok(!/sales/.test(t), `las 3 filas monday_refresh firmadas por sales se contaron como suyas: ${t}`);
  const a = despuesDe("Las dio el agente", 400).trim();
  ok(/^4\b/.test(a), `las 4 altas (3 recicladas + 1 del feeder) tienen que ser del agente, vino: ${a.slice(0, 40)}`);
});

test("emails encontrados = la columna email_found_at, no el contador de polish", () => {
  const t = despuesDe("Emails encontrados hoy (no tenían)");
  ok(/\b14\b/.test(t) && !/574/.test(t), `esperaba 14 (columna), vino: ${t}`);
});

test("lo enviado HOY no cuenta como 'ya contactado hace poco'", () => {
  ok(!/Ya contactados \(30 días\)/.test(html), "wielerrevue.nl se le escribió hoy: no es un 'ya contactado' que hizo bien en no tocar");
});

test("Gmail no es una URL abierta, y una web holandesa no es anglo", () => {
  const t = despuesDe("Total abiertas", 400).trim();
  ok(/^1\b/.test(t), `mail.google.com no cuenta: esperaba 1 URL abierta, vino: ${t.slice(0, 40)}`);
  ok(!/Fuera del foco/.test(html), "con Gmail afuera y wielerrevue.nl en NL no hay nada fuera del foco");
});

test("las congeladas se cuentan aparte y no como 'gasta créditos para nada'", () => {
  ok(/autopilot[^\n]*trajo\s+3[^\n]*3 congelados/.test(html), "autopilot: 3 congeladas tienen que verse como congeladas, no como 0%");
  ok(/🧊 autopilot/.test(html), "una fuente que sólo tiene congeladas lleva 🧊, no 🔴");
  ok(/crm_reciclado/.test(html) && !/\bmonday\s+trajo/.test(html), "la fuente `monday` se llama por lo que es: el reciclado del CRM");
});

test("el corte de turno por casilla llena se dice con todas las letras", () => {
  ok(/casilla ya tenía 25\/25/.test(html), "el mail tiene que explicar que el cupo del buzón lo usó el CRM");
  ok(!/no llegó a intentarlo/.test(html), "ya no puede decir que el agente no lo intentó: lo intentó y cortó");
});

test("Monday ya no aparece en el mail", () => {
  ok(!/Monday/.test(html), `queda un "Monday" visible: …${html.slice(Math.max(0, html.indexOf("Monday") - 60), html.indexOf("Monday") + 40).replace(/<[^>]+>/g, " ")}…`);
});

// ── El popup: el GEO del historial ────────────────────────────────────────────────────────
function extraerFuncion(src, firma) {
  const i = src.indexOf(firma); ok(i >= 0, `no encontré ${firma}`);
  let n = 0;
  for (let k = src.indexOf("{", i); k < src.length; k++) {
    if (src[k] === "{") n++;
    else if (src[k] === "}" && --n === 0) return src.slice(i, k + 1);
  }
  throw new Error(`no cerró ${firma}`);
}
const _tld = extraerFuncion(popupJs, "const _TLD_GEO = ") + ";";
const _det = extraerFuncion(popupJs, "function detectGeo() {");
const detectGeoCon = (state) => new Function("state", `${_tld}\n${_det}\nreturn detectGeo();`)(state);

test("el GEO del historial: primero SimilarWeb, después el TLD, y nunca 'US' por defecto", () => {
  strictEqual(detectGeoCon({ domain: "cyclingpro.net", trafficData: { topCountries: [{ code: "it" }] } }), "IT", "manda el país medido");
  strictEqual(detectGeoCon({ domain: "wielerrevue.nl" }), "NL", "sin dato de tráfico, el TLD");
  strictEqual(detectGeoCon({ domain: "ciclismointernacional.com" }), "", "un .com sin tráfico medido es 'no sé', no Estados Unidos");
  strictEqual(detectGeoCon({ domain: "mail.google.com" }), "", "Gmail no es Estados Unidos");
});

// ── Los detectores estáticos de lo que no se puede probar sin la base ──────────────────
test("el corte por casilla llena queda registrado, y el parte lo lee", () => {
  const i = indexJs.indexOf("const _cupo = await cupoDisponibleCasilla(userEmail);");
  ok(i >= 0, "no encontré el chequeo del cupo compartido");
  const bloque = indexJs.slice(i, i + 1600);
  ok(/action: "cycle_cupo_casilla"/.test(bloque), "el `break` del cupo tiene que dejar una acción: era mudo");
  ok(/action=in\.\(skipped,cycle_no_candidates,cycle_gates,cycle_cupo_casilla\)/.test(indexJs), "el parte tiene que leer los cortes de turno, no sólo los descartes");
});

test("AutoGoogle retira las frases muertas y el top exige haber calificado", () => {
  ok(/toolbar_keyword_yield\?searches=gte\.2&qualified=gt\.0&order=qualified\.desc/.test(indexJs), "el top de frases tiene que exigir qualified>0");
  ok(/searches=gte\.10&or=\(qualified\.is\.null,qualified\.eq\.0\)/.test(indexJs), "tiene que traer las muertas (≥10 búsquedas, 0 leads)");
  ok(/!_muertas\.has\(p\)/.test(indexJs), "la exploración tiene que saltear las muertas");
});

test("el vigilante del agente alarma con un cuarto del objetivo, no sólo con cero", () => {
  ok(/_total < Math\.ceil\(_esperado \* 0\.25\)/.test(indexJs), "1 de 40 tiene que ser alarma");
  ok(/cupoDisponibleCasilla\(u\)/.test(indexJs), "la alerta tiene que decir cuánto cupo del buzón queda, por casilla");
});
