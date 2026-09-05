// Los dos mails diarios tienen que ARMARSE de punta a punta sin tirar. (2026-09-04)
//
// El parte del día (parteDelDia) no salió ni una vez entre el 25/08 y el 04/09: una constante
// (`_ANGLO_PARTE`) se usaba adentro de un bucle 80 líneas antes de declararse, el
// ReferenceError caía en el catch del loop y quedaba en una línea de log. Este test carga el
// worker entero con un `fetch` falso que contesta la base y Gmail con datos inventados
// —incluida una fila del historial, que es la que disparaba el error— y exige que el parte
// llegue hasta el envío. Lo mismo para el boletín por sección del resumen de salud.
//
// Run: npm test
import { test } from "node:test";
import { ok } from "node:assert";
import { cargarWorker } from "./_worker-exportado.mjs";

const { parteDelDia, _boletinPorSeccion, _traerTodo } = await cargarWorker(["parteDelDia", "_boletinPorSeccion", "_traerTodo"], { fetchFalso: true });

const resp = (body, { status = 200, headers = {} } = {}) => ({
  ok: status >= 200 && status < 300, status,
  headers: { get: (k) => headers[k.toLowerCase()] ?? (k.toLowerCase() === "content-range" ? `0-0/${Array.isArray(body) ? body.length : 0}` : null) },
  json: async () => body, text: async () => JSON.stringify(body),
});
const ahora = new Date().toISOString();
const CONFIG = [
  { key: "agent_enabled_users", value: '["sales@adeqmedia.com","dhorovitz@adeqmedia.com"]' },
  { key: "agent_max_per_day", value: "20" },
  { key: "monday_sync_ultimo", value: JSON.stringify({ fecha: ahora.slice(0, 10), en_board: 100, contactados_recientes: 5, ya_en_cola: 3, reprospectables: 40, encolados: 40, techo: 700 }) },
];

function enrutador(registro) {
  return async (url, opts = {}) => {
    const u = String(url);
    registro.push({ u, m: (opts.method || "GET").toUpperCase(), b: String(opts.body || "") });
    if (u.includes("toolbar_config?select=key,value")) return resp(CONFIG);
    if (u.includes("toolbar_historial?source=eq.manual&date=eq.")) return resp([{
      domain: "diario-ejemplo.com.ar", media_buyer: "sales@adeqmedia.com", email: "ventas@diario-ejemplo.com.ar",
      geo: "United States", is_new: true, page_views: 500000, created_at: ahora,
    }]);
    if (u.includes("toolbar_agent_actions") && u.includes("action=eq.sent")) return resp([{ user_email: "sales@adeqmedia.com", email_to: "a@b.com" }]);
    if (u.includes("toolbar_health?job=in.")) return resp([{ job: "barrido_no_publisher", last_status: "ok", last_detail: "lote 25: revisados 18", last_run_at: ahora }]);
    if (u.includes("googleapis.com/oauth2") || u.includes("oauth2.googleapis.com")) return resp({ access_token: "falso" });
    if (u.includes("gmail.googleapis.com")) return resp({ id: "msg-falso" });
    return resp([]);
  };
}

test("el parte del día se arma entero y llega a Gmail con una fila de historial (regresión del 25/08)", async () => {
  const registro = [];
  globalThis.__fetchFalso = enrutador(registro);
  await parteDelDia("token-falso", { forzar: true });
  // Con el bug, la excepción salía de parteDelDia ENTERA: ni Gmail ni ping. Con el arreglo, el
  // cuerpo se arma y se intenta enviar; sin credenciales reales el envío falla y queda el ping
  // `parte_diario` en rojo — que es exactamente la prueba de que todo lo anterior corrió.
  const llegoAGmail = registro.some(r => /googleapis\.com/.test(r.u));
  const ping = registro.find(r => /toolbar_health/.test(r.u) && /"job":"parte_diario"/.test(r.b));
  ok(llegoAGmail || ping, `el parte no llegó ni a Gmail ni al ping: algo tiró antes de armarse. Pedidos: ${registro.length}; últimos: ${registro.slice(-3).map(r => r.u.slice(0, 80)).join(" | ")}`);
  if (!llegoAGmail && ping) {
    const detalle = (ping.b.match(/"last_detail":"([^"]*)"/) || [])[1] || "";
    ok(!/is not defined|before initialization|Cannot read/.test(detalle), `el parte se armó pero tiró un error de código: ${detalle}`);
  }
});

test("el boletín por sección del resumen de salud se arma sin fallar", async () => {
  const registro = [];
  globalThis.__fetchFalso = enrutador(registro);
  const lineas = await _boletinPorSeccion("token-falso");
  ok(Array.isArray(lineas) && lineas.length > 5, "el boletín vino vacío");
  ok(!lineas.some(l => /boletín por sección falló/.test(l)), `el boletín falló: ${lineas.find(l => /falló/.test(l))}`);
  for (const seccion of ["ENVÍO", "DESCUBRIMIENTO", "BÚSQUEDA DE EMAILS", "COLA", "CICLOS FINALIZADOS", "EL PLAN DEL 04/09"]) {
    ok(lineas.some(l => l.includes(seccion)), `falta la sección ${seccion}`);
  }
  ok(!lineas.some(l => /ENVÍO \(hoy\)/.test(l)), "ENVÍO tiene que juzgar AYER completo, no 'hoy' a la mañana");
});

test("_traerTodo pagina de a 1000 y no devuelve una lista parcial como si fuera entera", async () => {
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const rango = String(opts.headers?.Range || "");
    pedidos.push(rango);
    const desde = parseInt(rango.split("-")[0], 10) || 0;
    const total = 2300;
    if (desde >= total) return resp([], { status: 416 });
    const n = Math.min(1000, total - desde);
    return resp(Array.from({ length: n }, (_, i) => ({ id: desde + i })), { status: 206 });
  };
  const filas = await _traerTodo("http://x/rest/v1/tabla?select=id&limit=5000", {});
  ok(filas.length === 2300, `trajo ${filas.length}, esperaba 2300`);
  ok(pedidos[0] === "0-999" && pedidos[1] === "1000-1999" && pedidos[2] === "2000-2999", `rangos: ${pedidos.join(", ")}`);
  // Una página que falla → null, nunca una lista a medias.
  globalThis.__fetchFalso = async (url, opts = {}) => (String(opts.headers?.Range || "").startsWith("1000") ? resp([], { status: 500 }) : resp(Array.from({ length: 1000 }, (_, i) => ({ id: i })), { status: 206 }));
  ok((await _traerTodo("http://x/rest/v1/tabla?select=id", {})) === null, "con una página rota tiene que devolver null");
});
