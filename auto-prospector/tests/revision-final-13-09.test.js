// Revisión final antes del deploy del 13/09: lo que confirmó la lente de resoluciones y la de humo, y que
// no cubría ninguna ronda.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import { generateKeyPairSync } from "node:crypto";
import { cargarWorker } from "./_worker-exportado.mjs";

process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";
const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 1024 });
process.env.GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON
  || JSON.stringify({ client_email: "sa@x.iam.gserviceaccount.com", private_key: privateKey.export({ type: "pkcs8", format: "pem" }) });

const resp = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: () => null },
  json: async () => body,
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
globalThis.__fetchFalso = async () => resp([]);
const W = await cargarWorker(["_boletinPorSeccion"], { fetchFalso: true });

// 100 envíos del agente de sales@; la lectura de rebotes la decide cada caso.
const ENVIOS = Array.from({ length: 100 }, (_, i) => ({ user_email: "sales@adeqmedia.com", email_to: `c${i}@medio${i}.com` }));
const pagina = (filas, opts) => {
  const d = Number(String((opts?.headers || {}).Range || "0-999").split("-")[0]);
  return resp(filas.slice(d, d + 1000));
};
async function bloqueRebotes({ envios, rebotes }) {
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url);
    if (u.includes("toolbar_agent_actions?action=in.(sent,secondary_sent)&details->>ui_origin=is.null")) return envios(opts);
    if (u.includes("toolbar_bounced_emails?bounced_at=gte.") && u.includes("select=email&order=email")) return rebotes(opts);
    return resp([]);
  };
  const t = (await W._boletinPorSeccion("t")).join("\n");
  const i = t.indexOf("REBOTES (7d");
  return i >= 0 ? t.slice(Math.max(0, i - 4), i + 300) : "";
}

test("boletín, REBOTES por buzón: una lectura caída dice 'no se pudo medir' en amarillo, nunca 0% en verde", async () => {
  const caidaRebotes = await bloqueRebotes({ envios: (o) => pagina(ENVIOS, o), rebotes: () => resp({ code: "57014", message: "statement timeout" }, { status: 400 }) });
  ok(/No se pudo medir: falló la lectura de rebotes/.test(caidaRebotes) && /🟡/.test(caidaRebotes), caidaRebotes);
  ok(!/Todos por debajo del 3%/.test(caidaRebotes) && !/0\.0% \(0\/100\)/.test(caidaRebotes), `una lectura caída se leía como cero rebotes: ${caidaRebotes}`);

  const caidaEnvios = await bloqueRebotes({ envios: () => resp({ message: "boom" }, { status: 500 }), rebotes: (o) => pagina([], o) });
  ok(/No se pudo medir: falló la lectura de envíos/.test(caidaEnvios), `sin envíos el bloque desaparecía sin decir nada: ${caidaEnvios}`);

  const bien = await bloqueRebotes({ envios: (o) => pagina(ENVIOS, o), rebotes: (o) => pagina(ENVIOS.slice(0, 10).map(e => ({ email: e.email_to })), o) });
  ok(/sales 10\.0% \(10\/100\)/.test(bien) && /🔴/.test(bien), `con las dos lecturas bien, el cálculo sigue igual: ${bien}`);
});

// Lente de humo: el contador de intentos se leía del motivo de ESTA vuelta (siempre 0) y la marca
// "adicional_manual" se pisaba en el primer fallo. Resultado: un adicional que no salía se reintentaba cada
// 6 horas para siempre, y el que salía al segundo intento no escribía su fila de medición (manual_extra).
test("Email Futuro / adicionales: la marca adicional_manual sobrevive a los reintentos y el tope de 3 se alcanza", async () => {
  const MB = "mgargiulo@adeqmedia.com";
  const CFG = { monday_enabled: "false", crm_propio_enabled: "true", monday_api_key: "" };
  const BODY = "Hola Juan,\n\nTe escribo porque vi que diarioa.com.ar tiene un publico muy interesante en Argentina y creemos que podriamos ayudarte a mejorar el rendimiento de tus espacios publicitarios con demanda premium.\n\nSi te parece, coordinamos una llamada corta esta semana para contarte como trabajamos con otros medios de la region.\n\nSaludos";
  let fila, modoGmail, pedidos = [];
  const r = (body, status = 200) => new Response(typeof body === "string" ? body : JSON.stringify(body),
    { status, headers: { "content-type": "application/json", "content-range": `0-0/${Array.isArray(body) ? body.length : 0}` } });
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = (opts.method || "GET").toUpperCase();
    pedidos.push({ m, u, body: opts.body ? String(opts.body) : null });
    if (u.includes("/rest/v1/")) {
      const tabla = u.split("/rest/v1/")[1].split(/[?/]/)[0];
      if (tabla === "toolbar_config" && m === "GET") return r(Object.entries(CFG).map(([key, value]) => ({ key, value })));
      if (tabla === "toolbar_reengagement_queue" && m === "GET") return r(fila.status === "pending" ? [{ ...fila }] : []);
      if (tabla === "toolbar_reengagement_queue" && m === "PATCH") { Object.assign(fila, JSON.parse(opts.body)); return r([]); }
      if (m === "POST") return r([{ id: 1 }], 201);
      return r([]);
    }
    if (u.includes("oauth2.googleapis.com/token")) return r({ access_token: "gtok", expires_in: 3600 });
    if (u.includes("gmail.googleapis.com")) {
      if (/messages\/send/.test(u)) {
        if (modoGmail === "reset") throw new Error("request to https://gmail.googleapis.com/gmail/v1/users/me/messages/send failed, reason: socket hang up ECONNRESET");
        return r({ id: "g-sent-1", threadId: "t1", labelIds: ["SENT"] });
      }
      if (/settings\/sendAs/.test(u)) return r({ sendAs: [{ sendAsEmail: MB, isPrimary: true, displayName: "Maxi Gargiulo", signature: "" }] });
      return r({});
    }
    return r({});
  };
  const Wr = await cargarWorker(["processManualReengagementQueue"], { fetchFalso: true });
  const ronda = async (modo) => {
    modoGmail = modo; pedidos = [];
    fila.status = "pending";   // como si scheduled_for ya hubiera pasado
    await Wr.processManualReengagementQueue("tok");
    return pedidos.filter(p => p.m === "POST" && /toolbar_response_tracking/.test(p.u)).length;
  };
  const nueva = () => ({ id: 9, domain: "diarioa.com.ar", monday_item_id: null, mb_email: MB, future_email: "juan.perez@diarioa.com.ar",
    original_subject: "Consulta sobre diarioa", original_body: BODY, tracking_action_id: null, original_email: "publicidad@diarioa.com.ar",
    reason: "adicional_manual", status: "pending" });

  fila = nueva();
  await ronda("reset");
  strictEqual(fila.status, "pending", JSON.stringify(fila));
  ok(/^adicional_manual \| .*\(intento 1, reintenta en 6h\)$/.test(fila.reason), `la marca se perdía en el primer fallo: ${fila.reason}`);
  await ronda("reset");
  ok(fila.status === "pending" && /\(intento 2, /.test(fila.reason), `el contador no avanzaba: ${JSON.stringify(fila)}`);
  await ronda("reset");
  strictEqual(fila.status, "failed", `el tope de 3 no se alcanzaba nunca: ${fila.reason}`);

  fila = nueva();
  await ronda("reset");
  const tracking = await ronda("ok");
  strictEqual(fila.status, "sent", JSON.stringify(fila));
  strictEqual(tracking, 1, "el adicional que salió al segundo intento tiene que escribir su fila de medición");
});
