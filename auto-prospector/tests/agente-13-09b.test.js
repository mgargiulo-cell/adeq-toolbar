// El agente que manda: qué dirección elige, qué hace con lo que no puede mandar y en qué orden
// gasta. (2026-09-13, segunda vuelta de la auditoría pedida por el dueño)
//
// "Dejar una base de conceptos, lógicas y funcionamientos automáticos sin fallas: lo que arreglás
// un día no lo rompas al otro." Cada test es una regla que la auditoría encontró rota en
// runAgentCycle, con el código real:
//   1. Un veredicto `dudoso` se elegía como si fuera limpio y el lead entero se perdía, aunque
//      tuviera otra dirección buena (195 `mv_dudoso` el 12/09).
//   2. La verificación final pagaba MillionVerifier después del pitch y de la ficha del CRM.
//   3. Un lead que pasaba a la lista del CRM se salteaba cada día sin salir de Prospects.
//   4. El 2º email salía sin las puertas del principal: dudosos, catch-all, otra marca.
//   5. Lo que un buzón salteaba, el siguiente lo repetía en el mismo turno.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as acorn from "acorn";
import * as walk from "acorn-walk";
import { cargarWorker } from "./_worker-exportado.mjs";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (firma) => {
  const i = worker.indexOf(firma);
  ok(i >= 0, `no encontré ${firma}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const agente = cuerpoDe("async function runAgentCycle(");
const tramo = (desde, hasta) => {
  const i = agente.indexOf(desde);
  ok(i >= 0, `no encontré ${desde}`);
  const j = agente.indexOf(hasta, i);
  ok(j > i, `no encontré ${hasta} después de ${desde}`);
  return agente.slice(i, j);
};
const REGLAS = ["_accionPorVeredictoMV", "_elegirDireccion", "_motivoSinDireccionEnviable", "_segundoEmailEnviable", "_motivoNoSegundoGratis"];
let _cargado = null;
const cargar = () => (_cargado ??= cargarWorker(REGLAS));
const respuesta = (body, { status = 200 } = {}) => ({ ok: status >= 200 && status < 300, status, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) });

// Candidato con la forma de `ranked` y dependencias falsas: MillionVerifier contesta de una tabla y
// se anota cada consulta, así se ve cuánto se pagaría.
const cand = (email, source = "scrape") => ({ email, source, score: 80 });
const falsos = (veredictos, extra = {}) => {
  const consultas = [], quemados = [];
  return {
    consultas, quemados,
    opts: {
      rebotado: () => false, noEscribir: () => null, marcaOk: () => true,
      ruta: async () => ({ verificar: true, enviar: true }),
      verificar: async (e) => { consultas.push(e); return veredictos[e]; },
      quemar: (c) => quemados.push(c.email),
      ...extra,
    },
  };
};

// ── 1. La elección de la dirección ──────────────────────────────────────────────────────
test("cada veredicto de MillionVerifier tiene UNA acción: ok elige, riesgo reserva, dudoso salta, no quema", async () => {
  const { _accionPorVeredictoMV } = await cargar();
  strictEqual(_accionPorVeredictoMV("ok"), "elegir");
  strictEqual(_accionPorVeredictoMV(true), "elegir", "MV dormido (sin clave) no frena nada, como siempre");
  strictEqual(_accionPorVeredictoMV("riesgo"), "reserva");
  strictEqual(_accionPorVeredictoMV("dudoso"), "saltar", "el dudoso NO se elige: no va como primer contacto");
  strictEqual(_accionPorVeredictoMV("no"), "quemar");
});

test("[dudoso, ok] elige el ok: el dudoso salta a la dirección siguiente y no se quema", async () => {
  const { _elegirDireccion } = await cargar();
  const f = falsos({ "contacto@unoentrerios.com.ar": "dudoso", "redaccion@unoentrerios.com.ar": "ok" });
  const r = await _elegirDireccion([cand("contacto@unoentrerios.com.ar"), cand("redaccion@unoentrerios.com.ar")], f.opts);
  strictEqual(r.chosen?.email, "redaccion@unoentrerios.com.ar", "antes se elegía contacto@ y la red final salteaba el lead entero");
  strictEqual(r.deReserva, false);
  deepStrictEqual(f.consultas, ["contacto@unoentrerios.com.ar", "redaccion@unoentrerios.com.ar"]);
  deepStrictEqual(r.motivos, ["mv_dudoso"]);
  deepStrictEqual(f.quemados, [], "un dudoso es una duda, no una casilla probada mala");
  ok(r.noEnviables.has("contacto@unoentrerios.com.ar"), "queda anotado para que el 2º email no lo use");
});

test("[ok, dudoso] cuesta UNA consulta: se verifica de a uno y se corta en el primero elegible", async () => {
  const { _elegirDireccion } = await cargar();
  const f = falsos({ "a@diario.pe": "ok", "b@diario.pe": "dudoso" });
  const r = await _elegirDireccion([cand("a@diario.pe"), cand("b@diario.pe")], f.opts);
  strictEqual(r.chosen?.email, "a@diario.pe");
  deepStrictEqual(f.consultas, ["a@diario.pe"]);
  strictEqual(r.mvUsed, 1);
});

test("[dudoso, catch-all] manda al catch-all de reserva; [dudoso] solo no manda nada y no quema nada", async () => {
  const { _elegirDireccion } = await cargar();
  const f = falsos({ "a@radio.co": "dudoso", "b@radio.co": "riesgo" });
  const r = await _elegirDireccion([cand("a@radio.co"), cand("b@radio.co")], f.opts);
  strictEqual(r.chosen?.email, "b@radio.co", "antes la reserva nunca se usaba: el dudoso se elegía primero");
  strictEqual(r.deReserva, true);

  const g = falsos({ "a@radio.co": "dudoso" });
  const s = await _elegirDireccion([cand("a@radio.co")], g.opts);
  strictEqual(s.chosen, null);
  deepStrictEqual(s.motivos, ["mv_dudoso"], "sale por all_candidates_undeliverable con el motivo, que el filtro de 7 días ya lee");
  deepStrictEqual(g.quemados, []);
});

test("[no, ok] elige el ok y quema la que no existe", async () => {
  const { _elegirDireccion } = await cargar();
  const f = falsos({ "viejo@tv.cl": "no", "ventas@tv.cl": "ok" });
  const r = await _elegirDireccion([cand("viejo@tv.cl"), cand("ventas@tv.cl")], f.opts);
  strictEqual(r.chosen?.email, "ventas@tv.cl");
  deepStrictEqual(f.quemados, ["viejo@tv.cl"]);
  deepStrictEqual(r.motivos, ["mv_no"], "antes esta rama no dejaba motivo en la fila del salteo");
});

test("en M365 o un gateway (verificar:false) un dudoso también salta, y un catch-all sale como siempre", async () => {
  const { _elegirDireccion } = await cargar();
  const ciega = async () => ({ verificar: false, enviar: true, motivo: "m365 acepta todo" });
  const f = falsos({ "a@periodico.es": "dudoso", "b@periodico.es": "ok" }, { ruta: ciega });
  const r = await _elegirDireccion([cand("a@periodico.es"), cand("b@periodico.es")], f.opts);
  strictEqual(r.chosen?.email, "b@periodico.es", "antes a@ se elegía sin verificar, la red final le pagaba MV igual y salteaba el lead");

  const g = falsos({ "a@periodico.es": "riesgo", "b@periodico.es": "ok" }, { ruta: ciega });
  const s = await _elegirDireccion([cand("a@periodico.es"), cand("b@periodico.es")], g.opts);
  strictEqual(s.chosen?.email, "a@periodico.es", "ahí el catch-all es lo esperable: no se reordena por un 'ok' que no significa nada");
  deepStrictEqual(g.consultas, ["a@periodico.es"], "y no se paga una consulta de más buscándolo");
});

test("lo gratis descarta antes de pagar, y la hipótesis en un proveedor que acepta todo no se consulta", async () => {
  const { _elegirDireccion } = await cargar();
  const f = falsos({ "e@sitio.mx": "ok" }, {
    rebotado: (e) => e === "a@sitio.mx",
    noEscribir: (e) => e === "b@sitio.mx" ? "el dominio ya rechazó 2 direcciones" : null,
    marcaOk: (c) => c.email !== "c@whoisproxy.org",
    ruta: async (c) => c.email === "d@sitio.mx" ? { verificar: false, enviar: false, motivo: "patrón en m365" } : { verificar: true, enviar: true },
  });
  const r = await _elegirDireccion(["a@sitio.mx", "b@sitio.mx", "c@whoisproxy.org", "d@sitio.mx", "e@sitio.mx"].map(e => cand(e)), f.opts);
  strictEqual(r.chosen?.email, "e@sitio.mx");
  deepStrictEqual(r.motivos, ["ya_reboto", "dominio_ya_rechazo", "otra_marca", "catch_all_y_patron"]);
  deepStrictEqual(f.consultas, ["e@sitio.mx"]);
  strictEqual(r.noEnviables.size, 4);
});

test("tope de 3 consultas por lead: el cuarto candidato se elige sin verificar (lo mira la red final)", async () => {
  const { _elegirDireccion } = await cargar();
  const f = falsos({ "a@x.it": "riesgo", "b@x.it": "riesgo", "c@x.it": "riesgo", "d@x.it": "ok" });
  const r = await _elegirDireccion(["a@x.it", "b@x.it", "c@x.it", "d@x.it"].map(e => cand(e)), f.opts);
  strictEqual(r.chosen?.email, "d@x.it");
  strictEqual(f.consultas.length, 3);
  strictEqual(r.deReserva, false);
});

test("runAgentCycle elige con _elegirDireccion y le pasa MillionVerifier de verdad; el bucle viejo no vuelve", () => {
  ok(/await _elegirDireccion\(_orden, \{/.test(agente));
  ok(/verificar: \(e\) => _verifyEmailMV\(token, cfg, e\)/.test(agente));
  ok(/ruta: \(c\) => decidirVerificacionMV\(c\.email, c\.source\)/.test(agente));
  ok(!/_ruta\.verificar && mvUsed < MAX_MV_PER_LEAD/.test(agente), "el bucle que elegía un dudoso como limpio");
  ok(!/chosen = cand;/.test(agente), "la elección no se reescribe inline: una sola regla, la probada acá");
});

// ── 2. El orden del tramo final ─────────────────────────────────────────────────────────
test("el tramo final gasta en orden: lo gratis primero, MillionVerifier después, el pitch al final", () => {
  const orden = [
    "classifyByUrlOnly(domain,",
    "await cupoDisponibleCasilla(userEmail)",
    "await _fichaDelCrm(domain)",
    "const _blockGuard = await isDomainBlockedFull(",
    "&send_date=gte.${cutoff}",
    "await _elegirDireccion(_orden",
    "pitch = await generatePitchAgent(token",
    "await pickAnyTemplate(token, userEmail, lead.language)",
    "const subject = _subjs.length",
    'action: "reserved"',
    "const _mvEstado = await _verifyEmailMV(token, cfg, email)",
    "sendGmailServer(token, userEmail, { to: email,",
  ];
  const pos = (t) => { const i = agente.indexOf(t); ok(i >= 0, `no encontré ${t}`); return i; };
  for (let k = 1; k < orden.length; k++) {
    ok(pos(orden[k - 1]) < pos(orden[k]), `"${orden[k - 1]}" tiene que ir antes que "${orden[k]}"`);
  }
});

// ── 3. Lo que no se pudo mandar queda escrito en el lead ────────────────────────────────
test("el motivo de un lead sin dirección enviable: el descarte más frecuente, y la hipótesis de patrón conserva el suyo", async () => {
  const { _motivoSinDireccionEnviable } = await cargar();
  strictEqual(_motivoSinDireccionEnviable(["mv_dudoso", "otra_marca", "mv_dudoso"]), "sin_direccion_enviable:mv_dudoso");
  strictEqual(_motivoSinDireccionEnviable(["mv_no", "catch_all_y_patron"]), "solo_hipotesis_de_patron", "el motivo que ya existía no cambia");
  strictEqual(_motivoSinDireccionEnviable(["ya_reboto", "otra_marca"]), "sin_direccion_enviable:ya_reboto", "empate: el primero que apareció");
  strictEqual(_motivoSinDireccionEnviable([]), "sin_direccion_enviable");
  strictEqual(_motivoSinDireccionEnviable(null), "sin_direccion_enviable");
});

test("el lead sin dirección enviable queda marcado con la fecha de AHORA, nunca null, y sin tocar el orden de emails", () => {
  const sinDireccion = tramo('reason: "all_candidates_undeliverable"', "// 2. Decidir source");
  ok(/await _marcarLeadSinDireccion\(token, lead\.id, _motivoSinDireccionEnviable\(_motivosDescarte\)\)/.test(sinDireccion),
     "antes sólo se marcaba la hipótesis de patrón");
  ok(!/email_ultimo_intento: null/.test(agente), "con null el lead queda PRIMERO en el pool del agente (email_ultimo_intento.asc.nullsfirst)");
  const red = tramo("const _mvEstado = await _verifyEmailMV(token, cfg, email)", "TECHO DE TIEMPO al envío completo");
  ok(/await _marcarLeadSinDireccion\(token, lead\.id, _motivoSinDireccionEnviable\(\["mv_dudoso"\]\)\)/.test(red), "el dudoso de la red final se marca igual");
  for (const cuerpo of [...agente.matchAll(/JSON\.stringify\(\{ email_ultimo_intento:[^}]*\}\)/g)].map(m => m[0])) {
    ok(!/\bemails:/.test(cuerpo), `el orden de emails lo leen _rankIntento y la extensión: ${cuerpo}`);
  }
});

// ── 3b. La marca del agente no le devuelve el lead a Apollo ─────────────────────────────
// Revisión del 13/09: la marca nueva pisaba `apollo_sin_contacto`, que es lo único que evita que
// `apolloQuemarCiclo` vuelva a pagar reveals por un lead de sólo genéricos (su caché sólo guarda
// los que dieron email). Se simula la base: un PATCH de PostgREST aplica el body a las filas que
// cumplen TODOS los filtros de la URL; `neq` sobre NULL no es true (SQL), por eso el `is.null`.
const condicion = (fila, expr) => {
  const [campo, op, ...resto] = expr.split(".");
  const valor = resto.join("."), actual = fila[campo];
  if (op === "is" && valor === "null") return actual == null;
  if (op === "eq") return actual != null && String(actual) === valor;
  if (op === "neq") return actual != null && String(actual) !== valor;
  throw new Error(`operador no simulado: ${expr}`);
};
const cumpleFiltros = (fila, query) => {
  for (const [k, v] of new URLSearchParams(query)) {
    if (k === "or") { if (!v.slice(1, -1).split(",").some(p => condicion(fila, p))) return false; }
    else if (k !== "select" && k !== "order" && k !== "limit" && !condicion(fila, `${k}.${v}`)) return false;
  }
  return true;
};
const baseFalsa = (tabla, pedidos) => async (url, opts = {}) => {
  const u = String(url);
  pedidos.push({ url: u, opts });
  const query = u.slice(u.indexOf("?") + 1);
  if (opts.method === "PATCH") for (const f of tabla) if (cumpleFiltros(f, query)) Object.assign(f, JSON.parse(opts.body));
  return respuesta(null, { status: 204 });
};
let _conBase = null;
const cargarConBase = () => (_conBase ??= cargarWorker(
  ["_pedidosMarcaSinDireccion", "_marcarLeadSinDireccion", "_FILTRO_MOTIVO_QUE_EL_AGENTE_PUEDE_PISAR", "_motivoSinDireccionEnviable", "_elegirDireccion", "_isGenericLocalPart"],
  { fetchFalso: true },
));
// El filtro con el que `apolloQuemarCiclo` elige a quién pedirle, sacado de su código.
const filtroApollo = () => {
  const m = cuerpoDe("async function apolloQuemarCiclo(").match(/const _base = `[^`]*?[?&](or=\([^)]*\))/);
  ok(m, "no encontré el filtro de motivo en la consulta de apolloQuemarCiclo");
  return m[1];
};

test("la marca del agente: la fecha va siempre y el motivo nunca pisa 'apollo_sin_contacto'", async () => {
  const { _pedidosMarcaSinDireccion, _marcarLeadSinDireccion } = await cargarConBase();
  const [fecha, motivo] = _pedidosMarcaSinDireccion(7, "sin_direccion_enviable:mv_dudoso", "2026-09-13T12:00:00.000Z");
  deepStrictEqual(fecha.body, { email_ultimo_intento: "2026-09-13T12:00:00.000Z" }, "la fecha sola, sin condición: ordena el pool del agente");
  deepStrictEqual(motivo.body, { email_ultimo_motivo: "sin_direccion_enviable:mv_dudoso" }, "ninguno toca emails: su orden lo leen _rankIntento y la extensión");
  ok(!/email_ultimo_motivo/.test(fecha.ruta), "la fecha no depende del motivo que tenga el lead");

  const tabla = [
    { id: 1, email_ultimo_motivo: null, email_ultimo_intento: null },
    { id: 2, email_ultimo_motivo: "sin_direccion_enviable:mv_no", email_ultimo_intento: "2026-09-01T00:00:00.000Z" },
    { id: 3, email_ultimo_motivo: "apollo_sin_contacto", email_ultimo_intento: "2026-09-01T00:00:00.000Z" },
    { id: 4, email_ultimo_motivo: "apollo_sin_contacto", email_ultimo_intento: "2026-09-01T00:00:00.000Z" },
  ];
  const pedidos = [];
  globalThis.__fetchFalso = baseFalsa(tabla, pedidos);
  for (const id of [1, 2, 3]) await _marcarLeadSinDireccion("t", id, "sin_direccion_enviable:mv_dudoso");
  strictEqual(pedidos.length, 6, "dos PATCH por lead");
  for (const p of pedidos) {
    strictEqual(p.opts.method, "PATCH");
    ok(p.url.includes("/rest/v1/toolbar_review_queue?id=eq."), p.url);
    ok(p.opts.signal, "todo fetch con reloj");
  }
  for (const f of tabla.slice(0, 3)) ok(Date.parse(f.email_ultimo_intento) > Date.now() - 60_000, `lead ${f.id}: la fecha pasa a AHORA, también con apollo_sin_contacto`);
  strictEqual(tabla[0].email_ultimo_motivo, "sin_direccion_enviable:mv_dudoso");
  strictEqual(tabla[1].email_ultimo_motivo, "sin_direccion_enviable:mv_dudoso", "otro motivo del agente sí se actualiza");
  strictEqual(tabla[2].email_ultimo_motivo, "apollo_sin_contacto", "Apollo ya dijo que no hay nadie: la marca queda");
  deepStrictEqual(tabla[3], { id: 4, email_ultimo_motivo: "apollo_sin_contacto", email_ultimo_intento: "2026-09-01T00:00:00.000Z" }, "el PATCH es por id");
});

test("un lead de sólo genéricos que Apollo ya marcó sin contacto no vuelve a apolloQuemarCiclo después de que el agente lo saltea", async () => {
  const w = await cargarConBase();
  strictEqual(w._FILTRO_MOTIVO_QUE_EL_AGENTE_PUEDE_PISAR, filtroApollo(),
    "el agente sólo escribe el motivo en las filas que Apollo consultaría: si cambia uno, tiene que cambiar el otro");
  // El caso del revisor: contacto@ e info@, los dos dudosos, y Apollo ya había dicho que no hay nadie.
  const lead = { id: 99, emails: ["contacto@diario.com.ar", "info@diario.com.ar"], email_ultimo_motivo: "apollo_sin_contacto", email_ultimo_intento: null };
  const entraAApollo = (l) => cumpleFiltros(l, filtroApollo()) && (l.emails.length === 0 || l.emails.every(e => w._isGenericLocalPart(String(e))));
  strictEqual(entraAApollo(lead), false, "antes del agente, Apollo lo deja afuera");
  const f = falsos({ "contacto@diario.com.ar": "dudoso", "info@diario.com.ar": "dudoso" });
  const r = await w._elegirDireccion(lead.emails.map(e => cand(e)), f.opts);
  strictEqual(r.chosen, null);
  globalThis.__fetchFalso = baseFalsa([lead], []);
  await w._marcarLeadSinDireccion("t", lead.id, w._motivoSinDireccionEnviable(r.motivos));
  ok(lead.email_ultimo_intento, "el agente igual anota que lo intentó");
  strictEqual(entraAApollo(lead), false, "antes de este arreglo quedaba 'sin_direccion_enviable:mv_dudoso' y Apollo volvía a pagar cada semana");
  // Y uno que Apollo nunca miró sí le queda disponible: la marca del agente es la señal para buscarle otra dirección.
  const nuevo = { id: 100, emails: ["contacto@otro.com.ar"], email_ultimo_motivo: null, email_ultimo_intento: null };
  globalThis.__fetchFalso = baseFalsa([nuevo], []);
  await w._marcarLeadSinDireccion("t", nuevo.id, "sin_direccion_enviable:mv_dudoso");
  strictEqual(entraAApollo(nuevo), true);
});

test("runAgentCycle no escribe el motivo del lead por fuera de _marcarLeadSinDireccion", () => {
  ok(!/email_ultimo_motivo\s*:/.test(agente), "un PATCH directo con email_ultimo_motivo pisaría 'apollo_sin_contacto'");
  ok((agente.match(/await _marcarLeadSinDireccion\(token, lead\.id, /g) || []).length >= 2, "los dos lugares: sin dirección enviable y el dudoso de la red final");
});

// ── 4. Bloqueado = sale de Prospects ────────────────────────────────────────────────────
const ast = acorn.parse(worker, { ecmaVersion: "latest", sourceType: "module" });
let fnAgente = null;
walk.simple(ast, { FunctionDeclaration(n) { if (n.id?.name === "runAgentCycle") fnAgente = n; } });
const linea = (n) => worker.slice(0, n.start).split("\n").length;
const esLlamadaA = (nodo, nombre) => {
  let x = nodo;
  if (x?.type === "AwaitExpression") x = x.argument;
  if (x?.type === "CallExpression" && x.callee.type === "MemberExpression" && x.callee.object.type === "CallExpression") x = x.callee.object;
  return x?.type === "CallExpression" && x.callee.type === "Identifier" && x.callee.name === nombre;
};

test("toda puerta de blocklist del agente que corta el lead también lo saca de Prospects", () => {
  ok(fnAgente, "no encontré runAgentCycle en el AST");
  const variables = new Set();
  walk.simple(fnAgente, { VariableDeclarator(d) { if (d.id.type === "Identifier" && esLlamadaA(d.init, "isDomainBlockedFull")) variables.add(d.id.name); } });
  ok(variables.size >= 2, `esperaba las dos puertas (inicio del lead y antes de mandar): ${[...variables]}`);
  let puertas = 0;
  walk.simple(fnAgente, {
    IfStatement(n) {
      if (n.test.type !== "Identifier" || !variables.has(n.test.name)) return;
      let corta = false, rechaza = false;
      walk.simple(n.consequent, {
        ContinueStatement() { corta = true; },
        CallExpression(c) { if (c.callee.type === "Identifier" && c.callee.name === "_softRejectLead") rechaza = true; },
      });
      if (!corta) return;
      puertas++;
      ok(rechaza, `la puerta de la línea ${linea(n)} saltea el lead sin sacarlo: el MB lo sigue viendo y el agente lo retoma cada día`);
    },
  });
  strictEqual(puertas, 2);
});

test("el agente rechaza con credenciales que existen: nunca `auth`, que no está declarado en runAgentCycle", () => {
  const primeros = [];
  walk.simple(fnAgente, { CallExpression(c) { if (c.callee.type === "Identifier" && c.callee.name === "_softRejectLead") primeros.push(c.arguments[0]); } });
  ok(primeros.length >= 2);
  for (const a of primeros) ok(a?.type === "Identifier" && a.name === "_authRq", `_softRejectLead de la línea ${linea(a)} tiene que recibir _authRq`);
  let declarada = false, usaAuth = false;
  walk.simple(fnAgente, { VariableDeclarator(d) { if (d.id.type === "Identifier" && d.id.name === "_authRq") declarada = true; } });
  walk.full(fnAgente, (n) => { if (n.type === "Identifier" && n.name === "auth") usaAuth = true; });
  ok(declarada, "_authRq tiene que estar declarada en runAgentCycle");
  ok(!usaAuth, "un ReferenceError en la puerta de blocklist —que corre fuera del try del lead— tira el ciclo de envío entero");
  ok(agente.indexOf("const _authRq =") < agente.indexOf("for (const lead of fresh)"), "se declara antes del bucle de leads");
});

test("el rechazo suave es el mismo que el del pulido: rejected + suspect_reject + motivo 'purge: blocklist:…'", async () => {
  const { _softRejectLead } = await cargarWorker(["_softRejectLead"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => { pedidos.push({ url: String(url), opts }); return respuesta(null, { status: 204 }); };
  await _softRejectLead({ apikey: "k", Authorization: "Bearer t" }, 42, "blocklist:crm-no-recontactar");
  strictEqual(pedidos.length, 1);
  ok(pedidos[0].url.includes("toolbar_review_queue?id=eq.42"));
  strictEqual(pedidos[0].opts.method, "PATCH");
  strictEqual(pedidos[0].opts.headers.Authorization, "Bearer t");
  const body = JSON.parse(pedidos[0].opts.body);
  strictEqual(body.status, "rejected");
  strictEqual(body.suspect_reject, true, "reversible: el MB puede levantar la marca");
  strictEqual(body.suspect_reason, "purge: blocklist:crm-no-recontactar");
});

// ── 5. El 2º email ──────────────────────────────────────────────────────────────────────
test("el 2º email necesita las mismas puertas que el principal y un 'ok' de MillionVerifier", async () => {
  const { _segundoEmailEnviable } = await cargar();
  const base = {
    email: "comercial@diario.com.br", primario: "redacao@diario.com.br", score: 140,
    rebotado: false, noEnviable: false, noEscribir: null, marcaOk: true,
    ruta: { verificar: true, enviar: true }, veredictoMV: "ok",
  };
  deepStrictEqual(_segundoEmailEnviable(base), { ok: true, motivo: "ok" });
  const no = (cambio, motivo, porque) => deepStrictEqual(_segundoEmailEnviable({ ...base, ...cambio }), { ok: false, motivo }, porque);
  no({ veredictoMV: "dudoso" }, "mv_dudoso", "un dudoso no va como primer contacto, tampoco por esta vía");
  no({ veredictoMV: "riesgo" }, "mv_riesgo", "catch-all, tope o error de MV: el 2º nunca es reserva, el principal ya salió");
  no({ veredictoMV: true }, "mv_sin_verificar", "MV sin clave: sin verificar no sale un 2º");
  no({ veredictoMV: "no" }, "mv_no");
  no({ ruta: { verificar: false, enviar: false }, veredictoMV: null }, "catch_all_y_patron", "hipótesis de patrón en un proveedor que acepta todo");
  no({ email: "REDACAO@diario.com.br" }, "igual_al_primario");
  no({ rebotado: true }, "ya_reboto");
  no({ noEnviable: true }, "descartado_al_elegir", "lo que el bucle del principal ya descartó no se reusa");
  no({ noEscribir: "el dominio ya rechazó 2 direcciones" }, "dominio_ya_rechazo");
  no({ marcaOk: false }, "otra_marca");
  no({ score: 39 }, "score_bajo");
});

test("runAgentCycle decide el 2º email con esa regla, con una sola consulta por lead y antes de mandar", () => {
  const segundo = tramo("AGENTE TAMBIÉN manda al 2do mejor email", "// 5. Push al CRM.");
  ok(/ranked\.find\(r => r\.email && !_motivoNoSegundoGratis\(_datos2\(r\)\)\)/.test(segundo));
  ok(/noEnviable: _noEnviables\.has\(/.test(segundo));
  ok(/noEscribir: _porQueNoEscribirA\(r\.email\)/.test(segundo) && /marcaOk: _brandMatches\(r\.email, domain, r\.source\)/.test(segundo));
  ok(/decidirVerificacionMV\(secondCandidate\.email, secondCandidate\.source\)/.test(segundo));
  ok(/_segundoEmailEnviable\(\{ \.\.\._datos2\(secondCandidate\), ruta: _ruta2, veredictoMV: _v2 \}\)/.test(segundo));
  strictEqual((segundo.match(/_verifyEmailMV\(/g) || []).length, 1, "un solo candidato verificado por lead");
  ok(segundo.indexOf("if (_dec2.ok)") > 0 && segundo.indexOf("if (_dec2.ok)") < segundo.indexOf("sendGmailServer("), "la decisión va antes del envío");
  ok(!/_v2 === "no"\) \{ log\(`  ⛔/.test(segundo), "el freno viejo, que sólo miraba 'no', no puede volver");
  ok(agente.indexOf("const _noEnviables = new Set()") < agente.indexOf("await _elegirDireccion(_orden"), "el set vive en el alcance del lead, no adentro del if");
});

// ── 6. El turno siguiente no repite ─────────────────────────────────────────────────────
test("lo que un buzón saltea sin dirección, el siguiente no lo repite en el mismo turno", () => {
  ok(/reason: "all_candidates_undeliverable"[\s\S]{0,900}_saltadosSinDireccion7d\.add\(String\(domain \|\| ""\)\.toLowerCase\(\)\)/.test(agente),
     "el set de 7 días se cargó al empezar el ciclo: el salteo de ahora hay que sumarlo a mano");
  const red = tramo("const _mvEstado = await _verifyEmailMV(token, cfg, email)", "TECHO DE TIEMPO al envío completo");
  ok(/_saltadosSinDireccion7d\.add\(/.test(red), "el dudoso de la red final también");
  ok(/const _rp = await fetch\(`\$\{SUPABASE_URL\}\/rest\/v1\/toolbar_agent_actions\?id=eq\.\$\{reservedId\}`/.test(red),
     "la reserva se pasa a skipped con await: si se pierde, queda reserve_expired y el filtro no la ve");
  ok(/if \(!_anotado\) \{\s*await logAgentAction\(token, userEmail, \{ domain, action: "skipped", reason: _motivoMv/.test(red),
     "y si no se confirmó, el salteo se anota aparte (sólo en ese caso, para no duplicarlo en el parte)");
  ok(/SKIP — no_email_after_enrichment[^\n]*\n\s*_sinEmailEsteCiclo\.add\(/.test(agente));
  ok(/fresh = fresh\.filter\(l => !_sinEmailEsteCiclo\.has\(/.test(agente));
  ok(agente.indexOf("const _sinEmailEsteCiclo = new Set()") < agente.indexOf("for (const userEmail of _usuariosOrdenados)"),
     "vive todo el ciclo (todos los buzones), no uno por buzón");
  ok(/reason=in\.\(mv_dudoso,all_candidates_undeliverable\)&created_at=gte\./.test(agente),
     "la ventana de 7 días NO suma no_email_after_enrichment: eso lo decide el congelado de 3 fallas");
});
