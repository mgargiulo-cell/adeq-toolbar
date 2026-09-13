// La entrada, los sueltos del 13/09: defectos que los agentes de la auditoría del pool marcaron fuera
// de su zona y nadie tomó. (2026-09-13, cuarta ronda)
//
// "Lo que arreglás un día no lo rompas al otro." Cada test es una regla, y falla con el código de antes:
//   E1. Un DNS que no contestó (EAI_AGAIN) no es un dominio muerto: se reintenta una vez y, si sigue,
//       es "no pude leer" (null). Y el motivo de un muerto guarda el código, no 60 letras del mensaje.
//   E2. El mailto con arroba normal escondido en un bloque invisible, o con cara de trampa, entraba por
//       un camino que no pasaba por los filtros del texto (extractor, crawl del sitio e informer).
//   E3. Lo que Google (Serper) encuentra DENTRO del scrape se guardaba como scrape: el parte subestimaba
//       google_contact. scrapeEmailsForDomain lo devuelve en opts.googleOut y los llamadores lo usan.
//   E4. El webmail del registrante (informer + gmail/hotmail…) sólo lo filtraba el agente: ahora también
//       la cola, el autopilot y la auditoría del pool, con motivo propio.
//   E5. El 2.0 de páginas por visita estimadas seguía escrito a mano en la cola y en el autopilot.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual, match } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { _bouncedCache } from "../lib/email.js";

// SERPER_API_KEY es una constante de módulo: tiene que estar antes de cargar el worker (E3).
process.env.SERPER_API_KEY = process.env.SERPER_API_KEY || "clave-de-prueba";
process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const sinComentarios = (s) => s.split("\n").map(l => l.replace(/\/\/.*$/, "")).join("\n");

const resp = (body, { status = 200, tipo = "text/html; charset=utf-8" } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "", redirected: false, body: null,
  headers: { get: (k) => (String(k).toLowerCase() === "content-type" ? tipo : null) },
  json: async () => (typeof body === "string" ? JSON.parse(body) : body),
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
  arrayBuffer: async () => new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body)).buffer,
});
const json = (body, status = 200) => resp(body, { status, tipo: "application/json" });
const pagina = (cuerpo, titulo = "Diario de Prueba") => resp(`<!doctype html><html lang="es"><head><title>${titulo}</title></head><body>${cuerpo}</body></html>`);
const noEsta = () => resp("not found", { status: 404 });
const hostYRuta = (u) => { try { const p = new URL(String(u)); return [p.hostname.replace(/^www\./, ""), p.pathname]; } catch { return ["", ""]; } };

globalThis.__fetchFalso = async () => noEsta();
const W = await cargarWorker([
  "fetchPageContent", "extractEmailsFromHtml", "scrapeEmailsForDomain", "scrapeInformerOnly",
  "_planAuditoriaLead", "processCsvItem",
], { fetchFalso: true });

// ── E1. DNS temporal ≠ dominio muerto ───────────────────────────────────────────────────────
const errDns = (codigo, host) => Object.assign(new Error(`getaddrinfo ${codigo} ${host}`), { code: codigo, errno: codigo, syscall: "getaddrinfo" });
const LARGO = "www.un-dominio-con-nombre-bastante-largo-de-prueba.com.ar";

test("E1: un EAI_AGAIN (el DNS no contestó) se reintenta una vez y, si contesta, la página se lee", async () => {
  let n = 0;
  globalThis.__fetchFalso = async () => { n++; if (n === 1) throw errDns("EAI_AGAIN", "diariodeprueba.com.ar"); return pagina("<article>nota</article>"); };
  const pc = await W.fetchPageContent("diariodeprueba.com.ar");
  ok(pc && typeof pc === "object" && !pc.dead, `un tropiezo del DNS dejaba el dominio muerto: ${JSON.stringify(pc)?.slice(0, 120)}`);
  strictEqual(pc.title, "Diario de Prueba");
  strictEqual(n, 2, "un reintento");
});

test("E1: si el DNS sigue sin contestar es 'no pude leer' (null), nunca un dominio muerto — también con el código sólo en el mensaje", async () => {
  let n = 0;
  globalThis.__fetchFalso = async () => { n++; throw errDns("EAI_AGAIN", "diariodeprueba.com.ar"); };
  strictEqual(await W.fetchPageContent("diariodeprueba.com.ar"), null);
  strictEqual(n, 2, "un solo reintento, no un bucle");
  n = 0;
  globalThis.__fetchFalso = async () => { n++; throw new Error(`request to https://${LARGO}/ failed, reason: getaddrinfo EAI_AGAIN ${LARGO}`); };
  strictEqual(await W.fetchPageContent(LARGO), null, "node-fetch puede traer el código sólo en el mensaje");
  strictEqual(n, 2);
});

test("E1: un dominio que no existe (ENOTFOUND) sigue muerto, sin reintento, y el motivo guarda el código", async () => {
  let n = 0;
  globalThis.__fetchFalso = async () => { n++; throw new Error(`request to https://${LARGO}/ failed, reason: getaddrinfo ENOTFOUND ${LARGO}`); };
  const pc = await W.fetchPageContent(LARGO);
  strictEqual(pc?.dead, true);
  strictEqual(n, 1, "un dominio inexistente no paga la espera del reintento");
  match(pc.deadReason, /ENOTFOUND/, `el corte a 60 letras se comía el código: "${pc.deadReason}"`);
  ok(pc.deadReason.length <= 60);
  globalThis.__fetchFalso = async () => { throw errDns("ENOTFOUND", "toyotadeprueba.co.jp"); };
  deepStrictEqual(await W.fetchPageContent("toyotadeprueba.co.jp"), { dead: true, deadReason: "ENOTFOUND" });
});

test("E1: la entrada (processCsvItem) no descarta como dominio muerto un tropiezo del DNS: sigue su camino", async () => {
  const ADS_TXT = Array.from({ length: 25 }, (_, i) => `google.com, pub-${1000 + i}, DIRECT, f08c47fec0942fa0`).join("\n");
  const plano = (body, status = 200) => ({ ok: status >= 200 && status < 300, status, url: "", headers: { get: () => null }, json: async () => body, text: async () => (typeof body === "string" ? body : JSON.stringify(body)) });
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase();
    reg.push({ u, m, b: String(opts.body || "") });
    if (u.includes("/api/crm/ficha?domain=")) return plano({ found: false });
    if (/\/app-ads\.txt$/.test(u)) return plano("", 404);
    if (/\/ads\.txt$/.test(u)) return plano(ADS_TXT);
    if (u.includes("toolbar_traffic_cache?domain=eq.")) return plano([{ data: { visits: 2_000_000, rawVisits: 2_000_000, pagesPerVisit: 2, topCountries: [{ code: "PE" }], category: "News and Media" }, fetched_at: new Date().toISOString() }]);
    if (/^https?:\/\/[^/]+\/?$/.test(u)) throw errDns("EAI_AGAIN", "sextodiariodeprueba.com.pe");
    return plano([]);
  };
  const cfg = { rapidapi_key: "", worker_discovery_config: JSON.stringify({ geos_excluded: ["PE"] }) };
  await W.processCsvItem("t", { id: 201, domain: "sextodiariodeprueba.com.pe", source: "auto_feeder_sellers", uploaded_by: "worker@autofeeder", error_message: "" },
    cfg, { usedToday: 0, limit: 0, monthLimit: 0, usedThisMonth: 0 }, { count: 0 });
  const p = reg.filter(r => r.m === "PATCH" && r.u.includes("toolbar_csv_queue?id=eq.201")).map(r => JSON.parse(r.b));
  ok(!p.some(x => /dead_domain/.test(x.error_message || "")), `se descartó como dominio muerto: ${JSON.stringify(p)}`);
  strictEqual(p.length, 1, JSON.stringify(p));
  match(p[0].error_message, /^worker_geo_excluded:/, "siguió por las puertas de siempre");
});

// ── E2. El mailto pasa por los mismos filtros que el texto ─────────────────────────────────────
const D = "diariodeprueba.com.ar";
const E = `ventas@${D}`;
// Cloudflare email protection: primer byte = clave, el resto XOR.
const cfHex = (email, k = 0x5a) => [k, ...[...email].map(c => c.charCodeAt(0) ^ k)].map(b => b.toString(16).padStart(2, "0")).join("");

test("E2: un mailto con arroba normal dentro de un bloque invisible no entra, igual que el texto", () => {
  for (const html of [
    `<p>Hola</p><div style="display:none"><a href="mailto:${E}">Escribinos</a></div>`,
    `<div hidden><a href="mailto:${E}?subject=Pauta">Escribinos</a></div>`,
    `<span class="form-honeypot"><a href="mailto:${E}">x</a></span>`,
    `<!-- <a href="mailto:${E}">x</a> -->`,
  ]) deepStrictEqual(W.extractEmailsFromHtml(html), [], html);
  deepStrictEqual(W.extractEmailsFromHtml(`<footer><a href="mailto:${E}">Escribinos</a></footer>`), [E], "a la vista, entra como siempre");
  deepStrictEqual(W.extractEmailsFromHtml(`<footer class="overflow-hidden"><a href="mailto:${E}?subject=Pauta">x</a></footer>`), [E]);
});

test("E2: un mailto con cara de trampa no entra aunque esté a la vista", () => {
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="mailto:spamtrap@${D}">Escribinos</a>`), []);
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="mailto:a7f3k9x2m4q8@${D}">x</a>`), [], "local aleatorio de honeypot");
});

test("E2: los otros caminos del extractor (data-*, Cloudflare, JSON-LD) tampoco leen lo invisible ni las trampas", () => {
  deepStrictEqual(W.extractEmailsFromHtml(`<span data-email="${E}"></span>`), [E]);
  deepStrictEqual(W.extractEmailsFromHtml(`<div style="display:none"><span data-email="${E}"></span></div>`), []);
  deepStrictEqual(W.extractEmailsFromHtml(`<a class="__cf_email__" data-cfemail="${cfHex(E)}">[email&#160;protected]</a>`), [E]);
  deepStrictEqual(W.extractEmailsFromHtml(`<div hidden><a class="__cf_email__" data-cfemail="${cfHex(E)}">[email&#160;protected]</a></div>`), []);
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="/cdn-cgi/l/email-protection#${cfHex(E)}">x</a>`), [E]);
  deepStrictEqual(W.extractEmailsFromHtml(`<div style="display:none"><a href="/cdn-cgi/l/email-protection#${cfHex(E)}">x</a></div>`), []);
  deepStrictEqual(W.extractEmailsFromHtml(`<script type="application/ld+json">{"@type":"Organization","email":"${E}"}</script>`), [E]);
  deepStrictEqual(W.extractEmailsFromHtml(`<script type="application/ld+json">{"@type":"Organization","email":"spamtrap@${D}"}</script>`), []);
});

test("E2: el crawl del sitio no vuelve a sumar por su cuenta el mailto escondido", async () => {
  globalThis.__fetchFalso = async (url) => {
    const [host, ruta] = hostYRuta(url);
    if (host === D && ruta === "/") {
      return pagina(`<div style="display:none"><a href="mailto:${E}">.</a></div><div class="honeypot"><a href="mailto:redaccion@${D}">.</a></div>`
        + `<p>Publicidad: <a href="mailto:publicidad@${D}">publicidad@${D}</a></p><a href="mailto:spamtrap@${D}">.</a>`);
    }
    return noEsta();
  };
  const r = await W.scrapeEmailsForDomain(D, { sinSerper: true, maxMs: 15000 });
  ok(r.includes(`publicidad@${D}`), `el visible tiene que entrar: ${r}`);
  for (const malo of [E, `redaccion@${D}`, `spamtrap@${D}`]) ok(!r.includes(malo), `${malo} estaba escondido o es una trampa y entró: ${r}`);
});

test("E2: la página de informer tampoco suma un mailto escondido o con cara de trampa", async () => {
  globalThis.__fetchFalso = async (url) => {
    const u = String(url);
    if (u === `https://website.informer.com/${D}`) {
      return pagina(`<p>Registrant: dueno@${D}</p><div style="display:none"><a href="mailto:${E}">x</a></div><a href="mailto:spamtrap@${D}">x</a>`);
    }
    return noEsta();
  };
  const r = await W.scrapeInformerOnly(D);
  ok(r.emails.includes(`dueno@${D}`), `lo visible de informer tiene que entrar: ${r.emails}`);
  for (const malo of [E, `spamtrap@${D}`]) ok(!r.emails.includes(malo), `${malo} entró desde informer: ${r.emails}`);
});

// ── E3. Lo que trajo Google se guarda como google_contact ────────────────────────────────────
test("E3: lo que Google encuentra dentro del scrape (snippet y páginas que indexó) sale en opts.googleOut", async () => {
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url);
    pedidos.push(u);
    if (u.includes("google.serper.dev")) {
      return json({ organic: [{ title: "Contacto comercial", snippet: `Publicidad: comercial@${D}`, link: `https://${D}/contacto-comercial` }] });
    }
    if (u.includes("toolbar_config")) return json([]);
    const [host, ruta] = hostYRuta(u);
    if (host === D && ruta === "/") return pagina(`<p>Escribinos a info@${D}</p>`);
    if (host === D && ruta === "/contacto-comercial") return pagina(`<p>Editor: juan.perez@${D}</p>`);
    return noEsta();
  };
  const googleOut = new Set();
  const r = await W.scrapeEmailsForDomain(D, { googleOut, maxMs: 20000 });
  ok(pedidos.some(u => u.includes("google.serper.dev")), "el test sólo prueba algo si la búsqueda en Google corrió");
  ok(r.includes(`comercial@${D}`) && r.includes(`juan.perez@${D}`), `el scrape tiene que devolverlos: ${r}`);
  deepStrictEqual([...googleOut].sort(), [`comercial@${D}`, `juan.perez@${D}`], "lo que ya estaba antes de Google (info@) no cuenta como suyo");
});

test("E3: los que llaman al scrape guardan google_contact para lo que trajo Google", () => {
  const csv = cuerpoDe("processCsvItem");
  match(csv, /scrapeEmailsForDomain\(domain, \{[^}]*googleOut: googleSet[^}]*\}\)/);
  match(csv, /googleSet\.has\(lower\)[^\n]*\n[^\n]*source: "google_contact"/);
  const auto = cuerpoDe("runSession");
  strictEqual((auto.match(/scrapeEmailsForDomain\(domain, \{[^}]*googleOut: googleSetAuto[^}]*\}\)/g) || []).length, 2, "las dos llamadas del autopilot");
  match(auto, /googleSetAuto\.has\(lower\)[^\n]*\n[^\n]*source: "google_contact"/);
  const pulido = cuerpoDe("polishPool");
  match(pulido, /scrapeEmailsForDomain\(domain, \{[^}]*googleOut: _googleOut[^}]*\}\)/);
  // La vía de cada dirección se arma en _viaCrawl, que el pulido usa para sacar el webmail del registrante antes de
  // rankear y para foundSource (revisión final del 13/09, tests/worker_final-13-09e.test.js B1).
  match(pulido, /const _viaCrawl = \(e\) => \{[^\n]*_googleOut\.has\(_le\) \? "google_contact"/);
  match(pulido, /foundSource = _viaCrawl\(foundEmail\);/);
  const rebote = cuerpoDe("queueBounceRetry");
  match(rebote, /scrapeEmailsForDomain\(domain, \{[^}]*googleOut: _google[^}]*\}\)/);
  match(rebote, /_google\.has\(l\) \? "google_contact"/);
  const agente = cuerpoDe("runAgentCycle");
  match(agente, /scrapeEmailsForDomain\(domain, \{ googleOut: _googleScrape \}\)/);
  match(agente, /_googleScrape\.has\(/);
});

// ── E4. El webmail del registrante ──────────────────────────────────────────────────────────
test("E4: el webmail del registrante (informer + gmail) sale al guardar; un gmail publicado en el sitio queda", async () => {
  const { _quitarRegistranteWebmail } = await cargarWorker(["_quitarRegistranteWebmail"]);
  const fuentes = {
    "rudnypc@gmail.com": { source: "informer", url: "https://website.informer.com/baladag4.com.br" },
    "juan.perez@gmail.com": { source: "scrape", url: "https://baladag4.com.br/contato" },
    "dueno@hotmail.com": "informer",
    "publicidade@baladag4.com.br": { source: "informer" },
  };
  deepStrictEqual(
    _quitarRegistranteWebmail(["rudnypc@gmail.com", "publicidade@baladag4.com.br", "juan.perez@gmail.com", "dueno@hotmail.com"], fuentes),
    { quedan: ["publicidade@baladag4.com.br", "juan.perez@gmail.com"], fuera: ["rudnypc@gmail.com", "dueno@hotmail.com"] });
  deepStrictEqual(_quitarRegistranteWebmail(["RudnyPC@Gmail.com"], { "rudnypc@gmail.com": "informer" }).fuera, ["RudnyPC@Gmail.com"]);
  deepStrictEqual(_quitarRegistranteWebmail([], null), { quedan: [], fuera: [] });
});

test("E4: la cola y el autopilot filtran el registrante después de validar y antes de guardar", () => {
  for (const [fn, fuentes] of [["processCsvItem", "emailSources"], ["runSession", "emailSourcesAuto"]]) {
    const c = cuerpoDe(fn);
    const iFil = c.indexOf(`_quitarRegistranteWebmail(emails, ${fuentes})`);
    ok(iFil >= 0, `${fn} no filtra el webmail del registrante`);
    ok(c.lastIndexOf("await validateEmailsBatch(", iFil) >= 0, `${fn}: el filtro va después de validar`);
    ok(c.indexOf("saveToReviewQueue(token", iFil) > iFil, `${fn}: y antes de guardar en Prospects`);
  }
});

test("E4: la auditoría del pool saca el webmail del registrante con motivo propio", () => {
  _bouncedCache.set = new Set();
  const lead = { id: 1, domain: "baladag4.com.br", emails: ["rudnypc@gmail.com", "info@baladag4.com.br"], email_sources: { "rudnypc@gmail.com": { source: "informer" } }, status: "validated", category: "" };
  const r = W._planAuditoriaLead(lead);
  deepStrictEqual(r.plan?.buenos, ["info@baladag4.com.br"], "el gmail del WHOIS puntuaba 65 y pasaba la marca: quedaba primero");
  deepStrictEqual(r.malos.map(m => [m.email, m.motivo]), [["rudnypc@gmail.com", "registrante_webmail"]]);
  strictEqual(W._planAuditoriaLead({ ...lead, email_sources: { "rudnypc@gmail.com": "scrape" } }).plan, null, "un gmail publicado en el sitio no se toca");
  deepStrictEqual(W._planAuditoriaLead({ ...lead, status: "por_enviar" }).aviso, { email: "rudnypc@gmail.com", motivo: "registrante_webmail" }, "en la cola por enviar sólo se avisa");
});

// ── E5. Una sola constante de páginas por visita ─────────────────────────────────────────────
test("E5: el 2.0 de páginas por visita estimadas vive sólo en PPV_ESTIMADO", () => {
  const csv = cuerpoDe("processCsvItem");
  const auto = cuerpoDe("runSession");
  match(csv, /const PPV_FALLBACK = PPV_ESTIMADO;/);
  match(auto, /const ppvSafe\s*=\s*\(typeof pagesPerVisit === "number" && pagesPerVisit > 0\) \? pagesPerVisit : PPV_ESTIMADO;/);
  strictEqual((sinComentarios(csv).match(/\b2\.0\b/g) || []).length, 0, "processCsvItem no puede tener otro 2.0 escrito a mano");
  strictEqual((sinComentarios(auto).match(/\b2\.0\b/g) || []).length, 0, "runSession tampoco");
  const codigo = sinComentarios(worker);
  deepStrictEqual(codigo.match(/pagesPerVisit\s*>\s*0\)?\s*\?\s*pagesPerVisit\s*:\s*\d+(?:\.\d+)?/g) || [], [], "ningún respaldo de ppv con número literal");
  deepStrictEqual(codigo.match(/\bPPV\w*\s*=\s*\d+(?:\.\d+)?/g) || [], ["PPV_ESTIMADO = 2.0"], "una sola constante con número");
});
