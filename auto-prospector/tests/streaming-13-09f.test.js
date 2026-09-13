// Streaming de música, películas, series y TV: ya no es un TIPO no prospectable. (2026-09-13, noche)
//
// Decisión del dueño: "Streaming: si es de música podría ser, de películas también, podríamos intentar."
// Hasta hoy la regla del 27/08 decía que el tipo streaming no entraba ni por la puerta grande, y lo hacía en
// dos lugares: CATEGORIAS_NUNCA (comparaba por pedazo de texto: "arts_and_entertainment/tv_movies_and_streaming"
// o cualquier categoría de música o video con "streaming" adentro) y BLOCKED_CATEGORIES del agente (la
// heurística de la página le guardaba "streaming" a todo sitio que lo nombrara, así que una guía de música que
// entraba por otra categoría de SimilarWeb no recibía nunca un mail). Desde hoy pasa por las mismas puertas
// que cualquier medio. Cada test fija una punta de la regla nueva, con el código real:
//   S1. processCsvItem de verdad: tv-programme.com, filmelier.com y una guía de música con ads.txt y tráfico
//       en rango ya no salen 'tipo_no_prospectable:streaming': llegan a guardarse en Prospects.
//   S2. Las mismas puertas de siempre: sin ads.txt afuera; netflix.com y spotify.com por marca; un gigante por techo.
//   S3. Los demás tipos no cambian: apuestas, banco y e-commerce siguen vetados.
//   S4. Lo que no se abre: el streaming pirata y las retransmisiones en vivo siguen vetados al enviar, con una
//       sola regla para la heurística de entrada y para las filas viejas; la piratería con marcado, también.
//   S5. El barrido y el chequeo al enviar no sacan a un medio de streaming que entró.
//   S6. La extensión no lo marca "no prospectable" por tipo.
//
// Run: npm test
/* eslint-disable no-new-func */
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual, match } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";
import { isCategoryBlocked } from "../../modules/blocklist.js";

process.env.CRM_SYNC_SECRET = process.env.CRM_SYNC_SECRET || "secreto-de-prueba";
delete process.env.SERPER_API_KEY;   // ningún test sale a Serper

const aqui = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(aqui, "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const popup = fs.readFileSync(path.join(RAIZ, "..", "popup", "popup.js"), "utf8");
const popupHtml = fs.readFileSync(path.join(RAIZ, "..", "popup", "popup.html"), "utf8");

const resp = (body, { status = 200 } = {}) => ({
  ok: status >= 200 && status < 300, status, url: "",
  headers: { get: () => null },
  json: async () => body,
  text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
});
globalThis.__fetchFalso = async () => resp([]);

const W = await cargarWorker([
  "processCsvItem", "_categoriaNuncaProspectable", "CATEGORIAS_NUNCA", "BLOCKED_CATEGORIES", "scoreWebsite",
  "_esStreamingPirata", "classifyByUrlOnly", "_veredictoUrlAlEnviar", "_veredictoPorSimilarWeb",
  "fetchPageContent", "barridoNoPublisher", "_motivoCanonicoCola",
], { fetchFalso: true });

const ADS_TXT = Array.from({ length: 25 }, (_, i) => `google.com, pub-${3000 + i}, DIRECT, f08c47fec0942fa0`).join("\n");
const home = (title, extra = "") => `<!doctype html><html lang="es"><head><title>${title}</title><meta name="description" content="${title}"></head><body><article><h1>${title}</h1><p>${"Texto de la guia del dia. ".repeat(80)}</p></article>${extra}</body></html>`;
const USO_APOLLO = { usedToday: 0, limit: 0, monthLimit: 0, usedThisMonth: 0 };
const itemDe = (id, domain) => ({ id, domain, source: "auto_feeder_sellers", uploaded_by: "worker@autofeeder", error_message: "" });
const patchesDeCola = (reg, id) => reg.filter(r => r.m === "PATCH" && r.u.includes(`toolbar_csv_queue?id=eq.${id}`)).map(r => JSON.parse(r.b));
const altasEnProspects = (reg) => reg.filter(r => r.m === "POST" && r.u.includes("toolbar_review_queue?on_conflict=domain")).map(r => JSON.parse(r.b));

// La red de un sitio: el CRM dice "no está", el ads.txt existe (o no), la caché de tráfico trae la categoría de
// SimilarWeb y el país, y la home es la que se le pasa. Todo lo demás, lista vacía.
function red({ category, pais = "FR", visits = 1_000_000, conAdsTxt = true, html = home("Sitio de prueba"), homes = {}, extra = [] } = {}) {
  const reg = [];
  globalThis.__fetchFalso = async (url, opts = {}) => {
    const u = String(url), m = String(opts.method || "GET").toUpperCase();
    reg.push({ u, m, b: String(opts.body || "") });
    for (const [cond, contestar] of extra) if (cond(u, m)) return contestar(u, m);
    if (u.includes("/api/crm/ficha?domain=")) return resp({ found: false });
    if (/\/app-ads\.txt$/.test(u)) return resp("", { status: 404 });
    if (/\/ads\.txt$/.test(u)) return conAdsTxt ? resp(ADS_TXT) : resp("", { status: 404 });
    if (u.includes("toolbar_traffic_cache?domain=eq.")) {
      return resp([{ data: { visits, rawVisits: visits, pagesPerVisit: 2, topCountries: [{ code: pais }], category }, fetched_at: new Date().toISOString() }]);
    }
    const raiz = u.match(/^https?:\/\/(?:www\.)?([^/]+)\/?$/);
    if (raiz) return resp(homes[raiz[1]] || html);
    return resp([]);
  };
  return reg;
}

// ═══ S1 — el streaming legítimo entra ════════════════════════════════════════════════════════════════════
const MEDIOS_DE_STREAMING = [
  { id: 701, domain: "tv-programme.com", category: "arts_and_entertainment/tv_movies_and_streaming", pais: "FR",
    title: "Programme TV ce soir : films, series et streaming - tv-programme.com" },
  { id: 702, domain: "filmelier.com", category: "Arts_and_Entertainment/TV_Movies_and_Streaming", pais: "BR",
    title: "Filmelier: onde assistir filmes e series no streaming" },
  // La categoría de música de SimilarWeb nunca estuvo en CATEGORIAS_NUNCA: a la guía de música la frenaba el
  // "streaming" que la heurística guardaba, en el agente (ver S5). Acá se fija que entra y con qué categoría.
  { id: 703, domain: "guiademusicadeprueba.com.ar", category: "Arts_and_Entertainment/Music", pais: "AR",
    title: "Guia de musica en streaming: radios, playlists y estrenos" },
];

for (const s of MEDIOS_DE_STREAMING) {
  test(`S1 processCsvItem de verdad: ${s.domain} (${s.category}) con ads.txt y tráfico en rango entra a Prospects`, async () => {
    const reg = red({ category: s.category, pais: s.pais, html: home(s.title) });
    await W.processCsvItem("t", itemDe(s.id, s.domain), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
    const p = patchesDeCola(reg, s.id);
    ok(!p.some(x => /no_prospectable_tipo|tipo_no_prospectable/.test(x.error_message || "")),
      `decisión del dueño del 13/09: el streaming ya no es un tipo no prospectable (${JSON.stringify(p)})`);
    const altas = altasEnProspects(reg);
    strictEqual(altas.length, 1, `tenía que llegar a guardarse en Prospects: ${JSON.stringify(p)}`);
    strictEqual(altas[0].domain, s.domain);
    strictEqual(altas[0].category, "streaming", "la heurística le guarda 'streaming' (legítimo), no 'streaming_pirata'");
    deepStrictEqual(p.map(x => x.status), ["done"]);
    ok(!reg.some(r => r.m === "POST" && r.u.includes("toolbar_diag_descartes") && /tipo_de_negocio/.test(r.b)), "sin diagnóstico de descarte por tipo");
  });
}

test("S1: la lista compartida de tipos no prospectables ya no tiene streaming y no se lleva ninguna categoría de streaming", () => {
  ok(!W.CATEGORIAS_NUNCA.some(x => /stream/i.test(x)), `CATEGORIAS_NUNCA todavía nombra streaming: ${W.CATEGORIAS_NUNCA.filter(x => /stream/i.test(x))}`);
  for (const c of [
    "arts_and_entertainment/tv_movies_and_streaming", "Arts_and_Entertainment/TV_Movies_and_Streaming",
    "Arts & Entertainment > TV, Movies, and Streaming", "Music & Audio > Music Streaming", "arts_and_entertainment/music",
    "streaming", "video_streaming", "tv_streaming", "arts_and_entertainment/streaming",
  ]) strictEqual(W._categoriaNuncaProspectable(c), null, `"${c}" no es un tipo no prospectable desde el 13/09`);
  strictEqual(W._veredictoPorSimilarWeb({ category: "arts_and_entertainment/tv_movies_and_streaming", traffic: 2_000_000 }), "publisher",
    "con el sitio bloqueado, SimilarWeb lo sigue leyendo como medio");
});

// ═══ S2 — las mismas puertas que cualquier medio ═════════════════════════════════════════════════════════
test("S2 processCsvItem de verdad: un sitio de streaming sin ads.txt sigue afuera", async () => {
  const reg = red({ category: "arts_and_entertainment/tv_movies_and_streaming", conAdsTxt: false, html: home("Guia de series en streaming") });
  await W.processCsvItem("t", itemDe(711, "guiadeseriesdeprueba.fr"), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  deepStrictEqual(patchesDeCola(reg, 711).map(x => x.error_message), ["not_publisher: sin_ads_txt"]);
  strictEqual(altasEnProspects(reg).length, 0);
});

for (const [id, domain] of [[712, "netflix.com"], [713, "spotify.com"]]) {
  test(`S2 processCsvItem de verdad: ${domain} sigue afuera por marca, sin gastar nada`, async () => {
    const reg = red({ category: "arts_and_entertainment/tv_movies_and_streaming", pais: "BR", html: home("Streaming") });
    await W.processCsvItem("t", itemDe(id, domain), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
    const p = patchesDeCola(reg, id);
    strictEqual(p.length, 1, JSON.stringify(p));
    match(p[0].error_message, /^blocked: corporate\//, "la blocklist de marcas lo frena antes de todo");
    ok(!reg.some(r => /ads\.txt|toolbar_traffic_cache|rapidapi/.test(r.u)), "ni ads.txt ni tráfico");
    strictEqual(altasEnProspects(reg).length, 0);
  });
}

test("S2 processCsvItem de verdad: una plataforma de streaming gigante sigue afuera por el techo de tráfico", async () => {
  const reg = red({ category: "arts_and_entertainment/tv_movies_and_streaming", visits: 60_000_000, html: home("Mega streaming") });
  await W.processCsvItem("t", itemDe(714, "megastreamingdeprueba.fr"), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
  const p = patchesDeCola(reg, 714);
  strictEqual(p.length, 1, JSON.stringify(p));
  match(p[0].error_message, /above max 40000000/);
  strictEqual(altasEnProspects(reg).length, 0);
});

// ═══ S3 — los otros tipos no prospectables no cambian ═══════════════════════════════════════════════════
for (const [id, domain, category, tipo] of [
  [721, "sitiounodeprueba.com.pe", "Gambling/Casinos", "gambling"],
  [722, "sitiodosdeprueba.com.pe", "finance/banking_credit_and_lending", "banking"],
  [723, "sitiotresdeprueba.com.pe", "e-commerce_and_shopping/marketplace", "e-commerce"],
]) {
  test(`S3 processCsvItem de verdad: ${category} sigue vetado por tipo aunque tenga ads.txt y tráfico`, async () => {
    const reg = red({ category, pais: "PE", html: home("Portal de prueba") });
    await W.processCsvItem("t", itemDe(id, domain), { rapidapi_key: "" }, USO_APOLLO, { count: 0 });
    const p = patchesDeCola(reg, id);
    strictEqual(p.length, 1, JSON.stringify(p));
    match(p[0].error_message, new RegExp(`^no_prospectable_tipo: ".*" es ${tipo} \\(ni con ads\\.txt ni con tráfico\\)`));
    strictEqual(W._motivoCanonicoCola(p[0].error_message), `tipo_no_prospectable:${tipo}`, "el parte lo sigue agrupando igual");
    strictEqual(altasEnProspects(reg).length, 0);
    const diag = reg.filter(r => r.m === "POST" && r.u.includes("toolbar_diag_descartes")).map(r => r.b).join("\n");
    ok(!/porno\/streaming/.test(diag), "el comentario del diagnóstico ya no dice que el streaming no entra");
  });
}

test("S3: la lista compartida sigue vetando los demás tipos", () => {
  for (const [c, tipo] of [
    ["gambling/casinos", "gambling"], ["adult", "adult"], ["finance/banking_credit_and_lending", "banking"],
    ["e-commerce_and_shopping/marketplace", "e-commerce"], ["computers_electronics_and_technology/search_engines", "search_engines"],
    ["computers_electronics_and_technology/web_hosting_and_domain_names", "web_hosting"],
    ["computers_electronics_and_technology/file_sharing_and_hosting", "file_sharing"],
  ]) strictEqual(W._categoriaNuncaProspectable(c), tipo, c);
});

// ═══ S4 — lo que no se abre ═════════════════════════════════════════════════════════════════════════════
test("S4: la regla pirata separa lo pirata y las retransmisiones del streaming de música, películas, series y TV", () => {
  for (const t of ["Cuevana 3 | peliculas y series gratis", "ver peliculas online gratis hd", "123movies - watch free", "futbol en vivo hoy", "NBA live stream", "pelisplus"]) {
    strictEqual(W._esStreamingPirata(t), true, t);
  }
  for (const t of ["Filmelier: onde assistir filmes e series no streaming", "Programme TV ce soir : films, series et streaming", "Guia de musica en streaming", "tv-programme.com", ""]) {
    strictEqual(W._esStreamingPirata(t), false, t);
  }
  ok(W.BLOCKED_CATEGORIES.has("streaming_pirata") && !W.BLOCKED_CATEGORIES.has("streaming"), [...W.BLOCKED_CATEGORIES].join(","));
  ok(W.BLOCKED_CATEGORIES.has("adult") && W.BLOCKED_CATEGORIES.has("gambling"), "adulto y apuestas siguen");
});

test("S4 fetchPageContent de verdad: la heurística guarda 'streaming' al legítimo, 'streaming_pirata' al pirata y 'piracy' al que tiene magnet", async () => {
  red({ homes: {
    "guiastreamingdeprueba.com.ar": home("Guia de series y peliculas en streaming"),
    "cuevanadeprueba.net": home("Cuevana | peliculas y series gratis"),
    "torrentsdeprueba.net": home("Descargas de la semana", `<a href="magnet:?xt=urn:btih:abcdef">bajar</a>`),
  } });
  const legit = await W.fetchPageContent("guiastreamingdeprueba.com.ar");
  strictEqual(legit?.category, "streaming");
  strictEqual(legit?.nonPublisherType, null, "el streaming legítimo no es un tipo no-publisher");
  strictEqual((await W.fetchPageContent("cuevanadeprueba.net"))?.category, "streaming_pirata");
  strictEqual((await W.fetchPageContent("torrentsdeprueba.net"))?.nonPublisherType, "piracy", "piracyRe no se tocó");
});

test("S4 scoreWebsite: el streaming pirata no recibe mail; tampoco la fila vieja 'streaming' con título o dominio pirata", () => {
  const medio = { domain: "filmelier.com", category: "streaming", traffic: 2_000_000, geo: "Brazil", language: "pt", page_title: "Filmelier: onde assistir filmes e series no streaming" };
  ok(W.scoreWebsite(medio).score >= 0, JSON.stringify(W.scoreWebsite(medio)));
  deepStrictEqual(W.scoreWebsite({ ...medio, category: "streaming_pirata" }).reasons, ["cat_blocked:streaming_pirata"]);
  deepStrictEqual(W.scoreWebsite({ ...medio, domain: "cuevana3.io", page_title: "" }).reasons, ["cat_blocked:streaming_pirata"], "fila vieja: el dominio lo delata");
  deepStrictEqual(W.scoreWebsite({ ...medio, domain: "peliculashd.net", page_title: "Ver peliculas online gratis" }).reasons, ["cat_blocked:streaming_pirata"], "fila vieja: el título lo delata");
  strictEqual(W.scoreWebsite({ ...medio, category: "gambling" }).score, -1);
  strictEqual(W.scoreWebsite({ ...medio, category: "adult" }).score, -1);
  // El agente trae el título en la consulta del pool: sin eso, la regla de las filas viejas sólo vería el dominio.
  const i = worker.indexOf("const _urlPool");
  ok(i > 0, "no encontré la consulta del pool del agente");
  match(worker.slice(i, worker.indexOf("\n", worker.indexOf("&select=", i))), /&select=[^&`]*\bpage_title\b/);
});

// ═══ S5 — el barrido y el chequeo al enviar no lo sacan ══════════════════════════════════════════════════
test("S5 barridoNoPublisher de verdad: no marca al medio de streaming que entró; al pirata con magnet sí", async () => {
  const filas = [
    { id: 801, domain: "tv-programme.com", category: "streaming", traffic: 2_000_000, geo: "France", created_at: "2026-09-13T20:00:00Z" },
    { id: 802, domain: "bajadasdeprueba.net", category: "other", traffic: 2_000_000, geo: "Spain", created_at: "2026-09-13T20:01:00Z" },
  ];
  const reg = red({
    homes: {
      "tv-programme.com": home("Programme TV ce soir : films, series et streaming"),
      "bajadasdeprueba.net": home("Descargas de la semana", `<a href="magnet:?xt=urn:btih:abcdef">bajar</a>`),
    },
    extra: [[(u, m) => m === "GET" && u.includes("toolbar_review_queue?status=eq.pending&suspect_reject=not.is.true&created_at=gt."), () => resp(filas)]],
  });
  await W.barridoNoPublisher("t");
  const marcas = reg.filter(r => r.m === "PATCH" && r.u.includes("toolbar_review_queue?id=eq.")).map(r => ({ id: r.u.match(/id=eq\.(\d+)/)[1], b: JSON.parse(r.b) }));
  ok(!marcas.some(x => x.id === "801"), `el barrido marcó al medio de streaming: ${JSON.stringify(marcas)}`);
  ok(marcas.some(x => x.id === "802" && x.b.suspect_reject === true && /^barrido: nonpub_piracy/.test(x.b.suspect_reason)),
    `la piratería con marcado se sigue marcando: ${JSON.stringify(marcas)}`);
});

test("S5 chequeo al enviar: la URL de un medio de streaming pasa y el gate del agente no lo frena", () => {
  for (const [domain, category] of [["tv-programme.com", "streaming"], ["filmelier.com", "streaming"], ["guiademusicadeprueba.com.ar", "streaming"]]) {
    const v = W.classifyByUrlOnly(domain, category, 2_000_000);
    strictEqual(W._veredictoUrlAlEnviar(v, null, 2_000_000), "enviar", `${domain}: ${JSON.stringify(v)}`);
    ok(W.scoreWebsite({ domain, category, traffic: 2_000_000 }).score >= 0, `${domain} no puede caer en el gate duro`);
  }
  // Las marcas de streaming siguen siendo un rubro por URL: sin ads.txt confirmado no salen (y la blocklist las frena antes).
  strictEqual(W._veredictoUrlAlEnviar(W.classifyByUrlOnly("netflix.com", "", 2_000_000), { state: "no" }, 2_000_000), "rechazar");
});

// ═══ S6 — la extensión ═════════════════════════════════════════════════════════════════════════════════
// El detector de no-publisher del popup, tal cual está en popup.js (corre dentro de la página con executeScript).
const detectorDelPopup = (() => {
  const ini = popup.indexOf('let nonPublisherType = "";');
  const fin = popup.indexOf("return {", ini);
  ok(ini > 0 && fin > ini, "no encontré el detector de no-publisher en popup.js");
  return new Function("document", `${popup.slice(ini, fin)}\nreturn nonPublisherType;`);
})();
const docCon = (html) => ({ documentElement: { innerHTML: html } });

test("S6 extensión: el detector del popup no marca por tipo a un sitio de streaming; a la piratería con marcado sí", () => {
  strictEqual(detectorDelPopup(docCon(home("Guia de series y peliculas en streaming"))), "", "sin ads");
  strictEqual(detectorDelPopup(docCon(home("Musica en streaming", `<script src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js"></script>`))), "", "con AdSense");
  strictEqual(detectorDelPopup(docCon(home("Descargas", `<a href="magnet:?xt=urn:btih:abcdef">bajar</a>`))), "piracy / brand-unsafe");
  ok(!/nonPublisherType = "[^"]*stream/i.test(popup), "ningún tipo del popup nombra streaming");
});

test("S6 extensión: la categoría de streaming no está bloqueada y el panel ya no anuncia streaming como veto", () => {
  deepStrictEqual(isCategoryBlocked("arts_and_entertainment/tv_movies_and_streaming"), { blocked: false });
  deepStrictEqual(isCategoryBlocked("Music & Audio > Music Streaming"), { blocked: false });
  strictEqual(isCategoryBlocked("Banking, Credit, and Lending").blocked, true, "la banca sigue bloqueada");
  match(popupHtml, /Hard gates<\/strong>: Adult \/ Streaming pirata \/ Gambling/);
});
