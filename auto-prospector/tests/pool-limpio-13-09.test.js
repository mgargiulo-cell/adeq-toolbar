// El pool que ve el media buyer: que lo que entra sea bueno, que lo malo salga, y que lo que ya
// está no se ensucie. (2026-09-13, auditoría pedida por el dueño)
//
// "La clave es mantener limpio el pool para que los media buyers puedan prospectar, y que la
// información de Prospects sea la misma que aparece en Análisis." Cada test de este archivo es una
// regla que la auditoría encontró rota con el código real:
//   1. Congelados por falta de tráfico: el castigo 15→30→60 días nunca pasaba de 15.
//   2. Reciclables: el feeder por slot no cruzaba contra Prospects y le borraba la espera de
//      búsqueda de email a leads que ya estaban en el pool.
//   3. Marcas y TLD vetados: los feeders los dejaban pasar y la cola los descartaba después.
//   4. Una dirección ADIVINADA que MillionVerifier rechaza quemaba el dominio entero.
//   5. La mejora de contacto de la auditoría del pool no corría para nadie.
//   6. Los topes diarios de rol_mx y del patrón se reiniciaban con cada reinicio del worker.
//   7. Números del informe: el rescate de Apollo, la lectura fallida como cero, el stock partido.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { cargarWorker } from "./_worker-exportado.mjs";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const worker = fs.readFileSync(path.join(RAIZ, "index.js"), "utf8");
const entre = (desde, hasta) => worker.slice(worker.indexOf(desde), worker.indexOf(hasta, worker.indexOf(desde)));
const cuerpoDe = (nombre) => {
  const i = worker.indexOf(`function ${nombre}(`);
  ok(i >= 0, `no encontré la función ${nombre}`);
  return worker.slice(i, worker.indexOf("\n}\n", i));
};
const respuesta = (body, { status = 200 } = {}) => ({ ok: status >= 200 && status < 300, status, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) });

// ── 1. Congelados ───────────────────────────────────────────────────────────────────────
test("el castigo de un congelado por falta de tráfico escala 15 → 30 → 60 días y blocklist", async () => {
  const { _backoffCongelado } = await cargarWorker(["_backoffCongelado"]);
  deepStrictEqual(_backoffCongelado({ attemptFila: 0, errorMessage: "" }), { prevFreeze: 0, dias: 15, blocklist: false, attemptNuevo: 1 }, "lead nuevo");
  deepStrictEqual(_backoffCongelado({ attemptFila: 0, errorMessage: "unfrozen_retry_attempt_2 freeze_1" }), { prevFreeze: 1, dias: 30, blocklist: false, attemptNuevo: 2 },
    "el unfreezer borró la fila, pero la marca freeze_1 dice que ya se congeló una vez");
  deepStrictEqual(_backoffCongelado({ attemptFila: 0, errorMessage: "unfrozen_retry_attempt_3 freeze_2" }), { prevFreeze: 2, dias: 60, blocklist: true, attemptNuevo: 3 },
    "tercer ciclo: 60 días y blocklist 'inoperativo', que nunca se había ejecutado");
  strictEqual(_backoffCongelado({ attemptFila: 0, errorMessage: "traffic_api_transient retry_4 (reintento sin penalizar)" }).dias, 15, "sin marca: igual que antes, nunca peor");
  strictEqual(_backoffCongelado({ attemptFila: 2, errorMessage: "" }).dias, 60, "si la fila existe, manda la fila");
});

test("el unfreezer deja la marca sólo en los congelados por tráfico, y la marca no rompe el conteo de intentos", () => {
  ok(/select=domain,source,uploaded_by,attempt_count,last_error&limit=20/.test(worker), "el unfreezer tiene que leer last_error para saber de qué congelado se trata");
  ok(/row\.last_error === "no_traffic_data_after_3_attempts" \? ` freeze_\$\{row\.attempt_count \|\| 1\}` : ""/.test(worker),
     "los congelados de rebotes o re-engagement no heredan el ciclo de tráfico");
  const msg = "unfrozen_retry_attempt_2 freeze_1";
  strictEqual(parseInt(msg.match(/attempt_(\d+)/)?.[1] || "0", 10), 2, "sigue dando prevAttempts=2: se re-congela al primer fallo, una sola consulta paga");
  strictEqual(msg.match(/retry_(\d+)/), null, "y no se confunde con un reintento transitorio");
  ok(/_backoffCongelado\(\{ attemptFila: /.test(entre("async function processCsvItem(", "async function runCsvQueue(")), "processCsvItem usa la regla probada acá");
});

// ── 2. Reciclables ──────────────────────────────────────────────────────────────────────
test("los dos reciclados cruzan contra Prospects con la misma función, y un fallo de lectura es null, no 'ninguno'", async () => {
  const { _dominiosPendientesEnProspects } = await cargarWorker(["_dominiosPendientesEnProspects"], { fetchFalso: true });
  globalThis.__fetchFalso = async () => respuesta([{ domain: "Diario.com.ar" }]);
  deepStrictEqual([...await _dominiosPendientesEnProspects("t", ["diario.com.ar", "otro.pe"])], ["diario.com.ar"]);
  globalThis.__fetchFalso = async () => respuesta({ message: "boom" }, { status: 500 });
  strictEqual(await _dominiosPendientesEnProspects("t", ["diario.com.ar"]), null);
  ok(/_dominiosPendientesEnProspects\(token, pool\)/.test(cuerpoDe("_feederPullMonday")) && /!_yaEnProspects\.has\(d\)/.test(cuerpoDe("_feederPullMonday")),
     "el feeder por slot no miraba Prospects: el reciclable volvía a la cola");
  ok(/_dominiosPendientesEnProspects\(token, todos\)/.test(cuerpoDe("sincronizarFinalizadosDeMonday")), "el barrido diario usa la misma función");
});

test("borrar la marca de búsqueda de email nunca toca a un lead que ya está pendiente en Prospects", async () => {
  const { _limpiarMarcaDeEmail } = await cargarWorker(["_limpiarMarcaDeEmail"], { fetchFalso: true });
  const pedidos = [];
  globalThis.__fetchFalso = async (url, opts = {}) => { pedidos.push({ u: String(url), m: opts.method }); return respuesta([]); };
  await _limpiarMarcaDeEmail("t", ["diario.com.ar"]);
  const patch = pedidos.find(p => p.m === "PATCH");
  ok(patch && patch.u.includes("status=neq.pending"), `el PATCH tiene que excluir los pending: ${patch?.u}`);
});

// ── 3. Marcas y TLD vetados en los feeders ──────────────────────────────────────────────
test("una marca se reconoce con cualquier TLD o subdominio, en los feeders igual que en la cola", async () => {
  const { esMarcaBloqueada, esTldVetado, isDomainBlocked } = await cargarWorker(["esMarcaBloqueada", "esTldVetado", "isDomainBlocked"]);
  for (const d of ["zalando.de", "news.google.at", "rakuten.tv", "amazon.com.br", "mercadolibre.com.ar", "binance.com"]) ok(esMarcaBloqueada(d), `${d} es una marca bloqueada`);
  for (const d of ["clarin.com", "infobae.com", "lanacion.com.ar", "live.bbc.co.uk"]) ok(!esMarcaBloqueada(d), `${d} es un medio`);
  // mercadolibre.com.ar está además en la lista corporativa exacta, que se mira antes: cualquier
  // motivo corporativo vale. Lo que importa es que la rama de marca use el mismo helper.
  for (const d of ["mercadolibre.com.ar", "zalando.de", "news.google.at"]) ok(/^corporate\//.test(String(isDomainBlocked(d))), `${d} tiene que salir bloqueado: ${isDomainBlocked(d)}`);
  ok(/if \(esMarcaBloqueada\(d\)\) return "corporate\/brand-root";/.test(worker), "isDomainBlocked y los feeders usan el mismo criterio de marca");
  ok(esTldVetado("lenta.ru") && esTldVetado("sport.ua") && !esTldVetado("rusia.com"), "TLD vetado por sufijo, no por subcadena");
  const _codigo = worker.split("\n").filter(l => !l.trim().startsWith("//"));   // el comentario que explica el bug lo cita
  strictEqual(_codigo.filter(l => /BRAND_BLOCKLIST\.has\(d\)/.test(l)).length, 0, "`BRAND_BLOCKLIST.has(d)` con el dominio entero no filtra nada: no puede volver");
  strictEqual((worker.match(/esMarcaBloqueada\(d\) \|\| esTldVetado\(d\)|!esMarcaBloqueada\(d\) && !esTldVetado\(d\)/g) || []).length, 4, "los cuatro prefiltros (AutoGoogle, sellers de Google, Majestic, similares)");
});

// ── 4. Rebotes de direcciones adivinadas ────────────────────────────────────────────────
test("dos direcciones ADIVINADAS rechazadas por MillionVerifier no queman el dominio; dos rebotes reales sí", async () => {
  const w = await cargarWorker(["_recontarRebotesPorDominio", "_cuentaParaElDominio", "rankEmail"]);
  const real = "publicidad@diario.com.ar";
  w._recontarRebotesPorDominio([
    { email: "contacto@diario.com.ar", evidencia: "verificador", fuente: "rol_mx" },
    { email: "redaccion@diario.com.ar", evidencia: "verificador", fuente: { source: "rol_mx" } },
  ]);
  ok(w.rankEmail(real, "diario.com.ar") > 0, `un email publicado después tiene que poder usarse: dio ${w.rankEmail(real, "diario.com.ar")}`);
  w._recontarRebotesPorDominio([
    { email: "contacto@diario.com.ar", evidencia: "rebote_smtp", fuente: "scrape" },
    { email: "redaccion@diario.com.ar", evidencia: "rebote_smtp", fuente: "scrape" },
  ]);
  strictEqual(w.rankEmail(real, "diario.com.ar"), -1, "dos rebotes SMTP reales siguen bloqueando el dominio, como antes");
  ok(w._cuentaParaElDominio({ evidencia: "verificador", fuente: "scrape" }), "un 'no' de MV sobre una dirección publicada sigue contando");
  ok(!w._cuentaParaElDominio({ evidencia: "verificador", fuente: "pattern" }), "una hipótesis de patrón rechazada no cuenta");
  w._recontarRebotesPorDominio([]);   // no dejar estado compartido para otros tests
});

// ── 5. La mejora de contacto de la auditoría del pool ──────────────────────────────────
test("'de última' es un genérico no comercial o una dirección adivinada, venga la fuente como venga", async () => {
  const { _esEmailDeUltima } = await cargarWorker(["_esEmailDeUltima"]);
  ok(_esEmailDeUltima("info@diario.it", "scrape") && _esEmailDeUltima("contacto@diario.com", "generic"), "info@ y contacto@ publicados son de última");
  ok(!_esEmailDeUltima("publicidad@diario.com", "scrape") && !_esEmailDeUltima("comercial@diario.com", "scrape") && !_esEmailDeUltima("marketing@diario.com", "scrape"),
     "los comerciales nunca son de última, aunque figuren en la lista de genéricos");
  ok(!_esEmailDeUltima("redaccion@diario.com", "scrape"), "redaccion@ publicado no es de última");
  ok(_esEmailDeUltima("redaccion@diario.com", "rol_mx") && _esEmailDeUltima("redaccion@diario.com", { source: "rol_mx" }), "pero adivinado sí: no es un contacto publicado");
  ok(!_esEmailDeUltima("juan.perez@diario.com", "scrape"), "una persona no es de última");
});

test("la auditoría del pool decide 'de última' con esa regla, busca DESPUÉS de aplicar sus planes, y sin gastar Serper", () => {
  const fn = cuerpoDe("auditarEmailsDelPool");
  ok(!/rankEmail\(e, l\.domain, l\.category \|\| ""\) < 40/.test(fn), "el umbral de 40 creía que info@ valía 15 (vale 55): la búsqueda no corría para nadie");
  ok(/ms\.every\(e => _esEmailDeUltima\(e, fuentes\[String\(e\)\.toLowerCase\(\)\]\)\)/.test(fn));
  ok(/\.filter\(x => x\.s > 0 && !_esEmailDeUltima\(x\.e, "scrape"\)\)/.test(fn), "un contact@ nuevo no es una mejora");
  ok(fn.indexOf('setConfigValue(token, "auditoria_emails_cursor", ultimo)') < fn.indexOf("BUSCAR UNO MEJOR DONDE SOLO HAY"),
     "si busca antes de aplicar, el PATCH de planes pisa lo encontrado y un reinicio pierde el cursor");
  ok(/scrapeEmailsForDomain\(lead\.domain, \{ sinSerper: true \}\)/.test(fn), "la mejora promete ser gratis");
  ok(/SERPER_API_KEY && !opts\.sinSerper\)/.test(cuerpoDe("scrapeEmailsForDomain")), "y scrapeEmailsForDomain respeta la opción");
});

// ── 6. Topes diarios que sobreviven al reinicio ─────────────────────────────────────────
test("el tope diario se siembra desde la config y no vuelve a cero con cada reinicio", async () => {
  const { _sembrarTopeDiario } = await cargarWorker(["_sembrarTopeDiario"]);
  deepStrictEqual(_sembrarTopeDiario({ dia: "", n: 0 }, { polish_rol_mx_used: "2026-09-15:41" }, "polish_rol_mx_used", "2026-09-15"), { dia: "2026-09-15", n: 41 }, "reinicio a mitad del día");
  deepStrictEqual(_sembrarTopeDiario({ dia: "", n: 0 }, { polish_rol_mx_used: "2026-09-14:60" }, "polish_rol_mx_used", "2026-09-15"), { dia: "2026-09-15", n: 0 }, "día nuevo");
  const mismo = { dia: "2026-09-15", n: 7 };
  strictEqual(_sembrarTopeDiario(mismo, {}, "polish_rol_mx_used", "2026-09-15"), mismo, "mismo día en el mismo proceso: no se relee");
  const polish = cuerpoDe("polishPool");
  for (const clave of ["polish_patron_used", "polish_rol_mx_used"]) {
    ok(polish.includes(`_sembrarTopeDiario(`) && polish.includes(`"${clave}", _mDay)`), `falta sembrar ${clave}`);
    ok(polish.includes(`setConfigValue(token, "${clave}"`), `falta persistir ${clave} en cada uso`);
  }
});

// ── 7. Los números del informe ──────────────────────────────────────────────────────────
test("Apollo sólo marca rescate si el lead no tenía ningún email", () => {
  const fn = cuerpoDe("apolloQuemarCiclo");
  ok(/if \(!cur\.length\) patch\.email_found_at = new Date\(\)\.toISOString\(\);/.test(fn), "un info@ más una persona de Apollo es una mejora, no un rescate");
  ok(!/^\s*patch\.email_found_at = new Date/m.test(fn), "no puede quedar una escritura sin condición");
});

test("el boletín no muestra una lectura fallida como cero ni la pinta de verde, y el stock sin email es la misma población", () => {
  const bol = cuerpoDe("_boletinPorSeccion");
  ok(/const _cnt = async \(url\) => \{\s*try \{\s*const r = await fetch\([^;]+;\s*(?:\/\/[^\n]*\n\s*)*if \(!r\.ok\) return null;/.test(bol), "un 500 no es un cero");
  ok(/_rescatados == null \|\| _mudos == null \? "🟡"/.test(bol), "un conteo ilegible nunca es ✅");
  ok(/if \(n == null\) _porSt\[st\] = "\?";/.test(bol), "la cola no esconde un estado que no se pudo leer");
  const salud = cuerpoDe("enviarResumenSalud");
  ok(!/emails=eq\.%5B%5D&email_ultimo_motivo=not\.is\.null/.test(salud), "el 'stock' filtraba por motivo y salía con otro número que 'quedan N sin email'");
  ok(/SIN EMAIL — hay \$\{_pendSinMail\.length\} pendientes sin email/.test(salud));
});

// ── 8. Lo que no cambia reglas y se deja fijado ─────────────────────────────────────────
test("sellers de Google no busca TLDs que la cola descarta (Ucrania está vetada por decisión del user)", async () => {
  const { _TLDS_OBJETIVO_GOOGLE, BLACKLIST_TLDS } = await cargarWorker(["_TLDS_OBJETIVO_GOOGLE", "BLACKLIST_TLDS"]);
  const choques = [..._TLDS_OBJETIVO_GOOGLE].filter(t => BLACKLIST_TLDS.some(b => t === b || t.endsWith(b)));
  deepStrictEqual(choques, [], "un TLD objetivo que está en BLACKLIST_TLDS es trabajo tirado: ads.txt bajado y carril ocupado para un descarte seguro");
});

test("la caché negativa vence antes del primer congelado, y la blocklist 'inoperativo' nunca se decide con un dato de caché", () => {
  const ttl = parseInt(worker.match(/let _trafficNegCacheDias = (\d+);/)?.[1] || "0", 10);
  const primerCongelado = parseInt(cuerpoDe("_backoffCongelado").match(/prevFreeze === 0 \? (\d+)/)?.[1] || "0", 10);
  ok(ttl > 0 && primerCongelado > 0, "no encontré los dos plazos");
  ok(ttl < primerCongelado, `el negativo (${ttl}d) tiene que vencer antes del descongelado (${primerCongelado}d): cada vuelta se decide con un "no" recién preguntado`);
  ok(/if \(prevFreeze >= 2 && !trafficData\.fromCache\) \{/.test(worker), "con el castigo progresivo funcionando, el tercer congelado es permanente: no puede salir de un dato guardado");
});

test("la tarjeta de Prospects marca las direcciones adivinadas y preselecciona una publicada", () => {
  const popup = fs.readFileSync(path.join(RAIZ, "..", "popup", "popup.js"), "utf8");
  ok(/rol_mx:\s+\{ txt: "adivinado \(no publicado\)"/.test(popup), "el detalle del chip mostraba el texto crudo 'rol_mx'");
  ok(/adivinado · no publicado<\/span>/.test(popup), "la lista de radios no distinguía la dirección adivinada de la publicada");
  // Desde ranking_extension-13-09b la preselección de la tarjeta sale de la regla compartida con Análisis
  // (_elegirPreseleccionClient): la misma preferencia por la publicada, sobre el orden compartido y sin
  // dejar puesta nunca una dirección de tier -1. El caso se prueba con el código real en ese archivo.
  ok(/const _idxPreseleccion = emails\.indexOf\(_preseleccion\);/.test(popup), "la tarjeta preselecciona con la regla compartida");
  ok(/elegibles\.find\(e => _fuenteTextoClient\(ctx\.fuente\(e\)\) !== "rol_mx"\) \|\| elegibles\[0\]/.test(popup), "si hay una publicada, la adivinada no queda preseleccionada");
});
