// Extracción de emails del sitio: tres causas raíz que perdían o mal explicaban emails. (2026-09-13)
//
// El dueño pidió que Prospects muestre lo mismo que Análisis. La extensión lee el DOM entero
// (modules/scraper.js: innerText, innerHTML y a.href con decodeURIComponent) y el worker no:
//   C9  El filtro anti-trampas borraba bloques VISIBLES: "overflow-hidden" (Tailwind),
//       "elementor-hidden-mobile", "hidden md:block", aria-hidden="true", y hasta un
//       title="a hidden gem". El email estaba en la web, el MB lo veía, el worker no.
//   C12 mailto con la arroba codificada (mailto:ventas%40medio.com): el navegador lo decodifica,
//       la extensión también, el worker exigía una "@" literal.
//   C10 Un 403 con cf-ray en CUALQUIER URL (una ruta adivinada, la casa editora, informer)
//       marcaba "waf_nos_bloqueo" y le ganaba a todo, aunque el sitio se hubiera leído y los
//       candidatos los hubiera descartado el ranking — el renglón que usa el dueño para
//       detectar filtros demasiado estrictos.
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

const W = await cargarWorker(["extractEmailsFromHtml", "_motivoSinEmail", "_comentarioSinEmail", "_esBloqueoWaf"]);

// Dirección NEUTRA a propósito: con bait@ o trap@ detectarTrampaEmail la tira igual y el test
// daría verde aunque el filtro de bloques ocultos estuviera roto.
const E = "ventas@medio.com";

// ── C9: el filtro anti-trampas sólo saca lo que el visitante NO ve ─────────────────────────
test("C9: los bloques visibles con 'hidden' dentro de otra clase conservan el email", () => {
  const visibles = [
    `<footer class="relative overflow-hidden bg-gray-900"><p>Publicidad: ${E}</p></footer>`,
    `<main class="overflow-x-hidden"><p>${E}</p></main>`,
    `<footer aria-hidden="true"><p>${E}</p></footer>`,
    `<div class="hidden md:block"><p>${E}</p></div>`,
    `<div class="relative hidden lg:flex"><p>${E}</p></div>`,
    `<div class="elementor-widget elementor-hidden-mobile"><p>${E}</p></div>`,
    `<div class="hidden-xs"><p>${E}</p></div>`,
    `<header class="navbar hide-on-scroll"><p>${E}</p></header>`,
    `<p title="a hidden gem">${E}</p>`,
    `<div class="md:hidden"><p>${E}</p></div>`,
    // Tailwind: sólo para lectores en el celular, visible desde md.
    `<div class="sr-only md:not-sr-only"><p>${E}</p></div>`,
    `<p class="show-hide-toggle">${E}</p>`,
  ];
  const perdidos = visibles.filter(h => !W.extractEmailsFromHtml(h).includes(E));
  deepStrictEqual(perdidos, [], "estos bloques se ven en el navegador: la extensión muestra el email y el worker lo perdía");
});

test("C9: lo que de verdad está oculto se sigue borrando", () => {
  const ocultos = [
    `<div hidden><p>${E}</p></div>`,
    `<div class="x" hidden data-a="1"><p>${E}</p></div>`,
    `<div hidden="hidden"><p>${E}</p></div>`,
    `<DIV HIDDEN><p>${E}</p></DIV>`,
    `<div style="display:none"><p>${E}</p></div>`,
    `<div class="honeypot"><p>${E}</p></div>`,
    `<div class="hidden"><p>${E}</p></div>`,
    `<div class="hidden x"><p>${E}</p></div>`,
    `<span class="foo sr-only">${E}</span>`,
    `<span class="screen-reader-text">${E}</span>`,
    `<span class='visually-hidden'>${E}</span>`,
    // La trampa de Mailchimp: sale por el style, no por el aria-hidden.
    `<div style="position: absolute; left: -5000px;" aria-hidden="true"><p>${E}</p></div>`,
    // Revisión del 13/09: pasar TODO a token exacto dejó entrar estos. Palabras que sólo existen
    // en trampas se buscan como texto suelto y sin mirar breakpoints.
    `<div class="form-honeypot"><p>${E}</p></div>`,
    `<div class="honeypot-field"><p>${E}</p></div>`,
    `<div class="hp-field-wrap"><p>${E}</p></div>`,
    `<div class="wpcf7-hp_field"><p>${E}</p></div>`,
    `<div class="honeypot lg:block"><p>${E}</p></div>`,
    `<div class="spam-trap"><p>${E}</p></div>`,
    `<div class="contact-nospam-box"><p>${E}</p></div>`,
    // Clases genéricas que de verdad ocultan (token exacto).
    `<div class="hide"><p>${E}</p></div>`,
    `<div class="box is-hidden"><p>${E}</p></div>`,
    `<span class="screen-reader-only">${E}</span>`,
    `<span class="visuallyhidden">${E}</span>`,
    // Atributo `hidden` seguido de otro atributo booleano.
    `<div hidden inert><p>${E}</p></div>`,
    `<div hidden data-x><p>${E}</p></div>`,
  ];
  const filtrados = ocultos.filter(h => W.extractEmailsFromHtml(h).includes(E));
  deepStrictEqual(filtrados, [], "un bloque invisible para el visitante es el escondite clásico de un email cebo");
});

// ── C10: el comentario explica el MISMO motivo que se guardó en la fila ─────────────────────
// polishPool guarda motivo y comentario en la misma fila de toolbar_diag_sin_email y el parte
// imprime uno debajo del otro. Con dos cascadas copiadas, el 13/09 salió
// "rechazados_por_ranking:tvcherpak@gmail.com" explicado como "el sitio está detrás de un WAF".
const FIRMA_DEL_MOTIVO = {
  rechazados_por_ranking:         /ranking las rechazó/,
  email_en_imagen:                /IMAGEN/,
  waf_nos_bloqueo:                /WAF/,
  no_se_pudo_leer_el_sitio:       /NINGUNA/,
  la_web_no_publica_ningun_email: /solo tiene formulario|No quedó registro/,
};
function comentarioDeOtroMotivo(motivo, comentario) {
  if (/sin explicación escrita/.test(comentario)) return `el motivo ${motivo} no tiene su case`;
  if (!FIRMA_DEL_MOTIVO[motivo].test(comentario)) return `${motivo} sin su explicación: ${comentario}`;
  const ajeno = Object.entries(FIRMA_DEL_MOTIVO).find(([m, re]) => m !== motivo && re.test(comentario));
  return ajeno ? `${motivo} explicado como ${ajeno[0]}: ${comentario}` : "";
}

test("C10: el comentario sin email corresponde al motivo, en los casos del parte", () => {
  const casos = [
    [{ crudos: 3, rechazados: ["tvcherpak@gmail.com"] }, { ok: 12, fail: 1, waf: true }, "rechazados_por_ranking"],
    [{ crudos: 0, rechazados: [] }, { ok: 4, fail: 0, waf: true, emailEnImagen: true }, "email_en_imagen"],
    [{ crudos: 2, rechazados: ["a@b.com"] }, { ok: 0, fail: 5 }, "rechazados_por_ranking"],
  ];
  for (const [diag, stats, esperado] of casos) {
    const motivo = W._motivoSinEmail(diag, stats).split(":")[0];
    strictEqual(motivo, esperado);
    // Igual que la llamada de polishPool.
    const comentario = W._comentarioSinEmail({ ...diag, ...stats, paginas: stats.ok });
    strictEqual(comentarioDeOtroMotivo(motivo, comentario), "");
  }
});

test("C10: motivo y comentario no pueden divergir en ninguna combinación", () => {
  const mal = [], vistos = new Set();
  for (const crudos of [0, 2]) for (const rechazados of [[], ["a@b.com"]])
  for (const ok of [undefined, 0, 5]) for (const fail of [0, 4])
  for (const waf of [false, true]) for (const emailEnImagen of [false, true])
  for (const conPaginas of [false, true]) {
    const diag = { crudos, rechazados }, stats = { ok, fail, waf, emailEnImagen };
    const motivo = W._motivoSinEmail(diag, stats).split(":")[0];
    vistos.add(motivo);
    const comentario = W._comentarioSinEmail({ ...diag, ...stats, ...(conPaginas ? { paginas: ok } : {}) });
    const error = comentarioDeOtroMotivo(motivo, comentario);
    if (error) mal.push(`${JSON.stringify({ ...diag, ...stats })} → ${error}`);
  }
  deepStrictEqual(mal, []);
  deepStrictEqual([...vistos].sort(), Object.keys(FIRMA_DEL_MOTIVO).sort(), "la matriz recorre los cinco motivos");
  // Sin segunda cascada: el comentario le pregunta el motivo a _motivoSinEmail.
  const i = worker.indexOf("function _comentarioSinEmail(");
  const cuerpo = worker.slice(i, worker.indexOf("\n}\n", i));
  ok(/_motivoSinEmail\(d, d\)/.test(cuerpo), "el comentario sale del motivo");
  ok(!/if \(d\.waf\)|d\.ok === 0 && d\.fail > 0/.test(cuerpo), "no vuelve a decidir el tipo por su cuenta");
});

// ── C12: mailto con la arroba codificada ────────────────────────────────────────────────────
test("C12: mailto con %40 / %2E se extrae, igual que en la extensión", () => {
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="mailto:ventas%40medio.com">Escribinos</a>`), [E]);
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="mailto:ventas%40medio.com?subject=Hola%20pauta">x</a>`), [E]);
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="mailto:ventas%40medio%2Ecom">x</a>`), [E]);
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="mailto:ventas&#37;40medio.com">x</a>`), [E], "la entidad del % también");
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="mailto:%20publicidad%40medio.com">x</a>`), ["publicidad@medio.com"]);
  // Los dos arreglos juntos: el caso típico de un footer de Tailwind con un botón.
  deepStrictEqual(W.extractEmailsFromHtml(`<footer class="overflow-hidden"><a href="mailto:ventas%40medio.com">Escribinos</a></footer>`), [E]);
});

test("C12: el mailto decodificado pasa por los mismos filtros que cualquier otro email", () => {
  deepStrictEqual(W.extractEmailsFromHtml(`<a href="mailto:trap%40medio.com">x</a>`), [], "el filtro de trampas sigue aplicando");
  deepStrictEqual(W.extractEmailsFromHtml(`<div class="hidden"><a href="mailto:oculto%40medio.com">x</a></div>`), [], "y el de bloques ocultos");
  deepStrictEqual(W.extractEmailsFromHtml(`<img src="logo%40medio.png"><a href="https://x.com/a%40b.com">x</a>`), [], "sólo dentro de un mailto, no en cualquier URL");
  // El caso "usuario @dominio" con espacio queda afuera a propósito: la extensión tampoco lo
  // extrae y daría falsos ("Seguinos en Instagram @medio.com.ar" → instagram@medio.com.ar).
  deepStrictEqual(W.extractEmailsFromHtml(`Seguinos en Instagram @medio.com.ar`), []);
});

// ── C10: el WAF no le gana a un sitio leído ──────────────────────────────────────────────────
test("C10: el motivo sin email pone primero lo encontrado y después el bloqueo", () => {
  ok(W._motivoSinEmail({ crudos: 3, rechazados: ["tvcherpak@gmail.com"] }, { ok: 12, fail: 1, waf: true }).startsWith("rechazados_por_ranking:"),
     "la web SÍ publica y el ranking lo descartó: ese es el renglón que muestra si el filtro es demasiado estricto");
  strictEqual(W._motivoSinEmail(null, { ok: 1, fail: 0, waf: true }), "waf_nos_bloqueo", "un 200 de informer no esconde el bloqueo");
  strictEqual(W._motivoSinEmail({ crudos: 0 }, { ok: 5, emailEnImagen: true, waf: true }), "email_en_imagen");
  strictEqual(W._motivoSinEmail({ crudos: 0 }, { ok: 0, fail: 4 }), "no_se_pudo_leer_el_sitio");
  strictEqual(W._motivoSinEmail({ crudos: 0 }, { ok: 6, fail: 0 }), "la_web_no_publica_ningun_email");
  strictEqual(W._motivoSinEmail(null, { ok: 6, fail: 0 }), "la_web_no_publica_ningun_email");
});

test("C10: un 403/503 es el WAF sólo si viene del propio sitio y trae evidencia de bloqueo", () => {
  const D = "medio.com";
  const nginx403 = "<html><head><title>403 Forbidden</title></head><body><center><h1>403 Forbidden</h1></center><hr><center>nginx</center></body></html>";
  strictEqual(W._esBloqueoWaf({ url: "https://medio.com", status: 403, cfMitigated: "challenge", cfRay: "8a1-EZE" }, D), true, "challenge en el home");
  strictEqual(W._esBloqueoWaf({ url: "https://www.medio.com/contacto", status: 403, cfRay: "8a1", cuerpo: "<title>Attention Required! | Cloudflare</title>" }, D), true, "página de bloqueo de Cloudflare");
  strictEqual(W._esBloqueoWaf({ url: "https://comercial.medio.com/", status: 503, cfMitigated: "challenge" }, D), true, "un subdominio del sitio también es el sitio");
  strictEqual(W._esBloqueoWaf({ url: "https://medio.com", status: 403, cfMitigated: "challenge" }, "www.medio.com"), true);
  // xemboi.xemtuong.net: /sitemap → /sitemap/ da el 403 de directorio del ORIGEN, con cf-ray
  // porque todo lo que pasa por Cloudflare lo lleva. No es un bloqueo al crawler.
  strictEqual(W._esBloqueoWaf({ url: "https://medio.com/sitemap/", status: 403, cfRay: "8a1", cuerpo: nginx403 }, D), false, "cf-ray solo no prueba nada");
  strictEqual(W._esBloqueoWaf({ url: "https://medio.com/.htaccess", status: 403, cfRay: "8a1", cuerpo: "" }, D), false);
  strictEqual(W._esBloqueoWaf({ url: "https://website.informer.com/medio.com", status: 403, cfMitigated: "challenge", cfRay: "8a1" }, D), false, "informer bloqueando no es el sitio");
  strictEqual(W._esBloqueoWaf({ url: "https://grupoeditor.com/contacto", status: 403, cfMitigated: "challenge" }, D), false, "la casa editora no es el sitio");
  strictEqual(W._esBloqueoWaf({ url: "https://notmedio.com/", status: 403, cfMitigated: "challenge" }, D), false, "notmedio.com no es subdominio de medio.com");
  strictEqual(W._esBloqueoWaf({ url: "https://medio.com/contact", status: 521, cfRay: "8a1", cuerpo: "just a moment" }, D), false, "521 es el origen caído, no un bloqueo");
  strictEqual(W._esBloqueoWaf({ url: "https://medio.com/", status: 403, cuerpo: "just a moment" }, D), false, "sin cabecera de Cloudflare queda como estaba");
});

test("C10: tryFetch marca el WAF sólo a través de la regla, nunca por cf-ray suelto", () => {
  const i = worker.indexOf("async function scrapeEmailsForDomain(");
  const cuerpo = worker.slice(i, worker.indexOf("\n}\n", i));
  const marcas = [...cuerpo.matchAll(/_wafBloquea = true/g)];
  strictEqual(marcas.length, 1, "un solo lugar prende la bandera");
  ok(/_esBloqueoWaf\(/.test(cuerpo.slice(Math.max(0, marcas[0].index - 400), marcas[0].index)),
     "la bandera se prende sólo si _esBloqueoWaf lo confirma");
  ok(!/r\.headers\.get\("cf-ray"\)\)\)\s*\{\s*_wafBloquea = true/.test(cuerpo), "la condición vieja (cualquier 403 con cf-ray) no vuelve");
});
