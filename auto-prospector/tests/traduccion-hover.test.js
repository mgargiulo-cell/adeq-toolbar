// La traducción se pide SÓLO cuando el MB la quiere ver. (2026-09-07, regla del user)
//
// Textual: *"sólo si el usuario deja el cursor en el texto del borrador más de 2 segundos
// quieto, ahí se abre"* y *"si nadie navega en ese espacio, no se traduce nada, de esta manera
// evitamos cargas innecesarias y pérdidas de recursos"*.
//
// Lo que este test cuida es justamente eso: que pasar el mouse por encima camino a otro botón
// NO dispare una traducción. Se corre la función real de popup.js con un DOM y un traductor de
// mentira, contando cuántas veces se pediría traducir.
//
// Run: npm test
import { test } from "node:test";
import { strictEqual, ok, match } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const raiz = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const src  = fs.readFileSync(path.join(raiz, "popup", "popup.js"), "utf8");
const ini  = src.indexOf("// El panel de traducción de Analysis");
const fin  = src.indexOf("function applyCrmTemplate");
ok(ini > 0 && fin > ini, "no encontré _conectarTraduccionHover en popup.js");

const esperar = (ms) => new Promise((r) => setTimeout(r, ms));

// Monta el panel con un traductor falso y devuelve con qué disparar cada evento.
function montar() {
  const estado = { traducciones: 0 };
  const traducirAlCastellano = async () => { estado.traducciones++; return { ok: true, texto: "Hola, revisé su sitio…" }; };
  const oyentes = {};
  const on = (k, f) => { (oyentes[k] ??= []).push(f); };
  const zona = { addEventListener: on };
  const area = { addEventListener: on, closest: () => zona };
  const cab   = { textContent: "" };
  const panel = { hidden: true, querySelector: () => cab };
  const body  = { textContent: "", className: "" };
  const fn = new Function("LANG_NOMBRE", "traducirAlCastellano", src.slice(ini, fin) + "; return _conectarTraduccionHover;")(
    { en: "Inglés" }, traducirAlCastellano);
  const api = fn(area, panel, body, () => "Hi, I went through your site…", () => "en");
  return { estado, panel, body, cab, api, disparar: (k) => (oyentes[k] || []).forEach((f) => f()) };
}

test("pasar por encima de camino a otro botón NO traduce", async () => {
  const m = montar();
  m.disparar("mouseenter");
  await esperar(600);
  m.disparar("mouseleave");
  await esperar(2200);                    // más de la demora, para que salte si quedó programado
  strictEqual(m.estado.traducciones, 0, "se tradujo un mail que nadie pidió ver");
  strictEqual(m.panel.hidden, true, "el panel se abrió sin que el cursor se quedara quieto");
});

test("mover el cursor reinicia la cuenta: no se abre a los 2 s partidos", async () => {
  const m = montar();
  m.disparar("mouseenter");
  await esperar(1300);
  m.disparar("mousemove");                // el cursor se movió: vuelve a empezar
  await esperar(1300);
  strictEqual(m.estado.traducciones, 0);
  strictEqual(m.panel.hidden, true);
});

test("quieto más de 2 segundos: recién ahí se abre y se traduce UNA vez", async () => {
  const m = montar();
  m.disparar("mouseenter");
  await esperar(2300);
  strictEqual(m.panel.hidden, false, "no se abrió después de 2,3 s quieto");
  strictEqual(m.estado.traducciones, 1);
  match(m.cab.textContent, /traducción al castellano del email en Inglés/i);
  ok(m.body.textContent.length > 0, "el cuerpo del panel no puede quedar vacío");
});

test("si la traducción falla, se muestra el ORIGINAL y no un cartel vacío", async () => {
  // Regla del user: "si la traducción no carga, mostrar la original sin importar el idioma,
  // porque si no parece que se envía en blanco".
  const oyentes = {};
  const on = (k, f) => { (oyentes[k] ??= []).push(f); };
  const zona = { addEventListener: on };
  const area = { addEventListener: on, closest: () => zona };
  const cab = { textContent: "" }, panel = { hidden: true, querySelector: () => cab }, body = { textContent: "", className: "" };
  const original = "Hi, I went through your site…";
  const fn = new Function("LANG_NOMBRE", "traducirAlCastellano", src.slice(ini, fin) + "; return _conectarTraduccionHover;")(
    { en: "Inglés" }, async () => ({ ok: false, motivo: "sin conexión" }));
  fn(area, panel, body, () => original, () => "en");
  (oyentes.mouseenter || []).forEach((f) => f());
  await esperar(2300);
  strictEqual(body.textContent, original, "con la traducción caída hay que ver el original");
  match(cab.textContent, /no se pudo traducir/i);
});
