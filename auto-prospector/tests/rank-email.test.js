// rankEmail: el criterio del media buyer, fijado con los casos que salieron de medir envíos
// reales (2026-09-04). Cada bloque dice QUÉ dato lo justifica, así el que quiera cambiar un
// puntaje sabe contra qué evidencia va.
//
// Run: npm test
import { test } from "node:test";
import { ok, strictEqual } from "node:assert";
import { rankEmail } from "../lib/email.js";

const D = "diario-ejemplo.com";
const r = (email, site = D) => rankEmail(email, site, "");

// ── Lo que NO cambió y no tiene que cambiar (la referencia de siempre) ─────────────────
test("comercial y publicidad siguen siendo el ideal", () => {
  strictEqual(r(`publicidad@${D}`), 135);
  strictEqual(r(`comercial@${D}`), 135);
});
test("redacción vale lo mismo en todos los idiomas", () => {
  for (const l of ["redaccion", "redazione", "redaktion", "redaction", "redakcja"]) strictEqual(r(`${l}@${D}`), 115, l);
});
test("la casilla de prensa vale lo mismo en todos los idiomas", () => {
  for (const l of ["press", "prensa", "presse", "imprensa", "stampa"]) strictEqual(r(`${l}@${D}`), 115, l);
});
test("el buzón del sitio alojado en su grupo editor no es 'otra empresa'", () => {
  const enGrupo = rankEmail("contacto.topgear@henneomagazines.com", "topgear.es", "");
  ok(enGrupo > 0, `contacto.topgear@ en el grupo vale ${enGrupo}, tiene que ser enviable`);
  ok(rankEmail("contacto@henneomagazines.com", "topgear.es", "") < 0, "sin la marca del sitio sigue siendo otra empresa");
});
test("info@ es el de última", () => strictEqual(r(`info@${D}`), 55));
test("los robots siguen fuera", () => {
  for (const l of ["dmarcreport", "noreply", "postmaster", "abuse", "owner", "dpo", "legal"]) strictEqual(r(`${l}@${D}`), -1, l);
});
test("una persona del dominio propio", () => {
  strictEqual(r(`juan.perez@${D}`), 110);
  strictEqual(r(`jperez@${D}`), 95);
});

// ── Webmail del dueño: 7,7% de respuesta real y 1,8% de rebote, contra 3,5% / 7,3% ────────
test("una persona con gmail queda por encima de info@ y por debajo de la misma persona en el dominio", () => {
  const gmail = r("juanperez@gmail.com"), info = r(`info@${D}`), propio = r(`jperez@${D}`);
  ok(gmail > info, `persona@gmail (${gmail}) tiene que superar a info@ (${info})`);
  ok(gmail < propio, `persona@gmail (${gmail}) tiene que perder contra la persona del dominio (${propio})`);
});
test("el nombre del medio dentro de un gmail es el buzón del sitio", () => {
  const conMarca = r("diario-ejemplo.contacto@gmail.com"), info = r(`info@${D}`);
  ok(conMarca >= info, `el gmail con la marca (${conMarca}) no puede valer menos que info@ (${info})`);
});
test("un gmail sin forma sigue castigado", () => {
  ok(r("x7k9m2p4q@gmail.com") < 0);
});

// ── Los cinco huecos que salieron de sondear la función con casos reales ────────────────
test("protección de datos en cualquier idioma no es un contacto", () => {
  for (const l of ["datenschutz", "rgpd", "lopd", "privacidade"]) strictEqual(r(`${l}@${D}`), -1, l);
});
test("la defensoría del lector es una mesa de reclamos", () => {
  ok(r(`ouvidoria@${D}`) < 0, "ouvidoria");
  ok(r(`complaints@${D}`) < 0, "complaints");
});
test("geral@ es el info@ portugués", () => strictEqual(r(`geral@${D}`), r(`info@${D}`)));
test("el departamento comercial con la palabra adelante es comercial", () => {
  strictEqual(r(`departamentocomercial@${D}`), 135);
  // y en el dominio del grupo editor declarado como casa editora, no es 'otra empresa'
  const grupo = rankEmail("departamentocomercial@grupocronica.com.ar", "baenegocios.com", "", new Set(["grupocronica.com.ar"]));
  ok(grupo >= 100, `en la casa editora vale ${grupo}, tiene que ser comercial`);
});
test("un rol con prefijo de región sigue siendo ese rol", () => {
  // el media buyer usó de.adsales@insideevs.de; lat.press@motorsport.com es el buzón de prensa LATAM
  strictEqual(r(`de.adsales@${D}`), 135, "de.adsales es comercial");
  strictEqual(r(`sales@${D}`), r(`ventas@${D}`), "sales@ vale lo mismo que ventas@ (la palabra 'sale' no es spam acá)");
  strictEqual(r(`info.lat@${D}`), r(`info@${D}`), "info.lat es un genérico, no una persona");
  strictEqual(r(`lat.press@${D}`), r(`press@${D}`), "lat.press vale lo que press");
  strictEqual(r(`es.redaccion@${D}`), r(`redaccion@${D}`), "es.redaccion vale lo que redaccion");
});
test("inicial.apellido es la misma persona que inicialapellido", () => {
  strictEqual(r(`j.perez@${D}`), r(`juan.perez@${D}`));
  // pero un artefacto con genérico atrás no se convierte en persona
  ok(r(`e.mail@${D}`) < r(`j.perez@${D}`));
});
test("admin@ es de última, no basura", () => {
  const admin = r(`admin@${D}`), info = r(`info@${D}`);
  ok(admin > 0 && admin < info, `admin@ (${admin}) tiene que existir y perder contra info@ (${info})`);
  strictEqual(r(`admin2@${D}`) < 0, true, "admin2@ sigue siendo placeholder");
});
