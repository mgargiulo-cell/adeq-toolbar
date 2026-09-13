// Ninguna consulta a la base pide una columna que la tabla no tiene. (2026-09-13)
//
// Dos bloqueantes del 13/09 fueron exactamente esto: `order=id` sobre toolbar_bounced_emails (no tiene id)
// dejó muda la alerta de reputación, y `select=...,created_at` sobre la misma tabla (la fecha es
// bounced_at) hizo que la extensión contestara "no rebotó" a TODO durante diez días. La base responde 400,
// el lector lo tapa con un `catch {}` o un `return null`, y el error aparece semanas después como un job
// mudo o un parte que dice "no se pudo leer".
//
// La verdad está en sql/_columnas_actuales.json: el resultado real de information_schema.columns que Maxi
// corrió el 13/09 (una fila por tabla). Cuando se agregue una columna desde el panel, hay que volver a
// correr esa consulta y pegar el resultado ahí; el test lo dice con el nombre de la tabla y la columna.
//
// Run: npm test
import { test } from "node:test";
import { ok, deepStrictEqual } from "node:assert";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const leer = (rel) => fs.readFileSync(path.join(RAIZ, rel), "utf8");
const COLUMNAS = JSON.parse(leer("sql/_columnas_actuales.json"));
const dir = (rel) => fs.existsSync(path.join(RAIZ, rel)) ? fs.readdirSync(path.join(RAIZ, rel)).filter(f => f.endsWith(".js")).map(f => `${rel}/${f}`) : [];
const ARCHIVOS = ["auto-prospector/index.js", "auto-prospector/discovery.js", ...dir("auto-prospector/lib"), ...dir("modules"), "popup/popup.js", ...dir("background")];

// Las columnas que nombra un pedido: select, order, on_conflict y los filtros (`col=op.valor`).
// Lo que viene de una expresión (`${...}`) no se puede juzgar acá y se saltea.
export function columnasDePedido(query) {
  const out = [];
  for (const par of String(query).replace(/^\?/, "").split("&")) {
    const i = par.indexOf("=");
    if (i < 0) continue;
    const k = par.slice(0, i);
    let v = par.slice(i + 1);
    try { v = decodeURIComponent(v); } catch {}
    v = v.replace(/\$\{[^}]*\}/g, "X");
    let usadas = [];
    if (k === "select") usadas = v.split(",").map(c => c.split(/->|::|\(/)[0].split(":").pop().trim());
    else if (k === "order") usadas = v.split(",").map(c => c.split(".")[0].split("->")[0].trim());
    else if (k === "on_conflict") usadas = v.split(",");
    else if (/^(limit|offset|and|or|columns|not)$/.test(k) || k.includes("$") || !k) continue;
    else usadas = [k.split("->")[0]];
    for (const c of usadas) if (c && c !== "*" && c !== "X" && /^[a-z_][a-z0-9_]*$/.test(c)) out.push(c);
  }
  return out;
}

test("el detector lee select, order, filtros y on_conflict, y saltea lo que viene de una expresión", () => {
  deepStrictEqual(columnasDePedido("?select=email,reason,bounced_at&evidencia=in.(x)&order=email&limit=1"), ["email", "reason", "bounced_at", "evidencia", "email"]);
  deepStrictEqual(columnasDePedido("?status=eq.pending&details->>ui_origin=is.null&created_at=gte.${d}&select=id,details->>x&order=id.desc"), ["status", "details", "created_at", "id", "details", "id"]);
  deepStrictEqual(columnasDePedido("?or=(a.is.null,b.eq.1)&${extra}=x&select=*"), []);
});

test("ninguna URL de PostgREST del worker, la librería, los módulos o el popup pide una columna que su tabla no tiene", () => {
  const fuera = [];
  const desconocidas = new Set();
  for (const rel of ARCHIVOS) {
    const src = leer(rel);
    const re = /\/rest\/v1\/([a-z0-9_]+)(\?[^`"'\s]*)?/g;
    let m;
    while ((m = re.exec(src))) {
      const tabla = m[1];
      if (tabla === "rpc") continue;
      const linea = src.slice(0, m.index).split("\n").length;
      if (!COLUMNAS[tabla]) { desconocidas.add(`${tabla} (${rel}:${linea})`); continue; }
      if (!m[2]) continue;
      for (const c of columnasDePedido(m[2])) {
        if (!COLUMNAS[tabla].includes(c)) fuera.push(`${rel}:${linea} ${tabla}.${c}`);
      }
    }
  }
  deepStrictEqual([...desconocidas], [], "una tabla que no está en sql/_columnas_actuales.json: volver a correr la consulta de information_schema y pegar el resultado");
  deepStrictEqual(fuera, [], "la base contesta 400 y el lector lo tapa: la columna no existe (o falta actualizar sql/_columnas_actuales.json)");
  ok(Object.keys(COLUMNAS).length >= 58, "la lista de columnas quedó corta");
});
