// Carga index.js ENTERO con main() cortocircuitado y con funciones internas exportadas, para
// que los tests puedan llamar al CÓDIGO EXACTO del worker (no una copia de las regex).
// El 04/09 esto encontró en un minuto un bug que llevaba dos días en producción: fetchPageContent
// devolvía null para todos los sitios por una variable inexistente, y el catch lo escondía.
//
//   const { fetchPageContent } = await cargarWorker(["fetchPageContent"]);
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// `fetchFalso: true` reemplaza el `fetch` de node-fetch por `globalThis.__fetchFalso`, para que
// un test pueda contestar las consultas a la base y a Gmail con datos inventados y así correr
// funciones enteras del worker (el parte del día, el boletín) sin red ni credenciales.
export async function cargarWorker(funciones, { fetchFalso = false } = {}) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "adeq-worker-"));
  for (const f of ["node_modules", "lib", "discovery.js", "templates.js", "keywordsData.js", "package.json"]) {
    fs.symlinkSync(path.join(REPO, f), path.join(tmp, f));
  }
  let s = fs.readFileSync(path.join(REPO, "index.js"), "utf8");
  const n = (s.match(/main\(\)\.catch\(/g) || []).length;
  if (n !== 1) throw new Error(`esperaba 1 main().catch( en index.js, hay ${n}`);
  s = s.replace("main().catch(", "false && main().catch(");
  if (fetchFalso) {
    if (!/^import fetch from "node-fetch";/m.test(s)) throw new Error("no encontré el import de node-fetch para reemplazarlo");
    s = s.replace(/^import fetch from "node-fetch";/m, 'const fetch = (...a) => globalThis.__fetchFalso(...a);');
  }
  s += `\nexport { ${funciones.join(", ")} };\n`;
  fs.writeFileSync(path.join(tmp, "index.js"), s);
  return import(pathToFileURL(path.join(tmp, "index.js")).href);
}
