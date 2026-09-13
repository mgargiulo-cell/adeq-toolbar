#!/bin/bash
# Arma el zip de la extensión para el Chrome Web Store. Reproducible: hasta el 04/09 se
# armaba a mano y cada vez había que acordarse de tres cosas. Ahora es un comando.
#
#   scripts/empaquetar.sh            → ~/Desktop/adeq-toolbar-v<version>.zip
#
# Lo que hace, y por qué cada paso existe:
#   1. Copia SOLO lo que corre en el navegador. NO va `auto-prospector/` entero (es el worker
#      de Railway)… salvo `auto-prospector/lib/email.js` y `geo.js`, que desde la Fase 5 los
#      importa el popup: son el ranking de emails, y la extensión y el worker usan el MISMO
#      archivo a propósito, para que no vuelvan a discrepar.
#   2. Saca el campo `key` del manifest DEL PAQUETE (nunca del repo). La tienda lo rechaza con
#      "No se admite el campo key"; en el repo hace falta para que la carga local en modo
#      desarrollador conserve el ID.
#   3. Borra los .DS_Store, que macOS mete solos.
#   4. Verifica el paquete por dentro antes de dar por bueno: que no quede `key`, que no haya
#      entrado el worker, y que TODOS los imports relativos apunten a archivos que están.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="$(python3 -c "import json;print(json.load(open('$REPO/manifest.json'))['version'])")"
# SALIDA=/otra/ruta.zip para probar el empaquetado sin pisar el zip que ya se subió a la tienda.
ZIP="${SALIDA:-$HOME/Desktop/adeq-toolbar-v${VERSION}.zip}"
ST="$(mktemp -d)"

cp -R "$REPO"/{manifest.json,config.js,popup,modules,background,icons,docs} "$ST/"
mkdir -p "$ST/auto-prospector/lib"
cp "$REPO"/auto-prospector/lib/{email.js,geo.js} "$ST/auto-prospector/lib/"

python3 - "$ST/manifest.json" <<'PY'
import json, sys
p = sys.argv[1]; m = json.load(open(p)); m.pop("key", None)
json.dump(m, open(p, "w"), indent=2, ensure_ascii=False)
PY
find "$ST" -name ".DS_Store" -delete

# ── Verificación del paquete, no del repo ───────────────────────────────────────────────
python3 - "$ST" <<'PY'
import json, os, re, sys
raiz = sys.argv[1]; errores = []
m = json.load(open(os.path.join(raiz, "manifest.json")))
if "key" in m: errores.append("el manifest del paquete todavía tiene `key`")
if os.path.exists(os.path.join(raiz, "auto-prospector", "index.js")): errores.append("entró el worker (auto-prospector/index.js)")
# (2026-09-13) La expresión vieja exigía el `from` en la MISMA línea que el `import`: los imports
# escritos en varias líneas (supabase.js, colaEstado.js, userLimits.js y coordination.js en popup.js)
# no se miraban, y tampoco los import() dinámicos. Un import así hacia un archivo que el zip no copia
# daba el tilde verde y la extensión publicada no abría el popup. `[^;]` cruza saltos de línea; los
# dos totales tienen que coincidir con los que cuenta acorn (tests/extension_envio-13-09c.test.js).
RE_EST = re.compile(r'^[ \t]*(?:import|export)\b[^;]*?\bfrom\s*["\'](\.[^"\']+)["\']', re.M)
RE_DIN = re.compile(r'\bimport\s*\(\s*["\'](\.[^"\']+)["\']\s*\)')
n_est = n_din = 0
for dp, _, fs in os.walk(raiz):
    for f in fs:
        if not f.endswith(".js"): continue
        p = os.path.join(dp, f); src = open(p, encoding="utf8", errors="replace").read()
        for dinamico, rx in ((False, RE_EST), (True, RE_DIN)):
            for mm in rx.finditer(src):
                if dinamico: n_din += 1
                else: n_est += 1
                if not os.path.exists(os.path.normpath(os.path.join(dp, mm.group(1)))):
                    errores.append(f"{os.path.relpath(p, raiz)} importa {mm.group(1)}{' (import dinámico)' if dinamico else ''} y no está en el paquete")
if errores:
    print("❌ PAQUETE INVÁLIDO:"); [print("   - " + e) for e in errores]; sys.exit(1)
print(f"   imports relativos verificados: estáticos {n_est} · dinámicos {n_din} · sin `key` · sin worker")
PY

rm -f "$ZIP"
(cd "$ST" && zip -qr "$ZIP" .)
rm -rf "$ST"
echo "✅ $ZIP · $(du -h "$ZIP" | cut -f1) · $(unzip -l "$ZIP" | tail -1 | awk '{print $2}') archivos · versión $VERSION"
