#!/usr/bin/env python3
"""
Sube un zip al Chrome Web Store y (opcionalmente) lo publica.

    python3 scripts/cws-publish.py <ruta-al-zip>            # sube, NO publica
    python3 scripts/cws-publish.py <ruta-al-zip> --publicar # sube y publica

Credenciales: ~/.adeq-cws.json (ver scripts/README-chrome-web-store.md).

⚠️ SUBIR reemplaza el borrador de la ficha; PUBLICAR lo manda a revisión de Google y,
al aprobarse, les llega a los tres MB. Por eso publicar es un flag explícito y no el default.
"""
import sys, os, json, urllib.parse, subprocess

CRED = os.path.expanduser("~/.adeq-cws.json")

def _cargar():
    if not os.path.exists(CRED):
        sys.exit(f"❌ No existe {CRED}. Seguí el paso 6 del instructivo.")
    with open(CRED) as f: c = json.load(f)
    for k in ("client_id", "client_secret", "refresh_token", "extension_id"):
        if not c.get(k): sys.exit(f"❌ Falta '{k}' en {CRED}")
    return c

# ⚠️ Se usa `curl` y no `urllib` (Maxi 2026-08-28). En esta Mac urllib no encuentra los
# certificados raíz y falla con CERTIFICATE_VERIFY_FAILED contra varios hosts — ya había
# pasado con la API de Railway. curl usa el almacén de certificados del sistema y anda.
def _curl(args):
    r = subprocess.run(["curl", "-s", "-m", "300", *args], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"❌ curl falló ({r.returncode}): {r.stderr[:200]}")
    try:
        return json.loads(r.stdout or "{}")
    except json.JSONDecodeError:
        sys.exit(f"❌ Respuesta no-JSON: {r.stdout[:300]}")

def _access_token(c):
    d = urllib.parse.urlencode({
        "client_id": c["client_id"], "client_secret": c["client_secret"],
        "refresh_token": c["refresh_token"], "grant_type": "refresh_token",
    })
    j = _curl(["-X", "POST", "https://oauth2.googleapis.com/token", "-d", d])
    if "access_token" not in j:
        sys.exit(f"❌ No se pudo renovar el token: {json.dumps(j)[:300]}")
    return j["access_token"]

def _pedir(url, token, metodo="POST", archivo=None, ctype=None):
    args = ["-X", metodo, url,
            "-H", f"Authorization: Bearer {token}",
            "-H", "x-goog-api-version: 2"]
    if ctype: args += ["-H", f"Content-Type: {ctype}"]
    # El zip se manda por archivo (--data-binary @ruta): pasarlo como argumento
    # reventaría el límite de tamaño de la línea de comandos.
    if archivo: args += ["--data-binary", f"@{archivo}"]
    j = _curl(args)
    if "error" in j:
        e = j["error"]
        sys.exit(f"❌ HTTP {e.get('code')}: {str(e.get('message'))[:400]}")
    return j

def main():
    if len(sys.argv) < 2: sys.exit(__doc__)
    zip_path = sys.argv[1]
    publicar = "--publicar" in sys.argv
    if not os.path.exists(zip_path): sys.exit(f"❌ No existe el zip: {zip_path}")

    # Chequeo propio: el campo `key` en el zip hace que la tienda lo rechace.
    import zipfile
    with zipfile.ZipFile(zip_path) as z:
        m = json.loads(z.read("manifest.json"))
    if "key" in m:
        sys.exit("❌ El zip trae el campo `key` en el manifest — la tienda lo rechaza. "
                 "Armalo con el script de empaquetado, que lo saca.")
    print(f"📦 {os.path.basename(zip_path)} · versión {m.get('version')} · sin key ✓")

    c = _cargar()
    tok = _access_token(c)
    eid = c["extension_id"]

    print(f"⬆️  Subiendo {os.path.getsize(zip_path):,} bytes…")
    r = _pedir(f"https://www.googleapis.com/upload/chromewebstore/v1.1/items/{eid}",
               tok, "PUT", zip_path, "application/zip")
    estado = r.get("uploadState")
    if estado != "SUCCESS":
        sys.exit(f"❌ La subida terminó en {estado}: {json.dumps(r.get('itemError', r))[:400]}")
    print("✅ Subido — el borrador de la ficha quedó actualizado.")

    if not publicar:
        print("\nNO se publicó (falta --publicar). El borrador está listo para revisar en el panel.")
        return
    print("🚀 Publicando…")
    p = _pedir(f"https://www.googleapis.com/chromewebstore/v1.1/items/{eid}/publish", tok)
    print("✅", " · ".join(p.get("status", [])) or json.dumps(p)[:200])
    if p.get("statusDetail"): print("  ", " · ".join(p["statusDetail"]))

if __name__ == "__main__":
    main()
