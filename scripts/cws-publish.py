#!/usr/bin/env python3
"""
Sube un zip al Chrome Web Store y (opcionalmente) lo publica.

    python3 scripts/cws-publish.py <ruta-al-zip>            # sube, NO publica
    python3 scripts/cws-publish.py <ruta-al-zip> --publicar # sube y publica

Credenciales: ~/.adeq-cws.json (ver scripts/README-chrome-web-store.md).

⚠️ SUBIR reemplaza el borrador de la ficha; PUBLICAR lo manda a revisión de Google y,
al aprobarse, les llega a los tres MB. Por eso publicar es un flag explícito y no el default.
"""
import sys, os, json, urllib.request, urllib.parse

CRED = os.path.expanduser("~/.adeq-cws.json")

def _cargar():
    if not os.path.exists(CRED):
        sys.exit(f"❌ No existe {CRED}. Seguí el paso 6 del instructivo.")
    with open(CRED) as f: c = json.load(f)
    for k in ("client_id", "client_secret", "refresh_token", "extension_id"):
        if not c.get(k): sys.exit(f"❌ Falta '{k}' en {CRED}")
    return c

def _access_token(c):
    d = urllib.parse.urlencode({
        "client_id": c["client_id"], "client_secret": c["client_secret"],
        "refresh_token": c["refresh_token"], "grant_type": "refresh_token",
    }).encode()
    r = urllib.request.Request("https://oauth2.googleapis.com/token", data=d)
    return json.load(urllib.request.urlopen(r, timeout=30))["access_token"]

def _pedir(url, token, metodo="POST", cuerpo=None, ctype=None):
    r = urllib.request.Request(url, data=cuerpo, method=metodo)
    r.add_header("Authorization", f"Bearer {token}")
    r.add_header("x-goog-api-version", "2")
    if ctype: r.add_header("Content-Type", ctype)
    try:
        return json.load(urllib.request.urlopen(r, timeout=300))
    except urllib.error.HTTPError as e:
        sys.exit(f"❌ HTTP {e.code}: {e.read().decode()[:400]}")

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

    with open(zip_path, "rb") as f: datos = f.read()
    print(f"⬆️  Subiendo {len(datos):,} bytes…")
    r = _pedir(f"https://www.googleapis.com/upload/chromewebstore/v1.1/items/{eid}",
               tok, "PUT", datos, "application/zip")
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
