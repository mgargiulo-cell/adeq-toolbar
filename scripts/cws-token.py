#!/usr/bin/env python3
"""
Canjea el CÓDIGO de autorización de Google por un refresh_token para el Chrome Web Store.
Se corre UNA sola vez, cuando ya tenés el código del paso 4.

    python3 scripts/cws-token.py <CLIENT_ID> <CLIENT_SECRET> <CODIGO>

Imprime las tres líneas que hay que pegar en el archivo de credenciales.
El refresh_token NO vence salvo que se revoque o se cambie la contraseña de Google.
"""
import sys, json, urllib.parse, urllib.request

if len(sys.argv) != 4:
    print(__doc__); sys.exit(1)
cid, secret, code = sys.argv[1], sys.argv[2], sys.argv[3]

datos = urllib.parse.urlencode({
    "client_id": cid,
    "client_secret": secret,
    "code": code,
    "grant_type": "authorization_code",
    # Este redirect_uri es el que usa el flujo "aplicación de escritorio": no hace falta
    # levantar ningún servidor, Google muestra el código en pantalla para copiarlo.
    "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
}).encode()

try:
    r = urllib.request.Request("https://oauth2.googleapis.com/token", data=datos)
    j = json.load(urllib.request.urlopen(r, timeout=30))
except urllib.error.HTTPError as e:
    print("❌ Google rechazó el canje:", e.read().decode()[:300])
    print("\nCausas típicas: el código ya se usó (son de UN solo uso), venció (duran minutos),")
    print("o el client_id/secret no son del mismo proyecto que generó el código.")
    sys.exit(1)

if "refresh_token" not in j:
    print("❌ Google no devolvió refresh_token. Repetí el paso 4 agregando prompt=consent a la URL.")
    print(json.dumps(j, indent=2)[:400]); sys.exit(1)

print("✅ Listo. Pegá esto en ~/.adeq-cws.json (el paso 6 te dice cómo):\n")
print(json.dumps({
    "client_id": cid, "client_secret": secret,
    "refresh_token": j["refresh_token"],
    "extension_id": "jgbacjjjohjaiojjecgnejcalepkjclm",
}, indent=2))
