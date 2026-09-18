---
name: reference-no-prospectable-types
description: Tipos de web que NO son prospectables (el user los enseñó con ejemplos 2026-07-16) — para afinar el detector no-publisher del worker + popup.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 63cc8b14-2ceb-446a-9c44-9c75690d5923
  modified: 2026-09-15T11:24:16.532Z
---

## ⚠️ Streaming YA NO es no-prospectable (decisión del dueño, 13/09 noche; commits fff7694 → 7977c43)
Sitios y guías de música, películas, series y TV pasan por las mismas puertas que cualquier medio.
Lo que sigue vetado es **lo pirata y las retransmisiones en vivo**: categoría `streaming_pirata`
(`_STREAMING_PIRATA_RE`, `_esStreamingPirata`, una sola regla compartida worker+popup), y una fila
vieja guardada como "streaming" que vende el servicio (hosting, VPN) sigue vetada al enviar.
"streaming" vuelve a ser la etiqueta del MEDIO, no de quien vende el servicio.

# Tipos de web que NO van (ejemplos que enseñó el user, 2026-07-16)

El user fue tirando URLs de ejemplo para entrenar el detector no-publisher. NO son publishers →
no deben aparecer en Prospects. Sincronizar SIEMPRE worker (`fetchPageContent`/`classifyPublisher`)
y popup (`runPageContext`). Regla de oro intacta: nunca rechazar un publisher/medio real.

TODOS DEPLOYADOS 2026-07-16 (worker `fetchPageContent`/`classifyPublisher`). Buckets nuevos gateados por
`!hasDisplayAds` + requieren 2 hits (bajo FP). PENDIENTE: sincronizar los mismos al popup `runPageContext`.

| Tipo | Ejemplo | Detección (✅ deployada) |
|------|---------|-----------------|
| **Buscador de vuelos / travel / hoteles / OTA** | turismocity.cl, edreams.es, nta.co.jp | `travelKw`+`travelSchema` (ya existían). Revisar si pasan por tener ads display |
| **Cripto compra/venta/trading/exchange** | blockchaincenter.net | ✅ `cryptoKw` (buy/sell crypto, exchange, wallet, trading) |
| **Dev library / servicio técnico / docs de herramienta** | vueuse.org | ✅ `devToolKw` (npm install, API reference, GitHub stars, docs) |
| **APP (móvil / citas / servicio)** | babel.com | ✅ `appKw` (download app + app store/google play) |
| **Adulto / NSFW** | sexysluts.tv | ✅ ya detectado (regex worker → category "adult", gate duro) |
| **E-commerce / tienda / venta de productos** | dieteticacentral.com, tokyointerior-onlineshop.com | ✅ detector de tienda (storePlatform/cart/checkout) + `shopKw` de refuerzo |
| **Sitio MUERTO / SSL / certificado / privacidad** | zd.blog.jp (privacy error), gamepress.gg (can't be reached), eiga.com (ERR_SSL_VERSION_OR_CIPHER_MISMATCH) | ✅ FIX: `fetchPageContent` marca `dead:true` en ECONNREFUSED/CERT/SSL/TLS/etc (no solo DNS); `classifyPublisher` con `dead` → ok:false (antes `!pageContent` daba ok:true → se colaban) |
| **DNS no existe (NXDOMAIN)** | mafraslovakia.hnonline.sk | ✅ ya cubierto (ENOTFOUND/getaddrinfo → dead) |

## Casos de EMAIL/contacto (otro tema — feature contact-discovery)
- **massa.com.br** → `/fale-conosco-grupo-massa/` es SOLO un formulario, sin email → resolver con Google fallback (Serper) + capturar tel/WhatsApp. massa además salía mal-clasificado `cat_blocked:gambling` (es Grupo Massa, medios SBT — FALSO POSITIVO a revisar).
- **trikalaola.gr** → tiene `info@trikalaola.gr` + tel `2431770207/6977200738` en el FOOTER de cada nota → el scraper debería agarrarlo; si no, Google fallback (está indexado).
- Insight del user: **Google "url + contacto/contato/fale conosco" trae emails, teléfonos, 0800 y WhatsApp (wa.me/…)** → incluir teléfono + WhatsApp en la toolbar (campo nuevo en la card; contact_phone ya existe en DB + columna Monday tel_fono_1 ya cableada). Ver [[adeq-toolbar-estado-y-pendientes]].
