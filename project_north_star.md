---
name: project-north-star
description: "Objetivo CENTRAL de la toolbar ADEQ — sin fallas + agente que trae webs correctas, garantiza al menos 1 email de la web (buscándolo en TODO internet) y envía el correo."
metadata: 
  node_type: memory
  type: project
  originSessionId: 63cc8b14-2ceb-446a-9c44-9c75690d5923
---

## 🎯 Objetivo central (user 2026-07-15)
La toolbar NO debe tener fallas. El agente tiene que ser **lo más inteligente posible** para:
1. **Traer webs correctas** — solo publishers de contenido prospectables (no tiendas/bancos/servicios/etc.).
2. **Identificar SÍ O SÍ al menos un email de esa web** — buscándolo en **cualquier parte de todo internet**,
   no solo en el home. Fuentes: páginas internas multilingües (contacto/publicidad/about), redes sociales
   (FB/IG/Twitter/LinkedIn/YouTube/TikTok), website-informer/WHOIS/RDAP, Apollo, **búsqueda en Google (Serper)
   del email**, sellers.json/ads.txt, y como ÚLTIMO recurso patrones de rol (info@/contacto@/publicidad@)
   validados por MX. La meta es que NINGÚN publisher válido quede sin al menos un email.
3. **Enviar el correo** (en el idioma correcto, al mejor contacto — rol comercial/publicidad).

Toda mejora/corrección se prioriza por cuánto acerca a estos 3 pasos. Ver [[adeq-toolbar-estado-y-pendientes]].
Regla de oro sigue vigente: NUNCA rechazar un publisher real (falso positivo = lead perdido = inaceptable).
