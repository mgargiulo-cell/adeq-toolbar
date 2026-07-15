# Taxonomía de tipos de web a EXCLUIR (brainstorm 2026-07-15)

Referencia para robustecer el filtro de no-publishers. ADEQ compra inventario display DE publishers →
prospect válido = sitio de CONTENIDO que vende espacio publicitario. Todo lo de abajo es lo contrario.

## Regla maestra (protege la regla de oro)
VETO PUBLISHER: si el sitio tiene `ads.txt`/`sellers.json` real, o GPT/Prebid/AdSense (`googletag`,
`prebid.js`, `ca-pub-`), o Taboola/Outbrain, MÁS un home de feed de artículos → **es publisher, NO excluir**
aunque matchee un keyword de exclusión. Usar los schema/keywords de abajo para DOWN-RANK; el ad-tech vetea.
Schema barato de alta precisión: NewsMediaOrganization/NewsArticle/BlogPosting → include fuerte;
GovernmentOrganization/BankOrCreditUnion/LocalBusiness*/SoftwareApplication/Store/MedicalOrganization → exclude.

## (A) EXCLUIR

### Gobierno — sub-tipos (refinan el bloqueo gov con schema/TLD)
Cortes/judiciales, agencias tributarias, patentes/marcas, estadística/censo, servicio meteorológico GOB,
embajadas/cancillerías, bancos centrales, reguladores, municipios/ciudades, autoridades de transporte,
parlamentos, comisiones electorales, correos nacionales, policía/defensa, migraciones/aduanas.
FP guard: broadcaster público editorial (BBC/RTVE/DW/NHK) SÍ es publisher.

### Instituciones / membresía
Religiosas/iglesias, partidos políticos, sindicatos, cámaras de comercio, colegios profesionales,
organismos de estándares (w3/iso/ietf), museos, bibliotecas/archivos, fundaciones/think-tanks, clubes,
journals científicos con paywall.

### Servicios profesionales / local business (schema LocalBusiness*)
Estudios jurídicos, contadores/auditoras, consultoras, arquitectura/ingeniería, agencias marketing/PR/SEO,
consultorios médicos/dentales/veterinarios, laboratorios, gimnasios, restaurantes, concesionarias de autos,
peluquerías/spa, funerarias, oficios (plomería/electricidad/HVAC/mudanzas), guarderías/centros de tutoría.

### Corporate / industrial / B2B brochure
Sitios corporativos "about us", fabricantes/industriales, distribuidores/mayoristas, logística/envíos/courier,
servicios públicos (agua/luz/gas), desarrolladoras/construcción, energía/petróleo/minería, agribusiness,
páginas de investor-relations, reclutamiento de franquicias.

### Herramientas / utilidades online
Calculadoras/conversores, generadores, acortadores de URL, link-in-bio, QR, conversión/compresión PDF,
speed-test/whois/DNS/IP lookup, screenshot, survey/form builders, herramientas de texto.

### Plataformas transaccionales / servicios digitales
Procesadores de pago, hosting/registradores de dominio, cloud storage/file-sharing, webmail, ticketing/eventos,
citas/dating, app stores, portales de descarga, antivirus/VPN, streaming (Netflix/Spotify), hosting de podcasts,
crowdfunding, programas de fidelidad, reservas/booking, marketplaces freelance, print-on-demand, ride-hailing/delivery.

### Developer / técnico
API docs/portales dev, registries de paquetes (npm/pypi), repos (github), sitios de documentación,
status/uptime, sandboxes de código.

### Soporte / knowledge / gated
Help desks/KB, blogs de producto de una sola empresa, intranets/login-walls, wikis corporativos.

### Personal / micro-sites
Portfolios/CV, portfolios de fotógrafos/artistas, sitios de bodas/eventos, landing de una página.

### Baja calidad / abuso / piratería
Dominios parkeados/en venta, MFA (thin/AI-spam), doorway/SEO-spam/PBN, torrent/warez/cracks,
streaming ilegal, lectores piratas de manga/novelas, phishing/malware/scam, granjas de contenido AI.

### Marketplaces de assets
Stock photo/video/audio, fonts/iconos, templates/themes/plugins, NFT, librerías de assets por suscripción.

### Navegación / transporte / lending extra
Maps/navegación, apps de transporte, payday/micro-préstamos, cobranza/BNPL, directorios de empresas.

## (B) BORDERLINE (decisión humana — flag, no auto-reject)
Horóscopo/astrología, lyrics, foros UGC, wikis (Fandom vs Wikipedia), Q&A (Quora/StackExchange),
blogs personales/hobby, agregadores de clima, herramientas con muchos ads, portales de descarga,
portales de juegos flash, lectores piratas, directorios/reviews (Yelp), content farms, think-tanks con
comentario, fan sites, newsrooms nonprofit (ProPublica/NPR = include), sitios de scores/stats deportivos,
blogs de cupones. → Regla: incluir si feed de artículos + ad-tech real; excluir si transaccional/thin/ilegal.
