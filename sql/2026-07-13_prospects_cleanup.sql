-- ════════════════════════════════════════════════════════════════════════
-- LIMPIEZA DE PROSPECTS (2026-07-13) — pedido del user tras la auditoría 48h.
-- Objetivo: encontrar en Prospects (toolbar_review_queue status='pending') los que
-- NO son publishers para borrarlos. Los fixes del worker ya evitan que ENTREN nuevos
-- y que se les ENVÍE (blocklist en envío), pero los que YA están pending no se
-- re-evalúan solos → hay que barrerlos a mano.
-- ⚠️ REGLA DE ORO: NO borrar a ciegas. Estas regex son AMPLIAS para no perderse nada,
--    pero pueden pegar un publisher real (ej. "bolsamania" por 'bolsa', un diario que
--    hable de bancos). SIEMPRE revisar la lista antes de borrar.
-- ════════════════════════════════════════════════════════════════════════

-- ── 0) Panorama: cuántos prospects pending hay y de qué categoría ──
SELECT category, COUNT(*) AS n
FROM toolbar_review_queue
WHERE status = 'pending'
GROUP BY category
ORDER BY n DESC;


-- ── 1) SOSPECHOSOS a revisar (marca el motivo) — ordenado por tráfico (los que saldrían antes) ──
WITH flagged AS (
  SELECT id, domain, category, page_title, traffic, geo,
    CASE
      WHEN split_part(regexp_replace(lower(domain), '^www\.', ''), '.', 1) = ANY (ARRAY[
        'applovin','ironsource','criteo','taboola','outbrain','mgid','teads','vidoomy','revcontent',
        'adidas','nike','puma','reebok','realmadrid','fcbarcelona','cocacola','pepsico','mcdonalds','ikea','lego'])
        THEN 'adtech/marca'
      WHEN domain ~* '\.(gov|gob|edu|mil)(\.|$)' OR domain ~* '(^|\.)(gov|gob|edu|ac|mil)\.'
        THEN 'gobierno/educacion (TLD)'
      WHEN domain ~* '(bank|banco|banca|caixa|sparkasse|seguros?|insurance|assicuraz|broker|forex|trading|bolsa|kredit|hypothek|prestamo)'
        THEN 'banco/seguro/broker'
      WHEN domain ~* '(idealista|inmobili|imovel|imoveis|realestate|realty|properties|immo|fincaraiz|newhome|develia|zillow)'
        THEN 'inmobiliaria'
      WHEN domain ~* '(compara|comparer|comparison|verivox|check24|heureka)'
        THEN 'comparador'
      WHEN domain ~* '(classified|clasificado|marketplace|opensooq|letgo|dubicars|stepstone|michaelpage)'
        THEN 'marketplace/empleo'
      WHEN domain ~* '(universi|escuela|\.school|college|\.academy|hospital|clinic|clinica)'
        THEN 'institucion/salud'
      WHEN page_title ~* '(online banking|banca en l|checkout|carrito|add to cart|shopping cart|\blog ?in\b|sign ?in|admissions?|book (a|your)|donate now)'
        THEN 'titulo no-publisher'
      ELSE NULL
    END AS sospecha
  FROM toolbar_review_queue
  WHERE status = 'pending'
)
SELECT sospecha, domain, category, page_title, traffic, geo, id
FROM flagged
WHERE sospecha IS NOT NULL
ORDER BY sospecha, traffic DESC NULLS LAST
LIMIT 500;

-- (conteo por motivo, para ver el volumen de cada tipo)
-- WITH flagged AS ( ... igual que arriba ... )
-- SELECT sospecha, COUNT(*) FROM flagged WHERE sospecha IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;


-- ── 2) BORRADO (correr SOLO después de revisar la lista de arriba) ──
-- Opción A (recomendada): borrar por dominios PUNTUALES que confirmaste que no sirven.
-- DELETE FROM toolbar_review_queue
-- WHERE status = 'pending'
--   AND lower(regexp_replace(domain,'^www\.','')) IN (
--     'applovin.com','adidas.co','realmadrid.com','axa.de','heureka.cz'  -- ← reemplazá por tu lista
--   );

-- Opción B: borrar TODO un motivo entero (ej. todos los 'adtech/marca'), si revisaste que están OK.
--   Pegá el WITH flagged AS (...) de la consulta 1 y después:
-- DELETE FROM toolbar_review_queue
-- WHERE id IN (SELECT id FROM flagged WHERE sospecha = 'adtech/marca');
