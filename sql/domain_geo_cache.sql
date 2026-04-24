-- Cache permanente de país por dominio.
-- Se llena con CADA información que obtenemos: SimilarWeb, Cloudflare Radar,
-- footer-address scrappeado, page signals (lang-region, og:locale), etc.
--
-- Beneficios:
--  * Analysis tab: chequea cache antes de llamar SimilarWeb (gratis, instantáneo)
--  * Autopilot: filtra dominios que ya sabemos que son del país equivocado
--    SIN llamar SimilarWeb otra vez
--
-- TTL: 90 días (el país de un sitio rara vez cambia, pero refrescamos)

CREATE TABLE IF NOT EXISTS toolbar_domain_geo_cache (
  domain      TEXT PRIMARY KEY,
  country     TEXT NOT NULL,                 -- alpha-2 (US, ES, AR, ...)
  source      TEXT NOT NULL DEFAULT 'unknown', -- "similarweb" | "radar" | "footer" | "tld" | "page-signal"
  confidence  SMALLINT DEFAULT 5,            -- 1-10, cuán confiable es el source
  updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Index para búsquedas por país (ej. "todos los AR cacheados")
CREATE INDEX IF NOT EXISTS toolbar_domain_geo_cache_country_idx
  ON toolbar_domain_geo_cache (country);

-- Index para limpieza por fecha
CREATE INDEX IF NOT EXISTS toolbar_domain_geo_cache_updated_idx
  ON toolbar_domain_geo_cache (updated_at);

-- RLS: lectura abierta a todos los authenticated, escritura libre también
-- (es cache compartida, no info sensible)
ALTER TABLE toolbar_domain_geo_cache ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "geo_cache_select" ON toolbar_domain_geo_cache;
CREATE POLICY "geo_cache_select" ON toolbar_domain_geo_cache
  FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS "geo_cache_insert" ON toolbar_domain_geo_cache;
CREATE POLICY "geo_cache_insert" ON toolbar_domain_geo_cache
  FOR INSERT TO authenticated WITH CHECK (true);

DROP POLICY IF EXISTS "geo_cache_update" ON toolbar_domain_geo_cache;
CREATE POLICY "geo_cache_update" ON toolbar_domain_geo_cache
  FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
