-- ════════════════════════════════════════════════════════════════════════
-- AUDITORÍA 48H (2026-07-11) — pedido del user para afinar el filtro y el agente.
-- Correr en Supabase SQL Editor (proyecto ticjpwimhtfkbccchfyp). Son SELECT (no tocan nada).
-- ════════════════════════════════════════════════════════════════════════

-- ── 1) URLs que SUPERARON el mínimo de tráfico pero fueron RECHAZADAS POR CONTENIDO ──
-- El floor de tráfico se chequea ANTES que el filtro de contenido: los rechazados por
-- tráfico tienen error_message tipo 'pageviews ... below min' (NO aparecen acá). Así que
-- todo lo de abajo YA pasó el mínimo y cayó por tipo de web. El motivo está en error_message:
--   not_publisher: nonpub_<tipo>   → detector estructural (ecommerce/bank/travel/realestate/...)
--   not_publisher: haiku_<tipo>    → la IA (marketplace/service/corp/...)
--   not_publisher: title_nonpub    → título (login/checkout/panel)
-- Si ves un PUBLISHER REAL en esta lista → es un falso rechazo, pasámelo y afino.
SELECT domain, error_message, processed_at
FROM toolbar_csv_queue
WHERE status = 'skipped' AND error_message LIKE 'not_publisher:%'
ORDER BY processed_at DESC
LIMIT 300;

-- (opcional) conteo por motivo, para ver qué capa descarta más:
-- SELECT split_part(error_message,' ',2) AS motivo, COUNT(*)
-- FROM toolbar_csv_queue WHERE status='skipped' AND error_message LIKE 'not_publisher:%'
-- GROUP BY 1 ORDER BY 2 DESC;


-- ── 2) Qué email se ELIGIÓ sobre otros en cada URL + cuáles se DESCARTARON ──
-- candidates (jsonb) = TODOS los emails encontrados, ordenados best-first, con
-- {email, source, score, tier, bounced}. chosen_email = el que mandó el agente.
-- Los demás del array = los descartados. tier: 4 apollo/informer · 3 publicidad@ · 2 persona · 0 genérico.
-- Marcá en cuáles vos hubieras elegido lo mismo y en cuáles no.
SELECT created_at, domain, chosen_email, chosen_source, chosen_tier, n_candidates, candidates
FROM toolbar_email_picks
WHERE n_candidates >= 2
ORDER BY created_at DESC
LIMIT 100;

-- (opcional) versión legible: un renglón por email candidato al lado del elegido:
-- SELECT p.created_at, p.domain, p.chosen_email,
--        c->>'email' AS candidato, c->>'source' AS source, (c->>'tier')::int AS tier,
--        (c->>'email' = p.chosen_email) AS fue_elegido
-- FROM toolbar_email_picks p, jsonb_array_elements(p.candidates) c
-- WHERE p.n_candidates >= 2
-- ORDER BY p.created_at DESC, tier DESC LIMIT 300;


-- ── 3) A qué URL y a qué email se envió por día, y CUÁNTOS por día (cap = 10/día) ──
-- 3a) Conteo por día y por MB (verificar que ninguno pase de 10):
SELECT (sent_at AT TIME ZONE 'Europe/Madrid')::date AS dia,
       mb_email,
       COUNT(*) AS enviados
FROM toolbar_response_tracking
WHERE sent_at >= NOW() - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- 3b) Detalle: qué URL y a qué email exacto se envió cada día (últimos 3 días):
SELECT (sent_at AT TIME ZONE 'Europe/Madrid')::date AS dia,
       mb_email, domain, email_sent_to, source, sent_at
FROM toolbar_response_tracking
WHERE sent_at >= NOW() - INTERVAL '3 days'
ORDER BY sent_at DESC;

-- 3c) Si el conteo 3a parece pasarse de 10, cruzar con lo que el CAP realmente cuenta
--     (toolbar_agent_actions action in sent+reserved — 'reserved' son slots apartados pre-envío):
-- SELECT (created_at AT TIME ZONE 'Europe/Madrid')::date AS dia, user_email,
--        COUNT(*) FILTER (WHERE action='sent')     AS enviados,
--        COUNT(*) FILTER (WHERE action='reserved') AS reservados
-- FROM toolbar_agent_actions
-- WHERE action IN ('sent','reserved') AND created_at >= NOW() - INTERVAL '7 days'
-- GROUP BY 1,2 ORDER BY 1 DESC, 3 DESC;
-- NOTA: toolbar_response_tracking = envíos del AGENTE. Envíos MANUALES (popup) van por otra vía
-- y NO cuentan para el cap del agente — si Maxi "mandó 21", parte pueden ser manuales.
