-- ════════════════════════════════════════════════════════════════════════
-- DEDUP DE REBOTES POR MENSAJE (2026-07-17)
--
-- Bug que arregla: el dedup del scan era `if (!isBouncedSync(failed))` = la tabla
-- toolbar_bounced_emails. Pero los SOFT bounces a propósito NO se marcan ahí (son
-- transitorios, no se blacklistean) → nunca entraban → CADA pasada del scan volvía a
-- "descubrir" el mismo mensaje durante los 7 días de ventana de Gmail.
--
-- Medido el 17/07 (7 días): evima.gr detectado 64 veces, mail.gmail.com 61, gmail.com 22,
-- fumettologica.it 21. Total 233 detecciones sobre solo 46 dominios reales.
-- Lo grave no era el log inflado: queueBounceRetry se re-disparaba en cada pasada →
-- REENVÍOS repetidos (34 bounce_retry_sent sobre 20 dominios) quemando reputación.
--
-- Ahora el dedup es por ID de mensaje de Gmail: un rebote se procesa UNA vez, sea
-- hard, soft o unknown.
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS toolbar_bounce_seen (
  msg_id  text PRIMARY KEY,          -- id del mensaje de Gmail ya procesado
  seen_at timestamptz NOT NULL DEFAULT now()
);

-- Para la limpieza por TTL (la ventana del scan es 7d; 30d es margen de sobra).
CREATE INDEX IF NOT EXISTS idx_bounce_seen_at ON toolbar_bounce_seen (seen_at);

ALTER TABLE toolbar_bounce_seen ENABLE ROW LEVEL SECURITY;

-- Limpieza del blocklist: sacar SOLO infraestructura inequívoca que el parser roto metió.
-- OJO: NO se tocan gmail.com / outlook.com / hotmail.com / yahoo.com — un publisher chico
-- puede tener un contacto real ahí y borrarlo lo des-blacklistearía (le volveríamos a
-- escribir a una casilla que rebota de verdad). Para esos, ver el SELECT de abajo y decidir.
DELETE FROM toolbar_bounced_emails
WHERE split_part(email, '@', 2) IN (
  'mail.gmail.com','googlemail.com','google.com','mail.google.com','gsuite.google.com',
  'support.google.com','accounts.google.com','gstatic.com','yahoodns.net',
  'protection.outlook.com','amazonses.com','sendgrid.net','mailgun.org'
);

-- Revisión manual: entradas en freemail que PUEDEN ser basura del parser o contactos
-- reales. Si el "reason" es genérico y nunca les escribiste, se pueden borrar a mano.
SELECT email, reason, original_domain
FROM toolbar_bounced_emails
WHERE split_part(email, '@', 2) IN ('gmail.com','outlook.com','hotmail.com','yahoo.com')
ORDER BY email;
